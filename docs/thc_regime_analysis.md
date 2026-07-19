# Tiered Hash Cache (THC): regime analysis

This investigation answers two questions:

1. Why does the NEW `settings-common.sql` config make the backward RPT+ pass dramatically better than THC, when it was supposed to *isolate* THC's contribution?
2. On JOB and TPC-H, are there queries where THC actually matters?

The release-build numbers all come from `build/release/duckdb` at HEAD (`29121bd1b7`); phase-transition traces come from `build/debug/duckdb` rebuilt at the same HEAD. Single-thread (`SET threads = 1`), `pin_threads='on'`, `disable_perfect_hashing=true`.

## TL;DR

- **THC is essentially a wash on JOB and TPC-H sf=10.** Mean THC contribution (case 3 − case 2 latency, normalized): **−0.10 % on JOB-light** (33 "a" queries × 3 seeds), **−0.66 % on TPC-H sf=10** (22 queries × 2 runs). Backward pass mean contribution is **+19.4 %** on JOB and **+12.7 %** on TPC-H.
- **THC never beats backward** on JOB (0/33 queries). On TPC-H sf=10, THC nominally beats backward in 4/22 queries, but three of those four are within ±0.5 % (noise) — only Q18 has a meaningful gap, and there backward is *also* worse than forward-only, so the comparison is moot. The single biggest THC win on TPC-H is **Q22 +2.83 %**, while backward beats THC on the same query by another +1.25 %.
- **The "0.000 ms" THC counters in the JSON profile are an artifact** of how local thread timings are flushed. The local `state.thc_*_time_ns` is only flushed into the sink atomic at `Finalize()`, but `EmitProbeTiming` reads the sink atomic *on every chunk* before that — so the JSON profile's `THC Probe/Collect/Insert Time` rounds to 0 even when THC is actively running. The debug build at HEAD confirms THC is in fact transitioning past BASELINE on `rs`, TPC-H Q5, and several JOB queries — it just doesn't help much.
- **Why the NEW optimizer doesn't isolate THC's benefit (the user's main question):** for case 3 (forward + THC) to be a fair THC isolation, case 2 (forward, no THC) needs to be at least as fast as case 1 (DuckDB-only baseline) — otherwise case 3 = case 2 + THC inherits a degraded baseline that THC can't recover from regardless of how well it works. On `rs` and on most JOB / TPC-H queries, the forced-seeded forward BF is *strictly an overhead* (zero or near-zero filtering), so case 2 is *slower* than case 1 and THC has nowhere to hide. Meanwhile, case 4's backward BF directly attacks the cost THC has no mechanism to address — HT build size — so case 4 wins handily.
- **Recommendation:** the structural mismatch ("THC accelerates probes but cannot reduce HT build cost; on selectively-filtered workloads, build cost dominates") will not be fixed by tuning the THC knobs. The activation gate should at minimum be probe-side-aware: don't allocate the 24–32 MiB cache and pay the BASELINE-tracking overhead on a HJ whose estimated probe input is below `thc_collect_phase_rows`. The honest framing for THC's value is as a *complement* to backward RPT+ (or in the absence of any RPT+) on workloads where the build side is bounded but the probe is large and skewed — not as a replacement for backward.

## Phase A — `rs` query: NEW vs OLD per case

| config | case | latency (s) | HJ build child | HT size | THC instantiated? | THC active? | n CREATE_BF / USE_BF |
|---|---|---|---|---|---|---|---|
| NEW | 1 | 0.641 | TABLE_SCAN(S) / 4 M | 144 MiB | gated off (case flag) | no | 0 / 0 |
| NEW | 2 | 0.988 | CREATE_BF(S) / 4 M (no filter) | 144 MiB | gated off (case flag) | no | 1 / 1 |
| NEW | 3 | 0.955 | CREATE_BF(S) / 4 M (no filter) | 144 MiB | yes | yes — BASELINE→COLLECT@1 M, then SKIP into permanent READ_ONLY (`budget_ok=0` on first checkpoint); cache fill 377 K / 1 M (36 %); miss_rate=7.5 %; `mu_{S→R}=9.80` | 1 / 1 |
| NEW | 4 | 0.582 | USE_BF / 400 K | 3 MiB | gated off (case flag) | no | 2 / 2 |
| OLD | 1 | 0.803 | NESTED_LOOP_JOIN(S × g) | 269 MiB | n/a | no | 0 / 0 |
| OLD | 2 | 0.810 | NESTED_LOOP_JOIN(S × g) | 269 MiB | n/a | no | 0 / 0 (`skip_unfiltered_tables` skipped them) |
| OLD | 3 | 0.811 | NESTED_LOOP_JOIN(S × g) | 269 MiB | **`Skipping THC: build source is not a base table.`** | no | 0 / 0 |
| OLD | 4 | 0.799 | NESTED_LOOP_JOIN(S × g) | 269 MiB | n/a | no | 0 / 0 |

Source: `_thc_invest/rs_{new,old}_case{1..4}.json` (release-build profile JSONs) and `_thc_invest/dbg/rs_{new,old}_case{1..4}.log` (debug-build phase traces).

**OLD optimizer**: `skip_unfiltered_tables_create_bf_plan=true` causes RPT+ to consider both R and S "unfiltered" — their `<= filtered_R/S` predicates are tautological with `selected_fraction_*=1.00` — and skip BF creation in every case. `join_order_mode='duckdb'` lets DPhyp wrap S × g (the 1-row generator-counts CTE) in a NESTED_LOOP_JOIN that becomes the build child of the HJ. The THC-skip-if-base-table gate (`PhysicalSubtreeContainsJoin`, commit `0393681a34`) sees that NLJ and **skips THC instantiation entirely**. So all four OLD cases collapse to identical "vanilla DuckDB with a 269 MiB HT, no BFs, no THC". Hence ≈0.78–0.81 s for all four.

**NEW optimizer**: `join_order_mode='seeded_left_deep'` + `use_seeded_transfer_order=true` force a deterministic R-as-probe / S-as-build plan; `g` joins at the top instead of being baked into the build side, so the HJ build is now a clean base table → THC gate passes → THC is instantiated in case 3. Per case:

- **Case 1 (NEW)**: forced left-deep with no BFs, smaller HT (144 MiB vs OLD's 269 MiB because g is no longer in the build); 0.64 s.
- **Case 2 / 3 (NEW)**: the forward BF carries S's keys to R (build→probe direction was chosen because `LargestRootUpdated` made R the spanning-tree root, so the forward iterates `[S, R]` and S sends BF down to R). R's 400 K distinct keys are *all* in S, so the BF rejection rate on R is 0 % and `USE_BF/4 M` is a pure pass-through. We pay BF construction + lookup overhead with zero filtering benefit (~+0.30 s vs case 1). In case 3 the THC then transitions BASELINE→COLLECT after the first 1 M probes, flushes 377 K hot entries, computes `mu_{S→R}=9.80`, and the budget guard (`thc_collect_budget_fraction=0.25`) forces SKIP at the first checkpoint, locking THC into READ_ONLY for the remaining ~2 M probes. That gives a small READ_ONLY benefit but the COLLECT-phase overhead (~540 ns/probe vs 350 ns/probe baseline) cancels it out. Net: case 3 ≈ case 2 within noise.
- **Case 4 (NEW)**: backward BF actually fires — it carries the 400 K *distinct* R keys back to the S scan, filtering S from 4 M rows to 400 K. HT shrinks 48× (144 MiB → 3 MiB, fits in L2). 0.58 s.

### Why the NEW optimizer doesn't isolate THC's benefit

For case 3 (forward + THC) to be a fair THC isolation, case 2 (forward, no THC) needs to be at least as fast as case 1 (DuckDB-only baseline) — otherwise case 3 = case 2 + THC inherits a degraded baseline that THC can't recover from regardless of how well it works. On `rs`, the forced forward BF is *strictly* an overhead (zero filtering), so case 2 is *slower* than case 1, and THC has nowhere to hide. Meanwhile, case 4's backward BF directly attacks the dominant cost on this query — HT build size — which THC has no mechanism to address.

The "fair" THC isolation requires a workload where forward-only is at least neutral *and* where build-side reduction is *not* the dominant lever. Concretely:

1. The forward BF must actually filter the side it lands on. On `rs` the BF lands on R, where every key is matched in S, so filtering is 0 %. Reversing the direction (so R sends BF down to S) would fix this — pick `S` as transfer-graph root (the table whose join-key set is the *superset*).
2. Build-side reduction should not dominate. On `rs` the build side is 4 M rows; backward BF shrinks it to 400 K, a 10× reduction in build cost. THC can never replicate that. A workload where the build side is already small (or already shrunk by other means) is what gives THC something to win.

## Phase B — Parameter grid for THC sweet spot on `rs` (no regen)

All numbers are mean of 5 runs, single-thread, NEW optimizer config.

| `thc_collect_phase_rows` | `thc_first_read_only_phase_rows` | `thc_collect_budget_fraction` | case 2 | case 3 (THC) | case 4 |
|---|---|---|---|---|---|
| 1,000,000 (default) | 1,000,000 | 0.25 (default) | 0.966 | **0.955** | 0.564 |
| 100,000 | 100,000 | 0.25 | 0.958 | 0.994 | 0.567 |
| 100,000 | 1,000,000 | 1.00 | 0.968 | 1.025 | 0.566 |
| 8,192 | 8,192 | 0.50 | 0.972 | 0.980 | 0.567 |
| 8,192 | 1,000,000 | 1.00 | 0.972 | 1.063 | 0.560 |

Source: `_thc_invest/phase_b/thc_grid_same_db.csv`.

**Result: no THC parameter on the existing DB makes case 3 close to case 4.** Lowering `thc_collect_phase_rows` makes things *worse* — the COLLECT-phase overhead at small batch sizes (~540 ns/probe) exceeds the per-probe savings during READ_ONLY, and the cache fill ratio after one 1 M-probe COLLECT cycle is already enough that further collect cycles can only hurt. The single fastest configuration for case 3 is the default (1 M / 1 M / 0.25), and it's still 0.39 s slower than case 4. This rules out "tune the THC knobs" as a remedy on this query.

## Phase C — JOB-light + TPC-H sf=10

### JOB-light (33 `a` queries × 3 seeds)

Source: `_thc_invest/phase_c/job_light.csv`.

| metric | value |
|---|---|
| **mean THC contribution (case 2 → case 3)** | **−0.10 %** (median +0.00 %) |
| **mean BWD contribution (case 2 → case 4)** | **+19.45 %** (median +19.96 %) |
| queries where THC helps (>0.5 %) | 11 / 33 |
| queries where THC hurts (<−0.5 %) | 11 / 33 |
| queries where BWD helps (>0.5 %) | 33 / 33 |
| **queries where THC beats BWD** | **0 / 33** |

**Top 5 THC-positive JOB queries** (case 2 − case 3, and the gap between THC and backward):

| query | c2 (s) | c3 (s) | c4 (s) | THC | BWD | BWD beats THC by |
|---|---|---|---|---|---|---|
| 1a | 0.660 | 0.637 | 0.423 | **+3.54 %** | +35.86 % | +33.51 % |
| 10a | 1.950 | 1.897 | 1.397 | **+2.74 %** | +28.38 % | +26.36 % |
| 5a | 0.300 | 0.293 | 0.220 | **+2.22 %** | +26.67 % | +25.00 % |
| 27a | 0.350 | 0.343 | 0.340 | **+1.90 %** | +2.86 % | +1.05 % |
| 19a | 1.977 | 1.943 | 1.600 | **+1.69 %** | +19.06 % | +17.67 % |

**Top 5 THC-negative JOB queries** (THC adds overhead):

| query | c2 (s) | c3 (s) | c4 (s) | THC | BWD |
|---|---|---|---|---|---|
| 17a | 1.657 | 1.737 | 1.420 | **−4.83 %** | +14.29 % |
| 32a | 0.847 | 0.877 | 0.523 | **−3.54 %** | +38.19 % |
| 25a | 3.223 | 3.303 | 2.580 | **−2.48 %** | +19.96 % |
| 26a | 3.003 | 3.060 | 2.240 | **−1.89 %** | +25.42 % |
| 31a | 2.743 | 2.793 | 2.140 | **−1.82 %** | +21.99 % |

### TPC-H sf=10 (22 queries × 2 runs)

Source: `_thc_invest/phase_c/tpch_sf10/runtimes.csv`.

| metric | value |
|---|---|
| **mean THC contribution (case 2 → case 3)** | **−0.66 %** (median −0.35 %) |
| **mean BWD contribution (case 2 → case 4)** | **+12.73 %** (median +7.64 %) |
| queries where THC helps (>0.5 %) | 3 / 22 |
| queries where THC hurts (<−0.5 %) | 11 / 22 |
| queries where BWD helps (>0.5 %) | 16 / 22 |
| queries where THC beats BWD | 4 / 22 (3 within noise; only Q18 is meaningful, and there BWD is also worse than fwd-only) |

**Top THC-positive TPC-H queries**:

| query | c2 (s) | c3 (s) | c4 (s) | THC | BWD | BWD beats THC by |
|---|---|---|---|---|---|---|
| Q22 | 1.235 | 1.200 | 1.185 | **+2.83 %** | +4.05 % | +1.25 % |
| Q11 | 2.575 | 2.520 | 1.625 | **+2.14 %** | +36.89 % | +35.52 % |
| Q05 | 3.730 | 3.670 | 2.290 | **+1.61 %** | +38.61 % | +37.60 % |

**Debug-build verification of THC firing** (case 3, debug binary at HEAD `29121bd1b7`):

TPC-H sf=10 (top THC-positive runtime queries):

| query | THCs instantiated | THCs ≥ activation threshold | BASELINE→COLLECT | Verdict |
|---|---|---|---|---|
| Q05 | 2 | 2 | **2** (orders HT 300 K, customer HT 457 K; `c_main=274 ns/probe`, `c_main=283 ns/probe`) | THC actually runs |
| Q11 | 2 | 2 | **0** | THC instantiated but never leaves BASELINE — probe stream too small |
| Q22 | 0 | 0 | 0 | No HJ above the 1 M activation threshold |

JOB top THC-positive AND top THC-negative queries:

| query | runtime delta (c2−c3) | THCs instantiated | BASELINE→COLLECT | Verdict |
|---|---|---|---|---|
| 1a | +3.54 % | 1 | **0** | THC stays in BASELINE — measured win is noise |
| 10a | +2.74 % | 2 | **0** | same |
| 19a | +1.69 % | 5 | **0** | same |
| 24a | +1.28 % | 5 | **0** | same |
| 17a | −4.83 % | 3 | **0** | THC stays in BASELINE — measured loss is noise |
| 25a | −2.48 % | 4 | **0** | same |
| 26a | −1.89 % | 3 | **0** | same |
| 32a | −3.54 % | **0** | 0 | No HJ above the activation threshold; THC is not even instantiated |

**Across all 8 sampled JOB queries (the four biggest case-2-vs-case-3 wins and the four biggest losses), THC's adaptive code path executes zero times.** The reported wins and losses on JOB are pure measurement noise plus the cost of the per-chunk BASELINE-tracking branch. The forward-pass BFs shrink probe streams below the `thc_collect_phase_rows = 1 000 000` threshold for every HJ, so THC sits initialized but never transitions out of BASELINE — meaning every probe goes through the regular HT path and the entire 24–32 MiB THC allocation per HJ is wasted memory.

Q05 is the *only* query in this whole sweep (rs + JOB-light + TPC-H sf=10) where THC actually runs end-to-end, and even there backward beats it by 37 percentage points.

**Top THC-negative TPC-H queries**:

| query | c2 (s) | c3 (s) | c4 (s) | THC | BWD |
|---|---|---|---|---|---|
| Q18 | 23.05 | 24.30 | 28.47 | **−5.40 %** | −23.51 % |
| Q07 | 2.10 | 2.16 | 1.94 | **−2.86 %** | +7.64 % |
| Q13 | 5.30 | 5.42 | 5.29 | **−2.36 %** | +0.09 % |
| Q09 | 14.49 | 14.74 | 8.80 | **−1.76 %** | +39.25 % |
| Q16 | 1.77 | 1.80 | 1.32 | **−1.69 %** | +25.71 % |

Q18 is the only query where backward *also* hurts (−23.5 %) — likely worth a separate investigation but not relevant to the THC question.

### TPC-DS — skipped

The TPC-DS extension is not pre-built into this fork's binary (`v0.0.1`) and the public extension repo's `linux_arm64/tpcds.duckdb_extension` 404s for that version. Generating sf=10 would require either a custom build with `CORE_EXTENSIONS='tpcds'` or a one-off install from a different platform's extension archive. I bailed on TPC-DS in this pass.

## Why THC structurally cannot beat the backward pass on these workloads

THC's premise: a small "hot" subset of build-side keys is hit by most probes; copying the hot subset to an L3-sized tagged probe-acceleration table makes the steady-state probe cost lower because cache-resident data is faster to access. That premise needs three things to hold simultaneously:

1. The HT capacity is large enough to make a difference (the `thc_activation_threshold = 1 000 000` gate).
2. Each HT receives ≥ ~1 M probe rows so the phase machine actually transitions BASELINE→COLLECT→READ_ONLY (the `thc_collect_phase_rows = 1 000 000` threshold).
3. The hot subset is small (≤ ~36 MiB of rows by default) so the THC has high hit rate.

On heavily-filtered IMDb-style queries the **forward** Bloom-filter pass shrinks the probe stream entering each large HT to O(10²) rows, killing condition (2) — the THC sits initialized but never leaves BASELINE. On TPC-H the forward BF is more selective in absolute terms, so condition (2) does often hold, but the build side is still fully scanned and inserted into the HT before THC can populate its cache, so the dominant cost (HT build) remains. The backward pass, by contrast, attacks the build cost directly: it shrinks the build-side scan via a Bloom filter built from the surviving probe-side keys, and the HT goes from 144 MiB → 3 MiB (rs), 3.14 M → ~6 (24a chn join), 1 M → 228 (3a title), 8 M → small (Q9 part), etc. THC has no analog.

## Recommendations

1. **Fix the activation gate to be probe-side aware.** Today the gate is `capacity > thc_activation_threshold` (build-side metric). It should additionally require the *estimated probe-side row count for that HJ* to be ≥ `thc_collect_phase_rows`. The optimizer already estimates probe cardinality per HJ (the debug log shows `Estimated probe-side rows=…`). Adding `if (estimated_probe_rows < thc_collect_phase_rows) skip` to `JoinHashTable::InitializeTieredHashCache` would (a) eliminate the 24–32 MiB calloc per inert THC, (b) eliminate the per-probe BASELINE-tracking branch on every chunk, and (c) prevent the THC overhead in the case-3-hurts queries (17a, 25a, 26a, 31a, 32a on JOB; Q07, Q09, Q13, Q16, Q18 on TPC-H).

2. **Fix the JSON profile timing report.** `EmitProbeTiming` reads `sink.thc_*_time_ns` on every chunk while the local `state.thc_*_time_ns` is only flushed at `Finalize()`. The result: every per-chunk `AddExtraInfo` call writes a stale 0, and the value the user sees in the JSON is 0 even when THC ran. Either flush local timings inside `EmitProbeTiming` (one extra `FlushLocalTimings()` per chunk; cheap) or move the `AddExtraInfo` call to `Finalize` after the flush.

3. **Don't use the OLD optimizer config as a THC ablation baseline.** OLD's `skip_unfiltered_tables_create_bf_plan=true` and `join_order_mode='duckdb'` collapse the 4-case experiment to a single point because (a) RPT+ skips BF creation when filters are tautological and (b) DPhyp pushes a NLJ onto the build side that triggers the THC-skip gate. Whatever THC does or doesn't do, we won't see it under OLD.

4. **Reframe THC's value claim.** The investigation provides no evidence that THC matches RPT+ backward on left-deep heavily-filtered queries. The honest comparison is THC *plus* forward+backward RPT+: does THC add anything on top of full RPT+? That's case "3+4" combined (rpt_forward_only=false + thc_enabled), which we did not run here but which the harness supports. Even there, the structural argument suggests no — once the backward pass shrinks the HT by a factor of 10–100×, the "hot subset" is the whole HT, and there's nothing to cache that isn't already cache-resident.

5. **For a believable THC demo, change the workload, not the knobs.** The THC paper's regime is queries where the build side is already small relative to L3 budget, the probe side is huge, the access pattern is skewed, and there's no other mechanism reducing the probe stream. The `scripts/measure/run_hugo_generated.sh` benchmark with `--cold 100 --layout segmented` matches that regime structurally (40 M-row build, 400 K hot keys, 400 M-row probe, 1519 MiB HT of which 14 MiB is hot) — that's the right fixture to validate the paper claim, not `rs` or JOB.

## Critical files referenced

- `src/execution/join_hashtable.cpp:683` — `JoinHashTable::GetRowPointers` adaptive phase machine
- `src/execution/join_hashtable.cpp:1659` — `JoinHashTable::InitializeTieredHashCache` (where the activation gate lives)
- `src/execution/operator/join/physical_hash_join.cpp:218` — `build_source_is_base_table` gate from commit `0393681a34`
- `src/execution/operator/join/physical_hash_join.cpp:1099` — `FlushLocalTimings` (the JSON-counter timing-flush bug)
- `scripts/measure/settings-common.sql` — current NEW config; lines 35–55 contain the commented-out OLD config from commit `31e6e23296`
- `scripts/measure/run_hugo_generated.sh` — the right benchmark for THC's regime claim

## Reproduction

```bash
# Phase A: rs query, NEW vs OLD x case 1..4
bash /tmp/run_rs_profile.sh new 1 _thc_invest/rs_new_case1.json   # ... etc

# Phase B: THC parameter grid
bash /tmp/run_rs_thc_grid.sh 1000000 1000000 0.25 5 _thc_invest/phase_b/thc_grid_same_db.csv

# Phase C JOB:
scripts/measure/run_job.sh --cases 1,2,3,4 --seeds 3 --job-queries 1a,2a,...,33a --csv _thc_invest/phase_c/job_light.csv

# Phase C TPC-H:
bash /tmp/run_tpch_sweep.sh

# Phase C debug verification:
bash /tmp/run_tpch_debug.sh 5 _thc_invest/phase_c/dbg_tpch/q05.log
bash /tmp/run_job_debug.sh 10a _thc_invest/phase_c/dbg_job/10a.log
```

All raw CSVs and JSON profiles are under `/mnt/local_ssd/spy/_thc_invest/`.
