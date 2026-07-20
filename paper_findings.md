# RPT+ / THC Paper Findings

Working document for the analysis paper on hash joins: **vanilla DuckDB execution vs
forward-only predicate transfer vs THC vs full RPT+**, and the interplay between query
optimization and predicate-transfer robustness.

This file intentionally over-includes; trim for the paper. Every number cites its
generating script (under `scripts/analysis/`) and the source data (under
`/mnt/local_ssd/results-spy/results/`, indexed by `results_summary.csv`).

---

## 1. Methodology

### 1.1 The four execution modes ("cases")

All four cases run the **same physical join order** for a given `(query, seed)` pair;
they differ only in the predicate-transfer machinery layered on top
(`scripts/measure/run_tpc.sh`, `case_settings_for`):

| Case | Name | Settings | Meaning |
|---|---|---|---|
| 1 | DuckDB | `disable_rpt`, `disable_tiered_hash_cache` | vanilla execution, no Bloom filters |
| 2 | Fwd-only | `rpt_forward_only`, `disable_tiered_hash_cache` | forward Bloom-filter pass only |
| 3 | Fwd+THC | `rpt_forward_only` | forward pass + TieredHashCache |
| 4 | RPT+ | `disable_tiered_hash_cache` | full forward + backward pass |

### 1.2 Modeling optimizer fallibility: the seed sweep

`join_order_mode = 'seeded_left_deep'` with `use_seeded_transfer_order = true`
(`src/optimizer/join_order/join_order_optimizer.cpp`) replaces DuckDB's DPhyp join-order
optimizer with a **seeded random left-deep ordering**. Sweeping
`transfer_graph_seed = 0..19` yields 20 different join orders per query. This models an
optimizer that may produce any plan from a plausible plan space:

- `min` over seeds ≈ what a *perfect* optimizer would pick (best-plan proxy);
- `median` over seeds ≈ a *typical* plan from a fallible optimizer;
- spread over seeds = plan-quality risk that robustness mechanisms must absorb.

All cases share the seed, so per-seed comparisons are **same-plan** comparisons: the
effect of BFs/THC on a fixed join order.

### 1.3 Benchmarks and sweep grid

30 sweeps (`results_summary.csv`): JOB (113 queries), TPC-H SF{10,20,50,100} (22),
TPC-DS SF{10,20,50,100} (99), Appian (8) × threads {1, 8, 64} × cases {1,2,3,4} ×
seeds {0..19}, 1 run per tuple. Per-query DuckDB JSON profiles exist for every tuple
(`profiling_*` dirs). Timeout: 300 s (60 s Appian); sentinel 9999999. OOM/spill-limit
failure sentinel: 8888888.

Hardware: AWS instance, 64-core Arm Neoverse-N1 (Graviton2), 32 MiB L3, 125 GiB RAM,
local NVMe SSD. THC's `thc_l3_budget = 36 MB` (see §Threats: slightly above physical L3).
Key THC settings during the sweeps: activation threshold 1 M build rows, collect phase
100 k rows, runtime freeze/abandon checks disabled
(`thc_enable_first_cycle_check/delta_check/shrinkage_check = false`,
`thc_mu_s_method='none'`) — i.e. the sweeps measure a *non-adaptive* THC configuration.

### 1.4 Statistics conventions

- Timeouts are treated as +inf when computing per-(query, case) medians/mins (a timeout
  *is* worse than any measured runtime; dropping it would bias in favor of the failing
  case). A median can therefore be `inf` if ≥10/20 seeds timed out.
- "Win" = strictly lowest value among the four cases for that query.

---

## 2. Phase 0 — Corpus integrity and reproducibility

Script: `scripts/analysis/phase0_integrity.py` (loader: `scripts/analysis/corpus.py`).

### 2.1 Integrity

- All 30 sweeps are complete: every query has exactly 4 cases × 20 seeds. No gaps.
- **No OOM sentinels (8888888) anywhere in the 30 latest sweeps.** All failures are
  timeouts. (The TPC-DS OOM incident is separately investigated in §Phase 5; the sweep
  driver gained the OOM sentinel *because* of that incident, commit `6e24bd2df0`
  "continue sweep after OOMing".)
- Timeout counts per sweep (by case). Two distinct patterns:
  - **Case-1-only timeouts** (robustness signal): JOB t1: 9 timeouts, all case 1
    (queries 16b, 17b–f). TPC-DS accumulates case-1-only timeouts that grow with SF:
    SF10 t1 {case1: 24, others: 20}, SF100 t1 {case1: 77, case2: 43, case3: 44, case4: 43}.
  - **All-case timeouts** (plans beyond rescue): TPC-H Q05 times out for *all four cases*
    on the same 6 seeds at every SF and thread count (6 seeds × 4 cases = 24). TPC-DS
    Q24/Q44/Q54 similarly time out for all cases on many seeds at 1 thread.

### 2.2 Cross-sweep comparability

The five sweep commits (`bde1b45f`, `9f07e58e`, `d043edd7`, `b0b9a94f`, `6e24bd2d`) differ
**only in `scripts/measure/`** (thread-count setting, driver ergonomics). `git log
<sweep>..HEAD -- src/` is empty for all of them: **the engine is identical across all
sweeps and identical to the binary used for new experiments in this analysis.**

One caveat found (paper "threats to validity"): `max_temp_directory_size = '0KiB'`
(spilling *disabled*) was active for the t1 JOB/TPC-H/Appian sweeps (`bde1b45f`) and the
t64 JOB/TPC-H/Appian sweeps (`b0b9a94f`), but spilling was *allowed* for both TPC-DS t64
(`6e24bd2d`), all t8 sweeps (`d043edd7`), and TPC-DS/t1 (`9f07e58e`). Within any sweep all
four cases see the same policy, so case comparisons are fair; only cross-sweep absolute
comparisons of spill-prone queries are affected.

### 2.3 Reproducibility on this box (sanity reruns) — and a methodology discovery

- JOB t64, seed 0, cases 1/4 on 1a/10b/17a: reruns match the corpus within noise
  (e.g. 10b case1 0.31 vs 0.30 s; 17a case4 0.31 vs 0.41 s).
- TPC-H SF100 t64 Q05 case 1 seed 0, **warm** page cache: identical plan (operator
  cardinalities match exactly), join CPU timings within ~1%, but wall clock 2.8 s vs
  corpus 4.25 s, with `TABLE_SCAN` aggregated thread time 19 s vs corpus ~113 s.
  The corpus scan time is ~110–180 s for *every* seed — not a warm-up artifact.
- **Rerunning with `--drop-os-cache` reproduces the corpus exactly**: CSV runtime 4.12 s
  (corpus 4.25), latency 3.04 s (3.17), scan thread-time 105 s (113).
  → **The corpus sweeps were measured with a cold OS page cache before every query.**
  This must be stated in the paper's methodology (each query pays full NVMe read cost;
  scans are IO-heavy, which systematically raises the value of scan-reducing techniques
  at low thread counts and makes the memory-bandwidth picture at 64 threads more
  scan-dominated). All new experiments in this analysis therefore also use
  `--drop-os-cache` when their numbers are to be compared with corpus numbers.
- CSV `runtime_seconds` wraps the whole DuckDB CLI invocation (DB open + settings +
  query); JSON `latency` is query-only. At SF100/t64 the fixed overhead is ~1 s.
  Case comparisons are unaffected (same overhead in all cases), but per-query
  runtimes should not be read as pure query latencies.
- Implication: engine behavior is exactly reproducible; new targeted cold-cache
  experiments can be mixed with the corpus.

### 2.4 Re-validation of meeting-note examples on the latest sweeps

The meeting notes cite examples from **older** runs (May 14 JOB, Jun 30 TPC-H). On the
latest 1-thread sweeps the picture (min-over-seeds winner = case 1) is:

- JOB t1: case 1 holds the global best min for **15/113** queries:
  10b, 10c, 11a, 11b, 11c, 11d, 15b, 20b, 23b, 23c, 32a, 3b, 6b, 6c, 8c.
  (May-14 run had 13/113 incl. 10b, 23b — the notes' examples still hold.)
- TPC-H SF100 t1: only **1/22** (Q03). The notes' examples (Q4, Q11, Q12, Q13, Q16) came
  from the Jun-30 run (8/22 incl. all five). The Jun-30 run predates the final settings
  (`bde1b45f` "20 seeds + generate initially" / `18c10fed1b` "settings for benchmarks
  going forward"); with the final sweep configuration, DuckDB-wins on TPC-H become rarer
  still. → Paper should use the latest sweeps; the "DuckDB sometimes best" observation
  survives but is even weaker than the notes suggest. (Full winner taxonomy in Phase 1.)

---

## 3. Phase 1 — Aggregate results across the full corpus

Scripts: `scripts/analysis/phase1_aggregate.py` (tables in
`results/analysis/phase1/*.csv`, figures `winrate_median.png`,
`winrate_sameplan.png`, `geomean_speedup.png`) and
`scripts/analysis/phase1_threads.py` (`operator_breakdown.csv`).

Conventions: "win" = *strictly* lowest per-query value (exact ties count for
nobody; the runtime CSVs have 2-decimal precision, so at 64 threads many short
queries tie). Timeouts are +inf for winner determination and capped at the
timeout (300 s / 60 s Appian) for ratio metrics — this *understates* the
advantage of the more robust case.

### 3.1 Headline win rates (median-over-seeds = typical plan)

Strict per-query median wins, cases 1/2/3/4:

| Sweep | 1 thread | 8 threads | 64 threads |
|---|---|---|---|
| JOB (113 q) | 3 / 9 / 2 / **86** | 0 / 1 / 0 / **33** (79 tied) | 1 / 15 / 12 / **51** |
| TPC-H SF100 (22 q) | 2 / 4 / 3 / **13** | 2 / 2 / 0 / **11** | 3 / 3 / 0 / **5** (11 tied) |
| TPC-DS SF100 (99 q) | 20 / 11 / 7 / **45** | 10 / 7 / 2 / **24** | **14** / 4 / 5 / 13 (63 tied) |
| Appian (8 q) | 4 / 0 / 0 / 4 | 1 / 0 / 0 / 0 (7 tied) | 2 / 0 / 0 / 1 |

Same-plan tuple wins ((query, seed) pairs; all four cases execute the identical
join order) show the same shape, e.g. JOB t1: 150 / 135 / 93 / **1469** of 2260.

Full table: `results/analysis/phase1/win_rates.csv`.

### 3.2 Geomean speedups vs vanilla DuckDB (median plans, timeout-capped)

| Sweep | Fwd-only (c2) | Fwd+THC (c3) | RPT+ (c4) |
|---|---|---|---|
| JOB t1 | 2.53 | 2.51 | **2.78** |
| JOB t8 | 1.91 | 1.89 | **2.12** |
| JOB t64 | 1.57 | 1.56 | **1.58** |
| TPC-H SF100 t1 | 1.25 | 1.14 | **1.42** |
| TPC-H SF100 t8 | 1.27 | 1.16 | **1.39** |
| TPC-H SF100 t64 | 1.02 | 0.97 | 1.02 |
| TPC-DS SF100 t1 | 1.48 | 1.46 | **1.65** |
| TPC-DS SF100 t8 | 1.40 | 1.36 | **1.48** |
| TPC-DS SF100 t64 | 1.07 | 1.05 | 1.04 |
| Appian (any t) | ~1.00 | 0.85–0.93 | ~1.00 |

Consistent reading across all sweeps:

1. **Full RPT+ (c4) is the best overall performer at 1 and 8 threads on every
   join-heavy benchmark**; the backward pass adds a further 10–15% over
   forward-only (2.53→2.78 JOB t1; 1.25→1.42 TPC-H SF100 t1).
2. **THC (c3) never improves on plain forward-only (c2) in aggregate** — it is
   1–10% *slower* than c2 everywhere, and up to 15% slower on Appian. (§Phase 4.)
3. **At 64 threads the PT advantage collapses on TPC-H/TPC-DS** (geomeans ~1.0)
   but persists on JOB (1.58×). §3.5 explains why.
4. TPC-DS geomean rises with SF (t1: 1.55 @SF10 → 1.65 @SF100 for c4): PT's
   value grows as tables outgrow caches/memory.

### 3.3 Robustness: PT compresses the plan-quality distribution

Geomean over queries of the p90/p10 runtime ratio across the 20 seeds
(1.0 = plan choice does not matter; failures capped):

| Sweep | c1 DuckDB | c2 Fwd-only | c4 RPT+ |
|---|---|---|---|
| JOB t1 | **3.72** | 1.48 | **1.30** |
| JOB t8 | 2.81 | 1.30 | 1.19 |
| JOB t64 | 2.23 | 1.20 | 1.19 |
| TPC-H SF100 t1 | 1.24 | 1.17 | 1.13 |
| TPC-DS SF100 t1 | 1.33 | 1.11 | 1.10 |

On JOB, picking a bad plan costs vanilla DuckDB ~4× (p90/p10); under RPT+ the
same bad plan costs only ~1.3×. **Predicate transfer is plan-choice insurance.**
Timeout counts point the same way (TPC-DS SF100 t1: 77 case-1 timeout runs vs 43
for RPT+; JOB t1: 9 case-1 timeouts — 16b, 17b–f — vs zero for c2/c3/c4).

The forward pass provides most of the insurance (3.72→1.48); the backward pass
adds a further ~12% compression (1.48→1.30).

### 3.4 Orderings taxonomy (which stories exist, and how often)

Pattern of the four cases by median runtime, with ties collapsed at 5%
(`orderings_taxonomy.csv`; n = 605 query×sweep pairs per thread count):

- **1 thread**: `2~3~4<1` 17.7%, `4<2~3<1` 15.2%, all-tie 10.1%, `4~2~3<1` 7.9%,
  `4<3~2<1` 4.0% … → in ≈2/3 of queries some PT variant beats vanilla by >5%;
  in ≈1/3 of those the backward pass separates from forward-only.
- **8 threads**: `2~3~4<1` 30.2%, all-tie 25.1%, `4<2~3<1` 7.1%.
- **64 threads**: all-tie 30.4%, `1~2~3<4` 9.4% (RPT+ *loses* by >5%),
  `2~3~4<1` 9.3%, `1~2<3~4` 5.8%.
- Queries where vanilla DuckDB is *strictly* >5% faster than every PT variant
  are rare: **11/605 (t1), 6/605 (t8), 22/605 (t64)**. By contrast RPT+ is
  strictly >5% best in 188/605 (t1), 125/605 (t8), 36/605 (t64).
- The meeting-notes ordering `1>2>3>4` (each mechanism helps) and its
  tie-variants (`4<2~3<1` etc.) are the single most common non-tie pattern at
  t1/t8. TPC-H Q08 t1 shows the strict version.

**Correction to an earlier informal reading**: a naive "case 1 has the lowest
median" count at TPC-DS t64 gives 63/98 — but almost all of those are exact
ties at 2-decimal precision or <5% differences. The honest statement: at 64
threads most TPC queries are insensitive to the case, and case 1 strictly wins
only a handful (§3.5).

### 3.5 Why the PT advantage collapses at 64 threads (and where case 1 truly wins)

Operator-time attribution over all complete profiling tuples
(`operator_breakdown.csv`; thread-seconds by operator class, wall latency):

| Sweep | Case | Wall (s) | SCAN | JOIN | CREATE_BF | USE_BF |
|---|---|---|---|---|---|---|
| TPC-H SF100 t1 | 1 | 22 925 | 4 247 | 15 820 | 0 | 0 |
| | 4 | 15 494 | 4 170 | 7 050 | 722 | 1 144 |
| TPC-H SF100 t64 | 1 | 1 133 | 44 475 | 19 004 | 0 | 0 |
| | 4 | 1 105 | 53 873 | 7 406 | 650 | 1 409 |
| TPC-DS SF100 t64 | 1 | 11 311 | 156 997 | 196 770 | 0 | 0 |
| | 4 | 9 142 | 171 150 | 38 134 | 4 092 | 2 090 |
| JOB t64 | 1 | 1 330 | 7 122 | 23 312 | 0 | 0 |
| | 4 | 320 | 5 504 | 935 | 105 | 275 |

Mechanism:

- PT's benefit is **join CPU work**: on TPC-H t1 it removes ~8 770 thread-s of
  join time for ~1 870 thread-s of BF work → 1.42× wall win at 1 thread, where
  wall = total work.
- At 64 threads with cold page caches (§2.3), TPC-H/TPC-DS become
  **IO/scan-bound**: case 1's 19 000 thread-s of join work hides under 44 000+
  thread-s of scan/IO. Eliminating join work no longer shortens the critical
  path, so the geomean → 1.0. (SCAN thread-seconds even *rise* under PT — more
  threads idle in scan operators waiting on the same IO while downstream work
  has been filtered away.)
- JOB stays 4× at t64 because job.db is small (2.6 GB, quickly cached even
  after a cache drop) and JOB is join-CPU-bound, and JOB queries only reach
  ~23× effective parallelism anyway.
- Where case 1 *strictly* wins at t64 it is concentrated in TPC-DS
  Q04/Q11/Q14/Q38/Q70/Q74 — e.g. Q04: wall 5.8 s (c1) vs 12.1–12.4 s (c2/c4)
  even though PT *reduces* join thread-seconds (57→33). These are the
  year-over-year / multi-channel queries with big shared CTEs. The loss is not
  the BF probe work; it is **critical-path serialization**: `CREATE_BF` is a
  materializing pipeline breaker, and dependent scans cannot start until the
  BFs they consume are complete. At 1 thread extra dependencies are free (work
  is work); at 64 threads they lengthen the critical path on queries that are
  latency- not throughput-bound. → Paper point: **PT trades total work for
  dependency depth; at high parallelism on short queries this trade can lose.**

### 3.6 Scale-factor trend (TPC-H t1, c4 vs c1 geomean)

SF10 1.43× → SF20 1.41× → SF50 1.42× → SF100 1.42× (flat); TPC-DS t1:
SF10 1.55× → SF100 1.65× (rising). PT value is stable-to-rising with scale;
nothing suggests it degrades on larger data.

---

## 4. Phase 2 — Optimizer value vs predicate-transfer robustness

Script: `scripts/analysis/phase2_decomposition.py`
(`results/analysis/phase2/decomposition.csv`, `cdf_decomposition_t1.png`).

Per query: `best(c)` = min over 20 seeds (perfect-optimizer proxy), `median(c)`
= typical plan. Geomeans over queries per sweep (failures capped at timeout):

| Sweep | insurance med(c1)/med(c4) | fwd share med(c1)/med(c2) | residual QO w/ RPT+ med(c4)/best(c4) | residual QO w/o PT med(c1)/best(c1) | typical-RPT+ vs best-vanilla med(c4)/best(c1) | backward extra med(c2)/med(c4) |
|---|---|---|---|---|---|---|
| JOB t1 | **2.78** | 2.53 | 1.20 | 1.93 | **0.70** | 1.10 |
| TPC-H SF100 t1 | 1.42 | 1.25 | 1.03 | 1.10 | **0.77** | 1.14 |
| TPC-DS SF100 t1 | 1.65 | 1.48 | 1.04 | 1.24 | **0.75** | 1.11 |
| JOB t8 | 2.11 | 1.91 | 1.10 | 1.57 | 0.74 | 1.11 |
| TPC-H SF100 t8 | 1.39 | 1.27 | 1.05 | 1.10 | 0.79 | 1.09 |
| TPC-DS SF100 t8 | 1.48 | 1.40 | 1.04 | 1.24 | 0.84 | 1.06 |
| JOB t64 | 1.58 | 1.57 | 1.14 | 1.45 | 0.92 | 1.01 |
| TPC-H SF100 t64 | 1.02 | 1.02 | 1.02 | 1.03 | 1.01 | 1.00 |
| TPC-DS SF100 t64 | 1.04 | 1.07 | 1.03 | 1.11 | 1.07 | 0.97 |

Findings (the paper's core quantitative claims):

1. **A typical plan + RPT+ beats the best plan + vanilla execution** at 1–8
   threads: geomean med(c4)/best(c1) = 0.70–0.84; per-query share where
   typical RPT+ wins outright: JOB t1 **75%**, TPC-H SF100 t1 **59%**, TPC-DS
   SF100 t1 **72%**. Predicate transfer buys more than perfect join ordering
   does, in the regimes where execution is join-bound.
2. **But the optimizer is not replaceable**: even under RPT+, a perfect plan
   still adds 1.20× on JOB t1 (residual QO), and the tail is fat — 13% of JOB
   queries are >15% slower with typical-plan RPT+ than best-plan vanilla. And
   at 64 threads the claim flips entirely (med(c4)/best(c1) ≥ 1.0 everywhere):
   when PT cannot help (IO-bound), plan quality is all that is left.
3. **QO and PT are complements, not substitutes**: PT compresses the penalty of
   bad plans (residual QO 1.93 → 1.20 on JOB t1) but does not eliminate it.
   Takeaway sentence: *definitely do a forward pass, probably do a backward
   pass, and keep your optimizer.*
4. **The backward pass is worth ~+10% on typical plans at t1/t8**
   (med(c2)/med(c4) = 1.06–1.14), free at JOB t64 (1.01), and slightly
   *negative* on TPC-DS at t64 (0.97) where its extra CREATE_BF serialization
   costs latency (§3.5).
5. The extreme-looking tpcds_sf20_t64 numbers (insurance 0.88; 70.7% of
   queries >15% worse under typical RPT+ than best vanilla) were checked and
   are *not* data corruption — they are §3.5's serialization overhead in
   relative terms: at SF20/t64 most TPC-DS queries run in 0.2–3 s, and RPT+'s
   ~0.1–1 s of extra pipeline-dependency latency is a large fraction of that
   (e.g. Q13: 0.83 s c1 vs 1.24 s c4; Q43: 0.52 vs 0.73). Same absolute
   overheads exist at SF50 but are relatively smaller. → PT overhead behaves
   like a *fixed latency tax* per BF-consuming pipeline; it matters exactly
   when queries are short and parallelism is high.

---

## 5. Phase 3 — Case studies

Tools: `scripts/analysis/plan_tree.py` (renders corpus profiling JSONs);
new measurements under `results/analysis/phase3/` (this box, cold cache,
threads = 1 unless noted). Per-seed tables come from the latest t1 sweeps.

### 5.1 JOB 10b — when vanilla DuckDB is the global best (rare case, mechanism)

Per-seed runtimes (job_t1, seconds):
case 1 ranges **0.47–2.40** (5× plan sensitivity); case 4 is flat 0.63–0.69;
global best = case 1 at seeds 6/10/18 (0.47).

Plan forensics (`plan_tree.py job_t1 job_q10b {1,2} 6` vs `{1,4} 17`):

- The lucky vanilla plan (seed 6) wins because of **DuckDB's native dynamic
  join-filter pushdown**: `role_type` (1 row) sits directly above the
  `cast_info` scan, so a min/max filter collapses `cast_info` to 13 rows and
  `title`'s scan to 375 rows. Residual runtime is dominated by the `cast_info`
  scan itself (0.38 s of 0.43 s) — there is nothing left for BFs to prune.
- On the same seed-6 plan, case 2 costs 0.63 s (+45%): (a) `CREATE_BF` over
  391 k `title` rows (0.041 s + 0.07 s scan), and — the subtle part —
  (b) **`CREATE_BF`'s materialization breaks the native dynamic-filter
  pushdown**, so `title` is scanned in full (391 666 rows vs 375). PT here
  *replaces* a cheaper native mechanism with a more expensive general one.
- On a bad plan (seed 17: case 1 = 2.31 s), the same BFs rescue execution:
  `cast_info` collapses to 13 rows and `movie_companies` 2.2 M→9 252 via
  USE_BF; case 4 = 0.62 s. **PT is the same machinery that dynamic filters
  are, but plan-order-independent.**
- Vanilla's win is *plan luck*: only 3/20 seeds give case 1 < 0.6 s; its median
  is 1.24 s. In expectation over plans, case 4 (0.67 flat) wins comfortably.

Fixed-best-plan experiment (this box, seed 6, t1, cold, median of 3):
case1 0.72, case2 1.03, case3 0.93, case4 1.03 → on the *best* plan PT costs
+30–43%. JOB 23b behaves identically (0.72 vs 0.83, +15%).
DuckDB-best queries share this signature: highly selective early joins +
native dynamic filters already firing + sub-second runtimes where BF setup
cannot amortize.

### 5.2 JOB 17b — catastrophic plans and PT as insurance

Per-seed case 1: 1.76 s → 123 s → **timeout (>300 s)** at seed 8; cases 2/3/4:
1.1–4.8 s on every seed. This is the modal JOB story (9 case-1 timeouts in the
t1 sweep: 16b, 17b–f). Notably, on vanilla's own best plan (seed 18), PT still
wins: 2.05 s (c1) vs 1.64 s (c2/c4) — for probe-heavy queries even the best
join order leaves prunable work.

### 5.3 TPC-H Q08 SF100 — the clean 1>2>3>4 ordering

Medians over seeds (t1): 74.4 (c1) > 27.4 (c2) ≈ 27.3 (c3) > 23.3 (c4); the
strict ordering holds at nearly every seed. Same-plan forensics (seed 3;
`plan_tree.py tpch_sf100_t1 tpch_q8 {1,2,4} 3`):

- Case 1 (113 s): the `part` filter (134 k of 20 M parts) is applied at the
  *top* of the join tree, so `lineitem ⋈ orders ⋈ customer` materializes a
  182 M-row intermediate that flows through three more joins (~95 s of join
  time).
- Case 2 (25 s): the forward BF from `part` prunes the `lineitem` scan
  600 M → 4.1 M rows *at the scan*; every intermediate collapses to ≤1.2 M
  rows. Cost: 6.7 s of USE_BF on the lineitem stream.
- Case 4 (22.4 s): the backward pass additionally prunes `orders`
  45.6 M → 1.3 M before the HT build (smaller build: peak buffer 9.3 GB vs
  11.2 GB; the `l_orderkey` probe join drops 4.2 s → 0.35 s).
- Fixed-best-plan experiment (seed 19, vanilla's best): 53.4 / 37.7 / 37.6 /
  32.9 s — **even the best vanilla plan loses 1.6× to RPT+ on the same
  ordering**. Contrast with §5.1: whether "best plan vanilla" wins depends on
  whether the *best* ordering still carries prunable probe work.

### 5.4 TPC-DS Q37 SF100 — why the backward pass makes runtime plan-independent

Per-seed (t1): case 1 = {3.6 s on lucky seeds, 292–295 s on three seeds,
timeout on 9/20 seeds}; case 2 = 3.4–4.0 s; **case 4 = 2.05 s dead flat on all
20 seeds**. Forensics (seed 5):

- Case 1 (292 s): ordering joins `catalog_sales` (144 M) with
  `inventory⋈date_dim` (5.2 M) *before* the `item` filter → a
  **4.9-billion-row intermediate** (inventory holds ~many rows per item), then
  filters by item (104 s + 183 s of join time).
- Case 2 (3.3 s): forward BFs (item, date_dim) prune `inventory` to 217 rows —
  the explosion never happens. But `catalog_sales` still streams 78 M rows
  through the probe (2.0 s).
- Case 4 (1.85 s): the backward pass sends a BF from the filtered
  `inventory⋈date` result back onto `catalog_sales`: 78 M → **5 722** rows.
  All that remains on any seed is the base-table scan cost → identical runtime
  on every plan. TPC-DS Q82 shows the same pattern (c2 7.3–11 s vs c4 flat
  4.2–4.7 s).
- Paper framing: *forward-only insures against intermediate explosions; the
  backward pass also removes unproductive probe streams, which is what makes
  runtime fully plan-independent.*

### 5.5 TPC-H Q05 SF100 — plans beyond rescue (limits of PT)

Six seeds (1, 3, 5, 7, 13, 19) time out (>300 s) in **all four cases**, at
every SF and thread count. EXPLAIN on seed 1 shows why: the seeded ordering is
`((((lineitem ⋈ supplier) ⋈ nation) ⋈ customer ON s_nationkey = c_nationkey)
⋈ region) ⋈ orders` — it joins customer on **nationkey** (25 distinct values,
~600 k customers per key), a many-to-many key explosion. Every joined row is
"productive" w.r.t. that predicate, so no semi-join filter can prune it; the
selective predicates (`o_custkey = c_custkey`, `l_orderkey = o_orderkey`) sit
in the final join, too late. **Predicate transfer insures against bad filter
placement, not against join-shape blowups on low-cardinality keys — that
remains the optimizer's job.** (Supports takeaway: keep the optimizer.)

Also note: on vanilla's best Q05 seed (14), the forward pass alone does not
help (c1 78.3 s, c2 81.5 s, c3 85.7 s on the fixed plan — Q05's probe streams
are largely productive so forward BFs prune little and cost extra), but the
full backward pass still wins (c4 = 69.5 s): it shrinks the *build* sides
(orders/customer HTs) even when probe-side pruning is worthless.

### 5.5b Fixed-best-plan experiment, summary (the "value of QO" question)

Running all four cases on the seed where vanilla DuckDB found its best plan
(this box, t1, cold cache):

| Query | c1 (best plan) | c2 | c3 | c4 | verdict on best plan |
|---|---|---|---|---|---|
| JOB 10b (seed 6) | **0.72** | 1.03 | 0.93 | 1.03 | vanilla wins (+43% PT cost) |
| JOB 23b (seed 1) | **0.72** | 0.83 | 0.83 | 0.83 | vanilla wins (+15%) |
| JOB 17b (seed 18) | 2.05 | 1.64 | 1.65 | **1.64** | RPT+ wins 1.25× |
| TPC-H Q08 (seed 19) | 53.4 | 37.7 | 37.6 | **32.9** | RPT+ wins 1.62× |
| TPC-H Q05 (seed 14) | 78.3 | 81.5 | 85.7 | **69.5** | RPT+ wins 1.13× (backward only) |
| TPC-DS Q37 (seed 3) | 4.20 | 4.10 | 4.00 | **2.77** | RPT+ wins 1.52× |
| TPC-DS Q82 (seed 4) | 8.60 | 8.30 | 8.19 | **5.53** | RPT+ wins 1.56× |

Even a *perfect* join-order optimizer without PT loses to RPT+ on the
scan/join-heavy queries; it wins only on sub-second queries where DuckDB's
native dynamic filters already do the pruning and BF setup cannot amortize.
Query optimization's unique, irreplaceable contribution is avoiding
structural blowups (Q05's nationkey join, §5.5) — not filter placement.

### 5.6 TPC-DS Q72 — THC's best case (and how small it is)

Case 1 times out on 14/20 seeds; PT variants run 30–74 s. THC consistently
beats plain forward-only here: median c3/c2 = 0.98 (35.7 vs 39.1 s at seed 0).
Telemetry: the dominant join probes `customer_demographics` (384 k rows ≈
20 MB HT) **119 M times** with a hot set of ~93 k rows that fits the THC —
exactly THC's design regime. A ~2–9% gain on the single most THC-friendly
query in all four benchmarks is the ceiling we observed. (§6 for the full THC
autopsy.)

### 5.7 TPC-DS Q04/Q11/Q14/Q74 at 64 threads — where PT genuinely loses

(§3.5 mechanism.) Q04 t64: case 1 wall 5.8 s vs 12.1/12.4 s (c2/c4) while
PT *reduces* join thread-seconds 57→33: the loss is critical-path
serialization of `CREATE_BF` materialization on year-over-year CTE plans,
plus BF work on streams the year filters already made small. These queries are
the honest "PT hurts" examples for the paper — the cost is latency structure,
not probe overhead.

---

## 6. Phase 4 — THC autopsy: why explicit hot-set caching underperforms

Script: `scripts/analysis/phase4_thc.py`
(`results/analysis/phase4/{thc_states.csv, thc_deltas.csv}`).

Configuration caveat (must be stated in the paper): the sweeps ran THC
**non-adaptively** — all runtime abandon/freeze checks disabled
(`thc_enable_first_cycle_check/delta_check/shrinkage_check = false`,
`thc_mu_s_method = 'none'`), activation threshold 1 M build rows, 36 MB budget
on a 32 MiB-L3 machine. So case 3 measures "THC always on where activated".

### 6.1 THC rarely even activates

Share of hash-join instances (case 3, t1) whose build side reaches the 1 M-row
activation threshold: JOB **8.7%**, TPC-DS SF10 4.2% → SF100 10.1%,
TPC-H SF10 29.6% → SF100 38.6%, Appian 28.3%. Most joins in these workloads
build small hash tables (dimensions); the L3-cache problem THC targets is the
exception, not the rule. All observed freezes are `THC-Full` (capacity), zero
runtime abandons (checks were off).

### 6.2 When it activates, it usually taxes

Geomean case3/case2 runtime ratio over (query, seed) tuples, split by whether
any THC instantiated: without instantiation the ratio is 1.00 everywhere
(sanity check — same code path); with instantiation it is **1.01 (JOB), 1.05–1.07
(TPC-DS), 1.09–1.14 (TPC-H, growing with SF), 1.08 (Appian)**.

Worst regressions (median over seeds): TPC-H SF100 Q22 **+49%**, Q04 +36%,
Q21 +32%, Q12 +27%; TPC-DS Q01 +31%. Telemetry signature in every one of
them: joins with **hundreds of millions of probes** (Q22: 150 M; Q04: 379 M;
Q21: 600 M + 379 M) whose THC froze `THC-Full` after ~0.9–1.8 M inserts —
i.e. the hot set exceeds the 36 MB budget, and afterwards *every* probe pays a
futile THC lookup before falling through to the main hash table. A
double-probe tax on the highest-probe joins in the workload.

Best cases: TPC-H SF10 Q13 −3.8%, TPC-DS Q72 −2.2% (hot set fits; §5.6), a
handful of JOB queries −1%. **The observed upside of explicit caching is ≤4%
on the most favorable query; the downside is −50% without adaptivity.**

### 6.3 Implicit vs explicit caching (paper narrative)

The premise of THC was that copying hot build rows into a compact L3-resident
buffer beats leaving them scattered in a large hash table. The data say the
premise rarely pays off in-memory on a single node because:

1. Where the hot set fits in L3, **the hardware already keeps it there**
   (implicit caching): the frequently-touched HT rows and bucket-array
   entries stay resident, so THC's copy adds work without adding locality.
2. Where the hot set does not fit, THC cannot hold it either (`THC-Full`
   freezes) and degrades to a per-probe tax.
3. The backward pass attacks the same problem *upstream*: it deletes cold
   rows before the HT is built, so the HT itself becomes small(er) and cheap
   to build — it saves the build cost AND the locality, which the THC can
   never recoup (it runs after the build). Compare case 4's smaller peak
   buffer and faster builds in §5.3.
4. An idealized adaptive THC (perfect abandon policy) would merely converge
   to case 2 with upside limited to the §5.6-type queries — a few percent on
   a few queries per benchmark. (ASH-datagen, §Phase 7, probes the design
   space where THC *should* win by construction.)

---

## 7. Phase 5 — Memory risk: the TPC-DS Q54 OOM, and whether PT makes OOM worse

Motivation (meeting note): "I OOM'd on TPC-DS! (`run_tpc.sh --sf 10 --tpcds-query 54
--runs 1 --case 1 --seed 2`)" — and JP's comment that the original RPT PR into
DuckDB was rejected over OOM concerns, so any PT-related memory risk is of
direct interest.

### 7.1 The incident, reproduced exactly

The original incident ran under the **spilling-disabled** configuration
(`SET max_temp_directory_size='0KiB'`, active in `settings-common.sql` up to
commit `b0b9a94fe5`; commented out from `6e24bd2df0` "continue sweep after
OOMing" onward — i.e. all sweeps in the current corpus allow spilling).

Reproduction on TPC-DS SF10 (a **3.0 GB** database file, 125 GB RAM machine),
case 1, seed 2, spilling disabled:

| case | outcome | peak RSS | wall time |
|---|---|---|---|
| 1 (DuckDB, seeded plan) | **Out of Memory Error** ("failed to offload data block", limit hit = DuckDB's 100 GB memory_limit with 0-byte temp quota) | **101.2 GB** | 22 s |
| 2 (forward-only) | identical OOM | 101.2 GB | 17 s |
| 4 (full RPT+) | identical OOM | 101.2 GB | 17 s |

A ~34x blowup of the entire database size in RSS, from one query. With
spilling *enabled* (corpus configuration) the same runs do not OOM — they
grind and hit the 300 s timeout instead, which is exactly what the corpus
records: **Q54 times out in all 4 cases x 20 seeds at every thread count (1/8/64)
and both scale factors (SF10, SF100)**.

### 7.2 Predicate transfer neither causes nor prevents this explosion

The physical plan under the seeded left-deep ordering (seed 2) explains why all
cases fail identically. Q54's `my_customers` CTE constrains
`d_month_seq BETWEEN (subquery)+1 AND (subquery)+3`; the seeded ordering
produces a plan containing a **CROSS_PRODUCT** (customer set x `store` matched
only on `ca_county = s_county, ca_state = s_state`) and **two NESTED_LOOP_JOINs**
with inequality conditions (`d_month_seq >= SUBQUERY`, `<= SUBQUERY`) sitting
above a ~28.8 M-row `store_sales ⋈ date_dim` input. The intermediate result
explodes far beyond RAM.

Bloom filters only act on **equi-join hash edges**; the exploding operators here
are a cross product and inequality NLJs, which are invisible to the transfer
graph. Cases 2 and 4 OOM at the same 101 GB because the BFs they add prune
nothing on the exploding path. This is the mirror image of the TPC-H Q05
all-case-timeout finding (§5): *when the plan's damage comes from non-equi
operators or many-to-many key explosions, PT is not insurance.*

### 7.3 The default optimizer avoids it entirely — the strongest pro-QO datapoint

Running the identical query, identical settings (spilling still disabled!) but
with DuckDB's native join-order optimizer (`join_order_mode='dphyp'`, RPT off):
**0.16 s, 0.39 GB peak RSS.** More than four orders of magnitude less
memory-time product than the seeded plan; not even close to memory pressure.

Paper takeaway: join ordering is not only a latency concern but a *safety*
concern. A bad ordering does not just run slow — it can take down the process
(no-spill) or the disk (spill). PT bounds neither, because its pruning is
limited to equi-join edges. Only the optimizer can refuse to place a cross
product / NLJ below an explosion. This complements §4's decomposition: PT is
insurance against *mispredicted selectivities on hash edges*, the QO is
insurance against *structurally catastrophic shapes*.

### 7.4 Corpus-wide failure census

Scanning every runtime CSV in the corpus for sentinels: **zero OOM sentinels
(8888888) anywhere**; all recorded failures are timeouts (9999999). With
spilling enabled, "memory explosions" convert into timeouts. The all-case
failure set is small and structural:

- TPC-DS **Q54**: all cases, all seeds, SF10 & SF100, t1/t8/t64 (cross product +
  inequality NLJ, §7.2).
- TPC-DS **Q44** at SF100: all cases, all seeds (window/rank over an unfilterable
  self-joined aggregate; runs ~85–95 s at SF10 in every case — pure scale-out of
  work PT cannot prune).
- TPC-H **Q05** at SF100 t1: seed-dependent all-case timeouts (§5, nationkey
  many-to-many explosion).
- TPC-DS **Q24** at SF100 t1: 3–4 bad seeds per case time out; on good seeds PT
  is a 6.6x win (91.1 s case 1 vs 13.6–14.0 s cases 2–4, seed 0).

### 7.5 Does PT itself add memory risk? (the upstream-PR concern)

Mechanically, yes: `PhysicalCreateBF` **materializes its entire input** into a
`ColumnDataCollection` before building the filter (it must see all rows before
downstream probes consume the BF). A CreateBF placed on a huge unfiltered
intermediate would be a real OOM vector — this is precisely the concern that
blocked the original RPT PR upstream.

Measured in practice (peak RSS, no-spill-irrelevant since none approach limits;
threads=64):

| query (SF10) | case 1 | case 2 (fwd) | case 4 (RPT+) |
|---|---|---|---|
| TPC-H Q9 | 2.04 GB | 1.73 GB | 1.48 GB |
| TPC-H Q21 | 1.17 GB | 1.21 GB | 1.07 GB |
| TPC-DS Q72 | 1.65 GB | 0.62 GB | 0.87 GB |
| TPC-DS Q82 | 1.35 GB | 1.34 GB | 0.16 GB |
| TPC-DS Q37 | 0.14 GB | 0.14 GB | 0.13 GB |

On PT-favorable queries the **net** effect is memory *reduction* — BFs shrink
the intermediates (and, in case 4, the hash tables) by more than the CreateBF
materialization adds. Note Q72: case 4 uses more than case 2 (0.87 vs 0.62 GB)
because the backward pass adds probe-side CreateBF materializations — the
backward pass's robustness is not memory-free. The fork also ships two
mitigations upstream RPT lacked: `drop_bf_at_runtime` (a `GiveUpBFCreation`
heuristic abandons filter creation under memory pressure / bad selectivity,
enabled in all sweeps) and spill-to-disk of the materialized collections.

Honest summary for the paper: *we observed no OOM attributable to PT; the one
real OOM was a join-ordering failure that PT was powerless to prevent. But
CreateBF materialization is a genuine tail risk on adversarial plans, and the
runtime give-up heuristic is the price of admission for making PT safe.*

---

## 8. Phase 6 — Distributed-system estimate: how expensive can Bloom filters get before PT stops paying?

Script: `scripts/analysis/phase6_distributed.py`
(`results/analysis/phase6/{distributed_winrates.csv, breakeven.csv}`).

Motivation (meeting note): "Estimate if we would be better on a distributed
system — need to multiply USE and CREATE BF by like 2.0 and see if it helps."

### 8.1 Model

In a distributed engine, BF creation requires merging partial filters across
workers and broadcasting them (extra network rounds); probing is unchanged per
tuple but filters arrive later. We model this as a cost multiplier k on the
measured `CREATE_BF` + `USE_BF` operator time:

    latency'(q, case, seed, k) = latency + (k−1) · t_BF(q, case, seed)

computed per (query, case, seed) from the profiling JSONs at **t1**, where
operator thread-seconds equal wall-seconds, so the additive model is exact.
Medians over 20 seeds, winners recomputed per k ∈ {1, 1.5, 2, 3, 5, 10}.

Baseline BF share of case-4 latency: **13.3% (JOB), 12.0% (TPC-H SF100),
10.6% (TPC-DS SF100)** — PT spends about an eighth of its runtime on the
filter machinery itself.

### 8.2 Results: k=2 barely dents PT; the cliff is at k≈5

Median-of-seeds winners per query and geomean speedup of case 4 over case 1:

| benchmark (t1) | k | wins c1/c2/c3/c4 | geo c2 vs c1 | geo c4 vs c1 |
|---|---|---|---|---|
| JOB (112 q) | 1 | 3/10/2/97 | 2.65 | 2.92 |
| | 2 | 4/17/6/85 | 2.42 | 2.57 |
| | 5 | 9/46/19/38 | 1.94 | 1.91 |
| | 10 | 38/42/22/10 | 1.50 | 1.37 |
| TPC-H SF100 (22 q) | 1 | 2/3/3/14 | 1.25 | 1.43 |
| | 2 | 3/4/3/12 | 1.18 | 1.26 |
| | 5 | 7/6/3/6 | 1.05 | **0.98** |
| | 10 | 10/4/3/5 | 0.91 | 0.75 |
| TPC-DS SF100 (97 q) | 1 | 19/15/12/51 | 1.41 | 1.58 |
| | 2 | 28/24/13/32 | 1.31 | 1.38 |
| | 5 | 42/31/10/14 | 1.10 | 1.03 |
| | 10 | 67/18/4/8 | 0.89 | 0.74 |

Per-query break-even (smallest k at which case 1 overtakes case 4, among
queries case 4 wins at k=1):

- JOB: 107/108 still win at k=2, 99 at k=5, 62 at k=10; median break-even **k=12.6**.
- TPC-H SF100: 18/19 at k=2, 12 at k=5, 8 at k=10; median break-even **k=4.8**.
- TPC-DS SF100: 64/76 at k=2, 46 at k=5, 18 at k=10; median break-even **k=5.9**.

### 8.3 Reading

1. **At the professor's suggested k=2, the answer is unambiguous: PT still
   pays.** Aggregate geomean advantages remain 2.6x/1.3x/1.4x
   (JOB/TPC-H/TPC-DS) and case 4 keeps ≥84% of its per-query wins.
2. **The break-even is k≈5 on the TPC benchmarks** (geomean crosses 1.0
   between k=5 and k=10) but far higher (k≈12) on JOB, where BFs prune so
   much that even a 10x filter cost is amortized. Filter-friendliness of the
   workload, not filter cost, is the first-order variable.
3. **The backward pass is the first casualty of rising BF cost.** Case 4
   carries more BF operators than case 2 (backward round), so as k grows,
   forward-only overtakes full RPT+: on TPC-H at k=5 the geomeans are 1.05
   (c2) vs 0.98 (c4); on TPC-DS 1.10 vs 1.03; win counts shift from c4 to c2
   in every benchmark. In a distributed setting the recommendation gradient
   "definitely forward, maybe backward" becomes stronger.
4. **The estimate is conservative against PT.** In a real distributed system,
   BFs also prune rows *before shuffles*, so their benefit — not just their
   cost — scales with the network; our model inflates only the cost side.
   Conversely, t64 numbers would dilute BF wall-time share further. A real
   distributed evaluation would likely sit between k=1.5 and k=3 for
   broadcast-style BF exchange on a rack-scale cluster.

---

## 9. Phase 7 — ASH-datagen: the THC-favorable design space, and what PT can(not) do there

Scripts: `scripts/analysis/phase7_ash_grid.sh` (benchmark settings) and
`phase7_ash_grid_bf.sh` (BF-enabled variant); data in
`results/analysis/phase7/{ash_grid.csv, ash_grid_bf.csv, profiling*/}`.

### 9.1 The generator and why it is THC's home turf

`ASH-datagen` builds `R(join_key) ⋈ S(join_key)` with a **pinned plan**
(`disabled_optimizers = 'join_order,build_side_probe_side,statistics_propagation'`;
S is always the build side). Two knobs matter here:

- `join_fraction_RS` = the **hot fraction** of the 4M/16M-row build side S
  (fraction of S rows that ever get probed);
- `probe_multiplicity_in_R`, set to ~1/hot_frac so R always has 4M (16M at
  scale 160k) rows — every R probe hits, each hot key is probed ~1/frac times.

All `selected_fraction_* = 1.00`: **the base tables carry no filters.** By
construction this is the workload THC was invented for: a huge cold hash
table with a small hot region probed over and over.

### 9.2 Grid results (threads=1, 5 runs, medians)

Under the corpus benchmark settings (which include
`skip_unfiltered_tables_graph_creation = true`), RPT+ creates **zero** Bloom
filters on this benchmark — there are no filters to transfer — so cases 1, 2
and 4 execute byte-identical plans (verified via profiling JSONs). Case 3
(THC) is the only differentiated case:

| hot frac | S rows | c1=c2=c4 | c3 (THC) | c3 vs c2 | THC telemetry |
|---|---|---|---|---|---|
| 0.02 | 16M | 2.80 s | **2.14 s** | **−24%** | 320 k inserts, active |
| 0.02 | 4M | 0.56 s | **0.48 s** | **−15%** | 80 k inserts, active |
| 0.05 | 4M | 0.61 s | **0.50 s** | **−17%** | 200 k inserts, active |
| 0.10 | 4M | 0.63 s | 0.59 s | −6% | 399 k inserts, active |
| 0.10 | 16M | 2.94 s | 3.26 s | +11% | frozen `THC-Full` @ 917 k |
| 0.25 | 4M | 0.65 s | 0.77 s | +18% | 867 k inserts, active (at capacity) |
| 0.50 | 4M | 0.65 s | 0.87 s | +34% | frozen `THC-Full` @ 917 k |
| 1.00 | 4M | 0.56 s | 0.81 s | +45% | frozen `THC-Full` @ 917 k |

The boundary is exactly the THC's capacity: the 36 MB budget yields 2^20
slots x 0.875 max load = 917,504 entries. Hot sets well under that (≤400 k)
win up to **24%**; hot sets at/over it lose **11–45%**. Even the biggest win
is bounded: THC still probes, matches, and copies — it only relocates where
probe hits land.

Why explicit beats implicit *here* (and not on TPC-H/JOB, §6): at hot frac
0.02 / S=16M, the hot rows are 320 k rows scattered uniformly across a
>0.5 GB hash table. Hardware caching works at 64-byte-line granularity on
two scattered structures (bucket array + row heap), so the implicit hot
working set is ~2 cache lines per hot key ≈ 40 MB — just over L3 — while the
THC packs the same keys at ~36 B/entry ≈ 12 MB, comfortably L3-resident.
The microbenchmark sits precisely in the narrow band where compaction
changes which side of the L3 boundary you are on. Real-workload joins almost
never do: either the hot set is small enough that implicit caching already
holds it, or too big for the THC as well (§6.2's `THC-Full` double-probe tax).

### 9.3 What happens if we force predicate transfer to participate

Two escalations, run at the corner points (medians of 5):

1. **Enable BF creation on unfiltered tables** (`skip_unfiltered_* = false`,
   runtime heuristics still on): `GiveUpBFCreation` observes selectivity
   > 0.2 after 32 chunks and abandons every BF. Runtimes identical to the
   baseline grid. The heuristic correctly refuses this workload — there is
   no *filter* information to transfer.
2. **Additionally disable the runtime give-up** (`drop_bf_at_runtime =
   false`): BFs are built from the raw join keys. Now, at hot 0.02 / S=16M:

| case | runtime | mechanism |
|---|---|---|
| c1 (= benchmark c2/c4) | 2.89 s | 16M-row HT, DRAM-resident probes |
| c3 THC | **2.14 s** | hot 320 k keys compacted into L3 |
| c4 RPT+ forced BFs | 2.23 s | R→S BF prunes S 16M→**320 k** before the build |
| c2 forward-only forced BFs | 4.49 s | S→R BF prunes nothing, pays 16M-row materialization |

   The full RPT+ run is the "scan + build HT" story from the meeting notes:
   the probe-side-derived BF removes the cold 98% of S *upstream*, so the
   scan output collapses, the hash table build shrinks 50x, and the probes
   hit an L3-resident table — the same locality THC buys, plus a cheaper
   build, minus THC's capacity cliff (at hot 0.10 / S=16M forced-BF c4 stays
   at par with c1, 3.00 vs 2.97 s, while THC is +11%). Its cost is the
   CreateBF materialization of both sides (~2 s of operator time at S=16M),
   which is why it only ties rather than beats THC at the sweet spot.
3. **Direction is everything.** Forward-only with forced BFs (c2, 4.49 s,
   +55% over baseline) is a pure loss: the forward pass at seed 0 sends
   S→R, a filter containing all 16M S keys that prunes zero R rows, and
   pays the materialization anyway. The pruning direction this benchmark
   needs (R→S, i.e. *into the build side*) is exactly what the backward
   pass provides. On this microbenchmark "definitely forward, maybe
   backward" inverts: the backward direction carries all the value.

### 9.4 Paper takeaways for the ASH-gen section

1. THC does exactly what it was designed to do **in the regime it was
   designed for**: ≤~½ of THC capacity hot set, high probe multiplicity,
   cold-dominated build side → up to 24% faster. The regime is narrow, its
   boundary is a hardware constant (L3 size), and crossing it flips the
   sign (+45% at the far end). An adaptive freeze/abandon policy is
   mandatory for deployment; the sweeps ran with it off (§6).
2. Predicate transfer, *as specified*, is inert here: no filters → no
   transfer → RPT+ ≡ DuckDB. Its runtime heuristics correctly decline to
   build filters from unfiltered tables.
3. If one generalizes PT from "transfer filter predicates" to "transfer
   join-key existence" (forcing BF creation), full RPT+ approximately
   matches THC's best case, avoids THC's capacity cliff — and needs the
   backward direction to do it. This reframes the backward pass not just as
   robustness insurance (§4) but as the only PT mechanism that can shrink a
   *build side* whose probe partner is unfiltered.

---

## 10. Phase 8 — Synthesis: the paper in one place

### 10.1 Suggested narrative arc

1. **Question.** Hash-join pipelines can be protected from bad plans in two
   ways: pick better plans (query optimization) or make plans matter less
   (predicate transfer). A third idea — explicitly caching the hot part of
   the build side (THC) — promises the backward pass's locality benefits
   without a second pass. Which of these mechanisms earn their keep, where,
   and why?
2. **Method.** Fix the engine; sweep 20 random-but-plausible left-deep join
   orders per query (the "fallible optimizer" model); run each plan under
   four executions (vanilla / forward BF / forward+THC / forward+backward);
   113+22+99+8 queries × 3 thread counts × up to 4 scale factors; cold page
   cache; per-run operator profiles. (§1)
3. **Headline results.** (§3) Full RPT+ geomean 2.8×/1.4×/1.65× over vanilla
   on JOB/TPC-H/TPC-DS at 1 thread; plan-risk (p90/p10 over seeds) compressed
   from 3.7× to 1.3× on JOB. Forward pass provides ~80% of both benefits.
4. **The decomposition.** (§4) A typical plan + RPT+ beats the *best* plan +
   vanilla (0.70–0.77 geomean) — PT is worth more than perfect join ordering
   in join-bound regimes. But the optimizer retains irreplaceable value:
   residual best-vs-typical is still 1.2× under RPT+, and §5.5/§7 show
   failure modes only the optimizer can avoid.
5. **Case studies.** (§5) The full zoo: vanilla-wins (native dynamic filters
   + sub-second queries), PT-rescues (17-series, Q37, Q82), strict 1>2>3>4
   (Q08), beyond-rescue (Q05, Q54), PT-loses (Q04@t64 serialization).
6. **THC autopsy + ASH-datagen.** (§6, §9) Explicit hot-set caching loses to
   implicit (hardware) caching on every real benchmark; on the synthetic
   workload built for it, it wins ≤24% inside a narrow, capacity-bounded
   regime and loses up to 45% outside it. The backward pass dominates it as
   a mechanism: it removes cold rows *before* the build instead of
   double-probing after.
7. **Costs and risks of PT.** (§3.5, §7, §8) Critical-path serialization at
   high parallelism on short queries; CreateBF materialization as an OOM
   tail risk (mitigated by give-up heuristics; the one real OOM we hit was
   optimizer-caused, not PT-caused); distributed BF-cost break-even at
   k≈5 (TPC) to k≈12 (JOB), with the backward pass the first casualty.
8. **Takeaways.** Definitely do the forward pass. The backward pass is worth
   +10% typical / large tail wins in join-bound single-node settings, but is
   the first thing to drop under distribution or high parallelism. Keep the
   optimizer: PT converts "bad plan" from catastrophe to nuisance, but only
   the optimizer prevents structural blowups (many-to-many keys, cross
   products), and its value is *all* that remains when execution is
   IO-bound. Skip explicit hot-set caching; hardware does it better.

### 10.2 Meeting-note checklist → where each item landed

| Note | Disposition |
|---|---|
| "THC doesn't work with many benchmarks" | §6.1–6.2: activates on 4–39% of joins; when active, taxes 1–14% aggregate; worst −49%; best ≤4%. §9: purpose-built regime wins ≤24%. |
| Deemphasize DuckDB-is-best (10b, 23b, TPC-H Q4/Q11/Q12/Q13/Q16) | §2.4: on the final sweeps DuckDB-best shrinks to 15/113 JOB, 1/22 TPC-H. §5.1 explains the mechanism (native dynamic filters + non-amortizable BF setup). Framed as observation, per JP. |
| Case study: best vanilla plan vs THC/RPT+ = value of QO | §5.5b fixed-best-plan table: PT wins on 5/7 even against the best plan; vanilla wins only sub-second dynamic-filter queries. |
| Dig into where time goes (profiling) | §3.5 operator attribution; §5 per-query forensics. |
| Q8 ordering 1>2>3>4 | §5.3, mechanism traced (scan pruning vs build shrinking). |
| TPC-DS OOM (Q54) | §7: reproduced exactly (101 GB RSS), PT-neutral, optimizer-avoidable; upstream-PR concern addressed via CreateBF materialization discussion. |
| RPT+/THC beats DuckDB, explain why (common case) | §3, §5.2/5.4. |
| Single- vs two-pass; implicit vs explicit hash | §6.3 narrative + §9.2 (the one regime where explicit compaction beats implicit caching, and why it's narrow). |
| Role of query optimizer | §4 decomposition + §5.5 + §7.3. |
| ASH-gen section: THC should win, but RPT+ still wins via scan/build-HT | §9.3: confirmed *only if* BFs are forced past their heuristics and only with the backward direction; nuance documented. |
| Forward+backward = robustness+performance; QO = extra performance | §4 findings 1–3. |
| Takeaway: definitely forward, maybe backward, keep QO | §10.1 item 8, with the distributed/parallelism caveats quantified. |
| Distributed estimate (×2 BF cost) | §8: at k=2 PT clearly still pays; break-even k≈5 (TPC) / k≈12 (JOB); backward pass degrades first. |

### 10.3 Threats to validity / honest caveats (paper section)

1. **Cold-cache methodology.** Every corpus measurement drops the OS page
   cache first (§2.3). This inflates scan cost, which (a) at t1 slightly
   *understates* PT's relative advantage on join CPU, and (b) at t64 makes
   TPC-H/DS IO-bound, driving the PT-collapse finding. Warm-cache reruns
   (§2.3) show identical plans and join timings; the t64 collapse would
   soften with warm caches. State both regimes.
2. **Seeded left-deep plan space** models optimizer error as uniform over
   orderings. Real optimizers err non-uniformly (usually near-optimal,
   occasionally catastrophic). Our "median seed" is harsher than a real
   optimizer's typical output; the min-over-seeds proxy for "best plan" is
   only over 20 left-deep samples (DPhyp's true optimum may be bushy —
   cf. Q54 §7.3 where dphyp is 1000× better than the best seed).
3. **THC ran non-adaptively** (all freeze/abandon checks off, 36 MB budget >
   32 MiB physical L3, threshold 1 M rows). Case 3 is therefore a *lower
   bound* on THC quality; §6.2/§9.2 suggest an ideal adaptive policy would
   converge to case 2 plus ≤4% (real benchmarks) / ≤24% (synthetic sweet
   spot).
4. **Timeout censoring** at 300 s: all ratio metrics cap failures at the
   timeout, understating PT's advantage wherever case 1 timed out (and
   symmetrically for all-case timeouts).
5. **2-decimal CSV precision** creates massive tie-plateaus at t64;
   we report strict wins and 5%-tie taxonomies to stay honest (§3.4).
6. **Single machine, single engine.** Arm Graviton2, NVMe, DuckDB 1.3-fork;
   the distributed estimate (§8) is a cost model, not a measurement.
7. **Spill policy differed across sweeps** (no-spill for JOB/TPC-H t1/t64
   sweeps vs spill-allowed for t8 and TPC-DS sweeps, §2.2) — within-sweep
   case comparisons are unaffected.
8. **Appian** is small (8 queries) and near-saturated by native execution
   (all cases ~1.0×); we report it for completeness, not claims.

### 10.4 Figure/table shortlist for the paper

1. Fig: win-rate stacked bars per benchmark × thread count (median winners)
   — `results/analysis/phase1/winrate_median.png`.
2. Fig: seed-spread (p90/p10) compression bars c1 vs c2 vs c4 (§3.3).
3. Fig: CDF of per-query med(c4)/best(c1) at t1 (§4) —
   `results/analysis/phase2/cdf_decomposition_t1.png`.
4. Table: fixed-best-plan 7-query study (§5.5b).
5. Fig: JOB 10b + TPC-DS Q37 per-seed runtime strips (4 cases × 20 seeds) —
   the two faces of PT in one figure.
6. Fig: operator-time attribution t1 vs t64 (§3.5).
7. Fig: ASH-datagen grid — c3/c2 ratio vs hot-set size with THC capacity
   marked (§9.2); optionally the forced-BF bars (§9.3).
8. Fig: distributed win counts / geomean vs k (§8.2).
9. Table: failure census (§7.4) + Q54 RSS table (§7.1).

### 10.5 One-paragraph abstract draft

Predicate transfer (semi-join reduction via Bloom filters) and query
optimization are usually studied separately; we study them as substitutes
and complements, on one engine, under a fallible-optimizer model that sweeps
20 plausible join orders per query. Across JOB, TPC-H, TPC-DS and a
synthetic hot/cold benchmark (4 execution modes × 3 thread counts × 4 scale
factors, 145 200 measured runs), the forward Bloom-filter pass alone converts
plan choice from a 3.7× risk into a 1.5× one and speeds typical plans by
1.25–2.5×; the backward pass adds ~10% and flattens the per-plan distribution
almost completely. A typical plan with full predicate transfer beats the
best sampled plan without it on 59–75% of join-bound queries — yet the
optimizer remains irreplaceable: only it avoids many-to-many and
cross-product blowups that no filter can prune (including a reproduced
101 GB OOM on a 3 GB database), and its value is all that survives when
execution turns IO-bound at high parallelism. We also autopsy an explicit
hot-set cache for hash joins (THC) and show that hardware implicit caching
dominates it outside a narrow, capacity-bounded synthetic regime — the
backward pass achieves the same locality goal more robustly by deleting cold
build rows upstream. Cost models suggest the forward pass survives even 5×
Bloom-filter cost inflation (distributed settings); the backward pass is the
first thing to drop. Practitioners should: always run a forward pass,
consider the backward pass for single-node join-bound workloads, and keep
their optimizer.

---

*Document complete: Phases 0–8. All scripts under `scripts/analysis/`; all
generated tables/figures under `results/analysis/phase*/`.*
