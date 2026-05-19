# THC Session Handoff (2026-05-13 → 2026-05-19)

A multi-day session on `mp/optimization` focused on closing the TieredHashCache
(THC) regression gap that the regime doc identified. This file is a
context-free starting point for the next session.

---

## Where you are

- **Branch:** `mp/optimization` at `42c425fcac` (10+ commits since the prior tip
  `c1029678f3`).
- **Parent branch:** `hl/any_root` (Hugo's), itself off `main` (DuckDB v1.3.0
  fork). See `CLAUDE.md` for the authoritative map of the codebase.
- **What got done:** six performance interventions in the THC + supporting
  measurement infrastructure. All committed and pushed.
- **What's open:** clear remaining ceiling (regime-doc-bounded) plus one
  specific category of TPC-DS regressions that the gates don't catch.

---

## Quick context (read CLAUDE.md first if unfamiliar)

This fork implements two ideas on top of DuckDB v1.3.0:

1. **Robust Predicate Transfer Plus (RPT+):** forward + backward Bloom-filter
   passes that shrink join inputs before HT build. Lives in
   `src/optimizer/predicate_transfer/`.
2. **TieredHashCache (THC):** a small (~L3-sized) tagged probe-acceleration
   cache layered on top of `JoinHashTable`. Adaptive: BASELINE / COLLECT /
   READ_ONLY phases with cost-based DROP/FREEZE/CONTINUE per thread. Core in
   `src/include/duckdb/execution/tiered_hash_cache.hpp` and
   `src/execution/join_hashtable.cpp`.

Benchmarks (the primary feedback loop, not unit tests) are driven by
`scripts/measure/run_job.sh` (JOB), `run_tpc.sh` (TPC-H/DS) and the
matrix wrappers added this session.

---

## Commits landed this session (chronological)

```
b16aab439a  run_job.sh: add --passes N flag for multi-pass JOB sweeps
f55807d864  add overnight benchmark matrix wrapper + analysis script
e8acd81348  run_overnight_matrix.sh: trim matrix and add thc_disabled config
d915d8c3a0  THC: bundle pre-existing infra + probe-floor + hot-fraction
            + adaptive budget + cascade  (the big bundle commit)
af1c1dd2e1  THC: defer allocation until first probe crosses thc_collect_phase_rows
1aabace9f6  THC: move decision-log no-THC attribution from first-chunk to emit time
f9a8451844  THC: size cache capacity from observed probe rate at lazy-alloc time
d73733eaa7  run_tpc.sh: add --passes --cases --csv; new TPC-H matrix wrapper
1508b22102  add TPC-DS sf10 overnight matrix wrapper
b527b20659  run_tpc.sh: fix missing CSV header on --tpcds-only + --csv override
aa0fffc6cb  THC: mid-COLLECT early abandon + fast-bypass when never-eligible
cd199e5563  THC: mid-READ_ONLY early abandon + perc_hot clamp
42c425fcac  matrix wrappers: add collect_20k config variant
```

The big bundle (`d915d8c3a0`) collapses what would have been many small commits
into one because they shared the same files. The plan/spec/test docs that
guided it live at `docs/2026-05-14-thc-probe-side-row-floor*.md` (gitignored,
so local-only — see the corresponding commit message for the design).

---

## What the new features do

### Build-time gates (run at `InitializeTieredHashCache` / Finalize time)

| gate | constant / setting | what it catches |
|---|---|---|
| `disable_tiered_hash_cache` | config flag | THC off entirely |
| Build-source-base-table (`PhysicalSubtreeContainsJoin`) | n/a | THC off when build side is itself a join (pre-existing from Hugo) |
| **Probe-side row floor** | `estimated_probe_side_rows < 2 × thc_collect_phase_rows` | small joins where THC can't amortize warmup |
| **µ_SR upper bound** | `estimated_probe_side_rows / build_unique_keys < thc_min_estimated_mu_s_to_r` | low-multiplicity joins (gated on `thc_mu_s_method = build_count`) |
| **Build-time hot-fraction** | `min(1, thc_collect_phase_rows / build_unique_keys) > thc_max_estimated_perc_hot` | builds with no exploitable skew |
| `thc_activation_threshold` | HT capacity < threshold | tiny builds (existing) |
| Coverage ratio | `thc_size < thc_size_needed × thc_min_coverage_of_build_side` | THC too small for build |
| **Build-size-adaptive budget** | `THC_BUILD_OVERSIZE_FACTOR (=4) × build_size_bytes` clamp on capacity | over-sized cache on small builds |

### Runtime mechanisms (in `GetRowPointers`)

| mechanism | trigger | what it does |
|---|---|---|
| **Fast-bypass for never-eligible HTs** | `!tiered_hash_cache && !thc_deferred_allocation_eligible` at top of GetRowPointers | skips cascade load + deferred-eligibility accumulator entirely |
| **Shared abandon-cascade** | atomic `thc_globally_abandoned` set by any abandonment site | peer threads short-circuit within one chunk |
| **Deferred THC allocation** | `state.thc_pre_trigger_rows >= thc_collect_phase_rows` | calloc + BASELINE bookkeeping deferred until first probe sees enough rows; post-BF small probes never trigger |
| **Probe-rate capacity sizing** | at lazy-alloc time | shrink capacity to `min(thc_capacity, ComputeCapacity(row_size, post_trigger_probe × THC_PROBE_COVERAGE_FRACTION × stride))`. Default coverage = 0.10 |
| **Mid-COLLECT early abandon** | `probe_rows_in_phase >= MID_COLLECT_CHECKPOINT_ROWS (=5000)` and match_rate < 0.05 | catches 100%-miss-rate joins ~55k probes earlier per thread |
| **Mid-READ_ONLY early abandon** | `read_only_rows_processed >= MID_READ_ONLY_CHECKPOINT_ROWS (=2000)` and miss_rate ≥ 1.0 | catches "cache filled but useless" joins ~8k probes earlier per thread |
| Cycle-1 abandonment (pre-existing + tweaked) | end of cycle 1 evaluation | low-µ, high-hotness, small-capacity heuristics |
| Cost-rule DROP/FREEZE (pre-existing) | every checkpoint after warmup | running average c_grow vs c_main |
| **perc_hot clamp** | clamp to `[0, 1]` | fixes >1.0 estimator anomaly seen on TPC-H Q09 |

### Decision-log instrumentation

`SET thc_emit_decision_log = true` emits one `[THC_DECISION]` CSV row per
ProbeState at teardown. Format documented in `EmitDecisionLogRow` in
`src/execution/join_hashtable.cpp`. Reason labels:

```
never_activated, no_thc_at_join, no_thc_below_probe_floor,
no_thc_high_hotness_buildtime, no_thc_deferred_never_triggered,
kept, frozen, abandoned_low_mu, abandoned_high_hotness,
abandoned_small_capacity, abandoned_high_miss, abandoned_cascade,
dropped_by_cost
```

`SET thc_log_mu_s = true` enables verbose mu_s logging.

`SET thc_pointer_mode_min_row_size = N` switches THC entries from
`[tag | row_copy]` to `[tag | data_ptr]` for builds with row size ≥ N.
**Empirically regresses on all measured workloads. Default sentinel-off.
Consider dropping the feature in a future cleanup.**

---

## Measurement infrastructure

### Multi-pass benchmark wrappers

All under `scripts/measure/`. Standard usage:

```bash
PASSES=3 SEEDS=5 CASES=2,3 scripts/measure/run_overnight_matrix.sh         # JOB
PASSES=3 CASES=2,3 SF=10 scripts/measure/run_overnight_matrix_tpch.sh      # TPC-H sf10
PASSES=3 CASES=2,3 SF=10 scripts/measure/run_overnight_matrix_tpcds.sh     # TPC-DS sf10
```

Each writes to `job_results/overnight_<...>_<timestamp>/` with one CSV per
config plus the `.sql` actually used. CSV schema is
`query,case,seed,pass,runtime_seconds` (JOB) or
`query,case,pass,runtime_seconds` (TPC-H/DS).

The matrix runs 8 configs by default:

| config | override |
|---|---|
| `baseline` | (defaults) |
| `ptr_64` / `ptr_96` / `ptr_128` | `thc_pointer_mode_min_row_size = N` |
| `no_probe_floor` | `thc_collect_phase_rows = 1` (gate vacuous) |
| `no_hot_fraction` | `thc_max_estimated_perc_hot = 1.0` (gate never fires) |
| `thc_disabled` | `disable_tiered_hash_cache = true` |
| `collect_20k` | `thc_collect_phase_rows = 20000` (speculative) |

### Analysis

```bash
python3 scripts/measure/analyze_overnight.py job_results/overnight_<...>/
```

Reports per-config c3 − c2 with mean / median / CI95, plus pairwise deltas
vs baseline. Schema-agnostic — handles both JOB and TPC-H/DS output.

### Decision-log harvest

```bash
SF=10 scripts/measure/harvest_decision_log_tpch.sh
```

Runs each of 8 known TPC-H regressors (Q01/04/09/12/13/17/18/21) once with
`thc_emit_decision_log = true`. Writes per-query CSVs and a `summary.txt`
with the reason distribution + key per-row stats. Extend this script if you
want to harvest other workloads.

### TPC-DS sf10 database

Generated and on disk at `../benchmark_data/tpcds/tpcds_sf10.duckdb` (2.8 GB).
Reusable. To regenerate or scale up:

```bash
./build/release/duckdb ../benchmark_data/tpcds/tpcds_sfN.duckdb \
    -c "LOAD tpcds; CALL dsdgen(sf = N);"
```

The DuckDB extension repo doesn't have a `tpcds.duckdb_extension.gz` for this
fork's version — `INSTALL tpcds` fails with 404. Use `LOAD tpcds` only
(the extension is built statically via `BUILD_TPCDS=1`).

---

## Headline measurements (multi-pass, c3 − c2 mean per query)

JOB-engaged sweep at `c1029678f3` (pre-session, single-pass): **+4.9 ms per
query** (regression).

After all this session's work (commits up through `cd199e5563`):

| workload | baseline c3 − c2 | THC contribution (baseline c3 vs thc_disabled c3) |
|---|---:|---:|
| JOB-engaged (n=1695, 3 passes) | **−8.7 ms** median | ~5 ms/query saved by THC |
| TPC-H sf10 (n=110, 5 passes) | **−5.7 ms** mean (CI wide) | ~5-9 ms/query saved |
| TPC-DS sf10 (n=495, 5 passes) | **−2.7 ms** mean | within noise of zero |

Net session shift on JOB: **+4.9 ms regression → −8.7 ms median win** =
~13.6 ms per query swing.

JOB regression count: **0 of 113 queries** (all queries either help or are neutral).
TPC-H regression count was **8 of 22**; after the mid-COLLECT + mid-READ_ONLY
checkpoints, the worst regressor (Q18) went from +174 ms to +30 ms in a
small-N wall-clock check. A full re-measurement of TPC-H against the latest
binary wasn't run in this session — that's a candidate first task next time.

### Things confirmed across three workloads

1. **`pointer_mode` regresses everywhere.** JOB, TPC-H, TPC-DS all show
   `ptr_64/96/128` worse than baseline. Threshold gradient on TPC-DS confirms
   direction: more aggressive (lower threshold) = more regression. Recommend
   leaving the default at sentinel-off and considering removing the feature
   in a future cleanup.

2. **`collect_20k` (shortened COLLECT) regresses baseline by +12 ms on JOB.**
   With mid-COLLECT catching the bad cases at 5k, you'd think shortening the
   default from 50k would only cut overhead. It doesn't — the full COLLECT
   window helps the *kept* THC's cache fill quality. **Recommend keeping
   default at 50k.**

3. **All other gates earn their keep on at least one workload:**
   - probe-floor: +24 ms regression when disabled on JOB; +807 ms (one outlier
     query Q54) on TPC-DS.
   - hot-fraction: +562 ms regression on JOB (one outlier Q25b at 16 min).
   - cascade / deferred-allocation / probe-rate sizing: harder to ablate
     individually but in-aggregate baseline-vs-disabled shows ~5-10 ms/query
     saved.

---

## Open opportunities (in priority order)

The regime-doc finding holds: **backward RPT+ does the bulk of case-3's work;
THC is bounded by what backward already shrunk.** The remaining levers are
small (~few ms / query) and need careful measurement to claim.

### 1. Re-measure TPC-H + TPC-DS against the latest binary

Tonight's matrix only ran JOB. The mid-COLLECT + mid-READ_ONLY changes were
verified on TPC-H Q04 in a smoke test (5 passes) but haven't been measured at
matrix scale. Probable improvements live there. Time: ~5 hours for both.

```bash
PASSES=3 SEEDS=5 CASES=2,3 scripts/measure/run_overnight_matrix.sh        # 2.5-3h
PASSES=3 CASES=2,3 SF=10 scripts/measure/run_overnight_matrix_tpch.sh     # ~1h
PASSES=3 CASES=2,3 SF=10 scripts/measure/run_overnight_matrix_tpcds.sh    # ~4h
```

### 2. Decision-log harvest on the 8 TPC-H queries again

After Items 1+2+A+B landed, the previous failure modes may have shifted. Run
`harvest_decision_log_tpch.sh` against the latest binary and see whether the
4 abandoning queries now go through the mid-phase paths cleanly, and whether
the 4 non-instantiating queries (Q01/12/13/17) still regress.

### 3. Investigate Q12/Q13/Q17 regressions

These showed c3 > c2 in the previous TPC-H run despite the THC never being
instantiated. The Item 2 fast-bypass should have addressed the per-chunk
overhead component. If the regression persists, look at:

- Finalize-time gate-evaluation overhead (the gates themselves run on every
  HT build, including those that veto).
- Other case-3-vs-case-2 plan differences not related to the THC.

### 4. Approach (b) for capacity sizing

The current probe-rate-sized capacity (commit `f9a8451844`) uses *rate* as the
sole signal. Approach (b) was the deferred alternative: build a small
distribution sketch during the watch phase (rows seen pre-allocation) and
decide allocation + sizing from the sketch's skew, not just from total volume.
Bigger lift; could shift the needle on workloads where post-BF probe is
uniform-distributed (THC's bad case).

### 5. TPC-DS sf100 or larger

sf10 may be too small for THC's amortization to show. Per-query runtimes
average ~3-5 s; the per-probe overhead is small relative. At sf100 (queries
10× longer) the picture might differ. Data generation is hours though.

### 6. Cleanup: drop `pointer_mode` if no workload exercises it

After three independent workload sweeps showing regression, the feature has
no demonstrated use case. Removing it would shrink the binary, simplify
`tiered_hash_cache.hpp`, and eliminate a config knob. Risk: low. Reward:
maintenance burden reduction.

### 7. `thc_emit_decision_log` per-thread spurious rows

Multiple `[THC_DECISION]` rows can appear for the same JoinHashTable in a
single-threaded run (one per ProbeState destruct event). The
`decision_log_emitted` latch handles per-ProbeState idempotency, but multiple
ProbeStates per join can each emit. Cosmetic — affects log readability, not
runtime. Fix would be to track emission at the JoinHashTable level instead of
per-ProbeState.

---

## Things deliberately not pursued this session

- **Drop pointer_mode from the binary.** Sufficient evidence for the call,
  not done because it's bigger surgery and the user wanted forward progress
  on the perf work.
- **TPC-DS regime doc update.** Single-pass historical baselines remain in
  `docs/thc_regime_analysis.md`. Tonight's multi-pass numbers supersede them
  qualitatively but the doc wasn't rewritten.
- **Approach (b) for capacity sizing** (see #4 above).
- **A workload designed to exhibit THC's design strengths** (small bounded
  build, large repeated skewed probe, no BF reduction available) — would
  demonstrate the feature's value cleanly but doesn't help on JOB/TPC-H/TPC-DS.

---

## File / commit map

| topic | file(s) | commit(s) |
|---|---|---|
| Probe-floor + hot-fraction + adaptive budget + cascade | `src/execution/join_hashtable.cpp`, `src/include/duckdb/execution/join_hashtable.hpp` | `d915d8c3a0` |
| Deferred allocation | same | `af1c1dd2e1` |
| Decision-log emit-time labeling | same | `1aabace9f6` |
| Probe-rate capacity sizing | `src/execution/join_hashtable.cpp` | `f9a8451844` |
| Mid-COLLECT abandon + fast-bypass | same | `aa0fffc6cb` |
| Mid-READ_ONLY abandon + perc_hot clamp | same | `cd199e5563` |
| Multi-pass JOB harness | `scripts/measure/run_job.sh` | `b16aab439a` |
| Multi-pass TPC-H/DS harness | `scripts/measure/run_tpc.sh` | `d73733eaa7`, `b527b20659` |
| Matrix wrappers | `scripts/measure/run_overnight_matrix*.sh` | `f55807d864`, `e8acd81348`, `1508b22102`, `42c425fcac` |
| Analysis script | `scripts/measure/analyze_overnight.py` | `f55807d864` |
| Decision-log harvest script | `scripts/measure/harvest_decision_log_tpch.sh` (untracked — committed in next session? recheck) | n/a |

`docs/` is `.gitignored` on this repo, so design specs and per-feature plans
written during the session aren't tracked. Notable un-tracked-but-existing
docs in the working tree:

- `docs/2026-05-14-thc-probe-side-row-floor.md` — spec for Tasks 1-5
- `docs/2026-05-14-thc-probe-side-row-floor-plan.md` — implementation plan

If the next session wants those tracked, add them via `git add -f` or remove
the `docs/` line from `.gitignore`.

---

## Conventions worth knowing (from CLAUDE.md and the session)

- **Never run destructive git commands without explicit user confirmation**
  (committed in `.cursorrules`). Commit messages on this branch carry
  `Co-Authored-By: Claude` and `Signed-off-by: mprammer`.
- **Code-comment density.** This repo's `.cursor/rules/project-details.mdc`
  requires verbose WHY comments. Don't strip them.
- **Setting changes go in `settings-common*.sql` / `settings-run_*.sql`**, not
  per-query files. The matrix wrappers append config-specific SET overrides
  after sourcing `settings-common-engaged.sql`.
- **Tabs for indent, spaces for alignment, 120 col max, clang-format 11.0.1**
  (DuckDB upstream convention from `CONTRIBUTING.md`).
- **`idx_t` for offsets/counts, not `size_t`.** `D_ASSERT` for invariants.
  Use `MinValue<T>` / `MaxValue<T>` (DuckDB repo standard) over `std::min` /
  `std::max` when in `namespace duckdb` code.
- The **build incantation** for benchmarking is:
  ```bash
  GEN=ninja BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 BUILD_HTTPFS=1 \
      CORE_EXTENSIONS='tpch' make release -j $(nproc)
  ```
  Output binary at `./build/release/duckdb` and unittest at
  `./build/release/test/unittest`.

---

## How to verify the current binary

```bash
# 1. Confirm you're on the right commit
git log -1 --format='%H %s'           # expect 42c425fcac

# 2. Build release (incremental if build/release exists)
GEN=ninja BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 BUILD_HTTPFS=1 \
    CORE_EXTENSIONS='tpch' make release -j $(sysctl -n hw.ncpu)

# 3. Run the THC sqllogictest
./build/release/test/unittest "test/sql/optimizer/thc_probe_floor.test"
# expect "All tests passed (10 assertions in 1 test case)"

# 4. Smoke test the early-abandon paths on TPC-H Q04 (requires
#    ../benchmark_data/tpch/tpch_sf10.duckdb)
./build/release/duckdb ../benchmark_data/tpch/tpch_sf10.duckdb -c "
SET threads = 1; SET disable_perfect_hashing = true;
SET thc_activation_threshold = 10000; SET thc_collect_phase_rows = 50000;
SET rpt_forward_only = true; SET thc_emit_decision_log = true;
LOAD tpch; PRAGMA tpch(4);
" 2>&1 | grep '^\[THC_DECISION\]' | awk -F, '{print $3, "probe_rows=" $6}'
# expect two abandoned_high_miss rows: one at ~5199 (mid-COLLECT), one at
# ~54005 (mid-READ_ONLY)
```

If those three checks pass, the binary matches the documented behaviour.

---

## Pointers to prior work referenced during this session

- `docs/thc_regime_analysis.md` (gitignored) — Hugo's prior finding: THC is
  approximately neutral on JOB / TPC-H sf10.
- `optimization-pipeline.md` (gitignored) and `top_down_tour.md` (gitignored) —
  deep design notes for the RPT+ optimizer pipeline.
- `CLAUDE.md` — authoritative codebase map.

---

## Memory entries (claude-side, persistent across sessions)

Saved under `~/.claude/projects/-Users-martin-github-spy/memory/`:

- `project_spy_overview.md` — repo high-level summary.
- `project_branch_topology.md` — `main → hl/any_root → mp/optimization`.
- `project_thc_regime.md` — Hugo's regime-doc finding.
- `project_thc_2026_05_session.md` — session-spanning summary (this work).
- `feedback_git_policy.md` — no-commit-without-confirmation rule.
- `feedback_doc_conventions.md` — comment density convention.
- `reference_settings_files.md` — settings-common*.sql conventions.
