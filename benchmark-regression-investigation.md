# Benchmark Regression Investigation

This file tracks findings while reconciling the ASH benchmark swings between
`748762827b` and `9895b41ec0c45fc4d4df4c6f6b9c12fd59ee76f8`.

## Baseline Findings

- The old ASH result is reproducible on `748762827b` with the `ash/` harness:
  case 2 around 0.59s, case 3 around 0.54s, case 4 around 0.58s.
- The same `ash/` harness on `9895b41ec0` gives roughly 0.69s for cases 2-4,
  and case 1 is also slower. That means part of the regression is in the
  generic hash join path, not only in RPT+ or THC.
- `535336835c` introduced build-side `mu_s` estimation. It counts unique build
  keys during hash-table insertion with an atomic counter and computes
  `mu_s_build_estimate` before the THC-disabled early return. This affects
  all hash joins, including cases where THC is disabled.
- `scripts/measure/run_ash_datagen_release.sh` is not equivalent to
  `ash/launch_release.sh`. The scripts/measure path loads
  `scripts/measure/settings-common.sql`, which currently sets
  `skip_unfiltered_tables_graph_creation = false`. On the ASH RS query this
  makes case 4 much faster because the backward pass can filter S from 4M rows
  to about 400k rows before hash-table build, while cases 2 and 3 pay Bloom
  filter overhead without that backward benefit.

## Latest-Code Reproduction Attempts

All runs below used `9895b41ec0c45fc4d4df4c6f6b9c12fd59ee76f8` and a freshly
rebuilt `build/release/duckdb`.

### `ash/` Harness, Settings-Only Tests

The `ash/` harness keeps `skip_unfiltered_tables_graph_creation` at the compiled
default (`true`) and uses the older query file that disables join-order,
build-side/probe-side, and statistics-propagation optimizers.

| Scenario | Case 2 | Case 3 | Case 4 | Notes |
| --- | ---: | ---: | ---: | --- |
| baseline | 0.670s | 0.694s | 0.719s | Latest code does not reproduce old case-3-best behavior. |
| `thc_enable_first_cycle_check=false` | 0.710s | 0.686s | 0.730s | Makes case 3 best relatively, but still far from old ~0.55s. |
| `thc_mu_s_method='none'` | 0.726s | 0.699s | 0.714s | Does not remove the hot-path build counter in current code. |
| both settings above | 0.708s | 0.684s | 0.692s | Case 3 best-ish, still not old absolute runtime. |

Conclusion: `thc_enable_first_cycle_check` is not the main missing piece. It can
change the relative order, but it does not recover the old runtime.

### `scripts/measure` Transfer-Graph Tests

The `scripts/measure` harness loads `scripts/measure/settings-common.sql` and
`settings-run_ash_datagen.sql`.

| Scenario | Case 2 | Case 3 | Case 4 | Notes |
| --- | ---: | ---: | ---: | --- |
| `skip_unfiltered_tables_graph_creation=false` | 1.026s | 1.000s | 0.640s | Reproduces the slow case 2/3 and fast case 4 split. |
| `skip_unfiltered_tables_graph_creation=true` | 0.715s | 0.720s | 0.712s | Removes the case 4 advantage and returns all cases to ~0.71s. |
| `skip_unfiltered_tables_graph_creation=true`, `thc_enable_first_cycle_check=false` | 0.697s | 0.699s | 0.715s | Similar; first-cycle check is secondary here. |

Conclusion: the `scripts/measure` anomaly is caused by
`skip_unfiltered_tables_graph_creation=false`, which enables a backward Bloom
filter in case 4 that filters S from 4M rows to about 400k rows before the hash
build. Cases 2 and 3 do not get that backward filtering benefit.

### Experimental Source-Gated Tests

To test whether remaining latest-code overhead is in code rather than settings,
I temporarily changed the source in two ways and then restored it:

1. Made `thc_mu_s_method='none'` actually skip the build-side unique-key counter
   in the hash-table insertion hot path. Current source still calls
   `CountOneUniqueBuildKey()` for successful first insertions regardless of the
   setting.
2. Suppressed per-chunk `HashJoinGlobalSinkState::EmitProbeTiming(context)`
   calls from `PhysicalHashJoin::ExecuteInternal`, keeping the final operator
   emit. Current source emits THC/hash-join telemetry on every hash-join chunk.

Results with those experimental gates and `thc_mu_s_method='none'`:

| Scenario | Case 1 | Case 2 | Case 3 | Case 4 |
| --- | ---: | ---: | ---: | ---: |
| gated build-count only | 0.631s | 0.651s | 0.625s | 0.637s |
| gated build-count + no per-chunk telemetry emit | 0.567s | 0.556s | 0.538s | 0.562s |

This reproduces the oldest qualitative and absolute result on the latest code:
case 3 is best, and runtimes are back in the ~0.54-0.56s range.

Current best explanation:

- `thc_enable_first_cycle_check` changes THC behavior but is not the main
  regression.
- The always-on build-side `mu_s` counter adds generic hash-join overhead.
- The per-chunk `EmitProbeTiming` telemetry is a larger remaining overhead in
  this small single-threaded benchmark.
- `skip_unfiltered_tables_graph_creation=false` explains the separate
  `scripts/measure` case-4-is-much-faster result.

## Reproduction Changes Applied

Applied on `9895b41ec0c45fc4d4df4c6f6b9c12fd59ee76f8`:

- `src/execution/join_hashtable.cpp` and
  `src/include/duckdb/execution/join_hashtable.hpp`: gate the build-side
  unique-key counter behind `thc_mu_s_method in ('build_count', 'all')`. This
  keeps parallel collision handling intact, but avoids the atomic increment
  when `thc_mu_s_method='none'`.
- `src/execution/operator/join/physical_hash_join.cpp`: remove per-chunk
  `EmitProbeTiming(context)` calls from `ExecuteInternal`. The final
  `HashJoinOperatorState::Finalize` emit remains, so profiling extra info is
  still populated once per operator.
- `ash/settings.sql`: set `thc_mu_s_method = 'none'` for the ASH reproduction.

Rebuild command:

```bash
TMPDIR=/mnt/local_ssd/tmp GEN=ninja BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 BUILD_HTTPFS=1 CORE_EXTENSIONS='tpch' make release -j $(nproc)
```

Verification using a fresh `ash` DB and explicit case overrides:

| Case | Average | Median | Min |
| --- | ---: | ---: | ---: |
| 1 | 0.559s | 0.552s | 0.549s |
| 2 | 0.551s | 0.551s | 0.548s |
| 3 | 0.546s | 0.537s | 0.535s |
| 4 | 0.560s | 0.558s | 0.547s |

This is in line with the earlier reproduction target
`0.567s / 0.556s / 0.538s / 0.562s`, with case 3 best by both average and
median.

## Script-Path ASH Reproduction

To make the same regime available through:

```bash
scripts/measure/run_ash_datagen_release.sh --cases 2,3,4 --query rs
```

the ASH-specific script settings were moved into
`scripts/measure/settings-run_ash_datagen.sql`:

- `disabled_optimizers = 'join_order,build_side_probe_side,statistics_propagation'`
- `enable_hash_join_timers = false`
- `thc_mu_s_method = 'none'`

The script path keeps the common 100k/100k THC phase cadence from
`scripts/measure/settings-common.sql`, which was faster for case 3 than the
1M/1M `ash/settings.sql` phase cadence in this harness. The common settings also
keep `skip_unfiltered_tables_graph_creation = true`, avoiding the case-4-only
backward-filter advantage.

Verification with the exact command:

| Case | Runtime |
| --- | ---: |
| 2 | 0.543s |
| 3 | 0.542s |
| 4 | 0.544s |

A 5-run sample stayed in the same range. Case 3 had the best median, though a
couple of outliers made averages very close:

| Case | Runs | Median |
| --- | --- | ---: |
| 2 | 0.550, 0.551, 0.552, 0.588, 0.554 | 0.552s |
| 3 | 0.544, 0.542, 0.550, 0.577, 0.599 | 0.550s |
| 4 | 0.558, 0.558, 0.561, 0.566, 0.563 | 0.561s |
