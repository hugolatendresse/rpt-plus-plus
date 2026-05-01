# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this fork is

This is a hard fork of `embryo-labs/dynamic-predicate-transfer` (which is itself a fork of DuckDB v1.3.0) that implements **Robust Predicate Transfer Plus (RPT+)** plus a custom **TieredHashCache (THC)** layered on top of `JoinHashTable`. The paper is *Robust Predicate Transfer with Dynamic Execution* (PVLDB 2026); a copy of the fork-specific design notes lives in `optimization-pipeline.md` and `top_down_tour.md` in the repo root.

The two ideas to keep in mind when navigating the code:

- **RPT+ forward + backward Bloom-filter passes** are implemented as new logical/physical operators (`LogicalCreateBF`, `LogicalUseBF`, `PhysicalCreateBF`, `PhysicalUseBF`) and a transfer-graph optimizer pass that runs before AND after join-order optimization (see `PredicateTransferOptimizer::PreOptimize` / `Optimize` in `src/optimizer/predicate_transfer/`).
- **TieredHashCache (THC)** caches the hot rows of the build-side hash table into a small (~L3-sized) tagged probe-acceleration table. It tries to adapt at runtime, periodically deciding whether to keep collecting, freeze, or abandon. See `src/include/duckdb/execution/tiered_hash_cache.hpp` and `src/execution/join_hashtable.cpp`.

## Build commands

The standard release build for benchmarking work in this repo is:

```bash
GEN=ninja BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 BUILD_HTTPFS=1 CORE_EXTENSIONS='tpch' make release -j $(nproc)
```

The release binary is at `./build/release/duckdb`. Most measurement scripts look there by default.

Other useful targets (defined in `Makefile`):

- `make debug` — debug build (sanitizers enabled by default; pass `DISABLE_SANITIZER=1` to drop them).


## Benchmark / measurement scripts

The project's primary feedback loop is benchmark runs, not unit tests. The drivers live in `scripts/measure/`:

- `scripts/measure/run_job.sh` — Join Order Benchmark. Cases 1–4 toggle RPT/THC combinations:
  - `1`: vanilla DuckDB (`disable_rpt`, `disable_tiered_hash_cache`)
  - `2`: forward-only RPT+ (`rpt_forward_only`, `disable_tiered_hash_cache`)
  - `3`: forward-only RPT+ with THC (`rpt_forward_only`)
  - `4`: full forward+backward RPT+ without THC (`disable_tiered_hash_cache`)

  Supports `--cases`, `--seeds N` (sweeps `transfer_graph_seed = 0..N-1`), `--job-query 10a`, `--perf`, `--debug`, `--duckdb-profiling`. Sweep mode auto-writes a CSV under `job_results/`.

- `scripts/measure/run_tpc.sh` — TPC-H / TPC-DS at a chosen scale factor (`--sf 100` default). Same `--case` semantics. `--generate` regenerates the benchmark `.duckdb` file under `../benchmark_data/`.

- `scripts/measure/settings-common.sql` and `settings-run_*.sql` are the canonical PRAGMA / SET preludes the run scripts splice into each query. **Edit these — not the per-query files — when changing default knob values for benchmarking.**

The JOB DB is expected at `join-order-benchmark/job.db`; the TPC dbs at `../benchmark_data/tpch/tpch_sf<SF>.duckdb` and `../benchmark_data/tpcds/tpcds_sf<SF>.duckdb`.

The DuckDB built-in benchmark runner is also available (see `README.md` for the exact `BUILD_BENCHMARK=1 ...` invocation).

## Architecture: where the RPT+/THC machinery lives

The optimizer pipeline is documented in `optimization-pipeline.md`. The high-level summary:

- **Pre-join-order**: `PredicateTransferOptimizer::PreOptimize` walks the logical plan, builds the transfer graph (`TransferGraphManager`), picks a root (largest filtered table by default, or seed-driven if `use_seeded_root=true`), and produces a transfer order. Edge information is extracted before join-order optimization because some join conditions are erased by it.
- **Post-join-order**: `PredicateTransferOptimizer::Optimize` materializes `LogicalCreateBF` and `LogicalUseBF` operators per the transfer plan. `CreateBloomFilterPlan` decides per-table BF creation and consumption.
- **Physical plan**: `transfer_bf_linker` (`src/execution/transfer_bf_linker.cpp`) wires `PhysicalCreateBF` outputs to `PhysicalUseBF` inputs, including across pipeline boundaries (uses `weak_ptr` for the cross-pipeline reference — see commit `d41318c0f0`).
- **Runtime**: `PhysicalCreateBF::GiveUpBFCreation` can abandon a planned BF based on selectivity / memory pressure (gated by `drop_bf_at_runtime`). The THC machinery sits inside `JoinHashTable::ProbeAndBuild` paths in `src/execution/join_hashtable.cpp`.

Key knobs (all are `SET name = value;` PRAGMAs; defaults and full doc strings are in `src/include/duckdb/main/client_config.hpp`):

- RPT+ control: `disable_rpt`, `rpt_forward_only`, `drop_bf_at_runtime`, `skip_unfiltered_tables_create_bf_plan`, `skip_unfiltered_tables_graph_creation`, `allow_build_probe_side_swap`.
- Transfer-graph determinism: `use_seeded_root`, `use_seeded_transfer_order`, `transfer_graph_seed`, `join_order_mode = 'seeded_left_deep' | 'dphyp' | ...`.
- THC control: `disable_tiered_hash_cache`, `thc_l3_budget`, `thc_activation_threshold`, `thc_collect_phase_rows`, `thc_first_read_only_phase_rows`, `thc_collect_budget_fraction`, `thc_miss_below_which_skip_collect`, `thc_max_load_factor`, `thc_mu_s_method`, `thc_warmup_cycles`, `thc_min_estimated_mu_s_to_r`, `thc_max_estimated_perc_hot`, `thc_min_coverage_of_build_side`.

When adding or changing a setting, update both `src/include/duckdb/main/client_config.hpp` (the field) and `src/include/duckdb/main/settings.hpp` (the named setting struct) — they are kept in sync manually.

## Project conventions (from `.cursor/rules/project-details.mdc`)

- **Document changes thoroughly** in code comments. Make sure both *what* and *why* are clear.
- Do **not** delete existing comments that remain true.
- Update existing comments only when logic changes have made them false.
- Keep a running report of the optimizations you try / keep and their effect on average runtime.

## Git policy (from `.cursorrules`)

Never run `git commit`, `git push`, `git add`, or any destructive git command without explicit user confirmation. Halt and ask first.

## DuckDB upstream conventions worth knowing

(Full list in `CONTRIBUTING.md`; the points that bite most often:)

- Tabs for indentation, spaces for alignment. 120-column max. clang-format **11.0.1** (pinned).
- Use `idx_t` for offsets/indices/counts; never `size_t`.
- Use `D_ASSERT` for invariants; user input must not be able to trigger them.
- Prefer `unique_ptr` over `shared_ptr`. Never `using namespace`. All core code lives in `namespace duckdb`.
- Tests are sqllogictest (`.test` / `.test_slow`) under `test/sql/`. Write C++ tests only when you genuinely need to (e.g. concurrency).
