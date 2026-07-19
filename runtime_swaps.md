# Phase D — Runtime build/probe swap: research findings (NOT to be implemented)

This section is the standalone findings document the user requested. It is **research, not a plan**: it lays out what the codebase contains today, what a runtime swap *would* mean at each lifecycle point, the design space, the tradeoffs, and the questions left for the user to answer before any implementation work begins.

## D.1 — What the codebase already has (negative result)

Exhaustive grep over `src/`, `src/execution/operator/join/`, and the optimizer for: `swap`, `flip`, `SwapChildren`, `FlipBuildProbe`, `ReverseSides`, `right_semi`, `right_anti`, `delim_flipped`, `runtime_swap`, `swap_join_sides`, `swap_build_and_probe`, `enable_runtime`, perfect-hash. Findings:

| Mechanism | Location | When it runs | Status | Relevant to runtime swap? |
|---|---|---|---|---|
| `BuildProbeSideOptimizer::TryFlipJoinChildren` | `src/optimizer/build_probe_side_optimizer.cpp:159-221` | optimizer time, after column-lifetime analysis | active, gated by `allow_build_probe_side_swap` | No — it's the *cost-based optimizer* swap; runs once before execution |
| `BuildProbeSideOptimizer::VisitOperator` DELIM-flip | `:226-231` | optimizer time | active, ungated, correctness-required | No — semantic flip for DELIM joins, not a perf swap |
| RIGHT→LEFT lowering | `src/planner/binder/tableref/plan_joinref.cpp` | binder time | active, correctness | No |
| `PerfectHashJoinExecutor::CanDoPerfectHashJoin` | `src/execution/operator/join/physical_hash_join.cpp:171` | sink-init time | active | No — picks hash strategy, doesn't swap sides |
| `HashJoinGlobalSinkState::InitializeProbeSpill` | `:666-671` | runtime, on memory pressure | active | No — handles probe-side overflow within the same side assignment |
| External hash join partitioning | `physical_hash_join.cpp` (multiple) | runtime, when `external` | active, gated by `debug_force_external` and memory | No — partitions both sides for memory, doesn't swap which is build/probe |

**Conclusion**: there is zero pre-existing logic — active, dead, or commented-out — that swaps the build/probe assignment at runtime. Phase D is greenfield.

## D.2 — Pipeline lifecycle and what "runtime swap" could mean

For a left-deep chain `(A ⋈ B) ⋈ C` with `HJ1 = A ⋈ B` (probe=A, build=B), `HJ2 = HJ1 ⋈ C` (probe=HJ1, build=C):

```
Pipeline P1: scan(B) -> HJ1.sink                          [build phase of HJ1]
Pipeline P2: scan(C) -> HJ2.sink                          [build phase of HJ2]
Pipeline P3: scan(A) -> HJ1.probe -> HJ2.probe -> result  [probe pipeline]
```

P1 and P2 are independent build pipelines. P3 is the final probe pipeline. Both P1 and P2 finish before P3 runs (HashJoin is a pipeline-breaker on the build side).

`HashJoinFinalizeEvent::FinishEvent()` fires once per HashJoin sink at the end of its build pipeline, after the global hash table is fully populated. This is currently where `InitializeTieredHashCache()` is called.

Runtime swap could in principle happen at five distinct lifecycle moments:

1. **Before P1/P2 start** — no information yet beyond optimizer estimates; equivalent to running the optimizer swap rule again. Pointless for runtime.
2. **During P1/P2 build** — partial cardinality of build side is observable; probe side has not yet been touched. The HT is being populated; swapping means discarding partial work and rebuilding on the other side, which requires having materialized the other side somewhere.
3. **Just after P1/P2 finish, just before THC init** — exact build cardinality is known. Probe cardinality is still unknown.
4. **During P3 probe (early chunks)** — exact build cardinality known; running estimate of probe cardinality from the chunks seen so far. THC has already been instantiated (or skipped). Swapping at this point requires discarding the HT and rebuilding on the other side — the data for the other side may or may not still be available (it was streaming in).
5. **After P3 finishes** — too late; the join is done.

Useful runtime-swap windows: **2** (with materialization), **3** (only useful if we already materialized both sides), and **4** (only useful if both sides are still recoverable).

## D.3 — Design space

Five distinguishable designs, ordered by increasing intrusiveness:

### D.3.a — Plan-time swap only (no runtime swap, status quo)

Lean entirely on the optimizer's `BuildProbeSideOptimizer` decision and on `seeded_left_deep` plus `swap=false` to guarantee base-table builds. Phase C's THC bypass handles the remaining edge cases when `swap=true`.

- **Pros**: zero new runtime code; fully covered by Phase C.
- **Cons**: still uses optimizer cardinality estimates, which are unreliable for chained joins.
- **THC interaction**: trivial — Phase C already handles this.

### D.3.b — Defer-and-decide (delayed build)

Don't build the HT immediately. Stream the build side into a `RowDataCollection` (`TupleDataCollection`) only. When P3 starts, also stream the first `N` chunks of the probe side into a second `RowDataCollection`. After hitting a budget (rows or memory), pick the smaller as the actual build side, hash-build it, then drain the other against the HT.

- **Pros**: uses real cardinality on both sides for the decision. No re-build from scratch.
- **Cons**: removes pipeline parallelism for the first `N` chunks; doubles memory peak; complex pipeline rewiring (HJ becomes both source and sink of a sub-pipeline that resolves the side choice).
- **Lifecycle moment**: 3, after first N probe chunks.
- **THC interaction**: defer the `InitializeTieredHashCache` call until after the side is decided. Phase C's `build_source_is_base_table` flag is set after the decision, querying which physical operator fed the *chosen* build side. Single setter call, fits Phase C's hook cleanly.
- **Engineering cost**: high. 2–4 weeks. Touches `Pipeline`, `PipelineExecutor`, `HashJoinGlobalSinkState`, the entire build-then-probe protocol.
- **Risk areas**: parallelism (N chunks per thread vs. global), external joins, memory accounting, perfect hash, semi/anti joins.

### D.3.c — Build-then-rebuild on swap signal

Build normally on the assigned side. While probing, monitor incoming probe cardinality. If a swap heuristic triggers (e.g., probe cardinality so far × 2 > build cardinality), discard the HT, materialize the rest of the probe stream, swap, re-hash on the new build side, re-probe with stored data.

- **Pros**: keeps normal pipeline structure for the common case.
- **Cons**: re-hash cost is enormous if the swap fires late; the original build's materialized rows must be retained (they normally aren't — only the HT keeps them). Probably not worth it.
- **Lifecycle moment**: 4.
- **THC interaction**: drop the THC on swap (`hash_table->DropTieredHashCache()`), re-run Phase C's check on the new build, possibly re-instantiate. Two-way interaction; Phase C's design accommodates it but complicates the contract.
- **Engineering cost**: very high, 4–6 weeks. Hash table data is normally not retained beyond the HT itself; we'd have to keep `TupleDataCollection` references alive on both sides.
- **Risk**: hard to win in practice; the late re-hash usually swamps the savings.

### D.3.d — Sample-based pre-decision

Before building, sample N rows from each side (perhaps the first N rows of each base scan), estimate cardinality of both. Pick build side based on the sample. Then proceed normally.

- **Pros**: cheap (sample is small). Doesn't disturb pipeline structure beyond a bit of bookkeeping.
- **Cons**: sampling N rows isn't free for joins-on-joins (the lower join's output requires running the lower join first). Doesn't help with chained joins. Effectively only useful for two-base-table joins.
- **Lifecycle moment**: 1 (with extra sampling pass) or 2 (sample first N from each pipeline).
- **THC interaction**: clean — the swap fires before any HT is built, so Phase C's static evaluation is already correct (just inspects the post-swap topology).
- **Engineering cost**: medium, 1–2 weeks.
- **Risk**: marginal benefit for the chained-join workloads we care about.

### D.3.e — Hybrid: defer-and-decide for chained joins, plan-time for leaf joins

Combine D.3.a (leaf joins use plan-time swap or `seeded_left_deep`'s base-on-build guarantee) with D.3.b (chained joins get defer-and-decide). Only chained joins benefit from runtime info anyway, since for leaf joins the optimizer already has good base-table statistics.

- **Pros**: focuses runtime complexity on the joins where it matters.
- **Cons**: same complexity as D.3.b for the chained-join path.
- **THC interaction**: same as D.3.b for chained joins; Phase C unchanged for leaf joins.
- **Engineering cost**: same as D.3.b. The "leaf joins use plan-time" part is already free.

## D.4 — What signals are available at each lifecycle moment

Useful for picking which design fits which signal.

| Moment | Build cardinality | Probe cardinality | Build size in bytes | Probe size in bytes | Notes |
|---|---|---|---|---|---|
| 1. Before pipelines | optimizer estimate | optimizer estimate | from row width × estimate | from row width × estimate | Cheap, unreliable |
| 2. During build pipeline | streaming partial count | unknown | streaming row width × count | unknown | The build side data is being assembled into a `TupleDataCollection` at this point |
| 3. After build, before THC | exact | unknown | exact (`hash_table->Count() * tuple_size`) | unknown | Current decision point for THC. We know `|build|` exactly here. |
| 4. During probe (chunk k) | exact | streaming partial (cumulative chunk rows) | exact | streaming | Probe data has been fully read upstream (lower join probed) — chunks are streaming through |
| 5. After probe completes | exact | exact | exact | exact | Too late |

**Key observation**: at moment 3 (where THC is currently decided) we have exact `|build|` but no info on `|probe|`. The optimizer's estimate of probe cardinality is the best we have unless we delay further.

A possible compromise: at moment 3, compute `|probe_estimate| / |build_exact|` ratio using the optimizer estimate and the runtime build count. If the ratio is highly out of line with what was expected (e.g., build is 100× larger than the optimizer thought), trigger a more conservative path. This is signal-driven without a full swap.

## D.5 — Phase C's contract with each Phase D design

| Design | Phase C touchpoint | Override timing | Re-instantiation needed? |
|---|---|---|---|
| D.3.a (status quo) | Phase C populates flag at sink ctor; never overridden | n/a | No |
| D.3.b (defer-and-decide) | Phase C ignores its initial value; flag is set fresh after the side decision, before THC init | between side decision and FinishEvent | No (THC was never built) |
| D.3.c (build-then-rebuild) | Phase C populates initial flag; on swap, must call `DropTieredHashCache` then `SetBuildSourceIsBaseTable(new_value)`, then re-call `InitializeTieredHashCache` | mid-probe | Yes, complicates Phase C |
| D.3.d (sample-based) | Same as D.3.a — swap happens before sink ctor's static walk, so the flag reads the post-swap topology | n/a after sink ctor | No |
| D.3.e (hybrid) | Same as D.3.b for chained joins, D.3.a for leaf joins | between decision and FinishEvent | No |

The Phase C revised design supports D.3.a, D.3.b, D.3.d, D.3.e cleanly with one setter. D.3.c is the only one that requires a teardown path — and we recommend not implementing D.3.c on cost-benefit grounds anyway.

## D.6 — What we don't yet know

These are the questions that block a Phase D decision. Recommend the user think through them before authorizing any implementation:

1. **Workload**: do JOB / TPC-H queries actually have many cases where the optimizer's build/probe choice is wrong by a meaningful margin? Need a measurement, not a guess. Phase A/B/C measurements will give us this.
2. **Cost model accuracy**: how often does `BuildProbeSideOptimizer::TryFlipJoinChildren` make a "wrong" choice given perfect runtime cardinality? If the answer is "rarely on JOB", design D.3.a is the rational choice and Phase D is unnecessary.
3. **Memory budget**: D.3.b doubles peak memory during the side-decision window. Is that acceptable in the workloads we run?
4. **External-join interaction**: external (memory-spilled) hash joins partition both sides for memory reasons. Adding a side-swap on top of partitioning is a much harder system; is it in scope?
5. **Perfect-hash interaction**: if the build side qualifies for perfect hashing, swapping invalidates that decision. Do we just not consider swap when perfect hash is enabled?
6. **THC's value when build is a join intermediate**: this is the question Phase C is implicitly answering "no" to. Is there a workload where THC on a join intermediate actually helps? We assume not, but should sanity-check by leaving the bypass behind a setting (`disable_thc_for_intermediate_builds`).

## D.7 — Recommendation for the user (as research output, not a plan)

If forced to recommend, the ordering is:

1. **Land Phase A, B, C first.** They give us the data needed to evaluate whether Phase D pays off at all.
2. If Phase A–C measurements show the optimizer is making bad build/probe choices on chained joins for non-trivial workloads, **D.3.b** (defer-and-decide for chained joins, the D.3.e hybrid form) is the highest-leverage option.
3. **D.3.c** (build-then-rebuild) is almost certainly not worth it; the late-stage re-hash dominates.
4. **D.3.d** (sampling) is a good fit only if the cost model is wrong primarily on leaf joins, which we don't expect on JOB.

But these are research conclusions, not a plan. Final call is the user's.
