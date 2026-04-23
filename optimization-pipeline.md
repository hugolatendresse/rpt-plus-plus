# Optimization Pipeline: From SQL to Physical Plan

This document traces the full optimization pipeline in this DuckDB/RPT+ fork, using
`CreatePreparedStatementInternal` as the entry point.

---

## Table of Contents

1. [High-Level Overview](#1-high-level-overview)
2. [Phase 1 -- Planner](#2-phase-1----planner)
3. [Phase 2 -- Optimizer](#3-phase-2----optimizer)
   - [Built-In Optimizer Passes (ordered)](#built-in-optimizer-passes-ordered)
   - [The RPT+ / Join Order Block](#the-rpt--join-order-block)
   - [Transfer Graph (RPT+)](#transfer-graph-rpt)
   - [Query Graph (Join Order)](#query-graph-join-order)
   - [Join Enumeration Algorithms](#join-enumeration-algorithms)
   - [Left-Deep vs Bushy](#left-deep-vs-bushy)
   - [Build Side vs Probe Side](#build-side-vs-probe-side)
4. [Phase 3 -- Physical Plan Generator](#4-phase-3----physical-plan-generator)
   - [Bloom Filter Linking](#bloom-filter-linking)
   - [Physical Hash Join](#physical-hash-join)
   - [TieredHashCache (THC)](#tieredhashcache-thc)
5. [End-to-End Call Graph](#5-end-to-end-call-graph)

---

## 1. High-Level Overview

`ClientContext::CreatePreparedStatementInternal`
(`src/main/client_context.cpp`, lines 354-411) orchestrates three sequential
phases:

```
SQL string
  |
  v
[Phase 1] Planner        -- bind + build logical operator tree
  |
  v
[Phase 2] Optimizer       -- rewrite / optimize the logical tree
  |
  v
[Phase 3] PhysicalPlanGenerator -- lower to physical operators
  |
  v
PhysicalPlan (ready for execution)
```

The code:

```cpp
// Phase 1: Planner (line 364-375)
Planner logical_planner(*this);
logical_planner.CreatePlan(std::move(statement));

// Phase 2: Optimizer (line 390-401)
if (config.enable_optimizer && logical_plan->RequireOptimizer()) {
    Optimizer optimizer(*logical_planner.binder, *this);
    logical_plan = optimizer.Optimize(std::move(logical_plan));
}

// Phase 3: Physical Plan (line 403-408)
PhysicalPlanGenerator physical_planner(*this);
result->physical_plan = physical_planner.Plan(std::move(logical_plan));
```

---

## 2. Phase 1 -- Planner

**Entry:** `Planner::CreatePlan` in `src/planner/planner.cpp` (lines 34-100).

The planner performs three sub-steps:

### 2a. Binding (`Binder::Bind`)

`src/planner/binder.cpp`, line 45 in `Planner::CreatePlan`:

```cpp
auto bound_statement = binder->Bind(statement);
```

The binder resolves table names, column references, and types against the
catalog. It dispatches on `StatementType` (e.g. `SelectStatement`) and
produces a `BoundStatement` containing column names/types and an initial
logical operator tree.

For SELECT queries, binding flows through `Binder::BindNode` and
`Binder::CreatePlan(BoundSelectNode)` in
`src/planner/binder/query_node/plan_select_node.cpp` (lines 18-131), which
builds `LogicalGet`, joins, `LogicalFilter`, aggregates, `LogicalProjection`,
etc.

### 2b. Decorrelation

`src/planner/planner.cpp`, line 50:

```cpp
this->plan = FlattenDependentJoins::DecorrelateIndependent(
    *binder, std::move(bound_statement.plan));
```

Pulls correlated subqueries into independent joins where possible.

### 2c. Verification

`Planner::VerifyPlan` (line 86) checks column-binding consistency and
serialization round-trips when enabled.

---

## 3. Phase 2 -- Optimizer

**Entry:** `Optimizer::Optimize` in `src/optimizer/optimizer.cpp` (lines 281-308).

```cpp
unique_ptr<LogicalOperator> Optimizer::Optimize(unique_ptr<LogicalOperator> plan_p) {
    Verify(*plan_p);
    this->plan = std::move(plan_p);
    // 1. Run extension pre-optimizers
    // 2. RunBuiltInOptimizers()
    // 3. Run extension post-optimizers
    // 4. Planner::VerifyPlan
    return std::move(plan);
}
```

`RunBuiltInOptimizers` (lines 102-279) runs the ordered sequence of optimizer
passes described below. Each pass is gated by `RunOptimizer`, which checks
`disabled_optimizers` and wraps the pass in profiler metrics.

### Built-In Optimizer Passes (ordered)

| # | OptimizerType | What it does | File |
|---|---|---|---|
| 1 | `EXPRESSION_REWRITER` | Simplifies expressions (constant folding, arithmetic, LIKE, CASE, etc.) using pattern-matching rules registered in the `Optimizer` constructor (lines 42-69). | `src/optimizer/optimizer.cpp:120` |
| 2 | `SUM_REWRITER` | Rewrites `SUM(x + C)` into `SUM(x) + C * COUNT(x)`. | `src/optimizer/optimizer.cpp:122-126` |
| 3 | `FILTER_PULLUP` | Pulls filters up through the logical tree. | `src/optimizer/optimizer.cpp:128-132` |
| 4 | `FILTER_PUSHDOWN` | Pushes filters down toward scans; also converts MARK joins to SEMI joins. | `src/optimizer/optimizer.cpp:134-140` |
| 5 | `CTE_FILTER_PUSHER` | Derives and pushes filters into materialized CTEs. | `src/optimizer/optimizer.cpp:142-146` |
| 6 | `REGEX_RANGE` | Rewrites regex filters into range scans where possible. | `src/optimizer/optimizer.cpp:148-151` |
| 7 | `DELIMINATOR` | Removes redundant `DelimGet`/`DelimJoin` operators. | `src/optimizer/optimizer.cpp:153-157` |
| 8 | `EMPTY_RESULT_PULLUP` | Propagates empty results upward to short-circuit computation. | `src/optimizer/optimizer.cpp:159-163` |
| 9 | `IN_CLAUSE` | Rewrites IN clauses (e.g. into hash lookups). | `src/optimizer/optimizer.cpp:165-168` |
| 10-12 | **RPT+ / JOIN_ORDER block** | `PredicateTransferOptimizer::PreOptimize` -> `JoinOrderOptimizer::Optimize` -> `PredicateTransferOptimizer::Optimize`. See [dedicated section below](#the-rpt--join-order-block). | `src/optimizer/optimizer.cpp:170-185` |
| 13 | `UNNEST_REWRITER` | Rewrites UNNESTs in DelimJoins by moving them to the projection. | `src/optimizer/optimizer.cpp:187-191` |
| 14 | `UNUSED_COLUMNS` | Removes unused columns from the plan. | `src/optimizer/optimizer.cpp:193-197` |
| 15 | `DUPLICATE_GROUPS` | Removes duplicate GROUP BY expressions. | `src/optimizer/optimizer.cpp:199-203` |
| 16 | `COMMON_SUBEXPRESSIONS` | Extracts common sub-expressions inside operators. | `src/optimizer/optimizer.cpp:205-209` |
| 17 | `COLUMN_LIFETIME` (1st pass) | Creates projection maps so unused columns are projected out early. | `src/optimizer/optimizer.cpp:211-215` |
| 18 | `BUILD_SIDE_PROBE_SIDE` | Decides which join child should be build vs probe. See [dedicated section](#build-side-vs-probe-side). | `src/optimizer/optimizer.cpp:217-222` |
| 19 | `LIMIT_PUSHDOWN` | Pushes LIMIT below PROJECTION. | `src/optimizer/optimizer.cpp:224-228` |
| 20 | `SAMPLING_PUSHDOWN` | Pushes sampling operators down. | `src/optimizer/optimizer.cpp:230-234` |
| 21 | `TOP_N` | Transforms ORDER BY + LIMIT into a TopN operator. | `src/optimizer/optimizer.cpp:236-240` |
| 22 | `LATE_MATERIALIZATION` | Delays materializing columns that aren't needed until later. | `src/optimizer/optimizer.cpp:242-246` |
| 23 | `STATISTICS_PROPAGATION` | Propagates statistics through the plan and stores a `statistics_map`. | `src/optimizer/optimizer.cpp:248-254` |
| 24 | `COMMON_AGGREGATE` | Removes duplicate aggregate functions. | `src/optimizer/optimizer.cpp:256-260` |
| 25 | `COLUMN_LIFETIME` (2nd pass) | Second column-lifetime pass after other rewrites. | `src/optimizer/optimizer.cpp:262-266` |
| 26 | `REORDER_FILTER` | Applies expression heuristics to reorder filter predicates. | `src/optimizer/optimizer.cpp:268-272` |
| 27 | `JOIN_FILTER_PUSHDOWN` | Pushes join-derived filters down after all other optimizations. | `src/optimizer/optimizer.cpp:274-278` |

### The RPT+ / Join Order Block

This is the most important block for join optimization. It lives in
`src/optimizer/optimizer.cpp` lines 170-185 and runs three steps inside a
single scope:

```cpp
{
    // Step 1: Extract predicate transfer info (before join reorder)
    PredicateTransferOptimizer PT(context);
    plan = PT.PreOptimize(std::move(plan));

    // Step 2: Join order optimization
    RunOptimizer(OptimizerType::JOIN_ORDER, [&]() {
        JoinOrderOptimizer optimizer(context);
        plan = optimizer.Optimize(std::move(plan));
    });

    // Step 3: Insert Bloom filter operators (after join reorder)
    plan = PT.Optimize(std::move(plan));
}
```

**Why this order matters:** `PreOptimize` must run before join reordering
because it extracts join-condition information from the original logical tree
that would be restructured by the join order optimizer. After join reordering,
`Optimize` uses the transfer graph to insert `LogicalCreateBF` /
`LogicalUseBF` operators for Bloom filter information passing.

### Transfer Graph (RPT+)

The transfer graph is the central data structure for RPT+ (Recursive Predicate
Transfer). It is a DAG that determines which tables create Bloom filters and
which tables consume them.

**Definition:** `src/include/duckdb/optimizer/predicate_transfer/dag.hpp`

```
TransferGraph = unordered_map<idx_t, unique_ptr<GraphNode>>
```

Key types:
- `GraphNode`: One per table operator. Contains `forward_stage_edges` and
  `backward_stage_edges` (each with `in`/`out` vectors of `GraphEdge`).
- `GraphEdge`: Points to a destination table; carries column bindings and
  `FilterPlan` pointers for the Bloom filters on that edge.
- `FilterPlan`: Specifies `build` columns (on the creating table) and `apply`
  columns (on the consuming table).

**Construction:** `TransferGraphManager::Build` in
`src/optimizer/predicate_transfer/transfer_graph_manager.cpp` (lines 50-69):

1. **Extract operators** -- `TableOperatorManager::ExtractOperators` walks the
   logical tree and collects table operators and join operators.
2. **Extract edge info** -- `ExtractEdgesInfo` (lines 157-263) reads equality
   join conditions from `LOGICAL_COMPARISON_JOIN` / `LOGICAL_DELIM_JOIN` and
   builds a `neighbor_matrix` with direction masks (`protect_left` /
   `protect_right`) derived from LEFT/RIGHT/MARK/delim semantics.
3. **Skip unfiltered tables** -- `SkipUnfilteredTable` marks tables with no
   filters as receive-only (they use BFs but don't create them).
4. **Create transfer plan** -- `CreateTransferPlanUpdated` (lines 677-733)
   turns selected edges into forward/backward edges.

**Root selection:** `LargestRootUpdated`
(`src/optimizer/predicate_transfer/transfer_graph_manager.cpp`, lines 557-626)
picks the root for transfer traversal order:

1. Iterate tables from largest to smallest.
2. Pick the largest table that has a filter predicate or is an intermediate
   result.
3. If no such table exists, pick the largest table overall.
4. Build the `transfer_order` by repeatedly calling `FindEdge` to connect
   unconstructed tables to the constructed set (spanning-tree style).

**Forward and backward passes** in `PredicateTransferOptimizer::Optimize`
(`src/optimizer/predicate_transfer/predicate_transfer_optimizer.cpp`, lines
22-49):

- **Forward pass**: Iterates `transfer_order` in reverse. For each node,
  creates BF plans and adds them to outgoing edges. This propagates
  selectivity information "forward" (from filtered tables to their neighbors).
- **Backward pass** (unless `rpt_forward_only` is set): Iterates
  `transfer_order` in original order. Propagates information in the opposite
  direction.

After both passes, `InsertTransferOperators` (lines 52-104) rewrites the
logical tree by wrapping table operators in chains of `LogicalCreateBF` and
`LogicalUseBF` operators.

### Query Graph (Join Order)

The query graph is a **hypergraph** used by the join order optimizer. It is
separate from the transfer graph.

**Definition:** `QueryGraphEdges` in
`src/include/duckdb/optimizer/join_order/query_graph.hpp` (lines 36-68).

- `QueryEdge`: A trie-like structure keyed by relation IDs, storing
  `NeighborInfo` at leaves.
- `NeighborInfo`: Points to a neighbor `JoinRelationSet` and a list of
  `FilterInfo*` (the join conditions on that edge).

**Construction:** `QueryGraphManager::Build` in
`src/optimizer/join_order/query_graph_manager.cpp` (lines 22-36):

1. **`ExtractJoinRelations`** -- Walks the logical tree and identifies
   reorderable join relations. Non-reorderable subtrees (e.g. subqueries,
   aggregates) are recursively optimized by child `JoinOrderOptimizer`
   instances.
2. **`ExtractEdges`** -- Collects all filter operators and determines which
   relations each filter references.
3. **`CreateHyperGraphEdges`** -- For each filter that spans two disjoint
   relation sets, creates bidirectional edges in the query graph via
   `query_graph.CreateEdge(left_set, right_set, filter_info)`.

Cross products (disconnected components) are handled later by
`CreateQueryGraphCrossProduct` (line 410-412).

### Join Enumeration Algorithms

**Entry:** `JoinOrderOptimizer::Optimize` in
`src/optimizer/join_order/join_order_optimizer.cpp` (lines 24-91).

After `QueryGraphManager::Build`, if the query is reorderable, a
`PlanEnumerator` is created and dispatched based on
`context.config.join_order_mode`:

| Mode | Method | Description |
|---|---|---|
| `DPHYP` (default) | `SolveJoinOrder()` | DPhyp algorithm ("Dynamic Programming Strikes Back" by Moerkotte & Neumann). Explores connected subgraph pairs (CSG-CMP enumeration). Falls back to greedy if >= 12 relations or 10K pairs exceeded. |
| `EXACT_LEFT_DEEP` | `SolveJoinOrderLeftDeep()` | Only considers plans where each join combines an accumulated partial plan with a single base relation (forces left-deep shape). |
| `RANDOM_BUSHY` | `SolveJoinOrderRandom()` | Randomly picks valid join pairs until one tree remains. |
| `RANDOM_LEFT_DEEP` | `SolveJoinOrderLeftDeepRandom()` | Random first pair, then always joins accumulated tree with a remaining base relation. |

**Implementation:** `src/optimizer/join_order/plan_enumerator.cpp`

- `SolveJoinOrder` (lines 692-725): Tries exact DPhyp first; greedy fallback.
  Threshold constant: `THRESHOLD_TO_SWAP_TO_APPROXIMATE = 12` (defined in
  `src/include/duckdb/optimizer/join_order/plan_enumerator.hpp`, line 36).
- `SolveJoinOrderExactly` (lines 322-342): Core DPhyp loop -- for each
  relation as start node, calls `EmitCSG` then `EnumerateCSGRecursive`.
- `SolveJoinOrderApproximately` (lines 345-447): Greedy algorithm (GOO) that
  repeatedly picks the minimum-cost connectable pair.
- `TryEmitPair` (lines 172-185): Counts pairs and aborts exact enumeration
  after 10,000 pairs.

**Cost model:** `CostModel::ComputeCost` in
`src/optimizer/join_order/cost_model.cpp` (lines 12-22):

```cpp
double CostModel::ComputeCost(DPJoinNode &left, DPJoinNode &right) {
    auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);
    auto join_cost = join_card;
    // Asymmetric cost for left-deep mode
    if (context.config.join_order_mode == JoinOrderMode::EXACT_LEFT_DEEP) {
        return join_cost + left.cost + 1.2 * right.cost;
    }
    return join_cost + left.cost + right.cost;
}
```

In `EXACT_LEFT_DEEP` mode, the right (build) side gets a 1.2x penalty to
encourage placing the smaller relation there.

**Reconstruction:** After enumeration, `QueryGraphManager::Reconstruct` calls
`GenerateJoins` (`src/optimizer/join_order/query_graph_manager.cpp`, lines
237-404) which recursively rebuilds the logical operator tree from the DP
table. The final plan covers all relations (the entry for the full
`JoinRelationSet`).

### Left-Deep vs Bushy

- **DPhyp (default)** explores arbitrary connected-subgraph pairs, so it
  produces **bushy** trees (any binary join tree shape).
- **`EXACT_LEFT_DEEP`** only joins partial plans with base relations, forcing
  a **left-deep** chain.
- **`RANDOM_BUSHY`** can produce any shape.
- **`RANDOM_LEFT_DEEP`** forces left-deep.
- There is **no dedicated right-deep enumerator**. However, the
  `BuildProbeSideOptimizer` can swap children after the fact, which can
  effectively mirror parts of the tree.

### Build Side vs Probe Side

**Convention:** Throughout the codebase, **`children[0]` = left = probe side**
and **`children[1]` = right = build side**.

**Set during join reconstruction:**
`QueryGraphManager::GenerateJoins`
(`src/optimizer/join_order/query_graph_manager.cpp`, lines 268-272):

```cpp
auto join = make_uniq<LogicalComparisonJoin>(chosen_filter->join_type);
// Our build side is the right side.
// So the right plans should have lower cardinalities.
join->children.push_back(std::move(left.op));   // child[0] = probe
join->children.push_back(std::move(right.op));  // child[1] = build
```

**Refined by `BuildProbeSideOptimizer`:**
`src/optimizer/build_probe_side_optimizer.cpp`

This optimizer runs as pass #18 (after column lifetime analysis provides row
width information). Its `TryFlipJoinChildren` method (lines 159-210):

1. Estimates **build cost** for each side using `GetBuildSizes` (cardinality
   times row width, with penalties for variable-length types).
2. If the right child is a bare table scan and the left child has nested
   joins, applies `PREFER_RIGHT_DEEP_PENALTY` to discourage building on the
   right (since left-side tuples are already in flight from prior joins).
3. If `right_side_build_cost > left_side_build_cost`, **swaps children** via
   `FlipChildren`.
4. When cardinalities are equal, prefers rowid-containing columns on the probe
   side (useful for UPDATE/DELETE).

`FlipChildren` (lines 54-81) swaps the children and **inverts the join type**
(e.g. LEFT -> RIGHT), swaps each condition's left/right expressions, and swaps
projection maps to maintain correctness.

---

## 4. Phase 3 -- Physical Plan Generator

**Entry:** `PhysicalPlanGenerator::Plan` in
`src/execution/physical_plan_generator.cpp` (lines 25-28).

`Plan` calls `ResolveAndPlan` (lines 31-56), which performs:

1. **Bloom filter linking** -- `TransferBFLinker::LinkBFOperators(*op)` (line
   36).
2. **Column binding resolution** -- `ColumnBindingResolver` replaces
   `ColumnBinding` references with physical `BoundReferenceExpression` indices
   (line 40-41).
3. **Type resolution** -- `op->ResolveOperatorTypes()` (line 46).
4. **Physical plan creation** -- `PlanInternal(*op)` -> `CreatePlan(op)` (line
   51, 63), which is a large switch on `LogicalOperatorType` (lines 76-194)
   that maps each logical operator to its physical counterpart.

### Bloom Filter Linking

`TransferBFLinker::LinkBFOperators` in
`src/execution/transfer_bf_linker.cpp` runs immediately before column binding
resolution. It:

1. Collects all `LogicalCreateBF` operators.
2. Links each `LogicalUseBF` to its corresponding `LogicalCreateBF` via
   `related_create_bf`.
3. Prunes useless BF operators.
4. Sets the `below_join` flag on `LogicalUseBF` nodes that sit directly under
   a join's probe (left) child.

Physical BF operators are then created during `CreatePlan`:
- `LOGICAL_CREATE_BF` -> `PhysicalCreateBF`
  (`src/execution/physical_plan/plan_create_bf.cpp`)
- `LOGICAL_USE_BF` -> `PhysicalUseBF`
  (`src/execution/physical_plan/plan_use_bf.cpp`)

### Physical Hash Join

When `CreatePlan` encounters a `LOGICAL_COMPARISON_JOIN`, it dispatches to
`PlanComparisonJoin` in `src/execution/physical_plan/plan_comparison_join.cpp`
(line 28).

If the join has at least one **equality** condition (and range joins are not
preferred), it creates a `PhysicalHashJoin` (lines 63-69):

```cpp
auto &join = Make<PhysicalHashJoin>(op, left, right, std::move(op.conditions),
    op.join_type, op.left_projection_map, op.right_projection_map,
    std::move(op.mark_types), op.estimated_cardinality,
    std::move(op.filter_pushdown));
```

The physical hash join inherits the child ordering from the logical plan:
- **`children[0]`** (left) = **probe side** -- keys come from `condition.left`
- **`children[1]`** (right) = **build side** -- keys come from
  `condition.right`, data is sunk into the hash table

Other physical join types (for non-equality conditions): `PhysicalIEJoin`,
`PhysicalPiecewiseMergeJoin`, `PhysicalNestedLoopJoin`,
`PhysicalBlockwiseNLJoin`.

### TieredHashCache (THC)

The TieredHashCache is this fork's optimization for hash joins where the hot
portion of the build-side hash table can fit in L3 cache.

**Definition:** `src/include/duckdb/execution/tiered_hash_cache.hpp`

**Initialization:** `JoinHashTable::InitializeTieredHashCache` in
`src/execution/join_hashtable.cpp` (lines 1659-1763). Called after the hash
table is finalized (from `HashJoinFinalizeEvent::FinishEvent` in
`src/execution/operator/join/physical_hash_join.cpp`, lines 639-646).

THC copies accessed rows to a compact buffer that ideally stays in L3 cache,
so subsequent probes hit cached hot data instead of cold build-side entries
that will never be accessed.

**Probe-time use:** `JoinHashTable::GetRowPointers`
(`src/execution/join_hashtable.cpp`, lines 683+) adaptively switches between
phases:
- **BASELINE** -- Measure performance without THC.
- **COLLECT** -- Copy accessed rows into the THC buffer.
- **READ_ONLY** -- Probe the THC first, fall back to the main HT on misses.

The THC can be disabled via `ClientConfig::disable_tiered_hash_cache`.

---

## 5. End-to-End Call Graph

```
ClientContext::CreatePreparedStatementInternal
    (src/main/client_context.cpp:354)
|
+-- Planner::CreatePlan
|       (src/planner/planner.cpp:34)
|   |
|   +-- Binder::Bind(statement)
|   |       (src/planner/binder.cpp:146)
|   |       Resolves tables/columns/types, builds BoundStatement
|   |
|   +-- FlattenDependentJoins::DecorrelateIndependent
|   |       Pulls correlated subqueries into independent joins
|   |
|   +-- Planner::VerifyPlan
|
+-- Optimizer::Optimize
|       (src/optimizer/optimizer.cpp:281)
|   |
|   +-- RunBuiltInOptimizers
|           (src/optimizer/optimizer.cpp:102)
|       |
|       +-- EXPRESSION_REWRITER
|       +-- SUM_REWRITER
|       +-- FILTER_PULLUP
|       +-- FILTER_PUSHDOWN
|       +-- CTE_FILTER_PUSHER
|       +-- REGEX_RANGE
|       +-- DELIMINATOR
|       +-- EMPTY_RESULT_PULLUP
|       +-- IN_CLAUSE
|       +-- [RPT+ / Join Order block]
|       |   |
|       |   +-- PredicateTransferOptimizer::PreOptimize
|       |   |       Builds TransferGraph from join conditions
|       |   |       (src/optimizer/predicate_transfer/predicate_transfer_optimizer.cpp:17)
|       |   |   |
|       |   |   +-- TransferGraphManager::Build
|       |   |           (src/optimizer/predicate_transfer/transfer_graph_manager.cpp:50)
|       |   |       |
|       |   |       +-- ExtractOperators (table + join operators)
|       |   |       +-- ExtractEdgesInfo (equality conditions -> neighbor_matrix)
|       |   |       +-- SkipUnfilteredTable
|       |   |       +-- CreateTransferPlanUpdated
|       |   |           |
|       |   |           +-- LargestRootUpdated (pick root, build transfer_order)
|       |   |
|       |   +-- JoinOrderOptimizer::Optimize
|       |   |       (src/optimizer/join_order/join_order_optimizer.cpp:24)
|       |   |   |
|       |   |   +-- QueryGraphManager::Build
|       |   |   |       (src/optimizer/join_order/query_graph_manager.cpp:22)
|       |   |   |   |
|       |   |   |   +-- ExtractJoinRelations
|       |   |   |   +-- ExtractEdges
|       |   |   |   +-- CreateHyperGraphEdges -> query_graph.CreateEdge(...)
|       |   |   |
|       |   |   +-- CostModel (src/optimizer/join_order/cost_model.cpp)
|       |   |   +-- PlanEnumerator
|       |   |   |       (src/optimizer/join_order/plan_enumerator.cpp)
|       |   |   |   |
|       |   |   |   +-- InitLeafPlans
|       |   |   |   +-- SolveJoinOrder (DPhyp) / SolveJoinOrderLeftDeep / etc.
|       |   |   |       |
|       |   |   |       +-- SolveJoinOrderExactly (DPhyp CSG-CMP enumeration)
|       |   |   |       +-- SolveJoinOrderApproximately (greedy fallback)
|       |   |   |
|       |   |   +-- QueryGraphManager::Reconstruct -> GenerateJoins
|       |   |           (src/optimizer/join_order/query_graph_manager.cpp:237)
|       |   |           Rebuilds LogicalComparisonJoin tree from DP table.
|       |   |           Sets child[0]=probe, child[1]=build.
|       |   |
|       |   +-- PredicateTransferOptimizer::Optimize
|       |           (src/optimizer/predicate_transfer/predicate_transfer_optimizer.cpp:22)
|       |       |
|       |       +-- Forward pass: create BFs on filtered tables
|       |       +-- Backward pass: propagate BFs in reverse direction
|       |       +-- InsertTransferOperators: wrap table ops with
|       |               LogicalCreateBF / LogicalUseBF
|       |
|       +-- UNNEST_REWRITER
|       +-- UNUSED_COLUMNS
|       +-- DUPLICATE_GROUPS
|       +-- COMMON_SUBEXPRESSIONS
|       +-- COLUMN_LIFETIME (1st pass)
|       +-- BUILD_SIDE_PROBE_SIDE
|       |       (src/optimizer/build_probe_side_optimizer.cpp)
|       |       May swap join children based on estimated build cost.
|       +-- LIMIT_PUSHDOWN
|       +-- SAMPLING_PUSHDOWN
|       +-- TOP_N
|       +-- LATE_MATERIALIZATION
|       +-- STATISTICS_PROPAGATION
|       +-- COMMON_AGGREGATE
|       +-- COLUMN_LIFETIME (2nd pass)
|       +-- REORDER_FILTER
|       +-- JOIN_FILTER_PUSHDOWN
|
+-- PhysicalPlanGenerator::Plan
        (src/execution/physical_plan_generator.cpp:25)
    |
    +-- TransferBFLinker::LinkBFOperators
    |       Links LogicalCreateBF <-> LogicalUseBF pairs
    |
    +-- ColumnBindingResolver
    |       Replaces ColumnBinding with physical indices
    |
    +-- ResolveOperatorTypes
    |
    +-- PlanInternal -> CreatePlan (switch on LogicalOperatorType)
        |
        +-- LOGICAL_COMPARISON_JOIN -> PlanComparisonJoin
        |       (src/execution/physical_plan/plan_comparison_join.cpp:28)
        |       Creates PhysicalHashJoin (if equality) or other join type
        |       child[0] = probe, child[1] = build
        |
        +-- LOGICAL_CREATE_BF -> PhysicalCreateBF
        |       (src/execution/physical_plan/plan_create_bf.cpp)
        |
        +-- LOGICAL_USE_BF -> PhysicalUseBF
        |       (src/execution/physical_plan/plan_use_bf.cpp)
        |
        +-- ... (all other logical -> physical mappings)
```

---

## Key File Index

| Component | Header | Implementation |
|---|---|---|
| Entry point | `src/include/duckdb/main/client_context.hpp` | `src/main/client_context.cpp` |
| Planner | `src/include/duckdb/planner/planner.hpp` | `src/planner/planner.cpp` |
| Binder | `src/include/duckdb/planner/binder.hpp` | `src/planner/binder.cpp` |
| Optimizer | `src/include/duckdb/optimizer/optimizer.hpp` | `src/optimizer/optimizer.cpp` |
| Join Order Optimizer | `src/include/duckdb/optimizer/join_order/join_order_optimizer.hpp` | `src/optimizer/join_order/join_order_optimizer.cpp` |
| Query Graph | `src/include/duckdb/optimizer/join_order/query_graph.hpp` | `src/optimizer/join_order/query_graph.cpp` |
| Query Graph Manager | `src/include/duckdb/optimizer/join_order/query_graph_manager.hpp` | `src/optimizer/join_order/query_graph_manager.cpp` |
| Plan Enumerator | `src/include/duckdb/optimizer/join_order/plan_enumerator.hpp` | `src/optimizer/join_order/plan_enumerator.cpp` |
| Cost Model | `src/include/duckdb/optimizer/join_order/cost_model.hpp` | `src/optimizer/join_order/cost_model.cpp` |
| Build/Probe Optimizer | `src/include/duckdb/optimizer/build_probe_side_optimizer.hpp` | `src/optimizer/build_probe_side_optimizer.cpp` |
| Transfer Graph (RPT+) | `src/include/duckdb/optimizer/predicate_transfer/dag.hpp` | `src/optimizer/predicate_transfer/dag.cpp` |
| Transfer Graph Manager | `src/include/duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp` | `src/optimizer/predicate_transfer/transfer_graph_manager.cpp` |
| Predicate Transfer Optimizer | `src/include/duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp` | `src/optimizer/predicate_transfer/predicate_transfer_optimizer.cpp` |
| Physical Plan Generator | `src/include/duckdb/execution/physical_plan_generator.hpp` | `src/execution/physical_plan_generator.cpp` |
| Physical Hash Join | `src/include/duckdb/execution/operator/join/physical_hash_join.hpp` | `src/execution/operator/join/physical_hash_join.cpp` |
| Join Hash Table | `src/include/duckdb/execution/join_hashtable.hpp` | `src/execution/join_hashtable.cpp` |
| TieredHashCache | `src/include/duckdb/execution/tiered_hash_cache.hpp` | (inline in header + `join_hashtable.cpp`) |
| BF Linker | -- | `src/execution/transfer_bf_linker.cpp` |
| Plan Comparison Join | -- | `src/execution/physical_plan/plan_comparison_join.cpp` |
