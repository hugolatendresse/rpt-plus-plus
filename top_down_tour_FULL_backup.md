# Cardinality swaps in transfer graph
_Exported on 4/21/2026 at 17:25:02 EDT from Cursor (3.0.12)_

---

**User**

This project implements this paper https://yimingqiao.github.io/files/rpt_plus.pdf


In @src/optimizer/predicate_transfer/transfer_graph_manager.cpp, in TransferGraphManager::CreateTransferPlanUpdated, we are doing cardinality swaps on the transfer graph? How come? This is not in the paper and wouldn't that break the correctness of the transfer schedule in BCY? 





---

**Cursor**

Let me read the relevant code and the paper to understand the cardinality swaps in question.

Good question. Let me walk through what the swap is actually doing and why it doesn't break correctness.

## The Swap is a Normalization Step, Not a Topology Change

Look at the key block (lines 780-785 in `CreateTransferPlanUpdated`, but identical in `CreateOriginTransferPlan` and `CreateTransferPlanTHC`):

```780:785:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		// smaller table is in the left
		if (left_node->cardinality_order > right_node->cardinality_order) {
			std::swap(left_node, right_node);
			std::swap(left_cols, right_cols);
			std::swap(protect_left, protect_right);
		}
```

This runs **after** the spanning tree has already been built by `LargestRootUpdated`. The selected edges and the transfer order are fixed at this point. The swap is not modifying the spanning tree or the transfer schedule -- it's just relabeling the local variables within each loop iteration so that `left_node` consistently refers to the smaller table and `right_node` to the larger one.

## Why It's Needed

When edges are created in `ExtractEdgesInfo`, the `left_table`/`right_table` assignment is arbitrary -- it comes from whichever side of the join condition happened to be on the left vs. right in the logical plan:

```207:208:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
			auto edge =
			    make_shared_ptr<EdgeInfo>(cond.left->return_type, *left_node, left_binding, *right_node, right_binding);
```

So when iterating over `selected_edges`, there's no guarantee that `edge->left_table` is the smaller or larger node. The swap canonicalizes this so the subsequent code can uniformly assign:

- **Forward** (lines 788-791): smaller (left) creates BF, larger (right) receives it -- i.e., leaf-to-root direction
- **Backward** (lines 794-807): larger creates BF, smaller receives it -- i.e., root-to-leaves direction

This matches exactly what the paper describes: forward pass flows from leaves to root (small to large), backward pass flows from root to leaves (large to small). The paper doesn't mention this because it's an implementation detail -- in the algorithm's formulation, the tree structure inherently defines these directions. In the code, edges are stored without inherent directionality, so the swap provides it.

## Why Correctness is Preserved

Three things are swapped **together**: `left_node`/`right_node`, `left_cols`/`right_cols`, and `protect_left`/`protect_right`. The protection flags (which encode join-type constraints like "don't filter the preserved side of a LEFT JOIN") stay correctly associated with their respective tables after the swap. So the filter direction constraints from `SkipUnfilteredTable` and the join-type analysis in `ExtractEdgesInfo` are all preserved.

The spanning tree topology, the node insertion order, and the BCY full-reduction guarantees from Algorithm 1 in the paper (Section 3.3) are completely unaffected -- those are all determined earlier in `LargestRootUpdated`/`FindEdge`. The swap is purely about orienting the edges for the forward/backward labeling step that comes next.

---

**User**

Isn't this a contradiction? YOu say there are no transfer changes due to cardinality, but the forward pass direction of BF passing actually varies based on who is `left` and `right`?

---

**Cursor**

You're right to push back -- I oversimplified. Let me correct myself.

The swap **does** determine the direction of BF flow on each edge. After the swap:

- **Forward**: `left_node` (smaller cardinality_order) **creates** the BF, `right_node` (larger) **receives** it
- **Backward**: `right_node` (larger) **creates** the BF, `left_node` (smaller) **receives** it

```788:791:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		if (!protect_right) {
			left_node->Add(right_node->id, {left_cols}, {right_cols}, {type}, true, false);
			right_node->Add(left_node->id, {left_cols}, {right_cols}, {type}, true, true);
		}
```

The last two booleans are `is_forward` and `is_receive`. So `(true, false)` = "I create and send a forward BF" and `(true, true)` = "I receive a forward BF." The swap determines who sends and who receives. That's not cosmetic -- I was wrong.

## Why It Doesn't Break Correctness

The reason it still works is that `cardinality_order` faithfully encodes the **parent-child relationship** in the spanning tree. Look at how it's assigned in `LargestRootUpdated`:

```578:589:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
	for (auto &entry : table_operator_manager.table_operators) {
		idx_t id = entry.first;
		if (id == root) {
			constructed_set.insert(id);
		} else {
			unconstructed_set.insert(id);
		}

		auto node = make_uniq<GraphNode>(id, prior_flag--);
		transfer_graph[id] = std::move(node);
	}
```

The root gets a value in `[0, n-1]` (positive). Then during tree building:

```611:612:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		auto node = transfer_graph[selected_edge.second].get();
		node->cardinality_order = prior_flag--;
```

Each subsequent child **overwrites** its cardinality_order to `-1, -2, -3, ...`. So:

- **Root**: positive cardinality_order (highest)
- **First child added**: -1
- **Second child added**: -2
- ... and so on

Since `FindEdge` returns `(j, i)` where `j` is already in the tree (the parent) and `i` is being added (the child), parents are **always** added before their children and therefore **always** have a higher `cardinality_order`.

So the swap is effectively orienting each edge as:
- Forward = child → parent (leaves to root)
- Backward = parent → child (root to leaves)

This is exactly the Yannakakis two-pass semantics. The swap is using `cardinality_order` as a **proxy for tree depth/insertion order**, not actual table cardinality. The comment `// smaller table is in the left` is misleading -- it's really "child node goes on the left." It works because tree insertion order and the parent-child relationship are the same thing by construction of the spanning tree.

So: you were right that it's not just cosmetic renaming, and I should have said that more clearly. But it doesn't break BCY correctness because the ordering perfectly mirrors the spanning tree structure.

---

**User**

In LargestRootUpdated in @src/optimizer/predicate_transfer/transfer_graph_manager.cpp , why do we need the logic inside the for-loop on line 566? That is, why don't we just want the bigger table as the root (O(1) access)?

---

**Cursor**

Good catch -- this is an optimization specific to RPT+ that ties into `SkipUnfilteredTable` (which ran earlier in `Build`).

## Why not just `sorted_nodes.back()`?

Because `sorted_nodes.back()` might be an **unfiltered table**, and rooting at an unfiltered table is broken given how `SkipUnfilteredTable` has mutated the edges.

Recall from `SkipUnfilteredTable` (lines 434-447):

```434:447:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		// ---- Step 4: disable all outgoing edges from this unfiltered table ----
			for (auto &e : outgoing) {
				if (e->left_binding.table_index == t) {
					if (!e->protect_right) {
						e->protect_right = true; // block Left→Right
						changed = true;
					}
				} else {
					if (!e->protect_left) {
						e->protect_left = true; // block Right→Left
						changed = true;
					}
				}
			}
```

For every unfiltered table `t`, the **outgoing** direction on all its edges is blocked (`protect_*` set to true). Only the incoming direction survives.

Now walk through what happens if we make an unfiltered table the root in `CreateTransferPlanUpdated`:

1. The root has the highest `cardinality_order`, so in every edge touching it, `left_node` (= root, before swap) has `cardinality_order > right_node`, which triggers the swap.
2. After the swap, the root sits on the *right* (large) side, and `protect_left` (post-swap) is whatever was `protect_right` originally -- i.e., the flag that blocked root → neighbor.
3. In the backward pass block:

```794:807:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		// backward: from the larger to the smaller
		if (!protect_left) {
			...
		}
```

   `protect_left` is true for every root-incident edge (because outgoing from root was disabled). So **the entire backward pass gets skipped** for the root -- no BF is ever broadcast from the root to its children, which is precisely the "reusable filter" step that RPT+'s ATP is built around.

The forward pass still works (neighbors can send into the root), but you've defeated the point of having a root at all: the root is supposed to hold the accumulated filter intersection and broadcast it back.

## Why prefer *intermediate* tables too?

`intermediate_table` is the set of bridge tables -- tables that belong to more than one equivalence class (line 301-304). The paper (Section 3.4.2, Figure 5) explicitly identifies bridge tables as the roots of each class's induced subtree, where per-class filters are constructed in the backward pass. Those are also valuable root candidates even if they don't carry a local predicate, because they're the natural aggregation points for multiple equivalence classes.

## Why "largest" among them?

Iterating `rbegin() → rend()` over a cardinality-sorted list picks the **largest** filtered-or-intermediate table. This matches the paper's original rationale for rooting at the largest table:

- A large root means more information flows upward during the forward pass before the final intersection is computed (fewer wasteful small BFs probing a huge table late), and the single filter created at the large root can be broadcast to many smaller tables in the backward pass.
- Ties with the original LargestRoot intuition but gated by the "must be filtered/intermediate" constraint introduced by this implementation.

## The fallback

```572:576:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
	// If we cannot find it, all tables have no filter
	if (root == std::numeric_limits<idx_t>::max()) {
		auto &node = sorted_nodes.back();
		root = table_operator_manager.GetScalarTableIndex(node);
	}
```

If literally no table qualifies (no filters anywhere, no bridges), then predicate transfer has nothing useful to do anyway, so we fall back to the paper's default: the largest overall. In this degenerate case the broken-backward-pass concern above also doesn't matter, because with no filtered tables there's nothing to propagate.

## TL;DR

O(1) `sorted_nodes.back()` would be correct in the pure paper algorithm, but after `SkipUnfilteredTable` has disabled outgoing edges from unfiltered tables, picking an unfiltered table as the root silently disables the backward pass on all root-incident edges. The loop skips such tables to ensure the root can actually broadcast filters back down the tree.

---

**User**

Explain what are intermediate tables, unfiltered tables, and filtered tables (those are defined in @src/include/duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp but I don't know where the values are set).

---

**Cursor**

These three sets are populated in `TransferGraphManager::ClassifyTables()` in the `.cpp` file (lines 265-309), which is called at the end of `ExtractEdgesInfo()`. Here's the full classification logic:

```265:309:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
void TransferGraphManager::ClassifyTables() {
	for (auto &pair : table_operator_manager.table_operators) {
		auto id = pair.first;
		auto &table = pair.second;
		auto &edges = neighbor_matrix[id];
		auto &join_keys = table_join_keys[id];

		// 1. Find belong groups
		unordered_set<JoinKeyTableGroup *> belong_groups;
		for (auto &sub_pair : edges) {
			auto &edge = sub_pair.second;
			auto &join_key_group = table_groups[edge->left_binding];
			belong_groups.insert(join_key_group.get());

			// Record all join keys of this table
			if (edge->left_binding.table_index == id) {
				join_keys.insert(edge->left_binding);
			} else {
				join_keys.insert(edge->right_binding);
			}
		}

		// 2. Check Filtered Table or very small table
		if (table->type == LogicalOperatorType::LOGICAL_FILTER) {
			filtered_table.insert(id);
			continue;
		} else if (table->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = table->Cast<LogicalGet>();

			if (!get.table_filters.filters.empty()) {
				filtered_table.insert(id);
				continue;
			}
		}

		// 3. Check Intermediate Table
		if (belong_groups.size() > 1) {
			intermediate_table.insert(id);
			continue;
		}

		// 4. Last, it is an unfiltered table
		unfiltered_table.insert(id);
	}
}
```

The classification is mutually exclusive (each `continue` prevents fall-through), and the ordering matters -- `filtered` takes priority over `intermediate`, which takes priority over `unfiltered`.

## Filtered Tables

A base table that has a **local predicate** applied to it. Two cases qualify:

1. The table operator is a `LOGICAL_FILTER` (a `Filter` on top of a `Get`), meaning a predicate couldn't be pushed into storage and sits above the scan.
2. The table is a `LOGICAL_GET` with non-empty `table_filters.filters` -- i.e., predicates that *were* pushed down into the scan (zone-map / pushed-down filters).

These are the "useful" tables for predicate transfer: they generate selective information that's worth propagating. In RPT+ terminology (paper Section 3.6 / 5.1), filter construction is restricted to tables with effective predicates because building a BF on an unfiltered table rarely pays for itself.

## Intermediate Tables

A table with **no local predicate** but that belongs to **more than one join-key equivalence class** (`belong_groups.size() > 1`).

Equivalence classes are computed in `ExtractEdgesInfo` via a union-find over join conditions (lines 164-166, 249-251, 256-259): two join keys end up in the same `JoinKeyTableGroup` if they're transitively connected by equijoin conditions. If a table has edges that belong to multiple such groups, it participates in multiple classes -- and is therefore a **bridge table** in the paper's terminology (Section 3.4.2, Figure 5).

Bridge tables matter because each equivalence class in an acyclic query forms its own induced subtree, and the class's filter must be created at the class's root -- which is either the overall root or a bridge table that first introduces the class. Even without a local predicate, a bridge table is a valid point of filter creation for backward-pass broadcast, which is why `LargestRootUpdated` accepts intermediate tables as root candidates alongside filtered ones.

## Unfiltered Tables

A table with **no local predicate** that participates in only **one equivalence class**. It's neither a filter source nor a bridge.

These tables contribute nothing useful to predicate transfer: they don't produce new filtering information and they don't connect separate classes. Accordingly, `SkipUnfilteredTable` (lines 311-511) rewires the graph to bypass them:

- Step 3 (lines 391-432): adds *direct* edges between each `(carrier → t)` and `(t → dst)` pair so filters can hop across `t` without building a BF on `t`.
- Step 4 (lines 434-447): disables all outgoing directions on edges from `t`, preventing `t` from being a BF source.
- Step 5 (lines 449-458): removes any edge whose both directions are now blocked.

After this pass, unfiltered tables can still *receive* filters (useful -- they get reduced for the later hash joins) but don't *create* filters, which is precisely the forward-pass restriction described in the paper's Section 5.1.

## Quick summary

| Category | Has predicate? | Multi-class? | Role in transfer |
|---|---|---|---|
| `filtered_table` | Yes | don't care | BF source; eligible root |
| `intermediate_table` | No | Yes | Bridge; eligible root; may create BFs for equivalence-class subtrees |
| `unfiltered_table` | No | No | Bypassed; receives only |

And the three sets are then used in a few places:

- `SkipUnfilteredTable` (line 360): iterates over `unfiltered_table` to bypass them.
- `SkipUnfilteredTable` second loop (line 463): special-cases intermediate tables that have no incoming edges (disables their outgoing to avoid useless BFs).
- `LargestRootUpdated` / `THCPickRoot` (lines 566, 643): restrict root candidates to filtered ∪ intermediate.

---

**User**

Does RPT+ create BF for tables that are unfiltered? Where can I see the logic for whether the answer is yes or no?

---

**Cursor**

Short answer: **No, by design unfiltered tables do not create BFs in RPT+.** This is enforced statically during plan construction (SkipUnfilteredTable + CreateTransferPlanUpdated), not at runtime. Let me show you exactly where.

## Step 1 -- The static decision lives in `SkipUnfilteredTable`

For each unfiltered table `t`, all of `t`'s **outgoing** edge directions are turned off:

```434:447:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		// ---- Step 4: disable all outgoing edges from this unfiltered table ----
			for (auto &e : outgoing) {
				if (e->left_binding.table_index == t) {
					if (!e->protect_right) {
						e->protect_right = true; // block Left→Right
						changed = true;
					}
				} else {
					if (!e->protect_left) {
						e->protect_left = true; // block Right→Left
						changed = true;
					}
				}
			}
```

Recall: `protect_right = true` means "the L→R direction on this edge is forbidden." So for every edge incident on `t`, the direction *originating at* `t` is blocked, while the direction flowing *into* `t` is preserved (so `t` can still receive BFs in the backward pass).

Step 3 (lines 391-432) also adds bypass edges so that a filtered carrier on one side of `t` can hand its filter directly to a destination on the other side of `t`, without `t` itself being a producer.

## Step 2 -- The decision is consumed in `CreateTransferPlanUpdated`

When edges are wired into the transfer graph, BF creation only fires when the corresponding `protect_*` flag is false:

```787:807:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		// forward: from the smaller to the larger
		if (!protect_right) {
			left_node->Add(right_node->id, {left_cols}, {right_cols}, {type}, true, false);
			right_node->Add(left_node->id, {left_cols}, {right_cols}, {type}, true, true);
		}

		// backward: from the larger to the smaller
		if (!protect_left) {
			auto &group = table_groups[right_cols];
			if (group) {
				auto group_leader = group->leader_id;
				auto &leader_cols = group->leader_column_binding;
				auto leader = transfer_graph[group_leader].get();

				left_node->Add(group_leader, {left_cols}, {leader_cols}, {type}, false, true);
				leader->Add(left_node->id, {left_cols}, {leader_cols}, {type}, false, false);
			} else {
				left_node->Add(right_node->id, {left_cols}, {right_cols}, {type}, false, true);
				right_node->Add(left_node->id, {left_cols}, {right_cols}, {type}, false, false);
			}
		}
```

The fourth argument to `Add` is `is_create` (false = receiver, true = creator). Walk through any edge `(filtered_parent, unfiltered_child)`:

- After the cardinality_order swap, `left_node` is the child (`t`), `right_node` is the parent.
- For the forward pass: `protect_right` was set to `true` by `SkipUnfilteredTable` on the `t`→parent direction → branch is **skipped** → no forward BF is built on `t`.
- For the backward pass: `protect_left` is `false` (parent → `t` direction was preserved) → branch fires → the parent (or its equivalence-class leader) builds a BF and `t` receives it.

So `t` ends up only as a receiver in the wired transfer plan, never as a creator.

## Step 3 -- Intermediate tables get a similar guard

Intermediate (bridge) tables normally *do* create BFs, but there's a guard immediately after the unfiltered loop:

```462:510:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
	// 2) For each intermediate table, if it has no filter and any in-edge, it should not have out-edge
	for (auto t : intermediate_table) {
		auto &adj = neighbor_matrix[t];

		// ---- collect all incoming edges from carrier tables → t ----
		unordered_map<idx_t, vector<shared_ptr<EdgeInfo>>> incoming_by_col;
		for (const auto &p : adj) {
			const auto &nbr = p.first;
			const auto &e = p.second;

			if (!e) {
				continue;
			}
			if (can_flow(e, nbr, t)) {
				incoming_by_col[col_at(e, t)].push_back(e);
			}
		}

		if (incoming_by_col.empty()) {
			for (const auto &p : adj) {
				const auto &nbr = p.first;
				const auto &e = p.second;

				if (!e) {
					continue;
				}
				if (can_flow(e, t, nbr)) {
					if (e->left_binding.table_index == t) {
						if (!e->protect_right) {
							e->protect_right = true; // block Left→Right
						}
					} else {
						if (!e->protect_left) {
							e->protect_left = true; // block Right→Left
						}
					}
				}
			}
            ...
```

If a bridge table has no surviving incoming edges (i.e., nothing useful is going to flow into it), its outgoing edges get disabled too -- so it won't create BFs from raw, unfiltered data either.

(Side note: the comment on line 462 reads slightly inverted from what the code does; the code disables outgoing only when `incoming_by_col.empty()` -- worth a closer read if you ever rework that block.)

## Step 4 -- The dynamic runtime check is a separate, orthogonal layer

The static rules above guarantee that BF *creators* are only ever placed on filtered or bridge tables. On top of that, RPT+'s dynamic pipeline (paper Algorithm 2, Section 5.3) can additionally **abort** a BF creation at runtime even for a filtered table if the observed selectivity is too high. That logic lives in the executor (cascade-filter creator / pipeline machinery), not in this optimizer file. If you want to find it:

```bash
rg -n "tau_sel|gamma|dynamic.*pipeline|AbandonBF|cascade.*creat" src/
```

is a reasonable starting search. But for the question "do unfiltered tables ever build a BF?" -- the static answer is no, decided entirely by `SkipUnfilteredTable` and the `protect_left`/`protect_right` checks in `CreateTransferPlanUpdated`. The runtime layer can only further suppress BF creation, never re-enable it.

## TL;DR pointer

Two locations together encode the rule:

1. `SkipUnfilteredTable` (lines 311-511) -- sets `protect_*` flags on edges so that unfiltered tables can't be sources, plus inserts bypass edges so filters can hop over them.
2. `CreateTransferPlanUpdated` (lines 752-810, particularly the `if (!protect_right)` and `if (!protect_left)` guards) -- consumes those flags when emitting `Add` calls; an unfiltered table's `Add` only ever gets called with `is_create = false` (receiver), never with `is_create = true`.

---

**User**

We need to change the way the transfer graph gets created. Right now it's being done in LargestRootUpdated in @src/optimizer/predicate_transfer/transfer_graph_manager.cpp .

Instead we want to do it in a new function called TransferGraphManager::THCRootAndTransferGraph.

THCRootAndTransferGraph will enable getting all the possible root picks and transfer graph orderings (in transfer_order).

It's gonna work like this, based on some seed coming from @src/include/duckdb/main/client_config.hpp 
- First, pick ANY of the tables as root. Even tables that are unfiltered. Enumerate all the choices in a deterministic way. If (seed % <number of options>) is 0, pick the first option. If (seed % <number of options>) is 1, pick the second option, and so on. Ideally the enumeration would be based on the table names or something like that, but don't do it if it's too complicated to obtain.
- Then, we build the transfer order table by table. Every time we add a table to the transfer order, we can choose from any of the tables connected to the tables selected so far (I hope that makes sense?). To make each selection, every time we use some hash on the existing seed to get a new seed (see @src/include/duckdb/common/types/hash.hpp), and then look at (seed % <number of options>).

---

**Cursor**

Let me gather some context before drafting a plan.

I have enough context to plan the mechanics, but two integration points are ambiguous and would significantly change the plan. Let me ask before drafting.

> **Where should the new THCRootAndTransferGraph be wired in? There are currently two paths: the default RPT+ pipeline uses LargestRootUpdated via CreateTransferPlanUpdated, while the THC path uses THCPickRoot via CreateTransferPlanTHC (triggered by RebuildForTHC).**
> - Replace LargestRootUpdated inside CreateTransferPlanUpdated (affects the default path for every query)
> - Replace THCPickRoot inside CreateTransferPlanTHC (only affects the THC rebuild path; default RPT+ stays on LargestRootUpdated)
> - Add a new CreateTransferPlan wrapper (e.g. CreateTransferPlanRandomized) gated on thc_random_seed != 0; both default and THC paths can route into it
>
> Answer: I don't see a RebuildForTHC? That's on another branch, but we're scrapping that branch.

> **The current LargestRootUpdated restricts roots to filtered/intermediate tables precisely because SkipUnfilteredTable already disabled outgoing edges on unfiltered tables (so rooting there would break the backward pass on those edges). Allowing ANY table as root conflicts with this. How should we handle it?**
> - Keep SkipUnfilteredTable as-is; accept that some BF wirings will be skipped when an unfiltered root is chosen (simplest, but some configurations will silently emit degraded plans)
> - When thc_random_seed is active, do NOT run SkipUnfilteredTable in Build(), so the full edge set is available for any root choice
> - Something else -- I'll clarify in a follow-up
>
> Answer: Would it be possible to have the option to Skip Unfiltered Tables or not?

Thanks, that clarifies both points. Let me lay out the plan.

---

**User**

I don't want seed = 0 to stay on current path. It's a good idea to keep the current path, but change the plan to instead have a toggle in @src/include/duckdb/main/client_config.hpp to decide if we use RPT+ transfer order or THC transfer order.

---

**Cursor**

Plan updated. Summary of the change:

- Replaced the seed-sentinel dispatch with an explicit `thc_use_thc_transfer_order` bool (default `false` = current RPT+ / LargestRootUpdated path).
- `transfer_graph_seed` is now consulted only when `thc_use_thc_transfer_order == true`, and `seed = 0` is a fully valid THC-path configuration (picks the first option at each step).
- `thc_skip_unfiltered_tables` remains an independent toggle that applies to both modes.
- Dispatch, diagram, and scope-boundaries sections updated to match.

Ready to execute when you are.

---

**User**

Implement the plan as specified, it is attached for your reference. Do NOT edit the plan file itself.

To-do's from the plan have already been created. Do not create them again. Mark them as in_progress as you work, starting with the first one. Don't stop until you have completed all the to-dos.

---

**Cursor**

Let me look at ParamsToString to understand how to get table names deterministically.

Now let me implement `THCRootAndTransferGraph`. I'll add the includes and the function body.

Now add `THCRootAndTransferGraph` after `LargestRootUpdated`.

Now add the `CreateTransferPlanSeeded` wrapper right after `CreateTransferPlanUpdated`.

Now update `Build()` to dispatch based on config.

All warnings are pre-existing and unrelated. Let me verify `MurmurHash64` is accessible and try a quick compile check.

Good. Let me do a quick compile check of just this translation unit.

The predicate_transfer module builds cleanly. Let me also quickly verify a wider link by running a full build to make sure the client_config changes don't break anything else.

Full release build succeeds. Let me mark the final todo as completed.

All five todos are done and a full release build completes cleanly. Summary of changes:

- `src/include/duckdb/main/client_config.hpp`: three new toggles — `thc_use_thc_transfer_order` (RPT+ vs THC mode), `transfer_graph_seed` (determinism seed), and `thc_skip_unfiltered_tables` (independent control of `SkipUnfilteredTable`).
- `src/include/duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp`: declared `CreateTransferPlanSeeded(uint64_t)` and `THCRootAndTransferGraph(uint64_t &)` in the private section.
- `src/optimizer/predicate_transfer/transfer_graph_manager.cpp`:
  - Added `#include "duckdb/common/types/hash.hpp"`, `duckdb/main/client_config.hpp`, and `<algorithm>`.
  - New `THCRootAndTransferGraph`: enumerates all candidate tables sorted by `(ParamsToString-name, table_index)`, picks root via `seed % N`, advances the seed through `MurmurHash64` for every subsequent neighbor/parent pick, assigns `cardinality_order` using the same `prior_flag--` scheme as `LargestRootUpdated` (preserving the parent > child invariant the edge-wiring swap depends on), and handles disconnected components in-loop by picking a fresh root seed-deterministically when no connecting edge is available.
  - New `CreateTransferPlanSeeded` wrapper: save/restore `table_operators`, call the seeded builder, then run the same forward/backward wiring (with group-leader backward routing) as `CreateTransferPlanUpdated`.
  - `Build()` now consults `ClientConfig::GetConfig(context)`, gates `SkipUnfilteredTable` behind `thc_skip_unfiltered_tables`, and dispatches to `CreateTransferPlanSeeded` when `thc_use_thc_transfer_order` is true (default false → unchanged RPT+ path).

`LargestRootUpdated`, `CreateTransferPlanUpdated`, `FindEdge`, `ClassifyTables`, and `SkipUnfilteredTable` are untouched, so the default path stays bit-identical to before.

---

**User**

Please explain this in a lot of details:

//! The `cardinality_order` assignment mirrors LargestRootUpdated: every table
//! first receives a distinct initial value during the init loop, and each
//! table added as a child during tree building gets its value overwritten
//! with a smaller `prior_flag--`. This preserves the invariant that the
//! parent's cardinality_order is strictly greater than the child's, which
//! the "smaller table is in the left" swap in CreateTransferPlan* relies on.


---

**Cursor**

Let me read the relevant sections to ground the explanation in the actual code.

## What `cardinality_order` actually is

Despite the name, `cardinality_order` is **not** an estimated cardinality. It is an integer priority stamped onto each `GraphNode` when that node gets its place in the spanning tree. The stamping is designed to satisfy a single invariant:

> For every edge `(P, C)` kept in `selected_edges`, where `P` is the parent (the table that was already in the spanning tree) and `C` is the child (the one that was just attached), `P.cardinality_order > C.cardinality_order`.

This invariant is the entire reason the "swap" block in `CreateTransferPlan*` works:

```910:915:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		// smaller table is in the left
		if (left_node->cardinality_order > right_node->cardinality_order) {
			std::swap(left_node, right_node);
			std::swap(left_cols, right_cols);
			std::swap(protect_left, protect_right);
		}
```

The comment says "smaller table is in the left" but what the code really does is normalize every edge so that `left_node` always has the **smaller** `cardinality_order` (the child) and `right_node` always has the **larger** one (the parent). The forward pass below it emits bloom filters `left → right` (child → parent), and the backward pass emits `right → left` (parent → child). If the invariant ever broke, the forward/backward directions would flip on that edge and RPT+'s Yannakakis-style schedule would be violated.

## The two-phase stamping scheme in `LargestRootUpdated`

Look at the actual stamping code:

```598:609:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
	// Initialize nodes
	for (auto &entry : table_operator_manager.table_operators) {
		idx_t id = entry.first;
		if (id == root) {
			constructed_set.insert(id);
		} else {
			unconstructed_set.insert(id);
		}

		auto node = make_uniq<GraphNode>(id, prior_flag--);
		transfer_graph[id] = std::move(node);
	}
```

and then later during the tree-growth loop:

```621:635:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
	// Build graph
	while (!unconstructed_set.empty()) {
		auto selected_edge = FindEdge(constructed_set, unconstructed_set);
		if (selected_edge.first == std::numeric_limits<idx_t>::max()) {
			break;
		}

		auto &edge = neighbor_matrix[selected_edge.first][selected_edge.second];
		selected_edges.emplace_back(std::move(edge));

		auto node = transfer_graph[selected_edge.second].get();
		node->cardinality_order = prior_flag--;
```

Note two things:

1. `prior_flag` is declared once before both phases (`int prior_flag = N - 1;` where `N` is the number of tables) and monotonically decrements through both of them. The init loop consumes the values `N-1, N-2, …, 0` (one per table). The build loop continues with `-1, -2, -3, …` until every non-root table has been attached.
2. The init loop's call to `make_uniq<GraphNode>(id, prior_flag--)` gives every table a distinct starting `cardinality_order`, but during the build loop, the child's value is **overwritten** by `node->cardinality_order = prior_flag--`. Only the **root's** value is never overwritten, because the root is inserted into `constructed_set` before the build loop runs.

So after both phases, the values partition as:

- **Root**: some value from `[0, N-1]`, whatever the init loop happened to assign it (depends on hash-map iteration order — that's fine, see below).
- **Every other table**: a negative value in `[-(N-1), -1]`, assigned in the order that table was attached to the tree (earlier attachments get larger values, later attachments get smaller).

## Why the invariant holds

Consider any edge `(P, C)` in `selected_edges`. By construction, `C` was added to the tree **strictly after** `P`. We need to show `P.cardinality_order > C.cardinality_order`.

Case 1: `P` is the root.
- `P.cardinality_order ∈ [0, N-1]` (init value, never overwritten).
- `C` was added during the build loop, so `C.cardinality_order` is negative.
- Therefore `P > C`. ✓

Case 2: `P` was added during the build loop (so it is a non-root ancestor).
- `P.cardinality_order = prior_flag_at_time_of_P_insertion` (a negative number, call it `-k_P`).
- `C` was added **later** than `P`, so `prior_flag` had been decremented further by then.
- `C.cardinality_order = -k_C` with `k_C > k_P`.
- Therefore `P = -k_P > -k_C = C`. ✓

In both cases the parent has a strictly greater `cardinality_order` than the child, so the swap in `CreateTransferPlan*` correctly identifies who is "upstream" in the tree.

## Why the arbitrary init order does not break anything

A fair question is: the init loop iterates `table_operator_manager.table_operators`, which is an `unordered_map`, so the order is nondeterministic. Doesn't that mean the root's `cardinality_order` is nondeterministic too?

Yes — but it does not matter, because:

- For any edge `(root, C)`, both sides of the inequality are on different sides of zero (root ≥ 0, child < 0), so the inequality holds regardless of which init value the root got.
- For any edge between two non-root nodes, both values are negative and are assigned strictly in tree-growth order during the build loop. The init values of those two nodes are never consulted again (both are overwritten), so the init order is irrelevant.

The only fact the invariant relies on is that the init loop produces values **strictly greater** than any value the build loop will produce — and that is guaranteed because the build loop starts from `prior_flag = -1` (every init value is `≥ 0`).

## Why disconnected components still work in `THCRootAndTransferGraph`

This is the subtle part that only shows up in the new seeded builder. In `LargestRootUpdated`, each call handles exactly one connected component and the outer loop in `CreateTransferPlanUpdated` calls it repeatedly — each call re-runs the init loop over the **remaining** tables, so each new component's root gets a fresh non-negative init value.

`THCRootAndTransferGraph` handles every component in a single pass, so the init loop runs once. When the algorithm hits a disconnected remainder, it picks a new root from `unconstructed_set` but does **not** overwrite that new root's `cardinality_order`. That new root therefore keeps its init value, which is some number in `[0, N-1]`.

Is the invariant still preserved? Yes, because:

- Selected edges only ever connect two tables inside the **same** component (by definition of "connected").
- Within a component, the parent is either the component's root (init value `≥ 0`) or an intermediate ancestor (build-loop value `< 0`), and the child is always a later build-loop value.
- Cross-component inequalities are never consulted — there is no edge between components, so the swap block never compares nodes from different components.

That is why the comment says the scheme "mirrors LargestRootUpdated": the per-component semantics of the stamping are identical, even though the init loop now runs only once at the start instead of once per component. The negative build-loop range is strictly below every possible init value, so every parent–child comparison inside every component lands on the right side of the inequality.

## One-sentence summary

`cardinality_order` is really "insertion order into the spanning tree" encoded as an integer, with a deliberate gap (positive init range vs. negative build range) that guarantees roots sit above their descendants and each ancestor sits above each descendant — and the `CreateTransferPlan*` swap uses that purely topological ordering to orient the forward/backward bloom-filter passes correctly.

---

**User**

I feel like I need a step back here to understand what's going on. I seriously don't really understand how this optimizer works, or even the RPT+.

Please explain the key data structures in TransferGraphManager and how they interact.

---

**Cursor**

# A top-down tour of `TransferGraphManager`

Before looking at any data structures, it helps to know what the whole thing is trying to produce. Everything in `TransferGraphManager` exists to answer one question:

> Given a join query, in what order should each base table build a bloom filter, and which tables should receive it, so that by the time we start hashing tuples into real hash tables, the inputs to every join are already heavily pre-filtered?

The answer it produces is a directed graph (`transfer_graph`) plus a schedule (`transfer_order`), which the predicate-transfer executor later walks to actually push bloom filters around. Every other data structure in the class is either raw input, scratch space for classification, or intermediate plumbing to get from "raw join plan" to that final graph+schedule.

## The RPT+ pipeline in one picture

The `Build()` method runs five phases in strict order. Each phase fills in or mutates some subset of the class's members.

```
                  phase               produces / mutates
                  -----               ------------------
LogicalOperator plan
      │
      │ 1. ExtractOperators ─────────▶ table_operators, sorted_table_operators,
      │                                rename_col_bindings, + list of joins
      ▼
joins (list)
      │
      │ 2. ExtractEdgesInfo ─────────▶ neighbor_matrix  (every raw edge)
      │                                table_join_keys  (per-table join cols)
      │                                table_groups     (equivalence classes)
      │                                (calls ClassifyTables inside)
      │                                 → filtered_table / intermediate_table
      │                                   / unfiltered_table
      ▼
annotated graph
      │
      │ 3. SkipUnfilteredTable ──────▶ mutates neighbor_matrix:
      │    (optional)                  - adds "bypass" edges
      │                                - sets protect_left / protect_right
      │                                  on edges out of unfiltered tables
      ▼
pruned annotated graph
      │
      │ 4. CreateTransferPlan* ──────▶ selected_edges  (spanning-tree edges)
      │    (LargestRootUpdated or      transfer_order  (execution schedule)
      │     THCRootAndTransferGraph)   transfer_graph  (Node for each table)
      │                                also assigns cardinality_order per node
      │                                and registers leaders in table_groups
      ▼
selected tree
      │
      │ 4b. edge-wiring loop ────────▶ fills GraphNode.forward_stage_edges
      │    (same function)              and GraphNode.backward_stage_edges
      ▼
transfer_graph fully wired, ready for the executor
```

Now let me take each of the key structures and describe what it represents.

## 1. `TableOperatorManager`: the set of base tables

```13:14:src/include/duckdb/optimizer/predicate_transfer/table_operator_namager.hpp
	vector<LogicalOperator *> sorted_table_operators;
	unordered_map<idx_t, LogicalOperator *> table_operators;
```

- `table_operators`: the "vertex set" of the graph. Key = `table_index` (DuckDB's unique id for a scan/aggregate/etc in the plan), value = pointer to the `LogicalOperator` that produces rows for that vertex. These are the nodes between which bloom filters can flow.
- `sorted_table_operators`: the same set sorted by estimated cardinality. `LargestRootUpdated` walks this in reverse to find "the largest filtered table" as root; `FindEdge` uses cardinalities out of this too.
- `rename_col_bindings`: DuckDB's plan has operators like projections that rename column bindings, so a join condition written against the join's view of a column needs to be mapped back to the base table's binding. `GetRenaming()` does that translation.

A subtle point: only operators that `OperatorNeedsRelation` accepts become vertices. Join operators are **not** vertices — they are the things that will generate edges in the next phase.

## 2. `neighbor_matrix`: the raw undirected multigraph of joins

```hpp:transfer_graph_manager.hpp
unordered_map<idx_t, unordered_map<idx_t, shared_ptr<EdgeInfo>>> neighbor_matrix;
```

Indexed as `neighbor_matrix[left_table_index][right_table_index]`. Every equi-join condition `A.x = B.y` that `ExtractEdgesInfo` keeps produces one `EdgeInfo` stored at **both** `neighbor_matrix[A][B]` and `neighbor_matrix[B][A]` (the same `shared_ptr`). That symmetry is why `FindEdge` / `THCRootAndTransferGraph` can use `neighbor_matrix[constructed][unconstructed]` to discover "does any constructed table connect to this unconstructed one?"

An `EdgeInfo` carries:

```13:31:src/include/duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp
class EdgeInfo {
...
	LogicalType return_type;

	LogicalOperator &left_table;
	ColumnBinding left_binding;

	LogicalOperator &right_table;
	ColumnBinding right_binding;

	bool protect_left = false;
	bool protect_right = false;
};
```

- The two endpoints (as operator references, so the wiring phase can look them up later).
- The column on each side that is being equated — this is the column a bloom filter would be built from / probed against.
- The two `protect_*` flags: "is it forbidden to send bloom filters out of this side of the edge?" They are set by `ExtractEdgesInfo` based on join type (LEFT, RIGHT, MARK, DELIM) and later potentially set again by `SkipUnfilteredTable`. An edge with both flags set is effectively dead — neither direction can originate a BF.

So `neighbor_matrix` is the raw, unordered "which tables share equi-join columns, and with what protection rules" structure. It is never turned into the final DAG directly; the spanning-tree pass picks a subset of its edges and pushes them into `selected_edges`.

## 3. `table_join_keys` and `table_groups`: equivalence classes of join columns

```109:112:src/include/duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp
	//! From table id to its join keys
	TableJoinKeyMap table_join_keys;
	//! From join keys to its table groups
	BindingGroupMap table_groups;
```

The idea: if you have `A.x = B.x = C.x` (three-way equi-join), you don't need to build three separate bloom filters — the three columns are in the same equivalence class, and a BF built from any of them applies to all of them.

`ExtractEdgesInfo` runs a union-find over `ColumnBinding`s using each join condition as a "merge" operation. The result is a set of `JoinKeyTableGroup`s. Each group holds:

```34:56:src/include/duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp
struct JoinKeyTableGroup {
	LogicalType return_type;
	unordered_set<idx_t> table_ids;
	idx_t leader_id;
	ColumnBinding leader_column_binding;
	...
	void RegisterLeader(const idx_t table_id, const ColumnBinding &column_binding) {
		if (std::numeric_limits<idx_t>::max() == leader_id) {
			leader_id = table_id;
			leader_column_binding = column_binding;
		}
	}
```

- `table_ids`: every table participating in this equivalence class.
- `leader_id`: the first table that is added to the spanning tree and happens to own a column in this class. Set by `LargestRootUpdated` / `THCRootAndTransferGraph` via `RegisterLeader`.

`table_groups` is a `ColumnBinding → shared_ptr<JoinKeyTableGroup>` map: any column binding lets you look up the whole equivalence class. `table_join_keys` is the inverse helper: per-table, "which column bindings on this table participate in join conditions?" — that's what the builder iterates when registering a table as a leader.

Why does this matter? In the final wiring step, the **backward pass** for the group is emitted through the leader, not through the physical edge's right endpoint:

```923:932:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		// backward: from the larger to the smaller
		if (!protect_left) {
			auto &group = table_groups[right_cols];
			if (group) {
				auto group_leader = group->leader_id;
				auto &leader_cols = group->leader_column_binding;
				auto leader = transfer_graph[group_leader].get();

				left_node->Add(group_leader, {left_cols}, {leader_cols}, {type}, false, true);
				leader->Add(left_node->id, {left_cols}, {leader_cols}, {type}, false, false);
```

Without the leader indirection, an equi-join chain `A - B - C - D` on the same key would emit three independent backward bloom filters. Going through the leader collapses that into a single BF per equivalence class.

## 4. Table classifications

```113:116:src/include/duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp
	unordered_set<idx_t> unfiltered_table;
	unordered_set<idx_t> filtered_table;
	unordered_set<idx_t> intermediate_table;
```

Populated by `ClassifyTables` (called from inside `ExtractEdgesInfo`):

- `filtered_table`: has a local predicate (table filter, pushed-down `WHERE`, etc.). These are the *sources* of useful bloom filters — their predicate means they will produce a smaller subset than the full table, and that subset is what the BF encodes.
- `intermediate_table`: no local predicate, but participates in **more than one** equivalence class (a "bridge"). E.g., a fact table joined on two different foreign keys. Even without a predicate, it funnels restrictions from one equivalence class to another, so it is allowed to originate bloom filters.
- `unfiltered_table`: no local predicate and participates in only one equivalence class. If it originated a BF, that BF would encode the full table and carry no useful signal. `SkipUnfilteredTable` is the phase that enforces this by marking outgoing edges with `protect_*`.

These three sets are **not** the vertex set of anything; they are just a per-table classification that drives root choice (in `LargestRootUpdated`) and edge pruning (in `SkipUnfilteredTable`).

## 5. `selected_edges`: the spanning tree

```120:120:src/include/duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp
vector<shared_ptr<EdgeInfo>> selected_edges;
```

This is what `LargestRootUpdated` / `THCRootAndTransferGraph` produce: an ordered list of edges taken out of `neighbor_matrix` that collectively form a spanning tree (or spanning forest, if the join graph has disconnected components). The order of the list matches the order in which tables were attached to the tree.

Note the element type is the same `shared_ptr<EdgeInfo>` that lives in `neighbor_matrix`. The builder literally does `selected_edges.emplace_back(std::move(edge))` where `edge` is a reference into `neighbor_matrix[parent][child]`. After a tree is built, those slots in the matrix are null — that's intentional and doesn't matter, because the matrix is scratch and the authoritative answer lives in `selected_edges`.

## 6. `transfer_graph`: the final DAG of bloom-filter flows

```69:69:src/include/duckdb/optimizer/predicate_transfer/dag.hpp
using TransferGraph = unordered_map<idx_t, unique_ptr<GraphNode>>;
```

Every table gets one `GraphNode`:

```49:60:src/include/duckdb/optimizer/predicate_transfer/dag.hpp
class GraphNode {
public:
	GraphNode(idx_t id, int32_t priority) : id(id), cardinality_order(priority) {
	}

	idx_t id;
	int32_t cardinality_order;

	//! Predicate Transfer has two stages. The transfer graph is different because of the existence of LEFT JOIN, RIGHT
	//! JOIN, etc.
	Edges forward_stage_edges;
	Edges backward_stage_edges;
```

and each `Edges` is two `vector<unique_ptr<GraphEdge>>`:

```44:47:src/include/duckdb/optimizer/predicate_transfer/dag.hpp
struct Edges {
	vector<unique_ptr<GraphEdge>> in;
	vector<unique_ptr<GraphEdge>> out;
};
```

This is the executor-facing structure. For each node, four lists:

- `forward_stage_edges.out`: "during the forward pass, I emit a bloom filter to these nodes."
- `forward_stage_edges.in`: "during the forward pass, I expect to receive bloom filters from these nodes."
- `backward_stage_edges.out` / `.in`: same thing for the backward pass.

The forward pass runs leaf-to-root: small filtered tables build BFs that shrink larger relatives on their join key. The backward pass runs root-to-leaf: the largest/most-filtered table at the top re-broadcasts a combined BF down to all descendants. This two-pass Yannakakis schedule is the core idea of RPT+ — forward shrinks builds, backward shrinks probes — and the asymmetry between the two passes (hence separate `forward_stage_edges` and `backward_stage_edges`) is what lets RPT+ handle LEFT / RIGHT / SEMI joins correctly. An outer-join protection flag on an edge disables only the direction that would change semantics.

Populating these four lists per node is exactly what the wiring loop at the bottom of `CreateTransferPlanUpdated` / `CreateTransferPlanSeeded` does. It calls `left_node->Add(...)` and `right_node->Add(...)` with `is_forward` and `is_in_edge` booleans to push into the right of the four lists. Which node is "left" (smaller, the child in the tree) vs "right" (larger, the parent) is decided by the `cardinality_order` swap.

## 7. `transfer_order`: the walking schedule

```85:85:src/include/duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp
vector<LogicalOperator *> transfer_order;
```

The sequence of tables in the order they were attached to the spanning tree (root first, then each subsequent attachment). The executor walks this forwards for the forward pass and backwards for the backward pass. It is the execution counterpart of `selected_edges` — edges tell you what to send, `transfer_order` tells you when.

## How they all click together on one edge

Let me trace what happens to a single join condition `A.x = B.x` from start to finish, naming every structure it touches:

1. **ExtractOperators** puts `A` and `B` into `table_operators` and the containing `LOGICAL_COMPARISON_JOIN` into the `joins` list.
2. **ExtractEdgesInfo** creates one `EdgeInfo` for the condition, stores it at both `neighbor_matrix[A.id][B.id]` and `neighbor_matrix[B.id][A.id]`. It merges `A.x` and `B.x` in the union-find, producing a `JoinKeyTableGroup` with `table_ids = {A, B}`, and registers this group for both `A.x` and `B.x` in `table_groups`. It adds `A.x` to `table_join_keys[A]` and `B.x` to `table_join_keys[B]`.
3. **ClassifyTables** assigns `A` and `B` to `filtered_table`, `intermediate_table`, or `unfiltered_table` based on their filters and the number of equivalence classes they touch.
4. **SkipUnfilteredTable** (if enabled) inspects the classification and possibly sets `edge->protect_left` / `edge->protect_right` so an unfiltered side can't originate a BF.
5. **LargestRootUpdated / THCRootAndTransferGraph** picks, say, `A` as root, decides `B` is the next attachment, moves `neighbor_matrix[A][B]` into `selected_edges`, assigns `B.cardinality_order = prior_flag--`, pushes `B`'s operator into `transfer_order`, and registers `A` (or `B`, whichever came first) as leader of the equivalence class in `table_groups`.
6. **Edge-wiring loop** iterates `selected_edges`, compares `cardinality_order` to identify `A` as parent and `B` as child, then calls `A.Add(...)` and `B.Add(...)` with the right `(is_forward, is_in_edge)` booleans to populate `forward_stage_edges` and `backward_stage_edges`. For the backward pass it redirects through the leader stored in `table_groups`.
7. The executor later walks `transfer_order`, and for each node consults its `forward_stage_edges` and `backward_stage_edges` to know which BFs to build, send, and probe.

## Mental model in one line

- `neighbor_matrix` = raw join graph (every edge both directions).
- `table_join_keys` + `table_groups` = equivalence classes on join columns.
- `filtered_table` / `intermediate_table` / `unfiltered_table` = "who is allowed to originate BFs?"
- `selected_edges` + `transfer_order` = chosen spanning tree + its walk order.
- `transfer_graph` = same tree re-expressed as four per-node edge lists (forward in/out, backward in/out), which is what the executor actually reads.

Everything before step 5 is scratch — `neighbor_matrix`, union-find, classifications. Everything from step 5 onward is what survives into the executor.

---

**User**

If transfer_graph is a DAG, I don't understand how it can be of type unordered_map<idx_t, unique_ptr<GraphNode>>;
Where are the Edges saved?

---

**Cursor**

Great catch — the name `TransferGraph` is a little misleading. The map itself only stores the **vertices**; the edges live **inside** each `GraphNode` as adjacency lists. This is the classic "adjacency-list graph" representation.

Look at `GraphNode` again:

```49:61:src/include/duckdb/optimizer/predicate_transfer/dag.hpp
class GraphNode {
public:
	GraphNode(idx_t id, int32_t priority) : id(id), cardinality_order(priority) {
	}

	idx_t id;
	int32_t cardinality_order;

	//! Predicate Transfer has two stages. The transfer graph is different because of the existence of LEFT JOIN, RIGHT
	//! JOIN, etc.
	Edges forward_stage_edges;
	Edges backward_stage_edges;
```

Each node carries its own `forward_stage_edges` and `backward_stage_edges`, and `Edges` is:

```44:47:src/include/duckdb/optimizer/predicate_transfer/dag.hpp
struct Edges {
	vector<unique_ptr<GraphEdge>> in;
	vector<unique_ptr<GraphEdge>> out;
};
```

So the edges are vectors of `GraphEdge` attached to the node, and a `GraphEdge` only has a `destination` plus the per-edge metadata:

```30:42:src/include/duckdb/optimizer/predicate_transfer/dag.hpp
class GraphEdge {
public:
	explicit GraphEdge(idx_t destination) : destination(destination) {
	}

	idx_t destination;

	// The left is the smaller table side, while the right is the bigger table side
	vector<ColumnBinding> left;
	vector<ColumnBinding> right;
	vector<LogicalType> return_types;
	vector<shared_ptr<FilterPlan>> filter_plan;
};
```

Notice there is no `source` field on `GraphEdge` — the source is implicit: it's the `GraphNode` whose `in`/`out` vector owns the edge. Only the `destination` is stored.

## Walking the graph

To find "where do bloom filters flow from node `A` during the forward pass?", you do:

```cpp
auto &A = *transfer_graph.at(A_id);
for (auto &e : A.forward_stage_edges.out) {
    auto &dest = *transfer_graph.at(e->destination);
    // e describes the bloom filter being sent from A to dest
}
```

To find "what does node `A` receive during the forward pass?":

```cpp
for (auto &e : A.forward_stage_edges.in) {
    auto &src = *transfer_graph.at(e->destination);
    // e->destination is actually the *other* endpoint (the source, from A's point of view)
}
```

The `destination` field of an `in` edge is a bit of a name collision — when the edge is in the `in` vector, `destination` refers to the **other endpoint** (the partner), not "where this edge is going". The semantics are: from the perspective of the node that owns the edge, `destination` is always the partner across the edge, and `in` vs `out` tells you direction.

## Why edges are duplicated (each edge stored twice)

Every logical edge between nodes `A` and `B` is represented by **two** `GraphEdge` objects, one on each endpoint, because each endpoint needs to know about the edge from its own point of view. That is exactly what the wiring block does:

```918:921:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		// forward: from the smaller to the larger
		if (!protect_right) {
			left_node->Add(right_node->id, {left_cols}, {right_cols}, {type}, true, false);
			right_node->Add(left_node->id, {left_cols}, {right_cols}, {type}, true, true);
		}
```

Two calls, same logical edge:

- `left_node->Add(right_node->id, …, is_forward=true, is_in_edge=false)` pushes into `left_node->forward_stage_edges.out` — "I (left) send forward BFs to right."
- `right_node->Add(left_node->id, …, is_forward=true, is_in_edge=true)` pushes into `right_node->forward_stage_edges.in` — "I (right) receive forward BFs from left."

The same pattern repeats for the backward pass (lines 923-932), producing two more `GraphEdge`s. So one join condition that ends up in the spanning tree typically becomes up to 4 `GraphEdge` objects in the adjacency lists (forward `out`+`in`, backward `out`+`in`).

## Why separate `forward` and `backward` adjacency lists

The two Yannakakis stages do not necessarily have the same edges. A LEFT JOIN protects one direction, a RIGHT JOIN protects the other, `SkipUnfilteredTable` can protect outgoing edges from unfiltered tables, etc. So an edge might exist in the forward pass but not the backward pass (or vice versa). Splitting into two `Edges` structures (each with its own `in`/`out`) lets the executor iterate just the relevant subset per stage without extra filtering.

Also, in the backward pass the code may redirect the edge through the equivalence-class leader instead of through the physical join partner, as we discussed earlier. That means the *destination id* on a forward edge and a backward edge between the "same" tables can differ, which is another reason you want two physically separate edge lists.

## One-line mental model

```
transfer_graph: id → GraphNode
GraphNode:    { id, cardinality_order, forward Edges, backward Edges }
Edges:        { in: vector<GraphEdge>, out: vector<GraphEdge> }
GraphEdge:    { destination_id, join-key metadata }
```

The map is vertices. The edges are the four per-vertex `vector<unique_ptr<GraphEdge>>` adjacency lists hanging off each vertex. That is the entire DAG representation.

---

**User**

What is the meaning of the keys in TransferGraph (idx_t)? @src/include/duckdb/optimizer/predicate_transfer/dag.hpp 

---

**Cursor**

The map key is the **table’s logical “table index”** in the predicate-transfer layer — the same identifier used everywhere else in this optimizer as `GraphNode::id` and as the first component of `ColumnBinding.table_index` after renaming.

Concretely:

- `TransferGraph` is `unordered_map<idx_t, unique_ptr<GraphNode>>` where each entry is “one base relation (scan / table-producing operator) in the plan.”
- That `idx_t` is whatever `TableOperatorManager` uses as the key in `table_operators` and what gets passed into `make_uniq<GraphNode>(id, …)` when nodes are created (see `transfer_graph_manager.cpp`: `transfer_graph[entry.first] = …` with `entry.first` from `table_operator_manager.table_operators`).
- It is **not** a row id or a physical table oid; it is DuckDB’s planner binding index for “which logical table operator this is” within the optimized subtree, so edges can say “from table 3 to table 7” in a stable way.

So: **`idx_t` key = scalar table operator id / `GraphNode::id`**, aligned with `TableOperatorManager::GetScalarTableIndex` and join `ColumnBinding.table_index` (after `GetRenaming`).

---

**User**

Why is neighbor_matrix needed? Aren't we representing the graph with transfer_graph?

Also, I can think of three graphs in this entire thing:
The join tree (the logical plan in the original query)
The transfer graph
The plan tree (for example, a left deep tree of hash join)

What data structures are used for each?

---

**Cursor**

## Why `neighbor_matrix` coexists with `transfer_graph`

They exist at **different stages of the pipeline** and carry **different information**. Merging them would lose the distinction.

- `neighbor_matrix` is the **input** to planning: it is the raw, undirected adjacency of the join graph. It holds every `EdgeInfo` that any join condition produced — including edges that will never be used. It is symmetric (an edge between `A` and `B` is stored at `[A][B]` and `[B][A]` as the same `shared_ptr`), and it has no notion of forward/backward direction. Think of it as "all the edges the builder is allowed to choose from."
- `transfer_graph` is the **output** of planning: it is a chosen spanning tree re-expressed as four directed adjacency lists per node (`forward.in`, `forward.out`, `backward.in`, `backward.out`). It has no `cardinality_order`-independent sense of undirectedness, and many edges from `neighbor_matrix` are not present in it at all.

The builder's job is exactly "pick edges from `neighbor_matrix` and stamp them into `transfer_graph`." So during step 4 of `Build()` you need both:

- you read from `neighbor_matrix` to find which neighbors are reachable from the currently-constructed set (`neighbor_matrix[constructed][unconstructed]` lookup);
- you write into `transfer_graph` (and the intermediate `selected_edges`) the chosen parent → child relationships.

Also, `neighbor_matrix` stores `protect_left` / `protect_right` flags *per raw edge* before they are resolved into per-direction edges. Those flags need to survive `SkipUnfilteredTable`'s pruning pass and get consumed by the wiring loop. `transfer_graph` wouldn't naturally carry them: by the time you're building `GraphEdge`s you've already resolved the protection flags into "include this forward edge? include this backward edge?" decisions.

You could, in principle, fold both into one structure by giving `GraphNode` an "unresolved undirected neighbors" list alongside the four resolved adjacency lists, but that would muddle the lifecycle. The current split is essentially "pre-decision graph" (`neighbor_matrix`) vs "post-decision graph" (`transfer_graph`).

## The three graphs and their data structures

### 1. Logical plan tree (the query as DuckDB sees it)

This is the tree of `LogicalOperator`s coming out of DuckDB's planner — `LogicalGet`, `LogicalProjection`, `LogicalComparisonJoin`, etc. It is the input to `TransferGraphManager::Build(LogicalOperator &plan)`.

- Data structure: `LogicalOperator` with a `vector<unique_ptr<LogicalOperator>> children`. Classic parent-owns-children tree.
- In this code you see it via:
  - The `LogicalOperator &plan` argument to `Build`.
  - `TableOperatorManager::ExtractOperatorsInternal` walking `plan.children` recursively.
  - `EdgeInfo::left_table` / `right_table` (both are `LogicalOperator &`) — these are pointers into nodes of this tree.
  - `table_operators` (`unordered_map<idx_t, LogicalOperator *>`) — a flat projection onto just the "table-producing" nodes of this tree.

Strictly speaking, the "join tree" is a subset of this: the subtree induced by operators of type `LOGICAL_COMPARISON_JOIN` / `LOGICAL_DELIM_JOIN`, flattened into a `vector<reference<LogicalOperator>>` by `ExtractOperators` and returned as the `joins` list.

### 2. Join graph (viewed as an undirected graph, not a tree)

Once you look at "table A joins with table B on some column," you leave the tree world and enter the graph world. This is what `neighbor_matrix` represents.

- Data structure: `unordered_map<idx_t, unordered_map<idx_t, shared_ptr<EdgeInfo>>>`.
- Indexed by table id on both axes; entries hold the per-edge metadata (`EdgeInfo`).
- Symmetric: each logical edge is stored twice (same `shared_ptr` at `[A][B]` and `[B][A]`).
- Conceptually undirected, but `EdgeInfo::protect_left` / `protect_right` encode asymmetry that only becomes meaningful later.

Alongside this live the equivalence-class overlay structures (`table_join_keys`, `table_groups`, `JoinKeyTableGroup`), which are really "coloring" on top of the join graph telling you which vertices share a join-key equivalence class.

### 3. Transfer graph (the bloom-filter flow DAG)

The output of the planning phase. This is where `transfer_graph` lives.

- Data structure: `TransferGraph = unordered_map<idx_t, unique_ptr<GraphNode>>`, with edges stored as adjacency lists inside each `GraphNode` (`forward_stage_edges.in/out`, `backward_stage_edges.in/out`).
- Each logical edge appears up to four times (forward.out on source, forward.in on destination, backward.out on source, backward.in on destination).
- Directedness is baked in (an edge only exists in the list of the endpoint it's attached to with the correct `is_forward`/`is_in_edge` combination).
- The intermediate `selected_edges` (`vector<shared_ptr<EdgeInfo>>`) is a flat ordered list of "which raw edges were chosen for the spanning tree," and `transfer_order` (`vector<LogicalOperator *>`) is the walking schedule across the tree. Together these feed the wiring loop that actually populates `transfer_graph`.

### 4. Physical plan tree (the hash-join tree after optimization)

This is **not** produced or touched by `TransferGraphManager` at all — predicate-transfer planning runs on the logical plan before physical planning. But since you asked:

- DuckDB's physical plans use `PhysicalOperator` with `vector<unique_ptr<PhysicalOperator>> children`. Same parent-owns-children tree shape as the logical plan.
- The join order / left-deep vs. bushy shape of this tree is decided by `JoinOrderOptimizer`, not by `TransferGraphManager`.
- `TransferGraphManager` produces `FilterPlan`s and a transfer schedule that are later consumed during execution — the bloom filters get installed on scans and hash-join build sides — but the tree shape itself is unaffected.

## Summary table

| "Graph" | Purpose | Topology | Data structure |
|---|---|---|---|
| Logical plan tree | What DuckDB's planner gives you | Tree | `LogicalOperator` with `children` |
| Join graph | "Which tables have equi-join conditions between them?" | Undirected (multi)graph | `neighbor_matrix` (+ `table_join_keys`, `table_groups` overlays) |
| Transfer graph | "In what order and direction do bloom filters flow?" | Directed spanning tree (forest, with up to two direction layers) | `transfer_graph` (vertices in a map, edges in per-node adjacency lists) + `transfer_order`, `selected_edges` |
| Physical plan tree | The executable shape of the query (e.g., left-deep hash joins) | Tree | `PhysicalOperator` with `children` — decided elsewhere, not here |

So: `neighbor_matrix` and `transfer_graph` are not redundant. They represent the join graph at different stages — before and after the spanning-tree + direction + leader-indirection decisions — and the builder is precisely the thing that converts one into the other.

---

**User**

The way I learned it at CMU:
The "Join Tree" is the tree where each edge is a join predicate.
The "Plan Tree" is the tree where each edge is an actual join operator (ie the query plan).

Where does the Logical Plan Tree provided by DuckDB fall in that? Are you saying that we basically deduct the neighbor_matrix / transfer_graph (which have to do with the Join Tree) from a Logical Plan Tree which is basically a Plan Tree?


---

**Cursor**

Yes — that is exactly right, and it's a useful reframing.

## How DuckDB's "Logical Plan Tree" maps to CMU's taxonomy

DuckDB's `LogicalOperator` tree is a **Plan Tree** in your CMU sense. By the time `PredicateTransferOptimizer` runs, the `JoinOrderOptimizer` has already fired, so every join in the tree is a concrete binary operator with a specific left child and a specific right child — there is a committed join **order**, not just a declarative join condition. The only thing that hasn't happened yet is physical operator selection (hash join vs merge join, side-of-build choice, etc.). So "logical plan tree" = "plan tree that has not yet been lowered to physical operators" — still a plan tree.

The distinction the literature often draws is:

- **Join Tree / Query Graph**: pure, declarative. Vertices = base relations, edges = join predicates. No order. Often drawn as an undirected (hyper)graph.
- **Plan Tree**: operational. Vertices = operators, edges = data flow. A specific shape (left-deep, bushy, etc.) that realizes one particular evaluation order for the join graph.
- **Physical Plan Tree**: the plan tree after costing and operator choice.

DuckDB's logical plan tree sits at the middle tier — it's a plan tree whose operators are still "logical" (abstract `LOGICAL_COMPARISON_JOIN` rather than `PHYSICAL_HASH_JOIN_BUILD` / `…_PROBE`), but which already commits to a join order.

## What `TransferGraphManager` actually does, in these terms

You nailed it: the optimizer takes the logical **plan tree** as input and reconstructs the **join tree (graph)** out of it. Specifically, `ExtractOperators` walks the plan tree top-down, collecting:

1. The set of base-relation producers into `table_operators` (the vertices of the join graph).
2. Every `LOGICAL_COMPARISON_JOIN` / `LOGICAL_DELIM_JOIN` it encounters into a flat `vector<reference<LogicalOperator>>` called `joins` — **flattening the tree into a bag of join nodes**.

Then `ExtractEdgesInfo` throws away the tree-shape entirely and iterates that bag of joins, producing `neighbor_matrix`. The join order encoded in the plan tree is effectively discarded — what survives is just "table A is equated with table B on column c in some join in the query." That is the undirected join graph.

So the flow is:

```
logical plan tree (a plan tree, already has join order)
        │
        │  ExtractOperators flattens joins
        │  ExtractEdgesInfo forgets ordering, collects conditions
        ▼
neighbor_matrix (the query's undirected join graph)
        │
        │  spanning-tree builder picks a root & attachments
        ▼
transfer_graph (a new, RPT+-specific directed spanning tree
                 over the same vertex set, entirely unrelated
                 to the plan tree's shape)
```

## The three trees do not have to agree

An important consequence: the **transfer graph's tree shape has nothing to do with the plan tree's shape.** The plan tree might be left-deep `(((A⋈B)⋈C)⋈D)`, and the transfer graph might root at `C` with children `B` and `D`, and `B` with child `A`. RPT+ does not care which operator in the plan tree is "on top"; it cares which base table is the best place to originate bloom filters.

Similarly, the *physical* plan tree that the rest of the optimizer pipeline builds afterwards is a fourth thing again — and the transfer graph's edges don't become operators in that tree. They become metadata attached to existing operators (build sides of hash joins get bloom-filter creates, scans get bloom-filter probes). The transfer graph never materializes as a tree of operators; it is a *schedule overlay* on the physical plan.

## Summary in one line

> DuckDB's logical plan tree is a plan tree (join order already fixed). `TransferGraphManager` extracts the underlying join graph from it by flattening the joins, and then builds its own spanning tree over that graph — the transfer graph — which is independent of the plan tree's shape.

---

**User**

Each edge in neighbor_matrix is a EdgeInfo. Does each edge represent:
A) A join in the original query or join tree
B) A transfer
C) Both
C) something else?

---

**Cursor**

Answer: **(D) something else.** It's a **join-graph candidate edge**, not a physical operator and not (yet) a transfer.

## What an `EdgeInfo` actually represents

Look at the inner loop in `ExtractEdgesInfo`:

```198:227:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
		for (auto &cond : comp_join.conditions) {
			// Only equal predicates between two column refs are supported
			if (cond.comparison != ExpressionType::COMPARE_EQUAL ||
			    cond.left->type != ExpressionType::BOUND_COLUMN_REF ||
			    cond.right->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			...
			// Create edge
			auto edge =
			    make_shared_ptr<EdgeInfo>(cond.left->return_type, *left_node, left_binding, *right_node, right_binding);
```

The unit of iteration is a `JoinCondition`, not a join operator. So:

- An `EdgeInfo` corresponds to **one equi-join condition** between two base tables — i.e., one "table A's column x must equal table B's column y" predicate — filtered to only `COMPARE_EQUAL` between two `BOUND_COLUMN_REF`s.
- A single `LogicalComparisonJoin` with multiple conditions (`A.x=B.x AND A.y=B.y`) produces multiple `EdgeInfo` *attempts*. But because the storage is a map `neighbor_matrix[a][b] = edge;` (overwriting assignment, not append), the map keeps only the **last** condition per unordered pair of tables. So effectively what survives is "one representative equi-condition per table pair," plus the `protect_left`/`protect_right` flags copied from that join's join-type.
- Conversely, one `EdgeInfo` does **not** correspond to one join operator either: two different join nodes in the plan tree that happen to equi-join the same pair of tables will collide in `neighbor_matrix` and only the last one written wins.

So it's not a "join." It is a candidate **edge in the query's join graph**, parameterized by one equi-condition.

## It is also not a transfer (yet)

Two reasons:

1. **It may not become a transfer at all.** The spanning-tree builder only selects a subset of `neighbor_matrix` entries into `selected_edges`. Edges between tables that are already connected through the tree are dropped. A candidate `EdgeInfo` that is not chosen produces zero transfers.
2. **If it does become a transfer, it produces up to four of them.** When the wiring loop fires, one chosen `EdgeInfo` expands into up to four `GraphEdge`s in the transfer graph: forward-out on the child, forward-in on the parent, backward-out on the parent, backward-in on the child. Fewer if `protect_left` / `protect_right` suppresses a direction.

## Mental picture

```
EdgeInfo in neighbor_matrix         =  one potential transfer channel
                                       (undirected, carries metadata:
                                        columns, return type, protection)

     │   spanning-tree builder
     │   keeps a subset
     ▼
EdgeInfo in selected_edges          =  one chosen channel
                                       (still undirected; parent/child
                                        identified via cardinality_order)

     │   wiring loop emits
     │   forward + backward
     ▼
up to 4 GraphEdges in transfer_graph =  the actual directed transfers
```

So: an `EdgeInfo` is best thought of as **a potential transfer arising from an equi-join condition in the query**. It is neither a physical join nor a transfer. It is the join graph's edge representation — the candidate pool the transfer planner chooses from.

---

**User**

1. Where is the spanning-tree builder?
2. Explain how join condition deduplication work in ExtracctEdgesInfo()
3. Explain the union-find (including its purpose) in ExtractEdgesInfo

---

**Cursor**

Good, I have the full picture now.

## 1. Where is the spanning-tree builder?

There are two of them, both are the functions we added/discussed earlier. They live in `transfer_graph_manager.cpp`:

- `TransferGraphManager::LargestRootUpdated(...)` — the RPT+ paper path (greedy, cardinality-driven). Called in a loop from `CreateTransferPlanUpdated` (one call per connected component).
- `TransferGraphManager::THCRootAndTransferGraph(uint64_t &seed)` — the new seed-driven path. Called once from `CreateTransferPlanSeeded` (handles all components in one pass).

Both do the same job: read candidate edges out of `neighbor_matrix`, pick a root, grow a spanning tree one edge at a time, and append each chosen edge into `selected_edges` while pushing each chosen table into `transfer_order`. They also stamp `cardinality_order` on the corresponding `GraphNode`s so the later wiring loop knows parent vs child.

The actual call chain is `Build()` → `CreateTransferPlan{Updated,Seeded}()` → spanning-tree builder.

## 2. Join-condition deduplication

The deduplication happens via `existed_set` at the top of `ExtractEdgesInfo`:

```177:210:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
	// Deduplicate join conditions
	unordered_set<hash_t> existed_set;
	auto ComputeConditionHash = [](const JoinCondition &cond) {
		return cond.left->Hash() + cond.right->Hash();
	};
	...
		for (auto &cond : comp_join.conditions) {
			// Only equal predicates between two column refs are supported
			if (cond.comparison != ExpressionType::COMPARE_EQUAL ||
			    cond.left->type != ExpressionType::BOUND_COLUMN_REF ||
			    cond.right->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}

			// Skip duplicate conditions
			hash_t hash = ComputeConditionHash(cond);
			if (!existed_set.insert(hash).second) {
				continue;
			}
```

What it is doing:

- For every `JoinCondition`, compute a hash = `cond.left->Hash() + cond.right->Hash()` (expression hash of the left side plus expression hash of the right side).
- `insert(hash).second` is `true` if the hash was newly inserted, `false` if it was already there. If it was already there, `continue` — skip this condition.

So the dedup key is **"the pair of expressions on either side of an equi-condition,"** treated as an *unordered pair* (because `+` is commutative, so `A.x = B.y` and `B.y = A.x` collide). The intent is: if two join nodes in the plan tree contain the literally-same equi-condition on the same two column refs, only the first one creates an `EdgeInfo`; the second is skipped.

Two caveats worth knowing:

- It dedupes **conditions**, not **table pairs**. Two different conditions on the same pair (`A.x = B.x` and `A.y = B.y`) have different hashes, so both pass the dedup check. But then they both try to write into `neighbor_matrix[A][B]`, and the second one overwrites the first in the map. That's a separate "collapse" step from the dedup above — the dedup survives the hashset, the overwrite is in the map storage.
- The hash is cond-expression-based (uses DuckDB's `Expression::Hash()`), so it is robust to pointer reuse but collides on semantic equality of the two operand expressions; ordering of left/right is intentionally folded away by the `+`.

Why is duplicate condition elimination needed at all? Because DuckDB's `JoinOrderOptimizer` sometimes emits the same logical predicate in more than one place (e.g., equivalent predicates derived from transitive closure, or repeated delim-join conditions), and feeding those into the builder would double-count the same join-graph edge.

## 3. Union-find in `ExtractEdgesInfo`

### Purpose

The union-find is **not** for the spanning tree. It builds join-key **equivalence classes** — what the header calls `JoinKeyTableGroup`s. The goal is to answer this question:

> Given a column binding like `A.x`, which other column bindings across the whole query are transitively equi-joined with it? And which tables are involved in that equivalence class?

So if the query has `A.x = B.x`, `B.x = C.x`, the union-find discovers `{A.x, B.x, C.x}` is one class and `{A, B, C}` are all in that class's `JoinKeyTableGroup`. Later, in the wiring loop:

```cpp
auto &group = table_groups[right_cols];
...
left_node->Add(group_leader, {left_cols}, {leader_cols}, ...);
```

the **backward pass** is redirected through the group's "leader" table instead of the immediate join partner, so the whole equivalence class receives a single bloom filter from the leader rather than one BF per physical edge. Without equivalence classes you'd emit redundant BFs along the chain.

### Data structures involved

```cpp
using BindingParentMap = unordered_map<ColumnBinding, ColumnBinding, ColumnBindingHashFunc>;
using BindingGroupMap  = unordered_map<ColumnBinding, shared_ptr<JoinKeyTableGroup>, ColumnBindingHashFunc>;
```

- `binding_parents` is the classic parent-pointer forest over `ColumnBinding`s. `parents[x]` is x's parent; if `x` is absent it is its own root.
- `group_map` maps each *root* binding to its current `JoinKeyTableGroup` (a set of table ids in this class).

### The two operations

**Find with path compression** (`FindBindingRoot`):

```15:26:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
static ColumnBinding FindBindingRoot(const ColumnBinding &binding, BindingParentMap &parents) {
	auto it = parents.find(binding);
	if (it == parents.end()) {
		return binding;
	}
	ColumnBinding root = it->second;
	if (root != binding) {
		root = FindBindingRoot(root, parents);
		parents[binding] = root; // Path compression
	}
	return root;
}
```

If `binding` has no parent recorded, it's its own root. Otherwise walk upward; on the way back, install the ultimate root as each visited node's parent (path compression, so future finds are near-`O(1)`).

**Union** (`UnionBindings`):

```28:51:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
static void UnionBindings(const ColumnBinding &a, const ColumnBinding &b, const LogicalType &type,
                          BindingParentMap &parents, BindingGroupMap &group_map) {
	ColumnBinding root_a = FindBindingRoot(a, parents);
	ColumnBinding root_b = FindBindingRoot(b, parents);
	if (root_a == root_b) {
		return;
	}

	// Union by attaching b to a
	parents[root_b] = root_a;

	auto &group_a = group_map[root_a];
	if (!group_a) {
		group_a = make_shared_ptr<JoinKeyTableGroup>(type, a.table_index);
	}

	auto &group_b = group_map[root_b];
	if (!group_b) {
		group_b = make_shared_ptr<JoinKeyTableGroup>(type, b.table_index);
	}

	group_a->Union(*group_b);
	group_map[root_a] = group_a;
}
```

Given two bindings from a condition `a = b`:

1. Find both roots. If already equal → same class, nothing to do.
2. Attach `root_b` to `root_a` in the parent map. (No rank balancing — straightforward naïve union.)
3. Lazily create the `JoinKeyTableGroup` for each side if it doesn't exist yet. Each group starts with its own binding's `table_index`.
4. Merge `group_b` into `group_a` via `JoinKeyTableGroup::Union` (which unions the `table_ids` sets) and record `group_a` as the group for `root_a`.

Note the group is only stored against the *root*, not against every member — intentional, so after a union the "defunct" root's group entry becomes stale but irrelevant.

### When is `UnionBindings` called?

```269:272:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
			// Merge groups if not protected
			if (!edge->protect_left && !edge->protect_right) {
				UnionBindings(left_binding, right_binding, cond.left->return_type, binding_parents, group_map);
			}
```

Only for **fully-unprotected** edges. This is important: if an edge has `protect_left` or `protect_right` set (LEFT / RIGHT / MARK / DELIM), the two sides are *not* truly interchangeable — bloom filters are one-way on that edge — so merging them into one equivalence class would be semantically wrong. Protected edges still produce an `EdgeInfo` (and so still exist in `neighbor_matrix`), but they do not contribute to equivalence classes.

### Finalization

After the main loop, `group_map` is keyed on whatever was a root *at the time of each union*, but subsequent unions may have moved some of those roots under other roots. So there is a normalization pass:

```277:280:src/optimizer/predicate_transfer/transfer_graph_manager.cpp
	// Finalize table_groups by resolving root bindings
	for (auto &entry : group_map) {
		ColumnBinding rep = FindBindingRoot(entry.first, binding_parents);
		table_groups[entry.first] = group_map[rep];
	}
```

For every binding that ever appeared in `group_map`, look up its current root and store `table_groups[binding] = group_for_current_root`. The result: `table_groups` is a `ColumnBinding → shared_ptr<JoinKeyTableGroup>` map where *every* binding in an equivalence class points at the **same** shared group object (all aliases share one `shared_ptr`). This is what lets `ClassifyTables` and the later wiring loop do `table_groups[some_binding]` and get "the equivalence class this binding belongs to" in `O(1)`.

### One-sentence summary per structure

- `binding_parents`: union-find internals, parent pointers over column bindings.
- `group_map`: root-only map to the per-class table-id set during construction.
- `table_groups`: finalized, flattened view where every binding maps to its class — the thing the rest of the optimizer actually reads.

So the union-find is there purely to produce `table_groups` (equivalence classes of equi-joined columns, along with the tables participating in each class), so that the transfer-graph wiring can collapse chains of equalities into a single bloom-filter broadcast per class instead of one BF per physical edge.

---

**User**

Does the union find only affect what filters are passed in the backward pass, or also in the forward pass?

---

**Cursor**

## Direct answer

**Only the backward pass.** The forward-pass emission (lines 929-933) does not consult `table_groups` — it emits one BF per selected edge, along the child → parent direction, using the two columns that actually appear in that edge's condition. The backward-pass emission (lines 935-949) is the only place `table_groups` is looked up, and that is where the leader indirection collapses a chain of equi-joined columns into a single broadcast.

## But there is an indirect effect on the forward pass too

The equivalence classes built by the union-find also drive `ClassifyTables`:

```cpp
// 1. Find belong groups
unordered_set<JoinKeyTableGroup *> belong_groups;
for (auto &sub_pair : edges) {
    auto &edge = sub_pair.second;
    auto &join_key_group = table_groups[edge->left_binding];
    belong_groups.insert(join_key_group.get());
    ...
}
...
// 3. Check Intermediate Table
if (belong_groups.size() > 1) {
    intermediate_table.insert(id);
}
```

The classifier uses the number of distinct `JoinKeyTableGroup`s that a table touches to decide whether a predicate-less table is an "intermediate" (bridge) table or an "unfiltered" table. If it is classified as `unfiltered_table`, then `SkipUnfilteredTable` will set `protect_*` flags on its outgoing edges, which will in turn suppress the **forward** `!protect_right` branch for those edges. That's the forward pass being affected — but indirectly, via classification, not via leader redirection.

## One-sentence summary

The union-find's direct effect is purely on the backward pass (leader indirection collapses an equivalence class into one BF broadcast). It affects the forward pass only *indirectly*, by letting `ClassifyTables` count equivalence-class membership to decide which tables become "intermediate" vs "unfiltered," which then changes which forward edges get their `protect_*` flags set.

