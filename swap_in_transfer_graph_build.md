# Cardinality swaps in transfer graph

*Exported on 4/21/2026 at 17:25:02 EDT from Cursor (3.0.12)*

---

**User**

This project implements this paper [https://yimingqiao.github.io/files/rpt_plus.pdf](https://yimingqiao.github.io/files/rpt_plus.pdf)

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