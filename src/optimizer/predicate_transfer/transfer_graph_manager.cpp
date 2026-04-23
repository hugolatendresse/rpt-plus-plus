#include "duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <algorithm>
#include <queue>

namespace duckdb {
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

bool TransferGraphManager::Build(LogicalOperator &plan) {
	// 1. Extract all operators, including table operators and join operators
	const vector<reference<LogicalOperator>> joins = table_operator_manager.ExtractOperators(plan);
	if (table_operator_manager.table_operators.size() < 2) {
		return false;
	}

	// 2. Getting graph edges information from join operators
	ExtractEdgesInfo(joins);
	if (neighbor_matrix.empty()) {
		return false;
	}

	auto &cfg = ClientConfig::GetConfig(context);

	// 3. Unfiltered tables only receive Bloom filters, they will not generate Bloom filters.
	//    Gated by skip_unfiltered_tables so the edge-pruning pass can be
	//    toggled independently of the transfer-order strategy. Disabling it
	//    lets THCRootAndTransferGraph explore plans where unfiltered tables
	//    also create BFs.
	if (cfg.skip_unfiltered_tables) {
		SkipUnfilteredTable(joins);
	}

	// 4. Create the transfer graph. The mode toggle is explicit: the RPT+ path 
	//    runs LargestRootUpdated (paper behavior), while the seeded
	//    path runs the seed-driven THCRootAndTransferGraph via
	//    CreateTransferPlanSeeded. The seed value itself (including 0) only
	//    affects which plan is enumerated within the seeded mode.
	if (cfg.use_seeded_transfer_order) {
		CreateTransferPlanSeeded(cfg.thc_transfer_graph_seed);
	} else {
		CreateTransferPlanUpdated();
	}

	return true;
}

void TransferGraphManager::AddFilterPlan(idx_t create_table, const shared_ptr<FilterPlan> &filter_plan, bool reverse) {
	bool is_forward = !reverse;

	D_ASSERT(!filter_plan->apply.empty());
	auto &expr = filter_plan->apply[0];
	auto node_idx = expr.table_index;
	transfer_graph[node_idx]->Add(create_table, filter_plan, is_forward, true);
}

void TransferGraphManager::PrintTransferPlan() {
	// Output table groups
	unordered_set<JoinKeyTableGroup *> visited;
	for (auto &pair : table_groups) {
		auto &group = pair.second;
		if (visited.count(group.get())) {
			continue;
		}
		visited.insert(group.get());
		group->Print();
	}

	// Local helper to get operator name
	auto GetName = [](LogicalOperator &op) -> string {
		string ret;
		auto params = op.ParamsToString();

		if (params.contains("Table")) {
			ret = params.at("Table");
		} else {
			ret = "Unknown";
		}

		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			if (get.table_filters.filters.empty()) {
				ret += " (No Filter)";
			}
		}
		return ret;
	};

	std::cout << "digraph G {" << '\n';

	// Create a map to store outgoing neighbors for each LogicalOperator
	std::unordered_map<std::string, std::vector<std::pair<std::string, std::pair<int, int>>>> outgoing_neighbors;

	// Populate the outgoing_neighbors map by traversing the edges
	for (const auto &edge : selected_edges) {
		std::string left_name = GetName(edge->left_table);
		std::string right_name = GetName(edge->right_table);
		idx_t left_column_id = edge->left_binding.column_index;   // Get the column id for the left table
		idx_t right_column_id = edge->right_binding.column_index; // Get the column id for the right table

		// Check the protection flags and only allow outgoing edges if the table is not protected
		if (!edge->protect_right) {
			outgoing_neighbors[left_name].emplace_back(right_name, std::make_pair(left_column_id, right_column_id));
		}
		if (!edge->protect_left) {
			outgoing_neighbors[right_name].emplace_back(left_name, std::make_pair(right_column_id, left_column_id));
		}
	}

	// Output nodes (LogicalOperators) and their outgoing neighbors (edges)
	for (const auto &op : transfer_order) {
		std::string op_name = GetName(*op); // Use GetName function for operator name
		std::cout << "\t\"" << op_name << "\";\n";

		// Output edges to each neighbor (only outgoing edges)
		if (outgoing_neighbors.find(op_name) != outgoing_neighbors.end()) {
			for (const auto &neighbor : outgoing_neighbors[op_name]) {
				std::string neighbor_name = neighbor.first;
				int left_column_id = neighbor.second.first;
				int right_column_id = neighbor.second.second;

				// Print edge with column ids
				std::cout << "\t\t\"" << op_name << "\" -> \"" << neighbor_name << "\" [label=\"Column "
				          << left_column_id << " -> Column " << right_column_id << "\"];\n";
			}
		}
	}

	std::cout << "}"
	          << "\n";
}

//! Go through list of join operators and grab all the edges, ie all the equi-join conditions.
//! Populate neighbor_matrix with all those conditions
//!
//! We use union-find to build join-key equivalence classes (ie JoinKeyTableGroup) into `table_groups`.
//! An equivalent class is basically one column's value: columns from tables that are expected to have the
//! same value based on equijoin predicates are in the same equivalence class.
//! NOTE: union-find / equivalence classes:
//! - directly affect the backward pass, which collapses chains of equijoins into a single broadcast  
//! - indirectly affects the forward pass since tables can be 'unfiltered' or 'intermediate' based on
//!   whether they are in multiple equivalent classes.   
void TransferGraphManager::ExtractEdgesInfo(const vector<reference<LogicalOperator>> &join_operators) {
	// Deduplicate join conditions.
	// Two join predicates are the same if they involve the same columns from the same tables (in any order)
	unordered_set<hash_t> existed_set;
	auto ComputeConditionHash = [](const JoinCondition &cond) {
		return cond.left->Hash() + cond.right->Hash();
	};

	// Union-Find structures
	BindingParentMap binding_parents;
	BindingGroupMap group_map;

	for (auto &join_ref : join_operators) {
		auto &join = join_ref.get();

		if (join.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		    join.type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			continue;
		}

		auto &comp_join = join.Cast<LogicalComparisonJoin>();
		D_ASSERT(comp_join.expressions.empty());

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

			auto &left_expr = cond.left->Cast<BoundColumnRefExpression>();
			auto &right_expr = cond.right->Cast<BoundColumnRefExpression>();

			ColumnBinding left_binding = table_operator_manager.GetRenaming(left_expr.binding);
			ColumnBinding right_binding = table_operator_manager.GetRenaming(right_expr.binding);

			auto left_node = table_operator_manager.GetTableOperator(left_binding.table_index);
			auto right_node = table_operator_manager.GetTableOperator(right_binding.table_index);

			if (!left_node || !right_node) {
				continue;
			}

			// Create edge
			auto edge =
			    make_shared_ptr<EdgeInfo>(cond.left->return_type, *left_node, left_binding, *right_node, right_binding);

			// Set edge protection flags based on join type
			switch (comp_join.type) {
			case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
				switch (comp_join.join_type) {
				case JoinType::LEFT:
					edge->protect_left = true;
					break;
				case JoinType::RIGHT:
					edge->protect_right = true;
					break;
				case JoinType::MARK:
					edge->protect_right = true;
					break;
				case JoinType::INNER:
				case JoinType::SEMI:
				case JoinType::RIGHT_SEMI:
					break;
				default:
					continue; // Skip unsupported types
				}
				break;

			case LogicalOperatorType::LOGICAL_DELIM_JOIN:
				if (comp_join.delim_flipped == 0) {
					edge->protect_left = true;
				} else {
					edge->protect_right = true;
				}
				break;

			default:
				continue;
			}

			// Store bidirectional edge
			neighbor_matrix[left_binding.table_index][right_binding.table_index] = edge;
			neighbor_matrix[right_binding.table_index][left_binding.table_index] = edge;

			// Merge groups if not protected
			if (!edge->protect_left && !edge->protect_right) {
				UnionBindings(left_binding, right_binding, cond.left->return_type, binding_parents, group_map);
			}
		}
	}

	// Finalize table_groups by resolving root bindings
	for (auto &entry : group_map) {
		ColumnBinding rep = FindBindingRoot(entry.first, binding_parents);
		table_groups[entry.first] = group_map[rep];
	}

	// Classify all tables into three categories: intermediate table, unfiltered table, and filtered table.
	ClassifyTables();
}

// Classify all tables into three categories: intermediate table, unfiltered table, and filtered table.
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

void TransferGraphManager::SkipUnfilteredTable(const vector<reference<LogicalOperator>> &joins) {
	// Helper: check whether data can flow from one table to another
	auto can_flow = [&](const shared_ptr<EdgeInfo> &e, idx_t from_tbl, idx_t to_tbl) -> bool {
		if (e->left_binding.table_index == from_tbl && e->right_binding.table_index == to_tbl) {
			return !e->protect_right; // Left → Right allowed if not protected
		}
		if (e->right_binding.table_index == from_tbl && e->left_binding.table_index == to_tbl) {
			return !e->protect_left; // Right → Left allowed if not protected
		}
		return false;
	};

	// Helper: get the column index corresponding to table `t` in edge `e`
	auto col_at = [&](const shared_ptr<EdgeInfo> &e, idx_t t) -> idx_t {
		return (e->left_binding.table_index == t) ? e->left_binding.column_index : e->right_binding.column_index;
	};

	// Helper: insert new edge into neighbor_matrix, merging if already exists
	auto upsert_edge = [&](const shared_ptr<EdgeInfo> &ne) {
		idx_t i = ne->left_binding.table_index;
		idx_t j = ne->right_binding.table_index;
		auto &ij = neighbor_matrix[i][j];
		auto &ji = neighbor_matrix[j][i];
		bool exists = false;

		if (ij) {
			bool same = (ij->left_binding == ne->left_binding && ij->right_binding == ne->right_binding);
			bool rev = (ij->left_binding == ne->right_binding && ij->right_binding == ne->left_binding);
			if (same) {
				ij->protect_left &= ne->protect_left;
				ij->protect_right &= ne->protect_right;
				exists = true;
			} else if (rev) {
				ij->protect_left &= ne->protect_right;
				ij->protect_right &= ne->protect_left;
				exists = true;
			}
		}
		if (!exists) {
			ij = ne;
			ji = ne;
		}
	};

	// 1) Iteratively skip unfiltered tables until no new edges are added.
	bool changed;
	do {
		changed = false;

		for (auto t : unfiltered_table) {
			auto &adj = neighbor_matrix[t];

			// ---- Step 1: collect all incoming edges from carrier tables → t ----
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

			// ---- Step 2: collect all outgoing edges t → neighbor ----
			vector<shared_ptr<EdgeInfo>> outgoing;
			for (const auto &p : adj) {
				const auto &nbr = p.first;
				const auto &e = p.second;

				if (!e) {
					continue;
				}
				if (can_flow(e, t, nbr)) {
					outgoing.push_back(e);
				}
			}

			// ---- Step 3: bypass t ----
			// For every (carrier → t) and (t → dst) pair with the same join column,
			// create a direct edge (carrier → dst).
			for (auto &in_group : incoming_by_col) {
				for (auto &out_e : outgoing) {
					if (col_at(out_e, t) != in_group.first) {
						continue; // must be same join key
					}

					auto &in_e = in_group.second[0]; // representative incoming edge

					shared_ptr<EdgeInfo> ne;
					if (out_e->left_binding.table_index == t) {
						// t is left side of out_e: disable Left→Right direction later
						if (in_e->left_binding.table_index == t) {
							// src is Right side of in_e
							ne = make_shared_ptr<EdgeInfo>(out_e->return_type, in_e->right_table, in_e->right_binding,
							                               out_e->right_table, out_e->right_binding);
						} else {
							// src is Left side of in_e
							ne = make_shared_ptr<EdgeInfo>(out_e->return_type, in_e->left_table, in_e->left_binding,
							                               out_e->right_table, out_e->right_binding);
						}
						ne->protect_left = true; // disable reverse flow (R → L)
					} else {
						// t is right side of out_e
						if (in_e->left_binding.table_index == t) {
							// src is Right side of in_e
							ne = make_shared_ptr<EdgeInfo>(out_e->return_type, in_e->right_table, in_e->right_binding,
							                               out_e->left_table, out_e->left_binding);
						} else {
							// src is Left side of in_e
							ne = make_shared_ptr<EdgeInfo>(out_e->return_type, in_e->left_table, in_e->left_binding,
							                               out_e->left_table, out_e->left_binding);
						}
						ne->protect_right = true; // disable reverse flow (L → R)
					}

					upsert_edge(ne);
					changed = true;
				}
			}

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

			// ---- Step 5: remove fully blocked edges (no direction left) ----
			for (auto it = adj.begin(); it != adj.end();) {
				auto &e = it->second;
				if (e && e->protect_left && e->protect_right) {
					it = adj.erase(it);
					changed = true;
				} else {
					++it;
				}
			}
		}
	} while (changed);

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

			for (auto it = adj.begin(); it != adj.end();) {
				auto &e = it->second;
				if (e && e->protect_left && e->protect_right) {
					it = adj.erase(it);
				} else {
					++it;
				}
			}
		}
	}
}

void TransferGraphManager::LargestRoot(vector<LogicalOperator *> &sorted_nodes) {
	unordered_set<idx_t> constructed_set, unconstructed_set;
	int prior_flag = static_cast<int>(table_operator_manager.table_operators.size()) - 1;
	idx_t root = std::numeric_limits<idx_t>::max();

	// Initialize nodes
	for (auto &entry : table_operator_manager.table_operators) {
		idx_t id = entry.first;
		auto node = make_uniq<GraphNode>(id, prior_flag--);

		if (entry.second == sorted_nodes.back()) {
			root = id;
			constructed_set.insert(id);
		} else {
			unconstructed_set.insert(id);
		}

		transfer_graph[id] = std::move(node);
	}

	// Add root
	transfer_order.push_back(table_operator_manager.GetTableOperator(root));
	table_operator_manager.table_operators.erase(root);

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

		transfer_order.push_back(table_operator_manager.GetTableOperator(node->id));
		table_operator_manager.table_operators.erase(node->id);

		unconstructed_set.erase(selected_edge.second);
		constructed_set.insert(selected_edge.second);
	}
}

//! This doesn't only pick a root. It also creates a spanning tree from the candidate edges in neighbor_matrix
//! The spanning tree is greedy and cardinality-driven. 
//! The chosen edges go into `selected_edges`, and the chosen tables order goes into `transfer_order`
void TransferGraphManager::LargestRootUpdated(vector<LogicalOperator *> &sorted_nodes) {
	unordered_set<idx_t> constructed_set, unconstructed_set;
	int prior_flag = static_cast<int>(table_operator_manager.table_operators.size()) - 1;
	idx_t root = std::numeric_limits<idx_t>::max();

	// Try to choose the largest filtered or intermediate table as the root
	for (auto it = sorted_nodes.rbegin(); it != sorted_nodes.rend(); ++it) {
		// If we ran SkipUnfilteredTable, we cannot choose an unfiltered table, because that
		// would effectively disable the backward pass. 
		auto &node = *it;
		auto id = table_operator_manager.GetScalarTableIndex(node);
		if (filtered_table.count(id) || intermediate_table.count(id)) {
			root = id;
			break;
		}
	}

	// If we cannot find it, all tables have no filter
	if (root == std::numeric_limits<idx_t>::max()) {
		auto &node = sorted_nodes.back();
		root = table_operator_manager.GetScalarTableIndex(node);
	}

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

	// Add root
	transfer_order.push_back(table_operator_manager.GetTableOperator(root));
	table_operator_manager.table_operators.erase(root);
	for (auto &col_binding : table_join_keys[root]) {
		auto &group = table_groups[col_binding];
		if (group) {
			group->RegisterLeader(root, col_binding);
		}
	}

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

		transfer_order.push_back(table_operator_manager.GetTableOperator(node->id));
		table_operator_manager.table_operators.erase(node->id);
		for (auto &col_binding : table_join_keys[node->id]) {
			auto &group = table_groups[col_binding];
			if (group) {
				group->RegisterLeader(node->id, col_binding);
			}
		}

		unconstructed_set.erase(selected_edge.second);
		constructed_set.insert(selected_edge.second);
	}
}

//! Seed-driven alternative to LargestRootUpdated: picks a root and then grows
//! the spanning tree one table at a time, each choice made by `seed % N` over
//! a deterministically-sorted list of candidates. After each pick `seed` is
//! advanced via MurmurHash64 so the same initial seed always produces the
//! same transfer order across runs.
//!
//! Unlike LargestRootUpdated, this function:
//!   - Does not restrict the root to filtered/intermediate tables -- any
//!     table is a valid root (useful when SkipUnfilteredTable is disabled
//!     via skip_unfiltered_tables=false). // TODO would not restricting the root but having SkipUnfilteredTable turned on cause an issue??
//!   - Handles disconnected components internally by picking a fresh root
//!     from the remaining unconstructed tables when no neighbor has an edge
//!     into the constructed set. Callers therefore do not need an outer
//!     `while (!empty)` loop.
//!
//! The `cardinality_order` assignment mirrors LargestRootUpdated: every table
//! first receives a distinct initial value during the init loop, and each
//! table added as a child during tree building gets its value overwritten
//! with a smaller `prior_flag--`. This preserves the invariant that the
//! parent's cardinality_order is strictly greater than the child's, which
//! the "smaller table is in the left" swap in CreateTransferPlan* relies on.
void TransferGraphManager::THCRootAndTransferGraph(uint64_t &seed) {
	// Deterministic name lookup. Matches the pattern used by PrintTransferPlan
	// so the enumeration is anchored on table names when available.
	auto GetName = [](LogicalOperator *op) -> string {
		auto params = op->ParamsToString();
		if (params.contains("Table")) {
			return params.at("Table");
		}
		return string("Unknown");
	};

	// Order by (name, table_index). The table_index tiebreaker guarantees a
	// total order even when multiple operators share the same displayed name
	// (e.g. two scans of the same physical table).
	auto LessByName = [&](idx_t a, idx_t b) {
		auto *op_a = table_operator_manager.GetTableOperator(a);
		auto *op_b = table_operator_manager.GetTableOperator(b);
		auto name_a = GetName(op_a);
		auto name_b = GetName(op_b);
		if (name_a != name_b) {
			return name_a < name_b;
		}
		return a < b;
	};

	// Use the current seed to pick an index in [0, n), then advance the seed.
	// Advance-after-use means the very first call consumes the original seed
	// directly (so the root pick is `seed % N_root`), which matches the plan.
	auto PickMod = [&](size_t n) -> size_t {
		size_t result = static_cast<size_t>(seed % n);
		seed = MurmurHash64(seed);
		return result;
	};

	// Register `id` as the leader for every join-key group it participates in
	// (same bookkeeping LargestRootUpdated performs after adding a table).
	auto RegisterLeaders = [&](idx_t id) {
		for (auto &col_binding : table_join_keys[id]) {
			auto &group = table_groups[col_binding];
			// TODO when do groups in table_groups ever get populated?
			if (group) {
				group->RegisterLeader(id, col_binding);
			}
		}
	};

	if (table_operator_manager.table_operators.empty()) {
		return;
	}

	// This decreasing counter is used to hand out distinct cardinality_order values to each GraphNode 	
	// The meaning of the cardinality_order is "which end of this edge is the parent in the spanning tree"
	// "Parent" means "added to the tree earlier" and "child" means "added to the tree later"
	// Every time we add a table to the transfer graph, we give it a GraphNode, and set its 
	// cardinality_order to prior_flag, before decrementing prior_flag. 
	int prior_flag = static_cast<int>(table_operator_manager.table_operators.size()) - 1;
	
	// 1. Initialize GraphNodes for all table. Each node starts
	//    with a distinct `prior_flag` value; children added later will
	//    overwrite this to a smaller value. Root's initial value is never
	//    overwritten, matching LargestRootUpdated's behavior.
	vector<idx_t> all_ids;
	all_ids.reserve(table_operator_manager.table_operators.size());
	for (auto &entry : table_operator_manager.table_operators) {
		all_ids.push_back(entry.first);
		
		// There are two situations in which the prior_flag assigned here will remain the final
		// cardinality value:
		//  1. The root (since it's never added as a child)
		//  2. Other roots (of disconnected components)
		auto node = make_uniq<GraphNode>(entry.first, prior_flag--);
		transfer_graph[entry.first] = std::move(node);
	}
	std::sort(all_ids.begin(), all_ids.end(), LessByName);

	// Table indices that have been attached to the spanning tree
	unordered_set<idx_t> constructed_set;

	// Table indices that have not been attached to the spanning tree yet
	unordered_set<idx_t> unconstructed_set(all_ids.begin(), all_ids.end());

	// 2. Root pick over the full deterministic candidate list.
	size_t root_idx = PickMod(all_ids.size());
	idx_t root = all_ids[root_idx];

	transfer_order.push_back(table_operator_manager.GetTableOperator(root));
	table_operator_manager.table_operators.erase(root);
	constructed_set.insert(root);
	unconstructed_set.erase(root);
	RegisterLeaders(root);

	// 3. Grow the tree: at each step, pick one unconstructed table that has
	//    at least one edge into the constructed set. If no such table exists
	//    but unconstructed tables remain, the graph is disconnected; pick a
	//    fresh root from the leftovers and continue.
	while (!unconstructed_set.empty()) {
		// Collect unconstructed tables that are reachable from the current
		// spanning tree via at least one surviving edge.
		vector<idx_t> neighbors;
		neighbors.reserve(unconstructed_set.size());
		for (auto &t : unconstructed_set) {
			bool has_edge = false;
			for (auto &j : constructed_set) {
				auto row_it = neighbor_matrix.find(j);
				if (row_it == neighbor_matrix.end()) {
					continue;
				}
				auto col_it = row_it->second.find(t);
				if (col_it != row_it->second.end() && col_it->second) {
					has_edge = true;
					break;
				}
			}
			if (has_edge) {
				neighbors.push_back(t);
			}
		}
		std::sort(neighbors.begin(), neighbors.end(), LessByName);

		if (!neighbors.empty()) {
			// Seed-driven neighbor pick.
			size_t idx = PickMod(neighbors.size());
			idx_t t = neighbors[idx];

			// Enumerate all valid parents (constructed tables with an edge to
			// t) in a deterministic order and let the seed pick among them.
			vector<idx_t> parents;
			for (auto &j : constructed_set) {
				auto row_it = neighbor_matrix.find(j);
				if (row_it == neighbor_matrix.end()) {
					continue;
				}
				auto col_it = row_it->second.find(t);
				if (col_it != row_it->second.end() && col_it->second) {
					parents.push_back(j);
				}
			}
			std::sort(parents.begin(), parents.end(), LessByName);
			D_ASSERT(!parents.empty());

			size_t p_idx = PickMod(parents.size());
			idx_t parent = parents[p_idx];

			auto &edge = neighbor_matrix[parent][t];
			selected_edges.emplace_back(std::move(edge));

			auto node = transfer_graph[t].get();
			node->cardinality_order = prior_flag--;

			transfer_order.push_back(table_operator_manager.GetTableOperator(t));
			table_operator_manager.table_operators.erase(t);
			RegisterLeaders(t);

			constructed_set.insert(t);
			unconstructed_set.erase(t);
		} else {
			// Disconnected remainder: pick a new root from the unconstructed
			// tables. No edge is recorded because by definition there is no
			// connection back to the existing tree. The new root keeps its
			// initial `cardinality_order` from the init loop, which remains
			// larger than any future child value (those are assigned
			// prior_flag-- and will be negative once the init range is used
			// up).
			vector<idx_t> remaining(unconstructed_set.begin(), unconstructed_set.end());
			std::sort(remaining.begin(), remaining.end(), LessByName);

			size_t new_root_idx = PickMod(remaining.size());
			idx_t new_root = remaining[new_root_idx];

			transfer_order.push_back(table_operator_manager.GetTableOperator(new_root));
			table_operator_manager.table_operators.erase(new_root);
			RegisterLeaders(new_root);

			constructed_set.insert(new_root);
			unconstructed_set.erase(new_root);
		}
	}
}

void TransferGraphManager::CreateOriginTransferPlan() {
	auto saved_nodes = table_operator_manager.table_operators;
	while (!table_operator_manager.table_operators.empty()) {
		LargestRoot(table_operator_manager.sorted_table_operators);
		table_operator_manager.SortTableOperators();
	}
	table_operator_manager.table_operators = saved_nodes;

	for (auto &edge : selected_edges) {
		if (!edge) {
			continue;
		}

		idx_t left_idx = TableOperatorManager::GetScalarTableIndex(&edge->left_table);
		idx_t right_idx = TableOperatorManager::GetScalarTableIndex(&edge->right_table);

		D_ASSERT(left_idx != std::numeric_limits<idx_t>::max() && right_idx != std::numeric_limits<idx_t>::max());

		auto &type = edge->return_type;
		auto left_node = transfer_graph[left_idx].get();
		auto right_node = transfer_graph[right_idx].get();

		auto left_cols = edge->left_binding;
		auto right_cols = edge->right_binding;

		auto protect_left = edge->protect_left;
		auto protect_right = edge->protect_right;

		// smaller table is in the left
		if (left_node->cardinality_order > right_node->cardinality_order) {
			std::swap(left_node, right_node);
			std::swap(left_cols, right_cols);
			std::swap(protect_left, protect_right);
		}

		// forward: from the smaller to the larger
		if (!protect_right) {
			left_node->Add(right_node->id, {left_cols}, {right_cols}, {type}, true, false);
			right_node->Add(left_node->id, {left_cols}, {right_cols}, {type}, true, true);
		}

		// backward: from the larger to the smaller
		if (!protect_left) {
			left_node->Add(right_node->id, {left_cols}, {right_cols}, {type}, false, true);
			right_node->Add(left_node->id, {left_cols}, {right_cols}, {type}, false, false);
		}
	}
}

void TransferGraphManager::CreateTransferPlanUpdated() {
	auto saved_nodes = table_operator_manager.table_operators;
	while (!table_operator_manager.table_operators.empty()) {
		LargestRootUpdated(table_operator_manager.sorted_table_operators);
		table_operator_manager.SortTableOperators();
	}
	table_operator_manager.table_operators = saved_nodes;

	for (auto &edge : selected_edges) {
		if (!edge) {
			continue;
		}

		idx_t left_idx = TableOperatorManager::GetScalarTableIndex(&edge->left_table);
		idx_t right_idx = TableOperatorManager::GetScalarTableIndex(&edge->right_table);

		D_ASSERT(left_idx != std::numeric_limits<idx_t>::max() && right_idx != std::numeric_limits<idx_t>::max());

		auto &type = edge->return_type;
		auto left_node = transfer_graph[left_idx].get();
		auto right_node = transfer_graph[right_idx].get();

		auto left_cols = edge->left_binding;
		auto right_cols = edge->right_binding;

		auto protect_left = edge->protect_left;
		auto protect_right = edge->protect_right;

		// smaller table is in the left
		// This is not an actual cardinality swap. It maintains the parent-child
		// order relationship. See swap_in_transfer_graph_build.md
		if (left_node->cardinality_order > right_node->cardinality_order) {
			std::swap(left_node, right_node);
			std::swap(left_cols, right_cols);
			std::swap(protect_left, protect_right);
		}

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
	}
}

//! Seed-driven counterpart of CreateTransferPlanUpdated: delegates the
//! root/spanning-tree selection to THCRootAndTransferGraph (which already
//! handles disconnected components internally) and then runs the same
//! forward/backward edge-wiring pass as CreateTransferPlanUpdated.
//!
//! The save/restore of table_operators around the builder call is required
//! because THCRootAndTransferGraph (like LargestRootUpdated) erases tables
//! from table_operator_manager.table_operators as they are placed; the outer
//! pipeline expects the full set to still be available after planning.
void TransferGraphManager::CreateTransferPlanSeeded(uint64_t seed) {
	auto saved_nodes = table_operator_manager.table_operators;
	THCRootAndTransferGraph(seed);
	table_operator_manager.table_operators = saved_nodes;

	for (auto &edge : selected_edges) {
		if (!edge) {
			continue;
		}

		idx_t left_idx = TableOperatorManager::GetScalarTableIndex(&edge->left_table);
		idx_t right_idx = TableOperatorManager::GetScalarTableIndex(&edge->right_table);

		D_ASSERT(left_idx != std::numeric_limits<idx_t>::max() && right_idx != std::numeric_limits<idx_t>::max());

		auto &type = edge->return_type;
		auto left_node = transfer_graph[left_idx].get();
		auto right_node = transfer_graph[right_idx].get();

		auto left_cols = edge->left_binding;
		auto right_cols = edge->right_binding;

		auto protect_left = edge->protect_left;
		auto protect_right = edge->protect_right;

		// smaller table is in the left
		if (left_node->cardinality_order > right_node->cardinality_order) {
			std::swap(left_node, right_node);
			std::swap(left_cols, right_cols);
			std::swap(protect_left, protect_right);
		}

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
	}
}

pair<idx_t, idx_t> TransferGraphManager::FindEdge(const unordered_set<idx_t> &constructed_set,
                                                  const unordered_set<idx_t> &unconstructed_set) {
	pair<idx_t, idx_t> result {std::numeric_limits<idx_t>::max(), std::numeric_limits<idx_t>::max()};
	idx_t max_cardinality = 0;
	bool is_indirected = false;

	for (auto i : unconstructed_set) {
		for (auto j : constructed_set) {
			auto &edge = neighbor_matrix[j][i];
			if (edge == nullptr) {
				continue;
			}

			idx_t cardinality = table_operator_manager.GetTableOperator(i)->estimated_cardinality;
			if (cardinality > max_cardinality ||
			    (is_indirected == false && !edge->protect_left && !edge->protect_right)) {
				max_cardinality = cardinality;
				result = {j, i};
				is_indirected = !edge->protect_left && !edge->protect_right;
			}
		}
	}
	return result;
}

} // namespace duckdb
