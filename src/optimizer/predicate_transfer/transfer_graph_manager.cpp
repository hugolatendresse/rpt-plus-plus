#include "duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp"

#include "duckdb/common/debug_log.hpp"
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
	// NOTE: the FILTER_PUSHDOWN optimization (using FilterCombiner) creates join conditions
	// based on equivalence classes. Hence, we might see edges below that are not in the 
	// original plan. 
	ExtractEdgesInfo(joins);
	if (neighbor_matrix.empty()) {
		return false;
	}

	auto &cfg = ClientConfig::GetConfig(context);

	// 3. Unfiltered tables only receive Bloom filters, they will not generate Bloom filters.
	//    Gated by skip_unfiltered_tables_graph_creation so the edge-pruning pass can be
	//    toggled independently of the transfer-order strategy. Disabling it
	//    lets CreateTransferPlanSPY explore plans where unfiltered tables
	//    also create BFs.
	if (cfg.skip_unfiltered_tables_graph_creation) {
		SkipUnfilteredTable(joins);
	}

	// 4. Create the transfer graph. Root selection and non-root growth are
	//    controlled by two independent flags so all four combinations work:
	//      - use_seeded_root=false, use_seeded_transfer_order=false: pure
	//        RPT+ paper behavior. Root is the largest filtered/intermediate
	//        table; subsequent nodes are picked greedily by cardinality.
	//      - use_seeded_root=false, use_seeded_transfer_order=true: RPT+
	//        root pick combined with seed-driven growth.
	//      - use_seeded_root=true,  use_seeded_transfer_order=false: seed
	//        picks the root, then greedy cardinality-driven growth.
	//      - use_seeded_root=true,  use_seeded_transfer_order=true: fully
	//        seed-driven (every choice from the seed).
	//    `transfer_graph_seed` is consulted only when at least one of the
	//    two strategies is seeded. The seed value itself (including 0) only
	//    affects which plan is enumerated; it is never random.
	CreateTransferPlanSPY(cfg.use_seeded_root, cfg.use_seeded_transfer_order, cfg.transfer_graph_seed);

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

//! Build the predicate-transfer spanning tree for any of the four
//! (seeded_root, seeded_growth) strategy combinations, then run the shared
//! forward/backward edge-wiring pass.
//!
//! The function tracks progress via local `constructed_set` /
//! `unconstructed_set` and never erases entries from
//! `table_operator_manager.table_operators`, so all four combos can rely on
//! `GetTableOperator(id)` staying valid for the full lifetime of the call.
//! Disconnected components are handled inline by picking a fresh root from
//! the leftovers using the current root strategy.
//!
//! `cardinality_order` invariant (used by the wiring pass below):
//!   - The init loop hands out distinct values `n-1 .. 0` to every table.
//!   - Every child placement overwrites the value with `prior_flag--`,
//!     which becomes negative once the init range is exhausted.
//!   - Initial roots and disconnected new roots keep their (>=0) init value.
//!   - For every recorded edge, parent.cardinality_order > child.cardinality_order
//!     because the parent was placed first. The "smaller table on the left"
//!     swap in WireTransferGraph relies on this strict ordering.
void TransferGraphManager::CreateTransferPlanSPY(bool seeded_root, bool seeded_growth, uint64_t seed) {
	if (table_operator_manager.table_operators.empty()) {
		return;
	}

	auto &sorted_nodes = table_operator_manager.sorted_table_operators;

	// Deterministic name lookup. Matches the pattern used by PrintTransferPlan
	// so the enumeration is anchored on table names when available.
	auto GetName = [](LogicalOperator *op) -> string {
		auto params = op->ParamsToString();
		if (params.contains("Table")) {
			return params.at("Table");
		}
		return string("Unknown");
	};

	// Use the current seed to pick an index in [0, n), then advance the seed
	// via MurmurHash64. Advance-after-use means the very first call consumes
	// the original seed directly. Only invoked along seeded code paths.
	auto PickMod = [&](size_t n) -> size_t {
		size_t result = static_cast<size_t>(seed % n);
		seed = MurmurHash64(seed);
		return result;
	};

	// Register `id` as the leader for every join-key group it participates in.
	// `table_groups` is populated by ExtractEdgesInfo via UnionBindings, so an
	// entry exists only for bindings that participated in at least one
	// unprotected equijoin (protected edges -- e.g. the outer side of a
	// LEFT/RIGHT/MARK join -- skip the union step). Bindings outside any
	// group leave `group` null, which is why the null check is needed.
	auto RegisterLeaders = [&](idx_t id) {
		for (auto &col_binding : table_join_keys[id]) {
			auto &group = table_groups[col_binding];
			if (group) {
				group->RegisterLeader(id, col_binding);
			}
		}
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

	// This decreasing counter is used to hand out distinct cardinality_order
	// values to each GraphNode. The meaning of cardinality_order is "which
	// end of this edge is the parent in the spanning tree". "Parent" means
	// "added to the tree earlier" and "child" means "added to the tree later".
	// Every time we add a table as a child, we overwrite its node's
	// cardinality_order with `prior_flag--`. The init loop below uses the
	// same counter, so child values always end up strictly smaller than any
	// initial value that survives (i.e. roots of connected components).
	int prior_flag = static_cast<int>(table_operator_manager.table_operators.size()) - 1;

	// 1. Initialize GraphNodes for all tables. Each node starts with a
	//    distinct `prior_flag` value; children added later will overwrite
	//    this to a smaller value. Roots of any connected component never
	//    have their value overwritten.
	vector<idx_t> all_ids;
	all_ids.reserve(table_operator_manager.table_operators.size());
	for (auto &entry : table_operator_manager.table_operators) {
		all_ids.push_back(entry.first);
		auto node = make_uniq<GraphNode>(entry.first, prior_flag--);
		transfer_graph[entry.first] = std::move(node);
	}

	// Table indices that have been attached to the spanning tree
	unordered_set<idx_t> constructed_set;
	// Table indices that have not been attached to the spanning tree yet
	unordered_set<idx_t> unconstructed_set(all_ids.begin(), all_ids.end());

	DEBUG_LOG("[CreateTransferPlan] seeded_root=%d seeded_growth=%d seed=%llu num_tables=%zu\n",
	          seeded_root ? 1 : 0, seeded_growth ? 1 : 0,
	          static_cast<unsigned long long>(seed),
	          table_operator_manager.table_operators.size());

	// PickRoot returns the next root (called for the initial root and for
	// every disconnected-component continuation). Both strategies pick
	// strictly out of `unconstructed_set` so they keep returning fresh ids.
	auto PickRoot = [&]() -> idx_t {
		D_ASSERT(!unconstructed_set.empty());
		if (seeded_root) {
			// Seed-driven pick over a deterministic name-sorted snapshot of
			// the unconstructed candidates. Any table is a valid root,
			// including unfiltered ones. Note: combining seeded_root=true
			// with skip_unfiltered_tables_graph_creation=true can root the
			// tree at a table whose outgoing edges were pruned by
			// SkipUnfilteredTable, which yields a degenerate spanning tree
			// for that component.
			vector<idx_t> remaining(unconstructed_set.begin(), unconstructed_set.end());
			std::sort(remaining.begin(), remaining.end(), LessByName);
			size_t idx = PickMod(remaining.size());
			return remaining[idx];
		}
		// RPT+ behavior: walk the cardinality-sorted list in reverse and
		// return the largest filtered/intermediate table that is still
		// unconstructed. Restricting to filtered/intermediate keeps the
		// backward pass meaningful when SkipUnfilteredTable has run.
		for (auto it = sorted_nodes.rbegin(); it != sorted_nodes.rend(); ++it) {
			idx_t id = TableOperatorManager::GetScalarTableIndex(*it);
			if (!unconstructed_set.count(id)) {
				continue;
			}
			if (filtered_table.count(id) || intermediate_table.count(id)) {
				return id;
			}
		}
		// Fallback: no filtered/intermediate table left, fall back to the
		// largest unconstructed table.
		for (auto it = sorted_nodes.rbegin(); it != sorted_nodes.rend(); ++it) {
			idx_t id = TableOperatorManager::GetScalarTableIndex(*it);
			if (unconstructed_set.count(id)) {
				return id;
			}
		}
		// unconstructed_set is non-empty (asserted above) but no entry was
		// reachable through sorted_nodes -- should not happen because every
		// initialized table is in sorted_table_operators.
		return *unconstructed_set.begin();
	};

	// PickEdge returns (parent, child); (max, max) signals "no edge into the
	// constructed set", which the outer loop treats as a disconnected
	// component and resolves via PickRoot().
	auto PickEdge = [&]() -> pair<idx_t, idx_t> {
		if (seeded_growth) {
			// Collect unconstructed tables that are reachable from the
			// current spanning tree via at least one surviving edge.
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
			if (neighbors.empty()) {
				return {std::numeric_limits<idx_t>::max(), std::numeric_limits<idx_t>::max()};
			}
			std::sort(neighbors.begin(), neighbors.end(), LessByName);
			size_t idx = PickMod(neighbors.size());
			idx_t t = neighbors[idx];

			// Enumerate all valid parents (constructed tables with an edge
			// to t) in a deterministic order and let the seed pick among
			// them.
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

			DEBUG_LOG("[CreateTransferPlan] step=%zu seeded-growth attach child=%s (table_index=%zu, "
			          "neighbor_idx=%zu/%zu) <- parent=%s (table_index=%zu, parent_idx=%zu/%zu)\n",
			          constructed_set.size(),
			          GetName(table_operator_manager.GetTableOperator(t)).c_str(),
			          static_cast<size_t>(t), idx, neighbors.size(),
			          GetName(table_operator_manager.GetTableOperator(parent)).c_str(),
			          static_cast<size_t>(parent), p_idx, parents.size());

			return {parent, t};
		}
		// RPT+ greedy growth: FindEdge already takes constructed/unconstructed
		// sets and picks the highest-cardinality unconstructed neighbor with
		// any qualifying edge into the constructed set. Returns max/max when
		// no such edge exists.
		auto picked = FindEdge(constructed_set, unconstructed_set);
		if (picked.first != std::numeric_limits<idx_t>::max()) {
			DEBUG_LOG("[CreateTransferPlan] step=%zu greedy-growth attach child=%s (table_index=%zu) <- "
			          "parent=%s (table_index=%zu)\n",
			          constructed_set.size(),
			          GetName(table_operator_manager.GetTableOperator(picked.second)).c_str(),
			          static_cast<size_t>(picked.second),
			          GetName(table_operator_manager.GetTableOperator(picked.first)).c_str(),
			          static_cast<size_t>(picked.first));
		}
		return picked;
	};

	// 2. Place the initial root.
	idx_t root = PickRoot();
	DEBUG_LOG("[CreateTransferPlan] root=%s (table_index=%zu)\n",
	          GetName(table_operator_manager.GetTableOperator(root)).c_str(),
	          static_cast<size_t>(root));
	transfer_order.push_back(table_operator_manager.GetTableOperator(root));
	constructed_set.insert(root);
	unconstructed_set.erase(root);
	RegisterLeaders(root);

	// 3. Grow the spanning tree. On "no edge" the leftovers form a
	//    disconnected component; pick a fresh root using the same root
	//    strategy and continue.
	while (!unconstructed_set.empty()) {
		auto picked = PickEdge();
		if (picked.first == std::numeric_limits<idx_t>::max()) {
			idx_t new_root = PickRoot();
			DEBUG_LOG("[CreateTransferPlan] step=%zu disconnected-component new_root=%s (table_index=%zu)\n",
			          constructed_set.size(),
			          GetName(table_operator_manager.GetTableOperator(new_root)).c_str(),
			          static_cast<size_t>(new_root));
			// No edge is recorded because by definition there is no
			// connection back to the existing tree. The new root keeps its
			// initial `cardinality_order` from the init loop.
			transfer_order.push_back(table_operator_manager.GetTableOperator(new_root));
			constructed_set.insert(new_root);
			unconstructed_set.erase(new_root);
			RegisterLeaders(new_root);
			continue;
		}

		idx_t parent = picked.first;
		idx_t child = picked.second;
		auto &edge = neighbor_matrix[parent][child];
		selected_edges.emplace_back(std::move(edge));

		auto node = transfer_graph[child].get();
		node->cardinality_order = prior_flag--;

		transfer_order.push_back(table_operator_manager.GetTableOperator(child));
		RegisterLeaders(child);

		constructed_set.insert(child);
		unconstructed_set.erase(child);
	}

	// Final summary so the full order is visible in one block without
	// having to stitch the per-step lines back together.
	DEBUG_LOG("[CreateTransferPlan] done. transfer_order size=%zu\n", transfer_order.size());
	for (size_t i = 0; i < transfer_order.size(); ++i) {
		auto *op = transfer_order[i];
		idx_t tid = TableOperatorManager::GetScalarTableIndex(op);
		DEBUG_LOG("[CreateTransferPlan]   [%zu] %s (table_index=%zu)\n",
		          i, op ? GetName(op).c_str() : "<null>", static_cast<size_t>(tid));
	}

	// 4. Wire forward/backward edges. This pass is identical for every
	//    strategy combo because it only consumes selected_edges,
	//    transfer_graph (with a populated cardinality_order on every node)
	//    and table_groups.
	WireTransferGraph();
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

//! Wire forward/backward Bloom-filter edges for every selected spanning-tree
//! edge. Shared by all four strategy combinations because it only consumes
//! `selected_edges`, `transfer_graph` (with a populated `cardinality_order`
//! on every node) and `table_groups` -- none of which depend on how the
//! spanning tree itself was picked.
void TransferGraphManager::WireTransferGraph() {
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
