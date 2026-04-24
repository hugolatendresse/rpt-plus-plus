#include "duckdb/optimizer/join_order/plan_enumerator.hpp"

#include "duckdb/common/debug_log.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"

#include <algorithm>
#include <cmath>
#include <random>

//SPY: See these commits for when this file diverged RPT->DPT:
//     JoinNode->DPJoinNode: https://github.com/duckdb/duckdb/commit/1f829be0640d9ec086a67185cf521d93a91a6890

namespace duckdb {

static vector<unordered_set<idx_t>> AddSuperSets(const vector<unordered_set<idx_t>> &current,
                                                 const vector<idx_t> &all_neighbors) {
	vector<unordered_set<idx_t>> ret;

	for (const auto &neighbor_set : current) {
		auto max_val = std::max_element(neighbor_set.begin(), neighbor_set.end());
		for (const auto &neighbor : all_neighbors) {
			if (*max_val >= neighbor) {
				continue;
			}
			if (neighbor_set.count(neighbor) == 0) {
				unordered_set<idx_t> new_set;
				for (auto &n : neighbor_set) {
					new_set.insert(n);
				}
				new_set.insert(neighbor);
				ret.push_back(new_set);
			}
		}
	}

	return ret;
}

//! Update the exclusion set with all entries in the subgraph
static void UpdateExclusionSet(optional_ptr<JoinRelationSet> node, unordered_set<idx_t> &exclusion_set) {
	for (idx_t i = 0; i < node->count; i++) {
		exclusion_set.insert(node->relations[i]);
	}
}

// works by first creating all sets with cardinality 1
// then iterates over each previously created group of subsets and will only add a neighbor if the neighbor
// is greater than all relations in the set.
static vector<unordered_set<idx_t>> GetAllNeighborSets(vector<idx_t> neighbors) {
	vector<unordered_set<idx_t>> ret;
	sort(neighbors.begin(), neighbors.end());
	vector<unordered_set<idx_t>> added;
	for (auto &neighbor : neighbors) {
		added.push_back(unordered_set<idx_t>({neighbor}));
		ret.push_back(unordered_set<idx_t>({neighbor}));
	}
	do {
		added = AddSuperSets(added, neighbors);
		for (auto &d : added) {
			ret.push_back(d);
		}
	} while (!added.empty());
#if DEBUG
	// drive by test to make sure we have an accurate amount of
	// subsets, and that each neighbor is in a correct amount
	// of those subsets.
	D_ASSERT(ret.size() == std::pow(2, neighbors.size()) - 1);
	for (auto &n : neighbors) {
		idx_t count = 0;
		for (auto &set : ret) {
			if (set.count(n) >= 1) {
				count += 1;
			}
		}
		D_ASSERT(count == std::pow(2, neighbors.size() - 1));
	}
#endif
	return ret;
}

void PlanEnumerator::GenerateCrossProducts() {
	// generate a set of cross products to combine the currently available plans into a full join plan
	// we create edges between every relation with a high cost
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		auto &left = query_graph_manager.set_manager.GetJoinRelation(i);
		for (idx_t j = 0; j < query_graph_manager.relation_manager.NumRelations(); j++) {
			auto cross_product_allowed = query_graph_manager.relation_manager.CrossProductWithRelationAllowed(i) &&
			                             query_graph_manager.relation_manager.CrossProductWithRelationAllowed(j);
			if (i != j && cross_product_allowed) {
				auto &right = query_graph_manager.set_manager.GetJoinRelation(j);
				query_graph_manager.CreateQueryGraphCrossProduct(left, right);
			}
		}
	}
	// Now that the query graph has new edges, we need to re-initialize our query graph.
	// TODO: do we need to initialize our qyery graph again?
	// query_graph = query_graph_manager.GetQueryGraph();
}

const reference_map_t<JoinRelationSet, unique_ptr<DPJoinNode>> &PlanEnumerator::GetPlans() const {
	return plans;
}

//! Create a new JoinTree node by joining together two previous JoinTree nodes
unique_ptr<DPJoinNode> PlanEnumerator::CreateJoinTree(JoinRelationSet &set,
                                                      const vector<reference<NeighborInfo>> &possible_connections,
                                                      DPJoinNode &left, DPJoinNode &right) {

	// FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
	optional_ptr<NeighborInfo> best_connection = possible_connections.back().get();
	// cross products are technically still connections, but the filter expression is a null_ptr
	bool found_non_cross_product_connection = false;
	for (auto &connection : possible_connections) {
		for (auto &filter : connection.get().filters) {
			if (filter->join_type != JoinType::INVALID) {
				best_connection = connection.get();
				found_non_cross_product_connection = true;
				break;
			}
		}
		if (found_non_cross_product_connection) {
			break;
		}
	}
	auto join_type = JoinType::INVALID;
	for (auto &filter_binding : best_connection->filters) {
		if (!filter_binding->left_set || !filter_binding->right_set) {
			continue;
		}

		join_type = filter_binding->join_type;
		// prefer joining on semi and anti joins as they have a higher chance of being more
		// selective
		if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
			break;
		}
	}
	// need the filter info from the Neighborhood info.
	auto cost = cost_model.ComputeCost(left, right);
	auto result = make_uniq<DPJoinNode>(set, best_connection, left.set, right.set, cost);
	result->cardinality = cost_model.cardinality_estimator.EstimateCardinalityWithSet<idx_t>(set);
	return result;
}

DPJoinNode &PlanEnumerator::EmitPair(JoinRelationSet &left, JoinRelationSet &right,
                                     const vector<reference<NeighborInfo>> &info) {
	// get the left and right join plans
	auto left_plan = plans.find(left);
	auto right_plan = plans.find(right);
	if (left_plan == plans.end() || right_plan == plans.end()) {
		throw InternalException("No left or right plan: internal error in join order optimizer");
	}
	auto &new_set = query_graph_manager.set_manager.Union(left, right);
	// create the join tree based on combining the two plans
	auto new_plan = CreateJoinTree(new_set, info, *left_plan->second, *right_plan->second);
	// check if this plan is the optimal plan we found for this set of relations
	auto entry = plans.find(new_set);
	auto new_cost = new_plan->cost;
	double old_cost = NumericLimits<double>::Maximum();
	if (entry != plans.end()) {
		old_cost = entry->second->cost;
	}
	if (entry == plans.end() || new_cost < old_cost) {
		// the new plan costs less than the old plan. Update our DP table.
		plans[new_set] = std::move(new_plan);
		return *plans[new_set];
	}
	// Create join node from the plan currently in the DP table.
	return *entry->second;
}

bool PlanEnumerator::TryEmitPair(JoinRelationSet &left, JoinRelationSet &right,
                                 const vector<reference<NeighborInfo>> &info) {
	pairs++;
	// If a full plan is created, it's possible a node in the plan gets updated. When this happens, make sure you keep
	// emitting pairs until you emit another final plan. Another final plan is guaranteed to be produced because of
	// our symmetry guarantees.
	if (pairs >= 10000) {
		// when the amount of pairs gets too large we exit the dynamic programming and resort to a greedy algorithm
		// FIXME: simple heuristic currently
		// at 10K pairs stop searching exactly and switch to heuristic
		return false;
	}
	EmitPair(left, right, info);
	return true;
}

bool PlanEnumerator::EmitCSG(JoinRelationSet &node) {
	if (node.count == query_graph_manager.relation_manager.NumRelations()) {
		return true;
	}
	// create the exclusion set as everything inside the subgraph AND anything with members BELOW it
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < node.relations[0]; i++) {
		exclusion_set.insert(i);
	}
	UpdateExclusionSet(&node, exclusion_set);
	// find the neighbors given this exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	//! Neighbors should be reversed when iterating over them.
	std::sort(neighbors.begin(), neighbors.end(), std::greater<idx_t>());
	for (idx_t i = 0; i < neighbors.size() - 1; i++) {
		D_ASSERT(neighbors[i] > neighbors[i + 1]);
	}

	// Dphyp paper missing this.
	// Because we are traversing in reverse order, we need to add neighbors whose number is smaller than the current
	// node to exclusion_set
	// This avoids duplicated enumeration
	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (idx_t i = 0; i < neighbors.size(); ++i) {
		D_ASSERT(new_exclusion_set.find(neighbors[i]) == new_exclusion_set.end());
		new_exclusion_set.insert(neighbors[i]);
	}

	for (auto neighbor : neighbors) {
		// since the GetNeighbors only returns the smallest element in a list, the entry might not be connected to
		// (only!) this neighbor,  hence we have to do a connectedness check before we can emit it
		auto &neighbor_relation = query_graph_manager.set_manager.GetJoinRelation(neighbor);
		auto connections = query_graph.GetConnections(node, neighbor_relation);
		if (!connections.empty()) {
			if (!TryEmitPair(node, neighbor_relation, connections)) {
				return false;
			}
		}

		if (!EnumerateCmpRecursive(node, neighbor_relation, new_exclusion_set)) {
			return false;
		}

		new_exclusion_set.erase(neighbor);
	}
	return true;
}

bool PlanEnumerator::EnumerateCmpRecursive(JoinRelationSet &left, JoinRelationSet &right,
                                           unordered_set<idx_t> &exclusion_set) {
	// get the neighbors of the second relation under the exclusion set
	auto neighbors = query_graph.GetNeighbors(right, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	auto all_subset = GetAllNeighborSets(neighbors);
	vector<reference<JoinRelationSet>> union_sets;
	union_sets.reserve(all_subset.size());
	for (const auto &rel_set : all_subset) {
		auto &neighbor = query_graph_manager.set_manager.GetJoinRelation(rel_set);
		// emit the combinations of this node and its neighbors
		auto &combined_set = query_graph_manager.set_manager.Union(right, neighbor);
		// If combined_set.count == right.count, This means we found a neighbor that has been present before
		// This means we didn't set exclusion_set correctly.
		D_ASSERT(combined_set.count > right.count);
		if (plans.find(combined_set) != plans.end()) {
			auto connections = query_graph.GetConnections(left, combined_set);
			if (!connections.empty()) {
				if (!TryEmitPair(left, combined_set, connections)) {
					return false;
				}
			}
		}
		union_sets.push_back(combined_set);
	}

	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (const auto &neighbor : neighbors) {
		new_exclusion_set.insert(neighbor);
	}

	// recursively enumerate the sets
	for (idx_t i = 0; i < union_sets.size(); i++) {
		// updated the set of excluded entries with this neighbor
		if (!EnumerateCmpRecursive(left, union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool PlanEnumerator::EnumerateCSGRecursive(JoinRelationSet &node, unordered_set<idx_t> &exclusion_set) {
	// find neighbors of S under the exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	auto all_subset = GetAllNeighborSets(neighbors);
	vector<reference<JoinRelationSet>> union_sets;
	union_sets.reserve(all_subset.size());
	for (const auto &rel_set : all_subset) {
		auto &neighbor = query_graph_manager.set_manager.GetJoinRelation(rel_set);
		// emit the combinations of this node and its neighbors
		auto &new_set = query_graph_manager.set_manager.Union(node, neighbor);
		D_ASSERT(new_set.count > node.count);
		if (plans.find(new_set) != plans.end()) {
			if (!EmitCSG(new_set)) {
				return false;
			}
		}
		union_sets.push_back(new_set);
	}

	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (const auto &neighbor : neighbors) {
		new_exclusion_set.insert(neighbor);
	}

	// recursively enumerate the sets
	for (idx_t i = 0; i < union_sets.size(); i++) {
		// updated the set of excluded entries with this neighbor
		if (!EnumerateCSGRecursive(union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool PlanEnumerator::SolveJoinOrderExactly() {
	// now we perform the actual dynamic programming to compute the final result
	// we enumerate over all the possible pairs in the neighborhood
	for (idx_t i = query_graph_manager.relation_manager.NumRelations(); i > 0; i--) {
		// for every node in the set, we consider it as the start node once
		auto &start_node = query_graph_manager.set_manager.GetJoinRelation(i - 1);
		// emit the start node
		if (!EmitCSG(start_node)) {
			return false;
		}
		// initialize the set of exclusion_set as all the nodes with a number below this
		unordered_set<idx_t> exclusion_set;
		for (idx_t j = 0; j < i; j++) {
			exclusion_set.insert(j);
		}
		// then we recursively search for neighbors that do not belong to the banned entries
		if (!EnumerateCSGRecursive(start_node, exclusion_set)) {
			return false;
		}
	}
	return true;
}

void PlanEnumerator::SolveJoinOrderApproximately() {
	// at this point, we exited the dynamic programming but did not compute the final join order because it took too
	// long instead, we use a greedy heuristic to obtain a join ordering now we use Greedy Operator Ordering to
	// construct the result tree first we start out with all the base relations (the to-be-joined relations)
	vector<reference<JoinRelationSet>> join_relations; // T in the paper
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		join_relations.push_back(query_graph_manager.set_manager.GetJoinRelation(i));
	}
	while (join_relations.size() > 1) {
		// now in every step of the algorithm, we greedily pick the join between the to-be-joined relations that has the
		// smallest cost. This is O(r^2) per step, and every step will reduce the total amount of relations to-be-joined
		// by 1, so the total cost is O(r^3) in the amount of relations
		// long is needed to prevent clang-tidy complaints. (idx_t) cannot be added to an iterator position because it
		// is unsigned.
		idx_t best_left = 0, best_right = 0;
		optional_ptr<DPJoinNode> best_connection;
		for (idx_t i = 0; i < join_relations.size(); i++) {
			auto left = join_relations[i];
			for (idx_t j = i + 1; j < join_relations.size(); j++) {
				auto right = join_relations[j];
				// check if we can connect these two relations
				auto connection = query_graph.GetConnections(left, right);
				if (!connection.empty()) {
					// we can check the cost of this connection
					auto node = EmitPair(left, right, connection);

					// update the DP tree in case a plan created by the DP algorithm uses the node
					// that was potentially just updated by EmitPair. You will get a use-after-free
					// error if future plans rely on the old node that was just replaced.
					// if node in FullPath, then updateDP tree.

					if (!best_connection || node.cost < best_connection->cost) {
						// best pair found so far
						best_connection = &EmitPair(left, right, connection);
						best_left = i;
						best_right = j;
					}
				}
			}
		}
		if (!best_connection) {
			// could not find a connection, but we were not done with finding a completed plan
			// we have to add a cross product; we add it between the two smallest relations
			optional_ptr<DPJoinNode> smallest_plans[2];
			size_t smallest_index[2];
			D_ASSERT(join_relations.size() >= 2);

			// first just add the first two join relations. It doesn't matter the cost as the JOO
			// will swap them on estimated cardinality anyway.
			for (idx_t i = 0; i < 2; i++) {
				optional_ptr<DPJoinNode> current_plan = plans[join_relations[i]];
				smallest_plans[i] = current_plan;
				smallest_index[i] = i;
			}

			// if there are any other join relations that don't have connections
			// add them if they have lower estimated cardinality.
			for (idx_t i = 2; i < join_relations.size(); i++) {
				// get the plan for this relation
				optional_ptr<DPJoinNode> current_plan = plans[join_relations[i]];
				// check if the cardinality is smaller than the smallest two found so far
				for (idx_t j = 0; j < 2; j++) {
					if (!smallest_plans[j] || smallest_plans[j]->cost > current_plan->cost) {
						smallest_plans[j] = current_plan;
						smallest_index[j] = i;
						break;
					}
				}
			}
			if (!smallest_plans[0] || !smallest_plans[1]) {
				throw InternalException("Internal error in join order optimizer");
			}
			D_ASSERT(smallest_plans[0] && smallest_plans[1]);
			D_ASSERT(smallest_index[0] != smallest_index[1]);
			auto &left = smallest_plans[0]->set;
			auto &right = smallest_plans[1]->set;
			// create a cross product edge (i.e. edge with empty filter) between these two sets in the query graph
			query_graph_manager.CreateQueryGraphCrossProduct(left, right);
			// now emit the pair and continue with the algorithm
			auto connections = query_graph.GetConnections(left, right);
			D_ASSERT(!connections.empty());

			best_connection = &EmitPair(left, right, connections);
			best_left = smallest_index[0];
			best_right = smallest_index[1];

			// the code below assumes best_right > best_left
			if (best_left > best_right) {
				std::swap(best_left, best_right);
			}
		}
		// now update the to-be-checked pairs
		// remove left and right, and add the combination

		// important to erase the biggest element first
		// if we erase the smallest element first the index of the biggest element changes
		auto &new_set = query_graph_manager.set_manager.Union(join_relations.at(best_left).get(),
		                                                      join_relations.at(best_right).get());
		D_ASSERT(best_right > best_left);
		join_relations.erase(join_relations.begin() + (int64_t)best_right);
		join_relations.erase(join_relations.begin() + (int64_t)best_left);
		join_relations.push_back(new_set);
	}
}

void PlanEnumerator::InitLeafPlans() {
	// First we initialize each of the single-node plans with themselves and with their cardinalities these are the leaf
	// nodes of the join tree NOTE: we can just use pointers to JoinRelationSet* here because the GetJoinRelation
	// function ensures that a unique combination of relations will have a unique JoinRelationSet object.
	// first initialize equivalent relations based on the filters
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();

	cost_model.cardinality_estimator.InitEquivalentRelations(query_graph_manager.GetFilterBindings());
	cost_model.cardinality_estimator.AddRelationNamesToTdoms(relation_stats);

	// then update the total domains based on the cardinalities of each relation.
	for (idx_t i = 0; i < relation_stats.size(); i++) {
		auto stats = relation_stats.at(i);
		auto &relation_set = query_graph_manager.set_manager.GetJoinRelation(i);
		auto join_node = make_uniq<DPJoinNode>(relation_set);
		join_node->cost = 0;
		join_node->cardinality = stats.cardinality;
		D_ASSERT(join_node->set.count == 1);
		plans[relation_set] = std::move(join_node);
		cost_model.cardinality_estimator.InitCardinalityEstimatorProps(&relation_set, stats);
	}
}

void PlanEnumerator::SolveJoinOrderLeftDeep() {
	vector<vector<JoinRelationSet*>> join_rels(query_graph_manager.relation_manager.NumRelations());
	for (int i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		join_rels[0].push_back(&query_graph_manager.set_manager.GetJoinRelation(i));
	}
	for (int join_size = 1; join_size < query_graph_manager.relation_manager.NumRelations(); join_size++) {
		for (int left_idx = 0; left_idx < join_rels[join_size - 1].size(); left_idx++) {
			auto &left = join_rels[join_size - 1][left_idx];
			for (int right_idx = 0; right_idx < join_rels[0].size(); right_idx++) {
				auto &right = join_rels[0][right_idx];
				if (!JoinRelationSet::IsSubset(*left, *right)) {
					auto connection = query_graph.GetConnections(*left, *right);
					if (!connection.empty()) {
						auto &new_set = query_graph_manager.set_manager.Union(*left, *right);
						bool add2join_rels = false;
						if(plans.find(new_set) == plans.end()) {
							add2join_rels = true;
						}
						auto &node = EmitPair(*left, *right, connection);
						if (add2join_rels) {
							join_rels[join_size].push_back(&node.set);
						}
						//SPY: REMOVED DOES NOT EXIST UpdateDPTree(node);
					}
				}
			}
		}
	}
	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		bindings.insert(i);
	}
	auto &total_relation = query_graph_manager.set_manager.GetJoinRelation(bindings);
	auto final_plan = plans.find(total_relation);
	if (final_plan == plans.end()) {
		// Disconnected query graph: some relations are only reachable via
		// cross products. Collect the largest partial plan for each connected
		// component and chain them together with cross-product edges.
		vector<JoinRelationSet *> components;
		unordered_set<idx_t> covered;
		idx_t num_rels = query_graph_manager.relation_manager.NumRelations();
		for (int level = (int)num_rels - 2; level >= 0 && covered.size() < num_rels; level--) {
			for (auto *set : join_rels[level]) {
				bool has_new = false;
				for (idx_t k = 0; k < set->count; k++) {
					if (covered.find(set->relations[k]) == covered.end()) {
						has_new = true;
						break;
					}
				}
				if (has_new) {
					components.push_back(set);
					for (idx_t k = 0; k < set->count; k++) {
						covered.insert(set->relations[k]);
					}
				}
			}
		}
		while (components.size() > 1) {
			auto *right = components.back();
			components.pop_back();
			auto *left = components.back();
			components.pop_back();
			query_graph_manager.CreateQueryGraphCrossProduct(*left, *right);
			auto connections = query_graph.GetConnections(*left, *right);
			D_ASSERT(!connections.empty());
			auto &node = EmitPair(*left, *right, connections);
			components.push_back(&node.set);
		}
	}
	//SPY: REMOVED RETURN TYPE CHANGED return std::move(final_plan->second);
}

void PlanEnumerator::SolveJoinOrderRandom() {
	std::random_device rd;
	std::mt19937 g(rd());
	vector<reference<JoinRelationSet>> join_relations; // T in the paper
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		join_relations.push_back(query_graph_manager.set_manager.GetJoinRelation(i));
	}
	while (join_relations.size() > 1) {
		idx_t best_left = 0, best_right = 0;
		optional_ptr<DPJoinNode> best_connection;
		int cnt = 0;
		while (true) {
			if(cnt > 10000) {
				//SPY: REMOVED DEBUG std::cout << "random generate failed" << std::endl;
				return SolveJoinOrder();
			}
			std::uniform_int_distribution<int> dist(0, join_relations.size() - 1);
			int i = dist(g);
			int j;
			do {
				j = dist(g);
			} while (j == i);
			auto left = join_relations[i];
			auto right = join_relations[j];
			// check if we can connect these two relations
			auto connection = query_graph.GetConnections(left, right);
			if (!connection.empty()) {
				auto &node = EmitPair(left, right, connection);
				//SPY: REMOVED DOES NOT EXIST UpdateDPTree(node);
				best_connection = &node;
				best_left = i;
				best_right = j;
				break;
			}
			cnt++;
		}
		if (!best_connection) {
			throw InvalidInputException("Query requires a cross-product");
		}
		if (best_right > best_left) {
			join_relations.erase(join_relations.begin() + best_right);
			join_relations.erase(join_relations.begin() + best_left);
		} else {
			join_relations.erase(join_relations.begin() + best_left);
			join_relations.erase(join_relations.begin() + best_right);
		}
		join_relations.push_back(best_connection->set);

	}
	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		bindings.insert(i);
	}
	auto &total_relation = query_graph_manager.set_manager.GetJoinRelation(bindings);
	auto final_plan = plans.find(total_relation);
	//SPY: REMOVED RETURN TYPE CHANGED return std::move(final_plan->second);
}

void PlanEnumerator::SolveJoinOrderLeftDeepRandom() {
	std::random_device rd;
	std::mt19937 g(rd());
	vector<reference<JoinRelationSet>> join_relations; // T in the paper
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		join_relations.push_back(query_graph_manager.set_manager.GetJoinRelation(i));
	}
	optional_ptr<DPJoinNode> best_left_tree = nullptr;
	while (join_relations.size() > 0) {
		idx_t best_left = 0, best_right = 0;
		optional_ptr<DPJoinNode> best_connection;
		int cnt = 0;
		while (true) {
			if(cnt > 10000) {
				//SPY: REMOVED DEBUG std::cout << "random generate failed" << std::endl;
				return SolveJoinOrder();
			}
			std::uniform_int_distribution<int> dist(0, join_relations.size() - 1);
			if (best_left_tree == nullptr) {
				// double max = 0;
				// int i = -1;
				// for(int k = 0; k < join_relations.size(); k++) {
				// 	auto card = cost_model.cardinality_estimator.EstimateCardinalityWithSet<double>(join_relations[k]);
				// 	if (card > max) {
				// 		i = k;
				// 		max = card;
				// 	}
				// }
				int i = dist(g);
				int j;
				do {
					j = dist(g);
				} while (j == i);
				auto left = join_relations[i];
				auto right = join_relations[j];
				// check if we can connect these two relations
				auto connection = query_graph.GetConnections(left, right);
				if (!connection.empty()) {
					auto &node = EmitPair(left, right, connection);
					//SPY: REMOVED DOES NOT EXIST UpdateDPTree(node);
					best_connection = &node;
					best_left = i;
					best_right = j;
					if (best_right > best_left) {
						join_relations.erase(join_relations.begin() + best_right);
						join_relations.erase(join_relations.begin() + best_left);
					} else {
						join_relations.erase(join_relations.begin() + best_left);
						join_relations.erase(join_relations.begin() + best_right);
					}
					break;
				}
			} else {
				int i = dist(g);
				auto right = join_relations[i];
				// check if we can connect these two relations
				auto connection = query_graph.GetConnections(best_left_tree->set, right);
				if (!connection.empty()) {
					auto &node = EmitPair(best_left_tree->set, right, connection);
					//SPY: REMOVED DOES NOT EXIST UpdateDPTree(node);
					best_connection = &node;
					best_right = i;
					join_relations.erase(join_relations.begin() + best_right);
					break;
				}
			}
			cnt++;
		}
		if (!best_connection) {
			throw InvalidInputException("Query requires a cross-product");
		}
		best_left_tree = best_connection;
	}
	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		bindings.insert(i);
	}
	auto &total_relation = query_graph_manager.set_manager.GetJoinRelation(bindings);
	auto final_plan = plans.find(total_relation);
	//SPY: REMOVED RETURN TYPE CHANGED return std::move(final_plan->second);
}

//! Build a left-deep plan whose base-table ordering is taken from
//! `forced_table_order`. See the header comment for the exact shape contract.
//!
//! Implementation notes:
//!   - We map every entry of `forced_table_order` through
//!     RelationManager::relation_mapping to find the corresponding
//!     relation_id. Entries that cannot be mapped (e.g. a table operator
//!     that the join-order optimizer treated as part of a non-reorderable
//!     subtree) are silently skipped.
//!   - After mapping, any relation_id the join-order optimizer knows about
//!     but that the forced order does not mention is appended at the end.
//!     This guarantees the final JoinRelationSet covers every relation so
//!     `QueryGraphManager::Reconstruct` can find a plan entry for the full
//!     set.
//!   - At every step we join the running left subtree (the "accumulator")
//!     with one additional base relation on the right. That means the
//!     resulting DP tree has the shape
//!         (((((T0 |><| T1) |><| T2) |><| T3) ... ) |><| Tn-1)
//!     which is exactly left-deep with the requested ordering.
//!   - If the accumulator has no query-graph edge to the next relation we
//!     insert a cross-product edge on-the-fly via
//!     CreateQueryGraphCrossProduct. This mirrors what SolveJoinOrder does
//!     when it discovers a disconnected component.
//!   - EmitPair stores every intermediate plan in the `plans` map keyed by
//!     JoinRelationSet, which is what `QueryGraphManager::Reconstruct`
//!     later looks up for the full relation set.
void PlanEnumerator::SolveJoinOrderFromTransferOrder(const vector<idx_t> &forced_table_order) {
	auto &relation_manager = query_graph_manager.relation_manager;
	idx_t num_relations = relation_manager.NumRelations();
	if (num_relations == 0) {
		return;
	}

	// 1. Map base-table indices from the transfer order to relation ids.
	//    We keep a `seen` set to detect which relations still need to be
	//    appended at the end (e.g. aggregate/materialized-CTE relations that
	//    are not base tables and therefore not in the transfer graph).
	vector<idx_t> relation_ids;
	relation_ids.reserve(num_relations);
	unordered_set<idx_t> seen;
	for (auto &table_index : forced_table_order) {
		auto it = relation_manager.relation_mapping.find(table_index);
		if (it == relation_manager.relation_mapping.end()) {
			// Table operator is not a relation in this JOO scope (it lives
			// in a non-reorderable subtree). That's fine -- just skip it.
			continue;
		}
		idx_t rel_id = it->second;
		if (seen.insert(rel_id).second) {
			relation_ids.push_back(rel_id);
		}
	}

	// 2. Append every relation the forced order did not already cover.
	//    Sorted by relation id so the result is deterministic.
	vector<idx_t> leftover;
	for (idx_t rel_id = 0; rel_id < num_relations; ++rel_id) {
		if (seen.count(rel_id) == 0) {
			leftover.push_back(rel_id);
		}
	}
	std::sort(leftover.begin(), leftover.end());
	for (auto rel_id : leftover) {
		relation_ids.push_back(rel_id);
	}

	DEBUG_LOG("[SolveJoinOrderFromTransferOrder] num_relations=%zu forced_size=%zu mapped_size=%zu\n",
	          static_cast<size_t>(num_relations), forced_table_order.size(), relation_ids.size());

	D_ASSERT(relation_ids.size() == num_relations);

	// 3. Start the accumulator at the very first relation. This is the
	//    probe side of the bottom-most join in the resulting plan.
	auto *current = &query_graph_manager.set_manager.GetJoinRelation(relation_ids[0]);

	// 4. Walk the remaining relations in order, joining each onto the right
	//    side of the accumulator. EmitPair both constructs the join tree
	//    and inserts it into the DP table used by Reconstruct().
	for (idx_t i = 1; i < relation_ids.size(); ++i) {
		auto &right = query_graph_manager.set_manager.GetJoinRelation(relation_ids[i]);

		auto connections = query_graph.GetConnections(*current, right);
		if (connections.empty()) {
			// No direct query-graph edge -- insert a cross-product edge so
			// we still have something for EmitPair / CreateJoinTree to use.
			query_graph_manager.CreateQueryGraphCrossProduct(*current, right);
			connections = query_graph.GetConnections(*current, right);
			D_ASSERT(!connections.empty());
		}

		auto &node = EmitPair(*current, right, connections);
		current = &node.set;
	}
}

// the plan enumeration is a straight implementation of the paper "Dynamic Programming Strikes Back" by Guido
// Moerkotte and Thomas Neumannn, see that paper for additional info/documentation bonus slides:
// https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
void PlanEnumerator::SolveJoinOrder() {
	bool force_no_cross_product = query_graph_manager.context.config.force_no_cross_product;
	// first try to solve the join order exactly
	if (query_graph_manager.relation_manager.NumRelations() >= THRESHOLD_TO_SWAP_TO_APPROXIMATE) {
		SolveJoinOrderApproximately();
	} else if (!SolveJoinOrderExactly()) {
		// otherwise, if that times out we resort to a greedy algorithm
		SolveJoinOrderApproximately();
	}

	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		bindings.insert(i);
	}
	auto &total_relation = query_graph_manager.set_manager.GetJoinRelation(bindings);
	auto final_plan = plans.find(total_relation);
	if (final_plan == plans.end()) {
		// could not find the final plan
		// this should only happen in case the sets are actually disjunct
		// in this case we need to generate cross product to connect the disjoint sets
		if (force_no_cross_product) {
			throw InvalidInputException(
			    "Query requires a cross-product, but 'force_no_cross_product' PRAGMA is enabled");
		}
		GenerateCrossProducts();
		//! solve the join order again, returning the final plan
		return SolveJoinOrder();
	}
}

} // namespace duckdb
