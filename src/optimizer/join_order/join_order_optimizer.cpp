#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/optimizer/join_order/plan_enumerator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

JoinOrderOptimizer::JoinOrderOptimizer(ClientContext &context) : context(context), query_graph_manager(context) {
}

JoinOrderOptimizer JoinOrderOptimizer::CreateChildOptimizer() {
	JoinOrderOptimizer child_optimizer(context);
	child_optimizer.materialized_cte_stats = materialized_cte_stats;
	child_optimizer.delim_scan_stats = delim_scan_stats;
	// Propagate the forced table order so that JoinOrderMode::SEEDED_LEFT_DEEP
	// works even when the reorderable join block sits below non-reorderable
	// operators (ORDER BY / AGGREGATE / PROJECTION / ...). In that setup the
	// top-level Optimize() call descends through a chain of child optimizers
	// before it reaches the actual join subtree, so every child needs to know
	// about the forced order too. PlanEnumerator::SolveJoinOrderFromTransferOrder
	// only uses the subset of forced entries that the child's RelationManager
	// actually knows about (the rest are silently skipped), and appends any
	// child-local relations that were not in the forced order at the end, so
	// unconditional propagation is safe for disjoint / partial subtrees.
	child_optimizer.forced_table_order = forced_table_order;
	return child_optimizer;
}

unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan,
                                                         optional_ptr<RelationStats> stats) {

	// make sure query graph manager has not extracted a relation graph already
	LogicalOperator *op = plan.get();

	// extract the relations that go into the hyper graph.
	// We optimize the children of any non-reorderable operations we come across.
	bool reorderable = query_graph_manager.Build(*this, *op);

	// get relation_stats here since the reconstruction process will move all relations.
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();
	unique_ptr<LogicalOperator> new_logical_plan = nullptr;

	if (reorderable) {
		// query graph now has filters and relations
		auto cost_model = CostModel(query_graph_manager);

		// Initialize a plan enumerator.
		auto plan_enumerator =
		    PlanEnumerator(query_graph_manager, cost_model, query_graph_manager.GetQueryGraphEdges());

		// Initialize the leaf/single node plans
		plan_enumerator.InitLeafPlans();
		switch (context.config.join_order_mode) {
		case JoinOrderMode::BEST_LEFT_DEEP:
			plan_enumerator.SolveJoinOrderLeftDeep();
			break;
		case JoinOrderMode::RANDOM_BUSHY:
			plan_enumerator.SolveJoinOrderRandom();
			break;
		case JoinOrderMode::RANDOM_LEFT_DEEP:
			plan_enumerator.SolveJoinOrderLeftDeepRandom();
			break;
		case JoinOrderMode::SEEDED_LEFT_DEEP:
			// The forced order is installed on the top-level optimizer from
			// optimizer.cpp and propagated to child optimizers via
			// CreateChildOptimizer(). If we still end up with an empty forced
			// order (e.g. the RPT+ transfer graph early-exited because there
			// were fewer than 2 base tables, or the user set SEEDED_LEFT_DEEP
			// without use_seeded_transfer_order actually producing anything)
			// fall back to SolveJoinOrderLeftDeep() so we still produce a
			// left-deep shape instead of crashing.
			if (forced_table_order.empty()) {
				plan_enumerator.SolveJoinOrderLeftDeep();
			} else {
				plan_enumerator.SolveJoinOrderFromTransferOrder(forced_table_order);
			}
			break;
		case JoinOrderMode::DPHYP:
		default:
			plan_enumerator.SolveJoinOrder();
			break;
		}
		// now reconstruct a logical plan from the query graph plan
		query_graph_manager.plans = &plan_enumerator.GetPlans();

		new_logical_plan = query_graph_manager.Reconstruct(std::move(plan));
	} else {
		new_logical_plan = std::move(plan);
		if (relation_stats.size() == 1) {
			new_logical_plan->estimated_cardinality = relation_stats.at(0).cardinality;
			new_logical_plan->has_estimated_cardinality = true;
		}
	}

	// Propagate up a stats object from the top of the new_logical_plan if stats exist.
	if (stats) {
		auto cardinality = new_logical_plan->EstimateCardinality(context);
		auto bindings = new_logical_plan->GetColumnBindings();
		auto new_stats = RelationStatisticsHelper::CombineStatsOfReorderableOperator(bindings, relation_stats);
		new_stats.cardinality = cardinality;
		RelationStatisticsHelper::CopyRelationStats(*stats, new_stats);
	} else {
		// starts recursively setting cardinality
		new_logical_plan->EstimateCardinality(context);
	}

	if (new_logical_plan->type == LogicalOperatorType::LOGICAL_EXPLAIN) {
		new_logical_plan->SetEstimatedCardinality(3);
	}

	return new_logical_plan;
}

void JoinOrderOptimizer::AddMaterializedCTEStats(idx_t index, RelationStats &&stats) {
	materialized_cte_stats.emplace(index, std::move(stats));
}

RelationStats JoinOrderOptimizer::GetMaterializedCTEStats(idx_t index) {
	auto it = materialized_cte_stats.find(index);
	if (it == materialized_cte_stats.end()) {
		throw InternalException("Unable to find materialized CTE stats with index %llu", index);
	}
	return it->second;
}

void JoinOrderOptimizer::AddDelimScanStats(RelationStats &stats) {
	delim_scan_stats = &stats;
}

RelationStats JoinOrderOptimizer::GetDelimScanStats() {
	if (!delim_scan_stats) {
		throw InternalException("Unable to find delim scan stats!");
	}
	return *delim_scan_stats;
}

void JoinOrderOptimizer::SetForcedTableOrder(vector<idx_t> order) {
	forced_table_order = std::move(order);
}

} // namespace duckdb
