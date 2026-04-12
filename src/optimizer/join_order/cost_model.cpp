#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

CostModel::CostModel(QueryGraphManager &query_graph_manager)
    : query_graph_manager(query_graph_manager), cardinality_estimator() {
}

double CostModel::ComputeCost(DPJoinNode &left, DPJoinNode &right) {
	auto &combination = query_graph_manager.set_manager.Union(left.set, right.set);
	auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);
	auto join_cost = join_card;
	// ExactLeftDeep uses an asymmetric cost model that penalizes the right (build) side
	// to encourage plans where the smaller relation is on the build side.
	if (query_graph_manager.context.config.join_order_mode == JoinOrderMode::EXACT_LEFT_DEEP) {
		return join_cost + left.cost + 1.2 * right.cost;
	}
	return join_cost + left.cost + right.cost;
}

} // namespace duckdb
