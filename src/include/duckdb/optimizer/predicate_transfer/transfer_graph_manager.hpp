#pragma once

#include "duckdb/optimizer/predicate_transfer/table_operator_namager.hpp"
#include "duckdb/optimizer/predicate_transfer/dag.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/vector.hpp"

#include <iostream>

namespace duckdb {

class EdgeInfo {
public:
	EdgeInfo(const LogicalType &type, LogicalOperator &left, const ColumnBinding &left_binding, LogicalOperator &right,
	         const ColumnBinding &right_binding)
	    : return_type(type), left_table(left), left_binding(left_binding), right_table(right),
	      right_binding(right_binding) {
	}

	LogicalType return_type;

	LogicalOperator &left_table;
	ColumnBinding left_binding;

	LogicalOperator &right_table;
	ColumnBinding right_binding;

	bool protect_left = false;
	bool protect_right = false;
};

//! Join Key Table Groups
//! Represents a join-key equivalent class, ie a group of columns
//! that are linked by equality predicates
//! At its core this data structure is a set of table_ids
struct JoinKeyTableGroup {
	LogicalType return_type; // type of the column (must be all the same)
	unordered_set<idx_t> table_ids; // the tables in the equivalent class
	idx_t leader_id; // the canonical table
	ColumnBinding leader_column_binding; // the column that is equal across tables

	JoinKeyTableGroup(const LogicalType &return_type, const idx_t table_id)
	    : return_type(return_type), leader_id(std::numeric_limits<idx_t>::max()) {
		table_ids.insert(table_id);
	}

	void Union(const JoinKeyTableGroup &other) {
		D_ASSERT(return_type == other.return_type);
		table_ids.insert(other.table_ids.begin(), other.table_ids.end());
	}

	void RegisterLeader(const idx_t table_id, const ColumnBinding &column_binding) {
		if (std::numeric_limits<idx_t>::max() == leader_id) {
			leader_id = table_id;
			leader_column_binding = column_binding;
		}
	}

	void Print() const {
		std::cout << "[Leader: " << leader_id << "]\t";
		for (auto &id : table_ids) {
			std::cout << id << " ";
		}
		std::cout << "\n";
	}
};

struct ColumnBindingHashFunc {
	size_t operator()(const ColumnBinding &key) const {
		return std::hash<uint64_t> {}(key.table_index) ^ (std::hash<uint64_t> {}(key.column_index) << 1);
	}
};

using JoinKeySet = unordered_set<ColumnBinding, ColumnBindingHashFunc>;
using TableJoinKeyMap = unordered_map<idx_t, JoinKeySet>;
using BindingParentMap = unordered_map<ColumnBinding, ColumnBinding, ColumnBindingHashFunc>;
using BindingGroupMap = unordered_map<ColumnBinding, shared_ptr<JoinKeyTableGroup>, ColumnBindingHashFunc>;

class TransferGraphManager {
public:
	explicit TransferGraphManager(ClientContext &context) : context(context), table_operator_manager(context) {
	}

	ClientContext &context;
	
	//! Knows which logical op produces each table
	TableOperatorManager table_operator_manager;
	
	//! A DAG of BF Flows
	//! Each node is a Table
	//! Each node has forward_stage_edges.in (nodes I expect an BF from) 
	//! Each node has forward_stage_edges.out (nodes I send a BF to) 
	TransferGraph transfer_graph;
	vector<LogicalOperator *> transfer_order;

public:
	bool Build(LogicalOperator &op);
	void AddFilterPlan(idx_t create_table, const shared_ptr<FilterPlan> &filter_plan, bool reverse);
	void PrintTransferPlan();

private:
	void ExtractEdgesInfo(const vector<reference<LogicalOperator>> &join_operators);
	void CreateOriginTransferPlan();
	void LargestRoot(vector<LogicalOperator *> &sorted_nodes);
	//! Build the predicate-transfer spanning tree using configurable strategies
	//! for root selection and tree growth, then run the forward/backward
	//! edge-wiring pass shared by every strategy combination.
	//! - `seeded_root` toggles between the RPT+ "largest filtered/intermediate"
	//!   root pick and the seed-driven pick over a deterministic candidate
	//!   list.
	//! - `seeded_growth` toggles between the RPT+ greedy `FindEdge` neighbor
	//!   pick and the seed-driven neighbor + parent pick.
	//! - `seed` is consulted only when at least one of the two strategies is
	//!   seeded; the value is advanced via MurmurHash64 after each pick so
	//!   the same initial seed reproduces the same transfer order across runs.
	//! Disconnected components are handled inline: if no edge into the
	//! constructed set remains, a fresh root is picked from the leftovers
	//! using the same root strategy.
	void CreateTransferPlanSPY(bool seeded_root, bool seeded_growth, uint64_t seed);
	// TODO get RPT+ CreateTransferPlanUpdated and make sure that results are the same when settings are supposed to reproduce RPT+
	//! Apply the spanning tree (already populated in `selected_edges`,
	//! `transfer_graph` and `transfer_order`) to wire the per-node forward
	//! and backward Bloom-filter edges. Shared between every strategy combo.
	void WireTransferGraph();

	pair<idx_t, idx_t> FindEdge(const unordered_set<idx_t> &constructed_set,
	                            const unordered_set<idx_t> &unconstructed_set);

private:
	//! Classify all tables into three categories: intermediate table, unfiltered table, and filtered table.
	//! Filtered tables are tables with a predicate (either explicit or pushed down). They make the BFs more worth propagating.
	//! Intermediate tables have no local predicate, but belong to more than one join-key equivalence classes (belong_groups > 1)
	void ClassifyTables();
	void SkipUnfilteredTable(const vector<reference<LogicalOperator>> &joins);

	//! From table id to its join keys
	TableJoinKeyMap table_join_keys;
	//! From join keys to its table groups
	BindingGroupMap table_groups;
	//! Table categories
	unordered_set<idx_t> unfiltered_table;
	unordered_set<idx_t> filtered_table;
	unordered_set<idx_t> intermediate_table;

private:
	unordered_map<idx_t, unordered_map<idx_t, shared_ptr<EdgeInfo>>> neighbor_matrix;
	vector<shared_ptr<EdgeInfo>> selected_edges;
};
} // namespace duckdb
