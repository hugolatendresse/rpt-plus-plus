#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {

//! This helps answer questions such as "Which logical op produces rows for a given table"
class TableOperatorManager {
public:
	explicit TableOperatorManager(ClientContext &context) : context(context) {
	}

	ClientContext &context;

	//! All operators that produce rows (Scans, etc.) sorted by estimated cardinality
	vector<LogicalOperator *> sorted_table_operators;
	
	//! key: DuckDB's table_index
	//! value: the LogicalOperator that produces the rows for that table 
	unordered_map<idx_t, LogicalOperator *> table_operators;

public:
	vector<reference<LogicalOperator>> ExtractOperators(LogicalOperator &plan);
	void SortTableOperators();

	LogicalOperator *GetTableOperator(idx_t table_idx);
	idx_t GetTableOperatorOrder(const LogicalOperator *node);
	ColumnBinding GetRenaming(ColumnBinding col_binding);

	static idx_t GetScalarTableIndex(LogicalOperator *op);
	static bool OperatorNeedsRelation(LogicalOperatorType op_type);

private:
	void AddTableOperator(LogicalOperator *op);
	void ExtractOperatorsInternal(LogicalOperator &plan, vector<reference<LogicalOperator>> &joins);

	struct HashFunc {
		size_t operator()(const ColumnBinding &key) const {
			return std::hash<uint64_t> {}(key.table_index) ^ (std::hash<uint64_t> {}(key.column_index) << 1);
		}
	};
	unordered_map<ColumnBinding, ColumnBinding, HashFunc> rename_col_bindings;
};
} // namespace duckdb
