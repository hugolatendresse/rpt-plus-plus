//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/join_order_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

//! Join order enumeration strategy.
//! Controls which algorithm the optimizer uses to explore join orderings.
enum class JoinOrderMode : uint8_t {
	//! Default DuckDB join order optimization (DPhyp + greedy fallback).
	//! Use "duckdb" as the string representation in SET statements.
	DPHYP = 0,
	//! Exact left-deep enumeration with asymmetric cost model (1.2x right-side penalty)
	EXACT_LEFT_DEEP = 1,
	//! Random bushy join tree generation
	RANDOM_BUSHY = 2,
	//! Random left-deep join tree generation
	RANDOM_LEFT_DEEP = 3
};

JoinOrderMode JoinOrderModeFromString(const string &str);
string JoinOrderModeToString(JoinOrderMode mode);

} // namespace duckdb
