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
	//! Exact (cost-based) left-deep enumeration with asymmetric cost model
	//! (1.2x right-side penalty). This is the strongest left-deep planner we
	//! have: it still performs full DP enumeration, but restricts the search
	//! space to left-deep shapes and is biased to make the smaller side the
	//! build side.
	BEST_LEFT_DEEP = 1,
	//! Random bushy join tree generation
	RANDOM_BUSHY = 2,
	//! Random left-deep join tree generation
	RANDOM_LEFT_DEEP = 3,
	//! Strictly left-deep plan whose base-table ordering is dictated by
	//! TransferGraphManager::transfer_order (populated by the RPT+
	//! predicate-transfer pass via PickRootAndOrderWithSeed). Does NOT perform
	//! any cost-based enumeration -- it just materializes the transfer order
	//! as a left-deep chain so the probe side of the bottom join is the root
	//! chosen by PickRootAndOrderWithSeed and each subsequent build side is
	//! the next table that was attached to the spanning tree.
	SEEDED_LEFT_DEEP = 4
};

JoinOrderMode JoinOrderModeFromString(const string &str);
string JoinOrderModeToString(JoinOrderMode mode);

} // namespace duckdb
