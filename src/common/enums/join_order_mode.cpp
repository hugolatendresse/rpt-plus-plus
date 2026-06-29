#include "duckdb/common/enums/join_order_mode.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

JoinOrderMode JoinOrderModeFromString(const string &str) {
	auto lower = StringUtil::Lower(str);
	if (lower == "duckdb") {
		return JoinOrderMode::DPHYP;
	} else if (lower == "exact_left_deep") {
		return JoinOrderMode::EXACT_LEFT_DEEP;
	} else if (lower == "random_bushy") {
		return JoinOrderMode::RANDOM_BUSHY;
	} else if (lower == "random_left_deep") {
		return JoinOrderMode::RANDOM_LEFT_DEEP;
	}
	throw InvalidInputException("Unknown join_order_mode: '%s'. Valid options: duckdb, exact_left_deep, random_bushy, random_left_deep", str);
}

string JoinOrderModeToString(JoinOrderMode mode) {
	switch (mode) {
	case JoinOrderMode::DPHYP:
		return "duckdb";
	case JoinOrderMode::EXACT_LEFT_DEEP:
		return "exact_left_deep";
	case JoinOrderMode::RANDOM_BUSHY:
		return "random_bushy";
	case JoinOrderMode::RANDOM_LEFT_DEEP:
		return "random_left_deep";
	default:
		return "duckdb";
	}
}

} // namespace duckdb
