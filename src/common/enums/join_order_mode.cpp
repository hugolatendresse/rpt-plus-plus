#include "duckdb/common/enums/join_order_mode.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

JoinOrderMode JoinOrderModeFromString(const string &str) {
	auto lower = StringUtil::Lower(str);
	if (lower == "duckdb") {
		return JoinOrderMode::DPHYP;
	} else if (lower == "best_left_deep") {
		return JoinOrderMode::BEST_LEFT_DEEP;
	} else if (lower == "random_bushy") {
		return JoinOrderMode::RANDOM_BUSHY;
	} else if (lower == "random_left_deep") {
		return JoinOrderMode::RANDOM_LEFT_DEEP;
	} else if (lower == "seeded_left_deep") {
		return JoinOrderMode::SEEDED_LEFT_DEEP;
	}
	throw InvalidInputException(
	    "Unknown join_order_mode: '%s'. Valid options: duckdb, best_left_deep, random_bushy, random_left_deep, "
	    "seeded_left_deep",
	    str);
}

string JoinOrderModeToString(JoinOrderMode mode) {
	switch (mode) {
	case JoinOrderMode::DPHYP:
		return "duckdb";
	case JoinOrderMode::BEST_LEFT_DEEP:
		return "best_left_deep";
	case JoinOrderMode::RANDOM_BUSHY:
		return "random_bushy";
	case JoinOrderMode::RANDOM_LEFT_DEEP:
		return "random_left_deep";
	case JoinOrderMode::SEEDED_LEFT_DEEP:
		return "seeded_left_deep";
	default:
		return "duckdb";
	}
}

} // namespace duckdb
