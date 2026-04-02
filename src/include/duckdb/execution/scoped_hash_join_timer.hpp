//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/scoped_hash_join_timer.hpp
//
// A small RAII helper for accumulating nanosecond timings into an optional
// uint64_t counter.
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#ifdef DUCKDB_ENABLE_HASH_JOIN_TIMERS
#include <chrono>
#endif

namespace duckdb {

class ScopedHashJoinTimer {
public:
#ifdef DUCKDB_ENABLE_HASH_JOIN_TIMERS
	explicit ScopedHashJoinTimer(uint64_t *target_p)
	    : target(target_p), start(std::chrono::steady_clock::now()) {
	}

	~ScopedHashJoinTimer() {
		if (!target) {
			return;
		}
		auto end = std::chrono::steady_clock::now();
		auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
		*target += static_cast<uint64_t>(elapsed_ns);
	}
#else
	explicit ScopedHashJoinTimer(uint64_t *target_p) {
		(void)target_p;
	}

	~ScopedHashJoinTimer() {
	}
#endif

private:
#ifdef DUCKDB_ENABLE_HASH_JOIN_TIMERS
	uint64_t *target;
	std::chrono::steady_clock::time_point start;
#endif
};

} // namespace duckdb

