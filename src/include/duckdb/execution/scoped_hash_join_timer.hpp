//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/scoped_hash_join_timer.hpp
//
// A small RAII helper for accumulating nanosecond timings into an optional
// uint64_t counter.
//
// Whether or not a particular scope actually does timing is controlled at 
// runtime via:
//   - the `target_p` pointer being non-null, AND
//   - the `enabled` argument being true.
//
// When either condition is false, the constructor and destructor do nothing.
//
// The runtime knob is the `enable_hash_join_timers` ClientConfig flag
// (see `client_config.hpp`).
//===----------------------------------------------------------------------===//

#pragma once

#include <chrono>
#include <cstdint>

namespace duckdb {

class ScopedHashJoinTimer {
public:
	// `target_p` may be nullptr; `enabled` may be false. In either case the
	// timer is a no-op and we skip the now() call entirely.
	explicit ScopedHashJoinTimer(uint64_t *target_p, bool enabled = true)
	    : target(enabled ? target_p : nullptr) {
		if (target) {
			start = std::chrono::steady_clock::now();
		}
	}

	~ScopedHashJoinTimer() {
		if (!target) {
			return;
		}
		auto end = std::chrono::steady_clock::now();
		auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
		*target += static_cast<uint64_t>(elapsed_ns);
	}

private:
	uint64_t *target;
	// Default-constructed; only meaningful when `target` is non-null, in which
	// case the constructor will have overwritten it with `now()`.
	std::chrono::steady_clock::time_point start;
};

} // namespace duckdb
