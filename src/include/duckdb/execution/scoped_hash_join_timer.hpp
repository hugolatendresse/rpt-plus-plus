//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/scoped_hash_join_timer.hpp
//
// A small RAII helper for accumulating nanosecond timings into a counter.
//
// Two constructor overloads are provided to support the two accumulation
// patterns used throughout the hash join code:
//
//   1. `ScopedHashJoinTimer(uint64_t *target, ...)` — non-atomic increment.
//      Use this when many timer scopes fire on the same thread and feed into
//      a thread-local counter inside the per-thread state (e.g. the per-chunk
//      timers in `JoinHashTable::GetRowPointers` writing to fields in
//      `HashJoinOperatorState`). The destructor does a single non-atomic
//      `*target += elapsed_ns`, so the timer is cheap enough to nest in inner
//      loops. The caller is responsible for rolling the thread-local counter
//      into a global atomic at a well-defined boundary (e.g. Combine,
//      FlushLocalTimings).
//
//   2. `ScopedHashJoinTimer(std::atomic<uint64_t> *target, ...)` — atomic
//      fetch_add. Use this at call sites that have no thread-local counter
//      to reuse: one-shot parallel tasks (`HashJoinFinalizeTask` etc.), and
//      coordinator-thread scopes inside event callbacks. The destructor does
//      one `fetch_add(elapsed_ns, std::memory_order_relaxed)`, so each scope
//      costs exactly one atomic op while the call site stays a single line
//      (no surrounding `uint64_t local = 0;` + explicit fetch_add).
//
// Whether or not a particular scope actually does timing is controlled at
// runtime via:
//   - the chosen `target_p` pointer being non-null, AND
//   - the `enabled` argument being true.
//
// When either condition is false, the constructor and destructor do nothing
// (no `now()` calls, no stores). The runtime knob is the
// `enable_hash_join_timers` ClientConfig flag (see `client_config.hpp`); the
// hash-join code mirrors it once into a `const bool enable_timers` field on
// the sink/operator state and passes that into every ScopedHashJoinTimer.
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace duckdb {

class ScopedHashJoinTimer {
public:
	// Plain `uint64_t *` target. Use this when many timer scopes accumulate into
	// the *same* thread-local counter inside a hot loop, and you intend to do
	// one fetch_add into the global atomic at a known boundary (e.g. Combine,
	// FlushLocalTimings). The increment is a single non-atomic `+=`, so it's
	// cheap enough to nest in inner loops.
	//
	// `target_p` may be nullptr; `enabled` may be false. In either case the
	// timer is a no-op and we skip the now() call entirely.
	explicit ScopedHashJoinTimer(uint64_t *target_p, bool enabled = true)
	    : single_threaded_target(enabled ? target_p : nullptr), atomic_target(nullptr) {
		if (single_threaded_target) {
			start = std::chrono::steady_clock::now();
		}
	}

	// `std::atomic<uint64_t> *` target. Use this at call sites that don't have
	// a thread-local accumulator to reuse — one-shot parallel tasks, or
	// coordinator-thread scopes in event callbacks. Destructor does one
	// `fetch_add(..., std::memory_order_relaxed)` so each scope still costs
	// exactly one atomic op, but the call site becomes a single line.
	explicit ScopedHashJoinTimer(std::atomic<uint64_t> *target_p, bool enabled = true)
	    : single_threaded_target(nullptr), atomic_target(enabled ? target_p : nullptr) {
		if (atomic_target) {
			start = std::chrono::steady_clock::now();
		}
	}

	~ScopedHashJoinTimer() {
		if (!single_threaded_target && !atomic_target) {
			return;
		}
		auto end = std::chrono::steady_clock::now();
		auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
		auto ns = static_cast<uint64_t>(elapsed_ns);
		if (single_threaded_target) {
			*single_threaded_target += ns;
		} else {
			atomic_target->fetch_add(ns, std::memory_order_relaxed);
		}
	}

private:
	uint64_t *single_threaded_target;
	std::atomic<uint64_t> *atomic_target;
	// Default-constructed; only meaningful when one of the targets is non-null,
	// in which case the constructor will have overwritten it with `now()`.
	std::chrono::steady_clock::time_point start;
};

} // namespace duckdb
