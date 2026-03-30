#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/debug_log.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/selection_vector.hpp"

#include <atomic>
#include <cstring>

namespace duckdb {

//! TieredHashCache is a hash table that caches recently
//! matched probe entries to accelerate repeated hash join lookups.
//!
//! Each entry stores: [Tag (2 bytes)] [full_row from data_collection]
//! Using a 16-bit tag instead of a 64-bit hash shrinks each entry.
//! The full_row is a copy of the entire data_collection row, so cache hits
//! bypass data_collection access for both key and payload
//!
//! If the build side has duplicate keys, only the first of the chain will be
//! copied to the THC, and others will need to be accessed from data_collection.
//! That is why we copy the next_pointer as part of the data_collection row.
//! Row chains only happen for identical keys in data_collection.
//!
//! Having unique keys guarantees no chaining (even upon 64-bit hash collisions).
//! However, is there are hash collisions from different keys on the build side,
//! only one entry will be added to the THC, and others will fall back to regular
//! probing.
//!
//! Thread safety is simply based on compare-and-swap (check if entry is empty)
class TieredHashCache {
public:
	using tag_t = uint16_t;

	//! Maximum fraction of capacity that may be filled
	//! Beyond this load factor, Insert silently drops new entries to avoid
	//! pathological linear-probing chains (the extreme case being an infinite loop).
	static constexpr double MAX_LOAD_FACTOR = 0.875; // TODO this should be a config parameter

	//! @param capacity_p is the number of slots to create (must be a power of 2)
	//! @param row_size_p is the number of bytes in each row of data_collection.
	//!            This is smaller than the entry size of each row of our
	//!            THC since the latter also includes a hash
	//! @param key_offset_in_row_p byte offset of key within the row (after validity bytes)
	//! @param row_copy_offset_p how many bytes to skip over in each data_collection row before starting copying into
	//! the fast cache
	TieredHashCache(idx_t capacity_p, idx_t row_size_p, idx_t key_offset_in_row_p, idx_t row_copy_offset_p = 0)
	    : capacity(capacity_p), bitmask(capacity_p - 1), row_size(row_size_p), key_offset_in_row(key_offset_in_row_p),
	      row_copy_offset(row_copy_offset_p), entry_stride(ComputeEntryStride(row_size_p)),
	      max_fill(static_cast<idx_t>(capacity_p * MAX_LOAD_FACTOR)), unsafe_fill_count(0) {
		D_ASSERT(IsPowerOfTwo(capacity)); // Needed for bitmask logic
		auto total_bytes = capacity * entry_stride;
		data = make_unsafe_uniq_array_uninitialized<data_t>(total_bytes);
		base_ptr = data.get();
		memset(base_ptr, 0, total_bytes);
	}

	//! Find the cache entry whose tag matches an input hash.
	//! Only compares 16-bit tags, which can lead to a false positive.
	//! Returns a pointer to the cached row data (usable by RowMatcher and GatherResult).
	//! On miss, doesn't go to data_collection, but records the row in cache_miss_sel (and cache_miss_count)
	//! @param cache_miss_sel holds the densely packed indices of `hashes_dense` that did not
	//!                       get a match in the THC.
	//! @tparam HAS_ROW_SEL compile-time constant eliminating the per-iteration has_row_sel branch
	template <bool HAS_ROW_SEL>
	void ProbeByHash(const hash_t *hashes_dense, idx_t count, const SelectionVector *row_sel,
	                 SelectionVector &cache_candidates_sel, idx_t &cache_candidates_count,
	                 data_ptr_t *cache_result_ptrs, data_ptr_t *cache_rhs_locations, SelectionVector &cache_miss_sel,
	                 idx_t &cache_miss_count) const {

		static constexpr idx_t SLOT_PREFETCH_DIST = 16;

		cache_candidates_count = 0;
		cache_miss_count = 0;

		for (idx_t p = 0; p < MinValue<idx_t>(SLOT_PREFETCH_DIST, count); p++) {
			__builtin_prefetch(GetEntryPtr(hashes_dense[p] & bitmask), 0, 3);
		}

		for (idx_t i = 0; i < count; i++) {
			if (i + SLOT_PREFETCH_DIST < count) {
				__builtin_prefetch(GetEntryPtr(hashes_dense[i + SLOT_PREFETCH_DIST] & bitmask), 0, 3);
			}

			const auto row_index = HAS_ROW_SEL ? row_sel->get_index(i) : i;
			const auto probe_tag = ComputeTag(hashes_dense[i]);
			auto slot = hashes_dense[i] & bitmask;

			auto entry_ptr = GetEntryPtr(slot);
			auto stored_tag = LoadTag(entry_ptr);

			if (__builtin_expect(stored_tag == probe_tag, 1)) {
				auto row_ptr = GetRowPtr(entry_ptr);
				cache_result_ptrs[row_index] = row_ptr;
				cache_rhs_locations[row_index] = row_ptr;
				cache_candidates_sel.set_index(cache_candidates_count++, row_index);
				continue;
			}
			if (stored_tag == 0) {
				cache_miss_sel.set_index(cache_miss_count++, row_index);
				continue;
			}

			bool found = false;
			for (idx_t probes = 1; probes < MAX_PROBE_DISTANCE; probes++) {
				slot = (slot + 1) & bitmask;
				entry_ptr = GetEntryPtr(slot);
				stored_tag = LoadTag(entry_ptr);
				if (stored_tag == 0) {
					break;
				}
				if (stored_tag == probe_tag) {
					auto row_ptr = GetRowPtr(entry_ptr);
					cache_result_ptrs[row_index] = row_ptr;
					cache_rhs_locations[row_index] = row_ptr;
					cache_candidates_sel.set_index(cache_candidates_count++, row_index);
					found = true;
					break;
				}
			}
			if (!found) {
				cache_miss_sel.set_index(cache_miss_count++, row_index);
			}
		}
	}

	//! Looks up based on tag and key (in a single phase).
	//! Returns true matches only (no false positives like ProbeByHash).
	//! On match, result_ptrs points to the cached full row (usable by GatherResult).
	//! @param miss_sel holds the densely packed indices of `probe_keys` that did not
	//!                 get a match in the THC
	//! @tparam HAS_ROW_SEL compile-time constant eliminating the per-iteration has_row_sel branch
	template <class T, bool HAS_ROW_SEL>
	void ProbeAndMatch(const hash_t *hashes_dense, const T *probe_keys, idx_t count, const SelectionVector *row_sel,
	                   data_ptr_t *result_ptrs, SelectionVector &match_sel, idx_t &match_count,
	                   SelectionVector &miss_sel, idx_t &miss_count) const {
		static constexpr idx_t SLOT_PREFETCH_DIST = 16;

		match_count = 0;
		miss_count = 0;

		// Constantly prefetch 16 probes ahead
		// TODO test again which value works best
		for (idx_t p = 0; p < MinValue<idx_t>(SLOT_PREFETCH_DIST, count); p++) {
			__builtin_prefetch(GetEntryPtr(hashes_dense[p] & bitmask), 0, 3);
		}

		for (idx_t i = 0; i < count; i++) {
			if (i + SLOT_PREFETCH_DIST < count) {
				__builtin_prefetch(GetEntryPtr(hashes_dense[i + SLOT_PREFETCH_DIST] & bitmask), 0, 3);
			}

			const auto row_index = HAS_ROW_SEL ? row_sel->get_index(i) : i;
			const auto probe_tag = ComputeTag(hashes_dense[i]);
			const auto probe_key = probe_keys[row_index];
			auto slot = hashes_dense[i] & bitmask;

			auto entry_ptr = GetEntryPtr(slot);
			auto stored_tag = LoadTag(entry_ptr);

			if (__builtin_expect(stored_tag == probe_tag, 1)) {
				auto row_ptr = GetRowPtr(entry_ptr);
				if (__builtin_expect(Load<T>(row_ptr + key_offset_in_row) == probe_key, 1)) {
					result_ptrs[row_index] = row_ptr;
					match_sel.set_index(match_count++, row_index);
					continue;
				}
			} else if (stored_tag == 0) {
				miss_sel.set_index(miss_count++, row_index);
				continue;
			}

			bool found = false;
			for (idx_t probes = 1; probes < MAX_PROBE_DISTANCE; probes++) {
				slot = (slot + 1) & bitmask; // linear probing
				entry_ptr = GetEntryPtr(slot);
				stored_tag = LoadTag(entry_ptr);
				if (stored_tag == 0) {
					break;
				}
				if (stored_tag == probe_tag) {
					auto row_ptr = GetRowPtr(entry_ptr);
					if (Load<T>(row_ptr + key_offset_in_row) == probe_key) {
						result_ptrs[row_index] = row_ptr;
						match_sel.set_index(match_count++, row_index);
						found = true;
						break;
					}
				}
			}
			if (!found) {
				miss_sel.set_index(miss_count++, row_index);
			}
		}
	}

	//! Counts how many times an Insert calls actually inserts a new cache entry
	std::atomic<idx_t> new_inserts_count {0};

	//! Counts how many times Insert does NOT insert an entry because its hash is already in the table
	std::atomic<idx_t> dup_inserts_count {0};

	//! Inserts an entry, including the row.
	//! Returns true if a genuinely new entry was inserted, false otherwise
	//! (duplicate hash, table full, or probe distance exceeded).
	bool Insert(hash_t hash, const_data_ptr_t row_data_ptr) {
		// Refuse to insert once we've reached the maximum load factor.
		// Without this guard the unbounded linear-probing loop below can
		// spin forever when the table is (nearly) full.
		if (new_inserts_count.load(std::memory_order_relaxed) >= max_fill) {
			return false; // TODO is there a way to communicate that to JoinHashTable to avoid having to try to insert
			              // thousands of additional times?
		}
		const auto tag = ComputeTag(hash);
		auto slot = hash & bitmask;
		for (idx_t probes = 0; probes < MAX_PROBE_DISTANCE; probes++) {
			auto entry_ptr = GetEntryPtr(slot);
			auto tag_atomic = reinterpret_cast<std::atomic<tag_t> *>(entry_ptr);

			tag_t expected = 0;
			if (tag_atomic->compare_exchange_strong(expected, tag, std::memory_order_acq_rel)) {
				memcpy(GetRowPtr(entry_ptr), row_data_ptr + row_copy_offset, row_size);
				new_inserts_count.fetch_add(1, std::memory_order_relaxed);
				return true;
			}
			if (expected == tag) {
				// Don't try linear probing if the hashes perfect match. TODO could try linear probing here too
				dup_inserts_count.fetch_add(1, std::memory_order_relaxed);
				return false;
			}

			slot = (slot + 1) & bitmask;
		}
		return false;
	}

	//! Non-atomic insert for single-threaded use. Avoids CAS overhead.
	//! The caller MUST guarantee no concurrent writers.
	bool InsertUnsafe(hash_t hash, const_data_ptr_t row_data_ptr) {
		if (unsafe_fill_count >= max_fill) {
			return false;
		}
		const auto tag = ComputeTag(hash);
		auto slot = hash & bitmask;
		for (idx_t probes = 0; probes < MAX_PROBE_DISTANCE; probes++) {
			auto entry_ptr = GetEntryPtr(slot);
			tag_t stored = LoadTag(entry_ptr);
			if (stored == 0) {
				memcpy(entry_ptr, &tag, sizeof(tag_t));
				memcpy(GetRowPtr(entry_ptr), row_data_ptr + row_copy_offset, row_size);
				unsafe_fill_count++;
				return true;
			}
			if (stored == tag) {
				return false;
			}
			slot = (slot + 1) & bitmask;
		}
		return false;
	}

	//! Sync the atomic `new_inserts_count` counter from the non-atomic `unsafe_fill_count`.
	//! Must be called after a batch of InsertUnsafe calls so that IsFull() and
	//! debug logging see the correct fill level.
	//! Exists so that the unsafe insertion path is compatible with the rest of the THC, which
	//! expects the atomic `new_inserts_count` to be updated.
	void SyncCountersFromUnsafe() {
		new_inserts_count.store(unsafe_fill_count, std::memory_order_relaxed);
	}

	idx_t GetCapacity() const {
		return capacity;
	}

	//! Returns true when the THC has reached its maximum load factor
	//! and will silently drop any further Insert calls.
	//! Used by the adaptive logic to skip collection phases
	//! when the cache is saturated and no new entries can be added.
	bool IsFull() const {
		return new_inserts_count.load(std::memory_order_relaxed) >= max_fill;
	}

	idx_t CountOccupiedEntries() const {
		idx_t count = 0;
		for (idx_t s = 0; s < capacity; s++) {
			if (LoadTag(GetEntryPtr(s)) != 0) {
				count++;
			}
		}
		return count;
	}

	idx_t GetRowSize() const {
		return row_size;
	}

	//! Largest power-of-2 capacity that fits within the budget.
	//! Returns the number of entries we can have in the THC
	static idx_t ComputeCapacity(idx_t row_size, idx_t l3_budget) {
		auto stride = ComputeEntryStride(row_size);
		auto raw = l3_budget / stride;
		if (raw < 64) {
			return 64;
		}
		auto pot = NextPowerOfTwo(raw);
		while (pot > raw) {
			pot >>= 1;
		}
		return pot;
	}

private:
	// We store the tags but not pointers
	// That allows faster linear probing
	// Pointers are not needed since we are copying the whole payload (TODO for now?)
	static constexpr idx_t HEADER_SIZE = sizeof(tag_t);

	//! Safety cap for linear probing in ProbeAndMatch.
	//! If we exceed this many probes we treat the lookup as a cache miss.
	static constexpr idx_t MAX_PROBE_DISTANCE = 10;

	static idx_t ComputeEntryStride(idx_t row_size) {
		idx_t stride = (HEADER_SIZE + row_size + 7) & ~idx_t(7);
		DEBUG_LOG("[THC] Stride is %lu bytes\n", stride);
		return stride;
	}

	__attribute__((always_inline)) static inline tag_t ComputeTag(hash_t h) {
		auto tag = static_cast<tag_t>(h >> 48);
		return tag == 0 ? 1 : tag;
	}

	__attribute__((always_inline)) inline data_ptr_t GetEntryPtr(idx_t slot) const {
		return base_ptr + slot * entry_stride;
	}

	__attribute__((always_inline)) static inline tag_t LoadTag(const data_ptr_t entry_ptr) {
		tag_t h;
		memcpy(&h, entry_ptr, sizeof(tag_t));
		return h;
	}

	__attribute__((always_inline)) static inline data_ptr_t GetRowPtr(data_ptr_t entry_ptr) {
		return entry_ptr + HEADER_SIZE;
	}

	idx_t capacity; // Number of entries the THC can fit
	idx_t bitmask;
	idx_t row_size;
	idx_t key_offset_in_row;
	idx_t row_copy_offset;
	idx_t entry_stride;
	idx_t max_fill;          //! capacity * MAX_LOAD_FACTOR — Insert refuses beyond this
	idx_t unsafe_fill_count; //! Non-atomic counter for InsertSafe
	unsafe_unique_array<data_t> data;
	data_ptr_t base_ptr; //! Cached raw pointer from data.get() for hot-path access
};

} // namespace duckdb
