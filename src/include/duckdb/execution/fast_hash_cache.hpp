//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/fast_hash_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/selection_vector.hpp"

#include <cstring>

namespace duckdb {

//! FastHashCache is a compact open-addressing hash table that caches recently
//! matched probe entries to accelerate repeated hash join lookups.
//!
//! Each entry stores: [hash (8B)] [full_row (tuple_size B)]
//! The full_row is a copy of the entire data_collection row, so cache hits
//! completely bypass data_collection access for both key matching AND payload
//! gathering (GatherResult / ScanStructure::Next).
//!
//! Thread safety during warmup uses CAS on the hash field to claim slots.
//! After warmup the cache is read-only.
class FastHashCache {
public:
	//! Memory budget for the cache (sized for L3)
	static constexpr idx_t DEFAULT_L3_BUDGET = 16ULL * 1024 * 1024;

	//! Minimum hash-table capacity before the cache is worth creating
	static constexpr idx_t ACTIVATION_THRESHOLD = 10ULL * 1024 * 1024 / sizeof(uint64_t);

	//! @param capacity_p        Number of slots (must be power of 2).
	//! @param row_size_p         Size of the full data_collection row to store per entry.
	//! @param row_copy_offset_p  Offset into the source row from which to start copying
	//!                           (normally 0 to copy the entire row).
	FastHashCache(idx_t capacity_p, idx_t row_size_p, idx_t row_copy_offset_p = 0)
	    : capacity(capacity_p), bitmask(capacity_p - 1), row_size(row_size_p),
	      row_copy_offset(row_copy_offset_p), entry_stride(ComputeEntryStride(row_size_p)) {
		D_ASSERT(IsPowerOfTwo(capacity));
		auto total_bytes = capacity * entry_stride;
		data = make_unsafe_uniq_array_uninitialized<data_t>(total_bytes);
		memset(data.get(), 0, total_bytes);
	}

	//! Find the cache entry whose stored hash matches. 
	//! Only compared hashes! Can have false positives!
	//! Returns pointers to the cached row data (usable by RowMatcher and GatherResult).
	//! On miss, doesn't go to data_collection but records row in cache_miss_sel (and cache_miss_count)
	void ProbeByHash(const hash_t *hashes_dense, idx_t count, const SelectionVector *row_sel, bool has_row_sel,
	                 SelectionVector &cache_candidates_sel, idx_t &cache_candidates_count,
	                 data_ptr_t *cache_result_ptrs, data_ptr_t *cache_rhs_locations,
	                 SelectionVector &cache_miss_sel, idx_t &cache_miss_count) const {

		static constexpr idx_t SLOT_PREFETCH_DIST = 16;

		cache_candidates_count = 0;
		cache_miss_count = 0;

		for (idx_t p = 0; p < MinValue<idx_t>(SLOT_PREFETCH_DIST, count); p++) {
			__builtin_prefetch(GetEntryPtr(hashes_dense[p] & bitmask), 0, 1);
		}

		for (idx_t i = 0; i < count; i++) {
			if (i + SLOT_PREFETCH_DIST < count) {
				__builtin_prefetch(GetEntryPtr(hashes_dense[i + SLOT_PREFETCH_DIST] & bitmask), 0, 1);
			}

			const auto row_index = has_row_sel ? row_sel->get_index(i) : i;
			const auto probe_hash = hashes_dense[i];
			auto slot = probe_hash & bitmask;

			bool found = false;
			while (true) {
				auto entry_ptr = GetEntryPtr(slot);
				const auto stored_hash = LoadHash(entry_ptr);
				if (stored_hash == 0) {
					break;
				}
				if (stored_hash == probe_hash) {
					auto row_ptr = GetRowPtr(entry_ptr);
					cache_result_ptrs[row_index] = row_ptr;
					cache_rhs_locations[row_index] = row_ptr;
					cache_candidates_sel.set_index(cache_candidates_count++, row_index);
					found = true;
					break;
				}
				slot = (slot + 1) & bitmask;
			}
			if (!found) {
				cache_miss_sel.set_index(cache_miss_count++, row_index);
			}
		}
	}

	//! Looks up based on hash and key.
	//! Returns true matches only (no false positives like ProbeByHash).
	//! On match, result_ptrs points to the cached full row (usable by GatherResult).
	template <class T>
	void ProbeAndMatch(const hash_t *hashes_dense, const T *probe_keys, idx_t key_offset, idx_t count,
	                   const SelectionVector *row_sel, bool has_row_sel, data_ptr_t *result_ptrs,
	                   SelectionVector &match_sel, idx_t &match_count, SelectionVector &miss_sel,
	                   idx_t &miss_count) const {

		static constexpr idx_t SLOT_PREFETCH_DIST = 16;

		match_count = 0;
		miss_count = 0;

		for (idx_t p = 0; p < MinValue<idx_t>(SLOT_PREFETCH_DIST, count); p++) {
			__builtin_prefetch(GetEntryPtr(hashes_dense[p] & bitmask), 0, 1);
		}

		for (idx_t i = 0; i < count; i++) {
			if (i + SLOT_PREFETCH_DIST < count) {
				__builtin_prefetch(GetEntryPtr(hashes_dense[i + SLOT_PREFETCH_DIST] & bitmask), 0, 1);
			}

			const auto row_index = has_row_sel ? row_sel->get_index(i) : i;
			const auto probe_hash = hashes_dense[i];
			const auto probe_key = probe_keys[row_index];
			auto slot = probe_hash & bitmask;

			bool found = false;
			while (true) {
				auto entry_ptr = GetEntryPtr(slot);
				const auto stored_hash = LoadHash(entry_ptr);
				if (stored_hash == 0) {
					break;
				}
				if (stored_hash == probe_hash) {
					auto row_ptr = GetRowPtr(entry_ptr);
					auto cache_key = Load<T>(row_ptr + key_offset);
					if (cache_key == probe_key) {
						result_ptrs[row_index] = row_ptr;
						match_sel.set_index(match_count++, row_index);
						found = true;
						break;
					}
				}
				slot = (slot + 1) & bitmask;
			}
			if (!found) {
				miss_sel.set_index(miss_count++, row_index);
			}
		}
	}

	// -----------------------------------------------------------------------
	// Insert (thread-safe via CAS on hash field)
	// -----------------------------------------------------------------------
	std::atomic<idx_t> insert_new{0};
	std::atomic<idx_t> insert_dup{0};

	//! Insert an entry. Copies row_size bytes from row_data_ptr + row_copy_offset.
	//! Thread-safe: uses CAS to claim empty slots. Same-hash duplicates are no-ops.
	void Insert(hash_t hash, const_data_ptr_t row_data_ptr) {
		auto slot = hash & bitmask;
		while (true) {
			auto entry_ptr = GetEntryPtr(slot);
			auto hash_ptr = reinterpret_cast<std::atomic<hash_t> *>(entry_ptr);

			hash_t expected = 0;
			if (hash_ptr->compare_exchange_strong(expected, hash, std::memory_order_acq_rel)) {
				memcpy(GetRowPtr(entry_ptr), row_data_ptr + row_copy_offset, row_size);
				insert_new.fetch_add(1, std::memory_order_relaxed);
				return;
			}
			if (expected == hash) {
				insert_dup.fetch_add(1, std::memory_order_relaxed);
				return;
			}
			slot = (slot + 1) & bitmask;
		}
	}

	// -----------------------------------------------------------------------
	// Accessors
	// -----------------------------------------------------------------------
	idx_t GetCapacity() const {
		return capacity;
	}

	idx_t CountOccupiedEntries() const {
		idx_t count = 0;
		for (idx_t s = 0; s < capacity; s++) {
			if (LoadHash(GetEntryPtr(s)) != 0) {
				count++;
			}
		}
		return count;
	}

	idx_t GetRowSize() const {
		return row_size;
	}

	//! Largest power-of-2 capacity that fits within the budget.
	static idx_t ComputeCapacity(idx_t row_size, idx_t l3_budget = DEFAULT_L3_BUDGET) {
		auto stride = ComputeEntryStride(row_size);
		auto raw = l3_budget / stride;
		if (raw < 64) {
			return 64;
		}
		auto pot = NextPowerOfTwo(raw);
		if (pot > raw) {
			pot >>= 1;
		}
		return pot;
	}

private:
	static constexpr idx_t HEADER_SIZE = sizeof(hash_t); // 8 bytes (hash only, no stored pointer)

	static idx_t ComputeEntryStride(idx_t row_size) {
		return (HEADER_SIZE + row_size + 7) & ~idx_t(7);
	}

	inline data_ptr_t GetEntryPtr(idx_t slot) const {
		return data.get() + slot * entry_stride;
	}

	static inline hash_t LoadHash(const data_ptr_t entry_ptr) {
		hash_t h;
		memcpy(&h, entry_ptr, sizeof(hash_t));
		return h;
	}

	//! Pointer to the cached row data within an entry (starts right after hash).
	static inline data_ptr_t GetRowPtr(data_ptr_t entry_ptr) {
		return entry_ptr + HEADER_SIZE;
	}

	idx_t capacity;
	idx_t bitmask;
	idx_t row_size;
	idx_t row_copy_offset;
	idx_t entry_stride;
	unsafe_unique_array<data_t> data;
};

} // namespace duckdb
