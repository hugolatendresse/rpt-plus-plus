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
//! It sits logically in front of the main JoinHashTable. On a cache hit the
//! caller can skip both ProbeForPointers (pointer-table access) and Match
//! (data_collection key comparison), because the cache stores a copy of the
//! serialized key data alongside the data_collection row pointer.
//!
//! The cache is shared across all probe threads. Thread safety relies on write
//! idempotency: the same key always produces the exact same cache entry, so
//! concurrent inserts are harmless. A torn read during warmup simply fails the
//! hash/key comparison and falls through to the regular (correct) path.
class FastHashCache {
public:
	//! Memory budget for the cache (fits comfortably in L3)
	static constexpr idx_t DEFAULT_L3_BUDGET = 16ULL * 1024 * 1024;

	//! Minimum hash-table capacity before the cache is worth creating
	static constexpr idx_t ACTIVATION_THRESHOLD = 1ULL * 1024 * 1024 / sizeof(uint64_t);

	//! Create a cache with the given capacity (must be power of 2) and the
	//! size of the mini-row (validity bytes + equality key columns).
	FastHashCache(idx_t capacity_p, idx_t mini_row_size_p)
	    : capacity(capacity_p), bitmask(capacity_p - 1), mini_row_size(mini_row_size_p),
	      entry_stride(ComputeEntryStride(mini_row_size_p)) {
		D_ASSERT(IsPowerOfTwo(capacity));
		auto total_bytes = capacity * entry_stride;
		data = make_unsafe_uniq_array_uninitialized<data_t>(total_bytes);
		memset(data.get(), 0, total_bytes);
	}

	//! Probe the cache for hash matches. For each of the `count` probe hashes
	//! (selected by `row_sel` if `has_row_sel` is true), find the first cache
	//! entry whose stored hash equals the probe hash.
	//!
	//! On return:
	//!   - cache_candidates_sel / cache_candidates_count: indices that found a
	//!     hash match in the cache (need key comparison via RowMatcher).
	//!   - cache_result_ptrs: for each candidate, the data_collection row ptr
	//!     stored in the cache entry.
	//!   - cache_rhs_row_locations: for each candidate, pointer to the mini-row
	//!     inside the cache (for RowMatcher to compare keys against).
	//!   - cache_miss_sel / cache_miss_count: indices with no cache entry.
	void ProbeByHash(const hash_t *hashes_dense, idx_t count, const SelectionVector *row_sel, bool has_row_sel,
	                 SelectionVector &cache_candidates_sel, idx_t &cache_candidates_count,
	                 data_ptr_t *cache_result_ptrs, data_ptr_t *cache_rhs_locations,
	                 SelectionVector &cache_miss_sel, idx_t &cache_miss_count) const {

		cache_candidates_count = 0;
		cache_miss_count = 0;

		for (idx_t i = 0; i < count; i++) {
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
					cache_result_ptrs[row_index] = LoadPointer(entry_ptr);
					cache_rhs_locations[row_index] = GetMiniRowPtr(entry_ptr);
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

	//! Single-pass probe: combines hash lookup and key comparison.
	//! For single-column fixed-size keys, this avoids a separate RowMatcher pass
	//! and keeps the cache entry in L1 while comparing the key.
	//!
	//! Prefetches for data_collection rows (for ScanStructure::Next) are issued
	//! in groups of PREFETCH_GROUP entries to avoid polluting the tight probe
	//! loop while still giving prefetches time to complete.
	//!
	//! On return:
	//!   - match_sel / match_count: confirmed matches (hash + key match).
	//!   - result_ptrs[row_index]: data_collection pointer for each match.
	//!   - miss_sel / miss_count: no cache entry or key mismatch.
	template <class T>
	void ProbeAndMatch(const hash_t *hashes_dense, const T *probe_keys, idx_t key_offset, idx_t count,
	                   const SelectionVector *row_sel, bool has_row_sel, data_ptr_t *result_ptrs,
	                   SelectionVector &match_sel, idx_t &match_count, SelectionVector &miss_sel,
	                   idx_t &miss_count) const {

		static constexpr idx_t PREFETCH_GROUP = 64;

		match_count = 0;
		miss_count = 0;
		idx_t prefetch_cursor = 0;

		for (idx_t i = 0; i < count; i++) {
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
					auto cache_key = Load<T>(GetMiniRowPtr(entry_ptr) + key_offset);
					if (cache_key == probe_key) {
						result_ptrs[row_index] = LoadPointer(entry_ptr);
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

			// Issue grouped prefetches for data_collection rows
			if (((i + 1) & (PREFETCH_GROUP - 1)) == 0) {
				for (idx_t p = prefetch_cursor; p < match_count; p++) {
					__builtin_prefetch(result_ptrs[match_sel.get_index(p)], 0, 3);
				}
				prefetch_cursor = match_count;
			}
		}
		// Prefetch remaining matches
		for (idx_t p = prefetch_cursor; p < match_count; p++) {
			__builtin_prefetch(result_ptrs[match_sel.get_index(p)], 0, 3);
		}
	}

	//! Insert an entry into the cache. Copies the first `mini_row_size` bytes
	//! from `row_data_ptr` (the start of the data_collection row) as key data.
	void Insert(hash_t hash, data_ptr_t data_collection_ptr, const_data_ptr_t row_data_ptr) {
		if (hash == 0) {
			return; // hash 0 is the empty-slot sentinel; cannot cache
		}
		auto slot = hash & bitmask;

		while (true) {
			auto entry_ptr = GetEntryPtr(slot);
			const auto stored_hash = LoadHash(entry_ptr);

			if (stored_hash == 0) {
				// Empty slot -- write the entry
				StoreHash(entry_ptr, hash);
				StorePointer(entry_ptr, data_collection_ptr);
				memcpy(GetMiniRowPtr(entry_ptr), row_data_ptr, mini_row_size);
				return;
			}
			if (stored_hash == hash) {
				// Already present (idempotent write from another thread or
				// same thread on a previous batch). Nothing to do.
				return;
			}
			slot = (slot + 1) & bitmask;
		}
	}

	idx_t GetCapacity() const {
		return capacity;
	}
	idx_t GetMiniRowSize() const {
		return mini_row_size;
	}

	//! Compute an appropriate cache capacity given a memory budget and per-
	//! entry mini-row size.
	static idx_t ComputeCapacity(idx_t mini_row_size, idx_t l3_budget = DEFAULT_L3_BUDGET) {
		auto stride = ComputeEntryStride(mini_row_size);
		auto raw = l3_budget / stride;
		if (raw < 64) {
			return 64;
		}
		return NextPowerOfTwo(raw);
	}

private:
	static constexpr idx_t HEADER_SIZE = sizeof(hash_t) + sizeof(data_ptr_t); // 16 bytes

	static idx_t ComputeEntryStride(idx_t mini_row_size) {
		// Round up to next multiple of 8 for alignment
		return (HEADER_SIZE + mini_row_size + 7) & ~idx_t(7);
	}

	inline data_ptr_t GetEntryPtr(idx_t slot) const {
		return data.get() + slot * entry_stride;
	}

	static inline hash_t LoadHash(const data_ptr_t entry_ptr) {
		hash_t h;
		memcpy(&h, entry_ptr, sizeof(hash_t));
		return h;
	}

	static inline void StoreHash(data_ptr_t entry_ptr, hash_t h) {
		memcpy(entry_ptr, &h, sizeof(hash_t));
	}

	static inline data_ptr_t LoadPointer(const data_ptr_t entry_ptr) {
		data_ptr_t p;
		memcpy(&p, entry_ptr + sizeof(hash_t), sizeof(data_ptr_t));
		return p;
	}

	static inline void StorePointer(data_ptr_t entry_ptr, data_ptr_t p) {
		memcpy(entry_ptr + sizeof(hash_t), &p, sizeof(data_ptr_t));
	}

	static inline data_ptr_t GetMiniRowPtr(data_ptr_t entry_ptr) {
		return entry_ptr + HEADER_SIZE;
	}

	idx_t capacity;
	idx_t bitmask;
	idx_t mini_row_size;
	idx_t entry_stride;
	unsafe_unique_array<data_t> data;
};

} // namespace duckdb
