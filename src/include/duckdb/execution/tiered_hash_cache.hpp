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
//! It uses a two-buffer layout with 16-bit hash tags:
//!
//!   tag_data[capacity]   — dense uint16 hash tags for cache-efficient probing
//!                          (32 tags per 64-byte cache line)
//!   row_data[capacity]   — full data_collection row copies
//!
//! ProbeAndMatch uses a two-phase algorithm:
//!   Phase 1: scan tag_data only (pure sequential, very cache-friendly)
//!   Phase 2: batch-prefetch row_data and compare keys for tag matches
//!
//! Thread safety is based on compare-and-swap on the tag field.
class TieredHashCache {
public:

	using tag_t = uint16_t;

	//! Only create the THC if the global hash table has at least that capacity
	static constexpr idx_t ACTIVATION_THRESHOLD = 10ULL * 1024 * 1024 / sizeof(uint64_t);

	//! Maximum fraction of capacity that may be filled
	//! Beyond this load factor, Insert silently drops new entries to avoid
	//! pathological linear-probing chains (the extreme case being an infinite loop).
	static constexpr double MAX_LOAD_FACTOR = 0.9;

	TieredHashCache(idx_t capacity_p, idx_t row_size_p, idx_t key_size_p, idx_t key_offset_in_row_p,
	                idx_t row_copy_offset_p = 0)
	    : capacity(capacity_p), bitmask(capacity_p - 1), row_size(row_size_p),
	      key_offset_in_row(key_offset_in_row_p), row_copy_offset(row_copy_offset_p),
	      row_stride((row_size_p + 7) & ~idx_t(7)),
	      max_fill(static_cast<idx_t>(capacity_p * MAX_LOAD_FACTOR)) {
		D_ASSERT(IsPowerOfTwo(capacity));

		tag_data = make_unsafe_uniq_array_uninitialized<tag_t>(capacity);
		memset(tag_data.get(), 0, capacity * sizeof(tag_t));

		row_data = make_unsafe_uniq_array_uninitialized<data_t>(capacity * row_stride);
		memset(row_data.get(), 0, capacity * row_stride);
	}

	//! Find the cache entry whose hash matches an input hash.
	//! Only compares hashes, which can lead to a false positive.
	//! Returns a pointer to the cached row data (usable by RowMatcher and GatherResult).
	//! On miss, doesn't go to data_collection, but records the row in cache_miss_sel (and cache_miss_count)
	//! @param cache_miss_sel holds the densely packed indices of `hashes_dense` that did not
	//!                       get a match in the THC.
	void ProbeByHash(const hash_t *hashes_dense, idx_t count, const SelectionVector *row_sel, bool has_row_sel,
	                 SelectionVector &cache_candidates_sel, idx_t &cache_candidates_count,
	                 data_ptr_t *cache_result_ptrs, data_ptr_t *cache_rhs_locations, SelectionVector &cache_miss_sel,
	                 idx_t &cache_miss_count) const {

		static constexpr idx_t SLOT_PREFETCH_DIST = 16;

		cache_candidates_count = 0;
		cache_miss_count = 0;

		for (idx_t p = 0; p < MinValue<idx_t>(SLOT_PREFETCH_DIST, count); p++) {
			__builtin_prefetch(&tag_data[hashes_dense[p] & bitmask], 0, 1);
		}

		for (idx_t i = 0; i < count; i++) {
			if (i + SLOT_PREFETCH_DIST < count) {
				__builtin_prefetch(&tag_data[hashes_dense[i + SLOT_PREFETCH_DIST] & bitmask], 0, 1);
			}

			const auto row_index = has_row_sel ? row_sel->get_index(i) : i;
			const auto probe_tag = ComputeTag(hashes_dense[i]);
			auto slot = hashes_dense[i] & bitmask;

			bool found = false;
			for (idx_t probes = 0; probes < MAX_PROBE_DISTANCE; probes++) {
				const auto stored_tag = tag_data[slot];
				if (stored_tag == 0) {
					break;
				}
				if (stored_tag == probe_tag) {
					auto row_ptr = GetRowPtr(slot);
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

	//! Two-phase hash + key probe.
	//! Phase 1: scan tag_data only (32 tags per cache line).
	//! Phase 2: batch-prefetch row_data and compare keys for tag matches.
	template <class T>
	void ProbeAndMatch(const hash_t *hashes_dense, const T *probe_keys, idx_t count,
	                   const SelectionVector *row_sel, bool has_row_sel, data_ptr_t *result_ptrs,
	                   SelectionVector &match_sel, idx_t &match_count, SelectionVector &miss_sel,
	                   idx_t &miss_count) const {
		static constexpr idx_t HASH_PREFETCH_DIST = 16;
		static constexpr idx_t ROW_PREFETCH_DIST = 16;

		match_count = 0;
		miss_count = 0;

		// Phase 1: tag-only scanning (touches only tag_data — 2 bytes per slot)
		idx_t tag_match_slots[STANDARD_VECTOR_SIZE];
		idx_t tag_match_row_indices[STANDARD_VECTOR_SIZE];
		idx_t tag_match_count = 0;

		for (idx_t p = 0; p < MinValue<idx_t>(HASH_PREFETCH_DIST, count); p++) {
			__builtin_prefetch(&tag_data[hashes_dense[p] & bitmask], 0, 1);
		}

		for (idx_t i = 0; i < count; i++) {
			if (i + HASH_PREFETCH_DIST < count) {
				__builtin_prefetch(&tag_data[hashes_dense[i + HASH_PREFETCH_DIST] & bitmask], 0, 1);
			}

			const auto row_index = has_row_sel ? row_sel->get_index(i) : i;
			const auto probe_tag = ComputeTag(hashes_dense[i]);
			auto slot = hashes_dense[i] & bitmask;

			bool found_tag = false;
			for (idx_t probes = 0; probes < MAX_PROBE_DISTANCE; probes++) {
				const auto stored_tag = tag_data[slot];
				if (stored_tag == 0) {
					break;
				}
				if (stored_tag == probe_tag) {
					tag_match_slots[tag_match_count] = slot;
					tag_match_row_indices[tag_match_count] = row_index;
					tag_match_count++;
					found_tag = true;
					break;
				}
				slot = (slot + 1) & bitmask;
			}
			if (!found_tag) {
				miss_sel.set_index(miss_count++, row_index);
			}
		}

		// Phase 2: key comparison with batched row_data prefetching
		for (idx_t p = 0; p < MinValue<idx_t>(ROW_PREFETCH_DIST, tag_match_count); p++) {
			__builtin_prefetch(GetRowPtr(tag_match_slots[p]), 0, 1);
		}

		for (idx_t j = 0; j < tag_match_count; j++) {
			if (j + ROW_PREFETCH_DIST < tag_match_count) {
				__builtin_prefetch(GetRowPtr(tag_match_slots[j + ROW_PREFETCH_DIST]), 0, 1);
			}

			const auto slot = tag_match_slots[j];
			const auto row_index = tag_match_row_indices[j];
			auto row_ptr = GetRowPtr(slot);
			auto cache_key = Load<T>(row_ptr + key_offset_in_row);

			if (cache_key == probe_keys[row_index]) {
				result_ptrs[row_index] = row_ptr;
				match_sel.set_index(match_count++, row_index);
			} else {
				miss_sel.set_index(miss_count++, row_index);
			}
		}
	}

	std::atomic<idx_t> insert_new {0};
	std::atomic<idx_t> insert_dup {0};

	bool Insert(hash_t hash, const_data_ptr_t src_row_ptr) {
		if (insert_new.load(std::memory_order_relaxed) >= max_fill) {
			return false;
		}
		const auto tag = ComputeTag(hash);
		auto slot = hash & bitmask;
		for (idx_t probes = 0; probes < MAX_PROBE_DISTANCE; probes++) {
			auto tag_atomic = reinterpret_cast<std::atomic<tag_t> *>(&tag_data[slot]);

			tag_t expected = 0;
			if (tag_atomic->compare_exchange_strong(expected, tag, std::memory_order_acq_rel)) {
				memcpy(GetRowPtr(slot), src_row_ptr + row_copy_offset, row_size);
				insert_new.fetch_add(1, std::memory_order_relaxed);
				return true;
			}
			if (expected == tag) {
				insert_dup.fetch_add(1, std::memory_order_relaxed);
				return false;
			}
			slot = (slot + 1) & bitmask;
		}
		return false;
	}

	idx_t GetCapacity() const {
		return capacity;
	}

	bool IsFull() const {
		return insert_new.load(std::memory_order_relaxed) >= max_fill;
	}

	idx_t CountOccupiedEntries() const {
		idx_t count = 0;
		for (idx_t s = 0; s < capacity; s++) {
			if (tag_data[s] != 0) {
				count++;
			}
		}
		return count;
	}

	idx_t GetRowSize() const {
		return row_size;
	}

	static idx_t ComputeCapacity(idx_t row_size, idx_t key_size, idx_t l3_budget) {
		auto row_stride = (row_size + 7) & ~idx_t(7);
		auto bytes_per_entry = sizeof(tag_t) + row_stride;
		auto raw = l3_budget / bytes_per_entry;
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
	static constexpr idx_t MAX_PROBE_DISTANCE = 10;

	static inline tag_t ComputeTag(hash_t h) {
		auto tag = static_cast<tag_t>(h >> 48);
		return tag == 0 ? 1 : tag;
	}

	inline data_ptr_t GetRowPtr(idx_t slot) const {
		return row_data.get() + slot * row_stride;
	}

	idx_t capacity;
	idx_t bitmask;
	idx_t row_size;
	idx_t key_offset_in_row;
	idx_t row_copy_offset;
	idx_t row_stride;
	idx_t max_fill;

	unsafe_unique_array<tag_t> tag_data;     //!< Dense 16-bit hash tag array
	unsafe_unique_array<data_t> row_data;    //!< Full data_collection row copies
};

} // namespace duckdb
