#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/debug_log.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/selection_vector.hpp"

#include <atomic>
#include <cstring>

namespace duckdb {

//! TieredHashCache stores entries in a single interleaved array:
//!
//!   [Tag₀|Row₀] [Tag₁|Row₁] [Tag₂|Row₂] ...
//!
//! Each entry is a 16-bit hash tag followed by the full data_collection row.
//! Using a 16-bit tag instead of a 64-bit hash shrinks each entry (e.g.
//! from 40 to 32 bytes for a 25-byte row), which can double THC capacity
//! within the same memory budget.
//!
//! ProbeAndMatch uses a two-phase algorithm:
//!   Phase 1: scan entries for tag matches (tight comparison loop).
//!   Phase 2: re-prefetch matched entries, then compare keys.
//!
//! Thread safety is based on compare-and-swap on the tag field.
class TieredHashCache {
public:
	using tag_t = uint16_t;

	static constexpr idx_t ACTIVATION_THRESHOLD = 10ULL * 1024 * 1024 / sizeof(uint64_t);
	static constexpr double MAX_LOAD_FACTOR = 0.9;

	//! @param capacity_p        number of slots (must be power of 2)
	//! @param row_size_p        bytes per data_collection row to copy
	//! @param key_offset_in_row_p byte offset of key within the row (after validity bytes)
	//! @param row_copy_offset_p bytes to skip at the start of each source row
	TieredHashCache(idx_t capacity_p, idx_t row_size_p, idx_t key_offset_in_row_p, idx_t row_copy_offset_p = 0)
	    : capacity(capacity_p), bitmask(capacity_p - 1), row_size(row_size_p),
	      key_offset_in_row(key_offset_in_row_p), row_copy_offset(row_copy_offset_p),
	      entry_stride(ComputeEntryStride(row_size_p)), max_fill(static_cast<idx_t>(capacity_p * MAX_LOAD_FACTOR)) {
		D_ASSERT(IsPowerOfTwo(capacity));
		auto total_bytes = capacity * entry_stride;
		data = make_unsafe_uniq_array_uninitialized<data_t>(total_bytes);
		memset(data.get(), 0, total_bytes);
	}

	//! Hash-only probe using 16-bit tags. Returns a pointer to the cached row data
	//! for tag matches. False positives are resolved by the caller via RowMatcher.
	void ProbeByHash(const hash_t *hashes_dense, idx_t count, const SelectionVector *row_sel, bool has_row_sel,
	                 SelectionVector &cache_candidates_sel, idx_t &cache_candidates_count,
	                 data_ptr_t *cache_result_ptrs, data_ptr_t *cache_rhs_locations, SelectionVector &cache_miss_sel,
	                 idx_t &cache_miss_count) const {

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
			const auto probe_tag = ComputeTag(hashes_dense[i]);
			auto slot = hashes_dense[i] & bitmask;

			bool found = false;
			for (idx_t probes = 0; probes < MAX_PROBE_DISTANCE; probes++) {
				auto entry_ptr = GetEntryPtr(slot);
				const auto stored_tag = LoadTag(entry_ptr);
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
				slot = (slot + 1) & bitmask;
			}
			if (!found) {
				cache_miss_sel.set_index(cache_miss_count++, row_index);
			}
		}
	}

	//! Single-phase tag + key probe on the interleaved layout.
	//! Tag and key are co-located in the same entry (same cache line).
	template <class T>
	void ProbeAndMatch(const hash_t *hashes_dense, const T *probe_keys, idx_t count,
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
			const auto probe_tag = ComputeTag(hashes_dense[i]);
			const auto probe_key = probe_keys[row_index];
			auto slot = hashes_dense[i] & bitmask;

			bool found = false;
			for (idx_t probes = 0; probes < MAX_PROBE_DISTANCE; probes++) {
				auto entry_ptr = GetEntryPtr(slot);
				const auto stored_tag = LoadTag(entry_ptr);
				if (stored_tag == 0) {
					break;
				}
				if (stored_tag == probe_tag) {
					auto row_ptr = GetRowPtr(entry_ptr);
					auto cache_key = Load<T>(row_ptr + key_offset_in_row);
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

	std::atomic<idx_t> insert_new {0};
	std::atomic<idx_t> insert_dup {0};

	bool Insert(hash_t hash, const_data_ptr_t row_data_ptr) {
		if (insert_new.load(std::memory_order_relaxed) >= max_fill) {
			return false;
		}
		const auto tag = ComputeTag(hash);
		auto slot = hash & bitmask;
		for (idx_t probes = 0; probes < MAX_PROBE_DISTANCE; probes++) {
			auto entry_ptr = GetEntryPtr(slot);
			auto tag_atomic = reinterpret_cast<std::atomic<tag_t> *>(entry_ptr);

			tag_t expected = 0;
			if (tag_atomic->compare_exchange_strong(expected, tag, std::memory_order_acq_rel)) {
				memcpy(GetRowPtr(entry_ptr), row_data_ptr + row_copy_offset, row_size);
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
			if (LoadTag(GetEntryPtr(s)) != 0) {
				count++;
			}
		}
		return count;
	}

	idx_t GetRowSize() const {
		return row_size;
	}

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
	static constexpr idx_t HEADER_SIZE = sizeof(tag_t);
	static constexpr idx_t MAX_PROBE_DISTANCE = 10;

	static idx_t ComputeEntryStride(idx_t row_size) {
		idx_t stride = (HEADER_SIZE + row_size + 7) & ~idx_t(7);
		DEBUG_LOG("[THC] Stride is %lu bytes\n", stride);
		return stride;
	}

	//! Extract upper 16 bits of the hash as a tag. Maps 0 → 1 (0 = empty slot).
	static inline tag_t ComputeTag(hash_t h) {
		auto tag = static_cast<tag_t>(h >> 48);
		return tag == 0 ? 1 : tag;
	}

	inline data_ptr_t GetEntryPtr(idx_t slot) const {
		return data.get() + slot * entry_stride;
	}

	static inline tag_t LoadTag(const data_ptr_t entry_ptr) {
		tag_t t;
		memcpy(&t, entry_ptr, sizeof(tag_t));
		return t;
	}

	static inline data_ptr_t GetRowPtr(data_ptr_t entry_ptr) {
		return entry_ptr + HEADER_SIZE;
	}

	idx_t capacity;
	idx_t bitmask;
	idx_t row_size;
	idx_t key_offset_in_row;
	idx_t row_copy_offset;
	idx_t entry_stride;
	idx_t max_fill;
	unsafe_unique_array<data_t> data;
};

} // namespace duckdb
