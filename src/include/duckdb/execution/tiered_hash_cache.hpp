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
	      max_fill(static_cast<idx_t>(capacity_p * MAX_LOAD_FACTOR)) {
		D_ASSERT(IsPowerOfTwo(capacity)); // Needed for bitmask logic
		auto total_bytes = capacity * entry_stride;
		// TODO should we use BPM? Or Arena?
		data = make_unsafe_uniq_array_uninitialized<data_t>(total_bytes);
		memset(data.get(), 0, total_bytes);
	}

	//! Find the cache entry whose tag matches an input hash.
	//! Only compares 16-bit tags, which can lead to a false positive.
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

	//! Looks up based on tag and key (in a single phase).
	//! Returns true matches only (no false positives like ProbeByHash).
	//! On match, result_ptrs points to the cached full row (usable by GatherResult).
	//! @param miss_sel holds the densely packed indices of `probe_keys` that did not
	//!                 get a match in the THC
	template <class T>
	void ProbeAndMatch(const hash_t *hashes_dense, const T *probe_keys, idx_t count, const SelectionVector *row_sel,
	                   bool has_row_sel, data_ptr_t *result_ptrs, SelectionVector &match_sel, idx_t &match_count,
	                   SelectionVector &miss_sel, idx_t &miss_count) const {
		static constexpr idx_t SLOT_PREFETCH_DIST = 16;

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
			const auto probe_key = probe_keys[row_index];
			auto slot = hashes_dense[i] & bitmask;

			bool found_tag = false;
			for (idx_t probes = 0; probes < MAX_PROBE_DISTANCE; probes++) {
				auto entry_ptr = GetEntryPtr(slot);
				const auto stored_tag = LoadTag(entry_ptr);
				if (stored_tag == 0) {
					// THC does not have a match
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
			auto entry_ptr = GetEntryPtr(slot);
			auto tag_atomic = reinterpret_cast<std::atomic<tag_t> *>(entry_ptr);

			tag_t expected = 0; // We only insert if the current hash is null
			// TODO double check the choice of CAS function and third argument below
			if (tag_atomic->compare_exchange_strong(expected, tag, std::memory_order_acq_rel)) {
				memcpy(GetRowPtr(entry_ptr), row_data_ptr + row_copy_offset, row_size);
				insert_new.fetch_add(1, std::memory_order_relaxed);
				return true;
			}
			if (expected == tag) {
				// Don't try linear probing if the hashes perfect match. TODO could try linear probing here too
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

	//! Extract upper 16 bits of the hash as a tag.
	//! Maps 0 to 1 since 0 means empty slot.
	static inline tag_t ComputeTag(hash_t h) {
		auto tag = static_cast<tag_t>(h >> 48);
		return tag == 0 ? 1 : tag;
	}

	// Get a pointer to the `slot`th entry in the THC
	inline data_ptr_t GetEntryPtr(idx_t slot) const {
		return data.get() + slot * entry_stride;
	}

	// Get the tag stored in an entry
	static inline tag_t LoadTag(const data_ptr_t entry_ptr) {
		tag_t h;
		memcpy(&h, entry_ptr, sizeof(tag_t)); // TODO can probably do this without memcpy... just derefence the value?
		                                      // or mem-compare? or something?
		return h;
	}

	//! Pointer to the cached row data within an entry (the first byte after the hash)
	static inline data_ptr_t GetRowPtr(data_ptr_t entry_ptr) {
		return entry_ptr + HEADER_SIZE;
	}

	idx_t capacity; // Number of entries the THC can fit
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
