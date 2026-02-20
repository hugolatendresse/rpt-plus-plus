#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/selection_vector.hpp"

#include <cstring>

namespace duckdb {

//! FastHashCache is a hash table that caches recently
//! matched probe entries to accelerate repeated hash join lookups.
//!
//! Each entry stores: [hash (8 bytes)] [full_row from data_selection]
//! The full_row is a copy of the entire data_collection row, so cache hits
//! completely bypass data_collection access for both key and payload
//!
//! Thread safety is simply based on compare-and-swap (check if entry is empty)
class FastHashCache {
public:
  //! Memory budget for the cache (sized for L3)
  static constexpr idx_t DEFAULT_L3_BUDGET = 16ULL * 1024 * 1024;

	//! Only create the fast hash cache if the global hash table has at least that capacity
	static constexpr idx_t ACTIVATION_THRESHOLD = 10ULL * 1024 * 1024 / sizeof(uint64_t);

	//! capacity_p is the number of slots to create
	//! row_size_p is the number of bytes in each row of data_collection.
	//!            This is smaller than the entry size of each row of our
	//!            fast cache since the latter also includes a hash
	FastHashCache(idx_t capacity_p, idx_t row_size_p)
	    : capacity(capacity_p), bitmask(capacity_p - 1), row_size(row_size_p),
	      entry_stride(ComputeEntryStride(row_size_p)) {
		D_ASSERT(IsPowerOfTwo(capacity)); // Needed for bitmask logic
		auto total_bytes = capacity * entry_stride;
		// TODO should we use BPM? Or Arena?
		data = make_unsafe_uniq_array_uninitialized<data_t>(total_bytes);
		memset(data.get(), 0, total_bytes);
	}

  

private:
	// We store the hashes but not pointers
	// Hashes allow faster linear probing
	// Pointers are not needed since are copying the whole payload (TODO for now?)
	static constexpr idx_t HEADER_SIZE = sizeof(hash_t);

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
  
  //! Pointer to the cached row data within an entry (the first byte after the hash)
  static inline data_ptr_t GetRowPtr(data_ptr_t entry_ptr) {
    return entry_ptr + HEADER_SIZE;
  }

  idx_t capacity;
	idx_t bitmask;
	idx_t row_size;
	idx_t entry_stride;
  unsafe_unique_array<data_t> data;
};

} // namespace duckdb