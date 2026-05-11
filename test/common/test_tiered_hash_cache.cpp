#include "catch.hpp"
#include "duckdb/execution/tiered_hash_cache.hpp"

#include <cstdint>
#include <cstring>
#include <vector>

using namespace duckdb;

namespace {

// Read the 8-byte row stored at `entry_row_ptr` (which points past the THC tag).
int64_t LoadKey(data_ptr_t entry_row_ptr) {
	int64_t value;
	std::memcpy(&value, entry_row_ptr, sizeof(int64_t));
	return value;
}

// Build a hash whose upper-16-bit tag and slot are explicitly controlled,
// so each test can place entries at known slots without relying on a hash
// function.
constexpr hash_t MakeHash(uint16_t tag, uint64_t slot_bits) {
	return (static_cast<hash_t>(tag) << 48) | (slot_bits & 0xFFFFFFFFFFFFULL);
}

} // namespace

TEST_CASE("THC: insert then probe-and-match returns the inserted row", "[thc]") {
	TieredHashCache thc(/*capacity*/ 64, /*row_size*/ sizeof(int64_t), /*key_offset_in_row*/ 0);

	int64_t row = 42;
	const hash_t h = MakeHash(/*tag*/ 0xBEEF, /*slot_bits*/ 7);
	REQUIRE(thc.InsertUnsafe(h, const_data_ptr_cast(&row)));

	hash_t hashes[1] = {h};
	int64_t keys[1] = {42};
	data_ptr_t result_ptrs[1] = {nullptr};
	SelectionVector match_sel(STANDARD_VECTOR_SIZE);
	SelectionVector miss_sel(STANDARD_VECTOR_SIZE);
	idx_t match_count = 0;
	idx_t miss_count = 0;

	thc.ProbeAndMatch<int64_t, false>(hashes, keys, 1, nullptr, result_ptrs, match_sel, match_count, miss_sel,
	                                  miss_count);

	REQUIRE(match_count == 1);
	REQUIRE(miss_count == 0);
	REQUIRE(result_ptrs[0] != nullptr);
	REQUIRE(LoadKey(result_ptrs[0]) == 42);
}

TEST_CASE("THC: probe of an empty slot is reported as a miss", "[thc]") {
	TieredHashCache thc(64, sizeof(int64_t), 0);

	hash_t hashes[1] = {MakeHash(0x1111, 3)};
	int64_t keys[1] = {7};
	data_ptr_t result_ptrs[1] = {nullptr};
	SelectionVector match_sel(STANDARD_VECTOR_SIZE);
	SelectionVector miss_sel(STANDARD_VECTOR_SIZE);
	idx_t match_count = 0;
	idx_t miss_count = 0;

	thc.ProbeAndMatch<int64_t, false>(hashes, keys, 1, nullptr, result_ptrs, match_sel, match_count, miss_sel,
	                                  miss_count);

	REQUIRE(match_count == 0);
	REQUIRE(miss_count == 1);
	REQUIRE(miss_sel.get_index(0) == 0);
}

TEST_CASE("THC: tag match with key mismatch falls back to miss", "[thc]") {
	TieredHashCache thc(64, sizeof(int64_t), 0);

	// Insert key=100 at slot 5 with tag 0xABCD.
	int64_t stored = 100;
	const hash_t h = MakeHash(0xABCD, 5);
	REQUIRE(thc.InsertUnsafe(h, const_data_ptr_cast(&stored)));

	// Probe with the same hash but a different key. Tag matches in slot 5,
	// key compare fails, linear probe hits an empty slot, miss is reported.
	hash_t hashes[1] = {h};
	int64_t keys[1] = {999};
	data_ptr_t result_ptrs[1] = {nullptr};
	SelectionVector match_sel(STANDARD_VECTOR_SIZE);
	SelectionVector miss_sel(STANDARD_VECTOR_SIZE);
	idx_t match_count = 0;
	idx_t miss_count = 0;

	thc.ProbeAndMatch<int64_t, false>(hashes, keys, 1, nullptr, result_ptrs, match_sel, match_count, miss_sel,
	                                  miss_count);

	REQUIRE(match_count == 0);
	REQUIRE(miss_count == 1);
}

TEST_CASE("THC: tag-zero hashes are remapped so insertion is still detectable", "[thc]") {
	TieredHashCache thc(64, sizeof(int64_t), 0);

	// Upper 16 bits are zero. The THC remaps zero tags to 1 so that "empty slot"
	// (tag == 0) remains distinguishable from a real entry.
	int64_t row = 12345;
	const hash_t h = MakeHash(0x0000, 9);
	REQUIRE(thc.InsertUnsafe(h, const_data_ptr_cast(&row)));

	hash_t hashes[1] = {h};
	int64_t keys[1] = {12345};
	data_ptr_t result_ptrs[1] = {nullptr};
	SelectionVector match_sel(STANDARD_VECTOR_SIZE);
	SelectionVector miss_sel(STANDARD_VECTOR_SIZE);
	idx_t match_count = 0;
	idx_t miss_count = 0;

	thc.ProbeAndMatch<int64_t, false>(hashes, keys, 1, nullptr, result_ptrs, match_sel, match_count, miss_sel,
	                                  miss_count);

	REQUIRE(match_count == 1);
	REQUIRE(LoadKey(result_ptrs[0]) == 12345);
}

TEST_CASE("THC: refuses inserts once max load factor is reached", "[thc]") {
	constexpr idx_t capacity = 64;
	constexpr double max_load_factor = 0.5;
	TieredHashCache thc(capacity, sizeof(int64_t), 0, /*row_copy_offset*/ 0, max_load_factor);

	idx_t inserted = 0;
	for (idx_t i = 0; i < capacity; i++) {
		int64_t row = static_cast<int64_t>(i);
		// Distinct tag per entry, slot = i mod capacity → no collisions.
		hash_t h = MakeHash(static_cast<uint16_t>(i + 1), i);
		if (thc.InsertUnsafe(h, const_data_ptr_cast(&row))) {
			inserted++;
		}
	}

	REQUIRE(inserted == static_cast<idx_t>(capacity * max_load_factor));

	// InsertUnsafe bumps unsafe_fill_count; IsFull reads the atomic new_inserts_count.
	// Real callers reconcile via SyncCountersFromUnsafe (also called from InsertBatch).
	thc.SyncCountersFromUnsafe();
	REQUIRE(thc.IsFull());
}

TEST_CASE("THC: duplicate-hash insert is rejected", "[thc]") {
	TieredHashCache thc(64, sizeof(int64_t), 0);
	int64_t row = 1;
	const hash_t h = MakeHash(0x4242, 11);

	REQUIRE(thc.InsertUnsafe(h, const_data_ptr_cast(&row)));
	REQUIRE_FALSE(thc.InsertUnsafe(h, const_data_ptr_cast(&row)));
}

TEST_CASE("THC: ComputeCapacity returns a power of two and respects the floor", "[thc]") {
	const auto cap_typical = TieredHashCache::ComputeCapacity(/*row_size*/ 8, /*l3_budget*/ 1 << 20);
	REQUIRE(cap_typical >= 64);
	REQUIRE((cap_typical & (cap_typical - 1)) == 0);

	const auto cap_tiny = TieredHashCache::ComputeCapacity(/*row_size*/ 8, /*l3_budget*/ 100);
	REQUIRE(cap_tiny == 64);
}

TEST_CASE("THC: insert/probe round-trips for a range of row sizes", "[thc]") {
	// Spans the small/medium/large stride regimes — including the awkward
	// 24/40/48/56 sizes that the old stride math produced.
	const std::vector<idx_t> row_sizes = {8, 14, 16, 22, 30, 32, 40, 48, 56, 62, 100};

	for (idx_t row_size : row_sizes) {
		TieredHashCache thc(/*capacity*/ 64, row_size, /*key_offset_in_row*/ 0);

		std::vector<uint8_t> row_buf(row_size, 0);
		const int64_t key = static_cast<int64_t>(0xCAFEBABE + row_size);
		std::memcpy(row_buf.data(), &key, sizeof(int64_t));

		const hash_t h = MakeHash(/*tag*/ 0xBEEF, /*slot_bits*/ row_size);
		REQUIRE(thc.InsertUnsafe(h, row_buf.data()));

		hash_t hashes[1] = {h};
		int64_t keys[1] = {key};
		data_ptr_t result_ptrs[1] = {nullptr};
		SelectionVector match_sel(STANDARD_VECTOR_SIZE);
		SelectionVector miss_sel(STANDARD_VECTOR_SIZE);
		idx_t match_count = 0;
		idx_t miss_count = 0;

		thc.ProbeAndMatch<int64_t, false>(hashes, keys, 1, nullptr, result_ptrs, match_sel, match_count, miss_sel,
		                                  miss_count);

		REQUIRE(match_count == 1);
		REQUIRE(miss_count == 0);
		REQUIRE(LoadKey(result_ptrs[0]) == key);
	}
}

TEST_CASE("THC: InsertBatch reports the number of newly inserted entries", "[thc]") {
	TieredHashCache thc(64, sizeof(int64_t), 0);

	struct Entry {
		hash_t hash;
		const_data_ptr_t row_ptr;
	};
	int64_t rows[3] = {1, 2, 3};
	Entry entries[3] = {
	    {MakeHash(0x0001, 0), const_data_ptr_cast(&rows[0])},
	    {MakeHash(0x0002, 1), const_data_ptr_cast(&rows[1])},
	    {MakeHash(0x0003, 2), const_data_ptr_cast(&rows[2])},
	};

	const auto new_entries = thc.InsertBatch<true>(entries, 3);
	REQUIRE(new_entries == 3);

	// Re-inserting the same entries should not count as new.
	const auto re_inserts = thc.InsertBatch<true>(entries, 3);
	REQUIRE(re_inserts == 0);
}
