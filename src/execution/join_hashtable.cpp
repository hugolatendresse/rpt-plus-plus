#include "duckdb/execution/join_hashtable.hpp"

#include <chrono>
#include <thread>
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/debug_log.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/execution/scoped_hash_join_timer.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {
using ValidityBytes = JoinHashTable::ValidityBytes;
using ScanStructure = JoinHashTable::ScanStructure;
using ProbeSpill = JoinHashTable::ProbeSpill;
using ProbeSpillLocalState = JoinHashTable::ProbeSpillLocalAppendState;

// Forward declaration for pointer chain walker used earlier in file
static data_ptr_t LoadPointer(const const_data_ptr_t &source);

JoinHashTable::SharedState::SharedState()
    : salt_v(LogicalType::UBIGINT), keys_to_compare_sel(STANDARD_VECTOR_SIZE), keys_no_match_sel(STANDARD_VECTOR_SIZE) {
}

JoinHashTable::ProbeState::ProbeState(idx_t collected_entries_capacity)
    : SharedState(), ht_offsets_v(LogicalType::UBIGINT), hashes_dense_v(LogicalType::HASH),
      non_empty_sel(STANDARD_VECTOR_SIZE), cache_rhs_row_locations(LogicalType::POINTER),
      cache_result_pointers(LogicalType::POINTER), cache_candidates_sel(STANDARD_VECTOR_SIZE),
      cache_miss_sel(STANDARD_VECTOR_SIZE) {
	if (collected_entries_capacity > 0) {
		collected_entries.reserve(collected_entries_capacity);
	}
}

JoinHashTable::InsertState::InsertState(const JoinHashTable &ht)
    : SharedState(), remaining_sel(STANDARD_VECTOR_SIZE), key_match_sel(STANDARD_VECTOR_SIZE),
      rhs_row_locations(LogicalType::POINTER) {
	ht.data_collection->InitializeChunk(lhs_data, ht.equality_predicate_columns);
	ht.data_collection->InitializeChunkState(chunk_state, ht.equality_predicate_columns);
}

JoinHashTable::JoinHashTable(ClientContext &context_p, const vector<JoinCondition> &conditions_p,
                             vector<LogicalType> btypes, JoinType type_p, const vector<idx_t> &output_columns_p,
                             idx_t estimated_probe_side_rows_p)
    : context(context_p), buffer_manager(BufferManager::GetBufferManager(context)), conditions(conditions_p),
      build_types(std::move(btypes)), output_columns(output_columns_p), entry_size(0), tuple_size(0),
      vfound(Value::BOOLEAN(false)), join_type(type_p), finalized(false), has_null(false),
      radix_bits(INITIAL_RADIX_BITS), estimated_probe_side_rows(estimated_probe_side_rows_p) {
	// Load THC parameters from the client configuration.
	// These are per-session settings that control THC sizing and adaptive behaviour
	// so that users can tune them via SQL SET commands without recompiling.
	auto &config = ClientConfig::GetConfig(context);
	thc_budget_bytes = config.thc_budget_bytes;
	thc_collect_phase_rows = config.thc_collect_phase_rows;
	thc_first_read_only_phase_rows = config.thc_first_read_only_phase_rows;
	thc_collect_budget_fraction = config.thc_collect_budget_fraction;
	thc_miss_below_which_skip_collect = config.thc_miss_below_which_skip_collect;
	thc_activation_threshold = config.thc_activation_threshold;
	thc_max_load_factor = config.thc_max_load_factor;
	thc_min_estimated_mu_s_to_r = config.thc_min_estimated_mu_s_to_r;
	thc_max_estimated_perc_hot = config.thc_max_estimated_perc_hot;
	thc_min_coverage_of_build_side = config.thc_min_coverage_of_build_side;
	thc_enable_first_cycle_check = config.thc_enable_first_cycle_check;
	thc_warmup_cycles = config.thc_warmup_cycles;
	// mu_s estimation controls (per-session)
	thc_mu_s_method = config.thc_mu_s_method;
	thc_log_mu_s = config.thc_log_mu_s;
	thc_mu_s_method = config.thc_mu_s_method;
	thc_log_mu_s = config.thc_log_mu_s;
	for (idx_t i = 0; i < conditions.size(); ++i) {
		auto &condition = conditions[i];
		D_ASSERT(condition.left->return_type == condition.right->return_type);
		auto type = condition.left->return_type;
		if (condition.comparison == ExpressionType::COMPARE_EQUAL ||
		    condition.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {

			// ensure that all equality conditions are at the front,
			// and that all other conditions are at the back
			D_ASSERT(equality_types.size() == condition_types.size());
			equality_types.push_back(type);
			equality_predicates.push_back(condition.comparison);
			equality_predicate_columns.push_back(i);

		} else {
			// all non-equality conditions are at the back
			non_equality_predicates.push_back(condition.comparison);
			non_equality_predicate_columns.push_back(i);
		}

		null_values_are_equal.push_back(condition.comparison == ExpressionType::COMPARE_DISTINCT_FROM ||
		                                condition.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM);

		condition_types.push_back(type);
	}
	// at least one equality is necessary
	D_ASSERT(!equality_types.empty());

	// Types for the layout
	auto layout = make_shared_ptr<TupleDataLayout>();
	vector<LogicalType> layout_types(condition_types);
	layout_types.insert(layout_types.end(), build_types.begin(), build_types.end());
	if (PropagatesBuildSide(join_type)) {
		// full/right outer joins need an extra bool to keep track of whether or not a tuple has found a matching entry
		// we place the bool before the NEXT pointer
		layout_types.emplace_back(LogicalType::BOOLEAN);
	}
	layout_types.emplace_back(LogicalType::HASH);
	layout->Initialize(layout_types, false);
	layout_ptr = std::move(layout);

	// Initialize the row matcher that are used for filtering during the probing only if there are non-equality
	if (!non_equality_predicates.empty()) {

		row_matcher_probe = unique_ptr<RowMatcher>(new RowMatcher());
		row_matcher_probe_no_match_sel = unique_ptr<RowMatcher>(new RowMatcher());

		row_matcher_probe->Initialize(false, *layout_ptr, non_equality_predicates, non_equality_predicate_columns);
		row_matcher_probe_no_match_sel->Initialize(true, *layout_ptr, non_equality_predicates,
		                                           non_equality_predicate_columns);

		needs_chain_matcher = true;
	} else {
		needs_chain_matcher = false;
	}

	chains_longer_than_one = false;
	row_matcher_build.Initialize(true, *layout_ptr, equality_predicates);

	const auto &offsets = layout_ptr->GetOffsets();
	tuple_size = offsets[condition_types.size() + build_types.size()];
	pointer_offset = offsets.back();
	entry_size = layout_ptr->GetRowWidth();

	data_collection = make_uniq<TupleDataCollection>(buffer_manager, layout_ptr);
	sink_collection =
	    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout_ptr, radix_bits, layout_ptr->ColumnCount() - 1);

	dead_end = make_unsafe_uniq_array_uninitialized<data_t>(layout_ptr->GetRowWidth());
	memset(dead_end.get(), 0, layout_ptr->GetRowWidth());

	if (join_type == JoinType::SINGLE) {
		auto &config = ClientConfig::GetConfig(context);
		single_join_error_on_multiple_rows = config.scalar_subquery_error_on_multiple_rows;
	}

	InitializePartitionMasks();
}

JoinHashTable::~JoinHashTable() {
}

void JoinHashTable::Merge(JoinHashTable &other) {
	{
		lock_guard<mutex> guard(data_lock);
		data_collection->Combine(*other.data_collection);
	}

	if (join_type == JoinType::MARK) {
		auto &info = correlated_mark_join_info;
		lock_guard<mutex> mj_lock(info.mj_lock);
		has_null = has_null || other.has_null;
		if (!info.correlated_types.empty()) {
			auto &other_info = other.correlated_mark_join_info;
			info.correlated_counts->Combine(*other_info.correlated_counts);
		}
	}

	sink_collection->Combine(*other.sink_collection);
}

static void ApplyBitmaskAndGetSaltBuild(Vector &hashes_v, Vector &salt_v, const idx_t &count, const idx_t &bitmask) {
	if (hashes_v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto &hash = *ConstantVector::GetData<hash_t>(hashes_v);
		salt_v.SetVectorType(VectorType::CONSTANT_VECTOR);

		*ConstantVector::GetData<hash_t>(salt_v) = ht_entry_t::ExtractSalt(hash);
		salt_v.Flatten(count);

		hash = hash & bitmask;
		hashes_v.Flatten(count);
	} else {
		hashes_v.Flatten(count);
		auto salts = FlatVector::GetData<hash_t>(salt_v);
		auto hashes = FlatVector::GetData<hash_t>(hashes_v);
		for (idx_t i = 0; i < count; i++) {
			salts[i] = ht_entry_t::ExtractSalt(hashes[i]);
			hashes[i] &= bitmask;
		}
	}
}

template <bool HAS_SEL>
idx_t GetOptionalIndex(const SelectionVector *sel, const idx_t idx) {
	return HAS_SEL ? sel->get_index(idx) : idx;
}

static void AddPointerToCompare(JoinHashTable::ProbeState &state, const ht_entry_t &entry, Vector &pointers_result_v,
                                idx_t row_ht_offset, idx_t &keys_to_compare_count, const idx_t &row_index) {

	const auto row_ptr_insert_to = FlatVector::GetData<data_ptr_t>(pointers_result_v);
	const auto ht_offsets = FlatVector::GetData<idx_t>(state.ht_offsets_v);

	state.keys_to_compare_sel.set_index(keys_to_compare_count, row_index);
	row_ptr_insert_to[row_index] = entry.GetPointer();
	ht_offsets[row_index] = row_ht_offset;
	keys_to_compare_count += 1;
}

template <bool USE_SALTS, bool HAS_SEL>
static idx_t ProbeForPointersInternal(JoinHashTable::ProbeState &state, JoinHashTable &ht, ht_entry_t *entries,
                                      Vector &hashes_v, Vector &pointers_result_v, const SelectionVector *row_sel,
                                      idx_t &count) {

	auto hashes_dense = FlatVector::GetData<hash_t>(state.hashes_dense_v);

	idx_t keys_to_compare_count = 0;

	// -------------------------------------------------------------------
	// Grouped two-pass prefetching for the entries array.
	//
	// For large hash tables (many millions of entries), each access to
	// entries[row_ht_offset] is very likely an L3 cache miss because the
	// entry positions are essentially random across hundreds of MB of
	// memory.  A simple streaming prefetch (N slots ahead) isn't effective
	// enough because the per-row processing is too fast to overlap the
	// full DRAM latency (~80-100ns).
	//
	// Instead, we process rows in GROUPS of PREFETCH_GROUP_SIZE.  For each
	// group:
	//   Pass 1: Compute all HT offsets and issue prefetch hints.
	//           This is pure arithmetic — no stalls.
	//   Pass 2: Process entries (which are now in L1/L2 from the prefetches
	//           issued in Pass 1).
	//
	// Tuning: Group size 32 was determined empirically on TPC-H Q5 SF200
	// (i7-11850H, 24MB L3, 4 threads). Measured ProbeForPointers time:
	//   vanilla: 12666ms | grp=16: 13315ms | grp=32: 11370ms | grp=64: 11369ms
	// Group_size=32 gives the best avg with lowest variance (CV=7.8% vs 10.8%).
	// Smaller groups (16) add overhead without benefit. Larger groups (64)
	// increase variance (possibly cache line eviction or prefetch drops).
	// -------------------------------------------------------------------
	static constexpr idx_t PREFETCH_GROUP_SIZE = 32;

	// Scratch buffer for precomputed HT offsets (on stack, small)
	idx_t ht_offsets_scratch[PREFETCH_GROUP_SIZE];

	for (idx_t group_start = 0; group_start < count; group_start += PREFETCH_GROUP_SIZE) {
		const idx_t group_end = MinValue<idx_t>(group_start + PREFETCH_GROUP_SIZE, count);

		// Pass 1: Compute HT offsets and issue prefetch hints for this group.
		// This pass is pure arithmetic + prefetch instructions — no memory stalls.
		for (idx_t i = group_start; i < group_end; i++) {
			ht_offsets_scratch[i - group_start] = hashes_dense[i] & ht.bitmask;
			__builtin_prefetch(&entries[ht_offsets_scratch[i - group_start]], 0 /* read */, 3 /* high temporal */);
		}

		// Pass 2: Process entries in this group.
		// By now the prefetched cache lines should be in L1/L2.
		for (idx_t i = group_start; i < group_end; i++) {
			const auto row_hash = hashes_dense[i];
			auto row_ht_offset = ht_offsets_scratch[i - group_start];

			if (USE_SALTS) {
				// increment the ht_offset of the entry as long as next entry is occupied and salt does not match
				while (true) {
					const ht_entry_t entry = entries[row_ht_offset];
					const bool occupied = entry.IsOccupied();

					// the entry is empty -> no match possible
					if (!occupied) {
						break;
					}

					const hash_t row_salt = ht_entry_t::ExtractSalt(row_hash);
					const bool salt_match = entry.GetSalt() == row_salt;
					if (salt_match) {
						// we know that the entry is occupied and the salt matches -> compare the keys
						auto row_index = GetOptionalIndex<HAS_SEL>(row_sel, i);
						AddPointerToCompare(state, entry, pointers_result_v, row_ht_offset, keys_to_compare_count,
						                    row_index);
						break;
					}

					// full and salt does not match -> continue probing
					IncrementAndWrap(row_ht_offset, ht.bitmask);
				}
			} else {
				const ht_entry_t entry = entries[row_ht_offset];
				const bool occupied = entry.IsOccupied();
				if (occupied) {
					// the entry is occupied -> compare the keys
					auto row_index = GetOptionalIndex<HAS_SEL>(row_sel, i);
					AddPointerToCompare(state, entry, pointers_result_v, row_ht_offset, keys_to_compare_count,
					                    row_index);
				}
			}
		} // end Pass 2
	} // end group loop

	return keys_to_compare_count;
}

/// for each entry, do linear probing until
/// a) an empty entry is found
///	   -> no match
/// b) an entry is found where (and the salt matches if USE_SALTS is true)
///	   -> match, add to compare sel and increase found count
template <bool USE_SALTS>
static idx_t ProbeForPointers(JoinHashTable::ProbeState &state, JoinHashTable &ht, ht_entry_t *entries,
                              Vector &hashes_v, Vector &pointers_result_v, const SelectionVector *row_sel, idx_t count,
                              const bool has_row_sel) {
	if (has_row_sel) {
		return ProbeForPointersInternal<USE_SALTS, true>(state, ht, entries, hashes_v, pointers_result_v, row_sel,
		                                                 count);
	} else {
		return ProbeForPointersInternal<USE_SALTS, false>(state, ht, entries, hashes_v, pointers_result_v, row_sel,
		                                                  count);
	}
}

//! Gets a pointer to the entry in the HT for each of the hashes_v using linear probing. Will update the key_match_sel
//! vector and the count argument to the number and position of the matches
//! If `keys` and `hashes_v` are not dense, `row_sel` dictates which keys to look for.
//! Pointers get populated in `pointers_result_v`.
template <bool USE_SALTS>
static void GetRowPointersInternal(DataChunk &keys, TupleDataChunkState &key_state, JoinHashTable::ProbeState &state,
                                   Vector &hashes_v, const SelectionVector *row_sel, idx_t &count, JoinHashTable &ht,
                                   ht_entry_t *entries, Vector &pointers_result_v, SelectionVector &match_sel,
                                   bool has_row_sel) {

	// in case of a hash collision, we need this information to correctly retrieve the salt of this hash
	bool uses_unified = false;
	UnifiedVectorFormat hashes_unified_v;

	// densify hashes: If there is no sel, flatten the hashes, else densify via UnifiedVectorFormat
	if (has_row_sel) {

		hashes_v.ToUnifiedFormat(count, hashes_unified_v);
		uses_unified = true;

		auto hashes_unified = UnifiedVectorFormat::GetData<hash_t>(hashes_unified_v);
		auto hashes_dense = FlatVector::GetData<idx_t>(state.hashes_dense_v);

		for (idx_t i = 0; i < count; i++) {
			const auto row_index = row_sel->get_index(i);
			const auto uvf_index = hashes_unified_v.sel->get_index(row_index);
			hashes_dense[i] = hashes_unified[uvf_index];
		}
	} else {
		hashes_v.Flatten(count);
		state.hashes_dense_v.Reference(hashes_v);
	}

	// the number of keys that match for all iterations of the following loop
	idx_t match_count = 0;

	idx_t keys_no_match_count;
	idx_t elements_to_probe_count = count;

	do {

		idx_t keys_to_compare_count = 0;
		{
			ScopedHashJoinTimer probe_for_pointers_timer(state.probe_for_pointers_time_ns);
			keys_to_compare_count = ProbeForPointers<USE_SALTS>(state, ht, entries, hashes_v, pointers_result_v,
			                                                    row_sel, elements_to_probe_count, has_row_sel);
		}

		// if there are no keys to compare, we are done
		if (keys_to_compare_count == 0) {
			break;
		}

		// Perform row comparisons, after Match function call salt_match_sel will point to the keys that match
		keys_no_match_count = 0;
		idx_t keys_match_count = 0;
		{
			ScopedHashJoinTimer match_timer(state.match_time_ns);
			keys_match_count = ht.row_matcher_build.Match(keys, key_state.vector_data, state.keys_to_compare_sel,
			                                              keys_to_compare_count, *ht.layout_ptr, pointers_result_v,
			                                              &state.keys_no_match_sel, keys_no_match_count);
		}

		D_ASSERT(keys_match_count + keys_no_match_count == keys_to_compare_count);

		// add the indices to the match_sel
		for (idx_t i = 0; i < keys_match_count; i++) {
			const auto row_index = state.keys_to_compare_sel.get_index(i);
			match_sel.set_index(match_count, row_index);
			match_count++;
		}

		// Linear probing for collisions: Move to the next entry in the HT
		auto ht_offsets = FlatVector::GetData<idx_t>(state.ht_offsets_v);
		auto hashes_unified = UnifiedVectorFormat::GetData<hash_t>(hashes_unified_v);
		auto hashes_dense = FlatVector::GetData<hash_t>(state.hashes_dense_v);

		for (idx_t i = 0; i < keys_no_match_count; i++) {
			const auto row_index = state.keys_no_match_sel.get_index(i);
			// The ProbeForPointers function calculates the ht_offset from the hash; therefore, we have to write the
			// new offset into the hashes_v; otherwise the next iteration will start at the old position. This might
			// seem as an overhead but assures that the first call of ProbeForPointers is optimized as conceding
			// calls are unlikely (Max 1-(65535/65536)^VectorSize = 3.1%)
			auto ht_offset = ht_offsets[row_index];
			IncrementAndWrap(ht_offset, ht.bitmask);

			// Get original hash from unified vector format to extract the salt if hashes_dense was populated that way
			hash_t hash;
			if (uses_unified) {
				const auto uvf_index = hashes_unified_v.sel->get_index(row_index);
				hash = hashes_unified[uvf_index];
			} else {
				hash = hashes_dense[row_index];
			}

			const auto offset_and_salt = ht_offset | (hash & ht_entry_t::SALT_MASK);

			hashes_dense[i] = offset_and_salt; // populate dense again
		}

		// in the next interation, we have a selection vector with the keys that do not match
		row_sel = &state.keys_no_match_sel;
		has_row_sel = true;
		elements_to_probe_count = keys_no_match_count;

	} while (DUCKDB_UNLIKELY(keys_no_match_count > 0));

	// set the count to the number of matches
	count = match_count;
}

inline bool JoinHashTable::UseSalt() const {
	// only use salt for large hash tables
	return this->capacity > USE_SALT_THRESHOLD;
}

//! =====================================================================
//! ProbeTHCAndFallback — shared probe path for READ_ONLY and COLLECT (cycle > 0)
//! =====================================================================
//!
//! Densifies hashes into state.hashes_dense_v, probes the THC using either
//! ProbeAndMatch (single integral key) or ProbeByHash (complex/multi keys),
//! then falls back to GetRowPointersInternal for any THC misses.
//!
//! Results are written into match_sel / pointers_result_v.
//! On return:
//!   match_count    = total number of rows that found a match (THC + fallback)
//!   cache_miss_count = number of rows the THC could not serve
//!
//! Side-effect for collection (cycle > 0):
//!   state.thc_miss_match_sel / thc_miss_match_count are populated with
//!   the fallback rows that actually matched, enabling the caller to collect
//!   {hash, row_ptr} pairs for THC insertion.
//!
//! IMPORTANT: hashes_v is NOT modified by this function (only Flatten/ToUnifiedFormat).
//!   The caller can safely read hashes from hashes_v after this call returns.
//!   Do NOT read from state.hashes_dense_v after this call — GetRowPointersInternal
//!   overwrites it during the fallback probe.
//!
void JoinHashTable::ProbeTHCAndFallback(DataChunk &keys, TupleDataChunkState &key_state, ProbeState &state,
                                        Vector &hashes_v, const SelectionVector *sel, idx_t &count, bool has_sel,
                                        Vector &pointers_result_v, SelectionVector &match_sel, idx_t &match_count,
                                        idx_t &cache_miss_count) {

	// ---- Step 1: Densify hashes ----
	// The THC probe functions expect a dense array of hashes (one per probe row,
	// indexed 0..count-1). If a selection vector is in use, we need to gather
	// the hashes into a contiguous buffer.
	// TODO Let's find a way to avoid those memcopies
	auto hashes_dense = FlatVector::GetData<hash_t>(state.hashes_dense_v);
	if (!has_sel) {
		// Already dense
		hashes_v.Flatten(count);
		auto hashes_flat = FlatVector::GetData<hash_t>(hashes_v);
		memcpy(hashes_dense, hashes_flat, count * sizeof(hash_t));
	} else {
		UnifiedVectorFormat hashes_unified;
		hashes_v.ToUnifiedFormat(count, hashes_unified);
		auto hashes_src = UnifiedVectorFormat::GetData<hash_t>(hashes_unified);
		for (idx_t i = 0; i < count; i++) {
			const auto row_index = sel->get_index(i);
			const auto uvf_index = hashes_unified.sel->get_index(row_index);
			hashes_dense[i] = hashes_src[uvf_index];
		}
	}

	// ---- Step 2: Probe the THC ----
	// For a single, integral key: use ProbeAndMatch (exact hash+key comparison).
	// For complex/multiple keys: use ProbeByHash (hash-only, then RowMatcher).

	match_count = 0;
	cache_miss_count = 0;
	auto pointers_result = FlatVector::GetData<data_ptr_t>(pointers_result_v);

	bool used_probe_and_match = false;
	if (equality_types.size() == 1 && equality_types[0].IsIntegral()) {
		ScopedHashJoinTimer tiered_hash_cache_timer(state.thc_probe_time_ns);
		keys.data[0].Flatten(keys.size());

// Dispatch ProbeAndMatch with compile-time HAS_ROW_SEL to eliminate the
// per-iteration branch on has_sel inside the hot probe loop.
#define THC_PROBE_AND_MATCH_DISPATCH(T)                                                                                \
	do {                                                                                                               \
		auto probe_keys = FlatVector::GetData<T>(keys.data[0]);                                                        \
		if (has_sel) {                                                                                                 \
			tiered_hash_cache->ProbeAndMatch<T, true>(hashes_dense, probe_keys, count, sel, pointers_result,           \
			                                          match_sel, match_count, state.cache_miss_sel, cache_miss_count); \
		} else {                                                                                                       \
			tiered_hash_cache->ProbeAndMatch<T, false>(hashes_dense, probe_keys, count, sel, pointers_result,          \
			                                           match_sel, match_count, state.cache_miss_sel,                   \
			                                           cache_miss_count);                                              \
		}                                                                                                              \
		used_probe_and_match = true;                                                                                   \
	} while (0)

		// This switch statement populates `match_sel` and `state.cache_miss_sel` with indexes of keys that
		// found and didn't find a match, respectively.
		switch (equality_types[0].InternalType()) {
		case PhysicalType::INT8: {
			THC_PROBE_AND_MATCH_DISPATCH(int8_t);
			break;
		}
		case PhysicalType::INT16: {
			THC_PROBE_AND_MATCH_DISPATCH(int16_t);
			break;
		}
		case PhysicalType::INT32: {
			THC_PROBE_AND_MATCH_DISPATCH(int32_t);
			break;
		}
		case PhysicalType::INT64: {
			THC_PROBE_AND_MATCH_DISPATCH(int64_t);
			break;
		}
		case PhysicalType::UINT8: {
			THC_PROBE_AND_MATCH_DISPATCH(uint8_t);
			break;
		}
		case PhysicalType::UINT16: {
			THC_PROBE_AND_MATCH_DISPATCH(uint16_t);
			break;
		}
		case PhysicalType::UINT32: {
			THC_PROBE_AND_MATCH_DISPATCH(uint32_t);
			break;
		}
		case PhysicalType::UINT64: {
			THC_PROBE_AND_MATCH_DISPATCH(uint64_t);
			break;
		}
		default:
			break;
		}
#undef THC_PROBE_AND_MATCH_DISPATCH
	}

	// ---- Step 3: Fallback for complex keys (ProbeByHash path) ----
	// ProbeAndMatch (above) is only used for single, integral keys.
	// Everything else uses ProbeByHash (hash-only lookup) followed by
	// RowMatcher.Match (actual key comparison on THC candidates).
	if (!used_probe_and_match) {
		auto cache_result_ptrs = FlatVector::GetData<data_ptr_t>(state.cache_result_pointers);
		auto cache_rhs_locations = FlatVector::GetData<data_ptr_t>(state.cache_rhs_row_locations);
		idx_t cache_candidates_count = 0;

		{
			ScopedHashJoinTimer tiered_hash_cache_timer(state.thc_probe_time_ns);
			if (has_sel) {
				tiered_hash_cache->ProbeByHash<true>(hashes_dense, count, sel, state.cache_candidates_sel,
				                                     cache_candidates_count, cache_result_ptrs, cache_rhs_locations,
				                                     state.cache_miss_sel, cache_miss_count);
			} else {
				tiered_hash_cache->ProbeByHash<false>(hashes_dense, count, sel, state.cache_candidates_sel,
				                                      cache_candidates_count, cache_result_ptrs, cache_rhs_locations,
				                                      state.cache_miss_sel, cache_miss_count);
			}
		}

		if (cache_candidates_count > 0) {
			idx_t cache_no_match_count = 0;
			idx_t cache_match_count;
			{
				ScopedHashJoinTimer tiered_hash_cache_timer(state.thc_probe_time_ns);
				cache_match_count = row_matcher_build.Match(
				    keys, key_state.vector_data, state.cache_candidates_sel, cache_candidates_count, *layout_ptr,
				    state.cache_rhs_row_locations, &state.keys_no_match_sel, cache_no_match_count);
			}

			// TODO rewrite ProbeByHash OR .Match to do this automatically. Why do this in a separate step after
			// ProbeByHash?
			for (idx_t i = 0; i < cache_match_count; i++) {
				const auto row_index = state.cache_candidates_sel.get_index(i);
				pointers_result[row_index] = cache_result_ptrs[row_index];
				match_sel.set_index(match_count++, row_index);
			}

			// Key-comparison failures are reclassified as THC misses.
			// These rows had a hash match in the THC but different keys,
			// so they need to be resolved via the regular HT.
			// TODO also do this inside ProbeByHash? Or .Match?
			for (idx_t i = 0; i < cache_no_match_count; i++) {
				const auto row_index = state.keys_no_match_sel.get_index(i);
				state.cache_miss_sel.set_index(cache_miss_count++, row_index);
			}
		}
	}

	// ---- Step 4: Regular (fallback) HT probe for THC misses ----
	// Rows that the THC could not serve are resolved via the original
	// DuckDB linear-probing HT (GetRowPointersInternal).
	state.thc_miss_match_count = 0; // Reset miss-match tracking for collection
	if (cache_miss_count > 0) {
		SelectionVector regular_match_sel(STANDARD_VECTOR_SIZE);
		idx_t regular_count = cache_miss_count;

		// Pass `state.cache_miss_sel` as the `row_sel` argument to get pointers.
		// The indices of the matches found go in `regular_match_sel`
		if (UseSalt()) {
			GetRowPointersInternal<true>(keys, key_state, state, hashes_v, &state.cache_miss_sel, regular_count, *this,
			                             entries, pointers_result_v, regular_match_sel, true);
		} else {
			GetRowPointersInternal<false>(keys, key_state, state, hashes_v, &state.cache_miss_sel, regular_count, *this,
			                              entries, pointers_result_v, regular_match_sel, true);
		}

		// Update the combined match_sel with fallback matches.
		// Also populate thc_miss_match_sel + dense-index mapping, but ONLY
		// during COLLECT phase (cycle > 0) where the caller will consume them
		// to insert new entries into the THC. In READ_ONLY phase this work
		// is wasted — the data is never read.
		D_ASSERT(global_thc_state &&
		         global_thc_state->completed_collect_cycles.load(std::memory_order_relaxed) > 0);
		// We need to know whether the current call is from a COLLECT-phase
		// caller (in which case we must populate thc_miss_match_sel for the
		// caller to consume) or a READ_ONLY caller (in which case populating
		// it would be wasted work). `state.observed_phase` is the caller's
		// per-thread snapshot of the global phase taken at the top of
		// GetRowPointers, so it cannot race with another thread's transition
		// during this call.
		const bool in_collect_phase = state.observed_phase == TieredHashCachePhase::COLLECT;

		// Populate `thc_miss_match_sel` selection vector with all the entries not found in
		// THC but found in data_collection. Those entries will be pushed back to collected_entries
		// after this function returns.
		for (idx_t i = 0; i < regular_count; i++) {
			const auto row_index = regular_match_sel.get_index(i);
			match_sel.set_index(match_count++, row_index);
			if (in_collect_phase) {
				state.thc_miss_match_sel.set_index(state.thc_miss_match_count++, row_index);
			}
		}

		// NOTE: No dense-index mapping is needed here.  The caller reads
		// hashes directly from hashes_v (which is preserved through
		// ProbeTHCAndFallback) rather than from hashes_dense_v (which
		// GetRowPointersInternal overwrites internally).
	}
}

//! Get pointers to rows on the build side that match probe side keys
//!
//! Uses THC's `ProbeAndMatch` for single integer keys and `ProbeByHash` for everything else
//! `ProbeAndMatch` does equality comparison on the keys
//! `ProbeByHash` only compares hashes, and `Match` compares the keys
//!
//! If there are duplicate keys on the build side, data_collection is guaranteed to link them
//! through NEXT pointers. ScanStructure will walk that linked list regardless of whether
//! `ProbeAndMatch` or `ProbeByHash` is used.
//!
//! If there are different keys with the same hash:
//! - `ProbeAndMatch` compares keys and moves on to next slot of THC
//! - `ProbeByHash` will stop at the first hash collision, the `Match` will
//!   find that the keys are different, and the probe will fall back to
//!   regular DuckDB probe with `GetRowPointersInternal`.
//!
//! @param keys chunk of keys to match
//! @param key_state TODO
//! @param state the per-thread state (contains ht_offsets_v, etc)
//! @param hashes_v the hashes of the keys to match (rows indicated by `sel` and `count)
//! @param sel array of indices of the keys to probe
//! @param count On input: the number of rows to probe. On output: number of matches
//! @param pointers_result_v On output: contains the pointers to payloads
//! @param match_sel On output: arrays of indices of the keys that found a match
//! @param has_sel if true, use `sel`, if false, use first `count` rows of the arrays
//!
void JoinHashTable::GetRowPointers(DataChunk &keys, TupleDataChunkState &key_state, ProbeState &state, Vector &hashes_v,
                                   const SelectionVector *sel, idx_t &count, Vector &pointers_result_v,
                                   SelectionVector &match_sel, const bool has_sel) {
	// =====================================================================
	// Cost-Based Adaptive THC Algorithm (global / cross-thread variant).
	// =====================================================================
	//
	// Three phases drive the algorithm globally — every probing thread of
	// this join shares the same phase, the same cost accumulators, and the
	// same freeze / abandon outcome. The state lives on
	// `global_thc_state` (`GlobalTHCAdaptiveState`); `ProbeState` only holds
	// per-call scratch and a thread-local snapshot of the global phase.
	//
	//   BASELINE:   Probe the main HT only (no THC) for ~thc_collect_phase_rows
	//               *global* rows. Measures C_main = sum(time) / sum(probes).
	//               Happens exactly once per join.
	//
	//   COLLECT:    Each thread probes (regular HT in cycle 0; THC + HT
	//               fallback in cycle > 0), buffers matched entries
	//               locally, and flushes them into the shared THC at the
	//               next observed phase boundary. Measures C_grow^t
	//               (includes InsertBatch time).
	//
	//   READ_ONLY:  Each thread probes the THC and falls back to the
	//               regular HT on misses. Measures C_eval^t. When the
	//               *global* read_only counter reaches its target, a
	//               single thread enters the checkpoint critical section
	//               and runs the three-way cost-rule decision (DROP /
	//               FREEZE / CONTINUE).
	//
	// Phase transitions and checkpoint decisions run under
	// `g.transition_mutex` using double-checked locking, so the decision
	// body fires exactly once per global threshold crossing.
	// =====================================================================

	if (!tiered_hash_cache || global_thc_state->abandoned.load(std::memory_order_relaxed)) {
		// Fast path: THC disabled or globally abandoned. Skip all adaptive machinery.
		if (UseSalt()) {
			GetRowPointersInternal<true>(keys, key_state, state, hashes_v, sel, count, *this, entries,
			                             pointers_result_v, match_sel, has_sel);
		} else {
			GetRowPointersInternal<false>(keys, key_state, state, hashes_v, sel, count, *this, entries,
			                              pointers_result_v, match_sel, has_sel);
		}
		return;
	}

	auto &g = *global_thc_state;
	const idx_t input_count = count;

	// Lifetime probe-rows counter (denominator of the COLLECT-budget guard).
	g.total_probe_rows.fetch_add(input_count, std::memory_order_relaxed);

	// ----------------------------------------------------------------
	// Capture the thread's view of the global phase. Both `observed_phase`
	// (the phase type) and `observed_phase_number` (the row index `n` into
	// the per-phase accumulator table) are snapshotted *at chunk entry*
	// and remain stable for the whole chunk. Every post-call `fetch_add`
	// from this chunk writes to `phase_metrics[observed_phase_number %
	// MAX_PHASES]`, so contributions land in the phase the chunk was
	// started in even if the global phase has since advanced.
	//
	// If we observe a phase-type change since our previous call (the
	// leader moved the global phase forward in the meantime), flush our
	// local COLLECT buffer into the shared THC — its contents belong to
	// the just-ended COLLECT phase, indexed by the *old*
	// `observed_phase_number` that we still hold here.
	// ----------------------------------------------------------------
	const auto live_phase = g.phase.load(std::memory_order_acquire);
	if (live_phase != state.observed_phase) {
		if (state.observed_phase == TieredHashCachePhase::COLLECT && !state.collected_entries.empty()) {
			FlushCollectedEntriesIntoTHC(state, g);
		}
	}
	state.observed_phase = live_phase;
	state.observed_phase_number = g.phase_number.load(std::memory_order_acquire);

	// =================================================================
	// BASELINE PHASE — measure C_main (main HT only, no THC).
	// Contributions land in `phase_metrics[observed_phase_number %
	// MAX_PHASES]` (slot 0 for the canonical BASELINE phase). At the
	// transition the leader reads slot 0 once to compute c_main and
	// caches it as `g.c_main` so it survives any future wrap of
	// `phase_number`. Late stragglers writing to slot 0 after the
	// snapshot are harmless — slot 0 is never read again for cost-rule
	// purposes.
	// =================================================================
	if (state.observed_phase == TieredHashCachePhase::BASELINE) {
		const auto phase_t0 = std::chrono::steady_clock::now();
		if (UseSalt()) {
			GetRowPointersInternal<true>(keys, key_state, state, hashes_v, sel, count, *this, entries,
			                             pointers_result_v, match_sel, has_sel);
		} else {
			GetRowPointersInternal<false>(keys, key_state, state, hashes_v, sel, count, *this, entries,
			                              pointers_result_v, match_sel, has_sel);
		}
		const auto phase_t1 = std::chrono::steady_clock::now();
		const auto delta_ns = static_cast<uint64_t>(
		    std::chrono::duration_cast<std::chrono::nanoseconds>(phase_t1 - phase_t0).count());
		auto &m = g.phase_metrics[state.observed_phase_number % g.MAX_PHASES];
		m.time_ns.fetch_add(delta_ns, std::memory_order_relaxed);
		const idx_t new_probe_count = m.probe_count.fetch_add(input_count, std::memory_order_relaxed) + input_count;

		if (new_probe_count - input_count < thc_collect_phase_rows && new_probe_count >= thc_collect_phase_rows) {
			// This thread is the unique leader for the BASELINE→COLLECT crossing.
			std::lock_guard<std::mutex> lk(g.transition_mutex);
			// Double-check: another thread may have already transitioned (impossible by the CAS-style
			// pre-add check, but defensive).
			if (g.phase.load(std::memory_order_relaxed) == TieredHashCachePhase::BASELINE) {
				auto &b = g.phase_metrics[state.observed_phase_number % g.MAX_PHASES];
				const auto baseline_time = b.time_ns.load(std::memory_order_relaxed);
				const auto baseline_count = b.probe_count.load(std::memory_order_relaxed);
				g.c_main = baseline_count > 0
				               ? static_cast<double>(baseline_time) / static_cast<double>(baseline_count)
				               : 0.0;
				DEBUG_LOG("[BASELINE->COLLECT] c_main=%.2f ns/probe, phase_probes=%lu\n", g.c_main,
				          (unsigned long)baseline_count);
				const idx_t new_phase = g.phase_number.fetch_add(1, std::memory_order_relaxed) + 1;
				g.current_collect_phase_number = new_phase; // = 1 in practice
				// Wraparound clear is impossible here (new_phase == 1).
				g.phase.store(TieredHashCachePhase::COLLECT, std::memory_order_release);
			}
		}
		return;
	}

	// =================================================================
	// COLLECT PHASE
	// =================================================================
	if (state.observed_phase == TieredHashCachePhase::COLLECT) {
		const auto collect_phase_t0 = std::chrono::steady_clock::now();

		{
			ScopedHashJoinTimer collect_timer(state.thc_collect_time_ns);

			// Cycle 0's COLLECT phase has `observed_phase_number == 1` (BASELINE
			// was 0). Subsequent COLLECT phases get later odd phase indices
			// (after RO segments). Wraparound: after `phase_number` exceeds
			// MAX_PHASES the index `1` recurs (4097, 8193, ...), but by then
			// `first_cycle_multiplicity_checked` is long since true and the
			// cycle-0 logic is moot.
			if (state.observed_phase_number == 1) {
				// ----------------------------------------------------------
				// First collect phase (cycle 0): THC is empty, use regular DuckDB probe.
				// Save hashes before GetRowPointersInternal modifies them,
				// then collect all matched rows into collected_entries.
				// ----------------------------------------------------------

				// Save original hashes before GetRowPointersInternal modifies them
				hash_t saved_hashes[STANDARD_VECTOR_SIZE];
				if (!has_sel) {
					hashes_v.Flatten(input_count);
					memcpy(saved_hashes, FlatVector::GetData<hash_t>(hashes_v), input_count * sizeof(hash_t));
				} else {
					UnifiedVectorFormat hashes_unified;
					hashes_v.ToUnifiedFormat(input_count, hashes_unified);
					auto hashes_src = UnifiedVectorFormat::GetData<hash_t>(hashes_unified);
					for (idx_t i = 0; i < input_count; i++) {
						const auto row_index = sel->get_index(i);
						const auto uvf_index = hashes_unified.sel->get_index(row_index);
						saved_hashes[row_index] = hashes_src[uvf_index];
					}
				}

				// Run the regular DuckDB probe (no THC involvement)
				if (UseSalt()) {
					GetRowPointersInternal<true>(keys, key_state, state, hashes_v, sel, count, *this, entries,
					                             pointers_result_v, match_sel, has_sel);
				} else {
					GetRowPointersInternal<false>(keys, key_state, state, hashes_v, sel, count, *this, entries,
					                              pointers_result_v, match_sel, has_sel);
				}

				// Collect every matched row as a collected entry so that the first
				// THC population covers the broadest possible set of hot keys.
				auto pointers_result = FlatVector::GetData<data_ptr_t>(pointers_result_v);
				for (idx_t i = 0; i < count; i++) {
					const auto row_index = match_sel.get_index(i);
					const auto hash = saved_hashes[row_index];
					if (hash != 0) {
						state.collected_entries.push_back({hash, pointers_result[row_index]});
					}
				}

				// mu_s estimation Approach B: During cycle 0 COLLECT, optionally sample build-side chain lengths
				// for the matched rows to estimate within-build-side multiplicity (mu_s).
				if (thc_mu_s_method == "probe_sample" || thc_mu_s_method == "all") {
					// Limit the number of chains we walk per chunk to bound overhead.
					static constexpr idx_t MU_S_SAMPLE_LIMIT = 1024;
					idx_t samples = MinValue<idx_t>(count, MU_S_SAMPLE_LIMIT);
					// Accumulate locally then publish once per call to avoid
					// hammering the global atomics in a tight inner loop.
					idx_t local_chain_len_sum = 0;
					idx_t local_chain_count = 0;
					for (idx_t si = 0; si < samples; si++) {
						const auto row_index = match_sel.get_index(si);
						data_ptr_t ptr = pointers_result[row_index];
						idx_t chain_len = 0;
						while (ptr) {
							chain_len++;
							ptr = LoadPointer(ptr + pointer_offset);
						}
						local_chain_len_sum += chain_len;
						local_chain_count++;
					}
					if (local_chain_count > 0) {
						g.mu_s_chain_length_sum.fetch_add(local_chain_len_sum, std::memory_order_relaxed);
						g.mu_s_chain_count.fetch_add(local_chain_count, std::memory_order_relaxed);
					}
				}

			} else {
				// ----------------------------------------------------------
				// Subsequent collect phase (cycle > 0): THC already has entries.
				// Probe THC first, fall back to regular HT for misses.
				// Only collect THC-miss matches into collected_entries so we
				// insert exactly the "new hot" keys that the THC is missing.
				// ----------------------------------------------------------

				// The THC probe + regular fallback path is shared with READ_ONLY.
				// We call the same logic, then additionally collect miss-matched rows.
				idx_t match_count = 0;
				idx_t cache_miss_count = 0;
				ProbeTHCAndFallback(keys, key_state, state, hashes_v, sel, count, has_sel, pointers_result_v, match_sel,
				                    match_count, cache_miss_count);
				count = match_count;

				// Collect the THC-miss rows that found a match in the regular HT.
				// These are exactly the rows that the THC should learn about.
				//
				// BUG FIX (preserved): read hashes from hashes_v, not from
				// hashes_dense_v. GetRowPointersInternal (called inside
				// ProbeTHCAndFallback step 4) overwrites hashes_dense_v during
				// densification + collision resolution; reading it here
				// returned garbage hashes and inserted unreachable THC
				// entries. hashes_v is preserved through ProbeTHCAndFallback
				// (only Flatten or ToUnifiedFormat, no data mutation).
				auto pointers_result = FlatVector::GetData<data_ptr_t>(pointers_result_v);
				if (!has_sel) {
					// hashes_v was Flattened in ProbeTHCAndFallback step 1.
					// Row index == flat index, so direct indexing is correct.
					auto hashes_flat = FlatVector::GetData<hash_t>(hashes_v);
					for (idx_t i = 0; i < state.thc_miss_match_count; i++) {
						const auto row_index = state.thc_miss_match_sel.get_index(i);
						const auto hash = hashes_flat[row_index];
						if (hash != 0) {
							state.collected_entries.push_back({hash, pointers_result[row_index]});
						}
					}
				} else {
					// hashes_v is in its original format; use UnifiedVectorFormat.
					UnifiedVectorFormat hashes_uf;
					hashes_v.ToUnifiedFormat(input_count, hashes_uf);
					auto hashes_src = UnifiedVectorFormat::GetData<hash_t>(hashes_uf);
					for (idx_t i = 0; i < state.thc_miss_match_count; i++) {
						const auto row_index = state.thc_miss_match_sel.get_index(i);
						const auto uvf_index = hashes_uf.sel->get_index(row_index);
						const auto hash = hashes_src[uvf_index];
						if (hash != 0) {
							state.collected_entries.push_back({hash, pointers_result[row_index]});
						}
					}
				}
			}

		} // end collect_timer scope for THC collect time

		// ----------------------------------------------------------
		// Per-call accounting: publish this call's contribution to the
		// COLLECT phase's slot (indexed by the phase number captured at
		// chunk entry). `total_collect_phase_rows` is a lifetime counter
		// used by the budget guard at the next RO checkpoint.
		// ----------------------------------------------------------
		g.total_collect_phase_rows.fetch_add(input_count, std::memory_order_relaxed);
		const auto collect_phase_t1 = std::chrono::steady_clock::now();
		const auto delta_ns = static_cast<uint64_t>(
		    std::chrono::duration_cast<std::chrono::nanoseconds>(collect_phase_t1 - collect_phase_t0).count());
		auto &m = g.phase_metrics[state.observed_phase_number % g.MAX_PHASES];
		m.time_ns.fetch_add(delta_ns, std::memory_order_relaxed);
		const idx_t new_phase_probe_count =
		    m.probe_count.fetch_add(input_count, std::memory_order_relaxed) + input_count;

		// ----------------------------------------------------------
		// Threshold check. Only the unique thread whose `fetch_add` first
		// pushed `probe_count` past the threshold becomes the leader for
		// this COLLECT→READ_ONLY transition. Other threads still in this
		// COLLECT phase keep finishing their chunks — their writes land
		// in this same slot and are correctly attributed to cycle K.
		// ----------------------------------------------------------
		if (new_phase_probe_count - input_count < thc_collect_phase_rows &&
		    new_phase_probe_count >= thc_collect_phase_rows) {
			std::lock_guard<std::mutex> lk(g.transition_mutex);
			// Double-check phase_number — another thread may have transitioned us.
			if (g.phase_number.load(std::memory_order_relaxed) == state.observed_phase_number &&
			    g.phase.load(std::memory_order_relaxed) == TieredHashCachePhase::COLLECT) {
				if (thc_first_read_only_phase_rows > 0) {
					g.read_only_rows_target.store(thc_first_read_only_phase_rows, std::memory_order_relaxed);
					const idx_t completed_now =
					    g.completed_collect_cycles.fetch_add(1, std::memory_order_relaxed) + 1;
					// Advance phase_number to the new RO phase. Clear the
					// array if we just wrapped past MAX_PHASES.
					const idx_t new_phase_number = g.phase_number.fetch_add(1, std::memory_order_relaxed) + 1;
					if (new_phase_number > 0 && new_phase_number % g.MAX_PHASES == 0) {
						for (idx_t i = 0; i < g.MAX_PHASES; ++i) {
							g.phase_metrics[i].time_ns.store(0, std::memory_order_relaxed);
							g.phase_metrics[i].probe_count.store(0, std::memory_order_relaxed);
							g.phase_metrics[i].miss_count.store(0, std::memory_order_relaxed);
						}
					}
					g.phase.store(TieredHashCachePhase::READ_ONLY, std::memory_order_release);

					DEBUG_LOG("[Collect->Read-Only] cycle=%lu, total_collect_phase_rows=%lu, total_probe=%lu, "
					          "phase_number=%lu\n",
					          (unsigned long)completed_now,
					          (unsigned long)g.total_collect_phase_rows.load(std::memory_order_relaxed),
					          (unsigned long)g.total_probe_rows.load(std::memory_order_relaxed),
					          (unsigned long)new_phase_number);

					// If cycle 0 just ended and probe-sample mu_s was enabled,
					// log the global estimate.
					if (completed_now == 1 &&
					    (thc_mu_s_method == "probe_sample" || thc_mu_s_method == "all") &&
					    g.mu_s_chain_count.load(std::memory_order_relaxed) > 0 && thc_log_mu_s) {
						const double mu_s_probe_estimate =
						    static_cast<double>(g.mu_s_chain_length_sum.load(std::memory_order_relaxed)) /
						    static_cast<double>(g.mu_s_chain_count.load(std::memory_order_relaxed));
						std::fprintf(stderr, "[mu_s probe_sample] chains=%lu mean_len=%.6f\n",
						             (unsigned long)g.mu_s_chain_count.load(std::memory_order_relaxed),
						             mu_s_probe_estimate);
						std::fflush(stderr);
					}
				}
				// (thc_first_read_only_phase_rows == 0 is a config option
				// that skips RO entirely; with per-phase indexing nothing
				// further is needed — the same slot keeps growing and the
				// threshold check fires only once because of the pre-add
				// guard. Same behaviour as before.)
			}
		}
		return;
	}

	// =================================================================
	// READ_ONLY (EVALUATION) PHASE
	// =================================================================
	// Probe the THC, fall back to regular HT for misses.
	// Track miss rate and wall-clock cost. At checkpoint boundaries,
	// one thread enters the critical section and applies the cost-based
	// three-way decision rule.
	// =================================================================

	const auto eval_phase_t0 = std::chrono::steady_clock::now();

	idx_t match_count = 0;
	idx_t cache_miss_count = 0;
	ProbeTHCAndFallback(keys, key_state, state, hashes_v, sel, count, has_sel, pointers_result_v, match_sel,
	                    match_count, cache_miss_count);
	count = match_count;

	const auto eval_phase_t1 = std::chrono::steady_clock::now();
	const auto delta_ns = static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(eval_phase_t1 - eval_phase_t0).count());

	auto &ro_slot = g.phase_metrics[state.observed_phase_number % g.MAX_PHASES];
	ro_slot.time_ns.fetch_add(delta_ns, std::memory_order_relaxed);
	ro_slot.miss_count.fetch_add(cache_miss_count, std::memory_order_relaxed);
	const idx_t new_ro_rows = ro_slot.probe_count.fetch_add(input_count, std::memory_order_relaxed) + input_count;

	const idx_t target = g.read_only_rows_target.load(std::memory_order_relaxed);
	if (target == 0 || new_ro_rows - input_count >= target || new_ro_rows < target) {
		// Either not yet past target, or someone else already crossed it.
		// Only the unique thread whose fetch_add brought probe_count from
		// < target to >= target becomes the leader for this checkpoint.
		return;
	}

	// ----------------------------------------------------------------
	// Checkpoint: this thread is the unique leader. Run the decision
	// body exactly once.
	// ----------------------------------------------------------------
	std::lock_guard<std::mutex> lk(g.transition_mutex);
	// Double-check phase_number — another thread (e.g. one that took the
	// mutex first while we were waiting) may have already transitioned us.
	if (g.phase_number.load(std::memory_order_relaxed) != state.observed_phase_number ||
	    g.phase.load(std::memory_order_relaxed) != TieredHashCachePhase::READ_ONLY) {
		return;
	}

	// Frozen path: collection is permanently disabled (THC-full or a prior
	// cost-rule freeze). Stay in this RO phase indefinitely.
	if (!g.collection_enabled.load(std::memory_order_relaxed)) {
		DEBUG_LOG("Staying in read-only mode since collection phase has been permanently disabled\n");
		return;
	}

	// ----- Snapshot this RO phase's metrics. -----
	const auto eval_total_ns = ro_slot.time_ns.load(std::memory_order_relaxed);
	const auto eval_total_count = ro_slot.probe_count.load(std::memory_order_relaxed);
	const double c_eval_current = eval_total_count > 0
	                                   ? static_cast<double>(eval_total_ns) / static_cast<double>(eval_total_count)
	                                   : 0.0;
	// ----- Snapshot c_grow from the COLLECT phase that fed this RO. -----
	auto &collect_slot = g.phase_metrics[g.current_collect_phase_number % g.MAX_PHASES];
	const auto collect_total_ns = collect_slot.time_ns.load(std::memory_order_relaxed);
	const auto collect_total_count = collect_slot.probe_count.load(std::memory_order_relaxed);
	const double c_grow_current = collect_total_count > 0
	                                   ? static_cast<double>(collect_total_ns) /
	                                         static_cast<double>(collect_total_count)
	                                   : 0.0;
	// ----- Snapshot c_eval_prev from the previous RO segment's slot. -----
	double c_eval_prev = 0.0;
	if (g.prev_eval_phase_number > 0) {
		auto &prev_slot = g.phase_metrics[g.prev_eval_phase_number % g.MAX_PHASES];
		const auto prev_ns = prev_slot.time_ns.load(std::memory_order_relaxed);
		const auto prev_count = prev_slot.probe_count.load(std::memory_order_relaxed);
		c_eval_prev = prev_count > 0
		                  ? static_cast<double>(prev_ns) / static_cast<double>(prev_count)
		                  : 0.0;
	}

	const auto ro_total = ro_slot.probe_count.load(std::memory_order_relaxed);
	const auto ro_miss = ro_slot.miss_count.load(std::memory_order_relaxed);
	const double miss_rate =
	    ro_total > 0 ? static_cast<double>(ro_miss) / static_cast<double>(ro_total) : 0.0;

	// ----- One-shot first-cycle multiplicity / hotness / coverage check. -----
	const idx_t cycles_done = g.completed_collect_cycles.load(std::memory_order_relaxed);
	if (thc_enable_first_cycle_check && !g.first_cycle_multiplicity_checked.load(std::memory_order_relaxed) &&
	    cycles_done == 1) {
		g.first_cycle_multiplicity_checked.store(true, std::memory_order_relaxed);
		const idx_t U1 = g.first_collect_new_entries.load(std::memory_order_relaxed);
		if (U1 > 0) {
			// mu_{S→R} = |R| (1 − p_miss) / U1
			const double estimated_mu_s_to_r =
			    (static_cast<double>(estimated_probe_side_rows) * (1.0 - miss_rate)) / static_cast<double>(U1);
			DEBUG_LOG("[THC First-Cycle Mu] |R|_est=%lu, U1=%lu, miss_rate=%.2f%%, mu_{S->R}=%.4f\n",
			          (unsigned long)estimated_probe_side_rows, (unsigned long)U1, miss_rate * 100.0,
			          estimated_mu_s_to_r);
			if (estimated_mu_s_to_r < thc_min_estimated_mu_s_to_r) {
				DEBUG_LOG("[THC Low-Cross-Multiplicity Bypass] mu_{S->R}=%.4f < %.1f -> abandoning THC globally\n",
				          estimated_mu_s_to_r, thc_min_estimated_mu_s_to_r);
				g.probes_at_abandon.store(g.total_probe_rows.load(std::memory_order_relaxed),
				                          std::memory_order_relaxed);
				g.abandon_reason.store(static_cast<uint8_t>(THCAbandonReason::LowCrossMultiplicity),
				                       std::memory_order_relaxed);
				g.collection_enabled.store(false, std::memory_order_relaxed);
				g.abandoned.store(true, std::memory_order_release);
				return;
			}

			// Estimate the % of rows that are hot on build side
			// % hot = (U1 * u_s) / (|S| * (1−pmiss))
			const double estimated_perc_hot = (static_cast<double>(U1) * mu_s_build_estimate) /
			                                  (static_cast<double>(Count()) * (1.0 - miss_rate));
			if (estimated_perc_hot > thc_max_estimated_perc_hot) {
				DEBUG_LOG("[THC High-Hotness Bypass]: Estimated Hotness is %.2f -> abandoning THC globally\n",
				          estimated_perc_hot);
				g.probes_at_abandon.store(g.total_probe_rows.load(std::memory_order_relaxed),
				                          std::memory_order_relaxed);
				g.abandon_reason.store(static_cast<uint8_t>(THCAbandonReason::HighHotness),
				                       std::memory_order_relaxed);
				g.collection_enabled.store(false, std::memory_order_relaxed);
				g.abandoned.store(true, std::memory_order_release);
				return;
			}

			// Estimate the THC entry count needed to store all of the hot entries
			const idx_t unique_keys_cnt = build_unique_keys_cnt.load(std::memory_order_relaxed);
			const double thc_capacity_needed = static_cast<double>(unique_keys_cnt) * estimated_perc_hot;
			if (static_cast<double>(thc_capacity) < thc_capacity_needed * thc_min_coverage_of_build_side) {
				DEBUG_LOG("THC Too Small for Build Side Bypass: THC capacity needed is %.0f, THC capacity is %lu\n",
				          thc_capacity_needed, thc_capacity);
				g.probes_at_abandon.store(g.total_probe_rows.load(std::memory_order_relaxed),
				                          std::memory_order_relaxed);
				g.abandon_reason.store(static_cast<uint8_t>(THCAbandonReason::THCTooSmallForBuildSide),
				                       std::memory_order_relaxed);
				g.collection_enabled.store(false, std::memory_order_relaxed);
				g.abandoned.store(true, std::memory_order_release);
				return;
			}
		} else {
			DEBUG_LOG("[THC First-Cycle Mu] skipped estimation because U1==0\n");
		}
	}

	// ----- Budget guard for the next COLLECT phase. -----
	const idx_t total_collect = g.total_collect_phase_rows.load(std::memory_order_relaxed);
	const idx_t total_probe = g.total_probe_rows.load(std::memory_order_relaxed);
	const bool budget_ok =
	    (total_collect + thc_collect_phase_rows) <=
	    static_cast<idx_t>(static_cast<double>(total_probe) * thc_collect_budget_fraction);
	const bool can_collect = g.collection_enabled.load(std::memory_order_relaxed) &&
	                         (miss_rate >= thc_miss_below_which_skip_collect) && budget_ok;

	DEBUG_LOG("[Checkpoint] eval_phase=%lu (eval_cycles_done=%lu), miss_rate=%.2f%%, budget_ok=%d -> %s\n",
	          (unsigned long)state.observed_phase_number,
	          (unsigned long)g.completed_evaluation_cycles.load(std::memory_order_relaxed), miss_rate * 100.0,
	          (int)budget_ok, can_collect ? "COLLECT" : "SKIP");

	// Helper: advance phase_number by one and (if we just wrapped past
	// MAX_PHASES) clear the entire metrics array. Called inside
	// transition_mutex. Returns the new phase_number.
	auto advance_phase_number = [&]() {
		const idx_t new_phase = g.phase_number.fetch_add(1, std::memory_order_relaxed) + 1;
		if (new_phase > 0 && new_phase % g.MAX_PHASES == 0) {
			for (idx_t i = 0; i < g.MAX_PHASES; ++i) {
				g.phase_metrics[i].time_ns.store(0, std::memory_order_relaxed);
				g.phase_metrics[i].probe_count.store(0, std::memory_order_relaxed);
				g.phase_metrics[i].miss_count.store(0, std::memory_order_relaxed);
			}
		}
		return new_phase;
	};

	// Record this RO segment as the "previous eval" for the next checkpoint's
	// shrinkage computation. Do this before any branch returns so all paths
	// agree on what counts as "previous."
	g.prev_eval_phase_number = state.observed_phase_number;

	// ----- High-miss abandonment. -----
	if (miss_rate >= THC_ABANDON_MISS_THRESHOLD) {
		const idx_t streak = g.consecutive_high_miss_checkpoints.fetch_add(1, std::memory_order_relaxed) + 1;
		if (streak >= THC_ABANDON_CONSECUTIVE_MISSES) {
			DEBUG_LOG("[THC Abandon] global abandon after %lu consecutive high-miss checkpoints "
			          "(miss_rate=%.2f%%)\n",
			          (unsigned long)streak, miss_rate * 100.0);
			g.probes_at_abandon.store(g.total_probe_rows.load(std::memory_order_relaxed),
			                          std::memory_order_relaxed);
			g.abandon_reason.store(static_cast<uint8_t>(THCAbandonReason::HighMissRate),
			                       std::memory_order_relaxed);
			g.abandoned.store(true, std::memory_order_release);
			return;
		}
	} else {
		g.consecutive_high_miss_checkpoints.store(0, std::memory_order_relaxed);
	}

	g.completed_evaluation_cycles.fetch_add(1, std::memory_order_relaxed);

	if (!can_collect) {
		// STAY-in-RO: refresh the target for a new RO segment and advance
		// phase_number so the next segment gets its own clean slot. The
		// phase type stays READ_ONLY; current_collect_phase_number is
		// unchanged (the COLLECT that fed *both* this and the next RO
		// segment was the same one).
		g.read_only_rows_target.store(thc_first_read_only_phase_rows, std::memory_order_relaxed);
		advance_phase_number();
		return;
	}

	// ----- Cost-based three-way decision rule. -----
	const double delta_t = c_eval_current - g.c_main;
	const double gamma_t = c_grow_current - c_eval_current;
	// shrinkage = δ^{t-1} − δ^t = c_eval_prev − c_eval_current
	const double shrinkage = c_eval_prev - c_eval_current;

	const idx_t current_eval_cycle = g.eval_cycle_count.load(std::memory_order_relaxed);
	g.eval_cycle_count.fetch_add(1, std::memory_order_relaxed);

	// Helper: transition to the next COLLECT cycle. Increments
	// phase_number to the new COLLECT phase, captures that index as the
	// new `current_collect_phase_number`, and sets phase type COLLECT.
	auto enter_next_collect_cycle = [&]() {
		const idx_t new_phase = advance_phase_number();
		g.current_collect_phase_number = new_phase;
		g.phase.store(TieredHashCachePhase::COLLECT, std::memory_order_release);
	};

	// Warmup: unconditionally continue to give the THC time to stabilize.
	if (current_eval_cycle + 1 <= thc_warmup_cycles) {
		DEBUG_LOG("[Eval Checkpoint] eval_cycle=%lu (warmup, need %lu), c_eval=%.2f, c_main=%.2f, "
		          "delta=%.2f, miss_rate=%.2f%% -> CONTINUE (warmup)\n",
		          (unsigned long)current_eval_cycle, (unsigned long)thc_warmup_cycles, c_eval_current, g.c_main,
		          delta_t, miss_rate * 100.0);
		enter_next_collect_cycle();
		return;
	}

	if (delta_t >= 0) {
		DEBUG_LOG("[Eval Checkpoint] eval_cycle=%lu, c_eval=%.2f, c_main=%.2f, delta=%.2f >= 0 -> DROP\n",
		          (unsigned long)current_eval_cycle, c_eval_current, g.c_main, delta_t);
		g.probes_at_abandon.store(g.total_probe_rows.load(std::memory_order_relaxed), std::memory_order_relaxed);
		g.abandon_reason.store(static_cast<uint8_t>(THCAbandonReason::THCIncreasesProbeCost),
		                       std::memory_order_relaxed);
		g.abandoned.store(true, std::memory_order_release);
		return;
	}

	// delta_t < 0: THC is useful. Check if further growth is worth paying for.
	if (shrinkage < gamma_t) {
		DEBUG_LOG("[Eval Checkpoint] eval_cycle=%lu, c_eval=%.2f, c_main=%.2f, delta=%.2f, "
		          "shrinkage=%.2f < gamma=%.2f -> FREEZE\n",
		          (unsigned long)current_eval_cycle, c_eval_current, g.c_main, delta_t, shrinkage, gamma_t);
		g.probes_at_freeze.store(g.total_probe_rows.load(std::memory_order_relaxed), std::memory_order_relaxed);
		g.freeze_reason.store(static_cast<uint8_t>(THCFreezeReason::MarginalGainNotWorthCollectionCost),
		                      std::memory_order_relaxed);
		g.collection_enabled.store(false, std::memory_order_release);
		// Stay in READ_ONLY but advance phase_number so future RO segments
		// get fresh slots. current_collect_phase_number is unchanged.
		advance_phase_number();
		return;
	}

	// Growth paid for itself — continue to next COLLECT phase.
	DEBUG_LOG("[Eval Checkpoint] eval_cycle=%lu, c_eval=%.2f, c_main=%.2f, delta=%.2f, "
	          "shrinkage=%.2f >= gamma=%.2f -> CONTINUE\n",
	          (unsigned long)current_eval_cycle, c_eval_current, g.c_main, delta_t, shrinkage, gamma_t);
	enter_next_collect_cycle();
}

void JoinHashTable::FlushCollectedEntriesIntoTHC(ProbeState &state, GlobalTHCAdaptiveState &g) {
	if (state.collected_entries.empty()) {
		return;
	}
	ScopedHashJoinTimer insert_timer(state.thc_insert_time_ns);

	// Time the flush so the InsertBatch cost is attributed to the COLLECT
	// cycle (forms part of c_grow_current → gamma_t in the cost rule).
	const auto t0 = std::chrono::steady_clock::now();
	idx_t new_entries_this_phase;
	if (thc_single_threaded) {
		new_entries_this_phase = tiered_hash_cache->InsertBatch<true>(state.collected_entries.data(),
		                                                              state.collected_entries.size());
	} else {
		new_entries_this_phase = tiered_hash_cache->InsertBatch<false>(state.collected_entries.data(),
		                                                               state.collected_entries.size());
	}
	const auto t1 = std::chrono::steady_clock::now();
	const auto delta_ns =
	    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
	// Credit InsertBatch time to the COLLECT phase this buffer was
	// collected during, indexed by the per-thread `observed_phase_number`
	// captured at the START of that COLLECT-phase call. Late arrivals
	// after the leader's RO-checkpoint snapshot of c_grow are harmless —
	// they accumulate in the just-ended phase's own slot, never
	// contaminate any other phase.
	g.phase_metrics[state.observed_phase_number % g.MAX_PHASES].time_ns.fetch_add(delta_ns,
	                                                                              std::memory_order_relaxed);

	if (tiered_hash_cache->IsFull()) {
		DEBUG_LOG("THC has reached desired load factor - global freeze (THC-Full).\n");
		// First thread to observe THC-full wins the freeze_reason CAS.
		uint8_t expected_none = static_cast<uint8_t>(THCFreezeReason::None);
		g.freeze_reason.compare_exchange_strong(expected_none, static_cast<uint8_t>(THCFreezeReason::THCFull),
		                                         std::memory_order_relaxed);
		// probes_at_freeze: take the max across observers (multiple threads
		// may detect IsFull at slightly different probe counts).
		const idx_t snapshot = g.total_probe_rows.load(std::memory_order_relaxed);
		idx_t cur = g.probes_at_freeze.load(std::memory_order_relaxed);
		while (snapshot > cur &&
		       !g.probes_at_freeze.compare_exchange_weak(cur, snapshot, std::memory_order_relaxed)) {
		}
		g.collection_enabled.store(false, std::memory_order_release);
	}

	g.total_new_entries.fetch_add(new_entries_this_phase, std::memory_order_relaxed);
	if (state.observed_phase_number == 1) {
		// Phase 1 is, by construction, cycle-0's COLLECT (BASELINE was 0).
		// Only flushes from that phase contribute to U1, the
		// cross-multiplicity estimator's denominator. After wraparound
		// (phase_number = 4097, 8193, ...) the index recurs, but by then
		// first_cycle_multiplicity_checked is already true and U1 is
		// unused, so a stale contribution would be moot.
		g.first_collect_new_entries.fetch_add(new_entries_this_phase, std::memory_order_relaxed);
	}

	DEBUG_LOG("[Flush] phase=%lu, buffered=%lu, new_entries=%lu, total_new=%lu, fill=%lu/%lu\n",
	          (unsigned long)state.observed_phase_number, (unsigned long)state.collected_entries.size(),
	          (unsigned long)new_entries_this_phase,
	          (unsigned long)g.total_new_entries.load(std::memory_order_relaxed),
	          (unsigned long)tiered_hash_cache->new_inserts_count.load(),
	          (unsigned long)tiered_hash_cache->GetCapacity());

	state.collected_entries.clear();
	state.collected_entries.shrink_to_fit();
}

void JoinHashTable::Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes) {
	if (count == keys.size()) {
		// no null values are filtered: use regular hash functions // USING THIS since we dont have nulls
		VectorOperations::Hash(keys.data[0], hashes, keys.size());
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], keys.size());
		}
	} else {
		// null values were filtered: use selection vector
		VectorOperations::Hash(keys.data[0], hashes, sel, count);
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], sel, count);
		}
	}
}

static idx_t FilterNullValues(UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t count,
                              SelectionVector &result) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto key_idx = vdata.sel->get_index(idx);
		if (vdata.validity.RowIsValid(key_idx)) {
			result.set_index(result_count++, idx);
		}
	}
	return result_count;
}

void JoinHashTable::Build(PartitionedTupleDataAppendState &append_state, DataChunk &keys, DataChunk &payload) {
	D_ASSERT(!finalized);
	D_ASSERT(keys.size() == payload.size());
	if (keys.size() == 0) {
		return;
	}
	// special case: correlated mark join
	if (join_type == JoinType::MARK && !correlated_mark_join_info.correlated_types.empty()) {
		auto &info = correlated_mark_join_info;
		lock_guard<mutex> mj_lock(info.mj_lock);
		// Correlated MARK join
		// for the correlated mark join we need to keep track of COUNT(*) and COUNT(COLUMN) for each of the
		// correlated columns push into the aggregate hash table
		D_ASSERT(info.correlated_counts);
		info.group_chunk.SetCardinality(keys);
		for (idx_t i = 0; i < info.correlated_types.size(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		if (info.correlated_payload.data.empty()) {
			vector<LogicalType> types;
			types.push_back(keys.data[info.correlated_types.size()].GetType());
			info.correlated_payload.InitializeEmpty(types);
		}
		info.correlated_payload.SetCardinality(keys);
		info.correlated_payload.data[0].Reference(keys.data[info.correlated_types.size()]);
		info.correlated_counts->AddChunk(info.group_chunk, info.correlated_payload, AggregateType::NON_DISTINCT);
	}

	// build a chunk to append to the data collection [keys, payload, (optional "found" boolean), hash]
	DataChunk source_chunk;
	source_chunk.InitializeEmpty(layout_ptr->GetTypes());
	for (idx_t i = 0; i < keys.ColumnCount(); i++) {
		source_chunk.data[i].Reference(keys.data[i]);
	}
	idx_t col_offset = keys.ColumnCount();
	D_ASSERT(build_types.size() == payload.ColumnCount());
	for (idx_t i = 0; i < payload.ColumnCount(); i++) {
		source_chunk.data[col_offset + i].Reference(payload.data[i]);
	}
	col_offset += payload.ColumnCount();
	if (PropagatesBuildSide(join_type)) {
		// for FULL/RIGHT OUTER joins initialize the "found" boolean to false
		source_chunk.data[col_offset].Reference(vfound);
		col_offset++;
	}
	Vector hash_values(LogicalType::HASH);
	source_chunk.data[col_offset].Reference(hash_values);
	source_chunk.SetCardinality(keys);

	// ToUnifiedFormat the source chunk
	TupleDataCollection::ToUnifiedFormat(append_state.chunk_state, source_chunk);

	// prepare the keys for processing
	const SelectionVector *current_sel;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t added_count = PrepareKeys(keys, append_state.chunk_state.vector_data, current_sel, sel, true);
	if (added_count < keys.size()) {
		has_null = true;
	}
	if (added_count == 0) {
		return;
	}

	// hash the keys and obtain an entry in the list
	// note that we only hash the keys used in the equality comparison
	Hash(keys, *current_sel, added_count, hash_values);

	// Re-reference and ToUnifiedFormat the hash column after computing it
	source_chunk.data[col_offset].Reference(hash_values);
	hash_values.ToUnifiedFormat(source_chunk.size(), append_state.chunk_state.vector_data.back().unified);

	// We already called TupleDataCollection::ToUnifiedFormat, so we can AppendUnified here
	sink_collection->AppendUnified(append_state, source_chunk, *current_sel, added_count);
}

idx_t JoinHashTable::PrepareKeys(DataChunk &keys, vector<TupleDataVectorFormat> &vector_data,
                                 const SelectionVector *&current_sel, SelectionVector &sel, bool build_side) {
	// figure out which keys are NULL, and create a selection vector out of them
	current_sel = FlatVector::IncrementalSelectionVector();
	idx_t added_count = keys.size();
	if (build_side && PropagatesBuildSide(join_type)) {
		// in case of a right or full outer join, we cannot remove NULL keys from the build side
		return added_count;
	}

	for (idx_t col_idx = 0; col_idx < keys.ColumnCount(); col_idx++) {
		// see internal issue 3717.
		if (join_type == JoinType::MARK && !correlated_mark_join_info.correlated_types.empty()) {
			continue;
		}
		if (null_values_are_equal[col_idx]) {
			continue;
		}
		auto &col_key_data = vector_data[col_idx].unified;
		if (col_key_data.validity.AllValid()) {
			continue;
		}
		added_count = FilterNullValues(col_key_data, *current_sel, added_count, sel);
		// null values are NOT equal for this column, filter them out
		current_sel = &sel;
	}
	return added_count;
}

static void StorePointer(const const_data_ptr_t &pointer, const data_ptr_t &target) {
	Store<uint64_t>(cast_pointer_to_uint64(pointer), target);
}

static data_ptr_t LoadPointer(const const_data_ptr_t &source) {
	return cast_uint64_to_pointer(Load<uint64_t>(source));
}

//! If we consider to insert into an entry we expct to be empty, if it was filled in the meantime the insert will
//! not happen and we need to return the pointer to the to row with which the new entry would have collided. In any
//! other case we return a nullptr
template <bool PARALLEL, bool EXPECT_EMPTY>
static inline data_ptr_t InsertRowToEntry(atomic<ht_entry_t> &entry, const data_ptr_t &row_ptr_to_insert,
                                          const hash_t &salt, const idx_t &pointer_offset) {
	const ht_entry_t desired_entry(salt, row_ptr_to_insert);
	if (PARALLEL) {
		if (EXPECT_EMPTY) {
			// Add nullptr to the end of the list to mark the end
			StorePointer(nullptr, row_ptr_to_insert + pointer_offset);

			ht_entry_t expected_entry;
			entry.compare_exchange_strong(expected_entry, desired_entry, std::memory_order_acquire,
			                              std::memory_order_relaxed);

			// The expected entry is updated with the encountered entry by the compare exchange
			// So, this returns a nullptr if it was empty, and a non-null if it was not (which cancels the insert)
			return expected_entry.GetPointerOrNull();
		} else {
			// At this point we know that the keys match, so we can try to insert until we succeed
			ht_entry_t expected_entry = entry.load(std::memory_order_relaxed);
			D_ASSERT(expected_entry.IsOccupied());
			do {
				data_ptr_t current_row_pointer = expected_entry.GetPointer();
				StorePointer(current_row_pointer, row_ptr_to_insert + pointer_offset);
			} while (!entry.compare_exchange_weak(expected_entry, desired_entry, std::memory_order_release,
			                                      std::memory_order_relaxed));

			return nullptr;
		}
	} else {
		// If we are not in parallel mode, we can just do the operation without any checks
		data_ptr_t current_row_pointer = entry.load(std::memory_order_relaxed).GetPointerOrNull();
		StorePointer(current_row_pointer, row_ptr_to_insert + pointer_offset);
		entry = desired_entry;
		return nullptr;
	}
}
static inline void PerformKeyComparison(JoinHashTable::InsertState &state, JoinHashTable &ht,
                                        const TupleDataCollection &data_collection, Vector &row_locations,
                                        const idx_t count, idx_t &key_match_count, idx_t &key_no_match_count) {
	// Get the data for the rows that need to be compared
	state.lhs_data.Reset();
	state.lhs_data.SetCardinality(count); // the right size

	// The target selection vector says where to write the results into the lhs_data, we just want to write
	// sequentially as otherwise we trigger a bug in the Gather function
	data_collection.ResetCachedCastVectors(state.chunk_state, ht.equality_predicate_columns);
	data_collection.Gather(row_locations, state.keys_to_compare_sel, count, ht.equality_predicate_columns,
	                       state.lhs_data, *FlatVector::IncrementalSelectionVector(),
	                       state.chunk_state.cached_cast_vectors);
	TupleDataCollection::ToUnifiedFormat(state.chunk_state, state.lhs_data);

	for (idx_t i = 0; i < count; i++) {
		state.key_match_sel.set_index(i, i);
	}

	// Perform row comparisons
	key_match_count = ht.row_matcher_build.Match(state.lhs_data, state.chunk_state.vector_data, state.key_match_sel,
	                                             count, *ht.layout_ptr, state.rhs_row_locations,
	                                             &state.keys_no_match_sel, key_no_match_count);

	D_ASSERT(key_match_count + key_no_match_count == count);
}

template <bool PARALLEL>
static inline void InsertMatchesAndIncrementMisses(atomic<ht_entry_t> entries[], JoinHashTable::InsertState &state,
                                                   JoinHashTable &ht, const data_ptr_t lhs_row_locations[],
                                                   idx_t ht_offsets[], const hash_t hash_salts[],
                                                   const idx_t capacity_mask, const idx_t key_match_count,
                                                   const idx_t key_no_match_count) {
	if (key_match_count != 0) {
		ht.chains_longer_than_one = true;
	}

	// Insert the rows that match
	for (idx_t i = 0; i < key_match_count; i++) {
		const auto need_compare_idx = state.key_match_sel.get_index(i);
		const auto entry_index = state.keys_to_compare_sel.get_index(need_compare_idx);

		const auto &ht_offset = ht_offsets[entry_index];
		auto &entry = entries[ht_offset];
		const auto row_ptr_to_insert = lhs_row_locations[entry_index];

		const auto salt = hash_salts[entry_index];
		InsertRowToEntry<PARALLEL, false>(entry, row_ptr_to_insert, salt, ht.pointer_offset);
	}

	// Linear probing: each of the entries that do not match move to the next entry in the HT
	for (idx_t i = 0; i < key_no_match_count; i++) {
		const auto need_compare_idx = state.keys_no_match_sel.get_index(i);
		const auto entry_index = state.keys_to_compare_sel.get_index(need_compare_idx);

		auto &ht_offset = ht_offsets[entry_index];
		IncrementAndWrap(ht_offset, capacity_mask);

		state.remaining_sel.set_index(i, entry_index);
	}
}

template <bool PARALLEL>
static void InsertHashesLoop(atomic<ht_entry_t> entries[], Vector &row_locations, Vector &hashes_v, const idx_t &count,
                             JoinHashTable::InsertState &state, const TupleDataCollection &data_collection,
                             JoinHashTable &ht) {
	D_ASSERT(hashes_v.GetType().id() == LogicalType::HASH);
	ApplyBitmaskAndGetSaltBuild(hashes_v, state.salt_v, count, ht.bitmask);

	// the salts offset for each row to insert
	const auto ht_offsets = FlatVector::GetData<idx_t>(hashes_v);
	const auto hash_salts = FlatVector::GetData<hash_t>(state.salt_v);
	// the row locations of the rows that are already in the hash table
	const auto rhs_row_locations = FlatVector::GetData<data_ptr_t>(state.rhs_row_locations);
	// the row locations of the rows that are to be inserted
	const auto lhs_row_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// we start off with the entire chunk
	idx_t remaining_count = count;
	const auto *remaining_sel = FlatVector::IncrementalSelectionVector();

	if (PropagatesBuildSide(ht.join_type)) {
		// if we propagate the build side, we may have added rows with NULL keys to the HT
		// these may need to be filtered out depending on the comparison type (exactly like PrepareKeys does)
		for (idx_t col_idx = 0; col_idx < ht.conditions.size(); col_idx++) {
			// if null values are NOT equal for this column we filter them out
			if (ht.NullValuesAreEqual(col_idx)) {
				continue;
			}

			idx_t entry_idx;
			idx_t idx_in_entry;
			ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

			idx_t new_remaining_count = 0;
			for (idx_t i = 0; i < remaining_count; i++) {
				const auto idx = remaining_sel->get_index(i);
				if (ValidityBytes(lhs_row_locations[idx], count).RowIsValidUnsafe(col_idx)) {
					state.remaining_sel.set_index(new_remaining_count++, idx);
				}
			}
			remaining_count = new_remaining_count;
			remaining_sel = &state.remaining_sel;
		}
	}

	// use the ht bitmask to make the modulo operation faster but keep the salt bits intact
	idx_t capacity_mask = ht.bitmask | ht_entry_t::SALT_MASK;
	while (remaining_count > 0) {
		idx_t salt_match_count = 0;

		// iterate over each entry to find out whether it belongs to an existing list or will start a new list
		for (idx_t i = 0; i < remaining_count; i++) {
			const idx_t row_index = remaining_sel->get_index(i);
			auto &ht_offset = ht_offsets[row_index];
			auto &salt = hash_salts[row_index];

			// increment the ht_offset of the entry as long as next entry is occupied and salt does not match
			ht_entry_t entry;
			bool occupied;
			while (true) {
				atomic<ht_entry_t> &atomic_entry = entries[ht_offset];
				entry = atomic_entry.load(std::memory_order_relaxed);
				occupied = entry.IsOccupied();

				// condition for incrementing the ht_offset: occupied and row_salt does not match -> move to next
				// entry
				if (!occupied) {
					break;
				}
				if (entry.GetSalt() == salt) {
					break;
				}

				IncrementAndWrap(ht_offset, capacity_mask);
			}

			if (!occupied) { // insert into free
				auto &atomic_entry = entries[ht_offset];
				const auto row_ptr_to_insert = lhs_row_locations[row_index];
				const auto potential_collided_ptr =
				    InsertRowToEntry<PARALLEL, true>(atomic_entry, row_ptr_to_insert, salt, ht.pointer_offset);

				// mu_s estimation Approach A: count unique build-side keys as successful first-insertions
				// Only count when the slot was truly empty (no race). In non-parallel builds,
				// InsertRowToEntry always returns nullptr here.
				if (!PARALLEL) {
					ht.CountOneUniqueBuildKey();
				} else {
					// if the insertion was not successful, the entry was occupied in the meantime, so we have to
					// compare the keys and insert the row to the next entry
					if (DUCKDB_UNLIKELY(potential_collided_ptr != nullptr)) {
						// if the entry was occupied, we need to compare the keys and insert the row to the next
						// entry we need to compare the keys and insert the row to the next entry
						state.keys_to_compare_sel.set_index(salt_match_count, row_index);
						rhs_row_locations[salt_match_count] = potential_collided_ptr;
						salt_match_count += 1;
					} else {
						// truly first insertion into this slot -> new unique key chain
						ht.CountOneUniqueBuildKey();
					}
				}

			} else { // compare with full entry
				state.keys_to_compare_sel.set_index(salt_match_count, row_index);
				rhs_row_locations[salt_match_count] = entry.GetPointer();
				salt_match_count += 1;
			}
		}

		// at this step, for all the rows to insert we stepped either until we found an empty entry or an entry with
		// a matching salt, we now need to compare the keys for the ones that have a matching salt
		idx_t key_no_match_count = 0;
		if (salt_match_count != 0) {
			idx_t key_match_count = 0;
			PerformKeyComparison(state, ht, data_collection, row_locations, salt_match_count, key_match_count,
			                     key_no_match_count);
			InsertMatchesAndIncrementMisses<PARALLEL>(entries, state, ht, lhs_row_locations, ht_offsets, hash_salts,
			                                          capacity_mask, key_match_count, key_no_match_count);
		}

		// update the overall selection vector to only point the entries that still need to be inserted
		// as there was no match found for them yet
		remaining_sel = &state.remaining_sel;
		remaining_count = key_no_match_count;
	}
}

void JoinHashTable::InsertHashes(Vector &hashes_v, const idx_t count, TupleDataChunkState &chunk_state,
                                 InsertState &insert_state, bool parallel) {
	auto atomic_entries = reinterpret_cast<atomic<ht_entry_t> *>(this->entries);
	auto row_locations = chunk_state.row_locations;
	if (parallel) {
		InsertHashesLoop<true>(atomic_entries, row_locations, hashes_v, count, insert_state, *data_collection, *this);
	} else {
		InsertHashesLoop<false>(atomic_entries, row_locations, hashes_v, count, insert_state, *data_collection, *this);
	}
}

void JoinHashTable::AllocatePointerTable() {
#ifdef DEBUG
	std::this_thread::sleep_for(std::chrono::seconds(1));
	fprintf(stderr, "//////////////////////////////////////////////////////////////////////////////////////////////////"
	                "//////////////////////\n");
	fprintf(stderr, "//////////////////////////////////////////////////////////////////////////////////////////////////"
	                "//////////////////////\n");
	fprintf(stderr, "////////////////////   STARTING A NEW HASH JOIN BETWEEN %s AND %s   ///////////////////////////////\n",
	        probe_table_name.empty() ? "?" : probe_table_name.c_str(),
	        build_table_name.empty() ? "?" : build_table_name.c_str());
	fprintf(stderr, "//////////////////////////////////////////////////////////////////////////////////////////////////"
	                "//////////////////////\n");
	fprintf(stderr, "//////////////////////////////////////////////////////////////////////////////////////////////////"
	                "//////////////////////\n");
#endif

	idx_t data_collection_row_cnt = Count();
	capacity = PointerTableCapacity(data_collection_row_cnt);
	D_ASSERT(IsPowerOfTwo(capacity));
	DEBUG_LOG("[JoinHashTable::AllocatePointerTable] Pointer table capacity is %lu for a build side row count of %lu\n",
	          (unsigned long)capacity, data_collection_row_cnt);

	if (hash_map.get()) {
		// There is already a hash map
		auto current_capacity = hash_map.GetSize() / sizeof(ht_entry_t);
		if (capacity > current_capacity) {
			// Need more space
			hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(ht_entry_t));
			entries = reinterpret_cast<ht_entry_t *>(hash_map.get());
		} else {
			// Just use the current hash map
			capacity = current_capacity;
		}
	} else {
		// Allocate a hash map
		hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(ht_entry_t));
		entries = reinterpret_cast<ht_entry_t *>(hash_map.get());
	}
	D_ASSERT(hash_map.GetSize() == capacity * sizeof(ht_entry_t));

	bitmask = capacity - 1;
}

void JoinHashTable::InitializePointerTable(idx_t entry_idx_from, idx_t entry_idx_to) {
	// initialize HT with all-zero entries
	std::fill_n(entries + entry_idx_from, entry_idx_to - entry_idx_from, ht_entry_t());
}

void JoinHashTable::Finalize(idx_t chunk_idx_from, idx_t chunk_idx_to, bool parallel) {
	// Pointer table should be allocated
	D_ASSERT(hash_map.get());

	Vector hashes(LogicalType::HASH);
	auto hash_data = FlatVector::GetData<hash_t>(hashes);

	TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::KEEP_EVERYTHING_PINNED, chunk_idx_from,
	                                chunk_idx_to, false);
	const auto row_locations = iterator.GetRowLocations();

	InsertState insert_state(*this);
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = 0; i < count; i++) {
			hash_data[i] = Load<hash_t>(row_locations[i] + pointer_offset);
		}
		TupleDataChunkState &chunk_state = iterator.GetChunkState();

		InsertHashes(hashes, count, chunk_state, insert_state, parallel);
	} while (iterator.Next());
}

void JoinHashTable::InitializeTieredHashCache() {
	auto &config = ClientConfig::GetConfig(context);
	// Before any early returns, compute and optionally log build-side mu_s estimates
	// that rely solely on the finalized HT (Build-phase approach and HT sampling approach). These are independent
	// of whether the THC itself is enabled.

	DEBUG_LOG("Size in mb of hash table is %lu\n", Count() * 8 / (1024 * 1024));
	if (thc_mu_s_method == "build_count" || thc_mu_s_method == "all") {
		const idx_t unique_keys_cnt = build_unique_keys_cnt.load(std::memory_order_relaxed);
		if (unique_keys_cnt > 0) {
			mu_s_build_estimate = static_cast<double>(Count()) / static_cast<double>(unique_keys_cnt);
			if (thc_log_mu_s) {
				std::fprintf(stderr, "[mu_s build_count] rows=%lu unique=%lu mu_s=%.6f\n", (unsigned long)Count(),
				             (unsigned long)unique_keys_cnt, mu_s_build_estimate);
				std::fflush(stderr);
			}
		}
	}
	if (thc_mu_s_method == "ht_sample" || thc_mu_s_method == "all") {
		mu_s_ht_sample_estimate = EstimateMuSFromHTSample();
		if (thc_log_mu_s) {
			std::fprintf(stderr, "[mu_s ht_sample] capacity=%lu mu_s=%.6f\n", (unsigned long)capacity,
			             mu_s_ht_sample_estimate);
			std::fflush(stderr);
		}
	}

	if (config.disable_tiered_hash_cache) {
		DEBUG_LOG("[JoinHashTable::InitializeTieredHashCache] Not instantiating THC since it's disabled with "
		          "disable_tiered_hash_cache.\n");
		return;
	}

	if (capacity <= thc_activation_threshold) {
		DEBUG_LOG("[JoinHashTable::InitializeTieredHashCache] Not instantiating THC since capacity of %lu does not "
		          "meet thc_activation_threshold of %lu\n",
		          capacity, thc_activation_threshold);
		return;
	}

	// Only activate for all-constant (fixed-size) equality key types
	// TODO support non-fixed sized merge keys in THC
	for (const auto &type : equality_types) {
		if (type.InternalType() == PhysicalType::VARCHAR || type.InternalType() == PhysicalType::STRUCT ||
		    type.InternalType() == PhysicalType::LIST) {
			DEBUG_LOG("[JoinHashTable::InitializeTieredHashCache] Not instantiating THC since unsupported key type.\n");
			return;
		}
	}

	// THC stores one data_collection row per cache entry, including the next_pointer
	// at the end of the row that acts as a chain pointer on the build side.
	// It's found at pointer_offset on the build side and enables AdvancePointers to follow chains.
	// Cache hits in THC completely bypass data_collection for key matching
	// and payload gathering (GatherResult), but only for the first key match.
	// For chain following (in case there are duplicate keys), need to go to data_collection.
	// TODO consts below are hacks - generalize!!!
	const idx_t data_collection_row_size =
	    pointer_offset + sizeof(data_ptr_t);                    // TODO might be duplicative of logic in THC
	const idx_t row_copy_offset = 0;                            // TODO hack?
	tiered_hash_cache_key_offset = layout_ptr->GetOffsets()[0]; // key after validity bytes // TODO this is a hack!!!
	thc_capacity = TieredHashCache::ComputeCapacity(data_collection_row_size, thc_budget_bytes);

	// ---------------------------------------------------------------
	// Coverage ratio check: skip THC if it can only cache a tiny
	// fraction of the hash table.
	//
	// For uniform access patterns (e.g., TPC-H Q5 l_orderkey join),
	// the THC hit rate approximately equals cache_capacity / ht_capacity.
	// If this ratio is too low, the per-probe overhead of THC (hash
	// densification, cache lookup, fallback merge) exceeds the savings
	// from the rare hits.  We require the THC to cover at least 5% of
	// the HT to have a reasonable chance of helping.
	//
	// A 5% coverage means 5% of probes hit on average (for uniform
	// access), saving ~200ns per hit but costing ~10ns per miss.
	// Break-even: 0.95 × 10ns < 0.05 × 200ns → 9.5 < 10 → marginal.
	// Below 5%, THC is net negative for uniform workloads and quickly
	// gets abandoned anyway (wasting the collect-phase overhead).
	// ---------------------------------------------------------------
	static constexpr double MIN_COVERAGE_RATIO = 0.00;
	const double coverage_ratio = static_cast<double>(thc_capacity) / static_cast<double>(capacity);
	if (coverage_ratio < MIN_COVERAGE_RATIO) {
		DEBUG_LOG("[JoinHashTable::InitializeTieredHashCache] Not instantiating THC since coverage ratio %.2f%% "
		          "(cache_capacity=%lu, ht_capacity=%lu) below %.0f%% threshold\n",
		          coverage_ratio * 100.0, (unsigned long)thc_capacity, (unsigned long)capacity,
		          MIN_COVERAGE_RATIO * 100.0);
		return;
	}

	thc_entry_stride = (sizeof(TieredHashCache::tag_t) + data_collection_row_size + 7) & ~idx_t(7);
	DEBUG_LOG("[JoinHashTable::InitializeTieredHashCache] Instantiating THC (cache_capacity=%lu, row_size=%lu, "
	          "key_offset=%lu, row_copy_offset=%lu, "
	          "coverage=%.2f%%, tuple_size=%lu, pointer_offset=%lu, entry_stride=%lu, total=%.1f MiB)\n",
	          (unsigned long)thc_capacity, (unsigned long)data_collection_row_size,
	          (unsigned long)tiered_hash_cache_key_offset, (unsigned long)row_copy_offset, coverage_ratio * 100.0,
	          (unsigned long)tuple_size, (unsigned long)pointer_offset, (unsigned long)thc_entry_stride,
	          (double)(thc_capacity * thc_entry_stride) / (1024.0 * 1024.0));
	DEBUG_LOG("[JoinHashTable::InitializeTieredHashCache] Estimated probe-side rows=%lu\n",
	          (unsigned long)estimated_probe_side_rows);
	tiered_hash_cache = make_uniq<TieredHashCache>(thc_capacity, data_collection_row_size, tiered_hash_cache_key_offset,
	                                               row_copy_offset, thc_max_load_factor);

	thc_single_threaded = (TaskScheduler::GetScheduler(context).NumberOfThreads() == 1);

	// Cross-thread adaptive state lives alongside the THC and is destroyed
	// with it. Every probing thread reads/updates the same instance.
	global_thc_state = make_uniq<GlobalTHCAdaptiveState>();
}

void JoinHashTable::CountOneUniqueBuildKey() {
	build_unique_keys_cnt.fetch_add(1, std::memory_order_relaxed);
}

double JoinHashTable::EstimateMuSFromHTSample() {
	if (!entries || capacity == 0) {
		return 0.0;
	}
	// Sample up to 1024 entries evenly across the table
	const idx_t target_samples = 1024;
	const idx_t stride = capacity <= target_samples ? 1 : (capacity / target_samples);
	idx_t observed = 0;
	idx_t total_chain_len = 0;
	for (idx_t i = 0; i < capacity; i += stride) {
		const ht_entry_t &entry = entries[i];
		if (!entry.IsOccupied()) {
			continue;
		}
		idx_t chain_len = 0;
		const_data_ptr_t ptr = entry.GetPointer();
		while (ptr) {
			chain_len++;
			ptr = LoadPointer(ptr + pointer_offset);
		}
		total_chain_len += chain_len;
		observed++;
	}
	if (observed == 0) {
		return 0.0;
	}
	return static_cast<double>(total_chain_len) / static_cast<double>(observed);
}

void JoinHashTable::InitializeScanStructure(ScanStructure &scan_structure, DataChunk &keys,
                                            TupleDataChunkState &key_state, const SelectionVector *&current_sel) {
	D_ASSERT(Count() > 0); // should be handled before
	D_ASSERT(finalized);

	// set up the scan structure
	scan_structure.is_null = false;
	scan_structure.finished = false;
	if (join_type != JoinType::INNER) {
		memset(scan_structure.found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
	}

	// first prepare the keys for probing
	TupleDataCollection::ToUnifiedFormat(key_state, keys);
	scan_structure.count = PrepareKeys(keys, key_state.vector_data, current_sel, scan_structure.sel_vector, false);

	if (scan_structure.count < keys.size()) {
		scan_structure.has_null_value_filter = true;
	} else {
		scan_structure.has_null_value_filter = false;
	}
}

void JoinHashTable::Probe(ScanStructure &scan_structure, DataChunk &keys, TupleDataChunkState &key_state,
                          ProbeState &probe_state, optional_ptr<Vector> precomputed_hashes) {
	const SelectionVector *current_sel;
	InitializeScanStructure(scan_structure, keys, key_state, current_sel);
	if (scan_structure.count == 0) {
		return;
	}
	if (precomputed_hashes) {
		GetRowPointers(keys, key_state, probe_state, *precomputed_hashes, current_sel, scan_structure.count,
		               scan_structure.pointers, scan_structure.sel_vector, scan_structure.has_null_value_filter);
	} else {
		Vector hashes(LogicalType::HASH);
		// hash all the keys
		Hash(keys, *current_sel, scan_structure.count, hashes);

		// now initialize the pointers of the scan structure based on the hashes
		GetRowPointers(keys, key_state, probe_state, hashes, current_sel, scan_structure.count, scan_structure.pointers,
		               scan_structure.sel_vector, scan_structure.has_null_value_filter);
	}
}

ScanStructure::ScanStructure(JoinHashTable &ht_p, TupleDataChunkState &key_state_p)
    : key_state(key_state_p), pointers(LogicalType::POINTER), count(0), sel_vector(STANDARD_VECTOR_SIZE),
      chain_match_sel_vector(STANDARD_VECTOR_SIZE), chain_no_match_sel_vector(STANDARD_VECTOR_SIZE),
      found_match(make_unsafe_uniq_array_uninitialized<bool>(STANDARD_VECTOR_SIZE)), ht(ht_p), finished(false),
      is_null(true), rhs_pointers(LogicalType::POINTER), lhs_sel_vector(STANDARD_VECTOR_SIZE), last_match_count(0),
      last_sel_vector(STANDARD_VECTOR_SIZE) {
}

void ScanStructure::Next(DataChunk &keys, DataChunk &left, DataChunk &result) {
	D_ASSERT(keys.size() == left.size());
	if (finished) {
		return;
	}
	switch (ht.join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
		NextInnerJoin(keys, left, result);
		break;
	case JoinType::SEMI:
		NextSemiJoin(keys, left, result);
		break;
	case JoinType::MARK:
		NextMarkJoin(keys, left, result);
		break;
	case JoinType::ANTI:
		NextAntiJoin(keys, left, result);
		break;
	case JoinType::RIGHT_ANTI:
	case JoinType::RIGHT_SEMI:
		NextRightSemiOrAntiJoin(keys);
		break;
	case JoinType::OUTER:
	case JoinType::LEFT:
		NextLeftJoin(keys, left, result);
		break;
	case JoinType::SINGLE:
		NextSingleJoin(keys, left, result);
		break;
	default:
		throw InternalException("Unhandled join type in JoinHashTable");
	}
}

bool ScanStructure::PointersExhausted() const {
	// AdvancePointers creates a "new_count" for every pointer advanced during the
	// previous advance pointers call. If no pointers are advanced, new_count = 0.
	// count is then set ot new_count.
	return count == 0;
}

idx_t ScanStructure::ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector *no_match_sel) {

	// Initialize the found_match array to the current sel_vector
	for (idx_t i = 0; i < this->count; ++i) {
		match_sel.set_index(i, this->sel_vector.get_index(i));
	}

	// If there is a matcher for the probing side because of non-equality predicates, use it
	if (ht.needs_chain_matcher) {
		idx_t no_match_count = 0;
		auto &matcher = no_match_sel ? ht.row_matcher_probe_no_match_sel : ht.row_matcher_probe;
		D_ASSERT(matcher);

		// we need to only use the vectors with the indices of the columns that are used in the probe phase, namely
		// the non-equality columns
		return matcher->Match(keys, key_state.vector_data, match_sel, this->count, *ht.layout_ptr, pointers,
		                      no_match_sel, no_match_count, ht.non_equality_predicate_columns);
	} else {
		// no match sel is the opposite of match sel
		return this->count;
	}
}

idx_t ScanStructure::ScanInnerJoin(DataChunk &keys, SelectionVector &result_vector) {
	while (true) {
		// resolve the equality_predicates for this set of keys
		idx_t result_count = ResolvePredicates(keys, result_vector, nullptr);

		// after doing all the comparisons set the found_match vector
		if (found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				auto idx = result_vector.get_index(i);
				found_match[idx] = true;
			}
		}
		if (result_count > 0) {
			return result_count;
		}
		// no matches found: check the next set of pointers
		AdvancePointers();
		if (this->count == 0) {
			return 0;
		}
	}
}

void ScanStructure::AdvancePointers(const SelectionVector &sel, const idx_t sel_count) {

	if (!ht.chains_longer_than_one) {
		this->count = 0;
		return;
	}

	// now for all the pointers, we move on to the next set of pointers
	idx_t new_count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(this->pointers);
	for (idx_t i = 0; i < sel_count; i++) {
		auto idx = sel.get_index(i);
		ptrs[idx] = LoadPointer(ptrs[idx] + ht.pointer_offset);
		if (ptrs[idx]) {
			this->sel_vector.set_index(new_count++, idx);
		}
	}
	this->count = new_count;
}

void ScanStructure::AdvancePointers() {
	AdvancePointers(this->sel_vector, this->count);
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &result_vector,
                                 const SelectionVector &sel_vector, const idx_t count, const idx_t col_no) {
	ht.data_collection->Gather(pointers, sel_vector, count, col_no, result, result_vector, nullptr);
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &sel_vector, const idx_t count,
                                 const idx_t col_idx) {
	GatherResult(result, *FlatVector::IncrementalSelectionVector(), sel_vector, count, col_idx);
}

void ScanStructure::GatherResult(Vector &result, const idx_t count, const idx_t col_idx) {
	ht.data_collection->Gather(rhs_pointers, *FlatVector::IncrementalSelectionVector(), count, col_idx, result,
	                           *FlatVector::IncrementalSelectionVector(), nullptr);
}

void ScanStructure::UpdateCompactionBuffer(idx_t base_count, SelectionVector &result_vector, idx_t result_count) {
	// matches were found
	// record the result
	// on the LHS, we store result vector
	for (idx_t i = 0; i < result_count; i++) {
		lhs_sel_vector.set_index(base_count + i, result_vector.get_index(i));
	}

	// on the RHS, we collect their pointers
	VectorOperations::Copy(pointers, rhs_pointers, result_vector, result_count, 0, base_count);
}

void ScanStructure::NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	if (ht.join_type != JoinType::RIGHT_SEMI && ht.join_type != JoinType::RIGHT_ANTI) {
		D_ASSERT(result.ColumnCount() == left.ColumnCount() + ht.output_columns.size());
	}

	idx_t base_count = 0;
	idx_t result_count;
	while (this->count > 0) {
		// if we have saved the match result, we need not call ScanInnerJoin again
		if (last_match_count == 0) {
			result_count = ScanInnerJoin(keys, chain_match_sel_vector);
		} else {
			chain_match_sel_vector.Initialize(last_sel_vector);
			result_count = last_match_count;
			last_match_count = 0;
		}

		if (result_count > 0) {
			// the result chunk cannot contain more data, we record the match result for future use
			if (base_count + result_count > STANDARD_VECTOR_SIZE) {
				last_sel_vector.Initialize(chain_match_sel_vector);
				last_match_count = result_count;
				break;
			}

			if (PropagatesBuildSide(ht.join_type)) {
				// full/right outer join: mark join matches as FOUND in the HT
				auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
				for (idx_t i = 0; i < result_count; i++) {
					auto idx = chain_match_sel_vector.get_index(i);
					// NOTE: threadsan reports this as a data race because this can be set concurrently by separate
					// threads Technically it is, but it does not matter, since the only value that can be written
					// is "true"
					Store<bool>(true, ptrs[idx] + ht.tuple_size);
				}
			}

			if (ht.join_type != JoinType::RIGHT_SEMI && ht.join_type != JoinType::RIGHT_ANTI) {
				// Fast Path: if there is NO more than one element in the chain, we construct the result chunk
				// directly
				if (!ht.chains_longer_than_one) {
					// matches were found
					// on the LHS, we create a slice using the result vector
					result.Slice(left, chain_match_sel_vector, result_count);

					// on the RHS, we need to fetch the data from the hash table
					for (idx_t i = 0; i < ht.output_columns.size(); i++) {
						auto &vector = result.data[left.ColumnCount() + i];
						const auto output_col_idx = ht.output_columns[i];
						D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[output_col_idx]);
						GatherResult(vector, chain_match_sel_vector, result_count, output_col_idx);
					}

					AdvancePointers();
					return;
				}

				// Common Path: use a buffer to store temporary data
				UpdateCompactionBuffer(base_count, chain_match_sel_vector, result_count);
				base_count += result_count;
			}
		}
		AdvancePointers();
	}

	if (base_count > 0) {
		// create result chunk, we have two steps:
		// 1) slice LHS vectors
		result.Slice(left, lhs_sel_vector, base_count);

		// 2) gather RHS vectors
		for (idx_t i = 0; i < ht.output_columns.size(); i++) {
			auto &vector = result.data[left.ColumnCount() + i];
			const auto output_col_idx = ht.output_columns[i];
			D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[output_col_idx]);
			GatherResult(vector, base_count, output_col_idx);
		}
	}
}

void ScanStructure::ScanKeyMatches(DataChunk &keys) {
	// the semi-join, anti-join and mark-join we handle a differently from the inner join
	// since there can be at most STANDARD_VECTOR_SIZE results
	// we handle the entire chunk in one call to Next().
	// for every pointer, we keep chasing pointers and doing comparisons.
	// this results in a boolean array indicating whether or not the tuple has a match
	// Start with the scan selection

	while (this->count > 0) {
		// resolve the equality_predicates for the current set of pointers
		idx_t match_count = ResolvePredicates(keys, chain_match_sel_vector, &chain_no_match_sel_vector);
		idx_t no_match_count = this->count - match_count;

		// mark each of the matches as found
		for (idx_t i = 0; i < match_count; i++) {
			found_match[chain_match_sel_vector.get_index(i)] = true;
		}
		// continue searching for the ones where we did not find a match yet
		AdvancePointers(chain_no_match_sel_vector, no_match_count);
	}
}

template <bool MATCH>
void ScanStructure::NextSemiOrAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	D_ASSERT(left.ColumnCount() == result.ColumnCount());
	// create the selection vector from the matches that were found
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	for (idx_t i = 0; i < keys.size(); i++) {
		if (found_match[i] == MATCH) {
			// part of the result
			sel.set_index(result_count++, i);
		}
	}
	// construct the final result
	if (result_count > 0) {
		// we only return the columns on the left side
		// reference the columns of the left side from the result
		result.Slice(left, sel, result_count);
	} else {
		D_ASSERT(result.size() == 0);
	}
}

void ScanStructure::NextSemiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// first scan for key matches
	ScanKeyMatches(keys);
	// then construct the result from all tuples with a match
	NextSemiOrAntiJoin<true>(keys, left, result);

	finished = true;
}

void ScanStructure::NextAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// first scan for key matches
	ScanKeyMatches(keys);
	// then construct the result from all tuples that did not find a match
	NextSemiOrAntiJoin<false>(keys, left, result);

	finished = true;
}

void ScanStructure::NextRightSemiOrAntiJoin(DataChunk &keys) {
	const auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
	while (!PointersExhausted()) {
		// resolve the equality_predicates for this set of keys
		idx_t result_count = ResolvePredicates(keys, chain_match_sel_vector, nullptr);

		// for each match, fully follow the chain
		for (idx_t i = 0; i < result_count; i++) {
			const auto idx = chain_match_sel_vector.get_index(i);
			auto &ptr = ptrs[idx];
			if (Load<bool>(ptr + ht.tuple_size)) { // Early out: chain has been fully marked as found before
				ptr = ht.dead_end.get();
				continue;
			}

			// Fully mark chain as found
			while (true) {
				// NOTE: threadsan reports this as a data race because this can be set concurrently by separate
				// threads Technically it is, but it does not matter, since the only value that can be written is
				// "true"
				Store<bool>(true, ptr + ht.tuple_size);
				auto next_ptr = LoadPointer(ptr + ht.pointer_offset);
				if (!next_ptr) {
					break;
				}
				ptr = next_ptr;
			}
		}

		// check the next set of pointers
		AdvancePointers();
	}

	finished = true;
}

void ScanStructure::ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &child, DataChunk &result) {
	// for the initial set of columns we just reference the left side
	result.SetCardinality(child);
	for (idx_t i = 0; i < child.ColumnCount(); i++) {
		result.data[i].Reference(child.data[i]);
	}
	auto &mark_vector = result.data.back();
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	auto bool_result = FlatVector::GetData<bool>(mark_vector);
	auto &mask = FlatVector::Validity(mark_vector);
	for (idx_t col_idx = 0; col_idx < join_keys.ColumnCount(); col_idx++) {
		if (ht.null_values_are_equal[col_idx]) {
			continue;
		}
		UnifiedVectorFormat jdata;
		join_keys.data[col_idx].ToUnifiedFormat(join_keys.size(), jdata);
		if (!jdata.validity.AllValid()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				if (!jdata.validity.RowIsValidUnsafe(jidx)) {
					mask.SetInvalid(i);
				}
			}
		}
	}
	// now set the remaining entries to either true or false based on whether a match was found
	D_ASSERT(found_match);
	for (idx_t i = 0; i < child.size(); i++) {
		bool_result[i] = found_match[i];
	}
	// if the right side contains NULL values, the result of any FALSE becomes NULL
	if (ht.has_null) {
		for (idx_t i = 0; i < child.size(); i++) {
			if (!bool_result[i]) {
				mask.SetInvalid(i);
			}
		}
	}
}

void ScanStructure::NextMarkJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	D_ASSERT(result.ColumnCount() == left.ColumnCount() + 1);
	D_ASSERT(result.data.back().GetType() == LogicalType::BOOLEAN);
	// this method should only be called for a non-empty HT
	D_ASSERT(ht.Count() > 0);

	ScanKeyMatches(keys);
	if (ht.correlated_mark_join_info.correlated_types.empty()) {
		ConstructMarkJoinResult(keys, left, result);
	} else {
		auto &info = ht.correlated_mark_join_info;
		lock_guard<mutex> mj_lock(info.mj_lock);

		// there are correlated columns
		// first we fetch the counts from the aggregate hashtable corresponding to these entries
		D_ASSERT(keys.ColumnCount() == info.group_chunk.ColumnCount() + 1);
		info.group_chunk.SetCardinality(keys);
		for (idx_t i = 0; i < info.group_chunk.ColumnCount(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		info.correlated_counts->FetchAggregates(info.group_chunk, info.result_chunk);

		// for the initial set of columns we just reference the left side
		result.SetCardinality(left);
		for (idx_t i = 0; i < left.ColumnCount(); i++) {
			result.data[i].Reference(left.data[i]);
		}
		// create the result matching vector
		auto &last_key = keys.data.back();
		auto &result_vector = result.data.back();
		// first set the nullmask based on whether or not there were NULL values in the join key
		result_vector.SetVectorType(VectorType::FLAT_VECTOR);
		auto bool_result = FlatVector::GetData<bool>(result_vector);
		auto &mask = FlatVector::Validity(result_vector);
		switch (last_key.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR:
			if (ConstantVector::IsNull(last_key)) {
				mask.SetAllInvalid(left.size());
			}
			break;
		case VectorType::FLAT_VECTOR:
			mask.Copy(FlatVector::Validity(last_key), left.size());
			break;
		default: {
			UnifiedVectorFormat kdata;
			last_key.ToUnifiedFormat(keys.size(), kdata);
			for (idx_t i = 0; i < left.size(); i++) {
				auto kidx = kdata.sel->get_index(i);
				mask.Set(i, kdata.validity.RowIsValid(kidx));
			}
			break;
		}
		}

		auto count_star = FlatVector::GetData<int64_t>(info.result_chunk.data[0]);
		auto count = FlatVector::GetData<int64_t>(info.result_chunk.data[1]);
		// set the entries to either true or false based on whether a match was found
		for (idx_t i = 0; i < left.size(); i++) {
			D_ASSERT(count_star[i] >= count[i]);
			bool_result[i] = found_match ? found_match[i] : false;
			if (!bool_result[i] && count_star[i] > count[i]) {
				// RHS has NULL value and result is false: set to null
				mask.SetInvalid(i);
			}
			if (count_star[i] == 0) {
				// count == 0, set nullmask to false (we know the result is false now)
				mask.SetValid(i);
			}
		}
	}
	finished = true;
}

void ScanStructure::NextLeftJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// a LEFT OUTER JOIN is identical to an INNER JOIN except all tuples that do
	// not have a match must return at least one tuple (with the right side set
	// to NULL in every column)
	NextInnerJoin(keys, left, result);
	if (result.size() == 0) {
		// no entries left from the normal join
		// fill in the result of the remaining left tuples
		// together with NULL values on the right-hand side
		idx_t remaining_count = 0;
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < left.size(); i++) {
			if (!found_match[i]) {
				sel.set_index(remaining_count++, i);
			}
		}
		if (remaining_count > 0) {
			// have remaining tuples
			// slice the left side with tuples that did not find a match
			result.Slice(left, sel, remaining_count);

			// now set the right side to NULL
			for (idx_t i = left.ColumnCount(); i < result.ColumnCount(); i++) {
				Vector &vec = result.data[i];
				vec.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(vec, true);
			}
		}
		finished = true;
	}
}

void ScanStructure::NextSingleJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// single join
	// this join is similar to the semi join except that
	// (1) we actually return data from the RHS and
	// (2) we return NULL for that data if there is no match
	// (3) if single_join_error_on_multiple_rows is set, we need to keep looking for duplicates after fetching
	idx_t result_count = 0;
	SelectionVector result_sel(STANDARD_VECTOR_SIZE);

	while (this->count > 0) {
		// resolve the equality_predicates for the current set of pointers
		idx_t match_count = ResolvePredicates(keys, chain_match_sel_vector, &chain_no_match_sel_vector);
		idx_t no_match_count = this->count - match_count;

		// mark each of the matches as found
		for (idx_t i = 0; i < match_count; i++) {
			// found a match for this index
			auto index = chain_match_sel_vector.get_index(i);
			found_match[index] = true;
			result_sel.set_index(result_count++, index);
		}
		// continue searching for the ones where we did not find a match yet
		AdvancePointers(chain_no_match_sel_vector, no_match_count);
	}
	// reference the columns of the left side from the result
	D_ASSERT(left.ColumnCount() > 0);
	for (idx_t i = 0; i < left.ColumnCount(); i++) {
		result.data[i].Reference(left.data[i]);
	}
	// now fetch the data from the RHS
	for (idx_t i = 0; i < ht.output_columns.size(); i++) {
		auto &vector = result.data[left.ColumnCount() + i];
		// set NULL entries for every entry that was not found
		for (idx_t j = 0; j < left.size(); j++) {
			if (!found_match[j]) {
				FlatVector::SetNull(vector, j, true);
			}
		}
		const auto output_col_idx = ht.output_columns[i];
		D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[output_col_idx]);
		GatherResult(vector, result_sel, result_sel, result_count, output_col_idx);
	}
	result.SetCardinality(left.size());

	// like the SEMI, ANTI and MARK join types, the SINGLE join only ever does one pass over the HT per input chunk
	finished = true;

	if (ht.single_join_error_on_multiple_rows && result_count > 0) {
		// we need to throw an error if there are multiple rows per key
		// advance pointers for those rows
		AdvancePointers(result_sel, result_count);

		// now resolve the predicates
		idx_t match_count = ResolvePredicates(keys, chain_match_sel_vector, nullptr);
		if (match_count > 0) {
			// we found at least one duplicate row - throw
			throw InvalidInputException(
			    "More than one row returned by a subquery used as an expression - scalar subqueries can only "
			    "return a single row.\n\nUse \"SET scalar_subquery_error_on_multiple_rows=false\" to revert to "
			    "previous behavior of returning a random row.");
		}

		this->count = 0;
	}
}

void JoinHashTable::ScanFullOuter(JoinHTScanState &state, Vector &addresses, DataChunk &result) const {
	// scan the HT starting from the current position and check which rows from the build side did not find a match
	auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
	idx_t found_entries = 0;

	auto &iterator = state.iterator;
	if (iterator.Done()) {
		return;
	}

	// When scanning Full Outer for right semi joins, we only propagate matches that have true
	// Right Semi Joins do not propagate values during the probe phase, since we do not want to
	// duplicate RHS rows.
	bool match_propagation_value = false;
	if (join_type == JoinType::RIGHT_SEMI) {
		match_propagation_value = true;
	}

	const auto row_locations = iterator.GetRowLocations();
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = state.offset_in_chunk; i < count; i++) {
			auto found_match = Load<bool>(row_locations[i] + tuple_size);
			if (found_match == match_propagation_value) {
				key_locations[found_entries++] = row_locations[i];
				if (found_entries == STANDARD_VECTOR_SIZE) {
					state.offset_in_chunk = i + 1;
					break;
				}
			}
		}
		if (found_entries == STANDARD_VECTOR_SIZE) {
			break;
		}
		state.offset_in_chunk = 0;
	} while (iterator.Next());

	// now gather from the found rows
	if (found_entries == 0) {
		return;
	}
	result.SetCardinality(found_entries);

	idx_t left_column_count = result.ColumnCount() - output_columns.size();
	if (join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI) {
		left_column_count = 0;
	}
	const auto &sel_vector = *FlatVector::IncrementalSelectionVector();
	// set the left side as a constant NULL
	for (idx_t i = 0; i < left_column_count; i++) {
		Vector &vec = result.data[i];
		vec.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(vec, true);
	}

	// gather the values from the RHS
	for (idx_t i = 0; i < output_columns.size(); i++) {
		auto &vector = result.data[left_column_count + i];
		const auto output_col_idx = output_columns[i];
		D_ASSERT(vector.GetType() == layout_ptr->GetTypes()[output_col_idx]);
		data_collection->Gather(addresses, sel_vector, found_entries, output_col_idx, vector, sel_vector, nullptr);
	}
}

idx_t JoinHashTable::FillWithHTOffsets(JoinHTScanState &state, Vector &addresses) {
	// iterate over HT
	auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
	idx_t key_count = 0;

	auto &iterator = state.iterator;
	const auto row_locations = iterator.GetRowLocations();
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = 0; i < count; i++) {
			key_locations[key_count + i] = row_locations[i];
		}
		key_count += count;
	} while (iterator.Next());

	return key_count;
}

idx_t JoinHashTable::GetTotalSize(const vector<idx_t> &partition_sizes, const vector<idx_t> &partition_counts,
                                  idx_t &max_partition_size, idx_t &max_partition_count) const {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);

	idx_t total_size = 0;
	idx_t total_count = 0;
	idx_t max_partition_ht_size = 0;
	max_partition_size = 0;
	max_partition_count = 0;
	for (idx_t i = 0; i < num_partitions; i++) {
		total_size += partition_sizes[i];
		total_count += partition_counts[i];

		auto partition_size = partition_sizes[i] + PointerTableSize(partition_counts[i]);
		if (partition_size > max_partition_ht_size) {
			max_partition_ht_size = partition_size;
			max_partition_size = partition_sizes[i];
			max_partition_count = partition_counts[i];
		}
	}

	if (total_count == 0) {
		return 0;
	}

	return total_size + PointerTableSize(total_count);
}

idx_t JoinHashTable::GetTotalSize(const vector<unique_ptr<JoinHashTable>> &local_hts, idx_t &max_partition_size,
                                  idx_t &max_partition_count) const {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	vector<idx_t> partition_sizes(num_partitions, 0);
	vector<idx_t> partition_counts(num_partitions, 0);
	for (auto &ht : local_hts) {
		ht->GetSinkCollection().GetSizesAndCounts(partition_sizes, partition_counts);
	}

	return GetTotalSize(partition_sizes, partition_counts, max_partition_size, max_partition_count);
}

idx_t JoinHashTable::GetRemainingSize() const {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	auto &partitions = sink_collection->GetPartitions();

	idx_t count = 0;
	idx_t data_size = 0;
	for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
		if (completed_partitions.RowIsValidUnsafe(partition_idx)) {
			continue;
		}
		count += partitions[partition_idx]->Count();
		data_size += partitions[partition_idx]->SizeInBytes();
	}

	return data_size + PointerTableSize(count);
}

void JoinHashTable::Unpartition() {
	data_collection = sink_collection->GetUnpartitioned(); // Key move from sink_collection to data_collection
}

void JoinHashTable::SetRepartitionRadixBits(const idx_t max_ht_size, const idx_t max_partition_size,
                                            const idx_t max_partition_count) {
	D_ASSERT(max_partition_size + PointerTableSize(max_partition_count) > max_ht_size);

	const auto max_added_bits = RadixPartitioning::MAX_RADIX_BITS - radix_bits;
	idx_t added_bits = 1;
	for (; added_bits < max_added_bits; added_bits++) {
		double partition_multiplier = static_cast<double>(RadixPartitioning::NumberOfPartitions(added_bits));

		auto new_estimated_size = static_cast<double>(max_partition_size) / partition_multiplier;
		auto new_estimated_count = static_cast<double>(max_partition_count) / partition_multiplier;
		auto new_estimated_ht_size =
		    new_estimated_size + static_cast<double>(PointerTableSize(LossyNumericCast<idx_t>(new_estimated_count)));

		if (new_estimated_ht_size <= static_cast<double>(max_ht_size) / 4) {
			// Aim for an estimated partition size of max_ht_size / 4
			break;
		}
	}
	radix_bits += added_bits;
	sink_collection =
	    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout_ptr, radix_bits, layout_ptr->ColumnCount() - 1);

	// Need to initialize again after changing the number of bits
	InitializePartitionMasks();
}

void JoinHashTable::InitializePartitionMasks() {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);

	current_partitions.Initialize(num_partitions);
	current_partitions.SetAllInvalid(num_partitions);

	completed_partitions.Initialize(num_partitions);
	completed_partitions.SetAllInvalid(num_partitions);
}

idx_t JoinHashTable::CurrentPartitionCount() const {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	D_ASSERT(current_partitions.Capacity() == num_partitions);
	return current_partitions.CountValid(num_partitions);
}

idx_t JoinHashTable::FinishedPartitionCount() const {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	D_ASSERT(completed_partitions.Capacity() == num_partitions);
	// We already marked the active partitions as done, so we have to subtract them here
	return completed_partitions.CountValid(num_partitions) - CurrentPartitionCount();
}

void JoinHashTable::Repartition(JoinHashTable &global_ht) {
	auto new_sink_collection = make_uniq<RadixPartitionedTupleData>(buffer_manager, layout_ptr, global_ht.radix_bits,
	                                                                layout_ptr->ColumnCount() - 1);
	sink_collection->Repartition(context, *new_sink_collection);
	sink_collection = std::move(new_sink_collection);
	global_ht.Merge(*this);
}

void JoinHashTable::Reset() {
	data_collection->Reset();
	hash_map.Reset();
	current_partitions.SetAllInvalid(RadixPartitioning::NumberOfPartitions(radix_bits));
	finalized = false;
}

bool JoinHashTable::PrepareExternalFinalize(const idx_t max_ht_size) {
	if (finalized) {
		Reset();
	}

	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	D_ASSERT(current_partitions.Capacity() == num_partitions);
	D_ASSERT(completed_partitions.Capacity() == num_partitions);
	D_ASSERT(current_partitions.CheckAllInvalid(num_partitions));

	if (completed_partitions.CheckAllValid(num_partitions)) {
		return false; // All partitions are done
	}

	// Create vector with unfinished partition indices
	auto &partitions = sink_collection->GetPartitions();
	auto min_partition_size = NumericLimits<idx_t>::Maximum();
	vector<idx_t> partition_indices;
	partition_indices.reserve(num_partitions);
	for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
		if (completed_partitions.RowIsValidUnsafe(partition_idx)) {
			continue;
		}
		partition_indices.push_back(partition_idx);
		// Keep track of min partition size
		const auto size =
		    partitions[partition_idx]->SizeInBytes() + PointerTableSize(partitions[partition_idx]->Count());
		min_partition_size = MinValue(min_partition_size, size);
	}

	// Sort partitions by size, from small to large
	std::stable_sort(partition_indices.begin(), partition_indices.end(), [&](const idx_t &lhs, const idx_t &rhs) {
		const auto lhs_size = partitions[lhs]->SizeInBytes() + PointerTableSize(partitions[lhs]->Count());
		const auto rhs_size = partitions[rhs]->SizeInBytes() + PointerTableSize(partitions[rhs]->Count());
		// We divide by min_partition_size, effectively rouding everything down to a multiple of min_partition_size
		// Makes it so minor differences in partition sizes don't mess up the original order
		// Retaining as much of the original order as possible reduces I/O (partition idx determines eviction queue
		// idx)
		return lhs_size / min_partition_size < rhs_size / min_partition_size;
	});

	// Determine which partitions should go next
	idx_t count = 0;
	idx_t data_size = 0;
	for (const auto &partition_idx : partition_indices) {
		D_ASSERT(!completed_partitions.RowIsValidUnsafe(partition_idx));
		const auto incl_count = count + partitions[partition_idx]->Count();
		const auto incl_data_size = data_size + partitions[partition_idx]->SizeInBytes();
		const auto incl_ht_size = incl_data_size + PointerTableSize(incl_count);
		if (count > 0 && incl_ht_size > max_ht_size) {
			break; // Always add at least one partition
		}
		count = incl_count;
		data_size = incl_data_size;
		current_partitions.SetValidUnsafe(partition_idx);     // Mark as currently active
		data_collection->Combine(*partitions[partition_idx]); // Move partition to the main data collection
		completed_partitions.SetValidUnsafe(partition_idx);   // Also already mark as done
	}
	D_ASSERT(Count() == count);

	return true;
}

void JoinHashTable::ProbeAndSpill(ScanStructure &scan_structure, DataChunk &probe_keys, TupleDataChunkState &key_state,
                                  ProbeState &probe_state, DataChunk &probe_chunk, ProbeSpill &probe_spill,
                                  ProbeSpillLocalAppendState &spill_state, DataChunk &spill_chunk) {
	// hash all the keys
	Vector hashes(LogicalType::HASH);
	Hash(probe_keys, *FlatVector::IncrementalSelectionVector(), probe_keys.size(), hashes);

	// find out which keys we can match with the current pinned partitions
	SelectionVector true_sel(STANDARD_VECTOR_SIZE);
	SelectionVector false_sel(STANDARD_VECTOR_SIZE);
	const auto true_count =
	    RadixPartitioning::Select(hashes, FlatVector::IncrementalSelectionVector(), probe_keys.size(), radix_bits,
	                              current_partitions, &true_sel, &false_sel);
	const auto false_count = probe_keys.size() - true_count;

	// can't probe these values right now, append to spill
	spill_chunk.Reset();
	spill_chunk.Reference(probe_chunk);
	spill_chunk.data.back().Reference(hashes);
	spill_chunk.Slice(false_sel, false_count);
	probe_spill.Append(spill_chunk, spill_state);

	// slice the stuff we CAN probe right now
	hashes.Slice(true_sel, true_count);
	probe_keys.Slice(true_sel, true_count);
	probe_chunk.Slice(true_sel, true_count);

	const SelectionVector *current_sel;
	InitializeScanStructure(scan_structure, probe_keys, key_state, current_sel);
	if (scan_structure.count == 0) {
		return;
	}

	// now initialize the pointers of the scan structure based on the hashes
	GetRowPointers(probe_keys, key_state, probe_state, hashes, current_sel, scan_structure.count,
	               scan_structure.pointers, scan_structure.sel_vector, scan_structure.has_null_value_filter);
}

ProbeSpill::ProbeSpill(JoinHashTable &ht, ClientContext &context, const vector<LogicalType> &probe_types)
    : ht(ht), context(context), probe_types(probe_types) {
	global_partitions =
	    make_uniq<RadixPartitionedColumnData>(context, probe_types, ht.radix_bits, probe_types.size() - 1);
	column_ids.reserve(probe_types.size());
	for (column_t column_id = 0; column_id < probe_types.size(); column_id++) {
		column_ids.emplace_back(column_id);
	}
}

ProbeSpillLocalState ProbeSpill::RegisterThread() {
	ProbeSpillLocalAppendState result;
	lock_guard<mutex> guard(lock);
	local_partitions.emplace_back(global_partitions->CreateShared());
	local_partition_append_states.emplace_back(make_uniq<PartitionedColumnDataAppendState>());
	local_partitions.back()->InitializeAppendState(*local_partition_append_states.back());

	result.local_partition = local_partitions.back().get();
	result.local_partition_append_state = local_partition_append_states.back().get();
	return result;
}

void ProbeSpill::Append(DataChunk &chunk, ProbeSpillLocalAppendState &local_state) {
	local_state.local_partition->Append(*local_state.local_partition_append_state, chunk);
}

void ProbeSpill::Finalize() {
	D_ASSERT(local_partitions.size() == local_partition_append_states.size());
	for (idx_t i = 0; i < local_partition_append_states.size(); i++) {
		local_partitions[i]->FlushAppendState(*local_partition_append_states[i]);
	}
	for (auto &local_partition : local_partitions) {
		global_partitions->Combine(*local_partition);
	}
	local_partitions.clear();
	local_partition_append_states.clear();
}

void ProbeSpill::PrepareNextProbe() {
	global_spill_collection.reset();
	auto &partitions = global_partitions->GetPartitions();
	if (partitions.empty() || ht.current_partitions.CheckAllInvalid(partitions.size())) {
		// Can't probe, just make an empty one
		global_spill_collection =
		    make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), probe_types);
	} else {
		// Move current partitions to the global spill collection
		for (idx_t partition_idx = 0; partition_idx < partitions.size(); partition_idx++) {
			if (!ht.current_partitions.RowIsValidUnsafe(partition_idx)) {
				continue;
			}
			auto &partition = partitions[partition_idx];
			if (!global_spill_collection) {
				global_spill_collection = std::move(partition);
			} else if (partition->Count() != 0) {
				global_spill_collection->Combine(*partition);
			}
			partition.reset();
		}
	}
	consumer = make_uniq<ColumnDataConsumer>(*global_spill_collection, column_ids);
	consumer->InitializeScan();
}

} // namespace duckdb
