//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/join_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_consumer.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/tiered_hash_cache.hpp"
#include "duckdb/execution/ht_entry.hpp"

#include <atomic>
#include <array>
#include <mutex>

namespace duckdb {

class BufferManager;
class BufferHandle;
class ColumnDataCollection;
struct ColumnDataAppendState;
struct ClientConfig;

struct JoinHTScanState {
public:
	JoinHTScanState(TupleDataCollection &collection, idx_t chunk_idx_from, idx_t chunk_idx_to,
	                TupleDataPinProperties properties = TupleDataPinProperties::ALREADY_PINNED)
	    : iterator(collection, properties, chunk_idx_from, chunk_idx_to, false), offset_in_chunk(0) {
	}

	TupleDataChunkIterator iterator;
	idx_t offset_in_chunk;

private:
	//! Implicit copying is not allowed
	JoinHTScanState(const JoinHTScanState &) = delete;
};

//! JoinHashTable is a linear probing HT that is used for computing joins
/*!
   The JoinHashTable concatenates incoming chunks inside a linked list of
   data ptrs. The storage looks like this internally.
   [SERIALIZED ROW][NEXT POINTER]
   [SERIALIZED ROW][NEXT POINTER]
   There is a separate hash map of pointers that point into this table.
   This is what is used to resolve the hashes.
   [POINTER]
   [POINTER]
   [POINTER]
   The pointers are either NULL
*/
class JoinHashTable {
public:
	using ValidityBytes = TemplatedValidityMask<uint8_t>;

	//! only compare salts with the ht entries if the capacity is larger than 8192 so
	//! that it does not fit into the CPU cache
	static constexpr const idx_t USE_SALT_THRESHOLD = 8192;

	//! Scan structure that can be used to resume scans, as a single probe can
	//! return 1024*N values (where N is the size of the HT). This is
	//! returned by the JoinHashTable::Scan function and can be used to resume a
	//! probe.
	struct ScanStructure {
		TupleDataChunkState &key_state;
		//! Directly point to the entry in the hash table
		Vector pointers;
		idx_t count;
		SelectionVector sel_vector;
		SelectionVector chain_match_sel_vector;
		SelectionVector chain_no_match_sel_vector;

		// whether or not the given tuple has found a match
		unsafe_unique_array<bool> found_match;
		JoinHashTable &ht;
		bool finished;
		bool is_null;
		bool has_null_value_filter = false;

		// it records the RHS pointers for the result chunk
		Vector rhs_pointers;
		// it records the LHS sel vector for the result chunk
		SelectionVector lhs_sel_vector;
		// these two variable records the last match results
		idx_t last_match_count;
		SelectionVector last_sel_vector;

		explicit ScanStructure(JoinHashTable &ht, TupleDataChunkState &key_state);
		//! Get the next batch of data from the scan structure
		void Next(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Are pointer chains all pointing to NULL?
		bool PointersExhausted() const;

	private:
		//! Next operator for the inner join
		void NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the semi join
		void NextSemiJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the anti join
		void NextAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the RIGHT semi and anti join
		void NextRightSemiOrAntiJoin(DataChunk &keys);
		//! Next operator for the left outer join
		void NextLeftJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the mark join
		void NextMarkJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the single join
		void NextSingleJoin(DataChunk &keys, DataChunk &left, DataChunk &result);

		//! Scan the hashtable for matches of the specified keys, setting the found_match[] array to true or false
		//! for every tuple
		void ScanKeyMatches(DataChunk &keys);
		template <bool MATCH>
		void NextSemiOrAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result);

		void ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &child, DataChunk &result);

		idx_t ScanInnerJoin(DataChunk &keys, SelectionVector &result_vector);

		//! Update the data chunk compaction buffer
		void UpdateCompactionBuffer(idx_t base_count, SelectionVector &result_vector, idx_t result_count);

	public:
		void AdvancePointers();
		void AdvancePointers(const SelectionVector &sel, idx_t sel_count);
		void GatherResult(Vector &result, const SelectionVector &result_vector, const SelectionVector &sel_vector,
		                  const idx_t count, const idx_t col_idx);
		void GatherResult(Vector &result, const SelectionVector &sel_vector, const idx_t count, const idx_t col_idx);
		void GatherResult(Vector &result, const idx_t count, const idx_t col_idx);
		idx_t ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector *no_match_sel);
	};

public:
	struct SharedState {
		SharedState();

		Vector salt_v;

		SelectionVector keys_to_compare_sel;
		SelectionVector keys_no_match_sel;
	};

	//! The three phases of the adaptive THC lifecycle.
	//! BASELINE: probe the main HT only (no THC) for p probes; measures C_main.
	//! COLLECT: probe the regular HT (or THC + regular HT fallback on cycles > 0),
	//!         and collect matched entries into collected_entries for later THC insertion.
	//! READ_ONLY: probe the THC first, fall back to regular HT for misses.
	//!            At the end of each evaluation phase, the cost-based decision rule
	//!            decides whether to drop, freeze, or continue building the THC.
	enum class TieredHashCachePhase : uint8_t { BASELINE, COLLECT, READ_ONLY };

	//! Reason the THC was abandoned by a thread (thc_abandoned = true).
	//! Reported in the profiling JSON / benchmark CSVs as a short label.
	enum class THCAbandonReason : uint8_t {
		None = 0,
		LowCrossMultiplicity,    // first-cycle mu_{S->R} below thc_min_estimated_mu_s_to_r
		HighHotness,        // first-cycle estimated_perc_hot above thc_max_estimated_perc_hot
		THCTooSmallForBuildSide,   // THC capacity insufficient to cover thc_min_coverage_of_build_side hot entries
		HighMissRate,       // miss rate stayed > thc_miss_above_which_abandon for thc_abandon_consecutive_misses checkpoints
		THCIncreasesProbeCost            // cost-based decision rule: delta_t >= 0 (THC made probes slower)
	};

	//! Reason the THC stopped growing (thc_collection_enabled = false) without
	//! being abandoned. Reported in the profiling JSON / benchmark CSVs as a
	//! short label.
	enum class THCFreezeReason : uint8_t {
		None = 0,
		THCFull,            // THC reached thc_max_load_factor during a COLLECT flush
		MarginalGainNotWorthCollectionCost          // cost-based decision rule: shrinkage < gamma_t (growth no longer pays for itself)
	};

	//! Short, stable token for an abandon reason, used in profiling output.
	static const char *THCAbandonReasonLabel(THCAbandonReason r) {
		switch (r) {
		case THCAbandonReason::LowCrossMultiplicity:
			return "Low-Cross-Multiplicity";
		case THCAbandonReason::HighHotness:
			return "High-Hotness";
		case THCAbandonReason::THCTooSmallForBuildSide:
			return "Too-Small-For-Build";
		case THCAbandonReason::HighMissRate:
			return "High-Miss-Rate";
		case THCAbandonReason::THCIncreasesProbeCost:
			return "THC-Increases-Probe-Cost";
		case THCAbandonReason::None:
			return "";
		}
		return "";
	}

	//! Short, stable token for a freeze reason, used in profiling output.
	static const char *THCFreezeReasonLabel(THCFreezeReason r) {
		switch (r) {
		case THCFreezeReason::THCFull:
			return "THC-Full";
		case THCFreezeReason::MarginalGainNotWorthCollectionCost:
			return "Marginal-Gain-Not-Worth-Collection-Cost";
		case THCFreezeReason::None:
			return "";
		}
		return "";
	}

// TODO most items below should be config params. Some might already be duplicates of config params

	struct CollectedEntry {
		hash_t hash;
		const_data_ptr_t row_ptr;
	};

	//! Per-thread state for a single probing thread. After the THC adaptive
	//! algorithm was globalised across threads, this struct only holds:
	//!   (a) Per-call scratch space (vectors, selection vectors) that the
	//!       per-call probing/matching paths read and write.
	//!   (b) Per-thread timer pointers wired to the operator state's local
	//!       timing counters (the operator state then folds those into the
	//!       PhysicalHashJoin sink atomics at FlushLocalTimings time).
	//!   (c) The thread's local COLLECT buffer (`collected_entries`), drained
	//!       via `TieredHashCache::InsertBatch<...>` when this thread next
	//!       observes a COLLECT→READ_ONLY transition.
	//!   (d) The thread's last-observed global phase (`observed_phase`) and
	//!       the cycle index it captured on entry to COLLECT
	//!       (`collecting_for_cycle`). These exist so each thread knows
	//!       whether it has work to flush and which `first_collect_new_entries`
	//!       bucket its first-cycle flush contributes to.
	//!
	//! All decision-affecting fields (phase counters, cost doubles, miss
	//! rates, terminal flags, transition reasons, etc.) live on
	//! `JoinHashTable::GlobalTHCAdaptiveState` and are shared by every
	//! probing thread of this join.
	struct ProbeState : SharedState {
		explicit ProbeState(idx_t collected_entries_capacity = 0);

		Vector ht_offsets_v;
		Vector hashes_dense_v;
		SelectionVector non_empty_sel;
		uint64_t *probe_for_pointers_time_ns = nullptr;
		uint64_t *match_time_ns = nullptr;
		uint64_t *thc_probe_time_ns = nullptr;
		uint64_t *thc_collect_time_ns = nullptr;
		uint64_t *thc_insert_time_ns = nullptr;

		//! Per-thread vectors for THC probing
		Vector cache_rhs_row_locations;
		Vector cache_result_pointers;
		SelectionVector cache_candidates_sel;
		SelectionVector cache_miss_sel;

		//! Buffer of {hash, row_ptr} pairs collected during the current COLLECT
		//! cycle. Flushed into the shared THC when this thread next observes a
		//! COLLECT→READ_ONLY phase change (or, for the thread that triggers the
		//! transition, immediately after exiting the transition critical section).
		vector<CollectedEntry> collected_entries;

		//! --- Scratch space for collecting THC-miss matches during collect phase (cycle > 0) ---
		//! After ProbeTHCAndFallback runs, these record which miss-fallback rows actually
		//! found a match in the regular HT, so we can insert them into the THC.

		//! Selection vector of row indices that were THC misses but matched in regular HT
		//! It's used to note we need to fall back to regular probing for which rows within
		//! the chunk.
		//! Moreover, during the COLLECT phase, we iterate over those and create corresponding
		//! collected_entries.
		SelectionVector thc_miss_match_sel = SelectionVector(STANDARD_VECTOR_SIZE);

		//! Count of entries in thc_miss_match_sel
		idx_t thc_miss_match_count = 0;

		//! Last phase TYPE this thread observed via the global state's
		//! `phase` atomic. When `live_phase != observed_phase` we know
		//! the global phase has changed since our previous call and we
		//! must run the per-call phase-change actions (flush any pending
		//! `collected_entries` to `phase_metrics[observed_phase_number]`,
		//! update both observed_phase and observed_phase_number).
		TieredHashCachePhase observed_phase = TieredHashCachePhase::BASELINE;
		//! Last phase INDEX (the row `n` in the cycle-clean accumulator
		//! table) this thread observed. Captured at chunk entry alongside
		//! `observed_phase`. Every post-call `fetch_add` from this chunk
		//! writes to `phase_metrics[observed_phase_number % MAX_PHASES]`,
		//! so contributions are always tagged with the phase the chunk
		//! was started in — even if the global phase has since advanced.
		idx_t observed_phase_number = 0;
	};

	//! Per-phase accumulator slot. Each (global, monotonic) phase index has
	//! its own slot at `phase_metrics[phase_number % MAX_PHASES]`. Threads
	//! write only to the slot tagged with the phase they captured at chunk
	//! entry; the leader reads slots for the just-ended phase (and one or
	//! two prior phases) when computing cost-rule metrics. Because each
	//! phase owns exactly one slot, late `fetch_add`s from in-flight
	//! chunks can never contaminate a future phase's metrics.
	struct PhaseMetrics {
		std::atomic<uint64_t> time_ns {0};       // wall-clock time accumulated during this phase
		std::atomic<idx_t>    probe_count {0};   // input rows processed in this phase
		std::atomic<idx_t>    miss_count {0};    // THC misses (only meaningful for RO phases)
	};

	//! Cross-thread coordination state for the adaptive THC. One instance
	//! lives on `JoinHashTable` while the THC is active; every probing thread
	//! reads and updates it. Decision logic that needs to fire exactly once
	//! per phase transition runs under `transition_mutex` using
	//! double-checked locking on the relevant atomic counter. Cycle-counter
	//! increments and the `c_main` double are written only while holding
	//! the mutex; everything else is plain lock-free fetch_add / load /
	//! store traffic.
	struct GlobalTHCAdaptiveState {
		// ---- Phase / lifecycle ----
		//! Current global phase. All threads switch to this phase the next
		//! time they make a probe call.
		std::atomic<TieredHashCachePhase> phase {TieredHashCachePhase::BASELINE};
		//! When false, no thread should add to `collected_entries` or run a
		//! flush; the THC is frozen (still useful for reads) or abandoned.
		std::atomic<bool> collection_enabled {true};
		//! When true, every probing thread short-circuits to the vanilla
		//! probe path (no THC machinery at all).
		std::atomic<bool> abandoned {false};
		//! Set true by whichever thread runs the one-shot first-cycle
		//! multiplicity / hotness / coverage check inside the transition
		//! mutex. Ensures the check fires exactly once globally.
		std::atomic<bool> first_cycle_multiplicity_checked {false};

		// ---- Per-phase indexed accumulators ----
		//! Maximum number of phase slots; slot for phase k is
		//! `phase_metrics[k % MAX_PHASES]`. `phase_number` is monotonic
		//! (never reset). When the leader increments `phase_number` to a
		//! non-zero multiple of MAX_PHASES, it clears the entire array
		//! inside `transition_mutex` so the next round of phases starts
		//! in fresh slots. The clear is race-free because any in-flight
		//! chunk's `observed_phase_number` is at most one phase behind
		//! `phase_number` (a chunk takes microseconds while a phase spans
		//! tens of milliseconds), so its modulo index can never collide
		//! with the slot being reused 4096 phases later.
		static constexpr idx_t MAX_PHASES = 4096;
		std::array<PhaseMetrics, MAX_PHASES> phase_metrics {};
		//! Strictly monotonic counter — the "row index" `n` of the user's
		//! n×m mental model. Each transition increments this by one.
		std::atomic<idx_t> phase_number {0};
		//! Phase index of the most recent COLLECT phase. Set at every
		//! BASELINE→COLLECT and RO→COLLECT transition (the moment a new
		//! COLLECT begins). Read at the next RO checkpoint to compute
		//! c_grow_current. Written only inside `transition_mutex`.
		idx_t current_collect_phase_number = 0;
		//! Phase index of the most recently evaluated RO segment. Set at
		//! every RO checkpoint while the shrinkage check is enabled, after
		//! computing the cycle's metrics. Read at the NEXT RO checkpoint to
		//! compute c_eval_prev for the shrinkage formula. Written only
		//! inside `transition_mutex`.
		idx_t prev_eval_phase_number = 0;

		// ---- Lifetime counters (never reset; just keep growing) ----
		//! Lifetime probe rows; the budget-fraction guard divides this.
		std::atomic<idx_t> total_probe_rows {0};
		//! Lifetime rows spent in COLLECT (sum across cycles).
		std::atomic<idx_t> total_collect_phase_rows {0};
		//! Lifetime count of THC inserts across all flushes.
		std::atomic<idx_t> total_new_entries {0};
		//! Count of unique keys inserted during the FIRST COLLECT phase.
		//! Used as U1 in the cross-multiplicity estimator. Threads
		//! contribute only when `state.observed_phase_number == 1`.
		std::atomic<idx_t> first_collect_new_entries {0};

		// ---- mu_s estimation (probe-sample method, cycle-0 only) ----
		std::atomic<idx_t> mu_s_chain_length_sum {0};
		std::atomic<idx_t> mu_s_chain_count {0};

		// ---- Cycle counters ----
		//! Number of COLLECT phases that have fully transitioned to READ_ONLY.
		//! 0 → we are in BASELINE or the very first COLLECT.
		//! 1 → first COLLECT just finished; the one-shot multiplicity check
		//!     becomes due at the next READ_ONLY checkpoint.
		std::atomic<idx_t> completed_collect_cycles {0};
		//! Number of READ_ONLY checkpoints that have evaluated the decision
		//! body (advances at every checkpoint after the high-miss
		//! abandonment short-circuit).
		std::atomic<idx_t> completed_evaluation_cycles {0};
		//! Number of completed cost-rule evaluations (= t in the decision
		//! rule). Drives the warmup gate (`<= thc_warmup_cycles`).
		std::atomic<idx_t> eval_cycle_count {0};
		//! Consecutive high-miss checkpoints; reset on any low-miss segment.
		std::atomic<idx_t> consecutive_high_miss_checkpoints {0};
		//! Length (in probe rows) of the current READ_ONLY segment.
		std::atomic<idx_t> read_only_rows_target {0};

		//! Baseline average ns/probe measured during the BASELINE phase
		//! (main HT only). Cached as a plain double (written once inside
		//! `transition_mutex` at the BASELINE→COLLECT transition) so it
		//! survives any future wrap of `phase_number`. `c_grow_current`,
		//! `c_eval_current`, and `c_eval_prev` are derived on demand from
		//! `phase_metrics[...]` at decision time.
		double c_main = 0.0;
		// NOTE: C_Grow and C_eval are calculated in-flight by the leader of each phase 

		// ---- Telemetry surfaced through PhysicalHashJoin's extra_info ----
		std::atomic<idx_t> probes_at_freeze {0};
		std::atomic<idx_t> probes_at_abandon {0};
		std::atomic<uint8_t> freeze_reason {static_cast<uint8_t>(THCFreezeReason::None)};
		std::atomic<uint8_t> abandon_reason {static_cast<uint8_t>(THCAbandonReason::None)};

		// ---- Critical section ----
		//! Protects (a) the cost doubles and (b) every "phase transition" /
		//! "checkpoint decision body" so the transition logic runs once per
		//! threshold crossing even when many threads observe the crossing
		//! simultaneously. Use double-checked locking on the relevant
		//! atomic counter to avoid taking the mutex on every probe call.
		std::mutex transition_mutex;
	};

	struct InsertState : SharedState {
		explicit InsertState(const JoinHashTable &ht);
		/// Because of the index hick up
		SelectionVector remaining_sel;
		SelectionVector key_match_sel;

		// The ptrs to the row to which a key should be inserted into during building
		// or matched against during probing
		Vector rhs_row_locations;

		DataChunk lhs_data;
		TupleDataChunkState chunk_state;
	};

	JoinHashTable(ClientContext &context, const vector<JoinCondition> &conditions, vector<LogicalType> build_types,
	              JoinType type, const vector<idx_t> &output_columns, idx_t estimated_probe_side_rows);
	~JoinHashTable();

	//! Add the given data to the HT
	void Build(PartitionedTupleDataAppendState &append_state, DataChunk &keys, DataChunk &input);
	//! Merge another HT into this one
	void Merge(JoinHashTable &other);
	//! Combines the partitions in sink_collection into data_collection, as if it were not partitioned
	void Unpartition();
	//! Allocate the pointer table for the probe
	void AllocatePointerTable();
	//! Initialize the pointer table for the probe
	void InitializePointerTable(idx_t entry_idx_from, idx_t entry_idx_to);
	//! Finalize the build of the HT, constructing the actual hash table and making the HT ready for probing.
	//! Finalize must be called before any call to Probe, and after Finalize is called Build should no longer be
	//! ever called.
	void Finalize(idx_t chunk_idx_from, idx_t chunk_idx_to, bool parallel);
	//! Create the (shared) THC if the table is large enough.
	//! Must be called after the Finalize tasks that create the global HT
	void InitializeTieredHashCache();
	//! Probe the HT with the given input chunk, resulting in the given result
	void Probe(ScanStructure &scan_structure, DataChunk &keys, TupleDataChunkState &key_state, ProbeState &probe_state,
	           optional_ptr<Vector> precomputed_hashes = nullptr);
	//! Scan the HT to construct the full outer join result
	void ScanFullOuter(JoinHTScanState &state, Vector &addresses, DataChunk &result) const;

	//! Fill the pointer with all the addresses from the hashtable for full scan
	static idx_t FillWithHTOffsets(JoinHTScanState &state, Vector &addresses);

	//! Increment unique key counter during build (Build-phase approach of mu_s estimation)
	void CountOneUniqueBuildKey();
	//! Whether this join needs the build phase to populate build_unique_keys_cnt.
	bool ShouldCountUniqueBuildKeys() const {
		return thc_count_unique_build_keys;
	}

	idx_t Count() const {
		return data_collection->Count();
	}
	idx_t SizeInBytes() const {
		return data_collection->SizeInBytes();
	}

	//! True when this join's adaptive THC was actually created during finalize.
	//! Used by PhysicalHashJoin to emit per-join THC telemetry.
	bool HasTieredHashCache() const {
		return tiered_hash_cache != nullptr;
	}

	//! Read-only access to the cross-thread adaptive state.  Returns nullptr
	//! when this join does not have an active THC (the global state is created
	//! alongside the THC in `InitializeTieredHashCache`).  Used by
	//! PhysicalHashJoin to emit per-join lifecycle telemetry.
	const GlobalTHCAdaptiveState *GetGlobalTHCState() const {
		return global_thc_state.get();
	}

	PartitionedTupleData &GetSinkCollection() {
		return *sink_collection;
	}

	TupleDataCollection &GetDataCollection() {
		return *data_collection;
	}
	bool NullValuesAreEqual(idx_t col_idx) const {
		return null_values_are_equal[col_idx];
	}

	//! Base-table names of the probe/build inputs, resolved by
	//! PhysicalHashJoin::InitializeHashTable. Empty when the input is not a
	//! plain base-table scan. Used only for DEBUG logging.
	string probe_table_name;
	string build_table_name;

	ClientContext &context;
	//! BufferManager
	BufferManager &buffer_manager;
	//! The join conditions
	const vector<JoinCondition> &conditions;
	//! The types of the keys used in equality comparison
	vector<LogicalType> equality_types;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! Positions of the columns that need to output
	const vector<idx_t> &output_columns;
	//! The comparison predicates that only contain equality predicates
	vector<ExpressionType> equality_predicates;
	//! The comparison predicates that contain non-equality predicates
	vector<ExpressionType> non_equality_predicates;

	//! The column indices of the equality predicates to be used to compare the rows
	vector<column_t> equality_predicate_columns;
	//! The column indices of the non-equality predicates to be used to compare the rows
	vector<column_t> non_equality_predicate_columns;
	//! Data column layout
	shared_ptr<TupleDataLayout> layout_ptr;
	//! Matches the equal condition rows during the build phase of the hash join to prevent
	//! duplicates in a list because of hash-collisions
	RowMatcher row_matcher_build;
	//! Efficiently matches the non-equi rows during the probing phase, only there if non_equality_predicates is not
	//! empty
	unique_ptr<RowMatcher> row_matcher_probe;
	//! Matches the same rows as the row_matcher, but also returns a vector for no matches
	unique_ptr<RowMatcher> row_matcher_probe_no_match_sel;
	//! Is true if there are predicates that are not equality predicates and we need to use the matchers during probing
	bool needs_chain_matcher;

	//! If there is more than one element in the chain, we need to scan the next elements of the chain
	bool chains_longer_than_one;

	//! The capacity of the HT (count of entries). Is the same as hash_map.GetSize() / sizeof(ht_entry_t)
	idx_t capacity = DConstants::INVALID_INDEX;
	//! The size of an entry as stored in the HashTable
	idx_t entry_size;
	//! The total tuple size
	idx_t tuple_size;
	//! Next pointer offset in tuple, also used for the position of the hash, which then gets overwritten by the pointer
	idx_t pointer_offset;
	//! A constant false column for initialising right outer joins
	Vector vfound;
	//! The join type of the HT
	JoinType join_type;
	//! Whether or not the HT has been finalized
	bool finalized;
	//! Whether or not any of the key elements contain NULL
	bool has_null;
	//! Bitmask for getting relevant bits from the hashes to determine the position
	uint64_t bitmask = DConstants::INVALID_INDEX;
	//! Whether or not we error on multiple rows found per match in a SINGLE join
	bool single_join_error_on_multiple_rows = true;

	struct {
		mutex mj_lock;
		//! The types of the duplicate eliminated columns, only used in correlated MARK JOIN for flattening
		//! ANY()/ALL() expressions
		vector<LogicalType> correlated_types;
		//! The aggregate expression nodes used by the HT
		vector<unique_ptr<Expression>> correlated_aggregates;
		//! The HT that holds the group counts for every correlated column
		unique_ptr<GroupedAggregateHashTable> correlated_counts;
		//! Group chunk used for aggregating into correlated_counts
		DataChunk group_chunk;
		//! Payload chunk used for aggregating into correlated_counts
		DataChunk correlated_payload;
		//! Result chunk used for aggregating into correlated_counts
		DataChunk result_chunk;
	} correlated_mark_join_info;

private:
	void InitializeScanStructure(ScanStructure &scan_structure, DataChunk &keys, TupleDataChunkState &key_state,
	                             const SelectionVector *&current_sel);
	void Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes);

	bool UseSalt() const;

	//! Gets a pointer to the entry in the HT for each of the hashes_v using linear probing. Will update the
	//! key_match_sel vector and the count argument to the number and position of the matches
	void GetRowPointers(DataChunk &keys, TupleDataChunkState &key_state, ProbeState &state, Vector &hashes_v,
	                    const SelectionVector *sel, idx_t &count, Vector &pointers_result_v, SelectionVector &match_sel,
	                    bool has_sel);

	//! Shared THC probe + regular HT fallback logic used by both READ_ONLY and COLLECT (cycle > 0).
	//! Densifies hashes, probes the THC (ProbeAndMatch or ProbeByHash), falls back to
	//! GetRowPointersInternal for misses, and returns the combined match results.
	//! On return, match_count and cache_miss_count are set.
	//! When called from COLLECT (cycle > 0), the caller reads state.thc_miss_match_sel
	//! and state.thc_miss_match_count to find which miss-fallback rows found a match,
	//! and reads hashes directly from hashes_v (not hashes_dense_v, which is overwritten
	//! by GetRowPointersInternal during fallback).
	void ProbeTHCAndFallback(DataChunk &keys, TupleDataChunkState &key_state, ProbeState &state,
	                        Vector &hashes_v, const SelectionVector *sel, idx_t &count, bool has_sel,
	                        Vector &pointers_result_v, SelectionVector &match_sel,
	                        idx_t &match_count, idx_t &cache_miss_count);

	//! Drain a thread's local `collected_entries` buffer into the shared THC.
	//! Called when a probing thread observes a COLLECT→READ_ONLY phase change
	//! (so its cycle-K buffer is flushed before it starts cycle K+1 / READ_ONLY
	//! work). Also called immediately by the thread that triggers the
	//! transition. Updates `g.total_new_entries`, `g.first_collect_new_entries`
	//! (only when `state.observed_phase_number == 1`, i.e. the very first
	//! COLLECT — cycle 0), and the InsertBatch time on the per-phase slot
	//! `g.phase_metrics[state.observed_phase_number % MAX_PHASES].time_ns`
	//! so the cost is attributed to the COLLECT phase the buffer belonged
	//! to. Sets `g.freeze_reason = THCFull` and `g.collection_enabled = false`
	//! if the THC fills up.
	void FlushCollectedEntriesIntoTHC(ProbeState &state, GlobalTHCAdaptiveState &g);

private:
	//! Insert the given set of locations into the HT with the given set of hashes_v
	void InsertHashes(Vector &hashes_v, idx_t count, TupleDataChunkState &chunk_state, InsertState &insert_statebool,
	                  bool parallel);
	//! Prepares keys by filtering NULLs
	idx_t PrepareKeys(DataChunk &keys, vector<TupleDataVectorFormat> &vector_data, const SelectionVector *&current_sel,
	                  SelectionVector &sel, bool build_side);

	//! Lock for combining data_collection when merging HTs
	mutex data_lock;
	//! Partitioned data collection that the data is sunk into when building
	unique_ptr<PartitionedTupleData> sink_collection;
	//! The DataCollection holding the main data of the hash table
	unique_ptr<TupleDataCollection> data_collection;

	//! The hash map of the HT, created after finalization
	AllocatedData hash_map;
	ht_entry_t *entries = nullptr;
	//! Whether or not NULL values are considered equal in each of the comparisons
	vector<bool> null_values_are_equal;
	//! An empty tuple that's a "dead end", can be used to stop chains early
	unsafe_unique_array<data_t> dead_end;

	//! Shared THC for accelerating repeated probe lookups.
	//! Created during Finalize when the hash table is large enough.
	unique_ptr<TieredHashCache> tiered_hash_cache;

	//! Cross-thread coordination state for the adaptive THC algorithm.
	//! Created in lockstep with `tiered_hash_cache` (see
	//! `InitializeTieredHashCache`); null when the THC is disabled or could
	//! not be activated for this join. Every probing thread reads/updates
	//! this struct rather than its own `ProbeState`.
	unique_ptr<GlobalTHCAdaptiveState> global_thc_state;

	//! The byte offset of the join key in each cached row
	//! Before that key, there is the validity byte coming from data_collection
	idx_t tiered_hash_cache_key_offset = 0;

	// ---- Per-instance THC parameters (loaded from ClientConfig at construction) ----
	//! The capacity of the THC (in count of entries) computed by ComputeCapacity.
	idx_t thc_capacity;
	//! Memory budget (bytes) for the THC. Controls ComputeCapacity.
	idx_t thc_budget_bytes;
	//! Number of probe rows per collect phase before flushing to the THC.
	idx_t thc_collect_phase_rows;
	//! Base length (in probe rows) of the first READ_ONLY phase; doubles each cycle.
	idx_t thc_first_read_only_phase_rows;
	//! Maximum fraction of probe rows that can be spent in collect phases.
	double thc_collect_budget_fraction;
	//! Miss rate threshold for skipping collect phases.
	double thc_miss_below_which_skip_collect;
	//! Miss rate threshold above which THC is abandoned.
	double thc_miss_above_which_abandon;
	//! Consecutive high-miss checkpoints required before abandoning THC.
	idx_t thc_abandon_consecutive_misses;
	//! Minimum HT capacity to activate the THC.
	idx_t thc_activation_threshold;
	//! Maximum THC load factor; inserts stop beyond this fill ratio.
	double thc_max_load_factor;
	//! Maximum estimated fraction of hot build-side rows before abandoning THC.
	double thc_max_estimated_perc_hot;
	//! Minimum coverage factor: THC is abandoned when thc_size_needed * this > thc_size.
	double thc_min_coverage_of_build_side;
	//! Toggle for the one-shot first-cycle multiplicity/hotness/coverage abandon check.
	bool thc_enable_first_cycle_check;
	//! Number of COLLECT+EVAL cycles that must complete before the cost-based
	//! decision rule (drop/freeze/continue) activates. During warmup, every
	//! evaluation checkpoint unconditionally proceeds to the next COLLECT phase.
	idx_t thc_warmup_cycles;
	//! Toggle for abandoning THC when current eval cost is no better than baseline.
	bool thc_enable_delta_check;
	//! Toggle for freezing THC when marginal gain is lower than collect cost.
	bool thc_enable_shrinkage_check;
	//! If the estimated probe multiplicity mu_{S->R} after the first
	//! COLLECT+READ_ONLY cycle is below this threshold, THC is skipped
	//! entirely and probing falls back to the regular hash table path.
	double thc_min_estimated_mu_s_to_r;
	//! The size of an entry in the THC, including the tag, the row, and the padding.
	idx_t thc_entry_stride;
	
	//! Estimated probe-side row count passed from the physical hash join.
	idx_t estimated_probe_side_rows;
	//! True when only one thread is active, enabling non-atomic InsertUnsafe.
	bool thc_single_threaded = false;

	// ---- mu_s estimation ----
	//! Which mu_s estimation method(s) to run: "none", "build_count", "probe_sample", "ht_sample", "all".
	std::string thc_mu_s_method;
	//! When true, log mu_s estimates to stderr.
	bool thc_log_mu_s = false;
	//! Build-phase approach: count of unique keys inserted during build (Finalize). Atomic for parallel Finalize.
	std::atomic<idx_t> build_unique_keys_cnt {0};
	//! Avoid paying the build-count estimator cost when no enabled THC decision will use it.
	bool thc_count_unique_build_keys = false;
	//! Build phase approach result: mu_s computed after Finalize as Count() / build_unique_keys.
	double mu_s_build_estimate = 0.0;
	//! Hash table sampling approach: mu_s from post-finalize HT sampling.
	double mu_s_ht_sample_estimate = 0.0;
	//! Hash table sampling approach: post-finalize HT sampling. Returns mean chain length.
	double EstimateMuSFromHTSample();

	//! Copying not allowed
	JoinHashTable(const JoinHashTable &) = delete;

public:
	//===--------------------------------------------------------------------===//
	// External Join
	//===--------------------------------------------------------------------===//
	static constexpr const idx_t INITIAL_RADIX_BITS = 4;

	struct ProbeSpillLocalAppendState {
		ProbeSpillLocalAppendState() {
		}
		//! Local partition and append state (if partitioned)
		optional_ptr<PartitionedColumnData> local_partition;
		optional_ptr<PartitionedColumnDataAppendState> local_partition_append_state;
	};
	//! ProbeSpill represents materialized probe-side data that could not be probed during PhysicalHashJoin::Execute
	//! because the HashTable did not fit in memory. The ProbeSpill is not partitioned if the remaining data can be
	//! dealt with in just 1 more round of probing, otherwise it is radix partitioned in the same way as the HashTable
	struct ProbeSpill {
	public:
		ProbeSpill(JoinHashTable &ht, ClientContext &context, const vector<LogicalType> &probe_types);

	public:
		//! Create a state for a new thread
		ProbeSpillLocalAppendState RegisterThread();
		//! Append a chunk to this ProbeSpill
		void Append(DataChunk &chunk, ProbeSpillLocalAppendState &local_state);
		//! Finalize by merging the thread-local accumulated data
		void Finalize();

	public:
		//! Prepare the next probe round
		void PrepareNextProbe();
		//! Scans and consumes the ColumnDataCollection
		unique_ptr<ColumnDataConsumer> consumer;

	private:
		JoinHashTable &ht;
		mutex lock;
		ClientContext &context;

		//! The types of the probe DataChunks
		const vector<LogicalType> &probe_types;
		//! The column ids
		vector<column_t> column_ids;

		//! The partitioned probe data and append states
		unique_ptr<PartitionedColumnData> global_partitions;
		vector<unique_ptr<PartitionedColumnData>> local_partitions;
		vector<unique_ptr<PartitionedColumnDataAppendState>> local_partition_append_states;

		//! The active probe data
		unique_ptr<ColumnDataCollection> global_spill_collection;
	};

	idx_t GetRadixBits() const {
		return radix_bits;
	}

	//! For a LOAD_FACTOR of 2.0, the HT is between 25% and 50% full
	static constexpr double DEFAULT_LOAD_FACTOR = 2.0;
	//! For a LOAD_FACTOR of 1.5, the HT is between 33% and 67% full
	static constexpr double EXTERNAL_LOAD_FACTOR = 1.5;

	double load_factor = DEFAULT_LOAD_FACTOR;

	//! Capacity of the pointer table given the ht count
	idx_t PointerTableCapacity(idx_t count) const {
		static constexpr idx_t MINIMUM_CAPACITY = 16384;

		const auto capacity = NextPowerOfTwo(LossyNumericCast<idx_t>(static_cast<double>(count) * load_factor));
		return MaxValue<idx_t>(capacity, MINIMUM_CAPACITY);
	}
	//! Size of the pointer table (in bytes)
	idx_t PointerTableSize(idx_t count) const {
		return PointerTableCapacity(count) * sizeof(data_ptr_t);
	}

	//! Get total size of HT if all partitions would be built
	idx_t GetTotalSize(const vector<unique_ptr<JoinHashTable>> &local_hts, idx_t &max_partition_size,
	                   idx_t &max_partition_count) const;
	idx_t GetTotalSize(const vector<idx_t> &partition_sizes, const vector<idx_t> &partition_counts,
	                   idx_t &max_partition_size, idx_t &max_partition_count) const;
	//! Get the remaining size of the unbuilt partitions
	idx_t GetRemainingSize() const;
	//! Sets number of radix bits according to the max ht size
	void SetRepartitionRadixBits(const idx_t max_ht_size, const idx_t max_partition_size,
	                             const idx_t max_partition_count);
	//! Initialized "current_partitions" and "completed_partitions"
	void InitializePartitionMasks();
	//! How many partitions are currently active
	idx_t CurrentPartitionCount() const;
	//! How many partitions are fully done
	idx_t FinishedPartitionCount() const;
	//! Partition this HT
	void Repartition(JoinHashTable &global_ht);

	//! Delete blocks that belong to the current partitioned HT
	void Reset();
	//! Build HT for the next partitioned probe round
	bool PrepareExternalFinalize(const idx_t max_ht_size);
	//! Probe whatever we can, sink the rest into a thread-local HT
	void ProbeAndSpill(ScanStructure &scan_structure, DataChunk &probe_keys, TupleDataChunkState &key_state,
	                   ProbeState &probe_state, DataChunk &probe_chunk, ProbeSpill &probe_spill,
	                   ProbeSpillLocalAppendState &spill_state, DataChunk &spill_chunk);

private:
	//! The current number of radix bits used to partition
	idx_t radix_bits;

	//! Bits set to 1 for currently active partitions
	ValidityMask current_partitions;
	//! Bits set to 1 for completed partitions
	ValidityMask completed_partitions;
};

} // namespace duckdb
