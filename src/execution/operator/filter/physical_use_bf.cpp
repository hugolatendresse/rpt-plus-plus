#include "duckdb/execution/operator/filter/physical_use_bf.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/scoped_hash_join_timer.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

// Mirror of GetCreateBFTimingInfo / GetHashJoinTimingInfo. Two disjoint
// buckets that partition ExecuteInternal: every nanosecond inside it is
// charged to exactly one of `lookup_time_ns` (the BF probe call) or
// `filter_apply_time_ns` (selection vector + slice + selectivity check, or
// the pass-through Reference when the BF was abandoned/disabled). Their sum
// is the total CPU time spent in UseBF without double-counting.
static InsertionOrderPreservingMap<string> GetUseBFTimingInfo(const uint64_t lookup_ns,
                                                              const uint64_t filter_apply_ns) {
	InsertionOrderPreservingMap<string> result;
	const auto total_ns = lookup_ns + filter_apply_ns;
	result["UseBF Total Time"] = StringUtil::Format("%.3f ms", static_cast<double>(total_ns) / 1000000.0);
	result["UseBF Lookup Time"] = StringUtil::Format("%.3f ms", static_cast<double>(lookup_ns) / 1000000.0);
	result["UseBF Filter Apply Time"] =
	    StringUtil::Format("%.3f ms", static_cast<double>(filter_apply_ns) / 1000000.0);
	return result;
}

//! Per-execute global state for PhysicalUseBF. Holds the cross-thread atomic
//! timer accumulators. Created fresh by GetGlobalOperatorState on every
//! execute, so values don't leak across re-runs of a prepared statement.
class UseBFGlobalState : public GlobalOperatorState {
public:
	std::atomic<uint64_t> lookup_time_ns {0};
	std::atomic<uint64_t> filter_apply_time_ns {0};
};
PhysicalUseBF::PhysicalUseBF(vector<LogicalType> types, const shared_ptr<FilterPlan> &filter_plan,
                             unique_ptr<BloomFilterUsage> bf, PhysicalCreateBF *related_create_bfs,
                             idx_t estimated_cardinality, bool below_join)
    : CachingPhysicalOperator(PhysicalOperatorType::USE_BF, std::move(types), estimated_cardinality),
      filter_plan(filter_plan), related_creator(related_create_bfs), bf_to_use(std::move(bf)) {
	if (below_join) {
		cache_threshold = COMPACTION_THRESHOLD;
	}
}

class UseBFState : public CachingOperatorState {
public:
	static constexpr int64_t NUM_CHUNK_FOR_CHECK = 32;
	static constexpr double SELECTIVITY_THRESHOLD = 0.9;

public:
	UseBFState(bool valid_bf, bool enable_timers_p)
	    : sel_vector(STANDARD_VECTOR_SIZE), lookup_results(STANDARD_VECTOR_SIZE), use_bf(valid_bf),
	      enable_timers(enable_timers_p) {
	}

	SelectionVector sel_vector;
	vector<uint32_t> lookup_results;

	bool use_bf;
	bool is_checked = false;
	int64_t num_chunk = 0;
	uint64_t num_received = 0;
	uint64_t num_sent = 0;

	//! Mirror of ClientConfig.enable_hash_join_timers. Snapshotted once per
	//! state at construction; passed into every ScopedHashJoinTimer.
	const bool enable_timers;

public:
	void CheckBFSelectivity(uint64_t num_in, uint64_t num_out) {
		num_received += num_in;
		num_sent += num_out;
		num_chunk++;

		if (num_chunk > NUM_CHUNK_FOR_CHECK) {
			is_checked = true;

			double selectivity = static_cast<double>(num_sent) / static_cast<double>(num_received);
			if (selectivity > SELECTIVITY_THRESHOLD && selectivity < 1) {
				use_bf = false;
			}
		}
	}

	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		// Note: we intentionally do NOT AddExtraInfo here — by the time the
		// pipeline executor calls OperatorState::Finalize, StartOperator has
		// already been paired with EndOperator and the profiler's
		// active_operator is null, so AddExtraInfo would silently no-op.
		// The per-chunk emission inside ExecuteInternal (where the executor
		// sets active_operator) is what actually lands the values in JSON.
		context.thread.profiler.Flush(op);
	}
};

unique_ptr<OperatorState> PhysicalUseBF::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<UseBFState>(bf_to_use->IsValid(),
	                             ClientConfig::GetConfig(context.client).enable_hash_join_timers);
}

unique_ptr<GlobalOperatorState> PhysicalUseBF::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<UseBFGlobalState>();
}

InsertionOrderPreservingMap<string> PhysicalUseBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["BF Creators"] = "0x" + std::to_string(reinterpret_cast<size_t>(related_creator)) + "\n";
	return result;
}

void PhysicalUseBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, *this);
	related_creator->BuildPipelinesFromRelated(current, meta_pipeline);
	children[0].get().BuildPipelines(current, meta_pipeline);
}

OperatorResultType PhysicalUseBF::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                  GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &state = state_p.Cast<UseBFState>();
	auto &gstate = gstate_p.Cast<UseBFGlobalState>();

	// Use atomic-target timers (one fetch_add per scope per chunk) directly
	// against per-execute globals. We do not use thread-local accumulators
	// here: there are only two scopes per chunk so the per-scope atomic cost
	// is negligible, and emitting from inside ExecuteInternal (where the
	// pipeline executor has set the profiler's active_operator) requires the
	// global values to already reflect this thread's contribution.
	if (!state.use_bf) {
		// Pass-through path. Reference is essentially free, but we attribute
		// the wall-time to filter_apply_time_ns so the two buckets partition
		// ExecuteInternal cleanly.
		{
			ScopedHashJoinTimer filter_timer(&gstate.filter_apply_time_ns, state.enable_timers);
			chunk.Reference(input);
		}
	} else {
		// 1. Lookup the BloomFilter
		{
			ScopedHashJoinTimer lookup_timer(&gstate.lookup_time_ns, state.enable_timers);
			bf_to_use->Lookup(input, state.lookup_results);
		}

		// 2. Fill results and slice. Times the selection-vector build, the
		//    Reference/Slice branch, and the selectivity check below — i.e.
		//    everything in ExecuteInternal that is *not* the BF lookup itself.
		{
			ScopedHashJoinTimer filter_timer(&gstate.filter_apply_time_ns, state.enable_timers);

			idx_t result_count = 0;
			auto &sel = state.sel_vector;
			for (size_t i = 0; i < input.size(); i++) {
				sel.set_index(result_count, i);
				result_count += state.lookup_results[i];
			}
			if (result_count == input.size()) {
				// nothing was filtered: skip adding any selection vectors
				chunk.Reference(input);
			} else {
				chunk.Slice(input, sel, result_count);
			}

			// 3. Update statistics
			if (!state.is_checked) {
				state.CheckBFSelectivity(input.size(), result_count);
			}
		}
	}

	// Emit current global values into the per-thread profiler. The executor
	// wraps ExecuteInternal with StartOperator/EndOperator so active_operator
	// is set; AddExtraInfo's replace-merge semantics make repeated per-chunk
	// emission safe (every call overwrites with the same atomic snapshot).
	if (state.enable_timers) {
		auto lookup_ns = gstate.lookup_time_ns.load(std::memory_order_relaxed);
		auto filter_apply_ns = gstate.filter_apply_time_ns.load(std::memory_order_relaxed);
		context.thread.profiler.AddExtraInfo(GetUseBFTimingInfo(lookup_ns, filter_apply_ns));
	}

	return OperatorResultType::NEED_MORE_INPUT;
}
} // namespace duckdb
