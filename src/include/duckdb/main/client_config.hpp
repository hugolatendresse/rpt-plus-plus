//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/join_order_mode.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/profiling_info.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

class ClientContext;
class PhysicalResultCollector;
class PreparedStatementData;

typedef std::function<unique_ptr<PhysicalResultCollector>(ClientContext &context, PreparedStatementData &data)>
    get_result_collector_t;

struct ClientConfig {
	//! The home directory used by the system (if any)
	string home_directory;
	//! If the query profiler is enabled or not.
	bool enable_profiler = false;
	//! If detailed query profiling is enabled
	bool enable_detailed_profiling = false;
	//! The format to print query profiling information in (default: query_tree), if enabled.
	ProfilerPrintFormat profiler_print_format = ProfilerPrintFormat::QUERY_TREE;
	//! The file to save query profiling information to, instead of printing it to the console
	//! (empty = print to console)
	string profiler_save_location;
	//! The custom settings for the profiler
	//! (empty = use the default settings)
	profiler_settings_t profiler_settings = ProfilingInfo::DefaultSettings();

	//! Allows suppressing profiler output, even if enabled. We turn on the profiler on all test runs but don't want
	//! to output anything
	bool emit_profiler_output = true;

	//! system-wide progress bar disable.
	const char *system_progress_bar_disable_reason = nullptr;
	//! If the progress bar is enabled or not.
	bool enable_progress_bar = false;
	//! If the print of the progress bar is enabled
	bool print_progress_bar = true;
	//! The wait time before showing the progress bar
	int wait_time = 2000;

	//! Preserve identifier case while parsing.
	//! If false, all unquoted identifiers are lower-cased (e.g. "MyTable" -> "mytable").
	bool preserve_identifier_case = true;
	//! The maximum expression depth limit in the parser
	idx_t max_expression_depth = 1000;

	//! Whether or not aggressive query verification is enabled
	bool query_verification_enabled = false;
	//! Whether or not verification of external operators is enabled, used for testing
	bool verify_external = false;
	//! Whether or not verification of fetch row code is enabled, used for testing
	bool verify_fetch_row = false;
	//! Whether or not we should verify the serializer
	bool verify_serializer = false;
	//! Enable the running of optimizers
	bool enable_optimizer = true;
	//! Which join order enumeration strategy to use (default: DuckDB's DPhyp)
	JoinOrderMode join_order_mode = JoinOrderMode::DPHYP;
	//! When true, only the forward pass of RPT+ is executed (backward pass is skipped)
	bool rpt_forward_only = false;
	//! When true, all of RPT+ is disable (both forward and backware passes are skipped)
	bool disable_rpt = false;
	//! When true, skip initializing the tiered hash cache
	bool disable_tiered_hash_cache = false;
	//! When false, PhysicalCreateBF never gives up constructing a bloom filter based on
	//! observed selectivity or estimated/actual memory pressure. All three give-up
	//! branches in PhysicalCreateBF::GiveUpBFCreation (OOM against the temp-memory
	//! reservation, unselective base-table pipeline, and projected OOM) are bypassed,
	//! so every BF scheduled by RPT+ is built to completion. Intended for
	//! benchmarking / A-B comparisons where we want the BF set to be independent of
	//! the runtime heuristics; may increase memory use and slow queries when the
	//! heuristics would otherwise have correctly discarded a useless BF.
	bool drop_bf_at_runtime = true;
	//! When false, `CreateBloomFilterPlan` creates a Bloom Filter for every base
	//! table that participates in the transfer graph, bypassing the
	//! `HasAnyFilter` gate that normally suppresses BF creation on tables that
	//! have neither a local filter nor an incoming BF to use. This lets
	//! otherwise "useless" full-column BFs still be built -- useful for
	//! benchmarking / THC experiments where we want a BF attached to every
	//! base table independent of local predicates or transfer-order position.
	//! Default true matches the RPT+ paper behavior.
	bool skip_unfiltered_tables_create_bf_plan = true;
	//! Whether SkipUnfilteredTable is executed during transfer-graph
	//! construction. True matches the RPT+ paper / current behavior.
	bool skip_unfiltered_tables_graph_creation = true;
	//! Controls how the ROOT of the predicate-transfer spanning tree is picked.
	//! False (default): RPT+ behavior -- the root is the largest filtered or
	//!                  intermediate table (LargestRootUpdated path), falling
	//!                  back to the largest table if none qualifies.
	//! True:            seed-driven pick over a deterministic name-sorted list
	//!                  of all candidate tables (any table can be the root,
	//!                  including unfiltered ones).
	//! Independent of `use_seeded_transfer_order`; both flags can be combined.
	bool use_seeded_root = false;
	//! Controls how every NON-ROOT node of the predicate-transfer spanning tree
	//! is picked.
	//! False (default): RPT+ behavior -- greedy cardinality-driven pick via
	//!                  FindEdge (the unconstructed table with the highest
	//!                  estimated cardinality reachable from the constructed
	//!                  set).
	//! True:            seed-driven pick over a deterministic name-sorted list
	//!                  of reachable unconstructed tables; the parent inside
	//!                  the constructed set is also picked via the seed.
	//! Independent of `use_seeded_root`; both flags can be combined.
	bool use_seeded_transfer_order = false;
	//! Seed used for any seed-driven pick during transfer-graph construction.
	//! Consulted whenever `use_seeded_root` or `use_seeded_transfer_order` is
	//! true (otherwise ignored). Seed = 0 is a valid value and will
	//! deterministically pick the first option at each step. The seed is
	//! advanced via MurmurHash64 after every pick that consumes it. // TODO shouldn't we use a hash at each step???
	idx_t transfer_graph_seed = 0;
	//! Controls the cost-based build/probe side swap performed by
	//! BuildProbeSideOptimizer::TryFlipJoinChildren. When false, that
	//! optimizer leaves the (left=probe, right=build) assignment that
	//! comes out of the join-order optimizer untouched. The semantic
	//! flipping paths (DELIM join delim_flipped bookkeeping, RIGHT->LEFT
	//! conversion in the binder) are not affected by this flag -- those
	//! are required for correctness, not a performance swap.
	//!
	//! Default true matches the upstream DuckDB behavior. Set to false
	//! together with `join_order_mode = 'seeded_left_deep'` to guarantee
	//! the left-deep shape survives all of the optimizer pipeline.
	bool allow_build_probe_side_swap = true;
	//! When true, never use perfect hash join
	bool disable_perfect_hashing = false;
	//! When true, populate fine-grained hash-join timing counters
	//! (Build Time, Probe Time, Match Time, THC Collect/Insert/Probe Time, ...)
	//! into the query profiler output. Off by default because each timer adds
	//! a pair of `std::chrono::steady_clock::now()` calls per scoped region,
	//! which is non-trivial overhead on the hottest probe paths.
	//! Replaces the historical DUCKDB_ENABLE_HASH_JOIN_TIMERS compile-time flag,
	//! so the timers can be toggled at SQL level (PRAGMA / SET) without rebuilding.
	bool enable_hash_join_timers = false;
	//! Memory budget (in bytes) for the Tiered Hash Cache.
	//! Controls how much of L3 the THC is allowed to occupy.
	//! Default: 32 MiB (sized for typical L3 caches).
	idx_t thc_budget_bytes = 32ULL * 1024 * 1024;
	//! Number of probe-side rows processed per THC collect phase.
	//! Smaller values mean faster warm-up but more frequent collect/flush cycles.
	idx_t thc_collect_phase_rows = 200000;
	//! Base length (in probe rows) of the first READ_ONLY phase after a collect.
	//! Subsequent READ_ONLY phases double this via exponential backoff.
	idx_t thc_first_read_only_phase_rows = 999999999;
	//! Maximum fraction of probe rows that can be spent in THC collect phases.
	//! Example: 0.02 means collect overhead is capped at 2% of probe rows.
	double thc_collect_budget_fraction = 0.02;
	//! THC miss rate threshold (0.0–1.0). If the miss rate in a READ_ONLY
	//! segment is below this, we skip the next collect phase.
	double thc_miss_below_which_skip_collect = 0.10;
	//! Minimum HT capacity (in entries) to activate the THC.
	//! Hash tables smaller than this are assumed to fit in L3 naturally.
	idx_t thc_activation_threshold = 10ULL * 1024 * 1024 / sizeof(uint64_t);
	//! Maximum load factor for the THC (0.0–1.0).
	//! Beyond this fill ratio, THC does not insert new entries. This
	//! is to avoid pathological linear-probing chains.
	double thc_max_load_factor = 0.875;
	//! Which mu_s (within-build side multiplicity) estimation method(s) to run during hash join.
	//! Values: "none", "build_count", "probe_sample", "ht_sample", or "all".
	//! "none" bypasses mu_s estimation
	//! "build_count" is during hash table build
	//! "probe_sample" is during the first cycle of probing
	//! "ht_sample" is between building and probing
	std::string thc_mu_s_method =
	    "build_count"; // TODO we are now incurring a cost on every single build. Other methods are less precise but
	                   // could be done only if we are to use a THC
	//! When true, log mu_s estimates to stderr (works in both debug and release builds).
	bool thc_log_mu_s = false;
	//! Minimum estimated mu_{S->R} to keep THC active after the first cycle.
	double thc_min_estimated_mu_s_to_r = 4.0;
	//! Maximum estimated fraction of hot build-side rows before abandoning THC.
	double thc_max_estimated_perc_hot = 0.5;
	//! Minimum coverage factor: THC is abandoned when thc_size < thc_size_needed * this.
	double thc_min_coverage_of_build_side = 0.1;
	//! Toggle the one-time first-cycle multiplicity/hotness/coverage abandon check.
	bool thc_enable_first_cycle_check = true;
	//! Number of COLLECT+EVAL cycles that must complete before the cost-based
	//! decision rule (drop/freeze/continue) activates.
	idx_t thc_warmup_cycles = 4;
	//! Enable caching operators
	bool enable_caching_operators = true;
	//! Force parallelism of small tables, used for testing
	bool verify_parallelism = false;
	//! Force out-of-core computation for operators that support it, used for testing
	bool force_external = false;
	//! Force disable cross product generation when hyper graph isn't connected, used for testing
	bool force_no_cross_product = false;
	//! Force use of IEJoin to implement AsOfJoin, used for testing
	bool force_asof_iejoin = false;
	//! Force use of fetch row instead of scan, used for testing
	bool force_fetch_row = false;
	//! Use range joins for inequalities, even if there are equality predicates
	bool prefer_range_joins = false;
	//! If this context should also try to use the available replacement scans
	//! True by default
	bool use_replacement_scans = true;
	//! Maximum bits allowed for using a perfect hash table (i.e. the perfect HT can hold up to 2^perfect_ht_threshold
	//! elements)
	idx_t perfect_ht_threshold = 12;
	//! The maximum number of rows to accumulate before sorting ordered aggregates.
	idx_t ordered_aggregate_threshold = (idx_t(1) << 18);
	//! The number of rows to accumulate before flushing during a partitioned write
	idx_t partitioned_write_flush_threshold = idx_t(1) << idx_t(19);
	//! The amount of rows we can keep open before we close and flush them during a partitioned write
	idx_t partitioned_write_max_open_files = idx_t(100);
	//! The number of rows we need on either table to choose a nested loop join
	idx_t nested_loop_join_threshold = 5;
	//! The number of rows we need on either table to choose a merge join over an IE join
	idx_t merge_join_threshold = 1000;
	//! The maximum number of rows to use the nested loop join implementation
	idx_t asof_loop_join_threshold = 64;

	//! The maximum amount of memory to keep buffered in a streaming query result. Default: 1mb.
	idx_t streaming_buffer_size = 1000000;

	//! Callback to create a progress bar display
	progress_bar_display_create_func_t display_create_func = nullptr;

	//! The explain output type used when none is specified (default: PHYSICAL_ONLY)
	ExplainOutputType explain_output_type = ExplainOutputType::PHYSICAL_ONLY;

	//! The maximum amount of pivot columns
	idx_t pivot_limit = 100000;

	//! The threshold at which we switch from using filtered aggregates to LIST with a dedicated pivot operator
	idx_t pivot_filter_threshold = 20;

	//! The maximum amount of OR filters we generate dynamically from a hash join
	idx_t dynamic_or_filter_threshold = 50;

	//! The maximum amount of rows in the LIMIT/SAMPLE for which we trigger late materialization
	idx_t late_materialization_max_rows = 50;

	//! Whether the "/" division operator defaults to integer division or floating point division
	bool integer_division = false;
	//! When a scalar subquery returns multiple rows - return a random row instead of returning an error
	bool scalar_subquery_error_on_multiple_rows = true;
	//! Use IEE754-compliant floating point operations (returning NAN instead of errors/NULL)
	bool ieee_floating_point_ops = true;
	//! Allow ordering by non-integer literals - ordering by such literals has no effect
	bool order_by_non_integer_literal = false;
	//! Disable casting from timestamp => timestamptz (naïve timestamps)
	bool disable_timestamptz_casts = false;
	//! If DEFAULT or ENABLE_SINGLE_ARROW, it is possible to use the deprecated single arrow operator (->) for lambda
	//! functions. Otherwise, DISABLE_SINGLE_ARROW.
	LambdaSyntax lambda_syntax = LambdaSyntax::DEFAULT;
	//! The profiling coverage. SELECT is the default behavior, and ALL emits profiling information for all operator
	//! types.
	ProfilingCoverage profiling_coverage = ProfilingCoverage::SELECT;

	//! Output error messages as structured JSON instead of as a raw string
	bool errors_as_json = false;

	//! Generic options
	case_insensitive_map_t<Value> set_variables;

	//! Variables set by the user
	case_insensitive_map_t<Value> user_variables;

	//! Function that is used to create the result collector for a materialized result
	//! Defaults to PhysicalMaterializedCollector
	get_result_collector_t result_collector = nullptr;

	//! If HTTP logging is enabled or not.
	bool enable_http_logging = true;

	//! **DEPRECATED** The file to save query HTTP logging information to, instead of printing it to the console
	//! (empty = output to the DuckDB logger)
	string http_logging_output;

public:
	static ClientConfig &GetConfig(ClientContext &context);
	static const ClientConfig &GetConfig(const ClientContext &context);

	bool AnyVerification() {
		return query_verification_enabled || verify_external || verify_serializer || verify_fetch_row;
	}

	void SetUserVariable(const string &name, Value value) {
		user_variables[name] = std::move(value);
	}

	bool GetUserVariable(const string &name, Value &result) {
		auto entry = user_variables.find(name);
		if (entry == user_variables.end()) {
			return false;
		}
		result = entry->second;
		return true;
	}

	void ResetUserVariable(const string &name) {
		user_variables.erase(name);
	}

	template <class OP>
	static typename OP::RETURN_TYPE GetSetting(const ClientContext &context) {
		return OP::GetSetting(context).template GetValue<typename OP::RETURN_TYPE>();
	}

	template <class OP>
	static Value GetSettingValue(const ClientContext &context) {
		return OP::GetSetting(context);
	}

public:
	void SetDefaultStreamingBufferSize();
};

struct ScopedConfigSetting {
public:
	using config_modify_func_t = std::function<void(ClientConfig &config)>;

public:
	explicit ScopedConfigSetting(ClientConfig &config, config_modify_func_t set_f = nullptr,
	                             config_modify_func_t unset_f = nullptr)
	    : config(config), set(std::move(set_f)), unset(std::move(unset_f)) {
		if (set) {
			set(config);
		}
	}
	~ScopedConfigSetting() {
		if (unset) {
			unset(config);
		}
	}

public:
	ClientConfig &config;
	config_modify_func_t set;
	config_modify_func_t unset;
};

} // namespace duckdb
