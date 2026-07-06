#!/usr/bin/env bash
# Allows running the JOB benchmark
# Usage:
# scripts/measure/run_job.sh --case <1|2|3|4> [--perf] [--debug] [--job-query NX]
# Sweep example:
# scripts/measure/run_job.sh --cases 2,3,4 --seeds 5 [--job-query 10a] [--csv path]
set -euo pipefail

CASE=""
CASES_LIST=""
JOB_QUERY=""
JOB_QUERIES_LIST=""
SEED=""
SEEDS_COUNT=""
RUNS=1
CSV_PATH=""
USE_PERF=false
USE_DEBUG=false
USE_DUCKDB_PROFILING=false
CREATE_BOXPLOTS=false
DROP_OS_CACHE=false
TIMEOUT_SECONDS=""
PERF_EVENTS="cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,mem_access,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd,branch-instructions,branch-misses,br_retired,br_mis_pred_retired"

usage() {
	cat <<'USAGE'
Usage: scripts/measure/run_job.sh (--case <1|2|3|4> | --cases <list>) [options]

Options:
  --case <1|2|3|4>      Optimizer case (mutually exclusive with --cases)
  --cases <list>        Comma-separated case list, e.g. 2,3,4
  --job-query <id>      Run one JOB query (e.g. 10a, 24a, or 10a.sql)
  --job-queries <list>  Comma-separated JOB query list, e.g. 1a,2a,2b,4c
  --seed <int>          Override transfer_graph_seed (mutually exclusive with --seeds)
  --seeds <N>           Sweep seeds 0..N-1 (overrides transfer_graph_seed)
  --runs <N>            Number of benchmark runs per (case, seed) tuple (default: 1)
  --csv <path>          Write per-run CSV (auto-named if omitted in sweep mode)
  --perf                Run query/queries under perf stat
  --debug               Use debug build (build/debug/duckdb)
  --duckdb-profiling    Enable DuckDB JSON profiling: per-query JSON files
                        under results/job/profiling_<timestamp>/, plus an
                        augmented runtime CSV with per-join THC telemetry
                        columns Join1..JoinN (via thc_csv_postprocess.py).
  --create-boxplots     Create runtime boxplot PNGs from the final runtime CSV
  --drop-os-cache       Run sync + drop Linux page cache before each measured
                        DuckDB query. Requires sudo and affects the whole host.
  --timeout <seconds>   Per-query wall-clock cap; on timeout DuckDB is killed
                        and the run records a runtime of 9999999 in the CSV.
                        DuckDB OOM/temp-spill-limit failures record 8888888
                        so long sweeps can continue.
  -h, --help            Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--case)
		CASE="$2"
		shift 2
		;;
	--cases)
		CASES_LIST="$2"
		shift 2
		;;
	--job-query)
		JOB_QUERY="$2"
		shift 2
		;;
	--job-queries)
		JOB_QUERIES_LIST="$2"
		shift 2
		;;
	--seed)
		SEED="$2"
		shift 2
		;;
	--seeds)
		SEEDS_COUNT="$2"
		shift 2
		;;
	--runs)
		RUNS="$2"
		shift 2
		;;
	--csv)
		CSV_PATH="$2"
		shift 2
		;;
	--perf)
		USE_PERF=true
		shift
		;;
	--debug)
		USE_DEBUG=true
		shift
		;;
	--duckdb-profiling)
		USE_DUCKDB_PROFILING=true
		shift
		;;
	--create-boxplots)
		CREATE_BOXPLOTS=true
		shift
		;;
	--drop-os-cache)
		DROP_OS_CACHE=true
		shift
		;;
	--timeout)
		TIMEOUT_SECONDS="$2"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "Unknown option: $1" >&2
		usage
		exit 1
		;;
	esac
done

if [[ -n "$CASE" && -n "$CASES_LIST" ]]; then
	echo "Error: --case and --cases are mutually exclusive." >&2
	exit 1
fi
if [[ -n "$SEED" && -n "$SEEDS_COUNT" ]]; then
	echo "Error: --seed and --seeds are mutually exclusive." >&2
	exit 1
fi
if [[ -n "$JOB_QUERY" && -n "$JOB_QUERIES_LIST" ]]; then
	echo "Error: --job-query and --job-queries are mutually exclusive." >&2
	exit 1
fi
if [[ -z "$CASE" && -z "$CASES_LIST" ]]; then
	echo "Error: --case or --cases is required." >&2
	exit 1
fi
if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
	echo "Error: --runs must be a positive integer (got: $RUNS)" >&2
	exit 1
fi
if [[ -n "$TIMEOUT_SECONDS" ]]; then
	if ! [[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || [[ "$TIMEOUT_SECONDS" -lt 1 ]]; then
		echo "Error: --timeout must be a positive integer number of seconds (got: $TIMEOUT_SECONDS)" >&2
		exit 1
	fi
fi

CASES=()
if [[ -n "$CASE" ]]; then
	CASES=("$CASE")
else
	IFS=',' read -r -a CASES <<<"$CASES_LIST"
fi
for c in "${CASES[@]}"; do
	case "$c" in
	1 | 2 | 3 | 4) ;;
	*)
		echo "Error: case must be 1, 2, 3, or 4 (got: $c)" >&2
		exit 1
		;;
	esac
done

case_settings_for() {
	case "$1" in
	1) printf '%s\n' "SET disable_rpt = true;" "SET disable_tiered_hash_cache = true;" ;;
	2) printf '%s\n' "SET rpt_forward_only = true;" "SET disable_tiered_hash_cache = true;" ;;
	3) printf '%s\n' "SET rpt_forward_only = true;" ;;
	4) printf '%s\n' "SET disable_tiered_hash_cache = true;" ;;
	esac
}

SEEDS=()
SWEEPING_SEEDS=false
if [[ -n "$SEEDS_COUNT" ]]; then
	if ! [[ "$SEEDS_COUNT" =~ ^[0-9]+$ ]] || [[ "$SEEDS_COUNT" -lt 1 ]]; then
		echo "Error: --seeds must be a positive integer (got: $SEEDS_COUNT)" >&2
		exit 1
	fi
	for ((i = 0; i < SEEDS_COUNT; i++)); do SEEDS+=("$i"); done
	SWEEPING_SEEDS=true
elif [[ -n "$SEED" ]]; then
	if ! [[ "$SEED" =~ ^[0-9]+$ ]]; then
		echo "Error: --seed must be a non-negative integer (got: $SEED)" >&2
		exit 1
	fi
	SEEDS=("$SEED")
else
	# Empty marker: do not emit a seed override; use whatever settings-common.sql sets.
	SEEDS=("")
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JOB_DIR="$REPO_ROOT/join-order-benchmark"
COMMON_SETTINGS_SQL="$REPO_ROOT/scripts/measure/settings-common.sql"
RUN_SETTINGS_SQL="$REPO_ROOT/scripts/measure/settings-run_job.sql"
# When DuckDB profiling is on, the JSON for each query is written to its own
# path under PROFILING_DIR/ so it survives the next query's run; the
# benchmark post-processor walks those files to extract per-join THC
# telemetry. PROFILING_OUTPUT itself is set per query in the inner loop.
PROFILING_OUTPUT=""
PROFILING_DIR="$REPO_ROOT/results/job/profiling_$(date +%Y%m%d_%H%M%S)"
if $USE_DEBUG; then
	DUCKDB_BIN="$REPO_ROOT/build/debug/duckdb"
else
	DUCKDB_BIN="$REPO_ROOT/build/release/duckdb"
fi
DB_FILE="$JOB_DIR/job.db"

if [[ ! -f "$DB_FILE" ]]; then
	echo "Error: Database file $DB_FILE not found. Execute setup.sh first."
	exit 1
fi
if [[ ! -f "$COMMON_SETTINGS_SQL" ]]; then
	echo "Error: Common settings file not found: $COMMON_SETTINGS_SQL"
	exit 1
fi
if [[ ! -f "$RUN_SETTINGS_SQL" ]]; then
	echo "Error: Run-specific settings file not found: $RUN_SETTINGS_SQL"
	exit 1
fi

QUERY_FILES=()
if [[ -n "$JOB_QUERY" ]]; then
	JOB_QUERY="${JOB_QUERY%.sql}"
	TARGET_QUERY_FILE="$JOB_DIR/queries/${JOB_QUERY}.sql"
	if [[ ! -f "$TARGET_QUERY_FILE" ]]; then
		echo "Error: JOB query file not found: $TARGET_QUERY_FILE" >&2
		exit 1
	fi
	QUERY_FILES+=("queries/${JOB_QUERY}.sql")
elif [[ -n "$JOB_QUERIES_LIST" ]]; then
	IFS=',' read -r -a _JOB_QUERIES <<<"$JOB_QUERIES_LIST"
	for q in "${_JOB_QUERIES[@]}"; do
		q="${q%.sql}"
		TARGET_QUERY_FILE="$JOB_DIR/queries/${q}.sql"
		if [[ ! -f "$TARGET_QUERY_FILE" ]]; then
			echo "Error: JOB query file not found: $TARGET_QUERY_FILE" >&2
			exit 1
		fi
		QUERY_FILES+=("queries/${q}.sql")
	done
else
	while IFS= read -r q; do
		QUERY_FILES+=("${q#"$JOB_DIR/"}")
	done < <(printf "%s\n" "$JOB_DIR"/queries/*.sql | sort -V)
fi

# Decide whether to write a CSV. Sweep mode auto-names if --csv was not given;
# single-shot mode (one case, one query, no seed sweep) only writes a CSV
# when --csv is explicitly provided.
SWEEPING=false
if $SWEEPING_SEEDS || [[ ${#CASES[@]} -gt 1 ]] || [[ ${#QUERY_FILES[@]} -gt 1 ]] || [[ "$RUNS" -gt 1 ]]; then
	SWEEPING=true
fi
if $SWEEPING && [[ -z "$CSV_PATH" ]]; then
	mkdir -p "$REPO_ROOT/results/job"
	CSV_PATH="$REPO_ROOT/results/job/job_runtimes_$(date +%Y%m%d_%H%M%S).csv"
fi
if [[ -n "$CSV_PATH" ]]; then
	case "$CSV_PATH" in
	/*) ;;
	*) CSV_PATH="$(pwd)/$CSV_PATH" ;;
	esac
	mkdir -p "$(dirname "$CSV_PATH")"
	printf "query,case,seed,run_idx,runtime_seconds\n" >"$CSV_PATH"
fi

if $USE_DUCKDB_PROFILING; then
	mkdir -p "$PROFILING_DIR"
fi

build_sql() {
	local case_num="$1"
	local seed_val="$2"
	local query_file="$3"
	if $USE_DUCKDB_PROFILING && [[ -n "$PROFILING_OUTPUT" ]]; then
		printf "PRAGMA enable_profiling = 'json';\n"
		printf "PRAGMA profiling_output = '%s';\n" "$PROFILING_OUTPUT"
		printf "PRAGMA profiling_coverage = 'SELECT';\n"
	fi
	grep '^SET ' "$COMMON_SETTINGS_SQL" || true
	grep '^SET ' "$RUN_SETTINGS_SQL" || true
	case_settings_for "$case_num"
	if [[ -n "$seed_val" ]]; then
		printf 'SET transfer_graph_seed = %s;\n' "$seed_val"
	fi
	cat "$query_file"
}

cd "$JOB_DIR"

# DuckDB can reject a query because it cannot allocate memory or because the
# configured temp directory limit prevents spilling. Treat those as per-query
# benchmark misses, distinct from timeout misses, rather than losing a sweep.
is_recoverable_duckdb_resource_error() {
	local error_file="$1"
	grep -Eiq 'Out of Memory Error|failed to offload data block|max_temp_directory_size' "$error_file"
}

process_group_alive() {
	local pid="$1"
	kill -0 -- "-$pid" 2>/dev/null || kill -0 "$pid" 2>/dev/null
}

terminate_process_group() {
	local pid="$1"
	local grace_seconds=5
	local waited=0

	kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
	while process_group_alive "$pid" && [[ "$waited" -lt "$grace_seconds" ]]; do
		sleep 1
		waited=$((waited + 1))
	done
	if process_group_alive "$pid"; then
		kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
	fi
}

run_query() {
	local case_num="$1"
	local seed_val="$2"
	local query_file="$3"
	local sql sql_file error_file cmd_pid runtime rc timed_out start_time end_time deadline
	sql="$(build_sql "$case_num" "$seed_val" "$query_file")"
	sql_file=$(mktemp)
	error_file=$(mktemp)
	rc=0
	timed_out=0
	printf '%s\n' "$sql" >"$sql_file"
	drop_os_page_cache
	start_time=$(date +%s.%N)
	# Run DuckDB in its own process group so timeout cleanup also kills any
	# DuckDB child that would otherwise keep the database lock alive.
	if $USE_PERF; then
		setsid bash -c 'exec sudo perf stat -e "$1" -- "$2" "$3" < "$4" 2>"$5"' \
			_ "$PERF_EVENTS" "$DUCKDB_BIN" "$DB_FILE" "$sql_file" "$error_file" &
	else
		setsid bash -c 'exec "$1" "$2" < "$3" 2>"$4"' \
			_ "$DUCKDB_BIN" "$DB_FILE" "$sql_file" "$error_file" &
	fi
	cmd_pid=$!
	if [[ -n "$TIMEOUT_SECONDS" ]]; then
		deadline=$(($(date +%s) + TIMEOUT_SECONDS))
		while process_group_alive "$cmd_pid"; do
			if [[ "$(date +%s)" -ge "$deadline" ]]; then
				timed_out=1
				break
			fi
			sleep 0.1
		done
	fi
	if [[ "$timed_out" -eq 1 ]]; then
		terminate_process_group "$cmd_pid"
		wait "$cmd_pid" 2>/dev/null || true
		rc=124
	else
		wait "$cmd_pid" || rc=$?
	fi
	end_time=$(date +%s.%N)
	if [[ -s "$error_file" ]]; then
		cat "$error_file" >&2
	fi
	if [[ "$rc" -eq 0 ]]; then
		runtime=$(awk -v s="$start_time" -v e="$end_time" 'BEGIN{printf "%.2f", e - s}')
	elif [[ "$rc" -eq 124 || "$rc" -eq 137 ]]; then
		echo "Warning: query ${query_file} timed out after ${TIMEOUT_SECONDS}s (case ${case_num}, seed ${seed_val:-default}); recording runtime 9999999" >&2
		runtime=9999999
	elif is_recoverable_duckdb_resource_error "$error_file"; then
		echo "Warning: query ${query_file} hit DuckDB OOM/temp-spill limit (case ${case_num}, seed ${seed_val:-default}); recording runtime 8888888" >&2
		runtime=8888888
	else
		rm -f "$sql_file" "$error_file"
		echo "Error: query ${query_file} failed (case ${case_num}, seed ${seed_val:-default})" >&2
		exit 1
	fi
	rm -f "$sql_file" "$error_file"
	LAST_RUNTIME="$runtime"
}

drop_os_page_cache() {
	if ! $DROP_OS_CACHE; then
		return
	fi
	# Linux page cache is process-external, so a fresh DuckDB process is not
	# enough for cold I/O measurements. This is intentionally opt-in because it
	# requires sudo and disrupts every workload on the host.
	echo "Dropping Linux page cache before measured DuckDB query..." >&2
	sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
}

if [[ -n "$JOB_QUERY" ]]; then
	echo "Starting Join Order Benchmark execution (cases: ${CASES[*]}, seeds: ${SEEDS[*]:-default}, runs/tuple: ${RUNS}, query: ${JOB_QUERY})..."
else
	echo "Starting Join Order Benchmark execution (cases: ${CASES[*]}, seeds: ${SEEDS[*]:-default}, runs/tuple: ${RUNS})..."
fi

TOTAL_RUNTIME=0
ROW_COUNT=0
for c in "${CASES[@]}"; do
	for s in "${SEEDS[@]}"; do
		for RUN_IDX in $(seq 1 "$RUNS"); do
			for q in "${QUERY_FILES[@]}"; do
				query_name="${q#queries/}"
				query_name="${query_name%.sql}"
				seed_for_path="${s:-default}"
				if $USE_DUCKDB_PROFILING; then
					PROFILING_OUTPUT="$PROFILING_DIR/job_q${query_name}_case${c}_seed${seed_for_path}_run${RUN_IDX}.json"
				else
					PROFILING_OUTPUT=""
				fi
				echo "Executing case=${c} seed=${s:-default} run=${RUN_IDX}/${RUNS} query=${query_name}..."
				run_query "$c" "$s" "$q"
				echo "  runtime: ${LAST_RUNTIME} s"
				if [[ -n "$CSV_PATH" ]]; then
					printf '%s,%s,%s,%s,%s\n' "$query_name" "$c" "$s" "$RUN_IDX" "$LAST_RUNTIME" >>"$CSV_PATH"
				fi
				TOTAL_RUNTIME=$(awk -v t="$TOTAL_RUNTIME" -v r="$LAST_RUNTIME" 'BEGIN{printf "%.6f", t + r}')
				ROW_COUNT=$((ROW_COUNT + 1))
			done
		done
	done
done

echo "Benchmark execution complete."
echo "Completed ${ROW_COUNT} run(s)."
echo "Total wall clock time across runs: ${TOTAL_RUNTIME} seconds"
if [[ -n "$CSV_PATH" ]]; then
	echo "CSV written to: $CSV_PATH"
fi
if $USE_DUCKDB_PROFILING; then
	echo "DuckDB profiling output written to: $PROFILING_DIR/"
	POSTPROCESS="$SCRIPT_DIR/thc_csv_postprocess.py"
	if [[ -n "$CSV_PATH" ]] && [[ -f "$POSTPROCESS" ]] && command -v python3 >/dev/null; then
		python3 "$POSTPROCESS" --csv "$CSV_PATH" --profiling-dir "$PROFILING_DIR" --prefix job || \
			echo "warning: thc_csv_postprocess failed for $CSV_PATH" >&2
	fi
fi
# Condense the runtime CSV to one (median) row per (query, case). Runs after the
# THC postprocess above so the median CSV also carries any Join*-* columns.
MEDIAN_SCRIPT="$SCRIPT_DIR/median_runtime_csv.py"
if [[ -n "$CSV_PATH" ]] && [[ -f "$MEDIAN_SCRIPT" ]] && command -v python3 >/dev/null; then
	python3 "$MEDIAN_SCRIPT" --csv "$CSV_PATH" || \
		echo "warning: median_runtime_csv failed for $CSV_PATH" >&2
fi
BOXPLOT_SCRIPT="$SCRIPT_DIR/plot_runtime_boxplots.py"
if $CREATE_BOXPLOTS; then
	if [[ -z "$CSV_PATH" ]]; then
		echo "warning: --create-boxplots requested, but no runtime CSV was written" >&2
	elif [[ -f "$BOXPLOT_SCRIPT" ]] && command -v python3 >/dev/null; then
		python3 "$BOXPLOT_SCRIPT" --csv "$CSV_PATH" || \
			echo "warning: plot_runtime_boxplots failed for $CSV_PATH" >&2
	else
		echo "warning: cannot create boxplots because $BOXPLOT_SCRIPT or python3 is missing" >&2
	fi
fi
