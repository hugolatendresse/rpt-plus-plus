#!/usr/bin/env bash
# Allows running the Appian benchmark
# Usage:
# scripts/measure/run_appian.sh --case <1|2|3|4> [--perf] [--debug] [--query NN]
# Sweep example:
# scripts/measure/run_appian.sh --cases 2,3,4 --seeds 5 [--query 3] [--csv path]
set -euo pipefail

CASE=""
CASES_LIST=""
APPIAN_QUERY=""
APPIAN_QUERIES_LIST=""
SEED=""
SEEDS_COUNT=""
CSV_PATH=""
USE_PERF=false
USE_DEBUG=false
USE_DUCKDB_PROFILING=false
GENERATE_DATA=false
PERF_EVENTS="cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,mem_access,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd,branch-instructions,branch-misses,br_retired,br_mis_pred_retired"

usage() {
	cat <<'USAGE'
Usage: scripts/measure/run_appian.sh (--case <1|2|3|4> | --cases <list>) [options]

Options:
  --case <1|2|3|4>      Optimizer case (mutually exclusive with --cases)
  --cases <list>        Comma-separated case list, e.g. 2,3,4
  --query <id>          Run one Appian query (e.g. 3, 03, or q03.sql)
  --queries <list>      Comma-separated Appian query list, e.g. 1,2,3
  --seed <int>          Override transfer_graph_seed (mutually exclusive with --seeds)
  --seeds <N>           Sweep seeds 0..N-1 (overrides transfer_graph_seed)
  --csv <path>          Write per-run CSV (auto-named if omitted in sweep mode)
  --perf                Run query/queries under perf stat
  --debug               Use debug build (build/debug/duckdb)
  --duckdb-profiling    Enable DuckDB JSON profiling, output to results/appian/appian.json
  --generate            Force (re)download of the Appian database
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
	--query)
		APPIAN_QUERY="$2"
		shift 2
		;;
	--queries)
		APPIAN_QUERIES_LIST="$2"
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
	--generate)
		GENERATE_DATA=true
		shift
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
if [[ -n "$APPIAN_QUERY" && -n "$APPIAN_QUERIES_LIST" ]]; then
	echo "Error: --query and --queries are mutually exclusive." >&2
	exit 1
fi
if [[ -z "$CASE" && -z "$CASES_LIST" ]]; then
	echo "Error: --case or --cases is required." >&2
	exit 1
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
APPIAN_QUERIES_DIR="$REPO_ROOT/benchmark/appian_benchmarks/queries"
COMMON_SETTINGS_SQL="$REPO_ROOT/scripts/measure/settings-common.sql"
RUN_SETTINGS_SQL="$REPO_ROOT/scripts/measure/settings-run_appian.sql"
# Absolute path so the file is easy to locate regardless of the script's CWD.
PROFILING_OUTPUT="$REPO_ROOT/results/appian/appian.json"
# See https://duckdb.org/docs/stable/dev/profiling
PROFILING_PRAGMAS="PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = '$PROFILING_OUTPUT';
PRAGMA profiling_coverage = 'SELECT';"
if $USE_DUCKDB_PROFILING; then
	mkdir -p "$(dirname "$PROFILING_OUTPUT")"
fi
if $USE_DEBUG; then
	DUCKDB_BIN="$REPO_ROOT/build/debug/duckdb"
else
	DUCKDB_BIN="$REPO_ROOT/build/release/duckdb"
fi
# Mirror the TPC convention: local benchmark databases live in ../benchmark_data/.
DB_DIR="$REPO_ROOT/../benchmark_data/appian"
DB_FILE="$DB_DIR/appian.duckdb"
# Note: the S3-style URL in benchmark/appian_benchmarks/appian.benchmark.in
# (`duckdb-blobs.s3.amazonaws.com`) returns 403. The live public mirror is
# `blobs.duckdb.org`.
APPIAN_REMOTE_DB="https://blobs.duckdb.org/data/appian_benchmark_data.duckdb"

# Tables to copy from the remote Appian DB on first-time setup. Mirrors the
# `load` block in benchmark/appian_benchmarks/appian.benchmark.in.
APPIAN_TABLES=(
	AddressView
	CustomerView
	OrderView
	CategoryView
	OrderItemNovelty_Update
	ProductView
	CreditCardView
	OrderItemView
	TaxRecordView
)

setup_db() {
	# One-time download of the Appian DB. The remote URL is already a DuckDB
	# database with the expected tables in `main`, so we just curl it straight
	# into $DB_FILE rather than ATTACH-and-copy (which is slower and was hitting
	# httpfs lock races). Download into `.partial` and rename on success so a
	# failed download doesn't leave a usable-looking stub.
	echo "Downloading Appian DB from $APPIAN_REMOTE_DB to $DB_FILE ..."
	mkdir -p "$DB_DIR"
	local partial="${DB_FILE}.partial"
	rm -f "$partial"
	if ! curl --fail --location --show-error --progress-bar -o "$partial" "$APPIAN_REMOTE_DB"; then
		echo "Error: failed to download $APPIAN_REMOTE_DB" >&2
		rm -f "$partial"
		exit 1
	fi
	# Sanity-check: the downloaded file should contain the expected tables.
	local table_count
	table_count=$("$DUCKDB_BIN" -readonly -noheader -list "$partial" \
		-c "SELECT count(*) FROM duckdb_tables() WHERE table_name IN ('AddressView','CustomerView','OrderView','CategoryView','OrderItemNovelty_Update','ProductView','CreditCardView','OrderItemView','TaxRecordView');" 2>/dev/null || echo 0)
	if [[ "$table_count" != "${#APPIAN_TABLES[@]}" ]]; then
		echo "Error: downloaded Appian DB is missing tables (got $table_count/${#APPIAN_TABLES[@]}). Leaving partial at $partial for inspection." >&2
		exit 1
	fi
	mv "$partial" "$DB_FILE"
	echo "Appian DB ready at $DB_FILE."
}

if $GENERATE_DATA; then
	rm -f "$DB_FILE"
fi
if [[ ! -f "$DB_FILE" ]]; then
	setup_db
fi
if [[ ! -f "$COMMON_SETTINGS_SQL" ]]; then
	echo "Error: Common settings file not found: $COMMON_SETTINGS_SQL"
	exit 1
fi
if [[ ! -f "$RUN_SETTINGS_SQL" ]]; then
	echo "Error: Run-specific settings file not found: $RUN_SETTINGS_SQL"
	exit 1
fi

# Normalize an Appian query id into the zero-padded "qNN" basename used on disk.
normalize_query_id() {
	local q="$1"
	q="${q%.sql}"
	q="${q#q}"
	# Strip leading zeros so we can re-pad to width 2.
	q="$((10#$q))"
	printf 'q%02d' "$q"
}

QUERY_FILES=()
if [[ -n "$APPIAN_QUERY" ]]; then
	normalized="$(normalize_query_id "$APPIAN_QUERY")"
	TARGET_QUERY_FILE="$APPIAN_QUERIES_DIR/${normalized}.sql"
	if [[ ! -f "$TARGET_QUERY_FILE" ]]; then
		echo "Error: Appian query file not found: $TARGET_QUERY_FILE" >&2
		exit 1
	fi
	QUERY_FILES+=("queries/${normalized}.sql")
elif [[ -n "$APPIAN_QUERIES_LIST" ]]; then
	IFS=',' read -r -a _APPIAN_QUERIES <<<"$APPIAN_QUERIES_LIST"
	for q in "${_APPIAN_QUERIES[@]}"; do
		normalized="$(normalize_query_id "$q")"
		TARGET_QUERY_FILE="$APPIAN_QUERIES_DIR/${normalized}.sql"
		if [[ ! -f "$TARGET_QUERY_FILE" ]]; then
			echo "Error: Appian query file not found: $TARGET_QUERY_FILE" >&2
			exit 1
		fi
		QUERY_FILES+=("queries/${normalized}.sql")
	done
else
	while IFS= read -r q; do
		QUERY_FILES+=("queries/$(basename "$q")")
	done < <(printf "%s\n" "$APPIAN_QUERIES_DIR"/*.sql | sort -V)
fi

# Decide whether to write a CSV. Sweep mode auto-names if --csv was not given;
# single-shot mode (one case, one query, no seed sweep) only writes a CSV
# when --csv is explicitly provided.
SWEEPING=false
if $SWEEPING_SEEDS || [[ ${#CASES[@]} -gt 1 ]] || [[ ${#QUERY_FILES[@]} -gt 1 ]]; then
	SWEEPING=true
fi
if $SWEEPING && [[ -z "$CSV_PATH" ]]; then
	mkdir -p "$REPO_ROOT/results/appian"
	CSV_PATH="$REPO_ROOT/results/appian/appian_runtimes_$(date +%Y%m%d_%H%M%S).csv"
fi
if [[ -n "$CSV_PATH" ]]; then
	mkdir -p "$(dirname "$CSV_PATH")"
	printf "query,case,seed,runtime_seconds\n" >"$CSV_PATH"
fi

build_sql() {
	local case_num="$1"
	local seed_val="$2"
	local query_file="$3"
	if $USE_DUCKDB_PROFILING; then printf '%s\n' "$PROFILING_PRAGMAS"; fi
	grep '^SET ' "$COMMON_SETTINGS_SQL" || true
	grep '^SET ' "$RUN_SETTINGS_SQL" || true
	case_settings_for "$case_num"
	if [[ -n "$seed_val" ]]; then
		printf 'SET transfer_graph_seed = %s;\n' "$seed_val"
	fi
	cat "$query_file"
}

# Match run_job.sh: cd into the benchmark dir so relative query paths resolve.
cd "$REPO_ROOT/benchmark/appian_benchmarks"

run_query() {
	local case_num="$1"
	local seed_val="$2"
	local query_file="$3"
	local sql time_file runtime
	sql="$(build_sql "$case_num" "$seed_val" "$query_file")"
	time_file=$(mktemp)
	if $USE_PERF; then
		if /usr/bin/time -f "%e" -o "$time_file" bash -c \
			'printf "%s\n" "$1" | sudo perf stat -e "$2" -- "$3" "$4"' \
			_ "$sql" "$PERF_EVENTS" "$DUCKDB_BIN" "$DB_FILE"; then
			runtime=$(awk 'NR==1{print $1}' "$time_file")
		else
			rm -f "$time_file"
			echo "Error: query ${query_file} failed (case ${case_num}, seed ${seed_val:-default})" >&2
			exit 1
		fi
	else
		if /usr/bin/time -f "%e" -o "$time_file" bash -c \
			'printf "%s\n" "$1" | "$2" "$3"' \
			_ "$sql" "$DUCKDB_BIN" "$DB_FILE"; then
			runtime=$(awk 'NR==1{print $1}' "$time_file")
		else
			rm -f "$time_file"
			echo "Error: query ${query_file} failed (case ${case_num}, seed ${seed_val:-default})" >&2
			exit 1
		fi
	fi
	rm -f "$time_file"
	LAST_RUNTIME="$runtime"
}

if [[ -n "$APPIAN_QUERY" ]]; then
	echo "Starting Appian Benchmark execution (cases: ${CASES[*]}, seeds: ${SEEDS[*]:-default}, query: ${APPIAN_QUERY})..."
else
	echo "Starting Appian Benchmark execution (cases: ${CASES[*]}, seeds: ${SEEDS[*]:-default})..."
fi

TOTAL_RUNTIME=0
ROW_COUNT=0
for c in "${CASES[@]}"; do
	for s in "${SEEDS[@]}"; do
		for q in "${QUERY_FILES[@]}"; do
			query_name="${q#queries/}"
			query_name="${query_name%.sql}"
			echo "Executing case=${c} seed=${s:-default} query=${query_name}..."
			run_query "$c" "$s" "$q"
			echo "  runtime: ${LAST_RUNTIME} s"
			if [[ -n "$CSV_PATH" ]]; then
				printf '%s,%s,%s,%s\n' "$query_name" "$c" "$s" "$LAST_RUNTIME" >>"$CSV_PATH"
			fi
			TOTAL_RUNTIME=$(awk -v t="$TOTAL_RUNTIME" -v r="$LAST_RUNTIME" 'BEGIN{printf "%.6f", t + r}')
			ROW_COUNT=$((ROW_COUNT + 1))
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
	echo "DuckDB profiling output written to: $PROFILING_OUTPUT"
fi
