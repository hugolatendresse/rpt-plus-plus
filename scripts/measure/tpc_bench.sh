#!/usr/bin/env bash
set -euo pipefail

# Default settings; can be overridden via flags.
SF=100
DUCKDB_BIN="./build/release/duckdb"
GENERATE_DATA=0
OUT_DIR="./tpch_results"
RUN_TPCH=1
RUN_TPCDS=1
DB_BASE_PATH=""
RPT_FORWARD_ONLY=0
DISABLE_RPT=0
DISABLE_TIERED_HASH_CACHE=0
TPCH_QUERY=""
RUNS=1
BENCH_THREADS=4
PIN_THREADS="on"
THC_L3_BUDGET=4194304
THC_COLLECT_PHASE_ROWS=100000
THC_COLLECT_BUDGET_FRACTION=0.02
THC_MISS_THRESHOLD=0.05
THC_ACTIVATION_THRESHOLD=500000

# Track which options were explicitly passed
PASSED_OPTIONS=()

usage() {
	cat <<'USAGE'
Usage: scripts/measure/tpc_bench.sh [options]

Options:
	--sf <scale_factor>     Scale factor for dbgen (default: 100)
	--db <db_base_path>     Base path for databases (default: ../benchmark_data)
	--duckdb <bin_path>     DuckDB CLI binary (default: ./build/release/duckdb)
	--generate              Generate TPC-H/TPC-DS data with dbgen (default assumes they already exist)
	--out-dir <dir>         Output directory for results (default: ./tpch_results)
	--tpch-only             Run only TPC-H (default: run both TPC-H and TPC-DS)
	--tpcds-only            Run only TPC-DS (default: run both TPC-H and TPC-DS)
	--tpch-query <number>   Run only a specific TPC-H query (1-22, implies --tpch-only)
	--runs <number>         Number of benchmark runs (default: 1)
	--threads <number>      Value for SET threads (default: 4)
	--pin-threads <mode>    Value for SET pin_threads (default: on)
	--thc-l3-budget <num>   Value for SET thc_l3_budget (default: 4194304)
	--thc-collect-phase-rows <num>
	                        Value for SET thc_collect_phase_rows (default: 100000)
	--thc-collect-budget-fraction <num>
	                        Value for SET thc_collect_budget_fraction (default: 0.02)
	--thc-miss-threshold <num>
	                        Value for SET thc_miss_threshold (default: 0.05)
	--thc-activation-threshold <num>
	                        Value for SET thc_activation_threshold (default: 500000)
	--rpt-forward-only      Disable the RPT backward pass (do forward pass only)
	--disable-rpt           Disable both the RPT forward and backward pass
	--disable-thc           Disable the tiered hash cache
	-h, --help              Show this help

Examples:
	scripts/run_TPC_bench.sh --db ./data --sf 1
	scripts/run_TPC_bench.sh --generate --sf 10
	scripts/run_TPC_bench.sh --tpch-only --sf 5
	scripts/run_TPC_bench.sh --tpch-query 5 --sf 500
	scripts/run_TPC_bench.sh --tpch-query 5 --sf 10 --runs 5
	scripts/run_TPC_bench.sh --tpcds-only --sf 10
	scripts/run_TPC_bench.sh --disable-thc --tpch-only --sf 10
USAGE
}

# First we build
GEN=ninja BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 BUILD_HTTPFS=1 CORE_EXTENSIONS='tpch' make release -j 7

while [[ $# -gt 0 ]]; do
	case "$1" in
		--sf)
			SF="$2"
			PASSED_OPTIONS+=("--sf $2")
			shift 2
			;;
		--db)
			DB_BASE_PATH="$2"
			PASSED_OPTIONS+=("--db $2")
			shift 2
			;;
		--duckdb)
			DUCKDB_BIN="$2"
			PASSED_OPTIONS+=("--duckdb $2")
			shift 2
			;;
		--generate)
			GENERATE_DATA=1
			PASSED_OPTIONS+=("--generate")
			shift
			;;
		--out-dir)
			OUT_DIR="$2"
			PASSED_OPTIONS+=("--out-dir $2")
			shift 2
			;;
		--tpch-only)
			RUN_TPCH=1
			RUN_TPCDS=0
			PASSED_OPTIONS+=("--tpch-only")
			shift
			;;
		--tpcds-only)
			RUN_TPCH=0
			RUN_TPCDS=1
			PASSED_OPTIONS+=("--tpcds-only")
			shift
			;;
		--tpch-query)
			TPCH_QUERY="$2"
			RUN_TPCH=1
			RUN_TPCDS=0
			PASSED_OPTIONS+=("--tpch-query $2")
			shift 2
			;;
		--runs)
			RUNS="$2"
			PASSED_OPTIONS+=("--runs $2")
			shift 2
			;;
		--threads)
			BENCH_THREADS="$2"
			PASSED_OPTIONS+=("--threads $2")
			shift 2
			;;
		--pin-threads)
			PIN_THREADS="$2"
			PASSED_OPTIONS+=("--pin-threads $2")
			shift 2
			;;
		--thc-l3-budget)
			THC_L3_BUDGET="$2"
			PASSED_OPTIONS+=("--thc-l3-budget $2")
			shift 2
			;;
		--thc-collect-phase-rows)
			THC_COLLECT_PHASE_ROWS="$2"
			PASSED_OPTIONS+=("--thc-collect-phase-rows $2")
			shift 2
			;;
		--thc-collect-budget-fraction)
			THC_COLLECT_BUDGET_FRACTION="$2"
			PASSED_OPTIONS+=("--thc-collect-budget-fraction $2")
			shift 2
			;;
		--thc-miss-threshold)
			THC_MISS_THRESHOLD="$2"
			PASSED_OPTIONS+=("--thc-miss-threshold $2")
			shift 2
			;;
		--thc-activation-threshold)
			THC_ACTIVATION_THRESHOLD="$2"
			PASSED_OPTIONS+=("--thc-activation-threshold $2")
			shift 2
			;;
		--rpt-forward-only)
			RPT_FORWARD_ONLY=1
			PASSED_OPTIONS+=("--rpt-forward-only")
			shift
			;;
		--disable-rpt)
			DISABLE_RPT=1
			PASSED_OPTIONS+=("--disable-rpt")
			shift
			;;
		--disable-thc)
			DISABLE_TIERED_HASH_CACHE=1
			PASSED_OPTIONS+=("--disable-thc")
			shift
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			echo "Unknown argument: $1" >&2
			usage
			exit 1
			;;
	esac
done

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
	echo "Error: --runs must be a positive integer" >&2
	exit 1
fi

if ! [[ "$BENCH_THREADS" =~ ^[0-9]+$ ]] || [[ "$BENCH_THREADS" -lt 1 ]]; then
	echo "Error: --threads must be a positive integer" >&2
	exit 1
fi

case "$PIN_THREADS" in
	on|off|auto)
		;;
	*)
		echo "Error: --pin-threads must be one of: on, off, auto" >&2
		exit 1
		;;
esac

if [[ -z "$DB_BASE_PATH" ]]; then
	DB_BASE_PATH="../benchmark_data"
fi

TPCH_DB_PATH="${DB_BASE_PATH}/tpch/tpch_sf${SF}.duckdb"
TPCDS_DB_PATH="${DB_BASE_PATH}/tpcds/tpcds_sf${SF}.duckdb"

if [[ ! -x "$DUCKDB_BIN" ]]; then
	echo "DuckDB binary not found or not executable: $DUCKDB_BIN" >&2
	echo "Build it or pass --duckdb <path>." >&2
	exit 1
fi

mkdir -p "$OUT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Build optional SET prefix
# For reproducible benchmarks, always use four threads and pinned them 
EXTRA_SET="SET threads = ${BENCH_THREADS}; SET pin_threads = '${PIN_THREADS}'; SET thc_l3_budget = ${THC_L3_BUDGET}; SET thc_collect_phase_rows = ${THC_COLLECT_PHASE_ROWS}; SET thc_collect_budget_fraction = ${THC_COLLECT_BUDGET_FRACTION}; SET thc_miss_threshold = ${THC_MISS_THRESHOLD}; SET thc_activation_threshold = ${THC_ACTIVATION_THRESHOLD};"
if [[ $DISABLE_RPT -eq 1 ]]; then
	EXTRA_SET="${EXTRA_SET} SET disable_rpt = true;"
fi
if [[ $RPT_FORWARD_ONLY -eq 1 ]]; then
	EXTRA_SET="${EXTRA_SET} SET rpt_forward_only = true;"
fi
if [[ $DISABLE_TIERED_HASH_CACHE -eq 1 ]]; then
	EXTRA_SET="${EXTRA_SET} SET disable_tiered_hash_cache = true;"
fi

TPCH_CSV_PATH="$OUT_DIR/tpch_runtimes_sf${SF}_${TIMESTAMP}.csv"
TPCH_TXT_PATH="$OUT_DIR/tpch_runtimes_sf${SF}_${TIMESTAMP}.txt"

TPCDS_CSV_PATH="$OUT_DIR/tpcds_runtimes_sf${SF}_${TIMESTAMP}.csv"
TPCDS_TXT_PATH="$OUT_DIR/tpcds_runtimes_sf${SF}_${TIMESTAMP}.txt"

COMBINED_TXT_PATH="$OUT_DIR/combined_runtimes_sf${SF}_${TIMESTAMP}.txt"

DBGEN_LOG="$OUT_DIR/dbgen_sf${SF}_${TIMESTAMP}.log"

# ===== Validate database files exist (unless generating) =====

if [[ $RUN_TPCH -eq 1 ]] && [[ $GENERATE_DATA -eq 0 ]]; then
	if [[ ! -f "$TPCH_DB_PATH" ]]; then
		echo "Error: TPC-H database not found at ${TPCH_DB_PATH}" >&2
		echo "Generate it with --generate or specify a different path with --db." >&2
		exit 1
	fi
fi

if [[ $RUN_TPCDS -eq 1 ]] && [[ $GENERATE_DATA -eq 0 ]]; then
	if [[ ! -f "$TPCDS_DB_PATH" ]]; then
		echo "Error: TPC-DS database not found at ${TPCDS_DB_PATH}" >&2
		echo "Generate it with --generate or specify a different path with --db." >&2
		exit 1
	fi
fi

# ===== TPC-H Data Generation and Execution =====

if [[ $RUN_TPCH -eq 1 ]] && [[ $GENERATE_DATA -eq 1 ]]; then
	mkdir -p "$(dirname "$TPCH_DB_PATH")"
	echo "Generating TPC-H data (sf=${SF}) into ${TPCH_DB_PATH}..."

	# Prefer a simple LOAD; if it fails, attempt INSTALL + LOAD (for non-bundled builds).
	if ! "$DUCKDB_BIN" "$TPCH_DB_PATH" -c "LOAD tpch;" > "$DBGEN_LOG" 2>&1; then
		"$DUCKDB_BIN" "$TPCH_DB_PATH" <<SQL >> "$DBGEN_LOG" 2>&1
INSTALL tpch;
LOAD tpch;
SQL
	fi

	"$DUCKDB_BIN" "$TPCH_DB_PATH" <<SQL >> "$DBGEN_LOG" 2>&1
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS supplier;
CALL dbgen(sf = ${SF});
SQL

	if ! "$DUCKDB_BIN" "$TPCH_DB_PATH" -c "SELECT COUNT(*) FROM lineitem;" >> "$DBGEN_LOG" 2>&1; then
		echo "dbgen did not create TPC-H tables. See: ${DBGEN_LOG}" >&2
		exit 1
	fi
fi

# ===== TPC-DS Data Generation =====

if [[ $RUN_TPCDS -eq 1 ]] && [[ $GENERATE_DATA -eq 1 ]]; then
	mkdir -p "$(dirname "$TPCDS_DB_PATH")"
	echo "Generating TPC-DS data (sf=${SF}) into ${TPCDS_DB_PATH}..."

	# Prefer a simple LOAD; if it fails, attempt INSTALL + LOAD (for non-bundled builds).
	if ! "$DUCKDB_BIN" "$TPCDS_DB_PATH" -c "LOAD tpcds;" > "$DBGEN_LOG" 2>&1; then
		"$DUCKDB_BIN" "$TPCDS_DB_PATH" <<SQL >> "$DBGEN_LOG" 2>&1
INSTALL tpcds;
LOAD tpcds;
SQL
	fi

	"$DUCKDB_BIN" "$TPCDS_DB_PATH" <<SQL >> "$DBGEN_LOG" 2>&1
LOAD tpcds;
CALL dsdgen(sf = ${SF});
SQL

	if ! "$DUCKDB_BIN" "$TPCDS_DB_PATH" -c "SELECT COUNT(*) FROM store;" >> "$DBGEN_LOG" 2>&1; then
		echo "dsdgen did not create TPC-DS tables. See: ${DBGEN_LOG}" >&2
		exit 1
	fi
fi

# ===== Query Execution (supports multiple runs) =====

MULTIRUN_TOTAL_SECONDS=0

for RUN_IDX in $(seq 1 "$RUNS"); do
	echo "===== RUN ${RUN_IDX}/${RUNS} ====="
	RUN_START_WALL=$(date +%s.%N)

	TPCH_TOTAL=0
	TPCH_WALL_SECONDS=0
	TPCDS_TOTAL=0
	TPCDS_WALL_SECONDS=0

	# ===== TPC-H Query Execution =====
	if [[ $RUN_TPCH -eq 1 ]]; then
		printf "query,runtime_seconds\n" > "$TPCH_CSV_PATH"

		TPCH_START_WALL=$(date +%s.%N)

		# Determine which queries to run
		if [[ -n "$TPCH_QUERY" ]]; then
			if ! [[ "$TPCH_QUERY" =~ ^[0-9]+$ ]] || [[ "$TPCH_QUERY" -lt 1 ]] || [[ "$TPCH_QUERY" -gt 22 ]]; then
				echo "Error: --tpch-query must be between 1 and 22" >&2
				exit 1
			fi
			QUERY_RANGE="$TPCH_QUERY"
		else
			QUERY_RANGE=$(seq 1 22)
		fi

		for Q in $QUERY_RANGE; do
			echo "Running TPC-H query ${Q}..."
			TIME_FILE=$(mktemp)
			ERROR_FILE=$(mktemp)
			# Show output only when running a single specific query via --tpch-query
			OUTPUT_REDIRECT="/dev/null"
			if [[ -n "$TPCH_QUERY" ]]; then
				OUTPUT_REDIRECT="/dev/stdout"
			fi
			if /usr/bin/time -f "%e" -o "$TIME_FILE" \
				"$DUCKDB_BIN" "$TPCH_DB_PATH" -c "${EXTRA_SET} LOAD tpch; PRAGMA tpch(${Q});" > "$OUTPUT_REDIRECT" 2>"$ERROR_FILE"; then
				RUNTIME=$(cat "$TIME_FILE")
			else
				EXIT_CODE=$?
				echo "Error: TPC-H query ${Q} failed with exit code ${EXIT_CODE}" >&2
				echo "Error output:" >&2
				cat "$ERROR_FILE" >&2
				ERROR_LOG="$OUT_DIR/tpch_q${Q}_error_${TIMESTAMP}.log"
				cp "$ERROR_FILE" "$ERROR_LOG"
				echo "Full error saved to: ${ERROR_LOG}" >&2
				rm -f "$TIME_FILE" "$ERROR_FILE"
				exit 1
			fi
			rm -f "$TIME_FILE" "$ERROR_FILE"
			printf "Q%02d,%s\n" "$Q" "$RUNTIME" >> "$TPCH_CSV_PATH"
			if [[ "$RUNTIME" != "error" ]]; then
				TPCH_TOTAL=$(awk -v t="$TPCH_TOTAL" -v r="$RUNTIME" 'BEGIN{printf "%.6f", t + r}')
			fi
		done

		TPCH_END_WALL=$(date +%s.%N)
		TPCH_WALL_SECONDS=$(awk -v s="$TPCH_START_WALL" -v e="$TPCH_END_WALL" 'BEGIN{printf "%.6f", e - s}')

		{
			echo "TPC-H runtimes (sf=${SF}, run=${RUN_IDX})"
			echo "DB: ${TPCH_DB_PATH}"
			echo "DuckDB: ${DUCKDB_BIN}"
			echo "Results CSV: ${TPCH_CSV_PATH}"
			echo "Sum of per-query runtimes (s): ${TPCH_TOTAL}"
			echo "Wall-clock time for query loop (s): ${TPCH_WALL_SECONDS}"
		} | tee "$TPCH_TXT_PATH"
	fi

	# ===== TPC-DS Query Execution =====
	if [[ $RUN_TPCDS -eq 1 ]]; then
		printf "query,runtime_seconds\n" > "$TPCDS_CSV_PATH"

		TPCDS_START_WALL=$(date +%s.%N)

		for Q in $(seq 1 99); do
			echo "Running TPC-DS query ${Q}..."
			TIME_FILE=$(mktemp)
			ERROR_FILE=$(mktemp)
			if /usr/bin/time -f "%e" -o "$TIME_FILE" \
				"$DUCKDB_BIN" "$TPCDS_DB_PATH" -c "${EXTRA_SET} LOAD tpcds; PRAGMA tpcds(${Q});" > /dev/null 2>"$ERROR_FILE"; then
				RUNTIME=$(cat "$TIME_FILE")
			else
				EXIT_CODE=$?
				echo "Error: TPC-DS query ${Q} failed with exit code ${EXIT_CODE}" >&2
				echo "Error output:" >&2
				cat "$ERROR_FILE" >&2
				ERROR_LOG="$OUT_DIR/tpcds_q${Q}_error_${TIMESTAMP}.log"
				cp "$ERROR_FILE" "$ERROR_LOG"
				echo "Full error saved to: ${ERROR_LOG}" >&2
				rm -f "$TIME_FILE" "$ERROR_FILE"
				exit 1
			fi
			rm -f "$TIME_FILE" "$ERROR_FILE"
			printf "Q%02d,%s\n" "$Q" "$RUNTIME" >> "$TPCDS_CSV_PATH"
			if [[ "$RUNTIME" != "error" ]]; then
				TPCDS_TOTAL=$(awk -v t="$TPCDS_TOTAL" -v r="$RUNTIME" 'BEGIN{printf "%.6f", t + r}')
			fi
		done

		TPCDS_END_WALL=$(date +%s.%N)
		TPCDS_WALL_SECONDS=$(awk -v s="$TPCDS_START_WALL" -v e="$TPCDS_END_WALL" 'BEGIN{printf "%.6f", e - s}')

		{
			echo "TPC-DS runtimes (sf=${SF}, run=${RUN_IDX})"
			echo "DB: ${TPCDS_DB_PATH}"
			echo "DuckDB: ${DUCKDB_BIN}"
			echo "Results CSV: ${TPCDS_CSV_PATH}"
			echo "Sum of per-query runtimes (s): ${TPCDS_TOTAL}"
			echo "Wall-clock time for query loop (s): ${TPCDS_WALL_SECONDS}"
		} | tee "$TPCDS_TXT_PATH"
	fi

	# ===== Combined Results for this run (if both were run) =====
	if [[ $RUN_TPCH -eq 1 ]] && [[ $RUN_TPCDS -eq 1 ]]; then
		COMBINED_TOTAL=$(awk -v t1="$TPCH_TOTAL" -v t2="$TPCDS_TOTAL" 'BEGIN{printf "%.6f", t1 + t2}')
		COMBINED_WALL=$(awk -v w1="$TPCH_WALL_SECONDS" -v w2="$TPCDS_WALL_SECONDS" 'BEGIN{printf "%.6f", w1 + w2}')

		{
			echo "===== COMBINED RESULTS (run ${RUN_IDX}) ====="
			echo "Scale Factor: ${SF}"
			echo ""
			echo "TPC-H:"
			echo "  Sum of per-query runtimes (s): ${TPCH_TOTAL}"
			echo "  Wall-clock time (s): ${TPCH_WALL_SECONDS}"
			echo ""
			echo "TPC-DS:"
			echo "  Sum of per-query runtimes (s): ${TPCDS_TOTAL}"
			echo "  Wall-clock time (s): ${TPCDS_WALL_SECONDS}"
			echo ""
			echo "TOTAL Combined:"
			echo "  Sum of per-query runtimes (s): ${COMBINED_TOTAL}"
			echo "  Wall-clock time (s): ${COMBINED_WALL}"
		} | tee "$COMBINED_TXT_PATH"
	fi

	RUN_END_WALL=$(date +%s.%N)
	RUN_WALL_SECONDS=$(awk -v s="$RUN_START_WALL" -v e="$RUN_END_WALL" 'BEGIN{printf "%.6f", e - s}')
	MULTIRUN_TOTAL_SECONDS=$(awk -v t="$MULTIRUN_TOTAL_SECONDS" -v r="$RUN_WALL_SECONDS" 'BEGIN{printf "%.6f", t + r}')

	echo "Run ${RUN_IDX} total wall-clock time (s): ${RUN_WALL_SECONDS}"
done

MULTIRUN_AVERAGE_SECONDS=$(awk -v t="$MULTIRUN_TOTAL_SECONDS" -v n="$RUNS" 'BEGIN{printf "%.6f", t / n}')

echo ""
echo "===== COMMAND LINE OPTIONS USED ====="
if [[ ${#PASSED_OPTIONS[@]} -gt 0 ]]; then
	for opt in "${PASSED_OPTIONS[@]}"; do
		echo "  $opt"
	done
else
	echo "  (none - all defaults)"
fi

echo "===== MULTI-RUN SUMMARY ====="
echo "Total time (s): ${MULTIRUN_TOTAL_SECONDS}"
echo "Number of runs: ${RUNS}"
echo "Average time per run (s): ${MULTIRUN_AVERAGE_SECONDS}"
