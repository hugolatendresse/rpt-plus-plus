#!/usr/bin/env bash
set -euo pipefail

SF=100
DUCKDB_BIN=""
GENERATE_DATA=0
OUT_DIR="./tpch_results"
RUN_TPCH=1
RUN_TPCDS=1
DB_BASE_PATH="../benchmark_data"
TPCH_QUERY=""
RUNS=1
CASE=""
USE_PERF=false
USE_DEBUG=false
USE_DUCKDB_PROFILING=false
COMMON_SETTINGS_SQL="${COMMON_SETTINGS_SQL:-scripts/measure/settings-common.sql}"
RUN_SETTINGS_SQL="scripts/measure/settings-run_tpc.sql"
PERF_EVENTS="cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,mem_access,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd,branch-instructions,branch-misses,br_retired,br_mis_pred_retired"

usage() {
cat <<'USAGE'
Usage: scripts/measure/run_tpc.sh [options]

Options:
  --sf <scale_factor>    Scale factor for dbgen/dsdgen (default: 100)
  --db <db_base_path>    Base path for databases (default: ../benchmark_data)
  --duckdb <bin_path>    DuckDB CLI binary (default: ./build/release/duckdb)
  --generate             Generate TPC-H/TPC-DS data
  --out-dir <dir>        Output directory for results (default: ./tpch_results)
  --tpch-only            Run only TPC-H
  --tpcds-only           Run only TPC-DS
  --tpch-query <1..22>   Run one TPC-H query (implies --tpch-only)
  --runs <N>             Number of benchmark runs (default: 1)
  --case <1|2|3|4>       Optimizer case (required)
  --perf                 Run each query under perf stat
  --debug                Use debug build (build/debug/duckdb)
  --duckdb-profiling     Enable DuckDB JSON profiling, output to tpc_results.json
  -h, --help             Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SF="$2"; shift 2 ;;
        --db) DB_BASE_PATH="$2"; shift 2 ;;
        --duckdb) DUCKDB_BIN="$2"; shift 2 ;;
        --generate) GENERATE_DATA=1; shift ;;
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        --tpch-only) RUN_TPCH=1; RUN_TPCDS=0; shift ;;
        --tpcds-only) RUN_TPCH=0; RUN_TPCDS=1; shift ;;
        --tpch-query) TPCH_QUERY="$2"; RUN_TPCH=1; RUN_TPCDS=0; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --case) CASE="$2"; shift 2 ;;
        --perf) USE_PERF=true; shift ;;
        --debug) USE_DEBUG=true; shift ;;
        --duckdb-profiling) USE_DUCKDB_PROFILING=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
    esac
done

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
    echo "Error: --runs must be a positive integer" >&2
    exit 1
fi

if [[ -z "$CASE" ]]; then
    echo "Error: --case is required (1, 2, 3, or 4)." >&2
    exit 1
fi

case "$CASE" in
    1) CASE_SETTINGS="SET disable_rpt = true;
SET disable_tiered_hash_cache = true;" ;;
    2) CASE_SETTINGS="SET rpt_forward_only = true;
SET disable_tiered_hash_cache = true;" ;;
    3) CASE_SETTINGS="SET rpt_forward_only = true;" ;;
    4) CASE_SETTINGS="SET disable_tiered_hash_cache = true;" ;;
    *) echo "Error: --case must be 1, 2, 3, or 4 (got: $CASE)" >&2; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# Absolute path so the file is easy to locate regardless of the script's CWD.
PROFILING_OUTPUT="$REPO_ROOT/tpc_results.json"
# See https://duckdb.org/docs/stable/dev/profiling
PROFILING_PRAGMAS="PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = '$PROFILING_OUTPUT';
PRAGMA profiling_coverage = 'SELECT';"

# Pick default DuckDB binary based on --debug if the user did not override with --duckdb.
if [[ -z "$DUCKDB_BIN" ]]; then
    if $USE_DEBUG; then
        DUCKDB_BIN="./build/debug/duckdb"
    else
        DUCKDB_BIN="./build/release/duckdb"
    fi
fi

TPCH_DB_PATH="${DB_BASE_PATH}/tpch/tpch_sf${SF}.duckdb"
TPCDS_DB_PATH="${DB_BASE_PATH}/tpcds/tpcds_sf${SF}.duckdb"

if [[ ! -x "$DUCKDB_BIN" ]]; then
    echo "DuckDB binary not found or not executable: $DUCKDB_BIN" >&2
    exit 1
fi
if [[ ! -f "$COMMON_SETTINGS_SQL" ]]; then
    echo "Common settings file not found: $COMMON_SETTINGS_SQL" >&2
    exit 1
fi
if [[ ! -f "$RUN_SETTINGS_SQL" ]]; then
    echo "Run-specific settings file not found: $RUN_SETTINGS_SQL" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DBGEN_LOG="$OUT_DIR/dbgen_sf${SF}_${TIMESTAMP}.log"
TPCH_CSV_PATH="$OUT_DIR/tpch_runtimes_sf${SF}_${TIMESTAMP}.csv"
TPCDS_CSV_PATH="$OUT_DIR/tpcds_runtimes_sf${SF}_${TIMESTAMP}.csv"

build_sql() {
    local extension="$1"
    local query_stmt="$2"
    if $USE_DUCKDB_PROFILING; then
        printf '%s\n' "$PROFILING_PRAGMAS"
    fi
    grep '^SET ' "$COMMON_SETTINGS_SQL" || true
    grep '^SET ' "$RUN_SETTINGS_SQL" || true
    printf '%s\n' "$CASE_SETTINGS"
    printf 'LOAD %s;\n' "$extension"
    printf '%s\n' "$query_stmt"
}

if [[ $RUN_TPCH -eq 1 ]] && [[ $GENERATE_DATA -eq 1 ]]; then
    mkdir -p "$(dirname "$TPCH_DB_PATH")"
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
fi

if [[ $RUN_TPCDS -eq 1 ]] && [[ $GENERATE_DATA -eq 1 ]]; then
    mkdir -p "$(dirname "$TPCDS_DB_PATH")"
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
fi

if [[ $RUN_TPCH -eq 1 ]] && [[ $GENERATE_DATA -eq 0 ]] && [[ ! -f "$TPCH_DB_PATH" ]]; then
    echo "Error: TPC-H database not found at ${TPCH_DB_PATH}" >&2
    exit 1
fi
if [[ $RUN_TPCDS -eq 1 ]] && [[ $GENERATE_DATA -eq 0 ]] && [[ ! -f "$TPCDS_DB_PATH" ]]; then
    echo "Error: TPC-DS database not found at ${TPCDS_DB_PATH}" >&2
    exit 1
fi

TOTAL_WALL=0
for RUN_IDX in $(seq 1 "$RUNS"); do
    RUN_START=$(date +%s.%N)
    echo "===== RUN ${RUN_IDX}/${RUNS} (case ${CASE}) ====="

    if [[ $RUN_TPCH -eq 1 ]]; then
        printf "query,runtime_seconds\n" > "$TPCH_CSV_PATH"
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
            SQL="$(build_sql tpch "PRAGMA tpch(${Q});")"
            if $USE_PERF; then
                if /usr/bin/time -f "%e" -o "$TIME_FILE" bash -c 'printf "%s\n" "$1" | sudo perf stat -e "$2" -- "$3" "$4" >/dev/null' _ "$SQL" "$PERF_EVENTS" "$DUCKDB_BIN" "$TPCH_DB_PATH"; then
                    RUNTIME=$(awk 'NR==1{print $1}' "$TIME_FILE")
                    printf "Q%02d,%s\n" "$Q" "$RUNTIME" >> "$TPCH_CSV_PATH"
                else
                    rm -f "$TIME_FILE"
                    echo "Error: TPC-H query ${Q} failed" >&2
                    exit 1
                fi
            else
                if /usr/bin/time -f "%e" -o "$TIME_FILE" bash -c 'printf "%s\n" "$1" | "$2" "$3" >/dev/null' _ "$SQL" "$DUCKDB_BIN" "$TPCH_DB_PATH"; then
                    RUNTIME=$(awk 'NR==1{print $1}' "$TIME_FILE")
                    printf "Q%02d,%s\n" "$Q" "$RUNTIME" >> "$TPCH_CSV_PATH"
                else
                    rm -f "$TIME_FILE"
                    echo "Error: TPC-H query ${Q} failed" >&2
                    exit 1
                fi
            fi
            rm -f "$TIME_FILE"
        done
    fi

    if [[ $RUN_TPCDS -eq 1 ]]; then
        printf "query,runtime_seconds\n" > "$TPCDS_CSV_PATH"
        for Q in $(seq 1 99); do
            echo "Running TPC-DS query ${Q}..."
            TIME_FILE=$(mktemp)
            SQL="$(build_sql tpcds "PRAGMA tpcds(${Q});")"
            if $USE_PERF; then
                if /usr/bin/time -f "%e" -o "$TIME_FILE" bash -c 'printf "%s\n" "$1" | sudo perf stat -e "$2" -- "$3" "$4" >/dev/null' _ "$SQL" "$PERF_EVENTS" "$DUCKDB_BIN" "$TPCDS_DB_PATH"; then
                    RUNTIME=$(awk 'NR==1{print $1}' "$TIME_FILE")
                    printf "Q%02d,%s\n" "$Q" "$RUNTIME" >> "$TPCDS_CSV_PATH"
                else
                    rm -f "$TIME_FILE"
                    echo "Error: TPC-DS query ${Q} failed" >&2
                    exit 1
                fi
            else
                if /usr/bin/time -f "%e" -o "$TIME_FILE" bash -c 'printf "%s\n" "$1" | "$2" "$3" >/dev/null' _ "$SQL" "$DUCKDB_BIN" "$TPCDS_DB_PATH"; then
                    RUNTIME=$(awk 'NR==1{print $1}' "$TIME_FILE")
                    printf "Q%02d,%s\n" "$Q" "$RUNTIME" >> "$TPCDS_CSV_PATH"
                else
                    rm -f "$TIME_FILE"
                    echo "Error: TPC-DS query ${Q} failed" >&2
                    exit 1
                fi
            fi
            rm -f "$TIME_FILE"
        done
    fi

    RUN_END=$(date +%s.%N)
    RUN_WALL=$(awk -v s="$RUN_START" -v e="$RUN_END" 'BEGIN{printf "%.6f", e - s}')
    TOTAL_WALL=$(awk -v t="$TOTAL_WALL" -v r="$RUN_WALL" 'BEGIN{printf "%.6f", t + r}')
    echo "Run ${RUN_IDX} total wall-clock time (s): ${RUN_WALL}"
done

AVG_WALL=$(awk -v t="$TOTAL_WALL" -v n="$RUNS" 'BEGIN{printf "%.6f", t / n}')
echo "===== MULTI-RUN SUMMARY ====="
echo "Total time (s): ${TOTAL_WALL}"
echo "Number of runs: ${RUNS}"
echo "Average time per run (s): ${AVG_WALL}"
if $USE_DUCKDB_PROFILING; then
    echo "DuckDB profiling output written to: $PROFILING_OUTPUT"
fi
