#!/usr/bin/env bash
# Run THC benchmark: generate data (optionally) then run the join query under perf (optionally).
#
# Usage:
#   ./scripts/measure/launch.sh [OPTIONS]
#
# Options:
#   --cold N        Cold-to-hot ratio: 1, 5, 10, 100  (default: 10)
#   --layout L      Layout: interleaved, segmented     (default: interleaved)
#   --case N        Optimizer case (required):
#                     1 = Old DuckDB (no RPT, no THC)
#                     2 = RPT+ Forward Pass Only
#                     3 = RPT+ Forward + THC
#                     4 = RPT+ Forward + Backward
#   --runs N        Run the query N times with a warmup and print average (default: 1)
#   --generate      (Re)generate the data before running the query
#   --perf          Run under perf stat
#   --profile       Enable DuckDB JSON profiling output
#   --no-taskset    Don't pin DuckDB to cores 4-59 via taskset (pinning is on by default)
#
# Examples:
#   ./scripts/measure/launch.sh --case 3                  # 10 cold, interleaved, RPT+Forward+THC
#   ./scripts/measure/launch.sh --case 1 --generate      # generate data first, Old DuckDB
#   ./scripts/measure/launch.sh --case 4 --cold 100 --layout segmented --generate --perf
set -euo pipefail

COLD=10
LAYOUT=interleaved
CASE=""
RUNS=1
GENERATE=false
USE_PERF=false
PROFILE=false
USE_TASKSET=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cold)      COLD="$2";    shift 2 ;;
        --layout)    LAYOUT="$2";  shift 2 ;;
        --case)      CASE="$2";    shift 2 ;;
        --runs)      RUNS="$2";    shift 2 ;;
        --generate)  GENERATE=true;  shift ;;
        --perf)      USE_PERF=true;  shift ;;
        --profile)   PROFILE=true;   shift ;;
        --no-taskset) USE_TASKSET=false; shift ;;
        -h|--help)
            sed -n '2,/^set /{ /^#/s/^# \?//p }' "$0"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

case "$COLD" in
    1|5|10|100) ;;
    *) echo "Error: --cold must be 1, 5, 10, or 100 (got: $COLD)"; exit 1 ;;
esac
case "$LAYOUT" in
    interleaved|segmented) ;;
    *) echo "Error: --layout must be interleaved or segmented (got: $LAYOUT)"; exit 1 ;;
esac
if [[ -z "$CASE" ]]; then
    echo "Error: --case is required (1, 2, 3, or 4). See --help."; exit 1
fi
case "$CASE" in
    1) CASE_SETTINGS="SET disable_rpt = true;
SET disable_tiered_hash_cache = true;" ;;
    2) CASE_SETTINGS="SET rpt_forward_only = true;
SET disable_tiered_hash_cache = true;" ;;
    3) CASE_SETTINGS="SET rpt_forward_only = true;" ;;
    4) CASE_SETTINGS="SET disable_tiered_hash_cache = true;" ;;
    *) echo "Error: --case must be 1, 2, 3, or 4 (got: $CASE)"; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

DB_NAME="${COLD}_cold_${LAYOUT}"
DB="scripts/measure/data/${DB_NAME}.duckdb"
SETUP_SQL="scripts/measure/generation/${DB_NAME}.sql"
SETTINGS_SQL="scripts/measure/settings.sql"
QUERY_SQL="scripts/measure/query.sql"
PROFILE_JSON="scripts/measure/${DB_NAME}.json"

if $GENERATE; then
    echo "=== Generating data: ${DB_NAME} ==="
    rm -f "$DB" "${DB}.wal"
    build/release/duckdb "$DB" < "$SETUP_SQL"
    echo "=== Data generated: $DB ==="
fi

if [[ ! -f "$DB" ]]; then
    echo "Error: $DB does not exist. Run with --generate first."
    exit 1
fi

PROFILING_HEADER=""
if $PROFILE; then
    PROFILING_HEADER="PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = '${PROFILE_JSON}';
PRAGMA profiling_coverage = 'SELECT';
"
fi

TASKSET_PREFIX=()
if $USE_TASKSET; then
    TASKSET_PREFIX=(taskset -c 4-59)
fi

if $USE_PERF; then
    CMD=(sudo perf stat \
        -e cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd,branch-instructions,branch-misses \
        -- "${TASKSET_PREFIX[@]}" build/release/duckdb "$DB")
else
    CMD=("${TASKSET_PREFIX[@]}" build/release/duckdb "$DB")
fi

build_bench_sql() {
    printf '%s\n' "$PROFILING_HEADER"
    printf '%s\n' "$CASE_SETTINGS"
    cat "$SETTINGS_SQL"
    echo ""
    echo "PREPARE benchmark_query AS"
    echo "SELECT min(b.valueB1)"
    echo "FROM a"
    echo "JOIN b ON a.keyB1 = b.keyB1;"
    echo ""
    echo "EXECUTE benchmark_query;"
    echo ".print Warmup done — running $RUNS timed iterations"
    echo "SET VARIABLE t0 = epoch_ms(now());"
    echo ".timer on"
    for ((i = 0; i < RUNS; i++)); do
        echo "EXECUTE benchmark_query;"
    done
    echo ".timer off"
    echo "SET VARIABLE t1 = epoch_ms(now());"
    echo ""
    echo "SELECT printf('Average run time: %.3f s', (getvariable('t1') - getvariable('t0')) / ${RUNS}.0 / 1000.0) AS info;"
}

echo "=== Running query: ${DB_NAME} (Case #$CASE, warmup + $RUNS runs) ==="
build_bench_sql | "${CMD[@]}"
