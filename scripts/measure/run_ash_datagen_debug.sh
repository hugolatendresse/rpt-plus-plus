#!/usr/bin/env bash
# Run table generation (release) + benchmark query (debug).
# Usage: run_ash_datagen_debug.sh [--perf] [--no-taskset] [--runs N] --case <1|2|3|4> rs|rst
set -euo pipefail

PERF=false
USE_TASKSET=true
RUNS=1
CASE=""
COMMON_SETTINGS_SQL="scripts/measure/settings-common.sql"
RUN_SETTINGS_SQL="scripts/measure/settings-run_ash_datagen.sql"
PERF_EVENTS="cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,mem_access,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd,branch-instructions,branch-misses,br_mis_pred_retired"

while [[ "${1:-}" == --* ]]; do
    case "$1" in
        --perf) PERF=true; shift ;;
        --no-taskset) USE_TASKSET=false; shift ;;
        --runs) RUNS="$2"; shift 2 ;;
        --case) CASE="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--perf] [--no-taskset] [--runs N] --case <1|2|3|4> rs|rst"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "${CASE}" ]]; then
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

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
    echo "Error: --runs must be a positive integer" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

QUERY="${1:?Usage: $0 [--perf] [--no-taskset] [--runs N] --case <1|2|3|4> rs|rst}"
DB="ASH-datagen/bench.duckdb"

if [[ ! -f "$COMMON_SETTINGS_SQL" ]]; then
    echo "Error: Common settings file not found: $COMMON_SETTINGS_SQL" >&2
    exit 1
fi
if [[ ! -f "$RUN_SETTINGS_SQL" ]]; then
    echo "Error: Run-specific settings file not found: $RUN_SETTINGS_SQL" >&2
    exit 1
fi

COMMON_SETTINGS_SQL="$COMMON_SETTINGS_SQL" RUN_SETTINGS_SQL="$RUN_SETTINGS_SQL" "ASH-datagen/run_generation.sh" "$DB"

echo "=== Phase 2: running query_${QUERY}.sql with debug build (${RUNS} runs, case ${CASE}) ==="

TASKSET_PREFIX=()
if $USE_TASKSET; then
    TASKSET_PREFIX=(taskset -c 4-59)
fi

if $PERF; then
    CMD=(sudo perf stat -e "$PERF_EVENTS" -- "${TASKSET_PREFIX[@]}" build/debug/duckdb "$DB")
else
    CMD=("${TASKSET_PREFIX[@]}" build/debug/duckdb "$DB")
fi

COMMON_SETTINGS_SQL="$COMMON_SETTINGS_SQL" RUN_SETTINGS_SQL="$RUN_SETTINGS_SQL" CASE_SETTINGS="$CASE_SETTINGS" "ASH-datagen/run_benchmark.sh" "$QUERY" "$RUNS" "$DB" "${CMD[@]}"
