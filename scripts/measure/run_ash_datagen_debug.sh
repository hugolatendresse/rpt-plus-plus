#!/usr/bin/env bash
# Run table generation (release) + benchmark query (debug).
# Usage: run_ash_datagen_debug.sh [--perf] [--no-taskset] [--runs N] [--csv PATH] --case <1|2|3|4> rs|rst
set -euo pipefail

PERF=false
USE_TASKSET=true
DROP_OS_CACHE=false
RUNS=1
CASE=""
CSV_PATH=""
COMMON_SETTINGS_SQL="scripts/measure/settings-common.sql"
RUN_SETTINGS_SQL="scripts/measure/settings-run_ash_datagen.sql"
PERF_EVENTS="cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,mem_access,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd,branch-instructions,branch-misses,br_retired,br_mis_pred_retired"

while [[ "${1:-}" == --* ]]; do
    case "$1" in
        --perf) PERF=true; shift ;;
        --no-taskset) USE_TASKSET=false; shift ;;
        --drop-os-cache) DROP_OS_CACHE=true; shift ;;
        --runs) RUNS="$2"; shift 2 ;;
        --case) CASE="$2"; shift 2 ;;
        --csv) CSV_PATH="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--perf] [--no-taskset] [--drop-os-cache] [--runs N] [--csv PATH] --case <1|2|3|4> rs|rst"
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

QUERY="${1:?Usage: $0 [--perf] [--no-taskset] [--runs N] [--csv PATH] --case <1|2|3|4> rs|rst}"
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

if [[ -z "$CSV_PATH" ]]; then
    COMMON_SETTINGS_SQL="$COMMON_SETTINGS_SQL" RUN_SETTINGS_SQL="$RUN_SETTINGS_SQL" \
        CASE_SETTINGS="$CASE_SETTINGS" DROP_OS_CACHE="$DROP_OS_CACHE" \
        "ASH-datagen/run_benchmark.sh" "$QUERY" "$RUNS" "$DB" "${CMD[@]}"
    exit 0
fi

# The legacy debug runner writes DuckDB's per-query timer output directly to
# the terminal. Capture that same output when a CSV is requested so each timed
# EXECUTE becomes one row without changing the normal no-CSV behavior.
mkdir -p "$(dirname "$CSV_PATH")"
printf "query,case,seed,runtime_seconds\n" >"$CSV_PATH"
tmp_out="$(mktemp)"
if ! COMMON_SETTINGS_SQL="$COMMON_SETTINGS_SQL" RUN_SETTINGS_SQL="$RUN_SETTINGS_SQL" \
    CASE_SETTINGS="$CASE_SETTINGS" DROP_OS_CACHE="$DROP_OS_CACHE" \
    "ASH-datagen/run_benchmark.sh" "$QUERY" "$RUNS" "$DB" "${CMD[@]}" 2>&1 | tee "$tmp_out"; then
    rm -f "$tmp_out"
    echo "Error: debug benchmark failed" >&2
    exit 1
fi

run_count=0
while IFS= read -r line; do
    if [[ "$line" =~ ^Run\ Time\ \(s\):\ real\ ([0-9.]+) ]]; then
        printf '%s,%s,,%s\n' "$QUERY" "$CASE" "${BASH_REMATCH[1]}" >>"$CSV_PATH"
        run_count=$((run_count + 1))
    fi
done <"$tmp_out"
rm -f "$tmp_out"

if [[ "$run_count" -ne "$RUNS" ]]; then
    echo "warning: captured ${run_count}/${RUNS} debug runtimes in $CSV_PATH" >&2
fi
echo "CSV written to: $CSV_PATH"

# Condense the runtime CSV to one (median) row for this query/case.
MEDIAN_SCRIPT="$SCRIPT_DIR/median_runtime_csv.py"
if [[ -f "$MEDIAN_SCRIPT" ]] && command -v python3 >/dev/null; then
    python3 "$MEDIAN_SCRIPT" --csv "$CSV_PATH" || \
        echo "warning: median_runtime_csv failed for $CSV_PATH" >&2
fi
