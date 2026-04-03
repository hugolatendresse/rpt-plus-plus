#!/usr/bin/env bash
# Run table generation (release) + benchmark query (release).
# Usage: launch_release.sh [--perf] [--no-taskset] rs|rst
set -euo pipefail

PERF=false
USE_TASKSET=true

while [[ "${1:-}" == --* ]]; do
    case "$1" in
        --perf)       PERF=true;        shift ;;
        --no-taskset) USE_TASKSET=false; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

QUERY="${1:?Usage: $0 [--perf] [--no-taskset] rs|rst}"
DB="ASH-datagen/bench.duckdb"

"$SCRIPT_DIR/run_generation_old.sh" "$DB"

echo "=== Phase 2: running query_${QUERY}.sql with release build (5 runs) ==="

TASKSET_PREFIX=()
if $USE_TASKSET; then
    TASKSET_PREFIX=(taskset -c 4-59)
fi

if $PERF; then
    CMD=(sudo perf stat -e cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,mem_access,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd, br_miss_pred_retired -- "${TASKSET_PREFIX[@]}" build/release/duckdb "$DB")
else
    CMD=("${TASKSET_PREFIX[@]}" build/release/duckdb "$DB")
fi

"$SCRIPT_DIR/run_benchmark_old.sh" "$QUERY" 5 "$DB" "${CMD[@]}"