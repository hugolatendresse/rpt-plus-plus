#!/usr/bin/env bash
# Run table generation (release) + benchmark query (debug).
# Usage: launch_debug.sh [--no-taskset] rs|rst
set -euo pipefail

USE_TASKSET=true

while [[ "${1:-}" == --* ]]; do
    case "$1" in
        --no-taskset) USE_TASKSET=false; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

QUERY="${1:?Usage: $0 [--no-taskset] rs|rst}"
DB="ASH-datagen/bench.duckdb"

"$SCRIPT_DIR/run_generation.sh" "$DB"

TASKSET_PREFIX=()
if $USE_TASKSET; then
    TASKSET_PREFIX=(taskset -c 4-59)
fi

echo "=== Phase 2: running query_${QUERY}.sql with debug build (1 run) ==="
"$SCRIPT_DIR/run_benchmark.sh" "$QUERY" 1 "$DB" "${TASKSET_PREFIX[@]}" build/debug/duckdb "$DB"
