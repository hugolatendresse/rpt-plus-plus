#!/usr/bin/env bash
# Allows running the JOB benchmark
# Usage:
# scripts/measure/run_job.sh --case <1|2|3|4> [--perf]
set -euo pipefail

CASE=""
JOB_QUERY=""
USE_PERF=false
PERF_EVENTS="cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,mem_access,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd,branch-instructions,branch-misses,br_mis_pred_retired"

usage() {
    cat <<'USAGE'
Usage: scripts/measure/run_job.sh --case <1|2|3|4> [options]

Options:
  --case <1|2|3|4>      Optimizer case (required)
  --job-query <id>      Run one JOB query (e.g. 10a, 24a, or 10a.sql)
  --perf                Run query/queries under perf stat
  -h, --help            Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --case) CASE="$2"; shift 2 ;;
        --job-query) JOB_QUERY="$2"; shift 2 ;;
        --perf) USE_PERF=true; shift ;;
        -h|--help)
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
JOB_DIR="$REPO_ROOT/join-order-benchmark"
COMMON_SETTINGS_SQL="$REPO_ROOT/scripts/measure/settings-common.sql"
RUN_SETTINGS_SQL="$REPO_ROOT/scripts/measure/settings-run_job.sql"
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
else
    while IFS= read -r q; do
        QUERY_FILES+=("${q#"$JOB_DIR/"}")
    done < <(printf "%s\n" "$JOB_DIR"/queries/*.sql | sort -V)
fi

run_query() {
    local query_file="$1"
    if $USE_PERF; then
        {
            grep '^SET ' "$COMMON_SETTINGS_SQL" || true
            grep '^SET ' "$RUN_SETTINGS_SQL" || true
            printf '%s\n' "$CASE_SETTINGS"
            cat "$query_file"
        } | sudo perf stat -e "$PERF_EVENTS" -- "$REPO_ROOT/build/release/duckdb" "$DB_FILE"
    else
        {
            grep '^SET ' "$COMMON_SETTINGS_SQL" || true
            grep '^SET ' "$RUN_SETTINGS_SQL" || true
            printf '%s\n' "$CASE_SETTINGS"
            cat "$query_file"
        } | "$REPO_ROOT/build/release/duckdb" "$DB_FILE"
    fi
}

if [[ -n "$JOB_QUERY" ]]; then
    echo "Starting Join Order Benchmark execution (case ${CASE}, query ${JOB_QUERY})..."
else
    echo "Starting Join Order Benchmark execution (case ${CASE})..."
fi
cd "$JOB_DIR"
TIMEFORMAT='Total wall clock time: %3R seconds'
time {
    for q in "${QUERY_FILES[@]}"; do
        echo "Executing $q..."
        run_query "$q"
    done
}
echo "Benchmark execution complete."
