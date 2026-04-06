#!/usr/bin/env bash
# Shared benchmark runner: prepare + timed execution + report.
# Usage: run_benchmark.sh <query> <num_runs> <db_path> <duckdb_cmd...>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

QUERY="${1:?Usage: $0 <query> <num_runs> <db_path> <duckdb_cmd...>}"
NUM_RUNS="${2:?Usage: $0 <query> <num_runs> <db_path> <duckdb_cmd...>}"
DB="${3:?Usage: $0 <query> <num_runs> <db_path> <duckdb_cmd...>}"
COMMON_SETTINGS_SQL="${COMMON_SETTINGS_SQL:-scripts/measure/settings-common.sql}"
RUN_SETTINGS_SQL="${RUN_SETTINGS_SQL:-}"
shift 3

if [[ ! -f "$COMMON_SETTINGS_SQL" ]]; then
    echo "Error: Common settings file not found: $COMMON_SETTINGS_SQL" >&2
    exit 1
fi
if [[ -n "$RUN_SETTINGS_SQL" ]] && [[ ! -f "$RUN_SETTINGS_SQL" ]]; then
    echo "Error: Run-specific settings file not found: $RUN_SETTINGS_SQL" >&2
    exit 1
fi

{
    grep '^SET ' "$COMMON_SETTINGS_SQL" || true
    if [[ -n "$RUN_SETTINGS_SQL" ]]; then
        grep '^SET ' "$RUN_SETTINGS_SQL" || true
    fi
    if [[ -n "${CASE_SETTINGS:-}" ]]; then
        printf '%s\n' "${CASE_SETTINGS}"
    fi
    echo "CREATE OR REPLACE TEMP TABLE generator_counts AS SELECT * FROM generator_counts_persistent;"
    echo ".read ASH-datagen/query_${QUERY}.sql"

    echo ".print Running benchmark (${NUM_RUNS} runs)"
    echo "SET VARIABLE t0 = epoch_ms(now());"
    echo ".timer on"
    for i in $(seq 1 "$NUM_RUNS"); do
        echo "EXECUTE benchmark_query;"
    done
    echo ".timer off"
    echo "SET VARIABLE t_end = epoch_ms(now());"

    echo ".print Show the detailed timed query plan"
    echo ".output stdout"
    echo "EXPLAIN ANALYZE EXECUTE benchmark_query;"
    echo "SELECT printf('Average run time: %.3f s', (getvariable('t_end') - getvariable('t0')) / ${NUM_RUNS}.0 / 1000.0) AS info;"
} | "$@"
