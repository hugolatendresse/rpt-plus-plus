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
shift 3

{
    grep '^SET ' ASH-datagen/settings.sql
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

    echo "SET disabled_optimizers = '';"
    echo "SET threads = getvariable('old_threads');"
    echo "RESET VARIABLE old_threads;"
} | "$@"
