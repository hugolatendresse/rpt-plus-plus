#!/bin/bash
# Allows running the JOB benchmark
# USAGE:
# scripts/measure/run_job.sh

cd /mnt/local_ssd/spy/join-order-benchmark

DB_FILE="job.db"

# Verify the database file exists
if [ ! -f "$DB_FILE" ]; then
    echo "Error: Database file $DB_FILE not found. Execute setup_and_load.sh first."
    exit 1
fi

echo "Starting Join Order Benchmark execution..."

TIMEFORMAT='Total wall clock time: %3R seconds'
time {
    for q in queries/*.sql; do
        # Exclude structural SQL files
        if [ "$q" != "schema.sql" ] && [ "$q" != "fkindexes.sql" ]; then
            echo "Executing $q..."
            ../build/release/duckdb "$DB_FILE" < "$q"
        fi
    done
}

echo "Benchmark execution complete."