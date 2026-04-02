#!/usr/bin/env bash
# Phase 1: Generate tables R, S, T using the release binary.
# Called by scripts/measure/run_ash_datagen_debug.sh and run_ash_datagen_release.sh.
# Usage: run_generation.sh <db-path>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DB="${1:?Usage: $0 <db-path>}"
COMMON_SETTINGS_SQL="${COMMON_SETTINGS_SQL:-scripts/measure/settings-common.sql}"
RUN_SETTINGS_SQL="${RUN_SETTINGS_SQL:-scripts/measure/settings-run_ash_datagen.sql}"

if [[ ! -f "$COMMON_SETTINGS_SQL" ]]; then
    echo "Error: Common settings file not found: $COMMON_SETTINGS_SQL" >&2
    exit 1
fi
if [[ -n "$RUN_SETTINGS_SQL" ]] && [[ ! -f "$RUN_SETTINGS_SQL" ]]; then
    echo "Error: Run-specific settings file not found: $RUN_SETTINGS_SQL" >&2
    exit 1
fi

rm -f "$DB" "${DB}.wal"

echo "=== Phase 1: generating tables with release build ==="
{
    grep '^SET VARIABLE' "$COMMON_SETTINGS_SQL" || true
    if [[ -n "$RUN_SETTINGS_SQL" ]]; then
        grep '^SET VARIABLE' "$RUN_SETTINGS_SQL" || true
    fi
    # Generation queries hit a known unstable path in this branch's join-order optimizer.
    # Keep generation deterministic by pinning optimizer behavior during table creation.
    echo "SET disabled_optimizers = 'join_order,build_side_probe_side,statistics_propagation';"
    cat <<'SQL'
.read ASH-datagen/generate_tables.sql
CREATE OR REPLACE TABLE generator_counts_persistent AS SELECT * FROM generator_counts;
SQL
} | build/release/duckdb "$DB"
echo "=== Phase 1 done ==="
