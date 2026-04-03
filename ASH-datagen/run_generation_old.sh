#!/usr/bin/env bash
# Phase 1: Generate tables R, S, T using the release binary.
# Called by launch_debug.sh / launch_release.sh.
# Usage: run_generation.sh <db-path>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DB="${1:?Usage: $0 <db-path>}"
rm -f "$DB" "${DB}.wal"

echo "=== Phase 1: generating tables with release build ==="
{
    grep '^SET VARIABLE' ASH-datagen/settings_old.sql
    cat <<'SQL'
.read ASH-datagen/generate_tables.sql
CREATE OR REPLACE TABLE generator_counts_persistent AS SELECT * FROM generator_counts;
SQL
} | build/release/duckdb "$DB"
echo "=== Phase 1 done ==="