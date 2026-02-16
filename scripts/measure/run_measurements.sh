#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS="$SCRIPT_DIR/results.txt"

: > "$RESULTS"

SQL_FILES=(
  0_cold
  1_cold_interleaved
  1_cold_segmented
  5_cold_interleaved
  5_cold_segmented
  10_cold_interleaved
  10_cold_segmented
  100_cold_interleaved
  100_cold_segmented
)

for base in "${SQL_FILES[@]}"; do
  echo "running ${base}"
  build/release/duckdb -f "$SCRIPT_DIR/${base}.sql"   # TODO uncomment
done

# Keys to extract from HASH_JOIN extra_info (first occurrence in tree)
EXTRA_KEYS=(
  "Build Time"
  "Probe Time"
  "Probe Time (ExecuteInternal)"
  "Probe Time (ExternalProbe)"
  "Scan Structure Next Time (ExecuteInternal)"
  "ProbeForPointers Time"
  "Match Time"
)

for base in "${SQL_FILES[@]}"; do
  json="$SCRIPT_DIR/${base}.json"
  [ -f "$json" ] || continue
  cpu_time=$(jq -r '.cpu_time' "$json")
  {
    echo "$base"
    echo "CPU Time"
    echo "$cpu_time"
    for key in "${EXTRA_KEYS[@]}"; do
      val=$(jq -r --arg k "$key" '[.. | objects | .extra_info[$k]? // empty] | first // "N/A"' "$json")
      echo "$key"
      echo "$val"
    done
    echo
  } >> "$RESULTS"
done