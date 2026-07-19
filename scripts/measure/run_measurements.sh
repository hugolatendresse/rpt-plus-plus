#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS="$SCRIPT_DIR/results.txt"
DROP_OS_CACHE=false

usage() {
  echo "Usage: scripts/measure/run_measurements.sh [--drop-os-cache]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --drop-os-cache) DROP_OS_CACHE=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

drop_os_page_cache() {
  if ! $DROP_OS_CACHE; then
    return
  fi
  # Linux page cache survives across DuckDB CLI processes. Keep cold-cache
  # measurements explicit because this sudo operation affects the whole host.
  echo "Dropping Linux page cache before measurement SQL file..." >&2
  sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
}

is_recoverable_duckdb_resource_error() {
  local error_file="$1"
  grep -Eiq 'Out of Memory Error|failed to offload data block|max_temp_directory_size' "$error_file"
}

: > "$RESULTS"
declare -A RESOURCE_FAILURES=()

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
  tmp_out="$(mktemp)"
  drop_os_page_cache
  if ! build/release/duckdb -f "$SCRIPT_DIR/${base}.sql" >"$tmp_out" 2>&1; then
    cat "$tmp_out" >&2
    if is_recoverable_duckdb_resource_error "$tmp_out"; then
      echo "Warning: ${base} hit DuckDB OOM/temp-spill limit; recording CPU Time 8888888" >&2
      RESOURCE_FAILURES["$base"]=1
      rm -f "$tmp_out"
      continue
    fi
    rm -f "$tmp_out"
    exit 1
  fi
  if [[ -s "$tmp_out" ]]; then
    cat "$tmp_out"
  fi
  rm -f "$tmp_out"
done

# Keys to extract from HASH_JOIN extra_info (first occurrence in tree)
EXTRA_KEYS=(
  "Build Time"
  "Probe Time"
  "Probe Time (ExecuteInternal)"
  "Probe Time (ExternalProbe)"
  "ProbeForPointers Time"
  "Match Time"
  "Scan Structure Next Time (ExecuteInternal)"
)

for base in "${SQL_FILES[@]}"; do
  json="$SCRIPT_DIR/${base}.json"
  if [[ -n "${RESOURCE_FAILURES[$base]:-}" ]]; then
    {
      echo "$base"
      echo "CPU Time"
      echo "8888888"
      echo
    } >> "$RESULTS"
    continue
  fi
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