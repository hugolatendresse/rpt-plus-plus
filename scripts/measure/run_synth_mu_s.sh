#!/usr/bin/env bash
set -euo pipefail

# Synthetic mu_s experiment: generate two-table joins with different build-side within multiplicities
# and compare mu_s estimation methods (none, build_count, probe_sample, ht_sample).
#
# Usage:
#   scripts/measure/run_synth_mu_s.sh \
#     --rows 2000000 \
#     --mus "1 2 3 4 5 10" \
#     --runs 5 \
#     --methods "none build_count probe_sample ht_sample" \
#     --out ./results/tpch/synth_mu_s
#
# Notes:
# - Uses DuckDB CLI at ./build/release/duckdb
# - Creates DBs under ../benchmark_data/synth/
# - For each MU, generates a build table with skewed multiplicity (10% hot keys, 90% cold keys)
# - Probe table has the same number of rows with keys uniform over build key domain
# - Measures latency via profiling results.json and collects stderr logs for mu_s

ROWS=2000000
MUS="1 2 3 4 5 10"
RUNS=5
METHODS="none build_count probe_sample ht_sample"
DUCKDB_BIN="./build/release/duckdb"
DB_DIR="../benchmark_data/synth"
OUT_DIR="./results/tpch/synth_mu_s"
THREADS=16
DROP_OS_CACHE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rows) ROWS="$2"; shift 2 ;;
    --mus) MUS="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --methods) METHODS="$2"; shift 2 ;;
    --duckdb) DUCKDB_BIN="$2"; shift 2 ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    --drop-os-cache) DROP_OS_CACHE=true; shift ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

mkdir -p "$DB_DIR" "$OUT_DIR"

if [[ ! -x "$DUCKDB_BIN" ]]; then
  echo "DuckDB binary not found: $DUCKDB_BIN" >&2
  exit 1
fi

log() { echo "[synth_mu_s] $*"; }

drop_os_page_cache() {
  if ! $DROP_OS_CACHE; then
    return
  fi
  # Linux page cache survives across DuckDB CLI processes. Keep cold-cache
  # measurements explicit because this sudo operation affects the whole host.
  log "Dropping Linux page cache before profiled query"
  sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
}

# Helper: write SQL to generate skewed build/probe tables for a target average mu
# Approach: choose m_hot = round(2*mu), m_cold = max(1, round(0.5*mu)); K ~ N / (0.1*m_hot + 0.9*m_cold)
# Then build rows = H*m_hot + (K-H)*m_cold ~ N (close enough for large N)

for MU in $MUS; do
  DB="$DB_DIR/synth_mu_${MU}.duckdb"
  log "Generating DB for mu=$MU at $DB (ROWS=$ROWS)"

  # Compute integers in bash using awk for rounding
  # Choose 10% hot keys with multiplicity m_hot to hit exact average mu with m_cold=1
  M_COLD=1
  M_HOT=$(awk -v mu="$MU" 'BEGIN{printf("%d", (10*mu - 9))}')
  if [[ "$M_HOT" -lt 1 ]]; then M_HOT=1; fi
  # Effective multiplicity per key mix (should equal mu approximately with integer rounding)
  DEN=$(awk -v mh="$M_HOT" -v mc="$M_COLD" 'BEGIN{printf("%.6f", (0.1*mh + 0.9*mc))}')
  K_FLOAT=$(awk -v n="$ROWS" -v den="$DEN" 'BEGIN{printf("%f", n/den)}')
  K=$(awk -v k="$K_FLOAT" 'BEGIN{printf("%d", k)}')
  if [[ "$K" -lt 2 ]]; then K=2; fi
  H=$(( K / 10 ))
  if [[ "$H" -lt 1 ]]; then H=1; fi

  log "Params: mu=$MU m_hot=$M_HOT m_cold=$M_COLD K=$K H=$H (expected avg≈$DEN)"

  # Generate SQL - use double quotes so bash can substitute variables
  SQL_GEN=$(cat <<SQL
PRAGMA disable_profiling;
SET threads=$THREADS;

DROP TABLE IF EXISTS build;
DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS key_ranges;

-- Simple approach: create key_ranges table with (key, reps)
-- Then expand each key with the specified repetitions
CREATE TEMP TABLE key_ranges (k INTEGER, reps INTEGER);

-- Insert hot keys (keys 1..H with m_hot reps each)
INSERT INTO key_ranges SELECT key_id, $M_HOT FROM range(1, $H+1) AS hot(key_id);

-- Insert cold keys (keys H+1..K with 1 rep each)
INSERT INTO key_ranges SELECT key_id, 1 FROM range($H+1, $K+1) AS cold(key_id);

-- Build table: expand each (k, reps) into reps rows
CREATE TABLE build AS
SELECT kr.k, rep_id AS rid, random() AS payload
FROM key_ranges kr
CROSS JOIN range(1, kr.reps + 1) AS rep_range(rep_id);

-- Probe table: uniform keys over domain [1..K], same cardinality as build
CREATE TABLE probe AS
SELECT ((i % $K) + 1) AS k, i AS rid, random() AS payload
FROM range(1, (SELECT COUNT(*) FROM build)+1) AS i(i);

-- Analyze to stabilize join planning
ANALYZE;
SQL
)

  # Create DB and generate data
  echo "$SQL_GEN" | "$DUCKDB_BIN" "$DB" >/dev/null

  # Compute ground-truth mu_s for this build
  GT=$("$DUCKDB_BIN" "$DB" -c "COPY (SELECT CAST(COUNT(*) AS DOUBLE)/COUNT(DISTINCT k) FROM build) TO '/dev/stdout' (FORMAT CSV, HEADER false);" | tr -d '[:space:]')
  log "Ground-truth mu_s(build) ~ $GT"

  for METHOD in $METHODS; do
    log "Running method=$METHOD mu=$MU runs=$RUNS"
    RUN_DIR="$OUT_DIR/mu_${MU}/$METHOD"; mkdir -p "$RUN_DIR"
    : > "$RUN_DIR/times.txt"

    for ((r=1; r<=RUNS; r++)); do
      RES_JSON="$RUN_DIR/results_run${r}.json"
      LOG_ERR="$RUN_DIR/stderr_run${r}.log"
      # Settings
      case "$METHOD" in
        none)
          SETTINGS="SET thc_mu_s_method='none'; SET thc_log_mu_s=false; SET disable_tiered_hash_cache=false;"
          ;;
        build_count)
          SETTINGS="SET thc_mu_s_method='build_count'; SET thc_log_mu_s=true; SET disable_tiered_hash_cache=true;"
          ;;
        ht_sample)
          SETTINGS="SET thc_mu_s_method='ht_sample'; SET thc_log_mu_s=true; SET disable_tiered_hash_cache=true;"
          ;;
        probe_sample)
          SETTINGS="SET thc_mu_s_method='probe_sample'; SET thc_log_mu_s=true; SET disable_tiered_hash_cache=false;"
          ;;
        *) echo "Unknown method $METHOD" >&2; exit 1 ;;
      esac

      QUERY=$(cat <<'Q'
PRAGMA enable_profiling='json';
PRAGMA profiling_output='results.json';
PRAGMA profiling_coverage='SELECT';
-- Simple join to trigger HT build/probe
    SELECT COUNT(*) FROM build b JOIN probe p USING (k);
Q
)
      drop_os_page_cache
      echo -e "$SETTINGS\n$QUERY" | "$DUCKDB_BIN" "$DB" 2>"$LOG_ERR" >/dev/null || true
      # Copy profiling file if present
      if [[ -f results.json ]]; then
        cp results.json "$RES_JSON"
      fi
      # Extract latency
      LAT=$(jq -r '.latency' "$RES_JSON" 2>/dev/null || echo "NaN")
      echo "$LAT" >> "$RUN_DIR/times.txt"
      # Extract last mu_s line if any
      tail -n 50 "$LOG_ERR" | sed -n 's/.*\(\[mu_s [^]]*\].*\)/\1/p' >> "$RUN_DIR/mu_s.log" || true
    done

    # Summarize times (bash/awk)
  done

done

log "Done. Results under $OUT_DIR"
