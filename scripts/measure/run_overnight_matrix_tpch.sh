#!/usr/bin/env bash
# Overnight THC benchmark matrix for TPC-H sf10.
#
# Mirrors run_overnight_matrix.sh's structure but drives run_tpc.sh against
# TPC-H sf10 instead of JOB.  Same config set so JOB and TPC-H results are
# directly comparable per-config.  TPC-H lineitem has wider rows than JOB
# (~120 bytes for a materialized HT) so this is the workload where
# pointer-mode might pay off.
#
# Required env (defaults shown):
#   PASSES=5                       # how many independent passes per config
#   CASES=2,3                      # which run_tpc.sh cases to run
#   SF=10                          # TPC-H scale factor (must match data on disk)
#
# Usage:
#   scripts/measure/run_overnight_matrix_tpch.sh

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO"

SHA="$(git rev-parse --short HEAD)"
TS="$(date +%Y%m%d_%H%M%S)"
PASSES="${PASSES:-5}"
CASES="${CASES:-2,3}"
SF="${SF:-10}"

OUT_DIR="$REPO/job_results/overnight_tpch_${TS}"
mkdir -p "$OUT_DIR"

write_settings() {
	local out="$1"
	shift
	cat "$REPO/scripts/measure/settings-common-engaged.sql" >"$out"
	if [[ $# -gt 0 ]]; then
		printf '\n-- overnight matrix override\n' >>"$out"
		for sql in "$@"; do
			printf '%s\n' "$sql" >>"$out"
		done
	fi
}

declare -a CFG_NAMES=()
declare -a CFG_OVERRIDES=()
add_cfg() { CFG_NAMES+=("$1"); CFG_OVERRIDES+=("$2"); }

# Same config grid as the JOB matrix.
add_cfg "baseline" ""
add_cfg "ptr_64"  "SET thc_pointer_mode_min_row_size = 64;"
add_cfg "ptr_96"  "SET thc_pointer_mode_min_row_size = 96;"
add_cfg "ptr_128" "SET thc_pointer_mode_min_row_size = 128;"
add_cfg "no_probe_floor" "SET thc_collect_phase_rows = 1;"
add_cfg "no_hot_fraction" "SET thc_max_estimated_perc_hot = 1.0;"
add_cfg "thc_disabled" "SET disable_tiered_hash_cache = true;"

echo "TPC-H matrix start: ${TS} SHA=${SHA} PASSES=${PASSES} CASES=${CASES} SF=${SF}"
echo "Output: ${OUT_DIR}"

for i in "${!CFG_NAMES[@]}"; do
	name="${CFG_NAMES[$i]}"
	override="${CFG_OVERRIDES[$i]}"
	cfg_sql="${OUT_DIR}/cfg_${name}.sql"
	csv="${OUT_DIR}/ab_tpch_${name}_${SHA}.csv"

	echo "=== Config: ${name} ==="
	if [[ -n "$override" ]]; then
		write_settings "$cfg_sql" "$override"
	else
		write_settings "$cfg_sql"
	fi

	COMMON_SETTINGS_SQL="$cfg_sql" \
		"$REPO/scripts/measure/run_tpc.sh" \
		--sf "$SF" \
		--tpch-only \
		--cases "$CASES" \
		--passes "$PASSES" \
		--csv "$csv" \
		--out-dir "$OUT_DIR" \
		>"${OUT_DIR}/log_${name}.txt" 2>&1 || {
			echo "  FAILED — see ${OUT_DIR}/log_${name}.txt"
			continue
		}

	rows="$(wc -l <"$csv")"
	echo "  done; ${rows} rows in CSV"
done

# Aggregate per-config c3 minus c2.
echo ""
echo "=== Aggregate per-config c3 minus c2 (mean over all passes) ==="
for i in "${!CFG_NAMES[@]}"; do
	name="${CFG_NAMES[$i]}"
	csv="${OUT_DIR}/ab_tpch_${name}_${SHA}.csv"
	if [[ ! -s "$csv" ]]; then
		printf '%-20s SKIPPED (no data)\n' "$name"
		continue
	fi
	# TPC-H CSV columns: query,case,pass,runtime_seconds
	awk -F, -v label="$name" '
		NR > 1 && $2 == 2 { c2_sum += $4; c2_n++ }
		NR > 1 && $2 == 3 { c3_sum += $4; c3_n++ }
		END {
			printf "%-20s c2_total=%.2fs (n=%d)  c3_total=%.2fs (n=%d)  c3-c2_total=%+.2fs\n",
			       label, c2_sum, c2_n, c3_sum, c3_n, c3_sum - c2_sum
		}
	' "$csv"
done

echo ""
echo "TPC-H matrix complete: ${TS}"
echo "All output under ${OUT_DIR}"
