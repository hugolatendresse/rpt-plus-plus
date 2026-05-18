#!/usr/bin/env bash
# Overnight THC benchmark matrix for TPC-DS sf10.
#
# Sibling of run_overnight_matrix_tpch.sh. Same 7-config grid run against the
# 99-query TPC-DS suite at scale factor 10. TPC-DS has joins on wider keys in
# several places (catalog_sales, store_sales fact tables vs dimension tables)
# so it is the natural workload to settle the pointer-mode question that JOB
# and TPC-H both regressed on.
#
# Required env (defaults shown):
#   PASSES=5                       # how many independent passes per config
#   CASES=2,3                      # which run_tpc.sh cases to run
#   SF=10                          # TPC-DS scale factor (must match data on disk)
#
# Usage:
#   scripts/measure/run_overnight_matrix_tpcds.sh

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO"

SHA="$(git rev-parse --short HEAD)"
TS="$(date +%Y%m%d_%H%M%S)"
PASSES="${PASSES:-5}"
CASES="${CASES:-2,3}"
SF="${SF:-10}"

OUT_DIR="$REPO/job_results/overnight_tpcds_${TS}"
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

# Same config grid as JOB and TPC-H matrices.
add_cfg "baseline" ""
add_cfg "ptr_64"  "SET thc_pointer_mode_min_row_size = 64;"
add_cfg "ptr_96"  "SET thc_pointer_mode_min_row_size = 96;"
add_cfg "ptr_128" "SET thc_pointer_mode_min_row_size = 128;"
add_cfg "no_probe_floor" "SET thc_collect_phase_rows = 1;"
add_cfg "no_hot_fraction" "SET thc_max_estimated_perc_hot = 1.0;"
add_cfg "thc_disabled" "SET disable_tiered_hash_cache = true;"
add_cfg "collect_20k" "SET thc_collect_phase_rows = 20000;"

echo "TPC-DS matrix start: ${TS} SHA=${SHA} PASSES=${PASSES} CASES=${CASES} SF=${SF}"
echo "Output: ${OUT_DIR}"

for i in "${!CFG_NAMES[@]}"; do
	name="${CFG_NAMES[$i]}"
	override="${CFG_OVERRIDES[$i]}"
	cfg_sql="${OUT_DIR}/cfg_${name}.sql"
	csv="${OUT_DIR}/ab_tpcds_${name}_${SHA}.csv"

	echo "=== Config: ${name} ==="
	if [[ -n "$override" ]]; then
		write_settings "$cfg_sql" "$override"
	else
		write_settings "$cfg_sql"
	fi

	COMMON_SETTINGS_SQL="$cfg_sql" \
		"$REPO/scripts/measure/run_tpc.sh" \
		--sf "$SF" \
		--tpcds-only \
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

echo ""
echo "=== Aggregate per-config c3 minus c2 (mean over all passes) ==="
for i in "${!CFG_NAMES[@]}"; do
	name="${CFG_NAMES[$i]}"
	csv="${OUT_DIR}/ab_tpcds_${name}_${SHA}.csv"
	if [[ ! -s "$csv" ]]; then
		printf '%-20s SKIPPED (no data)\n' "$name"
		continue
	fi
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
echo "TPC-DS matrix complete: ${TS}"
echo "All output under ${OUT_DIR}"
