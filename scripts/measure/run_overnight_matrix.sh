#!/usr/bin/env bash
# Overnight THC benchmark matrix.
#
# Runs the JOB sweep across several settings variants using the multi-pass
# harness so each (case, seed, query) tuple is sampled N times.  Output goes
# under job_results/overnight_<timestamp>/ — one CSV per config plus a tarball
# of the config SQL files used.
#
# Required env (defaults shown):
#   PASSES=5                       # how many independent passes per config
#   SEEDS=5                        # how many transfer_graph seeds to sweep
#   CASES=2,3                      # which run_job.sh cases to run
#
# Usage:
#   scripts/measure/run_overnight_matrix.sh
#
# Designed for low-noise overnight execution: don't pin or nice, don't kill
# competing workloads — just measure what the host gives us and let the
# multi-pass average wash out per-run jitter.  If the host has noisy
# neighbours, increase PASSES.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO"

SHA="$(git rev-parse --short HEAD)"
TS="$(date +%Y%m%d_%H%M%S)"
PASSES="${PASSES:-5}"
SEEDS="${SEEDS:-5}"
CASES="${CASES:-2,3}"

OUT_DIR="$REPO/job_results/overnight_${TS}"
mkdir -p "$OUT_DIR"

# Write a settings file rooted at settings-common-engaged.sql plus any extra
# SET statements appended after a separator comment.
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

# Each entry: cfg_name "set statements..."
declare -a CFG_NAMES=()
declare -a CFG_OVERRIDES=()

add_cfg() {
	CFG_NAMES+=("$1")
	CFG_OVERRIDES+=("$2")
}

# 1) Baseline — settings-common-engaged.sql defaults.  All in-binary
#    optimizations active (probe-floor, hot-fraction, adaptive-budget).
add_cfg "baseline" ""

# 2) Pointer-mode threshold sweep.  Probe-side row floor + hot-fraction
#    + adaptive-budget all stay on; we vary thc_pointer_mode_min_row_size.
add_cfg "ptr_off" "SET thc_pointer_mode_min_row_size = 18446744073709551615;"
add_cfg "ptr_64"  "SET thc_pointer_mode_min_row_size = 64;"
add_cfg "ptr_96"  "SET thc_pointer_mode_min_row_size = 96;"
add_cfg "ptr_128" "SET thc_pointer_mode_min_row_size = 128;"

# 3) Disable each new gate individually so we can isolate its contribution.
#    These configs preserve the rest of the new machinery.
add_cfg "no_probe_floor" "SET thc_collect_phase_rows = 1;"   # 2x = 2 rows; gate vacuous
add_cfg "no_hot_fraction" "SET thc_max_estimated_perc_hot = 1.0;"  # gate never fires
add_cfg "no_decision_log" "SET thc_emit_decision_log = false;"  # no-op for c2/c3 numbers

# Per-config runner.
echo "Matrix start: ${TS} SHA=${SHA} PASSES=${PASSES} SEEDS=${SEEDS} CASES=${CASES}"
echo "Output: ${OUT_DIR}"

for i in "${!CFG_NAMES[@]}"; do
	name="${CFG_NAMES[$i]}"
	override="${CFG_OVERRIDES[$i]}"
	cfg_sql="${OUT_DIR}/cfg_${name}.sql"
	csv="${OUT_DIR}/ab_${name}_${SHA}.csv"

	echo "=== Config: ${name} ==="
	echo "  settings: ${cfg_sql}"
	echo "  csv:      ${csv}"

	if [[ -n "$override" ]]; then
		write_settings "$cfg_sql" "$override"
	else
		write_settings "$cfg_sql"
	fi

	COMMON_SETTINGS_SQL="$cfg_sql" \
		"$REPO/scripts/measure/run_job.sh" \
		--cases "$CASES" \
		--seeds "$SEEDS" \
		--passes "$PASSES" \
		--csv "$csv" \
		>"${OUT_DIR}/log_${name}.txt" 2>&1 || {
			echo "  FAILED — see ${OUT_DIR}/log_${name}.txt"
			continue
		}

	rows="$(wc -l <"$csv")"
	echo "  done; ${rows} rows in CSV"
done

# Final summary table per config.
echo ""
echo "=== Aggregate per-config c3 minus c2 (mean over all passes) ==="
for i in "${!CFG_NAMES[@]}"; do
	name="${CFG_NAMES[$i]}"
	csv="${OUT_DIR}/ab_${name}_${SHA}.csv"
	if [[ ! -s "$csv" ]]; then
		printf '%-20s SKIPPED (no data)\n' "$name"
		continue
	fi
	awk -F, -v label="$name" '
		NR > 1 && $2 == 2 { c2_sum += $5; c2_n++ }
		NR > 1 && $2 == 3 { c3_sum += $5; c3_n++ }
		END {
			printf "%-20s c2_total=%.2fs (n=%d)  c3_total=%.2fs (n=%d)  c3-c2_total=%+.2fs\n",
			       label, c2_sum, c2_n, c3_sum, c3_n, c3_sum - c2_sum
		}
	' "$csv"
done

echo ""
echo "Matrix complete: ${TS}"
echo "All output under ${OUT_DIR}"
