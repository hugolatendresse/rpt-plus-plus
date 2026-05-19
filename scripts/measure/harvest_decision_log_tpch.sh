#!/usr/bin/env bash
# Harvest [THC_DECISION] CSV records from each of the 8 TPC-H queries where
# the THC currently regresses baseline c3 vs thc_disabled c3 (per the
# 2026-05-16 multi-pass overnight run).  For each query, runs once with the
# decision-log emitter on and captures stderr.
#
# Output structure under job_results/decisionlog_tpch_<ts>/:
#   q01.log, q04.log, ...        — full stderr from each invocation
#   q01.csv, q04.csv, ...        — parsed [THC_DECISION] rows with header
#   summary.txt                   — per-query reason distribution + key stats
#
# Single-threaded run (engaged settings has SET threads = 1) so one row per
# JoinHashTable. The "thread" column in the CSV is then the OS thread id of
# the worker, not a meaningful index.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO"

SF="${SF:-10}"
TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="$REPO/job_results/decisionlog_tpch_${TS}"
mkdir -p "$OUT_DIR"

# Settings overlay: base = engaged + emit decision log + build-count mu_s so
# the cycle-1 µ_SR upper-bound has a chance to fire.
CFG="${OUT_DIR}/cfg_harvest.sql"
cat "$REPO/scripts/measure/settings-common-engaged.sql" >"$CFG"
cat >>"$CFG" <<'EOF'

-- decision-log harvest overrides
SET thc_emit_decision_log = true;
SET thc_mu_s_method = 'build_count';
EOF

DUCKDB="$REPO/build/release/duckdb"
DB="$REPO/../benchmark_data/tpch/tpch_sf${SF}.duckdb"
if [[ ! -x "$DUCKDB" ]]; then echo "missing $DUCKDB" >&2; exit 1; fi
if [[ ! -f "$DB" ]]; then echo "missing $DB" >&2; exit 1; fi

# THC_DECISION CSV column header (from EmitDecisionLogRow comment block in
# src/execution/join_hashtable.cpp).
HEADER='ht_addr,thread,reason,cycles,evals,probe_rows,new_entries,collect_rows,u1,mu_sr,perc_hot,miss_rate,c_main,c_eval_cur,c_eval_prev,c_grow,ro_miss,ro_total,abandoned,frozen,planner_R'

run_one() {
	local qnum="$1"
	local qpad
	qpad="$(printf "%02d" "$qnum")"
	local log="${OUT_DIR}/q${qpad}.log"
	local csv="${OUT_DIR}/q${qpad}.csv"
	local sqlfile="${OUT_DIR}/q${qpad}.sql"
	echo "=== Q${qpad} ==="

	# Build the SQL: common engaged settings + decision-log overrides + case-3
	# toggle (rpt_forward_only=true) + the actual TPC-H query.  Write to a file
	# so duckdb reads via stdin redirection rather than a pipeline (pipelines
	# from grouped commands seem to interact badly with duckdb's stdout in
	# non-TTY mode on some platforms).
	{
		grep '^SET ' "$CFG"
		echo 'SET rpt_forward_only = true;'
		echo 'LOAD tpch;'
		printf 'PRAGMA tpch(%d);\n' "$qnum"
	} >"$sqlfile"

	"$DUCKDB" "$DB" <"$sqlfile" >/dev/null 2>"$log" || {
		echo "  query failed; see $log"
		return
	}

	# Tolerate zero THC_DECISION rows (e.g. Q01 has no joins). pipefail would
	# otherwise propagate grep's exit-1 and abort the harvest mid-loop.
	{
		printf '%s\n' "$HEADER"
		grep -E '^\[THC_DECISION\]' "$log" | sed 's/^\[THC_DECISION\] //' || true
	} >"$csv"
	local nrows
	nrows="$(($(wc -l <"$csv") - 1))"
	echo "  $nrows decision rows"
}

for q in 1 4 9 12 13 17 18 21; do
	run_one "$q"
done

# Summary: per-query reason distribution + a couple of representative rows
SUMMARY="${OUT_DIR}/summary.txt"
{
	echo "TPC-H sf${SF} decision-log harvest"
	echo "binary: $(git rev-parse --short HEAD)"
	echo "settings: $CFG"
	echo ""
	for q in 1 4 9 12 13 17 18 21; do
		qpad="$(printf "%02d" "$q")"
		csv="${OUT_DIR}/q${qpad}.csv"
		[[ -f "$csv" ]] || continue
		echo "=== Q${qpad} ==="
		awk -F, 'NR>1{print $3}' "$csv" | sort | uniq -c | sort -rn | awk '{printf "  %s\n", $0}'
		echo "  sample rows (first 3 with non-zero probe_rows):"
		awk -F, 'NR>1 && $6>0' "$csv" | head -3 | awk -F, '{printf "    %s rows=%s cycles=%s evals=%s mu_sr=%s perc_hot=%s miss_rate=%s c_main=%s c_eval=%s\n", $3, $6, $4, $5, $10, $11, $12, $13, $14}'
		echo ""
	done
} >"$SUMMARY"

echo ""
echo "Wrote ${SUMMARY}"
echo "Per-query CSVs under ${OUT_DIR}"
