#!/usr/bin/env bash
# THC parameter search - coordinate descent.
# Phase 1 already found: collect_phase_rows=2000000 → 3.181s
set -euo pipefail

cd "$(cd "$(dirname "$0")/.." && pwd)"

DB="ASH-datagen/bench.duckdb"
RESULTS="ASH-datagen/search_results2.csv"
NUM_RUNS=3

echo "phase,param_name,param_value,budget,collect_rows,first_ro,budget_frac,miss_thresh,activation,avg_time_s" > "$RESULTS"

B_budget=$((60 * 1024 * 1024))
B_collect=1000000
B_first_ro=1000000
B_frac="1.00"
B_miss="1.00"
B_activ=10000

run_one() {
    local tag="$1" pname="$2" pval="$3"
    local budget="$4" collect="$5" first_ro="$6" frac="$7" miss="$8" activ="$9"

    local output avg rc=0
    output=$({
        grep '^SET ' ASH-datagen/settings.sql \
            | grep -v 'thc_collect_phase_rows\|thc_first_read_only_phase_rows\|thc_l3_budget\|thc_collect_budget_fraction\|thc_miss_below_which_skip_collect\|thc_activation_threshold'
        echo "SET thc_l3_budget = ${budget};"
        echo "SET thc_collect_phase_rows = ${collect};"
        echo "SET thc_first_read_only_phase_rows = ${first_ro};"
        echo "SET thc_collect_budget_fraction = ${frac};"
        echo "SET thc_miss_below_which_skip_collect = ${miss};"
        echo "SET thc_activation_threshold = ${activ};"
        echo "CREATE OR REPLACE TEMP TABLE generator_counts AS SELECT * FROM generator_counts_persistent;"
        echo ".read ASH-datagen/query_rs.sql"
        echo "SET VARIABLE t0 = epoch_ms(now());"
        echo ".timer on"
        for i in $(seq 1 $NUM_RUNS); do echo "EXECUTE benchmark_query;"; done
        echo ".timer off"
        echo "SET VARIABLE t_end = epoch_ms(now());"
        echo ".output stdout"
        echo "SELECT printf('%.3f', (getvariable('t_end') - getvariable('t0')) / ${NUM_RUNS}.0 / 1000.0) AS avg_time;"
        echo "SET disabled_optimizers = '';"
        echo "SET threads = getvariable('old_threads');"
        echo "RESET VARIABLE old_threads;"
    } | taskset -c 4-59 build/release/duckdb "$DB" 2>&1) || rc=$?
    if [[ $rc -ne 0 ]]; then
        echo "ERROR in run_one (${tag} ${pname}=${pval}), duckdb exit code ${rc}:" >&2
        echo "$output" >&2
    fi

    avg=$(echo "$output" | grep -oP '\d+\.\d{3}' | tail -1)
    if [[ -z "$avg" ]]; then
        avg="ERR"
    fi

    echo "${tag},${pname},${pval},${budget},${collect},${first_ro},${frac},${miss},${activ},${avg}" >> "$RESULTS"
    printf "  %-30s -> %s s\n" "${pname}=${pval}" "${avg}"
}

pick_best_col() {
    local phase="$1" col="$2"
    awk -F, -v ph="$phase" -v c="$col" \
        '$1==ph && $NF+0>0 { if (!seen || $NF+0<best) { best=$NF; val=$c; seen=1 } } END { print val }' "$RESULTS"
}

echo "=== Phase 2: thc_l3_budget ==="
for mb in 16 24 32 48 64 128 256; do
    run_one P2 budget "${mb}MB" $((mb*1024*1024)) $B_collect $B_first_ro "$B_frac" "$B_miss" $B_activ
done
B_budget=$(pick_best_col P2 4)
echo ">>> Best budget = $B_budget ($(( B_budget / 1024 / 1024 )) MB)"

echo ""
echo "=== Phase 3: thc_first_read_only_phase_rows ==="
for v in 1 1000 10000 100000 1000000 10000000 999999999; do
    run_one P3 first_ro "$v" $B_budget $B_collect $v "$B_frac" "$B_miss" $B_activ
done
B_first_ro=$(pick_best_col P3 6)
echo ">>> Best first_ro = $B_first_ro"

echo ""
echo "=== Phase 4: thc_collect_budget_fraction ==="
for v in 0.10 0.25 0.50 1.00; do
    run_one P4 budget_frac "$v" $B_budget $B_collect $B_first_ro "$v" "$B_miss" $B_activ
done
B_frac=$(pick_best_col P4 7)
echo ">>> Best frac = $B_frac"

echo ""
echo "=== Phase 5: thc_miss_below_which_skip_collect ==="
for v in 0.01 0.05 0.10; do
    run_one P5 miss_thresh "$v" $B_budget $B_collect $B_first_ro "$B_frac" "$v" $B_activ
done
B_miss=$(pick_best_col P5 8)
echo ">>> Best miss = $B_miss"

echo ""
echo "=== Phase 6: thc_activation_threshold ==="
for v in 10000 100000 1000000 ; do
    run_one P6 activation "$v" $B_budget $B_collect $B_first_ro "$B_frac" "$B_miss" "$v"
done
B_activ=$(pick_best_col P6 9)
echo ">>> Best activ = $B_activ"

echo ""
echo "=== Phase 7: Re-sweep collect_phase_rows with best settings ==="
for v in  10000 50000 100000 250000 500000 1000000 1500000 2000000; do
    run_one P7 collect "$v" $B_budget $v $B_first_ro "$B_frac" "$B_miss" $B_activ
done
B_collect=$(pick_best_col P7 5)
echo ">>> Best collect = $B_collect"

echo ""
echo "=== Phase 8: Fine-tune budget around best ==="
best_mb=$(( B_budget / 1024 / 1024 ))
for mb in $(seq $((best_mb > 8 ? best_mb - 8 : 4)) 4 $((best_mb + 16))); do
    run_one P8 budget "${mb}MB" $((mb*1024*1024)) $B_collect $B_first_ro "$B_frac" "$B_miss" $B_activ
done
B_budget=$(pick_best_col P8 4)
echo ">>> Best budget (fine) = $B_budget ($(( B_budget / 1024 / 1024 )) MB)"

echo ""
echo "=== FINAL: verification (5 runs) ==="
NUM_RUNS=5
run_one FINAL best all $B_budget $B_collect $B_first_ro "$B_frac" "$B_miss" $B_activ

echo ""
echo "========================================"
echo " BEST SETTINGS"
echo "========================================"
echo "SET thc_l3_budget = $B_budget;"
echo "SET thc_collect_phase_rows = $B_collect;"
echo "SET thc_first_read_only_phase_rows = $B_first_ro;"
echo "SET thc_collect_budget_fraction = $B_frac;"
echo "SET thc_miss_below_which_skip_collect = $B_miss;"
echo "SET thc_activation_threshold = $B_activ;"
echo ""
echo "Full CSV: $RESULTS"
column -t -s, "$RESULTS"
