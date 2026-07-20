#!/usr/bin/env bash
# Phase 7b: same ASH-datagen grid as phase7_ash_grid.sh, but with
# skip_unfiltered_tables_graph_creation / skip_unfiltered_tables_create_bf_plan
# turned OFF so the RPT+ forward pass actually builds Bloom filters on this
# benchmark. With the default (skip=true) settings, ash-datagen tables carry
# no base-table filter (selected_fraction = 1.00), so RPT+ creates zero BFs
# and cases 1/2/4 are byte-identical plans (verified via profiling JSONs:
# no CREATE_BF/USE_BF operators). This variant measures what PT can do on the
# THC-motivating workload when allowed to pass join-key information:
# a BF built from R's join keys prunes the cold portion of S before the hash
# table is built ("RPT+ wins via scan + build-HT" hypothesis from the meeting
# notes).
#
# Output: results/analysis/phase7/ash_grid_bf.csv (+ profiling JSONs).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="results/analysis/phase7"
mkdir -p "$OUT_DIR" "$OUT_DIR/profiling_bf"
CSV="$OUT_DIR/ash_grid_bf.csv"
RUNS=5
DB="ASH-datagen/bench_phase7.duckdb"
TASKSET_PREFIX=(taskset -c 4-59)

CONFIGS=(
    "0.02 63 40000"
    "0.05 20 40000"
    "0.10 10 40000"
    "0.25 4 40000"
    "0.50 2 40000"
    "1.00 1 40000"
    "0.02 63 160000"
    "0.10 10 160000"
)

case_settings_for() {
    case "$1" in
        1) printf '%s\n' "SET disable_rpt = true;" "SET disable_tiered_hash_cache = true;" ;;
        2) printf '%s\n' "SET rpt_forward_only = true;" "SET disable_tiered_hash_cache = true;" ;;
        3) printf '%s\n' "SET rpt_forward_only = true;" ;;
        4) printf '%s\n' "SET disable_tiered_hash_cache = true;" ;;
    esac
}

common_settings() {
    grep '^SET ' scripts/measure/settings-common.sql | sed 's/^SET threads *=.*/SET threads = 1;/'
    echo "SET disabled_optimizers = 'join_order,build_side_probe_side,statistics_propagation';"
    # The one deliberate difference vs phase7_ash_grid.sh:
    echo "SET skip_unfiltered_tables_graph_creation = false;"
    echo "SET skip_unfiltered_tables_create_bf_plan = false;"
}

gen_settings_file() {
    local frac="$1" mult="$2" scale="$3" out="$4"
    cat >"$out" <<EOF
SET VARIABLE scale_factor = ${scale};
SET VARIABLE base_row_count_R = 100;
SET VARIABLE base_row_count_S = 100;
SET VARIABLE base_row_count_T = 100;
SET VARIABLE selected_fraction_R = 1.00;
SET VARIABLE selected_fraction_S = 1.00;
SET VARIABLE selected_fraction_T = 1.00;
SET VARIABLE join_fraction_RS = ${frac};
SET VARIABLE join_fraction_ST = 0.00;
SET VARIABLE bridge_fraction = 0.00;
SET VARIABLE probe_multiplicity_in_R = ${mult};
SET VARIABLE probe_multiplicity_in_S = 1;
SET VARIABLE unproductive_rate_RS = 0.00;
SET VARIABLE unproductive_rate_ST = 0.00;
EOF
}

echo "hot_frac,mult,scale,case,run,runtime_seconds" >"$CSV"

for cfg in "${CONFIGS[@]}"; do
    read -r FRAC MULT SCALE <<<"$cfg"
    echo "=== config: hot_frac=$FRAC mult=$MULT scale=$SCALE ==="
    GEN_SETTINGS="$(mktemp)"
    gen_settings_file "$FRAC" "$MULT" "$SCALE" "$GEN_SETTINGS"
    COMMON_SETTINGS_SQL="scripts/measure/settings-common.sql" \
        RUN_SETTINGS_SQL="$GEN_SETTINGS" \
        "ASH-datagen/run_generation.sh" "$DB" >/dev/null
    rm -f "$GEN_SETTINGS"

    for c in 1 2 3 4; do
        SQL="$(mktemp)"
        {
            common_settings
            case_settings_for "$c"
            echo "SET transfer_graph_seed = 0;"
            echo "CREATE OR REPLACE TEMP TABLE generator_counts AS SELECT * FROM generator_counts_persistent;"
            echo ".read ASH-datagen/query_rs.sql"
            echo ".mode csv"
            echo ".headers off"
            for ((i = 1; i <= RUNS; i++)); do
                echo "SET VARIABLE _t0_${i} = epoch_ms(now());"
                echo "EXECUTE benchmark_query;"
                echo ".output stdout"
                echo "SELECT printf('PERRUN_S=%d=%.6f', ${i}, (epoch_ms(now()) - getvariable('_t0_${i}')) / 1000.0);"
                echo ".output /dev/null"
            done
        } >"$SQL"
        OUT="$(mktemp)"
        "${TASKSET_PREFIX[@]}" build/release/duckdb "$DB" <"$SQL" >"$OUT" 2>&1 || {
            echo "case $c FAILED:"; cat "$OUT"; rm -f "$SQL" "$OUT"; continue
        }
        while IFS= read -r line; do
            line="${line%$'\r'}"
            if [[ "$line" =~ ^PERRUN_S=([0-9]+)=([0-9.]+)$ ]]; then
                echo "$FRAC,$MULT,$SCALE,$c,${BASH_REMATCH[1]},${BASH_REMATCH[2]}" >>"$CSV"
            fi
        done <"$OUT"
        rm -f "$SQL" "$OUT"

        PROF="$OUT_DIR/profiling_bf/ash_f${FRAC}_m${MULT}_s${SCALE}_case${c}.json"
        SQL="$(mktemp)"
        {
            common_settings
            case_settings_for "$c"
            echo "SET transfer_graph_seed = 0;"
            echo "CREATE OR REPLACE TEMP TABLE generator_counts AS SELECT * FROM generator_counts_persistent;"
            echo ".output /dev/null"
            sed -n '/^PREPARE/,/;/{/^PREPARE/!p}' ASH-datagen/query_rs.sql
            echo "PRAGMA enable_profiling = 'json';"
            echo "PRAGMA profiling_output = '$PROF';"
            sed -n '/^PREPARE/,/;/{/^PREPARE/!p}' ASH-datagen/query_rs.sql
            echo "PRAGMA enable_profiling = 'no_output';"
        } >"$SQL"
        "${TASKSET_PREFIX[@]}" build/release/duckdb "$DB" <"$SQL" >/dev/null 2>&1 || echo "  (profiling run failed for case $c)"
        rm -f "$SQL"
        avg=$(awk -F, -v f="$FRAC" -v m="$MULT" -v s="$SCALE" -v c="$c" \
            '$1==f && $2==m && $3==s && $4==c {t+=$6; n++} END {if (n) printf "%.3f", t/n}' "$CSV")
        echo "  case $c: avg ${avg}s over $RUNS runs"
    done
done

echo "Grid complete. CSV: $CSV"
