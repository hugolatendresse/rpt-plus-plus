#!/usr/bin/env bash
# Run THC benchmark: generate data (optionally) then run the join query under perf (optionally).
#
# Usage:
#   ./scripts/measure/run_hugo_generated.sh [OPTIONS]
#
# Options:
#   --cold N        Cold-to-hot ratio: 1, 5, 10, 100  (default: 10)
#   --layout L      Layout: interleaved, segmented     (default: interleaved)
#   --mult M        Multiplicity suffix (REQUIRED): selects file
#                   scripts/measure/generation/<cold>_cold_<layout>_<mult>x.sql
#                   (programmer's responsibility to ensure that file exists)
#   --case N        Optimizer case (mutually exclusive with --cases):
#                     1 = Old DuckDB (no RPT, no THC)
#                     2 = RPT+ Forward Pass Only
#                     3 = RPT+ Forward + THC
#                     4 = RPT+ Forward + Backward
#   --cases <list>  Comma-separated case list, e.g. 2,3,4
#   --runs N        Run the query N times and print average (default: 1)
#   --no-warmup     Skip the warmup EXECUTE before timed iterations
#   --seed <int>    Override transfer_graph_seed (mutually exclusive with --seeds)
#   --seeds <N>     Sweep seeds 0..N-1 (overrides transfer_graph_seed)
#   --csv <path>    Write per-run CSV (auto-named if omitted in sweep mode)
#   --generate      (Re)generate the data before running the query
#   --perf          Run under perf stat
#   --profile       Enable DuckDB JSON profiling output
#   --drop-os-cache Drop Linux page cache before measured/profiled DuckDB runs
#   --debug         Use debug build (build/debug/duckdb) instead of release for the benchmark
#   --no-taskset    Don't pin DuckDB to cores 4-59 via taskset (pinning is on by default)
#
# Examples:
#   ./scripts/measure/run_hugo_generated.sh --case 3 --mult 100                                # 10 cold, interleaved, 100x, RPT+Forward+THC
#   ./scripts/measure/run_hugo_generated.sh --case 1 --mult 100 --generate                     # generate data first, Old DuckDB
#   ./scripts/measure/run_hugo_generated.sh --case 4 --cold 100 --layout segmented --mult 1000 --generate --perf
#
# Sweep example (box plots):
#   ./scripts/measure/run_hugo_generated.sh --cases 2,3,4 --mult 100 --seeds 5
set -euo pipefail

COLD=10
LAYOUT=interleaved
MULT=""
CASE=""
CASES_LIST=""
RUNS=1
SEED=""
SEEDS_COUNT=""
CSV_PATH=""
GENERATE=false
USE_PERF=false
PROFILE=false
DROP_OS_CACHE=false
USE_TASKSET=true
NO_WARMUP=false
BUILD_TYPE=release

usage() {
    cat <<'USAGE'
Usage: scripts/measure/run_hugo_generated.sh (--case <c> | --cases <list>) [options]

Options:
  --cold <1|5|10|100>   Cold-to-hot ratio (default: 10)
  --layout <L>          Layout: interleaved, segmented (default: interleaved)
  --mult <int>          Multiplicity suffix (REQUIRED): selects file
                        scripts/measure/generation/<cold>_cold_<layout>_<mult>x.sql
  --case <1|2|3|4>      Optimizer case (mutually exclusive with --cases)
  --cases <list>        Comma-separated case list, e.g. 2,3,4
  --runs <N>            EXECUTE iterations per (case, seed) tuple (default: 1)
  --no-warmup           Skip the warmup EXECUTE before timed iterations
  --seed <int>          Override transfer_graph_seed (mutually exclusive with --seeds)
  --seeds <N>           Sweep seeds 0..N-1 (overrides transfer_graph_seed)
  --csv <path>          Write per-run CSV (auto-named if omitted in sweep mode)
  --generate            (Re)generate the data before running the query
  --perf                Run under perf stat
  --profile, --duckdb-profiling
                        Enable DuckDB JSON profiling output
  --drop-os-cache       Run sync + drop Linux page cache before measured and
                        profiled DuckDB queries. Requires sudo and affects the
                        whole host.
  --debug               Use debug build (build/debug/duckdb) instead of release
  --no-taskset          Don't pin DuckDB to cores 4-59 via taskset
  -h, --help            Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cold)      COLD="$2";    shift 2 ;;
        --layout)    LAYOUT="$2";  shift 2 ;;
        --mult)      MULT="$2";    shift 2 ;;
        --case)      CASE="$2";    shift 2 ;;
        --cases)     CASES_LIST="$2"; shift 2 ;;
        --runs)      RUNS="$2";    shift 2 ;;
        --seed)      SEED="$2";    shift 2 ;;
        --seeds)     SEEDS_COUNT="$2"; shift 2 ;;
        --csv)       CSV_PATH="$2"; shift 2 ;;
        --generate)  GENERATE=true;  shift ;;
        --perf)      USE_PERF=true;  shift ;;
        --profile|--duckdb-profiling)   PROFILE=true;   shift ;;
        --drop-os-cache) DROP_OS_CACHE=true; shift ;;
        --debug)     BUILD_TYPE=debug;   shift ;;
        --no-taskset) USE_TASKSET=false; shift ;;
        --no-warmup) NO_WARMUP=true; shift ;;
        -h|--help)   usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

case "$COLD" in
    1|5|10|100) ;;
    *) echo "Error: --cold must be 1, 5, 10, or 100 (got: $COLD)"; exit 1 ;;
esac
case "$LAYOUT" in
    interleaved|segmented) ;;
    *) echo "Error: --layout must be interleaved or segmented (got: $LAYOUT)"; exit 1 ;;
esac
if [[ -z "$MULT" ]]; then
    echo "Error: --mult is required (positive integer). See --help." >&2; exit 1
fi
if ! [[ "$MULT" =~ ^[0-9]+$ ]] || [[ "$MULT" -lt 1 ]]; then
    echo "Error: --mult must be a positive integer (got: $MULT)" >&2; exit 1
fi
if [[ -n "$CASE" && -n "$CASES_LIST" ]]; then
    echo "Error: --case and --cases are mutually exclusive." >&2; exit 1
fi
if [[ -n "$SEED" && -n "$SEEDS_COUNT" ]]; then
    echo "Error: --seed and --seeds are mutually exclusive." >&2; exit 1
fi
if [[ -z "$CASE" && -z "$CASES_LIST" ]]; then
    echo "Error: --case or --cases is required (1, 2, 3, or 4). See --help." >&2; exit 1
fi
if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
    echo "Error: --runs must be a positive integer" >&2; exit 1
fi

CASES=()
if [[ -n "$CASE" ]]; then
    CASES=("$CASE")
else
    IFS=',' read -r -a CASES <<<"$CASES_LIST"
fi
for c in "${CASES[@]}"; do
    case "$c" in
        1|2|3|4) ;;
        *) echo "Error: case must be 1, 2, 3, or 4 (got: $c)" >&2; exit 1 ;;
    esac
done

case_settings_for() {
    case "$1" in
        1) printf '%s\n' "SET disable_rpt = true;" "SET disable_tiered_hash_cache = true;" ;;
        2) printf '%s\n' "SET rpt_forward_only = true;" "SET disable_tiered_hash_cache = true;" ;;
        3) printf '%s\n' "SET rpt_forward_only = true;" ;;
        4) printf '%s\n' "SET disable_tiered_hash_cache = true;" ;;
    esac
}

SEEDS=()
SWEEPING_SEEDS=false
if [[ -n "$SEEDS_COUNT" ]]; then
    if ! [[ "$SEEDS_COUNT" =~ ^[0-9]+$ ]] || [[ "$SEEDS_COUNT" -lt 1 ]]; then
        echo "Error: --seeds must be a positive integer (got: $SEEDS_COUNT)" >&2; exit 1
    fi
    for ((i = 0; i < SEEDS_COUNT; i++)); do SEEDS+=("$i"); done
    SWEEPING_SEEDS=true
elif [[ -n "$SEED" ]]; then
    if ! [[ "$SEED" =~ ^[0-9]+$ ]]; then
        echo "Error: --seed must be a non-negative integer (got: $SEED)" >&2; exit 1
    fi
    SEEDS=("$SEED")
else
    # Empty marker: do not emit a seed override; use whatever settings-common.sql sets.
    SEEDS=("")
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

DB_NAME="${COLD}_cold_${LAYOUT}_${MULT}x"
DB="scripts/measure/data/${DB_NAME}.duckdb"
SETUP_SQL="scripts/measure/generation/${DB_NAME}.sql"
COMMON_SETTINGS_SQL="scripts/measure/settings-common.sql"
RUN_SETTINGS_SQL="scripts/measure/settings-run_hugo_generated.sql"
PROFILE_JSON="scripts/measure/${DB_NAME}.json"

if $GENERATE; then
    echo "=== Generating data: ${DB_NAME} ==="
    rm -f "$DB" "${DB}.wal"
    build/release/duckdb "$DB" < "$SETUP_SQL"
    echo "=== Data generated: $DB ==="
fi

if [[ ! -f "$DB" ]]; then
    echo "Error: $DB does not exist. Run with --generate first."
    exit 1
fi
if [[ ! -f "$COMMON_SETTINGS_SQL" ]]; then
    echo "Error: Common settings file not found: $COMMON_SETTINGS_SQL"
    exit 1
fi
if [[ ! -f "$RUN_SETTINGS_SQL" ]]; then
    echo "Error: Run-specific settings file not found: $RUN_SETTINGS_SQL"
    exit 1
fi

PROFILING_HEADER=""
if $PROFILE; then
    PROFILING_HEADER="PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = '${PROFILE_JSON}';
PRAGMA profiling_coverage = 'SELECT';
"
fi

TASKSET_PREFIX=()
if $USE_TASKSET; then
    TASKSET_PREFIX=(taskset -c 4-59)
fi

PERF_EVENTS="cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,mem_access,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd,branch-instructions,branch-misses,br_retired,br_mis_pred_retired"

if $USE_PERF; then
    CMD=(sudo perf stat -e "$PERF_EVENTS" \
        -- "${TASKSET_PREFIX[@]}" build/${BUILD_TYPE}/duckdb "$DB")
else
    CMD=("${TASKSET_PREFIX[@]}" build/${BUILD_TYPE}/duckdb "$DB")
fi

drop_os_page_cache() {
    if ! $DROP_OS_CACHE; then
        return
    fi
    # Linux page cache survives across DuckDB CLI processes. This is opt-in
    # because it requires sudo and disrupts every workload on the host.
    echo "Dropping Linux page cache before DuckDB benchmark/profiling query..." >&2
    sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
}

# DuckDB resource failures are per-query benchmark misses for sweep CSVs: record
# them explicitly and keep later cases/seeds running.
is_recoverable_duckdb_resource_error() {
    local error_file="$1"
    grep -Eiq 'Out of Memory Error|failed to offload data block|max_temp_directory_size' "$error_file"
}

# Sweep mode: any time we span more than a single (case, seed) tuple,
# or whenever the user explicitly asks for a CSV.
SWEEPING=false
if $SWEEPING_SEEDS || [[ ${#CASES[@]} -gt 1 ]]; then
    SWEEPING=true
fi
if $SWEEPING && [[ -z "$CSV_PATH" ]]; then
    mkdir -p "$REPO_ROOT/results/hugo_generated"
    CSV_PATH="$REPO_ROOT/results/hugo_generated/hugo_generated_runtimes_$(date +%Y%m%d_%H%M%S).csv"
fi
if [[ -n "$CSV_PATH" ]]; then
    mkdir -p "$(dirname "$CSV_PATH")"
    printf "query,case,seed,runtime_seconds\n" >"$CSV_PATH"
fi

# --- Single-shot mode (one case, no seed sweep, no CSV requested) ----------
# Preserves the legacy behavior: warmup + timed iterations with .timer on,
# followed by average wall clock time.
if ! $SWEEPING && [[ -z "$CSV_PATH" ]]; then
    c="${CASES[0]}"
    s="${SEEDS[0]:-}"

    build_bench_sql() {
        grep '^SET ' "$COMMON_SETTINGS_SQL" || true
        grep '^SET ' "$RUN_SETTINGS_SQL" || true
        case_settings_for "$c"
        if [[ -n "$s" ]]; then
            printf 'SET transfer_graph_seed = %s;\n' "$s"
        fi
        echo ""
        echo "PREPARE benchmark_query AS"
        echo "SELECT min(b.valueB1)"
        echo "FROM a"
        echo "JOIN b ON a.keyB1 = b.keyB1;"
        echo ""
        if ! $NO_WARMUP; then
            echo "EXECUTE benchmark_query;"
            echo ".print Warmup done - running $RUNS timed iterations"
        else
            echo ".print Skipping warmup - running $RUNS timed iterations"
        fi
        echo "SET VARIABLE t0 = epoch_ms(now());"
        echo ".timer on"
        for ((i = 0; i < RUNS; i++)); do
            echo "EXECUTE benchmark_query;"
        done
        echo ".timer off"
        echo "SET VARIABLE t1 = epoch_ms(now());"
        echo ""
        echo "SELECT printf('Average run time: %.3f s', (getvariable('t1') - getvariable('t0')) / ${RUNS}.0 / 1000.0) AS info;"
    }

    build_profile_sql() {
        grep '^SET ' "$COMMON_SETTINGS_SQL" || true
        grep '^SET ' "$RUN_SETTINGS_SQL" || true
        case_settings_for "$c"
        if [[ -n "$s" ]]; then
            printf 'SET transfer_graph_seed = %s;\n' "$s"
        fi
        printf '%s\n' "$PROFILING_HEADER"
        # Run the raw SELECT in a fresh process so cache-dropping happens
        # immediately before the profiled query, not before earlier warmups.
        echo "SELECT min(b.valueB1)"
        echo "FROM a"
        echo "JOIN b ON a.keyB1 = b.keyB1;"
        echo "PRAGMA enable_profiling = 'no_output';"
    }

    echo "=== Running query: ${DB_NAME} (Case #$c, warmup + $RUNS runs) ==="
    drop_os_page_cache
    build_bench_sql | "${CMD[@]}"
    if $PROFILE; then
        drop_os_page_cache
        build_profile_sql | "${CMD[@]}"
        echo "DuckDB profiling output written to: $PROFILE_JSON"
    fi
    exit 0
fi

# --- Sweep mode: per-run timings into CSV ----------------------------------
# Per-run timings are captured inside DuckDB itself by bracketing each EXECUTE
# with epoch_ms() reads and printing the diff as a PERRUN_S line.
build_sweep_sql() {
    local case_num="$1"
    local seed_val="$2"
    grep '^SET ' "$COMMON_SETTINGS_SQL" || true
    grep '^SET ' "$RUN_SETTINGS_SQL" || true
    case_settings_for "$case_num"
    if [[ -n "$seed_val" ]]; then
        printf 'SET transfer_graph_seed = %s;\n' "$seed_val"
    fi
    echo ""
    echo "PREPARE benchmark_query AS"
    echo "SELECT min(b.valueB1)"
    echo "FROM a"
    echo "JOIN b ON a.keyB1 = b.keyB1;"
    echo ""
    # Suppress non-timing output so only PERRUN_S lines reach the captured stream.
    # (The .output /dev/null persists across iterations; each loop iteration
    # briefly toggles back to stdout to print the PERRUN_S line.)
    echo ".output /dev/null"
    if ! $NO_WARMUP; then
        # Warmup run; its result is suppressed by the .output /dev/null above.
        echo "EXECUTE benchmark_query;"
    fi
    # CSV mode + headers off so PERRUN_S lines come out as bare text we can grep.
    echo ".mode csv"
    echo ".headers off"
    for ((i = 1; i <= RUNS; i++)); do
        echo "SET VARIABLE _t0_${i} = epoch_ms(now());"
        echo "EXECUTE benchmark_query;"
        echo ".output stdout"
        echo "SELECT printf('PERRUN_S=%d=%.6f', ${i}, (epoch_ms(now()) - getvariable('_t0_${i}')) / 1000.0);"
        echo ".output /dev/null"
    done
}

build_profile_sql() {
    local case_num="$1"
    local seed_val="$2"
    grep '^SET ' "$COMMON_SETTINGS_SQL" || true
    grep '^SET ' "$RUN_SETTINGS_SQL" || true
    case_settings_for "$case_num"
    if [[ -n "$seed_val" ]]; then
        printf 'SET transfer_graph_seed = %s;\n' "$seed_val"
    fi
    printf '%s\n' "$PROFILING_HEADER"
    # Run profiling as its own DuckDB process so --drop-os-cache can clear the
    # OS page cache immediately before the profiled SELECT.
    echo "SELECT min(b.valueB1)"
    echo "FROM a"
    echo "JOIN b ON a.keyB1 = b.keyB1;"
    echo "PRAGMA enable_profiling = 'no_output';"
}

echo "Starting hugo-generated sweep (cases: ${CASES[*]}, seeds: ${SEEDS[*]:-default}, runs/tuple: ${RUNS}, db: ${DB_NAME})..."

TOTAL_RUNS=0
for c in "${CASES[@]}"; do
    for s in "${SEEDS[@]}"; do
        seed_disp="${s:-default}"
        echo "=== case=${c} seed=${seed_disp} runs=${RUNS} ==="
        sql="$(build_sweep_sql "$c" "$s")"
        tmp_out="$(mktemp)"
        drop_os_page_cache
        if ! printf '%s\n' "$sql" | "${CMD[@]}" >"$tmp_out" 2>&1; then
            cat "$tmp_out" >&2
            if is_recoverable_duckdb_resource_error "$tmp_out"; then
                echo "Warning: query hit DuckDB OOM/temp-spill limit (case ${c}, seed ${seed_disp}); recording runtime 8888888" >&2
                if [[ -n "$CSV_PATH" ]]; then
                    for ((i = 1; i <= RUNS; i++)); do
                        printf '%s,%s,%s,%s\n' "$DB_NAME" "$c" "$s" "8888888" >>"$CSV_PATH"
                    done
                fi
                TOTAL_RUNS=$((TOTAL_RUNS + RUNS))
                rm -f "$tmp_out"
                continue
            fi
            rm -f "$tmp_out"
            echo "Error: query failed (case ${c}, seed ${seed_disp})" >&2
            exit 1
        fi
        run_count=0
        while IFS= read -r line; do
            # DuckDB CSV mode terminates rows with CRLF; strip the trailing
            # CR so the $ in the regex below matches.
            line="${line%$'\r'}"
            if [[ "$line" =~ ^PERRUN_S=([0-9]+)=([0-9.]+)$ ]]; then
                run_idx="${BASH_REMATCH[1]}"
                rt="${BASH_REMATCH[2]}"
                run_count=$((run_count + 1))
                if [[ -n "$CSV_PATH" ]]; then
                    printf '%s,%s,%s,%s\n' "$DB_NAME" "$c" "$s" "$rt" >>"$CSV_PATH"
                fi
                echo "  run ${run_idx}: ${rt} s"
            fi
        done <"$tmp_out"
        if [[ "$run_count" -lt "$RUNS" ]]; then
            echo "Warning: captured ${run_count}/${RUNS} runs for case=${c} seed=${seed_disp}" >&2
            echo "--- duckdb output: ---" >&2
            cat "$tmp_out" >&2
            echo "--- end ---" >&2
        fi
        TOTAL_RUNS=$((TOTAL_RUNS + run_count))
        rm -f "$tmp_out"
        if $PROFILE; then
            tmp_profile="$(mktemp)"
            drop_os_page_cache
            if ! build_profile_sql "$c" "$s" | "${CMD[@]}" >"$tmp_profile" 2>&1; then
                cat "$tmp_profile" >&2
                if is_recoverable_duckdb_resource_error "$tmp_profile"; then
                    echo "Warning: profiling hit DuckDB OOM/temp-spill limit (case ${c}, seed ${seed_disp}); continuing sweep" >&2
                    rm -f "$tmp_profile"
                    continue
                fi
                rm -f "$tmp_profile"
                echo "Error: profiling failed (case ${c}, seed ${seed_disp})" >&2
                exit 1
            fi
            if [[ -s "$tmp_profile" ]]; then
                cat "$tmp_profile"
            fi
            rm -f "$tmp_profile"
        fi
    done
done

echo "Sweep complete. Captured ${TOTAL_RUNS} run(s)."
if [[ -n "$CSV_PATH" ]]; then
    echo "CSV written to: $CSV_PATH"
fi
if $PROFILE; then
    echo "DuckDB profiling output written to: $PROFILE_JSON"
fi
