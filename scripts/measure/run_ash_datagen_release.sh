#!/usr/bin/env bash
# Run table generation (release) + benchmark query (release).
# Usage:
#   scripts/measure/run_ash_datagen_release.sh --case <1|2|3|4> [options] [<query>]
# Sweep example (box plots):
#   scripts/measure/run_ash_datagen_release.sh --cases 2,3,4 --queries rs,rst --seeds 5
set -euo pipefail

PERF=false
USE_DUCKDB_PROFILING=false
USE_TASKSET=true
DROP_OS_CACHE=false
RUNS=1
CASE=""
CASES_LIST=""
QUERY=""
QUERIES_LIST=""
SEED=""
SEEDS_COUNT=""
CSV_PATH=""
COMMON_SETTINGS_SQL="scripts/measure/settings-common.sql"
RUN_SETTINGS_SQL="scripts/measure/settings-run_ash_datagen.sql"
PERF_EVENTS="cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,mem_access,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd,branch-instructions,branch-misses,br_retired,br_mis_pred_retired"

usage() {
    cat <<'USAGE'
Usage: scripts/measure/run_ash_datagen_release.sh (--case <c> | --cases <list>) [options] [<query>]

Options:
  --case <1|2|3|4>      Optimizer case (mutually exclusive with --cases)
  --cases <list>        Comma-separated case list, e.g. 2,3,4
  --query <rs|rst>      Benchmark query (also accepted as positional arg)
  --queries <list>      Comma-separated query list, e.g. rs,rst
  --seed <int>          Override transfer_graph_seed (mutually exclusive with --seeds)
  --seeds <N>           Sweep seeds 0..N-1 (overrides transfer_graph_seed)
  --csv <path>          Write per-run CSV (auto-named if omitted in sweep mode)
  --runs <N>            EXECUTE iterations per (case,query,seed) (default 1)
  --perf                Run benchmark phase under perf stat
  --duckdb-profiling    Enable DuckDB JSON profiling, output to ash_datagen_results.json
  --drop-os-cache       Run sync + drop Linux page cache before measured and
                        profiled DuckDB queries. Requires sudo and affects the
                        whole host.
  --no-taskset          Don't taskset to cores 4-59
  -h, --help            Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --case) CASE="$2"; shift 2 ;;
        --cases) CASES_LIST="$2"; shift 2 ;;
        --query) QUERY="$2"; shift 2 ;;
        --queries) QUERIES_LIST="$2"; shift 2 ;;
        --seed) SEED="$2"; shift 2 ;;
        --seeds) SEEDS_COUNT="$2"; shift 2 ;;
        --csv) CSV_PATH="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --perf) PERF=true; shift ;;
        --duckdb-profiling) USE_DUCKDB_PROFILING=true; shift ;;
        --drop-os-cache) DROP_OS_CACHE=true; shift ;;
        --no-taskset) USE_TASKSET=false; shift ;;
        -h|--help) usage; exit 0 ;;
        --*) echo "Unknown option: $1" >&2; usage; exit 1 ;;
        *)
            # Back-compat: positional <query> argument (rs|rst).
            if [[ -z "$QUERY" && -z "$QUERIES_LIST" ]]; then
                QUERY="$1"; shift
            else
                echo "Unexpected argument: $1" >&2; usage; exit 1
            fi
            ;;
    esac
done

if [[ -n "$CASE" && -n "$CASES_LIST" ]]; then
    echo "Error: --case and --cases are mutually exclusive." >&2; exit 1
fi
if [[ -n "$SEED" && -n "$SEEDS_COUNT" ]]; then
    echo "Error: --seed and --seeds are mutually exclusive." >&2; exit 1
fi
if [[ -n "$QUERY" && -n "$QUERIES_LIST" ]]; then
    echo "Error: --query and --queries are mutually exclusive." >&2; exit 1
fi
if [[ -z "$CASE" && -z "$CASES_LIST" ]]; then
    echo "Error: --case or --cases is required." >&2; exit 1
fi
if [[ -z "$QUERY" && -z "$QUERIES_LIST" ]]; then
    echo "Error: a query (rs|rst) is required (use --query, --queries, or positional)." >&2; exit 1
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

QUERIES=()
if [[ -n "$QUERY" ]]; then
    QUERIES=("$QUERY")
else
    IFS=',' read -r -a QUERIES <<<"$QUERIES_LIST"
fi
for q in "${QUERIES[@]}"; do
    case "$q" in
        rs|rst) ;;
        *) echo "Error: query must be 'rs' or 'rst' (got: $q)" >&2; exit 1 ;;
    esac
done

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

case_settings_for() {
    case "$1" in
        1) printf '%s\n' "SET disable_rpt = true;" "SET disable_tiered_hash_cache = true;" ;;
        2) printf '%s\n' "SET rpt_forward_only = true;" "SET disable_tiered_hash_cache = true;" ;;
        3) printf '%s\n' "SET rpt_forward_only = true;" ;;
        4) printf '%s\n' "SET disable_tiered_hash_cache = true;" ;;
    esac
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# Absolute path so the file is easy to locate regardless of the script's CWD.
PROFILING_OUTPUT="$REPO_ROOT/ash_datagen_results.json"
# See https://duckdb.org/docs/stable/dev/profiling
PROFILING_PRAGMAS="PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = '$PROFILING_OUTPUT';
PRAGMA profiling_coverage = 'SELECT';"

DB="ASH-datagen/bench.duckdb"

if [[ ! -f "$COMMON_SETTINGS_SQL" ]]; then
    echo "Error: Common settings file not found: $COMMON_SETTINGS_SQL" >&2; exit 1
fi
if [[ ! -f "$RUN_SETTINGS_SQL" ]]; then
    echo "Error: Run-specific settings file not found: $RUN_SETTINGS_SQL" >&2; exit 1
fi

# Sweep mode: any time we span more than a single (case, query, seed) tuple,
# or whenever the user explicitly asks for a CSV.
SWEEPING=false
if $SWEEPING_SEEDS || [[ ${#CASES[@]} -gt 1 ]] || [[ ${#QUERIES[@]} -gt 1 ]]; then
    SWEEPING=true
fi
if $SWEEPING && [[ -z "$CSV_PATH" ]]; then
    mkdir -p "$REPO_ROOT/results/ash_datagen"
    CSV_PATH="$REPO_ROOT/results/ash_datagen/ash_datagen_runtimes_$(date +%Y%m%d_%H%M%S).csv"
fi
if [[ -n "$CSV_PATH" ]]; then
    mkdir -p "$(dirname "$CSV_PATH")"
    # Match the header consumed by scripts/measure/plot_runtime_boxplots.py so
    # the same downstream box-plot tooling works on ash-datagen results.
    printf "query,case,seed,runtime_seconds\n" >"$CSV_PATH"
fi

# Phase 1: generate tables once (data layout is independent of the optimizer
# case settings, so a single generation feeds every (case, query, seed) tuple).
COMMON_SETTINGS_SQL="$COMMON_SETTINGS_SQL" RUN_SETTINGS_SQL="$RUN_SETTINGS_SQL" \
    "ASH-datagen/run_generation.sh" "$DB"

TASKSET_PREFIX=()
if $USE_TASKSET; then
    TASKSET_PREFIX=(taskset -c 4-59)
fi

drop_os_page_cache() {
    if ! $DROP_OS_CACHE; then
        return
    fi
    # Linux page cache survives across DuckDB CLI processes. Keep cold-cache
    # measurements explicit because this sudo operation affects the whole host.
    echo "Dropping Linux page cache before DuckDB benchmark/profiling query..." >&2
    sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
}

# DuckDB resource failures are per-query benchmark misses for sweep CSVs: record
# them explicitly and keep later cases/queries/seeds running.
is_recoverable_duckdb_resource_error() {
    local error_file="$1"
    grep -Eiq 'Out of Memory Error|failed to offload data block|max_temp_directory_size' "$error_file"
}

# --- Single-shot mode (one case, one query, no CSV requested) -------------
# Preserves the legacy behavior of run_benchmark.sh, including EXPLAIN ANALYZE
# at the end of the run, so users invoking this script the old way see the
# same output as before.
if ! $SWEEPING && [[ -z "$CSV_PATH" ]]; then
    c="${CASES[0]}"
    q="${QUERIES[0]}"
    s="${SEEDS[0]:-}"
    case_settings_str="$(case_settings_for "$c")"
    if [[ -n "$s" ]]; then
        case_settings_str="${case_settings_str}"$'\n'"SET transfer_graph_seed = ${s};"
    fi
    echo "=== Phase 2: running query_${q}.sql with release build (${RUNS} runs, case ${c}) ==="
    if $PERF; then
        CMD=(sudo perf stat -e "$PERF_EVENTS" -- "${TASKSET_PREFIX[@]}" build/release/duckdb "$DB")
    else
        CMD=("${TASKSET_PREFIX[@]}" build/release/duckdb "$DB")
    fi
    profiling_env=""
    if $USE_DUCKDB_PROFILING; then profiling_env="$PROFILING_PRAGMAS"; fi
    COMMON_SETTINGS_SQL="$COMMON_SETTINGS_SQL" RUN_SETTINGS_SQL="$RUN_SETTINGS_SQL" \
        CASE_SETTINGS="$case_settings_str" \
        PROFILING_PRAGMAS="$profiling_env" \
        DROP_OS_CACHE="$DROP_OS_CACHE" \
        "ASH-datagen/run_benchmark.sh" "$q" "$RUNS" "$DB" "${CMD[@]}"
    if $USE_DUCKDB_PROFILING; then
        echo "DuckDB profiling output written to: $PROFILING_OUTPUT"
    fi
    exit 0
fi

# --- Sweep mode: per-run timings into CSV ---------------------------------
# We bypass run_benchmark.sh here because that script only reports an average
# for the whole batch; for box plots we need one row per individual EXECUTE.
# Per-run timings are captured inside DuckDB itself by bracketing each EXECUTE
# with epoch_ms() reads stored in named variables and printing the diff. The
# warmup EXECUTE inside query_<q>.sql remains under .output /dev/null so it
# never produces a PERRUN_S line.
build_sweep_sql() {
    local case_num="$1"
    local seed_val="$2"
    local query_name="$3"
    grep '^SET ' "$COMMON_SETTINGS_SQL" || true
    grep '^SET ' "$RUN_SETTINGS_SQL" || true
    case_settings_for "$case_num"
    if [[ -n "$seed_val" ]]; then
        printf 'SET transfer_graph_seed = %s;\n' "$seed_val"
    fi
    # Mirror the prelude run_benchmark.sh uses: rebuild the temp generator_counts
    # and prepare the benchmark query (the .read also runs one warmup EXECUTE).
    echo "CREATE OR REPLACE TEMP TABLE generator_counts AS SELECT * FROM generator_counts_persistent;"
    echo ".read ASH-datagen/query_${query_name}.sql"
    # CSV mode + headers off so the printf rows come out as a single bare line
    # we can grep for. .output is left at /dev/null (set by query_<q>.sql) and
    # only flipped to stdout for the timing print so the COUNT(*) result of
    # each EXECUTE stays out of the captured stream.
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
    local query_name="$3"
    grep '^SET ' "$COMMON_SETTINGS_SQL" || true
    grep '^SET ' "$RUN_SETTINGS_SQL" || true
    case_settings_for "$case_num"
    if [[ -n "$seed_val" ]]; then
        printf 'SET transfer_graph_seed = %s;\n' "$seed_val"
    fi
    echo "CREATE OR REPLACE TEMP TABLE generator_counts AS SELECT * FROM generator_counts_persistent;"
    echo ".output /dev/null"
    printf '%s\n' "$PROFILING_PRAGMAS"
    # Profile the raw SELECT in its own DuckDB process so --drop-os-cache can
    # run immediately before the profiled query instead of before warmups.
    sed -n '/^PREPARE/,/;/{/^PREPARE/!p}' "ASH-datagen/query_${query_name}.sql"
    echo "PRAGMA enable_profiling = 'no_output';"
}

if $PERF; then
    DUCKDB_CMD=(sudo perf stat -e "$PERF_EVENTS" -- "${TASKSET_PREFIX[@]}" build/release/duckdb "$DB")
else
    DUCKDB_CMD=("${TASKSET_PREFIX[@]}" build/release/duckdb "$DB")
fi

echo "Starting ASH-datagen sweep (cases: ${CASES[*]}, queries: ${QUERIES[*]}, seeds: ${SEEDS[*]:-default}, runs/tuple: ${RUNS})..."

TOTAL_RUNS=0
for c in "${CASES[@]}"; do
    for q in "${QUERIES[@]}"; do
        for s in "${SEEDS[@]}"; do
            seed_disp="${s:-default}"
            echo "=== case=${c} query=${q} seed=${seed_disp} runs=${RUNS} ==="
            sql="$(build_sweep_sql "$c" "$s" "$q")"
            tmp_out="$(mktemp)"
            drop_os_page_cache
            if ! printf '%s\n' "$sql" | "${DUCKDB_CMD[@]}" >"$tmp_out" 2>&1; then
                cat "$tmp_out" >&2
                if is_recoverable_duckdb_resource_error "$tmp_out"; then
                    echo "Warning: query ${q} hit DuckDB OOM/temp-spill limit (case ${c}, seed ${seed_disp}); recording runtime 8888888" >&2
                    if [[ -n "$CSV_PATH" ]]; then
                        for ((i = 1; i <= RUNS; i++)); do
                            printf '%s,%s,%s,%s\n' "$q" "$c" "$s" "8888888" >>"$CSV_PATH"
                        done
                    fi
                    TOTAL_RUNS=$((TOTAL_RUNS + RUNS))
                    rm -f "$tmp_out"
                    continue
                fi
                rm -f "$tmp_out"
                echo "Error: query ${q} failed (case ${c}, seed ${seed_disp})" >&2
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
                        printf '%s,%s,%s,%s\n' "$q" "$c" "$s" "$rt" >>"$CSV_PATH"
                    fi
                    echo "  run ${run_idx}: ${rt} s"
                fi
            done <"$tmp_out"
            if [[ "$run_count" -lt "$RUNS" ]]; then
                echo "Warning: captured ${run_count}/${RUNS} runs for case=${c} query=${q} seed=${seed_disp}" >&2
                echo "--- duckdb output: ---" >&2
                cat "$tmp_out" >&2
                echo "--- end ---" >&2
            fi
            TOTAL_RUNS=$((TOTAL_RUNS + run_count))
            rm -f "$tmp_out"
            if $USE_DUCKDB_PROFILING; then
                tmp_profile="$(mktemp)"
                drop_os_page_cache
                if ! build_profile_sql "$c" "$s" "$q" | "${DUCKDB_CMD[@]}" >"$tmp_profile" 2>&1; then
                    cat "$tmp_profile" >&2
                    if is_recoverable_duckdb_resource_error "$tmp_profile"; then
                        echo "Warning: profiling query ${q} hit DuckDB OOM/temp-spill limit (case ${c}, seed ${seed_disp}); continuing sweep" >&2
                        rm -f "$tmp_profile"
                        continue
                    fi
                    rm -f "$tmp_profile"
                    echo "Error: profiling query ${q} failed (case ${c}, seed ${seed_disp})" >&2
                    exit 1
                fi
                if [[ -s "$tmp_profile" ]]; then
                    cat "$tmp_profile"
                fi
                rm -f "$tmp_profile"
            fi
        done
    done
done

echo "Sweep complete. Captured ${TOTAL_RUNS} run(s)."
if [[ -n "$CSV_PATH" ]]; then
    echo "CSV written to: $CSV_PATH"
fi
if $USE_DUCKDB_PROFILING; then
    echo "DuckDB profiling output written to: $PROFILING_OUTPUT"
fi
# Condense the runtime CSV to one (median) row per (query, case), matching the
# postprocessing performed by the JOB, Appian, and TPC benchmark drivers.
MEDIAN_SCRIPT="$SCRIPT_DIR/median_runtime_csv.py"
if [[ -n "$CSV_PATH" ]] && [[ -f "$MEDIAN_SCRIPT" ]] && command -v python3 >/dev/null; then
    python3 "$MEDIAN_SCRIPT" --csv "$CSV_PATH" || \
        echo "warning: median_runtime_csv failed for $CSV_PATH" >&2
fi
