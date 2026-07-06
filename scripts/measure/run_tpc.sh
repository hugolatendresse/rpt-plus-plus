#!/usr/bin/env bash
set -euo pipefail

SF=100
DUCKDB_BIN=""
GENERATE_DATA=0
OUT_DIR="./results/tpch"
RUN_TPCH=1
RUN_TPCDS=1
DB_BASE_PATH="../benchmark_data"
TPCH_QUERY=""
TPCDS_QUERY=""
RUNS=1
CASE=""
CASES_LIST=""
SEED=""
SEEDS_COUNT=""
USE_PERF=false
USE_DEBUG=false
USE_DUCKDB_PROFILING=false
CREATE_BOXPLOTS=false
DROP_OS_CACHE=false
TIMEOUT_SECONDS=""
COMMON_SETTINGS_SQL="scripts/measure/settings-common.sql"
RUN_SETTINGS_SQL="scripts/measure/settings-run_tpc.sql"
PERF_EVENTS="cpu-cycles,instructions,bus_access,bus_access_rd,bus_access_wr,mem_access,l3d_cache,l3d_cache_refill,ll_cache_rd,ll_cache_miss_rd,branch-instructions,branch-misses,br_retired,br_mis_pred_retired"

usage() {
cat <<'USAGE'
Usage: scripts/measure/run_tpc.sh (--case <c> | --cases <list>) [options]

Options:
  --sf <scale_factor>    Scale factor for dbgen/dsdgen (default: 100)
  --db <db_base_path>    Base path for databases (default: ../benchmark_data)
  --duckdb <bin_path>    DuckDB CLI binary (default: ./build/release/duckdb)
  --generate             Generate TPC-H/TPC-DS data
  --out-dir <dir>        Output directory for results (default: ./results/tpch)
  --tpch-only            Run only TPC-H
  --tpcds-only           Run only TPC-DS
  --tpch-query <1..22>   Run one TPC-H query (implies --tpch-only)
  --tpcds-query <1..99>  Run one TPC-DS query (implies --tpcds-only)
  --runs <N>             Number of benchmark runs per (case, seed) tuple (default: 1)
  --case <1|2|3|4>       Optimizer case (mutually exclusive with --cases)
  --cases <list>         Comma-separated case list, e.g. 2,3,4
  --seed <int>           Override transfer_graph_seed (mutually exclusive with --seeds)
  --seeds <N>            Sweep seeds 0..N-1 (overrides transfer_graph_seed)
  --perf                 Run each query under perf stat
  --debug                Use debug build (build/debug/duckdb)
  --duckdb-profiling     Enable DuckDB JSON profiling: per-query JSON files
                         under <out-dir>/profiling_<timestamp>/, plus an
                         augmented runtime CSV with per-join THC telemetry
                         columns Join1..JoinN (via thc_csv_postprocess.py).
  --create-boxplots      Create runtime boxplot PNGs from each final runtime CSV
  --drop-os-cache        Run sync + drop Linux page cache before each measured
                         DuckDB query. Requires sudo and affects the whole host.
  --timeout <seconds>    Per-query wall-clock cap; on timeout DuckDB is killed
                         and the run records a runtime of 9999999 in the CSV.
                         DuckDB OOM/temp-spill-limit failures record 8888888
                         so long sweeps can continue.
  -h, --help             Show this help

Sweep example (box plots):
  scripts/measure/run_tpc.sh --cases 1,2,3,4 --tpch-only --runs 3
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SF="$2"; shift 2 ;;
        --db) DB_BASE_PATH="$2"; shift 2 ;;
        --duckdb) DUCKDB_BIN="$2"; shift 2 ;;
        --generate) GENERATE_DATA=1; shift ;;
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        --tpch-only) RUN_TPCH=1; RUN_TPCDS=0; shift ;;
        --tpcds-only) RUN_TPCH=0; RUN_TPCDS=1; shift ;;
        --tpch-query) TPCH_QUERY="$2"; RUN_TPCH=1; RUN_TPCDS=0; shift 2 ;;
        --tpcds-query) TPCDS_QUERY="$2"; RUN_TPCH=0; RUN_TPCDS=1; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --case) CASE="$2"; shift 2 ;;
        --cases) CASES_LIST="$2"; shift 2 ;;
        --seed) SEED="$2"; shift 2 ;;
        --seeds) SEEDS_COUNT="$2"; shift 2 ;;
        --perf) USE_PERF=true; shift ;;
        --debug) USE_DEBUG=true; shift ;;
        --duckdb-profiling) USE_DUCKDB_PROFILING=true; shift ;;
        --create-boxplots) CREATE_BOXPLOTS=true; shift ;;
        --drop-os-cache) DROP_OS_CACHE=true; shift ;;
        --timeout) TIMEOUT_SECONDS="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
    esac
done

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
    echo "Error: --runs must be a positive integer" >&2
    exit 1
fi
if [[ -n "$TIMEOUT_SECONDS" ]]; then
    if ! [[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || [[ "$TIMEOUT_SECONDS" -lt 1 ]]; then
        echo "Error: --timeout must be a positive integer number of seconds (got: $TIMEOUT_SECONDS)" >&2
        exit 1
    fi
fi

if [[ -n "$CASE" && -n "$CASES_LIST" ]]; then
    echo "Error: --case and --cases are mutually exclusive." >&2
    exit 1
fi
if [[ -n "$SEED" && -n "$SEEDS_COUNT" ]]; then
    echo "Error: --seed and --seeds are mutually exclusive." >&2
    exit 1
fi
if [[ -z "$CASE" && -z "$CASES_LIST" ]]; then
    echo "Error: --case or --cases is required (1, 2, 3, or 4)." >&2
    exit 1
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
if [[ -n "$SEEDS_COUNT" ]]; then
    if ! [[ "$SEEDS_COUNT" =~ ^[0-9]+$ ]] || [[ "$SEEDS_COUNT" -lt 1 ]]; then
        echo "Error: --seeds must be a positive integer (got: $SEEDS_COUNT)" >&2
        exit 1
    fi
    for ((i = 0; i < SEEDS_COUNT; i++)); do SEEDS+=("$i"); done
elif [[ -n "$SEED" ]]; then
    if ! [[ "$SEED" =~ ^[0-9]+$ ]]; then
        echo "Error: --seed must be a non-negative integer (got: $SEED)" >&2
        exit 1
    fi
    SEEDS=("$SEED")
else
    # Empty marker: do not emit a seed override; use whatever settings-common.sql sets.
    SEEDS=("")
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# When DuckDB profiling is on, the JSON for each query is written to its own
# path under $OUT_DIR/profiling/ so it survives the next query's run; the
# benchmark post-processor walks those files to extract per-join THC
# telemetry. PROFILING_OUTPUT itself is unset here and recomputed per query
# inside the inner loop (see build_sql).
PROFILING_OUTPUT=""
# See https://duckdb.org/docs/stable/dev/profiling

# Pick default DuckDB binary based on --debug if the user did not override with --duckdb.
if [[ -z "$DUCKDB_BIN" ]]; then
    if $USE_DEBUG; then
        DUCKDB_BIN="./build/debug/duckdb"
    else
        DUCKDB_BIN="./build/release/duckdb"
    fi
fi

TPCH_DB_PATH="${DB_BASE_PATH}/tpch/tpch_sf${SF}.duckdb"
TPCDS_DB_PATH="${DB_BASE_PATH}/tpcds/tpcds_sf${SF}.duckdb"

if [[ ! -x "$DUCKDB_BIN" ]]; then
    echo "DuckDB binary not found or not executable: $DUCKDB_BIN" >&2
    exit 1
fi
if [[ ! -f "$COMMON_SETTINGS_SQL" ]]; then
    echo "Common settings file not found: $COMMON_SETTINGS_SQL" >&2
    exit 1
fi
if [[ ! -f "$RUN_SETTINGS_SQL" ]]; then
    echo "Run-specific settings file not found: $RUN_SETTINGS_SQL" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DBGEN_LOG="$OUT_DIR/dbgen_sf${SF}_${TIMESTAMP}.log"
TPCH_CSV_PATH="$OUT_DIR/tpch_runtimes_sf${SF}_${TIMESTAMP}.csv"
TPCDS_CSV_PATH="$OUT_DIR/tpcds_runtimes_sf${SF}_${TIMESTAMP}.csv"

build_sql() {
    local extension="$1"
    local query_stmt="$2"
    local case_num="$3"
    local seed_val="$4"
    # PROFILING_OUTPUT is set by the inner loop to a per-(query, case, seed,
    # run) path when --duckdb-profiling is on; empty otherwise.
    if $USE_DUCKDB_PROFILING && [[ -n "$PROFILING_OUTPUT" ]]; then
        printf "PRAGMA enable_profiling = 'json';\n"
        printf "PRAGMA profiling_output = '%s';\n" "$PROFILING_OUTPUT"
        printf "PRAGMA profiling_coverage = 'SELECT';\n"
    fi
    grep '^SET ' "$COMMON_SETTINGS_SQL" || true
    grep '^SET ' "$RUN_SETTINGS_SQL" || true
    case_settings_for "$case_num"
    if [[ -n "$seed_val" ]]; then
        printf 'SET transfer_graph_seed = %s;\n' "$seed_val"
    fi
    printf 'LOAD %s;\n' "$extension"
    printf '%s\n' "$query_stmt"
}

drop_os_page_cache() {
    if ! $DROP_OS_CACHE; then
        return
    fi
    # Linux page cache survives across DuckDB CLI processes. Keep cold-cache
    # measurements explicit because this sudo operation affects the whole host.
    echo "Dropping Linux page cache before measured DuckDB query..." >&2
    sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
}

# DuckDB can reject a query because it cannot allocate memory or because the
# configured temp directory limit prevents spilling. Treat those as per-query
# benchmark misses, just like timeouts, rather than losing a long sweep.
is_recoverable_duckdb_resource_error() {
    local error_file="$1"
    grep -Eiq 'Out of Memory Error|failed to offload data block|max_temp_directory_size' "$error_file"
}

process_group_alive() {
    local pid="$1"
    kill -0 -- "-$pid" 2>/dev/null || kill -0 "$pid" 2>/dev/null
}

terminate_process_group() {
    local pid="$1"
    local grace_seconds=5
    local waited=0

    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    while process_group_alive "$pid" && [[ "$waited" -lt "$grace_seconds" ]]; do
        sleep 1
        waited=$((waited + 1))
    done
    if process_group_alive "$pid"; then
        kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
    fi
}

# Runs one query in its own process group (optionally perf-wrapped, optionally
# wall-clock capped). Sets the global RUNTIME to the measured seconds, to 9999999
# if the query timed out, or to 8888888 if DuckDB hit an OOM/temp-spill-limit
# failure. Aborts the whole sweep on any other failure.
#
# The process-group wrapper matters because a shell pipeline like
# `printf SQL | timeout duckdb` can leave the DuckDB child alive after timeout
# escalation. A leaked child keeps the database lock and breaks the next query.
run_timed_query() {
    local sql="$1"
    local db_path="$2"
    local label="$3"
    local sql_file error_file cmd_pid rc timed_out start_time end_time deadline
    sql_file=$(mktemp)
    error_file=$(mktemp)
    rc=0
    timed_out=0
    printf '%s\n' "$sql" > "$sql_file"
    drop_os_page_cache
    start_time=$(date +%s.%N)
    if $USE_PERF; then
        setsid bash -c 'exec sudo perf stat -e "$1" -- "$2" "$3" < "$4" >/dev/null 2>"$5"' \
            _ "$PERF_EVENTS" "$DUCKDB_BIN" "$db_path" "$sql_file" "$error_file" &
    else
        setsid bash -c 'exec "$1" "$2" < "$3" >/dev/null 2>"$4"' \
            _ "$DUCKDB_BIN" "$db_path" "$sql_file" "$error_file" &
    fi
    cmd_pid=$!
    if [[ -n "$TIMEOUT_SECONDS" ]]; then
        deadline=$(($(date +%s) + TIMEOUT_SECONDS))
        while process_group_alive "$cmd_pid"; do
            if [[ "$(date +%s)" -ge "$deadline" ]]; then
                timed_out=1
                break
            fi
            sleep 0.1
        done
    fi
    if [[ "$timed_out" -eq 1 ]]; then
        terminate_process_group "$cmd_pid"
        wait "$cmd_pid" 2>/dev/null || true
        rc=124
    else
        wait "$cmd_pid" || rc=$?
    fi
    end_time=$(date +%s.%N)
    if [[ -s "$error_file" ]]; then
        cat "$error_file" >&2
    fi
    if [[ "$rc" -eq 0 ]]; then
        RUNTIME=$(awk -v s="$start_time" -v e="$end_time" 'BEGIN{printf "%.2f", e - s}')
    elif [[ "$rc" -eq 124 || "$rc" -eq 137 ]]; then
        echo "Warning: ${label} timed out after ${TIMEOUT_SECONDS}s; recording runtime 9999999" >&2
        RUNTIME=9999999
    elif is_recoverable_duckdb_resource_error "$error_file"; then
        echo "Warning: ${label} hit DuckDB OOM/temp-spill limit; recording runtime 8888888" >&2
        RUNTIME=8888888
    else
        rm -f "$sql_file" "$error_file"
        echo "Error: ${label} failed" >&2
        exit 1
    fi
    rm -f "$sql_file" "$error_file"
}

if [[ $RUN_TPCH -eq 1 ]] && [[ $GENERATE_DATA -eq 1 ]]; then
    mkdir -p "$(dirname "$TPCH_DB_PATH")"
    if ! "$DUCKDB_BIN" "$TPCH_DB_PATH" -c "LOAD tpch;" > "$DBGEN_LOG" 2>&1; then
        "$DUCKDB_BIN" "$TPCH_DB_PATH" <<SQL >> "$DBGEN_LOG" 2>&1
INSTALL tpch;
LOAD tpch;
SQL
    fi
    "$DUCKDB_BIN" "$TPCH_DB_PATH" <<SQL >> "$DBGEN_LOG" 2>&1
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS supplier;
CALL dbgen(sf = ${SF});
SQL
fi

if [[ $RUN_TPCDS -eq 1 ]] && [[ $GENERATE_DATA -eq 1 ]]; then
    mkdir -p "$(dirname "$TPCDS_DB_PATH")"
    if ! "$DUCKDB_BIN" "$TPCDS_DB_PATH" -c "LOAD tpcds;" > "$DBGEN_LOG" 2>&1; then
        "$DUCKDB_BIN" "$TPCDS_DB_PATH" <<SQL >> "$DBGEN_LOG" 2>&1
INSTALL tpcds;
LOAD tpcds;
SQL
    fi
    "$DUCKDB_BIN" "$TPCDS_DB_PATH" <<SQL >> "$DBGEN_LOG" 2>&1
LOAD tpcds;
CALL dsdgen(sf = ${SF});
SQL
fi

if [[ $RUN_TPCH -eq 1 ]] && [[ $GENERATE_DATA -eq 0 ]] && [[ ! -f "$TPCH_DB_PATH" ]]; then
    echo "Error: TPC-H database not found at ${TPCH_DB_PATH}" >&2
    exit 1
fi
if [[ $RUN_TPCDS -eq 1 ]] && [[ $GENERATE_DATA -eq 0 ]] && [[ ! -f "$TPCDS_DB_PATH" ]]; then
    echo "Error: TPC-DS database not found at ${TPCDS_DB_PATH}" >&2
    exit 1
fi

if [[ -n "$TPCH_QUERY" ]]; then
    if ! [[ "$TPCH_QUERY" =~ ^[0-9]+$ ]] || [[ "$TPCH_QUERY" -lt 1 ]] || [[ "$TPCH_QUERY" -gt 22 ]]; then
        echo "Error: --tpch-query must be between 1 and 22" >&2
        exit 1
    fi
    TPCH_QUERY_RANGE="$TPCH_QUERY"
else
    TPCH_QUERY_RANGE=$(seq 1 22)
fi

if [[ -n "$TPCDS_QUERY" ]]; then
    if ! [[ "$TPCDS_QUERY" =~ ^[0-9]+$ ]] || [[ "$TPCDS_QUERY" -lt 1 ]] || [[ "$TPCDS_QUERY" -gt 99 ]]; then
        echo "Error: --tpcds-query must be between 1 and 99" >&2
        exit 1
    fi
    TPCDS_QUERY_RANGE="$TPCDS_QUERY"
else
    TPCDS_QUERY_RANGE=$(seq 1 99)
fi

if [[ $RUN_TPCH -eq 1 ]]; then
    printf "query,case,seed,run_idx,runtime_seconds\n" > "$TPCH_CSV_PATH"
fi
if [[ $RUN_TPCDS -eq 1 ]]; then
    printf "query,case,seed,run_idx,runtime_seconds\n" > "$TPCDS_CSV_PATH"
fi

# Per-query profiling JSON files land here when --duckdb-profiling is on.
# Filename pattern: <prefix>_q<Q>_case<C>_seed<S>_run<R>.json. The Python
# post-processor reconstructs the same paths to map CSV rows to JSONs.
PROFILING_DIR="$OUT_DIR/profiling_${TIMESTAMP}"
if $USE_DUCKDB_PROFILING; then
    mkdir -p "$PROFILING_DIR"
fi

echo "Starting TPC sweep (cases: ${CASES[*]}, seeds: ${SEEDS[*]:-default}, runs/tuple: ${RUNS}, sf: ${SF})..."

TOTAL_WALL=0
TOTAL_ROWS=0
for c in "${CASES[@]}"; do
    for s in "${SEEDS[@]}"; do
        seed_disp="${s:-default}"
        for RUN_IDX in $(seq 1 "$RUNS"); do
            RUN_START=$(date +%s.%N)
            echo "===== case=${c} seed=${seed_disp} run=${RUN_IDX}/${RUNS} ====="

            if [[ $RUN_TPCH -eq 1 ]]; then
                for Q in $TPCH_QUERY_RANGE; do
                    echo "Running TPC-H query ${Q}..."
                    seed_for_path="${s:-default}"
                    if $USE_DUCKDB_PROFILING; then
                        PROFILING_OUTPUT="$PROFILING_DIR/tpch_q${Q}_case${c}_seed${seed_for_path}_run${RUN_IDX}.json"
                    else
                        PROFILING_OUTPUT=""
                    fi
                    SQL="$(build_sql tpch "PRAGMA tpch(${Q});" "$c" "$s")"
                    run_timed_query "$SQL" "$TPCH_DB_PATH" "TPC-H query ${Q} (case ${c}, seed ${seed_disp})"
                    printf "Q%02d,%s,%s,%s,%s\n" "$Q" "$c" "$s" "$RUN_IDX" "$RUNTIME" >> "$TPCH_CSV_PATH"
                    TOTAL_ROWS=$((TOTAL_ROWS + 1))
                done
            fi

            if [[ $RUN_TPCDS -eq 1 ]]; then
                for Q in $TPCDS_QUERY_RANGE; do
                    echo "Running TPC-DS query ${Q}..."
                    seed_for_path="${s:-default}"
                    if $USE_DUCKDB_PROFILING; then
                        PROFILING_OUTPUT="$PROFILING_DIR/tpcds_q${Q}_case${c}_seed${seed_for_path}_run${RUN_IDX}.json"
                    else
                        PROFILING_OUTPUT=""
                    fi
                    SQL="$(build_sql tpcds "PRAGMA tpcds(${Q});" "$c" "$s")"
                    run_timed_query "$SQL" "$TPCDS_DB_PATH" "TPC-DS query ${Q} (case ${c}, seed ${seed_disp})"
                    printf "Q%02d,%s,%s,%s,%s\n" "$Q" "$c" "$s" "$RUN_IDX" "$RUNTIME" >> "$TPCDS_CSV_PATH"
                    TOTAL_ROWS=$((TOTAL_ROWS + 1))
                done
            fi

            RUN_END=$(date +%s.%N)
            RUN_WALL=$(awk -v s="$RUN_START" -v e="$RUN_END" 'BEGIN{printf "%.6f", e - s}')
            TOTAL_WALL=$(awk -v t="$TOTAL_WALL" -v r="$RUN_WALL" 'BEGIN{printf "%.6f", t + r}')
            echo "case=${c} seed=${seed_disp} run=${RUN_IDX} wall-clock (s): ${RUN_WALL}"
        done
    done
done

TOTAL_TUPLES=$((${#CASES[@]} * ${#SEEDS[@]} * RUNS))
AVG_WALL=$(awk -v t="$TOTAL_WALL" -v n="$TOTAL_TUPLES" 'BEGIN{printf "%.6f", t / n}')
echo "===== SWEEP SUMMARY ====="
echo "Cases: ${CASES[*]}"
echo "Seeds: ${SEEDS[*]:-default}"
echo "Runs per (case, seed) tuple: ${RUNS}"
echo "Total (case, seed, run) tuples: ${TOTAL_TUPLES}"
echo "Total query rows captured: ${TOTAL_ROWS}"
echo "Total time (s): ${TOTAL_WALL}"
echo "Average wall-clock per tuple (s): ${AVG_WALL}"
if [[ $RUN_TPCH -eq 1 ]]; then
    echo "TPC-H CSV: $TPCH_CSV_PATH"
fi
if [[ $RUN_TPCDS -eq 1 ]]; then
    echo "TPC-DS CSV: $TPCDS_CSV_PATH"
fi
if $USE_DUCKDB_PROFILING; then
    echo "DuckDB profiling output written to: $PROFILING_DIR/"
    POSTPROCESS="$SCRIPT_DIR/thc_csv_postprocess.py"
    if [[ -x "$POSTPROCESS" ]] || command -v python3 >/dev/null; then
        if [[ $RUN_TPCH -eq 1 ]]; then
            python3 "$POSTPROCESS" --csv "$TPCH_CSV_PATH" --profiling-dir "$PROFILING_DIR" --prefix tpch || \
                echo "warning: thc_csv_postprocess failed for $TPCH_CSV_PATH" >&2
        fi
        if [[ $RUN_TPCDS -eq 1 ]]; then
            python3 "$POSTPROCESS" --csv "$TPCDS_CSV_PATH" --profiling-dir "$PROFILING_DIR" --prefix tpcds || \
                echo "warning: thc_csv_postprocess failed for $TPCDS_CSV_PATH" >&2
        fi
    fi
fi
# Condense each runtime CSV to one (median) row per (query, case). Runs after the
# THC postprocess above so the median CSV also carries any Join*-* columns.
MEDIAN_SCRIPT="$SCRIPT_DIR/median_runtime_csv.py"
if [[ -f "$MEDIAN_SCRIPT" ]] && command -v python3 >/dev/null; then
    if [[ $RUN_TPCH -eq 1 ]]; then
        python3 "$MEDIAN_SCRIPT" --csv "$TPCH_CSV_PATH" || \
            echo "warning: median_runtime_csv failed for $TPCH_CSV_PATH" >&2
    fi
    if [[ $RUN_TPCDS -eq 1 ]]; then
        python3 "$MEDIAN_SCRIPT" --csv "$TPCDS_CSV_PATH" || \
            echo "warning: median_runtime_csv failed for $TPCDS_CSV_PATH" >&2
    fi
fi
BOXPLOT_SCRIPT="$SCRIPT_DIR/plot_runtime_boxplots.py"
if $CREATE_BOXPLOTS; then
    if [[ -f "$BOXPLOT_SCRIPT" ]] && command -v python3 >/dev/null; then
        if [[ $RUN_TPCH -eq 1 ]]; then
            python3 "$BOXPLOT_SCRIPT" --csv "$TPCH_CSV_PATH" || \
                echo "warning: plot_runtime_boxplots failed for $TPCH_CSV_PATH" >&2
        fi
        if [[ $RUN_TPCDS -eq 1 ]]; then
            python3 "$BOXPLOT_SCRIPT" --csv "$TPCDS_CSV_PATH" || \
                echo "warning: plot_runtime_boxplots failed for $TPCDS_CSV_PATH" >&2
        fi
    else
        echo "warning: cannot create boxplots because $BOXPLOT_SCRIPT or python3 is missing" >&2
    fi
fi
