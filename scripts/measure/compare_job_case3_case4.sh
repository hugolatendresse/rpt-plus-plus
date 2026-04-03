#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUN_JOB_SCRIPT="$SCRIPT_DIR/run_job.sh"
JOB_QUERIES_DIR="$REPO_ROOT/join-order-benchmark/queries"

CSV_OUT=""

usage() {
    cat <<'USAGE'
Usage: scripts/measure/compare_job_case3_case4.sh [options]

Runs every JOB query for case 3 and case 4 (via run_job.sh) and prints:
  - a paste-friendly TSV table on stdout
  - optional CSV file for spreadsheet import

Options:
  --csv <path>        Write results as CSV to this file
  -h, --help          Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --csv)
            CSV_OUT="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ ! -x "$RUN_JOB_SCRIPT" ]]; then
    echo "Error: Expected executable script at $RUN_JOB_SCRIPT" >&2
    exit 1
fi

if [[ ! -d "$JOB_QUERIES_DIR" ]]; then
    echo "Error: JOB queries directory not found: $JOB_QUERIES_DIR" >&2
    exit 1
fi

readarray -t QUERY_IDS < <(
    for f in "$JOB_QUERIES_DIR"/*.sql; do
        basename "${f%.sql}"
    done | sort -V
)

if [[ ${#QUERY_IDS[@]} -eq 0 ]]; then
    echo "Error: No JOB query files found in $JOB_QUERIES_DIR" >&2
    exit 1
fi

if [[ -n "$CSV_OUT" ]]; then
    mkdir -p "$(dirname "$CSV_OUT")"
    printf "query,case3_seconds,case4_seconds,delta_case4_minus_case3_seconds,speedup_case3_over_case4\n" > "$CSV_OUT"
fi

extract_seconds() {
    local output="$1"
    awk '
        /Total wall clock time:/ { t = $(NF-1) }
        END {
            if (t == "") {
                exit 1
            }
            print t
        }
    ' <<< "$output"
}

run_one_case() {
    local case_id="$1"
    local query_id="$2"
    local output
    output="$("$RUN_JOB_SCRIPT" --case "$case_id" --job-query "$query_id" 2>&1)" || return 1
    extract_seconds "$output"
}

printf "query\tcase3_s\tcase4_s\tdelta_4_minus_3_s\tspeedup_3_over_4\n"

for query_id in "${QUERY_IDS[@]}"; do
    case3_s="NA"
    case4_s="NA"
    delta_s="NA"
    speedup="NA"

    if case3_result="$(run_one_case 3 "$query_id")"; then
        case3_s="$case3_result"
    else
        echo "Warning: case 3 failed for query $query_id" >&2
    fi

    if case4_result="$(run_one_case 4 "$query_id")"; then
        case4_s="$case4_result"
    else
        echo "Warning: case 4 failed for query $query_id" >&2
    fi

    if [[ "$case3_s" != "NA" && "$case4_s" != "NA" ]]; then
        delta_s="$(awk -v c3="$case3_s" -v c4="$case4_s" 'BEGIN { printf "%.6f", c4 - c3 }')"
        speedup="$(awk -v c3="$case3_s" -v c4="$case4_s" 'BEGIN { if (c4 == 0) { print "inf" } else { printf "%.6f", c3 / c4 } }')"
    fi

    printf "%s\t%s\t%s\t%s\t%s\n" "$query_id" "$case3_s" "$case4_s" "$delta_s" "$speedup"

    if [[ -n "$CSV_OUT" ]]; then
        printf "%s,%s,%s,%s,%s\n" "$query_id" "$case3_s" "$case4_s" "$delta_s" "$speedup" >> "$CSV_OUT"
    fi
done

if [[ -n "$CSV_OUT" ]]; then
    echo "Wrote CSV results to: $CSV_OUT" >&2
fi
