#!/usr/bin/env bash

# To run, do:
# tmux new -s bench
# scripts/measure/run_rptplus_paper_benchmarks.sh
# To detach: Ctrl-b then d
# Later: tmux attach -t bench

set -euo pipefail

DROP_OS_CACHE_ARG=()
while [[ $# -gt 0 ]]; do
	case "$1" in
	--drop-os-cache)
		DROP_OS_CACHE_ARG=(--drop-os-cache)
		shift
		;;
	-h | --help)
		echo "Usage: scripts/measure/run_rptplus_paper_benchmarks.sh [--drop-os-cache]"
		exit 0
		;;
	*)
		echo "Unknown option: $1" >&2
		exit 1
		;;
	esac
done

scripts/measure/run_appian.sh --cases 1,2,3,4 --duckdb-profiling --seeds 20 --timeout 60 "${DROP_OS_CACHE_ARG[@]}"
scripts/measure/run_job.sh --cases 1,2,3,4 --duckdb-profiling --seeds 20 --timeout 300 "${DROP_OS_CACHE_ARG[@]}"
scripts/measure/run_tpc.sh --sf 100 --tpch-only --cases 1,2,3,4 --duckdb-profiling --seeds 20 --timeout 300 "${DROP_OS_CACHE_ARG[@]}"

# scripts/measure/run_appian.sh --cases 1,2,3,4 --duckdb-profiling --seeds 1
# scripts/measure/run_job.sh --cases 1,2,3,4 --duckdb-profiling --seeds 1
# scripts/measure/run_tpc.sh --sf 100 --tpch-only --cases 1,2,3,4 --duckdb-profiling --seeds 1
