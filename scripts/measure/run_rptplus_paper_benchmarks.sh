#!/usr/bin/env bash

# To run, do:
# Generate TPC-H 100 if it doesn't exist!!!!!
# tmux new -s bench
# scripts/measure/run_rptplus_paper_benchmarks.sh --drop-os-cache --create-boxplots
# To detach: Ctrl-b then d
# Later: tmux attach -t bench
#
# The results csv and medians csv are created automatically; pass
# --create-boxplots to also create the per-query runtime boxplots.
# 
# Dont' forget to run the "Move results/ to results-spy" VS code task

set -euo pipefail

DROP_OS_CACHE_ARG=()
CREATE_BOXPLOTS_ARG=()
while [[ $# -gt 0 ]]; do
	case "$1" in
	--drop-os-cache)
		DROP_OS_CACHE_ARG=(--drop-os-cache)
		shift
		;;
	--create-boxplots)
		CREATE_BOXPLOTS_ARG=(--create-boxplots)
		shift
		;;
	-h | --help)
		echo "Usage: scripts/measure/run_rptplus_paper_benchmarks.sh [--drop-os-cache] [--create-boxplots]"
		exit 0
		;;
	*)
		echo "Unknown option: $1" >&2
		exit 1
		;;
	esac
done

scripts/measure/run_appian.sh --cases 1,2,3,4 --duckdb-profiling --seeds 20 --timeout 60 "${DROP_OS_CACHE_ARG[@]}" "${CREATE_BOXPLOTS_ARG[@]}"
scripts/measure/run_job.sh --cases 1,2,3,4 --duckdb-profiling --seeds 20 --timeout 300 "${DROP_OS_CACHE_ARG[@]}" "${CREATE_BOXPLOTS_ARG[@]}"
scripts/measure/run_tpc.sh --sf 10 --tpch-only --cases 1,2,3,4 --duckdb-profiling --seeds 20 --timeout 300 "${DROP_OS_CACHE_ARG[@]}" "${CREATE_BOXPLOTS_ARG[@]}"
scripts/measure/run_tpc.sh --sf 20 --tpch-only --cases 1,2,3,4 --duckdb-profiling --seeds 20 --timeout 300 "${DROP_OS_CACHE_ARG[@]}" "${CREATE_BOXPLOTS_ARG[@]}"
scripts/measure/run_tpc.sh --sf 50 --tpch-only --cases 1,2,3,4 --duckdb-profiling --seeds 20 --timeout 300 "${DROP_OS_CACHE_ARG[@]}" "${CREATE_BOXPLOTS_ARG[@]}"
scripts/measure/run_tpc.sh --sf 100 --tpch-only --cases 1,2,3,4 --duckdb-profiling --seeds 20 --timeout 300 "${DROP_OS_CACHE_ARG[@]}" "${CREATE_BOXPLOTS_ARG[@]}"
