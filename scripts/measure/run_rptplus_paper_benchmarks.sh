#!/usr/bin/env bash

# To run, do:
# tmux new -s bench
# scripts/measure/run_rptplus_paper_benchmarks.sh
# To detach: Ctrl-b then d
# Later: tmux attach -t bench

scripts/measure/run_appian.sh --cases 1,2,3,4 --duckdb-profiling --seeds 20
scripts/measure/run_job.sh --cases 1,2,3,4 --duckdb-profiling --seeds 20
scripts/measure/run_tpc.sh --sf 100 --tpch-only --cases 1,2,3,4 --duckdb-profiling --seeds 20 
