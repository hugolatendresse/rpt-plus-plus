-- Can run with:
-- build/release/duckdb ../benchmark_data/tpch/tpch_sf10.duckdb -f scripts/measure/tpch5.sql

-- https://duckdb.org/docs/stable/dev/profiling
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'results.json';
PRAGMA profiling_coverage = 'SELECT';
-- PRAGMA profiling_mode = 'detailed';

SET threads = 1;
SET pin_threads = 'on';
SET thc_l3_budget = 4194304;
SET thc_collect_phase_rows = 100000;
SET thc_collect_budget_fraction = 0.02;
SET thc_miss_threshold = 0.05;
SET thc_activation_threshold = 500000;

SET disable_tiered_hash_cache = 'false';

SET threads = 4;
SET pin_threads = 'on';
SET thc_l3_budget = 4194304;
SET thc_collect_phase_rows = 100000; 
SET thc_collect_budget_fraction = 0.02; 
SET thc_miss_threshold = 0.05; 
SET thc_activation_threshold = 500000;
SET rpt_forward_only = true;

load tpch;
-- call dbgen(sf = 10);
pragma tpch(5);
