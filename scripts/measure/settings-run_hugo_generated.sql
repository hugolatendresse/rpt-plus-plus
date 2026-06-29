-- Overrides for scripts/measure/run_hugo_generated.sh.
-- Loaded after scripts/measure/settings-common.sql.
-- Add SET/SET VARIABLE statements here when needed.

-- statistics propagation removes the cold portion 
SET disabled_optimizers = 'statistics_propagation';


-- Don't stop early due to small cross multiplicity or high hotness
SET thc_enable_first_cycle_check = false;
-- SET thc_warmup_cycles = 1000000;

SET thc_activation_threshold = 1_000_000;
SET thc_collect_budget_fraction = 0.25;
SET thc_collect_phase_rows = 1_000_000;
-- SET thc_collect_phase_rows = 100_000;
-- SET thc_collect_phase_rows = 8192;
SET thc_first_read_only_phase_rows = 1_000_000;
-- SET thc_first_read_only_phase_rows = 100_000;
-- SET thc_first_read_only_phase_rows = 8192;
-- SET thc_l3_budget = 33_554_432; -- 32M
SET thc_l3_budget = 37_748_736; -- 36MB
-- SET thc_l3_budget = 62_914_560; -- 60MB
-- SET thc_l3_budget = 67_108_864; -- 64MB
SET thc_miss_below_which_skip_collect = 0.1;


SET join_order_mode = 'duckdb';

SET max_temp_directory_size='0KiB';
SET threads = 1;
SET disable_perfect_hashing = true;
SET pin_threads = 'on';

-- Populate fine-grained hash-join timers (Build Time, Probe Time, Match Time,
-- THC Collect/Insert/Probe Time, ...) in the DuckDB profiling JSON. 
SET enable_hash_join_timers = false;
