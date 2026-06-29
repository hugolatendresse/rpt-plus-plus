-- Overrides for scripts/measure/run_hugo_generated.sh.
-- Loaded after scripts/measure/settings-common.sql.
-- Add SET/SET VARIABLE statements here when needed.

-- statistics propagation removes the cold portion 
SET disabled_optimizers = 'statistics_propagation';

SET join_order_mode = 'duckdb';

SET max_temp_directory_size='0KiB';
SET threads = 1;
SET disable_perfect_hashing = true;
SET pin_threads = 'on';

-- Populate fine-grained hash-join timers (Build Time, Probe Time, Match Time,
-- THC Collect/Insert/Probe Time, ...) in the DuckDB profiling JSON. 
SET enable_hash_join_timers = true;