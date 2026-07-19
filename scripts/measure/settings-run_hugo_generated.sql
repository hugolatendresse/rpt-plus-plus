-- Overrides for scripts/measure/run_hugo_generated.sh.
-- Loaded after scripts/measure/settings-common.sql.
-- Add SET/SET VARIABLE statements here when needed.

-- statistics propagation removes the cold portion 
SET disabled_optimizers = 'statistics_propagation';
SET thc_first_read_only_phase_rows = 1_000_000; -- To make microbenchmark a perfect example

-- Populate fine-grained hash-join timers (Build Time, Probe Time, Match Time,
-- THC Collect/Insert/Probe Time, ...) in the DuckDB profiling JSON. 
-- SET enable_hash_join_timers = true;