-- Overrides for scripts/measure/run_hugo_generated.sh.
-- Loaded after scripts/measure/settings-common.sql.
-- Add SET/SET VARIABLE statements here when needed.

-- statistics propagation removes the cold portion 
SET disabled_optimizers = 'statistics_propagation';


-- Don't stop early due to small cross multiplicity or high hotness
SET thc_enable_first_cycle_check = false;