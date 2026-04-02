-- Overrides for scripts/measure/run_ash_datagen_release.sh.
-- Loaded after scripts/measure/settings-common.sql.
-- Add SET/SET VARIABLE statements here when needed.



--Generation queries hit a known unstable path in this branch's join-order optimizer.
-- Keep generation deterministic by pinning optimizer behavior during table creation.
SET disabled_optimizers = 'join_order,build_side_probe_side,statistics_propagation';