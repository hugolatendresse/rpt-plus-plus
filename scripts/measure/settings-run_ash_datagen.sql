-- Overrides for scripts/measure/run_ash_datagen_release.sh.
-- Loaded after scripts/measure/settings-common.sql.
-- Add SET/SET VARIABLE statements here when needed.



--Generation queries hit a known unstable path in this branch's join-order optimizer.
-- Keep generation deterministic by pinning optimizer behavior during table creation.
-- SET disabled_optimizers = 'compressed_materialization,join_order,build_side_probe_side,statistics_propagation';



SET VARIABLE scale_factor = 40_000;

SET VARIABLE base_row_count_R = 100; -- the MAX number of rows. Count of rows = min(hot entries in S * probe_mmultiplicity_in_R, base_row_count_R)
SET VARIABLE base_row_count_S = 100;
SET VARIABLE base_row_count_T = 100;

SET VARIABLE selected_fraction_R = 1.00;
SET VARIABLE selected_fraction_S = 1.00;
SET VARIABLE selected_fraction_T = 1.00;

SET VARIABLE join_fraction_RS = 0.10; -- % of build side that is hot (i.e. that finds a match on the probe side)
-- SET VARIABLE join_fraction_RS = 0.20; -- % of build side that is hot (i.e. that finds a match on the probe side)
SET VARIABLE join_fraction_ST = 0.00;
SET VARIABLE bridge_fraction = 0.00;

SET VARIABLE probe_multiplicity_in_R = 10;
-- SET VARIABLE probe_multiplicity_in_R = 63;
SET VARIABLE probe_multiplicity_in_S = 1; -- higher than 1 means keys are duplicated in S hash table.

SET VARIABLE unproductive_rate_RS = 0.00;
SET VARIABLE unproductive_rate_ST = 0.00;

