------------------------- Common Settings -----------------------------

SET thc_activation_threshold = 1_000_000;
SET thc_collect_budget_fraction = 1.00;
SET thc_collect_phase_rows = 100_000;
SET thc_first_read_only_phase_rows = 0;
-- SET thc_l3_budget = 33_554_432; -- 32M
SET thc_l3_budget = 37_748_736; -- 36MB
-- SET thc_l3_budget = 62_914_560; -- 60MB
-- SET thc_l3_budget = 67_108_864; -- 64MB
SET thc_miss_below_which_skip_collect = 0.1;


SET max_temp_directory_size='0KiB';
SET threads = 1;
-- -- Statistics propagation sometimes just removes the cold portion
SET disabled_optimizers = 'compressed_materialization,statistics_propagation';
SET thc_collect_phase_rows = 400_000;
SET disable_perfect_hashing = true;
SET pin_threads = 'on';


------------ ASH-Datagen Generation Settings ---------------------


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

