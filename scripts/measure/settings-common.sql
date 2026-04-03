------------------------- Common Settings -----------------------------

SET thc_activation_threshold = 1_000_000;
SET thc_collect_budget_fraction = 0.25;
SET thc_collect_phase_rows = 1_000_000;
SET thc_first_read_only_phase_rows = 1_000_000;
-- SET thc_l3_budget = 33_554_432; -- 32M
SET thc_l3_budget = 37_748_736; -- 36MB
-- SET thc_l3_budget = 62_914_560; -- 60MB
-- SET thc_l3_budget = 67_108_864; -- 64MB
SET thc_miss_below_which_skip_collect = 0.1;


SET max_temp_directory_size='0KiB';
SET threads = 1;
-- -- Statistics propagation sometimes just removes the cold portion
SET disabled_optimizers = 'join_order,build_side_probe_side,statistics_propagation'; -- TODO REMOVE THAT!!! And even remove statistics propagation for tpch and jobs
-- SET disabled_optimizers = 'compressed_materialization,statistics_propagation';
SET disable_perfect_hashing = true;
SET pin_threads = 'on';


