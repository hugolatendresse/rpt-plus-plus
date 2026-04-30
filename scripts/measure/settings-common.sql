------------------------- Common Settings -----------------------------

-- THC
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


-- General Parameters
SET max_temp_directory_size='0KiB';
SET threads = 1;
SET disable_perfect_hashing = true;
SET pin_threads = 'on';


-- Optimizer
SET transfer_graph_seed = 0;
SET use_seeded_root = false;
SET use_seeded_transfer_order = true;
SET join_order_mode = 'seeded_left_deep';
SET allow_build_probe_side_swap = false;
SET skip_unfiltered_tables_create_bf_plan = false; -- Run RPT+ logic to not create BF for tables with filters (during CreateBloomFilterPlan)
SET skip_unfiltered_tables_graph_creation = false; -- Same as above but during TransferGraphCreation
SET drop_bf_at_runtime = false; -- Give up BF creation at runtime due to selectivity or memory usage
