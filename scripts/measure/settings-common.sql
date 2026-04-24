------------------------- Common Settings -----------------------------

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


-- SET join_order_mode = 'duckdb';
SET join_order_mode = 'exact_left_deep';

SET max_temp_directory_size='0KiB';
SET threads = 1;
SET disable_perfect_hashing = true;
SET pin_threads = 'on';

SET use_seeded_transfer_order = true;
SET thc_transfer_graph_seed = 0;
SET skip_unfiltered_tables = true;
SET disable_bf_dropping = true;