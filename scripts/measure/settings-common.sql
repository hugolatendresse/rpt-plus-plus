------------------------- Common Settings -----------------------------

-- THC
SET thc_activation_threshold = 1_000_000;
SET thc_collect_budget_fraction = 1.00;
-- SET thc_collect_phase_rows = 1_000_000;
SET thc_collect_phase_rows = 100_000;
-- SET thc_collect_phase_rows = 8192;
SET thc_first_read_only_phase_rows = 100_000;
-- SET thc_first_read_only_phase_rows = 100_000;
-- SET thc_first_read_only_phase_rows = 8192;
-- SET thc_l3_budget = 16_777_216; -- 16M
-- SET thc_l3_budget = 25_165_824; -- 24M
-- SET thc_l3_budget = 33_554_432; -- 32M
SET thc_l3_budget = 37_748_736; -- 36MB
-- SET thc_l3_budget = 62_914_560; -- 60MB
-- SET thc_l3_budget = 67_108_864; -- 64MB
SET thc_miss_below_which_skip_collect = 0.0;

-- General Parameters
-- SET max_temp_directory_size='0KiB'; -- Setting this to 0 prevents spilling
SET threads = 8;
SET pin_threads = 'on';

-- Populate fine-grained hash-join timers (Build Time, Probe Time, Match Time,
-- THC Collect/Insert/Probe Time, ...) in the DuckDB profiling JSON. 
SET enable_hash_join_timers = false; -- CAREFUL - makes ASH-datagen take swings!

-- Optimizer
-- FINAL ANSWER FOR BENCHMARKS: keep all RPT+ optimizations on in all cases.
SET disable_perfect_hashing = false;
SET transfer_graph_seed = 0;
SET use_seeded_root = false;
SET use_seeded_transfer_order = true;
SET join_order_mode = 'seeded_left_deep'; -- Is what we want in the end and does not cause the issue
SET allow_build_probe_side_swap = true; -- Is what we want in the end and does not cause the issue
SET skip_unfiltered_tables_create_bf_plan = true; -- Run RPT+ logic to not create BF for tables with filters (during CreateBloomFilterPlan) // Is what we want in the end and does not cause the issue
SET skip_unfiltered_tables_graph_creation = true; -- Same as above but during TransferGraphCreation // CULPRIT!!!!
SET drop_bf_at_runtime = true; -- Give up BF creation at runtime due to selectivity or memory usage // Is what we want in the end and does not cause the issue

-- Runtime checks to freeze/abandon the THC
SET thc_enable_first_cycle_check = false;
SET thc_mu_s_method = 'none';
SET thc_enable_delta_check = false; -- Abandons if THC increases probe cost
SET thc_enable_shrinkage_check = false; -- Freezes if marginal gain not worth collection cost

-- -- NEW OPTIMIZER
-- SET disable_perfect_hashing = true; -- don't disable for all Cases. Just add a value of 1T in Ash-datagen so that it's not triggered. Never disable it - makes paper more honest.
-- SET transfer_graph_seed = 0;  -- 
-- SET use_seeded_root = false;
-- SET use_seeded_transfer_order = true;
-- SET join_order_mode = 'seeded_left_deep';
-- SET allow_build_probe_side_swap = false;
-- SET skip_unfiltered_tables_create_bf_plan = false; -- Run RPT+ logic to not create BF for tables with filters (during CreateBloomFilterPlan)
-- SET skip_unfiltered_tables_graph_creation = false; -- Same as above but during TransferGraphCreation
-- SET drop_bf_at_runtime = false; -- Give up BF creation at runtime due to selectivity or memory usage

-- -- OLD OPTIMIZER
-- SET disable_perfect_hashing = false;
-- SET transfer_graph_seed = 0;
-- SET use_seeded_root = false;
-- SET use_seeded_transfer_order = false;
-- SET join_order_mode = 'duckdb';
-- SET allow_build_probe_side_swap = true;
-- SET skip_unfiltered_tables_create_bf_plan = true; -- Run RPT+ logic to not create BF for tables with filters (during CreateBloomFilterPlan)
-- SET skip_unfiltered_tables_graph_creation = true; -- Same as above but during TransferGraphCreation
-- SET drop_bf_at_runtime = true; -- Give up BF creation at runtime due to selectivity or memory usage