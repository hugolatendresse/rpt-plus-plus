------------------------- Common Settings (engaged-THC variant) ---------------
-- Forces the THC to activate on most TPC-H / JOB joins so that the cost-model
-- and per-probe paths actually run.  Critical: this file deliberately omits
-- thc_collect_phase_rows, thc_first_read_only_phase_rows, and thc_l3_budget so
-- whatever compile-time defaults the binary ships with take effect.  Use this
-- for A/B runs where the binary is the independent variable.
--
-- Notable: on origin/hl/any_root the compile-time default for
-- thc_first_read_only_phase_rows is 999_999_999, which means the first-cycle
-- abandonment heuristics never fire at all.  On mp/optimization the default is
-- 50_000, so abandonment fires after ~100k probes per join.  That gap is the
-- whole point of running this benchmark file.

-- THC
SET thc_activation_threshold = 10_000;  -- aggressive: activate on nearly all joins
SET thc_collect_budget_fraction = 0.25;
SET thc_miss_below_which_skip_collect = 0.1;

-- General Parameters
SET max_temp_directory_size='0KiB';
SET threads = 1;
SET disable_perfect_hashing = true;
SET pin_threads = 'on';
SET enable_hash_join_timers = false;

-- Optimizer
SET transfer_graph_seed = 0;
SET use_seeded_root = false;
SET use_seeded_transfer_order = true;
SET join_order_mode = 'seeded_left_deep';
SET allow_build_probe_side_swap = false;
SET skip_unfiltered_tables_create_bf_plan = false;
SET skip_unfiltered_tables_graph_creation = false;
SET drop_bf_at_runtime = false;
