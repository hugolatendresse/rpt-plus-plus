------------------------- Common Settings (mp/optimization variant) -----------
-- Mirrors scripts/measure/settings-common.sql but drops the three SET overrides
-- whose compile-time defaults moved in mp/optimization:
--
--   thc_l3_budget                  (now 48 MiB default; was SET to 36 MiB)
--   thc_collect_phase_rows         (now 50_000 default; was SET to 1_000_000)
--   thc_first_read_only_phase_rows (now 50_000 default; was SET to 1_000_000)
--
-- Using this file lets the patched defaults take effect during box-plot runs
-- so we can measure the full design intent (code + defaults), not just the
-- code-side wins.
--
-- Once the PR lands and Hugo decides which defaults to keep, this file can be
-- dropped or merged back into settings-common.sql.

-- THC
SET thc_activation_threshold = 1_000_000;
SET thc_collect_budget_fraction = 0.25;
-- (thc_collect_phase_rows now uses the compile-time default of 50_000)
-- (thc_first_read_only_phase_rows now uses the compile-time default of 50_000)
-- (thc_l3_budget now uses the compile-time default of 48 MiB)
SET thc_miss_below_which_skip_collect = 0.1;


-- General Parameters
SET max_temp_directory_size='0KiB';
SET threads = 1;
SET disable_perfect_hashing = true;
SET pin_threads = 'on';

-- Populate fine-grained hash-join timers (Build Time, Probe Time, Match Time,
-- THC Collect/Insert/Probe Time, ...) in the DuckDB profiling JSON.
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
