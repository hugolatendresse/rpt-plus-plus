SET max_temp_directory_size='0KiB';
SET threads = 1;
SET disabled_optimizers = 'compressed_materialization,join_order,build_side_probe_side,statistics_propagation';
SET thc_collect_phase_rows = 400_000;
SET disable_perfect_hashing = true;