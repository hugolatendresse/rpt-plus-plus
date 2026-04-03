[1mdiff --git a/scripts/measure/settings-common.sql b/scripts/measure/settings-common.sql[m
[1mindex 21b38e045e..33bf9ccb57 100644[m
[1m--- a/scripts/measure/settings-common.sql[m
[1m+++ b/scripts/measure/settings-common.sql[m
[36m@@ -13,9 +13,6 @@[m [mSET thc_miss_below_which_skip_collect = 0.1;[m
 [m
 SET max_temp_directory_size='0KiB';[m
 SET threads = 1;[m
[31m--- -- Statistics propagation sometimes just removes the cold portion[m
[31m-SET disabled_optimizers = 'join_order,build_side_probe_side,statistics_propagation'; -- TODO REMOVE THAT!!! And even remove statistics propagation for tpch and jobs[m
[31m--- SET disabled_optimizers = 'compressed_materialization,statistics_propagation';[m
 SET disable_perfect_hashing = true;[m
 SET pin_threads = 'on';[m
 [m
[1mdiff --git a/scripts/measure/settings-run_ash_datagen.sql b/scripts/measure/settings-run_ash_datagen.sql[m
[1mindex 2fc54c3f16..4cfb50e59b 100644[m
[1m--- a/scripts/measure/settings-run_ash_datagen.sql[m
[1m+++ b/scripts/measure/settings-run_ash_datagen.sql[m
[36m@@ -2,6 +2,8 @@[m
 -- Loaded after scripts/measure/settings-common.sql.[m
 -- Add SET/SET VARIABLE statements here when needed.[m
 [m
[32m+[m[32m-- statistics propagation removes the cold portion[m[41m [m
[32m+[m[32mSET disabled_optimizers = 'statistics_propagation';[m
 [m
 SET VARIABLE scale_factor = 40_000;[m
 [m
[1mdiff --git a/scripts/measure/settings-run_hugo_generated.sql b/scripts/measure/settings-run_hugo_generated.sql[m
[1mindex 18ceb392b9..6021123439 100644[m
[1m--- a/scripts/measure/settings-run_hugo_generated.sql[m
[1m+++ b/scripts/measure/settings-run_hugo_generated.sql[m
[36m@@ -1,3 +1,6 @@[m
 -- Overrides for scripts/measure/run_hugo_generated.sh.[m
 -- Loaded after scripts/measure/settings-common.sql.[m
 -- Add SET/SET VARIABLE statements here when needed.[m
[32m+[m
[32m+[m[32m-- statistics propagation removes the cold portion[m[41m [m
[32m+[m[32mSET disabled_optimizers = 'statistics_propagation';[m
\ No newline at end of file[m
