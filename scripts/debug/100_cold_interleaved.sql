-- 100 cold entries for each hot entry - interleaved
-- Hash table size: 1672MiB, of which 14MiB is hot
-- Does not use perfect hashing
SET rpt_forward_only = true;
SET max_temp_directory_size='0KiB';
SET threads = 1;
SET disabled_optimizers = 'compressed_materialization,join_order,build_side_probe_side,statistics_propagation';
SET thc_collect_phase_rows = 400_000;
SET disable_perfect_hashing = true;
SET pin_threads = 'on';
SET thc_l3_budget = 67108864;


PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'results.json';
PRAGMA profiling_coverage = 'SELECT';
-- PRAGMA profiling_mode = 'detailed';



CREATE TABLE a AS
SELECT
    range AS id,
    range % 40_000_000 AS keyB1
FROM range(0, 40_000_000_000, 100)
UNION ALL
SELECT 999_999_999 AS id, 999_999_999 AS keyB1;

CREATE TABLE b AS
WITH base_data AS (
    SELECT range AS keyB1,
           range AS valueB1,
           FALSE AS hot
    FROM range(0, 40_000_000)
    UNION ALL
    SELECT 999_999_999 AS keyB1,
           999_999_999 AS valueB1,
           FALSE AS hot
)
SELECT * FROM base_data
ORDER BY random();

ANALYZE a;
ANALYZE b;

EXPLAIN ANALYZE
SELECT min(b.valueB1)
FROM a
JOIN b ON a.keyB1 = b.keyB1;
