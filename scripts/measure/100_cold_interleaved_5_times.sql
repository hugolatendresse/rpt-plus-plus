-- 100 cold entries for each hot entry - interlaced
-- Hash table size: 1672MiB, of which 14MiB is hot
-- Does not use perfect hashing

-------- Case #1: Old DuckDB --------------  
-- SET disable_rpt = true;
-- SET disable_tiered_hash_cache = true;
------------------------------------------

-------- Case #2: RPT+ Forward Pass Only -------- 
-- SET rpt_forward_only = true;
-- SET disable_tiered_hash_cache = true;
-------------------------------------------------

-------- Case #3: RPT+ Forward + THC -------- 
-- SET rpt_forward_only = true;
---------------------------------------------

-------- Case #4: RPT+ Forward + Backward ----
SET disable_tiered_hash_cache = true;
----------------------------------------------

SET thc_collect_phase_rows = 400_000;



-- https://duckdb.org/docs/stable/dev/profiling
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'scripts/measure/100_cold_interleaved_5_times.json';
PRAGMA profiling_coverage = 'SELECT';
-- PRAGMA profiling_mode = 'detailed';


-- https://duckdb.org/docs/stable/configuration/overview#:~:text=max_temp_directory_size
SET max_temp_directory_size='0KiB'; -- Forces no disk spill, I think?
SET threads = 64; 
SET disabled_optimizers = 'compressed_materialization';


-- Clean up
DROP TABLE IF EXISTS a; 
DROP TABLE IF EXISTS b; 

-- Create Fact Table A
-- Hits every hot key 1000 times
CREATE TABLE a AS 
SELECT 
    range AS id, 
    range % 40_000_000 AS keyB1
FROM range(0, 40_000_000_000, 100)
UNION ALL
SELECT 999_999_999 AS id, 999_999_999 as keyB1; -- Have large min/max filter and disable perfect hashing

-- Create Dimension Table B
-- 400k hot entries in hashtable (1 in every 100), 40M total 
CREATE TABLE b AS
WITH base_data AS (
    SELECT range AS keyB1,
           range as valueB1,
           FALSE as hot
    FROM range(0, 40_000_000)    
 UNION ALL
    SELECT 999_999_999 as keyB1, -- Have large min/max filter and disable perfect hashing
           999_999_999 as valueB1,
           FALSE as hot

)
SELECT * FROM base_data
ORDER BY random();

-- Update statistics for the cost-based optimizer
ANALYZE a;
ANALYZE b;

PREPARE benchmark_query AS
SELECT min(b.valueB1) 
FROM a 
JOIN b ON a.keyB1 = b.keyB1;

-- Warmup: prime the OS page cache.
EXECUTE benchmark_query;

.print Running the 100_cold_interleaved benchmark query
SET VARIABLE t0 = epoch_ms(now());
.timer on
EXECUTE benchmark_query;
EXECUTE benchmark_query;
EXECUTE benchmark_query;
EXECUTE benchmark_query;
EXECUTE benchmark_query;
.timer off
SET VARIABLE t5 = epoch_ms(now());

.print Show the detailed timed query plan
.output stdout
EXPLAIN ANALYZE EXECUTE benchmark_query;

SELECT printf('Average run time: %.3f s', (getvariable('t5') - getvariable('t0')) / 5.0 / 1000.0) AS info;
