-- No cold entries - all 400k entries are hot
-- Does not use perfect hashing


-- https://duckdb.org/docs/stable/dev/profiling
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'scripts/measure/0_cold.json';
-- PRAGMA profiling_mode = 'detailed';


-- Forces no disk spill, I think?
-- https://duckdb.org/docs/stable/configuration/overview#:~:text=max_temp_directory_size
SET max_temp_directory_size='0KiB';


-- Clean up
DROP TABLE IF EXISTS a; 
DROP TABLE IF EXISTS b; 

-- Create Fact Table A
-- Hits every key 1000 times
CREATE TABLE a AS 
SELECT 
    range AS id, 
    range % 4_000_000 AS keyB1
FROM range(0, 4_000_000_000, 10);

-- Create Dimension Table B
-- 400k entries in hashtable, all hot 
-- Since the range is > 1M wide, perfect hashing is disabled
CREATE TABLE b AS SELECT range AS keyB1 FROM range(0, 4_000_000, 10);

-- Update statistics for the cost-based optimizer
ANALYZE a;
ANALYZE b;

-- EXPLAIN ANALYZE SELECT count(*) 
SELECT count(*) 
FROM a 
JOIN b ON a.keyB1 = b.keyB1;