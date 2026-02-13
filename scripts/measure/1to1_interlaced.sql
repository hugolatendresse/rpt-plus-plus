-- https://duckdb.org/docs/stable/dev/profiling
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'scripts/measure/1to1_interlaced.json';

-- Cold and Hot in 1:1 proportion
-- Cold entries and Hot entries are interlaced
-- TODO ensure Does NOT use perfect hashing

-- Clean up
DROP TABLE IF EXISTS a; 
DROP TABLE IF EXISTS b; 

-- Create Fact Table A
CREATE TABLE a AS 
SELECT 
    range AS id, 
    range % 250_000 AS keyB1
FROM range(100_000_000);

-- Create Dimension Table B
CREATE TABLE b AS SELECT range % 250_000 AS keyB1 FROM range(500_000);

-- Update statistics for the cost-based optimizer
ANALYZE a;
ANALYZE b;

-- EXPLAIN ANALYZE SELECT count(*) 
SELECT count(*) 
FROM a 
JOIN b ON a.keyB1 = b.keyB1;