-- Only a handful of rows, but jumps of 1M to avoid perfect hashing

SET threads = 1; 


-- Clean up
DROP TABLE IF EXISTS a; 
DROP TABLE IF EXISTS b; 

-- Create Fact Table A
CREATE TABLE a AS 
SELECT 
    range AS id, 
    range % 5 AS barn, 
    range % 5 AS court 
FROM range(0,10_000_000, 1_000_000);

-- Table B
CREATE TABLE b AS SELECT range AS barn FROM range(0,4_000_000,1_000_000);

-- Update statistics for the cost-based optimizer
ANALYZE a;
ANALYZE b;

-- EXPLAIN ANALYZE SELECT count(*) 
SELECT min(b.barn) 
FROM a 
JOIN b ON a.barn = b.barn 