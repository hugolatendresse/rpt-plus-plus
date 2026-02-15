-- Does use perfect hashing with the unique keys!

-- Clean up
DROP TABLE IF EXISTS a; 
DROP TABLE IF EXISTS b; 

-- Create Fact Table A
CREATE TABLE a AS 
SELECT 
    range AS id, 
    range % 500_000 AS barn, 
    range % 500_000 AS court 
FROM range(10_000_000);

-- Table B
CREATE TABLE b AS SELECT range AS barn FROM range(1_000_000);

-- Update statistics for the cost-based optimizer
ANALYZE a;
ANALYZE b;

-- EXPLAIN ANALYZE SELECT count(*) 
SELECT count(*) 
FROM a 
JOIN b ON a.barn = b.barn 