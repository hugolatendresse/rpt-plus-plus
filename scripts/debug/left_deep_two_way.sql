-- Uses Perfect Hash!

-- Clean up
DROP TABLE IF EXISTS a; 
DROP TABLE IF EXISTS b; 

-- Create Fact Table A
CREATE TABLE a AS 
SELECT 
    range AS id, 
    range % 5 AS barn, 
    range % 5 AS court 
FROM range(10);

-- Table B
CREATE TABLE b AS SELECT range AS barn FROM range(4);

-- Update statistics for the cost-based optimizer
ANALYZE a;
ANALYZE b;

-- EXPLAIN ANALYZE SELECT count(*) 
SELECT count(*) 
FROM a 
JOIN b ON a.barn = b.barn 