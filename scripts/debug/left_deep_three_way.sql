-- Clean up
DROP TABLE IF EXISTS a; 
DROP TABLE IF EXISTS b; 
DROP TABLE IF EXISTS c;

-- Create Fact Table A (The largest table)
-- barn values range 0-99, court values range 0-9
CREATE TABLE a AS 
SELECT 
    range AS id, 
    range % 5 AS barn, 
    range % 5 AS court 
FROM range(10);

-- Create Dimension Table B (Smallest, high selectivity)
-- Only 5% of A (where barn < 5) will match this table
CREATE TABLE b AS SELECT range AS barn FROM range(4);

-- Create Dimension Table C (Larger than B, low selectivity)
-- 100% of A will match this table
CREATE TABLE c AS SELECT range AS court FROM range(8);

-- Update statistics for the cost-based optimizer
ANALYZE a;
ANALYZE b;
ANALYZE c;

-- Inspect the plan
EXPLAIN ANALYZE SELECT count(*) 
FROM a 
JOIN b ON a.barn = b.barn 
JOIN c ON a.court = c.court;