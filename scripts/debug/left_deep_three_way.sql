-- Clean up
DROP TABLE IF EXISTS a; 
DROP TABLE IF EXISTS b; 
DROP TABLE IF EXISTS c;

-- Create Fact Table A
CREATE TABLE a AS 
SELECT 
    range AS id, 
    range % 5 AS barn, 
    range % 5 AS court 
FROM range(10);

-- Table B merges first
CREATE TABLE b AS SELECT range AS barn FROM range(4);

-- Table C merges second
CREATE TABLE c AS SELECT range AS court FROM range(8);

-- Update statistics for the cost-based optimizer
ANALYZE a;
ANALYZE b;
ANALYZE c;

-- EXPLAIN ANALYZE SELECT count(*) 
SELECT count(*) 
FROM a 
JOIN b ON a.barn = b.barn 
JOIN c ON a.court = c.court;