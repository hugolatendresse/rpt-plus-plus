-- 10 cold entries for each hot entry - interleaved
-- Hash table size: 128MiB, of which 14MiB is hot
-- Does not use perfect hashing

CREATE TABLE a AS
SELECT
    range AS id,
    range % 4_000_000 AS keyB1
FROM range(0, 20_000_000, 10)
UNION ALL
SELECT 999_999_999 AS id, 999_999_999 AS keyB1;

CREATE TABLE b AS
WITH base_data AS (
    SELECT range AS keyB1,
           range AS valueB1,
           FALSE AS hot
    FROM range(0, 4_000_000)
    UNION ALL
    SELECT 999_999_999 AS keyB1,
           999_999_999 AS valueB1,
           FALSE AS hot
)
SELECT * FROM base_data
ORDER BY random();

ANALYZE a;
ANALYZE b;
