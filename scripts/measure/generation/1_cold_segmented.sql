-- 1 cold entry for each hot entry - segmented
-- Hash table size: 28MiB, of which 14MiB is hot
-- Does not use perfect hashing

CREATE TABLE a AS
SELECT
    range AS id,
    range % 400_000 AS keyB1
FROM range(0, 400_000_000)
UNION ALL
SELECT 999_999_999 AS id, 999_999_999 AS keyB1;

CREATE TABLE b AS
WITH base_data AS (
    SELECT range AS keyB1,
           range AS valueB1
    FROM range(0, 800_000)
    UNION ALL
    SELECT 999_999_999 AS keyB1,
           999_999_999 AS valueB1
)
SELECT keyB1, valueB1, (keyB1 < 400_000) AS hot FROM base_data
ORDER BY hot, random();

ANALYZE a;
ANALYZE b;
