-- ============================================================
-- BENCHMARK: R ⋈ S ⋈ T
-- ============================================================
-- Requires: generate_tables.sql has been run first.
-- ============================================================

.output /dev/null

PREPARE benchmark_query AS
WITH g AS (SELECT * FROM generator_counts)
SELECT COUNT(*) AS full_result_count
FROM R
JOIN S ON R.join_key_RS = S.join_key_RS
JOIN T ON S.join_key_ST = T.join_key_ST
JOIN g ON TRUE
WHERE R.sel_key_R <= g.filtered_R
  AND S.sel_key_S <= g.filtered_S
  AND T.sel_key_T <= g.filtered_T;

-- Warmup: prime the OS page cache.
EXECUTE benchmark_query;
