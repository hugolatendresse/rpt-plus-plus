-- ============================================================
-- BENCHMARK: R ⋈ S
-- ============================================================
-- Requires: generate_tables.sql has been run first.
-- ============================================================

.output /dev/null

-- SET VARIABLE old_threads = current_setting('threads');
-- SET threads = getvariable('benchmark_threads');

PREPARE benchmark_query AS
WITH g AS (SELECT * FROM generator_counts)
SELECT COUNT(*) AS full_result_count
FROM R
JOIN S ON R.join_key_RS = S.join_key_RS
JOIN g ON TRUE
WHERE R.sel_key_R <= g.filtered_R
  AND S.sel_key_S <= g.filtered_S;

-- Warmup: prime the OS page cache.
EXECUTE benchmark_query;
