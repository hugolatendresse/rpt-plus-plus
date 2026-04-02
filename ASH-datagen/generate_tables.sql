-- ============================================================
-- Synthetic 3-Table Join Benchmark Data Generator for DuckDB
-- ============================================================
--
-- This script generates three synthetic tables:
--
--   R(sel_key_R, join_key_RS)
--   S(sel_key_S, join_key_RS, join_key_ST)
--   T(sel_key_T, join_key_ST)
--
-- intended for experiments with:
--
--   * filter selectivity
--   * pairwise join selectivity
--   * final 3-way join size
--   * selected rows that do not find a match
--
-- The query shape is:
--
--   SELECT ...
--   FROM R
--   JOIN S ON R.join_key_RS = S.join_key_RS
--   JOIN T ON S.join_key_ST = T.join_key_ST
--   WHERE R.sel_key_R <= cutoff_R
--     AND S.sel_key_S <= cutoff_S
--     AND T.sel_key_T <= cutoff_T;
--
-- Core design idea
-- ----------------
-- Among the filtered rows of S, we create four conceptual groups:
--
--   1) bridge rows   : join with both R and T
--   2) RS-only rows  : join with R but not T
--   3) ST-only rows  : join with T but not R
--   4) neither rows  : join with neither side
--
-- This lets us independently control:
--   * size of filtered R
--   * size of filtered S
--   * size of filtered T
--   * size of R ⋈ S
--   * size of S ⋈ T
--   * size of R ⋈ S ⋈ T
--   * explicit selected unmatched blocks for RS
--   * explicit selected unmatched blocks for ST
--
-- Important simplification
-- ------------------------
-- This script uses a deterministic construction.
-- The ST side is still one-to-one.
-- The RS side now supports repeated join keys across the WHOLE table:
--   * probe_multiplicity_in_R
--   * probe_multiplicity_in_S
--
-- Interpretation A is used:
--   * join_fraction_RS, join_fraction_ST, and bridge_fraction
--     keep their old meanings as fractions of filtered S rows
--   * the filtered S row classes stay unchanged
--   * repeated productive RS keys increase the R ⋈ S output size
--
-- Parameter meanings
-- ------------------
-- scale_factor
--     Multiplies the base table sizes. Use 1, 10, 100, or 1000.
--     To change the scale factor, look below for a line like:
--                ::BIGINT AS scale_factor
--
-- base_row_count_R / S / T
--     Base sizes for the three tables at scale factor 1.
--
-- selected_fraction_R / S / T
--     Fraction of each table that should pass the filter.
--     The filtered rows are simply those with:
--
--         sel_key_R <= filtered_R
--         sel_key_S <= filtered_S
--         sel_key_T <= filtered_T
--
-- join_fraction_RS
--     Fraction of filtered S rows that find a match in filtered R.
--
-- join_fraction_ST
--     Fraction of filtered S rows that find a match in filtered T.
--
-- bridge_fraction
--     Fraction of filtered S rows that join on BOTH sides and
--     therefore survive the full 3-way join.
--
-- probe_multiplicity_in_R
--     Every distinct join_key_RS in R appears exactly this many times.
--
-- probe_multiplicity_in_S
--     Every distinct join_key_RS in S appears exactly this many times.
--
-- unproductive_rate_RS
--     Symmetric selected non-match rate for the RS join:
--     creates equal-sized selected unmatched blocks in R and S.
--
-- unproductive_rate_ST
--     Symmetric selected non-match rate for the ST join:
--     creates equal-sized selected unmatched blocks in S and T.
--
-- Notes
-- -----
-- * join keys are BIGINT so the disjoint key ranges stay safe.
-- * sel keys are INTEGER.
-- * generation is deterministic and unshuffled before the shuffle step.
-- * if the parameter combination is infeasible, generator_status
--   will say so and the generated tables will be empty because
--   creation is gated on is_feasible.
--
-- ============================================================

-- ============================================================
-- 0) CLEAN UP AND BASIC SETUP
-- ============================================================

SET VARIABLE _gen_old_threads = current_setting('threads');
SET threads = 64;

DROP TABLE IF EXISTS R;
DROP TABLE IF EXISTS S;
DROP TABLE IF EXISTS T;

DROP TABLE IF EXISTS generator_params;
DROP TABLE IF EXISTS generator_counts;
DROP TABLE IF EXISTS generator_status;

-- .timer on

-- ============================================================
-- 1) PARAMETER BLOCK
-- ============================================================
-- Parameters are injected via `SET VARIABLE ...` in a driver SQL file.
-- Required variables include all numeric parameters below
-- ============================================================

CREATE OR REPLACE TEMP TABLE generator_params AS
SELECT
    getvariable('scale_factor')::BIGINT AS scale_factor,

    getvariable('base_row_count_R')::BIGINT AS base_row_count_R,
    getvariable('base_row_count_S')::BIGINT  AS base_row_count_S,
    getvariable('base_row_count_T')::BIGINT  AS base_row_count_T,

    getvariable('selected_fraction_R')::DOUBLE AS selected_fraction_R,
    getvariable('selected_fraction_S')::DOUBLE AS selected_fraction_S,
    getvariable('selected_fraction_T')::DOUBLE AS selected_fraction_T,

    getvariable('join_fraction_RS')::DOUBLE AS join_fraction_RS,
    getvariable('join_fraction_ST')::DOUBLE AS join_fraction_ST,
    getvariable('bridge_fraction')::DOUBLE AS bridge_fraction,

    getvariable('probe_multiplicity_in_R')::BIGINT AS probe_multiplicity_in_R,
    getvariable('probe_multiplicity_in_S')::BIGINT AS probe_multiplicity_in_S,

    getvariable('unproductive_rate_RS')::DOUBLE AS unproductive_rate_RS,
    getvariable('unproductive_rate_ST')::DOUBLE AS unproductive_rate_ST;



-- ============================================================
-- 2) DERIVED COUNTS + FEASIBILITY
-- ============================================================

CREATE OR REPLACE TEMP TABLE generator_counts AS
WITH p AS (
    SELECT * FROM generator_params
),
base AS (
    SELECT
        scale_factor,
        base_row_count_R * scale_factor AS row_count_R,
        base_row_count_S * scale_factor AS row_count_S,
        base_row_count_T * scale_factor AS row_count_T,

        -- Working row count for R: over-provisioned so that all
        -- multiplicity-related divisibility constraints on R are met.
        -- R is generated with this many rows, then trimmed to row_count_R.
        CAST(floor(
            CAST(floor(
                (base_row_count_S * scale_factor)::DOUBLE * selected_fraction_S
            ) AS BIGINT)::DOUBLE * join_fraction_RS
        ) AS BIGINT) * probe_multiplicity_in_R
        / (probe_multiplicity_in_S::DOUBLE * selected_fraction_R)
            AS working_row_count_R_exact,

        CAST(round(
            CAST(floor(
                CAST(floor(
                    (base_row_count_S * scale_factor)::DOUBLE * selected_fraction_S
                ) AS BIGINT)::DOUBLE * join_fraction_RS
            ) AS BIGINT) * probe_multiplicity_in_R
            / (probe_multiplicity_in_S::DOUBLE * selected_fraction_R)
        ) AS BIGINT) AS working_row_count_R,

        selected_fraction_R,
        selected_fraction_S,
        selected_fraction_T,

        join_fraction_RS,
        join_fraction_ST,
        bridge_fraction,

        probe_multiplicity_in_R,
        probe_multiplicity_in_S,

        unproductive_rate_RS,
        unproductive_rate_ST
    FROM p
),
filtered AS (
    SELECT
        *,
        CAST(floor(working_row_count_R * selected_fraction_R) AS BIGINT) AS filtered_R,
        CAST(floor(row_count_S * selected_fraction_S) AS BIGINT) AS filtered_S,
        CAST(floor(row_count_T * selected_fraction_T) AS BIGINT) AS filtered_T
    FROM base
),
productive AS (
    SELECT
        *,
        CAST(floor(filtered_S * bridge_fraction) AS BIGINT)  AS bridge_rows,
        CAST(floor(filtered_S * join_fraction_RS) AS BIGINT) AS matched_rows_RS_in_S,
        CAST(floor(filtered_S * join_fraction_ST) AS BIGINT) AS matched_rows_ST_in_S
    FROM filtered
),
classes AS (
    SELECT
        *,
        matched_rows_RS_in_S - bridge_rows AS rs_only_rows_in_S,
        matched_rows_ST_in_S - bridge_rows AS st_only_rows_in_S,
        filtered_S - matched_rows_RS_in_S - matched_rows_ST_in_S + bridge_rows AS neither_rows_in_S
    FROM productive
),
unproductive AS (
    SELECT
        *,
        CAST(floor(least(filtered_R, filtered_S) * unproductive_rate_RS) AS BIGINT) AS unproductive_rows_RS_each_side,
        CAST(floor(least(filtered_S, filtered_T) * unproductive_rate_ST) AS BIGINT) AS unproductive_rows_ST_each_side
    FROM classes
),
s_layout AS (
    SELECT
        *,

        neither_rows_in_S - unproductive_rows_RS_each_side - unproductive_rows_ST_each_side AS extra_neither_S,

        row_count_S - filtered_S AS unfiltered_S
    FROM unproductive
),
rs_distinct AS (
    SELECT
        *,

        CASE
            WHEN probe_multiplicity_in_S > 0
                 AND bridge_rows % probe_multiplicity_in_S = 0
            THEN bridge_rows / probe_multiplicity_in_S
            ELSE NULL
        END AS distinct_bridge_keys_RS,

        CASE
            WHEN probe_multiplicity_in_S > 0
                 AND rs_only_rows_in_S % probe_multiplicity_in_S = 0
            THEN rs_only_rows_in_S / probe_multiplicity_in_S
            ELSE NULL
        END AS distinct_rs_only_keys_RS
    FROM s_layout
),
r_layout AS (
    SELECT
        *,

        COALESCE(distinct_bridge_keys_RS, 0) + COALESCE(distinct_rs_only_keys_RS, 0) AS distinct_productive_keys_RS,

        (COALESCE(distinct_bridge_keys_RS, 0) + COALESCE(distinct_rs_only_keys_RS, 0)) * probe_multiplicity_in_R AS productive_rows_R_needed
    FROM rs_distinct
),
remainders AS (
    SELECT
        *,

        filtered_R - productive_rows_R_needed - unproductive_rows_RS_each_side AS extra_selected_R,
        working_row_count_R - filtered_R AS unfiltered_R,

        matched_rows_RS_in_S * probe_multiplicity_in_R AS expected_rs_join_rows,
        matched_rows_ST_in_S AS expected_st_join_rows,
        bridge_rows * probe_multiplicity_in_R AS expected_rst_join_rows,

        CASE
            WHEN probe_multiplicity_in_R > 0
                 AND working_row_count_R % probe_multiplicity_in_R = 0
            THEN working_row_count_R / probe_multiplicity_in_R
            ELSE NULL
        END AS distinct_join_keys_R,

        CASE
            WHEN probe_multiplicity_in_S > 0
                 AND row_count_S % probe_multiplicity_in_S = 0
            THEN row_count_S / probe_multiplicity_in_S
            ELSE NULL
        END AS distinct_join_keys_S
    FROM r_layout
)
SELECT
    *,
    (
        (selected_fraction_R BETWEEN 0 AND 1)
        AND (selected_fraction_S BETWEEN 0 AND 1)
        AND (selected_fraction_T BETWEEN 0 AND 1)
        AND (join_fraction_RS BETWEEN 0 AND 1)
        AND (join_fraction_ST BETWEEN 0 AND 1)
        AND (bridge_fraction >= 0)
        AND (bridge_fraction <= join_fraction_RS)
        AND (bridge_fraction <= join_fraction_ST)
        AND (join_fraction_RS + join_fraction_ST - bridge_fraction <= 1)

        AND (probe_multiplicity_in_R >= 1)
        AND (probe_multiplicity_in_S >= 1)

        AND (unproductive_rate_RS BETWEEN 0 AND 1)
        AND (unproductive_rate_ST BETWEEN 0 AND 1)

        AND (bridge_rows >= 0)
        AND (rs_only_rows_in_S >= 0)
        AND (st_only_rows_in_S >= 0)
        AND (neither_rows_in_S >= 0)
        AND (extra_neither_S >= 0)

        AND (distinct_bridge_keys_RS IS NOT NULL)
        AND (distinct_rs_only_keys_RS IS NOT NULL)

        AND (productive_rows_R_needed >= 0)
        AND (extra_selected_R >= 0)

        AND (distinct_join_keys_R IS NOT NULL)
        AND (distinct_join_keys_S IS NOT NULL)

        AND (abs(working_row_count_R_exact - working_row_count_R) < 0.001)
        AND (working_row_count_R >= row_count_R)

        AND (unproductive_rows_RS_each_side % probe_multiplicity_in_R = 0)
        AND (extra_selected_R % probe_multiplicity_in_R = 0)
        AND (unfiltered_R % probe_multiplicity_in_R = 0)

        AND (st_only_rows_in_S % probe_multiplicity_in_S = 0)
        AND (unproductive_rows_RS_each_side % probe_multiplicity_in_S = 0)
        AND (unproductive_rows_ST_each_side % probe_multiplicity_in_S = 0)
        AND (extra_neither_S % probe_multiplicity_in_S = 0)
        AND (unfiltered_S % probe_multiplicity_in_S = 0)
    ) AS is_feasible
FROM remainders;

CREATE OR REPLACE TEMP TABLE generator_status AS
WITH kv AS (
    SELECT
        key,
        CASE
            WHEN regexp_full_match(value, '^-?[0-9]+$')
                THEN format('{:,}', CAST(value AS BIGINT))
            WHEN try_cast(value AS DOUBLE) IS NOT NULL
                 AND CAST(value AS DOUBLE) = floor(CAST(value AS DOUBLE))
                THEN format('{:,}', CAST(CAST(value AS DOUBLE) AS BIGINT))
            WHEN try_cast(value AS DOUBLE) IS NOT NULL
                THEN printf('%.2f', CAST(value AS DOUBLE))
            ELSE value
        END AS formatted_value
    FROM (
        UNPIVOT (
            SELECT COLUMNS(*)::VARCHAR
            FROM generator_counts
        )
        ON COLUMNS(*)
        INTO
            NAME key
            VALUE value
    )
),
widths AS (
    SELECT
        max(length(key)) AS param_w,
        max(length(formatted_value)) AS val_w
    FROM kv
)
SELECT
    format('{:>' || param_w || '}', key) AS Parameter,
    format('{:>' || val_w || '}', formatted_value) AS Value
FROM kv, widths;

.print ======================= Generator Stats noted below =======================

SELECT * FROM generator_status;


-- ============================================================
-- 3) GENERATE R
-- ============================================================

.print ======================= Generating R, S and T next =======================

CREATE OR REPLACE TABLE R AS
WITH c AS (
    SELECT * FROM generator_counts
),
bounds AS (
    SELECT
        *,

        productive_rows_R_needed AS end_productive_R,
        productive_rows_R_needed + unproductive_rows_RS_each_side AS end_unproductive_RS,
        filtered_R AS end_selected_R
    FROM c
),
rows AS (
    SELECT i AS row_id
    FROM bounds, range(1, working_row_count_R + 1) AS t(i)
    WHERE is_feasible
)
SELECT
    CAST(row_id AS INTEGER) AS sel_key_R,
    CASE
        WHEN row_id <= (SELECT distinct_bridge_keys_RS * probe_multiplicity_in_R FROM bounds)
            THEN
                1
                + CAST(
                    floor(
                        ((row_id - 1)::DOUBLE)
                        / (SELECT probe_multiplicity_in_R::DOUBLE FROM bounds)
                    ) AS BIGINT
                )

        WHEN row_id <= (SELECT end_productive_R FROM bounds)
            THEN
                (SELECT distinct_bridge_keys_RS FROM bounds)
                + 1
                + CAST(
                    floor(
                        (
                            row_id
                            - (SELECT distinct_bridge_keys_RS * probe_multiplicity_in_R FROM bounds)
                            - 1
                        )::DOUBLE
                        / (SELECT probe_multiplicity_in_R::DOUBLE FROM bounds)
                    ) AS BIGINT
                )

        WHEN row_id <= (SELECT end_unproductive_RS FROM bounds)
            THEN
                1000000000000
                + 1
                + CAST(
                    floor(
                        (
                            row_id
                            - (SELECT end_productive_R FROM bounds)
                            - 1
                        )::DOUBLE
                        / (SELECT probe_multiplicity_in_R::DOUBLE FROM bounds)
                    ) AS BIGINT
                )

        WHEN row_id <= (SELECT end_selected_R FROM bounds)
            THEN
                1100000000000
                + 1
                + CAST(
                    floor(
                        (
                            row_id
                            - (SELECT end_unproductive_RS FROM bounds)
                            - 1
                        )::DOUBLE
                        / (SELECT probe_multiplicity_in_R::DOUBLE FROM bounds)
                    ) AS BIGINT
                )

        ELSE
            1200000000000
            + 1
            + CAST(
                floor(
                    (
                        row_id
                        - (SELECT end_selected_R FROM bounds)
                        - 1
                    )::DOUBLE
                    / (SELECT probe_multiplicity_in_R::DOUBLE FROM bounds)
                ) AS BIGINT
            )
    END::BIGINT AS join_key_RS
FROM rows;


-- ============================================================
-- 4) GENERATE S
-- ============================================================

CREATE OR REPLACE TABLE S AS
WITH c AS (
    SELECT * FROM generator_counts
),
bounds AS (
    SELECT
        *,

        bridge_rows AS end_bridge,
        bridge_rows + rs_only_rows_in_S AS end_rs_only,
        bridge_rows + rs_only_rows_in_S + st_only_rows_in_S AS end_st_only,
        bridge_rows + rs_only_rows_in_S + st_only_rows_in_S + unproductive_rows_RS_each_side AS end_unproductive_RS,
        bridge_rows + rs_only_rows_in_S + st_only_rows_in_S + unproductive_rows_RS_each_side + unproductive_rows_ST_each_side AS end_unproductive_ST,
        filtered_S AS end_selected
    FROM c
),
rows AS (
    SELECT i AS row_id
    FROM bounds, range(1, row_count_S + 1) AS t(i)
    WHERE is_feasible
)
SELECT
    CAST(row_id AS INTEGER) AS sel_key_S,

    CASE
        WHEN row_id <= (SELECT end_bridge FROM bounds)
            THEN
                1
                + CAST(
                    floor(
                        ((row_id - 1)::DOUBLE)
                        / (SELECT probe_multiplicity_in_S::DOUBLE FROM bounds)
                    ) AS BIGINT
                )

        WHEN row_id <= (SELECT end_rs_only FROM bounds)
            THEN
                (SELECT distinct_bridge_keys_RS FROM bounds)
                + 1
                + CAST(
                    floor(
                        (
                            row_id
                            - (SELECT end_bridge FROM bounds)
                            - 1
                        )::DOUBLE
                        / (SELECT probe_multiplicity_in_S::DOUBLE FROM bounds)
                    ) AS BIGINT
                )

        WHEN row_id <= (SELECT end_st_only FROM bounds)
            THEN
                2000000000000
                + 1
                + CAST(
                    floor(
                        (
                            row_id
                            - (SELECT end_rs_only FROM bounds)
                            - 1
                        )::DOUBLE
                        / (SELECT probe_multiplicity_in_S::DOUBLE FROM bounds)
                    ) AS BIGINT
                )

        WHEN row_id <= (SELECT end_unproductive_RS FROM bounds)
            THEN
                2100000000000
                + 1
                + CAST(
                    floor(
                        (
                            row_id
                            - (SELECT end_st_only FROM bounds)
                            - 1
                        )::DOUBLE
                        / (SELECT probe_multiplicity_in_S::DOUBLE FROM bounds)
                    ) AS BIGINT
                )

        WHEN row_id <= (SELECT end_unproductive_ST FROM bounds)
            THEN
                2200000000000
                + 1
                + CAST(
                    floor(
                        (
                            row_id
                            - (SELECT end_unproductive_RS FROM bounds)
                            - 1
                        )::DOUBLE
                        / (SELECT probe_multiplicity_in_S::DOUBLE FROM bounds)
                    ) AS BIGINT
                )

        WHEN row_id <= (SELECT end_selected FROM bounds)
            THEN
                2300000000000
                + 1
                + CAST(
                    floor(
                        (
                            row_id
                            - (SELECT end_unproductive_ST FROM bounds)
                            - 1
                        )::DOUBLE
                        / (SELECT probe_multiplicity_in_S::DOUBLE FROM bounds)
                    ) AS BIGINT
                )

        ELSE
            2400000000000
            + 1
            + CAST(
                floor(
                    (
                        row_id
                        - (SELECT end_selected FROM bounds)
                        - 1
                    )::DOUBLE
                    / (SELECT probe_multiplicity_in_S::DOUBLE FROM bounds)
                ) AS BIGINT
            )
    END::BIGINT AS join_key_RS,

    CASE
        WHEN row_id <= (SELECT end_bridge FROM bounds)
            THEN row_id

        WHEN row_id <= (SELECT end_rs_only FROM bounds)
            THEN 3000000000000 + (row_id - (SELECT end_bridge FROM bounds))

        WHEN row_id <= (SELECT end_st_only FROM bounds)
            THEN (row_id - (SELECT end_rs_only FROM bounds)) + (SELECT bridge_rows FROM bounds)

        WHEN row_id <= (SELECT end_unproductive_RS FROM bounds)
            THEN 3100000000000 + (row_id - (SELECT end_st_only FROM bounds))

        WHEN row_id <= (SELECT end_unproductive_ST FROM bounds)
            THEN 3200000000000 + (row_id - (SELECT end_unproductive_RS FROM bounds))

        WHEN row_id <= (SELECT end_selected FROM bounds)
            THEN 3300000000000 + (row_id - (SELECT end_unproductive_ST FROM bounds))

        ELSE 3400000000000 + (row_id - (SELECT end_selected FROM bounds))
    END::BIGINT AS join_key_ST
FROM rows;


-- ============================================================
-- 5) GENERATE T
-- ============================================================

CREATE OR REPLACE TABLE T AS
WITH c AS (
    SELECT * FROM generator_counts
),
rows AS (
    SELECT i AS row_id
    FROM c, range(1, row_count_T + 1) AS t(i)
    WHERE is_feasible
)
SELECT
    CAST(row_id AS INTEGER) AS sel_key_T,
    CASE
        WHEN row_id <= (SELECT bridge_rows FROM c)
            THEN row_id

        WHEN row_id <= (SELECT matched_rows_ST_in_S FROM c)
            THEN row_id

        WHEN row_id <= (SELECT matched_rows_ST_in_S + unproductive_rows_ST_each_side FROM c)
            THEN 4000000000000 + (row_id - (SELECT matched_rows_ST_in_S FROM c))

        WHEN row_id <= (SELECT filtered_T FROM c)
            THEN 4100000000000 + (row_id - (SELECT matched_rows_ST_in_S + unproductive_rows_ST_each_side FROM c))

        ELSE 4200000000000 + (row_id - (SELECT filtered_T FROM c))
    END::BIGINT AS join_key_ST
FROM rows;

.print ======================= Shuffle R, S and T =======================
-- The keys in the tables are ordered in a specific pattern, we can shuffle
-- the tuples to spread the regions in the S table
CREATE OR REPLACE TABLE R AS
    SELECT *
    FROM R
    ORDER BY hash(sel_key_R)
    LIMIT (SELECT row_count_R FROM generator_counts);

CREATE OR REPLACE TABLE S AS
    SELECT *
    FROM S
    ORDER BY hash(sel_key_S);

CREATE OR REPLACE TABLE T AS
    SELECT *
    FROM T
    ORDER BY hash(sel_key_T);


-- ============================================================
-- 6) BASIC SANITY CHECKS
-- ============================================================
.print ======================= Sanity checks =======================

SELECT 'R row count' AS check_name, COUNT(*) AS observed_rows FROM R
UNION ALL
SELECT 'S row count', COUNT(*) FROM S
UNION ALL
SELECT 'T row count', COUNT(*) FROM T;

WITH c AS (SELECT * FROM generator_counts)
SELECT 'filtered_R expected' AS check_name, filtered_R AS value FROM c
UNION ALL
SELECT 'filtered_R observed', COUNT(*)::BIGINT FROM R, c WHERE sel_key_R <= filtered_R
UNION ALL
SELECT 'filtered_S expected', filtered_S FROM c
UNION ALL
SELECT 'filtered_S observed', COUNT(*)::BIGINT FROM S, c WHERE sel_key_S <= filtered_S
UNION ALL
SELECT 'filtered_T expected', filtered_T FROM c
UNION ALL
SELECT 'filtered_T observed', COUNT(*)::BIGINT FROM T, c WHERE sel_key_T <= filtered_T;


-- ============================================================
-- 7) INTERMEDIATE TEST QUERIES
-- ============================================================
.print ======================= Intermediate Test Queries =======================

-- 7a) Filtered R only
WITH g AS (SELECT * FROM generator_counts)
SELECT COUNT(*) AS filtered_R_rows
FROM R, g
WHERE R.sel_key_R <= g.filtered_R;

-- 7b) Filtered S only
WITH g AS (SELECT * FROM generator_counts)
SELECT COUNT(*) AS filtered_S_rows
FROM S, g
WHERE S.sel_key_S <= g.filtered_S;

-- 7c) Filtered T only
WITH g AS (SELECT * FROM generator_counts)
SELECT COUNT(*) AS filtered_T_rows
FROM T, g
WHERE T.sel_key_T <= g.filtered_T;

-- 7d) Intermediate join: R ⋈ S
WITH g AS (SELECT * FROM generator_counts)
SELECT COUNT(*) AS rs_join_rows
FROM R
JOIN S ON R.join_key_RS = S.join_key_RS
JOIN g ON TRUE
WHERE R.sel_key_R <= g.filtered_R
  AND S.sel_key_S <= g.filtered_S;

-- 7e) Intermediate join: S ⋈ T
WITH g AS (SELECT * FROM generator_counts)
SELECT COUNT(*) AS st_join_rows
FROM S
JOIN T ON S.join_key_ST = T.join_key_ST
JOIN g ON TRUE
WHERE S.sel_key_S <= g.filtered_S
  AND T.sel_key_T <= g.filtered_T;

-- 7f) Final join: R ⋈ S ⋈ T
WITH g AS (SELECT * FROM generator_counts)
SELECT COUNT(*) AS rst_join_rows
FROM R
JOIN S ON R.join_key_RS = S.join_key_RS
JOIN T ON S.join_key_ST = T.join_key_ST
JOIN g ON TRUE
WHERE R.sel_key_R <= g.filtered_R
  AND S.sel_key_S <= g.filtered_S
  AND T.sel_key_T <= g.filtered_T;


-- ============================================================
-- 8) EXPECTED VS OBSERVED JOIN COUNTS
-- ============================================================
.print ======================= Check expected vs actual join cardinalities  =======================

WITH c AS (SELECT * FROM generator_counts)
SELECT 'RS join expected' AS check_name, expected_rs_join_rows AS value FROM c
UNION ALL
SELECT 'RS join observed',
       COUNT(*)::BIGINT
FROM R
JOIN S ON R.join_key_RS = S.join_key_RS
JOIN c ON TRUE
WHERE R.sel_key_R <= c.filtered_R
  AND S.sel_key_S <= c.filtered_S

UNION ALL
SELECT 'ST join expected', expected_st_join_rows FROM c
UNION ALL
SELECT 'ST join observed',
       COUNT(*)::BIGINT
FROM S
JOIN T ON S.join_key_ST = T.join_key_ST
JOIN c ON TRUE
WHERE S.sel_key_S <= c.filtered_S
  AND T.sel_key_T <= c.filtered_T

UNION ALL
SELECT 'RST join expected', expected_rst_join_rows FROM c
UNION ALL
SELECT 'RST join observed',
       COUNT(*)::BIGINT
FROM R
JOIN S ON R.join_key_RS = S.join_key_RS
JOIN T ON S.join_key_ST = T.join_key_ST
JOIN c ON TRUE
WHERE R.sel_key_R <= c.filtered_R
  AND S.sel_key_S <= c.filtered_S
  AND T.sel_key_T <= c.filtered_T;


-- ============================================================
-- 9) MULTIPLICITY CHECKS
-- ============================================================
.print ======================= Multiplicity Checks =======================

WITH c AS (SELECT * FROM generator_counts)
SELECT 'R distinct join_key_RS expected' AS check_name, distinct_join_keys_R AS value FROM c
UNION ALL
SELECT 'R distinct join_key_RS observed', COUNT(DISTINCT join_key_RS)::BIGINT FROM R
UNION ALL
SELECT 'S distinct join_key_RS expected', distinct_join_keys_S FROM c
UNION ALL
SELECT 'S distinct join_key_RS observed', COUNT(DISTINCT join_key_RS)::BIGINT FROM S;


-- ============================================================
-- 10) UNMATCHED FILTERED R ROWS (R anti-join S)
-- ============================================================
.print ======================= Filtered R rows that do not find a match in S =======================

WITH c AS (SELECT * FROM generator_counts)
SELECT COUNT(*) AS filtered_R_rows_not_matching_S
FROM R
CROSS JOIN c
LEFT JOIN S ON R.join_key_RS = S.join_key_RS AND S.sel_key_S <= c.filtered_S
WHERE R.sel_key_R <= c.filtered_R
  AND S.join_key_RS IS NULL;

SET threads = getvariable('_gen_old_threads');
RESET VARIABLE _gen_old_threads;
