-- Ground truth for within-build-side multiplicity (mu_s) on TPC-H Q5 (SF configurable)
-- IMPORTANT: Compute mu_s for both plausible build sides of each merge. Use EXPLAIN ANALYZE under
-- ExactLeftDeep to confirm which side is the actual build side, then pick the corresponding mu_s.

PRAGMA disable_profiling;

SET threads = 16; -- adjust as desired
LOAD tpch;
-- call dbgen(sf = 50); -- ensure DB is generated already

-- Common filters / CTEs from tpch5_analysis.sql
WITH
REST AS (
  SELECT n.n_name AS n_name, c.c_custkey AS c_custkey, c.c_nationkey AS c_nationkey
  FROM customer c
  JOIN nation n ON c.c_nationkey = n.n_nationkey
  JOIN region r ON n.n_regionkey = r.r_regionkey
  WHERE r.r_name = 'ASIA'
),
ORDERS2 AS (
  SELECT o_orderkey, o_custkey
  FROM orders
  WHERE o_orderdate >= DATE '1994-01-01'
    AND o_orderdate <  DATE '1995-01-01'
),
BULK AS (
  SELECT n.n_name AS n_name, o.o_orderkey AS o_orderkey, c.c_nationkey AS c_nationkey
  FROM customer c
  JOIN orders o ON c.c_custkey = o.o_custkey
  JOIN nation n ON c.c_nationkey = n.n_nationkey
  JOIN region r ON n.n_regionkey = r.r_regionkey
  WHERE r.r_name = 'ASIA'
    AND o.o_orderdate >= DATE '1994-01-01'
    AND o.o_orderdate <  DATE '1995-01-01'
),
PENULTIMATE AS (
  SELECT b.n_name AS n_name, b.c_nationkey AS c_nationkey, l.l_suppkey AS l_suppkey,
         l.l_extendedprice AS l_extendedprice, l.l_discount AS l_discount
  FROM BULK b
  JOIN lineitem l ON l.l_orderkey = b.o_orderkey
)

-- Orders merge (pick the one that matches the actual build side):
-- If build side is filtered ORDERS: key = o_custkey
SELECT 'orders_build_filtered_orders' AS label,
       (SELECT COUNT(*)::DOUBLE FROM orders WHERE o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01')
       /
       (SELECT COUNT(DISTINCT o_custkey) FROM orders WHERE o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01')
       AS mu_s
UNION ALL
-- If build side is REST: key = c_custkey (this is unlikely for Q5 under ExactLeftDeep, but compute anyway)
SELECT 'orders_build_rest',
       (SELECT COUNT(*)::DOUBLE FROM REST)
       /
       (SELECT COUNT(DISTINCT c_custkey) FROM REST)
       AS mu_s
UNION ALL

-- Lineitem merge (pick the one that matches the actual build side):
-- If build side is BULK: key = o_orderkey
SELECT 'lineitem_build_bulk',
       (SELECT COUNT(*)::DOUBLE FROM BULK)
       /
       (SELECT COUNT(DISTINCT o_orderkey) FROM BULK)
       AS mu_s
UNION ALL
-- If build side is LINEITEM: key = l_orderkey
SELECT 'lineitem_build_lineitem',
       (SELECT COUNT(*)::DOUBLE FROM lineitem)
       /
       (SELECT COUNT(DISTINCT l_orderkey) FROM lineitem)
       AS mu_s
UNION ALL

-- Supplier merge (pick the one that matches the actual build side):
-- If build side is SUPPLIER: key = (s_nationkey, s_suppkey)
SELECT 'supplier_build_supplier',
       (SELECT COUNT(*)::DOUBLE FROM supplier)
       /
       (SELECT COUNT(DISTINCT (s_nationkey, s_suppkey)) FROM supplier)
       AS mu_s
UNION ALL
-- If build side is PENULTIMATE: key = (c_nationkey, l_suppkey)
SELECT 'supplier_build_penultimate',
       (SELECT COUNT(*)::DOUBLE FROM PENULTIMATE)
       /
       (SELECT COUNT(DISTINCT (c_nationkey, l_suppkey)) FROM PENULTIMATE)
       AS mu_s;
