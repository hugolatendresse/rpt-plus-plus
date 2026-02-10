-- Minimal 3-table join for debugging
-- RPT+ USES THE FOLLOWING LEFT-DEEP PLAN: JOIN(JOIN(b,c),a)
CREATE TABLE a AS SELECT unnest(range(1, 6)) AS id;
CREATE TABLE b AS SELECT unnest(range(1, 5)) AS id;
CREATE TABLE c AS SELECT unnest(range(1, 4)) AS id;

SELECT count(*) FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id;
