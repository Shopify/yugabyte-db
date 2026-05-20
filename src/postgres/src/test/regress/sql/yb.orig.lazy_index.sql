SET client_min_messages = warning;
DROP TABLE IF EXISTS yb_lazy_index_t;
DROP FUNCTION IF EXISTS yb_lazy_index_plan_has(text, text);

CREATE FUNCTION yb_lazy_index_plan_has(query_sql text, needle text)
RETURNS bool
LANGUAGE plpgsql
AS $$
DECLARE
  plan_line text;
BEGIN
  FOR plan_line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query_sql LOOP
    IF plan_line LIKE '%' || needle || '%' THEN
      RETURN true;
    END IF;
  END LOOP;

  RETURN false;
END;
$$;

CREATE TABLE yb_lazy_index_t(k int PRIMARY KEY, v int, w int) SPLIT INTO 1 TABLETS;
INSERT INTO yb_lazy_index_t
SELECT i, i, i % 10 FROM generate_series(1, 10000) AS i;

CREATE INDEX yb_lazy_index_t_v_lazy_idx
ON yb_lazy_index_t(v)
WITH (yb_external_maintenance = true);

CREATE INDEX yb_lazy_index_t_w_idx ON yb_lazy_index_t(w);

SET enable_seqscan = off;

SELECT yb_lazy_index_plan_has(
  'SELECT * FROM yb_lazy_index_t WHERE v = 42',
  'yb_lazy_index_t_v_lazy_idx') AS non_serving_uses_lazy_idx;

DO $$
BEGIN
  PERFORM yb_backfill_external_index('yb_lazy_index_t_v_lazy_idx'::regclass);
END
$$;

ALTER INDEX yb_lazy_index_t_v_lazy_idx SET (yb_lazy_index_serving = true);

SET yb_index_consistency = eventual;
SELECT yb_lazy_index_plan_has(
  'SELECT * FROM yb_lazy_index_t WHERE v = 42',
  'yb_lazy_index_t_v_lazy_idx') AS serving_eventual_uses_lazy_idx;

SET yb_index_consistency = strong;
SELECT yb_lazy_index_plan_has(
  'SELECT * FROM yb_lazy_index_t WHERE v = 42',
  'yb_lazy_index_t_v_lazy_idx') AS serving_strong_uses_lazy_idx;

SELECT yb_lazy_index_plan_has(
  'SELECT * FROM yb_lazy_index_t WHERE w = 7',
  'yb_lazy_index_t_w_idx') AS strong_uses_regular_idx;

SET yb_index_consistency = eventual;
INSERT INTO yb_lazy_index_t VALUES (20000, 20000, 777);

SELECT count(*) AS lazy_before_mock_refresh
FROM yb_lazy_index_t
WHERE v = 20000;

SET yb_index_consistency = strong;
SELECT count(*) AS strong_after_lazy_insert
FROM yb_lazy_index_t
WHERE v = 20000;

SELECT count(*) AS regular_index_after_insert
FROM yb_lazy_index_t
WHERE w = 777;

SET yb_index_consistency = eventual;
DO $$
BEGIN
  PERFORM yb_backfill_external_index('yb_lazy_index_t_v_lazy_idx'::regclass);
END
$$;

SELECT count(*) AS lazy_after_mock_refresh
FROM yb_lazy_index_t
WHERE v = 20000;

ALTER INDEX yb_lazy_index_t_v_lazy_idx SET (yb_lazy_index_serving = false);
SET yb_index_consistency = eventual;
SET plan_cache_mode = force_generic_plan;

PREPARE lazy_prepared_nonserving AS
SELECT * FROM yb_lazy_index_t WHERE v = 42;
SELECT yb_lazy_index_plan_has(
  'EXECUTE lazy_prepared_nonserving',
  'yb_lazy_index_t_v_lazy_idx') AS prepared_nonserving_before_flip;
ALTER INDEX yb_lazy_index_t_v_lazy_idx SET (yb_lazy_index_serving = true);
SELECT yb_lazy_index_plan_has(
  'EXECUTE lazy_prepared_nonserving',
  'yb_lazy_index_t_v_lazy_idx') AS prepared_nonserving_after_serving;
DEALLOCATE lazy_prepared_nonserving;

SET yb_index_consistency = eventual;
PREPARE lazy_prepared_eventual AS
SELECT * FROM yb_lazy_index_t WHERE v = 42;
SELECT yb_lazy_index_plan_has(
  'EXECUTE lazy_prepared_eventual',
  'yb_lazy_index_t_v_lazy_idx') AS prepared_eventual_before_strong;
SET yb_index_consistency = strong;
SELECT yb_lazy_index_plan_has(
  'EXECUTE lazy_prepared_eventual',
  'yb_lazy_index_t_v_lazy_idx') AS prepared_eventual_after_strong;
DEALLOCATE lazy_prepared_eventual;

SET yb_index_consistency = eventual;
ALTER INDEX yb_lazy_index_t_v_lazy_idx SET (yb_lazy_index_serving = true);
PREPARE lazy_prepared_serving AS
SELECT * FROM yb_lazy_index_t WHERE v = 42;
SELECT yb_lazy_index_plan_has(
  'EXECUTE lazy_prepared_serving',
  'yb_lazy_index_t_v_lazy_idx') AS prepared_serving_before_nonserving;
ALTER INDEX yb_lazy_index_t_v_lazy_idx SET (yb_lazy_index_serving = false);
SELECT yb_lazy_index_plan_has(
  'EXECUTE lazy_prepared_serving',
  'yb_lazy_index_t_v_lazy_idx') AS prepared_serving_after_nonserving;
DEALLOCATE lazy_prepared_serving;

RESET plan_cache_mode;
RESET yb_index_consistency;
RESET enable_seqscan;
RESET client_min_messages;

DROP TABLE yb_lazy_index_t;
DROP FUNCTION yb_lazy_index_plan_has(text, text);
