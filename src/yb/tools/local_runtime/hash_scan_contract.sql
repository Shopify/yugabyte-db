\set ON_ERROR_STOP on
-- Hash-scan contract for YSQL compatibility checks.

CREATE SCHEMA local_hash_contract;
SET search_path = local_hash_contract, pg_catalog;

CREATE TABLE t (
  k bigint,
  v text,
  PRIMARY KEY (k HASH)
) SPLIT INTO 4 TABLETS;
CREATE INDEX t_v_idx ON t (v);

CREATE TABLE tc (
  a bigint,
  b bigint,
  v text,
  PRIMARY KEY ((a, b) HASH)
) SPLIT INTO 4 TABLETS;


CREATE TEMP TABLE ref_t (
  bucket int,
  k bigint,
  v text,
  PRIMARY KEY (bucket, k, v)
);
CREATE TEMP TABLE seen_t (
  bucket int,
  k bigint,
  v text,
  PRIMARY KEY (bucket, k, v)
);
CREATE TEMP TABLE ref_tc (
  bucket int,
  a bigint,
  b bigint,
  v text,
  PRIMARY KEY (bucket, a, b, v)
);
CREATE TEMP TABLE seen_tc (
  bucket int,
  a bigint,
  b bigint,
  v text,
  PRIMARY KEY (bucket, a, b, v)
);

INSERT INTO t(k, v)
SELECT k, 'k' || k::text
FROM (
  SELECT g::bigint AS k FROM generate_series(-32, 96) AS g
  UNION
  SELECT g::bigint FROM generate_series(0, 1000000) AS g
  WHERE yb_hash_code(g::bigint) IN (0, 65535)
) keys;

INSERT INTO tc(a, b, v)
SELECT a, b, 'p' || a::text || '_' || b::text
FROM (
  SELECT g::bigint AS a, (g + 1000)::bigint AS b
  FROM generate_series(-32, 96) AS g
  UNION
  SELECT g::bigint, (g + 1)::bigint FROM generate_series(0, 1000000) AS g
  WHERE yb_hash_code(g::bigint, (g + 1)::bigint) IN (0, 65535)
) keys;

-- Scalar projection over a full scan is independent of hash-bound pushdown.
CREATE TEMP TABLE src_t AS SELECT k, v, yb_hash_code(k) AS hashv FROM t;
CREATE TEMP TABLE src_tc AS SELECT a, b, v, yb_hash_code(a, b) AS hashv FROM tc;

INSERT INTO ref_t SELECT (hashv / 257) * 257, k, v FROM src_t;
INSERT INTO ref_tc SELECT (hashv / 257) * 257, a, b, v FROM src_tc;

DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT lo, LEAST(lo + 257, 65536) AS hi
    FROM generate_series(0, 65535, 257) AS lo
  LOOP
    EXECUTE format(
      'INSERT INTO seen_t(bucket, k, v) SELECT %s, k, v FROM t WHERE yb_hash_code(k) >= %s AND yb_hash_code(k) < %s',
      r.lo, r.lo, r.hi
    );
    EXECUTE format(
      'INSERT INTO seen_tc(bucket, a, b, v) SELECT %s, a, b, v FROM tc WHERE yb_hash_code(a, b) >= %s AND yb_hash_code(a, b) < %s',
      r.lo, r.lo, r.hi
    );
  END LOOP;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM t WHERE yb_hash_code(k) = 0)
     OR NOT EXISTS (SELECT 1 FROM t WHERE yb_hash_code(k) = 65535) THEN
    RAISE EXCEPTION 'missing scalar witnesses';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM tc WHERE yb_hash_code(a, b) = 0)
     OR NOT EXISTS (SELECT 1 FROM tc WHERE yb_hash_code(a, b) = 65535) THEN
    RAISE EXCEPTION 'missing composite witnesses';
  END IF;
  IF (SELECT count(*) FROM t WHERE yb_hash_code(k) >= 12345 AND yb_hash_code(k) < 12345) <> 0
     OR (SELECT count(*) FROM tc WHERE yb_hash_code(a, b) >= 12345 AND yb_hash_code(a, b) < 12345) <> 0 THEN
    RAISE EXCEPTION 'empty bucket should yield zero rows';
  END IF;
END $$;

DO $$
DECLARE
  scalar_diff_count bigint;
  composite_diff_count bigint;
BEGIN
  SELECT count(*) INTO scalar_diff_count
  FROM (
    (SELECT bucket, k, v FROM ref_t EXCEPT ALL SELECT bucket, k, v FROM seen_t)
    UNION ALL
    (SELECT bucket, k, v FROM seen_t EXCEPT ALL SELECT bucket, k, v FROM ref_t)
  ) d;
  SELECT count(*) INTO composite_diff_count
  FROM (
    (SELECT bucket, a, b, v FROM ref_tc EXCEPT ALL SELECT bucket, a, b, v FROM seen_tc)
    UNION ALL
    (SELECT bucket, a, b, v FROM seen_tc EXCEPT ALL SELECT bucket, a, b, v FROM ref_tc)
  ) d;
  IF scalar_diff_count + composite_diff_count <> 0 THEN
    RAISE EXCEPTION 'reference/seen mismatch: scalar=%, composite=%', scalar_diff_count, composite_diff_count;
  END IF;
END $$;

DROP TABLE pg_temp.src_t, pg_temp.src_tc, pg_temp.ref_t, pg_temp.ref_tc,
           pg_temp.seen_t, pg_temp.seen_tc;
DROP SCHEMA local_hash_contract CASCADE;
RESET search_path;
SELECT 'hash_scan_contract_ok' AS status;
