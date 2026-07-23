-- Online schema change migration tracking (roadmap Section 0).
-- Adds yb_start_online_schema_change / yb_cancel_schema_migration /
-- yb_get_schema_migrations and the yb_schema_migrations view for clusters that
-- were initialized before these catalog entries existed. Must stay in sync with
-- pg_proc.dat (oids 8119-8121), yb_system_views.sql, and yb_system_functions.sql.
BEGIN;
  SET LOCAL yb_non_ddl_txn_for_sys_tables_allowed TO true;

  INSERT INTO pg_catalog.pg_proc (
    oid, proname, pronamespace, proowner, prolang, procost, prorows, provariadic, protransform,
    prokind, prosecdef, proleakproof, proisstrict, proretset, provolatile, proparallel,
    pronargs, pronargdefaults, prorettype, proargtypes, proallargtypes, proargmodes,
    proargnames, proargdefaults, protrftypes, prosrc, probin, proconfig, proacl
  ) VALUES
    (8119, 'yb_start_online_schema_change', 11, 10, 12, 1, 0, 0, '-', 'f', false,
     false, false, false, 'v', 'u', 2, 0, 25, '25 25', NULL, NULL,
     '{ddl,request_id}', NULL, NULL, 'yb_start_online_schema_change', NULL, NULL, NULL),
    (8120, 'yb_cancel_schema_migration', 11, 10, 12, 1, 0, 0, '-', 'f', false,
     false, true, false, 'v', 'u', 1, 0, 16, '25', NULL, NULL,
     '{migration_id}', NULL, NULL, 'yb_cancel_schema_migration', NULL, NULL, NULL),
    (8121, 'yb_get_schema_migrations', 11, 10, 12, 1, 100, 0, '-', 'f', false,
     false, false, true, 'v', 'u', 1, 0, 2249, '25',
     '{25,25,25,25,25,20,26,26,26,25,1184,1184,25}',
     '{i,o,o,o,o,o,o,o,o,o,o,o,o}',
     '{state_filter,migration_id,kind,state,phase,state_epoch,database_oid,table_oid,' ||
     'submitted_by,submitted_ddl,created_time,updated_time,terminal_error}',
     NULL, NULL, 'yb_get_schema_migrations', NULL, NULL, NULL),
    (8122, 'yb_get_schema_migration_progress', 11, 10, 12, 1, 1000, 0, '-', 'f', false,
     false, false, true, 'v', 'u', 1, 0, 2249, '25',
     '{25,25,25,26,26,25,20,20,1184}',
     '{i,o,o,o,o,o,o,o,o}',
     '{state_filter,migration_id,work_kind,source_table_id,tablet_id,state,' ||
     'rows_done,rows_total,updated_time}',
     NULL, NULL, 'yb_get_schema_migration_progress', NULL, NULL, NULL)
  ON CONFLICT DO NOTHING;

  -- Dependency records (pg_depend has no OID/unique constraint; guard manually).
  DO $$
  BEGIN
    IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_depend WHERE refclassid = 1255 AND refobjid = 8119
    ) THEN
      INSERT INTO pg_catalog.pg_depend (
        classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype
      ) VALUES
        (0, 0, 0, 1255, 8119, 0, 'p'),
        (0, 0, 0, 1255, 8120, 0, 'p'),
        (0, 0, 0, 1255, 8121, 0, 'p'),
        (0, 0, 0, 1255, 8122, 0, 'p');
    END IF;
  END $$;
COMMIT;

-- Restrict the mutating functions to yb_db_admin (the C functions also enforce
-- this, but revoke EXECUTE from PUBLIC to match fresh initdb ACLs).
REVOKE EXECUTE ON FUNCTION yb_start_online_schema_change(text, text) FROM public;
GRANT EXECUTE ON FUNCTION yb_start_online_schema_change(text, text) TO yb_db_admin;
REVOKE EXECUTE ON FUNCTION yb_cancel_schema_migration(text) FROM public;
GRANT EXECUTE ON FUNCTION yb_cancel_schema_migration(text) TO yb_db_admin;

-- Read-only status view.
CREATE OR REPLACE VIEW pg_catalog.yb_schema_migrations WITH (use_initdb_acl = true) AS
  SELECT
    migration_id,
    kind,
    state,
    phase,
    state_epoch,
    database_oid,
    table_oid,
    submitted_by,
    submitted_ddl,
    created_time,
    updated_time,
    terminal_error
  FROM yb_get_schema_migrations('');

-- Per-work-unit progress detail view.
CREATE OR REPLACE VIEW pg_catalog.yb_schema_migration_progress WITH (use_initdb_acl = true) AS
  SELECT
    migration_id,
    work_kind,
    source_table_id,
    tablet_id,
    state,
    rows_done,
    rows_total,
    updated_time
  FROM yb_get_schema_migration_progress('');
