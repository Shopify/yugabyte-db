# Externally Maintained Lazy Indexes

## Goal

YugabyteDB can define a normal YSQL secondary index table without maintaining
that index in the base-table write transaction. An external materializer, such
as QueryEngine, owns population and refresh of the index. Reads may then use the
index with eventual-consistency semantics.

The first implementation is intentionally batch-only:

1. Create a secondary index with `WITH (yb_external_maintenance = true)`.
2. YugabyteDB creates the index table and catalog metadata, but skips the
   initial index build and suppresses base-table write-path maintenance.
3. The index starts non-serving because `yb_lazy_index_serving` defaults to
   `false`.
4. A mock materializer scans the base table and backfills index entries.
5. The materializer flips the index to serving with
   `ALTER INDEX ... SET (yb_lazy_index_serving = true)`.

## Read Semantics

`yb_index_consistency` controls whether lazy indexes are visible to the planner.

- `eventual`: serving lazy indexes are planned like regular indexes. This means
  index scans, index-only scans, covering columns, ordering, limits, bitmap
  scans, joins, and aggregate pushdown may all use the lazy index.
- `strong`: lazy indexes are excluded from planning. Regular Yugabyte-maintained
  indexes remain available.

Lazy indexes deliberately have materialized-view or read-replica semantics:
missing rows, stale rows, stale ordering, stale limits, and stale aggregates are
allowed when a query plan uses a lazy index.

`yb_index_consistency` is a planning-time policy, so changing it invalidates the
session plan cache. A prepared statement that was planned while lazy indexes were
allowed will be replanned after switching to `strong`.

`yb_lazy_index_serving` is also a planner-admission flag. Changing it invalidates
plans for the indexed base table so prepared statements replan when an index is
flipped serving or non-serving. The flag does not cancel already-running
queries; an in-flight query that planned while the index was serving may still
finish using that plan.

DocDB treats the index metadata as externally maintained, but the current PoC's
behavioral enforcement is in YSQL: reloptions drive planner gating, write-path
suppression, DDL validation, and the batch backfill helper.

## Initial Scope

The MVP only supports non-unique YB LSM indexes on non-partitioned YSQL tables.
Primary-key, unique, constraint, exclusion, partitioned, and concurrent index
creation paths are rejected for externally maintained indexes.

The mock materializer in `scripts/yb_lazy_index_mock_qe.py` is not a CDC service.
It only coordinates a batch backfill:

```sh
scripts/yb_lazy_index_mock_qe.py public.my_lazy_idx
```

Internally it marks the index non-serving, calls
`yb_backfill_external_index(regclass)` to scan the base table and write index
entries through the normal YB index build path, and then marks the index
serving. This gives end-to-end coverage for planner gating and serving state
before QueryEngine owns incremental maintenance.

The mock helper does not currently truncate the index table or remove stale
index entries; it is sufficient for initial population and insert-only refresh
tests.

## PoC Success Criteria

The regression coverage centers on the database contract:

- Non-serving lazy indexes are not planned.
- Serving lazy indexes are planned in `eventual` mode when the planner would
  otherwise prefer the index.
- Serving lazy indexes are not planned in `strong` mode.
- Base-table DML does not maintain lazy indexes.
- Ordinary Yugabyte-maintained indexes remain maintained and usable in `strong`
  mode.
- Generic prepared plans are invalidated across serving-state changes and
  `yb_index_consistency` changes.
