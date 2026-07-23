# Copy: Snapshot, Clone, and Restore

## Current versus target

| | Current prototype | Target |
|---|---|---|
| Bulk mechanism | Tablet snapshot + SST hard-link clone + restore | Same fast path where physical layouts are compatible; logical distributed copy otherwise |
| Tablet shape | Source and shadow zipped 1:1 by partition | Independent source/target layouts and routing |
| Schema | Same source/shadow schema | Transform source rows into target schema |
| Execution | Synchronous on catalog background phase | Resumable asynchronous workers |

## Why clone

For a very large tablet, row-by-row copy can take hours and write the full data
volume again. The prototype uses tablet clone: RocksDB SST files from a source
snapshot are hard-linked into the corresponding shadow tablet. Work is mostly
metadata and remains practical for multi-terabyte tablets.

This optimization applies only while source and target physical schemas and
partitions are compatible. Structural rewrites or repartitioning require a
logical scan/transform path.

## Boundary

Change capture is armed before snapshot creation. The source snapshot HybridTime
is persisted as `S`:

```text
commit_ht <= S : represented by the restored snapshot
commit_ht >  S : represented by logical replay
```

A transaction that starts before `S` but commits after `S` belongs to replay.

## Current copy algorithm

`CatalogManager::CopyGenerationData` performs:

1. Load source and shadow tablets ordered by partition and verify equal counts.
2. Allocate a fresh non-zero namespace `clone_request_seq_no`; zero means
   "never cloned" and would be skipped.
3. Snapshot the source and wait for `COMPLETE`.
4. Read and return the source snapshot HybridTime `S`.
5. Create an imported target snapshot without waiting for live shadow tablets.
6. Seed each shadow tablet's consensus peers from its source counterpart.
7. Schedule one `AsyncCloneTablet` per source/shadow pair.
8. Wait for all shadow tablets to become `RUNNING`.
9. Restore the target snapshot at its snapshot HybridTime.
10. Wait for restoration state `RESTORED`.

Key findings:

- Shadow tablets must be created as clone targets in `CREATING` state. A
  pre-existing `RUNNING` target is rejected as already present.
- The restore is mandatory. Without it, hard-linked snapshot files exist but are
  not loaded into the target tablet's active RocksDB.
- Source snapshot entry collection must not request index expansion for this
  single-table prototype.

## Target distributed copy

When clone is not applicable, copy work is keyed by immutable source range:

```text
(migration_id, source_table_id, tablet_lineage_id,
 range_start, range_end, S, next_source_key)
```

Workers read at `S`, evaluate a persisted target transform, route through the
target partition schema, update target indexes, and checkpoint after target ack.
Source and target tablet counts can differ. Backfill writes must order below
post-`S` replay so a late copy cannot resurrect a deleted row or overwrite a
newer update.

The transform contract and catalog-object side-effect rules are described in
[API and workflow evolution](api-and-workflows.md) and
[Catalog objects and dependency semantics](catalog-object-semantics.md).

## Capacity and throttling

The target design needs:

- per-node and per-tablet concurrency limits;
- read/write bandwidth throttling;
- disk-space preflight and pressure cancellation;
- per-region throttling for geo-partitioned targets;
- split-lineage handling rather than freezing tablet topology indefinitely.

## Code and tests

- `src/yb/master/catalog_manager.cc`: `CreateAndWaitTableSnapshot`,
  `CopyGenerationData`.
- `src/yb/master/clone/`: tablet clone orchestration.
- `src/yb/yql/pgwrapper/pg_online_schema_change-test.cc`:
  `MigrationCopiesAndCutsOver`, `FinalizeServesFromShadow`,
  `PgOnlineSchemaChangeRf3Test.MultiTabletMigration`.
