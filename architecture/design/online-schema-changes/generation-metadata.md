# Physical Generation Metadata (Online Schema Changes)

Detail companion to
`../online-schema-changes-async-shadow-roadmap.md` (Milestone A, "Generation
metadata + hidden shadow"). This document records the in-tree design and
findings for how a hidden second physical copy of a live table (a *shadow
generation*) is represented and kept invisible to everything user-facing.

## Concept

An online `ALTER TABLE` builds a second DocDB table (a new `relfilenode` /
physical generation) that shares the source table's stable logical identity
(`pg_class.oid`, carried by `SysTablesEntryPB.pg_table_id`). Until cutover the
new generation must be completely invisible to users and to every subsystem that
enumerates tables, yet remain addressable by internal machinery (the migration
job).

We model this with a **physical-generation role** on the table entity, distinct
from the existing `HideState`:

| Concept | Field | Meaning |
|---|---|---|
| Dropped/hidden user object (PITR, drop) | `SysTablesEntryPB.hide_state` (`VISIBLE`/`HIDING`/`HIDDEN`) | The USER object is gone or being hidden. |
| Physical generation role (this doc) | `SysTablesEntryPB.physical_generation_role` (`ACTIVE`/`SHADOW`/`RETIRED`) | Which physical copy of a *live* logical table this is. |

The two are orthogonal: a `SHADOW` generation is `VISIBLE` in the hide-state
sense (it is not a dropped object) but must still never appear to users. Keeping
them separate avoids overloading the drop/PITR machinery and its GC.

### Roles

- `ACTIVE` (default, and the value for every ordinary table): the user-visible
  generation.
- `SHADOW`: a hidden target generation being populated by a migration. Excluded
  from SQL lookup/listing, CDC discovery, backup enumeration, DDL events, and
  hidden-object GC.
- `RETIRED`: a former `ACTIVE` generation kept only for rollback/retention after
  a cutover; not user-visible.

`owning_migration_id` links a `SHADOW`/`RETIRED` generation to the
schema-migration job (`SysSchemaMigrationEntryPB`) that owns it, so access can be
fenced and cleanup driven by the job.

## Implementation

Fields (`src/yb/master/catalog_entity_info.proto`, `SysTablesEntryPB`):

```proto
enum PhysicalGenerationRole { ACTIVE = 0; SHADOW = 1; RETIRED = 2; }
optional PhysicalGenerationRole physical_generation_role = 43 [ default = ACTIVE ];
optional bytes owning_migration_id = 44;
```

Accessors (`src/yb/master/catalog_entity_info.h`, `PersistentTableInfo`):

- `physical_generation_role()`, `is_active_generation()`,
  `is_shadow_generation()`, `is_retired_generation()`.
- `visible_to_client()` now also requires `is_active_generation()`, so every
  caller that already gated on client visibility excludes shadow/retired
  generations for free.

Explicit exclusions added at the enumeration seams that do NOT go through
`visible_to_client()`:

- **`ListTables`** (`catalog_manager.cc`): skip non-active generations even when
  `include_not_running` is set (that flag otherwise surfaces PREPARING tables).
- **CDC discovery** (`IsTableEligibleForCDCSDKStream`, `xrepl_catalog_manager.cc`):
  a non-active generation is ineligible. The active generation carries the
  logical table's CDC; the migration performs the generation/schema handoff at
  cutover (roadmap Section 3.3).
- **Backup enumeration** (`catalog_manager_ext.cc`): the namespace-snapshot table
  filter already uses `visible_to_client()`, so shadows are excluded. Explicit
  single-table backup of a shadow id is rejected in migration preflight (future
  work).

## Findings and rationale

- **Reuse `visible_to_client()` as the choke point.** Most user-facing paths
  (SQL relcache lookups, `ListTables`, namespace backup) already funnel through
  it. Adding the generation check there covers them with one edit and avoids
  scattering role checks. The only places needing an explicit check are the ones
  that deliberately bypass client-visibility (`include_not_running`, CDC
  eligibility).
- **Do not reuse `HIDDEN`.** A shadow is not a dropped object; marking it
  `HIDDEN` would entangle it with PITR retention and the hidden-table GC, which
  would try to delete it. A `VISIBLE` + `SHADOW` combination keeps GC away
  (hidden-object GC only looks at `hide_state`).
- **Default `ACTIVE`** means the field is upgrade-safe: existing tables and old
  masters that never set it read as `ACTIVE`, i.e. unchanged behavior.

## Upgrade / rollback safety

- Proto fields 43/44 are appended; the `sys.catalog` physical schema is
  unchanged. Old masters ignore the unknown fields and treat every table as
  `ACTIVE` (its default), which is the pre-feature behavior.
- No shadow generation is ever created unless an online schema change runs
  (gated, preview). On rollback with no in-flight migration there is nothing to
  reconcile. A rollback with a live shadow present requires the migration to be
  cancelled first (tracked with the job lifecycle, not here).

## Executor phase pipeline (landed)

The master-owned job executor (`SchemaMigrationManager::AdvanceJob`) now drives
an observable phase pipeline inside the `RUNNING` state, advancing one phase per
background tick and persisting each transition:

```
NEW -> RUNNING[PREFLIGHT -> SHADOW_CREATING -> COPYING -> CUTOVER] -> SUCCEEDED
```

These phases surface through `yb_schema_migrations.phase` and
`yb_schema_migration_progress.state`, so a consumer sees a real lifecycle. No
DocDB storage work is performed yet - no table is mutated - so this is safe to
run (behind the preview gate) while the actual per-phase backend is built. The
phases are the seams where that backend plugs in:

| Phase | Will do (future) |
|---|---|
| `PREFLIGHT` | validate DDL/target, reject xCluster/unsupported shapes |
| `SHADOW_CREATING` | create the hidden `SHADOW` generation (below) |
| `COPYING` | fixed-HT distributed copy + CDC replay into the shadow |
| `CUTOVER` | barrier + adopt the shadow into the source OID |

## Shadow generation creation (landed)

`CatalogManager::CreateShadowGeneration(source_table_id, migration_id, epoch)`
creates the hidden generation entirely master-side (no Postgres backend, no
client RPC):

1. Read-lock the source; require a non-colocated, non-index, active YSQL table.
2. Copy its `schema`, `partition_schema`, and tablet count into a
   `CreateTableRequestPB`; set `pg_table_id` to the source's logical id (the
   shared identity) and the new fields `physical_generation_role = SHADOW` +
   `owning_migration_id`.
3. Mint a fresh relfilenode from the source database's normal OID space (via
   `ReservePgsqlOids`) and set the new physical `table_id`
   (`database_oid + relfilenode`), so it never collides with a future
   PG-allocated one.
4. Call the internal `CreateTable(&req, &resp, /*rpc=*/nullptr, epoch)`.

`CreateTableInfo` sets the role from the request, so the table is `SHADOW` from
its first sys-catalog write - never transiently visible as an ACTIVE copy. The
new `CreateTableRequestPB.physical_generation_role` / `owning_migration_id`
fields carry this (option (a): request-carried, atomic with the table row).

The migration job's `SHADOW_CREATING` phase calls this (gated by
`TEST_schema_migration_create_shadow` until the full copy/replay/cutover
pipeline exists) and records the result in
`SysSchemaMigrationEntryPB.shadow_table_id`. Idempotent across failover: if
`shadow_table_id` is already set, the phase is a no-op.

Tested by `pg_online_schema_change-test`:
`CreateShadowGeneration` (direct) and `MigrationCreatesShadowGeneration`
(end-to-end through the job): the shadow exists, is a `SHADOW` generation owned
by the migration, shares the source logical id, and stays out of `ListTables`.

## Copy (landed)

`CatalogManager::CopyGenerationData(source_table_id, shadow_table_id, epoch)`
bulk-copies the source data into the shadow by **tablet clone** (SST hard-link)
plus **snapshot restore**, reusing the clone infrastructure. Validated with real
data parity on an RF3, multi-tablet (4-tablet) cluster. Steps:

1. Allocate a fresh, non-zero `clone_request_seq_no` from the source namespace's
   monotonic counter. `seq_no = 0` is the "never cloned" sentinel and every
   clone would be trivially skipped.
2. Snapshot the source and wait `COMPLETE` (`CreateAndWaitTableSnapshot`).
   Entries are collected with `CollectEntriesFromActiveSysCatalog` using minimal
   flags - NOT `kAddIndexes` (unsupported for a single YSQL table id).
3. Create the target snapshot `imported = true` (no wait): the shadow tablets
   are still `CREATING` (the shadow was created `is_clone = true`), so they can't
   be live-snapshotted; this only allocates the target snapshot id/dirs the clone
   writes into.
4. Seed each shadow tablet's consensus peers from the source (so cloned replicas
   are not tombstoned on first heartbeat), then issue one `AsyncCloneTablet` per
   source/shadow tablet pair (zipped by partition range). The tserver hard-links
   the source snapshot SSTs into the shadow tablet.
5. Wait for every shadow tablet to reach `RUNNING` (the master leaves
   `created_by_clone` tablets for the source replicas to materialize; they come
   up as the cloned replicas heartbeat).
6. `Restore()` the target snapshot at its own hybrid time and wait `RESTORED` -
   this loads the hard-linked SSTs into the shadow tablets' active RocksDB.

Key requirements discovered:

- The shadow must be created with `is_clone = true` so its tablets are `CREATING`
  (materialized by the clone). If created `RUNNING`, `DoApplyCloneTablet` rejects
  them ("Clone target tablet is already present") and copies nothing.
- The `Restore` step is essential: without it the tablets are `RUNNING` with the
  hard-linked snapshot present but the data is not in the active RocksDB.

Prototype scope: assumes the source is quiesced (no incremental CDC replay of
post-snapshot writes yet); non-colocated single heap; clone runs synchronously
inside the COPYING phase (bounded by a deadline).

## Cutover (landed)

Cutover has two halves:

1. **Master role flip** - `CatalogManager::CutoverToShadow(source, shadow, epoch)`
   flips generation roles in one sys-catalog batch: shadow -> `ACTIVE`, old
   source -> `RETIRED`. This governs visibility/enumeration/GC.
2. **Postgres relfilenode repoint** - `yb_finalize_online_schema_change(rel
   regclass, migration_id text)` (a `yb_db_admin` SQL function) repoints the
   logical relation's `pg_class.relfilenode` to the shadow's relfilenode. This
   is the load-bearing step: in YSQL the physical table is resolved per-query
   from `pg_class.relfilenode` (via `YbGetRelfileNodeId`), not from the master's
   generation role, so this is what actually redirects reads/writes to the
   copied shadow data. It sets `yb_non_ddl_txn_for_sys_tables_allowed` for the
   catalog write and invalidates the relcache (same net effect as
   `swap_relation_files`, minus creating a new DocDB table). The shadow's
   relfilenode is surfaced to PG via `PgSchemaMigrationInfoPB.shadow_relfilenode`
   (the pg table oid encoded in the shadow's physical id).

Validated end to end (RF1 `FinalizeServesFromShadow` and live ysqlsh): after
finalize, `SELECT` on the original table name serves from the shadow generation
and returns all copied rows; `pg_class.relfilenode` has changed to the shadow's.

Prototype ordering caveat: the master auto-flips roles at the `CUTOVER` phase,
and the client calls `yb_finalize_online_schema_change` separately. For a
quiesced source there is no concurrent DML in the window between the two.
Coordinating them atomically (single distributed commit) and gating reads via a
catalog-version bump is future work.

## Job wiring and SQL entry (landed)

The executor phases call the real backend (gated by
`TEST_schema_migration_create_shadow`):
`SHADOW_CREATING` -> `CreateShadowGeneration`, `COPYING` -> `CopyGenerationData`,
`CUTOVER` -> `CutoverToShadow`. `TEST_schema_migration_stop_before_cutover` lets
tests observe the populated shadow while it is still a `SHADOW`.

The SQL entry point now takes the target relation:
`yb_start_online_schema_change(rel regclass, ddl text, request_id text)`. It
passes the relation's **relfilenode** (`YbGetRelfileNodeId`, the YB physical id,
not `pg_class.oid`) as `table_oid`, so the master resolves the correct source
physical generation. Verified end to end from `ysqlsh`: start returns an id, the
job runs shadow-create -> copy -> cutover to `SUCCEEDED`, and
`yb_schema_migrations` / `yb_schema_migration_progress` report the real
`table_oid` / `source_table_id`.

Tested by `pg_online_schema_change-test`: `MigrationCreatesShadowGeneration`
(stops before cutover; shadow is SHADOW, owned, hidden),
`MigrationCopiesAndCutsOver` (RF1 full pipeline to SUCCEEDED; source -> RETIRED,
shadow -> ACTIVE), and `MultiTabletMigration` (RF3 `PgOnlineSchemaChangeRf3Test`:
4-tablet source across all tservers, verifies the shadow holds the same DocDB
record count as the source - real data parity - then roles flipped).

## Not yet done (drives later steps)

- Atomically coordinate the master role flip and the PG relfilenode repoint
  (single distributed commit) and gate reads via a catalog-version bump, so
  cutover is safe under concurrent DML. Today they are two steps over a quiesced
  source, and `yb_finalize_online_schema_change` is a manual client call.
- Make COPYING resumable/async rather than blocking the catalog bg thread for
  the clone+restore duration (bounded by a deadline today).
- Incremental CDC replay of writes committed after the copy snapshot time: DONE
  (the `REPLAYING` phase; see "Online change replay" below). Remaining: shrink
  the cutover fence to just the final barrier (dual-write), delete the capture
  stream on terminal states to release retention, and handle structural schema
  changes (column add/drop) in the per-record translation.
- Migration-driven GC of `SHADOW` (on cancel) and `RETIRED` (after retention).
- Preflight rejection of explicit operations targeting a shadow physical id.
- Colocated / indexed / geo shapes (copy zips tablets 1:1 and skips indexes).

## Online change replay (gh-ost/LHM ordering)

The copy above (clone + snapshot restore) is a physical snapshot at a single
hybrid time `S`; it is O(metadata) (SST hard-link), so it scales to large
tablets, unlike a row-by-row backfill. But writes committed to the source
*after* `S` land only on the source and would be lost at cutover. To be correct
under concurrent DML without a whole-migration lock, the phases follow the
classic online-migration ordering:

1. **Arm capture before the snapshot** (`SHADOW_CREATING`/`COPYING`):
   `CatalogManager::ArmChangeCapture` creates an internal, slot-less change
   stream bound to the single source table (via
   `CreateNewCDCStreamForNamespace` with `cdcsdk_stream_create_options
   .bound_table_ids`, `NOEXPORT_SNAPSHOT` - we already clone the base data).
   Arming first installs WAL/history retention barriers so records from `S`
   onward are preserved. The stream id is persisted on the migration entry
   (`capture_stream_id`).
2. **Copy at `S`** (`COPYING`): existing clone + restore. `CopyGenerationData`
   now reports the source snapshot hybrid time `S` out; it is persisted as
   `copy_snapshot_ht`.
3. **Replay `> S`** (`REPLAYING`): `CatalogManager::ReplayGenerationChanges`
   polls `GetChanges` per source tablet from `S` and applies each change into
   the corresponding shadow tablet, then establishes a barrier
   `F = clock.Now()`, drains until each source tablet's `safe_hybrid_time >= F`,
   and persists `F` as `cutover_barrier_ht`. On return the shadow reflects every
   source write with `commit_time <= F`.
4. **Cutover** (`CUTOVER`): role flip + relfilenode repoint. A brief write fence
   at `F` (see below) closes the tail window; because the copy+replay already
   carried the shadow to `F`, the fenced window is just the pointer swap, not the
   whole migration.

### Apply path decision: logical (CDCSDK) vs raw-KV (xCluster-style)

Two apply paths were considered:

- **Raw-KV (xCluster style):** for a non-colocated YSQL table the row-key
  encoding carries no cotable/colocation prefix (verified against
  `dockv/doc_key.cc` `DocKeyEncoder::Schema`/`CotableId`/`ColocationId`), so raw
  DocDB KV pairs transplant *verbatim* into a clone shadow with only a
  packed-row `schema_version` remap. Least code - **but only while the shadow
  shares the source's physical encoding.** The whole point of an online `ALTER`
  is a *different* shadow schema (add/drop/retype a column), which changes the
  packed-row layout and column ids, so verbatim KV replay is incorrect for the
  target use case.
- **Logical (CDCSDK):** reconstruct each row from the change record and re-encode
  it against the shadow's own schema. Correct across schema changes. Chosen.

Key enabler (validated by `cdcsdk_ysql-test` `WalsenderQlValueSmoke`): a
`GetChanges` request with `cdcsdk_request_source = WALSENDER` returns YSQL column
values as clean `pg_ql_value` (`QLValuePB`), keyed by `column_name` with PG type
oid and attnum. This is a per-request flag with **no replication-slot
dependency**, and it works on user tablets (in-tree it is otherwise only
exercised via the VirtualWAL/slot path). `ReplayGenerationChanges` copies each
`pg_ql_value` into the correct slot of a shadow `PgsqlWriteRequestPB`
(hash/range/regular, keyed by the shadow schema) and applies INSERT/UPDATE as an
idempotent `PGSQL_UPSERT`, DELETE as `PGSQL_DELETE`.

Implementation findings:

- **Opening the shadow for writes:** the shadow is a SHADOW generation
  (`visible_to_client() == false`), so `YBClient::OpenTable` fails its
  visibility check ("object is not running"). `CatalogManager::
  BuildHiddenTableForWrite` builds the `client::YBTable` directly from the master
  `TableInfo` (schema + partition list) - the session write path routes by that
  partition list and does not re-check visibility.
- **Schema version:** `SchemaPB` carries no version, so the built `YBSchema` must
  have `set_version(table_entry.version())` or the shadow tablet rejects the
  write with `PGSQL_STATUS_SCHEMA_VERSION_MISMATCH`.
- **Arming must not run on the catalog bg thread's critical path for table
  creation:** arming a CDCSDK stream lazily creates the `cdc_state` system table,
  and the synchronous `WaitForCreateTableToFinish` inside that call deadlocks
  against the same leader/bg loop that must drive the new tablet to RUNNING. Fix:
  eagerly `CreateCdcStateTableIfNotFound` at migration *admission* (on the RPC
  thread), so by arming time the table is RUNNING and the wait returns instantly.
- **Checkpoint:** start GetChanges from `from_cdc_sdk_checkpoint` with
  `term=0,index=0,key="",write_id=0,snapshot_time=S`; `term=-1/index=-1` trips a
  `log_index > 0` CHECK in the tserver.

Caveats for later: colocated/cotable tables carry a differing key prefix; a
shadow with structural schema changes needs column add/drop handling in the
per-record translation (absent columns are skipped today).

### Cutover fence / downtime

YB offers a smoother redirect than a hard table lock: a **catalog-version bump**
forces backends to replan against the swapped relfilenode within ~1 heartbeat
(`FLAGS_heartbeat_interval_ms`, default 1000 ms) with no in-flight aborts;
`WaitForYsqlBackendsCatalogVersion` can confirm all backends flipped. For a
strict serialization instant, a momentary `LOCK TABLE ... ACCESS EXCLUSIVE`
(cluster-wide only when `enable_object_locking_for_table_locks` is on - off by
default on macOS, enable on the Linux test cluster) fences the swap for tens to
low-hundreds of ms. The prototype targets: replay to `F`, brief fence for the
swap, unfence + catalog-version bump. Full near-zero-downtime (dual-write +
permission-state flip, mirroring the index-backfill `IndexPermissions` state
machine) is future work.
