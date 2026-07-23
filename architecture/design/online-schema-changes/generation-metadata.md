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

## Cutover (partial - master side landed)

`CatalogManager::CutoverToShadow(source_table_id, shadow_table_id, epoch)` flips
roles in one sys-catalog batch: shadow -> `ACTIVE`, old source -> `RETIRED`.

**Important:** this master-side role flip alone does NOT redirect reads/writes.
In YSQL the physical table is resolved per-query from `pg_class.relfilenode`
(via `YbGetRelfileNodeId`), not from the master's generation role. A complete
cutover must also repoint the logical relation's `pg_class.relfilenode` to the
shadow's relfilenode (a Postgres-layer change, e.g. via a tserver RPC or the
`swap_relation_files` mechanics). That PG repoint is the remaining
correctness-critical piece; until it lands, the role flip is validated in
isolation (test asserts the roles, not query routing).

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

- The Postgres-layer `pg_class.relfilenode` repoint at cutover (the load-bearing
  step that actually redirects I/O). Needs a tserver RPC / PG catalog update;
  today only the master role flip is done, so reads/writes still resolve to the
  source generation.
- Make COPYING resumable/async rather than blocking the catalog bg thread for
  the clone+restore duration (bounded by a deadline today).
- Incremental CDC replay of writes committed after the copy snapshot time
  (the copy currently assumes a quiesced source).
- Migration-driven GC of `SHADOW` (on cancel) and `RETIRED` (after retention).
- Preflight rejection of explicit operations targeting a shadow physical id.
- Colocated / indexed / geo shapes (copy zips tablets 1:1 and skips indexes).
