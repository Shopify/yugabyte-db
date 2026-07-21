# Online Schema Changes via Async Shadow-Table Mirroring

**Status:** Draft roadmap
**Tracking issue:** Online schema migrations [#4192](https://github.com/yugabyte/yugabyte-db/issues/4192)
**Related internal doc:** `Mirror Table - Functional and Design Doc.md`

This document is a proposed implementation roadmap for online, near-zero-downtime
schema changes in YugabyteDB (YSQL). It targets the class of `ALTER TABLE`
operations that currently require a full table rewrite and hold an
`ACCESS EXCLUSIVE` lock for O(rows) time.

The approach is: build a hidden second physical copy of the table (a separate
DocDB table generation), copy existing rows in parallel, keep it current with
asynchronous WAL/CDC-based mirroring, then perform an atomic storage switch that
preserves the table's logical PostgreSQL identity.

## Decisions and Scope

- Mirroring is **asynchronous** via WAL/CDC, not synchronous triggers.
  Trigger-based approaches (LHM-style) were considered and rejected as the core
  mechanism because they slow the foreground write path and do not scale with
  tablet count.
- Internal capture uses **CDCSDK logical records**, not raw Raft WAL.
- The initial implementation may reuse an internalized form of VWAL for
  cross-tablet transaction assembly. The long-term implementation distributes
  capture and apply per tablet/range.
- The original `pg_class.oid` is the **stable logical identity** and never
  changes. This is not a MySQL-style rename swap.
- **xCluster is out of scope.** Migrations on xCluster-managed tables are
  rejected in preflight.
- **Required before GA:** logical replication CDC, gRPC CDC, distributed/full
  backup, incremental backup, PITR, partitioned and geo-partitioned tables, and
  colocated tables. Early prototypes may support only regular, non-colocated
  tables.

### Terminology

```text
L  = stable logical table identity, based on original pg_class.oid
G0 = currently active physical DocDB generation
G1 = shadow (target) physical DocDB generation
S  = backfill snapshot HybridTime
F  = final source WAL barrier HybridTime
K  = catalog cutover commit HybridTime
```

Core invariant:

```text
G0 is the active generation for read time H < K
G1 is the active generation for read time H >= K
```

At any read time, exactly one physical generation is user-visible. There must be
no user DML committed exactly at `K`, so HybridTime-based generation selection is
unambiguous.

## Assumptions and Findings

- YSQL already separates logical identity (`pg_class.oid`) from physical storage
  identity (`pg_class.relfilenode`), and a DocDB table id encodes
  `database_oid + relfilenode_oid`. After a rewrite the two diverge, and
  `SysTablesEntryPB.pg_table_id` carries the stable logical id.
  - `src/postgres/src/backend/utils/misc/pg_yb_utils.c` (`YbGetRelfileNodeId`,
    `YbRelationSetNewRelfileNode`)
  - `src/yb/common/entity_ids.cc`, `src/yb/common/pg_types.h`
  - `src/yb/master/catalog_entity_info.cc` (`GetPgTableOid`)
- The current table rewrite path already creates a new DocDB table with a new
  relfilenode and swaps physical storage while preserving the logical OID, but it
  copies a snapshot and explicitly warns that concurrent DML may be lost.
  - `src/postgres/src/backend/commands/tablecmds.c` (`ATRewriteTables`,
    `ATRewriteTable`, unsafe-rewrite notice)
  - `src/postgres/src/backend/commands/cluster.c` (`make_new_heap`,
    `swap_relation_files`, `finish_heap_swap`)
- Online index backfill is the closest existing precedent for online copy: it
  picks a safe read time, scans per tablet, mirrors concurrent writes, retains
  delete markers, checks uniqueness across time, and survives failover.
  - `src/yb/master/backfill_index.cc`
  - `src/yb/tserver/tablet_service.cc` (backfill + safe time)
  - `src/yb/tablet/tablet.cc` (`BackfillIndexesForYsql`)
  - `architecture/design/online-index-backfill.md`
- CDCSDK / logical replication provides transaction-ordered logical rows,
  configurable full row images, consistent snapshot boundaries, per-tablet
  checkpoints, and tablet-split handling.
  - `src/yb/cdc/cdc_service.proto` (`PG_FULL`, `RowMessage`)
  - `src/yb/cdc/cdcsdk_producer.cc`, `src/yb/cdc/cdcsdk_virtual_wal.cc`
  - `src/yb/master/xrepl_catalog_manager.cc` (consistent snapshot, retention)
- Backup/PITR resolve committed DocDB tables against `pg_class.relfilenode` at
  the snapshot HybridTime; incremental backups are SST-file based.
  - `src/yb/master/catalog_manager_ext.cc` (repack/import)
  - `src/yb/master/master_snapshot_coordinator.cc`,
    `src/yb/master/restore_sys_catalog_state.cc`
- Current external-CDC limitations that this roadmap must replace: rewritten
  rows are re-streamed to logical replication, gRPC CDC rewrites are blocked, and
  colocated logical-CDC rewrites are blocked.
  - `src/yb/master/xrepl_catalog_manager.cc` (rewrite gate)
  - `docs/content/stable/additional-features/change-data-capture/using-logical-replication/advanced-topic.md`

## Current DDL Classification

Legend: **M** metadata-only, **V** validation scan, **I** index build/backfill,
**R** full table rewrite, **P** async placement move, **U** unsupported in-place.

| Operation | Current class | Uses new shadow flow? |
|---|---|---|
| Add nullable column, no default | M | No |
| Add column with nonvolatile default | M (missing value) | No |
| Add column with volatile default | R | Yes |
| Add identity / stored-generated column needing materialization | R | Yes |
| Drop ordinary non-key column | M | No |
| Drop primary-key column | R | Yes |
| Rename table / column / constraint | M | No |
| Set / drop column default | M | No |
| Set NOT NULL | V | No |
| Drop NOT NULL | M | No |
| Add CHECK / FOREIGN KEY (optionally NOT VALID) | V | No |
| Drop constraint | M (+ index delete) | No |
| Add UNIQUE | I (nonconcurrent today) | Prefer online index flow |
| Add UNIQUE USING INDEX | M | No |
| Add / drop PRIMARY KEY | R | Yes |
| ALTER TYPE binary-compatible, no affected index | M | No |
| ALTER TYPE compatible base, index semantics change | I (+ V) | Prefer online index flow |
| ALTER TYPE storage/data change or nontrivial USING | R | Yes |
| Replica identity change | M | No |
| SET TABLESPACE | M + P | No |
| Attach partition (proved bound, matching indexes) | M | No |
| Attach partition needing scan / child PK rewrite | V / I / R | If R |
| Detach partition | M (+ V) | No |
| Change colocation / tablegroup | U | Yes |
| Change tablet count / split points | U | Yes |
| Regular <-> partitioned | U | Yes |
| Repartition / change geo layout | U | Yes |

Metadata-only and index-backfill operations keep their current fast paths. The
new flow targets the **R** and **U** rows: the expensive rewrites and the
reshaping operations that have no in-place path today.

# Section 1: Shadow Generation and Distributed Copy

## 1.1 Physical identity

Create `G1` as a distinct DocDB table:

```text
G1 physical table id = database OID + new relfilenode
G1 pg_table_id       = stable logical table id L
generation role      = SHADOW
migration id         = durable UUID
```

`G1` is `RUNNING` (its tablets must serve internal reads/writes), not the
existing `HIDDEN` state which marks dropped/replaced objects for GC. Add an
explicit generation role independent of `HIDDEN`:

```text
ACTIVE | SHADOW | RETIRED
```

A `SHADOW` generation must be excluded from: normal table lookup and listing,
direct user DML, dynamic CDC publication discovery, ordinary backup enumeration,
user-visible DDL events, and hidden-object garbage collection. A guessed target
relfilenode must never be sufficient authorization; internal ops carry a job
capability/lease (prototype may use a superuser GUC bypass).

## 1.2 Job metadata

Add a first-class master-owned online-rewrite job (do not overload
`BackfillJobPB` long term):

```text
migration id, state, state epoch
logical table / root OID
G0 and G1 physical ids (base + index bundles)
source and target schemas + hashes
transform plan + dependency fingerprints
source and target partition manifests
S, F, K
per-range copy checkpoints
internal CDC stream id + checkpoints
capture frontier, apply frontier
external CDC handoff metadata
retention owners
error classification + retry state
```

Store high-volume per-tablet/range state in an internal replicated DocDB table
keyed by `(migration_id, source_table_id, tablet_lineage_id, range_start)`, not
in the master sys catalog. Every worker RPC carries `migration_id` and
`state_epoch`; stale workers are fenced after failover/cancel/cutover.

## 1.3 Creation sequence

1. Acquire a short distributed schema/object lock over the hierarchy.
2. Resolve the full dependency + partition hierarchy.
3. Reject xCluster-managed tables and unsupported CDC configurations.
4. Validate target schema, placement, capacity, and transform.
5. Allocate stable target physical ids (base + indexes + leaves).
6. Create target generations (hidden).
7. Wait for all target tablets to reach `RUNNING`.
8. Arm internal WAL, intent, and history retention on every source tablet.
9. Persist source checkpoints.
10. Choose snapshot HybridTime `S = MaxGlobalNow()`.
11. Start distributed copy and WAL capture.

Retention must be armed **before** `S` so no commit can fall between snapshot and
streaming.

## 1.4 Distributed copy

Work is scheduled by immutable source key range, not by tablet id alone:

```text
source physical table, source tablet/split lineage
range [start, end), snapshot HT S, next source DocKey
```

Each worker: reads source rows at fixed `S`; runs the target transform in
PostgreSQL semantics; routes rows via the target partition layout; writes base +
target indexes; persists the next source key after target ack; retries
idempotently after failover.

Source and target tablet layouts are independent (1:N and N:1 allowed). Snapshot
writes are timestamped at `S`; replay writes (Section 2) retain source commit
order above `S`, so a late copy write cannot overwrite a newer replayed
update/delete. Target heap/indexes retain tombstones until copy and replay
converge (as index backfill does with retain-delete-markers).

Reuse existing primitives: `BackfillTable`/`BackfillChunk` orchestration,
safe-time selection, `backfilled_until`-style checkpoints, and the internal
per-tablet PostgreSQL backfill connection generalized to full row transforms.

## 1.5 Table shapes

| Shape | Handling |
|---|---|
| Regular table | Workers per source tablet/range |
| Existing partitioned | Scan leaf tablets; parent is metadata only |
| Regular -> partitioned | Route source rows through target partition tree |
| Partitioned -> regular | All source leaves feed one target heap |
| Repartitioned | Independent source/target leaf sets |
| Geo-partitioned | Target leaves created in final tablespaces/placements; per-region throttle |
| Colocated -> same group | Allocate a new target colocation prefix in shared tablet |
| Colocated -> non-colocated | Scan source prefix; route to normal target tablets |
| Non-colocated -> colocated | Funnel source ranges into new colocated prefix |
| Colocated -> another group | Create target child under target group tablet |

Never reuse the source colocation id until the source generation is fully
retired. For colocated tables, physical SSTs may contain both generations;
logical visibility is by table metadata and generation prefix.

## 1.6 Transform policy

Initial support: identity projection, immutable casts, deterministic built-ins,
deterministic PK/partition-key transforms.

Later support: volatile defaults via persisted evaluated results or deterministic
event-seeded PRF; sequence/identity allocation with durable idempotency; UDFs
with pinned dependencies and explicit safety restrictions.

The transform is a versioned, compiled plan addressed by column identity, with
per-column policy `DERIVE | INITIALIZE | PRESERVE | DROP`. Backfill and mirror
writes must not fire user triggers or external side effects.

# Section 2: Streaming Fresh Data with WAL/CDC

## 2.1 Capture choice

Use a dedicated internal CDCSDK stream:

```text
PG_FULL row images
explicit checkpoints
consistent NOEXPORT snapshot boundary
static source physical table set
transaction ordering
no user publication membership
hard retention owned by migration id
```

Initial: reuse CDCSDK producer decoding + an internalized, corrected VWAL for
cross-tablet transaction assembly; bypass `pgoutput`/network serialization;
preserve real DocDB transaction ids (do not group solely by commit HT).

Long term: poll source tablets independently, persist distributed transaction
fragments, apply ranges in parallel, track a global resolved frontier, and avoid
the centralized VWAL bottleneck.

Raw Raft WAL is rejected as the API: it exposes intents, packed rows, and
metadata records without stable logical old/new images.

## 2.2 Boundary protocol

```text
commit HT <= S: represented by snapshot copy
commit HT >  S: represented by WAL replay
```

A transaction started before `S` but committed after `S` belongs to replay.
Retention protects Raft WAL, intents, historical row versions (for full
before-images), and historical schemas/packings.

## 2.3 Transaction records

Normalize to:

```text
BEGIN(source txn id, commit HT)
ROW(event id, source table/schema, full OLD, full NEW, source key)
COMMIT(source txn id, commit HT)
SAFEPOINT(tablet, resolved HT)
```

An update that changes the target key or partition becomes delete(OLD key) +
insert(NEW row). Force full old/new images independently of the user's
`REPLICA IDENTITY`; do not mutate `pg_class.relreplident`. Require intra-transaction
before-images (or collapse to first-old/final-new per row per transaction).

## 2.4 Idempotent apply

Transport is at-least-once; effects must be once. Each target mutation carries:

```text
migration id, source txn id, source OpId/write id,
source commit HT, stable event id, migration origin
```

Target-side mirror version marker `(source commit HT, txn discriminator,
mutation ordinal)`:

- older version -> ignore
- same event + same payload -> duplicate, ignore
- same version + different payload -> corruption
- newer version -> apply row/tombstone, update marker

Backfill versions order below all post-`S` replay events. Base-row and index KVs
are written with external MVCC HT equal to the source commit HT; the local target
Raft op commits at a local HT. Two checkpoint frontiers: capture (durably queued)
and apply (durably applied). Source CDC checkpoint may advance to capture; queue
deletion only to apply.

## 2.5 Constraints and errors

Async mirroring cannot reject an already-committed source transaction. On target
transform / uniqueness / NOT NULL / CHECK failure: stop checkpoint advancement,
keep `G0` authoritative, mark `FAILED_SEMANTIC`, persist source txn + safely
redacted key, require repair/cancel/restart. Foreign keys are created `NOT VALID`
and validated after convergence. Final validation happens at a prior frontier;
under the barrier only verify validation reached `F`.

## 2.6 Final barrier

1. Require snapshot complete and low mirror lag.
2. Lock the full source/target hierarchy in canonical OID order.
3. Drain source transactions (including prepared txns).
4. Choose `F = MaxGlobalNow()`.
5. Wait for every source tablet to resolve through `F`.
6. Apply every transaction with `S < commit HT <= F`.
7. Verify no partial transaction remains.
8. Validate constraints and target indexes through `F`.
9. Record dormant target CDC start checkpoints `Q1[t]`.
10. Proceed to atomic storage switch (Section 3).

No full scan / index rebuild while holding the barrier lock. On finalization
timeout, release the lock and return to replay; never hold the workload
indefinitely.

## 2.7 State machine

```text
NEW -> PREFLIGHT -> SHADOW_CREATING -> CAPTURE_ARMING -> SNAPSHOTTING
    -> REPLAYING -> VALIDATING -> READY -> FENCING
    -> CUTOVER_PENDING_VERIFICATION -> NEW_GENERATION_ACTIVE
    -> DRAINING_OLD_GENERATION -> SUCCEEDED

side: PAUSED_RESOURCE | FAILED_RETRYABLE | FAILED_SEMANTIC
    | CANCELLING | CANCELLED | CLEANUP_FAILED
```

Before `K`, cancel leaves `G0` authoritative and drops `G1`. After `K`, recovery
is roll-forward only.

# Section 3: Atomic Storage Switch

## 3.1 Near-term: OID-preserving relfilenode swap

Reuse and extend `make_new_heap()` / `swap_relation_files()` /
`finish_heap_swap()` with a caught-up-shadow variant:

1. Preserve the source table OID.
2. Apply target logical schema changes to the source OID.
3. Swap source and shadow physical relfilenodes.
4. Swap paired physical index storage; preserve surviving logical index OIDs.
5. Skip rebuilding already-validated target indexes.
6. Increment the database catalog version as a **breaking** change.
7. Invalidate source, old-physical, and new-physical cache entries.
8. Retire the old physical generation.
9. Commit the full table/index/partition bundle atomically.

The source OID stays referenced by views, rules, FKs, triggers, policies,
publications, row types, functions, ACLs/ownership, comments, and dependencies.

### Catalogs that change at the switch

`pg_class` physical fields; `pg_attribute` target schema; `pg_attrdef`;
`pg_constraint` (semantic only); `pg_index` (validity/def only when the DDL
changes it); `pg_depend` (changed semantics only); `pg_statistic` + extended
stats; catalog version + invalidation messages.

### Catalogs that stay keyed by the source OID

`pg_type.typrelid`; `pg_inherits`; `pg_partitioned_table`;
`pg_constraint.conrelid/confrelid/conindid`;
`pg_trigger.tgrelid`; `pg_rewrite.ev_class`; `pg_policy.polrelid`;
`pg_publication_rel.prrelid`; `pg_subscription_rel.srrelid`; ACLs, comments,
security labels, ownership, extension membership.

### Master / tserver metadata

Extend `SysTablesEntryPB` with generation state (role, generation number,
predecessor, rewrite id, valid-from/until, activation txn). Target base rows set
`pg_table_id = L` and carry the target index bundle; source rows become `RETIRED`
with `valid_until_ht = K`. Add a per-request generation fence on tablets so
normal requests carrying a retired generation are rejected (required because
catalog-version checks are skipped when object locking is enabled). Invalidate
pggate `PgSession` table cache, tserver `PgTableCache`, YBClient schema handles,
and `MetaCache` partition entries for old and new physical ids. Replace
single-relfilenode `YbTrackAlteredTableId` tracking with explicit
logical/old/new tracking.

Partitioned operations update the whole root/leaf/index bundle in one catalog
transaction (O(participating relations)); impose a temporary hierarchy-size limit
if needed.

### Failure recovery

Failure before PG commit leaves `G0` active and the migration cancellable.
Ambiguous commit is resolved from transaction status and committed
`pg_class.relfilenode`, never from object names; finalization is idempotent by
rewrite UUID. If PG commits but master finalization is interrupted, the
generation record conditioned on the same DDL transaction makes `G1` active and
background verification retires `G0`.

## 3.2 Long-term: logical-to-physical pointer flip

Introduce explicit records:

```text
LogicalTable { logical_table_id, active_generation,
               active_physical_table_id, pointer_epoch,
               logical_schema_version, prepared_flip }

PhysicalGeneration { logical_table_id, generation, physical_table_id,
                     generation_group_id, valid_from_ht, valid_until_ht,
                     state, predecessor, logical_to_physical_index_map,
                     ddl_transaction_id }
```

Cutover becomes activation of one generation group (constant work) instead of
rewriting every physical relation pointer, which matters for large partition
hierarchies. Transactions pin `(logical id, pointer epoch, physical generation,
schema version)` at first access and never silently re-resolve on retry.
Historical reads/CDC/PITR/backup select a generation by validity interval.

### Change surface (long term)

`PgObjectId` logical-vs-physical split; `PgTableDesc`; pggate table cache;
`PgClient` request protobufs; master schema/location lookup; master logical +
reverse generation maps; YBClient/MetaCache routing (physical); tablet generation
fencing + historical-read exception; CDC stream membership and `cdc_state`;
backup manifests + restore; PITR generation selection; admin/diagnostic APIs;
mixed-version compatibility via continued relfilenode mirroring during the
rollout window.

## 3.3 CDC during mirroring and switch

Internal backfill/mirror writes carry durable origins and are filtered
server-side before transaction assembly:

```text
USER | SHADOW_BACKFILL | SHADOW_MIRROR
```

External CDC observes exactly:

```text
all G0 user transactions with commit HT < K
one schema/generation transition at K
all G1 user transactions with commit HT > K
```

No internal backfill/mirror events. Filtering cannot rely on target table id
(post-cutover user writes use it), commit HT (mirrors preserve source HT),
`external_hybrid_time`, or volatile in-memory state.

**Logical replication:** ackable generation/schema transition (DDL currently has
no LSN and is inferred from later DML, which is insufficient when no DML follows);
retain old-tablet checkpoints through `K`; start new-tablet checkpoints after all
internal target writes; define slot restart when the restart HT is before, at, or
after `K`. Replace the current behavior where rewritten rows are re-streamed.

**gRPC CDC:** add a generation-transition record carrying logical table id, old
and new generation/tablets, `K`, old terminal checkpoints, new starting
checkpoints, and schema/hierarchy version. Until server + Debezium connector
negotiate this capability, reject migrations when an incompatible gRPC stream
covers the table.

**Transformed existing rows:** external CDC sees a schema transition, not one DML
per rewritten row. A consumer must apply the equivalent transform or opt into a
resnapshot; transforms that cannot be represented are rejected while CDC is
active unless the consumer opts into rebootstrap.

Retain `G0` and its WAL until every relevant slot/stream has crossed `K` or
expired under its own contract.

## 3.4 Backups and PITR during mirroring and switch

Ordinary backup at snapshot HybridTime `H` (the master-selected snapshot time,
not the job start time):

```text
H <  K -> export G0 only (even if G1 is fully populated)
H >= K -> export G1 only
```

A backup started before cutover but assigned `H < K` still backs up `G0`, even
if upload/repack completes after `K`. Shadow generations are excluded from
namespace enumeration. Backup selection must be catalog-as-of-time, not
collect-current-then-snapshot.

Restore yields an ordinary database: one active generation, no resumed migration,
migration job metadata stripped, CDC streams/slots not resumed.

**PITR** physically covers both generations while a schedule can restore across
`K`:

```text
restore T <  K -> restore G0, cancel the historical migration
restore T >= K -> restore G1 as active
```

Register candidate target tablets with the schedule from creation; retain the old
source after cutover per schedule retention; PITR and migration setup/cutover are
mutually exclusive.

**Incremental (YBC) backup** is SST-file based, so `G1` is new files. The first
incremental crossing `K` can approach full-table size (and, for colocated
targets, can amplify unrelated colocated tables sharing the tablet). Initial safe
choices: (1) force a full backup after cutover, or (2) reject an incremental that
crosses `K`. Long term, add a generation-replacement manifest so restore
materializes the selected generation's tablet inventory instead of overlaying
`G1` SSTs onto `G0`.

## 3.5 Partitioned, geo-partitioned, and colocated handling summary

- **Partitioned:** the rewrite group is root + all descendants + indexes.
  Lock the hierarchy against attach/detach/topology DDL. Backfill routes source
  leaf rows through the target partition tree (no 1:1 tablet mapping).
  Partition-moving updates need full old/new images. Cutover switches the whole
  hierarchy atomically; near term this updates every participating
  `pg_class.relfilenode`, long term it is one generation-group pointer.
- **Geo-partitioned:** each target leaf preserves/selects its tablespace and
  placement; validate placement before cutover; backfill writes directly to final
  geo placement; per-region throttling. PITR does not restore tablespaces, so
  referenced tablespaces must persist across the restore window.
- **Colocated:** table-level (not tablet-level) generation management. Allocate a
  new target colocation prefix in the shared tablet; scan only the source prefix;
  CDC filtering and checkpoints are per logical table within the shared tablet;
  do not delete the shared tablet or unrelated tables when one table cuts over.
  Colocated + logical CDC and any gRPC CDC are blocked initially. A safer
  intermediate is to build the shadow in a dedicated temporary tablegroup.

# Roadmap (Milestones)

| Milestone | Deliverable |
|---|---|
| A | Generation metadata, hidden shadow table, regular-table fixed-HT copy |
| B | Internal CDCSDK capture, durable queue, idempotent replay |
| C | Correct final barrier + OID-preserving relfilenode cutover |
| D | Logical CDC handoff, no internal duplicate events |
| E | Full backup, restore, PITR, old-generation retention |
| F | Partitioned + geo-partitioned generation groups |
| G | Colocated generations + shared-table CDC/backup behavior |
| H | gRPC CDC generation-transition protocol |
| I | Incremental backup replacement manifests or auto-full rollover |
| J | GA hardening: failover, upgrades, observability, cancellation |
| K | Long-term logical-to-physical generation pointer |

GA is blocked until D through J are complete. xCluster tables remain
unsupported.

# Required Testing

- Master / tserver / source-leader / target-leader failure at every phase.
- Crash before and after target apply and before/after checkpoint.
- Multi-tablet transaction spanning the final barrier.
- No user DML exactly at `K`.
- Logical slot restart with restart HT before, at, and after `K`.
- gRPC checkpoint transition and schema-before-DML ordering.
- CDC event-multiset comparison proving no backfill/mirror records.
- Backup snapshot selected before cutover but uploaded after it.
- Backup started before cutover but assigned `H >= K`.
- Full restore from both sides of `K`.
- Incremental chain crossing `K`, including compaction during migration.
- PITR before setup, during copy, immediately before, at, and after `K`.
- Source and target tablet splits; partition-key-changing updates.
- Nested/default partitions; attach/detach attempts blocked.
- Geo leaves in distinct tablespaces.
- Colocated source with unrelated active tables; colocated target sharing a tablet.
- Cancellation before cutover; ambiguous cutover commit.
- Disk/WAL/history retention pressure and exhaustion policy.
- Semantic transform and uniqueness failures.

# Open Decisions

1. CDC consumer contract when existing rows are transformed (apply transform vs.
   resnapshot vs. reject).
2. Initial behavior for incremental backups crossing cutover (auto-full vs.
   reject).
3. Durable representation of transformation plans (portable IR vs. canonical SQL
   + dependency fingerprints).
4. Volatile defaults and sequence/identity allocation semantics under retries.
5. Stable row identity for tables without an explicit primary key.
6. Eager target index maintenance vs. base copy then target index backfill.
7. Initial centralized VWAL vs. direct distributed CDCSDK assembly.
8. Hard-retention vs. migration cancellation under disk pressure (default:
   cancel to protect source availability).
9. Freeze source/target splits vs. live split-lineage handling.
10. Partition hierarchy size limit for near-term relfilenode cutover.
11. Physical id format: reuse PG-OID-based relfilenodes vs. generation UUIDs.
12. Job storage: standalone sys-catalog entity vs. state attached to source tables
    (standalone needs explicit PITR restore support).

# Non-Goals

- xCluster support.
- Trigger-based synchronous mirroring as the core mechanism.
- MySQL-style rename swap that changes the logical table OID.
- Replacing metadata-only or existing online-index paths.
