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
- **Submission is asynchronous with a durable, server-generated id.** Starting a
  migration returns a `migration_id` (a master-generated 16-byte UUID) as soon
  as the job is durably admitted, without waiting for copy/replay/cutover. The
  client uses that id to query status later. This mirrors the snapshot-
  restoration contract (`RestoreSnapshot -> restoration_id ->
  ListSnapshotRestorations`), not the current `CREATE INDEX` contract (which
  blocks until backfill completes and exposes no durable id).
- **Ordinary DDL wire behavior is preserved.** We do NOT change `ALTER TABLE`
  (or any existing DDL) to return a UUID. Returning an id from every DDL is
  broadly incompatible: encoding it in the `CommandComplete` command tag is not
  reachable through standard JDBC and can trip pgjdbc's trailing-token parser;
  encoding it as a result row flips libpq from `PGRES_COMMAND_OK` to
  `PGRES_TUPLES_OK` and makes JDBC `executeUpdate(DDL)` throw. The migration id
  is exposed only through an explicit, opt-in, row-returning API.
- **The status model is generic, with OSC as its first producer.** State is
  stored under a generic schema-migration entity keyed by `migration_id` and a
  `kind` discriminator (`ONLINE_TABLE_REWRITE` first). The design leaves room to
  later fold other long-running schema changes (index backfill, validation
  scans, `SET TABLESPACE` placement moves) into the same view, but that is not
  required for the OSC feature.
- **Status is queryable as read-only relations, not just functions.** Summary
  and per-tablet/range detail are exposed as `pg_catalog` relations filterable
  with ordinary predicates (`WHERE migration_id = ...`); mutating operations
  (start, cancel) remain functions. See Section "Migration Identity, State
  Tracking, and Observability".
- **A client-supplied idempotency key is optional and distinct from the id.**
  The server always owns the canonical `migration_id`. A caller may pass a
  request token so a retried submission after a lost response resolves to the
  same job rather than starting a second one.

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

## Prototype Findings (validated)

A working prototype has de-risked the core mechanics of this design. It has two
parts: an in-tree C++ regression test and an out-of-tree SQL/`pg_recvlogical`
harness. Artifacts:

- `src/yb/yql/pgwrapper/pg_online_schema_change-test.cc` (in-tree, committed).
- `prototype/osc-logical-slot/` (harness + applier + README/NEXT-STEPS).

What the prototype proves:

1. **OID-preserving storage switch already exists.** The current YB table
   rewrite path preserves `pg_class.oid` while moving `relfilenode` to a new
   DocDB table, and OID-bound dependents (views, foreign keys) keep working.
   Confirmed by `SwapPreservesOid` and `SwapPreservesDependents`. Implication:
   the roadmap's cutover is not new machinery to invent; the remaining work is
   to drive that swap from an externally-built, caught-up shadow instead of the
   inline snapshot copy at `tablecmds.c:6390-6409`.

2. **Distributed shape holds (N:M).** With source `SPLIT INTO 4` and shadow
   `SPLIT INTO 6` and a primary-key-changing transform (`new_id = id*2`),
   transformed rows fan out across all 6 shadow tablets, and multi-row
   cross-tablet source transactions apply atomically on the target when source
   `BEGIN`/`COMMIT` framing is preserved. A "single shadow DocDB table" is a
   multi-tablet distributed table; source and target tablet layouts need not
   match; routing is by transformed primary key.

3. **Exactly-once effect over at-least-once transport.** A per-source-
   transaction target transaction plus an idempotency-ledger row committed in
   the same transaction gives exactly-once effect. Killing the applier
   mid-stream causes the slot to replay; the ledger makes replayed transactions
   full no-ops (verified to skip mutations, not just the ledger insert).

4. **Real barrier drain (no sleep).** Fencing the source with ACCESS EXCLUSIVE,
   inserting a sentinel, and draining until the sentinel's transformed row is
   visible in the shadow is a deterministic "caught up" signal. Validated under
   a continuous background writer; post-cutover parity is exact including the
   full write tail.

Constraints and gotchas discovered (fold into design):

- The logical-slot SQL query API that exposes per-commit LSN
  (`ysql_yb_enable_replication_slot_query_api`) is a preview flag that did not
  take effect via session GUC or `--tserver_flags` in the prototype env; the
  prototype used `pg_recvlogical` + source `xid` as the idempotency key. A
  production applier should prefer the durable slot LSN / `restart_lsn`.
- `pg_current_wal_lsn()` is unsupported in YB (issue 30243); slot LSNs
  (`SEQUENCE` type) and `pg_current_wal_flush_lsn()` are not in comparable
  spaces, so "caught up" cannot be a naive LSN comparison. Use a sentinel or a
  per-tablet resolved-HybridTime vector (the real distributed signal).
- Do not spawn many short-lived `pg_recvlogical` consumers; each holds the slot
  `active` for a lease window and the next capture gets nothing. Drain once; the
  slot retains all unacked history.
- A doubling transform must use a `bigint` target key and cast (`id::bigint*2`)
  to avoid `int4` overflow on large source ids.
- YB replication slots can linger `active` briefly after the consumer exits;
  slot drop needs retry-with-backoff.

What the prototype did NOT prove (still open, drives the roadmap below):

- Wiring the real OID-preserving switch to an externally-built shadow (the
  harness cutover is still a name swap; the switch is validated only in the
  isolated C++ test).
- A master-owned migration job and its failover behavior.
- A per-tablet resolved-HybridTime barrier vector (prototype used a single-node
  sentinel).
- Any of the CDC-handoff, backup, PITR, partition, geo, or colocation behavior.

## Existing Status and Progress Surfaces (findings)

Surveyed so the migration status model reuses proven mechanics and avoids the
gaps that keep index backfill from being a durable, queryable operation.

- **Index backfill** is the closest execution precedent but is NOT a durable
  job: `BackfillJobPB` (embedded in the base table's `SysTablesEntryPB`) holds
  aggregate rows-read / rows-inserted and per-index state; per-tablet
  checkpoints live in `SysTabletsEntryPB.backfilled_until`. There is no job id,
  no authoritative percentage, and the job record is cleared on completion.
  `CREATE INDEX` (even plain, which is implicitly concurrent) waits for backfill
  via `PgClientSession::BackfillIndex(..., wait=true)`.
  - `src/yb/master/catalog_entity_info.proto` (`BackfillJobPB`)
  - `src/yb/master/backfill_index.cc` (launch/checkpoint/clear)
  - `src/yb/master/master_client.proto` (`GetIndexBackfillProgress` -> rows only)
  - `src/postgres/src/backend/catalog/system_views.sql`
    (`pg_stat_progress_create_index`: node-local, cleared on completion,
    `tuples_total` is an estimate)
- **Snapshot restoration** is the strongest reusable contract: a dedicated,
  server-generated `restoration_id` returned after durable admission, later
  looked up by `ListSnapshotRestorations`, persisted as its own
  `SysRestorationEntryPB` sys-catalog entity, and resumed after master failover.
  The migration job adopts this contract.
  - `src/yb/master/master_backup.proto` (`RestoreSnapshotResponsePB.restoration_id`,
    `SysRestorationEntryPB`, `RestorationInfoPB`, `ListSnapshotRestorations*`)
  - `src/yb/master/master_snapshot_coordinator.cc` (generate id, replicate, reload)
- **Queryable metadata precedent:** `yb_servers()`, `yb_local_tablets`,
  `yb_tablet_metadata` expose master/cluster state as `pg_catalog` relations
  (views over internal C SRFs that fetch via RPC). Important caveat for the
  status design: these SRF-backed views materialize the FULL result before SQL
  applies `WHERE`; a literal `WHERE id = ...` is a post-RPC filter, not RPC-side
  pushdown (`FunctionScan` fills a tuplestore, then `ExecScan` filters). So a
  plain view-over-SRF does not give efficient point lookup for large progress
  histories; the status relations need explicit `migration_id` pushdown (custom
  scan / parameterized RPC) or a directly queryable internal table.
  - `src/postgres/src/backend/catalog/yb_system_views.sql`,
    `src/postgres/src/include/catalog/pg_proc.dat`,
    `src/postgres/src/backend/utils/misc/pg_yb_utils.c`
  - `src/postgres/src/backend/executor/nodeFunctionscan.c`
  - Real indexed system catalog with DocDB key pushdown as the alternative model:
    `src/postgres/src/include/catalog/pg_yb_catalog_version.h`,
    `src/postgres/src/backend/access/yb_access/yb_scan.c`
- **Driver-compatibility finding** (why start is a function, not DDL): adding a
  UUID to the `CommandComplete` tag is unreachable through standard JDBC and
  ~39% of random UUIDs trip pgjdbc's numeric trailing-token parser; returning a
  UUID row flips libpq to `PGRES_TUPLES_OK` and makes JDBC `executeUpdate(DDL)`
  throw "A result was returned when none was expected". The repo itself relies
  on both behaviors (`libpq_utils.cc` treats tuples from `Execute` as an error;
  many `executeUpdate(DDL)` call sites in `java/yb-pgsql`).
  - `src/postgres/src/include/tcop/cmdtaglist.h` (`CMDTAG_ALTER_TABLE`, no rowcount)
  - `src/postgres/src/interfaces/libpq/fe-protocol3.c`,
    `src/yb/yql/pgwrapper/libpq_utils.cc`

## Current Instant / Existing Migration Classes (findings)

Today's YSQL schema changes fall into these cost classes; only the last two are
in scope for this feature, and none currently has a durable, queryable
operation identity tying the SQL statement to master-side work.

| Class | Examples | Lock / wait today | Durable state today |
|---|---|---|---|
| Metadata-only, near-instant | rename, set/drop default, drop NOT NULL, ADD ... USING INDEX | usually ACCESS EXCLUSIVE, trivial work | none operation-specific |
| Distributed metadata, no scan | add nullable / nonvolatile-default column, drop ordinary column | ACCESS EXCLUSIVE; waits for tablet schema-version propagation | master `ALTERING` (version/done only) |
| Async placement | `SET TABLESPACE` | metadata commits; replicas move in background | load-balancer moves, no unified job |
| Validation scan | `SET NOT NULL`, validate CHECK / FK, `ADD UNIQUE` (nonconcurrent) | ACCESS EXCLUSIVE (FK: SHARE ROW EXCL); synchronous PG scan | none (foreground) |
| Online index backfill | `CREATE INDEX` [CONCURRENTLY] | SHARE UPDATE EXCL; SQL waits for backfill | index permissions + `BackfillJobPB` (resumable, cleared on done) |
| Full table rewrite (**in scope**) | add/drop PK, volatile default, non-binary-compatible type change | ACCESS EXCLUSIVE; synchronous PG rewrite + swap | none unified; only constituent create/drop/DDL-verify |
| Reshape (**in scope**) | colocation/tablegroup change, repartition, geo relayout | no in-place path today | none |

Implication: OSC is the first schema change to get a durable, server-owned job
and a queryable status surface. The entity is deliberately generic
(`kind = ONLINE_TABLE_REWRITE` first) so backfill/validation/placement could be
represented later without a second framework.
  - Dispatch/lock selection: `src/postgres/src/backend/commands/tablecmds.c`
    (`ATController`, `ATRewriteTables`, `ATColumnChangeRequiresRewrite`)
  - Alter wait/state: `src/yb/client/table_alterer.cc`,
    `src/yb/master/catalog_manager.cc` (`IsAlterTableDone` == not `ALTERING`)

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

# Section 0: Migration Identity, State Tracking, and Observability

This is the user-facing contract and the durable substrate the rest of the
design reports through. It is a prerequisite for every later section: copy,
replay, barrier, and cutover all record progress here.

## 0.1 Submission and identity

Submission is asynchronous and returns a durable, server-generated id.

```sql
-- Returns a migration_id (uuid) immediately after durable admission.
SELECT yb_start_online_schema_change(
  ddl        => 'ALTER TABLE t ALTER COLUMN value TYPE bigint',
  request_id => NULL            -- optional client idempotency token
);
```

- The master generates a random 16-byte `migration_id` (UUID), writes the
  initial job row to the sys catalog, waits for that write to replicate, then
  returns the id. Returning only after durable admission matches snapshot
  creation and guarantees a subsequently supplied id is always resolvable.
- `request_id` is an optional client token, distinct from `migration_id`. If a
  submission is retried after a lost response, the same token resolves to the
  existing job instead of starting a second migration. The server still owns the
  canonical id.
- Ordinary `ALTER TABLE` is unchanged. Start is a row-returning SQL function so
  the uuid travels through every driver as a normal `uuid` value; callers must
  use `execute()` / `executeQuery()`, not `executeUpdate()`. The same id is also
  returned by the equivalent master RPC and `yb-admin` command.
- The submitting `ALTER TABLE`-style DDL semantics (whether a convenience
  wrapper blocks until terminal state) are layered on top of this async core;
  the async id is the primitive.

## 0.2 Storage: two tiers

State is split by cardinality and update frequency, following the split that
index backfill approximates (aggregate job vs. per-tablet checkpoint) but making
the job a first-class, durably-retained, id-addressable entity.

Tier 1 - one summary row per migration, a dedicated master sys-catalog entity
(a new `SCHEMA_MIGRATION` catalog entity type, modeled on `SNAPSHOT_RESTORATION`,
NOT embedded in `SysTablesEntryPB`):

```text
SysSchemaMigrationEntryPB
  migration_id            # server-generated UUID (primary identity)
  request_id              # optional client idempotency token
  kind                    # ONLINE_TABLE_REWRITE (first); extensible
  state, phase, state_epoch
  database_oid, logical_table_oid (L)
  g0_physical_id, g1_physical_id  (base + index bundles)
  source_schema_hash, target_schema_hash, plan_hash
  submitted_by, submitted_ddl_text
  created_ht, updated_ht, completed_ht
  S, F, K
  aggregate_counters      # rows/bytes copied+applied, tablets resolved, lag
  retry_state
  terminal_error          # AppStatusPB, separate from status-query error
```

Tier 2 - one row per work unit (potentially thousands), in an internal
replicated DocDB table (NOT a master sys-catalog protobuf, to avoid a hot,
oversized entity), keyed by the SOURCE tablet/range because transformed rows fan
out to many target tablets:

```text
key: (migration_id, work_kind, source_table_id, tablet_lineage_id, range_start)
val: state, checkpoint, rows_processed, bytes_processed,
     cdc_checkpoint, resolved_ht, applied_ht, attempt, worker_epoch, error
```

`work_kind` distinguishes copy, capture/apply, validation, and barrier work.
Workers update Tier 2 at chunk/transaction boundaries; the master updates the
Tier 1 summary only on phase transitions or coalesced intervals.

## 0.3 Observability: queryable relations

Status is exposed as read-only `pg_catalog` relations, filterable with ordinary
predicates, plus mutating functions for actions.

```sql
-- summary: one row per migration
SELECT * FROM yb_schema_migrations WHERE migration_id = '...';
SELECT * FROM yb_schema_migrations WHERE state = 'REPLAYING';

-- detail: one row per tablet/range work unit
SELECT * FROM yb_schema_migration_progress
WHERE migration_id = '...'
ORDER BY work_kind, tablet_id, range_start;

-- actions (mutating -> functions)
SELECT yb_cancel_schema_migration('...');
```

Requirements on these relations:

- Native `uuid` `migration_id` as the identity/lookup key.
- Efficient point lookup by `migration_id`. A plain view-over-SRF (the
  `yb_tablet_metadata` pattern) materializes all rows then filters, which does
  not scale to large progress histories; the relations must push
  `migration_id = ...` into the RPC / DocDB scan (custom scan or parameterized
  read), or be backed by a directly queryable internal table with the id as a
  key prefix. This is called out as Open Decision below.
- Also filterable by `state`, `kind`, `database`, `relation`, and submission
  time for a fleet-wide view.
- Read-only SQL permission distinct from the privilege to start/cancel.
- Tier 1 summary rows are retained after terminal state for a configurable
  window (so `SUCCEEDED` / `FAILED` remain queryable, unlike `BackfillJobPB`
  which is cleared); Tier 2 detail may be pruned earlier.

## 0.4 Progress semantics

- Raw counters and phase are authoritative; any percentage is advisory only.
  Tablet splits change the denominator, row totals are estimates, and replay
  lag is time-based rather than row-based.
- Phases derive from the state machine in Section 2.7 (e.g. `SNAPSHOTTING`,
  `REPLAYING`, `VALIDATING`, `FENCING`, `CUTOVER_PENDING_VERIFICATION`).
- Failover: a new master leader reloads all non-terminal jobs, bumps
  `state_epoch`, fences stale-epoch worker callbacks, and resumes. This is the
  snapshot-coordinator reload pattern applied to migrations.

## 0.5 Generic-but-scoped

The entity is generic (`kind`) and the relations are named generically
(`yb_schema_migrations`) so index backfill, validation scans, and placement
moves can later be surfaced through the same view. For this feature only
`ONLINE_TABLE_REWRITE` is implemented; folding in other kinds is explicitly out
of scope here and tracked as a follow-on.

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

The durable job entity and its two-tier storage are defined in Section 0.2
(`SysSchemaMigrationEntryPB` summary + internal per-work-unit table). It is a
first-class master-owned entity, not an overload of `BackfillJobPB`. Beyond the
public status fields, the job additionally carries the copy/replay internals:

```text
transform plan + dependency fingerprints
source and target partition manifests
internal CDC stream id + checkpoints
capture frontier, apply frontier
external CDC handoff metadata
retention owners
```

Every worker RPC carries `migration_id` and `state_epoch`; stale workers are
fenced after failover/cancel/cutover. High-volume per-tablet/range state lives
in the Tier 2 internal table keyed by
`(migration_id, work_kind, source_table_id, tablet_lineage_id, range_start)`,
which is exactly what `yb_schema_migration_progress` (Section 0.3) reads.

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

These states are the authoritative source of the `state` / `phase` columns
exposed by `yb_schema_migrations` (Section 0.3). The side states map to the
public terminal/paused values (`FAILED`, `CANCELLED`, `PAUSED_RESOURCE`), and
`terminal_error` carries the classified reason. `state_epoch` is bumped on every
master-leader reload so stale-epoch worker callbacks are fenced.

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

Status legend: [proto] prototype-validated (mechanic proven out-of-tree or in
an isolated test); [done] landed in-tree; otherwise not started.

| Milestone | Deliverable | Status |
|---|---|---|
| A0 | Durable async migration job entity + `migration_id` + queryable status/progress relations | not started |
| A | Generation metadata, hidden shadow table, regular-table fixed-HT copy | partial: OID-preserving switch and fixed-HT copy [proto]; hidden-generation metadata not started |
| B | Internal CDCSDK capture, durable queue, idempotent replay | [proto] via logical slot + `(slot,xid)` ledger; internal CDCSDK stream not started |
| C | Correct final barrier + OID-preserving relfilenode cutover | [proto] sentinel barrier; OID-preserving swap [done] in isolated test; not yet wired to a caught-up shadow |
| D | Logical CDC handoff, no internal duplicate events | not started |
| E | Full backup, restore, PITR, old-generation retention | not started |
| F | Partitioned + geo-partitioned generation groups | not started |
| G | Colocated generations + shared-table CDC/backup behavior | not started |
| H | gRPC CDC generation-transition protocol | not started |
| I | Incremental backup replacement manifests or auto-full rollover | not started |
| J | GA hardening: failover, upgrades, observability, cancellation | not started |
| K | Long-term logical-to-physical generation pointer | not started |

GA is blocked until D through J are complete. xCluster tables remain
unsupported.

## Next Investigations (post-prototype, in priority order)

The prototype validated the three core mechanics in isolation (switch, applier,
barrier). The following are the concrete next in-tree work items; each is scoped
to be landable behind a preview gate with its own test.

0. **Durable async migration job + queryable status (Milestone A0, Section 0).**
   Add the `SCHEMA_MIGRATION` sys-catalog entity (`SysSchemaMigrationEntryPB`,
   modeled on `SysRestorationEntryPB`) and the Tier 2 internal progress table.
   Add master RPCs `StartSchemaMigration` (returns server-generated
   `migration_id` after durable admission), `GetSchemaMigrationStatus`,
   `ListSchemaMigrations`, `CancelSchemaMigration`; reload non-terminal jobs on
   leader change. Expose `yb_start_online_schema_change(...)` (row-returning
   function) plus read-only relations `yb_schema_migrations` and
   `yb_schema_migration_progress` with `migration_id` predicate pushdown. First
   `kind` is `ONLINE_TABLE_REWRITE`; the job initially drives a no-op/skeleton
   pipeline so the contract can land before the copy/replay backend.
   Tests: start returns a resolvable uuid; status/progress queryable by
   `migration_id` and by `state`; terminal rows retained; a duplicate
   `request_id` resolves to the same job; master-failover mid-flight resumes and
   re-exposes the same id; ordinary `ALTER TABLE` wire behavior unchanged
   (`PGRES_COMMAND_OK`, JDBC `executeUpdate` still returns 0).

1. **Generation metadata + hidden shadow (Milestone A backend).**
   Add an explicit physical-generation role (`ACTIVE`/`SHADOW`/`RETIRED`) to
   `SysTablesEntryPB`, distinct from `HIDDEN`. Teach `ListTables`, CDC dynamic
   discovery, backup enumeration, and GC to exclude `SHADOW`. Create a shadow
   generation via the existing `YbRelationSetNewRelfileNode`/`CreateTable`
   plumbing with `pg_table_id` set to the source logical id. Test: shadow is
   invisible to SQL/list/backup but addressable by physical id.

2. **Wire the caught-up shadow into the OID-preserving switch (Milestone C).**
   The seam is `tablecmds.c:6390-6409` (`make_new_heap` -> inline
   `ATRewriteTable` copy -> `finish_heap_swap`). Add a `finish`-side path that
   adopts an already-populated shadow generation and SKIPS the inline copy and
   index rebuild. Test: swap a pre-filled shadow into the source OID; dependents
   survive; no data copied during cutover.

3. **Master-owned migration job (Milestones A-C orchestration).**
   A `SysOnlineSchemaChangeJobPB` state machine (states from Section 2.7),
   resumable across master failover, fencing stale-epoch callbacks. Test:
   kill/restart master at each state; job resumes, never double-applies.

4. **Per-tablet resolved-HybridTime barrier (Milestone C, real drain).**
   Replace the prototype sentinel with a per-source-tablet resolved-HT vector:
   cutover proceeds only when every source tablet has resolved through `F` and
   the applier frontier covers `F`. Reuse the index-backfill safe-time
   primitives (`backfill_index.cc`, `tablet_service.cc` safe-time path).

5. **Internal CDCSDK capture (Milestone B backend).**
   Replace the user-facing logical slot with an internal, hard-retention
   CDCSDK stream bound to source physical ids, full row images, transaction
   assembly. Decide: initial internalized-VWAL vs direct per-tablet assembly
   (Open Decision 7).

6. **External CDC handoff (Milestone D).**
   Suppress internal backfill/mirror writes from external CDC; emit one
   generation/schema transition at `K`; retain old tablets until consumers
   cross `K`. Blocks GA.

7. **Backup / PITR generation awareness (Milestone E).**
   Generation selection by snapshot HybridTime; PITR physical coverage of
   shadow generations; incremental-backup replacement manifest or auto-full.
   Blocks GA.

8. **Shapes (Milestones F, G).**
   Partitioned/geo generation groups, then colocated generations. Each is a
   separate investigation with its own routing and retention concerns.

Item 0 lands the user-facing contract and durable substrate first (everything
below reports through it). Items 1-4 make a single non-colocated table migrate
end to end in-tree with a real cutover. Items 5-8 are the GA gates.

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
- Async submission returns a resolvable `migration_id` before any copy work.
- Duplicate `request_id` (lost-response retry) resolves to the same job, not a
  second migration.
- Status/progress relations queryable by `migration_id` and by `state`; point
  lookup does not full-scan all progress rows.
- Terminal (`SUCCEEDED`/`FAILED`/`CANCELLED`) summary rows retained and
  queryable after completion; detail pruned per policy.
- Master-failover mid-migration: job reloads, `state_epoch` bumps, same
  `migration_id` still resolvable, stale-epoch worker callbacks fenced.
- Ordinary DDL wire behavior unchanged: `ALTER TABLE` stays `PGRES_COMMAND_OK`,
  JDBC `executeUpdate(DDL)` returns 0, command tag unchanged.

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
    (standalone needs explicit PITR restore support). Leaning standalone
    (`SCHEMA_MIGRATION` entity) per Section 0.2, matching `SNAPSHOT_RESTORATION`.
13. Status-relation implementation: `migration_id` predicate pushdown via a
    custom/foreign scan or parameterized RPC, vs. a directly queryable internal
    DocDB table with the id as key prefix. A plain view-over-SRF (the
    `yb_tablet_metadata` pattern) is rejected for progress because it
    materializes all rows before filtering.
14. Submission surface: keep async-only `yb_start_online_schema_change(...)`, or
    also provide a blocking `ALTER TABLE ... ONLINE` convenience wrapper that
    polls to terminal state on top of the same async id.
15. Scope of the generic status view: OSC-only now vs. later folding index
    backfill / validation / placement moves into the same `kind`-discriminated
    relations (out of scope for this feature, but the schema must not preclude
    it).
16. Percentage semantics: expose an advisory percent column at all, or only raw
    counters + phase (denominator is unstable under tablet splits and estimates).
17. Retention windows for terminal summary rows and detail rows, and whether
    they are covered by backup/PITR.

# Non-Goals

- xCluster support.
- Trigger-based synchronous mirroring as the core mechanism.
- MySQL-style rename swap that changes the logical table OID.
- Replacing metadata-only or existing online-index paths.
- Changing ordinary DDL to return an id (compatibility-breaking across drivers).
- Folding non-OSC schema changes (index backfill, validation, placement) into
  the status view now; the entity is designed to allow it later, but only
  `ONLINE_TABLE_REWRITE` is implemented for this feature.

# Prototype Artifacts

- `src/yb/yql/pgwrapper/pg_online_schema_change-test.cc` - in-tree C++
  regression test for the OID-preserving storage switch (Milestone C mechanic).
- `prototype/osc-logical-slot/` - SQL/`pg_recvlogical` validation harness:
  - `mirror_harness.sh` (1:1), `mirror_harness_nm.sh` (N:M fan-out + atomicity),
    `mirror_harness_restart.sh` (exactly-once across restart),
    `mirror_harness_barrier.sh` (real barrier drain under a writer).
  - `streaming_applier.py` (per-xid target txn + `(slot,xid)` ledger),
    `apply_changes.py` (transaction-framed batch apply).
  - `README.md` (results), `NEXT-STEPS.md` (in-tree step plan).
