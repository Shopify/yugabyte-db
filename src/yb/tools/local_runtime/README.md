# Local runtime experiments

A standalone real-YSQL developer/test prototype with frozen read-only mode and opt-in
serialized SQL transactions. It also contains a lower-level DocDB executor, a cluster-backed
read compatibility harness, and storage/CPU measurements. It is not a production replacement.

## Standalone read-only boot

`yb-local-readonly` runs real PostgreSQL/PgGate against a local PgClient RPC service.
It starts no master, tserver, Tablet, TabletPeer, or Raft lifecycle. Catalog schemas
and rows come from the matching build's `initial_sys_catalog_snapshot`, opened
read-only; catalog reads execute native `PgsqlReadOperation` against that store.

The default mode serves only frozen built-in catalogs; it cannot create application
tables or indexes. The experimental write mode below has a separate storage format.
SQL functions, catalog queries, and catalog paging work. Writes, row locks, strong
object locks, historical reads, and unimplemented RPCs fail explicitly. Disabling
PostgreSQL's read-only setting does not enable backend writes.

```sh
./yb_build.sh release --clang21 --target yb-local-readonly --force-run-cmake \
  --sj --skip-pg-parquet --no-odyssey --no-ybc --no-postgres --skip-initdb

build/latest/bin/yb-local-readonly \
  --data_dir "$PWD/build/local-runtime/readonly-dev" --sql_port 15433
```

Use a new directory initially. Startup emits a one-line JSON readiness record on stdout
after SQL verification; process logs go to stderr. The record has `protocol_version`
(currently 1), `state` (`ready`), `host`, `sql_port`, `rpc_address`, `mode` (`readonly`
or `experimental-serialized-sql`), `pid` (the CLI process to signal for shutdown), and
`data_directory` (the PostgreSQL data directory, matching `SHOW data_directory`).
In another terminal:

```sh
build/latest/postgres/bin/ysqlsh -X -h 127.0.0.1 -p 15433 -U yugabyte -d yugabyte \
  -c "SELECT yb_hash_code(42::bigint), (SELECT count(*) FROM pg_class)"
```

Stop with SIGINT/SIGTERM; the owned PostgreSQL processes are stopped before the
RPC service. Restart with the same arguments. The directory is locked and pinned
to the engine version and a SHA-256 fingerprint of the seed. Unowned directories,
symlinked markers/locks, or mismatched seeds are rejected. Startup does not kill
an existing port owner, and readiness verifies the connected postmaster's ownership.
Do not copy runtime directories: their PostgreSQL data link may still reference the
source. Startup rejects foreign/dangling data links and symlinked versioned PostgreSQL
directories before initdb or config generation. Use a fresh directory; a coordinated
cloning workflow is not implemented.

Run one instance per process, on a distinct loopback SQL port. This is trusted-user
development tooling, not an authenticated RPC service or filesystem sandbox. There
is no child-process supervisor or standalone crash-recovery claim yet. Do not change
the bundled seed or binaries while an instance runs. ASH, autovacuum, DocDB statement
metrics, and the tserver-spawned relcache helper are disabled; the SQL backend builds
its own relcache. Catalog versions are loaded from the seed, not fabricated.

```sh
for test in BootsWithoutCluster RestartsAndOwnsDirectory RejectsWritesAtRpcBoundary \
            CommandLifecycle RejectsForeignPostgresDirectory; do
  ./yb_build.sh release --clang21 --cxx-test pg_local_readonly-test \
    --gtest_filter ReadonlyPgServerTest.$test --sj --skip-pg-parquet --no-odyssey --no-ybc \
    --no-postgres --skip-initdb || exit 1
done
```

These tests cover SQL/hash-function evaluation, catalog reads/paging, clean restart,
unchanged seed bytes, ownership checks, direct-RPC write rejection, and actual CLI
readiness/port-collision/shutdown behavior. The experimental-mode tests below cover
application tables. Neither suite establishes production transaction/plan parity or
the 1,000-table resource/provisioning goals.

## Experimental serialized SQL

Add --experimental_catalog_writes using a NEW dedicated runtime directory. Readiness
reports mode experimental-serialized-sql. Frozen mode remains the default:

```sh
build/latest/bin/yb-local-readonly --experimental_catalog_writes \
  --data_dir "$PWD/build/local-runtime/sql-dev" --sql_port 15433
```

```sql
CREATE TABLE events(h bigint, r bigint, v bigint, PRIMARY KEY(h HASH, r ASC))
  SPLIT INTO 4 TABLETS;
INSERT INTO events VALUES (1, 2, 3), (4, 5, 6);
BEGIN;
UPDATE events SET v = v + 1 WHERE h = 1 AND r = 2;
SELECT * FROM events WHERE yb_hash_code(h) >= 0 AND yb_hash_code(h) < 32768;
ROLLBACK;
```

### SQL contract

Backends run SQL transactions concurrently. Each data transaction is an independent native
transaction; conflicting row and unique-index writes go through native DocDB conflict
resolution and fail with a serialization error instead of waiting. Reads and conflict checks
use the backend's read point, chosen like PgClientSession: a new read_time_serial_no picks the
current clock, so REPEATABLE READ keeps one snapshot and READ COMMITTED gets one per statement.
SERIALIZABLE is rejected rather than weakened. There is no wait queue or deadlock detection.

Catalog changes, catalog row locks, and object locks stronger than ROW EXCLUSIVE take one
exclusive lease until COMMIT/ROLLBACK or backend retirement. It is granted only while no other
backend has an open SQL transaction, and other backends' SQL fails with a serialization error
while it is held. A backend's open data transaction is adopted into its catalog transaction, so
both still publish in one WAL batch. Parallel workers and session object locks are
disabled/rejected.

Application row locks (FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE, FOR KEY SHARE) follow the
tserver path: lock intents from docdb::GetIntents are conflict-checked and persisted before the
read. NOWAIT and the default wait policy fail on conflict; SKIP LOCKED skips the row. Lock-only
intents are removed when their transaction finishes, like any other intent.

Finished transactions are reclaimed the way a tablet does it: committed intents are applied to
regular rows at the commit time (skipping row locks and rolled-back savepoints), then all of the
transaction's intents and its status record are removed. Aborts are not synced, since recovery
treats a pending record as aborted. A transaction aborted by a conflicting one stays visible as
aborted until its owner finishes it. Recovery finds each leftover transaction's tables from the
intent reverse index and reclaims them before serving.

PgGate explicitly reports whether a SQL transaction is active, distinguishing backend
startup reads from SQL transaction lifetime. Native table locking is enabled and its
shared-memory fast path disabled, so even read-only SQL transactions send FinishTransaction.
Metadata RPCs carry the backend ID: another backend, or a caller without an ID, cannot
read private DDL descriptors. Catalog shared-memory versions publish only after commit;
missing incremental invalidations cause native cache refresh. Catalog reads outside the
catalog transaction may use any time from the last catalog commit to the clock; data commits do
not change catalog rows. Do not admit catalog work while another backend has an open SQL
transaction, even a read-only one without native state.

Supported SQL includes native scalar encodings for smallint/integer/bigint, boolean,
real/double precision, numeric, UUID, date/time/timestamps, text/varchar, bytea, and JSON/JSONB;
INSERT/UPDATE/DELETE; transactions/savepoints; namespaces, functions, casts, and bundled
unaccent extension/dictionary operations. HASH/composite HASH, HASH+RANGE, and single-partition
RANGE-only tables preserve native schemas, keys, partition maps, and non-colocated properties.
HASH defaults to four logical tablets. No physical Tablet, TabletPeer, or Raft instance is
created. Backward scans support one hash code or a single-partition relation, including paging.
Reads without partition bounds route a scalar ybctid by its DocKey hash; unbounded
ybctid batches are rejected on multi-partition relations. PgGate supplies bounds for both.

Sequences, serial/bigserial, and identity columns use native nontransactional sequence values.
Every tuple change, including each connection's cached-range reservation, is synced before it
is acknowledged: nextval/setval survive ROLLBACK and savepoint rollback, and restart does not
reissue reserved values. Caches are connection-local, so values interleave across connections
and rollback, rejected inserts, and restart leave gaps. Sequence RPCs neither start a SQL
transaction nor take the lease, but fail while another backend holds it. The exception is
DROP SEQUENCE/owning-table tuple deletion: PostgreSQL sends it after FinishTransaction released
the lease, so it is admitted under any lease, still only for the backend's own database. DROP
DATABASE cleanup deletes only a durably retired database's tuples. PostgreSQL merely warns when
post-commit deletion fails. Tuples from a failed or rolled-back CREATE, or from missed
post-commit cleanup, are unreachable (OIDs are not reused) but persist until their database is
retired or the directory is recreated. Server-side sequence caching
(ysql_sequence_cache_method=server), historical sequence reads, sequences in template
databases, and TRUNCATE ... RESTART IDENTITY remain unsupported. A rolled-back ALTER that
widened bounds and RESTARTed can leave an uncalled value outside the reverted
MINVALUE/MAXVALUE; nextval then fails before changing state, and an in-range setval recovers.

Foreground non-unique, unique, covering, partial, and expression indexes use native
PostgreSQL/PgGate population and maintenance. Base/index intents share a transaction;
unique NULL-distinct/NULLS NOT DISTINCT encodings are unchanged. Explicit CONCURRENTLY,
online backfill, and nested application index reads remain unsupported.

ALTER supports nonkey column addition/removal/rename, table/schema rename, defaults,
nullability and CHECK catalog changes, and native transactional relation rewrites such as
primary-key replacement. Old packing layouts remain available for existing rows/intents.
Persistent column-ID high-watermarks prevent DROP/ADD from reinterpreting old cells.
Savepoint rollback restores descriptors and both catalog/data mutations. Direct key-layout
mutation and replica-identity changes are rejected; rewrites create new storage identities.

Table and index DROP remove reciprocal descriptors atomically. Published CFs survive an
uncommitted DROP and its savepoints; reclamation follows acknowledged publication.
CREATE DATABASE copies nonshared native catalogs from template0/template1, with the real
name/OID, OID reservation bound, SQL pg_database/dependency/version tuples, and descriptor
bundle in one transaction. Templates containing application tables are unsupported.
DROP DATABASE accepts only locally created databases without live target backends. It
atomically retires their namespace and application descriptors with the SQL catalog rows.
Historical catalog keys and sequence tuples are then deleted in <=256 KiB synced batches; a
retirement marker restarts interrupted cleanup. Retired database OIDs cannot be reused.
Catalog shared-memory slots are not renumbered under live backends; dropped slots are cleared
and retained until restart. Database rename and arbitrary database cloning remain unsupported.

Colocation, tablegroups/tablespaces, explicit split bounds, cross-partition backward scans,
replay/retry, general historical reads, and unimplemented RPCs fail explicitly.
This is not a general transaction/planner compatibility claim. Do not use it to edit arbitrary
system catalogs.

### Storage and lifecycle

One RocksDB holds catalogs in its default CF, descriptors/counters/sequence tuples in
local-metadata, regular application data in table/<id>, intents in intents/<id>, and native
transaction status/bindings in local-native-transactions. All participants share the cache, CF
ownership, mutex, allocated clock, committed watermark, and persistent I/O failure fence.

Catalog tuples/descriptors use a private WriteBatchWithIndex overlay. Application reads and
writes use native DocDB operations, intents, and status resolution through PgLocalTxnStore,
not repeated autocommit calls. PgCatalogTransaction coordinates one synchronous WAL batch
containing catalog changes, native committed status, and clocks. WAL sync covers the earlier
streamed intents. Catalog timestamp suffixes are rebased at publication; native intents
resolve through the jointly committed status. Shared-memory versions advance afterward.

First native access durably binds a table to intent-aware execution. Legacy application
reads/writes cannot bypass a binding. Bindings on existing tables survive abort; bindings
on rolled-back CREATEs are removed. Missing published storage is corruption, not permission
to recreate empty CFs. Cleanup releases intent views and drops intent families before regular
families. Startup and rollback validate the descriptor graph; healthy CREATE refreshes only
changed descriptors.

OID allocation reserves 256 IDs per synced write; rollback/restart leaves gaps, not reusable
identities. Native statement own-write limits remain distinct from the committed snapshot.
Joined transactions permit writes to live parent subtransactions, including ID 1 for PgGate's
commit-time catalog updates; aborted IDs remain rejected.

Limits are 16 MiB for the encoded catalog overlay/final commit batch and for an encoded
intent batch plus pending catalog bytes, with additional arenas/overlay/rollback overhead.
Attached SQL transactions may stream up to 512 MiB of cumulative encoded intents. These are
not all buffered in the catalog overlay. There are 1,024 rollback ranges and 65,536 unfinished
native transactions. History GC is disabled, so committed versions accumulate. Intents and
metadata families use small write buffers (--local_runtime_churn_write_buffer_bytes), and the
largest intents families are flushed once unflushed intents exceed
--local_runtime_intents_memtable_budget_bytes: removed intents otherwise stay in memtables beside
their tombstones. Past --local_runtime_max_wal_bytes of WAL, every family holding the oldest log is
flushed so it can be deleted. All families use universal compaction without a filter, which merges
files and keeps every version, and SSTs carry no bloom filter: forced flushes write a small file
per family, which a fixed 64 KiB filter block would dwarf. Synced writes are commits, catalog
changes, sequence state and table creation; aborts and savepoint rollbacks are not synced.

Each backend owns its request-sequence watermark, queued calls, deadline timer, and error state.
Sequencing has no lifetime request-count cap. Out-of-order arrivals park without blocking
RPC workers; the 64-call / 16 MiB encoded queue bound is service-wide. Missing predecessors
must arrive within five seconds or the RPC deadline. Replay, timeout, overflow, or backend
death cancels only that session; an unrelated abort cannot cancel the current owner's work.
Shutdown drains retained contexts before messenger teardown.

Catalog-only reads remain available after logical SQL errors for constraint-name/SQLSTATE
reporting; application operations require rollback. Ambiguous durability errors and failed
catalog-cache rebuilds fence all access until reopen. A failed acknowledgement is not proof
of abort. There is no commit replay protocol. Storage-worker SIGKILL tests are not whole-runtime
or power-loss tests, and the CLI still has no child-process supervisor.

Write mode explicitly enables the native DDL/savepoint/table-lock, packed-row V2,
missing-column-default and relation-rewrite prerequisites normally promoted in a new universe.
It disables new-relation intent bypass, online index backfill, parallel workers, and connection
manager statistics. The actual CLI cannot depend on test-only AutoFlag promotion or a nonexistent
connection-manager shared-memory segment.

### Tests and compatibility

Run each C++ case separately using yb_build.sh. In addition to the original catalog, scalar,
index, hash-range, paging, queue, error, ownership, and restart tests, targeted SQL gates are:

```sh
for test in ConcurrentSqlSessions SchemaLifecycle PrimaryKeyRewrite LargeNativeTransaction \
            DropsDatabasesAndRetainsOtherSlots CommandCatalogTransactions \
            SequenceBackedSchemaLifecycle SequenceValuesAreNontransactional \
            BoundedSequenceLimits SequenceTuplesDeletedOnDrop SequenceRpcGuards \
            SequenceRestartRollbackBounds; do
  ./yb_build.sh release --clang21 --cxx-test pg_local_readonly-test \
    --gtest_filter ReadonlyPgServerTest.$test --sj --skip-pg-parquet --no-odyssey --no-ybc \
    --no-postgres --skip-initdb || exit 1
done
```

Upgrade/rollback: write mode uses ownership marker yb-local-catalog-txn-v10, which adds durable
sequence tuples. Recreate v1-v9 SQL directories; never edit markers to bypass engine/seed checks.
Frozen, production-tablet, and mismatched-build directories cannot be reused. The exact build
identity includes its build timestamp. There is no experimental data-format upgrade promise.

The production protocol gains additive backend-ID fields on OpenTable, GetTablePartitionList,
and GetDatabaseInfo, plus sql_transaction_active on Perform options. Ordinary tservers ignore
these fields; production defaults are unchanged. Older peers can ignore the fields, but this
local runtime requires the matching rebuilt PgGate for writable SQL and private metadata.

## Shared catalog/native-intent storage

PgCatalogTransaction::BeginWithNative acquires catalog ownership and returns a joined native
transaction ID. Commit/abort/savepoint operations must go through the catalog coordinator;
calling the joined participant's lifecycle methods directly is rejected. Active independent
native snapshots exclude catalog DDL.

The standalone PgLocalTxnStore PB API remains limited to immutable version-zero HASH schemas
using INT64/INT32/STRING. It allows overlapping snapshot transactions with native fail-on-conflict
arbitration, at most 64 active transactions, and 16 MiB cumulative intents per transaction.
The attached SQL API loads authoritative schemas, index contexts, and old row packings instead.
Committed intents are applied and removed after the durable commit. Intent SST filtering is
disabled because this store has no Raft hybrid-time frontiers.

pg_local_shared_commit-test covers atomic catalog/data publication, snapshots/DDL exclusion,
parent/savepoint writes, native SQL types/sidecars, schema packing and column-ID recovery,
table/database retirement, bounded cleanup and SIGKILL, missing storage, and shared failure
fences. SequenceStateSurvivesRollbackAndReopen and SequenceRetirementAndWriteFailureFence cover
sequence tuples across abort/reopen, database retirement, and ambiguous write failure.
pg_local_txn_store-test retains its standalone conflict/snapshot/paging/concurrent-caller
coverage and optional CPU/RSS probe. These component tests do not establish concurrent writable
SQL or power-loss durability.

Standalone native format is local-native-intents/2 (v1 requires recreation); the shared SQL
storage format remains local-shared-sql/1 inside the v10 runtime ownership boundary.

## Integrated standalone measurement

```sh
python3 src/yb/tools/local_runtime/measure_local.py NEW_OUTPUT_DIRECTORY \
  --tables 1000 --batch 100 --idle 60 --settle 15 > NEW_RESULTS.jsonl
```

The runner launches the actual CLI, creates 1,000 HASH tables plus one non-unique HASH
index per table, and validates catalog readiness and non-colocated one-tablet topology.
It then inserts four 64-byte-payload rows per table, checks indexed lookups, samples
idle CPU, cleanly restarts, and revalidates data/indexes. Cold schema timing includes
startup and validation but excludes fixture insertion, idle windows, and restart.
SQL sessions run sequentially; the batch parameter controls connection reuse. The
recorded measurement below predates removal of the lifetime request cap.

CPU accounting includes PostgreSQL descendants and reaped backends, excludes SQL
clients/sampler, and fails on incomplete phase boundaries. RSS is a process-tree sum
and double-counts shared pages. Readiness, SQL, and CPU traces stay in the new output
directory. No SQL or filesystem surveys run inside idle windows. Do not rebuild
binaries or run other workloads during measurement. A 30-second budget miss is a
reported false result, not a validation success or a benchmark-process failure.

This is a sparse, single-backend workload with foreground index builds and no ALTER,
ANALYZE, concurrent clients, or full-runtime crash recovery. Ordinary-YSQL baseline
settings differ (including ASH and online index build), so memory/CPU deltas are not
storage-only savings or proof of production equivalence.

One observed run on the Apple M4 Pro / 48 GiB / macOS / release-Clang 21 machine used
for the earlier baselines, with 100 statements per SQL session:

| Phase | Wall seconds | Server CPU-seconds |
|---|---:|---:|
| Create 1,000 tables | 16.79 | 14.26 |
| Create 1,000 indexes | 6.36 | 5.46 |
| Validate schema/topology | 2.59 | 2.04 |
| Insert four rows/table | 3.09 | 2.53 |
| Validate rows/indexed lookups | 3.49 | 2.87 |

Schema application plus validation took **25.95s**; cold process start through usable,
validated schema took **27.73s**, below 30 seconds in this run. This is a fresh runtime
directory, not a cold OS page-cache test or a guarantee for arbitrary schemas/migrations.

| State | Process-tree RSS sum (MiB) | Idle CPU (% of one core) |
|---|---:|---:|
| Empty | 181.7 | not sampled |
| Populated, settled | 275.2 | 0.053 |
| Restarted and verified | 288.0 | 0.047 |

Restart-to-SQL readiness was **1.86s**; subsequent schema/data/index validation took
7.13s. Both idle windows lasted 60 seconds after 15 seconds settling, without SQL.
Peak sampled RSS across measured work phases was 314.4 MiB (not a startup peak).
A few transient samples during active work were incomplete; phase boundaries and idle
windows were complete, with no CPU counter regressions. Sampled peaks remain approximate.

The shared store uses a 128 MiB manifest rollover limit: at 2,000 CFs, a recovery
snapshot exceeded the production per-tablet 10 MiB limit and otherwise rolled on every
CF recovery flush. This is local-store configuration, not a production default change.

Artifacts: /tmp/claude/yb-local-indexes/final-1000-b100.jsonl and its sibling directory.
The earlier integrated-1000 attempt missed the budget and timed out during restart;
verified-1000 passed recovery but missed 30 seconds. Neither is the final result.


## Request-execution component

`pg_local_store.{h,cc}` executes selected `PgsqlReadRequestPB` and
`PgsqlWriteRequestPB` messages using `PgsqlReadOperation` / `PgsqlWriteOperation`.
It returns PgGate-format row buffers. The compatibility harness below connects its
read path to real YSQL; the component itself has no standalone SQL endpoint.

- One RocksDB instance, one column family per table, shared block cache. Original
  DocDB keys are not prefixed or rewritten, and schemas are not marked colocated.
- Canonical YSQL hash encoding, HASH plus RANGE keys, and logical partition bounds
  remain distinct from the physical store. Logical tablets have no peers or pollers.
- Single-row autocommit insert/update/delete and explicit-time MVCC reads. Operations
  are serialized; this is not a SQL transaction implementation. A multi-tablet read
  must reuse one captured timestamp. There is no request retry/deduplication protocol.
- Schema/partition metadata and commit times persist. Row changes and the commit
  clock share a synchronous WAL batch. Recovery completes a table create interrupted
  after schema publication. Any durability/CF failure stops the whole store, including
  unrelated tables, until reopen.
- Request fields outside the supported subset fail explicitly. No secondary-index
  maintenance, SQL expressions, paging, row locks, savepoints, or schema alteration.

Use a dedicated directory; these experimental stores are not production tablet
directories and have no format-upgrade guarantee. Compaction/history GC are disabled.
Per-table column families still have overhead: no 1,000-table resource or DDL budget
has been demonstrated for this component.

Run each C++ case separately (use the checkout's compiler/build type):

```sh
for test in HashRangesSnapshotsAndRestart RejectsUnsupportedAndInvalidRequests \
            PointCompositeAndDuplicateKeys DocKeyScanBounds \
            SigkillRecoveryWithInterruptedCreate AmbiguousWriteFailureRemainsFenced; do
  ./yb_build.sh release --clang21 --cxx-test pg_local_store-test \
    --gtest_filter PgLocalStoreTest.$test --sj --skip-pg-parquet --no-odyssey --no-ybc \
    --no-postgres --skip-initdb || exit 1
done
```

The tests cover scalar/composite hashes, disjoint interval scans, namespace isolation,
point reads, DocKey bounds, duplicate keys, snapshots across updates/deletes, clean reopen, and
SIGKILL with acknowledged rows plus an interrupted table create. SIGKILL is not a
power-loss test. Hash oracle values come from ordinary YSQL on this checkout.

`hash_scan_contract.sql` is a separate SQL compatibility test. It uses ordinary HASH
tables with four logical tablets and checks `[0,65536)` in half-open intervals of
width 257 against materialized full-scan references, including hashes 0 and 65535.
Run it on a disposable ordinary YSQL database. StandaloneHashRanges separately covers
base-table and secondary-index iteration on the local runtime:

```sh
build/latest/postgres/bin/ysqlsh -X -h HOST -p PORT -U yugabyte -d DATABASE \
  -f src/yb/tools/local_runtime/hash_scan_contract.sql
```

Do not map a multi-row SQL statement to repeated autocommit calls: statement atomicity
and retry identities need a coordinator before exposing SQL writes.

This component is intentionally separate from the standalone transaction engine.
Its tests do not establish planner parity, a complete local database, or resource/DDL
budgets.

## Real-YSQL read compatibility harness

`pg_local_ysql-test.cc` starts an ordinary RF1 mini-cluster for the SQL frontend,
catalogs, and table metadata. Application reads for two synthetic tables are served
by `PgLocalStore`; this is a test harness, not the deployment architecture.
It uses a typed post-sequencing hook under the existing `TEST_enable_pg_client_mock` flag.

The native tables contain only a sentinel row. The local store contains a different
dataset, including hash witnesses at 0 and 65535. SQL scans must return exactly the
local dataset, so falling through to native storage cannot accidentally pass.

Both TCP RPC and shared-memory tests exercise:

- Actual PgGate requests, native table schemas and four-tablet partition maps.
- Scalar/composite `yb_hash_code` intervals covering `[0,65536)` in steps of 257.
- Cross-tablet continuations, empty intervals, and reads after reopening the store.
- Explicit rejection of SQL writes, mixed batches, and within-tablet pagination without fallback.
- Subsequent native-table reads and hook removal, guarding against request-sequence gaps.
- Existing before/after mock callbacks alongside the selective hook.

The fixture uses bigint columns and a frozen catalog/snapshot. It verifies column
metadata before adapting modern references to the executor's reference format.
DocKey bounds pass through unchanged. DocDB metrics collection is disabled for this
fixture rather than returning fabricated metrics. It does not test SQL transactions,
secondary indexes, catalog invalidation, arbitrary types, or query-plan parity.

```sh
for suite in PgLocalYsqlRpcTest PgLocalYsqlSharedMemoryTest; do
  ./yb_build.sh release --clang21 --cxx-test pg_local_ysql-test \
    --gtest_filter $suite.HashScanContract --sj --skip-pg-parquet --no-odyssey --no-ybc \
    --no-postgres --skip-initdb || exit 1
done
```

PgLocalIndexMetadataTest.IndexDescriptorParity separately captures ordinary YSQL
foreground index requests and compares native/local schemas, column IDs, and logical
partition maps for non-unique, unique, covering, and RANGE indexes. This is descriptor
parity, not optimizer-plan parity:

```sh
./yb_build.sh release --clang21 --cxx-test pg_local_ysql-test \
  --gtest_filter PgLocalIndexMetadataTest.IndexDescriptorParity \
  --sj --skip-pg-parquet --no-odyssey --no-ybc --no-postgres --skip-initdb
```

These tests require the existing YSQL binaries and initial catalog snapshot. The
normal test harness owns and shuts down its processes. Keep the lower-level tests
as well: they run without a cluster and cover persistence and SIGKILL recovery.

## Build

Use the compiler/build type of the existing checkout. For example:

```sh
./yb_build.sh release --clang21 --target yb-local-storage-probe --force-run-cmake \
  --sj --skip-pg-parquet --no-odyssey --no-ybc
```

The YSQL baseline also requires a consistent `daemons initdb` build. Do not run
experiments while rebuilding their binaries or shared libraries.

## Storage-only comparison

```sh
mkdir -p /tmp/claude/local-measurements
python3 src/yb/tools/local_runtime/measure.py \
  --output /tmp/claude/local-measurements/storage \
  storage --tablets 2000 --stores 2000 1 \
  > /tmp/claude/local-measurements/storage.jsonl
```

Two thousand logical namespaces model the storage count of 1,000 tables with one
secondary index and one tablet per relation. The layouts have identical keys,
values, DocDB RocksDB options, and a shared 64 MiB block cache. Each physical store
is a regular/intents DB pair. Four records are written to every regular namespace;
intents stores remain empty. Compaction is disabled in both layouts.

The native probe uses existing DocDB key encodings, including a storage namespace
prefix and hash components. It does not change SQL schemas or implement a transparent
non-colocated-table adapter. It verifies all point reads, strict iterator ordering,
and the total record count, then flushes and reopens in a fresh process.

Each layout runs in its own process, avoiding allocator high-water marks carried
over from the other layout. JSON lines report RSS, peak RSS, allocator bytes, CPU
time, open file descriptors, and cumulative elapsed time at each phase. Compare
`written` and `opened` against that process's `baseline`. `reopening` distinguishes
the second process. File counts, apparent sizes, and allocated disk bytes are
reported separately.

Add `storage --idle-seconds 60` to measure idle CPU after flush and after reopen.
`storage_idle_cpu` reports CPU-seconds and average core utilization. The native
probe measures its own threads with `getrusage`; it has no server process tree.

To check recovery after SIGKILL, before the explicit flush or normal close:

```sh
python3 src/yb/tools/local_runtime/measure.py \
  --output /tmp/claude/local-measurements/crash \
  storage --tablets 2000 --stores 2000 1 --crash \
  > /tmp/claude/local-measurements/crash.jsonl
```

Both layouts enable RocksDB WAL and synchronize each namespace's write batch.
Production tablets instead use the Raft WAL. To separate DB overhead from RocksDB
WAL overhead, repeat the clean-restart comparison with `storage --no-wal`. This
mode explicitly flushes before close and cannot be combined with `--crash`.
SIGKILL checks process-crash recovery, not power-loss durability.

For a small smoke check, use `storage --tablets 8 --stores 8 1 --crash`.
The probe never deletes its data and refuses to create over an existing directory.

## YSQL CPU and schema latency

```sh
python3 src/yb/tools/local_runtime/measure.py \
  --output /tmp/claude/local-measurements/ysql \
  ysql --tables 1000 --batch 1000 --ddl-workers 8 \
  --idle-seconds 60 --settle-seconds 15 \
  > /tmp/claude/local-measurements/ysql.jsonl
```

Use `ysql --tables 0` in a separate run for the empty-instance idle control.
`--ddl-workers` defaults to one. Only independent synthetic statements are
parallelized; this is not permission to reorder an application's migrations.
`--batch` controls how many statements each group of connections handles before
closing. Its default is 1,000, avoiding connection churn during a phase.

The runner owns an RF1 master/tserver pair on ephemeral loopback ports. The
phases are startup, namespace/table creation, index creation, catalog validation,
fixture insertion, nullable ADD COLUMN, migration validation, idle, restart,
recovery verification, and another idle window. All tables and secondary indexes
are HASH partitioned with one tablet each; there is no SQL colocation. Each table
gets four rows. `--alter-tables` defaults to every table; zero skips the migration.
Catalog/index validity, added columns, and row counts are checked after restart.

Each workload emits wall seconds, server CPU-seconds, average cores/CPU percent,
sampled peak cores, and peak RSS sum. Per-statement client timings from `ysqlsh`
provide p50/p95/max latency; their sum is not elapsed time when workers overlap.
Raw SQL, client timing output, and per-process `cpu-<workload>.jsonl` traces are kept.

CPU includes master, tserver, and PostgreSQL descendants, including CPU of children
that exited between samples. Linux `/proc` and macOS `proc_pid_rusage` supply
self/reaped-child counters; macOS Mach ticks are converted using the host timebase.
The sampler and SQL client processes are excluded. 100% means one fully occupied
core, not the entire machine. Samples are not atomic: incomplete samples and
counter regressions are reported, and incomplete phase boundaries fail the run.
Sampled peaks are approximate and can be null when no adjacent complete samples exist.

Idle windows start after `--settle-seconds` (default 15). No SQL, metrics scraping,
or directory scanning runs during the window. `--cpu-interval` defaults to a
0.5-second delay between samples. RSS sums double-count shared pages; use allocator
gauges as a separate measurement, not as a substitute for physical-memory accounting.

The default schema budget is strictly less than 30 seconds, configurable with
`--schema-budget-seconds`. `schema_budget` reports both schema application on a
ready server and actual cold start through usable, validated tables/indexes.
`migration_budget` times ADD COLUMN plus validation separately. Fixture insertion
and deliberate idle-measurement windows are not included in either DDL budget.
A budget miss is recorded as false, not a failed experiment exit status.

Automatic splitting, YCQL/Redis listeners, and call-home telemetry are disabled.
Limits are 1 GiB for master, 4 GiB for tserver, and 20 SQL connections. All daemon
arguments are logged. These are ceilings, not preallocations. No lease or catalog
safety checks are disabled to improve timings.

Output directories must not exist. Daemons are stopped on completion, errors,
SIGINT, or SIGTERM. Logs and data remain available for inspection. Port selection
has a bind/release race; a conflicting bind fails the run rather than attaching
to another instance.

## What the comparison does not establish

The storage probe omits SQL/catalog caches, tablet peers, transaction participants,
active intents, per-tablet RocksDB statistics and retention policies, contention,
and production maintenance. Its memory saving is not an end-to-end emulator saving.
The YSQL run is a separate baseline, not the same workload running on a completed
shared-storage runtime.

Plan parity, concurrent transactions, namespace-scoped truncate/drop, full-runtime
recovery, and integrated resource measurements remain work beyond these baselines.

## Initial memory measurements

One exploratory run on Apple M4 Pro, 48 GiB RAM, macOS 26.5.2, release/Clang 21,
gperftools TCMalloc, at `d73aa205b83` plus this probe. These are observations, not
performance guarantees. Values below are after reopening and verifying every record.

| Storage-only layout | RSS (MiB) | Allocated heap (MiB) | Open FDs |
|---|---:|---:|---:|
| 2,000 DB pairs, WAL enabled | 365.5 | 321.9 | 20,004 |
| 1 shared pair, WAL enabled | 37.9 | 2.0 | 14 |
| 2,000 DB pairs, no WAL | 283.9 | 240.6 | 12,004 |
| 1 shared pair, no WAL | 37.7 | 1.9 | 10 |

The separate/shared layouts passed clean-restart verification with and without WAL.
Both WAL-enabled layouts also passed SIGKILL/reopen verification at 2,000 namespaces.

The initial YSQL memory run interleaved table/index creation and fixture insertion
in 100-table batches: 1,000 tables, 1,000 indexes, four rows per table, eight workers,
and a 15-second idle interval, without ADD COLUMN. Row counts survived restart.
Its live-process CPU sums omitted exited backends; use the CPU experiment below
for work-rate comparisons.

| YSQL phase | Process RSS sum (MiB) | Master allocated heap (MiB) | TServer allocated heap (MiB) |
|---|---:|---:|---:|
| Before application schema | 544.3 | 38.5 | 24.3 |
| Populated, then idle | 1,493.2 | 77.2 | 622.7 |
| Restarted, read all tables, then idle | 1,489.1 | 68.8 | 586.7 |

Restart-to-SQL-readiness took 6.35 seconds; that does not include reading all tables.
The PostgreSQL parent remained near 349 MiB RSS. Inspection found
`yb_ash_circular_buffer_size=262144` KiB: a 256 MiB history buffer unrelated to the
choice of storage engine. Neither it nor `shared_buffers` was reduced for this run.

The shared-store result justifies an integration spike, not a claim of a 38 MiB
YSQL runtime. Consolidation saves hundreds of MiB in this sparse workload; tablet
lifecycle, catalog state, and SQL-side budgets still need separate treatment.

Raw JSON, logs, and data from this run are under `/tmp/claude/yb-local-spike/`:
`storage-2000`, `storage-2000-no-wal`, `storage-2000-crash`, and `ysql-1000`, with
matching `.jsonl` files. An unrelated macOS build issue required aligning the
PostgreSQL `PG_SYSROOT` make variable with CMake's SDK; no build-system source
changes were needed.

## CPU and DDL observations

A subsequent run on the same hardware/build used the phased workload above with
eight workers, batch size 1,000, and ADD COLUMN on all 1,000 populated tables.
Values include server CPU consumed by exited SQL backends.

| Workload | Wall seconds | Server CPU-seconds | Average cores |
|---|---:|---:|---:|
| Create 1,000 tables | 14.68 | 78.68 | 5.37 |
| Create 1,000 secondary indexes | 64.23 | 280.79 | 4.37 |
| Seed four rows per table | 1.56 | 10.96 | 7.21 |
| ADD COLUMN to 1,000 populated tables | 19.18 | 66.21 | 3.46 |

Full schema application including catalog validation took 79.36 seconds; cold
start through validated schema took 85.82 seconds. Both miss the 30-second goal.
The ADD COLUMN migration plus validation took 19.44 seconds and met it. This is
a nullable, non-rewriting change on tiny tables, not a bound for arbitrary DDL.
Schema creation consumed about 360 CPU-seconds: more concurrency alone is not
a lightweight solution to the wall-clock target.

| Idle workload | Window seconds | CPU-seconds | Average CPU, one-core basis |
|---|---:|---:|---:|
| Empty YSQL instance | 60.06 | 1.118 | 1.86% |
| 1,000 tables + 1,000 indexes, after migration | 60.04 | 17.400 | 28.98% |
| Same schema, restarted and verified | 60.06 | 18.647 | 31.05% |
| Bare RocksDB, 2,000 DB pairs, reopened | 60.01 | 0.001435 | 0.00239% |
| Bare RocksDB, one shared pair, reopened | 60.00 | 0.000048 | 0.00008% |

The tserver contributed 26.31 percentage points of the 28.98% loaded-idle CPU;
master contributed 2.61. Bare RocksDB lacks tablet/transaction maintenance and has
compaction disabled, so its idle result does not predict an integrated runtime's
CPU. It does show why replacing the key-value engine alone is not sufficient evidence
of an idle-CPU improvement.

A separate 10-second CPU profile (271 samples, outside the measurement windows)
pointed to transaction-participant polling through `HybridClock::Now*` and macOS
Mach calendar-clock IPC, plus `MaintenanceManager`/`LogGCOp` bookkeeping. The clock
path in `src/yb/util/env_posix.cc` is a platform-specific optimization lead; this
sample is not a portable attribution percentage. The symbolized profile is saved
as `/tmp/claude/yb-local-cpu/idle-tserver-profile.txt`.

A two-table cold-start check also found roughly 25 seconds waiting after index
backfill had completed. Logs identified `WaitForYsqlBackendsCatalogVersion` retries
against `CheckLeadership` in `src/yb/master/ysql_backends_manager.cc`. That guard
waits a full lease period after election; the operation-lease default in
`src/yb/master/object_lock_info_manager.cc` is 30 seconds.

The local-runtime latency work therefore needs two separate changes:

- Eliminate per-logical-tablet polling/replication work while retaining transaction
  correctness; sharing physical stores alone is not the whole CPU design.
- Replace distributed catalog/lease waits with local epoch fencing and acknowledged
  invalidation. Do not simply shorten production leases or skip index visibility checks.

For worktrees and repeated CI jobs, also measure provisioning from a consistent,
versioned schema/fixture seed plus pending migrations. That avoids replaying an
unchanged schema; it must be reported separately from making DDL execution faster.
A cache key must include engine compatibility and the ordered schema/migration inputs.

The remaining fresh-load target is 1,000 HASH tables plus 1,000 usable indexes in
under 30 seconds, including startup and validation, without schema rewrites. Large
data backfills and serial dependency chains require their own workload budgets.

CPU traces, SQL timings, logs, and data are in `/tmp/claude/yb-local-cpu/` under
`empty`, `ysql-1000`, and `storage-2000`, with matching top-level `.jsonl` files.

## CPU accounting checks

```sh
PYTEST_ADDOPTS='src/yb/tools/local_runtime/test_cpu_usage.py -k TestLocalRuntimeCpu -q' \
  ./yb_build.sh release --clang21 --python-tests --sj
```

The tests compare reaped-child accounting with `wait4`/`getrusage`, exercise a
short-lived-backend workload, check Linux stat parsing, and reject incomplete
process-tree snapshots.
