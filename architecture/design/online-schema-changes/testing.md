# Testing

## Build

Use the repository build wrapper:

```bash
PATH="/usr/bin:/bin:$PATH" ./yb_build.sh release \
  --target pg_online_schema_change-test \
  --sj --skip-pg-parquet --no-odyssey --no-ybc
```

Catalog/protobuf changes may require rebuilding daemons and initdb:

```bash
PATH="/usr/bin:/bin:$PATH" ./yb_build.sh release daemons initdb \
  --sj --skip-pg-parquet --no-odyssey --no-ybc
```

## Core C++ tests

Run one test per invocation:

```bash
PATH="/usr/bin:/bin:$PATH" ./yb_build.sh release \
  --cxx-test pg_online_schema_change-test \
  --gtest_filter 'PgOnlineSchemaChangeTest.ReplayCapturesConcurrentWrites'
```

Important test coverage:

| Test | Proves |
|---|---|
| `SwapPreservesOid` | Baseline PostgreSQL rewrite preserves logical relation OID |
| `SwapPreservesDependents` | Baseline PostgreSQL rewrite preserves views and foreign keys |
| `ShadowGenerationHiddenFromListTables` | Shadow is not user-enumerated |
| `CreateShadowGeneration` | Master creates owned hidden generation |
| `MigrationCreatesShadowGeneration` | Durable job drives shadow creation |
| `MigrationCopiesAndCutsOver` | RF1 clone/restore and role flip |
| `FinalizeServesFromShadow` | PostgreSQL relfilenode repoint serves copied data |
| `PgOnlineSchemaChangeRf3Test.MultiTabletMigration` | RF3/multi-tablet physical copy parity |
| `ReplayCapturesConcurrentWrites` | Post-`S` INSERT/UPDATE/DELETE are replayed before switch |
| `CDCSDKYsqlTest.WalsenderQlValueSmoke` | Non-slot WALSENDER GetChanges yields `pg_ql_value` |

Durable job tests:

```bash
PATH="/usr/bin:/bin:$PATH" ./yb_build.sh release \
  --cxx-test schema_migration_manager-itest
```

They cover admission/success, phase reporting, request-id idempotency,
cancellation, and master-failover reload.

## Local manual cluster

Prototype flags:

```bash
./bin/yb-ctl --binary_dir "$(pwd)/build/latest" create --rf 1 \
  --master_flags \
  "TEST_enable_schema_migration_admission=true,TEST_schema_migration_create_shadow=true"
```

Connect with:

```bash
build/latest/postgres/bin/ysqlsh -h 127.0.0.1 -p 5433 -U yugabyte yugabyte
```

The current manual flow is start -> poll -> finalize. It is not safe with
uninterrupted writers through cutover; see
[Cutover and fencing](cutover-and-fencing.md).

## Next correctness tests

- Continuous writer while replay catches up, followed by a real distributed
  final fence; exact row parity after release.
- Multi-row and cross-tablet transaction preserved atomically.
- Crash after shadow apply but before durable checkpoint.
- Source/shadow tablet split during replay.
- Master failover during copy, replay, final fence, and ambiguous cutover.
- CDC stream/retention cleanup on success, cancel, and failure.
- Structural target schema: add/drop/retype/default and primary-key change.
- Target index and constraint validation through `F`.
- External CDC event-set proving no internal copy/replay events.
- Backup/PITR before, during, and after `K`.
- Object-lock cutover timing under long and short transactions on Linux.

## Full production matrix

- RF1/RF3 and multiple source/target tablet counts.
- Large-table resource throttling and disk/WAL pressure cancellation.
- Partitioned, geo-partitioned, and colocated generation groups.
- Incremental backup chain crossing cutover.
- Mixed-version upgrade/rollback with a paused migration.
- Cancellation before `K` and roll-forward recovery after `K`.
