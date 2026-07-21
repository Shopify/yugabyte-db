# OSC prototype: first concrete in-tree implementation steps

Context: the external harness (`mirror_harness.sh`, `mirror_harness_nm.sh`) has
validated the mirror mechanics end to end, including the distributed N:M shape.
It fakes three things: cutover (name swap), drain (sleep), and the applier
(batch, external, no ledger).

Decision: the first in-tree work targets the **highest-risk, no-precedent**
mechanic, not more mirror plumbing. That is the **OID-preserving storage
switch**. Logical replication already gives us capture; the switch is the part
that can invalidate the roadmap.

## Guiding principle

Decouple the three mechanics and land them independently, each behind a test:

1. Storage switch (this step) - highest risk, smallest blast radius.
2. Live streaming applier with idempotency ledger - correctness of mirroring.
3. Master-owned job + barrier/drain - orchestration and failover.

Do NOT start by wiring mirror + switch + job together. Prove the switch alone.

## Step 1 (first PR): reusable "adopt a caught-up shadow" storage switch

Goal: given a shadow DocDB table that already holds the fully transformed data
for a source table, atomically make the source relation point at the shadow's
storage while preserving `pg_class.oid` and dependencies. No mirroring, no job.

Reuse existing primitives (already in tree):

- `YbRelationSetNewRelfileNode` (`pg_yb_utils.c`): creates a new DocDB table
  under a new relfilenode while keeping the stable PG OID and passing
  `old_rewrite_table_id`.
- `make_new_heap` / `finish_heap_swap` / `swap_relation_files` (`cluster.c`):
  swap physical identity (relfilenode) while preserving logical OID.
- `pg_table_id` / `old_rewrite_table_id` in `master_ddl.proto` +
  `CreateTableInfo` persistence.
- `ysql_ddl_verification_task.cc`: already checks both `oid` and `relfilenode`
  in `pg_class` for rewritten tables.

New surface (minimal):

- A `finish_yb_adopt_shadow(source_oid, shadow_oid, index_pairs)` path (a YB
  mode of `finish_heap_swap`) that:
  - applies target logical schema changes to the source OID,
  - swaps base + paired index physical relfilenodes,
  - SKIPS index rebuild (indexes were prebuilt/caught-up),
  - bumps catalog version as a breaking change,
  - invalidates source/old/new caches,
  - retires the old physical generation.

Explicitly out of scope for Step 1: mirroring, drain, master job, partitions,
colocation, CDC/backup handoff. The shadow is populated by a plain snapshot copy
in the test (no concurrent writes), so the switch is tested in isolation.

### Step 1 test (C++, pgwrapper)

New suite, for example `pg_online_schema_change-test.cc`:

- `SwapPreservesOid`: create `t`, capture `t`'s `pg_class.oid`; build a shadow
  with target schema; snapshot-copy rows; run the adopt-shadow switch; assert
  `pg_class.oid` unchanged, new `relfilenode`, data present with new schema.
- `SwapPreservesDependents`: a view and an FK reference `t`; after switch they
  still resolve and enforce (this is the whole point of preserving OID vs a
  name swap).
- `SwapCrashRecovery` (later): inject failure around the catalog commit; assert
  DDL verification resolves to exactly one generation (old on abort, new on
  commit), never both.

Build/run:

```
PATH="/usr/bin:/bin:$PATH" ./yb_build.sh release --cxx-test pg_online_schema_change-test \
  --gtest_filter 'PgOnlineSchemaChangeTest.SwapPreservesOid'
```

## Step 2 (second PR): live streaming applier + idempotency ledger

Replace the harness's batch apply with a real consumer:

- Consume the logical slot continuously (VWAL / walsender path).
- Preserve source transaction framing into one target transaction (the N:M
  harness already proved this is required and sufficient for atomicity).
- Add a durable ledger row `(slot_id, commit_lsn) -> applied` committed in the
  SAME target transaction; on redelivery, skip already-applied commits.
- Acknowledge the source LSN only after the target commit.

Test: kill the applier mid-stream and restart; assert no duplicate/lost effects
(exactly-once effect on top of at-least-once transport).

## Step 3 (third PR): master-owned job + real barrier/drain

- A `SysOnlineSchemaChangeJobPB`-style entity (state machine from the roadmap).
- Real "mirror applied through barrier F" check replacing the sleep: per-source-
  tablet resolved-HT vector, not a single LSN.
- Wire Step 1 switch as the job's cutover action under a short object lock.

## Sequencing rationale

- Step 1 first because it has no existing precedent and the largest chance of
  surprising us (catalog + master + tablet + cache + verification interplay).
  If OID-preserving swap turns out to be harder than the roadmap assumes, we
  want to know before investing in applier/job machinery.
- Step 2 hardens the mechanic the harness only faked, using primitives we have
  already verified work (`pg_recvlogical`, `REPLICA IDENTITY FULL`, framing).
- Step 3 assembles the orchestration once the two hard mechanics are proven.

## Non-goals until Steps 1-3 land

Partitioned/geo/colocated shapes, CDC external handoff, backup/PITR generation
awareness, gRPC CDC, long-term logical-to-physical pointer. These are roadmap
GA gates but must not block proving the core switch + applier + job loop.
