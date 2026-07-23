# API and Workflow Evolution

This document separates the durable execution primitive from possible user-facing
syntax. The initial feature should have one authoritative migration engine even
if multiple workflows are added later.

## Design goals

- Keep ordinary `ALTER TABLE` wire behavior compatible with PostgreSQL drivers.
- Make the long-running operation durable and resumable after the initiating
  backend disconnects.
- Preserve the original logical table OID and dependent-object identity.
- Keep one authoritative writable generation throughout the migration.
- Apply one deterministic transform consistently during copy and replay.
- Prevent direct user writes from bypassing migration ordering/checkpoints.

## Layer 1: durable asynchronous primitive

The core primitive is the master-owned job described in
[Job and SQL API](job-and-sql-api.md):

```sql
SELECT yb_start_online_schema_change(
  't'::regclass,
  'ALTER TABLE t ALTER COLUMN value TYPE bigint',
  NULL -- optional request id
);
```

Submission durably records intent and returns a migration id. It does not imply
that the submitted DDL is already compiled or applied in the current prototype.
The production primitive must persist:

```text
source logical/physical identity
target schema and physical-generation bundle
versioned transform plan and dependency fingerprints
copy/replay checkpoints and retention owners
validation plan
cutover policy and retry state
```

This primitive should remain asynchronous even if convenience syntax waits for
completion. A backend disconnect must not abandon the operation.

## Layer 2: automated online DDL

The normal workflow should derive target state directly from DDL:

```sql
ALTER TABLE t ALTER COLUMN value TYPE bigint CONCURRENTLY;
```

The exact syntax is an open product decision. Its implementation should:

1. Parse and classify the DDL using normal PostgreSQL semantics.
2. Reject metadata-only operations that should use the existing fast path.
3. Compile target schema/catalog changes and a row transform.
4. Submit the same durable migration job used by the explicit function.
5. Either return after durable admission or wait by polling the job, without
   changing the underlying operation identity.

The convenience statement must preserve standard command tags/result types. If
returning the migration id would break driver behavior, expose it through a
separate function or notice/status lookup rather than a result row.

## Expert-defined target workflow

Some reshapes cannot be expressed as one existing `ALTER TABLE` clause: changing
partition topology, placement, colocation, or a complex primary-key mapping may
need an explicit target definition.

A future expert API could accept:

- a target table/schema template;
- a target partition/placement specification;
- an explicit source-to-target transform; and
- a policy for indexes, constraints, and dependents.

This does **not** imply that an ordinary writable user table becomes the shadow.
The migration should validate and seal the target definition, create or adopt an
internally owned `SHADOW` generation, and prohibit direct user DML while it owns
that generation. Otherwise user writes could bypass source ordering, replay
checkpoints, constraints, and the final barrier.

Questions that require an explicit contract:

- Can a pre-created target contain data, or must it be empty?
- Is the target object consumed at admission or only used as a schema template?
- Which ownership/ACL/tablespace properties are copied versus explicitly set?
- How are target OIDs mapped back to stable source OIDs at cutover?
- Can a user inspect target rows before cutover, and if so, at what consistency
  frontier and under which read-only capability?

## Transform plan

Copy and replay must execute the same versioned transform. The plan should be
addressed by stable source/target column identity, not only column position.

Suggested per-target-column policies:

```text
PRESERVE    copy a source value
DERIVE      evaluate a deterministic expression over source values
INITIALIZE  evaluate a value once for a pre-existing source row
DROP        omit the source value
```

The persisted plan needs:

- source and target schema versions/hashes;
- canonical expression representation;
- referenced type/function/collation dependency fingerprints;
- volatility and side-effect classification;
- nullability, cast, and overflow behavior;
- old-key/new-key mapping for UPDATE and DELETE; and
- versioning rules across retries and rolling upgrades.

### User functions

Allowing an arbitrary SQL function is flexible but introduces volatility,
security-definer, search-path, dependency, and side-effect concerns. Initial
support should prefer generated deterministic expressions and immutable built-ins.
If user functions are supported later, admission must pin dependencies and reject
unsafe volatility or external side effects.

### Volatile defaults and sequences

Operations such as `DEFAULT random()` or sequence/identity allocation require a
durable once-per-row result. Re-evaluating during retry or replay can produce a
different target row. Options include persisting evaluated values/events or a
deterministic event-seeded function. The choice must be part of the transform
contract, not an implementation accident.

## Transaction boundaries of control functions

Starting an asynchronous migration inside a user transaction is problematic: the
job can outlive the transaction/backend and cannot safely expose work before the
transaction commits. The production API should either:

- reject start inside an explicit transaction, similar to concurrent index
  operations; or
- defer durable admission until commit through explicit transactional-DDL
  integration.

Final cutover should be job-owned. Exposing a user transaction around finalize is
useful only if all catalog/object changes participate in the same authoritative
cutover transaction and the write fence remains correctly owned on abort.

## Read-only target inspection

Users may want to validate the target before cutover. A safe inspection feature
would be read-only and frontier-aware:

```text
target is complete at snapshot S
target replay is applied through frontier A
queries must state or expose A
```

It must not make the hidden generation generally discoverable or writable, and
results must not be presented as current if replay is behind. This is separate
from bidirectional mirroring.

## Manual synchronization

Manual sync could reduce background resource use by replaying only when requested,
but it does not eliminate capture/retention requirements. WAL and history must be
retained continuously from `S`, so a long pause can increase disk pressure and
make the final sync unbounded.

If added, it should be an explicit job policy with:

- maximum retained bytes/time;
- source-availability-first cancellation thresholds;
- a command to advance the apply frontier;
- clear lag/status reporting; and
- the same final fenced drain as automatic replay.

It is not required for the initial feature.

## Bidirectional mirroring

Bidirectional writes are not part of the online schema change invariant. They
introduce conflict detection/resolution, loop prevention, schema-asymmetric
updates, and two authoritative sources. That is a replication/data-migration
feature rather than a cutover optimization.

Initial OSC semantics should remain:

```text
before K: G0 is the only user-writable authority
after K:  G1 is the only user-writable authority
```

Before `K`, rollback means cancel the migration and discard `G1`. After `K`,
recovery is roll-forward; retaining `G0` does not make it writable. A future
bidirectional workflow should be designed independently with an explicit conflict
contract.

## CTAS and materialized views

The distributed copy engine may be reusable beyond table rewrites, but semantics
differ:

- `CREATE TABLE AS` creates a new logical object and normally has no source-write
  mirroring requirement. It can reuse parallel snapshot scan, transform, routing,
  throttling, and progress infrastructure.
- `REFRESH MATERIALIZED VIEW` replaces data behind an existing logical object and
  is closer to generation activation. Its source is a query plan over possibly
  many relations, not one CDC stream, so maintaining it incrementally requires
  query-incrementalization semantics that OSC does not provide.
- Simple single-source projections/filters could use the transform/copy engine;
  joins, aggregates, and volatile queries need a separate strategy or fall back
  to existing execution.

These are follow-on consumers of shared infrastructure, not initial OSC scope.

## API decisions to resolve

1. Final syntax for automated online DDL.
2. Whether an expert target-definition API is needed for the first reshape
   release.
3. Durable transform representation and supported expression classes.
4. Transaction restrictions on submission and finalize.
5. Read-only target inspection semantics.
6. Whether manual synchronization is valuable enough to support.
7. Explicit non-goal or separate roadmap for bidirectional mirroring.
8. Which parts of CTAS/materialized-view execution should reuse OSC workers.
