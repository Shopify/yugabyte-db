# Alternatives and Decision Rationale

This document records architectural alternatives, the properties they optimize,
and why the initial design uses hidden physical generations, fast snapshot copy,
and asynchronous logical CDC replay.

## Decision summary

| Concern | Selected direction |
|---|---|
| Logical identity | Preserve the original PostgreSQL table OID |
| Target ownership | Database-owned hidden physical generation |
| Bulk copy | Snapshot clone/restore fast path when compatible; logical transform copy otherwise |
| Concurrent changes | Internal CDCSDK logical records |
| Apply | Re-encode through target schema; idempotent, transaction-aware target |
| Final correctness | Brief distributed write fence and final barrier drain |
| Recovery | Durable master-owned job; cancel before `K`, roll forward after `K` |

## Existing blocking rewrite

The baseline PostgreSQL rewrite creates new storage, copies rows, and swaps
storage while holding a strong table lock. It naturally preserves transaction and
catalog semantics but makes downtime proportional to table size and single-backend
copy performance.

It remains appropriate for small tables and unsupported online shapes. Online
schema change moves O(data) work out of the final lock window.

## Synchronous trigger mirroring

### Model

Install source triggers that transform each INSERT/UPDATE/DELETE into target
writes while a background backfill copies pre-existing rows.

### Advantages

- Target does not lag after each source transaction commits.
- Source and target mutations can participate in one transaction.
- Final catch-up is naturally small.
- Semantic target errors can fail the foreground source transaction.
- A user-defined target/transform workflow is straightforward.

### Costs

- Every source write pays target routing/write/replication latency.
- UPDATE can require old-row materialization plus target DELETE and INSERT.
- Statement transition tables consume memory and may spill to disk.
- Trigger presence can disable YSQL fast-path write optimizations.
- User statement boundaries, transition-table behavior, trigger recursion, and
  side effects become part of the mirroring protocol.
- Target/tablet count and index count multiply foreground write amplification.
- A slow/unavailable target directly affects source availability.

### Decision

Do not use user-visible PostgreSQL triggers as the core mechanism. The feature's
primary goal is to avoid prolonged workload interruption without imposing a large
foreground penalty for the entire copy. Trigger mirroring may remain useful for a
separate expert workflow or as a narrowly scoped fallback, but it requires its
own performance and semantic contract.

## Asynchronous CDCSDK logical replay

### Model

Arm a source-table CDCSDK stream before snapshot `S`, decode full logical rows,
and replay committed changes into the target asynchronously.

### Advantages

- No extra target flush on the foreground source write path.
- Logical row images can be transformed into a different schema/partition key.
- Existing CDC retention, checkpoints, safe time, and tablet-split machinery can
  be reused.
- Replay can be distributed and throttled separately from user writes.

### Costs

- Target can lag; convergence and retained WAL/history require monitoring.
- Final cutover needs a write fence and deterministic drain barrier.
- Already committed source transactions cannot be rejected for asynchronous
  target semantic errors.
- Full transaction assembly, idempotency, and crash-safe checkpoints are required.
- A centralized VirtualWAL-style assembler can become a bottleneck; long-term
  replay should distribute by source tablet/range while preserving transaction
  atomicity.

### Decision

Selected for the main design. The current prototype proves per-tablet logical
records and per-row apply; durable transaction-aware replay remains roadmap work.

## Query-layer or storage-layer synchronous dual write

YugabyteDB could add an internal table permission state similar to online index
backfill, causing normal DML to write both generations without PostgreSQL user
triggers.

This avoids trigger semantics and could shrink final downtime, but still adds
foreground write amplification and requires target transform execution in the
write path. Complex PostgreSQL expressions/types may not be available at the
DocDB layer. It also expands every DML RPC and transaction participant set.

This may be a future optimization for the final low-lag phase, not the initial
copy-time mechanism.

## External audit/replay-table tools

External PostgreSQL online-schema-change tools commonly install triggers, append
changes to an audit table, copy rows in a long snapshot transaction, replay the
audit queue, then rename tables.

The model is poorly matched to YugabyteDB:

- a centralized SQL replay queue limits distributed convergence;
- long serializable/snapshot transactions and distributed conflicts are costly;
- table rename swaps do not preserve all OID-bound dependencies;
- trigger and audit-table writes amplify distributed replication;
- tool ownership of retries/failover/checkpoints is weaker than a durable master
  job; and
- external tools cannot safely coordinate DocDB generation visibility, backup,
  PITR, and CDC handoff.

External tools remain useful references for workflow expectations, not the core
implementation.

## Raw Raft WAL or raw DocDB KV replay

Raw WAL/KV avoids logical decoding/re-encoding and is efficient for a physically
identical clone. For a non-colocated same-schema table, keys may transplant and
packed schema versions may be remapped.

It is not a general schema-change API:

- raw WAL includes intents/metadata/packed values rather than stable full rows;
- target column ids and packed layouts change across structural DDL;
- primary-key/partition transforms require rerouting;
- old/new logical row images and PostgreSQL types are needed for expressions;
  and
- colocated keys embed table/colocation identity.

The clone fast path may copy the base snapshot physically, but post-`S` replay
uses logical CDC records.

## Logical row-by-row copy only

A general transform scan can support arbitrary source/target schema and partition
layouts. It also rewrites the full data volume, consumes read/write bandwidth,
and takes substantial time for very large tablets.

The selected hybrid is:

- clone/restore when target physical layout is compatible;
- logical distributed scan/transform when it is not; and
- logical CDC replay in both cases.

## User-visible target table

Allowing users to create and inspect the target makes arbitrary transforms easy
to express, but raises ownership and correctness questions:

- direct DML can bypass source ordering/checkpoints;
- target triggers/defaults can produce duplicate side effects;
- target OIDs differ from stable source OIDs;
- privileges/ownership may accidentally change at cutover;
- backup/CDC may discover the target as an ordinary table; and
- bidirectional writes require conflict resolution.

The selected default is an internally owned hidden generation. A future expert
workflow may accept a target schema template while still sealing/adopting an
internal shadow; see [API and workflow evolution](api-and-workflows.md).

## Rename swap versus OID-preserving generation activation

Renaming source and target tables is operationally simple but changes the object
behind the original name. Views, incoming foreign keys, row types, publications,
policies, parsed function bodies, ACL/provider state, and extensions may remain
bound to the old OID.

The design preserves the original table OID and changes physical storage/schema
behind it. This makes cutover more catalog-intensive but avoids widespread
dependency retargeting.

## Full-copy lock versus final barrier lock

Locking before snapshot and holding through copy is simple and correct but defeats
the online objective. The selected protocol captures/replays while writes continue
and acquires the distributed write fence only after target lag is small.

If the final drain exceeds a deadline, release the fence and return to replay
rather than extending workload downtime indefinitely.

## Manual synchronization

Applying captured changes only on demand reduces background replay load but does
not reduce WAL/history retention. It increases the risk of an unbounded final
catch-up and disk pressure. Automatic continuous replay is the default; manual
sync is a possible policy described in [API and workflow evolution](api-and-workflows.md).

## Bidirectional mirroring

Bidirectional writes offer target testing and apparent rollback but create a
multi-master conflict-resolution system. They violate the one-authoritative-
generation invariant and are not required to perform an online cutover. Treat
them as a separate replication/data-migration feature.

## Long-term logical generation pointer

The near-term approach changes `pg_class.relfilenode` entries. For large
partition/index bundles, a first-class logical-to-physical generation-group
pointer can make activation constant-work and give backup/PITR/CDC an explicit
validity interval. This is a later metadata architecture, not required to prove
the initial flow.
