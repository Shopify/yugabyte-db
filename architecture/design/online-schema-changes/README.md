# Online Schema Changes

This directory describes the design and current prototype for online YSQL table
rewrites in YugabyteDB. The feature builds a hidden physical generation, copies
the source at a fixed HybridTime, replays later changes, and eventually redirects
the original PostgreSQL relation to the new storage.

**Status:** preview prototype behind test flags. The copy and logical replay path
work for a regular, non-colocated table. The final write fence and atomic cutover
are not implemented, so the current prototype is not safe for uninterrupted
production writes through cutover.

Tracking issue: [#4192](https://github.com/yugabyte/yugabyte-db/issues/4192)

## Start here

| If you want to understand... | Read |
|---|---|
| The idea in five minutes | [Overview](overview.md) |
| What works today versus the target design | [Implementation status](implementation-status.md) |
| The order of upcoming work | [Roadmap](roadmap.md) |
| How to run and extend the tests | [Testing](testing.md) |

## Component design

| Component | Document |
|---|---|
| Durable migration identity, phases, SQL/RPC surfaces | [Job and SQL API](job-and-sql-api.md) |
| Active, shadow, and retired physical storage | [Physical generations](physical-generations.md) |
| Snapshot, tablet clone, and restore | [Copy: clone and restore](copy-clone-restore.md) |
| Internal CDCSDK capture and logical apply | [Change capture and replay](change-capture-replay.md) |
| Barrier, downtime, relfilenode switch, and cache invalidation | [Cutover and fencing](cutover-and-fencing.md) |
| Failover, cancellation, cleanup, and retention | [Failure recovery](failure-recovery.md) |
| Supported and future table/feature shapes | [Compatibility and scope](compatibility-scope.md) |

## Diagrams

Rendered diagrams are embedded in the relevant documents. PlantUML sources are
kept in [`diagrams/`](diagrams/README.md):

- [High-level lifecycle](diagrams/01-high-level-lifecycle.puml)
- [Component topology](diagrams/02-component-topology.puml)
- [Physical-generation roles](diagrams/03-generation-roles.puml)
- [Copy, replay, and cutover timeline](diagrams/04-copy-replay-timeline.puml)
- [Logical replay data flow](diagrams/05-replay-dataflow.puml)
- [Current versus target cutover](diagrams/06-cutover-current-vs-target.puml)
- [Migration job state machine](diagrams/07-job-state-machine.puml)
- [Failover and recovery](diagrams/08-failure-recovery.puml)

## Terminology

| Symbol | Meaning |
|---|---|
| `L` | Stable logical table identity: the original `pg_class.oid` |
| `G0` | Currently active physical DocDB generation |
| `G1` | Shadow target physical DocDB generation |
| `S` | Snapshot HybridTime used by the bulk copy |
| `F` | Final source barrier HybridTime replay must cover |
| `K` | Atomic catalog cutover commit HybridTime in the target design |

The desired invariant is:

```text
G0 serves the logical relation before K.
G1 serves the logical relation at and after K.
Exactly one generation is user-visible at any point in time.
```

The current prototype does not yet implement an atomic `K`; see
[Cutover and fencing](cutover-and-fencing.md).
