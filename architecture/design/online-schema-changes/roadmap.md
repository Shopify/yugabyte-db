# Roadmap

The roadmap is organized by correctness dependency rather than code ownership.
Each milestone should be independently testable behind the preview gate.

Status legend: **Done**, **Prototype**, **Next**, **Planned**.

## Milestones

| ID | Deliverable | Status | Exit criterion |
|---|---|---|---|
| A0 | Durable migration job and SQL status API | Done | Job survives master failover; idempotent submission; terminal state queryable |
| A1 | Hidden physical generation | Done | Shadow is internally addressable and excluded from user enumeration |
| A2 | Fixed-HT physical copy | Prototype | RF3 multi-tablet clone/restore parity at snapshot `S` |
| B1 | Internal logical change capture | Prototype | Slot-less table-bound CDCSDK stream armed before `S` |
| B2 | Logical replay | Prototype | Post-`S` INSERT/UPDATE/DELETE reach the shadow and drain to `F` |
| B3 | Durable transaction-aware replay | Next | Preserve source transactions; durable per-tablet checkpoints; retry exactly once in effect |
| C1 | OID-preserving storage switch | Prototype | Original relation OID serves from shadow relfilenode |
| C2 | Brief fenced cutover | Next | Writes fenced only for final tail drain + switch; no post-`F` loss window |
| C3 | Atomic catalog activation | Planned | Role flip, relfilenode/schema switch, catalog version, and invalidation recover as one decision |
| D | Target schema and transform plan | Planned | Apply submitted rewrite DDL; deterministic copy/replay transform; constraints validated |
| E | Cleanup and retention ownership | Next | Drop capture stream on terminal paths; GC abandoned shadow and retained source safely |
| F | External CDC handoff | Planned | No internal mirror events; one generation/schema transition; checkpoint handoff |
| G | Backup, restore, and PITR | Planned | Generation selected by snapshot time; restore returns one ordinary active generation |
| H | Partition/index generation groups | Planned | Root/leaves/index bundle switches atomically; independent source/target routing |
| I | Geo and colocated layouts | Planned | Placement-aware copy; table-level generation handling in shared tablets |
| J | GA hardening | Planned | Upgrade/rollback, pressure policy, observability, cancellation, and fault matrix complete |
| K | Logical-to-physical generation pointer | Long term | Constant-work generation-group activation replaces per-relation relfilenode edits |

## Immediate sequence

### 1. Correct final cutover

- Replay continuously until lag is small instead of choosing `F` immediately.
- Acquire a distributed table write fence (`ACCESS EXCLUSIVE` through object
  locks on Linux test clusters).
- Choose `F` only after the fence blocks new source writes and drains in-flight
  transactions.
- Replay the final tail through `F`.
- Repoint the PostgreSQL relation, bump catalog version, flip generation roles,
  and release the fence.
- Add a writer-through-cutover test proving no loss after the final barrier.

### 2. Replay durability and transaction semantics

- Persist one checkpoint and applied frontier per source tablet/range.
- Preserve BEGIN/COMMIT and apply one source transaction atomically on the
  target, including cross-tablet transactions.
- Make replay idempotent across crash after apply but before checkpoint.
- Handle tablet splits through source lineage rather than a fixed tablet list.

### 3. Resource cleanup

- Delete the internal CDCSDK stream on success, cancel, and terminal failure.
- Release WAL/history retention and remove `cdc_state` rows.
- Drop abandoned shadows on pre-cutover cancel.
- Retain and later GC the old generation according to rollback/CDC/PITR owners.

### 4. Real target schemas

- Compile and persist the submitted DDL/transform plan.
- Create `G1` with the target schema, not a source-schema clone.
- Define add/drop/retype/default/primary-key mappings for copy and replay.
- Validate constraints and indexes through the final barrier.

## Production gates

Before enabling the feature for general use:

- External logical and gRPC CDC understand generation transitions.
- Full/incremental backup and PITR select generations by HybridTime.
- Partitioned, indexed, geo-partitioned, and colocated shapes have explicit
  support or deterministic preflight rejection.
- Disk/WAL/history pressure has a source-availability-first cancellation policy.
- Mixed-version upgrade and rollback behavior is specified and tested.
- Every phase has crash, failover, retry, cancellation, and timeout tests.

## Long-term direction

The near-term cutover updates `pg_class.relfilenode` for each physical relation.
For large partition/index bundles, the desired model is an explicit logical
table record pointing to an active physical generation group. Transactions pin
the pointer epoch at first access, and cutover activates one generation-group
pointer rather than rewriting every relation entry.
