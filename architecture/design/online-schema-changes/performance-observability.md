# Performance, Observability, and Supportability

Online schema change trades a long blocking rewrite for background resource use,
retention, and a short final interruption. Production safety requires both
workload protection and enough telemetry to explain why a migration is slow or
stuck.

## Performance objectives

- Avoid synchronous target writes on the foreground source DML path during copy.
- Keep source latency impact within configured resource budgets.
- Make copy/replay throughput scale with tablets and tservers.
- Converge replay faster than the source change rate before attempting cutover.
- Bound final write-fence duration and release it on timeout.
- Prefer cancelling the migration over exhausting source disk/WAL/history.
- Expose progress and pressure without logging user row values.

No fixed latency/downtime target should be promised until measured under
representative workloads, transaction lengths, tablet counts, and placements.

## Resource model by phase

| Phase | Source impact | Target impact | Primary risk |
|---|---|---|---|
| Admission/preflight | Catalog reads/locks | Metadata planning | Large hierarchy/dependency enumeration |
| Capture arming | CDC metadata + retention barriers | None | WAL/history retention begins before useful progress |
| Snapshot/clone | Snapshot flush/checkpoint and file links | Tablet creation/restore | Compaction/disk amplification after clone |
| Logical copy | Snapshot reads | Full transformed writes/index maintenance | Saturating source reads or target writes |
| Replay | CDC decode/history reads | Transform, routing, transactional writes | Apply rate below source change rate |
| Validation | Fixed-frontier scans | Index/constraint reads | Long scan delaying cutover readiness |
| Final fence | Transaction drain/object locks | Tiny replay tail/catalog commit | Long transactions or stale lag extend downtime |
| Retention/cleanup | Old WAL/generation retained | Old + new storage | Disk pressure if owners do not release |

## Copy cost

### Clone/restore fast path

SST hard links avoid initially rewriting every byte and are appropriate for very
large physically compatible tablets. They are not free:

- snapshot creation may flush memtables and interact with compaction;
- source and shadow initially share files but later compactions create new files;
- retaining old/new generations increases metadata, WAL, and eventual disk use;
- target tablet bootstrap/restore consumes I/O and CPU; and
- placement differences may require physical transfer rather than local links.

Report copied/logically referenced bytes separately from newly allocated physical
bytes so progress is not misleading.

### Logical transform path

Arbitrary schema/partition changes read and rewrite the full table. Throughput is
bounded by the minimum of source snapshot read, transform CPU, network routing,
target Raft/write, and index maintenance capacity.

Workers need per-node/tablet concurrency and byte-rate limits rather than an
unbounded task per tablet.

## Replay cost

CDC avoids a second target flush in the foreground transaction, but adds:

- WAL/history retention and CDC decoding CPU;
- full old/new row image materialization for transformations;
- transaction assembly/state;
- target transform/routing/write load; and
- checkpoint/queue durability.

The current prototype is intentionally inefficient: source tablets are processed
sequentially and every record is flushed separately. Production replay should
batch while preserving source transaction boundaries, parallelize independent
ranges/transactions, and adapt concurrency to lag and target pressure.

The readiness condition is not "copy complete". Replay apply throughput must
exceed incoming change throughput long enough for lag to approach the final-fence
budget.

## Retention and disk pressure

Capture arming can retain:

- source tablet WAL;
- transaction intents;
- historical row versions needed for old/full images;
- historical schema packings;
- target snapshots/generation files; and
- the retired source generation after cutover.

Track ownership independently for migration, external CDC, PITR, backup, and
rollback grace periods. A migration may release only its own owner.

Suggested policy thresholds:

```text
soft: throttle copy/replay peers and warn
pause: stop new copy work but continue replay/cleanup if it reduces retention
hard: cancel pre-K migration and release its retention to protect source
```

Thresholds should use free bytes plus retained-byte growth rate, not only elapsed
time.

## Final-fence performance

Fence duration consists of:

```text
wait for conflicting transactions
+ distributed object-lock fanout
+ resolve source tablets through F
+ apply final replay tail
+ catalog/generation commit and cache invalidation
```

Long transactions dominate this window. Before acquiring the fence, preflight
should estimate/observe:

- replay lag and apply rate;
- oldest active/prepared transaction touching the hierarchy;
- target validation/index frontier;
- tserver/object-lock health; and
- catalog-version propagation health.

If readiness regresses or a deadline expires, release the fence and resume replay.

## Status model

Current SQL surfaces:

- `yb_schema_migrations`: durable summary and current phase;
- `yb_schema_migration_progress`: prototype aggregate/detail projection.

Target per-work-unit status should expose:

```text
migration_id, work_kind, source table/tablet lineage/range
state, attempt, worker epoch, last error
snapshot/copy checkpoint and frontier
CDC checkpoint, captured_ht, applied_ht, resolved_ht
rows/bytes read and written
last progress time
```

Point lookup by migration id must push down to RPC/DocDB rather than materialize
all jobs and filter in PostgreSQL.

## Metrics

### Job summary

- jobs by lifecycle state/phase/kind;
- phase duration and total age;
- retry/failure/cancellation counts by classified reason;
- time since last durable progress; and
- final-fence attempts, duration, timeout, and rollback-to-replay count.

### Copy

- source rows/bytes scanned;
- logical rows/bytes transformed;
- clone tablets/files/bytes referenced and restored;
- source/target throughput and throttled time;
- copy work units pending/running/retrying/complete; and
- split-lineage/restart counts.

### Replay

- source commit/resolved HybridTime;
- capture and apply frontiers;
- replay lag in HybridTime/wall-clock terms;
- input/apply transactions, records, and bytes per second;
- queue depth/bytes and oldest unapplied transaction age;
- duplicate/idempotent skips and semantic errors; and
- checkpoint age/update failures.

### Retention and storage

- WAL/history bytes retained by migration;
- oldest required OpId/HybridTime per tablet;
- shadow/retired generation logical and physical bytes;
- snapshot/restoration age;
- free disk and retained-byte growth rate; and
- cleanup owner count/age.

### Catalog and cutover

- object-lock acquisition/drain time;
- oldest conflicting transaction age;
- catalog commit and invalidation propagation time;
- backends/tservers waiting for target catalog version; and
- stale/retired generation request rejections.

Avoid migration-id labels on high-cardinality Prometheus metrics if job count can
grow unbounded; use SQL detail views/log correlation for individual jobs.

## Active Session History and wait states

Long waits should be classified rather than appearing as generic RPC/sleep time.
Candidate wait states include:

- schema migration admission/preflight;
- source snapshot creation;
- shadow clone/restore;
- CDC GetChanges / resolved-time wait;
- shadow replay apply/flush;
- validation/index frontier wait;
- distributed object-lock acquisition/transaction drain;
- catalog-version/invalidation propagation; and
- cleanup/retention release.

Only foreground/user-visible waits should be attributed to the submitting session;
background jobs need their own migration metadata and background trackers.

## Logs and events

Every structured event should include:

```text
migration_id, state_epoch, phase, source/target physical ids,
tablet/range when relevant, attempt, status classification
```

Log phase transitions and classified failures at normal levels; rate-limit
per-record/per-poll messages. Never log row payloads. Keys in semantic errors must
be omitted, hashed, or safely redacted according to logging policy.

Important durable events:

- admitted/cancel requested;
- shadow/capture/copy/replay/validation ready;
- fence requested/acquired/released/timeout;
- cutover decision and transaction id;
- roll-forward verification;
- stream/snapshot/generation cleanup; and
- retention-pressure pause/cancel.

## Tuning controls

Production controls should be runtime-updatable where safe and scoped by job or
resource group:

- maximum copy/replay workers per node/table/tablet;
- source read and target write bytes/sec;
- replay batch size and in-flight transaction/byte limit;
- checkpoint interval by time/bytes;
- target lag threshold before fence attempt;
- final-fence acquisition/hold deadline;
- validation concurrency;
- retained WAL/history soft/hard limits; and
- terminal metadata/detail retention windows.

Test flags must not become the production configuration contract.

## Troubleshooting workflow

### Copy not progressing

Check worker error/epoch, source snapshot safe time, tablet leader health, target
tablet state, restore state, throttling, split lineage, and source/target disk.

### Replay lag growing

Compare source change rate with capture/apply rates; inspect queue oldest age,
transaction assembly, target write latency, target hot tablets/indexes, semantic
errors, and retained-history availability.

### Fence cannot be acquired

Inspect conflicting/long/prepared transactions, object-lock propagation, DDL
leases, and whether the hierarchy changed after planning.

### Migration cannot clean up

List remaining retention owners, CDC stream/state rows, snapshots/restorations,
active readers/backup/PITR references, and generation/tablet deletion errors.

## Support bundle content

Include, with sensitive values removed:

- durable summary and per-work-unit progress rows;
- source/shadow generation metadata and tablet lineage;
- transform/schema hashes (not user data);
- CDC stream/checkpoints/frontiers and retention owners;
- snapshot/restore ids and states;
- classified recent events/errors/retries;
- relevant flags/resource limits;
- object-lock/conflicting transaction summary; and
- catalog version/invalidation state.

The bundle should make it possible to answer: Is source still authoritative?
What is blocking progress? How far is target applied? What retains disk/WAL? Is
cancel or roll-forward safe?

## Benchmark and acceptance matrix

Measure source latency/throughput, migration throughput, replay lag, storage
growth, and final-fence duration across:

- read-heavy, write-heavy, and mixed workloads;
- INSERT/UPDATE/DELETE and multi-row/cross-tablet transactions;
- different row sizes, index counts, tablet counts, and RF;
- clone-compatible and logical-transform copies;
- partition-key-changing updates and skew/hot tablets;
- local, multi-zone, and geo placements;
- long/prepared transactions at cutover;
- compaction, tablet split, leader failover, and rolling restart; and
- disk/WAL pressure with pause/cancel thresholds.

Compare against the same workload without migration. Record percentiles and
resource saturation, not only median statement latency.
