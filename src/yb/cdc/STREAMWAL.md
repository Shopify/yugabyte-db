# Prototype StreamWAL Data Contract

## Purpose

`StreamWAL` is a per-tablet, leader-only, unary tserver RPC that delivers
**committed, decoded change events** as a stream of `CDCSDKProtoRecordPB`. It
replaces the data plane of `GetChanges`. It does not register state on the server, does not use `cdc_state`, does not require stream IDs, and the client never has to buffer or join intents.

The wire format reuses the existing `CDCSDKProtoRecordPB` produced by the
`cdcsdk_producer.cc` decoder family. The proto delta is two optional envelope
fields on `CDCSDKProtoRecordPB`, three new top-level messages (request,
response, cursor), and one new error code.

Behaviorally, the server is doing what `GetChangesForCDCSDK` already does
today — it wires the existing `ProcessIntentsWithInvalidSchemaRetry` codepath
behind a stream-id-less RPC. Transactional `WRITE_OP`s are dropped on the
wire; at `APPLYING` time the server reads the transaction's intents from
IntentsDB (via `Tablet::GetIntentsForCDC` → `PopulateCDCSDKIntentRecord`),
filters aborted subtxns, stamps `commit_hybrid_time`, and emits the per-row
DML records sandwiched between `BEGIN` and `COMMIT` envelopes.

## Scope of this contract

This document specifies:

- The RPC, its request/response shape, and the cursor type (including the
  mid-APPLYING resume fields).
- Per-WAL-`OperationType` emission rules, including the transactional
  WRITE_OP "skip" and the APPLYING → IntentsDB read.
- The synthetic DDL bootstrap protocol.
- Cursor and at-least-once delivery semantics (including partial APPLYING
  batches).
- Error taxonomy.
- Schema versioning invariants the server guarantees.
- IntentsDB retention coupling.
- What information SPLIT_OP carries on the wire.

This document explicitly does **not** specify:

- How the client decides whether a transaction has been committed (it doesn't
  have to — the server only emits committed records).
- How the client handles SPLIT_OP-with-pending-transactions (a connector
  concern, but greatly simplified by the IntentsDB-based design: intents on
  the parent are inherited by the children. The SPLIT_OP record itself
  carries the child tablet IDs (`split_tablet_request.new_tablet1_id` /
  `new_tablet2_id`); the connector only needs `GetTabletLocations(child_id)`
  to resolve each child's leader tserver before re-opening StreamWAL there.
  No buffer handoff).
- Anything about polling `GetTransactionStatus` (the connector does not need
  to; ABORTED transactions are simply never emitted because the server never
  reads their intents).

---

## Service and RPC

Added to `cdc.CDCService` (same file, same service):

```protobuf
service CDCService {
  // ... existing RPCs ...
  rpc StreamWAL(StreamWalRequestPB) returns (StreamWalResponsePB);
}
```

**Semantics:** unary, leader-only, per-tablet. One call returns one batch of
**committed, decoded** change records starting strictly after `from_op_id`,
plus a `next_op_id` cursor for the next call. The server transparently joins
WAL `APPLYING` records to IntentsDB intent data — the client sees a single
linear stream of committed events.

---

## Cursor

```protobuf
// Resume cursor for StreamWAL. Identifies a unique resumption point on the
// leader: a WAL OpId, optionally augmented with a mid-APPLYING continuation
// position when a single transaction's intents could not fit in one batch.
//
// Three meaningful values in a request:
//   {term: 0,  index: 0}  -> "from start of retained WAL"; server emits
//                            synthetic bootstrap DDLs before any real records.
//                            intent_key / intent_write_id MUST be unset.
//   {term: -1, index: -1} -> SKIP-TO-TIP sentinel. Server emits synthetic
//                            bootstrap DDLs only; sets next_op_id to the
//                            current leader tip. No historical records.
//                            intent_key / intent_write_id MUST be unset.
//   {term: T,  index: I}  -> "records strictly after OpId(T, I)"; no bootstrap.
//                            T must be >= 1, I must be >= 0. Caller must
//                            already have schema cache for any schema_version
//                            active between (0,0) and (T,I).
//
// In the response, next_op_id is always a real (T, I) value (never a sentinel).
//
// PARTIAL-APPLYING RESUMPTION:
//
//   When a single UPDATE_TRANSACTION_OP { APPLYING } at OpId(T, I) produces
//   more intent records than fit in one batch (bounded by max_records /
//   max_bytes), the server returns:
//
//     next_op_id = {term: T, index: I,
//                   intent_key: <docdb reverse-index key of the LAST intent
//                                already emitted in this batch>,
//                   intent_write_id: <IntraTxnWriteId of that intent>}
//
//   The (term, index) are the APPLYING op's own OpId -- the cursor has NOT
//   advanced past the APPLYING. On the next call the server re-reads the
//   APPLYING WAL entry (cheap: single op fetch by reading from index - 1),
//   resumes the IntentsDB scan ONE POSITION PAST (intent_key, intent_write_id),
//   and continues emitting intent records. When intent_key / intent_write_id
//   are unset (the empty / zero values), the APPLYING is fully drained and
//   the cursor effectively means "strictly after (T, I)".
//
//   Clients pass `next_op_id` back as `from_op_id` verbatim; they do not
//   inspect intent_key / intent_write_id. The fields are encoded on the
//   cursor only so the server can be stateless across calls.
message StreamWalCursorPB {
  optional int64 term = 1;
  optional int64 index = 2;

  // Mid-APPLYING resume fields. Both MUST be unset together (the common
  // case) or both MUST be set together (resuming a spilled APPLYING).
  // Servers reject mixed states with INVALID_REQUEST.
  //
  // intent_key is the bytes form of docdb::ApplyTransactionState::key (the
  // reverse-index key into IntentsDB). intent_write_id is the
  // IntraTxnWriteId at which the prior batch stopped. The server resumes
  // the scan one position past this point.
  optional bytes  intent_key      = 3;
  optional uint32 intent_write_id = 4;
}
```

The cursor is **per-tablet**, advances monotonically within a tablet, and is
leader-independent (the underlying Raft OpId space is the same across replicas;
IntentsDB content is also replicated, so the intent_key / intent_write_id
position is meaningful on any replica's IntentsDB after the APPLYING has been
durably replicated). Cursor granularity is at WAL `ReplicateMsg` boundaries for
non-transactional ops, and at intent-record boundaries within a spilled
APPLYING.

---

## Request

```protobuf
message StreamWalRequestPB {
  // REQUIRED. The tablet to stream from.
  optional bytes tablet_id = 1;

  // REQUIRED. See StreamWalCursorPB for the valid shapes.
  optional StreamWalCursorPB from_op_id = 2;

  // Soft cap on records returned in this batch. Default 1000. Server may
  // return fewer. Server never splits a single non-transactional WAL op
  // across batches even if it produces more than max_records RowMessages.
  // Transactional APPLYING ops MAY span batches via the partial-APPLYING
  // cursor (see StreamWalCursorPB); when they do, the response always ends
  // on an intent boundary, never mid-row.
  optional uint32 max_records = 3 [default = 1000];

  // Soft cap on response bytes (decoded record payload). Default 4 MiB.
  // Server defers the next record to the next batch if including it would
  // exceed max_bytes. Records are never truncated.
  optional uint64 max_bytes = 4 [default = 4194304];

  // Additional server-side wait budget *on top of* the WAL reader's
  // mandatory baseline (cdcsdk_wal_reads_deadline_buffer_secs + 1 seconds;
  // default ~6 s). The effective server deadline is:
  //   now + (cdcsdk_wal_reads_deadline_buffer_secs + 1) + deadline_ms
  //
  // 0 -> short-poll: return on the baseline budget; do NOT wait for new
  //     ops to arrive past the leader tip. Empty batch + unchanged cursor
  //     + has_more=false if there is nothing to read.
  // N -> long-poll: server may block up to an additional N ms beyond the
  //     baseline waiting for new entries.
  // Default: 0.
  optional uint32 deadline_ms = 5 [default = 0];
}
```

There is no `stream_id`, no `db_stream_id`, no `table_id` filter, no checkpoint
type, no record format, no request source. The connector is stateless from the
cluster's perspective; all per-tablet state lives in the cursor.

---

## Response

```protobuf
message StreamWalResponsePB {
  // Error envelope. On error, only `error` and (for LEADER_NOT_READY)
  // `tablet_consensus_info` are populated. All other fields are unset.
  optional CDCErrorPB error = 1;

  // Decoded records in strict committed order:
  //   - Synthetic bootstrap DDLs (when applicable) -- see "Synthetic DDL
  //     bootstrap" below.
  //   - Per-WAL-op records in WAL order: (cdc_sdk_op_id.term,
  //     cdc_sdk_op_id.index, cdc_sdk_op_id.write_id) monotonically
  //     increasing.
  //
  // Transactional WRITE_OPs are NEVER emitted; the corresponding row records
  // appear at the APPLYING op's (term, index), sandwiched between
  // RowMessage{BEGIN} and RowMessage{COMMIT}, all stamped with
  // commit_hybrid_time.
  repeated CDCSDKProtoRecordPB records = 2;

  // REQUIRED on success (including empty success batches). Cursor to pass
  // as from_op_id on the next call.
  //   - If `records` is non-empty and the batch ended on a non-transactional
  //     op or on a fully-drained APPLYING:
  //         next_op_id = (last_consumed_op.term, last_consumed_op.index),
  //         intent_key / intent_write_id unset.
  //   - If `records` is non-empty and the batch ended mid-APPLYING (the
  //     server stopped before emitting all intent records for a single
  //     APPLYING):
  //         next_op_id = (applying_op.term, applying_op.index,
  //                       intent_key = <last intent emitted's reverse-index key>,
  //                       intent_write_id = <last intent emitted's write_id>).
  //   - If `records` contains only synthetic bootstrap DDLs (either the
  //     skip-to-tip sentinel path OR a from-start request whose tablet had
  //     no consumable WAL traffic yet),
  //         next_op_id = leader_tip_op_id at the time of the bootstrap;
  //         intent_key / intent_write_id unset.
  //   - If `records` is empty, next_op_id == the request's from_op_id
  //     verbatim (no progress).
  optional StreamWalCursorPB next_op_id = 3;

  // REQUIRED on success. Last replicated OpId on the leader at the moment
  // this response was generated. Use for:
  //   - Per-tablet lag computation (e.g. leader_tip.index - next_op_id.index).
  //   - "Caught up" detection (next_op_id.index == leader_tip.index AND
  //     next_op_id.intent_key unset).
  // intent_key / intent_write_id are never set on leader_tip_op_id.
  optional StreamWalCursorPB leader_tip_op_id = 4;

  // Populated on success when the leader's safe-time computation
  // (Tablet::SafeTime) succeeds. May be unset in rare cases (e.g. tablet
  // still propagating safe time, transient SafeTime failure); the server
  // logs a WARNING in that case. Clients that consume this field for
  // downstream SAFEPOINT / heartbeat emission should treat absence as a
  // "no update" signal, not an error.
  optional fixed64 leader_safe_hybrid_time = 5;

  // True iff more data is immediately available past next_op_id. When
  // false, the client should backoff before the next call. (When true,
  // the client can call again immediately without backoff.)
  //
  // has_more is set to true whenever next_op_id carries intent_key /
  // intent_write_id (a spilled APPLYING is by definition "more is available").
  optional bool has_more = 6;

  // Populated only when error.code == LEADER_NOT_READY. Lets the client
  // refresh its meta-cache without going to master.
  optional yb.tserver.TabletConsensusInfoPB tablet_consensus_info = 7;
}
```

---

## Records

Records on the wire are existing `CDCSDKProtoRecordPB` messages, produced by
the existing `Populate*` decoder family in `cdcsdk_producer.cc`. Two optional
envelope fields are added:

```protobuf
message CDCSDKProtoRecordPB {
  optional RowMessage row_message = 1;
  optional CDCSDKOpIdPB cdc_sdk_op_id = 2;
  optional CDCSDKOpIdPB from_op_id = 3;

  // Populated only on COMMIT envelope records for committed multi-shard
  // transactions (those emitted at an APPLYING WAL entry). Mirrors
  // TransactionStatePB.aborted from the source WAL entry. Lists subtxn IDs
  // that were rolled back via ROLLBACK TO SAVEPOINT inside the committing
  // transaction.
  //
  // Per-row DML records for committed transactions have ALREADY been
  // filtered against this set server-side (via PopulateCDCSDKIntentRecord).
  // The field is preserved on the COMMIT envelope for observability /
  // debugging only; clients do not need to re-filter.
  optional SubtxnSetPB aborted_subtxn_set = 4;

  // Populated only on the terminal SPLIT_OP record. Carries the
  // SplitTabletRequestPB payload (new_tablet1_id, new_tablet2_id,
  // split_partition_key, split_encoded_key) so the client can resolve and
  // resume on the child tablets. See "Tablet split signaling" below.
  optional tablet.SplitTabletRequestPB split_tablet_request = 5;
}
```

### Per-`OperationType` emission rules

For each `ReplicateMsg` read from the WAL between `from_op_id` (exclusive,
modulo mid-APPLYING resumption) and the response boundary, the server emits
records as follows:

| WAL `OperationType` | Decoder used | Records emitted |
|---|---|---|
| `WRITE_OP` (no `transaction` field) | `PopulateCDCSDKWriteRecord` | A `RowMessage{BEGIN}`, one `RowMessage{INSERT \| UPDATE \| DELETE}` per logical row, and a `RowMessage{COMMIT}`. `commit_time = ReplicateMsg.hybrid_time` on all three (single-shard writes commit at intent time). `transaction_id` unset. `primary_key` populated on the DML records. |
| `WRITE_OP` (with `transaction` field) | — | **None.** Silently skipped on the wire. The cursor advances past the op. The corresponding row records are emitted later, at the matching APPLYING op's OpId, sandwiched between BEGIN and COMMIT envelopes. |
| `UPDATE_TRANSACTION_OP { APPLYING }` | `tablet->GetIntentsForCDC(...)` → `PopulateCDCSDKIntentRecord(...)` + envelope assemblers | A `RowMessage{BEGIN}`, one `RowMessage{INSERT \| UPDATE \| DELETE}` per surviving intent (server-side filtered against `TransactionStatePB.aborted`), and a `RowMessage{COMMIT}`. All records carry `commit_time = commit_hybrid_time` and `transaction_id` (UUID-string form). `xrepl_origin_id` is set on the COMMIT envelope when present on `TransactionStatePB`. `aborted_subtxn_set` carried on the COMMIT envelope. `primary_key` populated on DML records. If the intents do not fit in this batch, the server stops at an intent boundary and returns the partial-APPLYING cursor; the COMMIT envelope is NOT emitted until the final batch. |
| `UPDATE_TRANSACTION_OP { PROMOTING }` | — | None (silent cursor advance). |
| `CHANGE_METADATA_OP` | `PopulateCDCSDKDDLRecord` | One `RowMessage{DDL}` with `schema`, `schema_version`, `new_table_name`, `pgschema_name`, `table_id`, `commit_time = ReplicateMsg.hybrid_time` populated. |
| `TRUNCATE_OP` | `PopulateCDCSDKTruncateRecord` | One `RowMessage{TRUNCATE}` with `table_id` populated. (The legacy `truncate_request_info` field is left unset by the existing CDCSDK decoder.) |
| `SPLIT_OP` | `PopulateStreamWalSplitRecord` | One terminal record with `row_message.op = UNKNOWN`, `row_message.commit_time = ReplicateMsg.hybrid_time`, and `CDCSDKProtoRecordPB.split_tablet_request` carrying the `SplitTabletRequestPB` payload. Always the **last** non-synthetic record emitted on this tablet's stream. The cursor advances to the SPLIT_OP's OpId. The next call returns `error = TABLET_SPLIT`. SPLIT_OPs whose `split_request.tablet_id` does not match the polled tablet are silent. |
| `NO_OP`, `CHANGE_CONFIG_OP`, `SNAPSHOT_OP`, `HISTORY_CUTOFF_OP`, `CHANGE_AUTO_FLAGS_CONFIG_OP`, `CLONE_OP`, `UNKNOWN_OP` | — | None (silent cursor advance). |

### Field-level invariants

- **`cdc_sdk_op_id`** is populated on every emitted record.
  - For records emitted at a non-transactional `WRITE_OP`, `write_id` ranges
    over [0, N) where N is the number of `write_pairs` in the batch.
  - For BEGIN/COMMIT envelope records emitted at an APPLYING op, `write_id = 0`.
  - For per-row DML records emitted from intents at an APPLYING op,
    `write_id = IntentKeyValueForCDC.write_id` (the IntraTxnWriteId baked into
    the IntentsDB key); this preserves intra-txn write ordering.
- **`xrepl_origin_id`** propagation:
  - Single-shard writes: `WritePB.xrepl_origin_id` → `RowMessage{COMMIT}`
    envelope at end-of-batch (via `FillCommitRecordForSingleShardTransaction`).
    `RowMessage{BEGIN}` for single-shard writes does NOT carry it.
  - Transactional writes: `TransactionStatePB.xrepl_origin_id` →
    `RowMessage{COMMIT}` envelope only (via `FillCommitRecord`).
    `RowMessage{BEGIN}` for transactional writes does NOT carry it.
  - Per-row INSERT/UPDATE/DELETE records do not carry `xrepl_origin_id`.
- **`primary_key`** is the encoded DocKey on DML row messages, suitable for
  client-side range filtering against `split_encoded_key`.
- **`from_op_id`** on `CDCSDKProtoRecordPB` echoes the request's
  `(from_op_id.term, from_op_id.index)` (preserved for compatibility with
  existing decoder callsites; not load-bearing for clients). The mid-APPLYING
  cursor fields are NOT echoed onto records.
- **`transaction_id`** when populated is the UUID-string form
  (e.g. `"01234567-89ab-cdef-..."`), matching the form existing CDCSDK
  BEGIN/COMMIT envelope records use.
- **Subtxn rollback filtering** happens server-side at intent decode time
  (`PopulateCDCSDKIntentRecord` honors the `aborted` subtxn set). Per-row DML
  records the client receives have already had aborted-subtxn intents removed.

---

## Synthetic DDL bootstrap

### When the server emits bootstrap records

| `from_op_id` value | Server emits bootstrap? | Server emits real WAL records? |
|---|---|---|
| `{0, 0}` | Yes | Yes (starting at earliest retained OpId) |
| `{-1, -1}` (skip-to-tip) | Yes | **No** |
| `{T, I, ...}` (T ≥ 1, I ≥ 0) | No | Yes |

### What bootstrap records look like

One synthetic `CDCSDKProtoRecordPB` per `(table_id, current_schema_version)`
derived from `tablet->metadata()`. Each carries:

- `row_message.op = DDL`
- `row_message.schema`, `row_message.schema_version`, `row_message.table`
  (table name), `row_message.table_id`, `row_message.pgschema_name` — from
  current metadata
- `cdc_sdk_op_id = { term: 0, index: 0, write_id: i }` where `i` is the
  bootstrap record's index in the batch (disambiguator across colocated tables)
- All other fields unset (notably `commit_time` is not stamped on bootstrap
  records; they are synthesized from current metadata, not from a real
  `ReplicateMsg`)

Parent tablegroup / colocation tables and sys-catalog tablets are excluded from
bootstrap; their schemas are either not user-visible or resolved per-row by the
decoder at runtime.

Bootstrap records appear at the **front** of `records[]`, before any real WAL
records in the same batch.

### Skip-to-tip response shape

For `from_op_id = {-1, -1}`:

- `records[]` contains only synthetic bootstrap DDLs.
- `next_op_id = leader_tip_op_id` (computed once during the call).
- `has_more = false` if no new entries arrived during the call; `true` if they
  did.
- Subsequent calls with `from_op_id = leader_tip_op_id` proceed normally and
  never re-emit bootstrap.

### From-start with no consumable WAL traffic

For `from_op_id = {0, 0}` against a tablet whose retained WAL contains only
silent ops (or is otherwise empty after bootstrap synthesis):

- `records[]` contains only synthetic bootstrap DDLs.
- `next_op_id = leader_tip_op_id` at call time (parallel to the skip-to-tip
  path — the client must not re-issue `{0,0}` against the same tablet, or
  bootstrap will be re-synthesized).

### Schema cache invariant

The server guarantees: any client that consumes responses in cursor order,
starting from `{0, 0}` or `{-1, -1}`, will receive every `schema_version`
referenced by subsequent records before that schema_version is referenced.
This holds because:

1. The bootstrap covers all schemas active at stream start.
2. Every `CHANGE_METADATA_OP` in the WAL is emitted in order as a
   `RowMessage{DDL}`.
3. WAL ordering guarantees the `CHANGE_METADATA_OP` defining schema_version N
   precedes any committing `APPLYING` whose intents reference schema_version N
   (the schema change is itself a Raft op interleaved with WRITE_OPs and
   APPLYING ops).

Clients that send `from_op_id = {T, I, ...}` with T ≥ 1 are asserting they
already hold the schema cache for `[(0,0), (T,I)]`. The server does no
re-bootstrap in this case.

---

## At-least-once and replay semantics

- Re-issuing the same `from_op_id` (including the mid-APPLYING cursor fields)
  after a crash returns the same logical records (same `cdc_sdk_op_id`s,
  byte-identical `RowMessage` payloads for real WAL+intent records). Bootstrap
  records may differ if the tablet schema changed between calls.
- The server may re-deliver records the client already saw if the client
  persists `next_op_id` non-atomically with downstream emission. Downstream
  dedup via Kafka idempotent producer keyed on
  `(tablet_id, cdc_sdk_op_id.term, cdc_sdk_op_id.index, cdc_sdk_op_id.write_id)`
  is the contract assumption.
- The server does not deliver records out of committed order.
- The server does not skip committed records — provided the IntentsDB
  retention barrier has been maintained correctly (see "Retention coupling"
  below). Failure to do so results in `INTENTS_GC_ERROR`.
- The server does not deliver records for aborted transactions.

---

## Error taxonomy

Reuses `CDCErrorPB::Code` with one new code: `INTENTS_GC_ERROR`.

| Code | When server returns it | What the client should do |
|---|---|---|
| `TABLET_NOT_FOUND` | This tserver doesn't host the tablet, or tablet was deleted (including post-split cleanup) | Re-resolve via `GetTabletLocations(tablet_id)` against master. |
| `LEADER_NOT_READY` | Not leader for this tablet, or leader is bootstrapping/transitioning | Use `tablet_consensus_info` (populated on this response) to find the current leader; retry there. |
| `TABLET_NOT_RUNNING` | Tablet exists but is not in RUNNING state | Backoff and retry. |
| `CHECKPOINT_TOO_OLD` | `from_op_id.index < cdc_min_replicated_index` (WAL has been GC'd past this cursor) | Halt, alert. Recovery: send `from_op_id = {0,0}` or `{-1,-1}` (loses data between the two points). |
| `INTENTS_GC_ERROR` | The cursor resolves to an APPLYING (either via partial-APPLYING resumption or via a normal WAL read where the APPLYING comes from the requested range) but `tablet->GetIntentsForCDC` returns zero intents AND the APPLYING's `op_id <= tablet_peer->GetLatestCheckPoint()`. The classic "intents have been GC'd before we could read them" condition (the same condition `ProcessIntents` checks today at `cdcsdk_producer.cc:1779`). | Halt, alert. This is unrecoverable data loss for the affected transaction. Recovery: send `from_op_id = {0,0}` or `{-1,-1}` to restart from scratch (loses all data up to that point). **Operator action:** investigate why post-apply intent cleanup ran before the connector caught up. In the time-based-retention design, this means `intents_min_seconds_to_retain` was shorter than the observed connector lag for at least one transaction. Raise the flag, or alert on connector lag approaching `intents_min_seconds_to_retain - safety_margin`. |
| `TABLET_SPLIT` | Tablet has been split; no more records will arrive on this stream | Client already received the SPLIT_OP record in a prior batch — use the `SplitTabletRequestPB` payload's `new_tablet1_id` / `new_tablet2_id` and open StreamWAL on each child. |
| `INVALID_REQUEST` | Malformed request: missing `tablet_id`, missing `from_op_id`, `from_op_id` shape outside the valid forms, `from_op_id.index > leader_tip_op_id.index`, or mixed/partial mid-APPLYING fields (one of `intent_key` / `intent_write_id` set without the other). Also returned when a mid-APPLYING resume cursor points at an OpId whose op is not an `UPDATE_TRANSACTION_OP { APPLYING }` (cursor is misdirected, e.g. fabricated or pointed at a Raft re-write). | Bug; do not retry without fixing. |
| `INTERNAL_ERROR` | Decode failure, WAL read failure, IntentsDB read failure other than the "GC'd" condition above (e.g. RocksDB I/O error), or a mid-APPLYING resume whose targeted OpId is no longer accessible (e.g. WAL was re-written under the client). | Retry with exponential backoff; alert if persistent. |

`INTENTS_GC_ERROR` is added to the proto enum as:

```protobuf
enum Code {
  // ... existing codes ...
  INTENTS_GC_ERROR = 15;
}
```

---

## Retention coupling

Because the server reads intents from IntentsDB at APPLYING time, IntentsDB
content must outlive the lag between APPLYING-on-WAL and
APPLYING-streamed-to-client. The design uses **two wall-clock gflags** to
gate retention; the connector does not heartbeat any checkpoint barrier.

| Subsystem | gflag | What it does |
|---|---|---|
| WAL | `log_min_seconds_to_retain` (existing) | A WAL segment is not GC'd until its wall-clock age exceeds this value. Already governed WAL retention; we now rely on it directly without the CDC-specific extensions (`cdc_wal_retention_ms`, lease aggregation). |
| IntentsDB | `intents_min_seconds_to_retain` (new, runtime, default 0) | If > 0, post-apply intent cleanup for a committed transaction is deferred until the wall-clock age of the transaction's apply hybrid time exceeds this many seconds. Default 0 preserves existing behavior (immediate cleanup post-apply); the StreamWAL connector sets this > 0 to match `log_min_seconds_to_retain`. Implementation: `src/yb/tablet/transaction_participant.cc:121` and the `ScheduleRemoveIntents` gating path. |

The contract for the connector is:

- Set `intents_min_seconds_to_retain` and `log_min_seconds_to_retain` so that
  both exceed the worst-case connector outage you are willing to tolerate
  without data loss.
- Do NOT send `UpdateCdcReplicatedIndex` — the legacy lease path is not part
  of this consumer's retention model. (The tserver RPC itself still exists
  for backwards compatibility with the legacy connector, but StreamWAL does
  not depend on it.)
- Monitor connector lag and alert when it approaches either retention floor.

**No multi-consumer caveat.** The time-based design retains intents and WAL
for *every* consumer equally, regardless of how many independent streams
exist on a tablet — there is no per-consumer barrier to silently advance
past a slower peer.

`INTENTS_GC_ERROR` is still emitted if intents *are* cleaned up before the
connector reads them — that means `intents_min_seconds_to_retain` was set
too low for the observed lag. See the error taxonomy table.

---

## Tablet split signaling

The SPLIT_OP record is the wire-level signal that a tablet has terminated. Its
payload (via `tablet.SplitTabletRequestPB` carried on
`CDCSDKProtoRecordPB.split_tablet_request`) contains exactly:

- `new_tablet1_id`, `new_tablet2_id` — child tablet IDs (the proto is a fixed
  2-way split).
- `split_partition_key` — partition-key boundary between the two children.
- `split_encoded_key` — encoded DocKey boundary, directly comparable against
  `RowMessage.primary_key`.

The server guarantees:

- The SPLIT_OP record is the last non-synthetic record emitted on this tablet's
  stream.
- `next_op_id` in the response containing SPLIT_OP equals the SPLIT_OP's OpId.
- Any subsequent call with `from_op_id` ≥ SPLIT_OP's OpId returns
  `error = TABLET_SPLIT`. The server also short-circuits to `TABLET_SPLIT` as
  soon as `tablet_data_state == TABLET_DATA_SPLIT_COMPLETED`, even before any
  cursor would naturally land past the SPLIT_OP.
- After the parent tablet is cleaned up (`CanBeDeleted` true on all replicas,
  then `DeleteNotServingTablet`), calls return `error = TABLET_NOT_FOUND`.
  Clients that missed the SPLIT_OP can recover the child set via
  `GetTabletLocations(parent_id)` against master.

**Split-with-pending-transactions:** when a tablet splits while a multi-shard
transaction is pending, the parent's WAL ends with SPLIT_OP and the children
inherit the intents (this is how YB itself works — nothing CDC-specific). On
the parent's stream the connector sees the SPLIT_OP and stops. On each child's
stream the connector sees the APPLYING in due course and the server reads the
inherited intents from the child's IntentsDB. No buffer handoff is required.

The contract does not specify what the client does between SPLIT_OP and
resuming on children. That's a connector design concern.

---

## Cursor edge cases

| Situation | Server behavior |
|---|---|
| `from_op_id` is exactly the leader tip (no mid-APPLYING fields) | Empty `records[]`; `next_op_id = from_op_id`; `has_more = false`. |
| `from_op_id` carries mid-APPLYING fields and `(term, index)` is exactly the leader tip | Server re-reads the APPLYING WAL entry at `(term, index)` (by reading the WAL from `(term, index - 1)`), resumes IntentsDB scan one position past `(intent_key, intent_write_id)`, emits remaining intent records + COMMIT envelope. `next_op_id.term/index` advance past the APPLYING; mid-APPLYING fields cleared on the response. |
| `from_op_id.index < cdc_min_replicated_index` (rewind past WAL GC) | `error = CHECKPOINT_TOO_OLD`. |
| `from_op_id` carries mid-APPLYING fields and the underlying IntentsDB has been GC'd past `intent_key` | `error = INTENTS_GC_ERROR`. |
| `from_op_id.index > leader_tip_op_id.index` (caller ahead of leader, e.g. after stale failover) | `error = INVALID_REQUEST`. |
| `from_op_id.term < 0` and not `{-1,-1}` | `error = INVALID_REQUEST`. |
| `from_op_id.term == 0` and `from_op_id.index > 0` (or any other shape outside the valid forms) | `error = INVALID_REQUEST`. |
| `from_op_id` carries `intent_key` set but `intent_write_id` unset, or vice versa | `error = INVALID_REQUEST`. |
| `from_op_id` carries mid-APPLYING fields but `(term, index)` is `{0,0}` or `{-1,-1}` | `error = INVALID_REQUEST`. (Mid-APPLYING resumption is meaningless without a concrete APPLYING op to resume.) |
| `from_op_id` carries mid-APPLYING fields and the op at `(term, index)` is *not* an `UPDATE_TRANSACTION_OP { APPLYING }` (cursor misdirected; e.g. Raft rewrote the index) | `error = INVALID_REQUEST`. |
| `from_op_id` carries mid-APPLYING fields and the op at `(term, index)` IS an APPLYING but its concrete `(term, index)` differs from what the cursor claims (leader changed the WAL out from under us) | `error = INTERNAL_ERROR`. |
| `from_op_id = {0, 0}` and WAL has been GC'd | Bootstrap is emitted from current `tablet->metadata()`; real WAL records start at the earliest retained OpId. Schema cache invariant still holds. |
| `from_op_id = {-1, -1}` repeated across calls | Server re-bootstraps on every call. Cheap (O(tables)) but wasteful. The contract doesn't forbid this — it's a client misuse pattern. |
| `from_op_id = {0, 0}` and the tablet has no consumable WAL traffic (only silent ops) | Bootstrap is emitted; `next_op_id = leader_tip_op_id` at call time. |

---

## What the contract does NOT include (and why)

These are deliberately omitted:

- **No flag for whether to emit transactional WRITE_OPs.** They are always
  skipped. This is the committed-events-only RPC; intent-time emission is
  intentionally not exposed.
- **No client-side buffering / topology log / pending-txn reconciliation
  guidance.** N/A — the server only emits committed records.
- **No long-poll behavior beyond the `deadline_ms` field.** Default is
  short-poll; long-poll adds to the WAL reader's baseline budget via that
  field.
- **No multi-tablet multiplexing.** One RPC, one tablet.
- **No server-side `table_ids[]` filter for colocated tablets.** Client filters
  locally.
- **No snapshot phase.** Out of scope per project design.
- **No PostgreSQL replication slot or LSN semantics.** `pg_lsn` and
  `pg_transaction_id` on `RowMessage` remain unset.
- **No before-image / replica identity FULL.** Out of scope per project design.
- **No xCluster `auto_flags_config_version` negotiation.** Not relevant.
- **No retention heartbeats.** Retention is wall-clock-only; see "Retention
  coupling".

Each of these can be added as a strictly-additive proto field later without
breaking existing clients.

---

## Proto changes summary

**File: `src/yb/cdc/cdc_service.proto`**

1. Add the `StreamWAL` RPC to `service CDCService`:

   ```protobuf
   rpc StreamWAL(StreamWalRequestPB) returns (StreamWalResponsePB);
   ```

2. Add three new top-level messages: `StreamWalCursorPB`,
   `StreamWalRequestPB`, `StreamWalResponsePB` (as defined above).

3. Add two fields to the existing `CDCSDKProtoRecordPB`:

   ```protobuf
   optional SubtxnSetPB aborted_subtxn_set = 4;
   optional tablet.SplitTabletRequestPB split_tablet_request = 5;
   ```

4. Add one new error code to `CDCErrorPB.Code`:

   ```protobuf
   INTENTS_GC_ERROR = 15;
   ```

---

## Appendix: client implementation notes 


- **`intent_key` is opaque binary.** It is the bytes form of
  `docdb::ApplyTransactionState::key` — an arbitrary, non-UTF8 reverse-index
  key. Clients that persist the cursor through Kafka Connect's
  `Map<String, Object>` source-offset store must Base64-encode it on write
  and decode it on read. Stringifying the bytes directly will silently
  corrupt the cursor.
- **The two mid-APPLYING fields are a paired group.** `intent_key` and
  `intent_write_id` must be persisted atomically — never one without the
  other. Sending a cursor with only one of them set is rejected with
  `INVALID_REQUEST`.
- **`deadline_ms` budgeting.** Because the WAL reader takes a mandatory
  baseline budget on top of `deadline_ms`, a tight client-side RPC timeout
  must allow for at least `cdcsdk_wal_reads_deadline_buffer_secs + 1 + deadline_ms`
  seconds (the default baseline is ~6 s). Setting client deadlines below
  that will manifest as `DeadlineExceeded` even on an idle, healthy
  tablet.
- **Retention monitoring.** The design has no per-stream retention barrier
  and no master-side aggregation; the only safety valve is
  `intents_min_seconds_to_retain` / `log_min_seconds_to_retain`. Per-tablet
  lag (`leader_tip_op_id.index - next_op_id.index`, plus wall-clock since
  last successful drain) should be alerted on with thresholds well below
  those gflag values.

