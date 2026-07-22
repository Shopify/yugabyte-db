# OSC Prototype A: logical-replication-slot shadow mirror

Roadmap validation harness for online schema changes via async WAL/CDC
mirroring into a shadow table. See
`architecture/design/online-schema-changes-async-shadow-roadmap.md`.

This is throwaway validation glue, not production code.

## What it validates

The end-to-end thin slice for a regular, non-colocated table with a primary key:

1. Create source `G0`, seed rows, set `REPLICA IDENTITY FULL`.
2. Create a logical replication slot; its consistent point is the snapshot
   boundary `S`.
3. Create shadow `G1` with the target (post-ALTER) schema.
4. Backfill: copy the source snapshot into `G1` applying a deterministic
   transform (`+c2 DEFAULT 0`, `+note = 'v='||v`).
5. Mirror: `pg_recvlogical` with `test_decoding` streams post-`S` changes.
6. Run concurrent INSERT/UPDATE/DELETE on the source.
7. Drain the mirror, translate changes, apply to `G1` (idempotent upsert/delete).
8. Cutover via a brief `ACCESS EXCLUSIVE` lock + name swap.
9. Assert exact parity: live table == `transform(old source)`, 0 missing/extra.

## Run

Requires a running local cluster (see `/tmp/claude/osc-dev-env.md`):

```
PATH="/usr/bin:/bin:$PATH" ./bin/yb-ctl --binary_dir "$(pwd)/build/latest" create --rf 1
PATH="/usr/bin:/bin:$PATH" bash prototype/osc-logical-slot/mirror_harness.sh
```

Tunables: `SEED_ROWS` (default 2000), `CONCURRENT_OPS` (default 500).

## Result (2026-07-21)

`mirror_harness.sh` (1:1, single tablet-set): PASS. 2000 seed rows + 1000
concurrent ops (500 insert / 250 update / 250 delete); 3008 change lines
captured; final parity 2250 == 2250, 0 mismatched, 0 extra.

`mirror_harness_nm.sh` (N:M distributed): PASS. Source SPLIT INTO 4 tablets,
shadow SPLIT INTO 6 tablets, PK-changing transform (`new_id = id*2`). 4000 seed
rows + 100 multi-row cross-tablet transactions (8 rows each). Transformed rows
fanned out across all 6/6 shadow tablets. Source BEGIN framing preserved into
100 atomic target transactions. Final parity 4700 == 4700, 0 missing, 0 extra;
exactly 800 inserted rows (100x8) confirms transaction atomicity.

This validates the DISTRIBUTED shape: a "single shadow DocDB table" is really a
multi-tablet distributed table, source and target tablet layouts need not match,
and one source tablet's rows route to many target tablets by the transformed
primary key.

`mirror_harness_restart.sh` (Step 2: exactly-once across restart): PASS.
Streaming applier consumes the slot, applies each SOURCE transaction (keyed by
xid) together with an idempotency-ledger row `(slot, xid)` in ONE target
transaction. Pass 1 captures changes WITHOUT sending feedback and is killed
mid-stream (4 commits applied, ledger=4). Because pass 1 never acked, the slot
REPLAYS everything in pass 2 (8 commits observed, including pass 1's 4); the
ledger guard makes the replayed xids no-ops, so ledger ends at 8 rows with 0
duplicates and final parity is exact (2200 == 2200). A focused guard test also
confirms a replayed xid with a different payload is fully skipped (mutations
included), not just the ledger insert.

This validates exactly-once EFFECT on top of at-least-once transport: the
`(slot, xid)` ledger committed in the same target transaction as the mutations
survives an applier restart mid-stream.

`mirror_harness_barrier.sh` (Step 3: real barrier drain, no sleep): PASS.
A continuous background writer mutates the source (produced a ~212-row tail in a
run). To finalize, the harness takes `ACCESS EXCLUSIVE` on the source (fences
new writes), inserts a unique BARRIER sentinel row, then drains the slot and
applies it, polling until the sentinel's TRANSFORMED row appears in the shadow.
That sentinel visibility is the deterministic "mirror caught up" signal that
replaces the earlier fixed `sleep`. After cutover, parity is exact including the
full writer tail (2212 == 2212, 0 missing/extra, 0 ledger dupes).

Notes from building Step 3:
- Do NOT spawn many short-lived `pg_recvlogical` consumers in a tight loop; each
  leaves the slot `active` for a lease window and the next start captures
  nothing. Drain once after the barrier (the slot retains all unacked history);
  a small bounded retry only rides out a transient slot-active window.
- The transform `new_id = id*2` must target a `bigint` column and cast
  (`id::bigint*2`); doubling a large `int` source id overflows `int4`.

## Validated primitives

- `wal_level = logical`; slots via `pg_create_logical_replication_slot`.
- Output plugins present: `test_decoding`, `pgoutput`, `yboutput`.
- `pg_recvlogical` streams ordered, transaction-framed changes.
- `REPLICA IDENTITY FULL` yields full old images on UPDATE/DELETE.

## Known gaps (intentionally faked; next iterations)

- Cutover is a NAME SWAP in these harnesses. NOTE: the OID-preserving storage
  switch itself is validated separately in-tree by
  `src/yb/yql/pgwrapper/pg_online_schema_change-test.cc` (roadmap Step 1);
  wiring it as the harness cutover is future work.
- Drain: `mirror_harness_barrier.sh` (Step 3) uses a real sentinel-based
  caught-up check. The earlier harnesses still use a fixed sleep for brevity.
- Idempotency ledger keys on source `xid` (from test_decoding) rather than a
  commit LSN, because the SQL query API (`pg_logical_slot_get_changes`) that
  exposes LSN is gated behind a preview flag that did not take effect in this
  environment. `(slot, xid)` is sufficient for the exactly-once demonstration.
- Single node, single tablet-set. No tablet splits, partitions, geo, colocation.
- No external-CDC handoff; no backup/PITR; no master-owned job/barrier yet.

## Operational note

YB logical slots can linger `active=t` briefly after the consumer exits (lease
timeout), so slot drop needs retry-with-backoff. The harness handles this in
`drop_slot_retry`.

## Files

- `mirror_harness.sh`         - 1:1 single-tablet flow; asserts parity.
- `mirror_harness_nm.sh`      - N:M distributed flow; PK-changing transform,
  multi-row cross-tablet txns; asserts fan-out + atomicity + parity.
- `mirror_harness_restart.sh` - Step 2: kills the applier mid-stream and
  restarts; asserts exactly-once effect (ledger dedupe) + parity.
- `mirror_harness_barrier.sh` - Step 3: continuous background writer + real
  sentinel-based barrier drain (no sleep); asserts the write tail is caught and
  parity is exact.
- `apply_changes.py`          - parses `test_decoding` output into shadow apply
  SQL, preserving source BEGIN/COMMIT framing into atomic target transactions.
- `streaming_applier.py`      - Step 2 applier: per-source-xid target
  transaction with a `(slot, xid)` idempotency-ledger guard (DO-block early
  return makes a replayed xid a full no-op).

## Run N:M

```
PATH="/usr/bin:/bin:$PATH" bash prototype/osc-logical-slot/mirror_harness_nm.sh
```

Tunables: `SRC_TABLETS` (4), `SHADOW_TABLETS` (6), `SEED_ROWS` (4000),
`TXNS` (100), `ROWS_PER_TXN` (8).
