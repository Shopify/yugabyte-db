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

## Validated primitives

- `wal_level = logical`; slots via `pg_create_logical_replication_slot`.
- Output plugins present: `test_decoding`, `pgoutput`, `yboutput`.
- `pg_recvlogical` streams ordered, transaction-framed changes.
- `REPLICA IDENTITY FULL` yields full old images on UPDATE/DELETE.

## Known gaps (intentionally faked; next iterations)

- Cutover is a NAME SWAP, not an OID-preserving relfilenode/storage switch.
  Real design preserves `pg_class.oid` so views/FKs/triggers/publications and
  CDC/backup identity survive.
- Drain is a fixed sleep, not a "mirror applied through barrier F" check.
- Apply is batch/post-hoc, not a live streaming applier with an idempotency
  ledger keyed by `(slot, commit_lsn)`.
- Single node, single tablet-set. No tablet splits, partitions, geo, colocation.
- No crash/failover injection; no external-CDC handoff; no backup/PITR.

## Operational note

YB logical slots can linger `active=t` briefly after the consumer exits (lease
timeout), so slot drop needs retry-with-backoff. The harness handles this in
`drop_slot_retry`.

## Files

- `mirror_harness.sh`    - 1:1 single-tablet flow; asserts parity.
- `mirror_harness_nm.sh` - N:M distributed flow; PK-changing transform,
  multi-row cross-tablet txns; asserts fan-out + atomicity + parity.
- `apply_changes.py`     - parses `test_decoding` output into shadow apply SQL,
  preserving source BEGIN/COMMIT framing into atomic target transactions.

## Run N:M

```
PATH="/usr/bin:/bin:$PATH" bash prototype/osc-logical-slot/mirror_harness_nm.sh
```

Tunables: `SRC_TABLETS` (4), `SHADOW_TABLETS` (6), `SEED_ROWS` (4000),
`TXNS` (100), `ROWS_PER_TXN` (8).
