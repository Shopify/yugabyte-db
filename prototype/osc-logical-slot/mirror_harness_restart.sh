#!/usr/bin/env bash
#
# OSC prototype A (Step 2): live streaming applier + idempotency ledger,
# proving exactly-once EFFECT across an applier restart mid-stream.
#
# Flow:
#   1. source + seed, REPLICA IDENTITY FULL
#   2. logical slot (snapshot boundary S)
#   3. shadow table + ledger table (slot, xid)
#   4. backfill snapshot -> shadow (transform new_id=id*2)
#   5. concurrent DML on source
#   6. CONSUME PASS 1: stream changes WITHOUT feedback, kill mid-way, apply the
#      transactions we did capture (some xids land in the ledger)
#   7. CONSUME PASS 2: because pass 1 never acked, the slot REPLAYS all changes;
#      the ledger makes already-applied xids no-ops, and new ones apply once
#   8. assert exactly-once: parity holds, no duplicate effects, ledger has each
#      xid exactly once
#
# The key property under test: at-least-once transport (redelivery on restart)
# yields exactly-once effect because of the (slot,xid) ledger committed in the
# same target transaction as the mutations.

set -uo pipefail

HOST=127.0.0.1; PORT=5433; USER=yugabyte; DB=yugabyte
PGBIN="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/build/latest/postgres/bin"
YSQLSH="$PGBIN/ysqlsh"; RECVLOGICAL="$PGBIN/pg_recvlogical"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SRC=osc_src; SHADOW=osc_shadow; LEDGER=osc_apply_ledger; SLOT=osc_mirror_slot
SEED_ROWS=${SEED_ROWS:-2000}
CONCURRENT_OPS=${CONCURRENT_OPS:-400}
WORKDIR="$(mktemp -d /tmp/claude/osc-restart.XXXXXX)"

psql_yb() { "$YSQLSH" -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -X "$@"; }
psql_q()  { psql_yb -tAqc "$1"; }
say()     { printf '\n=== %s ===\n' "$*"; }
fail()    { printf '\nFAIL: %s\n' "$*" >&2; cleanup; exit 1; }

drop_slot_retry() {
  local slot=$1 i active
  for i in $(seq 1 30); do
    active=$(psql_q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$slot' AND active;" 2>/dev/null)
    [ "${active:-1}" = "0" ] && psql_yb -c "SELECT pg_drop_replication_slot('$slot');" >/dev/null 2>&1 && return 0
    sleep 2
  done
  psql_yb -c "SELECT pg_drop_replication_slot('$slot');" >/dev/null 2>&1 || true
}
cleanup() {
  drop_slot_retry "$SLOT" >/dev/null 2>&1
  psql_yb -c "DROP TABLE IF EXISTS $SRC, $SHADOW, $LEDGER, ${SRC}_old CASCADE;" >/dev/null 2>&1
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

# stream from slot WITHOUT sending feedback, for a bounded time, into a file.
# Killing before status-interval means the slot is NOT advanced -> replay next.
capture_no_ack() { # $1 outfile  $2 seconds
  timeout "$2" "$RECVLOGICAL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
    --slot="$SLOT" --start --no-loop --status-interval=3600 --fsync-interval=0 \
    -f "$1" >/dev/null 2>&1 || true
}

apply_capture() { # $1 changes-file  -> builds + runs transaction-per-xid SQL
  local sqlf="$WORKDIR/apply.$RANDOM.sql"
  python3 "$HERE/streaming_applier.py" \
    --src "$SRC" --shadow "$SHADOW" --ledger "$LEDGER" --slot "$SLOT" \
    < "$1" > "$sqlf" || fail "applier parse"
  psql_yb -v ON_ERROR_STOP=1 -f "$sqlf" >/dev/null 2>&1 || fail "applier apply"
  grep -c '^BEGIN;' "$sqlf" 2>/dev/null || echo 0
}

say "reset"
drop_slot_retry "$SLOT"
psql_yb -c "DROP TABLE IF EXISTS $SRC, $SHADOW, $LEDGER, ${SRC}_old CASCADE;" >/dev/null 2>&1

say "source + seed ($SEED_ROWS rows)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "src setup"
CREATE TABLE $SRC (id int PRIMARY KEY, v text);
INSERT INTO $SRC SELECT g,'val_'||g FROM generate_series(1,$SEED_ROWS) g;
ALTER TABLE $SRC REPLICA IDENTITY FULL;
SQL

say "logical slot"
psql_q "SELECT slot_name FROM pg_create_logical_replication_slot('$SLOT','test_decoding');" \
  | grep -q "$SLOT" || fail "slot"

say "shadow + ledger"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "shadow/ledger"
CREATE TABLE $SHADOW (new_id int PRIMARY KEY, v text, c2 int NOT NULL DEFAULT 0, note text);
CREATE TABLE $LEDGER (slot text, xid bigint, PRIMARY KEY(slot, xid));
SQL

say "backfill snapshot -> shadow"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "backfill"
INSERT INTO $SHADOW (new_id, v, c2, note)
SELECT id*2, v, 0, 'v='||v FROM $SRC ON CONFLICT (new_id) DO NOTHING;
SQL

say "concurrent DML ($CONCURRENT_OPS ops)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "dml"
INSERT INTO $SRC SELECT g,'val_'||g FROM generate_series($((SEED_ROWS+1)),$((SEED_ROWS+CONCURRENT_OPS))) g;
UPDATE $SRC SET v=v||'_upd' WHERE id BETWEEN 1 AND $((CONCURRENT_OPS/2));
DELETE FROM $SRC WHERE id BETWEEN $((SEED_ROWS-CONCURRENT_OPS/2+1)) AND $SEED_ROWS;
SQL

# ---- CONSUME PASS 1: capture without ack, then apply (partial or full) ----
say "consume pass 1 (no ack) + apply"
C1="$WORKDIR/c1.txt"
capture_no_ack "$C1" 5
LINES1=$(wc -l < "$C1" | tr -d ' ')
XIDS1=$(grep -c '^COMMIT' "$C1" 2>/dev/null || echo 0)
echo "pass1 captured lines=$LINES1 commits=$XIDS1"
APPLIED1=$(apply_capture "$C1")
LEDGER1=$(psql_q "SELECT count(*) FROM $LEDGER;")
echo "pass1 applied txn-blocks=$APPLIED1 ledger-rows=$LEDGER1"

# ---- CONSUME PASS 2: slot replays everything (pass 1 never acked) ----
say "consume pass 2 (replay) + apply"
C2="$WORKDIR/c2.txt"
capture_no_ack "$C2" 6
LINES2=$(wc -l < "$C2" | tr -d ' ')
XIDS2=$(grep -c '^COMMIT' "$C2" 2>/dev/null || echo 0)
echo "pass2 captured lines=$LINES2 commits=$XIDS2 (should include replays of pass1)"
APPLIED2=$(apply_capture "$C2")
LEDGER2=$(psql_q "SELECT count(*) FROM $LEDGER;")
echo "pass2 applied txn-blocks=$APPLIED2 ledger-rows=$LEDGER2"

# ---- exactly-once evidence: replayed xids did not double-apply ----
say "exactly-once checks"
# ledger must hold each xid exactly once (PK guarantees; count distinct == count)
LEDGER_DUP=$(psql_q "SELECT count(*)-count(DISTINCT (slot,xid)) FROM $LEDGER;")
echo "ledger duplicate rows: $LEDGER_DUP"
[ "$LEDGER_DUP" = "0" ] || fail "ledger has duplicate (slot,xid)"

# pass2 must have re-observed pass1's commits (redelivery happened)
if [ "$XIDS2" -lt "$XIDS1" ]; then fail "pass2 did not replay pass1 (no redelivery observed)"; fi

# ---- final parity: live shadow == transform(source) ----
say "cutover + parity"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "cutover"
BEGIN;
LOCK TABLE $SRC IN ACCESS EXCLUSIVE MODE;
ALTER TABLE $SRC RENAME TO ${SRC}_old;
ALTER TABLE $SHADOW RENAME TO $SRC;
COMMIT;
SQL
OLD=$(psql_q "SELECT count(*) FROM ${SRC}_old;")
NEW=$(psql_q "SELECT count(*) FROM $SRC;")
MISSING=$(psql_q "
  SELECT count(*) FROM ${SRC}_old s
  LEFT JOIN $SRC t ON t.new_id = s.id*2
  WHERE t.new_id IS NULL OR t.v IS DISTINCT FROM s.v
     OR t.c2 <> 0 OR t.note IS DISTINCT FROM 'v='||s.v;")
EXTRA=$(psql_q "
  SELECT count(*) FROM $SRC t
  LEFT JOIN ${SRC}_old s ON s.id*2 = t.new_id WHERE s.id IS NULL;")
echo "old=$OLD new=$NEW missing=$MISSING extra=$EXTRA"

if [ "$OLD" = "$NEW" ] && [ "$MISSING" = "0" ] && [ "$EXTRA" = "0" ] && [ "$LEDGER_DUP" = "0" ]; then
  say "RESULT: PASS - exactly-once effect across restart, parity exact"
  cleanup; trap - EXIT; exit 0
else
  fail "parity/exactly-once mismatch"
fi
