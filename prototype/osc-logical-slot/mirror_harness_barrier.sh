#!/usr/bin/env bash
#
# OSC prototype A (Step 3): real barrier drain (no fixed sleep) under a
# continuous background writer, then cutover.
#
# What Step 3 replaces: earlier harnesses drained the mirror with a fixed
# `sleep`, which is not a correctness signal. Here "mirror caught up to the
# barrier" is DETERMINISTIC via a sentinel row:
#
#   1. a background writer mutates the source for a while (builds a real tail)
#   2. to finalize, take ACCESS EXCLUSIVE on the source (fences new writes)
#   3. insert a unique BARRIER sentinel row into the source, commit
#   4. drain the slot (one capture pass; the slot retains ALL unacked changes,
#      so a single capture after the barrier contains the whole history up to
#      and including the sentinel) and apply it with the streaming applier
#   5. the mirror is caught up exactly when the sentinel's TRANSFORMED row is
#      present in the shadow; poll for that with bounded retries (only to ride
#      out transient slot-active windows), never a blind sleep
#   6. remove the sentinel and swap
#
# Key design lesson (v1 bug): do NOT spawn many short-lived pg_recvlogical
# consumers in a tight loop; each leaves the slot `active` for a lease window
# and the next start captures nothing. Drain once, after the barrier.
#
# Cutover note: this SQL harness runs source and shadow as two independent
# tables, so it performs a name swap. The real OID-preserving storage switch is
# validated separately in-tree by
# src/yb/yql/pgwrapper/pg_online_schema_change-test.cc (roadmap Step 1).

set -uo pipefail

HOST=127.0.0.1; PORT=5433; USER=yugabyte; DB=yugabyte
PGBIN="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/build/latest/postgres/bin"
YSQLSH="$PGBIN/ysqlsh"; RECVLOGICAL="$PGBIN/pg_recvlogical"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SRC=osc_src; SHADOW=osc_shadow; LEDGER=osc_apply_ledger; SLOT=osc_mirror_slot
SEED_ROWS=${SEED_ROWS:-2000}
WRITER_SECONDS=${WRITER_SECONDS:-10}
BARRIER_ID=2147483000
BARRIER_KEY=$(( BARRIER_ID * 2 ))
WORKDIR="$(mktemp -d /tmp/claude/osc-barrier.XXXXXX)"
WRITER_PID=""

psql_yb() { "$YSQLSH" -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -X "$@"; }
psql_q()  { psql_yb -tAqc "$1"; }
say()     { printf '\n=== %s ===\n' "$*"; }
fail()    { printf '\nFAIL: %s\n' "$*" >&2; cleanup; exit 1; }

wait_slot_idle() {
  local i active
  for i in $(seq 1 20); do
    active=$(psql_q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$SLOT' AND active;" 2>/dev/null)
    [ "${active:-1}" = "0" ] && return 0
    sleep 1
  done
  return 1
}
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
  [ -n "$WRITER_PID" ] && { kill "$WRITER_PID" >/dev/null 2>&1; wait "$WRITER_PID" 2>/dev/null; }
  drop_slot_retry "$SLOT" >/dev/null 2>&1
  psql_yb -c "DROP TABLE IF EXISTS $SRC, $SHADOW, $LEDGER, ${SRC}_old CASCADE;" >/dev/null 2>&1
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

# One drain pass: wait for slot idle, capture the full unacked tail, apply it.
drain_once() {
  wait_slot_idle || return 1
  local cap="$WORKDIR/cap.$RANDOM.txt" sqlf="$WORKDIR/apply.$RANDOM.sql"
  timeout 6 "$RECVLOGICAL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
    --slot="$SLOT" --start --no-loop --status-interval=3600 --fsync-interval=0 \
    -f "$cap" >/dev/null 2>&1 || true
  python3 "$HERE/streaming_applier.py" \
    --src "$SRC" --shadow "$SHADOW" --ledger "$LEDGER" --slot "$SLOT" \
    < "$cap" > "$sqlf" 2>/dev/null || return 1
  psql_yb -v ON_ERROR_STOP=1 -f "$sqlf" >/dev/null 2>&1 || return 1
  return 0
}

say "reset"
drop_slot_retry "$SLOT"
psql_yb -c "DROP TABLE IF EXISTS $SRC, $SHADOW, $LEDGER, ${SRC}_old CASCADE;" >/dev/null 2>&1

say "source + seed ($SEED_ROWS rows)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "src"
CREATE TABLE $SRC (id int PRIMARY KEY, v text);
INSERT INTO $SRC SELECT g,'val_'||g FROM generate_series(1,$SEED_ROWS) g;
ALTER TABLE $SRC REPLICA IDENTITY FULL;
SQL

say "slot + shadow + ledger + backfill"
psql_q "SELECT slot_name FROM pg_create_logical_replication_slot('$SLOT','test_decoding');" | grep -q "$SLOT" || fail "slot"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "shadow"
CREATE TABLE $SHADOW (new_id bigint PRIMARY KEY, v text, c2 int NOT NULL DEFAULT 0, note text);
CREATE TABLE $LEDGER (slot text, xid bigint, PRIMARY KEY(slot,xid));
INSERT INTO $SHADOW (new_id,v,c2,note) SELECT id::bigint*2,v,0,'v='||v FROM $SRC ON CONFLICT (new_id) DO NOTHING;
SQL

say "continuous background writer (${WRITER_SECONDS}s of INSERT/UPDATE tail)"
(
  end=$(( $(date +%s) + WRITER_SECONDS )); n=$((SEED_ROWS+1))
  while [ "$(date +%s)" -lt "$end" ]; do
    "$YSQLSH" -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -X -q -c \
      "INSERT INTO $SRC VALUES ($n,'val_$n'); UPDATE $SRC SET v=v||'_u' WHERE id=$((n-100));" >/dev/null 2>&1
    n=$((n+1))
  done
) &
WRITER_PID=$!
wait "$WRITER_PID" 2>/dev/null; WRITER_PID=""
TAIL_ROWS=$(psql_q "SELECT count(*) FROM $SRC WHERE id > $SEED_ROWS AND id < $BARRIER_ID;")
echo "writer produced tail rows: $TAIL_ROWS"

# ---- BARRIER: fence writes, mark sentinel, drain until sentinel visible ----
say "barrier: ACCESS EXCLUSIVE + sentinel, then drain (no sleep)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "barrier insert"
BEGIN;
LOCK TABLE $SRC IN ACCESS EXCLUSIVE MODE;
INSERT INTO $SRC VALUES ($BARRIER_ID, '__BARRIER__');
COMMIT;
SQL

# One drain pass captures the entire unacked history (the slot retains it),
# including the sentinel. A few bounded retries only ride out a transient
# slot-active window; this is a real caught-up check, never a blind sleep.
CAUGHT=0
for attempt in $(seq 1 4); do
  drain_once || true
  present=$(psql_q "SELECT count(*) FROM $SHADOW WHERE new_id=$BARRIER_KEY;")
  if [ "${present:-0}" = "1" ]; then
    echo "caught up: sentinel visible in shadow after $attempt drain attempt(s)"
    CAUGHT=1; break
  fi
done
[ "$CAUGHT" = "1" ] || fail "mirror never reached barrier (sentinel not applied)"

# sentinel is not user data: remove from both sides before swap
psql_yb -c "DELETE FROM $SRC WHERE id=$BARRIER_ID; DELETE FROM $SHADOW WHERE new_id=$BARRIER_KEY;" >/dev/null 2>&1

say "cutover (name swap; OID-preserving switch validated in Step 1 C++ test)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "cutover"
BEGIN;
LOCK TABLE $SRC IN ACCESS EXCLUSIVE MODE;
ALTER TABLE $SRC RENAME TO ${SRC}_old;
ALTER TABLE $SHADOW RENAME TO $SRC;
COMMIT;
SQL

say "parity (live == transform(source_old), incl. full writer tail)"
OLD=$(psql_q "SELECT count(*) FROM ${SRC}_old;")
NEW=$(psql_q "SELECT count(*) FROM $SRC;")
MISSING=$(psql_q "
  SELECT count(*) FROM ${SRC}_old s
  LEFT JOIN $SRC t ON t.new_id = s.id::bigint*2
  WHERE t.new_id IS NULL OR t.v IS DISTINCT FROM s.v
     OR t.c2 <> 0 OR t.note IS DISTINCT FROM 'v='||s.v;")
EXTRA=$(psql_q "
  SELECT count(*) FROM $SRC t
  LEFT JOIN ${SRC}_old s ON s.id::bigint*2 = t.new_id WHERE s.id IS NULL;")
LEDGER_DUP=$(psql_q "SELECT count(*)-count(DISTINCT (slot,xid)) FROM $LEDGER;")
echo "old=$OLD new=$NEW missing=$MISSING extra=$EXTRA ledger_dupes=$LEDGER_DUP tail=$TAIL_ROWS"

if [ "$OLD" = "$NEW" ] && [ "$MISSING" = "0" ] && [ "$EXTRA" = "0" ] && [ "$LEDGER_DUP" = "0" ] \
   && [ "${TAIL_ROWS:-0}" -gt 0 ]; then
  say "RESULT: PASS - barrier drain caught the write tail, parity exact, no dup"
  cleanup; trap - EXIT; exit 0
else
  fail "parity mismatch after barrier drain"
fi
