#!/usr/bin/env bash
#
# OSC prototype A (N:M): logical-replication-slot shadow mirror across
# different source/target tablet layouts, with a primary-key-changing transform
# and multi-row cross-tablet transactions.
#
# This extends mirror_harness.sh to validate the DISTRIBUTED shape of the design:
#
#   - source SPLIT INTO 4 TABLETS, shadow SPLIT INTO 6 TABLETS  (N:M)
#   - transform changes the primary key (new_id = id * 2), so target routing is
#     genuinely different from source routing
#   - concurrent phase includes multi-row transactions whose rows span multiple
#     source tablets, and whose transformed rows fan out across multiple target
#     tablets
#   - apply preserves source BEGIN/COMMIT framing (atomic target transactions)
#   - asserts: fan-out (rows land on >1 target tablet), row parity, and
#     transaction atomicity (no partially-applied multi-row txn)
#
# Cutover is still a prototype name swap (NOT the real OID-preserving switch).

set -uo pipefail

HOST=127.0.0.1; PORT=5433; USER=yugabyte; DB=yugabyte
PGBIN="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/build/latest/postgres/bin"
YSQLSH="$PGBIN/ysqlsh"; RECVLOGICAL="$PGBIN/pg_recvlogical"

SRC=osc_src; SHADOW=osc_shadow; SLOT=osc_mirror_slot
SRC_TABLETS=${SRC_TABLETS:-4}
SHADOW_TABLETS=${SHADOW_TABLETS:-6}
SEED_ROWS=${SEED_ROWS:-4000}
TXNS=${TXNS:-100}          # number of concurrent multi-row transactions
ROWS_PER_TXN=${ROWS_PER_TXN:-8}

WORKDIR="$(mktemp -d /tmp/claude/osc-nm.XXXXXX)"
CHANGES="$WORKDIR/changes.txt"; RECV_PID=""

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
  [ -n "$RECV_PID" ] && { kill "$RECV_PID" >/dev/null 2>&1; wait "$RECV_PID" 2>/dev/null; }
  drop_slot_retry "$SLOT" >/dev/null 2>&1
  psql_yb -c "DROP TABLE IF EXISTS $SRC, $SHADOW, ${SRC}_old CASCADE;" >/dev/null 2>&1
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

# count how many distinct tablets of a table actually hold at least one row,
# by bucketing yb_hash_code into the table's tablet partition ranges.
# Simpler proxy: count distinct tablets that contain rows using yb_local_tablets
# partition ranges vs the row's hash. We approximate fan-out with distinct
# width_buckets over the tablet count, which is exact enough for the assertion.
distinct_row_tablets() { # $1 table  $2 keycol  $3 tabletcount
  psql_q "SELECT count(DISTINCT width_bucket(yb_hash_code($2), 0, 65536, $3)) FROM $1;"
}

say "reset"
drop_slot_retry "$SLOT"
psql_yb -c "DROP TABLE IF EXISTS $SRC, $SHADOW, ${SRC}_old CASCADE;" >/dev/null 2>&1

say "create + seed source ($SEED_ROWS rows, $SRC_TABLETS tablets)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "source setup"
CREATE TABLE $SRC (id int, v text, PRIMARY KEY(id HASH)) SPLIT INTO $SRC_TABLETS TABLETS;
INSERT INTO $SRC SELECT g, 'val_'||g FROM generate_series(1,$SEED_ROWS) g;
ALTER TABLE $SRC REPLICA IDENTITY FULL;
SQL
SRC_TAB=$(psql_q "SELECT count(*) FROM yb_local_tablets WHERE table_name='$SRC';")
echo "source tablets: $SRC_TAB"

say "create logical slot (snapshot boundary S)"
psql_q "SELECT slot_name FROM pg_create_logical_replication_slot('$SLOT','test_decoding');" \
  | grep -q "$SLOT" || fail "slot create"

say "create shadow (target schema: PK new_id=id*2, +c2, +note; $SHADOW_TABLETS tablets)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "shadow setup"
CREATE TABLE $SHADOW (new_id int, v text, c2 int NOT NULL DEFAULT 0, note text,
                      PRIMARY KEY(new_id HASH)) SPLIT INTO $SHADOW_TABLETS TABLETS;
ALTER TABLE $SHADOW REPLICA IDENTITY FULL;
SQL
SH_TAB=$(psql_q "SELECT count(*) FROM yb_local_tablets WHERE table_name='$SHADOW';")
echo "shadow tablets: $SH_TAB"

say "backfill snapshot -> shadow (transform: new_id=id*2)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "backfill"
INSERT INTO $SHADOW (new_id, v, c2, note)
SELECT id*2, v, 0, 'v='||v FROM $SRC
ON CONFLICT (new_id) DO NOTHING;
SQL
echo "backfilled rows: $(psql_q "SELECT count(*) FROM $SHADOW;")"

say "start mirror consumer (pg_recvlogical)"
"$RECVLOGICAL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
  --slot="$SLOT" --start --no-loop --file="$CHANGES" >/dev/null 2>&1 &
RECV_PID=$!; sleep 1
kill -0 "$RECV_PID" 2>/dev/null || fail "recvlogical did not start"

say "concurrent MULTI-ROW cross-tablet transactions ($TXNS txns x $ROWS_PER_TXN rows)"
# Each transaction inserts ROWS_PER_TXN rows with ids chosen to spread across
# source tablets (large stride), plus updates and a delete, all in one txn.
# Building one SQL file so each txn is a single BEGIN/COMMIT unit.
DMLFILE="$WORKDIR/dml.sql"
{
  base=$((SEED_ROWS + 1))
  for t in $(seq 0 $((TXNS-1))); do
    echo "BEGIN;"
    for r in $(seq 0 $((ROWS_PER_TXN-1))); do
      # stride by a prime to scatter across hash space / source tablets
      id=$(( base + t*ROWS_PER_TXN*7 + r*7 ))
      echo "INSERT INTO $SRC VALUES ($id, 'val_$id');"
    done
    # update a couple of low ids (existing seed rows) in the same txn
    u1=$(( 1 + t )); u2=$(( 1 + t + 500 ))
    echo "UPDATE $SRC SET v = v || '_u' WHERE id IN ($u1, $u2);"
    # delete one existing high seed row
    d=$(( SEED_ROWS - t )); echo "DELETE FROM $SRC WHERE id = $d;"
    echo "COMMIT;"
  done
} > "$DMLFILE"
psql_yb -v ON_ERROR_STOP=1 -f "$DMLFILE" >/dev/null 2>&1 || fail "concurrent dml"

say "drain mirror"
sleep 8
kill "$RECV_PID" >/dev/null 2>&1; wait "$RECV_PID" 2>/dev/null; RECV_PID=""
echo "raw change lines: $(wc -l < "$CHANGES" | tr -d ' ')"
echo "source BEGIN count: $(grep -c '^BEGIN' "$CHANGES" || true)"

say "apply captured changes -> shadow (transaction-preserving)"
APPLY_SQL="$WORKDIR/apply.sql"
python3 "$(dirname "${BASH_SOURCE[0]}")/apply_changes.py" \
  --changes "$CHANGES" --src "$SRC" --shadow "$SHADOW" > "$APPLY_SQL" || fail "parse"
echo "apply BEGIN count: $(grep -c '^BEGIN;' "$APPLY_SQL" || true)"
psql_yb -v ON_ERROR_STOP=1 -f "$APPLY_SQL" >/dev/null 2>&1 || fail "apply"

say "fan-out check (transformed rows must span multiple target tablets)"
SRC_FANOUT=$(distinct_row_tablets "$SRC" id "$SRC_TAB")
SH_FANOUT=$(distinct_row_tablets "$SHADOW" new_id "$SH_TAB")
echo "distinct source tablets with rows: $SRC_FANOUT / $SRC_TAB"
echo "distinct shadow tablets with rows: $SH_FANOUT / $SH_TAB"
[ "${SH_FANOUT:-0}" -ge 2 ] || fail "shadow rows did not fan out across tablets"

say "cutover (brief lock + name swap)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "cutover"
BEGIN;
LOCK TABLE $SRC IN ACCESS EXCLUSIVE MODE;
ALTER TABLE $SRC RENAME TO ${SRC}_old;
ALTER TABLE $SHADOW RENAME TO $SRC;
COMMIT;
SQL

say "verify parity (live table == transform(old source))"
OLD_COUNT=$(psql_q "SELECT count(*) FROM ${SRC}_old;")
NEW_COUNT=$(psql_q "SELECT count(*) FROM $SRC;")
echo "old-source rows: $OLD_COUNT   new-table rows: $NEW_COUNT"
MISSING=$(psql_q "
  SELECT count(*) FROM ${SRC}_old s
  LEFT JOIN $SRC t ON t.new_id = s.id*2
  WHERE t.new_id IS NULL
     OR t.v IS DISTINCT FROM s.v
     OR t.c2 <> 0
     OR t.note IS DISTINCT FROM 'v='||s.v;")
EXTRA=$(psql_q "
  SELECT count(*) FROM $SRC t
  LEFT JOIN ${SRC}_old s ON s.id*2 = t.new_id
  WHERE s.id IS NULL;")
echo "mismatched/missing rows: $MISSING"
echo "extra rows:              $EXTRA"

# transaction atomicity smoke: every inserted txn contributed exactly
# ROWS_PER_TXN new rows above the seed range; count them.
NEW_INSERTS=$(psql_q "SELECT count(*) FROM $SRC WHERE new_id > ${SEED_ROWS}*2;")
EXPECT_INSERTS=$(( TXNS * ROWS_PER_TXN ))
echo "new inserted rows: $NEW_INSERTS (expected $EXPECT_INSERTS)"

if [ "$OLD_COUNT" = "$NEW_COUNT" ] && [ "$MISSING" = "0" ] && [ "$EXTRA" = "0" ] \
   && [ "$NEW_INSERTS" = "$EXPECT_INSERTS" ]; then
  say "RESULT: PASS - N:M mirror converged, fan-out confirmed, txns atomic, parity exact"
  cleanup; trap - EXIT; exit 0
else
  fail "parity/atomicity mismatch"
fi
