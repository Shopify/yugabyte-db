#!/usr/bin/env bash
#
# Online schema change (OSC) prototype A: logical-replication-slot shadow mirror.
#
# Roadmap validation harness. NOT production code. Drives the end-to-end thin
# slice against a running local cluster using only existing YSQL primitives:
#
#   1. create source table G0 (regular, non-colocated, has PK)
#   2. seed rows
#   3. REPLICA IDENTITY FULL on source (full old images for the mirror)
#   4. create a logical replication slot (consistent point == snapshot boundary S)
#   5. create shadow table G1 with the target (post-ALTER) schema
#   6. backfill: copy source rows as of the slot's snapshot into G1 via a transform
#   7. start async mirror: pg_recvlogical(test_decoding) -> apply to G1
#   8. run concurrent DML on the source while the mirror runs
#   9. quiesce writers, drain the mirror
#  10. cutover: brief lock, final drain, swap names (prototype-level swap)
#  11. assert row-parity between the transformed source and G1 (no lost writes)
#
# This prototype uses a name swap for the cutover step only because it is the
# cheapest way to validate the mirror+drain+parity mechanics from SQL. The
# roadmap's real cutover preserves the source OID via a relfilenode/storage
# switch; that is out of scope for this harness.
#
# The transform under test models: ALTER TABLE ADD COLUMN c2 int DEFAULT 0,
# plus a derived column note = 'v=' || v. Deterministic so backfill and mirror
# agree.

set -uo pipefail

# ---- config ---------------------------------------------------------------
HOST=127.0.0.1
PORT=5433
USER=yugabyte
DB=yugabyte
PGBIN="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/build/latest/postgres/bin"
YSQLSH="$PGBIN/ysqlsh"
RECVLOGICAL="$PGBIN/pg_recvlogical"

SRC=osc_src
SHADOW=osc_shadow
SLOT=osc_mirror_slot
WORKDIR="$(mktemp -d /tmp/claude/osc-harness.XXXXXX)"
CHANGES="$WORKDIR/changes.txt"
RECV_PID=""

SEED_ROWS=${SEED_ROWS:-2000}
CONCURRENT_OPS=${CONCURRENT_OPS:-500}

# ---- helpers --------------------------------------------------------------
psql_yb()  { "$YSQLSH" -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -X "$@"; }
psql_q()   { psql_yb -tAqc "$1"; }         # quiet, tuples-only, single value
say()      { printf '\n=== %s ===\n' "$*"; }
fail()     { printf '\nFAIL: %s\n' "$*" >&2; cleanup; exit 1; }

drop_slot_retry() {
  local slot=$1 i active
  for i in $(seq 1 30); do
    active=$(psql_q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$slot' AND active;" 2>/dev/null)
    if [ "${active:-1}" = "0" ]; then
      psql_yb -c "SELECT pg_drop_replication_slot('$slot');" >/dev/null 2>&1 && return 0
    fi
    sleep 2
  done
  # last attempt, report but don't hard-fail cleanup
  psql_yb -c "SELECT pg_drop_replication_slot('$slot');" 2>&1 || true
}

cleanup() {
  [ -n "$RECV_PID" ] && kill "$RECV_PID" >/dev/null 2>&1
  wait "$RECV_PID" 2>/dev/null
  drop_slot_retry "$SLOT" >/dev/null 2>&1
  psql_yb -c "DROP TABLE IF EXISTS $SRC;    " >/dev/null 2>&1
  psql_yb -c "DROP TABLE IF EXISTS $SHADOW; " >/dev/null 2>&1
  psql_yb -c "DROP TABLE IF EXISTS ${SRC}_old;" >/dev/null 2>&1
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

# transform(v)   -> derived note column, deterministic
# transform SQL used identically for backfill and mirror-apply.
#   target row = (id, v, c2=0-unless-set, note='v='||v)
# For simplicity the prototype keeps c2 constant (models a constant DEFAULT),
# and note is a pure function of v so INSERT/UPDATE stay consistent.

# ---- 0. clean any prior state --------------------------------------------
say "reset"
drop_slot_retry "$SLOT"
psql_yb -c "DROP TABLE IF EXISTS $SRC, $SHADOW, ${SRC}_old CASCADE;" >/dev/null 2>&1

# ---- 1. source table + seed ----------------------------------------------
say "create + seed source ($SEED_ROWS rows)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "source setup"
CREATE TABLE $SRC (id int PRIMARY KEY, v text);
INSERT INTO $SRC SELECT g, 'val_'||g FROM generate_series(1,$SEED_ROWS) g;
ALTER TABLE $SRC REPLICA IDENTITY FULL;
SQL

# ---- 2. slot (snapshot boundary S) ---------------------------------------
# NOTE: creating the slot BEFORE we snapshot-copy guarantees every commit after
# S is captured by the slot. We copy "as of now after slot creation"; because
# the slot streams all post-creation commits, and we only start concurrent
# writers AFTER the slot exists, the copy+stream union is complete.
say "create logical slot"
psql_q "SELECT slot_name FROM pg_create_logical_replication_slot('$SLOT','test_decoding');" \
  | grep -q "$SLOT" || fail "slot create"

# ---- 3. shadow table (target schema) -------------------------------------
say "create shadow (target schema: +c2, +note)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "shadow setup"
CREATE TABLE $SHADOW (id int PRIMARY KEY, v text, c2 int NOT NULL DEFAULT 0, note text);
ALTER TABLE $SHADOW REPLICA IDENTITY FULL;
SQL

# ---- 4. backfill (transform applied) -------------------------------------
# Copy the source snapshot into shadow, applying the transform. Backfill uses
# upsert so a row that a concurrent mirror event already wrote is not clobbered
# incorrectly: ON CONFLICT DO NOTHING lets the newer mirror write win.
say "backfill snapshot -> shadow"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "backfill"
INSERT INTO $SHADOW (id, v, c2, note)
SELECT id, v, 0, 'v='||v FROM $SRC
ON CONFLICT (id) DO NOTHING;
SQL
BACKFILLED=$(psql_q "SELECT count(*) FROM $SHADOW;")
echo "backfilled rows: $BACKFILLED"

# ---- 5. start async mirror ------------------------------------------------
say "start mirror consumer (pg_recvlogical)"
"$RECVLOGICAL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
  --slot="$SLOT" --start --no-loop --file="$CHANGES" >/dev/null 2>&1 &
RECV_PID=$!
sleep 1
kill -0 "$RECV_PID" 2>/dev/null || fail "recvlogical did not start"

# ---- 6. concurrent DML on source -----------------------------------------
say "concurrent DML on source ($CONCURRENT_OPS ops)"
# inserts of new ids, updates of existing, deletes of a disjoint band.
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "concurrent dml"
-- inserts above the seed range
INSERT INTO $SRC SELECT g, 'val_'||g
  FROM generate_series($((SEED_ROWS+1)), $((SEED_ROWS+CONCURRENT_OPS))) g;
-- updates to a band of existing rows
UPDATE $SRC SET v = v || '_upd' WHERE id BETWEEN 1 AND $((CONCURRENT_OPS/2));
-- deletes of another band
DELETE FROM $SRC WHERE id BETWEEN $((SEED_ROWS - CONCURRENT_OPS/2 + 1)) AND $SEED_ROWS;
SQL

# ---- 7. quiesce + drain ---------------------------------------------------
say "drain mirror"
# Give the walsender time to stream the tail, then stop the consumer.
sleep 6
kill "$RECV_PID" >/dev/null 2>&1
wait "$RECV_PID" 2>/dev/null
RECV_PID=""
echo "raw change lines captured: $(wc -l < "$CHANGES" | tr -d ' ')"

# ---- 8. apply captured changes to shadow ---------------------------------
# test_decoding output is textual; parse INSERT/UPDATE/DELETE for table osc_src
# and translate to shadow upserts/deletes with the transform applied.
say "apply captured changes -> shadow"
APPLY_SQL="$WORKDIR/apply.sql"
python3 "$(dirname "${BASH_SOURCE[0]}")/apply_changes.py" \
  --changes "$CHANGES" --src "$SRC" --shadow "$SHADOW" > "$APPLY_SQL" || fail "parse changes"
APPLIED=$(grep -c ';' "$APPLY_SQL" || true)
echo "apply statements: $APPLIED"
psql_yb -v ON_ERROR_STOP=1 -f "$APPLY_SQL" >/dev/null 2>&1 || fail "apply changes"

# ---- 9. cutover (prototype name swap) ------------------------------------
say "cutover (brief lock + name swap)"
psql_yb -v ON_ERROR_STOP=1 <<SQL || fail "cutover"
BEGIN;
LOCK TABLE $SRC IN ACCESS EXCLUSIVE MODE;
ALTER TABLE $SRC RENAME TO ${SRC}_old;
ALTER TABLE $SHADOW RENAME TO $SRC;
COMMIT;
SQL

# ---- 10. parity assertions -----------------------------------------------
say "verify parity"
# Expected target = transform(source_old). Compare the now-live table ($SRC,
# formerly shadow) against the transform of the retained old source.
SRC_COUNT=$(psql_q "SELECT count(*) FROM ${SRC}_old;")
NEW_COUNT=$(psql_q "SELECT count(*) FROM $SRC;")
echo "old-source rows: $SRC_COUNT   new-table rows: $NEW_COUNT"

# rows in old-source not correctly represented in new table
MISSING=$(psql_q "
  SELECT count(*) FROM ${SRC}_old s
  LEFT JOIN $SRC t ON t.id = s.id
  WHERE t.id IS NULL
     OR t.v IS DISTINCT FROM s.v
     OR t.c2 <> 0
     OR t.note IS DISTINCT FROM 'v='||s.v;")
# rows in new table that should not exist (deleted / never existed)
EXTRA=$(psql_q "
  SELECT count(*) FROM $SRC t
  LEFT JOIN ${SRC}_old s ON s.id = t.id
  WHERE s.id IS NULL;")

echo "mismatched/missing rows: $MISSING"
echo "extra rows:              $EXTRA"

if [ "$SRC_COUNT" = "$NEW_COUNT" ] && [ "$MISSING" = "0" ] && [ "$EXTRA" = "0" ]; then
  say "RESULT: PASS - shadow converged to transform(source), no lost/extra rows"
  cleanup; trap - EXIT; exit 0
else
  fail "parity mismatch (count $SRC_COUNT vs $NEW_COUNT, missing $MISSING, extra $EXTRA)"
fi
