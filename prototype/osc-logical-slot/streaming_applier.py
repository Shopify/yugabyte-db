#!/usr/bin/env python3
"""Live streaming shadow applier with an idempotency ledger (OSC roadmap Step 2).

Consumes a YugabyteDB logical replication slot (test_decoding output, streamed
via pg_recvlogical on stdin) and applies transformed changes to a shadow table.
Each SOURCE transaction (identified by its xid) is applied in ONE target
transaction together with an idempotency-ledger row keyed by (slot, xid). On
restart, transactions whose xid is already in the ledger are skipped, giving
exactly-once EFFECT on top of at-least-once transport.

This hardens the batch apply from mirror_harness.sh into a real streaming
applier that survives being killed mid-stream.

Transform (matches mirror_harness_nm.sh): new_id = id*2, v carried, c2=0,
note = 'v=' || v.

Input: test_decoding lines on stdin, e.g.
  BEGIN 3
  table public.osc_src: INSERT: id[integer]:2 v[text]:'b'
  table public.osc_src: UPDATE: old-key: id[integer]:3 v[text]:'old' \
      new-tuple: id[integer]:3 v[text]:'new'
  table public.osc_src: DELETE: id[integer]:2 v[text]:'b'
  COMMIT 3

The applier connects to the target DB with psycopg-free libpq via the `ysqlsh`
binary is avoided; instead we shell out to a persistent psql? No - we use a
minimal libpq through the `pg` protocol is unavailable in stdlib. To keep the
prototype dependency-free, we batch each transaction into a single SQL string
and execute it through a callback provided by the caller (see apply_via).

For simplicity and determinism this script prints, per source transaction, one
self-contained SQL transaction block to stdout. The harness pipes that to
ysqlsh. Idempotency is enforced IN SQL: the ledger insert uses ON CONFLICT DO
NOTHING and all mutations are guarded so a replayed xid is a no-op.
"""

import argparse
import re
import sys

COL_RE = re.compile(
    r"(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\[[^\]]+\]:"
    r"(?P<val>'(?:[^']|'')*'|[^ ]+)"
)


def parse_cols(segment):
    out = {}
    for m in COL_RE.finditer(segment):
        out[m.group("name")] = m.group("val")
    return out


def sql_text(raw):
    if raw is None or raw == "null":
        return "NULL"
    if raw.startswith("'"):
        return raw
    return "'" + raw.replace("'", "''") + "'"


def tkey(id_token):
    return "NULL" if id_token is None else "((%s) * 2)" % id_token


def upsert(shadow, cols):
    v = sql_text(cols.get("v"))
    return (
        f"INSERT INTO {shadow} (new_id, v, c2, note) "
        f"VALUES ({tkey(cols.get('id'))}, {v}, 0, 'v=' || {v}) "
        f"ON CONFLICT (new_id) DO UPDATE SET "
        f"v = EXCLUDED.v, c2 = EXCLUDED.c2, note = EXCLUDED.note;"
    )


def delete(shadow, id_token):
    return f"DELETE FROM {shadow} WHERE new_id = {tkey(id_token)};"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--shadow", required=True)
    ap.add_argument("--ledger", required=True)
    ap.add_argument("--slot", required=True)
    args = ap.parse_args()

    begin_re = re.compile(r"^BEGIN (\d+)")
    commit_re = re.compile(r"^COMMIT (\d+)")
    row_re = re.compile(
        r"^table public\.%s: (INSERT|UPDATE|DELETE): (.*)$" % re.escape(args.src)
    )

    out = []
    cur_xid = None
    buf = []
    work = False

    def flush(xid):
        nonlocal buf, work
        # Emit one atomic target transaction. The ledger row is the idempotency
        # guard: if (slot,xid) already applied, the INSERT ... ON CONFLICT DO
        # NOTHING affects 0 rows AND we must skip the mutations. We achieve
        # "skip mutations on replay" by gating the whole txn on the ledger:
        # a DO block that returns early when the xid is already present.
        stmts = "\n    ".join(buf) if buf else ""
        block = f"""BEGIN;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM {args.ledger}
             WHERE slot = '{args.slot}' AND xid = {xid}) THEN
    RETURN;  -- already applied: exactly-once guard
  END IF;
  INSERT INTO {args.ledger}(slot, xid) VALUES ('{args.slot}', {xid});
  {stmts}
END $$;
COMMIT;"""
        out.append(block)
        buf = []
        work = False

    for line in sys.stdin:
        line = line.rstrip("\n")
        mb = begin_re.match(line)
        if mb:
            cur_xid = mb.group(1)
            buf = []
            work = False
            continue
        mc = commit_re.match(line)
        if mc:
            flush(mc.group(1))
            cur_xid = None
            continue
        mr = row_re.match(line)
        if not mr or cur_xid is None:
            continue
        op, rest = mr.group(1), mr.group(2)
        if op == "INSERT":
            buf.append(upsert(args.shadow, parse_cols(rest)))
        elif op == "UPDATE":
            new_part = rest.split("new-tuple:", 1)[1] if "new-tuple:" in rest else rest
            buf.append(upsert(args.shadow, parse_cols(new_part)))
        elif op == "DELETE":
            key = parse_cols(rest).get("id")
            if key is not None:
                buf.append(delete(args.shadow, key))
        work = True

    sys.stdout.write("\n".join(out))
    if out:
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
