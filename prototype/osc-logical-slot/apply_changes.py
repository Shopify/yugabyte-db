#!/usr/bin/env python3
"""Translate test_decoding output for the source table into shadow apply SQL,
preserving source transaction framing.

Prototype glue for the OSC logical-slot mirror harness. Parses the textual
test_decoding stream and emits idempotent upsert/delete statements against the
shadow table wrapped in the SAME transaction boundaries as the source, applying
the target transform.

Transform under test (models ADD COLUMN + PK change):

    target row:
      new_id = id * 2          (primary key change; forces target re-routing)
      v      = v               (carried through)
      c2     = 0               (constant default)
      note   = 'v=' || v       (derived)

Transaction framing:

    BEGIN <xid>   -> BEGIN;
    <changes>     -> upsert/delete against shadow
    COMMIT <xid>  -> COMMIT;

This preserves atomicity: all target effects of one source transaction commit
together, which matters when transformed rows fan out across multiple target
tablets.

test_decoding line formats handled (REPLICA IDENTITY FULL on source):

  BEGIN <xid>
  COMMIT <xid>
  table public.osc_src: INSERT: id[integer]:3 v[text]:'c'
  table public.osc_src: UPDATE: old-key: id[integer]:3 v[text]:'old' \
      new-tuple: id[integer]:3 v[text]:'new'
  table public.osc_src: DELETE: id[integer]:2 v[text]:'b'
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


def target_key(id_token):
    """new_id = id * 2. id_token is a bare integer token from test_decoding."""
    if id_token is None:
        return "NULL"
    return "((%s) * 2)" % id_token


def emit_upsert(shadow, cols):
    nid = target_key(cols.get("id"))
    v_lit = sql_text(cols.get("v"))
    return (
        f"INSERT INTO {shadow} (new_id, v, c2, note) "
        f"VALUES ({nid}, {v_lit}, 0, 'v=' || {v_lit}) "
        f"ON CONFLICT (new_id) DO UPDATE SET "
        f"v = EXCLUDED.v, c2 = EXCLUDED.c2, note = EXCLUDED.note;"
    )


def emit_delete(shadow, id_token):
    return f"DELETE FROM {shadow} WHERE new_id = {target_key(id_token)};"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--changes", required=True)
    ap.add_argument("--src", required=True)
    ap.add_argument("--shadow", required=True)
    args = ap.parse_args()

    begin_re = re.compile(r"^BEGIN\b")
    commit_re = re.compile(r"^COMMIT\b")
    row_re = re.compile(
        r"^table public\.%s: (INSERT|UPDATE|DELETE): (.*)$" % re.escape(args.src)
    )

    out = []
    # Track whether the current transaction touched the source table. Only open
    # a target transaction that actually has work, to avoid empty BEGIN/COMMIT.
    txn_open = False
    txn_has_work = False
    txn_buf = []

    def flush_txn():
        nonlocal txn_open, txn_has_work, txn_buf
        if txn_has_work:
            out.append("BEGIN;")
            out.extend(txn_buf)
            out.append("COMMIT;")
        txn_open = False
        txn_has_work = False
        txn_buf = []

    with open(args.changes, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if begin_re.match(line):
                # start a new logical transaction window
                if txn_open:
                    flush_txn()
                txn_open = True
                txn_has_work = False
                txn_buf = []
                continue
            if commit_re.match(line):
                flush_txn()
                continue
            m = row_re.match(line)
            if not m:
                continue
            op, rest = m.group(1), m.group(2)
            if op == "INSERT":
                stmt = emit_upsert(args.shadow, parse_cols(rest))
            elif op == "UPDATE":
                new_part = rest.split("new-tuple:", 1)[1] if "new-tuple:" in rest else rest
                stmt = emit_upsert(args.shadow, parse_cols(new_part))
            elif op == "DELETE":
                cols = parse_cols(rest)
                key = cols.get("id")
                stmt = emit_delete(args.shadow, key) if key is not None else None
            else:
                stmt = None
            if stmt:
                txn_buf.append(stmt)
                txn_has_work = True

    # a trailing BEGIN without COMMIT (partial capture) is dropped on purpose
    sys.stdout.write("\n".join(out))
    if out:
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
