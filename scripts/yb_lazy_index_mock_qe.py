#!/usr/bin/env python3
#
# Minimal batch materializer for externally maintained YSQL indexes.
#
# This intentionally does not stream changes. It coordinates a simple test flow:
# mark the lazy index non-serving, backfill it by scanning the base table via
# Yugabyte's index build path, and then flip the index to serving for
# eventual-consistency reads.

import argparse
import os
import subprocess
import sys


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run_ysqlsh(args: argparse.Namespace, sql: str, *, tuples_only: bool = False) -> str:
    cmd = [
        args.ysqlsh,
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        args.host,
        "-p",
        str(args.port),
        "-U",
        args.user,
        "-d",
        args.database,
    ]
    if tuples_only:
        cmd.append("-At")
    if args.quiet:
        cmd.append("-q")
    cmd.extend(["-c", sql])

    if args.echo:
        print("+ " + " ".join(cmd[:-1] + [sql]), file=sys.stderr)

    completed = subprocess.run(
        cmd,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr)
        raise SystemExit(completed.returncode)
    if completed.stderr and not args.quiet:
        sys.stderr.write(completed.stderr)
    return completed.stdout


def resolve_lazy_index(args: argparse.Namespace) -> str:
    sql = f"""
SELECT format('%I.%I', n.nspname, c.relname)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = {sql_literal(args.index)}::regclass
  AND c.relkind = 'i'
  AND COALESCE(c.reloptions, ARRAY[]::text[]) @> ARRAY['yb_external_maintenance=true'];
"""
    qualified = run_ysqlsh(args, sql, tuples_only=True).strip()
    if not qualified:
        raise SystemExit(
            f"{args.index!r} is not an externally maintained index or does not exist"
        )
    return qualified


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Batch-materialize a Yugabyte externally maintained lazy index."
    )
    parser.add_argument("index", help="Index name, optionally schema-qualified.")
    parser.add_argument("--ysqlsh", default=os.environ.get("YSQLSH", "bin/ysqlsh"))
    parser.add_argument("--host", default=os.environ.get("PGHOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "5433")))
    parser.add_argument("--user", default=os.environ.get("PGUSER", "yugabyte"))
    parser.add_argument("--database", default=os.environ.get("PGDATABASE", "yugabyte"))
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--echo", action="store_true")
    args = parser.parse_args()

    qualified_index = resolve_lazy_index(args)

    run_ysqlsh(
        args,
        f"ALTER INDEX {qualified_index} SET (yb_lazy_index_serving = false);",
    )
    run_ysqlsh(
        args,
        f"SELECT yb_backfill_external_index({sql_literal(qualified_index)}::regclass);",
    )
    run_ysqlsh(
        args,
        f"ALTER INDEX {qualified_index} SET (yb_lazy_index_serving = true);",
    )

    if not args.quiet:
        print(f"{qualified_index} backfilled and marked serving")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
