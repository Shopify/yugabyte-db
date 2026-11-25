#!/usr/bin/env python3
# Copyright (c) YugabyteDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations
# under the License.

"""Resource experiments, not a local YSQL implementation. Uses only Python's standard library."""

import argparse
import json
import math
import os
from pathlib import Path
import platform
import re
import selectors
import signal
import socket
import subprocess
import sys
import time
import urllib.request

from cpu_usage import CpuWindow, process_tree


def emit(record):
    print(json.dumps(record, sort_keys=True), flush=True)


def disk_usage(root):
    files = 0
    size = 0
    allocated = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                stat = path.stat()
                size += stat.st_size
                allocated += stat.st_blocks * 512
                files += 1
        except FileNotFoundError:
            pass  # A running daemon can unlink a file during the survey.
    return {"files": files, "file_bytes": size, "disk_allocated_bytes": allocated}


def stop(process):
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
    finally:
        # Each owned daemon has its own session. Also clean up any orphaned postgres children.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def read_records(process, deadline, stop_phase=None):
    records = []
    # Binary, unbuffered reads avoid hiding an already-buffered line from select().
    pending = b""
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while time.monotonic() < deadline:
            if not selector.select(timeout=min(1, max(0, deadline - time.monotonic()))):
                continue
            chunk = os.read(process.stdout.fileno(), 65536)
            if not chunk:
                if pending:
                    raise RuntimeError("Incomplete probe JSON output")
                return records
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                record = json.loads(line)
                records.append(record)
                emit(record)
                if record["phase"] == "idle_end":
                    begin = next(r for r in reversed(records) if r["phase"] == "idle_begin")
                    wall = record["elapsed_seconds"] - begin["elapsed_seconds"]
                    cpu = record["cpu_seconds"] - begin["cpu_seconds"]
                    emit({"phase": "storage_idle_cpu", "reopening": record["reopening"],
                          "physical_db_pairs": record["physical_db_pairs"],
                          "wall_seconds": wall, "cpu_seconds": cpu,
                          "average_cores": cpu / wall, "average_cpu_percent": 100 * cpu / wall})
                if record["phase"] == stop_phase:
                    return records
    raise TimeoutError("Storage probe exceeded timeout")


def storage(args):
    executable = args.build_root / "bin/yb-local-storage-probe"
    for stores in args.stores:
        root = args.output / f"stores-{stores}"
        common = [str(executable), f"--probe_dir={root}",
                  f"--probe_tablets={args.tablets}", f"--probe_stores={stores}",
                  f"--probe_rows={args.rows}"]
        if args.idle_seconds:
            common.append(f"--probe_idle_seconds={args.idle_seconds}")
        if args.no_wal:
            common.append("--noprobe_wal")
        for verify in (False, True):
            command = common + (["--probe_verify"] if verify else [])
            if args.crash and not verify:
                command.append(f"--probe_pause_seconds={args.timeout + 60}")
            with (args.output / f"stores-{stores}-{'verify' if verify else 'create'}.log").open(
                    "wb") as log:
                process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=log, bufsize=0,
                                           start_new_session=True)
                try:
                    records = read_records(
                        process, time.monotonic() + args.timeout,
                        "written" if args.crash and not verify else None)
                    if args.crash and not verify:
                        if not any(r["phase"] == "written" for r in records):
                            raise RuntimeError(f"Probe exited before writing; see {log.name}")
                        process.kill()
                        process.wait(timeout=10)
                        if process.returncode != -signal.SIGKILL:
                            raise RuntimeError("Probe did not exit from SIGKILL")
                        emit({"phase": "killed", "physical_db_pairs": stores})
                    else:
                        process.wait(timeout=10)
                        if process.returncode or not any(r["phase"] == "verified" for r in records):
                            raise RuntimeError(f"Probe failed; see {log.name}")
                finally:
                    stop(process)
                    process.stdout.close()
        emit({"phase": "disk", "physical_db_pairs": stores, **disk_usage(root)})


def free_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def latency_summary(values):
    if not values:
        return {"statements": 0}
    ordered = sorted(values)
    return {"statements": len(values), "statement_ms_sum": sum(values),
            "statement_ms_p50": ordered[math.ceil(len(ordered) * 0.50) - 1],
            "statement_ms_p95": ordered[math.ceil(len(ordered) * 0.95) - 1],
            "statement_ms_max": ordered[-1]}


def server_memory(port):
    names = {"generic_current_allocated_bytes", "generic_heap_size"}
    url = f"http://127.0.0.1:{port}/metrics?metrics=" + ",".join(sorted(names))
    url += "&reset_histograms=false"
    with urllib.request.urlopen(url, timeout=10) as response:
        entities = json.load(response)
    return {metric["name"]: metric["value"] for entity in entities
            if entity["type"] == "server" for metric in entity["metrics"]
            if metric["name"] in names}


def ysql(args):
    master_rpc, master_http, tserver_rpc, tserver_http, sql_port, sql_http = free_ports(6)
    master_addr = f"127.0.0.1:{master_rpc}"
    master_dir = args.output / "master"
    tserver_dir = args.output / "tserver"
    base = ["--logtostderr=true", "--webserver_interface=127.0.0.1",
            "--enable_ysql=true", "--ysql_num_shards_per_tserver=1", "--callhome_enabled=false"]
    master_command = [str(args.build_root / "bin/yb-master"), *base,
                      f"--fs_data_dirs={master_dir}", f"--rpc_bind_addresses={master_addr}",
                      f"--master_addresses={master_addr}", f"--webserver_port={master_http}",
                      "--replication_factor=1", "--enable_automatic_tablet_splitting=false",
                      "--master_auto_run_initdb=true", "--memory_limit_hard_bytes=1073741824"]
    tserver_command = [str(args.build_root / "bin/yb-tserver"), *base,
                       f"--fs_data_dirs={tserver_dir}",
                       f"--rpc_bind_addresses=127.0.0.1:{tserver_rpc}",
                       f"--tserver_master_addrs={master_addr}",
                       f"--webserver_port={tserver_http}",
                       f"--pgsql_proxy_bind_address=127.0.0.1:{sql_port}",
                       f"--pgsql_proxy_webserver_port={sql_http}",
                       "--start_cql_proxy=false", "--start_redis_proxy=false",
                       "--enable_ysql_conn_mgr=false", "--ysql_pg_conf_csv=max_connections=20",
                       "--memory_limit_hard_bytes=4294967296"]
    sql_command = [str(args.build_root / "postgres/bin/ysqlsh"), "-X", "-w", "-qAt",
                   "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1", "-p", str(sql_port),
                   "-U", "yugabyte", "-d", "yugabyte"]
    sql_env = dict(os.environ, LC_ALL="C", PGCONNECT_TIMEOUT="5")
    emit({"phase": "configuration", "master": master_command, "tserver": tserver_command,
          "ddl_workers": args.ddl_workers, "batch": args.batch, "tables": args.tables,
          "alter_tables": args.alter_tables, "idle_seconds": args.idle_seconds,
          "settle_seconds": args.settle_seconds, "cpu_interval": args.cpu_interval,
          "schema_budget_seconds": args.schema_budget_seconds, "logical_cpus": os.cpu_count(),
          "platform": platform.platform()})
    processes = []
    logs = []
    started = time.monotonic()

    def roots():
        return [p.pid for p in processes]

    def measured(name, operation):
        emit({"phase": "workload_start", "workload": name})
        begin = time.monotonic()
        try:
            with CpuWindow(roots, args.output / f"cpu-{name}.jsonl", args.cpu_interval) as cpu:
                result = operation()
            summary = {"phase": "workload", "workload": name,
                       "wall_seconds": time.monotonic() - begin, **cpu.summary()}
            if isinstance(result, dict):
                summary.update(result)
            emit(summary)
            return summary
        except BaseException:
            emit({"phase": "workload_failed", "workload": name,
                  "wall_seconds": time.monotonic() - begin})
            raise

    def start():
        for name, command in (("master", master_command), ("tserver", tserver_command)):
            log = (args.output / f"{name}.log").open("ab")
            logs.append(log)
            processes.append(subprocess.Popen(command, stdout=log, stderr=log,
                                              start_new_session=True))
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if any(p.poll() is not None for p in processes):
                raise RuntimeError(f"Daemon exited; inspect logs in {args.output}")
            result = subprocess.run(sql_command + ["-c", "SHOW data_directory"],
                                    env=sql_env, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                if Path(result.stdout.strip()).resolve() != (tserver_dir / "pg_data").resolve():
                    raise RuntimeError(
                        "SQL port belongs to a different instance; refusing to use it")
                return
            time.sleep(1)
        raise TimeoutError(f"YSQL not ready; inspect logs in {args.output}")

    def sql(statement):
        subprocess.run(sql_command, input=statement, text=True, check=True, env=sql_env,
                       stdout=subprocess.DEVNULL, timeout=args.timeout)

    def statements(phase, count, make_statement):
        durations = []
        for first in range(0, count, args.batch):
            end = min(first + args.batch, count)
            jobs = []
            outputs = []
            deadline = time.monotonic() + args.timeout
            try:
                for worker in range(min(args.ddl_workers, end - first)):
                    body = [make_statement(i)
                            for i in range(first + worker, end, args.ddl_workers)]
                    prefix = args.output / f"{phase}-{first}-worker-{worker}"
                    script = prefix.with_suffix(".sql")
                    script.write_text("\\timing on\n" + "\n".join(body), encoding="utf-8")
                    output = prefix.with_suffix(".out").open("w", encoding="utf-8")
                    outputs.append(output)
                    jobs.append(subprocess.Popen(
                        sql_command + ["-f", str(script)], stdout=output, env=sql_env,
                        start_new_session=True))
                for job in jobs:
                    if job.wait(timeout=max(0.1, deadline - time.monotonic())):
                        raise RuntimeError(f"{phase} failed; inspect scripts in {args.output}")
            finally:
                for job in jobs:
                    stop(job)
                for output in outputs:
                    output.close()
            for output in outputs:
                text = Path(output.name).read_text()
                durations.extend(float(value) for value in re.findall(
                    r"^Time:\s+([0-9.]+)\s+ms", text, flags=re.MULTILINE))
        if len(durations) != count:
            raise RuntimeError(f"Expected {count} statement timings, got {len(durations)}")
        return latency_summary(durations)

    def sample(phase):
        tree = process_tree(roots())
        emit({"phase": phase, "target_tables": args.tables,
              "elapsed_seconds": time.monotonic() - started, "processes": tree,
              "rss_sum_bytes": sum(p["rss_bytes"] for p in tree),
              "master_memory": server_memory(master_http),
              "tserver_memory": server_memory(tserver_http), **disk_usage(args.output)})

    def idle(name):
        emit({"phase": "settling", "next_workload": name, "seconds": args.settle_seconds})
        time.sleep(args.settle_seconds)
        measured(name, lambda: time.sleep(args.idle_seconds))

    def shutdown():
        for process in reversed(processes):
            stop(process)
        processes.clear()
        for log in logs:
            log.close()
        logs.clear()

    def validate_schema():
        sql(f"""DO $$ BEGIN
            IF (SELECT count(*) FROM pg_class WHERE relnamespace = 'local_probe'::regnamespace
                AND relkind = 'r') <> {args.tables} THEN
                RAISE EXCEPTION 'table count mismatch';
            END IF;
            IF (SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
                WHERE c.relnamespace = 'local_probe'::regnamespace
                AND i.indisvalid AND NOT i.indisprimary) <> {args.tables} THEN
                RAISE EXCEPTION 'valid secondary index count mismatch';
            END IF;
            END $$;""")

    def validate_migration():
        sql(f"""DO $$ BEGIN
            IF (SELECT count(*) FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
                WHERE c.relnamespace = 'local_probe'::regnamespace
                AND c.relkind = 'r' AND a.attname = 'extra' AND NOT a.attisdropped)
                <> {args.alter_tables} THEN RAISE EXCEPTION 'migration column count mismatch';
            END IF; END $$;""")

    try:
        measured("startup", start)
        sample("ready")
        if not args.tables:
            idle("idle_empty")
            sample("idle_empty_memory")
            return
        schema_started = time.monotonic()
        measured("create_namespace", lambda: sql("CREATE SCHEMA local_probe;"))
        measured("create_tables", lambda: statements(
            "create_tables", args.tables, lambda i:
            f"CREATE TABLE local_probe.t_{i} (k bigint, v bigint, payload text, "
            "PRIMARY KEY (k HASH)) SPLIT INTO 1 TABLETS;"))
        measured("create_indexes", lambda: statements(
            "create_indexes", args.tables, lambda i:
            f"CREATE INDEX ON local_probe.t_{i} (v HASH) SPLIT INTO 1 TABLETS;"))
        measured("validate_schema", validate_schema)
        schema_wall = time.monotonic() - schema_started
        cold_wall = time.monotonic() - started
        emit({"phase": "schema_budget", "tables": args.tables, "indexes": args.tables,
              "target_seconds": args.schema_budget_seconds,
              "schema_wall_seconds": schema_wall, "cold_start_through_schema_seconds": cold_wall,
              "schema_under_budget": schema_wall < args.schema_budget_seconds,
              "cold_start_under_budget": cold_wall < args.schema_budget_seconds})
        measured("seed_rows", lambda: statements(
            "seed_rows", args.tables, lambda i:
            f"INSERT INTO local_probe.t_{i} SELECT k, k, repeat('v', 64) "
            f"FROM generate_series(1, {args.rows}) k;"))
        migration_started = time.monotonic()
        if args.alter_tables:
            measured("add_columns", lambda: statements(
                "add_columns", args.alter_tables, lambda i:
                f"ALTER TABLE local_probe.t_{i} ADD COLUMN extra bigint;"))
            measured("validate_migration", validate_migration)
            migration_wall = time.monotonic() - migration_started
            emit({"phase": "migration_budget", "alter_tables": args.alter_tables,
                  "target_seconds": args.schema_budget_seconds, "wall_seconds": migration_wall,
                  "under_budget": migration_wall < args.schema_budget_seconds})
        sample("loaded_memory")
        idle("idle_loaded")
        sample("idle_loaded_memory")
        shutdown()
        measured("restart", start)

        def verify():
            validate_schema()
            validate_migration()
            sql("\n".join(
                f"DO $$ BEGIN IF (SELECT count(*) FROM local_probe.t_{i}) <> {args.rows} "
                "THEN RAISE EXCEPTION 'row count mismatch'; END IF; END $$;"
                for i in range(args.tables)))

        measured("restart_verify", verify)
        idle("idle_restarted")
        sample("restart_verified")
    finally:
        shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-root", type=Path, default=Path("build/latest"))
    parser.add_argument("--output", type=Path, required=True,
                        help="New directory; never overwritten")
    parser.add_argument("--rows", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=600)
    subparsers = parser.add_subparsers(dest="experiment", required=True)
    storage_parser = subparsers.add_parser("storage")
    storage_parser.add_argument("--tablets", type=int, default=2000)
    storage_parser.add_argument("--stores", type=int, nargs="+", default=[2000, 1])
    storage_parser.add_argument("--idle-seconds", type=int, default=0)
    storage_parser.add_argument("--crash", action="store_true",
                                help="SIGKILL before close, then verify")
    storage_parser.add_argument("--no-wal", action="store_true",
                                help="Measure storage overhead without WAL; clean restart only")
    sql_parser = subparsers.add_parser("ysql")
    sql_parser.add_argument("--tables", type=int, default=1000, help="0 measures an empty instance")
    sql_parser.add_argument("--batch", type=int, default=1000)
    sql_parser.add_argument("--idle-seconds", type=float, default=60)
    sql_parser.add_argument("--settle-seconds", type=float, default=15)
    sql_parser.add_argument("--cpu-interval", type=float, default=0.5)
    sql_parser.add_argument("--alter-tables", type=int, default=None,
                            help="Tables changed by ADD COLUMN; defaults to all tables")
    sql_parser.add_argument("--schema-budget-seconds", type=float, default=30)
    sql_parser.add_argument("--ddl-workers", type=int, default=1,
                            help="Concurrent independent DDL connections; closed before sampling")
    args = parser.parse_args()
    if args.rows <= 0 or args.timeout <= 0:
        parser.error("rows and timeout must be positive")
    if args.experiment == "storage":
        if (args.tablets <= 0 or args.idle_seconds < 0 or
                any(s <= 0 or s > args.tablets for s in args.stores)):
            parser.error("Invalid tablet/store count or negative idle duration")
        if len(set(args.stores)) != len(args.stores):
            parser.error("store counts must be unique")
        if args.crash and args.no_wal:
            parser.error("--crash requires WAL")
    else:
        if args.alter_tables is None:
            args.alter_tables = args.tables
        if (args.tables < 0 or args.batch <= 0 or not 0 <= args.alter_tables <= args.tables or
                not 0 < args.idle_seconds < math.inf or
                not 0 <= args.settle_seconds < math.inf or
                not 0.1 <= args.cpu_interval <= 10 or
                not 0 < args.schema_budget_seconds < math.inf or
                not 1 <= args.ddl_workers <= 8):
            parser.error("Invalid dimensions, timing options, or ddl-workers outside 1..8")
    args.build_root = args.build_root.resolve()
    args.output = args.output.resolve()

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        args.output.mkdir(parents=True, exist_ok=False)
        (storage if args.experiment == "storage" else ysql)(args)
    except (KeyboardInterrupt, OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Experiment failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
