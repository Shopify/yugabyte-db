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

"""Measure standalone single-session schema provisioning, data validation, idle CPU, and restart.

Uses disposable public.t_N tables in a NEW runtime directory. Schema timing excludes fixture
insertion, idle windows, and restart. This is not a migration or concurrent-transaction benchmark.
"""

import argparse
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import subprocess
import sys
import time

from cpu_usage import CpuWindow, snapshot

REPO = Path(__file__).resolve().parents[4]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--tables', type=int, default=1000)
    parser.add_argument('--idle', type=float, default=60)
    parser.add_argument('--batch', type=int, default=100)
    parser.add_argument('--settle', type=float, default=15)
    args = parser.parse_args()
    if args.tables < 1 or args.batch < 1 or args.idle < 0 or args.settle < 0:
        parser.error('tables/batch must be positive and idle/settle nonnegative')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    command = [str(REPO / 'build/latest/bin/yb-local-readonly'),
               '--experimental_catalog_writes', '--data_dir=' + str(args.output / 'data'),
               '--sql_port=' + str(port)]
    ysql = [str(REPO / 'build/latest/postgres/bin/ysqlsh'), '-X', '-A', '-t', '-q',
            '-h', '127.0.0.1', '-p', str(port), '-U', 'yugabyte', '-d', 'yugabyte',
            '-v', 'ON_ERROR_STOP=1']
    server = None
    server_log = None
    sql_serial = 0

    def emit(value):
        print(json.dumps(value, sort_keys=True), flush=True)

    def roots():
        return [server.pid] if server is not None and server.poll() is None else []

    def start(label):
        nonlocal server, server_log
        begin = time.monotonic()
        server_log = (args.output / (label + '.stderr')).open('w')
        server = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=server_log, text=True)
        with selectors.DefaultSelector() as selector:
            selector.register(server.stdout, selectors.EVENT_READ)
            if not selector.select(60):
                raise RuntimeError('Readiness timeout')
            ready = json.loads(server.stdout.readline())
        assert ready['state'] == 'ready' and ready['sql_port'] == port
        assert ready['mode'] == 'experimental-serialized-sql'
        emit({'phase': label, 'wall_seconds': time.monotonic() - begin, 'ready': ready})

    def stop():
        nonlocal server, server_log
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=20)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
                raise
            assert server.returncode == 0, server.returncode
            server = None
        if server_log is not None:
            server_log.close()
            server_log = None

    def sql(text):
        nonlocal sql_serial
        sql_serial += 1
        source = 'SET enable_seqscan=off;\n' + text + '\nSELECT pg_backend_pid();\n'
        (args.output / f'sql-{sql_serial}.sql').write_text(source)
        result = subprocess.run(ysql, input=source, text=True, capture_output=True, timeout=180)
        (args.output / f'sql-{sql_serial}.log').write_text(result.stdout + result.stderr)
        if result.returncode:
            raise RuntimeError(result.stderr[-3000:])
        rows = result.stdout.strip().splitlines()
        pid = int(rows.pop())
        deadline = time.monotonic() + 5
        while True:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            if time.monotonic() > deadline:
                raise RuntimeError('SQL backend did not exit')
            time.sleep(0.005)
        return rows

    def statements(make):
        for start_at in range(0, args.tables, args.batch):
            end_at = min(args.tables, start_at + args.batch)
            sql('\n'.join(make(i) for i in range(start_at, end_at)))

    def measure(name, action):
        with CpuWindow(roots, args.output / (name + '-cpu.jsonl')) as cpu:
            begin = time.monotonic()
            result = action()
            wall = time.monotonic() - begin
        emit({'phase': name, 'wall_seconds': wall, **cpu.summary()})
        return result

    def memory(name):
        state = snapshot(roots)
        assert state['complete']
        emit({'phase': name, **state})

    def validate_schema():
        rows = sql("SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace "
                   "AND relname ~ '^t_[0-9]+$' AND relkind='r';"
                   "SELECT count(*) FROM pg_index i JOIN pg_class t ON t.oid=i.indrelid "
                   "WHERE t.relnamespace='public'::regnamespace AND t.relname ~ '^t_[0-9]+$' "
                   "AND NOT i.indisprimary AND i.indisvalid AND i.indisready;")
        assert rows == [str(args.tables), str(args.tables)], rows
        statements(lambda i:
            f"DO $$ BEGIN IF NOT (SELECT num_tablets=1 "
            f"AND num_hash_key_columns=1 AND NOT is_colocated "
            f"FROM yb_table_properties('t_{i}'::regclass)) OR NOT "
            f"(SELECT num_tablets=1 AND num_hash_key_columns=1 AND NOT is_colocated "
            f"FROM yb_table_properties('t_{i}_v_idx'::regclass)) THEN "
            f"RAISE EXCEPTION 'bad topology {i}'; END IF; END $$;")

    def validate_rows():
        statements(lambda i:
            f"DO $$ BEGIN IF (SELECT count(*) FROM t_{i})<>4 OR "
            f"(SELECT payload FROM t_{i} WHERE v=1) IS DISTINCT FROM repeat('v',64) THEN "
            f"RAISE EXCEPTION 'bad rows {i}'; END IF; END $$;")
        for i in {0, args.tables - 1}:
            plan = '\n'.join(sql(f'EXPLAIN (COSTS OFF) SELECT payload FROM t_{i} WHERE v=1;'))
            assert f't_{i}_v_idx' in plan, plan

    try:
        emit({'phase': 'configuration', 'tables': args.tables, 'batch': args.batch,
              'idle_seconds': args.idle, 'settle_seconds': args.settle,
              'ddl_workers': 1, 'rows_per_table': 4, 'hash_tablets_per_relation': 1,
              'enable_seqscan': False})
        cold = time.monotonic()
        start('start')
        memory('empty_memory')
        schema = time.monotonic()
        measure('create_tables', lambda: statements(lambda i:
            f'CREATE TABLE t_{i}(k bigint,v bigint,payload text,PRIMARY KEY(k HASH)) '
            'SPLIT INTO 1 TABLETS;'))
        measure('create_indexes', lambda: statements(lambda i:
            f'CREATE INDEX ON t_{i}(v HASH) SPLIT INTO 1 TABLETS;'))
        measure('validate_schema', validate_schema)
        schema_wall = time.monotonic() - schema
        cold_wall = time.monotonic() - cold
        emit({'phase': 'schema_budget', 'tables': args.tables, 'indexes': args.tables,
              'schema_wall_seconds': schema_wall, 'cold_start_through_schema_seconds': cold_wall,
              'schema_under_30_seconds': schema_wall < 30, 'cold_under_30_seconds': cold_wall < 30})
        measure('seed_rows', lambda: statements(lambda i:
            f"INSERT INTO t_{i} SELECT k,k,repeat('v',64) FROM generate_series(1,4) k;"))
        measure('validate_rows', validate_rows)
        memory('loaded_memory')
        if args.idle:
            time.sleep(args.settle)
            measure('loaded_idle', lambda: time.sleep(args.idle))
            memory('loaded_idle_memory')
        stop()
        start('restart')
        measure('restart_validate', lambda: (validate_schema(), validate_rows()))
        memory('restarted_memory')
        if args.idle:
            time.sleep(args.settle)
            measure('restarted_idle', lambda: time.sleep(args.idle))
        emit({'phase': 'complete', 'tables': args.tables, 'indexes': args.tables})
    finally:
        stop()

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(128 + signum))
    main()
