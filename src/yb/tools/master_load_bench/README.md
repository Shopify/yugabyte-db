# master-load-bench

Reproduces production-shaped load on a single `yb-master`: one real master, one real tserver
(the only YSQL ingress, hosting only `system.transactions`) and N in-process hollow tservers
that register, heartbeat and ack tablet RPCs but store nothing. The schema comes from a
production backup, loaded through `ysqlsh` exactly as dumped.

All commands run from the repo root.

## 1. Get the dump from the backup bucket

The bucket, project and object layout are GlobalDB specific, so the download script lives in
the globaldb-yugabyte repo, on its `fetch-backup-ysql-dump` branch until that merges:

```
python3 ~/src/github.com/Shopify/globaldb-yugabyte/tools/scripts/fetch_backup_ysql_dump.py \
    --out-dir /tmp/gdb-backup --latest
python3 ~/src/github.com/Shopify/globaldb-yugabyte/tools/scripts/fetch_backup_ysql_dump.py \
    --out-dir /tmp/gdb-backup gs://production-gdb-backups/globaldb/incremental/<TIMESTAMP>/
```

`--latest` takes the newest backup with a `success` marker; the second form takes a specific one.
It downloads only the backup's `backup.md` object (about 125 MB, reused if already complete) and
writes `extracted_YSQLDump_tablespaces` (CREATE TABLESPACE with placement JSON) and
`extracted_YSQLDump` (database, roles, tables, indexes, statistics) into the out dir, then prints
the step 2 command. The tablespaces file must load first: tables pick their tablespace with
`SET default_tablespace`.

## 2. Prepare the inputs

Needs a checkout of the globaldb-yugabyte repo and a built `master-load-bench` target:

```
python3 src/yb/tools/master_load_bench/prepare.py --out-dir /tmp/mlb-prep \
    --globaldb-repo ~/src/github.com/Shopify/globaldb-yugabyte \
    /tmp/gdb-backup/extracted_YSQLDump_tablespaces /tmp/gdb-backup/extracted_YSQLDump
```

Writes into `/tmp/mlb-prep`:

- `topology.txt`: one `cloud region zone percent` line per zone named by the tablespaces'
  `replica_placement` blocks. Percentages follow the per-zone tserver counts in the repo's
  production `cluster_configs/gdb.yml`; without `--globaldb-repo` every zone gets an equal
  share. `read_replica_placement` (observer) zones are not live-tserver zones and are skipped.
- `roles.sql`: `CREATE ROLE` for every role the dump references, so the dump's own
  "role exists" guards take the production branch.
- a copy of each input, byte-identical to the original unless one of the options below is given.

It also installs into the build whatever the dump needs from outside the release. Today that
is one file: the dump creates a text search dictionary on a Shopify-specific unaccent rules
file that production ansible copies into every node's `postgres/share/tsearch_data`. prepare.py
scans the dump for `rules = '…'`, and copies each missing file from the repo's ansible node
files into the build (`build/latest`, or `--build-dir`). Postgres only looks in that directory.

The last line printed is the `--topology=… --sql_file=…` pair to pass to the bench.

Options not used today, kept for when the load has to be cut down:

- `--drop-stats`: removes every `SELECT * FROM pg_catalog.pg_restore_*_stats(...)` call
  (40652 in the current dump). Statistics are wanted: the tserver preloads `pg_statistic`, so
  they size the catalog refetch wave the bench exists to reproduce.
- `--scale-splits N:PROD`: rewrites `SPLIT INTO n TABLETS` to `max(1, round(n × N / PROD))`,
  e.g. `100:162` to shrink a 162-tserver dump to a 100-tserver fleet. Not used because the
  tablet count per tserver is part of the production shape being reproduced; `SPLIT AT` clauses
  are left alone either way.

## 3. Load

```
build/latest/bin/master-load-bench --flagfile=src/yb/tools/master_load_bench/bench.flags \
    --num_tservers=100 \
    --topology=/tmp/mlb-prep/topology.txt \
    --sql_file=/tmp/mlb-prep/roles.sql,/tmp/mlb-prep/extracted_YSQLDump_tablespaces,/tmp/mlb-prep/extracted_YSQLDump \
    --csv_path=/tmp/mlb.csv
```

The bench wipes `--data_dir` (`/tmp/mlb` from bench.flags), starts the master and real
tserver, and spreads `--num_tservers` hollow tservers over the topology zones: each zone gets
`round(percent × N)`, at least 1, and the rounding remainder goes to the zones furthest below
their share. The transactions table is created on the real tserver and pinned there; then the
default placement switches to the hollow fleet and the SQL files run in order. After `Load
complete` the cluster idles, tservers heartbeating, until Ctrl-C (or `--run_seconds`).

While it runs: master UI on http://127.0.0.1:21101, YSQL on port 21105
(`build/latest/postgres/bin/ysqlsh -h 127.0.0.1 -p 21105 -U yugabyte`).

On exit the hollow tablet registry (`hollow_tablets.txt`) and the fleet description
(`meta.txt`) are left in `--data_dir` next to the master and tserver data.

## 4. Resume

```
build/latest/bin/master-load-bench --flagfile=src/yb/tools/master_load_bench/bench.flags \
    --resume --csv_path=/tmp/mlb.csv
```

Master and tserver restart on their existing data; the hollow tservers re-register with the
same UUIDs and zones (read from `meta.txt`, so `--topology` and `--num_tservers` are not
needed) and re-report their tablets. After `Resume complete` the cluster idles as in step 3.
Changing the fleet means a fresh load, not a resume.

**macOS: keep the fleet small.** A resume or restore with 162 tservers and ~113k tablets hangs the
master for good. The master logs one INFO line per reported tablet and one per lease grant, so
during the registration storm every heartbeat handler (up to `--rpc_workers_limit`, 4096 in
`master.flags`) ends up queued as a writer on glog's lock, a `pthread_rwlock`. Once ~3000 of them
are queued, macOS loses the handoff: the lock is free, nobody is woken, the master sits at 0% CPU
and TSHeartbeat/RefreshYsqlLease time out until it is killed. Observed 2026-10-14; lldb showed the
lock words with the held bit clear and grants minus releases equal to the waiter count. The same
snapshot resumes fine on Linux, whose futex-based rwlock just gets slow. Use the full fleet on
Linux; on a Mac stay at a few dozen tservers.

## 5. Snapshot and start from a snapshot

A snapshot is a copy of the data dir taken after the processes are stopped, so one load can
seed many later runs. Add `--snapshot_dir` to the load (or to a resume):

```
build/latest/bin/master-load-bench --flagfile=src/yb/tools/master_load_bench/bench.flags \
    --num_tservers=100 --topology=/tmp/mlb-prep/topology.txt \
    --sql_file=/tmp/mlb-prep/roles.sql,/tmp/mlb-prep/extracted_YSQLDump_tablespaces,/tmp/mlb-prep/extracted_YSQLDump \
    --snapshot_dir=/tmp/mlb-snapshot
```

On Ctrl-C the bench stops the tserver and master and copies the master data, tserver data,
`hollow_tablets.txt` and `meta.txt` into `/tmp/mlb-snapshot` (replacing its contents).

To start from it:

```
build/latest/bin/master-load-bench --flagfile=src/yb/tools/master_load_bench/bench.flags \
    --from_snapshot=/tmp/mlb-snapshot --csv_path=/tmp/mlb.csv
```

This copies the snapshot back into `--data_dir` and then behaves like `--resume`. Tablet
metadata stores absolute paths, so a snapshot only restores into the `--data_dir` it was taken
from.

## 6. Scenario file

`--scenario=<file>` runs after `--sql_file` (or after a resume/restore). One directive per line,
parameters as `key=value` before any free text, `#` starts a comment. Directives run top to
bottom and each blocks until done; `parallel` is the only way two overlap. Durations take
`ms`, `s`, `m`, `h`.

### Directives

| Directive | Meaning |
|---|---|
| `sql [keys]: <statement>` | Run the statement through one ysqlsh session on the real tserver. `sql [keys]: << ... >>` runs a multi-line block. The colon is always present. DDL and catalog queries only: user tables live on the hollow tservers, which store nothing, so DML or a SELECT on one fails and aborts the run. |
| `sql_file <path>` | Run a whole file the same way; relative to the scenario file. |
| `database <name>` | Database for the following `sql`, `sql_file` and catalog batch recordings. |
| `rpc method=<Name> ts=<sel> [keys][: <body>]` | Send one master RPC from the selected hollow tservers. `method` is the proto method name, see [RPC methods](#rpc-methods); an unknown name fails the parse. The optional body after the colon is the request in protobuf text format; `: << ... >>` for a multi-line one. |
| `ysql_connect ts=<sel> [keys]` | Simulate new YSQL connections on the selected hollow tservers: each replays the catalog Read batch recorded from a fresh connection on the real tserver, stamped with its own last-received catalog version and a fresh read time. Every connection sends the batch. |
| `sleep <dur>` | Pause. |
| `mark <label>` | Write a `mark` row to the CSV and set the `phase` column for the rows that follow. |
| `stop <name>` | End the named directive early. |
| `serial << ... >>` | Run the members in order; a sequential branch inside `parallel`. |
| `parallel << ... >>` | Start all members together, end when the last one ends. |
| `repeat count=N << ... >>` | Run the block N times in order. |

Repetition keys, shared by `sql`, `rpc` and `ysql_connect`:

| Key | Meaning |
|---|---|
| `count=N` | Send N times. Default 1. |
| `interval=<dur>` | Spacing between sends; needs `count`, `duration` or `name`. `interval=0` with a `duration` means do not send (`rpc` and `ysql_connect` only). |
| `duration=<dur>` | Keep sending at `interval` for this long. |
| `name=<id>` | Lets `stop <id>` end it from another branch of a `parallel`; with `interval` alone the directive runs until stopped. |

`ts=` is required for `rpc` and `ysql_connect` and selects hollow tservers by number, 1 to
`--num_tservers`, matching the uuid suffix: `all`, `7`, `1-40`, `3,17,90`.

For `rpc` and `ysql_connect` the directive replaces, for its duration, whatever the selected
tservers were doing with that method; when it ends they return to the default schedule from the
flags: `TSHeartbeat` and `RefreshYsqlLease` every 1 s, `ListTabletServers` every 60 s,
`ysql_connect` as `--ysql_connect_interval_ms` (0 = off), nothing for other methods. Other
methods are sent with an empty request and count as failed when the response carries an error. So
`rpc method=TSHeartbeat ts=1-40 interval=100ms duration=60s` is a heartbeat storm from 40
tservers, and `rpc method=RefreshYsqlLease ts=17 interval=0 duration=120s` silences one.

### RPC methods

Three methods are sent the way a real tserver sends them: `TSHeartbeat` with the tserver's
registration, tablet report and metrics; `RefreshYsqlLease` with its lease state;
`ListTabletServers` as the periodic connectivity check; they take no body. Any other method of
the master services (`src/yb/master/master_*.proto`) is accepted too, goes out with the body
given after the colon (empty without one), and counts as failed when the response carries an
error. The body is the request message in protobuf text format and is checked at parse time.
The master cannot look up a YSQL table by name (schemas make it ambiguous), so a
`TableIdentifierPB` for one needs `table_id`; `ListTables` with a `name_filter` is the way to
go by name:

```
rpc method=ListTables ts=1-10 count=100 interval=50ms: <<
  namespace { name: "core" database_type: YQL_DATABASE_PGSQL }
  name_filter: "orders"
>>
```

### Functions

`@name()` inside a `sql` statement or an `rpc` body is replaced before every send:

| Function | Value |
|---|---|
| `@iter()` | Iteration of the innermost enclosing `repeat`, from 1. Parse error outside `repeat`. |
| `@seq()` | Send number within the directive, from 1; 1 for a directive that sends once. |
| `@uuid()` | Sending hollow tserver's uuid. `rpc` only. |
| `@index()` | Sending hollow tserver's `ts=` number. `rpc` only. |

A `@` not followed by one of these names and `()` is left as written, so SQL operators such as
`@>` are unaffected. `sql_file` contents are not expanded.

```
repeat count=50 <<
  sql: CREATE TABLE t_@iter() (k int PRIMARY KEY) SPLIT INTO 3 TABLETS;
>>
sql count=20 interval=1s: CREATE TABLE t_@seq() (k int PRIMARY KEY);
```

Nesting alternates: `parallel` directly inside `parallel`, or `serial` directly inside `serial`,
is a parse error because it means the same as the flattened block.

### Examples

```
mark baseline
sleep 60s

mark ddl_only
sql count=20 interval=3s: ALTER TABLE core.orders ADD COLUMN c1 int;

mark ddl_with_connections
parallel <<
  ysql_connect ts=all interval=1s duration=60s
  sql count=20 interval=3s: ALTER TABLE core.orders ADD COLUMN c2 int;
>>
```

```
repeat count=3 <<
  parallel <<
    rpc name=hb_storm method=TSHeartbeat ts=1-40 interval=100ms duration=300s
    serial <<
      sql count=10 interval=2s: ALTER TABLE core.orders ADD COLUMN c int;
      sql: ALTER TABLE core.orders DROP COLUMN c;
      stop hb_storm
    >>
  >>
  sleep 60s
>>
```

### Catalog batch recording

After the preload (or on restore) the bench opens one fresh connection on the real tserver; the
relay records the sys catalog Read batch that connection's catalog preload sends to the master.
Whenever a TSHeartbeat response carries a new catalog version the bench opens another connection
and re-records, so `ysql_connect` always replays what a backend would read now. The real
tserver runs with production's `ysql_catalog_preload_additional_table_list` (tserver.flags), so
the batch has production's shape and the internal relcache-init connection is never involved.

### Output

The CSV is long format, one row per metric per 5 s sample: `epoch_ms, elapsed_sec, phase,
metric, value`. Per sample: master CPU seconds and RSS, the master Prometheus series matching
`--metric_substrings` (per-method handler latency, queue depth and time, overflows, bytes), and
the bench's own counters `bench_rpc_<Method>_{ok_total,failed_total,latency_us_sum,latency_count}`
and `bench_ysql_connect_{...}`. Written immediately rather than per sample: `mark,<label>` and
`bench_sql_<name|lineN>_latency_ms` per `sql` statement.

`report.py` prints, per window between marks, master CPU and RSS, per-method rate and mean
latency, queue depth, failures and `sql` latencies:

```
python3 src/yb/tools/master_load_bench/report.py /tmp/mlb-smoke.csv
python3 src/yb/tools/master_load_bench/report.py before.csv after.csv
```

The two-file form compares a run before a master change with one after: each window shows both
runs side by side with the delta per metric. The bench appends to an existing `--csv`, so a file
can hold several runs; the report takes the last one and says so.

## Files

| File | Purpose |
|---|---|
| `bench.flags`, `bench-linux.flags` | bench flags for a Mac / a 192-core Linux box |
| `master.flags`, `tserver.flags` | flags for the spawned master and real tserver |
| `prepare.py` | step 2 |
| `preload.scenario` | example `--scenario` file (directives run after `--sql_file`) |
| `report.py` | step 6 output post-processing |
