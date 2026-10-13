# master-load-bench

Reproduces production-shaped load on a single `yb-master`: one real master, one real tserver
(the only YSQL ingress, hosting only `system.transactions`) and N in-process hollow tservers
that register, heartbeat and ack tablet RPCs but store nothing. The schema comes from a
production backup, loaded through `ysqlsh` exactly as dumped.

All commands run from the repo root.

## 1. Get the dump from the backup bucket

The bucket, project and object layout are GlobalDB specific, so the download script lives in
the globaldb-yugabyte repo:

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

## Files

| File | Purpose |
|---|---|
| `bench.flags`, `bench-linux.flags` | bench flags for a Mac / a 192-core Linux box |
| `master.flags`, `tserver.flags` | flags for the spawned master and real tserver |
| `prepare.py` | step 2 |
| `preload.scenario` | example `--scenario` file (directives run after `--sql_file`) |
