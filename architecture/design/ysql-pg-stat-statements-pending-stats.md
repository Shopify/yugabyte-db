# pg_stat_statements: backend-local pending statistics

Upstream `pg_stat_statements` updates the shared entry of a statement at the end of every
statement: `pgss_store` takes `pgss->lock` (an LWLock) shared and then the entry's spinlock, once for
the execution and, with `track_planning` on (the YugabyteDB default), once more for the plan. When
many backends run the same statement shape, they all write one entry's cache lines and the
LWLock's word. On a CPU-saturated host a descheduled spinlock holder sends every waiter into
`s_lock` back-off sleeps, which `pg_stat_activity` does not show.

A client backend therefore keeps the statistics of its statements in local memory and folds them
into the shared entries at most once per `pg_stat_statements.yb_flush_interval` (default 1000 ms).
On the per-statement path it writes no shared memory and takes no shared lock; it only reads one
shared counter. With `yb_flush_interval = 0` every statement takes the upstream path unchanged.

All of this lives in `src/postgres/contrib/pg_stat_statements/pg_stat_statements.c`.

## Pending records

Each backend has a hash table, `pgssHashKey -> YbPgssPending`, in its own memory context under
`TopMemoryContext`. A record holds:

- the `incarnation` of the shared entry it belongs to and the `gen` read with it;
- deltas, kept in a `pgssEntry` so that a store updates them with the same code as a shared entry:
  the additive counters are sums; for each kind (plan, exec) `calls`, `total_time`, `min`/`max`
  and the Welford `mean`/`sum_var` of this batch only; a latency histogram with the shared
  histograms' configuration (`cfg`); and `yb_slow_executions`.

Retry counts, `yb_stats` and the query-diagnostics accumulation (`YbQueryDiagnosticsAccumulatePgss`)
are read or done at store time, as on the synchronous path.

## Knowing that the shared entry still exists

Two fields in `pgssSharedState`, each on its own cache line:

- `removal_gen` (`pg_atomic_uint64`): bumped under the exclusive `pgss->lock` by every
  `entry_reset` call, whatever its form and even when nothing is removed, and by every
  `entry_dealloc`. It is written only on reset and eviction, so its line stays shared in every
  cache.
- `next_incarnation`: source of `pgssEntry.yb_incarnation`, assigned by `entry_alloc` to each new
  entry (never 0). Written under the exclusive lock, on a line of its own so it does not invalidate
  `removal_gen`'s.

Both are initialized before the saved statistics are loaded at startup, because the load creates
entries through `entry_alloc`.

A record is **current** when its `gen` equals `removal_gen`: no entry has been removed since its
incarnation was observed, so its entry exists. The invariant that makes this exact is that `gen`
and `incarnation` are read together under `pgss->lock`. Removals bump `removal_gen` only under the
exclusive lock, so a read under the shared lock pairs the generation exactly with the incarnation
seen. A generation read after unlocking could postdate a reset that removed the entry; the record
would then look current, and the statements it collects would be dropped at the fold.

## Store

For a statement with `jstate == NULL` in a client backend with a nonzero interval:

1. Read `removal_gen` without a lock. If the backend has a current record for the key, add the
   statement to it. No lock, no shared write, no text work. This path skips the normalization
   discard rule of the synchronous path; that is correct only because a current record proves that
   the entry exists, so no text is stored.
2. If the record is stale, fold first (below). The fold drops the record's data if its entry is
   gone, or merges it and makes the record current again, in which case step 1 applies.
3. Otherwise run the synchronous path: lookup or creation of the entry with its text, the
   `yb_pgss_needs_normalization` discard rule, `yb_max_query_text_length`, redaction of sensitive
   utility statements, `YbSetPgssNormalizedQueryText`, and the counters updated under the
   spinlock. In the same critical section it reads the entry's incarnation and `removal_gen`, and
   after unlocking it starts or refreshes the record with them. A discarded statement starts no
   record.

So the first store for a key in a backend, and the first after any reset or eviction, is
synchronous and visible to everyone at once. With `track_planning` on, that is the plan of the
first execution; the execution itself is already pending.

`pgss_post_parse_analyze` skips its jstate store when the record is current, since the entry
exists. Utility statements, including the backfill worker's `BACKFILL INDEX`, take the same path.

The interval is effectively 0 for anything other than a client backend (`MyBackendType !=
B_BACKEND`): background workers, walsenders and the pg_cron workers update the shared entries per
statement. At interval 0 a store first folds whatever is still pending from an earlier nonzero
setting, then runs the synchronous path; there is no GUC assign hook, because assign hooks also run
during transaction abort, where shared-memory work is unsafe.

## Fold

`yb_pgss_fold_pending` takes `pgss->lock` shared once and, for each record holding data, looks
up its key:

- entry absent, or `yb_incarnation` differs: drop the data. These statements predate a reset or
  eviction of the entry, which discards them anyway. The record stays stale, so its next store is
  synchronous.
- otherwise, under the entry's spinlock: unstick a sticky entry as a store would; add the additive
  counters, `calls` and `total_time`; merge `min`/`max`; merge `mean` and `sum_var` with the
  pairwise (Chan et al.) update; `hdr_add` the histogram and add `yb_slow_executions`; and add
  `USAGE_EXEC` once per folded store to `usage`, so eviction order sees the same usage as the
  synchronous path. After the spinlock, `YbSetPgssNormalizedQueryText` when query diagnostics is
  on. The record's `gen` is then refreshed from `removal_gen` read under the same lock, which pairs
  exactly with the incarnation just checked; an unrelated reset or eviction therefore costs no
  trip through the synchronous path.

The fold neither allocates nor raises errors, and a `yb_pgss_in_fold` flag keeps it from being
re-entered.

Triggers:

- the end of a store, when the oldest pending statistics (`yb_pgss_pending_since`) are at least
  `yb_flush_interval` old. The check uses a monotonic `instr_time`, reusing the end instant the
  planner and utility hooks already take;
- reaching 64 pending keys. After that fold the table is destroyed, and each key takes the
  synchronous path once more;
- the start of `pg_stat_statements_internal` (every `pg_stat_statements*` view),
  `yb_get_histogram_jsonb`, `pg_stat_statements_info` and both `pg_stat_statements_reset`
  functions, so a session reads and resets its own statements like everyone's;
- backend exit, through a `before_shmem_exit` callback registered with the first record. It is
  registered after `ShutdownPostgres`, so on a FATAL exit it runs before LWLocks are released; it
  does nothing if shared state is missing, a fold is in progress or `pgss->lock` is held. Exit
  folds run before the postmaster's shutdown dump, so a clean restart keeps them.

## What other sessions see

A completed statement becomes visible to other sessions when its backend folds: at the end of a
statement that finishes at least `yb_flush_interval` after the oldest statistics it holds, when it
reads or resets `pg_stat_statements`, or when it exits. Its own reads see it at once. A backend
killed without running exit callbacks (SIGKILL, crash) loses its pending statistics.

Readers in other processes fold nothing: the tserver webserver's `/statements` endpoint
(`getYsqlStatementStats`), and remote reads through global views such as
`gv$partial_pg_stat_statements`. They see the shared entries as of the last folds. The webserver's
reset (`resetYsqlStatementStats`) still goes through `entry_reset`, so it bumps `removal_gen`.
Tests that read counters written by another backend run at `yb_flush_interval = 0`.

`mean` and `stddev` of a folded entry equal what adding the statements one by one would give, up
to floating-point rounding. QPM (`yb_pg_stat_plans`) keeps its own sequential mean, which equals
pgss's `mean_exec_time` exactly only at `yb_flush_interval = 0`. Merging histograms with
`hdr_add` re-records each bucket at its lowest value, so the shared histogram's `min_value` and
`max_value` become bucket bounds; no reader uses them.

## Memory

A record and its deltas take about 2.7 KB with the default histogram configuration, so a backend
holds at most about 175 KB of pending statistics. A backend that cycles through more than 64
shapes per interval folds at the cap and does no worse than the synchronous path.

## On-disk layout

`pgss_shmem_shutdown` writes raw `pgssEntry` bytes and `read_entry_hdr` reads them back with
`sizeof(pgssEntry)`, under `PGSS_FILE_HEADER` `0x20250425`. `yb_incarnation` (`uint32`) therefore
sits in the padding after `mutex` (`slock_t` is 1 byte on x86_64 and 4 on aarch64; both leave 4
bytes before the 8-aligned `yb_slow_executions`). Restore copies only the key, counters, text
metadata and the bytes from `yb_slow_executions` on, so the padding is ignored in both directions
and files stay compatible across upgrade and rollback. `YbPgssEntryDumpLayout` freezes the layout
and static assertions check the size and offsets against it. A new field anywhere else in
`pgssEntry` needs a new file header and reader.

## Rejected alternatives

- Per-backend pending slots in shared memory, summed on read: exact with no lag, but
  `max_connections` x slots of shared memory, a scan of every slot per read, and fold-versus-read
  exclusion.
- Atomics for the hot counters: every backend still writes one cache line, and min/max/Welford
  cannot be updated atomically together.
- Striping each entry: memory times the stripe count for every entry, and `pgss->lock` stays per
  statement.
- A shorter spinlock critical section: the same number of acquisitions, plus the LWLock word.
- An LWLock per entry: no spinlock back-off, but the shared word and semaphore wake-ups stay per
  statement.
- Folding whenever a backend goes idle: in non-pipelined OLTP every statement is followed by idle,
  so this restores per-statement shared updates.
