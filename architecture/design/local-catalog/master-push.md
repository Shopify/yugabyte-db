# Tserver-local catalog, step 1: master pushes the sys catalog WAL

## Goal

Each tserver will keep a local copy of the YSQL system catalog, so backends read catalog rows
locally instead of from the master. This step builds the replication channel: the master leader
pushes the system catalog WAL to every tserver that holds a YSQL operation lease, and tracks how far
each copy has applied. The tserver side is a stub that does not apply anything yet; applying
records to a real catalog is later work.

Everything is off unless `enable_local_tserver_catalog` (non-runtime, default false) is set. The
master also pushes only while the YSQL operation lease is on (`enable_ysql_operation_lease` or
`enable_object_locking_for_table_locks`) and `ysql_yb_enable_invalidation_messages` is true.

## The contract: safe time S and position E

A push cycle certifies a pair (S, E) such that every catalog transaction committed at or before
hybrid time S has its records in the WAL at or before index E. A copy that has applied every entry
up to E may then serve catalog reads at S.

The certification is the one xCluster uses for its apply safe time:

1. `SafeTime(RequireLease::kTrue)` on the sys catalog tablet gives S. Without a leader lease it
   fails, so a deposed leader cannot certify.
2. `ResolveIntents(S)` waits until every transaction that may commit at or before S has its apply
   record in the WAL.
3. E is the `majority_replicated_index` at that point.
4. The leader term is checked again; if it changed, the cycle ends as if this master were not the
   leader.

Each copy is then sent the records it lacks up to E. Only the batch that reaches E carries S, so a
copy can never report S without holding every entry up to E.

## Wire protocol (`tserver/local_catalog.proto`)

```
PushCatalogChanges(PushCatalogChangesRequestPB) -> PushCatalogChangesResponsePB

Request:  from_op_id      position the batch starts at; must equal the copy's position
          to_op_id        position after applying the batch
          records         cdc.CDCRecordPB, the xCluster record format
          safe_time       only on the batch that reaches E
          reseed_required the copy cannot be continued from this log; no records
Response: code            OK | GAP | NO_COPY
          applied_op_id   the copy's position after this push (always set)
          safe_time       the copy's safe time, when it has one
```

The master trusts `applied_op_id` rather than the code: whatever the code, the next batch starts at
the position the copy reports. That makes GAP, duplicates and lost answers self-correcting without
special cases. An answer without a code this master knows, or without a position, ends the
target's cycle.

A tserver without the flag has no such service. The master maps the RPC's "no such service" or
"no such method" error to "no copy" and does not count it as a failure, which is also how a mixed
version cluster behaves during an upgrade. The flag is not runtime, so such a tserver is not pushed
to again until it holds a new lease; a tserver that has the service but answers NO_COPY is pushed to
again once the WAL moves. Nothing is persisted, so rollback is a flag flip and a
restart.

## Master side: `SysCatalogPusher` (`master/sys_catalog_pusher.{h,cc}`)

### Lifecycle

`CatalogManager` creates the pusher once the sys catalog is loaded (`StartSysCatalogPusher`) and
shuts it down before the sys catalog on master shutdown and before `GoIntoShellMode`. The metrics
object is created once and outlives the pusher, because the pusher is recreated when the master
leaves shell mode. The pusher is created, started and published in one `mutex_` section, and only if
the catalog manager is not closing. A shutdown that started first gets no pusher; one that starts
later finds a running pusher and stops it. Stopping the pusher returns only once its thread has
exited, for every caller: a master shutdown that races `GoIntoShellMode` waits too, so the sys
catalog never shuts down under a running cycle.

The pusher runs one thread. Every `local_catalog_push_interval_ms` (default 60 s) it runs a cycle,
skipping it when the WAL has not moved since the last successful cycle, that cycle caught up every
target that keeps a copy, and the lease holders are exactly the last cycle's targets.
`RequestCycle()` wakes the thread for a cycle that is never skipped; requests made before it starts
share it. The catalog manager requests one once a new leader has loaded the catalog, so the copies
do not keep the old leader's safe time for up to an interval. After a failed cycle the thread runs
the next one within 1 s rather than a full interval. Tests run cycles with `TEST_RunCycle()`, which
never skips one either. Cycles are serialized by `cycle_mutex_`; a master that is not the leader
returns nullopt and resets its gauges.

### Object locking off

The pusher needs the YSQL operation lease, not object locking: its targets are the lease holders,
and without object locking the lease is held under `enable_ysql_operation_lease`. Serving the copy
without object locking needs more outside the pusher: the DDL commit path sending a release with no
locks, a release handler that runs without a lock manager, and the tserver publishing its copy's
catalog versions after each apply.

`RequestCycle()` is where the pushes still to come will enter: one after the release of a
version-incrementing DDL, which without object locking is the only repair for a lost release
because master holds no lock records to resend it from, and one for a tserver waiting for its copy
to reach a time. Both need a request to carry that time and to complete once the tserver's copy has
reached it (TODO on `RequestCycle`).

### Targets

The targets of a cycle are the tservers with a live YSQL operation lease, read in one locked pass
with their lease epochs (`ObjectLockInfoManager::GetLiveLeases`) when the cycle starts; the skip
check and the target list use that one snapshot. Replaced tservers are left out: their address now
reaches the tserver that replaced it. The `targets_` map is rebuilt each cycle, so tservers that
lose their lease drop out. A `Target` keeps, across cycles, the copy's
last reported position and safe time and whether it has no copy. A new lease epoch means a new
process, so that state is reset and the copy is asked for its position again.

Targets are held by value. The send queue holds `Target*` during a cycle, so `targets_` must not
change between `RebuildTargets` and the end of the send loop. `RebuildTargets` moves each kept
target's node into the new map with `extract`, so a kept target is not copied.

### The send loop (`SendToTargets`)

One thread drives many async RPCs through a bounded window:

- `waiting` (`std::deque<Target*>`) is a FIFO of targets that still need a push. A target is
  queued at the start and re-queued after each answer that leaves it behind, at the back, so a far
  behind copy does not monopolize the window.
- At most `local_catalog_push_max_inflight_rpcs` (default 64) RPCs are in flight, and at most one
  per target. Batches to one copy are strictly sequential, each built from its last reported
  position.
- RPC callbacks only append to an `Inbox` (mutex, condition variable, deque of answers). The
  thread running the cycle takes answers one at a time, and cycles are serialized by
  `cycle_mutex_`, so cycle state is touched by one thread at a time. Callbacks capture the inbox
  by `shared_ptr` and never the pusher, so a late answer after destruction is harmless; once the
  inbox is closed at shutdown, it drops them. A cycle ends only once every push it sent is
  answered, so every answer belongs to the cycle taking it.
- A target leaves the cycle when it is caught up, fails (an RPC error, no proxy, or a failed batch
  build or refused reseed, both of which also fail the cycle), has no copy, answers a reseed,
  answers without a known code or position, repeats the same answer, or stalls (answers without
  progress) more than 3 times. These rules end the loop for a copy that reports only safe times it
  was sent: its index cannot rise past the log's end without a reseed.

### Batch sharing (`CycleState::slots`)

Targets at the same position are sent the same batch, which is the common case: one batch is built
and sent to every copy.

```cpp
struct Batch { tserver::PushCatalogChangesRequestPB request; size_t bytes; ScopedTrackedConsumption consumption; };
struct Slot  { int users = 0; std::optional<Batch> batch; };
std::map<OpId, Slot> slots;   // keyed by from_op_id
```

`Enqueue` adds a user to the slot of the target's position, `Release` (on answer or failure)
removes one, and the slot is freed with its batch when no queued or in-flight target needs it. An
answered target is queued again before its slot is released, so a target sent the same position
again reuses the batch. The
batch is built lazily, when the first target at that position is sent. `std::map` is used because
its nodes do not move, so the `const Batch*` handed out stays valid while other slots come and go.
Each batch is charged to the pusher's memory tracker at its serialized size until its slot is
freed.

### Building a batch (`BuildBatch`)

Checks before reading the log, in order:

- A position with an unknown term behind E: **reseed**.
- A position whose entry in this log has a different term, is no longer in the log, or is past the
  log's end: **reseed**.
- A position at or past E: an empty batch, carrying S if the position's term is known. This is the
  steady state: the WAL has not moved but the safe time advances.
- A position at index 0: **reseed**.

Every send of a reseed, including one built for an earlier target in the cycle, first checks that
this master is still leader in the cycle's term, so a deposed leader cannot tell copies to throw
theirs away; otherwise the cycle fails. A leader that has lost its lease is refused the same way,
since it may already have been replaced, but the cycle ends with TryAgain and is not counted as a
failure.

Then the read loop. Entries are read with `ReadReplicatedMessagesForXCluster` (committed entries
only) and converted with `PopulateXClusterRecords`, so the pusher reuses the xCluster reader and
record format. A batch ends at E (and then carries S), after a change metadata entry, or once its
records reach `local_catalog_push_batch_max_bytes` (default 1 MB, checked after each entry). If the
log no longer holds the next entry, or a read returns nothing (committed index behind, or the log
cache over its memory limit), the copy is sent what was read so far; with nothing read, a missing
entry reseeds and an empty read waits until the RPC timeout.

## Metrics

All metrics are on the master's cluster metric entity, prefixed `local_catalog_push_`. They are
owned by `SysCatalogPusherMetrics`, which `CatalogManager` creates once, so counters keep counting
across shell mode and pusher restarts.

Gauges describe the last cycle on this master. They are reset to 0 when the master gains or loses
leadership, so only the leader reports nonzero values.

| Gauge | Meaning |
|---|---|
| `targets` | Tservers holding a YSQL operation lease in the last cycle. |
| `targets_caught_up` | Targets whose copy reached E and S of the last cycle. |
| `targets_without_copy` | Targets that answered NO_COPY, including tservers without the service. |
| `max_target_lag_ops` | WAL entries between E and the copy furthest behind it, among copies that reported a position. |
| `safe_time_lag_ms` | Time since S of the last cycle that caught up every copy. 0 while timed cycles are skipped (nothing to push) and on a non-leader. |

`safe_time_lag_ms` is the staleness signal: it bounds how old a catalog read served from the
slowest copy can be. It is a function gauge computed on read, so it keeps growing while cycles
fail or leave a copy behind, and drops back once a cycle catches every copy up. A newly elected
leader starts it from the election time, not 0, until its first full catch-up.

| Counter | Counts |
|---|---|
| `cycles` | Cycles that certified (S, E). |
| `cycles_skipped` | Timed cycles skipped because the WAL had not moved and every copy was caught up. |
| `cycle_failures` | Cycles that failed, such as certification or a batch build. A cycle cut short by a leader change, TryAgain or shutdown is not counted, nor is an error while preparing a cycle or deciding to skip it. |
| `rpcs` | Push RPCs sent. |
| `rpc_failures` | Pushes with no proxy to the tserver or whose RPC failed; "no such service" is not counted. A failed batch build counts in `cycle_failures`. |
| `reseeds` | Pushes telling a copy it must be rebuilt. |
| `stalls` | Answers without progress, other than answers to a reseed. |
| `bytes`, `records` | Request bytes and change records sent, counted once per RPC. |

| Event stats | Measures |
|---|---|
| `cycle_duration` (us) | Duration of a cycle that certified (S, E). |
| `resolve_intents_wait` (us) | Time `ResolveIntents(S)` waited. |
| `batch_bytes` | Serialized size of each batch built, once per batch regardless of how many targets share it. |

Batches in memory are charged to the `SysCatalogPusher` memory tracker until their slot is freed.

What to watch: a growing `safe_time_lag_ms` or `max_target_lag_ops` means a copy is falling
behind; `reseeds` rising without restarts or failovers means copies are being outrun by log GC;
`stalls` and `rpc_failures` point at a misbehaving tserver; `bytes / rpcs` against `batch_bytes`
shows how much sharing saves.

## Tserver side: `LocalCatalogServiceImpl` (`tserver/local_catalog_service.{h,cc}`)

Registered only when `enable_local_tserver_catalog` is set. By default it answers NO_COPY. With
`TEST_local_catalog_record_pushes` it is a recording receiver: it accepts a batch only if
`from_op_id` equals its position, moves to `to_op_id`, keeps the highest safe time it was sent,
answers OK to duplicates it already has and GAP to batches ahead of it, and records each accepted
batch for tests. Other test flags make it fail, answer a gap, move backward, or delay its answer to every push or
only to reseeds.

## Major code changes

| Area | Change |
|---|---|
| `master/sys_catalog_pusher.{h,cc}` | New. Cycle certification, target selection, send loop, batch building, metrics. |
| `tserver/local_catalog.proto`, `local_catalog_service.{h,cc}` | New RPC service and the stub receiver. |
| `master/catalog_manager.{h,cc}` | Creates, starts and shuts down the pusher; owns its metrics; shuts it down before shell mode. |
| `cdc/xcluster_producer.cc`, `cdc/cdc_producer.h` | Record conversion extracted from `GetChangesForXCluster` into `PopulateXClusterRecords` with an `XClusterRecordsContext`, so the pusher can convert sys catalog entries. No behavior change for xCluster streams. |
| `master/object_lock_info_manager.{h,cc}` | `GetLiveLeases()`: live lease holders with their lease epochs from one read. |
| `master/ts_descriptor.h` | `LocalCatalogServiceProxy` added to the per-tserver proxy cache. |
| `tserver/tablet_server.{h,cc}` | Registers the service behind the flag, with its own queue length flag. |
| `common/common_flags.{h,cc}` | `enable_local_tserver_catalog`. |
| `yql/pgwrapper/pg_local_catalog-test.cc` | 39 tests, listed under [Tests](#tests). |

## Tests

All in `yql/pgwrapper/pg_local_catalog-test.cc`, grouped by fixture.

| Test | Checks |
|---|---|
| **`PgLocalCatalogTest`** | |
| `TestReceiverAppliesBatchesInOrder` | The test receiver records only a batch at its position, acks one it is past, answers with position and safe time. |
| **`PgLocalCatalogPushTest`** (one copy) | |
| `LongCatchUpIsSplitIntoBatches` | A far-behind copy catches up in one cycle through a chain of batches. |
| `CycleAfterDdlPushesDdl` | A DDL's rows and apply record are pushed, with a safe time at or after its commit. |
| `RequestedCycleMovesSafeTime` | Every requested cycle certifies a new safe time. |
| `RequestCycleRunsOnPusherThread` | A requested cycle runs at once, and is not skipped when nothing moved. |
| `FailedCycleIsRetried` | A failed cycle is retried without waiting for the interval. |
| `GapAnswerIsStall` | A GAP answer stalls the copy and ends its cycle. |
| `BatchMemoryIsChargedWhileInFlight` | A batch is charged to the memory tracker while its push is in flight. |
| `DivergedCopyIsReseeded` | A copy whose position's term differs from the log is reseeded. |
| `CycleWaitsForCommittedDdlApply` | An open DDL's rows are pushed; after commit, the safe time waits for its apply record. |
| `CycleFailsWithoutDdlApply` | A cycle fails if a committed DDL's apply record never shows up. |
| `CopyFallingBackIsLeftForNextCycle` | A copy that keeps answering without progress is left after its fourth such answer. |
| `LogGoneAtFirstReadIsReseeded` | A copy is reseeded when the log is gone at the first read. |
| `LogGoneMidBatchSendsWhatWasRead` | Log gone mid batch: what was read is sent without the safe time, and the next batch continues. |
| `EmptyReadMidBatchSendsWhatWasRead` | An empty read mid batch: what was read is sent, and the next batch continues. |
| `ChangeMetadataEndsBatch` | A tablet metadata change ends its batch. |
| **`PgLocalCatalogPushLogGcTest`** | |
| `CopyBehindLogGcIsReseeded` | A copy behind log GC is reseeded. |
| **`PgLocalCatalogPushTwoCopiesTest`** | |
| `OlderCopyGetsRecordsFromItsPosition` | Copies at different positions are each sent from their own position. |
| `ManyBatchesWithOneRpcInFlight` | Copies at one position share every batch of a long catch-up, one RPC in flight. |
| `FailedBatchBuildEndsCycle` | A failed batch build fails the cycle without sending or rebuilding it. |
| `FailedPushLeavesOtherCopyCatchingUp` | A failed push to one copy does not stop the other from catching up. |
| `TserverThatLeavesIsDropped` | A tserver that shuts down loses its lease and is no longer a target. |
| `RestartedTserverIsReseeded` | A restarted tserver with an empty copy is reseeded. |
| `JoiningTserverIsCaughtUp` | A joining tserver becomes a target and is caught up. |
| `TserverWithoutLocalCatalogHasNoCopy` | A tserver without the service counts as a target without a copy. |
| **`PgLocalCatalogPushLegacyTest`** (object locking off) | |
| `DdlReachesEveryCopy` | Every lease holder is a target, and a DDL reaches every copy. |
| `GateOffPushesNothing` | Nothing is pushed while the YSQL lease or invalidation messages are off. |
| **`PgLocalCatalogPushFailoverTest`** (three masters) | |
| `NewLeaderPushesFromCopyPositions` | A new leader continues from the copies' positions, without a reseed. |
| `NewLeaderPushesWithoutWaiting` | A new leader pushes once it has loaded the catalog. |
| `LeaseLapseKeepsGauges` | A leader without its lease pushes nothing and keeps its gauges. |
| `LeaderChangeBeforePushEndsCycle` | A leader change after resolving intents ends the cycle uncounted. |
| `OldLeaderDoesNotReseed` | A deposed leader sends no reseed and counts no failure. |
| `ShutdownWithPushInFlight` | Shutdown does not wait for an in-flight answer; the next leader catches up. |
| **`PgLocalCatalogPushFailoverTwoCopiesTest`** | |
| `OldLeaderDoesNotSendSharedReseed` | A reseed built for one copy is not sent to another once the leader is deposed. |
| **`PgLocalCatalogPushTimerTest`** | |
| `MasterWriteArrivesOnTimer` | A master-only write reaches the copy on the timer. |
| `TimerCatchesUpCopyLeftBehind` | The timer catches up a copy left behind although the WAL is idle. |
| `TimerSkipsTargetWithoutCopy` | A target without a copy is pushed to only once the WAL moves. |
| `LagGaugesTrackCopyLeftBehind` | The lag gauges grow while a copy lags and drop to 0 once caught up. |
| **`PgLocalCatalogMasterOffTest`** | |
| `MasterWithoutLocalCatalogPushesNothing` | A master without the local catalog has no pusher or metrics and pushes nothing. |

## Known limitations (TODOs in the code)

- A cycle ends only when every target is caught up or has left it, so a slow or far-behind target
  delays the next safe time for every copy, and one failed batch build stops the pushes to the
  targets still queued. Per-target scheduling, with one push in flight per target and backoff for
  only the target that fails, is to replace the cycle loop.
- A batch is built on the pusher thread and can wait up to the RPC timeout for the committed
  index, without handling answers meanwhile.
- Each batch rereads the log from its start and converts one message at a time; the serialized
  request is re-created for every target it is sent to and that copy is not charged to the memory
  tracker. In-flight memory is bounded by the number of RPCs, not by bytes, so a catch-up of
  large batches can hold `local_catalog_push_max_inflight_rpcs` times
  `local_catalog_push_batch_max_bytes` at once.
- The leader can still change between the reseed term check and the push; the request should carry
  the leader term so a copy refuses a reseed from an older term.
- Right after a leader change the lease set may not be rebuilt yet, so a lease holder can be left
  out until the next cycle.
- A copy that appears under an unchanged lease is not pushed to until the next catalog write; the
  tserver should report that it now keeps a copy.
