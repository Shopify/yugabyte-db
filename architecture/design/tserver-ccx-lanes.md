# Tablet server CPU lanes

On hosts with many L3 caches (for example AMD EPYC with one L3 per 8-core CCX), a remote write
crosses many thread handoffs inside one yb-tserver, and a handoff between threads on different L3s
costs several times the CPU of one between threads sharing an L3. Lanes partition the tserver's
threads and tablets by L3 domain so that a tablet's request path stays within one domain. Lanes
are off by default (`--tserver_lanes=1`).

What is implemented: topology discovery, the lane model and its flags, the confinement and
accounting mechanisms that consume the topology (new threads, thread pools, priority-pool tasks,
reactors, the postmaster spawn, the crossing counters), the laned tablet server messenger, and
laned tablet pipelines. With lanes on, the tserver messenger runs a group of reactors and handler
pools per lane, and an inbound call is handled in the lane of the reactor that read it. Each
tablet's Raft, log, preparer, leader-side apply and service-strand work and its RocksDB
background jobs run in its lane, and consensus peers send their RPCs with the tablet's lane hint.
Inbound connections are not yet bound to lanes: an inbound write crosses into the tablet's lane
once, at the preparer, and a follower prepares and applies follower-side operations inline in its
`UpdateConsensus` handler, in the lane of the reactor that read the call. PG sessions and the
tserver's embedded client are not laned and send no hints.

## Definitions

- **Domain:** the CPUs sharing one L3 (or one socket, see below). SMT siblings are always in the
  same domain.
- **Lane:** a set of whole domains plus the threads confined to them and the tablets whose
  pipelines run there. `L` is the number of lanes; `LaneId` is one byte, `L <= 254`, and `0xFF`
  (`kNoLane`) means "no lane".
- **Service set:** threads that belong to no lane. They run on `--tserver_service_lane_cpus` if
  set, otherwise anywhere in the process cpuset.
- **Crossing:** a handoff between threads in different lanes.

Lane masks are always whole domains, never single CPUs: pinning threads 1:1 to CPUs costs far more
than it saves.

## Lanes off is today's behaviour

Lane mode is active only when the effective `L > 1` (`LaneTopology::active()`). With
`--tserver_lanes=1` nothing is discovered or logged and no topology is installed. When discovery
yields one domain, or service CPUs leave none, the result is the same inactive topology. Every
lane-related code path must be a no-op for an inactive topology.

Correctness never depends on lanes: a tablet's pipeline runs on its lane's pools on every host,
and everything else (connection choice, reactor binding, session placement) is a locality hint.
A lane value received from elsewhere is reduced modulo the local `L`, never used as an index.

## Topology discovery

`DiscoverCpuDomains` (`util/cpu_topology.h`) groups the process cpuset into domains. The cpuset is
the starting thread's `sched_getaffinity`, so a cgroup cpuset is respected; once an active
topology is installed, a repeated `InitTserverLaneTopology` takes it from that topology instead,
because the calling thread may be confined by then:

- `--tserver_lane_domain=l3`: CPUs sharing the highest cache level present under
  `/sys/devices/system/cpu/cpu*/cache/index*` (`level`, `shared_cpu_list`). If that level is
  private to each core (every group equals the CPU's `thread_siblings_list`, as on some ARM
  servers without a shared L3), it would yield single-core domains, so it is treated as no cache
  info.
- `socket`, and `l3` without usable cache info: CPUs with the same
  `topology/physical_package_id`.
- No package info: one domain covering the cpuset.

Domains are intersected with the cpuset, empty ones dropped, and ordered by lowest CPU. Where a
socket has one L3 (Intel), both modes agree. A sysfs file that exists but does not parse, or
groups that overlap, are discovery errors; the tserver then logs a WARNING and runs with lanes
off rather than failing to start. Non-Linux builds see one domain.

Tests use `--TEST_cpu_topology_sysfs_root` (a fake sysfs tree) and `--TEST_cpu_topology_cpuset`
(a fake cpuset) to describe hosts larger than the test machine without changing any real
affinity.

## Flags

All are startup-only and tagged `advanced`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `tserver_lanes` | `1` | `1`: lanes off. `0`: one lane per domain, capped at 254. `N` (2-254): exactly `N` lanes. |
| `tserver_lane_domain` | `l3` | `l3` or `socket`, as above. |
| `tserver_lane_cpu_affinity` | `true` | `false` keeps the lane structure without pinning threads. |
| `tserver_service_lane_cpus` | empty | Cpu-list reserved for the service set and removed from every domain. |
| `tserver_lane_reactors` | `0` | Reactors per lane of the tserver messenger, replacing `--num_reactor_threads`. `0`: see [Messenger lanes](#messenger-lanes). |
| `tserver_lane_connections_per_remote` | `2` | Connections per (remote, lane) for calls with a lane hint (`k` below). |
| `tserver_lane_background` | `lane` | `lane` runs a tablet's RocksDB flushes and compactions in its lane, `service` on the service mask. See [Tablet pipelines](#tablet-pipelines). |
| `log_appender_lane_mode_max_wait_ms` | `1` | In lane mode, how long a tablet's log appender keeps its append thread after its queue empties; `0` releases it at once. Replaces `taskstream_queue_max_wait_ms` for the appender. |

Mapping `N` lanes onto `D` domains keeps masks whole and contiguous blocks within a socket:

- `N <= D` (including auto mode with more than 254 domains): lane `i` owns domains
  `[floor(i*D/N), floor((i+1)*D/N))`.
- `N > D`: lane `i` uses domain `floor(i*D/N)`, so several lanes share one domain's mask. This also
  lets the lane structure run on small hosts.

Service CPUs outside the process cpuset are dropped with a WARNING. The rest are removed from
every domain before mapping; domains left empty are dropped, and if none remain the tserver runs
with lanes off and a WARNING. The service mask is the service CPUs if any, else the full process
cpuset.

The resulting topology is logged once at INFO at startup ("Lane topology: ...") with its source,
domains, lane-to-domain map, service mask and whether affinity is on.

## Tablet to lane

`LaneOf(tablet_id)` is `HashUtil::MurmurHash2_64(tablet_id, seed 0) % L` (0 when inactive), and
every tablet-to-lane lookup goes through it. Hosts with the same `L` compute the same lane for a
tablet without coordination, so a leader and its followers replicate within one lane on both
ends. This is a cross-host, cross-version contract: the hash and seed are golden-tested in
`util/lanes-test.cc` and must not change (and `std::hash` must never be used). Hosts with
different `L` disagree, which costs locality only.

A tablet server stores the lane on the peer (`TabletPeer::lane()`) when it creates the peer in
`TSTabletManager::CreateAndRegisterTabletPeer`, the one path for tablets opened at startup, new
tablets, split children and remote-bootstrapped replicas, so a split child gets the lane of its own
id, not its parent's. `TserverTabletLane` gives `LaneOf(tablet_id)` in lane mode and `kNoLane`
otherwise (not `LaneOf`'s 0), so with lanes off every peer selects the unlaned pools and sends no
hints. Masters never set a peer lane.

## Process-global topology

`InitTserverLaneTopology()` runs at the start of `TabletServer::Init`, before
`DbServerBase::Init` builds the messenger and before `TSTabletManager::Init` builds the tablet
pools, and installs the topology with `SetLaneTopology`. It cannot run in the `TabletServer`
constructor because `TSTabletManager` is constructed in its initializer list.

The topology is process-wide and immutable once an active one is set. MiniCluster and
`PgMiniTestBase` run several tservers in one process and restart them, so setting an equal
topology again is a no-op; setting a different one is a DFATAL. Consequently all tservers in a
process share one `L`, and masters in the same process see it too: their threads start on the
service mask like every other `yb::Thread`, and their messengers stay unlaned. Only
`TabletServer::SetupMessengerBuilder` lanes a messenger (`SetupTserverLaneMessenger`); the CQL
server, masters, and the xCluster and other clients that build their own messengers never do.
Tests that need tservers with different `L` use ExternalMiniCluster. Unit tests install their own
topology with the scoped `TEST_SetLaneTopology`.

## Thread confinement

Lane state lives on the thread: `ThisThreadLane()` is the lane a thread runs in, `kNoLane` for
the service set. `ApplyLaneToCurrentThread(lane)` sets it and confines the thread to the lane's
whole-domain mask (the service mask for `kNoLane`); repeating the current lane makes no system
call. Lane values from configuration go through `NormalizeLane` (modulo `L`, `kNoLane` when
inactive) and are never used as raw indexes. With `--tserver_lane_cpu_affinity=false` lanes are
assigned and crossings counted, but no mask changes. A failed `sched_setaffinity` logs a
rate-limited WARNING and the thread runs unpinned. Every affinity change goes through
`SetCurrentThreadCpuAffinity`, whose call counter tests use to show that lanes off makes none.

No thread keeps its creator's mask:

- A new `yb::Thread` applies the service mask before running its body (`Thread::SuperviseThread`),
  since it inherits the mask of whatever thread created it, which may be a lane's.
- `YBThreadPool` creates workers lazily on whichever thread submits, so lanes are a pool option
  (`ThreadPoolOptions::lane`, `ThreadPoolBuilder::set_lane`) that each worker applies itself in
  `Worker::Execute`. The check runs per task and costs no system call unless the lane or the
  topology changed, so workers started before the topology was installed are confined from their
  next task.
- `PriorityThreadPool` workers serve every lane: a task's lane (`SetTaskLane`) is applied before it
  runs, switching the mask only when it differs from the worker's current lane; unlaned tasks go
  back to the service mask. RocksDB `Env` pool threads apply their DB's lane per job the same way.
- A reactor applies its lane at the start of `Reactor::RunThread`. A laned messenger assigns its
  reactors' lanes itself (see below); tests can also give an unlaned messenger reactor lanes with
  `MessengerBuilder::set_reactor_lanes`, one per reactor (empty leaves all unlaned; a size other
  than the reactor count fails `Messenger::Init`).

Service CPUs confine threads started after the topology is installed. A `yb::Thread` created
earlier (in the `TabletServer` constructor or by a process-wide singleton, such as the maintenance
manager) that is not a pool worker keeps the full mask, as do threads not created through
`yb::Thread` and the process's main thread.

## Messenger lanes

`MessengerBuilder::set_lane_topology` with an active topology makes a messenger laned
(`Messenger::laned()`, `num_lanes()`). The topology must be the installed process topology,
because reactor and pool threads take their masks and `ThisThreadLane()` from it; a different one
fails `Messenger::Init` with `InvalidArgument`. The messenger copies it and sizes every per-lane
array by its `L`. Lane values from a controller hint or a thread are reduced modulo `L` before
indexing, and `kNoLane` always selects the unlaned state. Rpc reads no tserver flags: the tserver
passes `tserver_lane_reactors` and `tserver_lane_connections_per_remote` through the builder.

**Reactors.** A laned messenger builds `r` reactors per lane, lane by lane
(`Messenger::ReactorsOfLane`), and ignores the reactor count from `--num_reactor_threads`, logging
one INFO line ("Lane mode: L lanes, N reactors (per lane: ...); the configured reactor count ...
is ignored"). With `tserver_lane_reactors=0`, `r = max(1, CPUs in the lane / 4 / lanes sharing
the lane's domains)`, so forcing more lanes than domains splits a domain's reactors rather than
multiplying them. Lanes whose domains lost service CPUs can get fewer reactors, so groups differ in
size. This default is provisional; the final value is to be set by measurement on multi-L3
hardware.

**Thread names.** Linux keeps the first 15 characters of a thread name, so lane threads start
with the lane as `L` and three digits: reactors `L007_reactor_<i>` (i within the lane), normal
handler workers `L007_rpc_<n>_worker`, high-priority workers `L007_high_<n>_worker`, and tagged
workers `L007_t<tag>_<n>_worker`. Thread categories keep the messenger name
(`TabletServer_reactor`, `TabletServer_L007`, `TabletServer_L007-high-pri`,
`TabletServer_L007_pool_<tag>`). Unlaned messengers and the unlaned pools keep their names.

**Handler pools.** Each lane has a normal pool, a high-priority pool created on first use, and a
`TaggedThreadPools` instance of its own for QoS tags (tag 0 is the lane's normal pool, held by the
messenger and returned without a lookup). Their workers run in the lane and count handoffs as
`rpc_handler`. Limits and cgroups are those of the unlaned pools: `rpc_workers_limit` (the
builder's `workers_limit`) for normal and tagged pools, `rpc_high_priority_workers_limit` for
high-priority ones, the system-medium, system-high and per-tag cgroups. The unlaned pools stay
and serve `kNoLane`: `Messenger::ThreadPool(priority, lane)` and `TaggedThreadPool(tag, lane)`
give a lane's pool and today's pool for `kNoLane`; the signatures without a lane are `kNoLane`.
Looking up a tagged pool takes only that lane's lock, and the process-wide instance is unchanged.

`ServicePool` asks its provider for the pool of `(call->pool_tag(), call->lane())`, and
`RpcServer` maps that through `Messenger::HandlerThreadPool` to the lane's tagged pool for normal
services and the lane's high-priority pool for high-priority ones. `InboundCall::lane()` is the
receiving reactor's lane for calls read from a connection (no `RequestHeader` field), so an
inbound call is handled in the lane that read it; inbound sockets are still assigned to reactors
by a hash over all reactors.

QoS thread count: at most `L x (1 normal + 1 high + T tagged)` handler pools for `T` active tags,
next to the unlaned set, each up to the unlaned limit. Idle workers exit after
`default_idle_timeout_ms`, and an idle tagged pool is reclaimed the next time its lane creates a
tagged pool, so idle lanes cost no threads. Thread counts with QoS on and the real database count
are to be measured on a cgroup-v2 host.

**Outbound connections.** A call whose controller has a lane hint (`RpcController::set_lane`)
through a laned messenger goes on a connection keyed by `(remote, lane, idx)` (`ConnectionId`
has a separate lane field) and owned by a reactor of that lane, picked by a hash of the remote and
`idx` over the lane's group. `idx` rotates over `k = tserver_lane_connections_per_remote`, so a
tserver opens up to `L x remotes x k` lane-keyed connections next to the unlaned ones. The
rotation counter is per `Proxy` and per submitting thread's lane, one cache line each plus a
spare for unlaned threads, allocated only in a laned messenger, so threads of many lanes calling
into one lane through one cached `Proxy` share no counter. Calls without a hint, and every call
through an unlaned messenger, keep today's ids (`kNoLane`, `idx < num_connections_to_server`) and
reactors. A reactor erases a closed client connection by the key it was registered under.

**Callbacks.** In thread-pool callback modes a call's callback runs in the normal or high-priority
pool of the controller's issuing lane, and in today's pools when that is `kNoLane` or the
messenger is unlaned. The controller records the issuing lane when a call is sent: a send from a
reactor thread keeps the lane an earlier send recorded, so a retry rescheduled by `RpcRetrier` or
`Messenger::ScheduleOnReactor` on any reactor calls back in the original lane; a first send, or a
send from any other thread, records the sending thread's lane, so a controller reused by another
lane follows it. `Reset()` keeps the issuing lane and clears the hint, which callers set per send.
`ScheduleOnReactor` from a lane thread picks a reactor of that lane.

**Local calls.** A local call is handled by the pool of its lane: the controller's hint, or the
issuing lane without one, so un-hinted local calls stay in the caller's lane as they stay on the
caller's thread today. It runs inline iff `allow_local_calls_in_curr_thread` is set, the calling
thread is in the call's lane (always true in an unlaned messenger), and the calling thread is a
worker of the pool the provider returns (`ServicePool::Process`'s `OwnsThisThread` check), which
a thread of another lane never is; otherwise it is queued to that pool. Shared-memory PG sessions
run on threads no handler pool owns, so their local calls always queue.

## Tablet pipelines

**Pools.** The `TSTabletManager` constructor builds one unlaned set of pipeline pools (`prepare`,
`append`, `consensus`, `raft_notifications`, `log-sync`, `log-alloc`) as without lanes; it runs in
the `TabletServer` constructor, before the topology is installed. In lane mode
`TSTabletManager::Init` builds one more set per lane, before the cgroup setup, with the same limits
(min 1, unlimited, since their tasks block on IO), the lane, and the `tablet_pipeline` crossing
category. Lane pools are named `<pool>_L007` (the `/threadz` category) and their workers
`L007_prepare_<n>_worker`, `L007_append_...`, `L007_raft_...`, `L007_notif_...`,
`L007_logsync_...` and `L007_logalloc_...`. `TSTabletManager::pipeline_pools(lane)` selects a set
(`lane % L`; the unlaned set for `kNoLane` or with lanes off), and `ForEachPipelinePools` visits the
unlaned set and every lane's, so the `@system-high` cgroup assignment (consensus system mode) and
shutdown cover all of them. In per-DB cgroup mode per-task cgroups are set on each tablet's tokens,
which come from its lane's pools.

`OpenTablet` gives a tablet its lane's append, allocation and log-sync pools for the log, its Raft,
notification and prepare pools for consensus and the preparer, its Raft pool for intent cleanup,
and `Messenger::TaggedThreadPool(PoolTagForTablet(tablet), lane)` for the service strand. On the
leader, op apply runs on the Raft notification strand, so it follows the tablet without a pool of
its own. A follower prepares and applies follower-side operations inline in its `UpdateConsensus`
handler, so that work runs in the handler's lane, which is lane(T) only once inbound connections
are bound to lanes. The other tablet-manager pools (`apply` for split and clone,
`snapshot-cleanup`, `tablet-bootstrap`, `flush-retryable-requests`, `read-parallel`,
`admin-compaction`, `full-compaction`, `wait-queue`, vector index pools) stay in the service set.
With lanes on the unlaned pipeline set serves only work that belongs to no tablet, such as
`StepDownLocalLeaders` on the Raft pool; it runs on the service mask, and its pools hold only their
one idle persistent worker each.

**Log appender park.** A tablet's log appender drains its `TaskStream` on an append worker and,
once the queue is empty, keeps that worker waiting for more entries for up to
`taskstream_queue_max_wait_ms` (1000 ms). Every tablet written to within that time thus holds an
append thread, which a lane hosting many tablets cannot afford, so in lane mode the appender waits
`log_appender_lane_mode_max_wait_ms` (1 ms) instead; 0 releases the worker as soon as the queue is
empty. Released workers stay idle in the pool until `default_idle_timeout_ms`. The value is to be
set by measurement at production tablet density. Masters in a process with an active topology
(tests) also use it.

**Background jobs.** Flushes and compactions keep their shared pools so that priority ordering
across tablets is kept: compactions, and flushes with `use_priority_thread_pool_for_flushes`, run on
the global `PriorityThreadPool`, other flushes on the RocksDB `Env` high-priority pool. A tablet
sets its lane on its regular and intents `DBImpl` (`SetTaskLane`) right after each open, including
re-opens, from `TabletInitData::rocksdb_task_lane`: priority pool tasks carry it and `Env` pool
threads apply it per job, so a job runs in its tablet's lane and an unlaned one on the service mask,
with an affinity call only when consecutive jobs differ. `TSTabletManager` sets the field to the
tablet's lane, or to `kNoLane` with `--tserver_lane_background=service`; `tablet` reads no tserver
flag, and masters leave it `kNoLane`. Jobs scheduled while a DB is opening run on the service mask.
Whether compaction volume overloads lanes, and so which mode suits a workload, is to be measured;
`lane` is the default.

**Consensus hints.** A replica's `RpcPeerProxyFactory` takes the peer's lane
(`ConsensusOptions::lane`) when the server's messenger is laned, and `RpcPeerProxy` sets it as the
controller's hint on every `UpdateConsensus`, `RequestConsensusVote`, `RunLeaderElection`,
`LeaderElectionLost` and `StartRemoteBootstrap` call, since `RpcController::Reset()` clears it. Raft
calls thus go on `(follower, lane(T))` connections owned by lane(T)'s reactors on the sender. A
callback runs in the pool of the lane that sent the call. A leader sends `UpdateConsensus` and
remote bootstrap requests only from lane(T)'s threads (its Raft pool token, and the callback that
sends the next request), so those callbacks run in lane(T). Vote and election calls are sent from
the thread that starts the election (the Raft pool token after a failure detection, or the handler
of a requested election or step-down) and call back in that thread's lane. Masters
replicate their sys catalog with the same code but never send a hint: their peers have no lane and
their messengers are unlaned. Multi-tablet heartbeats (`enable_multi_raft_heartbeat_batcher`,
default off) span tablets of many lanes, so they carry no hint and use unlaned connections; this
costs locality only.

## Service set and the postmaster

The service mask is `--tserver_service_lane_cpus` if set, otherwise the full process cpuset. The
postmaster and its backends must float over every CPU even when service CPUs are set, but Linux
`Subprocess::Start` uses `posix_spawn`, which has no affinity attribute or pre-exec hook, and the
child inherits the spawning thread's mask. `PgWrapper::Start` therefore spawns inside
`ScopedThreadAffinity(full_process_cpuset())`, which restores the thread's mask afterwards. The
first start and supervisor restarts (`ProcessSupervisor::StartProcessUnlocked`) both go through
it. initdb and odyssey keep the spawning thread's mask, the service set.
`ScopedThreadAffinity` is a no-op with lanes off, with affinity disabled, or with an empty mask.

## Relation to QoS cgroups

Lanes and QoS are independent. QoS moves threads between cgroup-v2 cpu-controller groups without
a cpuset (`util/cgroups.cc`); lanes set per-thread masks with `sched_setaffinity`. QoS tagged
handler pools are per (lane, tag), see [Messenger lanes](#messenger-lanes). A pool's
`cgroup` and `lane` options are applied side by side in `Worker::Execute`, and per-task cgroups
are unchanged. Lane pipeline pools get the cgroups of the unlaned pipeline pools, see
[Tablet pipelines](#tablet-pipelines). A cpuset imposed on the process from outside is honoured
through discovery.

## Crossing accounting

A handoff records the submitting thread's lane in one byte when it is queued and compares it with
the executing thread's lane when it runs. If both are laned and differ it counts a crossing for
the executor's category; if only the executor is laned it counts an entry from unlaned, except
for handoffs into reactors (see below). Categories are reactor, rpc handler, tablet pipeline,
callback and other, set per pool with `ThreadPoolOptions::crossing_category` (default other); an
item can override its pool's category (`LaneHandoff::SetCategory`), as RPC callbacks do, since
they share the handler pools. Counted handoffs:

| Primitive | Counted when | Category |
| --- | --- | --- |
| `YBThreadPool` task (also `ThreadSubPool`, CONCURRENT tokens) | the worker starts it | the pool's |
| `Strand` item (SERIAL tokens, the raft-notifications strand) | the drain loop runs it | the strand's pool's |
| `TaskStream` item, including items taken by a running stream | it is processed | the stream's pool's |
| Leader-side operation queued on a tablet's preparer (`PreparerImpl::Submit`) | the drain loop takes it | the prepare pool's |
| Reactor task (`Reactor::ScheduleReactorTask`) | it is scheduled | reactor |
| Outbound call (`Reactor::QueueOutboundCall`), response or other outbound data from another thread (`Connection::QueueOutboundData`), Redis reply (`ConnectionContextWithQueue::QueueResponse`) | it is queued | reactor |
| Inbound or local call queued to a lane's handler pool (`ServicePool::Process`) | a handler starts it | rpc handler |
| RPC callback queued to the issuing lane's pool (`OutboundCall::InvokeCallback`) | the callback starts | callback |

Many primitives queue items privately and wake their executor once per batch. Their items are
counted, and the wrapper task that only drains them is not, so each handoff counts once whether or
not the executor was already running: `Strand`'s pool task and the task that starts a `TaskStream`
draining are submitted uncounted (`ThreadPoolToken::SubmitDrainFunc`), and reactor queues count
each item with `Reactor::CountLaneHandoff` and schedule the drain with `ScheduleDrainTask`. The
end-of-group call with no item is not counted. A reactor's lane is fixed, so its handoffs are
counted on the submitting side, and only by laned submitters: an unlaned thread's only counter
slot is the spare one all unlaned threads share, and every call it hands to a laned reactor (all
client calls from PG sessions, which are not laned yet) would write it.

Within a tablet's pipeline work stays in the tablet's lane, and Raft callbacks run in the lane
that sent the call, which is also the lane of the reactor that reads the response, so neither
crosses. Until inbound connections are bound to lanes, an inbound write is handled in the lane of
the reactor that read it and crosses into the tablet's pipeline once, at the preparer. A
follower's `UpdateConsensus` handler prepares and applies follower-side operations inline, in the
lane of the reactor that read the call, and crosses into the tablet's pipeline for the log append
and the other tasks it submits to the tablet's pools.

Not counted: work handed to a reactor by an unlaned thread (see above), `PriorityThreadPool` tasks
and `Env` pool jobs (flush and compaction), `rpc::Scheduler` and other io-service
timers, `CompletedCallQueue`, futures, promises and `Synchronizer` wakeups, raw
`std::condition_variable` or `BlockingQueue` handoffs outside `TaskStream`, and lock wait queues.
Wakeups that coalesce signals rather than carry items count one handoff per task, not per signal:
Raft peer request signals (`Peer::SignalRequest`), the log's background sync task, and a PG
session's deferred processing of pending requests. Crossings through these do not show, or show once
per batch, in the metrics.

Counters are sharded per lane, one cache line per lane plus one for unlaned threads, each written
only by threads of its lane (the executing thread, or the submitter for reactors), and summed on
read (`LaneShardedCounters` in `util/lanes.h`). With lanes on the tserver exposes them as counters
`lane_crossings_reactor`, `lane_crossings_rpc_handler`, `lane_crossings_tablet_pipeline`,
`lane_crossings_callback`, `lane_crossings_other` and `lane_entries_from_unlaned`; with lanes off
they are not registered and nothing is counted. `lane_entries_from_unlaned` covers laned pools,
strands and task streams only, not work unlaned threads hand to reactors. The values are
process-wide, so tservers sharing a process report the same totals. A crossing counter's rate
divided by the operation rate is the number of crossings per operation on that path.
