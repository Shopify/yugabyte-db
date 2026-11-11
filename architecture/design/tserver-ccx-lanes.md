# Tablet server CPU lanes

On hosts with many L3 caches (for example AMD EPYC with one L3 per 8-core CCX), a remote write
crosses many thread handoffs inside one yb-tserver, and a handoff between threads on different L3s
costs several times the CPU of one between threads sharing an L3. Lanes partition the tserver's
threads and tablets by L3 domain so that a tablet's request path stays within one domain. Lanes
are off by default (`--tserver_lanes=1`).

What is implemented: topology discovery, the lane model and its flags, and the confinement and
accounting mechanisms that consume the topology: new threads, thread pools, priority-pool tasks,
reactors given a lane, the postmaster spawn, and the crossing counters. Nothing assigns lanes in
production yet: there are no per-lane pools or laned messengers, so with lanes on every tserver
thread runs in the service set. The tablet-to-lane rule below is a contract for code that routes
by lane.

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

## Process-global topology

`InitTserverLaneTopology()` runs at the start of `TabletServer::Init`, before
`DbServerBase::Init` builds the messenger and before `TSTabletManager::Init` builds the tablet
pools, and installs the topology with `SetLaneTopology`. It cannot run in the `TabletServer`
constructor because `TSTabletManager` is constructed in its initializer list.

The topology is process-wide and immutable once an active one is set. MiniCluster and
`PgMiniTestBase` run several tservers in one process and restart them, so setting an equal
topology again is a no-op; setting a different one is a DFATAL. Consequently all tservers in a
process share one `L`, and masters in the same process see it too: their threads start on the
service mask like every other `yb::Thread`, and their messengers are never given reactor lanes.
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
  back to the service mask.
- A reactor applies its lane at the start of `Reactor::RunThread`. Reactor lanes come from
  `MessengerBuilder::set_reactor_lanes`, one per reactor (empty leaves all unlaned; a size other
  than the reactor count fails `Messenger::Init`).

Service CPUs confine threads started after the topology is installed. A `yb::Thread` created
earlier (in the `TabletServer` constructor or by a process-wide singleton, such as the maintenance
manager) that is not a pool worker keeps the full mask, as do threads not created through
`yb::Thread` and the process's main thread.

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
a cpuset (`util/cgroups.cc`); lanes set per-thread masks with `sched_setaffinity`. A pool's
`cgroup` and `lane` options are applied side by side in `Worker::Execute`, and per-task cgroups
are unchanged. A cpuset imposed on the process from outside is honoured through discovery.

## Crossing accounting

A handoff records the submitting thread's lane in one byte when it is queued and compares it with
the executing thread's lane when it runs. If both are laned and differ it counts a crossing for
the executor's category; if only the executor is laned it counts an entry from unlaned. Categories
are reactor, rpc handler, tablet pipeline, callback and other, set per pool with
`ThreadPoolOptions::crossing_category` (default other). Counted handoffs:

| Primitive | Counted when | Category |
| --- | --- | --- |
| `YBThreadPool` task (also `ThreadSubPool`, CONCURRENT tokens) | the worker starts it | the pool's |
| `Strand` item (SERIAL tokens, the raft-notifications strand) | the drain loop runs it | the strand's pool's |
| `TaskStream` item, including items taken by a running stream | it is processed | the stream's pool's |
| `Reactor::ScheduleReactorTask`, `Reactor::QueueOutboundCall` | it is scheduled | reactor |

Wrapper tasks that only drain items (`Strand`'s pool task, the task that starts a `TaskStream`
draining) are not counted, so each handoff counts once; nor is the end-of-group call with no
item. A reactor's lane is fixed, so its handoffs are counted on the submitting side.

Not counted: `PriorityThreadPool` tasks (flush and compaction), `rpc::Scheduler` and other
io-service timers, `CompletedCallQueue`, futures, promises and `Synchronizer` wakeups, raw
`std::condition_variable` or `BlockingQueue` handoffs outside `TaskStream`, and lock wait
queues. Crossings through these do not show in the metrics.

Counters are sharded per lane, one cache line per lane plus one for unlaned threads, each written
only by threads of its lane (the executing thread, or the submitter for reactors), and summed on
read (`LaneShardedCounters` in `util/lanes.h`). With lanes on the tserver exposes them as
counters `lane_crossings_reactor`, `lane_crossings_rpc_handler`, `lane_crossings_tablet_pipeline`,
`lane_crossings_callback`, `lane_crossings_other` and `lane_entries_from_unlaned`; with lanes off
they are not registered and nothing is counted. The values are process-wide, so tservers sharing a
process report the same totals. A crossing counter's rate divided by the operation rate is the
number of crossings per operation on that path.
