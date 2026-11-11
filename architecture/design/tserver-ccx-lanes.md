# Tablet server CPU lanes

On hosts with many L3 caches (for example AMD EPYC with one L3 per 8-core CCX), a remote write
crosses many thread handoffs inside one yb-tserver, and a handoff between threads on different L3s
costs several times the CPU of one between threads sharing an L3. Lanes partition the tserver's
threads and tablets by L3 domain so that a tablet's request path stays within one domain. Lanes
are off by default (`--tserver_lanes=1`).

What is implemented: topology discovery, the lane model and its flags. The topology is computed,
logged and exposed through `GetLaneTopology()`; no thread, pool or RPC path reads it yet, so the
rules below for those are the contract they must follow.

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

`DiscoverCpuDomains` (`util/cpu_topology.h`) groups the process cpuset (`sched_getaffinity`, so a
cgroup cpuset is respected) into domains:

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
process share one `L`, and masters in the same process see it too: their threads are treated as
service set and their messengers are never laned. Tests that need tservers with different `L` use
ExternalMiniCluster. Unit tests install their own topology with the scoped
`TEST_SetLaneTopology`.
