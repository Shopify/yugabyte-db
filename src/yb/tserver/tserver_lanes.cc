// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include "yb/tserver/tserver_lanes.h"

#include "yb/gutil/bind.h"

#include "yb/rpc/connection_context.h"
#include "yb/rpc/messenger.h"

#include "yb/util/cpu_topology.h"
#include "yb/util/flag_validators.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/logging.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/metrics.h"

static_assert(yb::kMaxLanes == 254, "Keep the tserver_lanes validator in sync with kMaxLanes");

DEFINE_NON_RUNTIME_int32(tserver_lanes, 1,
    "Number of CPU lanes the tablet server partitions its threads and tablets into. 1 (default) "
    "disables lanes: the server behaves exactly as without this feature. 0 creates one lane per "
    "CPU domain (see tserver_lane_domain). N > 1 forces N lanes, mapped onto whole domains.");
DEFINE_validator(tserver_lanes, FLAG_RANGE_VALIDATOR(0, 254));
TAG_FLAG(tserver_lanes, advanced);

DEFINE_NON_RUNTIME_bool(tserver_lane_cpu_affinity, true,
    "When lanes are enabled (tserver_lanes != 1), pin each lane's threads to its CPU domain. "
    "false keeps the lane structure without pinning. No effect when lanes are disabled.");
TAG_FLAG(tserver_lane_cpu_affinity, advanced);

DEFINE_NON_RUNTIME_string(tserver_lane_domain, "l3",
    "What a CPU domain is when lanes are enabled: 'l3' groups CPUs sharing the highest cache "
    "level (falling back to 'socket' without shared cache info), 'socket' groups CPUs by "
    "physical package. No effect when lanes are disabled (tserver_lanes=1).");
DEFINE_validator(tserver_lane_domain, FLAG_IN_SET_VALIDATOR("l3", "socket"));
TAG_FLAG(tserver_lane_domain, advanced);

DEFINE_NON_RUNTIME_string(tserver_service_lane_cpus, "",
    "Cpu-list (e.g. '0-3,192-195') reserved for threads that belong to no lane when lanes are "
    "enabled. These CPUs are removed from every lane. Empty (default): no reserved CPUs and "
    "non-lane threads may run anywhere in the process cpuset. No effect when lanes are disabled.");
DEFINE_validator(tserver_service_lane_cpus,
    FLAG_OK_VALIDATOR(::yb::CpuSet::Parse(_value)));
TAG_FLAG(tserver_service_lane_cpus, advanced);

DEFINE_NON_RUNTIME_int32(tserver_lane_reactors, 0,
    "Reactor threads per CPU lane when lanes are enabled; these replace num_reactor_threads. 0 "
    "(default): max(1, CPUs in the lane / 4 / number of lanes sharing the lane's CPU domains). No "
    "effect when lanes are disabled (tserver_lanes=1).");
DEFINE_validator(tserver_lane_reactors, FLAG_RANGE_VALIDATOR(0, 256));
TAG_FLAG(tserver_lane_reactors, advanced);

DEFINE_NON_RUNTIME_int32(tserver_lane_connections_per_remote, 2,
    "Outbound connections per remote server and CPU lane for calls routed to a lane when lanes "
    "are enabled. Other calls keep using num_connections_to_server connections per remote. No "
    "effect when lanes are disabled (tserver_lanes=1).");
DEFINE_validator(tserver_lane_connections_per_remote, FLAG_RANGE_VALIDATOR(1, 256));
TAG_FLAG(tserver_lane_connections_per_remote, advanced);

DEFINE_NON_RUNTIME_string(tserver_lane_background, "lane",
    "Where a tablet's RocksDB flushes and compactions run when lanes are enabled: 'lane' (default) "
    "runs each task on the CPUs of the tablet's lane, 'service' on the CPUs of threads that belong "
    "to no lane. No effect when lanes are disabled (tserver_lanes=1).");
DEFINE_validator(tserver_lane_background, FLAG_IN_SET_VALIDATOR("lane", "service"));
TAG_FLAG(tserver_lane_background, advanced);

METRIC_DEFINE_gauge_uint64(server, lane_crossings_reactor,
    "Lane Crossings Into Reactors", yb::MetricUnit::kOperations,
    "Tasks and outbound calls handed to a reactor of one CPU lane by a thread of another lane.",
    yb::EXPOSE_AS_COUNTER);
METRIC_DEFINE_gauge_uint64(server, lane_crossings_rpc_handler,
    "Lane Crossings Into RPC Handlers", yb::MetricUnit::kOperations,
    "Tasks handed to an RPC handler thread of one CPU lane by a thread of another lane.",
    yb::EXPOSE_AS_COUNTER);
METRIC_DEFINE_gauge_uint64(server, lane_crossings_tablet_pipeline,
    "Lane Crossings Into Tablet Pipelines", yb::MetricUnit::kOperations,
    "Tasks handed to a tablet pipeline thread of one CPU lane by a thread of another lane.",
    yb::EXPOSE_AS_COUNTER);
METRIC_DEFINE_gauge_uint64(server, lane_crossings_callback,
    "Lane Crossings Into Callbacks", yb::MetricUnit::kOperations,
    "Callbacks handed to a callback thread of one CPU lane by a thread of another lane.",
    yb::EXPOSE_AS_COUNTER);
METRIC_DEFINE_gauge_uint64(server, lane_crossings_other,
    "Other Lane Crossings", yb::MetricUnit::kOperations,
    "Tasks handed to any other laned thread by a thread of another CPU lane.",
    yb::EXPOSE_AS_COUNTER);
METRIC_DEFINE_gauge_uint64(server, lane_entries_from_unlaned,
    "Lane Entries From Unlaned Threads", yb::MetricUnit::kOperations,
    "Tasks handed to a laned thread pool, strand or task stream by a thread that belongs to no "
    "lane. Work handed to reactors by threads that belong to no lane is not counted.",
    yb::EXPOSE_AS_COUNTER);

namespace yb::tserver {

Result<LaneTopology> ComputeTserverLaneTopology(std::string* description) {
  if (FLAGS_tserver_lanes == 1) {
    if (description) {
      *description = "lanes disabled (tserver_lanes=1)";
    }
    return LaneTopology();
  }

  CpuTopologyOptions options {
    .domain_kind = FLAGS_tserver_lane_domain == "socket" ? CpuDomainKind::kSocket
                                                         : CpuDomainKind::kL3,
  };
  // Pin the cpuset so discovery, service CPUs and the topology all use the same snapshot. Threads
  // are only confined while a topology is active, so before that the calling thread's mask is the
  // process cpuset, and after that the installed topology holds it: a tablet server restarted in
  // the same process then computes an equal topology even from a confined thread.
  const auto& installed = GetLaneTopology();
  options.cpuset = installed.active() ? installed.full_process_cpuset()
                                      : VERIFY_RESULT(GetProcessCpuSet(options));
  std::string source;
  auto domains = VERIFY_RESULT(DiscoverCpuDomains(options, &source));

  auto service_cpus = VERIFY_RESULT(CpuSet::Parse(FLAGS_tserver_service_lane_cpus));
  auto outside = service_cpus - *options.cpuset;
  if (!outside.empty()) {
    LOG(WARNING) << "Ignoring tserver_service_lane_cpus " << outside.ToString()
                 << ": not in the process cpuset " << options.cpuset->ToString();
    service_cpus -= outside;
  }

  auto topology = LaneTopology::Build(LaneTopologyOptions {
    .domains = domains,
    .process_cpuset = *options.cpuset,
    .requested_lanes = static_cast<size_t>(FLAGS_tserver_lanes),
    .service_cpus = service_cpus,
    .affinity_enabled = FLAGS_tserver_lane_cpu_affinity,
  });

  if (description) {
    std::string domain_list;
    for (const auto& domain : domains) {
      domain_list += Format("$0[$1]", domain_list.empty() ? "" : " ", domain.ToString());
    }
    *description = topology.active()
        ? Format("lanes active (tserver_lanes=$0), $1 CPU domain(s) from $2: $3",
                 FLAGS_tserver_lanes, domains.size(), source, topology)
        : Format("lanes inactive, one lane (tserver_lanes=$0), $1 CPU domain(s) from $2: $3",
                 FLAGS_tserver_lanes, domains.size(), source, domain_list);
  }
  return topology;
}

void InitTserverLaneTopology() {
  if (FLAGS_tserver_lanes == 1) {
    return;
  }
  std::string description;
  auto topology = ComputeTserverLaneTopology(&description);
  if (!topology.ok()) {
    LOG(WARNING) << "CPU topology discovery failed, running with lanes off: "
                 << topology.status();
    return;
  }
  LOG(INFO) << "Lane topology: " << description;
  SetLaneTopology(*topology);
}

void SetupTserverLaneMessenger(rpc::MessengerBuilder* builder) {
  const auto& topology = GetLaneTopology();
  if (!topology.active()) {
    return;
  }
  builder->set_lane_topology(&topology)
      .set_reactors_per_lane(FLAGS_tserver_lane_reactors)
      .set_connections_per_lane_remote(FLAGS_tserver_lane_connections_per_remote);
}

LaneId TserverTabletLane(Slice tablet_id) {
  return LaneModeActive() ? LaneOf(tablet_id) : kNoLane;
}

LaneId TserverBackgroundTaskLane(LaneId tablet_lane) {
  return FLAGS_tserver_lane_background == "service" ? kNoLane : tablet_lane;
}

void RegisterTserverLaneMetrics(const scoped_refptr<MetricEntity>& entity) {
  if (!GetLaneTopology().active()) {
    return;
  }
  for (auto [prototype, category] : {
           std::pair(&METRIC_lane_crossings_reactor, LaneCrossingCategory::kReactor),
           std::pair(&METRIC_lane_crossings_rpc_handler, LaneCrossingCategory::kRpcHandler),
           std::pair(&METRIC_lane_crossings_tablet_pipeline,
                     LaneCrossingCategory::kTabletPipeline),
           std::pair(&METRIC_lane_crossings_callback, LaneCrossingCategory::kCallback),
           std::pair(&METRIC_lane_crossings_other, LaneCrossingCategory::kOther)}) {
    entity->NeverRetire(prototype->InstantiateFunctionGauge(
        entity, Bind(&LaneCrossingCount, category)));
  }
  entity->NeverRetire(METRIC_lane_entries_from_unlaned.InstantiateFunctionGauge(
      entity, Bind(&LaneEntriesFromUnlanedCount)));
}

void EnableTserverLaneMemTrackerBatching(const std::shared_ptr<MemTracker>& server_tracker) {
  if (!LaneModeActive()) {
    return;
  }
  MemTracker::GetRootTracker()->EnableLaneBatching();
  // The server tracker's children are server-wide (RPC buffers and calls, tablets overhead, the
  // global log cache, block cache, PG response cache, big shared memory, ...); per-tablet trackers
  // are further down and stay unbatched, since only their tablet's lane updates them.
  server_tracker->EnableLaneBatching(MemTrackerLaneBatching::kChildren);
  rpc::EnableLaneBatchingForRpcMemTrackers(server_tracker);
}

} // namespace yb::tserver
