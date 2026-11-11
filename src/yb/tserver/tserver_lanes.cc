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

#include "yb/util/cpu_topology.h"
#include "yb/util/flag_validators.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/logging.h"

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
  // Pin the cpuset so discovery, service CPUs and the topology all use the same snapshot.
  options.cpuset = VERIFY_RESULT(GetProcessCpuSet(options));
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

} // namespace yb::tserver
