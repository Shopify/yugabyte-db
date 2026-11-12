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

#include "yb/util/lanes_test_util.h"

#include <thread>

#include "yb/util/format.h"
#include "yb/util/logging.h"
#include "yb/util/result.h"
#include "yb/util/status_log.h"

namespace yb {

std::optional<LaneTopology> MakeTwoLaneTestTopology(bool affinity_enabled, bool service_cpu) {
  const auto process_cpuset = ThisThreadMask();
  auto cpus = process_cpuset.ToVector();
  cpus.resize(std::min<size_t>(cpus.size(), 4));
  CpuSet service_cpus;
  if (service_cpu && !cpus.empty()) {
    service_cpus.Set(cpus.back());
    cpus.pop_back();
  }
  if (cpus.size() < 2) {
    return std::nullopt;
  }
  std::vector<CpuSet> domains(2);
  for (size_t i = 0; i != cpus.size(); ++i) {
    domains[i < cpus.size() / 2 ? 0 : 1].Set(cpus[i]);
  }
  auto topology = LaneTopology::Build(LaneTopologyOptions {
    .domains = domains,
    .process_cpuset = process_cpuset,
    .requested_lanes = 2,
    .service_cpus = service_cpus,
    .affinity_enabled = affinity_enabled,
  });
  CHECK(topology.active()) << topology.ToString();
  return topology;
}

CpuSet ThisThreadMask() {
  return CHECK_RESULT(GetCurrentThreadCpuAffinity());
}

void RunOnStdThread(const std::function<void()>& f) {
  std::thread thread(f);
  thread.join();
}

LaneHandoffCounts LaneHandoffCounts::Now() {
  LaneHandoffCounts result;
  for (auto category : LaneCrossingCategoryList()) {
    result.crossings[static_cast<size_t>(category)] = LaneCrossingCount(category);
  }
  result.entries_from_unlaned = LaneEntriesFromUnlanedCount();
  return result;
}

uint64_t LaneHandoffCounts::total_crossings() const {
  uint64_t result = 0;
  for (auto value : crossings) {
    result += value;
  }
  return result;
}

LaneHandoffCounts LaneHandoffCounts::operator-(const LaneHandoffCounts& rhs) const {
  LaneHandoffCounts result;
  for (size_t i = 0; i != crossings.size(); ++i) {
    result.crossings[i] = crossings[i] - rhs.crossings[i];
  }
  result.entries_from_unlaned = entries_from_unlaned - rhs.entries_from_unlaned;
  return result;
}

std::string LaneHandoffCounts::ToString() const {
  std::string result = "{";
  for (auto category : LaneCrossingCategoryList()) {
    result += Format(" $0: $1", category, crossing(category));
  }
  return result + Format(" entries_from_unlaned: $0 }", entries_from_unlaned);
}

} // namespace yb
