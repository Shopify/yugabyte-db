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

#include "yb/util/lanes.h"

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>

#include "yb/util/format.h"
#include "yb/util/hash_util.h"
#include "yb/util/logging.h"

namespace yb {

namespace {

std::mutex& TopologyMutex() {
  static std::mutex mutex;
  return mutex;
}

// nullptr means never set. Points into RetainedTopologies(), so readers never see a freed object.
std::atomic<const LaneTopology*> current_topology{nullptr};

const LaneTopology& InactiveTopology() {
  static const LaneTopology* topology = new LaneTopology();
  return *topology;
}

// Every topology ever installed, kept for the process lifetime because GetLaneTopology hands out
// references without synchronization. Requires TopologyMutex.
const LaneTopology* RetainTopology(const LaneTopology& topology) {
  static auto* retained = new std::vector<std::unique_ptr<const LaneTopology>>();
  retained->push_back(std::make_unique<const LaneTopology>(topology));
  return retained->back().get();
}

std::string RangeToString(size_t first, size_t last) {
  return first == last ? std::to_string(first) : Format("$0-$1", first, last);
}

} // namespace

LaneTopology LaneTopology::Build(const LaneTopologyOptions& options) {
  if (options.requested_lanes == 1) {
    return LaneTopology();
  }

  std::vector<CpuSet> domains;
  for (auto domain : options.domains) {
    domain -= options.service_cpus;
    if (!domain.empty()) {
      domains.push_back(domain);
    }
  }
  if (domains.empty()) {
    LOG(WARNING) << "No CPUs left for lanes after removing service CPUs "
                 << options.service_cpus.ToString() << "; running with one lane";
    return LaneTopology();
  }
  std::sort(domains.begin(), domains.end(), [](const CpuSet& lhs, const CpuSet& rhs) {
    return *lhs.Lowest() < *rhs.Lowest();
  });

  const size_t num_domains = domains.size();
  LOG_IF(DFATAL, options.requested_lanes > kMaxLanes)
      << "Requested " << options.requested_lanes << " lanes, more than " << kMaxLanes;
  const size_t num_lanes = std::min(
      options.requested_lanes == 0 ? num_domains : options.requested_lanes, kMaxLanes);
  if (num_lanes <= 1) {
    return LaneTopology();
  }

  LaneTopology result;
  for (size_t lane = 0; lane != num_lanes; ++lane) {
    // N <= D: lane owns a contiguous block of whole domains. N > D: lanes share one domain.
    size_t first = lane * num_domains / num_lanes;
    size_t last = num_lanes <= num_domains ? (lane + 1) * num_domains / num_lanes : first + 1;
    CpuSet mask;
    for (auto i = first; i != last; ++i) {
      mask |= domains[i];
    }
    result.lane_masks_.push_back(mask);
    result.lane_domains_.emplace_back(first, last);
  }
  result.domains_ = std::move(domains);
  result.service_mask_ =
      options.service_cpus.empty() ? options.process_cpuset : options.service_cpus;
  result.full_process_cpuset_ = options.process_cpuset;
  result.affinity_enabled_ = options.affinity_enabled;
  return result;
}

const CpuSet& LaneTopology::lane_mask(LaneId lane) const {
  DCHECK_LT(lane, lane_masks_.size());
  return lane < lane_masks_.size() ? lane_masks_[lane] : full_process_cpuset_;
}

std::pair<size_t, size_t> LaneTopology::lane_domains(LaneId lane) const {
  DCHECK_LT(lane, lane_domains_.size());
  return lane < lane_domains_.size() ? lane_domains_[lane] : std::pair<size_t, size_t>(0, 0);
}

std::string LaneTopology::ToString() const {
  if (!active()) {
    return "{ inactive }";
  }
  std::string domains;
  for (size_t i = 0; i != domains_.size(); ++i) {
    domains += Format("$0[$1]", i ? " " : "", domains_[i].ToString());
  }

  // One entry per run of lanes with the same shape: with N <= D, lanes holding the same number
  // of consecutive domains; with N > D, lanes sharing a domain. Mappings that need many runs
  // (e.g. 300 domains on 254 lanes) are printed as the block formula instead.
  constexpr size_t kMaxRuns = 16;
  const size_t num_lanes = lane_masks_.size();
  const size_t num_domains = domains_.size();
  const bool shared = num_lanes > num_domains;
  std::vector<std::string> runs;
  for (size_t begin = 0; begin != num_lanes;) {
    auto key = [this, shared](size_t lane) {
      const auto& [first, last] = lane_domains_[lane];
      return shared ? first : last - first;
    };
    auto end = begin + 1;
    while (end != num_lanes && key(end) == key(begin)) {
      ++end;
    }
    if (shared) {
      runs.push_back(Format("lanes $0 -> domain $1", RangeToString(begin, end - 1), key(begin)));
    } else {
      runs.push_back(Format(
          "lanes $0 -> domains $1 ($2 each)", RangeToString(begin, end - 1),
          RangeToString(lane_domains_[begin].first, lane_domains_[end - 1].second - 1),
          key(begin)));
    }
    begin = end;
  }
  std::string map;
  if (runs.size() <= kMaxRuns) {
    for (const auto& run : runs) {
      map += (map.empty() ? "" : ", ") + run;
    }
  } else if (shared) {
    map = Format("lane i -> domain i*$0/$1", num_domains, num_lanes);
  } else {
    map = Format("lane i -> domains [i*$0/$1, (i+1)*$0/$1)", num_domains, num_lanes);
  }

  return Format(
      "{ lanes: $0 affinity: $1 domains: $2 map: $3 service_mask: $4 process_cpuset: $5 }",
      num_lanes, affinity_enabled_, domains, map, service_mask_.ToString(),
      full_process_cpuset_.ToString());
}

LaneId LaneOf(Slice tablet_id, size_t num_lanes) {
  if (num_lanes <= 1) {
    return 0;
  }
  DCHECK_LE(num_lanes, kMaxLanes);
  return static_cast<LaneId>(
      HashUtil::MurmurHash2_64(tablet_id.AsStringView(), /* seed= */ 0) % num_lanes);
}

LaneId LaneOf(Slice tablet_id) {
  return LaneOf(tablet_id, GetLaneTopology().num_lanes());
}

const LaneTopology& GetLaneTopology() {
  const auto* topology = current_topology.load(std::memory_order_acquire);
  return topology ? *topology : InactiveTopology();
}

void SetLaneTopology(const LaneTopology& topology) {
  std::lock_guard lock(TopologyMutex());
  const auto* current = current_topology.load(std::memory_order_acquire);
  if ((current ? *current : InactiveTopology()) == topology) {
    return;
  }
  if (current) {
    LOG(DFATAL) << "Lane topology already set to " << current->ToString()
                << ", ignoring different topology " << topology.ToString();
    return;
  }
  current_topology.store(RetainTopology(topology), std::memory_order_release);
}

TEST_SetLaneTopology::TEST_SetLaneTopology() {
  std::lock_guard lock(TopologyMutex());
  previous_ = current_topology.exchange(nullptr, std::memory_order_acq_rel);
}

TEST_SetLaneTopology::TEST_SetLaneTopology(const LaneTopology& topology) {
  std::lock_guard lock(TopologyMutex());
  previous_ = current_topology.exchange(RetainTopology(topology), std::memory_order_acq_rel);
}

TEST_SetLaneTopology::~TEST_SetLaneTopology() {
  std::lock_guard lock(TopologyMutex());
  current_topology.store(previous_, std::memory_order_release);
}

} // namespace yb
