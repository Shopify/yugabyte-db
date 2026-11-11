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

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "yb/util/cpu_topology.h"
#include "yb/util/slice.h"

namespace yb {

// A lane is a CPU domain plus the threads and tablets confined to it. See
// architecture/design/tserver-ccx-lanes.md.
using LaneId = uint8_t;

// One byte, so a lane fits the RPC lane preamble; 0xFF is reserved for "no lane".
constexpr LaneId kNoLane = 0xFF;
constexpr size_t kMaxLanes = 254;

struct LaneTopologyOptions {
  // CPU domains as returned by DiscoverCpuDomains.
  std::vector<CpuSet> domains = {};

  // CPUs the process may run on.
  CpuSet process_cpuset = {};

  // 0: one lane per domain (capped at kMaxLanes); N: exactly N lanes.
  size_t requested_lanes = 1;

  // CPUs reserved for the service set, already restricted to process_cpuset. They are removed
  // from every domain before lanes are mapped.
  CpuSet service_cpus = {};

  bool affinity_enabled = true;
};

// Immutable lane layout. The default value is the inactive (single-lane) topology, which is also
// what Build returns whenever the layout resolves to one lane, so every inactive topology
// compares equal.
class LaneTopology {
 public:
  LaneTopology() = default;

  static LaneTopology Build(const LaneTopologyOptions& options);

  // Lane mode is on only with more than one lane; with one lane nothing lane-related runs.
  bool active() const { return lane_masks_.size() > 1; }

  size_t num_lanes() const { return active() ? lane_masks_.size() : 1; }

  // Whole-domain CPU mask of an active topology's lane.
  const CpuSet& lane_mask(LaneId lane) const;

  // CPUs for threads that belong to no lane.
  const CpuSet& service_mask() const { return service_mask_; }

  bool affinity_enabled() const { return affinity_enabled_; }

  // The process cpuset at discovery: the mask for children that must float across all CPUs
  // (the postmaster) even when service CPUs are set.
  const CpuSet& full_process_cpuset() const { return full_process_cpuset_; }

  // Domains left after removing service CPUs, ordered by lowest CPU.
  const std::vector<CpuSet>& domains() const { return domains_; }

  // Domain index range [first, second) whose union is the lane's mask.
  std::pair<size_t, size_t> lane_domains(LaneId lane) const;

  std::string ToString() const;

  bool operator==(const LaneTopology& rhs) const = default;

 private:
  std::vector<CpuSet> domains_;
  std::vector<CpuSet> lane_masks_;
  std::vector<std::pair<size_t, size_t>> lane_domains_;
  CpuSet service_mask_;
  CpuSet full_process_cpuset_;
  bool affinity_enabled_ = false;
};

// Lane of a tablet: MurmurHash2_64(tablet_id, seed 0) % num_lanes, 0 with one lane.
//
// Every host with the same lane count must compute the same lane for a tablet, across builds and
// versions: a leader and its followers then replicate within one lane on both ends without
// coordination. Changing the hash or seed silently loses that locality on mixed clusters; never
// use std::hash here. All lane lookups for tablets go through this function.
LaneId LaneOf(Slice tablet_id, size_t num_lanes);

// LaneOf with the lane count of the process-global topology.
LaneId LaneOf(Slice tablet_id);

// The process-global topology; the inactive topology until SetLaneTopology is called.
// Safe to call from any thread; the returned reference stays valid for the process lifetime.
const LaneTopology& GetLaneTopology();

// Sets the process-global topology. Several tablet servers (and masters) can share a process in
// tests, and a tablet server can restart in place, so setting a topology equal to the current
// one is a no-op; in particular the inactive topology never replaces an unset one. Once an
// active topology is set, setting a different one is a DFATAL and leaves the current one.
void SetLaneTopology(const LaneTopology& topology);

// Replaces the process-global topology for the scope's lifetime and restores the previous state,
// including "never set", on exit. Without an argument the scope starts unset, so
// SetLaneTopology inside it behaves as at process start. For tests only.
class TEST_SetLaneTopology {
 public:
  TEST_SetLaneTopology();
  explicit TEST_SetLaneTopology(const LaneTopology& topology);
  ~TEST_SetLaneTopology();

  TEST_SetLaneTopology(const TEST_SetLaneTopology&) = delete;
  void operator=(const TEST_SetLaneTopology&) = delete;

 private:
  const LaneTopology* previous_;
};

} // namespace yb
