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

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "yb/gutil/port.h"

#include "yb/util/cpu_topology.h"
#include "yb/util/enums.h"
#include "yb/util/logging.h"
#include "yb/util/slice.h"

namespace yb {

// A lane is a CPU domain plus the threads and tablets confined to it. See
// architecture/design/tserver-ccx-lanes.md.
using LaneId = uint8_t;

// One byte, so a lane fits the RPC lane preamble; 0xFF is reserved for "no lane".
constexpr LaneId kNoLane = 0xFF;
constexpr size_t kMaxLanes = 254;

// "none" for kNoLane, else the lane number.
std::string LaneToString(LaneId lane);

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

namespace lanes_internal {

// Mirrors GetLaneTopology().active(); read on hot paths so that with lanes off each lane hook
// costs one relaxed load.
extern std::atomic<bool> lane_mode_active;

void ApplyLane(LaneId lane);
void CountHandoff(LaneId submitter, LaneId executor, LaneId slot, size_t counter);

} // namespace lanes_internal

// Whether the process-global topology is active.
inline bool LaneModeActive() {
  return lanes_internal::lane_mode_active.load(std::memory_order_relaxed);
}

// A lane value usable as an index for topology: kNoLane for kNoLane or an inactive topology,
// otherwise lane reduced modulo the lane count. Lane values from configuration or from elsewhere
// go through this before they are used.
LaneId NormalizeLane(const LaneTopology& topology, LaneId lane);

// Lane of the calling thread as set by ApplyLaneToCurrentThread or SetThisThreadLane; kNoLane
// unless a lane was set. A thread keeps its lane if lane mode is turned off afterwards (only tests
// do that), so callers that count or route by lane also check LaneModeActive().
LaneId ThisThreadLane();

// Sets the calling thread's lane without changing its CPU mask.
void SetThisThreadLane(LaneId lane);

// When lane mode is active: confines the calling thread to the lane's mask, or to the service
// mask for kNoLane, and makes it ThisThreadLane(). The lane is set even with
// --tserver_lane_cpu_affinity=false; only the mask is skipped. Repeating the current lane makes no
// system call. A failure to set the mask is logged (rate limited) and the thread runs unpinned.
// No-op when lane mode is off.
inline void ApplyLaneToCurrentThread(LaneId lane) {
  if (LaneModeActive()) {
    lanes_internal::ApplyLane(lane);
  }
}

inline void ApplyServiceMaskToCurrentThread() {
  ApplyLaneToCurrentThread(kNoLane);
}

// Sets the calling thread's CPU mask for the scope's lifetime and restores the previous mask on
// exit. No-op when lane mode is off, when affinity is disabled for the topology (no lane mask was
// ever set, so there is nothing to undo), or when mask is empty.
class ScopedThreadAffinity {
 public:
  explicit ScopedThreadAffinity(const CpuSet& mask);
  ~ScopedThreadAffinity();

  ScopedThreadAffinity(const ScopedThreadAffinity&) = delete;
  void operator=(const ScopedThreadAffinity&) = delete;

 private:
  std::optional<CpuSet> saved_;
};

// What kind of thread a cross-lane handoff lands on; each has its own crossing counter.
YB_DEFINE_ENUM(LaneCrossingCategory, (kReactor)(kRpcHandler)(kTabletPipeline)(kCallback)(kOther));

// Counters sharded by lane: one cache-line slot per lane plus a spare slot for kNoLane, summed on
// read. A slot must only be written by threads of its lane, so that counting never adds a write
// shared between lanes.
template <size_t N>
class LaneShardedCounters {
 public:
  void Increment(LaneId lane, size_t counter) {
    DCHECK_LT(counter, N);
    if (counter < N) {
      slots_[std::min<size_t>(lane, kMaxLanes)].values[counter].fetch_add(
          1, std::memory_order_relaxed);
    }
  }

  uint64_t Sum(size_t counter) const {
    uint64_t result = 0;
    if (counter < N) {
      for (const auto& slot : slots_) {
        result += slot.values[counter].load(std::memory_order_relaxed);
      }
    }
    return result;
  }

 private:
  static_assert(sizeof(std::atomic<uint64_t>) * N <= CACHELINE_SIZE);

  struct alignas(CACHELINE_SIZE) Slot {
    std::array<std::atomic<uint64_t>, N> values = {};
  };

  std::array<Slot, kMaxLanes + 1> slots_;
};

// Cross-lane handoffs counted in lane mode, by the category of the executing thread.
uint64_t LaneCrossingCount(LaneCrossingCategory category);

// Handoffs from a thread with no lane to a laned thread.
uint64_t LaneEntriesFromUnlanedCount();

// Counts a handoff that the calling thread executes and that was submitted from submitter: a
// crossing when both are laned and differ, an entry from unlaned when only the calling thread is
// laned.
inline void CountLaneHandoffToThisThread(LaneId submitter, LaneCrossingCategory category) {
  if (LaneModeActive()) {
    const auto executor = ThisThreadLane();
    lanes_internal::CountHandoff(
        submitter, executor, executor, static_cast<size_t>(category));
  }
}

// Same for a handoff the calling thread submits to an executor with a fixed lane (a reactor). The
// count goes to the calling thread's slot, which is the one written from this lane.
inline void CountLaneHandoffFromThisThread(LaneId executor, LaneCrossingCategory category) {
  if (executor != kNoLane && LaneModeActive()) {
    const auto submitter = ThisThreadLane();
    lanes_internal::CountHandoff(
        submitter, executor, submitter, static_cast<size_t>(category));
  }
}

// The submitter's lane carried by a queued item and counted when the item runs. Wrapper tasks
// that only drain items carrying their own LaneHandoff disable counting, so that one handoff is
// counted once.
class LaneHandoff {
 public:
  void RecordSubmitter() {
    if (counted_) {
      submitter_ = LaneModeActive() ? ThisThreadLane() : kNoLane;
    }
  }

  void RecordExecution(LaneCrossingCategory category) const {
    if (counted_) {
      CountLaneHandoffToThisThread(submitter_, category);
    }
  }

  void DisableCounting() { counted_ = false; }

  LaneId submitter() const { return submitter_; }

 private:
  LaneId submitter_ = kNoLane;
  bool counted_ = true;
};

} // namespace yb
