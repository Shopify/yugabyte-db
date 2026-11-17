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

#include <array>
#include <functional>
#include <optional>
#include <string>

#include "yb/util/lanes.h"

namespace yb {

// Two-lane topology laid over the real CPUs of the calling thread, for tests that confine
// threads: the first (up to) four allowed CPUs are split into two domains, one lane each. With
// service_cpu the last of those CPUs is reserved for the service set instead. nullopt when the
// thread has too few CPUs (2, or 3 with service_cpu).
std::optional<LaneTopology> MakeTwoLaneTestTopology(
    bool affinity_enabled = true, bool service_cpu = false);

// The calling thread's CPU mask; CHECK-fails if it cannot be read.
CpuSet ThisThreadMask();

// Runs f on a new std::thread and joins it. Unlike a yb::Thread, a std::thread keeps the creating
// thread's mask, so f starts with the caller's mask and lane state untouched.
void RunOnStdThread(const std::function<void()>& f);

// Lane handoff counters at one point in time; subtract two to count what happened in between.
struct LaneHandoffCounts {
  std::array<uint64_t, kLaneCrossingCategoryMapSize> crossings = {};
  uint64_t entries_from_unlaned = 0;

  static LaneHandoffCounts Now();

  uint64_t crossing(LaneCrossingCategory category) const {
    return crossings[static_cast<size_t>(category)];
  }

  uint64_t total_crossings() const;

  LaneHandoffCounts operator-(const LaneHandoffCounts& rhs) const;

  std::string ToString() const;
};

} // namespace yb
