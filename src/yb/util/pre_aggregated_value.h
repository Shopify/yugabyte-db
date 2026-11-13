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

#include <atomic>
#include <memory>

#include "yb/gutil/port.h"

#include "yb/util/lanes.h"

namespace yb {

// The value of a metric pre-aggregated for Prometheus: the sum of the values of the metrics that
// share it, which is every tablet of a table for a tablet-level metric. Each of those metrics
// adds to it whenever its own value changes, so that a scrape reads one number per table instead
// of visiting every tablet.
//
// That makes it a word that the tablets of a table write on every operation, from the threads of
// every lane, even though each tablet's own metric is only written by its lane. In lane mode the
// value therefore gets a cell per lane once it has proved to be busy: threads of a lane add to
// their lane's cell, a cache line no other lane writes, and Load() sums the cells. Cells hold
// signed deltas that are only ever summed, so an amount may be added in one lane and taken back in
// another.
//
// The cells cost (L + 1) cache lines and there is a value per metric per table, so they are only
// allocated after kLaneCellsAfterIncrements increments from laned threads: metrics that are never
// or rarely updated, and tables without traffic, do not pay. With lanes off nothing is allocated.
class PreAggregatedValue {
 public:
  static constexpr int64_t kLaneCellsAfterIncrements = 1000;

  PreAggregatedValue() = default;

  PreAggregatedValue(const PreAggregatedValue&) = delete;
  void operator=(const PreAggregatedValue&) = delete;

  // For the update of a metric's value on behalf of an operation.
  void IncrementBy(int64_t amount) {
    if (auto* cell = lane_cells_.LaneLocal()) {
      cell->fetch_add(amount, std::memory_order_relaxed);
      return;
    }
    value_.fetch_add(amount, std::memory_order_relaxed);
    if (LaneModeActive() && !lane_cells_.allocated() && ThisThreadLane() != kNoLane &&
        laned_increments_.fetch_add(1, std::memory_order_relaxed) + 1 ==
            kLaneCellsAfterIncrements) {
      lane_cells_.Allocate(GetLaneTopology().num_lanes());
    }
  }

  // For what a metric brings when it joins the aggregate and takes with it when it leaves: does
  // not count towards allocating the cells.
  void AddShared(int64_t amount) {
    value_.fetch_add(amount, std::memory_order_relaxed);
  }

  int64_t Load() const {
    auto result = value_.load(std::memory_order_relaxed);
    lane_cells_.ForEach([&result](const std::atomic<int64_t>& cell) {
      result += cell.load(std::memory_order_relaxed);
    });
    return result;
  }

  bool TEST_has_lane_cells() const {
    return lane_cells_.allocated();
  }

 private:
  // Read by every increment and written once.
  LaneSharded<std::atomic<int64_t>> lane_cells_;

  // Written by threads without a lane, and by all threads until the cells exist: kept off the
  // cache line of lane_cells_, which laned threads read on every increment.
  alignas(CACHELINE_SIZE) std::atomic<int64_t> value_{0};
  std::atomic<int64_t> laned_increments_{0};
};

using PreAggregatedValuePtr = std::shared_ptr<PreAggregatedValue>;

}  // namespace yb
