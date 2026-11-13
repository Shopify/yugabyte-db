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

#include <atomic>
#include <map>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "yb/tserver/pg_mutation_counter.h"

#include "yb/util/lanes.h"
#include "yb/util/test_util.h"

namespace yb::tserver {

namespace {

LaneTopology TestLanes(size_t num_lanes) {
  std::vector<CpuSet> domains(num_lanes);
  for (size_t i = 0; i != num_lanes; ++i) {
    domains[i].Set(i);
  }
  return LaneTopology::Build(LaneTopologyOptions{
      .domains = domains,
      .process_cpuset = CpuSet::FirstN(num_lanes),
      .requested_lanes = num_lanes,
      .affinity_enabled = false,
  });
}

using Totals = std::map<TableId, uint64_t>;

void AddTo(Totals* totals, const TableMutationCounts& counts) {
  for (const auto& [table_id, count] : counts) {
    (*totals)[table_id] += count.load();
  }
}

// Threads of the given lanes increase the counts of a common table and of a table of their own,
// one by one and in batches, while the counts are collected: nothing is lost or counted twice.
void RunWriters(const std::vector<LaneId>& lanes) {
  constexpr uint64_t kIterations = 20000;
  PgMutationCounter counter;
  std::atomic<size_t> running{lanes.size()};
  std::vector<std::thread> threads;
  for (size_t i = 0; i != lanes.size(); ++i) {
    threads.emplace_back([&counter, &running, lane = lanes[i], i] {
      SetThisThreadLane(lane);
      const TableId own_table = "own-" + std::to_string(i);
      for (uint64_t n = 0; n != kIterations; ++n) {
        counter.Increase("common", 1);
        counter.Increase(own_table, 2);
        counter.IncreaseBatch(std::vector<std::pair<TableId, uint64_t>>{
            {"common", 3}, {own_table, 4}, {"batch-only", 5}});
      }
      --running;
    });
  }
  Totals totals;
  while (running.load() != 0) {
    AddTo(&totals, counter.GetAndClear());
  }
  for (auto& thread : threads) {
    thread.join();
  }
  AddTo(&totals, counter.GetAndClear());
  ASSERT_TRUE(counter.GetAndClear().empty());

  Totals expected;
  expected["common"] = lanes.size() * kIterations * 4;
  expected["batch-only"] = lanes.size() * kIterations * 5;
  for (size_t i = 0; i != lanes.size(); ++i) {
    expected["own-" + std::to_string(i)] = kIterations * 6;
  }
  ASSERT_EQ(totals, expected);
}

} // namespace

TEST(PgMutationCounterTest, LanesOff) {
  RunWriters({kNoLane, kNoLane, LaneId(0), LaneId(1)});
}

TEST(PgMutationCounterTest, CountsOfAllLanesAreCollected) {
  TEST_SetLaneTopology scope(TestLanes(3));
  RunWriters({LaneId(0), LaneId(0), LaneId(1), LaneId(2), kNoLane, kNoLane});
}

TEST(PgMutationCounterTest, EmptyBatchAndEmptyCounter) {
  TEST_SetLaneTopology scope(TestLanes(2));
  PgMutationCounter counter;
  counter.IncreaseBatch(std::vector<std::pair<TableId, uint64_t>>{});
  ASSERT_TRUE(counter.GetAndClear().empty());
  counter.Increase("t", 7);
  auto counts = counter.GetAndClear();
  ASSERT_EQ(counts.size(), 1);
  ASSERT_EQ(counts["t"].load(), 7);
}

} // namespace yb::tserver
