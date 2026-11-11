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

#include <algorithm>
#include <random>

#include <gtest/gtest.h>

#include "yb/gutil/casts.h"

#include "yb/util/format.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/logging_test_util.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"

namespace yb {

class LanesTest : public YBTest {
};

namespace {

CpuSet Cpus(std::string_view list) {
  auto result = CpuSet::Parse(list);
  CHECK(result.ok()) << result.status();
  return *result;
}

// num_domains domains of two CPUs each: {0-1}, {2-3}, ...
std::vector<CpuSet> PairDomains(size_t num_domains) {
  std::vector<CpuSet> result;
  for (size_t i = 0; i != num_domains; ++i) {
    CpuSet domain;
    domain.Set(2 * i);
    domain.Set(2 * i + 1);
    result.push_back(domain);
  }
  return result;
}

LaneTopology BuildPairs(size_t num_domains, size_t requested_lanes, CpuSet service = CpuSet()) {
  return LaneTopology::Build(LaneTopologyOptions{
      .domains = PairDomains(num_domains),
      .process_cpuset = CpuSet::FirstN(2 * num_domains),
      .requested_lanes = requested_lanes,
      .service_cpus = service,
  });
}

std::vector<std::string> Masks(const LaneTopology& topology) {
  std::vector<std::string> result;
  for (size_t lane = 0; lane != topology.num_lanes(); ++lane) {
    result.push_back(topology.lane_mask(narrow_cast<LaneId>(lane)).ToString());
  }
  return result;
}

size_t CountContaining(const StringVectorSink& sink, std::string_view text) {
  return std::count_if(
      sink.logged_msgs().begin(), sink.logged_msgs().end(),
      [text](const std::string& msg) { return msg.find(text) != std::string::npos; });
}

} // namespace

// N < D: lane i owns domains [floor(i * D / N), floor((i + 1) * D / N)).
TEST_F(LanesTest, FewerLanesThanDomains) {
  auto topology = BuildPairs(24, 5);
  ASSERT_TRUE(topology.active());
  ASSERT_EQ(topology.num_lanes(), 5);
  // Blocks: [0,4) [4,9) [9,14) [14,19) [19,24).
  ASSERT_EQ(Masks(topology),
            (std::vector<std::string>{"0-7", "8-17", "18-27", "28-37", "38-47"}));
  ASSERT_EQ(topology.lane_domains(1), (std::pair<size_t, size_t>(4, 9)));
  ASSERT_EQ(topology.service_mask().ToString(), "0-47");
  ASSERT_EQ(topology.full_process_cpuset().ToString(), "0-47");
  ASSERT_TRUE(topology.affinity_enabled());
  ASSERT_STR_CONTAINS(
      topology.ToString(),
      "map: lanes 0 -> domains 0-3 (4 each), lanes 1-4 -> domains 4-23 (5 each) service_mask:");
}

TEST_F(LanesTest, LanesEqualDomains) {
  auto forced = BuildPairs(4, 4);
  ASSERT_EQ(Masks(forced), (std::vector<std::string>{"0-1", "2-3", "4-5", "6-7"}));
  auto automatic = BuildPairs(4, 0);
  ASSERT_EQ(automatic, forced);
  ASSERT_EQ(automatic.domains().size(), 4);
  ASSERT_EQ(automatic.ToString(),
            "{ lanes: 4 affinity: 1 domains: [0-1] [2-3] [4-5] [6-7] "
            "map: lanes 0-3 -> domains 0-3 (1 each) service_mask: 0-7 process_cpuset: 0-7 }");
  ASSERT_STR_CONTAINS(BuildPairs(24, 0).ToString(), "map: lanes 0-23 -> domains 0-23 (1 each) ");
}

// N > D: lane i uses domain floor(i * D / N); lanes share whole-domain masks.
TEST_F(LanesTest, MoreLanesThanDomains) {
  auto topology = BuildPairs(2, 5);
  ASSERT_EQ(topology.num_lanes(), 5);
  ASSERT_EQ(Masks(topology), (std::vector<std::string>{"0-1", "0-1", "0-1", "2-3", "2-3"}));
  ASSERT_EQ(topology.ToString(),
            "{ lanes: 5 affinity: 1 domains: [0-1] [2-3] map: lanes 0-2 -> domain 0, "
            "lanes 3-4 -> domain 1 service_mask: 0-3 process_cpuset: 0-3 }");

  // Forced lanes on one domain still give an active topology, as on small test hosts.
  auto one_domain = BuildPairs(1, 4);
  ASSERT_TRUE(one_domain.active());
  ASSERT_EQ(Masks(one_domain), (std::vector<std::string>(4, "0-1")));
}

TEST_F(LanesTest, ServiceCpusRemovedFromDomains) {
  auto topology = LaneTopology::Build(LaneTopologyOptions{
      .domains = {Cpus("0-3"), Cpus("4-7"), Cpus("8-11")},
      .process_cpuset = Cpus("0-11"),
      .requested_lanes = 0,
      .service_cpus = Cpus("0-3"),
      .affinity_enabled = false,
  });
  ASSERT_EQ(Masks(topology), (std::vector<std::string>{"4-7", "8-11"}));
  ASSERT_EQ(topology.service_mask().ToString(), "0-3");
  ASSERT_EQ(topology.full_process_cpuset().ToString(), "0-11");
  ASSERT_FALSE(topology.affinity_enabled());

  // Partial removal keeps the domain; ordering follows the lowest remaining CPU.
  topology = LaneTopology::Build(LaneTopologyOptions{
      .domains = {Cpus("0-3"), Cpus("4-7"), Cpus("8-11")},
      .process_cpuset = Cpus("0-11"),
      .requested_lanes = 0,
      .service_cpus = Cpus("0,4"),
  });
  ASSERT_EQ(Masks(topology), (std::vector<std::string>{"1-3", "5-7", "8-11"}));
  ASSERT_EQ(topology.service_mask().ToString(), "0,4");
}

TEST_F(LanesTest, AllCpusRemovedIsInactive) {
  StringVectorSink sink;
  ScopedRegisterSink registration(&sink);
  auto topology = BuildPairs(2, 0, Cpus("0-3"));
  ASSERT_FALSE(topology.active());
  ASSERT_EQ(topology, LaneTopology());
  ASSERT_EQ(CountContaining(sink, "No CPUs left for lanes"), 1);
}

TEST_F(LanesTest, SingleLaneIsInactive) {
  ASSERT_FALSE(LaneTopology().active());
  ASSERT_EQ(LaneTopology().num_lanes(), 1);
  ASSERT_EQ(LaneTopology().ToString(), "{ inactive }");

  // Requested 1, or auto with one domain, give the canonical inactive value.
  ASSERT_EQ(BuildPairs(24, 1), LaneTopology());
  ASSERT_EQ(BuildPairs(1, 0), LaneTopology());
  ASSERT_EQ(BuildPairs(1, 0).num_lanes(), 1);
}

// Auto mode with more than kMaxLanes domains uses the block mapping onto kMaxLanes lanes.
TEST_F(LanesTest, AutoCapsAtMaxLanes) {
  constexpr size_t kDomains = 300;
  auto topology = BuildPairs(kDomains, 0);
  ASSERT_EQ(topology.num_lanes(), kMaxLanes);
  size_t next = 0;
  for (size_t lane = 0; lane != kMaxLanes; ++lane) {
    auto [first, last] = topology.lane_domains(narrow_cast<LaneId>(lane));
    ASSERT_EQ(first, next);
    ASSERT_EQ(first, lane * kDomains / kMaxLanes);
    ASSERT_GE(last - first, 1);
    ASSERT_LE(last - first, 2);
    next = last;
  }
  ASSERT_EQ(next, kDomains);
  // The mapping is printed compactly, not one entry per lane.
  ASSERT_STR_CONTAINS(topology.ToString(), "map: lane i -> domains [i*300/254, (i+1)*300/254)");
}

// Cross-host, cross-version contract: these values must never change.
TEST_F(LanesTest, LaneOfGolden) {
  const std::string kIds[] = {
      "00000000000000000000000000000000",
      "0123456789abcdef0123456789abcdef",
      "fbe3a7e9c4e14d7d9c5bd0a6a8c3b211",
  };
  std::vector<int> lanes_24, lanes_7;
  for (const auto& id : kIds) {
    lanes_24.push_back(LaneOf(id, 24));
    lanes_7.push_back(LaneOf(id, 7));
  }
  // Values match an independent little-endian MurmurHash64A reference, so they hold on x86_64
  // and aarch64 alike.
  ASSERT_EQ(lanes_24, (std::vector<int>{13, 10, 6}));
  ASSERT_EQ(lanes_7, (std::vector<int>{1, 0, 5}));

  // Slice, std::string and std::string_view callers agree; one lane is always lane 0.
  ASSERT_EQ(LaneOf(Slice(kIds[1]), 24), LaneOf(kIds[1], 24));
  ASSERT_EQ(LaneOf(std::string_view(kIds[1]), 24), LaneOf(kIds[1], 24));
  ASSERT_EQ(LaneOf(kIds[1], 1), 0);
  ASSERT_EQ(LaneOf(kIds[1], 0), 0);
  ASSERT_DEBUG_DEATH(
      ASSERT_EQ(LaneOf(kIds[1], 1000), LaneOf(kIds[1], kMaxLanes)), "num_lanes <= kMaxLanes");
}

TEST_F(LanesTest, LaneOfDistribution) {
  constexpr size_t kLanes = 24;
  constexpr size_t kIds = 100000;
  std::mt19937_64 rng(42);
  std::vector<size_t> counts(kLanes);
  for (size_t i = 0; i != kIds; ++i) {
    auto id = Format("$0$1", rng(), rng());
    ++counts[LaneOf(id, kLanes)];
  }
  const size_t mean = kIds / kLanes;
  for (auto count : counts) {
    ASSERT_GT(count, mean * 9 / 10) << AsString(counts);
    ASSERT_LT(count, mean * 11 / 10) << AsString(counts);
  }
}

TEST_F(LanesTest, LaneOfUsesGlobalTopology) {
  const std::string id = "0123456789abcdef0123456789abcdef";
  {
    TEST_SetLaneTopology scope;
    ASSERT_EQ(LaneOf(id), 0);
  }
  TEST_SetLaneTopology scope(BuildPairs(24, 7));
  ASSERT_EQ(LaneOf(id), LaneOf(id, 7));
}

TEST_F(LanesTest, SetLaneTopologyEqualRepeatIsNoOp) {
  TEST_SetLaneTopology scope;
  ASSERT_EQ(GetLaneTopology(), LaneTopology());

  // The inactive topology never claims the unset global.
  SetLaneTopology(LaneTopology());
  auto topology = BuildPairs(4, 0);
  SetLaneTopology(topology);
  const auto* installed = &GetLaneTopology();
  ASSERT_EQ(*installed, topology);

  StringVectorSink sink;
  ScopedRegisterSink registration(&sink);
  SetLaneTopology(BuildPairs(4, 4));
  ASSERT_EQ(&GetLaneTopology(), installed);
  ASSERT_EQ(CountContaining(sink, "Lane topology"), 0);
}

TEST_F(LanesTest, SetDifferentLaneTopologyIsDFatal) {
  TEST_SetLaneTopology scope;
  auto topology = BuildPairs(4, 0);
  SetLaneTopology(topology);
  ASSERT_DEBUG_DEATH(SetLaneTopology(BuildPairs(4, 2)), "Lane topology already set");
  ASSERT_EQ(GetLaneTopology(), topology);
  ASSERT_DEBUG_DEATH(SetLaneTopology(LaneTopology()), "Lane topology already set");
  ASSERT_EQ(GetLaneTopology(), topology);
}

TEST_F(LanesTest, ScopedOverrideRestoresPrevious) {
  auto outer_topology = BuildPairs(4, 0);
  auto inner_topology = BuildPairs(2, 0);
  TEST_SetLaneTopology outer(outer_topology);
  ASSERT_EQ(GetLaneTopology(), outer_topology);
  {
    TEST_SetLaneTopology inner(inner_topology);
    ASSERT_EQ(GetLaneTopology(), inner_topology);
  }
  ASSERT_EQ(GetLaneTopology(), outer_topology);
  {
    TEST_SetLaneTopology unset;
    ASSERT_EQ(GetLaneTopology(), LaneTopology());
    SetLaneTopology(inner_topology);
    ASSERT_EQ(GetLaneTopology(), inner_topology);
  }
  ASSERT_EQ(GetLaneTopology(), outer_topology);
}

TEST_F(LanesTest, NormalizeLane) {
  const LaneTopology inactive;
  ASSERT_EQ(NormalizeLane(inactive, 0), kNoLane);
  ASSERT_EQ(NormalizeLane(inactive, kNoLane), kNoLane);
  auto topology = LaneTopology::Build({
      .domains = PairDomains(3), .process_cpuset = Cpus("0-5"), .requested_lanes = 3});
  ASSERT_EQ(NormalizeLane(topology, 2), 2);
  ASSERT_EQ(NormalizeLane(topology, 7), 1);
  ASSERT_EQ(NormalizeLane(topology, kMaxLanes - 1), (kMaxLanes - 1) % 3);
  ASSERT_EQ(NormalizeLane(topology, kNoLane), kNoLane);
  ASSERT_EQ(LaneToString(kNoLane), "none");
  ASSERT_EQ(LaneToString(7), "7");
}

TEST_F(LanesTest, ApplyLaneSetsMaskAndLane) {
  auto topology = MakeTwoLaneTestTopology();
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);
  ASSERT_TRUE(LaneModeActive());
  RunOnStdThread([&topology] {
    ASSERT_EQ(ThisThreadLane(), kNoLane);
    auto calls = TEST_CpuAffinityCallCount();
    ApplyLaneToCurrentThread(0);
    ASSERT_EQ(ThisThreadLane(), 0);
    ASSERT_EQ(ThisThreadMask(), topology->lane_mask(0));
    ASSERT_EQ(TEST_CpuAffinityCallCount(), calls + 1);

    // The current lane again: no system call.
    ApplyLaneToCurrentThread(0);
    ASSERT_EQ(TEST_CpuAffinityCallCount(), calls + 1);

    ApplyLaneToCurrentThread(1);
    ASSERT_EQ(ThisThreadLane(), 1);
    ASSERT_EQ(ThisThreadMask(), topology->lane_mask(1));

    ApplyServiceMaskToCurrentThread();
    ASSERT_EQ(ThisThreadLane(), kNoLane);
    ASSERT_EQ(ThisThreadMask(), topology->service_mask());

    // Out of range lanes are reduced modulo the lane count.
    ApplyLaneToCurrentThread(5);
    ASSERT_EQ(ThisThreadLane(), 1);
    ASSERT_EQ(ThisThreadMask(), topology->lane_mask(1));
    ASSERT_EQ(TEST_CpuAffinityCallCount(), calls + 4);
  });
}

// With lane mode off nothing changes a mask, a lane or a counter, and the inactive topology's
// empty process cpuset is never passed to the kernel.
TEST_F(LanesTest, LanesOffChangesNothing) {
  TEST_SetLaneTopology scope{LaneTopology()};
  ASSERT_FALSE(LaneModeActive());
  ASSERT_TRUE(GetLaneTopology().full_process_cpuset().empty());
  RunOnStdThread([] {
    const auto mask = ThisThreadMask();
    const auto calls = TEST_CpuAffinityCallCount();
    const auto counts = LaneHandoffCounts::Now();
    ApplyLaneToCurrentThread(0);
    ApplyServiceMaskToCurrentThread();
    {
      ScopedThreadAffinity full_cpuset(GetLaneTopology().full_process_cpuset());
      ScopedThreadAffinity other(CpuSet::FirstN(1));
      ASSERT_EQ(ThisThreadMask(), mask);
    }
    LaneHandoff handoff;
    handoff.RecordSubmitter();
    ASSERT_EQ(handoff.submitter(), kNoLane);
    handoff.RecordExecution(LaneCrossingCategory::kOther);
    CountLaneHandoffToThisThread(1, LaneCrossingCategory::kOther);
    CountLaneHandoffFromThisThread(1, LaneCrossingCategory::kReactor);
    ASSERT_EQ(ThisThreadLane(), kNoLane);
    ASSERT_EQ(ThisThreadMask(), mask);
    ASSERT_EQ(TEST_CpuAffinityCallCount(), calls);
    auto delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.total_crossings(), 0) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 0);
  });
}

// --tserver_lane_cpu_affinity=false: lanes are assigned and handoffs counted, but no mask changes.
TEST_F(LanesTest, AffinityDisabledSetsLaneOnly) {
  auto topology = MakeTwoLaneTestTopology(/* affinity_enabled= */ false);
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);
  LaneHandoff handoff;
  RunOnStdThread([&handoff] {
    ApplyLaneToCurrentThread(1);
    handoff.RecordSubmitter();
  });
  ASSERT_EQ(handoff.submitter(), 1);
  RunOnStdThread([&topology, &handoff] {
    const auto mask = ThisThreadMask();
    const auto calls = TEST_CpuAffinityCallCount();
    const auto counts = LaneHandoffCounts::Now();
    ApplyLaneToCurrentThread(0);
    ASSERT_EQ(ThisThreadLane(), 0);
    {
      ScopedThreadAffinity scoped(topology->lane_mask(1));
      ASSERT_EQ(ThisThreadMask(), mask);
    }
    handoff.RecordExecution(LaneCrossingCategory::kTabletPipeline);
    ASSERT_EQ(ThisThreadMask(), mask);
    ASSERT_EQ(TEST_CpuAffinityCallCount(), calls);
    auto delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.crossing(LaneCrossingCategory::kTabletPipeline), 1) << delta.ToString();
    ASSERT_EQ(delta.total_crossings(), 1);
  });
}

TEST_F(LanesTest, ScopedThreadAffinitySetsAndRestores) {
  auto topology = MakeTwoLaneTestTopology(/* affinity_enabled= */ true, /* service_cpu= */ true);
  if (!topology) {
    GTEST_SKIP() << "Needs at least 3 CPUs";
  }
  ASSERT_NE(topology->service_mask(), topology->full_process_cpuset());
  TEST_SetLaneTopology scope(*topology);
  RunOnStdThread([&topology] {
    ApplyServiceMaskToCurrentThread();
    ASSERT_EQ(ThisThreadMask(), topology->service_mask());
    const auto calls = TEST_CpuAffinityCallCount();
    {
      ScopedThreadAffinity full_cpuset(topology->full_process_cpuset());
      ASSERT_EQ(ThisThreadMask(), topology->full_process_cpuset());
    }
    ASSERT_EQ(ThisThreadMask(), topology->service_mask());
    ASSERT_EQ(ThisThreadLane(), kNoLane);
    ASSERT_EQ(TEST_CpuAffinityCallCount(), calls + 2);

    // An empty mask, or the mask the thread already has, makes no system call.
    {
      ScopedThreadAffinity empty{CpuSet()};
      ScopedThreadAffinity same(topology->service_mask());
      ASSERT_EQ(ThisThreadMask(), topology->service_mask());
    }
    ASSERT_EQ(TEST_CpuAffinityCallCount(), calls + 2);
  });
}

TEST_F(LanesTest, HandoffCounting) {
  auto topology = MakeTwoLaneTestTopology();
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);
  LaneHandoff from_lane_0, from_lane_1, from_unlaned, uncounted, callback_from_lane_1;
  uncounted.DisableCounting();
  callback_from_lane_1.SetCategory(LaneCrossingCategory::kCallback);
  RunOnStdThread([&] {
    ApplyLaneToCurrentThread(0);
    from_lane_0.RecordSubmitter();
    uncounted.RecordSubmitter();
  });
  RunOnStdThread([&] {
    ApplyLaneToCurrentThread(1);
    from_lane_1.RecordSubmitter();
    callback_from_lane_1.RecordSubmitter();
  });
  RunOnStdThread([&] {
    ApplyServiceMaskToCurrentThread();
    from_unlaned.RecordSubmitter();
  });
  ASSERT_EQ(from_lane_0.submitter(), 0);
  ASSERT_EQ(from_lane_1.submitter(), 1);
  ASSERT_EQ(from_unlaned.submitter(), kNoLane);

  RunOnStdThread([&] {
    ApplyLaneToCurrentThread(0);
    auto counts = LaneHandoffCounts::Now();
    from_lane_0.RecordExecution(LaneCrossingCategory::kRpcHandler);
    uncounted.RecordExecution(LaneCrossingCategory::kRpcHandler);
    auto delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.total_crossings(), 0) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 0);

    from_lane_1.RecordExecution(LaneCrossingCategory::kRpcHandler);
    delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.crossing(LaneCrossingCategory::kRpcHandler), 1) << delta.ToString();
    ASSERT_EQ(delta.total_crossings(), 1);

    // An item's own category wins over the executing pool's.
    callback_from_lane_1.RecordExecution(LaneCrossingCategory::kRpcHandler);
    delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.crossing(LaneCrossingCategory::kCallback), 1) << delta.ToString();
    ASSERT_EQ(delta.crossing(LaneCrossingCategory::kRpcHandler), 1);
    ASSERT_EQ(delta.total_crossings(), 2);

    from_unlaned.RecordExecution(LaneCrossingCategory::kRpcHandler);
    delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.total_crossings(), 2) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 1);

    // A reactor-style handoff counted by the submitter: lane 0 into a lane 1 executor.
    CountLaneHandoffFromThisThread(1, LaneCrossingCategory::kReactor);
    CountLaneHandoffFromThisThread(0, LaneCrossingCategory::kReactor);
    CountLaneHandoffFromThisThread(kNoLane, LaneCrossingCategory::kReactor);
    delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.crossing(LaneCrossingCategory::kReactor), 1) << delta.ToString();
    ASSERT_EQ(delta.total_crossings(), 3);
  });

  // An unlaned submitter counts nothing into a reactor.
  RunOnStdThread([&] {
    ApplyServiceMaskToCurrentThread();
    auto counts = LaneHandoffCounts::Now();
    CountLaneHandoffFromThisThread(1, LaneCrossingCategory::kReactor);
    auto delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.total_crossings(), 0) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 0);
  });

  // Unlaned executors count nothing.
  RunOnStdThread([&] {
    ApplyServiceMaskToCurrentThread();
    auto counts = LaneHandoffCounts::Now();
    from_lane_1.RecordExecution(LaneCrossingCategory::kOther);
    from_unlaned.RecordExecution(LaneCrossingCategory::kOther);
    auto delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.total_crossings(), 0) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 0);
  });
}

TEST_F(LanesTest, LaneShardedCounters) {
  auto counters = std::make_unique<LaneShardedCounters<2>>();
  counters->Increment(0, 0);
  counters->Increment(kMaxLanes - 1, 0);
  // kNoLane has its own slot.
  counters->Increment(kNoLane, 0);
  counters->Increment(kNoLane, 1);
  ASSERT_EQ(counters->Sum(0), 3);
  ASSERT_EQ(counters->Sum(1), 1);
  ASSERT_EQ(counters->Sum(2), 0);
}

} // namespace yb
