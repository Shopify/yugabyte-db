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
#include <map>
#include <set>

#include <gtest/gtest.h>

#include "yb/gutil/casts.h"

#include "yb/rpc/messenger.h"

#include "yb/tserver/tserver_lanes.h"

#include "yb/util/cpu_topology_test_util.h"
#include "yb/util/env.h"
#include "yb/util/flags.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/logging_test_util.h"
#include "yb/util/metrics.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"

DECLARE_string(TEST_cpu_topology_cpuset);
DECLARE_string(TEST_cpu_topology_sysfs_root);
DECLARE_int32(log_appender_lane_mode_max_wait_ms);
DECLARE_string(tserver_lane_background);
DECLARE_int32(tserver_lane_connections_per_remote);
DECLARE_bool(tserver_lane_cpu_affinity);
DECLARE_string(tserver_lane_domain);
DECLARE_int32(tserver_lane_reactors);
DECLARE_int32(tserver_lanes);
DECLARE_string(tserver_service_lane_cpus);

namespace yb::tserver {

class TserverLanesTest : public YBTest {
 protected:
  // Two sockets with one L3 each, 4 cores x 2 SMT per socket: domains {0-3,8-11}, {4-7,12-15}.
  void UseTwoDomainTree() {
    auto root = GetTestPath("sys_cpu");
    ASSERT_OK(WriteFakeSysfsCpuTree(
        root, MakeFakeCpus({.sockets = 2, .l3_per_socket = 1, .cores_per_l3 = 4, .smt = 2})));
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_sysfs_root) = root;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_cpuset) = "0-15";
  }

  static std::vector<std::string> Masks(const LaneTopology& topology) {
    std::vector<std::string> result;
    for (size_t lane = 0; lane != topology.num_lanes(); ++lane) {
      result.push_back(topology.lane_mask(narrow_cast<LaneId>(lane)).ToString());
    }
    return result;
  }

  static size_t CountContaining(const StringVectorSink& sink, std::string_view text) {
    return std::count_if(
        sink.logged_msgs().begin(), sink.logged_msgs().end(),
        [text](const std::string& msg) { return msg.find(text) != std::string::npos; });
  }
};

TEST_F(TserverLanesTest, FlagValidation) {
  for (auto value : {"0", "1", "24", "254"}) {
    ASSERT_OK(flags_internal::ValidateFlagValue("tserver_lanes", value));
  }
  for (auto value : {"-1", "255", "1000"}) {
    ASSERT_NOK(flags_internal::ValidateFlagValue("tserver_lanes", value));
  }
  ASSERT_OK(flags_internal::ValidateFlagValue("tserver_lane_domain", "l3"));
  ASSERT_OK(flags_internal::ValidateFlagValue("tserver_lane_domain", "socket"));
  ASSERT_NOK(flags_internal::ValidateFlagValue("tserver_lane_domain", "numa"));
  ASSERT_NOK(flags_internal::ValidateFlagValue("tserver_lane_domain", ""));
  ASSERT_OK(flags_internal::ValidateFlagValue("tserver_service_lane_cpus", ""));
  ASSERT_OK(flags_internal::ValidateFlagValue("tserver_service_lane_cpus", "0-3,192-195"));
  ASSERT_NOK(flags_internal::ValidateFlagValue("tserver_service_lane_cpus", "0-"));
  ASSERT_NOK(flags_internal::ValidateFlagValue("tserver_service_lane_cpus", "2048"));
  for (auto value : {"0", "1", "256"}) {
    ASSERT_OK(flags_internal::ValidateFlagValue("tserver_lane_reactors", value));
  }
  for (auto value : {"-1", "257"}) {
    ASSERT_NOK(flags_internal::ValidateFlagValue("tserver_lane_reactors", value));
  }
  for (auto value : {"1", "2", "256"}) {
    ASSERT_OK(flags_internal::ValidateFlagValue("tserver_lane_connections_per_remote", value));
  }
  for (auto value : {"0", "257"}) {
    ASSERT_NOK(flags_internal::ValidateFlagValue("tserver_lane_connections_per_remote", value));
  }
}

// Only an active topology makes the tablet server's messenger laned, with the lane flags.
TEST_F(TserverLanesTest, MessengerLaneSettings) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lane_reactors) = 3;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lane_connections_per_remote) = 4;
  {
    TEST_SetLaneTopology scope{LaneTopology()};
    rpc::MessengerBuilder builder("TabletServer");
    SetupTserverLaneMessenger(&builder);
    ASSERT_EQ(builder.lane_topology(), nullptr);
    ASSERT_EQ(builder.reactors_per_lane(), 0);
    ASSERT_EQ(builder.connections_per_lane_remote(), 2);
  }
  auto topology = MakeTwoLaneTestTopology(/* affinity_enabled= */ false);
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);
  rpc::MessengerBuilder builder("TabletServer");
  SetupTserverLaneMessenger(&builder);
  ASSERT_EQ(builder.lane_topology(), &GetLaneTopology());
  ASSERT_EQ(builder.reactors_per_lane(), 3);
  ASSERT_EQ(builder.connections_per_lane_remote(), 4);
}

TEST_F(TserverLanesTest, AutoLanes) {
  ASSERT_NO_FATALS(UseTwoDomainTree());
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = 0;
  std::string description;
  auto topology = ASSERT_RESULT(ComputeTserverLaneTopology(&description));
  ASSERT_EQ(topology.num_lanes(), 2);
  ASSERT_EQ(Masks(topology), (std::vector<std::string>{"0-3,8-11", "4-7,12-15"}));
  ASSERT_EQ(topology.service_mask().ToString(), "0-15");
  ASSERT_TRUE(topology.affinity_enabled());
  ASSERT_STR_CONTAINS(description, "lanes active");
  ASSERT_STR_CONTAINS(description, "cache level 3");
  LOG(INFO) << description;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lane_domain) = "socket";
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lane_cpu_affinity) = false;
  auto by_socket = ASSERT_RESULT(ComputeTserverLaneTopology(&description));
  ASSERT_EQ(Masks(by_socket), Masks(topology));
  ASSERT_FALSE(by_socket.affinity_enabled());
  ASSERT_STR_CONTAINS(description, "physical_package_id");
}

TEST_F(TserverLanesTest, ForcedLanes) {
  ASSERT_NO_FATALS(UseTwoDomainTree());
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = 4;
  auto topology = ASSERT_RESULT(ComputeTserverLaneTopology());
  ASSERT_EQ(Masks(topology),
            (std::vector<std::string>{"0-3,8-11", "0-3,8-11", "4-7,12-15", "4-7,12-15"}));
}

// With the default lanes=1 nothing is read: neither sysfs nor the cpuset (an unparseable
// cpuset would fail discovery, as the lanes=0 case shows).
TEST_F(TserverLanesTest, OneLaneSkipsDiscovery) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_sysfs_root) = GetTestPath("does_not_exist");
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_cpuset) = "not a cpu list";
  ASSERT_EQ(FLAGS_tserver_lanes, 1);
  std::string description;
  auto topology = ASSERT_RESULT(ComputeTserverLaneTopology(&description));
  ASSERT_EQ(topology, LaneTopology());
  ASSERT_EQ(description, "lanes disabled (tserver_lanes=1)");

  TEST_SetLaneTopology scope;
  StringVectorSink sink;
  ScopedRegisterSink registration(&sink);
  InitTserverLaneTopology();
  ASSERT_EQ(GetLaneTopology(), LaneTopology());
  ASSERT_EQ(CountContaining(sink, "Lane topology"), 0);
  ASSERT_EQ(CountContaining(sink, "lanes"), 0);

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = 0;
  ASSERT_NOK(ComputeTserverLaneTopology());
}

// On a host with one domain, auto mode resolves to one inactive lane.
TEST_F(TserverLanesTest, AutoOnOneDomainIsInactive) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_sysfs_root) = GetTestPath("does_not_exist");
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_cpuset) = "0-3";
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = 0;
  std::string description;
  auto topology = ASSERT_RESULT(ComputeTserverLaneTopology(&description));
  ASSERT_EQ(topology, LaneTopology());
  ASSERT_STR_CONTAINS(description, "lanes inactive, one lane");
  ASSERT_STR_CONTAINS(description, "[0-3]");
}

TEST_F(TserverLanesTest, ServiceCpusOutsideCpusetDropped) {
  ASSERT_NO_FATALS(UseTwoDomainTree());
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_service_lane_cpus) = "0,8,100-101";
  StringVectorSink sink;
  ScopedRegisterSink registration(&sink);
  auto topology = ASSERT_RESULT(ComputeTserverLaneTopology());
  ASSERT_EQ(CountContaining(sink, "Ignoring tserver_service_lane_cpus 100-101"), 1);
  ASSERT_EQ(topology.service_mask().ToString(), "0,8");
  ASSERT_EQ(Masks(topology), (std::vector<std::string>{"1-3,9-11", "4-7,12-15"}));
}

TEST_F(TserverLanesTest, RepeatInitWithEqualTopologyIsNoOp) {
  ASSERT_NO_FATALS(UseTwoDomainTree());
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = 0;
  TEST_SetLaneTopology scope;
  InitTserverLaneTopology();
  ASSERT_EQ(GetLaneTopology().num_lanes(), 2);
  const auto* installed = &GetLaneTopology();

  StringVectorSink sink;
  ScopedRegisterSink registration(&sink);
  InitTserverLaneTopology();
  ASSERT_EQ(&GetLaneTopology(), installed);
  ASSERT_EQ(CountContaining(sink, "already set"), 0);
  ASSERT_EQ(CountContaining(sink, "Lane topology: lanes active"), 1);
}

// Discovery errors never fail startup: the tserver runs with lanes off.
TEST_F(TserverLanesTest, DiscoveryErrorRunsInactive) {
  ASSERT_NO_FATALS(UseTwoDomainTree());
  ASSERT_OK(WriteStringToFile(
      Env::Default(), "garbage\n", GetTestPath("sys_cpu") + "/cpu0/cache/index3/level"));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = 0;
  ASSERT_NOK(ComputeTserverLaneTopology());

  TEST_SetLaneTopology scope;
  StringVectorSink sink;
  ScopedRegisterSink registration(&sink);
  InitTserverLaneTopology();
  ASSERT_EQ(GetLaneTopology(), LaneTopology());
  ASSERT_EQ(CountContaining(sink, "CPU topology discovery failed, running with lanes off"), 1);
}

// A tablet server restarted in the same process runs Init again, possibly on a thread already
// confined to the service CPUs. It must compute the installed topology again, not one built from
// that thread's mask. The cpuset comes from the real thread mask here (no TEST cpuset).
TEST_F(TserverLanesTest, RepeatInitFromConfinedThreadIsNoOp) {
  const auto process_cpuset = ThisThreadMask();
  const auto cpus = process_cpuset.ToVector();
  if (cpus.size() < 2) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  // Two sockets over the real CPUs, no cache info.
  std::vector<FakeCpu> fake_cpus;
  for (size_t i = 0; i != cpus.size(); ++i) {
    fake_cpus.push_back(FakeCpu { .cpu = cpus[i], .package = i < cpus.size() / 2 ? 0 : 1 });
  }
  auto root = GetTestPath("sys_cpu");
  ASSERT_OK(WriteFakeSysfsCpuTree(root, fake_cpus));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_sysfs_root) = root;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = 2;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_service_lane_cpus) = std::to_string(cpus.back());

  TEST_SetLaneTopology scope;
  InitTserverLaneTopology();
  const auto* installed = &GetLaneTopology();
  ASSERT_TRUE(installed->active());
  ASSERT_EQ(installed->full_process_cpuset(), process_cpuset);
  ASSERT_NE(installed->service_mask(), process_cpuset);

  StringVectorSink sink;
  ScopedRegisterSink registration(&sink);
  RunOnStdThread([installed] {
    ApplyServiceMaskToCurrentThread();
    ASSERT_EQ(ThisThreadMask(), installed->service_mask());
    auto recomputed = ASSERT_RESULT(ComputeTserverLaneTopology());
    ASSERT_EQ(recomputed.ToString(), installed->ToString());
    ASSERT_EQ(recomputed, *installed);
    InitTserverLaneTopology();
  });
  ASSERT_EQ(&GetLaneTopology(), installed);
  ASSERT_EQ(CountContaining(sink, "already set"), 0);
}

TEST_F(TserverLanesTest, LaneMetricsRegisteredOnlyWhenActive) {
  MetricRegistry registry;
  auto entity = METRIC_ENTITY_server.Instantiate(&registry, "lanes-test");
  std::map<std::string, std::string> descriptions;
  auto lane_metrics = [&entity, &descriptions] {
    std::map<std::string, uint64_t> result;
    for (const auto& [prototype, metric] : entity->TEST_UsageMetricsMap()) {
      std::string name = prototype->name();
      if (name.starts_with("lane_")) {
        EXPECT_EQ(prototype->type(), MetricType::kCounter) << name;
        result[name] = down_cast<FunctionGauge<uint64_t>*>(metric.get())->value();
        descriptions[name] = prototype->description();
      }
    }
    return result;
  };

  {
    TEST_SetLaneTopology scope{LaneTopology()};
    RegisterTserverLaneMetrics(entity);
    ASSERT_TRUE(lane_metrics().empty());
  }

  // The fake topology's CPUs need not exist here, so keep masks off; lanes are still assigned.
  ASSERT_NO_FATALS(UseTwoDomainTree());
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lane_cpu_affinity) = false;
  TEST_SetLaneTopology scope;
  InitTserverLaneTopology();
  ASSERT_TRUE(GetLaneTopology().active());
  RegisterTserverLaneMetrics(entity);
  RegisterTserverLaneMetrics(entity);
  auto before = lane_metrics();
  std::vector<std::string> names;
  for (const auto& [name, value] : before) {
    names.push_back(name);
  }
  ASSERT_EQ(names, (std::vector<std::string>{
      "lane_crossings_callback", "lane_crossings_other", "lane_crossings_reactor",
      "lane_crossings_rpc_handler", "lane_crossings_tablet_pipeline",
      "lane_entries_from_unlaned"}));

  RunOnStdThread([] {
    ApplyLaneToCurrentThread(0);
    CountLaneHandoffToThisThread(1, LaneCrossingCategory::kRpcHandler);
    CountLaneHandoffToThisThread(kNoLane, LaneCrossingCategory::kRpcHandler);
  });
  auto after = lane_metrics();
  ASSERT_EQ(after["lane_crossings_rpc_handler"], before["lane_crossings_rpc_handler"] + 1);
  ASSERT_EQ(after["lane_entries_from_unlaned"], before["lane_entries_from_unlaned"] + 1);
  ASSERT_EQ(after["lane_crossings_other"], before["lane_crossings_other"]);

  // Work an unlaned thread hands to a laned reactor is not counted, as the help text says.
  RunOnStdThread([] {
    ApplyServiceMaskToCurrentThread();
    CountLaneHandoffFromThisThread(1, LaneCrossingCategory::kReactor);
  });
  ASSERT_EQ(lane_metrics(), after);
  ASSERT_EQ(descriptions["lane_entries_from_unlaned"],
            "Tasks handed to a laned thread pool, strand or task stream by a thread that belongs "
            "to no lane. Work handed to reactors by threads that belong to no lane is not "
            "counted.");
}

// The tablet pipeline flags accept only their documented values and describe them.
TEST_F(TserverLanesTest, PipelineFlagValidation) {
  ASSERT_OK(flags_internal::ValidateFlagValue("tserver_lane_background", "lane"));
  ASSERT_OK(flags_internal::ValidateFlagValue("tserver_lane_background", "service"));
  for (auto value : {"", "Lane", "none"}) {
    ASSERT_NOK(flags_internal::ValidateFlagValue("tserver_lane_background", value));
  }
  for (auto value : {"0", "1", "1000"}) {
    ASSERT_OK(flags_internal::ValidateFlagValue("log_appender_lane_mode_max_wait_ms", value));
  }
  ASSERT_NOK(flags_internal::ValidateFlagValue("log_appender_lane_mode_max_wait_ms", "-1"));

  gflags::CommandLineFlagInfo info;
  ASSERT_TRUE(gflags::GetCommandLineFlagInfo("tserver_lane_background", &info));
  ASSERT_EQ(info.default_value, "lane");
  ASSERT_EQ(info.description,
            "Where a tablet's RocksDB flushes and compactions run when lanes are enabled: 'lane' "
            "(default) runs each task on the CPUs of the tablet's lane, 'service' on the CPUs of "
            "threads that belong to no lane. No effect when lanes are disabled (tserver_lanes=1).");
  ASSERT_TRUE(gflags::GetCommandLineFlagInfo("log_appender_lane_mode_max_wait_ms", &info));
  ASSERT_EQ(info.default_value, "1");
  ASSERT_EQ(info.description,
            "When CPU lanes are enabled, maximum time in ms a tablet's log appender waits for "
            "more entries after its queue empties before releasing its append thread; replaces "
            "taskstream_queue_max_wait_ms for the log appender. 0 releases the thread as soon as "
            "the queue is empty. No effect when lanes are disabled.");
}

// A tablet's lane is LaneOf(tablet_id) in lane mode and kNoLane otherwise; its background tasks
// follow it unless tserver_lane_background=service.
TEST_F(TserverLanesTest, TabletAndBackgroundLanes) {
  const std::vector<std::string> tablet_ids = {"t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7"};
  {
    TEST_SetLaneTopology scope{LaneTopology()};
    for (const auto& tablet_id : tablet_ids) {
      ASSERT_EQ(TserverTabletLane(tablet_id), kNoLane);
    }
  }
  CpuSet cpus;
  std::vector<CpuSet> domains(4);
  for (size_t cpu = 0; cpu != 8; ++cpu) {
    cpus.Set(cpu);
    domains[cpu / 2].Set(cpu);
  }
  TEST_SetLaneTopology scope(LaneTopology::Build(LaneTopologyOptions {
    .domains = domains,
    .process_cpuset = cpus,
    .requested_lanes = 4,
    .affinity_enabled = false,
  }));
  ASSERT_TRUE(LaneModeActive());
  std::set<LaneId> seen;
  for (const auto& tablet_id : tablet_ids) {
    const auto lane = TserverTabletLane(tablet_id);
    ASSERT_EQ(lane, LaneOf(tablet_id, 4));
    seen.insert(lane);
    ASSERT_EQ(TserverBackgroundTaskLane(lane), lane);
  }
  ASSERT_GT(seen.size(), 1);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lane_background) = "service";
  for (LaneId lane = 0; lane != 4; ++lane) {
    ASSERT_EQ(TserverBackgroundTaskLane(lane), kNoLane);
  }
}

} // namespace yb::tserver
