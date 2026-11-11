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

#include <gtest/gtest.h>

#include "yb/gutil/casts.h"

#include "yb/tserver/tserver_lanes.h"

#include "yb/util/cpu_topology_test_util.h"
#include "yb/util/env.h"
#include "yb/util/flags.h"
#include "yb/util/logging_test_util.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"

DECLARE_string(TEST_cpu_topology_cpuset);
DECLARE_string(TEST_cpu_topology_sysfs_root);
DECLARE_bool(tserver_lane_cpu_affinity);
DECLARE_string(tserver_lane_domain);
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

} // namespace yb::tserver
