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

// Tablet pipelines and Raft traffic of tablet servers with CPU lanes.

#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"

#include "yb/consensus/log.h"
#include "yb/consensus/raft_consensus.h"

#include "yb/integration-tests/cluster_verifier.h"
#include "yb/integration-tests/external_mini_cluster.h"
#include "yb/integration-tests/external_mini_cluster-itest-base.h"
#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/tablet-split-itest-base.h"
#include "yb/integration-tests/test_workload.h"

#include "yb/master/master.h"
#include "yb/master/mini_master.h"

#include "yb/rpc/connection_id.h"
#include "yb/rpc/messenger.h"
#include "yb/rpc/reactor.h"

#include "yb/tablet/preparer.h"
#include "yb/tablet/tablet_peer.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"
#include "yb/tserver/ts_tablet_manager.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/cpu_topology.h"
#include "yb/util/cpu_topology_test_util.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/test_macros.h"
#include "yb/util/tsan_util.h"

DECLARE_string(TEST_cpu_topology_cpuset);
DECLARE_string(TEST_cpu_topology_sysfs_root);
DECLARE_int32(tserver_lanes);

namespace yb {

using namespace std::literals;

namespace {

using Submitter = std::function<Status(std::function<void()>)>;

Result<LaneId> LaneOfTask(const Submitter& submit) {
  CountDownLatch latch(1);
  LaneId lane = kNoLane;
  RETURN_NOT_OK(submit([&lane, &latch] {
    lane = ThisThreadLane();
    latch.CountDown();
  }));
  SCHECK(latch.WaitFor(10s * kTimeMultiplier), TimedOut, "Task did not run");
  return lane;
}

// Lanes of the threads that run the peer's prepare, Raft, notification (and apply) and log append
// work.
Result<std::map<std::string, LaneId>> PipelineLanes(tablet::TabletPeer* peer) {
  auto consensus = VERIFY_RESULT(peer->GetRaftConsensus());
  auto* log = peer->log();
  SCHECK_NOTNULL(log);
  std::map<std::string, Submitter> stages = {
    {"prepare", [peer](auto f) { return peer->DEBUG_GetPreparer()->PoolToken()->SubmitFunc(f); }},
    {"raft", [consensus](auto f) { return consensus->TEST_SubmitToRaftPool(f); }},
    {"notifications", [consensus](auto f) {
      consensus->TEST_EnqueueNotification(f);
      return Status::OK();
    }},
    {"append", [log](auto f) { return log->TEST_SubmitFuncToAppendToken(f); }},
  };
  std::map<std::string, LaneId> result;
  for (const auto& [stage, submit] : stages) {
    result[stage] = VERIFY_RESULT_PREPEND(LaneOfTask(submit), stage);
  }
  return result;
}

// Client connection ids of the messenger, each with the lane of the reactor that owns it.
Result<std::vector<std::pair<rpc::ConnectionId, LaneId>>> ClientConnections(
    rpc::Messenger* messenger) {
  std::vector<std::pair<rpc::ConnectionId, LaneId>> result;
  for (size_t i = 0; i != messenger->num_reactors(); ++i) {
    auto* reactor = messenger->TEST_reactor(i);
    for (const auto& id : VERIFY_RESULT(reactor->TEST_ClientConnectionIds())) {
      result.emplace_back(id, reactor->lane());
    }
  }
  return result;
}

} // namespace

// MiniCluster of three tablet servers with forced 4 lanes over a fake topology of two 2-CPU L3
// domains laid over CPUs 0-3. Tables are not transactional, so the only outbound calls tablet
// servers make from lane threads are Raft calls.
class TserverLanesITest : public TabletSplitITest {
 protected:
  void SetUp() override {
    auto mask = ThisThreadMask();
    for (size_t cpu = 0; cpu != 4; ++cpu) {
      if (!mask.Test(cpu)) {
        GTEST_SKIP() << "Needs CPUs 0-3, have " << mask.ToString();
      }
    }
    auto root = GetTestPath("sys_cpu");
    ASSERT_OK(WriteFakeSysfsCpuTree(
        root, MakeFakeCpus({.sockets = 1, .l3_per_socket = 2, .cores_per_l3 = 2, .smt = 1})));
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_sysfs_root) = root;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_cpuset) = "0-3";
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = 4;
    SetIsolationLevel(IsolationLevel::NON_TRANSACTIONAL);
    // Masters replicate their sys catalog with the same consensus code, but send no lane hints.
    mini_cluster_opt_.num_masters = 3;
    // Unset, so that the tablet servers install their topology; restored after they stop.
    lane_scope_.emplace();
    TabletSplitITest::SetUp();
    ASSERT_TRUE(LaneModeActive());
    ASSERT_EQ(GetLaneTopology().num_lanes(), 4);
  }

  void DoTearDown() override {
    TabletSplitITest::DoTearDown();
    lane_scope_.reset();
  }

  tserver::MiniTabletServer* ServerOf(const tablet::TabletPeer& peer) {
    for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
      auto* ts = cluster_->mini_tablet_server(i);
      if (ts->server()->permanent_uuid() == peer.permanent_uuid()) {
        return ts;
      }
    }
    return nullptr;
  }

  // Every replica of the table runs in LaneOf(tablet_id), pipeline included.
  void CheckReplicaLanes() {
    auto peers = ListTableActiveTabletPeers(cluster_.get(), table_->id());
    ASSERT_FALSE(peers.empty());
    for (const auto& peer : peers) {
      SCOPED_TRACE(peer->LogPrefix());
      const auto lane = LaneOf(peer->tablet_id());
      ASSERT_EQ(peer->lane(), lane);
      for (const auto& [stage, stage_lane] : ASSERT_RESULT(PipelineLanes(peer.get()))) {
        ASSERT_EQ(stage_lane, lane) << stage;
      }
    }
  }

  Status WaitReplicated() {
    return WaitFor([this]() -> Result<bool> {
      std::map<TabletId, std::set<int64_t>> committed;
      for (const auto& peer : ListTableActiveTabletPeers(cluster_.get(), table_->id())) {
        auto consensus = VERIFY_RESULT(peer->GetRaftConsensus());
        committed[peer->tablet_id()].insert(consensus->GetLastCommittedOpId().index);
      }
      for (const auto& [tablet_id, indexes] : committed) {
        if (indexes.size() != 1) {
          return false;
        }
      }
      return true;
    }, 30s * kTimeMultiplier, "Replicas agree on the committed op id");
  }

 private:
  std::optional<TEST_SetLaneTopology> lane_scope_;
};

// RF3 writes replicate; each replica runs in its tablet's lane; leaders send Raft calls on
// connections of that lane owned by that lane's reactors, and their callbacks run in it; the
// in-process master, whose messenger is unlaned, keys no connection by lane.
TEST_F(TserverLanesITest, RaftStaysInTabletLane) {
  SetNumTablets(4);
  CreateTable();
  ASSERT_OK(WaitAllReplicasReady(cluster_.get(), table_->id(), 30s * kTimeMultiplier));
  ASSERT_OK(WriteRows(&table_, 100, 1));
  ASSERT_OK(WaitReplicated());
  ASSERT_NO_FATALS(CheckReplicaLanes());

  auto session = NewSession();
  constexpr int kWrites = 50;
  auto before = LaneHandoffCounts::Now();
  for (int key = 1000; key != 1000 + kWrites; ++key) {
    ASSERT_RESULT(WriteRow(session, key, key));
  }
  auto delta = LaneHandoffCounts::Now() - before;
  LOG(INFO) << "Lane handoffs during " << kWrites << " writes: " << delta.ToString();
  // A Raft callback runs in the pool of the lane that sent the call, which is the lane of the
  // reactor that reads the response.
  ASSERT_EQ(delta.crossing(LaneCrossingCategory::kCallback), 0) << delta.ToString();
  // Inbound calls are handled in the lane of whichever reactor read them, so each write crosses
  // into its tablet's pipeline at most once on the leader (the preparer) and at most twice on each
  // follower (the log append and the other tasks its UpdateConsensus handler submits). Work within
  // the pipeline crosses no lane.
  ASSERT_LE(delta.crossing(LaneCrossingCategory::kTabletPipeline), kWrites * (1 + 2 * 2))
      << delta.ToString();
  ASSERT_OK(CheckRowsCount(100 + kWrites));
  ASSERT_OK(WaitReplicated());

  // Peers reach each other by broadcast address, so tell servers apart by port.
  std::map<std::string, uint16_t> rpc_ports;
  for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
    auto* ts = cluster_->mini_tablet_server(i);
    rpc_ports[ts->server()->permanent_uuid()] = ts->bound_rpc_addr().port();
  }
  auto peers = ListTableActiveTabletPeers(cluster_.get(), table_->id());
  size_t leaders = 0;
  for (const auto& leader : peers) {
    if (!leader->IsLeaderAndReady()) {
      continue;
    }
    ++leaders;
    SCOPED_TRACE(leader->LogPrefix());
    const auto lane = LaneOf(leader->tablet_id());
    auto connections = ASSERT_RESULT(ClientConnections(ServerOf(*leader)->server()->messenger()));
    for (const auto& [id, reactor_lane] : connections) {
      if (id.lane() != kNoLane) {
        ASSERT_EQ(id.lane(), reactor_lane) << id.ToString();
      }
    }
    for (const auto& follower : peers) {
      if (follower->tablet_id() != leader->tablet_id() || follower == leader) {
        continue;
      }
      const auto port = rpc_ports[follower->permanent_uuid()];
      bool found = false;
      for (const auto& [id, reactor_lane] : connections) {
        found = found || (id.remote().port() == port && id.lane() == lane);
      }
      ASSERT_TRUE(found) << "No lane " << static_cast<int>(lane) << " connection to port " << port
                         << ": " << AsString(connections);
    }
  }
  ASSERT_EQ(leaders, 4);

  size_t master_connections = 0;
  for (size_t i = 0; i != cluster_->num_masters(); ++i) {
    auto* messenger = cluster_->mini_master(i)->master()->messenger();
    ASSERT_FALSE(messenger->laned());
    for (const auto& [id, reactor_lane] : ASSERT_RESULT(ClientConnections(messenger))) {
      ASSERT_EQ(id.lane(), kNoLane) << id.ToString();
      ++master_connections;
    }
  }
  ASSERT_GT(master_connections, 0);
}

// Split children and a remote-bootstrapped replica run in the lanes of their own tablet ids.
TEST_F(TserverLanesITest, SplitChildrenAndRemoteBootstrapUseTheirLanes) {
  const auto parent_id = ASSERT_RESULT(CreateSingleTabletAndSplit(kDefaultNumRows));
  ASSERT_OK(WaitForTabletSplitCompletion(/* expected_non_split_tablets= */ 2));
  std::set<TabletId> children;
  for (const auto& peer : ListTableActiveTabletPeers(cluster_.get(), table_->id())) {
    children.insert(peer->tablet_id());
  }
  ASSERT_EQ(children.size(), 2);
  ASSERT_FALSE(children.contains(parent_id));
  ASSERT_NO_FATALS(CheckReplicaLanes());

  const auto& child_id = *children.begin();
  auto leader = ASSERT_RESULT(GetLeaderPeerForTablet(cluster_.get(), child_id));
  tserver::MiniTabletServer* follower_ts = nullptr;
  for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
    auto* ts = cluster_->mini_tablet_server(i);
    if (ts->server()->permanent_uuid() != leader->permanent_uuid()) {
      follower_ts = ts;
      break;
    }
  }
  ASSERT_NE(follower_ts, nullptr);
  auto* tablet_manager = follower_ts->server()->tablet_manager();
  auto old_peer = ASSERT_RESULT(tablet_manager->GetTablet(child_id));
  std::optional<tserver::TabletServerErrorPB::Code> error_code;
  ASSERT_OK(tablet_manager->DeleteTablet(
      child_id, tablet::TABLET_DATA_TOMBSTONED, tablet::ShouldAbortActiveTransactions::kFalse,
      /* cas_config_opid_index_less_or_equal= */ std::nullopt, /* hide_only= */ false,
      /* keep_data= */ false, &error_code));
  tablet::TabletPeerPtr new_peer;
  ASSERT_OK(WaitFor([tablet_manager, &child_id, &old_peer, &new_peer]() -> Result<bool> {
    auto peer = tablet_manager->GetTablet(child_id);
    if (!peer.ok() || *peer == old_peer || !(*peer)->IsRunning()) {
      return false;
    }
    new_peer = *peer;
    return true;
  }, 60s * kTimeMultiplier, "Remote bootstrap of the tombstoned replica"));
  ASSERT_EQ(new_peer->lane(), LaneOf(child_id));
  for (const auto& [stage, stage_lane] : ASSERT_RESULT(PipelineLanes(new_peer.get()))) {
    ASSERT_EQ(stage_lane, new_peer->lane()) << stage;
  }
  ASSERT_OK(CheckRowsCount(kDefaultNumRows));
}

// Tablet servers with different lane counts replicate to each other: lanes cost locality only.
class TserverMixedLanesITest : public ExternalMiniClusterITestBase {};

TEST_F(TserverMixedLanesITest, WritesAndReadsWithMixedLaneCounts) {
  ASSERT_NO_FATALS(StartCluster({"--tserver_lanes=4"}));
  auto* ts = cluster_->tablet_server(0);
  ts->Shutdown();
  ts->mutable_flags()->push_back("--tserver_lanes=1");
  ASSERT_OK(ts->Restart());
  ASSERT_OK(cluster_->WaitForTabletServerCount(3, 30s * kTimeMultiplier));
  for (size_t i = 0; i != 3; ++i) {
    auto lanes = ASSERT_RESULT(cluster_->GetFlag(cluster_->tablet_server(i), "tserver_lanes"));
    ASSERT_EQ(lanes, i == 0 ? "1" : "4");
  }

  TestYcqlWorkload workload(cluster_.get());
  workload.set_num_tablets(3);
  workload.set_num_write_threads(2);
  workload.Setup();
  workload.Start();
  workload.WaitInserted(500);
  workload.StopAndJoin();

  ClusterVerifier verifier(cluster_.get());
  ASSERT_NO_FATALS(verifier.CheckCluster());
  ASSERT_NO_FATALS(verifier.CheckRowCount(
      workload.table_name(), ClusterVerifier::AT_LEAST, workload.rows_inserted()));
}

} // namespace yb
