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

// Tablet pipelines, Raft traffic and embedded-client callbacks of tablet servers with CPU lanes.

#include <array>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "yb/client/client.h"
#include "yb/client/ql-dml-test-base.h"
#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"

#include "yb/common/ql_protocol_util.h"

#include "yb/consensus/log.h"
#include "yb/consensus/raft_consensus.h"

#include "yb/integration-tests/cluster_verifier.h"
#include "yb/integration-tests/external_mini_cluster.h"
#include "yb/integration-tests/external_mini_cluster-itest-base.h"
#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/path_handlers_util.h"
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
#include "yb/util/json_document.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/metrics.h"
#include "yb/util/test_macros.h"
#include "yb/util/tsan_util.h"

DECLARE_bool(enable_rpc_lane_preamble);
DECLARE_int32(force_lookup_cache_refresh_secs);
DECLARE_int32(master_inject_latency_on_tablet_lookups_ms);
DECLARE_string(TEST_cpu_topology_cpuset);
DECLARE_string(TEST_cpu_topology_sysfs_root);
DECLARE_int32(tserver_lanes);

METRIC_DECLARE_entity(server);
METRIC_DECLARE_counter(rpc_lane_preamble_absent);
METRIC_DECLARE_counter(rpc_lane_preamble_bound);
METRIC_DECLARE_counter(rpc_lane_preamble_peek_timeouts);
METRIC_DECLARE_counter(rpc_lane_preamble_stripped_late);

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

// Lanes of the threads that run the peer's prepare, Raft, notification (and leader-side apply) and
// log append work.
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

struct LaneFlush {
  CountDownLatch done{1};
  LaneId start_lane = kNoLane;
  LaneId callback_lane = kNoLane;
  Status status;
};

// Flushes session from a worker of lane's normal handler pool and records the lanes of the thread
// that flushed and of the thread that ran the flush callback.
Status StartFlushInLane(
    rpc::Messenger* messenger, LaneId lane, const client::YBSessionPtr& session,
    LaneFlush* result) {
  SCHECK(messenger->ThreadPool(rpc::ServicePriority::kNormal, lane).EnqueueFunctor(
      [session, result] {
        result->start_lane = ThisThreadLane();
        session->FlushAsync([result](client::FlushStatus* status) {
          result->callback_lane = ThisThreadLane();
          result->status = status->status;
          result->done.CountDown();
        });
      }), IllegalState, "Failed to enqueue flush");
  return Status::OK();
}

} // namespace

// MiniCluster of three tablet servers with forced 4 lanes over a fake topology of two 2-CPU L3
// domains laid over CPUs 0-3. Tables are not transactional, so the test client's writes make
// tablet servers send only Raft calls from lane threads.
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

  // Sum of a preamble counter over the tablet servers; a server that did not instantiate it adds 0.
  int64_t PreambleCounter(const CounterPrototype& prototype) {
    int64_t result = 0;
    for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
      auto* entity = cluster_->mini_tablet_server(i)->server()->metric_entity().get();
      auto counter = entity->FindOrNull<Counter>(prototype);
      result += counter ? counter->value() : 0;
    }
    return result;
  }

  void TestRaftStaysInTabletLane(bool preamble);

 private:
  std::optional<TEST_SetLaneTopology> lane_scope_;
};

// The same cluster with the lane preamble AutoFlag off from the start, so that no inbound
// connection is bound to a lane.
class TserverLanesNoPreambleITest : public TserverLanesITest {
 protected:
  void SetUp() override {
    // Set as an override, which AutoFlag promotion at cluster start leaves alone.
    ASSERT_OK(SET_FLAG(enable_rpc_lane_preamble, false));
    TserverLanesITest::SetUp();
  }
};

// RF3 writes replicate; each replica runs in its tablet's lane; leaders send Raft calls on
// connections of that lane owned by that lane's reactors, and the reactors that read the responses
// are in the lane that issued the calls, so no callback crosses; the in-process master, whose
// messenger is unlaned, keys no connection by lane. With the preamble, followers handle
// UpdateConsensus in the tablet's lane.
void TserverLanesITest::TestRaftStaysInTabletLane(bool preamble) {
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
  // A Raft callback runs in the pool of the lane that sent the call. A leader sends
  // UpdateConsensus only from lane(T) threads (the Raft pool token, whose lane is probed above,
  // and the callback that sends the next request), so that lane is lane(T), as is the reactor that
  // reads the response, and no callback crosses. Without the hint the response reactor is in a
  // random lane and callbacks cross.
  ASSERT_EQ(delta.crossing(LaneCrossingCategory::kCallback), 0) << delta.ToString();
  // The test client's messenger is unlaned, so its writes arrive on connections not bound to a lane
  // and are handled in the lane of whichever reactor read them: each write crosses into its
  // tablet's pipeline at most once on the leader (the preparer). Work within the pipeline crosses
  // no lane.
  // With the preamble, the leader's UpdateConsensus connection to a follower, keyed by lane(T), is
  // bound to a lane(T) reactor of the follower, so the follower's handler, and the follower-side
  // prepare and apply it runs inline, are in lane(T) and cross nothing. The slack covers pipeline
  // work started by calls of unbound connections other than the writes, such as the master's.
  // Without the preamble the follower handles UpdateConsensus in the lane of whichever reactor read
  // it, and crosses into the pipeline for the log append and the other tasks it submits, at most
  // twice per write on each of two followers.
  constexpr int kSlack = 5;
  if (preamble) {
    ASSERT_LE(delta.crossing(LaneCrossingCategory::kTabletPipeline), kWrites + kSlack)
        << delta.ToString();
    ASSERT_GT(PreambleCounter(METRIC_rpc_lane_preamble_bound), 0);
  } else {
    // A follower handles a call in lane(T) with probability 1/L, so the followers' crossings,
    // about 2 x 50 x 3/4 at least, put the count well above the bound with the preamble.
    ASSERT_GT(delta.crossing(LaneCrossingCategory::kTabletPipeline), kWrites + kSlack)
        << delta.ToString();
    ASSERT_LE(delta.crossing(LaneCrossingCategory::kTabletPipeline), kWrites * (1 + 2 * 2))
        << delta.ToString();
    ASSERT_EQ(PreambleCounter(METRIC_rpc_lane_preamble_bound), 0);
  }
  // Masters' and the test client's connections carry no preamble; none arrives late.
  ASSERT_GT(PreambleCounter(METRIC_rpc_lane_preamble_absent), 0);
  ASSERT_EQ(PreambleCounter(METRIC_rpc_lane_preamble_peek_timeouts), 0);
  ASSERT_EQ(PreambleCounter(METRIC_rpc_lane_preamble_stripped_late), 0);
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

TEST_F(TserverLanesITest, RaftStaysInTabletLane) {
  TestRaftStaysInTabletLane(/* preamble= */ true);
}

TEST_F(TserverLanesNoPreambleITest, RaftStaysInTabletLane) {
  TestRaftStaysInTabletLane(/* preamble= */ false);
}

// The server client's tablet RPCs call back in the lane that flushed them even when the tablet
// lookup they waited for was started by, and completed in, another lane: the master delays lookups
// so that lane 1's lookup joins the one lane 0 started, and lane 0's thread completes both. Without
// the carried issuing lane, lane 1's RPC is sent from lane 0's thread and calls back in lane 0.
TEST_F(TserverLanesITest, CallbackInIssuingLaneAfterSharedLookup) {
  SetNumTablets(1);
  CreateTable();
  ASSERT_OK(WaitAllReplicasReady(cluster_.get(), table_->id(), 30s * kTimeMultiplier));
  auto* server = cluster_->mini_tablet_server(0)->server();
  auto* client = server->client();
  ASSERT_TRUE(client->messenger()->laned());
  client::TableHandle table;
  ASSERT_OK(table.Open(table_->name(), client));
  static constexpr int32_t kLookupLatencyMs = 1000;
  static constexpr auto kSecondStartDelay = 200ms;

  auto run_pair = [client, server](
      const std::function<void(const client::YBSessionPtr&, int32_t)>& apply) -> Status {
    std::array<LaneFlush, 2> flushes;
    std::array<client::YBSessionPtr, 2> sessions;
    for (LaneId lane = 0; lane != 2; ++lane) {
      sessions[lane] = client->NewSession(60s * kTimeMultiplier);
      apply(sessions[lane], static_cast<int32_t>(lane) + 1);
    }
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_master_inject_latency_on_tablet_lookups_ms) =
        kLookupLatencyMs;
    RETURN_NOT_OK(StartFlushInLane(server->messenger(), 0, sessions[0], &flushes[0]));
    std::this_thread::sleep_for(kSecondStartDelay);
    RETURN_NOT_OK(StartFlushInLane(server->messenger(), 1, sessions[1], &flushes[1]));
    for (auto& flush : flushes) {
      SCHECK(flush.done.WaitFor(60s * kTimeMultiplier), TimedOut, "Flush did not finish");
    }
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_master_inject_latency_on_tablet_lookups_ms) = 0;
    for (LaneId lane = 0; lane != 2; ++lane) {
      RETURN_NOT_OK(flushes[lane].status);
      SCHECK_EQ(flushes[lane].start_lane, lane, IllegalState, "Flush started in another lane");
      SCHECK_EQ(
          flushes[lane].callback_lane, lane, IllegalState,
          Format("Flush started in lane $0 called back in lane $1", lane,
                 flushes[lane].callback_lane));
    }
    return Status::OK();
  };

  // Writes with a cold meta cache: lane 1's batcher waits for lane 0's lookup by key and creates
  // its RPC on the thread that completes it.
  ASSERT_OK(run_pair([&table](const client::YBSessionPtr& session, int32_t key) {
    CHECK_OK(client::kv_table_test::WriteRow(
        &table, session, key, key, client::WriteOpType::INSERT, client::Flush::kFalse));
  }));

  // Consistent-prefix reads that refresh the cached locations: lane 1's invoker waits for lane 0's
  // lookup by id and resends from the thread that completes it.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_force_lookup_cache_refresh_secs) = 1;
  std::this_thread::sleep_for(1500ms);
  ASSERT_OK(run_pair([&table](const client::YBSessionPtr& session, int32_t key) {
    auto op = table.NewReadOp(session->arena());
    QLAddInt32HashValue(op->mutable_request(), key);
    table.AddColumns({client::kv_table_test::kValueColumn}, op->mutable_request());
    op->set_yb_consistency_level(YBConsistencyLevel::CONSISTENT_PREFIX);
    session->Apply(op);
  }));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_force_lookup_cache_refresh_secs) = 0;
}

namespace {

Result<std::set<std::string>> JsonKeys(const JsonValue& value) {
  std::set<std::string> result;
  for (const auto& [key, _] : VERIFY_RESULT(value.GetObject())) {
    result.emplace(key);
  }
  return result;
}

const std::map<LaneCrossingCategory, std::string> kCrossingCategoryKeys = {
    {LaneCrossingCategory::kReactor, "reactor"},
    {LaneCrossingCategory::kRpcHandler, "rpc_handler"},
    {LaneCrossingCategory::kTabletPipeline, "tablet_pipeline"},
    {LaneCrossingCategory::kCallback, "callback"},
    {LaneCrossingCategory::kOther, "other"},
};

const std::set<std::string> kCrossingKeys = {
    "reactor", "rpc_handler", "tablet_pipeline", "callback", "other", "entries_from_unlaned"};

const std::set<std::string> kLaneRowKeys = {
    "tablet_replicas", "tablet_leaders", "pg_sessions", "reactors", "client_connections",
    "server_connections", "handler_threads", "pipeline_threads", "exchange_threads",
    "queued_calls"};

} // namespace

// The /lanes page and /api/v1/lanes show the topology the server installed and, per lane, the
// tablets it hosts and leads, its reactors and live threads, and the crossing and preamble
// counters, without creating pools.
TEST_F(TserverLanesITest, LanesPage) {
  SetNumTablets(8);
  CreateTable();
  ASSERT_OK(WaitAllReplicasReady(cluster_.get(), table_->id(), 30s * kTimeMultiplier));
  ASSERT_OK(WriteRows(&table_, 100, 1));
  ASSERT_OK(WaitReplicated());
  const auto& topology = GetLaneTopology();
  auto* ts = cluster_->mini_tablet_server(0);
  auto* messenger = ts->server()->messenger();
  const auto url = Format("http://$0", ts->bound_http_addr());
  std::vector<size_t> tagged_pools;
  for (LaneId lane = 0; lane != 4; ++lane) {
    tagged_pools.push_back(messenger->TEST_ActiveTaggedPools(lane));
  }

  // Tablet counts as the tablet manager reports them; leadership can move while the page is read,
  // so both are read until they agree.
  using LaneCounts = std::vector<std::pair<int64_t, int64_t>>;
  auto expected_counts = [ts] {
    LaneCounts result(4);
    for (const auto& peer : ts->server()->tablet_manager()->GetTabletPeers()) {
      auto& counts = result[LaneOf(peer->tablet_id())];
      ++counts.first;
      counts.second += peer->IsLeaderAndReady();
    }
    return result;
  };
  std::string json;
  LaneCounts page_counts;
  LaneHandoffCounts crossings_before;
  LaneHandoffCounts crossings_after;
  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    const auto expected = expected_counts();
    crossings_before = LaneHandoffCounts::Now();
    faststring body;
    RETURN_NOT_OK(integration_tests::path_handlers_util::GetUrl(url + "/api/v1/lanes", &body));
    crossings_after = LaneHandoffCounts::Now();
    json = body.ToString();
    JsonDocument doc;
    auto root = VERIFY_RESULT(doc.Parse(json));
    page_counts.clear();
    for (const auto& lane : VERIFY_RESULT(root["lanes"].GetArray())) {
      page_counts.emplace_back(
          VERIFY_RESULT(lane["tablet_replicas"].GetInt64()),
          VERIFY_RESULT(lane["tablet_leaders"].GetInt64()));
    }
    return page_counts == expected && expected == expected_counts();
  }, 30s * kTimeMultiplier, "Page tablet counts match the tablet manager"));
  LOG(INFO) << "/api/v1/lanes: " << json;

  JsonDocument doc;
  auto root = ASSERT_RESULT(doc.Parse(json));
  ASSERT_EQ(ASSERT_RESULT(JsonKeys(root)), (std::set<std::string>{
      "lanes_active", "topology", "num_lanes", "cpu_affinity", "service_cpus", "process_cpus",
      "domains", "lanes", "unlaned", "crossings", "service_queue_lanes_over_share", "preamble"}));
  ASSERT_TRUE(ASSERT_RESULT(root["lanes_active"].GetBool()));
  ASSERT_TRUE(ASSERT_RESULT(root["topology"].GetString()).starts_with(
      "lanes active (tserver_lanes=4), 2 CPU domain(s) from "))
      << ASSERT_RESULT(root["topology"].GetString());
  ASSERT_EQ(ASSERT_RESULT(root["num_lanes"].GetUint64()), 4);
  ASSERT_TRUE(ASSERT_RESULT(root["cpu_affinity"].GetBool()));
  ASSERT_EQ(ASSERT_RESULT(root["service_cpus"].GetString()), topology.service_mask().ToString());
  ASSERT_EQ(ASSERT_RESULT(root["process_cpus"].GetString()), "0-3");
  std::vector<std::string> domains;
  for (const auto& domain : ASSERT_RESULT(root["domains"].GetArray())) {
    domains.push_back(ASSERT_RESULT(domain.GetString()));
  }
  ASSERT_EQ(domains, (std::vector<std::string>{"0-1", "2-3"}));

  auto lanes = ASSERT_RESULT(root["lanes"].GetArray());
  ASSERT_EQ(lanes.size(), 4);
  auto lane_keys = kLaneRowKeys;
  lane_keys.insert({"lane", "cpus", "crossings_counted"});
  std::array<uint64_t, kLaneCrossingCategoryMapSize> lane_crossing_sums = {};
  int64_t replicas = 0;
  for (LaneId lane = 0; lane != 4; ++lane) {
    SCOPED_TRACE(Format("lane $0", lane));
    auto row = lanes[lane];
    ASSERT_EQ(ASSERT_RESULT(JsonKeys(row)), lane_keys);
    ASSERT_EQ(ASSERT_RESULT(row["lane"].GetUint64()), lane);
    ASSERT_EQ(ASSERT_RESULT(row["cpus"].GetString()), topology.lane_mask(lane).ToString());
    replicas += page_counts[lane].first;
    ASSERT_EQ(ASSERT_RESULT(row["reactors"].GetUint64()), messenger->ReactorsOfLane(lane).size());
    ASSERT_GT(ASSERT_RESULT(row["reactors"].GetUint64()), 0);
    // Each of the lane's five pipeline pools keeps one persistent worker.
    ASSERT_GE(ASSERT_RESULT(row["pipeline_threads"].GetUint64()), 5);
    ASSERT_EQ(ASSERT_RESULT(row["pg_sessions"].GetUint64()), 0);
    ASSERT_EQ(ASSERT_RESULT(row["exchange_threads"].GetUint64()), 0);
    auto counted = row["crossings_counted"];
    ASSERT_EQ(ASSERT_RESULT(JsonKeys(counted)), kCrossingKeys);
    for (auto category : LaneCrossingCategoryList()) {
      lane_crossing_sums[static_cast<size_t>(category)] +=
          ASSERT_RESULT(counted[kCrossingCategoryKeys.at(category)].GetUint64());
    }
  }
  ASSERT_EQ(replicas, ts->server()->tablet_manager()->GetTabletPeers().size());
  // Raft replicates on lane-keyed connections, owned and served by lane reactors and handled by
  // lane handler pools.
  uint64_t lane_client_connections = 0;
  uint64_t lane_server_connections = 0;
  uint64_t lane_handler_threads = 0;
  for (const auto& row : lanes) {
    lane_client_connections += ASSERT_RESULT(row["client_connections"].GetUint64());
    lane_server_connections += ASSERT_RESULT(row["server_connections"].GetUint64());
    lane_handler_threads += ASSERT_RESULT(row["handler_threads"].GetUint64());
  }
  ASSERT_GT(lane_client_connections, 0);
  ASSERT_GT(lane_server_connections, 0);
  ASSERT_GT(lane_handler_threads, 0);

  auto unlaned = root["unlaned"];
  ASSERT_EQ(ASSERT_RESULT(JsonKeys(unlaned)), kLaneRowKeys);
  ASSERT_EQ(ASSERT_RESULT(unlaned["tablet_replicas"].GetUint64()), 0);
  ASSERT_EQ(ASSERT_RESULT(unlaned["reactors"].GetUint64()), 0);
  ASSERT_GE(ASSERT_RESULT(unlaned["pipeline_threads"].GetUint64()), 5);

  auto crossings = root["crossings"];
  ASSERT_EQ(ASSERT_RESULT(JsonKeys(crossings)), kCrossingKeys);
  for (auto category : LaneCrossingCategoryList()) {
    SCOPED_TRACE(AsString(category));
    const auto total = ASSERT_RESULT(crossings[kCrossingCategoryKeys.at(category)].GetUint64());
    ASSERT_GE(total, crossings_before.crossing(category));
    ASSERT_LE(total, crossings_after.crossing(category));
    ASSERT_LE(lane_crossing_sums[static_cast<size_t>(category)], total);
  }
  ASSERT_EQ(ASSERT_RESULT(JsonKeys(root["preamble"])),
            (std::set<std::string>{"bound", "absent", "peek_timeouts", "stripped_late"}));
  ASSERT_GT(ASSERT_RESULT(root["preamble"]["bound"].GetInt64()), 0);

  for (LaneId lane = 0; lane != 4; ++lane) {
    ASSERT_EQ(messenger->TEST_ActiveTaggedPools(lane), tagged_pools[lane]);
  }

  faststring html;
  ASSERT_OK(integration_tests::path_handlers_util::GetUrl(url + "/lanes", &html));
  for (const auto* text : {
           "<h2>CPU lanes</h2>", "Lane mode is active: lanes active (tserver_lanes=4)",
           "<h3>Topology</h3>", "<h3>Lanes</h3>", "<h3>Lane crossings</h3>",
           "<h3>Lane preamble</h3>", "<td>rpc_lane_preamble_bound</td>"}) {
    ASSERT_STR_CONTAINS(html.ToString(), text);
  }
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

// Tablet servers with different lane counts replicate to each other: lanes cost locality only. The
// lanes=1 server does not peek for lane preambles but drops those of the laned servers' lane-keyed
// connections on its reactors; the laned servers bind each other's.
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

  auto preamble_counter = [this](size_t ts_idx, const CounterPrototype& prototype) {
    return cluster_->tablet_server(ts_idx)->GetMetric<int64>(
        &METRIC_ENTITY_server, "yb.tabletserver", &prototype, "value");
  };
  ASSERT_GT(ASSERT_RESULT(preamble_counter(0, METRIC_rpc_lane_preamble_stripped_late)), 0);
  for (size_t i = 1; i != 3; ++i) {
    ASSERT_GT(ASSERT_RESULT(preamble_counter(i, METRIC_rpc_lane_preamble_bound)), 0);
  }
}

// Promoting and demoting the lane preamble AutoFlag at runtime in a universe whose tablet server 0
// runs with lanes off. Every process starts with the flag explicitly off, which AutoFlag promotion
// at universe creation leaves alone. The flag is read when a connection is created, so after each
// flip the test restarts tablet servers 0 and 1 to make the others open new connections to them.
// Tablet server 1's inbound lane-keyed connections then come only from tablet server 2 (tablet
// server 0 keys none by lane), so its bound count shows tablet server 2's runtime value; tablet
// server 0's late strips show what tablet servers 1 and 2 send.
TEST_F(TserverMixedLanesITest, PreamblePromoteAndDemote) {
  ASSERT_NO_FATALS(StartCluster(
      {"--tserver_lanes=4", "--enable_rpc_lane_preamble=false"},
      {"--enable_rpc_lane_preamble=false"}));
  auto* ts0 = cluster_->tablet_server(0);
  auto* ts1 = cluster_->tablet_server(1);
  ts0->Shutdown();
  ts0->mutable_flags()->push_back("--tserver_lanes=1");
  ASSERT_OK(ts0->Restart());
  ASSERT_OK(cluster_->WaitForTabletServerCount(3, 30s * kTimeMultiplier));
  for (size_t i = 0; i != 3; ++i) {
    ASSERT_EQ(ASSERT_RESULT(cluster_->GetFlag(cluster_->tablet_server(i), "tserver_lanes")),
              i == 0 ? "1" : "4");
  }

  int64_t rows_inserted = 0;
  client::YBTableName table_name;
  auto run_workload = [this, &rows_inserted, &table_name] {
    TestYcqlWorkload workload(cluster_.get());
    workload.set_num_tablets(6);
    workload.set_num_write_threads(2);
    workload.Setup();
    workload.Start();
    workload.WaitInserted(200);
    workload.StopAndJoin();
    rows_inserted += workload.rows_inserted();
    table_name = workload.table_name();
  };
  auto counter = [this](size_t ts_idx, const CounterPrototype& prototype) {
    return cluster_->tablet_server(ts_idx)->GetMetric<int64>(
        &METRIC_ENTITY_server, "yb.tabletserver", &prototype, "value");
  };
  auto restart = [this](ExternalTabletServer* ts) -> Status {
    ts->Shutdown();
    RETURN_NOT_OK(ts->Restart());
    return cluster_->WaitForTabletServerCount(3, 30s * kTimeMultiplier);
  };
  auto set_flag = [this](const std::string& value) -> Status {
    RETURN_NOT_OK(cluster_->SetFlagOnMasters("enable_rpc_lane_preamble", value));
    return cluster_->SetFlagOnTServers("enable_rpc_lane_preamble", value);
  };

  // Flag off: no preamble anywhere. The laned servers' acceptors instantiate their counters; the
  // lanes=1 server has nothing to strip, so it instantiates none.
  ASSERT_NO_FATALS(run_workload());
  for (size_t i = 1; i != 3; ++i) {
    ASSERT_EQ(ASSERT_RESULT(counter(i, METRIC_rpc_lane_preamble_bound)), 0);
    ASSERT_GT(ASSERT_RESULT(counter(i, METRIC_rpc_lane_preamble_absent)), 0);
  }
  ASSERT_NOK(counter(0, METRIC_rpc_lane_preamble_stripped_late));

  // Promote at runtime; existing connections stay unbound, so nothing moves until the restarts.
  ASSERT_OK(set_flag("true"));
  ASSERT_NO_FATALS(run_workload());
  for (size_t i = 1; i != 3; ++i) {
    ASSERT_EQ(ASSERT_RESULT(counter(i, METRIC_rpc_lane_preamble_bound)), 0);
  }
  ASSERT_NOK(counter(0, METRIC_rpc_lane_preamble_stripped_late));
  // Tablet server 1 restarts with the flag off on its command line, then gets it at runtime.
  ASSERT_OK(restart(ts1));
  ASSERT_OK(cluster_->SetFlag(ts1, "enable_rpc_lane_preamble", "true"));
  ASSERT_OK(restart(ts0));
  ASSERT_NO_FATALS(run_workload());
  ASSERT_GT(ASSERT_RESULT(counter(1, METRIC_rpc_lane_preamble_bound)), 0);
  const auto stripped_promoted = ASSERT_RESULT(counter(0, METRIC_rpc_lane_preamble_stripped_late));
  ASSERT_GT(stripped_promoted, 0);
  const auto bound2_promoted = ASSERT_RESULT(counter(2, METRIC_rpc_lane_preamble_bound));

  // Demote at runtime: existing connections, bound or not, keep working and new ones carry no
  // preamble.
  ASSERT_OK(set_flag("false"));
  ASSERT_NO_FATALS(run_workload());
  ASSERT_EQ(ASSERT_RESULT(counter(0, METRIC_rpc_lane_preamble_stripped_late)), stripped_promoted);
  ASSERT_EQ(ASSERT_RESULT(counter(2, METRIC_rpc_lane_preamble_bound)), bound2_promoted);
  ASSERT_OK(restart(ts1));
  ASSERT_OK(restart(ts0));
  ASSERT_NO_FATALS(run_workload());
  ASSERT_EQ(ASSERT_RESULT(counter(1, METRIC_rpc_lane_preamble_bound)), 0);
  ASSERT_GT(ASSERT_RESULT(counter(1, METRIC_rpc_lane_preamble_absent)), 0);
  ASSERT_NOK(counter(0, METRIC_rpc_lane_preamble_stripped_late));
  ASSERT_EQ(ASSERT_RESULT(counter(2, METRIC_rpc_lane_preamble_bound)), bound2_promoted);

  ClusterVerifier verifier(cluster_.get());
  ASSERT_NO_FATALS(verifier.CheckCluster());
  ASSERT_NO_FATALS(verifier.CheckRowCount(table_name, ClusterVerifier::AT_LEAST, rows_inserted));
}

} // namespace yb
