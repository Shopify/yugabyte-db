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

// A single tablet replica whose ReplicaState update lock is held for a long time must not take
// the rest of the tablet server down with it. These tests hold the lock on one tablet and check
// that reads of that tablet fail at their deadline instead of parking an RPC worker, that reads
// of other tablets keep working, that the tablet server keeps heartbeating to the master, and
// that, when enabled, the followers of the stalled tablet elect a new leader. The last test shows
// what a healthy follower of the stalled tablet can serve to a follower read while the leader is
// stalled, and that a new leader restores it. The small pool tests fill the leader's RPC
// worker pool with reads of the stalled tablet and check that reads of a healthy tablet on the
// same server still get a worker, which is what the stall threshold is for.

#include <atomic>
#include <thread>

#include "yb/client/client-test-util.h"
#include "yb/client/client.h"
#include "yb/client/schema.h"
#include "yb/client/ql-dml-test-base.h"
#include "yb/client/session.h"
#include "yb/client/table.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"
#include "yb/client/yb_table_name.h"

#include "yb/common/ql_value.h"
#include "yb/common/read_hybrid_time.h"
#include "yb/common/schema.h"

#include "yb/consensus/consensus.h"
#include "yb/consensus/consensus.pb.h"
#include "yb/consensus/raft_consensus.h"
#include "yb/consensus/replica_state.h"

#include "yb/dockv/partition.h"

#include "yb/integration-tests/mini_cluster.h"

#include "yb/qlexpr/ql_rowblock.h"

#include "yb/master/master.h"
#include "yb/master/mini_master.h"
#include "yb/master/ts_descriptor.h"
#include "yb/master/ts_manager.h"

#include "yb/rpc/rpc_controller.h"

#include "yb/server/clock.h"

#include "yb/tablet/tablet.h"
#include "yb/tablet/tablet_peer.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"
#include "yb/tserver/tserver_error.h"
#include "yb/tserver/tserver_service.proxy.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/flags.h"
#include "yb/util/hdr_histogram.h"
#include "yb/util/memory/arena.h"
#include "yb/util/metrics.h"
#include "yb/util/monotime.h"
#include "yb/util/scope_exit.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"

#include "yb/yql/cql/ql/util/statement_result.h"

using namespace std::literals;

METRIC_DECLARE_histogram(handler_latency_yb_tserver_TabletServerService_Read);

DECLARE_string(TEST_hold_update_lock_tablet_id);
DECLARE_bool(stop_leader_requests_when_update_lock_stalled);
DECLARE_bool(tserver_heartbeat_metrics_add_leader_info);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(leader_lease_duration_ms);
DECLARE_int32(tserver_heartbeat_metrics_interval_ms);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_bool(enable_load_balancing);
DECLARE_int32(rpc_workers_limit);

namespace yb {

namespace {

const client::YBTableName kOtherTableName(
    YQL_DATABASE_CQL, "my_keyspace", "ql_client_other_table");

constexpr int32_t kKey = 1;
constexpr int32_t kValue = 42;

}  // namespace

class UpdateLockStallTest : public client::KeyValueTableTest<MiniCluster> {
 public:
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_load_balancing) = false;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_heartbeat_metrics_add_leader_info) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_heartbeat_interval_ms) = 500;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_heartbeat_metrics_interval_ms) = 500;
    // A 1 s failure detection period, which is also the stall threshold the fix derives from it.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_raft_heartbeat_interval_ms) = 250;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_leader_failure_max_missed_heartbeat_periods) = 4;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_stop_leader_requests_when_update_lock_stalled) = false;
    KeyValueTableTest::SetUp();

    CreateTable(client::Transactional::kFalse);
    client::kv_table_test::CreateTable(
        client::Transactional::kFalse, NumTablets(), client_.get(), &other_table_,
        kOtherTableName);

    auto session = NewSession();
    ASSERT_OK(WriteRow(session, kKey, kValue));
    ASSERT_OK(client::kv_table_test::WriteRow(&other_table_, session, kKey, kValue));

    auto tablet_ids = ListTabletIdsForTable(cluster_.get(), table_->id());
    ASSERT_EQ(tablet_ids.size(), 1);
    stalled_tablet_id_ = *tablet_ids.begin();
    auto other_tablet_ids = ListTabletIdsForTable(cluster_.get(), other_table_->id());
    ASSERT_EQ(other_tablet_ids.size(), 1);
    other_tablet_id_ = *other_tablet_ids.begin();
    leader_peer_ = ASSERT_RESULT(GetLeaderPeerForTablet(cluster_.get(), stalled_tablet_id_));
    leader_uuid_ = leader_peer_->permanent_uuid();
    LOG(INFO) << "Stalled tablet " << stalled_tablet_id_ << " led by " << leader_uuid_;
  }

  void DoTearDown() override {
    ReleaseLock();
    KeyValueTableTest::DoTearDown();
  }

  int NumTablets() override { return 1; }

  // Starts a write that stops inside DoReplicateBatch while holding the update lock of the
  // stalled tablet, and returns once the lock is held.
  void HoldLock() {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_hold_update_lock_tablet_id) = stalled_tablet_id_;
    holder_.AddThreadFunctor([this] {
      // The write completes once the lock is released. A long session timeout keeps the client
      // from giving up first.
      auto session = client_->NewSession(120s);
      auto result = WriteRow(session, kKey + 1, kValue);
      LOG(INFO) << "Lock holding write finished: " << (result.ok() ? "OK" : result.status().ToString());
    });
    ASSERT_OK(WaitFor([this] {
      return UpdateLockHeldFor() != MonoDelta::kZero;
    }, 10s, "Update lock held"));
    LOG(INFO) << "Update lock held on " << stalled_tablet_id_;
  }

  void ReleaseLock() {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_hold_update_lock_tablet_id) = "";
    holder_.JoinAll();
  }

  MonoDelta UpdateLockHeldFor() {
    auto consensus = CHECK_RESULT(leader_peer_->GetRaftConsensus());
    auto since = consensus->GetReplicaStateForTests()->UpdateLockHeldSince();
    if (since == CoarseTimePoint::max()) {
      return MonoDelta::kZero;
    }
    return MonoDelta(CoarseMonoClock::Now() - since);
  }

  const HdrHistogram* LeaderReadHistogram() {
    for (size_t i = 0; i < cluster_->num_tablet_servers(); ++i) {
      auto* mini_server = cluster_->mini_tablet_server(i);
      if (mini_server->server()->permanent_uuid() == leader_uuid_) {
        return mini_server->metric_entity().FindOrCreateMetric<Histogram>(
            &METRIC_handler_latency_yb_tserver_TabletServerService_Read)->underlying();
      }
    }
    LOG(FATAL) << "No tablet server with uuid " << leader_uuid_;
  }

  tserver::MiniTabletServer* LeaderServer() {
    for (size_t i = 0; i < cluster_->num_tablet_servers(); ++i) {
      auto* mini_server = cluster_->mini_tablet_server(i);
      if (mini_server->server()->permanent_uuid() == leader_uuid_) {
        return mini_server;
      }
    }
    LOG(FATAL) << "No tablet server with uuid " << leader_uuid_;
  }

  // The first tablet server that does not lead the stalled tablet. Every server holds a replica
  // of it, so this one is a follower.
  tserver::MiniTabletServer* FollowerServer() {
    for (size_t i = 0; i < cluster_->num_tablet_servers(); ++i) {
      auto* mini_server = cluster_->mini_tablet_server(i);
      if (mini_server->server()->permanent_uuid() != leader_uuid_) {
        return mini_server;
      }
    }
    LOG(FATAL) << "Every tablet server has uuid " << leader_uuid_;
  }

  // A follower read of the stalled tablet sent straight to one server, with the read time fixed
  // by the caller the way a YSQL follower read fixes it before picking a replica.
  Result<int32_t> FollowerRead(
      tserver::MiniTabletServer* server, HybridTime read_time, MonoDelta timeout) {
    return DirectRead(
        server, table_, stalled_tablet_id_, YBConsistencyLevel::CONSISTENT_PREFIX, read_time,
        timeout);
  }

  // One Read RPC of one tablet sent straight to one server, with no client side retry. An invalid
  // read time lets the server pick one.
  Result<int32_t> DirectRead(
      tserver::MiniTabletServer* server, const client::TableHandle& table,
      const std::string& tablet_id, YBConsistencyLevel consistency, HybridTime read_time,
      MonoDelta timeout) {
    tserver::TabletServerServiceProxy proxy(
        &cluster_->proxy_cache(), HostPort(server->bound_rpc_addr()));

    tserver::ReadRequestPB req;
    auto op = client::CreateReadOp(SharedThreadSafeArena(), kKey, table, kValueColumn);
    auto* ql_batch = req.add_ql_batch();
    *ql_batch = op->request().ToGoogleProtobuf();
    std::string partition_key;
    RETURN_NOT_OK(op->GetPartitionKey(&partition_key));
    const auto hash_code = dockv::PartitionSchema::DecodeMultiColumnHashValue(partition_key);
    ql_batch->set_hash_code(hash_code);
    ql_batch->set_max_hash_code(hash_code);
    req.set_tablet_id(tablet_id);
    req.set_consistency_level(consistency);
    if (read_time.is_valid()) {
      ReadHybridTime::SingleTime(read_time).ToPB(req.mutable_read_time());
    }

    rpc::RpcController controller;
    controller.set_timeout(timeout);
    tserver::ReadResponsePB resp;
    RETURN_NOT_OK(proxy.Read(req, &resp, &controller));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status()).CloneAndAddErrorCode(
          tserver::TabletServerError(resp.error().code()));
    }
    const auto& batch = resp.ql_batch(0);
    if (batch.status() != QLResponsePB::YQL_STATUS_OK) {
      return STATUS_FORMAT(
          RemoteError, "Bad response status: $0", QLResponsePB_QLStatus_Name(batch.status()));
    }
    Schema projection;
    const std::vector<ColumnId> column_ids = {ColumnId(table.ColumnId(kValueColumn))};
    RETURN_NOT_OK(client::internal::GetSchema(table->schema()).CreateProjectionByIdsIgnoreMissing(
        column_ids, &projection));
    auto columns = std::make_shared<std::vector<ColumnSchema>>(
        client::YBSchema(projection).columns());
    ql::RowsResult result(
        table->name(), columns,
        VERIFY_RESULT(controller.ExtractSidecar(batch.rows_data_sidecar())));
    auto row_block = result.GetRowBlock();
    if (row_block->row_count() != 1) {
      return STATUS_FORMAT(NotFound, "Expected one row, got $0", row_block->row_count());
    }
    return row_block->row(0).column(0).int32_value();
  }

  Result<master::TSDescriptorPtr> LeaderDescriptor() {
    auto* master = VERIFY_RESULT(cluster_->GetLeaderMiniMaster())->master();
    return master->ts_manager()->LookupTSByUUID(leader_uuid_);
  }

  // The leader uuid of the stalled tablet as seen by a follower replica, read from the cache so
  // the call itself never touches the stalled lock.
  Result<std::string> LeaderSeenByFollower() {
    for (const auto& peer : ListTabletPeers(cluster_.get(), ListPeersFilter::kAll)) {
      if (peer->tablet_id() != stalled_tablet_id_ || peer->permanent_uuid() == leader_uuid_) {
        continue;
      }
      auto consensus = VERIFY_RESULT(peer->GetRaftConsensus());
      return consensus->GetReplicaStateForTests()->GetConsensusStateFromCache().leader_uuid();
    }
    return STATUS(NotFound, "No follower peer found");
  }

 protected:
  client::TableHandle other_table_;
  std::string stalled_tablet_id_;
  std::string other_tablet_id_;
  tablet::TabletPeerPtr leader_peer_;
  std::string leader_uuid_;
  TestThreadHolder holder_;
};

// A read of the stalled tablet fails at its deadline and is counted as a not-leader rejection,
// instead of waiting for the lock. Reads of another tablet on the same servers keep working.
TEST_F(UpdateLockStallTest, ReadsFailAtDeadline) {
  // The server gives up on the lock after the 1 s failure detection period, well inside the 5 s
  // client deadline, so the client gets a NOT_THE_LEADER response and retries instead of timing
  // out on a worker parked on the lock.
  ASSERT_NO_FATALS(HoldLock());
  // Reads consult a cached leader state until the leader lease expires. Only after that do they
  // reach the update lock, so wait the lease out before reading.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_leader_lease_duration_ms + 1000));

  auto read_histogram = LeaderReadHistogram();
  const auto reads_before = read_histogram->TotalCount();
  const auto start = CoarseMonoClock::Now();
  auto session = client_->NewSession(5s);
  auto result = SelectRow(session, kKey);
  const auto elapsed = MonoDelta(CoarseMonoClock::Now() - start);
  LOG(INFO) << "Read of the stalled tablet finished in " << elapsed << ": "
            << (result.ok() ? "OK" : result.status().ToString());
  ASSERT_NOK(result);
  // The client retries until its own deadline, so the whole read takes about the session timeout.
  ASSERT_LT(elapsed, MonoDelta::FromSeconds(10));
  // The server answered every attempt at the stall threshold instead of holding the worker until
  // the client deadline. Without the fix the handler count does not move while the lock is held.
  const auto reads_answered = read_histogram->TotalCount() - reads_before;
  const auto slowest_read = MonoDelta::FromMicroseconds(read_histogram->MaxValue());
  LOG(INFO) << "Read RPCs answered while stalled: " << reads_answered
            << ", slowest: " << slowest_read;
  ASSERT_GE(reads_answered, 2);
  ASSERT_LT(slowest_read, MonoDelta::FromSeconds(3));
  ASSERT_GT(UpdateLockHeldFor(), MonoDelta::FromSeconds(1));

  auto other_session = client_->NewSession(5s);
  ASSERT_EQ(ASSERT_RESULT(
      client::kv_table_test::SelectRow(&other_table_, other_session, kKey)), kValue);

  ReleaseLock();

  // Both tablets serve reads once the lock is released.
  ASSERT_EQ(ASSERT_RESULT(SelectRow(NewSession(), kKey)), kValue);
  ASSERT_EQ(ASSERT_RESULT(
      client::kv_table_test::SelectRow(&other_table_, NewSession(), kKey)), kValue);
}

// The tablet server keeps heartbeating to the master while one tablet holds its update lock.
TEST_F(UpdateLockStallTest, HeartbeatsContinue) {
  ASSERT_NO_FATALS(HoldLock());

  // Several heartbeat periods. Without the fix the heartbeater parks on the lock inside
  // CreateReportedTabletPB or the metrics provider and the master stops hearing from the server.
  const auto deadline = CoarseMonoClock::Now() + 6s;
  MonoDelta max_gap = MonoDelta::kZero;
  while (CoarseMonoClock::Now() < deadline) {
    auto descriptor = ASSERT_RESULT(LeaderDescriptor());
    max_gap = std::max(max_gap, descriptor->TimeSinceHeartbeat());
    SleepFor(200ms);
  }
  LOG(INFO) << "Longest gap between heartbeats while stalled: " << max_gap;
  ASSERT_LT(max_gap, MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms * 4));
  ASSERT_GT(UpdateLockHeldFor(), MonoDelta::FromSeconds(5));

  ReleaseLock();
  ASSERT_EQ(ASSERT_RESULT(SelectRow(NewSession(), kKey)), kValue);
}

// Without the stop flag, the stalled leader keeps sending Raft heartbeats and stays leader. With
// the flag, it stops once the hold passes the threshold and a follower takes over.
TEST_F(UpdateLockStallTest, FollowersElectNewLeader) {
  ASSERT_NO_FATALS(HoldLock());
  SleepFor(MonoDelta::FromMilliseconds(static_cast<int64_t>(
      FLAGS_raft_heartbeat_interval_ms * FLAGS_leader_failure_max_missed_heartbeat_periods * 2)));
  ASSERT_EQ(ASSERT_RESULT(LeaderSeenByFollower()), leader_uuid_);

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_stop_leader_requests_when_update_lock_stalled) = true;
  const auto start = CoarseMonoClock::Now();
  ASSERT_OK(WaitFor([this]() -> Result<bool> {
    auto leader = VERIFY_RESULT(LeaderSeenByFollower());
    return !leader.empty() && leader != leader_uuid_;
  }, 20s, "Followers elect a new leader"));
  LOG(INFO) << "New leader elected after " << MonoDelta(CoarseMonoClock::Now() - start);

  // The new leader serves reads while the old one still holds its lock.
  ASSERT_GT(UpdateLockHeldFor(), MonoDelta::FromSeconds(1));
  ASSERT_EQ(ASSERT_RESULT(SelectRow(client_->NewSession(10s), kKey)), kValue);

  ReleaseLock();
  ASSERT_EQ(ASSERT_RESULT(SelectRow(NewSession(), kKey)), kValue);
}

// A follower read picks its replica by locality and retries on the same one, so what a healthy
// follower of the stalled tablet can serve decides whether those reads survive. The leader
// refreshes its propagated safe time under the update lock, so the safe time it sends to followers
// stops about one lease into the stall. The follower still serves data as of before the stall but
// cannot serve a newer read time until a new leader takes over.
TEST_F(UpdateLockStallTest, FollowerReadsOfStalledTablet) {
  auto* follower = FollowerServer();
  const auto before_stall = follower->server()->Clock()->Now();

  ASSERT_NO_FATALS(HoldLock());
  // Well past the point where the propagated safe time stops moving.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_leader_lease_duration_ms * 2 + 1000));

  // Data as of before the stall is still readable on the follower.
  ASSERT_EQ(ASSERT_RESULT(FollowerRead(follower, before_stall, 5s)), kValue);

  // A read time from during the stall waits for a safe time that never arrives.
  const auto during_stall = follower->server()->Clock()->Now();
  auto start = CoarseMonoClock::Now();
  auto result = FollowerRead(follower, during_stall, 3s);
  auto elapsed = MonoDelta(CoarseMonoClock::Now() - start);
  LOG(INFO) << "Follower read at a read time from during the stall finished in " << elapsed << ": "
            << (result.ok() ? "OK" : result.status().ToString());
  ASSERT_NOK(result);
  ASSERT_GE(elapsed, MonoDelta::FromMilliseconds(2500));
  ASSERT_GT(UpdateLockHeldFor(), MonoDelta::FromMilliseconds(FLAGS_leader_lease_duration_ms * 2));

  // A new leader propagates a fresh safe time and the same read succeeds.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_stop_leader_requests_when_update_lock_stalled) = true;
  start = CoarseMonoClock::Now();
  ASSERT_OK(WaitFor([this]() -> Result<bool> {
    auto leader = VERIFY_RESULT(LeaderSeenByFollower());
    return !leader.empty() && leader != leader_uuid_;
  }, 20s, "Followers elect a new leader"));
  LOG(INFO) << "New leader elected after " << MonoDelta(CoarseMonoClock::Now() - start);
  start = CoarseMonoClock::Now();
  ASSERT_EQ(ASSERT_RESULT(FollowerRead(follower, during_stall, 10s)), kValue);
  LOG(INFO) << "Follower read at the same read time finished in "
            << MonoDelta(CoarseMonoClock::Now() - start) << " under the new leader";

  ReleaseLock();
  ASSERT_EQ(ASSERT_RESULT(SelectRow(NewSession(), kKey)), kValue);
}

// The same cluster with a small RPC worker pool on every server, so a few reads parked on the
// stalled lock are enough to fill it.
class UpdateLockStallSmallPoolTest : public UpdateLockStallTest {
 public:
  static constexpr int kWorkers = 8;
  static constexpr int kParkedReads = 2 * kWorkers;

  void SetUp() override {
    // Read once at server start, so it has to be set before the cluster comes up.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_workers_limit) = kWorkers;
    UpdateLockStallTest::SetUp();
  }

  // Sends kParkedReads strong reads of the stalled tablet to its leader at once, each with a long
  // deadline, and returns once they are all in flight. Without a stall threshold every one of them
  // parks a worker on the update lock until its deadline.
  void ParkReadsOnLeader(MonoDelta read_timeout) {
    auto* leader = LeaderServer();
    for (int i = 0; i < kParkedReads; ++i) {
      readers_.AddThreadFunctor([this, leader, read_timeout] {
        auto result = DirectRead(
            leader, table_, stalled_tablet_id_, YBConsistencyLevel::STRONG, HybridTime::kInvalid,
            read_timeout);
        if (!result.ok()) {
          ++parked_reads_failed_;
        }
      });
    }
    // Give the reads time to reach the server and park.
    SleepFor(1s);
  }

  // A read of the healthy tablet at the stalled leader's server. Every server holds a replica of
  // the other table, so a consistent prefix read is served locally and needs only a free worker.
  Result<int32_t> ReadOtherTabletAtLeader(MonoDelta timeout) {
    return DirectRead(
        LeaderServer(), other_table_, other_tablet_id_, YBConsistencyLevel::CONSISTENT_PREFIX,
        HybridTime::kInvalid, timeout);
  }

  void DoTearDown() override {
    ReleaseLock();
    readers_.JoinAll();
    UpdateLockStallTest::DoTearDown();
  }

 protected:
  TestThreadHolder readers_;
  std::atomic<int> parked_reads_failed_{0};
};

// With the stall threshold in place, reads of the stalled tablet give their worker back after
// the threshold, so twice as many reads as workers cannot keep a healthy tablet from being read.
TEST_F(UpdateLockStallSmallPoolTest, HealthyTabletReadableUnderStall) {
  ASSERT_NO_FATALS(HoldLock());
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_leader_lease_duration_ms + 1000));

  ASSERT_NO_FATALS(ParkReadsOnLeader(30s));

  const auto start = CoarseMonoClock::Now();
  auto result = ReadOtherTabletAtLeader(5s);
  const auto elapsed = MonoDelta(CoarseMonoClock::Now() - start);
  LOG(INFO) << "Read of the healthy tablet at the stalled leader finished in " << elapsed << ": "
            << (result.ok() ? "OK" : result.status().ToString());
  ASSERT_OK(result);
  ASSERT_EQ(*result, kValue);
  // A worker came free at the threshold, not at the parked reads' 30 s deadline.
  ASSERT_LT(elapsed, MonoDelta::FromSeconds(3));

  readers_.JoinAll();
  LOG(INFO) << "Parked reads that failed: " << parked_reads_failed_ << " of " << kParkedReads;
  ASSERT_EQ(parked_reads_failed_, kParkedReads);
  ASSERT_GT(UpdateLockHeldFor(), MonoDelta::FromSeconds(1));

  ReleaseLock();
  ASSERT_EQ(ASSERT_RESULT(SelectRow(NewSession(), kKey)), kValue);
  ASSERT_EQ(ASSERT_RESULT(ReadOtherTabletAtLeader(5s)), kValue);
}

// The control. With the failure detection period, and so the threshold, far above the read
// deadline, the parked reads hold every worker and the healthy tablet cannot be read at that server
// until they time out. This is the incident shape, kept as a test so the fix is measured against it.
TEST_F(UpdateLockStallSmallPoolTest, HealthyTabletStarvedWithoutThreshold) {
  // 600 s at the fixture's 250 ms heartbeat interval.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_leader_failure_max_missed_heartbeat_periods) = 2400;

  ASSERT_NO_FATALS(HoldLock());
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_leader_lease_duration_ms + 1000));

  ASSERT_NO_FATALS(ParkReadsOnLeader(15s));

  const auto start = CoarseMonoClock::Now();
  auto result = ReadOtherTabletAtLeader(5s);
  const auto elapsed = MonoDelta(CoarseMonoClock::Now() - start);
  LOG(INFO) << "Read of the healthy tablet at the stalled leader finished in " << elapsed << ": "
            << (result.ok() ? "OK" : result.status().ToString());
  ASSERT_NOK(result);
  ASSERT_GE(elapsed, MonoDelta::FromSeconds(4));

  ReleaseLock();
  readers_.JoinAll();
  ASSERT_EQ(ASSERT_RESULT(ReadOtherTabletAtLeader(5s)), kValue);
}

}  // namespace yb
