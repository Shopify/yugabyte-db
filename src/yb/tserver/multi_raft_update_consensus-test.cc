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

// Tests of ConsensusServiceImpl::MultiRaftUpdateConsensus. The test acts as the Raft leader of
// several tablets hosted by one tablet server: the tablets' configs name the server, a fake leader
// and a fake peer, so the server's replicas are followers of whatever the test sends.

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "yb/client/yb_table_name.h"

#include "yb/common/opid.h"
#include "yb/common/wire_protocol-test-util.h"
#include "yb/common/wire_protocol.h"

#include "yb/consensus/consensus.messages.h"
#include "yb/consensus/consensus.pb.h"
#include "yb/consensus/consensus.proxy.h"
#include "yb/consensus/log.h"
#include "yb/consensus/metadata.pb.h"
#include "yb/consensus/raft_consensus.h"

#include "yb/rpc/messenger.h"
#include "yb/rpc/rpc_controller.h"
#include "yb/rpc/service_pool.h"
#include "yb/rpc/thread_pool.h"

#include "yb/server/clock.h"
#include "yb/server/rpc_server.h"

#include "yb/tablet/operations.pb.h"
#include "yb/tablet/tablet_peer.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_service.h"
#include "yb/tserver/tablet_server.h"
#include "yb/tserver/tablet_server-test-base.h"
#include "yb/tserver/ts_tablet_manager.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/cgroups.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/metrics.h"
#include "yb/util/scope_exit.h"
#include "yb/util/sync_point.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/thread.h"
#include "yb/util/tsan_util.h"

using namespace std::literals;

DECLARE_bool(allow_encryption_at_rest);
DECLARE_bool(disable_auto_flags_management);
DECLARE_bool(enable_leader_failure_detection);
DECLARE_bool(enable_multi_raft_data_batching);
DECLARE_bool(enforce_xcluster_guarded_lease);
DECLARE_int32(multi_raft_update_max_threads);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_bool(use_preelection);

METRIC_DECLARE_entity(server);
METRIC_DECLARE_histogram(handler_latency_yb_consensus_ConsensusService_MultiRaftUpdateConsensus);
METRIC_DECLARE_counter(multi_raft_update_deadline_responses);
METRIC_DECLARE_counter(multi_raft_update_sub_requests_concurrent);

namespace yb::tserver {

using consensus::ConsensusRequestPB;
using consensus::ConsensusResponsePB;
using consensus::MultiRaftConsensusResponsePB;
using consensus::VoteRequestPB;
using consensus::VoteResponsePB;

namespace {

constexpr auto kFakeLeader = "fake-leader";
constexpr auto kFakePeer = "fake-peer";

// Kept short so that the tests about election timeouts are fast: the minimum election timeout is
// leader_failure_max_missed_heartbeat_periods (6) heartbeat intervals.
constexpr int kHeartbeatIntervalMs = 100;

struct Envelope {
  consensus::MultiRaftConsensusRequestPB req;
  MultiRaftConsensusResponsePB resp;
  rpc::RpcController controller;
  CountDownLatch done{1};
  MonoTime start;
  MonoTime finish;

  bool finished() const { return done.count() == 0; }
  MonoDelta elapsed() const { return finish - start; }
};

void AssertSlotOk(const ConsensusResponsePB& slot, const OpId& last_received) {
  ASSERT_FALSE(slot.has_error()) << slot.ShortDebugString();
  ASSERT_FALSE(slot.status().has_error()) << slot.ShortDebugString();
  ASSERT_EQ(OpId::FromPB(slot.status().last_received()), last_received)
      << slot.ShortDebugString();
}

void AssertSlotError(const ConsensusResponsePB& slot, AppStatusPB::ErrorCode code) {
  ASSERT_TRUE(slot.has_error()) << slot.ShortDebugString();
  ASSERT_EQ(slot.error().status().code(), code) << slot.ShortDebugString();
}

} // namespace

class MultiRaftUpdateConsensusTest : public TabletServerTestBase {
 protected:
  struct TestTablet {
    TabletId id;
    tablet::TabletPeerPtr peer;
    // Last op the test, as leader, has sent, and the commit index it has sent.
    OpId last = OpId::Min();
    OpId committed = OpId::Min();
  };

  void SetUp() override {
    TabletServerTestBase::SetUp();
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_leader_failure_detection) = false;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_raft_heartbeat_interval_ms) = kHeartbeatIntervalMs;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = true;
  }

  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    TabletServerTestBase::TearDown();
  }

  // Starts the tablet server with num_tablets follower tablets, and makes the test their leader in
  // term 1.
  void StartServer(size_t num_tablets) {
    // There is no master, as in TabletServerTestBase::StartTabletServer.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_disable_auto_flags_management) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_allow_encryption_at_rest) = false;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enforce_xcluster_guarded_lease) = false;
    mini_server_ = ASSERT_RESULT(CreateMiniTabletServer());
    auto master_addresses = std::make_shared<server::MasterAddresses>();
    master_addresses->push_back({HostPort("255.255.255.255", 1)});
    mini_server_->options()->SetMasterAddresses(master_addresses);
    ASSERT_OK(mini_server_->Start(tserver::WaitTabletsBootstrapped::kFalse));
    server_uuid_ = mini_server_->server()->permanent_uuid();

    consensus::RaftConfigPB config;
    auto add_peer = [&config](const std::string& uuid, const HostPort& host_port) {
      auto* peer = config.add_peers();
      peer->set_permanent_uuid(uuid);
      peer->set_member_type(consensus::PeerMemberType::VOTER);
      HostPortToPB(host_port, peer->mutable_last_known_private_addr()->Add());
    };
    add_peer(server_uuid_, HostPort::FromBoundEndpoint(mini_server_->bound_rpc_addr()));
    // Nothing listens on port 1, so elections started by the server fail fast.
    add_peer(kFakeLeader, HostPort("127.0.0.1", 1));
    add_peer(kFakePeer, HostPort("127.0.0.1", 1));

    auto* tablet_manager = mini_server_->server()->tablet_manager();
    for (size_t i = 0; i != num_tablets; ++i) {
      TestTablet tablet;
      tablet.id = Format("tablet-$0", i);
      ASSERT_OK(mini_server_->AddTestTablet(
          kTableName.namespace_name(), Format("table-$0", i), tablet.id, schema_, config,
          table_type_));
      tablet.peer = ASSERT_RESULT(tablet_manager->GetTablet(tablet.id));
      tablets_.push_back(std::move(tablet));
    }
    for (auto& tablet : tablets_) {
      ASSERT_OK(tablet.peer->WaitUntilConsensusRunning(60s));
    }
    ResetClientProxies();

    std::vector<ConsensusRequestPB> heartbeats;
    for (auto& tablet : tablets_) {
      heartbeats.push_back(Heartbeat(tablet));
    }
    auto resp = ASSERT_RESULT(Send(std::move(heartbeats)));
    for (int i = 0; i != resp.consensus_response_size(); ++i) {
      ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(i), OpId::Min()));
    }
  }

  ConsensusRequestPB MakeRequest(const TestTablet& tablet) {
    ConsensusRequestPB req;
    req.set_tablet_id(tablet.id);
    req.set_dest_uuid(server_uuid_);
    req.set_caller_uuid(kFakeLeader);
    req.set_caller_term(term_);
    tablet.last.ToPB(req.mutable_preceding_id());
    tablet.committed.ToPB(req.mutable_committed_op_id());
    return req;
  }

  // A request without ops, built the way the leader builds a heartbeat.
  ConsensusRequestPB Heartbeat(const TestTablet& tablet) {
    return MakeRequest(tablet);
  }

  // A request with num_ops new write ops, optionally committing all of them. Writes, unlike other
  // operation types, are applied before they are durable
  // (ReplicaState::ApplyPendingOperationsUnlocked), so committing them does not make
  // UpdateReplica wait for the log.
  ConsensusRequestPB Data(TestTablet* tablet, int num_ops, bool commit) {
    auto req = MakeRequest(*tablet);
    for (int i = 0; i != num_ops; ++i) {
      tablet->last = OpId(term_, tablet->last.index + 1);
      auto* op = req.add_ops();
      tablet->last.ToPB(op->mutable_id());
      op->set_hybrid_time(mini_server_->server()->clock()->Now().ToUint64());
      op->set_op_type(consensus::WRITE_OP);
      auto key = narrow_cast<int32_t>(tablet->last.index);
      AddKVToPB(key, key + 10, Format("value-$0", key), op->mutable_write()->mutable_write_batch());
    }
    if (commit) {
      tablet->committed = tablet->last;
      tablet->committed.ToPB(req.mutable_committed_op_id());
    }
    return req;
  }

  std::unique_ptr<Envelope> SendAsync(
      std::vector<ConsensusRequestPB> requests, MonoDelta timeout = 30s) {
    auto envelope = std::make_unique<Envelope>();
    for (auto& request : requests) {
      *envelope->req.add_consensus_request() = std::move(request);
    }
    envelope->controller.set_timeout(timeout);
    envelope->start = MonoTime::Now();
    auto* e = envelope.get();
    consensus_proxy_->MultiRaftUpdateConsensusAsync(
        e->req, &e->resp, &e->controller, [e] {
          e->finish = MonoTime::Now();
          e->done.CountDown();
        });
    return envelope;
  }

  Status Wait(Envelope* envelope) {
    SCHECK(envelope->done.WaitFor(60s), TimedOut, "Envelope did not complete");
    RETURN_NOT_OK(envelope->controller.status());
    SCHECK_EQ(
        envelope->resp.consensus_response_size(), envelope->req.consensus_request_size(),
        IllegalState, "Wrong number of responses");
    return Status::OK();
  }

  Result<MultiRaftConsensusResponsePB> Send(
      std::vector<ConsensusRequestPB> requests, MonoDelta timeout = 30s) {
    auto envelope = SendAsync(std::move(requests), timeout);
    RETURN_NOT_OK(Wait(envelope.get()));
    return envelope->resp;
  }

  std::shared_ptr<consensus::RaftConsensus> Consensus(const TestTablet& tablet) {
    return CHECK_RESULT(tablet.peer->GetRaftConsensus());
  }

  OpId LastReceived(const TestTablet& tablet) {
    return Consensus(tablet)->GetLastReceivedOpId();
  }

  int64_t CurrentTerm(const TestTablet& tablet) {
    return Consensus(tablet)->ConsensusState(
        consensus::CONSENSUS_CONFIG_COMMITTED, nullptr).current_term();
  }

  void SetSlowDisk(const TestTablet& tablet, MonoDelta delay) {
    tablet.peer->log()->TEST_SetSleepDuration(delay.ToSteadyDuration());
  }

  Status WaitForLastReceived(const TestTablet& tablet, MonoDelta timeout = 10s) {
    return WaitFor(
        [this, &tablet] { return LastReceived(tablet) == tablet.last; }, timeout,
        Format("$0 receives $1", tablet.id, tablet.last));
  }

  int64_t ServerCounter(const CounterPrototype& prototype) {
    return prototype.Instantiate(mini_server_->server()->metric_entity())->value();
  }

  int64_t ConcurrentSubRequests() {
    return ServerCounter(METRIC_multi_raft_update_sub_requests_concurrent);
  }

  int64_t DeadlineResponses() {
    return ServerCounter(METRIC_multi_raft_update_deadline_responses);
  }

  // Holds tablet's update_mutex_ for about duration: sends it a direct UpdateConsensus with one op
  // and TEST_DelayUpdate, which sleeps under the mutex after UpdateReplica, and returns once that
  // sleep has started. direct_done is set when the direct call returns.
  void HoldUpdateMutex(
      TestTablet* tablet, MonoDelta duration, TestThreadHolder* thread_holder,
      std::atomic<bool>* direct_done) {
    auto consensus = Consensus(*tablet);
    consensus->TEST_DelayUpdate(duration);
    auto request = rpc::CopySharedMessage(Data(tablet, 1, /* commit= */ true));
    thread_holder->AddThreadFunctor([this, request, direct_done] {
      auto response = rpc::MakeSharedMessage<consensus::LWConsensusResponsePB>();
      rpc::RpcController controller;
      controller.set_timeout(60s);
      ASSERT_OK(consensus_proxy_->UpdateConsensus(*request, response.get(), &controller));
      ASSERT_FALSE(response->has_error()) << response->ShortDebugString();
      direct_done->store(true);
    });
    // UpdateReplica has run once the op is received; the sleep under the mutex follows at once.
    ASSERT_OK(WaitForLastReceived(*tablet));
    consensus->TEST_DelayUpdate(MonoDelta::kZero);
  }

  VoteRequestPB MakeVoteRequest(const TestTablet& tablet, int64_t term, bool ignore_live_leader) {
    VoteRequestPB req;
    req.set_tablet_id(tablet.id);
    req.set_dest_uuid(server_uuid_);
    req.set_candidate_uuid(kFakePeer);
    req.set_candidate_term(term);
    OpId(term, 1000).ToPB(req.mutable_candidate_status()->mutable_last_received());
    req.set_ignore_live_leader(ignore_live_leader);
    return req;
  }

  Result<VoteResponsePB> RequestVote(const VoteRequestPB& req) {
    VoteResponsePB resp;
    rpc::RpcController controller;
    controller.set_timeout(10s);
    RETURN_NOT_OK(consensus_proxy_->RequestConsensusVote(req, &resp, &controller));
    return resp;
  }

  std::string server_uuid_;
  int64_t term_ = 1;
  std::vector<TestTablet> tablets_;
};

// Response slots are in request order even when the sub-requests complete in another order, and
// the requests went through the concurrent path.
TEST_F(MultiRaftUpdateConsensusTest, SlotsInRequestOrder) {
  ASSERT_NO_FATALS(StartServer(4));
  const auto concurrent_before = ConcurrentSubRequests();
  // The first sub-request completes last.
  SetSlowDisk(tablets_[0], 500ms);
  std::vector<ConsensusRequestPB> requests;
  for (size_t i = 0; i != tablets_.size(); ++i) {
    // Different op counts give every slot a different last_received.
    requests.push_back(Data(&tablets_[i], narrow_cast<int>(i) + 1, /* commit= */ i != 0));
  }
  auto envelope = SendAsync(std::move(requests));
  // The other sub-requests finish while the first is still waiting for its log.
  for (size_t i = 1; i != tablets_.size(); ++i) {
    ASSERT_OK(WaitForLastReceived(tablets_[i]));
  }
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_GE(envelope->elapsed(), 500ms);
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        envelope->resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
  ASSERT_EQ(ConcurrentSubRequests() - concurrent_before, tablets_.size());
  // The handler latency covers the time until the response, not until the handler returned.
  auto handler_latency =
      METRIC_handler_latency_yb_consensus_ConsensusService_MultiRaftUpdateConsensus.Instantiate(
          mini_server_->server()->metric_entity());
  ASSERT_GE(handler_latency->MaxValue(), ToMicroseconds(500ms));
}

// A failing sub-request gets an error in its own slot and does not affect the others.
TEST_F(MultiRaftUpdateConsensusTest, ErrorIsolation) {
  ASSERT_NO_FATALS(StartServer(3));
  auto& shut_down = tablets_[2];
  ASSERT_TRUE(shut_down.peer->StartShutdown(
      tablet::DisableFlushOnShutdown::kFalse, tablet::AbortOps::kTrue));
  shut_down.peer->CompleteShutdown();

  auto wrong_uuid = Heartbeat(tablets_[0]);
  wrong_uuid.set_dest_uuid("no-such-server");
  auto unknown_tablet = Heartbeat(tablets_[0]);
  unknown_tablet.set_tablet_id("no-such-tablet");
  auto stale_term = Heartbeat(tablets_[1]);
  stale_term.set_caller_term(term_ - 1);
  auto good = Data(&tablets_[0], 2, /* commit= */ true);

  auto resp = ASSERT_RESULT(Send({
      wrong_uuid, unknown_tablet, Heartbeat(shut_down), stale_term, good}));
  ASSERT_TRUE(resp.consensus_response(0).has_error());
  ASSERT_EQ(resp.consensus_response(0).error().code(), TabletServerErrorPB::WRONG_SERVER_UUID);
  ASSERT_TRUE(resp.consensus_response(1).has_error());
  ASSERT_EQ(resp.consensus_response(1).error().code(), TabletServerErrorPB::TABLET_NOT_FOUND);
  ASSERT_TRUE(resp.consensus_response(2).has_error()) << resp.consensus_response(2).DebugString();
  // A rejected term is a Raft-level error in the response status, as on the direct path.
  ASSERT_FALSE(resp.consensus_response(3).has_error());
  ASSERT_EQ(resp.consensus_response(3).status().error().code(),
            consensus::ConsensusErrorPB::INVALID_TERM);
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(4), tablets_[0].last));
}

// A tablet shut down while its sub-request waits for the log completes that sub-request with an
// error, well before the log would have become durable.
TEST_F(MultiRaftUpdateConsensusTest, ShutdownWhileWaitingForLog) {
  ASSERT_NO_FATALS(StartServer(2));
  auto& shut_down = tablets_[0];
  SetSlowDisk(shut_down, 3s);
  auto envelope = SendAsync({Data(&shut_down, 1, false), Data(&tablets_[1], 1, true)});
  ASSERT_OK(WaitForLastReceived(shut_down));
  ASSERT_FALSE(envelope->finished());

  TestThreadHolder thread_holder;
  thread_holder.AddThreadFunctor([peer = shut_down.peer] {
    ASSERT_TRUE(peer->StartShutdown(
        tablet::DisableFlushOnShutdown::kFalse, tablet::AbortOps::kTrue));
    peer->CompleteShutdown();
  });
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_LT(envelope->elapsed(), 2500ms);
  ASSERT_TRUE(envelope->resp.consensus_response(0).has_error())
      << envelope->resp.consensus_response(0).ShortDebugString();
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(1), tablets_[1].last));
  thread_holder.JoinAll();
}

// A durability wait whose step the Raft notifications pool does not run still completes its
// sub-request, with an error, long before the envelope deadline.
TEST_F(MultiRaftUpdateConsensusTest, NotificationPoolShutdownCompletesWait) {
  ASSERT_NO_FATALS(StartServer(1));
  auto& tablet = tablets_[0];
  SetSlowDisk(tablet, 1s);
  auto envelope = SendAsync({Data(&tablet, 1, false)});
  ASSERT_OK(WaitForLastReceived(tablet));
  ASSERT_FALSE(envelope->finished());
  mini_server_->server()->tablet_manager()->raft_notifications_pool()->Shutdown();
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_LT(envelope->elapsed(), 5s);
  ASSERT_NO_FATALS(AssertSlotError(envelope->resp.consensus_response(0), AppStatusPB::ABORTED));
}

// At the deadline the RPC is answered with the completed slots and TimedOut in the others. A
// sub-request that completes after that is dropped.
TEST_F(MultiRaftUpdateConsensusTest, DeadlinePartialResponse) {
  ASSERT_NO_FATALS(StartServer(2));
  auto& paused = tablets_[1];
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });
  const auto deadline_responses_before = DeadlineResponses();
  auto envelope = SendAsync({Data(&tablets_[0], 1, true), Data(&paused, 1, true)}, 2s);
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_EQ(DeadlineResponses() - deadline_responses_before, 1);
  // Answered before the client's own timeout, at 90% of it.
  ASSERT_GE(envelope->elapsed(), 1500ms);
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotError(envelope->resp.consensus_response(1), AppStatusPB::TIMED_OUT));
  ASSERT_NE(LastReceived(paused), paused.last);

  // The paused sub-request completes after the response.
  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(WaitForLastReceived(paused));
  auto resp = ASSERT_RESULT(Send({Heartbeat(tablets_[0]), Heartbeat(paused)}));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(1), paused.last));
}

// Once the RPC is answered, a sub-request that is still running no longer keeps the inbound call
// alive. The paused sub-request ignores the deadline, so it outlives the response.
TEST_F(MultiRaftUpdateConsensusTest, AnsweredEnvelopeReleasedWhileSubRequestPending) {
  ASSERT_NO_FATALS(StartServer(1));
  auto& paused = tablets_[0];
  auto calls = mini_server_->server()->mem_tracker()->FindChild("Call");
  ASSERT_NE(calls, nullptr);
  auto inbound_calls = calls->FindChild("Inbound RPC");
  ASSERT_NE(inbound_calls, nullptr);
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });
  auto envelope = SendAsync({Data(&paused, 1, true)}, 1s);
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_NO_FATALS(AssertSlotError(envelope->resp.consensus_response(0), AppStatusPB::TIMED_OUT));
  ASSERT_OK(WaitFor(
      [&inbound_calls] { return inbound_calls->consumption() == 0; }, 3s,
      "Inbound call released"));
  // The sub-request was still paused the whole time.
  ASSERT_NE(LastReceived(paused), paused.last);

  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(WaitForLastReceived(paused));
}

// A sub-request's wait for its log ends at the envelope deadline even though the log is still
// slow, so the wait does not outlive the response.
TEST_F(MultiRaftUpdateConsensusTest, DurabilityWaitEndsAtDeadline) {
  ASSERT_NO_FATALS(StartServer(1));
  auto& tablet = tablets_[0];
  auto* log = tablet.peer->log();
  SetSlowDisk(tablet, 10s);
  auto envelope = SendAsync({Data(&tablet, 1, false)}, 3s);
  ASSERT_OK(WaitFor(
      [log] { return log->TEST_num_safe_op_id_waiters() == 1; }, 2s, "Waiting for the log"));
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_NO_FATALS(AssertSlotError(envelope->resp.consensus_response(0), AppStatusPB::TIMED_OUT));
  ASSERT_OK(WaitFor(
      [log] { return log->TEST_num_safe_op_id_waiters() == 0; }, 2s, "Wait ended"));
  ASSERT_FALSE(log->IsSafeOpIdToApply(tablet.last));
  SetSlowDisk(tablet, 0s);
  ASSERT_OK(WaitFor(
      [log, &tablet] { return log->IsSafeOpIdToApply(tablet.last); }, 30s, "Log catches up"));
}

// A sub-request whose task the pool drops at shutdown, before it ran, completes its slot with the
// pool's error rather than leaving it to the deadline.
TEST_F(MultiRaftUpdateConsensusTest, PoolShutdownCompletesQueuedSubRequest) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_max_threads) = 1;
  ASSERT_NO_FATALS(StartServer(2));
  auto& paused = tablets_[0];
  const auto queued = tablets_[1];
  auto service = std::dynamic_pointer_cast<ConsensusServiceImpl>(
      mini_server_->server()->rpc_server()->TEST_service_pool("yb.consensus.ConsensusService")
          ->TEST_get_service());
  ASSERT_NE(service, nullptr);
  std::atomic<int> started{0};
  auto* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("MultiRaftUpdateTask::BeforeUpdate", [&started](void*) { ++started; });
  sync_point->EnableProcessing();
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });

  // The pool's only thread is held by the paused sub-request, so the other one stays queued.
  auto envelope = SendAsync({Data(&paused, 1, true), Data(&tablets_[1], 1, true)}, 30s);
  ASSERT_OK(WaitFor([&started] { return started.load() == 1; }, 10s, "Paused sub-request runs"));
  TestThreadHolder thread_holder;
  // Shutdown drops the queued task at once, then waits for the running one.
  thread_holder.AddThreadFunctor([service] { service->Shutdown(); });
  SleepFor(500ms * kTimeMultiplier);
  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(Wait(envelope.get()));
  thread_holder.JoinAll();
  ASSERT_LT(envelope->elapsed(), 10s);
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(0), paused.last));
  ASSERT_NO_FATALS(AssertSlotError(envelope->resp.consensus_response(1), AppStatusPB::ABORTED));
  // The queued sub-request never ran.
  ASSERT_EQ(started.load(), 1);
  ASSERT_EQ(LastReceived(tablets_[1]), queued.last);
}

// The log waits of sub-requests for different tablets overlap, and do not hold a thread: a single
// pool thread serves them all.
TEST_F(MultiRaftUpdateConsensusTest, DistinctTabletsOverlap) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_max_threads) = 1;
  ASSERT_NO_FATALS(StartServer(4));
  constexpr auto kDiskDelay = 1s;
  std::vector<ConsensusRequestPB> requests;
  for (auto& tablet : tablets_) {
    SetSlowDisk(tablet, kDiskDelay);
    requests.push_back(Data(&tablet, 1, false));
  }
  auto envelope = SendAsync(std::move(requests));
  ASSERT_OK(Wait(envelope.get()));
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        envelope->resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
  // Each wait took at least kDiskDelay; done one after another they would take 4 times that.
  ASSERT_GE(envelope->elapsed(), kDiskDelay);
  ASSERT_LT(envelope->elapsed(), 2500ms);
}

// A sub-request whose tablet's update_mutex_ is held does not hold up its envelope-mates, and
// succeeds once the mutex is free.
TEST_F(MultiRaftUpdateConsensusTest, HeldUpdateMutexDoesNotBlockEnvelopeMates) {
  ASSERT_NO_FATALS(StartServer(3));
  TestThreadHolder thread_holder;
  std::atomic<bool> direct_done{false};
  ASSERT_NO_FATALS(HoldUpdateMutex(&tablets_[0], 3s, &thread_holder, &direct_done));

  auto envelope = SendAsync(
      {Heartbeat(tablets_[0]), Data(&tablets_[1], 1, true), Data(&tablets_[2], 1, true)});
  ASSERT_OK(WaitForLastReceived(tablets_[1], 1s));
  ASSERT_OK(WaitForLastReceived(tablets_[2], 1s));
  // Still held: the envelope waits for its first sub-request.
  ASSERT_FALSE(direct_done.load());
  ASSERT_FALSE(envelope->finished());

  ASSERT_OK(Wait(envelope.get()));
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        envelope->resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
  thread_holder.JoinAll();
}

// While a sub-request waits for its own slow log, the follower neither starts an election nor
// grants a vote, although the wait is longer than the election timeout and no heartbeat arrives.
TEST_F(MultiRaftUpdateConsensusTest, SlowLogNoElection) {
  ASSERT_NO_FATALS(StartServer(1));
  auto& tablet = tablets_[0];
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_use_preelection) = false;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_leader_failure_detection) = true;
  // A new term makes the replica follow the test again, which starts its failure detector.
  term_ = 2;
  ASSERT_NO_FATALS(AssertSlotOk(
      ASSERT_RESULT(Send({Heartbeat(tablet)})).consensus_response(0), tablet.last));
  ASSERT_EQ(CurrentTerm(tablet), 2);

  constexpr auto kDiskDelay = 2500ms;
  SetSlowDisk(tablet, kDiskDelay);
  auto envelope = SendAsync({Data(&tablet, 1, false)});
  SleepFor(1200ms);
  ASSERT_FALSE(envelope->finished());
  auto vote = ASSERT_RESULT(RequestVote(MakeVoteRequest(tablet, 3, false)));
  ASSERT_FALSE(vote.vote_granted()) << vote.ShortDebugString();
  ASSERT_EQ(vote.consensus_error().code(), consensus::ConsensusErrorPB::LEADER_IS_ALIVE)
      << vote.ShortDebugString();

  ASSERT_OK(Wait(envelope.get()));
  ASSERT_GE(envelope->elapsed(), kDiskDelay - 500ms);
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(0), tablet.last));
  ASSERT_EQ(CurrentTerm(tablet), 2);
}

// A term change while a sub-request waits for its log fails that sub-request without waiting for
// the log.
TEST_F(MultiRaftUpdateConsensusTest, TermChangeWhileWaitingForLog) {
  ASSERT_NO_FATALS(StartServer(1));
  auto& tablet = tablets_[0];
  SetSlowDisk(tablet, 3s);
  auto envelope = SendAsync({Data(&tablet, 1, false)});
  ASSERT_OK(WaitForLastReceived(tablet));

  auto vote = ASSERT_RESULT(RequestVote(MakeVoteRequest(tablet, 2, true)));
  ASSERT_TRUE(vote.vote_granted()) << vote.ShortDebugString();
  ASSERT_EQ(CurrentTerm(tablet), 2);
  ASSERT_FALSE(envelope->finished());

  ASSERT_OK(Wait(envelope.get()));
  ASSERT_LT(envelope->elapsed(), 2s);
  ASSERT_NO_FATALS(AssertSlotError(
      envelope->resp.consensus_response(0), AppStatusPB::ILLEGAL_STATE));
  ASSERT_STR_CONTAINS(
      envelope->resp.consensus_response(0).error().status().message(), "Term changed");
}

namespace {

class PostUpdateRecorder : public consensus::Consensus::ConsensusFaultHooks {
 public:
  PostUpdateRecorder(log::Log* log, OpId op_id) : log_(log), op_id_(op_id) {}

  Status PostUpdate() override {
    std::lock_guard lock(mutex_);
    durable_at_post_update_.push_back(log_->IsSafeOpIdToApply(op_id_));
    auto* thread = Thread::current_thread();
    thread_names_.push_back(thread ? thread->name() : "");
    return Status::OK();
  }

  std::vector<std::string> thread_names() {
    std::lock_guard lock(mutex_);
    return thread_names_;
  }

  std::vector<bool> durable_at_post_update() {
    std::lock_guard lock(mutex_);
    return durable_at_post_update_;
  }

 private:
  log::Log* const log_;
  const OpId op_id_;
  std::mutex mutex_;
  std::vector<bool> durable_at_post_update_;
  std::vector<std::string> thread_names_;
};

} // namespace

// A pending election and POST_UPDATE run only after the sub-request's ops are durable.
TEST_F(MultiRaftUpdateConsensusTest, PostDurabilityStepsAfterDurability) {
  ASSERT_NO_FATALS(StartServer(1));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_use_preelection) = false;
  auto& tablet = tablets_[0];
  auto consensus = Consensus(tablet);
  auto request = Data(&tablet, 2, /* commit= */ true);
  // An election that waits for the commit of an op that has not arrived yet, as after a leader
  // stepdown.
  ASSERT_OK(consensus->StartElection(consensus::LeaderElectionData{
      .mode = consensus::ElectionMode::ELECT_EVEN_IF_LEADER_IS_ALIVE,
      .pending_commit = true,
      .must_be_committed_opid = tablet.last}));
  ASSERT_EQ(CurrentTerm(tablet), 1);

  auto hooks = std::make_shared<PostUpdateRecorder>(tablet.peer->log(), tablet.last);
  consensus->SetFaultHooks(hooks);
  constexpr auto kDiskDelay = 1s;
  SetSlowDisk(tablet, kDiskDelay);
  auto envelope = SendAsync({request});
  ASSERT_OK(WaitFor(
      [&consensus, &tablet] { return consensus->GetLastCommittedOpId() == tablet.last; }, 10s,
      "Pending election op committed"));
  // Committed but not durable yet: the election waits for the log.
  ASSERT_FALSE(envelope->finished());
  ASSERT_EQ(CurrentTerm(tablet), 1);
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_GE(envelope->elapsed(), kDiskDelay);
  // Had the election started before durability, the term change would have failed the wait.
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(0), tablet.last));
  ASSERT_EQ(hooks->durable_at_post_update(), std::vector<bool>{true});
  // Not on the log appender thread, which runs group commit for every tablet of the log.
  auto thread_names = hooks->thread_names();
  ASSERT_EQ(thread_names.size(), 1);
  LOG(INFO) << "POST_UPDATE ran on thread " << thread_names[0];
  ASSERT_STR_CONTAINS(thread_names[0], "raft_notifications");
  ASSERT_OK(WaitFor([this, &tablet] { return CurrentTerm(tablet) == 2; }, 10s, "Election"));
}

// Heartbeats in an envelope advance the commit index; this is all a follower gets from a leader
// that batches only heartbeats.
TEST_F(MultiRaftUpdateConsensusTest, HeartbeatOnlyEnvelope) {
  ASSERT_NO_FATALS(StartServer(3));
  std::vector<ConsensusRequestPB> requests;
  for (auto& tablet : tablets_) {
    requests.push_back(Data(&tablet, 2, /* commit= */ false));
  }
  ASSERT_RESULT(Send(std::move(requests)));
  for (auto& tablet : tablets_) {
    ASSERT_EQ(Consensus(tablet)->GetLastCommittedOpId(), OpId::Min());
  }

  const auto concurrent_before = ConcurrentSubRequests();
  requests.clear();
  for (auto& tablet : tablets_) {
    tablet.committed = tablet.last;
    requests.push_back(Heartbeat(tablet));
  }
  auto resp = ASSERT_RESULT(Send(std::move(requests)));
  ASSERT_EQ(ConcurrentSubRequests() - concurrent_before, tablets_.size());
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
    ASSERT_OK(WaitFor(
        [this, i] { return Consensus(tablets_[i])->GetLastCommittedOpId() == tablets_[i].last; },
        10s, "Commit index advances"));
  }
}

// A heartbeat whose log wait cannot finish answers within about one heartbeat interval, not at
// the far-out deadline of a heartbeat envelope.
TEST_F(MultiRaftUpdateConsensusTest, HeartbeatWaitBoundedByInterval) {
  ASSERT_NO_FATALS(StartServer(1));
  auto& tablet = tablets_[0];
  constexpr auto kDiskDelay = 3s;
  SetSlowDisk(tablet, kDiskDelay);
  auto data = SendAsync({Data(&tablet, 1, false)});
  ASSERT_OK(WaitForLastReceived(tablet));

  // The heartbeat waits for last_received, which is the data op that is not durable yet.
  auto heartbeat = SendAsync({Heartbeat(tablet)}, 60s);
  ASSERT_OK(Wait(heartbeat.get()));
  ASSERT_LT(heartbeat->elapsed(), 1500ms);
  ASSERT_NO_FATALS(AssertSlotError(heartbeat->resp.consensus_response(0), AppStatusPB::TIMED_OUT));
  ASSERT_FALSE(data->finished());

  ASSERT_OK(Wait(data.get()));
  ASSERT_NO_FATALS(AssertSlotOk(data->resp.consensus_response(0), tablet.last));
}

// Heartbeats and data requests for different tablets in one envelope.
TEST_F(MultiRaftUpdateConsensusTest, MixedEnvelope) {
  ASSERT_NO_FATALS(StartServer(3));
  ASSERT_RESULT(Send({Data(&tablets_[2], 2, false)}));
  tablets_[2].committed = tablets_[2].last;
  auto resp = ASSERT_RESULT(Send(
      {Heartbeat(tablets_[0]), Data(&tablets_[1], 3, true), Heartbeat(tablets_[2])}));
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
  ASSERT_OK(WaitFor(
      [this] { return Consensus(tablets_[2])->GetLastCommittedOpId() == tablets_[2].last; },
      10s, "Commit index advances"));
}

// An envelope can carry a heartbeat and a data request for the same tablet (the heartbeat was
// parked in the leader's batch when the data request joined it). Both request orders, each forced
// into both execution orders, are correct. That the leader drops the heartbeat's response is
// covered by StaleHeartbeatResponseDroppedAfterBatchedSend in consensus_peers-test.
TEST_F(MultiRaftUpdateConsensusTest, SameTabletHeartbeatAndData) {
  struct Case {
    bool heartbeat_first_in_request;
    bool heartbeat_runs_first;
  };
  const std::vector<Case> cases = {
      {true, true}, {true, false}, {false, true}, {false, false}};
  ASSERT_NO_FATALS(StartServer(cases.size()));

  struct Order {
    std::mutex mutex;
    TabletId tablet_id;
    OpId heartbeat_preceding;
    OpId data_first_op;
    bool heartbeat_runs_first = false;
    std::unique_ptr<CountDownLatch> first_done;
    std::vector<bool> ran_heartbeat;
  } order;
  // Only the two requests of the current case: the task of an earlier request can still reach a
  // sync point after its response has been received.
  auto in_case = [&order](const consensus::LWConsensusRequestPB& req) {
    if (req.tablet_id() != order.tablet_id) {
      return false;
    }
    return req.ops().empty()
        ? OpId::FromPB(req.preceding_id()) == order.heartbeat_preceding
        : OpId::FromPB(req.ops().front().id()) == order.data_first_op;
  };
  auto is_first = [&order](const consensus::LWConsensusRequestPB& req) {
    return req.ops().empty() == order.heartbeat_runs_first;
  };
  auto* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("MultiRaftUpdateTask::BeforeUpdate", [&](void* arg) {
    auto& req = *static_cast<consensus::LWConsensusRequestPB*>(arg);
    CountDownLatch* first_done;
    {
      std::lock_guard lock(order.mutex);
      if (!in_case(req) || is_first(req)) {
        return;
      }
      first_done = order.first_done.get();
    }
    ASSERT_TRUE(first_done->WaitFor(10s));
  });
  sync_point->SetCallBack("MultiRaftUpdateTask::AfterUpdate", [&](void* arg) {
    auto& req = *static_cast<consensus::LWConsensusRequestPB*>(arg);
    std::lock_guard lock(order.mutex);
    if (!in_case(req)) {
      return;
    }
    order.ran_heartbeat.push_back(req.ops().empty());
    if (is_first(req)) {
      order.first_done->CountDown();
    }
  });
  sync_point->EnableProcessing();

  for (size_t i = 0; i != cases.size(); ++i) {
    const auto& test_case = cases[i];
    SCOPED_TRACE(Format(
        "heartbeat first in request: $0, heartbeat runs first: $1",
        test_case.heartbeat_first_in_request, test_case.heartbeat_runs_first));
    auto& tablet = tablets_[i];
    ASSERT_RESULT(Send({Data(&tablet, 1, true)}));
    const auto before_data = tablet.last;
    const auto committed_before = tablet.committed;
    // Built as the leader built it: before the data request, so with the older preceding id and
    // commit index.
    auto heartbeat = Heartbeat(tablet);
    auto data = Data(&tablet, 2, /* commit= */ true);
    {
      std::lock_guard lock(order.mutex);
      order.tablet_id = tablet.id;
      order.heartbeat_preceding = before_data;
      order.data_first_op = OpId::FromPB(data.ops(0).id());
      order.heartbeat_runs_first = test_case.heartbeat_runs_first;
      order.first_done = std::make_unique<CountDownLatch>(1);
      order.ran_heartbeat.clear();
    }
    const int heartbeat_slot = test_case.heartbeat_first_in_request ? 0 : 1;
    auto resp = ASSERT_RESULT(Send(test_case.heartbeat_first_in_request
        ? std::vector<ConsensusRequestPB>{heartbeat, data}
        : std::vector<ConsensusRequestPB>{data, heartbeat}));
    {
      std::lock_guard lock(order.mutex);
      ASSERT_EQ(order.ran_heartbeat,
                (std::vector<bool>{test_case.heartbeat_runs_first,
                                   !test_case.heartbeat_runs_first}));
    }
    ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(1 - heartbeat_slot), tablet.last));
    // The heartbeat reports what the follower had when it ran.
    ASSERT_NO_FATALS(AssertSlotOk(
        resp.consensus_response(heartbeat_slot),
        test_case.heartbeat_runs_first ? before_data : tablet.last));
    // The data ops are in the log, nothing was truncated, and the commit index did not regress.
    ASSERT_EQ(LastReceived(tablet), tablet.last);
    ASSERT_GE(Consensus(tablet)->GetLastCommittedOpId(), committed_before);
    ASSERT_OK(WaitFor(
        [this, &tablet] { return Consensus(tablet)->GetLastCommittedOpId() == tablet.last; },
        10s, "Commit index advances"));
    // A later heartbeat sees the same state.
    ASSERT_NO_FATALS(AssertSlotOk(
        ASSERT_RESULT(Send({Heartbeat(tablet)})).consensus_response(0), tablet.last));
  }
}

// The flag is read once per RPC: an envelope in progress finishes on the path it started on, in
// both directions, and the next envelope takes the new path.
TEST_F(MultiRaftUpdateConsensusTest, FlagFlipWithEnvelopesInFlight) {
  ASSERT_NO_FATALS(StartServer(2));

  // On to off while a concurrent envelope waits for its log.
  SetSlowDisk(tablets_[0], 1s);
  auto concurrent_before = ConcurrentSubRequests();
  auto envelope = SendAsync({Data(&tablets_[0], 1, false), Heartbeat(tablets_[1])});
  ASSERT_OK(WaitForLastReceived(tablets_[0]));
  ASSERT_EQ(ConcurrentSubRequests() - concurrent_before, 2);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = false;
  ASSERT_FALSE(envelope->finished());
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(1), tablets_[1].last));
  SetSlowDisk(tablets_[0], 0s);
  concurrent_before = ConcurrentSubRequests();
  auto resp = ASSERT_RESULT(Send({Heartbeat(tablets_[0]), Heartbeat(tablets_[1])}));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(0), tablets_[0].last));
  ASSERT_EQ(ConcurrentSubRequests(), concurrent_before);

  // Off to on while a serial envelope is stuck behind a held update_mutex_.
  TestThreadHolder thread_holder;
  std::atomic<bool> direct_done{false};
  constexpr auto kHold = 2s;
  ASSERT_NO_FATALS(HoldUpdateMutex(&tablets_[0], kHold, &thread_holder, &direct_done));
  envelope = SendAsync({Heartbeat(tablets_[0]), Data(&tablets_[1], 1, true)});
  // The serial loop handles the first sub-request first, so the second waits for the mutex too.
  SleepFor(500ms);
  ASSERT_NE(LastReceived(tablets_[1]), tablets_[1].last);
  ASSERT_FALSE(direct_done.load());
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = true;
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_GE(envelope->elapsed(), kHold - 1s);
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(1), tablets_[1].last));
  ASSERT_EQ(ConcurrentSubRequests(), concurrent_before);
  resp = ASSERT_RESULT(Send({Heartbeat(tablets_[0]), Heartbeat(tablets_[1])}));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(1), tablets_[1].last));
  ASSERT_EQ(ConcurrentSubRequests() - concurrent_before, 2);
  thread_holder.JoinAll();
}

// With enable_multi_raft_data_batching off, envelopes are handled serially.
TEST_F(MultiRaftUpdateConsensusTest, FlagOffUsesSerialHandler) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = false;
  ASSERT_NO_FATALS(StartServer(2));
  const auto concurrent_before = ConcurrentSubRequests();
  auto resp = ASSERT_RESULT(Send({Data(&tablets_[0], 1, true), Heartbeat(tablets_[1])}));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(1), tablets_[1].last));
  ASSERT_EQ(ConcurrentSubRequests(), concurrent_before);
}

TEST_F(MultiRaftUpdateConsensusTest, EmptyEnvelope) {
  ASSERT_NO_FATALS(StartServer(1));
  auto resp = ASSERT_RESULT(Send({}));
  ASSERT_EQ(resp.consensus_response_size(), 0);
}

// With every pool thread blocked, queued sub-requests time out at the deadline instead of piling
// up, and once a thread is free the pool serves new envelopes.
TEST_F(MultiRaftUpdateConsensusTest, PoolBackPressure) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_max_threads) = 1;
  ASSERT_NO_FATALS(StartServer(3));
  auto& paused = tablets_[0];
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });

  // The queued sub-requests never run, so the follower keeps the old state of their tablets.
  const auto queued_1 = tablets_[1];
  const auto queued_2 = tablets_[2];
  const auto deadline_responses_before = DeadlineResponses();
  auto envelope = SendAsync(
      {Data(&paused, 1, true), Data(&tablets_[1], 1, true), Data(&tablets_[2], 1, true)}, 2s);
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_EQ(DeadlineResponses() - deadline_responses_before, 1);
  for (int i = 0; i != 3; ++i) {
    ASSERT_NO_FATALS(AssertSlotError(envelope->resp.consensus_response(i), AppStatusPB::TIMED_OUT));
  }

  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(WaitForLastReceived(paused));
  tablets_[1] = queued_1;
  tablets_[2] = queued_2;
  auto resp = ASSERT_RESULT(Send({Heartbeat(tablets_[1]), Heartbeat(tablets_[2])}));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(0), queued_1.last));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(1), queued_2.last));
  resp = ASSERT_RESULT(Send({Data(&tablets_[1], 1, true), Data(&tablets_[2], 1, true)}));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(0), tablets_[1].last));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(1), tablets_[2].last));
}

// The multi_raft_update pool runs in the cgroup of the messenger's high-priority pool, where the
// direct UpdateConsensus path runs, so that QoS treats follower Raft work the same on both paths.
// The cgroup object is not backed by a cgroup file system; only its identity is checked (moving a
// thread into it fails with a warning). tserver_cgroup_manager-test checks the real placement.
TEST(MultiRaftUpdatePoolTest, SameCgroupAsHighPriorityPool) {
  Cgroup root(nullptr, "");
  Cgroup system_high(&root, "no-such-cgroup-multi-raft-update-test");
  auto messenger = ASSERT_RESULT(
      rpc::MessengerBuilder("multi-raft-update-pool-test").SetSystemHighCgroup(&system_high)
          .Build());
  auto shutdown_messenger = ScopeExit([&messenger] { messenger->Shutdown(); });
  MetricRegistry registry;
  auto entity = METRIC_ENTITY_server.Instantiate(&registry, "test");
  ConsensusServiceImpl service(entity, /* tablet_manager= */ nullptr, messenger.get());
  ASSERT_EQ(service.TEST_multi_raft_update_pool().options().cgroup, &system_high);
  ASSERT_EQ(messenger->ThreadPool(rpc::ServicePriority::kHigh).options().cgroup, &system_high);
  service.Shutdown();
}

} // namespace yb::tserver
