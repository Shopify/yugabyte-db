// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugabyteDB development.
//
// Portions Copyright (c) YugabyteDB, Inc.
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
#include <mutex>

#include <gtest/gtest.h>

#include "yb/common/opid.h"
#include "yb/common/schema.h"
#include "yb/common/wire_protocol-test-util.h"

#include "yb/consensus/consensus-test-util.h"
#include "yb/consensus/log.h"
#include "yb/consensus/log_util.h"
#include "yb/consensus/multi_raft_batcher.h"
#include "yb/consensus/multi_raft_batcher-test-util.h"
#include "yb/consensus/opid_util.h"

#include "yb/fs/fs_manager.h"

#include "yb/rpc/messenger.h"

#include "yb/server/hybrid_clock.h"

#include "yb/util/logging_test_util.h"
#include "yb/util/metrics.h"
#include "yb/util/scope_exit.h"
#include "yb/util/status_log.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/test_util.h"
#include "yb/util/threadpool.h"
#include "yb/util/to_stream.h"
#include "yb/util/trace.h"

using namespace std::chrono_literals;

METRIC_DECLARE_entity(server);
METRIC_DECLARE_entity(tablet);

METRIC_DECLARE_counter(multi_raft_data_sends_direct);
METRIC_DECLARE_counter(multi_raft_data_sub_requests);
METRIC_DECLARE_counter(multi_raft_heartbeat_sub_requests);

DECLARE_bool(TEST_enable_remote_bootstrap);
DECLARE_bool(collect_update_consensus_traces);
DECLARE_bool(consensus_force_recover_from_stuck_peer_call);
DECLARE_int32(consensus_rpc_timeout_ms);
DECLARE_int32(consensus_stuck_peer_call_threshold_ms);
DECLARE_bool(enable_consensus_exponential_backoff);
DECLARE_bool(enable_multi_raft_data_batching);
DECLARE_bool(enable_multi_raft_heartbeat_batcher);
DECLARE_uint64(multi_raft_batch_size);
DECLARE_uint64(multi_raft_data_direct_threshold_bytes);
DECLARE_uint64(multi_raft_heartbeat_interval_ms);
DECLARE_uint32(multi_raft_rpc_timeout_ms);
DECLARE_int32(raft_heartbeat_interval_ms);

namespace yb::consensus {

using log::Log;
using log::LogOptions;
using rpc::Messenger;
using rpc::MessengerBuilder;
using std::shared_ptr;
using std::unique_ptr;
using std::string;

const char* kTableId = "test-peers-table";
const char* kTabletId = "test-peers-tablet";
const char* kLeaderUuid = "peer-0";
const char* kFollowerUuid = "peer-1";
const char* kFollower2Uuid = "peer-2";

constexpr int kNeverMs = 3600 * 1000;

std::vector<int64_t> OpIndexes(const ConsensusRequestPB& request) {
  std::vector<int64_t> result;
  for (const auto& op : request.ops()) {
    result.push_back(op.id().index());
  }
  return result;
}

std::vector<int64_t> Range(int64_t first, int64_t last) {
  std::vector<int64_t> result;
  for (auto i = first; i <= last; ++i) {
    result.push_back(i);
  }
  return result;
}

// Records the notifications that the queue sends to the consensus implementation.
class RecordingQueueObserver : public TestRaftConsensusQueueIface {
 public:
  std::vector<int64_t> term_changes() {
    std::lock_guard lock(mutex_);
    return term_changes_;
  }

  std::vector<std::string> failed_followers() {
    std::lock_guard lock(mutex_);
    return failed_followers_;
  }

 protected:
  void NotifyTermChange(int64_t term) override {
    std::lock_guard lock(mutex_);
    term_changes_.push_back(term);
  }

  void NotifyFailedFollower(
      const std::string& uuid, int64_t term, const std::string& reason) override {
    std::lock_guard lock(mutex_);
    failed_followers_.push_back(uuid);
  }

 private:
  std::mutex mutex_;
  std::vector<int64_t> term_changes_;
  std::vector<std::string> failed_followers_;
};

// A follower replica that serves both channels of a peer: direct UpdateConsensus calls, and the
// sub-requests of multi-Raft envelopes. Accepts requests like NoOpTestPeerProxy, and records the
// index of every op it receives, in order.
class TestFollower {
 public:
  explicit TestFollower(std::string uuid) : uuid_(std::move(uuid)) {}

  ConsensusResponsePB Update(const ConsensusRequestPB& request) {
    ConsensusResponsePB response;
    std::lock_guard lock(mutex_);
    response.set_responder_uuid(uuid_);
    auto* status = response.mutable_status();
    if (term_ > request.caller_term()) {
      response.set_responder_term(term_);
      auto* error = status->mutable_error();
      error->set_code(ConsensusErrorPB::INVALID_TERM);
      StatusToPB(STATUS(IllegalState, "Stale term"), error->mutable_status());
    } else {
      response.set_responder_term(request.caller_term());
      if (last_received_ < OpId::FromPB(request.preceding_id())) {
        auto* error = status->mutable_error();
        error->set_code(ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH);
        StatusToPB(STATUS(IllegalState, "LMP mismatch"), error->mutable_status());
      } else if (!request.ops().empty()) {
        for (const auto& op : request.ops()) {
          received_.push_back(op.id().index());
        }
        last_received_ = OpId::FromPB(request.ops().rbegin()->id());
      }
    }
    last_received_.ToPB(status->mutable_last_received());
    last_received_.ToPB(status->mutable_last_received_current_leader());
    status->set_last_committed_idx(last_received_.index);
    return response;
  }

  // Rejects requests from leaders with a lower term.
  void SetTerm(int64_t term) {
    std::lock_guard lock(mutex_);
    term_ = term;
  }

  std::vector<int64_t> received() const {
    std::lock_guard lock(mutex_);
    return received_;
  }

  OpId last_received() const {
    std::lock_guard lock(mutex_);
    return last_received_;
  }

 private:
  const std::string uuid_;
  mutable std::mutex mutex_;
  int64_t term_ = 0;
  OpId last_received_;
  std::vector<int64_t> received_;
};

// The direct channel to a TestFollower. Records the requests, and can hold the response.
class TestFollowerProxy : public TestPeerProxy {
 public:
  TestFollowerProxy(ThreadPool* pool, TestFollower* follower)
      : TestPeerProxy(pool), follower_(follower) {}

  void UpdateAsync(const LWConsensusRequestPB* request,
                   RequestTriggerMode trigger_mode,
                   LWConsensusResponsePB* response,
                   rpc::RpcController* controller,
                   const rpc::ResponseCallback& callback) override {
    // As RpcPeerProxy does, so that the peer's controller looks as it does in production.
    controller->set_timeout(FLAGS_consensus_rpc_timeout_ms * 1ms);
    ConsensusRequestPB request_pb;
    request->ToGoogleProtobuf(&request_pb);
    bool hold;
    {
      std::lock_guard lock(mutex_);
      requests_.push_back(request_pb);
      request_sizes_.push_back(request->SerializedSize());
      hold = hold_;
    }
    response->CopyFrom(follower_->Update(request_pb));
    if (hold) {
      RegisterCallback(Method::kUpdate, callback);
    } else {
      RegisterCallbackAndRespond(Method::kUpdate, callback);
    }
  }

  void RequestConsensusVoteAsync(const VoteRequestPB* request,
                                 VoteResponsePB* response,
                                 rpc::RpcController* controller,
                                 const rpc::ResponseCallback& callback) override {
    LOG(FATAL) << "Not implemented";
  }

  // Holds the responses to later requests until Release().
  void Hold() {
    std::lock_guard lock(mutex_);
    hold_ = true;
  }

  // Responds to the held request, and stops holding.
  void Release() {
    {
      std::lock_guard lock(mutex_);
      hold_ = false;
    }
    Respond(Method::kUpdate);
  }

  bool HasHeldRequest() const {
    std::lock_guard lock(lock_);
    return callbacks_.contains(Method::kUpdate);
  }

  size_t NumRequests() const {
    std::lock_guard lock(mutex_);
    return requests_.size();
  }

  // Requests that carry ops.
  size_t NumDataRequests() const {
    std::lock_guard lock(mutex_);
    return std::count_if(requests_.begin(), requests_.end(), [](const auto& request) {
      return !request.ops().empty();
    });
  }

  ConsensusRequestPB Request(size_t idx) const {
    std::lock_guard lock(mutex_);
    CHECK_LT(idx, requests_.size());
    return requests_[idx];
  }

  size_t RequestSize(size_t idx) const {
    std::lock_guard lock(mutex_);
    CHECK_LT(idx, request_sizes_.size());
    return request_sizes_[idx];
  }

 private:
  TestFollower* const follower_;
  mutable std::mutex mutex_;
  bool hold_ = false;
  std::vector<ConsensusRequestPB> requests_;
  std::vector<size_t> request_sizes_;
};

// A remote peer whose direct channel and batcher both reach the same TestFollower.
struct TestPeer {
  std::shared_ptr<Peer> peer;
  TestFollower* follower;
  TestFollowerProxy* proxy;
};

class ConsensusPeersTest : public YBTest {
 public:
  ConsensusPeersTest()
      : metric_entity_(METRIC_ENTITY_tablet.Instantiate(&metric_registry_, "peer-test")),
        server_metric_entity_(METRIC_ENTITY_server.Instantiate(&metric_registry_, "test-server")),
        schema_(GetSimpleTestSchema()) {
  }

  void SetUp() override {
    YBTest::SetUp();
    MessengerBuilder bld("test");
    messenger_ = ASSERT_RESULT(bld.Build());
    ASSERT_OK(ThreadPoolBuilder("test-raft-pool").Build(&raft_pool_));
    raft_pool_token_ = raft_pool_->NewToken(ThreadPool::ExecutionMode::CONCURRENT);
    ASSERT_OK(ThreadPoolBuilder("log").Build(&log_thread_pool_));
    fs_manager_.reset(new FsManager(env_.get(), GetTestPath("fs_root"), "tserver_test"));

    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->CheckAndOpenFileSystemRoots());
    ASSERT_OK(Log::Open(options_,
                       kTabletId,
                       fs_manager_->GetFirstTabletWalDirOrDie(kTableId, kTabletId),
                       fs_manager_->uuid(),
                       schema_,
                       /*schema_version=*/0,
                       /*table_metric_entity=*/nullptr,
                       /*tablet_metric_entity=*/nullptr,
                       /*read_wal_mem_tracker=*/nullptr,
                       log_thread_pool_.get(),
                       log_thread_pool_.get(),
                       log_thread_pool_.get(),
                       &log_));
    clock_.reset(new server::HybridClock());
    ASSERT_OK(clock_->Init());

    consensus_.reset(new RecordingQueueObserver());
    raft_notifications_pool_ = std::make_unique<rpc::ThreadPool>(rpc::ThreadPoolOptions {
      .name = "raft_notifications",
      .max_workers = rpc::ThreadPoolOptions::kUnlimitedWorkers
    });
    message_queue_.reset(new PeerMessageQueue(
        metric_entity_,
        log_.get(),
        /*server_tracker=*/nullptr,
        /*parent_tracker=*/nullptr,
        FakeRaftPeerPB(kLeaderUuid),
        kTabletId,
        clock_,
        /*context=*/nullptr,
        std::make_unique<rpc::Strand>(raft_notifications_pool_.get())));
    message_queue_->RegisterObserver(consensus_.get());

    message_queue_->Init(OpId::Min());
    message_queue_->SetLeaderMode(OpId::Min(),
                                  OpId().term,
                                  OpId::Min(),
                                  OpId(),
                                  BuildRaftConfigPBForTests(3));
  }

  void TearDown() override {
    for (const auto& peer : test_peers_) {
      peer->Close();
    }
    if (batcher_) {
      batcher_->Shutdown();
      // Complete what is still in flight, so that no callback outlives the test.
      for (size_t i = 0; i != sender_state_->NumEnvelopes(); ++i) {
        sender_state_->Complete(i, STATUS(Aborted, "Test shutdown"), {});
      }
    }
    ASSERT_OK(log_->WaitUntilAllFlushed());
    log_thread_pool_->Shutdown();
    raft_pool_->Shutdown();
    if (batcher_messenger_) {
      batcher_messenger_->Shutdown();
    }
    messenger_->Shutdown();
  }

  // Sets the batching flags, and creates the batcher that NewTestPeer gives to peers.
  void SetUpBatcher(
      bool heartbeat_batching, bool data_batching, bool inline_sub_callbacks = false) {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_heartbeat_batcher) = heartbeat_batching;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = data_batching;
    // Peers send only when the test signals them, and the batcher sends heartbeats only with data
    // or at multi_raft_batch_size.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_raft_heartbeat_interval_ms) = kNeverMs;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_heartbeat_interval_ms) = kNeverMs;
    batcher_messenger_ = ASSERT_RESULT(rpc::MessengerBuilder("batcher").Build());
    if (inline_sub_callbacks) {
      // Data sub-callbacks then run inline, on the thread that completes the envelope.
      batcher_messenger_->ThreadPool(rpc::ServicePriority::kHigh).Shutdown();
    }
    batcher_ = std::make_shared<MultiRaftBatcher>(
        batcher_messenger_.get(), std::make_unique<FakeBatchSender>(sender_state_),
        &running_calls_, server_metric_entity_);
    batcher_->Start();
  }

  TestPeer NewTestPeer(const std::string& uuid, bool with_batcher = true) {
    auto& follower = *followers_.emplace(uuid, std::make_unique<TestFollower>(uuid))
        .first->second;
    auto* proxy = new TestFollowerProxy(raft_pool_.get(), &follower);
    auto peer = CHECK_RESULT(Peer::NewRemotePeer(
        FakeRaftPeerPB(uuid), kTabletId, kLeaderUuid, PeerProxyPtr(proxy), message_queue_.get(),
        with_batcher ? batcher_ : nullptr, raft_pool_token_.get(), nullptr /* consensus */,
        messenger_.get()));
    test_peers_.push_back(peer);
    return TestPeer{.peer = peer, .follower = &follower, .proxy = proxy};
  }

  // Sends the peer's first, status-only, request directly and waits until it is processed.
  void Negotiate(const TestPeer& test_peer) {
    ASSERT_OK(test_peer.peer->SignalRequest(RequestTriggerMode::kAlwaysSend));
    ASSERT_OK(WaitFor([&test_peer] {
      return test_peer.proxy->NumRequests() == 1 && !test_peer.peer->TEST_performing_update();
    }, 10s, "Negotiation"));
  }

  void WaitIdle(const TestPeer& test_peer) {
    ASSERT_OK(WaitFor([&test_peer] {
      return !test_peer.peer->TEST_performing_update();
    }, 10s, "Peer idle"));
  }

  size_t NumEnvelopes() {
    return sender_state_->NumEnvelopes();
  }

  std::shared_ptr<FakeBatchEnvelope> GetEnvelope(size_t idx) {
    return sender_state_->GetEnvelope(idx);
  }

  void WaitForEnvelopes(size_t num_envelopes) {
    ASSERT_OK(WaitFor([this, num_envelopes] {
      return NumEnvelopes() >= num_envelopes;
    }, 10s, Format("$0 envelopes", num_envelopes)));
    ASSERT_EQ(NumEnvelopes(), num_envelopes);
  }

  // Answers each sub-request from its follower.
  FakeBatchSenderState::FillResponse FollowerResponses() {
    return [this](size_t, const ConsensusRequestPB& request, ConsensusResponsePB* response) {
      *response = followers_.at(request.dest_uuid())->Update(request);
    };
  }

  // Completes the envelope with status. With an OK status, each sub-request is answered by its
  // follower.
  void CompleteEnvelope(size_t idx, const Status& status = Status::OK()) {
    ASSERT_TRUE(sender_state_->Complete(idx, status, FollowerResponses()));
  }

  // Completes the envelope with the given response to each sub-request.
  void CompleteEnvelopeWith(size_t idx, const ConsensusResponsePB& response) {
    ASSERT_TRUE(sender_state_->Complete(
        idx, Status::OK(),
        [&response](size_t, const ConsensusRequestPB&, ConsensusResponsePB* out) {
          *out = response;
        }));
  }

  // Completes envelopes as they are sent, until the peer has nothing outstanding.
  void CompleteUntilIdle(const TestPeer& test_peer) {
    ASSERT_OK(WaitFor([this, &test_peer] {
      bool completed = false;
      for (size_t i = 0; i != NumEnvelopes(); ++i) {
        completed |= sender_state_->Complete(i, Status::OK(), FollowerResponses());
      }
      return !completed && !test_peer.peer->TEST_performing_update();
    }, 10s, "Peer idle"));
  }

  int64_t CounterValue(const CounterPrototype& proto) {
    return proto.Instantiate(server_metric_entity_)->value();
  }

  // Sends a heartbeat and then a data request through the batcher, each in its own envelope,
  // and leaves both outstanding: the heartbeat in envelope 1, the data in envelope 2.
  void SendBatchedHeartbeatThenData(const TestPeer& test_peer);

  void TestEnvelopeFailure(const Status& status);

  DelayablePeerProxy<NoOpTestPeerProxy>* NewRemotePeer(
      const string& peer_name,
      std::shared_ptr<Peer>* peer) {
    RaftPeerPB peer_pb;
    peer_pb.set_permanent_uuid(peer_name);
    auto proxy_ptr = new DelayablePeerProxy<NoOpTestPeerProxy>(
        raft_pool_.get(), new NoOpTestPeerProxy(raft_pool_.get(), peer_pb));
    *peer = CHECK_RESULT(Peer::NewRemotePeer(
        peer_pb, kTabletId, kLeaderUuid, PeerProxyPtr(proxy_ptr), message_queue_.get(),
        nullptr /* multi raft batcher */, raft_pool_token_.get(),
        nullptr /* consensus */, messenger_.get()));
    return proxy_ptr;
  }

  void CheckLastLogEntry(int64_t term, int64_t index) {
    ASSERT_EQ(log_->GetLatestEntryOpId(), OpId(term, index));
  }

  void CheckLastRemoteEntry(
      DelayablePeerProxy<NoOpTestPeerProxy>* proxy, int64_t term, int64_t index) {
    ASSERT_EQ(proxy->proxy()->last_received(), OpId(term, index));
  }

 protected:
  unique_ptr<ThreadPool> raft_pool_;
  std::unique_ptr<RecordingQueueObserver> consensus_;
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  scoped_refptr<MetricEntity> server_metric_entity_;
  std::unique_ptr<FsManager> fs_manager_;
  unique_ptr<ThreadPool> log_thread_pool_;
  scoped_refptr<Log> log_;
  std::unique_ptr<PeerMessageQueue> message_queue_;
  const Schema schema_;
  LogOptions options_;
  unique_ptr<ThreadPoolToken> raft_pool_token_;
  scoped_refptr<server::Clock> clock_;
  std::unique_ptr<Messenger> messenger_;
  std::unique_ptr<rpc::ThreadPool> raft_notifications_pool_;

  std::unique_ptr<Messenger> batcher_messenger_;
  std::atomic<int> running_calls_{0};
  std::shared_ptr<FakeBatchSenderState> sender_state_ = std::make_shared<FakeBatchSenderState>();
  std::shared_ptr<MultiRaftBatcher> batcher_;
  // Outlive the peers, whose proxies reference them.
  std::map<std::string, std::unique_ptr<TestFollower>> followers_;
  std::vector<std::shared_ptr<Peer>> test_peers_;
};

// Tests that a remote peer is correctly built and tracked
// by the message queue.
// After the operations are considered done the proxy (which
// simulates the other endpoint) should reflect the replicated
// messages.
TEST_F(ConsensusPeersTest, TestRemotePeer) {
  // We use a majority size of 2 since we make one fake remote peer
  // in addition to our real local log.

  std::shared_ptr<Peer> remote_peer;
  auto se = ScopeExit([&remote_peer] {
    // This guarantees that the Peer object doesn't get destroyed if there is a pending request.
    remote_peer->Close();
  });

  DelayablePeerProxy<NoOpTestPeerProxy>* proxy = NewRemotePeer(kFollowerUuid, &remote_peer);

  // Append a bunch of messages to the queue.
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 20);

  // The above append ends up appending messages in term 2, so we update the peer's term to match.
  ThreadSafeArena arena;
  remote_peer->TEST_SetTerm(2, &arena);

  // signal the peer there are requests pending.
  ASSERT_OK(remote_peer->SignalRequest(RequestTriggerMode::kNonEmptyOnly));

  // Now wait on the status of the last operation. This will complete once the peer has logged all
  // requests.
  consensus_->WaitForMajorityReplicatedIndex(20);
  // Verify that the replicated watermark corresponds to the last replicated message.
  CheckLastRemoteEntry(proxy, 2, 20);
}

TEST_F(ConsensusPeersTest, TestLocalAppendAndRemotePeerDelay) {
  // Create a set of remote peers.
  std::shared_ptr<Peer> remote_peer1;
  NewRemotePeer("peer-1", &remote_peer1);

  std::shared_ptr<Peer> remote_peer2;
  DelayablePeerProxy<NoOpTestPeerProxy>* remote_peer2_proxy =
      NewRemotePeer("peer-2", &remote_peer2);

  auto se = ScopeExit([&remote_peer1, &remote_peer2] {
    // This guarantees that the Peer objects don't get destroyed if there is a pending request.
    remote_peer1->Close();
    remote_peer2->Close();
  });

  // Delay the response from the second remote peer.
  const auto kAppendDelayTime = 1s;
  log_->TEST_SetSleepDuration(kAppendDelayTime);
  remote_peer2_proxy->DelayResponse();
  auto se2 = ScopeExit([this, &remote_peer2_proxy] {
    log_->TEST_SetSleepDuration(0s);
    remote_peer2_proxy->Respond(TestPeerProxy::Method::kUpdate);
  });

  // Append one message to the queue.
  const auto start_time = MonoTime::Now();
  OpIdPB first = MakeOpId(0, 1);
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, first.index(), 1);

  ASSERT_OK(remote_peer1->SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_OK(remote_peer2->SignalRequest(RequestTriggerMode::kNonEmptyOnly));

  // Replication should time out, because of the delayed response.
  consensus_->WaitForMajorityReplicatedIndex(first.index());
  const auto elapsed_time = MonoTime::Now() - start_time;
  LOG(INFO) << "Replication elapsed time: " << elapsed_time;
  // Replication should take at least as much time as it takes the local peer to append, because
  // there is only one remote peer that is responding.
  ASSERT_GE(elapsed_time, kAppendDelayTime);
}

TEST_F(ConsensusPeersTest, TestRemotePeers) {
  // Create a set of remote peers.
  std::shared_ptr<Peer> remote_peer1;
  DelayablePeerProxy<NoOpTestPeerProxy>* remote_peer1_proxy =
      NewRemotePeer("peer-1", &remote_peer1);

  std::shared_ptr<Peer> remote_peer2;
  DelayablePeerProxy<NoOpTestPeerProxy>* remote_peer2_proxy =
      NewRemotePeer("peer-2", &remote_peer2);

  auto se = ScopeExit([&remote_peer1, &remote_peer2] {
    // This guarantees that the Peer objects don't get destroyed if there is a pending request.
    remote_peer1->Close();
    remote_peer2->Close();
  });

  // Delay the response from the second remote peer.
  remote_peer2_proxy->DelayResponse();

  // Append one message to the queue.
  OpId first(0, 1);

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, first.index, 1);

  ASSERT_OK(remote_peer1->SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_OK(remote_peer2->SignalRequest(RequestTriggerMode::kNonEmptyOnly));

  // Now wait for the message to be replicated, this should succeed since
  // majority = 2 and only one peer was delayed. The majority is made up
  // of remote-peer1 and the local log.
  consensus_->WaitForMajorityReplicatedIndex(first.index);

  SCOPED_TRACE(Format(
      "Written to log locally: $0, Received by peer1: {$1}, by peer2: {$2}",
      log_->GetLatestEntryOpId(),
      remote_peer1_proxy->proxy()->last_received(),
      remote_peer2_proxy->proxy()->last_received()));

  CheckLastLogEntry(first.term, first.index);
  CheckLastRemoteEntry(remote_peer1_proxy, first.term, first.index);

  remote_peer2_proxy->Respond(TestPeerProxy::Method::kUpdate);
  // Wait until all peers have replicated the message, otherwise
  // when we add the next one remote_peer2 might find the next message
  // in the queue and will replicate it, which is not what we want.
  while (message_queue_->TEST_GetAllReplicatedIndex() != first) {
    std::this_thread::sleep_for(1ms);
  }

  // Now append another message to the queue.
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 2, 1);

  // We should not see it replicated, even after 10ms,
  // since only the local peer replicates the message.
  SleepFor(MonoDelta::FromMilliseconds(10));
  ASSERT_FALSE(consensus_->IsMajorityReplicated(2));

  // Signal one of the two remote peers.
  ASSERT_OK(remote_peer1->SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  // We should now be able to wait for it to replicate, since two peers (a majority)
  // have replicated the message.
  consensus_->WaitForMajorityReplicatedIndex(2);
}

// Regression test for KUDU-699: even if a peer isn't making progress,
// and thus always has data pending, we should be able to close the peer.
TEST_F(ConsensusPeersTest, TestCloseWhenRemotePeerDoesntMakeProgress) {
  auto mock_proxy = new MockedPeerProxy(raft_pool_.get());
  auto peer = ASSERT_RESULT(Peer::NewRemotePeer(
      FakeRaftPeerPB(kFollowerUuid), kTabletId, kLeaderUuid, PeerProxyPtr(mock_proxy),
      message_queue_.get(), nullptr /* multi raft batcher */,
      raft_pool_token_.get(), nullptr /* consensus */,
      messenger_.get()));

  // Make the peer respond without making any progress -- it always returns
  // that it has only replicated op 0.0. When we see the response, we always
  // decide that more data is pending, and we want to send another request.
  ConsensusResponsePB peer_resp;
  peer_resp.set_responder_uuid(kFollowerUuid);
  peer_resp.set_responder_term(0);
  peer_resp.mutable_status()->mutable_last_received()->CopyFrom(
      MakeOpId(0, 0));
  peer_resp.mutable_status()->mutable_last_received_current_leader()->CopyFrom(
      MakeOpId(0, 0));
  peer_resp.mutable_status()->set_last_committed_idx(0);

  mock_proxy->set_update_response(peer_resp);

  // Add an op to the queue and start sending requests to the peer.
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 1);
  ASSERT_OK(peer->SignalRequest(RequestTriggerMode::kAlwaysSend));

  // We should be able to close the peer even though it has more data pending.
  peer->Close();
}

TEST_F(ConsensusPeersTest, TestDontSendOneRpcPerWriteWhenPeerIsDown) {
  auto mock_proxy = new MockedPeerProxy(raft_pool_.get());
  auto peer = ASSERT_RESULT(Peer::NewRemotePeer(
      FakeRaftPeerPB(kFollowerUuid), kTabletId, kLeaderUuid, PeerProxyPtr(mock_proxy),
      message_queue_.get(), nullptr /* multi raft batcher */, raft_pool_token_.get(),
      nullptr /* consensus */,
      messenger_.get()));

  auto se = ScopeExit([&peer] {
    // This guarantees that the Peer object doesn't get destroyed if there is a pending request.
    peer->Close();
  });

  // Initial response has to be successful -- otherwise we'll consider the peer "new" and only send
  // heartbeat RPCs.
  ConsensusResponsePB initial_resp;
  initial_resp.set_responder_uuid(kFollowerUuid);
  initial_resp.set_responder_term(0);
  initial_resp.mutable_status()->mutable_last_received()->CopyFrom(
      MakeOpId(1, 1));
  initial_resp.mutable_status()->mutable_last_received_current_leader()->CopyFrom(
      MakeOpId(1, 1));
  initial_resp.mutable_status()->set_last_committed_idx(0);
  mock_proxy->set_update_response(initial_resp);

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 1);
  LOG(INFO) << "Initial SignalRequest";
  ASSERT_OK(peer->SignalRequest(RequestTriggerMode::kAlwaysSend));
  LOG(INFO) << "Initial SignalRequest done";

  // Now wait for the message to be replicated, this should succeed since the local (leader) peer
  // always acks and the follower also acked this time.
  consensus_->WaitForMajorityReplicatedIndex(1);
  LOG(INFO) << "Message replicated, setting error response";

  // Set up the peer to respond with an error.
  ConsensusResponsePB error_resp;
  error_resp.mutable_error()->set_code(tserver::TabletServerErrorPB::UNKNOWN_ERROR);
  StatusToPB(STATUS(NotFound, "fake error"), error_resp.mutable_error()->mutable_status());
  mock_proxy->set_update_response(error_resp);

  // Up to this point we might have sent a lot of updates: we get the response from the fake peer
  // that it accepted our entry, we consider it replicated, and we are trying to tell the fake peer
  // about that, but it replies with the same canned response without bumping up the committed
  // index. We can keep spinning in this loop for a few hundred times until we replace the response
  // with an error. At the end of the test we should just consider the UpdateAsync calls made after
  // this point.
  int initial_update_count = mock_proxy->update_count();

  // Add a bunch of messages to the queue.
  for (int i = 2; i <= 100; i++) {
    AppendReplicateMessagesToQueue(message_queue_.get(), clock_, i, /* count */ 1);
    ASSERT_OK(peer->SignalRequest(RequestTriggerMode::kNonEmptyOnly));
    // Sleep for a longer time during the first iteration so we have a higher chance of handling
    // the response and incrementing failed_attempts_.
    std::this_thread::sleep_for(i == 2 ? 100ms : 2ms);
  }

  LOG(INFO) << YB_EXPR_TO_STREAM(mock_proxy->update_count());
  LOG(INFO) << YB_EXPR_TO_STREAM(initial_update_count);
  // Check that we didn't attempt to send one UpdateConsensus call per
  // Write. 100 writes might have taken a second or two, though, so it's
  // OK to have called UpdateConsensus() a few times due to regularly
  // scheduled heartbeats.
  ASSERT_LT(mock_proxy->update_count() - initial_update_count, 5);
}


// With only data batching on, heartbeats go directly and data goes through the batcher. A batched
// data request holds the update lock until its response, and the follow-up request that the
// response handler sends goes through the batcher too.
TEST_F(ConsensusPeersTest, BatchedDataHoldsUpdateLockUntilResponse) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));
  ASSERT_EQ(NumEnvelopes(), 0);

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 3);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  auto envelope = GetEnvelope(0);
  ASSERT_EQ(envelope->tablet_ids, std::vector<std::string>{kTabletId});
  ASSERT_EQ(OpIndexes(envelope->req.consensus_request(0)), Range(1, 3));
  ASSERT_EQ(envelope->callback_mode, rpc::InvokeCallbackMode::kThreadPoolHigh);
  ASSERT_TRUE(peer.TEST_performing_update());
  ASSERT_EQ(peer.TEST_batched_handle().TEST_controller(), envelope->controller);

  // One round per follower: a new op neither starts a second round nor goes directly.
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 4, 1);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(test_peer.proxy->NumRequests(), 1);
  ASSERT_TRUE(peer.TEST_performing_update());

  // The data sub-callback sends the next request, which goes through the batcher.
  ASSERT_NO_FATALS(CompleteEnvelope(0));
  ASSERT_NO_FATALS(WaitForEnvelopes(2));
  ASSERT_EQ(OpIndexes(GetEnvelope(1)->req.consensus_request(0)), Range(4, 4));
  ASSERT_TRUE(peer.TEST_performing_update());

  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  consensus_->WaitForMajorityReplicatedIndex(4);
  ASSERT_EQ(test_peer.follower->received(), Range(1, 4));
  ASSERT_EQ(peer.failed_attempts(), 0);
  ASSERT_TRUE(peer.TEST_batched_handle().expired());
  ASSERT_EQ(CounterValue(METRIC_multi_raft_data_sub_requests), NumEnvelopes());
  ASSERT_EQ(CounterValue(METRIC_multi_raft_heartbeat_sub_requests), 0);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_data_sends_direct), 0);

  // A heartbeat still goes directly. The first request may still advance the commit index.
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kAlwaysSend));
  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  const auto num_envelopes = NumEnvelopes();
  const auto num_requests = test_peer.proxy->NumRequests();
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kAlwaysSend));
  ASSERT_OK(WaitFor([&test_peer, num_requests] {
    return test_peer.proxy->NumRequests() == num_requests + 1 &&
           !test_peer.peer->TEST_performing_update();
  }, 10s, "Heartbeat"));
  ASSERT_TRUE(test_peer.proxy->Request(num_requests).ops().empty());
  ASSERT_EQ(NumEnvelopes(), num_envelopes);
}

// A data request of at least multi_raft_data_direct_threshold_bytes goes directly, and counts as a
// direct data send. 0 means no threshold.
TEST_F(ConsensusPeersTest, DirectThresholdBoundary) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_direct_threshold_bytes) = 0;
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));

  // Requests carrying one op of the same size have the same size.
  constexpr int64_t kPayloadSize = 100;
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 1, kPayloadSize);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  const auto request_size = GetEnvelope(0)->req.consensus_request(0).ByteSizeLong();
  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_data_sends_direct), 0);

  // Exactly the threshold: direct.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_direct_threshold_bytes) = request_size;
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 2, 1, kPayloadSize);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_OK(WaitFor([&test_peer] {
    return test_peer.proxy->NumRequests() == 2;
  }, 10s, "Direct request"));
  ASSERT_EQ(OpIndexes(test_peer.proxy->Request(1)), Range(2, 2));
  ASSERT_EQ(test_peer.proxy->RequestSize(1), request_size);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_data_sends_direct), 1);
  // A follow-up that only advances the commit index is smaller, so it is batched.
  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));

  // One byte below the threshold: batched.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_direct_threshold_bytes) = request_size + 1;
  const auto num_direct_requests = test_peer.proxy->NumRequests();
  const auto envelope_idx = NumEnvelopes();
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 3, 1, kPayloadSize);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(envelope_idx + 1));
  const auto& request = GetEnvelope(envelope_idx)->req.consensus_request(0);
  ASSERT_EQ(OpIndexes(request), Range(3, 3));
  ASSERT_EQ(request.ByteSizeLong(), request_size);
  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  ASSERT_EQ(test_peer.proxy->NumRequests(), num_direct_requests);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_data_sends_direct), 1);
  ASSERT_EQ(test_peer.follower->received(), Range(1, 3));
}

// With only heartbeat batching on, data goes directly and is not counted as a direct data send.
TEST_F(ConsensusPeersTest, HeartbeatOnlyBatcherSendsDataDirectly) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ true, /* data_batching= */ false));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_batch_size) = 1;
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;

  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kAlwaysSend));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  ASSERT_TRUE(GetEnvelope(0)->req.consensus_request(0).ops().empty());
  ASSERT_EQ(GetEnvelope(0)->callback_mode, rpc::InvokeCallbackMode::kThreadPoolNormal);
  // The heartbeat sub-callback runs on this thread.
  ASSERT_NO_FATALS(CompleteEnvelope(0));
  ASSERT_EQ(test_peer.proxy->NumRequests(), 0);

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 2);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_OK(WaitFor([&test_peer] {
    return test_peer.proxy->NumDataRequests() == 1 && !test_peer.peer->TEST_performing_update();
  }, 10s, "Direct data"));
  consensus_->WaitForMajorityReplicatedIndex(2);
  ASSERT_EQ(test_peer.follower->received(), Range(1, 2));
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_data_sub_requests), 0);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_data_sends_direct), 0);
}

// When the batch RPC fails, batched_response_ still holds an earlier round's response, which must
// not be processed.
TEST_F(ConsensusPeersTest, EnvelopeFailureIgnoresStaleBatchedResponse) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));
  const auto last_received = message_queue_->GetTrackedPeerForTests(kFollowerUuid).last_received;

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 3);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));

  // A well-formed success that acknowledges every op.
  auto& stale = *peer.TEST_batched_response();
  stale.set_responder_uuid(kFollowerUuid);
  stale.set_responder_term(0);
  *stale.mutable_status()->mutable_last_received() = MakeOpId(0, 3);
  *stale.mutable_status()->mutable_last_received_current_leader() = MakeOpId(0, 3);
  stale.mutable_status()->set_last_committed_idx(3);

  ASSERT_NO_FATALS(CompleteEnvelope(0, STATUS(TimedOut, "Envelope timed out")));
  ASSERT_NO_FATALS(WaitIdle(test_peer));
  ASSERT_EQ(peer.failed_attempts(), 1);
  ASSERT_EQ(message_queue_->GetTrackedPeerForTests(kFollowerUuid).last_received, last_received);
  ASSERT_FALSE(consensus_->IsMajorityReplicated(1));
}

void ConsensusPeersTest::TestEnvelopeFailure(const Status& status) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 3);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  ASSERT_NO_FATALS(CompleteEnvelope(0, status));
  ASSERT_NO_FATALS(WaitIdle(test_peer));
  ASSERT_EQ(peer.failed_attempts(), 1);
  ASSERT_TRUE(test_peer.follower->received().empty());

  // As after a direct failure, a new op does not retry; the next heartbeat does.
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 4, 1);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(test_peer.proxy->NumRequests(), 1);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kAlwaysSend));
  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  consensus_->WaitForMajorityReplicatedIndex(4);
  ASSERT_EQ(test_peer.follower->received(), Range(1, 4));
  ASSERT_EQ(peer.failed_attempts(), 0);
}

TEST_F(ConsensusPeersTest, EnvelopeTimeoutReleasesLockAndRetries) {
  TestEnvelopeFailure(STATUS(TimedOut, "Envelope timed out"));
}

TEST_F(ConsensusPeersTest, EnvelopeAbortReleasesLockAndRetries) {
  TestEnvelopeFailure(STATUS(Aborted, "MultiRaft shutdown"));
}

TEST_F(ConsensusPeersTest, WrongServerUuidSubResponse) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 2);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  ConsensusResponsePB response;
  response.mutable_error()->set_code(tserver::TabletServerErrorPB::WRONG_SERVER_UUID);
  StatusToPB(STATUS(InvalidArgument, "Wrong server UUID"),
             response.mutable_error()->mutable_status());
  ASSERT_NO_FATALS(CompleteEnvelopeWith(0, response));
  ASSERT_NO_FATALS(WaitIdle(test_peer));
  ASSERT_EQ(peer.failed_attempts(), 1);
  ASSERT_OK(WaitFor([this] {
    return consensus_->failed_followers() == std::vector<std::string>{kFollowerUuid};
  }, 10s, "Failed follower"));

  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kAlwaysSend));
  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  consensus_->WaitForMajorityReplicatedIndex(2);
  ASSERT_EQ(test_peer.follower->received(), Range(1, 2));
}

// TABLET_NOT_FOUND passes through to the queue, which asks for a remote bootstrap.
TEST_F(ConsensusPeersTest, TabletNotFoundSubResponse) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_enable_remote_bootstrap) = false;
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 2);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  ConsensusResponsePB response;
  response.set_responder_uuid(kFollowerUuid);
  response.mutable_error()->set_code(tserver::TabletServerErrorPB::TABLET_NOT_FOUND);
  StatusToPB(STATUS(NotFound, "Tablet not found"), response.mutable_error()->mutable_status());
  ASSERT_NO_FATALS(CompleteEnvelopeWith(0, response));
  ASSERT_NO_FATALS(WaitIdle(test_peer));
  ASSERT_TRUE(message_queue_->GetTrackedPeerForTests(kFollowerUuid).needs_remote_bootstrap);
  // The follow-up remote bootstrap attempt failed, because it is disabled.
  ASSERT_EQ(peer.failed_attempts(), 1);
  ASSERT_EQ(NumEnvelopes(), 1);
}

// A follower with a higher term rejects a batched request as it rejects a direct one.
TEST_F(ConsensusPeersTest, TermRejectionMatchesDirectPath) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  auto direct = NewTestPeer(kFollowerUuid, /* with_batcher= */ false);
  auto batched = NewTestPeer(kFollower2Uuid);
  ASSERT_NO_FATALS(Negotiate(direct));
  ASSERT_NO_FATALS(Negotiate(batched));
  constexpr int64_t kFollowerTerm = 5;
  direct.follower->SetTerm(kFollowerTerm);
  batched.follower->SetTerm(kFollowerTerm);
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 2);

  ASSERT_OK(direct.peer->SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_OK(WaitFor([&direct] {
    return direct.proxy->NumRequests() == 2 && !direct.peer->TEST_performing_update();
  }, 10s, "Direct request"));
  ASSERT_OK(WaitFor([this] {
    return consensus_->term_changes() == std::vector<int64_t>{kFollowerTerm};
  }, 10s, "Term change"));

  ASSERT_OK(batched.peer->SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  ASSERT_NO_FATALS(CompleteEnvelope(0));
  ASSERT_NO_FATALS(WaitIdle(batched));
  ASSERT_OK(WaitFor([this] {
    return consensus_->term_changes() == std::vector<int64_t>{kFollowerTerm, kFollowerTerm};
  }, 10s, "Term change"));

  for (const auto* test_peer : {&direct, &batched}) {
    const auto uuid = test_peer->peer->peer_pb().permanent_uuid();
    SCOPED_TRACE(uuid);
    ASSERT_EQ(test_peer->peer->failed_attempts(), 0);
    ASSERT_FALSE(message_queue_->GetTrackedPeerForTests(uuid).is_last_exchange_successful);
    ASSERT_TRUE(test_peer->follower->received().empty());
  }
  // Neither sends a follow-up.
  ASSERT_EQ(direct.proxy->NumRequests(), 2);
  ASSERT_EQ(batched.proxy->NumRequests(), 1);
  ASSERT_EQ(NumEnvelopes(), 1);
}

// Turning data batching off while a batched request is outstanding: it completes through the
// batcher, then the next request goes directly. The follower gets every op once, in order.
TEST_F(ConsensusPeersTest, KillSwitchOnToOffMidStream) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 2);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = false;
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 3, 2);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_EQ(test_peer.proxy->NumRequests(), 1);

  ASSERT_NO_FATALS(CompleteEnvelope(0));
  ASSERT_OK(WaitFor([&test_peer] {
    return test_peer.proxy->NumDataRequests() == 1;
  }, 10s, "Direct data"));
  ASSERT_EQ(OpIndexes(test_peer.proxy->Request(1)), Range(3, 4));
  ASSERT_NO_FATALS(WaitIdle(test_peer));
  consensus_->WaitForMajorityReplicatedIndex(4);
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(test_peer.follower->received(), Range(1, 4));
}

// Turning data batching on while a direct request is outstanding: it completes directly, then the
// next request goes through the batcher. The follower gets every op once, in order.
TEST_F(ConsensusPeersTest, KillSwitchOffToOnMidStream) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ false));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));

  test_peer.proxy->Hold();
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 2);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_OK(WaitFor([&test_peer] {
    return test_peer.proxy->HasHeldRequest();
  }, 10s, "Direct data"));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = true;
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 3, 2);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_EQ(NumEnvelopes(), 0);

  test_peer.proxy->Release();
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  ASSERT_EQ(OpIndexes(GetEnvelope(0)->req.consensus_request(0)), Range(3, 4));
  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  consensus_->WaitForMajorityReplicatedIndex(4);
  ASSERT_EQ(test_peer.proxy->NumRequests(), 2);
  ASSERT_EQ(test_peer.follower->received(), Range(1, 4));
}

// After the batcher shuts down, adding a request runs its callback inline, inside
// AddRequestToBatch. That releases the update lock without deadlocking on the peer lock.
TEST_F(ConsensusPeersTest, SendAfterBatcherShutdown) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));
  batcher_->Shutdown();

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 1);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_OK(WaitFor([&peer] {
    return !peer.TEST_performing_update() && peer.failed_attempts() == 1;
  }, 10s, "Aborted"));
  ASSERT_TRUE(peer.TEST_batched_handle().expired());

  // The heartbeat goes directly and succeeds; its data follow-up is aborted again.
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kAlwaysSend));
  ASSERT_OK(WaitFor([&test_peer] {
    return test_peer.proxy->NumRequests() == 2 && !test_peer.peer->TEST_performing_update() &&
           test_peer.peer->failed_attempts() == 1;
  }, 10s, "Aborted again"));
  ASSERT_EQ(NumEnvelopes(), 0);
  ASSERT_TRUE(test_peer.follower->received().empty());
}

// The response, and the next round's send, can complete before AddRequestToBatch returns. The
// handle the peer keeps is then the next round's.
TEST_F(ConsensusPeersTest, SynchronousCompletionKeepsLatestHandle) {
  ASSERT_NO_FATALS(SetUpBatcher(
      /* heartbeat_batching= */ false, /* data_batching= */ true,
      /* inline_sub_callbacks= */ true));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));

  std::atomic<bool> next_round_sent_inside_add{false};
  sender_state_->SetSendHook([this, &next_round_sent_inside_add](size_t envelope_idx) {
    if (envelope_idx != 0) {
      return;
    }
    // Gives the response a follow-up.
    AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 4, 1);
    CompleteEnvelope(0);
    next_round_sent_inside_add = NumEnvelopes() == 2;
  });
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 3);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(2));
  // The first round's add has returned.
  ASSERT_TRUE(raft_pool_->WaitFor(10s));
  ASSERT_TRUE(next_round_sent_inside_add.load());
  ASSERT_EQ(OpIndexes(GetEnvelope(1)->req.consensus_request(0)), Range(4, 4));
  ASSERT_TRUE(peer.TEST_performing_update());
  ASSERT_EQ(peer.TEST_batched_handle().TEST_controller(), GetEnvelope(1)->controller);

  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  ASSERT_EQ(test_peer.follower->received(), Range(1, 4));
}

// The stuck-call detector reads the batch handle while sends and responses replace it.
TEST_F(ConsensusPeersTest, ConcurrentStuckDetectorAndBatchedSends) {
  constexpr int kNumOps = 300;
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));

  std::atomic<bool> stop{false};
  TestThreadHolder holder;
  holder.AddThread([this, &peer] {
    for (int i = 1; i <= kNumOps; ++i) {
      AppendReplicateMessagesToQueue(message_queue_.get(), clock_, i, 1);
      ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
      std::this_thread::sleep_for(100us);
    }
  });
  for (int i = 0; i != 2; ++i) {
    holder.AddThread([&peer, &stop] {
      while (!stop.load()) {
        // Runs the detector whenever the update lock is held.
        ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
        auto handle = peer.TEST_batched_handle();
        handle.timeout();
        handle.finished();
      }
    });
  }
  holder.AddThread([this, &stop] {
    size_t next = 0;
    while (!stop.load()) {
      if (next < NumEnvelopes()) {
        CompleteEnvelope(next++);
      } else {
        std::this_thread::sleep_for(100us);
      }
    }
  });
  ASSERT_OK(WaitFor([&test_peer] {
    return test_peer.follower->last_received().index == kNumOps;
  }, 30s, "All ops received"));
  stop = true;
  holder.JoinAll();

  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  ASSERT_EQ(test_peer.follower->received(), Range(1, kNumOps));
  ASSERT_EQ(peer.failed_attempts(), 0);
  ASSERT_TRUE(peer.TEST_batched_handle().expired());
  LOG(INFO) << "Envelopes: " << NumEnvelopes();
}

// The detector logs at DFATAL, which is fatal in debug builds.
TEST_F(ConsensusPeersTest, YB_NEVER_DEBUG_TEST(StuckBatchIsDetectedAndFailed)) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ false, /* data_batching= */ true));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_rpc_timeout_ms) = 50;
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 2);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  ASSERT_EQ(GetEnvelope(0)->timeout, 50ms);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_consensus_stuck_peer_call_threshold_ms) = 1;

  // Without force recovery the detector only logs.
  StringWaiterLogSink log_waiter("Found an RPC call in stuck state");
  ASSERT_OK(WaitFor([&peer, &log_waiter] {
    EXPECT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
    return log_waiter.IsEventOccurred();
  }, 10s, "Stuck call logged"));
  ASSERT_TRUE(peer.TEST_performing_update());
  ASSERT_FALSE(GetEnvelope(0)->marked_failed);

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_consensus_force_recover_from_stuck_peer_call) = true;
  ASSERT_OK(WaitFor([&peer] {
    EXPECT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
    return !peer.TEST_performing_update();
  }, 10s, "Stuck call failed"));
  ASSERT_TRUE(GetEnvelope(0)->marked_failed);
  ASSERT_EQ(peer.failed_attempts(), 1);

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_consensus_stuck_peer_call_threshold_ms) = kNeverMs;
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kAlwaysSend));
  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  ASSERT_EQ(test_peer.follower->received(), Range(1, 2));
}

void ConsensusPeersTest::SendBatchedHeartbeatThenData(const TestPeer& test_peer) {
  // Otherwise the peer sends no ops while a heartbeat is unacknowledged.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_consensus_exponential_backoff) = false;
  auto& peer = *test_peer.peer;
  // The first request is a heartbeat.
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kAlwaysSend));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  ASSERT_NO_FATALS(CompleteEnvelope(0));
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kAlwaysSend));
  ASSERT_NO_FATALS(WaitForEnvelopes(2));
  ASSERT_TRUE(GetEnvelope(1)->req.consensus_request(0).ops().empty());

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 2);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(3));
  ASSERT_EQ(OpIndexes(GetEnvelope(2)->req.consensus_request(0)), Range(1, 2));
  ASSERT_EQ(test_peer.proxy->NumRequests(), 0);
}

// A heartbeat sent before a batched data request is stale once the data request is sent, and its
// response is dropped.
TEST_F(ConsensusPeersTest, StaleHeartbeatResponseDroppedAfterBatchedSend) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ true, /* data_batching= */ true));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_batch_size) = 1;
  auto test_peer = NewTestPeer(kFollowerUuid);
  ASSERT_NO_FATALS(SendBatchedHeartbeatThenData(test_peer));

  // Heartbeat sub-callbacks run on this thread.
  ASSERT_NO_FATALS(CompleteEnvelope(1, STATUS(TimedOut, "Envelope timed out")));
  ASSERT_EQ(test_peer.peer->failed_attempts(), 0);

  ASSERT_NO_FATALS(CompleteUntilIdle(test_peer));
  ASSERT_EQ(test_peer.follower->received(), Range(1, 2));
  ASSERT_EQ(test_peer.peer->failed_attempts(), 0);
}

// A heartbeat response that arrives while a batched data request is outstanding does not hide
// that request from the detector. The detector logs at DFATAL, which is fatal in debug builds.
TEST_F(ConsensusPeersTest, YB_NEVER_DEBUG_TEST(HeartbeatResponseDoesNotHideStuckBatch)) {
  ASSERT_NO_FATALS(SetUpBatcher(/* heartbeat_batching= */ true, /* data_batching= */ true));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_batch_size) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_rpc_timeout_ms) = 50;
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(SendBatchedHeartbeatThenData(test_peer));
  ASSERT_EQ(GetEnvelope(2)->timeout, 50ms);

  ASSERT_NO_FATALS(CompleteEnvelope(1));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_consensus_stuck_peer_call_threshold_ms) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_consensus_force_recover_from_stuck_peer_call) = true;
  ASSERT_OK(WaitFor([&peer] {
    EXPECT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
    return !peer.TEST_performing_update();
  }, 10s, "Stuck call failed"));
  ASSERT_TRUE(GetEnvelope(2)->marked_failed);
  ASSERT_EQ(peer.failed_attempts(), 1);
}

// Batched requests do not ask for traces, which only the direct UpdateConsensus handler returns.
TEST_F(ConsensusPeersTest, BatchedRequestNotTraced) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_collect_update_consensus_traces) = true;
  ASSERT_NO_FATALS(SetUpBatcher(
      /* heartbeat_batching= */ false, /* data_batching= */ true,
      /* inline_sub_callbacks= */ true));
  auto test_peer = NewTestPeer(kFollowerUuid);
  auto& peer = *test_peer.peer;
  ASSERT_NO_FATALS(Negotiate(test_peer));

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 1);
  ASSERT_OK(peer.SignalRequest(RequestTriggerMode::kNonEmptyOnly));
  ASSERT_NO_FATALS(WaitForEnvelopes(1));
  TracePtr trace(new Trace);
  {
    ADOPT_TRACE(trace.get());
    // The response's follow-up is sent inline, on this thread, with the trace.
    AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 2, 1);
    ASSERT_NO_FATALS(CompleteEnvelope(0));
  }
  ASSERT_EQ(NumEnvelopes(), 2);
  const auto& request = GetEnvelope(1)->req.consensus_request(0);
  ASSERT_EQ(OpIndexes(request), Range(2, 2));
  ASSERT_FALSE(request.has_trace_requested());

  // The same follow-up, sent directly, asks for a trace.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = false;
  {
    ADOPT_TRACE(trace.get());
    AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 3, 1);
    ASSERT_NO_FATALS(CompleteEnvelope(1));
  }
  ASSERT_OK(WaitFor([&test_peer] {
    return test_peer.proxy->NumDataRequests() == 1;
  }, 10s, "Direct data"));
  ASSERT_EQ(OpIndexes(test_peer.proxy->Request(1)), Range(3, 3));
  ASSERT_TRUE(test_peer.proxy->Request(1).trace_requested());
  ASSERT_NO_FATALS(WaitIdle(test_peer));
}

} // namespace yb::consensus
