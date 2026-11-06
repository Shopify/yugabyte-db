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
#include <map>
#include <memory>
#include <mutex>
#include <set>
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
#include "yb/consensus/multi_raft_batcher.h"
#include "yb/consensus/raft_async_wait_sweeper.h"
#include "yb/consensus/raft_consensus.h"
#include "yb/consensus/replicate_msgs_holder.h"

#include "yb/gutil/thread_annotations.h"

#include "yb/rpc/messenger.h"
#include "yb/rpc/rpc_context.h"
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
#include "yb/util/logging_test_util.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/memory/arena.h"
#include "yb/util/metrics.h"
#include "yb/util/random_util.h"
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
DECLARE_bool(enable_multi_raft_streaming_responses);
DECLARE_bool(enforce_xcluster_guarded_lease);
DECLARE_uint64(multi_raft_batch_size);
DECLARE_uint32(multi_raft_data_flush_delay_ms);
DECLARE_uint64(multi_raft_heartbeat_interval_ms);
DECLARE_uint32(multi_raft_rpc_timeout_ms);
DECLARE_bool(multi_raft_update_inline_first);
DECLARE_int32(multi_raft_update_max_threads);
DECLARE_bool(raft_async_wait_use_sweeper);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_bool(TEST_multi_raft_update_fail_partial_frames);
DECLARE_bool(use_preelection);

METRIC_DECLARE_entity(server);
METRIC_DECLARE_histogram(handler_latency_yb_consensus_ConsensusService_MultiRaftUpdateConsensus);
METRIC_DECLARE_counter(multi_raft_envelopes_failed);
METRIC_DECLARE_counter(multi_raft_envelopes_sent);
METRIC_DECLARE_event_stats(multi_raft_frame_slots);
METRIC_DECLARE_counter(multi_raft_frames_received);
METRIC_DECLARE_counter(multi_raft_update_deadline_responses);
METRIC_DECLARE_event_stats(multi_raft_update_frame_slots);
METRIC_DECLARE_counter(multi_raft_update_frames_sent);
METRIC_DECLARE_counter(multi_raft_update_inline_sub_requests);
METRIC_DECLARE_event_stats(multi_raft_update_pool_queue_time_us);
METRIC_DECLARE_event_stats(multi_raft_update_pool_run_time_us);
METRIC_DECLARE_event_stats(multi_raft_update_slot_wait_us);
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

// Every line the leader's batcher logs when a response frame breaks the streaming protocol or
// leaves a request unanswered.
constexpr auto kLeaderFrameErrors =
    "Malformed MultiRaft|has an invalid index|has no response for request|"
    "has no index after partial frames|responses for [0-9]+ requests";

// Kept short so that the tests about election timeouts are fast: the minimum election timeout is
// leader_failure_max_missed_heartbeat_periods (6) heartbeat intervals.
constexpr int kHeartbeatIntervalMs = 100;

// Partial frames received for one envelope, recorded by its partial-response callback on the
// client's reactor thread. Shared with the callback, so it lives as long as the callback does.
struct FrameLog {
  std::mutex mutex;
  std::vector<MultiRaftConsensusResponsePB> partials GUARDED_BY(mutex);
  std::vector<MonoTime> partial_times GUARDED_BY(mutex);
  int parse_errors GUARDED_BY(mutex) = 0;

  size_t num_partials() {
    std::lock_guard lock(mutex);
    return partials.size();
  }
};

struct Envelope {
  consensus::MultiRaftConsensusRequestPB req;
  MultiRaftConsensusResponsePB resp;
  rpc::RpcController controller;
  CountDownLatch done{1};
  MonoTime start;
  MonoTime finish;
  // Set when the envelope was sent with a partial-response callback.
  std::shared_ptr<FrameLog> frames;
  // Partial frames received when the final callback ran: the client runs every partial callback
  // for frames that precede the final before the final callback.
  size_t partials_before_final = 0;

  // The final callback references this envelope.
  ~Envelope() { done.WaitFor(60s); }

  bool finished() const { return done.count() == 0; }
  MonoDelta elapsed() const { return finish - start; }
};

// The frames of a streamed envelope, and the slots merged into request order.
struct StreamedResult {
  MultiRaftConsensusResponsePB merged;
  std::vector<MultiRaftConsensusResponsePB> partials;
  std::vector<MonoTime> partial_times;
  // The final frame as received.
  MultiRaftConsensusResponsePB final_frame;

  std::set<uint32_t> PartialIndices() const {
    std::set<uint32_t> result;
    for (const auto& frame : partials) {
      result.insert(frame.index().begin(), frame.index().end());
    }
    return result;
  }

  std::set<uint32_t> FinalIndices() const {
    return {final_frame.index().begin(), final_frame.index().end()};
  }
};

// Records the test sync points of streamed envelopes, for the first streamed envelope that reaches
// one, and can hold its sender at the n-th SenderBeforeSend or SenderBeforeRelease until Resume().
// It can also hold the sub-request for one tablet before its update until ResumeUpdate().
// Declare it after everything its owner reads from it: on destruction it releases every hold,
// clears every sync-point callback and waits for the callbacks still running.
class StreamSyncPoints {
 public:
  struct Send {
    size_t num_slots;
    bool final;
  };

  struct Completion {
    size_t index;
    bool accepted;
  };

  // hold_send, hold_release: 1-based ordinal of the sync point at which the sender waits; 0 for
  // none.
  explicit StreamSyncPoints(
      size_t hold_send = 0, size_t hold_release = 0, TabletId hold_update = {})
      : hold_send_(hold_send), hold_release_(hold_release), hold_update_(std::move(hold_update)) {
    auto* sync_point = SyncPoint::GetInstance();
    if (!hold_update_.empty()) {
      sync_point->SetCallBack("MultiRaftUpdateTask::BeforeUpdate", [this](void* arg) {
        if (static_cast<consensus::LWConsensusRequestPB*>(arg)->tablet_id() == hold_update_) {
          update_resume_.WaitFor(60s);
        }
      });
    }
    sync_point->SetCallBack("MultiRaftUpdateState::Completed", [this](void* arg) {
      auto& info = *static_cast<MultiRaftUpdateSyncPointArg*>(arg);
      std::lock_guard lock(mutex_);
      if (Track(info.envelope)) {
        completions_.push_back({info.index, info.accepted});
      }
    });
    sync_point->SetCallBack("MultiRaftUpdateState::DeadlineExpired", [this](void* arg) {
      auto& info = *static_cast<MultiRaftUpdateSyncPointArg*>(arg);
      std::lock_guard lock(mutex_);
      if (Track(info.envelope)) {
        deadline_timed_out_.push_back(info.num_slots);
      }
    });
    sync_point->SetCallBack("MultiRaftUpdateState::SenderBeforeSend", [this](void* arg) {
      auto& info = *static_cast<MultiRaftUpdateSyncPointArg*>(arg);
      {
        std::lock_guard lock(mutex_);
        if (!Track(info.envelope)) {
          return;
        }
        sends_.push_back({info.num_slots, info.final});
        if (sends_.size() != hold_send_) {
          return;
        }
        held_ = true;
      }
      Hold();
    });
    sync_point->SetCallBack("MultiRaftUpdateState::SenderBeforeRelease", [this](void* arg) {
      auto& info = *static_cast<MultiRaftUpdateSyncPointArg*>(arg);
      {
        std::lock_guard lock(mutex_);
        if (!Track(info.envelope)) {
          return;
        }
        if (++releases_ != hold_release_) {
          return;
        }
        held_ = true;
      }
      Hold();
    });
    sync_point->EnableProcessing();
  }

  ~StreamSyncPoints() {
    Resume();
    ResumeUpdate();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  void Resume() { resume_.CountDown(); }

  void ResumeUpdate() { update_resume_.CountDown(); }

  Status WaitHeld() {
    return WaitFor([this] {
      std::lock_guard lock(mutex_);
      return held_;
    }, 30s, "Sender held");
  }

  // Waits until request index has completed num times with the given accepted value.
  Status WaitCompleted(size_t index, bool accepted) {
    return WaitFor([this, index, accepted] {
      std::lock_guard lock(mutex_);
      for (const auto& completion : completions_) {
        if (completion.index == index && completion.accepted == accepted) {
          return true;
        }
      }
      return false;
    }, 30s, Format("Request $0 completed, accepted: $1", index, accepted));
  }

  // Waits until the deadline has fired, and returns how many slots it timed out.
  Result<size_t> WaitDeadline() {
    RETURN_NOT_OK(WaitFor([this] {
      std::lock_guard lock(mutex_);
      return !deadline_timed_out_.empty();
    }, 30s, "Deadline fired"));
    std::lock_guard lock(mutex_);
    return deadline_timed_out_.front();
  }

  std::vector<Send> sends() {
    std::lock_guard lock(mutex_);
    return sends_;
  }

  std::vector<Completion> completions() {
    std::lock_guard lock(mutex_);
    return completions_;
  }

 private:
  bool Track(const void* envelope) REQUIRES(mutex_) {
    if (!envelope_) {
      envelope_ = envelope;
    }
    return envelope_ == envelope;
  }

  void Hold() {
    // Bounded, so that a failed test cannot hang the server's shutdown.
    resume_.WaitFor(60s);
  }

  const size_t hold_send_;
  const size_t hold_release_;
  const TabletId hold_update_;
  CountDownLatch resume_{1};
  CountDownLatch update_resume_{1};
  std::mutex mutex_;
  const void* envelope_ GUARDED_BY(mutex_) = nullptr;
  std::vector<Send> sends_ GUARDED_BY(mutex_);
  std::vector<Completion> completions_ GUARDED_BY(mutex_);
  // Slots timed out by each DeadlineExpired.
  std::vector<size_t> deadline_timed_out_ GUARDED_BY(mutex_);
  size_t releases_ GUARDED_BY(mutex_) = 0;
  bool held_ GUARDED_BY(mutex_) = false;
};

// Records, by tablet, the thread on which each sub-request starts its update
// (MultiRaftUpdateTask::BeforeUpdate). On destruction it clears every sync-point callback and
// waits for those still running.
class UpdateThreads {
 public:
  UpdateThreads() {
    sync_point_->SetCallBack("MultiRaftUpdateTask::BeforeUpdate", [this](void* arg) {
      auto& req = *static_cast<consensus::LWConsensusRequestPB*>(arg);
      auto* thread = Thread::current_thread();
      std::lock_guard lock(mutex_);
      threads_[req.tablet_id().ToBuffer()].push_back(thread ? thread->name() : "");
    });
    sync_point_->EnableProcessing();
  }

  // The threads of the updates of tablet_id so far.
  std::vector<std::string> Get(const TabletId& tablet_id) {
    std::lock_guard lock(mutex_);
    return threads_[tablet_id];
  }

 private:
  std::mutex mutex_;
  std::map<std::string, std::vector<std::string>> threads_ GUARDED_BY(mutex_);
  // Destroyed first.
  SyncPointScope sync_point_;
};

// Threads of the consensus service (the messenger's high-priority pool) and of the multi-Raft
// update pool.
constexpr auto kServiceThread = "-high-pri";
constexpr auto kPoolThread = "multi_raft_update";

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
    ShutDownBatcher();
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

  // A request the test, as leader, sends through a MultiRaftBatcher, and what its callback got.
  struct BatchedCall {
    consensus::LWConsensusRequestPB* request = nullptr;
    consensus::MultiRaftBatcher::BatchHandle handle;
    Status status;
    ConsensusResponsePB response;
    bool had_holder = false;
    std::atomic<bool> done{false};
  };

  // A MultiRaftBatcher to the server with the production sender, as the leader's Raft peers use
  // it. It sends data requests only when one is added with nothing in flight, or at
  // multi_raft_batch_size requests.
  void CreateBatcher() {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_heartbeat_interval_ms) = 3600 * 1000;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_flush_delay_ms) = 3600 * 1000;
    batcher_metric_entity_ = METRIC_ENTITY_server.Instantiate(&batcher_metric_registry_, "leader");
    batcher_ = std::make_shared<consensus::MultiRaftBatcher>(
        client_messenger_.get(),
        consensus::CreateMultiRaftBatchSender(
            proxy_cache_.get(), HostPort::FromBoundEndpoint(mini_server_->bound_rpc_addr())),
        &batcher_running_calls_, batcher_metric_entity_);
    batcher_->Start();
  }

  BatchedCall& AddToBatcher(const ConsensusRequestPB& request) {
    auto& call = *batched_calls_.emplace_back(std::make_unique<BatchedCall>());
    call.request = batcher_arena_.NewObject<consensus::LWConsensusRequestPB>(
        &batcher_arena_, request);
    call.handle = batcher_->AddRequestToBatch(
        call.request, consensus::LWReplicateMsgsHolder(),
        [&call](
            const Status& status, const consensus::LWConsensusResponsePB* response,
            std::shared_ptr<void> holder) {
          call.status = status;
          call.had_holder = holder != nullptr;
          if (response) {
            response->ToGoogleProtobuf(&call.response);
          }
          call.done.store(true, std::memory_order_release);
        },
        consensus::MultiRaftBatcher::RequestKind::kData);
    return call;
  }

  static bool IsDone(const BatchedCall& call) {
    return call.done.load(std::memory_order_acquire);
  }

  // Waits until every batched call's callback has run and its batch, which holds the callback, is
  // gone, so that nothing references the calls or their requests.
  void ShutDownBatcher() {
    if (!batcher_) {
      return;
    }
    batcher_->Shutdown();
    for (const auto& call : batched_calls_) {
      CHECK_OK(WaitFor(
          [&call] { return IsDone(*call) && call->handle.expired(); }, 60s,
          "Batched call finished"));
    }
    batcher_.reset();
  }

  int64_t LeaderCounter(const CounterPrototype& prototype) {
    return prototype.Instantiate(batcher_metric_entity_)->value();
  }

  // Sends one write op for each of tablets_[0..2] in one envelope through the batcher, and returns
  // their calls. They are sent by the count cap, behind an envelope for tablets_[3] that is held in
  // flight by pausing that tablet until they are sent.
  void SendThroughBatcher(std::vector<BatchedCall*>* calls) {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_batch_size) = 3;
    auto& blocker_tablet = tablets_[3];
    Consensus(blocker_tablet)->TEST_PauseUpdateConsensus(true);
    BatchedCall* blocker;
    {
      auto unpause = ScopeExit([this, &blocker_tablet] {
        Consensus(blocker_tablet)->TEST_PauseUpdateConsensus(false);
      });
      blocker = &AddToBatcher(Data(&blocker_tablet, 1, /* commit= */ false));
      ASSERT_EQ(LeaderCounter(METRIC_multi_raft_envelopes_sent), 1);
      calls->clear();
      for (size_t i = 0; i != 3; ++i) {
        calls->push_back(&AddToBatcher(Data(&tablets_[i], 1, /* commit= */ false)));
      }
      ASSERT_EQ(LeaderCounter(METRIC_multi_raft_envelopes_sent), 2);
      ASSERT_FALSE(IsDone(*blocker));
    }
    ASSERT_OK(WaitFor([blocker] { return IsDone(*blocker); }, 10s, "Blocker answered"));
    ASSERT_OK(blocker->status);
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

  // One write op per tablet, committed on every tablet but tablets_[0].
  std::vector<ConsensusRequestPB> OneOpPerTablet() {
    std::vector<ConsensusRequestPB> requests;
    for (size_t i = 0; i != tablets_.size(); ++i) {
      requests.push_back(Data(&tablets_[i], 1, /* commit= */ i != 0));
    }
    return requests;
  }

  Result<MultiRaftConsensusResponsePB> Send(
      std::vector<ConsensusRequestPB> requests, MonoDelta timeout = 30s) {
    auto envelope = SendAsync(std::move(requests), timeout);
    RETURN_NOT_OK(Wait(envelope.get()));
    return envelope->resp;
  }

  // Sends an envelope with a partial-response callback that records every partial frame, and with
  // stream_responses set if stream.
  std::unique_ptr<Envelope> SendRecordedAsync(
      std::vector<ConsensusRequestPB> requests, MonoDelta timeout = 30s, bool stream = true) {
    auto envelope = std::make_unique<Envelope>();
    for (auto& request : requests) {
      *envelope->req.add_consensus_request() = std::move(request);
    }
    if (stream) {
      envelope->req.set_stream_responses(true);
    }
    envelope->frames = std::make_shared<FrameLog>();
    envelope->controller.set_timeout(timeout);
    envelope->controller.set_partial_response_callback(
        [frames = envelope->frames](Slice body, RefCntBuffer) {
          MultiRaftConsensusResponsePB frame;
          auto parsed = frame.ParseFromArray(body.data(), narrow_cast<int>(body.size()));
          std::lock_guard lock(frames->mutex);
          if (!parsed) {
            ++frames->parse_errors;
            return;
          }
          frames->partials.push_back(std::move(frame));
          frames->partial_times.push_back(MonoTime::Now());
        });
    envelope->start = MonoTime::Now();
    auto* e = envelope.get();
    consensus_proxy_->MultiRaftUpdateConsensusAsync(
        e->req, &e->resp, &e->controller, [e] {
          {
            std::lock_guard lock(e->frames->mutex);
            e->partials_before_final = e->frames->partials.size();
          }
          e->finish = MonoTime::Now();
          e->done.CountDown();
        });
    return envelope;
  }

  // Waits for a streamed envelope and checks the frame shape: every frame sets index, parallel to
  // its responses; no partial frame is empty or arrives after the final; the final carries at
  // least one slot; and every request is answered exactly once.
  Result<StreamedResult> WaitStreamed(Envelope* envelope) {
    SCHECK(envelope->done.WaitFor(60s), TimedOut, "Envelope did not complete");
    RETURN_NOT_OK(envelope->controller.status());
    StreamedResult result;
    {
      std::lock_guard lock(envelope->frames->mutex);
      SCHECK_EQ(envelope->frames->parse_errors, 0, IllegalState, "Unparsable partial frame");
      result.partials = envelope->frames->partials;
      result.partial_times = envelope->frames->partial_times;
    }
    SCHECK_EQ(
        envelope->partials_before_final, result.partials.size(), IllegalState,
        "Partial frame after the final");
    result.final_frame = envelope->resp;
    const auto num_requests = static_cast<size_t>(envelope->req.consensus_request_size());
    std::vector<const ConsensusResponsePB*> slots(num_requests);
    auto add_frame = [&slots, num_requests](
        const MultiRaftConsensusResponsePB& frame, const char* kind) -> Status {
      SCHECK_EQ(
          frame.index_size(), frame.consensus_response_size(), IllegalState,
          Format("$0 frame: index not parallel to the responses: $1", kind,
                 frame.ShortDebugString()));
      SCHECK_GT(frame.index_size(), 0, IllegalState, Format("Empty $0 frame", kind));
      for (int i = 0; i != frame.index_size(); ++i) {
        const auto index = frame.index(i);
        SCHECK_LT(index, num_requests, IllegalState, Format("$0 frame: bad index", kind));
        SCHECK(!slots[index], IllegalState, Format("Request $0 answered twice", index));
        slots[index] = &frame.consensus_response(i);
      }
      return Status::OK();
    };
    for (const auto& frame : result.partials) {
      RETURN_NOT_OK(add_frame(frame, "partial"));
    }
    RETURN_NOT_OK(add_frame(result.final_frame, "final"));
    for (size_t i = 0; i != num_requests; ++i) {
      SCHECK(slots[i], IllegalState, Format("Request $0 not answered", i));
      *result.merged.add_consensus_response() = *slots[i];
    }
    return result;
  }

  Result<StreamedResult> SendStreamed(
      std::vector<ConsensusRequestPB> requests, MonoDelta timeout = 30s) {
    auto envelope = SendRecordedAsync(std::move(requests), timeout);
    return WaitStreamed(envelope.get());
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

  int64_t FramesSent() {
    return ServerCounter(METRIC_multi_raft_update_frames_sent);
  }

  int64_t InlineSubRequests() {
    return ServerCounter(METRIC_multi_raft_update_inline_sub_requests);
  }

  // Asserts that the only update of tablet ran on a thread whose name contains thread.
  void AssertUpdateThread(UpdateThreads* threads, const TestTablet& tablet, const char* thread) {
    auto names = threads->Get(tablet.id);
    ASSERT_EQ(names.size(), 1) << tablet.id << ": " << AsString(names);
    LOG(INFO) << "Update of " << tablet.id << " ran on " << names[0];
    ASSERT_STR_CONTAINS(names[0], thread);
  }

  consensus::RaftAsyncWaitSweeper& Sweeper() {
    return *mini_server_->server()->tablet_manager()->raft_async_wait_sweeper();
  }

  uint64_t SweeperRegistrations() {
    return Sweeper().TEST_counters().registered;
  }

  // Asserts that durability waits registered with the sweeper since registered_before, at least
  // min_registrations times, exactly when raft_async_wait_use_sweeper is set, so that a run with
  // it set is not vacuous. A wait registers right after it adds its log waiter.
  void AssertSweeperUse(uint64_t registered_before, uint64_t min_registrations = 1) {
    if (FLAGS_raft_async_wait_use_sweeper) {
      ASSERT_OK(WaitFor(
          [this, registered_before, min_registrations] {
            return SweeperRegistrations() >= registered_before + min_registrations;
          },
          5s, "Waits registered with the sweeper"));
    } else {
      ASSERT_EQ(SweeperRegistrations(), registered_before);
    }
  }

  scoped_refptr<EventStats> ServerStats(const EventStatsPrototype& prototype) {
    return prototype.Instantiate(mini_server_->server()->metric_entity());
  }

  // The server records a frame's metrics right after queueing it, so the final callback can run
  // before they are recorded.
  Status WaitForFramesSent(int64_t expected) {
    return WaitFor(
        [this, expected] { return FramesSent() >= expected; }, 10s,
        Format("$0 frames sent", expected));
  }

  Status WaitForStatsCount(const scoped_refptr<EventStats>& stats, uint64_t expected) {
    return WaitFor(
        [&stats, expected] { return stats->TotalCount() >= expected; }, 10s,
        Format("$0 values recorded", expected));
  }

  std::shared_ptr<MemTracker> InboundCallsTracker() {
    auto calls = mini_server_->server()->mem_tracker()->FindChild("Call");
    CHECK(calls);
    auto inbound_calls = calls->FindChild("Inbound RPC");
    CHECK(inbound_calls);
    return inbound_calls;
  }

  // The params of the inbound call of the first envelope handled concurrently from now on.
  class ParamsCapture {
   public:
    ParamsCapture() {
      auto* sync_point = SyncPoint::GetInstance();
      sync_point->SetCallBack("MultiRaftUpdateConsensus::Params", [this](void* arg) {
        std::lock_guard lock(mutex_);
        if (!captured_) {
          params_ = *static_cast<std::shared_ptr<rpc::RpcCallParams>*>(arg);
          captured_ = true;
        }
      });
      sync_point->EnableProcessing();
    }

    ~ParamsCapture() {
      SyncPoint::GetInstance()->SetCallBack("MultiRaftUpdateConsensus::Params", [](void*) {});
    }

    bool captured() {
      std::lock_guard lock(mutex_);
      return captured_;
    }

    bool alive() {
      std::lock_guard lock(mutex_);
      return !params_.expired();
    }

   private:
    std::mutex mutex_;
    bool captured_ = false;
    std::weak_ptr<rpc::RpcCallParams> params_;
  };

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

  // Sends {Heartbeat(tablets_[0]), Data(tablets_[1])} with tablets_[1] paused, deletes tablets_[0]
  // after its sub-request has completed and before the envelope is answered, and checks that its
  // slot still reports this server: a held response must not borrow from the deleted tablet's
  // RaftConsensus. With concurrent, the envelope is answered at its deadline; otherwise the serial
  // loop answers once tablets_[1] is unpaused.
  void TestResponseOutlivesDeletedTablet(bool concurrent) {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = concurrent;
    ASSERT_NO_FATALS(StartServer(2));
    auto& deleted = tablets_[0];
    auto& paused = tablets_[1];
    Consensus(paused)->TEST_PauseUpdateConsensus(true);
    auto unpause = ScopeExit([this, &paused] {
      Consensus(paused)->TEST_PauseUpdateConsensus(false);
    });
    std::weak_ptr<consensus::RaftConsensus> deleted_consensus = Consensus(deleted);
    auto envelope = SendAsync({Heartbeat(deleted), Data(&paused, 1, true)}, 5s);
    // The first sub-request has completed by then; its slot is checked below.
    SleepFor(500ms * kTimeMultiplier);
    ASSERT_FALSE(envelope->finished());
    std::optional<TabletServerErrorPB::Code> error_code;
    ASSERT_OK(mini_server_->server()->tablet_manager()->DeleteTablet(
        deleted.id, tablet::TABLET_DATA_DELETED, tablet::ShouldAbortActiveTransactions::kFalse,
        /* cas_config_opid_index_less_or_equal= */ std::nullopt, /* hide_only= */ false,
        /* keep_data= */ false, &error_code));
    deleted.peer.reset();
    ASSERT_OK(WaitFor(
        [&deleted_consensus] { return deleted_consensus.expired(); }, 10s,
        "RaftConsensus of the deleted tablet destroyed"));
    ASSERT_FALSE(envelope->finished());
    if (!concurrent) {
      Consensus(paused)->TEST_PauseUpdateConsensus(false);
    }
    ASSERT_OK(Wait(envelope.get()));
    const auto& slot = envelope->resp.consensus_response(0);
    ASSERT_NO_FATALS(AssertSlotOk(slot, deleted.last));
    ASSERT_EQ(slot.responder_uuid(), server_uuid_) << slot.ShortDebugString();
    if (concurrent) {
      ASSERT_NO_FATALS(
          AssertSlotError(envelope->resp.consensus_response(1), AppStatusPB::TIMED_OUT));
    } else {
      ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(1), paused.last));
    }
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

  void TestSameTabletHeartbeatAndData(bool stream);

  std::string server_uuid_;
  int64_t term_ = 1;
  std::vector<TestTablet> tablets_;
  MetricRegistry batcher_metric_registry_;
  MetricEntityPtr batcher_metric_entity_;
  std::atomic<int> batcher_running_calls_{0};
  // The requests and callback state of batched calls, which outlive the batcher's batches.
  ThreadSafeArena batcher_arena_;
  std::vector<std::unique_ptr<BatchedCall>> batched_calls_;
  std::shared_ptr<consensus::MultiRaftBatcher> batcher_;
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

// A completed slot held until the envelope is answered stays valid after its tablet is deleted.
TEST_F(MultiRaftUpdateConsensusTest, ConcurrentResponseOutlivesDeletedTablet) {
  TestResponseOutlivesDeletedTablet(/* concurrent= */ true);
}

// The serial handler's slots stay valid until the envelope is answered, after their tablet is
// deleted.
TEST_F(MultiRaftUpdateConsensusTest, SerialResponseOutlivesDeletedTablet) {
  TestResponseOutlivesDeletedTablet(/* concurrent= */ false);
}

// When every sub-request fails before dispatch, the handler thread fills every slot and answers.
TEST_F(MultiRaftUpdateConsensusTest, AllSubRequestsFailBeforeDispatch) {
  ASSERT_NO_FATALS(StartServer(1));
  const auto concurrent_before = ConcurrentSubRequests();
  const auto deadline_responses_before = DeadlineResponses();
  auto wrong_uuid = Heartbeat(tablets_[0]);
  wrong_uuid.set_dest_uuid("no-such-server");
  auto unknown_tablet = Heartbeat(tablets_[0]);
  unknown_tablet.set_tablet_id("no-such-tablet");
  auto envelope = SendAsync({wrong_uuid, unknown_tablet}, 30s);
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_LT(envelope->elapsed(), 5s);
  ASSERT_EQ(ConcurrentSubRequests() - concurrent_before, 2);
  ASSERT_EQ(DeadlineResponses(), deadline_responses_before);
  ASSERT_EQ(envelope->resp.consensus_response(0).error().code(),
            TabletServerErrorPB::WRONG_SERVER_UUID);
  ASSERT_EQ(envelope->resp.consensus_response(1).error().code(),
            TabletServerErrorPB::TABLET_NOT_FOUND);
}

// A tablet shut down while its sub-request waits for the log completes that sub-request with an
// error, well before the log would have become durable.
TEST_F(MultiRaftUpdateConsensusTest, ShutdownWhileWaitingForLog) {
  ASSERT_NO_FATALS(StartServer(2));
  auto& shut_down = tablets_[0];
  SetSlowDisk(shut_down, 3s);
  const auto registered_before = SweeperRegistrations();
  auto envelope = SendAsync({Data(&shut_down, 1, false), Data(&tablets_[1], 1, true)});
  ASSERT_OK(WaitForLastReceived(shut_down));
  ASSERT_FALSE(envelope->finished());
  ASSERT_NO_FATALS(AssertSweeperUse(registered_before));

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
  const auto registered_before = SweeperRegistrations();
  auto envelope = SendAsync({Data(&tablet, 1, false)});
  ASSERT_OK(WaitForLastReceived(tablet));
  ASSERT_FALSE(envelope->finished());
  ASSERT_NO_FATALS(AssertSweeperUse(registered_before));
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
// alive, although it still holds the call's params (which carry its request). The paused
// sub-request ignores the deadline, so it outlives the response.
TEST_F(MultiRaftUpdateConsensusTest, AnsweredEnvelopeReleasedWhileSubRequestPending) {
  ASSERT_NO_FATALS(StartServer(1));
  auto& paused = tablets_[0];
  auto inbound_calls = InboundCallsTracker();
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });
  ParamsCapture params;
  auto envelope = SendAsync({Data(&paused, 1, true)}, 1s);
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_NO_FATALS(AssertSlotError(envelope->resp.consensus_response(0), AppStatusPB::TIMED_OUT));
  ASSERT_TRUE(params.captured());
  ASSERT_OK(WaitFor(
      [&inbound_calls] { return inbound_calls->consumption() == 0; }, 3s,
      "Inbound call released"));
  // The call was released while its params were still held, and the sub-request was still paused
  // the whole time.
  ASSERT_TRUE(params.alive());
  ASSERT_NE(LastReceived(paused), paused.last);

  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(WaitForLastReceived(paused));
  ASSERT_OK(WaitFor([&params] { return !params.alive(); }, 10s, "Params released"));
}

// A sub-request that is still running after the RPC was answered keeps the inbound call's params,
// and with them its request, alive until it completes, and no longer.
TEST_F(MultiRaftUpdateConsensusTest, SubRequestPinsParamsUntilDone) {
  ASSERT_NO_FATALS(StartServer(2));
  auto& paused = tablets_[1];
  auto inbound_calls = InboundCallsTracker();
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });
  const auto deadline_responses_before = DeadlineResponses();
  ParamsCapture params;
  auto envelope = SendAsync({Data(&tablets_[0], 1, true), Data(&paused, 2, true)}, 2s);
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_EQ(DeadlineResponses() - deadline_responses_before, 1);
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotError(envelope->resp.consensus_response(1), AppStatusPB::TIMED_OUT));
  ASSERT_TRUE(params.captured());
  // Once the inbound call is gone, only the paused sub-request can hold the params.
  ASSERT_OK(WaitFor(
      [&inbound_calls] { return inbound_calls->consumption() == 0; }, 3s,
      "Inbound call released"));
  ASSERT_TRUE(params.alive());
  ASSERT_NE(LastReceived(paused), paused.last);

  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(WaitForLastReceived(paused));
  ASSERT_OK(WaitFor([&params] { return !params.alive(); }, 10s, "Params released"));
  // The update ran on the request in the params: the ops it carried are in the log.
  auto resp = ASSERT_RESULT(Send({Heartbeat(tablets_[0]), Heartbeat(paused)}));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(1), paused.last));
}

// A sub-request's wait for its log ends at the envelope deadline even though the log is still
// slow, so the wait does not outlive the response.
TEST_F(MultiRaftUpdateConsensusTest, DurabilityWaitEndsAtDeadline) {
  ASSERT_NO_FATALS(StartServer(1));
  auto& tablet = tablets_[0];
  auto* log = tablet.peer->log();
  SetSlowDisk(tablet, 10s);
  const auto registered_before = SweeperRegistrations();
  auto envelope = SendAsync({Data(&tablet, 1, false)}, 3s);
  ASSERT_OK(WaitFor(
      [log] { return log->TEST_num_safe_op_id_waiters() == 1; }, 2s, "Waiting for the log"));
  ASSERT_NO_FATALS(AssertSweeperUse(registered_before));
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
  const auto registered_before = SweeperRegistrations();
  auto envelope = SendAsync(std::move(requests));
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_NO_FATALS(AssertSweeperUse(registered_before));
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
  const auto registered_before = SweeperRegistrations();
  auto envelope = SendAsync({Data(&tablet, 1, false)});
  SleepFor(1200ms);
  ASSERT_FALSE(envelope->finished());
  // With the sweeper, the wait has re-registered on every tick so far.
  ASSERT_NO_FATALS(AssertSweeperUse(registered_before, /* min_registrations= */ 5));
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
  const auto registered_before = SweeperRegistrations();
  auto envelope = SendAsync({Data(&tablet, 1, false)});
  ASSERT_OK(WaitForLastReceived(tablet));
  ASSERT_NO_FATALS(AssertSweeperUse(registered_before));

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
  const auto registered_before = SweeperRegistrations();
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
  ASSERT_NO_FATALS(AssertSweeperUse(registered_before));
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
  const auto registered_before = SweeperRegistrations();
  auto data = SendAsync({Data(&tablet, 1, false)});
  ASSERT_OK(WaitForLastReceived(tablet));
  ASSERT_NO_FATALS(AssertSweeperUse(registered_before));

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
// into both execution orders, are correct, with the envelope streamed if stream. That the leader
// drops the heartbeat's response is covered by StaleHeartbeatResponseDroppedAfterBatchedSend in
// consensus_peers-test.
void MultiRaftUpdateConsensusTest::TestSameTabletHeartbeatAndData(bool stream) {
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
  // Destroyed first: the callbacks capture the locals above.
  SyncPointScope sync_point_scope;
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
    auto requests = test_case.heartbeat_first_in_request
        ? std::vector<ConsensusRequestPB>{heartbeat, data}
        : std::vector<ConsensusRequestPB>{data, heartbeat};
    MultiRaftConsensusResponsePB resp;
    if (stream) {
      resp = ASSERT_RESULT(SendStreamed(std::move(requests))).merged;
    } else {
      resp = ASSERT_RESULT(Send(std::move(requests)));
    }
    // The envelope can be answered from inside UpdateAsync, before the second request reaches
    // AfterUpdate.
    ASSERT_OK(WaitFor(
        [&order] {
          std::lock_guard lock(order.mutex);
          return order.ran_heartbeat.size() >= 2;
        },
        10s, "Both requests passed AfterUpdate"));
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

TEST_F(MultiRaftUpdateConsensusTest, SameTabletHeartbeatAndData) {
  TestSameTabletHeartbeatAndData(/* stream= */ false);
}

// The streamed frames carry the right index for each of the two requests, in both execution
// orders.
TEST_F(MultiRaftUpdateConsensusTest, StreamedHeartbeatAndDataSameTablet) {
  TestSameTabletHeartbeatAndData(/* stream= */ true);
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

// The serial handler fills each slot in request order from the sub-request's own update, with
// per-sub-request error isolation.
TEST_F(MultiRaftUpdateConsensusTest, SerialHandlerLw) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = false;
  ASSERT_NO_FATALS(StartServer(2));
  const auto concurrent_before = ConcurrentSubRequests();
  auto wrong_uuid = Heartbeat(tablets_[0]);
  wrong_uuid.set_dest_uuid("no-such-server");
  auto first = Data(&tablets_[0], 2, /* commit= */ true);
  auto second = Data(&tablets_[1], 3, /* commit= */ false);
  auto resp = ASSERT_RESULT(Send({first, wrong_uuid, second}));
  ASSERT_EQ(ConcurrentSubRequests(), concurrent_before);
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(0), tablets_[0].last));
  ASSERT_TRUE(resp.consensus_response(1).has_error());
  ASSERT_EQ(resp.consensus_response(1).error().code(), TabletServerErrorPB::WRONG_SERVER_UUID);
  ASSERT_FALSE(resp.consensus_response(1).has_status());
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(2), tablets_[1].last));
  for (int i : {0, 2}) {
    ASSERT_EQ(resp.consensus_response(i).responder_uuid(), server_uuid_)
        << resp.consensus_response(i).ShortDebugString();
    ASSERT_EQ(resp.consensus_response(i).responder_term(), term_)
        << resp.consensus_response(i).ShortDebugString();
  }
  ASSERT_EQ(LastReceived(tablets_[0]), tablets_[0].last);
  ASSERT_EQ(LastReceived(tablets_[1]), tablets_[1].last);
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

// With stream_responses, each response goes out as soon as its sub-request completes: the fast
// slots in partial frames, the slow one in the final. Every frame sets index.
TEST_F(MultiRaftUpdateConsensusTest, StreamedFramesCarryIndices) {
  ASSERT_NO_FATALS(StartServer(3));
  constexpr auto kDiskDelay = 1s;
  SetSlowDisk(tablets_[0], kDiskDelay);
  std::vector<ConsensusRequestPB> requests;
  for (size_t i = 0; i != tablets_.size(); ++i) {
    requests.push_back(Data(&tablets_[i], narrow_cast<int>(i) + 1, /* commit= */ i != 0));
  }
  auto envelope = SendRecordedAsync(std::move(requests));
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  // The slow slot completed last.
  ASSERT_GE(envelope->elapsed(), kDiskDelay);
  ASSERT_EQ(result.PartialIndices(), (std::set<uint32_t>{1, 2}));
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{0}));
  // The fast slots did not wait for it.
  for (auto time : result.partial_times) {
    ASSERT_GT(envelope->finish - time, 500ms);
  }
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        result.merged.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
}

// Completions that arrive while the sender is busy share its next frame.
TEST_F(MultiRaftUpdateConsensusTest, StreamedCoalescesCoincidingCompletions) {
  ASSERT_NO_FATALS(StartServer(4));
  std::unique_ptr<Envelope> envelope;
  for (size_t i = 1; i != tablets_.size(); ++i) {
    Consensus(tablets_[i])->TEST_PauseUpdateConsensus(true);
  }
  auto unpause = ScopeExit([this] {
    for (size_t i = 1; i != tablets_.size(); ++i) {
      Consensus(tablets_[i])->TEST_PauseUpdateConsensus(false);
    }
  });
  const auto frames_before = FramesSent();
  StreamSyncPoints sync_points(/* hold_send= */ 1);
  std::vector<ConsensusRequestPB> requests;
  for (auto& tablet : tablets_) {
    requests.push_back(Data(&tablet, 1, /* commit= */ true));
  }
  envelope = SendRecordedAsync(std::move(requests));
  // The first sender holds slot 0's frame while the other three complete.
  ASSERT_OK(sync_points.WaitHeld());
  for (size_t i = 1; i != tablets_.size(); ++i) {
    Consensus(tablets_[i])->TEST_PauseUpdateConsensus(false);
  }
  for (size_t i = 1; i != tablets_.size(); ++i) {
    ASSERT_OK(sync_points.WaitCompleted(i, /* accepted= */ true));
  }
  sync_points.Resume();
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_EQ(result.partials.size(), 1);
  ASSERT_EQ(result.PartialIndices(), (std::set<uint32_t>{0}));
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{1, 2, 3}));
  auto sends = sync_points.sends();
  ASSERT_EQ(sends.size(), 2);
  ASSERT_EQ(sends[0].num_slots, 1);
  ASSERT_FALSE(sends[0].final);
  ASSERT_EQ(sends[1].num_slots, 3);
  ASSERT_TRUE(sends[1].final);
  ASSERT_OK(WaitForFramesSent(frames_before + 2));
  ASSERT_EQ(FramesSent() - frames_before, 2);
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        result.merged.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
}

// The final carries exactly the slots no partial frame carried, with their indices; an error slot
// goes out in a partial frame like any other.
TEST_F(MultiRaftUpdateConsensusTest, StreamedFinalCarriesRemainder) {
  ASSERT_NO_FATALS(StartServer(2));
  constexpr auto kDiskDelay = 1s;
  SetSlowDisk(tablets_[1], kDiskDelay);
  auto unknown_tablet = Heartbeat(tablets_[0]);
  unknown_tablet.set_tablet_id("no-such-tablet");
  auto envelope = SendRecordedAsync(
      {Data(&tablets_[0], 1, true), unknown_tablet, Data(&tablets_[1], 2, false)});
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_GE(envelope->elapsed(), kDiskDelay);
  ASSERT_EQ(result.PartialIndices(), (std::set<uint32_t>{0, 1}));
  ASSERT_EQ(result.final_frame.consensus_response_size(), 1);
  ASSERT_EQ(result.final_frame.index_size(), 1);
  ASSERT_EQ(result.final_frame.index(0), 2);
  ASSERT_NO_FATALS(AssertSlotOk(result.final_frame.consensus_response(0), tablets_[1].last));
  ASSERT_NO_FATALS(AssertSlotOk(result.merged.consensus_response(0), tablets_[0].last));
  ASSERT_EQ(result.merged.consensus_response(1).error().code(),
            TabletServerErrorPB::TABLET_NOT_FOUND);
}

// A slot still running at the deadline goes out TimedOut in the final, after the partial frames
// of the others.
TEST_F(MultiRaftUpdateConsensusTest, StreamedDeadlineAfterPartials) {
  ASSERT_NO_FATALS(StartServer(3));
  auto& paused = tablets_[2];
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });
  const auto deadline_responses_before = DeadlineResponses();
  auto envelope = SendRecordedAsync(
      {Data(&tablets_[0], 1, true), Data(&tablets_[1], 1, true), Data(&paused, 1, true)}, 2s);
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_EQ(DeadlineResponses() - deadline_responses_before, 1);
  ASSERT_GE(envelope->elapsed(), 1500ms);
  ASSERT_EQ(result.PartialIndices(), (std::set<uint32_t>{0, 1}));
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{2}));
  ASSERT_NO_FATALS(AssertSlotOk(result.merged.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotOk(result.merged.consensus_response(1), tablets_[1].last));
  ASSERT_NO_FATALS(AssertSlotError(result.merged.consensus_response(2), AppStatusPB::TIMED_OUT));
}

// The deadline times a slot out while the sender is busy, and then the slot's own completion
// arrives. The slot is answered once, TimedOut, by the sender.
TEST_F(MultiRaftUpdateConsensusTest, StreamedDeadlineRacesCompletion) {
  ASSERT_NO_FATALS(StartServer(2));
  auto& held = tablets_[1];
  std::unique_ptr<Envelope> envelope;
  const auto deadline_responses_before = DeadlineResponses();
  StreamSyncPoints sync_points(
      /* hold_send= */ 1, /* hold_release= */ 0, /* hold_update= */ held.id);
  // The client gives up 10% after the server's deadline: enough for the steps below, which wait
  // for no timer.
  envelope = SendRecordedAsync(
      {Data(&tablets_[0], 1, true), Data(&held, 1, true)}, 6s * kTimeMultiplier);
  ASSERT_OK(sync_points.WaitHeld());
  ASSERT_OK(WaitFor(
      [this, deadline_responses_before] {
        return DeadlineResponses() - deadline_responses_before == 1;
      },
      20s * kTimeMultiplier, "Deadline fired while the sender is held"));
  sync_points.ResumeUpdate();
  ASSERT_OK(sync_points.WaitCompleted(1, /* accepted= */ false));
  sync_points.Resume();
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_EQ(result.PartialIndices(), (std::set<uint32_t>{0}));
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{1}));
  ASSERT_NO_FATALS(AssertSlotOk(result.merged.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotError(result.merged.consensus_response(1), AppStatusPB::TIMED_OUT));
  ASSERT_EQ(sync_points.sends().size(), 2);
}

// The deadline fires while every slot is already ready, one of them waiting for the busy sender.
// It times nothing out, counts nothing, and the sender sends the final.
TEST_F(MultiRaftUpdateConsensusTest, StreamedDeadlineWithEverySlotReady) {
  ASSERT_NO_FATALS(StartServer(2));
  std::unique_ptr<Envelope> envelope;
  const auto deadline_responses_before = DeadlineResponses();
  StreamSyncPoints sync_points(
      /* hold_send= */ 1, /* hold_release= */ 0, /* hold_update= */ tablets_[1].id);
  // As in StreamedDeadlineRacesCompletion, the steps after the deadline wait for no timer.
  envelope = SendRecordedAsync(
      {Data(&tablets_[0], 1, true), Data(&tablets_[1], 1, true)}, 6s * kTimeMultiplier);
  // The sender holds slot 0's frame, and slot 1 becomes ready behind it.
  ASSERT_OK(sync_points.WaitHeld());
  sync_points.ResumeUpdate();
  ASSERT_OK(sync_points.WaitCompleted(1, /* accepted= */ true));
  ASSERT_EQ(ASSERT_RESULT(sync_points.WaitDeadline()), 0);
  sync_points.Resume();
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_EQ(result.PartialIndices(), (std::set<uint32_t>{0}));
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{1}));
  ASSERT_NO_FATALS(AssertSlotOk(result.merged.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotOk(result.merged.consensus_response(1), tablets_[1].last));
  ASSERT_EQ(DeadlineResponses(), deadline_responses_before);
  // Only the held sender sent.
  ASSERT_EQ(sync_points.sends().size(), 2);
}

// When every sub-request fails before dispatch, the handler thread sends the final, with index,
// before the deadline and without partial frames.
TEST_F(MultiRaftUpdateConsensusTest, StreamedAllSubRequestsFailBeforeDispatch) {
  ASSERT_NO_FATALS(StartServer(1));
  const auto deadline_responses_before = DeadlineResponses();
  const auto frames_before = FramesSent();
  auto wrong_uuid = Heartbeat(tablets_[0]);
  wrong_uuid.set_dest_uuid("no-such-server");
  auto unknown_tablet = Heartbeat(tablets_[0]);
  unknown_tablet.set_tablet_id("no-such-tablet");
  auto envelope = SendRecordedAsync({wrong_uuid, unknown_tablet}, 30s);
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_LT(envelope->elapsed(), 5s);
  ASSERT_EQ(result.partials.size(), 0);
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{0, 1}));
  ASSERT_EQ(result.merged.consensus_response(0).error().code(),
            TabletServerErrorPB::WRONG_SERVER_UUID);
  ASSERT_EQ(result.merged.consensus_response(1).error().code(),
            TabletServerErrorPB::TABLET_NOT_FOUND);
  ASSERT_OK(WaitForFramesSent(frames_before + 1));
  ASSERT_EQ(FramesSent() - frames_before, 1);
  ASSERT_EQ(DeadlineResponses(), deadline_responses_before);
}

// A completion appended after the sender found the ready list empty, but before it gave up its
// role, is sent by that sender, not left for the deadline.
TEST_F(MultiRaftUpdateConsensusTest, StreamedNoStrandedCompletion) {
  ASSERT_NO_FATALS(StartServer(2));
  auto& paused = tablets_[1];
  std::unique_ptr<Envelope> envelope;
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });
  const auto deadline_responses_before = DeadlineResponses();
  StreamSyncPoints sync_points(/* hold_send= */ 0, /* hold_release= */ 1);
  constexpr auto kTimeout = 10s;
  envelope = SendRecordedAsync({Data(&tablets_[0], 1, true), Data(&paused, 1, true)}, kTimeout);
  // The sender has sent slot 0 and holds its role with an empty list.
  ASSERT_OK(sync_points.WaitHeld());
  ASSERT_EQ(sync_points.sends().size(), 1);
  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(sync_points.WaitCompleted(1, /* accepted= */ true));
  sync_points.Resume();
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_LT(envelope->elapsed(), kTimeout / 2);
  ASSERT_EQ(DeadlineResponses(), deadline_responses_before);
  ASSERT_EQ(result.PartialIndices(), (std::set<uint32_t>{0}));
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{1}));
  ASSERT_NO_FATALS(AssertSlotOk(result.merged.consensus_response(0), tablets_[0].last));
  ASSERT_NO_FATALS(AssertSlotOk(result.merged.consensus_response(1), paused.last));
}

// Many sub-requests completing at random times: every response arrives long before the deadline.
TEST_F(MultiRaftUpdateConsensusTest, StreamedNoStrandedCompletionStress) {
  constexpr size_t kTablets = 8;
  constexpr int kEnvelopes = 20;
  constexpr auto kTimeout = 10s;
  ASSERT_NO_FATALS(StartServer(kTablets));
  // Spreads the completions over the sender's sends and releases.
  auto* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("MultiRaftUpdateTask::BeforeUpdate", [](void*) {
    SleepFor(RandomUniformInt(0, 3000) * 1us);
  });
  sync_point->EnableProcessing();
  size_t num_partials = 0;
  for (int i = 0; i != kEnvelopes; ++i) {
    std::vector<ConsensusRequestPB> requests;
    for (auto& tablet : tablets_) {
      requests.push_back(Data(&tablet, 1, /* commit= */ true));
    }
    auto envelope = SendRecordedAsync(std::move(requests), kTimeout);
    auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
    ASSERT_LT(envelope->elapsed(), kTimeout / 2);
    for (size_t j = 0; j != kTablets; ++j) {
      ASSERT_NO_FATALS(AssertSlotOk(
          result.merged.consensus_response(narrow_cast<int>(j)), tablets_[j].last));
    }
    num_partials += result.partials.size();
  }
  LOG(INFO) << "Partial frames: " << num_partials << " in " << kEnvelopes << " envelopes";
  ASSERT_GT(num_partials, 0);
}

// After the final at the deadline, the paused slot's completion is dropped, and nothing more is
// sent.
TEST_F(MultiRaftUpdateConsensusTest, StreamedLateCompletionDropped) {
  ASSERT_NO_FATALS(StartServer(2));
  auto& paused = tablets_[1];
  std::unique_ptr<Envelope> envelope;
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });
  const auto deadline_responses_before = DeadlineResponses();
  const auto frames_before = FramesSent();
  StreamSyncPoints sync_points;
  envelope = SendRecordedAsync({Data(&tablets_[0], 1, true), Data(&paused, 1, true)}, 2s);
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_EQ(DeadlineResponses() - deadline_responses_before, 1);
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{1}));
  ASSERT_NO_FATALS(AssertSlotError(result.merged.consensus_response(1), AppStatusPB::TIMED_OUT));
  ASSERT_OK(WaitForFramesSent(frames_before + 2));
  const auto frames_sent = FramesSent();
  const auto num_sends = sync_points.sends().size();
  ASSERT_EQ(num_sends, 2);
  ASSERT_NE(LastReceived(paused), paused.last);

  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(sync_points.WaitCompleted(1, /* accepted= */ false));
  ASSERT_OK(WaitForLastReceived(paused));
  ASSERT_EQ(sync_points.sends().size(), num_sends);
  ASSERT_EQ(FramesSent(), frames_sent);
  ASSERT_EQ(envelope->frames->num_partials(), result.partials.size());
  auto resp = ASSERT_RESULT(Send({Heartbeat(tablets_[0]), Heartbeat(paused)}));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(1), paused.last));
}

// Sub-requests that fail before dispatch are streamed with their indices, together.
TEST_F(MultiRaftUpdateConsensusTest, StreamedErrorSlots) {
  ASSERT_NO_FATALS(StartServer(1));
  constexpr auto kDiskDelay = 500ms;
  SetSlowDisk(tablets_[0], kDiskDelay);
  auto wrong_uuid = Heartbeat(tablets_[0]);
  wrong_uuid.set_dest_uuid("no-such-server");
  auto unknown_tablet = Heartbeat(tablets_[0]);
  unknown_tablet.set_tablet_id("no-such-tablet");
  auto envelope = SendRecordedAsync({wrong_uuid, Data(&tablets_[0], 1, false), unknown_tablet});
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_GE(envelope->elapsed(), kDiskDelay);
  ASSERT_EQ(result.partials.size(), 1);
  ASSERT_EQ(result.PartialIndices(), (std::set<uint32_t>{0, 2}));
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{1}));
  ASSERT_EQ(result.merged.consensus_response(0).error().code(),
            TabletServerErrorPB::WRONG_SERVER_UUID);
  ASSERT_NO_FATALS(AssertSlotOk(result.merged.consensus_response(1), tablets_[0].last));
  ASSERT_EQ(result.merged.consensus_response(2).error().code(),
            TabletServerErrorPB::TABLET_NOT_FOUND);
}

// An envelope without sub-requests is answered once, without partial frames or index.
TEST_F(MultiRaftUpdateConsensusTest, StreamedEmptyEnvelope) {
  ASSERT_NO_FATALS(StartServer(1));
  const auto frames_before = FramesSent();
  auto envelope = SendRecordedAsync({});
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_EQ(envelope->resp.consensus_response_size(), 0);
  ASSERT_EQ(envelope->resp.index_size(), 0);
  ASSERT_EQ(envelope->frames->num_partials(), 0);
  ASSERT_EQ(FramesSent(), frames_before);
}

// The serial handler answers once, in request order and without index, whatever
// stream_responses says.
TEST_F(MultiRaftUpdateConsensusTest, SerialHandlerIgnoresStreamResponses) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = false;
  ASSERT_NO_FATALS(StartServer(3));
  SetSlowDisk(tablets_[0], 500ms);
  const auto concurrent_before = ConcurrentSubRequests();
  const auto frames_before = FramesSent();
  auto envelope = SendRecordedAsync(OneOpPerTablet());
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_EQ(envelope->frames->num_partials(), 0);
  ASSERT_EQ(envelope->resp.index_size(), 0);
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        envelope->resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
  ASSERT_EQ(ConcurrentSubRequests(), concurrent_before);
  ASSERT_EQ(FramesSent(), frames_before);

  // The same envelope is streamed by the concurrent handler.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = true;
  envelope = SendRecordedAsync(OneOpPerTablet());
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_GT(result.partials.size(), 0);
}

// Without stream_responses the concurrent handler answers once, in request order and without
// index, even to a caller that accepts partial frames.
TEST_F(MultiRaftUpdateConsensusTest, NonStreamedUnchanged) {
  ASSERT_NO_FATALS(StartServer(3));
  constexpr auto kDiskDelay = 500ms;
  SetSlowDisk(tablets_[0], kDiskDelay);
  auto frame_slots = ServerStats(METRIC_multi_raft_update_frame_slots);
  const auto frames_before = FramesSent();
  const auto frame_slots_before = frame_slots->TotalCount();
  auto envelope = SendRecordedAsync(OneOpPerTablet(), 30s, /* stream= */ false);
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_GE(envelope->elapsed(), kDiskDelay);
  ASSERT_EQ(envelope->frames->num_partials(), 0);
  ASSERT_EQ(envelope->resp.index_size(), 0);
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        envelope->resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
  ASSERT_EQ(FramesSent(), frames_before);
  ASSERT_EQ(frame_slots->TotalCount(), frame_slots_before);

  // The same envelope with stream_responses is streamed.
  envelope = SendRecordedAsync(OneOpPerTablet());
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_GT(result.partials.size(), 0);
}

// The slot wait measures the barrier without streaming, and almost nothing with it.
TEST_F(MultiRaftUpdateConsensusTest, SlotWaitMetric) {
  ASSERT_NO_FATALS(StartServer(3));
  constexpr auto kDiskDelay = 1s;
  SetSlowDisk(tablets_[0], kDiskDelay);
  auto slot_wait = ServerStats(METRIC_multi_raft_update_slot_wait_us);
  // StartServer's heartbeat envelope records its slot waits after its response is queued.
  ASSERT_OK(WaitForStatsCount(slot_wait, tablets_.size()));
  auto count_before = slot_wait->TotalCount();
  auto sum_before = slot_wait->TotalSum();
  auto envelope = SendAsync(OneOpPerTablet());
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_GE(envelope->elapsed(), kDiskDelay);
  ASSERT_OK(WaitForStatsCount(slot_wait, count_before + tablets_.size()));
  ASSERT_EQ(slot_wait->TotalCount() - count_before, tablets_.size());
  // Each of the two fast slots waited for the slow one.
  const auto barrier_sum = slot_wait->TotalSum() - sum_before;
  LOG(INFO) << "Slot wait without streaming: " << barrier_sum << " us";
  ASSERT_GE(barrier_sum, 2 * ToMicroseconds(kDiskDelay - 200ms));

  count_before = slot_wait->TotalCount();
  sum_before = slot_wait->TotalSum();
  envelope = SendRecordedAsync(OneOpPerTablet());
  ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_GE(envelope->elapsed(), kDiskDelay);
  ASSERT_OK(WaitForStatsCount(slot_wait, count_before + tablets_.size()));
  ASSERT_EQ(slot_wait->TotalCount() - count_before, tablets_.size());
  const auto streamed_sum = slot_wait->TotalSum() - sum_before;
  LOG(INFO) << "Slot wait with streaming: " << streamed_sum << " us";
  ASSERT_LT(streamed_sum, ToMicroseconds(200ms));
}

// The frame metrics count every frame of a streamed envelope and every slot once.
TEST_F(MultiRaftUpdateConsensusTest, StreamedFrameMetrics) {
  ASSERT_NO_FATALS(StartServer(4));
  SetSlowDisk(tablets_[0], 500ms);
  auto requests = OneOpPerTablet();
  auto frame_slots = ServerStats(METRIC_multi_raft_update_frame_slots);
  const auto frames_before = FramesSent();
  const auto frame_count_before = frame_slots->TotalCount();
  const auto frame_slots_before = frame_slots->TotalSum();
  auto result = ASSERT_RESULT(SendStreamed(std::move(requests)));
  ASSERT_GT(result.partials.size(), 0);
  ASSERT_OK(WaitForFramesSent(frames_before + result.partials.size() + 1));
  ASSERT_OK(WaitForStatsCount(frame_slots, frame_count_before + result.partials.size() + 1));
  ASSERT_EQ(FramesSent() - frames_before, result.partials.size() + 1);
  ASSERT_EQ(frame_slots->TotalCount() - frame_count_before, result.partials.size() + 1);
  ASSERT_EQ(frame_slots->TotalSum() - frame_slots_before, tablets_.size());
}

// A ready response waiting for a busy sender stays valid after its tablet is deleted.
TEST_F(MultiRaftUpdateConsensusTest, StreamedResponseOutlivesDeletedTablet) {
  ASSERT_NO_FATALS(StartServer(2));
  auto& deleted = tablets_[1];
  std::unique_ptr<Envelope> envelope;
  Consensus(deleted)->TEST_PauseUpdateConsensus(true);
  bool tablet_deleted = false;
  auto unpause = ScopeExit([this, &deleted, &tablet_deleted] {
    if (!tablet_deleted) {
      Consensus(deleted)->TEST_PauseUpdateConsensus(false);
    }
  });
  std::weak_ptr<consensus::RaftConsensus> deleted_consensus = Consensus(deleted);
  StreamSyncPoints sync_points(/* hold_send= */ 1);
  envelope = SendRecordedAsync({Data(&tablets_[0], 1, true), Data(&deleted, 2, true)});
  // The sender holds slot 0's frame while slot 1 becomes ready behind it.
  ASSERT_OK(sync_points.WaitHeld());
  Consensus(deleted)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(sync_points.WaitCompleted(1, /* accepted= */ true));
  tablet_deleted = true;
  std::optional<TabletServerErrorPB::Code> error_code;
  ASSERT_OK(mini_server_->server()->tablet_manager()->DeleteTablet(
      deleted.id, tablet::TABLET_DATA_DELETED, tablet::ShouldAbortActiveTransactions::kFalse,
      /* cas_config_opid_index_less_or_equal= */ std::nullopt, /* hide_only= */ false,
      /* keep_data= */ false, &error_code));
  deleted.peer.reset();
  ASSERT_OK(WaitFor(
      [&deleted_consensus] { return deleted_consensus.expired(); }, 10s,
      "RaftConsensus of the deleted tablet destroyed"));
  ASSERT_FALSE(envelope->finished());
  sync_points.Resume();
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{1}));
  const auto& slot = result.merged.consensus_response(1);
  ASSERT_NO_FATALS(AssertSlotOk(slot, deleted.last));
  ASSERT_EQ(slot.responder_uuid(), server_uuid_) << slot.ShortDebugString();
  ASSERT_EQ(slot.responder_term(), term_) << slot.ShortDebugString();
}

// When a partial frame cannot be sent, its slots and every later one go in the final, each once.
TEST_F(MultiRaftUpdateConsensusTest, StreamedPartialFailureFallsBack) {
  ASSERT_NO_FATALS(StartServer(3));
  constexpr auto kDiskDelay = 1s;
  SetSlowDisk(tablets_[0], kDiskDelay);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_multi_raft_update_fail_partial_frames) = true;
  auto requests = OneOpPerTablet();
  const auto frames_before = FramesSent();
  std::unique_ptr<Envelope> envelope;
  StreamSyncPoints sync_points;
  envelope = SendRecordedAsync(std::move(requests));
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_GE(envelope->elapsed(), kDiskDelay);
  // A partial frame was tried, and after it only the final.
  auto sends = sync_points.sends();
  ASSERT_EQ(sends.size(), 2);
  ASSERT_FALSE(sends[0].final);
  ASSERT_TRUE(sends[1].final);
  ASSERT_EQ(sends[1].num_slots, tablets_.size());
  ASSERT_EQ(result.partials.size(), 0);
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{0, 1, 2}));
  ASSERT_OK(WaitForFramesSent(frames_before + 1));
  ASSERT_EQ(FramesSent() - frames_before, 1);
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        result.merged.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
}

// A leader's batcher that streams gets each request's response as soon as the follower completes
// it: a paused tablet does not hold up the other requests of its envelope. Every frame the server
// sends passes the batcher's checks, and the batch is released once everything has run.
TEST_F(MultiRaftUpdateConsensusTest, BatcherStreamsAroundSlowSlot) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_streaming_responses) = true;
  ASSERT_NO_FATALS(StartServer(4));
  ASSERT_NO_FATALS(CreateBatcher());
  RegexWaiterLogSink frame_errors(kLeaderFrameErrors);
  auto& paused = tablets_[1];
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });
  const auto frames_sent_before = FramesSent();
  std::vector<BatchedCall*> calls;
  ASSERT_NO_FATALS(SendThroughBatcher(&calls));

  ASSERT_OK(WaitFor(
      [&calls] { return IsDone(*calls[0]) && IsDone(*calls[2]); }, 10s,
      "Envelope-mates of the paused tablet answered"));
  ASSERT_FALSE(IsDone(*calls[1]));
  ASSERT_NE(LastReceived(paused), paused.last);
  for (size_t i : {0, 2}) {
    ASSERT_OK(calls[i]->status);
    ASSERT_TRUE(calls[i]->had_holder);
    ASSERT_NO_FATALS(AssertSlotOk(calls[i]->response, tablets_[i].last));
  }

  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(WaitFor([&calls] { return IsDone(*calls[1]); }, 10s, "Paused tablet answered"));
  ASSERT_OK(calls[1]->status);
  ASSERT_NO_FATALS(AssertSlotOk(calls[1]->response, paused.last));
  for (auto* call : calls) {
    ASSERT_OK(WaitFor([call] { return call->handle.expired(); }, 10s, "Batch released"));
  }

  // The blocker's envelope, at least one partial frame, and the final of the second envelope.
  const auto frames_received = LeaderCounter(METRIC_multi_raft_frames_received);
  ASSERT_GE(frames_received, 3);
  ASSERT_OK(WaitForFramesSent(frames_sent_before + frames_received));
  ASSERT_EQ(FramesSent() - frames_sent_before, frames_received);
  ASSERT_EQ(LeaderCounter(METRIC_multi_raft_envelopes_failed), 0);
  ASSERT_FALSE(frame_errors.IsEventOccurred());
}

// Many streamed envelopes whose requests complete together, so frames are built concurrently and
// the final follows partial frames closely. The leader receives every request's response exactly
// once from the real follower's frames: none is missing, duplicated or malformed.
TEST_F(MultiRaftUpdateConsensusTest, BatcherStreamsCoincidingCompletions) {
  constexpr int kRounds = 200;
  constexpr size_t kTablets = 8;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_streaming_responses) = true;
  ASSERT_NO_FATALS(StartServer(kTablets));
  ASSERT_NO_FATALS(CreateBatcher());
  RegexWaiterLogSink frame_errors(kLeaderFrameErrors);
  const auto frames_sent_before = FramesSent();
  for (int round = 0; round != kRounds; ++round) {
    // The first request goes out alone; the others wait for its response and then go out
    // together (in more than one envelope if that response arrives during the adds).
    std::vector<BatchedCall*> calls;
    for (size_t i = 0; i != kTablets; ++i) {
      calls.push_back(&AddToBatcher(Data(&tablets_[i], 1, /* commit= */ false)));
    }
    for (size_t i = 0; i != kTablets; ++i) {
      auto* call = calls[i];
      ASSERT_OK(WaitFor([call] { return IsDone(*call); }, 10s, "Answered"));
      ASSERT_TRUE(call->status.ok())
          << "round " << round << ", tablet " << i << ": " << call->status;
      ASSERT_NO_FATALS(AssertSlotOk(call->response, tablets_[i].last));
    }
    for (auto* call : calls) {
      ASSERT_OK(WaitFor([call] { return call->handle.expired(); }, 10s, "Batch released"));
    }
  }
  ASSERT_GE(LeaderCounter(METRIC_multi_raft_envelopes_sent), 2 * kRounds);
  const auto frames_received = LeaderCounter(METRIC_multi_raft_frames_received);
  ASSERT_OK(WaitForFramesSent(frames_sent_before + frames_received));
  ASSERT_EQ(FramesSent() - frames_sent_before, frames_received);
  ASSERT_EQ(LeaderCounter(METRIC_multi_raft_envelopes_failed), 0);
  ASSERT_FALSE(frame_errors.IsEventOccurred());
  LOG(INFO) << "Frames received: " << frames_received << " for "
            << LeaderCounter(METRIC_multi_raft_envelopes_sent) << " envelopes";
}

// A follower that does not stream (here its serial handler, with its data flag off) answers once,
// without index. The streaming leader takes that as every response in request order, so the
// requests complete together.
TEST_F(MultiRaftUpdateConsensusTest, BatcherStreamingAgainstSerialFollower) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_streaming_responses) = true;
  ASSERT_NO_FATALS(StartServer(4));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = false;
  ASSERT_NO_FATALS(CreateBatcher());
  RegexWaiterLogSink frame_errors(kLeaderFrameErrors);
  auto& paused = tablets_[1];
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });
  const auto frames_sent_before = FramesSent();
  const auto frames_received_before = LeaderCounter(METRIC_multi_raft_frames_received);
  std::vector<BatchedCall*> calls;
  ASSERT_NO_FATALS(SendThroughBatcher(&calls));

  // The first request is processed, but its response waits for the paused one.
  ASSERT_OK(WaitForLastReceived(tablets_[0]));
  SleepFor(300ms * kTimeMultiplier);
  for (auto* call : calls) {
    ASSERT_FALSE(IsDone(*call));
  }

  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  for (size_t i = 0; i != calls.size(); ++i) {
    auto* call = calls[i];
    ASSERT_OK(WaitFor([call] { return IsDone(*call); }, 10s, "Answered"));
    ASSERT_OK(call->status);
    ASSERT_NO_FATALS(AssertSlotOk(call->response, tablets_[i].last));
    ASSERT_OK(WaitFor([call] { return call->handle.expired(); }, 10s, "Batch released"));
  }
  // The final of each envelope.
  ASSERT_EQ(LeaderCounter(METRIC_multi_raft_frames_received) - frames_received_before, 2);
  ASSERT_EQ(FramesSent(), frames_sent_before);
  ASSERT_EQ(LeaderCounter(METRIC_multi_raft_envelopes_failed), 0);
  ASSERT_FALSE(frame_errors.IsEventOccurred());
}

// Failing a streamed envelope through its handle, as the stuck-call detector does, fails only the
// request no frame answered. With the production sender the final callback then runs on this
// thread, under the controller's lock, so it must not call back into the controller. The
// follower's final is held until the call has expired on the client, as it would have when the
// detector fires: a final that reaches a call failed before its expiry is parsed into the
// response of a batch that may be gone.
TEST_F(MultiRaftUpdateConsensusTest, BatcherStreamedMarkCallAsFailed) {
  constexpr auto kTimeout = 1s;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_streaming_responses) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_rpc_timeout_ms) =
      narrow_cast<uint32_t>(ToMilliseconds(kTimeout));
  ASSERT_NO_FATALS(StartServer(4));
  ASSERT_NO_FATALS(CreateBatcher());
  auto& paused = tablets_[1];
  Consensus(paused)->TEST_PauseUpdateConsensus(true);
  auto unpause = ScopeExit([this, &paused] {
    Consensus(paused)->TEST_PauseUpdateConsensus(false);
  });
  const auto frames_sent_before = FramesSent();
  std::vector<BatchedCall*> calls;
  ASSERT_NO_FATALS(SendThroughBatcher(&calls));
  const auto sent = MonoTime::Now();

  CountDownLatch release_final(1);
  std::atomic<bool> final_held{false};
  SyncPointScope sync_point;
  auto release_on_failure = ScopeExit([&release_final] { release_final.CountDown(); });
  sync_point->SetCallBack(
      "MultiRaftUpdateState::SenderBeforeSend", [&release_final, &final_held](void* arg) {
        if (static_cast<MultiRaftUpdateSyncPointArg*>(arg)->final) {
          final_held = true;
          release_final.WaitFor(60s);
        }
      });
  sync_point->EnableProcessing();

  ASSERT_OK(WaitFor(
      [&calls] { return IsDone(*calls[0]) && IsDone(*calls[2]); }, 10s,
      "Envelope-mates of the paused tablet answered"));
  ASSERT_FALSE(IsDone(*calls[1]));
  ASSERT_FALSE(final_held.load());

  calls[1]->handle.MarkCallAsFailed();
  ASSERT_OK(WaitFor([&calls] { return IsDone(*calls[1]); }, 10s, "Paused tablet failed"));
  ASSERT_TRUE(calls[1]->status.IsTimedOut()) << calls[1]->status;
  for (size_t i : {0, 2}) {
    ASSERT_OK(calls[i]->status);
    ASSERT_NO_FATALS(AssertSlotOk(calls[i]->response, tablets_[i].last));
  }
  ASSERT_EQ(LeaderCounter(METRIC_multi_raft_envelopes_failed), 1);
  const auto frames_received = LeaderCounter(METRIC_multi_raft_frames_received);
  ASSERT_OK(WaitForFramesSent(frames_sent_before + frames_received));

  // The follower still answers the paused request, after the call failed and expired.
  Consensus(paused)->TEST_PauseUpdateConsensus(false);
  ASSERT_OK(WaitFor([&final_held] { return final_held.load(); }, 10s, "Final held"));
  SleepFor(sent + kTimeout + 1s * kTimeMultiplier - MonoTime::Now());
  release_final.CountDown();
  ASSERT_OK(WaitForFramesSent(frames_sent_before + frames_received + 1));
  for (auto* call : calls) {
    ASSERT_OK(WaitFor([call] { return call->handle.expired(); }, 10s, "Batch released"));
  }
  ASSERT_EQ(LeaderCounter(METRIC_multi_raft_frames_received), frames_received);
}

// With multi_raft_update_inline_first, the handler thread runs the first sub-request itself and
// the others go to the pool.
TEST_F(MultiRaftUpdateConsensusTest, InlineFirstRunsOnHandlerThread) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_inline_first) = true;
  ASSERT_NO_FATALS(StartServer(3));
  const auto inline_before = InlineSubRequests();
  const auto concurrent_before = ConcurrentSubRequests();
  UpdateThreads threads;
  auto resp = ASSERT_RESULT(Send(OneOpPerTablet()));
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[0], kServiceThread));
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[1], kPoolThread));
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[2], kPoolThread));
  ASSERT_EQ(InlineSubRequests() - inline_before, 1);
  ASSERT_EQ(ConcurrentSubRequests() - concurrent_before, tablets_.size());
}

// The sub-request run inline is the first one that passed the pre-dispatch checks.
TEST_F(MultiRaftUpdateConsensusTest, InlineFirstSkipsFailedSlotZero) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_inline_first) = true;
  ASSERT_NO_FATALS(StartServer(3));
  const auto inline_before = InlineSubRequests();
  UpdateThreads threads;
  auto wrong_uuid = Heartbeat(tablets_[0]);
  wrong_uuid.set_dest_uuid("no-such-server");
  auto resp = ASSERT_RESULT(Send(
      {wrong_uuid, Data(&tablets_[1], 1, true), Data(&tablets_[2], 1, true)}));
  ASSERT_EQ(resp.consensus_response(0).error().code(), TabletServerErrorPB::WRONG_SERVER_UUID);
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(1), tablets_[1].last));
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(2), tablets_[2].last));
  ASSERT_TRUE(threads.Get(tablets_[0].id).empty());
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[1], kServiceThread));
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[2], kPoolThread));
  ASSERT_EQ(InlineSubRequests() - inline_before, 1);
}

// An envelope whose only dispatchable sub-request runs inline never uses the pool, and the inline
// run is not counted in the pool's metrics.
TEST_F(MultiRaftUpdateConsensusTest, InlineFirstSingleSubRequestSkipsPool) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_inline_first) = true;
  // StartServer's heartbeat envelope has one sub-request too.
  ASSERT_NO_FATALS(StartServer(1));
  const auto inline_before = InlineSubRequests();
  UpdateThreads threads;
  auto unknown_tablet = Heartbeat(tablets_[0]);
  unknown_tablet.set_tablet_id("no-such-tablet");
  auto resp = ASSERT_RESULT(Send({unknown_tablet, Data(&tablets_[0], 1, false)}));
  ASSERT_EQ(resp.consensus_response(0).error().code(), TabletServerErrorPB::TABLET_NOT_FOUND);
  ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(1), tablets_[0].last));
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[0], kServiceThread));
  ASSERT_EQ(InlineSubRequests() - inline_before, 1);
  // Nothing has been queued to the pool since the server started.
  ASSERT_EQ(ServerStats(METRIC_multi_raft_update_pool_queue_time_us)->TotalCount(), 0);
  ASSERT_EQ(ServerStats(METRIC_multi_raft_update_pool_run_time_us)->TotalCount(), 0);
}

// The inline sub-request's frame of a streamed envelope goes out like the others, with the right
// indices.
TEST_F(MultiRaftUpdateConsensusTest, InlineFirstWithStreaming) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_inline_first) = true;
  ASSERT_NO_FATALS(StartServer(3));
  constexpr auto kDiskDelay = 1s;
  SetSlowDisk(tablets_[2], kDiskDelay);
  const auto inline_before = InlineSubRequests();
  UpdateThreads threads;
  std::vector<ConsensusRequestPB> requests;
  for (size_t i = 0; i != tablets_.size(); ++i) {
    requests.push_back(Data(&tablets_[i], narrow_cast<int>(i) + 1, /* commit= */ i != 2));
  }
  auto envelope = SendRecordedAsync(std::move(requests));
  auto result = ASSERT_RESULT(WaitStreamed(envelope.get()));
  ASSERT_GE(envelope->elapsed(), kDiskDelay);
  // The inline slot did not wait for the slow one.
  ASSERT_TRUE(result.PartialIndices().contains(0)) << AsString(result.PartialIndices());
  ASSERT_EQ(result.FinalIndices(), (std::set<uint32_t>{2}));
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        result.merged.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[0], kServiceThread));
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[1], kPoolThread));
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[2], kPoolThread));
  ASSERT_EQ(InlineSubRequests() - inline_before, 1);
}

// The inline sub-request blocked on its tablet's update_mutex_ does not hold up the others, which
// were queued to the pool before it ran.
TEST_F(MultiRaftUpdateConsensusTest, InlineFirstHeldMutexDoesNotBlockMates) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_inline_first) = true;
  ASSERT_NO_FATALS(StartServer(3));
  TestThreadHolder thread_holder;
  std::atomic<bool> direct_done{false};
  ASSERT_NO_FATALS(HoldUpdateMutex(&tablets_[0], 3s, &thread_holder, &direct_done));
  const auto inline_before = InlineSubRequests();
  UpdateThreads threads;

  auto envelope = SendAsync(
      {Heartbeat(tablets_[0]), Data(&tablets_[1], 1, true), Data(&tablets_[2], 1, true)});
  ASSERT_OK(WaitForLastReceived(tablets_[1], 1s));
  ASSERT_OK(WaitForLastReceived(tablets_[2], 1s));
  // Still held: the handler thread is blocked in the first sub-request.
  ASSERT_FALSE(direct_done.load());
  ASSERT_FALSE(envelope->finished());
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[0], kServiceThread));
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[1], kPoolThread));
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[2], kPoolThread));

  ASSERT_OK(Wait(envelope.get()));
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        envelope->resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
  ASSERT_EQ(InlineSubRequests() - inline_before, 1);
  thread_holder.JoinAll();
}

// An envelope whose inline sub-request is blocked on update_mutex_ past the deadline is still
// answered at the deadline, with the others' results: either the deadline task answers, or the
// inline sub-request gives up waiting for the mutex at the deadline.
TEST_F(MultiRaftUpdateConsensusTest, InlineFirstHeldMutexAnsweredAtDeadline) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_inline_first) = true;
  ASSERT_NO_FATALS(StartServer(2));
  TestThreadHolder thread_holder;
  std::atomic<bool> direct_done{false};
  ASSERT_NO_FATALS(HoldUpdateMutex(&tablets_[0], 4s, &thread_holder, &direct_done));
  const auto deadline_responses_before = DeadlineResponses();
  UpdateThreads threads;

  auto envelope = SendAsync({Heartbeat(tablets_[0]), Data(&tablets_[1], 1, true)}, 2s);
  ASSERT_OK(Wait(envelope.get()));
  ASSERT_GE(envelope->elapsed(), 1500ms);
  ASSERT_LT(envelope->elapsed(), 3s);
  ASSERT_FALSE(direct_done.load());
  const auto& slot = envelope->resp.consensus_response(0);
  ASSERT_NO_FATALS(AssertSlotError(slot, AppStatusPB::TIMED_OUT));
  LOG(INFO) << "Inline slot: " << slot.error().status().message() << ", deadline responses: "
            << DeadlineResponses() - deadline_responses_before;
  ASSERT_NO_FATALS(AssertSlotOk(envelope->resp.consensus_response(1), tablets_[1].last));
  ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[0], kServiceThread));
  thread_holder.JoinAll();
  ASSERT_TRUE(direct_done.load());
  ASSERT_NO_FATALS(AssertSlotOk(
      ASSERT_RESULT(Send({Heartbeat(tablets_[0])})).consensus_response(0), tablets_[0].last));
}

// With multi_raft_update_inline_first off, every sub-request runs on the pool.
TEST_F(MultiRaftUpdateConsensusTest, InlineFirstOffUnchanged) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_inline_first) = false;
  ASSERT_NO_FATALS(StartServer(3));
  const auto inline_before = InlineSubRequests();
  UpdateThreads threads;
  auto resp = ASSERT_RESULT(Send(OneOpPerTablet()));
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
    ASSERT_NO_FATALS(AssertUpdateThread(&threads, tablets_[i], kPoolThread));
  }
  ASSERT_EQ(InlineSubRequests(), inline_before);
}

// With the sweeper, many concurrent durability waits share one scheduler task: the sweeper arms
// far fewer tasks than the waits register ticks, and at most one at a time (two while an aborted
// one has not run yet). A timer per wait would keep one task armed per pending wait.
TEST_F(MultiRaftUpdateConsensusTest, SweeperOneTimerForManyWaits) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_raft_async_wait_use_sweeper) = true;
  constexpr size_t kNumWaits = 8;
  ASSERT_NO_FATALS(StartServer(kNumWaits));
  constexpr auto kDiskDelay = 1s;
  std::vector<ConsensusRequestPB> requests;
  for (auto& tablet : tablets_) {
    SetSlowDisk(tablet, kDiskDelay);
    requests.push_back(Data(&tablet, 1, false));
  }
  const auto before = Sweeper().TEST_counters();
  auto envelope = SendAsync(std::move(requests));
  ASSERT_OK(Wait(envelope.get()));
  for (size_t i = 0; i != tablets_.size(); ++i) {
    ASSERT_NO_FATALS(AssertSlotOk(
        envelope->resp.consensus_response(narrow_cast<int>(i)), tablets_[i].last));
  }
  ASSERT_EQ(Sweeper().TEST_num_pending(), 0);
  const auto after = Sweeper().TEST_counters();
  const auto registered = after.registered - before.registered;
  const auto scheduled = after.scheduled - before.scheduled;
  const auto aborted = after.aborted - before.aborted;
  const auto expiries = after.expiries - before.expiries;
  const auto due = after.due - before.due;
  LOG(INFO) << "Sweeper for " << kNumWaits << " waits of " << AsString(kDiskDelay)
            << ": registered " << registered << ", scheduled " << scheduled << ", aborted "
            << aborted << ", expiries " << expiries << ", due " << due
            << ", max outstanding " << after.max_outstanding;
  // Every wait registered once at its start and once per tick (heartbeat interval 100 ms).
  ASSERT_GE(registered, kNumWaits * 5);
  ASSERT_LE(scheduled * 4, registered);
  ASSERT_LE(after.max_outstanding, 2);
}

// Sweeper shutdown at tablet manager shutdown finishes the pending waits, so each sub-request is
// answered with the abort status at once, not at the deadline or when the log catches up.
TEST_F(MultiRaftUpdateConsensusTest, SweeperShutdownFailsPendingWaits) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_raft_async_wait_use_sweeper) = true;
  ASSERT_NO_FATALS(StartServer(3));
  // No tick before the shutdown, so that only the shutdown can finish the waits.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_raft_heartbeat_interval_ms) = 20000;
  constexpr auto kDiskDelay = 5s;
  std::vector<ConsensusRequestPB> requests;
  for (auto& tablet : tablets_) {
    SetSlowDisk(tablet, kDiskDelay);
    requests.push_back(Data(&tablet, 1, false));
  }
  auto envelope = SendAsync(std::move(requests), 60s);
  ASSERT_OK(WaitFor(
      [this] { return Sweeper().TEST_num_pending() == tablets_.size(); }, 10s,
      "Every wait pending in the sweeper"));
  ASSERT_FALSE(envelope->finished());
  const auto shutdown_start = MonoTime::Now();
  mini_server_->server()->tablet_manager()->StartShutdown();
  ASSERT_OK(Wait(envelope.get()));
  LOG(INFO) << "Answered " << MonoTime::Now() - shutdown_start << " after the shutdown started";
  ASSERT_LT(envelope->elapsed(), kDiskDelay - 2s);
  for (int i = 0; i != envelope->resp.consensus_response_size(); ++i) {
    const auto& slot = envelope->resp.consensus_response(i);
    ASSERT_NO_FATALS(AssertSlotError(slot, AppStatusPB::ABORTED));
    ASSERT_STR_CONTAINS(slot.error().status().message(), "Scheduler is shutting down");
  }
  ASSERT_EQ(Sweeper().TEST_num_pending(), 0);
}

// A durability wait that starts after the sweeper shut down finishes at once with the shutdown
// status and leaves nothing behind, whether it starts on a pool thread or inline on the handler
// thread.
TEST_F(MultiRaftUpdateConsensusTest, SweeperRegisterAfterShutdown) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_raft_async_wait_use_sweeper) = true;
  ASSERT_NO_FATALS(StartServer(1));
  auto& tablet = tablets_[0];
  auto* log = tablet.peer->log();
  Sweeper().Shutdown(consensus::RaftAsyncWaitSweeper::ShutdownStatus());
  SetSlowDisk(tablet, 3s);
  for (bool inline_first : {false, true}) {
    SCOPED_TRACE(Format("inline first: $0", inline_first));
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_update_inline_first) = inline_first;
    const auto inline_before = InlineSubRequests();
    auto envelope = SendAsync({Data(&tablet, 1, false)});
    ASSERT_OK(Wait(envelope.get()));
    ASSERT_LT(envelope->elapsed(), 2s);
    const auto& slot = envelope->resp.consensus_response(0);
    ASSERT_NO_FATALS(AssertSlotError(slot, AppStatusPB::ABORTED));
    ASSERT_STR_CONTAINS(slot.error().status().message(), "Scheduler is shutting down");
    ASSERT_EQ(InlineSubRequests() - inline_before, inline_first ? 1 : 0);
    // The log waiter was removed, and the sweeper kept nothing.
    ASSERT_EQ(log->TEST_num_safe_op_id_waiters(), 0);
    ASSERT_EQ(Sweeper().TEST_num_pending(), 0);
  }
  ASSERT_EQ(Sweeper().TEST_counters().registered, 0);
  SetSlowDisk(tablet, 0s);
}

// A wait keeps the tick source it started with when raft_async_wait_use_sweeper changes, in both
// directions, and both waits complete.
TEST_F(MultiRaftUpdateConsensusTest, SweeperFlagFlipMidWait) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_raft_async_wait_use_sweeper) = false;
  ASSERT_NO_FATALS(StartServer(2));
  auto& timer_tablet = tablets_[0];
  auto& sweeper_tablet = tablets_[1];
  constexpr auto kDiskDelay = 2s;
  SetSlowDisk(timer_tablet, kDiskDelay);
  SetSlowDisk(sweeper_tablet, kDiskDelay);

  auto timer_wait = SendAsync({Data(&timer_tablet, 1, false)});
  ASSERT_OK(WaitFor(
      [log = timer_tablet.peer->log()] { return log->TEST_num_safe_op_id_waiters() == 1; }, 2s,
      "Timer wait started"));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_raft_async_wait_use_sweeper) = true;
  // Several ticks of the first wait, none through the sweeper.
  SleepFor(400ms);
  ASSERT_FALSE(timer_wait->finished());
  ASSERT_EQ(SweeperRegistrations(), 0);

  auto sweeper_wait = SendAsync({Data(&sweeper_tablet, 1, false)});
  ASSERT_OK(WaitFor(
      [this] { return SweeperRegistrations() >= 1; }, 2s, "Sweeper wait started"));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_raft_async_wait_use_sweeper) = false;
  const auto registered = SweeperRegistrations();
  // The second wait keeps ticking through the sweeper.
  SleepFor(400ms);
  ASSERT_FALSE(sweeper_wait->finished());
  ASSERT_GE(SweeperRegistrations(), registered + 2);
  ASSERT_LE(Sweeper().TEST_num_pending(), 1);

  ASSERT_OK(Wait(timer_wait.get()));
  ASSERT_OK(Wait(sweeper_wait.get()));
  ASSERT_NO_FATALS(AssertSlotOk(timer_wait->resp.consensus_response(0), timer_tablet.last));
  ASSERT_NO_FATALS(AssertSlotOk(sweeper_wait->resp.consensus_response(0), sweeper_tablet.last));
  ASSERT_GE(timer_wait->elapsed(), kDiskDelay - 500ms);
  ASSERT_GE(sweeper_wait->elapsed(), kDiskDelay - 500ms);
  ASSERT_EQ(Sweeper().TEST_num_pending(), 0);
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
