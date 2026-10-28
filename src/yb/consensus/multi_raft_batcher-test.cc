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

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "yb/consensus/consensus.pb.h"
#include "yb/consensus/metadata.pb.h"
#include "yb/consensus/multi_raft_batcher.h"
#include "yb/consensus/multi_raft_batcher-test-util.h"

#include "yb/rpc/messenger.h"
#include "yb/rpc/proxy.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/logging_test_util.h"
#include "yb/util/metrics.h"
#include "yb/util/scope_exit.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/test_util.h"
#include "yb/util/thread_pool.h"

using namespace std::literals;

DECLARE_int32(consensus_rpc_timeout_ms);
DECLARE_bool(enable_multi_raft_data_batching);
DECLARE_bool(enable_multi_raft_heartbeat_batcher);
DECLARE_uint64(multi_raft_batch_bytes);
DECLARE_uint64(multi_raft_batch_size);
DECLARE_uint32(multi_raft_data_batch_size);
DECLARE_uint32(multi_raft_data_flush_delay_ms);
DECLARE_uint64(multi_raft_heartbeat_interval_ms);
DECLARE_uint32(multi_raft_rpc_timeout_ms);

METRIC_DECLARE_event_stats(multi_raft_batched_response_processing_us);
METRIC_DECLARE_counter(multi_raft_data_sends_direct);
METRIC_DECLARE_counter(multi_raft_data_sub_requests);
METRIC_DECLARE_counter(multi_raft_envelopes_failed);
METRIC_DECLARE_counter(multi_raft_envelopes_sent);
METRIC_DECLARE_counter(multi_raft_flush_bytes_cap);
METRIC_DECLARE_counter(multi_raft_flush_count_cap);
METRIC_DECLARE_counter(multi_raft_flush_deadline);
METRIC_DECLARE_counter(multi_raft_flush_immediate);
METRIC_DECLARE_counter(multi_raft_flush_response_clocked);
METRIC_DECLARE_counter(multi_raft_flush_timer);
METRIC_DECLARE_counter(multi_raft_heartbeat_sub_requests);
METRIC_DECLARE_event_stats(multi_raft_envelope_bytes);
METRIC_DECLARE_event_stats(multi_raft_envelope_ops);
METRIC_DECLARE_event_stats(multi_raft_envelope_sub_requests);

namespace yb::consensus {

using RequestKind = MultiRaftBatcher::RequestKind;

namespace {

constexpr auto kNeverMs = 3600 * 1000;

// A request added to the batcher, and what its callback observed.
struct TestCall {
  std::string tablet_id;
  ConsensusRequestPB request;
  ConsensusResponsePB response;
  MultiRaftBatcher::BatchHandle handle;
  std::function<void()> on_callback;

  Status status;
  bool on_high_pool = false;
  std::thread::id thread_id;
  std::atomic<int> num_callbacks{0};
  std::atomic<bool> done{false};
};

std::vector<std::string> Tablets(std::initializer_list<std::string> ids) {
  return ids;
}

} // namespace

class MultiRaftBatcherTest : public YBTest {
 protected:
  void SetUp() override {
    YBTest::SetUp();
    // Only the flush path under test may fire.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_heartbeat_interval_ms) = kNeverMs;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_flush_delay_ms) = kNeverMs;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_consensus_rpc_timeout_ms) = 1000;
    messenger_ = ASSERT_RESULT(rpc::MessengerBuilder("multi-raft-batcher-test").Build());
    metric_entity_ = METRIC_ENTITY_server.Instantiate(&metric_registry_, "test-server");
  }

  void TearDown() override {
    if (batcher_) {
      batcher_->Shutdown();
    }
    WaitAllDone();
    batcher_.reset();
    messenger_->Shutdown();
    YBTest::TearDown();
  }

  void CreateBatcher(bool start = true) {
    batcher_ = std::make_shared<MultiRaftBatcher>(
        messenger_.get(), std::make_unique<FakeBatchSender>(sender_state_), &running_calls_,
        metric_entity_);
    if (start) {
      batcher_->Start();
    }
  }

  ConsensusRequestPB MakeRequest(
      const std::string& tablet_id, size_t payload_bytes, int num_ops = 0) {
    ConsensusRequestPB request;
    request.set_dest_uuid("follower-1");
    request.set_tablet_id(tablet_id);
    request.set_caller_uuid(std::string(payload_bytes, 'x'));
    request.set_caller_term(1);
    for (int i = 1; i <= num_ops; ++i) {
      auto* op = request.add_ops();
      op->mutable_id()->set_term(1);
      op->mutable_id()->set_index(i);
      op->set_hybrid_time(i);
      op->set_op_type(NO_OP);
    }
    return request;
  }

  size_t RequestBytes(const std::string& tablet_id, size_t payload_bytes = 100) {
    return MakeRequest(tablet_id, payload_bytes).ByteSizeLong();
  }

  // Creates a call without adding it to the batcher.
  TestCall& NewCall(
      const std::string& tablet_id, size_t payload_bytes = 100,
      std::function<void()> on_callback = {}) {
    auto& call = *calls_.emplace_back(std::make_unique<TestCall>());
    call.tablet_id = tablet_id;
    call.request = MakeRequest(tablet_id, payload_bytes);
    call.on_callback = std::move(on_callback);
    return call;
  }

  // Adds a call created by NewCall to the batcher. Safe to call from any thread.
  void Submit(TestCall& call, RequestKind kind) {
    auto* high_pool = &messenger_->ThreadPool(rpc::ServicePriority::kHigh);
    call.handle = batcher_->AddRequestToBatch(
        &call.request, &call.response, [&call, high_pool](const Status& status) {
          call.status = status;
          call.on_high_pool = high_pool->OwnsThisThread();
          call.thread_id = std::this_thread::get_id();
          ++call.num_callbacks;
          if (call.on_callback) {
            call.on_callback();
          }
          call.done.store(true, std::memory_order_release);
        },
        kind);
  }

  TestCall& Add(
      const std::string& tablet_id, RequestKind kind, size_t payload_bytes = 100,
      std::function<void()> on_callback = {}) {
    auto& call = NewCall(tablet_id, payload_bytes, std::move(on_callback));
    Submit(call, kind);
    return call;
  }

  size_t NumEnvelopes() {
    return sender_state_->NumEnvelopes();
  }

  std::shared_ptr<FakeBatchEnvelope> GetEnvelope(size_t idx) {
    return sender_state_->GetEnvelope(idx);
  }

  // Fills the envelope's response with num_responses sub-responses (default: one per request),
  // then completes the RPC with status, on this thread.
  void Complete(size_t idx, const Status& status = Status::OK(), int num_responses = -1) {
    ASSERT_TRUE(sender_state_->Complete(
        idx, status,
        [](size_t, const ConsensusRequestPB& request, ConsensusResponsePB* response) {
          response->set_responder_uuid(request.tablet_id() + "-resp");
        },
        num_responses));
  }

  bool IsDone(const TestCall& call) {
    return call.done.load(std::memory_order_acquire);
  }

  void WaitDone(const TestCall& call) {
    ASSERT_OK(WaitFor([this, &call] { return IsDone(call); }, 10s, "Callback " + call.tablet_id));
  }

  void WaitAllDone() {
    // Calls whose envelope was never completed have no callback to wait for.
    std::vector<TestCall*> pending;
    for (auto& call : calls_) {
      if (!IsDone(*call)) {
        pending.push_back(call.get());
      }
    }
    for (auto* call : pending) {
      // Give dispatched tasks a chance to finish; ignore calls that were never completed.
      auto status = WaitFor([this, call] { return IsDone(*call); }, 1s, "Callback");
      (void)status;
    }
  }

  int64_t CounterValue(const CounterPrototype& proto) {
    return proto.Instantiate(metric_entity_)->value();
  }

  scoped_refptr<EventStats> Stats(const EventStatsPrototype& proto) {
    return proto.Instantiate(metric_entity_);
  }

  std::unique_ptr<rpc::Messenger> messenger_;
  MetricRegistry metric_registry_;
  MetricEntityPtr metric_entity_;
  std::atomic<int> running_calls_{0};
  // Destroyed after the sender state, which may hold callbacks that reference these calls.
  std::vector<std::unique_ptr<TestCall>> calls_;
  std::shared_ptr<FakeBatchSenderState> sender_state_ = std::make_shared<FakeBatchSenderState>();
  std::shared_ptr<MultiRaftBatcher> batcher_;
};

TEST_F(MultiRaftBatcherTest, ImmediateFlushOnFirstDataAdd) {
  CreateBatcher();
  auto& d1 = Add("t1", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 1);
  auto envelope = GetEnvelope(0);
  ASSERT_EQ(envelope->tablet_ids, Tablets({"t1"}));
  ASSERT_EQ(envelope->callback_mode, rpc::InvokeCallbackMode::kThreadPoolHigh);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_immediate), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_envelopes_sent), 1);
  ASSERT_EQ(running_calls_.load(), 1);

  Complete(0);
  ASSERT_NO_FATALS(WaitDone(d1));
  ASSERT_OK(d1.status);
  ASSERT_EQ(d1.response.responder_uuid(), "t1-resp");
  ASSERT_EQ(running_calls_.load(), 0);
}

TEST_F(MultiRaftBatcherTest, AccumulateThenFlushOnResponse) {
  CreateBatcher();
  auto& d1 = Add("t1", RequestKind::kData);
  auto& d2 = Add("t2", RequestKind::kData);
  auto& d3 = Add("t3", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 1);

  Complete(0);
  ASSERT_EQ(NumEnvelopes(), 2);
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t2", "t3"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_response_clocked), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_immediate), 1);

  Complete(1);
  for (auto* call : {&d1, &d2, &d3}) {
    ASSERT_NO_FATALS(WaitDone(*call));
    ASSERT_OK(call->status);
    ASSERT_EQ(call->response.responder_uuid(), call->tablet_id + "-resp");
  }
  // Nothing was pending when the second envelope completed.
  ASSERT_EQ(NumEnvelopes(), 2);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_deadline), 0);
}

TEST_F(MultiRaftBatcherTest, DeadlineFlushWhileResponseOutstanding) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_flush_delay_ms) = 20;
  CreateBatcher();
  Add("t1", RequestKind::kData);
  Add("t2", RequestKind::kData);
  ASSERT_OK(WaitFor([this] { return NumEnvelopes() == 2; }, 10s, "Deadline flush"));
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t2"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_deadline), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_response_clocked), 0);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_timer), 0);
  Complete(0);
  Complete(1);
}

// The deadline armed for a batch is cancelled when that batch is flushed another way, so it
// cannot cut short the wait of the next pending batch, which arms its own.
TEST_F(MultiRaftBatcherTest, ResponseFlushCancelsDeadline) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_flush_delay_ms) = 1000;
  CreateBatcher();
  Add("t1", RequestKind::kData);
  Add("t2", RequestKind::kData);
  Complete(0);
  ASSERT_EQ(NumEnvelopes(), 2);
  SleepFor(500ms);
  Add("t3", RequestKind::kData);
  // A negative check: past t2's deadline, but before t3's.
  SleepFor(700ms);
  ASSERT_EQ(NumEnvelopes(), 2);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_deadline), 0);
  ASSERT_OK(WaitFor([this] { return NumEnvelopes() == 3; }, 10s, "Deadline flush"));
  ASSERT_EQ(GetEnvelope(2)->tablet_ids, Tablets({"t3"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_deadline), 1);
  Complete(1);
  Complete(2);
}

TEST_F(MultiRaftBatcherTest, HeartbeatOnlyBatchDoesNotFlushEarly) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_flush_delay_ms) = 10;
  CreateBatcher();
  auto& h1 = Add("t1", RequestKind::kHeartbeat);
  // A negative check: wait well past the data deadline.
  SleepFor(200ms);
  ASSERT_EQ(NumEnvelopes(), 0);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_immediate), 0);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_deadline), 0);
  ASSERT_FALSE(IsDone(h1));
}

TEST_F(MultiRaftBatcherTest, HeartbeatRidesDataFlush) {
  CreateBatcher();
  auto& h1 = Add("t1", RequestKind::kHeartbeat);
  ASSERT_EQ(NumEnvelopes(), 0);
  auto& d2 = Add("t2", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 1);
  auto envelope = GetEnvelope(0);
  ASSERT_EQ(envelope->tablet_ids, Tablets({"t1", "t2"}));
  ASSERT_EQ(envelope->callback_mode, rpc::InvokeCallbackMode::kThreadPoolHigh);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_immediate), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_heartbeat_sub_requests), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_data_sub_requests), 1);
  auto sub_requests = Stats(METRIC_multi_raft_envelope_sub_requests);
  ASSERT_EQ(sub_requests->TotalCount(), 1);
  ASSERT_EQ(sub_requests->TotalSum(), 2);
  auto bytes = Stats(METRIC_multi_raft_envelope_bytes);
  ASSERT_EQ(bytes->TotalCount(), 1);
  ASSERT_EQ(bytes->TotalSum(), RequestBytes("t1") + RequestBytes("t2"));

  // The heartbeat callback runs on the completing thread; the data callback on the High pool.
  Complete(0);
  ASSERT_TRUE(IsDone(h1));
  ASSERT_EQ(h1.thread_id, std::this_thread::get_id());
  ASSERT_OK(h1.status);
  ASSERT_EQ(h1.response.responder_uuid(), "t1-resp");
  ASSERT_NO_FATALS(WaitDone(d2));
  ASSERT_TRUE(d2.on_high_pool);
  ASSERT_OK(d2.status);
  ASSERT_EQ(d2.response.responder_uuid(), "t2-resp");
}

// A heartbeat parked behind an in-flight data envelope neither arms the data deadline nor is
// response-clocked; it waits for the timer or rides the next data flush.
TEST_F(MultiRaftBatcherTest, HeartbeatParkedBehindDataInFlightWaitsForTimer) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_flush_delay_ms) = 10;
  CreateBatcher();
  auto& d1 = Add("t1", RequestKind::kData);
  auto& h2 = Add("t2", RequestKind::kHeartbeat);
  ASSERT_EQ(NumEnvelopes(), 1);
  // A negative check: wait well past the data deadline.
  SleepFor(200ms);
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_deadline), 0);

  Complete(0);
  ASSERT_NO_FATALS(WaitDone(d1));
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_response_clocked), 0);
  ASSERT_FALSE(IsDone(h2));

  // With nothing in flight, the next data add goes out immediately and carries the heartbeat.
  auto& d3 = Add("t3", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 2);
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t2", "t3"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_immediate), 2);
  Complete(1);
  for (auto* call : {&h2, &d3}) {
    ASSERT_NO_FATALS(WaitDone(*call));
    ASSERT_OK(call->status);
  }
}

// Sub-callbacks run outside the batcher lock and may add to the same batcher, as Raft peers do
// when they send their next request. The response-clocked flush happens first, so a data re-add
// waits behind it rather than going out immediately.
TEST_F(MultiRaftBatcherTest, SubCallbackReAddsToBatcher) {
  CreateBatcher();
  auto& h1b = NewCall("t1b");
  auto& d2b = NewCall("t2b");
  auto& h1 = Add("t1", RequestKind::kHeartbeat, 100, [this, &h1b] {
    Submit(h1b, RequestKind::kHeartbeat);
  });
  auto& d2 = Add("t2", RequestKind::kData, 100, [this, &d2b] {
    Submit(d2b, RequestKind::kData);
  });
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(GetEnvelope(0)->tablet_ids, Tablets({"t1", "t2"}));
  auto& d3 = Add("t3", RequestKind::kData);

  // The heartbeat callback re-adds inline on this thread.
  Complete(0);
  ASSERT_TRUE(IsDone(h1));
  ASSERT_NO_FATALS(WaitDone(d2));
  ASSERT_EQ(NumEnvelopes(), 2);
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t3"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_immediate), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_response_clocked), 1);

  // Both re-adds are pending behind the envelope carrying t3.
  Complete(1);
  ASSERT_NO_FATALS(WaitDone(d3));
  ASSERT_EQ(NumEnvelopes(), 3);
  ASSERT_EQ(GetEnvelope(2)->tablet_ids, Tablets({"t1b", "t2b"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_response_clocked), 2);
  Complete(2);
  for (auto* call : {&h1b, &d2b}) {
    ASSERT_NO_FATALS(WaitDone(*call));
    ASSERT_OK(call->status);
    ASSERT_EQ(call->response.responder_uuid(), call->tablet_id + "-resp");
  }
}

// Several threads add data and heartbeats, and fail some of their batches through the handle,
// while another completes envelopes as they appear, with the deadline and timer flushes active.
// Every request is sent exactly once and every callback runs exactly once, with its own response
// or with the failure.
TEST_F(MultiRaftBatcherTest, ConcurrentAdds) {
  constexpr int kThreads = 4;
  constexpr int kRequestsPerThread = 300;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_flush_delay_ms) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_heartbeat_interval_ms) = 5;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_batch_size) = 8;
  CreateBatcher();

  std::vector<std::vector<TestCall*>> per_thread(kThreads);
  for (int t = 0; t != kThreads; ++t) {
    for (int i = 0; i != kRequestsPerThread; ++i) {
      per_thread[t].push_back(&NewCall(Format("t$0-$1", t, i)));
    }
  }
  const auto total = kThreads * kRequestsPerThread;

  std::atomic<int> adders_running{kThreads};
  TestThreadHolder holder;
  for (int t = 0; t != kThreads; ++t) {
    holder.AddThread([this, &calls = per_thread[t], &adders_running] {
      int i = 0;
      for (auto* call : calls) {
        Submit(*call, i % 5 == 0 ? RequestKind::kHeartbeat : RequestKind::kData);
        call->handle.CallStateDebugString();
        call->handle.timeout();
        call->handle.finished();
        if (i % 10 == 3) {
          call->handle.MarkCallAsFailed();
        }
        ++i;
      }
      --adders_running;
    });
  }
  std::atomic<bool> completer_failed{false};
  holder.AddThread([this, &adders_running, &completer_failed] {
    size_t next = 0;
    const auto deadline = CoarseMonoClock::Now() + 60s;
    for (;;) {
      while (next < NumEnvelopes()) {
        // The envelope may have been failed through a handle already.
        sender_state_->Complete(
            next++, Status::OK(),
            [](size_t, const ConsensusRequestPB& request, ConsensusResponsePB* response) {
              response->set_responder_uuid(request.tablet_id() + "-resp");
            });
      }
      if (adders_running.load() == 0 && std::all_of(
              calls_.begin(), calls_.end(), [this](const auto& call) { return IsDone(*call); })) {
        return;
      }
      if (CoarseMonoClock::Now() > deadline) {
        completer_failed = true;
        return;
      }
      std::this_thread::sleep_for(1ms);
    }
  });
  holder.JoinAll();
  ASSERT_FALSE(completer_failed.load());

  std::unordered_map<std::string, int> sent;
  std::unordered_map<std::string, bool> marked_failed;
  size_t num_marked_failed = 0;
  for (size_t i = 0; i != NumEnvelopes(); ++i) {
    auto envelope = GetEnvelope(i);
    num_marked_failed += envelope->marked_failed;
    for (const auto& tablet_id : envelope->tablet_ids) {
      ++sent[tablet_id];
      marked_failed[tablet_id] = envelope->marked_failed;
    }
  }
  ASSERT_EQ(sent.size(), static_cast<size_t>(total));
  LOG(INFO) << "Envelopes: " << NumEnvelopes() << ", failed through a handle: "
            << num_marked_failed;
  for (const auto& call : calls_) {
    ASSERT_EQ(sent[call->tablet_id], 1) << call->tablet_id;
    ASSERT_EQ(call->num_callbacks.load(), 1) << call->tablet_id;
    if (marked_failed[call->tablet_id]) {
      ASSERT_TRUE(call->status.IsTimedOut()) << call->tablet_id << ": " << call->status;
      ASSERT_FALSE(call->response.has_responder_uuid()) << call->tablet_id;
    } else {
      ASSERT_OK(call->status);
      ASSERT_EQ(call->response.responder_uuid(), call->tablet_id + "-resp");
    }
  }
  ASSERT_EQ(CounterValue(METRIC_multi_raft_heartbeat_sub_requests) +
            CounterValue(METRIC_multi_raft_data_sub_requests), total);
  ASSERT_EQ(running_calls_.load(), 0);
}

TEST_F(MultiRaftBatcherTest, ByteCapSplitsBeforeOverLimitAdd) {
  const auto request_bytes = RequestBytes("t1");
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_batch_bytes) = 2 * request_bytes + request_bytes / 2;
  CreateBatcher();
  Add("t1", RequestKind::kData);
  Add("t2", RequestKind::kData);
  Add("t3", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_bytes_cap), 0);

  // The pending batch is sent before t4 is added, and t4 waits behind the two envelopes now in
  // flight.
  Add("t4", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 2);
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t2", "t3"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_bytes_cap), 1);

  Complete(1);
  ASSERT_EQ(NumEnvelopes(), 3);
  ASSERT_EQ(GetEnvelope(2)->tablet_ids, Tablets({"t4"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_response_clocked), 1);

  // Every request was sent exactly once.
  Complete(0);
  Complete(2);
  ASSERT_EQ(NumEnvelopes(), 3);
  for (auto& call : calls_) {
    ASSERT_NO_FATALS(WaitDone(*call));
    ASSERT_OK(call->status);
    ASSERT_EQ(call->response.responder_uuid(), call->tablet_id + "-resp");
  }
}

TEST_F(MultiRaftBatcherTest, CountCapSplitsBeforeOverLimitAdd) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_batch_size) = 2;
  CreateBatcher();
  Add("t1", RequestKind::kData);
  // Heartbeats do not count against the data cap.
  Add("t2", RequestKind::kHeartbeat);
  Add("t3", RequestKind::kData);
  Add("t4", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 1);

  Add("t5", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 2);
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t2", "t3", "t4"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_count_cap), 1);

  Complete(0);
  ASSERT_EQ(NumEnvelopes(), 3);
  ASSERT_EQ(GetEnvelope(2)->tablet_ids, Tablets({"t5"}));

  Complete(1);
  Complete(2);
  ASSERT_EQ(NumEnvelopes(), 3);
  for (auto& call : calls_) {
    ASSERT_NO_FATALS(WaitDone(*call));
    ASSERT_OK(call->status);
    ASSERT_EQ(call->response.responder_uuid(), call->tablet_id + "-resp");
  }
}

TEST_F(MultiRaftBatcherTest, OversizedRequestGoesAlone) {
  const auto request_bytes = RequestBytes("t1");
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_batch_bytes) = request_bytes;
  CreateBatcher();
  // Oversized into an empty batch with nothing in flight: an immediate flush, not a cap flush.
  Add("big1", RequestKind::kData, 5 * request_bytes);
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(GetEnvelope(0)->tablet_ids, Tablets({"big1"}));
  // Oversized into an empty batch while an envelope is in flight: it waits.
  Add("big2", RequestKind::kData, 5 * request_bytes);
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_bytes_cap), 0);
  // The next add sends it alone.
  Add("t3", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 2);
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"big2"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_bytes_cap), 1);

  Complete(0);
  ASSERT_EQ(NumEnvelopes(), 3);
  ASSERT_EQ(GetEnvelope(2)->tablet_ids, Tablets({"t3"}));
  Complete(1);
  Complete(2);
}

// A data add with nothing in flight that hits a cap on parked heartbeats: the heartbeats go out
// first (as a cap flush), then the data request goes out immediately in its own envelope.
TEST_F(MultiRaftBatcherTest, CapFlushOfHeartbeatsThenImmediateData) {
  const auto request_bytes = RequestBytes("t1");
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_batch_bytes) = request_bytes + request_bytes / 2;
  CreateBatcher();
  Add("t1", RequestKind::kHeartbeat);
  ASSERT_EQ(NumEnvelopes(), 0);
  Add("t2", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 2);
  ASSERT_EQ(GetEnvelope(0)->tablet_ids, Tablets({"t1"}));
  ASSERT_EQ(GetEnvelope(0)->callback_mode, rpc::InvokeCallbackMode::kThreadPoolNormal);
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t2"}));
  ASSERT_EQ(GetEnvelope(1)->callback_mode, rpc::InvokeCallbackMode::kThreadPoolHigh);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_bytes_cap), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_immediate), 1);
  Complete(0);
  Complete(1);
}

TEST_F(MultiRaftBatcherTest, TimeoutCapApplied) {
  CreateBatcher();
  Add("t1", RequestKind::kData);
  ASSERT_EQ(GetEnvelope(0)->timeout, 1000ms);

  // Default cap is 2 * consensus_rpc_timeout_ms.
  Add("t2", RequestKind::kData);
  Add("t3", RequestKind::kHeartbeat);
  Add("t4", RequestKind::kData);
  Complete(0);
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t2", "t3", "t4"}));
  ASSERT_EQ(GetEnvelope(1)->timeout, 2000ms);

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_rpc_timeout_ms) = 2500;
  Add("t5", RequestKind::kData);
  Add("t6", RequestKind::kData);
  Add("t7", RequestKind::kData);
  Complete(1);
  ASSERT_EQ(GetEnvelope(2)->tablet_ids, Tablets({"t5", "t6", "t7"}));
  ASSERT_EQ(GetEnvelope(2)->timeout, 2500ms);
  Complete(2);
}

// Heartbeat-only envelopes keep consensus_rpc_timeout_ms * count, the normal pool, and run their
// callbacks serially on the completing thread.
TEST_F(MultiRaftBatcherTest, HeartbeatOnlyEnvelopeUnchanged) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_batch_size) = 3;
  CreateBatcher();
  std::vector<std::string> order;
  auto record = [&order](const std::string& id) {
    return [&order, id] { order.push_back(id); };
  };
  auto& h1 = Add("t1", RequestKind::kHeartbeat, 100, record("t1"));
  auto& h2 = Add("t2", RequestKind::kHeartbeat, 100, record("t2"));
  ASSERT_EQ(NumEnvelopes(), 0);
  auto& h3 = Add("t3", RequestKind::kHeartbeat, 100, record("t3"));
  ASSERT_EQ(NumEnvelopes(), 1);
  auto envelope = GetEnvelope(0);
  ASSERT_EQ(envelope->tablet_ids, Tablets({"t1", "t2", "t3"}));
  ASSERT_EQ(envelope->timeout, 3000ms);
  ASSERT_EQ(envelope->callback_mode, rpc::InvokeCallbackMode::kThreadPoolNormal);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_count_cap), 1);

  Complete(0);
  ASSERT_EQ(order, Tablets({"t1", "t2", "t3"}));
  for (auto* call : {&h1, &h2, &h3}) {
    ASSERT_TRUE(IsDone(*call));
    ASSERT_EQ(call->thread_id, std::this_thread::get_id());
    ASSERT_OK(call->status);
  }
}

TEST_F(MultiRaftBatcherTest, ShutdownAbortsPendingBatch) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_data_flush_delay_ms) = 50;
  CreateBatcher();
  auto& d1 = Add("t1", RequestKind::kData);
  auto& h2 = Add("t2", RequestKind::kHeartbeat);
  auto& d3 = Add("t3", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 1);

  batcher_->Shutdown();
  for (auto* call : {&h2, &d3}) {
    ASSERT_TRUE(IsDone(*call));
    ASSERT_TRUE(call->status.IsAborted()) << call->status;
  }
  auto& d4 = Add("t4", RequestKind::kData);
  ASSERT_TRUE(IsDone(d4));
  ASSERT_TRUE(d4.status.IsAborted()) << d4.status;

  // The envelope in flight at shutdown still completes its callbacks and sends nothing new.
  Complete(0);
  ASSERT_NO_FATALS(WaitDone(d1));
  ASSERT_OK(d1.status);
  // A negative check: the deadline armed by t3 must not fire.
  SleepFor(200ms);
  ASSERT_EQ(NumEnvelopes(), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_deadline), 0);
  ASSERT_EQ(running_calls_.load(), 0);
}

TEST_F(MultiRaftBatcherTest, ShutdownWithoutStart) {
  CreateBatcher(/* start= */ false);
  batcher_->Shutdown();
  batcher_->Shutdown();
}

TEST_F(MultiRaftBatcherTest, BatcherReleasedWhileEnvelopeInFlight) {
  CreateBatcher();
  auto& d1 = Add("t1", RequestKind::kData);
  batcher_->Shutdown();
  batcher_.reset();
  Complete(0);
  ASSERT_NO_FATALS(WaitDone(d1));
  ASSERT_OK(d1.status);
}

TEST_F(MultiRaftBatcherTest, BackstopTimerFlushesWhenResponseClockingStalls) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_heartbeat_interval_ms) = 100;
  CreateBatcher();
  Add("t1", RequestKind::kData);
  Add("t2", RequestKind::kData);
  ASSERT_OK(WaitFor([this] { return NumEnvelopes() == 2; }, 10s, "Timer flush"));
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t2"}));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_timer), 1);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_deadline), 0);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_flush_response_clocked), 0);
  Complete(0);
  Complete(1);
}

TEST_F(MultiRaftBatcherTest, DataSubCallbacksRunAsSeparateHighPoolTasks) {
  CreateBatcher();
  Add("t1", RequestKind::kData);
  // Each callback blocks until both have started, which only succeeds if they run concurrently.
  CountDownLatch latch(2);
  std::atomic<int> concurrent{0};
  auto rendezvous = [&latch, &concurrent] {
    latch.CountDown();
    if (latch.WaitFor(10s)) {
      ++concurrent;
    }
  };
  auto& d2 = Add("t2", RequestKind::kData, 100, rendezvous);
  auto& d3 = Add("t3", RequestKind::kData, 100, rendezvous);
  Complete(0);
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t2", "t3"}));
  Complete(1);
  for (auto* call : {&d2, &d3}) {
    ASSERT_NO_FATALS(WaitDone(*call));
    ASSERT_TRUE(call->on_high_pool);
    ASSERT_NE(call->thread_id, std::this_thread::get_id());
    ASSERT_OK(call->status);
  }
  ASSERT_EQ(concurrent.load(), 2);
}

TEST_F(MultiRaftBatcherTest, EnvelopeFailureFansOutWithoutSwap) {
  CreateBatcher();
  auto& h1 = Add("t1", RequestKind::kHeartbeat);
  h1.response.set_responder_uuid("stale-t1");
  auto& d2 = Add("t2", RequestKind::kData);
  d2.response.set_responder_uuid("stale-t2");
  const auto h1_before = h1.response.SerializeAsString();
  const auto d2_before = d2.response.SerializeAsString();

  // The fake still fills the batch response, which the batcher must not hand out.
  Complete(0, STATUS(TimedOut, "Envelope timed out"));
  for (auto* call : {&h1, &d2}) {
    ASSERT_NO_FATALS(WaitDone(*call));
    ASSERT_TRUE(call->status.IsTimedOut()) << call->status;
  }
  ASSERT_EQ(h1.response.SerializeAsString(), h1_before);
  ASSERT_EQ(d2.response.SerializeAsString(), d2_before);
}

TEST_F(MultiRaftBatcherTest, ResponseCountMismatchFailsAllCallbacks) {
  CreateBatcher();
  auto& h1 = Add("t1", RequestKind::kHeartbeat);
  auto& d2 = Add("t2", RequestKind::kData);
  StringWaiterLogSink log_waiter("1 responses for 2 requests, destination: follower-1");
  Complete(0, Status::OK(), /* num_responses= */ 1);
  for (auto* call : {&h1, &d2}) {
    ASSERT_NO_FATALS(WaitDone(*call));
    ASSERT_TRUE(call->status.IsIllegalState()) << call->status;
    ASSERT_FALSE(call->response.has_responder_uuid());
  }
  ASSERT_TRUE(log_waiter.IsEventOccurred());
}

// An envelope that fails as a whole counts once, whatever it carries. Successful envelopes, errors
// inside sub-responses, and a pending batch aborted at shutdown do not count.
TEST_F(MultiRaftBatcherTest, EnvelopesFailedMetric) {
  CreateBatcher();
  auto& d1 = Add("t1", RequestKind::kData);
  ASSERT_TRUE(sender_state_->Complete(
      0, Status::OK(),
      [](size_t, const ConsensusRequestPB&, ConsensusResponsePB* response) {
        response->mutable_error()->set_code(tserver::TabletServerErrorPB::TABLET_NOT_FOUND);
      }));
  ASSERT_NO_FATALS(WaitDone(d1));
  ASSERT_OK(d1.status);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_envelopes_failed), 0);

  // The RPC fails.
  auto& d2 = Add("t2", RequestKind::kData);
  auto& h3 = Add("t3", RequestKind::kHeartbeat);
  ASSERT_EQ(NumEnvelopes(), 2);
  Complete(1, STATUS(TimedOut, "Envelope timed out"));
  ASSERT_NO_FATALS(WaitDone(d2));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_envelopes_failed), 1);

  // Fewer responses than requests.
  auto& d4 = Add("t4", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 3);
  ASSERT_EQ(GetEnvelope(2)->tablet_ids, Tablets({"t3", "t4"}));
  Complete(2, Status::OK(), /* num_responses= */ 1);
  for (auto* call : {&h3, &d4}) {
    ASSERT_NO_FATALS(WaitDone(*call));
    ASSERT_TRUE(call->status.IsIllegalState()) << call->status;
  }
  ASSERT_EQ(CounterValue(METRIC_multi_raft_envelopes_failed), 2);

  // A heartbeat-only envelope.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_batch_size) = 1;
  auto& h5 = Add("t5", RequestKind::kHeartbeat);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_multi_raft_batch_size) = 0;
  ASSERT_EQ(NumEnvelopes(), 4);
  Complete(3, STATUS(NetworkError, "Connection refused"));
  ASSERT_NO_FATALS(WaitDone(h5));
  ASSERT_EQ(CounterValue(METRIC_multi_raft_envelopes_failed), 3);

  // Shutdown aborts the pending batch, which was never sent.
  auto& d6 = Add("t6", RequestKind::kData);
  auto& d7 = Add("t7", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 5);
  batcher_->Shutdown();
  ASSERT_NO_FATALS(WaitDone(d7));
  ASSERT_TRUE(d7.status.IsAborted()) << d7.status;
  Complete(4);
  ASSERT_NO_FATALS(WaitDone(d6));
  ASSERT_OK(d6.status);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_envelopes_failed), 3);
  ASSERT_EQ(CounterValue(METRIC_multi_raft_envelopes_sent), 5);
}

TEST_F(MultiRaftBatcherTest, EnqueueFailureRunsSubCallbackInline) {
  messenger_->ThreadPool(rpc::ServicePriority::kHigh).Shutdown();
  CreateBatcher();
  auto& d1 = Add("t1", RequestKind::kData);
  Complete(0);
  ASSERT_TRUE(IsDone(d1));
  ASSERT_EQ(d1.thread_id, std::this_thread::get_id());
  ASSERT_OK(d1.status);
  ASSERT_EQ(d1.response.responder_uuid(), "t1-resp");
}

TEST_F(MultiRaftBatcherTest, BatchHandle) {
  CreateBatcher();
  MultiRaftBatcher::BatchHandle empty_handle;
  ASSERT_TRUE(empty_handle.expired());
  empty_handle.MarkCallAsFailed();
  ASSERT_EQ(empty_handle.CallStateDebugString(), "batch released");

  auto& d1 = Add("t1", RequestKind::kData);
  auto& d2 = Add("t2", RequestKind::kData);
  auto& h3 = Add("t3", RequestKind::kHeartbeat);
  ASSERT_EQ(d1.handle.TEST_controller(), GetEnvelope(0)->controller);
  // Sent, but the fake sender issues no real RPC call.
  ASSERT_EQ(d1.handle.CallStateDebugString(), "call not set");

  // Pending: the handle names the next batch, and its methods do nothing.
  ASSERT_NE(d2.handle.TEST_controller(), nullptr);
  ASSERT_NE(d2.handle.TEST_controller(), GetEnvelope(0)->controller);
  ASSERT_EQ(d2.handle.TEST_controller(), h3.handle.TEST_controller());
  ASSERT_EQ(d2.handle.CallStateDebugString(), "batch not sent");
  d2.handle.MarkCallAsFailed();

  Complete(0);
  ASSERT_EQ(d2.handle.TEST_controller(), GetEnvelope(1)->controller);
  ASSERT_NO_FATALS(WaitDone(d1));
  ASSERT_OK(WaitFor([&d1] { return d1.handle.expired(); }, 10s, "Batch released"));
  ASSERT_EQ(d1.handle.TEST_controller(), nullptr);
  ASSERT_EQ(d1.handle.CallStateDebugString(), "batch released");
  d1.handle.MarkCallAsFailed();
  Complete(1);
}

TEST_F(MultiRaftBatcherTest, BatchHandleTimeoutAndFinished) {
  CreateBatcher();
  MultiRaftBatcher::BatchHandle empty_handle;
  ASSERT_FALSE(empty_handle.timeout().Initialized());
  ASSERT_TRUE(empty_handle.finished());

  auto& d1 = Add("t1", RequestKind::kData);
  auto& d2 = Add("t2", RequestKind::kData);
  auto& d3 = Add("t3", RequestKind::kData);
  // Sent.
  ASSERT_EQ(d1.handle.timeout(), 1000ms);
  ASSERT_FALSE(d1.handle.finished());
  // Pending: not inspectable yet.
  ASSERT_FALSE(d2.handle.timeout().Initialized());
  ASSERT_FALSE(d2.handle.finished());

  // Completed while its sub-callback still holds the batch.
  CountDownLatch release_d1(1);
  d1.on_callback = [&release_d1] { release_d1.Wait(); };
  auto release_on_failure = ScopeExit([&release_d1] { release_d1.CountDown(); });
  Complete(0);
  ASSERT_FALSE(d1.handle.expired());
  ASSERT_TRUE(d1.handle.finished());
  ASSERT_EQ(d1.handle.timeout(), 1000ms);
  // The response-clocked envelope carrying t2 and t3 is now sent.
  ASSERT_EQ(d2.handle.timeout(), 2000ms);
  ASSERT_FALSE(d2.handle.finished());

  // Released.
  release_d1.CountDown();
  ASSERT_NO_FATALS(WaitDone(d1));
  ASSERT_OK(WaitFor([&d1] { return d1.handle.expired(); }, 10s, "Batch released"));
  ASSERT_TRUE(d1.handle.finished());
  ASSERT_FALSE(d1.handle.timeout().Initialized());
  Complete(1);
  for (auto* call : {&d2, &d3}) {
    ASSERT_NO_FATALS(WaitDone(*call));
  }
}

// Failing a batch through the handle fails the whole envelope, through the sender.
TEST_F(MultiRaftBatcherTest, BatchHandleMarkCallAsFailed) {
  CreateBatcher();
  auto& h1 = Add("t1", RequestKind::kHeartbeat);
  auto& d2 = Add("t2", RequestKind::kData);
  auto& d3 = Add("t3", RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 1);
  // Pending: nothing to fail.
  d3.handle.MarkCallAsFailed();
  ASSERT_FALSE(IsDone(d3));

  d2.handle.MarkCallAsFailed();
  ASSERT_TRUE(GetEnvelope(0)->marked_failed);
  for (auto* call : {&h1, &d2}) {
    ASSERT_NO_FATALS(WaitDone(*call));
    ASSERT_TRUE(call->status.IsTimedOut()) << call->status;
    ASSERT_EQ(call->num_callbacks.load(), 1);
  }
  // Finished: a second failure does nothing.
  h1.handle.MarkCallAsFailed();
  ASSERT_EQ(h1.num_callbacks.load(), 1);

  // The failed envelope response-clocked the next one.
  ASSERT_EQ(NumEnvelopes(), 2);
  ASSERT_EQ(GetEnvelope(1)->tablet_ids, Tablets({"t3"}));
  Complete(1);
  ASSERT_NO_FATALS(WaitDone(d3));
  ASSERT_OK(d3.status);
  ASSERT_FALSE(GetEnvelope(1)->marked_failed);
}

TEST_F(MultiRaftBatcherTest, EnvelopeOpsMetric) {
  CreateBatcher();
  auto& h1 = NewCall("t1");
  Submit(h1, RequestKind::kHeartbeat);
  auto& d2 = NewCall("t2");
  d2.request = MakeRequest("t2", 100, /* num_ops= */ 3);
  Submit(d2, RequestKind::kData);
  ASSERT_EQ(NumEnvelopes(), 1);
  auto& d3 = NewCall("t3");
  d3.request = MakeRequest("t3", 100, /* num_ops= */ 2);
  Submit(d3, RequestKind::kData);
  auto& d4 = NewCall("t4");
  d4.request = MakeRequest("t4", 100, /* num_ops= */ 4);
  Submit(d4, RequestKind::kData);
  Complete(0);
  ASSERT_EQ(NumEnvelopes(), 2);
  auto ops = Stats(METRIC_multi_raft_envelope_ops);
  ASSERT_EQ(ops->TotalCount(), 2);
  ASSERT_EQ(ops->TotalSum(), 3 + 2 + 4);
  ASSERT_EQ(ops->MaxValue(), 6);
  Complete(1);
}

// The time spent in each data sub-callback is recorded; heartbeat sub-callbacks are not counted.
TEST_F(MultiRaftBatcherTest, BatchedResponseProcessingTimeMetric) {
  CreateBatcher();
  constexpr auto kCallbackTime = 30ms;
  auto slow_callback = [kCallbackTime] { std::this_thread::sleep_for(kCallbackTime); };
  auto& h1 = Add("t1", RequestKind::kHeartbeat);
  auto& d2 = Add("t2", RequestKind::kData, 100, slow_callback);
  ASSERT_EQ(NumEnvelopes(), 1);
  auto& d3 = Add("t3", RequestKind::kData, 100, slow_callback);
  auto& d4 = Add("t4", RequestKind::kData, 100, slow_callback);
  Complete(0);
  ASSERT_NO_FATALS(WaitDone(h1));
  ASSERT_NO_FATALS(WaitDone(d2));
  ASSERT_EQ(NumEnvelopes(), 2);
  // A failed envelope's data sub-callbacks are timed too.
  Complete(1, STATUS(TimedOut, "Test timeout"));
  ASSERT_NO_FATALS(WaitDone(d3));
  ASSERT_NO_FATALS(WaitDone(d4));

  // Recorded after the callback returns.
  auto stats = Stats(METRIC_multi_raft_batched_response_processing_us);
  ASSERT_OK(WaitFor([&stats] { return stats->TotalCount() >= 3; }, 10s, "Recorded"));
  ASSERT_EQ(stats->TotalCount(), 3);
  ASSERT_GE(stats->TotalSum(), 3 * ToMicroseconds(kCallbackTime));
}

// A batcher exists when either batching flag is set.
TEST_F(MultiRaftBatcherTest, AddOrGetBatcherEitherFlag) {
  rpc::ProxyCache proxy_cache(messenger_.get());
  auto manager = std::make_shared<MultiRaftManager>(
      messenger_.get(), &proxy_cache, CloudInfoPB(), metric_entity_);
  RaftPeerPB peer_pb;
  peer_pb.set_permanent_uuid("follower-1");
  auto* addr = peer_pb.add_last_known_private_addr();
  addr->set_host("127.0.0.1");
  addr->set_port(12345);

  for (auto heartbeat : {false, true}) {
    for (auto data : {false, true}) {
      ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_heartbeat_batcher) = heartbeat;
      ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_multi_raft_data_batching) = data;
      auto batcher = manager->AddOrGetBatcher(peer_pb);
      ASSERT_EQ(batcher != nullptr, heartbeat || data)
          << "heartbeat: " << heartbeat << ", data: " << data;
    }
  }
  manager->StartShutdown();
  manager->CompleteShutdown();
}

TEST_F(MultiRaftBatcherTest, RecordDirectDataSend) {
  CreateBatcher();
  batcher_->RecordDirectDataSend();
  batcher_->RecordDirectDataSend();
  ASSERT_EQ(CounterValue(METRIC_multi_raft_data_sends_direct), 2);
  ASSERT_EQ(NumEnvelopes(), 0);
}

} // namespace yb::consensus
