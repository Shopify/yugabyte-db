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

#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "yb/consensus/consensus.messages.h"
#include "yb/consensus/consensus.pb.h"
#include "yb/consensus/multi_raft_batcher.h"

#include "yb/rpc/messenger.h"

#include "yb/util/countdown_latch.h"
#include "yb/util/logging.h"
#include "yb/util/ref_cnt_buffer.h"
#include "yb/util/source_location.h"
#include "yb/util/status.h"
#include "yb/util/thread.h"
#include "yb/util/tsan_util.h"

namespace yb::consensus {

// Whether this is a reactor thread of a messenger: reactor threads are named
// "<messenger name>_reactor...".
inline bool IsReactorThread() {
  auto* thread = Thread::current_thread();
  return thread && thread->name().find("_reactor") != std::string::npos;
}

// Runs f on a reactor thread of messenger, where the RPC layer runs response callbacks, and waits
// for it to return.
inline void RunOnReactor(rpc::Messenger* messenger, const std::function<void()>& f) {
  CountDownLatch done(1);
  auto task_id = messenger->ScheduleOnReactor(
      [&f, &done](const Status& status) {
        CHECK_OK(status);
        f();
        done.CountDown();
      },
      MonoDelta::kZero, SOURCE_LOCATION());
  CHECK_OK(task_id);
  done.Wait();
}

// One MultiRaftUpdateConsensus RPC captured by FakeBatchSender.
struct FakeBatchEnvelope {
  // What went on the wire: the envelope serialized inside SendBatch, as the proxy does, and parsed
  // back.
  MultiRaftConsensusRequestPB req;
  std::vector<std::string> tablet_ids;
  // The batcher's envelope, valid only inside SendBatch (in the send hook), null afterwards.
  const LWMultiRaftConsensusRequestPB* lw_req = nullptr;
  // The batch's response. Valid only until the envelope is completed, null afterwards: the batch
  // may be released as soon as its done callback has run.
  LWMultiRaftConsensusResponsePB* resp = nullptr;
  // The sub-responses written into resp by Complete, in order. For address comparisons only: they
  // live as long as the batch.
  std::vector<const LWConsensusResponsePB*> responses;
  rpc::RpcController* controller = nullptr;
  // When SendBatch was called.
  std::chrono::steady_clock::time_point sent_at;
  MonoDelta timeout;
  rpc::InvokeCallbackMode callback_mode = rpc::InvokeCallbackMode::kThreadPoolNormal;
  // Whether the controller had a partial-response callback when the envelope was sent.
  bool had_partial_callback = false;
  // Set when the envelope was failed through MultiRaftBatchSender::MarkCallAsFailed.
  bool marked_failed = false;

  // Taken under FakeBatchSenderState's mutex by whoever completes the envelope.
  MultiRaftBatchSender::DoneCallback done;
  // Taken from the controller in SendBatch, as the proxy moves it into the call, and dropped
  // together with done, as the call drops it when it finishes. A partial delivery copies it under
  // the mutex and invokes it outside, so it may finish after the envelope was completed.
  std::shared_ptr<rpc::PartialResponseCallback> partial_callback;
};

// What a FakeBatchSender has sent. Shared by the sender, which the batcher owns, and the test.
class FakeBatchSenderState {
 public:
  // Fills the response to the request at the given index of an envelope. request is the wire
  // copy. response is copied into the batch's response on its arena.
  using FillResponse = std::function<void(
      size_t idx, const ConsensusRequestPB& request, ConsensusResponsePB* response)>;
  // Runs on the sending thread, inside SendBatch, after the envelope is serialized and recorded.
  using SendHook = std::function<void(size_t envelope_idx)>;

  // Installs a send hook for its lifetime. On destruction it uninstalls the hook and waits for
  // every running invocation to return, so a hook never runs after the locals it captures: declare
  // it after them. Sends can come from pool, timer and reactor threads after the test body has
  // returned, on success as well as on an early ASSERT exit. The hook must not destroy its scope.
  class ScopedSendHook {
   public:
    ScopedSendHook(std::shared_ptr<FakeBatchSenderState> state, SendHook hook)
        : state_(std::move(state)) {
      state_->SetSendHook(std::move(hook));
    }

    ~ScopedSendHook() {
      state_->ClearSendHook();
    }

    ScopedSendHook(const ScopedSendHook&) = delete;
    void operator=(const ScopedSendHook&) = delete;

   private:
    const std::shared_ptr<FakeBatchSenderState> state_;
  };

  size_t NumEnvelopes() {
    std::lock_guard lock(mutex_);
    return envelopes_.size();
  }

  std::shared_ptr<FakeBatchEnvelope> GetEnvelope(size_t idx) {
    std::lock_guard lock(mutex_);
    CHECK_LT(idx, envelopes_.size());
    return envelopes_[idx];
  }

  // Completes the envelope with status, on this thread. With an OK status, first fills
  // num_responses sub-responses (default: one per request) using fill. Returns false if the
  // envelope was already completed.
  bool Complete(
      size_t idx, const Status& status, const FillResponse& fill, int num_responses = -1) {
    return DoComplete(idx, status, status.ok() ? fill : FillResponse(), num_responses);
  }

  // Fills one sub-response per request using fill, then completes the envelope with the error
  // status: a well-formed response that the batcher must not hand out.
  bool FailWithResponses(size_t idx, const Status& status, const FillResponse& fill) {
    CHECK(!status.ok());
    return DoComplete(idx, status, fill, /* num_responses= */ -1);
  }

  // Fails the uncompleted envelope sent on controller with TimedOut, as RpcController does.
  void MarkCallAsFailed(rpc::RpcController* controller) {
    MultiRaftBatchSender::DoneCallback done;
    std::shared_ptr<rpc::PartialResponseCallback> partial_callback;
    {
      std::lock_guard lock(mutex_);
      for (auto it = envelopes_.rbegin(); it != envelopes_.rend(); ++it) {
        auto& envelope = **it;
        if (envelope.controller == controller && envelope.done) {
          envelope.marked_failed = true;
          done = std::move(envelope.done);
          envelope.done = nullptr;
          partial_callback = std::move(envelope.partial_callback);
          envelope.resp = nullptr;
          break;
        }
      }
    }
    // Released before the final callback runs, as the call does.
    partial_callback.reset();
    if (done) {
      done(STATUS(TimedOut, "Forced timed out detected by sender."));
    }
  }

  // Builds a response frame for the given requests of an envelope, using fill. Lists them in
  // index (a streamed frame) if with_index.
  MultiRaftConsensusResponsePB MakeFrame(
      size_t envelope_idx, const std::vector<uint32_t>& indices, const FillResponse& fill,
      bool with_index = true) {
    auto envelope = GetEnvelope(envelope_idx);
    MultiRaftConsensusResponsePB frame;
    for (auto idx : indices) {
      fill(idx, envelope->req.consensus_request(idx), frame.add_consensus_response());
      if (with_index) {
        frame.add_index(idx);
      }
    }
    return frame;
  }

  // Delivers frame as a partial response frame of the envelope, on this thread, through the
  // partial-response callback. Returns false if it was dropped, as the connection drops a frame
  // for a finished call: the envelope was completed, or it has no callback.
  bool DeliverPartial(size_t idx, const MultiRaftConsensusResponsePB& frame) {
    return DeliverPartialBytes(idx, frame.SerializeAsString());
  }

  // Delivers body, which need not parse, as a partial response frame. See DeliverPartial.
  bool DeliverPartialBytes(size_t idx, Slice body) {
    std::shared_ptr<rpc::PartialResponseCallback> callback;
    {
      std::lock_guard lock(mutex_);
      CHECK_LT(idx, envelopes_.size());
      auto& envelope = *envelopes_[idx];
      if (!envelope.done || !envelope.partial_callback) {
        return false;
      }
      callback = envelope.partial_callback;
    }
    RefCntBuffer buffer(body);
    (*callback)(Slice(buffer.data(), buffer.size()), buffer);
    return true;
  }

  // Completes the envelope on this thread with frame as its final response (copied into the
  // batch's response, as the RPC layer parses it there) and status. Returns false if the envelope
  // was already completed.
  bool CompleteWithFrame(
      size_t idx, const Status& status, const MultiRaftConsensusResponsePB& frame) {
    auto envelope = GetEnvelope(idx);
    auto [done, resp] = TakeDone(envelope.get());
    if (!done) {
      return false;
    }
    if (status.ok()) {
      resp->CopyFrom(frame);
      for (const auto& response : resp->consensus_response()) {
        envelope->responses.push_back(&response);
      }
    }
    done(status);
    return true;
  }

 private:
  friend class FakeBatchSender;

  void SetSendHook(SendHook hook) {
    std::lock_guard lock(mutex_);
    CHECK(!send_hook_) << "One send hook at a time";
    send_hook_ = std::move(hook);
  }

  // Bounded, so that a hook that never returns fails the test instead of hanging it.
  void ClearSendHook() {
    const auto timeout = std::chrono::seconds(30) * kTimeMultiplier;
    std::unique_lock lock(mutex_);
    send_hook_ = nullptr;
    if (!hooks_running_cond_.wait_for(lock, timeout, [this] { return hooks_running_ == 0; })) {
      LOG(FATAL) << hooks_running_ << " send hook(s) still running after " << timeout.count()
                 << " s; their captured state would be destroyed under them";
    }
  }

  bool DoComplete(
      size_t idx, const Status& status, const FillResponse& fill, int num_responses) {
    auto envelope = GetEnvelope(idx);
    auto [done, resp] = TakeDone(envelope.get());
    if (!done) {
      return false;
    }
    if (fill) {
      if (num_responses < 0) {
        num_responses = envelope->req.consensus_request_size();
      }
      for (int i = 0; i != num_responses; ++i) {
        ConsensusResponsePB response;
        fill(i, envelope->req.consensus_request(i), &response);
        auto* lw_response = resp->add_consensus_response();
        lw_response->CopyFrom(response);
        envelope->responses.push_back(lw_response);
      }
    }
    done(status);
    return true;
  }

  // Drops the sender's reference to the batch with the callback, as the RPC layer does after
  // invoking it, and returns the response to fill. Only the caller that gets done may use it.
  // Drops the partial-response callback, as the call does when it finishes.
  std::pair<MultiRaftBatchSender::DoneCallback, LWMultiRaftConsensusResponsePB*> TakeDone(
      FakeBatchEnvelope* envelope) {
    std::shared_ptr<rpc::PartialResponseCallback> partial_callback;
    std::lock_guard lock(mutex_);
    auto done = std::move(envelope->done);
    envelope->done = nullptr;
    partial_callback = std::move(envelope->partial_callback);
    auto* resp = envelope->resp;
    envelope->resp = nullptr;
    return {std::move(done), resp};
  }

  void Record(std::shared_ptr<FakeBatchEnvelope> envelope) {
    SendHook hook;
    size_t idx;
    {
      std::lock_guard lock(mutex_);
      idx = envelopes_.size();
      envelopes_.push_back(std::move(envelope));
      hook = send_hook_;
      // Entered under the mutex that ClearSendHook takes, before the hook can run.
      if (hook) {
        ++hooks_running_;
      }
    }
    if (hook) {
      hook(idx);
      hook = nullptr;
      std::lock_guard lock(mutex_);
      --hooks_running_;
      hooks_running_cond_.notify_all();
    }
  }

  std::mutex mutex_;
  std::vector<std::shared_ptr<FakeBatchEnvelope>> envelopes_;
  SendHook send_hook_;
  int hooks_running_ = 0;
  std::condition_variable hooks_running_cond_;
};

// Captures envelopes instead of sending them, so that tests can complete them on demand.
class FakeBatchSender : public MultiRaftBatchSender {
 public:
  explicit FakeBatchSender(std::shared_ptr<FakeBatchSenderState> state)
      : state_(std::move(state)) {}

  // Serializes req before returning, as Proxy::PrepareCall does, and never reads it afterwards.
  void SendBatch(
      const LWMultiRaftConsensusRequestPB& req, LWMultiRaftConsensusResponsePB* resp,
      rpc::RpcController* controller, DoneCallback done) override {
    auto envelope = std::make_shared<FakeBatchEnvelope>();
    envelope->sent_at = std::chrono::steady_clock::now();
    // Partial: neither the LW serializer nor the LW parser on the follower checks required fields.
    CHECK(envelope->req.ParsePartialFromString(req.SerializeAsString()));
    for (const auto& request : envelope->req.consensus_request()) {
      envelope->tablet_ids.push_back(request.tablet_id());
    }
    envelope->lw_req = &req;
    envelope->resp = resp;
    envelope->controller = controller;
    envelope->timeout = controller->timeout();
    envelope->callback_mode = controller->invoke_callback_mode();
    // Taken once, as the proxy moves it into the call.
    auto partial_callback = controller->TakePartialResponseCallback();
    envelope->had_partial_callback = static_cast<bool>(partial_callback);
    if (partial_callback) {
      envelope->partial_callback =
          std::make_shared<rpc::PartialResponseCallback>(std::move(partial_callback));
    }
    envelope->done = std::move(done);
    auto* recorded = envelope.get();
    state_->Record(std::move(envelope));
    recorded->lw_req = nullptr;
  }

  // There is no RPC call to fail, so fail the envelope instead.
  void MarkCallAsFailed(rpc::RpcController* controller) override {
    state_->MarkCallAsFailed(controller);
  }

 private:
  std::shared_ptr<FakeBatchSenderState> state_;
};

} // namespace yb::consensus
