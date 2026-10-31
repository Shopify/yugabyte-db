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

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "yb/consensus/consensus.pb.h"
#include "yb/consensus/multi_raft_batcher.h"

#include "yb/util/logging.h"
#include "yb/util/status.h"

namespace yb::consensus {

// One MultiRaftUpdateConsensus RPC captured by FakeBatchSender.
struct FakeBatchEnvelope {
  MultiRaftConsensusRequestPB req;
  std::vector<std::string> tablet_ids;
  MultiRaftConsensusResponsePB* resp = nullptr;
  rpc::RpcController* controller = nullptr;
  MonoDelta timeout;
  rpc::InvokeCallbackMode callback_mode = rpc::InvokeCallbackMode::kThreadPoolNormal;
  // Set when the envelope was failed through MultiRaftBatchSender::MarkCallAsFailed.
  bool marked_failed = false;

  // Taken under FakeBatchSenderState's mutex by whoever completes the envelope.
  MultiRaftBatchSender::DoneCallback done;
};

// What a FakeBatchSender has sent. Shared by the sender, which the batcher owns, and the test.
class FakeBatchSenderState {
 public:
  // Fills the response to the request at the given index of an envelope.
  using FillResponse = std::function<void(
      size_t idx, const ConsensusRequestPB& request, ConsensusResponsePB* response)>;
  // Runs on the sending thread, inside SendBatch, after the envelope is recorded.
  using SendHook = std::function<void(size_t envelope_idx)>;

  size_t NumEnvelopes() {
    std::lock_guard lock(mutex_);
    return envelopes_.size();
  }

  std::shared_ptr<FakeBatchEnvelope> GetEnvelope(size_t idx) {
    std::lock_guard lock(mutex_);
    CHECK_LT(idx, envelopes_.size());
    return envelopes_[idx];
  }

  void SetSendHook(SendHook hook) {
    std::lock_guard lock(mutex_);
    send_hook_ = std::move(hook);
  }

  // Completes the envelope with status, on this thread. With an OK status, first fills
  // num_responses sub-responses (default: one per request) using fill. Returns false if the
  // envelope was already completed.
  bool Complete(
      size_t idx, const Status& status, const FillResponse& fill, int num_responses = -1) {
    auto envelope = GetEnvelope(idx);
    auto done = TakeDone(envelope.get());
    if (!done) {
      return false;
    }
    if (status.ok()) {
      if (num_responses < 0) {
        num_responses = envelope->req.consensus_request_size();
      }
      for (int i = 0; i != num_responses; ++i) {
        fill(i, envelope->req.consensus_request(i), envelope->resp->add_consensus_response());
      }
    }
    done(status);
    return true;
  }

  // Fails the uncompleted envelope sent on controller with TimedOut, as RpcController does.
  void MarkCallAsFailed(rpc::RpcController* controller) {
    MultiRaftBatchSender::DoneCallback done;
    {
      std::lock_guard lock(mutex_);
      for (auto it = envelopes_.rbegin(); it != envelopes_.rend(); ++it) {
        auto& envelope = **it;
        if (envelope.controller == controller && envelope.done) {
          envelope.marked_failed = true;
          done = std::move(envelope.done);
          envelope.done = nullptr;
          break;
        }
      }
    }
    if (done) {
      done(STATUS(TimedOut, "Forced timed out detected by sender."));
    }
  }

 private:
  friend class FakeBatchSender;

  // Drops the sender's reference to the batch with the callback, as the RPC layer does after
  // invoking it.
  MultiRaftBatchSender::DoneCallback TakeDone(FakeBatchEnvelope* envelope) {
    std::lock_guard lock(mutex_);
    auto done = std::move(envelope->done);
    envelope->done = nullptr;
    return done;
  }

  void Record(std::shared_ptr<FakeBatchEnvelope> envelope) {
    SendHook hook;
    size_t idx;
    {
      std::lock_guard lock(mutex_);
      idx = envelopes_.size();
      envelopes_.push_back(std::move(envelope));
      hook = send_hook_;
    }
    if (hook) {
      hook(idx);
    }
  }

  std::mutex mutex_;
  std::vector<std::shared_ptr<FakeBatchEnvelope>> envelopes_;
  SendHook send_hook_;
};

// Captures envelopes instead of sending them, so that tests can complete them on demand.
class FakeBatchSender : public MultiRaftBatchSender {
 public:
  explicit FakeBatchSender(std::shared_ptr<FakeBatchSenderState> state)
      : state_(std::move(state)) {}

  void SendBatch(
      const MultiRaftConsensusRequestPB& req, MultiRaftConsensusResponsePB* resp,
      rpc::RpcController* controller, DoneCallback done) override {
    auto envelope = std::make_shared<FakeBatchEnvelope>();
    envelope->req = req;
    for (const auto& request : req.consensus_request()) {
      envelope->tablet_ids.push_back(request.tablet_id());
    }
    envelope->resp = resp;
    envelope->controller = controller;
    envelope->timeout = controller->timeout();
    envelope->callback_mode = controller->invoke_callback_mode();
    envelope->done = std::move(done);
    state_->Record(std::move(envelope));
  }

  // There is no RPC call to fail, so fail the envelope instead.
  void MarkCallAsFailed(rpc::RpcController* controller) override {
    state_->MarkCallAsFailed(controller);
  }

 private:
  std::shared_ptr<FakeBatchSenderState> state_;
};

} // namespace yb::consensus
