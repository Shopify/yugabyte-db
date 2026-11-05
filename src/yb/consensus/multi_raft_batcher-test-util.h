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

#include "yb/consensus/consensus.messages.h"
#include "yb/consensus/consensus.pb.h"
#include "yb/consensus/multi_raft_batcher.h"

#include "yb/util/logging.h"
#include "yb/util/status.h"

namespace yb::consensus {

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
  // Fills the response to the request at the given index of an envelope. request is the wire
  // copy. response is copied into the batch's response on its arena.
  using FillResponse = std::function<void(
      size_t idx, const ConsensusRequestPB& request, ConsensusResponsePB* response)>;
  // Runs on the sending thread, inside SendBatch, after the envelope is serialized and recorded.
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
    {
      std::lock_guard lock(mutex_);
      for (auto it = envelopes_.rbegin(); it != envelopes_.rend(); ++it) {
        auto& envelope = **it;
        if (envelope.controller == controller && envelope.done) {
          envelope.marked_failed = true;
          done = std::move(envelope.done);
          envelope.done = nullptr;
          envelope.resp = nullptr;
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
  std::pair<MultiRaftBatchSender::DoneCallback, LWMultiRaftConsensusResponsePB*> TakeDone(
      FakeBatchEnvelope* envelope) {
    std::lock_guard lock(mutex_);
    auto done = std::move(envelope->done);
    envelope->done = nullptr;
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

  // Serializes req before returning, as Proxy::PrepareCall does, and never reads it afterwards.
  void SendBatch(
      const LWMultiRaftConsensusRequestPB& req, LWMultiRaftConsensusResponsePB* resp,
      rpc::RpcController* controller, DoneCallback done) override {
    auto envelope = std::make_shared<FakeBatchEnvelope>();
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
