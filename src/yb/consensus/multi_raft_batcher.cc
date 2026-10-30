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

#include "yb/consensus/multi_raft_batcher.h"
#include <memory>
#include <thread>

#include "yb/common/wire_protocol.h"

#include "yb/consensus/consensus_meta.h"
#include "yb/consensus/consensus.proxy.h"

#include "yb/gutil/casts.h"
#include "yb/gutil/port.h"

#include "yb/rpc/messenger.h"
#include "yb/rpc/periodic.h"
#include "yb/rpc/scheduler.h"

#include "yb/util/dist_trace.h"
#include "yb/util/flags.h"
#include "yb/util/logging.h"
#include "yb/util/metrics.h"
#include "yb/util/size_literals.h"
#include "yb/util/thread_pool.h"

using namespace std::literals;
using namespace std::placeholders;
using namespace yb::size_literals;

DEFINE_UNKNOWN_bool(enable_multi_raft_heartbeat_batcher, false,
            "If true, enables multi-Raft batching of raft heartbeats.");

DEFINE_UNKNOWN_uint64(multi_raft_heartbeat_interval_ms, 50,
              "The heartbeat interval for batch Raft replication: how often each multi-Raft "
              "batcher sends its pending batch. Also the backstop for batched data requests. "
              "Read when the batcher is created.");
TAG_FLAG(multi_raft_heartbeat_interval_ms, advanced);

DEFINE_UNKNOWN_uint64(multi_raft_batch_size, 0,
              "Maximum batch size for a multi-Raft consensus payload. Ignored if set to zero.");
TAG_FLAG(multi_raft_batch_size, advanced);

DEFINE_RUNTIME_bool(enable_multi_raft_data_batching, false,
    "If true, this server does two things. As a Raft leader, it sends Raft requests that carry "
    "operations or advance the commit index, and are smaller than "
    "multi_raft_data_direct_threshold_bytes, to a remote server through the multi-Raft batcher, so "
    "that requests for different tablets share one MultiRaftUpdateConsensus RPC. Raft peers "
    "created while this or enable_multi_raft_heartbeat_batcher is set get a batcher; turning this "
    "on at runtime affects only peers that already have one. As a Raft follower, it processes the "
    "Raft requests inside each MultiRaftUpdateConsensus RPC it receives, heartbeats included, "
    "concurrently instead of one after another; this is decided when each RPC arrives.");

DEFINE_RUNTIME_uint32(multi_raft_data_flush_delay_ms, 2,
    "Maximum time, in milliseconds, that a data request waits in a multi-Raft batch while another "
    "batch carrying data to the same server is in flight. The batch is sent earlier if one of "
    "those in-flight batches completes first.");
TAG_FLAG(multi_raft_data_flush_delay_ms, advanced);

DEFINE_RUNTIME_uint32(multi_raft_data_batch_size, 32,
    "Maximum number of data requests in one multi-Raft batch. A data request that would exceed "
    "this sends the current batch first. Heartbeats are not counted. 0 means no limit.");
TAG_FLAG(multi_raft_data_batch_size, advanced);

DEFINE_RUNTIME_uint64(multi_raft_batch_bytes, 4_MB,
    "Maximum total serialized size, in bytes, of the requests in one multi-Raft batch. A request "
    "that would exceed this sends the current batch first, so a request larger than this is sent "
    "in a batch of its own. 0 means no limit.");
TAG_FLAG(multi_raft_batch_bytes, advanced);

DEFINE_RUNTIME_uint32(multi_raft_rpc_timeout_ms, 0,
    "Upper bound, in milliseconds, on the RPC timeout of a multi-Raft batch that carries data "
    "requests. Such a batch otherwise gets consensus_rpc_timeout_ms times its number of requests. "
    "0 means 2 * consensus_rpc_timeout_ms. Batches of heartbeats only are not capped.");
TAG_FLAG(multi_raft_rpc_timeout_ms, advanced);

DEFINE_RUNTIME_uint64(multi_raft_data_direct_threshold_bytes, 1_MB,
    "When enable_multi_raft_data_batching is set, Raft data requests whose serialized size is at "
    "least this many bytes are sent in their own UpdateConsensus RPC instead of through the "
    "multi-Raft batcher. 0 means no threshold: every data request is batched, still subject to "
    "multi_raft_batch_bytes.");
TAG_FLAG(multi_raft_data_direct_threshold_bytes, advanced);

DECLARE_int32(consensus_rpc_timeout_ms);

METRIC_DEFINE_counter(server, multi_raft_envelopes_sent,
    "Multi-Raft Envelopes Sent", yb::MetricUnit::kRequests,
    "Number of MultiRaftUpdateConsensus RPCs sent by multi-Raft batchers.");
METRIC_DEFINE_counter(server, multi_raft_envelopes_failed,
    "Multi-Raft Envelopes Failed", yb::MetricUnit::kRequests,
    "Number of MultiRaftUpdateConsensus RPCs sent by multi-Raft batchers that failed as a whole, "
    "failing every Raft request they carried with the same error: the RPC itself failed (for "
    "example it timed out, the connection failed, or the sending or receiving server shut down), "
    "or its response did not hold exactly one response per request. Errors returned for "
    "individual Raft requests inside a successful RPC are not counted, nor are batches aborted "
    "at shutdown before being sent.");
METRIC_DEFINE_event_stats(server, multi_raft_envelope_sub_requests,
    "Multi-Raft Envelope Sub-Requests", yb::MetricUnit::kRequests,
    "Number of Raft requests in each MultiRaftUpdateConsensus RPC.");
METRIC_DEFINE_event_stats(server, multi_raft_envelope_bytes,
    "Multi-Raft Envelope Bytes", yb::MetricUnit::kBytes,
    "Total serialized size of the Raft requests in each MultiRaftUpdateConsensus RPC.");
METRIC_DEFINE_event_stats(server, multi_raft_envelope_ops,
    "Multi-Raft Envelope Operations", yb::MetricUnit::kOperations,
    "Total number of Raft operations in the requests of each MultiRaftUpdateConsensus RPC.");
METRIC_DEFINE_counter(server, multi_raft_flush_immediate,
    "Multi-Raft Immediate Flushes", yb::MetricUnit::kRequests,
    "Multi-Raft batches sent as soon as a data request was added, because no batch carrying data "
    "to that server was in flight.");
METRIC_DEFINE_counter(server, multi_raft_flush_response_clocked,
    "Multi-Raft Response-Clocked Flushes", yb::MetricUnit::kRequests,
    "Multi-Raft batches sent because a batch carrying data to the same server completed.");
METRIC_DEFINE_counter(server, multi_raft_flush_deadline,
    "Multi-Raft Deadline Flushes", yb::MetricUnit::kRequests,
    "Multi-Raft batches sent because multi_raft_data_flush_delay_ms elapsed.");
METRIC_DEFINE_counter(server, multi_raft_flush_count_cap,
    "Multi-Raft Count Cap Flushes", yb::MetricUnit::kRequests,
    "Multi-Raft batches sent because of a request count limit, either multi_raft_batch_size or "
    "multi_raft_data_batch_size.");
METRIC_DEFINE_counter(server, multi_raft_flush_bytes_cap,
    "Multi-Raft Bytes Cap Flushes", yb::MetricUnit::kRequests,
    "Multi-Raft batches sent because the next request would exceed multi_raft_batch_bytes.");
METRIC_DEFINE_counter(server, multi_raft_flush_timer,
    "Multi-Raft Timer Flushes", yb::MetricUnit::kRequests,
    "Multi-Raft batches sent by the multi_raft_heartbeat_interval_ms timer.");
METRIC_DEFINE_counter(server, multi_raft_heartbeat_sub_requests,
    "Multi-Raft Heartbeat Sub-Requests", yb::MetricUnit::kRequests,
    "Raft heartbeat requests sent inside MultiRaftUpdateConsensus RPCs.");
METRIC_DEFINE_counter(server, multi_raft_data_sub_requests,
    "Multi-Raft Data Sub-Requests", yb::MetricUnit::kRequests,
    "Raft data requests sent inside MultiRaftUpdateConsensus RPCs.");
METRIC_DEFINE_event_stats(server, multi_raft_batched_response_processing_us,
    "Multi-Raft Batched Response Processing Time", yb::MetricUnit::kMicroseconds,
    "Time spent processing the response of each Raft data request sent inside a "
    "MultiRaftUpdateConsensus RPC, i.e. running its response callback.");
METRIC_DEFINE_counter(server, multi_raft_data_sends_direct,
    "Multi-Raft Direct Data Sends", yb::MetricUnit::kRequests,
    "Raft data requests sent in their own UpdateConsensus RPC instead of through the multi-Raft "
    "batcher because of multi_raft_data_direct_threshold_bytes, while "
    "enable_multi_raft_data_batching was set.");

namespace yb {
namespace consensus {

using rpc::PeriodicTimer;

namespace {

// One request of a batch: where to put its response, and the callback that completes it
// (Peer::ProcessHeartbeatResponse or Peer::ProcessBatchedResponse, by kind).
struct ResponseCallbackData {
  ConsensusResponsePB* resp;
  MultiRaftBatcher::BatchResponseCallback callback;
  MultiRaftBatcher::RequestKind kind;
};

class ProxyBatchSender : public MultiRaftBatchSender {
 public:
  ProxyBatchSender(rpc::ProxyCache* proxy_cache, const HostPort& hostport)
      : proxy_(proxy_cache, hostport) {}

  void SendBatch(
      const MultiRaftConsensusRequestPB& req, MultiRaftConsensusResponsePB* resp,
      rpc::RpcController* controller, DoneCallback done) override {
    proxy_.MultiRaftUpdateConsensusAsync(
        req, resp, controller, [controller, done = std::move(done)] {
          done(controller->status());
        });
  }

 private:
  ConsensusServiceProxy proxy_;
};

} // namespace

std::unique_ptr<MultiRaftBatchSender> CreateMultiRaftBatchSender(
    rpc::ProxyCache* proxy_cache, const HostPort& hostport) {
  return std::make_unique<ProxyBatchSender>(proxy_cache, hostport);
}

struct MultiRaftBatcher::MultiRaftConsensusData {
  MultiRaftConsensusRequestPB batch_req;
  MultiRaftConsensusResponsePB batch_res;
  rpc::RpcController controller;
  std::vector<ResponseCallbackData> response_callback_data;
  size_t num_data_requests = 0;
  size_t num_ops = 0;
  size_t total_bytes = 0;

  // Set before SendBatch.
  std::shared_ptr<MultiRaftBatchSender> sender;
  CounterPtr envelopes_failed;
  EventStatsPtr batched_response_processing_us;

  // Set once the sender has returned from SendBatch. RpcController assigns its call without
  // holding its lock, so BatchHandle must not touch the controller before then. After it, nothing
  // writes the controller's call again.
  std::atomic<bool> sent{false};

  // Set when the batch RPC completes, before any sub-callback runs.
  std::atomic<bool> completed{false};

  bool empty() const { return response_callback_data.empty(); }
  bool has_data() const { return num_data_requests > 0; }
};

struct MultiRaftBatcher::Metrics {
  explicit Metrics(const MetricEntityPtr& entity) {
    if (!entity) {
      return;
    }
    envelopes_sent = METRIC_multi_raft_envelopes_sent.Instantiate(entity);
    envelopes_failed = METRIC_multi_raft_envelopes_failed.Instantiate(entity);
    envelope_sub_requests = METRIC_multi_raft_envelope_sub_requests.Instantiate(entity);
    envelope_bytes = METRIC_multi_raft_envelope_bytes.Instantiate(entity);
    envelope_ops = METRIC_multi_raft_envelope_ops.Instantiate(entity);
    flush_immediate = METRIC_multi_raft_flush_immediate.Instantiate(entity);
    flush_response_clocked = METRIC_multi_raft_flush_response_clocked.Instantiate(entity);
    flush_deadline = METRIC_multi_raft_flush_deadline.Instantiate(entity);
    flush_count_cap = METRIC_multi_raft_flush_count_cap.Instantiate(entity);
    flush_bytes_cap = METRIC_multi_raft_flush_bytes_cap.Instantiate(entity);
    flush_timer = METRIC_multi_raft_flush_timer.Instantiate(entity);
    heartbeat_sub_requests = METRIC_multi_raft_heartbeat_sub_requests.Instantiate(entity);
    data_sub_requests = METRIC_multi_raft_data_sub_requests.Instantiate(entity);
    data_sends_direct = METRIC_multi_raft_data_sends_direct.Instantiate(entity);
    batched_response_processing_us =
        METRIC_multi_raft_batched_response_processing_us.Instantiate(entity);
  }

  CounterPtr envelopes_sent;
  CounterPtr envelopes_failed;
  EventStatsPtr envelope_sub_requests;
  EventStatsPtr envelope_bytes;
  EventStatsPtr envelope_ops;
  CounterPtr flush_immediate;
  CounterPtr flush_response_clocked;
  CounterPtr flush_deadline;
  CounterPtr flush_count_cap;
  CounterPtr flush_bytes_cap;
  CounterPtr flush_timer;
  CounterPtr heartbeat_sub_requests;
  CounterPtr data_sub_requests;
  CounterPtr data_sends_direct;
  EventStatsPtr batched_response_processing_us;
};

enum class MultiRaftBatcher::FlushReason {
  kImmediate,
  kResponseClocked,
  kDeadline,
  kCountCap,
  kBytesCap,
  kTimer,
};

void MultiRaftBatcher::BatchHandle::MarkCallAsFailed() const {
  auto data = data_.lock();
  if (!data || !data->sent.load(std::memory_order_acquire) ||
      data->completed.load(std::memory_order_acquire) || data->controller.finished()) {
    return;
  }
  data->sender->MarkCallAsFailed(&data->controller);
}

MonoDelta MultiRaftBatcher::BatchHandle::timeout() const {
  auto data = data_.lock();
  if (!data || !data->sent.load(std::memory_order_acquire)) {
    return MonoDelta();
  }
  return data->controller.timeout();
}

bool MultiRaftBatcher::BatchHandle::finished() const {
  auto data = data_.lock();
  if (!data) {
    return true;
  }
  if (!data->sent.load(std::memory_order_acquire)) {
    return false;
  }
  return data->completed.load(std::memory_order_acquire) || data->controller.finished();
}

std::string MultiRaftBatcher::BatchHandle::CallStateDebugString() const {
  auto data = data_.lock();
  if (!data) {
    return "batch released";
  }
  if (!data->sent.load(std::memory_order_acquire)) {
    return "batch not sent";
  }
  return data->controller.CallStateDebugString();
}

const rpc::RpcController* MultiRaftBatcher::BatchHandle::TEST_controller() const {
  auto data = data_.lock();
  return data ? &data->controller : nullptr;
}

MultiRaftBatcher::MultiRaftBatcher(
    rpc::Messenger* messenger,
    std::unique_ptr<MultiRaftBatchSender> sender,
    std::atomic<int>* running_calls,
    const MetricEntityPtr& metric_entity)
    : messenger_(messenger),
      sender_(std::move(sender)),
      metrics_(std::make_unique<Metrics>(metric_entity)),
      current_batch_(std::make_shared<MultiRaftConsensusData>()),
      running_calls_(running_calls) {}

void MultiRaftBatcher::Start() {
  std::weak_ptr<MultiRaftBatcher> weak_self = shared_from_this();
  periodic_flush_timer_ = PeriodicTimer::Create(
    messenger_,
    [weak_self]() {
      if (auto self = weak_self.lock()) {
        self->PrepareAndSendBatchRequest();
      }
    },
    MonoDelta::FromMilliseconds(FLAGS_multi_raft_heartbeat_interval_ms));
  periodic_flush_timer_->Start();
}

MultiRaftBatcher::~MultiRaftBatcher() {
  std::lock_guard lock(mutex_);
  LOG_IF(DFATAL, current_batch_ && !current_batch_->response_callback_data.empty())
      << "Not empty batch in ~MultiRaftBatcher";
}

// Kept out of line as a uprobe attach point for timing Raft rounds.
ATTRIBUTE_NOINLINE MultiRaftBatcher::BatchHandle MultiRaftBatcher::AddRequestToBatch(
    ConsensusRequestPB* request, ConsensusResponsePB* response, BatchResponseCallback callback,
    RequestKind kind) {
  const auto request_bytes = request->ByteSizeLong();
  BatchData pre_add_batch;
  BatchData post_add_batch;
  BatchHandle handle;
  bool aborted = false;
  {
    std::lock_guard lock(mutex_);
    if (shutdown_) {
      aborted = true;
    } else {
      handle = AddRequestUnlocked(
          request, response, std::move(callback), kind, request_bytes, &pre_add_batch,
          &post_add_batch);
    }
  }
  if (aborted) {
    callback(STATUS(Aborted, "MultiRaft shutdown"));
    return handle;
  }
  SendBatchRequest(std::move(pre_add_batch));
  SendBatchRequest(std::move(post_add_batch));
  return handle;
}

MultiRaftBatcher::BatchHandle MultiRaftBatcher::AddRequestUnlocked(
    ConsensusRequestPB* request, ConsensusResponsePB* response, BatchResponseCallback callback,
    RequestKind kind, size_t request_bytes, BatchData* pre_add_batch, BatchData* post_add_batch) {
  const bool is_data = kind == RequestKind::kData;

  // Caps apply before the add, so a request never makes the batch exceed them. A request that
  // exceeds a cap on its own still goes into the (empty) batch.
  if (!current_batch_->empty()) {
    const auto data_batch_size = FLAGS_multi_raft_data_batch_size;
    const auto batch_bytes = FLAGS_multi_raft_batch_bytes;
    if (is_data && data_batch_size > 0 &&
        current_batch_->num_data_requests + 1 > data_batch_size) {
      *pre_add_batch = PrepareNextBatchRequest(FlushReason::kCountCap);
    } else if (batch_bytes > 0 && current_batch_->total_bytes + request_bytes > batch_bytes) {
      *pre_add_batch = PrepareNextBatchRequest(FlushReason::kBytesCap);
    }
  }

  current_batch_->response_callback_data.push_back({
    .resp = response,
    .callback = std::move(callback),
    .kind = kind,
  });
  current_batch_->total_bytes += request_bytes;
  current_batch_->num_ops += request->ops_size();
  if (is_data) {
    ++current_batch_->num_data_requests;
  }
  // Add a ConsensusRequestPB to the batch
  current_batch_->batch_req.add_consensus_request()->Swap(request);
  BatchHandle handle(current_batch_);

  if (FLAGS_multi_raft_batch_size > 0
      && current_batch_->response_callback_data.size() >= FLAGS_multi_raft_batch_size) {
    *post_add_batch = PrepareNextBatchRequest(FlushReason::kCountCap);
  } else if (is_data) {
    // A pre-add flush that carried data counts as in flight here, so this request waits for it
    // to complete (or for the deadline) instead of going out in a second envelope.
    if (data_batches_in_flight_ == 0) {
      *post_add_batch = PrepareNextBatchRequest(FlushReason::kImmediate);
    } else {
      ArmDataFlushDeadline();
    }
  }
  return handle;
}

void MultiRaftBatcher::RecordDirectDataSend() {
  IncrementCounter(metrics_->data_sends_direct);
}

void MultiRaftBatcher::PrepareAndSendBatchRequest() {
  BatchData data;
  {
    std::lock_guard lock(mutex_);
    data = PrepareNextBatchRequest(FlushReason::kTimer);
  }
  SendBatchRequest(data);
}

MultiRaftBatcher::BatchData MultiRaftBatcher::PrepareNextBatchRequest(FlushReason reason) {
  if (shutdown_ || current_batch_->empty()) {
    return nullptr;
  }
  if (periodic_flush_timer_) {
    periodic_flush_timer_->Snooze();
  }
  CancelDataFlushDeadline();
  auto data = std::make_shared<MultiRaftConsensusData>();
  current_batch_.swap(data);
  auto running_calls = ++*running_calls_;
  LOG_IF(DFATAL, running_calls <= 0) << "Wrong number or running calls: " << running_calls;
  if (data->has_data()) {
    ++data_batches_in_flight_;
  }

  switch (reason) {
    case FlushReason::kImmediate:
      IncrementCounter(metrics_->flush_immediate);
      break;
    case FlushReason::kResponseClocked:
      IncrementCounter(metrics_->flush_response_clocked);
      break;
    case FlushReason::kDeadline:
      IncrementCounter(metrics_->flush_deadline);
      break;
    case FlushReason::kCountCap:
      IncrementCounter(metrics_->flush_count_cap);
      break;
    case FlushReason::kBytesCap:
      IncrementCounter(metrics_->flush_bytes_cap);
      break;
    case FlushReason::kTimer:
      IncrementCounter(metrics_->flush_timer);
      break;
  }
  return data;
}

void MultiRaftBatcher::ArmDataFlushDeadline() {
  if (data_flush_deadline_task_ != rpc::kInvalidTaskId) {
    return;
  }
  std::weak_ptr<MultiRaftBatcher> weak_self = shared_from_this();
  // The task cannot observe data_flush_deadline_task_ before it is assigned, because it takes
  // mutex_, which is held here.
  data_flush_deadline_task_ = messenger_->scheduler().Schedule(
      [weak_self](rpc::ScheduledTaskId task_id, const Status& status) {
        if (!status.ok()) {
          return;
        }
        if (auto self = weak_self.lock()) {
          self->DataFlushDeadlineExpired(task_id);
        }
      },
      FLAGS_multi_raft_data_flush_delay_ms * 1ms);
}

void MultiRaftBatcher::CancelDataFlushDeadline() {
  if (data_flush_deadline_task_ == rpc::kInvalidTaskId) {
    return;
  }
  // Abort runs the task asynchronously with an error status, which the task ignores.
  messenger_->scheduler().Abort(data_flush_deadline_task_);
  data_flush_deadline_task_ = rpc::kInvalidTaskId;
}

void MultiRaftBatcher::DataFlushDeadlineExpired(rpc::ScheduledTaskId task_id) {
  BatchData data;
  {
    std::lock_guard lock(mutex_);
    // The task may have fired after the batch that armed it was flushed and a new deadline was
    // armed.
    if (data_flush_deadline_task_ != task_id) {
      return;
    }
    data_flush_deadline_task_ = rpc::kInvalidTaskId;
    data = PrepareNextBatchRequest(FlushReason::kDeadline);
  }
  SendBatchRequest(std::move(data));
}

// Kept out of line as a uprobe attach point for timing envelopes. `used` keeps LTO from rewriting
// the signature (it could pass only `data.has_data()` instead of the batch the probe keys on).
__attribute__((used)) ATTRIBUTE_NOINLINE void MultiRaftBatcher::BatchCompleted(
    const MultiRaftConsensusData& data) {
  if (!data.has_data()) {
    return;
  }
  BatchData next;
  {
    std::lock_guard lock(mutex_);
    LOG_IF(DFATAL, data_batches_in_flight_ == 0) << "No data batch in flight";
    --data_batches_in_flight_;
    // Parked heartbeats alone are left for the timer.
    if (current_batch_->has_data()) {
      next = PrepareNextBatchRequest(FlushReason::kResponseClocked);
    }
  }
  SendBatchRequest(std::move(next));
}

void MultiRaftBatcher::InvokeSubCallback(
    const BatchData& data, size_t idx, const Status& status) {
  auto& callback_data = data->response_callback_data[idx];
  // Every data sub-callback, whether it runs in a SubCallbackTask, inline from its Done, or inline
  // when there is no pool, comes through here.
  const auto start = callback_data.kind == RequestKind::kData &&
                     data->batched_response_processing_us
      ? MonoTime::Now() : MonoTime();
  if (status.ok()) {
    callback_data.resp->Swap(data->batch_res.mutable_consensus_response(narrow_cast<int>(idx)));
  }
  callback_data.callback(status);
  if (start.Initialized()) {
    data->batched_response_processing_us->Increment(
        (MonoTime::Now() - start).ToMicroseconds());
  }
}

// Runs one sub-callback on a thread pool. If the pool drops the task (it is shutting down), the
// sub-callback runs inline in Done instead: a caller may hold a lock until its callback runs.
class MultiRaftBatcher::SubCallbackTask : public ThreadPoolTask {
 public:
  SubCallbackTask(BatchData data, size_t idx, Status status)
      : data_(std::move(data)), idx_(idx), status_(std::move(status)) {}

  void Run() override {
    ran_ = true;
    InvokeSubCallback(data_, idx_, status_);
  }

  void Done(const Status& status) override {
    if (!ran_) {
      Run();
    }
    delete this;
  }

 private:
  BatchData data_;
  size_t idx_;
  Status status_;
  bool ran_ = false;
};

void MultiRaftBatcher::DispatchSubCallbacks(
    const BatchData& data, Status status, rpc::ThreadPool* data_pool) {
  const auto num_requests = data->response_callback_data.size();
  if (status.ok() && data->batch_res.consensus_response_size() != std::ssize(
          data->response_callback_data)) {
    status = STATUS_FORMAT(
        IllegalState, "MultiRaft response has $0 responses for $1 requests",
        data->batch_res.consensus_response_size(), num_requests);
    // Every sub-request in an envelope goes to the same server.
    YB_LOG_EVERY_N_SECS(ERROR, 1)
        << status << ", destination: "
        << data->batch_req.consensus_request(0).dest_uuid();
  }
  if (!status.ok()) {
    IncrementCounter(data->envelopes_failed);
  }
  // Each data sub-callback gets its own task because it does a full Raft response round
  // (majority tracking and building the next request). Heartbeat sub-callbacks are cheap and
  // run serially on this thread.
  for (size_t i = 0; i != num_requests; ++i) {
    if (data_pool && data->response_callback_data[i].kind == RequestKind::kData) {
      data_pool->Enqueue(new SubCallbackTask(data, i, status));
    } else {
      InvokeSubCallback(data, i, status);
    }
  }
}

// Kept out of line as a uprobe attach point for timing envelopes.
ATTRIBUTE_NOINLINE void MultiRaftBatcher::SendBatchRequest(BatchData data) {
  if (!data) {
    return;
  }
  // TODO(#16670): give consensus its own root trace.
  auto detach_token = dist_trace::DetachTraceContext();

  const auto num_requests = data->batch_req.consensus_request_size();
  IncrementCounter(metrics_->envelopes_sent);
  IncrementStats(metrics_->envelope_sub_requests, num_requests);
  IncrementStats(metrics_->envelope_bytes, data->total_bytes);
  IncrementStats(metrics_->envelope_ops, data->num_ops);
  const auto num_data_requests = narrow_cast<int>(data->num_data_requests);
  IncrementCounterBy(metrics_->heartbeat_sub_requests, num_requests - num_data_requests);
  IncrementCounterBy(metrics_->data_sub_requests, num_data_requests);

  data->controller.Reset();
  int64_t timeout_ms = int64_t{FLAGS_consensus_rpc_timeout_ms} * num_requests;
  rpc::ThreadPool* data_pool = nullptr;
  if (data->has_data()) {
    // Data acks are latency-critical, like those of the direct UpdateConsensus path, which also
    // uses the high-priority pool. Heartbeat-only batches keep the normal pool and the uncapped
    // timeout they have always had.
    auto timeout_cap_ms = int64_t{FLAGS_multi_raft_rpc_timeout_ms};
    if (timeout_cap_ms == 0) {
      timeout_cap_ms = 2 * int64_t{FLAGS_consensus_rpc_timeout_ms};
    }
    timeout_ms = std::min(timeout_ms, timeout_cap_ms);
    data->controller.set_invoke_callback_mode(rpc::InvokeCallbackMode::kThreadPoolHigh);
    data_pool = &messenger_->ThreadPool(rpc::ServicePriority::kHigh);
  }
  data->controller.set_timeout(MonoDelta::FromMilliseconds(timeout_ms));

  // The response-clocked flush runs before the sub-callbacks are dispatched. Requests that were
  // already pending go out without waiting for them, and requests that the sub-callbacks add
  // next see that envelope in flight and accumulate behind it rather than each going out alone.
  auto callback = [data, data_pool, weak_self = weak_from_this(),
                   running_calls = running_calls_](const Status& status) {
    data->completed.store(true, std::memory_order_release);
    --*running_calls;
    if (auto self = weak_self.lock()) {
      self->BatchCompleted(*data);
    }
    DispatchSubCallbacks(data, status, data_pool);
  };
  data->sender = sender_;
  data->envelopes_failed = metrics_->envelopes_failed;
  data->batched_response_processing_us = metrics_->batched_response_processing_us;
  sender_->SendBatch(data->batch_req, &data->batch_res, &data->controller, std::move(callback));
  data->sent.store(true, std::memory_order_release);
}

void MultiRaftBatcher::Shutdown() {
  BatchData batch;
  if (periodic_flush_timer_) {
    periodic_flush_timer_->Stop();
  }
  {
    std::lock_guard lock(mutex_);
    if (shutdown_) {
      return;
    }
    shutdown_ = true;
    CancelDataFlushDeadline();
    batch = std::make_shared<MultiRaftConsensusData>();
    batch.swap(current_batch_);
  }
  static const Status status = STATUS(Aborted, "MultiRaft shutdown");
  for (const auto& callback : batch->response_callback_data) {
    callback.callback(status);
  }
}

MultiRaftManager::MultiRaftManager(rpc::Messenger* messenger,
                                   rpc::ProxyCache* proxy_cache,
                                   CloudInfoPB local_peer_cloud_info_pb,
                                   MetricEntityPtr metric_entity)
    : messenger_(messenger), proxy_cache_(proxy_cache),
      local_peer_cloud_info_pb_(std::move(local_peer_cloud_info_pb)),
      metric_entity_(std::move(metric_entity)) {}

MultiRaftBatcherPtr MultiRaftManager::AddOrGetBatcher(const RaftPeerPB& remote_peer_pb) {
  if (!FLAGS_enable_multi_raft_heartbeat_batcher && !FLAGS_enable_multi_raft_data_batching) {
    return nullptr;
  }

  auto hostport = HostPortFromPB(DesiredHostPort(remote_peer_pb, local_peer_cloud_info_pb_));
  std::lock_guard lock(mutex_);
  if (shutdown_) {
    LOG(DFATAL) << __func__ << " after shutdown";
    return nullptr;
  }
  MultiRaftBatcherPtr batcher;

  // After taking the lock, check if there is already a batcher
  // for the same remote host and return it.
  auto res = batchers_.find(hostport);
  if (res != batchers_.end() && (batcher = res->second.lock())) {
    return batcher;
  }
  batcher = std::make_shared<MultiRaftBatcher>(
      messenger_, CreateMultiRaftBatchSender(proxy_cache_, hostport), &running_calls_,
      metric_entity_);
  batchers_[hostport] = batcher;
  batcher->Start();
  return batcher;
}

MultiRaftManager::~MultiRaftManager() {
  LOG_IF(DFATAL, !shutdown_) << "Shutdown was not invoked";
}

void MultiRaftManager::StartShutdown() {
  std::vector<MultiRaftBatcherPtr> batchers;
  {
    std::lock_guard lock(mutex_);
    shutdown_ = true;
    batchers.reserve(batchers_.size());
    for (const auto& [hostport, weak_batcher] : batchers_) {
      auto batcher = weak_batcher.lock();
      if (batcher) {
        batchers.push_back(batcher);
      }
    }
    batchers_.clear();
  }
  for (const auto& batcher : batchers) {
    batcher->Shutdown();
  }
}

void MultiRaftManager::CompleteShutdown() {
  int expected = 0;
  while (!running_calls_.compare_exchange_weak(expected, std::numeric_limits<int>::min() / 2)) {
    std::this_thread::sleep_for(10ms);
    expected = 0;
  }
}

}  // namespace consensus
}  // namespace yb
