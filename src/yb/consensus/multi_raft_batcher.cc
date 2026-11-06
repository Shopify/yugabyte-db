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

#include "yb/consensus/consensus.messages.h"
#include "yb/consensus/consensus.proxy.h"
#include "yb/consensus/consensus_meta.h"
#include "yb/consensus/replicate_msgs_holder.h"

#include "yb/gutil/casts.h"
#include "yb/gutil/port.h"

#include "yb/rpc/messenger.h"
#include "yb/rpc/periodic.h"
#include "yb/rpc/scheduler.h"

#include "yb/gutil/thread_annotations.h"

#include "yb/util/dist_trace.h"
#include "yb/util/flags.h"
#include "yb/util/locks.h"
#include "yb/util/logging.h"
#include "yb/util/memory/arena.h"
#include "yb/util/metrics.h"
#include "yb/util/ref_cnt_buffer.h"
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

DEFINE_RUNTIME_bool(enable_multi_raft_streaming_responses, false,
    "If true, a Raft leader asks the follower to answer each Raft request of a multi-Raft batch "
    "that carries data as soon as that request completes, instead of answering the whole batch "
    "once its slowest request completes. A follower streams only if its own "
    "enable_multi_raft_data_batching is on; otherwise it answers once, as without this flag. "
    "Read when each batch is sent.");
TAG_FLAG(enable_multi_raft_streaming_responses, advanced);

DEFINE_RUNTIME_uint32(multi_raft_data_flush_delay_ms, 2,
    "Maximum time, in milliseconds, that a data request waits in a multi-Raft batch while another "
    "batch carrying data to the same server is in flight. The batch is sent earlier if one of "
    "those in-flight batches completes first. Ignored while multi_raft_data_flush_window_us is "
    "above 0.");
TAG_FLAG(multi_raft_data_flush_delay_ms, advanced);

DEFINE_RUNTIME_uint32(multi_raft_data_flush_window_us, 0,
    "Interval, in microseconds, at which multi-Raft batches carrying data to the same server are "
    "paced. A data request added once the interval has passed since the last such batch was sent "
    "is sent at once; one added earlier waits for the interval to end. Batch size limits and the "
    "multi_raft_heartbeat_interval_ms timer can send a batch earlier. 0 keeps response clocking "
    "instead: a data request is sent at once when no batch carrying data to the server is in "
    "flight, and otherwise waits for one to complete or for multi_raft_data_flush_delay_ms. Read "
    "on each add.");
TAG_FLAG(multi_raft_data_flush_window_us, advanced);

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
    "Number of MultiRaftUpdateConsensus RPCs sent by multi-Raft batchers that failed, failing "
    "each Raft request they carried that had no response yet with the same error: the RPC "
    "itself failed (for example it timed out, the connection failed, or the sending or receiving "
    "server shut down), or its responses did not answer every request exactly once. Without "
    "enable_multi_raft_streaming_responses that is every request of the RPC. With it, requests "
    "already answered by an earlier response frame keep their response, and an RPC whose final "
    "response leaves requests unanswered counts once. Errors returned for individual Raft "
    "requests are not counted, nor are batches aborted at shutdown before being sent.");
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
    "to that server was in flight, while multi_raft_data_flush_window_us was 0.");
METRIC_DEFINE_counter(server, multi_raft_flush_response_clocked,
    "Multi-Raft Response-Clocked Flushes", yb::MetricUnit::kRequests,
    "Multi-Raft batches sent because a batch carrying data to the same server completed, while "
    "multi_raft_data_flush_window_us was 0.");
METRIC_DEFINE_counter(server, multi_raft_flush_deadline,
    "Multi-Raft Deadline Flushes", yb::MetricUnit::kRequests,
    "Multi-Raft batches sent because multi_raft_data_flush_delay_ms elapsed. The delay is armed "
    "only while multi_raft_data_flush_window_us is 0.");
METRIC_DEFINE_counter(server, multi_raft_flush_paced_inline,
    "Multi-Raft Paced Inline Flushes", yb::MetricUnit::kRequests,
    "Multi-Raft batches sent as soon as a data request was added, because "
    "multi_raft_data_flush_window_us had passed since the last batch carrying data to that server "
    "was sent, or none was.");
METRIC_DEFINE_counter(server, multi_raft_flush_paced_timer,
    "Multi-Raft Paced Timer Flushes", yb::MetricUnit::kRequests,
    "Multi-Raft batches sent at the end of the multi_raft_data_flush_window_us interval that "
    "followed the last batch carrying data to that server, for data requests added within it.");
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
METRIC_DEFINE_event_stats(server, multi_raft_data_add_to_send_us,
    "Multi-Raft Data Add To Send Time", yb::MetricUnit::kMicroseconds,
    "Time each Raft data request waited in a multi-Raft batcher, from being added to a batch "
    "until that batch's MultiRaftUpdateConsensus RPC was sent. Batches aborted at shutdown before "
    "being sent are not recorded.");
METRIC_DEFINE_counter(server, multi_raft_frames_received,
    "Multi-Raft Response Frames Received", yb::MetricUnit::kMessages,
    "Response frames, partial and final, received for MultiRaftUpdateConsensus RPCs whose "
    "responses were streamed: those carrying data requests, sent while "
    "enable_multi_raft_streaming_responses was on. A failed RPC has no final frame.");
METRIC_DEFINE_event_stats(server, multi_raft_frame_slots,
    "Multi-Raft Response Frame Slots", yb::MetricUnit::kRequests,
    "Number of Raft request responses in each response frame counted by "
    "multi_raft_frames_received.");
METRIC_DEFINE_counter(server, multi_raft_data_sends_direct,
    "Multi-Raft Direct Data Sends", yb::MetricUnit::kRequests,
    "Raft data requests sent in their own UpdateConsensus RPC instead of through the multi-Raft "
    "batcher because of multi_raft_data_direct_threshold_bytes, while "
    "enable_multi_raft_data_batching was set.");

namespace yb {
namespace consensus {

using rpc::PeriodicTimer;

namespace {

// One request of a batch: the callback that completes it (Peer::ProcessHeartbeatResponse or
// Peer::ProcessBatchedResponse, by kind).
struct ResponseCallbackData {
  MultiRaftBatcher::BatchResponseCallback callback;
  MultiRaftBatcher::RequestKind kind;
  // When a data request was added, for multi_raft_data_add_to_send_us. Read before SendBatch.
  std::chrono::steady_clock::time_point add_time;
};

// Runs f on a thread pool. If the pool drops the task (it is shutting down), f runs inline in
// Done, on the enqueuing thread, instead: f must run, since a peer holds a lock until its callback
// runs, and a completed envelope must leave data_batches_in_flight_.
template <class F>
class InlineOnRejectTask : public ThreadPoolTask {
 public:
  explicit InlineOnRejectTask(F f) : f_(std::move(f)) {}

  void Run() override {
    ran_ = true;
    f_();
  }

  void Done(const Status& status) override {
    if (!ran_) {
      Run();
    }
    delete this;
  }

 private:
  F f_;
  bool ran_ = false;
};

template <class F>
void EnqueueOrRunInline(rpc::ThreadPool* pool, F&& f) {
  pool->Enqueue(new InlineOnRejectTask<std::decay_t<F>>(std::forward<F>(f)));
}

// A partial response frame of a streamed batch, parsed in place: response lives on arena and
// points into buffer. The holder of the callbacks of the slots it answers.
struct StreamedFrame {
  StreamedFrame(RefCntBuffer buffer_, size_t arena_size)
      : buffer(std::move(buffer_)), arena(arena_size), response(&arena) {}

  RefCntBuffer buffer;
  ThreadSafeArena arena;
  LWMultiRaftConsensusResponsePB response;
};

constexpr size_t kFrameArenaBaseBytes = 512;
constexpr size_t kFrameArenaBytesPerBodyByte = 10;

// The first block of a frame's arena is sized from the frame instead of the 4 KB default, which
// would be mostly unused for the usual frame of one or two responses. Parsed LW messages take
// several times their wire size: a follower's response of about 80 bytes on the wire takes about
// 600 bytes parsed, and the ratio is higher for responses that set fewer fields.
size_t FrameArenaSize(size_t body_size) {
  return std::min<size_t>(kFrameArenaBaseBytes + kFrameArenaBytesPerBodyByte * body_size,
                          ThreadSafeArena::kMaxBlockSize);
}

class ProxyBatchSender : public MultiRaftBatchSender {
 public:
  ProxyBatchSender(rpc::ProxyCache* proxy_cache, const HostPort& hostport)
      : proxy_(proxy_cache, hostport) {}

  void SendBatch(
      const LWMultiRaftConsensusRequestPB& req, LWMultiRaftConsensusResponsePB* resp,
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

// - batch_req is serialized in SendBatch and must not be read after it returns, nor by any
//   response path (which may run inside SendBatch). Its sub-requests belong to their Raft peers,
//   and a peer may rebuild its request in place as soon as its slot is delivered, which for a
//   streamed batch is while the call is still in flight. Anything needed later (the destination,
//   the request count) is recorded separately: dest_uuid and response_callback_data.size().
// - Each sub-request, and the Peer members it references (Peer::update_request_ refs the
//   Peer's tablet id and uuids), is kept alive until serialization by its slot's callback, which
//   binds a strong reference to the Peer. So no slot callback is moved out, cleared or reset
//   before the batch is destroyed: slot tasks run each callback in place. Batches that are never
//   serialized (Shutdown, an add after Shutdown) may run their callbacks at once.
// - A slot answered by the final response points into batch_res, which lives on arena, and into
//   the buffer of controller's call; its holder is this batch. controller is reset only before the
//   send, and the batcher makes one attempt per batch, so it is never reset while a holder may be
//   alive. A slot answered by a partial frame points into the frame, which its holder keeps.
// - Streamed batches only: slots_lock guards answered, partials_processed and final_processed.
//   Partial frames and the final normally arrive on the reactor in order, but the final can also
//   run on the thread that fails the call (RpcController::MarkCallAsFailed from the Peer's stuck
//   call detector, or a send failure inside SendBatch) while a partial frame is being processed
//   on the reactor. The lock makes each slot answered exactly once: by a frame, or by the final.
//   The critical sections only flip bits, so a spinlock suits them. Nothing is enqueued or invoked
//   under it, and it allocates only to record an invalid index.
struct MultiRaftBatcher::MultiRaftConsensusData {
  // Declared before the messages that live on it.
  ThreadSafeArena arena;
  LWMultiRaftConsensusRequestPB batch_req{&arena};
  LWMultiRaftConsensusResponsePB batch_res{&arena};
  rpc::RpcController controller;
  std::vector<ResponseCallbackData> response_callback_data;
  // Pins the log cache messages that the ops of the sub-requests point to, one per slot. Cleared
  // as soon as SendBatch returns. Slot callbacks may already be running on other threads by then,
  // so these are kept apart from response_callback_data.
  std::vector<LWReplicateMsgsHolder> msgs_holders;
  // Streamed batches only, sized when the batch is sent: whether a frame has answered each slot.
  std::vector<bool> answered GUARDED_BY(slots_lock);
  // Copied from the first sub-request. Every sub-request goes to the same server.
  std::string dest_uuid;
  size_t num_data_requests = 0;
  size_t num_ops = 0;
  size_t total_bytes = 0;

  // Set before SendBatch.
  std::shared_ptr<MultiRaftBatchSender> sender;
  std::shared_ptr<Metrics> metrics;

  // Set once the sender has returned from SendBatch. RpcController assigns its call without
  // holding its lock, so BatchHandle must not touch the controller before then. After it, nothing
  // writes the controller's call again.
  std::atomic<bool> sent{false};

  // Set when the batch RPC completes, before any sub-callback runs.
  std::atomic<bool> completed{false};

  // Set before SendBatch.
  bool streamed = false;

  simple_spinlock slots_lock;
  bool partials_processed GUARDED_BY(slots_lock) = false;
  bool final_processed GUARDED_BY(slots_lock) = false;

  bool empty() const { return response_callback_data.empty(); }
  bool has_data() const { return num_data_requests > 0; }
};

// The callback of one slot to run, with its outcome.
struct MultiRaftBatcher::SlotDelivery {
  size_t idx;
  Status status;
  const LWConsensusResponsePB* response;
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
    flush_paced_inline = METRIC_multi_raft_flush_paced_inline.Instantiate(entity);
    flush_paced_timer = METRIC_multi_raft_flush_paced_timer.Instantiate(entity);
    flush_count_cap = METRIC_multi_raft_flush_count_cap.Instantiate(entity);
    flush_bytes_cap = METRIC_multi_raft_flush_bytes_cap.Instantiate(entity);
    flush_timer = METRIC_multi_raft_flush_timer.Instantiate(entity);
    heartbeat_sub_requests = METRIC_multi_raft_heartbeat_sub_requests.Instantiate(entity);
    data_sub_requests = METRIC_multi_raft_data_sub_requests.Instantiate(entity);
    data_sends_direct = METRIC_multi_raft_data_sends_direct.Instantiate(entity);
    batched_response_processing_us =
        METRIC_multi_raft_batched_response_processing_us.Instantiate(entity);
    data_add_to_send_us = METRIC_multi_raft_data_add_to_send_us.Instantiate(entity);
    frames_received = METRIC_multi_raft_frames_received.Instantiate(entity);
    frame_slots = METRIC_multi_raft_frame_slots.Instantiate(entity);
  }

  CounterPtr envelopes_sent;
  CounterPtr envelopes_failed;
  EventStatsPtr envelope_sub_requests;
  EventStatsPtr envelope_bytes;
  EventStatsPtr envelope_ops;
  CounterPtr flush_immediate;
  CounterPtr flush_response_clocked;
  CounterPtr flush_deadline;
  CounterPtr flush_paced_inline;
  CounterPtr flush_paced_timer;
  CounterPtr flush_count_cap;
  CounterPtr flush_bytes_cap;
  CounterPtr flush_timer;
  CounterPtr heartbeat_sub_requests;
  CounterPtr data_sub_requests;
  CounterPtr data_sends_direct;
  EventStatsPtr batched_response_processing_us;
  EventStatsPtr data_add_to_send_us;
  CounterPtr frames_received;
  EventStatsPtr frame_slots;
};

enum class MultiRaftBatcher::FlushReason {
  kImmediate,
  kResponseClocked,
  kDeadline,
  kCountCap,
  kBytesCap,
  kTimer,
  kPacedInline,
  kPacedTimer,
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
      metrics_(std::make_shared<Metrics>(metric_entity)),
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
    LWConsensusRequestPB* request, LWReplicateMsgsHolder holder, BatchResponseCallback callback,
    RequestKind kind) {
  const auto request_bytes = request->SerializedSize();
  // The pacing decision and multi_raft_data_add_to_send_us need it for data requests only.
  const auto add_time = kind == RequestKind::kData
      ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
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
          request, &holder, std::move(callback), kind, request_bytes, add_time, &pre_add_batch,
          &post_add_batch);
    }
  }
  if (aborted) {
    callback(STATUS(Aborted, "MultiRaft shutdown"), /* response= */ nullptr, /* holder= */ {});
    return handle;
  }
  SendBatchRequest(std::move(pre_add_batch));
  SendBatchRequest(std::move(post_add_batch));
  return handle;
}

MultiRaftBatcher::BatchHandle MultiRaftBatcher::AddRequestUnlocked(
    LWConsensusRequestPB* request, LWReplicateMsgsHolder* holder, BatchResponseCallback callback,
    RequestKind kind, size_t request_bytes, std::chrono::steady_clock::time_point add_time,
    BatchData* pre_add_batch, BatchData* post_add_batch) {
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

  if (current_batch_->empty()) {
    current_batch_->dest_uuid = request->dest_uuid().ToBuffer();
  }
  current_batch_->response_callback_data.push_back({
    .callback = std::move(callback),
    .kind = kind,
    .add_time = add_time,
  });
  current_batch_->msgs_holders.push_back(std::move(*holder));
  current_batch_->total_bytes += request_bytes;
  current_batch_->num_ops += request->ops().size();
  if (is_data) {
    ++current_batch_->num_data_requests;
  }
  current_batch_->batch_req.mutable_consensus_request()->push_back_ref(request);
  BatchHandle handle(current_batch_);

  if (FLAGS_multi_raft_batch_size > 0
      && current_batch_->response_callback_data.size() >= FLAGS_multi_raft_batch_size) {
    *post_add_batch = PrepareNextBatchRequest(FlushReason::kCountCap);
  } else if (is_data) {
    // A pre-add flush that carried data counts here: under response clocking it is in flight, and
    // under pacing it started a new window. Either way this request waits instead of going out in
    // a second envelope right behind it.
    const auto window_us = FLAGS_multi_raft_data_flush_window_us;
    if (window_us > 0) {
      const auto window = std::chrono::microseconds(window_us);
      // add_time was read before mutex_, so it may precede last_data_send_; that only parks.
      if (last_data_send_ == std::chrono::steady_clock::time_point::min() ||
          add_time - last_data_send_ >= window) {
        *post_add_batch = PrepareNextBatchRequest(FlushReason::kPacedInline);
      } else {
        ArmPacingTask(last_data_send_ + window);
      }
    } else if (data_batches_in_flight_ == 0) {
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
  CancelPacingTask();
  auto data = std::make_shared<MultiRaftConsensusData>();
  current_batch_.swap(data);
  auto running_calls = ++*running_calls_;
  LOG_IF(DFATAL, running_calls <= 0) << "Wrong number or running calls: " << running_calls;
  if (data->has_data()) {
    ++data_batches_in_flight_;
    last_data_send_ = std::chrono::steady_clock::now();
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
    case FlushReason::kPacedInline:
      IncrementCounter(metrics_->flush_paced_inline);
      break;
    case FlushReason::kPacedTimer:
      IncrementCounter(metrics_->flush_paced_timer);
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

void MultiRaftBatcher::ArmPacingTask(std::chrono::steady_clock::time_point time) {
  // A task already armed keeps its time, even if the window flag changed since: the armed time is
  // computed only when a request parks.
  if (pacing_task_ != rpc::kInvalidTaskId) {
    return;
  }
  std::weak_ptr<MultiRaftBatcher> weak_self = shared_from_this();
  // An absolute time, so that the time spent between the add and here does not stretch the
  // window. It is in the future relative to the add time, since an add whose window has passed
  // sends inline. An add delayed on mutex_ past the end of the window arms a time already past,
  // which fires at once from the scheduler. The task cannot observe pacing_task_ before it is
  // assigned, because it takes mutex_, which is held here.
  pacing_task_ = messenger_->scheduler().Schedule(
      [weak_self](rpc::ScheduledTaskId task_id, const Status& status) {
        if (!status.ok()) {
          return;
        }
        if (auto self = weak_self.lock()) {
          self->PacingTaskExpired(task_id);
        }
      },
      time);
}

void MultiRaftBatcher::CancelPacingTask() {
  if (pacing_task_ == rpc::kInvalidTaskId) {
    return;
  }
  // Abort runs the task asynchronously with an error status, which the task ignores.
  messenger_->scheduler().Abort(pacing_task_);
  pacing_task_ = rpc::kInvalidTaskId;
}

void MultiRaftBatcher::PacingTaskExpired(rpc::ScheduledTaskId task_id) {
  BatchData data;
  {
    std::lock_guard lock(mutex_);
    // The task may have fired after the batch that armed it was sent and a new task was armed.
    if (pacing_task_ != task_id) {
      return;
    }
    pacing_task_ = rpc::kInvalidTaskId;
    // Every send cancels this task, so the batch still holds the data that armed it. Parked
    // heartbeats alone would be left for the heartbeat timer.
    if (current_batch_->has_data()) {
      data = PrepareNextBatchRequest(FlushReason::kPacedTimer);
    }
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
    // Parked heartbeats alone are left for the timer. Under pacing a completion sends nothing:
    // parked data has the pacing timer armed (or, after a switch at runtime, the deadline).
    if (FLAGS_multi_raft_data_flush_window_us == 0 && current_batch_->has_data()) {
      next = PrepareNextBatchRequest(FlushReason::kResponseClocked);
    }
  }
  SendBatchRequest(std::move(next));
}

void MultiRaftBatcher::InvokeSubCallback(
    const BatchData& data, size_t idx, const Status& status,
    const LWConsensusResponsePB* response, const std::shared_ptr<void>& holder) {
  auto& callback_data = data->response_callback_data[idx];
  // Every data sub-callback, whether it runs in a SubCallbackTask, inline from its Done, or inline
  // when there is no pool, comes through here.
  const auto& processing_us = data->metrics->batched_response_processing_us;
  const auto start = callback_data.kind == RequestKind::kData && processing_us
      ? MonoTime::Now() : MonoTime();
  if (status.ok()) {
    callback_data.callback(status, response, holder);
  } else {
    callback_data.callback(status, /* response= */ nullptr, /* holder= */ {});
  }
  if (start.Initialized()) {
    processing_us->Increment((MonoTime::Now() - start).ToMicroseconds());
  }
}

// Runs one sub-callback on a thread pool. If the pool drops the task (it is shutting down), the
// sub-callback runs inline in Done instead: a caller may hold a lock until its callback runs. For
// a streamed batch the enqueuing thread is normally the reactor, which runs a callback inline this
// way only while the messenger shuts its pools down.
class MultiRaftBatcher::SubCallbackTask : public ThreadPoolTask {
 public:
  SubCallbackTask(
      BatchData data, size_t idx, Status status, const LWConsensusResponsePB* response,
      std::shared_ptr<void> holder)
      : data_(std::move(data)), idx_(idx), status_(std::move(status)), response_(response),
        holder_(std::move(holder)) {}

  void Run() override {
    ran_ = true;
    InvokeSubCallback(data_, idx_, status_, response_, holder_);
  }

  void Done(const Status& status) override {
    if (!ran_) {
      Run();
    }
    delete this;
  }

 private:
  // Keeps the slot's callback, which stays in the batch, alive.
  BatchData data_;
  size_t idx_;
  Status status_;
  const LWConsensusResponsePB* response_;
  // Keeps response_ valid until the callback returns.
  std::shared_ptr<void> holder_;
  bool ran_ = false;
};

void MultiRaftBatcher::DispatchSubCallbacks(
    const BatchData& data, Status status, rpc::ThreadPool* data_pool) {
  const auto num_requests = data->response_callback_data.size();
  if (status.ok() && data->batch_res.consensus_response_size() != num_requests) {
    status = STATUS_FORMAT(
        IllegalState, "MultiRaft response has $0 responses for $1 requests",
        data->batch_res.consensus_response_size(), num_requests);
    YB_LOG_EVERY_N_SECS(ERROR, 1) << status << ", destination: " << data->dest_uuid;
  }
  if (!status.ok()) {
    IncrementCounter(data->metrics->envelopes_failed);
  }
  const std::shared_ptr<void> holder = status.ok() ? data : nullptr;
  auto response_it = data->batch_res.consensus_response().begin();
  // Each data sub-callback gets its own task because it does a full Raft response round
  // (majority tracking and building the next request). Heartbeat sub-callbacks are cheap and
  // run serially on this thread.
  for (size_t i = 0; i != num_requests; ++i) {
    const LWConsensusResponsePB* response = nullptr;
    if (status.ok()) {
      response = &*response_it;
      ++response_it;
    }
    if (data_pool && data->response_callback_data[i].kind == RequestKind::kData) {
      data_pool->Enqueue(new SubCallbackTask(data, i, status, response, holder));
    } else {
      InvokeSubCallback(data, i, status, response, holder);
    }
  }
}

void MultiRaftBatcher::EnqueueSlots(
    const BatchData& data, const std::vector<SlotDelivery>& slots,
    const std::shared_ptr<void>& holder, rpc::ThreadPool* pool) {
  std::vector<SlotDelivery> heartbeats;
  for (const auto& slot : slots) {
    if (data->response_callback_data[slot.idx].kind == RequestKind::kData) {
      pool->Enqueue(new SubCallbackTask(data, slot.idx, slot.status, slot.response, holder));
    } else {
      heartbeats.push_back(slot);
    }
  }
  // A heartbeat callback can send its peer's next request, and with it an envelope, so it must
  // not run on the reactor. They are cheap, so one task runs those of the frame in order.
  if (!heartbeats.empty()) {
    EnqueueOrRunInline(pool, [data, holder, heartbeats = std::move(heartbeats)] {
      for (const auto& slot : heartbeats) {
        InvokeSubCallback(data, slot.idx, slot.status, slot.response, holder);
      }
    });
  }
}

void MultiRaftBatcher::ProcessPartialFrame(
    const BatchData& data, Slice body, RefCntBuffer buffer, rpc::ThreadPool* pool) {
  DCHECK(data->streamed);
  auto frame = std::make_shared<StreamedFrame>(std::move(buffer), FrameArenaSize(body.size()));
  auto& response = frame->response;
  auto status = response.ParseFromSlice(body);
  if (!status.ok()) {
    LOG(DFATAL) << "Malformed MultiRaft partial response frame: " << status
                << ", destination: " << data->dest_uuid;
    return;
  }
  const auto num_entries = response.index_size();
  if (num_entries == 0 || num_entries != response.consensus_response_size()) {
    LOG(DFATAL) << "Malformed MultiRaft partial response frame: " << num_entries
                << " indices for " << response.consensus_response_size()
                << " responses, destination: " << data->dest_uuid;
    return;
  }
  IncrementCounter(data->metrics->frames_received);
  IncrementStats(data->metrics->frame_slots, num_entries);

  const auto num_requests = data->response_callback_data.size();
  std::vector<SlotDelivery> slots;
  slots.reserve(num_entries);
  std::vector<uint32_t> invalid;
  {
    std::lock_guard lock(data->slots_lock);
    if (data->final_processed) {
      // The final ran first, on the thread that failed the call, and has failed every slot this
      // frame would answer.
      return;
    }
    data->partials_processed = true;
    auto response_it = response.consensus_response().begin();
    for (auto idx : response.index()) {
      const auto* slot_response = &*response_it;
      ++response_it;
      if (idx >= num_requests || data->answered[idx]) {
        // Rare enough that the allocation under the lock does not matter.
        invalid.push_back(idx);
        continue;
      }
      data->answered[idx] = true;
      slots.push_back({.idx = idx, .status = Status::OK(), .response = slot_response});
    }
  }
  for (auto idx : invalid) {
    LOG(DFATAL) << "MultiRaft response frame has an invalid index " << idx << " for "
                << num_requests << " requests (duplicate or out of range), destination: "
                << data->dest_uuid;
  }
  EnqueueSlots(data, slots, frame, pool);
}

void MultiRaftBatcher::ProcessStreamedFinal(
    const BatchData& data, const Status& status, rpc::ThreadPool* pool) {
  DCHECK(data->streamed);
  const auto num_requests = data->response_callback_data.size();
  std::vector<SlotDelivery> slots;
  slots.reserve(num_requests);
  std::vector<uint32_t> invalid;
  std::vector<size_t> unanswered;
  const auto& response = data->batch_res;
  const auto num_entries = response.index_size();
  const auto num_responses = response.consensus_response_size();
  // What the final response carries, decided under the lock.
  enum class Kind {
    kFailed,        // The RPC failed: unanswered slots get its status.
    kIndexed,       // The final lists the slots it answers.
    kOldFollower,   // No index and no partial frame: every response in request order.
    kMalformed,     // Unusable: unanswered slots get IllegalState.
  };
  Kind kind;
  {
    std::lock_guard lock(data->slots_lock);
    data->final_processed = true;
    if (!status.ok()) {
      kind = Kind::kFailed;
    } else if (num_entries == 0 && !data->partials_processed) {
      kind = Kind::kOldFollower;
    } else if (num_entries == num_responses) {
      // A final with nothing left to answer (no index and no responses) is well formed.
      kind = Kind::kIndexed;
      auto response_it = response.consensus_response().begin();
      for (auto idx : response.index()) {
        const auto* slot_response = &*response_it;
        ++response_it;
        if (idx >= num_requests || data->answered[idx]) {
          invalid.push_back(idx);
          continue;
        }
        data->answered[idx] = true;
        slots.push_back({.idx = idx, .status = Status::OK(), .response = slot_response});
      }
    } else {
      kind = Kind::kMalformed;
    }
    if (kind != Kind::kOldFollower) {
      for (size_t idx = 0; idx != num_requests; ++idx) {
        if (!data->answered[idx]) {
          unanswered.push_back(idx);
        }
      }
    }
  }

  if (status.ok()) {
    IncrementCounter(data->metrics->frames_received);
    IncrementStats(data->metrics->frame_slots, num_responses);
  }
  for (auto idx : invalid) {
    LOG(DFATAL) << "MultiRaft response frame has an invalid index " << idx << " for "
                << num_requests << " requests (duplicate or out of range), destination: "
                << data->dest_uuid;
  }
  switch (kind) {
    case Kind::kFailed:
      // The RPC failed as a whole, so it counts, as for a batch answered once.
      IncrementCounter(data->metrics->envelopes_failed);
      for (auto idx : unanswered) {
        slots.push_back({.idx = idx, .status = status, .response = nullptr});
      }
      break;
    case Kind::kOldFollower: {
      // The follower did not stream: an old binary, its serial handler, or its
      // enable_multi_raft_data_batching off.
      Status slot_status;
      if (num_responses != num_requests) {
        slot_status = STATUS_FORMAT(
            IllegalState, "MultiRaft response has $0 responses for $1 requests", num_responses,
            num_requests);
        YB_LOG_EVERY_N_SECS(ERROR, 1) << slot_status << ", destination: " << data->dest_uuid;
        IncrementCounter(data->metrics->envelopes_failed);
      }
      auto response_it = response.consensus_response().begin();
      for (size_t idx = 0; idx != num_requests; ++idx) {
        const LWConsensusResponsePB* slot_response = nullptr;
        if (slot_status.ok()) {
          slot_response = &*response_it;
          ++response_it;
        }
        slots.push_back({.idx = idx, .status = slot_status, .response = slot_response});
      }
      break;
    }
    case Kind::kMalformed:
      if (num_entries == 0) {
        LOG(DFATAL) << "MultiRaft final response has no index after partial frames: "
                    << num_responses << " responses, destination: " << data->dest_uuid;
      } else {
        LOG(DFATAL) << "Malformed MultiRaft final response: " << num_entries << " indices for "
                    << num_responses << " responses, destination: " << data->dest_uuid;
      }
      [[fallthrough]];
    case Kind::kIndexed:
      if (!unanswered.empty()) {
        const auto& destination = data->dest_uuid;
        YB_LOG_EVERY_N_SECS(ERROR, 1)
            << "MultiRaft response has no response for requests " << AsString(unanswered)
            << " of " << num_requests << ", destination: " << destination;
        IncrementCounter(data->metrics->envelopes_failed);
        for (auto idx : unanswered) {
          slots.push_back({
              .idx = idx,
              .status = STATUS_FORMAT(
                  IllegalState,
                  "MultiRaft response has no response for request $0, destination: $1", idx,
                  destination),
              .response = nullptr});
        }
      }
      break;
  }
  EnqueueSlots(data, slots, data, pool);
}

// Kept out of line as a uprobe attach point for timing envelopes.
ATTRIBUTE_NOINLINE void MultiRaftBatcher::SendBatchRequest(BatchData data) {
  if (!data) {
    return;
  }
  // TODO(#16670): give consensus its own root trace.
  auto detach_token = dist_trace::DetachTraceContext();

  const auto num_requests = narrow_cast<int>(data->response_callback_data.size());
  IncrementCounter(metrics_->envelopes_sent);
  IncrementStats(metrics_->envelope_sub_requests, num_requests);
  IncrementStats(metrics_->envelope_bytes, data->total_bytes);
  IncrementStats(metrics_->envelope_ops, data->num_ops);
  const auto num_data_requests = narrow_cast<int>(data->num_data_requests);
  IncrementCounterBy(metrics_->heartbeat_sub_requests, num_requests - num_data_requests);
  IncrementCounterBy(metrics_->data_sub_requests, num_data_requests);

  // The only Reset: the batcher makes one attempt per batch, so the partial-response callback
  // installed below never needs to be installed again after a Reset.
  data->controller.Reset();
  int64_t timeout_ms = int64_t{FLAGS_consensus_rpc_timeout_ms} * num_requests;
  rpc::ThreadPool* data_pool = nullptr;
  const bool streamed = data->has_data() && FLAGS_enable_multi_raft_streaming_responses;
  if (data->has_data()) {
    // Data acks are latency-critical, like those of the direct UpdateConsensus path, which also
    // uses the high-priority pool. Heartbeat-only batches keep the normal pool and the uncapped
    // timeout they have always had.
    auto timeout_cap_ms = int64_t{FLAGS_multi_raft_rpc_timeout_ms};
    if (timeout_cap_ms == 0) {
      timeout_cap_ms = 2 * int64_t{FLAGS_consensus_rpc_timeout_ms};
    }
    timeout_ms = std::min(timeout_ms, timeout_cap_ms);
    // A streamed batch is received on the reactor, which only enqueues to the High pool.
    data->controller.set_invoke_callback_mode(
        streamed ? rpc::InvokeCallbackMode::kReactorThread
                 : rpc::InvokeCallbackMode::kThreadPoolHigh);
    data_pool = &messenger_->ThreadPool(rpc::ServicePriority::kHigh);
  }
  data->controller.set_timeout(MonoDelta::FromMilliseconds(timeout_ms));
  data->sender = sender_;
  data->metrics = metrics_;

  MultiRaftBatchSender::DoneCallback callback;
  if (streamed) {
    data->streamed = true;
    {
      std::lock_guard lock(data->slots_lock);
      data->answered.resize(num_requests);
    }
    // Serialized in SendBatch, so this is still before the request is dead.
    data->batch_req.set_stream_responses(true);
    // Weak: the batch owns the controller, which holds the callback until the call takes it. While
    // the call is in flight its final callback keeps the batch alive.
    data->controller.set_partial_response_callback(
        [weak_data = std::weak_ptr<MultiRaftConsensusData>(data), data_pool](
            Slice body, RefCntBuffer buffer) {
          if (auto data = weak_data.lock()) {
            ProcessPartialFrame(data, body, std::move(buffer), data_pool);
          }
        });
    // Runs on the reactor, or on the thread that failed the call (under the controller's lock for
    // RpcController::MarkCallAsFailed, so it must not call into the controller). BatchCompleted
    // is enqueued before the slots, but a slot task that adds its peer's next request may still
    // run first: under response clocking it sees this envelope in flight and waits for the
    // response-clocked flush.
    callback = [data, data_pool, weak_self = weak_from_this(),
                running_calls = running_calls_](const Status& status) {
      data->completed.store(true, std::memory_order_release);
      --*running_calls;
      // Inline only if the pool is shutting down, which on the reactor happens only while the
      // messenger shuts down. MultiRaftManager::CompleteShutdown waits for running_calls, but
      // only after StartShutdown has set every batcher's shutdown_, so a BatchCompleted that runs
      // after that wait never prepares another batch.
      EnqueueOrRunInline(data_pool, [weak_self, data] {
        if (auto self = weak_self.lock()) {
          self->BatchCompleted(*data);
        }
      });
      ProcessStreamedFinal(data, status, data_pool);
    };
  } else {
    // Under response clocking, the response-clocked flush runs before the sub-callbacks are
    // dispatched. Requests that were already pending go out without waiting for them, and requests
    // that the sub-callbacks add next see that envelope in flight and accumulate behind it rather
    // than each going out alone.
    callback = [data, data_pool, weak_self = weak_from_this(),
                running_calls = running_calls_](const Status& status) {
      data->completed.store(true, std::memory_order_release);
      --*running_calls;
      if (auto self = weak_self.lock()) {
        self->BatchCompleted(*data);
      }
      DispatchSubCallbacks(data, status, data_pool);
    };
  }
  // Before SendBatch: the final can run inside it, and slot tasks may run before it returns.
  if (data->has_data() && metrics_->data_add_to_send_us) {
    const auto send_time = std::chrono::steady_clock::now();
    for (const auto& slot : data->response_callback_data) {
      if (slot.kind == RequestKind::kData) {
        metrics_->data_add_to_send_us->Increment(
            std::chrono::duration_cast<std::chrono::microseconds>(send_time - slot.add_time)
                .count());
      }
    }
  }
  sender_->SendBatch(data->batch_req, &data->batch_res, &data->controller, std::move(callback));
  data->msgs_holders.clear();
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
    CancelPacingTask();
    batch = std::make_shared<MultiRaftConsensusData>();
    batch.swap(current_batch_);
  }
  // The batch was never sent, so nothing has serialized or will serialize its requests.
  batch->msgs_holders.clear();
  static const Status status = STATUS(Aborted, "MultiRaft shutdown");
  for (const auto& callback : batch->response_callback_data) {
    callback.callback(status, /* response= */ nullptr, /* holder= */ {});
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
