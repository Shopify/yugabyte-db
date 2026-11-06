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
#include <memory>

#include "yb/common/common_net.pb.h"

#include "yb/consensus/consensus_fwd.h"

#include "yb/rpc/rpc_controller.h"

#include "yb/util/metrics_fwd.h"
#include "yb/util/net/net_util.h"

namespace yb {

namespace rpc {
class Messenger;
class PeriodicTimer;
}

namespace consensus {

// Sends one MultiRaftUpdateConsensus RPC. The batcher's only dependency on the RPC layer, so that
// tests can capture envelopes and complete them on demand.
class MultiRaftBatchSender {
 public:
  // Envelope-level status of the RPC (the controller's status in production).
  using DoneCallback = std::function<void(const Status&)>;

  virtual ~MultiRaftBatchSender() = default;

  // Must invoke done exactly once. done may run before SendBatch returns, on this or another
  // thread.
  // - req references requests that belong to their Raft peers. It must be serialized before
  //   SendBatch returns and must not be read afterwards. The proxy serializes it in
  //   Proxy::PrepareCall. A local call would read req later, but a batcher always sends to a
  //   remote HostPort, so the envelope never goes through LocalOutboundCall.
  // - resp and controller stay valid until done has run. resp is parsed in place: the parsed
  //   messages live on resp's arena and point into the buffer of controller's call.
  virtual void SendBatch(
      const LWMultiRaftConsensusRequestPB& req, LWMultiRaftConsensusResponsePB* resp,
      rpc::RpcController* controller, DoneCallback done) = 0;

  // Fails a batch RPC that SendBatch issued on controller and that has not completed, so that
  // its done callback runs with an error.
  virtual void MarkCallAsFailed(rpc::RpcController* controller) {
    controller->MarkCallAsFailed();
  }
};

std::unique_ptr<MultiRaftBatchSender> CreateMultiRaftBatchSender(
    rpc::ProxyCache* proxy_cache, const HostPort& hostport);

// - MultiRaftBatcher batches Raft requests from the local peers of different tablets that
//   replicate to the same remote server (a tserver, or a master for sys catalog peers), and sends
//   each batch as one MultiRaftUpdateConsensus RPC.
// - Heartbeats (RequestKind::kHeartbeat) are sent every FLAGS_multi_raft_heartbeat_interval_ms
//   ms, once the batch reaches FLAGS_multi_raft_batch_size requests, or with the next batch that
//   carries data. Adding one sends a batch only through a size limit, and never arms a data
//   timer.
// - Data requests (RequestKind::kData) follow one of two policies, chosen on each add (and, for
//   the response clock, on each completion) by FLAGS_multi_raft_data_flush_window_us:
//   - 0, response clocking: a data request is sent immediately when no batch carrying data to
//     this server is in flight. Otherwise it waits until such a batch completes or
//     FLAGS_multi_raft_data_flush_delay_ms elapses, whichever is first. A streamed batch stays in
//     flight until its final response, however many requests partial frames have answered.
//   - Above 0, pacing: batches carrying data to this server go out at most once per window,
//     except when a size limit or the heartbeat timer sends one. A data request is sent
//     immediately when, at its add, a window has passed since the last one was sent (or none
//     was). Otherwise it waits for one timer, armed for the end of that window; an add delayed on
//     the batcher's lock past that end arms a time already past, which fires at once. Completions
//     and response frames send nothing: back-pressure does not need a response clock, because
//     each Raft peer has at most one data request parked or in flight. Every batch that carries
//     data restarts the window, whatever sent it (a cap, a timer, a completion), so a request
//     added just after a cap flush waits a full window. A timer that finds only heartbeats left
//     sends nothing and leaves them to the heartbeat timer.
//   Changing the flag at runtime needs no migration: a deadline or pacing timer already armed
//   fires and sends what is parked, any send cancels both, and the heartbeat timer remains the
//   backstop under either policy. Pacing times are std::chrono::steady_clock, not
//   CoarseMonoClock, whose 1-4 ms resolution cannot pace a window of tens of microseconds.
// - Under either policy, a request that would push the batch past
//   FLAGS_multi_raft_data_batch_size data requests or FLAGS_multi_raft_batch_bytes bytes sends the
//   current batch first.
// - Multiple batches may be in flight concurrently, but only one is built at a time.
//
// Nothing is copied on the way. The envelope references each added request, together with the
// caller's LWReplicateMsgsHolder, which pins the log cache messages that the request's ops point
// to. The envelope is serialized once, in MultiRaftBatchSender::SendBatch, and the holders are
// released when SendBatch returns. A batch that is never sent (aborted at Shutdown) is never
// serialized. Responses are parsed in place, and each request's callback gets a pointer to its own
// response together with a holder that keeps that response alive.
//
// An envelope is completed in one of two modes, chosen when it is sent:
// - Answered once (heartbeat-only envelopes, and every envelope while
//   FLAGS_enable_multi_raft_streaming_responses is off). The final response carries every
//   request's response in request order. Its callback runs on the High pool for envelopes that
//   carry data (the normal pool otherwise), runs BatchCompleted (the response-clocked flush under
//   response clocking), then runs each
//   heartbeat callback inline and gives each data callback its own High-pool task. Every holder
//   is the batch. If the RPC fails, every request fails with its error.
// - Streamed (envelopes that carry data, while the flag is on). The follower answers each request
//   as soon as it completes, in partial response frames followed by the final response; every
//   frame lists the requests it answers. Frames are received on the reactor, and so is the final
//   unless the call is failed on another thread (RpcController::MarkCallAsFailed, or a send failure
//   inside SendBatch), which then runs it. Receiving only parses the frame, marks its requests
//   answered and enqueues: one High-pool task per data request, and one per frame for its
//   heartbeats (a heartbeat callback can send the peer's next request, which may serialize an
//   envelope). BatchCompleted, which sends a batch under response clocking, also runs in a
//   High-pool task, so no envelope is serialized on a reactor (unless the pool is shutting down,
//   when tasks run inline on the enqueuing thread). A request answered by a partial frame
//   gets that frame as its holder; one answered by the final gets the batch. A failed RPC fails
//   only the requests no frame has answered, and an OK final that leaves a request unanswered
//   fails it with IllegalState. A final without the list is from a follower that did not stream,
//   and is taken as every response in request order.
class MultiRaftBatcher : public std::enable_shared_from_this<MultiRaftBatcher> {
 private:
  // Tracks all the metadata for a single batch request, including a list of all
  // ResponseCallbackData registered by each local peer with this batch in AddRequestToBatch().
  struct MultiRaftConsensusData;
  using BatchData = std::shared_ptr<MultiRaftConsensusData>;

 public:
  // Refers to the batch that carries a request, for diagnosing and failing a stuck batch RPC.
  // Does not keep the batch alive. Until the batch's RPC has been issued, and after the batch has
  // been released, the batch cannot be inspected: timeout() is uninitialized and
  // MarkCallAsFailed() does nothing.
  class BatchHandle {
   public:
    BatchHandle() = default;

    // Fails the batch RPC through MultiRaftBatchSender::MarkCallAsFailed, unless it has finished.
    // Every request of the batch that no response frame has answered yet gets the failure.
    void MarkCallAsFailed() const;
    std::string CallStateDebugString() const;
    // The batch RPC's timeout.
    MonoDelta timeout() const;
    // True once the batch RPC has completed or the batch has been released. False before it is
    // issued.
    bool finished() const;
    bool expired() const { return data_.expired(); }

    const rpc::RpcController* TEST_controller() const;

   private:
    friend class MultiRaftBatcher;

    explicit BatchHandle(std::weak_ptr<MultiRaftConsensusData> data) : data_(std::move(data)) {}

    std::weak_ptr<MultiRaftConsensusData> data_;
  };

  MultiRaftBatcher(rpc::Messenger* messenger,
                   std::unique_ptr<MultiRaftBatchSender> sender,
                   std::atomic<int>* running_calls,
                   const MetricEntityPtr& metric_entity);

  ~MultiRaftBatcher();

  // Required to start a periodic timer to send out batches.
  void Start();

  enum class RequestKind { kHeartbeat, kData };
  // response is valid only while holder is alive. It is null, and holder empty, iff status is not
  // OK. holder is the whole batch, or the partial response frame that answered this request
  // together with every other response in it, so it must not be kept past the callback.
  using BatchResponseCallback = std::function<void(
      const Status& status, const LWConsensusResponsePB* response, std::shared_ptr<void> holder)>;

  // Adds the request to a batch. The batch references request instead of copying it, so request,
  // and everything it references, must stay unchanged and alive until the callback runs. holder
  // pins the messages that request's ops point to, and is released once the envelope has been
  // serialized (or the batch is aborted). The callback runs exactly once: with the request's own
  // response once a response frame answers it, or with an error if the batch RPC fails (or ends)
  // before any frame answered it. After Shutdown() it runs immediately with Aborted. The callback
  // may run before this returns. Returns a handle to the batch the request was added to.
  BatchHandle AddRequestToBatch(LWConsensusRequestPB* request,
                                LWReplicateMsgsHolder holder,
                                BatchResponseCallback callback,
                                RequestKind kind);

  // Counts a data request that bypassed the batcher.
  void RecordDirectDataSend();

  void Shutdown();

 private:
  struct Metrics;
  struct SlotDelivery;
  enum class FlushReason;
  class SubCallbackTask;

  // Adds the request, and returns in pre_add_batch / post_add_batch any batches to send.
  // add_time is when a data request was added (unset for heartbeats).
  BatchHandle AddRequestUnlocked(
      LWConsensusRequestPB* request, LWReplicateMsgsHolder* holder, BatchResponseCallback callback,
      RequestKind kind, size_t request_bytes, std::chrono::steady_clock::time_point add_time,
      BatchData* pre_add_batch, BatchData* post_add_batch) REQUIRES(mutex_);

  void PrepareAndSendBatchRequest();

  // This method will return a nullptr if the current batch is empty.
  BatchData PrepareNextBatchRequest(FlushReason reason) REQUIRES(mutex_);

  // If data is null then we will not send a batch request.
  void SendBatchRequest(BatchData data);

  // Completes every request of a batch that was answered once. Data requests are dispatched to
  // data_pool when it is set.
  static void DispatchSubCallbacks(
      const BatchData& data, Status status, rpc::ThreadPool* data_pool);
  static void InvokeSubCallback(
      const BatchData& data, size_t idx, const Status& status,
      const LWConsensusResponsePB* response, const std::shared_ptr<void>& holder);

  // Streamed batches. Run on the reactor, except that the final can run on the thread that fails
  // the call. They only parse, do bookkeeping and enqueue to pool.
  static void ProcessPartialFrame(
      const BatchData& data, Slice body, RefCntBuffer buffer, rpc::ThreadPool* pool);
  static void ProcessStreamedFinal(
      const BatchData& data, const Status& status, rpc::ThreadPool* pool);
  // Enqueues the callbacks of the requests answered (or failed) by one frame.
  static void EnqueueSlots(
      const BatchData& data, const std::vector<SlotDelivery>& slots,
      const std::shared_ptr<void>& holder, rpc::ThreadPool* pool);

  void ArmDataFlushDeadline() REQUIRES(mutex_);
  void CancelDataFlushDeadline() REQUIRES(mutex_);
  void DataFlushDeadlineExpired(rpc::ScheduledTaskId task_id);

  // Arms the pacing task for time unless one is armed. The task (PacingTaskExpired) runs on a
  // scheduler io thread.
  void ArmPacingTask(std::chrono::steady_clock::time_point time) REQUIRES(mutex_);
  void CancelPacingTask() REQUIRES(mutex_);
  void PacingTaskExpired(rpc::ScheduledTaskId task_id);

  // Bookkeeping for a completed envelope, plus the response-clocked flush when
  // FLAGS_multi_raft_data_flush_window_us is 0. Runs on the thread that completed an envelope
  // answered once, and in a High-pool task for a streamed envelope (inline on the enqueuing thread,
  // which may be the reactor, only while the pool shuts down).
  void BatchCompleted(const MultiRaftConsensusData& data);

  rpc::Messenger* const messenger_;

  // Shared with each sent batch, which uses it to fail its RPC.
  const std::shared_ptr<MultiRaftBatchSender> sender_;

  // Shared with each sent batch, which may outlive the batcher.
  const std::shared_ptr<Metrics> metrics_;

  std::shared_ptr<rpc::PeriodicTimer> periodic_flush_timer_;

  std::mutex mutex_;

  std::shared_ptr<MultiRaftConsensusData> current_batch_ GUARDED_BY(mutex_);

  // Envelopes carrying data that are in flight to this host. Drives the immediate and
  // response-clocked data flushes under response clocking; under pacing it is only kept current,
  // for a switch back at runtime. Unlike running_calls_, which the manager shares across all
  // batchers to drain at shutdown, this is per destination and ignores heartbeat-only envelopes.
  size_t data_batches_in_flight_ GUARDED_BY(mutex_) = 0;

  rpc::ScheduledTaskId data_flush_deadline_task_ GUARDED_BY(mutex_) = rpc::kInvalidTaskId;

  // When the last batch carrying data to this host was prepared, whatever sent it; the start of
  // the current pacing window. min() means none was, and is tested with == before any arithmetic,
  // which would overflow. Updated under response clocking too, so a switch to pacing at runtime
  // starts from the real last send.
  std::chrono::steady_clock::time_point last_data_send_ GUARDED_BY(mutex_) =
      std::chrono::steady_clock::time_point::min();

  rpc::ScheduledTaskId pacing_task_ GUARDED_BY(mutex_) = rpc::kInvalidTaskId;

  bool shutdown_ GUARDED_BY(mutex_) = false;

  std::atomic<int>* running_calls_;
};

// MultiRaftManager is responsible for managing all MultiRaftBatchers
// of a given server (utilizes a mapping between a hostport and the corresponding batcher).
// MultiRaftManager allows multiple peers to share the same batcher
// if they are connected to the same remote host.
class MultiRaftManager: public std::enable_shared_from_this<MultiRaftManager> {
 public:
  MultiRaftManager(rpc::Messenger* messenger,
                   rpc::ProxyCache* proxy_cache,
                   CloudInfoPB local_peer_cloud_info_pb,
                   MetricEntityPtr metric_entity);

  ~MultiRaftManager();

  // Add a batcher with the given hostport (if one does not already exist)
  // and returns the newly created batcher.
  MultiRaftBatcherPtr AddOrGetBatcher(const RaftPeerPB& remote_peer_pb);

  void StartShutdown();
  void CompleteShutdown();

 private:
  rpc::Messenger* messenger_;

  rpc::ProxyCache* proxy_cache_;

  CloudInfoPB local_peer_cloud_info_pb_;

  MetricEntityPtr metric_entity_;

  std::mutex mutex_;

  // Uses a weak_ptr value in the map to allow for deallocation of unneeded batchers
  // since each consensus peer will own a shared_ptr to the batcher (performance optimization).
  // MultiRaftBatchers will be shared for the same remote peer info.
  std::unordered_map<HostPort, std::weak_ptr<MultiRaftBatcher>,
                     HostPortHash> batchers_ GUARDED_BY(mutex_);

  bool shutdown_ = false;
  std::atomic<int> running_calls_{0};
};

}   // namespace consensus
}   // namespace yb
