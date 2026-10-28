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

  // Must invoke done exactly once. req, resp and controller stay valid until then.
  virtual void SendBatch(
      const MultiRaftConsensusRequestPB& req, MultiRaftConsensusResponsePB* resp,
      rpc::RpcController* controller, DoneCallback done) = 0;

  // Fails a batch RPC that SendBatch issued on controller and that has not completed, so that
  // its done callback runs with an error.
  virtual void MarkCallAsFailed(rpc::RpcController* controller) {
    controller->MarkCallAsFailed();
  }
};

std::unique_ptr<MultiRaftBatchSender> CreateMultiRaftBatchSender(
    rpc::ProxyCache* proxy_cache, const HostPort& hostport);

// - MultiRaftBatcher batches ConsensusRequestPBs from the local peers of different tablets that
//   replicate to the same remote tserver, and sends each batch as one MultiRaftUpdateConsensus RPC.
// - Heartbeats (RequestKind::kHeartbeat) are sent every FLAGS_multi_raft_heartbeat_interval_ms
//   ms, or once the batch reaches FLAGS_multi_raft_batch_size requests.
// - Data requests (RequestKind::kData) are sent immediately when no batch carrying data to this
//   server is in flight. Otherwise they accumulate until such a batch completes or
//   FLAGS_multi_raft_data_flush_delay_ms elapses, whichever is first. Pending heartbeats ride
//   along. The heartbeat timer remains a backstop.
// - A request that would push the batch past FLAGS_multi_raft_data_batch_size data requests or
//   FLAGS_multi_raft_batch_bytes bytes sends the current batch first.
// - Multiple batches may be in flight concurrently, but only one is built at a time.
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

    // Fails the whole batch RPC through MultiRaftBatchSender::MarkCallAsFailed, unless it has
    // finished.
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
  using BatchResponseCallback = std::function<void(const Status&)>;

  // When called adds the request to a batch (request data will be swapped).
  // If the batch executes sucessfully then the response is populated and the callback is executed.
  // If the batch rpc call fails the response will NOT be populated and the callback will be
  // executed with an error status. After Shutdown() the callback runs immediately with Aborted.
  // Returns a handle to the batch the request was added to.
  BatchHandle AddRequestToBatch(ConsensusRequestPB* request,
                                ConsensusResponsePB* response,
                                BatchResponseCallback callback,
                                RequestKind kind);

  // Counts a data request that bypassed the batcher.
  void RecordDirectDataSend();

  void Shutdown();

 private:
  struct Metrics;
  enum class FlushReason;
  class SubCallbackTask;

  // Adds the request, and returns in pre_add_batch / post_add_batch any batches to send.
  BatchHandle AddRequestUnlocked(
      ConsensusRequestPB* request, ConsensusResponsePB* response, BatchResponseCallback callback,
      RequestKind kind, size_t request_bytes, BatchData* pre_add_batch, BatchData* post_add_batch)
      REQUIRES(mutex_);

  void PrepareAndSendBatchRequest();

  // This method will return a nullptr if the current batch is empty.
  BatchData PrepareNextBatchRequest(FlushReason reason) REQUIRES(mutex_);

  // If data is null then we will not send a batch request.
  void SendBatchRequest(BatchData data);

  // Completes every request of a sent batch. Data requests are dispatched to data_pool when it
  // is set.
  static void DispatchSubCallbacks(
      const BatchData& data, Status status, rpc::ThreadPool* data_pool);
  static void InvokeSubCallback(const BatchData& data, size_t idx, const Status& status);

  void ArmDataFlushDeadline() REQUIRES(mutex_);
  void CancelDataFlushDeadline() REQUIRES(mutex_);
  void DataFlushDeadlineExpired(rpc::ScheduledTaskId task_id);

  // Bookkeeping for a completed envelope, plus the response-clocked flush.
  void BatchCompleted(const MultiRaftConsensusData& data);

  rpc::Messenger* const messenger_;

  // Shared with each sent batch, which uses it to fail its RPC.
  const std::shared_ptr<MultiRaftBatchSender> sender_;

  const std::unique_ptr<Metrics> metrics_;

  std::shared_ptr<rpc::PeriodicTimer> periodic_flush_timer_;

  std::mutex mutex_;

  std::shared_ptr<MultiRaftConsensusData> current_batch_ GUARDED_BY(mutex_);

  // Envelopes carrying data that are in flight to this host. Drives the immediate and
  // response-clocked data flushes. Unlike running_calls_, which the manager shares across all
  // batchers to drain at shutdown, this is per destination and ignores heartbeat-only envelopes.
  size_t data_batches_in_flight_ GUARDED_BY(mutex_) = 0;

  rpc::ScheduledTaskId data_flush_deadline_task_ GUARDED_BY(mutex_) = rpc::kInvalidTaskId;

  bool shutdown_ GUARDED_BY(mutex_) = false;

  std::atomic<int>* running_calls_;
};

// MultiRaftManager is responsible for managing all MultiRaftBatchers
// for a given tserver (utilizes a mapping between a hostport and the corresponding batcher).
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
