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
//

#pragma once

#include <array>
#include <atomic>
#include <deque>
#include <string>

#include "yb/gutil/ref_counted.h"

#include "yb/rpc/rpc_fwd.h"
#include "yb/rpc/thread_pool.h"

#include "yb/util/enums.h"
#include "yb/util/locks.h"
#include "yb/util/metrics_fwd.h"

namespace yb::rpc {

// RpcPriorityQueue gates admission of RPC worker-pool work behind a single global permit budget
// and orders waiting work by RpcPriority.
//
// All RPC work that would otherwise be submitted directly to a worker YBThreadPool (inbound
// service calls, async outbound-call callbacks) is instead submitted here together with the pool
// it should execute on. While fewer than max_dispatched tasks are in flight and nothing is
// waiting, tasks are forwarded to their target pool immediately (lock-free fast path). Once the
// budget is exhausted, tasks wait in per-priority FIFO bands and are released strictly in
// kHigh -> kNormal -> kLow order as in-flight tasks complete.
//
// Because the budget is shared across every target pool, it is the operative node-level bound on
// concurrently executing RPC work; the individual pools' worker limits only act as upper bounds.
//
// Invariant: max_dispatched must not exceed any target pool's max_workers. This guarantees that
// every task the queue dispatches has a worker available, so "dispatched" means "executing" for
// the queue's own work and permits are released at the rate work actually completes. Without it,
// tasks could pile up inside one pool's internal (non-priority) queue while holding permits that a
// higher-priority task for an idle pool is waiting for. Enqueue enforces this with a DCHECK. Note
// that work submitted to a target pool directly, bypassing this queue (e.g. TabletPeer::Enqueue),
// can still occupy that pool's workers and delay the queue's dispatched tasks; that exposure is
// bounded by the pool's worker count and is unchanged from pre-queue behavior.
//
// Scheduling policy (v1) is strict priority with no preemption: a permit held by a running task is
// released only when that task finishes, and a waiting lower-priority task runs only when no
// higher-priority task is waiting. Consequences and intended follow-ups are documented at
// PopHighestPriorityLocked().
//
// Thread safety: all public methods may be called from any thread.
class RpcPriorityQueue {
 public:
  // max_dispatched: permit budget, i.e. the maximum number of tasks concurrently handed to target
  // pools. Must be > 0.
  // metric_entity may be null (no metrics).
  RpcPriorityQueue(
      std::string name, size_t max_dispatched, const scoped_refptr<MetricEntity>& metric_entity);
  ~RpcPriorityQueue();

  RpcPriorityQueue(const RpcPriorityQueue&) = delete;
  void operator=(const RpcPriorityQueue&) = delete;

  // Submits task for execution on target_pool, subject to the permit budget and priority ordering.
  // Ownership follows the YBThreadPool convention: the queue guarantees that exactly one of
  // task->Run() (followed by task->Done(OK)) or task->Done(!OK) is eventually invoked.
  // Returns false only when the queue is shutting down; in that case task->Done(aborted status)
  // has already been called.
  // target_pool->options().max_workers must be >= max_dispatched() (see class comment).
  bool Enqueue(ThreadPoolTask* task, RpcPriority priority, const ThreadPoolPtr& target_pool);

  // Two-phase shutdown. StartShutdown atomically closes admission (no permit can be claimed after
  // it returns, even by an Enqueue that was already in progress) and fails every waiting task with
  // an Aborted status (the same status YBThreadPool uses when it shuts down with queued tasks, so
  // downstream handling is identical). CompleteShutdown blocks until all previously dispatched
  // tasks have finished and the queue is no longer referenced by any completion in progress, after
  // which it is safe to destroy the queue. Both are idempotent.
  void StartShutdown();
  void CompleteShutdown();

  const std::string& name() const { return name_; }
  size_t max_dispatched() const { return max_dispatched_; }

  // Point-in-time counters, primarily for tests and introspection.
  size_t TEST_dispatched() const;
  size_t TEST_queued() const;
  size_t TEST_queued(RpcPriority priority) const;

 private:
  class Task;
  friend class Task;

  // dispatched, queued and closed are packed into one atomic word so that admission can observe
  // and update all of them with a single compare-exchange. This is what makes shutdown safe
  // against a concurrent Enqueue: once the closed bit is set, no permit-claiming CAS can succeed,
  // so an enqueue that started before StartShutdown cannot slip a task through afterwards. It also
  // lets a completion atomically decide between "release permit" and "hand permit to a waiting
  // task".
  struct State {
    uint32_t dispatched = 0;  // 31 bits used.
    uint32_t queued = 0;
    bool closed = false;
  };
  static constexpr uint64_t kClosedBit = 1ULL << 63;
  static constexpr uint32_t kMaxDispatchedLimit = 0x7fffffff;
  static uint64_t Pack(State state);
  static State Unpack(uint64_t value);
  State LoadState() const;

  // Called by Task when the underlying task has finished (successfully or not) to return the
  // permit or hand it to the highest-priority waiting task. Completions are processed by a single
  // drainer at a time: the caller that transitions pending_completions_ from 0 becomes the drainer
  // and loops until it has processed every completion registered meanwhile. A completion that
  // happens synchronously inside a dispatch (target pool refusing the task) therefore only bumps
  // the counter and returns, instead of nesting another dispatch on the same stack.
  void TaskFinished();
  void ProcessOneCompletion();

  // Lock-free attempt to claim a permit while nothing is waiting and the queue is open. Returns
  // true if claimed.
  bool TryClaimPermitFastPath();

  // Slow-path admission: in a single CAS either claims a permit (if one is free and nothing waits)
  // or increments queued. REQUIRES(mutex_) so that the subsequent band push is ordered against the
  // consumers of the bands.
  enum class AdmitOutcome { kClaimed, kQueued, kClosed };
  AdmitOutcome ClaimOrRegisterQueued() REQUIRES(mutex_);

  // Pops the highest-priority waiting task and decrements queued. Returns null if nothing waits.
  Task* PopHighestPriorityLocked() REQUIRES(mutex_);

  void RecordQueueTime(Task* task);
  void OnQueued(RpcPriority priority);
  void OnDequeued(RpcPriority priority);

  const std::string name_;
  const size_t max_dispatched_;

  std::atomic<uint64_t> state_{0};

  // Number of completions registered but not yet processed by the drain loop in TaskFinished.
  // Non-zero also means some thread is still inside TaskFinished and may touch this object.
  std::atomic<uint32_t> pending_completions_{0};

  // Guards bands_. Adaptive lock (bounded spin, then futex): the critical sections here are a
  // pointer push/pop plus a counter update with no allocation, callbacks, or IO, so the spin phase
  // is cheaper than parking. The lock is only taken when work is actually waiting (queued > 0);
  // the unsaturated path never touches it.
  mutable simple_spinlock mutex_;
  std::array<std::deque<Task*>, kRpcPriorityMapSize> bands_ GUARDED_BY(mutex_);

  // Metrics. Per-band entries are indexed by std::to_underlying(RpcPriority).
  std::array<scoped_refptr<AtomicGauge<int64_t>>, kRpcPriorityMapSize> queued_gauges_;
  std::array<scoped_refptr<Counter>, kRpcPriorityMapSize> dispatched_counters_;
  std::array<scoped_refptr<EventStats>, kRpcPriorityMapSize> queue_time_stats_;
  scoped_refptr<Counter> aborted_counter_;
};

} // namespace yb::rpc
