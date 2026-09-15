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
#include "yb/util/thread.h"

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
// higher-priority task for an idle pool is waiting for. Enqueue enforces this with a DCHECK.
//
// What is not gated (v1). The queue sees exactly two kinds of work: inbound service calls
// (ServicePoolImpl::Process) and async outbound-call callbacks (OutboundCall::InvokeCallback via
// the messenger's callback recipients). Work reaching RPC worker pools by any other route bypasses
// the queue. It shares pool workers with dispatched tasks, so the pool-size invariant above bounds
// but does not eliminate in-pool waiting for the queue's own work; the exposure is limited by the
// pool's worker count and is unchanged from pre-queue behavior. The routes are:
//  - Inline local calls: a call whose handler is in the same process and that arrives on a thread
//    the target pool already owns runs inline (ServicePoolImpl::Process fast path). The worker is
//    already occupied by a task (normally one dispatched through the queue), so no additional
//    concurrency is created and no permit is needed.
//  - Direct submission to Messenger::ThreadPool(): continuations of already-admitted work, e.g.
//    wait-queue and object-lock waiter resumption (docdb), PgClientService session cleanup and
//    table-query continuations, CQL processor rescheduling, xCluster safe-time service. Bounded,
//    kNormal-equivalent, and a candidate for routing through the queue via a Messenger-level
//    helper in a follow-up.
//  - TabletPeer::Enqueue: already-admitted per-tablet work continuing on the tablet's tagged
//    pool. Same follow-up as above.
//  - The PgClientService shared-memory exchange pool (one dedicated thread per PG session,
//    unbounded, no queue): a separate request path that does not use the RPC worker pools.
//  - Callbacks run on the reactor (InvokeCallbackMode::kReactorThread, sync-RPC latches) and
//    non-RPC pools (raft, prepare, apply): never on RPC worker pools.
//
// Task classes and deadlock freedom: an inbound handler may block waiting for the callback of an
// outbound call it made (synchronous YBClient calls from inside handlers do exactly this), and
// that callback needs a permit from this same queue. If inbound handlers could hold every permit,
// enough simultaneously blocked handlers would stall the node permanently: the callbacks that
// would unblock them could never be dispatched. Two mechanisms address this:
//  - Handler -> callback chains: impossible by construction. Inbound handlers may hold at most
//    max_dispatched_inbound permits (max_dispatched minus a reserve), while callbacks may use the
//    whole budget, so a callback can always obtain a permit no matter how many handlers block.
//  - Callback -> callback chains: a finite reserve cannot break these, so callbacks must not
//    block. That is a contract, enforced in debug builds whenever the queue is enabled
//    (FLAGS_rpc_priority_queue_check_callbacks_do_not_wait): a callback that waits on a latch,
//    condition variable or sleep crashes the process. Note that at the default budget
//    (rpc_workers_limit) the callback -> callback exposure is identical to the pre-queue thread
//    pool, whose worker count bounded blocked callbacks the same way; the reserve only makes
//    small-budget configurations more sensitive to a violated contract.
//
// Scheduling policy (v1) is strict priority with no preemption: a permit held by a running task is
// released only when that task finishes, and a waiting lower-priority task runs only when no
// higher-priority task is waiting. Consequences and intended follow-ups are documented at
// PopEligibleLocked().
//
// Thread safety: all public methods may be called from any thread.
class RpcPriorityQueue {
 public:
  // max_dispatched: permit budget, i.e. the maximum number of tasks concurrently handed to target
  // pools. Must be > 0.
  // callback_reserve: permits that inbound handlers may never occupy, so they stay available to
  // callbacks. The inbound cap is max(1, max_dispatched - callback_reserve); with 0 there is no
  // reserve (and no deadlock protection).
  // metric_entity may be null (no metrics).
  RpcPriorityQueue(
      std::string name, size_t max_dispatched, size_t callback_reserve,
      const scoped_refptr<MetricEntity>& metric_entity);
  ~RpcPriorityQueue();

  RpcPriorityQueue(const RpcPriorityQueue&) = delete;
  void operator=(const RpcPriorityQueue&) = delete;

  // Submits task for execution on target_pool, subject to the permit budget, the inbound cap for
  // kInbound tasks, and priority ordering.
  // Ownership follows the YBThreadPool convention: the queue guarantees that exactly one of
  // task->Run() (followed by task->Done(OK)) or task->Done(!OK) is eventually invoked.
  // Returns false when the queue itself rejects admission; task->Done() has then already been
  // invoked before Enqueue returns, with:
  //   - Aborted: the queue is shutting down;
  //   - ServiceUnavailable: max_queued() tasks are already waiting (see the counter bound).
  // Returns true when the queue accepted the task. The target pool may still fail it, and does so
  // synchronously (Done(Aborted) before Enqueue returns) if the pool is closing.
  // Task code (Run() or Done(), on any of these paths) may use the queue and may call
  // StartShutdown on it; it must not call CompleteShutdown synchronously, see below.
  // target_pool->options().max_workers must be >= max_dispatched() (see class comment).
  bool Enqueue(
      ThreadPoolTask* task, RpcPriority priority, RpcTaskClass task_class,
      const ThreadPoolPtr& target_pool);

  // Two-phase shutdown.
  // StartShutdown atomically closes admission (no permit can be claimed after it returns, even by
  // an Enqueue that was already in progress) and fails every waiting task with an Aborted status
  // (the same status YBThreadPool uses when it shuts down with queued tasks, so downstream handling
  // is identical). Concurrent callers all return only once that drain has completed.
  // CompleteShutdown blocks until the queue is quiescent: the drain has completed, no thread is
  // inside Enqueue, every dispatched task has finished, and no completion is being processed.
  // After it returns it is safe to destroy the queue. Both are idempotent.
  // Task code may call StartShutdown (from Run(), from Done(), including tasks failed by the drain
  // or refused by the pool). It must not call CompleteShutdown synchronously: a dispatched task
  // holds a permit until its Done() returns, and a task being drained is what the drain is still
  // running, so quiescence is unreachable from either and waiting would deadlock. Such a call is
  // reported as a DFATAL and ignored; defer completion (and destruction) to another thread. This
  // is the same constraint YBThreadPool has for a task calling Shutdown() on its own pool.
  void StartShutdown();
  void CompleteShutdown();

  const std::string& name() const { return name_; }
  size_t max_dispatched() const { return max_dispatched_; }
  size_t max_dispatched_inbound() const { return max_dispatched_inbound_; }
  // Bound on the number of waiting tasks. Enqueue fails with ServiceUnavailable beyond it.
  static constexpr size_t max_queued() { return kFieldMask; }

  // Point-in-time counters, primarily for tests and introspection.
  size_t TEST_dispatched() const;
  size_t TEST_dispatched_inbound() const;
  size_t TEST_queued() const;
  size_t TEST_queued(RpcPriority priority) const;
  // Overwrites the queued counter (bands are untouched). Only for exercising the counter's bound;
  // the caller must restore it to the real band size before shutdown.
  void TEST_SetQueuedCount(uint32_t value);

 private:
  class Task;
  friend class Task;

  // All admission-relevant counters are packed into one atomic word so that admission can observe
  // and update all of them with a single compare-exchange. This is what makes shutdown safe
  // against a concurrent Enqueue: once the closed bit is set, no permit-claiming CAS can succeed,
  // so an enqueue that started before StartShutdown cannot slip a task through afterwards. It also
  // lets a completion atomically decide between "release permit" and "hand permit to a waiting
  // task".
  //
  // Layout (bit 63 -> 0): closed:1 | dispatched:21 | dispatched_inbound:21 | queued:21.
  struct State {
    uint32_t dispatched = 0;
    uint32_t dispatched_inbound = 0;
    uint32_t queued = 0;
    bool closed = false;
  };
  static constexpr int kFieldBits = 21;
  static constexpr uint32_t kFieldMask = (1u << kFieldBits) - 1;
  static constexpr uint64_t kClosedBit = 1ULL << 63;
  static constexpr uint32_t kMaxDispatchedLimit = kFieldMask;
  static uint64_t Pack(State state);
  static State Unpack(uint64_t value);
  State LoadState() const;

  // Applies mutate to the current state in a CAS loop. mutate returns false to abandon the update
  // (the state is then left unchanged and UpdateState returns false). All state_ transitions go
  // through here so that the packing and retry logic exists in exactly one place.
  template <class Mutate>
  bool UpdateState(Mutate&& mutate) {
    auto packed = state_.load(std::memory_order_acquire);
    for (;;) {
      auto state = Unpack(packed);
      if (!mutate(&state)) {
        return false;
      }
      if (state_.compare_exchange_weak(
              packed, Pack(state), std::memory_order_acq_rel, std::memory_order_acquire)) {
        return true;
      }
    }
  }

  // Whether a task of task_class may claim a permit right now, given state.
  bool CanClaim(const State& state, RpcTaskClass task_class) const;

  // Called by Task when the underlying task has finished (successfully or not) to return the
  // permit or hand it to the highest-priority waiting task. Completions are processed by a single
  // drainer at a time: the caller that transitions pending_completions_ from 0 becomes the drainer
  // and loops until it has processed every completion registered meanwhile. A completion that
  // happens synchronously inside a dispatch (target pool refusing the task) therefore only bumps
  // the counters and returns, instead of nesting another dispatch on the same stack.
  void TaskFinished(RpcTaskClass task_class);
  void ProcessOneCompletion(RpcTaskClass finished_class);

  // Lock-free attempt to claim a permit for a task of task_class. Returns true if claimed.
  bool TryClaimPermitFastPath(RpcTaskClass task_class);

  // Slow-path admission: in a single CAS either claims a permit or increments queued. kOverloaded
  // means the queued counter is at its bound and the task must be rejected rather than counted.
  // REQUIRES(mutex_) so that the subsequent band push is ordered against the consumers of the
  // bands.
  enum class AdmitOutcome { kClaimed, kQueued, kClosed, kOverloaded };
  AdmitOutcome ClaimOrRegisterQueued(RpcTaskClass task_class) REQUIRES(mutex_);

  // Pops the highest-priority waiting task that may run given inbound_eligible (whether an inbound
  // task may currently take a permit). Returns null if nothing eligible waits. Does not touch
  // state_; the caller applies the corresponding queued/dispatched_inbound changes.
  Task* PopEligibleLocked(bool inbound_eligible) REQUIRES(mutex_);

  void RecordQueueTime(Task* task);
  void OnQueued(RpcPriority priority);
  void OnDequeued(RpcPriority priority);

  const std::string name_;
  const size_t max_dispatched_;
  const size_t max_dispatched_inbound_;

  std::atomic<uint64_t> state_{0};

  // Threads currently inside Enqueue. CompleteShutdown waits for this to reach zero so that an
  // Enqueue that read the state before closure cannot touch the queue after it is destroyed (the
  // same role YBThreadPool's adding_ counter plays).
  std::atomic<size_t> active_enqueues_{0};

  // Set once StartShutdown has drained the bands. Concurrent StartShutdown callers and
  // CompleteShutdown wait for it, so neither can return while the drain is still failing tasks.
  std::atomic<bool> drain_complete_{false};
  // Thread performing the drain, to recognize re-entrant shutdown calls from tasks it is failing.
  std::atomic<ThreadIdForStack> draining_thread_{0};
  bool IsDrainingThread() const;
  // Whether the calling thread is inside Run()/Done() of one of this queue's tasks (dispatched or
  // being drained), from where CompleteShutdown cannot be satisfied.
  bool CalledFromOwnTask() const;

  // Completions registered but not yet processed by the drain loop in TaskFinished, per class,
  // plus the total which also serves as the drainer election counter. A class counter is always
  // incremented before the total, so when the drainer observes total > 0 some class counter is
  // > 0 as well. Non-zero total also means some thread is still inside TaskFinished and may touch
  // this object.
  std::array<std::atomic<uint32_t>, kRpcTaskClassMapSize> pending_completions_by_class_{};
  std::atomic<uint32_t> pending_completions_{0};

  // Guards bands_. Adaptive lock (bounded spin, then futex): the critical sections here are a
  // pointer push/pop plus a counter update with no allocation, callbacks, or IO, so the spin phase
  // is cheaper than parking. The lock is only taken when work is actually waiting (queued > 0);
  // the unsaturated path never touches it.
  mutable simple_spinlock mutex_;
  // bands_[priority][class]. Two FIFOs per band so that a callback can be dispatched past inbound
  // tasks of the same priority that are blocked by the inbound cap.
  using Band = std::array<std::deque<Task*>, kRpcTaskClassMapSize>;
  std::array<Band, kRpcPriorityMapSize> bands_ GUARDED_BY(mutex_);

  // Metrics. Per-band entries are indexed by std::to_underlying(RpcPriority).
  std::array<scoped_refptr<AtomicGauge<int64_t>>, kRpcPriorityMapSize> queued_gauges_;
  std::array<scoped_refptr<Counter>, kRpcPriorityMapSize> dispatched_counters_;
  std::array<scoped_refptr<EventStats>, kRpcPriorityMapSize> queue_time_stats_;
  scoped_refptr<Counter> dispatched_callbacks_counter_;
  scoped_refptr<Counter> aborted_counter_;
  scoped_refptr<Counter> rejected_full_counter_;
};

// Adapter that submits every task it receives to a queue at a fixed priority and target pool, as
// kCallback work. Lets outbound-call callbacks be routed through the queue wherever a plain
// YBThreadPool was previously used as the callback recipient.
class PriorityQueueCallbackRecipient : public ThreadPoolTaskRecipient {
 public:
  PriorityQueueCallbackRecipient(RpcPriorityQueue* queue, RpcPriority priority, ThreadPoolPtr pool)
      : queue_(queue), priority_(priority), pool_(std::move(pool)) {}

  bool Enqueue(ThreadPoolTask* task) override {
    return queue_->Enqueue(task, priority_, RpcTaskClass::kCallback, pool_);
  }

 private:
  RpcPriorityQueue* const queue_;
  const RpcPriority priority_;
  const ThreadPoolPtr pool_;
};

} // namespace yb::rpc
