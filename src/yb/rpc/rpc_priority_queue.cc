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

#include "yb/rpc/rpc_priority_queue.h"

#include <chrono>
#include <thread>
#include <utility>

#include "yb/gutil/port.h"

#include "yb/util/flags.h"
#include "yb/util/logging.h"
#include "yb/util/metrics.h"
#include "yb/util/monotime.h"
#include "yb/util/status.h"

DEFINE_NON_RUNTIME_bool(rpc_priority_queue_enabled, false,
    "Route RPC worker-pool work (inbound service calls and async callbacks) through a single "
    "per-messenger RpcPriorityQueue that bounds the number of concurrently executing RPC tasks "
    "across all RPC worker pools and dispatches waiting work in priority order "
    "(cluster health > user-facing > background). When enabled, total RPC concurrency is bounded "
    "by rpc_priority_queue_max_dispatched rather than by each pool's worker limit independently.");
TAG_FLAG(rpc_priority_queue_enabled, advanced);

DEFINE_NON_RUNTIME_int32(rpc_priority_queue_max_dispatched, 0,
    "Maximum number of RPC tasks concurrently dispatched to worker pools by the RpcPriorityQueue "
    "(its permit budget). 0 means use rpc_workers_limit. Only used when "
    "rpc_priority_queue_enabled is true.");
TAG_FLAG(rpc_priority_queue_max_dispatched, advanced);

METRIC_DEFINE_gauge_int64(server, rpc_priority_queue_queued_high,
    "Number of kHigh RPC tasks waiting in the RPC priority queue.",
    yb::MetricUnit::kTasks,
    "Number of kHigh (cluster health) RPC tasks waiting for a dispatch permit.");
METRIC_DEFINE_gauge_int64(server, rpc_priority_queue_queued_normal,
    "Number of kNormal RPC tasks waiting in the RPC priority queue.",
    yb::MetricUnit::kTasks,
    "Number of kNormal (user-facing) RPC tasks waiting for a dispatch permit.");
METRIC_DEFINE_gauge_int64(server, rpc_priority_queue_queued_low,
    "Number of kLow RPC tasks waiting in the RPC priority queue.",
    yb::MetricUnit::kTasks,
    "Number of kLow (background) RPC tasks waiting for a dispatch permit.");

METRIC_DEFINE_counter(server, rpc_priority_queue_dispatched_high,
    "kHigh RPC tasks dispatched by the RPC priority queue.",
    yb::MetricUnit::kTasks,
    "Number of kHigh RPC tasks handed to a worker pool by the RPC priority queue.");
METRIC_DEFINE_counter(server, rpc_priority_queue_dispatched_normal,
    "kNormal RPC tasks dispatched by the RPC priority queue.",
    yb::MetricUnit::kTasks,
    "Number of kNormal RPC tasks handed to a worker pool by the RPC priority queue.");
METRIC_DEFINE_counter(server, rpc_priority_queue_dispatched_low,
    "kLow RPC tasks dispatched by the RPC priority queue.",
    yb::MetricUnit::kTasks,
    "Number of kLow RPC tasks handed to a worker pool by the RPC priority queue.");

METRIC_DEFINE_event_stats(server, rpc_priority_queue_wait_time_high,
    "Time kHigh RPC tasks waited in the RPC priority queue.",
    yb::MetricUnit::kMicroseconds,
    "Microseconds kHigh RPC tasks spent waiting for a dispatch permit. Only tasks that actually "
    "waited (i.e. did not take the fast path) are recorded.");
METRIC_DEFINE_event_stats(server, rpc_priority_queue_wait_time_normal,
    "Time kNormal RPC tasks waited in the RPC priority queue.",
    yb::MetricUnit::kMicroseconds,
    "Microseconds kNormal RPC tasks spent waiting for a dispatch permit. Only tasks that actually "
    "waited (i.e. did not take the fast path) are recorded.");
METRIC_DEFINE_event_stats(server, rpc_priority_queue_wait_time_low,
    "Time kLow RPC tasks waited in the RPC priority queue.",
    yb::MetricUnit::kMicroseconds,
    "Microseconds kLow RPC tasks spent waiting for a dispatch permit. Only tasks that actually "
    "waited (i.e. did not take the fast path) are recorded.");

METRIC_DEFINE_counter(server, rpc_priority_queue_aborted,
    "RPC tasks aborted by the RPC priority queue.",
    yb::MetricUnit::kTasks,
    "Number of waiting RPC tasks failed by the RPC priority queue because it was shutting down.");

using namespace std::literals;

namespace yb::rpc {

namespace {

const auto kShuttingDownStatus = STATUS(Aborted, "Service is shutting down");

size_t Index(RpcPriority priority) {
  return std::to_underlying(priority);
}

} // namespace

// Wraps a caller-provided task so the queue observes its completion. The pool contract guarantees
// Done() is invoked exactly once on every path (after Run(), or directly on failure to run), so the
// permit release in Done() cannot leak. Heap-allocated per submission; the fast path pays one
// allocation, which is comparable to what YBThreadPool's own functor tasks cost.
// TODO(rpc-priority-queue): consider a freelist or embedding the wrapper in InboundCall /
// OutboundCall if profiling shows this allocation matters.
class RpcPriorityQueue::Task : public ThreadPoolTask {
 public:
  Task(RpcPriorityQueue* queue, ThreadPoolTask* inner, RpcPriority priority, ThreadPoolPtr pool)
      : queue_(queue), inner_(inner), priority_(priority), pool_(std::move(pool)) {}

  RpcPriority priority() const { return priority_; }
  const ThreadPoolPtr& pool() const { return pool_; }

  // Records that this task is about to wait in a band, so its wait time can be measured.
  void MarkQueued() {
    queued_at_ = MonoTime::Now();
  }

  bool waited() const {
    return queued_at_.Initialized();
  }

  MonoTime queued_at() const {
    return queued_at_;
  }

  // Forwards the wrapped task to its target pool. If the pool refuses (it is shutting down), the
  // pool has already invoked our Done(), which releases the permit; nothing more to do here.
  void Dispatch() {
    pool_->Enqueue(this);
  }

  // Fails the wrapped task without ever dispatching it. Used when the queue shuts down with
  // waiting work. No permit is held by a waiting task, so none is released.
  void Abort(const Status& status) {
    inner_->Done(status);
    delete this;
  }

 private:
  void Run() override {
    inner_->Run();
  }

  void Done(const Status& status) override {
    inner_->Done(status);
    // inner_ may have deleted itself in Done(); do not touch it afterwards.
    queue_->TaskFinished();
    delete this;
  }

  RpcPriorityQueue* const queue_;
  ThreadPoolTask* const inner_;
  const RpcPriority priority_;
  const ThreadPoolPtr pool_;
  MonoTime queued_at_;
};

RpcPriorityQueue::RpcPriorityQueue(
    std::string name, size_t max_dispatched, const scoped_refptr<MetricEntity>& metric_entity)
    : name_(std::move(name)), max_dispatched_(max_dispatched) {
  CHECK_GT(max_dispatched_, 0) << name_ << ": permit budget must be positive";
  CHECK_LE(max_dispatched_, kMaxDispatchedLimit)
      << name_ << ": permit budget does not fit in the packed state word";
  if (metric_entity) {
    queued_gauges_[Index(RpcPriority::kHigh)] =
        METRIC_rpc_priority_queue_queued_high.Instantiate(metric_entity, 0);
    queued_gauges_[Index(RpcPriority::kNormal)] =
        METRIC_rpc_priority_queue_queued_normal.Instantiate(metric_entity, 0);
    queued_gauges_[Index(RpcPriority::kLow)] =
        METRIC_rpc_priority_queue_queued_low.Instantiate(metric_entity, 0);
    dispatched_counters_[Index(RpcPriority::kHigh)] =
        METRIC_rpc_priority_queue_dispatched_high.Instantiate(metric_entity);
    dispatched_counters_[Index(RpcPriority::kNormal)] =
        METRIC_rpc_priority_queue_dispatched_normal.Instantiate(metric_entity);
    dispatched_counters_[Index(RpcPriority::kLow)] =
        METRIC_rpc_priority_queue_dispatched_low.Instantiate(metric_entity);
    queue_time_stats_[Index(RpcPriority::kHigh)] =
        METRIC_rpc_priority_queue_wait_time_high.Instantiate(metric_entity);
    queue_time_stats_[Index(RpcPriority::kNormal)] =
        METRIC_rpc_priority_queue_wait_time_normal.Instantiate(metric_entity);
    queue_time_stats_[Index(RpcPriority::kLow)] =
        METRIC_rpc_priority_queue_wait_time_low.Instantiate(metric_entity);
    aborted_counter_ = METRIC_rpc_priority_queue_aborted.Instantiate(metric_entity);
  }
  LOG(INFO) << "RpcPriorityQueue " << name_ << " created with permit budget " << max_dispatched_;
}

RpcPriorityQueue::~RpcPriorityQueue() {
  StartShutdown();
  CompleteShutdown();
}

uint64_t RpcPriorityQueue::Pack(State state) {
  DCHECK_LE(state.dispatched, kMaxDispatchedLimit);
  return (state.closed ? kClosedBit : 0) |
         (static_cast<uint64_t>(state.dispatched) << 32) |
         state.queued;
}

RpcPriorityQueue::State RpcPriorityQueue::Unpack(uint64_t value) {
  return State{
    .dispatched = static_cast<uint32_t>((value >> 32) & kMaxDispatchedLimit),
    .queued = static_cast<uint32_t>(value & 0xffffffffULL),
    .closed = (value & kClosedBit) != 0,
  };
}

RpcPriorityQueue::State RpcPriorityQueue::LoadState() const {
  return Unpack(state_.load(std::memory_order_acquire));
}

bool RpcPriorityQueue::TryClaimPermitFastPath() {
  auto packed = state_.load(std::memory_order_acquire);
  for (;;) {
    auto state = Unpack(packed);
    // The queued == 0 check is what preserves priority: once anything is waiting, a new arrival
    // must not overtake it, so it goes through the slow path even if a permit happens to be free
    // at this instant (a completion is about to hand that permit to the waiting task).
    // The closed check in the same CAS is what makes shutdown safe against concurrent enqueues.
    if (state.closed || state.queued != 0 || state.dispatched >= max_dispatched_) {
      return false;
    }
    ++state.dispatched;
    if (state_.compare_exchange_weak(
            packed, Pack(state), std::memory_order_acq_rel, std::memory_order_acquire)) {
      return true;
    }
  }
}

bool RpcPriorityQueue::Enqueue(
    ThreadPoolTask* inner, RpcPriority priority, const ThreadPoolPtr& target_pool) {
  DCHECK_ONLY_NOTNULL(inner);
  DCHECK_ONLY_NOTNULL(target_pool.get());

  // See the class comment: a pool smaller than the budget lets dispatched tasks pile up inside it
  // while holding permits. This is a configuration error, not a runtime condition.
  if (PREDICT_FALSE(target_pool->options().max_workers < max_dispatched_)) {
    YB_LOG_EVERY_N_SECS(DFATAL, 60)
        << "RpcPriorityQueue " << name_ << " permit budget " << max_dispatched_
        << " exceeds max_workers " << target_pool->options().max_workers << " of target pool "
        << target_pool->options().name << "; dispatched tasks may wait inside the pool";
  }

  // Cheap early reject to avoid allocating for a closed queue. Not sufficient on its own: the
  // authoritative check is the closed bit inside the permit CAS and the re-check under the lock.
  if (LoadState().closed) {
    inner->Done(kShuttingDownStatus);
    return false;
  }

  auto* task = new Task(this, inner, priority, target_pool);

  // Fast path: nothing waiting and a permit available. No lock, no band traffic.
  if (TryClaimPermitFastPath()) {
    IncrementCounter(dispatched_counters_[Index(priority)]);
    task->Dispatch();
    return true;
  }

  // Slow path: wait in the band for this priority. Dispatch and abort happen after the lock is
  // released: both may synchronously invoke Done() (pool refusing the task, or inner task
  // cleanup), which must not run under mutex_.
  auto outcome = AdmitOutcome::kQueued;
  {
    std::lock_guard lock(mutex_);
    outcome = ClaimOrRegisterQueued();
    if (outcome == AdmitOutcome::kQueued) {
      // The band push happens after queued was incremented, which is safe because the only
      // consumers of the bands (PopHighestPriorityLocked, StartShutdown's drain) run under this
      // same lock and therefore cannot observe the incremented count before the push.
      task->MarkQueued();
      bands_[Index(priority)].push_back(task);
      OnQueued(priority);
    }
  }
  switch (outcome) {
    case AdmitOutcome::kQueued:
      return true;
    case AdmitOutcome::kClaimed:
      IncrementCounter(dispatched_counters_[Index(priority)]);
      task->Dispatch();
      return true;
    case AdmitOutcome::kClosed:
      // Lost a race with StartShutdown, which drains the bands without seeing this task; fail it
      // directly rather than leaving it stranded.
      task->Abort(kShuttingDownStatus);
      return false;
  }
  LOG(FATAL) << "Unexpected AdmitOutcome: " << std::to_underlying(outcome);
}

RpcPriorityQueue::AdmitOutcome RpcPriorityQueue::ClaimOrRegisterQueued() {
  // Deciding between "claim a permit" and "register as waiting" must be one CAS on the state word.
  // If they were separate steps, a completion's release (queued == 0 -> dispatched--) could land
  // between them, leaving dispatched below the budget with a task registered as waiting and no
  // completion left to hand it a permit: a permanent stall. Because the release CAS also operates
  // on the whole word, the two are linearizable: either we register first and the release retries
  // and hands off to us, or the release happens first and we observe the free permit and claim it.
  auto packed = state_.load(std::memory_order_acquire);
  for (;;) {
    auto state = Unpack(packed);
    if (state.closed) {
      return AdmitOutcome::kClosed;
    }
    AdmitOutcome outcome;
    if (state.queued == 0 && state.dispatched < max_dispatched_) {
      ++state.dispatched;
      outcome = AdmitOutcome::kClaimed;
    } else {
      ++state.queued;
      outcome = AdmitOutcome::kQueued;
    }
    if (state_.compare_exchange_weak(
            packed, Pack(state), std::memory_order_acq_rel, std::memory_order_acquire)) {
      return outcome;
    }
  }
}

RpcPriorityQueue::Task* RpcPriorityQueue::PopHighestPriorityLocked() {
  // Strict priority: the first non-empty band, kHigh -> kNormal -> kLow, always wins. Under
  // sustained kNormal load this can starve kLow indefinitely; waiting kLow tasks eventually fail
  // via their client deadline (ServicePool's early timeout check) and are retried by the caller.
  // That is the intended v1 behavior: during overload it is preferable to fail background work.
  //
  // TODO(rpc-priority-queue): policy extensions considered and deferred, in likely order of value:
  //  - Aging: promote the head of a band to the next band once it has waited longer than a
  //    threshold (e.g. a flag, well below max_time_in_queue_ms), bounding worst-case wait for
  //    lower priorities. Task::queued_at() already provides the signal.
  //  - Reserved permits for kHigh so cluster-health work never waits for a running kNormal/kLow
  //    task to complete (strict priority is non-preemptive).
  //  - A kLow concurrency cap so background work cannot occupy the whole budget on an otherwise
  //    idle node, leaving headroom for a burst of user-facing work.
  //  - Per-pool-tag (per-database) sub-queues inside the kNormal band with round-robin dispatch,
  //    for gate-level fairness between databases.
  for (auto& band : bands_) {
    if (!band.empty()) {
      auto* task = band.front();
      band.pop_front();
      state_.fetch_sub(1, std::memory_order_acq_rel);
      return task;
    }
  }
  return nullptr;
}

void RpcPriorityQueue::TaskFinished() {
  // Single-drainer protocol. If another thread is already draining, registering the completion is
  // all we need to do; that thread will process it before it leaves. This bounds stack depth when
  // a dispatch fails synchronously (the target pool refusing the task invokes Done() inline, which
  // lands here while the drainer is still inside next->Dispatch()).
  if (pending_completions_.fetch_add(1, std::memory_order_acq_rel) != 0) {
    return;
  }
  do {
    ProcessOneCompletion();
    // The decrement below is the last access to this object by the drainer; CompleteShutdown waits
    // for pending_completions_ to reach zero for that reason.
  } while (pending_completions_.fetch_sub(1, std::memory_order_acq_rel) != 1);
}

void RpcPriorityQueue::ProcessOneCompletion() {
  for (;;) {
    // Fast path: nothing waiting, so simply release the permit.
    auto packed = state_.load(std::memory_order_acquire);
    bool released = false;
    for (;;) {
      auto state = Unpack(packed);
      if (state.queued != 0) {
        break;
      }
      DCHECK_GT(state.dispatched, 0) << name_ << ": permit released that was never claimed";
      --state.dispatched;
      if (state_.compare_exchange_weak(
              packed, Pack(state), std::memory_order_acq_rel, std::memory_order_acquire)) {
        released = true;
        break;
      }
    }
    if (released) {
      return;
    }

    // Slow path: hand our permit directly to the highest-priority waiting task. dispatched is left
    // unchanged (the permit transfers rather than being released and re-claimed), which is what
    // prevents a concurrent fast-path arrival from stealing it out from under the waiting task.
    Task* next;
    {
      std::lock_guard lock(mutex_);
      next = PopHighestPriorityLocked();
    }
    if (next != nullptr) {
      OnDequeued(next->priority());
      RecordQueueTime(next);
      IncrementCounter(dispatched_counters_[Index(next->priority())]);
      next->Dispatch();
      return;
    }
    // Everything that was waiting drained (by StartShutdown) between our check and the lock. Loop:
    // queued is now 0 and the release branch will take the permit back.
  }
}

void RpcPriorityQueue::StartShutdown() {
  // Set the closed bit with a CAS so that it is ordered against every permit-claiming CAS: after
  // this succeeds no Enqueue can dispatch, whether it started before or after this call.
  auto packed = state_.load(std::memory_order_acquire);
  for (;;) {
    auto state = Unpack(packed);
    if (state.closed) {
      return;
    }
    state.closed = true;
    if (state_.compare_exchange_weak(
            packed, Pack(state), std::memory_order_acq_rel, std::memory_order_acquire)) {
      break;
    }
  }
  std::array<std::deque<Task*>, kRpcPriorityMapSize> drained;
  {
    std::lock_guard lock(mutex_);
    for (size_t i = 0; i < bands_.size(); ++i) {
      drained[i].swap(bands_[i]);
      state_.fetch_sub(drained[i].size(), std::memory_order_acq_rel);
    }
  }
  size_t aborted = 0;
  for (size_t i = 0; i < drained.size(); ++i) {
    for (auto* task : drained[i]) {
      OnDequeued(static_cast<RpcPriority>(i));
      task->Abort(kShuttingDownStatus);
      ++aborted;
    }
  }
  if (aborted != 0) {
    LOG(INFO) << "RpcPriorityQueue " << name_ << " aborted " << aborted << " waiting tasks";
    IncrementCounterBy(aborted_counter_, aborted);
  }
}

void RpcPriorityQueue::CompleteShutdown() {
  // Dispatched tasks belong to their target pools, which will finish or abort them and thereby
  // call TaskFinished. Shutdown is rare, so a simple wait suffices.
  //
  // dispatched must be read before pending_completions_: a completion increments
  // pending_completions_ before it decrements dispatched, so once dispatched is observed at 0
  // every completion has at least registered, and a subsequent pending_completions_ == 0 means the
  // drainer has also finished touching this object.
  auto last_log = CoarseMonoClock::Now();
  for (;;) {
    auto dispatched = LoadState().dispatched;
    if (dispatched == 0 && pending_completions_.load(std::memory_order_acquire) == 0) {
      return;
    }
    std::this_thread::sleep_for(1ms);
    auto now = CoarseMonoClock::Now();
    if (now - last_log >= 5s) {
      LOG(WARNING) << "RpcPriorityQueue " << name_ << " still waiting for " << dispatched
                   << " dispatched tasks to finish";
      last_log = now;
    }
  }
}

size_t RpcPriorityQueue::TEST_dispatched() const {
  return LoadState().dispatched;
}

size_t RpcPriorityQueue::TEST_queued() const {
  return LoadState().queued;
}

size_t RpcPriorityQueue::TEST_queued(RpcPriority priority) const {
  std::lock_guard lock(mutex_);
  return bands_[Index(priority)].size();
}

void RpcPriorityQueue::RecordQueueTime(Task* task) {
  if (!task->waited()) {
    return;
  }
  const auto& stats = queue_time_stats_[Index(task->priority())];
  if (stats) {
    stats->Increment((MonoTime::Now() - task->queued_at()).ToMicroseconds());
  }
}

void RpcPriorityQueue::OnQueued(RpcPriority priority) {
  IncrementGauge(queued_gauges_[Index(priority)]);
}

void RpcPriorityQueue::OnDequeued(RpcPriority priority) {
  DecrementGauge(queued_gauges_[Index(priority)]);
}

} // namespace yb::rpc
