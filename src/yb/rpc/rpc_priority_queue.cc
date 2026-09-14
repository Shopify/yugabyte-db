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
#include "yb/util/sync_point.h"
#include "yb/util/thread.h"
#include "yb/util/thread_restrictions.h"

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

DEFINE_NON_RUNTIME_int32(rpc_priority_queue_callback_reserve, -1,
    "Number of RpcPriorityQueue permits that inbound RPC handlers may never occupy, so that they "
    "remain available to outbound-call callbacks. An inbound handler can block waiting for such a "
    "callback; without a reserve, enough simultaneously blocked handlers would stall all RPC "
    "processing on the node. -1 means max(2, 5% of the permit budget). 0 disables the reserve. "
    "Only used when rpc_priority_queue_enabled is true.");
TAG_FLAG(rpc_priority_queue_callback_reserve, advanced);

DEFINE_RUNTIME_bool(rpc_priority_queue_check_callbacks_do_not_wait, true,
    "In debug builds, crash if a callback task dispatched by the RpcPriorityQueue blocks on a "
    "latch, condition variable or sleep. Callbacks that block on other callbacks could exhaust the "
    "permits reserved for callbacks and stall the queue, so non-blocking callbacks are a contract "
    "the queue relies on; this check enforces it wherever the queue is enabled in a debug build. "
    "No effect in release builds.");
TAG_FLAG(rpc_priority_queue_check_callbacks_do_not_wait, advanced);

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

METRIC_DEFINE_counter(server, rpc_priority_queue_dispatched_callbacks,
    "Outbound-call callback tasks dispatched by the RPC priority queue.",
    yb::MetricUnit::kTasks,
    "Number of kCallback tasks (of any priority) handed to a worker pool by the RPC priority "
    "queue. The per-priority dispatched counters include these.");

METRIC_DEFINE_counter(server, rpc_priority_queue_rejected_full,
    "RPC tasks rejected because the RPC priority queue's waiting-task counter was at its bound.",
    yb::MetricUnit::kTasks,
    "Number of RPC tasks rejected with ServiceUnavailable because the RPC priority queue could not "
    "account for another waiting task. Should be zero in practice; non-zero indicates extreme "
    "overload.");

METRIC_DEFINE_counter(server, rpc_priority_queue_aborted,
    "RPC tasks aborted by the RPC priority queue.",
    yb::MetricUnit::kTasks,
    "Number of waiting RPC tasks failed by the RPC priority queue because it was shutting down.");

using namespace std::literals;

namespace yb::rpc {

namespace {

const auto kShuttingDownStatus = STATUS(Aborted, "Service is shutting down");
const auto kQueueFullStatus = STATUS(ServiceUnavailable, "RPC priority queue is full");

// The queue whose dispatched task is currently executing (Run() or Done()) on this thread, if any.
// Lets CompleteShutdown recognize that it was called from inside one of its own tasks, where
// quiescence is unreachable.
thread_local const RpcPriorityQueue* executing_task_queue = nullptr;

class ScopedExecutingTaskQueue {
 public:
  explicit ScopedExecutingTaskQueue(const RpcPriorityQueue* queue)
      : previous_(executing_task_queue) {
    executing_task_queue = queue;
  }
  ~ScopedExecutingTaskQueue() {
    executing_task_queue = previous_;
  }

 private:
  const RpcPriorityQueue* const previous_;
};

size_t Index(RpcPriority priority) {
  return std::to_underlying(priority);
}

size_t Index(RpcTaskClass task_class) {
  return std::to_underlying(task_class);
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
  Task(RpcPriorityQueue* queue, ThreadPoolTask* inner, RpcPriority priority,
       RpcTaskClass task_class, ThreadPoolPtr pool)
      : queue_(queue), inner_(inner), priority_(priority), task_class_(task_class),
        pool_(std::move(pool)) {}

  RpcPriority priority() const { return priority_; }
  RpcTaskClass task_class() const { return task_class_; }
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
    ScopedExecutingTaskQueue executing(queue_);
#ifdef ENABLE_THREAD_RESTRICTIONS
    if (task_class_ == RpcTaskClass::kCallback &&
        FLAGS_rpc_priority_queue_check_callbacks_do_not_wait) {
      // Enforce the no-blocking contract for callbacks; see the class comment. Debug builds only.
      bool previous = ThreadRestrictions::SetWaitAllowed(false);
      inner_->Run();
      ThreadRestrictions::SetWaitAllowed(previous);
      return;
    }
#endif
    inner_->Run();
  }

  void Done(const Status& status) override {
    {
      // The permit is released only after the inner Done() returns, so that code may still use
      // the queue (e.g. enqueue a retry) without racing CompleteShutdown. The flip side is that it
      // cannot itself complete the shutdown; CompleteShutdown detects that via the thread-local.
      ScopedExecutingTaskQueue executing(queue_);
      inner_->Done(status);
    }
    // inner_ may have deleted itself in Done(); do not touch it afterwards.
    queue_->TaskFinished(task_class_);
    delete this;
  }

  RpcPriorityQueue* const queue_;
  ThreadPoolTask* const inner_;
  const RpcPriority priority_;
  const RpcTaskClass task_class_;
  const ThreadPoolPtr pool_;
  MonoTime queued_at_;
};

RpcPriorityQueue::RpcPriorityQueue(
    std::string name, size_t max_dispatched, size_t callback_reserve,
    const scoped_refptr<MetricEntity>& metric_entity)
    : name_(std::move(name)),
      max_dispatched_(max_dispatched),
      max_dispatched_inbound_(
          max_dispatched > callback_reserve ? max_dispatched - callback_reserve : 1) {
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
    dispatched_callbacks_counter_ =
        METRIC_rpc_priority_queue_dispatched_callbacks.Instantiate(metric_entity);
    aborted_counter_ = METRIC_rpc_priority_queue_aborted.Instantiate(metric_entity);
    rejected_full_counter_ = METRIC_rpc_priority_queue_rejected_full.Instantiate(metric_entity);
  }
  LOG(INFO) << "RpcPriorityQueue " << name_ << " created with permit budget " << max_dispatched_
            << ", inbound cap " << max_dispatched_inbound_;
  if (max_dispatched_inbound_ == max_dispatched_) {
    LOG(WARNING) << "RpcPriorityQueue " << name_ << " has no permits reserved for callbacks; "
                 << "inbound handlers that block on outbound-call callbacks can stall the node "
                 << "under saturation";
  }
}

RpcPriorityQueue::~RpcPriorityQueue() {
  StartShutdown();
  CompleteShutdown();
}

uint64_t RpcPriorityQueue::Pack(State state) {
  DCHECK_LE(state.dispatched, kFieldMask);
  DCHECK_LE(state.dispatched_inbound, kFieldMask);
  DCHECK_LE(state.queued, kFieldMask);
  return (state.closed ? kClosedBit : 0) |
         (static_cast<uint64_t>(state.dispatched) << (2 * kFieldBits)) |
         (static_cast<uint64_t>(state.dispatched_inbound) << kFieldBits) |
         state.queued;
}

RpcPriorityQueue::State RpcPriorityQueue::Unpack(uint64_t value) {
  return State{
    .dispatched = static_cast<uint32_t>((value >> (2 * kFieldBits)) & kFieldMask),
    .dispatched_inbound = static_cast<uint32_t>((value >> kFieldBits) & kFieldMask),
    .queued = static_cast<uint32_t>(value & kFieldMask),
    .closed = (value & kClosedBit) != 0,
  };
}

RpcPriorityQueue::State RpcPriorityQueue::LoadState() const {
  return Unpack(state_.load(std::memory_order_acquire));
}

bool RpcPriorityQueue::CanClaim(const State& state, RpcTaskClass task_class) const {
  // The closed check inside the same CAS as the claim is what makes shutdown safe against
  // concurrent enqueues.
  if (state.closed || state.dispatched >= max_dispatched_) {
    return false;
  }
  switch (task_class) {
    case RpcTaskClass::kInbound:
      // The queued == 0 check is what preserves priority: once anything is waiting, a new arrival
      // must not overtake it, so it goes through the slow path even if a permit happens to be free
      // at this instant (a completion is about to hand that permit to the waiting task).
      return state.queued == 0 && state.dispatched_inbound < max_dispatched_inbound_;
    case RpcTaskClass::kCallback:
      // A callback may additionally overtake waiting work when the inbound cap is saturated: in
      // that situation everything waiting is inbound work that cannot use the free permit anyway
      // (a waiting callback would have been handed the permit by the completion that freed it
      // rather than the permit being released), so letting the callback through is what keeps
      // blocked handlers from stalling the queue.
      return state.queued == 0 || state.dispatched_inbound >= max_dispatched_inbound_;
  }
  FATAL_INVALID_ENUM_VALUE(RpcTaskClass, task_class);
}

bool RpcPriorityQueue::TryClaimPermitFastPath(RpcTaskClass task_class) {
  return UpdateState([this, task_class](State* state) {
    if (!CanClaim(*state, task_class)) {
      return false;
    }
    ++state->dispatched;
    if (task_class == RpcTaskClass::kInbound) {
      ++state->dispatched_inbound;
    }
    return true;
  });
}

bool RpcPriorityQueue::Enqueue(
    ThreadPoolTask* inner, RpcPriority priority, RpcTaskClass task_class,
    const ThreadPoolPtr& target_pool) {
  DCHECK_ONLY_NOTNULL(inner);
  DCHECK_ONLY_NOTNULL(target_pool.get());

  // Registered before the first read of state_ and released after this call's last access to the
  // queue, so that CompleteShutdown cannot return while this call is anywhere inside the queue.
  // The release must happen BEFORE any task-controlled code runs (Done(), or Dispatch() which may
  // synchronously invoke Done()): that code may legitimately shut the queue down, and
  // CompleteShutdown would otherwise wait for a registration that only this call can release.
  // Once released, nothing below may touch the queue except through the task's own claimed permit
  // (which CompleteShutdown waits on separately).
  active_enqueues_.fetch_add(1, std::memory_order_acq_rel);
  bool active = true;
  auto release_active = [this, &active] {
    DCHECK(active);
    active = false;
    active_enqueues_.fetch_sub(1, std::memory_order_acq_rel);
  };
  DEBUG_ONLY_TEST_SYNC_POINT("RpcPriorityQueue::Enqueue::Registered");

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
    release_active();
    inner->Done(kShuttingDownStatus);
    return false;
  }

  auto* task = new Task(this, inner, priority, task_class, target_pool);

  // Fast path: nothing waiting and a permit available. No lock, no band traffic.
  if (TryClaimPermitFastPath(task_class)) {
    IncrementCounter(dispatched_counters_[Index(priority)]);
    if (task_class == RpcTaskClass::kCallback) {
      IncrementCounter(dispatched_callbacks_counter_);
    }
    release_active();
    task->Dispatch();
    return true;
  }

  // Slow path: wait in the band for this priority. Dispatch and abort happen after the lock is
  // released: both may synchronously invoke Done() (pool refusing the task, or inner task
  // cleanup), which must not run under mutex_.
  auto outcome = AdmitOutcome::kQueued;
  {
    std::lock_guard lock(mutex_);
    outcome = ClaimOrRegisterQueued(task_class);
    if (outcome == AdmitOutcome::kQueued) {
      // The band push happens after queued was incremented, which is safe because the only
      // consumers of the bands (PopEligibleLocked, StartShutdown's drain) run under this same lock
      // and therefore cannot observe the incremented count before the push.
      task->MarkQueued();
      bands_[Index(priority)][Index(task_class)].push_back(task);
      OnQueued(priority);
    }
  }
  switch (outcome) {
    case AdmitOutcome::kQueued:
      release_active();
      return true;
    case AdmitOutcome::kClaimed:
      IncrementCounter(dispatched_counters_[Index(priority)]);
      if (task_class == RpcTaskClass::kCallback) {
        IncrementCounter(dispatched_callbacks_counter_);
      }
      release_active();
      task->Dispatch();
      return true;
    case AdmitOutcome::kClosed:
      // Lost a race with StartShutdown, which drains the bands without seeing this task; fail it
      // directly rather than leaving it stranded.
      release_active();
      task->Abort(kShuttingDownStatus);
      return false;
    case AdmitOutcome::kOverloaded:
      // The waiting-task counter is at its bound. Failing admission is the only safe option: an
      // overload-control component must not corrupt its own accounting.
      YB_LOG_EVERY_N_SECS(WARNING, 1)
          << "RpcPriorityQueue " << name_ << " rejecting task: " << kFieldMask
          << " tasks already waiting";
      IncrementCounter(rejected_full_counter_);
      release_active();
      task->Abort(kQueueFullStatus);
      return false;
  }
  LOG(FATAL) << "Unexpected AdmitOutcome: " << std::to_underlying(outcome);
}

RpcPriorityQueue::AdmitOutcome RpcPriorityQueue::ClaimOrRegisterQueued(RpcTaskClass task_class) {
  // Deciding between "claim a permit" and "register as waiting" must be one CAS on the state word.
  // If they were separate steps, a completion's release (queued == 0 -> dispatched--) could land
  // between them, leaving dispatched below the budget with a task registered as waiting and no
  // completion left to hand it a permit: a permanent stall. Because the release CAS also operates
  // on the whole word, the two are linearizable: either we register first and the release retries
  // and hands off to us, or the release happens first and we observe the free permit and claim it.
  auto outcome = AdmitOutcome::kQueued;
  UpdateState([this, task_class, &outcome](State* state) {
    if (state->closed) {
      outcome = AdmitOutcome::kClosed;
      return false;
    }
    if (CanClaim(*state, task_class)) {
      ++state->dispatched;
      if (task_class == RpcTaskClass::kInbound) {
        ++state->dispatched_inbound;
      }
      outcome = AdmitOutcome::kClaimed;
      return true;
    }
    if (state->queued >= kFieldMask) {
      outcome = AdmitOutcome::kOverloaded;
      return false;
    }
    ++state->queued;
    outcome = AdmitOutcome::kQueued;
    return true;
  });
  return outcome;
}

RpcPriorityQueue::Task* RpcPriorityQueue::PopEligibleLocked(bool inbound_eligible) {
  // Strict priority: the first band with an eligible task, kHigh -> kNormal -> kLow, always wins.
  // Within a band, the older of the inbound and callback heads goes first, so arrival order is
  // preserved between the two classes unless the inbound cap makes inbound work ineligible. Under
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
    auto& inbound = band[Index(RpcTaskClass::kInbound)];
    auto& callbacks = band[Index(RpcTaskClass::kCallback)];
    std::deque<Task*>* chosen = nullptr;
    if (inbound_eligible && !inbound.empty()) {
      chosen = &inbound;
    }
    if (!callbacks.empty() &&
        (chosen == nullptr || callbacks.front()->queued_at() < chosen->front()->queued_at())) {
      chosen = &callbacks;
    }
    if (chosen != nullptr) {
      auto* task = chosen->front();
      chosen->pop_front();
      return task;
    }
  }
  return nullptr;
}

void RpcPriorityQueue::TaskFinished(RpcTaskClass task_class) {
  // Single-drainer protocol. If another thread is already draining, registering the completion is
  // all we need to do; that thread will process it before it leaves. This bounds stack depth when
  // a dispatch fails synchronously (the target pool refusing the task invokes Done() inline, which
  // lands here while the drainer is still inside next->Dispatch()).
  // The class counter is incremented before the total so the drainer always finds a class to
  // attribute each total unit to.
  pending_completions_by_class_[Index(task_class)].fetch_add(1, std::memory_order_acq_rel);
  if (pending_completions_.fetch_add(1, std::memory_order_acq_rel) != 0) {
    return;
  }
  do {
    RpcTaskClass finished_class = RpcTaskClass::kInbound;
    for (auto cls : RpcTaskClassList()) {
      auto& counter = pending_completions_by_class_[Index(cls)];
      if (counter.load(std::memory_order_acquire) != 0) {
        // Only the drainer decrements, so the observed value cannot drop under us.
        counter.fetch_sub(1, std::memory_order_acq_rel);
        finished_class = cls;
        break;
      }
    }
    ProcessOneCompletion(finished_class);
    // The decrement below is the last access to this object by the drainer; CompleteShutdown waits
    // for pending_completions_ to reach zero for that reason.
  } while (pending_completions_.fetch_sub(1, std::memory_order_acq_rel) != 1);
}

void RpcPriorityQueue::ProcessOneCompletion(RpcTaskClass finished_class) {
  const bool finished_inbound = finished_class == RpcTaskClass::kInbound;
  auto release_permit = [finished_inbound](State* state) {
    DCHECK_GT(state->dispatched, 0) << "permit released that was never claimed";
    --state->dispatched;
    if (finished_inbound) {
      DCHECK_GT(state->dispatched_inbound, 0) << "inbound permit accounting underflow";
      --state->dispatched_inbound;
    }
  };

  for (;;) {
    // Fast path: nothing waiting, so simply release the permit.
    bool released = UpdateState([&release_permit](State* state) {
      if (state->queued != 0) {
        return false;
      }
      release_permit(state);
      return true;
    });
    if (released) {
      return;
    }

    // Slow path: hand our permit directly to the highest-priority eligible waiting task.
    // dispatched is left unchanged (the permit transfers rather than being released and
    // re-claimed), which is what prevents a concurrent fast-path arrival from stealing it out from
    // under the waiting task. Registration and popping both happen under mutex_, so while we hold
    // it queued cannot change; only the dispatched fields can (via fast-path claims and other
    // completions), which UpdateState tolerates.
    DEBUG_ONLY_TEST_SYNC_POINT("RpcPriorityQueue::ProcessOneCompletion::BeforeLock");
    Task* next = nullptr;
    {
      std::lock_guard lock(mutex_);
      auto state = LoadState();
      if (state.closed) {
        // Shutdown owns the bands from here on: everything waiting is (or is about to be) failed by
        // StartShutdown's drain. Return the permit instead of dispatching past the drain.
        UpdateState([&release_permit](State* s) {
          release_permit(s);
          return true;
        });
        DEBUG_ONLY_TEST_SYNC_POINT("RpcPriorityQueue::ProcessOneCompletion::AfterLock");
        return;
      }
      // An inbound task may take over this permit if, once the finished task's own inbound slot
      // (if any) is returned, the inbound cap still has room.
      const size_t inbound_after = state.dispatched_inbound - (finished_inbound ? 1 : 0);
      next = PopEligibleLocked(inbound_after < max_dispatched_inbound_);
      if (next != nullptr) {
        // queued--, and move the inbound slot between the finished and the popped task.
        const bool next_inbound = next->task_class() == RpcTaskClass::kInbound;
        UpdateState([finished_inbound, next_inbound](State* s) {
          --s->queued;
          if (finished_inbound && !next_inbound) {
            --s->dispatched_inbound;
          } else if (!finished_inbound && next_inbound) {
            ++s->dispatched_inbound;
          }
          return true;
        });
      } else if (state.queued != 0) {
        // Work is waiting but none of it may run: it is all inbound and the inbound cap is
        // saturated. Release the permit so a callback can claim it (CanClaim lets callbacks bypass
        // waiting inbound work in exactly this situation). Done under the lock so no task can
        // register as waiting between our decision and the release.
        UpdateState([&release_permit](State* s) {
          release_permit(s);
          return true;
        });
        DEBUG_ONLY_TEST_SYNC_POINT("RpcPriorityQueue::ProcessOneCompletion::AfterLock");
        return;
      }
      // else: everything that was waiting drained between our check and the lock. Fall through
      // and loop: queued is now 0 and the release branch will take the permit back.
    }
    DEBUG_ONLY_TEST_SYNC_POINT("RpcPriorityQueue::ProcessOneCompletion::AfterLock");
    if (next != nullptr) {
      OnDequeued(next->priority());
      RecordQueueTime(next);
      IncrementCounter(dispatched_counters_[Index(next->priority())]);
      if (next->task_class() == RpcTaskClass::kCallback) {
        IncrementCounter(dispatched_callbacks_counter_);
      }
      next->Dispatch();
      return;
    }
  }
}

void RpcPriorityQueue::StartShutdown() {
  // Set the closed bit with a CAS so that it is ordered against every permit-claiming CAS: after
  // this succeeds no Enqueue can dispatch, whether it started before or after this call.
  bool won = UpdateState([](State* state) {
    if (state->closed) {
      return false;
    }
    state->closed = true;
    return true;
  });
  DEBUG_ONLY_TEST_SYNC_POINT("RpcPriorityQueue::StartShutdown::Closed");
  if (!won) {
    if (IsDrainingThread()) {
      // Re-entered from a task's Done() while we are failing the waiting tasks (that code may
      // legitimately shut down the queue's owner). The drain is already in progress; nothing to do.
      return;
    }
    // Another caller is (or was) draining. Do not return before it has finished failing the
    // waiting tasks, so that "StartShutdown returned" means the same thing for every caller.
    while (!drain_complete_.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(1ms);
    }
    return;
  }
  draining_thread_.store(Thread::CurrentThreadIdForStack(), std::memory_order_release);

  DEBUG_ONLY_TEST_SYNC_POINT("RpcPriorityQueue::StartShutdown::BeforeDrain");
  std::array<Band, kRpcPriorityMapSize> drained;
  {
    std::lock_guard lock(mutex_);
    size_t total = 0;
    for (size_t i = 0; i < bands_.size(); ++i) {
      for (size_t c = 0; c < bands_[i].size(); ++c) {
        drained[i][c].swap(bands_[i][c]);
        total += drained[i][c].size();
      }
    }
    UpdateState([total](State* state) {
      DCHECK_GE(state->queued, total);
      state->queued -= total;
      return true;
    });
  }
  // From here on no one else touches the bands: admission sees closed and fails tasks directly,
  // and completions see closed and release their permits. Failing the drained tasks outside the
  // lock is therefore safe, and necessary since Done() may run arbitrary code.
  size_t aborted = 0;
  for (size_t i = 0; i < drained.size(); ++i) {
    for (auto& fifo : drained[i]) {
      for (auto* task : fifo) {
        OnDequeued(static_cast<RpcPriority>(i));
        task->Abort(kShuttingDownStatus);
        ++aborted;
      }
    }
  }
  if (aborted != 0) {
    LOG(INFO) << "RpcPriorityQueue " << name_ << " aborted " << aborted << " waiting tasks";
    IncrementCounterBy(aborted_counter_, aborted);
  }
  drain_complete_.store(true, std::memory_order_release);
}

bool RpcPriorityQueue::IsDrainingThread() const {
  return draining_thread_.load(std::memory_order_acquire) == Thread::CurrentThreadIdForStack();
}

bool RpcPriorityQueue::CalledFromOwnTask() const {
  if (executing_task_queue == this) {
    // Inside Run() or Done() of a task this queue dispatched (including a Done() invoked
    // synchronously by a pool that refused the task). That task still holds a permit.
    return true;
  }
  // Inside Done() of a task being failed by this thread's own drain in StartShutdown.
  return !drain_complete_.load(std::memory_order_acquire) && IsDrainingThread();
}

void RpcPriorityQueue::CompleteShutdown() {
  if (CalledFromOwnTask()) {
    // Quiescence cannot be reached from inside one of the queue's own tasks: this thread is what
    // is still running, so waiting would deadlock. Callers that need to shut the queue down from
    // task code may call StartShutdown here but must defer CompleteShutdown (and any destruction)
    // to another thread.
    LOG(DFATAL) << "RpcPriorityQueue " << name_
                << ": CompleteShutdown called synchronously from one of the queue's own tasks; "
                << "ignored. Defer it to another thread.";
    return;
  }
  // Quiescence means: the drain has completed, no thread is inside Enqueue, every dispatched task
  // has finished, and no completion is being processed. Dispatched tasks belong to their target
  // pools, which will finish or abort them and thereby call TaskFinished. Shutdown is rare, so a
  // simple wait suffices.
  //
  // Ordering: dispatched must be read before pending_completions_. A completion increments
  // pending_completions_ before it decrements dispatched, so once dispatched is observed at 0
  // every completion has at least registered, and a subsequent pending_completions_ == 0 means the
  // drainer has also finished touching this object.
  auto last_log = CoarseMonoClock::Now();
  for (;;) {
    auto state = LoadState();
    const bool quiescent =
        drain_complete_.load(std::memory_order_acquire) &&
        active_enqueues_.load(std::memory_order_acquire) == 0 &&
        state.dispatched == 0 &&
        pending_completions_.load(std::memory_order_acquire) == 0;
    if (quiescent) {
      DCHECK_EQ(LoadState().queued, 0) << name_ << ": tasks left waiting after shutdown";
      return;
    }
    std::this_thread::sleep_for(1ms);
    auto now = CoarseMonoClock::Now();
    if (now - last_log >= 5s) {
      LOG(WARNING) << "RpcPriorityQueue " << name_ << " still waiting for shutdown: "
                   << state.dispatched << " dispatched tasks, "
                   << active_enqueues_.load(std::memory_order_acquire) << " active enqueues, "
                   << "drain complete: " << drain_complete_.load(std::memory_order_acquire);
      last_log = now;
    }
  }
}

size_t RpcPriorityQueue::TEST_dispatched() const {
  return LoadState().dispatched;
}

size_t RpcPriorityQueue::TEST_dispatched_inbound() const {
  return LoadState().dispatched_inbound;
}

size_t RpcPriorityQueue::TEST_queued() const {
  return LoadState().queued;
}

size_t RpcPriorityQueue::TEST_queued(RpcPriority priority) const {
  std::lock_guard lock(mutex_);
  size_t result = 0;
  for (const auto& fifo : bands_[Index(priority)]) {
    result += fifo.size();
  }
  return result;
}

void RpcPriorityQueue::TEST_SetQueuedCount(uint32_t value) {
  UpdateState([value](State* state) {
    state->queued = value;
    return true;
  });
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
