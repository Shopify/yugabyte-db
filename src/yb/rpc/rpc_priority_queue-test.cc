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

#include <atomic>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "yb/gutil/casts.h"

#include "yb/rpc/rpc_priority_queue.h"
#include "yb/rpc/thread_pool.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/metrics.h"
#include "yb/util/random_util.h"
#include "yb/util/scope_exit.h"
#include "yb/util/status.h"
#include "yb/util/sync_point.h"
#include "yb/util/test_util.h"
#include "yb/util/tsan_util.h"

METRIC_DECLARE_entity(server);
METRIC_DECLARE_event_stats(rpc_priority_queue_wait_time_normal);
METRIC_DECLARE_counter(rpc_priority_queue_dispatched_low);
METRIC_DECLARE_counter(rpc_priority_queue_aborted);

DECLARE_bool(rpc_priority_queue_check_callbacks_do_not_wait);

using namespace std::literals;

namespace yb::rpc {

namespace {

constexpr auto kWaitTimeout = 10s;

// Records the order in which tasks ran and how they completed. Shared by all test tasks.
struct Recorder {
  std::mutex mutex;
  std::vector<int> run_order;
  std::vector<Status> done_statuses;

  void RecordRun(int id) {
    std::lock_guard lock(mutex);
    run_order.push_back(id);
  }

  void RecordDone(const Status& status) {
    std::lock_guard lock(mutex);
    done_statuses.push_back(status);
  }

  std::vector<int> RunOrder() {
    std::lock_guard lock(mutex);
    return run_order;
  }

  size_t DoneCount() {
    std::lock_guard lock(mutex);
    return done_statuses.size();
  }

  size_t CountDoneWith(Status::Code code) {
    std::lock_guard lock(mutex);
    size_t result = 0;
    for (const auto& status : done_statuses) {
      if (status.code() == code) {
        ++result;
      }
    }
    return result;
  }
};

// Task whose Run() blocks until released, so tests can hold permits deterministically.
class BlockingTask : public ThreadPoolTask {
 public:
  BlockingTask(int id, Recorder* recorder) : id_(id), recorder_(recorder) {}

  void Run() override {
    recorder_->RecordRun(id_);
    started_.CountDown();
    release_.Wait();
  }

  void Done(const Status& status) override {
    recorder_->RecordDone(status);
    done_.CountDown();
  }

  bool WaitStarted() { return started_.WaitFor(kWaitTimeout); }
  void Release() { release_.CountDown(); }
  bool WaitDone() { return done_.WaitFor(kWaitTimeout); }

 private:
  const int id_;
  Recorder* const recorder_;
  CountDownLatch started_{1};
  CountDownLatch release_{1};
  CountDownLatch done_{1};
};

// Task that completes as soon as it runs.
class InstantTask : public ThreadPoolTask {
 public:
  InstantTask(int id, Recorder* recorder) : id_(id), recorder_(recorder) {}

  void Run() override {
    recorder_->RecordRun(id_);
  }

  void Done(const Status& status) override {
    recorder_->RecordDone(status);
    done_.CountDown();
  }

  bool WaitDone() { return done_.WaitFor(kWaitTimeout); }

 private:
  const int id_;
  Recorder* const recorder_;
  CountDownLatch done_{1};
};

ThreadPoolPtr MakePool(const std::string& name, size_t max_workers) {
  return std::make_shared<ThreadPool>(ThreadPoolOptions {
    .name = name,
    .max_workers = max_workers,
  });
}

} // namespace

class RpcPriorityQueueTest : public YBTest {
 protected:
  void SetUp() override {
    YBTest::SetUp();
    metric_entity_ = METRIC_ENTITY_server.Instantiate(&metric_registry_, "test.rpc_priority_queue");
  }

  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  Recorder recorder_;
};

// With free permits and nothing waiting, tasks go straight to the pool without touching a band.
TEST_F(RpcPriorityQueueTest, FastPathDoesNotQueue) {
  auto pool = MakePool("fast", 4);
  RpcPriorityQueue queue("fast", 4, /* callback_reserve= */ 0, metric_entity_);

  std::vector<std::unique_ptr<BlockingTask>> tasks;
  for (int i = 0; i < 4; ++i) {
    tasks.push_back(std::make_unique<BlockingTask>(i, &recorder_));
    ASSERT_TRUE(queue.Enqueue(tasks.back().get(), RpcPriority::kLow, RpcTaskClass::kInbound, pool));
    ASSERT_EQ(queue.TEST_queued(), 0);
  }
  ASSERT_EQ(queue.TEST_dispatched(), 4);
  for (auto& task : tasks) {
    ASSERT_TRUE(task->WaitStarted());
  }
  for (auto& task : tasks) {
    task->Release();
    ASSERT_TRUE(task->WaitDone());
  }
  ASSERT_OK(WaitFor(
      [&queue] { return queue.TEST_dispatched() == 0; }, kWaitTimeout, "permits released"));

  // Nothing waited, so no wait-time samples were recorded.
  auto wait_stats = METRIC_rpc_priority_queue_wait_time_normal.Instantiate(metric_entity_);
  ASSERT_EQ(wait_stats->TotalCount(), 0);
  queue.StartShutdown();
  queue.CompleteShutdown();
  pool->Shutdown();
}

// Once the budget is exhausted, waiting tasks are released strictly kHigh -> kNormal -> kLow,
// regardless of arrival order.
TEST_F(RpcPriorityQueueTest, StrictPriorityOrderingUnderSaturation) {
  auto pool = MakePool("strict", 4);
  RpcPriorityQueue queue("strict", 1, /* callback_reserve= */ 0, metric_entity_);

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&blocker, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(blocker.WaitStarted());

  // Enqueue in "wrong" order: low first, then normal, then high, then more of each.
  InstantTask low1(1, &recorder_), low2(2, &recorder_);
  InstantTask normal1(3, &recorder_), normal2(4, &recorder_);
  InstantTask high1(5, &recorder_), high2(6, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&low1, RpcPriority::kLow, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(queue.Enqueue(&normal1, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(queue.Enqueue(&high1, RpcPriority::kHigh, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(queue.Enqueue(&low2, RpcPriority::kLow, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(queue.Enqueue(&normal2, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(queue.Enqueue(&high2, RpcPriority::kHigh, RpcTaskClass::kInbound, pool));

  ASSERT_EQ(queue.TEST_queued(), 6);
  ASSERT_EQ(queue.TEST_queued(RpcPriority::kHigh), 2);
  ASSERT_EQ(queue.TEST_queued(RpcPriority::kNormal), 2);
  ASSERT_EQ(queue.TEST_queued(RpcPriority::kLow), 2);
  ASSERT_EQ(queue.TEST_dispatched(), 1);
  // Nothing beyond the blocker has run yet.
  ASSERT_EQ(recorder_.RunOrder(), std::vector<int>{0});

  blocker.Release();
  ASSERT_TRUE(blocker.WaitDone());
  for (auto* task : {&high1, &high2, &normal1, &normal2, &low1, &low2}) {
    ASSERT_TRUE(task->WaitDone());
  }

  ASSERT_EQ(recorder_.RunOrder(), (std::vector<int>{0, 5, 6, 3, 4, 1, 2}));
  ASSERT_EQ(queue.TEST_queued(), 0);
  ASSERT_OK(WaitFor(
      [&queue] { return queue.TEST_dispatched() == 0; }, kWaitTimeout, "permits released"));

  auto low_dispatched = METRIC_rpc_priority_queue_dispatched_low.Instantiate(metric_entity_);
  ASSERT_EQ(low_dispatched->value(), 2);
  auto wait_stats = METRIC_rpc_priority_queue_wait_time_normal.Instantiate(metric_entity_);
  ASSERT_EQ(wait_stats->TotalCount(), 2);

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool->Shutdown();
}

// A permit released by a completing task goes to the waiting task, not to a concurrently arriving
// new task, even if the new arrival would have taken the fast path on an empty queue.
TEST_F(RpcPriorityQueueTest, WaitingTaskIsNotOvertakenByNewArrival) {
  auto pool = MakePool("overtake", 4);
  RpcPriorityQueue queue("overtake", 1, /* callback_reserve= */ 0, metric_entity_);

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&blocker, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(blocker.WaitStarted());

  BlockingTask waiting_high(1, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&waiting_high, RpcPriority::kHigh, RpcTaskClass::kInbound, pool));
  ASSERT_EQ(queue.TEST_queued(), 1);

  // While the high task is waiting, new arrivals must queue behind it rather than fast-path.
  InstantTask late_normal(2, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&late_normal, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_EQ(queue.TEST_queued(), 2);

  blocker.Release();
  ASSERT_TRUE(blocker.WaitDone());
  // The permit was handed to the waiting high task, which now holds it.
  ASSERT_TRUE(waiting_high.WaitStarted());
  ASSERT_EQ(queue.TEST_dispatched(), 1);
  ASSERT_EQ(queue.TEST_queued(), 1);
  ASSERT_EQ(recorder_.RunOrder(), (std::vector<int>{0, 1}));

  // A new arrival while the permit is held and something waits still cannot fast-path.
  InstantTask another_normal(3, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&another_normal, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_EQ(queue.TEST_queued(), 2);

  waiting_high.Release();
  ASSERT_TRUE(waiting_high.WaitDone());
  ASSERT_TRUE(late_normal.WaitDone());
  ASSERT_TRUE(another_normal.WaitDone());
  ASSERT_EQ(recorder_.RunOrder(), (std::vector<int>{0, 1, 2, 3}));

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool->Shutdown();
}

// The budget is shared across target pools: a task for pool B waits while pool A's task holds the
// only permit, and is dispatched to B once A's task completes.
TEST_F(RpcPriorityQueueTest, BudgetIsSharedAcrossTargetPools) {
  auto pool_a = MakePool("pool_a", 2);
  auto pool_b = MakePool("pool_b", 2);
  RpcPriorityQueue queue("shared", 1, /* callback_reserve= */ 0, metric_entity_);

  BlockingTask on_a(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&on_a, RpcPriority::kNormal, RpcTaskClass::kInbound, pool_a));
  ASSERT_TRUE(on_a.WaitStarted());

  BlockingTask on_b(1, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&on_b, RpcPriority::kHigh, RpcTaskClass::kInbound, pool_b));
  ASSERT_EQ(queue.TEST_queued(), 1);
  // Pool B has idle workers, but the shared budget is exhausted, so on_b must not have started.
  SleepFor(50ms * kTimeMultiplier);
  ASSERT_EQ(recorder_.RunOrder(), std::vector<int>{0});

  on_a.Release();
  ASSERT_TRUE(on_a.WaitDone());
  ASSERT_TRUE(on_b.WaitStarted());
  ASSERT_EQ(queue.TEST_dispatched(), 1);
  on_b.Release();
  ASSERT_TRUE(on_b.WaitDone());

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool_a->Shutdown();
  pool_b->Shutdown();
}

// StartShutdown fails all waiting tasks with an Aborted status without running them, mirroring
// YBThreadPool's behavior, and CompleteShutdown waits for in-flight tasks.
TEST_F(RpcPriorityQueueTest, ShutdownAbortsWaitingAndWaitsForDispatched) {
  auto pool = MakePool("shutdown", 4);
  RpcPriorityQueue queue("shutdown", 1, /* callback_reserve= */ 0, metric_entity_);

  BlockingTask running(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&running, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(running.WaitStarted());

  InstantTask waiting1(1, &recorder_), waiting2(2, &recorder_), waiting3(3, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&waiting1, RpcPriority::kHigh, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(queue.Enqueue(&waiting2, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(queue.Enqueue(&waiting3, RpcPriority::kLow, RpcTaskClass::kInbound, pool));
  ASSERT_EQ(queue.TEST_queued(), 3);

  queue.StartShutdown();
  // Waiting tasks were aborted synchronously and never ran.
  ASSERT_EQ(queue.TEST_queued(), 0);
  ASSERT_TRUE(waiting1.WaitDone());
  ASSERT_TRUE(waiting2.WaitDone());
  ASSERT_TRUE(waiting3.WaitDone());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kAborted), 3);
  ASSERT_EQ(recorder_.RunOrder(), std::vector<int>{0});
  auto aborted = METRIC_rpc_priority_queue_aborted.Instantiate(metric_entity_);
  ASSERT_EQ(aborted->value(), 3);

  // New submissions are refused and failed immediately.
  InstantTask late(4, &recorder_);
  ASSERT_FALSE(queue.Enqueue(&late, RpcPriority::kHigh, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(late.WaitDone());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kAborted), 4);

  // CompleteShutdown blocks until the running task finishes.
  std::atomic<bool> complete_returned{false};
  std::thread completer([&] {
    queue.CompleteShutdown();
    complete_returned = true;
  });
  SleepFor(50ms * kTimeMultiplier);
  ASSERT_FALSE(complete_returned.load());
  ASSERT_EQ(queue.TEST_dispatched(), 1);

  running.Release();
  ASSERT_TRUE(running.WaitDone());
  completer.join();
  ASSERT_TRUE(complete_returned.load());
  ASSERT_EQ(queue.TEST_dispatched(), 0);
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kOk), 1);
  pool->Shutdown();
}

// Counting task that owns itself, for tests that submit many tasks from many threads.
class CountingTask : public ThreadPoolTask {
 public:
  struct Counters {
    std::atomic<int> ran{0};
    std::atomic<int> done_ok{0};
    std::atomic<int> done_aborted{0};
    std::atomic<int> done_other{0};
    // Set by the test once CompleteShutdown has returned; any Run() after that is a bug.
    std::atomic<bool> shutdown_complete{false};
    std::atomic<int> ran_after_shutdown_complete{0};

    int done_total() const {
      return done_ok.load() + done_aborted.load() + done_other.load();
    }
  };

  explicit CountingTask(Counters* counters) : counters_(counters) {}

  void Run() override {
    counters_->ran.fetch_add(1);
    if (counters_->shutdown_complete.load()) {
      counters_->ran_after_shutdown_complete.fetch_add(1);
    }
  }

  void Done(const Status& status) override {
    if (status.ok()) {
      counters_->done_ok.fetch_add(1);
    } else if (status.IsAborted()) {
      counters_->done_aborted.fetch_add(1);
    } else {
      counters_->done_other.fetch_add(1);
    }
    delete this;
  }

 private:
  Counters* const counters_;
};

// Enqueue racing with shutdown: an enqueue that has already passed its initial closed check must
// not be able to claim a permit after StartShutdown, and nothing may run after CompleteShutdown
// returns. Every submitted task must receive exactly one Done().
TEST_F(RpcPriorityQueueTest, EnqueueRacesWithShutdown) {
  constexpr int kProducers = 8;
  constexpr int kAttemptsPerProducer = NonTsanVsTsan(2000, 300);

  auto pool = MakePool("race", 4);
  auto queue = std::make_unique<RpcPriorityQueue>("race", 4, /* callback_reserve= */ 0, metric_entity_);
  CountingTask::Counters counters;
  std::atomic<int> submitted{0};
  CountDownLatch producers_started(kProducers);

  std::vector<std::thread> producers;
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      producers_started.CountDown();
      for (int i = 0; i < kAttemptsPerProducer; ++i) {
        auto priority = static_cast<RpcPriority>((p + i) % kRpcPriorityMapSize);
        submitted.fetch_add(1);
        // Both return values are fine; a false return means Done(aborted) was already invoked.
        queue->Enqueue(new CountingTask(&counters), priority, RpcTaskClass::kInbound, pool);
      }
    });
  }

  // Shut down while producers are mid-flight.
  producers_started.Wait();
  SleepFor(RandomUniformInt(0, 2) * 1ms);
  queue->StartShutdown();
  queue->CompleteShutdown();
  counters.shutdown_complete.store(true);

  for (auto& producer : producers) {
    producer.join();
  }

  // Late enqueues after shutdown are rejected synchronously, so by the time producers have joined
  // every task has its Done() accounted for.
  ASSERT_EQ(counters.done_total(), submitted.load());
  ASSERT_EQ(counters.done_other.load(), 0);
  ASSERT_EQ(counters.ran.load(), counters.done_ok.load());
  ASSERT_EQ(counters.ran_after_shutdown_complete.load(), 0)
      << "a task ran after CompleteShutdown returned";
  ASSERT_EQ(queue->TEST_dispatched(), 0);
  ASSERT_EQ(queue->TEST_queued(), 0);
  // Destroying the queue immediately after CompleteShutdown must be safe.
  queue.reset();
  pool->Shutdown();
}

// Pool-first shutdown with a deep backlog. Once the running task releases its permit, the queue
// hands off to the next waiting task, the closing pool refuses it synchronously (invoking Done()),
// and so on for the entire backlog. This must be processed iteratively: a recursive implementation
// overflows the stack here.
TEST_F(RpcPriorityQueueTest, PoolShutdownWithDeepBacklogDoesNotRecurse) {
  constexpr int kBacklog = NonTsanVsTsan(20000, 2000);

  auto pool = MakePool("deep", 1);
  RpcPriorityQueue queue("deep", 1, /* callback_reserve= */ 0, metric_entity_);
  CountingTask::Counters counters;

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&blocker, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(blocker.WaitStarted());

  for (int i = 0; i < kBacklog; ++i) {
    auto priority = static_cast<RpcPriority>(i % kRpcPriorityMapSize);
    ASSERT_TRUE(queue.Enqueue(new CountingTask(&counters), priority, RpcTaskClass::kInbound, pool));
  }
  ASSERT_EQ(queue.TEST_queued(), kBacklog);

  // Shut the pool down first. YBThreadPool::Shutdown joins its worker, which is blocked in the
  // blocker, so run it on a helper thread; wait until the pool is closing (so every dispatch in
  // the cascade below is refused rather than accepted by the pool) and then release the blocker.
  std::thread pool_shutdown([&pool] { pool->Shutdown(); });
  ASSERT_OK(WaitFor([&pool] { return pool->IsClosing(); }, kWaitTimeout, "pool closing"));
  blocker.Release();
  ASSERT_TRUE(blocker.WaitDone());
  pool_shutdown.join();

  // The blocker's completion cascaded through the whole backlog on the blocker's worker thread.
  ASSERT_OK(WaitFor(
      [&counters] { return counters.done_total() == kBacklog; }, 60s, "backlog drained"));
  ASSERT_EQ(counters.done_aborted.load(), kBacklog);
  ASSERT_EQ(counters.ran.load(), 0);
  ASSERT_EQ(queue.TEST_queued(), 0);
  ASSERT_OK(WaitFor(
      [&queue] { return queue.TEST_dispatched() == 0; }, kWaitTimeout, "permits released"));

  queue.StartShutdown();
  queue.CompleteShutdown();
}

// Cross-pool priority: with the budget held by tasks running on a saturated pool A, a kHigh task
// for idle pool B is dispatched ahead of kLow tasks waiting for A as soon as any permit frees.
// Relies on the invariant max_dispatched <= pool max_workers, so that everything dispatched to A
// is actually running (and therefore will release its permit).
TEST_F(RpcPriorityQueueTest, HighPriorityForIdlePoolNotBlockedBySaturatedPool) {
  auto pool_a = MakePool("sat_a", 2);
  auto pool_b = MakePool("idle_b", 2);
  RpcPriorityQueue queue("cross", 2, /* callback_reserve= */ 0, metric_entity_);

  BlockingTask a1(0, &recorder_), a2(1, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&a1, RpcPriority::kLow, RpcTaskClass::kInbound, pool_a));
  ASSERT_TRUE(queue.Enqueue(&a2, RpcPriority::kLow, RpcTaskClass::kInbound, pool_a));
  ASSERT_TRUE(a1.WaitStarted());
  ASSERT_TRUE(a2.WaitStarted());
  ASSERT_EQ(queue.TEST_dispatched(), 2);

  InstantTask low_a3(2, &recorder_), low_a4(3, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&low_a3, RpcPriority::kLow, RpcTaskClass::kInbound, pool_a));
  ASSERT_TRUE(queue.Enqueue(&low_a4, RpcPriority::kLow, RpcTaskClass::kInbound, pool_a));
  InstantTask high_b(4, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&high_b, RpcPriority::kHigh, RpcTaskClass::kInbound, pool_b));
  ASSERT_EQ(queue.TEST_queued(), 3);

  a1.Release();
  ASSERT_TRUE(a1.WaitDone());
  ASSERT_TRUE(high_b.WaitDone());
  // high_b took the first freed permit, ahead of both waiting low tasks for A. (low_a3 may already
  // have run by now on high_b's released permit, so only the third position is asserted.)
  auto order = recorder_.RunOrder();
  ASSERT_GE(order.size(), 3);
  ASSERT_EQ(order[2], 4);

  a2.Release();
  ASSERT_TRUE(a2.WaitDone());
  ASSERT_TRUE(low_a3.WaitDone());
  ASSERT_TRUE(low_a4.WaitDone());

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool_a->Shutdown();
  pool_b->Shutdown();
}

// If the target pool refuses a task (because it has shut down), the pool invokes Done(), which must
// release the permit so the queue does not leak budget.
TEST_F(RpcPriorityQueueTest, PoolRefusalReleasesPermit) {
  auto pool = MakePool("refuse", 2);
  RpcPriorityQueue queue("refuse", 2, /* callback_reserve= */ 0, metric_entity_);
  pool->Shutdown();

  InstantTask task(0, &recorder_);
  // Enqueue returns true (the queue accepted it); the pool fails it synchronously.
  ASSERT_TRUE(queue.Enqueue(&task, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(task.WaitDone());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kAborted), 1);
  ASSERT_EQ(recorder_.RunOrder().size(), 0);
  ASSERT_EQ(queue.TEST_dispatched(), 0);

  queue.StartShutdown();
  queue.CompleteShutdown();
}

// Many producers and a small budget: dispatched never exceeds the budget, every task completes
// exactly once, and all permits are returned.
TEST_F(RpcPriorityQueueTest, ConcurrentStressDoesNotLeakPermits) {
  constexpr size_t kBudget = 4;
  constexpr int kProducers = 8;
  constexpr int kTasksPerProducer = NonTsanVsTsan(500, 100);
  constexpr int kTotalTasks = kProducers * kTasksPerProducer;

  auto pool = MakePool("stress", kBudget);
  RpcPriorityQueue queue("stress", kBudget, /* callback_reserve= */ 1, metric_entity_);

  std::atomic<int> running{0};
  std::atomic<int> max_running{0};
  std::atomic<int> completed{0};

  class StressTask : public ThreadPoolTask {
   public:
    StressTask(std::atomic<int>* running, std::atomic<int>* max_running,
               std::atomic<int>* completed)
        : running_(running), max_running_(max_running), completed_(completed) {}

    void Run() override {
      auto now_running = running_->fetch_add(1) + 1;
      auto observed_max = max_running_->load();
      while (now_running > observed_max &&
             !max_running_->compare_exchange_weak(observed_max, now_running)) {
      }
      // Tiny bit of work so tasks overlap.
      std::this_thread::yield();
      running_->fetch_sub(1);
    }

    void Done(const Status& status) override {
      CHECK_OK(status);
      completed_->fetch_add(1);
      delete this;
    }

   private:
    std::atomic<int>* const running_;
    std::atomic<int>* const max_running_;
    std::atomic<int>* const completed_;
  };

  std::vector<std::thread> producers;
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      for (int i = 0; i < kTasksPerProducer; ++i) {
        auto priority = static_cast<RpcPriority>((p + i) % kRpcPriorityMapSize);
        auto task_class = static_cast<RpcTaskClass>(i % kRpcTaskClassMapSize);
        CHECK(queue.Enqueue(
            new StressTask(&running, &max_running, &completed), priority, task_class, pool));
      }
    });
  }
  for (auto& producer : producers) {
    producer.join();
  }

  ASSERT_OK(WaitFor(
      [&completed] { return completed.load() == kTotalTasks; }, 60s, "all tasks complete"));
  ASSERT_OK(WaitFor(
      [&queue] { return queue.TEST_dispatched() == 0; }, kWaitTimeout, "permits released"));
  ASSERT_EQ(queue.TEST_queued(), 0);
  ASSERT_EQ(queue.TEST_dispatched_inbound(), 0);
  ASSERT_LE(max_running.load(), static_cast<int>(kBudget));

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool->Shutdown();
}

// The inbound cap: inbound tasks may hold at most budget - reserve permits, while callbacks may
// use the whole budget and may bypass inbound work that is waiting on the cap.
TEST_F(RpcPriorityQueueTest, CallbacksBypassInboundWaitingOnCap) {
  // This test holds a permit with a callback that blocks on a latch, which the callback contract
  // forbids (and which is fatal in debug builds); it is the simplest way to observe permit
  // accounting for callbacks, so the check is disabled for the duration of the test.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_priority_queue_check_callbacks_do_not_wait) = false;
  auto flag_reset = ScopeExit([] {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_priority_queue_check_callbacks_do_not_wait) = true;
  });

  auto pool = MakePool("cap", 4);
  // Budget 2, reserve 1 -> at most one inbound task at a time.
  RpcPriorityQueue queue("cap", 2, /* callback_reserve= */ 1, metric_entity_);
  ASSERT_EQ(queue.max_dispatched_inbound(), 1);

  BlockingTask inbound1(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&inbound1, RpcPriority::kHigh, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(inbound1.WaitStarted());
  ASSERT_EQ(queue.TEST_dispatched(), 1);
  ASSERT_EQ(queue.TEST_dispatched_inbound(), 1);

  // A second inbound task waits on the cap even though a permit is free.
  InstantTask inbound2(1, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&inbound2, RpcPriority::kHigh, RpcTaskClass::kInbound, pool));
  ASSERT_EQ(queue.TEST_queued(), 1);
  ASSERT_EQ(queue.TEST_dispatched(), 1);

  // A lower-priority callback takes the free permit past the waiting high-priority inbound task.
  BlockingTask callback1(2, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&callback1, RpcPriority::kLow, RpcTaskClass::kCallback, pool));
  ASSERT_TRUE(callback1.WaitStarted());
  ASSERT_EQ(queue.TEST_dispatched(), 2);
  ASSERT_EQ(queue.TEST_dispatched_inbound(), 1);
  ASSERT_EQ(recorder_.RunOrder(), (std::vector<int>{0, 2}));

  // Budget is now full: a further callback waits.
  InstantTask callback2(3, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&callback2, RpcPriority::kLow, RpcTaskClass::kCallback, pool));
  ASSERT_EQ(queue.TEST_queued(), 2);

  // Callback finishing: its permit goes to the waiting callback, not the capped inbound task,
  // despite the inbound task's higher priority.
  callback1.Release();
  ASSERT_TRUE(callback1.WaitDone());
  ASSERT_TRUE(callback2.WaitDone());
  ASSERT_EQ(recorder_.RunOrder(), (std::vector<int>{0, 2, 3}));
  ASSERT_OK(WaitFor(
      [&queue] { return queue.TEST_dispatched() == 1; }, kWaitTimeout, "callback permit released"));
  ASSERT_EQ(queue.TEST_queued(), 1);

  // Inbound finishing frees the inbound slot: the waiting inbound task runs.
  inbound1.Release();
  ASSERT_TRUE(inbound1.WaitDone());
  ASSERT_TRUE(inbound2.WaitDone());
  ASSERT_EQ(recorder_.RunOrder(), (std::vector<int>{0, 2, 3, 1}));
  ASSERT_OK(WaitFor(
      [&queue] { return queue.TEST_dispatched() == 0; }, kWaitTimeout, "permits released"));
  ASSERT_EQ(queue.TEST_dispatched_inbound(), 0);

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool->Shutdown();
}

// Inbound handlers that block until a callback they submitted to the same queue has run: with a
// callback reserve this always makes progress, no matter how many such handlers are submitted.
TEST_F(RpcPriorityQueueTest, BlockingHandlersDoNotStarveCallbacks) {
  constexpr int kHandlers = 20;
  auto pool = MakePool("deadlock", 4);
  RpcPriorityQueue queue("deadlock", 2, /* callback_reserve= */ 1, metric_entity_);

  // Handler: submits a callback to the queue and waits for it to complete, mirroring a
  // synchronous YBClient call made from inside an RPC handler.
  class BlockingHandler : public ThreadPoolTask {
   public:
    BlockingHandler(RpcPriorityQueue* queue, ThreadPoolPtr pool, std::atomic<int>* completed)
        : queue_(queue), pool_(std::move(pool)), completed_(completed) {}

    void Run() override {
      CountDownLatch callback_done(1);
      auto* callback = MakeFunctorThreadPoolTask<std::function<void()>, ThreadPoolTask>(
          std::function<void()>([&callback_done] { callback_done.CountDown(); }));
      CHECK(queue_->Enqueue(callback, RpcPriority::kNormal, RpcTaskClass::kCallback, pool_));
      callback_done.Wait();
    }

    void Done(const Status& status) override {
      CHECK_OK(status);
      completed_->fetch_add(1);
      delete this;
    }

   private:
    RpcPriorityQueue* const queue_;
    const ThreadPoolPtr pool_;
    std::atomic<int>* const completed_;
  };

  std::atomic<int> completed{0};
  for (int i = 0; i < kHandlers; ++i) {
    ASSERT_TRUE(queue.Enqueue(
        new BlockingHandler(&queue, pool, &completed), RpcPriority::kNormal,
        RpcTaskClass::kInbound, pool));
  }
  ASSERT_OK(WaitFor(
      [&completed] { return completed.load() == kHandlers; }, 30s, "all handlers complete"));
  ASSERT_OK(WaitFor(
      [&queue] { return queue.TEST_dispatched() == 0; }, kWaitTimeout, "permits released"));

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool->Shutdown();
}

// Within a band, inbound and callback tasks are dispatched in arrival order when both are eligible.
TEST_F(RpcPriorityQueueTest, ArrivalOrderPreservedAcrossClassesWithinBand) {
  auto pool = MakePool("order", 4);
  RpcPriorityQueue queue("order", 1, /* callback_reserve= */ 0, metric_entity_);

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&blocker, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(blocker.WaitStarted());

  InstantTask callback1(1, &recorder_), inbound1(2, &recorder_), callback2(3, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&callback1, RpcPriority::kNormal, RpcTaskClass::kCallback, pool));
  // Ensure distinct queued_at timestamps.
  SleepFor(1ms);
  ASSERT_TRUE(queue.Enqueue(&inbound1, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  SleepFor(1ms);
  ASSERT_TRUE(queue.Enqueue(&callback2, RpcPriority::kNormal, RpcTaskClass::kCallback, pool));
  ASSERT_EQ(queue.TEST_queued(), 3);

  blocker.Release();
  ASSERT_TRUE(blocker.WaitDone());
  for (auto* task : {&callback1, &inbound1, &callback2}) {
    ASSERT_TRUE(task->WaitDone());
  }
  ASSERT_EQ(recorder_.RunOrder(), (std::vector<int>{0, 1, 2, 3}));

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool->Shutdown();
}

// Two threads call StartShutdown concurrently: neither may return before every waiting task has
// been failed, and CompleteShutdown must then find the queue quiescent.
TEST_F(RpcPriorityQueueTest, ConcurrentStartShutdownWaitsForDrain) {
  constexpr int kWaiting = 20;
  auto pool = MakePool("concurrent_shutdown", 4);
  auto queue = std::make_unique<RpcPriorityQueue>(
      "concurrent_shutdown", 1, /* callback_reserve= */ 0, metric_entity_);

  // Waiting task whose Done() is slow, to widen the drain window.
  class SlowDoneTask : public ThreadPoolTask {
   public:
    explicit SlowDoneTask(std::atomic<int>* done) : done_(done) {}
    void Run() override { LOG(FATAL) << "waiting task must not run"; }
    void Done(const Status& status) override {
      CHECK(status.IsAborted()) << status;
      SleepFor(10ms);
      done_->fetch_add(1);
      delete this;
    }
   private:
    std::atomic<int>* const done_;
  };

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue->Enqueue(&blocker, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(blocker.WaitStarted());
  std::atomic<int> done{0};
  for (int i = 0; i < kWaiting; ++i) {
    ASSERT_TRUE(queue->Enqueue(
        new SlowDoneTask(&done), RpcPriority::kLow, RpcTaskClass::kInbound, pool));
  }
  ASSERT_EQ(queue->TEST_queued(), kWaiting);

  std::atomic<int> done_observed_at_return[2] = {-1, -1};
  std::thread shutdown_threads[2];
  for (int t = 0; t < 2; ++t) {
    shutdown_threads[t] = std::thread([&, t] {
      queue->StartShutdown();
      done_observed_at_return[t] = done.load();
    });
  }
  for (auto& thread : shutdown_threads) {
    thread.join();
  }
  // Both callers returned only after the whole drain, including the loser of the closed-bit race.
  ASSERT_EQ(done_observed_at_return[0].load(), kWaiting);
  ASSERT_EQ(done_observed_at_return[1].load(), kWaiting);
  ASSERT_EQ(queue->TEST_queued(), 0);

  blocker.Release();
  ASSERT_TRUE(blocker.WaitDone());
  queue->CompleteShutdown();
  queue.reset();
  pool->Shutdown();
}

// The queued counter is bounded: at the bound, admission fails with ServiceUnavailable instead of
// wrapping the counter.
TEST_F(RpcPriorityQueueTest, QueuedCounterBoundRejectsAdmission) {
  auto pool = MakePool("bound", 2);
  RpcPriorityQueue queue("bound", 1, /* callback_reserve= */ 0, metric_entity_);

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&blocker, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(blocker.WaitStarted());

  // Pretend the bands hold max_queued() tasks (without allocating them).
  queue.TEST_SetQueuedCount(narrow_cast<uint32_t>(RpcPriorityQueue::max_queued()));
  InstantTask rejected(1, &recorder_);
  ASSERT_FALSE(queue.Enqueue(&rejected, RpcPriority::kHigh, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(rejected.WaitDone());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kServiceUnavailable), 1);
  ASSERT_EQ(recorder_.RunOrder(), std::vector<int>{0});
  ASSERT_EQ(queue.TEST_queued(), RpcPriorityQueue::max_queued());
  // Callbacks are bounded the same way (the budget is full so it cannot fast-path either).
  InstantTask rejected_callback(2, &recorder_);
  ASSERT_FALSE(queue.Enqueue(
      &rejected_callback, RpcPriority::kHigh, RpcTaskClass::kCallback, pool));
  ASSERT_TRUE(rejected_callback.WaitDone());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kServiceUnavailable), 2);

  // Restore the real (empty) band size; admission works again.
  queue.TEST_SetQueuedCount(0);
  InstantTask accepted(3, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&accepted, RpcPriority::kHigh, RpcTaskClass::kInbound, pool));
  ASSERT_EQ(queue.TEST_queued(), 1);
  blocker.Release();
  ASSERT_TRUE(blocker.WaitDone());
  ASSERT_TRUE(accepted.WaitDone());
  ASSERT_EQ(recorder_.RunOrder(), (std::vector<int>{0, 3}));

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool->Shutdown();
}

// Task whose Done() shuts the queue down, modelling a callback that reacts to a failure by
// shutting down its owner. Enqueue must have released its active-admission registration before
// invoking Done(), or CompleteShutdown would wait for it forever.
class ShutdownFromDoneTask : public ThreadPoolTask {
 public:
  ShutdownFromDoneTask(RpcPriorityQueue* queue, bool complete, Recorder* recorder)
      : queue_(queue), complete_(complete), recorder_(recorder) {}

  void Run() override { LOG(FATAL) << "must not run"; }

  void Done(const Status& status) override {
    recorder_->RecordDone(status);
    queue_->StartShutdown();
    if (complete_) {
      queue_->CompleteShutdown();
    }
    done_.CountDown();
  }

  bool WaitDone() { return done_.WaitFor(kWaitTimeout); }

 private:
  RpcPriorityQueue* const queue_;
  const bool complete_;
  Recorder* const recorder_;
  CountDownLatch done_{1};
};

// A task rejected because the queue is closed may shut the queue down from its Done().
TEST_F(RpcPriorityQueueTest, RejectedTaskMayShutDownQueueFromDone) {
  auto pool = MakePool("reject_shutdown", 2);
  auto queue = std::make_unique<RpcPriorityQueue>(
      "reject_shutdown", 1, /* callback_reserve= */ 0, metric_entity_);
  queue->StartShutdown();

  ShutdownFromDoneTask task(queue.get(), /* complete= */ true, &recorder_);
  ASSERT_FALSE(queue->Enqueue(&task, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(task.WaitDone());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kAborted), 1);
  // Done() already completed the shutdown; destroying is safe.
  queue.reset();
  pool->Shutdown();
}

// A task rejected at the queued-counter bound may shut the queue down from its Done(), including
// waiting in CompleteShutdown for tasks that other threads are still running.
TEST_F(RpcPriorityQueueTest, TaskRejectedAtBoundMayShutDownQueueFromDone) {
  auto pool = MakePool("bound_shutdown", 2);
  auto queue = std::make_unique<RpcPriorityQueue>(
      "bound_shutdown", 1, /* callback_reserve= */ 0, metric_entity_);

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue->Enqueue(&blocker, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(blocker.WaitStarted());
  queue->TEST_SetQueuedCount(narrow_cast<uint32_t>(RpcPriorityQueue::max_queued()));

  // Done() restores the real (empty) band count before shutting down, since the bound was faked.
  class RejectedThenShutdown : public ThreadPoolTask {
   public:
    RejectedThenShutdown(RpcPriorityQueue* queue, Recorder* recorder)
        : queue_(queue), recorder_(recorder) {}
    void Run() override { LOG(FATAL) << "must not run"; }
    void Done(const Status& status) override {
      recorder_->RecordDone(status);
      queue_->TEST_SetQueuedCount(0);
      queue_->StartShutdown();
      queue_->CompleteShutdown();  // Blocks until the blocker (another thread) finishes.
      done_.CountDown();
    }
    bool WaitDone() { return done_.WaitFor(kWaitTimeout); }
   private:
    RpcPriorityQueue* const queue_;
    Recorder* const recorder_;
    CountDownLatch done_{1};
  };

  RejectedThenShutdown task(queue.get(), &recorder_);
  std::atomic<bool> enqueue_returned{false};
  std::thread enqueuer([&] {
    ASSERT_FALSE(queue->Enqueue(&task, RpcPriority::kHigh, RpcTaskClass::kInbound, pool));
    enqueue_returned = true;
  });
  // The rejected task's Done() is now inside CompleteShutdown, waiting only for the blocker (not
  // for the Enqueue call it is nested in).
  SleepFor(100ms * kTimeMultiplier);
  ASSERT_FALSE(enqueue_returned.load());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kServiceUnavailable), 1);
  blocker.Release();
  ASSERT_TRUE(blocker.WaitDone());
  enqueuer.join();
  ASSERT_TRUE(task.WaitDone());
  queue.reset();
  pool->Shutdown();
}

// A task refused synchronously by a closing target pool (Enqueue returns true, Done(Aborted) has
// run) may call StartShutdown from its Done(); the shutdown is then completed from outside.
TEST_F(RpcPriorityQueueTest, PoolRefusedTaskMayStartShutdownFromDone) {
  auto pool = MakePool("refused_start", 2);
  auto queue = std::make_unique<RpcPriorityQueue>(
      "refused_start", 2, /* callback_reserve= */ 0, metric_entity_);
  pool->Shutdown();

  ShutdownFromDoneTask task(queue.get(), /* complete= */ false, &recorder_);
  ASSERT_TRUE(queue->Enqueue(&task, RpcPriority::kNormal, RpcTaskClass::kCallback, pool));
  ASSERT_TRUE(task.WaitDone());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kAborted), 1);
  // Permit was returned after Done(); the queue is closed and quiescent.
  ASSERT_EQ(queue->TEST_dispatched(), 0);
  InstantTask late(1, &recorder_);
  ASSERT_FALSE(queue->Enqueue(&late, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  queue->CompleteShutdown();
  queue.reset();
}

namespace {

// A task refused by a closing pool whose Done() also calls CompleteShutdown: the task still holds
// its permit at that point, so completion cannot be satisfied from there.
void CompleteShutdownFromPoolRefusedDone(
    const scoped_refptr<MetricEntity>& metric_entity, Recorder* recorder) {
  auto pool = MakePool("refused_complete", 2);
  RpcPriorityQueue queue("refused_complete", 2, /* callback_reserve= */ 0, metric_entity);
  pool->Shutdown();
  ShutdownFromDoneTask task(&queue, /* complete= */ true, recorder);
  CHECK(queue.Enqueue(&task, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  CHECK(task.WaitDone());
  // Only reached in release builds, where the misuse is logged rather than fatal: the queue must
  // still be shut down cleanly from outside and nothing may have hung.
  queue.CompleteShutdown();
}

} // namespace

TEST_F(RpcPriorityQueueTest, CompleteShutdownFromDispatchedTaskIsRejected) {
#ifndef NDEBUG
  ASSERT_DEATH(CompleteShutdownFromPoolRefusedDone(metric_entity_, &recorder_),
               "CompleteShutdown called synchronously from one of the queue's own tasks");
#else
  ASSERT_NO_FATALS(CompleteShutdownFromPoolRefusedDone(metric_entity_, &recorder_));
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kAborted), 1);
#endif
}

// A task failed by StartShutdown's drain may re-enter StartShutdown from its Done().
TEST_F(RpcPriorityQueueTest, DrainedTaskMayReenterStartShutdown) {
  auto pool = MakePool("drain_reenter", 2);
  RpcPriorityQueue queue("drain_reenter", 1, /* callback_reserve= */ 0, metric_entity_);

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&blocker, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(blocker.WaitStarted());
  ShutdownFromDoneTask waiting(&queue, /* complete= */ false, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&waiting, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_EQ(queue.TEST_queued(), 1);

  // The drain fails `waiting`, whose Done() re-enters StartShutdown; that must not hang.
  queue.StartShutdown();
  ASSERT_TRUE(waiting.WaitDone());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kAborted), 1);
  ASSERT_EQ(queue.TEST_queued(), 0);

  blocker.Release();
  ASSERT_TRUE(blocker.WaitDone());
  queue.CompleteShutdown();
  pool->Shutdown();
}

#ifndef NDEBUG
// A completion that observes the closed bit under the lock returns its permit instead of
// dispatching a waiting task past StartShutdown's drain. Sync points force the interleaving that
// used to dispatch: the completion takes the lock after the closed bit is set but before
// StartShutdown drains the bands.
TEST_F(RpcPriorityQueueTest, CompletionAfterCloseReleasesPermit) {
  auto pool = MakePool("close_race", 2);
  RpcPriorityQueue queue("close_race", 1, /* callback_reserve= */ 0, metric_entity_);

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&blocker, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_TRUE(blocker.WaitStarted());
  InstantTask waiting(1, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&waiting, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
  ASSERT_EQ(queue.TEST_queued(), 1);

  auto* sync = SyncPoint::GetInstance();
  sync->LoadDependency({
      // The completion may take the lock only once the closed bit is set...
      {"RpcPriorityQueue::StartShutdown::Closed",
       "RpcPriorityQueue::ProcessOneCompletion::BeforeLock"},
      // ...and the drain may run only once the completion has made its decision.
      {"RpcPriorityQueue::ProcessOneCompletion::AfterLock",
       "RpcPriorityQueue::StartShutdown::BeforeDrain"}});
  sync->ClearTrace();
  sync->EnableProcessing();
  auto sync_reset = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearTrace();
  });

  // The blocker's completion finds queued != 0 and pauses before taking the lock.
  blocker.Release();
  ASSERT_TRUE(blocker.WaitDone());
  std::thread shutdown([&queue] { queue.StartShutdown(); });
  shutdown.join();

  // The completion saw closed and released its permit; the drain then failed the waiting task.
  ASSERT_TRUE(waiting.WaitDone());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kAborted), 1);
  ASSERT_EQ(recorder_.RunOrder(), std::vector<int>{0});
  queue.CompleteShutdown();
  ASSERT_EQ(queue.TEST_dispatched(), 0);
  ASSERT_EQ(queue.TEST_queued(), 0);
  pool->Shutdown();
}

// CompleteShutdown does not return while a thread is still inside Enqueue, even if that thread
// registered before closure and has not yet observed the closed bit.
TEST_F(RpcPriorityQueueTest, CompleteShutdownWaitsForActiveEnqueue) {
  auto pool = MakePool("active_enqueue", 2);
  auto queue = std::make_unique<RpcPriorityQueue>(
      "active_enqueue", 1, /* callback_reserve= */ 0, metric_entity_);

  auto* sync = SyncPoint::GetInstance();
  sync->LoadDependency({{
      "RpcPriorityQueueTest::CompleteShutdownWaitsForActiveEnqueue::Proceed",
      "RpcPriorityQueue::Enqueue::Registered"}});
  sync->ClearTrace();
  sync->EnableProcessing();
  auto sync_reset = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearTrace();
  });

  InstantTask task(0, &recorder_);
  std::atomic<bool> enqueue_returned{false};
  std::thread enqueuer([&] {
    // Pauses inside Enqueue right after registering as active.
    ASSERT_FALSE(queue->Enqueue(&task, RpcPriority::kNormal, RpcTaskClass::kInbound, pool));
    enqueue_returned = true;
  });

  queue->StartShutdown();
  std::atomic<bool> complete_returned{false};
  std::thread completer([&] {
    queue->CompleteShutdown();
    complete_returned = true;
  });
  SleepFor(100ms * kTimeMultiplier);
  ASSERT_FALSE(complete_returned.load()) << "CompleteShutdown returned with an Enqueue in flight";
  ASSERT_FALSE(enqueue_returned.load());

  TEST_SYNC_POINT("RpcPriorityQueueTest::CompleteShutdownWaitsForActiveEnqueue::Proceed");
  enqueuer.join();
  completer.join();
  ASSERT_TRUE(task.WaitDone());
  ASSERT_EQ(recorder_.CountDoneWith(Status::Code::kAborted), 1);
  ASSERT_EQ(recorder_.RunOrder().size(), 0);
  // Safe to destroy now.
  queue.reset();
  pool->Shutdown();
}

namespace {

// Submits a callback that waits on a latch, which violates the callback contract.
void RunBlockingCallback(const scoped_refptr<MetricEntity>& metric_entity) {
  auto pool = MakePool("fatal", 2);
  RpcPriorityQueue queue("fatal", 2, /* callback_reserve= */ 1, metric_entity);
  CountDownLatch never(1);
  auto* callback = MakeFunctorThreadPoolTask<std::function<void()>, ThreadPoolTask>(
      std::function<void()>([&never] { never.WaitFor(1s); }));
  queue.Enqueue(callback, RpcPriority::kNormal, RpcTaskClass::kCallback, pool);
  SleepFor(2s);
}

} // namespace

// The no-blocking contract for callbacks is enforced: a callback that waits crashes the process.
TEST_F(RpcPriorityQueueTest, BlockingCallbackIsFatal) {
  ASSERT_TRUE(FLAGS_rpc_priority_queue_check_callbacks_do_not_wait);
  ASSERT_DEATH(RunBlockingCallback(metric_entity_), "Waiting is not allowed");
}

// Boundary of what the reserve can do when callbacks violate the contract: a chain of nested
// waits (handler -> callback -> callback ...) completes only as long as its depth does not exceed
// the reserve. Documents the limit rather than protecting against it; the check above is what
// protects. The check is disabled here so the chain can be built.
TEST_F(RpcPriorityQueueTest, NestedBlockingCallbackChainWithinReserveCompletes) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_priority_queue_check_callbacks_do_not_wait) = false;
  auto flag_reset = ScopeExit([] {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_priority_queue_check_callbacks_do_not_wait) = true;
  });

  constexpr size_t kReserve = 2;
  auto pool = MakePool("nested", 4);
  RpcPriorityQueue queue("nested", 1 + kReserve, kReserve, metric_entity_);
  ASSERT_EQ(queue.max_dispatched_inbound(), 1);

  // Task that submits a chain of `depth` further callbacks to the queue, each waiting on the next.
  class ChainTask : public ThreadPoolTask {
   public:
    ChainTask(RpcPriorityQueue* queue, ThreadPoolPtr pool, size_t depth, CountDownLatch* done)
        : queue_(queue), pool_(std::move(pool)), depth_(depth), done_(done) {}

    void Run() override {
      if (depth_ == 0) {
        return;
      }
      CountDownLatch next_done(1);
      CHECK(queue_->Enqueue(
          new ChainTask(queue_, pool_, depth_ - 1, &next_done), RpcPriority::kNormal,
          RpcTaskClass::kCallback, pool_));
      next_done.Wait();
    }

    void Done(const Status& status) override {
      CHECK_OK(status);
      done_->CountDown();
      delete this;
    }

   private:
    RpcPriorityQueue* const queue_;
    const ThreadPoolPtr pool_;
    const size_t depth_;
    CountDownLatch* const done_;
  };

  // Handler waiting on a chain of kReserve nested callbacks: the deepest one takes the last
  // reserved permit, so the chain completes.
  CountDownLatch done(1);
  ASSERT_TRUE(queue.Enqueue(
      new ChainTask(&queue, pool, kReserve, &done), RpcPriority::kNormal, RpcTaskClass::kInbound,
      pool));
  ASSERT_TRUE(done.WaitFor(kWaitTimeout));
  ASSERT_OK(WaitFor(
      [&queue] { return queue.TEST_dispatched() == 0; }, kWaitTimeout, "permits released"));

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool->Shutdown();
}
#endif  // NDEBUG

} // namespace yb::rpc
