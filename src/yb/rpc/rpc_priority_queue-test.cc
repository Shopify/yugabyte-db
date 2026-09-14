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
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "yb/rpc/rpc_priority_queue.h"
#include "yb/rpc/thread_pool.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/metrics.h"
#include "yb/util/random_util.h"
#include "yb/util/status.h"
#include "yb/util/test_util.h"
#include "yb/util/tsan_util.h"

METRIC_DECLARE_entity(server);
METRIC_DECLARE_event_stats(rpc_priority_queue_wait_time_normal);
METRIC_DECLARE_counter(rpc_priority_queue_dispatched_low);
METRIC_DECLARE_counter(rpc_priority_queue_aborted);

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
  RpcPriorityQueue queue("fast", 4, metric_entity_);

  std::vector<std::unique_ptr<BlockingTask>> tasks;
  for (int i = 0; i < 4; ++i) {
    tasks.push_back(std::make_unique<BlockingTask>(i, &recorder_));
    ASSERT_TRUE(queue.Enqueue(tasks.back().get(), RpcPriority::kLow, pool));
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
  RpcPriorityQueue queue("strict", 1, metric_entity_);

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&blocker, RpcPriority::kNormal, pool));
  ASSERT_TRUE(blocker.WaitStarted());

  // Enqueue in "wrong" order: low first, then normal, then high, then more of each.
  InstantTask low1(1, &recorder_), low2(2, &recorder_);
  InstantTask normal1(3, &recorder_), normal2(4, &recorder_);
  InstantTask high1(5, &recorder_), high2(6, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&low1, RpcPriority::kLow, pool));
  ASSERT_TRUE(queue.Enqueue(&normal1, RpcPriority::kNormal, pool));
  ASSERT_TRUE(queue.Enqueue(&high1, RpcPriority::kHigh, pool));
  ASSERT_TRUE(queue.Enqueue(&low2, RpcPriority::kLow, pool));
  ASSERT_TRUE(queue.Enqueue(&normal2, RpcPriority::kNormal, pool));
  ASSERT_TRUE(queue.Enqueue(&high2, RpcPriority::kHigh, pool));

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
  RpcPriorityQueue queue("overtake", 1, metric_entity_);

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&blocker, RpcPriority::kNormal, pool));
  ASSERT_TRUE(blocker.WaitStarted());

  BlockingTask waiting_high(1, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&waiting_high, RpcPriority::kHigh, pool));
  ASSERT_EQ(queue.TEST_queued(), 1);

  // While the high task is waiting, new arrivals must queue behind it rather than fast-path.
  InstantTask late_normal(2, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&late_normal, RpcPriority::kNormal, pool));
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
  ASSERT_TRUE(queue.Enqueue(&another_normal, RpcPriority::kNormal, pool));
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
  RpcPriorityQueue queue("shared", 1, metric_entity_);

  BlockingTask on_a(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&on_a, RpcPriority::kNormal, pool_a));
  ASSERT_TRUE(on_a.WaitStarted());

  BlockingTask on_b(1, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&on_b, RpcPriority::kHigh, pool_b));
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
  RpcPriorityQueue queue("shutdown", 1, metric_entity_);

  BlockingTask running(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&running, RpcPriority::kNormal, pool));
  ASSERT_TRUE(running.WaitStarted());

  InstantTask waiting1(1, &recorder_), waiting2(2, &recorder_), waiting3(3, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&waiting1, RpcPriority::kHigh, pool));
  ASSERT_TRUE(queue.Enqueue(&waiting2, RpcPriority::kNormal, pool));
  ASSERT_TRUE(queue.Enqueue(&waiting3, RpcPriority::kLow, pool));
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
  ASSERT_FALSE(queue.Enqueue(&late, RpcPriority::kHigh, pool));
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
  auto queue = std::make_unique<RpcPriorityQueue>("race", 4, metric_entity_);
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
        queue->Enqueue(new CountingTask(&counters), priority, pool);
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
  RpcPriorityQueue queue("deep", 1, metric_entity_);
  CountingTask::Counters counters;

  BlockingTask blocker(0, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&blocker, RpcPriority::kNormal, pool));
  ASSERT_TRUE(blocker.WaitStarted());

  for (int i = 0; i < kBacklog; ++i) {
    auto priority = static_cast<RpcPriority>(i % kRpcPriorityMapSize);
    ASSERT_TRUE(queue.Enqueue(new CountingTask(&counters), priority, pool));
  }
  ASSERT_EQ(queue.TEST_queued(), kBacklog);

  // Shut the pool down first. YBThreadPool::Shutdown joins its worker, which is blocked in the
  // blocker, so run it on a helper thread and then release the blocker.
  std::thread pool_shutdown([&pool] { pool->Shutdown(); });
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
  RpcPriorityQueue queue("cross", 2, metric_entity_);

  BlockingTask a1(0, &recorder_), a2(1, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&a1, RpcPriority::kLow, pool_a));
  ASSERT_TRUE(queue.Enqueue(&a2, RpcPriority::kLow, pool_a));
  ASSERT_TRUE(a1.WaitStarted());
  ASSERT_TRUE(a2.WaitStarted());
  ASSERT_EQ(queue.TEST_dispatched(), 2);

  InstantTask low_a3(2, &recorder_), low_a4(3, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&low_a3, RpcPriority::kLow, pool_a));
  ASSERT_TRUE(queue.Enqueue(&low_a4, RpcPriority::kLow, pool_a));
  InstantTask high_b(4, &recorder_);
  ASSERT_TRUE(queue.Enqueue(&high_b, RpcPriority::kHigh, pool_b));
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
  RpcPriorityQueue queue("refuse", 2, metric_entity_);
  pool->Shutdown();

  InstantTask task(0, &recorder_);
  // Enqueue returns true (the queue accepted it); the pool fails it synchronously.
  ASSERT_TRUE(queue.Enqueue(&task, RpcPriority::kNormal, pool));
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
  RpcPriorityQueue queue("stress", kBudget, metric_entity_);

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
        CHECK(queue.Enqueue(
            new StressTask(&running, &max_running, &completed), priority, pool));
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
  ASSERT_LE(max_running.load(), static_cast<int>(kBudget));

  queue.StartShutdown();
  queue.CompleteShutdown();
  pool->Shutdown();
}

} // namespace yb::rpc
