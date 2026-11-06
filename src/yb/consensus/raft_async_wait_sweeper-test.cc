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

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "yb/consensus/raft_async_wait_sweeper.h"

#include "yb/rpc/io_thread_pool.h"
#include "yb/rpc/scheduler.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/errno.h"
#include "yb/util/logging.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/test_util.h"

using namespace std::literals;

namespace yb::consensus {

namespace {

using Clock = RaftAsyncWaitSweeper::Clock;

// What the entries of a test were called with. Outlives the sweeper's callbacks: the fixture shuts
// the sweeper and the scheduler down before destroying it.
class Recorder {
 public:
  void Due(int id) {
    std::lock_guard lock(mutex_);
    due_.push_back(id);
    due_times_.push_back(Clock::now());
  }

  void Shutdown(int id, const Status& status) {
    std::lock_guard lock(mutex_);
    shutdown_.push_back(id);
    shutdown_statuses_.push_back(status);
  }

  std::vector<int> due() {
    std::lock_guard lock(mutex_);
    return due_;
  }

  std::vector<Clock::time_point> due_times() {
    std::lock_guard lock(mutex_);
    return due_times_;
  }

  std::vector<int> shutdown() {
    std::lock_guard lock(mutex_);
    return shutdown_;
  }

  std::vector<Status> shutdown_statuses() {
    std::lock_guard lock(mutex_);
    return shutdown_statuses_;
  }

  Status WaitDue(size_t count) {
    return WaitFor([this, count] { return due().size() >= count; }, 10s,
                   Format("$0 entries due", count));
  }

  Status WaitShutdown(size_t count) {
    return WaitFor([this, count] { return shutdown().size() >= count; }, 10s,
                   Format("$0 entries shut down", count));
  }

 private:
  std::mutex mutex_;
  std::vector<int> due_;
  std::vector<Clock::time_point> due_times_;
  std::vector<int> shutdown_;
  std::vector<Status> shutdown_statuses_;
};

class TestEntry : public RaftAsyncWaitSweeper::Entry {
 public:
  TestEntry(Recorder* recorder, int id) : recorder_(*recorder), id_(id) {}

  void OnDue() override { recorder_.Due(id_); }

  void OnShutdown(const Status& status) override { recorder_.Shutdown(id_, status); }

 private:
  Recorder& recorder_;
  const int id_;
};

// An entry whose destructor registers another entry with the sweeper, as the last reference to an
// AsyncWriteWait can destroy a RaftConsensus that reaches the sweeper again.
class ReentrantEntry : public TestEntry {
 public:
  ReentrantEntry(
      Recorder* recorder, int id, RaftAsyncWaitSweeper* sweeper, std::atomic<int>* destroyed)
      : TestEntry(recorder, id), recorder_(*recorder), sweeper_(*sweeper),
        destroyed_(*destroyed) {}

  ~ReentrantEntry() {
    // Fails after shutdown, which is fine here: it only has to return.
    auto result = sweeper_.Register(std::make_shared<TestEntry>(&recorder_, -1), 1h);
    LOG_IF(INFO, !result.ok()) << "Register from a destructor: " << result.status();
    ++destroyed_;
  }

 private:
  Recorder& recorder_;
  RaftAsyncWaitSweeper& sweeper_;
  std::atomic<int>& destroyed_;
};

} // namespace

class RaftAsyncWaitSweeperTest : public YBTest {
 protected:
  void SetUp() override {
    YBTest::SetUp();
    // One io thread, so that the callbacks of successive expiries cannot overlap and their order
    // is the order of the entries.
    pool_.emplace("sweeper-test", 1);
    scheduler_.emplace(&pool_->io_service());
    CreateSweeper();
  }

  void TearDown() override {
    if (sweeper_) {
      sweeper_->Shutdown(RaftAsyncWaitSweeper::ShutdownStatus());
    }
    scheduler_->Shutdown();
    pool_->Shutdown();
    pool_->Join();
    sweeper_.reset();
    YBTest::TearDown();
  }

  void CreateSweeper() {
    if (sweeper_) {
      sweeper_->Shutdown(RaftAsyncWaitSweeper::ShutdownStatus());
    }
    sweeper_ = std::make_shared<RaftAsyncWaitSweeper>(&*scheduler_);
  }

  Result<RaftAsyncWaitSweeper::Handle> Register(int id, Clock::duration delay) {
    return sweeper_->Register(std::make_shared<TestEntry>(&recorder_, id), delay);
  }

  RaftAsyncWaitSweeper::Counters Counters() {
    return sweeper_->TEST_counters();
  }

  // Waits until every scheduled sweeper task has run its callback.
  Status WaitNoOutstanding() {
    return WaitFor([this] { return Counters().outstanding == 0; }, 10s, "No outstanding task");
  }

  Recorder recorder_;
  std::optional<rpc::IoThreadPool> pool_;
  std::optional<rpc::Scheduler> scheduler_;
  std::shared_ptr<RaftAsyncWaitSweeper> sweeper_;
};

// Entries registered in any order fire in due order, each once.
TEST_F(RaftAsyncWaitSweeperTest, FireInDueOrder) {
  ASSERT_OK(Register(3, 300ms));
  ASSERT_OK(Register(1, 100ms));
  ASSERT_OK(Register(4, 400ms));
  ASSERT_OK(Register(2, 200ms));
  ASSERT_OK(recorder_.WaitDue(4));
  ASSERT_EQ(recorder_.due(), (std::vector<int>{1, 2, 3, 4}));
  ASSERT_EQ(sweeper_->TEST_num_pending(), 0);
  // Every entry fired in an expiry of its own: they are 100 ms apart.
  ASSERT_EQ(Counters().expiries, 4);
  ASSERT_EQ(Counters().due, 4);
  SleepFor(200ms);
  ASSERT_EQ(recorder_.due().size(), 4);
}

// The timer is armed only for an entry due before the armed time. A superseded task is aborted
// and, when its callback runs, ignored.
TEST_F(RaftAsyncWaitSweeperTest, RearmOnlyForNewEarliest) {
  const auto start = Clock::now();
  ASSERT_OK(Register(2, 500ms));
  ASSERT_EQ(Counters().scheduled, 1);
  ASSERT_OK(Register(3, 600ms));
  ASSERT_OK(Register(4, 700ms));
  // Not earlier than the armed time.
  ASSERT_EQ(Counters().scheduled, 1);
  ASSERT_EQ(Counters().aborted, 0);
  ASSERT_OK(Register(1, 300ms));
  ASSERT_EQ(Counters().scheduled, 2);
  ASSERT_EQ(Counters().aborted, 1);
  // The aborted task's callback ran with an error and changed nothing.
  ASSERT_OK(WaitFor([this] { return Counters().outstanding == 1; }, 10s, "Aborted task ran"));
  ASSERT_EQ(Counters().expiries, 0);
  ASSERT_TRUE(recorder_.due().empty());
  ASSERT_TRUE(recorder_.shutdown().empty());

  ASSERT_OK(recorder_.WaitDue(4));
  ASSERT_EQ(recorder_.due(), (std::vector<int>{1, 2, 3, 4}));
  ASSERT_GE(recorder_.due_times()[0] - start, 300ms);
  // One task per remaining entry after the first fired: each expiry re-arms for the next.
  ASSERT_OK(WaitNoOutstanding());
  ASSERT_EQ(Counters().scheduled, 5);
  ASSERT_EQ(Counters().aborted, 1);
  ASSERT_EQ(Counters().expiries, 4);
  ASSERT_LE(Counters().max_outstanding, 2);
}

// A removed entry does not fire, whether it was the earliest, in the middle, or the last. Removing
// an entry that already fired or that shutdown took is a no-op.
TEST_F(RaftAsyncWaitSweeperTest, RemovedEntryDoesNotFire) {
  auto first = ASSERT_RESULT(Register(1, 200ms));
  ASSERT_OK(Register(2, 300ms));
  auto middle = ASSERT_RESULT(Register(3, 400ms));
  ASSERT_OK(Register(4, 500ms));
  auto last = ASSERT_RESULT(Register(5, 600ms));
  ASSERT_EQ(sweeper_->TEST_num_pending(), 5);
  sweeper_->Remove(first);
  sweeper_->Remove(middle);
  sweeper_->Remove(last);
  ASSERT_EQ(sweeper_->TEST_num_pending(), 2);
  // Removing needs no scheduler call: the task armed for the removed earliest entry fires with
  // nothing due and re-arms for the next one.
  ASSERT_EQ(Counters().scheduled, 1);
  ASSERT_EQ(Counters().aborted, 0);
  ASSERT_OK(recorder_.WaitDue(2));
  ASSERT_OK(WaitNoOutstanding());
  ASSERT_EQ(recorder_.due(), (std::vector<int>{2, 4}));
  // Fired for 1 (nothing due), 2 and 4; the removed last entry left nothing to re-arm for.
  ASSERT_EQ(Counters().expiries, 3);
  ASSERT_EQ(Counters().scheduled, 3);

  // Already fired.
  auto fired = ASSERT_RESULT(Register(6, 10ms));
  ASSERT_OK(recorder_.WaitDue(3));
  sweeper_->Remove(fired);
  sweeper_->Remove(fired);
  // Already taken by shutdown.
  auto taken = ASSERT_RESULT(Register(7, 1h));
  sweeper_->Shutdown(STATUS(Aborted, "Test shutdown"));
  sweeper_->Remove(taken);
  ASSERT_EQ(recorder_.shutdown(), (std::vector<int>{7}));
  ASSERT_EQ(recorder_.due(), (std::vector<int>{2, 4, 6}));
}

// Entries due together fire in one expiry, in order: the scheduler fires the task later than the
// microseconds between their due times.
TEST_F(RaftAsyncWaitSweeperTest, DueTogetherFireInOneExpiry) {
  for (int i = 0; i != 5; ++i) {
    ASSERT_OK(Register(i, 200ms));
  }
  ASSERT_OK(recorder_.WaitDue(5));
  ASSERT_EQ(recorder_.due(), (std::vector<int>{0, 1, 2, 3, 4}));
  ASSERT_EQ(Counters().expiries, 1);
  ASSERT_EQ(Counters().due, 5);
  ASSERT_EQ(Counters().scheduled, 1);
}

// Sub-millisecond delays fire in order and not before they are due.
TEST_F(RaftAsyncWaitSweeperTest, SubMillisecondDelaysInOrder) {
  const auto start = Clock::now();
  for (int i = 9; i != 0; --i) {
    ASSERT_OK(Register(i, i * 100us));
  }
  ASSERT_OK(recorder_.WaitDue(9));
  ASSERT_EQ(recorder_.due(), (std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 9}));
  auto times = recorder_.due_times();
  for (int i = 0; i != 9; ++i) {
    ASSERT_GE(times[i] - start, (i + 1) * 100us) << "Entry " << i + 1;
  }
}

// Shutdown calls OnShutdown with its status on every pending entry, and nothing fires afterwards.
TEST_F(RaftAsyncWaitSweeperTest, ShutdownFailsPendingEntries) {
  ASSERT_OK(Register(1, 1h));
  ASSERT_OK(Register(2, 200ms));
  ASSERT_OK(Register(3, 2h));
  const auto status = STATUS(Aborted, "Test shutdown");
  sweeper_->Shutdown(status);
  ASSERT_EQ(recorder_.shutdown(), (std::vector<int>{2, 1, 3}));
  for (const auto& entry_status : recorder_.shutdown_statuses()) {
    ASSERT_TRUE(entry_status.IsAborted()) << entry_status;
    ASSERT_STR_CONTAINS(entry_status.ToString(), "Test shutdown");
  }
  ASSERT_EQ(sweeper_->TEST_num_pending(), 0);
  // The task armed for entry 1, superseded by entry 2's, and the one armed at shutdown.
  ASSERT_EQ(Counters().aborted, 2);
  ASSERT_OK(WaitNoOutstanding());
  SleepFor(400ms);
  ASSERT_TRUE(recorder_.due().empty());
  // A second shutdown has no effect.
  sweeper_->Shutdown(STATUS(IllegalState, "Second shutdown"));
  ASSERT_EQ(recorder_.shutdown().size(), 3);
}

// A Register after shutdown stores nothing, calls nothing on the entry and returns the shutdown
// status, for the caller to complete the entry itself.
TEST_F(RaftAsyncWaitSweeperTest, RegisterAfterShutdown) {
  sweeper_->Shutdown(RaftAsyncWaitSweeper::ShutdownStatus());
  auto entry = std::make_shared<TestEntry>(&recorder_, 1);
  std::weak_ptr<TestEntry> weak_entry = entry;
  auto result = sweeper_->Register(std::move(entry), 10ms);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsAborted()) << result.status();
  ASSERT_EQ(Errno::ValueFromStatus(result.status()), ESHUTDOWN) << result.status();
  ASSERT_STR_CONTAINS(result.status().ToString(), "Scheduler is shutting down");
  // Not kept.
  ASSERT_TRUE(weak_entry.expired());
  ASSERT_EQ(sweeper_->TEST_num_pending(), 0);
  ASSERT_EQ(Counters().registered, 0);
  ASSERT_EQ(Counters().scheduled, 0);
  SleepFor(100ms);
  ASSERT_TRUE(recorder_.due().empty());
  ASSERT_TRUE(recorder_.shutdown().empty());
}

// A scheduler that shuts down with the sweeper's task armed shuts the sweeper down with the
// scheduler's status, so no entry is stranded, and later registrations fail with it.
TEST_F(RaftAsyncWaitSweeperTest, SchedulerShutdownShutsSweeperDown) {
  ASSERT_OK(Register(1, 1h));
  ASSERT_OK(Register(2, 2h));
  scheduler_->Shutdown();
  ASSERT_OK(recorder_.WaitShutdown(2));
  ASSERT_EQ(recorder_.shutdown(), (std::vector<int>{1, 2}));
  for (const auto& status : recorder_.shutdown_statuses()) {
    ASSERT_TRUE(status.IsAborted()) << status;
    ASSERT_EQ(Errno::ValueFromStatus(status), ESHUTDOWN) << status;
  }
  ASSERT_EQ(sweeper_->TEST_num_pending(), 0);
  auto result = Register(3, 10ms);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsAborted()) << result.status();
  ASSERT_TRUE(recorder_.due().empty());
}

// An entry registered with no task armed after the scheduler shut down is shut down too: the task
// armed for it runs at once with the scheduler's error.
TEST_F(RaftAsyncWaitSweeperTest, RegisterAfterSchedulerShutdown) {
  scheduler_->Shutdown();
  ASSERT_OK(Register(1, 1h));
  ASSERT_OK(recorder_.WaitShutdown(1));
  ASSERT_TRUE(recorder_.shutdown_statuses()[0].IsAborted()) << recorder_.shutdown_statuses()[0];
  ASSERT_EQ(sweeper_->TEST_num_pending(), 0);
}

// The sweeper keeps an entry alive while it is pending, even with no other reference to it, and
// releases it once it has fired.
TEST_F(RaftAsyncWaitSweeperTest, SweeperKeepsWaitAlive) {
  auto entry = std::make_shared<TestEntry>(&recorder_, 1);
  std::weak_ptr<TestEntry> weak_entry = entry;
  ASSERT_OK(sweeper_->Register(std::move(entry), 300ms));
  ASSERT_FALSE(weak_entry.expired());
  SleepFor(100ms);
  ASSERT_FALSE(weak_entry.expired());
  ASSERT_OK(recorder_.WaitDue(1));
  ASSERT_OK(WaitFor([&weak_entry] { return weak_entry.expired(); }, 10s, "Entry released"));
  ASSERT_EQ(recorder_.due(), std::vector<int>{1});
}

// An entry whose destructor calls back into the sweeper does not deadlock when its last reference
// is dropped by Remove, by an expiry or by Shutdown.
TEST_F(RaftAsyncWaitSweeperTest, SweeperDropsReferenceOutsideMutex) {
  std::atomic<int> destroyed{0};
  auto make_entry = [this, &destroyed](int id) {
    return std::make_shared<ReentrantEntry>(&recorder_, id, sweeper_.get(), &destroyed);
  };
  TestThreadHolder thread_holder;
  CountDownLatch done(1);
  thread_holder.AddThreadFunctor([this, &make_entry, &destroyed, &done] {
    // Remove.
    auto handle = ASSERT_RESULT(sweeper_->Register(make_entry(1), 1h));
    ASSERT_EQ(destroyed.load(), 0);
    sweeper_->Remove(handle);
    ASSERT_EQ(destroyed.load(), 1);

    // Expiry, on the scheduler's thread.
    ASSERT_OK(sweeper_->Register(make_entry(2), 10ms));
    ASSERT_OK(WaitFor([&destroyed] { return destroyed.load() == 2; }, 10s, "Expired entry gone"));
    ASSERT_EQ(recorder_.due(), std::vector<int>{2});

    // Shutdown.
    ASSERT_OK(sweeper_->Register(make_entry(3), 1h));
    sweeper_->Shutdown(STATUS(Aborted, "Test shutdown"));
    ASSERT_EQ(destroyed.load(), 3);
    done.CountDown();
  });
  ASSERT_TRUE(done.WaitFor(30s)) << "Deadlocked on the sweeper mutex";
  thread_holder.JoinAll();
  // The entries each destructor registered: two before the shutdown took them, one rejected.
  ASSERT_EQ(Counters().registered, 3 + 2);
}

// A task still armed when its sweeper is destroyed does nothing when it fires. A sweeper destroyed
// without a shutdown still shuts its pending entries down rather than dropping them.
TEST_F(RaftAsyncWaitSweeperTest, TaskOutlivesSweeper) {
  auto handle = ASSERT_RESULT(Register(1, 100ms));
  // Remove leaves the task armed.
  sweeper_->Remove(handle);
  ASSERT_EQ(Counters().outstanding, 1);
  std::weak_ptr<RaftAsyncWaitSweeper> weak_sweeper = sweeper_;
  sweeper_.reset();
  ASSERT_TRUE(weak_sweeper.expired());
  SleepFor(300ms);
  ASSERT_TRUE(recorder_.due().empty());

  CreateSweeper();
  ASSERT_OK(Register(2, 100ms));
  sweeper_.reset();
  ASSERT_EQ(recorder_.shutdown(), std::vector<int>{2});
  ASSERT_TRUE(recorder_.shutdown_statuses()[0].IsAborted()) << recorder_.shutdown_statuses()[0];
  SleepFor(300ms);
  ASSERT_TRUE(recorder_.due().empty());
}

} // namespace yb::consensus
