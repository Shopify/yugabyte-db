// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugabyteDB development.
//
// Portions Copyright (c) YugabyteDB, Inc.
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
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "yb/util/background_task.h"
#include "yb/util/backoff_waiter.h"
#include "yb/util/format.h"
#include "yb/util/result.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"
#include "yb/util/tsan_util.h"

using namespace std::chrono_literals;

namespace yb {

class BackgroundTaskTest : public YBTest {
 public:
  BackgroundTaskTest() : YBTest(), run_count_(0) {}

  std::unique_ptr<BackgroundTask> GetTask(std::chrono::milliseconds timeout = 0s) {
    auto task = std::make_unique<BackgroundTask>([this]() {
      run_count_.fetch_add(1);
    }, "test", "test", timeout);
    EXPECT_OK(task->Init());
    return task;
  }

 protected:
  std::atomic<int> run_count_;
};

TEST_F(BackgroundTaskTest, RunsTaskOnWake) {
  constexpr int kNumTaskRuns = 1000;
  auto bg_task = GetTask();

  for (int i = 1; i < kNumTaskRuns; ++i) {
    EXPECT_OK(bg_task->Wake());
    EXPECT_OK(WaitFor([this, i]() {
      return run_count_ == i;
    }, 1s * kTimeMultiplier, Format("Wait for i-th ($0) task run.", i)));
  }

  bg_task->StartShutdown();
  bg_task->CompleteShutdown();
}

TEST_F(BackgroundTaskTest, YB_DISABLE_TEST_ON_MACOS(RunsTaskOnInterval)) {
  constexpr int kNumTaskRuns = 1000;

  auto interval = 10ms * kTimeMultiplier;
  auto bg_task = GetTask(interval);

  for (int i = 1; i < kNumTaskRuns; ++i) {
    std::this_thread::sleep_for(interval);
    EXPECT_OK(WaitFor([this, i]() {
      return run_count_ >= i;
    }, interval / 10, Format("Wait for i-th ($0) task run.", i)));
  }

  bg_task->StartShutdown();
  bg_task->CompleteShutdown();
}

// Wake() from many threads, also while the task is running and while a run is pending: every
// wake-up is followed by a run that starts after it, and concurrent wake-ups share runs.
TEST_F(BackgroundTaskTest, ConcurrentWakesAreNotLost) {
  constexpr int kThreads = 8;
  constexpr int kWakesPerThread = 2000;
  std::atomic<int64_t> runs_started{0};
  BackgroundTask task([&runs_started] {
    runs_started.fetch_add(1);
    // Long enough for wake-ups to arrive during a run.
    std::this_thread::sleep_for(20us);
  }, "test", "test");
  ASSERT_OK(task.Init());

  std::atomic<int> lost{0};
  std::vector<std::thread> threads;
  for (int i = 0; i != kThreads; ++i) {
    threads.emplace_back([&] {
      for (int n = 0; n != kWakesPerThread; ++n) {
        const auto started_before = runs_started.load();
        ASSERT_OK(task.Wake());
        // A run that starts after Wake() returned is counted at once: a run that was in progress
        // when Wake() was called does not count, so wait for two more starts at most.
        const auto deadline = std::chrono::steady_clock::now() + 10s * kTimeMultiplier;
        while (runs_started.load() == started_before) {
          if (std::chrono::steady_clock::now() > deadline) {
            lost.fetch_add(1);
            return;
          }
          std::this_thread::yield();
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  ASSERT_EQ(lost.load(), 0);
  ASSERT_LE(runs_started.load(), kThreads * kWakesPerThread + 1);

  // Wake-ups during one long run cost one more run, not one each.
  std::atomic<bool> release{false};
  BackgroundTask slow([&] {
    run_count_.fetch_add(1);
    while (!release.load()) {
      std::this_thread::sleep_for(1ms);
    }
  }, "test", "slow");
  ASSERT_OK(slow.Init());
  ASSERT_OK(slow.Wake());
  ASSERT_OK(WaitFor([this] { return run_count_ == 1; }, 10s * kTimeMultiplier, "First run"));
  for (int i = 0; i != 1000; ++i) {
    ASSERT_OK(slow.Wake());
  }
  release = true;
  ASSERT_OK(WaitFor([this] { return run_count_ == 2; }, 10s * kTimeMultiplier, "Second run"));
  std::this_thread::sleep_for(50ms);
  ASSERT_EQ(run_count_, 2);

  slow.Shutdown();
  ASSERT_TRUE(slow.Wake().IsShutdownInProgress());
  task.Shutdown();
}

class BackgroundTaskShutdownTest :
    public BackgroundTaskTest, public ::testing::WithParamInterface<std::chrono::milliseconds> {};

TEST_P(BackgroundTaskShutdownTest, InitAndShutdown) {
  auto interval = GetParam() * kTimeMultiplier;
  auto bg_task = GetTask(interval);
  std::this_thread::sleep_for(1s * kTimeMultiplier);
  bg_task->StartShutdown();
  bg_task->CompleteShutdown();
}

INSTANTIATE_TEST_CASE_P(
    InitAndShutdown, BackgroundTaskShutdownTest, ::testing::Values(0s, 20ms, 5s));

}  // namespace yb
