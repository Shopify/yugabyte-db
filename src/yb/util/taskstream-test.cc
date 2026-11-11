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
#include <functional>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

#include "yb/util/flags.h"
#include "yb/util/logging.h"
#include <gtest/gtest.h>

#include "yb/gutil/atomicops.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/blocking_queue.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/scope_exit.h"
#include "yb/util/status_fwd.h"
#include "yb/util/taskstream.h"
#include "yb/util/test_macros.h"
#include "yb/util/threadpool.h"

using std::atomic;
using std::string;
using std::unique_ptr;

DECLARE_bool(enable_tracing);

namespace yb {

namespace {

static Status BuildMinMaxTestPool(
    int min_threads, int max_threads, std::unique_ptr<ThreadPool> *pool) {
  return ThreadPoolBuilder("test").set_min_threads(min_threads)
      .set_max_threads(max_threads)
      .Build(pool);
}

} // anonymous namespace

class TestTaskStream : public ::testing::Test {
 public:
  TestTaskStream() {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_tracing) = true;
  }
 protected:
  const int32_t kTaskstreamQueueMaxSize = 100000;
  const MonoDelta kTaskstreamQueueMaxWait = MonoDelta::FromMilliseconds(1000);
};

static void SimpleTaskStreamMethod(int* value, std::atomic<int32_t>* counter) {
  if (value == nullptr) {
    return;
  }
  int n = *value;
  while (n--) {
    (*counter)++;
  }
}

TEST_F(TestTaskStream, TestSimpleTaskStream) {
  using namespace std::placeholders;
  std::unique_ptr<ThreadPool> thread_pool;
  ASSERT_OK(BuildMinMaxTestPool(1, 1, &thread_pool));

  std::atomic<int32_t> counter(0);
  std::function<void (int*)> f1 = std::bind(&SimpleTaskStreamMethod, _1, &counter);

  TaskStream<int> taskStream(f1, thread_pool.get(), kTaskstreamQueueMaxSize,
                             kTaskstreamQueueMaxWait);
  ASSERT_OK(taskStream.Start());
  int a[4] = {10, 9, 8, 7};
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(taskStream.Submit(&a[i]));
  }
  thread_pool->Wait();
  ASSERT_EQ(34, counter.load(std::memory_order_acquire));
  taskStream.Stop();
  thread_pool->Shutdown();
}

TEST_F(TestTaskStream, TestTwoTaskStreams) {
  using namespace std::placeholders;
  std::unique_ptr<ThreadPool> thread_pool;
  ASSERT_OK(BuildMinMaxTestPool(1, 1, &thread_pool));

  std::atomic<int32_t> counter0(0);
  std::atomic<int32_t> counter1(0);
  std::function<void (int*)> f0 = std::bind(&SimpleTaskStreamMethod, _1, &counter0);
  std::function<void (int*)> f1 = std::bind(&SimpleTaskStreamMethod, _1, &counter1);

  TaskStream<int> taskStream0(f0, thread_pool.get(), kTaskstreamQueueMaxSize,
                              kTaskstreamQueueMaxWait);
  TaskStream<int> taskStream1(f1, thread_pool.get(), kTaskstreamQueueMaxSize,
                              kTaskstreamQueueMaxWait);
  ASSERT_OK(taskStream0.Start());
  ASSERT_OK(taskStream1.Start());
  int a[4] = {10, 9, 8, 7};
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(taskStream0.Submit(&a[i]));
  }
  int b[5] = {1, 2, 3, 4, 5};
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(taskStream1.Submit(&b[i]));
  }
  thread_pool->Wait();
  ASSERT_EQ(34, counter0.load(std::memory_order_acquire));
  ASSERT_EQ(15, counter1.load(std::memory_order_acquire));
  taskStream0.Stop();
  taskStream1.Stop();
  thread_pool->Shutdown();
}
namespace {

// Process function for lane tests: counts items and the lane they ran in, and blocks on items
// equal to kBlockingItem until released.
class LaneTestItems {
 public:
  static constexpr int kBlockingItem = -1;

  std::function<void(int*)> ProcessFunction() {
    return [this](int* item) {
      if (!item) {
        ++groups_;
        return;
      }
      lane_ = ThisThreadLane();
      if (*item == kBlockingItem) {
        started_.CountDown();
        release_.Wait();
      }
      ++processed_;
    };
  }

  Status WaitProcessed(int count) {
    return WaitFor(
        [this, count] { return processed_.load() >= count; }, MonoDelta::FromSeconds(10),
        "Items processed");
  }

  int processed() const { return processed_.load(); }
  int groups() const { return groups_.load(); }
  LaneId lane() const { return lane_.load(); }
  CountDownLatch& started() { return started_; }
  CountDownLatch& release() { return release_; }

 private:
  std::atomic<int> processed_{0};
  std::atomic<int> groups_{0};
  std::atomic<LaneId> lane_{kNoLane};
  CountDownLatch started_{1};
  CountDownLatch release_{1};
};

void SubmitFrom(TaskStream<int>* stream, int* item, const std::function<void()>& confine) {
  RunOnStdThread([stream, item, &confine] {
    confine();
    ASSERT_OK(stream->Submit(item));
  });
}

auto InLane(LaneId lane) {
  return [lane] { ApplyLaneToCurrentThread(lane); };
}

std::unique_ptr<ThreadPool> BuildLanePool() {
  std::unique_ptr<ThreadPool> pool;
  CHECK_OK(ThreadPoolBuilder("lane0")
      .set_max_threads(1)
      .set_lane(0)
      .set_crossing_category(LaneCrossingCategory::kTabletPipeline)
      .Build(&pool));
  return pool;
}

} // namespace

// Each item counts its own handoff once: the task that starts draining the stream is not counted,
// and neither is the end-of-group call with no item.
TEST_F(TestTaskStream, CountsLaneHandoffsPerItem) {
  auto topology = MakeTwoLaneTestTopology();
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);
  auto pool = BuildLanePool();
  LaneTestItems items;
  {
    // A short wait, so the stream goes idle after each item and every submit starts a new drain
    // task on the pool.
    TaskStream<int> stream(items.ProcessFunction(), pool.get(), kTaskstreamQueueMaxSize,
                           MonoDelta::FromMilliseconds(1));
    int value = 1;
    auto submit_and_drain = [&](const std::function<void()>& confine, int expected) {
      SubmitFrom(&stream, &value, confine);
      ASSERT_OK(items.WaitProcessed(expected));
      ASSERT_TRUE(pool->WaitFor(MonoDelta::FromSeconds(10)));
    };

    auto counts = LaneHandoffCounts::Now();
    ASSERT_NO_FATALS(submit_and_drain(InLane(0), 1));
    ASSERT_EQ(items.lane(), 0);
    auto delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.total_crossings(), 0) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 0);

    ASSERT_NO_FATALS(submit_and_drain(InLane(1), 2));
    delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.crossing(LaneCrossingCategory::kTabletPipeline), 1) << delta.ToString();
    ASSERT_EQ(delta.total_crossings(), 1);
    ASSERT_EQ(delta.entries_from_unlaned, 0);

    ASSERT_NO_FATALS(submit_and_drain([] { ApplyServiceMaskToCurrentThread(); }, 3));
    delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.total_crossings(), 1) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 1);
    ASSERT_GE(items.groups(), 3);
    stream.Stop();
  }
  pool->Shutdown();
}

// Items picked up by a stream that is already running, with no pool enqueue, are counted too.
TEST_F(TestTaskStream, CountsLaneHandoffsWhileRunning) {
  auto topology = MakeTwoLaneTestTopology();
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);
  auto pool = BuildLanePool();
  LaneTestItems items;
  {
    // A long wait keeps the stream parked on its queue between items.
    TaskStream<int> stream(items.ProcessFunction(), pool.get(), kTaskstreamQueueMaxSize,
                           MonoDelta::FromSeconds(60));
    int value = 1;
    int blocking = LaneTestItems::kBlockingItem;
    auto counts = LaneHandoffCounts::Now();
    SubmitFrom(&stream, &value, InLane(0));
    ASSERT_OK(items.WaitProcessed(1));

    // Parked: drained without a pool enqueue.
    SubmitFrom(&stream, &value, InLane(1));
    ASSERT_OK(items.WaitProcessed(2));
    auto delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.crossing(LaneCrossingCategory::kTabletPipeline), 1) << delta.ToString();
    ASSERT_EQ(delta.total_crossings(), 1);

    // Processing: submitted while another item is running.
    SubmitFrom(&stream, &blocking, InLane(1));
    items.started().Wait();
    SubmitFrom(&stream, &value, InLane(1));
    SubmitFrom(&stream, &value, InLane(0));
    items.release().CountDown();
    ASSERT_OK(items.WaitProcessed(5));
    delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.crossing(LaneCrossingCategory::kTabletPipeline), 3) << delta.ToString();
    ASSERT_EQ(delta.total_crossings(), 3);
    ASSERT_EQ(delta.entries_from_unlaned, 0);
    stream.Stop();
  }
  pool->Shutdown();
}

TEST_F(TestTaskStream, CountsNothingWithLanesOff) {
  TEST_SetLaneTopology scope{LaneTopology()};
  auto pool = BuildLanePool();
  LaneTestItems items;
  {
    TaskStream<int> stream(items.ProcessFunction(), pool.get(), kTaskstreamQueueMaxSize,
                           kTaskstreamQueueMaxWait);
    int value = 1;
    const auto calls = TEST_CpuAffinityCallCount();
    auto counts = LaneHandoffCounts::Now();
    // Even a submitter carrying a lane counts nothing while lane mode is off.
    SubmitFrom(&stream, &value, [] { SetThisThreadLane(1); });
    ASSERT_OK(items.WaitProcessed(1));
    ASSERT_EQ(items.lane(), kNoLane);
    auto delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.total_crossings(), 0) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 0);
    ASSERT_EQ(TEST_CpuAffinityCallCount(), calls);
    stream.Stop();
  }
  pool->Shutdown();
}

} // namespace yb
