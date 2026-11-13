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

#include <algorithm>
#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "yb/util/logging.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "yb/server/hybrid_clock.h"

#include "yb/util/atomic.h"
#include "yb/util/monotime.h"
#include "yb/util/random.h"
#include "yb/util/random_util.h"
#include "yb/util/status_log.h"
#include "yb/util/test_util.h"
#include "yb/util/thread.h"

using std::vector;

DECLARE_uint64(max_clock_sync_error_usec);
DECLARE_bool(disable_clock_sync_error);

namespace yb {
namespace server {

class HybridClockTest : public YBTest {
 public:
  HybridClockTest()
      : clock_(new HybridClock()) {
  }

  void SetUp() override {
    YBTest::SetUp();
    ASSERT_OK(clock_->Init());
  }

 protected:
  void RunMultiThreadedTest(int num_reads_per_update);

  scoped_refptr<HybridClock> clock_;
};

TEST(MockHybridClockTest, TestMockedSystemClock) {
  ASSERT_EQ(kMaxHybridTimePhysicalMicros, HybridTime::kMax.GetPhysicalValueMicros());
  MockClock mock_clock;
  scoped_refptr<HybridClock> clock(new HybridClock(mock_clock.AsClock()));
  ASSERT_OK(clock->Init());
  HybridTime hybrid_time;
  uint64_t max_error_usec;
  clock->NowWithError(&hybrid_time, &max_error_usec);
  ASSERT_EQ(hybrid_time.ToUint64(), 0);
  ASSERT_EQ(max_error_usec, 0);
  // If we read the clock again we should see the logical component be incremented.
  clock->NowWithError(&hybrid_time, &max_error_usec);
  ASSERT_EQ(hybrid_time.ToUint64(), 1);
  // Now set an arbitrary time and check that is the time returned by the clock.
  PhysicalTime time = {1234, 100 * 1000};
  mock_clock.Set(time);
  clock->NowWithError(&hybrid_time, &max_error_usec);
  ASSERT_EQ(hybrid_time.ToUint64(),
            HybridTime::FromMicrosecondsAndLogicalValue(time.time_point, 0).ToUint64());
  ASSERT_EQ(max_error_usec, time.max_error);
  // Perform another read, we should observe the logical component increment, again.
  clock->NowWithError(&hybrid_time, &max_error_usec);
  ASSERT_EQ(hybrid_time.ToUint64(),
            HybridTime::FromMicrosecondsAndLogicalValue(time.time_point, 1).ToUint64());
}

// Test that two subsequent time reads are monotonically increasing.
TEST_F(HybridClockTest, TestNow_ValuesIncreaseMonotonically) {
  const HybridTime now1 = clock_->Now();
  const HybridTime now2 = clock_->Now();
  ASSERT_LT(now1.value(), now2.value());
}

// Concurrent readers never get the same hybrid time, and each reader's values only increase.
TEST_F(HybridClockTest, TestNow_ConcurrentValuesAreUniqueAndIncreasePerThread) {
  constexpr int kThreads = 8;
  constexpr int kReadsPerThread = 200000;
  std::vector<std::vector<uint64_t>> values(kThreads);
  std::vector<std::thread> threads;
  for (int i = 0; i != kThreads; ++i) {
    threads.emplace_back([this, &out = values[i]] {
      out.reserve(kReadsPerThread);
      for (int n = 0; n != kReadsPerThread; ++n) {
        out.push_back(clock_->Now().ToUint64());
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  std::vector<uint64_t> all;
  all.reserve(kThreads * kReadsPerThread);
  for (const auto& thread_values : values) {
    ASSERT_TRUE(std::adjacent_find(
        thread_values.begin(), thread_values.end(), std::greater_equal<uint64_t>()) ==
            thread_values.end()) << "A reader saw a value that did not increase";
    all.insert(all.end(), thread_values.begin(), thread_values.end());
  }
  std::sort(all.begin(), all.end());
  ASSERT_TRUE(std::adjacent_find(all.begin(), all.end()) == all.end())
      << "Two readers got the same hybrid time";
}

// An update to a later time is never undone by concurrent readers: after Update(t) returns, every
// Now() is above t.
TEST_F(HybridClockTest, TestUpdate_HoldsAgainstConcurrentReaders) {
  std::atomic<bool> stop{false};
  std::vector<std::thread> readers;
  for (int i = 0; i != 4; ++i) {
    readers.emplace_back([this, &stop] {
      while (!stop.load(std::memory_order_relaxed)) {
        clock_->Now();
      }
    });
  }
  for (int n = 0; n != 20000; ++n) {
    auto target = clock_->Now().AddMicroseconds(50);
    clock_->Update(target);
    ASSERT_GT(clock_->Now().ToUint64(), target.ToUint64());
  }
  stop = true;
  for (auto& reader : readers) {
    reader.join();
  }
}

// Tests the clock updates with the incoming value if it is higher.
TEST_F(HybridClockTest, TestUpdate_LogicalValueIncreasesByAmount) {
  HybridTime now = clock_->Now();
  uint64_t now_micros = now.GetPhysicalValueMicros();

  // increase the logical value
  auto logical = now.GetLogicalValue();
  logical += 10;

  // increase the physical value so that we're sure the clock will take this
  // one, 200 msecs should be more than enough.
  now_micros += 200000;

  HybridTime now_increased = HybridTime::FromMicrosecondsAndLogicalValue(now_micros, logical);

  clock_->Update(now_increased);

  HybridTime now2 = clock_->Now();
  ASSERT_EQ(logical + 1, now2.GetLogicalValue());
  ASSERT_EQ(now.GetPhysicalValueMicros() + 200000,
            now2.GetPhysicalValueMicros());
}

// Thread which loops polling the clock and updating it slightly
// into the future.
void StresserThread(HybridClock* clock, AtomicBool* stop, int num_reads_per_update) {
  Random rng(GetRandomSeed32());
  HybridTime prev = HybridTime::kMin;
  while (!stop->Load()) {
    HybridTime t;
    for (int i = 0; i < num_reads_per_update; ++i) {
      t = clock->Now();
      ASSERT_GT(t, prev);
      prev = t;
    }

    // Add a random bit of offset to the clock, and perform an update.
    HybridTime new_ht = t.AddDelta(MonoDelta::FromMicroseconds(rng.Uniform(10000)));
    clock->Update(new_ht);
    prev = new_ht;
  }
}

// Regression test for KUDU-953: if threads are updating and polling the
// clock concurrently, the clock should still never run backwards.
TEST_F(HybridClockTest, TestClockDoesntGoBackwardsWithUpdates) {
  RunMultiThreadedTest(1);
}

TEST_F(HybridClockTest, TestClockDoesntGoBackwardsWithOccasionalUpdates) {
  RunMultiThreadedTest(1000000);
}

void HybridClockTest::RunMultiThreadedTest(int num_reads_per_update) {
  vector<scoped_refptr<yb::Thread> > threads;

  AtomicBool stop(false);
  for (int i = 0; i < 4; i++) {
    scoped_refptr<Thread> thread;
    ASSERT_OK(Thread::Create("test", "stresser",
                             &StresserThread, clock_.get(), &stop, num_reads_per_update,
                             &thread));
    threads.push_back(thread);
  }

  SleepFor(MonoDelta::FromSeconds(10));
  stop.Store(true);
  for (const scoped_refptr<Thread>& t : threads) {
    t->Join();
  }
}

TEST_F(HybridClockTest, CompareHybridClocksToDelta) {
  EXPECT_EQ(1, CompareHybridTimesToDelta(
      HybridTime::FromMicrosecondsAndLogicalValue(1000, 10),
      HybridTime::FromMicrosecondsAndLogicalValue(1002, 10),
      MonoDelta::FromMicroseconds(1)));

  EXPECT_EQ(-1, CompareHybridTimesToDelta(
      HybridTime::FromMicrosecondsAndLogicalValue(1000, 10),
      HybridTime::FromMicrosecondsAndLogicalValue(1002, 10),
      MonoDelta::FromMicroseconds(5)));

  EXPECT_EQ(0, CompareHybridTimesToDelta(
      HybridTime::FromMicrosecondsAndLogicalValue(1000, 10),
      HybridTime::FromMicrosecondsAndLogicalValue(1001, 10),
      MonoDelta::FromMicroseconds(1)));

  EXPECT_EQ(1, CompareHybridTimesToDelta(
      HybridTime::FromMicrosecondsAndLogicalValue(1000, 10),
      HybridTime::FromMicrosecondsAndLogicalValue(1001, 11),
      MonoDelta::FromMicroseconds(1)));

  EXPECT_EQ(-1, CompareHybridTimesToDelta(
      HybridTime::FromMicrosecondsAndLogicalValue(1000, 10),
      HybridTime::FromMicrosecondsAndLogicalValue(1001, 9),
      MonoDelta::FromMicroseconds(1)));

  EXPECT_EQ(-1, CompareHybridTimesToDelta(
      HybridTime::FromMicrosecondsAndLogicalValue(1000, 10),
      HybridTime::FromMicrosecondsAndLogicalValue(1000, 10),
      MonoDelta::FromNanoseconds(MonoTime::kNanosecondsPerMicrosecond - 1)));

  EXPECT_EQ(-1, CompareHybridTimesToDelta(
      HybridTime::FromMicrosecondsAndLogicalValue(1000, 10),
      HybridTime::FromMicrosecondsAndLogicalValue(1001, 10),
      MonoDelta::FromNanoseconds(MonoTime::kNanosecondsPerMicrosecond + 1)));

  ASSERT_NO_FATALS(HybridTime(std::numeric_limits<uint64_t>::max()).GetPhysicalValueNanos());

  EXPECT_EQ(-1, CompareHybridTimesToDelta(
      HybridTime::FromMicrosecondsAndLogicalValue(1000, 10),
      HybridTime::FromMicrosecondsAndLogicalValue(1000, 9),
      MonoDelta::FromMicroseconds(1)));
}

namespace {

size_t CountOccurrences(
    const std::string& haystack, const std::string& needle) {
  size_t count = 0;
  for (auto pos = haystack.find(needle); pos != std::string::npos;
       pos = haystack.find(needle, pos + needle.size())) {
    ++count;
  }
  return count;
}

} // anonymous namespace

// Regression test for #33606.
TEST(ClockDiagnosticsTest, CollectedOnceWhenClockUnsynchronized) {
  auto read_clock_concurrently = [] {
    FLAGS_logtostderr = true;
    MockClock mock_clock;
    mock_clock.Set(STATUS(ServiceUnavailable, "Clock unsynchronized"));
    scoped_refptr<HybridClock> clock(new HybridClock(mock_clock.AsClock()));
    CHECK_OK(clock->Init());

    constexpr int kNumClockReaders = 8;
    std::vector<scoped_refptr<Thread>> readers;
    for (int i = 0; i < kNumClockReaders; ++i) {
      scoped_refptr<Thread> reader;
      CHECK_OK(Thread::Create("test", "clock_reader", [&clock]() {
        HybridTime hybrid_time;
        uint64_t max_error_usec;
        clock->NowWithError(&hybrid_time, &max_error_usec);
      }, &reader));
      readers.push_back(reader);
    }
    for (const auto& reader : readers) {
      reader->Join();
    }
  };

  auto collected_once = [](const std::string& output) {
    return CountOccurrences(output, "chronyc tracking") == 1;
  };

  ASSERT_DEATH(read_clock_concurrently(), ::testing::Truly(collected_once));
}

}  // namespace server
}  // namespace yb
