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
#include <chrono>
#include <functional>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

#include "yb/util/lane_sharded_rw_mutex.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/shared_lock.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"

using namespace std::literals;

namespace yb {

class LaneShardedRwMutexTest : public YBTest {
 protected:
  // Runs f on a thread of the given lane (kNoLane: the service mask) and reports whether it
  // finished within the wait; the thread is joined either way once `release` lets f return.
  struct Probe {
    std::thread thread;
    std::atomic<bool> done{false};

    void Start(LaneId lane, std::function<void()> f) {
      thread = std::thread([this, lane, f = std::move(f)] {
        if (lane == kNoLane) {
          ApplyServiceMaskToCurrentThread();
        } else {
          ApplyLaneToCurrentThread(lane);
        }
        f();
        done = true;
      });
    }

    bool DoneWithin(std::chrono::milliseconds wait) {
      auto deadline = std::chrono::steady_clock::now() + wait;
      while (!done && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
      }
      return done;
    }
  };
};

// With lane mode off every thread shares the spare slot: a writer excludes readers.
TEST_F(LaneShardedRwMutexTest, LaneModeOffIsOneMutex) {
  TEST_SetLaneTopology scope;
  ASSERT_FALSE(LaneModeActive());
  ASSERT_EQ(LaneShardedRwMutex::SlotIndex(), kMaxLanes);

  LaneShardedRwMutex mutex;
  Probe reader;
  {
    std::lock_guard lock(mutex);
    ASSERT_TRUE(mutex.is_write_locked());
    reader.Start(kNoLane, [&] { SharedLock lock(mutex); });
    ASSERT_FALSE(reader.DoneWithin(100ms));
  }
  ASSERT_TRUE(reader.DoneWithin(5000ms));
  reader.thread.join();
  ASSERT_FALSE(mutex.is_write_locked());
}

// Readers of different lanes use different slots and do not wait for each other; a writer waits
// for readers of every lane and, while it holds, excludes readers of every lane.
TEST_F(LaneShardedRwMutexTest, ReadersShardByLaneWritersLockAll) {
  auto topology = MakeTwoLaneTestTopology();
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);
  ASSERT_TRUE(LaneModeActive());

  LaneShardedRwMutex mutex;
  std::atomic<bool> release0{false};
  std::atomic<size_t> slot0{0}, slot1{0};

  Probe reader0;
  reader0.Start(0, [&] {
    slot0 = LaneShardedRwMutex::SlotIndex();
    SharedLock lock(mutex);
    while (!release0) {
      std::this_thread::sleep_for(1ms);
    }
  });
  // A lane-1 reader completes while lane 0 holds its slot.
  Probe reader1;
  reader1.Start(1, [&] {
    slot1 = LaneShardedRwMutex::SlotIndex();
    SharedLock lock(mutex);
  });
  ASSERT_TRUE(reader1.DoneWithin(5000ms));
  reader1.thread.join();
  ASSERT_EQ(slot0, 0);
  ASSERT_EQ(slot1, 1);

  // A writer waits for the lane-0 reader.
  std::atomic<bool> release_writer{false};
  std::atomic<bool> writer_holds{false};
  Probe writer;
  writer.Start(kNoLane, [&] {
    std::lock_guard lock(mutex);
    writer_holds = true;
    while (!release_writer) {
      std::this_thread::sleep_for(1ms);
    }
  });
  std::this_thread::sleep_for(100ms);
  ASSERT_FALSE(writer_holds);
  release0 = true;
  reader0.thread.join();
  while (!writer_holds) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(mutex.is_write_locked());

  // While the writer holds, a reader of lane 1 is excluded.
  Probe reader1_again;
  reader1_again.Start(1, [&] { SharedLock lock(mutex); });
  ASSERT_FALSE(reader1_again.DoneWithin(100ms));
  release_writer = true;
  writer.thread.join();
  ASSERT_TRUE(reader1_again.DoneWithin(5000ms));
  reader1_again.thread.join();
  ASSERT_FALSE(mutex.is_write_locked());
}

// Many readers across lanes and a few writers keep a guarded counter consistent.
TEST_F(LaneShardedRwMutexTest, Stress) {
  auto topology = MakeTwoLaneTestTopology();
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);

  LaneShardedRwMutex mutex;
  int64_t guarded = 0;
  std::atomic<bool> stop{false};
  std::atomic<int64_t> reads{0};
  std::vector<std::thread> readers;
  for (int i = 0; i < 6; ++i) {
    readers.emplace_back([&, i] {
      ApplyLaneToCurrentThread(static_cast<LaneId>(i % 2));
      while (!stop) {
        SharedLock lock(mutex);
        ASSERT_GE(guarded, 0);
        ++reads;
      }
    });
  }
  std::vector<std::thread> writers;
  for (int i = 0; i < 2; ++i) {
    writers.emplace_back([&] {
      ApplyServiceMaskToCurrentThread();
      for (int n = 0; n < 2000; ++n) {
        std::lock_guard lock(mutex);
        ++guarded;
      }
    });
  }
  for (auto& t : writers) {
    t.join();
  }
  stop = true;
  for (auto& t : readers) {
    t.join();
  }
  ASSERT_EQ(guarded, 4000);
  ASSERT_GT(reads, 0);
}

}  // namespace yb
