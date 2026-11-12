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

#include <gtest/gtest.h>

#include "yb/gutil/thread_annotations.h"

#include "yb/util/lane_sharded_rw_mutex.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/shared_lock.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"

namespace yb {

class LaneShardedRwMutexTest : public YBTest {};

namespace {

// try_lock as a scoped acquisition, so the thread-safety analysis sees the matching unlock.
class SCOPED_CAPABILITY TryWriteLock {
 public:
  explicit TryWriteLock(LaneShardedRwMutex& mutex) TRY_ACQUIRE(true, mutex)  // NOLINT
      : mutex_(mutex), locked_(mutex.try_lock()) {}
  ~TryWriteLock() RELEASE() {
    if (locked_) {
      mutex_.unlock();
    }
  }
  bool locked() const { return locked_; }

 private:
  LaneShardedRwMutex& mutex_;
  const bool locked_;
};

}  // namespace

// With lane mode off every thread shares the spare slot: a writer excludes readers and vice versa.
TEST_F(LaneShardedRwMutexTest, LaneModeOffIsOneMutex) {
  TEST_SetLaneTopology scope;
  ASSERT_FALSE(LaneModeActive());
  ASSERT_EQ(LaneShardedRwMutex::SlotIndex(), kMaxLanes);

  LaneShardedRwMutex mutex;
  {
    SharedLock lock(mutex);
    TryWriteLock write(mutex);
    ASSERT_FALSE(write.locked());
    ASSERT_FALSE(mutex.is_write_locked());
  }
  {
    TryWriteLock write(mutex);
    ASSERT_TRUE(write.locked());
    ASSERT_TRUE(mutex.is_write_locked());
  }
  ASSERT_FALSE(mutex.is_write_locked());
}

// Readers of different lanes use different slots; a writer still excludes all of them.
TEST_F(LaneShardedRwMutexTest, ReadersShardByLaneWritersLockAll) {
  auto topology = MakeTwoLaneTestTopology();
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);
  ASSERT_TRUE(LaneModeActive());

  LaneShardedRwMutex mutex;
  std::atomic<bool> lane0_holds{false};
  std::atomic<bool> release{false};
  std::atomic<size_t> lane1_slot{0};

  // A lane-0 reader holds its slot until told to release.
  std::thread reader0([&] {
    ApplyLaneToCurrentThread(0);
    ASSERT_EQ(LaneShardedRwMutex::SlotIndex(), 0);
    SharedLock lock(mutex);
    lane0_holds = true;
    while (!release) {
      std::this_thread::yield();
    }
  });
  while (!lane0_holds) {
    std::this_thread::yield();
  }

  // A lane-1 reader takes a different slot: try_lock on the whole mutex fails because of lane 0,
  // but the lane-1 reader is not blocked by it.
  RunOnStdThread([&] {
    ApplyLaneToCurrentThread(1);
    lane1_slot = LaneShardedRwMutex::SlotIndex();
    SharedLock lock(mutex);
  });
  ASSERT_EQ(lane1_slot, 1);
  {
    TryWriteLock write(mutex);
    ASSERT_FALSE(write.locked());
  }

  release = true;
  reader0.join();

  // With no readers, the writer takes every slot and readers of any lane are excluded.
  std::atomic<bool> reader_done{false};
  std::thread reader1;
  {
    TryWriteLock write(mutex);
    ASSERT_TRUE(write.locked());
    reader1 = std::thread([&] {
      ApplyLaneToCurrentThread(1);
      SharedLock lock(mutex);
      reader_done = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_FALSE(reader_done);
  }
  reader1.join();
  ASSERT_TRUE(reader_done);
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
  std::vector<std::thread> threads;
  for (int i = 0; i < 6; ++i) {
    threads.emplace_back([&, i] {
      ApplyLaneToCurrentThread(static_cast<LaneId>(i % 2));
      while (!stop) {
        SharedLock lock(mutex);
        ASSERT_GE(guarded, 0);
        ++reads;
      }
    });
  }
  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&] {
      ApplyServiceMaskToCurrentThread();
      for (int n = 0; n < 2000; ++n) {
        std::lock_guard lock(mutex);
        ++guarded;
      }
    });
  }
  threads[6].join();
  threads[7].join();
  stop = true;
  for (int i = 0; i < 6; ++i) {
    threads[i].join();
  }
  ASSERT_EQ(guarded, 4000);
  ASSERT_GT(reads, 0);
}

}  // namespace yb
