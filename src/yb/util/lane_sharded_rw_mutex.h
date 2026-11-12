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

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>

#include "yb/gutil/port.h"
#include "yb/gutil/thread_annotations.h"

#include "yb/util/lanes.h"
#include "yb/util/locks.h"

namespace yb {

// A reader-writer mutex whose shared side is sharded by lane.
//
// A reader locks only the slot of its lane, a cache line that only threads of that lane touch; a
// writer locks every slot, in slot order. For read-mostly state that every lane shares - the
// client MetaCache, where one std::shared_timed_mutex made the session threads of all lanes
// convoy on a single futex word (libc++'s shared_timed_mutex takes a plain mutex even for
// shared locking). With lane mode off every thread uses the spare slot and the mutex is one
// rw_spinlock.
//
// Exclusive locking costs kMaxLanes + 1 uncontended acquires (a few microseconds), so it suits
// state that is updated rarely relative to how often it is read.
//
// A thread must unlock_shared on the slot it lock_shared'ed: the slot is the calling thread's
// lane, which ApplyLaneToCurrentThread sets once per thread and does not change while the thread
// holds the lock. Lane mode activation (SetLaneTopology) happens before any sharded lock is
// taken under load; a thread that locked while lane mode was off and unlocks after it turned on
// would unlock the wrong slot, so callers must not hold this lock across SetLaneTopology.
//
// Provides lock/unlock/try_lock and lock_shared/unlock_shared (no try_lock_shared: the slot's
// rw_spinlock has none), so SharedLock<LaneShardedRwMutex>, std::shared_lock, std::lock_guard,
// std::unique_lock and UniqueLock all apply.
class CAPABILITY("mutex") LaneShardedRwMutex {
 public:
  LaneShardedRwMutex() = default;

  LaneShardedRwMutex(const LaneShardedRwMutex&) = delete;
  void operator=(const LaneShardedRwMutex&) = delete;

  void lock_shared() ACQUIRE_SHARED() {
    slots_[SlotIndex()].lock.lock_shared();
  }

  void unlock_shared() RELEASE_SHARED() {
    slots_[SlotIndex()].lock.unlock_shared();
  }

  void lock() ACQUIRE() {
    for (auto& slot : slots_) {
      slot.lock.lock();
    }
  }

  void unlock() RELEASE() {
    for (auto it = slots_.rbegin(); it != slots_.rend(); ++it) {
      it->lock.unlock();
    }
  }

  bool try_lock() TRY_ACQUIRE(true) {
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (!slots_[i].lock.try_lock()) {
        while (i--) {
          slots_[i].lock.unlock();
        }
        return false;
      }
    }
    return true;
  }

  // True if any slot is write-locked.
  bool is_write_locked() const {
    return std::any_of(slots_.begin(), slots_.end(), [](const Slot& slot) {
      return slot.lock.is_write_locked();
    });
  }

  // The slot the calling thread's shared locks use: its lane's, or the spare slot for an unlaned
  // thread or with lane mode off. Exposed for tests.
  static size_t SlotIndex() {
    const size_t lane = LaneModeActive() ? ThisThreadLane() : kNoLane;
    return std::min(lane, kMaxLanes);
  }

  static constexpr size_t kNumSlots = kMaxLanes + 1;

 private:
  struct alignas(CACHELINE_SIZE) Slot {
    rw_spinlock lock;
  };

  std::array<Slot, kNumSlots> slots_;
};

}  // namespace yb
