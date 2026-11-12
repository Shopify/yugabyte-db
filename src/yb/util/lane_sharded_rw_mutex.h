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
#include <atomic>
#include <cstddef>
#include <cstdint>

#include <boost/smart_ptr/detail/yield_k.hpp>

#include "yb/gutil/port.h"
#include "yb/gutil/thread_annotations.h"

#include "yb/util/lanes.h"

namespace yb {

// A reader-writer mutex whose shared side is sharded by lane.
//
// A reader locks only the slot of its lane, a cache line that only threads of that lane touch; a
// writer locks every slot. For read-mostly state that every lane shares - the client MetaCache,
// where one std::shared_timed_mutex made the session threads of all lanes convoy on a single
// futex word (libc++'s shared_timed_mutex takes a plain mutex even for shared locking). With lane
// mode off every thread uses the spare slot and the mutex is a single reader-writer spinlock.
//
// A writer acquires in two phases: it raises the writer flag on every slot (new readers of any
// lane now wait), then waits for the readers already inside each slot to leave. The drains run
// concurrently, so a write waits for the slowest reader of any slot once - not for 255 drains in
// sequence, each exposed to a reader preempted on a saturated host. Writers serialize on the
// flags in slot order. A write still costs kNumSlots atomic operations, so this suits state that
// is updated rarely relative to how often it is read; a cold MetaCache filling under load takes
// this path once per missed tablet.
//
// Readers and writers spin briefly, then yield (boost::detail::yield, as rw_semaphore). Critical sections are expected to be short (map
// lookups and updates); the lock is not for holding across I/O.
//
// A thread must unlock_shared on the slot it lock_shared'ed: the slot is the calling thread's
// lane, which ApplyLaneToCurrentThread sets once per thread and does not change while the thread
// holds the lock. Callers must not hold this lock across SetLaneTopology.
//
// Provides lock/unlock and lock_shared/unlock_shared, so SharedLock<LaneShardedRwMutex>,
// std::shared_lock, std::lock_guard, std::unique_lock and UniqueLock apply. No try_lock.
class CAPABILITY("mutex") LaneShardedRwMutex {
 public:
  LaneShardedRwMutex() = default;

  LaneShardedRwMutex(const LaneShardedRwMutex&) = delete;
  void operator=(const LaneShardedRwMutex&) = delete;

  void lock_shared() ACQUIRE_SHARED() {
    auto& state = slots_[SlotIndex()].state;
    uint32_t spins = 0;
    for (;;) {
      uint32_t current = state.load(std::memory_order_relaxed);
      if (!(current & kWriterFlag) &&
          state.compare_exchange_weak(
              current, current + 1, std::memory_order_acquire, std::memory_order_relaxed)) {
        return;
      }
      Pause(spins++);
    }
  }

  void unlock_shared() RELEASE_SHARED() {
    slots_[SlotIndex()].state.fetch_sub(1, std::memory_order_release);
  }

  void lock() ACQUIRE() {
    // Phase 1: raise the writer flag on every slot, waiting only for other writers.
    for (auto& slot : slots_) {
      uint32_t spins = 0;
      for (;;) {
        uint32_t current = slot.state.load(std::memory_order_relaxed);
        if (!(current & kWriterFlag) &&
            slot.state.compare_exchange_weak(
                current, current | kWriterFlag, std::memory_order_acquire,
                std::memory_order_relaxed)) {
          break;
        }
        Pause(spins++);
      }
    }
    // Phase 2: wait for the readers already inside each slot to leave.
    for (auto& slot : slots_) {
      uint32_t spins = 0;
      while (slot.state.load(std::memory_order_acquire) != kWriterFlag) {
        Pause(spins++);
      }
    }
  }

  void unlock() RELEASE() {
    for (auto it = slots_.rbegin(); it != slots_.rend(); ++it) {
      it->state.store(0, std::memory_order_release);
    }
  }

  // True if a writer holds or is acquiring the mutex.
  bool is_write_locked() const {
    return slots_.front().state.load(std::memory_order_acquire) & kWriterFlag;
  }

  // The slot the calling thread's shared locks use: its lane's, or the spare slot for an unlaned
  // thread or with lane mode off. Exposed for tests.
  static size_t SlotIndex() {
    const size_t lane = LaneModeActive() ? ThisThreadLane() : kNoLane;
    return std::min(lane, kMaxLanes);
  }

  static constexpr size_t kNumSlots = kMaxLanes + 1;

 private:
  static constexpr uint32_t kWriterFlag = 1u << 31;

  // The backoff rw_semaphore uses: spin, then sched_yield, then short sleeps.
  static void Pause(uint32_t spins) {
    boost::detail::yield(spins);
  }

  // Bit 31: a writer holds or is acquiring; bits 0-30: readers inside.
  struct alignas(CACHELINE_SIZE) Slot {
    std::atomic<uint32_t> state{0};
  };

  std::array<Slot, kNumSlots> slots_;
};

}  // namespace yb
