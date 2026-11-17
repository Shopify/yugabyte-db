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
// A reader touches only the slot of its lane, a cache line that only threads of that lane write;
// a writer visits every slot. For read-mostly state that every lane shares - the client MetaCache,
// where one std::shared_timed_mutex made the session threads of all lanes convoy on a single
// futex word (libc++'s shared_timed_mutex takes a plain mutex even for shared locking). With lane
// mode off every thread uses the spare slot and the mutex is a single reader-writer spinlock.
//
// Each slot is one word: bit 31 says a writer holds or wants the mutex, the low bits count the
// readers inside.
//
// Readers increment the count and are in unless they then see the writer bit, in which case they
// undo the increment and wait for the bit to clear. The increment is wait-free, so readers of one
// lane do not retry against each other.
//
// Writers serialize on writer_active_ (a spin flag: writers are rare), then set the writer bit on every slot with an atomic OR and
// wait for each slot's count to reach zero. The OR cannot fail, whatever the readers of that slot
// are doing, so a writer always makes progress: once the bit is set no new reader enters, and the
// readers already inside leave after their (short) critical sections. An earlier version raised
// the bit with compare-and-swap against the whole word; under a full write load the readers of a
// busy lane changed the word faster than the writer could swap it, and one MetaCache update took
// tens of seconds while the lanes it had already flagged stood still.
//
// The drains of all slots overlap, so a write waits for the slowest reader once. A write costs
// about three atomic operations per slot (kNumSlots slots), so this suits state that is updated
// rarely relative to how often it is read.
//
// Writers have priority: readers back off while the bit is set. A continuous stream of writers
// would starve readers; the intended users write rarely.
//
// Waiting uses boost::detail::yield, as rw_semaphore does: spin, then sched_yield, then short
// sleeps. Critical sections are expected to be short (map lookups and updates); the lock is not
// for holding across I/O.
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
    for (;;) {
      if (!(state.fetch_add(1, std::memory_order_acquire) & kWriterBit)) {
        return;
      }
      // A writer holds or wants the mutex: leave, and come back once it is done.
      state.fetch_sub(1, std::memory_order_release);
      uint32_t spins = 0;
      while (state.load(std::memory_order_relaxed) & kWriterBit) {
        boost::detail::yield(spins++);
      }
    }
  }

  void unlock_shared() RELEASE_SHARED() {
    slots_[SlotIndex()].state.fetch_sub(1, std::memory_order_release);
  }

  void lock() ACQUIRE() {
    for (uint32_t spins = 0; writer_active_.exchange(true, std::memory_order_acquire);) {
      boost::detail::yield(spins++);
    }
    for (auto& slot : slots_) {
      slot.state.fetch_or(kWriterBit, std::memory_order_acquire);
    }
    // A slot's count can also be raised briefly by a reader that is about to see the bit and
    // leave; either way it returns to zero without a reader entering.
    for (auto& slot : slots_) {
      uint32_t spins = 0;
      while (slot.state.load(std::memory_order_acquire) != kWriterBit) {
        boost::detail::yield(spins++);
      }
    }
  }

  void unlock() RELEASE() {
    // Clear only the bit: a reader may have its transient increment in the count.
    for (auto& slot : slots_) {
      slot.state.fetch_and(~kWriterBit, std::memory_order_release);
    }
    writer_active_.store(false, std::memory_order_release);
  }

  // True if a writer holds or is acquiring the mutex.
  bool is_write_locked() const {
    return slots_.front().state.load(std::memory_order_acquire) & kWriterBit;
  }

  // The slot the calling thread's shared locks use: its lane's, or the spare slot for an unlaned
  // thread or with lane mode off. Exposed for tests.
  static size_t SlotIndex() {
    const size_t lane = LaneModeActive() ? ThisThreadLane() : kNoLane;
    return std::min(lane, kMaxLanes);
  }

  static constexpr size_t kNumSlots = kMaxLanes + 1;

 private:
  static constexpr uint32_t kWriterBit = 1u << 31;

  struct alignas(CACHELINE_SIZE) Slot {
    std::atomic<uint32_t> state{0};
  };

  // Set while a writer holds or is acquiring the mutex; serializes writers.
  std::atomic<bool> writer_active_{false};
  std::array<Slot, kNumSlots> slots_;
};

}  // namespace yb
