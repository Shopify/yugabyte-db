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

#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "yb/gutil/thread_annotations.h"

#include "yb/rpc/rpc_fwd.h"

#include "yb/util/result.h"
#include "yb/util/status.h"

namespace yb::consensus {

// Calls entries back when they fall due, from one messenger-scheduler task per server instead of
// one per entry. Its client is RaftConsensus::AsyncWriteWait, whose periodic ticks would otherwise
// cost a Schedule and an Abort on the scheduler for every pending log-durability wait, on the io
// threads that also run the multi-Raft batcher's flush and pacing timers.
//
// Ownership. An entry is held strongly from Register until it fires, is removed, or the sweeper
// shuts down, as a scheduler task holds what it captures. An idle AsyncWriteWait has no other
// strong reference (its log waiter holds it weakly), so a weak entry would destroy it before it
// completes. Every reference leaves the container under mutex_ and is dropped after mutex_ is
// released: the last one may destroy the entry, and with it a RaftConsensus, whose destructor may
// call back into the sweeper.
//
// Timer. At most one scheduler task is armed, for the earliest due time, with the steady_clock
// time-point overload of Scheduler::Schedule. Register arms a new task only for an entry due
// before the armed time, and aborts the old one. Remove never touches the scheduler, so a task can
// fire with nothing due and just re-arm. A firing task takes every entry already due and re-arms
// for the next one; the scheduler fires late enough that entries registered microseconds apart, as
// the ticks of waits that started together are, usually fire together. A task that is no longer
// the armed one (aborted, or superseded) does nothing. A non-OK status on the armed task means the
// scheduler is shutting down, and is handled as Shutdown(status), so no entry is stranded.
//
// Shutdown(status) takes every pending entry and calls OnShutdown(status) on each. A Register
// after that stores nothing and returns the shutdown status, and the caller completes the entry
// itself: Register is called under the caller's own lock, which OnShutdown would need.
//
// Threads. OnDue runs on a scheduler io thread. OnShutdown runs on the thread that calls
// Shutdown (or destroys a sweeper that was not shut down), or on a scheduler io thread when the
// scheduler shuts down first. Neither runs under mutex_, so both may call Register or Remove. The
// sweeper must be owned by a std::shared_ptr (its scheduler task holds it weakly) and must outlive
// every thread that can call Register or Remove.
class RaftAsyncWaitSweeper : public std::enable_shared_from_this<RaftAsyncWaitSweeper> {
 public:
  using Clock = std::chrono::steady_clock;

  class Entry {
   public:
    virtual ~Entry() = default;

    virtual void OnDue() = 0;
    virtual void OnShutdown(const Status& status) = 0;
  };

  struct Handle {
    Clock::time_point due;
    // 0 for no entry.
    uint64_t id = 0;
  };

  // Scheduler calls and callbacks, for tests.
  struct Counters {
    uint64_t registered = 0;
    uint64_t scheduled = 0;
    uint64_t aborted = 0;
    // Armed tasks that fired with an OK status.
    uint64_t expiries = 0;
    // OnDue calls.
    uint64_t due = 0;
    // Scheduled tasks whose callback has not run yet.
    uint64_t outstanding = 0;
    uint64_t max_outstanding = 0;
  };

  // The status the scheduler gives the tasks it runs at its own shutdown, which a per-wait timer
  // finishes its wait with. Owners shut the sweeper down with it.
  static Status ShutdownStatus();

  explicit RaftAsyncWaitSweeper(rpc::Scheduler* scheduler);
  ~RaftAsyncWaitSweeper();

  RaftAsyncWaitSweeper(const RaftAsyncWaitSweeper&) = delete;
  void operator=(const RaftAsyncWaitSweeper&) = delete;

  // Calls entry->OnDue() once, delay from now, unless it is removed or the sweeper shuts down
  // first. Returns the shutdown status, without storing entry, after Shutdown.
  Result<Handle> Register(std::shared_ptr<Entry> entry, Clock::duration delay);

  // Drops the entry if it is still pending. An entry that already fired or was shut down is
  // ignored.
  void Remove(const Handle& handle);

  // Calls OnShutdown(status) on every pending entry. Only the first call has an effect.
  void Shutdown(const Status& status);

  Counters TEST_counters() const;
  size_t TEST_num_pending() const;

 private:
  using Key = std::pair<Clock::time_point, uint64_t>;
  using Entries = std::vector<std::shared_ptr<Entry>>;

  void ArmUnlocked(Clock::time_point due) REQUIRES(mutex_);
  void TimerFired(rpc::ScheduledTaskId task_id, const Status& status);
  // Marks the sweeper shut down and returns the pending entries.
  Entries ShutdownUnlocked(const Status& status) REQUIRES(mutex_);

  rpc::Scheduler& scheduler_;

  mutable std::mutex mutex_;
  std::map<Key, std::shared_ptr<Entry>> entries_ GUARDED_BY(mutex_);
  uint64_t next_id_ GUARDED_BY(mutex_) = 0;
  rpc::ScheduledTaskId timer_id_ GUARDED_BY(mutex_);
  // Time the armed task fires at.
  Clock::time_point timer_due_ GUARDED_BY(mutex_);
  bool shut_down_ GUARDED_BY(mutex_) = false;
  Status shutdown_status_ GUARDED_BY(mutex_);
  Counters counters_ GUARDED_BY(mutex_);
};

} // namespace yb::consensus
