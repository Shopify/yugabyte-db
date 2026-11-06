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

#include "yb/consensus/raft_async_wait_sweeper.h"

#include "yb/rpc/scheduler.h"

#include "yb/util/errno.h"
#include "yb/util/logging.h"
#include "yb/util/monotime.h"

namespace yb::consensus {

Status RaftAsyncWaitSweeper::ShutdownStatus() {
  return STATUS(Aborted, "Scheduler is shutting down", "" /* msg2 */, Errno(ESHUTDOWN));
}

RaftAsyncWaitSweeper::RaftAsyncWaitSweeper(rpc::Scheduler* scheduler)
    : scheduler_(*scheduler), timer_id_(rpc::kInvalidTaskId) {}

RaftAsyncWaitSweeper::~RaftAsyncWaitSweeper() {
  // Owners shut the sweeper down first. Without that, the pending entries are still completed
  // rather than dropped, but the scheduler, which may already be gone, is not touched.
  Entries entries;
  {
    std::lock_guard lock(mutex_);
    if (!shut_down_) {
      entries = ShutdownUnlocked(ShutdownStatus());
    }
  }
  LOG_IF(WARNING, !entries.empty())
      << "Raft async wait sweeper destroyed with " << entries.size() << " pending entries";
  for (auto& entry : entries) {
    entry->OnShutdown(ShutdownStatus());
  }
}

Result<RaftAsyncWaitSweeper::Handle> RaftAsyncWaitSweeper::Register(
    std::shared_ptr<Entry> entry, Clock::duration delay) {
  rpc::ScheduledTaskId superseded = rpc::kInvalidTaskId;
  Handle handle;
  {
    std::lock_guard lock(mutex_);
    if (shut_down_) {
      return shutdown_status_;
    }
    handle = Handle{.due = Clock::now() + delay, .id = ++next_id_};
    entries_.emplace(Key(handle.due, handle.id), std::move(entry));
    ++counters_.registered;
    VLOG(1) << "Registered an entry due in " << ToMicroseconds(delay) << " us, "
            << entries_.size() << " pending";
    if (timer_id_ == rpc::kInvalidTaskId || handle.due < timer_due_) {
      superseded = std::exchange(timer_id_, rpc::kInvalidTaskId);
      if (superseded != rpc::kInvalidTaskId) {
        ++counters_.aborted;
      }
      ArmUnlocked(handle.due);
    }
  }
  if (superseded != rpc::kInvalidTaskId) {
    // The aborted task runs with an error and is ignored, since it is no longer timer_id_.
    scheduler_.Abort(superseded);
  }
  return handle;
}

void RaftAsyncWaitSweeper::Remove(const Handle& handle) {
  if (handle.id == 0) {
    return;
  }
  std::shared_ptr<Entry> entry;
  {
    std::lock_guard lock(mutex_);
    auto it = entries_.find(Key(handle.due, handle.id));
    if (it == entries_.end()) {
      return;
    }
    entry = std::move(it->second);
    entries_.erase(it);
  }
}

void RaftAsyncWaitSweeper::Shutdown(const Status& status) {
  rpc::ScheduledTaskId timer_id;
  Entries entries;
  {
    std::lock_guard lock(mutex_);
    if (shut_down_) {
      return;
    }
    timer_id = std::exchange(timer_id_, rpc::kInvalidTaskId);
    if (timer_id != rpc::kInvalidTaskId) {
      ++counters_.aborted;
    }
    entries = ShutdownUnlocked(status);
  }
  if (timer_id != rpc::kInvalidTaskId) {
    scheduler_.Abort(timer_id);
  }
  LOG(INFO) << "Raft async wait sweeper shut down with " << entries.size() << " pending entries: "
            << status;
  for (auto& entry : entries) {
    entry->OnShutdown(status);
  }
}

RaftAsyncWaitSweeper::Entries RaftAsyncWaitSweeper::ShutdownUnlocked(const Status& status) {
  shut_down_ = true;
  shutdown_status_ = status;
  Entries entries;
  entries.reserve(entries_.size());
  for (auto& [key, entry] : entries_) {
    entries.push_back(std::move(entry));
  }
  entries_.clear();
  return entries;
}

void RaftAsyncWaitSweeper::ArmUnlocked(Clock::time_point due) {
  std::weak_ptr<RaftAsyncWaitSweeper> weak_self = shared_from_this();
  timer_due_ = due;
  // The task cannot observe timer_id_ before it is assigned, because it takes mutex_, which is
  // held here. The scheduler never runs a task on the calling thread.
  timer_id_ = scheduler_.Schedule(
      [weak_self](rpc::ScheduledTaskId task_id, const Status& status) {
        if (auto self = weak_self.lock()) {
          self->TimerFired(task_id, status);
        }
      },
      due);
  ++counters_.scheduled;
  counters_.max_outstanding = std::max(counters_.max_outstanding, ++counters_.outstanding);
}

void RaftAsyncWaitSweeper::TimerFired(rpc::ScheduledTaskId task_id, const Status& status) {
  Entries due;
  bool shut_down = false;
  {
    std::lock_guard lock(mutex_);
    --counters_.outstanding;
    if (task_id != timer_id_) {
      // Aborted when an earlier entry or Shutdown replaced it.
      return;
    }
    timer_id_ = rpc::kInvalidTaskId;
    if (!status.ok()) {
      due = ShutdownUnlocked(status);
      shut_down = true;
    } else {
      ++counters_.expiries;
      const auto now = Clock::now();
      while (!entries_.empty() && entries_.begin()->first.first <= now) {
        due.push_back(std::move(entries_.begin()->second));
        entries_.erase(entries_.begin());
      }
      counters_.due += due.size();
      if (!entries_.empty()) {
        ArmUnlocked(entries_.begin()->first.first);
      }
    }
  }
  if (shut_down) {
    LOG(INFO) << "Raft async wait sweeper shut down by its scheduler with " << due.size()
              << " pending entries: " << status;
    for (auto& entry : due) {
      entry->OnShutdown(status);
    }
    return;
  }
  for (auto& entry : due) {
    entry->OnDue();
  }
}

RaftAsyncWaitSweeper::Counters RaftAsyncWaitSweeper::TEST_counters() const {
  std::lock_guard lock(mutex_);
  return counters_;
}

size_t RaftAsyncWaitSweeper::TEST_num_pending() const {
  std::lock_guard lock(mutex_);
  return entries_.size();
}

} // namespace yb::consensus
