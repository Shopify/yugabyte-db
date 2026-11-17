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

#include "yb/util/background_task.h"
#include "yb/util/callsite_profiling.h"
#include "yb/util/status_log.h"
#include "yb/util/status.h"
#include "yb/util/thread.h"

namespace yb {

BackgroundTask::BackgroundTask(
    std::function<void()> task, std::string category, const std::string& name,
    std::chrono::milliseconds interval_msec)
    : task_(std::move(task)),
      category_(category),
      name_(std::move(name)),
      interval_(interval_msec) {}

BackgroundTask::~BackgroundTask() {
}

Status BackgroundTask::Init() {
  RETURN_NOT_OK(Thread::Create(category_, name_, &BackgroundTask::Run, this, &thread_));
  return Status::OK();
}

void BackgroundTask::SetInterval(std::chrono::milliseconds interval_msec) {
  std::unique_lock lock(mutex_);
  interval_ = interval_msec;
  VLOG(2) << "BackgroundTask interval changed to " << ToString(interval_msec);
}

// Wait for pending tasks and shut down
void BackgroundTask::Shutdown() {
  StartShutdown();
  CompleteShutdown();
}

void BackgroundTask::StartShutdown() {
  {
    std::unique_lock lock(mutex_);
    if (closing_) {
      VLOG(2) << "BackgroundTask already shut down";
      return;
    }
    closing_ = true;
  }
  YB_PROFILE(cond_.notify_one());
}

void BackgroundTask::CompleteShutdown() {
  CHECK_OK(ThreadJoiner(thread_.get()).Join());
}

Status BackgroundTask::Wake() {
  // Return without mutex_ when a run is already pending: callers that wake the task from many
  // threads while a condition lasts would otherwise serialize on it. Whoever set have_job_
  // notified the thread. The pending run must also see what the caller wrote before this call,
  // but nothing orders those writes before the have_job_ load, nor the task's clear before its
  // reads, so without fences the load can return a true the task already cleared while the run
  // misses the writes (store buffering). This fence pairs with the one in WaitForJob(): by the
  // fence rule ([atomics.order]), either this one comes first in their total order and the run
  // that clears the have_job_ read here sees the caller's writes, or the task's comes first and
  // the load sees the clear, so the caller takes the mutex path.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  if (have_job_.load(std::memory_order_acquire) && !closing_.load(std::memory_order_acquire)) {
    return Status::OK();
  }
  {
    std::lock_guard lock(mutex_);
    if (closing_) {
      return STATUS(ShutdownInProgress, "Task is shutting down.");
    }
    have_job_ = true;
  }
  YB_PROFILE(cond_.notify_one());
  return Status::OK();
}

void BackgroundTask::Run() {
  while (WaitForJob()) {
    task_();
  }
  VLOG(1) << "BackgroundTask thread shutting down";
}

bool BackgroundTask::WaitForJob() {
  std::unique_lock lock(mutex_);
  while (true) {
    if (closing_) {
      return false;
    }
    if (have_job_) {
      have_job_ = false;
      // Orders the clear before the run's reads; pairs with the fence in Wake(). Covers runs
      // started by the interval too, which set have_job_ below and clear it here.
      std::atomic_thread_fence(std::memory_order_seq_cst);
      return true;
    }

    // Wait
    if (interval_ != std::chrono::milliseconds::zero()) {
      cond_.wait_for(lock, interval_);
      // If we wake here from the interval_ timeout, then we should behave as if we have a job. If
      // we wake from an explicit notify from a Wake() call, we should still behave as if we have
      // a job.
      have_job_ = true;
    } else {
      cond_.wait(lock);
    }
  }
}

}  // namespace yb
