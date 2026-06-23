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

#include "yb/util/rw_mutex.h"

#include <atomic>
#include <mutex>

#include "yb/util/logging.h"

#include "yb/gutil/bind.h"
#include "yb/gutil/map-util.h"
#include "yb/gutil/walltime.h"
#include "yb/util/env.h"
#include "yb/util/metrics.h"

using std::lock_guard;

METRIC_DEFINE_gauge_uint64(server, rwmutex_contention_time, "RWMutex Contention Time",
    yb::MetricUnit::kMicroseconds,
    "Amount of time (microseconds) spent waiting to acquire yb::RWMutex (pthread_rwlock) locks, "
    "for both readers and writers, since the server started. If this increases rapidly, it may "
    "indicate a performance issue in YB internals triggered by a particular workload and warrant "
    "investigation.",
    yb::EXPOSE_AS_COUNTER);

namespace {

void unlock_rwlock(pthread_rwlock_t* rwlock) {
  int rv = pthread_rwlock_unlock(rwlock);
  DCHECK_EQ(0, rv) << strerror(rv);
}

std::atomic<int64_t> g_rwmutex_contention_micros{0};

uint64_t GetRWMutexContentionMicros() {
  return g_rwmutex_contention_micros.load(std::memory_order_relaxed);
}

} // anonymous namespace

namespace yb {

void RegisterRWMutexContentionMetric(const scoped_refptr<MetricEntity>& entity) {
  entity->NeverRetire(
      METRIC_rwmutex_contention_time.InstantiateFunctionGauge(
          entity, Bind(&GetRWMutexContentionMicros)));
}

RWMutex::RWMutex()
#ifndef NDEBUG
    : writer_tid_(0)
#endif
{
  Init(Priority::PREFER_READING);
}

RWMutex::RWMutex(Priority prio)
#ifndef NDEBUG
    : writer_tid_(0)
#endif
{
  Init(prio);
}

void RWMutex::Init(Priority prio) {
#ifdef __linux__
  // Adapt from priority to the pthread type.
  int kind = PTHREAD_RWLOCK_PREFER_READER_NP;
  switch (prio) {
    case Priority::PREFER_READING:
      kind = PTHREAD_RWLOCK_PREFER_READER_NP;
      break;
    case Priority::PREFER_WRITING:
      kind = PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP;
      break;
  }

  // Initialize the new rwlock with the user's preference.
  pthread_rwlockattr_t attr;
  int rv = pthread_rwlockattr_init(&attr);
  DCHECK_EQ(0, rv) << strerror(rv);
  rv = pthread_rwlockattr_setkind_np(&attr, kind);
  DCHECK_EQ(0, rv) << strerror(rv);
  rv = pthread_rwlock_init(&native_handle_, &attr);
  DCHECK_EQ(0, rv) << strerror(rv);
  rv = pthread_rwlockattr_destroy(&attr);
  DCHECK_EQ(0, rv) << strerror(rv);
#else
  int rv = pthread_rwlock_init(&native_handle_, NULL);
  DCHECK_EQ(0, rv) << strerror(rv);
#endif
}

RWMutex::~RWMutex() {
  int rv = pthread_rwlock_destroy(&native_handle_);
  DCHECK_EQ(0, rv) << strerror(rv);
}

void RWMutex::ReadLock() {
  CheckLockState(LockState::NEITHER);
  auto start_micros = GetMonoTimeMicros();
  int rv = pthread_rwlock_rdlock(&native_handle_);
  g_rwmutex_contention_micros.fetch_add(
      GetMonoTimeMicros() - start_micros, std::memory_order_relaxed);
  DCHECK_EQ(0, rv) << strerror(rv);
  MarkForReading();
}

void RWMutex::ReadUnlock() {
  CheckLockState(LockState::READER);
  UnmarkForReading();
  unlock_rwlock(&native_handle_);
}

bool RWMutex::TryReadLock() {
  CheckLockState(LockState::NEITHER);
  int rv = pthread_rwlock_tryrdlock(&native_handle_);
  if (rv == EBUSY) {
    return false;
  }
  DCHECK_EQ(0, rv) << strerror(rv);
  MarkForReading();
  return true;
}

void RWMutex::WriteLock() {
#if defined(THREAD_SANITIZER)
  if (TryWriteLock()) {
    return;
  }
#endif
  CheckLockState(LockState::NEITHER);
  auto start_micros = GetMonoTimeMicros();
  int rv = pthread_rwlock_wrlock(&native_handle_);
  g_rwmutex_contention_micros.fetch_add(
      GetMonoTimeMicros() - start_micros, std::memory_order_relaxed);
  DCHECK_EQ(0, rv) << strerror(rv);
  MarkForWriting();
}

void RWMutex::WriteUnlock() {
  CheckLockState(LockState::WRITER);
  UnmarkForWriting();
  unlock_rwlock(&native_handle_);
}

bool RWMutex::TryWriteLock() {
  CheckLockState(LockState::NEITHER);
  int rv = pthread_rwlock_trywrlock(&native_handle_);
  if (rv == EBUSY) {
    return false;
  }
  DCHECK_EQ(0, rv) << strerror(rv);
  MarkForWriting();
  return true;
}

#ifndef NDEBUG

void RWMutex::AssertAcquiredForReading() const {
  lock_guard<simple_spinlock> l(tid_lock_);
  CHECK(ContainsKey(reader_tids_, Env::Default()->gettid()));
}

void RWMutex::AssertAcquiredForWriting() const {
  lock_guard<simple_spinlock> l(tid_lock_);
  CHECK_EQ(Env::Default()->gettid(), writer_tid_);
}

void RWMutex::CheckLockState(LockState state) const {
  auto my_tid = Env::Default()->gettid();
  bool is_reader;
  bool is_writer;
  {
    lock_guard<simple_spinlock> l(tid_lock_);
    is_reader = ContainsKey(reader_tids_, my_tid);
    is_writer = writer_tid_ == my_tid;
  }

  CHECK_EQ(is_reader, (state == LockState::READER)) << "Invalid reading state";
  CHECK_EQ(is_writer, (state == LockState::WRITER)) << "Invalid writing state";
}

void RWMutex::MarkForReading() {
  lock_guard<simple_spinlock> l(tid_lock_);
  reader_tids_.insert(Env::Default()->gettid());
}

void RWMutex::MarkForWriting() {
  lock_guard<simple_spinlock> l(tid_lock_);
  writer_tid_ = Env::Default()->gettid();
}

void RWMutex::UnmarkForReading() {
  lock_guard<simple_spinlock> l(tid_lock_);
  reader_tids_.erase(Env::Default()->gettid());
}

void RWMutex::UnmarkForWriting() {
  lock_guard<simple_spinlock> l(tid_lock_);
  writer_tid_ = 0;
}

#endif

} // namespace yb
