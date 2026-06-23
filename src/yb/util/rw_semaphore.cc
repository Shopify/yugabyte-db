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

#include "yb/util/thread.h"

#include "yb/util/rw_semaphore.h"

#include "yb/gutil/bind.h"
#include "yb/util/metrics.h"

METRIC_DEFINE_gauge_uint64(server, rw_semaphore_contention_time, "RWSemaphore Contention Time",
    yb::MetricUnit::kMicroseconds,
    "Amount of time (microseconds) spent waiting to acquire yb::rw_semaphore locks, for both "
    "readers and writers, since the server started. If this increases rapidly, it may indicate a "
    "performance issue in YB internals triggered by a particular workload and warrant "
    "investigation.",
    yb::EXPOSE_AS_COUNTER);

namespace yb {

namespace {

std::atomic<int64_t> g_rw_semaphore_contention_micros{0};

uint64_t GetRWSemaphoreContentionMicros() {
  return g_rw_semaphore_contention_micros.load(std::memory_order_relaxed);
}

}  // namespace

void SubmitRWSemaphoreContentionMicros(int64_t micros) {
  g_rw_semaphore_contention_micros.fetch_add(micros, std::memory_order_relaxed);
}

void RegisterRWSemaphoreContentionMetric(const scoped_refptr<MetricEntity>& entity) {
  entity->NeverRetire(
      METRIC_rw_semaphore_contention_time.InstantiateFunctionGauge(
          entity, Bind(&GetRWSemaphoreContentionMicros)));
}

#ifndef NDEBUG
void rw_semaphore::AssignWriterTid() {
  writer_tid_ = Thread::CurrentThreadId();
}
#endif

}  // namespace yb
