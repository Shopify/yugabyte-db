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

#pragma once

#include <atomic>
#include <string>
#if !defined(__APPLE__)
#include <sys/timex.h>
#endif // !defined(__APPLE__)


#include "yb/gutil/port.h"
#include "yb/gutil/ref_counted.h"
#include "yb/server/clock.h"
#include "yb/util/locks.h"
#include "yb/util/physical_time.h"

namespace yb {
namespace server {

// The HybridTime clock.
//
// HybridTime should not be used on a distributed cluster running on OS X hosts,
// since NTP clock error is not available.
class HybridClock : public Clock {
 public:
  HybridClock();
  explicit HybridClock(PhysicalClockPtr clock);
  explicit HybridClock(const std::string& time_source);

  Status Init() override;

  HybridTimeRange NowRange() override;

  // See ClockBase::CoarseNow. Returns the clock's latest hybrid time without advancing the clock
  // while that is within FLAGS_hybrid_clock_coarse_now_max_lag_us of the physical clock, and
  // NowRange() otherwise or when the flag is negative.
  HybridTimeRange CoarseNowRange() override;

  // Updates the clock with a hybrid_time originating on another machine.
  void Update(const HybridTime& to_update) override;

  void RegisterMetrics(const scoped_refptr<MetricEntity>& metric_entity) override;

  // Obtains the hybrid_time corresponding to the current time and the associated
  // error in micros. This may fail if the clock is unsynchronized or synchronized
  // but the error is too high and, since we can't do anything about it,
  // LOG(FATAL)'s in that case.
  void NowWithError(HybridTime* hybrid_time, uint64_t* max_error_usec);

  // Static encoding/decoding methods for hybrid_times. Public mostly
  // for testing/debugging purposes.

  static void RegisterProvider(std::string name, PhysicalClockProvider provider);

  // Enables check whether clock skew within configured bounds.
  static void EnableClockSkewControl();

  const PhysicalClockPtr& physical_clock() { return clock_; }

 private:
  enum State {
    kNotInitialized,
    kInitialized
  };

  // Used to get the hybrid_time for metrics.
  uint64_t NowForMetrics();

  // Used to get the current error, for metrics.
  uint64_t ErrorForMetrics();

  // Used to get the current error, for metrics.
  int64_t SkewForMetrics();

  // Reports a physical clock that is behind the clock's hybrid time by more than the allowed skew.
  void CheckSkew(const PhysicalTime& now, MicrosTime last_usec);

  // Read by every call and never written after Init(): on a cache line of their own, away from the
  // reference count in the base class, which every copy of a ClockPtr writes, and from next_.
  alignas(CACHELINE_SIZE) PhysicalClockPtr clock_;
  State state_ = kNotInitialized;

  // The smallest hybrid time, as HybridTime::ToUint64, that this clock has not returned yet: every
  // hybrid time it has returned or was given in Update() is below it. The physical microseconds and
  // the logical counter that used to be two fields are the high and low bits of this one value
  // (HybridTime is microseconds * 4096 + logical, and the logical part carries into the
  // microseconds), so that reading it is a plain load. As a 16-byte atomic even a load was a locked
  // compare-and-swap, which took the cache line for writing: every call, also an Update() that
  // changed nothing, did that from every thread of the server.
  //
  // On a cache line of its own: Now() writes it.
  alignas(CACHELINE_SIZE) std::atomic<uint64_t> next_{0};

  // Clock metrics are set to detach to their last value. This means
  // that, during our destructor, we'll need to access other class members
  // declared above this. Hence, this member must be declared last.
  // Aligned so that it does not share the line of next_.
  alignas(CACHELINE_SIZE) std::shared_ptr<void> metric_detacher_;
};

}  // namespace server
}  // namespace yb
