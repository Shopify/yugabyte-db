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

#include "yb/gutil/ref_counted.h"
#include "yb/common/hybrid_time.h"

namespace yb {

typedef std::pair<HybridTime, HybridTime> HybridTimeRange;

class ClockBase : public RefCountedThreadSafe<ClockBase> {
 public:
  // Obtains a new transaction timestamp corresponding to the current instant.
  HybridTime Now() { return NowRange().first; }

  // Obtains the hybrid_time corresponding to the current time,
  // that is maximally possible in cluster.
  HybridTime MaxGlobalNow() { return NowRange().second; }

  virtual HybridTimeRange NowRange() = 0;

  // The current hybrid time for a caller that does not need it to be unique: one that stamps a
  // message with the sender's time, takes the physical part for lease or timeout arithmetic, or
  // asks whether a hybrid time has passed. Like Now(), the result is at least every hybrid time
  // this clock has returned or was given in Update() before the call, and every Now() after the
  // call returns a larger one. Unlike Now(), the call need not advance the clock, so:
  //   - other calls may get the same value, and it may be a value that Now() returned to someone;
  //   - it may be behind the physical clock, by an amount the implementation bounds.
  // Use Now() for a hybrid time that identifies something (an operation, a commit).
  // The default advances the clock.
  HybridTime CoarseNow() { return CoarseNowRange().first; }

  virtual HybridTimeRange CoarseNowRange() { return NowRange(); }

  virtual void Update(const HybridTime& to_update) = 0;

  virtual ~ClockBase() {}
};

// Returns time after wait.
// Returns error on timeout.
Result<HybridTime> WaitUntil(ClockBase* clock, HybridTime hybrid_time, CoarseTimePoint deadline);

} // namespace yb
