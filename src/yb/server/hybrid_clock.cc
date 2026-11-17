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

#include "yb/server/hybrid_clock.h"

#include <algorithm>
#include <mutex>
#include <string>
#include <vector>

#include "yb/gutil/bind.h"
#include "yb/gutil/walltime.h"

#include "yb/util/errno.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/locks.h"
#include "yb/util/logging.h"
#include "yb/util/metrics.h"
#include "yb/util/net/net_util.h"
#include "yb/util/result.h"
#include "yb/util/subprocess.h"

DEFINE_UNKNOWN_bool(use_hybrid_clock, true,
            "Whether HybridClock should be used as the default clock"
            " implementation. This should be disabled for testing purposes only.");
TAG_FLAG(use_hybrid_clock, hidden);

METRIC_DEFINE_gauge_uint64(server, hybrid_clock_hybrid_time,
                           "Hybrid Clock HybridTime",
                           yb::MetricUnit::kMicroseconds,
                           "Hybrid clock hybrid_time.");
METRIC_DEFINE_gauge_uint64(server, hybrid_clock_error,
                           "Hybrid Clock Error",
                           yb::MetricUnit::kMicroseconds,
                           "Server clock maximum error.");
METRIC_DEFINE_gauge_int64(server, hybrid_clock_skew,
                           "Hybrid Clock Skew",
                           yb::MetricUnit::kMicroseconds,
                           "Server clock skew.");

DEFINE_NON_RUNTIME_string(time_source, "",
              "The clock source that HybridClock should use. "
              "Leave empty for WallClock, clockbound for ClockboundClock, "
              "and other values depend on added clock providers and "
              "specific for appropriate tests, that adds them.");

DEFINE_UNKNOWN_bool(fail_on_out_of_range_clock_skew, true,
            "In case transactional tables are present, crash the process if clock skew greater "
            "than the configured maximum.");

DEFINE_UNKNOWN_uint64(clock_skew_force_crash_bound_usec, 60000000,
              "If the clock skew larger than this amount (microseconds) is observed, we will force "
              "a crash regardless of the value of fail_on_out_of_range_clock_skew. This is useful "
              "for avoiding really large hybrid clock jumps. Set to 0 to disable the check. Note "
              "that this check is only preformed for clock skew greater than max_clock_skew_usec.");

DEFINE_RUNTIME_int64(hybrid_clock_coarse_now_max_lag_us, 100,
    "How far behind the physical clock, in microseconds, the hybrid time may be that the clock "
    "hands to callers that do not need a unique hybrid time (CoarseNow: message stamps, lease and "
    "timeout arithmetic, the bound of a wait). Such a caller gets the latest hybrid time of the "
    "clock without advancing it, unless that is further behind than this, in which case the clock "
    "is advanced as by Now(). The effect on those callers is that of a physical clock that is "
    "slow by at most this much, so keep it far below max_clock_skew_usec. Negative: every such "
    "read advances the clock, as before this flag existed.");
TAG_FLAG(hybrid_clock_coarse_now_max_lag_us, advanced);

DECLARE_uint64(max_clock_skew_usec);


namespace yb {
namespace server {

namespace {

std::mutex providers_mutex;
std::unordered_map<std::string, PhysicalClockProvider> providers;

std::atomic<bool> clock_skew_control_enabled{false};

// options should be in format clock_name[,extra_data] and extra_data would be passed to
// clock factory.
PhysicalClockPtr GetClock(const std::string& options) {
  if (options.empty()) {
    return WallClock();
  }

  auto pos = options.find(',');
  auto name = pos == std::string::npos ? options : options.substr(0, pos);
  auto arg = pos == std::string::npos ? std::string() : options.substr(pos + 1);
  std::lock_guard lock(providers_mutex);
  auto it = providers.find(name);
  if (it == providers.end()) {
    LOG(DFATAL) << "Unknown time source: " << name;
    return WallClock();
  }
  return it->second(arg);
}

void TryRunClockboundStatus() {
  if (FLAGS_time_source != "clockbound") {
    LOG(INFO) << "Time source is not 'clockbound' (current value: "
                          << FLAGS_time_source << "), skipping clockbound status check";
    return;
  }

  LOG(INFO) << "$ systemctl status clockbound --no-pager";
  std::vector<std::string> argv = { "systemctl", "status", "clockbound", "--no-pager" };
  Status s = Subprocess::Call(argv, true);
  if (!s.ok()) {
    LOG(WARNING) << s.ToString();
  }
}

void TryRunClockDiagnosticsOnce() {
  static std::once_flag collected;
  std::call_once(collected, [] {
    TryRunChronycTracking();
    TryRunChronycSourceInfo();
    TryRunClockboundStatus();
  });
}

} // namespace

void HybridClock::RegisterProvider(std::string name, PhysicalClockProvider provider) {
  std::lock_guard lock(providers_mutex);
  providers.emplace(std::move(name), std::move(provider));
}

HybridClock::HybridClock() : HybridClock(FLAGS_time_source) {}

HybridClock::HybridClock(PhysicalClockPtr clock) : clock_(std::move(clock)) {}

HybridClock::HybridClock(const std::string& time_source) : HybridClock(GetClock(time_source)) {}

Status HybridClock::Init() {
#if defined(__APPLE__)
  LOG(WARNING) << "HybridClock initialized in local mode (OS X only). "
               << "Not suitable for distributed clusters.";
#endif // defined(__APPLE__)

  state_ = kInitialized;

  return Status::OK();
}

HybridTimeRange HybridClock::NowRange() {
  HybridTime now;
  uint64_t error;

  NowWithError(&now, &error);
  auto max_global_now = HybridTime::FromMicros(
      clock_->MaxGlobalTime({now.GetPhysicalValueMicros(), error}));
  return std::make_pair(now, max_global_now);
}

bool HybridClock::IsSkewed(const PhysicalTime& now, MicrosTime last_usec) {
  if (now.time_point >= last_usec) {
    return false;
  }
  // Propagated hybrid time cannot be higher than the global limit.
  return last_usec - now.time_point > FLAGS_max_clock_skew_usec ||
         last_usec > clock_->MaxGlobalTime(now);
}

void HybridClock::ReportSkew(const PhysicalTime& now, MicrosTime last_usec) {
  auto delta_us = last_usec - now.time_point;
  auto max_global_time = clock_->MaxGlobalTime(now);
  auto delta = MonoDelta::FromMicroseconds(delta_us);
  auto max_allowed = MonoDelta::FromMicroseconds(
      std::min(FLAGS_max_clock_skew_usec, max_global_time - now.time_point));
  if ((ANNOTATE_UNPROTECTED_READ(FLAGS_fail_on_out_of_range_clock_skew) ||
       (ANNOTATE_UNPROTECTED_READ(FLAGS_clock_skew_force_crash_bound_usec) > 0 &&
        delta_us > ANNOTATE_UNPROTECTED_READ(FLAGS_clock_skew_force_crash_bound_usec))) &&
      clock_skew_control_enabled.load(std::memory_order_acquire)) {
    TryRunClockDiagnosticsOnce();
    LOG(FATAL) << "Too big clock skew is detected: " << delta << ", while max allowed is: "
               << max_allowed << "; clock_skew_force_crash_bound_usec="
               << ANNOTATE_UNPROTECTED_READ(FLAGS_clock_skew_force_crash_bound_usec);
  } else {
    YB_LOG_EVERY_N_SECS(ERROR, 1)
        << "Too big clock skew is detected: " << delta << ", while max allowed is: "
        << max_allowed << "; clock_skew_force_crash_bound_usec="
        << ANNOTATE_UNPROTECTED_READ(FLAGS_clock_skew_force_crash_bound_usec);
  }
}

PhysicalTime HybridClock::ReadPhysicalClock() {
  auto now = clock_->Now();
  if (PREDICT_FALSE(!now.ok())) {
    TryRunClockDiagnosticsOnce();
    LOG(FATAL) << Format("Couldn't get the current time: Clock unsynchronized. "
        "Status: $0", now.status().ToString());
  }
  return *now;
}

void HybridClock::NowWithError(HybridTime *hybrid_time, uint64_t *max_error_usec) {
  DCHECK_EQ(state_, kInitialized) << "Clock not initialized. Must call Init() first.";

  auto now = ReadPhysicalClock();

  // The hybrid time to return is the physical time with a zero logical part, or, when the clock is
  // already at or past that (another call in the same microsecond, a physical clock that went
  // back, a larger hybrid time from another server), the next value after the last one returned.
  // That is max(physical, next_), and next_ becomes the result plus one.
  //
  // The usual case under load is the second: one atomic add takes the value and advances the
  // clock, and cannot fail whatever other threads do.
  const uint64_t physical = HybridTime::FromMicros(now.time_point).ToUint64();
  uint64_t issued = next_.fetch_add(1, std::memory_order_acq_rel);
  if (issued < physical) {
    // The physical clock is ahead: move the clock up to it. The value taken above is dropped (it
    // is below the physical time and nobody else can get it).
    uint64_t next = issued + 1;
    for (;;) {
      if (next > physical) {
        // Another thread moved the clock to this physical time, or past it, first.
        issued = next_.fetch_add(1, std::memory_order_acq_rel);
        break;
      }
      if (next_.compare_exchange_weak(next, physical + 1, std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
        issued = physical;
        break;
      }
    }
  }

  *hybrid_time = HybridTime(issued);
  if (issued == physical) {
    *max_error_usec = now.max_error;
    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Current clock is higher than the last one. Resetting logical values."
          << " Time: " << *hybrid_time << ", Error: " << *max_error_usec;
    }
    return;
  }

  // The clock is ahead of the physical time read above.
  const MicrosTime last_usec = HybridTime(issued + 1).GetPhysicalValueMicros();
  if (PREDICT_FALSE(IsSkewed(now, last_usec))) {
    // The physical time was read before the hybrid time was taken, so a thread that stalled in
    // between while others advanced the clock sees the clock ahead of a stale sample. A fresh read
    // is ordered after taking the hybrid time; only that one can show real skew.
    now = ReadPhysicalClock();
    if (IsSkewed(now, last_usec)) {
      ReportSkew(now, last_usec);
    }
  }

  // We don't have the last time read max error since it might have originated
  // in another machine, but we can put a bound on the maximum error of the
  // hybrid_time we are providing.
  // In particular we know that the "true" time falls within the interval
  // now_usec +- now.maxerror, where now is the physical sample kept above,
  // so we get the following situations:
  //
  // 1)
  // --------|----------|----|---------|--------------------------> time
  //     now - e       now  last   now + e
  // 2)
  // --------|----------|--------------|------|-------------------> time
  //     now - e       now         now + e   last
  // 3) A fresh sample read at or past last: the sample's own error, e, applies.
  //
  // Assuming, in the worst case, that the "true" time is now - error we need to
  // return: max(last, now) - (now - e) as the new maximum error.
  // This broadens the error interval for cases 1 and 2 but always returns
  // a correct error interval.
  *max_error_usec = std::max(last_usec, now.time_point) - (now.time_point - now.max_error);
  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    VLOG(2) << "Current clock is lower than the last one. Returning last read and incrementing"
        " logical values. Hybrid time: " << *hybrid_time << " Error: " << *max_error_usec;
  }
}

HybridTimeRange HybridClock::CoarseNowRange() {
  const int64_t max_lag_us = FLAGS_hybrid_clock_coarse_now_max_lag_us;
  if (max_lag_us < 0) {
    return NowRange();
  }
  auto now = clock_->Now();
  const uint64_t next = next_.load(std::memory_order_acquire);
  if (PREDICT_FALSE(!now.ok() || next == 0)) {
    return NowRange();
  }
  // The latest hybrid time the clock returned or was updated to: at least every hybrid time anyone
  // got from this clock before the load above, and below every hybrid time Now() returns after it.
  const HybridTime latest(next - 1);
  const MicrosTime latest_usec = latest.GetPhysicalValueMicros();
  // Not when it is too far behind the physical clock (the caller wants the current time), and not
  // when it is ahead by more than Now() allows, by either the skew flag or the physical clock's
  // MaxGlobalTime (Now() reports that, and the caller would propagate it).
  if (latest_usec + max_lag_us < now->time_point || IsSkewed(*now, latest_usec)) {
    return NowRange();
  }
  const MicrosTime usec = std::max(latest_usec, now->time_point);
  const uint64_t error = usec - (now->time_point - now->max_error);
  return std::make_pair(latest, HybridTime::FromMicros(clock_->MaxGlobalTime({usec, error})));
}

void HybridClock::Update(const HybridTime& to_update) {
  if (!to_update.is_valid()) {
    return;
  }

  // Every hybrid time returned from now on must be above to_update. Nothing is written when the
  // clock is there already, which is the usual case for a replica that follows a leader whose clock
  // is not ahead of its own.
  const uint64_t target = to_update.ToUint64() + 1;
  uint64_t next = next_.load(std::memory_order_acquire);

  // VLOG(4) crashes in TSAN mode
  if (VLOG_IS_ON(4)) {
    LOG(INFO) << __func__ << ", new: " << HybridTime(target) << ", current: " << HybridTime(next);
  }

  // Keep trying to CAS until it works or until HT has advanced past this update.
  while (next < target &&
         !next_.compare_exchange_weak(next, target, std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {}
}

// Used to get the hybrid_time for metrics.
uint64_t HybridClock::NowForMetrics() {
  return Now().ToUint64();
}

// Used to get the current error, for metrics.
uint64_t HybridClock::ErrorForMetrics() {
  HybridTime now;
  uint64_t error;

  NowWithError(&now, &error);
  return error;
}

int64_t HybridClock::SkewForMetrics() {
  const MicrosTime last_usec =
      HybridTime(next_.load(std::memory_order_acquire)).GetPhysicalValueMicros();
  auto now = clock_->Now();
  if (PREDICT_FALSE(!now.ok())) {
    LOG(DFATAL) << Format("Couldn't get the current time: Clock unsynchronized. "
        "Status: $0", now.status().ToString());
    return 0;
  }
  // Making sure we don't return a negative value.
  int64_t potential_skew = last_usec - now->time_point;
  return std::max<int64_t>(0, potential_skew);
}

void HybridClock::RegisterMetrics(const scoped_refptr<MetricEntity>& metric_entity) {
  METRIC_hybrid_clock_hybrid_time.InstantiateFunctionGauge(
      metric_entity,
      Bind(&HybridClock::NowForMetrics, Unretained(this)))
    ->AutoDetachToLastValue(&metric_detacher_);
  METRIC_hybrid_clock_error.InstantiateFunctionGauge(
      metric_entity,
      Bind(&HybridClock::ErrorForMetrics, Unretained(this)))
    ->AutoDetachToLastValue(&metric_detacher_);
  METRIC_hybrid_clock_skew.InstantiateFunctionGauge(
      metric_entity,
      Bind(&HybridClock::SkewForMetrics, Unretained(this)))
    ->AutoDetachToLastValue(&metric_detacher_);
}

void HybridClock::EnableClockSkewControl() {
  clock_skew_control_enabled.store(true, std::memory_order_release);
}

}  // namespace server
}  // namespace yb
