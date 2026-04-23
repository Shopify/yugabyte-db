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

#include "yb/master/cluster_balance_strategy.h"

#include <cmath>
#include <cstdint>

#include "yb/master/cluster_balance_util.h"

#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/logging.h"

DECLARE_double(load_balancer_heat_read_weight);
DECLARE_double(load_balancer_heat_write_weight);
DECLARE_double(load_balancer_heat_hysteresis_ops_per_sec);

namespace yb {
namespace master {

const char* const kLoadBalancerStrategyCountBased = "count_based";
const char* const kLoadBalancerStrategyHeatAwareExperimental = "heat_aware_experimental";

namespace {

const std::string kCountBasedName(kLoadBalancerStrategyCountBased);
const std::string kHeatAwareExperimentalName(kLoadBalancerStrategyHeatAwareExperimental);

double Heat(const GlobalLoadState::TServerLeaderHeat& h) {
  return FLAGS_load_balancer_heat_read_weight * h.sum_read_ops_per_sec +
         FLAGS_load_balancer_heat_write_weight * h.sum_write_ops_per_sec;
}

// Integer-valued bucket function over heat. Strict weak ordering holds by construction because
// buckets are totally ordered int64_t values; two heats that happen to be close but span a
// bucket boundary are still ordered consistently regardless of which third value they are
// compared against.
int64_t HeatBucket(double heat, double bucket_size) {
  DCHECK_GT(bucket_size, 0.0);
  return static_cast<int64_t>(std::floor(heat / bucket_size));
}

int64_t HeatBucketForTs(const GlobalLoadState& global_state, const TabletServerId& ts_uuid,
                        double bucket_size) {
  const auto it = global_state.heat_by_ts_.find(ts_uuid);
  const double heat = (it == global_state.heat_by_ts_.end()) ? 0.0 : Heat(it->second);
  return HeatBucket(heat, bucket_size);
}

}  // namespace

bool CountBasedLoadScorer::CompareLoad(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const TabletServerId& a, const TabletServerId& b,
    optional_ref<const TabletId> tablet_id) const {
  auto load_a = state.GetLoad(a);
  auto load_b = state.GetLoad(b);
  if (load_a != load_b) {
    return load_a < load_b;
  }
  // Use global load as a heuristic to help break ties.
  load_a = global_state.GetGlobalLoad(a);
  load_b = global_state.GetGlobalLoad(b);
  if (load_a != load_b) {
    return load_a < load_b;
  }
  if (tablet_id) {
    load_a = state.GetTabletDriveLoad(a, *tablet_id);
    load_b = state.GetTabletDriveLoad(b, *tablet_id);
    if (load_a != load_b) {
      return load_a < load_b;
    }
  }
  return a < b;
}

bool CountBasedLoadScorer::CompareLeaderLoad(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const TabletServerId& a, const TabletServerId& b) const {
  // Primary criteria: whether tserver is leader blacklisted.
  auto a_leader_blacklisted =
      global_state.leader_blacklisted_servers_.find(a) !=
          global_state.leader_blacklisted_servers_.end();
  auto b_leader_blacklisted =
      global_state.leader_blacklisted_servers_.find(b) !=
          global_state.leader_blacklisted_servers_.end();
  if (a_leader_blacklisted != b_leader_blacklisted) {
    return !a_leader_blacklisted;
  }

  // Use global leader load as tie-breaker.
  auto a_load = state.GetLeaderLoad(a);
  auto b_load = state.GetLeaderLoad(b);
  if (a_load == b_load) {
    a_load = global_state.GetGlobalLeaderLoad(a);
    b_load = global_state.GetGlobalLeaderLoad(b);
    if (a_load == b_load) {
      return a < b;
    }
  }
  // Secondary criteria: tserver leader load.
  return a_load < b_load;
}

bool HeatAwareLoadScorer::CompareLoad(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const TabletServerId& a, const TabletServerId& b,
    optional_ref<const TabletId> tablet_id) const {
  // Phase 3 does not alter tablet placement decisions. Heat is only available for leader peers,
  // so using it to order follower placement would be a category error; defer to count-based.
  return count_based_.CompareLoad(state, global_state, a, b, tablet_id);
}

bool HeatAwareLoadScorer::CompareLeaderLoad(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const TabletServerId& a, const TabletServerId& b) const {
  // Blacklist dominates heat: a leader-blacklisted tserver always sorts to the end, regardless
  // of how cold it might look on heat.
  const auto a_blacklisted =
      global_state.leader_blacklisted_servers_.find(a) !=
          global_state.leader_blacklisted_servers_.end();
  const auto b_blacklisted =
      global_state.leader_blacklisted_servers_.find(b) !=
          global_state.leader_blacklisted_servers_.end();
  if (a_blacklisted != b_blacklisted) {
    return !a_blacklisted;
  }

  const double bucket_size = FLAGS_load_balancer_heat_hysteresis_ops_per_sec;
  if (bucket_size > 0.0) {
    const int64_t bucket_a = HeatBucketForTs(global_state, a, bucket_size);
    const int64_t bucket_b = HeatBucketForTs(global_state, b, bucket_size);
    if (bucket_a != bucket_b) {
      return bucket_a < bucket_b;
    }
  }

  return count_based_.CompareLeaderLoad(state, global_state, a, b);
}

LeaderMoveAssessment CountBasedLoadBalancerStrategy::AssessLeaderMove(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const TabletServerId& high_uuid, const TabletServerId& low_uuid,
    bool high_leader_blacklisted, bool left_equals_right,
    bool right_equals_last_pos, bool can_balance_globally,
    bool count_based_threshold_capping) const {
  // `count_based_threshold_capping` is only meaningful when a different strategy (heat-aware)
  // decided to enter the main loop despite count-based wanting to skip. When count-based is the
  // active strategy and it would have capped, we'd have returned from GetLeaderToMove before
  // calling AssessLeaderMove. So from count-based's perspective the flag is either false or
  // moot, and we preserve the legacy inline decision verbatim.
  (void) count_based_threshold_capping;
  // Extracted verbatim from the pre-Phase-3 inline decision block in
  // ClusterLoadBalancer::GetLeaderToMove (cluster_balance.cc:1408-1450). Do not edit without
  // re-checking that original semantics are preserved.
  LeaderMoveAssessment out;

  const ssize_t high_load = static_cast<ssize_t>(state.GetLeaderLoad(high_uuid));
  const ssize_t low_load = static_cast<ssize_t>(state.GetLeaderLoad(low_uuid));
  const ssize_t load_variance = high_load - low_load;

  std::string reason;
  if (load_variance >= state.options_->kMinLoadVarianceToBalance /* 2 */) {
    reason = Format("Source tserver has more leaders for this table than destination ($0 > $1)",
                    high_load, low_load);
  } else if (high_leader_blacklisted) {
    reason = Format("Leader is on leader blacklisted tserver", high_uuid);
  }

  if (!left_equals_right && !reason.empty()) {
    out.should_move = true;
    out.reason = std::move(reason);
    return out;
  }

  // Either left == right (cannot rebalance onto self) or no per-table rationale was produced.
  // Fall through to the global-load branch.
  if (load_variance == 0 && right_equals_last_pos) {
    // Original behavior: on the very first inner-loop iteration (right == last_pos) with zero
    // leader-load variance between the current left and the top of the sorted list, no moves
    // are possible anywhere — the list is sorted ascending. Bail the entire function.
    out.should_return_nullopt = true;
    return out;
  }

  if (load_variance > 0 && can_balance_globally) {
    const auto global_high_load = global_state.GetGlobalLeaderLoad(high_uuid);
    const auto global_low_load = global_state.GetGlobalLeaderLoad(low_uuid);
    const int global_load_variance = global_high_load - global_low_load;
    if (global_load_variance < state.options_->kMinLoadVarianceToBalance /* 2 */) {
      out.should_break_inner_loop = true;
      return out;
    }
    out.should_move = true;
    out.reason = Format("Source tserver has more global leaders than destination ($0 > $1)",
                        global_high_load, global_low_load);
    out.is_global_balancing_move = true;
    return out;
  }

  out.should_break_inner_loop = true;
  return out;
}

bool CountBasedLoadBalancerStrategy::ShouldSkipLeaderBalancing(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const std::vector<TabletServerId>& sorted_leader_load,
    size_t adjusted_leader_threshold) const {
  // leader_balance_threshold=0 means "always balance" — never short-circuit here.
  if (adjusted_leader_threshold == 0) {
    return false;
  }
  // Reproduce the original "rightmost non-blacklisted's load <= threshold" check, but compute
  // the max explicitly so the result is sort-order independent. Under count-based sort the
  // rightmost non-blacklisted is the count-max, so this preserves legacy behavior exactly.
  size_t count_max = 0;
  for (const auto& uuid : sorted_leader_load) {
    if (global_state.leader_blacklisted_servers_.find(uuid) !=
        global_state.leader_blacklisted_servers_.end()) {
      continue;
    }
    count_max = std::max(count_max, state.GetLeaderLoad(uuid));
  }
  return count_max <= adjusted_leader_threshold;
}

bool CountBasedLoadBalancerStrategy::CountBasedThresholdCapsBalancing(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const std::vector<TabletServerId>& sorted_leader_load,
    size_t adjusted_leader_threshold) const {
  return ShouldSkipLeaderBalancing(state, global_state, sorted_leader_load,
                                   adjusted_leader_threshold);
}

const std::string& CountBasedLoadBalancerStrategy::name() const {
  return kCountBasedName;
}

LeaderMoveAssessment HeatAwareLoadBalancerStrategy::AssessLeaderMove(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const TabletServerId& high_uuid, const TabletServerId& low_uuid,
    bool high_leader_blacklisted, bool left_equals_right,
    bool right_equals_last_pos, bool can_balance_globally,
    bool count_based_threshold_capping) const {
  // Ask the count-based policy first. If it wants to move, pass the verdict through unchanged —
  // this preserves parity with the count-based strategy when heat is cold or flat (the key
  // invariant tested by `HeatAwareExperimentalFallsBackToCountBased`).
  LeaderMoveAssessment count_based = count_based_.AssessLeaderMove(
      state, global_state, high_uuid, low_uuid,
      high_leader_blacklisted, left_equals_right, right_equals_last_pos, can_balance_globally,
      count_based_threshold_capping);

  // When leader_balance_threshold would have capped count-based balancing for this table and we
  // only reached the main loop because a heat-bucket gap overrode the cap, plain count-driven
  // moves must stay suppressed — otherwise an unrelated (low, high-count) pair without any heat
  // signal would let count-balancing run in disguise, defeating threshold semantics. Blacklist
  // drains remain legal because they bypass the threshold in the legacy pre-loop check as well.
  //
  // The suppression only zeroes the should_move verdict: break / return_nullopt / reason are all
  // left untouched. For values count-based already had set, we fall through to the heat-override
  // check so that this (high, low) pair can still qualify on heat grounds alone.
  const bool suppress_count_move =
      count_based.should_move && count_based_threshold_capping && !high_leader_blacklisted;
  if (suppress_count_move) {
    count_based.should_move = false;
    count_based.reason.clear();
    count_based.is_global_balancing_move = false;
  }

  if (count_based.should_move) {
    return count_based;
  }

  // Count-based declined (or we suppressed it). Heat may still want a move between this
  // (high, low) pair. When heat override cannot fire, we must distinguish two cases:
  //
  // (1) `left_equals_right`: the inner loop has reached its structural bound (right met left).
  //     Advance the outer loop by setting should_break_inner_loop ourselves. Critically, we do
  //     NOT just fall through to `continue` here — that would let the inner loop walk right below
  //     left, where (under heat-sort) a same-count pair can have count[right] > count[left]
  //     across bucket boundaries, and count-based would fire should_move for a backward move
  //     (leader from a cool, count-heavy source onto a hot, count-light destination). The
  //     structural break is a heat-aware-independent invariant, so we re-assert it explicitly
  //     rather than propagating count-based's version (which carries count-sort assumptions).
  //
  // (2) bucket_size disabled, or buckets are equal: return a plain decline with no termination
  //     flags. The inner loop continues --right, eventually hitting left_equals_right and
  //     breaking via (1). This lets the scan examine pairs count-based would have skipped but
  //     never walks right past the structural bound.
  //
  // Why not propagate count_based.should_break_inner_loop / should_return_nullopt directly:
  // those flags encode count-sort invariants ("right=last_pos is count-max", "within-bucket
  // count-ascending") that are sound only for count-sorted input. Under heat-sort, sorted_leader_load_
  // is ordered by (bucket, count); same-bucket pairs still satisfy count-sort, but count-based's
  // break/nullopt verdict at one pair can no longer be extrapolated to "no move exists elsewhere
  // in the scan" because bucket boundaries decouple count order.
  if (left_equals_right) {
    LeaderMoveAssessment out;
    out.should_break_inner_loop = true;
    return out;
  }
  const double bucket_size = FLAGS_load_balancer_heat_hysteresis_ops_per_sec;
  if (bucket_size <= 0.0) {
    return LeaderMoveAssessment{};
  }
  const int64_t high_bucket = HeatBucketForTs(global_state, high_uuid, bucket_size);
  const int64_t low_bucket = HeatBucketForTs(global_state, low_uuid, bucket_size);
  if (high_bucket <= low_bucket) {
    return LeaderMoveAssessment{};
  }

  // Heat-aware sort reorders sorted_leader_load_ so that position 0 is the coolest-bucket tserver,
  // not necessarily the count-lightest. ClusterLoadBalancer::GetLeaderToMove still treats the left
  // side (sorted_leader_load_[left]) as the move destination, so under heat sort we can reach
  // pairs where the destination is count-heavier than the source. Firing a heat move here would
  // actively worsen leader-count variance (e.g. buckets/counts {A:0/10, B:1/0, C:2/1} would
  // otherwise emit C → A and push A from 10 leaders to 11). Refuse any heat move whose destination
  // is strictly count-heavier than the source; the outer loop will advance and, if a count-sensible
  // heat-viable pair exists elsewhere in the scan, surface it there. If no such pair exists for
  // this run, the loop exhausts and GetLeaderToMove returns std::nullopt — the correct outcome
  // when heat pressure and count pressure point in opposite directions.
  const ssize_t high_load = static_cast<ssize_t>(state.GetLeaderLoad(high_uuid));
  const ssize_t low_load = static_cast<ssize_t>(state.GetLeaderLoad(low_uuid));
  if (low_load > high_load) {
    return LeaderMoveAssessment{};
  }

  LeaderMoveAssessment out;
  out.should_move = true;
  out.is_heat_driven = true;
  out.reason = Format(
      "Heat imbalance: source heat bucket $0 exceeds destination heat bucket $1 "
      "(bucket_size=$2 ops/sec)",
      high_bucket, low_bucket, bucket_size);
  return out;
}

bool HeatAwareLoadBalancerStrategy::ShouldSkipLeaderBalancing(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const std::vector<TabletServerId>& sorted_leader_load,
    size_t adjusted_leader_threshold) const {
  // If count-based would not short-circuit (count-max above threshold), definitely don't skip —
  // a count-driven move is warranted independent of heat.
  if (!count_based_.ShouldSkipLeaderBalancing(
          state, global_state, sorted_leader_load, adjusted_leader_threshold)) {
    return false;
  }
  // Count-based wants to skip. A heat-driven move is still possible, but only if:
  //   (a) some tserver that actually owns leaders for this table (GetLeaderLoad(uuid) > 0) sits
  //       in a higher heat bucket than some candidate destination, and
  //   (b) the hysteresis flag is a positive bucket size.
  // Rationale for the leader-load gate: global_state.heat_by_ts_ is aggregated across all tables,
  // so a tserver may look hot purely because of a different table while owning zero leaders for
  // this one. Admitting such a tserver as a hot "source" would defeat leader_balance_threshold
  // on small/capped tables, letting count-based balancing run for no heat reason on this table.
  // Destinations do not need leaders for this table — a cold tserver that will receive a leader
  // is a valid target regardless of its current leader count.
  const double bucket_size = FLAGS_load_balancer_heat_hysteresis_ops_per_sec;
  if (bucket_size <= 0.0) {
    return true;
  }
  bool have_source = false;
  int64_t source_max_bucket = 0;
  bool have_dest = false;
  int64_t dest_min_bucket = 0;
  for (const auto& uuid : sorted_leader_load) {
    if (global_state.leader_blacklisted_servers_.find(uuid) !=
        global_state.leader_blacklisted_servers_.end()) {
      continue;
    }
    const int64_t b = HeatBucketForTs(global_state, uuid, bucket_size);
    if (state.GetLeaderLoad(uuid) > 0) {
      source_max_bucket = have_source ? std::max(source_max_bucket, b) : b;
      have_source = true;
    }
    dest_min_bucket = have_dest ? std::min(dest_min_bucket, b) : b;
    have_dest = true;
  }
  if (!have_source || !have_dest) {
    return true;
  }
  // A heat-driven move requires a source strictly hotter than some destination.
  return source_max_bucket <= dest_min_bucket;
}

bool HeatAwareLoadBalancerStrategy::CountBasedThresholdCapsBalancing(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const std::vector<TabletServerId>& sorted_leader_load,
    size_t adjusted_leader_threshold) const {
  return count_based_.ShouldSkipLeaderBalancing(state, global_state, sorted_leader_load,
                                                adjusted_leader_threshold);
}

const std::string& HeatAwareLoadBalancerStrategy::name() const {
  return kHeatAwareExperimentalName;
}

std::unique_ptr<LoadBalancerStrategy> CreateLoadBalancerStrategy(const std::string& name) {
  if (name == kLoadBalancerStrategyCountBased) {
    return std::make_unique<CountBasedLoadBalancerStrategy>();
  }
  if (name == kLoadBalancerStrategyHeatAwareExperimental) {
    return std::make_unique<HeatAwareLoadBalancerStrategy>();
  }
  YB_LOG_EVERY_N_SECS(WARNING, 60)
      << "Unknown load_balancer_strategy '" << name << "'; falling back to "
      << kLoadBalancerStrategyCountBased;
  return std::make_unique<CountBasedLoadBalancerStrategy>();
}

}  // namespace master
}  // namespace yb
