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
DECLARE_double(load_balancer_heat_replication_write_weight);
DECLARE_double(load_balancer_heat_placement_hysteresis_ops_per_sec);

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

// Placement heat: weighted composition of this tserver's write-side costs — both the leader-side
// write work (from heat_by_ts_) and the follower-side replicated writes (from tablet_heat_by_ts_).
// Read heat is deliberately EXCLUDED from this score. A tablet *move* redistributes which tserver
// holds each replica, but it does not redistribute read traffic: YugabyteDB reads flow to the
// Raft leader by default (follower reads are opt-in and unmeasured by leader-side telemetry), so
// moving an arbitrary replica — which GetTabletToMove is free to pick from any running tablet on
// the source, including tablets that tserver follows, not leads — does not necessarily relieve
// the measured read hotspot. Baking reads into this score made CompareLoad sort on a signal that
// the Phase 4 mechanism cannot act on, producing remote-bootstrap churn that did not reduce read
// load. Read hotspots belong to the Phase 3 leader-move path (CompareLeaderLoad / Heat()), which
// *can* redistribute read traffic by stepping down leadership — and which still scores reads via
// `load_balancer_heat_read_weight`. See reviewer comment at cluster_balance_strategy.cc:75-77 for
// the detailed argument.
//
// tablet_heat_by_ts_ is "every peer's write cost" and therefore double-counts the leader's own
// write contribution for every tablet it leads; subtract it out so the two terms add cleanly.
// Missing entries on either side contribute 0, matching the "no telemetry ⇒ no influence"
// convention used throughout the heat path.
double PlacementHeat(const GlobalLoadState& gs, const TabletServerId& ts) {
  // load_balancer_heat_replication_write_weight = 0 is documented as "collapse Phase 4 to Phase 3":
  // no tablet-move behavior driven by write-heat. A follower move cannot redistribute leader-side
  // write work (writes always land on the Raft leader, wherever it sits), so even if the leader-
  // write term were non-zero the move wouldn't shift the signal that selected the pair. Returning
  // 0 here gates the entire tablet-move heat path on the replication knob, matching the contract
  // covered by HeatAwareReplicationWeightZeroSuppressesTabletMove.
  if (FLAGS_load_balancer_heat_replication_write_weight <= 0.0) {
    return 0.0;
  }

  double leader_write_heat = 0.0;
  double leader_write_rate = 0.0;
  const auto leader_it = gs.heat_by_ts_.find(ts);
  if (leader_it != gs.heat_by_ts_.end()) {
    leader_write_heat =
        FLAGS_load_balancer_heat_write_weight * leader_it->second.sum_write_ops_per_sec;
    leader_write_rate = leader_it->second.sum_write_ops_per_sec;
  }

  double replicated_write_heat = 0.0;
  const auto repl_it = gs.tablet_heat_by_ts_.find(ts);
  if (repl_it != gs.tablet_heat_by_ts_.end()) {
    // tablet_heat_by_ts_ includes the leader's own write contribution for every tablet it leads.
    // Subtracting `leader_write_rate` here leaves exactly the follower-side replicated writes,
    // which is what the replication_write_weight knob should scale.
    const double follower_only = std::max(0.0, repl_it->second - leader_write_rate);
    replicated_write_heat = FLAGS_load_balancer_heat_replication_write_weight * follower_only;
  }

  return leader_write_heat + replicated_write_heat;
}

int64_t PlacementHeatBucket(double heat, double bucket_size) {
  DCHECK_GT(bucket_size, 0.0);
  return static_cast<int64_t>(std::floor(heat / bucket_size));
}

int64_t PlacementHeatBucketForTs(const GlobalLoadState& gs, const TabletServerId& ts_uuid,
                                 double bucket_size) {
  return PlacementHeatBucket(PlacementHeat(gs, ts_uuid), bucket_size);
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
  // Phase 4: order by placement-heat bucket first, falling through to count-based within a
  // bucket. Placement heat sums leader-side read/write plus follower-side replicated writes
  // (see PlacementHeat above) so this comparator is the right signal for tablet add/remove
  // decisions — which is what CompareLoad drives via LoadComparator and SortLoad.
  //
  // Unlike CompareLeaderLoad we do NOT add a blacklist short-circuit here: the count-based
  // CompareLoad has no blacklist branch either, since blacklisted tservers are filtered out
  // of add/remove decisions by HandleAddIfMissingPlacement / HandleRemoveIfWrongPlacement
  // before sorting runs. Adding one would diverge from count-based in cases where the heat
  // path is supposed to be invisible.
  //
  // Strict weak ordering is preserved by integer bucketing: bucket IDs are totally ordered
  // int64_t values, and the count-based comparator (used as the within-bucket tiebreaker) is
  // already transitive. If the hysteresis flag is disabled (<= 0), fall straight through to
  // count-based so nothing heat-aware changes — a runtime-disable escape hatch.
  const double bucket_size = FLAGS_load_balancer_heat_placement_hysteresis_ops_per_sec;
  if (bucket_size > 0.0) {
    const int64_t bucket_a = PlacementHeatBucketForTs(global_state, a, bucket_size);
    const int64_t bucket_b = PlacementHeatBucketForTs(global_state, b, bucket_size);
    if (bucket_a != bucket_b) {
      return bucket_a < bucket_b;
    }
  }
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

TabletMoveAssessment CountBasedLoadBalancerStrategy::AssessTabletMove(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const TabletServerId& high_uuid, const TabletServerId& low_uuid,
    bool left_equals_right, bool right_is_last_pos, bool can_balance_globally) const {
  // Extracted verbatim from the pre-Phase-4 inline decision block in
  // ClusterLoadBalancer::GetLoadToMove (cluster_balance.cc:1196-1238). Do not edit without
  // re-checking that original semantics are preserved — the parity-preserving tests in
  // load_balancer_mocked-test depend on byte-for-byte behavior.
  TabletMoveAssessment out;

  const ssize_t load_variance = static_cast<ssize_t>(state.GetLoad(high_uuid)) -
                                static_cast<ssize_t>(state.GetLoad(low_uuid));
  bool is_global_balancing_move = false;

  // Outer "state change or end conditions" block.
  if (left_equals_right || load_variance < state.options_->kMinLoadVarianceToBalance) {
    if (right_is_last_pos && load_variance == 0) {
      // Either both left and right are at the end, or there is no load variance anywhere, so
      // no moves are possible under this sort order. Bail the entire GetLoadToMove call.
      out.should_return_false = true;
      return out;
    }
    if (load_variance > 0 && can_balance_globally) {
      const int global_load_variance = global_state.GetGlobalLoad(high_uuid) -
                                       global_state.GetGlobalLoad(low_uuid);
      if (global_load_variance < state.options_->kMinLoadVarianceToBalance) {
        // Already globally balanced. Since we are sorted ascending by global load, there are
        // no other (low, high) pairs that could produce a global-balancing move either.
        out.should_return_false = true;
        return out;
      }
      is_global_balancing_move = true;
    } else {
      // Variance too low AND no global-balancing rescue. Advance the outer loop by breaking
      // out of the inner scan.
      out.should_break_inner_loop = true;
      return out;
    }
  }

  // Transient-load check. If the high tserver has over-replicated tablets about to be removed,
  // moving more onto the low side would over-commit once removals land. Skip this pair.
  if (!is_global_balancing_move &&
      load_variance - static_cast<ssize_t>(state.GetPossiblyTransientLoad(high_uuid)) <
          state.options_->kMinLoadVarianceToBalance) {
    // Neither should_move nor break nor return — "continue to next right-index iteration".
    return out;
  }

  out.should_move = true;
  out.is_global_balancing_move = is_global_balancing_move;
  out.reason = is_global_balancing_move ?
      Format("Source tserver has more tablets (globally) than destination ($0 > $1)",
             global_state.GetGlobalLoad(high_uuid), global_state.GetGlobalLoad(low_uuid)) :
      Format("Source tserver has more tablets for this table than destination ($0 > $1)",
             state.GetLoad(high_uuid), state.GetLoad(low_uuid));
  return out;
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

TabletMoveAssessment HeatAwareLoadBalancerStrategy::AssessTabletMove(
    const PerTableLoadState& state, const GlobalLoadState& global_state,
    const TabletServerId& high_uuid, const TabletServerId& low_uuid,
    bool left_equals_right, bool right_is_last_pos, bool can_balance_globally) const {
  // Ask count-based first. If it wants to move (including global balancing), pass the verdict
  // through — preserves parity with count-based when heat is cold or flat and guarantees that
  // count-unequal tablet counts always drive the decision even under the heat-aware strategy.
  TabletMoveAssessment count_based = count_based_.AssessTabletMove(
      state, global_state, high_uuid, low_uuid, left_equals_right, right_is_last_pos,
      can_balance_globally);
  if (count_based.should_move) {
    return count_based;
  }

  // Count-based declined. Every refusal path below returns a plain decline rather than
  // propagating count_based's verdict, because count_based's termination flags
  // (should_return_false / should_break_inner_loop) encode a COUNT-ascending sort invariant that
  // heat-aware sort only satisfies WITHIN a single bucket. Across bucket boundaries the next
  // `right` candidate may sit in a cooler bucket with a LARGER count than the current high —
  // where a valid count-driven move exists — and inheriting count_based's break/return_false
  // would mask it. Mirrors AssessLeaderMove's treatment; see that function for the full
  // argument.
  //
  // The one termination signal we *do* re-assert here is the structural `left_equals_right`
  // bound: the inner loop has reached its structural limit and must advance the outer loop.
  // We set should_break_inner_loop explicitly rather than propagating count_based's version
  // (which carries count-sort assumptions) so the intent is independent of how
  // CountBasedLoadBalancerStrategy::AssessTabletMove encodes it.
  if (left_equals_right) {
    TabletMoveAssessment out;
    out.should_break_inner_loop = true;
    return out;
  }

  const double bucket_size = FLAGS_load_balancer_heat_placement_hysteresis_ops_per_sec;
  if (bucket_size <= 0.0) {
    return TabletMoveAssessment{};  // Hysteresis disabled; no heat override available.
  }
  const int64_t high_bucket = PlacementHeatBucketForTs(global_state, high_uuid, bucket_size);
  const int64_t low_bucket = PlacementHeatBucketForTs(global_state, low_uuid, bucket_size);
  if (high_bucket <= low_bucket) {
    return TabletMoveAssessment{};  // No heat gap at this pair; continue the scan.
  }

  // Count-regression guard. Heat-aware sort orders `sorted_load_` bucket-first and count-second-
  // within-bucket, so after the heat rank flips, `low_uuid` (the sort's "low" end) may actually
  // have strictly MORE tablets than `high_uuid`. Issuing AddOrMoveReplica(high -> low) in that
  // state would take a count-heavy tserver `low` and pile another replica onto it, and the
  // subsequent over-replicated remove will drop the peer from the only peer set that still
  // includes the original (cool) `high` — so the next run observes a cluster that is MORE
  // count-imbalanced, not less. Mirror AssessLeaderMove's guard: refuse strict count-regressions,
  // but keep the `low_load == high_load` case (the canonical Phase 4 scenario where counts are
  // perfectly balanced and heat alone must drive the decision).
  //
  // On refusal, return a plain decline (TabletMoveAssessment{}) — NOT count_based — so the outer
  // loop continues decrementing `right` instead of breaking. Reason: count_based's termination
  // flags (should_break_inner_loop / should_return_false) are derived from load_variance under
  // count-ascending sort, where decrementing right only encounters tservers with strictly smaller
  // count. Under heat-aware sort the inner loop crosses bucket boundaries as right decrements, so
  // the next right candidate may have a LARGER count than the current high (if it sits in a cooler
  // bucket) and yield a valid count-driven move. Propagating count_based's break here would mask
  // that move. Mirrors AssessLeaderMove's treatment at cluster_balance_strategy.cc:466-470.
  const ssize_t high_load = static_cast<ssize_t>(state.GetLoad(high_uuid));
  const ssize_t low_load = static_cast<ssize_t>(state.GetLoad(low_uuid));
  if (low_load > high_load) {
    return TabletMoveAssessment{};
  }

  // Global-load regression guard. Count-based's AssessTabletMove only consults global load when
  // the PER-TABLE variance is already positive (see the `load_variance > 0 && can_balance_globally`
  // branch in CountBasedLoadBalancerStrategy::AssessTabletMove). On a locally count-balanced table
  // — the canonical Phase 4 scenario — that branch never fires, and count-based returns with no
  // should_move signal. If we then issue a heat-driven high -> low move without checking global
  // load, we can happily pile another replica onto a tserver that is already globally heavier,
  // because heat-aware sort orders by heat bucket and ignores global placement. Guard against that:
  // when global balancing is enabled, refuse heat-driven moves whose source is globally lighter
  // than OR equal to the destination (GetGlobalLoad(high) <= GetGlobalLoad(low)).
  //
  // Equal global loads are ALSO refused: the heat move commits the add on `low` immediately but
  // the paired remove on `high` does not land until the next run. Between add and remove the
  // global spread is 1 (low=+1); after the remove it is 2 (low=+1, high=-1). When global
  // balancing is enabled the next run's count-based global branch will see that 2-tablet skew
  // and issue a compensating move — potentially moving the just-added replica right back to the
  // source, because count-based global balancing has no heat awareness. Allowing equal-global
  // heat moves makes the heat path fight the global-balancing policy and wastes move budget on
  // two cancelling moves. Refuse upfront.
  //
  // On refusal return a plain decline (TabletMoveAssessment{}) for the same reason as the
  // count-regression path above: the outer loop must continue the scan rather than inherit
  // count_based's termination flags, which were derived under an assumption of count-ascending
  // sort that heat-aware ordering does not satisfy.
  //
  // `can_balance_globally` is plumbed from ClusterLoadBalancer::CanBalanceGlobalLoad(), which is
  // false when FLAGS_enable_global_load_balancing is off or when an earlier within-table move has
  // already committed this run (consumed the global-balancing budget). In those cases operators
  // have either disabled global balancing outright or we have no global budget left to spend — so
  // there is no global policy for the heat path to fight, and it is correct to skip this guard
  // and let heat alone drive the decision.
  if (can_balance_globally) {
    const ssize_t high_global = global_state.GetGlobalLoad(high_uuid);
    const ssize_t low_global = global_state.GetGlobalLoad(low_uuid);
    if (high_global <= low_global) {
      return TabletMoveAssessment{};
    }
  }

  // Transient-load guard. Mirror CountBasedLoadBalancerStrategy::AssessTabletMove's skip at
  // cluster_balance_strategy.cc:370-377: when the apparent load variance is at or above the
  // balancing threshold BUT entirely (or near-entirely) absorbed by over-replicated tablets
  // queued for removal on the hot source, count-based deliberately declined the move because
  // the imbalance will resolve itself once those removes land. Letting the heat override fire
  // anyway would take the move beyond the natural post-cleanup state — the destination
  // receives a fresh add, the transient removes drop high's count, and the result is a
  // destination that is count-heavier than the source. Refuse instead.
  //
  // Deliberately gated on `load_variance >= kMinLoadVarianceToBalance` so the canonical
  // Phase 4 scenario (counts genuinely flat, transient == 0, heat alone drives the decision)
  // still fires. In that scenario load_variance == 0, so this branch is never entered and
  // control falls through to the should_move assignment below.
  const ssize_t load_variance = high_load - low_load;
  const ssize_t transient_high =
      static_cast<ssize_t>(state.GetPossiblyTransientLoad(high_uuid));
  if (load_variance >= state.options_->kMinLoadVarianceToBalance &&
      load_variance - transient_high < state.options_->kMinLoadVarianceToBalance) {
    return TabletMoveAssessment{};
  }

  TabletMoveAssessment out;
  out.should_move = true;
  out.is_heat_driven = true;
  out.is_global_balancing_move = false;
  out.reason = Format(
      "Placement heat imbalance: source bucket $0 exceeds destination bucket $1 "
      "(bucket_size=$2 ops/sec)", high_bucket, low_bucket, bucket_size);
  return out;
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
