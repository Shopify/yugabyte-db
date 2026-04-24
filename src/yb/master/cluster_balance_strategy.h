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

#include <memory>
#include <string>

#include "yb/common/entity_ids_types.h"
#include "yb/util/std_util.h"

namespace yb {
namespace master {

class GlobalLoadState;
class PerTableLoadState;

// Outcome returned by LoadBalancerStrategy::AssessLeaderMove for a (high_uuid, low_uuid) pair.
// Mirrors the control flow of the original inline decision block in
// ClusterLoadBalancer::GetLeaderToMove so the caller can preserve legacy semantics verbatim while
// letting strategies (e.g. HeatAware) inject new trigger conditions.
struct LeaderMoveAssessment {
  // When false, the balancer advances past this (high, low) pair (either continues or breaks,
  // depending on should_break_inner_loop). When true, the caller looks for a moveable tablet
  // between high_uuid and low_uuid and propagates `reason` into LeaderMoveDetails.
  bool should_move = false;

  // Human-readable rationale, copied into LeaderMoveDetails and logged by MoveLeader.
  std::string reason;

  // Heat-aware strategies set this to true when the move was triggered on heat grounds (i.e. the
  // count-based assessment declined and a heat-bucket gap exists). Scopes cooldown bookkeeping so
  // placement-repair and blacklist-driven stepdowns do not poison the heat cooldown map.
  bool is_heat_driven = false;

  // Mirrors the pre-existing `is_global_balancing_move` local so the caller can keep its
  // can_perform_global_operations_ accounting.
  bool is_global_balancing_move = false;

  // When should_move is false, tells the caller to `break` out of the inner right-index loop
  // rather than `continue`. Reproduces the pre-existing "no more moves available at this
  // configuration" early-exit behavior.
  bool should_break_inner_loop = false;

  // When true, the caller should return std::nullopt from GetLeaderToMove entirely — no further
  // (left, right) pair is going to produce a move under this strategy. Reproduces the original
  // `load_variance == 0 && right == last_pos` early return.
  bool should_return_nullopt = false;
};

// Outcome returned by LoadBalancerStrategy::AssessTabletMove for a (high_uuid, low_uuid) pair
// in ClusterLoadBalancer::GetLoadToMove. Mirrors the control flow of the original inline decision
// block so the caller can preserve legacy semantics verbatim while letting heat-aware strategies
// inject new trigger conditions. Parallel to LeaderMoveAssessment but for tablet add/move
// decisions rather than leader stepdowns.
struct TabletMoveAssessment {
  // When true, the caller looks for a tablet to move between high_uuid and low_uuid and
  // propagates `reason` into the AddOrMoveReplica log line.
  bool should_move = false;

  // Human-readable rationale, logged by AddOrMoveReplica and surfaced in cluster balancer VLOGs.
  std::string reason;

  // Heat-aware strategies set this to true when the move was triggered on heat grounds (i.e. the
  // count-based assessment declined and a PlacementHeat-bucket gap exists). Scopes cooldown
  // bookkeeping on heat_aware_recent_tablet_moves_ so placement-repair, blacklist-drain, and
  // plain count-based tablet moves do not poison the heat cooldown map.
  bool is_heat_driven = false;

  // Mirrors the pre-existing `is_global_balancing_move` local so the caller can keep its
  // can_perform_global_operations_ accounting around cross-table global balancing.
  bool is_global_balancing_move = false;

  // When true, the caller should `break` out of the inner right-index loop rather than
  // `continue`. Reproduces the original early-exit behaviors at cluster_balance.cc:1199 and
  // cluster_balance.cc:1212.
  bool should_break_inner_loop = false;

  // When true, the caller should return false from GetLoadToMove entirely — no further
  // (left, right) pair will produce a move under this strategy. Reproduces the original
  // `load_variance == 0 && right == last_pos` early return at cluster_balance.cc:1182.
  bool should_return_false = false;
};

// Abstract scoring seam used by PerTableLoadState when sorting tablet servers by load and leader
// load. Implementations are expected to be stateless so that the same LoadScorer can be reused by
// the std::sort comparators that PerTableLoadState constructs during a balancer run.
class LoadScorer {
 public:
  virtual ~LoadScorer() = default;

  // Ascending load comparator. Returns true iff `a` is strictly less loaded than `b`.
  // When `tablet_id` is present, implementations may bias the comparison towards the drive on
  // which the tablet would land to mirror drive-aware placement.
  virtual bool CompareLoad(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& a, const TabletServerId& b,
      optional_ref<const TabletId> tablet_id) const = 0;

  // Ascending leader-load comparator. Returns true iff `a` has strictly less leader load than `b`.
  // Leader-blacklisted tablet servers are sorted to the end regardless of their actual count.
  virtual bool CompareLeaderLoad(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& a, const TabletServerId& b) const = 0;
};

// Count-based scorer. Reproduces the pre-strategy ordering exactly and is the default for the
// `count_based` strategy.
class CountBasedLoadScorer : public LoadScorer {
 public:
  bool CompareLoad(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& a, const TabletServerId& b,
      optional_ref<const TabletId> tablet_id) const override;

  bool CompareLeaderLoad(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& a, const TabletServerId& b) const override;
};

// Heat-aware scorer. Delegates CompareLoad to count-based (tablet placement does not use leader
// heat). For CompareLeaderLoad it quantizes weighted heat into integer buckets sized by
// FLAGS_load_balancer_heat_hysteresis_ops_per_sec and orders by bucket first, falling through to
// count-based within a bucket. Integer bucketing avoids the pairwise-threshold strict-weak-order
// defect that a naked |heat_a - heat_b| < threshold comparator would introduce.
class HeatAwareLoadScorer : public LoadScorer {
 public:
  bool CompareLoad(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& a, const TabletServerId& b,
      optional_ref<const TabletId> tablet_id) const override;

  bool CompareLeaderLoad(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& a, const TabletServerId& b) const override;

 private:
  CountBasedLoadScorer count_based_;
};

// Owns the scorer plus the strategy's move-assessment policy. Phase 1 exposed only the scorer;
// Phase 3 adds AssessLeaderMove so strategies can inject their own move-trigger conditions
// (the count-based variance check, heat-bucket differences, etc.) into ClusterLoadBalancer's
// leader-move loop.
class LoadBalancerStrategy {
 public:
  virtual ~LoadBalancerStrategy() = default;

  virtual const std::string& name() const = 0;
  virtual const LoadScorer& scorer() const = 0;

  // Decides whether a leader move from `high_uuid` to `low_uuid` is warranted under this strategy.
  // The caller has already sorted tservers ascending by leader load using `scorer()` and filtered
  // out blacklisted-with-zero-leaders; this hook replaces the inline reason-assignment block that
  // previously lived in ClusterLoadBalancer::GetLeaderToMove.
  //
  // `count_based_threshold_capping` signals that count-based's ShouldSkipLeaderBalancing would
  // have short-circuited balancing at the configured `leader_balance_threshold` if this run used
  // the count-based strategy. For count-based itself this is always false at the point
  // AssessLeaderMove is called (we would have already returned from GetLeaderToMove). For
  // heat-aware, it is true whenever the main loop was entered purely because a heat-bucket gap
  // override_d the threshold cap; in that case, per-pair count-driven moves must stay suppressed
  // so leader_balance_threshold semantics are preserved for count-balancing decisions while heat-
  // driven moves (and blacklist drains) remain legal.
  virtual LeaderMoveAssessment AssessLeaderMove(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& high_uuid, const TabletServerId& low_uuid,
      bool high_leader_blacklisted, bool left_equals_right,
      bool right_equals_last_pos, bool can_balance_globally,
      bool count_based_threshold_capping) const = 0;

  // Pre-loop short-circuit for GetLeaderToMove's `leader_balance_threshold` cap. Returns true iff
  // this strategy is sure no leader move is warranted given the current state and the configured
  // threshold. The count-based implementation replicates the legacy "max non-blacklisted leader
  // count <= threshold" check; heat-aware additionally requires all non-blacklisted tservers to
  // share the same heat bucket, so a hot-but-count-light tserver cannot spuriously trigger the
  // short-circuit.
  //
  // The legacy inline version walked `sorted_leader_load` right-to-left and used the rightmost
  // non-blacklisted entry as the "load max". That only works when the comparator sorts by count,
  // which the heat-aware scorer no longer does. Strategies now compute the relevant maxima
  // themselves so the short-circuit is independent of sort order.
  virtual bool ShouldSkipLeaderBalancing(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const std::vector<TabletServerId>& sorted_leader_load,
      size_t adjusted_leader_threshold) const = 0;

  // Side-channel that answers "would count-based's threshold short-circuit have fired here?"
  // Independent of the active strategy. GetLeaderToMove uses the answer to decide whether the
  // main loop is running purely because heat-aware overrode the skip, in which case the active
  // strategy's AssessLeaderMove is expected to keep plain count-driven moves suppressed.
  virtual bool CountBasedThresholdCapsBalancing(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const std::vector<TabletServerId>& sorted_leader_load,
      size_t adjusted_leader_threshold) const = 0;

  // Decides whether a tablet move from `high_uuid` to `low_uuid` is warranted under this
  // strategy. Replaces the inline `load_variance` / `is_global_balancing_move` /
  // `GetPossiblyTransientLoad` decision block that previously lived in
  // ClusterLoadBalancer::GetLoadToMove. The caller has already sorted sorted_load_ ascending
  // by this strategy's `CompareLoad` and is iterating (left, right) from outside in.
  //
  // `left_equals_right` and `right_is_last_pos` match the named state variables inside
  // GetLoadToMove. `can_balance_globally` mirrors the `CanBalanceGlobalLoad()` result once for
  // the whole call (reading it per-iteration inside the strategy would drift from the outer
  // loop's view of per-run global-balancing state).
  virtual TabletMoveAssessment AssessTabletMove(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& high_uuid, const TabletServerId& low_uuid,
      bool left_equals_right, bool right_is_last_pos, bool can_balance_globally) const = 0;
};

class CountBasedLoadBalancerStrategy : public LoadBalancerStrategy {
 public:
  const std::string& name() const override;
  const LoadScorer& scorer() const override { return scorer_; }

  LeaderMoveAssessment AssessLeaderMove(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& high_uuid, const TabletServerId& low_uuid,
      bool high_leader_blacklisted, bool left_equals_right,
      bool right_equals_last_pos, bool can_balance_globally,
      bool count_based_threshold_capping) const override;

  bool ShouldSkipLeaderBalancing(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const std::vector<TabletServerId>& sorted_leader_load,
      size_t adjusted_leader_threshold) const override;

  // For count-based the "would count-based have capped?" question is literally the same as
  // "would ShouldSkipLeaderBalancing have fired?". Delegates.
  bool CountBasedThresholdCapsBalancing(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const std::vector<TabletServerId>& sorted_leader_load,
      size_t adjusted_leader_threshold) const override;

  // Extracted verbatim from the pre-Phase-4 inline decision block in
  // ClusterLoadBalancer::GetLoadToMove. Produces the same control flow (should_move / break /
  // return_false / global-balancing fallback) that the inline code path did — any drift from
  // the pre-Phase-4 behavior is a bug.
  TabletMoveAssessment AssessTabletMove(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& high_uuid, const TabletServerId& low_uuid,
      bool left_equals_right, bool right_is_last_pos, bool can_balance_globally) const override;

 private:
  CountBasedLoadScorer scorer_;
};

class HeatAwareLoadBalancerStrategy : public LoadBalancerStrategy {
 public:
  const std::string& name() const override;
  const LoadScorer& scorer() const override { return scorer_; }

  // First asks the count-based policy. If count-based would move, returns its verdict unchanged
  // (preserves Phase 1/2 parity when heat is cold or flat). If count-based declines but
  // `bucket(Heat(high_uuid)) > bucket(Heat(low_uuid))`, returns a heat-driven move with
  // is_heat_driven=true.
  //
  // Under `count_based_threshold_capping=true` (i.e. leader_balance_threshold would have stopped
  // count-based from balancing entirely), plain count-driven should_move verdicts are suppressed
  // for non-blacklisted sources; only heat-bucket gaps (or blacklist drains) may produce a move.
  LeaderMoveAssessment AssessLeaderMove(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& high_uuid, const TabletServerId& low_uuid,
      bool high_leader_blacklisted, bool left_equals_right,
      bool right_equals_last_pos, bool can_balance_globally,
      bool count_based_threshold_capping) const override;

  // Count-based says skip only when max non-blacklisted leader count is at or below the
  // threshold; heat-aware requires that AND all non-blacklisted tservers sharing a heat bucket.
  // If any two non-blacklisted tservers span different buckets, a heat-driven move is still
  // possible even when count-based would cap out.
  bool ShouldSkipLeaderBalancing(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const std::vector<TabletServerId>& sorted_leader_load,
      size_t adjusted_leader_threshold) const override;

  // Delegates to the embedded count-based strategy. Independent of heat state — this answers
  // "would count-based have capped balancing?" which heat-aware uses to decide whether plain
  // count moves are allowed at per-pair assessment time.
  bool CountBasedThresholdCapsBalancing(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const std::vector<TabletServerId>& sorted_leader_load,
      size_t adjusted_leader_threshold) const override;

  // First asks the count-based policy. If count-based would move (including global-balancing
  // moves), returns its verdict unchanged — this preserves parity with count-based when heat
  // is cold or flat. If count-based declines and terminates (break / return_false), returns
  // its verdict unchanged so strategy does not extend the scan past count-based's search
  // bounds. Otherwise, if `bucket(PlacementHeat(high_uuid)) > bucket(PlacementHeat(low_uuid))`,
  // returns a heat-driven move with is_heat_driven=true.
  TabletMoveAssessment AssessTabletMove(
      const PerTableLoadState& state, const GlobalLoadState& global_state,
      const TabletServerId& high_uuid, const TabletServerId& low_uuid,
      bool left_equals_right, bool right_is_last_pos, bool can_balance_globally) const override;

 private:
  HeatAwareLoadScorer scorer_;
  CountBasedLoadBalancerStrategy count_based_;
};

// Flag values for --load_balancer_strategy.
extern const char* const kLoadBalancerStrategyCountBased;
extern const char* const kLoadBalancerStrategyHeatAwareExperimental;

// Factory. Returns a CountBasedLoadBalancerStrategy for `count_based`, a
// HeatAwareLoadBalancerStrategy for `heat_aware_experimental`, and falls back to count-based
// (with a warning) for any other name so that a flag typo does not silently stop balancing.
std::unique_ptr<LoadBalancerStrategy> CreateLoadBalancerStrategy(const std::string& name);

}  // namespace master
}  // namespace yb
