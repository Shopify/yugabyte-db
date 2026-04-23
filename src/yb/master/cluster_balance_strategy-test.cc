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

#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "yb/gutil/dynamic_annotations.h"
#include "yb/master/cluster_balance_strategy.h"
#include "yb/master/load_balancer_mocked-test_base.h"
#include "yb/util/flags.h"

DECLARE_double(load_balancer_heat_hysteresis_ops_per_sec);
DECLARE_int32(load_balancer_heat_leader_move_cooldown_secs);
DECLARE_int32(leader_balance_threshold);
DECLARE_bool(enable_load_balancer_heat_telemetry);

namespace yb {
namespace master {

// Golden-trace parity test. Phase 1 of heat-aware balancing introduces a LoadScorer /
// LoadBalancerStrategy seam inside the cluster balancer. This test pins down that the count-based
// strategy (the default) continues to produce the same sequence of add/leader/remove decisions on
// a fixed unbalanced scenario, regardless of whether the active strategy was constructed via
// `count_based` or via `heat_aware_experimental` (which in phase 1 transparently falls back to
// count-based).
class ClusterBalanceStrategyParityTest : public LoadBalancerMockedBase {
 protected:
  struct Decision {
    std::string action;
    TabletId tablet;
    TabletServerId from_ts;
    TabletServerId to_ts;

    std::string ToString() const {
      std::ostringstream out;
      out << action << "(" << tablet << ", " << from_ts << " -> " << to_ts << ")";
      return out.str();
    }

    bool operator==(const Decision& other) const {
      return action == other.action && tablet == other.tablet && from_ts == other.from_ts &&
             to_ts == other.to_ts;
    }
  };

  // Sets up a multi-az cluster with the default 4 tablets / 3 replicas plus an extra empty tserver
  // in zone "a", so the count-based balancer has concrete add moves to make.
  void PrepareImbalancedScenario() {
    PrepareTestStateMultiAz();
    replication_info_.mutable_live_replicas()->set_num_replicas(NumReplicas());
    ts_descs_.push_back(SetupTS("3333", "a"));
  }

  // Drives the balancer through a bounded number of add and leader-move decisions, recording each.
  // Uses a cap so a runaway strategy can't spin forever if parity regresses.
  // `post_reset_hook`, if provided, runs after the per-run global state is rebuilt but before any
  // decisions are taken — the right place to seed heat aggregates. A hook run is followed by an
  // unconditional re-sort so any heat values the hook seeded influence sorted_leader_load_, which
  // mirrors production (heat aggregation precedes AnalyzeTablets → SortLeaderLoad).
  Result<std::vector<Decision>> CollectTrace(
      int max_decisions = 12,
      std::function<void()> post_reset_hook = {}) NO_THREAD_SAFETY_ANALYSIS {
    std::vector<Decision> trace;
    RETURN_NOT_OK(ResetLoadBalancerAndAnalyzeTablets());
    if (post_reset_hook) {
      post_reset_hook();
      cb_.ResortAfterHeatSeedForTest();
    }
    for (int i = 0; i < max_decisions; ++i) {
      TabletId tablet;
      TabletServerId from_ts;
      TabletServerId to_ts;

      auto add_result = HandleAddReplicas(&tablet, &from_ts, &to_ts);
      if (add_result.ok() && *add_result) {
        trace.push_back({"add", tablet, from_ts, to_ts});
        continue;
      }

      auto leader_result = HandleLeaderMoves(&tablet, &from_ts, &to_ts);
      if (leader_result.ok() && *leader_result) {
        trace.push_back({"leader", tablet, from_ts, to_ts});
        continue;
      }

      // No more decisions to record.
      break;
    }
    return trace;
  }
};

TEST_F(ClusterBalanceStrategyParityTest, CountBasedStrategyProducesStableTrace) {
  PrepareImbalancedScenario();

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));
  auto trace_a = ASSERT_RESULT(CollectTrace());
  ASSERT_FALSE(trace_a.empty()) << "Expected at least one rebalancing decision for an imbalanced "
                                << "scenario";

  // Running the same scenario again from a fresh state must produce an identical trace. This
  // catches accidental reliance on run-to-run mutable state inside the scorer.
  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));
  auto trace_b = ASSERT_RESULT(CollectTrace());
  ASSERT_EQ(trace_a.size(), trace_b.size());
  for (size_t i = 0; i < trace_a.size(); ++i) {
    EXPECT_EQ(trace_a[i], trace_b[i]) << "Decision " << i << " differs between repeated runs: "
                                      << trace_a[i].ToString() << " vs " << trace_b[i].ToString();
  }
}

TEST_F(ClusterBalanceStrategyParityTest, HeatAwareExperimentalFallsBackToCountBased) {
  PrepareImbalancedScenario();

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));
  auto count_trace = ASSERT_RESULT(CollectTrace());

  // heat_aware_experimental is reserved for phase 3 and currently falls back to the count-based
  // strategy. The resulting trace must match count_based exactly.
  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  auto heat_trace = ASSERT_RESULT(CollectTrace());

  ASSERT_EQ(count_trace.size(), heat_trace.size());
  for (size_t i = 0; i < count_trace.size(); ++i) {
    EXPECT_EQ(count_trace[i], heat_trace[i])
        << "Decision " << i << " differs under heat_aware_experimental fallback: "
        << count_trace[i].ToString() << " vs " << heat_trace[i].ToString();
  }
}

TEST_F(ClusterBalanceStrategyParityTest, UnknownStrategyFallsBackToCountBased) {
  PrepareImbalancedScenario();

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));
  auto count_trace = ASSERT_RESULT(CollectTrace());

  // An unrecognized strategy name must be treated as count-based so that a typo in the flag does
  // not silently stop balancing.
  cb_.SetStrategyForTest(CreateLoadBalancerStrategy("not_a_real_strategy"));
  auto unknown_trace = ASSERT_RESULT(CollectTrace());

  ASSERT_EQ(count_trace.size(), unknown_trace.size());
  for (size_t i = 0; i < count_trace.size(); ++i) {
    EXPECT_EQ(count_trace[i], unknown_trace[i]);
  }
}

// Phase 2 adds a `heat_by_ts_` aggregate on `GlobalLoadState` but the count-based strategy must
// continue to ignore it. Seed arbitrary per-tserver heat values and assert the decision trace is
// identical to a run with no heat seeded.
TEST_F(ClusterBalanceStrategyParityTest, CountBasedStrategyIgnoresHeat) {
  PrepareImbalancedScenario();

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));
  auto baseline_trace = ASSERT_RESULT(CollectTrace());

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));
  // Seed extreme heat on the tservers that exist in PrepareImbalancedScenario. PrepareTestStateMultiAz
  // registers tservers with uuids "0000"/"1111"/"2222" and this test appends "3333". Seeding on a
  // uuid the balancer never sorts (e.g. a bare "0") would silently make this test a false
  // negative — count-based's heat-agnostic property would look preserved only because the heat
  // entry never attaches to a tserver in sorted_leader_load_. The count-based scorer looks only
  // at leader counts, so any heat value on a real tserver must not perturb the trace.
  auto seed_heat = [this]() {
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = 1e6, .sum_write_ops_per_sec = 1e6,
                 .leader_tablet_count = 42});
    cb_.SetHeatForTest(
        "3333", {.sum_read_ops_per_sec = 0, .sum_write_ops_per_sec = 0,
                 .leader_tablet_count = 0});
  };
  auto heat_trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, seed_heat));

  ASSERT_EQ(baseline_trace.size(), heat_trace.size());
  for (size_t i = 0; i < baseline_trace.size(); ++i) {
    EXPECT_EQ(baseline_trace[i], heat_trace[i])
        << "Decision " << i << " differs when heat is seeded: "
        << baseline_trace[i].ToString() << " vs " << heat_trace[i].ToString();
  }
}

// Phase 3. Count-based by itself cannot rebalance leader_counts {2,1,1} because the raw
// variance (1) is below kMinLoadVarianceToBalance (=2). The heat-aware strategy must still move a
// leader off the hot tserver in this configuration — exactly the case that motivated promoting
// AssessLeaderMove into a strategy hook.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareMovesLeaderOffHotterTsWhenCountBalanced) {
  PrepareTestStateMultiAz();

  // Sanity: count-based produces zero leader moves in this already-count-balanced scenario.
  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));
  auto count_trace = ASSERT_RESULT(CollectTrace());
  for (const auto& d : count_trace) {
    EXPECT_NE(d.action, "leader") << "count-based made a leader move in a count-balanced scenario: "
                                  << d.ToString();
  }

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  auto heat_trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, [this]() {
    // Hot tserver "0000" has 2 leaders. The other tservers remain at heat 0 (bucket 0), so the
    // bucket gap is large regardless of the default 50 ops/sec hysteresis.
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = 1e6, .sum_write_ops_per_sec = 1e6,
                 .leader_tablet_count = 2});
  }));

  bool saw_heat_move_off_0000 = false;
  for (const auto& d : heat_trace) {
    if (d.action == "leader" && d.from_ts == "0000") {
      saw_heat_move_off_0000 = true;
      break;
    }
  }
  EXPECT_TRUE(saw_heat_move_off_0000)
      << "Heat-aware strategy should have moved a leader off the hot tserver 0000";
}

// Seeding heat that falls inside the same hysteresis bucket must not generate a heat-driven move;
// a seed that crosses into a higher bucket must. This exercises HeatBucket arithmetic rather than
// a naked pairwise delta.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareRespectsHysteresisBuckets) {
  PrepareTestStateMultiAz();

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));

  // Same bucket (bucket_size=50 by default): heats 10, 40, 40 all fall in bucket 0. No heat-driven
  // move should fire; count-based also declines (variance=1<2). Expect zero leader moves.
  auto same_bucket_trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, [this]() {
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = 40.0, .sum_write_ops_per_sec = 0.0});
    cb_.SetHeatForTest(
        "1111", {.sum_read_ops_per_sec = 10.0, .sum_write_ops_per_sec = 0.0});
    cb_.SetHeatForTest(
        "2222", {.sum_read_ops_per_sec = 10.0, .sum_write_ops_per_sec = 0.0});
  }));
  for (const auto& d : same_bucket_trace) {
    EXPECT_NE(d.action, "leader")
        << "Heat-aware strategy fired a move within the same bucket: " << d.ToString();
  }

  // Crossing into the next bucket (55 > 50) now triggers the heat override.
  auto crossing_bucket_trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, [this]() {
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = 55.0, .sum_write_ops_per_sec = 0.0});
    cb_.SetHeatForTest(
        "1111", {.sum_read_ops_per_sec = 10.0, .sum_write_ops_per_sec = 0.0});
    cb_.SetHeatForTest(
        "2222", {.sum_read_ops_per_sec = 10.0, .sum_write_ops_per_sec = 0.0});
  }));
  bool saw_heat_move = false;
  for (const auto& d : crossing_bucket_trace) {
    if (d.action == "leader" && d.from_ts == "0000") {
      saw_heat_move = true;
      break;
    }
  }
  EXPECT_TRUE(saw_heat_move) << "Heat-aware strategy should move a leader once heat crosses into "
                             << "a hotter bucket";
}

// A pairwise-threshold ordering ("close enough is equal, else ordered by heat") cycles when three
// heats straddle the hysteresis. Bucketing makes the order integer-valued and transitive; we
// guard the invariant by seeding such a configuration and running the trace twice, asserting
// identical output. An intransitive comparator would give std::sort undefined behavior and could
// (under the right insertion order) produce different traces on different runs.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareTraceIsDeterministicAcrossHysteresis) {
  PrepareTestStateMultiAz();

  // This test is about comparator transitivity, not cooldown semantics. Disable the cooldown so
  // that the persistent heat_aware_recent_leader_moves_ map written by run A cannot perturb run B;
  // otherwise a heat-driven move recorded in run A would block the same triple in run B and force
  // the balancer down a different branch, producing two non-identical traces for reasons that
  // have nothing to do with sort stability.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 0;

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  // 10, 40, 80 with default bucket_size=50: pairwise deltas 30, 40, 70 straddle the threshold.
  // Buckets are {0, 0, 1} — totally ordered.
  auto seed = [this]() {
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = 10.0, .sum_write_ops_per_sec = 0.0});
    cb_.SetHeatForTest(
        "1111", {.sum_read_ops_per_sec = 40.0, .sum_write_ops_per_sec = 0.0});
    cb_.SetHeatForTest(
        "2222", {.sum_read_ops_per_sec = 80.0, .sum_write_ops_per_sec = 0.0});
  };
  auto trace_a = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, seed));
  auto trace_b = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, seed));

  ASSERT_EQ(trace_a.size(), trace_b.size());
  for (size_t i = 0; i < trace_a.size(); ++i) {
    EXPECT_EQ(trace_a[i], trace_b[i]) << "Heat-aware trace diverged on repeat run: "
                                      << trace_a[i].ToString() << " vs " << trace_b[i].ToString();
  }
}

// Blacklist dominates heat. An extreme heat value on a non-blacklisted tserver must not outrank a
// blacklisted tserver as the leader-move source — operators expect blacklisted leaders to drain
// first.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareRespectsLeaderBlacklist) {
  PrepareTestStateMultiAz();

  AddLeaderBlacklist("0000");

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, [this]() {
    // Extreme heat on "2222" (cold in count terms, but not blacklisted). If heat could
    // outrank blacklist the first leader move would originate from 2222 — it must not.
    cb_.SetHeatForTest(
        "2222", {.sum_read_ops_per_sec = 1e6, .sum_write_ops_per_sec = 1e6});
  }));

  ASSERT_FALSE(trace.empty()) << "Expected at least one leader move to drain the blacklisted TS";
  // Only the leader moves fired while the blacklisted tserver still holds leaders are subject
  // to the blacklist-dominates-heat rule. Once 0000 is fully drained, subsequent heat-driven
  // moves off other hot tservers are legitimate.
  int blacklisted_leaders_remaining = 2;  // 0000 initially has 2 leaders in the multi-az setup
  for (const auto& d : trace) {
    if (d.action != "leader") continue;
    if (blacklisted_leaders_remaining <= 0) break;
    EXPECT_EQ(d.from_ts, "0000")
        << "Heat must not displace the blacklisted tserver as the preferred drain source while "
        << "it still has leaders: " << d.ToString();
    if (d.from_ts == "0000") {
      --blacklisted_leaders_remaining;
    }
  }
  EXPECT_EQ(blacklisted_leaders_remaining, 0)
      << "Blacklisted tserver should have been fully drained before heat drove other moves";
}

// A heat-driven move records (tablet_id, from_ts, to_ts) in the cooldown map. A subsequent run
// with the same scenario must not repeat the exact same triple while the cooldown is still hot.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareCooldownBlocksExactRepeatMove) {
  PrepareTestStateMultiAz();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 3600;

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  auto seed_hot_0000 = [this]() {
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = 1e6, .sum_write_ops_per_sec = 1e6});
  };

  ASSERT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), 0u);
  auto first_trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, seed_hot_0000));
  ASSERT_GT(cb_.GetHeatAwareCooldownSizeForTest(), 0u)
      << "At least one heat-driven leader move should have populated the cooldown map";

  std::vector<Decision> first_leader_moves;
  for (const auto& d : first_trace) {
    if (d.action == "leader") {
      first_leader_moves.push_back(d);
    }
  }
  ASSERT_FALSE(first_leader_moves.empty()) << "First trace should contain at least one leader move";

  // The mock SendMoveLeader is a no-op, so tablet_map_'s replica roles are not updated when the
  // balancer issues a leader move. Simulate the move having taken effect before the second trace:
  // without this, ResetLoadBalancerAndAnalyzeTablets re-reads the original leader from the replica
  // map and per_tablet_meta_.leader_uuid reverts to from_ts. IsInHeatAwareCooldown would then
  // (correctly) evict the cooldown entry — that is the stranded-leader fix exercised by
  // HeatCooldownEvictedWhenLeaderStillOnSource — and this test would misinterpret that eviction
  // as "cooldown failed to block". Production sees the replica map update after a successful
  // stepdown, and this test models the post-success world.
  for (const auto& first : first_leader_moves) {
    std::shared_ptr<TSDescriptor> to_ts_desc;
    for (const auto& desc : ts_descs_) {
      if (desc->permanent_uuid() == first.to_ts) {
        to_ts_desc = desc;
        break;
      }
    }
    ASSERT_NE(to_ts_desc, nullptr) << "to_ts " << first.to_ts << " not in ts_descs_";
    MoveTabletLeader(tablet_map_[first.tablet].get(), to_ts_desc);
  }

  auto second_trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, seed_hot_0000));
  for (const auto& first : first_leader_moves) {
    for (const auto& second : second_trace) {
      if (second.action != "leader") continue;
      EXPECT_FALSE(first == second)
          << "Cooldown failed to block exact-repeat heat-driven move: " << second.ToString();
    }
  }
}

// The cooldown key is the full (tablet_id, from_ts, to_ts) triple. Priming it for one destination
// must not suppress a different destination for the same tablet, nor a different tablet or source.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareCooldownDoesNotBlockDifferentDestination) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 3600;

  cb_.RecordHeatAwareLeaderMoveForTest("tablet_T", "ts_A", "ts_B");

  EXPECT_TRUE(cb_.IsInHeatAwareCooldownForTest("tablet_T", "ts_A", "ts_B"));
  // Different destination: not blocked.
  EXPECT_FALSE(cb_.IsInHeatAwareCooldownForTest("tablet_T", "ts_A", "ts_C"));
  // Different source: not blocked.
  EXPECT_FALSE(cb_.IsInHeatAwareCooldownForTest("tablet_T", "ts_X", "ts_B"));
  // Different tablet: not blocked.
  EXPECT_FALSE(cb_.IsInHeatAwareCooldownForTest("tablet_U", "ts_A", "ts_B"));
}

// Setting the cooldown window to 0 disables it. The predicate must report "not in cooldown" even
// immediately after recording a move — exercised here in lieu of a wall-clock wait.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareCooldownElapsesAllowsMove) {
  cb_.RecordHeatAwareLeaderMoveForTest("tablet_T", "ts_A", "ts_B");
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 3600;
  ASSERT_TRUE(cb_.IsInHeatAwareCooldownForTest("tablet_T", "ts_A", "ts_B"));

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 0;
  EXPECT_FALSE(cb_.IsInHeatAwareCooldownForTest("tablet_T", "ts_A", "ts_B"))
      << "With cooldown disabled, the predicate must not block repeats";
}

// Regression guard: when the operator sets `leader_balance_threshold` and heat-aware sorting
// puts a hot-but-count-light tserver at the right end of sorted_leader_load, the pre-loop
// threshold short-circuit must not misidentify that entry as the count-max and return nullopt.
// The strategy's ShouldSkipLeaderBalancing hook computes count-max explicitly, and heat-aware
// additionally requires all non-blacklisted tservers to share a bucket before it will skip.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareThresholdShortCircuitUsesCountMax) {
  PrepareTestStateMultiAz();
  // leader counts are {2, 1, 1} for {0000, 1111, 2222}. Threshold=2 caps count-based rebalancing
  // (count-max == threshold → skip). But heat spread across buckets should still allow heat-
  // driven moves despite the threshold.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_leader_balance_threshold) = 2;

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  auto heat_trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, [this]() {
    // Heat on "1111" (count-light: only 1 leader). Under heat-aware sort, 1111 lands at the
    // right end — the pre-Phase-3 inline check would have used its count (1) against the
    // threshold (2), concluded "no rebalance", and returned nullopt.
    cb_.SetHeatForTest(
        "1111", {.sum_read_ops_per_sec = 1e6, .sum_write_ops_per_sec = 0.0});
  }));

  bool saw_heat_move = false;
  for (const auto& d : heat_trace) {
    if (d.action == "leader" && d.from_ts == "1111") {
      saw_heat_move = true;
      break;
    }
  }
  EXPECT_TRUE(saw_heat_move)
      << "Heat-aware strategy should still move a leader off the hot tserver even when "
      << "leader_balance_threshold is set — the threshold caps count-based, not heat-driven, "
      << "moves";
}

// Same regression case but for count-based: count-max=2, threshold=2 → skip. Behavior is
// preserved because ShouldSkipLeaderBalancing computes count-max explicitly.
TEST_F(ClusterBalanceStrategyParityTest, CountBasedThresholdShortCircuitPreserved) {
  PrepareTestStateMultiAz();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_leader_balance_threshold) = 2;

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));
  auto trace = ASSERT_RESULT(CollectTrace());
  for (const auto& d : trace) {
    EXPECT_NE(d.action, "leader")
        << "Count-based must honor leader_balance_threshold: " << d.ToString();
  }
}

// Regression guard: a stale heat cooldown entry must not block a legitimate non-heat leader
// move that happens to share the same (tablet, from_ts, to_ts) triple. This can happen when
// leadership drifts back between runs via preferred-leader, placement repair, or manual
// stepdown, and then count-based / blacklist drain wants to make the same move again.
TEST_F(ClusterBalanceStrategyParityTest, HeatCooldownDoesNotBlockNonHeatMoves) {
  PrepareTestStateMultiAz();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 3600;
  AddLeaderBlacklist("0000");

  // First, collect the count-based trace with no cooldown entries. This gives us the set of
  // (tablet, 0000, destination) triples the blacklist-drain will issue.
  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));
  auto baseline_trace = ASSERT_RESULT(CollectTrace());
  std::vector<Decision> baseline_leader_moves;
  for (const auto& d : baseline_trace) {
    if (d.action == "leader") baseline_leader_moves.push_back(d);
  }
  ASSERT_FALSE(baseline_leader_moves.empty())
      << "Blacklist-drain should produce at least one count-based leader move";
  EXPECT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), 0u)
      << "Count-based moves must not populate the heat cooldown map";

  // Now prime the cooldown map with each triple the first trace produced. A stale heat
  // cooldown entry — from a prior heat-aware run that has since been superseded — must NOT
  // suppress the count-based re-issuance of the same move.
  for (const auto& d : baseline_leader_moves) {
    cb_.RecordHeatAwareLeaderMoveForTest(d.tablet, d.from_ts, d.to_ts);
  }
  ASSERT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), baseline_leader_moves.size());

  auto second_trace = ASSERT_RESULT(CollectTrace());
  ASSERT_EQ(baseline_trace.size(), second_trace.size())
      << "Cooldown blocked count-based moves it should have let through";
  for (size_t i = 0; i < baseline_trace.size(); ++i) {
    EXPECT_EQ(baseline_trace[i], second_trace[i])
        << "Count-based trace diverged after priming heat cooldown entries: index " << i;
  }
}

// The cooldown map is scoped to heat-driven moves only. Count-based leader moves (and any other
// MoveLeader callers that do not set is_heat_driven) must leave the map untouched — otherwise a
// placement-repair stepdown could suppress a future heat-aware move for the same tablet.
TEST_F(ClusterBalanceStrategyParityTest, NonHeatLeaderMovesDoNotPopulateCooldown) {
  PrepareTestStateMultiAz();
  // Leader-blacklist "0000" so the count-based strategy has a concrete non-heat reason to move
  // its two leaders off.
  AddLeaderBlacklist("0000");

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));
  auto trace = ASSERT_RESULT(CollectTrace());

  bool saw_leader_move = false;
  for (const auto& d : trace) {
    if (d.action == "leader") saw_leader_move = true;
  }
  ASSERT_TRUE(saw_leader_move)
      << "Test precondition: the blacklist should force at least one count-based leader move";
  EXPECT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), 0u)
      << "Count-based leader moves must not populate the heat-aware cooldown map";
}

// Regression guard for heat-aware cooldown integrity under unrelated stepdown failures.
// leader_stepdown_failures on the TabletInfo is keyed only by destination tserver, so a failure
// like C -> B is indistinguishable from an A -> B failure when consulted. Earlier revisions of
// this code evicted the (tablet, from, to) cooldown entry on any matching destination failure,
// which let an unrelated C -> B failure drop a valid (T, A, B) cooldown. Once leadership drifted
// back to A (preferred-leader affinity, manual stepdown, placement repair, etc.), the balancer
// would reissue A -> B inside the original cooldown window — producing exactly the churn the
// cooldown was designed to prevent.
//
// The destination-failure side-channel eviction is therefore gone. A stepdown failure recorded
// against the same destination — no matter how recent — must not perturb a live (from, to)
// cooldown entry when the current leader is somewhere other than from_ts (i.e. our own move did
// take effect and leadership has since moved on). This test models that real-world scenario by
// moving the leader to 2222 before registering the failure, so the check exercises the pure
// destination-only-failure path without aliasing the separate stranded-leader eviction path
// that HeatCooldownEvictedWhenLeaderStillOnSource covers.
TEST_F(ClusterBalanceStrategyParityTest, HeatCooldownNotEvictedByUnrelatedStepdownFailure) {
  PrepareTestStateMultiAz();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 3600;

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));

  const auto& tablet_id = tablets_[0]->tablet_id();
  // Model the "our A -> B already took effect and leadership has since moved on" scenario. The
  // failure we register below must not be attributable to our cooldown's (0000 -> 1111) move,
  // so place the leader on 2222 before recording the failure.
  MoveTabletLeader(tablet_map_[tablet_id].get(), ts_descs_[2]);

  cb_.RecordHeatAwareLeaderMoveForTest(tablet_id, "0000", "1111");
  ASSERT_TRUE(cb_.IsInHeatAwareCooldownForTest(tablet_id, "0000", "1111"))
      << "Precondition: cooldown should be live immediately after recording";

  // Register a fresh stepdown failure against the same destination. In production this can come
  // from any FROM tserver (e.g. a later 2222 -> 1111 attempt fails while leadership was at 2222);
  // in the test there is no easy way to stamp a from_ts onto the failure entry because
  // leader_stepdown_failures does not carry one — which is exactly why the destination-only key
  // cannot disambiguate our (0000, 1111) move from any other attempt.
  tablet_map_[tablet_id]->RegisterLeaderStepDownFailure("1111", MonoDelta::FromMilliseconds(1));
  ASSERT_OK(ResetLoadBalancerAndAnalyzeTablets());

  EXPECT_TRUE(cb_.IsInHeatAwareCooldownForTest(tablet_id, "0000", "1111"))
      << "The (0000, 1111) cooldown must survive a stepdown failure recorded against 1111 — the "
      << "failure's originating from_ts is unknown here, and dropping the cooldown on that "
      << "signal would let a subsequent drift-back-to-0000 trigger a churn-inducing repeat move.";
  EXPECT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), 1u)
      << "No side-channel eviction should have removed the live cooldown entry.";
}

// Regression guard for a user-visible stall that the opposite side of the cooldown design can
// produce. SendMoveLeader records a heat cooldown for (tablet, from_ts, to_ts) synchronously when
// the balancer issues the async stepdown RPC. If that stepdown fails or times out, the leader
// never actually leaves from_ts — but the cooldown entry, indexed only by wall-clock age, will
// keep suppressing the same heat-aware move for the full cooldown window (default 300s). The
// short-term debounce at cluster_balance.cc's GetLeaderToMove only covers ~20s (driven by
// min_leader_stepdown_retry_interval_ms), leaving a multi-minute gap during which a hot leader
// is stranded on from_ts with no retry.
//
// IsInHeatAwareCooldown therefore evicts a recorded (tablet, from, to) entry as soon as it
// observes that the current leader is still on from_ts: that can only mean the scheduled move
// never took effect (case 3: async stepdown failed / case 4: still in flight), in which case
// blocking further attempts is exactly the opposite of what the cooldown is for. The companion
// test above defends the other side of the trade-off: if the leader has already left from_ts,
// a stale stepdown failure to the same destination must NOT evict the cooldown.
TEST_F(ClusterBalanceStrategyParityTest, HeatCooldownEvictedWhenLeaderStillOnSource) {
  PrepareTestStateMultiAz();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 3600;

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));

  // PrepareTestStateMultiAz places tablets_[0]'s leader on 0000 (i % N == j where i=0, j=0). That
  // is exactly the state we need: a recorded 0000 -> 1111 cooldown while leadership is still on
  // 0000, modeling "the scheduled stepdown never took effect".
  const auto& tablet_id = tablets_[0]->tablet_id();
  cb_.RecordHeatAwareLeaderMoveForTest(tablet_id, "0000", "1111");
  ASSERT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), 1u)
      << "Precondition: cooldown was recorded.";

  ASSERT_OK(ResetLoadBalancerAndAnalyzeTablets());

  EXPECT_FALSE(cb_.IsInHeatAwareCooldownForTest(tablet_id, "0000", "1111"))
      << "The cooldown must be evicted when per_tablet_meta_.leader_uuid is still from_ts — the "
      << "recorded move never took effect, so blocking retry for the full cooldown window would "
      << "strand a hot leader on from_ts for minutes.";
  EXPECT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), 0u)
      << "The stale cooldown entry must be evicted from the map, not just treated as false once.";
}

// Regression guard for P2b: the heat-aware threshold short-circuit must not be defeated by a
// non-blacklisted tserver that is hot cluster-wide but owns zero leaders for the current table.
// Such a tserver cannot be a heat-driven source for this table, so honoring it as a bucket-gap
// signal allows count-based rebalancing to run in the shadow of a threshold that was supposed to
// suppress it.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareThresholdSurvivesZeroLeaderHotTs) {
  PrepareTestStateMultiAz();
  // 4th tserver owns no replicas of this table.
  ts_descs_.push_back(SetupTS("3333", "a"));

  // Re-arrange base leader distribution so count variance reaches kMinLoadVarianceToBalance.
  // PrepareTestStateMultiAz gives {0000=2, 1111=1, 2222=1}; flip tablets_[1]'s leader from 1111
  // to 0000 to produce {0000=3, 1111=0, 2222=1, 3333=0}. Count-max is 3.
  MoveTabletLeader(tablet_map_[tablets_[1]->tablet_id()].get(), ts_descs_[0]);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_leader_balance_threshold) = 3;

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, [this]() {
    // 3333 is hot from other tables' traffic but owns no leaders here. Pre-fix,
    // ShouldSkipLeaderBalancing would report min_bucket=0, max_bucket=1, and refuse to skip — at
    // which point the loop would see a count variance of 3 (0000=3 vs 1111=0) and let a count-
    // based move through despite the threshold cap.
    cb_.SetHeatForTest(
        "3333", {.sum_read_ops_per_sec = 1e6, .sum_write_ops_per_sec = 0.0});
  }));

  for (const auto& d : trace) {
    EXPECT_NE(d.action, "leader")
        << "leader_balance_threshold must still hold — a 0-leader hot tserver cannot be a "
        << "heat-driven source for this table and must not defeat the short-circuit: "
        << d.ToString();
  }
}

// Regression guard for P2d: when count-based would have skipped at the configured
// `leader_balance_threshold` and heat-aware only entered the main loop because of a heat-bucket
// gap, plain count-driven per-pair moves must stay suppressed. Only heat-driven moves (and
// blacklist drains) are legitimate — otherwise an unrelated (low-count, high-count) pair can
// trigger count-based rebalancing under the guise of a heat-authorized run, defeating threshold
// semantics.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareThresholdSuppressesPlainCountMoves) {
  PrepareTestStateMultiAz();

  // Engineer leader counts {0000=2, 1111=0, 2222=2} by flipping the lone 1111 leader onto 2222.
  // Chosen so the scenario exercises P2d suppression WITHOUT tripping the P2 count-regression
  // guard in AssessLeaderMove: the natural heat destination (1111) is count-lighter than both
  // potential sources, and the adversarial cooldown fallback (0000→2222) is count-sensible
  // because both sit at count=2. An earlier variant used {1, 0, 3} with the fallback firing
  // 0000→2222 onto a count-3 destination — count-regressive, and correctly rejected by the P2
  // gate, but a false positive for this test's intent.
  //
  // With this layout, count-driven moves would originate from 2222 (variance 2-0 ≥ min_variance)
  // and heat-driven moves from 0000 (seeded below). The trace's from_ts column is the regression
  // signal: "2222" means the P2d suppression failed and a capped count move leaked through under
  // the guise of the heat-aware run.
  MoveTabletLeader(tablet_map_[tablets_[2]->tablet_id()].get(), ts_descs_[2]);

  // count-max is 2; threshold=2 caps count-based rebalancing. Were the active strategy
  // count-based, the pre-loop ShouldSkipLeaderBalancing would fire and no moves would happen.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_leader_balance_threshold) = 2;
  // Keep the cooldown entries we record below alive for the full trace.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 3600;

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));

  // 0000 owns tablets_[0] and tablets_[1] as leaders. Pre-populate the cooldown for BOTH against
  // destination 1111 so the heat override's natural target is entirely blocked. The inner loop
  // then advances to (low=1111, high=2222), where count-based wants to fire a plain count-driven
  // move despite the threshold cap. P2d suppression is supposed to zero that out; without it, a
  // count move from 2222 leaks through as the first decision in the trace. With both P2d
  // suppression AND the P2 count-regression gate in place, the loop eventually lands on the
  // (low=2222, high=0000) heat pair — whose cooldown key is different and whose count direction
  // is count-sensible (both tservers at count=2) — producing the legitimate 0000 → 2222 move.
  cb_.RecordHeatAwareLeaderMoveForTest(tablets_[0]->tablet_id(), "0000", "1111");
  cb_.RecordHeatAwareLeaderMoveForTest(tablets_[1]->tablet_id(), "0000", "1111");

  // Cap the trace at 1 decision so we observe only the first move. In iteration 2 the heat move
  // has made 2222=3, crossing the threshold and re-legitimizing count-based balancing — so a
  // count move from 2222 in iteration 2 is correct behavior, not a regression. The diagnostic
  // signal we care about is strictly the first-move origin.
  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/1, [this]() {
    // Heat only on 0000. Under default hysteresis, 0000 lands in a high bucket and the other
    // two stay in bucket 0, so heat-aware's ShouldSkipLeaderBalancing declines to skip.
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = 1e6, .sum_write_ops_per_sec = 0.0});
  }));

  // Without the P2d suppression, the (low=1111, high=2222) pair (load_variance=2) returns
  // should_move=true with reason="more leaders" and the balancer issues a count-driven move
  // from 2222 to 1111 before ever visiting the (low=2222, high=0000) heat pair. A first-move
  // from_ts of "2222" is the observable regression. With suppression, the first move is the
  // 0000 → 2222 heat override and from_ts is "0000".
  bool saw_any_leader_move = false;
  for (const auto& d : trace) {
    if (d.action != "leader") continue;
    saw_any_leader_move = true;
    EXPECT_EQ(d.from_ts, "0000")
        << "Under heat-aware threshold capping, only heat-driven (or blacklist-drain) moves are "
        << "legal. A plain count-based move leaked through: " << d.ToString();
  }
  EXPECT_TRUE(saw_any_leader_move)
      << "Precondition: at least one leader move should have fired, otherwise this test is not "
      << "exercising the suppression path at all";
}

// Regression guard for the blacklist-drain interaction with leader_balance_threshold under
// heat-aware. count_based_threshold_capping must only gate suppression when heat override is
// what admitted the main loop past the count-based skip; if the loop was entered via
// `blacklisted_drain_pending`, count-based would also have been in the loop and would have
// issued ordinary count-driven moves from non-blacklisted sources. Earlier revisions computed
// capping unconditionally, which made heat-aware swallow legitimate non-blacklist count moves
// whenever a leader-blacklisted source co-existed with a count-imbalanced, non-blacklisted one.
TEST_F(ClusterBalanceStrategyParityTest,
       HeatAwareBlacklistDrainDoesNotSuppressNonBlacklistCountMoves) {
  PrepareTestStateMultiAz();
  // PrepareTestStateMultiAz gives {0000=2, 1111=1, 2222=1}; flip tablets_[1] onto 0000 to land
  // at {0000=3, 1111=0, 2222=1}. 0000 is the count-heavy non-blacklisted source; 1111 is the
  // cold non-blacklisted destination; 2222 is about to be leader-blacklisted.
  MoveTabletLeader(tablet_map_[tablets_[1]->tablet_id()].get(), ts_descs_[0]);
  AddLeaderBlacklist("2222");
  // Non-blacklisted count-max = 3, threshold = 3 ⇒ count-based ShouldSkipLeaderBalancing would
  // return true on its own. Only blacklist-drain keeps the main loop alive.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_leader_balance_threshold) = 3;
  // Block the (1111, 2222) drain for tablet_2 via stepdown_failures[1111]. This forces the
  // inner loop past the blacklist pair and onto (1111, 0000) within the same GetLeaderToMove
  // call — precisely the "blacklist source has no movable leader for the current destination"
  // condition called out in the review. Under the pre-fix code, heat-aware's suppression then
  // zeroes out the count verdict for this non-blacklist pair and no move fires at all; the
  // outer loop eventually reaches (0000, 2222) and resolves the drain via 0000, swallowing the
  // legitimate 0000 → 1111 count move entirely.
  tablet_map_[tablets_[2]->tablet_id()]->RegisterLeaderStepDownFailure(
      "1111", MonoDelta::FromMilliseconds(1));

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  // No heat seeded — the defect is visible on a fully cold cluster because the suppression
  // decision only depends on blacklist drain + threshold capping, not on heat state.
  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/1));

  ASSERT_FALSE(trace.empty()) << "Expected at least one rebalancing decision";
  bool saw_count_move_off_0000 = false;
  for (const auto& d : trace) {
    if (d.action == "leader" && d.from_ts == "0000" && d.to_ts == "1111") {
      saw_count_move_off_0000 = true;
    }
  }
  EXPECT_TRUE(saw_count_move_off_0000)
      << "Heat-aware must let ordinary count-based moves from non-blacklisted sources fire when "
      << "the main loop was entered via blacklist drain. The observable regression is a first "
      << "leader move whose from_ts is 2222 (blacklist drain to 0000) instead of 0000 (count "
      << "move to 1111). First decision in trace was: " << trace.front().ToString();
}

// Regression guard for the P1 fix: HeatAwareLoadBalancerStrategy::AssessLeaderMove must not
// propagate count-based's should_break_inner_loop / should_return_nullopt in its fall-through
// path, and GetLeaderToMove's outer loop must terminate safely when no pair triggers an early
// exit. Earlier revisions returned the count_based verdict as-is whenever heat override could
// not fire (same-bucket, hysteresis disabled, left_equals_right), which made GetLeaderToMove
// rely on count-sort invariants ("right=last_pos is count-max", "within-bucket count-ascending")
// that no longer hold under heat-aware sorting. The corresponding FATAL_ERROR fallback in
// GetLeaderToMove was only reachable once those early-exit flags stopped firing, so stripping
// the flags without also replacing the FATAL_ERROR would crash the master.
//
// The scenario below runs heat-aware on a cluster with counts {2, 1, 1} and no heat seeded —
// every tserver lands in bucket 0 so the fall-through path fires on every (left, right) pair.
// Count-based alone declines at each pair (load_variance in {0, 1} and global variance under
// kMinLoadVarianceToBalance). With the P1 fix the heat-aware scan now walks through every pair
// without terminating early, ultimately exhausting the outer loop; the FATAL_ERROR replacement
// catches this and returns nullopt. Reverting the FATAL_ERROR replacement crashes this test.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareExhaustionReturnsNulloptNotFatalError) {
  PrepareTestStateMultiAz();
  // Default counts are {0000=2, 1111=1, 2222=1} — all variances ≤ 1, below kMinLoadVarianceToBalance.
  // Under default heat (all 0) every tserver shares bucket 0, forcing HeatAware::AssessLeaderMove
  // through its fall-through path on every pair.

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  ASSERT_OK(ResetLoadBalancerAndAnalyzeTablets());

  TabletId tablet_id;
  TabletServerId from_ts;
  TabletServerId to_ts;
  // The pair (load_variance=1) at (left='1111', right='0000') sets count_based's global-balancing
  // check. If global-balancing can fire for this table-less mock (it generally cannot without
  // a cross-table imbalance signal), the call returns a global move. Otherwise count-based
  // declines everywhere and the outer loop exhausts — the path the FATAL_ERROR replacement
  // guards. The only assertion we need is "no crash, Status is OK": the exact true/false verdict
  // depends on global-balancing state which this test does not control.
  auto result = HandleLeaderMoves(&tablet_id, &from_ts, &to_ts);
  // Heat-aware exhaustion must terminate cleanly, not via FATAL_ERROR. A crash here means the
  // FATAL_ERROR replacement is missing.
  ASSERT_OK(result);
  // No behavioral assertion on *result — both true (global move produced) and false (exhaustion
  // → nullopt) are valid post-conditions under this setup. The regression signal is the crash
  // itself.
}

// Regression guard for the P2 fix: under heat-aware sort, sorted_leader_load_[left] is the
// coolest-bucket tserver, not necessarily the count-lightest. ClusterLoadBalancer::GetLeaderToMove
// still uses the left side as the leader-move destination, so without a count-sensibility gate the
// heat override would happily emit a move from a hot, count-light source onto a cool, count-heavy
// destination — the exact configuration this test constructs. AssessLeaderMove must reject any
// heat-driven (high, low) pair where the destination's leader count for this table strictly
// exceeds the source's, so the outer loop can advance to a pair that does not regress counts.
//
// Scenario (matches the reviewer comment verbatim): buckets/counts {A:0/10, B:1/0, C:2/1}. Heat
// sort produces [A, B, C] ascending by bucket. At (left=A, right=C) the bucket gap is large but
// A already owns the most leaders. Without the fix the balancer fires C → A, pushing A from 10 to
// 11 leaders; with the fix the pair is rejected, the loop advances to (left=B, right=C), and
// C → B fires instead.
//
// PrepareTestStateMultiAz gives a 4-tablet cluster with default distribution {0000=2, 1111=1,
// 2222=1}. To match the reviewer's exact count shape we bump 0000 into the count-heaviest slot
// via preferred-leader affinity isn't available in the mock; instead we rely on the default 2/1/1
// distribution — the essential property (destination count strictly greater than source count) is
// already satisfied at (left=0000, right=2222). Seeding buckets 0/1/2 on 0000/1111/2222 produces
// the same (bucket-ASC sort, count-heavy at left, count-light at right) shape the reviewer flagged.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareRejectsCountRegressiveHeatMove) {
  PrepareTestStateMultiAz();  // default leader counts: 0000=2, 1111=1, 2222=1.

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, [this]() {
    // Place each tserver in a distinct hysteresis bucket with default bucket_size=50 ops/sec.
    // 0000 stays in bucket 0 (coolest) but owns the most leaders (2); 2222 lands in bucket 2
    // (hottest) with only 1 leader. Under heat-aware sort sorted_leader_load_ becomes
    // [0000, 1111, 2222]. Without the P2 gate the first (left=0000, right=2222) pair would
    // emit a heat-driven move 2222 → 0000, which is count-regressive.
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = 0.0, .sum_write_ops_per_sec = 0.0});
    cb_.SetHeatForTest(
        "1111", {.sum_read_ops_per_sec = 60.0, .sum_write_ops_per_sec = 0.0});
    cb_.SetHeatForTest(
        "2222", {.sum_read_ops_per_sec = 120.0, .sum_write_ops_per_sec = 0.0});
  }));

  const Decision* first_leader_move = nullptr;
  for (const auto& d : trace) {
    if (d.action == "leader") {
      first_leader_move = &d;
      break;
    }
  }
  ASSERT_NE(first_leader_move, nullptr)
      << "Heat-aware strategy should have moved a leader off the hot tserver 2222";

  // The P2 invariant: heat-driven moves must not regress leader-count balance. 0000 starts with
  // 2 leaders while every candidate source has 1; any move into 0000 pushes it to 3 leaders and
  // strictly worsens count variance. Reverting the `low_load > high_load` gate in
  // HeatAwareLoadBalancerStrategy::AssessLeaderMove reproduces this regression and fails the
  // assertion below.
  EXPECT_NE(first_leader_move->to_ts, "0000")
      << "Heat-aware strategy issued a count-regressive move toward the count-heaviest tserver: "
      << first_leader_move->ToString();
}

// After a successful leader move, ClusterLoadBalancer::MoveLeader must project the moved tablet's
// heat contribution out of heat_by_ts_[from_ts] and into heat_by_ts_[to_ts], and rewrite the
// per-tablet record's leader_uuid to the new owner. Without that projection, later iterations of
// the leader-balance loop read a start-of-run snapshot that still credits the source with the
// moved tablet's heat, causing redundant drains from a source that is already cold. This test
// validates the aggregate and per-tablet record state after a single heat-driven move.
TEST_F(ClusterBalanceStrategyParityTest, HeatProjectionUpdatesAggregatesOnSuccessfulMove) {
  PrepareTestStateMultiAz();

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));

  // tablets_[0] and tablets_[1] are the two leaders on 0000 in the default distribution. Give
  // only tablets_[0] heat so the TS aggregate is unambiguously driven by that single record —
  // makes the post-move arithmetic trivially checkable.
  constexpr double kHotRead = 800.0;
  constexpr double kHotWrite = 400.0;
  const auto hot_tablet_id = tablets_[0]->tablet_id();

  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/1, [&]() {
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = kHotRead, .sum_write_ops_per_sec = kHotWrite,
                 .leader_tablet_count = 1});
    cb_.SetTabletHeatForTest(
        hot_tablet_id,
        LeaderHeatRecord{.leader_uuid = "0000", .read_ops_per_sec = kHotRead,
                         .write_ops_per_sec = kHotWrite, .last_updated = MonoTime::Now()});
  }));

  ASSERT_EQ(trace.size(), 1u) << "Expected exactly one heat-driven leader move";
  const auto& move = trace.front();
  ASSERT_EQ(move.action, "leader");
  ASSERT_EQ(move.from_ts, "0000");
  ASSERT_EQ(move.tablet, hot_tablet_id)
      << "Heat-aware strategy should move the hot tablet, not the cold one on the same tserver";

  const auto post_source = cb_.GetHeatForTest("0000");
  EXPECT_DOUBLE_EQ(post_source.sum_read_ops_per_sec, 0.0)
      << "Source aggregate still credits the moved tablet's read ops.";
  EXPECT_DOUBLE_EQ(post_source.sum_write_ops_per_sec, 0.0)
      << "Source aggregate still credits the moved tablet's write ops.";
  EXPECT_EQ(post_source.leader_tablet_count, 0)
      << "Source aggregate still counts the moved tablet as a leader.";

  const auto post_dest = cb_.GetHeatForTest(move.to_ts);
  EXPECT_DOUBLE_EQ(post_dest.sum_read_ops_per_sec, kHotRead)
      << "Destination aggregate missing the moved tablet's read ops.";
  EXPECT_DOUBLE_EQ(post_dest.sum_write_ops_per_sec, kHotWrite)
      << "Destination aggregate missing the moved tablet's write ops.";
  EXPECT_EQ(post_dest.leader_tablet_count, 1)
      << "Destination aggregate missing the moved tablet's leader count.";

  const auto record = cb_.GetTabletHeatForTest(hot_tablet_id);
  ASSERT_TRUE(record.has_value())
      << "Per-tablet heat record was removed instead of re-attributed.";
  EXPECT_EQ(record->leader_uuid, move.to_ts)
      << "Per-tablet record's leader_uuid still points at the old leader after the move.";
}

// Reviewer P2: the heat-aware strategy previously read heat_by_ts_ as a start-of-run snapshot and
// did not refresh it between leader-move iterations. On clusters where one hot tablet dominated a
// tserver's aggregate, moving it off would leave the tserver looking hot for the rest of the run,
// which could drive additional drains of genuinely cold leaders from the same tserver. This test
// seeds exactly that configuration and asserts the exact property the projection preserves: the
// cold peer on 0000 is never drained, because after the hot tablet moves 0000's aggregate drops
// to 0 and no further heat-driven drain of 0000 is warranted.
//
// The test is deliberately agnostic about whether the hot tablet itself gets moved more than
// once (e.g., ping-pong back onto 0000 once its own heat has been projected onto the former
// destination, which now looks hot). That is a separate balancing-stability concern — addressed
// in production by heartbeat-driven refresh between runs and by the per-(tablet, from, to)
// cooldown within a run. What the projection MUST ensure, and what this test pins down, is that
// the start-of-run aggregate does not strand the cold peer in a drain decision it should never
// have qualified for.
TEST_F(ClusterBalanceStrategyParityTest, HeatProjectionPreventsStaleRedrainOfColdPeers) {
  PrepareTestStateMultiAz();
  // Keep the cooldown tight on the hot tablet so the regression is observable in the trace
  // without interference from debounce; this test is about projection arithmetic, not cooldown.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 3600;

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));

  // Default hysteresis is 50 ops/sec. 600 on 0000 puts it in bucket 12; the other tservers stay
  // at 0 (bucket 0) — bucket gap 12 easily admits a heat move. After the projection, the source
  // aggregate drops to 0 and the next heat-aware iteration has no reason to drain the remaining
  // cold leader on 0000.
  constexpr double kHotRead = 600.0;
  const auto hot_tablet_id = tablets_[0]->tablet_id();
  const auto cold_tablet_id = tablets_[1]->tablet_id();

  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/12, [&]() {
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = kHotRead, .sum_write_ops_per_sec = 0.0,
                 .leader_tablet_count = 2});
    // Only one of 0000's two leaders carries any heat. Without the projection fix the aggregate
    // stays at 600 for the whole run and the second iteration would fire a redundant drain of
    // cold_tablet_id because 0000 still looks hot.
    cb_.SetTabletHeatForTest(
        hot_tablet_id,
        LeaderHeatRecord{.leader_uuid = "0000", .read_ops_per_sec = kHotRead,
                         .write_ops_per_sec = 0.0, .last_updated = MonoTime::Now()});
    cb_.SetTabletHeatForTest(
        cold_tablet_id,
        LeaderHeatRecord{.leader_uuid = "0000", .read_ops_per_sec = 0.0,
                         .write_ops_per_sec = 0.0, .last_updated = MonoTime::Now()});
  }));

  int cold_tablet_drains_off_0000 = 0;
  bool saw_hot_tablet_move_off_0000 = false;
  for (const auto& d : trace) {
    if (d.action != "leader" || d.from_ts != "0000") continue;
    if (d.tablet == hot_tablet_id) {
      saw_hot_tablet_move_off_0000 = true;
    } else if (d.tablet == cold_tablet_id) {
      cold_tablet_drains_off_0000++;
    }
  }
  EXPECT_TRUE(saw_hot_tablet_move_off_0000)
      << "Precondition: the hot tablet should have been moved off 0000 at least once.";
  EXPECT_EQ(cold_tablet_drains_off_0000, 0)
      << "Cold peer on 0000 was drained after its hot peer moved. Without incremental heat "
      << "projection, heat_by_ts_[0000] stays at its start-of-run value and the next iteration "
      << "mistakenly sees 0000 as still hot — exactly the reviewer P2 regression this projection "
      << "guards against.";
}

// The heat projection must run on every successful MoveLeader regardless of the strategy that
// produced it — otherwise a count-based or blacklist-drain move that fires first in a
// heat-telemetry-enabled run would leave heat_by_ts_ stale for any heat-aware iterations that
// follow in the same run. Count-based never consults heat itself, but it shares the same
// GlobalLoadState, and a production run can oscillate strategies across tables. Validates the
// "always project" contract: a count-based blacklist-drain move seeded with heat_by_tablet_
// still updates heat_by_ts_.
TEST_F(ClusterBalanceStrategyParityTest, NonHeatMoveAlsoProjectsHeat) {
  PrepareTestStateMultiAz();
  AddLeaderBlacklist("0000");

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));

  constexpr double kHotRead = 500.0;
  const auto hot_tablet_id = tablets_[0]->tablet_id();

  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/1, [&]() {
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = kHotRead, .sum_write_ops_per_sec = 0.0,
                 .leader_tablet_count = 1});
    cb_.SetTabletHeatForTest(
        hot_tablet_id,
        LeaderHeatRecord{.leader_uuid = "0000", .read_ops_per_sec = kHotRead,
                         .write_ops_per_sec = 0.0, .last_updated = MonoTime::Now()});
  }));

  ASSERT_FALSE(trace.empty());
  const auto& move = trace.front();
  ASSERT_EQ(move.action, "leader");
  ASSERT_EQ(move.from_ts, "0000")
      << "Count-based blacklist drain should fire first; precondition for the heat-projection "
      << "check below.";

  if (move.tablet == hot_tablet_id) {
    const auto post_source = cb_.GetHeatForTest("0000");
    EXPECT_DOUBLE_EQ(post_source.sum_read_ops_per_sec, 0.0)
        << "Count-based MoveLeader did not update heat_by_ts_[from_ts] — future heat-aware "
        << "iterations in the same run would see a stale snapshot.";
    const auto post_dest = cb_.GetHeatForTest(move.to_ts);
    EXPECT_DOUBLE_EQ(post_dest.sum_read_ops_per_sec, kHotRead)
        << "Count-based MoveLeader did not update heat_by_ts_[to_ts] — future heat-aware "
        << "iterations would miss the heat that relocated.";
    const auto record = cb_.GetTabletHeatForTest(hot_tablet_id);
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->leader_uuid, move.to_ts);
  }
  // If the drain happened to pick the cold tablet instead of the hot one, the projection is still
  // active for that other tablet — but our heat_by_tablet_ entry for the cold tablet was not
  // seeded, so ProjectLeaderHeatMoveIntoGlobalState is a no-op and heat_by_ts_[0000] stays at
  // kHotRead. That exercises the "no fresh record" branch and is covered by a separate test.
}

// Covers the two no-op branches of ProjectLeaderHeatMoveIntoGlobalState: a successful move for a
// tablet that has no entry in heat_by_tablet_ must leave heat_by_ts_ untouched. This is the
// common case when heat telemetry is enabled but a particular tablet has not reported fresh heat
// within the staleness window — there is nothing to subtract from the source aggregate because
// nothing was credited at run start.
TEST_F(ClusterBalanceStrategyParityTest, HeatProjectionIsNoOpWhenTabletHasNoFreshHeat) {
  PrepareTestStateMultiAz();
  AddLeaderBlacklist("0000");

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyCountBased));

  // Seed heat_by_ts_[0000] but not heat_by_tablet_ — no individual tablet on 0000 reported fresh
  // telemetry, so aggregate came from somewhere else entirely (e.g., a stale cache entry that
  // AggregateLeaderHeatIntoGlobalState happened to pick up for a tablet that was subsequently
  // dropped from the fresh snapshot). The blacklist-drain move should not touch heat_by_ts_.
  constexpr double kPreExistingRead = 123.0;
  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/1, [&]() {
    cb_.SetHeatForTest(
        "0000", {.sum_read_ops_per_sec = kPreExistingRead, .sum_write_ops_per_sec = 0.0,
                 .leader_tablet_count = 1});
  }));

  ASSERT_FALSE(trace.empty());
  ASSERT_EQ(trace.front().from_ts, "0000");

  const auto post_source = cb_.GetHeatForTest("0000");
  EXPECT_DOUBLE_EQ(post_source.sum_read_ops_per_sec, kPreExistingRead)
      << "Projection touched heat_by_ts_ for a tablet with no heat_by_tablet_ record — should "
      << "have been a no-op.";
  EXPECT_EQ(post_source.leader_tablet_count, 1)
      << "Projection decremented leader_tablet_count on a tablet with no heat_by_tablet_ record.";
}

// Helper that constructs an AffinitizedZonesSet for the given zones in the default cloud/region.
namespace {
AffinitizedZonesSet MakeAffinitizedZoneSet(std::initializer_list<std::string> zones) {
  AffinitizedZonesSet out;
  for (const auto& zone : zones) {
    CloudInfoPB ci;
    ci.set_placement_cloud(default_cloud);
    ci.set_placement_region(default_region);
    ci.set_placement_zone(zone);
    out.insert(ci);
  }
  return out;
}
}  // namespace

// P1 regression: under heat_aware_experimental, sorted_leader_load_ is ordered by heat bucket
// first, not by leader count. GetLeaderToMoveAcrossAffinitizedPriorities scans sources right-to-
// left, breaking on the first non-blacklisted tserver whose per-table leader count is 0 — a
// correct stop condition under count-based ordering (all zero-count entries are at the left), but
// wrong under heat-aware ordering, where a hot-from-other-tables zero-count tserver can land at
// the right edge, making the loop break before it ever inspects a cooler source that still owns
// leaders. That skips the preferred-zone drain entirely for this priority.
TEST_F(ClusterBalanceStrategyParityTest, HeatAwareAffinitizedDrainSeesSourceBehindHotZeroCountTs) {
  PrepareTestStateMultiAz();
  // Priority 0: zone "a" (0000). Priority 1: zones "b" and "c" (1111, 2222).
  affinitized_zones_.clear();
  affinitized_zones_.push_back(MakeAffinitizedZoneSet({"a"}));
  affinitized_zones_.push_back(MakeAffinitizedZoneSet({"b", "c"}));

  // Consolidate every leader onto 1111 so 1111 has count 4, 0000 has count 0, 2222 has count 0.
  for (const auto& tablet : tablets_) {
    MoveTabletLeader(tablet.get(), ts_descs_[1]);
  }

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  // Seed heat so 2222 lands in a high bucket even though its per-table leader count is 0. Under
  // heat-aware ordering this places 2222 to the right of 1111 in sorted_leader_load_[priority 1].
  // Without the fix, the right-to-left source scan hits 2222 first, sees count=0, not blacklisted,
  // and breaks — skipping the legitimate drain from 1111.
  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/1, [&]() {
    cb_.SetHeatForTest(
        "2222", {.sum_read_ops_per_sec = 500.0, .sum_write_ops_per_sec = 0.0,
                 .leader_tablet_count = 0});
  }));

  ASSERT_FALSE(trace.empty())
      << "Affinitized priority drain produced no move — the source scan likely broke early on "
      << "hot zero-count 2222 and missed 1111 (which holds all four leaders).";
  EXPECT_EQ(trace.front().from_ts, "1111");
  EXPECT_EQ(trace.front().to_ts, "0000")
      << "Expected leader to drain into priority-0 zone (0000). Got to_ts=" << trace.front().to_ts
      << " — affinitized-priority balancer returned nullopt, so the move fell through to within-"
      << "priority balancing which cannot cross into zone a.";
}

// P2 regression: under heat_aware_experimental, GetLeaderToMoveAcrossAffinitizedPriorities picks
// the destination by iterating the higher-priority tserver list front-to-back and taking the
// first one with a follower replica — correct under count-based (front = count-lightest) but
// wrong under heat-aware (front = coolest bucket). A cool but count-heavy preferred-zone tserver
// can win over a warmer but count-lighter one, worsening leader-count balance inside the
// preferred zone.
TEST_F(ClusterBalanceStrategyParityTest,
       HeatAwareAffinitizedDestinationChosenByCountNotHeat) {
  // Custom layout: 0000 and 1111 both in zone "a" (priority 0); 2222 in zone "b" (priority 1).
  // Default PrepareTestStateMultiAz would put 1111 in zone "b"; we need two candidates in the
  // preferred zone so the destination scan has a real choice to make.
  std::vector<std::shared_ptr<TSDescriptor>> ts_descs = {
      SetupTS("0000", "a"), SetupTS("1111", "a"), SetupTS("2222", "b")};
  PrepareTestState(ts_descs);
  affinitized_zones_.clear();
  affinitized_zones_.push_back(MakeAffinitizedZoneSet({"a"}));
  affinitized_zones_.push_back(MakeAffinitizedZoneSet({"b"}));

  // Default leader distribution from PrepareTestState is round-robin:
  //   tablets_[0] -> 0000, tablets_[1] -> 1111, tablets_[2] -> 2222, tablets_[3] -> 0000.
  // Move tablets_[1] off 1111 and onto 2222 so the final counts are
  //   0000: 2 leaders, 1111: 0 leaders, 2222: 2 leaders.
  // The affinitized drain now has to pick between 0000 (count 2) and 1111 (count 0) in priority 0.
  MoveTabletLeader(tablets_[1].get(), ts_descs_[2]);

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));
  auto trace = ASSERT_RESULT(CollectTrace(/*max_decisions=*/1, [&]() {
    // Make 1111 heat-hot (bucket above hysteresis). 0000 and 2222 stay cool (no seed = bucket 0).
    // Under heat-aware ordering sorted_leader_load_[0] becomes [0000 (bucket 0), 1111 (bucket
    // high)]. Without the fix, the destination scan picks 0000 despite its higher count; with the
    // fix, the function uses a locally count-sorted view and picks 1111.
    cb_.SetHeatForTest(
        "1111", {.sum_read_ops_per_sec = 500.0, .sum_write_ops_per_sec = 0.0,
                 .leader_tablet_count = 0});
  }));

  ASSERT_FALSE(trace.empty());
  EXPECT_EQ(trace.front().from_ts, "2222");
  EXPECT_EQ(trace.front().to_ts, "1111")
      << "Expected destination to be 1111 (count 0) — the count-lightest priority-0 candidate. "
      << "Got to_ts=" << trace.front().to_ts << ", which means the preferred-zone balancer sent "
      << "a leader to an already count-heavier tserver just because it had a cooler heat bucket.";
}

// RemoveReplica's leader-stepdown path (cluster_balance.cc:1753) invokes MoveLeader with an empty
// to_ts when either FLAGS_cluster_balancer_stepdown_to_preferred_leader_on_remove is false, or
// SelectBestLeaderAfterStepdown finds no viable replica (returns ""). Without a guard, the
// projection would (a) credit heat_by_ts_[""] with the moved tablet's read/write ops, (b) rewrite
// heat_by_tablet_[tablet].leader_uuid to "", stranding this tablet's heat until the next
// heartbeat-driven snapshot. Both effects would corrupt any heat-aware decision made later in the
// same run.
TEST_F(ClusterBalanceStrategyParityTest, HeatProjectionIsNoOpWhenToTsIsEmpty) {
  PrepareTestStateMultiAz();

  constexpr double kSourceRead = 200.0;
  constexpr double kSourceWrite = 50.0;
  const auto tablet_id = tablets_[0]->tablet_id();
  cb_.SetHeatForTest(
      "0000", {.sum_read_ops_per_sec = kSourceRead, .sum_write_ops_per_sec = kSourceWrite,
               .leader_tablet_count = 1});
  cb_.SetTabletHeatForTest(
      tablet_id,
      LeaderHeatRecord{.leader_uuid = "0000", .read_ops_per_sec = kSourceRead,
                       .write_ops_per_sec = kSourceWrite, .last_updated = MonoTime::Now()});

  cb_.ProjectLeaderHeatMoveForTest(tablet_id, "0000", /*to_ts=*/"");

  const auto post_source = cb_.GetHeatForTest("0000");
  EXPECT_DOUBLE_EQ(post_source.sum_read_ops_per_sec, kSourceRead)
      << "Projection debited heat from source despite unknown destination — stale source heat is "
      << "preferable to mis-attributed heat.";
  EXPECT_DOUBLE_EQ(post_source.sum_write_ops_per_sec, kSourceWrite);
  EXPECT_EQ(post_source.leader_tablet_count, 1);

  EXPECT_FALSE(cb_.HasHeatEntryForTest(""))
      << "Projection created a bogus heat_by_ts_[\"\"] entry — any later heat-aware decision "
      << "would treat the empty string as a real tserver and possibly pick it as coolest.";

  const auto record = cb_.GetTabletHeatForTest(tablet_id);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->leader_uuid, "0000")
      << "Projection rewrote heat_by_tablet_[tablet].leader_uuid to empty string — subsequent "
      << "moves of the same tablet in this run would hit the mismatch no-op branch and lose "
      << "their projection.";
}

// heat_aware_recent_leader_moves_ survives ResetGlobalState, so EvictExpiredHeatCooldowns is the
// only thing that bounds its size across runs. Previously the eviction sweep was gated behind
// FLAGS_enable_load_balancer_heat_telemetry — turning that off while entries were still in the
// map would freeze them in place indefinitely, leaking memory and (once the flag turned back on)
// silently extending the effective cooldown past its configured window. The sweep must run
// regardless of telemetry state.
TEST_F(ClusterBalanceStrategyParityTest, HeatCooldownEvictsWhenTelemetryDisabled) {
  PrepareTestStateMultiAz();

  // Seed a cooldown entry. Value is MonoTime::Now() — a setting of
  // load_balancer_heat_leader_move_cooldown_secs=0 below will make EvictExpiredHeatCooldowns
  // treat every entry as expired (its short-circuit: cooldown disabled → clear map).
  cb_.RecordHeatAwareLeaderMoveForTest("tablet--0", "0000", "1111");
  ASSERT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), 1u);

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_load_balancer_heat_telemetry) = false;

  cb_.AggregateLeaderHeatForTest();

  EXPECT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), 0u)
      << "Cooldown entries must age out even when heat telemetry is disabled — otherwise "
      << "toggling enable_load_balancer_heat_telemetry off freezes the map indefinitely and "
      << "breaks the bounded-size guarantee on heat_aware_recent_leader_moves_.";
}

// Regression guard for the stranded-leader eviction path surviving AnalyzeTablets' pending-task
// replay. At the start of every run, AnalyzeTablets replays each entry in
// pending_stepdown_leader_tasks_ via state_->MoveLeader, which unconditionally rewrites
// per_tablet_meta_[tablet].leader_uuid to the pending task's to_ts — even when the async RPC is
// still in flight or being retried after failure. If IsInHeatAwareCooldown checked leader_uuid
// directly it would see to_ts, fail the stranded-leader test, and keep suppressing the retry for
// the full cooldown window (default 300s). initial_leader_uuid is the pre-replay snapshot of the
// replica map and is not mutated by MoveLeader, so it still reads as from_ts in that state and
// lets the eviction path fire.
TEST_F(ClusterBalanceStrategyParityTest, HeatCooldownEvictedDespitePendingReplayProjectingLeader) {
  PrepareTestStateMultiAz();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 3600;

  cb_.SetStrategyForTest(
      CreateLoadBalancerStrategy(kLoadBalancerStrategyHeatAwareExperimental));

  // PrepareTestStateMultiAz places tablets_[0]'s leader on 0000 in the replica map.
  const auto& tablet_id = tablets_[0]->tablet_id();
  cb_.RecordHeatAwareLeaderMoveForTest(tablet_id, "0000", "1111");
  ASSERT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), 1u);

  // Plant a pending stepdown task whose replay will project leader_uuid = 1111 inside
  // AnalyzeTablets. Production reaches this state when a prior run's heat-driven 0000 -> 1111
  // move is still in flight (or has silently failed and is being retried) when the next balancer
  // pass starts.
  cb_.pending_stepdown_leader_tasks_[tablet_id] = "1111";
  ASSERT_OK(ResetLoadBalancerAndAnalyzeTablets());

  const auto* table_state = cb_.GetTableState(kTableId);
  ASSERT_NE(table_state, nullptr);
  const auto& tablet_meta = table_state->per_tablet_meta_.at(tablet_id);
  ASSERT_EQ(tablet_meta.leader_uuid, "1111")
      << "Precondition: the pending-task replay must have projected leader_uuid forward — "
      << "otherwise this test is not exercising the bug.";
  ASSERT_EQ(tablet_meta.initial_leader_uuid, "0000")
      << "initial_leader_uuid must be preserved at its replica-map value; this is the signal "
      << "IsInHeatAwareCooldown relies on.";

  EXPECT_FALSE(cb_.IsInHeatAwareCooldownForTest(tablet_id, "0000", "1111"))
      << "Cooldown must evict even after the pending-task replay projected leader_uuid to the "
      << "destination — the replica map still reports the leader on 0000, so the recorded move "
      << "has not actually taken effect and retry must be allowed.";
  EXPECT_EQ(cb_.GetHeatAwareCooldownSizeForTest(), 0u)
      << "The stale cooldown entry must be removed from the map, not merely reported as false.";
}

// Regression guard for heat-snapshot / pending-task-replay drift. AggregateLeaderHeatIntoGlobalState
// runs at the top of a balancer run, before AnalyzeTablets replays pending_stepdown_leader_tasks_
// via state_->MoveLeader. Without reconciliation, heat_by_tablet_[tablet].leader_uuid keeps
// crediting the pre-replay source while per_tablet_meta_ / sorted_leader_load_ already reflect the
// pending task's destination. A subsequent heat-driven to_ts -> C move of the same tablet would
// then hit the record.leader_uuid != from_ts guard in ProjectLeaderHeatMoveIntoGlobalState and
// skip heat projection entirely — drifting heat_by_ts_ permanently out of sync with the
// balancer's own decisions for the rest of the run. The AnalyzeTablets replay therefore calls
// ProjectLeaderHeatMoveIntoGlobalState alongside state_->MoveLeader to keep the snapshot in step.
TEST_F(ClusterBalanceStrategyParityTest, HeatSnapshotReconciledByPendingStepdownReplay) {
  PrepareTestStateMultiAz();

  constexpr double kRead = 300.0;
  constexpr double kWrite = 80.0;
  const auto tablet_id = tablets_[0]->tablet_id();

  // Plant a pending stepdown on the mock. AnalyzeTablets will replay it and (with the fix) also
  // project the heat snapshot from 0000 onto 1111.
  cb_.pending_stepdown_leader_tasks_[tablet_id] = "1111";

  // Seed heat into the fresh global state AFTER ResetLoadBalancerState (which wipes global_state_)
  // but BEFORE AnalyzeTablets runs the pending-task replay. In production, this is the window
  // AggregateLeaderHeatIntoGlobalState occupies.
  ASSERT_OK(ResetLoadBalancerAndAnalyzeTablets([this, tablet_id]() {
    cb_.SetHeatForTest("0000", {.sum_read_ops_per_sec = kRead,
                                .sum_write_ops_per_sec = kWrite,
                                .leader_tablet_count = 1});
    cb_.SetTabletHeatForTest(
        tablet_id,
        LeaderHeatRecord{.leader_uuid = "0000", .read_ops_per_sec = kRead,
                         .write_ops_per_sec = kWrite, .last_updated = MonoTime::Now()});
  }));

  // After the fix: the heat snapshot is projected forward as part of the replay, so
  // heat_by_tablet_[tablet_id].leader_uuid is now 1111 and the per-tserver heat aggregates have
  // shifted to match what sorted_leader_load_ sees.
  const auto post_replay_record = cb_.GetTabletHeatForTest(tablet_id);
  ASSERT_TRUE(post_replay_record.has_value())
      << "Pre-replay seeded heat record must still exist in the map.";
  EXPECT_EQ(post_replay_record->leader_uuid, "1111")
      << "heat_by_tablet_[tablet].leader_uuid must be projected to the pending task's to_ts so "
      << "later heat projections for this tablet start from the correct from_ts.";

  const auto source_agg = cb_.GetHeatForTest("0000");
  EXPECT_DOUBLE_EQ(source_agg.sum_read_ops_per_sec, 0.0)
      << "heat_by_ts_[0000] must be debited by the pending task's read contribution so it no "
      << "longer credits leader rates that sorted_leader_load_ has already moved to 1111.";
  EXPECT_DOUBLE_EQ(source_agg.sum_write_ops_per_sec, 0.0);

  const auto dest_agg = cb_.GetHeatForTest("1111");
  EXPECT_DOUBLE_EQ(dest_agg.sum_read_ops_per_sec, kRead)
      << "heat_by_ts_[1111] must be credited with the pending task's read contribution.";
  EXPECT_DOUBLE_EQ(dest_agg.sum_write_ops_per_sec, kWrite);

  // A subsequent heat-driven move of the same tablet out of the replay's destination must be able
  // to project heat without tripping the record.leader_uuid != from_ts guard. Without the
  // reconciliation, record.leader_uuid would still be 0000 and this projection would no-op.
  cb_.ProjectLeaderHeatMoveForTest(tablet_id, "1111", "2222");
  const auto after_second_move_source = cb_.GetHeatForTest("1111");
  EXPECT_DOUBLE_EQ(after_second_move_source.sum_read_ops_per_sec, 0.0)
      << "Second projection must debit 1111 — proving record.leader_uuid was synced to 1111 and "
      << "the second projection did not silently skip.";
  const auto after_second_move_dest = cb_.GetHeatForTest("2222");
  EXPECT_DOUBLE_EQ(after_second_move_dest.sum_read_ops_per_sec, kRead)
      << "Second projection must credit 2222 with the tablet's heat.";
}

}  // namespace master
}  // namespace yb
