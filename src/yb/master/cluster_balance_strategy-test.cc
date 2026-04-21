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

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "yb/master/cluster_balance_strategy.h"
#include "yb/master/load_balancer_mocked-test_base.h"

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
  Result<std::vector<Decision>> CollectTrace(int max_decisions = 12) NO_THREAD_SAFETY_ANALYSIS {
    std::vector<Decision> trace;
    RETURN_NOT_OK(ResetLoadBalancerAndAnalyzeTablets());
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

}  // namespace master
}  // namespace yb
