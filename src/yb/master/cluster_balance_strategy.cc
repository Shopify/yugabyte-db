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

#include "yb/master/cluster_balance_util.h"

#include "yb/util/logging.h"

namespace yb {
namespace master {

const char* const kLoadBalancerStrategyCountBased = "count_based";
const char* const kLoadBalancerStrategyHeatAwareExperimental = "heat_aware_experimental";

namespace {
const std::string kCountBasedName(kLoadBalancerStrategyCountBased);
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

const std::string& CountBasedLoadBalancerStrategy::name() const {
  return kCountBasedName;
}

std::unique_ptr<LoadBalancerStrategy> CreateLoadBalancerStrategy(const std::string& name) {
  if (name == kLoadBalancerStrategyCountBased) {
    return std::make_unique<CountBasedLoadBalancerStrategy>();
  }
  if (name == kLoadBalancerStrategyHeatAwareExperimental) {
    YB_LOG_EVERY_N_SECS(WARNING, 60)
        << "load_balancer_strategy=" << name
        << " selected but heat-aware balancing is not yet implemented; falling back to "
        << kLoadBalancerStrategyCountBased;
    return std::make_unique<CountBasedLoadBalancerStrategy>();
  }
  YB_LOG_EVERY_N_SECS(WARNING, 60)
      << "Unknown load_balancer_strategy '" << name << "'; falling back to "
      << kLoadBalancerStrategyCountBased;
  return std::make_unique<CountBasedLoadBalancerStrategy>();
}

}  // namespace master
}  // namespace yb
