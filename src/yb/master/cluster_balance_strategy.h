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

// Owns the scorer plus any future per-run strategy state. Phase 1 only exposes the scorer; phase 3
// will extend this with candidate-selection and move-evaluation hooks.
class LoadBalancerStrategy {
 public:
  virtual ~LoadBalancerStrategy() = default;

  virtual const std::string& name() const = 0;
  virtual const LoadScorer& scorer() const = 0;
};

class CountBasedLoadBalancerStrategy : public LoadBalancerStrategy {
 public:
  const std::string& name() const override;
  const LoadScorer& scorer() const override { return scorer_; }

 private:
  CountBasedLoadScorer scorer_;
};

// Flag values for --load_balancer_strategy.
extern const char* const kLoadBalancerStrategyCountBased;
extern const char* const kLoadBalancerStrategyHeatAwareExperimental;

// Factory. `heat_aware_experimental` is reserved but not yet implemented; in phase 1 the factory
// logs a warning and returns the count-based strategy as a safe fallback. Unknown names also fall
// back to count-based.
std::unique_ptr<LoadBalancerStrategy> CreateLoadBalancerStrategy(const std::string& name);

}  // namespace master
}  // namespace yb
