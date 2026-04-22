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

#include "yb/master/cluster_balance_heat_cache.h"

namespace yb::master {

void ClusterBalanceHeatCache::UpdateLeaderHeat(
    const TabletId& tablet_id, const LeaderHeatRecord& record) {
  std::lock_guard lock(mu_);
  by_tablet_[tablet_id] = record;
}

void ClusterBalanceHeatCache::ClearLeaderHeatIfFrom(
    const TabletId& tablet_id, const TabletServerId& reporter_uuid) {
  std::lock_guard lock(mu_);
  const auto it = by_tablet_.find(tablet_id);
  if (it != by_tablet_.end() && it->second.leader_uuid == reporter_uuid) {
    by_tablet_.erase(it);
  }
}

std::optional<LeaderHeatRecord> ClusterBalanceHeatCache::GetLeaderHeat(
    const TabletId& tablet_id, MonoDelta staleness_threshold) const {
  std::lock_guard lock(mu_);
  const auto it = by_tablet_.find(tablet_id);
  if (it == by_tablet_.end()) {
    return std::nullopt;
  }
  if (MonoTime::Now().GetDeltaSince(it->second.last_updated) > staleness_threshold) {
    return std::nullopt;
  }
  return it->second;
}

std::unordered_map<TabletId, LeaderHeatRecord> ClusterBalanceHeatCache::SnapshotFresh(
    MonoDelta staleness_threshold) const {
  std::lock_guard lock(mu_);
  const MonoTime now = MonoTime::Now();
  std::unordered_map<TabletId, LeaderHeatRecord> result;
  result.reserve(by_tablet_.size());
  for (const auto& [tablet_id, record] : by_tablet_) {
    if (now.GetDeltaSince(record.last_updated) <= staleness_threshold) {
      result.emplace(tablet_id, record);
    }
  }
  return result;
}

void ClusterBalanceHeatCache::ClearForTest() {
  std::lock_guard lock(mu_);
  by_tablet_.clear();
}

size_t ClusterBalanceHeatCache::SizeForTest() const {
  std::lock_guard lock(mu_);
  return by_tablet_.size();
}

} // namespace yb::master
