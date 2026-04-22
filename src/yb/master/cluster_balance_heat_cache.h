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

#include <optional>
#include <unordered_map>

#include "yb/common/entity_ids_types.h"

#include "yb/gutil/thread_annotations.h"

#include "yb/util/locks.h"
#include "yb/util/monotime.h"

namespace yb::master {

struct LeaderHeatRecord {
  TabletServerId leader_uuid;
  double read_ops_per_sec = 0;
  double write_ops_per_sec = 0;
  MonoTime last_updated;
};

// In-memory per-tablet leader heat cache populated by the master heartbeat service whenever a
// leader tserver reports load_balancer_heat_telemetry fields. Read from at the start of each
// cluster balancer run to aggregate per-tserver heat in GlobalLoadState.
//
// Held on CatalogManager rather than on the per-tablet locked state so that heartbeat handling
// does not need to acquire tablet metadata locks.
class ClusterBalanceHeatCache {
 public:
  ClusterBalanceHeatCache() = default;

  // Insert or update the heat record for a tablet. `record.last_updated` should be set by the
  // caller, typically to MonoTime::Now().
  void UpdateLeaderHeat(const TabletId& tablet_id, const LeaderHeatRecord& record);

  // Drops the cached heat record for `tablet_id` only if the currently-recorded leader matches
  // `reporter_uuid`. Called when a tserver reports a follower/no-lease peer for a tablet it was
  // previously cached as leader of — so step-down immediately invalidates its stale heat instead
  // of waiting out the staleness window. No-op if the cache's leader_uuid differs (some other
  // tserver is now the leader and should own the entry) or if there is no cached record.
  void ClearLeaderHeatIfFrom(const TabletId& tablet_id, const TabletServerId& reporter_uuid);

  // Returns the latest heat record for the tablet, or nullopt if none was reported or the record
  // is older than `staleness_threshold`. Stale entries are NOT pruned here.
  std::optional<LeaderHeatRecord> GetLeaderHeat(
      const TabletId& tablet_id, MonoDelta staleness_threshold) const;

  // Returns a snapshot of all records whose age is <= `staleness_threshold`. Entries older than
  // that threshold are excluded from the returned map (and left in place; cleanup is opportunistic
  // on UpdateLeaderHeat for tablets that keep reporting).
  std::unordered_map<TabletId, LeaderHeatRecord> SnapshotFresh(
      MonoDelta staleness_threshold) const;

  // Test helper: drops all entries.
  void ClearForTest();

  // Test helper: returns the raw map size, including stale entries.
  size_t SizeForTest() const;

 private:
  mutable simple_spinlock mu_;
  std::unordered_map<TabletId, LeaderHeatRecord> by_tablet_ GUARDED_BY(mu_);
};

} // namespace yb::master
