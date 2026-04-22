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

#include <unordered_map>

#include "yb/common/entity_ids_types.h"
#include "yb/tserver/heartbeater.h"

namespace yb {
namespace tserver {

class TServerMetricsHeartbeatDataProvider : public PeriodicalHeartbeatDataProvider {
 public:
  explicit TServerMetricsHeartbeatDataProvider(TabletServer* server);

 private:
  void DoAddData(bool needs_full_tablet_report, master::TSHeartbeatRequestPB* req) override;
  MonoDelta Period() const override;

  uint64_t CalculateUptime();

  MonoTime start_time_;

  // Stores the total read and writes ops for computing iops.
  uint64_t prev_reads_ = 0;
  uint64_t prev_writes_ = 0;

  // Per-tablet previous read/write op counters used to compute per-leader op rates for
  // heat-aware balancing telemetry. Pruned lazily each iteration.
  struct PrevTabletOpCounts {
    uint64_t reads = 0;
    uint64_t writes = 0;
  };
  std::unordered_map<TabletId, PrevTabletOpCounts> prev_tablet_op_counts_;
};

} // namespace tserver
} // namespace yb
