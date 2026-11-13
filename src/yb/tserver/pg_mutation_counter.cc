// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugabyteDB development.
//
// Portions Copyright (c) YugabyteDB, Inc.
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

#include "yb/tserver/pg_mutation_counter.h"

#include "yb/common/entity_ids.h"

#include "yb/util/logging.h"
#include "yb/util/shared_lock.h"

namespace yb {
namespace tserver {

PgMutationCounter::Shard& PgMutationCounter::LocalShard() {
  if (auto* shard = lane_shards_.LaneLocal()) {
    return *shard;
  }
  if (LaneModeActive() && ThisThreadLane() != kNoLane && !lane_shards_.allocated()) {
    lane_shards_.Allocate(GetLaneTopology().num_lanes());
    if (auto* shard = lane_shards_.LaneLocal()) {
      return *shard;
    }
  }
  return shared_shard_;
}

void PgMutationCounter::Increase(const TableId& table_id, uint64 mutation_count) {
  VLOG_WITH_FUNC(4) << "table_id: " << table_id << " mutation_count: " << mutation_count;
  auto& shard = LocalShard();
  {
    // The mutex is used for only guarding membership changes to the map. A shared lock is needed
    // when checking for membership and an exclusive lock is needed if updating membership i.e.,
    // adding/ removing a key from the map.
    //
    // Incrementing the counters doesn't need a mutex because we use atomic for the counter.
    SharedLock shared_lock(shard.mutex);
    auto it = shard.counts.find(table_id);
    if (it != shard.counts.end()) {
      it->second += mutation_count;
      return;
    }
  }

  std::lock_guard lock(shard.mutex);
  shard.counts[table_id] += mutation_count;
}

void PgMutationCounter::MoveCounts(Shard* shard, TableMutationCounts* out) {
  TableMutationCounts counts;
  {
    std::lock_guard lock(shard->mutex);
    counts.swap(shard->counts);
  }
  if (out->empty()) {
    out->swap(counts);
    return;
  }
  for (const auto& [table_id, count] : counts) {
    (*out)[table_id] += count.load();
  }
}

TableMutationCounts PgMutationCounter::GetAndClear() {
  VLOG_WITH_FUNC(4) << "Getting and clearing global table mutation counter";
  TableMutationCounts result;
  MoveCounts(&shared_shard_, &result);
  lane_shards_.ForEach([&result](Shard& shard) { MoveCounts(&shard, &result); });
  return result;
}

}  // namespace tserver
}  // namespace yb
