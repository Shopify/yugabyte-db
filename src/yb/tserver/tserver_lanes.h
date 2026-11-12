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

#include <string>

#include "yb/gutil/ref_counted.h"

#include "yb/util/lanes.h"
#include "yb/util/metrics_fwd.h"
#include "yb/util/result.h"

namespace yb {

class MemTracker;

} // namespace yb

namespace yb::rpc {

class MessengerBuilder;

} // namespace yb::rpc

namespace yb::tserver {

// Computes the lane topology from the tserver_lane* flags, sysfs and the process cpuset, without
// touching the process-global topology. With --tserver_lanes=1 it reads nothing and returns the
// inactive topology. If description is not null it receives a one-line summary for the log.
//
// Once an active topology is installed, the process cpuset is taken from it rather than from
// the calling thread's mask, which may be confined by then.
Result<LaneTopology> ComputeTserverLaneTopology(std::string* description = nullptr);

// Computes the topology, installs it with SetLaneTopology and logs it. Never fails: a discovery
// error is logged as a WARNING and the tserver runs with lanes off.
void InitTserverLaneTopology();

// Makes builder build a laned messenger when the installed topology is active, with the
// tserver_lane_reactors and tserver_lane_connections_per_remote settings; does nothing
// otherwise. Only the tablet server's own messenger is laned.
void SetupTserverLaneMessenger(rpc::MessengerBuilder* builder);

// Lane of a tablet hosted by this tablet server: LaneOf(tablet_id) when lane mode is active,
// kNoLane otherwise, so that lanes off selects the unlaned pools and sends no lane hints.
LaneId TserverTabletLane(Slice tablet_id);

// Lane of the RocksDB flush and compaction tasks of a tablet in tablet_lane: tablet_lane, or
// kNoLane (the service mask) with --tserver_lane_background=service.
LaneId TserverBackgroundTaskLane(LaneId tablet_lane);

// Registers the lane crossing metrics (lane_crossings_<category>, lane_entries_from_unlaned) on
// entity when lane mode is active; does nothing otherwise. The counts are process-wide.
void RegisterTserverLaneMetrics(const scoped_refptr<MetricEntity>& entity);

// In lane mode, lane-batches the memory trackers that threads of every lane update per operation:
// the root, the server tracker and its children (existing and later), and the RPC read buffer and
// call subtrees. No-op with lanes off.
void EnableTserverLaneMemTrackerBatching(const std::shared_ptr<MemTracker>& server_tracker);

} // namespace yb::tserver
