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

#include "yb/util/lanes.h"
#include "yb/util/result.h"

namespace yb::tserver {

// Computes the lane topology from the tserver_lane* flags, sysfs and the process cpuset, without
// touching the process-global topology. With --tserver_lanes=1 it reads nothing and returns the
// inactive topology. If description is not null it receives a one-line summary for the log.
Result<LaneTopology> ComputeTserverLaneTopology(std::string* description = nullptr);

// Computes the topology, installs it with SetLaneTopology and logs it. Never fails: a discovery
// error is logged as a WARNING and the tserver runs with lanes off.
void InitTserverLaneTopology();

} // namespace yb::tserver
