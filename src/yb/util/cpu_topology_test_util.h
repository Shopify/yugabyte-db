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
#include <string>
#include <vector>

#include "yb/util/status.h"

namespace yb {

// Fake sysfs cpu tree (laid out like /sys/devices/system/cpu) for topology tests.

struct FakeCache {
  int level;
  std::string shared_cpu_list;
};

struct FakeCpu {
  size_t cpu;
  // nullopt: no topology/physical_package_id file.
  std::optional<int> package = {};
  // Empty: no topology/thread_siblings_list file.
  std::string thread_siblings = {};
  // Written as cache/index0, index1, ...
  std::vector<FakeCache> caches = {};
};

struct FakeCpuLayout {
  size_t sockets = 1;
  size_t l3_per_socket = 1;
  size_t cores_per_l3 = 1;
  // Threads per core. Thread t of core c is CPU c + t * total_cores, as Linux numbers SMT
  // siblings on x86.
  size_t smt = 1;
  // Each core gets L1d, L1i and L2 caches private to it; with_l3 adds an L3 per l3 group.
  bool with_l3 = true;
};

std::vector<FakeCpu> MakeFakeCpus(const FakeCpuLayout& layout);

Status WriteFakeSysfsCpuTree(const std::string& root, const std::vector<FakeCpu>& cpus);

} // namespace yb
