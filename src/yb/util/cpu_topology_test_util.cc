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

#include "yb/util/cpu_topology_test_util.h"

#include "yb/util/cpu_topology.h"
#include "yb/util/env.h"
#include "yb/util/format.h"

namespace yb {

std::vector<FakeCpu> MakeFakeCpus(const FakeCpuLayout& layout) {
  const size_t cores_per_socket = layout.l3_per_socket * layout.cores_per_l3;
  const size_t total_cores = layout.sockets * cores_per_socket;
  auto threads_of_cores = [&](size_t first_core, size_t num_cores) {
    CpuSet result;
    for (size_t t = 0; t != layout.smt; ++t) {
      for (size_t c = first_core; c != first_core + num_cores; ++c) {
        result.Set(c + t * total_cores);
      }
    }
    return result.ToString();
  };

  std::vector<FakeCpu> result;
  for (size_t t = 0; t != layout.smt; ++t) {
    for (size_t core = 0; core != total_cores; ++core) {
      auto siblings = threads_of_cores(core, 1);
      FakeCpu cpu {
        .cpu = core + t * total_cores,
        .package = static_cast<int>(core / cores_per_socket),
        .thread_siblings = siblings,
        .caches = {{1, siblings}, {1, siblings}, {2, siblings}},
      };
      if (layout.with_l3) {
        auto l3_first_core = core / layout.cores_per_l3 * layout.cores_per_l3;
        cpu.caches.push_back({3, threads_of_cores(l3_first_core, layout.cores_per_l3)});
      }
      result.push_back(std::move(cpu));
    }
  }
  return result;
}

Status WriteFakeSysfsCpuTree(const std::string& root, const std::vector<FakeCpu>& cpus) {
  auto* env = Env::Default();
  for (const auto& cpu : cpus) {
    auto cpu_dir = Format("$0/cpu$1", root, cpu.cpu);
    RETURN_NOT_OK(env->CreateDirs(cpu_dir + "/topology"));
    if (cpu.package) {
      RETURN_NOT_OK(WriteStringToFile(
          env, Format("$0\n", *cpu.package), cpu_dir + "/topology/physical_package_id"));
    }
    if (!cpu.thread_siblings.empty()) {
      RETURN_NOT_OK(WriteStringToFile(
          env, cpu.thread_siblings + "\n", cpu_dir + "/topology/thread_siblings_list"));
    }
    for (size_t i = 0; i != cpu.caches.size(); ++i) {
      auto index_dir = Format("$0/cache/index$1", cpu_dir, i);
      RETURN_NOT_OK(env->CreateDirs(index_dir));
      RETURN_NOT_OK(WriteStringToFile(
          env, Format("$0\n", cpu.caches[i].level), index_dir + "/level"));
      RETURN_NOT_OK(WriteStringToFile(
          env, cpu.caches[i].shared_cpu_list + "\n", index_dir + "/shared_cpu_list"));
    }
  }
  return Status::OK();
}

} // namespace yb
