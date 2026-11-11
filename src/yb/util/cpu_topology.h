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

#ifdef __linux__
#include <sched.h>
#endif

#include <array>
#include <bit>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "yb/util/enums.h"
#include "yb/util/result.h"

namespace yb {

// Set of CPU ids, sized to hold everything a glibc cpu_set_t can (CPU_SETSIZE = 1024).
class CpuSet {
 public:
  static constexpr size_t kMaxCpus = 1024;

  CpuSet() = default;

  // Parses a Linux cpu-list string such as "0-3,8,10-11", the format of sysfs shared_cpu_list
  // and cgroup cpuset files. Surrounding whitespace is ignored; an empty string is the empty set.
  static Result<CpuSet> Parse(std::string_view cpu_list);

  // CPUs [0, count).
  static CpuSet FirstN(size_t count);

#ifdef __linux__
  static CpuSet FromCpuSetT(const cpu_set_t& set);
  void ToCpuSetT(cpu_set_t* out) const;
#endif

  // Ids >= kMaxCpus are a DFATAL and are not added.
  void Set(size_t cpu);
  bool Test(size_t cpu) const;
  size_t Count() const;
  bool empty() const;

  // Lowest CPU in the set, or nullopt when empty.
  std::optional<size_t> Lowest() const;

  CpuSet& operator&=(const CpuSet& rhs);
  CpuSet& operator|=(const CpuSet& rhs);
  // Removes rhs from this set.
  CpuSet& operator-=(const CpuSet& rhs);

  friend CpuSet operator&(CpuSet lhs, const CpuSet& rhs) { return lhs &= rhs; }
  friend CpuSet operator|(CpuSet lhs, const CpuSet& rhs) { return lhs |= rhs; }
  friend CpuSet operator-(CpuSet lhs, const CpuSet& rhs) { return lhs -= rhs; }

  bool operator==(const CpuSet& rhs) const = default;

  template <class F>
  void ForEach(const F& f) const {
    for (size_t w = 0; w != kWords; ++w) {
      for (auto word = words_[w]; word; word &= word - 1) {
        f(w * kBitsPerWord + std::countr_zero(word));
      }
    }
  }

  std::vector<size_t> ToVector() const;

  // Cpu-list format, inverse of Parse. Empty set formats as "".
  std::string ToString() const;

 private:
  static constexpr size_t kBitsPerWord = 64;
  static constexpr size_t kWords = kMaxCpus / kBitsPerWord;

  std::array<uint64_t, kWords> words_ = {};
};

// kL3: CPUs sharing the highest cache level present. kSocket: CPUs in the same physical package.
YB_DEFINE_ENUM(CpuDomainKind, (kL3)(kSocket));

struct CpuTopologyOptions {
  CpuDomainKind domain_kind = CpuDomainKind::kL3;

  // Root of the sysfs cpu tree (normally /sys/devices/system/cpu). Empty means
  // --TEST_cpu_topology_sysfs_root if set, else the real tree on Linux and none elsewhere.
  std::string sysfs_root = {};

  // CPUs this process may run on. Unset means --TEST_cpu_topology_cpuset if set, else
  // sched_getaffinity on Linux and every CPU elsewhere.
  std::optional<CpuSet> cpuset = {};
};

// The CPUs the process may run on, resolved as described for CpuTopologyOptions::cpuset.
Result<CpuSet> GetProcessCpuSet(const CpuTopologyOptions& options);

// Groups the process cpuset into CPU domains: disjoint, non-empty, ordered by lowest CPU.
//
// kL3 groups by the shared_cpu_list of the highest cache level present. When that level is per
// core (every group equals the CPU's SMT siblings) or no CPU has cache info, it falls back to
// kSocket, which groups by physical_package_id; with no package info the result is one domain
// covering the cpuset. Files that exist but cannot be parsed, or inconsistent groups, are
// errors. If source is not null it receives a short description of what the domains came from.
Result<std::vector<CpuSet>> DiscoverCpuDomains(
    const CpuTopologyOptions& options, std::string* source = nullptr);

} // namespace yb
