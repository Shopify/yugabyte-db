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

#include "yb/util/cpu_topology.h"

#include <algorithm>
#include <map>

#include "yb/gutil/sysinfo.h"

#include "yb/util/env.h"
#include "yb/util/errno.h"
#include "yb/util/faststring.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/status_format.h"
#include "yb/util/stol_utils.h"

DEFINE_test_flag(string, cpu_topology_sysfs_root, "",
    "Root of a fake sysfs cpu tree (laid out like /sys/devices/system/cpu) used by CPU topology "
    "discovery instead of the real one.");

DEFINE_test_flag(string, cpu_topology_cpuset, "",
    "Cpu-list used by CPU topology discovery as the process cpuset instead of "
    "sched_getaffinity. Does not change the real affinity of any thread.");

namespace yb {

namespace {

constexpr std::string_view kWhitespace = " \t\r\n";

std::string_view Trim(std::string_view s) {
  auto begin = s.find_first_not_of(kWhitespace);
  if (begin == std::string_view::npos) {
    return {};
  }
  auto end = s.find_last_not_of(kWhitespace);
  return s.substr(begin, end - begin + 1);
}

Result<size_t> ParseCpuId(std::string_view token, std::string_view input) {
  // At most 4 digits keeps the conversion in range; ids >= kMaxCpus are rejected below.
  if (token.empty() || token.size() > 4 ||
      !std::all_of(token.begin(), token.end(), [](char c) { return c >= '0' && c <= '9'; })) {
    return STATUS_FORMAT(InvalidArgument, "Bad CPU id '$0' in cpu list '$1'", token, input);
  }
  size_t value = 0;
  for (char c : token) {
    value = value * 10 + static_cast<size_t>(c - '0');
  }
  if (value >= CpuSet::kMaxCpus) {
    return STATUS_FORMAT(
        InvalidArgument, "CPU id $0 in cpu list '$1' is not below $2", value, input,
        CpuSet::kMaxCpus);
  }
  return value;
}

class SysfsReader {
 public:
  explicit SysfsReader(std::string root) : root_(std::move(root)) {}

  std::string CpuPath(size_t cpu, std::string_view relative) const {
    return Format("$0/cpu$1/$2", root_, cpu, relative);
  }

  // nullopt if the file does not exist.
  Result<std::optional<std::string>> Read(const std::string& path) const {
    if (root_.empty() || !env_->FileExists(path)) {
      return std::nullopt;
    }
    faststring data;
    RETURN_NOT_OK_PREPEND(ReadFileToString(env_, path, &data), Format("Reading $0", path));
    return std::string(Trim(data.ToString()));
  }

  Result<std::optional<int64_t>> ReadInt(const std::string& path) const {
    auto content = VERIFY_RESULT(Read(path));
    if (!content) {
      return std::nullopt;
    }
    return VERIFY_RESULT_PREPEND(CheckedStoll(*content), Format("Parsing $0", path));
  }

  Result<std::optional<CpuSet>> ReadCpuList(const std::string& path) const {
    auto content = VERIFY_RESULT(Read(path));
    if (!content) {
      return std::nullopt;
    }
    return VERIFY_RESULT_PREPEND(CpuSet::Parse(*content), Format("Parsing $0", path));
  }

  Result<std::vector<std::string>> CacheIndexDirs(size_t cpu) const {
    std::vector<std::string> result;
    auto dir = CpuPath(cpu, "cache");
    if (root_.empty() || !env_->DirExists(dir)) {
      return result;
    }
    auto children = VERIFY_RESULT(env_->GetChildren(dir, ExcludeDots::kTrue));
    for (auto& child : children) {
      if (child.starts_with("index")) {
        result.push_back(Format("$0/$1", dir, child));
      }
    }
    std::sort(result.begin(), result.end());
    return result;
  }

 private:
  Env* const env_ = Env::Default();
  const std::string root_;
};

std::string ResolveSysfsRoot(const CpuTopologyOptions& options) {
  if (!options.sysfs_root.empty()) {
    return options.sysfs_root;
  }
  if (!FLAGS_TEST_cpu_topology_sysfs_root.empty()) {
    return FLAGS_TEST_cpu_topology_sysfs_root;
  }
#ifdef __linux__
  return "/sys/devices/system/cpu";
#else
  return std::string();
#endif
}

// Turns each CPU's group into the list of distinct domains, restricted to the cpuset.
Result<std::vector<CpuSet>> MakeDomains(
    const std::vector<std::pair<size_t, CpuSet>>& cpu_groups, const CpuSet& cpuset) {
  std::vector<CpuSet> domains;
  for (const auto& [cpu, raw_group] : cpu_groups) {
    auto group = raw_group & cpuset;
    group.Set(cpu);
    bool seen = false;
    for (const auto& domain : domains) {
      if (domain == group) {
        seen = true;
        break;
      }
      if (!(domain & group).empty()) {
        return STATUS_FORMAT(
            IllegalState, "Overlapping CPU groups $0 and $1 (from CPU $2)", domain, group, cpu);
      }
    }
    if (!seen) {
      domains.push_back(group);
    }
  }
  std::sort(domains.begin(), domains.end(), [](const CpuSet& lhs, const CpuSet& rhs) {
    return *lhs.Lowest() < *rhs.Lowest();
  });
  return domains;
}

// Per-CPU groups from the highest cache level, or nullopt when there is no shared cache info.
Result<std::optional<std::vector<std::pair<size_t, CpuSet>>>> GroupByCache(
    const SysfsReader& reader, const CpuSet& cpuset, int64_t* level_out) {
  struct CacheInfo {
    int64_t level;
    CpuSet shared;
  };
  std::vector<std::pair<size_t, std::vector<CacheInfo>>> per_cpu;
  int64_t max_level = 0;
  for (auto cpu : cpuset.ToVector()) {
    auto& caches = per_cpu.emplace_back(cpu, std::vector<CacheInfo>()).second;
    for (const auto& dir : VERIFY_RESULT(reader.CacheIndexDirs(cpu))) {
      auto level = VERIFY_RESULT(reader.ReadInt(dir + "/level"));
      auto shared = VERIFY_RESULT(reader.ReadCpuList(dir + "/shared_cpu_list"));
      if (!level || !shared) {
        continue;
      }
      caches.push_back(CacheInfo{*level, *shared});
      max_level = std::max(max_level, *level);
    }
  }
  if (max_level == 0) {
    return std::nullopt;
  }

  std::vector<std::pair<size_t, CpuSet>> result;
  bool any_shared = false;
  for (const auto& [cpu, caches] : per_cpu) {
    CpuSet group;
    bool found = false;
    for (const auto& cache : caches) {
      if (cache.level == max_level) {
        group |= cache.shared;
        found = true;
      }
    }
    if (!found) {
      // CPUs disagree on the cache hierarchy; grouping by it would be guesswork.
      return std::nullopt;
    }
    group.Set(cpu);
    auto siblings = VERIFY_RESULT(reader.ReadCpuList(
        reader.CpuPath(cpu, "topology/thread_siblings_list"))).value_or(CpuSet());
    siblings.Set(cpu);
    if (group != siblings) {
      any_shared = true;
    }
    result.emplace_back(cpu, group);
  }
  // A highest level shared only by SMT siblings (some ARM servers expose no L3) would give one
  // domain per core; lane masks must be whole multi-core domains, so treat it as no cache info.
  if (!any_shared) {
    return std::nullopt;
  }
  *level_out = max_level;
  return result;
}

// Per-CPU groups by physical package, or nullopt when any CPU lacks package info.
Result<std::optional<std::vector<std::pair<size_t, CpuSet>>>> GroupByPackage(
    const SysfsReader& reader, const CpuSet& cpuset) {
  std::map<int64_t, CpuSet> by_package;
  std::vector<std::pair<size_t, int64_t>> cpu_package;
  for (auto cpu : cpuset.ToVector()) {
    auto id = VERIFY_RESULT(reader.ReadInt(reader.CpuPath(cpu, "topology/physical_package_id")));
    // The kernel reports -1 when the platform does not describe packages.
    if (!id || *id < 0) {
      return std::nullopt;
    }
    by_package[*id].Set(cpu);
    cpu_package.emplace_back(cpu, *id);
  }
  std::vector<std::pair<size_t, CpuSet>> result;
  for (const auto& [cpu, id] : cpu_package) {
    result.emplace_back(cpu, by_package[id]);
  }
  return result;
}

} // namespace

Result<CpuSet> CpuSet::Parse(std::string_view cpu_list) {
  auto input = Trim(cpu_list);
  CpuSet result;
  if (input.empty()) {
    return result;
  }
  size_t pos = 0;
  for (;;) {
    auto comma = input.find(',', pos);
    auto token = input.substr(pos, comma == std::string_view::npos ? comma : comma - pos);
    auto dash = token.find('-');
    auto first = VERIFY_RESULT(ParseCpuId(token.substr(0, dash), cpu_list));
    auto last = first;
    if (dash != std::string_view::npos) {
      last = VERIFY_RESULT(ParseCpuId(token.substr(dash + 1), cpu_list));
      if (last < first) {
        return STATUS_FORMAT(
            InvalidArgument, "Bad range '$0' in cpu list '$1'", token, cpu_list);
      }
    }
    for (auto cpu = first; cpu <= last; ++cpu) {
      result.Set(cpu);
    }
    if (comma == std::string_view::npos) {
      break;
    }
    pos = comma + 1;
  }
  return result;
}

CpuSet CpuSet::FirstN(size_t count) {
  CpuSet result;
  for (size_t cpu = 0; cpu != std::min(count, kMaxCpus); ++cpu) {
    result.Set(cpu);
  }
  return result;
}

#ifdef __linux__
static_assert(CpuSet::kMaxCpus >= CPU_SETSIZE);

CpuSet CpuSet::FromCpuSetT(const cpu_set_t& set) {
  CpuSet result;
  for (size_t cpu = 0; cpu != CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &set)) {
      result.Set(cpu);
    }
  }
  return result;
}

void CpuSet::ToCpuSetT(cpu_set_t* out) const {
  CPU_ZERO(out);
  ForEach([out](size_t cpu) {
    if (cpu < CPU_SETSIZE) {
      CPU_SET(cpu, out);
    }
  });
}
#endif

void CpuSet::Set(size_t cpu) {
  DCHECK_LT(cpu, kMaxCpus);
  words_[cpu / kBitsPerWord] |= uint64_t{1} << (cpu % kBitsPerWord);
}

bool CpuSet::Test(size_t cpu) const {
  return cpu < kMaxCpus && (words_[cpu / kBitsPerWord] >> (cpu % kBitsPerWord)) & 1;
}

size_t CpuSet::Count() const {
  size_t result = 0;
  for (auto word : words_) {
    result += std::popcount(word);
  }
  return result;
}

bool CpuSet::empty() const {
  return std::all_of(words_.begin(), words_.end(), [](uint64_t word) { return word == 0; });
}

std::optional<size_t> CpuSet::Lowest() const {
  for (size_t w = 0; w != kWords; ++w) {
    if (words_[w]) {
      return w * kBitsPerWord + std::countr_zero(words_[w]);
    }
  }
  return std::nullopt;
}

CpuSet& CpuSet::operator&=(const CpuSet& rhs) {
  for (size_t w = 0; w != kWords; ++w) {
    words_[w] &= rhs.words_[w];
  }
  return *this;
}

CpuSet& CpuSet::operator|=(const CpuSet& rhs) {
  for (size_t w = 0; w != kWords; ++w) {
    words_[w] |= rhs.words_[w];
  }
  return *this;
}

CpuSet& CpuSet::operator-=(const CpuSet& rhs) {
  for (size_t w = 0; w != kWords; ++w) {
    words_[w] &= ~rhs.words_[w];
  }
  return *this;
}

std::vector<size_t> CpuSet::ToVector() const {
  std::vector<size_t> result;
  result.reserve(Count());
  ForEach([&result](size_t cpu) { result.push_back(cpu); });
  return result;
}

std::string CpuSet::ToString() const {
  std::string result;
  auto cpus = ToVector();
  for (size_t i = 0; i != cpus.size();) {
    auto j = i;
    while (j + 1 != cpus.size() && cpus[j + 1] == cpus[j] + 1) {
      ++j;
    }
    if (!result.empty()) {
      result += ',';
    }
    result += std::to_string(cpus[i]);
    if (j != i) {
      result += '-';
      result += std::to_string(cpus[j]);
    }
    i = j + 1;
  }
  return result;
}

Result<CpuSet> GetProcessCpuSet(const CpuTopologyOptions& options) {
  if (options.cpuset) {
    return *options.cpuset;
  }
  if (!FLAGS_TEST_cpu_topology_cpuset.empty()) {
    return CpuSet::Parse(FLAGS_TEST_cpu_topology_cpuset);
  }
#ifdef __linux__
  // The calling thread's mask; topology is computed during startup, before any thread is pinned.
  cpu_set_t set;
  CPU_ZERO(&set);
  if (sched_getaffinity(0, sizeof(set), &set) != 0) {
    return STATUS_FROM_ERRNO("sched_getaffinity", errno);
  }
  return CpuSet::FromCpuSetT(set);
#else
  return CpuSet::FirstN(static_cast<size_t>(std::max(base::NumCPUs(), 1)));
#endif
}

Result<std::vector<CpuSet>> DiscoverCpuDomains(
    const CpuTopologyOptions& options, std::string* source) {
  auto cpuset = VERIFY_RESULT(GetProcessCpuSet(options));
  if (cpuset.empty()) {
    return STATUS(IllegalState, "Process cpuset is empty");
  }
  SysfsReader reader(ResolveSysfsRoot(options));
  std::string description;

  if (options.domain_kind == CpuDomainKind::kL3) {
    int64_t level = 0;
    auto groups = VERIFY_RESULT(GroupByCache(reader, cpuset, &level));
    if (groups) {
      if (source) {
        *source = Format("cache level $0", level);
      }
      return MakeDomains(*groups, cpuset);
    }
    description = "no shared cache info, ";
  }

  auto groups = VERIFY_RESULT(GroupByPackage(reader, cpuset));
  if (groups) {
    if (source) {
      *source = description + "physical_package_id";
    }
    return MakeDomains(*groups, cpuset);
  }

  if (source) {
    *source = description + "no package info, process cpuset";
  }
  return std::vector<CpuSet>{cpuset};
}

} // namespace yb
