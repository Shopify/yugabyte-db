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

#include <gtest/gtest.h>

#include "yb/util/cpu_topology.h"
#include "yb/util/cpu_topology_test_util.h"
#include "yb/util/env.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"

DECLARE_string(TEST_cpu_topology_cpuset);
DECLARE_string(TEST_cpu_topology_sysfs_root);

namespace yb {

class CpuTopologyTest : public YBTest {
 protected:
  // Writes the fake tree and points discovery at it with the given cpuset.
  void UseFakeTree(const std::vector<FakeCpu>& cpus, const std::string& cpuset) {
    auto root = GetTestPath("sys_cpu");
    ASSERT_OK(WriteFakeSysfsCpuTree(root, cpus));
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_sysfs_root) = root;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_cpuset) = cpuset;
  }

  Result<std::vector<std::string>> Discover(
      CpuDomainKind kind, std::string* source = nullptr) {
    auto domains = VERIFY_RESULT(DiscoverCpuDomains(CpuTopologyOptions{.domain_kind = kind},
                                                    source));
    std::vector<std::string> result;
    for (const auto& domain : domains) {
      result.push_back(domain.ToString());
    }
    return result;
  }
};

CpuSet ParseOrDie(std::string_view list) {
  auto result = CpuSet::Parse(list);
  CHECK(result.ok()) << result.status();
  return *result;
}

TEST_F(CpuTopologyTest, CpuListRoundTrip) {
  for (std::string list : {"", "0", "5", "0-3", "0-3,8,10-11", "0,2,4", "1023", "0-1023",
                           "1-2,4-5,7-8"}) {
    auto set = ASSERT_RESULT(CpuSet::Parse(list));
    ASSERT_EQ(set.ToString(), list);
  }
  ASSERT_EQ(ParseOrDie("\n 0-3\n").ToString(), "0-3");
  ASSERT_EQ(ParseOrDie("3,0-1,2").ToString(), "0-3");
  ASSERT_EQ(ParseOrDie("7-7").ToString(), "7");
  ASSERT_EQ(ParseOrDie("0-1023").Count(), 1024);
}

TEST_F(CpuTopologyTest, CpuListBadInput) {
  for (std::string list : {"a", "1-", "-1", "3-1", "1,,2", "1,", ",1", "1024", "0-1024",
                           "1 - 2", "1:2", "0-7:2/4", "+1", "99999", "0x1"}) {
    ASSERT_NOK(CpuSet::Parse(list)) << list;
  }
}

TEST_F(CpuTopologyTest, CpuSetOperations) {
  auto a = ParseOrDie("0-7,100");
  auto b = ParseOrDie("4-11");
  ASSERT_EQ(a.Count(), 9);
  ASSERT_EQ((a & b).ToString(), "4-7");
  ASSERT_EQ((a | b).ToString(), "0-11,100");
  ASSERT_EQ((a - b).ToString(), "0-3,100");
  ASSERT_EQ(*(b - a).Lowest(), 8);
  ASSERT_FALSE(CpuSet().Lowest());
  ASSERT_TRUE(CpuSet().empty());
  ASSERT_FALSE(a.empty());
  ASSERT_TRUE(a.Test(100));
  ASSERT_FALSE(a.Test(99));
  ASSERT_FALSE(a.Test(5000));
  std::vector<size_t> cpus;
  a.ForEach([&cpus](size_t cpu) { cpus.push_back(cpu); });
  ASSERT_EQ(cpus, (std::vector<size_t>{0, 1, 2, 3, 4, 5, 6, 7, 100}));
  ASSERT_EQ(cpus, a.ToVector());
  ASSERT_EQ(CpuSet::FirstN(4).ToString(), "0-3");
  ASSERT_EQ(CpuSet::FirstN(5000).Count(), CpuSet::kMaxCpus);

  // Out-of-range ids never touch memory past the set.
  auto before = a;
  ASSERT_DEBUG_DEATH(a.Set(CpuSet::kMaxCpus), "is not below");
  ASSERT_DEBUG_DEATH(a.Set(CpuSet::kMaxCpus * 64), "is not below");
  ASSERT_EQ(a, before);
#ifdef __linux__
  cpu_set_t native;
  a.ToCpuSetT(&native);
  ASSERT_EQ(CPU_COUNT(&native), 9);
  ASSERT_TRUE(CPU_ISSET(100, &native));
  ASSERT_EQ(CpuSet::FromCpuSetT(native), a);
#endif
}

// 2 sockets x 12 L3 x 8 cores x 2 SMT, siblings n and n + 192.
TEST_F(CpuTopologyTest, AmdLikeL3) {
  ASSERT_NO_FATALS(UseFakeTree(
      MakeFakeCpus({.sockets = 2, .l3_per_socket = 12, .cores_per_l3 = 8, .smt = 2}), "0-383"));
  std::string source;
  auto domains = ASSERT_RESULT(Discover(CpuDomainKind::kL3, &source));
  ASSERT_EQ(source, "cache level 3");
  ASSERT_EQ(domains.size(), 24);
  for (size_t i = 0; i != domains.size(); ++i) {
    ASSERT_EQ(domains[i], Format("$0-$1,$2-$3", 8 * i, 8 * i + 7, 192 + 8 * i, 192 + 8 * i + 7));
  }
}

TEST_F(CpuTopologyTest, AmdLikeSocket) {
  ASSERT_NO_FATALS(UseFakeTree(
      MakeFakeCpus({.sockets = 2, .l3_per_socket = 12, .cores_per_l3 = 8, .smt = 2}), "0-383"));
  std::string source;
  auto domains = ASSERT_RESULT(Discover(CpuDomainKind::kSocket, &source));
  ASSERT_EQ(source, "physical_package_id");
  ASSERT_EQ(domains, (std::vector<std::string>{"0-95,192-287", "96-191,288-383"}));
}

// One L3 per socket: both modes give one domain per socket.
TEST_F(CpuTopologyTest, IntelLike) {
  ASSERT_NO_FATALS(UseFakeTree(
      MakeFakeCpus({.sockets = 2, .l3_per_socket = 1, .cores_per_l3 = 4, .smt = 2}), "0-15"));
  const std::vector<std::string> expected = {"0-3,8-11", "4-7,12-15"};
  std::string source;
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kL3, &source)), expected);
  ASSERT_EQ(source, "cache level 3");
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kSocket, &source)), expected);
  ASSERT_EQ(source, "physical_package_id");
}

// Without index3 the highest level present (a shared L2 here) defines the domains.
TEST_F(CpuTopologyTest, NoL3UsesHighestLevel) {
  std::vector<FakeCpu> cpus;
  for (size_t cpu = 0; cpu != 8; ++cpu) {
    auto self = std::to_string(cpu);
    cpus.push_back(FakeCpu{
        .cpu = cpu,
        .package = 0,
        .thread_siblings = self,
        .caches = {{1, self}, {1, self}, {2, cpu < 4 ? "0-3" : "4-7"}},
    });
  }
  ASSERT_NO_FATALS(UseFakeTree(cpus, "0-7"));
  std::string source;
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kL3, &source)),
            (std::vector<std::string>{"0-3", "4-7"}));
  ASSERT_EQ(source, "cache level 2");
}

// A highest level private to each core would give single-core domains; fall back to sockets.
TEST_F(CpuTopologyTest, PerCoreHighestLevelUsesSocket) {
  ASSERT_NO_FATALS(UseFakeTree(
      MakeFakeCpus({.sockets = 2, .l3_per_socket = 1, .cores_per_l3 = 4, .smt = 2,
                    .with_l3 = false}),
      "0-15"));
  std::string source;
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kL3, &source)),
            (std::vector<std::string>{"0-3,8-11", "4-7,12-15"}));
  ASSERT_EQ(source, "no shared cache info, physical_package_id");
}

// Same, with no thread_siblings_list: a one-CPU cache group counts as per core.
TEST_F(CpuTopologyTest, PerCoreHighestLevelNoSiblingsUsesSocket) {
  std::vector<FakeCpu> cpus;
  for (size_t cpu = 0; cpu != 4; ++cpu) {
    cpus.push_back(FakeCpu{
        .cpu = cpu,
        .package = cpu < 2 ? 0 : 1,
        .caches = {{1, std::to_string(cpu)}, {2, std::to_string(cpu)}},
    });
  }
  ASSERT_NO_FATALS(UseFakeTree(cpus, "0-3"));
  std::string source;
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kL3, &source)),
            (std::vector<std::string>{"0-1", "2-3"}));
  ASSERT_EQ(source, "no shared cache info, physical_package_id");
}

TEST_F(CpuTopologyTest, NoCacheUsesSocket) {
  auto cpus = MakeFakeCpus({.sockets = 2, .l3_per_socket = 1, .cores_per_l3 = 2});
  for (auto& cpu : cpus) {
    cpu.caches.clear();
  }
  ASSERT_NO_FATALS(UseFakeTree(cpus, "0-3"));
  std::string source;
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kL3, &source)),
            (std::vector<std::string>{"0-1", "2-3"}));
  ASSERT_EQ(source, "no shared cache info, physical_package_id");
}

TEST_F(CpuTopologyTest, NoCacheNoPackageOneDomain) {
  auto cpus = MakeFakeCpus({.sockets = 2, .l3_per_socket = 1, .cores_per_l3 = 2});
  for (auto& cpu : cpus) {
    cpu.caches.clear();
    cpu.package.reset();
  }
  ASSERT_NO_FATALS(UseFakeTree(cpus, "0-3"));
  std::string source;
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kL3, &source)),
            (std::vector<std::string>{"0-3"}));
  ASSERT_EQ(source, "no shared cache info, no package info, process cpuset");
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kSocket, &source)),
            (std::vector<std::string>{"0-3"}));
  ASSERT_EQ(source, "no package info, process cpuset");
}

// A sysfs root that does not exist behaves like a host without topology info.
TEST_F(CpuTopologyTest, MissingSysfsRootOneDomain) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_sysfs_root) = GetTestPath("does_not_exist");
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_cpuset) = "0-3";
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kL3)), (std::vector<std::string>{"0-3"}));
}

// Domains are restricted to the cpuset: half of domain 0, none of domain 1, all of domain 2.
TEST_F(CpuTopologyTest, CpusetRestrictsDomains) {
  ASSERT_NO_FATALS(UseFakeTree(
      MakeFakeCpus({.sockets = 2, .l3_per_socket = 12, .cores_per_l3 = 8, .smt = 2}),
      "0-3,192-195,16-23,208-215"));
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kL3)),
            (std::vector<std::string>{"0-3,192-195", "16-23,208-215"}));
  ASSERT_EQ(ASSERT_RESULT(Discover(CpuDomainKind::kSocket)),
            (std::vector<std::string>{"0-3,16-23,192-195,208-215"}));

  // The options override takes precedence over the test flag.
  CpuTopologyOptions options{.cpuset = ParseOrDie("200-207")};
  auto domains = ASSERT_RESULT(DiscoverCpuDomains(options));
  ASSERT_EQ(domains, std::vector<CpuSet>{ParseOrDie("200-207")});
  ASSERT_EQ(ASSERT_RESULT(GetProcessCpuSet(options)), ParseOrDie("200-207"));
}

TEST_F(CpuTopologyTest, MalformedFilesAreErrors) {
  auto cpus = MakeFakeCpus({.sockets = 1, .l3_per_socket = 1, .cores_per_l3 = 2});
  cpus[1].caches[3].shared_cpu_list = "0-x";
  ASSERT_NO_FATALS(UseFakeTree(cpus, "0-1"));
  ASSERT_NOK(Discover(CpuDomainKind::kL3));

  auto root = GetTestPath("sys_cpu");
  ASSERT_OK(WriteStringToFile(Env::Default(), "three\n", root + "/cpu0/cache/index3/level"));
  ASSERT_NOK(Discover(CpuDomainKind::kL3));

  ASSERT_OK(WriteStringToFile(
      Env::Default(), "zero\n", root + "/cpu0/topology/physical_package_id"));
  ASSERT_NOK(Discover(CpuDomainKind::kSocket));

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_cpuset) = "0-";
  ASSERT_NOK(GetProcessCpuSet(CpuTopologyOptions()));
}

TEST_F(CpuTopologyTest, OverlappingGroupsAreErrors) {
  std::vector<FakeCpu> cpus;
  for (size_t cpu = 0; cpu != 3; ++cpu) {
    cpus.push_back(FakeCpu{
        .cpu = cpu, .package = 0, .caches = {{3, cpu == 0 ? "0-1" : "1-2"}}});
  }
  ASSERT_NO_FATALS(UseFakeTree(cpus, "0-2"));
  ASSERT_NOK(Discover(CpuDomainKind::kL3));
}

// The real host: whatever its topology, domains partition the process cpuset.
TEST_F(CpuTopologyTest, RealHostPartitionsCpuset) {
  for (auto kind : {CpuDomainKind::kL3, CpuDomainKind::kSocket}) {
    std::string source;
    auto domains = ASSERT_RESULT(DiscoverCpuDomains(CpuTopologyOptions{.domain_kind = kind},
                                                    &source));
    CpuSet all;
    size_t count = 0;
    for (const auto& domain : domains) {
      ASSERT_FALSE(domain.empty());
      count += domain.Count();
      all |= domain;
    }
    ASSERT_EQ(count, all.Count());
    ASSERT_EQ(all, ASSERT_RESULT(GetProcessCpuSet(CpuTopologyOptions())));
    LOG(INFO) << ToString(kind) << ": " << domains.size() << " domain(s) from " << source << ": "
              << AsString(domains);
  }
}

} // namespace yb
