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

#include "yb/util/mem_tracker.h"

#include <chrono>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "yb/gutil/strings/human_readable.h"

#include "yb/tserver/server_main_util.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/flags.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/metrics.h"
#include "yb/util/monotime.h"
#include "yb/util/random_util.h"
#include "yb/util/result.h"
#include "yb/util/size_literals.h"
#include "yb/util/status_format.h"
#include "yb/util/stopwatch.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/test_util.h"
#include "yb/util/tcmalloc_util.h"

DECLARE_bool(mem_tracker_include_pageheap_free_in_root_consumption);
DECLARE_double(mem_tracker_external_consumption_accuracy_percentage);
DECLARE_int32(memory_limit_soft_percentage);
DECLARE_int64(mem_tracker_lane_batch_bytes);
DECLARE_int64(mem_tracker_tcmalloc_gc_release_bytes);
DECLARE_int64(mem_tracker_update_consumption_interval_us);

using namespace std::literals;

METRIC_DEFINE_entity(mem_tracker_lane_test);

namespace yb {

using std::equal_to;
using std::hash;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unordered_map;
using std::vector;

TEST(MemTrackerTest, SingleTrackerNoLimit) {
  shared_ptr<MemTracker> t = MemTracker::CreateTracker("t");
  EXPECT_FALSE(t->has_limit());
  t->Consume(10);
  EXPECT_EQ(t->consumption(), 10);
  t->Consume(10);
  EXPECT_EQ(t->consumption(), 20);
  t->Release(15);
  EXPECT_EQ(t->consumption(), 5);
  EXPECT_FALSE(t->LimitExceeded());
  t->Release(5);
  EXPECT_EQ(t->consumption(), 0);
}

TEST(MemTrackerTest, SingleTrackerWithLimit) {
  shared_ptr<MemTracker> t = MemTracker::CreateTracker(11, "t");
  EXPECT_TRUE(t->has_limit());
  t->Consume(10);
  EXPECT_EQ(t->consumption(), 10);
  EXPECT_FALSE(t->LimitExceeded());
  t->Consume(10);
  EXPECT_EQ(t->consumption(), 20);
  EXPECT_TRUE(t->LimitExceeded());
  t->Release(15);
  EXPECT_EQ(t->consumption(), 5);
  EXPECT_FALSE(t->LimitExceeded());
  t->Release(5);
}

TEST(MemTrackerTest, TrackerHierarchy) {
  shared_ptr<MemTracker> p = MemTracker::CreateTracker(100, "p");
  shared_ptr<MemTracker> c1 = MemTracker::CreateTracker(80, "c1", p);
  shared_ptr<MemTracker> c2 = MemTracker::CreateTracker(50, "c2", p);

  // everything below limits
  c1->Consume(60);
  EXPECT_EQ(c1->consumption(), 60);
  EXPECT_FALSE(c1->LimitExceeded());
  EXPECT_FALSE(c1->AnyLimitExceeded());
  EXPECT_EQ(c2->consumption(), 0);
  EXPECT_FALSE(c2->LimitExceeded());
  EXPECT_FALSE(c2->AnyLimitExceeded());
  EXPECT_EQ(p->consumption(), 60);
  EXPECT_FALSE(p->LimitExceeded());
  EXPECT_FALSE(p->AnyLimitExceeded());

  // p goes over limit
  c2->Consume(50);
  EXPECT_EQ(c1->consumption(), 60);
  EXPECT_FALSE(c1->LimitExceeded());
  EXPECT_TRUE(c1->AnyLimitExceeded());
  EXPECT_EQ(c2->consumption(), 50);
  EXPECT_FALSE(c2->LimitExceeded());
  EXPECT_TRUE(c2->AnyLimitExceeded());
  EXPECT_EQ(p->consumption(), 110);
  EXPECT_TRUE(p->LimitExceeded());

  // c2 goes over limit, p drops below limit
  c1->Release(20);
  c2->Consume(10);
  EXPECT_EQ(c1->consumption(), 40);
  EXPECT_FALSE(c1->LimitExceeded());
  EXPECT_FALSE(c1->AnyLimitExceeded());
  EXPECT_EQ(c2->consumption(), 60);
  EXPECT_TRUE(c2->LimitExceeded());
  EXPECT_TRUE(c2->AnyLimitExceeded());
  EXPECT_EQ(p->consumption(), 100);
  EXPECT_FALSE(p->LimitExceeded());
  c1->Release(40);
  c2->Release(60);
}

namespace {

class GcTest : public GarbageCollector {
 public:
  static const int NUM_RELEASE_BYTES = 1;

  explicit GcTest(MemTracker* tracker) : tracker_(tracker) {}

  void CollectGarbage(size_t required) override { tracker_->Release(NUM_RELEASE_BYTES); }

 private:
  MemTracker* tracker_;
};

} // namespace

TEST(MemTrackerTest, GcFunctions) {
  shared_ptr<MemTracker> t = MemTracker::CreateTracker(10, "");
  ASSERT_TRUE(t->has_limit());

  t->Consume(9);
  EXPECT_FALSE(t->LimitExceeded());

  // Test TryConsume()
  EXPECT_FALSE(t->TryConsume(2));
  EXPECT_EQ(t->consumption(), 9);
  EXPECT_FALSE(t->LimitExceeded());

  // Attach GcFunction that releases 1 byte
  auto gc = std::make_shared<GcTest>(t.get());
  t->AddGarbageCollector(gc);
  EXPECT_TRUE(t->TryConsume(2));
  EXPECT_EQ(t->consumption(), 10);
  EXPECT_FALSE(t->LimitExceeded());

  // GcFunction will be called even though TryConsume() fails
  EXPECT_FALSE(t->TryConsume(2));
  EXPECT_EQ(t->consumption(), 9);
  EXPECT_FALSE(t->LimitExceeded());

  // GcFunction won't be called
  EXPECT_TRUE(t->TryConsume(1));
  EXPECT_EQ(t->consumption(), 10);
  EXPECT_FALSE(t->LimitExceeded());

  // Test LimitExceeded()
  t->Consume(1);
  EXPECT_EQ(t->consumption(), 11);
  EXPECT_FALSE(t->LimitExceeded());
  EXPECT_EQ(t->consumption(), 10);

  // Add more GcFunctions, test that we only call them until the limit is no longer
  // exceeded
  auto gc2 = std::make_shared<GcTest>(t.get());
  t->AddGarbageCollector(gc2);
  auto gc3 = std::make_shared<GcTest>(t.get());
  t->AddGarbageCollector(gc3);
  t->Consume(1);
  EXPECT_EQ(t->consumption(), 11);
  EXPECT_FALSE(t->LimitExceeded());
  EXPECT_EQ(t->consumption(), 10);
  t->Release(10);
}

TEST(MemTrackerTest, STLContainerAllocator) {
  shared_ptr<MemTracker> t = MemTracker::CreateTracker("t");
  MemTrackerAllocator<int> vec_alloc(t);
  MemTrackerAllocator<pair<const int, int>> map_alloc(t);

  // Simple test: use the allocator in a vector.
  {
    vector<int, MemTrackerAllocator<int> > v(vec_alloc);
    ASSERT_EQ(0, t->consumption());
    v.reserve(5);
    ASSERT_EQ(5 * sizeof(int), t->consumption());
    v.reserve(10);
    ASSERT_EQ(10 * sizeof(int), t->consumption());
  }
  ASSERT_EQ(0, t->consumption());

  // Complex test: use it in an unordered_map, where it must be rebound in
  // order to allocate the map's buckets.
  {
    unordered_map<int, int, hash<int>, equal_to<int>, MemTrackerAllocator<pair<const int, int>>> um(
        10,
        hash<int>(),
        equal_to<int>(),
        map_alloc);

    // Don't care about the value (it depends on map internals).
    ASSERT_GT(t->consumption(), 0);
  }
  ASSERT_EQ(0, t->consumption());
}

TEST(MemTrackerTest, FindFunctionsTakeOwnership) {
  // In each test, ToString() would crash if the MemTracker is destroyed when
  // 'm' goes out of scope.

  shared_ptr<MemTracker> ref;
  {
    shared_ptr<MemTracker> m = MemTracker::CreateTracker("test");
    ref = MemTracker::FindTracker(m->id());
    ASSERT_TRUE(ref != nullptr);
  }
  LOG(INFO) << ref->ToString();
  ref.reset();

  {
    shared_ptr<MemTracker> m = MemTracker::CreateTracker("test");
    ref = MemTracker::FindOrCreateTracker(m->id());
  }
  LOG(INFO) << ref->ToString();
  ref.reset();

  vector<shared_ptr<MemTracker> > refs;
  {
    shared_ptr<MemTracker> m = MemTracker::CreateTracker("test");
    refs = MemTracker::ListTrackers();
  }
  for (const shared_ptr<MemTracker>& r : refs) {
    LOG(INFO) << r->ToString();
  }
  refs.clear();
}

TEST(MemTrackerTest, ScopedTrackedConsumption) {
  shared_ptr<MemTracker> m = MemTracker::CreateTracker("test");
  ASSERT_EQ(0, m->consumption());
  {
    ScopedTrackedConsumption consumption(m, 1);
    ASSERT_EQ(1, m->consumption());

    consumption.Reset(3);
    ASSERT_EQ(3, m->consumption());
  }
  ASSERT_EQ(0, m->consumption());
}

TEST(MemTrackerTest, SoftLimitExceeded) {
  const int kNumIters = 100000;
  const int kMemLimit = 1000;
  google::FlagSaver saver;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_memory_limit_soft_percentage) = 0;
  shared_ptr<MemTracker> m = MemTracker::CreateTracker(kMemLimit, "test");

  // Consumption is 0; the soft limit is never exceeded.
  for (int i = 0; i < kNumIters; i++) {
    ASSERT_FALSE(m->SoftLimitExceeded(0.0 /* score */).exceeded);
  }

  // Consumption is half of the actual limit, so we expect to exceed the soft
  // limit roughly half the time.
  ScopedTrackedConsumption consumption(m, kMemLimit / 2);
  int exceeded_count = 0;
  for (int i = 0; i < kNumIters; i++) {
    auto soft_limit_exceeded_result = m->SoftLimitExceeded(0.0 /* score */);
    if (soft_limit_exceeded_result.exceeded) {
      exceeded_count++;
      ASSERT_NEAR(50, soft_limit_exceeded_result.current_capacity_pct, 0.1);
    }
  }
  double exceeded_pct = static_cast<double>(exceeded_count) / kNumIters * 100;
  ASSERT_TRUE(exceeded_pct > 47 && exceeded_pct < 52);

  // Consumption is over the limit; the soft limit is always exceeded.
  consumption.Reset(kMemLimit + 1);
  for (int i = 0; i < kNumIters; i++) {
    auto soft_limit_exceeded_result = m->SoftLimitExceeded(0.0 /* score */);
    ASSERT_TRUE(soft_limit_exceeded_result.exceeded);
    ASSERT_NEAR(100, soft_limit_exceeded_result.current_capacity_pct, 0.1);
  }
}

#if YB_TCMALLOC_ENABLED
TEST(MemTrackerTest, TcMallocRootTracker) {
  MemTracker::TEST_SetReleasedMemorySinceGC(0);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_update_consumption_interval_us) = 100000;
  const auto kWaitTimeout = std::chrono::microseconds(
      FLAGS_mem_tracker_update_consumption_interval_us * 2);
  shared_ptr<MemTracker> root = MemTracker::GetRootTracker();

  // The root tracker's consumption and tcmalloc should agree.
  // Sleep to be sure that MaybeUpdateConsumption will take action.
  int64_t value = 0;
  ASSERT_OK(WaitFor([root, &value] {
    value = GetTCMallocActualHeapSizeBytes();
    return root->GetUpdatedConsumption() == value;
  }, kWaitTimeout, "Consumption actualized"));

  // Explicit Consume() and Release() should have effect.
  root->Consume(100);
  ASSERT_EQ(value + 100, root->consumption());
  root->Release(3);
  ASSERT_EQ(value + 100 - 3, root->consumption());

  const int64_t alloc_size = 4_MB;
  {
    // If we allocate something really big, we should see a change.
    std::unique_ptr<char[]> big_alloc(new char[alloc_size]);
    // clang in release mode can optimize out the above allocation unless
    // we do something with the pointer... so we just log it.
    VLOG(8) << static_cast<void*>(big_alloc.get());
    ASSERT_GE(root->GetUpdatedConsumption(true /* force */), value + alloc_size);
  }

  // The freed memory should go to the pageheap free bytes.
  ASSERT_GE(GetTCMallocPageHeapFreeBytes(), alloc_size);

  if (FLAGS_mem_tracker_include_pageheap_free_in_root_consumption) {
    // If we are including pageheap free size, consumption should stay the same.
    ASSERT_EQ(root->GetUpdatedConsumption(true /* force */), value + alloc_size);
  } else {
    // Consumption should decrease to near the original after the deallocation when
    // pageheap_free_bytes is not counted towards root consumption (the new default mode after
    // D24883).
    ASSERT_EQ(root->GetUpdatedConsumption(true /* force */), value);
  }
}

TEST(MemTrackerTest, TcMallocGC) {
  MemTracker::TEST_SetReleasedMemorySinceGC(0);
  shared_ptr<MemTracker> root = MemTracker::GetRootTracker();
  // Set a low GC threshold, so we can manage it easily in the test.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_tcmalloc_gc_release_bytes) = 1_MB;
  // Allocate something bigger than the threshold.
  std::unique_ptr<char[]> big_alloc(new char[4_MB]);
  // clang in release mode can optimize out the above allocation unless
  // we do something with the pointer... so we just log it.
  VLOG(8) << static_cast<void*>(big_alloc.get());
  // Check overhead at start of the test.
  auto overhead_before = GetTCMallocPageHeapFreeBytes();
  LOG(INFO) << "Initial overhead " << overhead_before;
  // Clear the memory, so tcmalloc gets free bytes.
  big_alloc.reset();
  // Check the overhead afterwards, should clearly be higher.
  auto overhead_after = GetTCMallocPageHeapFreeBytes();
  LOG(INFO) << "Post-free overhead " << overhead_after;
  ASSERT_GT(overhead_after, overhead_before);
  // Release up to the threshold. We only GC after we cross it, so nothing should happen now.
  root->Release(1_MB);
  ASSERT_EQ(overhead_after, GetTCMallocPageHeapFreeBytes());
  // Now we go over the limit and trigger a GC.
  root->Release(1);
  auto overhead_final = GetTCMallocPageHeapFreeBytes();
  LOG(INFO) << "Final overhead " << overhead_final;
  ASSERT_GT(overhead_after, overhead_final);
}

namespace {

Status CheckRootConsumptionAccuracy(bool dump_values = false) {
  auto root = MemTracker::GetRootTracker();
  const auto root_consumption = root->consumption();
  const auto real_consumption = GetTCMallocActualHeapSizeBytes();
  const auto delta = root_consumption - real_consumption;
  const auto allowed_delta =
      root->soft_limit() * FLAGS_mem_tracker_external_consumption_accuracy_percentage / 100;
  if (dump_values) {
    LOG(INFO) << "Root mem tracker consumption: "
              << HumanReadableNumBytes::ToString(root_consumption);
    LOG(INFO) << "Real consumption: " << HumanReadableNumBytes::ToString(real_consumption);
    LOG(INFO) << "Delta: " << HumanReadableNumBytes::ToString(delta);
    LOG(INFO) << "Allowed delta: " << HumanReadableNumBytes::ToString(allowed_delta);
  }
  if (abs(delta) <= allowed_delta) {
    return Status::OK();
  }
  return STATUS_FORMAT(
      InternalError,
      "Root mem tracker consumption bias is too much, tracker consumption: $0, real consumption: "
      "$1, delta: $2, allowed delta: $3",
      root_consumption, real_consumption, delta, allowed_delta);
}

} // namespace

// Tests that root memtracker consumption accuracy is within
// mem_tracker_external_consumption_accuracy_percentage comparing to external source
// (TCMalloc stats).
TEST(MemTrackerTest, RootConsumptionAccuracy) {
  constexpr auto kAllocationSize = 1_MB;

  MemTracker::TEST_SetReleasedMemorySinceGC(0);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_update_consumption_interval_us) =
      MonoDelta(3600s).ToMicroseconds();

  auto root = MemTracker::GetRootTracker();
  auto child = MemTracker::CreateTracker("child", root);

  ASSERT_OK(CheckRootConsumptionAccuracy());

  const size_t kMaxNumAllocations = root->soft_limit() / kAllocationSize / 10;
  EXPECT_GE(kMaxNumAllocations, 1);

  for (size_t i = 0; i < kMaxNumAllocations; ++i) {
    if (child->TryConsume(kAllocationSize)) {
      ASSERT_OK(CheckRootConsumptionAccuracy());
      child->Release(kAllocationSize);
      ASSERT_OK(CheckRootConsumptionAccuracy());
    }
  }

  // Make sure after both TryConsume and Consume, root memtracker still within required accuracy
  // comparing to TCMalloc stats during mem_tracker_update_consumption_interval_us period.
  for (bool use_try_consume : {false, true}) {
    for (auto num_allocations : std::initializer_list<size_t>{ 1, 5, 10, kMaxNumAllocations }) {
      LOG(INFO) << "use_try_consume: " << use_try_consume
                << ", num_allocations: " << num_allocations
                << ", child consumption: " << child->consumption();
      std::vector<std::pair<std::unique_ptr<uint8_t[]>, ScopedTrackedConsumption>> allocations;
      for (size_t i = 0; i < num_allocations; ++i) {
        if (!use_try_consume || child->TryConsume(kAllocationSize)) {
          ASSERT_OK(CheckRootConsumptionAccuracy());
          allocations.emplace_back(
              new uint8_t[kAllocationSize],
              ScopedTrackedConsumption(child, kAllocationSize, AlreadyConsumed(use_try_consume)));
          auto status = CheckRootConsumptionAccuracy();
          ASSERT_TRUE(status.ok()) << "Failed at iteration: " << i
                                   << " allocated: " << kAllocationSize * i << ": " << status;
        }
      }
      ASSERT_OK(CheckRootConsumptionAccuracy(true));
      // This will update consumption based on TCMalloc stats ignoring all flags.
      root->MaybeUpdateConsumption(/* force = */ true);
      ASSERT_OK(CheckRootConsumptionAccuracy(true));
      // This will release memory from both TCMalloc and memtracker.
      allocations.clear();
      ASSERT_OK(CheckRootConsumptionAccuracy(true));
    }
  }
}

#endif

TEST(MemTrackerTest, UnregisterFromParent) {
  shared_ptr<MemTracker> p = MemTracker::CreateTracker("parent");
  shared_ptr<MemTracker> c = MemTracker::CreateTracker("child", p);

  // Three trackers: root, parent, and child.
  auto all = MemTracker::ListTrackers();
  ASSERT_EQ(3, all.size()) << "All: " << yb::ToString(all);

  c.reset();
  all.clear();

  // Now only two because the child cannot be found from the root, though it is
  // still alive.
  all = MemTracker::ListTrackers();
  ASSERT_EQ(2, all.size());
  ASSERT_TRUE(MemTracker::FindTracker("child", p) == nullptr);

  // We can also recreate the child with the same name without colliding
  // with the old one.
  shared_ptr<MemTracker> c2 = MemTracker::CreateTracker("child", p);
}

// Disabled by default since it only measures overhead but does not test anything.
TEST(MemTrackerTest, DISABLED_Overhead) {
  const size_t kAllocationSize = 100_KB;
  const auto kNumCycles = 3;
  const size_t kNumIters = 100000000;
  const size_t kNumIters2 = kNumIters / 10;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_update_consumption_interval_us) =
      MonoDelta(3600s).ToMicroseconds();

  auto root = MemTracker::GetRootTracker();
  auto child = MemTracker::CreateTracker("child", root);

  auto old_now = CoarseMonoClock::now();

  for (auto k = 0; k < kNumCycles; ++k) {
    const auto num_iters = kNumIters;
    LOG(INFO) << "Doing " << num_iters << " calls of new[] + delete[]";

    Stopwatch s(Stopwatch::ALL_THREADS);
    s.start();

    for (size_t i = 0; i < num_iters; ++i) {
      auto* c = new char[100_KB];
      if (c) {
        delete[] c;
      }
    }

    s.stop();
    LOG(INFO) << "Elapsed: " << AsString(s.elapsed());
    LOG(INFO) << "Nanos per iteration: " << s.elapsed().wall_millis() * 1000000 / num_iters;
  }
  LOG(INFO) << "";

  for (auto k = 0; k < kNumCycles; ++k) {
    const auto num_iters = kNumIters;
    LOG(INFO) << "Doing " << num_iters << " calls of CoarseMonoClock::now()";

    Stopwatch s(Stopwatch::ALL_THREADS);
    s.start();

    for (size_t i = 0; i < num_iters; ++i) {
      ASSERT_GE(CoarseMonoClock::now(), old_now);
    }

    s.stop();
    LOG(INFO) << "Elapsed: " << AsString(s.elapsed());
    LOG(INFO) << "Nanos per iteration: " << s.elapsed().wall_millis() * 1000000 / num_iters;
  }
  LOG(INFO) << "";

  for (auto k = 0; k < kNumCycles; ++k) {
    const auto num_iters = kNumIters;
    LOG(INFO) << "Doing " << num_iters << " calls of: CoarseMonoClock::now() + new[] + delete[]";

    Stopwatch s(Stopwatch::ALL_THREADS);
    s.start();

    for (size_t i = 0; i < num_iters; ++i) {
      auto now = CoarseMonoClock::now();
      ASSERT_GE(now, old_now);
      auto* c = new char[100_KB];
      if (c) {
        delete[] c;
      }
    }

    s.stop();
    LOG(INFO) << "Elapsed: " << AsString(s.elapsed());
    LOG(INFO) << "Nanos per iteration: " << s.elapsed().wall_millis() * 1000000 / num_iters;
  }
  LOG(INFO) << "";

  for (auto k = 0; k < kNumCycles; ++k) {
    const auto num_iters = kNumIters2;
    LOG(INFO) << "Doing " << num_iters
              << " calls of: new[] + Consume + delete[] + Release";

    Stopwatch s(Stopwatch::ALL_THREADS);
    s.start();

    for (size_t i = 0; i < num_iters; ++i) {
      auto* c = new char[kAllocationSize];
      if (c) {
        child->Consume(kAllocationSize);
        delete[] c;
        child->Release(kAllocationSize);
      }
    }

    s.stop();
    LOG(INFO) << "Elapsed: " << AsString(s.elapsed());
    LOG(INFO) << "Nanos per iteration: " << s.elapsed().wall_millis() * 1000000 / num_iters;
  }
}

TEST(MemTrackerTest, ValidateMemoryLimitToRamRatio_ValidValues) {
  EXPECT_TRUE(internal::ValidateMemoryLimitToRamRatio("test_flag", 0.1));
  EXPECT_TRUE(internal::ValidateMemoryLimitToRamRatio("test_flag", 0.5));
  EXPECT_TRUE(internal::ValidateMemoryLimitToRamRatio("test_flag", 1.0));
  EXPECT_TRUE(internal::ValidateMemoryLimitToRamRatio("test_flag", USE_RECOMMENDED_MEMORY_VALUE));
}

TEST(MemTrackerTest, ValidateMemoryLimitToRamRatio_InvalidValues) {
  EXPECT_FALSE(internal::ValidateMemoryLimitToRamRatio("test_flag", 0.0));  // Boundary: not > 0.
  EXPECT_FALSE(internal::ValidateMemoryLimitToRamRatio("test_flag", -0.1)); // Negative.
  EXPECT_FALSE(internal::ValidateMemoryLimitToRamRatio("test_flag", 1.1));  // Above 1.
  EXPECT_FALSE(internal::ValidateMemoryLimitToRamRatio("test_flag", 2.0));  // Way above.
}

TEST(MemTrackerTest, ValidateMemoryLimitToRamRatio_EdgeCases) {
  // Test near boundaries.
  EXPECT_TRUE(internal::ValidateMemoryLimitToRamRatio("test_flag", 0.0001));  // Just above 0.
  EXPECT_FALSE(internal::ValidateMemoryLimitToRamRatio("test_flag", 1.0001)); // Just above 1.
}

// Lane batching. These tests build their own tracker trees under trackers that are not the root,
// except where they say they touch the root, which is process-wide and tcmalloc-backed.
class MemTrackerLaneTest : public YBTest {
 protected:
  static constexpr int64_t kBatch = 1000;

  void SetUp() override {
    YBTest::SetUp();
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_lane_batch_bytes) = kBatch;
  }

  // num_lanes lanes of one fake CPU each, without affinity: only ThisThreadLane matters here.
  static LaneTopology TestLanes(size_t num_lanes) {
    std::vector<CpuSet> domains(num_lanes);
    for (size_t i = 0; i != num_lanes; ++i) {
      domains[i].Set(i);
    }
    return LaneTopology::Build(LaneTopologyOptions{
        .domains = domains,
        .process_cpuset = CpuSet::FirstN(num_lanes),
        .requested_lanes = num_lanes,
        .affinity_enabled = false,
    });
  }

  static void RunInLane(LaneId lane, const std::function<void()>& f) {
    RunOnStdThread([lane, &f] {
      SetThisThreadLane(lane);
      f();
    });
  }
};

// With lanes off nothing is batched, and a thread with a lane updates the shared value per call.
// With lanes on, a batch size of 0 disables batching, and an unlaned thread updates the shared
// value per call of a batched tracker.
TEST_F(MemTrackerLaneTest, OffOrUnlanedIsUnbatched) {
  auto parent = MemTracker::CreateTracker("lane-off-parent");
  auto child = MemTracker::CreateTracker("lane-off-child", parent);
  ASSERT_FALSE(parent->EnableLaneBatching(MemTrackerLaneBatching::kSubtree));
  ASSERT_FALSE(parent->lane_batching_enabled());
  ASSERT_FALSE(child->lane_batching_enabled());
  RunInLane(0, [&] {
    for (int i = 1; i <= 3; ++i) {
      child->Consume(100);
      ASSERT_EQ(child->consumption(), 100 * i);
      ASSERT_EQ(parent->consumption(), 100 * i);
    }
    ASSERT_TRUE(child->TryConsume(100));
    ASSERT_EQ(parent->consumption(), 400);
    child->Release(400);
    ASSERT_EQ(parent->consumption(), 0);
  });

  TEST_SetLaneTopology scope(TestLanes(2));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_lane_batch_bytes) = 0;
  ASSERT_FALSE(parent->EnableLaneBatching());
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_lane_batch_bytes) = kBatch;
  ASSERT_TRUE(parent->EnableLaneBatching(MemTrackerLaneBatching::kSubtree));
  ASSERT_TRUE(child->lane_batching_enabled());
  for (int i = 1; i <= 3; ++i) {
    child->Consume(100);
    ASSERT_EQ(child->consumption(), 100 * i);
    ASSERT_EQ(parent->consumption(), 100 * i);
  }
  child->Release(300);
  ASSERT_EQ(parent->consumption(), 0);
  ASSERT_EQ(parent->TEST_UnflushedLaneBatches(), 0);
}

// Scopes: kSubtree batches existing and later descendants, kChildren only the children, kTracker
// only the tracker.
TEST_F(MemTrackerLaneTest, Scopes) {
  TEST_SetLaneTopology scope(TestLanes(2));
  auto subtree = MemTracker::CreateTracker("lane-subtree");
  auto existing = MemTracker::CreateTracker("existing", subtree);
  ASSERT_TRUE(subtree->EnableLaneBatching(MemTrackerLaneBatching::kSubtree));
  auto later = MemTracker::CreateTracker("later", existing);
  auto found = MemTracker::FindOrCreateTracker("found", later);
  for (const auto& tracker : {subtree, existing, later, found}) {
    ASSERT_TRUE(tracker->lane_batching_enabled()) << tracker->ToString();
  }

  auto children = MemTracker::CreateTracker("lane-children");
  auto child = MemTracker::CreateTracker("child", children);
  ASSERT_TRUE(children->EnableLaneBatching(MemTrackerLaneBatching::kChildren));
  auto later_child = MemTracker::FindOrCreateTracker("later-child", children);
  auto grandchild = MemTracker::CreateTracker("grandchild", child);
  ASSERT_TRUE(child->lane_batching_enabled());
  ASSERT_TRUE(later_child->lane_batching_enabled());
  ASSERT_FALSE(grandchild->lane_batching_enabled());

  auto single = MemTracker::CreateTracker("lane-single");
  ASSERT_TRUE(single->EnableLaneBatching());
  ASSERT_FALSE(MemTracker::CreateTracker("child", single)->lane_batching_enabled());
}

// A lane thread's deltas stay in its lane slot until the slot exceeds the batch, then move to the
// shared value; a delta larger than the batch goes to the shared value directly.
TEST_F(MemTrackerLaneTest, AbsorbsAndFlushes) {
  TEST_SetLaneTopology scope(TestLanes(2));
  auto parent = MemTracker::CreateTracker("lane-flush-parent");
  auto child = MemTracker::CreateTracker("lane-flush-child", parent);
  ASSERT_TRUE(parent->EnableLaneBatching(MemTrackerLaneBatching::kSubtree));
  RunInLane(0, [&] {
    child->Consume(600);
    child->Consume(kBatch - 600);
    for (const auto& tracker : {child, parent}) {
      ASSERT_EQ(tracker->consumption(), 0);
      ASSERT_EQ(tracker->TEST_UnflushedLaneBatches(), kBatch);
    }
    child->Consume(1);
    for (const auto& tracker : {child, parent}) {
      ASSERT_EQ(tracker->consumption(), kBatch + 1);
      ASSERT_EQ(tracker->TEST_UnflushedLaneBatches(), 0);
    }
  });
  RunInLane(1, [&] {
    child->Release(500);
    ASSERT_EQ(child->consumption(), kBatch + 1);
    ASSERT_EQ(child->TEST_UnflushedLaneBatches(), -500);
    child->Consume(5 * kBatch);
    ASSERT_EQ(child->consumption(), 6 * kBatch + 1);
    ASSERT_EQ(child->TEST_UnflushedLaneBatches(), -500);
  });
  for (const auto& tracker : {child, parent}) {
    tracker->FlushLaneBatches();
    ASSERT_EQ(tracker->consumption(), 6 * kBatch + 1 - 500);
    ASSERT_EQ(tracker->TEST_UnflushedLaneBatches(), 0);
  }
  child->Release(child->consumption());
}

// Consume and Release from several lanes at once: the shared value stays within L x batch of the
// tracked total and equals it after FlushLaneBatches.
TEST_F(MemTrackerLaneTest, MultiLaneConvergence) {
  constexpr size_t kLanes = 4;
  TEST_SetLaneTopology scope(TestLanes(kLanes));
  auto parent = MemTracker::CreateTracker("lane-converge-parent");
  auto child = MemTracker::CreateTracker("lane-converge-child", parent);
  ASSERT_TRUE(parent->EnableLaneBatching(MemTrackerLaneBatching::kSubtree));
  std::atomic<int64_t> total{0};
  TestThreadHolder threads;
  for (LaneId lane = 0; lane != kLanes; ++lane) {
    threads.AddThreadFunctor([&, lane] {
      SetThisThreadLane(lane);
      int64_t mine = 0;
      for (int i = 0; i != 20000; ++i) {
        auto bytes = RandomUniformInt<int64_t>(1, 300);
        if (mine >= bytes && RandomUniformBool()) {
          child->Release(bytes);
          mine -= bytes;
        } else {
          child->Consume(bytes);
          mine += bytes;
        }
      }
      total += mine;
    });
  }
  threads.JoinAll();
  for (const auto& tracker : {child, parent}) {
    ASSERT_LE(std::abs(tracker->consumption() - total.load()), kLanes * kBatch);
    ASSERT_EQ(tracker->consumption() + tracker->TEST_UnflushedLaneBatches(), total.load());
    tracker->FlushLaneBatches();
    ASSERT_EQ(tracker->consumption(), total.load());
  }
  child->Release(total.load());
}

// TryConsume on a batched limited tracker fails no later than limit + L x batch, from lanes taking
// turns and from lanes at once, and its rollback of an ancestor's batch is exact.
TEST_F(MemTrackerLaneTest, TryConsumeBound) {
  constexpr size_t kLanes = 4;
  constexpr int64_t kLimit = 10 * kBatch;
  constexpr int64_t kBytes = 100;
  TEST_SetLaneTopology scope(TestLanes(kLanes));
  auto limited = MemTracker::CreateTracker(kLimit, "lane-try-limited");
  ASSERT_TRUE(limited->EnableLaneBatching());

  // Lanes taking turns on one thread, each until its first failure.
  int64_t admitted = 0;
  RunOnStdThread([&] {
    std::vector<bool> failed(kLanes);
    for (size_t active = kLanes; active;) {
      for (LaneId lane = 0; lane != kLanes; ++lane) {
        if (failed[lane]) {
          continue;
        }
        SetThisThreadLane(lane);
        if (limited->TryConsume(kBytes)) {
          admitted += kBytes;
        } else {
          failed[lane] = true;
          --active;
        }
      }
    }
    // At the limit, a lane with an empty batch is refused too: it checks the shared value before
    // admitting into its batch.
    for (LaneId lane = 0; lane != kLanes; ++lane) {
      SetThisThreadLane(lane);
      ASSERT_FALSE(limited->TryConsume(kBytes));
    }
  });
  // The other lanes' batches let the total pass the limit, but by no more than L x batch.
  ASSERT_LE(admitted, kLimit + static_cast<int64_t>(kLanes) * kBatch);
  ASSERT_GT(admitted, kLimit);
  limited->FlushLaneBatches();
  ASSERT_EQ(limited->consumption(), admitted);
  limited->Release(admitted);

  // Lanes at once: calls racing with one check can each add kBytes.
  std::atomic<int64_t> concurrent{0};
  TestThreadHolder threads;
  for (LaneId lane = 0; lane != kLanes; ++lane) {
    threads.AddThreadFunctor([&, lane] {
      SetThisThreadLane(lane);
      while (limited->TryConsume(kBytes)) {
        concurrent += kBytes;
      }
    });
  }
  threads.JoinAll();
  ASSERT_LE(concurrent.load(), kLimit + static_cast<int64_t>(kLanes) * (kBatch + kBytes));
  ASSERT_GT(concurrent.load(), kLimit - kBytes);
  limited->FlushLaneBatches();
  ASSERT_EQ(limited->consumption(), concurrent.load());
  limited->Release(concurrent.load());

  // A child's own limit fails after the batched ancestor admitted the bytes into its slot.
  auto child = MemTracker::CreateTracker(50, "lane-try-child", limited);
  RunInLane(0, [&] {
    MemTracker* blocking = nullptr;
    ASSERT_FALSE(child->TryConsume(kBytes, &blocking));
    ASSERT_EQ(blocking, child.get());
  });
  limited->FlushLaneBatches();
  ASSERT_EQ(limited->consumption(), 0);
  ASSERT_EQ(child->consumption(), 0);
}

// A destroyed tracker releases its unflushed slots to its ancestors too.
TEST_F(MemTrackerLaneTest, DestructionReleasesUnflushed) {
  TEST_SetLaneTopology scope(TestLanes(2));
  auto parent = MemTracker::CreateTracker("lane-destroy-parent");
  auto child = MemTracker::CreateTracker("lane-destroy-child", parent);
  ASSERT_TRUE(parent->EnableLaneBatching(MemTrackerLaneBatching::kSubtree));
  RunInLane(0, [&] {
    child->Consume(500);
  });
  ASSERT_EQ(child->TEST_UnflushedLaneBatches(), 500);
  child->UnregisterFromParent();
  child.reset();
  parent->FlushLaneBatches();
  ASSERT_EQ(parent->consumption(), 0);
}

// When lane mode turns off (tests only), unflushed slots stay until FlushLaneBatches, and every
// thread updates the shared value directly.
TEST_F(MemTrackerLaneTest, LaneModeOffKeepsSlotsUntilFlushed) {
  auto tracker = MemTracker::CreateTracker("lane-mode-off");
  {
    TEST_SetLaneTopology scope(TestLanes(2));
    ASSERT_TRUE(tracker->EnableLaneBatching());
    RunInLane(1, [&] {
      tracker->Consume(500);
    });
  }
  ASSERT_FALSE(LaneModeActive());
  RunInLane(1, [&] {
    tracker->Consume(100);
  });
  ASSERT_EQ(tracker->consumption(), 100);
  ASSERT_EQ(tracker->TEST_UnflushedLaneBatches(), 500);
  tracker->FlushLaneBatches();
  ASSERT_EQ(tracker->consumption(), 600);
  tracker->Release(600);
}

// The tracker's metric moves with the shared value: not per call, but when a slot is flushed.
TEST_F(MemTrackerLaneTest, MetricFlushedWithSlot) {
  TEST_SetLaneTopology scope(TestLanes(2));
  MetricRegistry registry;
  auto entity = METRIC_ENTITY_mem_tracker_lane_test.Instantiate(&registry, "lane-metric");
  auto tracker = MemTracker::CreateTracker("lane-metric");
  tracker->SetMetricEntity(entity);
  ASSERT_TRUE(tracker->EnableLaneBatching());
  auto gauge_value = [&]() -> Result<int64_t> {
    for (const auto& [prototype, metric] : entity->TEST_UsageMetricsMap()) {
      if (prototype->name() == tracker->metric_name()) {
        return down_cast<AtomicGauge<int64_t>*>(metric.get())->value();
      }
    }
    return STATUS(NotFound, "No gauge");
  };
  RunInLane(0, [&] {
    tracker->Consume(kBatch);
    ASSERT_EQ(ASSERT_RESULT(gauge_value()), 0);
    tracker->Consume(1);
    ASSERT_EQ(ASSERT_RESULT(gauge_value()), kBatch + 1);
  });
  tracker->Release(kBatch + 1);
  ASSERT_EQ(ASSERT_RESULT(gauge_value()), 0);
}

// Touches the root tracker: batching the root also batches the memory released since the last
// tcmalloc GC, so a GC is triggered no later than the threshold + L x batch.
TEST_F(MemTrackerLaneTest, ReleasedSinceGcBatched) {
  constexpr size_t kLanes = 2;
  constexpr int64_t kThreshold = 2 * kBatch;
  TEST_SetLaneTopology scope(TestLanes(kLanes));
  const auto& root = MemTracker::GetRootTracker();
  ASSERT_TRUE(root->EnableLaneBatching());
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_tcmalloc_gc_release_bytes) = kThreshold;
  auto tracker = MemTracker::CreateTracker("lane-gc");
  tracker->Consume(10 * kBatch);
  MemTracker::TEST_SetReleasedMemorySinceGC(0);
  const auto unflushed_before = MemTracker::TEST_UnflushedReleasedMemorySinceGC();

  constexpr int64_t kRelease = 900;
  RunInLane(0, [&] { tracker->Release(kRelease); });
  RunInLane(1, [&] { tracker->Release(kRelease); });
  ASSERT_EQ(MemTracker::TEST_ReleasedMemorySinceGC(), 0);
  ASSERT_EQ(MemTracker::TEST_UnflushedReleasedMemorySinceGC() - unflushed_before, 2 * kRelease);
  // Lane 0's slot passes the batch and moves to the shared count, still below the threshold.
  RunInLane(0, [&] { tracker->Release(kRelease); });
  ASSERT_EQ(MemTracker::TEST_ReleasedMemorySinceGC(), 2 * kRelease);
  // Lane 1's flush crosses the threshold: the GC runs and resets the shared count. The released
  // total at that point, 4 x 900, is within the threshold + L x batch.
  RunInLane(1, [&] { tracker->Release(kRelease); });
  ASSERT_LE(4 * kRelease, kThreshold + static_cast<int64_t>(kLanes) * kBatch);
#if YB_TCMALLOC_ENABLED
  ASSERT_EQ(MemTracker::TEST_ReleasedMemorySinceGC(), 0);
#endif
  // An unlaned thread adds to the shared count directly.
  MemTracker::TEST_SetReleasedMemorySinceGC(0);
  tracker->Release(100);
  ASSERT_EQ(MemTracker::TEST_ReleasedMemorySinceGC(), 100);
  tracker->Release(tracker->consumption());
  root->FlushLaneBatches();
}

// Touches the root tracker: an update from its consumption functor drops its unflushed slots,
// since the functor's value already includes the memory they stand for.
TEST_F(MemTrackerLaneTest, RootResyncDropsSlots) {
  const auto& root = MemTracker::GetRootTracker();
  if (!root->HasExternalSource()) {
    GTEST_SKIP() << "Root tracker has no consumption functor in this build";
  }
  TEST_SetLaneTopology scope(TestLanes(2));
  ASSERT_TRUE(root->EnableLaneBatching());
  auto tracker = MemTracker::CreateTracker("lane-resync");
  // Long interval, so that only the forced update below reads the functor.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_update_consumption_interval_us) = 3600'000'000;
  ASSERT_TRUE(root->MaybeUpdateConsumption(/* force= */ true));
  RunInLane(0, [&] {
    tracker->Consume(500);
  });
  ASSERT_EQ(root->TEST_UnflushedLaneBatches(), 500);
  ASSERT_TRUE(root->MaybeUpdateConsumption(/* force= */ true));
  ASSERT_EQ(root->TEST_UnflushedLaneBatches(), 0);
  tracker->Release(500);
}

// The batch flag accepts 0 (off) up to 16 MiB, and describes the bound it sets.
TEST_F(MemTrackerLaneTest, BatchFlag) {
  for (auto value : {"0", "1", "65536", "16777216"}) {
    ASSERT_OK(flags_internal::ValidateFlagValue("mem_tracker_lane_batch_bytes", value));
  }
  for (auto value : {"-1", "16777217"}) {
    ASSERT_NOK(flags_internal::ValidateFlagValue("mem_tracker_lane_batch_bytes", value));
  }
  gflags::CommandLineFlagInfo info;
  ASSERT_TRUE(gflags::GetCommandLineFlagInfo("mem_tracker_lane_batch_bytes", &info));
  ASSERT_EQ(info.default_value, "65536");
  ASSERT_EQ(info.description,
            "In lane mode, the delta a lane thread accumulates per lane-batched memory tracker "
            "(and in the memory released since the last tcmalloc GC) before applying it to the "
            "shared value. Shared consumption values, the limit checks that use them and the GC "
            "trigger can lag the tracked total by up to the number of lanes times this value. 0 "
            "disables lane batching.");
}

} // namespace yb
