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

#include <algorithm>
#include <atomic>
#include <limits>
#include <mutex>
#include <stack>
#include <thread>

#include "yb/docdb/lock_batch.h"
#include "yb/docdb/lock_util.h"
#include "yb/docdb/shared_lock_manager.h"

#include "yb/rpc/thread_pool.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/ref_cnt_buffer.h"
#include "yb/util/result.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/test_util.h"

using namespace std::literals;

using std::string;
using std::vector;
using std::thread;

DECLARE_bool(dump_lock_keys);
DECLARE_uint32(shared_lock_manager_max_free_entries);

namespace yb {
namespace docdb {

using dockv::IntentType;
using dockv::IntentTypeSet;

const RefCntPrefix kKey1("foo"s);
const RefCntPrefix kKey2("bar"s);

class SharedLockManagerTest : public YBTest {
 protected:
  SharedLockManagerTest();

 protected:
  SharedLockManager lm_;

  LockBatch TestLockBatch(CoarseTimePoint deadline = CoarseTimePoint::max()) {
    return LockBatch(&lm_, {
        {kKey1, IntentTypeSet({IntentType::kStrongWrite, IntentType::kStrongRead})},
        {kKey2, IntentTypeSet({IntentType::kStrongWrite, IntentType::kStrongRead})}},
        deadline);
  }
};

SharedLockManagerTest::SharedLockManagerTest() {
}

TEST_F(SharedLockManagerTest, LockBatchAutoUnlockTest) {
  for (int i = 0; i < 2; ++i) {
    auto lb = TestLockBatch();
    EXPECT_EQ(2, lb.size());
    EXPECT_FALSE(lb.empty());
    // The locks get unlocked on scope exit.
  }
}

TEST_F(SharedLockManagerTest, LockBatchMoveConstructor) {
  LockBatch lb = TestLockBatch();
  EXPECT_EQ(2, lb.size());
  EXPECT_FALSE(lb.empty());
  ASSERT_OK(lb.status());

  LockBatch lb_fail = TestLockBatch(CoarseMonoClock::now() + 10ms);
  ASSERT_FALSE(lb_fail.status().ok());
  ASSERT_TRUE(lb_fail.empty());

  LockBatch lb2(std::move(lb));
  EXPECT_EQ(2, lb2.size());
  EXPECT_FALSE(lb2.empty());
  ASSERT_OK(lb2.status());

  // lb has been moved from and is now empty.
  EXPECT_EQ(0, lb.size()); // NOLINT(bugprone-use-after-move)
  EXPECT_TRUE(lb.empty()); // NOLINT(bugprone-use-after-move)
  ASSERT_OK(lb.status()); // NOLINT(bugprone-use-after-move)

  LockBatch lb_fail2(std::move(lb_fail));
  ASSERT_FALSE(lb_fail2.status().ok());
  ASSERT_TRUE(lb_fail2.empty());
}

TEST_F(SharedLockManagerTest, LockBatchMoveAssignment) {
  LockBatch lb = TestLockBatch();

  LockBatch lb_fail = TestLockBatch(CoarseMonoClock::now() + 10ms);
  ASSERT_FALSE(lb_fail.status().ok());
  ASSERT_TRUE(lb_fail.empty());

  LockBatch lb2 = std::move(lb);
  EXPECT_EQ(2, lb2.size());
  EXPECT_FALSE(lb2.empty());
  ASSERT_OK(lb2.status());

  // lb has been moved from and is now empty.
  EXPECT_EQ(0, lb.size()); // NOLINT(bugprone-use-after-move)
  EXPECT_TRUE(lb.empty()); // NOLINT(bugprone-use-after-move)

  LockBatch lb_fail2 = std::move(lb_fail);
  ASSERT_FALSE(lb_fail2.status().ok());
  ASSERT_TRUE(lb_fail2.empty());
}

TEST_F(SharedLockManagerTest, LockBatchReset) {
  LockBatch lb = TestLockBatch();
  lb.Reset();

  EXPECT_EQ(0, lb.size());
  EXPECT_TRUE(lb.empty());
}

// Launch pairs of threads. Each pair tries to lock/unlock on the same key sequence.
// This catches bug in SharedLockManager when condition is waited incorrectly.
TEST_F(SharedLockManagerTest, QuickLockUnlock) {
  const auto kThreads = 2 * 32; // Should be even

  std::atomic<bool> stop_requested{false};
  std::vector<std::thread> threads;
  std::atomic<size_t> finished_threads{0};
  while (threads.size() != kThreads) {
    size_t pair_idx = threads.size() / 2;
    threads.emplace_back([this, &stop_requested, &finished_threads, pair_idx] {
      int i = 0;
      while (!stop_requested.load(std::memory_order_acquire)) {
        RefCntPrefix key(Format("key_$0_$1", pair_idx, i));
        LockBatch lb(&lm_,
                     {{key, IntentTypeSet({IntentType::kStrongWrite, IntentType::kStrongRead})}},
                     CoarseTimePoint::max());
        ++i;
      }
      finished_threads.fetch_add(1, std::memory_order_acq_rel);
    });
  }

  std::this_thread::sleep_for(30s);
  LOG(INFO) << "Requesting stop";
  stop_requested.store(true, std::memory_order_release);

  ASSERT_OK(WaitFor(
      [&finished_threads] {
        return finished_threads.load(std::memory_order_acquire) == kThreads;
      },
      3s,
      "All threads finished"));

  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(SharedLockManagerTest, LockConflicts) {
  rpc::ThreadPool tp(rpc::ThreadPoolOptions{
    .name = "test_pool"s,
    .max_workers = 1,
  });

  for (size_t idx1 = 0; idx1 != dockv::kIntentTypeSetMapSize; ++idx1) {
    IntentTypeSet set1(idx1);
    SCOPED_TRACE(Format("Set1: $0", set1));
    for (size_t idx2 = 0; idx2 != dockv::kIntentTypeSetMapSize; ++idx2) {
      IntentTypeSet set2(idx2);
      SCOPED_TRACE(Format("Set2: $0", set2));
      LockBatch lb1(&lm_, {{kKey1, set1}}, CoarseTimePoint::max());
      ASSERT_OK(lb1.status());
      LockBatch lb2(&lm_, {{kKey1, set2}}, CoarseMonoClock::now());
      if (lb2.status().ok()) {
        // Lock on set2 was taken fast enough, it means that sets should NOT conflict.
        ASSERT_FALSE(IntentTypeSetsConflict(set1, set2));
      } else {
        // Lock on set2 was taken not taken for too long, it means that sets should conflict.
        ASSERT_TRUE(IntentTypeSetsConflict(set1, set2));
      }
    }
  }

  tp.Shutdown();
}

// A burst of distinct keys must not leave a permanent per-tablet footprint: released entries
// beyond FLAGS_shared_lock_manager_max_free_entries are freed rather than cached.
TEST_F(SharedLockManagerTest, FreeListBounded) {
  constexpr size_t kBurstKeys = 1000;
  constexpr uint32_t kCap = 8;

  auto make_burst = [](size_t num_keys) {
    LockBatchEntries<SharedLockManager> entries;
    entries.reserve(num_keys);
    for (size_t i = 0; i != num_keys; ++i) {
      entries.push_back(
          {RefCntPrefix(Format("burst_$0", i)), IntentTypeSet({IntentType::kWeakRead})});
    }
    return entries;
  };

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_shared_lock_manager_max_free_entries) = kCap;
  {
    LockBatch lb(&lm_, make_burst(kBurstKeys), CoarseTimePoint::max());
    ASSERT_OK(lb.status());
    ASSERT_EQ(lm_.TEST_LocksSize(), kBurstKeys);
  }
  ASSERT_EQ(lm_.TEST_LocksSize(), 0);
  ASSERT_EQ(lm_.TEST_FreeEntriesCount(), kCap);

  // Cached entries are reused before new ones are allocated.
  {
    auto lb = TestLockBatch();
    ASSERT_OK(lb.status());
    ASSERT_EQ(lm_.TEST_FreeEntriesCount(), kCap - lb.size());
  }
  ASSERT_EQ(lm_.TEST_FreeEntriesCount(), kCap);

  // Zero disables caching: the burst consumes the kCap cached entries and nothing is re-cached.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_shared_lock_manager_max_free_entries) = 0;
  {
    LockBatch lb(&lm_, make_burst(kBurstKeys), CoarseTimePoint::max());
    ASSERT_OK(lb.status());
  }
  ASSERT_EQ(lm_.TEST_LocksSize(), 0);
  ASSERT_EQ(lm_.TEST_FreeEntriesCount(), 0);
}

// ===== THROWAWAY BENCHMARK FOR #21013 -- DO NOT COMMIT =====
// Compares lock/unlock throughput with the entry cache disabled (cap 0), at the proposed default
// (256), and effectively unbounded (today's behavior). Keys are pre-built outside the timed loop
// and the LockBatchEntries vector is shuttled via Unlock()/TryLock() so nothing but the lock
// manager itself allocates in the hot path.
namespace {

struct BenchScenario {
  const char* name;
  size_t num_threads;
  size_t keys_per_batch;
  // Number of distinct pre-built batches each thread cycles through. 1 => the same keys every
  // iteration (hot row). >1 => by the time a batch is reused its keys are no longer in locks_, so
  // every Reserve misses the map exactly like a genuinely fresh key would.
  size_t ring_size;
};

}  // namespace

TEST_F(SharedLockManagerTest, TEMP_PoolBenchmark) {
  const std::vector<BenchScenario> kScenarios = {
    {"A  1 thread, same 4 keys",         1,   4,  1},
    {"B 16 threads, 4 fresh keys",      16,   4, 64},
    {"C  8 threads, 256 fresh keys",     8, 256, 64},
  };
  const std::vector<uint32_t> kCaps = {0, 256, std::numeric_limits<uint32_t>::max()};
  const auto kDuration = 3s;
  constexpr int kRepeats = 5;

  const auto intents = IntentTypeSet({IntentType::kStrongWrite, IntentType::kStrongRead});

  auto run_once = [&](const BenchScenario& scenario, uint32_t cap) -> double {
    // Fresh manager per run so cached entries never carry across variants.
    SharedLockManager lm;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_shared_lock_manager_max_free_entries) = cap;

    std::vector<size_t> per_thread_keys(scenario.num_threads, 0);
    TestThreadHolder holder(/* verbose= */ false);
    for (size_t t = 0; t != scenario.num_threads; ++t) {
      holder.AddThreadFunctor([&, t] {
        std::vector<UnlockedBatch> ring;
        ring.reserve(scenario.ring_size);
        for (size_t r = 0; r != scenario.ring_size; ++r) {
          LockBatchEntries<SharedLockManager> entries;
          entries.reserve(scenario.keys_per_batch);
          for (size_t k = 0; k != scenario.keys_per_batch; ++k) {
            entries.push_back({RefCntPrefix(Format("t$0_r$1_k$2", t, r, k)), intents});
          }
          ring.emplace_back(std::move(entries), &lm);
        }
        size_t count = 0;
        size_t idx = 0;
        while (!holder.stop_flag().load(std::memory_order_acquire)) {
          auto& unlocked = ring[idx];
          LockBatch lb = unlocked.TryLock(CoarseTimePoint::max());
          CHECK_OK(lb.status());
          unlocked = std::move(*lb.Unlock());
          count += scenario.keys_per_batch;
          if (++idx == scenario.ring_size) {
            idx = 0;
          }
        }
        per_thread_keys[t] = count;
      });
    }
    const auto start = MonoTime::Now();
    holder.WaitAndStop(kDuration);
    const double elapsed_sec = (MonoTime::Now() - start).ToSeconds();
    size_t total = 0;
    for (auto c : per_thread_keys) {
      total += c;
    }
    return total / elapsed_sec;
  };

  // results[scenario][cap] -> samples. Variants are interleaved per repeat so that slow drift
  // (thermal, background load) spreads evenly across them instead of biasing one cap.
  std::vector<std::vector<std::vector<double>>> results(
      kScenarios.size(), std::vector<std::vector<double>>(kCaps.size()));
  for (int rep = 0; rep < kRepeats; ++rep) {
    for (size_t s = 0; s != kScenarios.size(); ++s) {
      for (size_t c = 0; c != kCaps.size(); ++c) {
        results[s][c].push_back(run_once(kScenarios[s], kCaps[c]));
      }
    }
  }

  for (size_t s = 0; s != kScenarios.size(); ++s) {
    for (size_t c = 0; c != kCaps.size(); ++c) {
      auto& samples = results[s][c];
      std::sort(samples.begin(), samples.end());
      std::vector<size_t> rounded;
      for (auto v : samples) {
        rounded.push_back(static_cast<size_t>(v));
      }
      LOG(INFO) << Format(
          "BENCH scenario=[$0] cap=$1 median_keys_per_sec=$2 min=$3 max=$4",
          kScenarios[s].name,
          kCaps[c] == std::numeric_limits<uint32_t>::max() ? "max" : std::to_string(kCaps[c]),
          rounded[kRepeats / 2], rounded.front(), rounded.back());
    }
  }
}
// ===== END THROWAWAY BENCHMARK =====

TEST_F(SharedLockManagerTest, DumpKeys) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_dump_lock_keys) = true;

  auto lb1 = TestLockBatch();
  ASSERT_OK(lb1.status());
  auto lb2 = TestLockBatch(CoarseMonoClock::now() + 10ms);
  ASSERT_NOK(lb2.status());
  ASSERT_STR_CONTAINS(
      lb2.status().ToString(),
      "[{ key: 666F6F intent_types: [kStrongRead, kStrongWrite] }, "
      "{ key: 626172 intent_types: [kStrongRead, kStrongWrite] }]");
}

} // namespace docdb
} // namespace yb
