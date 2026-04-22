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

#include "yb/master/cluster_balance_heat_cache.h"

#include <atomic>
#include <thread>
#include <vector>

#include "yb/util/monotime.h"
#include "yb/util/test_util.h"

namespace yb::master {

namespace {

LeaderHeatRecord MakeRecord(const TabletServerId& ts_uuid, double reads, double writes,
                            MonoTime when) {
  return LeaderHeatRecord{
      .leader_uuid = ts_uuid,
      .read_ops_per_sec = reads,
      .write_ops_per_sec = writes,
      .last_updated = when,
  };
}

} // namespace

TEST(ClusterBalanceHeatCacheTest, UpdateAndRead) {
  ClusterBalanceHeatCache cache;
  const auto now = MonoTime::Now();
  cache.UpdateLeaderHeat("t1", MakeRecord("ts1", 10.0, 5.0, now));

  auto fetched = cache.GetLeaderHeat("t1", MonoDelta::FromSeconds(60));
  ASSERT_TRUE(fetched.has_value());
  EXPECT_EQ(fetched->leader_uuid, "ts1");
  EXPECT_DOUBLE_EQ(fetched->read_ops_per_sec, 10.0);
  EXPECT_DOUBLE_EQ(fetched->write_ops_per_sec, 5.0);

  EXPECT_FALSE(cache.GetLeaderHeat("missing", MonoDelta::FromSeconds(60)).has_value());
}

TEST(ClusterBalanceHeatCacheTest, UpdateOverwrites) {
  ClusterBalanceHeatCache cache;
  const auto now = MonoTime::Now();
  cache.UpdateLeaderHeat("t1", MakeRecord("ts1", 10.0, 5.0, now));
  cache.UpdateLeaderHeat("t1", MakeRecord("ts2", 20.0, 15.0, now));

  auto fetched = cache.GetLeaderHeat("t1", MonoDelta::FromSeconds(60));
  ASSERT_TRUE(fetched.has_value());
  EXPECT_EQ(fetched->leader_uuid, "ts2");
  EXPECT_DOUBLE_EQ(fetched->read_ops_per_sec, 20.0);
  EXPECT_DOUBLE_EQ(fetched->write_ops_per_sec, 15.0);
}

TEST(ClusterBalanceHeatCacheTest, StaleEntriesExcludedFromSnapshotAndGet) {
  ClusterBalanceHeatCache cache;
  const auto now = MonoTime::Now();
  // Record stale (5 minutes in the past).
  cache.UpdateLeaderHeat("stale", MakeRecord("ts1", 1.0, 2.0,
                                             now - MonoDelta::FromSeconds(300)));
  cache.UpdateLeaderHeat("fresh", MakeRecord("ts2", 3.0, 4.0, now));

  // GetLeaderHeat honors the staleness threshold.
  EXPECT_FALSE(cache.GetLeaderHeat("stale", MonoDelta::FromSeconds(30)).has_value());
  EXPECT_TRUE(cache.GetLeaderHeat("fresh", MonoDelta::FromSeconds(30)).has_value());

  // SnapshotFresh drops stale entries but leaves them in the underlying map (cleanup is
  // opportunistic; no-churn reporting tablets eventually get refreshed in place).
  auto snap = cache.SnapshotFresh(MonoDelta::FromSeconds(30));
  EXPECT_EQ(snap.size(), 1u);
  EXPECT_TRUE(snap.contains("fresh"));
  EXPECT_FALSE(snap.contains("stale"));
  EXPECT_EQ(cache.SizeForTest(), 2u);
}

TEST(ClusterBalanceHeatCacheTest, ClearLeaderHeatIfFromMatchingReporter) {
  ClusterBalanceHeatCache cache;
  const auto now = MonoTime::Now();
  cache.UpdateLeaderHeat("t1", MakeRecord("ts1", 10.0, 5.0, now));

  // A report from the same tserver that stored the record (i.e. step-down: former leader reports
  // leader_info without heat fields) must drop the record immediately.
  cache.ClearLeaderHeatIfFrom("t1", "ts1");
  EXPECT_FALSE(cache.GetLeaderHeat("t1", MonoDelta::FromSeconds(60)).has_value());
  EXPECT_EQ(cache.SizeForTest(), 0u);
}

TEST(ClusterBalanceHeatCacheTest, ClearLeaderHeatIfFromNonMatchingReporter) {
  ClusterBalanceHeatCache cache;
  const auto now = MonoTime::Now();
  cache.UpdateLeaderHeat("t1", MakeRecord("ts1", 10.0, 5.0, now));

  // A report from a different tserver (e.g. a follower peer that was never the cached leader)
  // must not disturb the cached record — ts1 is still the leader for this tablet.
  cache.ClearLeaderHeatIfFrom("t1", "ts2");
  auto fetched = cache.GetLeaderHeat("t1", MonoDelta::FromSeconds(60));
  ASSERT_TRUE(fetched.has_value());
  EXPECT_EQ(fetched->leader_uuid, "ts1");

  // Clearing for a tablet we've never recorded is a no-op.
  cache.ClearLeaderHeatIfFrom("never_seen", "ts1");
  EXPECT_EQ(cache.SizeForTest(), 1u);
}

TEST(ClusterBalanceHeatCacheTest, ConcurrentWritersSameTablet) {
  // Single tablet, many concurrent writers — the cache should not corrupt state. We do not
  // assert a particular winning value; the point is no crash, no torn reads, and a final read
  // returns something a writer actually wrote.
  ClusterBalanceHeatCache cache;
  constexpr int kNumWriters = 8;
  constexpr int kIterations = 1000;
  std::vector<std::thread> threads;
  std::atomic<bool> start{false};
  for (int i = 0; i < kNumWriters; ++i) {
    threads.emplace_back([&, i]() {
      while (!start.load(std::memory_order_acquire)) { }
      for (int j = 0; j < kIterations; ++j) {
        cache.UpdateLeaderHeat("t1",
            MakeRecord(Format("ts$0", i), static_cast<double>(j),
                       static_cast<double>(j + 1), MonoTime::Now()));
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& t : threads) {
    t.join();
  }

  auto final_read = cache.GetLeaderHeat("t1", MonoDelta::FromSeconds(60));
  ASSERT_TRUE(final_read.has_value());
  // Whatever winner landed, write_ops_per_sec is exactly one more than read_ops_per_sec in
  // every record a writer wrote, so a torn read would fail this check.
  EXPECT_DOUBLE_EQ(final_read->write_ops_per_sec, final_read->read_ops_per_sec + 1);
}

} // namespace yb::master
