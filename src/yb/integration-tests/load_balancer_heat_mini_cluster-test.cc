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

#include <chrono>

#include <gtest/gtest.h>

#include "yb/client/client.h"
#include "yb/client/client-test-util.h"
#include "yb/client/schema.h"
#include "yb/client/table_creator.h"
#include "yb/client/yb_table_name.h"

#include "yb/common/common.pb.h"
#include "yb/common/wire_protocol-test-util.h"

#include "yb/gutil/dynamic_annotations.h"
#include "yb/gutil/strings/substitute.h"

#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/test_workload.h"
#include "yb/integration-tests/yb_table_test_base.h"

#include "yb/master/catalog_entity_info.h"
#include "yb/master/catalog_manager.h"
#include "yb/master/cluster_balance_heat_cache.h"
#include "yb/master/cluster_balance_strategy.h"
#include "yb/master/master.h"
#include "yb/master/mini_master.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/monotime.h"
#include "yb/util/scope_exit.h"
#include "yb/util/test_util.h"

DECLARE_string(load_balancer_strategy);
DECLARE_bool(enable_load_balancer_heat_telemetry);
DECLARE_int32(tserver_heartbeat_metrics_interval_ms);
DECLARE_int32(catalog_manager_bg_task_wait_ms);
DECLARE_int32(min_leader_stepdown_retry_interval_ms);
DECLARE_int32(load_balancer_heat_staleness_threshold_secs);
DECLARE_double(load_balancer_heat_read_weight);
DECLARE_double(load_balancer_heat_write_weight);
DECLARE_double(load_balancer_heat_hysteresis_ops_per_sec);
DECLARE_int32(leader_balance_threshold);
DECLARE_int32(load_balancer_heat_leader_move_cooldown_secs);

using namespace std::literals;

namespace yb {
namespace integration_tests {

namespace {

const auto kDefaultTimeout = 30000ms;

void WaitLoadBalancerIdle(
    client::YBClient* client,
    const std::string& msg = "IsLoadBalancerIdle",
    const std::chrono::milliseconds timeout = kDefaultTimeout) {
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> { return client->IsLoadBalancerIdle(); },
      timeout * kTimeMultiplier, msg));
}

}  // namespace

class LoadBalancerHeatMiniClusterTest : public YBTableTestBase {
 protected:
  static constexpr int kNumTables = 4;

  bool use_yb_admin_client() override { return true; }
  bool use_external_mini_cluster() override { return false; }
  bool enable_ysql() override { return false; }
  size_t num_tablet_servers() override { return 3; }
  int num_tablets() override { return 1; }

  void SetUp() override {
    // Strategy: heat_aware_experimental. The flag is refreshed every balancer run.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_strategy) =
        master::kLoadBalancerStrategyHeatAwareExperimental;

    // Heat telemetry. Runtime AutoFlag (kLocalPersisted, default false).
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_load_balancer_heat_telemetry) = true;

    // Compress the heartbeat -> master -> balancer feedback loop so the test runs in seconds.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_heartbeat_metrics_interval_ms) = 200;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_catalog_manager_bg_task_wait_ms) = 200;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_min_leader_stepdown_retry_interval_ms) = 0;

    // Shrink heat staleness so a record from the previous heartbeat counts as fresh.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_staleness_threshold_secs) = 5;

    // Heat scoring: writes only, low hysteresis so the workload reliably crosses the bucket.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_read_weight) = 0.0;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_write_weight) = 1.0;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_hysteresis_ops_per_sec) = 1.0;

    // Disable count-based leader threshold so the count-balanced 2/1/1 leader distribution
    // doesn't suppress heat-driven moves on count-equality grounds.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_leader_balance_threshold) = 0;

    // Long cooldown so we catch the FIRST heat-driven move and don't reason about re-firing.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_heat_leader_move_cooldown_secs) = 3600;

    YBTableTestBase::SetUp();
  }

  // Override the base's default-table creation so the cluster has only the `kNumTables`
  // single-tablet test tables. The base's OpenTable() that follows will bind to table_name(),
  // which we override below to point at the first hot-test table.
  void CreateTable() override {
    for (int i = 0; i < kNumTables; ++i) {
      CreateSingleTabletTable(TableNameFor(i));
    }
  }

  client::YBTableName table_name() override { return TableNameFor(0); }

  static client::YBTableName TableNameFor(int i) {
    return client::YBTableName(
        YQL_DATABASE_CQL, "my_keyspace", strings::Substitute("heat_t$0", i));
  }

  void CreateSingleTabletTable(const client::YBTableName& name) {
    ASSERT_OK(client_->CreateNamespaceIfNotExists(name.namespace_name(), name.namespace_type()));
    auto client_schema = client::YBSchemaFromSchema(GetSimpleTestSchema());
    std::unique_ptr<client::YBTableCreator> creator(client_->NewTableCreator());
    ASSERT_OK(creator->table_name(name)
                  .schema(&client_schema)
                  .num_tablets(1)
                  .Create());
  }

  Result<scoped_refptr<master::TableInfo>> GetTableInfo(const client::YBTableName& name) {
    auto* mm = VERIFY_RESULT(mini_cluster()->GetLeaderMiniMaster());
    auto info = mm->catalog_manager().GetTableInfoFromNamespaceNameAndTableName(
        name.namespace_type(), name.namespace_name(), name.table_name());
    SCHECK(info != nullptr, NotFound, "table info not found: $0", name.ToString());
    return info;
  }

  Result<TabletId> GetSingleTabletId(const client::YBTableName& name) {
    auto info = VERIFY_RESULT(GetTableInfo(name));
    auto tablets = VERIFY_RESULT(info->GetTablets());
    SCHECK_EQ(tablets.size(), 1U, IllegalState,
              strings::Substitute("expected 1 tablet for $0, got $1",
                                  name.ToString(), tablets.size()));
    return tablets[0]->id();
  }

  Result<TabletServerId> GetLeaderTsForTable(const client::YBTableName& name) {
    auto info = VERIFY_RESULT(GetTableInfo(name));
    auto tablets = VERIFY_RESULT(info->GetTablets());
    SCHECK_EQ(tablets.size(), 1U, IllegalState, "expected single tablet");
    auto replicas = tablets[0]->GetReplicaLocations();
    for (const auto& [ts_uuid, replica] : *replicas) {
      if (replica.role == PeerRole::LEADER) {
        return ts_uuid;
      }
    }
    return STATUS_FORMAT(NotFound, "no leader for $0", name.ToString());
  }

  Result<std::unordered_map<std::string, TabletServerId>> GetTableToLeaderMap() {
    std::unordered_map<std::string, TabletServerId> result;
    for (int i = 0; i < kNumTables; ++i) {
      const auto name = TableNameFor(i);
      result[name.table_name()] = VERIFY_RESULT(GetLeaderTsForTable(name));
    }
    return result;
  }

  Result<TabletServerId> FindTserverWithTwoLeaders(
      const std::unordered_map<std::string, TabletServerId>& leader_map) {
    std::unordered_map<TabletServerId, int> counts;
    for (const auto& [_, ts] : leader_map) {
      counts[ts]++;
    }
    for (const auto& [ts, count] : counts) {
      if (count == 2) {
        return ts;
      }
    }
    return STATUS_FORMAT(NotFound,
                         "no tserver carries exactly 2 leaders (counts=$0)", counts.size());
  }

  Result<client::YBTableName> PickHotTable(
      const std::unordered_map<std::string, TabletServerId>& leader_map,
      const TabletServerId& source_ts) {
    for (int i = 0; i < kNumTables; ++i) {
      const auto name = TableNameFor(i);
      auto it = leader_map.find(name.table_name());
      if (it != leader_map.end() && it->second == source_ts) {
        return name;
      }
    }
    return STATUS_FORMAT(NotFound, "no table has its leader on $0", source_ts);
  }

  Status WaitForHeatOnTablet(
      const TabletId& tablet_id, const TabletServerId& expected_ts, double min_write_ops) {
    return WaitFor(
        [&]() -> Result<bool> {
          auto* mm = VERIFY_RESULT(mini_cluster()->GetLeaderMiniMaster());
          auto* heat_cache = mm->catalog_manager_impl().GetClusterBalanceHeatCache();
          if (heat_cache == nullptr) {
            return false;
          }
          auto rec = heat_cache->GetLeaderHeat(tablet_id, MonoDelta::FromSeconds(5));
          if (!rec.has_value()) {
            return false;
          }
          return rec->leader_uuid == expected_ts && rec->write_ops_per_sec >= min_write_ops;
        },
        30s * kTimeMultiplier,
        strings::Substitute(
            "heat cache reports tablet $0 hot on $1 (>= $2 wops)",
            tablet_id, expected_ts, min_write_ops));
  }

  Result<bool> IsReplicaOfTablet(const TabletId& tablet_id, const TabletServerId& ts_uuid) {
    auto* mm = VERIFY_RESULT(mini_cluster()->GetLeaderMiniMaster());
    // Iterate all our tables to find the tablet (small, only kNumTables).
    for (int i = 0; i < kNumTables; ++i) {
      const auto name = TableNameFor(i);
      auto info = mm->catalog_manager().GetTableInfoFromNamespaceNameAndTableName(
          name.namespace_type(), name.namespace_name(), name.table_name());
      if (info == nullptr) continue;
      auto tablets = VERIFY_RESULT(info->GetTablets());
      for (const auto& tablet : tablets) {
        if (tablet->id() != tablet_id) continue;
        auto replicas = tablet->GetReplicaLocations();
        return replicas->count(ts_uuid) > 0;
      }
    }
    return STATUS_FORMAT(NotFound, "tablet $0 not found", tablet_id);
  }
};

TEST_F(LoadBalancerHeatMiniClusterTest, MovesHotSingleTabletLeaderEndToEnd) {
  // 1. Wait for the LB to become active (first pass after the 4 tables came online), then
  //    settle into 2/1/1 leaders. We poll for 2-leader-on-some-tserver because count-based
  //    leader balancing happens piecemeal and "idle" can briefly co-exist with intermediate
  //    distributions.
  WaitLoadBalancerIdle(client_.get());

  // 2. Snapshot per-tserver leader counts. With 4 tables x 1 tablet x 3 tservers, the
  //    count-based balancer naturally distributes leaders 2/1/1.
  std::unordered_map<std::string, TabletServerId> leaders;
  TabletServerId hot_source_ts;
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        leaders = VERIFY_RESULT(GetTableToLeaderMap());
        auto res = FindTserverWithTwoLeaders(leaders);
        if (!res.ok()) {
          return false;
        }
        hot_source_ts = *res;
        return true;
      },
      60s * kTimeMultiplier, "leaders settle into 2/1/1 distribution"));
  const auto hot_table_name = ASSERT_RESULT(PickHotTable(leaders, hot_source_ts));

  // 3. Fetch the hot table's tablet id.
  const TabletId hot_tablet_id = ASSERT_RESULT(GetSingleTabletId(hot_table_name));

  LOG(INFO) << "Hot source tserver: " << hot_source_ts
            << ", hot table: " << hot_table_name.ToString()
            << ", hot tablet: " << hot_tablet_id;

  // 4. Drive a moderate write workload at the hot table only. We need enough writes to cross
  //    the heat hysteresis threshold (1 op/sec), but not so many that follower replication
  //    can't keep up — Raft refuses leader stepdown ("LEADER_NOT_READY_TO_STEP_DOWN: Suggested
  //    peer is not caught up yet") if the target follower is behind the majority op id, so a
  //    saturated write pipeline blocks the move we're trying to observe. TestYcqlWorkload's
  //    Setup() skips creation when the table already exists.
  TestYcqlWorkload workload(mini_cluster());
  workload.set_table_name(hot_table_name);
  workload.set_num_tablets(1);
  workload.set_num_write_threads(1);
  workload.set_num_read_threads(0);
  workload.set_write_batch_size(1);
  workload.set_write_interval_millis(20);
  workload.Setup();
  workload.Start();

  bool stopped = false;
  auto stop_workload = ScopeExit([&] {
    if (!stopped) {
      workload.StopAndJoin();
    }
  });

  // 5. Wait for the master heat cache to observe the hot tablet on the expected source.
  //    Proves: tablet writes -> tserver counters -> heartbeat -> master -> heat cache.
  ASSERT_OK(WaitForHeatOnTablet(hot_tablet_id, hot_source_ts, /*min_write_ops=*/10.0));

  // 6. Wait for the load balancer to move the leader off the hot source. Proves:
  //    heat cache -> AggregateLeaderHeatIntoGlobalState -> heat-aware AssessLeaderMove ->
  //    Raft stepdown.
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        const auto current_leader = VERIFY_RESULT(GetLeaderTsForTable(hot_table_name));
        return current_leader != hot_source_ts;
      },
      60s * kTimeMultiplier, "leader moves off hot source"));

  // 7. Stop the writer immediately so the new leader doesn't become the new hotspot.
  workload.StopAndJoin();
  stopped = true;
  WaitLoadBalancerIdle(client_.get());

  // 8. Final assertions: the hot table's leader is no longer on the original source, and the
  //    new leader is one of the other replicas of that tablet.
  const auto final_leader = ASSERT_RESULT(GetLeaderTsForTable(hot_table_name));
  EXPECT_NE(final_leader, hot_source_ts);
  EXPECT_TRUE(ASSERT_RESULT(IsReplicaOfTablet(hot_tablet_id, final_leader)));
}

}  // namespace integration_tests
}  // namespace yb
