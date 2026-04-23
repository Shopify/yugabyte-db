// Copyright (c) YugabyteDB, Inc.

#pragma once

#include "yb/master/cluster_balance.h"
#include "yb/master/cluster_balance_util.h"
#include "yb/master/master_fwd.h"
#include "yb/master/ts_manager.h"

namespace yb::master {

class ClusterLoadBalancerMocked : public ClusterLoadBalancer {
 public:
  explicit ClusterLoadBalancerMocked(const TableId& table_id) : ClusterLoadBalancer(nullptr)  {
    const int kHighNumber = 100;
    options_.kMaxConcurrentAdds = kHighNumber;
    options_.kMaxConcurrentRemovals = kHighNumber;
    options_.kAllowLimitStartingTablets = false;
    options_.kAllowLimitOverReplicatedTablets = false;

    auto table_state = std::make_unique<PerTableLoadState>(global_state_.get());
    table_state->options_ = &options_;
    table_state->scorer_ = &strategy_->scorer();
    state_ = table_state.get();
    per_table_states_[table_id] = std::move(table_state);
    ResetOptions();

    InitTablespaceManager();
  }

  // Overrides for base class functionality to bypass calling CatalogManager.
  void GetAllDescriptors(TSDescriptorVector* ts_descs) const override {
    *ts_descs = ts_descs_;
  }

  void GetAllAffinitizedZones(
      const ReplicationInfoPB& replication_info,
      std::vector<AffinitizedZonesSet>* affinitized_zones) const override {
    *affinitized_zones = affinitized_zones_;
  }

  std::optional<std::reference_wrapper<const TabletInfoPtr>> GetTabletInfo(
      const TabletId& id) const override {
    auto it = tablet_map_.find(id);
    if (it == tablet_map_.end()) {
      return std::nullopt;
    }
    return std::cref(it->second);
  }

  ReplicationInfoPB GetTableReplicationInfo(const TableInfoPtr& table) const override {
    return replication_info_;
  }

  const PerTableLoadState* GetTableState(const TableId& table_id) {
    return per_table_states_[table_id].get();
  }

  void SetBlacklistAndPendingDeleteTS() override {
    for (const auto& ts_desc : global_state_->ts_descs_) {
      AddTSIfBlacklisted(ts_desc, blacklist_, false);
      AddTSIfBlacklisted(ts_desc, leader_blacklist_, true);
      global_state_->pending_deletes_[ts_desc->permanent_uuid()] =
          ts_desc->TabletsPendingDeletion();
    }
  }

  Status SendAddReplica(
      const TabletInfoPtr& tablet, const TabletServerId& ts_uuid, const std::string& reason)
      override {
    return Status::OK();
  }
  Status SendRemoveReplica(
      const TabletInfoPtr& tablet, const TabletServerId& ts_uuid, const std::string& reason)
      override {
    return Status::OK();
  }
  Status SendMoveLeader(
      const TabletInfoPtr& tablet, const TabletServerId& ts_uuid,
      bool also_remove_replica, const std::string& reason,
      const TabletServerId& new_leader_ts_uuid) override {
    return Status::OK();
  }

  void GetPendingTasks(const TableInfoPtr& table,
                       TabletToTabletServerMap* pending_add_replica_tasks,
                       TabletToTabletServerMap* pending_remove_replica_tasks,
                       TabletToTabletServerMap* pending_stepdown_leader_tasks) override {
    *pending_add_replica_tasks = pending_add_replica_tasks_;
    *pending_remove_replica_tasks = pending_remove_replica_tasks_;
    *pending_stepdown_leader_tasks = pending_stepdown_leader_tasks_;
  }

  void ResetTableStatePtr(const TableId& table_id, Options* options) override {
    if (state_) {
      options = state_->options_;
    }
    auto table_state = std::make_unique<PerTableLoadState>(global_state_.get());
    table_state->options_ = options;
    table_state->scorer_ = &strategy_->scorer();
    table_state->check_ts_liveness_ = false;
    state_ = table_state.get();

    per_table_states_[table_id] = std::move(table_state);
  }

  void InitTablespaceManager() override {
    tablespace_manager_ = std::make_shared<YsqlTablespaceManager>(nullptr, nullptr);
  }

  void SetOptions(ReplicaType type, const std::string& placement_uuid) {
    state_->options_->type = type;
    state_->options_->placement_uuid = placement_uuid;
  }

  // Swaps the active strategy and re-points the current per-table state's scorer at it. Intended
  // for tests that want to exercise specific strategies without relying on the gflag.
  void SetStrategyForTest(std::unique_ptr<LoadBalancerStrategy> strategy) {
    strategy_ = std::move(strategy);
    for (auto& [_, table_state] : per_table_states_) {
      table_state->scorer_ = &strategy_->scorer();
    }
  }

  // Seed per-tserver heat aggregates on the current run's global state. Exists because the
  // count-based strategy must provably ignore heat values and tests need to assert that under
  // extreme seeds the decision trace is unchanged.
  void SetHeatForTest(
      const TabletServerId& ts_uuid, const GlobalLoadState::TServerLeaderHeat& heat) {
    global_state_->heat_by_ts_[ts_uuid] = heat;
  }

  // Seed a per-tablet heat record on the current run's global state. Tests that want to exercise
  // the in-run projection done by ProjectLeaderHeatMoveIntoGlobalState need an entry in
  // heat_by_tablet_ so that a subsequent MoveLeader will know how much to subtract from the
  // source and add to the destination. Production populates this map in
  // AggregateLeaderHeatIntoGlobalState from ClusterBalanceHeatCache::SnapshotFresh.
  void SetTabletHeatForTest(const TabletId& tablet_id, const LeaderHeatRecord& record) {
    global_state_->heat_by_tablet_[tablet_id] = record;
  }

  // Read-only accessor for a tserver's heat aggregate. Exposed so tests can assert on the
  // post-move projected state of heat_by_ts_.
  GlobalLoadState::TServerLeaderHeat GetHeatForTest(const TabletServerId& ts_uuid) const {
    const auto it = global_state_->heat_by_ts_.find(ts_uuid);
    return it == global_state_->heat_by_ts_.end() ? GlobalLoadState::TServerLeaderHeat{}
                                                  : it->second;
  }

  // Read-only accessor for the per-tablet heat snapshot. Tests use it to assert that the cached
  // record's leader_uuid was rewritten on a successful move.
  std::optional<LeaderHeatRecord> GetTabletHeatForTest(const TabletId& tablet_id) const {
    const auto it = global_state_->heat_by_tablet_.find(tablet_id);
    if (it == global_state_->heat_by_tablet_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  // Re-runs the load and leader-load sort after tests have seeded heat. In production,
  // AggregateLeaderHeatIntoGlobalState runs before AnalyzeTablets's SortLeaderLoad so the sort
  // reflects heat. In the mocked test flow tests call AnalyzeTablets first and seed heat after,
  // so tests that exercise heat-aware sort ordering must re-sort explicitly to replicate the
  // production ordering.
  void ResortAfterHeatSeedForTest() {
    if (state_ != nullptr) {
      state_->SortLoad();
      state_->SortLeaderLoad();
    }
  }

  // Exposes the heat-aware cooldown map size so tests can assert that non-heat-driven paths
  // (count-based moves, placement-repair stepdowns) never populate it.
  size_t GetHeatAwareCooldownSizeForTest() const {
    return heat_aware_recent_leader_moves_.size();
  }

  // Simulates a successful heat-driven leader move without going through MoveLeader. Useful in
  // unit tests that want to prime the cooldown map directly.
  void RecordHeatAwareLeaderMoveForTest(
      const TabletId& tablet_id, const TabletServerId& from_ts, const TabletServerId& to_ts) {
    RecordHeatAwareLeaderMove(tablet_id, from_ts, to_ts);
  }

  // Test-only accessor for the otherwise-private cooldown predicate. Lets tests assert on the
  // (tablet_id, from_ts, to_ts) key granularity without going through a full balancer run.
  // Not const — the underlying predicate evicts stale entries when it detects a recorded move
  // that did not actually take effect (failed async stepdown).
  bool IsInHeatAwareCooldownForTest(
      const TabletId& tablet_id, const TabletServerId& from_ts,
      const TabletServerId& to_ts) {
    return IsInHeatAwareCooldown(tablet_id, from_ts, to_ts);
  }

  // Direct entry point to the in-run heat projection helper. Lets tests exercise edge cases
  // (empty to_ts from RemoveReplica's leader-stepdown path, stale leader_uuid, tablet with no
  // fresh heat) without having to reproduce the full RemoveReplica → MoveLeader control flow.
  void ProjectLeaderHeatMoveForTest(
      const TabletId& tablet_id, const TabletServerId& from_ts, const TabletServerId& to_ts) {
    ProjectLeaderHeatMoveIntoGlobalState(tablet_id, from_ts, to_ts);
  }

  // Returns true iff heat_by_ts_ has an entry for ts_uuid (including default-constructed entries
  // inserted via operator[]). Tests use this to assert the projection did not inadvertently
  // create a bogus heat_by_ts_[""] entry when to_ts was empty.
  bool HasHeatEntryForTest(const TabletServerId& ts_uuid) const {
    return global_state_->heat_by_ts_.count(ts_uuid) > 0;
  }

  // Drives the once-per-run heat aggregation + cooldown-eviction hook without spinning up a full
  // balancer run. Tests use this to verify that cooldown eviction runs even when
  // enable_load_balancer_heat_telemetry is disabled.
  void AggregateLeaderHeatForTest() {
    AggregateLeaderHeatIntoGlobalState();
  }

  void ResetOptions() { SetOptions(ReplicaType::kLive, ""); }

  Options options_;
  TSDescriptorVector ts_descs_;
  std::vector<AffinitizedZonesSet> affinitized_zones_;
  TabletInfoMap tablet_map_;
  TableIndex tables_;
  ReplicationInfoPB replication_info_;
  BlacklistPB blacklist_;
  BlacklistPB leader_blacklist_;
  TabletToTabletServerMap pending_add_replica_tasks_;
  TabletToTabletServerMap pending_remove_replica_tasks_;
  TabletToTabletServerMap pending_stepdown_leader_tasks_;

  friend class TestLoadBalancerEnterprise;
};

} // namespace yb::master
