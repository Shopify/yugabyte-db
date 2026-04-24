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

  // Phase 4: test accessor for the shared global state. Used by tests that need to invoke
  // strategy-level hooks (e.g. AssessTabletMove) directly without routing through
  // GetLoadToMove — global_state_ itself is protected on the base class.
  GlobalLoadState& GlobalStateForTest() { return *global_state_; }

  // Phase 4.5: direct entry point for ClusterLoadBalancer::GetTabletToMove. Lets tests exercise
  // the heat-aware tablet-selection branch with arbitrary seeded heat without constructing a
  // full balancer run — the heat-qualified pair would otherwise have to be driven through
  // AssessTabletMove + HandleAddReplicas, which in the multi-az fixture gives at most one
  // candidate per source and cannot test "pick the best of N". The is_heat_driven argument is
  // forwarded verbatim so tests can assert both branches.
  Result<std::optional<TabletId>> GetTabletToMoveForTest(
      const TabletServerId& from_ts, const TabletServerId& to_ts, bool is_heat_driven) {
    return GetTabletToMove(from_ts, to_ts, is_heat_driven);
  }

  // Phase 4.5: test accessor for the per-table over-replicated set. Phase 4.5 tests use this to
  // mark a specific tablet as ineligible during the filter phase so the heat-aware branch has to
  // compose correctly with the existing candidate filters. state_ itself is protected on the base
  // class.
  void AddTabletToOverReplicatedForTest(const TabletId& tablet_id) {
    state_->tablets_over_replicated_.insert(tablet_id);
  }

  // Phase 4.5 reviewer P2: set per_tablet_meta_[tablet_id].is_over_replicated so
  // GetPossiblyTransientLoad counts the tablet's owning tservers against the transient-load
  // safeguard. Separate from AddTabletToOverReplicatedForTest because the filter-phase check
  // consults the per-table tablets_over_replicated_ set, while GetPossiblyTransientLoad
  // consults the per-tablet meta's is_over_replicated flag — tests need to exercise each
  // path independently.
  void MarkTabletIsOverReplicatedForTest(const TabletId& tablet_id) {
    state_->per_tablet_meta_[tablet_id].is_over_replicated = true;
  }

  // Phase 4.6: register (tablet_id, ts_uuid) as an over-replicated peer pair — inserts tablet_id
  // into tablets_over_replicated_ AND appends ts_uuid to the tablet's
  // over_replicated_tablet_servers list. HandleRemoveReplicas iterates the per-table set and
  // reads the per-tablet peer list, so both must be populated for the removal path to pick up
  // the over-replication. Distinct from AddTabletToOverReplicatedForTest (per-table set only)
  // and MarkTabletIsOverReplicatedForTest (per-tablet is_over_replicated flag only).
  void SetTabletOverReplicatedOnTsForTest(
      const TabletId& tablet_id, const TabletServerId& ts_uuid) {
    state_->tablets_over_replicated_.insert(tablet_id);
    state_->per_tablet_meta_[tablet_id].over_replicated_tablet_servers.insert(ts_uuid);
  }

  // Phase 4: seed GetGlobalLoad's output for a specific tserver without routing through the full
  // catalog-building path (AddRunningTablet etc.). Used by tests that exercise the global-load
  // regression guard in HeatAwareLoadBalancerStrategy::AssessTabletMove. Delegates to
  // GlobalLoadState::SetRunningTabletCountForTest because per_ts_global_meta_ itself is private.
  void SetGlobalRunningTabletCountForTest(const TabletServerId& ts_uuid, int count) {
    global_state_->SetRunningTabletCountForTest(ts_uuid, count);
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

  // Phase 4: seed the per-tserver follower-write heat aggregate directly. Production populates
  // it by fanning heat_by_tablet_ across running peers inside AggregateLeaderHeatIntoGlobalState;
  // tests that want to exercise PlacementHeat / CompareLoad / AssessTabletMove without also
  // having to construct a fully-seeded heat_by_tablet_ + consistent replica map can use this to
  // shortcut the aggregate directly. Counterpart of SetHeatForTest.
  void SetTabletHeatByTsForTest(const TabletServerId& ts_uuid, double aggregate) {
    global_state_->tablet_heat_by_ts_[ts_uuid] = aggregate;
  }

  // Read-only accessor for the per-tserver follower-write heat aggregate, mirroring
  // GetHeatForTest. Tests use this to assert the projections applied by
  // ProjectReplicaAddIntoGlobalState / ProjectReplicaRemoveIntoGlobalState landed correctly.
  double GetTabletHeatByTsForTest(const TabletServerId& ts_uuid) const {
    const auto it = global_state_->tablet_heat_by_ts_.find(ts_uuid);
    return it == global_state_->tablet_heat_by_ts_.end() ? 0.0 : it->second;
  }

  // Phase 4: direct entry points for the tablet-move cooldown. Mirror
  // RecordHeatAwareLeaderMoveForTest / IsInHeatAwareCooldownForTest so tests can exercise the
  // cooldown's (tablet, from, to) key granularity without threading a full balancer run.
  void RecordHeatAwareTabletMoveForTest(
      const TabletId& tablet_id, const TabletServerId& from_ts, const TabletServerId& to_ts) {
    RecordHeatAwareTabletMove(tablet_id, from_ts, to_ts);
  }

  bool IsInHeatAwareTabletMoveCooldownForTest(
      const TabletId& tablet_id, const TabletServerId& from_ts,
      const TabletServerId& to_ts) {
    return IsInHeatAwareTabletMoveCooldown(tablet_id, from_ts, to_ts);
  }

  // Direct entry points for the per-replica heat projections, symmetric to
  // ProjectLeaderHeatMoveForTest. Lets tests exercise the projection math without having to
  // reproduce the full AddOrMoveReplica / RemoveReplica control flow.
  void ProjectReplicaAddForTest(const TabletId& tablet_id, const TabletServerId& ts) {
    ProjectReplicaAddIntoGlobalState(tablet_id, ts);
  }
  void ProjectReplicaRemoveForTest(const TabletId& tablet_id, const TabletServerId& ts) {
    ProjectReplicaRemoveIntoGlobalState(tablet_id, ts);
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

  // Phase 4: mirror of GetHeatAwareCooldownSizeForTest for the tablet-move cooldown map.
  // Tests use this to assert that placement-repair / wrong-placement tablet adds never populate
  // the map (is_heat_driven_tablet_move defaults to false on those paths).
  size_t GetHeatAwareTabletMoveCooldownSizeForTest() const {
    return heat_aware_recent_tablet_moves_.size();
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

  // Phase 4: construct per_run_state_ over the fixture's tablet_map_. Production does this at
  // the top of RunClusterBalancerWithOptions; the mocked harness drives AnalyzeTablets directly
  // and so leaves per_run_state_ nullptr by default. Tests that need
  // IsInHeatAwareTabletMoveCooldown or AggregateLeaderHeatIntoGlobalState to see the catalog's
  // live replica map (e.g. stranded-add eviction, replication-heat aggregation) call this first.
  void SetupPerRunStateForTest() {
    per_run_state_ = std::make_unique<PerRunState>(tablet_map_);
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
