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

#include "yb/tserver/heartbeater.h"

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "yb/common/common_flags.h"
#include "yb/common/hybrid_time.h"
#include "yb/common/version_info.h"
#include "yb/common/wire_protocol.h"

#include "yb/master/master_heartbeat.proxy.h"
#include "yb/master/master_rpc.h"
#include "yb/master/master_types.pb.h"

#include "yb/rpc/rpc_fwd.h"

#include "yb/server/hybrid_clock.h"

#include "yb/tserver/master_leader_poller.h"
#include "yb/tserver/service_util.h"
#include "yb/tserver/tablet_server.h"
#include "yb/tserver/ts_tablet_manager.h"
#include "yb/tserver/tserver_cgroup_manager.h"

#include "yb/util/cgroups.h"
#include "yb/util/logging.h"
#include "yb/util/memory/arena.h"
#include "yb/util/monotime.h"
#include "yb/util/net/net_util.h"
#include "yb/util/slice.h"
#include "yb/util/status.h"
#include "yb/util/status_format.h"

using namespace std::literals;

DEFINE_RUNTIME_int32(heartbeat_rpc_timeout_ms, 15000,
    "Timeout used for the TS->Master heartbeat RPCs.");
TAG_FLAG(heartbeat_rpc_timeout_ms, advanced);

DEFINE_RUNTIME_int32(heartbeat_interval_ms, 1000,
    "Interval at which the TS heartbeats to the master.");
TAG_FLAG(heartbeat_interval_ms, advanced);

DEFINE_UNKNOWN_int32(heartbeat_max_failures_before_backoff, 3,
             "Maximum number of consecutive heartbeat failures until the "
             "Tablet Server backs off to the normal heartbeat interval, "
             "rather than retrying.");
TAG_FLAG(heartbeat_max_failures_before_backoff, advanced);

DEFINE_test_flag(bool, tserver_disable_heartbeat, false, "Should heartbeat be disabled");
DEFINE_test_flag(bool, tserver_disable_catalog_refresh_on_heartbeat, false,
    "When set, disable trigger of catalog cache refresh from tserver-master heartbeat path.");

DECLARE_bool(enable_db_history_retention_pins);
DECLARE_bool(skip_fields_moved_to_xcluster_guarded_info);

using yb::master::GetLeaderMasterRpc;
using yb::rpc::RpcController;
using std::shared_ptr;
using std::vector;

namespace yb::tserver {

class HeartbeatPoller : public MasterLeaderPollerInterface {
 public:
  HeartbeatPoller(
      TabletServer& server, MasterLeaderFinder& finder,
      std::vector<std::unique_ptr<HeartbeatDataProvider>>&& data_providers);
  ~HeartbeatPoller() override = default;
  Status Poll() override;
  MonoDelta IntervalToNextPoll(int32_t consecutive_failures) override;
  void Init() override;
  void ResetProxy() override;
  std::string name() override;
  const std::string& LogPrefix() const override;

 private:
  Status TryHeartbeat();
  Status SetupRegistration(master::TSRegistrationPB* reg);
  void SetupCommonField(master::LWTSToMasterCommonPB* common);
  MonoDelta GetMinimumHeartbeatMillis(int32_t consecutive_failures) const;

  TabletServer& server_;
  MasterLeaderFinder& finder_;
  std::optional<master::MasterHeartbeatProxy> proxy_;

  // Roundtrip time of previous heartbeat to yb-master.
  MonoDelta heartbeat_rtt_ = MonoDelta::kZero;

  // What the most recent successful heartbeat response asked for.
  bool needs_reregister_ = false;
  bool needs_full_tablet_report_ = false;

  // Full reports can take multiple heartbeats.
  // This field has value of the sequence number of the first heartbeat in this scenario.
  std::optional<int32> full_report_seq_no_;

  std::vector<std::unique_ptr<HeartbeatDataProvider>> data_providers_;
};

class Heartbeater::Impl {
 public:
  Impl(
      const TabletServerOptions& opts, TabletServer& server,
      std::vector<std::unique_ptr<HeartbeatDataProvider>>&& data_providers);
  Impl(const Impl& other) = delete;
  void operator=(const Impl& other) = delete;

  Status Start();
  void Shutdown();
  void TriggerASAP();

  void set_master_addresses(server::MasterAddressesPtr master_addresses) {
    VLOG_WITH_PREFIX(1) << "Setting master addresses to " << yb::ToString(master_addresses);
    finder_.set_master_addresses(std::move(master_addresses));
  }

  HostPort get_master_leader_hostport() { return finder_.get_master_leader_hostport(); }

 private:
  const std::string& LogPrefix() const { return server_.LogPrefix(); }

  TabletServer& server_;
  MasterLeaderFinder finder_;
  std::unique_ptr<HeartbeatPoller> poller_;
  MasterLeaderPollScheduler poll_scheduler_;
};

////////////////////////////////////////////////////////////
// Heartbeater
////////////////////////////////////////////////////////////

Heartbeater::Heartbeater(
    const TabletServerOptions& opts, TabletServer* server,
    std::vector<std::unique_ptr<HeartbeatDataProvider>>&& data_providers)
    : impl_(std::make_unique<Impl>(opts, *server, std::move(data_providers))) {
}

Heartbeater::~Heartbeater() {
  Shutdown();
}

Status Heartbeater::Start() { return impl_->Start(); }

void Heartbeater::Shutdown() {
  return impl_->Shutdown();
}

void Heartbeater::TriggerASAP() { impl_->TriggerASAP(); }

void Heartbeater::set_master_addresses(server::MasterAddressesPtr master_addresses) {
  impl_->set_master_addresses(std::move(master_addresses));
}

HostPort Heartbeater::get_master_leader_hostport() {
  return impl_->get_master_leader_hostport();
}


////////////////////////////////////////////////////////////
// Heartbeater::Impl
////////////////////////////////////////////////////////////

Heartbeater::Impl::Impl(
    const TabletServerOptions& opts, TabletServer& server,
    std::vector<std::unique_ptr<HeartbeatDataProvider>>&& data_providers)
    : server_(server),
      finder_(server.messenger(), server.proxy_cache(), opts.GetMasterAddresses()),
      poller_(std::make_unique<HeartbeatPoller>(server_, finder_, std::move(data_providers))),
      poll_scheduler_(finder_, *poller_.get()) {
  auto master_addresses = CHECK_NOTNULL(finder_.get_master_addresses());
  CHECK(!master_addresses->empty());
  VLOG_WITH_PREFIX(1) << "Initializing heartbeater thread with master addresses: "
                      << AsString(master_addresses);
}

Status Heartbeater::Impl::Start() {
  Cgroup* cgroup = nullptr;
#ifdef __linux__
  if (server_.cgroup_manager()) {
    cgroup = server_.cgroup_manager()->SystemHighCgroup();
  }
#endif
  return poll_scheduler_.Start(cgroup);
}

void Heartbeater::Impl::Shutdown() {
  poll_scheduler_.Shutdown();
}

void Heartbeater::Impl::TriggerASAP() {
  poll_scheduler_.TriggerASAP();
}

const std::string& HeartbeatDataProvider::LogPrefix() const {
  return server_.LogPrefix();
}

void PeriodicalHeartbeatDataProvider::AddData(
    bool needs_full_tablet_report, master::LWTSHeartbeatRequestPB* req) {
  // Save that fact that we will need to send a full report the next time we run.
  needs_full_tablet_report_ |= needs_full_tablet_report;

  if (prev_run_time_ + Period() < CoarseMonoClock::Now()) {
    DoAddData(needs_full_tablet_report_, req);
    prev_run_time_ = CoarseMonoClock::Now();
    needs_full_tablet_report_ = false;
  }
}

////////////////////////////////////////////////////////////
// HeartbeatPoller
////////////////////////////////////////////////////////////

HeartbeatPoller::HeartbeatPoller(
    TabletServer& server, MasterLeaderFinder& finder,
    std::vector<std::unique_ptr<HeartbeatDataProvider>>&& data_providers)
    : server_(server), finder_(finder), data_providers_(std::move(data_providers)) {}

Status HeartbeatPoller::Poll() {
  if (PREDICT_FALSE(server_.fail_heartbeats_for_tests())) {
    return STATUS(IOError, "failing all heartbeats for tests");
  }

  if (PREDICT_FALSE(FLAGS_TEST_tserver_disable_heartbeat)) {
    YB_LOG_EVERY_N_SECS(INFO, 1) << "Heartbeat disabled for testing.";
    return Status::OK();
  }

  if (!proxy_) {
    VLOG_WITH_PREFIX(1) << "No valid master proxy. Connecting...";
    proxy_ = VERIFY_RESULT(finder_.CreateProxy<master::MasterHeartbeatProxy>(
        MonoDelta::FromMilliseconds(FLAGS_heartbeat_rpc_timeout_ms)));
  }

  for (;;) {
    auto status = TryHeartbeat();
    if (!status.ok() && status.IsTryAgain()) {
      continue;
    }
    return status;
  }

  return Status::OK();
}

MonoDelta HeartbeatPoller::IntervalToNextPoll(int32_t consecutive_failures) {
  if (full_report_seq_no_ || needs_reregister_ || needs_full_tablet_report_) {
    return GetMinimumHeartbeatMillis(consecutive_failures);
  }

  return MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms);
}

void HeartbeatPoller::Init() {
  // Config the "last heartbeat response" to indicate that we need to register
  // -- since we've never registered before, we know this to be true.
  needs_reregister_ = true;

  // Have the Master request a full tablet report on 2nd HB.
  needs_full_tablet_report_ = false;
}

void HeartbeatPoller::ResetProxy() { proxy_ = std::nullopt; }

std::string HeartbeatPoller::name() {
  return "heartbeat";
}

const std::string& HeartbeatPoller::LogPrefix() const { return server_.LogPrefix(); }

Status HeartbeatPoller::TryHeartbeat() {
  ThreadSafeArena arena;
  master::LWTSHeartbeatRequestPB req(&arena);

  SetupCommonField(req.mutable_common());
  if (needs_reregister_) {
    LOG_WITH_PREFIX(INFO) << "Registering TS with master...";
    master::TSRegistrationPB registration;
    RETURN_NOT_OK_PREPEND(SetupRegistration(&registration), "Unable to set up registration");
    req.mutable_registration()->CopyFrom(registration);
  }

  auto& tablet_report = *req.mutable_tablet_report();
  if (needs_full_tablet_report_) {
    LOG_WITH_PREFIX(INFO) << "Sending a full tablet report to master...";
    server_.tablet_manager()->StartFullTabletReport(&tablet_report);
    full_report_seq_no_ = tablet_report.sequence_number();
    tablet_report.set_full_report_seq_no(*full_report_seq_no_);
  } else {
    server_.tablet_manager()->GenerateTabletReport(
        &tablet_report, !full_report_seq_no_ /* include_bootstrap */);
    if (full_report_seq_no_) {
      LOG_WITH_PREFIX(INFO) << "Continuing full tablet report to master...";
      tablet_report.set_full_report_seq_no(*full_report_seq_no_);
    } else {
      VLOG_WITH_PREFIX(2) << "Sending an incremental tablet report to master...";
    }
  }

  auto universe_uuid = VERIFY_RESULT(
      server_.fs_manager()->GetUniverseUuidFromTserverInstanceMetadata());
  if (!universe_uuid.empty()) {
    req.dup_universe_uuid(universe_uuid);
  }

  tablet_report.set_is_incremental(!full_report_seq_no_);
  req.set_num_live_tablets(server_.tablet_manager()->GetNumLiveTablets());
  req.set_leader_count(server_.tablet_manager()->GetLeaderCount());
  {
    auto fingerprint = server_.GetCatalogVersionsFingerprint();
    if (fingerprint.has_value()) {
      req.set_ysql_db_catalog_versions_fingerprint(*fingerprint);
    }
  }

  // Load local database history retention pins onto heartbeat for
  // master to aggregate and calculate global history retention pins
  if (FLAGS_enable_db_history_retention_pins) {
    for (const auto& [db_oid, pin] : server_.GetYsqlDbOldestPinnedReadTimes()) {
      auto* entry = req.add_ts_ysql_db_oldest_pinned_read_times();
      entry->set_key(db_oid);
      entry->mutable_value()->set_db_level_oldest_read_time(pin.ToPB());
    }
  }

  RETURN_NOT_OK(server_.XClusterPopulateMasterHeartbeatRequest(req, needs_full_tablet_report_));

  for (auto& data_provider : data_providers_) {
    data_provider->AddData(needs_full_tablet_report_, &req);
  }

  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_heartbeat_rpc_timeout_ms));

  req.set_config_index(server_.GetCurrentMasterIndex());
  req.set_cluster_config_version(server_.cluster_config_version());
  auto result = server_.XClusterConfigVersion();
  if (result.ok()) {
    req.set_xcluster_config_version(*result);
  } else if (!result.status().IsNotFound()) {
    return result.status();
  }
  req.set_rtt_us(heartbeat_rtt_.ToMicroseconds());
  if (server_.has_faulty_drive()) {
    req.set_faulty_drive(true);
  }

  // Include the hybrid time of this tablet server in the heartbeat.
  auto* hybrid_clock = dynamic_cast<server::HybridClock*>(server_.Clock());
  HybridTime heartbeat_send_time;
  if (hybrid_clock) {
    heartbeat_send_time = hybrid_clock->Now();
    req.set_ts_hybrid_time(heartbeat_send_time.ToUint64());
    // Also include the physical clock time of this tablet server in the heartbeat.
    Result<PhysicalTime> now = hybrid_clock->physical_clock()->Now();
    if (!now.ok()) {
      YB_LOG_EVERY_N_SECS(WARNING, 10) << "Failed to read clock: " << now.status();
      req.set_ts_physical_time(0);
    } else {
      req.set_ts_physical_time(now->time_point);
    }
  } else {
    req.set_ts_hybrid_time(0);
    req.set_ts_physical_time(0);
  }

  req.set_auto_flags_config_version(server_.GetAutoFlagConfigVersion());
  if (server_.SkipHeartbeatTServerList()) {
    req.set_skip_tservers(true);
  }

  master::LWTSHeartbeatResponsePB resp(&arena);
  {
    VLOG_WITH_PREFIX(2) << "Sending heartbeat:\n" << req.ShortDebugString();
    heartbeat_rtt_ = MonoDelta::kZero;
    MonoTime start_time = MonoTime::Now();
    RETURN_NOT_OK_PREPEND(proxy_->TSHeartbeat(req, &resp, &rpc),
                          "Failed to send heartbeat");
    MonoTime end_time = MonoTime::Now();
    if (!resp.universe_uuid().empty()) {
      RETURN_NOT_OK(server_.ValidateAndMaybeSetUniverseUuid(
          VERIFY_RESULT(UniverseUuid::FromString(resp.universe_uuid().ToBuffer()))));
    }

    if (resp.has_error()) {
      switch (resp.error().code()) {
        case master::MasterErrorPB::NOT_THE_LEADER: {
          DCHECK(!resp.leader_master());
          // Treat a not-the-leader error code as leader_master=false.
          if (resp.leader_master()) {
            LOG_WITH_PREFIX(WARNING) << "Setting leader master to false for "
                                    << resp.error().code() << " code.";
            resp.set_leader_master(false);
          }
          break;
        }
        default:
          auto status = StatusFromPB(resp.error().status());
          if (resp.is_fatal_error()) {
            // yb-master has requested us to terminate the process immediately.
            LOG(FATAL) << "Unable to join universe: " << status;
          }
          return status;
      }
    }

    VLOG_WITH_PREFIX(2) << "Received heartbeat response:\n" << resp.ShortDebugString();
    if (resp.has_master_config()) {
      LOG_WITH_PREFIX(INFO) << "Received heartbeat response with config "
                            << resp.ShortDebugString();

      RETURN_NOT_OK(server_.UpdateMasterAddresses(
          resp.master_config().ToGoogleProtobuf(), resp.leader_master()));
    }

    if (!resp.leader_master()) {
      // If the master is no longer a leader, reset proxy so that we can
      // determine the master and attempt to heartbeat during in the
      // next heartbeat interval.
      proxy_.reset();
      return STATUS_FORMAT(
          ServiceUnavailable, "Master is no longer the leader: $0",
          resp.error().ShortDebugString());
    }

    // Check for a universe key registry for encryption.
    if (resp.has_universe_key_registry()) {
      RETURN_NOT_OK(server_.SetUniverseKeyRegistry(
          resp.universe_key_registry().ToGoogleProtobuf()));
    }

    if (!resp.has_xcluster_guarded_info() && !FLAGS_skip_fields_moved_to_xcluster_guarded_info) {
      // To handle upgrades promoting auto flag skip_fields_moved_to_xcluster_guarded_info, before
      // the flag is promoted we apply (here and in XClusterHandleMasterHeartbeatResponse below) the
      // now deprecated fields that were moved into XClusterGuardedInfoPB unless the new
      // XClusterGuardedInfoPB structure is present.  That is, we never apply both a deprecated
      // field and its replacement in xcluster_guarded_info from the same heartbeat response.
      //
      // Put another way, during the upgrade master may be sending both the deprecated fields and
      // the xcluster_guarded_info or just the deprecated fields.  We want to apply only one of
      // them, giving preference to the new xcluster_guarded_info if present.
      server_.UpdateOidCacheInvalidationsCount(resp.deprecated_oid_cache_invalidations_count());
    }

    RETURN_NOT_OK(server_.ClusterConfigHandleMasterHeartbeatResponse(resp));

    // Heartbeat responses requesting re-registration are sent before master fills in any
    // xCluster-guarded information (or a lease), so keep our current copy and lease, if any.
    if (!resp.needs_reregister()) {
      // Compute the xCluster-guarded information lease expiration from the duration granted by the
      // master, anchored to the MonoTime recorded when this heartbeat request was sent.
      MonoTime lease_expiration_time;
      if (resp.has_xcluster_guarded_lease_duration_ms()) {
        lease_expiration_time =
            start_time + MonoDelta::FromMilliseconds(resp.xcluster_guarded_lease_duration_ms());
      }

      // Update the xCluster-guarded information lease after updating our copy of the
      // information with fresh info.  This ensures that when a TServer (re-)acquires a lease, it
      // has xCluster-guarded information at least as current as when that lease was
      // issued.
      if (resp.has_xcluster_guarded_info()) {
        server_.ApplyXClusterGuardedInfoIfNewer(resp.xcluster_guarded_info().ToGoogleProtobuf());
      }
      RETURN_NOT_OK(server_.XClusterHandleMasterHeartbeatResponse(resp, lease_expiration_time));
    }

    if (resp.leader_blacklisted_tservers_with_no_leaders_size() > 0) {
      // Mark the tservers as followers if they are leader-blacklisted. When the tserver gets
      // removed from the blacklist, we don't receive any reset signal from the master. The
      // meta cache would eventually figure out when a peer sends the latest consensus config
      // or it makes a GatTabletLocations call to the master.
      std::vector<std::string> blacklisted_uuids;
      for (const auto& uuid : resp.leader_blacklisted_tservers_with_no_leaders()) {
        blacklisted_uuids.push_back(uuid.ToBuffer());
      }
      server_.MarkTServersAsFollowers(blacklisted_uuids);
    }

    // At this point we know resp is a successful heartbeat response from the master.
    needs_reregister_ = resp.needs_reregister();
    needs_full_tablet_report_ = resp.needs_full_tablet_report();
    heartbeat_rtt_ = end_time.GetDeltaSince(start_time);
  }

  for (auto& data_provider : data_providers_) {
    data_provider->OnHeartbeatResponse(resp);
  }

  if (resp.has_cluster_uuid() && !resp.cluster_uuid().empty()) {
    server_.set_cluster_uuid(resp.cluster_uuid().ToBuffer());
  }

  // The Master responds with the max entries for a single Tablet Report to avoid overwhelming it.
  if (resp.has_tablet_report_limit()) {
    server_.tablet_manager()->SetReportLimit(resp.tablet_report_limit());
  }

  if (resp.needs_full_tablet_report()) {
    return STATUS(TryAgain, "");
  }

  // Handle TSHeartbeatResponsePB (e.g. tablets ack'd by master as processed)
  bool all_processed = req.tablet_report().remaining_tablet_count() == 0 &&
                       !resp.tablet_report().processing_truncated();
  server_.tablet_manager()->MarkTabletReportAcknowledged(
      req.tablet_report().sequence_number(), resp.tablet_report(), all_processed);

  // Trigger another heartbeat ASAP if we didn't process all tablets on this request.
  if (all_processed) {
    full_report_seq_no_.reset();
  }

  // Applied even when the response does not carry the field, which an old master's never does:
  // the default is UNKNOWN, so masters rolled back to a version that does not report load stop
  // the pacing instead of freezing it at the last level the new ones reported.
  server_.SetYsqlCatalogPrefetchLoad(resp.ysql_catalog_prefetch_load());

  // Update the master's YSQL catalog version (i.e. if there were schema changes for YSQL objects).
  if (FLAGS_enable_ysql &&
      PREDICT_TRUE(!FLAGS_TEST_tserver_disable_catalog_refresh_on_heartbeat)) {
    // In per-db catalog version mode (the only supported mode) we never use ysql_catalog_version.
    DCHECK(!resp.has_ysql_catalog_version());
    if (resp.has_db_catalog_version_data()) {
      if (FLAGS_log_ysql_catalog_versions) {
        VLOG_WITH_FUNC(1) << "got master db catalog version data: "
                          << resp.db_catalog_version_data().ShortDebugString()
                          << " db inval messages: "
                          << tserver::CatalogInvalMessagesDataDebugString(resp);
      }
      if (FLAGS_ysql_yb_enable_invalidation_messages) {
        if (resp.has_db_catalog_inval_messages_data()) {
          server_.SetYsqlDBCatalogVersionsWithInvalMessages(
              resp.db_catalog_version_data().ToGoogleProtobuf(),
              resp.db_catalog_inval_messages_data().ToGoogleProtobuf());
        } else {
          server_.SetYsqlDBCatalogVersions(resp.db_catalog_version_data().ToGoogleProtobuf());
          // If we only have catalog versions but not invalidation messages, it means that the last
          // heartbeat response was only able to read pg_yb_catalog_version, but the reading of
          // pg_yb_invalidation_messages failed. Clear the fingerprint so that next heartbeat
          // can read pg_yb_invalidation_messages again.
          server_.ResetCatalogVersionsFingerprint();
        }
      } else {
        server_.SetYsqlDBCatalogVersions(resp.db_catalog_version_data().ToGoogleProtobuf());
      }
    } else {
      // The master does not pass back any catalog versions. This can happen in
      // several cases:
      // * The fingerprints matched at master side which we assume tserver and
      //   master have identical catalog versions.
      // * If catalog versions cache is used, the cache is empty, either becuase it
      //   has never been populated yet, or because master reading of the table
      //   pg_yb_catalog_version has failed so the cache is cleared.
      // * If catalog versions cache is not used, master reading of the table
      //   pg_yb_catalog_version has failed, this is an unexpected case that is
      //   ignored by the heartbeat service at the master side.
      VLOG_WITH_FUNC(2) << "got no master catalog version data";
    }
  } else if (resp.has_ysql_catalog_version()) {
    // YSQL is disabled on this tserver but the master sent back the legacy single shared
    // ysql_catalog_version field; record it so the tserver still tracks it.
    DCHECK(!resp.has_db_catalog_version_data());
    if (FLAGS_log_ysql_catalog_versions) {
      VLOG_WITH_FUNC(1) << "got master catalog version: "
                        << resp.ysql_catalog_version()
                        << ", breaking version: "
                        << (resp.has_ysql_last_breaking_catalog_version()
                            ? Format("$0", resp.ysql_last_breaking_catalog_version())
                            : "(none)");
    }
    if (resp.has_ysql_last_breaking_catalog_version()) {
      server_.SetYsqlCatalogVersion(resp.ysql_catalog_version(),
                                     resp.ysql_last_breaking_catalog_version());
    } else {
      /* Assuming all changes are breaking if last breaking version not explicitly set. */
      server_.SetYsqlCatalogVersion(resp.ysql_catalog_version(),
                                     resp.ysql_catalog_version());
    }
  }

  RETURN_NOT_OK(server_.tablet_manager()->UpdateSnapshotsInfo(
      resp.snapshots_info().ToGoogleProtobuf()));

  server_.UpdateClusterYsqlDbOldestPinnedReadTimes(resp);

  if (resp.has_transaction_tables_version()) {
    server_.UpdateTransactionTablesVersion(resp.transaction_tables_version());
  }

  std::optional<AutoFlagsConfigPB> new_config;
  if (resp.has_auto_flags_config()) {
    new_config = resp.auto_flags_config().ToGoogleProtobuf();
  }
  server_.HandleMasterHeartbeatResponse(heartbeat_send_time, std::move(new_config));

  // Update the live tserver list, unless this tserver keeps it from the connectivity check.
  if (req.skip_tservers()) {
    return Status::OK();
  }
  google::protobuf::RepeatedPtrField<master::TSInformationPB> tservers;
  for (const auto& ts_info : resp.tservers()) {
    ts_info.ToGoogleProtobuf(tservers.Add());
  }
  return server_.PopulateLiveTServers(tservers);
}

Status HeartbeatPoller::SetupRegistration(master::TSRegistrationPB* reg) {
  reg->Clear();
  RETURN_NOT_OK(server_.GetRegistration(reg->mutable_common()));
  auto* resources = reg->mutable_resources();
  resources->set_core_count(NumEffectiveCPUs());
  int64_t tablet_overhead_limit = yb::tserver::ComputeTabletOverheadLimit();
  if (tablet_overhead_limit > 0) {
    resources->set_tablet_overhead_ram_in_bytes(tablet_overhead_limit);
  }
  VersionInfo::GetVersionInfoPB(reg->mutable_version_info());
  return Status::OK();
}

void HeartbeatPoller::SetupCommonField(master::LWTSToMasterCommonPB* common) {
  common->mutable_ts_instance()->CopyFrom(server_.instance_pb());
}

MonoDelta HeartbeatPoller::GetMinimumHeartbeatMillis(int32_t consecutive_failures) const {
  // If we've failed a few heartbeats in a row, back off to the normal
  // interval, rather than retrying in a loop.
  if (consecutive_failures == FLAGS_heartbeat_max_failures_before_backoff) {
    LOG_WITH_PREFIX(WARNING) << "Failed " << consecutive_failures << " heartbeats "
                             << "in a row: no longer allowing fast heartbeat attempts.";
  }
  return consecutive_failures > FLAGS_heartbeat_max_failures_before_backoff
             ? MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms)
             : MonoDelta::kZero;
}

}  // namespace yb::tserver
