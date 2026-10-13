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
// master-load-bench: reproduces a production-shaped yb-master leader workload on one host.
//
// One real yb-master, one real yb-tserver with YSQL (the only SQL ingress; it hosts the
// transaction status table and no user tablets), and N "hollow" tservers in this process that
// register, heartbeat, report tablets and ack tablet-level RPCs, so every user tablet the schema
// creates lands on them. A --scenario file drives the run: sql / sql_file lines go through ysqlsh
// to the real tserver, sleep paces, parallel << >> overlaps directives. Hollow tservers register
// with the production cloud/region/zone names from --topology, so dumps load as given; any
// editing of a dump is master_load_bench/prepare.py's job. --snapshot_dir saves the master and
// tserver data roots plus the hollow tablet registry on exit; --from_snapshot restores them, so
// one preload serves many scenarios. The real tserver's master traffic flows through a TCP relay
// whose captured heartbeats are the hollow fleet's payload templates (mirror mode).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cctype>
#include <deque>
#include <functional>
#include <fstream>
#include <map>
#include <mutex>
#include <set>
#include <thread>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>

#include "yb/client/client.h"
#include "yb/client/schema.h"
#include "yb/client/table_alterer.h"
#include "yb/client/yb_table_name.h"

#include "yb/common/common_net.pb.h"
#include "yb/common/entity_ids.h"
#include "yb/common/read_hybrid_time.h"
#include "yb/common/hybrid_time.h"
#include "yb/common/transaction.h"
#include "yb/common/wire_protocol.h"

#include "yb/consensus/metadata.pb.h"

#include "yb/gutil/casts.h"
#include "yb/gutil/stringprintf.h"
#include "yb/gutil/strings/escaping.h"
#include "yb/gutil/strings/split.h"

#include "yb/master/master_cluster.proxy.h"
#include "yb/master/master_defaults.h"
#include "yb/master/master_heartbeat.proxy.h"
#include "yb/master/master_ysql_lease.proxy.h"

#include "yb/rpc/messenger.h"
#include "yb/rpc/proxy.h"
#include "yb/rpc/remote_method.h"
#include "yb/rpc/rpc_controller.h"
#include "yb/rpc/rpc_header.pb.h"
#include "yb/rpc/service_pool.h"
#include "yb/rpc/sidecars.h"
#include "yb/rpc/thread_pool.h"
#include "yb/rpc/yb_rpc.h"

#include "yb/server/clock.h"
#include "yb/server/server_base.service.h"

#include "yb/tserver/tserver_admin.service.h"
#include "yb/tserver/tserver_service.service.h"

#include "yb/util/curl_util.h"
#include "yb/util/env.h"
#include "yb/util/faststring.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/logging.h"
#include "yb/util/metrics.h"
#include "yb/util/monotime.h"
#include "yb/util/net/net_util.h"
#include "yb/util/path_util.h"
#include "yb/util/result.h"
#include "yb/util/slice.h"
#include "yb/util/status.h"
#include "yb/util/status_format.h"
#include "yb/util/status_log.h"
#include "yb/util/stol_utils.h"
#include "yb/util/subprocess.h"
#include "yb/util/write_buffer.h"

DEFINE_NON_RUNTIME_int32(num_tservers, 100,
    "Number of hollow tservers to run, spread over the --topology zones by their percentages.");
DEFINE_NON_RUNTIME_string(topology, "",
    "Zone file (required): one 'cloud region zone percent' per line, '#' comments; percentages "
    "add up to 100 (or 1). Hollow tservers register with these placements so a dump's "
    "tablespaces resolve as-is. master_load_bench/prepare.py generates it from the dump.");
DEFINE_NON_RUNTIME_int32(replication_factor, 7,
    "Cluster default replication factor for tables that name no tablespace.");
DEFINE_NON_RUNTIME_string(sql_file, "",
    "Comma-separated SQL files to load in order through ysqlsh once the cluster is up, run "
    "exactly as given; e.g. roles, tablespaces dump, database dump.");
DEFINE_NON_RUNTIME_string(scenario, "",
    "Optional directive file run after --sql_file: sql <stmt> | sql << ... >> | "
    "sql_file <path> | sleep <dur> | database <name> | parallel << ... >>. '#' starts a comment.");
DEFINE_NON_RUNTIME_bool(resume, false,
    "Reuse --data_dir from the previous run as-is: master and tserver data are kept, the hollow "
    "tablet registry is reloaded, placement and transactions-table setup are skipped. The fleet "
    "(--topology, --num_tservers, --replication_factor) comes from the saved run.");
DEFINE_NON_RUNTIME_string(snapshot_dir, "",
    "On exit, stop master and tserver and copy --data_dir's data roots and hollow tablet "
    "registry here.");
DEFINE_NON_RUNTIME_string(from_snapshot, "",
    "Copy a --snapshot_dir into --data_dir first, then behave like --resume.");
DEFINE_NON_RUNTIME_int32(run_seconds, 0,
    "Stay up this long after loading, tservers heartbeating; 0 = until SIGINT/SIGTERM.");
DEFINE_NON_RUNTIME_int32(heartbeat_interval_ms, 1000, "Hollow tserver heartbeat interval.");
DEFINE_NON_RUNTIME_int32(full_metrics_beat_period, 5,
    "Send the tablet metrics payload (storage_metadata + leader_info) every Nth heartbeat, "
    "matching the real tserver's 5s metrics cadence.");
DEFINE_NON_RUNTIME_bool(send_lease_refreshes, true,
    "Send the per-second RefreshYsqlLease RPC from every hollow tserver.");
DEFINE_NON_RUNTIME_int32(connectivity_check_interval_ms, 60000,
    "ListTabletServers interval per hollow tserver, matching the real tserver's connectivity "
    "poller.");
DEFINE_NON_RUNTIME_string(master_bin, "", "Path to the yb-master binary (required).");
DEFINE_NON_RUNTIME_string(tserver_bin, "", "Path to the yb-tserver binary (required).");
DEFINE_NON_RUNTIME_string(ysqlsh_bin, "",
    "Path to ysqlsh; default <tserver_bin dir>/../postgres/bin/ysqlsh.");
DEFINE_NON_RUNTIME_string(data_dir, "",
    "Scratch directory for master and tserver data and logs (required). A snapshot restores "
    "only into the data_dir it was taken from.");
DEFINE_NON_RUNTIME_string(csv_path, "",
    "Append long-format samples to this CSV file; empty = summary on stdout only.");
DEFINE_NON_RUNTIME_int32(master_rpc_port, 21100, "RPC port for the spawned master.");
DEFINE_NON_RUNTIME_int32(master_web_port, 21101, "Webserver port for the spawned master.");
DEFINE_NON_RUNTIME_string(master_flags, "",
    "Comma-separated extra --flag=value arguments passed to yb-master.");
DEFINE_NON_RUNTIME_string(master_cpus, "",
    "Pin the spawned yb-master to these CPUs via taskset -c (Linux only); empty = no pinning.");
DEFINE_NON_RUNTIME_int32(relay_port, 21102,
    "Listen port of the capture relay between the real tserver and the master.");
DEFINE_NON_RUNTIME_int32(tserver_rpc_port, 21103, "RPC port for the spawned real tserver.");
DEFINE_NON_RUNTIME_int32(tserver_web_port, 21104, "Webserver port for the spawned real tserver.");
DEFINE_NON_RUNTIME_int32(pg_port, 21105, "YSQL port of the spawned real tserver.");
DEFINE_NON_RUNTIME_string(tserver_flags, "",
    "Comma-separated extra --flag=value arguments passed to yb-tserver.");
DEFINE_NON_RUNTIME_int32(metrics_interval_sec, 5, "Sampling interval for the collector.");
DEFINE_NON_RUNTIME_int32(register_timeout_sec, 300,
    "Timeout for all tservers to become live on the master, and for the transactions table.");
DEFINE_NON_RUNTIME_int32(connections_to_master, 0,
    "Outbound connections from the driver to the master; 0 = one per hollow tserver, "
    "approximating the per-tserver connections of a real cluster.");
DEFINE_NON_RUNTIME_int32(driver_reactors, 8, "Reactor threads in the driver messenger.");
DEFINE_NON_RUNTIME_string(metric_substrings,
    "handler_latency_yb_master_,"
    "rpcs_in_queue_,"
    "rpc_incoming_queue_time,"
    "rpcs_queue_overflow,"
    "rpcs_timed_out_in_queue,"
    "rpcs_timed_out_early_in_queue,"
    "service_request_bytes_yb_master_,"
    "service_response_bytes_yb_master_",
    "Master prometheus metrics to collect: comma-separated substrings; matching series are "
    "summed per metric name.");

METRIC_DECLARE_entity(server);

using std::string;
using namespace std::literals;

namespace yb {
namespace tools {

namespace {

// The real tserver's placement zone; hollow tservers use zone0..zone{num_zones-1}, so placement
// keeps real and hollow replicas out of each other's Raft groups.
std::atomic<bool> g_stop_requested{false};

void HandleStopSignal(int) {
  g_stop_requested.store(true, std::memory_order_release);
}

const char* const kRealTServerZone = "zone-real";
const char* const kPlacementCloud = "cloud1";
const char* const kPlacementRegion = "region1";
const char* const kMeasuredPhase = "steady";
const char* const kSnapshotMeta = "meta.txt";
const char* const kSnapshotTablets = "hollow_tablets.txt";
const char* const kMasterDataDir = "master-data";
const char* const kTServerDataDir = "ts-data";

CloudInfoPB MakeCloudInfo(const string& zone) {
  CloudInfoPB cloud;
  cloud.set_placement_cloud(kPlacementCloud);
  cloud.set_placement_region(kPlacementRegion);
  cloud.set_placement_zone(zone);
  return cloud;
}

// One fake tserver: identity, hosted tablet replicas, and heartbeat/report/lease state.
class HollowTServer {
 public:
  HollowTServer(int index, uint64_t instance_seqno, CloudInfoPB cloud_info)
      : index_(index),
        uuid_(StringPrintf("%032x", index + 1)),
        instance_seqno_(instance_seqno),
        cloud_info_(std::move(cloud_info)) {}

  const CloudInfoPB& cloud_info() const { return cloud_info_; }

  const string& uuid() const { return uuid_; }
  int index() const { return index_; }

  void SetHostPort(const HostPort& host_port) { host_port_ = host_port; }
  const HostPort& host_port() const { return host_port_; }

  void AddTablet(
      const string& tablet_id, const consensus::RaftConfigPB& config,
      uint32_t schema_version = 0) {
    // All replicas receive the same config, so hashing the tablet id picks the same
    // leader everywhere and the master sees a consistent consensus state.
    const auto& leader_uuid =
        config.peers(std::hash<string>()(tablet_id) % config.peers_size()).permanent_uuid();
    std::lock_guard l(mutex_);
    auto& info = tablets_[tablet_id];
    info.config = config;
    info.leader_uuid = leader_uuid;
    info.schema_version = schema_version;
    dirty_.insert(tablet_id);
  }

  // The master re-sends AlterSchema while any replica reports an older version than the
  // table's, so the next heartbeat must carry the new one.
  void SetSchemaVersion(const string& tablet_id, uint32_t schema_version) {
    std::lock_guard l(mutex_);
    auto it = tablets_.find(tablet_id);
    if (it == tablets_.end()) {
      return;
    }
    it->second.schema_version = std::max(it->second.schema_version, schema_version);
    dirty_.insert(tablet_id);
  }

  bool HasTablet(const string& tablet_id) {
    std::lock_guard l(mutex_);
    return tablets_.contains(tablet_id);
  }

  std::vector<string> PeerUuids(const string& tablet_id) {
    std::vector<string> uuids;
    std::lock_guard l(mutex_);
    auto it = tablets_.find(tablet_id);
    if (it != tablets_.end()) {
      for (const auto& peer : it->second.config.peers()) {
        uuids.push_back(peer.permanent_uuid());
      }
    }
    return uuids;
  }

  void RemoveTablet(const string& tablet_id) {
    std::lock_guard l(mutex_);
    tablets_.erase(tablet_id);
    dirty_.erase(tablet_id);
    removed_.insert(tablet_id);
  }

  size_t NumTablets() {
    std::lock_guard l(mutex_);
    return tablets_.size();
  }

  // Registered and no report in flight: the master's view of this tserver is current.
  bool ReportsSettled() {
    std::lock_guard l(mutex_);
    return hb_seq_ > 1 && !needs_registration_ && !registration_in_flight_ &&
           !full_report_active_ && dirty_.empty();
  }

  struct RegistryEntry {
    consensus::RaftConfigPB config;
    uint32_t schema_version;
  };

  void CollectTablets(std::map<string, RegistryEntry>* out) {
    std::lock_guard l(mutex_);
    for (const auto& [tablet_id, info] : tablets_) {
      out->emplace(tablet_id, RegistryEntry{info.config, info.schema_version});
    }
  }

  // mirror_tmpl, when set, is a captured real-tserver request used as the payload; the metrics
  // shape then follows metrics_override instead of the hb_seq_ cadence.
  void FillHeartbeat(const string& universe_uuid, const master::TSHeartbeatRequestPB* mirror_tmpl,
                     std::optional<bool> metrics_override, master::TSHeartbeatRequestPB* req) {
    req->Clear();
    std::lock_guard l(mutex_);
    const bool metrics_beat = metrics_override.has_value()
        ? *metrics_override
        : hb_seq_ % FLAGS_full_metrics_beat_period == 0;
    ++hb_seq_;
    // Registration and full reports need synthetic state the template can't carry.
    if (mirror_tmpl != nullptr && !needs_registration_ && !full_report_active_) {
      FillFromTemplate(*mirror_tmpl, universe_uuid, req);
      return;
    }

    auto* instance = req->mutable_common()->mutable_ts_instance();
    instance->set_permanent_uuid(uuid_);
    instance->set_instance_seqno(instance_seqno_);
    if (!universe_uuid.empty()) {
      req->set_universe_uuid(universe_uuid);
    }
    const auto now_micros = GetCurrentTimeMicros();
    req->set_ts_physical_time(now_micros);
    req->set_ts_hybrid_time(static_cast<uint64_t>(now_micros) << 12);

    req->set_rtt_us(last_rtt_us_);
    if (cluster_config_version_ >= 0) {
      req->set_cluster_config_version(narrow_cast<int32_t>(cluster_config_version_));
    }
    if (auto_flags_config_version_ >= 0) {
      req->set_auto_flags_config_version(narrow_cast<uint32_t>(auto_flags_config_version_));
    }

    if (needs_registration_) {
      auto* reg = req->mutable_registration()->mutable_common();
      auto* addr = reg->add_private_rpc_addresses();
      addr->set_host(host_port_.host());
      addr->set_port(host_port_.port());
      *reg->add_broadcast_addresses() = *addr;
      *reg->mutable_cloud_info() = cloud_info_;
    }

    FillTabletReport(req);

    int leader_count = 0;
    for (const auto& [_, info] : tablets_) {
      if (info.leader_uuid == uuid_) {
        ++leader_count;
      }
    }
    req->set_num_live_tablets(narrow_cast<int32_t>(tablets_.size()));
    req->set_leader_count(leader_count);

    if (metrics_beat) {
      constexpr uint64_t kSstBytes = 64ULL << 20;
      constexpr uint64_t kWalBytes = 16ULL << 20;
      auto* metrics = req->mutable_metrics();
      metrics->set_total_sst_file_size(tablets_.size() * kSstBytes);
      metrics->set_uncompressed_sst_file_size(tablets_.size() * kSstBytes * 3 / 2);
      metrics->set_num_sst_files(tablets_.size() * 3);
      metrics->set_total_ram_usage(1ULL << 30);
      metrics->set_read_ops_per_sec(0);
      metrics->set_write_ops_per_sec(0);
      metrics->set_uptime_seconds(hb_seq_ * FLAGS_heartbeat_interval_ms / 1000);
      auto* path_metric = metrics->add_path_metrics();
      path_metric->set_path_id(Format("/data/hollow-ts-$0", index_));
      path_metric->set_used_space(tablets_.size() * (kSstBytes + kWalBytes));
      path_metric->set_total_space(1ULL << 40);
      for (const auto& [tablet_id, info] : tablets_) {
        auto* storage = req->add_storage_metadata();
        storage->set_tablet_id(tablet_id);
        storage->set_sst_file_size(kSstBytes);
        storage->set_wal_file_size(kWalBytes);
        storage->set_uncompressed_sst_file_size(kSstBytes * 3 / 2);
        storage->set_total_size(kSstBytes + kWalBytes);
        storage->set_vector_index_size(0);
        storage->set_has_active_vector_index_backfill(false);
        storage->set_may_have_orphaned_post_split_data(false);
        // The real provider adds leader_info for every hosted tablet, not just led ones;
        // followers report NO_MAJORITY_REPLICATED_LEASE without an expiration.
        auto* leader = req->add_leader_info();
        leader->set_tablet_id(tablet_id);
        if (info.leader_uuid == uuid_) {
          leader->set_leader_lease_status(consensus::LeaderLeaseStatus::HAS_LEASE);
          leader->set_ht_lease_expiration((now_micros + 2 * 1000 * 1000) << 12);
        } else {
          leader->set_leader_lease_status(
              consensus::LeaderLeaseStatus::NO_MAJORITY_REPLICATED_LEASE);
        }
        auto* compaction = req->add_full_compaction_statuses();
        compaction->set_tablet_id(tablet_id);
        compaction->set_full_compaction_state(tablet::IDLE);
        compaction->set_last_full_compaction_time(0);
      }
    }
  }

  void HandleHeartbeatSuccess(const master::TSHeartbeatResponsePB& resp, int64_t rtt_us) {
    std::lock_guard l(mutex_);
    last_rtt_us_ = rtt_us;
    if (registration_in_flight_) {
      needs_registration_ = false;
      registration_in_flight_ = false;
    }
    if (resp.needs_reregister()) {
      needs_registration_ = true;
    }
    if (resp.has_cluster_config_version()) {
      cluster_config_version_ = resp.cluster_config_version();
    }
    if (resp.has_auto_flags_config()) {
      auto_flags_config_version_ = resp.auto_flags_config().config_version();
    }
    if (resp.has_tablet_report_limit()) {
      tablet_report_limit_ = resp.tablet_report_limit();
    }
    if (resp.has_tablet_report()) {
      for (const auto& update : resp.tablet_report().tablets()) {
        dirty_.erase(update.tablet_id());
      }
    }
    if (resp.needs_full_tablet_report()) {
      full_report_queue_.clear();
      for (const auto& [tablet_id, _] : tablets_) {
        full_report_queue_.push_back(tablet_id);
      }
      full_report_active_ = true;
      full_report_started_ = false;
    }
  }

  void HandleHeartbeatFailure() {
    std::lock_guard l(mutex_);
    registration_in_flight_ = false;
    // A lost chunk desyncs the master's report sequence; start the full report over and let
    // sequence-number checks on the master sort out duplicates.
    if (full_report_active_) {
      full_report_queue_.clear();
      for (const auto& [tablet_id, _] : tablets_) {
        full_report_queue_.push_back(tablet_id);
      }
      full_report_started_ = false;
    }
  }

  std::optional<uint64_t> lease_epoch() {
    std::lock_guard l(mutex_);
    return lease_epoch_;
  }

  void SetLeaseEpoch(uint64_t epoch) {
    std::lock_guard l(mutex_);
    lease_epoch_ = epoch;
  }

  uint64_t instance_seqno() const { return instance_seqno_; }

  // Scheduler-thread-only state.
  MonoTime next_heartbeat;
  MonoTime next_lease_refresh;
  MonoTime next_connectivity_check;
  MonoTime last_metrics_beat;
  MonoTime heartbeat_sent_time;
  MonoTime lease_sent_time;
  std::atomic<bool> heartbeat_in_flight{false};
  std::atomic<bool> lease_in_flight{false};
  std::atomic<bool> connectivity_in_flight{false};

  // Reused across calls; safe because at most one of each RPC is in flight.
  master::TSHeartbeatRequestPB hb_req;
  master::TSHeartbeatResponsePB hb_resp;
  rpc::RpcController hb_controller;
  master::RefreshYsqlLeaseRequestPB lease_req;
  master::RefreshYsqlLeaseResponsePB lease_resp;
  rpc::RpcController lease_controller;
  master::ListTabletServersRequestPB connectivity_req;
  master::ListTabletServersResponsePB connectivity_resp;
  rpc::RpcController connectivity_controller;
  std::unique_ptr<master::MasterHeartbeatProxy> heartbeat_proxy;
  std::unique_ptr<master::MasterYsqlLeaseProxy> lease_proxy;
  std::unique_ptr<master::MasterClusterProxy> cluster_proxy;

 private:
  struct TabletInfo {
    consensus::RaftConfigPB config;
    string leader_uuid;
    uint32_t schema_version = 0;
  };

  // Rewrites a captured real-tserver heartbeat to carry this hollow tserver's identity, tablets
  // and report state; every other field stays byte-faithful to the real request.
  void FillFromTemplate(
      const master::TSHeartbeatRequestPB& tmpl, const string& universe_uuid,
      master::TSHeartbeatRequestPB* req) REQUIRES(mutex_) {
    req->CopyFrom(tmpl);
    auto* instance = req->mutable_common()->mutable_ts_instance();
    instance->set_permanent_uuid(uuid_);
    instance->set_instance_seqno(instance_seqno_);
    req->clear_registration();
    if (!universe_uuid.empty()) {
      req->set_universe_uuid(universe_uuid);
    }
    const auto now_micros = GetCurrentTimeMicros();
    req->set_ts_physical_time(now_micros);
    req->set_ts_hybrid_time(static_cast<uint64_t>(now_micros) << 12);
    req->set_rtt_us(last_rtt_us_);
    if (cluster_config_version_ >= 0) {
      req->set_cluster_config_version(narrow_cast<int32_t>(cluster_config_version_));
    } else {
      req->clear_cluster_config_version();
    }
    if (auto_flags_config_version_ >= 0) {
      req->set_auto_flags_config_version(narrow_cast<uint32_t>(auto_flags_config_version_));
    } else {
      req->clear_auto_flags_config_version();
    }

    const bool tmpl_had_report = req->has_tablet_report();
    req->clear_tablet_report();
    FillTabletReport(req);
    if (!req->has_tablet_report() && tmpl_had_report) {
      // The real tserver attaches a report on every beat; keep the shape.
      auto* report = req->mutable_tablet_report();
      report->set_is_incremental(true);
      report->set_sequence_number(report_seqno_++);
      report->set_remaining_tablet_count(0);
    }

    int leader_count = 0;
    for (const auto& [_, info] : tablets_) {
      if (info.leader_uuid == uuid_) {
        ++leader_count;
      }
    }
    req->set_num_live_tablets(narrow_cast<int32_t>(tablets_.size()));
    req->set_leader_count(leader_count);

    // Per-tablet sections must list our tablet ids; entries are cloned from the template
    // round-robin so per-entry content and sizes stay real.
    const auto tmpl_storage = req->storage_metadata();
    const auto tmpl_compactions = req->full_compaction_statuses();
    const auto tmpl_leaders = req->leader_info();
    req->clear_storage_metadata();
    req->clear_full_compaction_statuses();
    req->clear_leader_info();
    if (tmpl_storage.empty() && tmpl_compactions.empty() && tmpl_leaders.empty()) {
      return;
    }
    int leader_tmpl_idx = -1;
    for (int j = 0; j < tmpl_leaders.size(); ++j) {
      if (tmpl_leaders.Get(j).leader_lease_status() == consensus::LeaderLeaseStatus::HAS_LEASE) {
        leader_tmpl_idx = j;
        break;
      }
    }
    int i = 0;
    for (const auto& [tablet_id, info] : tablets_) {
      if (!tmpl_storage.empty()) {
        auto* storage = req->add_storage_metadata();
        *storage = tmpl_storage.Get(i % tmpl_storage.size());
        storage->set_tablet_id(tablet_id);
      }
      if (!tmpl_compactions.empty()) {
        auto* compaction = req->add_full_compaction_statuses();
        *compaction = tmpl_compactions.Get(i % tmpl_compactions.size());
        compaction->set_tablet_id(tablet_id);
      }
      if (!tmpl_leaders.empty()) {
        auto* leader = req->add_leader_info();
        if (info.leader_uuid == uuid_ && leader_tmpl_idx >= 0) {
          *leader = tmpl_leaders.Get(leader_tmpl_idx);
        } else {
          leader->set_leader_lease_status(
              consensus::LeaderLeaseStatus::NO_MAJORITY_REPLICATED_LEASE);
        }
        leader->set_tablet_id(tablet_id);
      }
      ++i;
    }
  }

  void FillTabletReport(master::TSHeartbeatRequestPB* req) REQUIRES(mutex_) {
    const int32_t limit = tablet_report_limit_;
    if (full_report_active_) {
      auto* report = req->mutable_tablet_report();
      report->set_is_incremental(false);
      const int32_t seq = report_seqno_++;
      if (!full_report_started_) {
        full_report_first_seqno_ = seq;
        full_report_started_ = true;
      }
      report->set_sequence_number(seq);
      report->set_full_report_seq_no(full_report_first_seqno_);
      int32_t added = 0;
      while (!full_report_queue_.empty() && added < limit) {
        const auto it = tablets_.find(full_report_queue_.front());
        full_report_queue_.pop_front();
        if (it == tablets_.end()) {
          continue;
        }
        AddReportedTablet(it->first, it->second, report);
        ++added;
      }
      report->set_remaining_tablet_count(narrow_cast<int32_t>(full_report_queue_.size()));
      if (full_report_queue_.empty()) {
        full_report_active_ = false;
        full_report_started_ = false;
        // The full report supersedes any pending incremental updates.
        dirty_.clear();
        removed_.clear();
      }
      return;
    }
    if (dirty_.empty() && removed_.empty() && sent_initial_report_) {
      return;
    }
    auto* report = req->mutable_tablet_report();
    report->set_is_incremental(true);
    report->set_sequence_number(report_seqno_++);
    int32_t added = 0;
    for (const auto& tablet_id : dirty_) {
      if (added >= limit) {
        break;
      }
      const auto it = tablets_.find(tablet_id);
      if (it == tablets_.end()) {
        continue;
      }
      AddReportedTablet(it->first, it->second, report);
      ++added;
    }
    for (const auto& tablet_id : removed_) {
      report->add_removed_tablet_ids(tablet_id);
    }
    removed_.clear();
    report->set_remaining_tablet_count(
        narrow_cast<int32_t>(dirty_.size() > static_cast<size_t>(added)
                                 ? dirty_.size() - added : 0));
    sent_initial_report_ = true;
  }

  void AddReportedTablet(
      const string& tablet_id, const TabletInfo& info,
      master::TabletReportPB* report) REQUIRES(mutex_) {
    auto* tablet = report->add_updated_tablets();
    tablet->set_tablet_id(tablet_id);
    tablet->set_state(tablet::RUNNING);
    tablet->set_tablet_data_state(tablet::TABLET_DATA_READY);
    tablet->set_schema_version(info.schema_version);
    auto* cstate = tablet->mutable_committed_consensus_state();
    cstate->set_current_term(1);
    cstate->set_leader_uuid(info.leader_uuid);
    *cstate->mutable_config() = info.config;
  }

  const int index_;
  const string uuid_;
  const uint64_t instance_seqno_;
  const CloudInfoPB cloud_info_;
  HostPort host_port_;

  std::mutex mutex_;
  std::map<string, TabletInfo> tablets_ GUARDED_BY(mutex_);
  std::set<string> dirty_ GUARDED_BY(mutex_);
  std::set<string> removed_ GUARDED_BY(mutex_);
  std::deque<string> full_report_queue_ GUARDED_BY(mutex_);
  bool full_report_active_ GUARDED_BY(mutex_) = false;
  bool full_report_started_ GUARDED_BY(mutex_) = false;
  int32_t full_report_first_seqno_ GUARDED_BY(mutex_) = 0;
  int32_t report_seqno_ GUARDED_BY(mutex_) = 0;
  bool sent_initial_report_ GUARDED_BY(mutex_) = false;
  bool needs_registration_ GUARDED_BY(mutex_) = true;
  bool registration_in_flight_ GUARDED_BY(mutex_) = false;
  int32_t tablet_report_limit_ GUARDED_BY(mutex_) = 1000;
  int64_t cluster_config_version_ GUARDED_BY(mutex_) = -1;
  int64_t auto_flags_config_version_ GUARDED_BY(mutex_) = -1;
  std::optional<uint64_t> lease_epoch_ GUARDED_BY(mutex_);
  int64_t last_rtt_us_ GUARDED_BY(mutex_) = 0;
  uint64_t hb_seq_ GUARDED_BY(mutex_) = 0;

 public:
  bool MarkRegistrationInFlight() {
    std::lock_guard l(mutex_);
    if (needs_registration_) {
      registration_in_flight_ = true;
      return true;
    }
    return false;
  }
};

class HollowTServerRegistry {
 public:
  void Add(std::unique_ptr<HollowTServer> ts) {
    by_uuid_[ts->uuid()] = ts.get();
    tservers_.push_back(std::move(ts));
  }

  HollowTServer* Find(const string& uuid) const {
    auto it = by_uuid_.find(uuid);
    return it == by_uuid_.end() ? nullptr : it->second;
  }

  const std::vector<std::unique_ptr<HollowTServer>>& tservers() const { return tservers_; }

 private:
  std::vector<std::unique_ptr<HollowTServer>> tservers_;
  std::unordered_map<string, HollowTServer*> by_uuid_;
};

// Admin service shared by all hollow tservers; routes by dest_uuid.
class HollowTSAdminService : public tserver::TabletServerAdminServiceIf {
 public:
  HollowTSAdminService(
      const scoped_refptr<MetricEntity>& entity, HollowTServerRegistry* registry)
      : tserver::TabletServerAdminServiceIf(entity), registry_(registry) {}

  void CreateTablet(
      const tserver::CreateTabletRequestPB* req, tserver::CreateTabletResponsePB* resp,
      rpc::RpcContext context) override {
    auto* ts = registry_->Find(req->dest_uuid());
    if (!ts) {
      context.RespondFailure(STATUS_FORMAT(NotFound, "Unknown dest_uuid $0", req->dest_uuid()));
      return;
    }
    ts->AddTablet(req->tablet_id(), req->config());
    context.RespondSuccess();
  }

  void DeleteTablet(
      const tserver::DeleteTabletRequestPB* req, tserver::DeleteTabletResponsePB* resp,
      rpc::RpcContext context) override {
    auto* ts = registry_->Find(req->dest_uuid());
    if (ts) {
      ts->RemoveTablet(req->tablet_id());
    }
    context.RespondSuccess();
  }

  // Schema changes have no data to apply; recording the version is all the master checks. A
  // real leader replicates it through Raft, so here it is applied to every peer in-process.
  void AlterSchema(
      const tablet::ChangeMetadataRequestPB* req, tserver::ChangeMetadataResponsePB* resp,
      rpc::RpcContext context) override {
    ChangeMetadata(*req, resp, &context);
  }

  void BackfillDone(
      const tablet::ChangeMetadataRequestPB* req, tserver::ChangeMetadataResponsePB* resp,
      rpc::RpcContext context) override {
    ChangeMetadata(*req, resp, &context);
  }

  void GetSafeTime(
      const tserver::GetSafeTimeRequestPB* req, tserver::GetSafeTimeResponsePB* resp,
      rpc::RpcContext context) override {
    if (!FindTablet(req->dest_uuid(), req->tablet_id(), resp, &context)) {
      return;
    }
    const auto now = Now();
    resp->set_safe_time(std::max(now, HybridTime(req->min_hybrid_time_for_backfill())).ToUint64());
    resp->set_propagated_hybrid_time(now.ToUint64());
    context.RespondSuccess();
  }

  // No rows, so the chunk is complete: empty backfilled_until means the tablet is done.
  void BackfillIndex(
      const tserver::BackfillIndexRequestPB* req, tserver::BackfillIndexResponsePB* resp,
      rpc::RpcContext context) override {
    if (!FindTablet(req->dest_uuid(), req->tablet_id(), resp, &context)) {
      return;
    }
    resp->set_num_rows_read_from_table_for_backfill(0);
    resp->set_propagated_hybrid_time(Now().ToUint64());
    context.RespondSuccess();
  }

  // No YSQL backends here, so none can lag.
  void WaitForYsqlBackendsCatalogVersion(
      const tserver::WaitForYsqlBackendsCatalogVersionRequestPB*,
      tserver::WaitForYsqlBackendsCatalogVersionResponsePB* resp,
      rpc::RpcContext context) override {
    resp->set_num_lagging_backends(0);
    context.RespondSuccess();
  }

#define HOLLOW_UNSUPPORTED_METHOD(Method, ReqType, RespType) \
  void Method(const ReqType*, RespType*, rpc::RpcContext context) override { \
    context.RespondFailure(STATUS(NotSupported, #Method " not implemented by hollow tserver")); \
  }

  HOLLOW_UNSUPPORTED_METHOD(PrepareDeleteTransactionTablet,
                            tserver::PrepareDeleteTransactionTabletRequestPB,
                            tserver::PrepareDeleteTransactionTabletResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(FlushTablets, tserver::FlushTabletsRequestPB,
                            tserver::FlushTabletsResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(CountIntents, tserver::CountIntentsRequestPB,
                            tserver::CountIntentsResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(AddTableToTablet, tserver::AddTableToTabletRequestPB,
                            tserver::AddTableToTabletResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(RemoveTableFromTablet, tserver::RemoveTableFromTabletRequestPB,
                            tserver::RemoveTableFromTabletResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(SplitTablet, tablet::SplitTabletRequestPB,
                            tserver::SplitTabletResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(UpgradeYsql, tserver::UpgradeYsqlRequestPB,
                            tserver::UpgradeYsqlResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(TestRetry, tserver::TestRetryRequestPB, tserver::TestRetryResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(UpdateTransactionTablesVersion,
                            tserver::UpdateTransactionTablesVersionRequestPB,
                            tserver::UpdateTransactionTablesVersionResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ApplyXClusterGuardedInfoIfNewer,
                            tserver::ApplyXClusterGuardedInfoIfNewerRequestPB,
                            tserver::ApplyXClusterGuardedInfoIfNewerResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(CloneTablet, tablet::CloneTabletRequestPB,
                            tserver::CloneTabletResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ClonePgSchema, tserver::ClonePgSchemaRequestPB,
                            tserver::ClonePgSchemaResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(EnableDbConns, tserver::EnableDbConnsRequestPB,
                            tserver::EnableDbConnsResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetPgSocketDir, tserver::GetPgSocketDirRequestPB,
                            tserver::GetPgSocketDirResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetActiveRbsInfo, tserver::GetActiveRbsInfoRequestPB,
                            tserver::GetActiveRbsInfoResponsePB)
#undef HOLLOW_UNSUPPORTED_METHOD

 private:
  static HybridTime Now() { return HybridTime::FromMicros(GetCurrentTimeMicros()); }

  template <class Resp>
  static void RespondTabletNotFound(
      const string& dest_uuid, const string& tablet_id, Resp* resp, rpc::RpcContext* context) {
    StatusToPB(STATUS_FORMAT(NotFound, "Tablet $0 not on tserver $1", tablet_id, dest_uuid),
               resp->mutable_error()->mutable_status());
    resp->mutable_error()->set_code(tserver::TabletServerErrorPB::TABLET_NOT_FOUND);
    context->RespondSuccess();
  }

  template <class Resp>
  HollowTServer* FindTablet(
      const string& dest_uuid, const string& tablet_id, Resp* resp, rpc::RpcContext* context) {
    auto* ts = registry_->Find(dest_uuid);
    if (!ts) {
      context->RespondFailure(STATUS_FORMAT(NotFound, "Unknown dest_uuid $0", dest_uuid));
      return nullptr;
    }
    if (!ts->HasTablet(tablet_id)) {
      RespondTabletNotFound(dest_uuid, tablet_id, resp, context);
      return nullptr;
    }
    return ts;
  }

  void ChangeMetadata(
      const tablet::ChangeMetadataRequestPB& req, tserver::ChangeMetadataResponsePB* resp,
      rpc::RpcContext* context) {
    auto* ts = FindTablet(req.dest_uuid(), req.tablet_id(), resp, context);
    if (!ts) {
      return;
    }
    if (req.has_schema_version()) {
      for (const auto& uuid : ts->PeerUuids(req.tablet_id())) {
        if (auto* peer = registry_->Find(uuid)) {
          peer->SetSchemaVersion(req.tablet_id(), req.schema_version());
        }
      }
    }
    resp->set_propagated_hybrid_time(Now().ToUint64());
    context->RespondSuccess();
  }

  HollowTServerRegistry* registry_;
};

// Answers the tserver connectivity poller's Ping so hollow tservers look reachable, as real
// peers would. One registration covers the fleet: every hollow port shares the driver
// messenger, and service dispatch is per-messenger, not per-socket.
// Data path. Hollow tablets hold no rows, so a scan returns an empty result; everything else
// (writes included) fails at once rather than retrying until the statement times out.
class HollowTSService : public tserver::TabletServerServiceIf {
 public:
  HollowTSService(const scoped_refptr<MetricEntity>& entity, HollowTServerRegistry* registry)
      : tserver::TabletServerServiceIf(entity), registry_(registry) {}

  void Read(
      const tserver::LWReadRequestPB* req, tserver::LWReadResponsePB* resp,
      rpc::RpcContext context) override {
    const auto tablet_id = req->tablet_id().ToBuffer();
    bool hosted = false;
    for (const auto& ts : registry_->tservers()) {
      if (ts->HasTablet(tablet_id)) {
        hosted = true;
        break;
      }
    }
    if (!hosted) {
      StatusToPB(STATUS_FORMAT(NotFound, "Tablet $0 not on any hollow tserver", tablet_id),
                 resp->mutable_error()->mutable_status());
      resp->mutable_error()->set_code(tserver::TabletServerErrorPB::TABLET_NOT_FOUND);
      context.RespondSuccess();
      return;
    }
    // One response per request; the rows sidecar is the int64 row count the real read writes
    // first, here zero and nothing after it.
    for (size_t i = 0; i < req->pgsql_batch().size(); ++i) {
      static const char kZeroRows[sizeof(int64_t)] = {};
      context.sidecars().Start().Append(kZeroRows, sizeof(kZeroRows));
      auto* pgsql = resp->add_pgsql_batch();
      pgsql->set_status(PgsqlResponsePB::PGSQL_STATUS_OK);
      pgsql->set_rows_data_sidecar(narrow_cast<int32_t>(context.sidecars().Complete()));
    }
    const auto now = HybridTime::FromMicros(GetCurrentTimeMicros());
    if (!req->has_read_time()) {
      ReadHybridTime::SingleTime(now).ToPB(resp->mutable_used_read_time());
    }
    resp->set_propagated_hybrid_time(now.ToUint64());
    context.RespondSuccess();
  }

#define HOLLOW_UNSUPPORTED_METHOD(Method, ReqType, RespType) \
  void Method(const ReqType*, RespType*, rpc::RpcContext context) override { \
    context.RespondFailure(STATUS(NotSupported, #Method " not implemented by hollow tserver")); \
  }

  HOLLOW_UNSUPPORTED_METHOD(Write, tserver::LWWriteRequestPB, tserver::LWWriteResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(WaitForAsyncWrite, tserver::WaitForAsyncWriteRequestPB,
                            tserver::WaitForAsyncWriteResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(VerifyTableRowRange, tserver::VerifyTableRowRangeRequestPB,
                            tserver::VerifyTableRowRangeResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(NoOp, tserver::NoOpRequestPB, tserver::NoOpResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ListTablets, tserver::ListTabletsRequestPB,
                            tserver::ListTabletsResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetLogLocation, tserver::GetLogLocationRequestPB,
                            tserver::GetLogLocationResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(Checksum, tserver::ChecksumRequestPB, tserver::ChecksumResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ListTabletsForTabletServer,
                            tserver::ListTabletsForTabletServerRequestPB,
                            tserver::ListTabletsForTabletServerResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ImportData, tserver::ImportDataRequestPB, tserver::ImportDataResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(UpdateTransaction, tserver::UpdateTransactionRequestPB,
                            tserver::UpdateTransactionResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetTransactionStatus, tserver::GetTransactionStatusRequestPB,
                            tserver::GetTransactionStatusResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetOldTransactions, tserver::GetOldTransactionsRequestPB,
                            tserver::GetOldTransactionsResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetOldSingleShardWaiters, tserver::GetOldSingleShardWaitersRequestPB,
                            tserver::GetOldSingleShardWaitersResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetTransactionStatusAtParticipant,
                            tserver::GetTransactionStatusAtParticipantRequestPB,
                            tserver::GetTransactionStatusAtParticipantResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(AbortTransaction, tserver::AbortTransactionRequestPB,
                            tserver::AbortTransactionResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(UpdateTransactionWaitingForStatus,
                            tserver::UpdateTransactionWaitingForStatusRequestPB,
                            tserver::UpdateTransactionWaitingForStatusResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ProbeTransactionDeadlock, tserver::ProbeTransactionDeadlockRequestPB,
                            tserver::ProbeTransactionDeadlockResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(Truncate, tserver::TruncateRequestPB, tserver::TruncateResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetTabletStatus, tserver::GetTabletStatusRequestPB,
                            tserver::GetTabletStatusResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetMasterAddresses, tserver::GetMasterAddressesRequestPB,
                            tserver::GetMasterAddressesResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(Publish, tserver::PublishRequestPB, tserver::PublishResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(IsTabletServerReady, tserver::IsTabletServerReadyRequestPB,
                            tserver::IsTabletServerReadyResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetSplitKey, tserver::GetSplitKeyRequestPB,
                            tserver::GetSplitKeyResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetTserverCatalogVersionInfo,
                            tserver::GetTserverCatalogVersionInfoRequestPB,
                            tserver::GetTserverCatalogVersionInfoResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ListMasterServers, tserver::ListMasterServersRequestPB,
                            tserver::ListMasterServersResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetLockStatus, tserver::GetLockStatusRequestPB,
                            tserver::GetLockStatusResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetObjectLockStatus, tserver::GetObjectLockStatusRequestPB,
                            tserver::GetObjectLockStatusResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(CancelTransaction, tserver::CancelTransactionRequestPB,
                            tserver::CancelTransactionResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetCompatibleSchemaVersion,
                            tserver::GetCompatibleSchemaVersionRequestPB,
                            tserver::GetCompatibleSchemaVersionResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(StartRemoteSnapshotTransfer,
                            tserver::StartRemoteSnapshotTransferRequestPB,
                            tserver::StartRemoteSnapshotTransferResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetTabletKeyRanges, tserver::GetTabletKeyRangesRequestPB,
                            tserver::GetTabletKeyRangesResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(CheckTserverTabletHealth, tserver::CheckTserverTabletHealthRequestPB,
                            tserver::CheckTserverTabletHealthResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ClearAllMetaCachesOnServer,
                            tserver::ClearAllMetaCachesOnServerRequestPB,
                            tserver::ClearAllMetaCachesOnServerResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ClearMetacache, tserver::ClearMetacacheRequestPB,
                            tserver::ClearMetacacheResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ClearYCQLMetaDataCacheOnServer,
                            tserver::ClearYCQLMetaDataCacheOnServerRequestPB,
                            tserver::ClearYCQLMetaDataCacheOnServerResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ClearUniverseUuid, tserver::ClearUniverseUuidRequestPB,
                            tserver::ClearUniverseUuidResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(AcquireObjectLocks, tserver::AcquireObjectLockRequestPB,
                            tserver::AcquireObjectLockResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ReleaseObjectLocks, tserver::ReleaseObjectLockRequestPB,
                            tserver::ReleaseObjectLockResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(WaitForLockersMultiple, tserver::WaitForLockersMultipleRequestPB,
                            tserver::WaitForLockersMultipleResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetMetrics, tserver::GetMetricsRequestPB, tserver::GetMetricsResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(AdminExecutePgsql, tserver::AdminExecutePgsqlRequestPB,
                            tserver::AdminExecutePgsqlResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetLocalPgTxnSnapshot, tserver::GetLocalPgTxnSnapshotRequestPB,
                            tserver::GetLocalPgTxnSnapshotResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetTserverCatalogMessageLists,
                            tserver::GetTserverCatalogMessageListsRequestPB,
                            tserver::GetTserverCatalogMessageListsResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(TriggerRelcacheInitConnection,
                            tserver::TriggerRelcacheInitConnectionRequestPB,
                            tserver::TriggerRelcacheInitConnectionResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(PgRemoteExec, tserver::PgRemoteExecRequestPB,
                            tserver::PgRemoteExecResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(CollectYsqlCallHomeStats, tserver::CollectYsqlCallHomeStatsRequestPB,
                            tserver::CollectYsqlCallHomeStatsResponsePB)
#undef HOLLOW_UNSUPPORTED_METHOD

  // The generated interface declares these four with a Result-returning signature.
#define HOLLOW_UNSUPPORTED_RESULT_METHOD(method) \
  Result<tserver::method##ResponsePB> method( \
      const tserver::method##RequestPB& req, CoarseTimePoint deadline) override { \
    return STATUS(NotSupported, #method " not implemented by hollow tserver"); \
  }
  HOLLOW_UNSUPPORTED_RESULT_METHOD(GetYSQLLeaseInfo)
  HOLLOW_UNSUPPORTED_RESULT_METHOD(VerifyVectorIndexes)
  HOLLOW_UNSUPPORTED_RESULT_METHOD(DumpTabletData)
  HOLLOW_UNSUPPORTED_RESULT_METHOD(ConnectivityState)
#undef HOLLOW_UNSUPPORTED_RESULT_METHOD

 private:
  HollowTServerRegistry* registry_;
};

class HollowGenericService : public server::GenericServiceIf {
 public:
  explicit HollowGenericService(const scoped_refptr<MetricEntity>& entity)
      : server::GenericServiceIf(entity) {}

  void Ping(const server::PingRequestPB* req, server::PingResponsePB* resp,
            rpc::RpcContext context) override {
    context.RespondSuccess();
  }

#define HOLLOW_UNSUPPORTED_METHOD(Method, ReqType, RespType) \
  void Method(const ReqType*, RespType*, rpc::RpcContext context) override { \
    context.RespondFailure(STATUS(NotSupported, #Method " not implemented by hollow tserver")); \
  }

  HOLLOW_UNSUPPORTED_METHOD(SetFlag, server::SetFlagRequestPB, server::SetFlagResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetFlag, server::GetFlagRequestPB, server::GetFlagResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ValidateFlagValue, server::ValidateFlagValueRequestPB,
                            server::ValidateFlagValueResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(RefreshFlags, server::RefreshFlagsRequestPB,
                            server::RefreshFlagsResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetAutoFlagsConfigVersion, server::GetAutoFlagsConfigVersionRequestPB,
                            server::GetAutoFlagsConfigVersionResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(FlushCoverage, server::FlushCoverageRequestPB,
                            server::FlushCoverageResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ServerClock, server::ServerClockRequestPB,
                            server::ServerClockResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetStatus, server::GetStatusRequestPB, server::GetStatusResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(ReloadCertificates, server::ReloadCertificatesRequestPB,
                            server::ReloadCertificatesResponsePB)
#undef HOLLOW_UNSUPPORTED_METHOD
};

// Shared state of mirror mode: heartbeat templates, measured cadence, and captured requests
// queued for replay, all fed by the relay's request-stream tap.
class MirrorState {
 public:
  struct ReplayItem {
    string service;
    string method;
    string body;
  };

  void OnRequest(const rpc::RequestHeader& header, const Slice& body) {
    if (!header.has_remote_method()) {
      return;
    }
    const auto& service = header.remote_method().service_name();
    const auto& method = header.remote_method().method_name();
    // Only idempotent boot-time reads are safe to resend per hollow tserver: the relay also
    // carries the bench client's own DDL, and replaying that would corrupt the catalog.
    // ListTabletServers is the real tserver's periodic connectivity check; the scheduler sends
    // that one per hollow tserver itself.
    static const std::set<string> kReplayMethods = {
        "GetMasterRegistration", "ListMasters", "GetAutoFlagsConfig",
        "GetFullUniverseKeyRegistry"};

    std::lock_guard l(mutex_);
    auto& counters = methods_[service + "." + method];
    ++counters.seen;
    if (method == "TSHeartbeat") {
      ObserveHeartbeat(body);
      return;
    }
    if (header.sidecar_offsets_size() > 0) {
      return;
    }
    if (kReplayMethods.count(method) > 0 && replay_queue_.size() < 10000) {
      replay_queue_.push_back(ReplayItem{service, method, body.ToBuffer()});
    }
  }

  std::shared_ptr<const master::TSHeartbeatRequestPB> TemplateFor(bool metrics) {
    std::lock_guard l(mutex_);
    return metrics ? metrics_template_ : light_template_;
  }

  // Milliseconds, 0 until at least two beats of the kind were seen.
  double MeasuredIntervalMs() {
    std::lock_guard l(mutex_);
    return interval_ms_;
  }

  double MeasuredMetricsPeriodMs() {
    std::lock_guard l(mutex_);
    return metrics_period_ms_;
  }

  bool PopReplayItem(ReplayItem* item) {
    std::lock_guard l(mutex_);
    if (replay_queue_.empty()) {
      return false;
    }
    *item = std::move(replay_queue_.front());
    replay_queue_.pop_front();
    return true;
  }

  void RecordReplay(const string& key, bool ok, uint64_t count = 1) {
    std::lock_guard l(mutex_);
    auto& counters = methods_[key];
    (ok ? counters.replayed_ok : counters.replayed_failed) += count;
  }

  void CountParseFailure() {
    std::lock_guard l(mutex_);
    ++parse_failures_;
  }

  void PrintSummary(std::ostream& out) {
    std::lock_guard l(mutex_);
    out << "captured_from_real_tserver: heartbeats=" << heartbeats_captured_
        << " light_templates=" << light_templates_
        << " metrics_templates=" << metrics_templates_
        << " parse_failures=" << parse_failures_
        << " measured_heartbeat_interval_ms=" << interval_ms_
        << " measured_metrics_period_ms=" << metrics_period_ms_ << "\n";
    for (const auto& [key, c] : methods_) {
      out << "replayed_rpc " << key << ": seen=" << c.seen
          << " replayed_ok=" << c.replayed_ok
          << " replayed_failed=" << c.replayed_failed << "\n";
    }
  }

 private:
  struct MethodCounters {
    uint64_t seen = 0;
    uint64_t replayed_ok = 0;
    uint64_t replayed_failed = 0;
  };

  void ObserveHeartbeat(const Slice& body) REQUIRES(mutex_) {
    master::TSHeartbeatRequestPB req;
    if (!req.ParseFromArray(body.data(), narrow_cast<int>(body.size()))) {
      ++parse_failures_;
      return;
    }
    const auto now = MonoTime::Now();
    ++heartbeats_captured_;
    if (last_heartbeat_.Initialized()) {
      Ewma(&interval_ms_, (now - last_heartbeat_).ToSeconds() * 1000.0);
    }
    last_heartbeat_ = now;
    const bool metrics = req.has_metrics();
    if (metrics) {
      if (last_metrics_heartbeat_.Initialized()) {
        Ewma(&metrics_period_ms_, (now - last_metrics_heartbeat_).ToSeconds() * 1000.0);
      }
      last_metrics_heartbeat_ = now;
    }
    // Registration and full-report beats are one-offs, not steady-state payload shapes.
    if (req.has_registration() ||
        (req.has_tablet_report() && !req.tablet_report().is_incremental())) {
      return;
    }
    auto tmpl = std::make_shared<const master::TSHeartbeatRequestPB>(std::move(req));
    if (metrics) {
      metrics_template_ = std::move(tmpl);
      ++metrics_templates_;
    } else {
      light_template_ = std::move(tmpl);
      ++light_templates_;
    }
  }

  static void Ewma(double* value, double sample) {
    *value = *value == 0 ? sample : *value * 0.8 + sample * 0.2;
  }

  std::mutex mutex_;
  std::map<string, MethodCounters> methods_ GUARDED_BY(mutex_);
  std::shared_ptr<const master::TSHeartbeatRequestPB> light_template_ GUARDED_BY(mutex_);
  std::shared_ptr<const master::TSHeartbeatRequestPB> metrics_template_ GUARDED_BY(mutex_);
  std::deque<ReplayItem> replay_queue_ GUARDED_BY(mutex_);
  uint64_t heartbeats_captured_ GUARDED_BY(mutex_) = 0;
  uint64_t light_templates_ GUARDED_BY(mutex_) = 0;
  uint64_t metrics_templates_ GUARDED_BY(mutex_) = 0;
  uint64_t parse_failures_ GUARDED_BY(mutex_) = 0;
  MonoTime last_heartbeat_ GUARDED_BY(mutex_);
  MonoTime last_metrics_heartbeat_ GUARDED_BY(mutex_);
  double interval_ms_ GUARDED_BY(mutex_) = 0;
  double metrics_period_ms_ GUARDED_BY(mutex_) = 0;
};

// Reassembles the client->master byte stream of one relay connection into RPC calls.
// Wire format: 3-byte "YB\1" preamble, then frames of 4-byte big-endian length followed by
// varint header_len, RequestHeader, varint body_len, body.
class YbCallStreamParser {
 public:
  explicit YbCallStreamParser(MirrorState* mirror) : mirror_(mirror) {}

  void Feed(const char* data, size_t size) {
    if (broken_) {
      return;
    }
    buffer_.append(data, size);
    size_t pos = 0;
    if (!preamble_done_) {
      if (buffer_.size() < 3) {
        return;
      }
      if (memcmp(buffer_.data(), "YB", 2) != 0) {
        LOG(WARNING) << "Relay tap: unexpected connection preamble; not parsing this connection";
        broken_ = true;
        return;
      }
      pos = 3;
      preamble_done_ = true;
    }
    while (buffer_.size() - pos >= 4) {
      const auto* p = pointer_cast<const uint8_t*>(buffer_.data() + pos);
      const size_t frame_size = (static_cast<size_t>(p[0]) << 24) | (p[1] << 16) |
                                (p[2] << 8) | p[3];
      if (frame_size > (64ULL << 20)) {
        LOG(WARNING) << "Relay tap: implausible frame size " << frame_size
                     << "; not parsing this connection";
        broken_ = true;
        return;
      }
      if (buffer_.size() - pos - 4 < frame_size) {
        break;
      }
      ProcessCall(pointer_cast<const uint8_t*>(buffer_.data() + pos + 4), frame_size);
      pos += 4 + frame_size;
    }
    buffer_.erase(0, pos);
  }

 private:
  void ProcessCall(const uint8_t* data, size_t size) {
    google::protobuf::io::CodedInputStream in(data, narrow_cast<int>(size));
    uint32_t header_size = 0;
    if (!in.ReadVarint32(&header_size) || header_size > size) {
      mirror_->CountParseFailure();
      return;
    }
    rpc::RequestHeader header;
    const auto limit = in.PushLimit(header_size);
    if (!header.ParseFromCodedStream(&in) || !in.ConsumedEntireMessage()) {
      mirror_->CountParseFailure();
      return;
    }
    in.PopLimit(limit);
    uint32_t body_size = 0;
    if (!in.ReadVarint32(&body_size)) {
      mirror_->CountParseFailure();
      return;
    }
    const auto offset = static_cast<size_t>(in.CurrentPosition());
    if (offset + body_size > size) {
      mirror_->CountParseFailure();
      return;
    }
    mirror_->OnRequest(header, Slice(data + offset, body_size));
  }

  MirrorState* mirror_;
  string buffer_;
  bool preamble_done_ = false;
  bool broken_ = false;
};

// Byte-level TCP relay: forwards both directions verbatim (CRCs and call ids survive) and taps
// the client->master direction for capture.
class TcpRelay {
 public:
  TcpRelay(uint16_t listen_port, uint16_t target_port, MirrorState* mirror)
      : listen_port_(listen_port), target_port_(target_port), mirror_(mirror) {}

  Status Start() {
    // A peer closing mid-write must be an error, not a fatal signal.
    signal(SIGPIPE, SIG_IGN);
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    SCHECK_GE(listen_fd_, 0, NetworkError, "socket() failed");
    int one = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    auto addr = MakeLoopbackAddr(listen_port_);
    SCHECK_EQ(
        bind(listen_fd_, pointer_cast<sockaddr*>(&addr), sizeof(addr)), 0, NetworkError,
        Format("bind(127.0.0.1:$0) failed: $1", listen_port_, strerror(errno)));
    SCHECK_EQ(listen(listen_fd_, 64), 0, NetworkError, "listen() failed");
    accept_thread_ = std::thread([this] { AcceptLoop(); });
    LOG(INFO) << "Relay listening on 127.0.0.1:" << listen_port_
              << " -> 127.0.0.1:" << target_port_;
    return Status::OK();
  }

  void Stop() {
    if (stop_.exchange(true)) {
      return;
    }
    if (accept_thread_.joinable()) {
      // Closing the listener does not reliably unblock accept() on macOS; connect to wake it.
      const int fd = socket(AF_INET, SOCK_STREAM, 0);
      if (fd >= 0) {
        auto addr = MakeLoopbackAddr(listen_port_);
        connect(fd, pointer_cast<sockaddr*>(&addr), sizeof(addr));
        close(fd);
      }
      accept_thread_.join();
    }
    if (listen_fd_ >= 0) {
      close(listen_fd_);
      listen_fd_ = -1;
    }
    {
      std::lock_guard l(mutex_);
      for (const int fd : open_fds_) {
        shutdown(fd, SHUT_RDWR);
      }
    }
    for (auto& t : pump_threads_) {
      if (t.joinable()) {
        t.join();
      }
    }
    std::lock_guard l(mutex_);
    for (const int fd : open_fds_) {
      close(fd);
    }
    open_fds_.clear();
  }

  ~TcpRelay() { Stop(); }

 private:
  static sockaddr_in MakeLoopbackAddr(uint16_t port) {
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return addr;
  }

  void AcceptLoop() {
    while (!stop_.load(std::memory_order_acquire)) {
      const int client_fd = accept(listen_fd_, nullptr, nullptr);
      if (client_fd < 0) {
        if (stop_.load(std::memory_order_acquire)) {
          return;
        }
        continue;
      }
      if (stop_.load(std::memory_order_acquire)) {
        close(client_fd);
        return;
      }
      const int upstream_fd = socket(AF_INET, SOCK_STREAM, 0);
      auto addr = MakeLoopbackAddr(target_port_);
      if (upstream_fd < 0 ||
          connect(upstream_fd, pointer_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        LOG(WARNING) << "Relay: cannot connect to master: " << strerror(errno);
        if (upstream_fd >= 0) {
          close(upstream_fd);
        }
        close(client_fd);
        continue;
      }
      std::lock_guard l(mutex_);
      open_fds_.push_back(client_fd);
      open_fds_.push_back(upstream_fd);
      pump_threads_.emplace_back(
          [this, client_fd, upstream_fd] { Pump(client_fd, upstream_fd, /* tap= */ true); });
      pump_threads_.emplace_back(
          [this, client_fd, upstream_fd] { Pump(upstream_fd, client_fd, /* tap= */ false); });
    }
  }

  void Pump(int from_fd, int to_fd, bool tap) {
    std::optional<YbCallStreamParser> parser;
    if (tap) {
      parser.emplace(mirror_);
    }
    char buf[64 * 1024];
    while (!stop_.load(std::memory_order_acquire)) {
      const auto n = read(from_fd, buf, sizeof(buf));
      if (n <= 0) {
        break;
      }
      if (parser) {
        parser->Feed(buf, static_cast<size_t>(n));
      }
      size_t written = 0;
      while (written < static_cast<size_t>(n)) {
        const auto w = write(to_fd, buf + written, static_cast<size_t>(n) - written);
        if (w <= 0) {
          shutdown(from_fd, SHUT_RDWR);
          shutdown(to_fd, SHUT_RDWR);
          return;
        }
        written += static_cast<size_t>(w);
      }
    }
    // EOF or error: tear down both directions; fds are closed centrally in Stop().
    shutdown(from_fd, SHUT_RDWR);
    shutdown(to_fd, SHUT_RDWR);
  }

  const uint16_t listen_port_;
  const uint16_t target_port_;
  MirrorState* mirror_;
  int listen_fd_ = -1;
  std::atomic<bool> stop_{false};
  std::thread accept_thread_;
  std::mutex mutex_;
  std::vector<int> open_fds_ GUARDED_BY(mutex_);
  std::vector<std::thread> pump_threads_;
};

struct BenchStats {
  std::atomic<uint64_t> heartbeats_ok{0};
  std::atomic<uint64_t> heartbeats_failed{0};
  std::atomic<uint64_t> lease_refreshes_ok{0};
  std::atomic<uint64_t> lease_refreshes_failed{0};
  std::atomic<uint64_t> connectivity_checks_ok{0};
  std::atomic<uint64_t> connectivity_checks_failed{0};
  std::atomic<uint64_t> heartbeat_rtt_us_sum{0};
  std::atomic<uint64_t> heartbeat_rtt_count{0};
};

struct Sample {
  int64_t epoch_ms;
  double elapsed_sec;
  string phase;
  string metric;
  double value;
};

Result<double> ProcessCpuSeconds(pid_t pid) {
#if defined(__linux__)
  std::ifstream stat_file(Format("/proc/$0/stat", pid));
  SCHECK(stat_file.good(), IOError, "Cannot read /proc/<pid>/stat");
  string content((std::istreambuf_iterator<char>(stat_file)), std::istreambuf_iterator<char>());
  // Fields after the parenthesized comm: state is field 3; utime/stime are fields 14/15.
  const auto paren = content.rfind(')');
  SCHECK_NE(paren, string::npos, Corruption, "Malformed /proc/<pid>/stat");
  std::istringstream rest(content.substr(paren + 2));
  string field;
  uint64_t utime = 0, stime = 0;
  for (int i = 3; i <= 15 && (rest >> field); ++i) {
    if (i == 14) {
      utime = std::stoull(field);
    } else if (i == 15) {
      stime = std::stoull(field);
    }
  }
  return static_cast<double>(utime + stime) / sysconf(_SC_CLK_TCK);
#else
  string out;
  RETURN_NOT_OK(Subprocess::Call(
      {"/bin/ps", "-o", "cputime=", "-p", std::to_string(pid)}, &out));
  // Format: [[dd-]hh:]mm:ss.cc
  out.erase(std::remove_if(out.begin(), out.end(), ::isspace), out.end());
  SCHECK(!out.empty(), NotFound, "ps returned no cputime; master process gone?");
  double days = 0;
  const auto dash = out.find('-');
  if (dash != string::npos) {
    days = std::stod(out.substr(0, dash));
    out = out.substr(dash + 1);
  }
  std::vector<string> parts = strings::Split(out, ":");
  double seconds = 0;
  for (const auto& part : parts) {
    seconds = seconds * 60 + std::stod(part);
  }
  return days * 86400 + seconds;
#endif
}

Result<double> ProcessRssBytes(pid_t pid) {
#if defined(__linux__)
  std::ifstream statm(Format("/proc/$0/statm", pid));
  SCHECK(statm.good(), IOError, "Cannot read /proc/<pid>/statm");
  uint64_t size_pages = 0, rss_pages = 0;
  statm >> size_pages >> rss_pages;
  return static_cast<double>(rss_pages) * sysconf(_SC_PAGESIZE);
#else
  string out;
  RETURN_NOT_OK(Subprocess::Call({"/bin/ps", "-o", "rss=", "-p", std::to_string(pid)}, &out));
  return std::stod(out) * 1024;
#endif
}

// Sums prometheus series by metric name for names containing any of the substrings.
void ParsePrometheusMetrics(
    const string& body, const std::vector<string>& substrings, std::map<string, double>* out) {
  size_t pos = 0;
  while (pos < body.size()) {
    size_t eol = body.find('\n', pos);
    if (eol == string::npos) {
      eol = body.size();
    }
    const auto line = body.substr(pos, eol - pos);
    pos = eol + 1;
    if (line.empty() || line[0] == '#') {
      continue;
    }
    const auto name_end = line.find_first_of("{ ");
    if (name_end == string::npos) {
      continue;
    }
    const auto name = line.substr(0, name_end);
    bool matched = false;
    for (const auto& substring : substrings) {
      if (name.find(substring) != string::npos) {
        matched = true;
        break;
      }
    }
    if (!matched) {
      continue;
    }
    auto value_start = line.find(' ', line[name_end] == '{' ? line.find('}', name_end) : name_end);
    if (value_start == string::npos) {
      continue;
    }
    value_start = line.find_first_not_of(' ', value_start);
    if (value_start == string::npos) {
      continue;
    }
    const auto value_end = line.find(' ', value_start);
    try {
      (*out)[name] += std::stod(line.substr(value_start, value_end - value_start));
    } catch (const std::exception&) {
      // Skip NaN/unparseable values.
    }
  }
}

// One line of the scenario file, or a parallel block.
struct Directive {
  string kind;
  string arg;
  std::vector<Directive> children;
  int line = 0;
};

string Trim(const string& s) {
  const auto b = s.find_first_not_of(" \t\r\n");
  if (b == string::npos) {
    return "";
  }
  const auto e = s.find_last_not_of(" \t\r\n");
  return s.substr(b, e - b + 1);
}

// "500ms", "30s", "5m", "1h"; a bare number is seconds.
Result<MonoDelta> ParseDuration(const string& text) {
  size_t pos = 0;
  while (pos < text.size() && (std::isdigit(text[pos]) || text[pos] == '.')) {
    ++pos;
  }
  SCHECK_FORMAT(pos > 0, InvalidArgument, "Bad duration '$0'", text);
  const double value = std::stod(text.substr(0, pos));
  const auto unit = text.substr(pos);
  if (unit.empty() || unit == "s") {
    return MonoDelta::FromSeconds(value);
  }
  if (unit == "ms") {
    return MonoDelta::FromMilliseconds(static_cast<int64_t>(value));
  }
  if (unit == "m") {
    return MonoDelta::FromMinutes(value);
  }
  if (unit == "h") {
    return MonoDelta::FromHours(value);
  }
  return STATUS_FORMAT(InvalidArgument, "Bad duration unit in '$0'", text);
}

Result<std::vector<Directive>> ParseDirectives(
    std::vector<string>& lines, size_t* idx, bool in_block, const string& base_dir) {
  std::vector<Directive> out;
  while (*idx < lines.size()) {
    const int line_no = narrow_cast<int>(*idx + 1);
    const auto line = Trim(lines[(*idx)++]);
    if (line.empty() || line[0] == '#') {
      continue;
    }
    if (line == ">>") {
      SCHECK(in_block, InvalidArgument, "Line $0: '>>' without an open block", line_no);
      return out;
    }
    Directive d;
    d.line = line_no;
    const auto space = line.find_first_of(" \t");
    d.kind = line.substr(0, space);
    d.arg = space == string::npos ? "" : Trim(line.substr(space));
    const bool block = d.arg == "<<";
    if (d.kind == "sql") {
      if (block) {
        d.arg.clear();
        while (true) {
          SCHECK_FORMAT(*idx < lines.size(), InvalidArgument,
                        "Line $0: sql block never closed", line_no);
          const auto& raw = lines[(*idx)++];
          if (Trim(raw) == ">>") {
            break;
          }
          d.arg += raw + "\n";
        }
      }
      SCHECK(!Trim(d.arg).empty(), InvalidArgument, "Line $0: empty sql", line_no);
    } else if (d.kind == "sql_file") {
      SCHECK(!d.arg.empty(), InvalidArgument, "Line $0: sql_file needs a path", line_no);
      if (d.arg[0] != '/') {
        d.arg = JoinPathSegments(base_dir, d.arg);
      }
      SCHECK(Env::Default()->FileExists(d.arg), NotFound, "Line $0: $1 not found", line_no,
             d.arg);
    } else if (d.kind == "sleep") {
      RETURN_NOT_OK(ParseDuration(d.arg));
    } else if (d.kind == "database") {
      SCHECK(!d.arg.empty(), InvalidArgument, "Line $0: database needs a name", line_no);
    } else if (d.kind == "parallel") {
      SCHECK(block, InvalidArgument, "Line $0: parallel needs '<<'", line_no);
      d.arg.clear();
      d.children = VERIFY_RESULT(ParseDirectives(lines, idx, /* in_block= */ true, base_dir));
    } else {
      return STATUS_FORMAT(InvalidArgument, "Line $0: unknown directive '$1'", line_no, d.kind);
    }
    out.push_back(std::move(d));
  }
  SCHECK(!in_block, InvalidArgument, "parallel block never closed");
  return out;
}

Result<std::vector<Directive>> ParseScenarioFile(const string& path) {
  std::ifstream in(path);
  SCHECK(in.good(), NotFound, "Cannot read scenario $0", path);
  std::vector<string> lines;
  for (string line; std::getline(in, line);) {
    lines.push_back(line);
  }
  size_t idx = 0;
  return ParseDirectives(lines, &idx, /* in_block= */ false, DirName(path));
}

// One --topology line: where a share of the hollow fleet registers.
struct ZoneSpec {
  string cloud;
  string region;
  string zone;
  double share = 0;  // fraction of --num_tservers

  CloudInfoPB cloud_info() const {
    CloudInfoPB cloud_info;
    cloud_info.set_placement_cloud(cloud);
    cloud_info.set_placement_region(region);
    cloud_info.set_placement_zone(zone);
    return cloud_info;
  }
};

Result<std::vector<ZoneSpec>> ParseTopologyFile(const string& path) {
  std::ifstream in(path);
  SCHECK_FORMAT(in.good(), NotFound, "Cannot read topology $0", path);
  std::vector<ZoneSpec> zones;
  double sum = 0;
  int line_no = 0;
  for (string raw; std::getline(in, raw);) {
    ++line_no;
    const auto line = Trim(raw);
    if (line.empty() || line[0] == '#') {
      continue;
    }
    std::istringstream fields(line);
    ZoneSpec z;
    string percent;
    fields >> z.cloud >> z.region >> z.zone >> percent;
    SCHECK_FORMAT(fields && !percent.empty(), InvalidArgument,
                  "$0:$1: expected 'cloud region zone percent'", path, line_no);
    try {
      z.share = std::stod(percent);
    } catch (const std::exception&) {
      return STATUS_FORMAT(InvalidArgument, "$0:$1: bad percent '$2'", path, line_no, percent);
    }
    SCHECK_FORMAT(z.share > 0, InvalidArgument, "$0:$1: percent must be positive", path, line_no);
    sum += z.share;
    zones.push_back(std::move(z));
  }
  SCHECK_FORMAT(!zones.empty(), InvalidArgument, "$0 has no zones", path);
  // Either percentages (sum 100) or fractions (sum 1) are accepted.
  if (std::abs(sum - 100) < 0.01) {
    for (auto& z : zones) {
      z.share /= 100;
    }
  } else {
    SCHECK_FORMAT(std::abs(sum - 1) < 0.0001, InvalidArgument,
                  "$0: shares add up to $1, expected 100 or 1", path, sum);
  }
  return zones;
}

// Each zone gets round(share * total), at least 1; the total is then trimmed or topped up on the
// zones furthest from their exact share.
Result<std::vector<int>> AllocateTServers(const std::vector<ZoneSpec>& zones, int total) {
  SCHECK_FORMAT(total >= narrow_cast<int>(zones.size()), InvalidArgument,
                "--num_tservers=$0 is below the $1 zones in the topology", total, zones.size());
  std::vector<int> counts(zones.size());
  int sum = 0;
  for (size_t i = 0; i < zones.size(); ++i) {
    counts[i] = std::max(1, static_cast<int>(std::lround(zones[i].share * total)));
    sum += counts[i];
  }
  auto deviation = [&](size_t i) { return zones[i].share * total - counts[i]; };
  while (sum > total) {
    size_t pick = zones.size();
    for (size_t i = 0; i < zones.size(); ++i) {
      if (counts[i] > 1 && (pick == zones.size() || deviation(i) < deviation(pick))) {
        pick = i;
      }
    }
    SCHECK(pick < zones.size(), InvalidArgument, "Cannot fit the topology into --num_tservers");
    --counts[pick];
    --sum;
  }
  while (sum < total) {
    size_t pick = 0;
    for (size_t i = 1; i < zones.size(); ++i) {
      if (deviation(i) > deviation(pick)) {
        pick = i;
      }
    }
    ++counts[pick];
    ++sum;
  }
  return counts;
}

Status ResetDir(const string& dir) {
  auto* env = Env::Default();
  if (env->FileExists(dir)) {
    RETURN_NOT_OK(env->DeleteRecursively(dir));
  }
  return env->CreateDirs(dir);
}

Status CopyTree(const string& from, const string& to) {
  return Subprocess::Call({"cp", "-R", from, to}, /* log_stdout_and_stderr= */ true);
}

class MasterLoadBench {
 public:
  Status Run() {
    SCHECK(!FLAGS_master_bin.empty(), InvalidArgument, "--master_bin is required");
    SCHECK(!FLAGS_tserver_bin.empty(), InvalidArgument, "--tserver_bin is required");
    SCHECK(!FLAGS_data_dir.empty(), InvalidArgument, "--data_dir is required");
    SCHECK(FLAGS_from_snapshot.empty() || !FLAGS_resume, InvalidArgument,
           "--from_snapshot and --resume are exclusive");
    restoring_ = FLAGS_resume || !FLAGS_from_snapshot.empty();
    if (!FLAGS_from_snapshot.empty()) {
      RETURN_NOT_OK(RestoreSnapshotFiles());
    }
    if (restoring_) {
      // The fleet is whatever was loaded; the saved meta.txt replaces the fleet flags.
      if (!FLAGS_topology.empty()) {
        LOG(WARNING) << "--topology is ignored on resume; using " << kSnapshotMeta << " from "
                     << FLAGS_data_dir;
      }
      RETURN_NOT_OK(LoadRegistryMeta());
    } else {
      SCHECK(!FLAGS_topology.empty(), InvalidArgument, "--topology is required");
      SCHECK_GT(FLAGS_num_tservers, 0, InvalidArgument, "--num_tservers must be positive");
      SCHECK_GE(FLAGS_num_tservers, FLAGS_replication_factor, InvalidArgument,
                "Need at least replication_factor tservers");
      topology_ = VERIFY_RESULT(ParseTopologyFile(FLAGS_topology));
      allocation_ = VERIFY_RESULT(AllocateTServers(topology_, FLAGS_num_tservers));
    }
    for (size_t i = 0; i < topology_.size(); ++i) {
      LOG(INFO) << "Zone " << topology_[i].cloud << "/" << topology_[i].region << "/"
                << topology_[i].zone << ": " << allocation_[i] << " hollow tservers";
    }
    ysqlsh_bin_ = FLAGS_ysqlsh_bin.empty()
        ? JoinPathSegments(DirName(DirName(FLAGS_tserver_bin)), "postgres/bin/ysqlsh")
        : FLAGS_ysqlsh_bin;
    SCHECK_FORMAT(Env::Default()->FileExists(ysqlsh_bin_), NotFound, "ysqlsh not found at $0",
                  ysqlsh_bin_);
    for (const auto& file : strings::Split(FLAGS_sql_file, ",", strings::SkipEmpty())) {
      const auto path = file.ToString();
      SCHECK_FORMAT(Env::Default()->FileExists(path), NotFound, "$0 not found", path);
      scenario_.push_back(Directive{.kind = "sql_file", .arg = path, .children = {}, .line = 0});
    }
    if (!FLAGS_scenario.empty()) {
      auto parsed = VERIFY_RESULT(ParseScenarioFile(FLAGS_scenario));
      scenario_.insert(scenario_.end(), std::make_move_iterator(parsed.begin()),
                       std::make_move_iterator(parsed.end()));
    }

    master_addr_ = HostPort("127.0.0.1", FLAGS_master_rpc_port);
    start_time_ = MonoTime::Now();

    RETURN_NOT_OK(StartMaster());
    if (!restoring_) {
      // Boot default: what is created before the switch, i.e. the transactions table, lands on
      // the real tserver.
      RETURN_NOT_OK(SetClusterPlacement(RealTServerReplication()));
    }
    mirror_state_ = std::make_unique<MirrorState>();
    relay_ = std::make_unique<TcpRelay>(
        narrow_cast<uint16_t>(FLAGS_relay_port), narrow_cast<uint16_t>(FLAGS_master_rpc_port),
        mirror_state_.get());
    RETURN_NOT_OK(relay_->Start());
    RETURN_NOT_OK(StartRealTserver());
    RETURN_NOT_OK(StartHollowTservers());
    if (restoring_) {
      RETURN_NOT_OK(LoadHollowTablets());
    }

    collector_thread_ = std::thread([this] { CollectorLoop(); });

    SetPhase(restoring_ ? "restore" : "register");
    scheduler_thread_ = std::thread([this] { SchedulerLoop(); });
    replay_thread_ = std::thread([this] { ReplayLoop(); });
    RETURN_NOT_OK(WaitForRegistration());

    if (restoring_) {
      RETURN_NOT_OK(WaitForReportsSettled());
      LOG(INFO) << "Resume complete " << (MonoTime::Now() - start_time_).ToSeconds()
                << "s after start";
    } else {
      SetPhase("transactions");
      RETURN_NOT_OK(WaitForTransactionsTableAndPin());
      RETURN_NOT_OK(CreateSequencesDataTable());
      RETURN_NOT_OK(SetClusterPlacement(HollowReplication()));
    }

    if (!scenario_.empty()) {
      RETURN_NOT_OK(WaitForYsql());
      SetPhase("load");
      RETURN_NOT_OK(RunDirectives(scenario_));
      LOG(INFO) << "Load complete " << (MonoTime::Now() - start_time_).ToSeconds()
                << "s after start";
    }
    // The registry is what a --resume needs besides the data roots; write it as soon as the
    // tablet set is final so a hard kill during idle loses nothing.
    RETURN_NOT_OK(WriteRegistry());

    SetPhase(kMeasuredPhase);
    signal(SIGINT, HandleStopSignal);
    signal(SIGTERM, HandleStopSignal);
    const auto idle_deadline = FLAGS_run_seconds > 0
        ? MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_run_seconds) : MonoTime::Max();
    LOG(INFO) << "Idle" << (FLAGS_run_seconds > 0 ? Format(" for $0s", FLAGS_run_seconds) : "")
              << "; SIGINT/SIGTERM stops the run";
    while (!g_stop_requested.load(std::memory_order_acquire) && MonoTime::Now() < idle_deadline) {
      SleepFor(500ms);
    }
    if (!FLAGS_snapshot_dir.empty()) {
      SetPhase("snapshot");
      RETURN_NOT_OK(TakeSnapshot());
    }

    SetPhase("done");
    Shutdown();
    PrintSummary();
    return Status::OK();
  }

  ~MasterLoadBench() {

    Shutdown();
  }

 private:
  string PidFilePath(const string& name) const {
    return JoinPathSegments(FLAGS_data_dir, name + ".pid");
  }

  // Kills the process a previous crashed run left behind, identified by our own pid file and
  // verified by process name so a recycled pid is never killed.
  Status KillLeftoverProcess(Env* env, const string& name, const string& comm_substring) {
    const auto pid_file = PidFilePath(name);
    if (!env->FileExists(pid_file)) {
      return Status::OK();
    }
    pid_t old_pid = 0;
    {
      std::ifstream in(pid_file);
      in >> old_pid;
    }
    if (old_pid > 0) {
      string comm;
      const auto ps_status = Subprocess::Call(
          {"ps", "-o", "comm=", "-p", std::to_string(old_pid)}, &comm);
      if (ps_status.ok() && comm.find(comm_substring) != string::npos) {
        LOG(INFO) << "Killing leftover " << comm_substring << " pid " << old_pid
                  << " from a previous run";
        if (kill(old_pid, SIGKILL) == 0) {
          const auto deadline = MonoTime::Now() + 10s;
          while (kill(old_pid, 0) == 0) {
            SCHECK(MonoTime::Now() < deadline, TimedOut,
                   "Leftover $0 pid $1 did not die", comm_substring, old_pid);
            SleepFor(100ms);
          }
        }
      }
    }
    return env->DeleteFile(pid_file);
  }

  Status StartMaster() {
    const auto data_dir = JoinPathSegments(FLAGS_data_dir, kMasterDataDir);
    const auto log_dir = JoinPathSegments(FLAGS_data_dir, "master-logs");
    auto* env = Env::Default();
    RETURN_NOT_OK(KillLeftoverProcess(env, "master", "yb-master"));
    // A fresh run starts from an empty catalog; a restore already placed the data root.
    RETURN_NOT_OK(ResetDir(log_dir));
    if (!restoring_) {
      RETURN_NOT_OK(ResetDir(data_dir));
    }

    std::vector<string> argv = {
        FLAGS_master_bin,
        "--fs_data_dirs=" + data_dir,
        "--log_dir=" + log_dir,
        Format("--rpc_bind_addresses=127.0.0.1:$0", FLAGS_master_rpc_port),
        Format("--master_addresses=127.0.0.1:$0", FLAGS_master_rpc_port),
        "--webserver_interface=127.0.0.1",
        Format("--webserver_port=$0", FLAGS_master_web_port),
        Format("--replication_factor=$0", FLAGS_replication_factor),
        "--enable_load_balancing=false",
        "--enable_ysql=true",
        "--max_create_tablets_per_ts=1000000",
        "--callhome_enabled=false",
        // Every address the master hands out (leader hostport, master_config peers) must point at
        // the relay so the real tserver's traffic stays capturable; with use_private_ip=never
        // (the default) clients always prefer the broadcast address.
        Format("--server_broadcast_addresses=127.0.0.1:$0", FLAGS_relay_port),
    };
    const std::vector<string> extra_flags =
        strings::Split(FLAGS_master_flags, ",", strings::SkipEmpty());
    argv.insert(argv.end(), extra_flags.begin(), extra_flags.end());
    string program = FLAGS_master_bin;
    if (!FLAGS_master_cpus.empty()) {
      argv.insert(argv.begin(), {"taskset", "-c", FLAGS_master_cpus});
      program = "taskset";
    }
    master_process_ = std::make_unique<Subprocess>(program, argv);
    RETURN_NOT_OK(master_process_->Start());
    LOG(INFO) << "Started yb-master pid " << master_process_->pid();
    // Lets the next run kill this master if the driver dies without cleaning up.
    std::ofstream(PidFilePath("master")) << master_process_->pid() << "\n";

    // Master is usable once ListTabletServers succeeds (sys catalog loaded, leader elected).
    rpc::MessengerBuilder builder("bench-driver");
    builder.set_metric_entity(metric_entity_);
    builder.set_num_reactors(FLAGS_driver_reactors);
    builder.set_num_connections_to_server(
        FLAGS_connections_to_master > 0 ? FLAGS_connections_to_master : FLAGS_num_tservers);
    messenger_ = VERIFY_RESULT(builder.Build());
    proxy_cache_ = std::make_unique<rpc::ProxyCache>(messenger_.get());
    cluster_proxy_ = std::make_unique<master::MasterClusterProxy>(proxy_cache_.get(),
                                                                  master_addr_);
    generic_proxy_ = proxy_cache_->GetProxy(
        master_addr_, /* protocol= */ nullptr, /* resolve_cache_timeout= */ MonoDelta());
    const auto deadline = MonoTime::Now() + 60s;
    while (true) {
      master::ListTabletServersRequestPB req;
      master::ListTabletServersResponsePB resp;
      rpc::RpcController controller;
      controller.set_timeout(5s);
      auto status = cluster_proxy_->ListTabletServers(req, &resp, &controller);
      if (status.ok() && !resp.has_error()) {
        break;
      }
      SCHECK(MonoTime::Now() < deadline, TimedOut, "Master did not become ready");
      SleepFor(500ms);
    }
    LOG(INFO) << "Master is ready";
    return Status::OK();
  }

  Status StartRealTserver() {
    const auto data_dir = JoinPathSegments(FLAGS_data_dir, kTServerDataDir);
    const auto log_dir = JoinPathSegments(FLAGS_data_dir, "ts-logs");
    auto* env = Env::Default();
    RETURN_NOT_OK(KillLeftoverProcess(env, "tserver", "yb-tserver"));
    RETURN_NOT_OK(ResetDir(log_dir));
    if (!restoring_) {
      RETURN_NOT_OK(ResetDir(data_dir));
    }
    std::vector<string> argv = {
        FLAGS_tserver_bin,
        "--fs_data_dirs=" + data_dir,
        "--log_dir=" + log_dir,
        Format("--rpc_bind_addresses=127.0.0.1:$0", FLAGS_tserver_rpc_port),
        "--webserver_interface=127.0.0.1",
        Format("--webserver_port=$0", FLAGS_tserver_web_port),
        // Heartbeats reach the master through the relay, where they are captured.
        Format("--tserver_master_addrs=127.0.0.1:$0", FLAGS_relay_port),
        Format("--heartbeat_interval_ms=$0", FLAGS_heartbeat_interval_ms),
        "--enable_ysql=true",
        "--start_pgsql_proxy",
        Format("--pgsql_proxy_bind_address=127.0.0.1:$0", FLAGS_pg_port),
        Format("--placement_cloud=$0", kPlacementCloud),
        Format("--placement_region=$0", kPlacementRegion),
        Format("--placement_zone=$0", kRealTServerZone),
        "--callhome_enabled=false",
    };
    const std::vector<string> extra_flags =
        strings::Split(FLAGS_tserver_flags, ",", strings::SkipEmpty());
    argv.insert(argv.end(), extra_flags.begin(), extra_flags.end());
    tserver_process_ = std::make_unique<Subprocess>(FLAGS_tserver_bin, argv);
    RETURN_NOT_OK(tserver_process_->Start());
    LOG(INFO) << "Started yb-tserver pid " << tserver_process_->pid();
    std::ofstream(PidFilePath("tserver")) << tserver_process_->pid() << "\n";
    return Status::OK();
  }

  Status StartHollowTservers() {
    const auto instance_seqno = static_cast<uint64_t>(GetCurrentTimeMicros());
    auto factory = rpc::CreateConnectionContextFactory<rpc::YBInboundConnectionContext>();
    std::vector<CloudInfoPB> placements;
    for (size_t z = 0; z < topology_.size(); ++z) {
      for (int k = 0; k < allocation_[z]; ++k) {
        placements.push_back(topology_[z].cloud_info());
      }
    }
    for (int i = 0; i < FLAGS_num_tservers; ++i) {
      auto ts = std::make_unique<HollowTServer>(i, instance_seqno, placements[i]);
      Endpoint bound_endpoint;
      RETURN_NOT_OK(messenger_->ListenAddress(factory, Endpoint(), &bound_endpoint));
      ts->SetHostPort(HostPort("127.0.0.1", bound_endpoint.port()));
      ts->heartbeat_proxy = std::make_unique<master::MasterHeartbeatProxy>(
          proxy_cache_.get(), master_addr_);
      ts->lease_proxy = std::make_unique<master::MasterYsqlLeaseProxy>(
          proxy_cache_.get(), master_addr_);
      ts->cluster_proxy = std::make_unique<master::MasterClusterProxy>(
          proxy_cache_.get(), master_addr_);
      registry_.Add(std::move(ts));
    }

    service_thread_pool_ = std::make_shared<rpc::ThreadPool>(rpc::ThreadPoolOptions{
        .name = "hollow_ts",
        .max_workers = 8,
    });
    auto service = std::make_unique<HollowTSAdminService>(metric_entity_, &registry_);
    const auto service_name = service->service_name();
    service_pool_ = make_scoped_refptr<rpc::ServicePool>(
        /* max_tasks= */ 10000, [pool = service_thread_pool_](auto) { return pool; },
        &messenger_->scheduler(), std::move(service), messenger_->metric_entity());
    RETURN_NOT_OK(messenger_->RegisterService(service_name, service_pool_));

    auto ts_service = std::make_unique<HollowTSService>(metric_entity_, &registry_);
    const auto ts_service_name = ts_service->service_name();
    ts_service_pool_ = make_scoped_refptr<rpc::ServicePool>(
        /* max_tasks= */ 10000, [pool = service_thread_pool_](auto) { return pool; },
        &messenger_->scheduler(), std::move(ts_service), messenger_->metric_entity());
    RETURN_NOT_OK(messenger_->RegisterService(ts_service_name, ts_service_pool_));

    auto generic_service = std::make_unique<HollowGenericService>(metric_entity_);
    const auto generic_service_name = generic_service->service_name();
    generic_service_pool_ = make_scoped_refptr<rpc::ServicePool>(
        /* max_tasks= */ 10000, [pool = service_thread_pool_](auto) { return pool; },
        &messenger_->scheduler(), std::move(generic_service), messenger_->metric_entity());
    RETURN_NOT_OK(messenger_->RegisterService(generic_service_name, generic_service_pool_));

    RETURN_NOT_OK(messenger_->StartAcceptor());
    LOG(INFO) << "Started " << FLAGS_num_tservers << " hollow tservers";
    return Status::OK();
  }

  void SchedulerLoop() {
    const auto hb_interval = MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms);
    const auto lease_interval = 1000ms;
    const auto connectivity_interval =
        MonoDelta::FromMilliseconds(FLAGS_connectivity_check_interval_ms);
    // Stagger initial sends across the interval so load is even, as in a real cluster; metrics
    // beats and connectivity checks are staggered across their own periods the same way.
    const auto& tservers = registry_.tservers();
    auto now = MonoTime::Now();
    const auto metrics_period = hb_interval * FLAGS_full_metrics_beat_period;
    for (size_t i = 0; i < tservers.size(); ++i) {
      tservers[i]->next_heartbeat = now + hb_interval * i / tservers.size();
      tservers[i]->next_lease_refresh =
          now + lease_interval * i / tservers.size() + lease_interval / 2;
      tservers[i]->next_connectivity_check = now + connectivity_interval * i / tservers.size();
      tservers[i]->last_metrics_beat = now - metrics_period + metrics_period * i / tservers.size();
    }
    while (running_.load(std::memory_order_acquire)) {
      now = MonoTime::Now();
      // In mirror mode, follow the real tserver's measured cadence so tserver-side interval
      // changes propagate to the hollow fleet.
      auto interval = hb_interval;
      if (mirror_state_) {
        const auto measured = mirror_state_->MeasuredIntervalMs();
        if (measured > 0) {
          interval = MonoDelta::FromMilliseconds(static_cast<int64_t>(measured));
        }
      }
      for (const auto& ts : tservers) {
        if (now >= ts->next_heartbeat && !ts->heartbeat_in_flight.exchange(true)) {
          ts->next_heartbeat = now + interval;
          SendHeartbeat(ts.get());
        }
        if (FLAGS_send_lease_refreshes && now >= ts->next_lease_refresh &&
            !ts->lease_in_flight.exchange(true)) {
          ts->next_lease_refresh = now + lease_interval;
          SendLeaseRefresh(ts.get());
        }
        if (now >= ts->next_connectivity_check && !ts->connectivity_in_flight.exchange(true)) {
          ts->next_connectivity_check = now + connectivity_interval;
          SendConnectivityCheck(ts.get());
        }
      }
      SleepFor(10ms);
    }
  }

  void SendHeartbeat(HollowTServer* ts) {
    string universe_uuid;
    {
      std::lock_guard l(universe_uuid_mutex_);
      universe_uuid = universe_uuid_;
    }
    std::shared_ptr<const master::TSHeartbeatRequestPB> tmpl;
    std::optional<bool> metrics_override;
    if (mirror_state_) {
      const auto now = MonoTime::Now();
      const auto measured = mirror_state_->MeasuredMetricsPeriodMs();
      const auto period = MonoDelta::FromMilliseconds(static_cast<int64_t>(
          measured > 0
              ? measured
              : FLAGS_full_metrics_beat_period * FLAGS_heartbeat_interval_ms));
      const bool want_metrics =
          !ts->last_metrics_beat.Initialized() || now >= ts->last_metrics_beat + period;
      tmpl = mirror_state_->TemplateFor(want_metrics);
      if (tmpl) {
        metrics_override = want_metrics;
        if (want_metrics) {
          ts->last_metrics_beat = now;
        }
      }
    }
    ts->MarkRegistrationInFlight();
    ts->FillHeartbeat(universe_uuid, tmpl.get(), metrics_override, &ts->hb_req);
    ts->hb_controller.Reset();
    ts->hb_controller.set_timeout(15s);
    ts->heartbeat_sent_time = MonoTime::Now();
    ts->heartbeat_proxy->TSHeartbeatAsync(
        ts->hb_req, &ts->hb_resp, &ts->hb_controller, [this, ts] {
          HandleHeartbeatResponse(ts);
        });
  }

  void HandleHeartbeatResponse(HollowTServer* ts) {
    const auto rtt_us =
        (MonoTime::Now() - ts->heartbeat_sent_time).ToMicroseconds();
    const auto status = ts->hb_controller.status();
    if (!status.ok()) {
      stats_.heartbeats_failed.fetch_add(1, std::memory_order_relaxed);
      YB_LOG_EVERY_N_SECS(WARNING, 10)
          << "Heartbeat from " << ts->uuid() << " failed: " << status;
      ts->HandleHeartbeatFailure();
      ts->heartbeat_in_flight.store(false, std::memory_order_release);
      return;
    }
    const auto& resp = ts->hb_resp;
    // A first heartbeat without universe_uuid fails with INVALID_REQUEST but returns the
    // master's uuid for adoption; every later heartbeat echoes it.
    if (!resp.universe_uuid().empty()) {
      std::lock_guard l(universe_uuid_mutex_);
      if (universe_uuid_.empty()) {
        universe_uuid_ = resp.universe_uuid();
        LOG(INFO) << "Adopted universe uuid " << universe_uuid_;
      }
    }
    if (resp.has_error()) {
      stats_.heartbeats_failed.fetch_add(1, std::memory_order_relaxed);
      YB_LOG_EVERY_N_SECS(WARNING, 10)
          << "Heartbeat from " << ts->uuid() << " rejected: "
          << resp.error().ShortDebugString();
      ts->HandleHeartbeatFailure();
      ts->heartbeat_in_flight.store(false, std::memory_order_release);
      return;
    }
    stats_.heartbeats_ok.fetch_add(1, std::memory_order_relaxed);
    stats_.heartbeat_rtt_us_sum.fetch_add(rtt_us, std::memory_order_relaxed);
    stats_.heartbeat_rtt_count.fetch_add(1, std::memory_order_relaxed);
    ts->HandleHeartbeatSuccess(resp, rtt_us);
    ts->heartbeat_in_flight.store(false, std::memory_order_release);
  }

  void SendLeaseRefresh(HollowTServer* ts) {
    auto& req = ts->lease_req;
    req.Clear();
    auto* instance = req.mutable_instance();
    instance->set_permanent_uuid(ts->uuid());
    instance->set_instance_seqno(ts->instance_seqno());
    const auto epoch = ts->lease_epoch();
    if (epoch) {
      req.set_current_lease_epoch(*epoch);
    }
    req.set_local_request_send_time_ms(GetCurrentTimeMicros() / 1000);
    ts->lease_controller.Reset();
    ts->lease_controller.set_timeout(10s);
    ts->lease_proxy->RefreshYsqlLeaseAsync(
        req, &ts->lease_resp, &ts->lease_controller, [this, ts] {
          HandleLeaseResponse(ts);
        });
  }

  void HandleLeaseResponse(HollowTServer* ts) {
    const auto status = ts->lease_controller.status();
    if (!status.ok() || ts->lease_resp.has_error()) {
      stats_.lease_refreshes_failed.fetch_add(1, std::memory_order_relaxed);
      YB_LOG_EVERY_N_SECS(WARNING, 10)
          << "Lease refresh from " << ts->uuid() << " failed: "
          << (status.ok() ? ts->lease_resp.error().ShortDebugString() : status.ToString());
    } else {
      stats_.lease_refreshes_ok.fetch_add(1, std::memory_order_relaxed);
      if (ts->lease_resp.has_info() && ts->lease_resp.info().has_lease_epoch()) {
        ts->SetLeaseEpoch(ts->lease_resp.info().lease_epoch());
      }
    }
    ts->lease_in_flight.store(false, std::memory_order_release);
  }

  // The real tserver's connectivity poller lists all tservers once a minute and pings them;
  // only the master call is reproduced.
  void SendConnectivityCheck(HollowTServer* ts) {
    ts->connectivity_controller.Reset();
    ts->connectivity_controller.set_timeout(15s);
    ts->cluster_proxy->ListTabletServersAsync(
        ts->connectivity_req, &ts->connectivity_resp, &ts->connectivity_controller, [this, ts] {
          const auto status = ts->connectivity_controller.status();
          if (!status.ok() || ts->connectivity_resp.has_error()) {
            stats_.connectivity_checks_failed.fetch_add(1, std::memory_order_relaxed);
            YB_LOG_EVERY_N_SECS(WARNING, 10)
                << "ListTabletServers from " << ts->uuid() << " failed: "
                << (status.ok() ? ts->connectivity_resp.error().ShortDebugString()
                                : status.ToString());
          } else {
            stats_.connectivity_checks_ok.fetch_add(1, std::memory_order_relaxed);
          }
          ts->connectivity_in_flight.store(false, std::memory_order_release);
        });
  }

  void ReplayLoop() {
    google::protobuf::DynamicMessageFactory factory;
    while (running_.load(std::memory_order_acquire)) {
      MirrorState::ReplayItem item;
      if (!mirror_state_->PopReplayItem(&item)) {
        SleepFor(100ms);
        continue;
      }
      ReplayToHollowTservers(item, &factory);
    }
  }

  // Sends one captured request once per hollow tserver, via dynamic protobuf messages so any
  // linked master method can be replayed without a typed proxy.
  void ReplayToHollowTservers(
      const MirrorState::ReplayItem& item, google::protobuf::DynamicMessageFactory* factory) {
    const auto key = item.service + "." + item.method;
    const auto* pool = google::protobuf::DescriptorPool::generated_pool();
    const auto* service_desc = pool->FindServiceByName(item.service);
    const auto* method_desc =
        service_desc != nullptr ? service_desc->FindMethodByName(item.method) : nullptr;
    if (method_desc == nullptr) {
      // The split master services all declare custom_service_name "yb.master.MasterService",
      // so that is the name on the wire — but protobuf's registry only knows declared proto
      // names. Look the method up in the services that host the replayable methods.
      for (const auto* split_service : {"yb.master.MasterCluster", "yb.master.MasterEncryption"}) {
        const auto* split_desc = pool->FindServiceByName(split_service);
        if (split_desc != nullptr) {
          method_desc = split_desc->FindMethodByName(item.method);
          if (method_desc != nullptr) {
            break;
          }
        }
      }
    }
    if (method_desc == nullptr) {
      YB_LOG_EVERY_N_SECS(WARNING, 60) << "Cannot resolve " << key << " for replay";
      mirror_state_->RecordReplay(key, false, FLAGS_num_tservers);
      return;
    }
    const auto* request_prototype = factory->GetPrototype(method_desc->input_type());
    const auto* response_prototype = factory->GetPrototype(method_desc->output_type());
    struct ReplayCall {
      explicit ReplayCall(const MirrorState::ReplayItem& item)
          : method(item.service, item.method) {}
      rpc::RemoteMethod method;
      std::unique_ptr<google::protobuf::Message> request;
      std::unique_ptr<google::protobuf::Message> response;
      rpc::RpcController controller;
    };
    for (int i = 0; i < FLAGS_num_tservers && running_.load(std::memory_order_acquire); ++i) {
      auto call = std::make_shared<ReplayCall>(item);
      call->request.reset(request_prototype->New());
      if (!call->request->ParseFromString(item.body)) {
        mirror_state_->RecordReplay(key, false, FLAGS_num_tservers);
        return;
      }
      call->response.reset(response_prototype->New());
      call->controller.set_timeout(15s);
      generic_proxy_->AsyncRequest(
          &call->method, /* method_metrics= */ nullptr, *call->request, call->response.get(),
          &call->controller, [this, call, key] {
            mirror_state_->RecordReplay(key, call->controller.status().ok());
          });
      if (i % 32 == 31) {
        SleepFor(1ms);
      }
    }
  }

  Status WaitForRegistration() {
    const auto deadline = MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_register_timeout_sec);
    auto next_log = MonoTime::Now();
    const int expected = FLAGS_num_tservers + 1;
    while (true) {
      master::ListTabletServersRequestPB req;
      master::ListTabletServersResponsePB resp;
      rpc::RpcController controller;
      controller.set_timeout(10s);
      auto status = cluster_proxy_->ListTabletServers(req, &resp, &controller);
      int live = 0;
      if (status.ok() && !resp.has_error()) {
        for (const auto& server : resp.servers()) {
          if (server.alive()) {
            ++live;
          }
        }
        if (live >= expected) {
          LOG(INFO) << live << " tservers live on the master";
          return Status::OK();
        }
      }
      if (MonoTime::Now() >= next_log) {
        LOG(INFO) << "Waiting for registration: " << live << "/" << expected;
        next_log = MonoTime::Now() + 5s;
      }
      SCHECK(MonoTime::Now() < deadline, TimedOut, "Hollow tservers did not all register");
      SleepFor(500ms);
    }
  }

  ReplicationInfoPB RealTServerReplication() const {
    ReplicationInfoPB ri;
    auto* live = ri.mutable_live_replicas();
    live->set_num_replicas(1);
    auto* block = live->add_placement_blocks();
    *block->mutable_cloud_info() = MakeCloudInfo(kRealTServerZone);
    block->set_min_num_replicas(1);
    return ri;
  }

  // Default for tables that name no tablespace once the transactions table is pinned: RF spread
  // over the topology's regions (zone left open), the real tserver's zone excluded.
  ReplicationInfoPB HollowReplication() const {
    std::vector<std::pair<string, string>> regions;
    for (const auto& z : topology_) {
      if (std::find(regions.begin(), regions.end(), std::make_pair(z.cloud, z.region)) ==
          regions.end()) {
        regions.emplace_back(z.cloud, z.region);
      }
    }
    ReplicationInfoPB ri;
    auto* live = ri.mutable_live_replicas();
    live->set_num_replicas(FLAGS_replication_factor);
    const int n = narrow_cast<int>(regions.size());
    for (int i = 0; i < n; ++i) {
      const int min_replicas =
          FLAGS_replication_factor / n + (i < FLAGS_replication_factor % n ? 1 : 0);
      if (min_replicas == 0) {
        continue;
      }
      auto* block = live->add_placement_blocks();
      block->mutable_cloud_info()->set_placement_cloud(regions[i].first);
      block->mutable_cloud_info()->set_placement_region(regions[i].second);
      block->set_min_num_replicas(min_replicas);
    }
    return ri;
  }

  Status SetClusterPlacement(const ReplicationInfoPB& replication) {
    master::GetMasterClusterConfigRequestPB get_req;
    master::GetMasterClusterConfigResponsePB get_resp;
    rpc::RpcController controller;
    controller.set_timeout(30s);
    RETURN_NOT_OK(cluster_proxy_->GetMasterClusterConfig(get_req, &get_resp, &controller));
    SCHECK(!get_resp.has_error(), RuntimeError, "GetMasterClusterConfig: $0",
           get_resp.error().ShortDebugString());
    master::ChangeMasterClusterConfigRequestPB change_req;
    *change_req.mutable_cluster_config() = get_resp.cluster_config();
    *change_req.mutable_cluster_config()->mutable_replication_info() = replication;
    master::ChangeMasterClusterConfigResponsePB change_resp;
    controller.Reset();
    controller.set_timeout(30s);
    RETURN_NOT_OK(
        cluster_proxy_->ChangeMasterClusterConfig(change_req, &change_resp, &controller));
    SCHECK(!change_resp.has_error(), RuntimeError, "ChangeMasterClusterConfig: $0",
           change_resp.error().ShortDebugString());
    LOG(INFO) << "Cluster default placement: " << replication.ShortDebugString();
    return Status::OK();
  }

  Result<client::YBClient*> Client() {
    if (!client_) {
      client_ = VERIFY_RESULT(client::YBClientBuilder()
          .add_master_server_addr(master_addr_.ToString())
          .default_admin_operation_timeout(MonoDelta::FromSeconds(600))
          .Build());
    }
    return client_.get();
  }

  // The master creates system.transactions once replication_factor tservers have registered;
  // under the boot default it lands on the real tserver. The table-level override keeps it
  // there after the default switches to the hollow zones.
  Status WaitForTransactionsTableAndPin() {
    const client::YBTableName table_name(
        YQL_DATABASE_CQL, master::kSystemNamespaceName, kGlobalTransactionsTableName);
    RETURN_NOT_OK(WaitForTableRunning(table_name));
    LOG(INFO) << table_name.ToString() << " is running; pinning it to " << kRealTServerZone;
    auto alterer = VERIFY_RESULT(Client())->NewTableAlterer(table_name);
    return alterer->replication_info(RealTServerReplication())->Alter();
  }

  // Sequence values are rows in system_postgres.sequences_data, a data table the tserver
  // creates on first use and then writes through the regular tablet service, which hollow
  // tservers do not serve. Force the creation now so it also lands on the real tserver. The
  // master rejects table-level placement for YSQL tables, so with the load balancer on this
  // table would need another way to stay put.
  Status CreateSequencesDataTable() {
    RETURN_NOT_OK(WaitForYsql());
    const auto exit_code = VERIFY_RESULT(Ysqlsh(
        "yugabyte", {"-o", "/dev/null", "-c",
                     "CREATE SEQUENCE mlb_bootstrap_seq; DROP SEQUENCE mlb_bootstrap_seq"}));
    SCHECK_FORMAT(exit_code == 0, RuntimeError, "Bootstrap sequence failed: ysqlsh exited with $0",
                  exit_code);
    // YSQL tables are only addressable by id on the master.
    const client::YBTableName table_name(
        YQL_DATABASE_PGSQL, kPgSequencesDataNamespaceId, "system_postgres",
        kPgSequencesDataTableId, "sequences_data");
    RETURN_NOT_OK(WaitForTableRunning(table_name));
    LOG(INFO) << table_name.ToString() << " is running on the real tserver";
    return Status::OK();
  }

  Status WaitForTableRunning(const client::YBTableName& table_name) {
    auto* client = VERIFY_RESULT(Client());
    const auto deadline = MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_register_timeout_sec);
    while (true) {
      if (VERIFY_RESULT(client->TableExists(table_name))) {
        bool in_progress = false;
        RETURN_NOT_OK(client->IsCreateTableInProgress(table_name, &in_progress));
        if (!in_progress) {
          break;
        }
      }
      SCHECK_FORMAT(MonoTime::Now() < deadline, TimedOut, "$0 was not created",
                    table_name.ToString());
      SleepFor(1s);
    }
    return Status::OK();
  }

  // --- scenario -------------------------------------------------------------------------------

  Status RunDirectives(const std::vector<Directive>& directives) {
    for (const auto& d : directives) {
      RETURN_NOT_OK(RunDirective(d));
    }
    return Status::OK();
  }

  Status RunDirective(const Directive& d) {
    const auto started = MonoTime::Now();
    LOG(INFO) << "Directive line " << d.line << ": " << d.kind
              << (d.arg.empty() ? "" : " " + d.arg.substr(0, 80));
    Status status;
    if (d.kind == "sleep") {
      SleepFor(VERIFY_RESULT(ParseDuration(d.arg)));
    } else if (d.kind == "database") {
      std::lock_guard l(db_mutex_);
      current_db_ = d.arg;
    } else if (d.kind == "sql") {
      const auto path = JoinPathSegments(SqlDir(), Format("line$0.sql", d.line));
      std::ofstream(path, std::ios::trunc) << d.arg << "\n";
      status = RunYsqlsh(CurrentDb(), path);
    } else if (d.kind == "sql_file") {
      status = RunSqlFile(d.arg);
    } else if (d.kind == "parallel") {
      std::vector<std::thread> threads;
      std::vector<Status> results(d.children.size());
      for (size_t i = 0; i < d.children.size(); ++i) {
        threads.emplace_back([this, &d, &results, i] { results[i] = RunDirective(d.children[i]); });
      }
      for (auto& t : threads) {
        t.join();
      }
      for (const auto& r : results) {
        RETURN_NOT_OK(r);
      }
    } else {
      return STATUS_FORMAT(InvalidArgument, "Unknown directive $0", d.kind);
    }
    LOG(INFO) << "Directive line " << d.line << " took "
              << (MonoTime::Now() - started).ToSeconds() << "s: " << status;
    return status;
  }

  string SqlDir() {
    const auto dir = JoinPathSegments(FLAGS_data_dir, "sql");
    WARN_NOT_OK(Env::Default()->CreateDirs(dir), "Cannot create " + dir);
    return dir;
  }

  string CurrentDb() {
    std::lock_guard l(db_mutex_);
    return current_db_;
  }

  Status RunSqlFile(const string& path) {
    return RunYsqlsh(CurrentDb(), path);
  }

  Status RunYsqlsh(const string& db, const string& file) {
    const auto exit_code = VERIFY_RESULT(Ysqlsh(db, {"-o", "/dev/null", "-f", file}));
    SCHECK_FORMAT(exit_code == 0, RuntimeError, "ysqlsh -f $0 exited with $1", file, exit_code);
    return Status::OK();
  }

  // Exit code of ysqlsh against the real tserver with the given trailing arguments.
  Result<int> Ysqlsh(const string& db, std::vector<string> args) {
    std::vector<string> argv = {
        ysqlsh_bin_, "-h", "127.0.0.1", "-p", std::to_string(FLAGS_pg_port),
        "-U", "yugabyte", "-d", db, "-v", "ON_ERROR_STOP=1", "-q",
    };
    argv.insert(argv.end(), args.begin(), args.end());
    Subprocess ysqlsh(ysqlsh_bin_, argv);
    RETURN_NOT_OK(ysqlsh.Start());
    int exit_code = 0;
    RETURN_NOT_OK(ysqlsh.Wait(&exit_code));
    return exit_code;
  }

  // The real tserver's postgres comes up seconds after the tserver registers.
  Status WaitForYsql() {
    const auto deadline = MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_register_timeout_sec);
    while (VERIFY_RESULT(Ysqlsh("yugabyte", {"-o", "/dev/null", "-c", "SELECT 1"})) != 0) {
      SCHECK(MonoTime::Now() < deadline, TimedOut, "YSQL did not become ready");
      SleepFor(1s);
    }
    LOG(INFO) << "YSQL is ready";
    return Status::OK();
  }

  // --- snapshot / restore ---------------------------------------------------------------------

  // Fleet settings a resume needs: 'key=value' lines, zones as 'cloud/region/zone=count'.
  string RegistryMeta() const {
    string zones;
    for (size_t i = 0; i < topology_.size(); ++i) {
      zones += Format("$0/$1/$2=$3\n", topology_[i].cloud, topology_[i].region,
                      topology_[i].zone, allocation_[i]);
    }
    return Format("num_tservers=$0\nreplication_factor=$1\n$2",
                  FLAGS_num_tservers, FLAGS_replication_factor, zones);
  }

  Status LoadRegistryMeta() {
    const auto path = JoinPathSegments(FLAGS_data_dir, kSnapshotMeta);
    std::ifstream meta(path);
    SCHECK_FORMAT(meta.good(), NotFound, "No $0: nothing to resume", path);
    topology_.clear();
    allocation_.clear();
    string line;
    while (std::getline(meta, line)) {
      const auto eq = line.find('=');
      SCHECK_FORMAT(eq != string::npos, Corruption, "$0: bad line '$1'", path, line);
      const auto key = line.substr(0, eq);
      const auto value = VERIFY_RESULT(CheckedStoInt<int32_t>(line.substr(eq + 1)));
      if (key == "num_tservers") {
        FLAGS_num_tservers = value;
      } else if (key == "replication_factor") {
        FLAGS_replication_factor = value;
      } else {
        const std::vector<string> parts = strings::Split(key, "/");
        SCHECK_FORMAT(parts.size() == 3, Corruption, "$0: bad zone '$1'", path, key);
        topology_.push_back(ZoneSpec{
            .cloud = parts[0], .region = parts[1], .zone = parts[2], .share = 0});
        allocation_.push_back(value);
      }
    }
    SCHECK_FORMAT(!topology_.empty() && FLAGS_num_tservers > 0, Corruption, "$0: no fleet",
                  path);
    for (size_t i = 0; i < topology_.size(); ++i) {
      topology_[i].share = static_cast<double>(allocation_[i]) / FLAGS_num_tservers;
    }
    return Status::OK();
  }

  // Hollow tablet registry and the settings it is only valid for, both in --data_dir.
  Status WriteRegistry() {
    std::map<string, HollowTServer::RegistryEntry> tablets;
    for (const auto& ts : registry_.tservers()) {
      ts->CollectTablets(&tablets);
    }
    const auto tablets_path = JoinPathSegments(FLAGS_data_dir, kSnapshotTablets);
    {
      std::ofstream out(tablets_path + ".tmp", std::ios::trunc);
      SCHECK_FORMAT(out.good(), IOError, "Cannot write $0", tablets_path);
      for (const auto& [tablet_id, entry] : tablets) {
        out << tablet_id << "\t" << entry.schema_version << "\t"
            << b2a_hex(entry.config.SerializeAsString()) << "\n";
      }
    }
    RETURN_NOT_OK(Env::Default()->RenameFile(tablets_path + ".tmp", tablets_path));
    std::ofstream(JoinPathSegments(FLAGS_data_dir, kSnapshotMeta), std::ios::trunc)
        << RegistryMeta();
    LOG(INFO) << "Registry written: " << tablets.size() << " hollow tablets";
    return Status::OK();
  }

  Status TakeSnapshot() {
    LOG(INFO) << "Snapshot: stopping the fleet, tserver and master";
    running_.store(false, std::memory_order_release);
    if (scheduler_thread_.joinable()) {
      scheduler_thread_.join();
    }
    if (replay_thread_.joinable()) {
      replay_thread_.join();
    }
    RETURN_NOT_OK(StopProcess(&tserver_process_, "tserver"));
    RETURN_NOT_OK(StopProcess(&master_process_, "master"));

    RETURN_NOT_OK(ResetDir(FLAGS_snapshot_dir));
    for (const auto* entry : {kMasterDataDir, kTServerDataDir, kSnapshotTablets, kSnapshotMeta}) {
      RETURN_NOT_OK(CopyTree(JoinPathSegments(FLAGS_data_dir, entry), FLAGS_snapshot_dir));
    }
    LOG(INFO) << "Snapshot written to " << FLAGS_snapshot_dir;
    return Status::OK();
  }

  Status StopProcess(std::unique_ptr<Subprocess>* process, const string& name) {
    if (!*process) {
      return Status::OK();
    }
    RETURN_NOT_OK((*process)->Kill(SIGTERM));
    int exit_code = 0;
    RETURN_NOT_OK((*process)->Wait(&exit_code));
    LOG(INFO) << name << " exited with " << exit_code;
    process->reset();
    WARN_NOT_OK(Env::Default()->DeleteFile(PidFilePath(name)), "Failed to delete pid file");
    return Status::OK();
  }

  Status RestoreSnapshotFiles() {
    SCHECK_FORMAT(Env::Default()->FileExists(JoinPathSegments(FLAGS_from_snapshot, kSnapshotMeta)),
                  NotFound, "No $0 in $1", kSnapshotMeta, FLAGS_from_snapshot);
    RETURN_NOT_OK(Env::Default()->CreateDirs(FLAGS_data_dir));
    for (const auto* entry : {kMasterDataDir, kTServerDataDir, kSnapshotTablets, kSnapshotMeta}) {
      const auto target = JoinPathSegments(FLAGS_data_dir, entry);
      if (Env::Default()->FileExists(target)) {
        RETURN_NOT_OK(Env::Default()->DeleteRecursively(target));
      }
      RETURN_NOT_OK(CopyTree(JoinPathSegments(FLAGS_from_snapshot, entry), FLAGS_data_dir));
    }
    LOG(INFO) << "Restored " << FLAGS_from_snapshot << " into " << FLAGS_data_dir;
    return Status::OK();
  }

  Status LoadHollowTablets() {
    std::ifstream in(JoinPathSegments(FLAGS_data_dir, kSnapshotTablets));
    SCHECK_FORMAT(in.good(), NotFound, "No $0 in $1", kSnapshotTablets, FLAGS_data_dir);
    size_t count = 0;
    for (string line; std::getline(in, line);) {
      const std::vector<string> fields = strings::Split(line, "\t");
      SCHECK_FORMAT(fields.size() == 3, Corruption, "Bad registry line: $0", line);
      const auto& tablet_id = fields[0];
      const auto schema_version = VERIFY_RESULT(CheckedStoInt<uint32_t>(fields[1]));
      consensus::RaftConfigPB config;
      SCHECK(config.ParseFromString(a2b_hex(fields[2])), Corruption,
             "Bad config for tablet $0", tablet_id);
      for (const auto& peer : config.peers()) {
        auto* ts = registry_.Find(peer.permanent_uuid());
        SCHECK(ts != nullptr, NotFound, "Tablet $0 names unknown tserver $1", tablet_id,
               peer.permanent_uuid());
        ts->AddTablet(tablet_id, config, schema_version);
      }
      ++count;
    }
    LOG(INFO) << "Loaded " << count << " hollow tablets from the previous run";
    return Status::OK();
  }

  Status WaitForReportsSettled() {
    const auto deadline = MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_register_timeout_sec);
    auto next_log = MonoTime::Now();
    while (true) {
      size_t settled = 0;
      for (const auto& ts : registry_.tservers()) {
        settled += ts->ReportsSettled();
      }
      if (settled == registry_.tservers().size()) {
        LOG(INFO) << "All hollow tablet reports accepted by the master";
        return Status::OK();
      }
      if (MonoTime::Now() >= next_log) {
        LOG(INFO) << "Waiting for tablet reports: " << settled << "/"
                  << registry_.tservers().size();
        next_log = MonoTime::Now() + 5s;
      }
      SCHECK(MonoTime::Now() < deadline, TimedOut, "Hollow tablet reports did not settle");
      SleepFor(500ms);
    }
  }

  void CollectorLoop() {
    std::ofstream csv;
    if (!FLAGS_csv_path.empty()) {
      const bool existed = Env::Default()->FileExists(FLAGS_csv_path);
      csv.open(FLAGS_csv_path, std::ios::app);
      if (!existed) {
        csv << "epoch_ms,elapsed_sec,phase,metric,value\n";
      }
    }
    const std::vector<string> substrings =
        strings::Split(FLAGS_metric_substrings, ",", strings::SkipEmpty());
    const auto url =
        Format("http://127.0.0.1:$0/prometheus-metrics", FLAGS_master_web_port);
    while (running_.load(std::memory_order_acquire)) {
      std::map<string, double> values;
      values["bench_heartbeats_ok_total"] = stats_.heartbeats_ok.load();
      values["bench_heartbeats_failed_total"] = stats_.heartbeats_failed.load();
      values["bench_lease_refreshes_ok_total"] = stats_.lease_refreshes_ok.load();
      values["bench_lease_refreshes_failed_total"] = stats_.lease_refreshes_failed.load();
      values["bench_connectivity_checks_ok_total"] = stats_.connectivity_checks_ok.load();
      values["bench_connectivity_checks_failed_total"] =
          stats_.connectivity_checks_failed.load();
      values["bench_heartbeat_rtt_us_sum"] = stats_.heartbeat_rtt_us_sum.load();
      values["bench_heartbeat_rtt_count"] = stats_.heartbeat_rtt_count.load();

      const auto pid = master_process_ ? master_process_->pid() : 0;
      if (pid > 0) {
        auto cpu = ProcessCpuSeconds(pid);
        if (cpu.ok()) {
          values["master_cpu_seconds_total"] = *cpu;
        }
        auto rss = ProcessRssBytes(pid);
        if (rss.ok()) {
          values["master_rss_bytes"] = *rss;
        }
        EasyCurl curl;
        faststring body;
        auto fetch_status = curl.FetchURL(url, &body);
        if (fetch_status.ok()) {
          ParsePrometheusMetrics(body.ToString(), substrings, &values);
        } else {
          YB_LOG_EVERY_N_SECS(WARNING, 30) << "Metrics fetch failed: " << fetch_status;
        }
      }

      const auto now = MonoTime::Now();
      const auto epoch_ms = GetCurrentTimeMicros() / 1000;
      const auto elapsed = (now - start_time_).ToSeconds();
      const auto phase = GetPhase();
      {
        std::lock_guard l(samples_mutex_);
        for (const auto& [metric, value] : values) {
          samples_.push_back(Sample{epoch_ms, elapsed, phase, metric, value});
          if (csv.is_open()) {
            csv << epoch_ms << "," << elapsed << "," << phase << "," << metric << ","
                << value << "\n";
          }
        }
      }
      if (csv.is_open()) {
        csv.flush();
      }
      for (int i = 0; i < FLAGS_metrics_interval_sec * 10 &&
                      running_.load(std::memory_order_acquire); ++i) {
        SleepFor(100ms);
      }
    }
  }

  void PrintSummary() {
    std::lock_guard l(samples_mutex_);
    // First/last steady-phase sample per metric.
    std::map<string, std::pair<Sample, Sample>> steady;
    for (const auto& sample : samples_) {
      if (sample.phase != kMeasuredPhase) {
        continue;
      }
      auto [it, inserted] = steady.emplace(sample.metric, std::make_pair(sample, sample));
      if (!inserted) {
        it->second.second = sample;
      }
    }
    if (steady.empty()) {
      LOG(WARNING) << "No scenario-phase samples collected";
      return;
    }
    const double window =
        steady.begin()->second.second.elapsed_sec - steady.begin()->second.first.elapsed_sec;
    auto delta = [&steady](const string& metric) -> double {
      auto it = steady.find(metric);
      return it == steady.end() ? 0 : it->second.second.value - it->second.first.value;
    };

    std::cout << "\n=== master-load-bench summary ===\n"
              << "tservers=" << FLAGS_num_tservers
              << " rf=" << FLAGS_replication_factor
              << " scenario_window_sec=" << window << "\n";
    if (window > 0) {
      std::cout << "master_cpu_pct="
                << 100.0 * delta("master_cpu_seconds_total") / window << "\n";
    }
    const auto rtt_count = delta("bench_heartbeat_rtt_count");
    if (rtt_count > 0) {
      std::cout << "driver_observed_heartbeat_rtt_ms_avg="
                << delta("bench_heartbeat_rtt_us_sum") / rtt_count / 1000.0 << "\n";
    }
    if (window > 0) {
      double hb_in = 0, hb_out = 0, all_in = 0, all_out = 0;
      for (const auto& [metric, pair] : steady) {
        const double d = pair.second.value - pair.first.value;
        if (metric.starts_with("service_request_bytes_yb_master_")) {
          all_in += d;
          if (metric.ends_with("MasterHeartbeat_TSHeartbeat")) hb_in = d;
        } else if (metric.starts_with("service_response_bytes_yb_master_")) {
          all_out += d;
          if (metric.ends_with("MasterHeartbeat_TSHeartbeat")) hb_out = d;
        }
      }
      std::cout << "heartbeat_to_master_mb_per_sec=" << hb_in / window / 1e6
                << " heartbeat_from_master_mb_per_sec=" << hb_out / window / 1e6 << "\n"
                << "all_rpc_to_master_mb_per_sec=" << all_in / window / 1e6
                << " all_rpc_from_master_mb_per_sec=" << all_out / window / 1e6 << "\n";
    }
    for (const auto& [metric, pair] : steady) {
      // With whole-master collection most series are methods that never ran; keep the
      // summary to series that have ever moved. The CSV still has everything.
      if (pair.second.value == 0 && pair.first.value == 0) {
        continue;
      }
      std::cout << metric << ": last=" << pair.second.value
                << " steady_delta=" << (pair.second.value - pair.first.value) << "\n";
    }
    if (mirror_state_) {
      mirror_state_->PrintSummary(std::cout);
    }
    std::cout.flush();
  }

  void SetPhase(const string& phase) {
    std::lock_guard l(phase_mutex_);
    phase_ = phase;
    LOG(INFO) << "Phase: " << phase;
  }

  string GetPhase() {
    std::lock_guard l(phase_mutex_);
    return phase_;
  }

  void Shutdown() {
    if (shut_down_) {
      return;
    }
    shut_down_ = true;
    running_.store(false, std::memory_order_release);
    if (scheduler_thread_.joinable()) {
      scheduler_thread_.join();
    }
    if (replay_thread_.joinable()) {
      replay_thread_.join();
    }
    if (collector_thread_.joinable()) {
      collector_thread_.join();
    }
    if (messenger_) {
      messenger_->UnregisterAllServices();
    }
    if (service_pool_) {
      service_pool_->Shutdown();
    }
    if (ts_service_pool_) {
      ts_service_pool_->Shutdown();
    }
    if (generic_service_pool_) {
      generic_service_pool_->Shutdown();
    }
    if (messenger_) {
      messenger_->Shutdown();
    }
    if (tserver_process_) {
      WARN_NOT_OK(tserver_process_->Kill(SIGTERM), "Failed to SIGTERM tserver");
      int exit_code = 0;
      WARN_NOT_OK(tserver_process_->Wait(&exit_code), "Failed to wait for tserver");
      tserver_process_.reset();
      WARN_NOT_OK(Env::Default()->DeleteFile(PidFilePath("tserver")),
                  "Failed to delete tserver pid file");
    }
    if (relay_) {
      relay_->Stop();
    }
    if (master_process_) {
      WARN_NOT_OK(master_process_->Kill(SIGTERM), "Failed to SIGTERM master");
      int exit_code = 0;
      WARN_NOT_OK(master_process_->Wait(&exit_code), "Failed to wait for master");
      master_process_.reset();
      WARN_NOT_OK(Env::Default()->DeleteFile(PidFilePath("master")),
                  "Failed to delete master pid file");
    }
  }

  HostPort master_addr_;
  MonoTime start_time_;
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_ =
      METRIC_ENTITY_server.Instantiate(&metric_registry_, "master_load_bench");

  std::unique_ptr<Subprocess> master_process_;
  std::unique_ptr<Subprocess> tserver_process_;
  std::unique_ptr<MirrorState> mirror_state_;
  std::unique_ptr<TcpRelay> relay_;
  rpc::ProxyPtr generic_proxy_;
  std::unique_ptr<rpc::Messenger> messenger_;
  std::unique_ptr<rpc::ProxyCache> proxy_cache_;
  std::unique_ptr<master::MasterClusterProxy> cluster_proxy_;
  rpc::ThreadPoolPtr service_thread_pool_;
  scoped_refptr<rpc::ServicePool> service_pool_;
  scoped_refptr<rpc::ServicePool> ts_service_pool_;
  scoped_refptr<rpc::ServicePool> generic_service_pool_;
  HollowTServerRegistry registry_;

  std::mutex universe_uuid_mutex_;
  string universe_uuid_ GUARDED_BY(universe_uuid_mutex_);

  BenchStats stats_;
  std::atomic<bool> running_{true};
  bool shut_down_ = false;
  std::thread scheduler_thread_;
  std::thread collector_thread_;
  std::thread replay_thread_;

  std::unique_ptr<client::YBClient> client_;
  string ysqlsh_bin_;
  std::vector<ZoneSpec> topology_;
  std::vector<int> allocation_;
  std::vector<Directive> scenario_;
  bool restoring_ = false;
  std::mutex db_mutex_;
  string current_db_ GUARDED_BY(db_mutex_) = "yugabyte";

  std::mutex phase_mutex_;
  string phase_ GUARDED_BY(phase_mutex_) = "start";
  std::mutex samples_mutex_;
  std::vector<Sample> samples_ GUARDED_BY(samples_mutex_);
};

}  // namespace

int BenchMain(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_logtostderr = true;
  InitGoogleLoggingSafe(argv[0]);

  MasterLoadBench bench;
  auto status = bench.Run();
  if (!status.ok()) {
    LOG(ERROR) << "Benchmark failed: " << status;
    return 1;
  }
  return 0;
}

}  // namespace tools
}  // namespace yb

int main(int argc, char** argv) {
  return yb::tools::BenchMain(argc, argv);
}
