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
// master-heartbeat-bench: measures yb-master heartbeat-processing cost at high tserver counts
// without running real tservers.
//
// The driver spawns a real yb-master, then hosts N "hollow" tservers in one process. Each one
// listens on its own port (the master rejects host:port collisions between live tservers),
// registers via heartbeat, heartbeats every --heartbeat_interval_ms with the periodic
// tablet-metrics payload every --full_metrics_beat_period beats, refreshes its YSQL lease every
// second (the real per-second high-priority RPC), and acks CreateTablet so the master's catalog
// fills with genuine RUNNING tablets. Tables are created through the regular client API until
// each hollow tserver hosts ~--tablet_replicas_per_tserver replicas. The tool then samples
// master CPU, RSS and RPC metrics for --run_seconds and appends long-format CSV rows
// (epoch_ms,elapsed_sec,phase,metric,value) to --csv_path.
//
// One invocation measures one (num_tservers, tablet_replicas_per_tserver) point; sweep
// externally and re-run per fix commit to attribute improvements.
//
// Mirror mode (--tserver_bin set): additionally spawns one real yb-tserver whose master traffic
// flows through a byte-level TCP relay (--relay_port). The relay captures the real TSHeartbeat
// requests; hollow tservers resend the captured payloads with identity, tablet ids and report
// state rewritten, so the request side exercises real tserver heartbeat-building code and
// tserver-side fixes show up in the benchmark. The master's broadcast address points at the
// relay so leader resolution keeps the real tserver's traffic capturable (use_private_ip
// defaults to "never": clients prefer the broadcast address whenever one is set). The real
// tserver lives in its own placement zone and hosts its own RF1 table so no Raft group mixes
// real and hollow peers, which could never elect a leader.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <deque>
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
#include "yb/client/table_creator.h"
#include "yb/client/yb_table_name.h"

#include "yb/common/common_net.pb.h"
#include "yb/common/wire_protocol.h"

#include "yb/consensus/metadata.pb.h"

#include "yb/gutil/casts.h"
#include "yb/gutil/stringprintf.h"
#include "yb/gutil/strings/split.h"

#include "yb/master/master_cluster.proxy.h"
#include "yb/master/master_heartbeat.proxy.h"
#include "yb/master/master_ysql_lease.proxy.h"

#include "yb/rpc/messenger.h"
#include "yb/rpc/proxy.h"
#include "yb/rpc/remote_method.h"
#include "yb/rpc/rpc_controller.h"
#include "yb/rpc/rpc_header.pb.h"
#include "yb/rpc/service_pool.h"
#include "yb/rpc/thread_pool.h"
#include "yb/rpc/yb_rpc.h"

#include "yb/server/clock.h"
#include "yb/server/server_base.service.h"

#include "yb/tserver/tserver_admin.service.h"

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
#include "yb/util/subprocess.h"

DEFINE_NON_RUNTIME_int32(num_tservers, 100, "Number of hollow tservers to run.");
DEFINE_NON_RUNTIME_int32(tablet_replicas_per_tserver, 100,
    "Target tablet replicas hosted per hollow tserver.");
DEFINE_NON_RUNTIME_int32(replication_factor, 7, "Replication factor for created tables.");
DEFINE_NON_RUNTIME_int32(tablets_per_table, 2000,
    "Max tablets per created table; total tablet count is split into tables of this size.");
DEFINE_NON_RUNTIME_int32(run_seconds, 300, "Steady-state measurement window.");
DEFINE_NON_RUNTIME_int32(heartbeat_interval_ms, 1000, "Hollow tserver heartbeat interval.");
DEFINE_NON_RUNTIME_int32(full_metrics_beat_period, 5,
    "Send the tablet metrics payload (storage_metadata + leader_info) every Nth heartbeat, "
    "matching the real tserver's 5s metrics cadence.");
DEFINE_NON_RUNTIME_bool(send_lease_refreshes, true,
    "Send the per-second RefreshYsqlLease RPC from every hollow tserver.");
DEFINE_NON_RUNTIME_string(master_bin, "", "Path to the yb-master binary (required).");
DEFINE_NON_RUNTIME_string(data_dir, "",
    "Fresh scratch directory for master data and logs (required).");
DEFINE_NON_RUNTIME_string(csv_path, "",
    "Append long-format samples to this CSV file; empty = summary on stdout only.");
DEFINE_NON_RUNTIME_int32(master_rpc_port, 21100, "RPC port for the spawned master.");
DEFINE_NON_RUNTIME_int32(master_web_port, 21101, "Webserver port for the spawned master.");
DEFINE_NON_RUNTIME_string(master_flags, "",
    "Comma-separated extra --flag=value arguments passed to yb-master.");
DEFINE_NON_RUNTIME_string(tserver_bin, "",
    "Path to the yb-tserver binary. When set, enables mirror mode: one real tserver's master "
    "traffic is captured through a TCP relay and its heartbeat payloads are resent by the "
    "hollow tservers with identity rewritten.");
DEFINE_NON_RUNTIME_int32(relay_port, 21102,
    "Listen port of the capture relay between the real tserver and the master (mirror mode).");
DEFINE_NON_RUNTIME_int32(tserver_rpc_port, 21103,
    "RPC port for the spawned real tserver (mirror mode).");
DEFINE_NON_RUNTIME_int32(tserver_web_port, 21104,
    "Webserver port for the spawned real tserver (mirror mode).");
DEFINE_NON_RUNTIME_string(tserver_flags, "",
    "Comma-separated extra --flag=value arguments passed to yb-tserver (mirror mode).");
DEFINE_NON_RUNTIME_int32(metrics_interval_sec, 5, "Sampling interval for the collector.");
DEFINE_NON_RUNTIME_int32(register_timeout_sec, 300,
    "Timeout for all hollow tservers to become live on the master.");
DEFINE_NON_RUNTIME_int32(create_timeout_sec, 1800, "Timeout per created table.");
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

const char* const kNamespace = "hb_bench";
// The real tserver's placement zone; hollow tservers use zone0..zone2, so pinning tables per
// zone keeps real and hollow replicas out of each other's Raft groups.
const char* const kRealTServerZone = "zone-real";

// One fake tserver: identity, hosted tablet replicas, and heartbeat/report/lease state.
class HollowTServer {
 public:
  HollowTServer(int index, uint64_t instance_seqno)
      : index_(index),
        uuid_(StringPrintf("%032x", index + 1)),
        instance_seqno_(instance_seqno) {}

  const string& uuid() const { return uuid_; }
  int index() const { return index_; }

  void SetHostPort(const HostPort& host_port) { host_port_ = host_port; }
  const HostPort& host_port() const { return host_port_; }

  void AddTablet(const string& tablet_id, const consensus::RaftConfigPB& config) {
    // All replicas receive the same config, so hashing the tablet id picks the same
    // leader everywhere and the master sees a consistent consensus state.
    const auto& leader_uuid =
        config.peers(std::hash<string>()(tablet_id) % config.peers_size()).permanent_uuid();
    std::lock_guard l(mutex_);
    auto& info = tablets_[tablet_id];
    info.config = config;
    info.leader_uuid = leader_uuid;
    dirty_.insert(tablet_id);
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
      auto* cloud = reg->mutable_cloud_info();
      cloud->set_placement_cloud("cloud1");
      cloud->set_placement_region("region1");
      cloud->set_placement_zone(Format("zone$0", index_ % 3));
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
  MonoTime last_metrics_beat;
  MonoTime heartbeat_sent_time;
  MonoTime lease_sent_time;
  std::atomic<bool> heartbeat_in_flight{false};
  std::atomic<bool> lease_in_flight{false};

  // Reused across calls; safe because at most one of each RPC is in flight.
  master::TSHeartbeatRequestPB hb_req;
  master::TSHeartbeatResponsePB hb_resp;
  rpc::RpcController hb_controller;
  master::RefreshYsqlLeaseRequestPB lease_req;
  master::RefreshYsqlLeaseResponsePB lease_resp;
  rpc::RpcController lease_controller;
  std::unique_ptr<master::MasterHeartbeatProxy> heartbeat_proxy;
  std::unique_ptr<master::MasterYsqlLeaseProxy> lease_proxy;

 private:
  struct TabletInfo {
    consensus::RaftConfigPB config;
    string leader_uuid;
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
    tablet->set_schema_version(0);
    auto* cstate = tablet->mutable_committed_consensus_state();
    cstate->set_current_term(1);
    cstate->set_leader_uuid(info.leader_uuid);
    *cstate->mutable_config() = info.config;
  }

  const int index_;
  const string uuid_;
  const uint64_t instance_seqno_;
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

#define HOLLOW_UNSUPPORTED_METHOD(Method, ReqType, RespType) \
  void Method(const ReqType*, RespType*, rpc::RpcContext context) override { \
    context.RespondFailure(STATUS(NotSupported, #Method " not implemented by hollow tserver")); \
  }

  HOLLOW_UNSUPPORTED_METHOD(PrepareDeleteTransactionTablet,
                            tserver::PrepareDeleteTransactionTabletRequestPB,
                            tserver::PrepareDeleteTransactionTabletResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(AlterSchema, tablet::ChangeMetadataRequestPB,
                            tserver::ChangeMetadataResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(GetSafeTime, tserver::GetSafeTimeRequestPB,
                            tserver::GetSafeTimeResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(BackfillIndex, tserver::BackfillIndexRequestPB,
                            tserver::BackfillIndexResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(BackfillDone, tablet::ChangeMetadataRequestPB,
                            tserver::ChangeMetadataResponsePB)
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
  HOLLOW_UNSUPPORTED_METHOD(WaitForYsqlBackendsCatalogVersion,
                            tserver::WaitForYsqlBackendsCatalogVersionRequestPB,
                            tserver::WaitForYsqlBackendsCatalogVersionResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(TestRetry, tserver::TestRetryRequestPB, tserver::TestRetryResponsePB)
  HOLLOW_UNSUPPORTED_METHOD(UpdateTransactionTablesVersion,
                            tserver::UpdateTransactionTablesVersionRequestPB,
                            tserver::UpdateTransactionTablesVersionResponsePB)
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
  HollowTServerRegistry* registry_;
};

// Answers the tserver connectivity poller's Ping so hollow tservers look reachable, as real
// peers would. One registration covers the fleet: every hollow port shares the driver
// messenger, and service dispatch is per-messenger, not per-socket.
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
    static const std::set<string> kReplayMethods = {
        "GetMasterRegistration", "ListMasters", "GetAutoFlagsConfig", "ListTabletServers",
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
    out << "mirror: heartbeats_captured=" << heartbeats_captured_
        << " light_templates=" << light_templates_
        << " metrics_templates=" << metrics_templates_
        << " parse_failures=" << parse_failures_
        << " measured_heartbeat_interval_ms=" << interval_ms_
        << " measured_metrics_period_ms=" << metrics_period_ms_ << "\n";
    for (const auto& [key, c] : methods_) {
      out << "mirror_method " << key << ": seen=" << c.seen
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

class HeartbeatBench {
 public:
  Status Run() {
    SCHECK(!FLAGS_master_bin.empty(), InvalidArgument, "--master_bin is required");
    SCHECK(!FLAGS_data_dir.empty(), InvalidArgument, "--data_dir is required");
    SCHECK_GT(FLAGS_num_tservers, 0, InvalidArgument, "--num_tservers must be positive");
    SCHECK_GE(FLAGS_num_tservers, FLAGS_replication_factor, InvalidArgument,
              "Need at least replication_factor tservers");

    master_addr_ = HostPort("127.0.0.1", FLAGS_master_rpc_port);
    start_time_ = MonoTime::Now();

    RETURN_NOT_OK(StartMaster());
    if (!FLAGS_tserver_bin.empty()) {
      mirror_state_ = std::make_unique<MirrorState>();
      relay_ = std::make_unique<TcpRelay>(
          narrow_cast<uint16_t>(FLAGS_relay_port), narrow_cast<uint16_t>(FLAGS_master_rpc_port),
          mirror_state_.get());
      RETURN_NOT_OK(relay_->Start());
      RETURN_NOT_OK(StartRealTserver());
    }
    RETURN_NOT_OK(StartHollowTservers());

    collector_thread_ = std::thread([this] { CollectorLoop(); });

    SetPhase("register");
    scheduler_thread_ = std::thread([this] { SchedulerLoop(); });
    if (mirror_state_) {
      replay_thread_ = std::thread([this] { ReplayLoop(); });
    }
    RETURN_NOT_OK(WaitForRegistration());

    SetPhase("create");
    RETURN_NOT_OK(CreateTables());

    SetPhase("steady");
    LOG(INFO) << "Steady state: measuring for " << FLAGS_run_seconds << "s";
    SleepFor(MonoDelta::FromSeconds(FLAGS_run_seconds));

    SetPhase("done");
    Shutdown();
    PrintSummary();
    return Status::OK();
  }

  ~HeartbeatBench() {
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
    const auto data_dir = JoinPathSegments(FLAGS_data_dir, "master-data");
    const auto log_dir = JoinPathSegments(FLAGS_data_dir, "master-logs");
    auto* env = Env::Default();
    RETURN_NOT_OK(KillLeftoverProcess(env, "master", "yb-master"));
    // Reruns must start from an empty catalog; only the tool's own subdirs are wiped.
    for (const auto& dir : {data_dir, log_dir}) {
      if (env->FileExists(dir)) {
        RETURN_NOT_OK(env->DeleteRecursively(dir));
      }
      RETURN_NOT_OK(env->CreateDirs(dir));
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
        "--enable_ysql=false",
        "--max_create_tablets_per_ts=1000000",
        "--callhome_enabled=false",
    };
    if (!FLAGS_tserver_bin.empty()) {
      // Every address the master hands out (leader hostport, master_config peers) must point at
      // the relay so the real tserver's traffic stays capturable; with use_private_ip=never
      // (the default) clients always prefer the broadcast address.
      argv.push_back(Format("--server_broadcast_addresses=127.0.0.1:$0", FLAGS_relay_port));
    }
    const std::vector<string> extra_flags =
        strings::Split(FLAGS_master_flags, ",", strings::SkipEmpty());
    argv.insert(argv.end(), extra_flags.begin(), extra_flags.end());
    master_process_ = std::make_unique<Subprocess>(FLAGS_master_bin, argv);
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
    if (!FLAGS_tserver_bin.empty()) {
      generic_proxy_ = proxy_cache_->GetProxy(
          master_addr_, /* protocol= */ nullptr, /* resolve_cache_timeout= */ MonoDelta());
    }
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
    const auto data_dir = JoinPathSegments(FLAGS_data_dir, "ts-data");
    const auto log_dir = JoinPathSegments(FLAGS_data_dir, "ts-logs");
    auto* env = Env::Default();
    RETURN_NOT_OK(KillLeftoverProcess(env, "tserver", "yb-tserver"));
    for (const auto& dir : {data_dir, log_dir}) {
      if (env->FileExists(dir)) {
        RETURN_NOT_OK(env->DeleteRecursively(dir));
      }
      RETURN_NOT_OK(env->CreateDirs(dir));
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
        "--enable_ysql=false",
        "--placement_cloud=cloud1",
        "--placement_region=region1",
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
    for (int i = 0; i < FLAGS_num_tservers; ++i) {
      auto ts = std::make_unique<HollowTServer>(i, instance_seqno);
      Endpoint bound_endpoint;
      RETURN_NOT_OK(messenger_->ListenAddress(factory, Endpoint(), &bound_endpoint));
      ts->SetHostPort(HostPort("127.0.0.1", bound_endpoint.port()));
      ts->heartbeat_proxy = std::make_unique<master::MasterHeartbeatProxy>(
          proxy_cache_.get(), master_addr_);
      ts->lease_proxy = std::make_unique<master::MasterYsqlLeaseProxy>(
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
    // Stagger initial sends across the interval so load is even, as in a real cluster; metrics
    // beats are staggered across their own period the same way.
    const auto& tservers = registry_.tservers();
    auto now = MonoTime::Now();
    const auto metrics_period = hb_interval * FLAGS_full_metrics_beat_period;
    for (size_t i = 0; i < tservers.size(); ++i) {
      tservers[i]->next_heartbeat = now + hb_interval * i / tservers.size();
      tservers[i]->next_lease_refresh =
          now + lease_interval * i / tservers.size() + lease_interval / 2;
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
    const int expected = FLAGS_num_tservers + (mirror_state_ ? 1 : 0);
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

  Status CreateTables() {
    auto client = VERIFY_RESULT(client::YBClientBuilder()
        .add_master_server_addr(master_addr_.ToString())
        .default_admin_operation_timeout(MonoDelta::FromSeconds(600))
        .Build());
    RETURN_NOT_OK(client->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));

    client::YBSchema schema;
    client::YBSchemaBuilder schema_builder;
    schema_builder.AddColumn("k")->Type(DataType::INT32)->HashPrimaryKey()->NotNull();
    schema_builder.AddColumn("v")->Type(DataType::INT32);
    RETURN_NOT_OK(schema_builder.Build(&schema));

    auto create_table = [&](const string& name, int32_t num_tablets,
                            const ReplicationInfoPB* replication) -> Status {
      const client::YBTableName table_name(YQL_DATABASE_CQL, kNamespace, name);
      std::unique_ptr<client::YBTableCreator> creator(client->NewTableCreator());
      creator->table_name(table_name)
          .schema(&schema)
          .num_tablets(num_tablets)
          .wait(false);
      if (replication != nullptr) {
        creator->replication_info(*replication);
      }
      RETURN_NOT_OK(creator->Create());
      const auto deadline =
          MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_create_timeout_sec);
      while (true) {
        bool in_progress = false;
        RETURN_NOT_OK(client->IsCreateTableInProgress(table_name, &in_progress));
        if (!in_progress) {
          break;
        }
        SCHECK(MonoTime::Now() < deadline, TimedOut,
               "Table $0 not running within timeout", table_name.ToString());
        SleepFor(1s);
      }
      return Status::OK();
    };

    auto make_cloud_info = [](const string& zone) {
      CloudInfoPB cloud;
      cloud.set_placement_cloud("cloud1");
      cloud.set_placement_region("region1");
      cloud.set_placement_zone(zone);
      return cloud;
    };

    // In mirror mode the real tserver gets its own zone-pinned RF1 tables so its heartbeats
    // carry a realistic tablet load without ever sharing a Raft group with hollow peers.
    ReplicationInfoPB hollow_replication;
    const ReplicationInfoPB* hollow_replication_ptr = nullptr;
    if (mirror_state_) {
      ReplicationInfoPB real_replication;
      auto* live = real_replication.mutable_live_replicas();
      live->set_num_replicas(1);
      auto* block = live->add_placement_blocks();
      *block->mutable_cloud_info() = make_cloud_info(kRealTServerZone);
      block->set_min_num_replicas(1);

      auto* hollow_live = hollow_replication.mutable_live_replicas();
      hollow_live->set_num_replicas(FLAGS_replication_factor);
      for (int zone = 0; zone < 3; ++zone) {
        const int min_replicas =
            FLAGS_replication_factor / 3 + (zone < FLAGS_replication_factor % 3 ? 1 : 0);
        if (min_replicas == 0) {
          continue;
        }
        auto* hollow_block = hollow_live->add_placement_blocks();
        *hollow_block->mutable_cloud_info() = make_cloud_info(Format("zone$0", zone));
        hollow_block->set_min_num_replicas(min_replicas);
      }
      hollow_replication_ptr = &hollow_replication;

      const int64_t real_tablets = FLAGS_tablet_replicas_per_tserver;
      LOG(INFO) << "Creating " << real_tablets << " RF1 tablets on the real tserver";
      int64_t real_created = 0;
      int real_table_idx = 0;
      while (real_created < real_tablets) {
        const auto num_tablets = narrow_cast<int32_t>(
            std::min<int64_t>(FLAGS_tablets_per_table, real_tablets - real_created));
        RETURN_NOT_OK(create_table(
            Format("real_table_$0", real_table_idx++), num_tablets, &real_replication));
        real_created += num_tablets;
        LOG(INFO) << "Created real-tserver tablets: " << real_created << "/" << real_tablets;
      }
    }

    const int64_t total_tablets =
        static_cast<int64_t>(FLAGS_num_tservers) * FLAGS_tablet_replicas_per_tserver /
        FLAGS_replication_factor;
    LOG(INFO) << "Creating " << total_tablets << " tablets (RF" << FLAGS_replication_factor
              << ", " << FLAGS_tablet_replicas_per_tserver << " replicas/tserver)";
    int64_t created = 0;
    int table_idx = 0;
    while (created < total_tablets) {
      const auto num_tablets =
          narrow_cast<int32_t>(std::min<int64_t>(FLAGS_tablets_per_table,
                                                 total_tablets - created));
      RETURN_NOT_OK(create_table(
          Format("bench_table_$0", table_idx++), num_tablets, hollow_replication_ptr));
      created += num_tablets;
      LOG(INFO) << "Created bench_table_" << (table_idx - 1) << ": " << created << "/"
                << total_tablets << " tablets";
    }
    return Status::OK();
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
      if (sample.phase != "steady") {
        continue;
      }
      auto [it, inserted] = steady.emplace(sample.metric, std::make_pair(sample, sample));
      if (!inserted) {
        it->second.second = sample;
      }
    }
    if (steady.empty()) {
      LOG(WARNING) << "No steady-phase samples collected";
      return;
    }
    const double window =
        steady.begin()->second.second.elapsed_sec - steady.begin()->second.first.elapsed_sec;
    auto delta = [&steady](const string& metric) -> double {
      auto it = steady.find(metric);
      return it == steady.end() ? 0 : it->second.second.value - it->second.first.value;
    };

    std::cout << "\n=== master-heartbeat-bench summary ===\n"
              << "tservers=" << FLAGS_num_tservers
              << " tablet_replicas_per_tserver=" << FLAGS_tablet_replicas_per_tserver
              << " rf=" << FLAGS_replication_factor
              << " steady_window_sec=" << window << "\n";
    if (window > 0) {
      std::cout << "master_cpu_pct="
                << 100.0 * delta("master_cpu_seconds_total") / window << "\n";
    }
    const auto rtt_count = delta("bench_heartbeat_rtt_count");
    if (rtt_count > 0) {
      std::cout << "driver_observed_heartbeat_rtt_ms_avg="
                << delta("bench_heartbeat_rtt_us_sum") / rtt_count / 1000.0 << "\n";
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
      METRIC_ENTITY_server.Instantiate(&metric_registry_, "master_heartbeat_bench");

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

  HeartbeatBench bench;
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
