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

#include "yb/tserver/tserver_metrics_heartbeat_data_provider.h"

#include "yb/consensus/log.h"
#include "yb/consensus/raft_consensus.h"

#include "yb/master/master_heartbeat.pb.h"

#include "yb/tablet/tablet.h"
#include "yb/tablet/tablet_peer.h"

#include "yb/tserver/xcluster_consumer_if.h"
#include "yb/tserver/tablet_server.h"
#include "yb/tserver/ts_tablet_manager.h"
#include "yb/tserver/tserver_service.service.h"

#include "yb/util/logging.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/metrics.h"

DEFINE_RUNTIME_int32(tserver_heartbeat_metrics_interval_ms, 5000,
    "Interval (in milliseconds) at which tserver sends its metrics in a heartbeat to master.");

DEFINE_RUNTIME_bool(tserver_heartbeat_metrics_add_drive_data, true,
            "Add drive data to metrics which tserver sends to master");

DEFINE_RUNTIME_bool(tserver_heartbeat_metrics_add_replication_status, true,
            "Add replication status to metrics tserver sends to master");

DEFINE_RUNTIME_bool(tserver_heartbeat_metrics_add_leader_info, true,
            "Add leader info to metrics tserver sends to master");

DEFINE_RUNTIME_AUTO_bool(enable_load_balancer_heat_telemetry, kLocalPersisted, false, true,
    "When true, leader tservers emit per-tablet-peer read/write op rates in each heartbeat, and "
    "the master aggregates them per-tserver for the cluster load balancer. Changes no balancing "
    "decisions on its own; phase 3 of heat-aware balancing will consume the aggregate.");

DECLARE_uint64(rocksdb_max_file_size_for_compaction);

using namespace std::literals;

namespace yb {
namespace tserver {

TServerMetricsHeartbeatDataProvider::TServerMetricsHeartbeatDataProvider(TabletServer* server)
    : PeriodicalHeartbeatDataProvider(server), start_time_(MonoTime::Now()) {}

MonoDelta TServerMetricsHeartbeatDataProvider::Period() const {
  return MonoDelta::FromMilliseconds(FLAGS_tserver_heartbeat_metrics_interval_ms);
}

void TServerMetricsHeartbeatDataProvider::DoAddData(
    bool needs_full_tablet_report, master::TSHeartbeatRequestPB* req) {
  // Get the total memory used.
  size_t mem_usage = MemTracker::GetRootTracker()->GetUpdatedConsumption(true /* force */);
  auto* metrics = req->mutable_metrics();
  metrics->set_total_ram_usage(static_cast<int64_t>(mem_usage));
  VLOG_WITH_PREFIX(4) << "Total Memory Usage: " << mem_usage;

  uint64_t total_file_sizes = 0;
  uint64_t uncompressed_file_sizes = 0;
  uint64_t num_files = 0;

  bool no_full_tablet_report = !req->has_tablet_report() || req->tablet_report().is_incremental();
  bool should_add_tablet_data =
      FLAGS_tserver_heartbeat_metrics_add_drive_data && no_full_tablet_report;

  // Time delta since the last heartbeat-metrics run, used below to compute per-leader op rates.
  const MonoDelta heartbeat_interval = CoarseMonoClock::Now() - prev_run_time();
  const double heartbeat_interval_secs = heartbeat_interval.ToSeconds();

  // Rebuild per-tablet prev counters as we iterate; drops entries for tablets that are no longer
  // present on this tserver. Avoids a separate cleanup pass.
  std::unordered_map<TabletId, PrevTabletOpCounts> next_prev_tablet_op_counts;
  next_prev_tablet_op_counts.reserve(prev_tablet_op_counts_.size());

  for (const auto& tablet_peer : server().tablet_manager()->GetTabletPeers()) {
    if (tablet_peer) {
      auto tablet = tablet_peer->shared_tablet_maybe_null();
      if (tablet) {
        auto on_disk_size_info = tablet_peer->GetOnDiskSizeInfo();
        total_file_sizes += on_disk_size_info.sst_files_disk_size;
        uncompressed_file_sizes += on_disk_size_info.uncompressed_sst_files_disk_size;
        num_files += tablet->GetCurrentVersionNumSSTFiles();

        // Sample and refresh per-tablet op baselines for every tablet, every heartbeat —
        // independent of should_add_tablet_data and independent of whether this peer is currently
        // a leader. On full-tablet-report heartbeats the storage-metadata block below is skipped,
        // but prev_run_time() still advances; without this unconditional refresh the next
        // incremental heartbeat would compute a rate by subtracting counters from a much older
        // sample while dividing by only one heartbeat interval, inflating the reported rate.
        const uint64_t cur_reads = tablet->GetReadOpsServed();
        const uint64_t cur_writes = tablet->GetWriteOpsServed();
        next_prev_tablet_op_counts[tablet_peer->tablet_id()] = {cur_reads, cur_writes};

        if (should_add_tablet_data && tablet_peer->log_available() &&
            CanServeTabletData(tablet_peer->tablet_metadata()->tablet_data_state())) {
          auto storage_metadata = req->add_storage_metadata();
          storage_metadata->set_tablet_id(tablet_peer->tablet_id());
          storage_metadata->set_sst_file_size(on_disk_size_info.sst_files_disk_size);
          storage_metadata->set_wal_file_size(on_disk_size_info.wal_files_disk_size);
          storage_metadata->set_uncompressed_sst_file_size(
              on_disk_size_info.uncompressed_sst_files_disk_size);
          storage_metadata->set_may_have_orphaned_post_split_data(
                tablet->MayHaveOrphanedPostSplitData());
          storage_metadata->set_total_size(on_disk_size_info.total_on_disk_size);
          if (FLAGS_tserver_heartbeat_metrics_add_leader_info) {
            auto consensus_result = tablet_peer->GetRaftConsensus();
            if (consensus_result) {
              MicrosTime ht_lease_exp;
              consensus::LeaderLeaseStatus leader_lease_status =
                  consensus_result.get()->GetLeaderLeaseStatusIfLeader(&ht_lease_exp);
              auto leader_info = req->add_leader_info();
              leader_info->set_tablet_id(tablet_peer->tablet_id());
              leader_info->set_leader_lease_status(leader_lease_status);
              if (leader_lease_status == consensus::LeaderLeaseStatus::HAS_LEASE) {
                leader_info->set_ht_lease_expiration(ht_lease_exp);
              }

              // Emit heat rates only when this peer is the recognized active leader. Followers
              // may still serve reads (and their op counters still advance), but those counters
              // must not flow into the master's leader-heat aggregate or they would inflate
              // leader_tablet_count on every tserver holding a follower replica.
              if (FLAGS_enable_load_balancer_heat_telemetry &&
                  leader_lease_status == consensus::LeaderLeaseStatus::HAS_LEASE) {
                const auto prev_it = prev_tablet_op_counts_.find(tablet_peer->tablet_id());
                if (prev_it != prev_tablet_op_counts_.end() && heartbeat_interval_secs > 0) {
                  const uint64_t prev_reads = prev_it->second.reads;
                  const uint64_t prev_writes = prev_it->second.writes;
                  // Guard against counter reset across a leader step-down / new tablet-peer
                  // instance: treat a negative delta as zero rather than a huge spike.
                  const double reads_per_sec = (cur_reads >= prev_reads) ?
                      (static_cast<double>(cur_reads - prev_reads) / heartbeat_interval_secs) : 0;
                  const double writes_per_sec = (cur_writes >= prev_writes) ?
                      (static_cast<double>(cur_writes - prev_writes) / heartbeat_interval_secs) : 0;
                  leader_info->set_leader_read_ops_per_sec(reads_per_sec);
                  leader_info->set_leader_write_ops_per_sec(writes_per_sec);
                }
              }
            }
          }
        }

        if (no_full_tablet_report) {
          auto full_compaction_status = req->add_full_compaction_statuses();
          full_compaction_status->set_tablet_id(tablet->tablet_id());
          if (tablet->HasActiveFullCompaction()) {
            full_compaction_status->set_full_compaction_state(tablet::COMPACTING);
          } else {
            full_compaction_status->set_full_compaction_state(tablet::IDLE);
          }
          full_compaction_status->set_last_full_compaction_time(
              tablet->metadata()->last_full_compaction_time());
        }
      }
    }
  }
  // Swap in the freshly-built prev-counter map; any entries not refreshed above (e.g. tablets
  // that were removed from this tserver) are dropped.
  prev_tablet_op_counts_ = std::move(next_prev_tablet_op_counts);

  // Report xCluster consumer heartbeat info via the metric collector only when a full report is
  // required. The partial reports are sent via the regular heartbeat request.
  if (needs_full_tablet_report && FLAGS_tserver_heartbeat_metrics_add_replication_status) {
    auto xcluster_consumer = server().GetXClusterConsumer();
    if (xcluster_consumer != nullptr) {
      xcluster_consumer->PopulateMasterHeartbeatRequest(req, needs_full_tablet_report);
    }
  }

  metrics->set_total_sst_file_size(total_file_sizes);
  metrics->set_uncompressed_sst_file_size(uncompressed_file_sizes);
  metrics->set_num_sst_files(num_files);

  // Get the total number of read and write operations.
  auto reads_hist = server().GetMetricsHistogram(
      TabletServerServiceRpcMethodIndexes::kRead);
  uint64_t num_reads = (reads_hist != nullptr) ? reads_hist->TotalCount() : 0;

  auto writes_hist = server().GetMetricsHistogram(
      TabletServerServiceRpcMethodIndexes::kWrite);
  uint64_t num_writes = (writes_hist != nullptr) ? writes_hist->TotalCount() : 0;

  // Calculate the read and write ops per second.
  MonoDelta diff = CoarseMonoClock::Now() - prev_run_time();
  double_t div = diff.ToSeconds();

  double rops_per_sec = (div > 0 && num_reads > 0) ?
      (static_cast<double>(num_reads - prev_reads_) / div) : 0;

  double wops_per_sec = (div > 0 && num_writes > 0) ?
      (static_cast<double>(num_writes - prev_writes_) / div) : 0;

  prev_reads_ = num_reads;
  prev_writes_ = num_writes;
  metrics->set_read_ops_per_sec(rops_per_sec);
  metrics->set_write_ops_per_sec(wops_per_sec);
  uint64_t uptime_seconds = CalculateUptime();

  metrics->set_uptime_seconds(uptime_seconds);
  // If the "max file size for compaction" flag is greater than 0, then tablet splitting should
  // be disabled for tablets with a default TTL.
  metrics->set_disable_tablet_split_if_default_ttl(FLAGS_rocksdb_max_file_size_for_compaction > 0);

  VLOG_WITH_PREFIX(4) << "Read Ops per second: " << rops_per_sec;
  VLOG_WITH_PREFIX(4) << "Write Ops per second: " << wops_per_sec;
  VLOG_WITH_PREFIX(4) << "Total SST File Sizes: "<< total_file_sizes;
  VLOG_WITH_PREFIX(4) << "Uptime seconds: "<< uptime_seconds;

  if (FLAGS_tserver_heartbeat_metrics_add_drive_data) {
    for (const std::string& path : server().fs_manager()->GetFsRootDirs()) {
      auto stat = server().GetEnv()->GetFilesystemStatsBytes(path.c_str());
      if (!stat.ok()) {
        continue;
      }
      auto* path_metric = metrics->add_path_metrics();
      path_metric->set_path_id(path);
      path_metric->set_used_space(stat->used_space);
      path_metric->set_total_space(stat->total_space);
    }
  }
}

uint64_t TServerMetricsHeartbeatDataProvider::CalculateUptime() {
  MonoDelta delta = MonoTime::Now().GetDeltaSince(start_time_);
  uint64_t uptime_seconds = static_cast<uint64_t>(delta.ToSeconds());
  return uptime_seconds;
}


} // namespace tserver
} // namespace yb
