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
#pragma once

#include "yb/gutil/macros.h"
#include "yb/gutil/ref_counted.h"

#include "yb/util/monotime.h"

#include "yb/tablet/tablet.h"

namespace yb {

class Counter;
template <class T>
class AtomicGauge;
class EventStats;
class MetricEntity;

namespace xrepl {

// Container for all metrics specific to a single tablet.
class XClusterTabletMetrics {
 public:
  explicit XClusterTabletMetrics(const scoped_refptr<MetricEntity>& metric_entity);

  // Reset all the metrics to 0, except for the rpc_* related metrics.
  void ClearMetrics();

  scoped_refptr<EventStats> rpc_payload_bytes_responded;
  scoped_refptr<Counter> rpc_heartbeats_responded;
  // For rpc_latency & rpcs_responded_count, use 'handler_latency_yb_cdc_CDCService_GetChanges'.

  // Info about ID last read by CDC Consumer.
  scoped_refptr<AtomicGauge<int64_t>> last_read_opid_term;
  scoped_refptr<AtomicGauge<int64_t>> last_read_opid_index;
  scoped_refptr<AtomicGauge<int64_t>> last_checkpoint_opid_index;
  scoped_refptr<AtomicGauge<uint64_t>> last_read_hybridtime;
  scoped_refptr<AtomicGauge<uint64_t>> last_read_physicaltime;
  scoped_refptr<AtomicGauge<uint64_t>> last_checkpoint_physicaltime;

  // Info about last majority-replicated OpID by CDC Producer (upon last poll).
  scoped_refptr<AtomicGauge<int64_t>> last_readable_opid_index;
  // For last_committed_hybridtime, use 'hybrid_clock_hybrid_time'.

  // Lag between commit time of last record polled and last record applied on producer.
  scoped_refptr<AtomicGauge<int64_t>> async_replication_sent_lag_micros;
  // Lag between last record applied on consumer and producer.
  scoped_refptr<AtomicGauge<int64_t>> async_replication_committed_lag_micros;

  // Info about if a tablet has fallen too far behind in replication.
  scoped_refptr<AtomicGauge<bool>> is_bootstrap_required;

  // Info on the received GetChanges requests.
  scoped_refptr<AtomicGauge<uint64_t>> last_getchanges_time;
  scoped_refptr<AtomicGauge<int64_t>> time_since_last_getchanges;

  // Info on the time till which the consumer is caught-up with the producer.
  scoped_refptr<AtomicGauge<uint64_t>> last_caughtup_physicaltime;

 private:
  scoped_refptr<MetricEntity> entity_;
};

class CDCSDKTabletMetrics {
 public:
  explicit CDCSDKTabletMetrics(const scoped_refptr<MetricEntity>& metric_entity);

  void ClearMetrics();

  // Lag between last committed record in the producer and last sent record.
  scoped_refptr<AtomicGauge<int64_t>> cdcsdk_sent_lag_micros;
  // Total traffic sent in bytes.
  scoped_refptr<Counter> cdcsdk_traffic_sent;
  // Total change events sent.
  scoped_refptr<Counter> cdcsdk_change_event_count;
  // Remaining expiry time of stream in milli seconds.
  scoped_refptr<AtomicGauge<uint64_t>> cdcsdk_expiry_time_ms;
  // Last sent physical time is used for calculating sent lag micros
  scoped_refptr<AtomicGauge<uint64_t>> cdcsdk_last_sent_physicaltime;
  // Lag between last committed record in the WAL and replication slot's restart time.
  scoped_refptr<AtomicGauge<uint64_t>> cdcsdk_flush_lag;

  // Per-phase latency histograms for the GetChanges RPC on the CDCSDK path.
  // (Total RPC latency is covered by the server-wide handler_latency_yb_cdc_CDCService_GetChanges.)
  // Time spent before entering GetChangesForCDCSDK: semaphore, validation, stream/tablet/leader
  // lookup, schema/enum/composite cache.
  scoped_refptr<EventStats> cdcsdk_get_changes_preflight_latency;
  // Time spent in GetLastCheckpoint (cdc_state read) when the client sends no from_op_id.
  scoped_refptr<EventStats> cdcsdk_get_last_checkpoint_latency;
  // Time spent in UpdateCheckpointAndActiveTime (cdc_state write) on EXPLICIT ack.
  scoped_refptr<EventStats> cdcsdk_update_checkpoint_latency;
  // Time spent reading WAL records (GetConsistentWALRecords / GetWALRecords).
  scoped_refptr<EventStats> cdcsdk_wal_read_latency;
  // Time in ProcessIntentsWithInvalidSchemaRetry (intent fetch + record population).
  scoped_refptr<EventStats> cdcsdk_process_intents_latency;
  // Time in tablet->GetIntentsForCDC alone (the IntentsDB read inside ProcessIntents).
  scoped_refptr<EventStats> cdcsdk_get_intents_latency;
  // Time in PopulateCDCSDKWriteRecordWithInvalidSchemaRetry for single-shard writes.
  scoped_refptr<EventStats> cdcsdk_populate_write_record_latency;
  // Time in client->GetTableSchemaFromSysCatalog (master round-trips).
  scoped_refptr<EventStats> cdcsdk_schema_lookup_latency;
  // Time in GetConsistentStreamSafeTime (waiting for safe time / txn load).
  scoped_refptr<EventStats> cdcsdk_safe_time_wait_latency;

  // Per-call batch-size histograms (observed once per successful GetChanges).
  scoped_refptr<EventStats> cdcsdk_wal_records_read;
  scoped_refptr<EventStats> cdcsdk_wal_bytes_read;
  scoped_refptr<EventStats> cdcsdk_intents_per_txn;
  scoped_refptr<EventStats> cdcsdk_response_records;
  scoped_refptr<EventStats> cdcsdk_response_bytes;

  // Per-optype counters incremented in the GetChangesForCDCSDK main loop.
  scoped_refptr<Counter> cdcsdk_write_ops_seen;
  scoped_refptr<Counter> cdcsdk_update_txn_ops_seen;
  scoped_refptr<Counter> cdcsdk_change_metadata_ops_seen;
  scoped_refptr<Counter> cdcsdk_truncate_ops_seen;
  scoped_refptr<Counter> cdcsdk_split_ops_seen;

  Result<std::string> TEST_GetAttribute(const std::string& key) const;

 private:
  scoped_refptr<MetricEntity> entity_;
};

class CDCServerMetrics {
 public:
  explicit CDCServerMetrics(const scoped_refptr<MetricEntity>& metric_entity_server);

  scoped_refptr<Counter> cdc_rpc_proxy_count;
  // Future Metric: scoped_refptr<Counter> cdc_rpc_error_count;

 private:
  scoped_refptr<MetricEntity> entity_;
};

}  // namespace xrepl
}  // namespace yb
