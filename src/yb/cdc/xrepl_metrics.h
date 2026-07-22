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

  Result<std::string> TEST_GetAttribute(const std::string& key) const;

 private:
  scoped_refptr<MetricEntity> entity_;
};

// Container for StreamWAL (checkpoint-less, stream-id-less) per-tablet metrics.
//
// Unlike CDCSDKTabletMetrics / XClusterTabletMetrics -- which live on a separate
// per-(stream_id, tablet) metric entity -- these are attached directly to the
// tablet's own metric entity. There is exactly one StreamWAL consumer per
// tablet, so no stream_id keying is needed; the metrics aggregate at the table
// level (like every other tablet metric) and carry no stream_id / slot_name
// attribution.
class StreamWALTabletMetrics {
 public:
  explicit StreamWALTabletMetrics(const scoped_refptr<MetricEntity>& metric_entity);

  void ClearMetrics();

  // Total decoded change records sent over StreamWAL for this tablet.
  scoped_refptr<Counter> streamwal_records_sent;
  // Total decoded record payload bytes sent over StreamWAL for this tablet.
  scoped_refptr<Counter> streamwal_traffic_sent;
  // Lag between the leader's safe time and the commit time of the last record
  // sent over StreamWAL. Aggregated with kMax -> worst (most-behind) tablet.
  scoped_refptr<AtomicGauge<int64_t>> streamwal_sent_lag_micros;
  // WAL ops between the leader tip and the read cursor
  // (leader_tip.index - next_op_id.index). Aggregated with kMax.
  scoped_refptr<AtomicGauge<int64_t>> streamwal_wal_lag_index;
  // Current value of --intents_min_seconds_to_retain. Exposed so dashboards can
  // derive intent-retention headroom as (window - sent_lag) without hardcoding
  // the flag (a true kMin "headroom" gauge is not expressible -- only kSum/kMax
  // aggregation functions exist).
  scoped_refptr<AtomicGauge<uint64_t>> streamwal_intent_retention_window_secs;
  // Count of INTENTS_GC_ERROR responses (intents GC'd before StreamWAL could
  // read them). Must always be zero; non-zero indicates data loss.
  scoped_refptr<Counter> streamwal_intents_gc_errors;

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
