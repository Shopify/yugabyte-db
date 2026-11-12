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

#include <stdint.h>

#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "yb/gutil/integral_types.h"
#include "yb/gutil/macros.h"
#include "yb/gutil/ref_counted.h"

#include "yb/rpc/rpc_fwd.h"
#include "yb/rpc/rpc_service.h"

#include "yb/util/lanes.h"
#include "yb/util/status_fwd.h"
#include "yb/util/blocking_queue.h"
#include "yb/util/mutex.h"

namespace yb {

template<class T>
class AtomicGauge;

class Counter;
class EventStats;
class MetricEntity;
class Socket;

namespace rpc {

// Returns the pool that handles a call with the given tag and lane (InboundCall::lane(): the
// receiving reactor's lane, or the caller's lane for a local call; kNoLane for none).
using ThreadPoolProvider = std::function<ThreadPoolPtr(ThreadPoolTag, LaneId)>;

// A pool of threads that handle new incoming RPC calls.
// Also includes a queue that calls get pushed onto for handling by the pool.
class ServicePool : public RpcService {
 public:
  // num_lanes: the lane count of a laned messenger, 0 for an unlaned one. A laned pool keeps its
  // queued-call count and timeout pre-check queue per lane (plus one for calls without a lane),
  // and admits a call without a shared write while its lane is below its share of max_tasks
  // (calls without a lane always sum the lanes); see ServicePoolImpl::LaneCallQueued. An unlaned
  // pool keeps one count and one queue.
  ServicePool(size_t max_tasks,
              ThreadPoolProvider thread_pool_provider,
              Scheduler* scheduler,
              ServiceIfPtr service,
              const scoped_refptr<MetricEntity>& metric_entity,
              size_t num_lanes = 0);
  virtual ~ServicePool();

  void StartShutdown() override;
  void CompleteShutdown() override;

  void FillEndpoints(RpcEndpointMap* map) override;
  void Process(InboundCallPtr call, Queue queue) override;
  const Counter* RpcsTimedOutInQueueMetricForTests() const;
  const Counter* RpcsQueueOverflowMetric() const;
  std::string service_name() const;

  ServiceIfPtr TEST_get_service() const;

  // Calls counted as queued (for a laned pool, the sum over its lanes).
  int64_t QueuedCalls() const;

  // Per-lane counts of a laned pool, the spare slot last; empty for an unlaned pool.
  std::vector<int64_t> TEST_LaneQueuedCalls() const;
  // The count an unlaned pool uses; stays 0 in a laned pool.
  int64_t TEST_SharedQueuedCalls() const;
  int64_t TEST_LanesOverShare() const;
  // Admission and release as for a call of lane, without a call.
  std::optional<int64_t> TEST_CallQueued(LaneId lane);
  void TEST_CallDequeued(LaneId lane);

 private:
  std::unique_ptr<ServicePoolImpl> impl_;
};

} // namespace rpc
} // namespace yb
