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

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest_prod.h>

#include "yb/gutil/ref_counted.h"

#include "yb/rpc/rpc_fwd.h"
#include "yb/rpc/io_thread_pool.h"
#include "yb/rpc/local_call.h"
#include "yb/rpc/proxy_context.h"
#include "yb/rpc/scheduler.h"

#include "yb/util/async_util.h"
#include "yb/util/atomic.h"
#include "yb/util/lanes.h"
#include "yb/util/locks.h"
#include "yb/util/metrics_fwd.h"
#include "yb/util/monotime.h"
#include "yb/util/net/sockaddr.h"
#include "yb/util/one_time_bool.h"
#include "yb/util/operation_counter.h"
#include "yb/util/stack_trace.h"
#include "yb/util/status_fwd.h"

namespace yb {

class Cgroup;
class MemTracker;
class Socket;
struct SourceLocation;

namespace rpc {

template <class ContextType>
class ConnectionContextFactoryImpl;

typedef std::unordered_map<const Protocol*, StreamFactoryPtr> StreamFactories;

using ThreadPoolCgroupProvider = std::function<Cgroup*(TaggedThreadPools::Tag)>;

// Used to construct a Messenger.
class MessengerBuilder {
 public:
  friend class Messenger;

  explicit MessengerBuilder(std::string name);
  ~MessengerBuilder();

  MessengerBuilder(const MessengerBuilder&);

  // Set the length of time we will keep a TCP connection will alive with no traffic.
  MessengerBuilder &set_connection_keepalive_time(CoarseMonoClock::Duration keepalive);

  // Set the number of reactor threads that will be used for sending and receiving.
  MessengerBuilder &set_num_reactors(int num_reactors);

  // Lane of each reactor, by reactor index; empty (default) leaves every reactor unlaned. When
  // not empty its size must equal the number of reactors. Lanes are reduced modulo the lane
  // count of the process topology, and ignored while lane mode is off. Reactor lanes alone don't
  // make the messenger laned (see set_lane_topology); not used with a lane topology.
  MessengerBuilder &set_reactor_lanes(std::vector<LaneId> lanes);

  const std::vector<LaneId>& reactor_lanes() const {
    return reactor_lanes_;
  }

  // Makes the messenger laned when topology is active: it then builds reactors_per_lane reactors
  // for each lane (the reactor count set with set_num_reactors is ignored), a normal,
  // high-priority and tagged handler pool set per lane next to the unlaned ones, and keys
  // outbound connections of calls with a lane hint by (remote, lane). An active topology must be
  // the installed process topology (GetLaneTopology()), or Messenger::Init fails, because reactor
  // and pool threads take their masks from it. An inactive or null topology leaves the messenger
  // unlaned.
  MessengerBuilder &set_lane_topology(const LaneTopology* topology);

  // Reactors per lane of a laned messenger; 0 (default) means
  // max(1, CPUs in the lane / 4 / number of lanes sharing the lane's domains).
  MessengerBuilder &set_reactors_per_lane(int value);

  // Connections per (remote, lane) for calls with a lane hint in a laned messenger, in [1, 256].
  MessengerBuilder &set_connections_per_lane_remote(int value);

  const LaneTopology* lane_topology() const { return lane_topology_; }
  int reactors_per_lane() const { return reactors_per_lane_; }
  int connections_per_lane_remote() const { return connections_per_lane_remote_; }

  // Set the granularity with which connections are checked for keepalive.
  MessengerBuilder &set_coarse_timer_granularity(CoarseMonoClock::Duration granularity);

  // Set metric entity for use by RPC systems.
  MessengerBuilder &set_metric_entity(const scoped_refptr<MetricEntity>& metric_entity);

  // Uses the given connection type to handle the incoming connections.
  MessengerBuilder &UseConnectionContextFactory(const ConnectionContextFactoryPtr& factory) {
    connection_context_factory_ = factory;
    return *this;
  }

  MessengerBuilder &UseDefaultConnectionContextFactory(
      const std::shared_ptr<MemTracker>& parent_mem_tracker = nullptr);

  MessengerBuilder &AddStreamFactory(const Protocol* protocol, StreamFactoryPtr factory);

  MessengerBuilder &SetListenProtocol(const Protocol* protocol) {
    listen_protocol_ = protocol;
    return *this;
  }

  MessengerBuilder &SetUncompressedProtocol(const Protocol* protocol) {
    uncompressed_protocol_ = protocol;
    return *this;
  }

  MessengerBuilder& SetThreadPoolCgroupProvider(ThreadPoolCgroupProvider provider) {
    cgroup_provider_ = std::move(provider);
    return *this;
  }

  MessengerBuilder& SetSystemHighCgroup(Cgroup* cgroup) {
    system_high_cgroup_ = cgroup;
    return *this;
  }

  MessengerBuilder& SetSystemMedCgroup(Cgroup* cgroup) {
    system_med_cgroup_ = cgroup;
    return *this;
  }

  template <class ContextType>
  MessengerBuilder &CreateConnectionContextFactory(
      size_t memory_limit, const std::shared_ptr<MemTracker>& parent_mem_tracker = nullptr) {
    if (parent_mem_tracker) {
      last_used_parent_mem_tracker_ = parent_mem_tracker;
    }
    connection_context_factory_ =
        std::make_shared<ConnectionContextFactoryImpl<ContextType>>(
            memory_limit, parent_mem_tracker);
    return *this;
  }

  MessengerBuilder& UseLocalHostOutboundIpBaseInTests() {
    use_local_host_outbound_ip_base_in_tests_ = true;
    return *this;
  }

  Result<std::unique_ptr<Messenger>> Build();

  CoarseMonoClock::Duration connection_keepalive_time() const {
    return connection_keepalive_time_;
  }

  CoarseMonoClock::Duration coarse_timer_granularity() const {
    return coarse_timer_granularity_;
  }

  const ConnectionContextFactoryPtr& connection_context_factory() const {
    return connection_context_factory_;
  }

  MessengerBuilder& set_thread_pool_options(size_t workers_limit) {
    workers_limit_ = workers_limit;
    return *this;
  }

  MessengerBuilder& set_num_connections_to_server(int value) {
    num_connections_to_server_ = value;
    return *this;
  }

  int num_connections_to_server() const {
    return num_connections_to_server_;
  }

  const std::shared_ptr<MemTracker>& last_used_parent_mem_tracker() const {
    return last_used_parent_mem_tracker_;
  }

  Cgroup* system_high_cgroup() const {
    return system_high_cgroup_;
  }

  Cgroup* system_med_cgroup() const {
    return system_med_cgroup_;
  }

 private:
  const std::string name_;
  CoarseMonoClock::Duration connection_keepalive_time_;
  int num_reactors_ = 4;
  std::vector<LaneId> reactor_lanes_;
  const LaneTopology* lane_topology_ = nullptr;
  int reactors_per_lane_ = 0;
  int connections_per_lane_remote_ = 2;
  CoarseMonoClock::Duration coarse_timer_granularity_ = std::chrono::milliseconds(100);
  scoped_refptr<MetricEntity> metric_entity_;
  ConnectionContextFactoryPtr connection_context_factory_;
  StreamFactories stream_factories_;
  const Protocol* listen_protocol_;
  const Protocol* uncompressed_protocol_;
  size_t workers_limit_;
  int num_connections_to_server_;
  std::shared_ptr<MemTracker> last_used_parent_mem_tracker_;
  bool use_local_host_outbound_ip_base_in_tests_ = false;
  ThreadPoolCgroupProvider cgroup_provider_;
  Cgroup* system_high_cgroup_ = nullptr;
  Cgroup* system_med_cgroup_ = nullptr;
};

// A Messenger is a container for the reactor threads which run event loops for the RPC services.
// If the process is a server, a Messenger will also have an Acceptor.  In this case, calls received
// over the connection are enqueued into the messenger's service_queue for processing by a
// ServicePool.
//
// Users do not typically interact with the Messenger directly except to create one as a singleton,
// and then make calls using Proxy objects.
//
// See rpc-test.cc and rpc-bench.cc for example usages.
class Messenger : public ProxyContext {
 public:
  friend class MessengerBuilder;
  friend class Proxy;
  friend class Reactor;
  typedef std::unordered_map<std::string, scoped_refptr<RpcService>> RpcServicesMap;

  ~Messenger();

  // Stop all communication and prevent further use. Should be called explicitly by messenger owner.
  void Shutdown();

  // Setup messenger to listen connections on given address.
  Status ListenAddress(
      ConnectionContextFactoryPtr factory, const Endpoint& accept_endpoint,
      Endpoint* bound_endpoint = nullptr);

  // Stop accepting connections.
  void ShutdownAcceptor();

  // Start accepting connections.
  Status StartAcceptor();

  // Register a new RpcService to handle inbound requests.
  Status RegisterService(const std::string& service_name, const RpcServicePtr& service);

  void UnregisterAllServices();

  // Queue a call for transmission. This will pick the appropriate reactor, and enqueue a task on
  // that reactor to assign and send the call.
  void QueueOutboundCall(OutboundCallPtr call) override;

  // Invoke the RpcService to handle a call directly.
  void Handle(InboundCallPtr call, Queue queue);

  void HandleLocal(LocalYBInboundCallPtr call, Queue queue) override;

  const Protocol& DefaultProtocol() override { return listen_protocol_; }
  const Protocol& UncompressedProtocol() override { return uncompressed_protocol_; }

  rpc::ThreadPool& CallbackThreadPool(ServicePriority priority, LaneId lane) override {
    return ThreadPool(priority, lane);
  }

  Status QueueEventOnAllReactors(
      ServerEventListPtr server_event, const SourceLocation& source_location);

  Status QueueEventOnFilteredConnections(
      ServerEventListPtr server_event, const SourceLocation& source_location,
      ConnectionFilter connection_filter);

  // Dump the current RPCs into the given protobuf.
  Status DumpRunningRpcs(const DumpRunningRpcsRequestPB& req,
                         DumpRunningRpcsResponsePB* resp);

  void RemoveScheduledTask(ScheduledTaskId task_id);

  // This method will run 'func' with an ABORT status argument. It's not guaranteed that the task
  // will cancel because TimerHandler could run before this method.
  void AbortOnReactor(ScheduledTaskId task_id);

  // Run 'func' on a reactor thread after 'when' time elapses.
  //
  // The status argument conveys whether 'func' was run correctly (i.e. after the elapsed time) or
  // not.
  Result<ScheduledTaskId> ScheduleOnReactor(
      StatusFunctor func, MonoDelta when, const SourceLocation& source_location);

  std::string name() const {
    return name_;
  }

  scoped_refptr<MetricEntity> metric_entity() const override;

  RpcServicePtr TEST_rpc_service(const std::string& service_name) const;

  size_t max_concurrent_requests() const;

  const IpAddress& outbound_address_v4() const { return outbound_address_v4_; }
  const IpAddress& outbound_address_v6() const { return outbound_address_v6_; }

  void BreakConnectivityWith(const IpAddress& address);
  void BreakConnectivityTo(const IpAddress& address);
  void BreakConnectivityFrom(const IpAddress& address);
  void RestoreConnectivityWith(const IpAddress& address);
  void RestoreConnectivityTo(const IpAddress& address);
  void RestoreConnectivityFrom(const IpAddress& address);

  Scheduler& scheduler() {
    return scheduler_;
  }

  IoService& io_service() override {
    return io_thread_pool_.io_service();
  }

  DnsResolver& resolver() override {
    return *resolver_;
  }

  // Handler pools. kNoLane, and any lane in an unlaned messenger, give the messenger's unlaned
  // pools; another lane, reduced modulo num_lanes(), gives that lane's pool, whose workers run in
  // the lane. Each lane's pools have the limits and cgroups of the unlaned ones. High-priority
  // pools are created on first use; tagged pools on first use and reclaimed when idle the next
  // time the same lane creates a tagged pool.
  rpc::ThreadPool& ThreadPool(
      ServicePriority priority = ServicePriority::kNormal, LaneId lane = kNoLane);

  const rpc::ThreadPoolPtr& ThreadPoolPtr(
      ServicePriority priority = ServicePriority::kNormal, LaneId lane = kNoLane);

  // Tag 0 is the normal pool of the lane.
  Result<rpc::ThreadPoolPtr> TaggedThreadPool(
      TaggedThreadPools::Tag pool_tag = 0, LaneId lane = kNoLane);

  // Pool for an inbound call to a service of the given priority: the tagged pool of the call's
  // tag and lane for normal priority (the normal pool if that fails, e.g. during shutdown), the
  // high-priority pool of the lane for high priority.
  rpc::ThreadPoolPtr HandlerThreadPool(
      ServicePriority priority, ThreadPoolTag tag, LaneId lane);

  // Number of tagged pools of lane (unlaned for kNoLane), including the normal pool.
  size_t TEST_ActiveTaggedPools(LaneId lane = kNoLane);

  // The operation counters Handle takes per call: the one for threads without a lane first, then
  // each lane's in a laned messenger.
  std::vector<const RWOperationCounter*> TEST_RpcServicesCounters() const;
  // The one Handle takes on the calling thread.
  const RWOperationCounter* TEST_RpcServicesCounterOfThisThread() const;

  // Whether the messenger was built with an active lane topology.
  bool laned() const { return num_lanes_ != 0; }

  size_t num_lanes() const override { return num_lanes_; }

  // Topology of a laned messenger; the inactive topology otherwise.
  const LaneTopology& lane_topology() const { return lane_topology_; }

  int connections_per_lane_remote() const override { return connections_per_lane_remote_; }

  // Reactors of lane, reduced modulo num_lanes(); empty for kNoLane or an unlaned messenger.
  std::span<const std::unique_ptr<Reactor>> ReactorsOfLane(LaneId lane) const;

  const std::shared_ptr<RpcMetrics>& rpc_metrics() override {
    return rpc_metrics_;
  }

  const std::shared_ptr<MemTracker>& parent_mem_tracker() override;

  int num_connections_to_server() const override {
    return num_connections_to_server_;
  }

  size_t num_reactors() const {
    return reactors_.size();
  }

  Reactor* TEST_reactor(size_t index) const {
    return reactors_[index].get();
  }

  // Use specified IP address as base address for outbound connections from messenger.
  void TEST_SetOutboundIpBase(const IpAddress& value) {
    test_outbound_ip_base_ = value;
    has_outbound_ip_base_.store(true, std::memory_order_release);
  }

  bool TEST_ShouldArtificiallyRejectIncomingCallsFrom(const IpAddress &remote);

  Status TEST_GetReactorMetrics(size_t reactor_idx, ReactorMetrics* metrics);

  ScheduledTaskId TEST_next_task_id() const {
    return next_task_id_.load(std::memory_order_acquire);
  }

  void SetMetadataSerializerFactory(std::unique_ptr<MetadataSerializerFactory> factory) {
    metadata_serializer_factory_ = std::move(factory);
  }

  MetadataSerializerFactory* metadata_serializer_factory() override {
    return metadata_serializer_factory_.get();
  }

  void SetCallStateListenerFactory(std::unique_ptr<CallStateListenerFactory> factory) {
    call_state_listener_factory_ = std::move(factory);
  }

  CallStateListenerFactory* call_state_listener_factory() override {
    return call_state_listener_factory_.get();
  }

 private:
  friend class DelayedTask;

  explicit Messenger(const MessengerBuilder &bld);

  // Reactor for a connection: among the lane's reactors for a laned connection of a laned
  // messenger, among all reactors otherwise.
  Reactor* RemoteToReactor(const Endpoint& remote, uint32_t idx = 0, LaneId lane = kNoLane);
  Status Init(const MessengerBuilder &bld);
  Status InitLaned(const MessengerBuilder &bld);

  void ShutdownThreadPools();

  void BreakConnectivity(const IpAddress& address, bool incoming, bool outgoing);
  void RestoreConnectivity(const IpAddress& address, bool incoming, bool outgoing);

  // Take ownership of the socket via Socket::Release
  void RegisterInboundSocket(
      const ConnectionContextFactoryPtr& factory, Socket *new_socket, const Endpoint& remote);

  bool TEST_ShouldArtificiallyRejectOutgoingCallsTo(const IpAddress &remote);

  const std::string& LogPrefix() const {
    return log_prefix_;
  }

  ThreadPoolOptions MakeNormalThreadPoolOptions(TaggedThreadPools::Tag tag) const;
  ThreadPoolOptions MakeLaneThreadPoolOptions(
      LaneId lane, ServicePriority priority, TaggedThreadPools::Tag tag) const;

  // Handler pools of one lane; tagged pools are a separate instance per lane, so looking one up
  // takes only that lane's lock.
  struct LanePools {
    std::unique_ptr<TaggedThreadPools> tagged;
    rpc::ThreadPoolPtr normal;
    std::mutex high_mutex;
    rpc::ThreadPoolPtr high;
    std::atomic<bool> high_ready{false};
  };

  // The counter Handle takes for a call: the calling thread's lane's in a laned messenger, else
  // rpc_services_counter_. Shutdown disables and drains all of them (ForEachRpcServicesCounter).
  RWOperationCounter& RpcServicesCounter() const;
  template <class F>
  void ForEachRpcServicesCounter(const F& f) const;

  // Tracker of a local call of lane: the lane's in a laned messenger, else local_call_tracker_.
  LocalYBInboundCallTracker& LocalCallTracker(LaneId lane);

  LanePools* LanePoolsOf(LaneId lane) const {
    return lane == kNoLane || num_lanes_ == 0 ? nullptr : lane_pools_[lane % num_lanes_].get();
  }

  const std::string name_;

  const std::string log_prefix_;

  ConnectionContextFactoryPtr connection_context_factory_;

  const StreamFactories stream_factories_;

  const Protocol& listen_protocol_;
  const Protocol& uncompressed_protocol_;

  mutable PerCpuRwMutex lock_;

  std::atomic_bool closing_ = false;

  // Taken per call by Handle; with lanes, by threads without a lane only (see
  // RpcServicesCounter).
  mutable RWOperationCounter rpc_services_counter_;
  OneTimeBool rpc_services_stopped_;
  std::unordered_multimap<std::string, RpcServicePtr> rpc_services_;
  RpcEndpointMap rpc_endpoints_;

  std::vector<std::unique_ptr<Reactor>> reactors_;

  const scoped_refptr<MetricEntity> metric_entity_;
  const scoped_refptr<Histogram> outgoing_queue_time_;

  // Acceptor which is listening on behalf of this messenger.
  std::unique_ptr<Acceptor> acceptor_ GUARDED_BY(lock_);
  IpAddress outbound_address_v4_;
  IpAddress outbound_address_v6_;

  // Id that will be assigned to the next task that is scheduled on the reactor.
  std::atomic<ScheduledTaskId> next_task_id_ = {1};
  std::atomic<uint64_t> num_connections_accepted_ = {0};

  std::mutex mutex_scheduled_tasks_;

  std::unordered_map<ScheduledTaskId, std::shared_ptr<DelayedTask>> scheduled_tasks_
      GUARDED_BY(mutex_scheduled_tasks_);

  // Flag that we have at least on address with artificially broken connectivity.
  std::atomic<bool> has_broken_connectivity_ = {false};
  mutable PerCpuRwMutex broken_connectivity_lock_;

  // Set of addresses with artificially broken connectivity.
  std::unordered_set<IpAddress, IpAddressHash> broken_connectivity_from_
      GUARDED_BY(broken_connectivity_lock_);
  std::unordered_set<IpAddress, IpAddressHash> broken_connectivity_to_
      GUARDED_BY(broken_connectivity_lock_);

  IoThreadPool io_thread_pool_;
  Scheduler scheduler_;
  // Tracks local calls; with lanes, those without a lane only (see LocalCallTracker).
  LocalYBInboundCallTracker local_call_tracker_;

  // Per-call state of a laned messenger, one per lane, so that calls of different lanes write
  // different cache lines: the operation counter Handle takes for each call from a thread of the
  // lane, and the tracker of the lane's local calls.
  struct alignas(CACHELINE_SIZE) LaneCallState {
    LaneCallState(const std::string& name, Scheduler& scheduler)
        : rpc_services_counter(name), local_call_tracker(scheduler) {}

    RWOperationCounter rpc_services_counter;
    LocalYBInboundCallTracker local_call_tracker;
  };
  std::vector<std::unique_ptr<LaneCallState>> lane_call_states_;

  size_t thread_pool_workers_limit_;

  // Thread pools that are used by services running in this messenger.
  std::unique_ptr<TaggedThreadPools> normal_thread_pools_;

  rpc::ThreadPoolPtr default_normal_thread_pool_;

  std::mutex mutex_high_priority_thread_pool_;

  // This could be used for high-priority services such as Consensus.
  rpc::ThreadPoolPtr high_priority_thread_pool_;
  std::atomic<bool> high_priority_thread_pool_ready_;

  std::unique_ptr<DnsResolver> resolver_;

  std::shared_ptr<RpcMetrics> rpc_metrics_;

  // Use this IP address as base address for outbound connections from messenger.
  IpAddress test_outbound_ip_base_;
  std::atomic<bool> has_outbound_ip_base_{false};

  // Number of outbound connections to create per each destination server address.
  int num_connections_to_server_;

  // Lane state; num_lanes_ is 0 for an unlaned messenger.
  size_t num_lanes_ = 0;
  LaneTopology lane_topology_;
  int connections_per_lane_remote_ = 0;
  // Reactors of lane l are reactors_[lane_reactor_begin_[l], lane_reactor_begin_[l + 1]).
  std::vector<size_t> lane_reactor_begin_;
  std::vector<std::unique_ptr<LanePools>> lane_pools_;

  std::unique_ptr<ReactorMonitor> reactor_monitor_ GUARDED_BY(lock_);

  std::unique_ptr<CallStateListenerFactory> call_state_listener_factory_;
  std::unique_ptr<MetadataSerializerFactory> metadata_serializer_factory_;

  ThreadPoolCgroupProvider cgroup_provider_;
  Cgroup* system_high_cgroup_ = nullptr;
  Cgroup* system_med_cgroup_ = nullptr;

#ifndef NDEBUG
  // This is so we can log where exactly a Messenger was instantiated to better diagnose a CHECK
  // failure in the destructor (ENG-2838). This can be removed when that is fixed.
  StackTrace creation_stack_trace_;
#endif
  DISALLOW_COPY_AND_ASSIGN(Messenger);
};

}  // namespace rpc
}  // namespace yb
