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

#include "yb/rpc/messenger.h"

#include <sys/types.h>

#include <list>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>

#include "yb/util/logging.h"

#include "yb/gutil/casts.h"
#include "yb/gutil/map-util.h"
#include "yb/gutil/stl_util.h"

#include "yb/rpc/acceptor.h"
#include "yb/rpc/constants.h"
#include "yb/rpc/delayed_task.h"
#include "yb/rpc/reactor.h"
#include "yb/rpc/reactor_monitor.h"
#include "yb/rpc/reactor_thread_role.h"
#include "yb/rpc/rpc_header.pb.h"
#include "yb/rpc/rpc_metrics.h"
#include "yb/rpc/rpc_service.h"
#include "yb/rpc/rpc_util.h"
#include "yb/rpc/tcp_stream.h"
#include "yb/rpc/yb_rpc.h"

#include "yb/util/debug-util.h"
#include "yb/util/errno.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/metrics.h"
#include "yb/util/monotime.h"
#include "yb/util/net/dns_resolver.h"
#include "yb/util/net/socket.h"
#include "yb/util/result.h"
#include "yb/util/size_literals.h"
#include "yb/util/status.h"
#include "yb/util/status_format.h"
#include "yb/util/status_log.h"
#include "yb/util/thread_restrictions.h"
#include "yb/util/trace.h"

using namespace std::literals;
using namespace std::placeholders;
using namespace yb::size_literals;

using std::shared_ptr;
using std::string;

DECLARE_bool(TEST_running_test);

DECLARE_int32(num_connections_to_server);
DEFINE_UNKNOWN_int32(rpc_default_keepalive_time_ms, 65000,
             "If an RPC connection from a client is idle for this amount of time, the server "
             "will disconnect the client. Setting flag to 0 disables this clean up.");
TAG_FLAG(rpc_default_keepalive_time_ms, advanced);
DEFINE_UNKNOWN_uint64(io_thread_pool_size, 4, "Size of allocated IO Thread Pool.");

DEFINE_UNKNOWN_int64(outbound_rpc_memory_limit, 0, "Outbound RPC memory limit");

DEPRECATE_FLAG(int32, rpc_queue_limit, "03_2024");

DEFINE_NON_RUNTIME_int32(rpc_workers_limit, 1024, "Workers limit for rpc server");

DEFINE_NON_RUNTIME_int32(rpc_high_priority_workers_limit, 0,
    "Workers limit for the high priority rpc thread pool, which serves consensus. "
    "0 means use rpc_workers_limit.");

DEFINE_NON_RUNTIME_int32(rpc_reactor_task_timeout_ms, 0,
    "Report if reactor task task takes longer that specified amount of milliseconds. "
    "0 to disable monitoring.");

DEFINE_UNKNOWN_int32(socket_receive_buffer_size, 0, "Socket receive buffer size, 0 to use default");

DEFINE_test_flag(int32, rpc_reactor_index_for_init_failure_simulation,
    -1, "Index of reactor in Messenger to simulate failure of Reactor::Init method");

namespace yb {
namespace rpc {

class Messenger;
class ServerBuilder;

// ------------------------------------------------------------------------------------------------
// MessengerBuilder
// ------------------------------------------------------------------------------------------------

MessengerBuilder::MessengerBuilder(std::string name)
    : name_(std::move(name)),
      connection_keepalive_time_(FLAGS_rpc_default_keepalive_time_ms * 1ms),
      coarse_timer_granularity_(100ms),
      listen_protocol_(TcpStream::StaticProtocol()),
      uncompressed_protocol_(TcpStream::StaticProtocol()),
      workers_limit_(FLAGS_rpc_workers_limit),
      num_connections_to_server_(FLAGS_num_connections_to_server) {
  AddStreamFactory(TcpStream::StaticProtocol(), TcpStream::Factory());
}

MessengerBuilder::~MessengerBuilder() = default;
MessengerBuilder::MessengerBuilder(const MessengerBuilder&) = default;

MessengerBuilder& MessengerBuilder::set_connection_keepalive_time(
    CoarseMonoClock::Duration keepalive) {
  connection_keepalive_time_ = keepalive;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_num_reactors(int num_reactors) {
  num_reactors_ = num_reactors;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_reactor_lanes(std::vector<LaneId> lanes) {
  reactor_lanes_ = std::move(lanes);
  return *this;
}

MessengerBuilder& MessengerBuilder::set_lane_topology(const LaneTopology* topology) {
  lane_topology_ = topology;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_reactors_per_lane(int value) {
  reactors_per_lane_ = value;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_connections_per_lane_remote(int value) {
  connections_per_lane_remote_ = value;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_coarse_timer_granularity(
    CoarseMonoClock::Duration granularity) {
  coarse_timer_granularity_ = granularity;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_metric_entity(
    const scoped_refptr<MetricEntity>& metric_entity) {
  metric_entity_ = metric_entity;
  return *this;
}

Result<std::unique_ptr<Messenger>> MessengerBuilder::Build() {
  if (!connection_context_factory_) {
    UseDefaultConnectionContextFactory();
  }
  std::unique_ptr<Messenger, MessengerShutdownDeleter> messenger(new Messenger(*this));
  RETURN_NOT_OK(messenger->Init(*this));

  if (PREDICT_FALSE(use_local_host_outbound_ip_base_in_tests_ && FLAGS_TEST_running_test)) {
    messenger->TEST_SetOutboundIpBase(VERIFY_RESULT(HostToAddress("127.0.0.1")));
  }

  return std::unique_ptr<Messenger>(messenger.release());
}

MessengerBuilder &MessengerBuilder::AddStreamFactory(
    const Protocol* protocol, StreamFactoryPtr factory) {
  auto p = stream_factories_.emplace(protocol, std::move(factory));
  LOG_IF(DFATAL, !p.second) << "Duplicate stream factory: " << protocol->ToString();
  return *this;
}

MessengerBuilder &MessengerBuilder::UseDefaultConnectionContextFactory(
    const std::shared_ptr<MemTracker>& parent_mem_tracker) {
  if (parent_mem_tracker) {
    last_used_parent_mem_tracker_ = parent_mem_tracker;
  }
  connection_context_factory_ = rpc::CreateConnectionContextFactory<YBOutboundConnectionContext>(
      FLAGS_outbound_rpc_memory_limit, parent_mem_tracker);
  return *this;
}

// ------------------------------------------------------------------------------------------------
// Messenger
// ------------------------------------------------------------------------------------------------

void Messenger::Shutdown() {
  bool expected = false;
  if (!closing_.compare_exchange_strong(expected, true)) {
    return;
  }
  VLOG_WITH_PREFIX(1) << "shutting down messenger";

  ShutdownThreadPools();
  ShutdownAcceptor();
  UnregisterAllServices();

  // Since we're shutting down, it's OK to block.
  ThreadRestrictions::ScopedAllowWait allow_wait;

  std::vector<Reactor*> reactors;
  std::unique_ptr<Acceptor> acceptor;
  std::unique_ptr<ReactorMonitor> reactor_monitor;
  {
    std::lock_guard guard(lock_);

    acceptor.swap(acceptor_);

    reactor_monitor.swap(reactor_monitor_);

    for (const auto& reactor : reactors_) {
      reactors.push_back(reactor.get());
    }
  }

  if (acceptor) {
    acceptor->Shutdown();
  }

  if (reactor_monitor) {
    reactor_monitor->Shutdown();
    reactor_monitor.reset();
  }

  for (auto* reactor : reactors) {
    reactor->StartShutdown();
  }

  scheduler_.Shutdown();
  io_thread_pool_.Shutdown();

  for (auto* reactor : reactors) {
    reactor->Join();
  }

  io_thread_pool_.Join();

  {
    std::lock_guard guard(mutex_scheduled_tasks_);
    LOG_IF_WITH_PREFIX(DFATAL, !scheduled_tasks_.empty())
        << "Scheduled tasks is not empty after messenger shutdown: "
        << AsString(scheduled_tasks_);
  }

  // Safe to clear only after reactors have been shutdown as there may be CleanupHooks which access
  // data owned by the services.
  rpc_services_.clear();
}

Status Messenger::ListenAddress(
    ConnectionContextFactoryPtr factory, const Endpoint& accept_endpoint,
    Endpoint* bound_endpoint) {
  Acceptor* acceptor;
  {
    std::lock_guard guard(lock_);
    if (!acceptor_) {
      acceptor_.reset(new Acceptor(
          metric_entity_, std::bind(&Messenger::RegisterInboundSocket, this, factory, _1, _2),
          system_high_cgroup_));
    }
    auto accept_host = accept_endpoint.address();
    auto& outbound_address = accept_host.is_v6() ? outbound_address_v6_
                                                 : outbound_address_v4_;
    if (outbound_address.is_unspecified() && !accept_host.is_unspecified()) {
      outbound_address = accept_host;
    }
    acceptor = acceptor_.get();
  }
  return acceptor->Listen(accept_endpoint, bound_endpoint);
}

Status Messenger::StartAcceptor() {
  for (const auto& p : rpc_services_) {
    p.second->FillEndpoints(&rpc_endpoints_);
  }

  std::lock_guard guard(lock_);
  if (acceptor_) {
    return acceptor_->Start();
  }
  return STATUS(IllegalState, "Trying to start acceptor w/o active addresses");
}

void Messenger::BreakConnectivityWith(const IpAddress& address) {
  BreakConnectivity(address, /* incoming */ true, /* outgoing */ true);
}
void Messenger::BreakConnectivityTo(const IpAddress& address) {
  BreakConnectivity(address, /* incoming */ false, /* outgoing */ true);
}

void Messenger::BreakConnectivityFrom(const IpAddress& address) {
  BreakConnectivity(address, /* incoming */ true, /* outgoing */ false);
}

void Messenger::BreakConnectivity(const IpAddress& address, bool incoming, bool outgoing) {
  LOG_WITH_PREFIX(INFO) << "TEST: Break " << (incoming ? "incoming" : "") << "/"
                        << (outgoing ? "outgoing" : "") << " connectivity with: " << address;

  bool inserted_from = false;
  bool inserted_to = false;
  std::optional<CountDownLatch> latch;
  {
    std::lock_guard guard(broken_connectivity_lock_);
    if (broken_connectivity_from_.empty() || broken_connectivity_to_.empty()) {
      has_broken_connectivity_.store(true, std::memory_order_release);
    }
    if (incoming) {
      inserted_from = broken_connectivity_from_.insert(address).second;
    }
    if (outgoing) {
      inserted_to = broken_connectivity_to_.insert(address).second;
    }
  }
  if (inserted_from || inserted_to) {
    latch.emplace(reactors_.size());
    for (const auto& reactor : reactors_) {
      auto scheduling_status = reactor->ScheduleReactorTask(MakeFunctorReactorTask(
          [&latch, address, incoming, outgoing](Reactor* reactor) {
            ReactorThreadRoleGuard guard;
            if (incoming) {
              reactor->DropIncomingWithRemoteAddress(address);
            }
            if (outgoing) {
              reactor->DropOutgoingWithRemoteAddress(address);
            }
            latch->CountDown();
          },
          SOURCE_LOCATION()));
      if (!scheduling_status.ok()) {
        LOG_WITH_PREFIX(DFATAL) << "Failed to schedule drop connection with: "
                                << address.to_string() << ": " << scheduling_status;
        latch->CountDown();
      }
    }
  }

  if (latch) {
    latch->Wait();
  }
}

void Messenger::RestoreConnectivityWith(const IpAddress& address) {
  RestoreConnectivity(address, /* incoming */ true, /* outgoing */ true);
}
void Messenger::RestoreConnectivityTo(const IpAddress& address) {
  RestoreConnectivity(address, /* incoming */ false, /* outgoing */ true);
}

void Messenger::RestoreConnectivityFrom(const IpAddress& address) {
  RestoreConnectivity(address, /* incoming */ true, /* outgoing */ false);
}

void Messenger::RestoreConnectivity(const IpAddress& address, bool incoming, bool outgoing) {
  LOG_WITH_PREFIX(INFO) << "TEST: Restore " << (incoming ? "incoming" : "") << "/"
                        << (outgoing ? "outgoing" : "") << " connectivity with: " << address;

  std::lock_guard guard(broken_connectivity_lock_);
  if (incoming) {
    broken_connectivity_from_.erase(address);
  }
  if (outgoing) {
    broken_connectivity_to_.erase(address);
  }
  if (broken_connectivity_from_.empty() && broken_connectivity_to_.empty()) {
    has_broken_connectivity_.store(false, std::memory_order_release);
  }
}

bool Messenger::TEST_ShouldArtificiallyRejectIncomingCallsFrom(const IpAddress& remote) {
  if (has_broken_connectivity_.load(std::memory_order_acquire)) {
    PerCpuRwSharedLock guard(broken_connectivity_lock_);
    const auto do_reject = broken_connectivity_from_.count(remote) != 0;
    VLOG_IF_WITH_PREFIX(1, do_reject) << "TEST: Rejected incoming call from " << AsString(remote);
    return do_reject;
  }
  return false;
}

bool Messenger::TEST_ShouldArtificiallyRejectOutgoingCallsTo(const IpAddress &remote) {
  if (has_broken_connectivity_.load(std::memory_order_acquire)) {
    PerCpuRwSharedLock guard(broken_connectivity_lock_);
    const auto do_reject = broken_connectivity_to_.count(remote) != 0;
    VLOG_IF_WITH_PREFIX(1, do_reject) << "TEST: Rejected outgoing call to " << AsString(remote);
    return do_reject;
  }
  return false;
}

Status Messenger::TEST_GetReactorMetrics(size_t reactor_idx, ReactorMetrics* metrics) {
  if (reactor_idx >= reactors_.size()) {
    return STATUS_FORMAT(
        InvalidArgument, "Invalid reactor index $0, should be >=0 and <$1", reactor_idx,
        reactors_.size());
  }
  return reactors_[reactor_idx]->GetMetrics(metrics);
}

void Messenger::ShutdownAcceptor() {
  std::unique_ptr<Acceptor> acceptor;
  {
    std::lock_guard guard(lock_);
    acceptor.swap(acceptor_);
  }
  if (acceptor) {
    acceptor->Shutdown();
  }
}

rpc::ThreadPool& Messenger::ThreadPool(ServicePriority priority, LaneId lane) {
  return *ThreadPoolPtr(priority, lane);
}

const ThreadPoolPtr& Messenger::ThreadPoolPtr(ServicePriority priority, LaneId lane) {
  if (auto* lane_pools = LanePoolsOf(lane)) {
    switch (priority) {
      case ServicePriority::kNormal:
        return lane_pools->normal;
      case ServicePriority::kHigh:
        if (lane_pools->high_ready.load(std::memory_order_acquire)) {
          return lane_pools->high;
        }
        std::lock_guard lock(lane_pools->high_mutex);
        if (lane_pools->high_ready.load(std::memory_order_acquire)) {
          return lane_pools->high;
        }
        lane_pools->high = std::make_shared<rpc::ThreadPool>(MakeLaneThreadPoolOptions(
            narrow_cast<LaneId>(lane % num_lanes_), ServicePriority::kHigh, /* tag= */ 0));
        lane_pools->high_ready.store(true, std::memory_order_release);
        return lane_pools->high;
    }
    FATAL_INVALID_ENUM_VALUE(ServicePriority, priority);
  }
  switch (priority) {
    case ServicePriority::kNormal:
      return default_normal_thread_pool_;
    case ServicePriority::kHigh:
      if (high_priority_thread_pool_ready_.load(std::memory_order_acquire)) {
        return high_priority_thread_pool_;
      }
      std::lock_guard lock(mutex_high_priority_thread_pool_);
      if (high_priority_thread_pool_ready_.load(std::memory_order_acquire)) {
        return high_priority_thread_pool_;
      }
      high_priority_thread_pool_ = std::make_shared<rpc::ThreadPool>(
          rpc::ThreadPoolOptions {
            .name = name_ + "-high-pri",
            .max_workers = FLAGS_rpc_high_priority_workers_limit > 0
                ? static_cast<size_t>(FLAGS_rpc_high_priority_workers_limit)
                : thread_pool_workers_limit_,
            .cgroup = system_high_cgroup_,
          });
      high_priority_thread_pool_ready_.store(true, std::memory_order_release);
      return high_priority_thread_pool_;
  }
  FATAL_INVALID_ENUM_VALUE(ServicePriority, priority);
}

Result<ThreadPoolPtr> Messenger::TaggedThreadPool(TaggedThreadPools::Tag tag, LaneId lane) {
  if (auto* lane_pools = LanePoolsOf(lane)) {
    // The lane holds its normal pool, so the common case takes no lock.
    if (tag == 0) {
      return lane_pools->normal;
    }
    return lane_pools->tagged->Pool(tag);
  }
  return normal_thread_pools_->Pool(tag);
}

ThreadPoolPtr Messenger::HandlerThreadPool(
    ServicePriority priority, ThreadPoolTag tag, LaneId lane) {
  if (priority == ServicePriority::kNormal) {
    auto result = TaggedThreadPool(tag, lane);
    if (result.ok()) {
      return *result;
    }
    // Could be shutting down.
    LOG_WITH_PREFIX(WARNING) << "Failed to get thread pool with tag " << tag << ": "
                             << result.status();
  }
  return ThreadPoolPtr(priority, lane);
}

size_t Messenger::TEST_ActiveTaggedPools(LaneId lane) {
  auto* lane_pools = LanePoolsOf(lane);
  return lane_pools ? lane_pools->tagged->ActivePools() : normal_thread_pools_->ActivePools();
}

std::span<const std::unique_ptr<Reactor>> Messenger::ReactorsOfLane(LaneId lane) const {
  if (lane == kNoLane || num_lanes_ == 0) {
    return {};
  }
  lane %= num_lanes_;
  std::span<const std::unique_ptr<Reactor>> all(reactors_);
  return all.subspan(
      lane_reactor_begin_[lane], lane_reactor_begin_[lane + 1] - lane_reactor_begin_[lane]);
}

// Register a new RpcService to handle inbound requests.
Status Messenger::RegisterService(
    const std::string& service_name, const scoped_refptr<RpcService>& service) {
  DCHECK(service);
  rpc_services_.emplace(service_name, service);
  return Status::OK();
}

void Messenger::ShutdownThreadPools() {
  normal_thread_pools_->Shutdown();
  {
    std::lock_guard lock(mutex_high_priority_thread_pool_);
    if (high_priority_thread_pool_) {
      high_priority_thread_pool_->Shutdown();
    }
  }
  for (auto& lane_pools : lane_pools_) {
    lane_pools->tagged->Shutdown();
    std::lock_guard lock(lane_pools->high_mutex);
    if (lane_pools->high) {
      lane_pools->high->Shutdown();
    }
  }
}

void Messenger::UnregisterAllServices() {
  if (!rpc_services_stopped_.Set()) {
    return;
  }
  CHECK_OK(rpc_services_counter_.DisableAndWaitForOps(CoarseTimePoint::max(), Stop::kTrue));
  rpc_services_counter_.UnlockExclusiveOpMutex();

  for (const auto& [_, rpc_service]  : rpc_services_) {
    rpc_service->StartShutdown();
  }
  for (const auto& [_, rpc_service] : rpc_services_) {
    rpc_service->CompleteShutdown();
  }

  rpc_endpoints_.clear();
}

class NotifyDisconnectedReactorTask : public ReactorTask {
 public:
  NotifyDisconnectedReactorTask(OutboundCallPtr call, const SourceLocation& source_location)
      : ReactorTask(source_location), call_(std::move(call)) {}

  void Run(Reactor* reactor) override  {
    call_->Transferred(STATUS_FORMAT(
        NetworkError, "TEST: Connectivity is broken with $0",
        call_->conn_id().remote().address()), /* conn= */ nullptr);
  }
 private:
  void DoAbort(const Status &abort_status) override {
    call_->Transferred(abort_status, /* conn= */ nullptr);
  }

  OutboundCallPtr call_;
};

void Messenger::QueueOutboundCall(OutboundCallPtr call) {
  const auto& remote = call->conn_id().remote();
  Reactor *reactor = RemoteToReactor(remote, call->conn_id().idx(), call->conn_id().lane());

  if (TEST_ShouldArtificiallyRejectOutgoingCallsTo(remote.address())) {
    VLOG_WITH_PREFIX(1) << "TEST: Rejected connection to " << remote;
    auto scheduling_status =
        reactor->ScheduleReactorTask(std::make_shared<NotifyDisconnectedReactorTask>(
            call, SOURCE_LOCATION()));
    if (!scheduling_status.ok()) {
      call->Transferred(scheduling_status, nullptr /* conn */);
    }
    return;
  }

  reactor->QueueOutboundCall(std::move(call));
}

void Messenger::HandleLocal(LocalYBInboundCallPtr call, Queue queue) {
  local_call_tracker_.Enqueue(call);
  Handle(std::move(call), queue);
}

void Messenger::Handle(InboundCallPtr call, Queue queue) {
  ScopedRWOperation op(&rpc_services_counter_, CoarseTimePoint::min());
  if (!op.ok()) {
    call->RespondFailure(ErrorStatusPB::FATAL_SERVER_SHUTTING_DOWN, MoveStatus(op));
    return;
  }

  auto it = rpc_endpoints_.find(call->serialized_remote_method());
  if (it == rpc_endpoints_.end()) {
    auto remote_method = ParseRemoteMethod(call->serialized_remote_method());
    Status s;
    ErrorStatusPB::RpcErrorCodePB error_code = ErrorStatusPB::ERROR_NO_SUCH_SERVICE;
    if (remote_method.ok()) {
      if (rpc_services_.count(remote_method->service.ToBuffer())) {
        error_code = ErrorStatusPB::ERROR_NO_SUCH_METHOD;
        s = STATUS_FORMAT(
            InvalidArgument, "Call on service $0 received from $1 with an invalid method name: $2",
            remote_method->service.ToBuffer(),
            call->remote_address(),
            remote_method->method.ToBuffer());
      } else {
        s = STATUS_FORMAT(
            ServiceUnavailable, "Service $0 not registered on $1",
            remote_method->service.ToBuffer(), name_);
      }
    } else {
      s = remote_method.status();
    }
    LOG_WITH_PREFIX(WARNING) << s;
    call->RespondFailure(error_code, s);
    return;
  }

  // The RpcService will respond to the client on success or failure.
  call->set_method_index(it->second.second);
  it->second.first->Process(std::move(call), queue);
}

RpcServicePtr Messenger::TEST_rpc_service(const std::string& service_name) const {
  ScopedRWOperation op(&rpc_services_counter_, CoarseTimePoint::min());
  if (!op.ok()) {
    return nullptr;
  }
  auto it = rpc_services_.find(service_name);
  return it != rpc_services_.end() ? it->second : nullptr;
}

const std::shared_ptr<MemTracker>& Messenger::parent_mem_tracker() {
  return connection_context_factory_->buffer_tracker();
}

void Messenger::RegisterInboundSocket(
    const ConnectionContextFactoryPtr& factory, Socket *new_socket, const Endpoint& remote) {
  if (TEST_ShouldArtificiallyRejectIncomingCallsFrom(remote.address())) {
    auto status = new_socket->Close();
    VLOG_WITH_PREFIX(1) << "TEST: Rejected connection from " << remote
                        << ", close status: " << status.ToString();
    return;
  }

  if (FLAGS_socket_receive_buffer_size) {
    WARN_NOT_OK(new_socket->SetReceiveBufferSize(FLAGS_socket_receive_buffer_size),
                "Set receive buffer size failed: ");
  }

  auto receive_buffer_size = new_socket->GetReceiveBufferSize();
  if (!receive_buffer_size.ok()) {
    LOG_WITH_PREFIX(WARNING) << "Register inbound socket failed: " << receive_buffer_size.status();
    return;
  }

  int idx = num_connections_accepted_.fetch_add(1) % num_connections_to_server_;
  Reactor *reactor = RemoteToReactor(remote, idx);
  reactor->RegisterInboundSocket(new_socket, *receive_buffer_size, remote, factory);
}

Messenger::Messenger(const MessengerBuilder &bld)
    : name_(bld.name_),
      log_prefix_(Format("Messenger($0, \"$1\"): ", static_cast<void*>(this), name_)),
      connection_context_factory_(bld.connection_context_factory_),
      stream_factories_(bld.stream_factories_),
      listen_protocol_(*bld.listen_protocol_),
      uncompressed_protocol_(*bld.uncompressed_protocol_),
      rpc_services_counter_(name_ + " endpoints"),
      metric_entity_(bld.metric_entity_),
      io_thread_pool_(name_, FLAGS_io_thread_pool_size, bld.system_high_cgroup_),
      scheduler_(&io_thread_pool_.io_service()),
      local_call_tracker_(scheduler_),
      thread_pool_workers_limit_(bld.workers_limit_),
      normal_thread_pools_(new TaggedThreadPools(
          [this](TaggedThreadPools::Tag tag) { return MakeNormalThreadPoolOptions(tag); })),
      resolver_(new DnsResolver(&io_thread_pool_.io_service(), metric_entity_)),
      rpc_metrics_(std::make_shared<RpcMetrics>(bld.metric_entity_)),
      num_connections_to_server_(bld.num_connections_to_server_),
      cgroup_provider_(bld.cgroup_provider_),
      system_high_cgroup_(bld.system_high_cgroup_),
      system_med_cgroup_(bld.system_med_cgroup_) {
#ifndef NDEBUG
  creation_stack_trace_.Collect(/* skip_frames */ 1);
#endif
  VLOG_WITH_PREFIX(1) << "Messenger constructor called at:\n" << GetStackTrace();
  // Make sure skip buffer is allocated before we hit memory limit and try to use it.
  GetGlobalSkipBuffer();
}

Messenger::~Messenger() {
  std::lock_guard guard(lock_);
  // This logging and the corresponding logging in the constructor is here to track down the
  // occasional CHECK(closing_) failure below in some tests (ENG-2838).
  VLOG_WITH_PREFIX(1) << "Messenger destructor called at:\n" << GetStackTrace();
#ifndef NDEBUG
  if (!closing_) {
    LOG_WITH_PREFIX(DFATAL) << "Messenger created here:\n"
                            << creation_stack_trace_.Symbolize()
                            << "Messenger destructor called at:\n"
                            << GetStackTrace();
  }
#endif
  CHECK(closing_) << "Should have already shut down";
}

size_t Messenger::max_concurrent_requests() const {
  return num_connections_to_server_;
}

Reactor* Messenger::RemoteToReactor(const Endpoint& remote, uint32_t idx, LaneId lane) {
  auto hash_code = hash_value(remote);
  if (lane != kNoLane && num_lanes_ != 0) {
    // Lane groups are never empty, and may differ in size.
    auto group = ReactorsOfLane(lane);
    return group[(hash_code + idx) % group.size()].get();
  }
  auto reactor_idx = (hash_code + idx) % reactors_.size();
  // This is just a static partitioning; where each connection
  // to a remote is assigned to a particular reactor. We could
  // get a lot fancier with assigning Sockaddrs to Reactors,
  // but this should be good enough.
  return reactors_[reactor_idx].get();
}

Status Messenger::InitLaned(const MessengerBuilder &bld) {
  if (*bld.lane_topology_ != GetLaneTopology()) {
    return STATUS_FORMAT(
        InvalidArgument, "Lane topology $0 is not the process lane topology $1",
        *bld.lane_topology_, GetLaneTopology());
  }
  if (!bld.reactor_lanes_.empty()) {
    return STATUS(InvalidArgument, "Reactor lanes can't be set with a lane topology");
  }
  if (bld.reactors_per_lane_ < 0) {
    return STATUS_FORMAT(InvalidArgument, "Invalid reactors per lane: $0", bld.reactors_per_lane_);
  }
  if (bld.connections_per_lane_remote_ < 1 || bld.connections_per_lane_remote_ > 256) {
    return STATUS_FORMAT(
        InvalidArgument, "Connections per lane and remote must be in [1, 256]: $0",
        bld.connections_per_lane_remote_);
  }
  lane_topology_ = *bld.lane_topology_;
  connections_per_lane_remote_ = bld.connections_per_lane_remote_;
  const auto num_lanes = lane_topology_.num_lanes();
  lane_reactor_begin_.reserve(num_lanes + 1);
  lane_reactor_begin_.push_back(0);
  std::string per_lane;
  for (size_t lane = 0; lane != num_lanes; ++lane) {
    size_t reactors = bld.reactors_per_lane_;
    if (reactors == 0) {
      // Lanes sharing one domain range (more lanes than domains) split its CPUs.
      const auto domains = lane_topology_.lane_domains(narrow_cast<LaneId>(lane));
      size_t sharing = 0;
      for (size_t other = 0; other != num_lanes; ++other) {
        sharing += lane_topology_.lane_domains(narrow_cast<LaneId>(other)) == domains;
      }
      reactors = std::max<size_t>(
          1, lane_topology_.lane_mask(narrow_cast<LaneId>(lane)).Count() / 4 / sharing);
    }
    lane_reactor_begin_.push_back(lane_reactor_begin_.back() + reactors);
    per_lane += Format("$0$1", per_lane.empty() ? "" : ",", reactors);
  }
  LOG_WITH_PREFIX(INFO) << Format(
      "Lane mode: $0 lanes, $1 reactors (per lane: $2); the configured reactor count $3 "
      "(--num_reactor_threads) is ignored", num_lanes, lane_reactor_begin_.back(), per_lane,
      bld.num_reactors_);

  lane_pools_.reserve(num_lanes);
  for (size_t lane = 0; lane != num_lanes; ++lane) {
    auto lane_pools = std::make_unique<LanePools>();
    lane_pools->tagged = std::make_unique<TaggedThreadPools>(
        [this, lane = narrow_cast<LaneId>(lane)](TaggedThreadPools::Tag tag) {
          return MakeLaneThreadPoolOptions(lane, ServicePriority::kNormal, tag);
        });
    lane_pools->normal = VERIFY_RESULT(lane_pools->tagged->Pool(/*tag=*/0));
    lane_pools_.push_back(std::move(lane_pools));
  }
  // Set last: the lane accessors are used only once all lane state is built.
  num_lanes_ = num_lanes;
  return Status::OK();
}

Status Messenger::Init(const MessengerBuilder &bld) {
  default_normal_thread_pool_ = VERIFY_RESULT(normal_thread_pools_->Pool(/*tag=*/0));
  if (!bld.reactor_lanes_.empty() && std::ssize(bld.reactor_lanes_) != bld.num_reactors_) {
    return STATUS_FORMAT(
        InvalidArgument, "$0 reactor lanes given for $1 reactors", bld.reactor_lanes_.size(),
        bld.num_reactors_);
  }

  // Lane and index within the lane of each reactor.
  std::vector<std::pair<LaneId, int>> reactor_lanes;
  if (bld.lane_topology_ && bld.lane_topology_->active()) {
    RETURN_NOT_OK(InitLaned(bld));
    for (size_t lane = 0; lane != num_lanes_; ++lane) {
      for (auto i = lane_reactor_begin_[lane]; i != lane_reactor_begin_[lane + 1]; ++i) {
        reactor_lanes.emplace_back(
            narrow_cast<LaneId>(lane), narrow_cast<int>(i - lane_reactor_begin_[lane]));
      }
    }
  } else {
    std::vector<int> lane_counts(kMaxLanes);
    for (int i = 0; i < bld.num_reactors_; ++i) {
      auto lane = bld.reactor_lanes_.empty()
          ? kNoLane : NormalizeLane(GetLaneTopology(), bld.reactor_lanes_[i]);
      reactor_lanes.emplace_back(lane, lane == kNoLane ? 0 : lane_counts[lane]++);
    }
  }

  reactors_.reserve(reactor_lanes.size());
  ReactorMonitor* reactor_monitor = nullptr;
  if (FLAGS_rpc_reactor_task_timeout_ms) {
    std::lock_guard lock(lock_);
    reactor_monitor_ = std::make_unique<ReactorMonitor>(
        name_, FLAGS_rpc_reactor_task_timeout_ms * 1ms);
    reactor_monitor = reactor_monitor_.get();
  }
  for (int i = 0; i < std::ssize(reactor_lanes); ++i) {
    auto reactor = std::make_unique<Reactor>(
        this, i, bld, reactor_lanes[i].first, reactor_lanes[i].second);
    if (PREDICT_FALSE(FLAGS_TEST_rpc_reactor_index_for_init_failure_simulation == i)) {
      return STATUS(InternalError, "Reactor::Init simulated error");
    }
    RETURN_NOT_OK(reactor->Init());
    reactors_.push_back(std::move(reactor));
    if (reactor_monitor) {
      reactor_monitor->Track(*reactors_.back());
    }
  }
  return Status::OK();
}

Status Messenger::DumpRunningRpcs(const DumpRunningRpcsRequestPB& req,
                                  DumpRunningRpcsResponsePB* resp) {
  {
    PerCpuRwSharedLock guard(lock_);
    for (const auto& reactor : reactors_) {
      RETURN_NOT_OK(reactor->DumpRunningRpcs(req, resp));
    }
  }
  if (req.get_local_calls()) {
    RETURN_NOT_OK(local_call_tracker_.DumpRunningRpcs(req, resp));
  }
  return Status::OK();
}

Status Messenger::QueueEventOnAllReactors(
    ServerEventListPtr server_event, const SourceLocation& source_location) {
  PerCpuRwSharedLock guard(lock_);
  Status overall_status;
  for (const auto& reactor : reactors_) {
    auto queuing_status = reactor->QueueEventOnAllConnections(server_event, source_location);
    if (!queuing_status.ok()) {
      LOG_WITH_PREFIX(DFATAL) << "Failed to queue a server event on all connections of a reactor: "
                              << queuing_status;
      if (overall_status.ok()) {
        // Use the first error status.
        overall_status = std::move(queuing_status);
      }
    }
  }
  return overall_status;
}

Status Messenger::QueueEventOnFilteredConnections(
    ServerEventListPtr server_event, const SourceLocation& source_location,
    ConnectionFilter connection_filter) {
  PerCpuRwSharedLock guard(lock_);
  Status overall_status;
  for (const auto& reactor : reactors_) {
    auto queuing_status =
        reactor->QueueEventOnFilteredConnections(server_event, source_location, connection_filter);
    if (!queuing_status.ok()) {
      LOG_WITH_PREFIX(DFATAL)
          << "Failed to queue a server event on filtered connections of a reactor: "
          << queuing_status;
      if (overall_status.ok()) {
        // Use the first error status.
        overall_status = std::move(queuing_status);
      }
    }
  }
  return overall_status;
}

void Messenger::RemoveScheduledTask(ScheduledTaskId id) {
  CHECK_GT(id, 0);
  std::lock_guard guard(mutex_scheduled_tasks_);
  scheduled_tasks_.erase(id);
}

void Messenger::AbortOnReactor(ScheduledTaskId task_id) {
  DCHECK(!reactors_.empty());
  CHECK_GT(task_id, 0);

  std::shared_ptr<DelayedTask> task;
  {
    std::lock_guard guard(mutex_scheduled_tasks_);
    auto iter = scheduled_tasks_.find(task_id);
    if (iter != scheduled_tasks_.end()) {
      task = iter->second;
      scheduled_tasks_.erase(iter);
    }
  }
  if (task) {
    task->AbortTask(STATUS(Aborted, "Task aborted by messenger"));
  }
}

Result<ScheduledTaskId> Messenger::ScheduleOnReactor(
    StatusFunctor func, MonoDelta when, const SourceLocation& source_location) {
  if (closing_.load(std::memory_order_acquire)) {
    return STATUS(Aborted, "Cannot schedule task, messenger is closing");
  }
  DCHECK(!reactors_.empty());

  // If we're already running on a reactor thread, reuse it.
  Reactor* chosen_reactor = nullptr;
  for (const auto& r : reactors_) {
    if (r->IsCurrentThread()) {
      chosen_reactor = r.get();
      break;
    }
  }
  if (chosen_reactor == nullptr) {
    // Not running on a reactor thread, pick one at random, among the calling thread's lane's
    // reactors when it has one.
    auto candidates = num_lanes_ != 0 ? ReactorsOfLane(ThisThreadLane())
                                      : std::span<const std::unique_ptr<Reactor>>();
    if (candidates.empty()) {
      candidates = reactors_;
    }
    chosen_reactor = candidates[rand() % candidates.size()].get();
  }

  auto task_id = next_task_id_.fetch_add(1);
  auto task = std::make_shared<DelayedTask>(
      std::move(func), when, task_id, source_location, this);
  {
    std::lock_guard guard(mutex_scheduled_tasks_);
    scheduled_tasks_.emplace(task_id, task);
  }

  auto scheduling_status = chosen_reactor->ScheduleReactorTask(task);

  if (scheduling_status.ok()) {
    return task_id;
  }

  {
    std::lock_guard guard(mutex_scheduled_tasks_);
    scheduled_tasks_.erase(task_id);
  }

  return scheduling_status;
}

scoped_refptr<MetricEntity> Messenger::metric_entity() const {
  return metric_entity_;
}

ThreadPoolOptions Messenger::MakeLaneThreadPoolOptions(
    LaneId lane, ServicePriority priority, TaggedThreadPools::Tag tag) const {
  // Linux shows the first 15 characters of a thread name, so worker names start with the lane.
  const auto lane_name = Format("L$0", ZeroPadded(lane, 3));
  ThreadPoolOptions options {
    .name = Format("$0_$1", name_, lane_name),
    .max_workers = thread_pool_workers_limit_,
    .lane = lane,
    .crossing_category = LaneCrossingCategory::kRpcHandler,
    .worker_name_prefix = lane_name + "_rpc",
  };
  if (priority == ServicePriority::kHigh) {
    options.name += "-high-pri";
    options.worker_name_prefix = lane_name + "_high";
    options.max_workers = FLAGS_rpc_high_priority_workers_limit > 0
        ? static_cast<size_t>(FLAGS_rpc_high_priority_workers_limit)
        : thread_pool_workers_limit_;
    options.cgroup = system_high_cgroup_;
  } else if (tag == 0) {
    options.cgroup = system_med_cgroup_;
  } else {
    options.name += Format("_pool_$0", tag);
    options.worker_name_prefix = Format("$0_t$1", lane_name, tag);
    options.cgroup = cgroup_provider_ ? cgroup_provider_(tag) : nullptr;
  }
  return options;
}

ThreadPoolOptions Messenger::MakeNormalThreadPoolOptions(TaggedThreadPools::Tag tag) const {
  if (tag == 0) {
    return ThreadPoolOptions {
      .name = name_,
      .max_workers = thread_pool_workers_limit_,
      .cgroup = system_med_cgroup_,
    };
  }
  return ThreadPoolOptions {
    .name = Format("$0_pool_$1", name_, tag),
    .max_workers = thread_pool_workers_limit_,
    .cgroup = cgroup_provider_ ? cgroup_provider_(tag) : nullptr,
  };
}

} // namespace rpc
} // namespace yb
