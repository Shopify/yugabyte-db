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

// Laned messengers: reactor groups, per-lane handler pools, lane-keyed connections, the callback
// and local-call lane rules, and inbound binding by lane preamble.

#include <poll.h>

#ifdef __linux__
#include <sys/prctl.h>
#endif

#include <atomic>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "yb/rpc/circular_read_buffer.h"
#include "yb/rpc/compressed_stream.h"
#include "yb/rpc/lane_preamble.h"
#include "yb/rpc/proxy.h"
#include "yb/rpc/reactor.h"
#include "yb/rpc/rpc.h"
#include "yb/rpc/rpc-test-base.h"
#include "yb/rpc/rpc_controller.h"
#include "yb/rpc/rpc_with_queue.h"
#include "yb/rpc/secure_stream.h"
#include "yb/rpc/service_pool.h"
#include "yb/rpc/tcp_stream.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/flags.h"
#include "yb/util/flags/auto_flags.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/logging_test_util.h"
#include "yb/util/metrics.h"
#include "yb/util/operation_counter.h"
#include "yb/util/random_util.h"
#include "yb/util/scope_exit.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/thread.h"
#include "yb/util/tsan_util.h"

using namespace std::literals;

DECLARE_uint64(default_idle_timeout_ms);
DECLARE_bool(enable_rpc_lane_preamble);
DECLARE_uint32(rpc_lane_preamble_peek_timeout_ms);
DECLARE_int32(stream_compression_algo);
DECLARE_int32(TEST_delay_connect_ms);
DECLARE_bool(TEST_rpc_ignore_lane_preamble);
DECLARE_int32(TEST_rpc_lane_preamble_max_pending_sockets);

METRIC_DECLARE_counter(rpc_lane_preamble_absent);
METRIC_DECLARE_counter(rpc_lane_preamble_bound);
METRIC_DECLARE_counter(rpc_lane_preamble_peek_timeouts);
METRIC_DECLARE_counter(rpc_lane_preamble_stripped_late);
METRIC_DECLARE_counter(rpcs_queue_overflow);

namespace yb::rpc {

namespace {

const RemoteMethod& WhoAmIMethod() {
  static const RemoteMethod method(
      rpc_test::CalculatorServiceIf::static_service_name(), "WhoAmI");
  return method;
}

// The calling thread's name as the OS keeps it: at most 15 characters on Linux.
std::string OsThreadName() {
#ifdef __linux__
  char name[16] = {};
  CHECK_EQ(prctl(PR_GET_NAME, name), 0);
  return name;
#else
  return Thread::current_thread() ? Thread::current_thread()->name() : std::string();
#endif
}

struct ThreadView {
  LaneId lane = kNoLane;
  CpuSet mask;
  std::string os_name;
  int64_t thread_id = 0;
  bool on_reactor = false;
};

ThreadView ViewThisThread() {
  return ThreadView {
    .lane = ThisThreadLane(),
    .mask = ThisThreadMask(),
    .os_name = OsThreadName(),
    .thread_id = Thread::UniqueThreadId(),
    .on_reactor = Reactor::IsReactorThread(),
  };
}

ThreadView RunOnReactor(Reactor* reactor, const std::function<void()>& f = {}) {
  ThreadView result;
  CountDownLatch latch(1);
  CHECK_OK(reactor->ScheduleReactorFunctor([&result, &latch, &f](Reactor*) {
    result = ViewThisThread();
    if (f) {
      f();
    }
    latch.CountDown();
  }, SOURCE_LOCATION()));
  latch.Wait();
  return result;
}

ThreadView RunInPool(ThreadPool& pool, const std::function<void()>& f = {}) {
  ThreadView result;
  CountDownLatch latch(1);
  CHECK(pool.EnqueueFunctor([&result, &latch, &f] {
    result = ViewThisThread();
    if (f) {
      f();
    }
    latch.CountDown();
  }));
  latch.Wait();
  return result;
}

// Runs f on a new thread confined to lane (kNoLane: the service set).
void RunInLane(LaneId lane, const std::function<void()>& f) {
  RunOnStdThread([lane, &f] {
    ApplyLaneToCurrentThread(lane);
    f();
  });
}

Status WhoAmI(
    Proxy* proxy, RpcController* controller, rpc_test::WhoAmIResponsePB* resp) {
  rpc_test::WhoAmIRequestPB req;
  return proxy->SyncRequest(&WhoAmIMethod(), /* method_metrics= */ nullptr, req, resp, controller);
}

Status WhoAmIWithLane(Proxy* proxy, LaneId lane, rpc_test::WhoAmIResponsePB* resp = nullptr) {
  rpc_test::WhoAmIResponsePB ignored;
  RpcController controller;
  controller.set_lane(lane);
  return WhoAmI(proxy, &controller, resp ? resp : &ignored);
}

// Calls WhoAmI asynchronously with controller, and returns the state of the thread that ran the
// callback.
ThreadView WhoAmIAsync(Proxy* proxy, RpcController* controller) {
  rpc_test::WhoAmIRequestPB req;
  rpc_test::WhoAmIResponsePB resp;
  ThreadView result;
  CountDownLatch latch(1);
  proxy->AsyncRequest(
      &WhoAmIMethod(), /* method_metrics= */ nullptr, req, &resp, controller,
      [&result, &latch] {
        result = ViewThisThread();
        latch.CountDown();
      });
  latch.Wait();
  CHECK_OK(controller->status());
  return result;
}

Result<std::vector<std::pair<ConnectionId, LaneId>>> ClientConnections(Messenger* messenger) {
  std::vector<std::pair<ConnectionId, LaneId>> result;
  for (size_t i = 0; i != messenger->num_reactors(); ++i) {
    auto* reactor = messenger->TEST_reactor(i);
    for (const auto& id : VERIFY_RESULT(reactor->TEST_ClientConnectionIds())) {
      result.emplace_back(id, reactor->lane());
    }
  }
  return result;
}

// Thread names shorter than the OS limit are padded with 'x', see Thread::StartThread.
std::string PaddedThreadName(std::string name) {
  name.resize(std::max<size_t>(name.size(), 15), 'x');
  return name;
}

std::string LaneName(LaneId lane) {
  return Format("L$0", ZeroPadded(lane, 3));
}

} // namespace

class RpcLanesTest : public RpcTestBase {
 protected:
  void SetUp() override {
    RpcTestBase::SetUp();
    topology_ = MakeTwoLaneTestTopology();
  }

  MessengerBuilder LanedBuilder(
      const std::string& name, const LaneTopology& topology,
      const MessengerOptions& options = kDefaultClientMessengerOptions) {
    auto builder = CreateMessengerBuilder(name, options);
    builder.set_lane_topology(&topology);
    return builder;
  }

  TestServer StartServer(
      std::unique_ptr<Messenger> messenger = nullptr,
      std::optional<ServicePriority> messenger_pools = std::nullopt) {
    TestServerOptions options;
    options.messenger_pools = messenger_pools;
    return StartTestServer(options, "", std::move(messenger));
  }

  std::optional<LaneTopology> topology_;
};

#define SKIP_WITHOUT_TOPOLOGY() \
  if (!topology_) { \
    GTEST_SKIP() << "Needs at least 2 CPUs"; \
  }

// A laned messenger builds its own reactors per lane, ignoring the configured reactor count, and
// names reactor and pool threads by lane within the 15 characters Linux keeps.
TEST_F(RpcLanesTest, LanedReactorsPoolsAndNames) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  StringVectorSink log_sink;
  std::unique_ptr<Messenger> messenger;
  {
    ScopedRegisterSink registered(&log_sink);
    auto builder = LanedBuilder("Laned", *topology_);
    builder.set_num_reactors(7);
    messenger = ASSERT_RESULT(builder.Build());
  }
  auto se = ScopeExit([&messenger] { messenger->Shutdown(); });
  ASSERT_TRUE(messenger->laned());
  ASSERT_EQ(messenger->num_lanes(), 2);
  ASSERT_EQ(messenger->lane_topology(), *topology_);
  // Each test lane has at most 2 CPUs, so the default is one reactor per lane.
  ASSERT_EQ(messenger->num_reactors(), 2);
  bool logged = false;
  for (const auto& msg : log_sink.logged_msgs()) {
    logged = logged || msg.find(
        "Lane mode: 2 lanes, 2 reactors (per lane: 1,1); the configured reactor count 7 "
        "(--num_reactor_threads) is ignored") != std::string::npos;
  }
  ASSERT_TRUE(logged) << AsString(log_sink.logged_msgs());

  for (LaneId lane = 0; lane != 2; ++lane) {
    auto reactors = messenger->ReactorsOfLane(lane);
    ASSERT_EQ(reactors.size(), 1);
    ASSERT_EQ(reactors[0]->lane(), lane);
    // Lanes are reduced modulo the lane count.
    ASSERT_EQ(messenger->ReactorsOfLane(lane + 2)[0].get(), reactors[0].get());
    auto view = RunOnReactor(reactors[0].get());
    ASSERT_EQ(view.lane, lane);
    ASSERT_EQ(view.mask, topology_->lane_mask(lane));
    ASSERT_EQ(view.os_name, PaddedThreadName(Format("$0_reactor_0", LaneName(lane))));
    ASSERT_TRUE(view.on_reactor);

    auto& normal = messenger->ThreadPool(ServicePriority::kNormal, lane);
    ASSERT_EQ(normal.options().name, Format("Laned_$0", LaneName(lane)));
    ASSERT_EQ(normal.options().lane, lane);
    ASSERT_EQ(normal.options().crossing_category, LaneCrossingCategory::kRpcHandler);
    ASSERT_EQ(normal.options().max_workers, messenger->ThreadPool().options().max_workers);
    ASSERT_STR_CONTAINS(
        normal.options().ToString(),
        Format("{ name: Laned_$0 max_workers: $1 ", LaneName(lane), normal.options().max_workers));
    ASSERT_STR_CONTAINS(
        normal.options().ToString(), Format(" lane: $0 crossing_category: kRpcHandler }", lane));
    view = RunInPool(normal);
    ASSERT_EQ(view.lane, lane);
    ASSERT_EQ(view.mask, topology_->lane_mask(lane));
    ASSERT_TRUE(view.os_name.starts_with(LaneName(lane) + "_rpc_")) << view.os_name;

    auto& high = messenger->ThreadPool(ServicePriority::kHigh, lane);
    ASSERT_EQ(high.options().name, Format("Laned_$0-high-pri", LaneName(lane)));
    ASSERT_EQ(high.options().max_workers,
              messenger->ThreadPool(ServicePriority::kHigh).options().max_workers);
    view = RunInPool(high);
    ASSERT_EQ(view.lane, lane);
    ASSERT_TRUE(view.os_name.starts_with(LaneName(lane) + "_high_")) << view.os_name;

    auto tagged = ASSERT_RESULT(messenger->TaggedThreadPool(5, lane));
    ASSERT_EQ(tagged->options().name, Format("Laned_$0_pool_5", LaneName(lane)));
    view = RunInPool(*tagged);
    ASSERT_EQ(view.lane, lane);
    ASSERT_TRUE(view.os_name.starts_with(LaneName(lane) + "_t5_")) << view.os_name;
  }

  // kNoLane gives the unlaned pools, which keep their names and run in the service set.
  auto& unlaned = messenger->ThreadPool(ServicePriority::kNormal, kNoLane);
  ASSERT_EQ(&unlaned, &messenger->ThreadPool());
  ASSERT_EQ(unlaned.options().name, "Laned");
  auto view = RunInPool(unlaned);
  ASSERT_EQ(view.lane, kNoLane);
  ASSERT_EQ(view.mask, topology_->service_mask());
  ASSERT_TRUE(view.os_name.starts_with("Laned_")) << view.os_name;
}

// The default reactor count per lane is max(1, CPUs in the lane / 4 / lanes sharing the lane's
// domains). The topologies are larger than the test host, so nothing is pinned.
TEST_F(RpcLanesTest, DefaultReactorsPerLane) {
  struct Case {
    std::vector<CpuSet> domains = {};
    CpuSet service_cpus = {};
    size_t lanes = 0;
    std::vector<size_t> expected = {};
    int reactors_per_lane = 0;
  };
  auto cpus = [](const std::string& list) { return CHECK_RESULT(CpuSet::Parse(list)); };
  std::vector<Case> cases = {
    // One lane per 16-CPU domain.
    {.domains = {cpus("0-15"), cpus("16-31")}, .lanes = 2, .expected = {4, 4}},
    // Two lanes per domain split its CPUs.
    {.domains = {cpus("0-15"), cpus("16-31")}, .lanes = 4, .expected = {2, 2, 2, 2}},
    // Service CPUs shrink one domain, so the lanes get different reactor counts.
    {.domains = {cpus("0-15"), cpus("16-31")}, .service_cpus = cpus("16-23"), .lanes = 2,
     .expected = {4, 2}},
    // Small lanes still get one reactor.
    {.domains = {cpus("0-1"), cpus("2-3")}, .lanes = 4, .expected = {1, 1, 1, 1}},
    // An explicit count applies to every lane.
    {.domains = {cpus("0-15"), cpus("16-31")}, .lanes = 4, .expected = {3, 3, 3, 3},
     .reactors_per_lane = 3},
  };
  for (const auto& c : cases) {
    CpuSet process_cpuset;
    for (const auto& domain : c.domains) {
      process_cpuset |= domain;
    }
    auto topology = LaneTopology::Build(LaneTopologyOptions {
      .domains = c.domains,
      .process_cpuset = process_cpuset,
      .requested_lanes = c.lanes,
      .service_cpus = c.service_cpus,
      .affinity_enabled = false,
    });
    SCOPED_TRACE(topology.ToString());
    ASSERT_EQ(topology.num_lanes(), c.lanes);
    TEST_SetLaneTopology scope(topology);
    auto builder = LanedBuilder("Laned", topology);
    builder.set_reactors_per_lane(c.reactors_per_lane);
    auto messenger = ASSERT_RESULT(builder.Build());
    auto se = ScopeExit([&messenger] { messenger->Shutdown(); });
    size_t total = 0;
    for (size_t lane = 0; lane != c.lanes; ++lane) {
      auto reactors = messenger->ReactorsOfLane(narrow_cast<LaneId>(lane));
      ASSERT_EQ(reactors.size(), c.expected[lane]) << "lane " << lane;
      for (const auto& reactor : reactors) {
        ASSERT_EQ(reactor->lane(), lane);
      }
      total += reactors.size();
    }
    ASSERT_EQ(messenger->num_reactors(), total);
  }
}

// The builder's topology must be the installed one, since threads take their masks from it.
TEST_F(RpcLanesTest, InvalidLaneSettings) {
  SKIP_WITHOUT_TOPOLOGY();
  {
    TEST_SetLaneTopology scope{LaneTopology()};
    auto result = LanedBuilder("Laned", *topology_).Build();
    ASSERT_NOK(result);
    ASSERT_TRUE(result.status().IsInvalidArgument()) << result.status();
    ASSERT_STR_CONTAINS(result.status().message().ToBuffer(), "is not the process lane topology");
  }
  TEST_SetLaneTopology scope(*topology_);
  for (int connections : {0, 257}) {
    auto builder = LanedBuilder("Laned", *topology_);
    builder.set_connections_per_lane_remote(connections);
    auto result = builder.Build();
    ASSERT_NOK(result);
    ASSERT_TRUE(result.status().IsInvalidArgument()) << result.status();
  }
  auto builder = LanedBuilder("Laned", *topology_);
  builder.set_num_reactors(2).set_reactor_lanes({0, 1});
  ASSERT_NOK(builder.Build());
}

// A messenger without an active lane topology is exactly today's: reactor count and names, pool
// identity, connection ids, local calls, no affinity calls and no counted handoffs. This holds
// with lanes off and also, for messengers given no topology (masters, the CQL server), with lanes
// on.
TEST_F(RpcLanesTest, UnlanedMessengerIsUnchanged) {
  for (bool lane_mode : {false, true}) {
    if (lane_mode && !topology_) {
      continue;
    }
    SCOPED_TRACE(Format("lane mode: $0", lane_mode));
    TEST_SetLaneTopology scope(lane_mode ? *topology_ : LaneTopology());
    const auto calls = TEST_CpuAffinityCallCount();
    const auto counts = LaneHandoffCounts::Now();
    auto options = kDefaultClientMessengerOptions;
    options.n_reactors = 3;
    options.num_connections_to_server = 2;
    auto builder = CreateMessengerBuilder("Plain", options);
    if (!lane_mode) {
      // An inactive topology leaves the messenger unlaned.
      builder.set_lane_topology(&GetLaneTopology());
    }
    auto server = StartServer(ASSERT_RESULT(builder.Build()), ServicePriority::kNormal);
    auto* messenger = server.messenger();
    ASSERT_FALSE(messenger->laned());
    ASSERT_EQ(messenger->num_lanes(), 0);
    ASSERT_EQ(messenger->num_reactors(), 3);
    ASSERT_TRUE(messenger->ReactorsOfLane(0).empty());
    for (auto priority : {ServicePriority::kNormal, ServicePriority::kHigh}) {
      auto* pool = &messenger->ThreadPool(priority);
      for (LaneId lane : {LaneId(0), LaneId(1), kNoLane}) {
        ASSERT_EQ(&messenger->ThreadPool(priority, lane), pool);
        ASSERT_EQ(messenger->HandlerThreadPool(priority, 0, lane).get(), pool);
      }
    }
    auto tagged = ASSERT_RESULT(messenger->TaggedThreadPool(5));
    ASSERT_EQ(ASSERT_RESULT(messenger->TaggedThreadPool(5, 1)), tagged);
    ASSERT_EQ(messenger->ThreadPool().options().name, "Plain");
    ASSERT_EQ(messenger->ThreadPool().options().lane, kNoLane);
    for (size_t i = 0; i != messenger->num_reactors(); ++i) {
      auto view = RunOnReactor(messenger->TEST_reactor(i));
      ASSERT_EQ(view.lane, kNoLane);
      ASSERT_EQ(view.os_name, PaddedThreadName("Plain_reactor"));
    }

    // Hints are ignored: connections keep today's ids.
    Proxy proxy(messenger, HostPort::FromBoundEndpoint(server.bound_endpoint()));
    for (int i = 0; i != 4; ++i) {
      ASSERT_OK(WhoAmIWithLane(&proxy, narrow_cast<LaneId>(i % 2)));
    }
    ASSERT_EQ(proxy.TEST_LaneCallCount(0), 0);
    std::set<int> indexes;
    for (const auto& [id, reactor_lane] : ASSERT_RESULT(ClientConnections(messenger))) {
      ASSERT_EQ(id.lane(), kNoLane);
      ASSERT_EQ(reactor_lane, kNoLane);
      ASSERT_EQ(id.ToString().find("lane"), std::string::npos) << id.ToString();
      indexes.insert(id.idx());
    }
    ASSERT_EQ(indexes, (std::set<int>{0, 1}));

    // A local call from a handler thread runs inline, whatever the hint.
    Proxy local_proxy(messenger, HostPort());
    for (LaneId hint : {kNoLane, LaneId(1)}) {
      rpc_test::WhoAmIResponsePB resp;
      auto caller = RunInPool(messenger->ThreadPool(), [&local_proxy, &resp, hint] {
        RpcController controller;
        controller.set_allow_local_calls_in_curr_thread(true);
        controller.set_lane(hint);
        CHECK_OK(WhoAmI(&local_proxy, &controller, &resp));
      });
      ASSERT_EQ(resp.handler_thread_id(), caller.thread_id);
    }
    server.Shutdown();
    if (!lane_mode) {
      ASSERT_EQ(TEST_CpuAffinityCallCount(), calls);
    }
    auto delta = LaneHandoffCounts::Now() - counts;
    ASSERT_EQ(delta.total_crossings(), 0) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 0);
  }
}

// Calls with a lane hint go on (remote, lane, idx) connections owned by reactors of that lane,
// k per (remote, lane); calls without a hint keep unlaned connections.
TEST_F(RpcLanesTest, HintedCallsUseLaneConnections) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartServer();
  auto options = kDefaultClientMessengerOptions;
  options.num_connections_to_server = 3;
  auto builder = LanedBuilder("Client", *topology_, options);
  builder.set_reactors_per_lane(2).set_connections_per_lane_remote(2);
  auto client = rpc::CreateAutoShutdownMessengerHolder(ASSERT_RESULT(builder.Build()));
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));

  RunInLane(0, [&proxy] {
    // 3 is reduced to lane 1.
    for (LaneId hint : {LaneId(0), LaneId(3), kNoLane}) {
      for (int i = 0; i != 4; ++i) {
        ASSERT_OK(WhoAmIWithLane(&proxy, hint));
      }
    }
  });

  std::map<LaneId, std::set<int>> indexes;
  for (const auto& [id, reactor_lane] : ASSERT_RESULT(ClientConnections(client.get()))) {
    indexes[id.lane()].insert(id.idx());
    if (id.lane() == kNoLane) {
      ASSERT_EQ(id.ToString().find("lane"), std::string::npos) << id.ToString();
    } else {
      ASSERT_EQ(reactor_lane, id.lane()) << id.ToString();
      ASSERT_STR_CONTAINS(id.ToString(), Format(" idx: $0 protocol: ", id.idx()));
      ASSERT_TRUE(id.ToString().ends_with(Format(" lane: $0 }", id.lane()))) << id.ToString();
    }
  }
  ASSERT_EQ(indexes.size(), 3);
  ASSERT_EQ(indexes[0], (std::set<int>{0, 1}));
  ASSERT_EQ(indexes[1], (std::set<int>{0, 1}));
  // Unlaned connections are spread over num_connections_to_server, also across lanes' reactors.
  ASSERT_EQ(indexes[kNoLane], (std::set<int>{0, 1, 2}));
  server.Shutdown();
}

// Threads of two lanes calling into one lane through one Proxy use their own counters, and each
// spreads its calls over the k connections.
TEST_F(RpcLanesTest, CounterSlotPerSubmitterLane) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartServer();
  auto builder = LanedBuilder("Client", *topology_);
  builder.set_connections_per_lane_remote(2);
  auto client = rpc::CreateAutoShutdownMessengerHolder(ASSERT_RESULT(builder.Build()));
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));

  auto call_from = [&proxy](LaneId submitter) {
    RunInLane(submitter, [&proxy] {
      ASSERT_OK(WhoAmIWithLane(&proxy, 1));
    });
  };
  auto used_indexes = [&client]() -> Result<std::set<int>> {
    std::set<int> result;
    for (const auto& [id, reactor_lane] : VERIFY_RESULT(ClientConnections(client.get()))) {
      SCHECK_EQ(id.lane(), 1, IllegalState, id.ToString());
      result.insert(id.idx());
    }
    return result;
  };

  call_from(0);
  ASSERT_EQ(ASSERT_RESULT(used_indexes()), (std::set<int>{0}));
  // A shared counter would give lane 1 the next index.
  call_from(1);
  ASSERT_EQ(ASSERT_RESULT(used_indexes()), (std::set<int>{0}));
  call_from(0);
  ASSERT_EQ(ASSERT_RESULT(used_indexes()), (std::set<int>{0, 1}));
  call_from(1);
  call_from(kNoLane);
  ASSERT_EQ(proxy.TEST_LaneCallCount(0), 2);
  ASSERT_EQ(proxy.TEST_LaneCallCount(1), 2);
  ASSERT_EQ(proxy.TEST_LaneCallCount(kNoLane), 1);
  server.Shutdown();
}

// Closing a laned client connection removes it from its reactor, and later calls reconnect.
TEST_F(RpcLanesTest, CloseLanedConnections) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartServer();
  auto client = rpc::CreateAutoShutdownMessengerHolder(
      ASSERT_RESULT(LanedBuilder("Client", *topology_).Build()));
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
  for (int i = 0; i != 4; ++i) {
    ASSERT_OK(WhoAmIWithLane(&proxy, narrow_cast<LaneId>(i / 2)));
  }
  ASSERT_EQ(ASSERT_RESULT(ClientConnections(client.get())).size(), 4);

  client->BreakConnectivityWith(server.bound_endpoint().address());
  ASSERT_OK(WaitFor([&client]() -> Result<bool> {
    return VERIFY_RESULT(ClientConnections(client.get())).empty();
  }, 30s, "Laned connections closed"));
  client->RestoreConnectivityWith(server.bound_endpoint().address());

  ASSERT_OK(WhoAmIWithLane(&proxy, 1));
  auto connections = ASSERT_RESULT(ClientConnections(client.get()));
  ASSERT_EQ(connections.size(), 1);
  ASSERT_EQ(connections[0].first.lane(), 1);
  server.Shutdown();
}

// An inbound call is handled by the handler pool of the lane of the reactor that read it, for
// normal and high-priority services. The client is unlaned, so its connection carries no lane
// preamble and is not bound to a lane.
TEST_F(RpcLanesTest, HandlerRunsInReactorLane) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  for (auto priority : {ServicePriority::kNormal, ServicePriority::kHigh}) {
    SCOPED_TRACE(AsString(priority));
    auto server = StartServer(
        ASSERT_RESULT(LanedBuilder("Server", *topology_).Build()), priority);
    auto options = kDefaultClientMessengerOptions;
    options.num_connections_to_server = 1;
    auto client = CreateAutoShutdownMessengerHolder("Client", options);
    Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
    rpc_test::WhoAmIResponsePB resp;
    ASSERT_OK(WhoAmIWithLane(&proxy, kNoLane, &resp));

    // The one connection went to one of the server's reactors, one per lane.
    LaneId reactor_lane = kNoLane;
    for (size_t i = 0; i != server.messenger()->num_reactors(); ++i) {
      ReactorMetrics metrics;
      ASSERT_OK(server.messenger()->TEST_GetReactorMetrics(i, &metrics));
      if (metrics.num_server_connections) {
        ASSERT_EQ(reactor_lane, kNoLane);
        reactor_lane = server.messenger()->TEST_reactor(i)->lane();
      }
    }
    ASSERT_NE(reactor_lane, kNoLane);
    ASSERT_EQ(resp.handler_lane(), reactor_lane);
    ASSERT_TRUE(resp.handler_thread_name().starts_with(Format(
        "$0_$1_", LaneName(reactor_lane),
        priority == ServicePriority::kNormal ? "rpc" : "high"))) << resp.handler_thread_name();
    server.Shutdown();
  }
}

// A local call runs inline only on a handler thread of its own lane's pool; otherwise it is
// queued to its lane's pool. Without a hint its lane is the issuing thread's.
TEST_F(RpcLanesTest, LocalCallLaneRule) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartServer(
      ASSERT_RESULT(LanedBuilder("Server", *topology_).Build()), ServicePriority::kNormal);
  auto* messenger = server.messenger();
  Proxy local_proxy(messenger, HostPort());

  struct LocalCallResult {
    ThreadView caller;
    rpc_test::WhoAmIResponsePB resp;
  };
  auto call_from_pool = [&local_proxy, messenger](LaneId pool_lane, LaneId hint) {
    LocalCallResult result;
    result.caller = RunInPool(
        messenger->ThreadPool(ServicePriority::kNormal, pool_lane), [&local_proxy, &result, hint] {
      RpcController controller;
      controller.set_allow_local_calls_in_curr_thread(true);
      controller.set_lane(hint);
      CHECK_OK(WhoAmI(&local_proxy, &controller, &result.resp));
    });
    return result;
  };

  auto counts = LaneHandoffCounts::Now();
  // Same lane: inline.
  auto result = call_from_pool(0, 0);
  ASSERT_EQ(result.resp.handler_thread_id(), result.caller.thread_id);
  ASSERT_EQ(result.resp.handler_lane(), 0);
  // No hint: the issuing lane, so inline too.
  result = call_from_pool(1, kNoLane);
  ASSERT_EQ(result.resp.handler_thread_id(), result.caller.thread_id);
  ASSERT_EQ(result.resp.handler_lane(), 1);
  auto delta = LaneHandoffCounts::Now() - counts;
  ASSERT_EQ(delta.crossing(LaneCrossingCategory::kRpcHandler), 0) << delta.ToString();

  // Another lane: queued to that lane's pool, one handler crossing.
  result = call_from_pool(0, 1);
  ASSERT_NE(result.resp.handler_thread_id(), result.caller.thread_id);
  ASSERT_EQ(result.resp.handler_lane(), 1);
  ASSERT_TRUE(result.resp.handler_thread_name().starts_with("L001_rpc_"))
      << result.resp.handler_thread_name();
  delta = LaneHandoffCounts::Now() - counts;
  ASSERT_EQ(delta.crossing(LaneCrossingCategory::kRpcHandler), 1) << delta.ToString();

  // From a lane thread outside the handler pools: queued, in the hinted lane.
  RunInLane(0, [&local_proxy, &result] {
    RpcController controller;
    controller.set_allow_local_calls_in_curr_thread(true);
    controller.set_lane(0);
    CHECK_OK(WhoAmI(&local_proxy, &controller, &result.resp));
    ASSERT_NE(result.resp.handler_thread_id(), Thread::UniqueThreadId());
  });
  ASSERT_EQ(result.resp.handler_lane(), 0);
  server.Shutdown();
}

// Callbacks run in the pool of the lane that issued the call, also when the call is resent from a
// reactor of another lane, as a retry is.
TEST_F(RpcLanesTest, CallbackRunsInIssuingLane) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartServer();
  auto client = rpc::CreateAutoShutdownMessengerHolder(
      ASSERT_RESULT(LanedBuilder("Client", *topology_).Build()));
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
  // Sets up the lane 1 connection outside the counted windows.
  ASSERT_OK(WhoAmIWithLane(&proxy, 1));

  RpcController controller;
  ThreadView view;
  auto counts = LaneHandoffCounts::Now();
  RunInLane(0, [&] {
    controller.set_lane(1);
    view = WhoAmIAsync(&proxy, &controller);
  });
  ASSERT_EQ(controller.issuing_lane(), 0);
  ASSERT_EQ(view.lane, 0);
  ASSERT_TRUE(view.os_name.starts_with("L000_rpc_")) << view.os_name;
  // The call is handed from lane 0 to a lane 1 reactor, and the response from it to a lane 0
  // callback thread.
  auto delta = LaneHandoffCounts::Now() - counts;
  ASSERT_EQ(delta.crossing(LaneCrossingCategory::kReactor), 1) << delta.ToString();
  ASSERT_EQ(delta.crossing(LaneCrossingCategory::kCallback), 1);
  ASSERT_EQ(delta.crossing(LaneCrossingCategory::kRpcHandler), 0);
  ASSERT_EQ(delta.total_crossings(), 2);
  ASSERT_EQ(delta.entries_from_unlaned, 0);

  // High priority callbacks use the lane's high-priority pool.
  controller.Reset();
  ASSERT_EQ(controller.lane(), kNoLane);
  controller.set_invoke_callback_mode(InvokeCallbackMode::kThreadPoolHigh);
  RunInLane(0, [&] {
    view = WhoAmIAsync(&proxy, &controller);
  });
  ASSERT_TRUE(view.os_name.starts_with("L000_high_")) << view.os_name;
  controller.set_invoke_callback_mode(InvokeCallbackMode::kThreadPoolNormal);

  // A resend scheduled from lane 1 runs on a lane 1 reactor and keeps the issuing lane.
  ThreadView resend_thread;
  CountDownLatch resent(1);
  rpc_test::WhoAmIRequestPB req;
  rpc_test::WhoAmIResponsePB resp;
  RunInLane(1, [&] {
    ASSERT_OK(client->ScheduleOnReactor([&](const Status& status) {
      CHECK_OK(status);
      resend_thread = ViewThisThread();
      controller.Reset();
      controller.set_lane(1);
      proxy.AsyncRequest(
          &WhoAmIMethod(), /* method_metrics= */ nullptr, req, &resp, &controller,
          [&view, &resent] {
            view = ViewThisThread();
            resent.CountDown();
          });
    }, 0s, SOURCE_LOCATION()));
  });
  resent.Wait();
  ASSERT_OK(controller.status());
  ASSERT_TRUE(resend_thread.on_reactor);
  ASSERT_EQ(resend_thread.lane, 1);
  ASSERT_EQ(controller.issuing_lane(), 0);
  ASSERT_EQ(view.lane, 0);
  ASSERT_TRUE(view.os_name.starts_with("L000_rpc_")) << view.os_name;

  // A first send from a reactor records the reactor's lane.
  RpcController reactor_controller;
  rpc_test::WhoAmIResponsePB reactor_resp;
  RunOnReactor(client->ReactorsOfLane(1)[0].get(), [&] {
    proxy.AsyncRequest(
        &WhoAmIMethod(), /* method_metrics= */ nullptr, req, &reactor_resp, &reactor_controller,
        [] {});
  });
  ASSERT_EQ(reactor_controller.issuing_lane(), 1);

  // A controller reused from a thread of another lane records that lane.
  controller.Reset();
  RunInLane(1, [&] {
    view = WhoAmIAsync(&proxy, &controller);
  });
  ASSERT_EQ(controller.issuing_lane(), 1);
  ASSERT_EQ(view.lane, 1);

  // An unlaned issuer gets today's pool.
  controller.Reset();
  RunInLane(kNoLane, [&] {
    view = WhoAmIAsync(&proxy, &controller);
  });
  ASSERT_EQ(controller.issuing_lane(), kNoLane);
  ASSERT_EQ(view.lane, kNoLane);
  ASSERT_TRUE(view.os_name.starts_with("Client_")) << view.os_name;
  ASSERT_OK(WaitFor([&reactor_controller] { return reactor_controller.finished(); }, 10s,
                    "Call from reactor finished"));
  server.Shutdown();
}

// A pinned issuing lane decides the callback pool for every later send, from any thread, and is
// kept across Reset().
TEST_F(RpcLanesTest, PinnedIssuingLane) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartServer();
  auto client = rpc::CreateAutoShutdownMessengerHolder(
      ASSERT_RESULT(LanedBuilder("Client", *topology_).Build()));
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));

  RpcController controller;
  controller.set_issuing_lane(0);
  ThreadView view;
  RunInLane(1, [&] {
    view = WhoAmIAsync(&proxy, &controller);
  });
  ASSERT_EQ(controller.issuing_lane(), 0);
  ASSERT_EQ(view.lane, 0);
  ASSERT_TRUE(view.os_name.starts_with("L000_rpc_")) << view.os_name;

  controller.Reset();
  rpc_test::WhoAmIRequestPB req;
  rpc_test::WhoAmIResponsePB resp;
  CountDownLatch done(1);
  RunOnReactor(client->ReactorsOfLane(1)[0].get(), [&] {
    proxy.AsyncRequest(
        &WhoAmIMethod(), /* method_metrics= */ nullptr, req, &resp, &controller,
        [&view, &done] {
          view = ViewThisThread();
          done.CountDown();
        });
  });
  done.Wait();
  ASSERT_OK(controller.status());
  ASSERT_EQ(controller.issuing_lane(), 0);
  ASSERT_EQ(view.lane, 0);

  // Pinning kNoLane selects today's pools from a lane thread.
  controller.Reset();
  controller.set_issuing_lane(kNoLane);
  RunInLane(1, [&] {
    view = WhoAmIAsync(&proxy, &controller);
  });
  ASSERT_EQ(controller.issuing_lane(), kNoLane);
  ASSERT_EQ(view.lane, kNoLane);
  ASSERT_TRUE(view.os_name.starts_with("Client_")) << view.os_name;
  server.Shutdown();
}

namespace {

// Sends WhoAmI with lane hint 1 through an RpcRetrier, which retries the first attempt from a
// lane 1 thread, and records the threads that ran each attempt's callback.
class LaneRetryCommand : public RpcCommand {
 public:
  LaneRetryCommand(Messenger* messenger, ProxyCache* proxy_cache, Proxy* proxy)
      : retrier_(CoarseMonoClock::Now() + 60s, messenger, proxy_cache), proxy_(*proxy) {}

  void SendRpc() override {
    send_threads_.push_back(ViewThisThread());
    auto* controller = retrier_.mutable_controller();
    controller->set_lane(1);
    proxy_.AsyncRequest(
        &WhoAmIMethod(), /* method_metrics= */ nullptr, req_, &resp_, controller, [this] {
      callback_threads_.push_back(ViewThisThread());
      CHECK_OK(retrier_.controller().status());
      if (callback_threads_.size() == 1) {
        RunInLane(1, [this] {
          CHECK_OK(retrier_.DelayedRetry(this, STATUS(TryAgain, "Retry from lane 1")));
        });
      } else {
        done_.CountDown();
      }
    });
  }

  std::string ToString() const override { return "LaneRetryCommand"; }

  void Finished(const Status& status) override {
    CHECK_OK(status);
  }

  void Abort() override {}

  CoarseTimePoint deadline() const override { return retrier_.deadline(); }

  void Wait() {
    done_.Wait();
    retrier_.Abort();
  }

  const std::vector<ThreadView>& send_threads() const { return send_threads_; }
  const std::vector<ThreadView>& callback_threads() const { return callback_threads_; }
  const RpcController& controller() const { return retrier_.controller(); }

 private:
  RpcRetrier retrier_;
  Proxy& proxy_;
  rpc_test::WhoAmIRequestPB req_;
  rpc_test::WhoAmIResponsePB resp_;
  std::vector<ThreadView> send_threads_;
  std::vector<ThreadView> callback_threads_;
  CountDownLatch done_{1};
};

} // namespace

// A call retried by RpcRetrier, whose retry runs on a reactor of another lane, calls back in the
// lane that issued it.
TEST_F(RpcLanesTest, RetryKeepsIssuingLane) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartServer();
  auto client = rpc::CreateAutoShutdownMessengerHolder(
      ASSERT_RESULT(LanedBuilder("Client", *topology_).Build()));
  ProxyCache proxy_cache(client.get());
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
  auto command = std::make_shared<LaneRetryCommand>(client.get(), &proxy_cache, &proxy);
  RunInLane(0, [&command] {
    command->SendRpc();
  });
  command->Wait();
  ASSERT_EQ(command->send_threads().size(), 2);
  ASSERT_EQ(command->send_threads()[1].lane, 1);
  ASSERT_TRUE(command->send_threads()[1].on_reactor);
  ASSERT_EQ(command->controller().issuing_lane(), 0);
  for (const auto& view : command->callback_threads()) {
    ASSERT_EQ(view.lane, 0);
    ASSERT_TRUE(view.os_name.starts_with("L000_rpc_")) << view.os_name;
  }
  server.Shutdown();
}

// Tagged pools are per (lane, tag), one instance per lane: an idle tagged pool of a lane is
// reclaimed when that lane next creates a tagged pool, without touching other lanes.
TEST_F(RpcLanesTest, TaggedPoolsPerLane) {
  SKIP_WITHOUT_TOPOLOGY();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_default_idle_timeout_ms) = 100;
  TEST_SetLaneTopology scope(*topology_);
  auto messenger = rpc::CreateAutoShutdownMessengerHolder(
      ASSERT_RESULT(LanedBuilder("Laned", *topology_).Build()));
  constexpr ThreadPoolTag kTags[] = {5, 6, 7};
  for (auto tag : kTags) {
    std::set<ThreadPool*> pools;
    for (LaneId lane : {LaneId(0), LaneId(1), kNoLane}) {
      auto pool = ASSERT_RESULT(messenger->TaggedThreadPool(tag, lane));
      ASSERT_EQ(ASSERT_RESULT(messenger->TaggedThreadPool(tag, lane)), pool);
      ASSERT_EQ(pool->options().lane, lane);
      pools.insert(pool.get());
      ASSERT_EQ(RunInPool(*pool).lane, lane);
    }
    ASSERT_EQ(pools.size(), 3);
  }
  // Tag 0 is the lane's normal pool.
  ASSERT_EQ(ASSERT_RESULT(messenger->TaggedThreadPool(0, 1)).get(),
            &messenger->ThreadPool(ServicePriority::kNormal, 1));
  // T tags in L lanes: L x (T + 1) pools, the normal ones included.
  for (LaneId lane : {LaneId(0), LaneId(1)}) {
    ASSERT_EQ(messenger->TEST_ActiveTaggedPools(lane), std::size(kTags) + 1);
  }

  // Idle workers exit after the idle timeout.
  ASSERT_OK(WaitFor([&messenger, &kTags] {
    for (auto tag : kTags) {
      for (LaneId lane : {LaneId(0), LaneId(1)}) {
        if (CHECK_RESULT(messenger->TaggedThreadPool(tag, lane))->NumWorkers() != 0) {
          return false;
        }
      }
    }
    return true;
  }, 30s, "Idle workers exit"));

  // A new tagged pool in lane 0 reclaims lane 0's idle ones, and only those.
  ASSERT_OK(messenger->TaggedThreadPool(8, 0));
  ASSERT_EQ(messenger->TEST_ActiveTaggedPools(0), 2);
  ASSERT_EQ(messenger->TEST_ActiveTaggedPools(1), std::size(kTags) + 1);
  ASSERT_OK(messenger->TaggedThreadPool(8, 1));
  ASSERT_EQ(messenger->TEST_ActiveTaggedPools(1), 2);
}

// Messenger::ScheduleOnReactor called from a lane thread uses a reactor of that lane.
TEST_F(RpcLanesTest, ScheduleOnReactorInCallerLane) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto builder = LanedBuilder("Laned", *topology_);
  builder.set_reactors_per_lane(2);
  auto messenger = rpc::CreateAutoShutdownMessengerHolder(ASSERT_RESULT(builder.Build()));
  for (LaneId lane : {LaneId(0), LaneId(1)}) {
    for (int i = 0; i != 10; ++i) {
      ThreadView view;
      CountDownLatch latch(1);
      RunInLane(lane, [&] {
        ASSERT_OK(messenger->ScheduleOnReactor([&](const Status& status) {
          view = ViewThisThread();
          latch.CountDown();
        }, 0s, SOURCE_LOCATION()));
      });
      latch.Wait();
      ASSERT_TRUE(view.on_reactor);
      ASSERT_EQ(view.lane, lane);
    }
  }
}

namespace {

// num_lanes lanes of one fake CPU each, without affinity, for tests that only set thread lanes.
LaneTopology FakeLanes(size_t num_lanes) {
  std::vector<CpuSet> domains(num_lanes);
  for (size_t i = 0; i != num_lanes; ++i) {
    domains[i].Set(i);
  }
  return LaneTopology::Build(LaneTopologyOptions{
      .domains = domains,
      .process_cpuset = CpuSet::FirstN(num_lanes),
      .requested_lanes = num_lanes,
      .affinity_enabled = false,
  });
}

void RunWithLane(LaneId lane, const std::function<void()>& f) {
  RunOnStdThread([lane, &f] {
    SetThisThreadLane(lane);
    f();
  });
}

ThreadPoolPtr NoWorkersPool() {
  return std::make_shared<ThreadPool>(ThreadPoolOptions {
    .name = "no_workers",
    .max_workers = 0
  });
}

} // namespace

// A laned ServicePool admits a call of a lane below its share (ceil(limit / L)) by writing only
// that lane's slot; beyond the share it sums the slots, so a hot lane can use the whole limit but
// an idle lane cannot add to a full queue. Calls without a lane use the spare slot.
TEST_F(RpcLanesTest, ServicePoolLaneAdmission) {
  constexpr size_t kLanes = 4;
  constexpr size_t kLimit = 10;
  constexpr int64_t kShare = 3;
  auto messenger = CreateAutoShutdownMessengerHolder("Server");
  auto pool = make_scoped_refptr<ServicePool>(
      kLimit, [thread_pool = NoWorkersPool()](auto, auto) { return thread_pool; },
      &messenger->scheduler(), std::make_unique<GenericCalculatorService>(), metric_entity(),
      kLanes);
  auto slots = [&pool] { return pool->TEST_LaneQueuedCalls(); };
  using Slots = std::vector<int64_t>;

  // Below the share: only the lane's slot is written; the position is that slot's count.
  for (int64_t i = 0; i != kShare; ++i) {
    ASSERT_EQ(pool->TEST_CallQueued(0), i);
  }
  ASSERT_EQ(slots(), (Slots{kShare, 0, 0, 0, 0}));
  ASSERT_EQ(pool->TEST_SharedQueuedCalls(), 0);
  ASSERT_EQ(pool->TEST_LanesOverShare(), 0);

  // A hot lane fills the whole queue.
  for (size_t i = kShare; i != kLimit; ++i) {
    ASSERT_TRUE(pool->TEST_CallQueued(0));
  }
  ASSERT_FALSE(pool->TEST_CallQueued(0));
  ASSERT_EQ(pool->TEST_LanesOverShare(), 1);
  // An idle lane, below its share, is refused too: the queue is full.
  ASSERT_FALSE(pool->TEST_CallQueued(1));
  ASSERT_EQ(pool->QueuedCalls(), kLimit);
  ASSERT_EQ(slots(), (Slots{kLimit, 0, 0, 0, 0}));

  // Calls without a lane go to the spare slot, which counts in the sum.
  pool->TEST_CallDequeued(0);
  pool->TEST_CallDequeued(0);
  ASSERT_TRUE(pool->TEST_CallQueued(kNoLane));
  ASSERT_TRUE(pool->TEST_CallQueued(1));
  ASSERT_FALSE(pool->TEST_CallQueued(2));
  ASSERT_EQ(slots(), (Slots{kLimit - 2, 1, 0, 0, 1}));

  pool->TEST_CallDequeued(kNoLane);
  pool->TEST_CallDequeued(1);
  for (size_t i = 0; i != kLimit - 2; ++i) {
    pool->TEST_CallDequeued(0);
  }
  ASSERT_EQ(slots(), (Slots{0, 0, 0, 0, 0}));
  ASSERT_EQ(pool->TEST_LanesOverShare(), 0);
  pool->StartShutdown();
  pool->CompleteShutdown();

  // An unlaned pool keeps one shared count.
  auto unlaned = make_scoped_refptr<ServicePool>(
      kLimit, [thread_pool = NoWorkersPool()](auto, auto) { return thread_pool; },
      &messenger->scheduler(), std::make_unique<GenericCalculatorService>(), metric_entity());
  ASSERT_TRUE(unlaned->TEST_CallQueued(0));
  ASSERT_TRUE(unlaned->TEST_LaneQueuedCalls().empty());
  ASSERT_EQ(unlaned->TEST_SharedQueuedCalls(), 1);
  unlaned->TEST_CallDequeued(0);
  unlaned->StartShutdown();
  unlaned->CompleteShutdown();
}

// Calls without a lane share the spare slot, whose share is 0: they always take the sum, and while
// any is queued so do the lanes. So filling every slot, the spare included, up to its lane share
// queues no more than limit + L - 1 calls, in whichever order the slots fill.
TEST_F(RpcLanesTest, ServicePoolSpareSlotBound) {
  constexpr size_t kLanes = 4;
  constexpr int64_t kLimit = 10;
  constexpr int64_t kShare = 3;
  auto messenger = CreateAutoShutdownMessengerHolder("Server");
  auto pool = make_scoped_refptr<ServicePool>(
      kLimit, [thread_pool = NoWorkersPool()](auto, auto) { return thread_pool; },
      &messenger->scheduler(), std::make_unique<GenericCalculatorService>(), metric_entity(),
      kLanes);
  using Slots = std::vector<int64_t>;
  auto fill_to_share = [&pool](LaneId lane) {
    int64_t admitted = 0;
    for (int64_t i = 0; i != kShare; ++i) {
      admitted += pool->TEST_CallQueued(lane).has_value();
    }
    return admitted;
  };
  auto drain = [&pool](const Slots& slots) {
    for (size_t i = 0; i != slots.size(); ++i) {
      for (auto n = slots[i]; n; --n) {
        pool->TEST_CallDequeued(i < kLanes ? narrow_cast<LaneId>(i) : kNoLane);
      }
    }
  };

  // Lanes first: they fill their shares on the fast path, then the spare is refused by the sum.
  for (LaneId lane = 0; lane != kLanes; ++lane) {
    ASSERT_EQ(fill_to_share(lane), kShare);
  }
  ASSERT_EQ(fill_to_share(kNoLane), 0);
  ASSERT_LE(pool->QueuedCalls(), kLimit + static_cast<int64_t>(kLanes) - 1);
  ASSERT_EQ(pool->TEST_LaneQueuedCalls(), (Slots{kShare, kShare, kShare, kShare, 0}));
  drain(pool->TEST_LaneQueuedCalls());
  ASSERT_EQ(pool->TEST_LanesOverShare(), 0);

  // Spare first: while it holds calls the lanes take the sum, so the queue stops at the limit.
  ASSERT_EQ(fill_to_share(kNoLane), kShare);
  ASSERT_EQ(pool->TEST_LanesOverShare(), 1);
  for (LaneId lane = 0; lane != kLanes; ++lane) {
    fill_to_share(lane);
  }
  ASSERT_EQ(pool->QueuedCalls(), kLimit);
  drain(pool->TEST_LaneQueuedCalls());
  ASSERT_EQ(pool->QueuedCalls(), 0);
  ASSERT_EQ(pool->TEST_LanesOverShare(), 0);
  pool->StartShutdown();
  pool->CompleteShutdown();
}

// Lanes, and a thread queuing calls without a lane, admitting and releasing at once never have more
// than limit + L - 1 calls queued.
TEST_F(RpcLanesTest, ServicePoolConcurrentAdmissionBound) {
  constexpr size_t kLanes = 4;
  constexpr int64_t kLimit = 20;
  auto messenger = CreateAutoShutdownMessengerHolder("Server");
  auto pool = make_scoped_refptr<ServicePool>(
      kLimit, [thread_pool = NoWorkersPool()](auto, auto) { return thread_pool; },
      &messenger->scheduler(), std::make_unique<GenericCalculatorService>(), metric_entity(),
      kLanes);
  // Never above the calls the pool counts as admitted: raised after an admit, lowered before the
  // release.
  std::atomic<int64_t> admitted{0};
  std::atomic<int64_t> max_admitted{0};
  std::atomic<int64_t> rejected{0};
  TestThreadHolder threads;
  std::vector<LaneId> lanes;
  for (LaneId lane = 0; lane != kLanes; ++lane) {
    lanes.push_back(lane);
  }
  lanes.push_back(kNoLane);
  for (auto lane : lanes) {
    threads.AddThreadFunctor([&, lane, &stop = threads.stop_flag()] {
      int64_t mine = 0;
      while (!stop.load()) {
        if (pool->TEST_CallQueued(lane)) {
          ++mine;
          auto now = ++admitted;
          auto max = max_admitted.load();
          while (now > max && !max_admitted.compare_exchange_weak(max, now)) {}
          continue;
        }
        ++rejected;
        for (auto n = RandomUniformInt<int64_t>(0, mine); n; --n, --mine) {
          --admitted;
          pool->TEST_CallDequeued(lane);
        }
      }
      for (; mine; --mine) {
        --admitted;
        pool->TEST_CallDequeued(lane);
      }
    });
  }
  threads.WaitAndStop(1s);
  LOG(INFO) << "Max admitted: " << max_admitted.load() << ", rejected: " << rejected.load();
  ASSERT_LE(max_admitted.load(), kLimit + static_cast<int64_t>(kLanes) - 1);
  ASSERT_GE(max_admitted.load(), kLimit - static_cast<int64_t>(kLanes));
  ASSERT_GT(rejected.load(), 0);
  ASSERT_EQ(pool->QueuedCalls(), 0);
  ASSERT_EQ(pool->TEST_LanesOverShare(), 0);
  pool->StartShutdown();
  pool->CompleteShutdown();
}

// Calls read by one lane's reactor fill a laned pool's whole queue; the next is refused with the
// queue's total in the message and counted in rpcs_queue_overflow.
TEST_F(RpcLanesTest, LanedServiceQueueOverflow) {
  SKIP_WITHOUT_TOPOLOGY();
  constexpr size_t kLimit = 4;
  TEST_SetLaneTopology scope(*topology_);
  auto builder = LanedBuilder("Server", *topology_, kDefaultServerMessengerOptions);
  builder.set_metric_entity(metric_entity());
  auto server = rpc::CreateAutoShutdownMessengerHolder(ASSERT_RESULT(builder.Build()));
  Endpoint endpoint;
  ASSERT_OK(server->ListenAddress(
      CreateConnectionContextFactory<YBInboundConnectionContext>(), Endpoint(), &endpoint));
  auto service = std::make_unique<GenericCalculatorService>();
  auto service_name = service->service_name();
  auto thread_pool = NoWorkersPool();
  auto pool = make_scoped_refptr<ServicePool>(
      kLimit, [thread_pool](auto, auto) { return thread_pool; },
      &server->scheduler(), std::move(service), metric_entity(), server->num_lanes());
  ASSERT_OK(server->RegisterService(service_name, pool));
  ASSERT_OK(server->StartAcceptor());
  auto overflow = METRIC_rpcs_queue_overflow.Instantiate(metric_entity());
  const auto overflow_before = overflow->value();

  auto options = kDefaultClientMessengerOptions;
  options.num_connections_to_server = 1;
  auto client = CreateAutoShutdownMessengerHolder("Client", options);
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(endpoint));
  struct Call {
    RpcController controller;
    rpc_test::AddRequestPB req;
    rpc_test::AddResponsePB resp;
  };
  constexpr size_t kCalls = kLimit + 1;
  std::vector<Call> calls(kCalls);
  CountDownLatch first_done(1);
  for (auto& call : calls) {
    call.req.set_x(1);
    call.req.set_y(2);
    call.controller.set_timeout(60s);
    proxy.AsyncRequest(
        CalculatorServiceMethods::AddMethod(), /* method_metrics= */ nullptr, call.req,
        &call.resp, &call.controller, [&first_done] { first_done.CountDown(); });
  }
  first_done.Wait();
  const auto& refused = calls.back().controller.status();
  ASSERT_TRUE(refused.IsRemoteError()) << refused;
  ASSERT_STR_CONTAINS(
      refused.ToString(),
      Format("dropped due to backpressure. The service queue is full, it has $0 items", kLimit));
  ASSERT_EQ(overflow->value() - overflow_before, 1);
  auto slots = pool->TEST_LaneQueuedCalls();
  ASSERT_EQ(std::count(slots.begin(), slots.end(), kLimit), 1) << AsString(slots);
  ASSERT_EQ(pool->QueuedCalls(), kLimit);

  server->UnregisterAllServices();
  pool->Shutdown();
  thread_pool->Shutdown();
  server->Shutdown();
  client->Shutdown();
}

// A laned messenger takes a per-lane operation counter for each call handled from a lane thread,
// and shutdown stops every one of them.
TEST_F(RpcLanesTest, RpcServicesCounterPerLane) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto unlaned_server = StartServer();
  auto unlaned = unlaned_server.messenger();
  ASSERT_EQ(unlaned->TEST_RpcServicesCounters().size(), 1);
  RunWithLane(1, [unlaned] {
    ASSERT_EQ(unlaned->TEST_RpcServicesCounterOfThisThread(),
              unlaned->TEST_RpcServicesCounters()[0]);
  });
  unlaned_server.Shutdown();

  auto server = StartServer(
      ASSERT_RESULT(LanedBuilder("Server", *topology_).Build()), ServicePriority::kNormal);
  auto* messenger = server.messenger();
  auto counters = messenger->TEST_RpcServicesCounters();
  ASSERT_EQ(counters.size(), 3);
  ASSERT_EQ(messenger->TEST_RpcServicesCounterOfThisThread(), counters[0]);
  for (LaneId lane = 0; lane != 2; ++lane) {
    RunWithLane(lane, [messenger, &counters, lane] {
      ASSERT_EQ(messenger->TEST_RpcServicesCounterOfThisThread(), counters[lane + 1]);
    });
  }
  // A call from each lane, and one without a lane, succeeds through the laned messenger's counters.
  for (LaneId lane : {LaneId{0}, LaneId{1}, kNoLane}) {
    RunWithLane(lane, [messenger] {
      Proxy local_proxy(messenger, HostPort());
      rpc_test::WhoAmIResponsePB resp;
      RpcController controller;
      ASSERT_OK(WhoAmI(&local_proxy, &controller, &resp));
    });
  }
  messenger->UnregisterAllServices();
  for (const auto* counter : counters) {
    ASSERT_TRUE(counter->TEST_IsStopped());
  }
  server.Shutdown();
}

// Local calls of each lane are tracked by that lane's tracker, and DumpRunningRpcs lists all.
TEST_F(RpcLanesTest, LocalCallTrackersPerLane) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartServer(
      ASSERT_RESULT(LanedBuilder("Server", *topology_).Build()), ServicePriority::kNormal);
  auto* messenger = server.messenger();
  Proxy local_proxy(messenger, HostPort());
  struct Call {
    RpcController controller;
    rpc_test::SleepRequestPB req;
    rpc_test::SleepResponsePB resp;
  };
  std::vector<Call> calls(3);
  CountDownLatch latch(calls.size());
  for (size_t i = 0; i != calls.size(); ++i) {
    auto& call = calls[i];
    call.req.set_sleep_micros(2 * 1000000);
    call.req.set_deferred(true);
    call.controller.set_lane(i < 2 ? narrow_cast<LaneId>(i) : kNoLane);
    local_proxy.AsyncRequest(
        CalculatorServiceMethods::SleepMethod(), /* method_metrics= */ nullptr, call.req,
        &call.resp, &call.controller, latch.CountDownCallback());
  }
  DumpRunningRpcsRequestPB dump_req;
  dump_req.set_get_local_calls(true);
  DumpRunningRpcsResponsePB dump_resp;
  ASSERT_OK(messenger->DumpRunningRpcs(dump_req, &dump_resp));
  ASSERT_EQ(dump_resp.local_calls().calls_in_flight_size(), calls.size()) << AsString(dump_resp);
  latch.Wait();
  for (const auto& call : calls) {
    ASSERT_OK(call.controller.status());
  }
  server.Shutdown();
}

namespace {

// Registered in an Rpcs registry; Abort completes it, unregistering it as a finished call does.
class AbortableCommand : public RpcCommand {
 public:
  void SendRpc() override {}
  std::string ToString() const override { return "AbortableCommand"; }
  void Finished(const Status& status) override {}
  CoarseTimePoint deadline() const override { return CoarseMonoClock::Now() + 60s; }

  void Abort() override {
    aborted_ = true;
    if (rpcs_ && handle_ != rpcs_->InvalidHandle()) {
      rpcs_->Unregister(&handle_);
    }
  }

  bool Register(Rpcs* rpcs) {
    rpcs_ = rpcs;
    handle_ = rpcs->InvalidHandle();
    rpcs->Register(shared_from(this), &handle_);
    return handle_ != rpcs->InvalidHandle();
  }

  bool aborted() const { return aborted_; }
  Rpcs::Handle* handle() { return &handle_; }
  Rpcs* rpcs() const { return rpcs_; }

 private:
  std::atomic<bool> aborted_{false};
  Rpcs* rpcs_ = nullptr;
  Rpcs::Handle handle_;
};

} // namespace

// LaneShardedRpcs: a registry per lane and one for unlaned threads; a handle is unregistered
// through its own registry from any thread, and abort and shutdown cover every registry.
TEST_F(RpcLanesTest, LaneShardedRpcs) {
  {
    // Built without lanes: one registry for every thread.
    rpc::LaneShardedRpcs rpcs;
    ASSERT_EQ(rpcs.num_lanes(), 0);
    RunWithLane(1, [&rpcs] {
      ASSERT_EQ(&rpcs.Local(), &rpcs.Unlaned());
    });
  }

  constexpr size_t kLanes = 2;
  TEST_SetLaneTopology scope(FakeLanes(kLanes));
  rpc::LaneShardedRpcs rpcs;
  ASSERT_EQ(rpcs.num_lanes(), kLanes);
  std::vector<Rpcs*> registries;
  for (LaneId lane = 0; lane != kLanes; ++lane) {
    RunWithLane(lane, [&rpcs, &registries] { registries.push_back(&rpcs.Local()); });
  }
  registries.push_back(&rpcs.Local());
  ASSERT_EQ(registries.back(), &rpcs.Unlaned());
  ASSERT_EQ(std::set<Rpcs*>(registries.begin(), registries.end()).size(), kLanes + 1);

  // Registered from lane 0, unregistered from a lane 1 thread through the handle's registry.
  auto command = std::make_shared<AbortableCommand>();
  RunWithLane(0, [&rpcs, &command] { ASSERT_TRUE(command->Register(&rpcs.Local())); });
  ASSERT_EQ(rpcs.TEST_NumActiveCalls(), 1);
  ASSERT_EQ(registries[0]->TEST_NumActiveCalls(), 1);
  RunWithLane(1, [&command] {
    ASSERT_EQ(command->rpcs()->Unregister(command->handle()), command);
    ASSERT_EQ(*command->handle(), command->rpcs()->InvalidHandle());
  });
  ASSERT_EQ(rpcs.TEST_NumActiveCalls(), 0);

  auto register_everywhere = [&registries] {
    std::vector<std::shared_ptr<AbortableCommand>> result;
    for (auto* registry : registries) {
      result.push_back(std::make_shared<AbortableCommand>());
      CHECK(result.back()->Register(registry));
    }
    return result;
  };
  auto commands = register_everywhere();
  ASSERT_EQ(rpcs.TEST_NumActiveCalls(), kLanes + 1);
  rpcs.RequestAbortAll();
  for (const auto& aborted : commands) {
    ASSERT_TRUE(aborted->aborted());
  }
  ASSERT_EQ(rpcs.TEST_NumActiveCalls(), 0);

  commands = register_everywhere();
  rpcs.Shutdown();
  for (const auto& aborted : commands) {
    ASSERT_TRUE(aborted->aborted());
  }
  // Every registry refuses calls after shutdown.
  for (auto* registry : registries) {
    auto late = std::make_shared<AbortableCommand>();
    ASSERT_FALSE(late->Register(registry));
    ASSERT_TRUE(late->aborted());
  }
}


namespace {

YB_DEFINE_ENUM(LaneStack, (kPlain)(kCompressed)(kSecure)(kSecureCompressed));

const MessengerOptions kLanePreambleServerOptions = {
  .n_reactors = 2,
  // Long enough that no server connection closes as idle while a test inspects it.
  .keep_alive_timeout = 60s,
};

std::string LanePreamble(uint8_t lane) {
  return std::string(kLanePreambleMagic, sizeof(kLanePreambleMagic)) + static_cast<char>(lane);
}

Status WhoAmIWithTimeout(
    Proxy* proxy, LaneId lane, rpc_test::WhoAmIResponsePB* resp = nullptr,
    MonoDelta timeout = MonoDelta(10s) * kTimeMultiplier) {
  rpc_test::WhoAmIResponsePB ignored;
  RpcController controller;
  controller.set_lane(lane);
  controller.set_timeout(timeout);
  return WhoAmI(proxy, &controller, resp ? resp : &ignored);
}

// Server connections of the messenger per reactor lane.
Result<std::map<LaneId, size_t>> ServerConnectionsByLane(Messenger* messenger) {
  std::map<LaneId, size_t> result;
  for (size_t i = 0; i != messenger->num_reactors(); ++i) {
    ReactorMetrics metrics;
    RETURN_NOT_OK(messenger->TEST_GetReactorMetrics(i, &metrics));
    result[messenger->TEST_reactor(i)->lane()] += metrics.num_server_connections;
  }
  return result;
}

Result<size_t> ServerConnections(Messenger* messenger) {
  size_t result = 0;
  for (const auto& [lane, count] : VERIFY_RESULT(ServerConnectionsByLane(messenger))) {
    result += count;
  }
  return result;
}

Result<Socket> RawConnect(const Endpoint& endpoint) {
  Socket socket;
  RETURN_NOT_OK(socket.Init(0));
  RETURN_NOT_OK(socket.SetNoDelay(true));
  RETURN_NOT_OK(socket.Connect(endpoint));
  return socket;
}

Status RawSend(Socket* socket, std::string_view bytes) {
  return socket->BlockingWrite(
      pointer_cast<const uint8_t*>(bytes.data()), bytes.size(), MonoTime::Now() + 10s);
}

// Forwards one connection from a client to a target server. Bytes from the client are held while
// hold() returns true, and the lane byte of a preamble at their start can be replaced, so that a
// real client's preamble reaches the server late or with another lane.
class Relay {
 public:
  struct Options {
    std::function<bool()> hold;
    std::optional<uint8_t> lane_byte;
  };

  ~Relay() {
    threads_.Stop();
  }

  Status Start(const Endpoint& target, Options options) {
    RETURN_NOT_OK(listen_socket_.Init(0));
    RETURN_NOT_OK(listen_socket_.BindAndListen(
        Endpoint(IpAddress::from_string("127.0.0.1"), 0), /* listen_queue_size= */ 1));
    RETURN_NOT_OK(listen_socket_.GetSocketAddress(&endpoint_));
    RETURN_NOT_OK(listen_socket_.SetNonBlocking(true));
    threads_.AddThreadFunctor([this, target, options = std::move(options)] {
      auto status = Run(target, options);
      LOG_IF(INFO, !status.ok()) << "Relay stopped: " << status;
    });
    return Status::OK();
  }

  HostPort host_port() const {
    return HostPort::FromBoundEndpoint(endpoint_);
  }

 private:
  Status Run(const Endpoint& target, const Options& options) {
    auto& stop = threads_.stop_flag();
    Socket client;
    Endpoint remote;
    for (;;) {
      SCHECK(!stop.load(), Aborted, "Stopped before a client connected");
      auto status = listen_socket_.Accept(&client, &remote, Socket::FLAG_NONBLOCKING);
      if (status.ok()) {
        break;
      }
      SCHECK(status.IsTryAgain(), IllegalState, status.ToString());
      std::this_thread::sleep_for(1ms);
    }
    auto server = VERIFY_RESULT(RawConnect(target));
    RETURN_NOT_OK(server.SetNonBlocking(true));
    while (options.hold && options.hold()) {
      SCHECK(!stop.load(), Aborted, "Stopped while holding");
      std::this_thread::sleep_for(1ms);
    }
    size_t client_bytes = 0;
    std::array<pollfd, 2> fds = {
        pollfd{.fd = client.GetFd(), .events = POLLIN, .revents = 0},
        pollfd{.fd = server.GetFd(), .events = POLLIN, .revents = 0}};
    std::array<Socket*, 2> sockets = {&client, &server};
    while (!stop.load()) {
      if (::poll(fds.data(), fds.size(), 10) <= 0) {
        continue;
      }
      for (size_t i = 0; i != fds.size(); ++i) {
        if (!(fds[i].revents & (POLLIN | POLLHUP | POLLERR))) {
          continue;
        }
        uint8_t buf[4096];
        auto received = sockets[i]->Recv(buf, sizeof(buf));
        if (!received.ok()) {
          if (received.status().IsTryAgain()) {
            continue;
          }
          return received.status();
        }
        if (i == 0) {
          const auto lane_pos = sizeof(kLanePreambleMagic);
          if (options.lane_byte && client_bytes <= lane_pos &&
              client_bytes + *received > lane_pos) {
            buf[lane_pos - client_bytes] = *options.lane_byte;
          }
          client_bytes += *received;
        }
        RETURN_NOT_OK(sockets[1 - i]->BlockingWrite(buf, *received, MonoTime::Now() + 10s));
      }
    }
    return Status::OK();
  }

  Socket listen_socket_;
  Endpoint endpoint_;
  TestThreadHolder threads_;
};

// A connection context of a protocol other than YB RPC, as CQL and Redis have, recording the bytes
// it receives.
class RecordingConnectionContext : public ConnectionContextWithQueue {
 public:
  RecordingConnectionContext(
      size_t receive_buffer_size, const MemTrackerPtr& buffer_tracker, const MemTrackerPtr&)
      : ConnectionContextWithQueue(/* max_concurrent_calls= */ 1, /* max_queued_bytes= */ 1_MB),
        read_buffer_(receive_buffer_size, buffer_tracker) {}

  static std::string Name() { return "Recording"; }

  static std::string Received() {
    std::lock_guard lock(mutex_);
    return received_;
  }

 private:
  Result<ProcessCallsResult> ProcessCalls(
      const ConnectionPtr& connection, const IoVecs& data, ReadBufferFull read_buffer_full)
      ON_REACTOR_THREAD override {
    size_t consumed = 0;
    std::lock_guard lock(mutex_);
    for (const auto& iov : data) {
      received_.append(static_cast<const char*>(iov.iov_base), iov.iov_len);
      consumed += iov.iov_len;
    }
    return ProcessCallsResult{.consumed = consumed, .buffer = Slice()};
  }

  Status Connected(const ConnectionPtr& connection) override { return Status::OK(); }
  RpcConnectionPB::StateType State() override { return RpcConnectionPB::OPEN; }
  StreamReadBuffer& ReadBuffer() override { return read_buffer_; }

  CircularReadBuffer read_buffer_;

  static std::mutex mutex_;
  static std::string received_;
};

std::mutex RecordingConnectionContext::mutex_;
std::string RecordingConnectionContext::received_;

} // namespace

class RpcLanePreambleTest : public RpcLanesTest {
 protected:
  void SetUp() override {
    RpcLanesTest::SetUp();
    secure_context_ = std::make_unique<SecureContext>(
        RequireClientCertificate::kFalse, UseClientCertificate::kFalse);
    ASSERT_OK(secure_context_->TEST_GenerateKeys(2048, "127.0.0.1", MatchingCertKeyPair::kTrue));
  }

  void ApplyStack(LaneStack stack, MessengerBuilder* builder) {
    auto secure_factory = [this] {
      return SecureStreamFactory(
          TcpStream::Factory(), MemTracker::GetRootTracker(), secure_context_.get());
    };
    switch (stack) {
      case LaneStack::kPlain:
        return;
      case LaneStack::kCompressed:
        builder->SetListenProtocol(CompressedStreamProtocol());
        builder->AddStreamFactory(
            CompressedStreamProtocol(),
            CompressedStreamFactory(TcpStream::Factory(), MemTracker::GetRootTracker()));
        return;
      case LaneStack::kSecure:
        builder->SetListenProtocol(SecureStreamProtocol());
        builder->SetUncompressedProtocol(SecureStreamProtocol());
        builder->AddStreamFactory(SecureStreamProtocol(), secure_factory());
        return;
      case LaneStack::kSecureCompressed:
        builder->SetListenProtocol(CompressedStreamProtocol());
        builder->SetUncompressedProtocol(SecureStreamProtocol());
        builder->AddStreamFactory(
            CompressedStreamProtocol(),
            CompressedStreamFactory(secure_factory(), MemTracker::GetRootTracker()));
        return;
    }
    FATAL_INVALID_ENUM_VALUE(LaneStack, stack);
  }

  // A laned server messenger with reactors_per_lane reactors per lane, whose calls are handled by
  // the handler pools of the lane of the reactor that read them.
  TestServer StartLanedServer(LaneStack stack = LaneStack::kPlain, int reactors_per_lane = 1) {
    auto builder = LanedBuilder("Server", *topology_, kLanePreambleServerOptions);
    builder.set_reactors_per_lane(reactors_per_lane);
    ApplyStack(stack, &builder);
    return StartServer(EXPECT_RESULT(builder.Build()), ServicePriority::kNormal);
  }

  AutoShutdownMessengerHolder LanedClient(
      LaneStack stack = LaneStack::kPlain, int connections_per_lane_remote = 1) {
    auto builder = LanedBuilder("Client", *topology_);
    builder.set_connections_per_lane_remote(connections_per_lane_remote);
    ApplyStack(stack, &builder);
    return rpc::CreateAutoShutdownMessengerHolder(EXPECT_RESULT(builder.Build()));
  }

  AutoShutdownMessengerHolder UnlanedClient(LaneStack stack = LaneStack::kPlain) {
    auto builder = CreateMessengerBuilder("UnlanedClient");
    ApplyStack(stack, &builder);
    return rpc::CreateAutoShutdownMessengerHolder(EXPECT_RESULT(builder.Build()));
  }

  // Value of a preamble counter of the test metric entity, which servers and clients share; -1
  // when it is not instantiated.
  int64_t PreambleCounter(const CounterPrototype& prototype) {
    auto metric = metric_entity()->FindOrNull<Counter>(prototype);
    return metric ? metric->value() : -1;
  }

  struct PreambleCounts {
    int64_t bound;
    int64_t absent;
    int64_t peek_timeouts;
    int64_t stripped_late;

    bool operator==(const PreambleCounts&) const = default;

    std::string ToString() const {
      return YB_STRUCT_TO_STRING(bound, absent, peek_timeouts, stripped_late);
    }
  };

  PreambleCounts Counts() {
    return PreambleCounts {
      .bound = PreambleCounter(METRIC_rpc_lane_preamble_bound),
      .absent = PreambleCounter(METRIC_rpc_lane_preamble_absent),
      .peek_timeouts = PreambleCounter(METRIC_rpc_lane_preamble_peek_timeouts),
      .stripped_late = PreambleCounter(METRIC_rpc_lane_preamble_stripped_late),
    };
  }

  Status WaitCounts(
      const PreambleCounts& expected, MonoDelta timeout = MonoDelta(10s) * kTimeMultiplier) {
    return LoggedWaitFor([this, &expected] { return Counts() == expected; }, timeout,
                         Format("Preamble counts $0", expected));
  }

  // The first size bytes that a call through messenger with the lane hint writes on a new
  // connection, read by a listening socket.
  Result<std::string> FirstBytesOnWire(Messenger* messenger, LaneId hint, size_t size) {
    Socket listen_socket;
    HostPort host_port;
    RETURN_NOT_OK(StartFakeServer(&listen_socket, &host_port));
    Proxy proxy(messenger, host_port);
    RpcController controller;
    controller.set_lane(hint);
    controller.set_timeout(30s * kTimeMultiplier);
    rpc_test::WhoAmIRequestPB req;
    rpc_test::WhoAmIResponsePB resp;
    CountDownLatch done(1);
    proxy.AsyncRequest(
        &WhoAmIMethod(), /* method_metrics= */ nullptr, req, &resp, &controller,
        [&done] { done.CountDown(); });
    auto result = [&listen_socket, size]() -> Result<std::string> {
      Socket connection;
      Endpoint remote;
      RETURN_NOT_OK(listen_socket.Accept(&connection, &remote, 0));
      std::string bytes(size, '\0');
      RETURN_NOT_OK(connection.BlockingRecv(
          pointer_cast<uint8_t*>(bytes.data()), size, MonoTime::Now() + 10s * kTimeMultiplier));
      // Closing the connection fails the call.
      return bytes;
    }();
    done.Wait();
    return result;
  }

  std::unique_ptr<SecureContext> secure_context_;
};

class RpcLanePreambleStackTest : public RpcLanePreambleTest,
                                 public testing::WithParamInterface<LaneStack> {
 protected:
  void SetUp() override {
    if (GetParam() == LaneStack::kCompressed || GetParam() == LaneStack::kSecureCompressed) {
      ANNOTATE_UNPROTECTED_WRITE(FLAGS_stream_compression_algo) = 1;
    }
    RpcLanePreambleTest::SetUp();
  }

  // The first two bytes of the stream's own header: the TLS ClientHello, or "YB" for a compressed
  // stream's header and the YB connection header.
  std::string StackHeader() {
    return GetParam() == LaneStack::kSecure || GetParam() == LaneStack::kSecureCompressed
        ? std::string("\x16\x03", 2) : std::string("YB");
  }
};

// The preamble precedes the stream's own first bytes, whatever the upper layers are, only on
// lane-keyed connections of a laned messenger, and only while the AutoFlag is set.
TEST_P(RpcLanePreambleStackTest, FirstBytesOnWire) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto laned = LanedClient(GetParam());
  auto unlaned = UnlanedClient(GetParam());
  const auto header = StackHeader();
  ASSERT_EQ(ASSERT_RESULT(FirstBytesOnWire(laned.get(), 1, 5)), LanePreamble(1) + header);
  ASSERT_EQ(ASSERT_RESULT(FirstBytesOnWire(laned.get(), 0, 5)), LanePreamble(0) + header);
  // A hint above the lane count is reduced before it keys the connection.
  ASSERT_EQ(ASSERT_RESULT(FirstBytesOnWire(laned.get(), 3, 5)), LanePreamble(1) + header);
  ASSERT_EQ(ASSERT_RESULT(FirstBytesOnWire(laned.get(), kNoLane, 2)), header);
  ASSERT_EQ(ASSERT_RESULT(FirstBytesOnWire(unlaned.get(), 1, 2)), header);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_rpc_lane_preamble) = false;
  ASSERT_EQ(ASSERT_RESULT(FirstBytesOnWire(laned.get(), 1, 2)), header);
}

// A connection with a preamble is bound to a reactor of the preamble's lane, a lane's connections
// take its reactors in turn, and calls are handled in that lane.
TEST_P(RpcLanePreambleStackTest, BindsToPreambleLane) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartLanedServer(GetParam(), /* reactors_per_lane= */ 2);
  auto client = LanedClient(GetParam(), /* connections_per_lane_remote= */ 2);
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
  for (LaneId lane : {LaneId(0), LaneId(1)}) {
    for (int i = 0; i != 4; ++i) {
      rpc_test::WhoAmIResponsePB resp;
      ASSERT_OK(WhoAmIWithTimeout(&proxy, lane, &resp));
      ASSERT_EQ(resp.handler_lane(), lane);
    }
  }
  ASSERT_EQ(Counts(), (PreambleCounts{
      .bound = 4, .absent = 0, .peek_timeouts = 0, .stripped_late = 0}));
  for (size_t i = 0; i != server.messenger()->num_reactors(); ++i) {
    ReactorMetrics metrics;
    ASSERT_OK(server.messenger()->TEST_GetReactorMetrics(i, &metrics));
    ASSERT_EQ(metrics.num_server_connections, 1) << "reactor " << i;
  }
  server.Shutdown();
}

// Without a preamble (an older sender, or the AutoFlag off) a laned receiver consumes nothing and
// assigns the connection as without lanes.
TEST_P(RpcLanePreambleStackTest, NoPreamble) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_rpc_lane_preamble) = false;
  auto server = StartLanedServer(GetParam());
  auto client = LanedClient(GetParam());
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
  for (LaneId lane : {LaneId(0), LaneId(1), kNoLane}) {
    ASSERT_OK(WhoAmIWithTimeout(&proxy, lane));
  }
  ASSERT_EQ(Counts(), (PreambleCounts{
      .bound = 0, .absent = 3, .peek_timeouts = 0, .stripped_late = 0}));
  ASSERT_EQ(ASSERT_RESULT(ServerConnections(server.messenger())), 3);
  server.Shutdown();
}

// A receiver that neither peeks for nor strips the preamble, as one that predates it, rejects a
// connection that starts with one, which is why sending it is gated by an AutoFlag.
TEST_P(RpcLanePreambleStackTest, OldReceiverRejectsPreamble) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_rpc_ignore_lane_preamble) = true;
  auto server = StartLanedServer(GetParam());
  auto client = LanedClient(GetParam());
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
  auto status = WhoAmIWithTimeout(&proxy, 1);
  ASSERT_NOK(status);
  LOG(INFO) << "Call with a preamble to an old receiver: " << status;
  ASSERT_OK(WhoAmIWithTimeout(&proxy, kNoLane));
  server.Shutdown();
}

// A preamble that arrives after the peek timeout is dropped by the reactor, and the connection,
// not bound to a lane, serves calls.
TEST_P(RpcLanePreambleStackTest, LatePreambleStrippedByReactor) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_lane_preamble_peek_timeout_ms) = 100;
  auto server = StartLanedServer(GetParam());
  auto client = LanedClient(GetParam());
  Relay relay;
  ASSERT_OK(relay.Start(server.bound_endpoint(), Relay::Options{
      .hold = [this] { return PreambleCounter(METRIC_rpc_lane_preamble_peek_timeouts) < 1; },
      .lane_byte = std::nullopt,
  }));
  Proxy proxy(client.get(), relay.host_port());
  ASSERT_OK(WhoAmIWithTimeout(&proxy, 1));
  ASSERT_EQ(Counts(), (PreambleCounts{
      .bound = 0, .absent = 0, .peek_timeouts = 1, .stripped_late = 1}));
  server.Shutdown();
}

INSTANTIATE_TEST_SUITE_P(
    , RpcLanePreambleStackTest,
    testing::Values(
        LaneStack::kPlain, LaneStack::kCompressed, LaneStack::kSecure,
        LaneStack::kSecureCompressed),
    [](const testing::TestParamInfo<LaneStack>& info) {
      return ToString(info.param).substr(1);
    });

// The lane byte comes from the network: any value, including kNoLane's, binds to its value modulo
// the lane count.
TEST_F(RpcLanePreambleTest, SpoofedLaneIsReduced) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartLanedServer();
  auto client = LanedClient();
  int64_t bound = 0;
  for (uint8_t lane_byte : {2, 3, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}) {
    SCOPED_TRACE(Format("lane byte $0", static_cast<int>(lane_byte)));
    Relay relay;
    ASSERT_OK(relay.Start(server.bound_endpoint(), Relay::Options{
        .hold = {}, .lane_byte = lane_byte}));
    Proxy proxy(client.get(), relay.host_port());
    rpc_test::WhoAmIResponsePB resp;
    ASSERT_OK(WhoAmIWithTimeout(&proxy, 0, &resp));
    ASSERT_EQ(resp.handler_lane(), lane_byte % 2);
    ASSERT_EQ(PreambleCounter(METRIC_rpc_lane_preamble_bound), ++bound);
  }
  server.Shutdown();
}

// The acceptor decides on the first bytes as soon as they allow, waits for a preamble split over
// several reads, and gives up at the timeout; a socket that closes falls back at once.
TEST_F(RpcLanePreambleTest, PeekOutcomes) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  // Long enough that every outcome but the timeout one shows the acceptor did not wait for it.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_lane_preamble_peek_timeout_ms) = 60000;
  auto server = StartLanedServer();
  const auto endpoint = server.bound_endpoint();
  PreambleCounts expected{.bound = 0, .absent = 0, .peek_timeouts = 0, .stripped_late = 0};
  ASSERT_EQ(Counts(), expected);

  // Preamble split over three reads.
  {
    auto socket = ASSERT_RESULT(RawConnect(endpoint));
    for (char c : LanePreamble(1)) {
      ASSERT_OK(RawSend(&socket, std::string_view(&c, 1)));
      std::this_thread::sleep_for(20ms);
    }
    ++expected.bound;
    ASSERT_OK(WaitCounts(expected));
    ASSERT_OK(LoggedWaitFor([&server]() -> Result<bool> {
      return VERIFY_RESULT(ServerConnectionsByLane(server.messenger()))[1] == 1;
    }, 10s, "Connection on lane 1's reactor"));
    ASSERT_EQ(ASSERT_RESULT(ServerConnectionsByLane(server.messenger()))[0], 0);
  }
  ASSERT_OK(LoggedWaitFor([&server]() -> Result<bool> {
    return VERIFY_RESULT(ServerConnections(server.messenger())) == 0;
  }, 10s, "Connection closed"));

  // Decided at the first byte, at the second, and on EOF.
  for (std::string_view first_bytes : {"\x16"sv, "YB"sv, ""sv}) {
    SCOPED_TRACE(Slice(first_bytes).ToDebugHexString());
    auto socket = ASSERT_RESULT(RawConnect(endpoint));
    if (first_bytes.empty()) {
      ASSERT_OK(socket.Close());
    } else {
      ASSERT_OK(RawSend(&socket, first_bytes));
    }
    ++expected.absent;
    ASSERT_OK(WaitCounts(expected));
  }

  // Part of a preamble when the timeout passes: the rest is stripped by the reactor.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_lane_preamble_peek_timeout_ms) = 100;
  auto socket = ASSERT_RESULT(RawConnect(endpoint));
  ASSERT_OK(RawSend(&socket, "Y"));
  ++expected.peek_timeouts;
  ASSERT_OK(WaitCounts(expected));
  ASSERT_OK(RawSend(&socket, LanePreamble(1).substr(1)));
  ++expected.stripped_late;
  ASSERT_OK(WaitCounts(expected));
  server.Shutdown();
}

// Past the cap on held sockets an undecided socket is passed on at once, not bound; shutdown closes
// held sockets without passing them on.
TEST_F(RpcLanePreambleTest, HeldSocketsCapAndShutdown) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_lane_preamble_peek_timeout_ms) = 60000;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_rpc_lane_preamble_max_pending_sockets) = 0;
  auto server = StartLanedServer();
  auto first = ASSERT_RESULT(RawConnect(server.bound_endpoint()));
  PreambleCounts expected{.bound = 0, .absent = 0, .peek_timeouts = 1, .stripped_late = 0};
  ASSERT_OK(WaitCounts(expected));
  ASSERT_OK(LoggedWaitFor([&server]() -> Result<bool> {
    return VERIFY_RESULT(ServerConnections(server.messenger())) == 1;
  }, 10s, "Connection registered"));

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_rpc_lane_preamble_max_pending_sockets) = 1;
  auto held = ASSERT_RESULT(RawConnect(server.bound_endpoint()));
  std::this_thread::sleep_for(200ms);
  ASSERT_EQ(Counts(), expected);
  ASSERT_EQ(ASSERT_RESULT(ServerConnections(server.messenger())), 1);

  server.messenger()->ShutdownAcceptor();
  uint8_t byte;
  ASSERT_OK(held.SetRecvTimeout(10s));
  auto received = held.Recv(&byte, 1);
  ASSERT_NOK(received);
  ASSERT_EQ(Errno(received.status()), ESHUTDOWN) << received.status();
  ASSERT_EQ(Counts(), expected);
  ASSERT_EQ(ASSERT_RESULT(ServerConnections(server.messenger())), 1);
  server.Shutdown();
}

// An unlaned YB RPC receiver, as a lanes=1 tablet server's, does not peek but drops a preamble on
// the reactor; with no preamble received its metrics are those of a server without lanes.
TEST_F(RpcLanePreambleTest, UnlanedReceiverStripsWithoutPeeking) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_lane_preamble_peek_timeout_ms) = 60000;
  TestServerOptions options;
  options.messenger_options = kLanePreambleServerOptions;
  auto server = StartTestServer(options);
  const PreambleCounts none{.bound = -1, .absent = -1, .peek_timeouts = -1, .stripped_late = -1};

  {
    auto silent = ASSERT_RESULT(RawConnect(server.bound_endpoint()));
    ASSERT_OK(LoggedWaitFor([&server]() -> Result<bool> {
      return VERIFY_RESULT(ServerConnections(server.messenger())) == 1;
    }, 10s, "Silent connection registered without a peek"));
  }
  auto unlaned = UnlanedClient();
  Proxy unlaned_proxy(unlaned.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
  ASSERT_OK(WhoAmIWithTimeout(&unlaned_proxy, 1));
  ASSERT_EQ(Counts(), none);

  auto laned = LanedClient();
  Proxy laned_proxy(laned.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
  ASSERT_OK(WhoAmIWithTimeout(&laned_proxy, 1));
  ASSERT_OK(WhoAmIWithTimeout(&laned_proxy, 0));
  ASSERT_EQ(Counts(), (PreambleCounts{
      .bound = -1, .absent = -1, .peek_timeouts = -1, .stripped_late = 2}));
  server.Shutdown();
}

// A messenger whose inbound protocol is not YB RPC, as CQL's and Redis's, neither peeks nor strips:
// its connection context receives the preamble.
TEST_F(RpcLanePreambleTest, OtherProtocolsSeePreamble) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto client = LanedClient();
  for (bool laned_server : {false, true}) {
    SCOPED_TRACE(Format("laned server: $0", laned_server));
    auto builder = laned_server ? LanedBuilder("Server", *topology_, kLanePreambleServerOptions)
                                : CreateMessengerBuilder("Server", kLanePreambleServerOptions);
    auto server = rpc::CreateAutoShutdownMessengerHolder(ASSERT_RESULT(builder.Build()));
    Endpoint endpoint;
    ASSERT_OK(server->ListenAddress(
        CreateConnectionContextFactory<RecordingConnectionContext>(), Endpoint(), &endpoint));
    ASSERT_OK(server->StartAcceptor());
    const auto received_before = RecordingConnectionContext::Received().size();
    Proxy proxy(client.get(), HostPort::FromBoundEndpoint(endpoint));
    ASSERT_NOK(WhoAmIWithTimeout(&proxy, 1, nullptr, 500ms));
    auto received = RecordingConnectionContext::Received().substr(received_before);
    ASSERT_TRUE(received.starts_with(LanePreamble(1) + "YB\x01"))
        << Slice(received).ToDebugString();
  }
  ASSERT_EQ(Counts(), (PreambleCounts{
      .bound = -1, .absent = -1, .peek_timeouts = -1, .stripped_late = -1}));
}

// Turning the AutoFlag off stops preambles on new connections while existing bound ones keep
// serving calls; turning it on again leaves connections created meanwhile unbound.
TEST_F(RpcLanePreambleTest, RuntimeDemotion) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartLanedServer();
  auto client = LanedClient();
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
  rpc_test::WhoAmIResponsePB resp;
  ASSERT_OK(WhoAmIWithTimeout(&proxy, 1, &resp));
  ASSERT_EQ(resp.handler_lane(), 1);
  PreambleCounts expected{.bound = 1, .absent = 0, .peek_timeouts = 0, .stripped_late = 0};
  ASSERT_EQ(Counts(), expected);

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_rpc_lane_preamble) = false;
  ASSERT_OK(WhoAmIWithTimeout(&proxy, 0));
  ++expected.absent;
  ASSERT_EQ(Counts(), expected);
  for (int i = 0; i != 3; ++i) {
    ASSERT_OK(WhoAmIWithTimeout(&proxy, 1, &resp));
    ASSERT_EQ(resp.handler_lane(), 1);
  }

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_rpc_lane_preamble) = true;
  ASSERT_OK(WhoAmIWithTimeout(&proxy, 0));
  ASSERT_EQ(Counts(), expected);
  ASSERT_EQ(ASSERT_RESULT(ServerConnections(server.messenger())), 2);
  server.Shutdown();
}

// The preamble stays first when calls are queued before the connect completes, including one that
// expires there, and a connect that fails tears down a stream with a queued preamble.
TEST_F(RpcLanePreambleTest, QueuedBeforeConnect) {
  SKIP_WITHOUT_TOPOLOGY();
  TEST_SetLaneTopology scope(*topology_);
  auto server = StartLanedServer();
  auto client = LanedClient();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_delay_connect_ms) = 2000;
  Proxy proxy(client.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));
  auto status = WhoAmIWithTimeout(&proxy, 1, nullptr, 100ms);
  ASSERT_TRUE(status.IsTimedOut()) << status;
  rpc_test::WhoAmIResponsePB resp;
  ASSERT_OK(WhoAmIWithTimeout(&proxy, 1, &resp));
  ASSERT_EQ(resp.handler_lane(), 1);
  ASSERT_EQ(Counts(), (PreambleCounts{
      .bound = 1, .absent = 0, .peek_timeouts = 0, .stripped_late = 0}));

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_delay_connect_ms) = 0;
  // Bound but not listening, so a connect to it is refused.
  Socket closed;
  ASSERT_OK(closed.Init(0));
  ASSERT_OK(closed.Bind(Endpoint(IpAddress::from_string("127.0.0.1"), 0)));
  Endpoint closed_endpoint;
  ASSERT_OK(closed.GetSocketAddress(&closed_endpoint));
  Proxy refused(client.get(), HostPort::FromBoundEndpoint(closed_endpoint));
  for (int i = 0; i != 3; ++i) {
    ASSERT_NOK(WhoAmIWithTimeout(&refused, 1, nullptr, 5s));
  }
  ASSERT_OK(WhoAmIWithTimeout(&proxy, 1));
  server.Shutdown();
}

// The flags and metrics operators see: names, defaults, AutoFlag class and values, tags and help.
TEST_F(RpcLanePreambleTest, FlagAndMetricFormats) {
  auto tags_of = [](const std::string& flag) {
    std::unordered_set<FlagTag> tags;
    GetFlagTags(flag, &tags);
    return tags;
  };
  gflags::CommandLineFlagInfo info;
  ASSERT_TRUE(gflags::GetCommandLineFlagInfo("rpc_lane_preamble_peek_timeout_ms", &info));
  ASSERT_EQ(info.type, "uint32");
  ASSERT_EQ(info.default_value, "500");
  ASSERT_EQ(info.description,
            "How long the acceptor of a laned messenger waits for the first bytes of an accepted "
            "connection to show whether it starts with a lane preamble. A connection that has not "
            "decided by then is assigned to a reactor as without lanes, and is not bound to a lane "
            "for its lifetime.");
  auto tags = tags_of("rpc_lane_preamble_peek_timeout_ms");
  ASSERT_TRUE(tags.contains(FlagTag::kAdvanced));
  ASSERT_TRUE(tags.contains(FlagTag::kRuntime));

  const auto* auto_flag = GetAutoFlagDescription("enable_rpc_lane_preamble");
  ASSERT_NE(auto_flag, nullptr);
  ASSERT_EQ(auto_flag->flag_class, AutoFlagClass::kLocalVolatile);
  ASSERT_EQ(auto_flag->initial_val, "false");
  ASSERT_EQ(auto_flag->target_val, "true");
  ASSERT_TRUE(gflags::GetCommandLineFlagInfo("enable_rpc_lane_preamble", &info));
  ASSERT_EQ(info.type, "bool");
  ASSERT_EQ(info.description,
            "Laned tablet servers write a lane preamble at the start of each connection they key "
            "by lane, so that a receiving tablet server with lanes on can bind it to a reactor of "
            "that lane. Receivers that predate the preamble reject such connections, so enable it "
            "only after every process in the universe runs a version that accepts it. Read when a "
            "connection is created: a change affects only connections created afterwards.");
  tags = tags_of("enable_rpc_lane_preamble");
  ASSERT_TRUE(tags.contains(FlagTag::kAdvanced));
  ASSERT_TRUE(tags.contains(FlagTag::kRuntime));
  ASSERT_TRUE(tags.contains(FlagTag::kAuto));

  const std::vector<std::pair<const CounterPrototype*, std::string>> counters = {
    {&METRIC_rpc_lane_preamble_bound,
     "Number of inbound connections that the acceptor of a laned messenger bound to a reactor of "
     "the lane of their lane preamble."},
    {&METRIC_rpc_lane_preamble_absent,
     "Number of inbound connections of a laned messenger whose first bytes were not a lane "
     "preamble, that closed or failed before sending one, or whose preamble the acceptor could "
     "not read in full. They are assigned to a reactor as without lanes."},
    {&METRIC_rpc_lane_preamble_peek_timeouts,
     "Number of inbound connections of a laned messenger whose first bytes had not shown whether "
     "they start with a lane preamble within --rpc_lane_preamble_peek_timeout_ms, or that could "
     "not be held because the acceptor held the maximum number of such connections. They are "
     "assigned to a reactor as without lanes."},
    {&METRIC_rpc_lane_preamble_stripped_late,
     "Number of inbound connections whose lane preamble was dropped by the reactor because the "
     "acceptor did not consume it: the messenger does not peek, or the acceptor gave up before the "
     "preamble arrived (peek timeout, or too many connections already held). Such connections are "
     "not bound to the preamble's lane."},
  };
  std::vector<std::string> names;
  for (const auto& [prototype, description] : counters) {
    names.push_back(prototype->name());
    ASSERT_EQ(prototype->description(), description) << prototype->name();
    ASSERT_EQ(prototype->unit(), MetricUnit::kConnections) << prototype->name();
  }
  ASSERT_EQ(names, (std::vector<std::string>{
      "rpc_lane_preamble_bound", "rpc_lane_preamble_absent", "rpc_lane_preamble_peek_timeouts",
      "rpc_lane_preamble_stripped_late"}));
}

} // namespace yb::rpc
