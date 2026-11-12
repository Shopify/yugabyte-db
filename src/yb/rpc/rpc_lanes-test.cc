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

// Laned messengers: reactor groups, per-lane handler pools, lane-keyed connections and the
// callback and local-call lane rules.

#ifdef __linux__
#include <sys/prctl.h>
#endif

#include <atomic>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "yb/rpc/proxy.h"
#include "yb/rpc/reactor.h"
#include "yb/rpc/rpc.h"
#include "yb/rpc/rpc-test-base.h"
#include "yb/rpc/rpc_controller.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/flags.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/logging_test_util.h"
#include "yb/util/scope_exit.h"
#include "yb/util/test_macros.h"
#include "yb/util/thread.h"

using namespace std::literals;

DECLARE_uint64(default_idle_timeout_ms);

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
// normal and high-priority services.
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

} // namespace yb::rpc
