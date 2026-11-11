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

#include <thread>

#include "yb/rpc/proxy.h"
#include "yb/rpc/reactor.h"
#include "yb/rpc/rpc-test-base.h"
#include "yb/rpc/rpc_controller.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/logging.h"
#include "yb/util/scope_exit.h"
#include "yb/util/test_macros.h"
#include "yb/util/thread.h"

using namespace std::literals;
using namespace std::placeholders;

DECLARE_int32(TEST_rpc_reactor_index_for_init_failure_simulation);
DECLARE_int32(rpc_reactor_task_timeout_ms);

namespace yb::rpc {

MessengerOptions MakeMessengerOptions() {
  auto result = kDefaultClientMessengerOptions;
  result.n_reactors = 4;
  return result;
}

class ReactorTest : public RpcTestBase {
 public:
  ReactorTest() {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_reactor_task_timeout_ms) = 500;

    messenger_.reset(CreateMessenger("my_messenger", MakeMessengerOptions()).release());
  }

  void ScheduledTask(const Status& status, const Status& expected_status) {
    ASSERT_EQ(expected_status.CodeAsString(), status.CodeAsString());
    latch_.CountDown();
  }

  void ScheduledTaskCheckThread(const Status& status, const Thread* thread) {
    ASSERT_OK(status);
    ASSERT_EQ(thread, Thread::current_thread());
    latch_.CountDown();
  }

  void ScheduledTaskScheduleAgain(const Status& status) {
    auto expected_task_id = messenger_->TEST_next_task_id();
    auto task_id = ASSERT_RESULT(messenger_->ScheduleOnReactor(
        std::bind(&ReactorTest::ScheduledTaskCheckThread, this, _1, Thread::current_thread()),
        0s, SOURCE_LOCATION()));
    ASSERT_EQ(expected_task_id, task_id);
    latch_.CountDown();
  }

 protected:
  AutoShutdownMessengerHolder messenger_;
  CountDownLatch latch_{1};
};

TEST_F_EX(ReactorTest, MessengerInitFailure, YBTest) {
  MessengerBuilder builder("test-msgr");
  // Test Reactor::Init failure in very first Messenger's reactor
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_rpc_reactor_index_for_init_failure_simulation) = 0;
  ASSERT_NOK(builder.Build());
  // Test Reactor::Init failure in second Messenger's reactor
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_rpc_reactor_index_for_init_failure_simulation) = 1;
  ASSERT_NOK(builder.Build());
}

TEST_F(ReactorTest, TestFunctionIsCalled) {
  auto expected_task_id = messenger_->TEST_next_task_id();
  auto task_id = ASSERT_RESULT(messenger_->ScheduleOnReactor(
      std::bind(&ReactorTest::ScheduledTask, this, _1, Status::OK()), 0s,
      SOURCE_LOCATION()));
  ASSERT_EQ(expected_task_id, task_id);
  latch_.Wait();
}

TEST_F(ReactorTest, TestFunctionIsCalledAtTheRightTime) {
  MonoTime before = MonoTime::Now();
  auto expected_task_id = messenger_->TEST_next_task_id();
  auto task_id = ASSERT_RESULT(messenger_->ScheduleOnReactor(
      std::bind(&ReactorTest::ScheduledTask, this, _1, Status::OK()),
      100ms, SOURCE_LOCATION()));
  ASSERT_EQ(expected_task_id, task_id);
  latch_.Wait();
  MonoTime after = MonoTime::Now();
  MonoDelta delta = after.GetDeltaSince(before);
  CHECK_GE(delta.ToMilliseconds(), 100);
}

TEST_F(ReactorTest, TestFunctionIsCalledIfReactorShutdown) {
  auto expected_task_id = messenger_->TEST_next_task_id();
  auto task_id = ASSERT_RESULT(messenger_->ScheduleOnReactor(
      std::bind(&ReactorTest::ScheduledTask, this, _1, STATUS(Aborted, "doesn't matter")),
      60s, SOURCE_LOCATION()));
  ASSERT_EQ(expected_task_id, task_id);
  messenger_->Shutdown();
  latch_.Wait();
}

TEST_F(ReactorTest, TestReschedulesOnSameReactorThread) {
  // Our scheduled task will schedule yet another task.
  latch_.Reset(2);

  auto expected_task_id = messenger_->TEST_next_task_id();
  auto task_id = ASSERT_RESULT(messenger_->ScheduleOnReactor(
      std::bind(&ReactorTest::ScheduledTaskScheduleAgain, this, _1), 0s,
      SOURCE_LOCATION()));
  ASSERT_EQ(expected_task_id, task_id);
  latch_.Wait();
  latch_.Wait();
}

namespace {

class MonitorTestLogger : public google::base::Logger {
 public:
  MonitorTestLogger() : impl_(google::base::GetLogger(google::GLOG_WARNING)) {
  }

  size_t num_writes() const {
    return num_writes_.load();
  }
 private:
  void Write(bool force_flush, time_t timestamp, const char* message, int message_len) override {
    ++num_writes_;
    impl_->Write(force_flush, timestamp, message, message_len);
  }

  void Flush() override {
    impl_->Flush();
  }

  uint32 LogSize() override {
    return impl_->LogSize();
  }
 private:
  std::atomic<size_t> num_writes_{0};
  google::base::Logger* impl_;
};

} // namespace

TEST_F(ReactorTest, Monitor) {
  latch_.Reset(2);
  auto* logger = new MonitorTestLogger;
  google::base::SetLogger(google::GLOG_WARNING, logger);
  ASSERT_RESULT(messenger_->ScheduleOnReactor([this](const Status& status) {
    CHECK_OK(status);
    latch_.CountDown();
  }, 0s, SOURCE_LOCATION()));
  ASSERT_RESULT(messenger_->ScheduleOnReactor([this, logger](const Status& status) {
    CHECK_OK(status);
    while (!logger->num_writes()) {
      std::this_thread::sleep_for(100ms);
    }
    latch_.CountDown();
  }, 0s, SOURCE_LOCATION()));
  latch_.Wait();
  ASSERT_EQ(logger->num_writes(), 1);
}

namespace {

struct ReactorView {
  CpuSet mask;
  LaneId lane = kNoLane;
};

// Runs a task on reactor, scheduled from a new thread set up by confine_submitter, and returns
// what the reactor thread saw.
ReactorView RunOnReactorFrom(Reactor* reactor, const std::function<void()>& confine_submitter) {
  ReactorView result;
  RunOnStdThread([reactor, &confine_submitter, &result] {
    confine_submitter();
    CountDownLatch latch(1);
    CHECK_OK(reactor->ScheduleReactorFunctor([&result, &latch](Reactor*) {
      result = ReactorView { .mask = ThisThreadMask(), .lane = ThisThreadLane() };
      latch.CountDown();
    }, SOURCE_LOCATION()));
    latch.Wait();
  });
  return result;
}

auto InLane(LaneId lane) {
  return [lane] { ApplyLaneToCurrentThread(lane); };
}

} // namespace

TEST_F_EX(ReactorTest, LanedReactors, YBTest) {
  auto topology = MakeTwoLaneTestTopology();
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);
  MessengerBuilder builder("laned");
  // 5 is reduced modulo the 2 lanes.
  builder.set_num_reactors(3).set_reactor_lanes({0, 5, kNoLane});
  auto messenger = ASSERT_RESULT(builder.Build());
  auto se = ScopeExit([&messenger] { messenger->Shutdown(); });
  std::vector<LaneId> expected_lanes = {0, 1, kNoLane};
  for (size_t i = 0; i != expected_lanes.size(); ++i) {
    auto* reactor = messenger->TEST_reactor(i);
    ASSERT_EQ(reactor->lane(), expected_lanes[i]);
    auto view = RunOnReactorFrom(reactor, [] {});
    ASSERT_EQ(view.lane, expected_lanes[i]);
    ASSERT_EQ(view.mask, expected_lanes[i] == kNoLane ? topology->service_mask()
                                                      : topology->lane_mask(expected_lanes[i]));
  }

  // Tasks scheduled from another lane count as reactor crossings, by the submitting thread.
  auto* lane0_reactor = messenger->TEST_reactor(0);
  auto counts = LaneHandoffCounts::Now();
  RunOnReactorFrom(lane0_reactor, InLane(0));
  auto delta = LaneHandoffCounts::Now() - counts;
  ASSERT_EQ(delta.total_crossings(), 0) << delta.ToString();
  RunOnReactorFrom(lane0_reactor, InLane(1));
  delta = LaneHandoffCounts::Now() - counts;
  ASSERT_EQ(delta.crossing(LaneCrossingCategory::kReactor), 1) << delta.ToString();
  ASSERT_EQ(delta.total_crossings(), 1);
  // Unlaned submitters count nothing, so that they never write the counter slot they all share.
  auto unlaned_before = delta.entries_from_unlaned;
  RunOnReactorFrom(lane0_reactor, [] { ApplyServiceMaskToCurrentThread(); });
  delta = LaneHandoffCounts::Now() - counts;
  ASSERT_EQ(delta.total_crossings(), 1) << delta.ToString();
  ASSERT_EQ(delta.entries_from_unlaned, unlaned_before);
  // An unlaned reactor counts nothing.
  RunOnReactorFrom(messenger->TEST_reactor(2), InLane(1));
  delta = LaneHandoffCounts::Now() - counts;
  ASSERT_EQ(delta.total_crossings(), 1) << delta.ToString();
}

TEST_F_EX(ReactorTest, ReactorLanesMustMatchReactorCount, YBTest) {
  MessengerBuilder builder("laned");
  builder.set_num_reactors(3).set_reactor_lanes({0, 1});
  auto result = builder.Build();
  ASSERT_NOK(result);
  ASSERT_TRUE(result.status().IsInvalidArgument()) << result.status();
  ASSERT_STR_CONTAINS(result.status().ToString(), "2 reactor lanes given for 3 reactors");
}

// Reactor lanes are ignored while lane mode is off: no lane, no mask change.
TEST_F_EX(ReactorTest, ReactorLanesWithLanesOff, YBTest) {
  TEST_SetLaneTopology scope{LaneTopology()};
  const auto mask = ThisThreadMask();
  const auto calls = TEST_CpuAffinityCallCount();
  MessengerBuilder builder("laned");
  builder.set_num_reactors(2).set_reactor_lanes({0, 1});
  auto messenger = ASSERT_RESULT(builder.Build());
  auto se = ScopeExit([&messenger] { messenger->Shutdown(); });
  for (size_t i = 0; i != 2; ++i) {
    auto* reactor = messenger->TEST_reactor(i);
    ASSERT_EQ(reactor->lane(), kNoLane);
    auto view = RunOnReactorFrom(reactor, [] {});
    ASSERT_EQ(view.lane, kNoLane);
    ASSERT_EQ(view.mask, mask);
  }
  ASSERT_EQ(TEST_CpuAffinityCallCount(), calls);
}

// Outbound calls and responses go on queues that a reactor drains in batches; each one counts as a
// handoff. Calls are sent from a lane 0 thread through a lane 1 client reactor to a server whose
// reactor is in lane 0 and whose handlers are in lane 1, so per call the outbound call and the
// response are reactor crossings and the inbound call is a handler pool crossing.
TEST_F_EX(ReactorTest, OutboundItemsCountedEach, RpcTestBase) {
  auto topology = MakeTwoLaneTestTopology();
  if (!topology) {
    GTEST_SKIP() << "Needs at least 2 CPUs";
  }
  TEST_SetLaneTopology scope(*topology);
  constexpr int kCalls = 5;

  auto server_builder = CreateMessengerBuilder("Server", kDefaultClientMessengerOptions);
  server_builder.set_reactor_lanes({0});
  TestServerOptions server_options;
  server_options.n_worker_threads = kCalls;
  server_options.handler_lane = 1;
  auto server = StartTestServer(server_options, "", ASSERT_RESULT(server_builder.Build()));
  auto* server_reactor = server.messenger()->TEST_reactor(0);
  ASSERT_EQ(server_reactor->lane(), 0);

  // One connection, so that the responses share one queue on the server.
  auto client_options = kDefaultClientMessengerOptions;
  client_options.num_connections_to_server = 1;
  auto client_builder = CreateMessengerBuilder("Client", client_options);
  client_builder.set_reactor_lanes({1});
  auto client_messenger =
      rpc::CreateAutoShutdownMessengerHolder(ASSERT_RESULT(client_builder.Build()));
  Proxy proxy(client_messenger.get(), HostPort::FromBoundEndpoint(server.bound_endpoint()));

  rpc_test::SleepRequestPB req;
  // Long enough for the handlers to respond only after the server reactor is blocked below.
  req.set_sleep_micros(1'000'000);
  // Sets up the connection outside the counted window.
  RunOnStdThread([&proxy, &req] {
    ApplyLaneToCurrentThread(0);
    rpc_test::SleepResponsePB resp;
    RpcController controller;
    ASSERT_OK(proxy.SyncRequest(
        CalculatorServiceMethods::SleepMethod(), /* method_metrics= */ nullptr, req, &resp,
        &controller));
  });

  const auto counts = LaneHandoffCounts::Now();
  auto delta = [&counts] { return LaneHandoffCounts::Now() - counts; };
  std::vector<rpc_test::SleepResponsePB> responses(kCalls);
  std::vector<RpcController> controllers(kCalls);
  CountDownLatch done(kCalls);
  RunOnStdThread([&] {
    ApplyLaneToCurrentThread(0);
    for (int i = 0; i != kCalls; ++i) {
      proxy.AsyncRequest(
          CalculatorServiceMethods::SleepMethod(), /* method_metrics= */ nullptr, req,
          &responses[i], &controllers[i], [&done] { done.CountDown(); });
    }
  });
  ASSERT_OK(WaitFor(
      [&delta] { return delta().crossing(LaneCrossingCategory::kOther) == kCalls; },
      MonoDelta::FromSeconds(30), "All calls handed to the handlers"));

  // Hold the server reactor while the handlers respond, so that the responses queue up behind one
  // drain task.
  std::atomic<bool> blocked{false}, release{false};
  ASSERT_OK(server_reactor->ScheduleReactorFunctor([&blocked, &release](Reactor*) {
    blocked = true;
    while (!release) {
      std::this_thread::sleep_for(1ms);
    }
  }, SOURCE_LOCATION()));
  ASSERT_OK(WaitFor([&blocked] { return blocked.load(); }, MonoDelta::FromSeconds(30),
                    "Server reactor blocked"));
  auto status = WaitFor(
      [&delta] { return delta().crossing(LaneCrossingCategory::kReactor) == 2 * kCalls; },
      MonoDelta::FromSeconds(15), "Responses handed to the server reactor");
  release = true;
  ASSERT_TRUE(status.ok()) << status << ": " << delta().ToString();
  done.Wait();
  for (auto& controller : controllers) {
    ASSERT_OK(controller.status());
  }
  auto final_delta = delta();
  ASSERT_EQ(final_delta.crossing(LaneCrossingCategory::kReactor), 2 * kCalls)
      << final_delta.ToString();
  ASSERT_EQ(final_delta.crossing(LaneCrossingCategory::kOther), kCalls);
  ASSERT_EQ(final_delta.total_crossings(), 3 * kCalls);
  server.Shutdown();
}

} // namespace yb::rpc
