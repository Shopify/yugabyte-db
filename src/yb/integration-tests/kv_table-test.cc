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

#include <atomic>
#include <cmath>
#include <cstdlib>
#include <future>

#include "yb/util/flags.h"
#include "yb/util/logging.h"

#include "yb/client/client.h"
#include "yb/client/table.h"
#include "yb/client/tablet_server.h"

#include "yb/gutil/strings/split.h"
#include "yb/gutil/strings/substitute.h"

#include "yb/integration-tests/cluster_verifier.h"
#include "yb/integration-tests/load_generator.h"
#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/yb_table_test_base.h"

#include "yb/master/master.h"
#include "yb/master/mini_master.h"

#include "yb/rpc/messenger.h"
#include "yb/rpc/rpc_priority_queue.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/size_literals.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"

using namespace std::literals;

using std::string;
using std::vector;

DECLARE_int32(log_cache_size_limit_mb);
DECLARE_int32(global_log_cache_size_limit_mb);
DECLARE_bool(rpc_priority_queue_enabled);
DECLARE_int32(rpc_priority_queue_max_dispatched);
DECLARE_int32(rpc_priority_queue_callback_reserve);
DECLARE_int32(rpc_workers_limit);

METRIC_DECLARE_counter(rpc_priority_queue_dispatched_high);
METRIC_DECLARE_counter(rpc_priority_queue_dispatched_normal);
METRIC_DECLARE_counter(rpc_priority_queue_dispatched_low);
METRIC_DECLARE_counter(rpc_priority_queue_dispatched_callbacks);
METRIC_DECLARE_event_stats(rpc_priority_queue_wait_time_high);
METRIC_DECLARE_event_stats(rpc_priority_queue_wait_time_normal);
METRIC_DECLARE_event_stats(rpc_priority_queue_wait_time_low);

namespace yb {
namespace integration_tests {

class KVTableTest : public YBTableTestBase {
 protected:

  bool use_external_mini_cluster() override { return false; }

 protected:

  void PutSampleKeysValues() {
    PutKeyValue("key123", "value123");
    PutKeyValue("key200", "value200");
    PutKeyValue("key300", "value300");
  }

  void CheckSampleKeysValues() {
    auto result_kvs = GetScanResults(client::TableRange(table_));

    ASSERT_EQ(3, result_kvs.size());
    ASSERT_EQ("key123", result_kvs.front().first);
    ASSERT_EQ("value123", result_kvs.front().second);
    ASSERT_EQ("key200", result_kvs[1].first);
    ASSERT_EQ("value200", result_kvs[1].second);
    ASSERT_EQ("key300", result_kvs[2].first);
    ASSERT_EQ("value300", result_kvs[2].second);
  }

  // Test bodies shared with the fixtures below that change cluster configuration.
  void RunLoadTest();
  void RunRestartTest();
};

// Runs the cluster with rpc_priority_queue_enabled and a deliberately tiny dispatch budget, so that
// the queue is saturated and consensus, heartbeats and user traffic all contend for dispatch
// permits. Verifies that the cluster stays healthy (no write/read errors, consistent row counts,
// restart recovery) when everything flows through the priority queue, and that the queue was in
// fact exercised (see VerifyPriorityQueueUsage).
class KVTablePriorityQueueTest : public KVTableTest {
 protected:
  // RPC handlers are mostly asynchronous and release their worker quickly, so with a budget equal
  // to the worker count a handful of client threads never saturate it. A budget of 2 per server
  // guarantees contention under the load test while leaving the pools their full worker count.
  // With one permit reserved for callbacks, at most one inbound handler runs at a time on each
  // server, which is the harshest possible setting for the deadlock-freedom argument (handlers
  // blocking on callbacks) while still making progress.
  static constexpr int kRpcWorkersLimit = 8;
  static constexpr int kDispatchBudget = 2;
  static constexpr int kCallbackReserve = 1;
  static constexpr int kInboundCap = kDispatchBudget - kCallbackReserve;

  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_priority_queue_enabled) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_priority_queue_max_dispatched) = kDispatchBudget;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_priority_queue_callback_reserve) = kCallbackReserve;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_workers_limit) = kRpcWorkersLimit;
    KVTableTest::SetUp();
  }

  struct QueueUsage {
    int64_t dispatched_high = 0;
    int64_t dispatched_normal = 0;
    int64_t dispatched_low = 0;
    int64_t dispatched_callbacks = 0;
    uint64_t waits = 0;

    void Add(const scoped_refptr<MetricEntity>& entity) {
      dispatched_high += METRIC_rpc_priority_queue_dispatched_high.Instantiate(entity)->value();
      dispatched_normal +=
          METRIC_rpc_priority_queue_dispatched_normal.Instantiate(entity)->value();
      dispatched_low += METRIC_rpc_priority_queue_dispatched_low.Instantiate(entity)->value();
      dispatched_callbacks +=
          METRIC_rpc_priority_queue_dispatched_callbacks.Instantiate(entity)->value();
      waits += METRIC_rpc_priority_queue_wait_time_high.Instantiate(entity)->TotalCount();
      waits += METRIC_rpc_priority_queue_wait_time_normal.Instantiate(entity)->TotalCount();
      waits += METRIC_rpc_priority_queue_wait_time_low.Instantiate(entity)->TotalCount();
    }

    std::string ToString() const {
      return YB_STRUCT_TO_STRING(
          dispatched_high, dispatched_normal, dispatched_low, dispatched_callbacks, waits);
    }
  };

  struct ExpectedUsage {
    // At least one task cluster-wide had to wait for a permit, i.e. the budget was saturated.
    bool contention = false;
    // kLow work reached the tservers. Only CreateTablet (TabletServerAdminService) is kLow in this
    // test, so this holds after table creation but not for servers restarted afterwards.
    bool tserver_low = false;
  };

  // Asserts that every master and tserver messenger is actually gated by a queue of the configured
  // budget, and that RPC work of the expected priorities was dispatched through those queues.
  void VerifyPriorityQueueUsage(ExpectedUsage expected) {
    QueueUsage masters;
    for (size_t i = 0; i < mini_cluster()->num_masters(); ++i) {
      auto* master = mini_cluster()->mini_master(i)->master();
      auto* queue = master->messenger()->rpc_priority_queue();
      ASSERT_NE(queue, nullptr) << "master " << i << " messenger has no priority queue";
      ASSERT_EQ(queue->max_dispatched(), kDispatchBudget);
      ASSERT_EQ(queue->max_dispatched_inbound(), kInboundCap);
      masters.Add(master->metric_entity());
    }
    QueueUsage tservers;
    for (size_t i = 0; i < mini_cluster()->num_tablet_servers(); ++i) {
      auto* tserver = mini_cluster()->mini_tablet_server(i)->server();
      auto* queue = tserver->messenger()->rpc_priority_queue();
      ASSERT_NE(queue, nullptr) << "tserver " << i << " messenger has no priority queue";
      ASSERT_EQ(queue->max_dispatched(), kDispatchBudget);
      ASSERT_EQ(queue->max_dispatched_inbound(), kInboundCap);
      tservers.Add(tserver->metric_entity());
    }
    LOG(INFO) << "Priority queue usage: masters " << masters.ToString()
              << ", tservers " << tservers.ToString();

    // Masters: tserver heartbeats (kHigh) and client/DDL traffic (kNormal), plus the callbacks
    // of the master's own outbound RPCs (e.g. to tservers during table creation).
    ASSERT_GT(masters.dispatched_high, 0);
    ASSERT_GT(masters.dispatched_normal, 0);
    ASSERT_GT(masters.dispatched_callbacks, 0);
    // Tservers: consensus (kHigh), reads/writes (kNormal), and the callbacks of consensus and
    // client RPCs they issue themselves.
    ASSERT_GT(tservers.dispatched_high, 0);
    ASSERT_GT(tservers.dispatched_normal, 0);
    ASSERT_GT(tservers.dispatched_callbacks, 0);
    if (expected.tserver_low) {
      ASSERT_GT(tservers.dispatched_low, 0);
    }
    if (expected.contention) {
      ASSERT_GT(masters.waits + tservers.waits, 0)
          << "no RPC task ever waited for a permit; the budget was never saturated";
    }
  }
};

TEST_F(KVTableTest, SimpleKVTableTest) {
  ASSERT_NO_FATALS(PutSampleKeysValues());
  ASSERT_NO_FATALS(CheckSampleKeysValues());
  ClusterVerifier cluster_verifier(mini_cluster());
  ASSERT_NO_FATALS(cluster_verifier.CheckCluster());
  ASSERT_NO_FATALS(cluster_verifier.CheckRowCount(table_->name(), ClusterVerifier::EXACTLY, 3));
}

TEST_F(KVTableTest, PointQuery) {
  ASSERT_NO_FATALS(PutSampleKeysValues());

  client::TableIteratorOptions options;
  options.filter = client::FilterEqual("key200"s, "k"s);
  auto result_kvs = GetScanResults(client::TableRange(table_, options));
  ASSERT_EQ(1, result_kvs.size());
  ASSERT_EQ("key200", result_kvs.front().first);
  ASSERT_EQ("value200", result_kvs.front().second);
}

TEST_F(KVTableTest, Eng135MetricsTest) {
  ClusterVerifier cluster_verifier(mini_cluster());
  for (int idx = 0; idx < 10; ++idx) {
    ASSERT_NO_FATALS(PutSampleKeysValues());
    ASSERT_NO_FATALS(CheckSampleKeysValues());
    ASSERT_NO_FATALS(DeleteTable());
    ASSERT_NO_FATALS(CreateTable());
    ASSERT_NO_FATALS(OpenTable());
    ASSERT_NO_FATALS(cluster_verifier.CheckCluster());
    ASSERT_NO_FATALS(cluster_verifier.CheckRowCount(table_->name(), ClusterVerifier::EXACTLY, 0));
  }
}

void KVTableTest::RunLoadTest() {
  std::atomic_bool stop_requested_flag(false);
  int rows = 5000;
  int start_key = 0;
  int writer_threads = 4;
  int reader_threads = 4;
  int value_size_bytes = 16;
  int max_write_errors = 0;
  int max_read_errors = 0;

  // Create two separate clients for read and writes.
  auto write_client = CreateYBClient();
  auto read_client = CreateYBClient();
  yb::load_generator::YBSessionFactory write_session_factory(write_client.get(), &table_);
  yb::load_generator::YBSessionFactory read_session_factory(read_client.get(), &table_);

  yb::load_generator::MultiThreadedWriter writer(rows, start_key, writer_threads,
                                                 &write_session_factory, &stop_requested_flag,
                                                 value_size_bytes, max_write_errors);
  yb::load_generator::MultiThreadedReader reader(rows, reader_threads, &read_session_factory,
                                                 writer.InsertionPoint(), writer.InsertedKeys(),
                                                 writer.FailedKeys(), &stop_requested_flag,
                                                 value_size_bytes, max_read_errors);

  writer.Start();
  // Having separate write requires adding in write client id to the reader.
  reader.set_client_id(write_session_factory.ClientId());
  reader.Start();
  writer.WaitForCompletion();
  LOG(INFO) << "Writing complete";

  // The reader will not stop on its own, so we stop it after a couple of seconds after the writer
  // stops.
  SleepFor(MonoDelta::FromSeconds(2));
  reader.Stop();
  reader.WaitForCompletion();
  LOG(INFO) << "Reading complete";

  ASSERT_EQ(0, writer.num_write_errors());
  ASSERT_EQ(0, reader.num_read_errors());
  ASSERT_GE(writer.num_writes(), rows);
  ASSERT_GE(reader.num_reads(), rows);  // assuming reads are at least as fast as writes

  ClusterVerifier cluster_verifier(mini_cluster());
  ASSERT_NO_FATALS(cluster_verifier.CheckCluster());
  ASSERT_NO_FATALS(cluster_verifier.CheckRowCount(table_->name(), ClusterVerifier::EXACTLY, rows));
}

void KVTableTest::RunRestartTest() {
  ASSERT_NO_FATALS(PutSampleKeysValues());
  // Check we've written the data successfully.
  ASSERT_NO_FATALS(CheckSampleKeysValues());
  ASSERT_NO_FATALS(RestartCluster());

  LOG(INFO) << "Checking entries written before the cluster restart";
  ASSERT_NO_FATALS(CheckSampleKeysValues());
  ClusterVerifier cluster_verifier(mini_cluster());
  ASSERT_NO_FATALS(cluster_verifier.CheckCluster());
  ASSERT_NO_FATALS(cluster_verifier.CheckRowCount(table_->name(), ClusterVerifier::EXACTLY, 3));

  LOG(INFO) << "Issuing additional write operations";
  ASSERT_NO_FATALS(PutSampleKeysValues());
  ASSERT_NO_FATALS(CheckSampleKeysValues());

  // Wait until all tablet servers come up.
  std::vector<client::YBTabletServer> tablet_servers;
  do {
    tablet_servers = ASSERT_RESULT(client_->ListTabletServers());
    if (tablet_servers.size() == num_tablet_servers()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(100));
  } while (true);

  ASSERT_NO_FATALS(cluster_verifier.CheckCluster());
  ASSERT_NO_FATALS(cluster_verifier.CheckRowCount(table_->name(), ClusterVerifier::EXACTLY, 3));
}

TEST_F(KVTableTest, LoadTest) {
  RunLoadTest();
}

TEST_F(KVTableTest, Restart) {
  RunRestartTest();
}

TEST_F_EX(KVTableTest, LoadTestWithPriorityQueue, KVTablePriorityQueueTest) {
  ASSERT_NO_FATALS(RunLoadTest());
  // 8 client threads against a 2-permit budget on each server must have saturated it.
  ASSERT_NO_FATALS(VerifyPriorityQueueUsage({.contention = true, .tserver_low = true}));
}

TEST_F_EX(KVTableTest, RestartWithPriorityQueue, KVTablePriorityQueueTest) {
  ASSERT_NO_FATALS(RunRestartTest());
  // Verified against the post-restart servers (fresh messengers and metrics), so this also proves
  // the queue is recreated on restart. Not a load test, so contention is not asserted, and the
  // tablets were created before the restart, so no kLow work is expected.
  ASSERT_NO_FATALS(VerifyPriorityQueueUsage({}));
}

class KVTableSingleTabletTest : public KVTableTest {
 public:
  int num_tablets() override {
    return 1;
  }

  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_global_log_cache_size_limit_mb) = 1;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_log_cache_size_limit_mb) = 1;
    KVTableTest::SetUp();
  }
};

// Write big values with small log cache.
// And restart one tserver.
//
// So we expect that some operations would be unloaded to disk and loaded back
// after this tservers joined raft group again.
//
// Also check that we track such operations.
TEST_F_EX(KVTableTest, YB_DISABLE_TEST_ON_MACOS(BigValues), KVTableSingleTabletTest) {
  std::atomic_bool stop_requested_flag(false);
  SetFlagOnExit set_flag_on_exit(&stop_requested_flag);
  // The writers stop for good once they have written this many keys between them, while the wait
  // below needs 50 more writes AFTER the tserver goes down. Keep the budget well clear of that: a
  // slow shutdown (as under ASAN) lets the writers get most of the way through a small budget
  // first, leaving fewer keys than the wait asks for and a wait that can never complete.
  int rows = 10000;
  int start_key = 0;
  int writer_threads = 4;
  int value_size_bytes = 32_KB;
  int max_write_errors = 0;

  // Create two separate clients for read and writes.
  auto write_client = CreateYBClient();
  yb::load_generator::YBSessionFactory write_session_factory(write_client.get(), &table_);

  yb::load_generator::MultiThreadedWriter writer(rows, start_key, writer_threads,
                                                 &write_session_factory, &stop_requested_flag,
                                                 value_size_bytes, max_write_errors);

  writer.Start();
  mini_cluster_->mini_tablet_server(1)->Shutdown();
  auto start_writes = writer.num_writes();
  ASSERT_OK(WaitFor(
      [&writer, start_writes] { return writer.num_writes() - start_writes >= 50; }, 60s,
      "50 writes while the tserver is down"));
  ASSERT_OK(mini_cluster_->mini_tablet_server(1)->Start(tserver::WaitTabletsBootstrapped::kFalse));

  ASSERT_OK(WaitFor([] {
    std::vector<MemTrackerData> trackers;
    trackers.clear();
    CollectMemTrackerData(MemTracker::GetRootTracker(), 0, &trackers);
    bool found = false;
    for (const auto& data : trackers) {
      if (data.tracker->id() == "OperationsFromDisk" && data.tracker->peak_consumption()) {
        LOG(INFO) << "Tracker: " << data.tracker->ToString() << ", peak consumption: "
                  << data.tracker->peak_consumption();
        found = true;
      }
    }
    return found;
  }, 15s, "Load operations from disk"));

  // The writers no longer run out of keys on their own, so stop them before waiting.
  stop_requested_flag = true;
  writer.WaitForCompletion();
}

}  // namespace integration_tests
}  // namespace yb
