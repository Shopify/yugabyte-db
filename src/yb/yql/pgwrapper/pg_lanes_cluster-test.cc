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

// The lane crossing budget of YSQL writes in clusters whose tablet servers run as separate
// processes, so that each server's crossing counters cover that server only, and clusters with
// lanes off.

#include <cstdlib>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "yb/integration-tests/external_mini_cluster.h"

#include "yb/util/cpu_topology_test_util.h"
#include "yb/util/curl_util.h"
#include "yb/util/json_document.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/test_util.h"

#include "yb/yql/pgwrapper/libpq_test_base.h"

namespace yb::pgwrapper {

using namespace std::literals;

namespace {

const std::vector<std::string> kCrossingMetrics = {
    "lane_crossings_reactor", "lane_crossings_rpc_handler", "lane_crossings_tablet_pipeline",
    "lane_crossings_callback", "lane_crossings_other"};

const std::vector<std::string> kLaneMetrics = {
    "lane_crossings_reactor", "lane_crossings_rpc_handler", "lane_crossings_tablet_pipeline",
    "lane_crossings_callback", "lane_crossings_other", "lane_entries_from_unlaned",
    "rpc_lane_preamble_bound", "rpc_lane_preamble_absent", "rpc_lane_preamble_peek_timeouts",
    "rpc_lane_preamble_stripped_late"};

using MetricValues = std::map<std::string, int64_t>;

Result<MetricValues> LaneMetrics(const ExternalTabletServer& ts) {
  MetricValues result;
  for (const auto& name : kLaneMetrics) {
    result[name] = VERIFY_RESULT(ts.GetMetric<int64_t>(
        "server", "yb.tabletserver", name.c_str(), "value"));
  }
  return result;
}

MetricValues operator-(const MetricValues& lhs, const MetricValues& rhs) {
  MetricValues result;
  for (const auto& [name, value] : lhs) {
    result[name] = value - rhs.at(name);
  }
  return result;
}

int64_t TotalCrossings(const MetricValues& values) {
  int64_t result = 0;
  for (const auto& name : kCrossingMetrics) {
    result += values.at(name);
  }
  return result;
}

Result<std::string> FetchLanesJson(const ExternalDaemon& daemon) {
  EasyCurl curl;
  faststring body;
  RETURN_NOT_OK(curl.FetchURL(
      Format("http://$0/api/v1/lanes", daemon.bound_http_hostport()), &body));
  return body.ToString();
}

} // namespace

// Three tablet servers with forced 4 lanes over a fake topology of two 2-CPU L3 domains laid over
// CPUs 0-3, RF3, the lane preamble AutoFlag promoted (as in every fresh universe), and shared
// memory PG sessions. PG clients connect to tablet server 0.
class PgLanesClusterTest : public LibPqTestBase {
 protected:
  void SetUp() override {
    auto mask = ThisThreadMask();
    for (size_t cpu = 0; cpu != 4; ++cpu) {
      if (!mask.Test(cpu)) {
        GTEST_SKIP() << "Needs CPUs 0-3, have " << mask.ToString();
      }
    }
    LibPqTestBase::SetUp();
  }

  void UpdateMiniClusterOptions(ExternalMiniClusterOptions* options) override {
    LibPqTestBase::UpdateMiniClusterOptions(options);
    auto root = GetTestPath("sys_cpu");
    CHECK_OK(WriteFakeSysfsCpuTree(
        root, MakeFakeCpus({.sockets = 1, .l3_per_socket = 2, .cores_per_l3 = 2, .smt = 1})));
    options->replication_factor = 3;
    options->extra_tserver_flags.insert(options->extra_tserver_flags.end(), {
        "--tserver_lanes=4",
        "--TEST_cpu_topology_sysfs_root=" + root,
        "--TEST_cpu_topology_cpuset=0-3",
        "--pg_client_use_shared_memory=true",
    });
    if (UseNodeToNodeEncryption()) {
      for (auto* flags : {&options->extra_master_flags, &options->extra_tserver_flags}) {
        flags->insert(flags->end(), {
            "--use_node_to_node_encryption=true", "--certs_dir=" + GetCertsDir()});
      }
      options->use_even_ips = true;
    }
  }

  virtual bool UseNodeToNodeEncryption() const { return false; }

  void TestCrossingBudget();
};

// The same with TLS between the servers, so the preamble goes before the TLS ClientHello.
class PgLanesClusterTlsTest : public PgLanesClusterTest {
 protected:
  bool UseNodeToNodeEncryption() const override { return true; }
};

// Single-row INSERTs into a table of 12 tablets from 8 connections, two per session lane. Each
// write is one tablet RPC from its session's lane s to the tablet T. With equal lane counts and
// bound connections, the counted crossings of a write are:
// - on tablet server 0, the client side: none if s == lane(T); otherwise one handoff out of s
//   (into lane(T)'s reactor for a remote leader, into lane(T)'s handler pool for the local leader)
//   and the completion back into s's callback pool, so at most 2;
// - on every leader and follower receive path: none, since the leader's write handler, its pipeline
//   and Raft calls, and the followers' UpdateConsensus handlers all run in lane(T).
// The PG backend's two hops through shared memory are not counted.
void PgLanesClusterTest::TestCrossingBudget() {
  constexpr int kConnections = 8;
  constexpr int kWarmupRowsPerConnection = 24;
  constexpr int kRowsPerConnection = 50;
  constexpr int kWrites = kConnections * kRowsPerConnection;
  // Background work counted while the writes run, on any one server: Raft heartbeats and master
  // heartbeats start on unlaned threads and count no crossing, but a leader change and the retries
  // it causes, the ASH collector's session on every server and other periodic tasks can. Measured
  // with and without TLS: 0 on tablet servers 1 and 2, 0 pipeline crossings on tablet server 0, and
  // its outbound crossings equal to its callback crossings.
  constexpr int64_t kSlack = 10;

  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE t (k INT PRIMARY KEY, v INT) SPLIT INTO 12 TABLETS"));
  std::vector<PGConn> conns;
  for (int i = 0; i != kConnections; ++i) {
    conns.push_back(ASSERT_RESULT(Connect()));
  }

  std::vector<MetricValues> start;
  for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
    start.push_back(ASSERT_RESULT(LaneMetrics(*cluster_->tablet_server(i))));
  }

  // Each connection writes to every tablet, so tablet lookups, table loads and connection setup
  // happen here, outside the measured window.
  auto write_rows = [&conns](int first_key, int rows_per_connection) {
    TestThreadHolder threads;
    for (int i = 0; i != kConnections; ++i) {
      threads.AddThreadFunctor([&conn = conns[i], first_key, rows_per_connection, i] {
        for (int row = 0; row != rows_per_connection; ++row) {
          const auto key = first_key + i * rows_per_connection + row;
          ASSERT_OK(conn.ExecuteFormat("INSERT INTO t VALUES ($0, $0)", key));
        }
      });
    }
    threads.JoinAll();
  };
  write_rows(0, kWarmupRowsPerConnection);

  std::vector<MetricValues> before;
  for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
    before.push_back(ASSERT_RESULT(LaneMetrics(*cluster_->tablet_server(i))));
  }
  write_rows(1000000, kRowsPerConnection);
  std::vector<MetricValues> delta;
  for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
    delta.push_back(ASSERT_RESULT(LaneMetrics(*cluster_->tablet_server(i))) - before[i]);
    LOG(INFO) << "Tablet server " << i << " lane counters during setup and warm-up: "
              << AsString(before[i] - start[i]);
    LOG(INFO) << "Tablet server " << i << " lane counters during " << kWrites
              << " single-row inserts: " << AsString(delta[i]);
    LOG(INFO) << "Tablet server " << i << " /api/v1/lanes: "
              << ASSERT_RESULT(FetchLanesJson(*cluster_->tablet_server(i)));
  }

  ASSERT_EQ(ASSERT_RESULT(conn.FetchRow<PGUint64>("SELECT COUNT(*) FROM t")),
            kConnections * (kWarmupRowsPerConnection + kRowsPerConnection));

  // Receive paths: tablet servers 1 and 2 serve only leader writes and follower replication.
  // Their inbound tserver connections are bound to lanes, so their handlers run in the tablets'
  // lanes; a connection that was not bound would make the leader's preparer and the follower's
  // pipeline tasks cross for most writes.
  for (size_t i = 1; i != cluster_->num_tablet_servers(); ++i) {
    SCOPED_TRACE(Format("tablet server $0", i));
    ASSERT_LE(TotalCrossings(delta[i]), kSlack) << AsString(delta[i]);
    ASSERT_GT(before[i].at("rpc_lane_preamble_bound"), 0);
    ASSERT_EQ(before[i].at("rpc_lane_preamble_peek_timeouts"), 0);
  }

  // Tablet server 0: its own receive side (local leader writes, follower replication) crosses no
  // more than the others'.
  const auto& client = delta[0];
  ASSERT_LE(client.at("lane_crossings_tablet_pipeline"), kSlack) << AsString(client);
  // Client side: at most 2 per write. Sessions are spread over the 4 lanes and tablets hash
  // uniformly, so about 3/4 of the writes cross, twice each: at least one crossing per write on
  // average shows the count is not vacuous.
  ASSERT_LE(TotalCrossings(client), 2 * kWrites + kSlack) << AsString(client);
  ASSERT_GE(TotalCrossings(client), kWrites) << AsString(client);
  // Each write that crosses out of its session's lane crosses back once.
  const auto out = client.at("lane_crossings_reactor") + client.at("lane_crossings_rpc_handler");
  ASSERT_LE(std::abs(out - client.at("lane_crossings_callback")), kSlack) << AsString(client);
  ASSERT_GT(client.at("lane_crossings_reactor"), 0) << AsString(client);
  ASSERT_GT(client.at("lane_crossings_rpc_handler"), 0) << AsString(client);
}

TEST_F(PgLanesClusterTest, WriteCrossingBudget) {
  TestCrossingBudget();
}

TEST_F(PgLanesClusterTlsTest, WriteCrossingBudget) {
  TestCrossingBudget();
}

// A universe with lanes off (the default) exposes no lane metric on any server and reports lane
// mode as inactive.
class PgLanesOffClusterTest : public LibPqTestBase {};

TEST_F(PgLanesOffClusterTest, NoLaneMetricsOrState) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE t (k INT PRIMARY KEY, v INT) SPLIT INTO 6 TABLETS"));
  for (int i = 0; i != 100; ++i) {
    ASSERT_OK(conn.ExecuteFormat("INSERT INTO t VALUES ($0, $0)", i));
  }
  ASSERT_EQ(ASSERT_RESULT(conn.FetchRow<PGUint64>("SELECT COUNT(*) FROM t")), 100);

  for (auto* daemon : cluster_->daemons()) {
    SCOPED_TRACE(daemon->id());
    for (const auto& name : kLaneMetrics) {
      for (const auto* entity_id : {"yb.tabletserver", "yb.master"}) {
        auto value = daemon->GetMetric<int64_t>("server", entity_id, name.c_str(), "value");
        ASSERT_TRUE(!value.ok() && value.status().IsNotFound()) << name << ": " << AsString(value);
      }
    }
  }
  for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
    auto json = ASSERT_RESULT(FetchLanesJson(*cluster_->tablet_server(i)));
    JsonDocument doc;
    auto root = ASSERT_RESULT(doc.Parse(json));
    ASSERT_FALSE(ASSERT_RESULT(root["lanes_active"].GetBool())) << json;
    ASSERT_EQ(ASSERT_RESULT(root["topology"].GetString()), "lanes disabled (tserver_lanes=1)");
    ASSERT_EQ(ASSERT_RESULT(root["preamble"].size()), 0);
  }
}

} // namespace yb::pgwrapper
