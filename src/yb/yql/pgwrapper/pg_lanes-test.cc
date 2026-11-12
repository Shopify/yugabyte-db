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

// PG sessions and the embedded client of tablet servers with CPU lanes.

#include <signal.h>

#include <fstream>
#include <map>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "yb/consensus/raft_consensus.h"

#include "yb/integration-tests/mini_cluster.h"

#include "yb/master/master.h"
#include "yb/master/mini_master.h"

#include "yb/rpc/connection_id.h"
#include "yb/rpc/messenger.h"
#include "yb/rpc/reactor.h"

#include "yb/tablet/tablet_peer.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/pg_client_service.h"
#include "yb/tserver/tablet_server.h"
#include "yb/tserver/ts_tablet_manager.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/cpu_topology.h"
#include "yb/util/cpu_topology_test_util.h"
#include "yb/util/env.h"
#include "yb/util/errno.h"
#include "yb/util/lanes.h"
#include "yb/util/lanes_test_util.h"
#include "yb/util/path_util.h"
#include "yb/util/test_macros.h"
#include "yb/util/tsan_util.h"

#include "yb/yql/pgwrapper/pg_mini_test_base.h"

DECLARE_bool(pg_client_use_shared_memory);
DECLARE_int32(replication_factor);
DECLARE_string(TEST_cpu_topology_cpuset);
DECLARE_string(TEST_cpu_topology_sysfs_root);
DECLARE_int32(tserver_lanes);
DECLARE_string(tserver_service_lane_cpus);

namespace yb::pgwrapper {

using namespace std::literals;

namespace {

std::string LaneName(LaneId lane) {
  return Format("L$0", ZeroPadded(lane, 3));
}

// Cpus_allowed_list of a /proc/.../status file.
Result<CpuSet> ProcCpus(const std::string& status_path) {
  std::ifstream in(status_path);
  SCHECK(in.is_open(), IOError, Format("Cannot open $0", status_path));
  std::string line;
  constexpr std::string_view kKey = "Cpus_allowed_list:";
  while (std::getline(in, line)) {
    if (line.starts_with(kKey)) {
      auto list = line.substr(kKey.size());
      list.erase(0, list.find_first_not_of(" \t"));
      return CpuSet::Parse(list);
    }
  }
  return STATUS_FORMAT(NotFound, "No Cpus_allowed_list in $0", status_path);
}

Result<CpuSet> ProcessCpus(int64_t pid) {
  return ProcCpus(Format("/proc/$0/status", pid));
}

struct OsThread {
  std::string name;
  CpuSet cpus;
};

// Threads of this process as the OS reports them.
Result<std::vector<OsThread>> ProcessThreads() {
  std::vector<OsThread> result;
  for (const auto& tid : VERIFY_RESULT(Env::Default()->GetChildren(
           "/proc/self/task", ExcludeDots::kTrue))) {
    const auto dir = JoinPathSegments("/proc/self/task", tid);
    std::ifstream comm(JoinPathSegments(dir, "comm"));
    std::string name;
    if (!comm.is_open() || !std::getline(comm, name)) {
      continue;  // The thread exited.
    }
    auto cpus = ProcCpus(JoinPathSegments(dir, "status"));
    if (!cpus.ok()) {
      continue;
    }
    result.push_back(OsThread{.name = std::move(name), .cpus = *cpus});
  }
  return result;
}

// Client connection ids of the messenger, each with the lane of the reactor that owns it.
Result<std::vector<std::pair<rpc::ConnectionId, LaneId>>> ClientConnections(
    rpc::Messenger* messenger) {
  std::vector<std::pair<rpc::ConnectionId, LaneId>> result;
  for (size_t i = 0; i != messenger->num_reactors(); ++i) {
    auto* reactor = messenger->TEST_reactor(i);
    for (const auto& id : VERIFY_RESULT(reactor->TEST_ClientConnectionIds())) {
      result.emplace_back(id, reactor->lane());
    }
  }
  return result;
}

} // namespace

// Tablet servers with forced lanes over a fake topology of two 2-CPU L3 domains laid over CPUs
// 0-3. PG connects to tablet server 0, so its PgClientService hosts every session.
class PgLanesTestBase : public PgMiniTestBase {
 protected:
  void SetUp() override {
    auto mask = ThisThreadMask();
    for (size_t cpu = 0; cpu != 4; ++cpu) {
      if (!mask.Test(cpu)) {
        GTEST_SKIP() << "Needs CPUs 0-3, have " << mask.ToString();
      }
    }
    auto root = GetTestPath("sys_cpu");
    ASSERT_OK(WriteFakeSysfsCpuTree(
        root, MakeFakeCpus({.sockets = 1, .l3_per_socket = 2, .cores_per_l3 = 2, .smt = 1})));
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_sysfs_root) = root;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_cpu_topology_cpuset) = "0-3";
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_lanes) = NumLanes();
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_service_lane_cpus) = ServiceCpus();
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_pg_client_use_shared_memory) = true;
    // Unset, so that the tablet servers install their topology; restored after the cluster stops.
    lane_scope_.emplace();
    PgMiniTestBase::SetUp();
    ASSERT_TRUE(LaneModeActive());
    ASSERT_EQ(GetLaneTopology().num_lanes(), NumLanes());
  }

  void DoTearDown() override {
    PgMiniTestBase::DoTearDown();
    lane_scope_.reset();
  }

  virtual int NumLanes() const { return 4; }

  virtual std::string ServiceCpus() const { return ""; }

  tserver::PgClientServiceImpl& PgService() const {
    return *cluster_->mini_tablet_server(kPgTsIndex)->server()->TEST_GetPgClientService();
  }

  // Lane of the PgClientService session of the connection's backend.
  Result<LaneId> SessionLane(PGConn* conn) const {
    const auto pid = VERIFY_RESULT(conn->FetchRow<int32_t>("SELECT pg_backend_pid()"));
    auto lane = PgService().TEST_SessionLane(pid);
    SCHECK(lane.has_value(), NotFound, Format("No session for backend $0", pid));
    return *lane;
  }

  Result<TabletId> SingleTabletOf(const std::string& table_name) {
    auto peers = VERIFY_RESULT(ListTabletPeersForTableName(
        cluster_.get(), table_name, ListPeersFilter::kLeaders));
    SCHECK_EQ(peers.size(), 1U, IllegalState, "Expected one tablet");
    return peers.front()->tablet_id();
  }

 private:
  std::optional<TEST_SetLaneTopology> lane_scope_;
};

// Three tablet servers. Tables created by the tests have one replica each, so tablet server 0
// sends no Raft calls for them.
class PgLanesTest : public PgLanesTestBase {
 protected:
  void BeforePgProcessStart() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_replication_factor) = 1;
  }
};

// Sessions are spread round-robin over the lanes and counted per lane until they expire; each
// session's shared memory exchange runs on a worker of its lane's exchange pool, named by lane
// and confined to the lane's CPUs.
TEST_F(PgLanesTest, SessionsSpreadOverLanes) {
  const size_t kLanes = NumLanes();
  const size_t kSessions = 2 * kLanes;
  // The first connection to a database also opens an internal one, which builds the relcache init
  // file. Both are gone before the sessions below are opened one by one; only the ASH collector's
  // session stays.
  constexpr size_t kAshSessions = 1;
  ASSERT_RESULT(Connect());
  ASSERT_OK(WaitFor([this] { return PgService().TEST_SessionsCount() == kAshSessions; },
                    30s * kTimeMultiplier, "Sessions of the first connection expire"));
  const auto before = PgService().SessionsPerLane();
  ASSERT_EQ(before.size(), kLanes);
  ASSERT_EQ(std::accumulate(before.begin(), before.end(), size_t{0}), kAshSessions);

  std::vector<PGConn> conns;
  std::vector<LaneId> lanes;
  for (size_t i = 0; i != kSessions; ++i) {
    conns.push_back(ASSERT_RESULT(Connect()));
    lanes.push_back(ASSERT_RESULT(SessionLane(&conns.back())));
  }
  for (size_t i = 1; i != kSessions; ++i) {
    ASSERT_EQ(lanes[i], (lanes[i - 1] + 1) % kLanes) << AsString(lanes);
  }
  auto after = PgService().SessionsPerLane();
  ASSERT_EQ(after.size(), kLanes);
  for (size_t lane = 0; lane != kLanes; ++lane) {
    ASSERT_EQ(after[lane], before[lane] + kSessions / kLanes) << lane;
  }

  ASSERT_EQ(PgService().TEST_NumExchangeThreadPools(), kLanes);
  // Each session opened above holds one exchange worker. Tablet server 0's are the only ones: the
  // other tablet servers and the master host no session.
  std::map<LaneId, size_t> workers;
  for (const auto& thread : ASSERT_RESULT(ProcessThreads())) {
    ASSERT_FALSE(thread.name.starts_with("shmem_exchange")) << thread.name;
    for (LaneId lane = 0; lane != kLanes; ++lane) {
      if (thread.name.starts_with(LaneName(lane) + "_shmem_")) {
        ++workers[lane];
        ASSERT_EQ(thread.cpus, GetLaneTopology().lane_mask(lane))
            << thread.name << ": " << thread.cpus.ToString();
      }
    }
  }
  for (LaneId lane = 0; lane != kLanes; ++lane) {
    ASSERT_GE(workers[lane], kSessions / kLanes) << AsString(workers);
  }

  auto& conn = conns.front();
  ASSERT_OK(conn.Execute("CREATE TABLE t (k INT PRIMARY KEY, v INT)"));
  for (size_t i = 0; i != kSessions; ++i) {
    ASSERT_OK(conns[i].ExecuteFormat("INSERT INTO t VALUES ($0, $0)", i));
  }
  ASSERT_EQ(ASSERT_RESULT(conn.FetchRow<PGUint64>("SELECT COUNT(*) FROM t")), kSessions);

  // One session of each lane ends; the counts follow its expiry.
  conns.erase(conns.begin(), conns.begin() + kLanes);
  ASSERT_OK(WaitFor([this, &after, kLanes]() {
    auto now = PgService().SessionsPerLane();
    for (size_t lane = 0; lane != kLanes; ++lane) {
      if (now[lane] != after[lane] - 1) {
        return false;
      }
    }
    return true;
  }, 30s * kTimeMultiplier, "Sessions of closed connections expire"));
}

// The system tables created at startup (transaction status, advisory locks, ...) have three
// replicas, so tablet server 0 may hold lane-keyed connections for Raft in their lanes. With many
// lanes, most tablets of a table with one replica per tablet are in other lanes, where only client
// hints create lane-keyed connections.
class PgLanesConnectionsTest : public PgLanesTest {
 protected:
  int NumLanes() const override { return 16; }
};

// Tablet RPCs of the embedded client go on connections keyed by (tablet server, lane of the
// tablet) and owned by that lane's reactors; calls to the master, including the sys catalog reads
// of PG's catalog lookups, carry no lane.
TEST_F(PgLanesConnectionsTest, TabletRpcsUseTabletLaneConnections) {
  constexpr int kRows = 200;
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE t (k INT PRIMARY KEY, v INT) SPLIT INTO 16 TABLETS"));
  ASSERT_OK(conn.Execute("INSERT INTO t VALUES (-1, -1)"));
  const auto before = LaneHandoffCounts::Now();
  for (int i = 0; i != kRows; ++i) {
    ASSERT_OK(conn.ExecuteFormat("INSERT INTO t VALUES ($0, $0)", i));
  }
  // Process-wide, so the remote tablet servers' receive side is included. Logged for the record.
  const auto delta = LaneHandoffCounts::Now() - before;
  LOG(INFO) << "Lane handoffs during " << kRows << " single-row inserts: " << delta.ToString();
  for (int i = 0; i < kRows; i += 7) {
    ASSERT_EQ(ASSERT_RESULT(conn.FetchRow<int32_t>(Format("SELECT v FROM t WHERE k = $0", i))), i);
  }
  // A new connection reads the catalog from the master's sys catalog tablet.
  auto conn2 = ASSERT_RESULT(Connect());
  ASSERT_EQ(ASSERT_RESULT(conn2.FetchRow<PGUint64>("SELECT COUNT(*) FROM t")), kRows + 1);

  auto* ts0 = cluster_->mini_tablet_server(kPgTsIndex);
  std::map<uint16_t, std::string> tserver_ports;
  for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
    auto* ts = cluster_->mini_tablet_server(i);
    tserver_ports[ts->bound_rpc_addr().port()] = ts->server()->permanent_uuid();
  }
  std::set<uint16_t> master_ports;
  for (size_t i = 0; i != cluster_->num_masters(); ++i) {
    master_ports.insert(cluster_->mini_master(i)->bound_rpc_addr().port());
  }

  // Lanes in which tablet server 0 may hold lane-keyed connections for Raft: those of its
  // replicas in multi-replica configurations (system tables).
  std::set<LaneId> raft_lanes;
  for (const auto& peer : ts0->server()->tablet_manager()->GetTabletPeers()) {
    auto consensus = peer->GetRaftConsensus();
    if (consensus.ok() && (*consensus)->CommittedConfig().peers_size() > 1) {
      raft_lanes.insert(peer->lane());
    }
  }
  // Lanes of the tablets each tablet server leads.
  std::map<std::string, std::set<LaneId>> leader_lanes;
  std::vector<std::pair<std::string, LaneId>> remote_table_tablets;
  for (const auto& peer : ListTabletPeers(cluster_.get(), ListPeersFilter::kLeaders,
                                          UserTabletsOnly::kFalse)) {
    leader_lanes[peer->permanent_uuid()].insert(peer->lane());
  }
  for (const auto& peer : ASSERT_RESULT(ListTabletPeersForTableName(
           cluster_.get(), "t", ListPeersFilter::kLeaders))) {
    ASSERT_EQ(peer->lane(), LaneOf(peer->tablet_id()));
    if (peer->permanent_uuid() != ts0->server()->permanent_uuid()) {
      remote_table_tablets.emplace_back(peer->permanent_uuid(), peer->lane());
    }
  }
  ASSERT_FALSE(remote_table_tablets.empty());

  auto connections = ASSERT_RESULT(ClientConnections(ts0->server()->messenger()));
  LOG(INFO) << "Client connections of tablet server 0: " << AsString(connections)
            << ", Raft lanes: " << AsString(raft_lanes);
  size_t master_connections = 0;
  std::set<std::pair<std::string, LaneId>> laned;
  for (const auto& [id, reactor_lane] : connections) {
    const auto port = id.remote().port();
    if (master_ports.contains(port)) {
      ASSERT_EQ(id.lane(), kNoLane) << id.ToString();
      ++master_connections;
      continue;
    }
    if (id.lane() == kNoLane) {
      continue;
    }
    ASSERT_EQ(id.lane(), reactor_lane) << id.ToString();
    ASSERT_TRUE(tserver_ports.contains(port)) << id.ToString();
    const auto& uuid = tserver_ports[port];
    // Every lane-keyed connection is in the lane of a tablet its remote leads, or is Raft's.
    ASSERT_TRUE(leader_lanes[uuid].contains(id.lane()) || raft_lanes.contains(id.lane()))
        << id.ToString() << ", leader lanes: " << AsString(leader_lanes[uuid]);
    laned.emplace(uuid, id.lane());
  }
  ASSERT_GT(master_connections, 0);

  // Each tablet of t led by another tablet server was reached on its lane's connection. Lanes that
  // Raft could also explain are skipped, so the check is about client hints alone.
  size_t checked = 0;
  for (const auto& tablet : remote_table_tablets) {
    if (raft_lanes.contains(tablet.second)) {
      continue;
    }
    ++checked;
    ASSERT_TRUE(laned.contains(tablet)) << "No lane " << static_cast<int>(tablet.second)
                                        << " connection to " << tablet.first;
  }
  LOG(INFO) << "Checked " << checked << " of " << remote_table_tablets.size()
            << " remote tablets";
  ASSERT_GT(checked, 0) << "Every remote tablet's lane is also a Raft lane";
}

// One tablet server and one tablet T, so every write is a local call and the crossing counters
// see only this workload.
class PgLanesSingleServerTest : public PgLanesTestBase {
 protected:
  size_t NumTabletServers() override { return 1; }
};

// A session in lane(T) writes and reads T without crossing a lane. A session in another lane
// crosses exactly twice per operation: into lane(T)'s handler pool and back to its own lane's
// callback pool. Since lane(T)'s preparer counts a write submitted from an unlaned thread as an
// entry from unlaned and from another lane as a tablet pipeline crossing, zero of both shows the
// write handlers ran in lane(T).
TEST_F(PgLanesSingleServerTest, LocalCallCrossings) {
  constexpr int kOps = 50;
  constexpr int kWarmupOps = 10;
  auto setup = ASSERT_RESULT(Connect());
  ASSERT_OK(setup.Execute("CREATE TABLE t (k INT PRIMARY KEY, v INT) SPLIT INTO 1 TABLETS"));
  const auto tablet_lane = LaneOf(ASSERT_RESULT(SingleTabletOf("t")));

  std::optional<PGConn> same;
  std::optional<PGConn> other;
  while (!same || !other) {
    auto conn = ASSERT_RESULT(Connect());
    const auto lane = ASSERT_RESULT(SessionLane(&conn));
    auto& slot = lane == tablet_lane ? same : other;
    if (!slot) {
      slot.emplace(std::move(conn));
    }
  }

  int key = 0;
  for (auto* conn : {&*same, &*other}) {
    for (int i = 0; i != kWarmupOps; ++i, ++key) {
      ASSERT_OK(conn->ExecuteFormat("INSERT INTO t VALUES ($0, $0)", key));
      ASSERT_OK(conn->FetchRow<int32_t>(Format("SELECT v FROM t WHERE k = $0", key)));
    }
  }

  auto run = [&key](PGConn* conn, bool read) -> Result<LaneHandoffCounts> {
    auto before = LaneHandoffCounts::Now();
    for (int i = 0; i != kOps; ++i) {
      if (read) {
        RETURN_NOT_OK(conn->FetchRow<int32_t>(Format("SELECT v FROM t WHERE k = $0", i)));
      } else {
        RETURN_NOT_OK(conn->ExecuteFormat("INSERT INTO t VALUES ($0, $0)", key++));
      }
    }
    return LaneHandoffCounts::Now() - before;
  };

  for (bool read : {false, true}) {
    SCOPED_TRACE(read ? "reads" : "writes");
    auto delta = ASSERT_RESULT(run(&*same, read));
    LOG(INFO) << "Same lane " << (read ? "reads" : "writes") << ": " << delta.ToString();
    ASSERT_EQ(delta.total_crossings(), 0) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 0) << delta.ToString();

    delta = ASSERT_RESULT(run(&*other, read));
    LOG(INFO) << "Other lane " << (read ? "reads" : "writes") << ": " << delta.ToString();
    ASSERT_EQ(delta.crossing(LaneCrossingCategory::kRpcHandler), kOps) << delta.ToString();
    ASSERT_EQ(delta.crossing(LaneCrossingCategory::kCallback), kOps) << delta.ToString();
    ASSERT_EQ(delta.total_crossings(), 2 * kOps) << delta.ToString();
    ASSERT_EQ(delta.entries_from_unlaned, 0) << delta.ToString();
  }
}

// The postmaster and its backends float over the whole process cpuset even though the threads
// that spawn the postmaster are confined to the service CPU.
class PgLanesServiceCpusTest : public PgLanesTestBase {
 protected:
  size_t NumTabletServers() override { return 1; }
  int NumLanes() const override { return 2; }
  std::string ServiceCpus() const override { return "3"; }

  Result<int64_t> PostmasterPid() {
    auto pid = pg_supervisor_ ? pg_supervisor_->ProcessId() : std::nullopt;
    SCHECK(pid.has_value(), IllegalState, "Postmaster is not running");
    return *pid;
  }

  Result<int64_t> WaitNewPostmaster(int64_t old_pid) {
    int64_t new_pid = 0;
    RETURN_NOT_OK(WaitFor([this, old_pid, &new_pid]() -> Result<bool> {
      auto pid = pg_supervisor_->ProcessId();
      if (!pid || *pid == old_pid) {
        return false;
      }
      new_pid = *pid;
      return true;
    }, 60s * kTimeMultiplier, "Postmaster restarted"));
    return new_pid;
  }

  // The postmaster's CPUs, those of a backend it forks, and those of the supervisor thread that
  // spawns it.
  void CheckMasks(int64_t pid) {
    const auto& topology = GetLaneTopology();
    auto cpus = ASSERT_RESULT(ProcessCpus(pid));
    ASSERT_EQ(cpus, topology.full_process_cpuset()) << "Postmaster: " << cpus.ToString();
    auto conn = ASSERT_RESULT(Connect());
    const auto backend = ASSERT_RESULT(conn.FetchRow<int32_t>("SELECT pg_backend_pid()"));
    cpus = ASSERT_RESULT(ProcessCpus(backend));
    ASSERT_EQ(cpus, topology.full_process_cpuset()) << "Backend: " << cpus.ToString();
    size_t supervisors = 0;
    for (const auto& thread : ASSERT_RESULT(ProcessThreads())) {
      if (thread.name.starts_with("PostgreSQL sup")) {
        ++supervisors;
        ASSERT_EQ(thread.cpus, topology.service_mask())
            << thread.name << ": " << thread.cpus.ToString();
      }
    }
    ASSERT_EQ(supervisors, 1);
  }
};

TEST_F(PgLanesServiceCpusTest, PostmasterFloatsOverProcessCpuset) {
  const auto& topology = GetLaneTopology();
  ASSERT_EQ(topology.service_mask(), ASSERT_RESULT(CpuSet::Parse("3")));
  ASSERT_EQ(topology.full_process_cpuset(), ASSERT_RESULT(CpuSet::Parse("0-3")));
  auto pid = ASSERT_RESULT(PostmasterPid());
  ASSERT_NO_FATALS(CheckMasks(pid));

  // Restart through the tablet server, which the supervisor thread carries out.
  ASSERT_OK(RestartPostgres());
  pid = ASSERT_RESULT(WaitNewPostmaster(pid));
  ASSERT_NO_FATALS(CheckMasks(pid));

  // The supervisor restarts a postmaster that died.
  ASSERT_EQ(kill(static_cast<pid_t>(pid), SIGKILL), 0) << ErrnoToString(errno);
  pid = ASSERT_RESULT(WaitNewPostmaster(pid));
  ASSERT_NO_FATALS(CheckMasks(pid));
}

// With lanes off sessions and the embedded client are as without lanes: one unlaned exchange
// pool, no session lanes or per-lane counts, no lane hints, no affinity change and nothing
// counted.
class PgLanesOffTest : public PgMiniTestBase {
 protected:
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_pg_client_use_shared_memory) = true;
    PgMiniTestBase::SetUp();
  }
};

TEST_F(PgLanesOffTest, SessionsAndClientUnlaned) {
  ASSERT_EQ(FLAGS_tserver_lanes, 1);
  ASSERT_FALSE(LaneModeActive());
  const auto affinity_calls = TEST_CpuAffinityCallCount();
  const auto before = LaneHandoffCounts::Now();

  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE t (k INT PRIMARY KEY, v INT) SPLIT INTO 4 TABLETS"));
  for (int i = 0; i != 100; ++i) {
    ASSERT_OK(conn.ExecuteFormat("INSERT INTO t VALUES ($0, $0)", i));
  }
  ASSERT_EQ(ASSERT_RESULT(conn.FetchRow<PGUint64>("SELECT COUNT(*) FROM t")), 100);

  auto& service =
      *cluster_->mini_tablet_server(kPgTsIndex)->server()->TEST_GetPgClientService();
  const auto pid = ASSERT_RESULT(conn.FetchRow<int32_t>("SELECT pg_backend_pid()"));
  ASSERT_EQ(service.TEST_SessionLane(pid), kNoLane);
  ASSERT_TRUE(service.SessionsPerLane().empty());
  ASSERT_EQ(service.TEST_NumExchangeThreadPools(), 1);
  size_t exchange_workers = 0;
  for (const auto& thread : ASSERT_RESULT(ProcessThreads())) {
    ASSERT_FALSE(thread.name.starts_with("L0")) << thread.name;
    exchange_workers += thread.name.starts_with("shmem_exchange");
  }
  ASSERT_GE(exchange_workers, 1);

  for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
    auto* messenger = cluster_->mini_tablet_server(i)->server()->messenger();
    ASSERT_FALSE(messenger->laned());
    for (const auto& [id, reactor_lane] : ASSERT_RESULT(ClientConnections(messenger))) {
      ASSERT_EQ(id.lane(), kNoLane) << id.ToString();
      ASSERT_EQ(reactor_lane, kNoLane);
    }
  }
  ASSERT_EQ(TEST_CpuAffinityCallCount(), affinity_calls);
  ASSERT_EQ(affinity_calls, 0);
  const auto delta = LaneHandoffCounts::Now() - before;
  ASSERT_EQ(delta.total_crossings(), 0) << delta.ToString();
  ASSERT_EQ(delta.entries_from_unlaned, 0) << delta.ToString();
}

} // namespace yb::pgwrapper
