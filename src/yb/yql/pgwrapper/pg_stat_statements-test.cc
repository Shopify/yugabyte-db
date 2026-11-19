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

#include <fstream>
#include <optional>
#include <sstream>

#include "yb/util/backoff_waiter.h"
#include "yb/util/path_util.h"
#include "yb/util/test_thread_holder.h"
#include "yb/yql/pgwrapper/libpq_test_base.h"

using namespace std::chrono_literals;

namespace yb {
namespace pgwrapper {

class PgStatStatementsTest : public LibPqTestBase {
 public:
  int GetNumMasters() const override { return 1; }
  int GetNumTabletServers() const override { return 1; }

  void UpdateMiniClusterOptions(ExternalMiniClusterOptions* options) override {
    options->replication_factor = 1;
    options->extra_tserver_flags.push_back("--ysql_enable_auto_analyze=false");
  }

 protected:
  // A backend keeps its statistics in local memory and adds them to pg_stat_statements when it
  // folds: at the end of a statement or while idle, once its oldest statistics are
  // pg_stat_statements.yb_flush_interval old; when it reads or resets pg_stat_statements; and when
  // it exits. The first store for a key in a backend, and the first after any reset or eviction,
  // is synchronous; with track_planning on (the default) that is the plan of the first execution,
  // and its execution is already pending. So the cases below that check lag compare deltas or
  // bounds, and the others read after the writer has folded.

  static Result<int32_t> BackendPid(PGConn* conn) {
    return conn->FetchRow<int32_t>("SELECT pg_backend_pid()");
  }

  // A backend leaves pg_stat_activity in an on_shmem_exit callback, after the before_shmem_exit
  // callback that folds its statistics.
  static Status WaitForBackendsGone(PGConn* reader, const std::vector<int32_t>& pids) {
    std::string pid_list;
    for (auto pid : pids) {
      pid_list += Format("$0$1", pid_list.empty() ? "" : ",", pid);
    }
    return LoggedWaitFor(
        [reader, &pid_list]() -> Result<bool> {
          return VERIFY_RESULT(reader->FetchRow<int64_t>(Format(
              "SELECT count(*) FROM pg_stat_activity "
              "WHERE backend_type = 'client backend' AND pid IN ($0)", pid_list))) == 0;
        },
        30s * kTimeMultiplier, "waiting for writer backends to exit");
  }

  static Result<int64_t> SumCalls(PGConn* conn, const std::string& where) {
    return conn->FetchRow<int64_t>(Format(
        "SELECT COALESCE(sum(calls), 0)::bigint FROM pg_stat_statements WHERE $0", where));
  }

  static Result<int64_t> SumRows(PGConn* conn, const std::string& where) {
    return conn->FetchRow<int64_t>(Format(
        "SELECT COALESCE(sum(rows), 0)::bigint FROM pg_stat_statements WHERE $0", where));
  }

  // Sum of every bucket of the latency histogram, including the one for slow executions.
  static Result<int64_t> SumHistogram(PGConn* conn, const std::string& where) {
    return conn->FetchRow<int64_t>(Format(
        "SELECT COALESCE(sum(b.value::bigint), 0)::bigint "
        "FROM pg_stat_statements s, jsonb_array_elements(s.yb_latency_histogram) h, "
        "jsonb_each_text(h) b WHERE $0", where));
  }

  // A statement with no constants whose shape differs for each n.
  static std::string ShapeQuery(int n) {
    std::string query = "SELECT k";
    for (int i = 1; i < n; ++i) {
      query += ", k";
    }
    return query + " FROM test_table";
  }

  static Result<std::string> BackendState(PGConn* reader, int32_t pid) {
    return reader->FetchRow<std::string>(
        Format("SELECT state FROM pg_stat_activity WHERE pid = $0", pid));
  }

  // Waits until the reader sees at least `expected` calls of the INSERT while the writer stays
  // idle, so only its idle fold can deliver them, and logs how long that took.
  static Status WaitForIdleFold(PGConn* reader, int64_t expected, MonoTime last_statement_end) {
    RETURN_NOT_OK(LoggedWaitFor(
        [reader, expected]() -> Result<bool> {
          return VERIFY_RESULT(SumCalls(reader, kInsertWhere)) >= expected;
        },
        5s * kTimeMultiplier, "waiting for the idle writer to fold"));
    LOG(INFO) << "Idle fold visible "
              << (MonoTime::Now() - last_statement_end).ToMilliseconds()
              << " ms after the writer's last statement";
    return Status::OK();
  }

  // The writer sets kIdleIntervalMs, warms the INSERT's key and runs it kExecutions more times,
  // inside a transaction block if begin is set and then failing a statement in it if abort is set.
  // The reader then sees those executions within the interval while the writer stays idle in the
  // expected state.
  void TestIdleFoldsWithinInterval(bool begin, bool abort, const std::string& idle_state) {
    constexpr int kExecutions = 10;
    auto writer = ASSERT_RESULT(Connect());
    auto reader = ASSERT_RESULT(Connect());
    ASSERT_OK(writer.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
    ASSERT_OK(writer.Fetch("SELECT pg_stat_statements_reset()"));
    const auto writer_pid = ASSERT_RESULT(BackendPid(&writer));
    ASSERT_OK(writer.ExecuteFormat(
        "SET pg_stat_statements.yb_flush_interval = $0", kIdleIntervalMs));
    if (begin) {
      ASSERT_OK(writer.Execute("BEGIN"));
    }

    // The writer's oldest pending statistics are recorded after this instant.
    const auto start = MonoTime::Now();
    for (int i = 0; i <= kExecutions; ++i) {
      ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", i));
    }
    if (abort) {
      ASSERT_NOK(writer.Fetch("SELECT 1/0"));
    }
    const auto last_statement_end = MonoTime::Now();

    const auto before_fold = ASSERT_RESULT(SumCalls(&reader, kInsertWhere));
    const auto elapsed = MonoTime::Now() - start;
    if (elapsed.ToMilliseconds() < kIdleIntervalMs) {
      // No fold can have happened yet: at most the first execution reached the shared entry.
      ASSERT_LE(before_fold, 1);
    } else {
      LOG(INFO) << "Skipping the check before the fold: the writer took "
                << elapsed.ToMilliseconds() << " ms";
    }

    ASSERT_OK(WaitForIdleFold(&reader, kExecutions + 1, last_statement_end));
    ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), kExecutions + 1);
    ASSERT_EQ(ASSERT_RESULT(BackendState(&reader, writer_pid)), idle_state);
  }

  // The writer holds pending statistics at interval 60000 when the given idle timeout ends its
  // session; its exit fold must keep them.
  void TestIdleTimeoutExitFolds(bool begin, const std::string& set_timeout) {
    constexpr int kExecutions = 10;
    auto writer = ASSERT_RESULT(Connect());
    auto reader = ASSERT_RESULT(Connect());
    ASSERT_OK(writer.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
    ASSERT_OK(writer.Fetch("SELECT pg_stat_statements_reset()"));
    const auto writer_pid = ASSERT_RESULT(BackendPid(&writer));
    ASSERT_OK(writer.Execute(kSetLongInterval));
    if (begin) {
      ASSERT_OK(writer.Execute("BEGIN"));
    }
    for (int i = 0; i <= kExecutions; ++i) {
      ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", i));
    }
    // At most the first execution can have reached the shared entry.
    ASSERT_LE(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), 1);

    // Set last, so the timeout cannot end the session before the check above.
    ASSERT_OK(writer.Execute(set_timeout));
    ASSERT_OK(WaitForBackendsGone(&reader, {writer_pid}));
    ASSERT_NOK(writer.Fetch("SELECT 1"));

    ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), kExecutions + 1);
  }

  static constexpr auto kInsertWhere = "query LIKE 'INSERT INTO test_table%'";
  static constexpr auto kSetLongInterval = "SET pg_stat_statements.yb_flush_interval = 60000";
  static constexpr int kIdleIntervalMs = 1000;
};

class PgStatStatementsMaxTest : public PgStatStatementsTest {
 public:
  void UpdateMiniClusterOptions(ExternalMiniClusterOptions* options) override {
    PgStatStatementsTest::UpdateMiniClusterOptions(options);
    AppendCsvFlagValue(
        options->extra_tserver_flags, "ysql_pg_conf_csv", "pg_stat_statements.max=100");
  }
};

class PgStatStatementsQueryDiagnosticsTest : public PgStatStatementsTest {
 public:
  void UpdateMiniClusterOptions(ExternalMiniClusterOptions* options) override {
    PgStatStatementsTest::UpdateMiniClusterOptions(options);
    options->extra_tserver_flags.push_back("--ysql_yb_enable_query_diagnostics=true");
  }
};

// Verify that pg_stat_statements never stores unnormalized query text
// when a pgss entry is reset between pgss_post_parse_analyze and
// pgss_ExecutorEnd.
TEST_F(PgStatStatementsTest, NormalizationAfterReset) {
  auto conn1 = ASSERT_RESULT(Connect());
  auto conn2 = ASSERT_RESULT(Connect());

  ASSERT_OK(conn1.Fetch("SELECT pg_stat_statements_reset()"));

  TestThreadHolder thread_holder;
  thread_holder.AddThreadFunctor([&conn2] {
    ASSERT_OK(conn2.Fetch("SELECT pg_sleep(5), 'secret'"));
  });

  ASSERT_OK(LoggedWaitFor(
      [&conn1]() -> Result<bool> {
        auto n = VERIFY_RESULT(conn1.FetchRow<int64_t>(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE state = 'active' AND query LIKE '%secret%' "
            "AND query NOT LIKE '%pg_stat_activity%'"));
        return n > 0;
      },
      10s, "waiting for slow query to start"));

  ASSERT_OK(conn1.Fetch("SELECT pg_stat_statements_reset()"));

  thread_holder.JoinAll();

  // No unnormalized entry should be present.
  auto rows = ASSERT_RESULT(conn1.FetchRows<std::string>(
      "SELECT query FROM pg_stat_statements "
      "WHERE query LIKE '%secret%'"));
  ASSERT_TRUE(rows.empty())
      << "pg_stat_statements contains an unnormalized entry: " << rows[0];
}

// Verify the fix also works with the extended query protocol, where the second
// and subsequent executions of a named prepared statement skip the Parse message.
// After a pgss reset, the Parse-created entry is gone but the queryId is still
// present in yb_pgss_queries_needing_normalization from the original Parse.
// pgss_store must not fall through to storing unnormalized text.
TEST_F(PgStatStatementsTest, NormalizationAfterResetExtendedProtocol) {
  auto conn1 = ASSERT_RESULT(Connect());
  ASSERT_OK(conn1.Fetch("SELECT pg_stat_statements_reset()"));

  auto conn_str = Format("host=$0 port=$1 user=$2",
                         pg_ts->bind_host(), pg_ts->ysql_port(),
                         PGConnSettings::kDefaultUser);
  PGConnPtr raw_conn(PQconnectdb(conn_str.c_str()));
  ASSERT_EQ(PQstatus(raw_conn.get()), CONNECTION_OK);

  // Parse message: pgss_post_parse_analyze creates a normalized entry and
  // records the queryId in yb_pgss_queries_needing_normalization.
  PGResultPtr prep_result(PQprepare(
      raw_conn.get(), "test_stmt", "SELECT 1, 'secret'", 0, nullptr));
  ASSERT_EQ(PQresultStatus(prep_result.get()), PGRES_COMMAND_OK);

  // First Bind + Execute (no new Parse).
  PGResultPtr exec1(PQexecPrepared(
      raw_conn.get(), "test_stmt", 0, nullptr, nullptr, nullptr, 0));
  ASSERT_EQ(PQresultStatus(exec1.get()), PGRES_TUPLES_OK);

  ASSERT_OK(conn1.Fetch("SELECT pg_stat_statements_reset()"));

  // Second Bind + Execute: Parse is skipped, so pgss_post_parse_analyze does
  // not run.  The queryId is still in yb_pgss_queries_needing_normalization from the
  // original Parse, but jstate is NULL.  The fix must discard the stats
  // rather than storing the unnormalized source text.
  PGResultPtr exec2(PQexecPrepared(
      raw_conn.get(), "test_stmt", 0, nullptr, nullptr, nullptr, 0));
  ASSERT_EQ(PQresultStatus(exec2.get()), PGRES_TUPLES_OK);

  auto rows = ASSERT_RESULT(conn1.FetchRows<std::string>(
      "SELECT query FROM pg_stat_statements "
      "WHERE query LIKE '%secret%'"));
  ASSERT_TRUE(rows.empty())
      << "pg_stat_statements contains an unnormalized entry: " << rows[0];
}

// Test interleaved Parse/Execute of multiple prepared statements with different
// normalization requirements.  The per-queryId hash set
// (yb_pgss_queries_needing_normalization) must correctly track each query independently
// so that after a pgss reset:
//   - Queries that needed normalization are discarded (not stored with raw
//     literal text).
//   - Queries that never needed normalization are stored normally.
TEST_F(PgStatStatementsTest, InterleavedExtendedProtocolAfterReset) {
  auto ctrl = ASSERT_RESULT(Connect());
  auto conn_str = Format("host=$0 port=$1 user=$2",
                         pg_ts->bind_host(), pg_ts->ysql_port(),
                         PGConnSettings::kDefaultUser);

  // ---------------------------------------------------------------------------
  // Scenario A: Parse [plain, norm], reset, execute plain.
  // The plain query has no constants and must be stored after reset.
  // A single-bool implementation would see the bool as true (set by norm's
  // Parse) and incorrectly discard the plain query's stats.
  // ---------------------------------------------------------------------------
  {
    ASSERT_OK(ctrl.Fetch("SELECT pg_stat_statements_reset()"));
    PGConnPtr conn(PQconnectdb(conn_str.c_str()));
    ASSERT_EQ(PQstatus(conn.get()), CONNECTION_OK);

    PGResultPtr p1(PQprepare(
        conn.get(), "plain", "SELECT version()", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p1.get()), PGRES_COMMAND_OK);

    PGResultPtr p2(PQprepare(
        conn.get(), "norm", "SELECT 42, 'scenA_secret'", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p2.get()), PGRES_COMMAND_OK);

    // Initial execution to populate pgss entries.
    PGResultPtr e1(PQexecPrepared(
        conn.get(), "plain", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(e1.get()), PGRES_TUPLES_OK);
    PGResultPtr e2(PQexecPrepared(
        conn.get(), "norm", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(e2.get()), PGRES_TUPLES_OK);

    ASSERT_OK(ctrl.Fetch("SELECT pg_stat_statements_reset()"));

    // Re-execute plain (no new Parse, jstate is NULL, entry is missing).
    PGResultPtr re(PQexecPrepared(
        conn.get(), "plain", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(re.get()), PGRES_TUPLES_OK);

    auto rows = ASSERT_RESULT(ctrl.FetchRows<std::string>(
        "SELECT query FROM pg_stat_statements "
        "WHERE query LIKE '%version()%' "
        "AND query NOT LIKE '%pg_stat_statements%'"));
    ASSERT_FALSE(rows.empty())
        << "Scenario A: plain query should be stored after reset";
  }

  // ---------------------------------------------------------------------------
  // Scenario B: Parse [norm, plain], reset, execute norm.
  // The normalized query must NOT appear with unnormalized literal text.
  // ---------------------------------------------------------------------------
  {
    ASSERT_OK(ctrl.Fetch("SELECT pg_stat_statements_reset()"));
    PGConnPtr conn(PQconnectdb(conn_str.c_str()));
    ASSERT_EQ(PQstatus(conn.get()), CONNECTION_OK);

    PGResultPtr p1(PQprepare(
        conn.get(), "norm", "SELECT 42, 'scenB_secret'", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p1.get()), PGRES_COMMAND_OK);

    PGResultPtr p2(PQprepare(
        conn.get(), "plain", "SELECT version()", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p2.get()), PGRES_COMMAND_OK);

    PGResultPtr e1(PQexecPrepared(
        conn.get(), "norm", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(e1.get()), PGRES_TUPLES_OK);
    PGResultPtr e2(PQexecPrepared(
        conn.get(), "plain", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(e2.get()), PGRES_TUPLES_OK);

    ASSERT_OK(ctrl.Fetch("SELECT pg_stat_statements_reset()"));

    PGResultPtr re(PQexecPrepared(
        conn.get(), "norm", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(re.get()), PGRES_TUPLES_OK);

    auto rows = ASSERT_RESULT(ctrl.FetchRows<std::string>(
        "SELECT query FROM pg_stat_statements "
        "WHERE query LIKE '%scenB_secret%'"));
    ASSERT_TRUE(rows.empty())
        << "Scenario B: normalized query must not be stored with "
        << "unnormalized text: " << rows[0];
  }

  // ---------------------------------------------------------------------------
  // Scenario C: Parse [norm1, plain, norm2], reset, execute all three in a
  // scrambled order (norm2, plain, norm1).
  // Both normalized queries must be discarded; the plain query must be stored.
  // ---------------------------------------------------------------------------
  {
    ASSERT_OK(ctrl.Fetch("SELECT pg_stat_statements_reset()"));
    PGConnPtr conn(PQconnectdb(conn_str.c_str()));
    ASSERT_EQ(PQstatus(conn.get()), CONNECTION_OK);

    PGResultPtr p1(PQprepare(
        conn.get(), "norm1", "SELECT 42, 'scenC_secret1'", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p1.get()), PGRES_COMMAND_OK);

    PGResultPtr p2(PQprepare(
        conn.get(), "plain", "SELECT current_database()", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p2.get()), PGRES_COMMAND_OK);

    PGResultPtr p3(PQprepare(
        conn.get(), "norm2", "SELECT length('scenC_secret2')", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p3.get()), PGRES_COMMAND_OK);

    PGResultPtr e1(PQexecPrepared(
        conn.get(), "norm1", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(e1.get()), PGRES_TUPLES_OK);
    PGResultPtr e2(PQexecPrepared(
        conn.get(), "plain", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(e2.get()), PGRES_TUPLES_OK);
    PGResultPtr e3(PQexecPrepared(
        conn.get(), "norm2", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(e3.get()), PGRES_TUPLES_OK);

    ASSERT_OK(ctrl.Fetch("SELECT pg_stat_statements_reset()"));

    // Execute in a different order than they were parsed.
    PGResultPtr re3(PQexecPrepared(
        conn.get(), "norm2", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(re3.get()), PGRES_TUPLES_OK);
    PGResultPtr re2(PQexecPrepared(
        conn.get(), "plain", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(re2.get()), PGRES_TUPLES_OK);
    PGResultPtr re1(PQexecPrepared(
        conn.get(), "norm1", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(re1.get()), PGRES_TUPLES_OK);

    auto rows1 = ASSERT_RESULT(ctrl.FetchRows<std::string>(
        "SELECT query FROM pg_stat_statements "
        "WHERE query LIKE '%scenC_secret1%'"));
    ASSERT_TRUE(rows1.empty())
        << "Scenario C: norm1 must not be stored with unnormalized text: "
        << rows1[0];

    auto rows2 = ASSERT_RESULT(ctrl.FetchRows<std::string>(
        "SELECT query FROM pg_stat_statements "
        "WHERE query LIKE '%scenC_secret2%'"));
    ASSERT_TRUE(rows2.empty())
        << "Scenario C: norm2 must not be stored with unnormalized text: "
        << rows2[0];

    auto rows_p = ASSERT_RESULT(ctrl.FetchRows<std::string>(
        "SELECT query FROM pg_stat_statements "
        "WHERE query LIKE '%current_database()%' "
        "AND query NOT LIKE '%pg_stat_statements%'"));
    ASSERT_FALSE(rows_p.empty())
        << "Scenario C: plain query should be stored after reset";
  }

  // ---------------------------------------------------------------------------
  // Scenario D: Repeated resets with the same prepared statement.
  // The per-queryId tracking must survive across multiple pgss resets within
  // one session: every re-execution after reset must discard stats.
  // ---------------------------------------------------------------------------
  {
    ASSERT_OK(ctrl.Fetch("SELECT pg_stat_statements_reset()"));
    PGConnPtr conn(PQconnectdb(conn_str.c_str()));
    ASSERT_EQ(PQstatus(conn.get()), CONNECTION_OK);

    PGResultPtr p(PQprepare(
        conn.get(), "stmt", "SELECT 42, 'scenD_secret'", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p.get()), PGRES_COMMAND_OK);

    PGResultPtr e(PQexecPrepared(
        conn.get(), "stmt", 0, nullptr, nullptr, nullptr, 0));
    ASSERT_EQ(PQresultStatus(e.get()), PGRES_TUPLES_OK);

    for (int i = 0; i < 3; ++i) {
      ASSERT_OK(ctrl.Fetch("SELECT pg_stat_statements_reset()"));

      PGResultPtr re(PQexecPrepared(
          conn.get(), "stmt", 0, nullptr, nullptr, nullptr, 0));
      ASSERT_EQ(PQresultStatus(re.get()), PGRES_TUPLES_OK);

      auto rows = ASSERT_RESULT(ctrl.FetchRows<std::string>(
          "SELECT query FROM pg_stat_statements "
          "WHERE query LIKE '%scenD_secret%'"));
      ASSERT_TRUE(rows.empty())
          << "Scenario D iteration " << i
          << ": normalized query must not be stored with unnormalized text: "
          << rows[0];
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario E: Interleave two normalized queries with a plain query, reset,
  // then execute each one multiple times.  Verifies that repeated executions
  // of multiple prepared statements all remain correct after reset.
  // ---------------------------------------------------------------------------
  {
    ASSERT_OK(ctrl.Fetch("SELECT pg_stat_statements_reset()"));
    PGConnPtr conn(PQconnectdb(conn_str.c_str()));
    ASSERT_EQ(PQstatus(conn.get()), CONNECTION_OK);

    PGResultPtr p1(PQprepare(
        conn.get(), "n1", "SELECT 42, 'scenE_secret1'", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p1.get()), PGRES_COMMAND_OK);
    PGResultPtr p2(PQprepare(
        conn.get(), "n2", "SELECT length('scenE_secret2')", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p2.get()), PGRES_COMMAND_OK);
    PGResultPtr p3(PQprepare(
        conn.get(), "p1", "SELECT pg_backend_pid()", 0, nullptr));
    ASSERT_EQ(PQresultStatus(p3.get()), PGRES_COMMAND_OK);

    // Initial execution.
    for (const char *name : {"n1", "n2", "p1"}) {
      PGResultPtr e(PQexecPrepared(
          conn.get(), name, 0, nullptr, nullptr, nullptr, 0));
      ASSERT_EQ(PQresultStatus(e.get()), PGRES_TUPLES_OK);
    }

    ASSERT_OK(ctrl.Fetch("SELECT pg_stat_statements_reset()"));

    // Execute each statement three times in a round-robin pattern.
    for (int round = 0; round < 3; ++round) {
      for (const char *name : {"p1", "n2", "n1"}) {
        PGResultPtr re(PQexecPrepared(
            conn.get(), name, 0, nullptr, nullptr, nullptr, 0));
        ASSERT_EQ(PQresultStatus(re.get()), PGRES_TUPLES_OK);
      }
    }

    auto rows1 = ASSERT_RESULT(ctrl.FetchRows<std::string>(
        "SELECT query FROM pg_stat_statements "
        "WHERE query LIKE '%scenE_secret1%'"));
    ASSERT_TRUE(rows1.empty())
        << "Scenario E: n1 must not be stored with unnormalized text: "
        << rows1[0];

    auto rows2 = ASSERT_RESULT(ctrl.FetchRows<std::string>(
        "SELECT query FROM pg_stat_statements "
        "WHERE query LIKE '%scenE_secret2%'"));
    ASSERT_TRUE(rows2.empty())
        << "Scenario E: n2 must not be stored with unnormalized text: "
        << rows2[0];

    auto rows_p = ASSERT_RESULT(ctrl.FetchRows<std::string>(
        "SELECT query FROM pg_stat_statements "
        "WHERE query LIKE '%pg_backend_pid()%' "
        "AND query NOT LIKE '%pg_stat_statements%'"));
    ASSERT_FALSE(rows_p.empty())
        << "Scenario E: plain query should be stored after reset";
  }
}

// Verify that a statement prepared while it is not tracked is not stored with
// its literals when it is executed later at top level, where it is tracked.
TEST_F(PgStatStatementsTest, PreparedWhileUntrackedNotLeaked) {
  auto conn1 = ASSERT_RESULT(Connect());
  auto conn2 = ASSERT_RESULT(Connect());
  // Track only top-level statements, so nested statements are not tracked.
  ASSERT_OK(conn2.Execute("SET pg_stat_statements.track = 'top'"));

  ASSERT_OK(conn1.Execute("CREATE TABLE vault (v text)"));
  ASSERT_OK(conn1.Execute("INSERT INTO vault VALUES ('secret_1')"));
  ASSERT_OK(conn1.Fetch("SELECT pg_stat_statements_reset()"));

  // Each statement below has a different shape, so it gets its own queryId.
  // With a shared queryId, the first statement's entry or mark would also cover
  // the later ones, and they would not test their own path.

  // Prepared inside a DO block, which is nested and so not tracked.  The value
  // comes from the table, so it is not in the DO statement's own text.
  ASSERT_OK(conn2.Execute(
      "DO $$ BEGIN EXECUTE format('PREPARE stmt1 AS SELECT %L', "
      "(SELECT v FROM vault)); END $$"));
  ASSERT_OK(conn2.Fetch("EXECUTE stmt1"));

  // Prepared while tracking is off.
  ASSERT_OK(conn2.Execute("SET pg_stat_statements.track = 'none'"));
  ASSERT_OK(conn2.Execute("PREPARE stmt2 AS SELECT length('secret_2')"));
  ASSERT_OK(conn2.Execute("SET pg_stat_statements.track = 'top'"));
  ASSERT_OK(conn2.Fetch("EXECUTE stmt2"));

  // Prepared with Parse while tracking is off, then run with Bind + Execute,
  // as drivers do.
  auto conn_str = Format("host=$0 port=$1 user=$2",
                         pg_ts->bind_host(), pg_ts->ysql_port(),
                         PGConnSettings::kDefaultUser);
  PGConnPtr raw_conn(PQconnectdb(conn_str.c_str()));
  ASSERT_EQ(PQstatus(raw_conn.get()), CONNECTION_OK);

  PGResultPtr set_none(PQexec(raw_conn.get(), "SET pg_stat_statements.track = 'none'"));
  ASSERT_EQ(PQresultStatus(set_none.get()), PGRES_COMMAND_OK);
  PGResultPtr prep_result(PQprepare(
      raw_conn.get(), "stmt3", "SELECT upper('secret_3')", 0, nullptr));
  ASSERT_EQ(PQresultStatus(prep_result.get()), PGRES_COMMAND_OK);
  PGResultPtr set_top(PQexec(raw_conn.get(), "SET pg_stat_statements.track = 'top'"));
  ASSERT_EQ(PQresultStatus(set_top.get()), PGRES_COMMAND_OK);
  PGResultPtr exec_result(PQexecPrepared(
      raw_conn.get(), "stmt3", 0, nullptr, nullptr, nullptr, 0));
  ASSERT_EQ(PQresultStatus(exec_result.get()), PGRES_TUPLES_OK);

  // No unnormalized entry should be present.
  auto rows = ASSERT_RESULT(conn1.FetchRows<std::string>(
      "SELECT query FROM pg_stat_statements "
      "WHERE query LIKE '%secret%'"));
  ASSERT_TRUE(rows.empty())
      << "pg_stat_statements contains an unnormalized entry: " << rows[0];
}

// Verify that a statement inside a function is not stored with its literals
// when its plan, cached while nested statements were not tracked, runs again
// after they are.
TEST_F(PgStatStatementsTest, FunctionPlanCachedWhileUntrackedNotLeaked) {
  auto conn1 = ASSERT_RESULT(Connect());
  auto conn2 = ASSERT_RESULT(Connect());
  // Track only top-level statements, so nested statements are not tracked.
  ASSERT_OK(conn2.Execute("SET pg_stat_statements.track = 'top'"));

  ASSERT_OK(conn1.Execute("CREATE TABLE test_table (a text)"));
  ASSERT_OK(conn1.Execute("INSERT INTO test_table VALUES ('x')"));
  ASSERT_OK(conn1.Execute(
      "CREATE FUNCTION test_func() RETURNS text LANGUAGE plpgsql AS $$ "
      "DECLARE v text; "
      "BEGIN SELECT a INTO v FROM test_table WHERE a <> 'secret' LIMIT 1; "
      "RETURN v; END $$"));
  ASSERT_OK(conn1.Fetch("SELECT pg_stat_statements_reset()"));

  // Only top-level statements are tracked, so the statement inside the
  // function is parsed and cached without being tracked.
  ASSERT_OK(conn2.Fetch("SELECT test_func()"));

  // Track nested statements too.  The cached plan runs without a new parse.
  ASSERT_OK(conn2.Execute("SET pg_stat_statements.track = 'all'"));
  ASSERT_OK(conn2.Fetch("SELECT test_func()"));
  ASSERT_OK(conn2.Execute("RESET pg_stat_statements.track"));

  // No unnormalized entry should be present.
  auto rows = ASSERT_RESULT(conn1.FetchRows<std::string>(
      "SELECT query FROM pg_stat_statements "
      "WHERE query LIKE '%secret%'"));
  ASSERT_TRUE(rows.empty())
      << "pg_stat_statements contains an unnormalized entry: " << rows[0];

  // The top-level calls are still recorded.  conn2 reads them, as its last execution may still be
  // pending in its backend; its read folds it.
  auto calls = ASSERT_RESULT(conn2.FetchRow<int64_t>(
      "SELECT calls FROM pg_stat_statements WHERE query = 'SELECT test_func()'"));
  ASSERT_EQ(calls, 2);
}

// Verify that a prepared statement is not stored with its literals when a
// table change makes it be parsed again as a nested, untracked statement, and
// it is then executed at top level.
TEST_F(PgStatStatementsTest, ReparsedWhileUntrackedNotLeaked) {
  auto conn1 = ASSERT_RESULT(Connect());
  auto conn2 = ASSERT_RESULT(Connect());
  // Track only top-level statements, so nested statements are not tracked.
  ASSERT_OK(conn2.Execute("SET pg_stat_statements.track = 'top'"));

  ASSERT_OK(conn1.Execute("CREATE TABLE test_table (a text)"));
  ASSERT_OK(conn1.Execute("INSERT INTO test_table VALUES ('424242')"));
  ASSERT_OK(conn1.Fetch("SELECT pg_stat_statements_reset()"));

  ASSERT_OK(conn2.Execute(
      "PREPARE test_stmt AS SELECT count(*) FROM test_table WHERE a = '424242'"));
  ASSERT_OK(conn2.Fetch("EXECUTE test_stmt"));

  // The new column type changes the statement's shape.  EXPLAIN parses it again
  // as a nested statement, which is not tracked.
  ASSERT_OK(conn2.Execute(
      "ALTER TABLE test_table ALTER COLUMN a TYPE int USING a::int"));
  ASSERT_OK(conn2.Fetch("EXPLAIN (COSTS OFF) EXECUTE test_stmt"));
  ASSERT_OK(conn2.Fetch("EXECUTE test_stmt"));

  // No unnormalized entry should be present.
  auto rows = ASSERT_RESULT(conn1.FetchRows<std::string>(
      "SELECT query FROM pg_stat_statements "
      "WHERE query LIKE '%424242%'"));
  ASSERT_TRUE(rows.empty())
      << "pg_stat_statements contains an unnormalized entry: " << rows[0];

  // The execution before the table change is recorded with normalized text.  conn2 reads it, as
  // that execution may still be pending in its backend; its read folds it.
  rows = ASSERT_RESULT(conn2.FetchRows<std::string>(
      "SELECT query || ' calls=' || calls FROM pg_stat_statements "
      "WHERE query LIKE 'PREPARE test_stmt%'"));
  ASSERT_EQ(rows, std::vector<std::string>{
      "PREPARE test_stmt AS SELECT count(*) FROM test_table WHERE a = $1 calls=1"});
}


// Concurrent sessions on one node, half with the simple protocol and half with a prepared
// statement: once they have all exited, their exit folds make every count exact.
TEST_F(PgStatStatementsTest, ConcurrentCallsExactAfterDisconnect) {
  constexpr int kSessions = 8;
  constexpr int kExecutions = 200;
  constexpr int64_t kTotal = kSessions * kExecutions;

  auto reader = ASSERT_RESULT(Connect());
  ASSERT_OK(reader.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(reader.Fetch("SELECT pg_stat_statements_reset()"));

  std::vector<int32_t> pids(kSessions);
  TestThreadHolder thread_holder;
  for (int s = 0; s < kSessions; ++s) {
    thread_holder.AddThreadFunctor([this, s, &pids] {
      auto conn = ASSERT_RESULT(Connect());
      pids[s] = ASSERT_RESULT(BackendPid(&conn));
      const bool prepared = s % 2 == 1;
      if (prepared) {
        ASSERT_OK(conn.Prepare(
            "ins", "INSERT INTO test_table (k, v) VALUES ($1, 1) RETURNING k"));
      }
      for (int i = 0; i < kExecutions; ++i) {
        const auto k = std::to_string(s * kExecutions + i);
        if (prepared) {
          ASSERT_OK(conn.FetchPrepared("ins", {k.c_str()}));
        } else {
          ASSERT_OK(conn.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", k));
        }
      }
    });
  }
  thread_holder.JoinAll();
  ASSERT_OK(WaitForBackendsGone(&reader, pids));

  ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), kTotal);
  ASSERT_EQ(ASSERT_RESULT(SumRows(&reader, kInsertWhere)), kTotal);
  ASSERT_EQ(ASSERT_RESULT(SumHistogram(&reader, kInsertWhere)), kTotal);
}

// A session's own reads see its statements at once; another session's do not.
TEST_F(PgStatStatementsTest, ReadYourOwnWrites) {
  constexpr int kExecutions = 10;
  auto writer = ASSERT_RESULT(Connect());
  auto other = ASSERT_RESULT(Connect());
  ASSERT_OK(writer.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(writer.Fetch("SELECT pg_stat_statements_reset()"));
  ASSERT_OK(writer.Execute(kSetLongInterval));

  for (int i = 0; i <= kExecutions; ++i) {
    ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", i));
  }

  // At most the first execution can have reached the shared entry.
  ASSERT_LE(ASSERT_RESULT(SumCalls(&other, kInsertWhere)), 1);
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&writer, kInsertWhere)), kExecutions + 1);
}

// A full reset discards statistics another backend holds for the removed entries.
TEST_F(PgStatStatementsTest, FullResetDiscardsPending) {
  constexpr int kBeforeReset = 10;
  constexpr int kAfterReset = 5;
  auto other = ASSERT_RESULT(Connect());
  ASSERT_OK(other.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(other.Fetch("SELECT pg_stat_statements_reset()"));

  int32_t writer_pid = 0;
  {
    auto writer = ASSERT_RESULT(Connect());
    writer_pid = ASSERT_RESULT(BackendPid(&writer));
    ASSERT_OK(writer.Execute(kSetLongInterval));
    int k = 0;
    // The first execution warms the key; the rest are pending in the writer.
    for (int i = 0; i <= kBeforeReset; ++i) {
      ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", k++));
    }
    ASSERT_OK(other.Fetch("SELECT pg_stat_statements_reset()"));
    for (int i = 0; i < kAfterReset; ++i) {
      ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", k++));
    }
  }
  ASSERT_OK(WaitForBackendsGone(&other, {writer_pid}));

  ASSERT_EQ(ASSERT_RESULT(SumCalls(&other, kInsertWhere)), kAfterReset);
}

// A reset of one queryid discards pending statistics for it and keeps those of other keys.
TEST_F(PgStatStatementsTest, PartialResetDiscardsPending) {
  constexpr int kBeforeReset = 10;
  constexpr int kAfterReset = 5;
  constexpr auto kSelectWhere = "query LIKE 'SELECT v FROM test_table%'";
  auto other = ASSERT_RESULT(Connect());
  ASSERT_OK(other.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(other.Fetch("SELECT pg_stat_statements_reset()"));

  int32_t writer_pid = 0;
  {
    auto writer = ASSERT_RESULT(Connect());
    writer_pid = ASSERT_RESULT(BackendPid(&writer));
    ASSERT_OK(writer.Execute(kSetLongInterval));
    int k = 0;
    for (int i = 0; i <= kBeforeReset; ++i) {
      ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", k++));
      ASSERT_OK(writer.FetchFormat("SELECT v FROM test_table WHERE k = $0", i));
    }
    const auto insert_queryid = ASSERT_RESULT(other.FetchRow<int64_t>(Format(
        "SELECT queryid FROM pg_stat_statements WHERE $0", kInsertWhere)));
    ASSERT_OK(other.FetchFormat("SELECT pg_stat_statements_reset(0, 0, $0)", insert_queryid));
    for (int i = 0; i < kAfterReset; ++i) {
      ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", k++));
    }
  }
  ASSERT_OK(WaitForBackendsGone(&other, {writer_pid}));

  ASSERT_EQ(ASSERT_RESULT(SumCalls(&other, kInsertWhere)), kAfterReset);
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&other, kSelectWhere)), kBeforeReset + 1);
}

// An entry recreated after a reset does not receive statistics another backend held for the
// entry the reset removed.
TEST_F(PgStatStatementsTest, RecreatedEntryDoesNotInheritPending) {
  constexpr int kBeforeReset = 10;
  constexpr int kAfterReset = 5;
  auto recreator = ASSERT_RESULT(Connect());
  ASSERT_OK(recreator.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(recreator.Fetch("SELECT pg_stat_statements_reset()"));

  int k = 0;
  int32_t writer_pid = 0;
  {
    auto writer = ASSERT_RESULT(Connect());
    writer_pid = ASSERT_RESULT(BackendPid(&writer));
    ASSERT_OK(writer.Execute(kSetLongInterval));
    for (int i = 0; i <= kBeforeReset; ++i) {
      ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", k++));
    }
    ASSERT_OK(recreator.Fetch("SELECT pg_stat_statements_reset()"));
    for (int i = 0; i < kAfterReset; ++i) {
      ASSERT_OK(recreator.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", k++));
    }
  }
  // The writer's exit fold finds the recreated entry and must drop what it holds.
  ASSERT_OK(WaitForBackendsGone(&recreator, {writer_pid}));

  // Some of the recreator's own executions may be pending; its read folds them.
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&recreator, kInsertWhere)), kAfterReset);
}

// Eviction discards statistics another backend holds for the evicted entry.
TEST_F_EX(PgStatStatementsTest, EvictionDropsPending, PgStatStatementsMaxTest) {
  constexpr int kExecutions = 10;
  constexpr int kMaxShapes = 300;
  auto evictor = ASSERT_RESULT(Connect());
  ASSERT_OK(evictor.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(evictor.Fetch("SELECT pg_stat_statements_reset()"));
  // Updated per statement, so the evictor's shapes gain usage at once and outrank the victim.
  ASSERT_OK(evictor.Execute("SET pg_stat_statements.yb_flush_interval = 0"));

  int32_t writer_pid = 0;
  int64_t victim_queryid = 0;
  {
    auto writer = ASSERT_RESULT(Connect());
    writer_pid = ASSERT_RESULT(BackendPid(&writer));
    ASSERT_OK(writer.Execute(kSetLongInterval));
    for (int i = 0; i <= kExecutions; ++i) {
      ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", i));
    }
    victim_queryid = ASSERT_RESULT(evictor.FetchRow<int64_t>(Format(
        "SELECT queryid FROM pg_stat_statements WHERE $0", kInsertWhere)));
    const auto victim_present = Format(
        "SELECT count(*) FROM pg_stat_statements WHERE queryid = $0", victim_queryid);

    bool evicted = false;
    for (int n = 1; n <= kMaxShapes && !evicted; ++n) {
      for (int r = 0; r < 3; ++r) {
        ASSERT_OK(evictor.Fetch(ShapeQuery(n)));
      }
      if (n % 10 == 0) {
        evicted = ASSERT_RESULT(evictor.FetchRow<int64_t>(victim_present)) == 0;
      }
    }
    ASSERT_TRUE(evicted) << "the writer's entry was never evicted";
    ASSERT_GT(ASSERT_RESULT(evictor.FetchRow<int64_t>(
        "SELECT dealloc FROM pg_stat_statements_info")), 0);
  }
  ASSERT_OK(WaitForBackendsGone(&evictor, {writer_pid}));

  // The writer's exit fold must not bring back the evicted entry or add to it.
  ASSERT_EQ(ASSERT_RESULT(evictor.FetchRow<int64_t>(Format(
      "SELECT count(*) FROM pg_stat_statements WHERE queryid = $0", victim_queryid))), 0);
}

// With track = 'all', top-level and nested statements are both folded exactly.
TEST_F(PgStatStatementsTest, TrackAllNestedCounts) {
  constexpr int kExecutions = 20;
  auto reader = ASSERT_RESULT(Connect());
  ASSERT_OK(reader.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(reader.Execute(
      "CREATE FUNCTION test_fn(n int) RETURNS void LANGUAGE plpgsql AS $$ "
      "BEGIN INSERT INTO test_table (k, v) VALUES (n, 1); END $$"));
  ASSERT_OK(reader.Fetch("SELECT pg_stat_statements_reset()"));

  int32_t writer_pid = 0;
  {
    auto writer = ASSERT_RESULT(Connect());
    writer_pid = ASSERT_RESULT(BackendPid(&writer));
    ASSERT_OK(writer.Execute(kSetLongInterval));
    ASSERT_OK(writer.Execute("SET pg_stat_statements.track = 'all'"));
    for (int i = 0; i < kExecutions; ++i) {
      ASSERT_OK(writer.FetchFormat("SELECT test_fn($0)", i));
    }
  }
  ASSERT_OK(WaitForBackendsGone(&reader, {writer_pid}));

  ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, "query LIKE 'SELECT test_fn%' AND toplevel")),
            kExecutions);
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, Format("$0 AND NOT toplevel", kInsertWhere))),
            kExecutions);
}

// At interval 0 every statement updates the shared entry before it returns.
TEST_F(PgStatStatementsTest, FlushIntervalZeroIsSynchronous) {
  constexpr int kExecutions = 10;
  auto writer = ASSERT_RESULT(Connect());
  auto reader = ASSERT_RESULT(Connect());
  ASSERT_OK(writer.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(writer.Fetch("SELECT pg_stat_statements_reset()"));
  ASSERT_OK(writer.Execute("SET pg_stat_statements.yb_flush_interval = 0"));

  ASSERT_OK(writer.Execute("INSERT INTO test_table (k, v) VALUES (0, 1)"));
  const auto baseline = ASSERT_RESULT(SumCalls(&reader, kInsertWhere));
  for (int i = 1; i <= kExecutions; ++i) {
    ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", i));
  }
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), baseline + kExecutions);
}

// Switching to interval 0 folds what the session still holds: the SET's own store runs at
// interval 0, and a store at interval 0 folds leftover pending statistics first.
TEST_F(PgStatStatementsTest, SwitchToZeroFoldsPending) {
  constexpr int kExecutions = 10;
  auto writer = ASSERT_RESULT(Connect());
  auto reader = ASSERT_RESULT(Connect());
  ASSERT_OK(writer.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(writer.Fetch("SELECT pg_stat_statements_reset()"));
  ASSERT_OK(writer.Execute(kSetLongInterval));

  for (int i = 0; i <= kExecutions; ++i) {
    ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", i));
  }
  // At most the first execution can have reached the shared entry.
  ASSERT_LE(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), 1);

  ASSERT_OK(writer.Execute("SET pg_stat_statements.yb_flush_interval = 0"));
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), kExecutions + 1);
}

// Planning statistics are folded with the executions they belong to.
TEST_F(PgStatStatementsTest, TrackPlanningCounts) {
  constexpr int kExecutions = 20;
  auto reader = ASSERT_RESULT(Connect());
  ASSERT_OK(reader.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(reader.Fetch("SELECT pg_stat_statements_reset()"));

  int32_t writer_pid = 0;
  {
    auto writer = ASSERT_RESULT(Connect());
    writer_pid = ASSERT_RESULT(BackendPid(&writer));
    ASSERT_OK(writer.Execute(kSetLongInterval));
    ASSERT_OK(writer.Execute("SET pg_stat_statements.track_planning = on"));
    // Each execution of an unnamed statement is planned with a custom plan.
    for (int i = 0; i < kExecutions; ++i) {
      ASSERT_OK(writer.FetchFormat("SELECT v FROM test_table WHERE k = $0", i));
    }
  }
  ASSERT_OK(WaitForBackendsGone(&reader, {writer_pid}));

  const auto [plans, calls] = ASSERT_RESULT((reader.FetchRow<int64_t, int64_t>(
      "SELECT plans, calls FROM pg_stat_statements WHERE query LIKE 'SELECT v FROM test_table%'")));
  ASSERT_EQ(calls, kExecutions);
  ASSERT_EQ(plans, kExecutions);
}

// Reaching the cap on pending keys folds everything, with the writer still connected.
TEST_F(PgStatStatementsTest, PendingCapFolds) {
  constexpr int kShapes = 65;
  auto writer = ASSERT_RESULT(Connect());
  auto reader = ASSERT_RESULT(Connect());
  ASSERT_OK(writer.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(writer.Fetch("SELECT pg_stat_statements_reset()"));
  ASSERT_OK(writer.Execute(kSetLongInterval));

  const auto first_shape = Format("query = '$0'", ShapeQuery(1));
  for (int n = 1; n <= kShapes; ++n) {
    ASSERT_OK(writer.Fetch(ShapeQuery(n)));
    ASSERT_OK(writer.Fetch(ShapeQuery(n)));
    if (n == 2) {
      // Far below the cap: the second execution of shape 1 is still pending.
      ASSERT_LT(ASSERT_RESULT(SumCalls(&reader, first_shape)), 2);
    }
  }
  // The writer's own SET statements also take keys, so the cap is crossed at some shape before
  // the last one.
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, first_shape)), 2);
}

// A session that stays idle after its statements folds them within the interval.
TEST_F(PgStatStatementsTest, IdleSessionFoldsWithinInterval) {
  ASSERT_NO_FATALS(TestIdleFoldsWithinInterval(
      /* begin = */ false, /* abort = */ false, "idle"));
}

// Idle in a transaction block folds too: pg_stat_statements is not transactional.
TEST_F(PgStatStatementsTest, IdleInTransactionFoldsWithinInterval) {
  ASSERT_NO_FATALS(TestIdleFoldsWithinInterval(
      /* begin = */ true, /* abort = */ false, "idle in transaction"));
}

// Idle in an aborted transaction block folds the statements that completed before the error. The
// error's recovery disarms every timeout, so this also covers arming it again.
TEST_F(PgStatStatementsTest, IdleInAbortedTransactionFoldsWithinInterval) {
  ASSERT_NO_FATALS(TestIdleFoldsWithinInterval(
      /* begin = */ true, /* abort = */ true, "idle in transaction (aborted)"));
}

// Going idle does not fold statistics younger than the interval.
TEST_F(PgStatStatementsTest, IdleDoesNotFoldBeforeInterval) {
  constexpr int kExecutions = 10;
  auto writer = ASSERT_RESULT(Connect());
  auto reader = ASSERT_RESULT(Connect());
  ASSERT_OK(writer.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(writer.Fetch("SELECT pg_stat_statements_reset()"));
  ASSERT_OK(writer.Execute(kSetLongInterval));

  ASSERT_OK(writer.Execute("INSERT INTO test_table (k, v) VALUES (0, 1)"));
  const auto baseline = ASSERT_RESULT(SumCalls(&reader, kInsertWhere));
  for (int i = 1; i <= kExecutions; ++i) {
    ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", i));
  }
  // The writer went idle after every statement above and stays idle meanwhile.
  SleepFor(3s * kTimeMultiplier);
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), baseline);

  // The executions were pending: the writer's own read folds them.
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&writer, kInsertWhere)), kExecutions + 1);
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), kExecutions + 1);
}

// Switching to interval 0 folds pending statistics at the next idle, even though no tracked
// statement follows: with track_utility off, the SET itself is not stored.
TEST_F(PgStatStatementsTest, SwitchToZeroWhileIdleFolds) {
  constexpr int kExecutions = 10;
  auto writer = ASSERT_RESULT(Connect());
  auto reader = ASSERT_RESULT(Connect());
  ASSERT_OK(writer.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
  ASSERT_OK(writer.Fetch("SELECT pg_stat_statements_reset()"));
  ASSERT_OK(writer.Execute("SET pg_stat_statements.track_utility = off"));
  ASSERT_OK(writer.Execute(kSetLongInterval));

  for (int i = 0; i <= kExecutions; ++i) {
    ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", i));
  }
  // At most the first execution can have reached the shared entry.
  ASSERT_LE(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), 1);

  ASSERT_OK(writer.Execute("SET pg_stat_statements.yb_flush_interval = 0"));
  ASSERT_OK(WaitForIdleFold(&reader, kExecutions + 1, MonoTime::Now()));
  ASSERT_EQ(ASSERT_RESULT(SumCalls(&reader, kInsertWhere)), kExecutions + 1);
}

// idle_session_timeout still ends a session that holds pending statistics, and they survive.
TEST_F(PgStatStatementsTest, IdleSessionTimeoutExitFolds) {
  ASSERT_NO_FATALS(TestIdleTimeoutExitFolds(
      /* begin = */ false, "SET idle_session_timeout = '2s'"));
}

// The same for idle_in_transaction_session_timeout.
TEST_F(PgStatStatementsTest, IdleInTransactionTimeoutExitFolds) {
  ASSERT_NO_FATALS(TestIdleTimeoutExitFolds(
      /* begin = */ true, "SET idle_in_transaction_session_timeout = '2s'"));
}

class PgStatStatementsRestartTest : public PgStatStatementsTest {
 protected:
  // Statistics of a statement executed once to warm its key and then kExecutions more times, in a
  // writer that is still connected at the restart when writer_connected_at_restart is set.
  void TestStatsSurviveRestart(bool writer_connected_at_restart) {
    constexpr int kExecutions = 10;
    std::optional<PGConn> writer;
    {
      auto reader = ASSERT_RESULT(Connect());
      ASSERT_OK(reader.Execute("CREATE TABLE test_table (k INT PRIMARY KEY, v INT)"));
      ASSERT_OK(reader.Fetch("SELECT pg_stat_statements_reset()"));

      writer.emplace(ASSERT_RESULT(Connect()));
      const auto writer_pid = ASSERT_RESULT(BackendPid(&*writer));
      if (writer_connected_at_restart) {
        // Only the exit fold at shutdown can add these to the saved statistics.
        ASSERT_OK(writer->Execute(kSetLongInterval));
      }
      for (int i = 0; i <= kExecutions; ++i) {
        ASSERT_OK(writer->ExecuteFormat("INSERT INTO test_table (k, v) VALUES ($0, 1)", i));
      }
      if (!writer_connected_at_restart) {
        writer.reset();
        ASSERT_OK(WaitForBackendsGone(&reader, {writer_pid}));
      }
    }

    // Killing the tablet server makes the postmaster do a fast shutdown, which terminates the
    // remaining backends and then saves the statistics.
    pg_ts->Shutdown();
    writer.reset();
    ASSERT_OK(pg_ts->Restart());

    auto conn = ASSERT_RESULT(Connect());
    ASSERT_EQ(ASSERT_RESULT(SumCalls(&conn, kInsertWhere)), kExecutions + 1);
  }
};

TEST_F_EX(PgStatStatementsTest, StatsSurviveRestart, PgStatStatementsRestartTest) {
  ASSERT_NO_FATALS(TestStatsSurviveRestart(/* writer_connected_at_restart = */ false));
}

TEST_F_EX(PgStatStatementsTest, StatsSurviveRestartWriterConnected, PgStatStatementsRestartTest) {
  ASSERT_NO_FATALS(TestStatsSurviveRestart(/* writer_connected_at_restart = */ true));
}

// A query diagnostics bundle gets the statement's text from pg_stat_statements even when every
// execution during the bundle was pending: the fold supplies it.
TEST_F_EX(
    PgStatStatementsTest, QueryDiagnosticsGetsPgssText, PgStatStatementsQueryDiagnosticsTest) {
  constexpr int kExecutions = 10;
  constexpr int kBundleSeconds = 5;
  constexpr auto kNormalizedInsert = "INSERT INTO test_table VALUES ($1)";
  auto reader = ASSERT_RESULT(Connect());
  ASSERT_OK(reader.Execute("CREATE TABLE test_table (k INT)"));
  ASSERT_OK(reader.Fetch("SELECT pg_stat_statements_reset()"));

  int32_t writer_pid = 0;
  int64_t queryid = 0;
  std::string bundle_path;
  {
    auto writer = ASSERT_RESULT(Connect());
    writer_pid = ASSERT_RESULT(BackendPid(&writer));
    ASSERT_OK(writer.Execute(kSetLongInterval));
    // Warm the key before the bundle starts, so no execution during the bundle is synchronous.
    ASSERT_OK(writer.Execute("INSERT INTO test_table VALUES (0)"));
    queryid = ASSERT_RESULT(reader.FetchRow<int64_t>(Format(
        "SELECT queryid FROM pg_stat_statements WHERE query = '$0'", kNormalizedInsert)));
    bundle_path = ASSERT_RESULT(reader.FetchRow<std::string>(Format(
        "SELECT yb_query_diagnostics(query_id => $0, diagnostics_interval_sec => $1)",
        queryid, kBundleSeconds)));
    for (int i = 1; i <= kExecutions; ++i) {
      ASSERT_OK(writer.ExecuteFormat("INSERT INTO test_table VALUES ($0)", i));
    }
  }
  // The writer's exit fold, within the bundle's interval, gives the bundle the text.
  ASSERT_OK(WaitForBackendsGone(&reader, {writer_pid}));

  ASSERT_OK(LoggedWaitFor(
      [&reader, queryid]() -> Result<bool> {
        auto statuses = VERIFY_RESULT(reader.FetchRows<std::string>(Format(
            "SELECT status FROM yb_query_diagnostics_status WHERE query_id = $0", queryid)));
        return !statuses.empty() && statuses[0] != "In Progress";
      },
      60s * kTimeMultiplier, "waiting for the query diagnostics bundle to complete"));

  std::ifstream pgss_file(JoinPathSegments(bundle_path, "pg_stat_statements.csv"));
  ASSERT_TRUE(pgss_file.good()) << "no pg_stat_statements.csv in " << bundle_path;
  std::string header;
  std::string line;
  ASSERT_TRUE(static_cast<bool>(std::getline(pgss_file, header)));
  ASSERT_TRUE(static_cast<bool>(std::getline(pgss_file, line)));
  std::vector<std::string> fields;
  std::stringstream line_stream(line);
  for (std::string field; std::getline(line_stream, field, ',');) {
    fields.push_back(field);
  }
  ASSERT_GE(fields.size(), 3U) << line;
  ASSERT_EQ(fields[1], Format("\"$0\"", kNormalizedInsert)) << line;
  ASSERT_EQ(fields[2], std::to_string(kExecutions)) << line;
}

}  // namespace pgwrapper
}  // namespace yb
