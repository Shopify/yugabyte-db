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

#include <string>

#include "yb/util/backoff_waiter.h"
#include "yb/util/result.h"
#include "yb/util/test_macros.h"

#include "yb/yql/pgwrapper/pg_mini_test_base.h"

DECLARE_bool(TEST_enable_schema_migration_admission);
DECLARE_bool(TEST_pause_schema_migration_in_running);

using namespace std::chrono_literals;

namespace yb::pgwrapper {

// SQL-level end-to-end coverage of the online-schema-change migration tracker
// (roadmap Section 0), exercising yb_start_online_schema_change /
// yb_schema_migrations / yb_cancel_schema_migration through the full
// Postgres -> pggate -> tserver -> master path.
class PgSchemaMigrationTest : public PgMiniTestBase {
 public:
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_enable_schema_migration_admission) = true;
    PgMiniTestBase::SetUp();
  }

 protected:
  Result<std::string> Start(PGConn* conn, const std::string& ddl, const char* request_id) {
    if (request_id) {
      return conn->FetchRow<std::string>(Format(
          "SELECT yb_start_online_schema_change('$0', '$1')", ddl, request_id));
    }
    return conn->FetchRow<std::string>(Format(
        "SELECT yb_start_online_schema_change('$0', NULL)", ddl));
  }

  Result<std::string> State(PGConn* conn, const std::string& id) {
    return conn->FetchRow<std::string>(Format(
        "SELECT state FROM yb_schema_migrations WHERE migration_id = '$0'", id));
  }

  Status WaitForState(PGConn* conn, const std::string& id, const std::string& want) {
    return WaitFor([this, conn, &id, &want]() -> Result<bool> {
      return VERIFY_RESULT(State(conn, id)) == want;
    }, 30s, Format("migration $0 reaching $1", id, want));
  }
};

// Start returns a resolvable id and the skeleton executor advances it to
// SUCCEEDED; the terminal row is retained and queryable by id.
TEST_F(PgSchemaMigrationTest, StartQueryAndSucceed) {
  auto conn = ASSERT_RESULT(Connect());
  auto id = ASSERT_RESULT(Start(&conn, "ALTER TABLE t ALTER COLUMN v TYPE bigint", nullptr));
  ASSERT_FALSE(id.empty());
  // Immediately queryable by id.
  ASSERT_OK(State(&conn, id));
  ASSERT_OK(WaitForState(&conn, id, "SUCCEEDED"));
  // The row carries the recorded kind and DDL.
  auto kind = ASSERT_RESULT(conn.FetchRow<std::string>(Format(
      "SELECT kind FROM yb_schema_migrations WHERE migration_id = '$0'", id)));
  ASSERT_EQ(kind, "ONLINE_TABLE_REWRITE");
}

// A duplicate request_id resolves to the same job (lost-response idempotency).
TEST_F(PgSchemaMigrationTest, RequestIdIdempotency) {
  auto conn = ASSERT_RESULT(Connect());
  auto id1 = ASSERT_RESULT(Start(&conn, "ALTER TABLE t ADD c int", "tok-1"));
  auto id2 = ASSERT_RESULT(Start(&conn, "ALTER TABLE t ADD c int", "tok-1"));
  ASSERT_EQ(id1, id2);
  auto count = ASSERT_RESULT(conn.FetchRow<int64_t>("SELECT count(*) FROM yb_schema_migrations"));
  ASSERT_EQ(count, 1);
}

// Cancel drives a paused RUNNING job to CANCELLED.
TEST_F(PgSchemaMigrationTest, Cancel) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_schema_migration_in_running) = true;
  auto conn = ASSERT_RESULT(Connect());
  auto id = ASSERT_RESULT(Start(&conn, "ALTER TABLE t ADD c int", nullptr));
  ASSERT_OK(WaitForState(&conn, id, "RUNNING"));
  ASSERT_TRUE(ASSERT_RESULT(conn.FetchRow<bool>(Format(
      "SELECT yb_cancel_schema_migration('$0')", id))));
  ASSERT_OK(WaitForState(&conn, id, "CANCELLED"));
}

// With admission disabled, start raises a clear error and admits nothing.
TEST_F(PgSchemaMigrationTest, AdmissionGate) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_enable_schema_migration_admission) = false;
  auto conn = ASSERT_RESULT(Connect());
  auto result = Start(&conn, "ALTER TABLE t ADD c int", nullptr);
  ASSERT_NOK(result);
  auto count = ASSERT_RESULT(conn.FetchRow<int64_t>("SELECT count(*) FROM yb_schema_migrations"));
  ASSERT_EQ(count, 0);
}

}  // namespace yb::pgwrapper
