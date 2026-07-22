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

#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/yb_table_test_base.h"

#include "yb/master/catalog_manager.h"
#include "yb/master/leader_epoch.h"
#include "yb/master/master.h"
#include "yb/master/mini_master.h"
#include "yb/master/schema_migration/schema_migration_manager.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/result.h"
#include "yb/util/test_macros.h"

DECLARE_bool(TEST_enable_schema_migration_admission);
DECLARE_bool(TEST_pause_schema_migration_in_running);

using namespace std::chrono_literals;

namespace yb::master {

namespace {
constexpr uint32_t kDatabaseOid = 16384;
constexpr uint32_t kTableOid = 16385;
constexpr uint32_t kSubmittedBy = 10;
const std::string kDdl = "ALTER TABLE t ALTER COLUMN v TYPE bigint";
}  // namespace

class SchemaMigrationManagerITest : public integration_tests::YBTableTestBase {
 public:
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_enable_schema_migration_admission) = true;
    integration_tests::YBTableTestBase::SetUp();
  }

  size_t num_masters() override { return 3; }
  size_t num_tablet_servers() override { return 1; }
  bool use_external_mini_cluster() override { return false; }

 protected:
  SchemaMigrationManager& manager() {
    return CHECK_RESULT(mini_cluster()->GetLeaderMiniMaster())->master()->schema_migration_manager();
  }

  LeaderEpoch epoch() {
    return CHECK_RESULT(mini_cluster()->GetLeaderMiniMaster())
        ->catalog_manager_impl()
        .GetLeaderEpochInternal();
  }

  Result<std::string> StartOne(const std::string& request_id = "") {
    return manager().StartSchemaMigration(
        SysSchemaMigrationEntryPB::ONLINE_TABLE_REWRITE, kDatabaseOid, kTableOid, kSubmittedBy,
        kDdl, request_id, epoch());
  }

  Result<SysSchemaMigrationEntryPB::State> GetState(const std::string& id) {
    return VERIFY_RESULT(manager().GetSchemaMigration(id)).state();
  }

  Status WaitForState(const std::string& id, SysSchemaMigrationEntryPB::State want) {
    return WaitFor([this, &id, want]() -> Result<bool> {
      return VERIFY_RESULT(GetState(id)) == want;
    }, 30s, Format("migration $0 reaching state $1", id, want));
  }
};

// Admission returns a resolvable id, and the skeleton executor drives the job to
// SUCCEEDED without touching any table. Terminal jobs remain queryable.
TEST_F(SchemaMigrationManagerITest, AdmitAndSucceed) {
  auto id = ASSERT_RESULT(StartOne());
  ASSERT_FALSE(id.empty());
  // Immediately resolvable right after admission (durable-before-return).
  ASSERT_OK(GetState(id));
  ASSERT_OK(WaitForState(id, SysSchemaMigrationEntryPB::SUCCEEDED));
  // Terminal job is retained and still queryable.
  ASSERT_OK(GetState(id));
}

// A duplicate request_id resolves to the same job instead of creating a second.
TEST_F(SchemaMigrationManagerITest, RequestIdIdempotency) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_schema_migration_in_running) = true;
  const std::string request_id = "client-token-1";
  auto id1 = ASSERT_RESULT(StartOne(request_id));
  auto id2 = ASSERT_RESULT(StartOne(request_id));
  ASSERT_EQ(id1, id2);
  ASSERT_EQ(manager().ListSchemaMigrations(std::nullopt).size(), 1);
}

// Cancel drives a paused job to CANCELLED and is rejected once terminal.
TEST_F(SchemaMigrationManagerITest, Cancel) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_schema_migration_in_running) = true;
  auto id = ASSERT_RESULT(StartOne());
  ASSERT_OK(WaitForState(id, SysSchemaMigrationEntryPB::RUNNING));
  ASSERT_OK(manager().CancelSchemaMigration(id, epoch()));
  ASSERT_OK(WaitForState(id, SysSchemaMigrationEntryPB::CANCELLED));
  // Cancelling a terminal job fails.
  ASSERT_NOK(manager().CancelSchemaMigration(id, epoch()));
}

// A job paused in RUNNING survives master-leader failover: it is reloaded on the
// new leader, keeps the same id, and (once unpaused) completes exactly once.
TEST_F(SchemaMigrationManagerITest, FailoverResumesRunningJob) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_schema_migration_in_running) = true;
  auto id = ASSERT_RESULT(StartOne());
  ASSERT_OK(WaitForState(id, SysSchemaMigrationEntryPB::RUNNING));

  // Step down the current leader to force a new leader to reload from sys.catalog.
  ASSERT_OK(mini_cluster()->StepDownMasterLeader());

  // Same id still resolvable on the new leader (GetLeaderMiniMaster waits for a
  // leader), and still non-terminal because it is paused in RUNNING.
  ASSERT_OK(WaitFor([this, &id]() -> Result<bool> {
    auto state_result = GetState(id);
    if (!state_result.ok()) {
      return false;
    }
    return *state_result == SysSchemaMigrationEntryPB::RUNNING ||
           *state_result == SysSchemaMigrationEntryPB::NEW;
  }, 30s, "new leader reloads the paused job"));

  // Unpause and confirm it completes.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_schema_migration_in_running) = false;
  ASSERT_OK(WaitForState(id, SysSchemaMigrationEntryPB::SUCCEEDED));
}

}  // namespace yb::master
