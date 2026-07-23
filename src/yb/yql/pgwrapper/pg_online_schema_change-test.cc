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
// Online schema change (OSC) roadmap: Step 1 validation.
//
// These tests pin the mechanic the async shadow-table roadmap relies on for its
// cutover: a table rewrite must preserve the relation's stable logical identity
// (pg_class.oid) while replacing its physical storage (pg_class.relfilenode ->
// a new DocDB table), and dependents (views, foreign keys) that reference the
// relation by OID must keep working afterwards.
//
// This is deliberately NOT a MySQL-style rename swap (which would change the
// OID and break OID-bound dependents). See
// architecture/design/online-schema-changes-async-shadow-roadmap.md.

#include <string>

#include <gtest/gtest.h>

#include "yb/common/entity_ids.h"

#include "yb/integration-tests/mini_cluster.h"

#include "yb/master/catalog_entity_info.h"
#include "yb/master/catalog_manager.h"
#include "yb/master/catalog_manager_if.h"
#include "yb/master/master.h"
#include "yb/master/master_ddl.pb.h"
#include "yb/master/mini_master.h"
#include "yb/master/schema_migration/schema_migration_manager.h"

#include "yb/tablet/tablet.h"
#include "yb/tablet/tablet_peer.h"

#include "yb/yql/pgwrapper/libpq_utils.h"
#include "yb/yql/pgwrapper/pg_mini_test_base.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/test_macros.h"

DECLARE_bool(TEST_enable_schema_migration_admission);
DECLARE_bool(TEST_schema_migration_create_shadow);
DECLARE_bool(TEST_schema_migration_stop_before_cutover);

using namespace std::chrono_literals;

namespace yb {
namespace pgwrapper {

class PgOnlineSchemaChangeTest : public PgMiniTestBase {
 public:
  size_t NumTabletServers() override { return 1; }

 protected:
  // Find the master TableInfo for a YSQL user table by name.
  Result<master::TableInfoPtr> FindUserTable(const std::string& relname) {
    auto* cm = VERIFY_RESULT(catalog_manager_impl());
    for (const auto& table : cm->GetTables(master::GetTablesMode::kAll)) {
      auto l = table->LockForRead();
      if (l->table_type() == PGSQL_TABLE_TYPE && l->name() == relname &&
          l->is_active_generation()) {
        return table;
      }
    }
    return STATUS_FORMAT(NotFound, "user table $0 not found", relname);
  }

  // Number of times relname appears in a ListTables result (client-facing view).
  Result<int> CountInListTables(const std::string& relname) {
    auto* cm = VERIFY_RESULT(catalog_manager_impl());
    master::ListTablesRequestPB req;
    master::ListTablesResponsePB resp;
    req.set_name_filter(relname);
    RETURN_NOT_OK(cm->ListTables(&req, &resp));
    int count = 0;
    for (const auto& t : resp.tables()) {
      if (t.name() == relname) {
        ++count;
      }
    }
    return count;
  }

  // Number of RUNNING tablets for a table id (across all tservers/replicas the
  // master knows about).
  Result<size_t> RunningTabletCount(const TableId& table_id) {
    auto* cm = VERIFY_RESULT(catalog_manager_impl());
    auto table = cm->GetTableInfo(table_id);
    SCHECK(table != nullptr, NotFound, "table not found");
    auto tablets = VERIFY_RESULT(table->GetTablets());
    size_t running = 0;
    for (const auto& t : tablets) {
      if (t->LockForRead()->pb.state() == master::SysTabletsEntryPB::RUNNING) {
        ++running;
      }
    }
    return running;
  }

  // Count regular-DB DocDB records across a table's tablet leaders (one leader
  // per tablet, so replicas are not double-counted). For a freshly loaded table
  // with no updates/deletes this equals the row count.
  Result<size_t> CountDbRecords(const TableId& table_id) {
    size_t total = 0;
    for (const auto& peer : ListTableActiveTabletLeadersPeers(cluster_.get(), table_id)) {
      auto tablet = VERIFY_RESULT(peer->shared_tablet());
      total += VERIFY_RESULT(tablet->TEST_CountDBRecords(docdb::StorageDbType::kRegular));
    }
    return total;
  }

  // Start a migration for `relname` and wait until it reaches SUCCEEDED (or fail
  // the test on FAILED). Returns the job PB.
  Result<master::SysSchemaMigrationEntryPB> RunMigrationToCompletion(
      PGConn* conn, const std::string& relname) {
    const auto table_oid = VERIFY_RESULT(conn->FetchRow<PGOid>(
        Format("SELECT '$0'::regclass::oid", relname)));
    const auto database_oid = VERIFY_RESULT(conn->FetchRow<PGOid>(
        "SELECT oid FROM pg_database WHERE datname = current_database()"));

    auto* mini_master = VERIFY_RESULT(cluster_->GetLeaderMiniMaster());
    auto& mgr = mini_master->master()->schema_migration_manager();
    auto epoch = mini_master->catalog_manager_impl().GetLeaderEpochInternal();

    auto migration_id = VERIFY_RESULT(mgr.StartSchemaMigration(
        master::SysSchemaMigrationEntryPB::ONLINE_TABLE_REWRITE, database_oid, table_oid,
        /* submitted_by */ 10, Format("ALTER TABLE $0 ADD c int", relname),
        /* request_id */ "", epoch));

    RETURN_NOT_OK(WaitFor([&]() -> Result<bool> {
      auto pb = VERIFY_RESULT(mgr.GetSchemaMigration(migration_id));
      if (pb.state() == master::SysSchemaMigrationEntryPB::FAILED) {
        return STATUS_FORMAT(IllegalState, "migration failed: $0",
                             StatusFromPB(pb.terminal_error()));
      }
      return pb.state() == master::SysSchemaMigrationEntryPB::SUCCEEDED;
    }, 360s, "migration reaches SUCCEEDED"));

    return mgr.GetSchemaMigration(migration_id);
  }
};

// Multi-tablet, multi-tserver (RF3) variant to exercise per-tablet snapshot +
// tablet clone + consensus-peer seeding across nodes.
class PgOnlineSchemaChangeRf3Test : public PgOnlineSchemaChangeTest {
 public:
  size_t NumTabletServers() override { return 3; }
};

// A rewrite that materializes existing rows (volatile default) must keep the
// same pg_class.oid but move to a new relfilenode (new DocDB storage).
TEST_F(PgOnlineSchemaChangeTest, SwapPreservesOid) {
  auto conn = ASSERT_RESULT(Connect());

  ASSERT_OK(conn.Execute("CREATE TABLE t (id int PRIMARY KEY, v text)"));
  ASSERT_OK(conn.Execute(
      "INSERT INTO t SELECT g, 'val_' || g FROM generate_series(1, 100) g"));

  const auto oid_before = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT oid FROM pg_class WHERE relname = 't'"));
  const auto relfilenode_before = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT relfilenode FROM pg_class WHERE relname = 't'"));
  // On a fresh table, relfilenode == oid.
  ASSERT_EQ(oid_before, relfilenode_before);

  // A volatile default forces a full table rewrite (new physical storage).
  ASSERT_OK(conn.Execute(
      "ALTER TABLE t ADD COLUMN r double precision DEFAULT random()"));

  const auto oid_after = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT oid FROM pg_class WHERE relname = 't'"));
  const auto relfilenode_after = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT relfilenode FROM pg_class WHERE relname = 't'"));

  // Logical identity preserved.
  ASSERT_EQ(oid_before, oid_after);
  // Physical storage switched.
  ASSERT_NE(relfilenode_before, relfilenode_after);

  // All existing rows are present and carry the new, materialized column.
  const auto row_count = ASSERT_RESULT(conn.FetchRow<int64_t>(
      "SELECT count(*) FROM t"));
  ASSERT_EQ(row_count, 100);
  const auto r_count = ASSERT_RESULT(conn.FetchRow<int64_t>(
      "SELECT count(r) FROM t"));
  ASSERT_EQ(r_count, 100);
}

// Dependents that reference the relation by OID (a view and a foreign key)
// must continue to resolve and enforce after the storage switch.
TEST_F(PgOnlineSchemaChangeTest, SwapPreservesDependents) {
  auto conn = ASSERT_RESULT(Connect());

  ASSERT_OK(conn.Execute("CREATE TABLE parent (id int PRIMARY KEY, v text)"));
  ASSERT_OK(conn.Execute(
      "INSERT INTO parent SELECT g, 'p' || g FROM generate_series(1, 10) g"));
  // View depends on parent.id (not on the column we rewrite).
  ASSERT_OK(conn.Execute("CREATE VIEW parent_v AS SELECT id FROM parent"));
  // Child FK references parent(id) by OID.
  ASSERT_OK(conn.Execute(
      "CREATE TABLE child (id int PRIMARY KEY, pid int REFERENCES parent(id))"));
  ASSERT_OK(conn.Execute("INSERT INTO child VALUES (1, 5), (2, 7)"));

  const auto parent_oid_before = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT oid FROM pg_class WHERE relname = 'parent'"));

  // Rewrite parent's storage (volatile default column, unrelated to id/v).
  ASSERT_OK(conn.Execute(
      "ALTER TABLE parent ADD COLUMN r double precision DEFAULT random()"));

  const auto parent_oid_after = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT oid FROM pg_class WHERE relname = 'parent'"));
  ASSERT_EQ(parent_oid_before, parent_oid_after);

  // View still resolves against the rewritten parent.
  const auto view_rows = ASSERT_RESULT(conn.FetchRow<int64_t>(
      "SELECT count(*) FROM parent_v"));
  ASSERT_EQ(view_rows, 10);

  // FK still enforced: a child referencing a non-existent parent must fail.
  auto bad = conn.Execute("INSERT INTO child VALUES (3, 999)");
  ASSERT_NOK(bad);
  ASSERT_STR_CONTAINS(bad.ToString(), "foreign key constraint");

  // FK still permits a valid child referencing the rewritten parent.
  ASSERT_OK(conn.Execute("INSERT INTO child VALUES (3, 10)"));
  const auto child_rows = ASSERT_RESULT(conn.FetchRow<int64_t>(
      "SELECT count(*) FROM child"));
  ASSERT_EQ(child_rows, 3);
}

// Driven through the migration job: with the shadow-creation phase enabled and
// the pipeline paused in RUNNING, a started migration for a real table creates a
// hidden shadow generation owned by that migration and recorded on the job,
// while it is still a SHADOW (pre-cutover). Validates the SHADOW_CREATING phase.
TEST_F(PgOnlineSchemaChangeTest, MigrationCreatesShadowGeneration) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_enable_schema_migration_admission) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_schema_migration_create_shadow) = true;
  // Stop before cutover so we observe the shadow while it is still a SHADOW
  // generation (cutover would flip its role to ACTIVE).
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_schema_migration_stop_before_cutover) = true;

  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE mig_demo (id int PRIMARY KEY, v text)"));

  const auto table_oid = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT 'mig_demo'::regclass::oid"));
  const auto database_oid = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT oid FROM pg_database WHERE datname = current_database()"));

  auto* mini_master = ASSERT_RESULT(cluster_->GetLeaderMiniMaster());
  auto& mgr = mini_master->master()->schema_migration_manager();
  auto epoch = mini_master->catalog_manager_impl().GetLeaderEpochInternal();

  auto migration_id = ASSERT_RESULT(mgr.StartSchemaMigration(
      master::SysSchemaMigrationEntryPB::ONLINE_TABLE_REWRITE, database_oid, table_oid,
      /* submitted_by */ 10, "ALTER TABLE mig_demo ADD c int", /* request_id */ "", epoch));

  // The paused pipeline creates the shadow during PREFLIGHT->SHADOW_CREATING and
  // records it on the job, then stops (pause is checked at RUNNING).
  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    return !VERIFY_RESULT(mgr.GetSchemaMigration(migration_id)).shadow_table_id().empty();
  }, 60s, "shadow generation recorded on the job"));

  auto pb = ASSERT_RESULT(mgr.GetSchemaMigration(migration_id));

  // That shadow exists, is a SHADOW generation, owned by this migration.
  auto* cm = ASSERT_RESULT(catalog_manager_impl());
  auto shadow = cm->GetTableInfo(pb.shadow_table_id());
  ASSERT_TRUE(shadow != nullptr);
  {
    auto sl = shadow->LockForRead();
    ASSERT_TRUE(sl->is_shadow_generation());
    ASSERT_EQ(sl->pb.owning_migration_id(), migration_id);
  }
  // Still invisible to clients.
  ASSERT_EQ(ASSERT_RESULT(CountInListTables("mig_demo")), 1);
}

// Full pipeline through the job with the real backend enabled: the migration
// creates a shadow, copies the source data into it (tablet clone), and cuts
// over (shadow -> ACTIVE, source -> RETIRED). Verifies the copied row count in
// the shadow generation and the post-cutover role flip.
TEST_F(PgOnlineSchemaChangeTest, MigrationCopiesAndCutsOver) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_enable_schema_migration_admission) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_schema_migration_create_shadow) = true;

  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE copy_demo (id int PRIMARY KEY, v text)"));
  ASSERT_OK(conn.Execute(
      "INSERT INTO copy_demo SELECT g, 'v' || g FROM generate_series(1, 50) g"));

  const auto table_oid = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT 'copy_demo'::regclass::oid"));
  const auto database_oid = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT oid FROM pg_database WHERE datname = current_database()"));

  auto* mini_master = ASSERT_RESULT(cluster_->GetLeaderMiniMaster());
  auto& mgr = mini_master->master()->schema_migration_manager();
  auto* cm = ASSERT_RESULT(catalog_manager_impl());
  auto epoch = cm->GetLeaderEpochInternal();

  auto migration_id = ASSERT_RESULT(mgr.StartSchemaMigration(
      master::SysSchemaMigrationEntryPB::ONLINE_TABLE_REWRITE, database_oid, table_oid,
      /* submitted_by */ 10, "ALTER TABLE copy_demo ADD c int", /* request_id */ "", epoch));

  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    auto pb = VERIFY_RESULT(mgr.GetSchemaMigration(migration_id));
    if (pb.state() == master::SysSchemaMigrationEntryPB::FAILED) {
      return STATUS_FORMAT(IllegalState, "migration failed: $0",
                           StatusFromPB(pb.terminal_error()));
    }
    return pb.state() == master::SysSchemaMigrationEntryPB::SUCCEEDED;
  }, 120s, "migration reaches SUCCEEDED"));

  auto pb = ASSERT_RESULT(mgr.GetSchemaMigration(migration_id));
  ASSERT_FALSE(pb.shadow_table_id().empty());

  const auto source_id = GetPgsqlTableId(database_oid, table_oid);
  auto source = cm->GetTableInfo(source_id);
  auto shadow = cm->GetTableInfo(pb.shadow_table_id());
  ASSERT_TRUE(source != nullptr);
  ASSERT_TRUE(shadow != nullptr);

  // Cutover flipped the roles.
  ASSERT_TRUE(source->LockForRead()->is_retired_generation());
  ASSERT_TRUE(shadow->LockForRead()->is_active_generation());
}

// Full live-serving migration entirely from SQL: start via
// yb_start_online_schema_change, wait for the job to copy+cutover, then
// yb_finalize_online_schema_change repoints the relation's relfilenode to the
// shadow. After finalize, SELECT on the original table name serves from the
// shadow generation and returns all the copied rows.
TEST_F(PgOnlineSchemaChangeTest, FinalizeServesFromShadow) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_enable_schema_migration_admission) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_schema_migration_create_shadow) = true;

  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE srv (id int PRIMARY KEY, v text)"));
  ASSERT_OK(conn.Execute(
      "INSERT INTO srv SELECT g, 'v' || g FROM generate_series(1, 100) g"));

  const auto relfilenode_before = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT relfilenode FROM pg_class WHERE relname = 'srv'"));

  // Start the migration via SQL and wait for it to finish (copy + cutover).
  const auto migration_id = ASSERT_RESULT(conn.FetchRow<std::string>(
      "SELECT yb_start_online_schema_change('srv'::regclass, "
      "'ALTER TABLE srv ADD c int', NULL)"));
  ASSERT_FALSE(migration_id.empty());

  auto* mini_master = ASSERT_RESULT(cluster_->GetLeaderMiniMaster());
  auto& mgr = mini_master->master()->schema_migration_manager();
  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    auto pb = VERIFY_RESULT(mgr.GetSchemaMigration(migration_id));
    if (pb.state() == master::SysSchemaMigrationEntryPB::FAILED) {
      return STATUS_FORMAT(IllegalState, "migration failed: $0",
                           StatusFromPB(pb.terminal_error()));
    }
    return pb.state() == master::SysSchemaMigrationEntryPB::SUCCEEDED;
  }, 120s, "migration reaches SUCCEEDED"));

  // Finalize: repoint pg_class.relfilenode to the shadow.
  ASSERT_TRUE(ASSERT_RESULT(conn.FetchRow<bool>(Format(
      "SELECT yb_finalize_online_schema_change('srv'::regclass, '$0')", migration_id))));

  // The relation's relfilenode changed (now the shadow's).
  const auto relfilenode_after = ASSERT_RESULT(conn.FetchRow<PGOid>(
      "SELECT relfilenode FROM pg_class WHERE relname = 'srv'"));
  ASSERT_NE(relfilenode_before, relfilenode_after);

  // Reads on the original table name now serve from the shadow generation and
  // return all copied rows. Use a fresh connection to avoid any stale relcache.
  auto conn2 = ASSERT_RESULT(Connect());
  const auto row_count = ASSERT_RESULT(conn2.FetchRow<int64_t>(
      "SELECT count(*) FROM srv"));
  ASSERT_EQ(row_count, 100);
  const auto max_id = ASSERT_RESULT(conn2.FetchRow<int32_t>(
      "SELECT max(id) FROM srv"));
  ASSERT_EQ(max_id, 100);
}

// The master can create a hidden SHADOW physical generation of a live YSQL
// table: it shares the source's logical identity (pg_table_id) but has a fresh
// distinct physical id, is owned by the migration, and is excluded from the
// client-facing ListTables view.
TEST_F(PgOnlineSchemaChangeTest, CreateShadowGeneration) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE gen_demo (id int PRIMARY KEY, v text)"));
  ASSERT_OK(conn.Execute(
      "INSERT INTO gen_demo SELECT g, 'v' || g FROM generate_series(1, 20) g"));

  auto* cm = ASSERT_RESULT(catalog_manager_impl());
  auto source = ASSERT_RESULT(FindUserTable("gen_demo"));
  const auto source_id = source->id();
  const auto source_pg_table_id = source->LockForRead()->pb.pg_table_id();

  const std::string migration_id = "test-migration-abc";
  auto shadow_id = ASSERT_RESULT(cm->CreateShadowGeneration(
      source_id, migration_id, cm->GetLeaderEpochInternal()));

  // Distinct physical generation.
  ASSERT_NE(shadow_id, source_id);

  auto shadow = cm->GetTableInfo(shadow_id);
  ASSERT_TRUE(shadow != nullptr);
  {
    auto l = shadow->LockForRead();
    ASSERT_TRUE(l->is_shadow_generation());
    ASSERT_EQ(l->pb.owning_migration_id(), migration_id);
    // Shares the source's logical identity.
    const auto expected_logical =
        source_pg_table_id.empty() ? source_id : source_pg_table_id;
    const auto shadow_logical =
        l->pb.pg_table_id().empty() ? shadow_id : l->pb.pg_table_id();
    ASSERT_EQ(shadow_logical, expected_logical);
    // Same schema shape (column count).
    ASSERT_EQ(l->pb.schema().columns_size(), source->LockForRead()->pb.schema().columns_size());
  }

  // The shadow is invisible to clients; only the source's single name is listed.
  ASSERT_EQ(ASSERT_RESULT(CountInListTables("gen_demo")), 1);
}

// A table whose physical generation is marked SHADOW must disappear from the
// client-facing ListTables view (roadmap Milestone A / generation-metadata.md),
// while its logical pg_class row remains. This pins the invisibility invariant
// the migration relies on before cutover.
TEST_F(PgOnlineSchemaChangeTest, ShadowGenerationHiddenFromListTables) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE shadow_demo (id int PRIMARY KEY, v text)"));

  // Initially visible to clients.
  ASSERT_EQ(ASSERT_RESULT(CountInListTables("shadow_demo")), 1);
  auto table = ASSERT_RESULT(FindUserTable("shadow_demo"));

  // Flip the physical-generation role to SHADOW (as the migration job will when
  // it creates a hidden target generation).
  {
    auto l = table->LockForWrite();
    l.mutable_data()->pb.set_physical_generation_role(master::SysTablesEntryPB::SHADOW);
    l.Commit();
  }

  // No longer surfaced to clients, even though it still exists physically.
  ASSERT_EQ(ASSERT_RESULT(CountInListTables("shadow_demo")), 0);
  // The logical relation is untouched in pg_class.
  const auto pg_class_rows = ASSERT_RESULT(conn.FetchRow<int64_t>(
      "SELECT count(*) FROM pg_class WHERE relname = 'shadow_demo'"));
  ASSERT_EQ(pg_class_rows, 1);

  // Restoring ACTIVE makes it visible again.
  {
    auto l = table->LockForWrite();
    l.mutable_data()->pb.set_physical_generation_role(master::SysTablesEntryPB::ACTIVE);
    l.Commit();
  }
  ASSERT_EQ(ASSERT_RESULT(CountInListTables("shadow_demo")), 1);
}

// End-to-end on a multi-tablet table across an RF3 cluster: the migration
// creates a hidden shadow generation, clones every source tablet into the
// paired shadow tablet across all tservers, restores the data, and cuts over
// (shadow -> ACTIVE, source -> RETIRED). Verifies the shadow holds the same
// DocDB records as the source (real data parity) and the role flip. Exercises
// the per-tablet, cross-node path the single tablet/RF1 tests do not.
TEST_F(PgOnlineSchemaChangeRf3Test, MultiTabletMigration) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_enable_schema_migration_admission) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_schema_migration_create_shadow) = true;

  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute(
      "CREATE TABLE mt (id int PRIMARY KEY, v text) SPLIT INTO 4 TABLETS"));
  ASSERT_OK(conn.Execute(
      "INSERT INTO mt SELECT g, 'v' || g FROM generate_series(1, 500) g"));

  // Flush so the source rows are in SSTs; the copy snapshots/flushes anyway, but
  // this makes the DocDB record count comparison meaningful.
  ASSERT_OK(cluster_->FlushTablets());

  auto source = ASSERT_RESULT(FindUserTable("mt"));
  const auto source_id = source->id();
  const auto source_tablets = ASSERT_RESULT(RunningTabletCount(source_id));
  ASSERT_GE(source_tablets, 4);
  const auto source_records = ASSERT_RESULT(CountDbRecords(source_id));
  ASSERT_GT(source_records, 0);

  auto pb = ASSERT_RESULT(RunMigrationToCompletion(&conn, "mt"));
  ASSERT_FALSE(pb.shadow_table_id().empty());

  auto* cm = ASSERT_RESULT(catalog_manager_impl());
  auto shadow = cm->GetTableInfo(pb.shadow_table_id());
  ASSERT_TRUE(shadow != nullptr);

  // Shadow has the same number of running tablets as the source did, spread
  // across the RF3 cluster.
  ASSERT_EQ(ASSERT_RESULT(RunningTabletCount(pb.shadow_table_id())), source_tablets);

  // Real data parity: the shadow's tablets hold the same DocDB records as the
  // source (clone hard-linked the SSTs and restore loaded them).
  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    return VERIFY_RESULT(CountDbRecords(pb.shadow_table_id())) == source_records;
  }, 60s, "shadow tablets hold the copied records"));

  // Cutover flipped the roles.
  ASSERT_TRUE(source->LockForRead()->is_retired_generation());
  ASSERT_TRUE(shadow->LockForRead()->is_active_generation());
}

}  // namespace pgwrapper
}  // namespace yb
