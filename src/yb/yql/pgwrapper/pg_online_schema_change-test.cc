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

#include "yb/master/catalog_entity_info.h"
#include "yb/master/catalog_manager.h"
#include "yb/master/catalog_manager_if.h"
#include "yb/master/master_ddl.pb.h"

#include "yb/yql/pgwrapper/libpq_utils.h"
#include "yb/yql/pgwrapper/pg_mini_test_base.h"

#include "yb/util/test_macros.h"

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

}  // namespace pgwrapper
}  // namespace yb
