// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations
// under the License.

#include <array>
#include <atomic>
#include <map>
#include <mutex>
#include <set>
#include <tuple>

#include "yb/client/client.h"
#include "yb/client/table.h"
#include "yb/common/pgsql_protocol.messages.h"
#include "yb/common/ql_type.h"
#include "yb/common/schema_pbutil.h"
#include "yb/rpc/sidecars.h"
#include "yb/tools/local_runtime/pg_local_store.h"
#include "yb/tools/local_runtime/pg_local_table.h"
#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/pg_client_service.h"
#include "yb/tserver/tablet_server.h"
#include "yb/util/memory/arena.h"
#include "yb/util/test_macros.h"
#include "yb/yql/pgwrapper/pg_mini_test_base.h"

DECLARE_bool(TEST_enable_pg_client_mock);
DECLARE_bool(pg_client_use_shared_memory);
DECLARE_int32(replication_factor);
DECLARE_string(ysql_pg_conf_csv);

namespace yb::local_runtime {
namespace {

using Row = std::tuple<int64_t, int64_t, int64_t>;

// Compatibility harness, not a standalone runtime: the cluster owns SQL/catalogs, while
// intercepted application reads must use the local store. The two stores have different rows.
class PgLocalYsqlTest : public pgwrapper::PgMiniTestBase {
 protected:
  struct Table {
    std::string name;
    Schema schema;
    dockv::PartitionSchema partition_schema;
    std::vector<dockv::Partition> partitions;
    std::set<Row> expected;
    std::map<Row, uint16_t> hashes;
    bool composite;
  };

  virtual bool UseSharedMemory() const = 0;
  size_t NumTabletServers() override { return 1; }

  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_enable_pg_client_mock) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_pg_client_use_shared_memory) = UseSharedMemory();
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_replication_factor) = 1;
    PgMiniTestBase::SetUp();
    path_ = GetTestPath("local-ysql-store");
    store_ = ASSERT_RESULT(PgLocalStore::Open(path_));
  }

  void DoTearDown() override {
    mock_.reset();
    before_.reset();
    after_.reset();
    PgMiniTestBase::DoTearDown();
    store_.reset();
  }

  PgsqlWriteRequestPB WriteRequest(const TableId& id, const Table& table, int64_t h) {
    PgsqlWriteRequestPB request;
    request.set_client(YQL_CLIENT_PGSQL);
    request.set_stmt_type(PgsqlWriteRequestPB::PGSQL_INSERT);
    request.set_table_id(id);
    request.set_schema_version(0);
    request.add_partition_column_values()->mutable_value()->set_int64_value(h);
    auto* r =
        table.composite ? request.add_partition_column_values() : request.add_range_column_values();
    r->mutable_value()->set_int64_value(h + 1);
    auto* column = request.add_column_values();
    column->set_column_id(table.schema.column_id(2));
    column->mutable_expr()->mutable_value()->set_int64_value(h * 10 + 3);
    return request;
  }

  Status Seed(const TableId& id, Table* table, int64_t h) {
    const Row row{h, h + 1, h * 10 + 3};
    if (!table->expected.insert(row).second) {
      return Status::OK();
    }
    auto write = WriteRequest(id, *table, h);
    table->hashes[row] = VERIFY_RESULT(store_->HashCode(id, write.partition_column_values()));
    PgsqlResponsePB response;
    RETURN_NOT_OK(store_->ApplyAutocommitWrite(write, &response));
    SCHECK_EQ(
        response.status(), PgsqlResponsePB::PGSQL_STATUS_OK, IllegalState,
        response.ShortDebugString());
    return Status::OK();
  }

  void PrepareTables(pgwrapper::PGConn* connection) {
    for (bool composite : {false, true}) {
      const std::string name = composite ? "local_composite" : "local_scalar";
      ASSERT_OK(connection->ExecuteFormat(
          "CREATE TABLE $0 (h bigint, r bigint, v bigint, PRIMARY KEY $1) SPLIT INTO 4 TABLETS",
          name, composite ? "((h,r) HASH)" : "(h HASH,r ASC)"));
      ASSERT_OK(connection->ExecuteFormat("INSERT INTO $0 VALUES (-2000000,0,-1)", name));
      const auto id = ASSERT_RESULT(GetTableIDFromTableName(name));
      auto native = ASSERT_RESULT(client_->OpenTable(id));
      auto& table = tables_[id];
      table.name = name;
      table.schema = native->InternalSchema();
      table.partition_schema = native->partition_schema();
      table.composite = composite;
      ASSERT_OK(store_->CreateTable(id, table.schema, 4));
      table.partitions = ASSERT_RESULT(store_->GetPartitions(id));
      ASSERT_EQ(native->GetPartitionsShared()->size(), table.partitions.size());
      for (size_t i = 0; i < table.partitions.size(); ++i) {
        ASSERT_EQ((*native->GetPartitionsShared())[i], table.partitions[i].partition_key_start());
      }
      for (int64_t h = -32; h < 96; ++h) {
        ASSERT_OK(Seed(id, &table, h));
      }
      std::array<bool, 2> witnesses{};
      for (int64_t h = 0; h < 1000000 && (!witnesses[0] || !witnesses[1]); ++h) {
        const auto hash = ASSERT_RESULT(
            store_->HashCode(id, WriteRequest(id, table, h).partition_column_values()));
        if (hash == 0 || hash == 65535) {
          witnesses[hash == 0 ? 0 : 1] = true;
          ASSERT_OK(Seed(id, &table, h));
        }
      }
      ASSERT_TRUE(witnesses[0]);
      ASSERT_TRUE(witnesses[1]);
    }
    ASSERT_OK(connection->Execute("CREATE TABLE native_control (k bigint PRIMARY KEY)"));
    ASSERT_OK(connection->Execute("INSERT INTO native_control VALUES (7)"));
    snapshot_ = store_->LatestReadTime();
  }

  Result<ReadResult> Read(PgsqlReadRequestPB request, const Table& table) {
    SCHECK(request.is_forward_scan(), NotSupported, "Reverse scan outside fixture contract");
    SCHECK(
        !request.paging_state().has_next_row_key(), NotSupported,
        "Within-tablet paging outside fixture contract");
    RETURN_NOT_OK(dockv::InitPartitionKey(table.schema, table.partition_schema, &request));
    size_t tablet = 0;
    for (size_t i = 1; i < table.partitions.size(); ++i) {
      if (request.partition_key() >= table.partitions[i].partition_key_start()) {
        tablet = i;
      }
    }
    const auto total = request.paging_state().total_num_rows_read();
    const auto size_limit = request.size_limit();
    request.clear_partition_key();
    request.clear_paging_state();
    request.clear_return_paging_state();
    request.clear_size_limit();
    // This fixture has a frozen catalog and snapshot, not a catalog/transaction coordinator.
    request.clear_ysql_catalog_version();
    request.clear_ysql_db_catalog_version();
    request.clear_ysql_db_oid();
    const bool legacy_refs = !request.column_refs().ids().empty();
    size_t ref_index = 0;
    for (const auto& ref : request.col_refs()) {
      const auto i = table.schema.find_column_by_id(ColumnId(ref.column_id()));
      SCHECK(i >= 0, InvalidArgument, "Unknown fixture column");
      const auto& column = table.schema.column(i);
      SCHECK(
          ref.has_column_id() && column.type()->main() == DataType::INT64 &&
              (!ref.has_attno() || ref.attno() == column.order()) &&
              (!ref.has_typid() || ref.typid() == column.pg_type_oid()) &&
              (!ref.has_typmod() || ref.typmod() == -1) && (!ref.has_collid() || ref.collid() == 0),
          NotSupported,
          "Column metadata does not match bigint fixture schema: " + ref.ShortDebugString());
      if (legacy_refs) {
        SCHECK_LT(
            ref_index, static_cast<size_t>(request.column_refs().ids_size()), InvalidArgument,
            "Mismatched reference formats");
        SCHECK_EQ(
            request.column_refs().ids(static_cast<int>(ref_index)), ref.column_id(),
            InvalidArgument, "Mismatched reference formats");
      } else {
        request.mutable_column_refs()->add_ids(ref.column_id());
      }
      ++ref_index;
    }
    SCHECK(
        !legacy_refs || request.col_refs().empty() ||
            ref_index == static_cast<size_t>(request.column_refs().ids_size()),
        InvalidArgument, "Mismatched reference formats");
    request.clear_col_refs();
    if (!request.is_aggregate()) request.clear_is_aggregate();
    if (!request.distinct()) request.clear_distinct();
    if (!request.is_for_backfill()) request.clear_is_for_backfill();
    if (!request.read_at_in_txn_limit()) request.clear_read_at_in_txn_limit();
    if (!request.skip_intents_read()) request.clear_skip_intents_read();
    if (request.metrics_capture() == PGSQL_METRICS_CAPTURE_NONE) request.clear_metrics_capture();
    // Preserve DocKey bounds: current PgGate uses them even for HASH partition scans.
    auto result =
        VERIFY_RESULT_PREPEND(store_->Read(request, tablet, snapshot_), request.ShortDebugString());
    SCHECK(
        !request.has_limit() || result.row_count < request.limit(), NotSupported,
        "Within-tablet paging outside fixture contract");
    SCHECK(
        size_limit == 0 || result.rows.size() <= size_limit, NotSupported,
        "Response exceeds fixture byte limit");
    const auto& end = table.partitions[tablet].partition_key_end();
    if (request.partition_column_values().empty() && !end.empty() &&
        (!request.has_max_hash_code() ||
         request.max_hash_code() >= dockv::PartitionSchema::DecodeMultiColumnHashValue(end))) {
      auto* paging = result.response.mutable_paging_state();
      paging->set_next_partition_key(end);
      paging->set_next_tablet_bound(end);
      paging->set_total_num_rows_read(total + result.row_count);
    }
    return result;
  }

  void InstallHook() {
    auto* service = cluster_->mini_tablet_server(0)->server()->TEST_GetPgClientServiceMock();
    before_.emplace(service->MockPerformBefore([this](const auto*, auto*, auto*) {
      ++before_calls_;
      return Status::OK();
    }));
    after_.emplace(service->MockPerformAfter([this](const auto*, auto*, auto*) {
      ++after_calls_;
      return Status::OK();
    }));
    mock_.emplace(service->MockPerformAfterSequencing(
        [this](
            const tserver::PgPerformRequestMsg* req, tserver::PgPerformResponseMsg* resp,
            tserver::PgClientMockCallContext* context) -> Result<bool> {
          bool owned = false;
          // Claim any batch touching local data, so unsupported mixtures cannot fall back.
          for (const auto& op : req->ops()) {
            const auto id = op.has_read() ? op.read().table_id() : op.write().table_id();
            owned |= tables_.contains(id.ToBuffer());
          }
          if (!owned) return false;
          for (const auto& op : req->ops()) {
            SCHECK(op.has_read(), NotSupported, "Local compatibility harness is read-only");
            SCHECK(
                tables_.contains(op.read().table_id().ToBuffer()), NotSupported,
                "Mixed local/native batch outside fixture contract");
          }
          SCHECK(context->sidecars, IllegalState, "Missing response sidecars");
          if (context->rpc)
            ++rpc_reads_;
          else
            ++shared_reads_;
          for (const auto& op : req->ops()) {
            auto read = op.read().ToGoogleProtobuf();
            auto result = VERIFY_RESULT(Read(read, tables_.at(read.table_id())));
            context->sidecars->Start().Append(Slice(result.rows));
            auto* response = resp->add_responses();
            response->CopyFrom(result.response);
            response->set_rows_data_sidecar(static_cast<int>(context->sidecars->Complete()));
          }
          return true;
        }));
  }

  Result<std::vector<Row>> FetchRows(pgwrapper::PGConn* connection, const std::string& query) {
    return connection->FetchRows<int64_t, int64_t, int64_t>(query);
  }

  void RunContract() {
    auto connection = ASSERT_RESULT(Connect());
    ASSERT_OK(connection.Execute("SET statement_timeout = '20s'"));
    ASSERT_OK(connection.Execute("SET yb_enable_pg_stat_statements_docdb_metrics = false"));
    PrepareTables(&connection);
    ASSERT_FALSE(HasFatalFailure());
    InstallHook();
    {
      ThreadSafeArena arena;
      tserver::PgPerformRequestMsg request(&arena);
      tserver::PgPerformResponseMsg response(&arena);
      request.add_ops()->mutable_read()->dup_table_id(tables_.begin()->first);
      request.add_ops()->mutable_read()->dup_table_id(
          ASSERT_RESULT(GetTableIDFromTableName("native_control")));
      rpc::Sidecars sidecars;
      tserver::PgClientMockCallContext context{.sidecars = &sidecars};
      auto* service = cluster_->mini_tablet_server(0)->server()->TEST_GetPgClientServiceMock();
      auto result = service->DispatchPerformAfterSequencing(&request, &response, &context);
      ASSERT_NOK(result.status());
      ASSERT_STR_CONTAINS(result.status().ToString(), "Mixed local/native batch");
      ASSERT_EQ(response.responses_size(), 0);
      ASSERT_EQ(sidecars.size(), 0);
    }
    for (const auto& [id, table] : tables_) {
      const auto all = ASSERT_RESULT(FetchRows(&connection, "SELECT h,r,v FROM " + table.name));
      ASSERT_EQ((std::set<Row>(all.begin(), all.end())), table.expected);
      std::set<Row> seen;
      const std::string hash = table.composite ? "yb_hash_code(h,r)" : "yb_hash_code(h)";
      for (uint32_t lo = 0; lo < 65536; lo += 257) {
        const auto hi = std::min(lo + 257, 65536U);
        auto rows = ASSERT_RESULT(FetchRows(
            &connection,
            Format("SELECT h,r,v FROM $0 WHERE $1 >= $2 AND $1 < $3", table.name, hash, lo, hi)));
        std::set<Row> expected;
        for (const auto& [row, value] : table.hashes) {
          if (value >= lo && value < hi) expected.insert(row);
        }
        ASSERT_EQ((std::set<Row>(rows.begin(), rows.end())), expected);
        for (const auto& row : rows) ASSERT_TRUE(seen.insert(row).second);
      }
      ASSERT_EQ(seen, table.expected);
      auto empty = ASSERT_RESULT(FetchRows(
          &connection,
          Format("SELECT h,r,v FROM $0 WHERE $1 >= 12345 AND $1 < 12345", table.name, hash)));
      ASSERT_TRUE(empty.empty());
    }
    ASSERT_OK(connection.Execute("SET yb_fetch_row_limit = 1"));
    auto paging = connection.Fetch("SELECT h,r,v FROM local_scalar");
    ASSERT_NOK(paging.status());
    ASSERT_STR_CONTAINS(
        paging.status().ToString(), "Within-tablet paging outside fixture contract");
    ASSERT_OK(connection.Execute("RESET yb_fetch_row_limit"));
    auto rejected = connection.Execute("INSERT INTO local_scalar VALUES (9000000,1,2)");
    ASSERT_NOK(rejected);
    ASSERT_STR_CONTAINS(rejected.ToString(), "Local compatibility harness is read-only");
    // A skipped request sequence number would stall this real-service request.
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT k FROM native_control")), 7);
    ASSERT_GT(UseSharedMemory() ? shared_reads_.load() : rpc_reads_.load(), 0);
    ASSERT_EQ(UseSharedMemory() ? rpc_reads_.load() : shared_reads_.load(), 0);
    ASSERT_GT(before_calls_.load(), 0);
    ASSERT_GT(after_calls_.load(), 0);
    mock_.reset();
    for (const auto& [id, table] : tables_) {
      auto rows = ASSERT_RESULT(FetchRows(&connection, "SELECT h,r,v FROM " + table.name));
      ASSERT_EQ(rows, (std::vector<Row>{{-2000000, 0, -1}}));
    }
    store_.reset();
    store_ = ASSERT_RESULT(PgLocalStore::Open(path_));
    InstallHook();
    for (const auto& [id, table] : tables_) {
      auto rows = ASSERT_RESULT(FetchRows(&connection, "SELECT h,r,v FROM " + table.name));
      ASSERT_EQ((std::set<Row>(rows.begin(), rows.end())), table.expected);
    }
  }

  std::string path_;
  std::unique_ptr<PgLocalStore> store_;
  std::map<TableId, Table> tables_;
  HybridTime snapshot_;
  std::atomic<size_t> rpc_reads_{0};
  std::atomic<size_t> shared_reads_{0};
  std::atomic<size_t> before_calls_{0};
  std::atomic<size_t> after_calls_{0};
  std::optional<tserver::PgClientServiceMockImpl::Handle> before_;
  std::optional<tserver::PgClientServiceMockImpl::Handle> after_;
  std::optional<tserver::PgClientServiceMockImpl::PerformHookHandle> mock_;
};

class PgLocalYsqlRpcTest : public PgLocalYsqlTest {
  bool UseSharedMemory() const override { return false; }
};
class PgLocalYsqlSharedMemoryTest : public PgLocalYsqlTest {
  bool UseSharedMemory() const override { return true; }
};

class PgLocalIndexMetadataTest : public PgLocalYsqlRpcTest {
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_pg_conf_csv) =
        "yb_ddl_transaction_block_enabled=on,yb_non_ddl_txn_for_sys_tables_allowed=on";
    PgLocalYsqlRpcTest::SetUp();
  }
};

TEST_F(PgLocalIndexMetadataTest, IndexDescriptorParity) {
  auto connection = ASSERT_RESULT(Connect());
  ASSERT_OK(
      connection.Execute("CREATE TABLE descriptor_base(k bigint PRIMARY KEY, v bigint, s text)"));
  std::mutex mutex;
  tserver::PgCreateTableRequestPB captured;
  auto hook = cluster_->mini_tablet_server(0)
                  ->server()
                  ->TEST_GetPgClientServiceMock()
                  ->MockCreateTableBefore([&](const void* request, void*, auto*) {
                    std::lock_guard lock(mutex);
                    captured = *static_cast<const tserver::PgCreateTableRequestPB*>(request);
                    return Status::OK();
                  });
  for (const auto& [name, ddl] : std::vector<std::pair<std::string, std::string>>{
           {"plain_idx",
            "CREATE INDEX NONCONCURRENTLY plain_idx ON descriptor_base(v HASH) "
            "INCLUDE(s) SPLIT INTO 4 TABLETS"},
           {"unique_idx",
            "CREATE UNIQUE INDEX NONCONCURRENTLY unique_idx ON descriptor_base(v HASH) "
            "INCLUDE(s) SPLIT INTO 4 TABLETS"},
           {"range_idx",
            "CREATE INDEX NONCONCURRENTLY range_idx ON descriptor_base(s ASC) INCLUDE(v)"}}) {
    ASSERT_OK(connection.Execute(ddl));
    tserver::PgCreateTableRequestPB request;
    {
      std::lock_guard lock(mutex);
      request = captured;
    }
    ASSERT_EQ(request.table_name(), name);
    const auto local = ASSERT_RESULT(BuildLocalTable(request));
    auto native = ASSERT_RESULT(client_->OpenTable(ASSERT_RESULT(GetTableIDFromTableName(name))));
    Schema schema;
    ASSERT_OK(SchemaFromPB(local.schema(), &schema));
    ASSERT_TRUE(native->InternalSchema().Equals(schema))
        << "Native: " << native->InternalSchema().ToString() << "; local: " << schema.ToString();
    ASSERT_EQ(native->InternalSchema().column_ids(), schema.column_ids());
    dockv::PartitionSchema partition_schema;
    ASSERT_OK(dockv::PartitionSchema::FromPB(local.partition_schema(), schema, &partition_schema));
    ASSERT_TRUE(native->partition_schema().Equals(partition_schema));
    ASSERT_EQ(native->GetPartitionsShared()->size(), schema.table_properties().num_tablets());
    std::vector<dockv::Partition> partitions;
    if (schema.num_hash_key_columns()) {
      ASSERT_OK(
          partition_schema.CreatePartitions(schema.table_properties().num_tablets(), &partitions));
    } else {
      partitions.emplace_back();
    }
    for (size_t i = 0; i < partitions.size(); ++i)
      ASSERT_EQ((*native->GetPartitionsShared())[i], partitions[i].partition_key_start());
  }
}

TEST_F(PgLocalYsqlRpcTest, HashScanContract) { RunContract(); }
TEST_F(PgLocalYsqlSharedMemoryTest, HashScanContract) { RunContract(); }

}  // namespace
}  // namespace yb::local_runtime
