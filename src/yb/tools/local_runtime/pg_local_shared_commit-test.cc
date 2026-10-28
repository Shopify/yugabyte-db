// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations
// under the License.

#include "yb/tools/local_runtime/pg_catalog_transaction.h"

#include <sys/wait.h>

#include <algorithm>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <utility>

#include "yb/common/doc_hybrid_time.h"
#include "yb/dockv/doc_key.h"
#include "yb/common/entity_ids.h"
#include "yb/common/pgsql_error.h"
#include "yb/common/pgsql_protocol.messages.h"
#include "yb/common/schema_pbutil.h"
#include "yb/docdb/doc_read_context.h"
#include "yb/docdb/docdb.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/docdb/pgsql_operation.h"
#include "yb/docdb/ql_rocksdb_storage.h"
#include "yb/rocksdb/db.h"
#include "yb/rocksdb/perf_context.h"
#include "yb/rocksdb/write_batch.h"
#include "yb/rpc/sidecars.h"
#include "yb/tablet/tablet_options.h"
#include "yb/tools/local_runtime/pg_local_metadata.pb.h"
#include "yb/tools/local_runtime/pg_local_storage.h"
#include "yb/tools/local_runtime/pg_local_table.h"
#include "yb/tools/local_runtime/pg_local_txn_store.h"
#include "yb/tserver/pg_client.pb.h"
#include "yb/tserver/tserver_admin.pb.h"
#include "yb/util/env_util.h"
#include "yb/util/decimal.h"
#include "yb/util/pb_util.h"
#include "yb/util/scope_exit.h"
#include "yb/util/subprocess.h"
#include "yb/util/sync_point.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"
#include "yb/yql/pggate/util/pg_doc_data.h"
#include "yb/yql/pggate/util/pg_wire.h"

DECLARE_bool(ysql_enable_packed_row);

namespace yb::local_runtime {
namespace {

Result<tablet::TableInfoPB> Table(
    uint32_t oid, bool hash = true, DataType value_type = DataType::INT64, uint32_t base_oid = 0) {
  tserver::PgCreateTableRequestPB request;
  request.set_use_transaction(true);
  request.set_use_regular_transaction_block(true);
  request.mutable_table_id()->set_database_oid(1);
  request.mutable_table_id()->set_object_oid(oid);
  request.set_database_name("template1");
  request.set_table_name("joint_rows_" + std::to_string(oid));
  request.set_schema_name("public");
  request.set_num_tablets(hash ? 4 : 1);
  if (base_oid) {
    request.mutable_base_table_id()->set_database_oid(1);
    request.mutable_base_table_id()->set_object_oid(base_oid);
    request.set_skip_index_backfill(true);
  }
  for (int i = 0; i < 2; ++i) {
    auto* column = request.add_create_columns();
    column->set_attr_name(i == 0 ? "k" : "v");
    column->set_attr_num(i + 1);
    column->set_attr_pgoid(20);
    column->set_attr_ybtype(static_cast<int32_t>(i == 0 ? DataType::INT64 : value_type));
    column->set_is_hash(i == 0 && hash);
    column->set_is_range(i == 0 && !hash);
  }
  return BuildLocalTable(request);
}

Result<std::vector<int64_t>> Values(const std::string& bytes) {
  Slice slice(bytes);
  const auto count = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<int64_t>(&slice));
  std::vector<int64_t> result;
  for (int64_t i = 0; i < count; ++i) {
    SCHECK_EQ(
        VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint8_t>(&slice)), 0, Corruption,
        "Unexpected NULL");
    result.push_back(VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<int64_t>(&slice)));
  }
  SCHECK(slice.empty(), Corruption, "Unexpected trailing row data");
  return result;
}

// Mirrors the catalog's private sequence key layout, so tests can seed and inspect it in bulk.
std::string SequenceKey(uint32_t database_oid, uint32_t sequence_oid) {
  char name[32];
  snprintf(
      name, sizeof(name), "sequence/%08x/%08x", static_cast<unsigned>(database_oid),
      static_cast<unsigned>(sequence_oid));
  return PgLocalStorage::MetadataKey(name);
}

std::string SequenceValue(int64_t last_value) {
  SequencePB tuple;
  tuple.set_last_value(last_value);
  tuple.set_is_called(true);
  return tuple.SerializeAsString();
}

Result<size_t> CountSequences(const PgLocalStorage& storage, uint32_t database_oid) {
  const auto end = SequenceKey(database_oid + 1, 0);
  std::unique_ptr<rocksdb::Iterator> it(
      storage.db()->NewIterator(rocksdb::ReadOptions(), storage.metadata()));
  size_t count = 0;
  for (it->Seek(SequenceKey(database_oid, 0)); it->Valid() && it->key().compare(Slice(end)) < 0;
       it->Next()) {
    ++count;
  }
  RETURN_NOT_OK(it->status());
  return count;
}

class PgLocalSharedCommitTest : public YBTest {
 protected:
  std::string Seed() {
    return env_util::GetRootDir("share/initial_sys_catalog_snapshot") +
           "/share/initial_sys_catalog_snapshot";
  }
  void SetUp() override {
    YBTest::SetUp();
    path_ = std::getenv("YB_LOCAL_JOINT_CRASH_PATH") ? std::getenv("YB_LOCAL_JOINT_CRASH_PATH")
                                                     : GetTestPath("joint");
    tserver::ExportedTabletMetadataChanges changes;
    ASSERT_OK(
        pb_util::ReadPBContainerFromPath(
            Env::Default(), Seed() + "/exported_tablet_metadata_changes", &changes));
    auto registry = std::make_shared<dockv::SchemaPackingRegistry>("joint");
    const auto collect = [&](const tablet::TableInfoPB& table) -> Status {
      if (table.table_id() != kPgYbCatalogVersionTableId) return Status::OK();
      version_ = std::make_shared<docdb::DocReadContext>(
          "joint", TableType::PGSQL_TABLE_TYPE, docdb::Index::kFalse, registry);
      RETURN_NOT_OK(version_->LoadFromPB(table));
      version_->SetCotableId(VERIFY_RESULT(Uuid::FromHexString(table.table_id())));
      return Status::OK();
    };
    for (const auto& change : changes.metadata_changes()) {
      if (change.has_add_table()) ASSERT_OK(collect(change.add_table()));
      for (const auto& table : change.add_multiple_tables()) ASSERT_OK(collect(table));
    }
    ASSERT_TRUE(version_);
    catalog_ = ASSERT_RESULT(PgCatalogTransaction::Open(path_, Seed() + "/rocksdb"));
    initial_ = ASSERT_RESULT(Version());
    table_ = ASSERT_RESULT(Table(1000005000));
  }

  Status Version(int64_t value, uint32_t subtransaction) {
    ThreadSafeArena arena;
    rpc::Sidecars sidecars;
    PgsqlWriteRequestPB request;
    request.set_client(YQL_CLIENT_PGSQL);
    request.set_table_id(kPgYbCatalogVersionTableId);
    request.set_schema_version(0);
    request.set_stmt_type(PgsqlWriteRequestPB::PGSQL_UPDATE);
    request.add_range_column_values()->mutable_value()->set_uint32_value(1);
    auto* column = request.add_column_new_values();
    column->set_column_id(
        version_->schema().column_id(version_->schema().find_column("current_version")));
    column->mutable_expr()->mutable_value()->set_int64_value(value);
    for (const auto& id : version_->schema().column_ids())
      request.mutable_column_refs()->add_ids(id);
    LWPgsqlWriteRequestPB req(&arena);
    req.CopyFrom(request);
    LWPgsqlResponsePB response(&arena);
    RETURN_NOT_OK(catalog_->Apply(req, version_, &response, &sidecars, subtransaction));
    SCHECK_EQ(
        response.status(), PgsqlResponsePB::PGSQL_STATUS_OK, QLError, "Catalog update failed");
    return Status::OK();
  }

  Result<int64_t> Version() {
    ThreadSafeArena arena;
    LWPgsqlReadRequestPB request(&arena);
    request.set_client(YQL_CLIENT_PGSQL);
    request.dup_table_id(kPgYbCatalogVersionTableId);
    request.set_schema_version(0);
    request.set_is_forward_scan(true);
    request.set_limit(10);
    request.add_range_column_values()->mutable_value()->set_uint32_value(1);
    const auto id = version_->schema().column_id(version_->schema().find_column("current_version"));
    request.add_targets()->set_column_id(id);
    request.mutable_column_refs()->add_ids(id);
    auto db = docdb::DocDB::FromRegularUnbounded(VERIFY_RESULT(catalog_->ReadDB()));
    docdb::QLRocksDBStorage storage("joint", db, {});
    ScopedRWOperation pending(&operations_, CoarseTimePoint::max());
    RETURN_NOT_OK(pending);
    TransactionOperationContext txn;
    docdb::PgsqlReadOperationData data{
        .read_operation_data = docdb::ReadOperationData::FromSingleReadTime(catalog_->ReadTime()),
        .is_explicit_request_read_time = true,
        .request = request,
        .doc_read_context = *version_,
        .index_doc_read_context = nullptr,
        .txn_op_context = txn,
        .ql_storage = storage,
        .pending_op = pending,
        .vector_index = nullptr};
    LWPgsqlResponsePB response(&arena);
    WriteBuffer rows(1024);
    ReadRestartData restart;
    docdb::PgsqlReadOperation operation(data, response, &rows, &restart);
    VERIFY_RESULT(operation.Execute());
    auto values = VERIFY_RESULT(Values(rows.ToBuffer()));
    SCHECK_EQ(values.size(), 1, Corruption, "Missing catalog version");
    return values.front();
  }

  Result<PgsqlWriteRequestPB> Insert(const tablet::TableInfoPB& table, int64_t key, int64_t value) {
    Schema schema;
    RETURN_NOT_OK(SchemaFromPB(table.schema(), &schema));
    PgsqlWriteRequestPB request;
    request.set_client(YQL_CLIENT_PGSQL);
    request.set_table_id(table.table_id());
    request.set_schema_version(table.schema_version());
    request.set_stmt_type(PgsqlWriteRequestPB::PGSQL_INSERT);
    auto* key_expr = schema.num_hash_key_columns() > 0 ? request.add_partition_column_values()
                                                       : request.add_range_column_values();
    key_expr->mutable_value()->set_int64_value(key);
    auto* column = request.add_column_values();
    column->set_column_id(schema.column_id(schema.num_key_columns()));
    column->mutable_expr()->mutable_value()->set_int64_value(value);
    return request;
  }

  Status Write(
      const TransactionId& txn, const tablet::TableInfoPB& table, int64_t key, int64_t value,
      uint32_t subtransaction = 1) {
    ThreadSafeArena arena;
    LWPgsqlWriteRequestPB request(&arena);
    request.CopyFrom(VERIFY_RESULT(Insert(table, key, value)));
    LWPgsqlResponsePB response(&arena);
    rpc::Sidecars sidecars;
    RETURN_NOT_OK(catalog_->NativeTransactions().WriteSql(
        txn, request, &response, &sidecars, subtransaction));
    SCHECK_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK, AlreadyPresent, "Duplicate key");
    return Status::OK();
  }

  Result<PgsqlReadRequestPB> Scan(const tablet::TableInfoPB& table) {
    Schema schema;
    RETURN_NOT_OK(SchemaFromPB(table.schema(), &schema));
    PgsqlReadRequestPB request;
    request.set_client(YQL_CLIENT_PGSQL);
    request.set_table_id(table.table_id());
    request.set_schema_version(table.schema_version());
    request.set_is_forward_scan(true);
    request.set_limit(2);
    request.set_return_paging_state(true);
    const auto column = schema.column_id(schema.num_key_columns());
    request.add_targets()->set_column_id(column);
    request.mutable_column_refs()->add_ids(column);
    return request;
  }

  Result<std::vector<int64_t>> Rows(
      const TransactionId& txn, HybridTime own_limit = HybridTime::kInvalid) {
    ThreadSafeArena arena;
    LWPgsqlReadRequestPB request(&arena);
    request.CopyFrom(VERIFY_RESULT(Scan(VERIFY_RESULT(catalog_->GetTable(table_.table_id())))));
    std::vector<int64_t> result;
    for (size_t page = 0; page < 1000; ++page) {
      LWPgsqlResponsePB response(&arena);
      rpc::Sidecars sidecars;
      RETURN_NOT_OK(catalog_->NativeTransactions().ReadSql(
          txn, request, &response, &sidecars, CoarseTimePoint::max(), own_limit));
      std::string bytes;
      RETURN_NOT_OK(sidecars.AssignTo(response.rows_data_sidecar(), &bytes));
      auto rows = VERIFY_RESULT(Values(bytes));
      result.insert(result.end(), rows.begin(), rows.end());
      if (!response.has_paging_state() || (response.paging_state().next_row_key().empty() &&
                                           response.paging_state().next_partition_key().empty())) {
        std::sort(result.begin(), result.end());
        return result;
      }
      request.mutable_paging_state()->CopyFrom(response.paging_state());
    }
    return STATUS(IllegalState, "SQL paging did not terminate");
  }

  Result<std::pair<int64_t, bool>> Sequence(uint32_t database_oid, uint32_t sequence_oid) {
    const auto tuple = VERIFY_RESULT(catalog_->ReadSequence(database_oid, sequence_oid));
    return std::make_pair(tuple.last_value, tuple.is_called);
  }

  void Reopen() {
    catalog_.reset();
    catalog_ = ASSERT_RESULT(PgCatalogTransaction::Open(path_, Seed() + "/rocksdb"));
  }

  Result<std::vector<tablet::TableInfoPB>> PgClassSources() {
    tserver::ExportedTabletMetadataChanges changes;
    RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(
        Env::Default(), Seed() + "/exported_tablet_metadata_changes", &changes));
    std::vector<tablet::TableInfoPB> sources;
    const auto collect = [&](const tablet::TableInfoPB& table) {
      if (table.table_id() == GetPgsqlTableId(1, kPgClassTableOid)) sources.push_back(table);
    };
    for (const auto& change : changes.metadata_changes()) {
      if (change.has_add_table()) collect(change.add_table());
      for (const auto& table : change.add_multiple_tables()) collect(table);
    }
    SCHECK_EQ(sources.size(), 1, IllegalState, "Missing pg_class seed descriptor");
    return sources;
  }

  // Closes the catalog and opens its storage directly, for bulk synthetic seeding and inspection.
  Result<std::shared_ptr<PgLocalStorage>> OpenClosedStorage() {
    catalog_.reset();
    return PgLocalStorage::Open(path_, Seed() + "/rocksdb");
  }

  std::string path_;
  std::unique_ptr<PgCatalogTransaction> catalog_;
  std::shared_ptr<docdb::DocReadContext> version_;
  tablet::TableInfoPB table_;
  int64_t initial_;
  RWOperationCounter operations_{"joint-test"};
};

TEST_F(PgLocalSharedCommitTest, CatalogAndDataPublication) {
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Version(initial_ + 1, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  auto& native = catalog_->NativeTransactions();
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({10}));
  ASSERT_NOK(native.Commit(txn));
  ASSERT_NOK(native.Abort(txn));
  ASSERT_NOK(native.RollbackTo(txn, 1));
  ASSERT_NOK(native.Begin());
  ASSERT_NOK(catalog_->ReadDB(table_.table_id()));
  ASSERT_OK(catalog_->Commit());
  ASSERT_EQ(ASSERT_RESULT(Version()), initial_ + 1);
  const auto reader = ASSERT_RESULT(native.Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reader)), std::vector<int64_t>({10}));
  ASSERT_GT(ASSERT_RESULT(native.ReadPoint(reader)).read, catalog_->ReadTime());
  ASSERT_NOK(catalog_->Begin());
  ASSERT_NOK(catalog_->BeginWithNative());
  ASSERT_OK(native.Abort(reader));
  Reopen();
  const auto after = ASSERT_RESULT(catalog_->NativeTransactions().Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(after)), std::vector<int64_t>({10}));
  ASSERT_EQ(ASSERT_RESULT(Version()), initial_ + 1);
}

TEST_F(PgLocalSharedCommitTest, CoordinatedSavepoints) {
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Version(initial_ + 1, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  const auto rolled_back = ASSERT_RESULT(Table(1000005001));
  ASSERT_OK(catalog_->CreateTable(rolled_back, 2));
  ASSERT_OK(Write(txn, rolled_back, 2, 20, 2));
  ASSERT_OK(Write(txn, table_, 2, 20, 2));
  ASSERT_OK(Version(initial_ + 2, 2));
  ASSERT_OK(catalog_->RollbackTo(2));
  ASSERT_NOK(catalog_->GetTable(rolled_back.table_id()));
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({10}));
  ASSERT_EQ(ASSERT_RESULT(Version()), initial_ + 1);
  ASSERT_OK(catalog_->CreateTable(rolled_back, 3));
  ASSERT_OK(Write(txn, rolled_back, 3, 30, 3));
  ASSERT_OK(catalog_->RollbackTo(3));
  ASSERT_OK(Write(txn, table_, 3, 30, 4));
  ASSERT_OK(Version(initial_ + 1, 1));
  ASSERT_OK(catalog_->Commit());
  Reopen();
  ASSERT_NOK(catalog_->GetTable(rolled_back.table_id()));
  ASSERT_EQ(
      ASSERT_RESULT(Rows(ASSERT_RESULT(catalog_->NativeTransactions().Begin()))),
      std::vector<int64_t>({10, 30}));
  ASSERT_EQ(ASSERT_RESULT(Version()), initial_ + 1);
}

TEST_F(PgLocalSharedCommitTest, JointFailureRequiresRollback) {
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Version(initial_ + 1, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  ASSERT_NOK(Write(txn, table_, 1, 20, 2));
  ASSERT_NOK(catalog_->Commit());
  ASSERT_OK(catalog_->RollbackTo(2));
  ASSERT_OK(Write(txn, table_, 2, 20, 3));
  ASSERT_OK(catalog_->Commit());
  const auto discarded = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(Version(initial_ + 2, 1));
  ASSERT_OK(Write(discarded, table_, 3, 30));
  catalog_->Abort();
  ASSERT_EQ(ASSERT_RESULT(Version()), initial_ + 1);
  ASSERT_EQ(
      ASSERT_RESULT(Rows(ASSERT_RESULT(catalog_->NativeTransactions().Begin()))),
      std::vector<int64_t>({10, 20}));
}

TEST_F(PgLocalSharedCommitTest, AmbiguousJointCommitFencesBothEngines) {
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  ASSERT_OK(Version(initial_ + 1, 1));
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("PgCatalogTransaction::CommitStatus", [](void* status) {
    *static_cast<Status*>(status) = STATUS(IOError, "Ambiguous joint acknowledgement");
  });
  sync->EnableProcessing();
  ASSERT_TRUE(catalog_->Commit().IsIOError());
  for (size_t i = 0; i < 3; ++i) {
    catalog_->Abort();
    ASSERT_TRUE(catalog_->CheckHealthy().IsIOError());
    ASSERT_TRUE(ResultToStatus(catalog_->AllocateOid(1)).IsIOError());
    ASSERT_TRUE(ResultToStatus(catalog_->NativeTransactions().Begin()).IsIOError());
    ASSERT_TRUE(ResultToStatus(Rows(txn)).IsIOError());
  }
  sync->DisableProcessing();
  Reopen();
  ASSERT_EQ(ASSERT_RESULT(Version()), initial_ + 1);
  ASSERT_EQ(
      ASSERT_RESULT(Rows(ASSERT_RESULT(catalog_->NativeTransactions().Begin()))),
      std::vector<int64_t>({10}));
}

TEST_F(PgLocalSharedCommitTest, IntentWriteFailureFencesCatalog) {
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Version(initial_ + 1, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("PgLocalTxnStore::WriteStatus", [](void* status) {
    *static_cast<Status*>(status) = STATUS(IOError, "Ambiguous intent acknowledgement");
  });
  sync->EnableProcessing();
  ASSERT_TRUE(Write(txn, table_, 2, 20).IsIOError());
  ASSERT_TRUE(catalog_->Commit().IsIOError());
  catalog_->Abort();
  ASSERT_TRUE(catalog_->CheckHealthy().IsIOError());
  sync->DisableProcessing();
  Reopen();
  ASSERT_EQ(ASSERT_RESULT(Version()), initial_);
  ASSERT_TRUE(ASSERT_RESULT(catalog_->Tables()).empty());
  ASSERT_OK(catalog_->Begin());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(catalog_->Commit());
  ASSERT_TRUE(ASSERT_RESULT(Rows(ASSERT_RESULT(catalog_->NativeTransactions().Begin()))).empty());
}

TEST_F(PgLocalSharedCommitTest, IndependentSnapshotsExcludeCatalogDdl) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_enable_packed_row) = true;
  ASSERT_OK(catalog_->Begin());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ThreadSafeArena arena;
  rpc::Sidecars sidecars;
  auto registry = std::make_shared<dockv::SchemaPackingRegistry>("legacy-promotion");
  auto context = std::make_shared<docdb::DocReadContext>(
      "legacy-promotion", TableType::PGSQL_TABLE_TYPE, docdb::Index::kFalse, registry);
  ASSERT_OK(context->LoadFromPB(table_));
  PgsqlWriteRequestPB legacy;
  legacy.set_client(YQL_CLIENT_PGSQL);
  legacy.set_table_id(table_.table_id());
  legacy.set_schema_version(0);
  legacy.set_stmt_type(PgsqlWriteRequestPB::PGSQL_INSERT);
  legacy.set_hash_code(48975);
  legacy.add_partition_column_values()->mutable_value()->set_int64_value(42);
  auto* column = legacy.add_column_values();
  column->set_column_id(context->schema().column_id(1));
  column->mutable_expr()->mutable_value()->set_int64_value(5);
  LWPgsqlWriteRequestPB request(&arena);
  request.CopyFrom(legacy);
  LWPgsqlResponsePB response(&arena);
  ASSERT_OK(catalog_->Apply(request, context, &response, &sidecars, 1));
  ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
  ASSERT_OK(catalog_->Commit());
  auto& native = catalog_->NativeTransactions();
  const auto old = ASSERT_RESULT(native.Begin());
  const auto writer = ASSERT_RESULT(native.Begin());
  ASSERT_OK(Write(writer, table_, 1, 10));
  ASSERT_OK(native.Commit(writer));
  ASSERT_EQ(ASSERT_RESULT(Rows(old)), std::vector<int64_t>({5}));
  const auto later_limit = ASSERT_RESULT(native.ReadPoint(old)).in_txn_limit;
  ASSERT_EQ(ASSERT_RESULT(Rows(old, later_limit)), std::vector<int64_t>({5}));
  ASSERT_NOK(catalog_->Begin());
  ASSERT_OK(native.Abort(old));
  ASSERT_OK(catalog_->Begin());
  ASSERT_OK(Version(initial_ + 1, 1));
  ASSERT_NOK(native.Begin());
  ASSERT_OK(catalog_->IncrementSchemaVersion(table_.table_id(), 1));
  ASSERT_OK(catalog_->Commit());
  const auto now = ASSERT_RESULT(native.Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(now)), std::vector<int64_t>({5, 10}));
  ASSERT_OK(native.Abort(now));
  Reopen();
  ASSERT_EQ(
      ASSERT_RESULT(Rows(ASSERT_RESULT(catalog_->NativeTransactions().Begin()))),
      std::vector<int64_t>({5, 10}));
}

TEST_F(PgLocalSharedCommitTest, MissingNativeBindingStorageIsCorruption) {
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  ASSERT_OK(catalog_->Commit());
  catalog_.reset();
  rocksdb::Options options;
  docdb::InitRocksDBOptions(
      &options, "joint-corruption", "joint-corruption", nullptr, tablet::TabletOptions(),
      docdb::StorageDbType::kRegular);
  options.disable_wal = false;
  std::vector<std::string> names;
  ASSERT_OK(rocksdb::DB::ListColumnFamilies(options, path_, &names));
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  for (const auto& name : names) descriptors.emplace_back(name, options);
  rocksdb::DB* raw;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_OK(rocksdb::DB::Open(options, path_, descriptors, &handles, &raw));
  std::unique_ptr<rocksdb::DB> db(raw);
  bool removed = false;
  for (auto* handle : handles) {
    if (handle->GetName() == "intents/" + table_.table_id()) {
      ASSERT_OK(db->DropColumnFamily(handle));
      removed = true;
    }
  }
  for (auto* handle : handles) delete handle;
  db.reset();
  ASSERT_TRUE(removed);
  ASSERT_TRUE(
      ResultToStatus(PgCatalogTransaction::Open(path_, Seed() + "/rocksdb")).IsCorruption());
}

TEST_F(PgLocalSharedCommitTest, LiveParentCatalogWritesAfterRollback) {
  for (bool joint : {false, true}) {
    const auto before = ASSERT_RESULT(Version());
    if (joint)
      ASSERT_RESULT(catalog_->BeginWithNative());
    else
      ASSERT_OK(catalog_->Begin());
    ASSERT_OK(Version(before + 1, 1));
    ASSERT_OK(Version(before + 2, 2));
    ASSERT_OK(Version(before + 3, 3));
    ASSERT_OK(catalog_->RollbackTo(3));
    ASSERT_NOK(Version(before + 9, 3));
    ASSERT_NOK(catalog_->Commit());
    ASSERT_OK(catalog_->RollbackTo(3));
    ASSERT_OK(Version(before + 4, 2));
    ASSERT_OK(Version(before + 5, 1));
    ASSERT_OK(catalog_->Commit());
    ASSERT_EQ(ASSERT_RESULT(Version()), before + 5);
  }
}

TEST_F(PgLocalSharedCommitTest, SqlReturningAndStatementOwnWriteLimit) {
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  auto& native = catalog_->NativeTransactions();
  ASSERT_OK(Write(txn, table_, 1, 10));
  const auto before = ASSERT_RESULT(native.ReadPoint(txn));
  ThreadSafeArena arena;
  LWPgsqlWriteRequestPB request(&arena);
  const auto insert = ASSERT_RESULT(Insert(table_, 1, 10));
  auto update = insert;
  update.set_stmt_type(PgsqlWriteRequestPB::PGSQL_UPDATE);
  *update.add_column_new_values() = update.column_values(0);
  update.mutable_column_new_values(0)->mutable_expr()->mutable_value()->set_int64_value(11);
  const auto column = update.column_values(0).column_id();
  update.clear_column_values();
  update.add_targets()->set_column_id(column);
  update.mutable_column_refs()->add_ids(column);
  request.CopyFrom(update);
  LWPgsqlResponsePB response(&arena);
  rpc::Sidecars sidecars;
  sidecars.Start().Append(Slice("prefix"));
  ASSERT_EQ(sidecars.Complete(), 0);
  ASSERT_OK(native.WriteSql(txn, request, &response, &sidecars, 1));
  ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
  ASSERT_EQ(response.rows_data_sidecar(), 1);
  std::string bytes;
  ASSERT_OK(sidecars.AssignTo(response.rows_data_sidecar(), &bytes));
  ASSERT_EQ(ASSERT_RESULT(Values(bytes)), std::vector<int64_t>({11}));
  ASSERT_EQ(ASSERT_RESULT(Rows(txn, before.in_txn_limit)), std::vector<int64_t>({10}));
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({11}));
  ASSERT_EQ(ASSERT_RESULT(native.ReadPoint(txn)).read, before.read);

  request.CopyFrom(insert);
  ASSERT_OK(native.WriteSql(txn, request, &response, &sidecars, 2));
  ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_DUPLICATE_KEY_ERROR);
  ASSERT_FALSE(response.error_message().empty());
  ASSERT_NOK(catalog_->CheckHealthy());
  ASSERT_NOK(Version(initial_ + 1, 1));
  ASSERT_NOK(Rows(txn));
  ASSERT_NOK(catalog_->Commit());
  ASSERT_EQ(ASSERT_RESULT(Version()), initial_);
  ASSERT_OK(catalog_->RollbackTo(2));
  ASSERT_OK(Write(txn, table_, 2, 20, 1));
  ASSERT_OK(Version(initial_ + 1, 1));
  ASSERT_OK(catalog_->Commit());
  Reopen();
  ASSERT_EQ(
      ASSERT_RESULT(Rows(ASSERT_RESULT(catalog_->NativeTransactions().Begin()))),
      std::vector<int64_t>({11, 20}));
}

TEST_F(PgLocalSharedCommitTest, SqlMetadataVersionsAndPackingRollback) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_enable_packed_row) = true;
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  ASSERT_OK(catalog_->IncrementSchemaVersion(table_.table_id(), 1));
  auto altered = ASSERT_RESULT(catalog_->GetTable(table_.table_id()));
  ASSERT_EQ(altered.schema_version(), 1);
  ASSERT_FALSE(altered.old_schema_packings().empty());
  ASSERT_OK(Write(txn, altered, 2, 20));
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({10, 20}));
  ASSERT_OK(catalog_->IncrementSchemaVersion(table_.table_id(), 2));
  ASSERT_OK(Write(txn, ASSERT_RESULT(catalog_->GetTable(table_.table_id())), 3, 30, 2));
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({10, 20, 30}));
  ASSERT_OK(catalog_->RollbackTo(2));
  ASSERT_EQ(ASSERT_RESULT(catalog_->GetTable(table_.table_id())).schema_version(), 1);
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({10, 20}));
  ASSERT_OK(Write(txn, altered, 4, 40, 1));
  ASSERT_OK(catalog_->Commit());
  Reopen();
  ASSERT_EQ(
      ASSERT_RESULT(Rows(ASSERT_RESULT(catalog_->NativeTransactions().Begin()))),
      std::vector<int64_t>({10, 20, 40}));
}

TEST_F(PgLocalSharedCommitTest, SqlTypesAndSinglePartitionRange) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_enable_packed_row) = true;
  std::vector<std::pair<DataType, QLValuePB>> values;
  const auto add = [&](DataType type) -> QLValuePB* {
    return &values.emplace_back(type, QLValuePB()).second;
  };
  add(DataType::INT8)->set_int8_value(12);
  add(DataType::INT16)->set_int16_value(123);
  add(DataType::BOOL)->set_bool_value(true);
  add(DataType::FLOAT)->set_float_value(1.25f);
  add(DataType::DOUBLE)->set_double_value(2.5);
  add(DataType::DECIMAL)->set_decimal_value(util::Decimal("12.5").EncodeToComparable());
  add(DataType::INT32)->set_int32_value(1234);
  add(DataType::INT64)->set_int64_value(12345);
  add(DataType::STRING)->set_string_value("synthetic-value");
  add(DataType::BINARY)->set_binary_value(std::string("a\0b", 3));
  std::vector<tablet::TableInfoPB> tables;
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  for (size_t i = 0; i < values.size(); ++i) {
    auto table =
        ASSERT_RESULT(Table(1000005100 + static_cast<uint32_t>(i), i % 2 == 0, values[i].first));
    ASSERT_OK(catalog_->CreateTable(table, 1));
    auto insert = ASSERT_RESULT(Insert(table, 1, 0));
    *insert.mutable_column_values(0)->mutable_expr()->mutable_value() = values[i].second;
    ThreadSafeArena arena;
    LWPgsqlWriteRequestPB request(&arena);
    request.CopyFrom(insert);
    LWPgsqlResponsePB response(&arena);
    rpc::Sidecars sidecars;
    ASSERT_OK(catalog_->NativeTransactions().WriteSql(txn, request, &response, &sidecars, 1));
    ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
    ASSERT_OK(catalog_->IncrementSchemaVersion(table.table_id(), 1));
    tables.push_back(ASSERT_RESULT(catalog_->GetTable(table.table_id())));
  }
  ASSERT_OK(catalog_->Commit());
  Reopen();
  const auto reader = ASSERT_RESULT(catalog_->NativeTransactions().Begin());
  for (size_t i = 0; i < tables.size(); ++i) {
    ThreadSafeArena arena;
    auto scan = ASSERT_RESULT(Scan(tables[i]));
    scan.set_is_forward_scan(false);
    scan.set_limit(10);
    scan.set_return_paging_state(false);
    scan.add_partition_column_values()->mutable_value()->set_int64_value(1);
    if (tables[i].schema().table_properties().num_tablets() == 1) {
      scan.clear_partition_column_values();
      scan.add_range_column_values()->mutable_value()->set_int64_value(1);
    }
    LWPgsqlReadRequestPB request(&arena);
    request.CopyFrom(scan);
    LWPgsqlResponsePB response(&arena);
    rpc::Sidecars sidecars;
    ASSERT_OK(catalog_->NativeTransactions().ReadSql(
        reader, request, &response, &sidecars, CoarseTimePoint::max(), HybridTime::kInvalid));
    std::string bytes;
    ASSERT_OK(sidecars.AssignTo(response.rows_data_sidecar(), &bytes));
    WriteBuffer expected(1024);
    pggate::PgWire::WriteInt64(1, &expected);
    ASSERT_OK(pggate::WriteColumn(values[i].second, &expected));
    ASSERT_EQ(bytes, expected.ToBuffer());
  }
}

TEST_F(PgLocalSharedCommitTest, SqlIndexBindingsRollbackAndRecreate) {
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  const auto index = ASSERT_RESULT(Table(1000005200, true, DataType::INT64, 1000005000));
  auto insert = ASSERT_RESULT(Insert(index, 10, 100));
  insert.add_range_column_values()->mutable_value()->set_binary_value("base-row-key");
  ThreadSafeArena arena;
  LWPgsqlWriteRequestPB request(&arena);
  request.CopyFrom(insert);
  LWPgsqlResponsePB response(&arena);
  rpc::Sidecars sidecars;
  for (uint32_t subtransaction : {2, 3}) {
    ASSERT_OK(catalog_->CreateTable(index, subtransaction));
    ASSERT_OK(catalog_->NativeTransactions().WriteSql(
        txn, request, &response, &sidecars, subtransaction));
    ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
    ASSERT_EQ(ASSERT_RESULT(catalog_->GetTable(table_.table_id())).indexes_size(), 1);
    ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({10}));
    if (subtransaction == 2) {
      ASSERT_OK(catalog_->RollbackTo(2));
      ASSERT_NOK(catalog_->GetTable(index.table_id()));
      ASSERT_TRUE(ASSERT_RESULT(catalog_->GetTable(table_.table_id())).indexes().empty());
    }
  }
  ASSERT_OK(catalog_->IncrementSchemaVersion(index.table_id(), 1));
  ASSERT_EQ(ASSERT_RESULT(catalog_->GetTable(index.table_id())).schema_version(), 1);
  ASSERT_OK(catalog_->Commit());
  Reopen();
  const auto reader = ASSERT_RESULT(catalog_->NativeTransactions().Begin());
  LWPgsqlReadRequestPB scan(&arena);
  scan.CopyFrom(ASSERT_RESULT(Scan(ASSERT_RESULT(catalog_->GetTable(index.table_id())))));
  scan.add_partition_column_values()->mutable_value()->set_int64_value(10);
  ASSERT_OK(catalog_->NativeTransactions().ReadSql(
      reader, scan, &response, &sidecars, CoarseTimePoint::max(), HybridTime::kInvalid));
  std::string bytes;
  ASSERT_OK(sidecars.AssignTo(response.rows_data_sidecar(), &bytes));
  ASSERT_EQ(ASSERT_RESULT(Values(bytes)), std::vector<int64_t>({100}));
  ASSERT_NOK(catalog_->ReadDB(index.table_id()));
}

TEST_F(PgLocalSharedCommitTest, AlterPackedRowsRollbackAndColumnIds) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_enable_packed_row) = true;
  auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  ASSERT_OK(catalog_->Commit());
  tserver::PgAlterTableRequestPB alter;
  alter.set_use_transaction(true);
  alter.set_use_regular_transaction_block(true);
  alter.mutable_table_id()->set_database_oid(1);
  alter.mutable_table_id()->set_object_oid(1000005000);
  auto* column = alter.add_add_columns();
  column->set_attr_name("x");
  column->set_attr_num(3);
  column->set_attr_pgoid(20);
  column->set_attr_ybtype(static_cast<int32_t>(DataType::INT64));
  column->mutable_attr_missing_val()->set_int64_value(99);
  txn = ASSERT_RESULT(catalog_->BeginWithNative());
  alter.mutable_options()->set_active_sub_transaction_id(2);
  ASSERT_OK(catalog_->AlterTable(alter));
  auto changed = ASSERT_RESULT(catalog_->GetTable(table_.table_id()));
  ASSERT_EQ(changed.schema().columns(2).order(), 3);
  ASSERT_EQ(changed.schema().columns(2).pg_type_oid(), 20);
  ASSERT_FALSE(changed.old_schema_packings().empty());
  ASSERT_OK(Write(txn, changed, 2, 20, 2));
  ASSERT_OK(catalog_->RollbackTo(2));
  ASSERT_EQ(
      ASSERT_RESULT(catalog_->GetTable(table_.table_id())).SerializeAsString(),
      table_.SerializeAsString());
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({10}));
  alter.mutable_options()->set_active_sub_transaction_id(3);
  ASSERT_OK(catalog_->AlterTable(alter));
  alter.clear_add_columns();
  alter.add_drop_columns("v");
  ASSERT_OK(catalog_->AlterTable(alter));
  changed = ASSERT_RESULT(catalog_->GetTable(table_.table_id()));
  const auto dropped_id = changed.schema().columns(1).id();
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({99}));
  ASSERT_OK(catalog_->Commit());
  Reopen();
  txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({99}));
  alter.clear_drop_columns();
  alter.add_drop_columns("x");
  alter.mutable_options()->set_active_sub_transaction_id(1);
  ASSERT_OK(catalog_->AlterTable(alter));
  ASSERT_OK(catalog_->Commit());
  Reopen();
  txn = ASSERT_RESULT(catalog_->BeginWithNative());
  alter.clear_drop_columns();
  column = alter.add_add_columns();
  column->set_attr_name("x");
  column->set_attr_num(4);
  column->set_attr_pgoid(20);
  column->set_attr_ybtype(static_cast<int32_t>(DataType::INT64));
  column->mutable_attr_missing_val()->set_int64_value(101);
  ASSERT_OK(catalog_->AlterTable(alter));
  changed = ASSERT_RESULT(catalog_->GetTable(table_.table_id()));
  ASSERT_GT(changed.schema().columns(1).id(), dropped_id);
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({101}));
  ASSERT_OK(catalog_->Commit());
}

TEST_F(PgLocalSharedCommitTest, DropDescriptorsRollbackAndReopen) {
  auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  const auto index = ASSERT_RESULT(Table(1000005200, true, DataType::INT64, 1000005000));
  ASSERT_OK(catalog_->CreateTable(index, 1));
  auto insert = ASSERT_RESULT(Insert(index, 10, 100));
  insert.add_range_column_values()->mutable_value()->set_binary_value("base-row-key");
  ThreadSafeArena arena;
  LWPgsqlWriteRequestPB request(&arena);
  request.CopyFrom(insert);
  LWPgsqlResponsePB response(&arena);
  rpc::Sidecars sidecars;
  ASSERT_OK(catalog_->NativeTransactions().WriteSql(txn, request, &response, &sidecars, 1));
  ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
  ASSERT_OK(catalog_->Commit());
  txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->DropTable(index.table_id(), 2));
  ASSERT_TRUE(ASSERT_RESULT(catalog_->GetTable(table_.table_id())).indexes().empty());
  ASSERT_OK(catalog_->RollbackTo(2));
  ASSERT_EQ(ASSERT_RESULT(catalog_->GetTable(table_.table_id())).indexes_size(), 1);
  ASSERT_OK(catalog_->DropTable(table_.table_id(), 3));
  ASSERT_TRUE(ASSERT_RESULT(catalog_->Tables()).empty());
  ASSERT_OK(catalog_->RollbackTo(4));
  // Rolling back a later savepoint must not reclaim a still-pending DROP's storage.
  catalog_->Abort();
  Reopen();
  txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({10}));
  ASSERT_EQ(ASSERT_RESULT(catalog_->GetTable(table_.table_id())).indexes_size(), 1);
  ASSERT_OK(catalog_->DropTable(index.table_id(), 1));
  ASSERT_OK(catalog_->Commit());
  Reopen();
  ASSERT_TRUE(ASSERT_RESULT(catalog_->GetTable(table_.table_id())).indexes().empty());
  ASSERT_TRUE(catalog_->GetTable(index.table_id()).status().IsNotFound());
  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->DropTable(table_.table_id(), 1));
  ASSERT_OK(catalog_->Commit());
  Reopen();
  ASSERT_TRUE(ASSERT_RESULT(catalog_->Tables()).empty());
}

TEST_F(PgLocalSharedCommitTest, SqlYbctidRoutingAcrossHashPartitions) {
  auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  Schema schema;
  ASSERT_OK(SchemaFromPB(table_.schema(), &schema));
  const auto before = ASSERT_RESULT(catalog_->NativeTransactions().ReadPoint(txn));
  const std::vector<std::pair<int64_t, uint16_t>> witnesses = {
      {65535, 9647}, {-1, 22565}, {42, 48975}, {1, 60841}};
  for (const auto& witness : witnesses) {
    ASSERT_OK(Write(txn, table_, witness.first, witness.first + 100));
  }
  for (int phase = 0; phase < 2; ++phase) {
    for (size_t partition = 0; partition < witnesses.size(); ++partition) {
      const auto [key, hash] = witnesses[partition];
      ASSERT_EQ(hash / 16384, partition);
      const auto row_id =
          dockv::DocKey(schema, hash, {dockv::KeyEntryValue::Int64(key)}).Encode().ToStringBuffer();
      for (bool forward : {true, false}) {
        // Keep both normal PgGate bounded paths alongside the unbounded scalar regression.
        for (int mode = 0; mode < 3; ++mode) {
          auto scan = ASSERT_RESULT(Scan(table_));
          scan.set_is_forward_scan(forward);
          scan.mutable_ybctid_column_value()->mutable_value()->set_binary_value(row_id);
          if (mode != 0) {
            scan.mutable_lower_bound()->set_key(row_id);
            scan.mutable_lower_bound()->set_is_inclusive(true);
            scan.mutable_upper_bound()->set_key(row_id);
            scan.mutable_upper_bound()->set_is_inclusive(true);
          }
          if (mode == 2) {
            *scan.add_batch_arguments()->mutable_ybctid() = scan.ybctid_column_value();
            scan.clear_ybctid_column_value();
          }
          ThreadSafeArena arena;
          LWPgsqlReadRequestPB request(&arena);
          request.CopyFrom(scan);
          for (bool old_limit : {false, true}) {
            if (old_limit && phase != 0) continue;
            LWPgsqlResponsePB response(&arena);
            rpc::Sidecars sidecars;
            sidecars.Start().Append(Slice("prefix"));
            ASSERT_EQ(sidecars.Complete(), 0);
            ASSERT_OK(catalog_->NativeTransactions().ReadSql(
                txn, request, &response, &sidecars, CoarseTimePoint::max(),
                old_limit ? before.in_txn_limit : HybridTime::kInvalid));
            ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
            ASSERT_EQ(response.rows_data_sidecar(), 1);
            std::string bytes;
            ASSERT_OK(sidecars.AssignTo(response.rows_data_sidecar(), &bytes));
            ASSERT_EQ(
                ASSERT_RESULT(Values(bytes)),
                old_limit ? std::vector<int64_t>() : std::vector<int64_t>({key + 100}));
            ASSERT_TRUE(response.paging_state().next_row_key().empty());
            if (mode == 2) {
              ASSERT_EQ(response.batch_arg_count(), 1);
            } else {
              ASSERT_TRUE(response.paging_state().next_partition_key().empty());
            }
            ASSERT_OK(sidecars.AssignTo(0, &bytes));
            ASSERT_EQ(bytes, "prefix");
          }
        }
      }
    }
    if (phase == 0) {
      ASSERT_EQ(ASSERT_RESULT(catalog_->NativeTransactions().ReadPoint(txn)).read, before.read);
      ASSERT_OK(catalog_->Commit());
      Reopen();
      txn = ASSERT_RESULT(catalog_->BeginWithNative());
    }
  }
  ASSERT_OK(catalog_->Commit());
}

TEST_F(PgLocalSharedCommitTest, SqlYbctidRangeSinglePartition) {
  auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  const auto table = ASSERT_RESULT(Table(1000005001, false));
  ASSERT_OK(catalog_->CreateTable(table, 1));
  ASSERT_OK(Write(txn, table, 42, 142));
  Schema schema;
  ASSERT_OK(SchemaFromPB(table.schema(), &schema));
  const auto row_id =
      dockv::DocKey(schema, {dockv::KeyEntryValue::Int64(42)}).Encode().ToStringBuffer();
  for (int phase = 0; phase < 2; ++phase) {
    for (bool forward : {true, false}) {
      auto scan = ASSERT_RESULT(Scan(table));
      scan.set_is_forward_scan(forward);
      scan.mutable_ybctid_column_value()->mutable_value()->set_binary_value(row_id);
      ThreadSafeArena arena;
      LWPgsqlReadRequestPB request(&arena);
      request.CopyFrom(scan);
      LWPgsqlResponsePB response(&arena);
      rpc::Sidecars sidecars;
      ASSERT_OK(catalog_->NativeTransactions().ReadSql(
          txn, request, &response, &sidecars, CoarseTimePoint::max(), HybridTime::kInvalid));
      ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
      std::string bytes;
      ASSERT_OK(sidecars.AssignTo(response.rows_data_sidecar(), &bytes));
      ASSERT_EQ(ASSERT_RESULT(Values(bytes)), std::vector<int64_t>({142}));
      ASSERT_TRUE(response.paging_state().next_partition_key().empty());
    }
    if (phase == 0) {
      ASSERT_OK(catalog_->Commit());
      Reopen();
      txn = ASSERT_RESULT(catalog_->BeginWithNative());
    }
  }
  ASSERT_OK(catalog_->Commit());
}

TEST_F(PgLocalSharedCommitTest, SqlYbctidRejectionAndRollback) {
  const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  Schema schema;
  ASSERT_OK(SchemaFromPB(table_.schema(), &schema));
  const auto row_id =
      dockv::DocKey(schema, 60841, {dockv::KeyEntryValue::Int64(1)}).Encode().ToStringBuffer();
  const auto other_id =
      dockv::DocKey(schema, 9647, {dockv::KeyEntryValue::Int64(65535)}).Encode().ToStringBuffer();
  for (int mode = 0; mode < 8; ++mode) {
    auto scan = ASSERT_RESULT(Scan(table_));
    auto* expr = scan.mutable_ybctid_column_value();
    switch (mode) {
      case 0:
        expr->set_column_id(schema.column_id(0));
        break;
      case 1:
        expr->mutable_value()->set_int64_value(1);
        break;
      case 2:
        expr->mutable_value()->set_binary_value("");
        break;
      case 3:
        expr->mutable_value()->set_binary_value(row_id.substr(0, 2));
        break;
      case 4:
        expr->mutable_value()->set_binary_value(
            dockv::DocKey({dockv::KeyEntryValue::Int64(1)}).Encode().ToStringBuffer());
        break;
      default:
        scan.clear_ybctid_column_value();
        scan.add_batch_arguments()->mutable_ybctid()->mutable_value()->set_binary_value(row_id);
        if (mode == 6) {
          scan.add_batch_arguments()->mutable_ybctid()->mutable_value()->set_binary_value(other_id);
        } else if (mode == 7) {
          scan.add_batch_arguments()->mutable_ybctid()->mutable_value()->set_int64_value(1);
        }
        break;
    }
    ThreadSafeArena arena;
    LWPgsqlReadRequestPB request(&arena);
    request.CopyFrom(scan);
    LWPgsqlResponsePB response(&arena);
    rpc::Sidecars sidecars;
    const auto subtransaction = static_cast<uint32_t>(mode + 2);
    const auto status = catalog_->NativeTransactions().ReadSql(
        txn, request, &response, &sidecars, CoarseTimePoint::max(), HybridTime::kInvalid,
        subtransaction);
    ASSERT_NOK(status);
    if (mode >= 5) ASSERT_TRUE(status.IsNotSupported()) << status;
    ASSERT_FALSE(response.has_rows_data_sidecar());
    ASSERT_NOK(catalog_->CheckHealthy());
    ASSERT_NOK(Rows(txn));
    ASSERT_NOK(catalog_->Commit());
    ASSERT_OK(catalog_->RollbackTo(subtransaction));
    ASSERT_OK(catalog_->CheckHealthy());
    ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({10}));
  }
  ASSERT_OK(catalog_->Commit());
  Reopen();
  ASSERT_EQ(
      ASSERT_RESULT(Rows(ASSERT_RESULT(catalog_->NativeTransactions().Begin()))),
      std::vector<int64_t>({10}));
}

TEST_F(PgLocalSharedCommitTest, SqlRejectsUnsupportedReadModes) {
  auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  ASSERT_OK(catalog_->Commit());
  for (int mode = 0; mode < 7; ++mode) {
    txn = ASSERT_RESULT(catalog_->BeginWithNative());
    ThreadSafeArena arena;
    LWPgsqlReadRequestPB request(&arena);
    request.CopyFrom(ASSERT_RESULT(Scan(table_)));
    switch (mode) {
      case 0:
        request.set_row_mark_type(ROW_MARK_EXCLUSIVE);
        break;
      case 1:
        request.set_skip_intents_read(true);
        break;
      case 2:
        request.set_read_at_in_txn_limit(true);
        break;
      case 3:
        request.set_is_for_backfill(true);
        break;
      case 4:
        request.mutable_index_request()->CopyFrom(ASSERT_RESULT(Scan(table_)));
        break;
      case 5:
        request.set_is_forward_scan(false);
        break;
      case 6:
        ReadHybridTime::SingleTime(HybridTime::kInitial)
            .ToPB(request.mutable_paging_state()->mutable_read_time());
        break;
    }
    LWPgsqlResponsePB response(&arena);
    rpc::Sidecars sidecars;
    const auto status = catalog_->NativeTransactions().ReadSql(
        txn, request, &response, &sidecars, CoarseTimePoint::max(), HybridTime::kInvalid);
    ASSERT_TRUE(status.IsNotSupported()) << status;
    ASSERT_NOK(catalog_->CheckHealthy());
    ASSERT_NOK(catalog_->Commit());
    catalog_->Abort();
  }
}

TEST_F(PgLocalSharedCommitTest, CrashPublication) {
  if (std::getenv("YB_LOCAL_JOINT_CRASH_PATH")) {
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack(
        std::getenv("YB_LOCAL_JOINT_CRASH_POINT"), [](void*) { std::raise(SIGKILL); });
    sync->EnableProcessing();
    const auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
    ASSERT_OK(catalog_->CreateTable(table_, 1));
    ASSERT_OK(Version(initial_ + 1, 1));
    ASSERT_OK(Write(txn, table_, 1, 10));
    if (std::string(std::getenv("YB_LOCAL_JOINT_CRASH_POINT")) ==
        "PgLocalTxnStore::BindingDropped") {
      catalog_->Abort();
    } else {
      ASSERT_OK(catalog_->Commit());
    }
    FAIL() << "Expected SIGKILL";
    return;
  }
  catalog_.reset();
  std::string executable;
  ASSERT_OK(Env::Default()->GetExecutablePath(&executable));
  for (const auto* point :
       {"FamilyCreated", "BindingFamilyCreated", "BindingDropped", "BeforeCommit", "AfterCommit"}) {
    path_ = GetTestPath(point);
    Subprocess child(
        executable, {executable, "--gtest_filter=PgLocalSharedCommitTest.CrashPublication"});
    child.SetEnv("YB_LOCAL_JOINT_CRASH_PATH", path_);
    child.SetEnv(
        "YB_LOCAL_JOINT_CRASH_POINT",
        std::string(
            std::string(point).starts_with("Binding") ? "PgLocalTxnStore::"
                                                      : "PgCatalogTransaction::") +
            point);
    child.SetEnv("TEST_TMPDIR", GetTestPath(std::string("child-") + point));
    ASSERT_OK(child.Start());
    int code;
    ASSERT_OK(child.Wait(&code));
    ASSERT_TRUE(WIFSIGNALED(code));
    ASSERT_EQ(WTERMSIG(code), SIGKILL);
    Reopen();
    const bool committed = std::string(point) == "AfterCommit";
    ASSERT_EQ(ASSERT_RESULT(Version()), initial_ + (committed ? 1 : 0));
    ASSERT_EQ(ASSERT_RESULT(catalog_->Tables()).size(), committed ? 1 : 0);
    if (committed) {
      const auto reader = ASSERT_RESULT(catalog_->NativeTransactions().Begin());
      ASSERT_EQ(ASSERT_RESULT(Rows(reader)), std::vector<int64_t>({10}));
      ASSERT_OK(catalog_->NativeTransactions().Abort(reader));
    }
    catalog_.reset();
  }
}

TEST_F(PgLocalSharedCommitTest, DropDatabaseStorageRollbackAndReopen) {
  tserver::ExportedTabletMetadataChanges changes;
  ASSERT_OK(
      pb_util::ReadPBContainerFromPath(
          Env::Default(), Seed() + "/exported_tablet_metadata_changes", &changes));
  std::vector<tablet::TableInfoPB> sources;
  const auto collect = [&](const tablet::TableInfoPB& table) {
    if (table.table_id() == GetPgsqlTableId(1, kPgClassTableOid)) sources.push_back(table);
  };
  for (const auto& change : changes.metadata_changes()) {
    if (change.has_add_table()) collect(change.add_table());
    for (const auto& table : change.add_multiple_tables()) collect(table);
  }
  ASSERT_EQ(sources.size(), 1);
  const auto count_keys = [&]() -> Result<size_t> {
    auto* db = VERIFY_RESULT(catalog_->ReadDB());
    std::unique_ptr<rocksdb::Iterator> iterator(db->NewIterator(rocksdb::ReadOptions()));
    size_t count = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) ++count;
    RETURN_NOT_OK(iterator->status());
    return count;
  };
  const auto seed_keys = ASSERT_RESULT(count_keys());
  uint32_t oid = 1000005300;
  const std::string name = "drop_storage_test";
  ASSERT_NOK(catalog_->DropDatabase(oid, name, 1));
  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_NOK(catalog_->DropDatabase(1, "template1", 2));
  ASSERT_NOK(catalog_->CheckHealthy());
  ASSERT_OK(catalog_->RollbackTo(2));
  catalog_->Abort();

  auto txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateDatabase(oid, name, sources, 1));
  const auto cloned_keys = ASSERT_RESULT(count_keys());
  ASSERT_GT(cloned_keys, seed_keys);
  ASSERT_OK(catalog_->DropDatabase(oid, name, 2));
  ASSERT_TRUE(ASSERT_RESULT(catalog_->CatalogTables()).empty());
  ASSERT_EQ(ASSERT_RESULT(count_keys()), cloned_keys);
  ASSERT_OK(catalog_->RollbackTo(2));
  ASSERT_EQ(ASSERT_RESULT(count_keys()), cloned_keys);
  ASSERT_EQ(ASSERT_RESULT(catalog_->CatalogTables()).size(), 1);
  ASSERT_OK(catalog_->DropDatabase(oid, name, 3));
  ASSERT_OK(catalog_->Commit());
  Reopen();
  ASSERT_EQ(ASSERT_RESULT(count_keys()), seed_keys);
  ASSERT_TRUE(ASSERT_RESULT(catalog_->CatalogTables()).empty());

  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_TRUE(catalog_->CreateDatabase(oid, name, sources, 1).IsAlreadyPresent());
  catalog_->Abort();
  ++oid;  // The name is reusable, but the retired OID is not.
  txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateDatabase(oid, name, sources, 1));
  table_.set_table_id(GetPgsqlTableId(oid, 1000005000));
  table_.set_namespace_id(GetPgsqlNamespaceId(oid));
  table_.set_namespace_name(name);
  ASSERT_OK(catalog_->CreateTable(table_, 1));
  ASSERT_OK(Write(txn, table_, 1, 10));
  auto index = ASSERT_RESULT(Table(1000005200, true, DataType::INT64, 1000005000));
  index.set_table_id(GetPgsqlTableId(oid, 1000005200));
  index.set_namespace_id(GetPgsqlNamespaceId(oid));
  index.set_namespace_name(name);
  index.mutable_index_info()->set_table_id(index.table_id());
  index.mutable_index_info()->set_indexed_table_id(table_.table_id());
  ASSERT_OK(catalog_->CreateTable(index, 1));
  ASSERT_OK(catalog_->Commit());
  Reopen();
  txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_NOK(catalog_->DropDatabase(oid, "wrong_name", 2));
  ASSERT_NOK(catalog_->Commit());
  ASSERT_OK(catalog_->RollbackTo(2));
  ASSERT_OK(catalog_->DropDatabase(oid, name, 3));
  ASSERT_TRUE(ASSERT_RESULT(catalog_->Tables()).empty());
  ASSERT_GT(ASSERT_RESULT(count_keys()), seed_keys);
  ASSERT_OK(catalog_->RollbackTo(4));
  catalog_->Abort();
  Reopen();
  txn = ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)), std::vector<int64_t>({10}));
  ASSERT_EQ(ASSERT_RESULT(catalog_->GetTable(table_.table_id())).indexes_size(), 1);
  ASSERT_OK(catalog_->DropDatabase(oid, name, 1));
  ASSERT_OK(catalog_->Commit());
  Reopen();
  ASSERT_TRUE(ASSERT_RESULT(catalog_->Tables()).empty());
  ASSERT_TRUE(ASSERT_RESULT(catalog_->CatalogTables()).empty());
  ASSERT_EQ(ASSERT_RESULT(count_keys()), seed_keys);
  ASSERT_EQ(ASSERT_RESULT(Version()), initial_);
}

TEST_F(PgLocalSharedCommitTest, DropDatabaseChunkedCleanupCrash) {
  constexpr uint32_t oid = 1000005400;
  const std::string name = "retirement_test";
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  if (std::getenv("YB_LOCAL_JOINT_CRASH_PATH")) {
    sync->SetCallBack(
        std::getenv("YB_LOCAL_JOINT_CRASH_POINT"), [](void*) { std::raise(SIGKILL); });
    sync->EnableProcessing();
    ASSERT_RESULT(catalog_->BeginWithNative());
    ASSERT_OK(catalog_->DropDatabase(oid, name, 1));
    ASSERT_OK(catalog_->Commit());
    FAIL() << "Expected SIGKILL";
    return;
  }
  tserver::ExportedTabletMetadataChanges changes;
  ASSERT_OK(
      pb_util::ReadPBContainerFromPath(
          Env::Default(), Seed() + "/exported_tablet_metadata_changes", &changes));
  std::vector<tablet::TableInfoPB> sources;
  const auto collect = [&](const tablet::TableInfoPB& table) {
    if (table.table_id() == GetPgsqlTableId(1, kPgClassTableOid)) sources.push_back(table);
  };
  for (const auto& change : changes.metadata_changes()) {
    if (change.has_add_table()) collect(change.add_table());
    for (const auto& table : change.add_multiple_tables()) collect(table);
  }
  ASSERT_EQ(sources.size(), 1);
  const auto count_keys = [&]() -> Result<size_t> {
    auto* db = VERIFY_RESULT(catalog_->ReadDB());
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
    size_t count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) ++count;
    RETURN_NOT_OK(it->status());
    return count;
  };
  std::string executable;
  ASSERT_OK(Env::Default()->GetExecutablePath(&executable));
  for (const auto* point : {"BeforeRetiredDatabaseCleanup", "RetiredDatabaseCleanupBatch"}) {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
    catalog_.reset();
    path_ = GetTestPath(point);
    Reopen();
    ASSERT_RESULT(catalog_->BeginWithNative());
    ASSERT_OK(catalog_->CreateDatabase(oid + 1, "surviving_database", sources, 1));
    ASSERT_OK(catalog_->Commit());
    const auto surviving_keys = ASSERT_RESULT(count_keys());
    ASSERT_RESULT(catalog_->BeginWithNative());
    ASSERT_OK(catalog_->CreateDatabase(oid, name, sources, 1));
    ASSERT_OK(catalog_->Commit());
    catalog_.reset();
    rocksdb::Options options;
    docdb::InitRocksDBOptions(
        &options, "retirement", "retirement", nullptr, tablet::TabletOptions(),
        docdb::StorageDbType::kRegular);
    options.disable_wal = false;
    std::vector<std::string> names;
    ASSERT_OK(rocksdb::DB::ListColumnFamilies(options, path_, &names));
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    for (const auto& family : names) descriptors.emplace_back(family, options);
    rocksdb::DB* raw;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    ASSERT_OK(rocksdb::DB::Open(options, path_, descriptors, &handles, &raw));
    std::unique_ptr<rocksdb::DB> db(raw);
    auto close = ScopeExit([&] {
      for (auto* handle : handles) delete handle;
      db.reset();
    });
    dockv::DocKey row(ASSERT_RESULT(Uuid::FromHexString(GetPgsqlTableId(oid, kPgClassTableOid))));
    row.AddRangeComponent(dockv::KeyEntryValue(std::string(2048, 'x')));
    const auto prefix = row.Encode().ToStringBuffer();
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    // More than 16 MiB of historical keys, without a large staging transaction.
    for (int i = 0; i < 10000; ++i) {
      batch.Put(
          prefix + dockv::KeyEntryValue(DocHybridTime(HybridTime(100 + i), 0))
                       .ToKeyBytes()
                       .ToStringBuffer(),
          "synthetic");
      if (i % 100 == 99) {
        ASSERT_OK(db->Write(write_options, &batch));
        batch.Clear();
      }
    }
    for (auto* handle : handles) delete handle;
    handles.clear();
    db.reset();
    Subprocess child(
        executable,
        {executable, "--gtest_filter=PgLocalSharedCommitTest.DropDatabaseChunkedCleanupCrash"});
    child.SetEnv("YB_LOCAL_JOINT_CRASH_PATH", path_);
    child.SetEnv("YB_LOCAL_JOINT_CRASH_POINT", std::string("PgCatalogTransaction::") + point);
    child.SetEnv("TEST_TMPDIR", GetTestPath(std::string("child-") + point));
    ASSERT_OK(child.Start());
    int code;
    ASSERT_OK(child.Wait(&code));
    ASSERT_TRUE(WIFSIGNALED(code));
    ASSERT_EQ(WTERMSIG(code), SIGKILL);
    size_t batches = 0;
    sync->SetCallBack("PgCatalogTransaction::RetiredDatabaseCleanupBatch", [&](void* value) {
      ASSERT_LE(static_cast<rocksdb::WriteBatch*>(value)->GetDataSize(), 256 * 1024);
      ++batches;
    });
    sync->EnableProcessing();
    Reopen();
    ASSERT_GT(batches, 1);
    ASSERT_EQ(ASSERT_RESULT(count_keys()), surviving_keys);
    ASSERT_EQ(ASSERT_RESULT(Version()), initial_);
    ASSERT_EQ(ASSERT_RESULT(catalog_->CatalogTables()).size(), 1);
    ASSERT_RESULT(catalog_->BeginWithNative());
    ASSERT_TRUE(catalog_->CreateDatabase(oid, name, sources, 1).IsAlreadyPresent());
    catalog_->Abort();
    const auto completed_batches = batches;
    Reopen();
    ASSERT_EQ(batches, completed_batches);
  }
}

TEST_F(PgLocalSharedCommitTest, SequenceStateSurvivesRollbackAndReopen) {
  using Tuple = PgCatalogTransaction::SequenceTuple;
  using Range = std::pair<int64_t, int64_t>;
  using State = std::pair<int64_t, bool>;
  constexpr uint32_t kDatabase = 1;
  constexpr uint32_t kUp = 1000005100;
  constexpr uint32_t kDown = kUp + 1;
  constexpr uint32_t kCas = kUp + 2;
  constexpr auto kMax = std::numeric_limits<int64_t>::max();
  const auto fetch = [&](uint32_t sequence, uint32_t count, int64_t step, int64_t min,
                         int64_t max, bool cycle) {
    return catalog_->FetchSequence(kDatabase, sequence, count, step, min, max, cycle);
  };
  const auto update = [&](uint32_t sequence, Tuple value, std::optional<Tuple> expected) {
    return catalog_->UpdateSequence(kDatabase, sequence, value, expected);
  };
  ASSERT_OK(catalog_->InsertSequence(kDatabase, kUp, Tuple{kMax - 9, false}));
  ASSERT_NOK(catalog_->InsertSequence(kDatabase, kUp, Tuple{1, false}));
  ASSERT_TRUE(catalog_->InsertSequence(0, kDown, Tuple{1, false}).IsInvalidArgument());
  ASSERT_TRUE(catalog_->InsertSequence(kDatabase, 1, Tuple{1, false}).IsInvalidArgument());
  ASSERT_OK(catalog_->InsertSequence(kDatabase, kDown, Tuple{-1, false}));
  ASSERT_OK(catalog_->InsertSequence(kDatabase, kCas, Tuple{10, true}));

  // Neither a logical catalog abort nor the rollback below may undo sequence state.
  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(Version(initial_ + 1, 1));
  ASSERT_NOK(catalog_->DropDatabase(1, "template1", 2));
  ASSERT_NOK(catalog_->CheckHealthy());

  ASSERT_EQ(ASSERT_RESULT(fetch(kUp, 3, 2, 1, kMax, false)), Range(kMax - 9, kMax - 5));
  // Truncated at the bound; the next step would overflow int64.
  ASSERT_EQ(ASSERT_RESULT(fetch(kUp, 10, 2, 1, kMax, false)), Range(kMax - 3, kMax - 1));
  auto status = ResultToStatus(fetch(kUp, 1, 2, 1, kMax, false));
  ASSERT_EQ(PgsqlError(status), YBPgErrorCode::YB_PG_SEQUENCE_GENERATOR_LIMIT_EXCEEDED) << status;
  ASSERT_EQ(ASSERT_RESULT(fetch(kUp, 2, 2, 1, kMax, true)), Range(1, 3));

  ASSERT_EQ(ASSERT_RESULT(fetch(kDown, 5, -3, -100, -1, false)), Range(-1, -13));
  ASSERT_EQ(ASSERT_RESULT(fetch(kDown, 50, -3, -100, -1, false)), Range(-16, -100));
  status = ResultToStatus(fetch(kDown, 1, -3, -100, -1, false));
  ASSERT_EQ(PgsqlError(status), YBPgErrorCode::YB_PG_SEQUENCE_GENERATOR_LIMIT_EXCEEDED) << status;
  ASSERT_EQ(ASSERT_RESULT(fetch(kDown, 2, -3, -100, -1, true)), Range(-1, -4));

  ASSERT_TRUE(ResultToStatus(fetch(kDown, 0, -3, -100, -1, false)).IsInvalidArgument());
  ASSERT_TRUE(ResultToStatus(fetch(kDown, 1, 0, -100, -1, false)).IsInvalidArgument());
  ASSERT_TRUE(ResultToStatus(fetch(kDown, 1, -3, -1, -100, false)).IsInvalidArgument());
  ASSERT_TRUE(ResultToStatus(fetch(kCas + 1, 1, 1, 1, 10, false)).IsNotFound());
  ASSERT_EQ(ASSERT_RESULT(Sequence(kDatabase, kDown)), State(-4, true));

  ASSERT_TRUE(ASSERT_RESULT(update(kCas, Tuple{20, true}, Tuple{11, true})));
  ASSERT_TRUE(ASSERT_RESULT(update(kCas, Tuple{20, true}, Tuple{10, false})));
  ASSERT_EQ(ASSERT_RESULT(Sequence(kDatabase, kCas)), State(10, true));
  ASSERT_FALSE(ASSERT_RESULT(update(kCas, Tuple{20, false}, Tuple{10, true})));
  ASSERT_EQ(ASSERT_RESULT(Sequence(kDatabase, kCas)), State(20, false));
  ASSERT_FALSE(ASSERT_RESULT(update(kCas, Tuple{-5, true}, std::nullopt)));
  ASSERT_EQ(ASSERT_RESULT(Sequence(kDatabase, kCas)), State(-5, true));
  ASSERT_TRUE(ASSERT_RESULT(update(kCas + 1, Tuple{1, true}, std::nullopt)));
  ASSERT_OK(catalog_->DeleteSequence(kDatabase, kCas));
  ASSERT_OK(catalog_->DeleteSequence(kDatabase, kCas));
  ASSERT_TRUE(ResultToStatus(catalog_->ReadSequence(kDatabase, kCas)).IsNotFound());

  catalog_->Abort();
  ASSERT_OK(catalog_->CheckHealthy());
  ASSERT_EQ(ASSERT_RESULT(Version()), initial_);
  for (int i = 0; i < 2; ++i) {
    ASSERT_EQ(ASSERT_RESULT(Sequence(kDatabase, kUp)), State(3, true));
    ASSERT_EQ(ASSERT_RESULT(Sequence(kDatabase, kDown)), State(-4, true));
    ASSERT_TRUE(ResultToStatus(catalog_->ReadSequence(kDatabase, kCas)).IsNotFound());
    Reopen();
  }
  ASSERT_EQ(ASSERT_RESULT(fetch(kUp, 1, 2, 1, kMax, false)), Range(5, 5));
  // An uncalled value outside the request bounds is rejected before any reservation.
  ASSERT_OK(catalog_->InsertSequence(kDatabase, kCas, Tuple{50, false}));
  ASSERT_TRUE(ResultToStatus(fetch(kCas, 5, 1, 1, 10, false)).IsNotSupported());
  ASSERT_TRUE(ResultToStatus(fetch(kCas, 5, -1, 60, 100, true)).IsNotSupported());
  ASSERT_EQ(ASSERT_RESULT(Sequence(kDatabase, kCas)), State(50, false));
  ASSERT_EQ(ASSERT_RESULT(fetch(kCas, 5, 1, 1, 100, false)), Range(50, 54));
}

TEST_F(PgLocalSharedCommitTest, SequenceRetirementAndWriteFailureFence) {
  using Tuple = PgCatalogTransaction::SequenceTuple;
  using Range = std::pair<int64_t, int64_t>;
  using State = std::pair<int64_t, bool>;
  tserver::ExportedTabletMetadataChanges changes;
  ASSERT_OK(
      pb_util::ReadPBContainerFromPath(
          Env::Default(), Seed() + "/exported_tablet_metadata_changes", &changes));
  std::vector<tablet::TableInfoPB> sources;
  const auto collect = [&](const tablet::TableInfoPB& table) {
    if (table.table_id() == GetPgsqlTableId(1, kPgClassTableOid)) sources.push_back(table);
  };
  for (const auto& change : changes.metadata_changes()) {
    if (change.has_add_table()) collect(change.add_table());
    for (const auto& table : change.add_multiple_tables()) collect(table);
  }
  ASSERT_EQ(sources.size(), 1);
  // Adjacent OIDs share each sequence key up to the final database digit.
  constexpr uint32_t kRetired = 1000005500;
  constexpr uint32_t kSurviving = kRetired + 1;
  constexpr uint32_t kSequence = 1000005600;
  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateDatabase(kRetired, "retired_database", sources, 1));
  ASSERT_OK(catalog_->Commit());
  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateDatabase(kSurviving, "surviving_database", sources, 1));
  ASSERT_OK(catalog_->Commit());
  for (const auto database : {kRetired, kSurviving, 1u}) {
    ASSERT_OK(catalog_->InsertSequence(database, kSequence, Tuple{7, true}));
  }
  ASSERT_OK(catalog_->InsertSequence(kRetired, kSequence + 1, Tuple{1, false}));
  ASSERT_TRUE(catalog_->DeleteDatabaseSequences(kRetired).IsIllegalState());

  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->DropDatabase(kRetired, "retired_database", 1));
  catalog_->Abort();
  ASSERT_EQ(ASSERT_RESULT(Sequence(kRetired, kSequence + 1)), State(1, false));
  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->DropDatabase(kRetired, "retired_database", 1));
  ASSERT_OK(catalog_->Commit());
  for (int i = 0; i < 2; ++i) {
    for (const auto sequence : {kSequence, kSequence + 1}) {
      ASSERT_TRUE(ResultToStatus(catalog_->ReadSequence(kRetired, sequence)).IsNotFound());
    }
    ASSERT_EQ(ASSERT_RESULT(Sequence(kSurviving, kSequence)), State(7, true));
    ASSERT_EQ(ASSERT_RESULT(Sequence(1, kSequence)), State(7, true));
    ASSERT_TRUE(catalog_->InsertSequence(kRetired, kSequence, Tuple{1, false}).IsNotSupported());
    ASSERT_OK(catalog_->DeleteDatabaseSequences(kRetired));
    ASSERT_TRUE(catalog_->DeleteDatabaseSequences(kSurviving).IsIllegalState());
    Reopen();
  }

  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("PgCatalogTransaction::SequenceWriteStatus", [](void* status) {
    *static_cast<Status*>(status) = STATUS(IOError, "Ambiguous sequence acknowledgement");
  });
  sync->EnableProcessing();
  ASSERT_TRUE(
      ResultToStatus(catalog_->FetchSequence(kSurviving, kSequence, 3, 1, 1, 100, false))
          .IsIOError());
  sync->DisableProcessing();
  ASSERT_TRUE(ResultToStatus(catalog_->ReadSequence(1, kSequence)).IsIOError());
  ASSERT_TRUE(
      ResultToStatus(catalog_->FetchSequence(1, kSequence, 1, 1, 1, 100, false)).IsIOError());
  ASSERT_TRUE(catalog_->InsertSequence(1, kSequence + 1, Tuple{1, false}).IsIOError());
  ASSERT_TRUE(
      ResultToStatus(catalog_->UpdateSequence(1, kSequence, Tuple{1, false}, std::nullopt))
          .IsIOError());
  ASSERT_TRUE(catalog_->DeleteSequence(1, kSequence).IsIOError());
  ASSERT_TRUE(catalog_->DeleteDatabaseSequences(kRetired).IsIOError());
  ASSERT_TRUE(catalog_->CheckHealthy().IsIOError());
  ASSERT_TRUE(catalog_->Begin().IsIOError());
  ASSERT_TRUE(ResultToStatus(catalog_->AllocateOid(1)).IsIOError());
  ASSERT_TRUE(ResultToStatus(catalog_->NativeTransactions().Begin()).IsIOError());
  Reopen();
  // The unacknowledged reservation is durable, so its values are never reissued.
  ASSERT_EQ(ASSERT_RESULT(Sequence(kSurviving, kSequence)), State(10, true));
  ASSERT_EQ(
      ASSERT_RESULT(catalog_->FetchSequence(kSurviving, kSequence, 1, 1, 1, 100, false)),
      Range(11, 11));
  ASSERT_EQ(ASSERT_RESULT(Sequence(1, kSequence)), State(7, true));
}

TEST_F(PgLocalSharedCommitTest, SequenceCleanupResumesAfterDurableBatch) {
  using State = std::pair<int64_t, bool>;
  constexpr uint32_t kRetired = 1000005700;
  constexpr uint32_t kSequence = 1000010000;
  // Enough fixed-size keys for at least three 256 KiB cleanup batches.
  constexpr uint32_t kRetiredSequences = 15000;
  constexpr uint32_t kNeighborSequences = 3;
  const std::string name = "retired_sequences";
  const auto sources = ASSERT_RESULT(PgClassSources());
  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateDatabase(kRetired, name, sources, 1));
  ASSERT_OK(catalog_->Commit());
  {
    auto storage = ASSERT_RESULT(OpenClosedStorage());
    rocksdb::WriteBatch batch;
    for (uint32_t i = 0; i < kRetiredSequences; ++i) {
      batch.Put(storage->metadata(), SequenceKey(kRetired, kSequence + i), SequenceValue(i));
    }
    // Neighbors share every key byte up to the final database digit.
    for (const auto database : {kRetired - 1, kRetired + 1}) {
      for (uint32_t i = 0; i < kNeighborSequences; ++i) {
        batch.Put(
            storage->metadata(), SequenceKey(database, kSequence + i), SequenceValue(100 + i));
      }
    }
    ASSERT_OK(storage->Write(&batch, true));
  }
  Reopen();
  ASSERT_EQ(ASSERT_RESULT(Sequence(kRetired, kSequence + 1)), State(1, true));

  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  size_t sequence_batches = 0;
  size_t retired_batches = 0;
  size_t deleted = 0;
  bool interrupt = true;
  sync->SetCallBack("PgCatalogTransaction::SequenceCleanupBatch", [&](void* value) {
    // Storage::Write has appended both watermarks, so this is the whole synced batch.
    const auto* batch = static_cast<rocksdb::WriteBatch*>(value);
    ASSERT_LE(batch->GetDataSize(), 256 * 1024);
    deleted += batch->Count() - 2;
    ++sequence_batches;
  });
  sync->SetCallBack("PgCatalogTransaction::SequenceCleanupStatus", [&](void* status) {
    if (std::exchange(interrupt, false)) {
      *static_cast<Status*>(status) = STATUS(IOError, "Interrupted after a durable batch");
    }
  });
  sync->SetCallBack(
      "PgCatalogTransaction::RetiredDatabaseCleanupBatch", [&](void*) { ++retired_batches; });
  sync->EnableProcessing();
  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->DropDatabase(kRetired, name, 1));
  ASSERT_TRUE(catalog_->Commit().IsIOError());
  ASSERT_EQ(sequence_batches, 1);
  const auto first = deleted;
  ASSERT_GT(first, 0);
  {
    auto storage = ASSERT_RESULT(OpenClosedStorage());
    ASSERT_EQ(ASSERT_RESULT(CountSequences(*storage, kRetired)), kRetiredSequences - first);
  }

  sequence_batches = 0;
  deleted = 0;
  Reopen();
  ASSERT_GE(sequence_batches, 2);
  ASSERT_EQ(deleted, kRetiredSequences - first);
  for (const auto database : {kRetired - 1, kRetired + 1}) {
    for (uint32_t i = 0; i < kNeighborSequences; ++i) {
      ASSERT_EQ(ASSERT_RESULT(Sequence(database, kSequence + i)), State(100 + i, true));
    }
  }
  ASSERT_TRUE(catalog_->InsertSequence(kRetired, kSequence, {1, false}).IsNotSupported());

  sequence_batches = 0;
  retired_batches = 0;
  Reopen();
  ASSERT_EQ(sequence_batches, 0);
  ASSERT_EQ(retired_batches, 0);
  auto storage = ASSERT_RESULT(OpenClosedStorage());
  ASSERT_EQ(ASSERT_RESULT(CountSequences(*storage, kRetired)), 0);
  for (const auto database : {kRetired - 1, kRetired + 1}) {
    ASSERT_EQ(ASSERT_RESULT(CountSequences(*storage, database)), kNeighborSequences);
  }
}

TEST_F(PgLocalSharedCommitTest, RetiredCleanupScansStopAtUpperBounds) {
  constexpr uint32_t kRetired = 1000005800;
  constexpr uint32_t kSequence = 1000011000;
  constexpr uint32_t kTombstones = 2000;
  const std::string name = "scan_bound_test";
  const auto sources = ASSERT_RESULT(PgClassSources());
  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->CreateDatabase(kRetired, name, sources, 1));
  ASSERT_OK(catalog_->Commit());
  ASSERT_RESULT(catalog_->BeginWithNative());
  ASSERT_OK(catalog_->DropDatabase(kRetired, name, 1));
  ASSERT_OK(catalog_->Commit());
  {
    // Database 1's dropped sequences sort right after the retired-database markers, and the next
    // database's right after the retired database's own sequence range.
    auto storage = ASSERT_RESULT(OpenClosedStorage());
    rocksdb::WriteBatch batch;
    for (const auto database : {1u, kRetired + 1}) {
      for (uint32_t i = 0; i < kTombstones; ++i) {
        const auto key = SequenceKey(database, kSequence + i);
        batch.Put(storage->metadata(), key, SequenceValue(i));
        batch.Delete(storage->metadata(), key);
      }
    }
    ASSERT_OK(storage->Write(&batch, true));
  }
  {
    // Recovery keeps the tombstones. Without a physical bound, stepping past the last marker
    // walks every one of them.
    auto storage = ASSERT_RESULT(OpenClosedStorage());
    std::unique_ptr<rocksdb::Iterator> it(
        storage->db()->NewIterator(rocksdb::ReadOptions(), storage->metadata()));
    it->Seek(PgLocalStorage::MetadataKey("retired-database/"));
    ASSERT_TRUE(it->Valid());
    rocksdb::perf_context.Reset();
    it->Next();
    ASSERT_OK(it->status());
    ASSERT_GE(rocksdb::perf_context.internal_delete_skipped_count, 2 * kTombstones);
  }
  Reopen();
  // An empty commit's only metadata iteration is the retired-database marker scan.
  ASSERT_OK(catalog_->Begin());
  rocksdb::perf_context.Reset();
  ASSERT_OK(catalog_->Commit());
  ASSERT_EQ(rocksdb::perf_context.internal_delete_skipped_count, 0);
  rocksdb::perf_context.Reset();
  ASSERT_OK(catalog_->DeleteDatabaseSequences(kRetired));
  ASSERT_EQ(rocksdb::perf_context.internal_delete_skipped_count, 0);
}

}  // namespace
}  // namespace yb::local_runtime
