// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations
// under the License.

#include "yb/tools/local_runtime/pg_local_txn_store.h"

#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <csignal>
#include <cstdlib>
#include <functional>
#include <map>

#include "yb/common/transaction_error.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/dockv/doc_key.h"
#include "yb/rocksdb/db.h"
#include "yb/tablet/tablet_options.h"
#include "yb/util/env.h"
#include "yb/util/flags.h"
#include "yb/util/scope_exit.h"
#include "yb/util/subprocess.h"
#include "yb/util/sync_point.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/test_util.h"
#include "yb/yql/pggate/util/pg_wire.h"

DECLARE_uint64(local_runtime_churn_write_buffer_bytes);
DECLARE_uint64(local_runtime_intents_memtable_budget_bytes);

namespace yb::local_runtime {
namespace {

using Row = std::array<int64_t, 3>;

Result<Schema> TestSchema() {
  SchemaBuilder builder;
  RETURN_NOT_OK(builder.AddHashKeyColumn("h", DataType::INT64));
  RETURN_NOT_OK(builder.AddKeyColumn("r", DataType::INT64));
  RETURN_NOT_OK(builder.AddColumn("v", DataType::INT64));
  return builder.Build();
}

PgsqlWriteRequestPB WriteRequest(const Schema& schema, const TableId& table, int64_t r, int64_t v) {
  PgsqlWriteRequestPB request;
  request.set_client(YQL_CLIENT_PGSQL);
  request.set_table_id(table);
  request.set_schema_version(0);
  request.set_stmt_type(PgsqlWriteRequestPB::PGSQL_INSERT);
  request.add_partition_column_values()->mutable_value()->set_int64_value(7);
  request.add_range_column_values()->mutable_value()->set_int64_value(r);
  auto* column = request.add_column_values();
  column->set_column_id(schema.column_id(2));
  column->mutable_expr()->mutable_value()->set_int64_value(v);
  return request;
}

Status Put(
    PgLocalTxnStore* store, const TransactionId& txn, const Schema& schema, const TableId& table,
    int64_t r, int64_t v, uint32_t subtransaction = 1, bool update = false) {
  auto request = WriteRequest(schema, table, r, v);
  if (update) {
    request.set_stmt_type(PgsqlWriteRequestPB::PGSQL_UPDATE);
    *request.add_column_new_values() = request.column_values(0);
    request.clear_column_values();
    for (const auto& id : schema.column_ids()) request.mutable_column_refs()->add_ids(id);
  }
  PgsqlResponsePB response;
  RETURN_NOT_OK(store->Write(txn, request, &response, subtransaction));
  SCHECK_EQ(
      response.status(), PgsqlResponsePB::PGSQL_STATUS_OK, QLError, "Native row write failed");
  return Status::OK();
}

PgsqlReadRequestPB ReadRequest(const Schema& schema, const TableId& table, int64_t limit = 2) {
  PgsqlReadRequestPB request;
  request.set_client(YQL_CLIENT_PGSQL);
  request.set_table_id(table);
  request.set_schema_version(0);
  request.set_is_forward_scan(true);
  request.set_limit(limit);
  request.set_return_paging_state(true);
  for (const auto& id : schema.column_ids()) {
    request.mutable_column_refs()->add_ids(id);
    request.add_targets()->set_column_id(id);
  }
  return request;
}

Result<std::vector<Row>> DecodeRows(const TransactionReadResult& result) {
  Slice bytes(result.rows);
  const auto count = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<int64_t>(&bytes));
  SCHECK_EQ(count, result.row_count, Corruption, "Incorrect wire row count");
  std::vector<Row> rows;
  for (int64_t i = 0; i < count; ++i) {
    Row row;
    for (auto& value : row) {
      SCHECK_EQ(
          VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint8_t>(&bytes)), 0, Corruption,
          "Unexpected NULL");
      value = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<int64_t>(&bytes));
    }
    rows.push_back(row);
  }
  SCHECK(bytes.empty(), Corruption, "Trailing wire data");
  return rows;
}

Result<std::vector<Row>> ReadAll(
    PgLocalTxnStore* store, const TransactionId& txn, const Schema& schema, const TableId& table,
    HybridTime own_limit = HybridTime::kInvalid) {
  if (!own_limit.is_valid()) own_limit = VERIFY_RESULT(store->ReadPoint(txn)).in_txn_limit;
  std::vector<Row> rows;
  for (size_t tablet = 0; tablet < 4; ++tablet) {
    auto request = ReadRequest(schema, table);
    for (size_t page = 0;; ++page) {
      SCHECK_LT(page, 1000, IllegalState, "Unbounded paging");
      auto result = VERIFY_RESULT(store->Read(txn, request, tablet, own_limit));
      auto data = VERIFY_RESULT(DecodeRows(result));
      rows.insert(rows.end(), data.begin(), data.end());
      if (result.response.paging_state().next_row_key().empty()) break;
      *request.mutable_paging_state() = result.response.paging_state();
    }
  }
  std::sort(rows.begin(), rows.end());
  return rows;
}

Status MutateStorage(
    const std::string& path,
    const std::function<Status(rocksdb::DB*, const std::vector<rocksdb::ColumnFamilyHandle*>&)>&
        action) {
  rocksdb::Options options;
  docdb::InitRocksDBOptions(
      &options, "intent-test", "intent-test", nullptr, tablet::TabletOptions(),
      docdb::StorageDbType::kRegular);
  options.disable_wal = false;
  std::vector<std::string> names;
  RETURN_NOT_OK(rocksdb::DB::ListColumnFamilies(options, path, &names));
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  for (const auto& name : names) descriptors.emplace_back(name, options);
  rocksdb::DB* raw;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  RETURN_NOT_OK(rocksdb::DB::Open(options, path, descriptors, &handles, &raw));
  std::unique_ptr<rocksdb::DB> db(raw);
  auto cleanup = ScopeExit([&] {
    for (auto* handle : handles) delete handle;
  });
  return action(db.get(), handles);
}

// Live keys per column family; status records in the metadata family are counted as "tx".
Result<std::map<std::string, size_t>> CountKeys(const std::string& path) {
  std::map<std::string, size_t> counts;
  RETURN_NOT_OK(MutateStorage(path, [&](auto* db, const auto& handles) -> Status {
    for (auto* handle : handles) {
      std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions(), handle));
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        auto name = handle->GetName();
        if (name == rocksdb::kDefaultColumnFamilyName) {
          dockv::DocKey key;
          auto bytes = it->key();
          RETURN_NOT_OK(key.DecodeFrom(&bytes));
          if (key.range_group()[0].GetString().starts_with("tx/")) name = "tx";
        }
        ++counts[name];
      }
      RETURN_NOT_OK(it->status());
    }
    return Status::OK();
  }));
  return counts;
}

class PgLocalTxnStoreTest : public YBTest {
 protected:
  void SetUp() override {
    YBTest::SetUp();
    schema_ = ASSERT_RESULT(TestSchema());
    path_ = GetTestPath("native-intents");
    store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
    ASSERT_OK(store_->CreateTable("a", schema_, 4));
    ASSERT_OK(store_->CreateTable("b", schema_, 4));
  }
  Result<std::vector<Row>> Rows(const TransactionId& txn, const TableId& table = "a") {
    return ReadAll(store_.get(), txn, schema_, table);
  }
  Status Write(
      const TransactionId& txn, int64_t r, int64_t v, uint32_t subtransaction = 1,
      bool update = false) {
    return Put(store_.get(), txn, schema_, "a", r, v, subtransaction, update);
  }
  Schema schema_;
  std::string path_;
  std::unique_ptr<PgLocalTxnStore> store_;
};

TEST_F(PgLocalTxnStoreTest, SnapshotsAndCrossTableCommit) {
  const auto before = ASSERT_RESULT(store_->Begin());
  const auto writer = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(writer, 1, 10));
  ASSERT_OK(Put(store_.get(), writer, schema_, "b", 1, 20));
  ASSERT_EQ(ASSERT_RESULT(Rows(writer)), (std::vector<Row>{{7, 1, 10}}));
  ASSERT_EQ(ASSERT_RESULT(Rows(writer, "b")), (std::vector<Row>{{7, 1, 20}}));
  const auto between = ASSERT_RESULT(store_->Begin());
  ASSERT_TRUE(ASSERT_RESULT(Rows(before)).empty());
  ASSERT_TRUE(ASSERT_RESULT(Rows(between)).empty());
  ASSERT_OK(store_->Commit(writer));
  ASSERT_TRUE(ASSERT_RESULT(Rows(before)).empty());
  ASSERT_TRUE(ASSERT_RESULT(Rows(between, "b")).empty());
  const auto after = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(after)), (std::vector<Row>{{7, 1, 10}}));
  ASSERT_EQ(ASSERT_RESULT(Rows(after, "b")), (std::vector<Row>{{7, 1, 20}}));
  ASSERT_OK(store_->Abort(before));
  ASSERT_OK(store_->Abort(between));
  ASSERT_OK(store_->Abort(after));
  ASSERT_NOK(store_->Abort(writer));
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  const auto reopened = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reopened)), (std::vector<Row>{{7, 1, 10}}));
  ASSERT_EQ(ASSERT_RESULT(Rows(reopened, "b")), (std::vector<Row>{{7, 1, 20}}));
}

TEST_F(PgLocalTxnStoreTest, NativePriorityConflicts) {
  const auto low = ASSERT_RESULT(store_->Begin(10));
  const auto equal = ASSERT_RESULT(store_->Begin(10));
  ASSERT_OK(Write(low, 1, 10));
  auto conflict = Write(equal, 1, 20);
  ASSERT_TRUE(conflict.IsTryAgain()) << conflict;
  ASSERT_EQ(TransactionError::ValueFromStatus(conflict), TransactionErrorCode::kConflict);
  ASSERT_NOK(store_->Commit(equal));
  ASSERT_OK(store_->Abort(equal));
  const auto high = ASSERT_RESULT(store_->Begin(100));
  ASSERT_OK(Write(high, 1, 30));
  ASSERT_NOK(store_->Commit(low));
  ASSERT_NOK(Write(low, 2, 40));
  // The wounded transaction stays visible as aborted until its owner finishes it.
  ASSERT_OK(store_->Abort(low));
  ASSERT_OK(store_->Commit(high));
  const auto reader = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reader)), (std::vector<Row>{{7, 1, 30}}));
}

TEST_F(PgLocalTxnStoreTest, CommittedConflictAndRepeatableRead) {
  const auto initial = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(initial, 1, 10));
  ASSERT_OK(store_->Commit(initial));
  const auto stale = ASSERT_RESULT(store_->Begin(1000));
  const auto newer = ASSERT_RESULT(store_->Begin(1));
  ASSERT_OK(Write(newer, 1, 20, 1, true));
  ASSERT_OK(store_->Commit(newer));
  ASSERT_EQ(ASSERT_RESULT(Rows(stale)), (std::vector<Row>{{7, 1, 10}}));
  const auto conflict = Write(stale, 1, 30, 1, true);
  ASSERT_TRUE(conflict.IsTryAgain()) << conflict;
  ASSERT_EQ(TransactionError::ValueFromStatus(conflict), TransactionErrorCode::kConflict);
  ASSERT_NOK(store_->Commit(stale));
  ASSERT_OK(store_->Abort(stale));
  const auto reader = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reader)), (std::vector<Row>{{7, 1, 20}}));
}

TEST_F(PgLocalTxnStoreTest, SavepointsReleaseNativeConflicts) {
  const auto first = ASSERT_RESULT(store_->Begin(100));
  ASSERT_OK(Write(first, 1, 10));
  ASSERT_OK(Write(first, 2, 20, 2));
  ASSERT_OK(store_->RollbackTo(first, 2));
  const auto second = ASSERT_RESULT(store_->Begin(1));
  ASSERT_OK(Write(second, 2, 99));
  ASSERT_OK(store_->Commit(second));
  ASSERT_OK(Write(first, 3, 30, 3));
  ASSERT_OK(Write(first, 4, 40, 4));
  ASSERT_OK(store_->RollbackTo(first, 3));
  ASSERT_NOK(Write(first, 3, 31, 3));
  ASSERT_OK(Write(first, 5, 50, 5));
  ASSERT_EQ(ASSERT_RESULT(Rows(first)), (std::vector<Row>{{7, 1, 10}, {7, 5, 50}}));
  ASSERT_OK(store_->Commit(first));
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  const auto reader = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reader)), (std::vector<Row>{{7, 1, 10}, {7, 2, 99}, {7, 5, 50}}));
}

TEST_F(PgLocalTxnStoreTest, ReclaimsFinishedTransactions) {
  const auto committed = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(committed, 1, 10));
  ASSERT_OK(Write(committed, 2, 20, 2));
  ASSERT_OK(store_->RollbackTo(committed, 2));
  ASSERT_OK(Put(store_.get(), committed, schema_, "b", 1, 30, 3));
  const auto aborted = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(aborted, 3, 40));
  ASSERT_OK(store_->Abort(aborted));
  ASSERT_OK(store_->Commit(committed));
  // A reclaim failure after the durable commit fences the store but does not fail the commit.
  const auto stranded = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(stranded, 4, 50));
  {
    auto* sync = SyncPoint::GetInstance();
    auto cleanup = ScopeExit([&] {
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
    });
    sync->SetCallBack("PgLocalTxnStore::WriteStatus", [](void* status) {
      *static_cast<Status*>(status) = STATUS(IOError, "Injected reclaim failure");
    });
    sync->EnableProcessing();
    ASSERT_OK(store_->Commit(stranded));
    ASSERT_NOK(store_->Begin());
  }
  store_.reset();
  auto counts = ASSERT_RESULT(CountKeys(path_));
  ASSERT_EQ(counts["tx"], 1);
  ASSERT_GT(counts["i/a"], 0);
  ASSERT_EQ(counts["i/b"], 0);
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  const auto reader = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reader)), (std::vector<Row>{{7, 1, 10}, {7, 4, 50}}));
  ASSERT_EQ(ASSERT_RESULT(Rows(reader, "b")), (std::vector<Row>{{7, 1, 30}}));
  ASSERT_OK(store_->Abort(reader));
  store_.reset();
  counts = ASSERT_RESULT(CountKeys(path_));
  ASSERT_EQ(counts["tx"], 0);
  ASSERT_EQ(counts["i/a"], 0);
  ASSERT_EQ(counts["i/b"], 0);
  ASSERT_GT(counts["r/a"], 0);
  ASSERT_GT(counts["r/b"], 0);
}

TEST_F(PgLocalTxnStoreTest, FlushedIntentsStayCorrect) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_local_runtime_churn_write_buffer_bytes) = 64 * 1024;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_local_runtime_intents_memtable_budget_bytes) = 16 * 1024;
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  // Pending across every flush: its intents must survive them and still be removed at the end.
  const auto pending = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(pending, 1000, 1));
  std::vector<Row> expected;
  for (int64_t i = 1; i <= 300; ++i) {
    const auto txn = ASSERT_RESULT(store_->Begin());
    ASSERT_OK(Write(txn, i, i));
    ASSERT_OK(Write(txn, i, i * 10, 1, true));
    if (i % 3) {
      ASSERT_OK(store_->Commit(txn));
      expected.push_back({7, i, i * 10});
    } else {
      ASSERT_OK(store_->Abort(txn));
    }
  }
  // Its snapshot predates every other commit.
  ASSERT_EQ(ASSERT_RESULT(Rows(pending)), (std::vector<Row>{{7, 1000, 1}}));
  auto with_pending = expected;
  with_pending.push_back({7, 1000, 1});
  const auto reader = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reader)), expected);
  ASSERT_OK(store_->Abort(reader));
  ASSERT_OK(store_->Commit(pending));
  store_.reset();
  const auto counts = ASSERT_RESULT(CountKeys(path_));
  ASSERT_EQ(counts.contains("i/a") ? counts.at("i/a") : 0, 0);
  ASSERT_EQ(counts.contains("tx") ? counts.at("tx") : 0, 0);
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  const auto reopened = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reopened)), with_pending);
}

TEST_F(PgLocalTxnStoreTest, StatementLimitsAndPaging) {
  const auto txn = ASSERT_RESULT(store_->Begin());
  for (int64_t i = 1; i <= 8; ++i) ASSERT_OK(Write(txn, i, i));
  const auto point = ASSERT_RESULT(store_->ReadPoint(txn));
  for (int64_t i = 9; i <= 12; ++i) ASSERT_OK(Write(txn, i, i));
  auto rows = ASSERT_RESULT(ReadAll(store_.get(), txn, schema_, "a", point.in_txn_limit));
  ASSERT_EQ(rows.size(), 8);
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)).size(), 12);
  PgsqlResponsePB response;
  ASSERT_OK(store_->Write(txn, WriteRequest(schema_, "a", 1, 99), &response, 2));
  ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_DUPLICATE_KEY_ERROR);
  ASSERT_NOK(store_->Commit(txn));
  ASSERT_OK(store_->RollbackTo(txn, 2));
  ASSERT_OK(Write(txn, 13, 13, 3));
  ASSERT_EQ(ASSERT_RESULT(Rows(txn)).size(), 13);
  ASSERT_OK(store_->Commit(txn));
}

TEST_F(PgLocalTxnStoreTest, ConcurrentDisjointWriters) {
  const auto* probe = std::getenv("YB_LOCAL_INTENT_PROBE");
  if (probe) {
    ASSERT_OK(WriteStringToFileSync(
        Env::Default(), std::to_string(getpid()), std::string(probe) + ".start"));
  }
  std::vector<TransactionId> snapshots;
  for (size_t i = 0; i < 16; ++i) snapshots.push_back(ASSERT_RESULT(store_->Begin()));
  TestThreadHolder threads;
  for (int worker = 0; worker < 4; ++worker) {
    threads.AddThreadFunctor([&, worker] {
      for (int i = 0; i < 64; ++i) {
        const auto txn = ASSERT_RESULT(store_->Begin());
        const auto r = worker * 1000 + i;
        ASSERT_OK(Write(txn, r, worker));
        ASSERT_OK(Put(store_.get(), txn, schema_, "b", r, worker));
        ASSERT_OK(store_->Commit(txn));
      }
    });
  }
  threads.JoinAll();
  ASSERT_FALSE(HasFatalFailure());
  const auto reader = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reader)).size(), 256);
  ASSERT_EQ(ASSERT_RESULT(Rows(reader, "b")).size(), 256);
  ASSERT_OK(store_->Abort(reader));
  for (const auto& snapshot : snapshots) ASSERT_TRUE(ASSERT_RESULT(Rows(snapshot)).empty());
  if (probe) {
    // Sleep without polling so an external sampler can measure the storage process alone.
    ASSERT_OK(WriteStringToFileSync(Env::Default(), std::to_string(getpid()), probe));
    SleepFor(MonoDelta::FromSeconds(90));
  }
  for (const auto& snapshot : snapshots) ASSERT_OK(store_->Abort(snapshot));
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  const auto reopened = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reopened)).size(), 256);
  ASSERT_EQ(ASSERT_RESULT(Rows(reopened, "b")).size(), 256);
}

TEST_F(PgLocalTxnStoreTest, AmbiguousCommitIsFenced) {
  const auto txn = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(txn, 1, 10));
  ASSERT_OK(Put(store_.get(), txn, schema_, "b", 2, 20));
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("PgLocalTxnStore::CommitStatus", [](void* status) {
    *static_cast<Status*>(status) = STATUS(IOError, "Ambiguous native commit acknowledgement");
  });
  sync->EnableProcessing();
  ASSERT_TRUE(store_->Commit(txn).IsIOError());
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(store_->Abort(txn).IsIOError());
    ASSERT_TRUE(store_->RollbackTo(txn, 1).IsIOError());
    ASSERT_TRUE(ResultToStatus(store_->Begin()).IsIOError());
    ASSERT_TRUE(ResultToStatus(Rows(txn)).IsIOError());
  }
  sync->DisableProcessing();
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  const auto reader = ASSERT_RESULT(store_->Begin());
  ASSERT_EQ(ASSERT_RESULT(Rows(reader)), (std::vector<Row>{{7, 1, 10}}));
  ASSERT_EQ(ASSERT_RESULT(Rows(reader, "b")), (std::vector<Row>{{7, 2, 20}}));
}

TEST_F(PgLocalTxnStoreTest, ReopenAbortsPendingAndRejectsUnsupported) {
  const auto txn = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(txn, 1, 10));
  auto unsupported = ReadRequest(schema_, "a");
  unsupported.set_row_mark_type(ROW_MARK_EXCLUSIVE);
  ASSERT_NOK(store_->Read(txn, unsupported, 0));
  ASSERT_NOK(store_->CreateTable("c", schema_, 4));
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  ASSERT_NOK(store_->Commit(txn));
  const auto reader = ASSERT_RESULT(store_->Begin());
  ASSERT_TRUE(ASSERT_RESULT(Rows(reader)).empty());
  ASSERT_OK(Write(reader, 1, 20));
  ASSERT_OK(store_->Commit(reader));
}

TEST_F(PgLocalTxnStoreTest, UpdatesDeletesAndOldOwnWrites) {
  const auto initial = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(initial, 1, 10));
  ASSERT_OK(store_->Commit(initial));
  const auto old = ASSERT_RESULT(store_->Begin());
  const auto writer = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(writer, 1, 20, 1, true));
  const auto statement = ASSERT_RESULT(store_->ReadPoint(writer));
  ASSERT_OK(Write(writer, 1, 30, 2, true));
  ASSERT_EQ(ASSERT_RESULT(Rows(writer)), (std::vector<Row>{{7, 1, 30}}));
  ASSERT_EQ(
      ASSERT_RESULT(ReadAll(store_.get(), writer, schema_, "a", statement.in_txn_limit)),
      (std::vector<Row>{{7, 1, 20}}));
  ASSERT_OK(store_->RollbackTo(writer, 2));
  ASSERT_EQ(ASSERT_RESULT(Rows(writer)), (std::vector<Row>{{7, 1, 20}}));
  auto remove = WriteRequest(schema_, "a", 1, 0);
  remove.set_stmt_type(PgsqlWriteRequestPB::PGSQL_DELETE);
  remove.clear_column_values();
  PgsqlResponsePB response;
  ASSERT_OK(store_->Write(writer, remove, &response, 3));
  ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
  ASSERT_TRUE(ASSERT_RESULT(Rows(writer)).empty());
  ASSERT_EQ(ASSERT_RESULT(Rows(old)), (std::vector<Row>{{7, 1, 10}}));
  ASSERT_OK(store_->RollbackTo(writer, 3));
  ASSERT_EQ(ASSERT_RESULT(Rows(writer)), (std::vector<Row>{{7, 1, 20}}));
  ASSERT_OK(store_->Write(writer, remove, &response, 4));
  ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
  ASSERT_OK(store_->Commit(writer));
  ASSERT_EQ(ASSERT_RESULT(Rows(old)), (std::vector<Row>{{7, 1, 10}}));
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  ASSERT_TRUE(ASSERT_RESULT(Rows(ASSERT_RESULT(store_->Begin()))).empty());
}

TEST_F(PgLocalTxnStoreTest, HashPartitionsAndPagingValidation) {
  const auto writer = ASSERT_RESULT(store_->Begin());
  const std::array<int64_t, 4> keys = {65535, -1, 42, 1};
  const std::array<uint32_t, 4> hashes = {9647, 22565, 48975, 60841};
  for (size_t i = 0; i < keys.size(); ++i) {
    for (int r = 0; r < 3; ++r) {
      auto request = WriteRequest(schema_, "a", r, r);
      request.mutable_partition_column_values(0)->mutable_value()->set_int64_value(keys[i]);
      request.set_hash_code(hashes[i]);
      PgsqlResponsePB response;
      ASSERT_OK(store_->Write(writer, request, &response));
      ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
    }
    auto request = ReadRequest(schema_, "a");
    auto first = ASSERT_RESULT(store_->Read(writer, request, i));
    ASSERT_EQ(first.row_count, 2);
    for (const auto& row : ASSERT_RESULT(DecodeRows(first))) ASSERT_EQ(row[0], keys[i]);
    *request.mutable_paging_state() = first.response.paging_state();
    ASSERT_EQ(ASSERT_RESULT(store_->Read(writer, request, i)).row_count, 1);
    request.mutable_paging_state()->set_table_id("b");
    ASSERT_NOK(store_->Read(writer, request, i));
    request.clear_paging_state();
    request.add_partition_column_values()->mutable_value()->set_int64_value(keys[i]);
    request.set_hash_code(hashes[i] + 1);
    ASSERT_NOK(store_->Read(writer, request, i));
  }
  ASSERT_EQ(ASSERT_RESULT(Rows(writer)).size(), 12);
  ASSERT_OK(store_->Commit(writer));
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  ASSERT_EQ(ASSERT_RESULT(Rows(ASSERT_RESULT(store_->Begin()))).size(), 12);
}

TEST_F(PgLocalTxnStoreTest, MissingPublishedFamilyIsCorruption) {
  const auto writer = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(writer, 1, 10));
  ASSERT_OK(store_->Commit(writer));
  store_.reset();
  ASSERT_OK(MutateStorage(path_, [](auto* db, const auto& handles) -> Status {
    for (auto* handle : handles) {
      if (handle->GetName() == "i/a") return db->DropColumnFamily(handle);
    }
    return STATUS(NotFound, "Missing test family");
  }));
  ASSERT_TRUE(ResultToStatus(PgLocalTxnStore::Open(path_)).IsCorruption());
}

TEST_F(PgLocalTxnStoreTest, UnknownIntentTransactionIsCorruption) {
  // Finished transactions leave no intents, so strand an unfinished one's.
  const auto writer = ASSERT_RESULT(store_->Begin());
  ASSERT_OK(Write(writer, 1, 10));
  store_.reset();
  ASSERT_OK(MutateStorage(path_, [&](auto* db, const auto&) -> Status {
    const auto key =
        dockv::DocKey(dockv::KeyEntryValues{dockv::KeyEntryValue("tx/" + writer.ToString())})
            .Encode()
            .ToStringBuffer() +
        dockv::KeyEntryValue(DocHybridTime(HybridTime::kInitial, 0)).ToKeyBytes().ToStringBuffer();
    rocksdb::WriteOptions options;
    options.sync = true;
    return db->Delete(options, key);
  }));
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  ASSERT_TRUE(ResultToStatus(Rows(ASSERT_RESULT(store_->Begin()))).IsCorruption());
}

TEST_F(PgLocalTxnStoreTest, WriteFailureFencesUntilRecovery) {
  const auto txn = ASSERT_RESULT(store_->Begin());
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("PgLocalTxnStore::WriteStatus", [](void* status) {
    *static_cast<Status*>(status) = STATUS(IOError, "Ambiguous intent write acknowledgement");
  });
  sync->EnableProcessing();
  ASSERT_TRUE(Write(txn, 1, 10).IsIOError());
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(store_->Commit(txn).IsIOError());
    ASSERT_TRUE(store_->Abort(txn).IsIOError());
    ASSERT_TRUE(ResultToStatus(Rows(txn)).IsIOError());
  }
  sync->DisableProcessing();
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalTxnStore::Open(path_));
  ASSERT_TRUE(ASSERT_RESULT(Rows(ASSERT_RESULT(store_->Begin()))).empty());
}

TEST_F(PgLocalTxnStoreTest, ActiveTransactionBound) {
  std::vector<TransactionId> transactions;
  for (size_t i = 0; i < 64; ++i) transactions.push_back(ASSERT_RESULT(store_->Begin()));
  ASSERT_TRUE(ResultToStatus(store_->Begin()).IsNotSupported());
  ASSERT_OK(store_->Abort(transactions.back()));
  transactions.back() = ASSERT_RESULT(store_->Begin());
  ASSERT_TRUE(ResultToStatus(store_->Begin()).IsNotSupported());
  for (const auto& txn : transactions) ASSERT_OK(store_->Abort(txn));
}

class PgLocalTxnStoreCrashTest : public YBTest {};

TEST_F(PgLocalTxnStoreCrashTest, SyncedCommitRecovery) {
  const auto schema = ASSERT_RESULT(TestSchema());
  if (const char* path = std::getenv("YB_LOCAL_INTENT_CRASH_PATH")) {
    auto store = ASSERT_RESULT(PgLocalTxnStore::Open(path));
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack(
        std::getenv("YB_LOCAL_INTENT_CRASH_POINT"), [](void*) { std::raise(SIGKILL); });
    sync->EnableProcessing();
    ASSERT_OK(store->CreateTable("a", schema, 4));
    ASSERT_OK(store->CreateTable("b", schema, 4));
    const auto txn = ASSERT_RESULT(store->Begin());
    ASSERT_OK(Put(store.get(), txn, schema, "a", 1, 10));
    ASSERT_OK(Put(store.get(), txn, schema, "b", 2, 20));
    ASSERT_OK(store->Commit(txn));
    FAIL() << "Expected SIGKILL";
    return;
  }
  std::string executable;
  ASSERT_OK(Env::Default()->GetExecutablePath(&executable));
  for (const auto* point : {"FamilyCreated", "SchemaPersisted", "BeforeCommit", "AfterCommit"}) {
    const auto path = GetTestPath(point);
    Subprocess child(
        executable, {executable, "--gtest_filter=PgLocalTxnStoreCrashTest.SyncedCommitRecovery"});
    child.SetEnv("YB_LOCAL_INTENT_CRASH_PATH", path);
    child.SetEnv("YB_LOCAL_INTENT_CRASH_POINT", std::string("PgLocalTxnStore::") + point);
    child.SetEnv("TEST_TMPDIR", GetTestPath(std::string("child-") + point));
    ASSERT_OK(child.Start());
    int code;
    ASSERT_OK(child.Wait(&code));
    ASSERT_TRUE(WIFSIGNALED(code));
    ASSERT_EQ(WTERMSIG(code), SIGKILL);
    auto store = ASSERT_RESULT(PgLocalTxnStore::Open(path));
    if (std::string(point) == "FamilyCreated") ASSERT_OK(store->CreateTable("a", schema, 4));
    if (std::string(point) == "FamilyCreated" || std::string(point) == "SchemaPersisted") {
      ASSERT_OK(store->CreateTable("b", schema, 4));
    }
    const auto reader = ASSERT_RESULT(store->Begin());
    const auto a = ASSERT_RESULT(ReadAll(store.get(), reader, schema, "a"));
    const auto b = ASSERT_RESULT(ReadAll(store.get(), reader, schema, "b"));
    if (std::string(point) == "AfterCommit") {
      ASSERT_EQ(a, (std::vector<Row>{{7, 1, 10}}));
      ASSERT_EQ(b, (std::vector<Row>{{7, 2, 20}}));
    } else {
      ASSERT_TRUE(a.empty());
      ASSERT_TRUE(b.empty());
    }
  }
}

}  // namespace
}  // namespace yb::local_runtime
