// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations
// under the License.

#include "yb/tools/local_runtime/pg_local_store.h"

#include <sys/wait.h>

#include <array>
#include <csignal>
#include <set>

#include "yb/dockv/doc_key.h"

#include "yb/util/scope_exit.h"
#include "yb/util/subprocess.h"
#include "yb/util/sync_point.h"

#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"
#include "yb/yql/pggate/util/pg_wire.h"

namespace yb::local_runtime {
namespace {

using Row = std::array<int64_t, 3>;

Result<std::vector<Row>> DecodeRows(const ReadResult& result) {
  Slice data(result.rows);
  const auto count = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<int64_t>(&data));
  SCHECK_EQ(count, result.row_count, Corruption, "Incorrect wire row count");
  std::vector<Row> rows;
  for (int64_t i = 0; i != count; ++i) {
    Row row;
    for (auto& value : row) {
      const auto flags = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint8_t>(&data));
      SCHECK_EQ(flags, 0, Corruption, "Unexpected NULL column");
      value = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<int64_t>(&data));
    }
    rows.push_back(row);
  }
  SCHECK(data.empty(), Corruption, "Trailing PgGate wire data");
  return rows;
}

class PgLocalStoreTest : public YBTest {
 protected:
  void SetUp() override {
    YBTest::SetUp();
    SchemaBuilder builder;
    ASSERT_OK(builder.AddHashKeyColumn("h", DataType::INT64));
    ASSERT_OK(builder.AddKeyColumn("r", DataType::INT64));
    ASSERT_OK(builder.AddColumn("v", DataType::INT64));
    schema_ = builder.Build();
    path_ = GetTestPath("local-store");
    store_ = ASSERT_RESULT(PgLocalStore::Open(path_));
    ASSERT_OK(store_->CreateTable("a", schema_, 4));
    ASSERT_OK(store_->CreateTable("b", schema_, 4));
  }

  PgsqlWriteRequestPB WriteRequest(const std::string& table, int64_t h, int64_t r, int64_t v) {
    PgsqlWriteRequestPB request;
    request.set_client(YQL_CLIENT_PGSQL);
    request.set_stmt_id(1);
    request.set_table_id(table);
    request.set_schema_version(0);
    request.set_stmt_type(PgsqlWriteRequestPB::PGSQL_INSERT);
    request.add_partition_column_values()->mutable_value()->set_int64_value(h);
    request.add_range_column_values()->mutable_value()->set_int64_value(r);
    auto* column = request.add_column_values();
    column->set_column_id(schema_.column_id(2));
    column->mutable_expr()->mutable_value()->set_int64_value(v);
    return request;
  }

  PgsqlReadRequestPB ReadRequest(const std::string& table) {
    PgsqlReadRequestPB request;
    request.set_client(YQL_CLIENT_PGSQL);
    request.set_stmt_id(1);
    request.set_table_id(table);
    request.set_schema_version(0);
    request.set_limit(10000);
    for (size_t i = 0; i < schema_.num_columns(); ++i) {
      request.add_targets()->set_column_id(schema_.column_id(i));
      request.mutable_column_refs()->add_ids(schema_.column_id(i));
    }
    return request;
  }

  Status Insert(const std::string& table, int64_t h, int64_t r, int64_t v) {
    PgsqlResponsePB response;
    RETURN_NOT_OK(store_->ApplyAutocommitWrite(WriteRequest(table, h, r, v), &response));
    SCHECK_EQ(
        response.status(), PgsqlResponsePB::PGSQL_STATUS_OK, IllegalState, response.DebugString());
    return Status::OK();
  }

  Result<std::set<Row>> ReadAll(const std::string& table, HybridTime time = HybridTime::kInvalid) {
    std::set<Row> rows;
    const auto partitions = VERIFY_RESULT(store_->GetPartitions(table));
    for (size_t partition = 0; partition < partitions.size(); ++partition) {
      const auto result = VERIFY_RESULT(store_->Read(ReadRequest(table), partition, time));
      for (const auto& row : VERIFY_RESULT(DecodeRows(result))) {
        SCHECK(rows.insert(row).second, Corruption, "Duplicate row across logical tablets");
      }
    }
    return rows;
  }

  Schema schema_;
  std::string path_;
  std::unique_ptr<PgLocalStore> store_;
};

TEST_F(PgLocalStoreTest, HashRangesSnapshotsAndRestart) {
  std::set<Row> expected;
  for (int64_t h = -32; h < 96; ++h) {
    for (int64_t r = 0; r < 2; ++r) {
      ASSERT_OK(Insert("a", h, r, h * 10 + r));
      ASSERT_OK(Insert("b", h, r, -10000 + h * 10 + r));
      expected.insert({h, r, h * 10 + r});
    }
  }
  std::array<bool, 2> witnesses{};
  for (int64_t h = 0; h < 1000000 && (!witnesses[0] || !witnesses[1]); ++h) {
    auto request = WriteRequest("a", h, 99, 10000);
    const auto hash = ASSERT_RESULT(store_->HashCode("a", request.partition_column_values()));
    if (hash == 0 || hash == 65535) {
      const size_t index = hash == 0 ? 0 : 1;
      if (!witnesses[index]) {
        ASSERT_OK(Insert("a", h, 99, 10000));
        expected.insert({h, 99, 10000});
        witnesses[index] = true;
      }
    }
  }
  ASSERT_TRUE(witnesses[0]);
  ASSERT_TRUE(witnesses[1]);
  ASSERT_EQ(ASSERT_RESULT(ReadAll("a")), expected);
  const auto snapshot = store_->LatestReadTime();

  std::set<Row> seen;
  for (uint32_t lo = 0; lo < 65536; lo += 257) {
    const auto hi = std::min(lo + 257, 65536U);
    auto request = ReadRequest("a");
    request.set_hash_code(lo);
    request.set_max_hash_code(hi - 1);
    for (size_t tablet = 0; tablet < 4; ++tablet) {
      auto result = ASSERT_RESULT(store_->Read(request, tablet));
      for (const auto& row : ASSERT_RESULT(DecodeRows(result))) {
        const auto hash = ASSERT_RESULT(store_->HashCode(
            "a", WriteRequest("a", row[0], row[1], row[2]).partition_column_values()));
        ASSERT_GE(hash, lo);
        ASSERT_LT(hash, hi);
        ASSERT_TRUE(seen.insert(row).second);
      }
    }
  }
  ASSERT_EQ(seen, expected);

  auto update = WriteRequest("a", 42, 0, 999);
  update.set_stmt_type(PgsqlWriteRequestPB::PGSQL_UPDATE);
  *update.add_column_new_values() = update.column_values(0);
  update.clear_column_values();
  PgsqlResponsePB response;
  ASSERT_OK(store_->ApplyAutocommitWrite(update, &response));
  ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
  expected.erase(Row{42, 0, 420});
  expected.insert(Row{42, 0, 999});
  ASSERT_TRUE(ASSERT_RESULT(ReadAll("a", snapshot)).contains(Row{42, 0, 420}));
  ASSERT_TRUE(ASSERT_RESULT(ReadAll("b")).contains(Row{42, 0, -9580}));
  ASSERT_EQ(ASSERT_RESULT(ReadAll("a")), expected);

  auto remove = WriteRequest("a", 43, 0, 0);
  remove.set_stmt_type(PgsqlWriteRequestPB::PGSQL_DELETE);
  remove.clear_column_values();
  ASSERT_OK(store_->ApplyAutocommitWrite(remove, &response));
  ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
  expected.erase(Row{43, 0, 430});
  const auto latest = store_->LatestReadTime();
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalStore::Open(path_));
  ASSERT_EQ(store_->LatestReadTime(), latest);
  ASSERT_EQ(ASSERT_RESULT(ReadAll("a")), expected);
  ASSERT_TRUE(ASSERT_RESULT(ReadAll("a", snapshot)).contains(Row{43, 0, 430}));
  auto restored = ASSERT_RESULT(store_->GetSchema("a"));
  ASSERT_EQ(restored.num_hash_key_columns(), 1);
  ASSERT_FALSE(restored.has_colocation_id());
  ASSERT_EQ(ASSERT_RESULT(store_->GetPartitions("a")).size(), 4);
}

TEST_F(PgLocalStoreTest, RejectsUnsupportedAndInvalidRequests) {
  PgsqlResponsePB response;
  auto bad = WriteRequest("a", 1, 0, 2);
  bad.set_hash_code(65536);
  ASSERT_TRUE(store_->ApplyAutocommitWrite(bad, &response).IsInvalidArgument());
  bad.clear_hash_code();
  bad.set_schema_version(1);
  ASSERT_TRUE(store_->ApplyAutocommitWrite(bad, &response).IsInvalidArgument());
  bad.set_schema_version(0);
  bad.mutable_where_expr()->mutable_value()->set_bool_value(true);
  ASSERT_TRUE(store_->ApplyAutocommitWrite(bad, &response).IsNotSupported());
  ASSERT_TRUE(ASSERT_RESULT(ReadAll("a")).empty());
  auto read = ReadRequest("a");
  read.set_hash_code(65536);
  ASSERT_TRUE(store_->Read(read, 0).status().IsInvalidArgument());
  read.clear_hash_code();
  read.set_return_paging_state(true);
  ASSERT_TRUE(store_->Read(read, 0).status().IsNotSupported());
  read.clear_return_paging_state();
  ASSERT_TRUE(store_->Read(read, 4).status().IsInvalidArgument());
  read.clear_column_refs();
  read.add_col_refs();
  ASSERT_TRUE(store_->Read(read, 0).status().IsInvalidArgument());
  read.mutable_col_refs(0)->set_column_id(schema_.column_id(0));
  read.mutable_col_refs(0)->set_typid(25);
  ASSERT_TRUE(store_->Read(read, 0).status().IsNotSupported());
  read = ReadRequest("a");
  read.mutable_column_refs()->add_ids(999);
  ASSERT_TRUE(store_->Read(read, 0).status().IsInvalidArgument());
  bad = WriteRequest("a", 1, 0, 2);
  bad.mutable_partition_column_values(0)->mutable_value()->set_string_value("1");
  ASSERT_TRUE(store_->ApplyAutocommitWrite(bad, &response).IsInvalidArgument());
  bad = WriteRequest("a", 1, 0, 2);
  bad.mutable_column_values(0)->mutable_expr()->mutable_value()->set_string_value("2");
  ASSERT_TRUE(store_->ApplyAutocommitWrite(bad, &response).IsInvalidArgument());
  read = ReadRequest("a");
  read.set_hash_code(12345);
  read.set_max_hash_code(12344);
  ASSERT_TRUE(ASSERT_RESULT(DecodeRows(ASSERT_RESULT(store_->Read(read, 0)))).empty());
}

TEST_F(PgLocalStoreTest, PointCompositeAndDuplicateKeys) {
  SchemaBuilder builder;
  ASSERT_OK(builder.AddHashKeyColumn("h", DataType::INT64));
  ASSERT_OK(builder.AddHashKeyColumn("r", DataType::INT64));
  ASSERT_OK(builder.AddColumn("v", DataType::INT64));
  ASSERT_OK(store_->CreateTable("c", builder.Build(), 3));
  // SELECT k, yb_hash_code(k), yb_hash_code(k,k+1), with k explicitly bigint, on YSQL.
  const std::array<Row, 5> hashes = {
      {{-1, 22565, 26617},
       {0, 59242, 45198},
       {1, 60841, 59462},
       {42, 48975, 45438},
       {65535, 9647, 56920}}};
  for (const auto& oracle : hashes) {
    auto key = WriteRequest("a", oracle[0], oracle[0] + 1, 0);
    ASSERT_EQ(ASSERT_RESULT(store_->HashCode("a", key.partition_column_values())), oracle[1]);
    *key.add_partition_column_values() = key.range_column_values(0);
    ASSERT_EQ(ASSERT_RESULT(store_->HashCode("c", key.partition_column_values())), oracle[2]);
  }
  PgsqlResponsePB response;
  std::set<Row> expected;
  for (int64_t h = -10; h < 90; ++h) {
    auto write = WriteRequest("c", h, h + 1, h * 2);
    *write.add_partition_column_values() = write.range_column_values(0);
    write.clear_range_column_values();
    ASSERT_OK(store_->ApplyAutocommitWrite(write, &response));
    ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
    expected.insert({h, h + 1, h * 2});
  }
  std::array<bool, 2> witnesses{};
  for (int64_t h = 0; h < 1000000 && (!witnesses[0] || !witnesses[1]); ++h) {
    auto write = WriteRequest("c", h, h + 1, h * 2);
    *write.add_partition_column_values() = write.range_column_values(0);
    write.clear_range_column_values();
    const auto hash = ASSERT_RESULT(store_->HashCode("c", write.partition_column_values()));
    if (hash == 0 || hash == 65535) {
      witnesses[hash == 0 ? 0 : 1] = true;
      if (expected.insert({h, h + 1, h * 2}).second) {
        ASSERT_OK(store_->ApplyAutocommitWrite(write, &response));
        ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
      }
    }
  }
  ASSERT_TRUE(witnesses[0]);
  ASSERT_TRUE(witnesses[1]);
  ASSERT_EQ(ASSERT_RESULT(ReadAll("c")), expected);
  std::set<Row> observed;
  for (uint32_t lo = 0; lo < 65536; lo += 257) {
    auto read = ReadRequest("c");
    read.set_hash_code(lo);
    read.set_max_hash_code(std::min(lo + 257, 65536U) - 1);
    for (size_t tablet = 0; tablet < 3; ++tablet) {
      for (const auto& row : ASSERT_RESULT(DecodeRows(ASSERT_RESULT(store_->Read(read, tablet))))) {
        auto key = WriteRequest("c", row[0], row[1], row[2]);
        *key.add_partition_column_values() = key.range_column_values(0);
        key.clear_range_column_values();
        const auto hash = ASSERT_RESULT(store_->HashCode("c", key.partition_column_values()));
        ASSERT_GE(hash, lo);
        ASSERT_LE(hash, read.max_hash_code());
        ASSERT_TRUE(observed.insert(row).second);
      }
    }
  }
  ASSERT_EQ(observed, expected);
  ASSERT_OK(Insert("a", 42, 2, 7));
  ASSERT_OK(Insert("a", 42, 3, 8));
  auto point = ReadRequest("a");
  point.add_partition_column_values()->mutable_value()->set_int64_value(42);
  point.add_range_column_values()->mutable_value()->set_int64_value(2);
  size_t rows = 0;
  for (size_t tablet = 0; tablet < 4; ++tablet) {
    auto decoded = ASSERT_RESULT(DecodeRows(ASSERT_RESULT(store_->Read(point, tablet))));
    for (const auto& row : decoded) {
      ASSERT_EQ(row, (Row{42, 2, 7}));
    }
    rows += decoded.size();
  }
  ASSERT_EQ(rows, 1);
  const auto before = store_->LatestReadTime();
  ASSERT_OK(store_->ApplyAutocommitWrite(WriteRequest("a", 42, 2, 100), &response));
  ASSERT_NE(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
  ASSERT_EQ(store_->LatestReadTime(), before);
  ASSERT_TRUE(ASSERT_RESULT(ReadAll("a")).contains(Row{42, 2, 7}));
}

TEST_F(PgLocalStoreTest, DocKeyScanBounds) {
  ASSERT_OK(Insert("a", 42, 2, 7));
  ASSERT_OK(Insert("a", 42, 3, 8));
  const auto hash =
      ASSERT_RESULT(store_->HashCode("a", WriteRequest("a", 42, 2, 7).partition_column_values()));
  const auto key =
      dockv::DocKey(
          schema_, hash, {dockv::KeyEntryValue::Int64(42)}, {dockv::KeyEntryValue::Int64(2)})
          .Encode()
          .ToStringBuffer();
  auto request = ReadRequest("a");
  request.mutable_lower_bound()->set_key(key);
  request.mutable_lower_bound()->set_is_inclusive(true);
  request.mutable_upper_bound()->set_key(key);
  request.mutable_upper_bound()->set_is_inclusive(true);
  auto read = [&]() -> Result<std::vector<Row>> {
    std::vector<Row> rows;
    for (size_t tablet = 0; tablet < 4; ++tablet) {
      const auto batch = VERIFY_RESULT(DecodeRows(VERIFY_RESULT(store_->Read(request, tablet))));
      rows.insert(rows.end(), batch.begin(), batch.end());
    }
    return rows;
  };
  ASSERT_EQ(ASSERT_RESULT(read()), (std::vector<Row>{{42, 2, 7}}));
  request.mutable_upper_bound()->set_is_inclusive(false);
  ASSERT_TRUE(ASSERT_RESULT(read()).empty());
  request.mutable_lower_bound()->set_key(std::string(1, '\0'));
  ASSERT_NOK(store_->Read(request, 0).status());
}

TEST_F(PgLocalStoreTest, AmbiguousWriteFailureRemainsFenced) {
  auto* sync = SyncPoint::GetInstance();
  sync->SetCallBack("PgLocalStore::PersistStatus", [](void* status) {
    *static_cast<Status*>(status) = STATUS(IOError, "Simulated ambiguous write failure");
  });
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->EnableProcessing();
  ASSERT_TRUE(Insert("a", 9, 8, 7).IsIOError());
  ASSERT_TRUE(store_->CreateTable("c", schema_, 1).IsIOError());
  ASSERT_TRUE(ResultToStatus(ReadAll("a")).IsIOError());
  ASSERT_TRUE(Insert("b", 1, 2, 3).IsIOError());
  ASSERT_TRUE(ResultToStatus(ReadAll("b")).IsIOError());
  sync->DisableProcessing();
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalStore::Open(path_));
  ASSERT_TRUE(ASSERT_RESULT(ReadAll("a")).contains(Row{9, 8, 7}));
  ASSERT_TRUE(ASSERT_RESULT(ReadAll("b")).empty());
}

TEST_F(PgLocalStoreTest, SigkillRecoveryWithInterruptedCreate) {
  const auto* child_path = std::getenv("YB_LOCAL_STORE_CRASH_PATH");
  if (child_path) {
    store_.reset();
    store_ = ASSERT_RESULT(PgLocalStore::Open(child_path));
    ASSERT_OK(store_->CreateTable("a", schema_, 4));
    ASSERT_OK(Insert("a", 1, 2, 3));
    ASSERT_OK(Insert("a", 4, 5, 6));
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("PgLocalStore::SchemaPersisted", [](void*) { std::raise(SIGKILL); });
    sync->EnableProcessing();
    ASSERT_OK(store_->CreateTable("pending", schema_, 4));
    FAIL() << "Expected SIGKILL after durable schema publication";
  }
  std::string executable;
  ASSERT_OK(Env::Default()->GetExecutablePath(&executable));
  const auto crash_path = GetTestPath("crash-store");
  Subprocess child(
      executable,
      {executable, "--gtest_filter=PgLocalStoreTest.SigkillRecoveryWithInterruptedCreate"});
  child.SetEnv("YB_LOCAL_STORE_CRASH_PATH", crash_path);
  child.SetEnv("TEST_TMPDIR", GetTestPath("child-root"));
  ASSERT_OK(child.Start());
  int result;
  ASSERT_OK(child.Wait(&result));
  ASSERT_TRUE(WIFSIGNALED(result));
  ASSERT_EQ(WTERMSIG(result), SIGKILL);
  store_.reset();
  store_ = ASSERT_RESULT(PgLocalStore::Open(crash_path));
  ASSERT_EQ(ASSERT_RESULT(ReadAll("a")), (std::set<Row>{{1, 2, 3}, {4, 5, 6}}));
  ASSERT_EQ(ASSERT_RESULT(store_->GetPartitions("pending")).size(), 4);
  ASSERT_TRUE(ASSERT_RESULT(ReadAll("pending")).empty());
  const auto recovered = store_->LatestReadTime();
  ASSERT_OK(Insert("a", 7, 8, 9));
  ASSERT_GT(store_->LatestReadTime(), recovered);
  ASSERT_EQ(ASSERT_RESULT(ReadAll("a", recovered)).size(), 2);
}

}  // namespace
}  // namespace yb::local_runtime
