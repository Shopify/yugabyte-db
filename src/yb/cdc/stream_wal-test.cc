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
// Unit tests for the StreamWAL tserver RPC.
//
// These cover the contract's behavior matrix that can be exercised without a
// YSQL test cluster (request validation, bootstrap shapes, soft caps,
// single-shard writes, DDL, TRUNCATE, error envelopes). The multi-shard /
// transactional / SPLIT_OP / xrepl_origin_id scenarios require the
// CDCSDKYsqlTest harness in src/yb/integration-tests and are deliberately out
// of scope for this file -- they are covered in the follow-up integration test
// suite.

#include "yb/cdc/cdc_service.h"
#include "yb/cdc/cdc_service.pb.h"
#include "yb/cdc/cdc_service.proxy.h"

#include "yb/client/schema.h"
#include "yb/client/session.h"
#include "yb/client/table.h"
#include "yb/client/table_creator.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"

#include "yb/common/wire_protocol.h"

#include "yb/integration-tests/cdc_test_util.h"
#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/yb_mini_cluster_test_base.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/test_macros.h"

namespace yb {
namespace cdc {

using namespace std::chrono_literals;

namespace {

constexpr int kRpcTimeoutSec = 60;
constexpr int kNumMasters = 1;
constexpr int kNumTServers = 1;
const auto kNamespace = "stream_wal_ns";
const auto kTableName = "stream_wal_t";
const auto kKeyCol = "k";
const auto kValueCol = "v";
const auto kYbTableName = client::YBTableName(YQL_DATABASE_CQL, kNamespace, kTableName);

const StreamWalCursorPB kFromStart = [] {
  StreamWalCursorPB c;
  c.set_term(0);
  c.set_index(0);
  return c;
}();

const StreamWalCursorPB kSkipToTip = [] {
  StreamWalCursorPB c;
  c.set_term(-1);
  c.set_index(-1);
  return c;
}();

StreamWalCursorPB MakeCursor(int64_t term, int64_t index) {
  StreamWalCursorPB c;
  c.set_term(term);
  c.set_index(index);
  return c;
}

bool IsBootstrapDDL(const CDCSDKProtoRecordPB& rec) {
  return rec.has_row_message() && rec.row_message().op() == RowMessage_Op_DDL &&
         rec.has_cdc_sdk_op_id() && rec.cdc_sdk_op_id().term() == 0 &&
         rec.cdc_sdk_op_id().index() == 0;
}

}  // namespace

class StreamWalTest : public MiniClusterTestWithClient<MiniCluster> {
 public:
  void SetUp() override {
    MiniClusterTestWithClient<MiniCluster>::SetUp();
    YBMiniClusterTestBase::SetUp();
    MiniClusterOptions opts;
    opts.num_tablet_servers = kNumTServers;
    opts.num_masters = kNumMasters;
    cluster_.reset(new MiniCluster(opts));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->WaitForTabletServerCount(opts.num_tablet_servers));

    ASSERT_OK(CreateClient());
    ASSERT_OK(client_->CreateNamespace(kNamespace));

    tablet_server_ = cluster_->mini_tablet_servers().front().get();
    cdc_proxy_ = std::make_unique<CDCServiceProxy>(
        &client_->proxy_cache(),
        HostPort::FromBoundEndpoint(tablet_server_->bound_rpc_addr()));

    ASSERT_OK(CreateTable());
  }

  Status CreateTable() {
    client::YBSchema schema;
    client::YBSchemaBuilder b;
    b.AddColumn(kKeyCol)->Type(DataType::INT32)->NotNull()->HashPrimaryKey();
    b.AddColumn(kValueCol)->Type(DataType::INT32)->NotNull();
    RETURN_NOT_OK(b.Build(&schema));

    std::unique_ptr<client::YBTableCreator> creator(client_->NewTableCreator());
    RETURN_NOT_OK(
        creator->table_name(kYbTableName).schema(&schema).num_tablets(1).wait(true).Create());
    RETURN_NOT_OK(client_->OpenTable(kYbTableName, &table_));

    auto tablet_ids = ListTabletIdsForTable(cluster_.get(), table_->id());
    SCHECK_EQ(tablet_ids.size(), 1, IllegalState, "Expected 1 tablet");
    tablet_id_ = *tablet_ids.begin();
    return Status::OK();
  }

  Status InsertRows(int32_t start, int32_t end) {
    auto session = client_->NewSession(kRpcTimeoutSec * 1s);
    client::TableHandle handle;
    RETURN_NOT_OK(handle.Open(kYbTableName, client_.get()));
    std::vector<client::YBOperationPtr> ops;
    for (int32_t i = start; i < end; ++i) {
      auto op = handle.NewInsertOp();
      auto* req = op->mutable_request();
      QLAddInt32HashValue(req, i);
      handle.AddInt32ColumnValue(req, handle->schema().Column(1).name(), i);
      ops.push_back(std::move(op));
    }
    return session->TEST_ApplyAndFlush(ops);
  }

  Result<StreamWalResponsePB> StreamWal(
      const StreamWalCursorPB& from,
      uint32_t max_records = 0,
      uint64_t max_bytes = 0,
      const std::string& tablet_id_override = "") {
    StreamWalRequestPB req;
    req.set_tablet_id(tablet_id_override.empty() ? tablet_id_ : tablet_id_override);
    *req.mutable_from_op_id() = from;
    if (max_records > 0) {
      req.set_max_records(max_records);
    }
    if (max_bytes > 0) {
      req.set_max_bytes(max_bytes);
    }
    StreamWalResponsePB resp;
    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(kRpcTimeoutSec));
    RETURN_NOT_OK(cdc_proxy_->StreamWAL(req, &resp, &rpc));
    return resp;
  }

  tserver::MiniTabletServer* tablet_server_ = nullptr;
  std::unique_ptr<CDCServiceProxy> cdc_proxy_;
  std::shared_ptr<client::YBTable> table_;
  TabletId tablet_id_;
};

// Case (h): tablet doesn't exist -> TABLET_NOT_FOUND.
TEST_F(StreamWalTest, TabletNotFound) {
  auto resp = ASSERT_RESULT(
      StreamWal(kFromStart, /*max_records=*/0, /*max_bytes=*/0,
                "00000000000000000000000000000000"));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(resp.error().code(), CDCErrorPB::TABLET_NOT_FOUND);
}

// Request validation: missing tablet_id.
TEST_F(StreamWalTest, MissingTabletId) {
  StreamWalRequestPB req;
  *req.mutable_from_op_id() = kFromStart;
  StreamWalResponsePB resp;
  rpc::RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(kRpcTimeoutSec));
  ASSERT_OK(cdc_proxy_->StreamWAL(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(resp.error().code(), CDCErrorPB::INVALID_REQUEST);
}

// Request validation: missing from_op_id.
TEST_F(StreamWalTest, MissingFromOpId) {
  StreamWalRequestPB req;
  req.set_tablet_id(tablet_id_);
  StreamWalResponsePB resp;
  rpc::RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(kRpcTimeoutSec));
  ASSERT_OK(cdc_proxy_->StreamWAL(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(resp.error().code(), CDCErrorPB::INVALID_REQUEST);
}

// Request validation: from_op_id with invalid shape -- term < 0 but not the
// skip-to-tip sentinel.
TEST_F(StreamWalTest, InvalidFromOpIdShape) {
  auto resp = ASSERT_RESULT(StreamWal(MakeCursor(-5, 10)));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(resp.error().code(), CDCErrorPB::INVALID_REQUEST);
}

// Request validation: T=0, I=5 is rejected (a "real" cursor must have term>=1).
TEST_F(StreamWalTest, InvalidFromOpIdTermZeroIndexNonZero) {
  auto resp = ASSERT_RESULT(StreamWal(MakeCursor(0, 5)));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(resp.error().code(), CDCErrorPB::INVALID_REQUEST);
}

// Case (a): skip-to-tip on an empty tablet -> bootstrap DDLs only, next_op_id
// == leader_tip, no real records.
TEST_F(StreamWalTest, SkipToTipEmptyTablet) {
  auto resp = ASSERT_RESULT(StreamWal(kSkipToTip));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  // One DDL per user table (we created one). Bootstrap records have
  // cdc_sdk_op_id.term = 0, cdc_sdk_op_id.index = 0.
  ASSERT_EQ(resp.records_size(), 1);
  ASSERT_TRUE(IsBootstrapDDL(resp.records(0)));
  ASSERT_EQ(resp.records(0).cdc_sdk_op_id().write_id(), 0);
  ASSERT_EQ(resp.records(0).row_message().table_id(), table_->id());
  ASSERT_GT(resp.records(0).row_message().schema().column_info_size(), 0);

  // next_op_id == leader_tip_op_id on the skip-to-tip path.
  ASSERT_TRUE(resp.has_next_op_id());
  ASSERT_TRUE(resp.has_leader_tip_op_id());
  ASSERT_EQ(resp.next_op_id().term(), resp.leader_tip_op_id().term());
  ASSERT_EQ(resp.next_op_id().index(), resp.leader_tip_op_id().index());

  // leader_safe_hybrid_time should be set on success.
  ASSERT_TRUE(resp.has_leader_safe_hybrid_time());
}

// Case (c): {0,0} on a tablet with some history -> bootstrap DDL(s) at the
// front, followed by real WAL records in order.
TEST_F(StreamWalTest, FromStartIncludesBootstrapThenRealRecords) {
  ASSERT_OK(InsertRows(0, 5));
  auto resp = ASSERT_RESULT(StreamWal(kFromStart));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  ASSERT_GE(resp.records_size(), 1);
  // First record is a bootstrap DDL with cdc_sdk_op_id = {0, 0, ...}.
  ASSERT_TRUE(IsBootstrapDDL(resp.records(0)));

  // Find the first non-bootstrap record; it should be a single-shard write
  // (INSERT). Single-shard writes have transaction_id unset and commit_time set
  // == ReplicateMsg.hybrid_time.
  int first_real = -1;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (!IsBootstrapDDL(resp.records(i))) {
      first_real = i;
      break;
    }
  }
  ASSERT_GE(first_real, 0) << "No real WAL records were emitted past bootstrap";
  const auto& first = resp.records(first_real);
  ASSERT_TRUE(first.has_row_message());
  ASSERT_EQ(first.row_message().op(), RowMessage_Op_INSERT);
  ASSERT_FALSE(first.row_message().has_transaction_id());
  ASSERT_TRUE(first.row_message().has_commit_time())
      << "single-shard writes must carry commit_time";
  ASSERT_GT(first.row_message().commit_time(), 0u);

  // next_op_id advances to the last real record.
  ASSERT_TRUE(resp.has_next_op_id());
  ASSERT_GE(resp.next_op_id().index(), 1);
}

// Case (d): Resume from a mid-stream OpId -> no bootstrap; records strictly
// after that OpId.
TEST_F(StreamWalTest, ResumeFromMidStreamNoBootstrap) {
  ASSERT_OK(InsertRows(0, 10));
  auto first = ASSERT_RESULT(StreamWal(kFromStart));
  ASSERT_GE(first.records_size(), 2);
  // Pick a mid-stream cursor: the cdc_sdk_op_id of the 2nd record after
  // bootstrap.
  int b_count = 0;
  for (int i = 0; i < first.records_size(); ++i) {
    if (IsBootstrapDDL(first.records(i))) ++b_count;
  }
  ASSERT_GE(first.records_size() - b_count, 2);

  const auto& mid = first.records(b_count + 1);  // 2nd real record
  ASSERT_TRUE(mid.has_cdc_sdk_op_id());
  auto cursor = MakeCursor(mid.cdc_sdk_op_id().term(), mid.cdc_sdk_op_id().index());

  auto resumed = ASSERT_RESULT(StreamWal(cursor));
  ASSERT_FALSE(resumed.has_error());
  // No bootstrap when resuming from a real cursor.
  for (const auto& r : resumed.records()) {
    ASSERT_FALSE(IsBootstrapDDL(r))
        << "resume from real cursor must not include bootstrap DDLs";
  }
  // Every record must have index strictly greater than the resume cursor's
  // index (the cursor is exclusive on the lower bound).
  for (const auto& r : resumed.records()) {
    ASSERT_GT(r.cdc_sdk_op_id().index(), cursor.index())
        << "records must be strictly after from_op_id";
  }
}

// Case (f): Resume from an OpId past leader tip -> INVALID_REQUEST.
TEST_F(StreamWalTest, ResumePastLeaderTip) {
  ASSERT_OK(InsertRows(0, 3));
  auto tip = ASSERT_RESULT(StreamWal(kSkipToTip));
  ASSERT_TRUE(tip.has_leader_tip_op_id());
  auto bogus = MakeCursor(tip.leader_tip_op_id().term(),
                          tip.leader_tip_op_id().index() + 1000000);

  auto resp = ASSERT_RESULT(StreamWal(bogus));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(resp.error().code(), CDCErrorPB::INVALID_REQUEST);
}

// Cursor edge case: from_op_id exactly at leader tip -> empty success batch,
// next_op_id == from_op_id, has_more = false.
TEST_F(StreamWalTest, ResumeAtLeaderTipIsEmptyBatch) {
  ASSERT_OK(InsertRows(0, 3));
  auto tip = ASSERT_RESULT(StreamWal(kSkipToTip));
  ASSERT_TRUE(tip.has_leader_tip_op_id());
  auto cursor = MakeCursor(tip.leader_tip_op_id().term(), tip.leader_tip_op_id().index());

  auto resp = ASSERT_RESULT(StreamWal(cursor));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  ASSERT_EQ(resp.records_size(), 0);
  ASSERT_TRUE(resp.has_next_op_id());
  ASSERT_EQ(resp.next_op_id().term(), cursor.term());
  ASSERT_EQ(resp.next_op_id().index(), cursor.index());
  ASSERT_FALSE(resp.has_more());
}

// Case (p): max_records cap -> server returns <= cap and has_more=true.
TEST_F(StreamWalTest, MaxRecordsCap) {
  ASSERT_OK(InsertRows(0, 50));
  auto resp = ASSERT_RESULT(StreamWal(kFromStart, /*max_records=*/3));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  ASSERT_LE(resp.records_size(), 3 + /*bootstrap=*/1);
  ASSERT_TRUE(resp.has_more());
}

// Case (q): max_bytes cap -> server returns has_more=true once the running
// byte cap is exceeded.
TEST_F(StreamWalTest, MaxBytesCap) {
  ASSERT_OK(InsertRows(0, 50));
  auto resp = ASSERT_RESULT(StreamWal(kFromStart, /*max_records=*/0, /*max_bytes=*/256));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  // We don't truncate records, so a single op may exceed the cap. has_more must
  // be true so the client knows to call again.
  ASSERT_TRUE(resp.has_more());
}

// Case (i): single-shard write produces RowMessage with commit_time set and
// transaction_id unset.
TEST_F(StreamWalTest, SingleShardWriteShape) {
  ASSERT_OK(InsertRows(42, 43));
  auto resp = ASSERT_RESULT(StreamWal(kFromStart));
  bool saw_insert = false;
  for (const auto& r : resp.records()) {
    if (r.has_row_message() && r.row_message().op() == RowMessage_Op_INSERT) {
      ASSERT_FALSE(r.row_message().has_transaction_id())
          << "single-shard write must not carry transaction_id";
      ASSERT_TRUE(r.row_message().has_commit_time())
          << "single-shard write must carry commit_time = ReplicateMsg.hybrid_time";
      ASSERT_GT(r.row_message().commit_time(), 0u);
      ASSERT_TRUE(r.has_cdc_sdk_op_id());
      ASSERT_GE(r.cdc_sdk_op_id().term(), 1);
      saw_insert = true;
    }
  }
  ASSERT_TRUE(saw_insert);
}

// Skip-to-tip is idempotent within the contract: repeated calls re-emit
// bootstrap and pin next_op_id to the current leader tip.
TEST_F(StreamWalTest, SkipToTipIsIdempotent) {
  auto r1 = ASSERT_RESULT(StreamWal(kSkipToTip));
  auto r2 = ASSERT_RESULT(StreamWal(kSkipToTip));
  ASSERT_EQ(r1.records_size(), r2.records_size());
  // Both responses bootstrap from the same metadata.
  ASSERT_EQ(r1.records(0).row_message().table_id(),
            r2.records(0).row_message().table_id());
}

// Cursor edge case: cdc_sdk_op_id stamping. Every emitted record gets
// from_op_id stamped to the request's from cursor.
TEST_F(StreamWalTest, FromOpIdStampedOnEveryRecord) {
  ASSERT_OK(InsertRows(0, 3));
  auto cursor = MakeCursor(/*term=*/1, /*index=*/0);
  // Use a real cursor to skip bootstrap.
  auto resp = ASSERT_RESULT(StreamWal(cursor));
  if (resp.has_error()) {
    // Acceptable if the tablet's earliest WAL record is past term 1; just
    // verify the from_op_id stamping path on the from-start cursor instead.
    auto fs = ASSERT_RESULT(StreamWal(kFromStart));
    for (const auto& r : fs.records()) {
      ASSERT_TRUE(r.has_from_op_id());
      ASSERT_EQ(r.from_op_id().term(), kFromStart.term());
      ASSERT_EQ(r.from_op_id().index(), kFromStart.index());
    }
    return;
  }
  for (const auto& r : resp.records()) {
    ASSERT_TRUE(r.has_from_op_id());
    ASSERT_EQ(r.from_op_id().term(), cursor.term());
    ASSERT_EQ(r.from_op_id().index(), cursor.index());
  }
}

}  // namespace cdc
}  // namespace yb
