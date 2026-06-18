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
// single-shard writes, DDL, TRUNCATE, error envelopes). A small YSQL-backed
// fixture at the bottom covers the StreamWAL transactional shape that requires
// a PostgreSQL frontend (e.g. secondary-index writes). Broader multi-tablet /
// SPLIT_OP / xrepl_origin_id scenarios remain in the integration test suite.

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

#include "yb/tablet/tablet.h"
#include "yb/tablet/tablet_peer.h"
#include "yb/tablet/transaction_participant.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"

#include "yb/common/opid.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/flags.h"
#include "yb/util/test_macros.h"

#include "yb/yql/pgwrapper/pg_mini_test_base.h"

DECLARE_uint64(cdc_max_stream_intent_records);
DECLARE_uint32(intents_min_seconds_to_retain);
DECLARE_uint64(aborted_intent_cleanup_ms);

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

const StreamWalCursorPB kSkipToLatest = [] {
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

StreamWalCursorPB MakeMidApplyingCursor(
    int64_t term, int64_t index, const std::string& intent_key, uint32_t intent_write_id) {
  StreamWalCursorPB c;
  c.set_term(term);
  c.set_index(index);
  c.set_intent_key(intent_key);
  c.set_intent_write_id(intent_write_id);
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

  // Inserts [start, end) as a single batched flush. All rows land in one WAL
  // WRITE_OP (one ReplicateMsg with N write_pairs), since they target the same
  // single-tablet table.
  Status InsertRows(int32_t start, int32_t end) {
    auto session = client_->NewSession(kRpcTimeoutSec * 1s);
    client::TableHandle handle;
    RETURN_NOT_OK(handle.Open(kYbTableName, client_.get()));
    std::vector<client::YBOperationPtr> ops;
    for (int32_t i = start; i < end; ++i) {
      auto op = handle.NewInsertOp(session->arena());
      auto* req = op->mutable_request();
      QLAddInt32HashValue(req, i);
      handle.AddInt32ColumnValue(req, handle->schema().Column(1).name(), i);
      ops.push_back(std::move(op));
    }
    return session->TEST_ApplyAndFlush(ops);
  }

  // Inserts [start, end) one row per flush, so each row becomes its own WAL
  // WRITE_OP at a distinct OpId index. Needed by tests that exercise per-op
  // behavior (mid-stream cursor resume, max_records cap): a single batched
  // flush collapses every row into one WAL op the server cannot split.
  Status InsertRowsIndividually(int32_t start, int32_t end) {
    client::TableHandle handle;
    RETURN_NOT_OK(handle.Open(kYbTableName, client_.get()));
    for (int32_t i = start; i < end; ++i) {
      auto session = client_->NewSession(kRpcTimeoutSec * 1s);
      auto op = handle.NewInsertOp(session->arena());
      auto* req = op->mutable_request();
      QLAddInt32HashValue(req, i);
      handle.AddInt32ColumnValue(req, handle->schema().Column(1).name(), i);
      RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
    }
    return Status::OK();
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
// skip-to-latest sentinel.
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

// Request validation: mid-APPLYING fields must be both-or-neither.
TEST_F(StreamWalTest, MidApplyingCursorOnlyIntentKeyIsInvalid) {
  StreamWalCursorPB c;
  c.set_term(1);
  c.set_index(5);
  c.set_intent_key("some-key");
  // intent_write_id unset.
  auto resp = ASSERT_RESULT(StreamWal(c));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(resp.error().code(), CDCErrorPB::INVALID_REQUEST);
}

TEST_F(StreamWalTest, MidApplyingCursorOnlyIntentWriteIdIsInvalid) {
  StreamWalCursorPB c;
  c.set_term(1);
  c.set_index(5);
  c.set_intent_write_id(7);
  // intent_key unset.
  auto resp = ASSERT_RESULT(StreamWal(c));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(resp.error().code(), CDCErrorPB::INVALID_REQUEST);
}

// Request validation: mid-APPLYING fields on the from-start cursor are
// nonsense (no APPLYING to resume at index 0).
TEST_F(StreamWalTest, MidApplyingCursorOnFromStartIsInvalid) {
  auto resp = ASSERT_RESULT(StreamWal(MakeMidApplyingCursor(0, 0, "key", 1)));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(resp.error().code(), CDCErrorPB::INVALID_REQUEST);
}

// Request validation: mid-APPLYING fields on the skip-to-latest sentinel are
// nonsense.
TEST_F(StreamWalTest, MidApplyingCursorOnSkipToLatestIsInvalid) {
  auto resp = ASSERT_RESULT(StreamWal(MakeMidApplyingCursor(-1, -1, "key", 1)));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(resp.error().code(), CDCErrorPB::INVALID_REQUEST);
}

// Case (a): skip-to-latest on an empty tablet -> bootstrap DDLs only, next_op_id
// == leader_tip, no real records.
TEST_F(StreamWalTest, SkipToLatestEmptyTablet) {
  auto resp = ASSERT_RESULT(StreamWal(kSkipToLatest));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  // One DDL per user table (we created one). Bootstrap records have
  // cdc_sdk_op_id.term = 0, cdc_sdk_op_id.index = 0.
  ASSERT_EQ(resp.records_size(), 1);
  ASSERT_TRUE(IsBootstrapDDL(resp.records(0)));
  ASSERT_EQ(resp.records(0).cdc_sdk_op_id().write_id(), 0);
  ASSERT_EQ(resp.records(0).row_message().table_id(), table_->id());
  ASSERT_GT(resp.records(0).row_message().schema().column_info_size(), 0);

  // next_op_id == leader_tip_op_id on the skip-to-latest path.
  ASSERT_TRUE(resp.has_next_op_id());
  ASSERT_TRUE(resp.has_leader_tip_op_id());
  ASSERT_EQ(resp.next_op_id().term(), resp.leader_tip_op_id().term());
  ASSERT_EQ(resp.next_op_id().index(), resp.leader_tip_op_id().index());

  // leader_safe_hybrid_time should be set on success.
  ASSERT_TRUE(resp.has_leader_safe_hybrid_time());
}

// Case (c): {0,0} on a tablet with some history -> bootstrap DDL(s) at the
// front, followed by real WAL records in order. Single-shard writes are
// wrapped in a BEGIN/COMMIT envelope per the StreamWAL contract, so the first
// non-bootstrap record is the BEGIN marker, the row records follow, and a
// COMMIT closes it.
TEST_F(StreamWalTest, FromStartIncludesBootstrapThenRealRecords) {
  ASSERT_OK(InsertRows(0, 5));
  auto resp = ASSERT_RESULT(StreamWal(kFromStart));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  ASSERT_GE(resp.records_size(), 1);
  // First record is a bootstrap DDL with cdc_sdk_op_id = {0, 0, ...}.
  ASSERT_TRUE(IsBootstrapDDL(resp.records(0)));

  // Find the first INSERT row record. Per the contract a single-shard write is
  // emitted as BEGIN, one INSERT|UPDATE|DELETE per row, then COMMIT -- all
  // stamped commit_time = ReplicateMsg.hybrid_time, with transaction_id unset.
  int first_insert = -1;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).has_row_message() &&
        resp.records(i).row_message().op() == RowMessage_Op_INSERT) {
      first_insert = i;
      break;
    }
  }
  ASSERT_GE(first_insert, 0) << "No INSERT records were emitted past bootstrap";
  const auto& insert = resp.records(first_insert);
  ASSERT_FALSE(insert.row_message().has_transaction_id())
      << "single-shard write must not carry transaction_id";
  ASSERT_TRUE(insert.row_message().has_commit_time())
      << "single-shard writes must carry commit_time";
  ASSERT_GT(insert.row_message().commit_time(), 0u);
  ASSERT_TRUE(insert.has_cdc_sdk_op_id());
  ASSERT_GE(insert.cdc_sdk_op_id().term(), 1);

  // The INSERT must be preceded by a BEGIN envelope record (single-shard
  // writes are wrapped in BEGIN/COMMIT).
  bool saw_begin_before_insert = false;
  for (int i = 0; i < first_insert; ++i) {
    if (resp.records(i).has_row_message() &&
        resp.records(i).row_message().op() == RowMessage_Op_BEGIN) {
      saw_begin_before_insert = true;
    }
  }
  ASSERT_TRUE(saw_begin_before_insert)
      << "single-shard INSERT must be wrapped in a BEGIN/COMMIT envelope";

  // next_op_id advances to the last real record.
  ASSERT_TRUE(resp.has_next_op_id());
  ASSERT_GE(resp.next_op_id().index(), 1);
}

// Case (d): Resume from a mid-stream OpId -> no bootstrap; records strictly
// after that OpId.
TEST_F(StreamWalTest, ResumeFromMidStreamNoBootstrap) {
  // Insert one row per flush so each becomes its own WAL op at a distinct OpId
  // index. A batched flush collapses all rows into a single WAL op, leaving no
  // mid-stream OpId to resume from.
  ASSERT_OK(InsertRowsIndividually(0, 10));
  auto first = ASSERT_RESULT(StreamWal(kFromStart));
  ASSERT_FALSE(first.has_error()) << first.error().ShortDebugString();

  // Collect cursors from records carrying a real cdc_sdk_op_id (term >= 1).
  // BEGIN / COMMIT / schema-on-first-use DDL envelope records either carry no
  // cdc_sdk_op_id or the bootstrap {0,0}; the per-row DML and COMMIT records
  // carry the WAL op's (term, index).
  std::vector<StreamWalCursorPB> real_cursors;
  for (const auto& r : first.records()) {
    if (r.has_cdc_sdk_op_id() && r.cdc_sdk_op_id().term() >= 1) {
      real_cursors.push_back(
          MakeCursor(r.cdc_sdk_op_id().term(), r.cdc_sdk_op_id().index()));
    }
  }
  ASSERT_GE(real_cursors.size(), 2);

  // Pick a mid-stream cursor.
  const auto cursor = real_cursors[real_cursors.size() / 2];

  auto resumed = ASSERT_RESULT(StreamWal(cursor));
  ASSERT_FALSE(resumed.has_error()) << resumed.error().ShortDebugString();
  ASSERT_GT(resumed.records_size(), 0)
      << "a mid-stream resume must still return the trailing records";
  // No bootstrap when resuming from a real cursor.
  for (const auto& r : resumed.records()) {
    ASSERT_FALSE(IsBootstrapDDL(r))
        << "resume from real cursor must not include bootstrap DDLs";
  }
  // Every record carrying a real cdc_sdk_op_id must have index strictly greater
  // than the resume cursor's index (the cursor is exclusive on the lower
  // bound). Envelope records (BEGIN / schema-on-first-use DDL) carry no
  // cdc_sdk_op_id and are exempt.
  for (const auto& r : resumed.records()) {
    if (r.has_cdc_sdk_op_id() && r.cdc_sdk_op_id().term() >= 1) {
      ASSERT_GT(r.cdc_sdk_op_id().index(), cursor.index())
          << "records must be strictly after from_op_id";
    }
  }
}

// Case (f): Resume from an OpId past leader tip -> INVALID_REQUEST.
TEST_F(StreamWalTest, ResumePastLeaderTip) {
  ASSERT_OK(InsertRows(0, 3));
  auto tip = ASSERT_RESULT(StreamWal(kSkipToLatest));
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
  auto tip = ASSERT_RESULT(StreamWal(kSkipToLatest));
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

// Case (p): max_records cap -> server stops early (has_more=true) without
// streaming the whole tablet. The cap is soft: the server never splits a
// single WAL op across batches, so a batch may exceed the cap by up to one
// op's worth of records -- but it must be far short of the full stream.
TEST_F(StreamWalTest, MaxRecordsCap) {
  // Skip to latest first to isolate from bootstrap + create-table WAL traffic,
  // then insert one row per flush so each is its own (small) WAL op. A batched
  // flush would collapse into a single un-splittable op and defeat the cap.
  auto tip = ASSERT_RESULT(StreamWal(kSkipToLatest));
  ASSERT_FALSE(tip.has_error()) << tip.error().ShortDebugString();
  ASSERT_TRUE(tip.has_next_op_id());

  ASSERT_OK(InsertRowsIndividually(0, 50));

  auto resp = ASSERT_RESULT(StreamWal(tip.next_op_id(), /*max_records=*/3));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  ASSERT_TRUE(resp.has_more());
  // 50 single-row ops -> ~150+ records available; the cap must stop us well
  // short of that after the first op crosses the threshold.
  ASSERT_LT(resp.records_size(), 50);
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

// Skip-to-latest is idempotent within the contract: repeated calls re-emit
// bootstrap and pin next_op_id to the current leader tip.
TEST_F(StreamWalTest, SkipToLatestIsIdempotent) {
  auto r1 = ASSERT_RESULT(StreamWal(kSkipToLatest));
  auto r2 = ASSERT_RESULT(StreamWal(kSkipToLatest));
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

class StreamWalYsqlSecondaryIndexTest : public pgwrapper::PgMiniTestBase {
 protected:
  size_t NumTabletServers() override { return 1; }

  void SetUp() override {
    // StreamWAL reads a committed transaction's intents from IntentsDB at
    // APPLYING time. Without a wall-clock retention window the normal
    // post-apply cleanup tombstones those intents asynchronously, racing the
    // stream read -- so these tests must pin intents the way a real connector
    // does (intents_min_seconds_to_retain). Set it once for the whole fixture
    // before the cluster starts. aborted_intent_cleanup_ms is raised in tandem
    // so the compaction-filter cleanup path can't reclaim them mid-test.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_intents_min_seconds_to_retain) = 3600;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_aborted_intent_cleanup_ms) = 3600u * 1000u;
    pgwrapper::PgMiniTestBase::SetUp();
  }

  Result<StreamWalResponsePB> StreamWalForTablet(
      const TabletId& tablet_id, const StreamWalCursorPB& from) {
    CDCServiceProxy cdc_proxy(
        &client_->proxy_cache(),
        HostPort::FromBoundEndpoint(cluster_->mini_tablet_server(0)->bound_rpc_addr()));

    StreamWalRequestPB req;
    req.set_tablet_id(tablet_id);
    *req.mutable_from_op_id() = from;

    StreamWalResponsePB resp;
    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(kRpcTimeoutSec));
    RETURN_NOT_OK(cdc_proxy.StreamWAL(req, &resp, &rpc));
    return resp;
  }

  Result<TabletId> OnlyTabletIdForTable(const TableId& table_id) {
    auto tablet_ids = ListTabletIdsForTable(cluster_.get(), table_id);
    SCHECK_EQ(tablet_ids.size(), 1, IllegalState, "Expected exactly one tablet");
    return *tablet_ids.begin();
  }

  // Proves a zero-intent StreamWAL result is NOT caused by garbage collection:
  //   - the tablet's IntentsDB still physically holds intents, and
  //   - no CDC checkpoint is pinned, so GetLatestCheckPoint() == OpId::Max(),
  //     which is the always-true side of the ProcessIntents guard
  //     (op_id <= checkpoint_op_id) that misclassifies the batch as a GC error.
  void ExpectIntentsPresentAndCheckpointUnpinned(const TabletId& tablet_id) {
    auto peers = ASSERT_RESULT(ListTabletActivePeers(cluster_.get(), tablet_id));
    ASSERT_FALSE(peers.empty());
    auto tablet = ASSERT_RESULT(peers.front()->shared_tablet());
    ASSERT_GT(ASSERT_RESULT(tablet->CountIntents()), 0)
        << "intents must still be physically present in IntentsDB -- the zero-intent "
        << "result is not a retention GC";
    ASSERT_EQ(tablet->transaction_participant()->GetLatestCheckPoint(), OpId::Max())
        << "no checkpoint is pinned in the StreamWAL design; the ProcessIntents guard "
        << "(op_id <= checkpoint) is therefore always true";
  }

  // Drives StreamWAL forward from `cursor`, accumulating records, until a COMMIT
  // is observed or `timeout` elapses. Fails if the server ever returns an error.
  Result<std::vector<CDCSDKProtoRecordPB>> DrainUntilCommit(
      const TabletId& tablet_id, StreamWalCursorPB cursor, MonoDelta timeout) {
    std::vector<CDCSDKProtoRecordPB> seen;
    RETURN_NOT_OK(WaitFor(
        [&]() -> Result<bool> {
          auto resp = VERIFY_RESULT(StreamWalForTablet(tablet_id, cursor));
          SCHECK(!resp.has_error(), IllegalState, resp.error().ShortDebugString());
          for (const auto& record : resp.records()) {
            seen.push_back(record);
          }
          if (resp.has_next_op_id()) {
            cursor = resp.next_op_id();
          }
          for (const auto& record : seen) {
            if (record.has_row_message() &&
                record.row_message().op() == RowMessage_Op_COMMIT) {
              return true;
            }
          }
          return false;
        },
        timeout, "wait for StreamWAL to emit the empty BEGIN/COMMIT envelope"));
    return seen;
  }

  // Asserts the drained records are exactly an empty transaction envelope:
  // one BEGIN and one COMMIT sharing a transaction_id, and no DML rows.
  void ExpectEmptyTransactionEnvelope(const std::vector<CDCSDKProtoRecordPB>& records) {
    int begin_count = 0;
    int commit_count = 0;
    int dml_count = 0;
    std::string begin_txn_id;
    std::string commit_txn_id;
    for (const auto& record : records) {
      if (!record.has_row_message()) {
        continue;
      }
      switch (record.row_message().op()) {
        case RowMessage_Op_BEGIN:
          ++begin_count;
          begin_txn_id = record.row_message().transaction_id();
          break;
        case RowMessage_Op_COMMIT:
          ++commit_count;
          commit_txn_id = record.row_message().transaction_id();
          break;
        case RowMessage_Op_INSERT:
        case RowMessage_Op_UPDATE:
        case RowMessage_Op_DELETE:
          ++dml_count;
          break;
        default:
          break;
      }
    }
    ASSERT_EQ(begin_count, 1) << "records seen: " << AsString(records);
    ASSERT_EQ(commit_count, 1) << "records seen: " << AsString(records);
    ASSERT_EQ(dml_count, 0)
        << "a committed txn with no streamable intents must emit no DML rows; records seen: "
        << AsString(records);
    ASSERT_FALSE(begin_txn_id.empty());
    ASSERT_EQ(begin_txn_id, commit_txn_id);
  }
};

// A committed transaction whose only docdb effect is a row lock
// (SELECT ... FOR UPDATE) replicates an APPLYING op, but GetIntentsForCDC
// returns ZERO streamable intents because row-lock intents (kRowLock value)
// are skipped by the decoder.
//
// In the checkpoint-less StreamWAL design GetLatestCheckPoint() is permanently
// OpId::Max(), so the shared ProcessIntents guard `op_id <= checkpoint_op_id`
// is always true. Before the fix this misreported the empty batch as
// INTENTS_GC_ERROR even though nothing was garbage-collected. The fix probes
// the transaction's reverse index: because intents are still present (the apply
// never deletes them and intents_min_seconds_to_retain is one hour), StreamWAL
// now emits an empty BEGIN/COMMIT envelope -- matching GetChangesForCDCSDK's
// shape for an empty committed transaction -- instead of erroring.
//
// ExpectIntentsPresentAndCheckpointUnpinned confirms we exercised exactly the
// would-have-errored regime: intents on disk, checkpoint unpinned.
TEST_F(StreamWalYsqlSecondaryIndexTest, RowLockOnlyTxnEmitsEmptyEnvelope) {
  constexpr auto kYsqlTableName = "stream_wal_rowlock_t";
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.ExecuteFormat(
      "CREATE TABLE $0 (id INT PRIMARY KEY, v INT) SPLIT INTO 1 TABLETS", kYsqlTableName));
  const auto table_id = ASSERT_RESULT(GetTableIDFromTableName(kYsqlTableName));
  const auto tablet_id = ASSERT_RESULT(OnlyTabletIdForTable(table_id));

  // Seed one row via an autocommit (single-shard) write -- no APPLYING, no intents.
  ASSERT_OK(conn.ExecuteFormat("INSERT INTO $0 VALUES (1, 10)", kYsqlTableName));

  // Isolate from CREATE TABLE + seed-INSERT WAL traffic.
  auto tip = ASSERT_RESULT(StreamWalForTablet(tablet_id, kSkipToLatest));
  ASSERT_FALSE(tip.has_error()) << tip.error().ShortDebugString();

  // Pure row-lock transaction.
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  ASSERT_RESULT(conn.FetchFormat("SELECT * FROM $0 WHERE id = 1 FOR UPDATE", kYsqlTableName));
  ASSERT_OK(conn.CommitTransaction());

  auto records =
      ASSERT_RESULT(DrainUntilCommit(tablet_id, tip.next_op_id(), 60s * kTimeMultiplier));
  ASSERT_NO_FATALS(ExpectEmptyTransactionEnvelope(records));
  ASSERT_NO_FATALS(ExpectIntentsPresentAndCheckpointUnpinned(tablet_id));
}

// A transaction whose every write was rolled back via a savepoint:
// BEGIN; SAVEPOINT; INSERT; ROLLBACK TO SAVEPOINT; COMMIT; commits with every
// intent belonging to an aborted subtransaction. GetIntentsBatchForCDC filters
// aborted-subtxn intents, so the APPLYING yields zero streamable intents and
// trips the same always-true guard. The fix emits an empty BEGIN/COMMIT
// envelope rather than INTENTS_GC_ERROR.
TEST_F(StreamWalYsqlSecondaryIndexTest, AllWritesRolledBackEmitsEmptyEnvelope) {
  constexpr auto kYsqlTableName = "stream_wal_rolledback_t";
  auto conn = ASSERT_RESULT(Connect());
  // A UNIQUE secondary index forces the INSERT down the transactional path.
  ASSERT_OK(conn.ExecuteFormat(
      "CREATE TABLE $0 (id INT PRIMARY KEY, sku TEXT UNIQUE, v INT) SPLIT INTO 1 TABLETS",
      kYsqlTableName));
  const auto table_id = ASSERT_RESULT(GetTableIDFromTableName(kYsqlTableName));
  const auto tablet_id = ASSERT_RESULT(OnlyTabletIdForTable(table_id));

  auto tip = ASSERT_RESULT(StreamWalForTablet(tablet_id, kSkipToLatest));
  ASSERT_FALSE(tip.has_error()) << tip.error().ShortDebugString();

  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  ASSERT_OK(conn.Execute("SAVEPOINT sp"));
  ASSERT_OK(conn.ExecuteFormat("INSERT INTO $0 VALUES (1, 'sku-1', 10)", kYsqlTableName));
  ASSERT_OK(conn.Execute("ROLLBACK TO sp"));
  ASSERT_OK(conn.CommitTransaction());

  auto records =
      ASSERT_RESULT(DrainUntilCommit(tablet_id, tip.next_op_id(), 60s * kTimeMultiplier));
  ASSERT_NO_FATALS(ExpectEmptyTransactionEnvelope(records));
  ASSERT_NO_FATALS(ExpectIntentsPresentAndCheckpointUnpinned(tablet_id));
}

// YSQL can execute single-row auto-commit writes through two different DocDB
// paths. A relation with no secondary indexes/triggers can use the fast path
// (covered above by the CQL-shaped single-shard test). A relation with a
// secondary index is not fast-path eligible: YSQL sends a transactional write
// batch.
//
// Under the StreamWAL contract, transactional WRITE_OPs are NOT emitted on
// the wire -- they would carry intent-time data. Instead, the server reads
// the txn's intents from IntentsDB at APPLYING time and emits a complete
// BEGIN + per-row DML + COMMIT envelope, all stamped with commit_hybrid_time
// and the same transaction_id. The per-row record's cdc_sdk_op_id should sit
// at the APPLYING op's (term, index), not at the original WRITE_OP's OpId.
TEST_F(StreamWalYsqlSecondaryIndexTest, SecondaryIndexWriteEmitsAtApplying) {
  constexpr auto kYsqlTableName = "stream_wal_secondary_idx_t";

  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.ExecuteFormat(
      "CREATE TABLE $0 ("
      "  id INT PRIMARY KEY,"
      "  sku TEXT UNIQUE,"
      "  v INT"
      ") SPLIT INTO 1 TABLETS",
      kYsqlTableName));

  const auto table_id = ASSERT_RESULT(GetTableIDFromTableName(kYsqlTableName));
  const auto tablet_id = ASSERT_RESULT(OnlyTabletIdForTable(table_id));

  // Isolate the INSERT from table/index creation WAL traffic.
  auto tip = ASSERT_RESULT(StreamWalForTablet(tablet_id, kSkipToLatest));
  ASSERT_FALSE(tip.has_error()) << tip.error().ShortDebugString();
  ASSERT_TRUE(tip.has_next_op_id());
  StreamWalCursorPB cursor = tip.next_op_id();

  ASSERT_OK(conn.ExecuteFormat("INSERT INTO $0 VALUES (1, 'sku-1', 10)", kYsqlTableName));

  std::vector<CDCSDKProtoRecordPB> seen;
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        auto resp = VERIFY_RESULT(StreamWalForTablet(tablet_id, cursor));
        SCHECK(!resp.has_error(), IllegalState, resp.error().ShortDebugString());
        for (const auto& record : resp.records()) {
          seen.push_back(record);
        }
        if (resp.has_next_op_id()) {
          cursor = resp.next_op_id();
        }

        // Wait until we've seen the full envelope: BEGIN, an INSERT for our
        // table, and a matching COMMIT.
        std::string txn_id;
        bool saw_begin = false;
        bool saw_insert = false;
        bool saw_commit = false;
        for (const auto& record : seen) {
          if (!record.has_row_message()) {
            continue;
          }
          const auto& row = record.row_message();
          if (row.op() == RowMessage_Op_INSERT && row.table_id() == table_id) {
            saw_insert = true;
            txn_id = row.transaction_id();
          }
        }
        if (saw_insert && !txn_id.empty()) {
          for (const auto& record : seen) {
            const auto& row = record.row_message();
            if (row.op() == RowMessage_Op_BEGIN && row.has_transaction_id() &&
                row.transaction_id() == txn_id) {
              saw_begin = true;
            }
            if (row.op() == RowMessage_Op_COMMIT && row.has_transaction_id() &&
                row.transaction_id() == txn_id && row.has_commit_time()) {
              saw_commit = true;
            }
          }
        }
        return saw_begin && saw_insert && saw_commit;
      },
      30s * kTimeMultiplier,
      "wait for StreamWAL transactional secondary-index BEGIN+INSERT+COMMIT envelope"));

  // Walk `seen` and pick the BEGIN / INSERT / COMMIT records for our txn.
  const CDCSDKProtoRecordPB* begin_record = nullptr;
  const CDCSDKProtoRecordPB* insert_record = nullptr;
  const CDCSDKProtoRecordPB* commit_record = nullptr;
  std::string txn_id;
  for (const auto& record : seen) {
    if (!record.has_row_message()) {
      continue;
    }
    const auto& row = record.row_message();
    if (row.op() == RowMessage_Op_INSERT && row.table_id() == table_id) {
      insert_record = &record;
      txn_id = row.transaction_id();
      break;
    }
  }
  ASSERT_NE(insert_record, nullptr) << "records seen: " << AsString(seen);
  ASSERT_FALSE(txn_id.empty());

  for (const auto& record : seen) {
    const auto& row = record.row_message();
    if (row.op() == RowMessage_Op_BEGIN && row.has_transaction_id() &&
        row.transaction_id() == txn_id) {
      begin_record = &record;
    }
    if (row.op() == RowMessage_Op_COMMIT && row.has_transaction_id() &&
        row.transaction_id() == txn_id) {
      commit_record = &record;
    }
  }
  ASSERT_NE(begin_record, nullptr) << "records seen: " << AsString(seen);
  ASSERT_NE(commit_record, nullptr) << "records seen: " << AsString(seen);

  // The per-row INSERT must carry transaction_id AND commit_time -- both are
  // stamped server-side from the matching APPLYING entry's commit_hybrid_time.
  const auto& insert_row = insert_record->row_message();
  ASSERT_TRUE(insert_row.has_transaction_id())
      << "transactional intent row must carry transaction_id";
  ASSERT_TRUE(insert_row.has_commit_time())
      << "transactional intent row must carry commit_hybrid_time";
  ASSERT_GT(insert_row.commit_time(), 0u);
  ASSERT_TRUE(insert_record->has_cdc_sdk_op_id());

  // The COMMIT envelope carries the same commit_time as the per-row INSERT --
  // they are all stamped from the same TransactionStatePB.commit_hybrid_time.
  const auto& commit_row = commit_record->row_message();
  ASSERT_TRUE(commit_row.has_commit_time());
  ASSERT_EQ(commit_row.commit_time(), insert_row.commit_time())
      << "BEGIN/INSERT/COMMIT in a single APPLYING share commit_hybrid_time";

  // The BEGIN, per-row INSERT, and COMMIT records for one transaction are all
  // emitted at the APPLYING op's (term, index). The per-row INSERT's write_id
  // is the intent's IntraTxnWriteId (per the contract), while BEGIN/COMMIT
  // have write_id = 0; the (term, index) component is identical across all
  // three.
  ASSERT_TRUE(begin_record->has_cdc_sdk_op_id());
  ASSERT_TRUE(commit_record->has_cdc_sdk_op_id());
  ASSERT_EQ(begin_record->cdc_sdk_op_id().term(), insert_record->cdc_sdk_op_id().term());
  ASSERT_EQ(begin_record->cdc_sdk_op_id().index(), insert_record->cdc_sdk_op_id().index());
  ASSERT_EQ(commit_record->cdc_sdk_op_id().term(), insert_record->cdc_sdk_op_id().term());
  ASSERT_EQ(commit_record->cdc_sdk_op_id().index(), insert_record->cdc_sdk_op_id().index());
  ASSERT_EQ(begin_record->cdc_sdk_op_id().write_id(), 0u);
  ASSERT_EQ(commit_record->cdc_sdk_op_id().write_id(), 0u);

  // The order across the seen records must be BEGIN <= INSERT <= COMMIT.
  size_t begin_idx = 0, insert_idx = 0, commit_idx = 0;
  for (size_t i = 0; i < seen.size(); ++i) {
    if (&seen[i] == begin_record) begin_idx = i;
    if (&seen[i] == insert_record) insert_idx = i;
    if (&seen[i] == commit_record) commit_idx = i;
  }
  ASSERT_LE(begin_idx, insert_idx);
  ASSERT_LE(insert_idx, commit_idx);
}

// A multi-row transaction whose intent count exceeds
// FLAGS_cdc_max_stream_intent_records must spill across multiple StreamWAL
// batches. Each spilled response carries a partial-APPLYING cursor
// (intent_key + intent_write_id) that the client round-trips back as
// from_op_id. The final batch emits the COMMIT envelope and clears the
// intent fields.
TEST_F(StreamWalYsqlSecondaryIndexTest, MultiRowTxnSpillsAndResumes) {
  constexpr auto kYsqlTableName = "stream_wal_spill_t";
  constexpr int kRowCount = 20;

  // Force ProcessIntents to spill after a handful of intent records. The exact
  // value is below kRowCount so we are guaranteed at least one spill; on the
  // YSQL secondary-index path each row produces 2 intents (table + index) so
  // we'll see roughly kRowCount * 2 / kIntentBudget batches.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_cdc_max_stream_intent_records) = 3;

  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.ExecuteFormat(
      "CREATE TABLE $0 ("
      "  id INT PRIMARY KEY,"
      "  sku TEXT UNIQUE,"
      "  v INT"
      ") SPLIT INTO 1 TABLETS",
      kYsqlTableName));

  const auto table_id = ASSERT_RESULT(GetTableIDFromTableName(kYsqlTableName));
  const auto tablet_id = ASSERT_RESULT(OnlyTabletIdForTable(table_id));

  // Isolate from CREATE TABLE WAL traffic.
  auto tip = ASSERT_RESULT(StreamWalForTablet(tablet_id, kSkipToLatest));
  ASSERT_FALSE(tip.has_error()) << tip.error().ShortDebugString();
  StreamWalCursorPB cursor = tip.next_op_id();

  // Run a multi-row txn so the APPLYING produces many intents.
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  for (int i = 0; i < kRowCount; ++i) {
    ASSERT_OK(conn.ExecuteFormat(
        "INSERT INTO $0 VALUES ($1, 'sku-$1', $1)", kYsqlTableName, i));
  }
  ASSERT_OK(conn.CommitTransaction());

  // Drive StreamWAL to completion, accumulating records and observing at least
  // one spill (cursor with intent_key set).
  std::vector<CDCSDKProtoRecordPB> seen;
  bool saw_spilled_cursor = false;
  std::string spill_txn_id;
  int spill_batches = 0;
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        auto resp = VERIFY_RESULT(StreamWalForTablet(tablet_id, cursor));
        SCHECK(!resp.has_error(), IllegalState, resp.error().ShortDebugString());
        for (const auto& record : resp.records()) {
          seen.push_back(record);
        }

        const auto& next_cursor = resp.next_op_id();
        if (next_cursor.has_intent_key() && next_cursor.has_intent_write_id()) {
          saw_spilled_cursor = true;
          ++spill_batches;
          // While spilled, has_more MUST be true (more intents pending).
          SCHECK(resp.has_more(), IllegalState,
                 "spilled APPLYING must have has_more=true");
          // BEGIN appears only on the first spill batch; subsequent spill
          // batches MUST NOT re-emit BEGIN for the same transaction. Verify
          // this once we have a transaction_id from the records so far.
          for (const auto& record : resp.records()) {
            if (!record.has_row_message()) continue;
            const auto& row = record.row_message();
            if (row.op() == RowMessage_Op_BEGIN && row.has_transaction_id()) {
              if (spill_txn_id.empty()) {
                spill_txn_id = row.transaction_id();
              } else if (row.transaction_id() == spill_txn_id) {
                return STATUS(
                    IllegalState,
                    "BEGIN must be emitted only once per APPLYING across spill batches");
              }
            }
          }
        }

        cursor = next_cursor;

        // Stop once we have observed a COMMIT for the spilled transaction.
        if (!spill_txn_id.empty()) {
          for (const auto& record : seen) {
            if (!record.has_row_message()) continue;
            const auto& row = record.row_message();
            if (row.op() == RowMessage_Op_COMMIT && row.has_transaction_id() &&
                row.transaction_id() == spill_txn_id) {
              return true;
            }
          }
        }
        return false;
      },
      60s * kTimeMultiplier,
      "wait for spilled APPLYING to fully drain across multiple batches"));

  ASSERT_TRUE(saw_spilled_cursor)
      << "expected at least one batch with a partial-APPLYING cursor; spill_batches="
      << spill_batches;
  ASSERT_GE(spill_batches, 1);

  // Now reconcile the full envelope: exactly one BEGIN and one COMMIT for the
  // txn, kRowCount table-row INSERTs, all carrying the same commit_time.
  int begin_count = 0;
  int commit_count = 0;
  int table_insert_count = 0;
  uint64_t commit_time = 0;
  for (const auto& record : seen) {
    if (!record.has_row_message()) continue;
    const auto& row = record.row_message();
    if (!row.has_transaction_id() || row.transaction_id() != spill_txn_id) {
      continue;
    }
    switch (row.op()) {
      case RowMessage_Op_BEGIN:
        ++begin_count;
        commit_time = row.commit_time();
        break;
      case RowMessage_Op_COMMIT:
        ++commit_count;
        ASSERT_EQ(row.commit_time(), commit_time)
            << "COMMIT must share commit_time with BEGIN";
        break;
      case RowMessage_Op_INSERT:
        if (row.table_id() == table_id) {
          ++table_insert_count;
          ASSERT_EQ(row.commit_time(), commit_time)
              << "per-row INSERT must share commit_time with BEGIN/COMMIT";
        }
        break;
      default:
        break;
    }
  }
  ASSERT_EQ(begin_count, 1) << "exactly one BEGIN per APPLYING, even with spills";
  ASSERT_EQ(commit_count, 1) << "exactly one COMMIT per APPLYING, only on final batch";
  ASSERT_EQ(table_insert_count, kRowCount)
      << "every committed row must be emitted exactly once";
}

// Regression test for the checkpoint-less intent-retention fix.
//
// In the StreamWAL design GetLatestCheckPoint() is permanently OpId::Max(), so
// the IntentsDB compaction-filter GC path (DocDBIntentsCompactionFilter ->
// TransactionParticipant::Cleanup -> CleanupAbortsTask) treated CDC as inactive
// and reclaimed a committed transaction's intents as soon as its metadata aged
// past --aborted_intent_cleanup_ms -- bypassing the --intents_min_seconds_to_retain
// wall-clock window entirely.
//
// The fix persists PostApplyTransactionMetadata (carrying commit_ht) at apply and
// gates Cleanup on the wall-clock window, so a committed txn's intents survive an
// IntentsDB compaction while inside the retention window even with
// aborted_intent_cleanup_ms forced to 0 (i.e. the compaction filter surfaces the
// txn on every pass; only the retention window protects the intents).
TEST_F(StreamWalYsqlSecondaryIndexTest, CommittedIntentsSurviveCompactionWithinRetentionWindow) {
  constexpr int kRowCount = 20;
  // Surface the committed txn to the compaction filter immediately, so the
  // 1-hour retention window (set by the fixture) is the *only* thing that can
  // keep the intents from being GC'd.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_aborted_intent_cleanup_ms) = 0;

  constexpr auto kYsqlTableName = "stream_wal_retention_t";
  auto conn = ASSERT_RESULT(Connect());
  // UNIQUE secondary index forces the writes down the transactional path, so the
  // commit produces committed intents (an APPLYING op).
  ASSERT_OK(conn.ExecuteFormat(
      "CREATE TABLE $0 (id INT PRIMARY KEY, sku TEXT UNIQUE, v INT) SPLIT INTO 1 TABLETS",
      kYsqlTableName));
  const auto table_id = ASSERT_RESULT(GetTableIDFromTableName(kYsqlTableName));
  const auto tablet_id = ASSERT_RESULT(OnlyTabletIdForTable(table_id));

  auto tip = ASSERT_RESULT(StreamWalForTablet(tablet_id, kSkipToLatest));
  ASSERT_FALSE(tip.has_error()) << tip.error().ShortDebugString();

  // Explicit multi-row transaction (mirrors MultiRowTxnSpillsAndResumes, which is
  // the reliable committed-intents-at-APPLYING path in this fixture).
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  for (int i = 0; i < kRowCount; ++i) {
    ASSERT_OK(conn.ExecuteFormat("INSERT INTO $0 VALUES ($1, 'sku-$1', $1)", kYsqlTableName, i));
  }
  ASSERT_OK(conn.CommitTransaction());

  // Drain to COMMIT so we know the APPLYING has been processed and the committed
  // transaction has been evicted from the in-memory participant (the regime where
  // the bug bit: LocalCommitTime() can no longer vouch for it). This also forces
  // the server to read the intents at APPLYING, proving they are present.
  ASSERT_RESULT(DrainUntilCommit(tablet_id, tip.next_op_id(), 60s * kTimeMultiplier));

  auto peers = ASSERT_RESULT(ListTabletActivePeers(cluster_.get(), tablet_id));
  ASSERT_FALSE(peers.empty());
  auto tablet = ASSERT_RESULT(peers.front()->shared_tablet());
  ASSERT_GT(ASSERT_RESULT(tablet->CountIntents()), 0)
      << "precondition: committed intents present in IntentsDB";

  // Force IntentsDB flush + compaction repeatedly. With aborted_intent_cleanup_ms
  // == 0 the compaction filter surfaces the committed txn on every pass; pre-fix
  // this fed the blanket-add cleanup path and reclaimed the intents. Post-fix, the
  // wall-clock gate in TransactionParticipant::Cleanup keeps them (commit_ht is
  // inside the 1h retention window).
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(tablet->ForceManualRocksDBCompact());
  }

  ASSERT_GT(ASSERT_RESULT(tablet->CountIntents()), 0)
      << "committed-txn intents were GC'd inside the --intents_min_seconds_to_retain window";
  ASSERT_NO_FATALS(ExpectIntentsPresentAndCheckpointUnpinned(tablet_id));

  // StreamWAL can still replay the committed rows from the original cursor
  // (no INTENTS_GC_ERROR). DrainUntilCommit fails if the server returns an error.
  auto replay =
      ASSERT_RESULT(DrainUntilCommit(tablet_id, tip.next_op_id(), 60s * kTimeMultiplier));
  int insert_count = 0;
  for (const auto& record : replay) {
    if (record.has_row_message() &&
        record.row_message().op() == RowMessage_Op_INSERT &&
        record.row_message().table_id() == table_id) {
      ++insert_count;
    }
  }
  ASSERT_EQ(insert_count, kRowCount)
      << "StreamWAL must still replay every committed row after compaction within the window";
}

}  // namespace cdc
}  // namespace yb
