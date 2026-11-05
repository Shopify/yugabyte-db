// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations
// under the License.

#include "yb/tools/local_runtime/pg_local_readonly.h"

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <limits>
#include <map>
#include <set>

#include <rapidjson/document.h>

#include "yb/common/entity_ids.h"
#include "yb/common/pgsql_error.h"
#include "yb/common/version_info.h"
#include "yb/common/pgsql_protocol.messages.h"
#include "yb/docdb/doc_read_context.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/tablet/tablet_options.h"
#include "yb/common/wire_protocol.h"
#include "yb/dockv/partition.h"
#include "yb/dockv/doc_key.h"
#include "yb/rocksdb/db.h"
#include "yb/rpc/messenger.h"
#include "yb/rpc/proxy.h"
#include "yb/rpc/sidecars.h"
#include "yb/tools/local_runtime/pg_catalog_transaction.h"
#include "yb/tools/local_runtime/pg_local_table.h"
#include "yb/tools/local_runtime/pg_local_request_sequence.h"
#include "yb/rocksdb/iterator.h"
#include "yb/rpc/rpc_controller.h"
#include "yb/tserver/pg_client.messages.h"
#include "yb/tserver/pg_client.proxy.h"
#include "yb/tserver/tserver_admin.pb.h"
#include "yb/util/backoff_waiter.h"
#include "yb/util/env_util.h"
#include "yb/util/flags.h"
#include "yb/util/memory/arena.h"
#include "yb/util/pb_util.h"
#include "yb/util/scope_exit.h"
#include "yb/util/subprocess.h"
#include "yb/util/sync_point.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/test_util.h"
#include "yb/yql/pgwrapper/libpq_utils.h"

DECLARE_int32(ysql_num_shards_per_tserver);
DECLARE_bool(yb_enable_read_committed_isolation);
DECLARE_string(ysql_sequence_cache_method);

namespace yb::local_runtime {
namespace {

Result<tablet::TableInfoPB> TestTable(
    uint32_t database_oid, uint32_t table_oid, uint32_t base_oid = 0) {
  tserver::PgCreateTableRequestPB create;
  create.set_use_transaction(true);
  create.set_use_regular_transaction_block(true);
  create.mutable_table_id()->set_database_oid(database_oid);
  create.mutable_table_id()->set_object_oid(table_oid);
  create.set_database_name(database_oid == 1 ? "template1" : "yugabyte");
  create.set_table_name(base_oid ? "crash_index" : "crash_descriptor");
  if (base_oid) {
    create.mutable_base_table_id()->set_database_oid(database_oid);
    create.mutable_base_table_id()->set_object_oid(base_oid);
    create.set_skip_index_backfill(true);
  }
  create.set_schema_name("public");
  create.set_num_tablets(4);
  auto* column = create.add_create_columns();
  column->set_attr_name("k");
  column->set_attr_num(1);
  column->set_attr_pgoid(20);
  column->set_attr_ybtype(static_cast<int32_t>(DataType::INT64));
  column->set_is_hash(true);
  return BuildLocalTable(create);
}

// Direct sequence RPCs. Local handlers answer in-band, while the unsupported default fails the
// RPC itself; a transport failure therefore maps to RemoteError, which no handler returns.
class SequenceRpc {
 public:
  explicit SequenceRpc(const tserver::PgClientServiceProxy& proxy) : proxy_(proxy) {}

  Status Insert(uint64_t session, int64_t database, int64_t sequence) const {
    auto request = Make<tserver::PgInsertSequenceTupleRequestPB>(session, database, sequence);
    request.set_last_val(1);
    tserver::PgInsertSequenceTupleResponsePB response;
    return Call(&tserver::PgClientServiceProxy::InsertSequenceTuple, request, &response);
  }

  Result<std::pair<int64_t, bool>> Read(
      uint64_t session, int64_t database, int64_t sequence, uint64_t read_time = 0) const {
    auto request = Make<tserver::PgReadSequenceTupleRequestPB>(session, database, sequence);
    request.set_read_time(read_time);
    tserver::PgReadSequenceTupleResponsePB response;
    RETURN_NOT_OK(Call(&tserver::PgClientServiceProxy::ReadSequenceTuple, request, &response));
    return std::make_pair(response.last_val(), response.is_called());
  }

  Status Update(uint64_t session, int64_t database, int64_t sequence) const {
    auto request = Make<tserver::PgUpdateSequenceTupleRequestPB>(session, database, sequence);
    request.set_last_val(500);
    request.set_is_called(true);
    tserver::PgUpdateSequenceTupleResponsePB response;
    return Call(&tserver::PgClientServiceProxy::UpdateSequenceTuple, request, &response);
  }

  // Reserves up to three values of an ascending 1..1000 sequence.
  Result<std::pair<int64_t, int64_t>> Fetch(
      uint64_t session, int64_t database, int64_t sequence) const {
    auto request = Make<tserver::PgFetchSequenceTupleRequestPB>(session, database, sequence);
    request.set_fetch_count(3);
    request.set_inc_by(1);
    request.set_min_value(1);
    request.set_max_value(1000);
    tserver::PgFetchSequenceTupleResponsePB response;
    RETURN_NOT_OK(Call(&tserver::PgClientServiceProxy::FetchSequenceTuple, request, &response));
    return std::make_pair(response.first_value(), response.last_value());
  }

  Status Delete(uint64_t session, int64_t database, int64_t sequence) const {
    const auto request =
        Make<tserver::PgDeleteSequenceTupleRequestPB>(session, database, sequence);
    tserver::PgDeleteSequenceTupleResponsePB response;
    return Call(&tserver::PgClientServiceProxy::DeleteSequenceTuple, request, &response);
  }

  Status DeleteDatabase(uint64_t session, int64_t database) const {
    const auto request = Make<tserver::PgDeleteDBSequencesRequestPB>(session, database, 0);
    tserver::PgDeleteDBSequencesResponsePB response;
    return Call(&tserver::PgClientServiceProxy::DeleteDBSequences, request, &response);
  }

  std::vector<Status> All(uint64_t session, int64_t database, int64_t sequence) const {
    return {
        Insert(session, database, sequence), ResultToStatus(Read(session, database, sequence)),
        Update(session, database, sequence), ResultToStatus(Fetch(session, database, sequence)),
        Delete(session, database, sequence), DeleteDatabase(session, database)};
  }

 private:
  template <class Request>
  static Request Make(uint64_t session, int64_t database, int64_t sequence) {
    Request request;
    request.set_session_id(session);
    request.set_db_oid(database);
    if constexpr (requires { request.set_seq_oid(sequence); }) request.set_seq_oid(sequence);
    return request;
  }

  template <class Request, class Response>
  Status Call(
      Status (tserver::PgClientServiceProxy::*method)(
          const Request&, Response*, rpc::RpcController*) const,
      const Request& request, Response* response) const {
    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(10));
    const auto transport = (proxy_.*method)(request, response, &controller);
    if (!transport.ok()) {
      return STATUS_FORMAT(RemoteError, "Sequence RPC bypassed its local handler: $0", transport);
    }
    return response->has_status() ? StatusFromPB(response->status()) : Status::OK();
  }

  const tserver::PgClientServiceProxy& proxy_;
};

class ReadonlyPgServerTest : public YBTest {
 protected:
  void CheckCommand(bool writable);
  void CheckQueuedPerform(int mode);
  void CheckSessionIsolation(int mode);

  std::string Snapshot() {
    return env_util::GetRootDir("share/initial_sys_catalog_snapshot") +
           "/share/initial_sys_catalog_snapshot";
  }

  Result<pgwrapper::PGConn> Connect(
      const ReadonlyPgServer& server, const std::string& database = "yugabyte") {
    pgwrapper::PGConnSettings settings{
        .host = "127.0.0.1",
        .port = server.SqlPort(),
        .dbname = database,
        .user = "yugabyte",
        .connect_timeout = 20};
    auto connection = VERIFY_RESULT(pgwrapper::PGConnBuilder(settings).Connect());
    RETURN_NOT_OK(server.ValidateConnection(
        VERIFY_RESULT(connection.FetchRow<std::string>("SHOW data_directory"))));
    return connection;
  }
};

void ReadonlyPgServerTest::CheckQueuedPerform(int mode) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("queued-perform"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  rpc::MessengerBuilder builder("queued-test-client");
  builder.UseDefaultConnectionContextFactory();
  auto messenger = ASSERT_RESULT(builder.Build());
  auto shutdown = ScopeExit([&] { messenger->Shutdown(); });
  rpc::ProxyCache cache(messenger.get());
  tserver::PgClientServiceProxy proxy(&cache, server->RpcAddress());
  tserver::PgHeartbeatRequestPB heartbeat;
  heartbeat.set_pid(getpid());
  tserver::PgHeartbeatResponsePB reply;
  rpc::RpcController controller;
  controller.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(proxy.Heartbeat(heartbeat, &reply, &controller));
  ASSERT_FALSE(reply.has_status());
  struct Call {
    ThreadSafeArena arena;
    tserver::LWPgPerformRequestPB request{&arena};
    tserver::LWPgPerformResponsePB response{&arena};
    rpc::RpcController controller;
    std::promise<Status> done;
    std::future<Status> future = done.get_future();
  };
  const auto send = [&](uint64_t serial, std::optional<uint64_t> predecessor) {
    auto call = std::make_shared<Call>();
    call->request.set_session_id(reply.session_id());
    call->request.mutable_sequence_num()->set_serial_no(serial);
    if (predecessor)
      call->request.mutable_sequence_num()->mutable_predecessor_serial_no()->set_value(
          *predecessor);
    call->controller.set_timeout(MonoDelta::FromSeconds(15));
    proxy.PerformAsync(call->request, &call->response, &call->controller, [call] {
      call->done.set_value(call->controller.status());
    });
    return call;
  };
  std::atomic<size_t> queued{0};
  auto* sync = SyncPoint::GetInstance();
  sync->SetCallBack("LocalRuntime::PerformQueued", [&](void*) { ++queued; });
  sync->EnableProcessing();
  auto cleanup = ScopeExit([&] {
    server.reset();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  auto later = send(1, 0);
  ASSERT_OK(WaitFor([&] { return queued.load() == 1; }, 5s, "request parked"));
  if (mode == 2) {
    std::vector<std::shared_ptr<Call>> calls{later};
    for (uint64_t i = 2; i <= 64; ++i) calls.push_back(send(i, i - 1));
    ASSERT_OK(WaitFor([&] { return queued.load() == 64; }, 5s, "request queue full"));
    calls.push_back(send(65, 64));
    for (auto& call : calls) {
      ASSERT_EQ(call->future.wait_for(10s), std::future_status::ready);
      ASSERT_OK(call->future.get());
      ASSERT_TRUE(call->response.has_status());
    }
    return;
  }
  if (mode == 1) {
    ASSERT_EQ(later->future.wait_for(10s), std::future_status::ready);
    ASSERT_OK(later->future.get());
    ASSERT_TRUE(StatusFromPB(later->response.status()).IsTimedOut());
    auto missing = send(0, std::nullopt);
    ASSERT_EQ(missing->future.wait_for(5s), std::future_status::ready);
    ASSERT_OK(missing->future.get());
    ASSERT_TRUE(missing->response.has_status());
    return;
  }
  auto earlier = send(0, std::nullopt);
  for (auto call : {earlier, later}) {
    ASSERT_EQ(call->future.wait_for(5s), std::future_status::ready);
    ASSERT_OK(call->future.get());
    ASSERT_FALSE(call->response.has_status());
  }
  auto pending = send(3, 2);
  ASSERT_OK(WaitFor([&] { return queued.load() == 2; }, 5s, "shutdown request parked"));
  server.reset();
  ASSERT_EQ(pending->future.wait_for(5s), std::future_status::ready);
  ASSERT_OK(pending->future.get());
  ASSERT_TRUE(pending->response.has_status());
}

TEST_F(ReadonlyPgServerTest, QueuedPerformAndShutdown) { CheckQueuedPerform(0); }
TEST_F(ReadonlyPgServerTest, MissingPredecessorFencesSession) { CheckQueuedPerform(1); }
TEST_F(ReadonlyPgServerTest, RequestQueueBound) { CheckQueuedPerform(2); }

void ReadonlyPgServerTest::CheckSessionIsolation(int mode) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("session-isolation"), Snapshot(), AllocateFreePort(),
          mode >= 5 ? CatalogMode::kSerializedTransactions : CatalogMode::kFrozen));
  rpc::MessengerBuilder builder("session-test-client");
  builder.UseDefaultConnectionContextFactory();
  auto messenger = ASSERT_RESULT(builder.Build());
  auto shutdown = ScopeExit([&] { messenger->Shutdown(); });
  rpc::ProxyCache cache(messenger.get());
  tserver::PgClientServiceProxy proxy(&cache, server->RpcAddress());
  std::unique_ptr<Subprocess> backend;
  auto stop_backend = ScopeExit([&] {
    if (backend && backend->IsRunning()) {
      WARN_NOT_OK(backend->KillNoCheckIfRunning(SIGKILL), "Stopping synthetic backend");
      int code;
      WARN_NOT_OK(backend->Wait(&code), "Reaping synthetic backend");
    }
  });
  if (mode >= 4) {
    backend =
        std::make_unique<Subprocess>("/bin/sleep", std::vector<std::string>{"/bin/sleep", "60"});
    ASSERT_OK(backend->Start());
  }
  const auto connect = [&](pid_t pid) -> Result<uint64_t> {
    tserver::PgHeartbeatRequestPB request;
    request.set_pid(pid);
    tserver::PgHeartbeatResponsePB response;
    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy.Heartbeat(request, &response, &controller));
    if (response.has_status()) return StatusFromPB(response.status());
    return response.session_id();
  };
  struct Call {
    std::string payload;
    ThreadSafeArena arena;
    tserver::LWPgPerformRequestPB request{&arena};
    tserver::LWPgPerformResponsePB response{&arena};
    rpc::RpcController controller;
    std::promise<Status> done;
    std::future<Status> future = done.get_future();
  };
  const auto send = [&](uint64_t session, uint64_t serial, std::optional<uint64_t> predecessor,
                        size_t bytes = 0) {
    auto call = std::make_shared<Call>();
    call->request.set_session_id(session);
    call->request.mutable_sequence_num()->set_serial_no(serial);
    if (mode == 6) {
      auto* options = call->request.mutable_options();
      options->set_sql_transaction_active(true);
      options->set_is_using_table_locks(true);
      options->set_txn_serial_no(7);
      options->set_active_sub_transaction_id(1);
    }
    if (predecessor) {
      call->request.mutable_sequence_num()->mutable_predecessor_serial_no()->set_value(
          *predecessor);
    }
    if (bytes) {
      call->payload.assign(bytes, 'x');
      call->request.add_ops()->mutable_read()->ref_table_id(call->payload);
    }
    call->controller.set_timeout(MonoDelta::FromSeconds(15));
    proxy.PerformAsync(call->request, &call->response, &call->controller, [call] {
      call->done.set_value(call->controller.status());
    });
    return call;
  };
  const auto wait = [](const std::shared_ptr<Call>& call) -> Status {
    SCHECK(call->future.wait_for(10s) == std::future_status::ready, TimedOut, "RPC did not finish");
    RETURN_NOT_OK(call->future.get());
    return call->response.has_status() ? StatusFromPB(call->response.status()) : Status::OK();
  };
  std::atomic<size_t> queued{0};
  auto* sync = SyncPoint::GetInstance();
  sync->SetCallBack("LocalRuntime::PerformQueued", [&](void*) { ++queued; });
  sync->EnableProcessing();
  auto cleanup = ScopeExit([&] {
    server.reset();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  const auto first = ASSERT_RESULT(connect(backend ? backend->pid() : getpid()));
  TableId abandoned;
  if (mode >= 5) {
    tserver::PgCreateTableRequestPB create;
    create.set_session_id(first);
    create.set_use_transaction(true);
    create.set_use_regular_transaction_block(true);
    create.mutable_table_id()->set_database_oid(1);
    create.mutable_table_id()->set_object_oid(1000012345);
    create.set_database_name("template1");
    create.set_table_name("abandoned_table");
    create.set_schema_name("public");
    create.set_num_tablets(4);
    auto* column = create.add_create_columns();
    column->set_attr_name("k");
    column->set_attr_num(1);
    column->set_attr_pgoid(20);
    column->set_attr_ybtype(static_cast<int32_t>(DataType::INT64));
    column->set_is_hash(true);
    auto* options = create.mutable_options();
    options->set_ddl_mode(true);
    options->set_ddl_use_regular_transaction_block(true);
    options->set_is_using_table_locks(true);
    options->set_sql_transaction_active(true);
    options->set_txn_serial_no(7);
    options->set_active_sub_transaction_id(1);
    abandoned = ASSERT_RESULT(BuildLocalTable(create)).table_id();
    tserver::PgCreateTableResponsePB response;
    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy.CreateTable(create, &response, &controller));
    ASSERT_FALSE(response.has_status()) << response.ShortDebugString();
    if (mode >= 6) {
      std::promise<Status> killed;
      auto killed_future = killed.get_future();
      std::atomic<bool> injected{false};
      // Join handlers before destroying callback captures, including on assertion failure.
      auto stop_race = ScopeExit([&] {
        server.reset();
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
      });
      sync->SetCallBack("LocalRuntime::BeforeSqlOwnerCheck", [&, first](void* arg) {
        if (*static_cast<uint64_t*>(arg) != first || injected.exchange(true)) return;
        killed.set_value([&]() -> Status {
          RETURN_NOT_OK(backend->Kill(SIGKILL));
          int code;
          RETURN_NOT_OK(backend->Wait(&code));
          SCHECK(
              WIFSIGNALED(code) && WTERMSIG(code) == SIGKILL, IllegalState,
              "Synthetic backend did not die from SIGKILL");
          return Status::OK();
        }());
      });
      std::vector<TableId> abandoned_tables{abandoned};
      if (mode == 6) {
        ASSERT_TRUE(wait(send(first, 0, std::nullopt)).IsAborted());
      } else {
        create.mutable_table_id()->set_object_oid(1000012346);
        create.set_table_name("interrupted_table");
        abandoned_tables.push_back(ASSERT_RESULT(BuildLocalTable(create)).table_id());
        response.Clear();
        controller.Reset();
        controller.set_timeout(MonoDelta::FromSeconds(10));
        ASSERT_OK(proxy.CreateTable(create, &response, &controller));
        ASSERT_TRUE(response.has_status());
        ASSERT_TRUE(StatusFromPB(response.status()).IsAborted());
      }
      ASSERT_EQ(killed_future.wait_for(5s), std::future_status::ready);
      ASSERT_OK(killed_future.get());
      auto replacement = ASSERT_RESULT(Connect(*server));
      ASSERT_OK(replacement.Execute("CREATE TABLE recovered_rows(k bigint PRIMARY KEY)"));
      ASSERT_OK(replacement.Execute("INSERT INTO recovered_rows VALUES (42)"));
      ASSERT_EQ(ASSERT_RESULT(replacement.FetchRow<int64_t>("SELECT k FROM recovered_rows")), 42);
      for (const auto& table_id : abandoned_tables) {
        tserver::PgOpenTableRequestPB open;
        open.set_table_id(table_id);
        tserver::PgOpenTableResponsePB opened;
        controller.Reset();
        controller.set_timeout(MonoDelta::FromSeconds(10));
        ASSERT_OK(proxy.OpenTable(open, &opened, &controller));
        ASSERT_TRUE(opened.has_status());
        ASSERT_TRUE(StatusFromPB(opened.status()).IsNotFound());
        ASSERT_FALSE(opened.has_info());
        ASSERT_FALSE(opened.has_partitions());
      }
      return;
    }
    const auto contender = ASSERT_RESULT(connect(getpid()));
    for (const auto id : {uint64_t(0), contender, first}) {
      tserver::PgOpenTableRequestPB open;
      open.set_table_id(abandoned);
      open.set_session_id(id);
      tserver::PgOpenTableResponsePB opened;
      controller.Reset();
      controller.set_timeout(MonoDelta::FromSeconds(10));
      ASSERT_OK(proxy.OpenTable(open, &opened, &controller));
      if (id == first) {
        ASSERT_FALSE(opened.has_status());
        ASSERT_EQ(opened.info().identifier().table_id(), abandoned);
      } else {
        ASSERT_TRUE(opened.has_status());
        ASSERT_TRUE(StatusFromPB(opened.status()).IsTryAgain());
        ASSERT_FALSE(opened.has_info());
        ASSERT_FALSE(opened.has_partitions());
      }
      tserver::PgGetTablePartitionListRequestPB partitions;
      partitions.set_table_id(abandoned);
      partitions.set_session_id(id);
      tserver::PgGetTablePartitionListResponsePB partitioned;
      controller.Reset();
      controller.set_timeout(MonoDelta::FromSeconds(10));
      ASSERT_OK(proxy.GetTablePartitionList(partitions, &partitioned, &controller));
      tserver::PgGetDatabaseInfoRequestPB database;
      database.set_oid(1);
      database.set_session_id(id);
      tserver::PgGetDatabaseInfoResponsePB database_info;
      controller.Reset();
      controller.set_timeout(MonoDelta::FromSeconds(10));
      ASSERT_OK(proxy.GetDatabaseInfo(database, &database_info, &controller));
      if (id == first) {
        ASSERT_FALSE(partitioned.has_status());
        ASSERT_TRUE(partitioned.has_partitions());
        ASSERT_EQ(
            partitioned.partitions().SerializeAsString(), opened.partitions().SerializeAsString());
        ASSERT_FALSE(database_info.has_status());
        ASSERT_EQ(database_info.info().namespace_().id(), GetPgsqlNamespaceId(1));
      } else {
        ASSERT_TRUE(partitioned.has_status());
        ASSERT_TRUE(StatusFromPB(partitioned.status()).IsTryAgain());
        ASSERT_FALSE(partitioned.has_partitions());
        ASSERT_TRUE(database_info.has_status());
        ASSERT_TRUE(StatusFromPB(database_info.status()).IsTryAgain());
        ASSERT_FALSE(database_info.has_info());
      }
    }
  }
  auto pending = send(first, 1, 0, mode == 3 ? 8 * 1024 * 1024 : 0);
  ASSERT_OK(WaitFor([&] { return queued.load() == 1; }, 5s, "first session parked"));
  if (mode == 5) {
    ASSERT_OK(backend->Kill(SIGKILL));
    int code;
    ASSERT_OK(backend->Wait(&code));
    ASSERT_TRUE(WIFSIGNALED(code));
    ASSERT_EQ(WTERMSIG(code), SIGKILL);
    auto replacement = ASSERT_RESULT(Connect(*server));
    ASSERT_TRUE(wait(pending).IsAborted());
    tserver::PgOpenTableRequestPB open;
    open.set_table_id(abandoned);
    tserver::PgOpenTableResponsePB opened;
    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy.OpenTable(open, &opened, &controller));
    ASSERT_TRUE(opened.has_status());
    ASSERT_TRUE(StatusFromPB(opened.status()).IsNotFound());
    ASSERT_OK(replacement.Execute("BEGIN"));
    ASSERT_OK(replacement.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'replacement owner'"));
    tserver::PgFinishTransactionRequestPB finish;
    finish.set_session_id(first);
    finish.set_commit(false);
    tserver::PgFinishTransactionResponsePB finished;
    controller.Reset();
    controller.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy.FinishTransaction(finish, &finished, &controller));
    ASSERT_TRUE(finished.has_status());
    ASSERT_EQ(
        ASSERT_RESULT(replacement.FetchRow<std::string>(
            "SELECT obj_description('pg_catalog.pg_class'::regclass)")),
        "replacement owner");
    ASSERT_OK(replacement.Execute("COMMIT"));
    return;
  }
  const auto second = ASSERT_RESULT(connect(getpid()));
  if (mode == 2) {
    std::vector<std::shared_ptr<Call>> calls{pending};
    for (uint64_t i = 2; i <= 32; ++i) calls.push_back(send(first, i, i - 1));
    for (uint64_t i = 1; i <= 32; ++i) calls.push_back(send(second, i, i - 1));
    ASSERT_OK(WaitFor([&] { return queued.load() == 64; }, 5s, "aggregate queue full"));
    const auto third = ASSERT_RESULT(connect(getpid()));
    ASSERT_TRUE(wait(send(third, 1, 0)).IsServiceUnavailable());
    ASSERT_EQ(pending->future.wait_for(20ms), std::future_status::timeout);
    ASSERT_OK(wait(send(first, 0, std::nullopt)));
    ASSERT_OK(wait(send(second, 0, std::nullopt)));
    for (const auto& call : calls) ASSERT_OK(wait(call));
    const auto fourth = ASSERT_RESULT(connect(getpid()));
    auto next = send(fourth, 1, 0);
    ASSERT_OK(WaitFor([&] { return queued.load() == 65; }, 5s, "released queue capacity"));
    ASSERT_OK(wait(send(fourth, 0, std::nullopt)));
    ASSERT_OK(wait(next));
    return;
  }
  if (mode == 3) {
    ASSERT_TRUE(wait(send(second, 1, 0, 8 * 1024 * 1024)).IsServiceUnavailable());
    ASSERT_EQ(pending->future.wait_for(20ms), std::future_status::timeout);
    server.reset();
    ASSERT_TRUE(wait(pending).IsServiceUnavailable());
    return;
  }
  ASSERT_OK(wait(send(second, 0, std::nullopt)));
  ASSERT_EQ(pending->future.wait_for(20ms), std::future_status::timeout);
  if (mode == 1) {
    ASSERT_TRUE(wait(pending).IsTimedOut());
    ASSERT_NOK(wait(send(first, 0, std::nullopt)));
  } else if (mode == 4) {
    ASSERT_OK(backend->Kill(SIGKILL));
    int code;
    ASSERT_OK(backend->Wait(&code));
    ASSERT_TRUE(WIFSIGNALED(code));
    ASSERT_EQ(WTERMSIG(code), SIGKILL);
    ASSERT_TRUE(wait(pending).IsAborted());
    const auto replacement = ASSERT_RESULT(connect(getpid()));
    ASSERT_OK(wait(send(replacement, 0, std::nullopt)));
    ASSERT_NOK(wait(send(first, 0, std::nullopt)));
  } else {
    ASSERT_OK(wait(send(first, 0, std::nullopt)));
    ASSERT_OK(wait(pending));
    ASSERT_NOK(wait(send(first, 0, std::nullopt)));
    ASSERT_NOK(wait(send(first, 2, 1)));
  }
  ASSERT_OK(wait(send(second, 1, 0)));
}

TEST_F(ReadonlyPgServerTest, SessionRequestIsolation) { CheckSessionIsolation(0); }
TEST_F(ReadonlyPgServerTest, SessionDeadlineIsolation) { CheckSessionIsolation(1); }
TEST_F(ReadonlyPgServerTest, AggregateSessionQueueBound) { CheckSessionIsolation(2); }
TEST_F(ReadonlyPgServerTest, AggregateSessionByteBound) { CheckSessionIsolation(3); }
TEST_F(ReadonlyPgServerTest, DeadSessionQueueCleanup) { CheckSessionIsolation(4); }
TEST_F(ReadonlyPgServerTest, CatalogOwnerReplacement) { CheckSessionIsolation(5); }
TEST_F(ReadonlyPgServerTest, OwnerDiesDuringPerform) { CheckSessionIsolation(6); }
TEST_F(ReadonlyPgServerTest, OwnerDiesDuringCreateTable) { CheckSessionIsolation(7); }

TEST_F(ReadonlyPgServerTest, CatalogReloadFailureIsFailStop) {
  for (int mode = 0; mode < 2; ++mode) {
    const auto path = GetTestPath("reload-failure-" + std::to_string(mode));
    const auto port = AllocateFreePort();
    auto server = ASSERT_RESULT(
        ReadonlyPgServer::Start(path, Snapshot(), port, CatalogMode::kSerializedTransactions));
    {
      auto c = ASSERT_RESULT(Connect(*server));
      ASSERT_OK(c.Execute("CREATE TABLE cache_rows(k bigint PRIMARY KEY, v bigint)"));
      ASSERT_OK(c.Execute("INSERT INTO cache_rows VALUES(1,10)"));
      ASSERT_OK(c.Execute("BEGIN"));
      ASSERT_OK(c.Execute("UPDATE cache_rows SET v=20 WHERE k=1"));
      // Only catalog transactions rebuild descriptors on rollback; DDL adopts the data write.
      ASSERT_OK(c.Execute("ALTER TABLE cache_rows ADD COLUMN extra bigint"));
      ASSERT_OK(c.Execute("SAVEPOINT s"));
      ASSERT_OK(c.Execute("UPDATE cache_rows SET v=30 WHERE k=1"));
      auto* sync = SyncPoint::GetInstance();
      std::atomic<size_t> failures{0};
      auto cleanup = ScopeExit([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
      });
      sync->SetCallBack("LocalRuntime::ReloadStatus", [&](void* status) {
        if (failures.fetch_add(1) == 0) {
          *static_cast<Status*>(status) = STATUS(IOError, "Injected descriptor rebuild failure");
        }
      });
      sync->EnableProcessing();
      ASSERT_NOK(c.Execute(mode == 0 ? "ROLLBACK TO s" : "ROLLBACK"));
      ASSERT_EQ(failures.load(), 1);
      sync->DisableProcessing();
      // A later abort/retirement must not turn a failed metadata rebuild into a healthy cache.
      static_cast<void>(c.Execute("ROLLBACK"));
      rpc::MessengerBuilder builder("reload-fence-client");
      builder.UseDefaultConnectionContextFactory();
      auto messenger = ASSERT_RESULT(builder.Build());
      auto shutdown = ScopeExit([&] { messenger->Shutdown(); });
      rpc::ProxyCache cache(messenger.get());
      tserver::PgClientServiceProxy proxy(&cache, server->RpcAddress());
      for (size_t i = 0; i < 3; ++i) {
        rpc::RpcController controller;
        controller.set_timeout(MonoDelta::FromSeconds(10));
        tserver::PgOpenTableRequestPB open;
        open.set_table_id(GetPgsqlTableId(1, 1259));
        tserver::PgOpenTableResponsePB opened;
        ASSERT_OK(proxy.OpenTable(open, &opened, &controller));
        ASSERT_TRUE(opened.has_status());
        ASSERT_TRUE(StatusFromPB(opened.status()).IsIOError());
        ASSERT_FALSE(opened.has_info());
        ASSERT_FALSE(opened.has_partitions());
        controller.Reset();
        controller.set_timeout(MonoDelta::FromSeconds(10));
        tserver::PgGetDatabaseInfoRequestPB database;
        database.set_oid(1);
        tserver::PgGetDatabaseInfoResponsePB database_info;
        ASSERT_OK(proxy.GetDatabaseInfo(database, &database_info, &controller));
        ASSERT_TRUE(database_info.has_status());
        ASSERT_TRUE(StatusFromPB(database_info.status()).IsIOError());
        ASSERT_FALSE(database_info.has_info());
        controller.Reset();
        controller.set_timeout(MonoDelta::FromSeconds(10));
        tserver::PgGetNewObjectIdRequestPB oid;
        oid.set_db_oid(1);
        tserver::PgGetNewObjectIdResponsePB allocated;
        ASSERT_OK(proxy.GetNewObjectId(oid, &allocated, &controller));
        ASSERT_TRUE(allocated.has_status());
        ASSERT_TRUE(StatusFromPB(allocated.status()).IsIOError());
        ASSERT_EQ(allocated.new_oid(), 0);
      }
    }
    server.reset();
    server = ASSERT_RESULT(
        ReadonlyPgServer::Start(path, Snapshot(), port, CatalogMode::kSerializedTransactions));
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT v FROM cache_rows WHERE k=1")), 10);
    ASSERT_OK(c.Execute("UPDATE cache_rows SET v=11 WHERE k=1"));
  }
}

TEST_F(ReadonlyPgServerTest, ConcurrentFrozenSessions) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(GetTestPath("frozen-sessions"), Snapshot(), AllocateFreePort()));
  std::vector<pgwrapper::PGConn> connections;
  for (size_t i = 0; i < 3; ++i) connections.push_back(ASSERT_RESULT(Connect(*server)));
  const auto expected =
      ASSERT_RESULT(connections[0].FetchRow<int64_t>("SELECT count(*) FROM pg_class"));
  TestThreadHolder threads;
  for (auto& connection : connections) {
    threads.AddThreadFunctor([&connection, expected] {
      for (size_t i = 0; i < 20; ++i) {
        ASSERT_OK(connection.Execute("BEGIN ISOLATION LEVEL REPEATABLE READ"));
        ASSERT_EQ(
            ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM pg_class")), expected);
        ASSERT_OK(connection.Execute("COMMIT"));
      }
    });
  }
  threads.JoinAll();
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_OK(connections[0].Execute("BEGIN"));
  ASSERT_OK(connections[0].Execute("SAVEPOINT s"));
  const auto error = connections[0].Execute("SELECT * FROM absent_relation");
  ASSERT_EQ(PgsqlError(error), YBPgErrorCode::YB_PG_UNDEFINED_TABLE) << error;
  ASSERT_EQ(
      ASSERT_RESULT(connections[1].FetchRow<int64_t>("SELECT count(*) FROM pg_class")), expected);
  ASSERT_OK(connections[0].Execute("ROLLBACK TO s"));
  ASSERT_EQ(
      ASSERT_RESULT(connections[0].FetchRow<int64_t>("SELECT count(*) FROM pg_class")), expected);
  ASSERT_OK(connections[0].Execute("COMMIT"));
}

TEST_F(ReadonlyPgServerTest, RequestSequenceFence) {
  LocalRequestSequence sequence;
  ASSERT_OK(sequence.Register(0, std::nullopt));
  ASSERT_NOK(sequence.Register(0, std::nullopt));
  ASSERT_OK(sequence.Register(1, 0));
  ASSERT_NOK(sequence.Register(3, 2));
  ASSERT_OK(sequence.Register(2, 1));
  ASSERT_NOK(sequence.Register(3, std::nullopt));
  for (uint64_t i = 3; i < 20000; ++i) ASSERT_OK(sequence.Register(i, i - 1));
  ASSERT_NOK(sequence.Register(1, 0));
  ASSERT_NOK(sequence.Register(20000, 0));
  ASSERT_OK(sequence.Register(20001, 19999));
  sequence.Reset();
  ASSERT_OK(sequence.Register(0, std::nullopt));
}

TEST_F(ReadonlyPgServerTest, LongSqlSession) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("long-session"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  for (int i = 0; i < 240; ++i) {
    ASSERT_OK(c.ExecuteFormat(
        "CREATE TABLE relation_$0(k bigint PRIMARY KEY,v bigint DEFAULT 7,s text DEFAULT 'x')", i));
  }
  ASSERT_OK(c.Execute("INSERT INTO relation_239 VALUES(1,42,'last')"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT v FROM relation_239")), 42);
  const auto columns = ASSERT_RESULT(c.FetchRows<std::string>(
      "SELECT json_build_array(c.relname,a.attname,pg_get_expr(d.adbin,d.adrelid))::text "
      "FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
      "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_type t ON t.oid=a.atttypid "
      "LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum "
      "WHERE c.relkind='r' AND n.nspname='public' AND a.attnum>0 AND NOT a.attisdropped"));
  ASSERT_EQ(columns.size(), 720);
}

void ReadonlyPgServerTest::CheckCommand(bool writable) {
  const auto port = AllocateFreePort();
  const auto directory = GetTestPath("readonly-cli");
  const auto binary = env_util::GetRootDir("bin") + "/bin/yb-local-readonly";
  auto launch = [&](const std::string& path) -> Result<std::unique_ptr<Subprocess>> {
    auto process = std::make_unique<Subprocess>(
        binary, std::vector<std::string>{
                    binary, "--data_dir=" + path, "--sql_port=" + std::to_string(port),
                    std::string("--experimental_catalog_writes=") + (writable ? "true" : "false")});
    process->PipeParentStdout();
    process->ShareParentStderr();
    RETURN_NOT_OK(process->Start());
    const int fd = process->from_child_stdout_fd();
    SCHECK_EQ(
        fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK), 0, IOError,
        "Cannot configure command output");
    return process;
  };
  auto wait = [](Subprocess& process) -> Result<int> {
    int code = 0;
    RETURN_NOT_OK(WaitFor(
        [&]() -> Result<bool> {
          auto status = process.WaitNoBlock(&code);
          if (status.IsTimedOut()) return false;
          RETURN_NOT_OK(status);
          return true;
        },
        20s, "waiting for local command exit"));
    return code;
  };
  auto stop = [&](Subprocess& process) {
    if (!process.IsRunning()) return;
    WARN_NOT_OK(process.KillNoCheckIfRunning(SIGTERM), "Stopping local command");
    if (!wait(process).ok()) {
      WARN_NOT_OK(process.KillNoCheckIfRunning(SIGKILL), "Killing stuck local command");
      WARN_NOT_OK(process.Wait(), "Reaping local command");
    }
  };
  auto server = ASSERT_RESULT(launch(directory));
  auto cleanup_server = ScopeExit([&] { stop(*server); });
  std::string output;
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        char buffer[4096];
        const auto count = read(server->from_child_stdout_fd(), buffer, sizeof(buffer));
        if (count < 0 && (errno == EAGAIN || errno == EINTR)) return false;
        SCHECK_GT(count, 0, IOError, "Command exited without readiness");
        output.append(buffer, count);
        SCHECK_LE(output.size(), sizeof(buffer), Corruption, "Oversized readiness output");
        return output.find('\n') != std::string::npos;
      },
      30s, "waiting for standalone SQL readiness"));
  rapidjson::Document ready;
  ready.Parse(output.c_str());
  ASSERT_FALSE(ready.HasParseError()) << output;
  ASSERT_TRUE(ready.IsObject() && ready.HasMember("state") && ready["state"].IsString());
  ASSERT_STREQ(ready["state"].GetString(), "ready");
  ASSERT_TRUE(ready.HasMember("sql_port") && ready["sql_port"].IsInt());
  ASSERT_EQ(ready["sql_port"].GetInt(), port);
  ASSERT_TRUE(ready.HasMember("protocol_version") && ready["protocol_version"].IsInt());
  ASSERT_EQ(ready["protocol_version"].GetInt(), 1);
  ASSERT_TRUE(ready.HasMember("mode") && ready["mode"].IsString());
  ASSERT_STREQ(ready["mode"].GetString(), writable ? "experimental-serialized-sql" : "readonly");
  ASSERT_TRUE(ready.HasMember("pid") && ready["pid"].IsInt());
  ASSERT_EQ(ready["pid"].GetInt(), server->pid());
  ASSERT_TRUE(ready.HasMember("data_directory") && ready["data_directory"].IsString());
  const std::string ready_directory = ready["data_directory"].GetString();
  pgwrapper::PGConnSettings settings{
      .host = "127.0.0.1",
      .port = port,
      .dbname = "yugabyte",
      .user = "yugabyte",
      .connect_timeout = 20};
  auto connection = ASSERT_RESULT(pgwrapper::PGConnBuilder(settings).Connect());
  ASSERT_EQ(
      ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT yb_hash_code(42::bigint)::bigint")),
      48975);
  const auto connected_directory =
      ASSERT_RESULT(connection.FetchRow<std::string>("SHOW data_directory"));
  std::error_code error;
  ASSERT_TRUE(std::filesystem::equivalent(connected_directory, directory + "/pg_data", error));
  ASSERT_FALSE(error);
  ASSERT_TRUE(std::filesystem::equivalent(ready_directory, connected_directory, error));
  ASSERT_FALSE(error);
  if (writable) {
    const std::string query = "SELECT obj_description('pg_catalog.pg_class'::regclass)";
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'cli committed'"));
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'cli pending'"));
    ASSERT_OK(connection.Execute("SAVEPOINT s"));
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'cli nested'"));
    ASSERT_OK(connection.Execute("ROLLBACK TO s"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "cli pending");
    ASSERT_OK(connection.Execute("ROLLBACK"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "cli committed");
    ASSERT_OK(connection.Execute(
        "CREATE TABLE cli_rows(k bigint PRIMARY KEY, v bigint) SPLIT INTO 4 TABLETS"));
    ASSERT_OK(connection.Execute("INSERT INTO cli_rows VALUES(1,42)"));
    ASSERT_OK(connection.Execute("CREATE UNIQUE INDEX cli_value_idx ON cli_rows(v)"));
    ASSERT_OK(connection.Execute("CREATE DATABASE cli_database"));
    ASSERT_EQ(
        ASSERT_RESULT(connection.FetchRow<int64_t>(
            "SELECT count(*) FROM pg_database WHERE datname='cli_database'")),
        1);
    ASSERT_NOK(connection.Execute("INSERT INTO cli_rows VALUES(2,42)"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM cli_rows")), 1);
    ASSERT_OK(connection.Execute("SET enable_seqscan=off"));
    ASSERT_TRUE(ASSERT_RESULT(connection.HasIndexScan("SELECT k FROM cli_rows WHERE v=42")));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT v FROM cli_rows WHERE k=1")), 42);
    ASSERT_EQ(
        ASSERT_RESULT(connection.FetchRow<std::string>("SHOW yb_enable_alter_table_rewrite")),
        "on");
    ASSERT_OK(connection.Execute("ALTER TABLE cli_rows ADD COLUMN enabled boolean DEFAULT false"));
    ASSERT_FALSE(ASSERT_RESULT(connection.FetchRow<bool>("SELECT enabled FROM cli_rows")));
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute(
        "ALTER TABLE cli_rows DROP CONSTRAINT cli_rows_pkey; "
        "ALTER TABLE cli_rows ADD PRIMARY KEY((v) HASH,k ASC)"));
    ASSERT_OK(connection.Execute("COMMIT"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT v FROM cli_rows WHERE k=1")), 42);
    std::string notices;
    connection.SetNoticeProcessor(
        [](void* arg, const char* message) { static_cast<std::string*>(arg)->append(message); },
        &notices);
    auto clear_notices =
        ScopeExit([&] { connection.SetNoticeProcessor([](void*, const char*) {}, nullptr); });
    ASSERT_OK(connection.Execute("DROP DATABASE cli_database"));
    ASSERT_OK(connection.Execute("CREATE DATABASE cli_database"));
    connection.SetNoticeProcessor([](void*, const char*) {}, nullptr);
    ASSERT_TRUE(notices.empty()) << notices;
  } else {
    ASSERT_OK(connection.Execute("BEGIN"));
    const auto missing = connection.Execute("SELECT * FROM absent_relation");
    ASSERT_EQ(PgsqlError(missing), YBPgErrorCode::YB_PG_UNDEFINED_TABLE) << missing;
    ASSERT_OK(connection.Execute("ROLLBACK"));
    ASSERT_GT(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM pg_class")), 0);
    auto conflict = ASSERT_RESULT(launch(GetTestPath("conflicting-cli")));
    auto cleanup_conflict = ScopeExit([&] { stop(*conflict); });
    const auto conflict_stdout = conflict->from_child_stdout_fd();
    ASSERT_NE(ASSERT_RESULT(wait(*conflict)), 0);
    char unexpected;
    ASSERT_EQ(read(conflict_stdout, &unexpected, 1), 0);
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT 42::bigint")), 42);
  }
  ASSERT_OK(server->Kill(SIGTERM));
  const auto code = ASSERT_RESULT(wait(*server));
  ASSERT_TRUE(WIFEXITED(code));
  ASSERT_EQ(WEXITSTATUS(code), 0);
}

TEST_F(ReadonlyPgServerTest, CommandLifecycle) { CheckCommand(false); }

TEST_F(ReadonlyPgServerTest, CommandCatalogTransactions) { CheckCommand(true); }

TEST_F(ReadonlyPgServerTest, OidReservationsSurviveRestartAndFailure) {
  const auto path = GetTestPath("oid-reservations");
  auto store = ASSERT_RESULT(PgCatalogTransaction::Open(path, Snapshot() + "/rocksdb"));
  const auto first = ASSERT_RESULT(store->AllocateOid(1));
  ASSERT_EQ(ASSERT_RESULT(store->AllocateOid(1)), first + 1);
  store.reset();
  store = ASSERT_RESULT(PgCatalogTransaction::Open(path, Snapshot() + "/rocksdb"));
  const auto restarted = ASSERT_RESULT(store->AllocateOid(1));
  ASSERT_GT(restarted, first + 1);
  auto* sync = SyncPoint::GetInstance();
  sync->SetCallBack("PgCatalogTransaction::OidReservationStatus", [](void* status) {
    *static_cast<Status*>(status) = STATUS(IOError, "Ambiguous OID reservation");
  });
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->EnableProcessing();
  ASSERT_TRUE(ResultToStatus(store->AllocateOid(4)).IsIOError());
  ASSERT_TRUE(ResultToStatus(store->AllocateOid(1)).IsIOError());
  store->Abort();
  ASSERT_TRUE(ResultToStatus(store->AllocateOid(1)).IsIOError());
  ASSERT_TRUE(ResultToStatus(store->ReadDB()).IsIOError());
  sync->DisableProcessing();
  store.reset();
  store = ASSERT_RESULT(PgCatalogTransaction::Open(path, Snapshot() + "/rocksdb"));
  ASSERT_GT(ASSERT_RESULT(store->AllocateOid(1)), restarted);
  ASSERT_GT(ASSERT_RESULT(store->AllocateOid(4)), first);
}

TEST_F(ReadonlyPgServerTest, CatalogCommitFailureIsFailStop) {
  const auto path = GetTestPath("failed-catalog");
  auto transaction = ASSERT_RESULT(PgCatalogTransaction::Open(path, Snapshot() + "/rocksdb"));
  const auto base_oid = ASSERT_RESULT(transaction->AllocateOid(1));
  const auto descriptor = ASSERT_RESULT(TestTable(1, base_oid));
  const auto index_descriptor =
      ASSERT_RESULT(TestTable(1, ASSERT_RESULT(transaction->AllocateOid(1)), base_oid));
  ASSERT_OK(transaction->Begin());
  auto* sync = SyncPoint::GetInstance();
  sync->SetCallBack("PgCatalogTransaction::CommitStatus", [](void* status) {
    *static_cast<Status*>(status) = STATUS(IOError, "Simulated ambiguous commit failure");
  });
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  ASSERT_OK(transaction->CreateTable(descriptor, 1));
  ASSERT_OK(transaction->CreateTable(index_descriptor, 1));
  sync->EnableProcessing();
  ASSERT_TRUE(transaction->Commit().IsIOError());
  ASSERT_TRUE(ResultToStatus(transaction->ReadDB()).IsIOError());
  ASSERT_TRUE(transaction->Commit().IsIOError());
  transaction->Abort();
  ASSERT_TRUE(transaction->Begin().IsIOError());
  ASSERT_TRUE(transaction->RollbackTo(1).IsIOError());
  ASSERT_TRUE(ResultToStatus(transaction->ReadDB()).IsIOError());
  sync->DisableProcessing();
  transaction.reset();
  transaction = ASSERT_RESULT(PgCatalogTransaction::Open(path, Snapshot() + "/rocksdb"));
  ASSERT_OK(transaction->CheckHealthy());
  const auto recovered = ASSERT_RESULT(transaction->Tables());
  ASSERT_EQ(recovered.size(), 2);
  ASSERT_RESULT(transaction->ReadDB(descriptor.table_id()));
  ASSERT_RESULT(transaction->ReadDB(index_descriptor.table_id()));
  for (const auto& table : recovered) {
    if (!table.has_index_info()) {
      ASSERT_EQ(table.indexes_size(), 1);
      ASSERT_EQ(table.indexes(0).table_id(), index_descriptor.table_id());
    }
  }
}

TEST_F(ReadonlyPgServerTest, CatalogCommitCrashRecovery) {
  if (const char* child_path = std::getenv("YB_CATALOG_CRASH_PATH")) {
    const auto database_oid =
        static_cast<uint32_t>(std::stoul(std::getenv("YB_CATALOG_CRASH_OID")));
    tserver::ExportedTabletMetadataChanges changes;
    ASSERT_OK(
        pb_util::ReadPBContainerFromPath(
            Env::Default(), Snapshot() + "/exported_tablet_metadata_changes", &changes));
    auto packings = std::make_shared<dockv::SchemaPackingRegistry>("catalog-crash");
    std::shared_ptr<docdb::DocReadContext> context;
    auto load = [&](const tablet::TableInfoPB& table) -> Status {
      if (table.table_id() != kPgYbCatalogVersionTableId) return Status::OK();
      context = std::make_shared<docdb::DocReadContext>(
          "catalog-crash", TableType::PGSQL_TABLE_TYPE, docdb::Index::kFalse, packings);
      RETURN_NOT_OK(context->LoadFromPB(table));
      context->SetCotableId(VERIFY_RESULT(Uuid::FromHexString(table.table_id())));
      return Status::OK();
    };
    for (const auto& change : changes.metadata_changes()) {
      if (change.has_add_table()) ASSERT_OK(load(change.add_table()));
      for (const auto& table : change.add_multiple_tables()) ASSERT_OK(load(table));
    }
    ASSERT_TRUE(context);
    auto transaction =
        ASSERT_RESULT(PgCatalogTransaction::Open(child_path, Snapshot() + "/rocksdb"));
    ASSERT_OK(transaction->Begin());
    ThreadSafeArena arena;
    rpc::Sidecars sidecars;
    for (auto oid : {1u, database_oid}) {
      PgsqlWriteRequestPB request;
      request.set_client(YQL_CLIENT_PGSQL);
      request.set_table_id(kPgYbCatalogVersionTableId);
      request.set_schema_version(0);
      request.set_stmt_type(PgsqlWriteRequestPB::PGSQL_UPDATE);
      request.add_range_column_values()->mutable_value()->set_uint32_value(oid);
      auto* value = request.add_column_new_values();
      value->set_column_id(
          context->schema().column_id(context->schema().find_column("current_version")));
      value->mutable_expr()->mutable_value()->set_int64_value(2);
      for (const auto& id : context->schema().column_ids())
        request.mutable_column_refs()->add_ids(id);
      LWPgsqlWriteRequestPB req(&arena);
      req.CopyFrom(request);
      LWPgsqlResponsePB response(&arena);
      ASSERT_OK(transaction->Apply(req, context, &response, &sidecars, 1));
      ASSERT_EQ(response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
    }
    auto* sync = SyncPoint::GetInstance();
    const std::string point = std::getenv("YB_CATALOG_CRASH_POINT");
    int families = 0;
    if (point == "PgCatalogTransaction::IndexFamilyCreated") {
      sync->SetCallBack("PgCatalogTransaction::FamilyCreated", [&families](void*) {
        if (++families == 2) std::raise(SIGKILL);
      });
    } else {
      sync->SetCallBack(point, [](void*) { std::raise(SIGKILL); });
    }
    sync->EnableProcessing();
    const auto base_oid = ASSERT_RESULT(transaction->AllocateOid(database_oid));
    const auto descriptor = ASSERT_RESULT(TestTable(database_oid, base_oid));
    ASSERT_OK(transaction->CreateTable(descriptor, 1));
    auto row_context = std::make_shared<docdb::DocReadContext>(
        "crash-table", TableType::PGSQL_TABLE_TYPE, docdb::Index::kFalse, packings);
    ASSERT_OK(row_context->LoadFromPB(descriptor));
    PgsqlWriteRequestPB row;
    row.set_client(YQL_CLIENT_PGSQL);
    row.set_stmt_type(PgsqlWriteRequestPB::PGSQL_INSERT);
    row.set_table_id(descriptor.table_id());
    row.set_schema_version(0);
    row.add_partition_column_values()->mutable_value()->set_int64_value(42);
    dockv::PartitionSchema partition;
    ASSERT_OK(
        dockv::PartitionSchema::FromPB(
            descriptor.partition_schema(), row_context->schema(), &partition));
    row.set_hash_code(
        ASSERT_RESULT(partition.PgsqlHashColumnCompoundValue(row.partition_column_values())));
    LWPgsqlWriteRequestPB row_request(&arena);
    row_request.CopyFrom(row);
    LWPgsqlResponsePB row_response(&arena);
    ASSERT_OK(transaction->Apply(row_request, row_context, &row_response, &sidecars, 1));
    ASSERT_EQ(row_response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
    const auto index_descriptor = ASSERT_RESULT(
        TestTable(database_oid, ASSERT_RESULT(transaction->AllocateOid(database_oid)), base_oid));
    ASSERT_OK(transaction->CreateTable(index_descriptor, 1));
    auto index_context = std::make_shared<docdb::DocReadContext>(
        "crash-index", TableType::PGSQL_TABLE_TYPE, docdb::Index::kTrue, packings);
    ASSERT_OK(index_context->LoadFromPB(index_descriptor));
    row.set_table_id(index_descriptor.table_id());
    const dockv::DocKey base_key(row.hash_code(), {dockv::KeyEntryValue::Int64(42)}, {});
    row.add_range_column_values()->mutable_value()->set_binary_value(
        base_key.Encode().ToStringBuffer());
    LWPgsqlWriteRequestPB index_request(&arena);
    index_request.CopyFrom(row);
    LWPgsqlResponsePB index_response(&arena);
    ASSERT_OK(transaction->Apply(index_request, index_context, &index_response, &sidecars, 1));
    ASSERT_EQ(index_response.status(), PgsqlResponsePB::PGSQL_STATUS_OK);
    ASSERT_OK(transaction->Commit());
    FAIL() << "Expected SIGKILL at catalog commit boundary";
    return;
  }
  std::string executable;
  ASSERT_OK(Env::Default()->GetExecutablePath(&executable));
  for (auto point : {"FamilyCreated", "IndexFamilyCreated", "BeforeCommit", "AfterCommit"}) {
    const bool committed = std::string(point) == "AfterCommit";
    const auto path = GetTestPath(point);
    auto server = ASSERT_RESULT(
        ReadonlyPgServer::Start(
            path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
    int64_t oid;
    {
      auto connection = ASSERT_RESULT(Connect(*server));
      oid = ASSERT_RESULT(connection.FetchRow<int64_t>(
          "SELECT oid::bigint FROM pg_database WHERE datname = current_database()"));
    }
    server.reset();
    Subprocess child(
        executable, {executable, "--gtest_filter=ReadonlyPgServerTest.CatalogCommitCrashRecovery"});
    child.SetEnv("YB_CATALOG_CRASH_PATH", path + "/catalog");
    child.SetEnv("YB_CATALOG_CRASH_OID", std::to_string(oid));
    child.SetEnv("YB_CATALOG_CRASH_POINT", std::string("PgCatalogTransaction::") + point);
    child.SetEnv("TEST_TMPDIR", path + "/child-root");
    ASSERT_OK(child.Start());
    int code;
    ASSERT_OK(child.Wait(&code));
    ASSERT_TRUE(WIFSIGNALED(code));
    ASSERT_EQ(WTERMSIG(code), SIGKILL);
    server = ASSERT_RESULT(
        ReadonlyPgServer::Start(
            path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
    {
      auto connection = ASSERT_RESULT(Connect(*server));
      const auto versions = ASSERT_RESULT(connection.FetchRows<int64_t>(Format(
          "SELECT current_version FROM pg_yb_catalog_version WHERE db_oid IN (1, $0)", oid)));
      ASSERT_EQ(versions.size(), 2);
      for (const auto version : versions) ASSERT_EQ(version, committed ? 2 : 1);
    }
    server.reset();
    auto storage =
        ASSERT_RESULT(PgCatalogTransaction::Open(path + "/catalog", Snapshot() + "/rocksdb"));
    const auto descriptors = ASSERT_RESULT(storage->Tables());
    ASSERT_EQ(descriptors.size(), committed ? 2 : 0);
    ASSERT_GT(ASSERT_RESULT(storage->AllocateOid(static_cast<uint32_t>(oid))), 1000000000);
    for (const auto& descriptor : descriptors) {
      if (!descriptor.has_index_info()) {
        ASSERT_EQ(descriptor.indexes_size(), 1);
        ASSERT_EQ(descriptor.indexes(0).indexed_table_id(), descriptor.table_id());
      }
      auto* db = ASSERT_RESULT(storage->ReadDB(descriptor.table_id()));
      std::unique_ptr<rocksdb::Iterator> rows(db->NewIterator(rocksdb::ReadOptions()));
      rows->SeekToFirst();
      ASSERT_TRUE(rows->Valid());
      ASSERT_OK(rows->status());
    }
    storage.reset();
    rocksdb::Options options;
    docdb::InitRocksDBOptions(
        &options, "test", "test", nullptr, tablet::TabletOptions(), docdb::StorageDbType::kRegular);
    std::vector<std::string> families;
    ASSERT_OK(rocksdb::DB::ListColumnFamilies(options, path + "/catalog", &families));
    ASSERT_EQ(families.size(), committed ? 5 : 3);
  }
}

TEST_F(ReadonlyPgServerTest, ConcurrentSqlSessions) {
  // Release builds default to native READ COMMITTED; pin it so every build checks one contract.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_yb_enable_read_committed_isolation) = true;
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("concurrent-sql"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto first = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(first.Execute("CREATE TABLE lease_rows(k bigint PRIMARY KEY,v bigint)"));
  ASSERT_OK(first.Execute("CREATE UNIQUE INDEX lease_value ON lease_rows(v)"));
  ASSERT_OK(first.Execute("INSERT INTO lease_rows VALUES(1,10)"));
  auto second = ASSERT_RESULT(Connect(*server));
  for (auto* connection : {&first, &second})
    ASSERT_OK(connection->Execute("PREPARE lease_read AS SELECT v FROM lease_rows WHERE k=1"));
  const auto read = [](pgwrapper::PGConn& connection) {
    return connection.FetchRow<int64_t>("EXECUTE lease_read");
  };
  const auto keys = [&second](const std::string& where) {
    return second.FetchRow<std::string>(
        "SELECT coalesce(string_agg(k::text, ',' ORDER BY k), '') FROM lease_rows WHERE " + where);
  };

  // A snapshot reader neither blocks a writer nor sees its later commit.
  ASSERT_OK(first.Execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"));
  ASSERT_EQ(ASSERT_RESULT(read(first)), 10);
  ASSERT_OK(second.Execute("UPDATE lease_rows SET v=20 WHERE k=1"));
  ASSERT_EQ(ASSERT_RESULT(read(first)), 10);
  ASSERT_OK(first.Execute("COMMIT"));
  ASSERT_EQ(ASSERT_RESULT(read(first)), 20);

  // READ COMMITTED statements read and update as of their own start.
  ASSERT_OK(first.Execute("BEGIN ISOLATION LEVEL READ COMMITTED"));
  ASSERT_EQ(ASSERT_RESULT(read(first)), 20);
  ASSERT_OK(second.Execute("UPDATE lease_rows SET v=21 WHERE k=1"));
  ASSERT_EQ(ASSERT_RESULT(read(first)), 21);
  ASSERT_OK(first.Execute("UPDATE lease_rows SET v=v+1 WHERE k=1"));
  ASSERT_OK(first.Execute("COMMIT"));
  ASSERT_EQ(ASSERT_RESULT(read(second)), 22);

  // Disjoint writers commit concurrently, in either order.
  ASSERT_OK(first.Execute("BEGIN"));
  ASSERT_OK(second.Execute("BEGIN"));
  ASSERT_OK(first.Execute("INSERT INTO lease_rows VALUES(2,200)"));
  ASSERT_OK(second.Execute("INSERT INTO lease_rows VALUES(3,300)"));
  ASSERT_OK(second.Execute("COMMIT"));
  ASSERT_OK(first.Execute("COMMIT"));
  ASSERT_EQ(ASSERT_RESULT(keys("true")), "1,2,3");

  // Overlapping row and unique-index writes fail rather than wait, and the loser continues
  // after a savepoint rollback.
  ASSERT_OK(first.Execute("BEGIN ISOLATION LEVEL REPEATABLE READ"));
  ASSERT_OK(first.Execute("UPDATE lease_rows SET v=30 WHERE k=1"));
  ASSERT_OK(first.Execute("INSERT INTO lease_rows VALUES(4,400)"));
  ASSERT_OK(second.Execute("BEGIN ISOLATION LEVEL REPEATABLE READ"));
  for (const auto& statement :
       {"UPDATE lease_rows SET v=40 WHERE k=1", "INSERT INTO lease_rows VALUES(5,400)"}) {
    ASSERT_OK(second.Execute("SAVEPOINT attempt"));
    const auto conflict = second.Execute(statement);
    ASSERT_NOK(conflict);
    ASSERT_EQ(PgsqlError(conflict), YBPgErrorCode::YB_PG_T_R_SERIALIZATION_FAILURE) << conflict;
    ASSERT_OK(second.Execute("ROLLBACK TO attempt"));
  }
  ASSERT_OK(second.Execute("INSERT INTO lease_rows VALUES(6,600)"));
  ASSERT_OK(second.Execute("COMMIT"));
  ASSERT_OK(first.Execute("COMMIT"));
  ASSERT_EQ(ASSERT_RESULT(read(second)), 30);
  ASSERT_EQ(ASSERT_RESULT(keys("true")), "1,2,3,4,6");

  // Catalog changes need every other backend idle, and exclude them while open.
  ASSERT_OK(first.Execute("BEGIN"));
  ASSERT_EQ(ASSERT_RESULT(read(first)), 30);
  ASSERT_NOK(second.Execute("CREATE TABLE blocked_rows(k bigint PRIMARY KEY)"));
  ASSERT_OK(first.Execute("COMMIT"));
  ASSERT_OK(second.Execute("CREATE TABLE blocked_rows(k bigint PRIMARY KEY)"));
  ASSERT_OK(second.Execute("BEGIN"));
  ASSERT_OK(second.Execute("COMMENT ON TABLE lease_rows IS 'published'"));
  const auto excluded = ResultToStatus(first.Fetch("SELECT count(*) FROM lease_rows"));
  ASSERT_NOK(excluded);
  ASSERT_EQ(PgsqlError(excluded), YBPgErrorCode::YB_PG_T_R_SERIALIZATION_FAILURE) << excluded;
  ASSERT_OK(second.Execute("COMMIT"));
  ASSERT_EQ(
      ASSERT_RESULT(first.FetchRow<std::string>("SELECT obj_description('lease_rows'::regclass)")),
      "published");

  // DDL adopts the backend's open data transaction, so both commit or roll back together.
  ASSERT_OK(first.Execute("BEGIN"));
  ASSERT_OK(first.Execute("INSERT INTO lease_rows VALUES(7,700)"));
  ASSERT_OK(first.Execute("CREATE TABLE adopted_rows(k bigint PRIMARY KEY)"));
  ASSERT_OK(first.Execute("INSERT INTO adopted_rows VALUES(1)"));
  ASSERT_OK(first.Execute("COMMIT"));
  ASSERT_EQ(ASSERT_RESULT(second.FetchRow<int64_t>("SELECT k FROM adopted_rows")), 1);
  ASSERT_EQ(ASSERT_RESULT(keys("k=7")), "7");
  ASSERT_OK(first.Execute("BEGIN"));
  ASSERT_OK(first.Execute("INSERT INTO lease_rows VALUES(8,800)"));
  ASSERT_OK(first.Execute("CREATE TABLE discarded_rows(k bigint PRIMARY KEY)"));
  ASSERT_OK(first.Execute("ROLLBACK"));
  ASSERT_EQ(ASSERT_RESULT(keys("k=8")), "");
  ASSERT_TRUE(
      ASSERT_RESULT(second.FetchRow<bool>("SELECT to_regclass('discarded_rows') IS NULL")));

  // Savepoints in an independent transaction, including one no request ever used.
  ASSERT_OK(first.Execute("BEGIN"));
  ASSERT_OK(first.Execute("INSERT INTO lease_rows VALUES(10,1000)"));
  ASSERT_OK(first.Execute("SAVEPOINT partial"));
  ASSERT_OK(first.Execute("INSERT INTO lease_rows VALUES(11,1100)"));
  ASSERT_OK(first.Execute("ROLLBACK TO partial"));
  ASSERT_OK(first.Execute("SAVEPOINT unused"));
  ASSERT_OK(first.Execute("ROLLBACK TO unused"));
  ASSERT_OK(first.Execute("INSERT INTO lease_rows VALUES(12,1200)"));
  ASSERT_OK(first.Execute("COMMIT"));
  ASSERT_EQ(ASSERT_RESULT(keys("k>=10")), "10,12");

  // A backend that exits mid-transaction loses its writes and stops blocking catalog changes.
  {
    auto exiting = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(exiting.Execute("BEGIN"));
    ASSERT_OK(exiting.Execute("INSERT INTO lease_rows VALUES(20,2000)"));
  }
  ASSERT_OK(WaitFor(
      [&second]() -> Result<bool> {
        return second.Execute("CREATE TABLE after_exit(k bigint PRIMARY KEY)").ok();
      },
      30s, "exited backend released its transaction"));
  ASSERT_EQ(ASSERT_RESULT(keys("k=20")), "");

  // Catalog changes from one backend reach another's caches.
  ASSERT_OK(second.Execute(
      "CREATE FUNCTION payload_length(bytea) RETURNS bigint LANGUAGE sql IMMUTABLE "
      "AS $$ SELECT octet_length($1)::bigint $$"));
  ASSERT_OK(second.Execute("CREATE CAST(bytea AS bigint) WITH FUNCTION payload_length(bytea)"));
  ASSERT_EQ(ASSERT_RESULT(first.FetchRow<int64_t>("SELECT decode('010203','hex')::bigint")), 3);
  for (int i = 0; i < 20; ++i) {
    ASSERT_EQ(ASSERT_RESULT(read(first)), 30);
    ASSERT_EQ(ASSERT_RESULT(keys("k<=4")), "1,2,3,4");
  }
}

TEST_F(ReadonlyPgServerTest, RowLocks) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_yb_enable_read_committed_isolation) = true;
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("row-locks"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto locker = ASSERT_RESULT(Connect(*server));
  auto other = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(locker.Execute("CREATE TABLE locked_rows(k bigint PRIMARY KEY, v bigint)"));
  ASSERT_OK(locker.Execute("INSERT INTO locked_rows VALUES(1,10),(2,20),(3,30)"));
  // A REPEATABLE READ snapshot taken first keeps the query layer from retrying the conflict.
  const auto expect_conflict = [&other](const std::string& statement) -> Status {
    RETURN_NOT_OK(other.Execute("BEGIN ISOLATION LEVEL REPEATABLE READ"));
    RETURN_NOT_OK(other.Fetch("SELECT count(*) FROM locked_rows"));
    const auto status = ResultToStatus(other.Fetch(statement));
    RETURN_NOT_OK(other.Execute("ROLLBACK"));
    SCHECK(!status.ok(), IllegalState, "Expected a row-lock conflict: " + statement);
    SCHECK(
        PgsqlError(status) == YBPgErrorCode::YB_PG_T_R_SERIALIZATION_FAILURE, IllegalState,
        status.ToString());
    return Status::OK();
  };
  const auto expect_allowed = [&other](const std::string& statement) -> Status {
    RETURN_NOT_OK(other.Execute("BEGIN ISOLATION LEVEL REPEATABLE READ"));
    const auto status = ResultToStatus(other.Fetch(statement));
    RETURN_NOT_OK(other.Execute("ROLLBACK"));
    return status;
  };

  // FOR UPDATE excludes writers and other lockers of that row only; plain reads still proceed,
  // and the locker's own lock intents never mask its data.
  ASSERT_OK(locker.Execute("BEGIN"));
  ASSERT_EQ(
      ASSERT_RESULT(locker.FetchRow<int64_t>("SELECT v FROM locked_rows WHERE k=1 FOR UPDATE")),
      10);
  ASSERT_EQ(ASSERT_RESULT(locker.FetchRow<int64_t>("SELECT v FROM locked_rows WHERE k=1")), 10);
  ASSERT_OK(expect_conflict("UPDATE locked_rows SET v=11 WHERE k=1 RETURNING v"));
  ASSERT_OK(expect_conflict("SELECT v FROM locked_rows WHERE k=1 FOR SHARE"));
  ASSERT_OK(expect_allowed("SELECT v FROM locked_rows WHERE k=2 FOR UPDATE"));
  ASSERT_OK(expect_allowed("SELECT v FROM locked_rows WHERE k=1"));
  ASSERT_NOK(other.Fetch("SELECT v FROM locked_rows WHERE k=1 FOR UPDATE NOWAIT"));
  ASSERT_EQ(
      ASSERT_RESULT(other.FetchAllAsString(
          "SELECT k FROM locked_rows ORDER BY k FOR UPDATE SKIP LOCKED")),
      "2; 3");
  ASSERT_OK(locker.Execute("COMMIT"));
  ASSERT_OK(expect_allowed("UPDATE locked_rows SET v=11 WHERE k=1 RETURNING v"));

  // Shared modes are mutually compatible; FOR KEY SHARE conflicts only with FOR UPDATE.
  ASSERT_OK(locker.Execute("BEGIN"));
  ASSERT_OK(locker.Fetch("SELECT v FROM locked_rows WHERE k=1 FOR SHARE"));
  ASSERT_OK(expect_allowed("SELECT v FROM locked_rows WHERE k=1 FOR SHARE"));
  ASSERT_OK(expect_conflict("UPDATE locked_rows SET v=12 WHERE k=1 RETURNING v"));
  ASSERT_OK(locker.Execute("COMMIT"));
  ASSERT_OK(locker.Execute("BEGIN"));
  ASSERT_OK(locker.Fetch("SELECT v FROM locked_rows WHERE k=1 FOR KEY SHARE"));
  ASSERT_OK(expect_allowed("SELECT v FROM locked_rows WHERE k=1 FOR NO KEY UPDATE"));
  ASSERT_OK(expect_conflict("SELECT v FROM locked_rows WHERE k=1 FOR UPDATE"));
  ASSERT_OK(locker.Execute("COMMIT"));

  // A full-scan lock covers every row it read.
  ASSERT_OK(locker.Execute("BEGIN"));
  ASSERT_EQ(ASSERT_RESULT(locker.FetchAllAsString("SELECT k FROM locked_rows FOR UPDATE")).size(),
            ASSERT_RESULT(locker.FetchAllAsString("SELECT k FROM locked_rows")).size());
  ASSERT_OK(expect_conflict("UPDATE locked_rows SET v=21 WHERE k=2 RETURNING v"));
  ASSERT_OK(locker.Execute("COMMIT"));

  // Rollback and savepoint rollback release locks.
  ASSERT_OK(locker.Execute("BEGIN"));
  ASSERT_OK(locker.Fetch("SELECT v FROM locked_rows WHERE k=3 FOR UPDATE"));
  ASSERT_OK(locker.Execute("ROLLBACK"));
  ASSERT_OK(expect_allowed("UPDATE locked_rows SET v=31 WHERE k=3 RETURNING v"));
  ASSERT_OK(locker.Execute("BEGIN"));
  ASSERT_OK(locker.Execute("SAVEPOINT unlock"));
  ASSERT_OK(locker.Fetch("SELECT v FROM locked_rows WHERE k=3 FOR UPDATE"));
  ASSERT_OK(locker.Execute("ROLLBACK TO unlock"));
  ASSERT_OK(expect_allowed("UPDATE locked_rows SET v=32 WHERE k=3 RETURNING v"));
  ASSERT_OK(locker.Execute("COMMIT"));

  // A lock-only commit after another snapshot is a released lock, not a write conflict.
  ASSERT_OK(other.Execute("BEGIN ISOLATION LEVEL REPEATABLE READ"));
  // Every earlier probe rolled back, so the committed value is unchanged.
  ASSERT_EQ(ASSERT_RESULT(other.FetchRow<int64_t>("SELECT v FROM locked_rows WHERE k=1")), 10);
  ASSERT_OK(locker.Execute("BEGIN"));
  ASSERT_OK(locker.Fetch("SELECT v FROM locked_rows WHERE k=1 FOR UPDATE"));
  ASSERT_OK(locker.Execute("COMMIT"));
  ASSERT_OK(other.Execute("UPDATE locked_rows SET v=13 WHERE k=1"));
  ASSERT_OK(other.Execute("COMMIT"));
  ASSERT_EQ(ASSERT_RESULT(locker.FetchRow<int64_t>("SELECT v FROM locked_rows WHERE k=1")), 13);
}

TEST_F(ReadonlyPgServerTest, CatalogSavepointsAndSessionFence) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("catalog-boundaries"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  const std::string query = "SELECT obj_description('pg_catalog.pg_class'::regclass)";
  {
    auto connection = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'baseline'"));
    auto idle = ASSERT_RESULT(Connect(*server));
    ASSERT_EQ(ASSERT_RESULT(idle.FetchRow<std::string>(query)), "baseline");
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "baseline");
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'outer'"));
    ASSERT_OK(idle.Execute("BEGIN ISOLATION LEVEL REPEATABLE READ"));
    const auto blocked = ResultToStatus(idle.Fetch(query));
    ASSERT_NOK(blocked);
    ASSERT_EQ(PgsqlError(blocked), YBPgErrorCode::YB_PG_T_R_SERIALIZATION_FAILURE) << blocked;
    ASSERT_OK(idle.Execute("ROLLBACK"));
    ASSERT_OK(connection.Execute("SAVEPOINT s"));
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'inner'"));
    ASSERT_OK(connection.Execute("SAVEPOINT nested"));
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'nested'"));
    ASSERT_OK(connection.Execute("RELEASE nested"));
    ASSERT_OK(connection.Execute("ROLLBACK TO s"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "outer");
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'continued'"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "continued");
    ASSERT_OK(connection.Execute("ROLLBACK TO s"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "outer");
    ASSERT_OK(connection.Execute("RELEASE s"));
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'final'"));
    ASSERT_OK(connection.Execute("COMMIT"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "final");
    ASSERT_EQ(ASSERT_RESULT(idle.FetchRow<std::string>(query)), "final");
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'discard'"));
    const auto split = connection.Execute(
        "CREATE TABLE split_unsupported(k bigint,PRIMARY KEY(k ASC)) SPLIT AT VALUES((100))");
    ASSERT_NOK(split);
    ASSERT_STR_CONTAINS(split.ToString(), "Unsupported local table request");
    ASSERT_OK(connection.Execute("ROLLBACK"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "final");
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'disconnected'"));
  }
  auto replacement = ASSERT_RESULT(Connect(*server));
  ASSERT_EQ(ASSERT_RESULT(replacement.FetchRow<std::string>(query)), "final");
}

TEST_F(ReadonlyPgServerTest, DdlSavepointsAndStatementSnapshots) {
  const auto path = GetTestPath("ddl-rollback");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto connection = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute(
        "CREATE TABLE retained(k bigint PRIMARY KEY, v bigint) SPLIT INTO 4 TABLETS"));
    ASSERT_OK(connection.Execute("INSERT INTO retained VALUES (1,10), (2,20), (3,30)"));
    ASSERT_OK(connection.Execute("SAVEPOINT s"));
    ASSERT_OK(
        connection.Execute("CREATE TABLE discarded(k bigint PRIMARY KEY) SPLIT INTO 4 TABLETS"));
    const auto abandoned_oid =
        ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT 'discarded'::regclass::oid::bigint"));
    ASSERT_OK(connection.Execute("INSERT INTO discarded VALUES (1)"));
    ASSERT_OK(connection.Execute("ROLLBACK TO s"));
    ASSERT_TRUE(
        ASSERT_RESULT(connection.FetchRow<bool>("SELECT to_regclass('discarded') IS NULL")));
    ASSERT_OK(
        connection.Execute("CREATE TABLE discarded(k bigint PRIMARY KEY) SPLIT INTO 4 TABLETS"));
    ASSERT_GT(
        ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT 'discarded'::regclass::oid::bigint")),
        abandoned_oid);
    ASSERT_OK(connection.Execute("INSERT INTO discarded VALUES (2)"));
    ASSERT_OK(connection.Execute("COMMIT"));
    ASSERT_OK(connection.Execute("SET yb_fetch_row_limit = 1"));
    ASSERT_OK(connection.Execute("SET statement_timeout = 5000"));
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute("INSERT INTO retained SELECT k+100, v FROM retained"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM retained")), 6);
    ASSERT_OK(connection.Execute("ROLLBACK"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM retained")), 3);
    ASSERT_OK(connection.Execute("UPDATE retained SET v = v+1 WHERE k = 1"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT v FROM retained WHERE k=1")), 11);
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute("SAVEPOINT duplicate_guard"));
    ASSERT_NOK(connection.Execute("INSERT INTO retained VALUES (9,90), (1,100)"));
    ASSERT_OK(connection.Execute("ROLLBACK TO duplicate_guard"));
    ASSERT_OK(connection.Execute("INSERT INTO retained VALUES (9,91)"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT v FROM retained WHERE k=9")), 91);
    ASSERT_OK(connection.Execute("ROLLBACK"));
    ASSERT_NOK(connection.Execute("INSERT INTO retained VALUES (9,90), (1,100)"));
    ASSERT_EQ(
        ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM retained WHERE k=9")), 0);
    ASSERT_OK(connection.Execute("DELETE FROM retained WHERE k=3"));
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute("UPDATE retained SET k=k+1000"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT min(k) FROM retained")), 1001);
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM retained")), 2);
    ASSERT_OK(connection.Execute("ROLLBACK"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM retained")), 2);
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute("CREATE TABLE aborted(k bigint PRIMARY KEY)"));
    ASSERT_OK(connection.Execute("ROLLBACK"));
    ASSERT_TRUE(ASSERT_RESULT(connection.FetchRow<bool>("SELECT to_regclass('aborted') IS NULL")));
    ASSERT_NOK(connection.Execute("CREATE INDEX CONCURRENTLY unsupported_index ON retained(v)"));
    ASSERT_TRUE(ASSERT_RESULT(
        connection.FetchRow<bool>("SELECT to_regclass('unsupported_index') IS NULL")));
  }
  server.reset();
  std::vector<std::string> families;
  rocksdb::Options options;
  docdb::InitRocksDBOptions(
      &options, "test", "test", nullptr, tablet::TabletOptions(), docdb::StorageDbType::kRegular);
  ASSERT_OK(rocksdb::DB::ListColumnFamilies(options, path + "/catalog", &families));
  // Catalog and metadata families, plus regular/intents pairs for two committed tables.
  ASSERT_EQ(families.size(), 7);
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto connection = ASSERT_RESULT(Connect(*server));
  ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM retained")), 2);
  ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT k FROM discarded")), 2);
  ASSERT_TRUE(ASSERT_RESULT(connection.FetchRow<bool>("SELECT to_regclass('aborted') IS NULL")));
}

TEST_F(ReadonlyPgServerTest, SecondaryIndexes) {
  const auto path = GetTestPath("secondary-indexes");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(c.Execute(
        "CREATE TABLE indexed_rows(k bigint PRIMARY KEY, bucket bigint, v text) "
        "SPLIT INTO 4 TABLETS"));
    ASSERT_OK(c.Execute(
        "INSERT INTO indexed_rows SELECT k,k%4,'value-'||k "
        "FROM generate_series(1,40) k"));
    ASSERT_OK(c.Execute(
        "CREATE INDEX bucket_idx ON indexed_rows(bucket HASH) INCLUDE(v) "
        "SPLIT INTO 4 TABLETS"));
    ASSERT_OK(c.Execute("SET enable_seqscan=off"));
    ASSERT_OK(c.Execute("SET yb_fetch_row_limit=3"));
    ASSERT_TRUE(ASSERT_RESULT(c.HasIndexScan("SELECT k FROM indexed_rows WHERE bucket=1")));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE bucket=1")), 10);
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<std::string>("SELECT v FROM indexed_rows WHERE bucket=1 AND k=1")),
        "value-1");
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.Execute("UPDATE indexed_rows SET bucket=9,v='private' WHERE k=1"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE bucket=9")), 1);
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE bucket=1")), 9);
    ASSERT_OK(c.Execute("ROLLBACK"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE bucket=1")), 10);
    ASSERT_OK(c.Execute("DELETE FROM indexed_rows WHERE k=1"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE bucket=1")), 9);
    ASSERT_OK(c.Execute("INSERT INTO indexed_rows VALUES(41,1,'new')"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE bucket=1")), 10);
  }
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(c.Execute("SET enable_seqscan=off"));
  ASSERT_TRUE(ASSERT_RESULT(c.HasIndexScan("SELECT k FROM indexed_rows WHERE bucket=1")));
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE bucket=1")), 10);
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<std::string>("SELECT v FROM indexed_rows WHERE bucket=1 AND k=41")),
      "new");
}

TEST_F(ReadonlyPgServerTest, IndexTransactionsAndUniqueness) {
  const auto path = GetTestPath("unique-indexes");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(c.Execute("CREATE TABLE indexed_rows(k bigint PRIMARY KEY, v bigint, payload text)"));
    ASSERT_OK(c.Execute(
        "INSERT INTO indexed_rows VALUES (1,10,'one'),(2,20,'two'),"
        "(3,NULL,'three'),(4,NULL,'four')"));
    ASSERT_OK(c.Execute("CREATE INDEX payload_idx ON indexed_rows(payload HASH)"));
    ASSERT_OK(c.Execute("CREATE UNIQUE INDEX value_idx ON indexed_rows(v HASH) INCLUDE(payload)"));
    ASSERT_OK(c.Execute("SET enable_seqscan=off"));
    ASSERT_OK(c.Execute("SET yb_fetch_row_limit=1"));
    ASSERT_TRUE(ASSERT_RESULT(c.HasIndexScan("SELECT k FROM indexed_rows WHERE v=10")));
    ASSERT_NOK(c.Execute("INSERT INTO indexed_rows VALUES (5,50,'five'),(6,10,'six')"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows")), 4);
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>(
            "SELECT count(*) FROM indexed_rows WHERE payload IN ('five','six')")),
        0);
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE v=50")), 0);
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.Execute("SAVEPOINT duplicate_guard"));
    ASSERT_NOK(c.Execute("UPDATE indexed_rows SET v=10,payload='bad' WHERE k=2"));
    ASSERT_OK(c.Execute("ROLLBACK TO duplicate_guard"));
    ASSERT_OK(c.Execute("UPDATE indexed_rows SET k=12,v=21,payload='updated' WHERE k=2"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT k FROM indexed_rows WHERE v=21")), 12);
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE payload='bad'")),
        0);
    ASSERT_OK(c.Execute("COMMIT"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE v=20")), 0);
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE v IS NULL")), 2);
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.Execute("SAVEPOINT build_guard"));
    ASSERT_OK(c.Execute("CREATE INDEX temporary_idx ON indexed_rows(v ASC)"));
    ASSERT_OK(c.Execute("ROLLBACK TO build_guard"));
    ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>("SELECT to_regclass('temporary_idx') IS NULL")));
    ASSERT_OK(c.Execute("CREATE INDEX temporary_idx ON indexed_rows(v ASC)"));
    ASSERT_OK(c.Execute("ROLLBACK"));
    ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>("SELECT to_regclass('temporary_idx') IS NULL")));
    ASSERT_NOK(c.Execute("CREATE UNIQUE INDEX failed_idx ON indexed_rows(v) NULLS NOT DISTINCT"));
    ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>("SELECT to_regclass('failed_idx') IS NULL")));
    ASSERT_NOK(c.Execute("CREATE INDEX CONCURRENTLY online_idx ON indexed_rows(v)"));
    ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>("SELECT to_regclass('online_idx') IS NULL")));
    ASSERT_OK(c.Execute("DELETE FROM indexed_rows WHERE k=4"));
    ASSERT_OK(c.Execute("CREATE UNIQUE INDEX failed_idx ON indexed_rows(v) NULLS NOT DISTINCT"));
    ASSERT_NOK(c.Execute("INSERT INTO indexed_rows VALUES(5,NULL,'five')"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE k=5")), 0);
  }
  server.reset();
  {
    auto store =
        ASSERT_RESULT(PgCatalogTransaction::Open(path + "/catalog", Snapshot() + "/rocksdb"));
    const auto descriptors = ASSERT_RESULT(store->Tables());
    ASSERT_EQ(descriptors.size(), 4);
    for (const auto& table : descriptors) {
      if (table.has_index_info()) continue;
      ASSERT_EQ(table.indexes_size(), 3);
      for (const auto& index : table.indexes()) {
        ASSERT_EQ(index.indexed_table_id(), table.table_id());
        ASSERT_TRUE(
            std::any_of(descriptors.begin(), descriptors.end(), [&](const auto& descriptor) {
              return descriptor.table_id() == index.table_id() && descriptor.has_index_info() &&
                     descriptor.index_info().indexed_table_id() == table.table_id();
            }));
      }
    }
  }
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(c.Execute("SET enable_seqscan=off"));
  ASSERT_TRUE(ASSERT_RESULT(c.HasIndexScan("SELECT k FROM indexed_rows WHERE v=21")));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT k FROM indexed_rows WHERE v=21")), 12);
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows WHERE v IS NULL")), 1);
  ASSERT_NOK(c.Execute("INSERT INTO indexed_rows VALUES(5,21,'five')"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM indexed_rows")), 3);
}

TEST_F(ReadonlyPgServerTest, PrimaryKeyRewrite) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("key-rewrite"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(c.Execute("CREATE TABLE key_rows(k bigint PRIMARY KEY,v bigint NOT NULL,note text)"));
  ASSERT_OK(c.Execute("INSERT INTO key_rows VALUES(1,10,'one'),(2,20,'two')"));
  ASSERT_OK(c.Execute("CREATE INDEX key_note ON key_rows(note HASH) INCLUDE(v)"));
  ASSERT_OK(c.Execute("BEGIN"));
  ASSERT_OK(c.Execute("SAVEPOINT original_key"));
  ASSERT_OK(c.Execute(
      "ALTER TABLE key_rows DROP CONSTRAINT key_rows_pkey; "
      "ALTER TABLE key_rows ADD PRIMARY KEY((v) HASH,k ASC)"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT sum(v)::bigint FROM key_rows")), 30);
  ASSERT_OK(c.Execute("ROLLBACK TO original_key"));
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<int64_t>("SELECT indkey[0]::bigint FROM pg_index WHERE "
                                        "indrelid='key_rows'::regclass AND indisprimary")),
      1);
  ASSERT_OK(c.Execute(
      "ALTER TABLE key_rows DROP CONSTRAINT key_rows_pkey; "
      "ALTER TABLE key_rows ADD PRIMARY KEY((v) HASH,k ASC)"));
  ASSERT_OK(c.Execute("COMMIT"));
  ASSERT_OK(c.Execute("SET enable_seqscan=off"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT v FROM key_rows WHERE note='two'")), 20);
  ASSERT_OK(c.Execute("CREATE TABLE empty_keys(k bigint PRIMARY KEY,v bigint NOT NULL,note text)"));
  ASSERT_OK(c.Execute("CREATE INDEX empty_note ON empty_keys(note HASH) INCLUDE(v)"));
  ASSERT_OK(c.Execute("BEGIN"));
  ASSERT_OK(c.Execute(
      "ALTER TABLE empty_keys DROP CONSTRAINT empty_keys_pkey; "
      "ALTER TABLE empty_keys ADD PRIMARY KEY((v) HASH,k ASC)"));
  ASSERT_OK(c.Execute("COMMIT"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM empty_keys")), 0);
}

// A USING expression that is not a bare column rewrites the table, and the rewrite reindexes
// every index on the column without naming the index's old relfilenode.
TEST_F(ReadonlyPgServerTest, ColumnRewriteReindexes) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("column-rewrite"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(c.Execute("CREATE TABLE typed_rows(k bigint PRIMARY KEY,v bigint NOT NULL,note text)"));
  ASSERT_OK(c.Execute("CREATE UNIQUE INDEX typed_value ON typed_rows(v HASH) INCLUDE(note)"));
  ASSERT_OK(c.Execute("INSERT INTO typed_rows VALUES(1,10,'one'),(2,20,'two')"));
  ASSERT_OK(c.Execute("BEGIN"));
  ASSERT_OK(c.Execute("SAVEPOINT before_rewrite"));
  ASSERT_OK(c.Execute("ALTER TABLE typed_rows ALTER COLUMN v TYPE bigint USING v * 100"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT sum(v)::bigint FROM typed_rows")), 3000);
  ASSERT_OK(c.Execute("ROLLBACK TO before_rewrite"));
  ASSERT_OK(c.Execute("COMMIT"));
  ASSERT_OK(c.Execute("ALTER TABLE typed_rows ALTER COLUMN v TYPE bigint USING v + 1"));
  ASSERT_OK(c.Execute("SET enable_seqscan=off"));
  ASSERT_TRUE(ASSERT_RESULT(c.HasIndexScan("SELECT note FROM typed_rows WHERE v=21")));
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<std::string>("SELECT note FROM typed_rows WHERE v=21")), "two");
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM typed_rows WHERE v=20")), 0);
  const auto duplicate = c.Execute("INSERT INTO typed_rows VALUES(3,11,'three')");
  ASSERT_NOK(duplicate);
  ASSERT_STR_CONTAINS(
      duplicate.ToString(), "duplicate key value violates unique constraint \"typed_value\"");
  ASSERT_OK(c.Execute("INSERT INTO typed_rows VALUES(3,12,'three')"));
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<std::string>("SELECT note FROM typed_rows WHERE v=12")), "three");
  ASSERT_OK(c.Execute("DROP TABLE typed_rows"));
}

TEST_F(ReadonlyPgServerTest, SchemaLifecycle) {
  const auto path = GetTestPath("schema-lifecycle");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(c.Execute("CREATE TABLE evolving(k bigint PRIMARY KEY,v bigint)"));
    ASSERT_OK(c.Execute("INSERT INTO evolving VALUES(1,10),(2,20)"));
    ASSERT_OK(c.Execute("CREATE INDEX evolving_value ON evolving(v HASH) INCLUDE(k)"));
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.Execute("INSERT INTO evolving VALUES(3,30)"));
    ASSERT_OK(c.Execute("SAVEPOINT before_column"));
    ASSERT_OK(c.Execute("ALTER TABLE evolving ADD COLUMN note text DEFAULT 'initial'"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM evolving WHERE note='initial'")),
        3);
    ASSERT_OK(c.Execute("UPDATE evolving SET note='changed' WHERE k=1"));
    ASSERT_OK(c.Execute("SAVEPOINT before_drop"));
    ASSERT_OK(c.Execute("DROP TABLE evolving"));
    ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>("SELECT to_regclass('evolving') IS NULL")));
    ASSERT_OK(c.Execute("ROLLBACK TO before_drop"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<std::string>("SELECT note FROM evolving WHERE k=1")), "changed");
    ASSERT_OK(c.Execute("ROLLBACK TO before_column"));
    ASSERT_OK(c.Execute("ALTER TABLE evolving ADD COLUMN note bigint DEFAULT 77"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM evolving WHERE note=77")), 3);
    ASSERT_OK(c.Execute("COMMIT"));
    ASSERT_OK(c.Execute("ALTER TABLE evolving ADD COLUMN enabled boolean NOT NULL DEFAULT false"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM evolving WHERE enabled=false")), 3);
    ASSERT_OK(c.Execute("UPDATE evolving SET enabled=true WHERE k=1"));
    ASSERT_OK(c.Execute("ALTER TABLE evolving ALTER COLUMN enabled DROP DEFAULT"));
    ASSERT_OK(c.Execute("ALTER TABLE evolving ALTER COLUMN enabled DROP NOT NULL"));
    ASSERT_OK(c.Execute("INSERT INTO evolving(k,v) VALUES(4,40)"));
    ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>("SELECT enabled IS NULL FROM evolving WHERE k=4")));
    ASSERT_OK(c.Execute("ALTER TABLE evolving DROP COLUMN note"));
    ASSERT_OK(c.Execute("ALTER TABLE evolving ADD COLUMN note text DEFAULT 'new'"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM evolving WHERE note='new'")), 4);
    ASSERT_OK(c.Execute("ALTER TABLE evolving RENAME COLUMN note TO label"));
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.Execute("DROP INDEX evolving_value"));
    ASSERT_OK(c.Execute("ROLLBACK"));
    ASSERT_FALSE(ASSERT_RESULT(c.FetchRow<bool>("SELECT to_regclass('evolving_value') IS NULL")));
    ASSERT_OK(c.Execute("DROP INDEX evolving_value"));
    ASSERT_OK(c.Execute("CREATE SCHEMA target"));
    ASSERT_OK(c.Execute("ALTER TABLE evolving RENAME TO renamed"));
    ASSERT_OK(c.Execute("ALTER TABLE renamed SET SCHEMA target"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM target.renamed WHERE label='new'")),
        4);
  }
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT sum(v)::bigint FROM target.renamed")), 100);
  ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>("SELECT enabled FROM target.renamed WHERE k=1")));
  ASSERT_OK(c.Execute("BEGIN"));
  ASSERT_OK(c.Execute("DROP TABLE target.renamed"));
  ASSERT_OK(c.Execute("CREATE TABLE target.renamed(k uuid PRIMARY KEY,v text)"));
  ASSERT_OK(
      c.Execute("INSERT INTO target.renamed VALUES('00000000-0000-0000-0000-000000000001','new')"));
  ASSERT_OK(c.Execute("COMMIT"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<std::string>("SELECT v FROM target.renamed")), "new");
}

TEST_F(ReadonlyPgServerTest, MetadataAlterPreservesRows) {
  const auto path = GetTestPath("metadata-alter");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(c.Execute("CREATE TABLE alter_values(k bigint PRIMARY KEY,n bigint,note text)"));
    ASSERT_OK(c.Execute("INSERT INTO alter_values VALUES(1,-1,'old'),(2,10,'kept')"));
    ASSERT_OK(c.Execute("CREATE INDEX alter_idx ON alter_values(n HASH) INCLUDE(note)"));
    ASSERT_OK(c.Execute("PREPARE prior_plan(bigint) AS SELECT note FROM alter_values WHERE k=$1"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<std::string>("EXECUTE prior_plan(2)")), "kept");
    ASSERT_OK(c.Execute(
        "ALTER TABLE alter_values ADD CONSTRAINT positive_value "
        "CHECK(n>=0) NOT VALID"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT n FROM alter_values WHERE k=1")), -1);
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<std::string>("EXECUTE prior_plan(2)")), "kept");
    ASSERT_NOK(c.Execute("INSERT INTO alter_values VALUES(3,-2,'rejected')"));
    ASSERT_OK(c.Execute("INSERT INTO alter_values VALUES(3,20,'new')"));
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.Execute("SAVEPOINT before_check"));
    ASSERT_OK(c.Execute(
        "ALTER TABLE alter_values ADD CONSTRAINT small_value "
        "CHECK(n<100) NOT VALID"));
    ASSERT_OK(c.Execute("INSERT INTO alter_values VALUES(4,30,'private')"));
    ASSERT_OK(c.Execute("ROLLBACK TO before_check"));
    ASSERT_OK(c.Execute("INSERT INTO alter_values VALUES(4,150,'restored')"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<std::string>("EXECUTE prior_plan(2)")), "kept");
    ASSERT_OK(c.Execute("COMMIT"));
    ASSERT_NOK(c.Execute("ALTER TABLE alter_values VALIDATE CONSTRAINT positive_value"));
    ASSERT_FALSE(ASSERT_RESULT(
        c.FetchRow<bool>("SELECT convalidated FROM pg_constraint WHERE conname='positive_value'")));
    ASSERT_OK(c.Execute("UPDATE alter_values SET n=1 WHERE k=1"));
    ASSERT_OK(c.Execute("ALTER TABLE alter_values VALIDATE CONSTRAINT positive_value"));
    ASSERT_TRUE(ASSERT_RESULT(
        c.FetchRow<bool>("SELECT convalidated FROM pg_constraint WHERE conname='positive_value'")));
    ASSERT_NOK(c.Execute("ALTER TABLE alter_values ADD COLUMN unsupported int PRIMARY KEY"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>(
            "SELECT count(*) FROM pg_attribute WHERE attrelid='alter_values'::regclass "
            "AND attname='unsupported'")),
        0);
    ASSERT_OK(c.Execute("SET enable_seqscan=off"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<std::string>("SELECT note FROM alter_values WHERE n=10")), "kept");
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT sum(n)::bigint FROM alter_values")), 181);
  }
  server.reset();
  {
    auto store =
        ASSERT_RESULT(PgCatalogTransaction::Open(path + "/catalog", Snapshot() + "/rocksdb"));
    const auto tables = ASSERT_RESULT(store->Tables());
    ASSERT_EQ(tables.size(), 2);
    for (const auto& table : tables) {
      if (!table.has_index_info()) {
        ASSERT_GT(table.schema_version(), 0);
        ASSERT_GT(table.old_schema_packings_size(), 0);
      }
    }
  }
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT sum(n)::bigint FROM alter_values")), 181);
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<std::string>("SELECT note FROM alter_values WHERE k=2")), "kept");
  ASSERT_NOK(c.Execute("INSERT INTO alter_values VALUES(5,-5,'rejected')"));
}

TEST_F(ReadonlyPgServerTest, NamespacesAndScalarTypes) {
  const auto path = GetTestPath("scalar-types");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  const std::string check =
      "SELECT n=-23 AND b AND r=2.5 AND d=1.25 AND x=21.875 AND j='{\"n\":2}'::jsonb "
      "AND a='2001-02-03'::date AND tm='04:05:06'::time "
      "AND ts='2001-02-03 04:05:06'::timestamp AND z='2001-02-03 04:05:06+00'::timestamptz "
      "FROM sandbox.scalars WHERE id='00000000-0000-4000-8000-000000000001'::uuid";
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(c.Execute("CREATE SCHEMA empty_namespace"));
    ASSERT_OK(c.Execute("DROP SCHEMA empty_namespace"));
    ASSERT_OK(c.Execute("CREATE SCHEMA sandbox"));
    ASSERT_OK(c.Execute("CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public"));
    ASSERT_OK(c.Execute(
        "CREATE FUNCTION sandbox.strip_marks(v text) RETURNS text "
        "LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT public.unaccent(v) $$"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<std::string>("SELECT sandbox.strip_marks(U&'caf\\00E9')")),
        "cafe");
    ASSERT_OK(c.Execute(
        "CREATE TEXT SEARCH DICTIONARY sandbox.accent_dict "
        "(TEMPLATE=public.unaccent, RULES='unaccent')"));
    ASSERT_OK(c.Execute(
        "CREATE TABLE sandbox.scalars(id uuid PRIMARY KEY,n smallint,b boolean,"
        "r real,d double precision,x numeric(14,3),j jsonb,a date,tm time,"
        "ts timestamp,z timestamptz) SPLIT INTO 4 TABLETS"));
    ASSERT_OK(c.Execute(
        "INSERT INTO sandbox.scalars VALUES "
        "('00000000-0000-4000-8000-000000000001',-23,true,2.5,1.25,21.875,"
        "'{\"n\":2}','2001-02-03','04:05:06','2001-02-03 04:05:06',"
        "'2001-02-03 04:05:06+00')"));
    ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>(check)));
    ASSERT_OK(c.Execute("CREATE INDEX scalar_decimal_idx ON sandbox.scalars(x HASH) INCLUDE(id)"));
    ASSERT_OK(c.Execute("SET enable_seqscan=off"));
    ASSERT_TRUE(ASSERT_RESULT(c.HasIndexScan("SELECT id FROM sandbox.scalars WHERE x=21.8750")));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM sandbox.scalars WHERE x=21.8750")),
        1);
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.Execute("UPDATE sandbox.scalars SET x=0,j='null'::jsonb"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM sandbox.scalars WHERE x=21.875")),
        0);
    ASSERT_OK(c.Execute("ROLLBACK"));
    ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>(check)));
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.Execute("DROP SCHEMA sandbox CASCADE"));
    ASSERT_EQ(
        ASSERT_RESULT(
            c.FetchRow<int64_t>("SELECT count(*) FROM pg_namespace WHERE nspname='sandbox'")),
        0);
    ASSERT_TRUE(
        ASSERT_RESULT(c.FetchRow<bool>("SELECT to_regclass('sandbox.scalars') IS NULL "
                                       "AND to_regclass('sandbox.scalar_decimal_idx') IS NULL")));
    ASSERT_OK(c.Execute("ROLLBACK"));
    ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>(check)));
    ASSERT_TRUE(ASSERT_RESULT(c.HasIndexScan("SELECT id FROM sandbox.scalars WHERE x=21.8750")));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM sandbox.scalars WHERE x=21.8750")),
        1);
  }
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>(check)));
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<std::string>("SELECT sandbox.strip_marks(U&'caf\\00E9')")), "cafe");
}

TEST_F(ReadonlyPgServerTest, IndexBuildsOwnWrites) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("index-own-writes"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(c.Execute("CREATE TABLE index_source(k bigint PRIMARY KEY,v bigint)"));
  ASSERT_OK(c.Execute("INSERT INTO index_source VALUES(1,-1),(2,2)"));
  ASSERT_OK(c.Execute("SET enable_seqscan=off"));
  ASSERT_OK(c.Execute("SET yb_fetch_row_limit=1"));
  const std::string create = "CREATE INDEX expression_idx ON index_source((abs(v)) HASH) WHERE v>0";
  const std::string query = "SELECT k FROM index_source WHERE v>0 AND abs(v)=3";
  ASSERT_OK(c.Execute("BEGIN"));
  ASSERT_OK(c.Execute("INSERT INTO index_source VALUES(3,3)"));
  ASSERT_OK(c.Execute("SAVEPOINT before_index"));
  ASSERT_OK(c.Execute(create));
  ASSERT_TRUE(ASSERT_RESULT(c.HasIndexScan(query)));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>(query)), 3);
  ASSERT_OK(c.Execute("ROLLBACK TO before_index"));
  ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>("SELECT to_regclass('expression_idx') IS NULL")));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM index_source")), 3);
  ASSERT_OK(c.Execute(create));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>(query)), 3);
  ASSERT_OK(c.Execute("ROLLBACK"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM index_source")), 2);
  ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>("SELECT to_regclass('expression_idx') IS NULL")));
  ASSERT_OK(c.Execute(create));
  ASSERT_OK(c.Execute("UPDATE index_source SET v=3 WHERE k=1"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>(query)), 1);
  ASSERT_OK(c.Execute("BEGIN"));
  ASSERT_OK(c.Execute("UPDATE index_source SET v=-3 WHERE k=1"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRows<int64_t>(query)).size(), 0);
  ASSERT_OK(c.Execute("ROLLBACK"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>(query)), 1);
  ASSERT_OK(c.Execute("UPDATE index_source SET v=-3 WHERE k=1"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRows<int64_t>(query)).size(), 0);
}

TEST_F(ReadonlyPgServerTest, StandaloneHashRanges) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("hash-sql"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto connection = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(
      connection.Execute("CREATE TABLE scalar_hash(k bigint PRIMARY KEY) SPLIT INTO 4 TABLETS"));
  ASSERT_OK(connection.Execute(
      "CREATE TABLE composite_hash(h bigint, r bigint, "
      "PRIMARY KEY((h,r) HASH)) SPLIT INTO 4 TABLETS"));
  ASSERT_OK(connection.Execute(
      "INSERT INTO scalar_hash SELECT g::bigint FROM generate_series(-100,100) g"));
  ASSERT_OK(connection.Execute(
      "INSERT INTO scalar_hash SELECT DISTINCT k FROM ("
      "SELECT g::bigint k, yb_hash_code(g::bigint) h FROM generate_series(101,1000000) g) s "
      "WHERE h IN (0,65535) LIMIT 32"));
  ASSERT_OK(connection.Execute("INSERT INTO composite_hash SELECT k,k+1 FROM scalar_hash"));
  ASSERT_OK(connection.Execute(
      "INSERT INTO composite_hash SELECT k,k+1 FROM ("
      "SELECT g::bigint k, yb_hash_code(g::bigint,g::bigint+1) h "
      "FROM generate_series(2000000,3000000) g) s WHERE h IN (0,65535) LIMIT 32"));
  ASSERT_OK(connection.Execute("SET yb_fetch_row_limit = 7"));
  for (auto table : {"scalar_hash", "composite_hash"}) {
    const auto key = std::string(table) == "scalar_hash" ? "k" : "h";
    const auto hash = std::string(table) == "scalar_hash" ? "yb_hash_code(k)" : "yb_hash_code(h,r)";
    const auto all =
        ASSERT_RESULT(connection.FetchRows<int64_t>(Format("SELECT $0 FROM $1", key, table)));
    const std::set<int64_t> expected(all.begin(), all.end());
    std::set<int64_t> seen;
    for (int lo = 0; lo < 65536; lo += 257) {
      const auto rows = ASSERT_RESULT(connection.FetchRows<int64_t>(Format(
          "SELECT $0 FROM $1 WHERE $2 >= $3 AND $2 < $4", key, table, hash, lo,
          std::min(lo + 257, 65536))));
      for (const auto value : rows) ASSERT_TRUE(seen.insert(value).second);
    }
    ASSERT_EQ(seen, expected);
  }
  ASSERT_OK(connection.Execute("SET enable_seqscan=off"));
  for (const bool composite : {false, true}) {
    const std::string table = composite ? "indexed_composite" : "indexed_scalar";
    const std::string index = table + "_idx";
    ASSERT_OK(connection.ExecuteFormat(
        "CREATE TABLE $0(k bigint PRIMARY KEY,h bigint,r bigint) SPLIT INTO 4 TABLETS", table));
    ASSERT_OK(connection.ExecuteFormat(
        "INSERT INTO $0 SELECT $1", table,
        composite ? "h,h,r FROM composite_hash" : "k,k,k+1 FROM scalar_hash"));
    ASSERT_OK(connection.ExecuteFormat(
        "CREATE INDEX $0 ON $1($2) INCLUDE(k) SPLIT INTO 4 TABLETS", index, table,
        composite ? "(h,r) HASH" : "h HASH"));
    const std::string hash = composite ? "yb_hash_code(h,r)" : "yb_hash_code(h)";
    const auto query = Format("SELECT k FROM $0 WHERE $1 >= 0 AND $1 < 65536", table, hash);
    const auto plan = ASSERT_RESULT(connection.FetchRows<std::string>("EXPLAIN " + query));
    ASSERT_TRUE(std::any_of(plan.begin(), plan.end(), [&](const auto& line) {
      return line.find(index) != std::string::npos;
    }));
    const auto all =
        ASSERT_RESULT(connection.FetchRows<int64_t>(Format("SELECT k FROM $0", table)));
    const std::set<int64_t> expected(all.begin(), all.end());
    std::set<int64_t> seen;
    for (int lo = 0; lo < 65536; lo += 257) {
      const auto rows = ASSERT_RESULT(connection.FetchRows<int64_t>(Format(
          "SELECT k FROM $0 WHERE $1 >= $2 AND $1 < $3", table, hash, lo,
          std::min(lo + 257, 65536))));
      for (const auto value : rows) ASSERT_TRUE(seen.insert(value).second);
    }
    ASSERT_EQ(seen, expected);
  }
  ASSERT_OK(connection.Execute("SET enable_seqscan=on"));
  ASSERT_EQ(
      ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(DISTINCT yb_hash_code(k)) FROM "
                                                 "scalar_hash WHERE yb_hash_code(k) IN(0,65535)")),
      2);
  ASSERT_EQ(
      ASSERT_RESULT(connection.FetchRow<int64_t>(
          "SELECT count(DISTINCT yb_hash_code(h,r)) FROM composite_hash "
          "WHERE yb_hash_code(h,r) IN(0,65535)")),
      2);
  ASSERT_OK(connection.Execute("CREATE TABLE range_table(k bigint, v text, PRIMARY KEY(k ASC))"));
  ASSERT_OK(connection.Execute("INSERT INTO range_table VALUES (2,'two'), (1,'one')"));
  ASSERT_EQ(
      ASSERT_RESULT(connection.FetchRow<std::string>("SELECT v FROM range_table WHERE k=1")),
      "one");
}

TEST_F(ReadonlyPgServerTest, DefaultHashTabletsFollowShardFlag) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("shard-flag"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto connection = ASSERT_RESULT(Connect(*server));
  const auto tablets = [&connection](const std::string& table) -> Result<int64_t> {
    return connection.FetchRow<int64_t>(
        "SELECT num_tablets::bigint FROM yb_table_properties('" + table + "'::regclass)");
  };
  ASSERT_OK(connection.Execute("CREATE TABLE default_shards (h bigint PRIMARY KEY)"));
  ASSERT_EQ(ASSERT_RESULT(tablets("default_shards")), 4);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_num_shards_per_tserver) = 1;
  ASSERT_OK(connection.Execute("CREATE TABLE one_shard (h bigint PRIMARY KEY)"));
  ASSERT_EQ(ASSERT_RESULT(tablets("one_shard")), 1);
  ASSERT_OK(connection.Execute(
      "CREATE TABLE explicit_shards (h bigint PRIMARY KEY) SPLIT INTO 2 TABLETS"));
  ASSERT_EQ(ASSERT_RESULT(tablets("explicit_shards")), 2);
  ASSERT_OK(connection.Execute("INSERT INTO one_shard SELECT generate_series(1, 100)"));
  ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM one_shard")), 100);
}

TEST_F(ReadonlyPgServerTest, CreatesAndWritesApplicationTables) {
  const auto directory = GetTestPath("application");
  const auto fingerprint = ASSERT_RESULT(ReadonlyPgServer::CatalogFingerprint(Snapshot()));
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          directory, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto connection = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(connection.Execute(
        "CREATE TABLE local_rows (h bigint, r bigint, v bigint, "
        "PRIMARY KEY(h HASH, r ASC)) SPLIT INTO 4 TABLETS"));
    ASSERT_TRUE(ASSERT_RESULT(connection.FetchRow<bool>(
        "SELECT num_tablets=4 AND num_hash_key_columns=1 AND NOT is_colocated "
        "FROM yb_table_properties('local_rows'::regclass)")));
    ASSERT_OK(connection.Execute("INSERT INTO local_rows VALUES (1,2,3), (4,5,6)"));
    ASSERT_EQ(
        ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT sum(v)::bigint FROM local_rows")), 9);
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(connection.Execute("INSERT INTO local_rows VALUES (7,8,9)"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM local_rows")), 3);
    ASSERT_OK(connection.Execute("ROLLBACK"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM local_rows")), 2);
  }
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          directory, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto connection = ASSERT_RESULT(Connect(*server));
    ASSERT_EQ(
        ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT sum(v)::bigint FROM local_rows")), 9);
  }
  server.reset();
  ASSERT_EQ(ASSERT_RESULT(ReadonlyPgServer::CatalogFingerprint(Snapshot())), fingerprint);
}

TEST_F(ReadonlyPgServerTest, CatalogTransactions) {
  const auto fingerprint = ASSERT_RESULT(ReadonlyPgServer::CatalogFingerprint(Snapshot()));
  const auto directory = GetTestPath("catalog-transactions");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          directory, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto connection = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'local catalog commit'"));
    const std::string query = "SELECT obj_description('pg_catalog.pg_class'::regclass)";
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "local catalog commit");
    ASSERT_OK(connection.Execute("BEGIN"));
    ASSERT_OK(
        connection.Execute("COMMENT ON TABLE pg_catalog.pg_class IS 'local catalog rollback'"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "local catalog rollback");
    ASSERT_OK(connection.Execute("ROLLBACK"));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<std::string>(query)), "local catalog commit");
  }
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          directory, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto connection = ASSERT_RESULT(Connect(*server));
    ASSERT_EQ(
        ASSERT_RESULT(connection.FetchRow<std::string>(
            "SELECT obj_description('pg_catalog.pg_class'::regclass)")),
        "local catalog commit");
  }
  server.reset();
  ASSERT_EQ(ASSERT_RESULT(ReadonlyPgServer::CatalogFingerprint(Snapshot())), fingerprint);
}

TEST_F(ReadonlyPgServerTest, BootsWithoutCluster) {
  const auto fingerprint = ASSERT_RESULT(ReadonlyPgServer::CatalogFingerprint(Snapshot()));
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(GetTestPath("readonly"), Snapshot(), AllocateFreePort()));
  ASSERT_GT(server->CatalogTableCount(), 100);
  {
    auto connection = ASSERT_RESULT(Connect(*server));
    ASSERT_EQ(ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT 42::bigint")), 42);
    ASSERT_EQ(
        ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT yb_hash_code(42::bigint)::bigint")),
        48975);
    const auto before = server->ReadCount();
    const auto expected_rows =
        ASSERT_RESULT(connection.FetchRow<int64_t>("SELECT count(*) FROM pg_class"));
    ASSERT_GT(expected_rows, 100);
    ASSERT_GT(server->ReadCount(), before);
    ASSERT_OK(connection.Execute("SET yb_fetch_row_limit = 7"));
    const auto rows = ASSERT_RESULT(
        connection.FetchRows<int64_t>("SELECT oid::bigint FROM pg_class ORDER BY oid"));
    ASSERT_EQ(rows.size(), static_cast<size_t>(expected_rows));
    ASSERT_EQ(std::set<int64_t>(rows.begin(), rows.end()).size(), rows.size());
    ASSERT_NOK(connection.Execute("CREATE TABLE t(k bigint PRIMARY KEY)"));
    ASSERT_OK(connection.Execute("SET default_transaction_read_only = off"));
    ASSERT_NOK(connection.Execute("CREATE TABLE t(k bigint PRIMARY KEY)"));
  }
  server.reset();
  ASSERT_EQ(ASSERT_RESULT(ReadonlyPgServer::CatalogFingerprint(Snapshot())), fingerprint);
}

TEST_F(ReadonlyPgServerTest, RestartsAndOwnsDirectory) {
  const auto path = GetTestPath("readonly");
  auto server = ASSERT_RESULT(ReadonlyPgServer::Start(path, Snapshot(), AllocateFreePort()));
  ASSERT_NOK(ReadonlyPgServer::Start(path, Snapshot(), AllocateFreePort()).status());
  {
    auto connection = ASSERT_RESULT(Connect(*server));
    ASSERT_EQ(
        ASSERT_RESULT(connection.FetchRow<std::string>("SELECT current_database()")), "yugabyte");
  }
  server.reset();
  server = ASSERT_RESULT(ReadonlyPgServer::Start(path, Snapshot(), AllocateFreePort()));
  {
    auto connection = ASSERT_RESULT(Connect(*server));
    ASSERT_EQ(
        ASSERT_RESULT(connection.FetchRow<int64_t>(
            "SELECT count(*) FROM pg_database WHERE datname = 'yugabyte'")),
        1);
  }
  server.reset();
  ASSERT_OK(Env::Default()->RenameFile(path + "/LOCAL_READONLY", path + "/marker-real"));
  ASSERT_OK(Env::Default()->SymlinkPath(path + "/marker-real", path + "/LOCAL_READONLY"));
  ASSERT_TRUE(
      ReadonlyPgServer::Start(path, Snapshot(), AllocateFreePort()).status().IsInvalidArgument());
  ASSERT_OK(Env::Default()->DeleteFile(path + "/LOCAL_READONLY"));
  ASSERT_OK(Env::Default()->RenameFile(path + "/marker-real", path + "/LOCAL_READONLY"));
  ASSERT_OK(Env::Default()->RenameFile(path + "/LOCK", path + "/lock-real"));
  ASSERT_OK(Env::Default()->SymlinkPath(path + "/lock-real", path + "/LOCK"));
  ASSERT_TRUE(ReadonlyPgServer::Start(path, Snapshot(), AllocateFreePort()).status().IsIOError());
  ASSERT_OK(Env::Default()->DeleteFile(path + "/LOCK"));
  ASSERT_OK(Env::Default()->RenameFile(path + "/lock-real", path + "/LOCK"));
  const auto alias = GetTestPath("alias");
  ASSERT_OK(Env::Default()->SymlinkPath(path, alias));
  ASSERT_TRUE(
      ReadonlyPgServer::Start(alias, Snapshot(), AllocateFreePort()).status().IsInvalidArgument());
  ASSERT_OK(WriteStringToFileSync(Env::Default(), "wrong seed identity", path + "/LOCAL_READONLY"));
  ASSERT_TRUE(
      ReadonlyPgServer::Start(path, Snapshot(), AllocateFreePort()).status().IsInvalidArgument());
  const auto foreign = GetTestPath("foreign");
  ASSERT_OK(Env::Default()->CreateDir(foreign));
  ASSERT_OK(WriteStringToFileSync(Env::Default(), "keep", foreign + "/sentinel"));
  ASSERT_NOK(ReadonlyPgServer::Start(foreign, Snapshot(), AllocateFreePort()).status());
  faststring contents;
  ASSERT_OK(ReadFileToString(Env::Default(), foreign + "/sentinel", &contents));
  ASSERT_EQ(contents.ToString(), "keep");
}

TEST_F(ReadonlyPgServerTest, RejectsWritesAtRpcBoundary) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(GetTestPath("readonly"), Snapshot(), AllocateFreePort()));
  rpc::MessengerBuilder builder("readonly-test-client");
  builder.UseDefaultConnectionContextFactory();
  auto messenger = ASSERT_RESULT(builder.Build());
  auto shutdown = ScopeExit([&] { messenger->Shutdown(); });
  rpc::ProxyCache cache(messenger.get());
  tserver::PgClientServiceProxy proxy(&cache, server->RpcAddress());
  tserver::PgHeartbeatRequestPB heartbeat;
  heartbeat.set_pid(getpid());
  tserver::PgHeartbeatResponsePB heartbeat_response;
  rpc::RpcController controller;
  controller.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(proxy.Heartbeat(heartbeat, &heartbeat_response, &controller));
  ASSERT_FALSE(heartbeat_response.has_status());
  ASSERT_GT(heartbeat_response.session_id(), 0);
  ASSERT_TRUE(heartbeat_response.instance_id().empty());
  ThreadSafeArena arena;
  tserver::LWPgPerformRequestPB request(&arena);
  tserver::LWPgPerformResponsePB response(&arena);
  request.set_session_id(heartbeat_response.session_id());
  auto* write = request.add_ops()->mutable_write();
  write->dup_table_id(kPgYbCatalogVersionTableId);
  write->set_stmt_type(PgsqlWriteRequestPB::PGSQL_DELETE);
  controller.Reset();
  controller.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(proxy.Perform(request, &response, &controller));
  ASSERT_TRUE(StatusFromPB(response.status()).IsNotSupported());
  ASSERT_TRUE(response.responses().empty());
  const SequenceRpc sequences(proxy);
  for (const auto& status : sequences.All(heartbeat_response.session_id(), 16384, 16385)) {
    ASSERT_TRUE(status.IsNotSupported()) << status;
    ASSERT_STR_CONTAINS(status.ToString(), "Frozen catalog rejects sequence");
  }
}

TEST_F(ReadonlyPgServerTest, BackwardScans) {
  const auto path = GetTestPath("backward-scans");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  std::vector<int64_t> keys;
  const auto check = [&keys](pgwrapper::PGConn& c) {
    ASSERT_OK(c.Execute("SET yb_fetch_row_limit=2"));
    ASSERT_OK(c.Execute("SET enable_seqscan=off"));
    ASSERT_OK(c.Execute(
        "PREPARE backwards(bigint) AS SELECT r FROM reverse_rows "
        "WHERE h=$1 AND r>=3 AND r<15 ORDER BY r DESC"));
    for (const auto h : keys) {
      const auto rows = ASSERT_RESULT(c.FetchRows<int64_t>(Format("EXECUTE backwards($0)", h)));
      const std::vector<int64_t> expected{14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3};
      ASSERT_EQ(rows, expected) << "hash key " << h;
      const auto query =
          Format("SELECT r FROM reverse_rows WHERE h=$0 AND v>=85 AND v<98 ORDER BY v DESC", h);
      const auto plan = ASSERT_RESULT(c.FetchRows<std::string>("EXPLAIN " + query));
      ASSERT_FALSE(plan.empty());
      ASSERT_STR_CONTAINS(plan.front(), "Backward");
      ASSERT_STR_CONTAINS(plan.front(), "reverse_by_value");
      const auto indexed = ASSERT_RESULT(c.FetchRows<int64_t>(query));
      const std::vector<int64_t> index_expected{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
      ASSERT_EQ(indexed, index_expected);
    }
    const std::vector<int64_t> expected{10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRows<int64_t>("SELECT k FROM reverse_range ORDER BY k DESC")),
        expected);
  };
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(c.Execute(
        "CREATE TABLE reverse_rows(h bigint,r bigint,v bigint,PRIMARY KEY(h HASH,r ASC)) "
        "SPLIT INTO 4 TABLETS"));
    keys = ASSERT_RESULT(c.FetchRows<int64_t>(
        "SELECT min(k)::bigint FROM generate_series(0,1000000) k "
        "WHERE yb_hash_code(k::bigint) IN (0,16383,16384,32767,32768,49151,49152,65535) "
        "GROUP BY yb_hash_code(k::bigint) ORDER BY yb_hash_code(k::bigint)"));
    ASSERT_EQ(keys.size(), 8);
    for (const auto h : keys) {
      ASSERT_OK(c.ExecuteFormat(
          "INSERT INTO reverse_rows SELECT $0,r,100-r FROM generate_series(1,17) r", h));
    }
    ASSERT_OK(c.Execute("CREATE INDEX reverse_by_value ON reverse_rows(h HASH,v ASC) INCLUDE(r)"));
    ASSERT_OK(c.Execute("CREATE TABLE reverse_range(k bigint, PRIMARY KEY(k ASC))"));
    ASSERT_OK(c.Execute("INSERT INTO reverse_range SELECT generate_series(1,10)"));
    ASSERT_NO_FATALS(check(c));
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.ExecuteFormat("INSERT INTO reverse_rows VALUES($0,30,70)", keys.back()));
    const auto last =
        Format("SELECT r FROM reverse_rows WHERE h=$0 ORDER BY r DESC LIMIT 1", keys.back());
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>(last)), 30);
    ASSERT_OK(c.Execute("SAVEPOINT changed"));
    ASSERT_OK(c.ExecuteFormat("UPDATE reverse_rows SET r=31 WHERE h=$0 AND r=30", keys.back()));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>(last)), 31);
    ASSERT_OK(c.Execute("ROLLBACK TO changed"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>(last)), 30);
    ASSERT_OK(c.Execute("ROLLBACK"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>(last)), 17);
  }
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_NO_FATALS(check(c));
}

TEST_F(ReadonlyPgServerTest, UniqueViolationSqlState) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("error-reporting"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(
      c.Execute("CREATE TABLE error_rows(h bigint,r bigint,v text,PRIMARY KEY(h HASH,r ASC))"));
  ASSERT_OK(c.Execute("CREATE UNIQUE INDEX error_value ON error_rows(h HASH,v ASC) INCLUDE(r)"));
  ASSERT_OK(c.Execute(
      "PREPARE insert_row(bigint,bigint,text) AS "
      "INSERT INTO error_rows VALUES($1,$2,$3) RETURNING h,r"));
  ASSERT_OK(c.Execute("BEGIN"));
  ASSERT_RESULT(c.Fetch("EXECUTE insert_row(7,1,'first')"));
  ASSERT_OK(c.Execute("SAVEPOINT prior"));
  ASSERT_OK(c.Execute("UPDATE error_rows SET v='discarded' WHERE h=7 AND r=1"));
  ASSERT_OK(c.Execute("ROLLBACK TO prior"));
  ASSERT_OK(c.Execute("RELEASE prior"));
  for (const auto& values : {"(7,2,'first')", "(7,1,'other')"}) {
    ASSERT_OK(c.Execute("SAVEPOINT conflict"));
    const auto status = ResultToStatus(c.Fetch("EXECUTE insert_row" + std::string(values)));
    ASSERT_NOK(status);
    ASSERT_EQ(PgsqlError(status), YBPgErrorCode::YB_PG_UNIQUE_VIOLATION) << status;
    ASSERT_STR_CONTAINS(status.ToString(), "duplicate key value violates unique constraint");
    const auto blocked = ResultToStatus(c.Fetch("SELECT * FROM error_rows"));
    ASSERT_NOK(blocked);
    ASSERT_EQ(PgsqlError(blocked), YBPgErrorCode::YB_PG_IN_FAILED_SQL_TRANSACTION) << blocked;
    ASSERT_OK(c.Execute("ROLLBACK TO conflict"));
    ASSERT_OK(c.Execute("RELEASE conflict"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM error_rows")), 1);
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<std::string>("SELECT v FROM error_rows")), "first");
  }
  ASSERT_OK(c.Execute("COMMIT"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM error_rows")), 1);
  const auto duplicate = ResultToStatus(c.Fetch("EXECUTE insert_row(7,3,'first')"));
  ASSERT_NOK(duplicate);
  ASSERT_EQ(PgsqlError(duplicate), YBPgErrorCode::YB_PG_UNIQUE_VIOLATION) << duplicate;
  ASSERT_RESULT(c.Fetch("EXECUTE insert_row(7,3,'third')"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM error_rows")), 2);
}

TEST_F(ReadonlyPgServerTest, RejectsForeignPostgresDirectory) {
  const auto source = GetTestPath("source");
  auto server = ASSERT_RESULT(ReadonlyPgServer::Start(source, Snapshot(), AllocateFreePort()));
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT 1::bigint")), 1);
  }
  server.reset();
  const auto versioned = std::filesystem::canonical(source + "/pg_data");
  faststring original;
  ASSERT_OK(ReadFileToString(Env::Default(), source + "/pg_data/ysql_pg.conf", &original));
  for (int mode = 0; mode < 3; ++mode) {
    const auto copy = GetTestPath(Format("source-copy-$0", mode));
    ASSERT_TRUE(std::filesystem::create_directory(copy));
    ASSERT_TRUE(std::filesystem::copy_file(source + "/LOCAL_READONLY", copy + "/LOCAL_READONLY"));
    if (mode == 1) {
      std::filesystem::create_directory_symlink(
          versioned, std::filesystem::path(copy) / versioned.filename());
    } else {
      std::filesystem::create_directory_symlink(
          mode == 0 ? versioned : std::filesystem::path(source) / "missing", copy + "/pg_data");
    }
    const auto status =
        ResultToStatus(ReadonlyPgServer::Start(copy, Snapshot(), AllocateFreePort()));
    ASSERT_TRUE(status.IsInvalidArgument()) << status;
    ASSERT_STR_CONTAINS(status.ToString(), mode == 1 ? "not a plain directory" : "does not belong");
    faststring unchanged;
    ASSERT_OK(ReadFileToString(Env::Default(), source + "/pg_data/ysql_pg.conf", &unchanged));
    ASSERT_EQ(original, unchanged);
  }
  ASSERT_TRUE(std::filesystem::remove(source + "/pg_data"));
  std::filesystem::create_directory_symlink(versioned.filename(), source + "/pg_data");
  server = ASSERT_RESULT(ReadonlyPgServer::Start(source, Snapshot(), AllocateFreePort()));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT 42::bigint")), 42);
}

TEST_F(ReadonlyPgServerTest, LargeNativeTransaction) {
  const auto path = GetTestPath("large-native-txn");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(c.Execute("CREATE TABLE payloads(k bigint PRIMARY KEY,v text)"));
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(
        c.Execute("INSERT INTO payloads SELECT n,repeat('x',65536) FROM generate_series(1,400) n"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>("SELECT sum(length(v))::bigint FROM payloads")),
        400 * 65536);
    ASSERT_OK(c.Execute("SAVEPOINT before_delete"));
    ASSERT_OK(c.Execute("DELETE FROM payloads WHERE k<=200"));
    ASSERT_OK(c.Execute("ROLLBACK TO before_delete"));
    ASSERT_OK(c.Execute("COMMIT"));
    ASSERT_NOK(c.Execute("INSERT INTO payloads VALUES(401,repeat('y',17*1024*1024))"));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM payloads")), 400);
  }
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<int64_t>("SELECT sum(length(v))::bigint FROM payloads")),
      400 * 65536);
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<std::string>("SELECT left(v,1) FROM payloads WHERE k=400")), "x");
}

TEST_F(ReadonlyPgServerTest, DropsDatabasesAndRetainsOtherSlots) {
  const auto path = GetTestPath("drop-databases");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto admin = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(admin.Execute("CREATE DATABASE removed"));
    ASSERT_OK(admin.Execute("CREATE DATABASE retained"));
    auto retained = ASSERT_RESULT(Connect(*server, "retained"));
    ASSERT_OK(retained.Execute("CREATE TABLE rows(k bigint PRIMARY KEY,v text)"));
    ASSERT_OK(retained.Execute("INSERT INTO rows VALUES(1,'retained')"));
    const auto old_oid = ASSERT_RESULT(
        admin.FetchRow<int64_t>("SELECT oid::bigint FROM pg_database WHERE datname='removed'"));
    {
      auto removed = ASSERT_RESULT(Connect(*server, "removed"));
      ASSERT_OK(removed.Execute("CREATE TABLE rows(k bigint PRIMARY KEY,v text)"));
      ASSERT_OK(removed.Execute("CREATE INDEX rows_value ON rows(v HASH)"));
      ASSERT_OK(removed.Execute("INSERT INTO rows VALUES(1,'removed')"));
      ASSERT_NOK(admin.Execute("DROP DATABASE removed"));
      ASSERT_EQ(ASSERT_RESULT(removed.FetchRow<std::string>("SELECT v FROM rows")), "removed");
    }
    ASSERT_OK(admin.Execute("DROP DATABASE removed"));
    ASSERT_EQ(
        ASSERT_RESULT(
            admin.FetchRow<int64_t>("SELECT count(*) FROM pg_database WHERE datname='removed'")),
        0);
    ASSERT_OK(retained.Execute("COMMENT ON TABLE rows IS 'still present'"));
    ASSERT_EQ(ASSERT_RESULT(retained.FetchRow<std::string>("SELECT v FROM rows")), "retained");
    ASSERT_OK(admin.Execute("CREATE DATABASE replacement"));
    ASSERT_OK(admin.Execute("CREATE DATABASE removed"));
    ASSERT_GT(
        ASSERT_RESULT(
            admin.FetchRow<int64_t>("SELECT oid::bigint FROM pg_database WHERE datname='removed'")),
        old_oid);
    auto fresh = ASSERT_RESULT(Connect(*server, "removed"));
    ASSERT_TRUE(ASSERT_RESULT(fresh.FetchRow<bool>("SELECT to_regclass('rows') IS NULL")));
    ASSERT_EQ(ASSERT_RESULT(retained.FetchRow<std::string>("SELECT v FROM rows")), "retained");
  }
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto retained = ASSERT_RESULT(Connect(*server, "retained"));
  ASSERT_EQ(ASSERT_RESULT(retained.FetchRow<std::string>("SELECT v FROM rows")), "retained");
  auto fresh = ASSERT_RESULT(Connect(*server, "removed"));
  ASSERT_TRUE(ASSERT_RESULT(fresh.FetchRow<bool>("SELECT to_regclass('rows') IS NULL")));
}

TEST_F(ReadonlyPgServerTest, CreatesDatabases) {
  const auto path = GetTestPath("databases");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(c.Execute("CREATE DATABASE local_first"));
    ASSERT_OK(c.Execute("CREATE DATABASE local_second TEMPLATE template0"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>(
            "SELECT count(*) FROM pg_database WHERE datname IN ('local_first','local_second')")),
        2);
    const auto duplicate = c.Execute("CREATE DATABASE local_first");
    ASSERT_NOK(duplicate);
    ASSERT_EQ(PgsqlError(duplicate), YBPgErrorCode::YB_PG_DUPLICATE_DATABASE) << duplicate;
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_NOK(c.Execute("CREATE DATABASE forbidden"));
    ASSERT_OK(c.Execute("ROLLBACK"));
    ASSERT_NOK(c.Execute("CREATE DATABASE unsupported TEMPLATE local_first"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>(
            "SELECT count(*) FROM pg_database WHERE datname IN ('forbidden','unsupported')")),
        0);
    ASSERT_OK(c.Execute("CREATE TABLE isolated(k bigint PRIMARY KEY,v text)"));
    ASSERT_OK(c.Execute("INSERT INTO isolated VALUES(1,'source')"));
  }
  server.reset();
  for (const auto* name : {"local_first", "local_second"}) {
    server = ASSERT_RESULT(
        ReadonlyPgServer::Start(
            path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
    {
      auto c = ASSERT_RESULT(Connect(*server, name));
      ASSERT_EQ(ASSERT_RESULT(c.FetchRow<std::string>("SELECT current_database()")), name);
      ASSERT_GT(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM pg_class")), 300);
      ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>("SELECT to_regclass('public.isolated') IS NULL")));
      ASSERT_OK(
          c.Execute("CREATE TABLE isolated(k bigint PRIMARY KEY,v text) SPLIT INTO 4 TABLETS"));
      ASSERT_OK(c.ExecuteFormat("INSERT INTO isolated VALUES(1,'$0')", name));
      ASSERT_OK(c.Execute("CREATE UNIQUE INDEX isolated_v ON isolated(v)"));
      ASSERT_OK(c.Execute("BEGIN"));
      ASSERT_OK(c.Execute("UPDATE isolated SET v='discarded' WHERE k=1"));
      ASSERT_OK(c.Execute("ROLLBACK"));
      ASSERT_EQ(ASSERT_RESULT(c.FetchRow<std::string>("SELECT v FROM isolated WHERE k=1")), name);
    }
    server.reset();
  }
  for (const auto* name : {"yugabyte", "local_first", "local_second"}) {
    server = ASSERT_RESULT(
        ReadonlyPgServer::Start(
            path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
    {
      auto c = ASSERT_RESULT(Connect(*server, name));
      ASSERT_EQ(
          ASSERT_RESULT(c.FetchRow<std::string>("SELECT v FROM isolated WHERE k=1")),
          std::string(name) == "yugabyte" ? "source" : name);
    }
    server.reset();
  }
}

TEST_F(ReadonlyPgServerTest, DatabaseCreationFailureAtomicity) {
  const auto path = GetTestPath("database-failure");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  uint32_t failed_oid = 0;
  {
    auto c = ASSERT_RESULT(Connect(*server));
    sync->SetCallBack("PgCatalogTransaction::DatabaseCopied", [&](void* oid) {
      failed_oid = *static_cast<uint32_t*>(oid);
    });
    sync->SetCallBack("PgCatalogTransaction::DatabaseCopyStatus", [](void* status) {
      *static_cast<Status*>(status) = STATUS(Aborted, "Injected database copy failure");
    });
    sync->EnableProcessing();
    ASSERT_NOK(c.Execute("CREATE DATABASE failed_copy"));
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
    ASSERT_GT(failed_oid, 0);
    ASSERT_EQ(
        ASSERT_RESULT(
            c.FetchRow<int64_t>("SELECT count(*) FROM pg_database WHERE datname='failed_copy'")),
        0);
    ASSERT_OK(c.Execute("CREATE DATABASE completed"));
  }
  server.reset();
  {
    auto store =
        ASSERT_RESULT(PgCatalogTransaction::Open(path + "/catalog", Snapshot() + "/rocksdb"));
    for (const auto& table : ASSERT_RESULT(store->CatalogTables())) {
      ASSERT_EQ(table.namespace_name(), "completed");
    }
    std::unique_ptr<rocksdb::Iterator> rows(
        ASSERT_RESULT(store->ReadDB())->NewIterator(rocksdb::ReadOptions()));
    for (rows->SeekToFirst(); rows->Valid(); rows->Next()) {
      dockv::DocKey key;
      auto bytes = rows->key();
      ASSERT_OK(key.DecodeFrom(&bytes));
      if (!key.cotable_id().IsNil()) {
        ASSERT_NE(
            ASSERT_RESULT(GetPgsqlDatabaseOidByTableId(key.cotable_id().ToHexString())),
            failed_oid);
      }
    }
    ASSERT_OK(rows->status());
  }
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  {
    auto c = ASSERT_RESULT(Connect(*server));
    sync->SetCallBack("PgCatalogTransaction::CommitStatus", [](void* status) {
      *static_cast<Status*>(status) = STATUS(IOError, "Ambiguous database commit acknowledgement");
    });
    sync->EnableProcessing();
    ASSERT_NOK(c.Execute("CREATE DATABASE ambiguous_db"));
    ASSERT_NOK(c.Fetch("SELECT count(*) FROM pg_database"));
    // A failed abort may terminate the backend; neither outcome can clear the storage fence.
    (void)c.Execute("ROLLBACK");
    ASSERT_NOK(c.Fetch("SELECT count(*) FROM pg_database"));
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  }
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server, "ambiguous_db"));
  ASSERT_OK(c.Execute("CREATE TABLE recovered(k bigint PRIMARY KEY)"));
  ASSERT_OK(c.Execute("INSERT INTO recovered VALUES(7)"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT k FROM recovered")), 7);
}

TEST_F(ReadonlyPgServerTest, DatabaseTemplatesAndCatalogSlots) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("database-slots"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  rpc::MessengerBuilder builder("database-slot-client");
  builder.UseDefaultConnectionContextFactory();
  auto messenger = ASSERT_RESULT(builder.Build());
  auto cleanup = ScopeExit([&] { messenger->Shutdown(); });
  rpc::ProxyCache cache(messenger.get());
  tserver::PgClientServiceProxy proxy(&cache, server->RpcAddress());
  const auto slots = [&]() -> Result<std::map<uint32_t, int32_t>> {
    tserver::PgGetTserverCatalogVersionInfoRequestPB request;
    tserver::PgGetTserverCatalogVersionInfoResponsePB response;
    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy.GetTserverCatalogVersionInfo(request, &response, &controller));
    if (response.has_status()) RETURN_NOT_OK(StatusFromPB(response.status()));
    std::map<uint32_t, int32_t> result;
    for (const auto& entry : response.entries()) result.emplace(entry.db_oid(), entry.shm_index());
    SCHECK_EQ(result.size(), response.num_entries(), IllegalState, "Catalog slot count mismatch");
    return result;
  };
  const auto initial = ASSERT_RESULT(slots());
  int64_t pid;
  int64_t copied_oid;
  {
    auto c = ASSERT_RESULT(Connect(*server, "template1"));
    pid = ASSERT_RESULT(c.FetchRow<int64_t>("SELECT pg_backend_pid()::bigint"));
    ASSERT_OK(c.Execute("COMMENT ON TABLE pg_class IS 'copied catalog'"));
    ASSERT_OK(c.Execute(
        "CREATE FUNCTION public.copied_function(v bigint) RETURNS bigint "
        "LANGUAGE sql IMMUTABLE AS 'SELECT v+1'"));
    copied_oid = ASSERT_RESULT(
        c.FetchRow<int64_t>("SELECT 'public.copied_function(bigint)'::regprocedure::oid::bigint"));
    ASSERT_OK(c.Execute("CREATE DATABASE inherited"));
    ASSERT_NOK(c.Execute("CREATE TABLE template_data(k bigint PRIMARY KEY)"));
    ASSERT_NOK(c.Execute("CREATE DATABASE unsupported_colocation WITH COLOCATION=true"));
  }
  ASSERT_OK(WaitFor(
      [&] { return kill(static_cast<pid_t>(pid), 0) != 0 && errno == ESRCH; }, 5s,
      "template backend exit"));
  auto c = ASSERT_RESULT(Connect(*server, "inherited"));
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<std::string>("SELECT obj_description('pg_class'::regclass)")),
      "copied catalog");
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT public.copied_function(41)")), 42);
  ASSERT_OK(c.Execute(
      "CREATE FUNCTION public.new_function(v bigint) RETURNS bigint "
      "LANGUAGE sql IMMUTABLE AS 'SELECT v+2'"));
  ASSERT_GT(
      ASSERT_RESULT(
          c.FetchRow<int64_t>("SELECT 'public.new_function(bigint)'::regprocedure::oid::bigint")),
      copied_oid);
  const auto inherited = ASSERT_RESULT(slots());
  ASSERT_EQ(inherited.size(), initial.size() + 1);
  for (const auto& [oid, index] : initial) ASSERT_EQ(inherited.at(oid), index);
  ASSERT_OK(c.Execute("CREATE DATABASE next_database TEMPLATE template0"));
  const auto after = ASSERT_RESULT(slots());
  ASSERT_EQ(after.size(), inherited.size() + 1);
  for (const auto& [oid, index] : inherited) ASSERT_EQ(after.at(oid), index);
  ASSERT_OK(c.Execute("CREATE TABLE still_connected(k bigint PRIMARY KEY)"));
  ASSERT_OK(c.Execute("INSERT INTO still_connected VALUES(1)"));
  ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT k FROM still_connected")), 1);
}

TEST_F(ReadonlyPgServerTest, RejectsIncompleteDatabasePublication) {
  const auto path = GetTestPath("incomplete-database");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  server.reset();
  tserver::ExportedTabletMetadataChanges seed;
  ASSERT_OK(
      pb_util::ReadPBContainerFromPath(
          Env::Default(), Snapshot() + "/exported_tablet_metadata_changes", &seed));
  std::vector<tablet::TableInfoPB> sources;
  const auto collect = [&](const tablet::TableInfoPB& table) -> Status {
    // template0 (OID 4) has no shared catalog descriptors.
    if (VERIFY_RESULT(GetPgsqlDatabaseOidByTableId(table.table_id())) == 4) {
      sources.push_back(table);
    }
    return Status::OK();
  };
  for (const auto& change : seed.metadata_changes()) {
    if (change.has_add_table()) ASSERT_OK(collect(change.add_table()));
    for (const auto& table : change.add_multiple_tables()) ASSERT_OK(collect(table));
  }
  ASSERT_FALSE(sources.empty());
  {
    auto store =
        ASSERT_RESULT(PgCatalogTransaction::Open(path + "/catalog", Snapshot() + "/rocksdb"));
    ASSERT_OK(store->Begin());
    ASSERT_OK(store->CreateDatabase(1000000000, "incomplete_database", sources, 1));
    ASSERT_OK(store->Commit());
  }
  const auto status = ResultToStatus(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  ASSERT_TRUE(status.IsCorruption()) << status;
  ASSERT_STR_CONTAINS(status.ToString(), "missing its pg_database row");
}

TEST_F(ReadonlyPgServerTest, SequenceBackedSchemaLifecycle) {
  const auto path = GetTestPath("sequence-schema");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  const auto insert_item = [](pgwrapper::PGConn& c, const std::string& name) {
    return c.FetchRow<int64_t>("INSERT INTO seq_items(name) VALUES('" + name + "') RETURNING id");
  };
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(c.Execute("CREATE TABLE seq_items(id bigserial PRIMARY KEY,name text UNIQUE)"));
    ASSERT_EQ(ASSERT_RESULT(insert_item(c, "alpha")), 1);
    ASSERT_EQ(ASSERT_RESULT(insert_item(c, "beta")), 2);
    const std::vector<std::string> duplicates = {
        "INSERT INTO seq_items(name) VALUES('alpha')", "INSERT INTO seq_items VALUES(1,'other')"};
    for (const auto& statement : duplicates) {
      const auto status = c.Execute(statement);
      ASSERT_NOK(status);
      ASSERT_EQ(PgsqlError(status), YBPgErrorCode::YB_PG_UNIQUE_VIOLATION) << status;
    }
    // The rejected default still consumed 3; sequence values are never handed back.
    ASSERT_EQ(ASSERT_RESULT(insert_item(c, "gamma")), 4);
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.Execute(
        "CREATE TABLE seq_events(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
        "item_id bigint NOT NULL)"));
    ASSERT_OK(c.Execute("INSERT INTO seq_events(item_id) SELECT id FROM seq_items"));
    ASSERT_OK(c.Execute("SAVEPOINT before_scratch"));
    ASSERT_OK(c.Execute("CREATE TABLE seq_scratch(id serial PRIMARY KEY)"));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int32_t>("INSERT INTO seq_scratch DEFAULT VALUES RETURNING id")),
        1);
    ASSERT_OK(c.Execute("ROLLBACK TO before_scratch"));
    ASSERT_OK(c.Execute("ALTER TABLE seq_items ADD COLUMN note text DEFAULT 'migrated'"));
    ASSERT_OK(c.Execute("COMMIT"));
    ASSERT_TRUE(ASSERT_RESULT(c.FetchRow<bool>(
        "SELECT to_regclass('seq_scratch') IS NULL AND "
        "to_regclass('seq_scratch_id_seq') IS NULL")));
    ASSERT_EQ(ASSERT_RESULT(c.FetchRow<int64_t>("SELECT max(id) FROM seq_events")), 3);
    const auto generated = c.Execute("INSERT INTO seq_events VALUES(10,1)");
    ASSERT_NOK(generated);
    ASSERT_EQ(PgsqlError(generated), YBPgErrorCode::YB_PG_GENERATED_ALWAYS) << generated;
  }
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  // A restart may skip the rest of a cached range, but must not reissue values.
  ASSERT_GT(ASSERT_RESULT(insert_item(c, "delta")), 4);
  ASSERT_GT(
      ASSERT_RESULT(c.FetchRow<int64_t>("INSERT INTO seq_events(item_id) VALUES(1) RETURNING id")),
      3);
  ASSERT_EQ(
      ASSERT_RESULT(c.FetchRow<int64_t>("SELECT count(*) FROM seq_items WHERE note='migrated'")),
      4);
  ASSERT_TRUE(
      ASSERT_RESULT(c.FetchRow<bool>("SELECT count(DISTINCT id)=count(*) FROM seq_items")));
}

TEST_F(ReadonlyPgServerTest, SequenceValuesAreNontransactional) {
  const auto path = GetTestPath("sequence-values");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  std::set<int64_t> issued;
  const auto next = [&issued](pgwrapper::PGConn& c) -> Result<int64_t> {
    const auto value = VERIFY_RESULT(c.FetchRow<int64_t>("SELECT nextval('seq_counter')"));
    SCHECK(issued.insert(value).second, IllegalState, "Sequence value issued twice");
    return value;
  };
  {
    auto first = ASSERT_RESULT(Connect(*server));
    auto second = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(first.Execute("CREATE SEQUENCE seq_counter"));
    // ysql_sequence_cache_minval=100 gives each connection its own 100-value range.
    ASSERT_EQ(
        ASSERT_RESULT(first.FetchRow<int64_t>(
            "SELECT seqcache FROM pg_sequence WHERE seqrelid='seq_counter'::regclass")),
        100);
    ASSERT_EQ(ASSERT_RESULT(next(first)), 1);
    ASSERT_EQ(ASSERT_RESULT(next(second)), 101);
    ASSERT_OK(first.Execute("BEGIN"));
    ASSERT_EQ(ASSERT_RESULT(next(first)), 2);
    ASSERT_OK(first.Execute("ROLLBACK"));
    ASSERT_EQ(ASSERT_RESULT(next(first)), 3);
    ASSERT_OK(second.Execute("BEGIN"));
    ASSERT_EQ(ASSERT_RESULT(second.FetchRow<int64_t>("SELECT setval('seq_counter',500)")), 500);
    ASSERT_OK(second.Execute("ROLLBACK"));
    ASSERT_EQ(ASSERT_RESULT(next(second)), 501);
    ASSERT_EQ(ASSERT_RESULT(next(first)), 4);
    ASSERT_EQ(
        ASSERT_RESULT(first.FetchRow<int64_t>("SELECT setval('seq_counter',700,false)")), 700);
    ASSERT_OK(first.Execute("BEGIN"));
    ASSERT_OK(first.Execute("SAVEPOINT s"));
    ASSERT_EQ(ASSERT_RESULT(next(first)), 700);
    ASSERT_OK(first.Execute("ROLLBACK TO s"));
    ASSERT_EQ(ASSERT_RESULT(next(first)), 701);
    ASSERT_OK(first.Execute("COMMIT"));
    ASSERT_EQ(ASSERT_RESULT(first.FetchRow<int64_t>("SELECT currval('seq_counter')")), 701);
    for (int i = 0; i < 5; ++i) {
      ASSERT_RESULT(next(first));
      ASSERT_RESULT(next(second));
    }
  }
  const auto high = *issued.rbegin();
  server.reset();
  server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  auto first = ASSERT_RESULT(Connect(*server));
  auto second = ASSERT_RESULT(Connect(*server));
  ASSERT_GT(ASSERT_RESULT(next(first)), high);
  ASSERT_GT(ASSERT_RESULT(next(second)), high);
  ASSERT_RESULT(next(first));
}

TEST_F(ReadonlyPgServerTest, BoundedSequenceLimits) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("sequence-bounds"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto first = ASSERT_RESULT(Connect(*server));
  auto second = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(first.Execute("CREATE SEQUENCE seq_up MAXVALUE 3"));
  ASSERT_OK(first.Execute("CREATE SEQUENCE seq_down INCREMENT -2 MINVALUE -5 MAXVALUE -1"));
  ASSERT_OK(first.Execute("CREATE SEQUENCE seq_ring MAXVALUE 3 CYCLE"));
  const auto take = [](pgwrapper::PGConn& c, const std::string& sequence, int count) {
    return c.FetchRows<int64_t>(Format(
        "SELECT nextval('$0') FROM generate_series(1,$1)", sequence, count));
  };
  const auto exhausted = [](pgwrapper::PGConn& c, const std::string& sequence) {
    const auto status = ResultToStatus(c.Fetch("SELECT nextval('" + sequence + "')"));
    ASSERT_NOK(status);
    ASSERT_EQ(PgsqlError(status), YBPgErrorCode::YB_PG_SEQUENCE_GENERATOR_LIMIT_EXCEEDED)
        << status;
  };
  ASSERT_EQ(ASSERT_RESULT(take(first, "seq_up", 3)), (std::vector<int64_t>{1, 2, 3}));
  ASSERT_NO_FATALS(exhausted(first, "seq_up"));
  ASSERT_NO_FATALS(exhausted(second, "seq_up"));
  ASSERT_EQ(ASSERT_RESULT(take(first, "seq_down", 3)), (std::vector<int64_t>{-1, -3, -5}));
  ASSERT_NO_FATALS(exhausted(first, "seq_down"));
  ASSERT_NO_FATALS(exhausted(second, "seq_down"));
  ASSERT_EQ(ASSERT_RESULT(take(first, "seq_ring", 5)), (std::vector<int64_t>{1, 2, 3, 1, 2}));
  // The exhausted session refetches from shared state after another session's setval.
  ASSERT_EQ(ASSERT_RESULT(second.FetchRow<int64_t>("SELECT setval('seq_up',2)")), 2);
  ASSERT_EQ(ASSERT_RESULT(take(first, "seq_up", 1)), std::vector<int64_t>{3});
  ASSERT_NO_FATALS(exhausted(first, "seq_up"));
}

TEST_F(ReadonlyPgServerTest, SequenceTuplesDeletedOnDrop) {
  const auto path = GetTestPath("sequence-drop");
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          path, Snapshot(), AllocateFreePort(), CatalogMode::kSerializedTransactions));
  using Identity = std::pair<uint32_t, uint32_t>;
  const auto identity = [](pgwrapper::PGConn& c, const std::string& sequence) -> Result<Identity> {
    const auto database = VERIFY_RESULT(c.FetchRow<int64_t>(
        "SELECT oid::bigint FROM pg_database WHERE datname=current_database()"));
    const auto relation =
        VERIFY_RESULT(c.FetchRow<int64_t>(Format("SELECT '$0'::regclass::oid::bigint", sequence)));
    return Identity(static_cast<uint32_t>(database), static_cast<uint32_t>(relation));
  };
  const auto next = [](pgwrapper::PGConn& c, const std::string& sequence) {
    return c.FetchRow<int64_t>(Format("SELECT nextval('$0')", sequence));
  };
  std::vector<Identity> dropped;
  Identity kept;
  std::tuple<int64_t, bool> kept_state;
  {
    auto c = ASSERT_RESULT(Connect(*server));
    ASSERT_OK(c.Execute("CREATE SEQUENCE seq_dropped"));
    ASSERT_OK(c.Execute("CREATE SEQUENCE seq_kept"));
    ASSERT_OK(c.Execute("CREATE TABLE seq_owner(id bigserial PRIMARY KEY,v text)"));
    for (const auto* sequence : {"seq_dropped", "seq_kept", "seq_owner_id_seq"}) {
      ASSERT_EQ(ASSERT_RESULT(next(c, sequence)), 1);
    }
    dropped.push_back(ASSERT_RESULT(identity(c, "seq_dropped")));
    dropped.push_back(ASSERT_RESULT(identity(c, "seq_owner_id_seq")));
    kept = ASSERT_RESULT(identity(c, "seq_kept"));
    // Rolling back discards the postponed tuple deletion.
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.Execute("DROP SEQUENCE seq_kept"));
    ASSERT_OK(c.Execute("DROP TABLE seq_owner"));
    ASSERT_OK(c.Execute("ROLLBACK"));
    kept_state =
        ASSERT_RESULT((c.FetchRow<int64_t, bool>("SELECT last_value,is_called FROM seq_kept")));
    ASSERT_TRUE(std::get<1>(kept_state));
    ASSERT_EQ(ASSERT_RESULT(next(c, "seq_kept")), 2);
    ASSERT_EQ(
        ASSERT_RESULT(
            c.FetchRow<int64_t>("INSERT INTO seq_owner(v) VALUES('kept') RETURNING id")),
        2);
    ASSERT_OK(c.Execute("DROP SEQUENCE seq_dropped"));
    ASSERT_OK(c.Execute("DROP TABLE seq_owner"));
    ASSERT_OK(c.Execute("CREATE DATABASE seq_drop_db"));
    {
      auto local = ASSERT_RESULT(Connect(*server, "seq_drop_db"));
      ASSERT_OK(local.Execute("CREATE SEQUENCE seq_local"));
      ASSERT_OK(local.Execute("CREATE TABLE seq_local_rows(id serial PRIMARY KEY)"));
      ASSERT_EQ(ASSERT_RESULT(next(local, "seq_local")), 1);
      ASSERT_EQ(
          ASSERT_RESULT(
              local.FetchRow<int32_t>("INSERT INTO seq_local_rows DEFAULT VALUES RETURNING id")),
          1);
      dropped.push_back(ASSERT_RESULT(identity(local, "seq_local")));
      dropped.push_back(ASSERT_RESULT(identity(local, "seq_local_rows_id_seq")));
    }
    ASSERT_OK(c.Execute("DROP DATABASE seq_drop_db"));
  }
  server.reset();
  // DROP succeeds even if postcommit cleanup fails; only storage shows the deletion.
  auto store =
      ASSERT_RESULT(PgCatalogTransaction::Open(path + "/catalog", Snapshot() + "/rocksdb"));
  for (const auto& [database, sequence] : dropped) {
    const auto status = ResultToStatus(store->ReadSequence(database, sequence));
    ASSERT_TRUE(status.IsNotFound()) << database << "/" << sequence << ": " << status;
  }
  const auto retained = ASSERT_RESULT(store->ReadSequence(kept.first, kept.second));
  ASSERT_EQ(std::make_tuple(retained.last_value, retained.is_called), kept_state);
}

TEST_F(ReadonlyPgServerTest, SequenceRpcGuards) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("sequence-rpc"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  uint32_t database = 0;
  int64_t sequence = 0;
  {
    auto c = ASSERT_RESULT(Connect(*server));
    database = static_cast<uint32_t>(ASSERT_RESULT(c.FetchRow<int64_t>(
        "SELECT oid::bigint FROM pg_database WHERE datname=current_database()")));
    ASSERT_OK(c.Execute("CREATE SEQUENCE seq_rpc"));
    sequence = ASSERT_RESULT(c.FetchRow<int64_t>("SELECT 'seq_rpc'::regclass::oid::bigint"));
  }
  rpc::MessengerBuilder builder("sequence-rpc-client");
  builder.UseDefaultConnectionContextFactory();
  auto messenger = ASSERT_RESULT(builder.Build());
  auto shutdown = ScopeExit([&] { messenger->Shutdown(); });
  rpc::ProxyCache cache(messenger.get());
  tserver::PgClientServiceProxy proxy(&cache, server->RpcAddress());
  const SequenceRpc sequences(proxy);
  const auto connect = [&]() -> Result<uint64_t> {
    tserver::PgHeartbeatRequestPB request;
    request.set_pid(getpid());
    tserver::PgHeartbeatResponsePB response;
    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy.Heartbeat(request, &response, &controller));
    if (response.has_status()) return StatusFromPB(response.status());
    return response.session_id();
  };
  // Heartbeat admits a backend without a database; as in PgGate, a Perform binds it.
  const auto perform = [&](uint64_t session, uint64_t serial, uint32_t database_oid,
                           bool lease) -> Status {
    ThreadSafeArena arena;
    tserver::LWPgPerformRequestPB request(&arena);
    tserver::LWPgPerformResponsePB response(&arena);
    request.set_session_id(session);
    request.mutable_sequence_num()->set_serial_no(serial);
    if (serial) {
      request.mutable_sequence_num()->mutable_predecessor_serial_no()->set_value(serial - 1);
    }
    auto* options = request.mutable_options();
    options->set_connected_database_oid(database_oid);
    if (lease) {
      options->set_sql_transaction_active(true);
      options->set_is_using_table_locks(true);
      options->set_txn_serial_no(serial + 1);
    }
    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy.Perform(request, &response, &controller));
    return response.has_status() ? StatusFromPB(response.status()) : Status::OK();
  };
  // A strong object lock takes the exclusive lease, as DDL does; plain SQL transactions do not.
  const auto lock_exclusive = [&](uint64_t session, uint64_t serial) -> Status {
    tserver::PgAcquireObjectLockRequestPB request;
    request.set_session_id(session);
    auto* options = request.mutable_options();
    options->set_sql_transaction_active(true);
    options->set_is_using_table_locks(true);
    options->set_txn_serial_no(serial + 1);
    request.set_lock_type(tserver::ACCESS_EXCLUSIVE_LOCK);
    tserver::PgAcquireObjectLockResponsePB response;
    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy.AcquireObjectLock(request, &response, &controller));
    return response.has_status() ? StatusFromPB(response.status()) : Status::OK();
  };
  const auto finish = [&](uint64_t session) -> Status {
    tserver::PgFinishTransactionRequestPB request;
    request.set_session_id(session);
    request.set_commit(false);
    tserver::PgFinishTransactionResponsePB response;
    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy.FinishTransaction(request, &response, &controller));
    return response.has_status() ? StatusFromPB(response.status()) : Status::OK();
  };
  using Tuple = std::pair<int64_t, bool>;
  constexpr uint32_t kTemplate1 = 1;
  const auto owner = ASSERT_RESULT(connect());
  const auto cleaner = ASSERT_RESULT(connect());
  const auto unbound = ASSERT_RESULT(connect());
  const auto templated = ASSERT_RESULT(connect());
  ASSERT_OK(perform(owner, 0, database, false));
  ASSERT_OK(perform(cleaner, 0, database, false));
  ASSERT_OK(perform(templated, 0, kTemplate1, false));
  ASSERT_EQ(ASSERT_RESULT(sequences.Read(owner, database, sequence)), Tuple(1, false));

  // Cleanup skips the lease, cache, and template checks, but not the database identity check.
  const std::vector<Status> invalid = {
      ResultToStatus(sequences.Read(std::numeric_limits<uint64_t>::max() - 1, database, sequence)),
      ResultToStatus(sequences.Read(unbound, database, sequence)),
      ResultToStatus(sequences.Read(owner, kTemplate1, sequence)),
      ResultToStatus(sequences.Read(owner, database, 0)),
      ResultToStatus(sequences.Read(owner, database, int64_t{1} << 32)),
      sequences.Delete(templated, database, sequence)};
  for (const auto& status : invalid) ASSERT_TRUE(status.IsInvalidArgument()) << status;
  const auto in_template = sequences.Insert(templated, kTemplate1, sequence);
  ASSERT_TRUE(in_template.IsNotSupported()) << in_template;
  const auto historical = ResultToStatus(sequences.Read(owner, database, sequence, 1));
  ASSERT_TRUE(historical.IsNotSupported()) << historical;
  const auto cache_method = FLAGS_ysql_sequence_cache_method;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_sequence_cache_method) = "server";
  const auto server_cache = ResultToStatus(sequences.Fetch(owner, database, sequence));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_sequence_cache_method) = cache_method;
  ASSERT_TRUE(server_cache.IsNotSupported()) << server_cache;

  // Another backend's open SQL transaction does not block sequences.
  ASSERT_OK(perform(cleaner, 1, database, true));
  ASSERT_EQ(ASSERT_RESULT(sequences.Read(owner, database, sequence)), Tuple(1, false));
  // Its exclusive lease rejects access before any value is reserved or changed.
  ASSERT_OK(lock_exclusive(cleaner, 1));
  const std::vector<Status> leased = {
      sequences.Insert(owner, database, sequence),
      ResultToStatus(sequences.Read(owner, database, sequence)),
      sequences.Update(owner, database, sequence),
      ResultToStatus(sequences.Fetch(owner, database, sequence))};
  for (const auto& status : leased) ASSERT_TRUE(status.IsTryAgain()) << status;
  ASSERT_OK(finish(cleaner));
  ASSERT_EQ(ASSERT_RESULT(sequences.Read(owner, database, sequence)), Tuple(1, false));
  ASSERT_EQ(
      ASSERT_RESULT(sequences.Fetch(owner, database, sequence)),
      std::make_pair(int64_t{1}, int64_t{3}));

  // Postcommit cleanup arrives after FinishTransaction, here once the lease has moved on.
  ASSERT_OK(perform(owner, 1, database, true));
  ASSERT_OK(lock_exclusive(owner, 1));
  const auto blocked = ResultToStatus(sequences.Read(cleaner, database, sequence));
  ASSERT_TRUE(blocked.IsTryAgain()) << blocked;
  const auto live = sequences.DeleteDatabase(cleaner, database);
  ASSERT_TRUE(live.IsInvalidArgument() || live.IsIllegalState()) << live;
  ASSERT_EQ(ASSERT_RESULT(sequences.Read(owner, database, sequence)), Tuple(3, true));
  ASSERT_OK(sequences.Delete(cleaner, database, sequence));
  ASSERT_OK(sequences.Delete(cleaner, database, sequence));
  const auto deleted = ResultToStatus(sequences.Read(owner, database, sequence));
  ASSERT_TRUE(deleted.IsNotFound()) << deleted;
  ASSERT_OK(finish(owner));
}

TEST_F(ReadonlyPgServerTest, SequenceRestartRollbackBounds) {
  auto server = ASSERT_RESULT(
      ReadonlyPgServer::Start(
          GetTestPath("sequence-restart-rollback"), Snapshot(), AllocateFreePort(),
          CatalogMode::kSerializedTransactions));
  auto c = ASSERT_RESULT(Connect(*server));
  ASSERT_OK(c.Execute("CREATE SEQUENCE seq_widen_up MAXVALUE 3"));
  ASSERT_OK(c.Execute("CREATE SEQUENCE seq_widen_down INCREMENT -1 MINVALUE -3 MAXVALUE -1"));
  struct Case {
    std::string sequence;
    std::string widen;
    int64_t min, max, restart, setval, next;
  };
  const std::vector<Case> cases = {
      {"seq_widen_up", "MAXVALUE 100 RESTART 50", 1, 3, 50, 2, 3},
      {"seq_widen_down", "MINVALUE -100 RESTART -50", -3, -1, -50, -2, -3}};
  for (const auto& [sequence, widen, min, max, restart, setval, next] : cases) {
    ASSERT_OK(c.Execute("BEGIN"));
    ASSERT_OK(c.ExecuteFormat("ALTER SEQUENCE $0 $1", sequence, widen));
    ASSERT_OK(c.Execute("ROLLBACK"));
    // The bounds roll back with the catalog; the RESTART tuple is nontransactional.
    ASSERT_EQ(
        ASSERT_RESULT((c.FetchRow<int64_t, int64_t>(Format(
            "SELECT seqmin,seqmax FROM pg_sequence WHERE seqrelid='$0'::regclass", sequence)))),
        std::make_tuple(min, max));
    const auto state = Format("SELECT last_value,is_called FROM $0", sequence);
    ASSERT_EQ(ASSERT_RESULT((c.FetchRow<int64_t, bool>(state))), std::make_tuple(restart, false));
    const auto rejected = ResultToStatus(c.Fetch(Format("SELECT nextval('$0')", sequence)));
    ASSERT_NOK(rejected);
    ASSERT_STR_CONTAINS(rejected.ToString(), "outside MINVALUE/MAXVALUE");
    ASSERT_EQ(ASSERT_RESULT((c.FetchRow<int64_t, bool>(state))), std::make_tuple(restart, false));
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>(Format("SELECT setval('$0',$1)", sequence, setval))),
        setval);
    ASSERT_EQ(
        ASSERT_RESULT(c.FetchRow<int64_t>(Format("SELECT nextval('$0')", sequence))), next);
  }
}

}  // namespace
}  // namespace yb::local_runtime
