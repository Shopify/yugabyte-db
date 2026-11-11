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
#include <openssl/sha.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include <google/protobuf/io/coded_stream.h>

#include "yb/common/common.pb.h"
#include "yb/common/pg_types.h"
#include "yb/common/pgsql_error.h"
#include "yb/common/transaction_error.h"
#include "yb/common/pgsql_protocol.messages.h"
#include "yb/common/schema_pbutil.h"
#include "yb/common/wire_protocol.h"
#include "yb/common/version_info.h"
#include "yb/gutil/endian.h"
#include "yb/gutil/walltime.h"
#include "yb/docdb/doc_read_context.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/docdb/pgsql_operation.h"
#include "yb/docdb/ql_rocksdb_storage.h"
#include "yb/rpc/inbound_call.h"
#include "yb/rpc/messenger.h"
#include "yb/rpc/rpc_header.pb.h"
#include "yb/rpc/rpc_service.h"
#include "yb/rpc/scheduler.h"
#include "yb/rpc/serialization.h"
#include "yb/rpc/sidecars.h"
#include "yb/rpc/yb_rpc.h"
#include "yb/server/rpc_server.h"
#include "yb/tablet/metadata.pb.h"
#include "yb/tablet/tablet_options.h"
#include "yb/tools/local_runtime/pg_catalog_transaction.h"
#include "yb/tools/local_runtime/pg_local_txn_store.h"
#include "yb/tools/local_runtime/pg_local_table.h"
#include "yb/tools/local_runtime/pg_local_request_sequence.h"
#include "yb/dockv/partition.h"
#include "yb/tools/local_runtime/pg_local_service_base.h"
#include "yb/tserver/pg_shared_mem_pool.h"
#include "yb/tserver/tserver_admin.pb.h"
#include "yb/tserver/tserver_shared_mem.h"
#include "yb/util/coding.h"
#include "yb/util/env.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/memory/arena.h"
#include "yb/util/metrics.h"
#include "yb/util/pb_util.h"
#include "yb/util/scope_exit.h"
#include "yb/util/sync_point.h"
#include "yb/util/thread_pool.h"
#include "yb/util/thread_restrictions.h"
#include "yb/util/uuid.h"
#include "yb/util/write_buffer.h"
#include "yb/yql/pgwrapper/pg_wrapper.h"
#include "yb/yql/pggate/util/pg_wire.h"

DECLARE_bool(enable_object_lock_fastpath);
DECLARE_bool(enable_object_locking_for_table_locks);
DECLARE_bool(enable_ysql_conn_mgr);
DECLARE_bool(enable_ysql_conn_mgr_stats);
DECLARE_int32(pgsql_proxy_webserver_port);
DECLARE_bool(rpc_server_allow_ephemeral_ports);
DECLARE_bool(ysql_disable_index_backfill);
DECLARE_bool(ysql_enable_object_locking_infra);
DECLARE_bool(ysql_enable_packed_row);
DECLARE_bool(ysql_enable_packed_row_for_colocated_table);
DECLARE_bool(ysql_enable_relcache_init_optimization);
DECLARE_string(ysql_pg_conf_csv);
DECLARE_string(ysql_sequence_cache_method);
DECLARE_bool(ysql_use_packed_row_v2);
DECLARE_bool(ysql_yb_enable_add_column_missing_default);
DECLARE_bool(ysql_yb_enable_alter_table_rewrite);
DECLARE_bool(ysql_yb_enable_ddl_atomicity_infra);
DECLARE_bool(ysql_yb_enable_ddl_savepoint_infra);
DECLARE_bool(ysql_yb_enable_pg_locks);
DECLARE_bool(ysql_yb_pg_locks_integrate_advisory_locks);

METRIC_DECLARE_event_stats(rpc_incoming_queue_time);

DEFINE_NON_RUNTIME_bool(local_runtime_inline_rpc, true,
    "Handle local runtime RPCs on the reactor thread instead of a service thread pool.");

#if defined(__APPLE__)
// Interprocess semaphores there fall back to Boost's emulation, which measured slower than TCP.
constexpr bool kSharedMemoryByDefault = false;
#else
constexpr bool kSharedMemoryByDefault = true;
#endif

DEFINE_NON_RUNTIME_bool(local_runtime_shared_memory, kSharedMemoryByDefault,
    "Exchange Perform and object lock requests with PostgreSQL backends through per-session "
    "shared memory, as the tablet server does, instead of only over TCP.");

DEFINE_NON_RUNTIME_bool(local_runtime_wait_on_conflict, true,
    "Wait for conflicting transactions to finish, as tablet wait queues do, instead of failing "
    "the request.");

namespace yb::local_runtime {
namespace {

// Every handler serializes on the service mutex and never waits for another call (out-of-order
// requests are parked, not awaited), so a thread-pool hop adds two cross-thread wakeups per call
// and no concurrency. Handlers do storage I/O, which reactor threads otherwise forbid.
class InlineService : public rpc::RpcService {
 public:
  InlineService(rpc::ServiceIfPtr service, const scoped_refptr<MetricEntity>& entity)
      : service_(std::move(service)),
        queue_time_(METRIC_rpc_incoming_queue_time.Instantiate(entity)) {}

  void FillEndpoints(rpc::RpcEndpointMap* map) override {
    service_->FillEndpoints(rpc::RpcServicePtr(this), map);
  }

  void Process(rpc::InboundCallPtr call, rpc::Queue) override {
    call->RecordHandlingStarted(queue_time_);
    if (PREDICT_FALSE(call->ClientTimedOut())) {
      call->RespondTimedOutIfPending("Call timed out before local handling");
      return;
    }
    if (!call->TryStartProcessing()) return;
    ThreadRestrictions::ScopedAllowIO allow_io;
    ThreadRestrictions::ScopedAllowWait allow_wait;
    service_->Handle(std::move(call));
  }

  void StartShutdown() override { service_->Shutdown(); }
  void CompleteShutdown() override {}

 private:
  const rpc::ServiceIfPtr service_;
  const scoped_refptr<EventStats> queue_time_;
};

constexpr auto kOwnerMarker = "yb-local-readonly-v2\n";
std::mutex runtime_mutex;

class RuntimeDirectory {
 public:
  ~RuntimeDirectory() {
    if (fd_ >= 0) close(fd_);
  }

  Status Acquire(const std::string& path, const std::string& identity) {
    struct stat info;
    const bool exists = lstat(path.c_str(), &info) == 0;
    if (exists) {
      SCHECK(S_ISDIR(info.st_mode), InvalidArgument, "Runtime path is not a plain directory");
      RETURN_NOT_OK(ValidateMarker(path, identity));
    } else {
      SCHECK_EQ(errno, ENOENT, IOError, "Cannot inspect runtime directory");
      const auto parent = std::filesystem::path(path).parent_path().string();
      if (!parent.empty()) RETURN_NOT_OK(Env::Default()->CreateDirs(parent));
      SCHECK_EQ(mkdir(path.c_str(), 0700), 0, IOError, "Cannot create runtime directory");
    }
    fd_ = open((path + "/LOCK").c_str(), O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0600);
    SCHECK_GE(fd_, 0, IOError, "Cannot open runtime lock without following symlinks");
    SCHECK(
        fstat(fd_, &info) == 0 && S_ISREG(info.st_mode), InvalidArgument,
        "Runtime lock is not a regular file");
    SCHECK_EQ(flock(fd_, LOCK_EX | LOCK_NB), 0, IOError, "Runtime directory is already locked");
    if (exists) {
      RETURN_NOT_OK(ValidateMarker(path, identity));
      return ValidatePgData(path);
    }
    const int marker = open(
        (path + "/LOCAL_READONLY").c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        0600);
    SCHECK_GE(marker, 0, IOError, "Cannot create runtime marker");
    auto cleanup = ScopeExit([marker] { close(marker); });
    size_t offset = 0;
    while (offset < identity.size()) {
      const auto written = write(marker, identity.data() + offset, identity.size() - offset);
      if (written < 0 && errno == EINTR) continue;
      SCHECK_GT(written, 0, IOError, "Cannot write runtime marker");
      offset += written;
    }
    SCHECK_EQ(fsync(marker), 0, IOError, "Cannot sync runtime marker");
    return Status::OK();
  }

 private:
  Status ValidatePgData(const std::string& path) {
    std::error_code error;
    const auto root = std::filesystem::canonical(path, error);
    SCHECK(!error, IOError, "Cannot resolve runtime directory");
    const auto versioned =
        root / Format("pg_data_$0", static_cast<int32_t>(VersionInfo::YsqlMajorVersion()));
    struct stat info;
    if (lstat(versioned.c_str(), &info) == 0) {
      SCHECK(
          S_ISDIR(info.st_mode), InvalidArgument,
          "Versioned PostgreSQL path is not a plain directory");
    } else {
      SCHECK_EQ(errno, ENOENT, IOError, "Cannot inspect versioned PostgreSQL directory");
    }
    const auto link = root / "pg_data";
    if (lstat(link.c_str(), &info) != 0) {
      SCHECK_EQ(errno, ENOENT, IOError, "Cannot inspect PostgreSQL data link");
      return Status::OK();
    }
    SCHECK(S_ISLNK(info.st_mode), InvalidArgument, "PostgreSQL data path is not a symlink");
    // Directory copies preserve PgWrapper's absolute link to the source runtime. Reject before
    // initdb/config generation can modify that other runtime, even when its postmaster is stopped.
    const auto target = std::filesystem::canonical(link, error);
    SCHECK(
        !error && target == versioned, InvalidArgument,
        "PostgreSQL data link does not belong to this runtime");
    return Status::OK();
  }

  Status ValidateMarker(const std::string& path, const std::string& identity) {
    const int marker = open((path + "/LOCAL_READONLY").c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    SCHECK_GE(marker, 0, InvalidArgument, "Missing or symlinked runtime marker");
    auto cleanup = ScopeExit([marker] { close(marker); });
    struct stat info;
    SCHECK(
        fstat(marker, &info) == 0 && S_ISREG(info.st_mode), InvalidArgument,
        "Runtime marker is not a regular file");
    std::string contents;
    char buffer[4096];
    for (;;) {
      const auto count = read(marker, buffer, sizeof(buffer));
      if (count < 0 && errno == EINTR) continue;
      SCHECK_GE(count, 0, IOError, "Cannot read runtime marker");
      if (count == 0) break;
      contents.append(buffer, count);
      SCHECK_LE(contents.size(), sizeof(buffer), InvalidArgument, "Oversized runtime marker");
    }
    SCHECK_EQ(
        contents, identity, InvalidArgument,
        "Runtime directory ownership, engine, or catalog seed mismatch");
    return Status::OK();
  }

  int fd_ = -1;
};

Result<std::string> SnapshotFingerprint(const std::string& root) {
  try {
    std::vector<std::string> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
      SCHECK(!entry.is_symlink(), InvalidArgument, "Catalog seed contains a symlink");
      if (entry.is_directory()) continue;
      SCHECK(entry.is_regular_file(), InvalidArgument, "Catalog seed has a non-regular file");
      files.push_back(std::filesystem::relative(entry.path(), root).generic_string());
    }
    std::sort(files.begin(), files.end());
    SHA256_CTX hash;
    SHA256_Init(&hash);
    for (const auto& name : files) {
      auto size = VERIFY_RESULT(Env::Default()->GetFileSize(root + "/" + name));
      SCHECK_LE(size, 256ULL * 1024 * 1024, NotSupported, "Catalog seed file exceeds safety limit");
      faststring bytes;
      RETURN_NOT_OK(ReadFileToString(Env::Default(), root + "/" + name, &bytes));
      std::array<char, 8> length;
      BigEndian::Store64(length.data(), name.size());
      SHA256_Update(&hash, length.data(), length.size());
      SHA256_Update(&hash, name.data(), name.size());
      BigEndian::Store64(length.data(), bytes.size());
      SHA256_Update(&hash, length.data(), length.size());
      SHA256_Update(&hash, bytes.data(), bytes.size());
    }
    std::array<unsigned char, SHA256_DIGEST_LENGTH> digest;
    SHA256_Final(digest.data(), &hash);
    return Slice(digest.data(), digest.size()).ToDebugHexString();
  } catch (const std::filesystem::filesystem_error& error) {
    return STATUS(IOError, error.what());
  }
}

class FrozenCatalog {
 public:
  struct Table {
    tablet::TableInfoPB metadata;
    std::shared_ptr<docdb::DocReadContext> context;
    bool application = false;
    dockv::PartitionSchema partition_schema;
    std::vector<dockv::Partition> partitions;
    bool local_catalog = false;
  };

  Status Open(const std::string& snapshot, const std::string& log_directory, CatalogMode mode) {
    tserver::ExportedTabletMetadataChanges changes;
    RETURN_NOT_OK(
        pb_util::ReadPBContainerFromPath(
            Env::Default(), snapshot + "/exported_tablet_metadata_changes", &changes));
    for (const auto& change : changes.metadata_changes()) {
      if (change.has_add_table()) {
        RETURN_NOT_OK(Add(change.add_table()));
      } else if (!change.add_multiple_tables().empty()) {
        for (const auto& table : change.add_multiple_tables()) RETURN_NOT_OK(Add(table));
      } else {
        return STATUS_FORMAT(
            NotSupported, "Unsupported catalog metadata change: $0", change.ShortDebugString());
      }
    }
    SCHECK(!tables_.empty(), Corruption, "Catalog snapshot contains no tables");
    if (mode == CatalogMode::kSerializedTransactions) {
      transaction_ = VERIFY_RESULT(
          PgCatalogTransaction::Open(log_directory + "/catalog", snapshot + "/rocksdb"));
    } else {
      rocksdb::Options options;
      tablet::TabletOptions tablet_options;
      tablet_options.block_cache = rocksdb::NewLRUCache(16 * 1024 * 1024);
      docdb::InitRocksDBOptions(
          &options, "frozen-catalog", "local-catalog", nullptr, tablet_options,
          docdb::StorageDbType::kRegular);
      options.create_if_missing = false;
      options.db_log_dir = log_directory;
      rocksdb::DB* db = nullptr;
      RETURN_NOT_OK(rocksdb::DB::OpenForReadOnly(options, snapshot + "/rocksdb", &db, true));
      db_.reset(db);
      // The seed is immutable and may have been built on a clock ahead of this machine.
      read_time_ = HybridTime::kMax;
    }
    RETURN_NOT_OK(LoadDatabaseNames());
    seed_databases_ = databases_;
    RETURN_NOT_OK(ReloadTables());
    RETURN_NOT_OK(LoadDatabaseNames());
    RETURN_NOT_OK(LoadCatalogVersions());
    LOG(INFO) << "Loaded frozen catalog: " << tables_.size() << " tables, " << databases_.size()
              << " databases";
    return Status::OK();
  }

  Status LoadDatabaseNames() {
    const Table* database_table = nullptr;
    for (const auto& [id, table] : tables_) {
      if (table.metadata.table_name() == "pg_database") {
        database_table = &table;
        break;
      }
    }
    SCHECK(database_table, Corruption, "Seed lacks pg_database");
    const auto& schema = database_table->context->schema();
    PgsqlReadRequestPB request;
    request.set_client(YQL_CLIENT_PGSQL);
    request.set_table_id(database_table->metadata.table_id());
    request.set_schema_version(database_table->metadata.schema_version());
    for (auto name : {"oid", "datname"}) {
      const auto column = schema.find_column(name);
      SCHECK_GE(column, 0, Corruption, "Invalid pg_database schema");
      request.add_targets()->set_column_id(schema.column_id(column));
      request.mutable_column_refs()->add_ids(schema.column_id(column));
    }
    ThreadSafeArena arena;
    LWPgsqlReadRequestPB req(&arena);
    req.CopyFrom(request);
    LWPgsqlResponsePB response(&arena);
    rpc::Sidecars sidecars;
    RETURN_NOT_OK(Read(req, &response, &sidecars, CoarseTimePoint::max()));
    Slice bytes = sidecars.GetFirst();
    const auto count = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<int64_t>(&bytes));
    std::unordered_set<PgOid> seen;
    for (int64_t i = 0; i < count; ++i) {
      SCHECK_EQ(
          VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint8_t>(&bytes)), 0, Corruption,
          "NULL database OID");
      const auto oid = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint32_t>(&bytes));
      SCHECK_EQ(
          VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint8_t>(&bytes)), 0, Corruption,
          "NULL database name");
      const auto size = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint64_t>(&bytes));
      SCHECK(
          size > 0 && size <= bytes.size() && bytes[size - 1] == '\0', Corruption,
          "Invalid database name encoding");
      if (databases_.contains(oid)) {
        const auto name = bytes.Prefix(size - 1).ToBuffer();
        if (!seed_databases_.empty() && !seed_databases_.contains(oid)) {
          SCHECK_EQ(databases_.at(oid), name, Corruption, "Cloned database name mismatch");
        }
        databases_[oid] = name;
        seen.insert(oid);
      }
      bytes.remove_prefix(size);
    }
    SCHECK(bytes.empty(), Corruption, "Trailing database names");
    for (const auto& [oid, name] : databases_) {
      SCHECK(
          seed_databases_.empty() || seed_databases_.contains(oid) || seen.contains(oid),
          Corruption, "Cloned database is missing its pg_database row");
    }
    return Status::OK();
  }

  Status ReloadTables() {
    RETURN_NOT_OK(Status(failure_));
    if (!transaction_) return Status::OK();
    // A logical abort can be repaired by an earlier savepoint. Only a failed rebuild poisons
    // the descriptor cache; subsequent rollback must not clear that fence.
    RETURN_NOT_OK(transaction_->CheckHealthy());
    const auto rebuild = [&]() -> Status {
      auto descriptors = VERIFY_RESULT(transaction_->Tables());
      auto catalogs = VERIFY_RESULT(transaction_->CatalogTables());
      std::erase_if(tables_, [](const auto& item) {
        return item.second.application || item.second.local_catalog;
      });
      databases_ = seed_databases_;
      for (const auto& descriptor : catalogs) RETURN_NOT_OK(Add(descriptor, false, true));
      for (const auto& descriptor : descriptors) RETURN_NOT_OK(Add(descriptor, true));
      return Status::OK();
    };
    failure_ = rebuild();
    TEST_SYNC_POINT_CALLBACK("LocalRuntime::ReloadStatus", &failure_);
    return failure_;
  }

  Status RefreshTable(const TableId& id) {
    auto descriptor = VERIFY_RESULT(transaction_->GetTable(id));
    tables_.erase(id);
    return Add(descriptor, true);
  }

  Result<std::unordered_set<PgOid>> SharedCatalogOids(uint32_t database_oid) {
    const auto& table = VERIFY_RESULT_REF(Find(GetPgsqlTableId(database_oid, kPgClassTableOid)));
    ThreadSafeArena arena;
    LWPgsqlReadRequestPB request(&arena);
    request.set_client(YQL_CLIENT_PGSQL);
    request.dup_table_id(table.metadata.table_id());
    request.set_schema_version(table.metadata.schema_version());
    for (const auto* name : {"oid", "relisshared"}) {
      const auto index = table.context->schema().find_column(name);
      SCHECK_GE(index, 0, Corruption, "Invalid pg_class schema");
      const auto column_id = table.context->schema().column_id(index);
      request.add_targets()->set_column_id(column_id);
      request.mutable_column_refs()->add_ids(column_id);
    }
    LWPgsqlResponsePB response(&arena);
    rpc::Sidecars sidecars;
    RETURN_NOT_OK(Read(request, &response, &sidecars, CoarseTimePoint::max()));
    Slice bytes = sidecars.GetFirst();
    const auto count = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<int64_t>(&bytes));
    std::unordered_set<PgOid> result;
    for (int64_t i = 0; i < count; ++i) {
      SCHECK_EQ(
          VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint8_t>(&bytes)), 0, Corruption,
          "NULL catalog relation OID");
      const auto oid = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint32_t>(&bytes));
      SCHECK_EQ(
          VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint8_t>(&bytes)), 0, Corruption,
          "NULL catalog shared flag");
      if (VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint8_t>(&bytes))) result.insert(oid);
    }
    SCHECK(bytes.empty(), Corruption, "Trailing shared-catalog data");
    SCHECK(result.contains(kPgDatabaseTableOid), Corruption, "Missing shared database catalog");
    return result;
  }

  Status CreateDatabase(const tserver::PgCreateDatabaseRequestPB& request) {
    SCHECK(
        transaction_ && request.use_transaction() && request.use_regular_transaction_block(),
        NotSupported, "Local database creation requires native DDL transaction blocks");
    SCHECK(
        !request.colocated() && request.clone_time() == 0 && request.next_oid() == 0 &&
            request.source_database_name().empty() && request.target_owner().empty() &&
            request.GetReflection()->GetUnknownFields(request).empty(),
        NotSupported, "Database cloning, colocation, or custom OID allocation is not supported");
    SCHECK(
        databases_.contains(request.source_database_oid()) &&
            (databases_.at(request.source_database_oid()) == "template0" ||
             databases_.at(request.source_database_oid()) == "template1"),
        NotSupported, "Only built-in database templates are supported");
    SCHECK(
        !databases_.contains(request.database_oid()), AlreadyPresent,
        "Database OID already exists");
    SCHECK_LT(
        catalog_slots_.size(), tserver::TServerSharedData::kMaxNumDbCatalogVersions, NotSupported,
        "Local database limit reached");
    for (const auto& [oid, name] : databases_) {
      SCHECK_NE(name, request.database_name(), AlreadyPresent, "Database name already exists");
    }
    const auto shared = VERIFY_RESULT(SharedCatalogOids(request.source_database_oid()));
    std::vector<tablet::TableInfoPB> sources;
    for (const auto& [id, table] : tables_) {
      if (VERIFY_RESULT(GetPgsqlDatabaseOidByTableId(id)) != request.source_database_oid())
        continue;
      SCHECK(
          !table.application, NotSupported,
          "Database templates with application tables are unsupported");
      if (!shared.contains(VERIFY_RESULT(GetPgsqlTableOid(id)))) sources.push_back(table.metadata);
    }
    RETURN_NOT_OK(transaction_->CreateDatabase(
        request.database_oid(), request.database_name(), sources,
        request.options().active_sub_transaction_id()));
    return ReloadTables();
  }

  Status DropDatabase(const tserver::PgDropDatabaseRequestPB& request) {
    const auto oid = request.database_oid();
    SCHECK(
        transaction_ && databases_.contains(oid) && !seed_databases_.contains(oid), NotSupported,
        "Only locally created databases can be dropped");
    SCHECK_EQ(
        databases_.at(oid), request.database_name(), InvalidArgument, "Database name mismatch");
    RETURN_NOT_OK(transaction_->DropDatabase(oid, request.database_name(), 1));
    const auto ns = GetPgsqlNamespaceId(oid);
    std::erase_if(
        tables_, [&](const auto& entry) { return entry.second.metadata.namespace_id() == ns; });
    databases_.erase(oid);
    return Status::OK();
  }

  Status CreateTable(const tserver::PgCreateTableRequestPB& request) {
    SCHECK(transaction_, NotSupported, "Frozen catalog cannot create tables");
    SCHECK(databases_.contains(request.table_id().database_oid()), NotFound, "Unknown database");
    SCHECK_EQ(
        databases_.at(request.table_id().database_oid()), request.database_name(), InvalidArgument,
        "Database name does not match database OID");
    auto metadata = VERIFY_RESULT(BuildLocalTable(request));
    if (request.old_relfilenode_oid().object_oid()) {
      const auto& source =
          VERIFY_RESULT_REF(Find(PgObjectId::GetYbTableIdFromPB(request.old_relfilenode_oid())));
      const auto logical = source.metadata.pg_table_id().empty() ? source.metadata.table_id()
                                                                 : source.metadata.pg_table_id();
      SCHECK(
          source.application && logical == metadata.pg_table_id() &&
              source.metadata.has_index_info() == metadata.has_index_info(),
          InvalidArgument, "Rewrite source does not match the logical relation");
    }
    SCHECK(!tables_.contains(metadata.table_id()), AlreadyPresent, "Table ID already exists");
    RETURN_NOT_OK(
        transaction_->CreateTable(metadata, request.options().active_sub_transaction_id()));
    RETURN_NOT_OK(RefreshTable(metadata.table_id()));
    if (metadata.has_index_info()) {
      RETURN_NOT_OK(RefreshTable(metadata.index_info().indexed_table_id()));
    }
    return Status::OK();
  }

  Status AlterTable(const tserver::PgAlterTableRequestPB& request) {
    SCHECK(
        transaction_ && request.use_transaction() && request.use_regular_transaction_block(),
        NotSupported, "Local ALTER requires native DDL transaction blocks");
    const auto id = PgObjectId::GetYbTableIdFromPB(request.table_id());
    const auto& table = VERIFY_RESULT_REF(Find(id));
    SCHECK(table.application, NotSupported, "Frozen catalog schemas cannot be altered");
    const auto base = table.metadata.has_index_info()
                          ? table.metadata.index_info().indexed_table_id()
                          : TableId();
    RETURN_NOT_OK(transaction_->AlterTable(request));
    RETURN_NOT_OK(RefreshTable(id));
    if (!base.empty()) RETURN_NOT_OK(RefreshTable(base));
    return Status::OK();
  }

  Status DropTable(
      const tserver::PgDropTableRequestPB& request, tserver::PgDropTableResponsePB* response) {
    SCHECK(
        transaction_ && request.use_regular_transaction_block(), NotSupported,
        "Local DROP requires native DDL transaction blocks");
    SCHECK(
        request.GetReflection()->GetUnknownFields(request).empty(), InvalidArgument,
        "Unknown DROP fields");
    const auto id = PgObjectId::GetYbTableIdFromPB(request.table_id());
    const auto metadata = VERIFY_RESULT_REF(Find(id)).metadata;
    SCHECK(VERIFY_RESULT_REF(Find(id)).application, NotSupported, "Catalog DROP is unsupported");
    SCHECK_EQ(metadata.has_index_info(), request.index(), InvalidArgument, "DROP kind mismatch");
    TableId base;
    if (metadata.has_index_info()) {
      base = metadata.index_info().indexed_table_id();
      const auto& parent = VERIFY_RESULT_REF(Find(base)).metadata;
      auto* identifier = response->mutable_indexed_table();
      identifier->set_table_id(base);
      identifier->set_table_name(parent.table_name());
      identifier->mutable_namespace_()->set_id(parent.namespace_id());
      identifier->mutable_namespace_()->set_name(parent.namespace_name());
      identifier->mutable_namespace_()->set_database_type(YQL_DATABASE_PGSQL);
    }
    RETURN_NOT_OK(transaction_->DropTable(id, request.options().active_sub_transaction_id()));
    tables_.erase(id);
    for (const auto& index : metadata.indexes()) tables_.erase(index.table_id());
    if (!base.empty()) RETURN_NOT_OK(RefreshTable(base));
    return Status::OK();
  }

  HybridTime ReadTime() const { return transaction_ ? transaction_->ReadTime() : read_time_; }

  Status Write(
      const PgsqlWriteRequestMsg& request, PgsqlResponseMsg* response, rpc::Sidecars* sidecars,
      uint32_t subtransaction) {
    SCHECK(transaction_, NotSupported, "Local catalog server is read-only");
    const auto& table = VERIFY_RESULT_REF(Find(request.table_id().ToBuffer()));
    static const std::unordered_set<std::string> allowed = {
        "pg_database",
        "pg_database_oid_index",
        "pg_database_datname_index",
        "pg_namespace",
        "pg_namespace_oid_index",
        "pg_namespace_nspname_index",
        "pg_init_privs",
        "pg_init_privs_o_c_o_index",
        "pg_extension",
        "pg_extension_oid_index",
        "pg_extension_name_index",
        "pg_proc",
        "pg_proc_oid_index",
        "pg_proc_proname_args_nsp_index",
        "pg_cast",
        "pg_cast_oid_index",
        "pg_cast_source_target_index",
        "pg_ts_dict",
        "pg_ts_dict_oid_index",
        "pg_ts_dict_dictname_index",
        "pg_ts_template",
        "pg_ts_template_oid_index",
        "pg_ts_template_tmplname_index",
        "pg_class",
        "pg_class_oid_index",
        "pg_class_relname_nsp_index",
        "pg_class_tblspc_relfilenode_index",
        "pg_type",
        "pg_type_oid_index",
        "pg_type_typname_nsp_index",
        "pg_attribute",
        "pg_attribute_relid_attnam_index",
        "pg_attribute_relid_attnum_index",
        "pg_index",
        "pg_index_indexrelid_index",
        "pg_index_indrelid_index",
        "pg_constraint",
        "pg_constraint_conname_nsp_index",
        "pg_constraint_conrelid_contypid_conname_index",
        "pg_constraint_contypid_index",
        "pg_constraint_oid_index",
        "pg_constraint_conparentid_index",
        "pg_depend",
        "pg_depend_depender_index",
        "pg_depend_reference_index",
        "pg_shdepend",
        "pg_shdepend_depender_index",
        "pg_shdepend_reference_index",
        "pg_attrdef",
        "pg_attrdef_adrelid_adnum_index",
        "pg_attrdef_oid_index",
        "pg_description",
        "pg_description_o_c_o_index",
        "pg_shdescription",
        "pg_shdescription_o_c_index",
        "pg_sequence",
        "pg_sequence_seqrelid_index",
        "pg_yb_catalog_version",
        "pg_yb_catalog_version_pkey",
        "pg_yb_invalidation_messages",
        "pg_yb_invalidation_messages_db_oid_current_version_index"};
    SCHECK(
        table.application || allowed.contains(table.metadata.table_name()), NotSupported,
        "Unsupported mutable catalog table: " + table.metadata.table_name());
    SCHECK_EQ(
        request.schema_version(), table.metadata.schema_version(), InvalidArgument,
        "Catalog schema version mismatch");
    return transaction_->Apply(request, table.context, response, sidecars, subtransaction);
  }

  Status LoadCatalogVersions() {
    versions_.clear();
    max_version_ = 0;
    const auto& table = VERIFY_RESULT_REF(Find(kPgYbCatalogVersionTableId));
    const auto& schema = table.context->schema();
    const auto oid_index = schema.find_column("db_oid");
    const auto version_index = schema.find_column("current_version");
    SCHECK(oid_index >= 0 && version_index >= 0, Corruption, "Invalid catalog version schema");
    PgsqlReadRequestPB request;
    request.set_client(YQL_CLIENT_PGSQL);
    request.set_table_id(table.metadata.table_id());
    request.set_schema_version(table.metadata.schema_version());
    request.set_limit(tserver::TServerSharedData::kMaxNumDbCatalogVersions + 1);
    for (auto index : {oid_index, version_index}) {
      request.add_targets()->set_column_id(schema.column_id(index));
      request.mutable_column_refs()->add_ids(schema.column_id(index));
    }
    ThreadSafeArena arena;
    LWPgsqlReadRequestPB req(&arena);
    req.CopyFrom(request);
    LWPgsqlResponsePB response(&arena);
    rpc::Sidecars sidecars;
    RETURN_NOT_OK(Read(req, &response, &sidecars, CoarseTimePoint::max()));
    Slice bytes = sidecars.GetFirst();
    const auto count = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<int64_t>(&bytes));
    SCHECK_GT(count, 0, Corruption, "Empty catalog version table");
    SCHECK_LE(
        static_cast<uint64_t>(count), tserver::TServerSharedData::kMaxNumDbCatalogVersions,
        Corruption, "Too many catalog versions");
    for (int64_t row = 0; row < count; ++row) {
      SCHECK_EQ(
          VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint8_t>(&bytes)), 0, Corruption,
          "NULL database OID");
      auto oid = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint32_t>(&bytes));
      SCHECK_EQ(
          VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint8_t>(&bytes)), 0, Corruption,
          "NULL catalog version");
      auto version = VERIFY_RESULT(pggate::PgWire::CheckedReadNumber<uint64_t>(&bytes));
      SCHECK(
          version > 0 && versions_.emplace(oid, version).second, Corruption,
          "Invalid catalog version row");
      max_version_ = std::max(max_version_, version);
    }
    SCHECK(bytes.empty(), Corruption, "Trailing catalog version data");
    for (const auto& [oid, name] : databases_) {
      SCHECK(versions_.contains(oid), Corruption, "Namespace lacks a catalog version: " + name);
      if (!catalog_slots_.contains(oid)) catalog_slots_.emplace(oid, catalog_slots_.size());
    }
    return Status::OK();
  }

  Status Add(
      const tablet::TableInfoPB& metadata, bool application = false, bool local_catalog = false) {
    SCHECK_EQ(
        metadata.table_type(), TableType::PGSQL_TABLE_TYPE, NotSupported,
        "Snapshot table is not YSQL");
    SCHECK(!tables_.contains(metadata.table_id()), Corruption, "Duplicate catalog table");
    auto context = std::make_shared<docdb::DocReadContext>(
        "local-catalog", TableType::PGSQL_TABLE_TYPE, docdb::Index(metadata.has_index_info()),
        packings_);
    RETURN_NOT_OK(context->LoadFromPB(metadata));
    if (!application)
      context->SetCotableId(VERIFY_RESULT(Uuid::FromHexString(metadata.table_id())));
    const auto oid = VERIFY_RESULT(GetPgsqlDatabaseOidByTableId(metadata.table_id()));
    auto [db, inserted] = databases_.emplace(oid, metadata.namespace_name());
    SCHECK(
        inserted || db->second == metadata.namespace_name(), Corruption,
        "Inconsistent namespace names in seed");
    Table table{metadata, std::move(context), application, {}, {}, local_catalog};
    if (application) {
      RETURN_NOT_OK(
          dockv::PartitionSchema::FromPB(
              metadata.partition_schema(), table.context->schema(), &table.partition_schema));
      if (table.context->schema().num_hash_key_columns() > 0) {
        RETURN_NOT_OK(table.partition_schema.CreatePartitions(
            metadata.schema().table_properties().num_tablets(), &table.partitions));
      } else {
        table.partitions.emplace_back();
      }
    }
    tables_.emplace(metadata.table_id(), std::move(table));
    return Status::OK();
  }

  Result<const Table&> Find(const TableId& id) const {
    RETURN_NOT_OK(Status(failure_));
    auto it = tables_.find(id);
    SCHECK(it != tables_.end(), NotFound, "Table is not in frozen catalog: " + id);
    return it->second;
  }

  Status OpenTable(const TableId& id, tserver::PgOpenTableResponsePB* response) const {
    const auto& table = VERIFY_RESULT_REF(Find(id)).metadata;
    auto& info = *response->mutable_info();
    *info.mutable_schema() = table.schema();
    info.set_version(table.schema_version());
    *info.mutable_partition_schema() = table.partition_schema();
    info.set_create_table_done(true);
    info.set_table_type(table.table_type());
    auto* identifier = info.mutable_identifier();
    identifier->set_table_id(id);
    identifier->set_table_name(table.table_name());
    auto* ns = identifier->mutable_namespace_();
    ns->set_id(GetPgsqlNamespaceId(VERIFY_RESULT(GetPgsqlDatabaseOidByTableId(id))));
    ns->set_name(table.namespace_name());
    ns->set_database_type(YQL_DATABASE_PGSQL);
    *info.mutable_indexes() = table.indexes();
    if (table.has_index_info()) *info.mutable_index_info() = table.index_info();
    // PgGate reports constraint violations against this OID, so a rewritten relation must name
    // its logical OID rather than its relfilenode, as master does.
    if (!table.pg_table_id().empty()) info.set_pg_table_id(table.pg_table_id());
    const auto& descriptor = VERIFY_RESULT_REF(Find(id));
    if (descriptor.application) {
      for (const auto& partition : descriptor.partitions) {
        response->mutable_partitions()->add_keys(partition.partition_key_start());
      }
    } else {
      response->mutable_partitions()->add_keys("");
    }
    return Status::OK();
  }

  Status Read(
      const PgsqlReadRequestMsg& request, PgsqlResponseMsg* response, rpc::Sidecars* sidecars,
      CoarseTimePoint deadline, HybridTime selected_time = HybridTime::kInvalid) {
    if (!selected_time.is_valid()) selected_time = ReadTime();
    if (transaction_) RETURN_NOT_OK(transaction_->ValidateReadTime(selected_time));
    SCHECK(
        transaction_ || !request.has_row_mark_type() || request.row_mark_type() == ROW_MARK_ABSENT,
        NotSupported, "Frozen catalog does not support row locks");
    // Mutable catalog row locks are uncontended: their requester holds the exclusive lease.
    SCHECK_EQ(
        request.metrics_capture(), PGSQL_METRICS_CAPTURE_NONE, NotSupported,
        "Frozen catalog metrics collection is not implemented");
    SCHECK(
        !request.has_sampling_state() && !request.has_vector_idx_options() &&
            !request.has_get_tablet_key_ranges_request() && !request.is_for_backfill(),
        NotSupported, "Unsupported frozen catalog scan mode");
    SCHECK(
        !request.paging_state().has_read_time() ||
            ReadHybridTime::FromPB(request.paging_state().read_time()).read == selected_time,
        NotSupported, "Historical catalog paging is not supported");
    const auto& table = VERIFY_RESULT_REF(Find(request.table_id().ToBuffer()));
    SCHECK_EQ(
        request.schema_version(), table.metadata.schema_version(), InvalidArgument,
        "Frozen catalog schema version mismatch");
    const docdb::DocReadContext* index = nullptr;
    if (request.has_index_request()) {
      SCHECK(
          !table.application, NotSupported,
          "Nested application index reads require separate column-family storage");
      const auto& index_table =
          VERIFY_RESULT_REF(Find(request.index_request().table_id().ToBuffer()));
      index = index_table.context.get();
      SCHECK_EQ(
          request.index_request().schema_version(), index_table.metadata.schema_version(),
          InvalidArgument, "Frozen index schema version mismatch");
    }
    RETURN_NOT_OK(Status(failure_));
    auto doc_db = docdb::DocDB::FromRegularUnbounded(
        transaction_ ? VERIFY_RESULT(
                           transaction_->ReadDB(table.application ? table.metadata.table_id() : ""))
                     : db_.get());
    docdb::EncodedPartitionBounds bounds;
    docdb::KeyBounds key_bounds;
    size_t tablet_index = 0;
    ThreadSafeArena normalized_arena;
    LWPgsqlReadRequestPB normalized(&normalized_arena);
    const auto* effective = &request;
    if (table.application) {
      auto req = request.ToGoogleProtobuf();
      RETURN_NOT_OK(dockv::InitPartitionKey(table.context->schema(), table.partition_schema, &req));
      SCHECK(
          request.is_forward_scan() || table.partitions.size() == 1 ||
              (req.has_hash_code() && req.has_max_hash_code() &&
               req.hash_code() == req.max_hash_code()),
          NotSupported, "Reverse application scans require one hash code or one partition");
      auto position = table.partitions.end();
      if (request.is_forward_scan()) {
        position = std::upper_bound(
            table.partitions.begin(), table.partitions.end(), req.partition_key(),
            [](const auto& key, const auto& partition) {
              return key < partition.partition_key_start();
            });
      } else if (!req.partition_key().empty()) {
        // Native reverse partition keys are exclusive; an empty key means the last partition.
        position = std::lower_bound(
            table.partitions.begin(), table.partitions.end(), req.partition_key(),
            [](const auto& partition, const auto& key) {
              return partition.partition_key_start() < key;
            });
      }
      SCHECK(position != table.partitions.begin(), InvalidArgument, "Invalid scan partition key");
      tablet_index = std::distance(table.partitions.begin(), position) - 1;
      const auto& partition = table.partitions[tablet_index];
      const auto lower = VERIFY_RESULT(
          table.partition_schema.GetEncodedPartitionKey(partition.partition_key_start()));
      const auto upper = VERIFY_RESULT(
          table.partition_schema.GetEncodedPartitionKey(partition.partition_key_end()));
      bounds = {.start_key = KeyBuffer(lower), .end_key = KeyBuffer(upper)};
      key_bounds = docdb::KeyBounds(lower, upper);
      doc_db.key_bounds = &key_bounds;
      normalized.CopyFrom(req);
      effective = &normalized;
    }
    docdb::QLRocksDBStorage storage("local-runtime", doc_db, bounds);
    auto read_data = docdb::ReadOperationData::FromSingleReadTime(selected_time);
    read_data.deadline = deadline;
    ScopedRWOperation pending(&operations_, deadline);
    RETURN_NOT_OK(pending);
    const TransactionOperationContext transaction;
    docdb::PgsqlReadOperationData data{
        .read_operation_data = read_data,
        .is_explicit_request_read_time = true,
        .request = *effective,
        .doc_read_context = *table.context,
        .index_doc_read_context = index,
        .txn_op_context = transaction,
        .ql_storage = storage,
        .pending_op = pending,
        .vector_index = nullptr,
    };
    WriteBuffer rows(4096);
    ReadRestartData restart;
    docdb::PgsqlReadOperation operation(data, *response, &rows, &restart);
    const auto row_count = VERIFY_RESULT(operation.Execute());
    if (response->has_paging_state()) {
      response->mutable_paging_state()->set_total_num_rows_read(
          request.paging_state().total_num_rows_read() + row_count);
    }
    if (table.application && request.is_forward_scan() && request.return_paging_state() &&
        response->paging_state().next_row_key().empty() &&
        request.partition_column_values().empty() && !request.has_ybctid_column_value()) {
      const auto& end = table.partitions[tablet_index].partition_key_end();
      if (!end.empty() &&
          (!request.has_max_hash_code() ||
           request.max_hash_code() >= dockv::PartitionSchema::DecodeMultiColumnHashValue(end))) {
        auto* paging = response->mutable_paging_state();
        paging->dup_next_partition_key(end);
        paging->dup_next_tablet_bound(end);
        paging->set_total_num_rows_read(request.paging_state().total_num_rows_read() + row_count);
      }
    }
    SCHECK(!restart.is_valid(), IllegalState, "Local transaction requested read restart");
    response->set_status(PgsqlResponsePB::PGSQL_STATUS_OK);
    sidecars->Start().Take(&rows);
    response->set_rows_data_sidecar(static_cast<int>(sidecars->Complete()));
    ++reads_;
    return Status::OK();
  }

  std::unique_ptr<PgCatalogTransaction> transaction_;
  Status failure_;
  std::map<PgOid, std::string> databases_;
  std::map<PgOid, std::string> seed_databases_;
  // Slots cannot follow sorted OIDs: a later database may sort before the active backend's DB.
  std::map<PgOid, size_t> catalog_slots_;
  std::map<PgOid, uint64_t> versions_;
  uint64_t max_version_ = 0;
  std::unordered_map<TableId, Table> tables_;
  std::atomic<uint64_t> reads_{0};

 private:
  dockv::SchemaPackingRegistryPtr packings_ =
      std::make_shared<dockv::SchemaPackingRegistry>("local-catalog");
  std::unique_ptr<rocksdb::DB> db_;
  RWOperationCounter operations_{"local-catalog"};
  HybridTime read_time_;
};

struct SharedMemoryCounters {
  std::atomic<uint64_t> requests{0};
  std::atomic<uint64_t> segment_responses{0};
  std::atomic<uint64_t> saved_responses{0};

  SharedMemoryUse Snapshot() const {
    return {
        .requests = requests.load(std::memory_order_relaxed),
        .segment_responses = segment_responses.load(std::memory_order_relaxed),
        .saved_responses = saved_responses.load(std::memory_order_relaxed),
    };
  }
};

// Where a Perform or object-lock response goes. PgGate sends a backend's request through its
// shared-memory exchange when that is idle and over TCP otherwise, so a request on either
// transport may be parked until its predecessor arrives on the other.
class Responder {
 public:
  virtual ~Responder() = default;
  virtual rpc::Sidecars& sidecars() = 0;
  virtual CoarseTimePoint deadline() const = 0;
  // Called once, after the response message is complete.
  virtual void Send() = 0;
};

class RpcResponder : public Responder {
 public:
  explicit RpcResponder(rpc::RpcContext context) : context_(std::move(context)) {}

  rpc::Sidecars& sidecars() override { return context_.sidecars(); }
  CoarseTimePoint deadline() const override { return context_.GetClientDeadline(); }
  void Send() override { context_.RespondSuccess(); }

 private:
  rpc::RpcContext context_;
};

std::byte* WriteVarint32ToArray(uint32_t value, std::byte* out) {
  return pointer_cast<std::byte*>(google::protobuf::io::CodedOutputStream::WriteVarint32ToArray(
      value, pointer_cast<uint8_t*>(out)));
}

std::atomic<bool>& InUseFlag(const tserver::SharedMemorySegmentHandle& handle) {
  return *pointer_cast<std::atomic<bool>*>(handle.address() - sizeof(std::atomic<bool>));
}

// One backend's shared-memory exchange, polled by a dedicated thread as in the tablet server.
// Responses that outgrow the exchange go to a pooled segment that PostgreSQL maps by id, and
// larger ones wait for FetchData. Guarded by the service mutex, except for the thread itself.
class SharedTransport {
 public:
  SharedTransport(
      tserver::PgSessionSharedMemoryManager memory, tserver::PgSharedMemoryPool& segments,
      SharedMemoryCounters& counters)
      : memory_(std::move(memory)), segments_(segments), counters_(counters) {}

  tserver::SharedExchange& exchange() { return memory_.exchange(); }
  SharedMemoryCounters& counters() { return counters_; }

  Status Start(YBThreadPool& threads, tserver::SharedExchangeListener listener) {
    runnable_ = std::make_shared<tserver::SharedExchangeRunnable>(
        memory_.exchange(), memory_.session_id(), std::move(listener));
    return runnable_->Start(threads);
  }

  void StartShutdown() {
    if (runnable_) runnable_->StartShutdown();
  }

  // Waits for the polling thread, which may need the service mutex to finish its request.
  void CompleteShutdown() {
    if (runnable_) runnable_->CompleteShutdown();
  }

  // PostgreSQL clears the flag once it has copied a segment response, before its next request.
  // A request that finds it set follows an abandoned response, as after a timeout.
  void ReleaseAbandonedSegment() {
    if (segment_) InUseFlag(segment_).store(false, std::memory_order_release);
  }

  // Returns where to write a response of `size` bytes and the size word that tells PostgreSQL to
  // read it from the segment, or nullptr when no segment is that large.
  std::pair<std::byte*, size_t> ObtainSegment(size_t size) {
    if (segment_ && segment_.size() >= size) {
      InUseFlag(segment_).store(true, std::memory_order_release);
    } else {
      auto handle = segments_.Obtain(size + sizeof(std::atomic<bool>));
      if (!handle) return {nullptr, 0};
      new (handle.address()) std::atomic<bool>(true);
      handle.TruncateLeft(sizeof(std::atomic<bool>));
      segment_ = std::move(handle);
    }
    return {
        segment_.address(), tserver::kTooBigResponseMark | tserver::kBigSharedMemoryMark | size |
                                (segment_.id() << tserver::kBigSharedMemoryIdShift)};
  }

  size_t Save(const RefCntBuffer& buffer, WriteBuffer&& sidecars) {
    auto slot = std::ranges::find_if(saved_, [](auto& data) { return data.empty(); });
    if (slot == saved_.end()) slot = saved_.emplace(saved_.end(), 0);
    slot->AddBlock(buffer, 0);
    slot->Take(&sidecars);
    return slot - saved_.begin();
  }

  Status Fetch(uint64_t id, WriteBuffer* out) {
    SCHECK(
        id < saved_.size() && !saved_[id].empty(), NotFound, "Saved local response not found");
    out->Take(&saved_[id]);
    return Status::OK();
  }

 private:
  tserver::PgSessionSharedMemoryManager memory_;
  tserver::PgSharedMemoryPool& segments_;
  SharedMemoryCounters& counters_;
  std::shared_ptr<tserver::SharedExchangeRunnable> runnable_;
  tserver::SharedMemorySegmentHandle segment_;
  std::vector<WriteBuffer> saved_;
};

// A request read from a backend's exchange. It keeps its own copy of the request because the
// response is written over the exchange.
template <class Request, class Response>
class SharedCall : public Responder {
 public:
  explicit SharedCall(SharedTransport& transport)
      : request(&arena_), response(&arena_), transport_(transport) {}

  ~SharedCall() override { LOG_IF(DFATAL, !sent_) << "Shared memory request not answered"; }

  // Layout written by PgClient::PrepareAndSend: timeout in milliseconds, ASH metadata, trace
  // context, then the request.
  Status Parse(const std::byte* data, size_t size) {
    SCHECK_GE(size, sizeof(uint64_t), Corruption, "Truncated shared memory request");
    auto* input = static_cast<uint8_t*>(arena_.AllocateBytes(size));
    memcpy(input, data, size);
    const auto* end = input + size;
    deadline_ = CoarseMonoClock::Now() + std::chrono::milliseconds(LittleEndian::Load64(input));
    input += sizeof(uint64_t);
    AshMetadataPB ash;
    RETURN_NOT_OK(
        rpc::ParseMetadataFromSharedMemory(&input, end - input, rpc::AnyMessagePtr(&ash)));
    uint32_t trace_size = 0;
    input = const_cast<uint8_t*>(GetVarint32Ptr(input, end, &trace_size));
    SCHECK(
        input && trace_size <= static_cast<size_t>(end - input), Corruption,
        "Unable to parse trace context size");
    input += trace_size;
    return request.ParseFromSlice(Slice(input, end));
  }

  rpc::Sidecars& sidecars() override { return sidecars_; }
  CoarseTimePoint deadline() const override { return deadline_; }

  // Same framing as an RPC response, which is how PgGate parses it.
  void Send() override {
    sent_ = true;
    rpc::ResponseHeader header;
    header.set_call_id(42);
    const auto response_size = response.SerializedSize();
    sidecars_.MoveOffsetsTo(response_size, header.mutable_sidecar_offsets());
    const auto header_size = header.ByteSize();
    const auto body_size = narrow_cast<uint32_t>(response_size + sidecars_.size());
    using google::protobuf::io::CodedOutputStream;
    const size_t full_size = CodedOutputStream::VarintSize32(header_size) + header_size +
                             CodedOutputStream::VarintSize32(body_size) + body_size;
    auto& exchange = transport_.exchange();
    auto* start = exchange.Obtain(full_size);
    size_t size_word = full_size;
    RefCntBuffer buffer;
    if (!start) {
      std::tie(start, size_word) = transport_.ObtainSegment(full_size);
      transport_.counters().segment_responses.fetch_add(start != nullptr);
    }
    if (!start) {
      transport_.counters().saved_responses.fetch_add(1);
      buffer = RefCntBuffer(full_size - sidecars_.size());
      start = pointer_cast<std::byte*>(buffer.data());
    }
    auto* out = WriteVarint32ToArray(header_size, start);
    out = pointer_cast<std::byte*>(
        header.SerializeWithCachedSizesToArray(pointer_cast<uint8_t*>(out)));
    out = WriteVarint32ToArray(body_size, out);
    out = response.SerializeToArray(out);
    if (buffer) {
      size_word = tserver::kTooBigResponseMark |
                  transport_.Save(buffer, std::move(sidecars_.buffer()));
    } else {
      sidecars_.CopyTo(out);
    }
    exchange.Respond(size_word);
    exchange.ResetBusy();
  }

 private:
  ThreadSafeArena arena_;

 public:
  Request request;
  Response response;

 private:
  SharedTransport& transport_;
  rpc::Sidecars sidecars_;
  CoarseTimePoint deadline_;
  bool sent_ = false;
};

class ReadonlyService : public UnsupportedPgClientService,
                        public std::enable_shared_from_this<ReadonlyService> {
 private:
  // The responder owns what backs these pointers: the RpcContext's inbound call, or the shared
  // memory call's copy. Drain before messenger shutdown.
  struct PendingPerform {
    const tserver::PgPerformRequestMsg* request;
    tserver::PgPerformResponseMsg* response;
    std::unique_ptr<Responder> responder;
    CoarseTimePoint deadline;
    size_t bytes;
  };
  // Where a Perform resumes after waiting: ops before op_index have their responses, and the
  // statement limit and snapshot are the ones chosen when it first ran.
  struct PerformState {
    bool started = false;
    HybridTime statement_time;
    HybridTime snapshot;
    size_t op_index = 0;
    // The op at op_index already has a response slot from its first attempt.
    bool resume = false;
    LockWait wait;
  };
  // A Perform waiting for conflicting transactions to finish, as in a tablet's wait queue.
  struct ConflictWait {
    const tserver::PgPerformRequestMsg* request;
    tserver::PgPerformResponseMsg* response;
    std::unique_ptr<Responder> responder;
    PerformState state;
    TransactionId txn;
    HybridTime wait_start;
    CoarseTimePoint deadline;
    uint64_t timer = 0;
  };
  struct Session {
    Session(uint64_t id_, pid_t pid_) : id(id_), pid(pid_) {}

    void ResetTransaction() {
      txn_serial = 0;
      sql_serial = 0;
      sql_active = false;
      native_txn.reset();
      autonomous = false;
      transaction_failed = false;
      failed_subtxn = 0;
    }

    const uint64_t id;
    const pid_t pid;
    PgOid database_oid = 0;
    LocalRequestSequence sequence;
    std::map<uint64_t, PendingPerform> pending;
    size_t pending_bytes = 0;
    bool timer_armed = false;
    bool sequence_broken = false;
    uint64_t txn_serial = 0;
    uint64_t sql_serial = 0;
    // Open SQL transaction, including read-only ones without native state, until its finish.
    bool sql_active = false;
    // Snapshot for this backend's independent reads and writes. Like PgClientSession, a new
    // read_time_serial_no picks a new point: once per transaction for snapshot isolation, once per
    // statement for READ COMMITTED.
    uint64_t read_serial = 0;
    HybridTime read_point;
    // The joined catalog transaction for transaction_owner_, otherwise an independent one.
    std::optional<TransactionId> native_txn;
    bool autonomous = false;
    bool transaction_failed = false;
    uint32_t failed_subtxn = 0;
    std::shared_ptr<SharedTransport> transport;
    // Later Performs of the session queue behind it in pending. A tablet server would start them,
    // but they hold no locks yet, so holding them back cannot extend another backend's wait.
    std::unique_ptr<ConflictWait> waiter;
  };

 public:
  ReadonlyService(
      const scoped_refptr<MetricEntity>& entity, FrozenCatalog& catalog, rpc::Scheduler& scheduler,
      std::function<void()> publish_versions, std::string instance_id)
      : UnsupportedPgClientService(entity),
        catalog_(catalog),
        scheduler_(scheduler),
        publish_versions_(std::move(publish_versions)),
        instance_id_(std::move(instance_id)) {
    if (!FLAGS_local_runtime_shared_memory) return;
    segments_ =
        std::make_unique<tserver::PgSharedMemoryPool>(MemTracker::GetRootTracker(), instance_id_);
    segments_->Start(scheduler_);
    exchange_threads_ = std::make_unique<YBThreadPool>(ThreadPoolOptions{
        .name = "local_shmem",
        .max_workers = ThreadPoolOptions::kUnlimitedWorkersWithoutQueue,
    });
  }

  template <class Response>
  void Respond(const Status& status, Response* response, Responder& responder) {
    const auto& result = status.ok() ? catalog_.failure_ : status;
    if (!result.ok()) StatusToPB(result, response->mutable_status());
    responder.Send();
  }

  template <class Response>
  void Respond(const Status& status, Response* response, rpc::RpcContext& context) {
    const auto& result = status.ok() ? catalog_.failure_ : status;
    if (!result.ok()) StatusToPB(result, response->mutable_status());
    context.RespondSuccess();
  }

  Status AbortOwnedTransaction(Session& session) {
    if (exclusive_owner_ == session.id) exclusive_owner_ = 0;
    if (transaction_owner_ != session.id && !session.native_txn) return Status::OK();
    const auto txn = session.native_txn;
    Status status;
    if (transaction_owner_ != session.id) {
      session.native_txn.reset();
      // A deadlock or conflict abort may already have ended it; finished write-less ones are gone.
      status = catalog_.transaction_->NativeTransactions().Abort(*txn);
      if (status.IsNotFound()) status = Status::OK();
    } else {
      CHECK(catalog_.transaction_);
      catalog_.transaction_->Abort();
      transaction_owner_ = 0;
      catalog_.failure_ = catalog_.ReloadTables();
      status = catalog_.failure_;
    }
    if (txn) Wake(*txn);
    return status;
  }

  void RetireSession(Session& session) {
    CancelWaiter(session, STATUS(Aborted, "Local backend has exited"));
    WARN_NOT_OK(AbortOwnedTransaction(session), "Aborting exited local backend");
    CancelPending(session, STATUS(Aborted, "Local backend has exited"));
    if (session.transport) {
      session.transport->StartShutdown();
      // Joined outside the mutex, which its polling thread may be waiting for.
      retired_.push_back(std::move(session.transport));
      exchange_threads_->EnqueueFunctor([weak = weak_from_this()] {
        if (auto self = weak.lock()) self->JoinRetired();
      });
    }
    sessions_.erase(session.id);
  }

  void JoinRetired() {
    decltype(retired_) retired;
    {
      std::lock_guard lock(mutex_);
      retired.swap(retired_);
    }
    for (auto& transport : retired) transport->CompleteShutdown();
  }

  void Heartbeat(
      const tserver::PgHeartbeatRequestPB* req, tserver::PgHeartbeatResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    if (shutting_down_) {
      Respond(STATUS(ServiceUnavailable, "Local runtime is shutting down"), resp, context);
      return;
    }
    if (req->session_id() == std::numeric_limits<uint64_t>::max()) {
      Respond(Status::OK(), resp, context);
      return;
    }
    if (req->session_id() != 0) {
      Respond(CheckSession(req->session_id()), resp, context);
      return;
    }
    if (req->pid() == 0 || req->pid() > static_cast<uint32_t>(std::numeric_limits<pid_t>::max())) {
      Respond(STATUS(InvalidArgument, "Invalid backend PID"), resp, context);
      return;
    }
    for (auto it = sessions_.begin(); it != sessions_.end();) {
      auto session = (it++)->second;
      if (kill(session->pid, 0) != 0 && errno == ESRCH) RetireSession(*session);
    }
    if (sessions_.size() >= 1024 || next_session_ >= std::numeric_limits<uint64_t>::max() - 1) {
      Respond(STATUS(ServiceUnavailable, "Local session limit reached"), resp, context);
      return;
    }
    const auto id = ++next_session_;
    auto session = std::make_shared<Session>(id, static_cast<pid_t>(req->pid()));
    sessions_.emplace(id, session);
    resp->set_session_id(id);
    // An instance_id makes PgGate open the session's exchange; without one it uses TCP alone.
    if (exchange_threads_) {
      const auto status = StartTransport(*session);
      if (status.ok()) {
        resp->set_instance_id(instance_id_);
      } else {
        LOG(WARNING) << "Local session " << id << " uses TCP only: " << status;
      }
    }
    Respond(Status::OK(), resp, context);
  }

  Status StartTransport(Session& session) {
    auto memory = VERIFY_RESULT(tserver::PgSessionSharedMemoryManager::Make(
        instance_id_, session.id, tserver::Create::kTrue));
    auto transport =
        std::make_shared<SharedTransport>(std::move(memory), *segments_, shared_use_);
    // The transport outlives its polling thread, so the listener may hold it by reference.
    RETURN_NOT_OK(transport->Start(
        *exchange_threads_,
        [weak = weak_from_this(), id = session.id, &transport = *transport](size_t size) {
          if (auto self = weak.lock()) self->HandleShared(id, transport, size);
        }));
    session.transport = std::move(transport);
    return Status::OK();
  }

  // Runs on the session's polling thread. Every path answers the request exactly once.
  void HandleShared(uint64_t id, SharedTransport& transport, size_t size) {
    shared_use_.requests.fetch_add(1, std::memory_order_relaxed);
    const auto* data = transport.exchange().Obtain(size);
    const auto type = size && data ? static_cast<int>(*pointer_cast<const uint8_t*>(data)) : -1;
    switch (type) {
      case tserver::PgSharedExchangeReqType::PERFORM: {
        auto call = std::make_unique<
            SharedCall<tserver::LWPgPerformRequestPB, tserver::LWPgPerformResponsePB>>(transport);
        auto status = call->Parse(data + 1, size - 1);
        std::lock_guard lock(mutex_);
        transport.ReleaseAbandonedSegment();
        if (status.ok() && call->request.session_id() != id) {
          status = STATUS(InvalidArgument, "Shared memory request for another local session");
        }
        if (!status.ok()) {
          Respond(status, &call->response, *call);
          return;
        }
        auto* req = &call->request;
        auto* resp = &call->response;
        DoPerform(req, resp, std::move(call));
        return;
      }
      case tserver::PgSharedExchangeReqType::ACQUIRE_OBJECT_LOCK: {
        using LockCall = SharedCall<
            tserver::LWPgAcquireObjectLockRequestPB, tserver::LWPgAcquireObjectLockResponsePB>;
        LockCall call(transport);
        auto status = call.Parse(data + 1, size - 1);
        std::lock_guard lock(mutex_);
        transport.ReleaseAbandonedSegment();
        if (status.ok() && call.request.session_id() != id) {
          status = STATUS(InvalidArgument, "Shared memory request for another local session");
        }
        if (status.ok()) status = ObjectLockStatus(call.request);
        Respond(status, &call.response, call);
        return;
      }
      default:
        break;
    }
    // PgGate sends no other types; an empty answer fails the backend's request.
    LOG(DFATAL) << "Unexpected shared memory request type " << type << " for session " << id;
    transport.exchange().Respond(0);
    transport.exchange().ResetBusy();
  }

  Status CheckSession(uint64_t id) {
    std::lock_guard lock(mutex_);
    SCHECK(!shutting_down_, ServiceUnavailable, "Local runtime is shutting down");
    const auto it = sessions_.find(id);
    SCHECK(it != sessions_.end(), InvalidArgument, "Unknown local session");
    auto session = it->second;
    // A call on the session's own connection shows its backend is alive. Exited backends are
    // retired when a session is created or another session needs exclusive ownership.
    if (!catalog_.failure_.ok()) {
      CancelWaiter(*session, catalog_.failure_);
      CancelPending(*session, catalog_.failure_);
    }
    return catalog_.failure_;
  }

  // Fails while another backend holds the exclusive lease. Data transactions do not take it.
  Status CheckExclusive(uint64_t id) {
    TEST_SYNC_POINT_CALLBACK("LocalRuntime::BeforeSqlOwnerCheck", &id);
    if (exclusive_owner_ && sessions_.contains(exclusive_owner_)) {
      const auto owner = sessions_.at(exclusive_owner_);
      if (kill(owner->pid, 0) != 0 && errno == ESRCH) {
        // The requesting handler may retain only a Session&. Retire it on a later request.
        if (owner->id == id) return STATUS(Aborted, "Local backend has exited");
        RetireSession(*owner);
      }
    }
    if (exclusive_owner_ && exclusive_owner_ != id) {
      return STATUS(
          TryAgain, "Another local backend holds the exclusive catalog lease",
          TransactionError(TransactionErrorCode::kConflict));
    }
    return Status(catalog_.failure_);
  }

  // Catalog changes and strong object locks require that no other backend has an open SQL
  // transaction: the catalog overlay is shared, and the lease substitutes for the table locks
  // those transactions would otherwise hold.
  Status AcquireExclusive(Session& session) {
    RETURN_NOT_OK(CheckExclusive(session.id));
    if (exclusive_owner_ == session.id) return Status::OK();
    for (auto it = sessions_.begin(); it != sessions_.end();) {
      const auto other = (it++)->second;
      if (other->id == session.id || !other->sql_active) continue;
      if (kill(other->pid, 0) != 0 && errno == ESRCH) {
        RetireSession(*other);
        continue;
      }
      return STATUS(
          TryAgain, "Concurrent SQL transactions block local catalog changes",
          TransactionError(TransactionErrorCode::kConflict));
    }
    exclusive_owner_ = session.id;
    return Status::OK();
  }

  Status AcquireSqlTransaction(Session& session, const tserver::LWPgPerformOptionsPB& options) {
    if (!catalog_.transaction_) return Status::OK();
    RETURN_NOT_OK(CheckExclusive(session.id));
    if (options.connected_database_oid()) {
      SCHECK(
          session.database_oid == 0 || session.database_oid == options.connected_database_oid(),
          InvalidArgument, "Backend database identity changed");
      session.database_oid = options.connected_database_oid();
    }
    if (!options.sql_transaction_active()) return Status::OK();
    SCHECK(
        options.is_using_table_locks(), NotSupported,
        "Local SQL requires native table locking and transaction-end notifications");
    SCHECK(
        !session.sql_active || session.sql_serial == options.txn_serial_no(), NotSupported,
        "SQL transaction end notification missing; reconnect required");
    session.sql_active = true;
    session.sql_serial = options.txn_serial_no();
    return Status::OK();
  }

  Status CheckMetadata(uint64_t id) {
    if (id) RETURN_NOT_OK(CheckSession(id));
    RETURN_NOT_OK(Status(catalog_.failure_));
    // Legacy callers may see committed descriptors, never another backend's private DDL.
    return CheckExclusive(id);
  }

  std::optional<uint64_t> Predecessor(const tserver::PgPerformRequestMsg& req) const {
    return req.sequence_num().has_predecessor_serial_no()
               ? std::make_optional(req.sequence_num().predecessor_serial_no().value())
               : std::nullopt;
  }

  void CancelPending(Session& session, const Status& status) {
    for (auto& [serial, call] : session.pending) Respond(status, call.response, *call.responder);
    pending_calls_ -= session.pending.size();
    pending_bytes_ -= session.pending_bytes;
    session.pending.clear();
    session.pending_bytes = 0;
  }

  void FailSequence(Session& session, const Status& status) {
    session.sequence_broken = true;
    session.transaction_failed = true;
    CancelWaiter(session, status);
    CancelPending(session, status);
    WARN_NOT_OK(AbortOwnedTransaction(session), "Aborting failed local request sequence");
  }

  void Shutdown() override {
    decltype(retired_) transports;
    bool stop_threads = false;
    {
      std::lock_guard lock(mutex_);
      shutting_down_ = true;
      for (auto& [id, session] : sessions_) {
        CancelWaiter(*session, STATUS(ServiceUnavailable, "Local runtime is shutting down"));
        CancelPending(*session, STATUS(ServiceUnavailable, "Local runtime is shutting down"));
        if (session->transport) {
          session->transport->StartShutdown();
          transports.push_back(std::move(session->transport));
        }
      }
      std::ranges::move(retired_, std::back_inserter(transports));
      retired_.clear();
      stop_threads = exchange_threads_ && !std::exchange(exchange_threads_stopped_, true);
    }
    for (auto& transport : transports) transport->CompleteShutdown();
    transports.clear();
    if (stop_threads) exchange_threads_->Shutdown();
  }

  Status CheckNoPending(Session& session) {
    if (!session.pending.empty()) {
      const auto status =
          STATUS(IllegalState, "Transaction control overtook pending Perform requests");
      FailSequence(session, status);
      return status;
    }
    SCHECK(
        !session.sequence_broken, IllegalState,
        "Local request sequence failed; reconnect required");
    return Status::OK();
  }

  void SchedulePendingCheck(Session& session) {
    if (session.timer_armed || shutting_down_ || session.pending.empty()) return;
    session.timer_armed = true;
    scheduler_.Schedule(
        [weak = weak_from_this(), id = session.id](const Status& status) {
          if (!status.ok()) return;
          if (auto self = weak.lock()) self->CheckPendingDeadline(id);
        },
        std::chrono::milliseconds(100));
  }

  void CheckPendingDeadline(uint64_t id) {
    std::lock_guard lock(mutex_);
    if (!CheckSession(id).ok()) return;
    auto session = sessions_.at(id);
    session->timer_armed = false;
    // An exited backend never sends the predecessor that would release its parked requests.
    if (kill(session->pid, 0) != 0 && errno == ESRCH) {
      RetireSession(*session);
      return;
    }
    const auto now = CoarseMonoClock::Now();
    for (const auto& [serial, call] : session->pending) {
      if (call.deadline <= now) {
        FailSequence(
            *session, STATUS(TimedOut, "Missing local request predecessor; reconnect required"));
        return;
      }
    }
    SchedulePendingCheck(*session);
  }

  void ExecutePerform(
      Session& session, const tserver::PgPerformRequestMsg* req,
      tserver::PgPerformResponseMsg* resp, std::unique_ptr<Responder> responder) {
    auto status = session.sequence.Register(req->sequence_num().serial_no(), Predecessor(*req));
    PerformState state;
    if (status.ok()) status = PerformReads(session, *req, resp, *responder, state);
    if (status.ok() && !state.wait.blockers.empty()) {
      const auto deadline = responder->deadline();
      WaitForBlockers(
          session, std::make_unique<ConflictWait>(ConflictWait{
                       .request = req,
                       .response = resp,
                       .responder = std::move(responder),
                       .state = std::move(state),
                       .txn = *session.native_txn,
                       .wait_start = HybridTime::FromMicros(GetCurrentTimeMicros()),
                       .deadline = deadline}));
      return;
    }
    FinishPerform(session, *req, resp, *responder, status);
  }

  void FinishPerform(
      Session& session, const tserver::PgPerformRequestMsg& req,
      tserver::PgPerformResponseMsg* resp, Responder& responder, const Status& status) {
    if (!status.ok()) {
      // PgGate skips a SKIP LOCKED row reported as kSkipLocking; its statement continues.
      const bool skipped =
          TransactionError::ValueFromStatus(status) == TransactionErrorCode::kSkipLocking;
      if (catalog_.transaction_ && !skipped) {
        session.transaction_failed = true;
        session.failed_subtxn = std::max(1u, req.options().active_sub_transaction_id());
      }
      resp->Clear();
      responder.sidecars().Reset();
    }
    Respond(status, resp, responder);
  }

  // Parks a Perform until its blockers finish, like a tablet's wait queue. It fails at its
  // deadline, or as the deadlock detector would fail it when its wait closes a cycle.
  void WaitForBlockers(Session& session, std::unique_ptr<ConflictWait> wait) {
    if (CoarseMonoClock::Now() >= wait->deadline) {
      FinishWait(
          session, std::move(wait),
          STATUS(TimedOut, "Failed to resume waiter transaction from queue within deadline"));
      return;
    }
    const auto cycle = FindCycle(wait->txn, wait->state.wait.blockers);
    std::optional<TransactionId> victim;
    if (!cycle.empty()) victim = DeadlockVictim(cycle);
    if (victim == wait->txn) {
      AbortVictim(*victim);
      FinishWait(session, std::move(wait), DeadlockStatus(*victim, cycle));
      Wake(*victim);
      return;
    }
    waiting_txns_.emplace(wait->txn, session.id);
    session.waiter = std::move(wait);
    ArmWaitTimer(session);
    if (!victim) return;
    // Every other transaction in the cycle is itself waiting.
    const auto other = sessions_.at(waiting_txns_.at(*victim));
    AbortVictim(*victim);
    CancelWaiter(*other, DeadlockStatus(*victim, cycle));
    DrainPending(*other);
    Wake(*victim);
  }

  // Edges run from each waiting transaction to its blockers, so a new wait closes a cycle exactly
  // when one of its blockers reaches the waiter. Returns the cycle, starting at the waiter.
  std::vector<TransactionId> FindCycle(
      const TransactionId& waiter, const std::vector<TransactionId>& blockers) {
    std::unordered_map<TransactionId, TransactionId, TransactionIdHash> parent;
    std::vector<TransactionId> stack;
    for (const auto& blocker : blockers)
      if (parent.emplace(blocker, waiter).second) stack.push_back(blocker);
    while (!stack.empty()) {
      const auto id = stack.back();
      stack.pop_back();
      if (id == waiter) {
        std::vector<TransactionId> cycle;
        for (auto at = parent.at(waiter); at != waiter; at = parent.at(at)) cycle.push_back(at);
        cycle.push_back(waiter);
        std::ranges::reverse(cycle);
        return cycle;
      }
      const auto it = waiting_txns_.find(id);
      if (it == waiting_txns_.end()) continue;
      for (const auto& blocker : sessions_.at(it->second)->waiter->state.wait.blockers)
        if (parent.emplace(blocker, id).second) stack.push_back(blocker);
    }
    return {};
  }

  // The deadlock detector aborts the newest transaction in the cycle.
  TransactionId DeadlockVictim(const std::vector<TransactionId>& cycle) {
    std::unordered_map<TransactionId, int64_t, TransactionIdHash> starts;
    for (const auto& [id, start] :
         catalog_.transaction_->NativeTransactions().PendingTransactions()) {
      starts.emplace(id, start);
    }
    auto victim = TransactionId::Nil();
    int64_t newest = 0;
    for (const auto& id : cycle) {
      const auto it = starts.find(id);
      const auto start = it == starts.end() ? 0 : it->second;
      if (start > newest || (start == newest && id > victim)) {
        newest = start;
        victim = id;
      }
    }
    return victim;
  }

  static Status DeadlockStatus(
      const TransactionId& victim, const std::vector<TransactionId>& cycle) {
    return STATUS_EC_FORMAT(
               Expired, TransactionError(TransactionErrorCode::kDeadlock),
               "Transaction $0 aborted due to a deadlock: $1", victim, cycle)
        .CloneAndAddErrorCode(PgsqlError(YBPgErrorCode::YB_PG_YB_DEADLOCK));
  }

  // A catalog-owned victim cannot be aborted here; its request still fails, and PostgreSQL
  // aborts the transaction.
  void AbortVictim(const TransactionId& victim) {
    const auto status = catalog_.transaction_->NativeTransactions().AbortVictim(victim);
    if (!status.ok() && !status.IsNotSupported())
      WARN_NOT_OK(status, "Aborting local deadlock victim");
  }

  // Wakes cover every way a blocker finishes. The periodic retry also notices an exited backend,
  // and repeats conflict resolution as a tablet's wait queue refreshes its waiters.
  void ArmWaitTimer(Session& session) {
    auto& wait = *session.waiter;
    wait.timer = ++wait_timers_;
    const auto now = CoarseMonoClock::Now();
    scheduler_.Schedule(
        [weak = weak_from_this(), id = session.id, timer = wait.timer](const Status& status) {
          if (!status.ok()) return;
          if (auto self = weak.lock()) self->CheckWaiter(id, timer);
        },
        std::min(wait.deadline, now + std::chrono::seconds(1)) - now);
  }

  void CheckWaiter(uint64_t id, uint64_t timer) {
    std::lock_guard lock(mutex_);
    const auto it = sessions_.find(id);
    if (it == sessions_.end() || !it->second->waiter || it->second->waiter->timer != timer) return;
    const auto session = it->second;
    if (kill(session->pid, 0) != 0 && errno == ESRCH) {
      RetireSession(*session);
      return;
    }
    // An exited blocker's transaction is aborted, as its coordinator does once heartbeats stop.
    const auto& blockers = session->waiter->state.wait.blockers;
    std::vector<std::shared_ptr<Session>> exited;
    for (const auto& [other_id, other] : sessions_) {
      if (other->native_txn && std::ranges::find(blockers, *other->native_txn) != blockers.end() &&
          kill(other->pid, 0) != 0 && errno == ESRCH) {
        exited.push_back(other);
      }
    }
    for (const auto& other : exited) {
      if (sessions_.contains(other->id)) RetireSession(*other);
    }
    if (session->waiter) Resume(*session);
  }

  void Resume(Session& session) {
    auto wait = std::move(session.waiter);
    waiting_txns_.erase(wait->txn);
    const auto status =
        PerformReads(session, *wait->request, wait->response, *wait->responder, wait->state);
    if (status.ok() && !wait->state.wait.blockers.empty()) {
      WaitForBlockers(session, std::move(wait));
      return;
    }
    FinishWait(session, std::move(wait), status);
  }

  void FinishWait(Session& session, std::unique_ptr<ConflictWait> wait, const Status& status) {
    FinishPerform(session, *wait->request, wait->response, *wait->responder, status);
    wait.reset();
    DrainPending(session);
  }

  void CancelWaiter(Session& session, const Status& status) {
    if (!session.waiter) return;
    auto wait = std::move(session.waiter);
    waiting_txns_.erase(wait->txn);
    FinishPerform(session, *wait->request, wait->response, *wait->responder, status);
  }

  // A waiting backend sends transaction control only after it stopped waiting for the response.
  static Status OvertakenWait() {
    return STATUS(Aborted, "Transaction control overtook a waiting Perform");
  }

  // Resumes requests waiting on a transaction that finished or rolled back a savepoint, oldest
  // first. Resumption runs as its own task: callers may be retiring sessions or iterating them,
  // and a resumed request may finish or abort more transactions.
  void Wake(const TransactionId& txn) {
    std::vector<std::pair<HybridTime, uint64_t>> ready;
    for (const auto& [id, session_id] : waiting_txns_) {
      const auto& wait = *sessions_.at(session_id)->waiter;
      if (std::ranges::find(wait.state.wait.blockers, txn) != wait.state.wait.blockers.end())
        ready.emplace_back(wait.wait_start, session_id);
    }
    std::ranges::sort(ready);
    for (const auto& [start, id] : ready) wakes_.push_back(id);
    if (wakes_.empty() || shutting_down_ || std::exchange(wake_scheduled_, true)) return;
    scheduler_.Schedule(
        [weak = weak_from_this()](const Status& status) {
          if (!status.ok()) return;
          if (auto self = weak.lock()) self->ProcessWakes();
        },
        std::chrono::steady_clock::duration::zero());
  }

  void ProcessWakes() {
    std::lock_guard lock(mutex_);
    while (!wakes_.empty()) {
      const auto it = sessions_.find(wakes_.front());
      wakes_.pop_front();
      if (it == sessions_.end() || !it->second->waiter) continue;
      const auto session = it->second;
      Resume(*session);
    }
    wake_scheduled_ = false;
  }

  void DrainPending(Session& session) {
    while (!session.waiter && !session.pending.empty()) {
      auto first = session.pending.begin();
      auto ready = session.sequence.Ready(first->first, Predecessor(*first->second.request));
      if (!ready.ok()) {
        FailSequence(session, ready.status());
        return;
      }
      if (!*ready) return;
      auto call = std::move(first->second);
      session.pending_bytes -= call.bytes;
      pending_bytes_ -= call.bytes;
      --pending_calls_;
      session.pending.erase(first);
      if (call.deadline <= CoarseMonoClock::Now()) {
        const auto status = STATUS(TimedOut, "Queued local request expired; reconnect required");
        Respond(status, call.response, *call.responder);
        FailSequence(session, status);
        return;
      }
      ExecutePerform(session, call.request, call.response, std::move(call.responder));
    }
  }

  void Perform(
      const tserver::PgPerformRequestMsg* req, tserver::PgPerformResponseMsg* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    DoPerform(req, resp, std::make_unique<RpcResponder>(std::move(context)));
  }

  void DoPerform(
      const tserver::PgPerformRequestMsg* req, tserver::PgPerformResponseMsg* resp,
      std::unique_ptr<Responder> responder) {
    const auto status = CheckSession(req->session_id());
    if (!status.ok()) {
      Respond(status, resp, *responder);
      return;
    }
    auto session = sessions_.at(req->session_id());
    if (session->sequence_broken) {
      Respond(
          STATUS(IllegalState, "Local request sequence failed; reconnect required"), resp,
          *responder);
      return;
    }
    const auto serial = req->sequence_num().serial_no();
    auto ready = session->sequence.Ready(serial, Predecessor(*req));
    if (!ready.ok() || session->pending.contains(serial)) {
      const auto error =
          !ready.ok() ? ready.status() : STATUS(IllegalState, "Duplicate pending local request");
      Respond(error, resp, *responder);
      FailSequence(*session, error);
      return;
    }
    if (!*ready || session->waiter) {
      const auto bytes = req->SerializedSize();
      // Bounds apply to the whole service, not once per admitted backend.
      if (pending_calls_ >= 64 || bytes > 16ULL * 1024 * 1024 - pending_bytes_) {
        const auto error = STATUS(ServiceUnavailable, "Local request queue limit exceeded");
        Respond(error, resp, *responder);
        FailSequence(*session, error);
        return;
      }
      // A missing predecessor must arrive soon; a waiting one may take its whole deadline.
      const auto deadline = session->waiter ? responder->deadline()
                                            : std::min(
                                                  responder->deadline(),
                                                  CoarseMonoClock::Now() + std::chrono::seconds(5));
      session->pending.emplace(
          serial, PendingPerform{req, resp, std::move(responder), deadline, bytes});
      session->pending_bytes += bytes;
      pending_bytes_ += bytes;
      ++pending_calls_;
      TEST_SYNC_POINT("LocalRuntime::PerformQueued");
      SchedulePendingCheck(*session);
      return;
    }
    ExecutePerform(*session, req, resp, std::move(responder));
    DrainPending(*session);
  }

  // Catalog work joins the single catalog transaction under the exclusive lease, adopting any
  // independent data transaction this backend already started. Other work uses an independent
  // native transaction, concurrent with other backends.
  Status EnsureTransaction(
      Session& session, const tserver::LWPgPerformOptionsPB& options, bool read_only = false,
      bool catalog = true) {
    SCHECK(catalog_.transaction_, NotSupported, "Local catalog server is read-only");
    SCHECK(!session.transaction_failed, IllegalState, "Local transaction requires rollback");
    RETURN_NOT_OK(AcquireSqlTransaction(session, options));
    SCHECK(
        options.sql_transaction_active(), NotSupported,
        "Application access requires a SQL transaction");
    SCHECK(
        options.is_using_table_locks(), NotSupported,
        "Local transactions require native table locking and finish notifications");
    if (options.ddl_mode()) {
      SCHECK(
          options.ddl_use_regular_transaction_block(), NotSupported,
          "Local DDL requires native transaction blocks");
    } else {
      SCHECK(
          read_only || options.isolation() != IsolationLevel::NON_TRANSACTIONAL, NotSupported,
          "Nontransactional application writes are not supported");
    }
    if (catalog && transaction_owner_ != session.id) {
      RETURN_NOT_OK(AcquireExclusive(session));
      SCHECK(
          transaction_owner_ == 0 && !catalog_.transaction_->active(), NotSupported,
          "Another local backend owns the catalog transaction");
      if (session.native_txn) {
        SCHECK_EQ(
            session.txn_serial, options.txn_serial_no(), NotSupported,
            "Overlapping local transactions");
        RETURN_NOT_OK(catalog_.transaction_->BeginAdoptingNative(*session.native_txn));
      } else {
        session.native_txn = VERIFY_RESULT(catalog_.transaction_->BeginWithNative());
        session.txn_serial = options.txn_serial_no();
      }
      transaction_owner_ = session.id;
      session.autonomous = false;
    } else if (!session.native_txn) {
      session.native_txn = VERIFY_RESULT(
          catalog_.transaction_->NativeTransactions().BeginSql(
              static_cast<IsolationLevel>(options.isolation())));
      session.txn_serial = options.txn_serial_no();
    }
    SCHECK(
        transaction_owner_ == session.id ||
            options.isolation() != IsolationLevel::SERIALIZABLE_ISOLATION,
        NotSupported, "SERIALIZABLE isolation is not supported by the local runtime");
    SCHECK_EQ(
        session.txn_serial, options.txn_serial_no(), NotSupported,
        "Overlapping local transactions");
    return Status::OK();
  }

  void GetNewObjectId(
      const tserver::PgGetNewObjectIdRequestPB* req, tserver::PgGetNewObjectIdResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    if (!catalog_.failure_.ok()) {
      Respond(catalog_.failure_, resp, context);
      return;
    }
    if (!catalog_.transaction_ || !catalog_.versions_.contains(req->db_oid())) {
      Respond(
          STATUS(NotSupported, "Local OID allocation requires a known writable database"), resp,
          context);
      return;
    }
    auto oid = catalog_.transaction_->AllocateOid(req->db_oid());
    if (oid.ok()) resp->set_new_oid(*oid);
    Respond(ResultToStatus(oid), resp, context);
  }

  Status DropDatabase(Session& session, const tserver::PgDropDatabaseRequestPB& request) {
    SCHECK(
        catalog_.transaction_ && transaction_owner_ == session.id &&
            exclusive_owner_ == session.id &&
            catalog_.transaction_->active(),
        NotSupported, "DROP DATABASE must share the catalog transaction");
    SCHECK(
        session.database_oid && session.database_oid != request.database_oid(), InvalidArgument,
        "Cannot drop the connected database");
    for (auto it = sessions_.begin(); it != sessions_.end();) {
      const auto other = (it++)->second;
      if (other->database_oid != request.database_oid()) continue;
      if (kill(other->pid, 0) != 0 && errno == ESRCH) {
        RetireSession(*other);
      } else {
        return STATUS(IllegalState, "Database has a live local backend");
      }
    }
    return catalog_.DropDatabase(request);
  }

  void DropDatabase(
      const tserver::PgDropDatabaseRequestPB* req, tserver::PgDropDatabaseResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckSession(req->session_id());
    if (status.ok()) status = CheckNoPending(*sessions_.at(req->session_id()));
    if (status.ok()) {
      auto& session = *sessions_.at(req->session_id());
      status = DropDatabase(session, *req);
      if (!status.ok()) {
        session.transaction_failed = true;
        session.failed_subtxn = 1;
      }
    }
    Respond(status, resp, context);
  }

  struct SequenceId {
    uint32_t database_oid;
    uint32_t sequence_oid;
  };

  static Result<uint32_t> NarrowOid(int64_t oid) {
    SCHECK(
        oid > 0 && oid <= std::numeric_limits<uint32_t>::max(), InvalidArgument,
        "Invalid sequence request OID");
    return static_cast<uint32_t>(oid);
  }

  Result<std::shared_ptr<Session>> SequenceSession(uint64_t session_id) {
    SCHECK(catalog_.transaction_, NotSupported, "Frozen catalog rejects sequence access");
    RETURN_NOT_OK(CheckSession(session_id));
    auto session = sessions_.at(session_id);
    // Sequence RPCs are not ordered with Perform, so queued Performs are not a protocol error.
    SCHECK(
        !session->sequence_broken, IllegalState,
        "Local request sequence failed; reconnect required");
    return session;
  }

  // Sequence tuples are nontransactional, so admission never starts the catalog transaction or
  // takes the exclusive lease, but still excludes callers while another backend holds it.
  // Post-commit deletion (cleanup) runs after FinishTransaction released the lease, possibly to
  // another backend; it is idempotent, so only the backend's own database is checked.
  template <class Request>
  Result<SequenceId> AdmitSequence(const Request& req, bool cleanup = false) {
    const auto session = VERIFY_RESULT(SequenceSession(req.session_id()));
    const SequenceId id{
        VERIFY_RESULT(NarrowOid(req.db_oid())), VERIFY_RESULT(NarrowOid(req.seq_oid()))};
    SCHECK(
        session->database_oid != 0 && session->database_oid == id.database_oid, InvalidArgument,
        "Sequence database does not match the backend database");
    if (cleanup) return id;
    SCHECK(
        FLAGS_ysql_sequence_cache_method != "server", NotSupported,
        "Server-side sequence caching is not supported by the local runtime");
    const auto database = catalog_.databases_.find(id.database_oid);
    SCHECK(
        database != catalog_.databases_.end() && database->second != "template0" &&
            database->second != "template1",
        NotSupported, "Sequences require a known nontemplate database");
    RETURN_NOT_OK(CheckExclusive(session->id));
    return id;
  }

  void InsertSequenceTuple(
      const tserver::LWPgInsertSequenceTupleRequestPB* req,
      tserver::LWPgInsertSequenceTupleResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = [&]() -> Status {
      const auto id = VERIFY_RESULT(AdmitSequence(*req));
      return catalog_.transaction_->InsertSequence(
          id.database_oid, id.sequence_oid, {req->last_val(), req->is_called()});
    }();
    Respond(status, resp, context);
  }

  void ReadSequenceTuple(
      const tserver::LWPgReadSequenceTupleRequestPB* req,
      tserver::LWPgReadSequenceTupleResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto tuple = [&]() -> Result<PgCatalogTransaction::SequenceTuple> {
      const auto id = VERIFY_RESULT(AdmitSequence(*req));
      SCHECK(req->read_time() == 0, NotSupported, "Historical sequence reads are not supported");
      return catalog_.transaction_->ReadSequence(id.database_oid, id.sequence_oid);
    }();
    if (tuple.ok()) {
      resp->set_last_val(tuple->last_value);
      resp->set_is_called(tuple->is_called);
    }
    Respond(ResultToStatus(tuple), resp, context);
  }

  void UpdateSequenceTuple(
      const tserver::LWPgUpdateSequenceTupleRequestPB* req,
      tserver::LWPgUpdateSequenceTupleResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto skipped = [&]() -> Result<bool> {
      const auto id = VERIFY_RESULT(AdmitSequence(*req));
      std::optional<PgCatalogTransaction::SequenceTuple> expected;
      if (req->has_expected()) {
        expected = PgCatalogTransaction::SequenceTuple{
            req->expected_last_val(), req->expected_is_called()};
      }
      return catalog_.transaction_->UpdateSequence(
          id.database_oid, id.sequence_oid, {req->last_val(), req->is_called()}, expected);
    }();
    if (skipped.ok()) resp->set_skipped(*skipped);
    Respond(ResultToStatus(skipped), resp, context);
  }

  void FetchSequenceTuple(
      const tserver::LWPgFetchSequenceTupleRequestPB* req,
      tserver::LWPgFetchSequenceTupleResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto range = [&]() -> Result<std::pair<int64_t, int64_t>> {
      const auto id = VERIFY_RESULT(AdmitSequence(*req));
      return catalog_.transaction_->FetchSequence(
          id.database_oid, id.sequence_oid, req->fetch_count(), req->inc_by(), req->min_value(),
          req->max_value(), req->cycle());
    }();
    if (range.ok()) {
      resp->set_first_value(range->first);
      resp->set_last_value(range->second);
    }
    Respond(ResultToStatus(range), resp, context);
  }

  void DeleteSequenceTuple(
      const tserver::LWPgDeleteSequenceTupleRequestPB* req,
      tserver::LWPgDeleteSequenceTupleResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = [&]() -> Status {
      const auto id = VERIFY_RESULT(AdmitSequence(*req, /* cleanup= */ true));
      return catalog_.transaction_->DeleteSequence(id.database_oid, id.sequence_oid);
    }();
    Respond(status, resp, context);
  }

  // Post-commit DROP DATABASE cleanup targets another database; the store authorizes it only
  // after that database is durably retired.
  void DeleteDBSequences(
      const tserver::LWPgDeleteDBSequencesRequestPB* req,
      tserver::LWPgDeleteDBSequencesResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = [&]() -> Status {
      SCHECK(catalog_.transaction_, NotSupported, "Frozen catalog rejects sequence cleanup");
      RETURN_NOT_OK(ResultToStatus(SequenceSession(req->session_id())));
      const auto database_oid = VERIFY_RESULT(NarrowOid(req->db_oid()));
      return catalog_.transaction_->DeleteDatabaseSequences(database_oid);
    }();
    Respond(status, resp, context);
  }

  void CreateDatabase(
      const tserver::PgCreateDatabaseRequestPB* req, tserver::PgCreateDatabaseResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckSession(req->session_id());
    if (status.ok()) status = CheckNoPending(*sessions_.at(req->session_id()));
    if (status.ok()) {
      ThreadSafeArena arena;
      tserver::LWPgPerformOptionsPB options(&arena);
      options.CopyFrom(req->options());
      auto& session = *sessions_.at(req->session_id());
      status = EnsureTransaction(session, options);
      if (status.ok()) status = catalog_.CreateDatabase(*req);
      if (!status.ok() && catalog_.transaction_) {
        session.transaction_failed = true;
        session.failed_subtxn = std::max(1u, req->options().active_sub_transaction_id());
      }
    }
    Respond(status, resp, context);
  }

  void CreateTable(
      const tserver::PgCreateTableRequestPB* req, tserver::PgCreateTableResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckSession(req->session_id());
    if (status.ok()) status = CheckNoPending(*sessions_.at(req->session_id()));
    if (status.ok()) {
      ThreadSafeArena arena;
      tserver::LWPgPerformOptionsPB options(&arena);
      options.CopyFrom(req->options());
      auto& session = *sessions_.at(req->session_id());
      status = EnsureTransaction(session, options);
      if (status.ok()) status = catalog_.CreateTable(*req);
      if (!status.ok()) {
        session.transaction_failed = true;
        session.failed_subtxn = std::max(1u, req->options().active_sub_transaction_id());
      }
    }
    Respond(status, resp, context);
  }

  void AlterTable(
      const tserver::PgAlterTableRequestPB* req, tserver::PgAlterTableResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckSession(req->session_id());
    if (status.ok()) status = CheckNoPending(*sessions_.at(req->session_id()));
    if (status.ok()) {
      ThreadSafeArena arena;
      tserver::LWPgPerformOptionsPB options(&arena);
      options.CopyFrom(req->options());
      auto& session = *sessions_.at(req->session_id());
      status = EnsureTransaction(session, options);
      if (status.ok()) status = catalog_.AlterTable(*req);
      if (!status.ok() && catalog_.transaction_) {
        session.transaction_failed = true;
        session.failed_subtxn = std::max(1u, req->options().active_sub_transaction_id());
      }
    }
    Respond(status, resp, context);
  }

  void DropTable(
      const tserver::PgDropTableRequestPB* req, tserver::PgDropTableResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckSession(req->session_id());
    if (status.ok()) status = CheckNoPending(*sessions_.at(req->session_id()));
    if (status.ok()) {
      ThreadSafeArena arena;
      tserver::LWPgPerformOptionsPB options(&arena);
      options.CopyFrom(req->options());
      auto& session = *sessions_.at(req->session_id());
      status = EnsureTransaction(session, options);
      if (status.ok()) status = catalog_.DropTable(*req, resp);
      if (!status.ok() && catalog_.transaction_) {
        session.transaction_failed = true;
        session.failed_subtxn = std::max(1u, req->options().active_sub_transaction_id());
      }
    }
    Respond(status, resp, context);
  }

  // Runs a Perform's ops from state.op_index. An op that conflicts with pending transactions
  // stops the run with state.wait filled, and the caller resumes the same state once they finish.
  Status PerformReads(
      Session& session, const tserver::PgPerformRequestMsg& req,
      tserver::PgPerformResponseMsg* resp, Responder& responder, PerformState& state) {
    RETURN_NOT_OK(Status(catalog_.failure_));
    // Catalog mutations must be DDL, so DDL mode alone selects the catalog transaction.
    const bool catalog = req.options().ddl_mode();
    if (!std::exchange(state.started, true)) RETURN_NOT_OK(StartPerform(session, req, resp, state));
    auto* const wait = FLAGS_local_runtime_wait_on_conflict ? &state.wait : nullptr;
    const auto waiting = [&state, wait](const Status& status, size_t op_index) {
      if (status.ok() || !wait || wait->blockers.empty()) return false;
      state.op_index = op_index;
      state.resume = true;
      return true;
    };
    size_t op_index = 0;
    for (const auto& op : req.ops()) {
      if (op_index < state.op_index) {
        ++op_index;
        continue;
      }
      auto* slot = std::exchange(state.resume, false) ? &resp->mutable_responses()->back()
                                                      : resp->add_responses();
      if (op.has_write()) {
        RETURN_NOT_OK(EnsureTransaction(session, req.options(), false, catalog));
        const auto& table = VERIFY_RESULT_REF(catalog_.Find(op.write().table_id().ToBuffer()));
        SCHECK(
            table.application || req.options().ddl_mode(), NotSupported,
            "Catalog mutations must be part of DDL");
        if (table.application) {
          const auto status = catalog_.transaction_->NativeTransactions().WriteSql(
              *session.native_txn, op.write(), slot, &responder.sidecars(),
              req.options().active_sub_transaction_id(), state.snapshot, wait);
          if (waiting(status, op_index)) return Status::OK();
          RETURN_NOT_OK(status);
        } else {
          RETURN_NOT_OK(catalog_.Write(
              op.write(), slot, &responder.sidecars(), req.options().active_sub_transaction_id()));
        }
        const auto& response = *slot;
        if (response.status() != PgsqlResponsePB::PGSQL_STATUS_OK) {
          // Mirror PgClientSession's error envelope so PgGate aborts instead of committing a batch.
          auto error = response.error_status().empty()
                           ? STATUS(
                                 QLError, response.error_message(), Slice(),
                                 PgsqlRequestStatus(response.status()))
                           : StatusFromPB(*response.error_status().rbegin());
          if (response.has_pg_error_code())
            error = error.CloneAndAddErrorCode(
                PgsqlError(static_cast<YBPgErrorCode>(response.pg_error_code())));
          if (response.has_txn_error_code())
            error = error.CloneAndAddErrorCode(
                TransactionError(static_cast<TransactionErrorCode>(response.txn_error_code())));
          return error.CloneAndAddErrorCode(OpIndex(op_index));
        }
      } else {
        SCHECK(op.has_read(), NotSupported, "Unknown local catalog operation");
        // FetchUniqueConstraintName may miss syscache while formatting a duplicate-key error.
        SCHECK(
            !session.transaction_failed ||
                !VERIFY_RESULT_REF(catalog_.Find(op.read().table_id().ToBuffer())).application,
            IllegalState, "Application reads require rollback");
        const auto& table = VERIFY_RESULT_REF(catalog_.Find(op.read().table_id().ToBuffer()));
        if (catalog_.transaction_ && table.application) {
          const auto status = catalog_.transaction_->NativeTransactions().ReadSql(
              *session.native_txn, op.read(), slot, &responder.sidecars(), responder.deadline(),
              state.statement_time, std::max(1u, req.options().active_sub_transaction_id()),
              state.snapshot, wait);
          if (waiting(status, op_index)) return Status::OK();
          RETURN_NOT_OK(status);
          ++catalog_.reads_;
        } else {
          RETURN_NOT_OK(catalog_.Read(
              op.read(), slot, &responder.sidecars(), responder.deadline(),
              state.statement_time));
        }
      }
      ++op_index;
    }
    return Status::OK();
  }

  // Joins the backend's SQL transaction and picks the statement limit and snapshot.
  Status StartPerform(
      Session& session, const tserver::PgPerformRequestMsg& req,
      tserver::PgPerformResponseMsg* resp, PerformState& state) {
    RETURN_NOT_OK(AcquireSqlTransaction(session, req.options()));
    SCHECK(
        !req.options().use_historical_read_session(), NotSupported,
        "Historical reads are not available on a frozen catalog");
    // Start before selecting the statement limit, including read-only SQL transactions.
    // The native table-lock path guarantees a FinishTransaction even without data writes.
    const bool catalog = req.options().ddl_mode();
    for (const auto& op : req.ops()) {
      if (op.has_write()) {
        RETURN_NOT_OK(EnsureTransaction(session, req.options(), false, catalog));
      } else if (op.has_read() && catalog_.transaction_) {
        if (VERIFY_RESULT_REF(catalog_.Find(op.read().table_id().ToBuffer())).application) {
          RETURN_NOT_OK(EnsureTransaction(session, req.options(), true, catalog));
        } else if (
            op.read().has_row_mark_type() && op.read().row_mark_type() != ROW_MARK_ABSENT) {
          // Catalog row locks are not resolved against other backends; exclude them instead.
          RETURN_NOT_OK(AcquireExclusive(session));
        }
      }
    }
    // One shared clock orders every backend. Own-write limits stay distinct from the snapshot.
    state.statement_time =
        catalog_.transaction_ ? catalog_.transaction_->Clock() : catalog_.ReadTime();
    if (catalog_.transaction_ && req.options().has_in_txn_limit_ht()) {
      const auto limit = HybridTime(req.options().in_txn_limit_ht().value());
      if (limit.is_valid() && !limit.is_special()) state.statement_time = limit;
      RETURN_NOT_OK(catalog_.transaction_->ValidateReadTime(state.statement_time));
      resp->set_used_in_txn_limit_ht(state.statement_time.ToUint64());
    }
    const auto& read_options = req.options().read_time_options();
    if (read_options.has_read_time()) {
      const auto time = ReadHybridTime::FromPB(read_options.read_time()).read;
      if (catalog_.transaction_)
        RETURN_NOT_OK(catalog_.transaction_->ValidateReadTime(time));
      else
        SCHECK(time == catalog_.ReadTime(), NotSupported, "Historical reads are not supported");
    }
    if (catalog_.transaction_) {
      // Mirrors PgClientSession: an explicit read time wins, otherwise a changed serial (zero
      // keeps the current point) selects the current clock.
      const auto serial = read_options.read_time_serial_no();
      if (read_options.has_read_time() && read_options.read_time().has_read_ht()) {
        session.read_point = ReadHybridTime::FromPB(read_options.read_time()).read;
      } else if (!session.read_point.is_valid() || (serial && serial != session.read_serial)) {
        session.read_point = catalog_.transaction_->Clock();
      }
      if (serial) session.read_serial = serial;
    }
    // The catalog transaction excludes every other writer, so its begin time is its snapshot.
    state.snapshot =
        transaction_owner_ == session.id ? HybridTime::kInvalid : session.read_point;
    if (catalog_.transaction_) {
      SCHECK(
          !req.options().read_time_options().restart_transaction() &&
              req.options().read_time_options().read_time_manipulation() != tserver::RESTART,
          NotSupported, "Catalog transaction restarts are not supported");
    }
    return Status::OK();
  }

  void OpenTable(
      const tserver::PgOpenTableRequestPB* req, tserver::PgOpenTableResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckMetadata(req->session_id());
    if (status.ok()) status = catalog_.OpenTable(req->table_id(), resp);
    Respond(status, resp, context);
  }

  void GetTablePartitionList(
      const tserver::PgGetTablePartitionListRequestPB* req,
      tserver::PgGetTablePartitionListResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    tserver::PgOpenTableResponsePB info;
    auto status = CheckMetadata(req->session_id());
    if (status.ok()) status = catalog_.OpenTable(req->table_id(), &info);
    if (status.ok()) *resp->mutable_partitions() = info.partitions();
    Respond(status, resp, context);
  }

  void GetDatabaseInfo(
      const tserver::PgGetDatabaseInfoRequestPB* req, tserver::PgGetDatabaseInfoResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    const auto status = CheckMetadata(req->session_id());
    if (!status.ok()) {
      Respond(status, resp, context);
      return;
    }
    auto it = catalog_.databases_.find(req->oid());
    if (it == catalog_.databases_.end()) {
      Respond(STATUS(NotFound, "Database is not in frozen seed"), resp, context);
      return;
    }
    auto* ns = resp->mutable_info()->mutable_namespace_();
    resp->mutable_info()->set_state(master::SysNamespaceEntryPB::RUNNING);
    ns->set_id(GetPgsqlNamespaceId(req->oid()));
    ns->set_name(it->second);
    ns->set_database_type(YQL_DATABASE_PGSQL);
    Respond(Status::OK(), resp, context);
  }

  void IsObjectPartOfXRepl(
      const tserver::PgIsObjectPartOfXReplRequestPB*,
      tserver::PgIsObjectPartOfXReplResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    resp->set_is_object_part_of_xrepl(false);
    Respond(Status::OK(), resp, context);
  }

  void IsNamespacePartOfCDCSDK(
      const tserver::PgIsNamespacePartOfCDCSDKRequestPB*,
      tserver::PgIsNamespacePartOfCDCSDKResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    resp->set_is_namespace_part_of_cdcsdk(false);
    Respond(Status::OK(), resp, context);
  }

  void GetTserverCatalogVersionInfo(
      const tserver::PgGetTserverCatalogVersionInfoRequestPB* req,
      tserver::PgGetTserverCatalogVersionInfoResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    for (const auto& [oid, index] : catalog_.catalog_slots_) {
      if (catalog_.versions_.contains(oid) && !req->size_only() &&
          (req->db_oid() == 0 || req->db_oid() == oid)) {
        auto* entry = resp->add_entries();
        entry->set_db_oid(oid);
        entry->set_shm_index(static_cast<int32_t>(index));
      }
    }
    resp->set_num_entries(static_cast<uint32_t>(catalog_.versions_.size()));
    Respond(Status::OK(), resp, context);
  }

  void SetTserverCatalogMessageList(
      const tserver::PgSetTserverCatalogMessageListRequestPB* req,
      tserver::PgSetTserverCatalogMessageListResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    Status status;
    if (!catalog_.transaction_) {
      status = STATUS(NotSupported, "Frozen catalog cannot publish invalidations");
    } else if (
        !catalog_.versions_.contains(req->db_oid()) ||
        req->new_catalog_version() > catalog_.versions_.at(req->db_oid())) {
      status = STATUS(IllegalState, "Catalog invalidation precedes durable catalog version");
    }
    Respond(status, resp, context);
  }

  void GetTserverCatalogMessageLists(
      const tserver::PgGetTserverCatalogMessageListsRequestPB*,
      tserver::PgGetTserverCatalogMessageListsResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    // An empty queue tells native YSQL to do a full cache refresh, not to skip invalidation.
    Respond(
        catalog_.transaction_ ? Status::OK()
                              : STATUS(NotSupported, "Frozen catalog has no invalidation queue"),
        resp, context);
  }

  void IsDatabaseColocated(
      const tserver::PgIsDatabaseColocatedRequestPB* req,
      tserver::PgIsDatabaseColocatedResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    Respond(
        catalog_.versions_.contains(req->database_oid())
            ? Status::OK()
            : STATUS(NotFound, "Database is not in frozen seed"),
        resp, context);
  }

  void IsInitDbDone(
      const tserver::PgIsInitDbDoneRequestPB*, tserver::PgIsInitDbDoneResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    resp->set_done(true);
    Respond(Status::OK(), resp, context);
  }

  void GetCatalogMasterVersion(
      const tserver::PgGetCatalogMasterVersionRequestPB*,
      tserver::PgGetCatalogMasterVersionResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    resp->set_version(catalog_.max_version_);
    Respond(Status::OK(), resp, context);
  }

  void TabletServerCount(
      const tserver::PgTabletServerCountRequestPB*, tserver::PgTabletServerCountResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    resp->set_count(1);
    Respond(Status::OK(), resp, context);
  }

  void AcquireObjectLock(
      const tserver::LWPgAcquireObjectLockRequestPB* req,
      tserver::LWPgAcquireObjectLockResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    Respond(ObjectLockStatus(*req), resp, context);
  }

  Status ObjectLockStatus(const tserver::LWPgAcquireObjectLockRequestPB& req) {
    RETURN_NOT_OK(CheckSession(req.session_id()));
    if (req.lock_type() < tserver::ACCESS_SHARE_LOCK ||
        req.lock_type() > (catalog_.transaction_ ? tserver::ACCESS_EXCLUSIVE_LOCK
                                                 : tserver::ROW_EXCLUSIVE_LOCK)) {
      return STATUS_FORMAT(
          NotSupported, "Unsupported frozen catalog lock mode $0", req.lock_type());
    }
    SCHECK(!req.is_session_lock(), NotSupported, "Session object locks are not supported");
    auto& session = *sessions_.at(req.session_id());
    RETURN_NOT_OK(AcquireSqlTransaction(session, req.options()));
    // Data transactions only take weak locks, which never conflict with each other. Stronger
    // modes stand in for catalog-level exclusion until the transaction finishes.
    if (catalog_.transaction_ && req.lock_type() > tserver::ROW_EXCLUSIVE_LOCK) {
      return AcquireExclusive(session);
    }
    return Status::OK();
  }

  void RollbackToSubTransaction(
      const tserver::PgRollbackToSubTransactionRequestPB* req,
      tserver::PgRollbackToSubTransactionResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckSession(req->session_id());
    std::optional<TransactionId> rolled_back;
    if (status.ok()) {
      auto& session = *sessions_.at(req->session_id());
      CancelWaiter(session, OvertakenWait());
      status = CheckNoPending(session);
    }
    if (status.ok()) {
      auto& session = *sessions_.at(req->session_id());
      rolled_back = session.native_txn;
      if (catalog_.transaction_ && transaction_owner_ != session.id && session.native_txn) {
        if (req->sub_transaction_id() == 0) {
          status = STATUS(InvalidArgument, "Invalid subtransaction");
        } else if (req->options().txn_serial_no() != session.txn_serial) {
          status = STATUS(NotSupported, "Cannot roll back a different local transaction");
        } else {
          status = catalog_.transaction_->NativeTransactions().RollbackTo(
              *session.native_txn, req->sub_transaction_id());
          if (status.ok() && session.failed_subtxn >= req->sub_transaction_id())
            session.transaction_failed = false;
        }
      } else if (!catalog_.transaction_ || transaction_owner_ != session.id) {
        // This backend has no mutable participant. A rejected contender must never roll
        // back the current owner's catalog or release its exclusive lease.
        if (session.failed_subtxn >= req->sub_transaction_id()) session.transaction_failed = false;
        if (req->sub_transaction_id() == 0) {
          status = STATUS(InvalidArgument, "Invalid subtransaction");
        }
      } else if (
          catalog_.transaction_->active() && req->options().txn_serial_no() != session.txn_serial) {
        status = STATUS(NotSupported, "Cannot roll back a different catalog transaction");
      } else {
        status = catalog_.transaction_->RollbackTo(req->sub_transaction_id());
        if (status.ok()) status = catalog_.ReloadTables();
        if (status.ok() && session.failed_subtxn >= req->sub_transaction_id())
          session.transaction_failed = false;
      }
    }
    Respond(status, resp, context);
    // The savepoint's locks are released, which may unblock a waiter.
    if (status.ok() && rolled_back) Wake(*rolled_back);
  }

  void FinishTransaction(
      const tserver::PgFinishTransactionRequestPB* req,
      tserver::PgFinishTransactionResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckSession(req->session_id());
    std::optional<TransactionId> finished;
    if (status.ok()) {
      auto& session = *sessions_.at(req->session_id());
      finished = session.native_txn;
      CancelWaiter(session, OvertakenWait());
      status = CheckNoPending(session);
    }
    if (status.ok() && catalog_.transaction_ && transaction_owner_ == req->session_id()) {
      auto& session = *sessions_.at(req->session_id());
      auto& txn = *catalog_.transaction_;
      if (!req->commit()) {
        txn.Abort();
        status = catalog_.ReloadTables();
        session.transaction_failed = false;
      } else if (session.transaction_failed) {
        status = STATUS(IllegalState, "Catalog transaction was aborted");
        txn.Abort();
        catalog_.failure_ = catalog_.ReloadTables();
        session.transaction_failed = false;
      } else if (txn.active()) {
        const bool autonomous =
            req->has_ddl_mode() && !req->ddl_mode().use_regular_transaction_block();
        if (autonomous != session.autonomous) {
          session.transaction_failed = true;
          status = STATUS(NotSupported, "Unsupported catalog transaction finish mode");
        } else {
          status = txn.Commit();
          if (!status.ok()) {
            txn.Abort();
            catalog_.failure_ = catalog_.ReloadTables();
            session.transaction_failed = false;
          }
          if (status.ok()) {
            // CREATE already refreshed private descriptors. Commit only changes visibility;
            // rollback/startup still rebuild and validate the complete descriptor graph.
            catalog_.failure_ = catalog_.LoadDatabaseNames();
            if (catalog_.failure_.ok()) catalog_.failure_ = catalog_.LoadCatalogVersions();
            status = catalog_.failure_;
            if (status.ok()) publish_versions_();
          }
        }
      }
      if (!txn.active()) transaction_owner_ = 0;
    } else if (
        status.ok() && catalog_.transaction_ && sessions_.at(req->session_id())->native_txn) {
      auto& session = *sessions_.at(req->session_id());
      auto& native = catalog_.transaction_->NativeTransactions();
      const auto id = *session.native_txn;
      if (req->commit() && !session.transaction_failed) {
        status = native.Commit(id);
        // A pending transaction left behind would block every later catalog change.
        if (!status.ok()) {
          const auto abort = native.Abort(id);
          if (!abort.ok() && !abort.IsNotFound() && !abort.IsIllegalState())
            WARN_NOT_OK(abort, "Aborting uncommitted local transaction");
        }
      } else {
        status = native.Abort(id);
        // Conflict arbitration may already have aborted it; finished write-less ones are gone.
        if (status.IsNotFound()) status = Status::OK();
        if (status.ok() && req->commit()) status = STATUS(IllegalState, "Transaction was aborted");
      }
    }
    if (sessions_.contains(req->session_id()) && transaction_owner_ != req->session_id()) {
      if (exclusive_owner_ == req->session_id()) exclusive_owner_ = 0;
      sessions_.at(req->session_id())->ResetTransaction();
    }
    Respond(status, resp, context);
    // Waiters resume whichever way their blocker finished.
    if (finished) Wake(*finished);
  }

  void ReleaseSessionObjectLock(
      const tserver::PgReleaseSessionObjectLockRequestPB* req,
      tserver::PgReleaseSessionObjectLockResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    Respond(CheckSession(req->session_id()), resp, context);
  }

  // Collects a shared memory response too large for any segment.
  void FetchData(
      const tserver::PgFetchDataRequestPB* req, tserver::PgFetchDataResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckSession(req->session_id());
    if (status.ok()) {
      const auto& transport = sessions_.at(req->session_id())->transport;
      status = transport ? transport->Fetch(req->data_id(), &context.sidecars().Start())
                         : STATUS(NotFound, "Local session has no shared memory");
    }
    Respond(status, resp, context);
  }

  // Mirrors the tablet server: one requested transaction regardless of age, otherwise the oldest
  // max_num_txns transactions at least min_txn_age_ms old. Like it, ignores the relation.
  void GetLockStatus(
      const tserver::PgGetLockStatusRequestPB* req, tserver::PgGetLockStatusResponsePB* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    Respond(LockStatus(*req, resp), resp, context);
  }

  Status LockStatus(
      const tserver::PgGetLockStatusRequestPB& req, tserver::PgGetLockStatusResponsePB* resp) {
    RETURN_NOT_OK(Status(catalog_.failure_));
    SCHECK(catalog_.transaction_, NotSupported, "A read-only local runtime has no locks");
    auto& store = catalog_.transaction_->NativeTransactions();
    std::vector<TransactionId> transactions;
    if (!req.transaction_id().empty()) {
      transactions.push_back(VERIFY_RESULT(FullyDecodeTransactionId(req.transaction_id())));
    } else {
      SCHECK_GT(req.max_num_txns(), 0U, InvalidArgument, "Request must contain max_num_txns > 0");
      auto pending = store.PendingTransactions();
      const int64_t newest = GetCurrentTimeMicros() - int64_t{req.min_txn_age_ms()} * 1000;
      std::erase_if(pending, [newest](const auto& txn) { return txn.second > newest; });
      std::ranges::sort(pending, [](const auto& lhs, const auto& rhs) {
        return std::tie(lhs.second, lhs.first) < std::tie(rhs.second, rhs.first);
      });
      if (pending.size() > req.max_num_txns()) pending.resize(req.max_num_txns());
      auto& node = (*resp->mutable_transactions_by_node())[instance_id_];
      for (const auto& [id, start] : pending) {
        transactions.push_back(id);
        node.add_transaction_ids(id.data(), id.size());
      }
    }
    std::vector<WaitingLocks> waiting;
    for (const auto& id : transactions) {
      if (const auto it = waiting_txns_.find(id); it != waiting_txns_.end()) {
        const auto& wait = *sessions_.at(it->second)->waiter;
        waiting.push_back({.id = id, .wait_start = wait.wait_start, .wait = &wait.state.wait});
      }
    }
    auto* node = resp->add_node_locks();
    node->set_permanent_uuid(instance_id_);
    for (auto& tablet : VERIFY_RESULT(
             store.LockStatus(transactions, waiting, req.max_txn_locks_per_tablet()))) {
      node->add_tablet_lock_infos()->Swap(&tablet);
    }
    return Status::OK();
  }

  // Each backend's current transaction, which pg_stat_activity shows as yb_backend_xid.
  void GetActiveTransactionList(
      const tserver::PgGetActiveTransactionListRequestPB* req,
      tserver::PgGetActiveTransactionListResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    for (const auto& [id, session] : sessions_) {
      if ((req->has_session_id() && req->session_id().value() != id) || !session->native_txn)
        continue;
      auto& entry = *resp->add_entries();
      entry.set_session_id(id);
      entry.set_txn_id(session->native_txn->data(), session->native_txn->size());
    }
    Respond(Status::OK(), resp, context);
  }

  SharedMemoryUse shared_memory_use() const { return shared_use_.Snapshot(); }

 private:
  FrozenCatalog& catalog_;
  rpc::Scheduler& scheduler_;
  size_t pending_calls_ = 0;
  size_t pending_bytes_ = 0;
  bool shutting_down_ = false;
  std::recursive_mutex mutex_;
  std::function<void()> publish_versions_;
  // One overlay still owns all mutable descriptors. Never infer ownership from a session ID
  // being valid: per-session protocol state is not concurrent catalog isolation.
  uint64_t transaction_owner_ = 0;
  // Held from the first catalog change or strong object lock until the transaction finishes.
  // Other backends' SQL requests fail while it is held, and it requires them to be idle.
  uint64_t exclusive_owner_ = 0;
  uint64_t next_session_ = 0;
  std::unordered_map<uint64_t, std::shared_ptr<Session>> sessions_;
  // Names this runtime's shared memory objects; PgGate opens them from the Heartbeat response.
  const std::string instance_id_;
  SharedMemoryCounters shared_use_;
  std::unique_ptr<tserver::PgSharedMemoryPool> segments_;
  std::unique_ptr<YBThreadPool> exchange_threads_;
  bool exchange_threads_stopped_ = false;
  // Transports of retired sessions whose polling threads have not been joined yet.
  std::vector<std::shared_ptr<SharedTransport>> retired_;
  // Transactions with a waiting Perform, by session.
  std::unordered_map<TransactionId, uint64_t, TransactionIdHash> waiting_txns_;
  std::deque<uint64_t> wakes_;
  bool wake_scheduled_ = false;
  uint64_t wait_timers_ = 0;
};

}  // namespace

class ReadonlyPgServer::Impl : public pgwrapper::PgWrapperContext {
 public:
  ~Impl() {
    if (service_) {
      service_->Shutdown();
      const auto use = service_->shared_memory_use();
      LOG(INFO) << "Local shared memory requests: " << use.requests
                << ", segment responses: " << use.segment_responses
                << ", fetched responses: " << use.saved_responses;
    }
    if (postgres_ && postgres_->ProcessId()) {
      postgres_->Shutdown();
      WARN_NOT_OK(ResultToStatus(postgres_->Wait()), "Stopping local YSQL");
    }
    if (messenger_) {
      messenger_->UnregisterAllServices();
      messenger_->Shutdown();
    }
    WARN_NOT_OK(shared_memory_.ShutdownNegotiator(), "Stopping address negotiator");
    if (FLAGS_local_runtime_shared_memory && !uuid_.empty()) {
      WARN_NOT_OK(
          tserver::PgSessionSharedMemoryManager::Cleanup(uuid_),
          "Removing local session shared memory");
    }
  }

  Status Start(
      const std::string& directory, const std::string& snapshot, uint16_t sql_port,
      CatalogMode mode) {
    SCHECK(
        process_guard_.owns_lock(), IllegalState,
        "Only one local runtime per process is supported");
    SCHECK(sql_port != 0, InvalidArgument, "SQL port must be explicit");
    const auto marker = mode == CatalogMode::kFrozen ? kOwnerMarker : "yb-local-catalog-txn-v10\n";
    const auto identity = std::string(marker) + VersionInfo::GetShortVersionString() + "\n" +
                          VERIFY_RESULT(SnapshotFingerprint(snapshot)) + "\n";
    RETURN_NOT_OK(directory_lock_.Acquire(directory, identity));
    RETURN_NOT_OK(catalog_.Open(snapshot, directory, mode));
    sql_port_ = sql_port;
    uuid_ = Uuid::Generate().ToHexString();
    RETURN_NOT_OK(shared_memory_.InitializeTServer(uuid_));
    entity_ = METRIC_ENTITY_server.Instantiate(&metrics_, "local-readonly");
    rpc::MessengerBuilder builder("local-readonly");
    builder.set_num_reactors(1).set_metric_entity(entity_);
    builder.UseDefaultConnectionContextFactory();
    messenger_ = VERIFY_RESULT(builder.Build());
    FLAGS_rpc_server_allow_ephemeral_ports = true;
    server::RpcServerOptions options;
    options.rpc_bind_addresses = "127.0.0.1:0";
    rpc_server_ = std::make_unique<server::RpcServer>(
        "local-readonly", options,
        rpc::CreateConnectionContextFactory<rpc::YBInboundConnectionContext>());
    RETURN_NOT_OK(rpc_server_->Init(messenger_.get()));
    service_ = std::make_shared<ReadonlyService>(
        entity_, catalog_, messenger_->scheduler(), [this] { PublishVersions(); }, uuid_);
    if (FLAGS_local_runtime_inline_rpc) {
      RETURN_NOT_OK(messenger_->RegisterService(
          service_->service_name(), make_scoped_refptr<InlineService>(service_, entity_)));
    } else {
      RETURN_NOT_OK(rpc_server_->RegisterService(64, service_));
    }
    RETURN_NOT_OK(rpc_server_->Start());
    {
      auto data = shared_memory_.SharedData();
      data->SetHostEndpoint(rpc_server_->GetBoundAddresses().front(), "127.0.0.1");
      data->SetTserverUuid(uuid_);
      data->SetPid(getpid());
      data->SetPostgresAuthKey(0);
      data->SetYsqlCatalogVersion(catalog_.max_version_);
      for (const auto& [oid, index] : catalog_.catalog_slots_) {
        data->SetYsqlDbCatalogVersion(index, catalog_.versions_.at(oid));
      }
    }
    // Let the connecting backend build its own relcache, without a tserver-spawned helper.
    FLAGS_ysql_enable_relcache_init_optimization = false;
    FLAGS_pgsql_proxy_webserver_port = 0;
    // PgSupervisor normally disables these when no connection manager owns a stats segment.
    FLAGS_enable_ysql_conn_mgr = false;
    FLAGS_enable_ysql_conn_mgr_stats = false;
    FLAGS_ysql_pg_conf_csv = std::string("default_transaction_read_only=") +
                             (mode == CatalogMode::kFrozen ? "on" : "off") +
                             ",autovacuum=off,yb_enable_ash=off,"
                             "yb_enable_pg_stat_statements_docdb_metrics=off";
    if (mode != CatalogMode::kFrozen) {
      // There is no master to promote the prerequisites normally enabled in a new universe.
      FLAGS_ysql_yb_enable_add_column_missing_default = true;
      FLAGS_ysql_yb_enable_alter_table_rewrite = true;
      FLAGS_ysql_yb_enable_ddl_atomicity_infra = true;
      FLAGS_ysql_yb_enable_ddl_savepoint_infra = true;
      FLAGS_ysql_yb_enable_pg_locks = true;
      // The catalog declares yb_lock_status with the advisory-lock columns; without them the
      // function returns rows PostgreSQL rejects.
      FLAGS_ysql_yb_pg_locks_integrate_advisory_locks = true;
      FLAGS_ysql_enable_object_locking_infra = true;
      FLAGS_enable_object_lock_fastpath = false;
      FLAGS_enable_object_locking_for_table_locks = true;
      // Foreground index build holds the exclusive catalog lease.
      FLAGS_ysql_disable_index_backfill = true;
      // Match a newly initialized native universe, without relying on test-only AutoFlag promotion.
      // Unpacked template catalogs exceed the bounded atomic database-copy batch.
      FLAGS_ysql_enable_packed_row = true;
      FLAGS_ysql_enable_packed_row_for_colocated_table = true;
      FLAGS_ysql_use_packed_row_v2 = true;
      FLAGS_ysql_pg_conf_csv +=
          ",max_parallel_workers=0,max_parallel_workers_per_gather=0,"
          "yb_ddl_transaction_block_enabled=on,yb_non_ddl_txn_for_sys_tables_allowed=on,"
          "yb_enable_new_relation_fastpath_write=off";
    }
    auto conf = VERIFY_RESULT(
        pgwrapper::PgProcessConf::CreateValidateAndRunInitDb(
            Format("127.0.0.1:$0", sql_port), directory + "/pg_data"));
    conf.force_disable_log_file = true;
    conf.cleanup_previous_postgres = false;
    pg_directory_ = conf.data_dir;
    postgres_ = std::make_unique<pgwrapper::PgWrapper>(std::move(conf));
    postgres_->PrepareSharedMemoryNegotiation(this);
    RETURN_NOT_OK(postgres_->Start());
    return Status::OK();
  }

  void PublishVersions() {
    auto data = shared_memory_.SharedData();
    data->SetYsqlCatalogVersion(catalog_.max_version_);
    for (const auto& [oid, index] : catalog_.catalog_slots_) {
      const auto version = catalog_.versions_.find(oid);
      data->SetYsqlDbCatalogVersion(
          index, version == catalog_.versions_.end() ? 0 : version->second);
    }
  }

  void RegisterCertificateReloader(tserver::CertificateReloader) override {}
  void RegisterPgProcessRestarter(std::function<Status(void)>) override {}
  void RegisterPgProcessKiller(std::function<Status(void)>) override {}
  void RegisterPgConfigGenerator(pgwrapper::PgConfigGenerator) override {}
  Status StartSharedMemoryNegotiation() override {
    return shared_memory_.PrepareNegotiationTServer();
  }
  Status StopSharedMemoryNegotiation() override { return shared_memory_.ShutdownNegotiator(); }
  int SharedMemoryNegotiationFd() override { return shared_memory_.NegotiationFd(); }
  const std::string& permanent_uuid() const override { return uuid_; }

  std::unique_lock<std::mutex> process_guard_{runtime_mutex, std::try_to_lock};
  RuntimeDirectory directory_lock_;
  std::string pg_directory_;
  FrozenCatalog catalog_;
  uint16_t sql_port_ = 0;
  std::string uuid_;
  MetricRegistry metrics_;
  scoped_refptr<MetricEntity> entity_;
  tserver::SharedMemoryManager shared_memory_;
  std::shared_ptr<ReadonlyService> service_;
  std::unique_ptr<server::RpcServer> rpc_server_;
  std::unique_ptr<rpc::Messenger> messenger_;
  std::unique_ptr<pgwrapper::PgWrapper> postgres_;
};

ReadonlyPgServer::ReadonlyPgServer(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
ReadonlyPgServer::~ReadonlyPgServer() = default;

Result<std::string> ReadonlyPgServer::CatalogFingerprint(const std::string& snapshot) {
  return SnapshotFingerprint(snapshot);
}

Result<std::unique_ptr<ReadonlyPgServer>> ReadonlyPgServer::Start(
    const std::string& directory, const std::string& catalog_snapshot, uint16_t sql_port,
    CatalogMode mode) {
  auto impl = std::make_unique<Impl>();
  RETURN_NOT_OK(impl->Start(directory, catalog_snapshot, sql_port, mode));
  return std::unique_ptr<ReadonlyPgServer>(new ReadonlyPgServer(std::move(impl)));
}

size_t ReadonlyPgServer::CatalogTableCount() const { return impl_->catalog_.tables_.size(); }
uint64_t ReadonlyPgServer::ReadCount() const { return impl_->catalog_.reads_.load(); }
SharedMemoryUse ReadonlyPgServer::SharedMemory() const {
  return impl_->service_->shared_memory_use();
}
HostPort ReadonlyPgServer::RpcAddress() const {
  return HostPort(impl_->rpc_server_->GetBoundAddresses().front());
}
uint16_t ReadonlyPgServer::SqlPort() const { return impl_->sql_port_; }
const std::string& ReadonlyPgServer::PgDataDirectory() const { return impl_->pg_directory_; }

Status ReadonlyPgServer::ValidateConnection(const std::string& reported_directory) const {
  std::error_code error;
  SCHECK(
      std::filesystem::equivalent(reported_directory, impl_->pg_directory_, error) && !error,
      IllegalState, "SQL connection reached a different PostgreSQL data directory");
  std::ifstream pid_file(impl_->pg_directory_ + "/postmaster.pid");
  int64_t pid = 0;
  SCHECK(
      (pid_file >> pid) && impl_->postgres_->ProcessId() == pid, IllegalState,
      "SQL connection does not belong to this runtime postmaster");
  return Status::OK();
}

}  // namespace yb::local_runtime
