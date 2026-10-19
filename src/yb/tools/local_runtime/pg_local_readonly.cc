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
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "yb/common/pg_types.h"
#include "yb/common/pgsql_error.h"
#include "yb/common/transaction_error.h"
#include "yb/common/pgsql_protocol.messages.h"
#include "yb/common/schema_pbutil.h"
#include "yb/common/wire_protocol.h"
#include "yb/common/version_info.h"
#include "yb/gutil/endian.h"
#include "yb/docdb/doc_read_context.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/docdb/pgsql_operation.h"
#include "yb/docdb/ql_rocksdb_storage.h"
#include "yb/rpc/messenger.h"
#include "yb/rpc/scheduler.h"
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
#include "yb/tserver/tserver_admin.pb.h"
#include "yb/tserver/tserver_shared_mem.h"
#include "yb/util/env.h"
#include "yb/util/memory/arena.h"
#include "yb/util/metrics.h"
#include "yb/util/pb_util.h"
#include "yb/util/scope_exit.h"
#include "yb/util/sync_point.h"
#include "yb/util/uuid.h"
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
DECLARE_bool(ysql_use_packed_row_v2);
DECLARE_bool(ysql_yb_enable_add_column_missing_default);
DECLARE_bool(ysql_yb_enable_alter_table_rewrite);
DECLARE_bool(ysql_yb_enable_ddl_atomicity_infra);
DECLARE_bool(ysql_yb_enable_ddl_savepoint_infra);

namespace yb::local_runtime {
namespace {

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
    // Mutable catalog row locks are uncontended: no other SQL backend can be admitted.
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

class ReadonlyService : public UnsupportedPgClientService,
                        public std::enable_shared_from_this<ReadonlyService> {
 private:
  // RpcContext owns the inbound call backing these pointers; drain before messenger shutdown.
  struct PendingPerform {
    const tserver::PgPerformRequestMsg* request;
    tserver::PgPerformResponseMsg* response;
    rpc::RpcContext context;
    CoarseTimePoint deadline;
    size_t bytes;
  };
  struct Session {
    Session(uint64_t id_, pid_t pid_) : id(id_), pid(pid_) {}

    void ResetTransaction() {
      txn_serial = 0;
      sql_serial = 0;
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
    std::optional<TransactionId> native_txn;
    bool autonomous = false;
    bool transaction_failed = false;
    uint32_t failed_subtxn = 0;
  };

 public:
  ReadonlyService(
      const scoped_refptr<MetricEntity>& entity, FrozenCatalog& catalog, rpc::Scheduler& scheduler,
      std::function<void()> publish_versions)
      : UnsupportedPgClientService(entity),
        catalog_(catalog),
        scheduler_(scheduler),
        publish_versions_(std::move(publish_versions)) {}

  template <class Response>
  void Respond(const Status& status, Response* response, rpc::RpcContext& context) {
    const auto& result = status.ok() ? catalog_.failure_ : status;
    if (!result.ok()) StatusToPB(result, response->mutable_status());
    context.RespondSuccess();
  }

  Status AbortOwnedTransaction(Session& session) {
    if (sql_owner_ == session.id) sql_owner_ = 0;
    if (transaction_owner_ != session.id) return Status::OK();
    CHECK(catalog_.transaction_);
    catalog_.transaction_->Abort();
    transaction_owner_ = 0;
    catalog_.failure_ = catalog_.ReloadTables();
    return catalog_.failure_;
  }

  void RetireSession(Session& session) {
    WARN_NOT_OK(AbortOwnedTransaction(session), "Aborting exited local backend");
    CancelPending(session, STATUS(Aborted, "Local backend has exited"));
    sessions_.erase(session.id);
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
    sessions_.emplace(id, std::make_shared<Session>(id, static_cast<pid_t>(req->pid())));
    resp->set_session_id(id);
    // Empty instance_id selects the existing network transport in unmodified PgGate.
    Respond(Status::OK(), resp, context);
  }

  Status CheckSession(uint64_t id) {
    std::lock_guard lock(mutex_);
    SCHECK(!shutting_down_, ServiceUnavailable, "Local runtime is shutting down");
    const auto it = sessions_.find(id);
    SCHECK(it != sessions_.end(), InvalidArgument, "Unknown local session");
    auto session = it->second;
    if (kill(session->pid, 0) != 0 && errno == ESRCH) {
      RetireSession(*session);
      return STATUS(InvalidArgument, "Local backend has exited");
    }
    if (!catalog_.failure_.ok()) CancelPending(*session, catalog_.failure_);
    return catalog_.failure_;
  }

  Status CheckSqlOwner(uint64_t id) {
    TEST_SYNC_POINT_CALLBACK("LocalRuntime::BeforeSqlOwnerCheck", &id);
    if (sql_owner_ && sessions_.contains(sql_owner_)) {
      const auto owner = sessions_.at(sql_owner_);
      if (kill(owner->pid, 0) != 0 && errno == ESRCH) {
        // The requesting handler may retain only a Session&. Retire it on a later request.
        if (owner->id == id) return STATUS(Aborted, "Local backend has exited");
        RetireSession(*owner);
      }
    }
    if (sql_owner_ && sql_owner_ != id) {
      return STATUS(
          TryAgain, "Overlapping SQL transactions are not supported by the local runtime",
          TransactionError(TransactionErrorCode::kConflict));
    }
    return Status(catalog_.failure_);
  }

  Status AcquireSqlTransaction(Session& session, const tserver::LWPgPerformOptionsPB& options) {
    if (!catalog_.transaction_) return Status::OK();
    RETURN_NOT_OK(CheckSqlOwner(session.id));
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
        !sql_owner_ || session.sql_serial == options.txn_serial_no(), NotSupported,
        "SQL transaction end notification missing; reconnect required");
    sql_owner_ = session.id;
    session.sql_serial = options.txn_serial_no();
    return Status::OK();
  }

  Status CheckMetadata(uint64_t id) {
    if (id) RETURN_NOT_OK(CheckSession(id));
    RETURN_NOT_OK(Status(catalog_.failure_));
    // Legacy callers may see committed descriptors, never another backend's private DDL.
    return CheckSqlOwner(id);
  }

  std::optional<uint64_t> Predecessor(const tserver::PgPerformRequestMsg& req) const {
    return req.sequence_num().has_predecessor_serial_no()
               ? std::make_optional(req.sequence_num().predecessor_serial_no().value())
               : std::nullopt;
  }

  void CancelPending(Session& session, const Status& status) {
    for (auto& [serial, call] : session.pending) Respond(status, call.response, call.context);
    pending_calls_ -= session.pending.size();
    pending_bytes_ -= session.pending_bytes;
    session.pending.clear();
    session.pending_bytes = 0;
  }

  void FailSequence(Session& session, const Status& status) {
    session.sequence_broken = true;
    session.transaction_failed = true;
    CancelPending(session, status);
    WARN_NOT_OK(AbortOwnedTransaction(session), "Aborting failed local request sequence");
  }

  void Shutdown() override {
    std::lock_guard lock(mutex_);
    shutting_down_ = true;
    for (auto& [id, session] : sessions_) {
      CancelPending(*session, STATUS(ServiceUnavailable, "Local runtime is shutting down"));
    }
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
      tserver::PgPerformResponseMsg* resp, rpc::RpcContext context) {
    auto status = session.sequence.Register(req->sequence_num().serial_no(), Predecessor(*req));
    if (status.ok()) status = PerformReads(session, *req, resp, context);
    if (!status.ok()) {
      if (catalog_.transaction_) {
        session.transaction_failed = true;
        session.failed_subtxn = std::max(1u, req->options().active_sub_transaction_id());
      }
      resp->Clear();
      context.sidecars().Reset();
    }
    Respond(status, resp, context);
  }

  void DrainPending(Session& session) {
    while (!session.pending.empty()) {
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
        Respond(status, call.response, call.context);
        FailSequence(session, status);
        return;
      }
      ExecutePerform(session, call.request, call.response, std::move(call.context));
    }
  }

  void Perform(
      const tserver::PgPerformRequestMsg* req, tserver::PgPerformResponseMsg* resp,
      rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    const auto status = CheckSession(req->session_id());
    if (!status.ok()) {
      Respond(status, resp, context);
      return;
    }
    auto session = sessions_.at(req->session_id());
    if (session->sequence_broken) {
      Respond(
          STATUS(IllegalState, "Local request sequence failed; reconnect required"), resp, context);
      return;
    }
    const auto serial = req->sequence_num().serial_no();
    auto ready = session->sequence.Ready(serial, Predecessor(*req));
    if (!ready.ok() || session->pending.contains(serial)) {
      const auto error =
          !ready.ok() ? ready.status() : STATUS(IllegalState, "Duplicate pending local request");
      Respond(error, resp, context);
      FailSequence(*session, error);
      return;
    }
    if (!*ready) {
      const auto bytes = req->SerializedSize();
      // Bounds apply to the whole service, not once per admitted backend.
      if (pending_calls_ >= 64 || bytes > 16ULL * 1024 * 1024 - pending_bytes_) {
        const auto error = STATUS(ServiceUnavailable, "Local request queue limit exceeded");
        Respond(error, resp, context);
        FailSequence(*session, error);
        return;
      }
      const auto deadline =
          std::min(context.GetClientDeadline(), CoarseMonoClock::Now() + std::chrono::seconds(5));
      session->pending.emplace(
          serial, PendingPerform{req, resp, std::move(context), deadline, bytes});
      session->pending_bytes += bytes;
      pending_bytes_ += bytes;
      ++pending_calls_;
      TEST_SYNC_POINT("LocalRuntime::PerformQueued");
      SchedulePendingCheck(*session);
      return;
    }
    ExecutePerform(*session, req, resp, std::move(context));
    DrainPending(*session);
  }

  Status EnsureTransaction(
      Session& session, const tserver::LWPgPerformOptionsPB& options, bool read_only = false) {
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
    SCHECK(
        transaction_owner_ == 0 || transaction_owner_ == session.id, NotSupported,
        "Another local backend owns the catalog transaction");
    if (!catalog_.transaction_->active()) {
      session.native_txn = VERIFY_RESULT(catalog_.transaction_->BeginWithNative());
      transaction_owner_ = session.id;
      session.txn_serial = options.txn_serial_no();
      session.autonomous = false;
    }
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
        catalog_.transaction_ && transaction_owner_ == session.id && sql_owner_ == session.id &&
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

  void DeleteDBSequences(
      const tserver::LWPgDeleteDBSequencesRequestPB* req,
      tserver::LWPgDeleteDBSequencesResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    // Sequence creation is rejected, so no sequence state can exist for this deferred cleanup.
    Respond(
        catalog_.transaction_ ? CheckSession(req->session_id())
                              : STATUS(NotSupported, "Frozen catalog rejects sequence cleanup"),
        resp, context);
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

  Status PerformReads(
      Session& session, const tserver::PgPerformRequestMsg& req,
      tserver::PgPerformResponseMsg* resp, rpc::RpcContext& context) {
    RETURN_NOT_OK(Status(catalog_.failure_));
    RETURN_NOT_OK(AcquireSqlTransaction(session, req.options()));
    SCHECK(
        !req.options().use_historical_read_session(), NotSupported,
        "Historical reads are not available on a frozen catalog");
    // Start before selecting the statement limit, including read-only SQL transactions.
    // The native table-lock path guarantees a FinishTransaction even without data writes.
    for (const auto& op : req.ops()) {
      if (op.has_write()) {
        RETURN_NOT_OK(EnsureTransaction(session, req.options()));
      } else if (
          op.has_read() && catalog_.transaction_ &&
          VERIFY_RESULT_REF(catalog_.Find(op.read().table_id().ToBuffer())).application) {
        RETURN_NOT_OK(EnsureTransaction(session, req.options(), true));
      }
    }
    auto statement_time = catalog_.ReadTime();
    if (catalog_.transaction_ && req.options().has_in_txn_limit_ht()) {
      const auto limit = HybridTime(req.options().in_txn_limit_ht().value());
      if (limit.is_valid() && !limit.is_special()) statement_time = limit;
      RETURN_NOT_OK(catalog_.transaction_->ValidateReadTime(statement_time));
      resp->set_used_in_txn_limit_ht(statement_time.ToUint64());
    }
    // The exclusive SQL lease fixes committed state through FinishTransaction, including
    // read-only SQL. Own-write statement limits remain distinct from the native snapshot.
    if (req.options().read_time_options().has_read_time()) {
      const auto time = ReadHybridTime::FromPB(req.options().read_time_options().read_time()).read;
      if (catalog_.transaction_)
        RETURN_NOT_OK(catalog_.transaction_->ValidateReadTime(time));
      else
        SCHECK(time == catalog_.ReadTime(), NotSupported, "Historical reads are not supported");
    }
    if (catalog_.transaction_) {
      SCHECK(
          !req.options().read_time_options().restart_transaction() &&
              req.options().read_time_options().read_time_manipulation() != tserver::RESTART,
          NotSupported, "Catalog transaction restarts are not supported");
    }
    size_t op_index = 0;
    for (const auto& op : req.ops()) {
      if (op.has_write()) {
        RETURN_NOT_OK(EnsureTransaction(session, req.options()));
        const auto& table = VERIFY_RESULT_REF(catalog_.Find(op.write().table_id().ToBuffer()));
        SCHECK(
            table.application || req.options().ddl_mode(), NotSupported,
            "Catalog mutations must be part of DDL");
        if (table.application) {
          RETURN_NOT_OK(catalog_.transaction_->NativeTransactions().WriteSql(
              *session.native_txn, op.write(), resp->add_responses(), &context.sidecars(),
              req.options().active_sub_transaction_id()));
        } else {
          RETURN_NOT_OK(catalog_.Write(
              op.write(), resp->add_responses(), &context.sidecars(),
              req.options().active_sub_transaction_id()));
        }
        const auto& response = resp->responses().back();
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
          RETURN_NOT_OK(catalog_.transaction_->NativeTransactions().ReadSql(
              *session.native_txn, op.read(), resp->add_responses(), &context.sidecars(),
              context.GetClientDeadline(), statement_time,
              std::max(1u, req.options().active_sub_transaction_id())));
          ++catalog_.reads_;
        } else {
          RETURN_NOT_OK(catalog_.Read(
              op.read(), resp->add_responses(), &context.sidecars(), context.GetClientDeadline(),
              statement_time));
        }
      }
      ++op_index;
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
    auto status = CheckSession(req->session_id());
    if (status.ok() && (req->lock_type() < tserver::ACCESS_SHARE_LOCK ||
                        req->lock_type() > (catalog_.transaction_ ? tserver::ACCESS_EXCLUSIVE_LOCK
                                                                  : tserver::ROW_EXCLUSIVE_LOCK))) {
      status =
          STATUS_FORMAT(NotSupported, "Unsupported frozen catalog lock mode $0", req->lock_type());
    }
    if (status.ok() && req->is_session_lock()) {
      status = STATUS(NotSupported, "Session object locks are not supported");
    }
    if (status.ok()) {
      status = AcquireSqlTransaction(*sessions_.at(req->session_id()), req->options());
    }
    Respond(status, resp, context);
  }

  void RollbackToSubTransaction(
      const tserver::PgRollbackToSubTransactionRequestPB* req,
      tserver::PgRollbackToSubTransactionResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckSession(req->session_id());
    if (status.ok()) status = CheckNoPending(*sessions_.at(req->session_id()));
    if (status.ok()) {
      auto& session = *sessions_.at(req->session_id());
      if (!catalog_.transaction_ || transaction_owner_ != session.id) {
        // This backend has no mutable participant. A rejected contender must never roll
        // back the current owner's catalog or release its SQL lease.
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
  }

  void FinishTransaction(
      const tserver::PgFinishTransactionRequestPB* req,
      tserver::PgFinishTransactionResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    auto status = CheckSession(req->session_id());
    if (status.ok()) status = CheckNoPending(*sessions_.at(req->session_id()));
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
    }
    if (sessions_.contains(req->session_id()) && transaction_owner_ != req->session_id()) {
      if (sql_owner_ == req->session_id()) sql_owner_ = 0;
      sessions_.at(req->session_id())->ResetTransaction();
    }
    Respond(status, resp, context);
  }

  void ReleaseSessionObjectLock(
      const tserver::PgReleaseSessionObjectLockRequestPB* req,
      tserver::PgReleaseSessionObjectLockResponsePB* resp, rpc::RpcContext context) override {
    std::lock_guard lock(mutex_);
    Respond(CheckSession(req->session_id()), resp, context);
  }

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
  // This lease covers SQL reads and object locks, even before a storage transaction exists.
  uint64_t sql_owner_ = 0;
  uint64_t next_session_ = 0;
  std::unordered_map<uint64_t, std::shared_ptr<Session>> sessions_;
};

}  // namespace

class ReadonlyPgServer::Impl : public pgwrapper::PgWrapperContext {
 public:
  ~Impl() {
    if (service_) service_->Shutdown();
    if (postgres_ && postgres_->ProcessId()) {
      postgres_->Shutdown();
      WARN_NOT_OK(ResultToStatus(postgres_->Wait()), "Stopping local YSQL");
    }
    if (messenger_) {
      messenger_->UnregisterAllServices();
      messenger_->Shutdown();
    }
    WARN_NOT_OK(shared_memory_.ShutdownNegotiator(), "Stopping address negotiator");
  }

  Status Start(
      const std::string& directory, const std::string& snapshot, uint16_t sql_port,
      CatalogMode mode) {
    SCHECK(
        process_guard_.owns_lock(), IllegalState,
        "Only one local runtime per process is supported");
    SCHECK(sql_port != 0, InvalidArgument, "SQL port must be explicit");
    const auto marker = mode == CatalogMode::kFrozen ? kOwnerMarker : "yb-local-catalog-txn-v9\n";
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
        entity_, catalog_, messenger_->scheduler(), [this] { PublishVersions(); });
    RETURN_NOT_OK(rpc_server_->RegisterService(64, service_));
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
                             ",autovacuum=off,max_connections=20,yb_enable_ash=off,"
                             "yb_enable_pg_stat_statements_docdb_metrics=off";
    if (mode != CatalogMode::kFrozen) {
      // There is no master to promote the prerequisites normally enabled in a new universe.
      FLAGS_ysql_yb_enable_add_column_missing_default = true;
      FLAGS_ysql_yb_enable_alter_table_rewrite = true;
      FLAGS_ysql_yb_enable_ddl_atomicity_infra = true;
      FLAGS_ysql_yb_enable_ddl_savepoint_infra = true;
      FLAGS_ysql_enable_object_locking_infra = true;
      FLAGS_enable_object_lock_fastpath = false;
      FLAGS_enable_object_locking_for_table_locks = true;
      // Foreground index build holds the exclusive SQL transaction lease.
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
