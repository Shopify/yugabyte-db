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

#include <algorithm>
#include <charconv>
#include <cstring>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "yb/common/pgsql_protocol.messages.h"
#include "yb/common/schema_pbutil.h"
#include "yb/common/ql_type.h"
#include "yb/docdb/doc_read_context.h"
#include "yb/docdb/doc_write_batch.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/docdb/pgsql_operation.h"
#include "yb/docdb/ql_rocksdb_storage.h"
#include "yb/rocksdb/iterator.h"
#include "yb/rocksdb/write_batch.h"
#include "yb/rpc/sidecars.h"
#include "yb/tools/local_runtime/column_family_view.h"
#include "yb/tools/local_runtime/pg_local_request_validation.h"
#include "yb/util/env.h"
#include "yb/util/memory/arena.h"
#include "yb/util/status_format.h"
#include "yb/util/sync_point.h"
#include "yb/yql/pggate/util/pg_wire.h"

namespace yb::local_runtime {
namespace {

using detail::CheckColumns;
using detail::CheckFields;
using detail::CheckKeyValues;
using detail::CheckReferences;

constexpr auto kFormat = "ysql-local-requests/1";
constexpr auto kSchemaPrefix = "schema/";

const std::unordered_set<std::string> kReadFields = {
    "client",        "stmt_id",     "table_id",    "schema_version",          "hash_code",
    "max_hash_code", "lower_bound", "upper_bound", "partition_column_values", "range_column_values",
    "column_refs",   "col_refs",    "targets",     "is_forward_scan",         "limit"};
const std::unordered_set<std::string> kWriteFields = {
    "client",
    "stmt_id",
    "stmt_type",
    "table_id",
    "schema_version",
    "hash_code",
    "partition_column_values",
    "range_column_values",
    "column_values",
    "column_new_values",
    "column_refs",
    "col_refs"};

}  // namespace

class PgLocalStore::Impl {
 public:
  struct PartitionBounds {
    docdb::KeyBounds keys;
    docdb::EncodedPartitionBounds encoded;
  };

  struct Table {
    rocksdb::ColumnFamilyHandle* cf;
    std::unique_ptr<ColumnFamilyView> view;
    docdb::DocReadContextPtr context;
    dockv::PartitionSchema partition_schema;
    std::vector<dockv::Partition> partitions;
    std::vector<PartitionBounds> bounds;
  };

  Impl()
      : operations_("local-store"),
        packings_(std::make_shared<dockv::SchemaPackingRegistry>("local-store")) {
    options_.create_if_missing = true;
    options_.disable_wal = false;
    options_.compaction_style = rocksdb::kCompactionStyleNone;
    options_.num_levels = 1;
    options_.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
    options_.level0_stop_writes_trigger = std::numeric_limits<int>::max();
    tablet_options_.block_cache = rocksdb::NewLRUCache(16 * 1024 * 1024);
    table_options_ = options_;
    table_options_.table_factory =
        docdb::CreateRocksDBTableFactory(tablet_options_, docdb::StorageDbType::kRegular, nullptr);
  }

  Status Open(const std::string& directory) {
    const bool existed = Env::Default()->FileExists(directory);
    std::vector<std::string> names;
    if (existed) {
      RETURN_NOT_OK(rocksdb::DB::ListColumnFamilies(options_, directory, &names));
    } else {
      names.push_back(rocksdb::kDefaultColumnFamilyName);
    }
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    for (const auto& name : names) {
      descriptors.emplace_back(
          name, name == rocksdb::kDefaultColumnFamilyName ? options_ : table_options_);
    }
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* db = nullptr;
    RETURN_NOT_OK(rocksdb::DB::Open(options_, directory, descriptors, &handles, &db));
    db_.reset(db);
    for (auto* handle : handles) {
      handles_.emplace_back(handle);
    }
    if (!existed) {
      rocksdb::WriteBatch batch;
      batch.Put("format", kFormat);
      batch.Put("clock", std::to_string(latest_.ToUint64()));
      RETURN_NOT_OK(Persist(&batch));
    }
    std::string value;
    RETURN_NOT_OK(db_->Get(rocksdb::ReadOptions(), "format", &value));
    SCHECK_EQ(value, kFormat, NotSupported, "Not a supported local-request store");
    RETURN_NOT_OK(db_->Get(rocksdb::ReadOptions(), "clock", &value));
    uint64_t clock;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), clock);
    SCHECK(
        parsed.ec == std::errc() && parsed.ptr == value.data() + value.size(), Corruption,
        "Invalid persisted clock");
    latest_ = HybridTime(clock);
    SCHECK(!latest_.is_special(), Corruption, "Invalid persisted clock");
    std::unordered_map<TableId, SchemaPB> schemas;
    std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(rocksdb::ReadOptions()));
    for (iterator->Seek(kSchemaPrefix);
         iterator->Valid() && iterator->key().starts_with(kSchemaPrefix); iterator->Next()) {
      auto id = iterator->key().ToBuffer().substr(std::strlen(kSchemaPrefix));
      SCHECK(
          schemas[id].ParseFromString(iterator->value().ToBuffer()), Corruption,
          "Invalid stored schema");
    }
    RETURN_NOT_OK(iterator->status());
    iterator.reset();
    std::unordered_map<TableId, rocksdb::ColumnFamilyHandle*> families;
    for (auto* handle : handles) {
      if (handle->GetName() != rocksdb::kDefaultColumnFamilyName) {
        SCHECK(schemas.contains(handle->GetName()), Corruption, "Column family lacks schema");
        families.emplace(handle->GetName(), handle);
      }
    }
    for (const auto& [id, metadata] : schemas) {
      auto* cf = families.contains(id) ? families.at(id) : nullptr;
      if (!cf) {
        // Schema publication is durable before CF creation; finish interrupted creates.
        RETURN_NOT_OK(db_->CreateColumnFamily(table_options_, id, &cf));
        handles_.emplace_back(cf);
      }
      RETURN_NOT_OK(AddTable(id, metadata, cf));
    }
    return Status::OK();
  }

  Status CreateTable(const TableId& id, const Schema& schema, int32_t count) {
    std::lock_guard lock(mutex_);
    RETURN_NOT_OK(Status(failure_));
    SCHECK(
        !id.empty() && id != rocksdb::kDefaultColumnFamilyName, InvalidArgument,
        "Invalid table id");
    SCHECK(!tables_.contains(id), AlreadyPresent, "Table already exists");
    SCHECK(count > 0 && count <= 65536, InvalidArgument, "Invalid logical tablet count");
    SCHECK(
        schema.has_column_ids() && schema.num_hash_key_columns() > 0, NotSupported,
        "This slice requires a HASH schema with column IDs");
    SCHECK(
        !schema.has_colocation_id() && schema.cotable_id().IsNil(), NotSupported,
        "SQL colocation is not supported by this slice");
    for (const auto& column : schema.columns()) {
      const auto type = column.type()->main();
      SCHECK(
          type == DataType::INT64 || type == DataType::INT32 || type == DataType::STRING,
          NotSupported, "Unsupported prototype column type");
    }
    SchemaPB metadata;
    SchemaToPB(schema, &metadata);
    metadata.mutable_table_properties()->set_num_tablets(count);
    rocksdb::WriteBatch batch;
    batch.Put(std::string(kSchemaPrefix) + id, metadata.SerializeAsString());
    RETURN_NOT_OK(Persist(&batch));
    TEST_SYNC_POINT("PgLocalStore::SchemaPersisted");
    rocksdb::ColumnFamilyHandle* cf = nullptr;
    failure_ = db_->CreateColumnFamily(table_options_, id, &cf);
    RETURN_NOT_OK(Status(failure_));
    handles_.emplace_back(cf);
    failure_ = AddTable(id, metadata, cf);
    return failure_;
  }

  Status AddTable(const TableId& id, const SchemaPB& metadata, rocksdb::ColumnFamilyHandle* cf) {
    Schema schema;
    RETURN_NOT_OK(SchemaFromPB(metadata, &schema));
    SCHECK(
        schema.num_hash_key_columns() > 0 && metadata.table_properties().num_tablets() > 0 &&
            metadata.table_properties().num_tablets() <= 65536,
        Corruption, "Invalid stored partition metadata");
    auto table = std::make_unique<Table>();
    table->cf = cf;
    table->view = std::make_unique<ColumnFamilyView>(db_.get(), cf);
    table->context = std::make_shared<docdb::DocReadContext>(
        id, TableType::PGSQL_TABLE_TYPE, docdb::Index::kFalse, packings_, schema, 0);
    PartitionSchemaPB partition_pb;
    partition_pb.set_hash_schema(PartitionSchemaPB::PGSQL_HASH_SCHEMA);
    RETURN_NOT_OK(dockv::PartitionSchema::FromPB(partition_pb, schema, &table->partition_schema));
    RETURN_NOT_OK(table->partition_schema.CreatePartitions(
        metadata.table_properties().num_tablets(), &table->partitions));
    table->bounds.reserve(table->partitions.size());
    for (const auto& partition : table->partitions) {
      const auto lower = VERIFY_RESULT(
          table->partition_schema.GetEncodedPartitionKey(partition.partition_key_start()));
      const auto upper = VERIFY_RESULT(
          table->partition_schema.GetEncodedPartitionKey(partition.partition_key_end()));
      table->bounds.push_back(
          {docdb::KeyBounds(lower, upper),
           {.start_key = KeyBuffer(lower), .end_key = KeyBuffer(upper)}});
    }
    tables_.emplace(id, std::move(table));
    return Status::OK();
  }

  Result<Table*> Find(const TableId& id) const {
    RETURN_NOT_OK(Status(failure_));
    auto it = tables_.find(id);
    SCHECK(it != tables_.end(), NotFound, "Unknown local table: " + id);
    return it->second.get();
  }

  Status Persist(rocksdb::WriteBatch* batch) {
    RETURN_NOT_OK(Status(failure_));
    rocksdb::WriteOptions options;
    options.sync = true;
    // An I/O error can leave the WAL outcome ambiguous. Require reopen before further work.
    failure_ = db_->Write(options, batch);
    TEST_SYNC_POINT_CALLBACK("PgLocalStore::PersistStatus", &failure_);
    return failure_;
  }

  Status Write(const PgsqlWriteRequestPB& request, PgsqlResponsePB* response) {
    std::lock_guard lock(mutex_);
    SCHECK(response, InvalidArgument, "Response must not be null");
    response->Clear();
    RETURN_NOT_OK(CheckFields(request, kWriteFields));
    auto* table = VERIFY_RESULT(Find(request.table_id()));
    const auto& schema = table->context->schema();
    SCHECK_EQ(request.schema_version(), 0, InvalidArgument, "Schema version mismatch");
    SCHECK_EQ(request.client(), YQL_CLIENT_PGSQL, NotSupported, "YSQL requests only");
    VERIFY_RESULT(CheckReferences(schema, request));
    SCHECK(
        request.stmt_type() == PgsqlWriteRequestPB::PGSQL_INSERT ||
            request.stmt_type() == PgsqlWriteRequestPB::PGSQL_UPDATE ||
            request.stmt_type() == PgsqlWriteRequestPB::PGSQL_DELETE,
        NotSupported, "Unsupported write operation");
    RETURN_NOT_OK(CheckKeyValues(
        schema, request.partition_column_values(), 0, schema.num_hash_key_columns()));
    RETURN_NOT_OK(CheckKeyValues(
        schema, request.range_column_values(), schema.num_hash_key_columns(),
        schema.num_range_key_columns()));
    SCHECK(
        request.stmt_type() == PgsqlWriteRequestPB::PGSQL_INSERT || request.column_values().empty(),
        InvalidArgument, "Insert values in non-insert request");
    SCHECK(
        request.stmt_type() == PgsqlWriteRequestPB::PGSQL_UPDATE ||
            request.column_new_values().empty(),
        InvalidArgument, "Update values in non-update request");
    const auto hash = VERIFY_RESULT(
        table->partition_schema.PgsqlHashColumnCompoundValue(request.partition_column_values()));
    SCHECK(
        !request.has_hash_code() || request.hash_code() == hash, InvalidArgument,
        "Hash code does not match canonical YSQL hash");
    RETURN_NOT_OK(CheckColumns(schema, request.column_values()));
    RETURN_NOT_OK(CheckColumns(schema, request.column_new_values()));
    PgsqlWriteRequestPB normalized = request;
    normalized.set_hash_code(hash);
    ThreadSafeArena arena;
    LWPgsqlWriteRequestPB lw_request(&arena);
    lw_request.CopyFrom(normalized);
    LWPgsqlResponsePB lw_response(&arena);
    rpc::Sidecars sidecars;
    docdb::PgsqlWriteOperation operation(
        lw_request, table->context, TransactionOperationContext(), &sidecars);
    RETURN_NOT_OK(operation.Init(&lw_response));
    ScopedRWOperation pending(&operations_, CoarseTimePoint::max());
    RETURN_NOT_OK(pending);
    auto doc_db = docdb::DocDB::FromRegularUnbounded(table->view.get());
    docdb::DocWriteBatch doc_batch(doc_db, docdb::InitMarkerBehavior::kOptional, pending);
    ReadRestartData restart;
    docdb::DocOperationApplyData data{
        .doc_write_batch = &doc_batch,
        .read_operation_data = docdb::ReadOperationData::FromSingleReadTime(latest_),
        .read_restart_data = &restart,
        .schema_packing_provider = nullptr,
    };
    RETURN_NOT_OK(operation.Apply(data));
    *response = lw_response.ToGoogleProtobuf();
    SCHECK(!restart.is_valid(), TryAgain, "Read restart requested");
    if (response->status() != PgsqlResponsePB::PGSQL_STATUS_OK) {
      return Status::OK();
    }
    auto commit_time = HybridTime(
        std::max(
            latest_.ToUint64() + 1,
            HybridTime::FromMicros(Env::Default()->NowMicros()).ToUint64()));
    rocksdb::WriteBatch batch;
    IntraTxnWriteId write_id = 0;
    for (const auto& entry : doc_batch.key_value_pairs()) {
      const auto suffix = dockv::KeyEntryValue(DocHybridTime(commit_time, write_id++)).ToKeyBytes();
      batch.Put(table->cf, entry.key + suffix.ToStringBuffer(), entry.value);
    }
    batch.Put("clock", std::to_string(commit_time.ToUint64()));
    RETURN_NOT_OK(Persist(&batch));
    latest_ = commit_time;
    return Status::OK();
  }

  Result<ReadResult> Read(const PgsqlReadRequestPB& request, size_t tablet, HybridTime time) {
    std::lock_guard lock(mutex_);
    RETURN_NOT_OK(CheckFields(request, kReadFields));
    auto* table = VERIFY_RESULT(Find(request.table_id()));
    SCHECK_LT(tablet, table->partitions.size(), InvalidArgument, "Invalid logical tablet");
    SCHECK_EQ(request.schema_version(), 0, InvalidArgument, "Schema version mismatch");
    SCHECK(
        (!request.has_hash_code() || request.hash_code() <= 65535) &&
            (!request.has_max_hash_code() || request.max_hash_code() <= 65535),
        InvalidArgument, "Hash bound outside uint16 domain");
    SCHECK_EQ(request.client(), YQL_CLIENT_PGSQL, NotSupported, "YSQL requests only");
    const auto& schema = table->context->schema();
    const auto references = VERIFY_RESULT(CheckReferences(schema, request));
    for (const auto& target : request.targets()) {
      RETURN_NOT_OK(CheckFields(target, {"column_id"}));
      SCHECK(
          target.has_column_id() && references.contains(target.column_id()), InvalidArgument,
          "Projection requires a referenced ordinary column");
    }
    PgsqlReadRequestPB normalized = request;
    if (!request.partition_column_values().empty()) {
      RETURN_NOT_OK(CheckKeyValues(
          schema, request.partition_column_values(), 0, schema.num_hash_key_columns()));
      SCHECK_LE(
          static_cast<size_t>(request.range_column_values_size()), schema.num_range_key_columns(),
          InvalidArgument, "Too many range key components");
      RETURN_NOT_OK(CheckKeyValues(
          schema, request.range_column_values(), schema.num_hash_key_columns(),
          request.range_column_values_size()));
      const auto hash = VERIFY_RESULT(
          table->partition_schema.PgsqlHashColumnCompoundValue(request.partition_column_values()));
      SCHECK(
          !request.has_hash_code() || request.hash_code() == hash, InvalidArgument,
          "Hash code does not match canonical YSQL hash");
      normalized.set_hash_code(hash);
      // DocDB uses max_hash_code in the upper DocKey, including its range-key prefix.
      if (!normalized.has_max_hash_code() || normalized.max_hash_code() > hash) {
        normalized.set_max_hash_code(hash);
      }
    } else {
      SCHECK(
          request.range_column_values().empty(), NotSupported,
          "Range key prefix requires a complete hash key");
    }
    if (!time.is_valid()) {
      time = latest_;
    }
    SCHECK(!time.is_special() && time <= latest_, InvalidArgument, "Invalid read time");
    if (normalized.has_hash_code() && normalized.has_max_hash_code() &&
        normalized.hash_code() > normalized.max_hash_code()) {
      PgsqlResponsePB response;
      response.set_status(PgsqlResponsePB::PGSQL_STATUS_OK);
      WriteBuffer rows(64);
      pggate::PgWire::WriteInt64(0, &rows);
      return ReadResult{response, rows.ToBuffer(), 0, time};
    }
    const auto& bounds = table->bounds[tablet];
    docdb::DocDB doc_db{.regular = table->view.get(), .key_bounds = &bounds.keys};
    docdb::QLRocksDBStorage storage("local-store", doc_db, bounds.encoded);
    ThreadSafeArena arena;
    LWPgsqlReadRequestPB lw_request(&arena);
    lw_request.CopyFrom(normalized);
    LWPgsqlResponsePB response(&arena);
    auto read_data = docdb::ReadOperationData::FromSingleReadTime(time);
    ScopedRWOperation pending(&operations_, CoarseTimePoint::max());
    RETURN_NOT_OK(pending);
    const TransactionOperationContext txn_context;
    docdb::PgsqlReadOperationData data{
        .read_operation_data = read_data,
        .is_explicit_request_read_time = true,
        .request = lw_request,
        .doc_read_context = *table->context,
        .index_doc_read_context = nullptr,
        .txn_op_context = txn_context,
        .ql_storage = storage,
        .pending_op = pending,
        .vector_index = nullptr,
    };
    WriteBuffer rows(4096);
    ReadRestartData restart;
    docdb::PgsqlReadOperation operation(data, response, &rows, &restart);
    const auto count = VERIFY_RESULT(operation.Execute());
    SCHECK(!restart.is_valid(), TryAgain, "Read restart requested");
    response.set_status(PgsqlResponsePB::PGSQL_STATUS_OK);
    return ReadResult{response.ToGoogleProtobuf(), rows.ToBuffer(), count, time};
  }

  mutable std::mutex mutex_;
  rocksdb::Options options_;
  rocksdb::Options table_options_;
  tablet::TabletOptions tablet_options_;
  RWOperationCounter operations_;
  dockv::SchemaPackingRegistryPtr packings_;
  std::unique_ptr<rocksdb::DB> db_;
  std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> handles_;
  std::unordered_map<TableId, std::unique_ptr<Table>> tables_;
  HybridTime latest_ = HybridTime::kInitial;
  // RETURN_NOT_OK moves non-const statuses; returning this fence must copy it.
  Status failure_;
};

PgLocalStore::PgLocalStore(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
PgLocalStore::~PgLocalStore() = default;

Result<std::unique_ptr<PgLocalStore>> PgLocalStore::Open(const std::string& directory) {
  auto impl = std::make_unique<Impl>();
  RETURN_NOT_OK(impl->Open(directory));
  return std::unique_ptr<PgLocalStore>(new PgLocalStore(std::move(impl)));
}

Status PgLocalStore::CreateTable(const TableId& id, const Schema& schema, int32_t tablets) {
  return impl_->CreateTable(id, schema, tablets);
}

Result<Schema> PgLocalStore::GetSchema(const TableId& id) const {
  std::lock_guard lock(impl_->mutex_);
  return VERIFY_RESULT(impl_->Find(id))->context->schema();
}

Result<std::vector<dockv::Partition>> PgLocalStore::GetPartitions(const TableId& id) const {
  std::lock_guard lock(impl_->mutex_);
  return VERIFY_RESULT(impl_->Find(id))->partitions;
}

Result<uint16_t> PgLocalStore::HashCode(
    const TableId& id, const google::protobuf::RepeatedPtrField<PgsqlExpressionPB>& values) const {
  std::lock_guard lock(impl_->mutex_);
  const auto* table = VERIFY_RESULT(impl_->Find(id));
  RETURN_NOT_OK(CheckKeyValues(
      table->context->schema(), values, 0, table->context->schema().num_hash_key_columns()));
  return table->partition_schema.PgsqlHashColumnCompoundValue(values);
}

Status PgLocalStore::ApplyAutocommitWrite(
    const PgsqlWriteRequestPB& request, PgsqlResponsePB* response) {
  return impl_->Write(request, response);
}

Result<ReadResult> PgLocalStore::Read(
    const PgsqlReadRequestPB& request, size_t tablet, HybridTime read_time) {
  return impl_->Read(request, tablet, read_time);
}

HybridTime PgLocalStore::LatestReadTime() const {
  std::lock_guard lock(impl_->mutex_);
  return impl_->latest_;
}

}  // namespace yb::local_runtime
