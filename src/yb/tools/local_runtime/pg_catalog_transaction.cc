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

#include <charconv>
#include <cstdio>
#include <algorithm>
#include <filesystem>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <limits>

#include "yb/common/doc_hybrid_time.h"
#include "yb/common/entity_ids.h"
#include "yb/common/pgsql_error.h"
#include "yb/dockv/doc_key.h"
#include "yb/common/pgsql_protocol.messages.h"
#include "yb/docdb/doc_read_context.h"
#include "yb/docdb/doc_rowwise_iterator.h"
#include "yb/docdb/doc_write_batch.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/docdb/pgsql_operation.h"
#include "yb/dockv/reader_projection.h"
#include "yb/qlexpr/ql_expr.h"
#include "yb/rocksdb/utilities/write_batch_with_index.h"
#include "yb/rpc/sidecars.h"
#include "yb/tablet/tablet_options.h"
#include "yb/tools/local_runtime/column_family_view.h"
#include "yb/tools/local_runtime/pg_local_metadata.pb.h"
#include "yb/tools/local_runtime/pg_local_storage.h"
#include "yb/tools/local_runtime/pg_local_table.h"
#include "yb/tools/local_runtime/pg_local_txn_store.h"
#include "yb/util/env_util.h"
#include "yb/util/memory/arena.h"
#include "yb/util/status_format.h"
#include "yb/util/sync_point.h"

namespace yb::local_runtime {
namespace {

class OverlayView : public ColumnFamilyView {
 public:
  OverlayView(
      rocksdb::DB* db, rocksdb::ColumnFamilyHandle* family, rocksdb::WriteBatchWithIndex* pending)
      : ColumnFamilyView(db, family), root_(db), pending_(pending) {}

  using rocksdb::DB::Get;
  rocksdb::Status Get(
      const rocksdb::ReadOptions& options, rocksdb::ColumnFamilyHandle* family,
      const rocksdb::Slice& key, std::string* value) override {
    SCHECK(family == DefaultColumnFamily(), InvalidArgument, "Read outside catalog namespace");
    return pending_->GetFromBatchAndDB(root_, options, family, key, value);
  }

  using rocksdb::DB::NewIterator;
  rocksdb::Iterator* NewIterator(
      const rocksdb::ReadOptions& options, rocksdb::ColumnFamilyHandle* family) override {
    if (family != DefaultColumnFamily()) {
      return rocksdb::NewErrorIterator(STATUS(InvalidArgument, "Read outside catalog namespace"));
    }
    return pending_->NewIteratorWithBase(family, root_->NewIterator(options, family));
  }

  std::vector<rocksdb::Status> MultiGet(
      const rocksdb::ReadOptions& options,
      const std::vector<rocksdb::ColumnFamilyHandle*>& families,
      const std::vector<rocksdb::Slice>& keys, std::vector<std::string>* values) override {
    values->assign(keys.size(), {});
    if (families.size() != keys.size()) {
      return std::vector<rocksdb::Status>(keys.size(), STATUS(InvalidArgument, "Mismatched keys"));
    }
    std::vector<rocksdb::Status> result;
    for (size_t i = 0; i < keys.size(); ++i) {
      result.push_back(Get(options, families[i], keys[i], &(*values)[i]));
    }
    return result;
  }

  rocksdb::Status NewIterators(
      const rocksdb::ReadOptions& options,
      const std::vector<rocksdb::ColumnFamilyHandle*>& families,
      std::vector<rocksdb::Iterator*>* iterators) override {
    for (auto* family : families) {
      SCHECK(family == DefaultColumnFamily(), InvalidArgument, "Read outside catalog namespace");
    }
    for (auto* family : families) iterators->push_back(NewIterator(options, family));
    return Status::OK();
  }

  using rocksdb::DB::KeyMayExist;
  bool KeyMayExist(
      const rocksdb::ReadOptions& options, rocksdb::ColumnFamilyHandle* family,
      const rocksdb::Slice& key, std::string* value, bool* found = nullptr) override {
    const auto status = Get(options, family, key, value);
    if (found) *found = status.ok();
    return !status.IsNotFound();
  }

  std::unique_ptr<rocksdb::Iterator> NewIndexIterator(
      const rocksdb::ReadOptions&, rocksdb::SkipLastEntry, rocksdb::ColumnFamilyHandle*) override {
    return std::unique_ptr<rocksdb::Iterator>(rocksdb::NewErrorIterator(
        STATUS(NotSupported, "Physical index iteration cannot bypass the catalog overlay")));
  }

  std::unique_ptr<rocksdb::DataBlockAwareIndexIterator> NewDataBlockAwareIndexIterator(
      const rocksdb::ReadOptions& options, rocksdb::SkipLastEntry skip,
      rocksdb::ColumnFamilyHandle*) override {
    return ColumnFamilyView::NewDataBlockAwareIndexIterator(options, skip, nullptr);
  }

 private:
  rocksdb::DB* root_;
  rocksdb::WriteBatchWithIndex* pending_;
};

constexpr auto kMetadataFamily = "local-metadata";
constexpr auto kTableFamilyPrefix = "table/";

std::string MetadataKey(const std::string& name) {
  auto key = dockv::DocKey(dockv::KeyEntryValues{dockv::KeyEntryValue(name)}).Encode();
  return key.ToStringBuffer() +
         dockv::KeyEntryValue(DocHybridTime(HybridTime::kInitial, 0)).ToKeyBytes().ToStringBuffer();
}

// Fixed-width hex keeps each database's sequences contiguous in metadata key order.
std::string SequenceName(uint32_t database_oid, std::optional<uint32_t> sequence_oid = {}) {
  char name[32];
  if (sequence_oid) {
    snprintf(
        name, sizeof(name), "sequence/%08x/%08x", static_cast<unsigned>(database_oid),
        static_cast<unsigned>(*sequence_oid));
  } else {
    snprintf(name, sizeof(name), "sequence/%08x/", static_cast<unsigned>(database_oid));
  }
  return name;
}

Result<std::string> MetadataName(Slice key) {
  dockv::DocKey doc_key;
  RETURN_NOT_OK(doc_key.DecodeFrom(&key));
  SCHECK(
      doc_key.range_group().size() == 1 &&
          doc_key.range_group()[0].type() == dockv::KeyEntryType::kString,
      Corruption, "Invalid local metadata key");
  return doc_key.range_group()[0].GetString();
}

Status SequenceLimitExceeded(bool maximum) {
  return STATUS(
      QLError,
      maximum ? "nextval: reached maximum value of sequence \"%s\" (%s)"
              : "nextval: reached minimum value of sequence \"%s\" (%s)",
      Slice(), PgsqlError(YBPgErrorCode::YB_PG_SEQUENCE_GENERATOR_LIMIT_EXCEEDED));
}

}  // namespace

class PgCatalogTransaction::Impl {
 public:
  explicit Impl(std::shared_ptr<PgLocalStorage> storage)
      : storage_(std::move(storage)),
        db_(storage_->db()),
        latest_(storage_->committed()),
        failure_(storage_->failure()) {}

  Status Initialize() {
    for (const auto& [name, handle] : storage_->families()) {
      if (name != rocksdb::kDefaultColumnFamilyName && name != kMetadataFamily &&
          !name.starts_with(kTableFamilyPrefix))
        continue;
      families_.emplace(
          name, Family{handle.get(), std::make_unique<OverlayView>(db_, handle.get(), &pending_)});
    }
    // A committed descriptor is the publication marker, never the CF's physical existence.
    VERIFY_RESULT(Tables());
    VERIFY_RESULT(CatalogTables());
    for (const auto& [name, family] : families_) {
      if (name.starts_with(kTableFamilyPrefix)) created_.insert(name);
    }
    return Status::OK();
  }

  rocksdb::ColumnFamilyHandle* MetadataFamily() { return families_.at(kMetadataFamily).handle; }

  Result<rocksdb::ColumnFamilyHandle*> FamilyFor(const TableId& id) {
    RETURN_NOT_OK(Status(failure_));
    // Error reporting may need catalog tuples before PostgreSQL rolls back the failed statement.
    if (id.empty()) return families_.at(rocksdb::kDefaultColumnFamilyName).handle;
    RETURN_NOT_OK(CheckHealthy());
    SCHECK(
        storage_->active_data() == 0, NotSupported,
        "Active native transactions require intent-aware application access");
    SCHECK(
        !storage_->families().contains("intents/" + id), NotSupported,
        "Native-bound application tables require intent-aware reads and writes");
    const auto name = std::string(kTableFamilyPrefix) + id;
    SCHECK(families_.contains(name), NotFound, "Unknown local table namespace");
    std::string metadata;
    RETURN_NOT_OK(pending_.GetFromBatchAndDB(
        db_, rocksdb::ReadOptions(), MetadataFamily(), MetadataKey(name), &metadata));
    return families_.at(name).handle;
  }

  Result<rocksdb::DB*> ReadDB(const TableId& id) {
    auto* family = VERIFY_RESULT(FamilyFor(id));
    return families_.at(family->GetName()).view.get();
  }

  Result<std::vector<tablet::TableInfoPB>> CatalogTables() {
    RETURN_NOT_OK(CheckHealthy());
    std::unique_ptr<rocksdb::Iterator> iterator(pending_.NewIteratorWithBase(
        MetadataFamily(), db_->NewIterator(rocksdb::ReadOptions(), MetadataFamily())));
    std::vector<tablet::TableInfoPB> result;
    std::unordered_set<TableId> ids;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      dockv::DocKey key;
      auto slice = iterator->key();
      RETURN_NOT_OK(key.DecodeFrom(&slice));
      SCHECK(
          key.range_group().size() == 1 &&
              key.range_group()[0].type() == dockv::KeyEntryType::kString,
          Corruption, "Invalid local metadata key");
      const auto& name = key.range_group()[0].GetString();
      if (!name.starts_with("database/")) continue;
      const auto ns = name.substr(std::string("database/").size());
      const auto oid = VERIFY_RESULT(GetPgsqlDatabaseOid(ns));
      DatabasePB bundle;
      SCHECK(
          bundle.ParseFromString(iterator->value().ToBuffer()) && bundle.oid() == oid &&
              !bundle.name().empty() && !bundle.catalogs().empty(),
          Corruption, "Invalid local database descriptor");
      for (const auto& table : bundle.catalogs()) {
        SCHECK(
            table.namespace_id() == ns && table.namespace_name() == bundle.name() &&
                VERIFY_RESULT(GetPgsqlDatabaseOidByTableId(table.table_id())) == oid &&
                IsCurrentVersionYsqlCatalogTable(table.table_id()) &&
                table.schema().table_properties().is_ysql_catalog_table() &&
                ids.insert(table.table_id()).second,
            Corruption, "Invalid cloned catalog identity");
        result.push_back(table);
      }
    }
    RETURN_NOT_OK(iterator->status());
    return result;
  }

  Status CopyCatalog(const tablet::TableInfoPB& source, const tablet::TableInfoPB& target) {
    auto registry = std::make_shared<dockv::SchemaPackingRegistry>("local-database-copy");
    auto source_context = std::make_shared<docdb::DocReadContext>(
        "source-catalog", TableType::PGSQL_TABLE_TYPE, docdb::Index(source.has_index_info()),
        registry);
    auto target_context = std::make_shared<docdb::DocReadContext>(
        "target-catalog", TableType::PGSQL_TABLE_TYPE, docdb::Index(target.has_index_info()),
        registry);
    RETURN_NOT_OK(source_context->LoadFromPB(source));
    source_context->SetCotableId(VERIFY_RESULT(Uuid::FromHexString(source.table_id())));
    RETURN_NOT_OK(target_context->LoadFromPB(target));
    target_context->SetCotableId(VERIFY_RESULT(Uuid::FromHexString(target.table_id())));
    const auto& schema = source_context->schema();
    SCHECK_EQ(schema.num_hash_key_columns(), 0, Corruption, "Hash-partitioned system catalog");
    dockv::ReaderProjection projection(schema);
    ScopedRWOperation pending(&operations_, CoarseTimePoint::max());
    RETURN_NOT_OK(pending);
    // CREATE DATABASE starts a fresh transaction; the source is a committed, immutable view.
    // Do not iterate a WriteBatchWithIndex while adding the cloned rows to that same overlay.
    docdb::DocRowwiseIterator iterator(
        projection, *source_context, TransactionOperationContext(),
        docdb::DocDB::FromRegularUnbounded(db_),
        docdb::ReadOperationData::FromSingleReadTime(latest_), pending);
    RETURN_NOT_OK(iterator.InitForTableType(TableType::PGSQL_TABLE_TYPE));
    qlexpr::QLTableRow row;
    while (VERIFY_RESULT(iterator.FetchNext(&row))) {
      ThreadSafeArena arena;
      LWPgsqlWriteRequestPB request(&arena);
      request.set_client(YQL_CLIENT_PGSQL);
      request.set_stmt_type(PgsqlWriteRequestPB::PGSQL_UPSERT);
      request.dup_table_id(target.table_id());
      request.set_schema_version(target.schema_version());
      // Same native row conversion as SysCatalogWriter::InsertPgsqlTableRow.
      for (size_t i = 0; i < schema.num_columns(); ++i) {
        const auto& value = row.GetValue(schema.column_id(i));
        if (i < schema.num_key_columns()) {
          SCHECK(value, Corruption, "Missing catalog key while copying database");
          request.add_range_column_values()->mutable_value()->CopyFrom(*value);
        } else if (value) {
          auto* column = request.add_column_values();
          column->set_column_id(target_context->schema().column_id(i));
          column->mutable_expr()->mutable_value()->CopyFrom(*value);
        }
      }
      LWPgsqlResponsePB response(&arena);
      rpc::Sidecars sidecars;
      RETURN_NOT_OK(DoApply(request, target_context, &response, &sidecars));
      SCHECK_EQ(
          response.status(), PgsqlResponsePB::PGSQL_STATUS_OK, IllegalState,
          "Native catalog copy failed");
    }
    SCHECK(
        !VERIFY_RESULT(iterator.GetReadRestartData()).is_valid(), IllegalState,
        "Catalog copy requested read restart");
    return Status::OK();
  }

  Status DoCreateDatabase(
      uint32_t oid, const std::string& name, const std::vector<tablet::TableInfoPB>& sources) {
    SCHECK(active_ && staged_.empty(), NotSupported, "Database copy requires a fresh transaction");
    SCHECK(
        oid >= kPgFirstNormalObjectId && oid < kPgUpperBoundNormalObjectId && !name.empty() &&
            name.size() <= 63 && name.find('\0') == std::string::npos && !sources.empty(),
        InvalidArgument, "Invalid database identity or empty template");
    const auto ns = GetPgsqlNamespaceId(oid);
    const auto key = MetadataKey("database/" + ns);
    std::string existing;
    const auto found =
        pending_.GetFromBatchAndDB(db_, rocksdb::ReadOptions(), MetadataFamily(), key, &existing);
    if (!found.IsNotFound()) {
      RETURN_NOT_OK(found);
      return STATUS(AlreadyPresent, "Database descriptor already exists");
    }
    const auto retired = pending_.GetFromBatchAndDB(
        db_, rocksdb::ReadOptions(), MetadataFamily(), MetadataKey("retired-database/" + ns),
        &existing);
    if (!retired.IsNotFound()) {
      RETURN_NOT_OK(retired);
      return STATUS(AlreadyPresent, "Retired database OID cannot be reused");
    }
    DatabasePB bundle;
    bundle.set_oid(oid);
    bundle.set_name(name);
    bundle.set_template_oid(
        VERIFY_RESULT(GetPgsqlDatabaseOidByTableId(sources.front().table_id())));
    for (const auto& source : sources) {
      SCHECK(
          IsCurrentVersionYsqlCatalogTable(source.table_id()) &&
              source.schema().table_properties().is_ysql_catalog_table(),
          NotSupported, "Only system catalogs can be copied");
      SCHECK_EQ(
          VERIFY_RESULT(GetPgsqlDatabaseOidByTableId(source.table_id())), bundle.template_oid(),
          InvalidArgument, "Mixed database templates");
      auto* target = bundle.add_catalogs();
      *target = source;
      target->set_namespace_id(ns);
      target->set_namespace_name(name);
      target->set_table_id(
          GetPgsqlTableId(oid, VERIFY_RESULT(GetPgsqlTableOid(source.table_id()))));
      if (target->has_pg_table_id()) target->set_pg_table_id(target->table_id());
      target->clear_op_id();
      target->clear_hybrid_time();
      target->mutable_schema()->mutable_colocated_table_id()->set_cotable_id(
          VERIFY_RESULT(Uuid::FromHexString(target->table_id())).ToString());
      const auto remap_index = [oid](IndexInfoPB* index) -> Status {
        index->set_table_id(
            GetPgsqlTableId(oid, VERIFY_RESULT(GetPgsqlTableOid(index->table_id()))));
        index->set_indexed_table_id(
            GetPgsqlTableId(oid, VERIFY_RESULT(GetPgsqlTableOid(index->indexed_table_id()))));
        return Status::OK();
      };
      for (auto& index : *target->mutable_indexes()) RETURN_NOT_OK(remap_index(&index));
      if (target->has_index_info()) RETURN_NOT_OK(remap_index(target->mutable_index_info()));
      RETURN_NOT_OK(CopyCatalog(source, *target));
    }
    // Template-local objects can already occupy our normal OID range. Copy the durable
    // reservation bound, not the source process's partly consumed in-memory range.
    std::string next_oid;
    const auto oid_status = db_->Get(
        rocksdb::ReadOptions(), MetadataFamily(),
        MetadataKey("oid/" + std::to_string(bundle.template_oid())), &next_oid);
    if (oid_status.IsNotFound()) {
      next_oid = "1000000000";
    } else {
      RETURN_NOT_OK(oid_status);
    }
    uint32_t bound;
    const auto parsed = std::from_chars(next_oid.data(), next_oid.data() + next_oid.size(), bound);
    SCHECK(
        parsed.ec == std::errc() && parsed.ptr == next_oid.data() + next_oid.size() &&
            bound >= 1000000000 && bound < kPgUpperBoundNormalObjectId,
        Corruption, "Invalid template OID reservation");
    RETURN_NOT_OK(Stage(MetadataFamily(), MetadataKey("oid/" + std::to_string(oid)), next_oid));
    RETURN_NOT_OK(Stage(MetadataFamily(), key, bundle.SerializeAsString()));
    TEST_SYNC_POINT_CALLBACK("PgCatalogTransaction::DatabaseCopied", &oid);
    return Status::OK();
  }

  Status CreateDatabase(
      uint32_t oid, const std::string& name, const std::vector<tablet::TableInfoPB>& sources,
      uint32_t subtransaction) {
    RETURN_NOT_OK(CheckHealthy());
    RETURN_NOT_OK(SetSubtransaction(subtransaction));
    auto status = DoCreateDatabase(oid, name, sources);
    TEST_SYNC_POINT_CALLBACK("PgCatalogTransaction::DatabaseCopyStatus", &status);
    if (!status.ok()) {
      aborted_ = true;
      error_subtxn_ = active_subtxn_;
    }
    return status;
  }

  Status DropDatabase(uint32_t oid, const std::string& name, uint32_t subtransaction) {
    RETURN_NOT_OK(CheckHealthy());
    SCHECK(
        active_ && storage_->catalog_owner() == this, IllegalState,
        "DROP DATABASE requires exclusive catalog ownership");
    RETURN_NOT_OK(SetSubtransaction(subtransaction));
    SCHECK(
        oid >= kPgFirstNormalObjectId && oid < kPgUpperBoundNormalObjectId && !name.empty() &&
            name.size() <= 63 && name.find('\0') == std::string::npos,
        InvalidArgument, "Invalid database identity");
    const auto ns = GetPgsqlNamespaceId(oid);
    const auto bundle_key = MetadataKey("database/" + ns);
    std::string encoded;
    // A seed database has no bundle and is never eligible for storage-level DROP.
    RETURN_NOT_OK(pending_.GetFromBatchAndDB(
        db_, rocksdb::ReadOptions(), MetadataFamily(), bundle_key, &encoded));
    DatabasePB bundle;
    SCHECK(
        bundle.ParseFromString(encoded) && bundle.oid() == oid && !bundle.name().empty() &&
            !bundle.catalogs().empty(),
        Corruption, "Invalid local database descriptor");
    SCHECK_EQ(bundle.name(), name, InvalidArgument, "Database name does not match database OID");
    std::unordered_set<Uuid> catalogs;
    for (const auto& table : bundle.catalogs()) {
      SCHECK(
          table.namespace_id() == ns && table.namespace_name() == name &&
              VERIFY_RESULT(GetPgsqlDatabaseOidByTableId(table.table_id())) == oid &&
              IsCurrentVersionYsqlCatalogTable(table.table_id()) &&
              table.schema().table_properties().is_ysql_catalog_table() &&
              catalogs.insert(VERIFY_RESULT(Uuid::FromHexString(table.table_id()))).second,
          Corruption, "Invalid cloned catalog identity");
    }
    // Tables() validates that every index belongs to a base in the same namespace.
    const auto tables = VERIFY_RESULT(Tables());
    for (const auto& table : tables) {
      if (table.namespace_id() == ns && !table.has_index_info()) {
        RETURN_NOT_OK(DropTable(table.table_id(), subtransaction));
      }
    }
    // The bundle and shared SQL rows retire atomically. Historical versions are reclaimed only
    // after publication, under the caller's exclusive SQL lease and this catalog owner.
    RETURN_NOT_OK(Stage(MetadataFamily(), MetadataKey("retired-database/" + ns), encoded));
    RETURN_NOT_OK(Stage(MetadataFamily(), bundle_key, "", "", 0, true));
    return Stage(MetadataFamily(), MetadataKey("oid/" + std::to_string(oid)), "", "", 0, true);
  }

  Result<std::vector<tablet::TableInfoPB>> Tables() {
    RETURN_NOT_OK(CheckHealthy());
    std::unique_ptr<rocksdb::Iterator> iterator(pending_.NewIteratorWithBase(
        MetadataFamily(), db_->NewIterator(rocksdb::ReadOptions(), MetadataFamily())));
    std::vector<tablet::TableInfoPB> result;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      dockv::DocKey key;
      auto slice = iterator->key();
      RETURN_NOT_OK(key.DecodeFrom(&slice));
      SCHECK(
          key.range_group().size() == 1 &&
              key.range_group()[0].type() == dockv::KeyEntryType::kString,
          Corruption, "Invalid local metadata key");
      const auto& name = key.range_group()[0].GetString();
      if (!name.starts_with(kTableFamilyPrefix)) continue;
      tablet::TableInfoPB descriptor;
      SCHECK(
          descriptor.ParseFromString(iterator->value().ToBuffer()), Corruption,
          "Invalid local table descriptor");
      SCHECK(
          name == std::string(kTableFamilyPrefix) + descriptor.table_id() &&
              families_.contains(name),
          Corruption, "Descriptor has no matching column family");
      VERIFY_RESULT(NextColumnId(descriptor));
      result.push_back(std::move(descriptor));
    }
    RETURN_NOT_OK(iterator->status());
    std::unordered_map<TableId, const tablet::TableInfoPB*> by_id;
    for (const auto& table : result) {
      SCHECK(by_id.emplace(table.table_id(), &table).second, Corruption, "Duplicate descriptor");
    }
    std::unordered_set<TableId> referenced;
    for (const auto& table : result) {
      SCHECK(!table.has_index_info() || table.indexes().empty(), Corruption, "Index of index");
      for (const auto& index : table.indexes()) {
        const auto found = by_id.find(index.table_id());
        SCHECK(
            found != by_id.end() && found->second->has_index_info(), Corruption,
            "Indexed table references a missing index");
        auto expected = found->second->index_info();
        expected.set_index_permissions(INDEX_PERM_READ_WRITE_AND_DELETE);
        SCHECK(
            index.indexed_table_id() == table.table_id() &&
                found->second->namespace_id() == table.namespace_id() &&
                index.SerializeAsString() == expected.SerializeAsString() &&
                referenced.insert(index.table_id()).second,
            Corruption, "Inconsistent local index descriptors");
      }
    }
    for (const auto& table : result) {
      SCHECK(
          !table.has_index_info() || referenced.contains(table.table_id()), Corruption,
          "Index has no indexed table reference");
    }
    return result;
  }

  Result<tablet::TableInfoPB> GetTable(const TableId& id) {
    RETURN_NOT_OK(CheckHealthy());
    const auto name = std::string(kTableFamilyPrefix) + id;
    std::string encoded;
    RETURN_NOT_OK(pending_.GetFromBatchAndDB(
        db_, rocksdb::ReadOptions(), MetadataFamily(), MetadataKey(name), &encoded));
    tablet::TableInfoPB descriptor;
    SCHECK(
        descriptor.ParseFromString(encoded) && descriptor.table_id() == id &&
            families_.contains(name),
        Corruption, "Invalid local table descriptor");
    VERIFY_RESULT(NextColumnId(descriptor));
    return descriptor;
  }

  Result<uint32_t> AllocateOid(uint32_t database_oid) {
    RETURN_NOT_OK(Status(failure_));
    auto& range = oid_ranges_[database_oid];
    if (range.first != range.second) return range.first++;
    const auto key = MetadataKey("oid/" + std::to_string(database_oid));
    std::string encoded;
    auto status = db_->Get(rocksdb::ReadOptions(), MetadataFamily(), key, &encoded);
    // The bundled initdb seed occupies the low OID range. Never recycle an allocated OID.
    uint32_t oid = 1000000000;
    if (status.ok()) {
      auto parsed = std::from_chars(encoded.data(), encoded.data() + encoded.size(), oid);
      SCHECK(
          parsed.ec == std::errc() && parsed.ptr == encoded.data() + encoded.size(), Corruption,
          "Invalid local OID counter");
    } else if (!status.IsNotFound()) {
      return status;
    }
    SCHECK_LT(oid, kPgUpperBoundNormalObjectId - 1, InvalidArgument, "Local OID range exhausted");
    // Reserve before returning any ID; unused IDs in this process's block become restart gaps.
    const auto end = static_cast<uint32_t>(
        std::min<uint64_t>(static_cast<uint64_t>(oid) + 256, kPgUpperBoundNormalObjectId - 1));
    rocksdb::WriteBatch reservation;
    reservation.Put(MetadataFamily(), key, std::to_string(end));
    failure_ = storage_->Write(&reservation, true);
    TEST_SYNC_POINT_CALLBACK("PgCatalogTransaction::OidReservationStatus", &failure_);
    RETURN_NOT_OK(Status(failure_));
    range = {oid, end};
    return range.first++;
  }

  Result<std::string> SequenceKey(uint32_t database_oid, uint32_t sequence_oid) {
    SCHECK(
        database_oid != 0 && database_oid < kPgUpperBoundNormalObjectId &&
            sequence_oid >= kPgFirstNormalObjectId && sequence_oid < kPgUpperBoundNormalObjectId,
        InvalidArgument, "Invalid sequence identity");
    return MetadataKey(SequenceName(database_oid, sequence_oid));
  }

  Result<std::optional<SequenceTuple>> GetSequence(const std::string& key) {
    std::string encoded;
    const auto status = db_->Get(rocksdb::ReadOptions(), MetadataFamily(), key, &encoded);
    if (status.IsNotFound()) return std::nullopt;
    RETURN_NOT_OK(status);
    SequencePB tuple;
    SCHECK(tuple.ParseFromString(encoded), Corruption, "Invalid local sequence tuple");
    return SequenceTuple{tuple.last_value(), tuple.is_called()};
  }

  Status SyncSequence(rocksdb::WriteBatch* batch) {
    TEST_SYNC_POINT("PgCatalogTransaction::BeforeSequenceWrite");
    // Never publishes a commit time: sequence state is independent of SQL commit visibility.
    failure_ = storage_->Write(batch, true);
    TEST_SYNC_POINT_CALLBACK("PgCatalogTransaction::SequenceWriteStatus", &failure_);
    if (!failure_.ok()) return failure_.CloneAndPrepend("Sequence write outcome is unknown");
    return Status::OK();
  }

  Status PutSequence(const std::string& key, const SequenceTuple& value) {
    SequencePB tuple;
    tuple.set_last_value(value.last_value);
    tuple.set_is_called(value.is_called);
    rocksdb::WriteBatch batch;
    batch.Put(MetadataFamily(), key, tuple.SerializeAsString());
    return SyncSequence(&batch);
  }

  Status InsertSequence(uint32_t database_oid, uint32_t sequence_oid, const SequenceTuple& value) {
    RETURN_NOT_OK(Status(failure_));
    const auto key = VERIFY_RESULT(SequenceKey(database_oid, sequence_oid));
    // A completed retirement is never revisited, so no state may reappear after it.
    std::string marker;
    const auto retired = db_->Get(
        rocksdb::ReadOptions(), MetadataFamily(),
        MetadataKey("retired-database/" + GetPgsqlNamespaceId(database_oid)), &marker);
    if (!retired.IsNotFound()) {
      RETURN_NOT_OK(retired);
      return STATUS(NotSupported, "Retired databases cannot create sequences");
    }
    if (VERIFY_RESULT(GetSequence(key))) {
      return STATUS_FORMAT(
          InternalError, "Unknown error while trying to insert into sequences_data DocDB table: $0",
          PgsqlResponsePB::RequestStatus_Name(PgsqlResponsePB::PGSQL_STATUS_DUPLICATE_KEY_ERROR));
    }
    return PutSequence(key, value);
  }

  Result<SequenceTuple> ReadSequence(uint32_t database_oid, uint32_t sequence_oid) {
    RETURN_NOT_OK(Status(failure_));
    const auto tuple =
        VERIFY_RESULT(GetSequence(VERIFY_RESULT(SequenceKey(database_oid, sequence_oid))));
    if (!tuple) {
      return STATUS_FORMAT(NotFound, "Unable to find relation for sequence $0", sequence_oid);
    }
    return *tuple;
  }

  Result<bool> UpdateSequence(
      uint32_t database_oid, uint32_t sequence_oid, const SequenceTuple& value,
      const std::optional<SequenceTuple>& expected) {
    RETURN_NOT_OK(Status(failure_));
    const auto key = VERIFY_RESULT(SequenceKey(database_oid, sequence_oid));
    const auto current = VERIFY_RESULT(GetSequence(key));
    if (!current || (expected && (current->last_value != expected->last_value ||
                                  current->is_called != expected->is_called))) {
      return true;
    }
    RETURN_NOT_OK(PutSequence(key, value));
    return false;
  }

  Result<std::pair<int64_t, int64_t>> FetchSequence(
      uint32_t database_oid, uint32_t sequence_oid, uint32_t fetch_count, int64_t increment,
      int64_t min_value, int64_t max_value, bool cycle) {
    RETURN_NOT_OK(Status(failure_));
    // Native DocDB only DCHECKs these; a malformed request must not produce a wrapped range.
    SCHECK(
        fetch_count > 0 && increment != 0 && min_value <= max_value, InvalidArgument,
        "Invalid sequence fetch parameters");
    const auto key = VERIFY_RESULT(SequenceKey(database_oid, sequence_oid));
    const auto current = VERIFY_RESULT(GetSequence(key));
    if (!current) return STATUS(NotFound, "Unable to find relation for sequence");
    // A rolled-back ALTER can leave a RESTART value outside the reverted bounds. PostgreSQL would
    // return it anyway; native DocDB narrows the resulting negative count to a wrapped uint32.
    SCHECK(
        current->is_called ||
            (current->last_value >= min_value && current->last_value <= max_value),
        NotSupported, "Uncalled sequence value outside MINVALUE/MAXVALUE is not supported");
    // PgsqlWriteOperation::ApplyFetchSequence arithmetic, widened so no step can overflow.
    const __int128 step = increment;
    const __int128 min = min_value;
    const __int128 max = max_value;
    __int128 first = current->last_value;
    if (current->is_called) {
      if (step > 0 && first > max - step) {
        if (!cycle) return SequenceLimitExceeded(true);
        first = min;
      } else if (step < 0 && first < min - step) {
        if (!cycle) return SequenceLimitExceeded(false);
        first = max;
      } else {
        first += step;
      }
    }
    __int128 count = fetch_count - 1;
    if (step > 0 && first + step * count > max) {
      count = (max - first) / step;
    } else if (step < 0 && first + step * count < min) {
      count = (min - first) / step;
    }
    const auto last = static_cast<int64_t>(first + step * count);
    if (!current->is_called || current->last_value != last) {
      RETURN_NOT_OK(PutSequence(key, SequenceTuple{last, true}));
    }
    return std::make_pair(static_cast<int64_t>(first), last);
  }

  Status DeleteSequence(uint32_t database_oid, uint32_t sequence_oid) {
    RETURN_NOT_OK(Status(failure_));
    const auto key = VERIFY_RESULT(SequenceKey(database_oid, sequence_oid));
    if (!VERIFY_RESULT(GetSequence(key))) return Status::OK();
    rocksdb::WriteBatch batch;
    batch.Delete(MetadataFamily(), key);
    return SyncSequence(&batch);
  }

  // Bounded synced batches; the root iterator keeps a stable view across these writes.
  Status DeleteSequences(uint32_t database_oid) {
    constexpr size_t kBatchLimit = 256 * 1024;
    const auto prefix = SequenceName(database_oid);
    rocksdb::WriteBatch watermarks;
    watermarks.Put(
        MetadataFamily(), MetadataKey("clock"), std::to_string(storage_->clock().ToUint64()));
    watermarks.Put(
        MetadataFamily(), MetadataKey("committed"), std::to_string(latest_.ToUint64()));
    rocksdb::WriteBatch batch;
    const auto flush = [&]() -> Status {
      if (!batch.Count()) return Status::OK();
      failure_ = storage_->Write(&batch, true);
      TEST_SYNC_POINT_CALLBACK("PgCatalogTransaction::SequenceCleanupBatch", &batch);
      TEST_SYNC_POINT_CALLBACK("PgCatalogTransaction::SequenceCleanupStatus", &failure_);
      RETURN_NOT_OK(Status(failure_));
      batch.Clear();
      return Status::OK();
    };
    // '0' sorts right after '/'. The physical bound keeps the scan from walking the next
    // database's keys and tombstones; the name check alone stops only at a live key.
    auto bound_name = prefix;
    bound_name.back() = '0';
    const auto bound = MetadataKey(bound_name);
    const Slice upper(bound);
    rocksdb::ReadOptions options;
    options.iterate_upper_bound = &upper;
    std::unique_ptr<rocksdb::Iterator> rows(db_->NewIterator(options, MetadataFamily()));
    for (rows->Seek(MetadataKey(prefix)); rows->Valid(); rows->Next()) {
      const auto name = VERIFY_RESULT(MetadataName(rows->key()));
      if (!name.starts_with(prefix)) break;
      uint32_t sequence_oid = 0;
      const auto* end = name.data() + name.size();
      const auto parsed = std::from_chars(name.data() + prefix.size(), end, sequence_oid, 16);
      SCHECK(
          parsed.ec == std::errc() && parsed.ptr == end &&
              SequenceName(database_oid, sequence_oid) == name,
          Corruption, "Invalid local sequence key");
      if (batch.GetDataSize() + rows->key().size() + 16 + watermarks.GetDataSize() > kBatchLimit) {
        RETURN_NOT_OK(flush());
      }
      batch.Delete(MetadataFamily(), rows->key());
    }
    RETURN_NOT_OK(rows->status());
    rows.reset();
    return flush();
  }

  Status DeleteDatabaseSequences(uint32_t database_oid) {
    RETURN_NOT_OK(Status(failure_));
    SCHECK(
        database_oid >= kPgFirstNormalObjectId && database_oid < kPgUpperBoundNormalObjectId,
        InvalidArgument, "Invalid database OID");
    std::string marker;
    const auto retired = db_->Get(
        rocksdb::ReadOptions(), MetadataFamily(),
        MetadataKey("retired-database/" + GetPgsqlNamespaceId(database_oid)), &marker);
    if (retired.IsNotFound()) {
      return STATUS(IllegalState, "Sequence cleanup requires a retired database");
    }
    RETURN_NOT_OK(retired);
    return DeleteSequences(database_oid);
  }

  Status CreateTable(const tablet::TableInfoPB& metadata, uint32_t subtransaction) {
    RETURN_NOT_OK(CheckHealthy());
    SCHECK(active_, IllegalState, "Table creation requires an active transaction");
    SCHECK(metadata.indexes().empty(), InvalidArgument, "New tables cannot have existing indexes");
    std::optional<tablet::TableInfoPB> base;
    if (metadata.has_index_info()) {
      SCHECK(
          !storage_->families().contains("intents/" + metadata.index_info().indexed_table_id()) ||
              storage_->catalog_owner() == this,
          NotSupported, "Native-bound index DDL requires catalog ownership");
      const auto& info = metadata.index_info();
      base = VERIFY_RESULT(GetTable(info.indexed_table_id()));
      SCHECK(
          !base->has_index_info() && base->namespace_id() == metadata.namespace_id() &&
              info.table_id() == metadata.table_id(),
          InvalidArgument, "Invalid indexed table identity");
      auto* index = base->add_indexes();
      *index = info;
      index->set_index_permissions(INDEX_PERM_READ_WRITE_AND_DELETE);
    }
    const auto name = std::string(kTableFamilyPrefix) + metadata.table_id();
    SCHECK(!families_.contains(name), AlreadyPresent, "Local table already exists");
    RETURN_NOT_OK(SetSubtransaction(subtransaction));
    const auto next_column_id = VERIFY_RESULT(NextColumnId(metadata, true));
    auto* family = VERIFY_RESULT(storage_->CreateFamily(name));
    families_.emplace(name, Family{family, std::make_unique<OverlayView>(db_, family, &pending_)});
    created_.insert(name);
    TEST_SYNC_POINT("PgCatalogTransaction::FamilyCreated");
    auto status = Stage(MetadataFamily(), MetadataKey(name), metadata.SerializeAsString());
    if (status.ok()) {
      status = Stage(
          MetadataFamily(), MetadataKey("column-id/" + metadata.table_id()),
          next_column_id.ToString());
    }
    if (status.ok() && base) {
      status = Stage(
          MetadataFamily(), MetadataKey(std::string(kTableFamilyPrefix) + base->table_id()),
          base->SerializeAsString());
    }
    if (!status.ok()) {
      aborted_ = true;
      error_subtxn_ = active_subtxn_;
    }
    return status;
  }

  Result<ColumnId> NextColumnId(const tablet::TableInfoPB& metadata, bool creating = false) {
    Schema schema;
    RETURN_NOT_OK(SchemaFromPB(metadata.schema(), &schema));
    int32_t next = SchemaBuilder(schema).next_column_id().rep();
    std::string encoded;
    auto status = pending_.GetFromBatchAndDB(
        db_, rocksdb::ReadOptions(), MetadataFamily(),
        MetadataKey("column-id/" + metadata.table_id()), &encoded);
    if (status.ok()) {
      int32_t stored;
      const auto parsed = std::from_chars(encoded.data(), encoded.data() + encoded.size(), stored);
      SCHECK(
          parsed.ec == std::errc() && parsed.ptr == encoded.data() + encoded.size() &&
              stored >= next && stored < std::numeric_limits<int32_t>::max(),
          Corruption, "Invalid next column ID");
      next = stored;
    } else {
      SCHECK(status.IsNotFound() && creating, Corruption, "Missing column ID watermark");
    }
    return ColumnId(next);
  }

  Status AlterTable(const tserver::PgAlterTableRequestPB& request) {
    return ChangeSchema(
        PgObjectId::GetYbTableIdFromPB(request.table_id()),
        request.options().active_sub_transaction_id(), &request);
  }

  Status IncrementSchemaVersion(const TableId& id, uint32_t subtransaction) {
    return ChangeSchema(id, subtransaction, nullptr);
  }

  Status ChangeSchema(
      const TableId& id, uint32_t subtransaction, const tserver::PgAlterTableRequestPB* request) {
    RETURN_NOT_OK(CheckHealthy());
    SCHECK(active_, IllegalState, "Schema change requires an active transaction");
    SCHECK(
        !storage_->families().contains("intents/" + id) || storage_->catalog_owner() == this,
        NotSupported, "Native-bound schema changes require catalog ownership");
    RETURN_NOT_OK(SetSubtransaction(subtransaction));
    auto metadata = VERIFY_RESULT(GetTable(id));
    SCHECK_LT(
        metadata.schema_version(), std::numeric_limits<uint32_t>::max(), InvalidArgument,
        "Schema version exhausted");
    const auto version = metadata.schema_version() + 1;
    auto registry = std::make_shared<dockv::SchemaPackingRegistry>("local-schema");
    docdb::DocReadContext context(
        "local-schema", TableType::PGSQL_TABLE_TYPE, docdb::Index(metadata.has_index_info()),
        registry);
    RETURN_NOT_OK(context.LoadFromPB(metadata));
    auto next_column_id = VERIFY_RESULT(NextColumnId(metadata));
    if (request) RETURN_NOT_OK(AlterLocalTable(*request, &metadata, &next_column_id));
    Schema schema;
    RETURN_NOT_OK(SchemaFromPB(metadata.schema(), &schema));
    // Keep the old packing, including the version being replaced, for existing rows and intents.
    docdb::DocReadContext changed(context, schema, version);
    metadata.clear_old_schema_packings();
    changed.ToPB(version, &metadata);
    metadata.set_schema_version(version);
    std::optional<tablet::TableInfoPB> base;
    if (metadata.has_index_info()) {
      metadata.mutable_index_info()->set_version(version);
      base = VERIFY_RESULT(GetTable(metadata.index_info().indexed_table_id()));
      bool found = false;
      for (auto& index : *base->mutable_indexes()) {
        if (index.table_id() == id) {
          index.set_version(version);
          found = true;
        }
      }
      SCHECK(found, Corruption, "Index has no base-table reference");
    }
    RETURN_NOT_OK(
        Stage(MetadataFamily(), MetadataKey("column-id/" + id), next_column_id.ToString()));
    auto status = Stage(
        MetadataFamily(), MetadataKey(std::string(kTableFamilyPrefix) + id),
        metadata.SerializeAsString());
    if (status.ok() && base) {
      status = Stage(
          MetadataFamily(), MetadataKey(std::string(kTableFamilyPrefix) + base->table_id()),
          base->SerializeAsString());
    }
    if (!status.ok()) {
      aborted_ = true;
      error_subtxn_ = active_subtxn_;
    }
    return status;
  }

  Status DropTable(const TableId& id, uint32_t subtransaction) {
    RETURN_NOT_OK(CheckHealthy());
    SCHECK(
        active_ && storage_->catalog_owner() == this, IllegalState,
        "DROP requires exclusive catalog ownership");
    RETURN_NOT_OK(SetSubtransaction(subtransaction));
    auto metadata = VERIFY_RESULT(GetTable(id));
    std::vector<TableId> removed{id};
    if (metadata.has_index_info()) {
      auto base = VERIFY_RESULT(GetTable(metadata.index_info().indexed_table_id()));
      bool found = false;
      for (int i = base.indexes_size() - 1; i >= 0; --i) {
        if (base.indexes(i).table_id() != id) continue;
        base.mutable_indexes()->DeleteSubrange(i, 1);
        found = true;
      }
      SCHECK(found, Corruption, "Index has no base-table reference");
      RETURN_NOT_OK(Stage(
          MetadataFamily(), MetadataKey(std::string(kTableFamilyPrefix) + base.table_id()),
          base.SerializeAsString()));
    } else {
      for (const auto& index : metadata.indexes()) {
        auto secondary = VERIFY_RESULT(GetTable(index.table_id()));
        SCHECK(
            secondary.has_index_info() && secondary.index_info().indexed_table_id() == id,
            Corruption, "Invalid secondary descriptor");
        removed.push_back(index.table_id());
      }
    }
    for (const auto& table : removed) {
      const auto name = std::string(kTableFamilyPrefix) + table;
      created_.insert(name);
      RETURN_NOT_OK(Stage(MetadataFamily(), MetadataKey(name), "", "", 0, true));
      RETURN_NOT_OK(Stage(MetadataFamily(), MetadataKey("column-id/" + table), "", "", 0, true));
    }
    return Status::OK();
  }

  Status SetSubtransaction(uint32_t subtransaction) {
    subtransaction = std::max(1u, subtransaction);
    // PgGate returns catalog-version writes to a live parent (including ID 1 at COMMIT).
    // A high-water mark cannot distinguish that from replay; only aborted IDs are forbidden.
    for (const auto& [first, last] : aborted_ranges_) {
      SCHECK(
          subtransaction < first || subtransaction > last, IllegalState, "Aborted subtransaction");
    }
    if (native_txn_) RETURN_NOT_OK(native_->SetCatalogSubtransaction(*native_txn_, subtransaction));
    highest_subtxn_ = std::max(highest_subtxn_, subtransaction);
    active_subtxn_ = subtransaction;
    return Status::OK();
  }

  Status Stage(
      rocksdb::ColumnFamilyHandle* family, std::string key, std::string value,
      std::string doc_key = "", IntraTxnWriteId write_id = 0, bool deleted = false) {
    // Include the maximum tag, CF ID and length-varint overhead before extending the overlay.
    SCHECK_LE(
        pending_.GetWriteBatch()->GetDataSize() + key.size() + value.size() + 16,
        16ULL * 1024 * 1024, InvalidArgument, "Local transaction exceeds 16 MiB staging limit");
    staged_.push_back(
        {active_subtxn_, family, std::move(key), std::move(value), std::move(doc_key), write_id,
         deleted});
    const auto& entry = staged_.back();
    if (entry.deleted)
      pending_.Delete(family, entry.key);
    else
      pending_.Put(family, entry.key, entry.value);
    SCHECK_LE(
        pending_.GetWriteBatch()->GetDataSize(), 16ULL * 1024 * 1024, InvalidArgument,
        "Local transaction exceeds 16 MiB staging limit");
    return Status::OK();
  }

  void Rebuild() {
    pending_.Clear();
    for (const auto& entry : staged_) {
      if (entry.deleted)
        pending_.Delete(entry.family, entry.key);
      else
        pending_.Put(entry.family, entry.key, entry.value);
    }
  }

  Status CleanupOrphans() {
    RETURN_NOT_OK(Status(failure_));
    std::vector<std::string> orphaned;
    for (const auto& name : created_) {
      std::string descriptor;
      auto status = pending_.GetFromBatchAndDB(
          db_, rocksdb::ReadOptions(), MetadataFamily(), MetadataKey(name), &descriptor);
      if (status.IsNotFound()) {
        // A pending DROP may survive a savepoint rollback. Its published storage stays live.
        if (active_) {
          const auto key = MetadataKey(name);
          if (std::any_of(staged_.begin(), staged_.end(), [&](const auto& entry) {
                return entry.family == MetadataFamily() && entry.key == key && entry.deleted;
              }))
            continue;
          status =
              db_->Get(rocksdb::ReadOptions(), MetadataFamily(), MetadataKey(name), &descriptor);
          if (status.ok()) continue;
          if (!status.IsNotFound()) return status;
        }
        orphaned.push_back(name);
      } else {
        RETURN_NOT_OK(status);
      }
    }
    for (const auto& name : orphaned) {
      auto* family = families_.at(name).handle;
      std::erase_if(staged_, [family](const auto& entry) { return entry.family == family; });
      Rebuild();
      families_.at(name).view.reset();
      RETURN_NOT_OK(storage_->DropFamily(name));
      families_.erase(name);
      created_.erase(name);
    }
    return Status::OK();
  }

  Status CleanupRetiredDatabases() {
    RETURN_NOT_OK(Status(failure_));
    constexpr size_t kBatchLimit = 256 * 1024;
    const std::string prefix = "retired-database/";
    // Sequence keys and tombstones sort right after the markers; the name check alone would walk
    // every leading tombstone on each cleanup pass.
    const auto bound = MetadataKey("retired-database0");
    const Slice upper(bound);
    rocksdb::ReadOptions options;
    options.iterate_upper_bound = &upper;
    std::unique_ptr<rocksdb::Iterator> markers(db_->NewIterator(options, MetadataFamily()));
    for (markers->Seek(MetadataKey(prefix)); markers->Valid(); markers->Next()) {
      dockv::DocKey key;
      auto slice = markers->key();
      RETURN_NOT_OK(key.DecodeFrom(&slice));
      SCHECK(
          key.range_group().size() == 1 &&
              key.range_group()[0].type() == dockv::KeyEntryType::kString,
          Corruption, "Invalid local metadata key");
      const auto& name = key.range_group()[0].GetString();
      if (!name.starts_with(prefix)) break;
      const auto ns = name.substr(prefix.size());
      const auto oid = VERIFY_RESULT(GetPgsqlDatabaseOid(ns));
      SCHECK(
          oid >= kPgFirstNormalObjectId && oid < kPgUpperBoundNormalObjectId &&
              ns == GetPgsqlNamespaceId(oid),
          Corruption, "Invalid retired database namespace");
      std::string live;
      const auto found =
          db_->Get(rocksdb::ReadOptions(), MetadataFamily(), MetadataKey("database/" + ns), &live);
      if (!found.IsNotFound()) {
        RETURN_NOT_OK(found);
        return STATUS(Corruption, "Retired database is still published");
      }
      if (markers->value().ToBuffer() == "complete") continue;
      DatabasePB bundle;
      SCHECK(
          bundle.ParseFromString(markers->value().ToBuffer()) && bundle.oid() == oid &&
              !bundle.name().empty() && !bundle.catalogs().empty(),
          Corruption, "Invalid retired database descriptor");
      std::unordered_set<Uuid> catalogs;
      for (const auto& table : bundle.catalogs()) {
        SCHECK(
            table.namespace_id() == ns && table.namespace_name() == bundle.name() &&
                VERIFY_RESULT(GetPgsqlDatabaseOidByTableId(table.table_id())) == oid &&
                IsCurrentVersionYsqlCatalogTable(table.table_id()) &&
                table.schema().table_properties().is_ysql_catalog_table() &&
                catalogs.insert(VERIFY_RESULT(Uuid::FromHexString(table.table_id()))).second,
            Corruption, "Invalid retired catalog identity");
      }
      TEST_SYNC_POINT("PgCatalogTransaction::BeforeRetiredDatabaseCleanup");
      rocksdb::WriteBatch watermarks;
      watermarks.Put(
          MetadataFamily(), MetadataKey("clock"), std::to_string(storage_->clock().ToUint64()));
      watermarks.Put(
          MetadataFamily(), MetadataKey("committed"), std::to_string(latest_.ToUint64()));
      rocksdb::WriteBatch batch;
      const auto flush = [&]() -> Status {
        if (!batch.Count()) return Status::OK();
        RETURN_NOT_OK(storage_->Write(&batch, true));
        TEST_SYNC_POINT_CALLBACK("PgCatalogTransaction::RetiredDatabaseCleanupBatch", &batch);
        batch.Clear();
        return Status::OK();
      };
      // Root iterators have a stable view across WAL batches; never mutate a WBI being iterated.
      auto* family = families_.at(rocksdb::kDefaultColumnFamilyName).handle;
      std::unique_ptr<rocksdb::Iterator> rows(db_->NewIterator(rocksdb::ReadOptions(), family));
      for (rows->SeekToFirst(); rows->Valid(); rows->Next()) {
        Uuid cotable;
        if (!VERIFY_RESULT(dockv::DocKeyDecoder(rows->key()).DecodeCotableId(&cotable)) ||
            !catalogs.contains(cotable))
          continue;
        const auto bytes = rows->key().size() + 16;
        if (batch.GetDataSize() + bytes + watermarks.GetDataSize() > kBatchLimit) {
          RETURN_NOT_OK(flush());
        }
        SCHECK_LE(
            batch.GetDataSize() + bytes + watermarks.GetDataSize(), kBatchLimit, NotSupported,
            "Retired catalog key exceeds cleanup batch limit");
        batch.Delete(family, rows->key());
      }
      RETURN_NOT_OK(rows->status());
      rows.reset();
      RETURN_NOT_OK(flush());
      RETURN_NOT_OK(DeleteSequences(oid));
      batch.Put(MetadataFamily(), markers->key(), "complete");
      RETURN_NOT_OK(flush());
    }
    return markers->status();
  }

  Status StatementStatus(Status status, uint32_t subtransaction) {
    if (!status.ok() && active_) {
      aborted_ = true;
      error_subtxn_ = std::max(highest_subtxn_, std::max(1u, subtransaction));
    }
    return status;
  }

  Status CheckHealthy() const {
    RETURN_NOT_OK(Status(failure_));
    SCHECK(!aborted_, IllegalState, "Catalog transaction is aborted; rollback is required");
    if (native_txn_) RETURN_NOT_OK(native_->CheckCatalogHealthy(*native_txn_));
    return Status::OK();
  }

  Status Begin() {
    RETURN_NOT_OK(CheckHealthy());
    SCHECK(!active_, IllegalState, "Catalog transaction already active");
    time_ = VERIFY_RESULT(storage_->Tick());
    RETURN_NOT_OK(storage_->AcquireCatalog(this));
    active_ = true;
    return Status::OK();
  }

  Status Apply(
      const PgsqlWriteRequestMsg& request, const docdb::DocReadContextPtr& context,
      PgsqlResponseMsg* response, rpc::Sidecars* sidecars, uint32_t subtransaction) {
    RETURN_NOT_OK(CheckHealthy());
    RETURN_NOT_OK(SetSubtransaction(subtransaction));
    SCHECK(active_, IllegalState, "No active catalog transaction");
    auto status = DoApply(request, context, response, sidecars);
    if (!status.ok() || response->status() != PgsqlResponsePB::PGSQL_STATUS_OK) {
      aborted_ = true;
      error_subtxn_ = active_subtxn_;
    }
    return status;
  }

  Status DoApply(
      const PgsqlWriteRequestMsg& request, const docdb::DocReadContextPtr& context,
      PgsqlResponseMsg* response, rpc::Sidecars* sidecars) {
    SCHECK(
        request.stmt_type() == PgsqlWriteRequestPB::PGSQL_INSERT ||
            request.stmt_type() == PgsqlWriteRequestPB::PGSQL_UPDATE ||
            request.stmt_type() == PgsqlWriteRequestPB::PGSQL_DELETE ||
            request.stmt_type() == PgsqlWriteRequestPB::PGSQL_UPSERT,
        NotSupported, "Unsupported catalog write operation");
    SCHECK(!request.is_backfill(), NotSupported, "Online index backfill writes are not supported");
    if (context->schema().cotable_id().IsNil()) {
      SCHECK(!native_txn_, NotSupported, "Joint transactions require native application writes");
      SCHECK(
          !storage_->families().contains("intents/" + request.table_id().ToBuffer()), NotSupported,
          "Native-bound application tables cannot use the catalog overlay");
    }
    time_ = VERIFY_RESULT(storage_->Tick());
    docdb::PgsqlWriteOperation operation(request, context, TransactionOperationContext(), sidecars);
    RETURN_NOT_OK(operation.Init(response));
    ScopedRWOperation operation_guard(&operations_, CoarseTimePoint::max());
    RETURN_NOT_OK(operation_guard);
    auto* family = VERIFY_RESULT(
        FamilyFor(context->schema().cotable_id().IsNil() ? request.table_id().ToBuffer() : ""));
    auto db = docdb::DocDB::FromRegularUnbounded(families_.at(family->GetName()).view.get());
    docdb::DocWriteBatch batch(db, docdb::InitMarkerBehavior::kOptional, operation_guard);
    ReadRestartData restart;
    docdb::DocOperationApplyData data{
        .doc_write_batch = &batch,
        .read_operation_data = docdb::ReadOperationData::FromSingleReadTime(time_),
        .read_restart_data = &restart,
        .schema_packing_provider = nullptr,
    };
    RETURN_NOT_OK(operation.Apply(data));
    SCHECK(!restart.is_valid(), NotSupported, "Catalog transaction restart is not supported");
    if (response->status() != PgsqlResponsePB::PGSQL_STATUS_OK) return Status::OK();
    SCHECK_LT(
        batch.key_value_pairs().size(), std::numeric_limits<IntraTxnWriteId>::max() - write_id_,
        InvalidArgument, "Catalog transaction write IDs exhausted");
    auto next_write_id = native_txn_ ? VERIFY_RESULT(native_->ReserveCatalogWriteIds(
                                           *native_txn_, batch.key_value_pairs().size()))
                                     : write_id_;
    for (const auto& entry : batch.key_value_pairs()) {
      const auto write_id = next_write_id++;
      auto suffix = dockv::KeyEntryValue(DocHybridTime(time_, write_id)).ToKeyBytes();
      auto key = entry.key + suffix.ToStringBuffer();
      RETURN_NOT_OK(Stage(family, std::move(key), entry.value, entry.key, write_id));
    }
    if (!native_txn_) write_id_ = next_write_id;
    return Status::OK();
  }

  Status Commit() {
    RETURN_NOT_OK(CheckHealthy());
    SCHECK(active_, IllegalState, "No active catalog transaction");
    const auto commit_time = VERIFY_RESULT(storage_->Tick());
    rocksdb::WriteBatch commit;
    for (const auto& entry : staged_) {
      auto key =
          entry.doc_key.empty()
              ? entry.key
              : entry.doc_key + dockv::KeyEntryValue(DocHybridTime(commit_time, entry.write_id))
                                    .ToKeyBytes()
                                    .ToStringBuffer();
      if (entry.deleted)
        commit.Delete(entry.family, key);
      else
        commit.Put(entry.family, key, entry.value);
    }
    if (native_txn_) {
      RETURN_NOT_OK(native_->AppendCatalogCommit(*native_txn_, commit_time, &commit));
    }
    // Storage::Write appends these watermarks. Include them, but not already durable intents.
    rocksdb::WriteBatch watermarks;
    watermarks.Put(
        MetadataFamily(), MetadataKey("clock"), std::to_string(storage_->clock().ToUint64()));
    watermarks.Put(
        MetadataFamily(), MetadataKey("committed"), std::to_string(commit_time.ToUint64()));
    SCHECK_LE(
        commit.GetDataSize() + watermarks.GetDataSize(), 16ULL * 1024 * 1024, NotSupported,
        "Catalog commit exceeds 16 MiB batch limit");
    TEST_SYNC_POINT("PgCatalogTransaction::BeforeCommit");
    failure_ = storage_->Write(&commit, true, commit_time);
    TEST_SYNC_POINT_CALLBACK("PgCatalogTransaction::CommitStatus", &failure_);
    RETURN_NOT_OK(Status(failure_));
    std::erase_if(oid_ranges_, [&](const auto& range) {
      const auto key = MetadataKey("oid/" + std::to_string(range.first));
      return std::any_of(staged_.begin(), staged_.end(), [&](const auto& entry) {
        return entry.family == MetadataFamily() && entry.deleted && entry.key == key;
      });
    });
    if (native_txn_) {
      native_->CatalogCommitted(*native_txn_, commit_time);
      native_txn_.reset();
    }
    TEST_SYNC_POINT("PgCatalogTransaction::AfterCommit");
    Abort();
    return failure_;
  }

  Status RollbackTo(uint32_t subtransaction) {
    RETURN_NOT_OK(Status(failure_));
    SCHECK_GE(subtransaction, 1u, InvalidArgument, "Invalid subtransaction");
    SCHECK_LT(aborted_ranges_.size(), 1024, NotSupported, "Catalog savepoint limit reached");
    if (!active_) return Status::OK();
    if (native_txn_) {
      highest_subtxn_ =
          std::max(highest_subtxn_, VERIFY_RESULT(native_->HighestSubtransaction(*native_txn_)));
    }
    highest_subtxn_ = std::max(highest_subtxn_, subtransaction);
    if (native_txn_) RETURN_NOT_OK(native_->RollbackCatalog(*native_txn_, subtransaction));
    aborted_ranges_.emplace_back(subtransaction, highest_subtxn_);
    std::erase_if(staged_, [subtransaction](const auto& entry) {
      return entry.subtransaction >= subtransaction;
    });
    Rebuild();
    if (error_subtxn_ >= subtransaction) aborted_ = false;
    if (native_) {
      preserve_deleted_bindings_ = true;
      auto status = native_->RefreshCatalogBindings();
      preserve_deleted_bindings_ = false;
      RETURN_NOT_OK(status);
    }
    RETURN_NOT_OK(CleanupOrphans());
    return Status::OK();
  }

  void Abort() {
    if (native_txn_) {
      if (failure_.ok()) failure_ = native_->AbortCatalog(*native_txn_);
      native_txn_.reset();
    }
    pending_.Clear();
    staged_.clear();
    aborted_ranges_.clear();
    highest_subtxn_ = 1;
    error_subtxn_ = 0;
    active_ = false;
    aborted_ = false;
    write_id_ = 0;
    // A failed commit acknowledgement may have published these families. Recovery decides;
    // do not destroy storage or clear the fail-stop fence on this path.
    if (failure_.ok() && native_) failure_ = native_->RefreshCatalogBindings();
    if (failure_.ok() && native_) failure_ = CleanupOrphans();
    if (failure_.ok() && storage_->catalog_owner() == this) {
      failure_ = CleanupRetiredDatabases();
    }
    if (failure_.ok()) created_.clear();
    storage_->ReleaseCatalog(this);
  }

  struct StagedEntry {
    uint32_t subtransaction;
    rocksdb::ColumnFamilyHandle* family;
    std::string key;
    std::string value;
    std::string doc_key;
    IntraTxnWriteId write_id;
    bool deleted;
  };
  std::vector<StagedEntry> staged_;
  std::vector<std::pair<uint32_t, uint32_t>> aborted_ranges_;
  uint32_t active_subtxn_ = 1;
  uint32_t highest_subtxn_ = 1;
  uint32_t error_subtxn_ = 0;
  std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>> oid_ranges_;
  std::shared_ptr<PgLocalStorage> storage_;
  rocksdb::DB* db_;
  rocksdb::WriteBatchWithIndex pending_{rocksdb::BytewiseComparator(), 0, true};
  struct Family {
    rocksdb::ColumnFamilyHandle* handle;
    std::unique_ptr<OverlayView> view;
  };
  std::unordered_map<std::string, Family> families_;
  std::unordered_set<std::string> created_;
  std::unique_ptr<PgLocalTxnStore> native_;
  std::optional<TransactionId> native_txn_;
  RWOperationCounter operations_{"local-catalog-txn"};
  HybridTime& latest_;
  HybridTime time_;
  IntraTxnWriteId write_id_ = 0;
  bool active_ = false;
  bool aborted_ = false;
  bool preserve_deleted_bindings_ = false;
  // RETURN_NOT_OK moves non-const statuses; returning this fence must copy it.
  Status& failure_;
};

PgCatalogTransaction::PgCatalogTransaction(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
PgCatalogTransaction::~PgCatalogTransaction() {
  std::lock_guard lock(impl_->storage_->mutex());
  impl_->Abort();
}
Result<std::unique_ptr<PgCatalogTransaction>> PgCatalogTransaction::Open(
    const std::string& directory, const std::string& seed) {
  auto storage = VERIFY_RESULT(PgLocalStorage::Open(directory, seed));
  auto impl = std::make_unique<Impl>(storage);
  RETURN_NOT_OK(impl->Initialize());
  auto result = std::unique_ptr<PgCatalogTransaction>(new PgCatalogTransaction(std::move(impl)));
  result->impl_->native_ = VERIFY_RESULT(
      PgLocalTxnStore::Attach(
          storage, result->impl_.get(),
          [ptr = result->impl_.get()](const TableId& id) -> Result<tablet::TableInfoPB> {
            auto table = ptr->GetTable(id);
            if (table.ok() || !table.status().IsNotFound() || !ptr->preserve_deleted_bindings_)
              return table;
            for (auto it = ptr->staged_.rbegin(); it != ptr->staged_.rend(); ++it) {
              if (it->family != ptr->MetadataFamily() || it->deleted ||
                  it->key != MetadataKey(std::string(kTableFamilyPrefix) + id))
                continue;
              tablet::TableInfoPB staged;
              SCHECK(staged.ParseFromString(it->value), Corruption, "Invalid staged descriptor");
              return staged;
            }
            std::string encoded;
            RETURN_NOT_OK(ptr->db_->Get(
                rocksdb::ReadOptions(), ptr->MetadataFamily(),
                MetadataKey(std::string(kTableFamilyPrefix) + id), &encoded));
            tablet::TableInfoPB committed;
            SCHECK(committed.ParseFromString(encoded), Corruption, "Invalid table descriptor");
            return committed;
          },
          [ptr = result->impl_.get()](uint32_t subtransaction) {
            RETURN_NOT_OK(ptr->CheckHealthy());
            return ptr->StatementStatus(ptr->SetSubtransaction(subtransaction), subtransaction);
          },
          [ptr = result->impl_.get()] { return ptr->pending_.GetWriteBatch()->GetDataSize(); }));
  // Attachment releases native views and removes orphan intents before regular CF cleanup.
  std::lock_guard lock(storage->mutex());
  RETURN_NOT_OK(storage->AcquireCatalog(result->impl_.get()));
  result->impl_->failure_ = result->impl_->CleanupOrphans();
  if (result->impl_->failure_.ok()) {
    result->impl_->failure_ = result->impl_->CleanupRetiredDatabases();
  }
  storage->ReleaseCatalog(result->impl_.get());
  RETURN_NOT_OK(Status(result->impl_->failure_));
  result->impl_->created_.clear();
  return result;
}
Result<rocksdb::DB*> PgCatalogTransaction::ReadDB(const TableId& table) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->ReadDB(table);
}
Result<uint32_t> PgCatalogTransaction::AllocateOid(uint32_t database_oid) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->AllocateOid(database_oid);
}
Status PgCatalogTransaction::InsertSequence(
    uint32_t database_oid, uint32_t sequence_oid, const SequenceTuple& value) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->InsertSequence(database_oid, sequence_oid, value);
}
Result<PgCatalogTransaction::SequenceTuple> PgCatalogTransaction::ReadSequence(
    uint32_t database_oid, uint32_t sequence_oid) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->ReadSequence(database_oid, sequence_oid);
}
Result<bool> PgCatalogTransaction::UpdateSequence(
    uint32_t database_oid, uint32_t sequence_oid, const SequenceTuple& value,
    const std::optional<SequenceTuple>& expected) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->UpdateSequence(database_oid, sequence_oid, value, expected);
}
Result<std::pair<int64_t, int64_t>> PgCatalogTransaction::FetchSequence(
    uint32_t database_oid, uint32_t sequence_oid, uint32_t fetch_count, int64_t increment,
    int64_t min_value, int64_t max_value, bool cycle) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->FetchSequence(
      database_oid, sequence_oid, fetch_count, increment, min_value, max_value, cycle);
}
Status PgCatalogTransaction::DeleteSequence(uint32_t database_oid, uint32_t sequence_oid) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->DeleteSequence(database_oid, sequence_oid);
}
Status PgCatalogTransaction::DeleteDatabaseSequences(uint32_t database_oid) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->DeleteDatabaseSequences(database_oid);
}
Status PgCatalogTransaction::CreateDatabase(
    uint32_t oid, const std::string& name, const std::vector<tablet::TableInfoPB>& sources,
    uint32_t subtransaction) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->CreateDatabase(oid, name, sources, subtransaction);
}
Status PgCatalogTransaction::DropDatabase(
    uint32_t oid, const std::string& name, uint32_t subtransaction) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->StatementStatus(impl_->DropDatabase(oid, name, subtransaction), subtransaction);
}
Result<std::vector<tablet::TableInfoPB>> PgCatalogTransaction::CatalogTables() {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->CatalogTables();
}
Status PgCatalogTransaction::CreateTable(
    const tablet::TableInfoPB& metadata, uint32_t subtransaction) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->StatementStatus(impl_->CreateTable(metadata, subtransaction), subtransaction);
}
Status PgCatalogTransaction::AlterTable(const tserver::PgAlterTableRequestPB& request) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->StatementStatus(
      impl_->AlterTable(request), request.options().active_sub_transaction_id());
}
Status PgCatalogTransaction::DropTable(const TableId& id, uint32_t subtransaction) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->StatementStatus(impl_->DropTable(id, subtransaction), subtransaction);
}

Status PgCatalogTransaction::IncrementSchemaVersion(const TableId& id, uint32_t subtransaction) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->StatementStatus(impl_->IncrementSchemaVersion(id, subtransaction), subtransaction);
}
Result<std::vector<tablet::TableInfoPB>> PgCatalogTransaction::Tables() {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->Tables();
}
Result<tablet::TableInfoPB> PgCatalogTransaction::GetTable(const TableId& id) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->GetTable(id);
}
HybridTime PgCatalogTransaction::ReadTime() const {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->active_ ? impl_->storage_->clock() : impl_->latest_;
}
bool PgCatalogTransaction::active() const {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->active_;
}
Status PgCatalogTransaction::ValidateReadTime(HybridTime time) const {
  std::lock_guard lock(impl_->storage_->mutex());
  RETURN_NOT_OK(Status(impl_->failure_));
  SCHECK(
      time >= impl_->latest_ && time <= ReadTime(), NotSupported,
      "Read time is outside the active local snapshot");
  return Status::OK();
}
Status PgCatalogTransaction::CheckHealthy() const {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->CheckHealthy();
}
Status PgCatalogTransaction::Begin() {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->Begin();
}
Status PgCatalogTransaction::Apply(
    const PgsqlWriteRequestMsg& request, const docdb::DocReadContextPtr& context,
    PgsqlResponseMsg* response, rpc::Sidecars* sidecars, uint32_t subtransaction) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->StatementStatus(
      impl_->Apply(request, context, response, sidecars, subtransaction), subtransaction);
}
Status PgCatalogTransaction::Commit() {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->Commit();
}
void PgCatalogTransaction::Abort() {
  std::lock_guard lock(impl_->storage_->mutex());
  impl_->Abort();
}
Status PgCatalogTransaction::RollbackTo(uint32_t subtransaction) {
  std::lock_guard lock(impl_->storage_->mutex());
  return impl_->StatementStatus(impl_->RollbackTo(subtransaction), subtransaction);
}

Result<TransactionId> PgCatalogTransaction::BeginWithNative(uint64_t priority) {
  std::lock_guard lock(impl_->storage_->mutex());
  RETURN_NOT_OK(impl_->Begin());
  auto txn = impl_->native_->BeginCatalog(priority);
  if (!txn.ok()) {
    impl_->Abort();
    return txn.status();
  }
  impl_->native_txn_ = *txn;
  return *txn;
}
PgLocalTxnStore& PgCatalogTransaction::NativeTransactions() { return *impl_->native_; }

}  // namespace yb::local_runtime
