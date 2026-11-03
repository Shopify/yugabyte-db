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

#include <algorithm>
#include <charconv>
#include <deque>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "yb/common/pgsql_protocol.messages.h"
#include "yb/common/row_mark.h"
#include "yb/common/schema_pbutil.h"
#include "yb/common/transaction_error.h"
#include "yb/docdb/conflict_resolution.h"
#include "yb/docdb/doc_read_context.h"
#include "yb/docdb/doc_write_batch.h"
#include "yb/docdb/docdb.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/docdb/pgsql_operation.h"
#include "yb/docdb/ql_rocksdb_storage.h"
#include "yb/docdb/rocksdb_writer.h"
#include "yb/docdb/shared_lock_manager.h"
#include "yb/dockv/doc_key.h"
#include "yb/dockv/partition.h"
#include "yb/rocksdb/db.h"
#include "yb/rocksdb/iterator.h"
#include "yb/rpc/sidecars.h"
#include "yb/tablet/tablet_metrics.h"
#include "yb/tablet/tablet_options.h"
#include "yb/tools/local_runtime/column_family_view.h"
#include "yb/tools/local_runtime/pg_local_metadata.pb.h"
#include "yb/tools/local_runtime/pg_local_request_validation.h"
#include "yb/tools/local_runtime/pg_local_storage.h"
#include "yb/util/env.h"
#include "yb/util/flags.h"
#include "yb/util/sync_point.h"

DEFINE_NON_RUNTIME_bool(local_runtime_cache_bindings, true,
    "Reuse native table bindings until the catalog metadata changes.");

namespace yb::local_runtime {
namespace {

constexpr auto kFormat = "local-native-intents/2";
constexpr size_t kMaxTransactions = 65536;
constexpr size_t kMaxActive = 64;
constexpr size_t kMaxResidentBytes = 16 * 1024 * 1024;
constexpr size_t kMaxStandaloneIntentBytes = 16 * 1024 * 1024;
constexpr size_t kMaxAttachedIntentBytes = 512 * 1024 * 1024;
const TabletId kStatusLocation = "local-native-intents";

std::string MetaKey(const std::string& name) {
  return dockv::DocKey(dockv::KeyEntryValues{dockv::KeyEntryValue(name)})
             .Encode()
             .ToStringBuffer() +
         dockv::KeyEntryValue(DocHybridTime(HybridTime::kInitial, 0)).ToKeyBytes().ToStringBuffer();
}

// Feed the native intent encoder into a CF-aware, WAL-backed atomic WriteBatch. The native
// DirectWriter path writes straight to a memtable; that is not a substitute for this WAL batch.
class IntentBatch : public rocksdb::DirectWriteHandler {
 public:
  IntentBatch(rocksdb::WriteBatch* batch, rocksdb::ColumnFamilyHandle* family)
      : batch_(batch), family_(family) {}

  std::pair<Slice, Slice> Put(const SliceParts& key, const SliceParts& value) override {
    auto& entry = entries_.emplace_back(Join(key), Join(value));
    batch_->Put(family_, entry.first, entry.second);
    return {entry.first, entry.second};
  }

  void SingleDelete(const Slice& key) override { batch_->Delete(family_, key); }

 private:
  static std::string Join(const SliceParts& parts) {
    std::string result;
    for (int i = 0; i < parts.num_parts; ++i) {
      result.append(parts.parts[i].cdata(), parts.parts[i].size());
    }
    return result;
  }
  rocksdb::WriteBatch* batch_;
  rocksdb::ColumnFamilyHandle* family_;
  std::deque<std::pair<std::string, std::string>> entries_;
};

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
const std::unordered_set<std::string> kReadFields = {
    "client",
    "stmt_id",
    "table_id",
    "schema_version",
    "hash_code",
    "max_hash_code",
    "partition_column_values",
    "range_column_values",
    "column_refs",
    "col_refs",
    "targets",
    "is_forward_scan",
    "lower_bound",
    "upper_bound",
    "limit",
    "paging_state",
    "return_paging_state"};

}  // namespace

// All manager callbacks are inline, and all operations hold mutex_. In particular no iterator,
// conflict resolver, arena or RequestScope may escape an operation. Transactions can interleave
// between operations; their native intents are shared, unlike the SQL prototype's private overlay.
class PgLocalTxnStore::Impl : public TransactionStatusManager {
 public:
  Impl(
      std::shared_ptr<PgLocalStorage> storage, rocksdb::ColumnFamilyHandle* metadata,
      const void* owner = nullptr, TableLookup lookup = {},
      std::function<Status(uint32_t)> statement_start = {},
      std::function<size_t()> staged_bytes = {},
      std::function<uint64_t()> catalog_generation = {})
      : storage_(std::move(storage)),
        mutex_(storage_->mutex()),
        db_(storage_->db()),
        handles_(storage_->families()),
        metadata_(metadata),
        owner_(owner),
        lookup_(std::move(lookup)),
        statement_start_(std::move(statement_start)),
        staged_bytes_(std::move(staged_bytes)),
        catalog_generation_(std::move(catalog_generation)),
        clock_(storage_->clock()),
        failure_(storage_->failure()) {}

  struct Table {
    rocksdb::ColumnFamilyHandle* intents;
    std::unique_ptr<ColumnFamilyView> regular_view;
    std::unique_ptr<ColumnFamilyView> intents_view;
    docdb::DocReadContextPtr context;
    dockv::PartitionSchema partition_schema;
    std::vector<dockv::Partition> partitions;

    std::string descriptor;
    uint32_t schema_version = 0;
    // Catalog metadata generation at which Find last confirmed descriptor and schema_version.
    uint64_t generation = std::numeric_limits<uint64_t>::max();
    docdb::DocDB DB() const {
      return {
          .regular = regular_view.get(),
          .intents = intents_view.get(),
          .key_bounds = &docdb::KeyBounds::kNoBounds};
    }
  };
  struct Txn {
    LocalTransactionPB pb;
    TransactionMetadata metadata;
    SubTransactionMetadata subtransaction;
    std::unordered_set<TableId> touched;
    bool catalog_owned = false;
    uint32_t current_subtransaction = 1;
  };

  Status Initialize() {
    std::string value;
    auto status = db_->Get(rocksdb::ReadOptions(), metadata_, MetaKey("format"), &value);
    if (!status.IsNotFound()) RETURN_NOT_OK(status);
    if (owner_) {
      std::string initialized;
      const auto marker = db_->Get(
          rocksdb::ReadOptions(), storage_->metadata(), MetaKey("native-initialized"),
          &initialized);
      if (!marker.IsNotFound()) RETURN_NOT_OK(marker);
      SCHECK(
          (marker.IsNotFound() && status.IsNotFound()) ||
              (marker.ok() && initialized == "1" && status.ok()),
          Corruption, "Native metadata initialization marker mismatch");
    }
    if (status.IsNotFound()) {
      std::unique_ptr<rocksdb::Iterator> check(db_->NewIterator(rocksdb::ReadOptions(), metadata_));
      check->SeekToFirst();
      RETURN_NOT_OK(check->status());
      SCHECK(!check->Valid(), Corruption, "Native metadata has no format");
      rocksdb::WriteBatch batch;
      batch.Put(metadata_, MetaKey("format"), kFormat);
      if (owner_) batch.Put(storage_->metadata(), MetaKey("native-initialized"), "1");
      RETURN_NOT_OK(Persist(&batch, true));
    } else {
      RETURN_NOT_OK(status);
      SCHECK_EQ(value, kFormat, NotSupported, "Not a native local transaction store");
    }
    std::unordered_map<TableId, SchemaPB> schemas;
    rocksdb::WriteBatch recovery;
    std::unique_ptr<rocksdb::Iterator> iterator(
        db_->NewIterator(rocksdb::ReadOptions(), metadata_));
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      dockv::DocKey key;
      auto bytes = iterator->key();
      RETURN_NOT_OK(key.DecodeFrom(&bytes));
      SCHECK(
          key.range_group().size() == 1 &&
              key.range_group()[0].type() == dockv::KeyEntryType::kString,
          Corruption, "Invalid transaction-store metadata key");
      const auto& name = key.range_group()[0].GetString();
      if (name.starts_with("table/")) {
        SCHECK(!owner_, Corruption, "Attached tables must use catalog descriptors");
        SCHECK(
            schemas[name.substr(6)].ParseFromString(iterator->value().ToBuffer()), Corruption,
            "Invalid stored table schema");
      } else if (name.starts_with("binding/")) {
        SCHECK(
            owner_ && name.size() > 8 && iterator->value() == "1", Corruption,
            "Invalid native catalog binding");
        bindings_.insert(name.substr(8));
      } else if (name.starts_with("tx/")) {
        Txn txn;
        SCHECK(
            txn.pb.ParseFromString(iterator->value().ToBuffer()), Corruption,
            "Invalid stored transaction");
        txn.metadata = VERIFY_RESULT(TransactionMetadata::FromPB(txn.pb.metadata()));
        txn.subtransaction = VERIFY_RESULT(SubTransactionMetadata::FromPB(txn.pb.subtransaction()));
        const auto& id = txn.metadata.transaction_id;
        SCHECK(
            !id.IsNil() && name == "tx/" + id.ToString() &&
                txn.metadata.isolation == IsolationLevel::SNAPSHOT_ISOLATION &&
                txn.metadata.status_tablet == kStatusLocation &&
                !HybridTime(txn.pb.read_ht()).is_special() &&
                txn.pb.read_ht() <= clock_.ToUint64() &&
                txn.metadata.start_time.ToUint64() == txn.pb.read_ht() &&
                txn.pb.intent_bytes() <= MaxIntentBytes() && txn.pb.rollbacks() <= 1024 &&
                txn.subtransaction.subtransaction_id > 0 &&
                txn.pb.failed_subtransaction() <= txn.subtransaction.subtransaction_id &&
                (txn.pb.status() == TransactionStatus::PENDING ||
                 txn.pb.status() == TransactionStatus::ABORTED ||
                 (txn.pb.status() == TransactionStatus::COMMITTED &&
                  !txn.pb.failed_subtransaction() && txn.pb.commit_ht() > txn.pb.read_ht() &&
                  txn.pb.commit_ht() <= storage_->committed().ToUint64())),
            Corruption, "Invalid transaction identity or state");
        if (txn.pb.status() == TransactionStatus::PENDING) {
          // No transaction resumes across a process restart. Committed intents remain readable
          // using their durable status; unknown transactions must never be treated as aborted.
          txn.pb.set_status(TransactionStatus::ABORTED);
          recovery.Put(metadata_, MetaKey(name), txn.pb.SerializeAsString());
        }
        SCHECK(txns_.emplace(id, std::move(txn)).second, Corruption, "Duplicate transaction");
      } else {
        SCHECK(name == "format", Corruption, "Unknown transaction metadata");
      }
    }
    RETURN_NOT_OK(iterator->status());
    iterator.reset();
    SCHECK_LE(txns_.size(), kMaxTransactions, Corruption, "Too many persisted transactions");
    std::vector<std::string> orphaned;
    for (const auto& [name, handle] : handles_) {
      if (owner_) continue;
      if (name == rocksdb::kDefaultColumnFamilyName || name == "local-metadata") continue;
      SCHECK(
          name.size() > 2 && (name.starts_with("r/") || name.starts_with("i/")), Corruption,
          "Unknown column family");
      if (!schemas.contains(name.substr(2))) orphaned.push_back(name);
    }
    for (const auto& name : orphaned) RETURN_NOT_OK(storage_->DropFamily(name));
    for (const auto& [id, schema] : schemas) RETURN_NOT_OK(AddTable(id, schema));
    if (recovery.Count()) RETURN_NOT_OK(Persist(&recovery, true));
    return RefreshCatalogBindings(true);
  }

  Status Persist(
      rocksdb::WriteBatch* batch, bool sync, bool commit = false,
      HybridTime commit_time = HybridTime::kInvalid) {
    RETURN_NOT_OK(Status(failure_));
    failure_ = storage_->Write(batch, sync, commit_time);
    if (commit)
      TEST_SYNC_POINT_CALLBACK("PgLocalTxnStore::CommitStatus", &failure_);
    else
      TEST_SYNC_POINT_CALLBACK("PgLocalTxnStore::WriteStatus", &failure_);
    return failure_;
  }

  Result<HybridTime> Tick() { return storage_->Tick(); }

  Status RefreshCatalogBindings(bool recovery = false) {
    RETURN_NOT_OK(Status(failure_));
    if (!owner_ || (!recovery && bindings_.empty())) return Status::OK();
    tables_.clear();
    std::vector<TableId> removed;
    rocksdb::WriteBatch batch;
    for (const auto& id : bindings_) {
      auto table = lookup_(id);
      if (!table.ok()) {
        if (!table.status().IsNotFound()) return table.status();
        batch.Delete(metadata_, MetaKey("binding/" + id));
        removed.push_back(id);
      } else {
        SCHECK(
            handles_.contains("intents/" + id), Corruption,
            "Published native binding is missing its intents family");
      }
    }
    for (const auto& id : removed) {
      if (handles_.contains("intents/" + id)) {
        RETURN_NOT_OK(storage_->DropFamily("intents/" + id));
        TEST_SYNC_POINT("PgLocalTxnStore::BindingDropped");
      }
    }
    if (batch.Count()) RETURN_NOT_OK(Persist(&batch, true));
    for (const auto& id : removed) bindings_.erase(id);
    if (!removed.empty()) {
      // Recreating a rolled-back table needs native transaction metadata in the new CF too.
      for (auto& [transaction_id, txn] : txns_) {
        if (txn.pb.status() != TransactionStatus::PENDING) continue;
        for (const auto& id : removed) txn.touched.erase(id);
      }
    }
    if (recovery) {
      std::vector<std::string> orphaned;
      for (const auto& [name, handle] : handles_) {
        if (name.starts_with("intents/") && !bindings_.contains(name.substr(8)))
          orphaned.push_back(name);
      }
      for (const auto& name : orphaned) {
        std::unique_ptr<rocksdb::Iterator> iterator(
            db_->NewIterator(rocksdb::ReadOptions(), handles_.at(name).get()));
        iterator->SeekToFirst();
        RETURN_NOT_OK(iterator->status());
        SCHECK(!iterator->Valid(), Corruption, "Unpublished native binding contains intents");
        iterator.reset();
        RETURN_NOT_OK(storage_->DropFamily(name));
      }
    }
    return Status::OK();
  }

  Status AddTable(const TableId& id, const SchemaPB& metadata) {
    Schema schema;
    RETURN_NOT_OK(SchemaFromPB(metadata, &schema));
    SCHECK(
        !id.empty() && schema.has_column_ids() && schema.num_hash_key_columns() > 0 &&
            !schema.has_colocation_id() && schema.cotable_id().IsNil() &&
            metadata.table_properties().num_tablets() > 0 &&
            metadata.table_properties().num_tablets() <= 65536,
        InvalidArgument, "Expected a non-colocated HASH table with logical partitions");
    for (const auto& column : schema.columns()) {
      SCHECK(
          column.type()->main() == DataType::INT64 || column.type()->main() == DataType::INT32 ||
              column.type()->main() == DataType::STRING,
          NotSupported, "Unsupported column type");
    }
    const auto regular = (owner_ ? "table/" : "r/") + id;
    const auto intents = (owner_ ? "intents/" : "i/") + id;
    SCHECK(handles_.contains(regular), Corruption, "Published table is missing a column family");
    if (owner_ && !bindings_.contains(id)) {
      if (!handles_.contains(intents)) {
        VERIFY_RESULT(storage_->CreateFamily(intents));
        TEST_SYNC_POINT("PgLocalTxnStore::BindingFamilyCreated");
      }
      rocksdb::WriteBatch batch;
      batch.Put(metadata_, MetaKey("binding/" + id), "1");
      RETURN_NOT_OK(Persist(&batch, true));
      bindings_.insert(id);
    }
    SCHECK(handles_.contains(intents), Corruption, "Published table is missing an intents family");
    auto table = std::make_unique<Table>();
    table->intents = handles_.at(intents).get();
    table->regular_view = std::make_unique<ColumnFamilyView>(db_, handles_.at(regular).get());
    table->intents_view = std::make_unique<ColumnFamilyView>(db_, table->intents);
    table->context = std::make_shared<docdb::DocReadContext>(
        id, TableType::PGSQL_TABLE_TYPE, docdb::Index::kFalse, packings_, schema, 0);
    PartitionSchemaPB pb;
    pb.set_hash_schema(PartitionSchemaPB::PGSQL_HASH_SCHEMA);
    RETURN_NOT_OK(dockv::PartitionSchema::FromPB(pb, schema, &table->partition_schema));
    RETURN_NOT_OK(table->partition_schema.CreatePartitions(
        metadata.table_properties().num_tablets(), &table->partitions));
    tables_.emplace(id, std::move(table));
    return Status::OK();
  }

  Status AddAttachedTable(const TableId& id, const tablet::TableInfoPB& metadata) {
    SCHECK_EQ(metadata.table_id(), id, Corruption, "Native binding identity mismatch");
    SCHECK_EQ(
        metadata.table_type(), TableType::PGSQL_TABLE_TYPE, NotSupported, "Expected a YSQL table");
    auto table = std::make_unique<Table>();
    auto context = std::make_shared<docdb::DocReadContext>(
        id, TableType::PGSQL_TABLE_TYPE, docdb::Index(metadata.has_index_info()), packings_);
    RETURN_NOT_OK(context->LoadFromPB(metadata));
    table->context = std::move(context);
    const auto& schema = table->context->schema();
    const auto tablets = metadata.schema().table_properties().num_tablets();
    SCHECK(
        !id.empty() && schema.has_column_ids() && schema.num_key_columns() > 0 &&
            !schema.has_colocation_id() && schema.cotable_id().IsNil() &&
            !schema.table_properties().is_ysql_catalog_table() && tablets > 0 && tablets <= 65536,
        NotSupported, "Expected a non-colocated application table with logical partitions");
    RETURN_NOT_OK(
        dockv::PartitionSchema::FromPB(
            metadata.partition_schema(), schema, &table->partition_schema));
    if (schema.num_hash_key_columns() > 0) {
      SCHECK_EQ(
          metadata.partition_schema().hash_schema(), PartitionSchemaPB::PGSQL_HASH_SCHEMA,
          NotSupported, "Expected YSQL HASH partitioning");
      RETURN_NOT_OK(table->partition_schema.CreatePartitions(tablets, &table->partitions));
    } else {
      SCHECK_EQ(tablets, 1, NotSupported, "RANGE tables require a single partition");
      table->partitions.emplace_back();
    }
    const auto regular = "table/" + id;
    const auto intents = "intents/" + id;
    SCHECK(handles_.contains(regular), Corruption, "Published table is missing a column family");
    if (!bindings_.contains(id)) {
      if (!handles_.contains(intents)) {
        VERIFY_RESULT(storage_->CreateFamily(intents));
        TEST_SYNC_POINT("PgLocalTxnStore::BindingFamilyCreated");
      }
      rocksdb::WriteBatch batch;
      batch.Put(metadata_, MetaKey("binding/" + id), "1");
      RETURN_NOT_OK(Persist(&batch, true));
      bindings_.insert(id);
    }
    SCHECK(
        handles_.contains(intents), Corruption,
        "Published native binding is missing its intents family");
    table->intents = handles_.at(intents).get();
    table->regular_view = std::make_unique<ColumnFamilyView>(db_, handles_.at(regular).get());
    table->intents_view = std::make_unique<ColumnFamilyView>(db_, table->intents);
    table->descriptor = metadata.SerializeAsString();
    tables_.insert_or_assign(id, std::move(table));
    return Status::OK();
  }

  Status CreateTable(const TableId& id, const Schema& schema, int32_t tablets) {
    std::lock_guard lock(mutex_);
    RETURN_NOT_OK(Status(failure_));
    SCHECK(!owner_, NotSupported, "Attached tables must be created by the catalog transaction");
    SCHECK(!tables_.contains(id), AlreadyPresent, "Table already exists");
    SCHECK(ActiveCount() == 0, NotSupported, "Table creation requires no active transactions");
    SCHECK(
        !id.empty() && schema.has_column_ids() && schema.num_hash_key_columns() > 0 &&
            !schema.has_colocation_id() && schema.cotable_id().IsNil() && tablets > 0 &&
            tablets <= 65536,
        InvalidArgument, "Invalid immutable HASH table");
    for (const auto& column : schema.columns()) {
      SCHECK(
          column.type()->main() == DataType::INT64 || column.type()->main() == DataType::INT32 ||
              column.type()->main() == DataType::STRING,
          NotSupported, "Unsupported column type");
    }
    SchemaPB pb;
    SchemaToPB(schema, &pb);
    pb.mutable_table_properties()->set_num_tablets(tablets);
    for (const auto& prefix : {"r/", "i/"}) {
      const auto name = prefix + id;
      VERIFY_RESULT(storage_->CreateFamily(name));
      TEST_SYNC_POINT("PgLocalTxnStore::FamilyCreated");
    }
    rocksdb::WriteBatch batch;
    batch.Put(metadata_, MetaKey("table/" + id), pb.SerializeAsString());
    RETURN_NOT_OK(Persist(&batch, true));
    TEST_SYNC_POINT("PgLocalTxnStore::SchemaPersisted");
    failure_ = AddTable(id, pb);
    return failure_;
  }

  size_t ActiveCount() const { return storage_->active_data(); }

  size_t MaxIntentBytes() const {
    return owner_ ? kMaxAttachedIntentBytes : kMaxStandaloneIntentBytes;
  }

  Result<Txn*> Active(const TransactionId& id, bool allow_failed = false) {
    RETURN_NOT_OK(Status(failure_));
    const auto it = txns_.find(id);
    SCHECK(it != txns_.end(), NotFound, "Unknown transaction");
    if (it->second.pb.status() != TransactionStatus::PENDING) {
      // Conflict arbitration may abort another backend's transaction between its operations.
      return STATUS(
          Aborted, "Transaction is no longer pending",
          TransactionError(TransactionErrorCode::kAborted));
    }
    SCHECK(
        allow_failed || !it->second.pb.failed_subtransaction(), Aborted,
        "Statement failed; rollback required");
    return &it->second;
  }

  Result<Table*> Find(const TableId& id) {
    if (owner_) {
      const auto generation = catalog_generation_();
      const auto cached = tables_.find(id);
      if (FLAGS_local_runtime_cache_bindings && cached != tables_.end() &&
          cached->second->generation == generation)
        return cached->second.get();
      const auto metadata = VERIFY_RESULT(lookup_(id));
      // Revalidate even cached bindings: ALTER and index creation can change the descriptor
      // without changing the schema itself. Never fall back to a regular-only view.
      SCHECK(handles_.contains("table/" + id), Corruption, "Missing native regular family");
      SCHECK(
          !bindings_.contains(id) || handles_.contains("intents/" + id), Corruption,
          "Published native binding is missing its intents family");
      if (cached == tables_.end() || cached->second->descriptor != metadata.SerializeAsString())
        RETURN_NOT_OK(AddAttachedTable(id, metadata));
      auto& table = *tables_.at(id);
      table.schema_version = metadata.schema_version();
      table.generation = generation;
      return &table;
    }
    const auto it = tables_.find(id);
    SCHECK(it != tables_.end(), NotFound, "Unknown transaction-store table");
    return it->second.get();
  }

  Result<TransactionId> Begin(
      uint64_t priority, bool catalog_owned = false) {
    std::lock_guard lock(mutex_);
    RETURN_NOT_OK(Status(failure_));
    SCHECK(
        catalog_owned ? owner_ && storage_->catalog_owner() == owner_
                      : storage_->catalog_owner() == nullptr,
        NotSupported, "Catalog changes require exclusive transaction ownership");
    SCHECK_LT(txns_.size(), kMaxTransactions, NotSupported, "Transaction history limit reached");
    SCHECK_LT(ActiveCount(), kMaxActive, NotSupported, "Active transaction limit reached");
    Txn txn;
    txn.catalog_owned = catalog_owned;
    txn.metadata.transaction_id = TransactionId::GenerateRandom();
    txn.metadata.isolation = IsolationLevel::SNAPSHOT_ISOLATION;
    txn.metadata.priority = priority;
    txn.metadata.status_tablet = kStatusLocation;
    txn.metadata.start_time = VERIFY_RESULT(Tick());
    txn.metadata.pg_txn_start_us =
        static_cast<int64_t>(txn.metadata.start_time.GetPhysicalValueMicros());
    txn.metadata.ToPB(txn.pb.mutable_metadata());
    txn.pb.set_status(TransactionStatus::PENDING);
    txn.pb.set_read_ht(txn.metadata.start_time.ToUint64());
    txn.subtransaction.ToPB(txn.pb.mutable_subtransaction());
    rocksdb::WriteBatch batch;
    const auto id = txn.metadata.transaction_id;
    batch.Put(metadata_, MetaKey("tx/" + id.ToString()), txn.pb.SerializeAsString());
    RETURN_NOT_OK(Persist(&batch, false));
    txns_.emplace(id, std::move(txn));
    storage_->DataStarted();
    return id;
  }

  // A SQL backend's read point is its statement or transaction snapshot, as PgGate selects; the
  // transaction may begin after it. No own write precedes the read point of a transaction that
  // began after it, so the own-write limit never needs to.
  ReadHybridTime ReadTime(
      const Txn& txn, HybridTime own_limit, HybridTime snapshot = HybridTime::kInvalid) const {
    const auto read = snapshot.is_valid() ? snapshot : HybridTime(txn.pb.read_ht());
    auto result = ReadHybridTime::SingleTime(read);
    result.in_txn_limit = own_limit.is_valid() ? std::max(own_limit, read) : clock_;
    return result;
  }

  Result<ReadHybridTime> ReadPoint(const TransactionId& id) {
    std::lock_guard lock(mutex_);
    return ReadTime(*VERIFY_RESULT(Active(id)), clock_);
  }

  Status Save(Txn* txn, LocalTransactionPB next, bool sync, bool commit = false) {
    rocksdb::WriteBatch batch;
    batch.Put(
        metadata_, MetaKey("tx/" + txn->metadata.transaction_id.ToString()),
        next.SerializeAsString());
    RETURN_NOT_OK(Persist(
        &batch, sync, commit, commit ? HybridTime(next.commit_ht()) : HybridTime::kInvalid));
    if (txn->pb.status() == TransactionStatus::PENDING &&
        next.status() != TransactionStatus::PENDING) {
      storage_->DataFinished();
    }
    txn->pb = std::move(next);
    txn->subtransaction = VERIFY_RESULT(SubTransactionMetadata::FromPB(txn->pb.subtransaction()));
    return Status::OK();
  }

  Status StartWrite(Txn* txn, uint32_t subtransaction) {
    SCHECK(
        subtransaction > 0 &&
            (txn->catalog_owned || subtransaction >= txn->subtransaction.subtransaction_id) &&
            !txn->subtransaction.aborted.Test(subtransaction),
        InvalidArgument, "Subtransaction IDs must advance after rollback");
    if (txn->catalog_owned) RETURN_NOT_OK(statement_start_(subtransaction));
    txn->current_subtransaction = subtransaction;
    txn->subtransaction.subtransaction_id =
        std::max(txn->subtransaction.subtransaction_id, subtransaction);
    return Status::OK();
  }

  Status FinishStatement(Txn* txn, Status status, const PgsqlResponseMsg& response) {
    // SKIP LOCKED reports a locked row with kSkipLocking; PgGate skips it and continues.
    const bool skipped =
        TransactionError::ValueFromStatus(status) == TransactionErrorCode::kSkipLocking;
    if ((!status.ok() || response.status() != PgsqlResponsePB::PGSQL_STATUS_OK) && failure_.ok() &&
        !skipped && txn->pb.status() == TransactionStatus::PENDING) {
      auto next = txn->pb;
      next.set_failed_subtransaction(txn->current_subtransaction);
      next.clear_subtransaction();
      txn->subtransaction.ToPB(next.mutable_subtransaction());
      RETURN_NOT_OK(Save(txn, std::move(next), false));
    }
    return status;
  }

  Status Write(
      const TransactionId& id, const PgsqlWriteRequestPB& request, PgsqlResponsePB* response,
      uint32_t subtransaction) {
    std::lock_guard lock(mutex_);
    SCHECK(response, InvalidArgument, "Missing write response");
    response->Clear();
    auto* txn = VERIFY_RESULT(Active(id));
    RETURN_NOT_OK(StartWrite(txn, subtransaction));
    const auto status = DoWrite(txn, request, response);
    ThreadSafeArena arena;
    LWPgsqlResponsePB result(&arena);
    result.CopyFrom(*response);
    return FinishStatement(txn, status, result);
  }

  Status WriteSql(
      const TransactionId& id, const PgsqlWriteRequestMsg& request, PgsqlResponseMsg* response,
      rpc::Sidecars* sidecars, uint32_t subtransaction, HybridTime snapshot) {
    std::lock_guard lock(mutex_);
    SCHECK(response && sidecars, InvalidArgument, "Missing SQL response or sidecars");
    response->Clear();
    auto* txn = VERIFY_RESULT(Active(id));
    SCHECK(owner_, NotSupported, "SQL execution requires authoritative catalog descriptors");
    RETURN_NOT_OK(StartWrite(txn, subtransaction));
    return FinishStatement(
        txn, DoWriteSql(txn, request, response, sidecars, snapshot), *response);
  }

  Status DoWrite(Txn* txn, const PgsqlWriteRequestPB& request, PgsqlResponsePB* response) {
    RETURN_NOT_OK(detail::CheckFields(request, kWriteFields));
    auto* table = VERIFY_RESULT(Find(request.table_id()));
    const auto& schema = table->context->schema();
    SCHECK(
        request.client() == YQL_CLIENT_PGSQL && request.schema_version() == 0, InvalidArgument,
        "Expected native YSQL schema version zero");
    SCHECK(
        request.stmt_type() == PgsqlWriteRequestPB::PGSQL_INSERT ||
            request.stmt_type() == PgsqlWriteRequestPB::PGSQL_UPDATE ||
            request.stmt_type() == PgsqlWriteRequestPB::PGSQL_DELETE,
        NotSupported, "Unsupported transactional write");
    VERIFY_RESULT(detail::CheckReferences(schema, request));
    RETURN_NOT_OK(
        detail::CheckKeyValues(
            schema, request.partition_column_values(), 0, schema.num_hash_key_columns()));
    RETURN_NOT_OK(
        detail::CheckKeyValues(
            schema, request.range_column_values(), schema.num_hash_key_columns(),
            schema.num_range_key_columns()));
    RETURN_NOT_OK(detail::CheckColumns(schema, request.column_values()));
    RETURN_NOT_OK(detail::CheckColumns(schema, request.column_new_values()));
    SCHECK(
        (request.stmt_type() == PgsqlWriteRequestPB::PGSQL_INSERT ||
         request.column_values().empty()) &&
            (request.stmt_type() == PgsqlWriteRequestPB::PGSQL_UPDATE ||
             request.column_new_values().empty()),
        InvalidArgument, "Incorrect write column list");
    auto normalized = request;
    const auto hash = VERIFY_RESULT(
        table->partition_schema.PgsqlHashColumnCompoundValue(request.partition_column_values()));
    SCHECK(
        !request.has_hash_code() || request.hash_code() == hash, InvalidArgument,
        "Incorrect hash code");
    normalized.set_hash_code(hash);
    ThreadSafeArena arena;
    LWPgsqlWriteRequestPB req(&arena);
    req.CopyFrom(normalized);
    LWPgsqlResponsePB resp(&arena);
    rpc::Sidecars sidecars;
    const auto status = ExecuteWrite(txn, table, req, &resp, &sidecars, HybridTime::kInvalid);
    *response = resp.ToGoogleProtobuf();
    return status;
  }

  Status DoWriteSql(
      Txn* txn, const PgsqlWriteRequestMsg& request, PgsqlResponseMsg* response,
      rpc::Sidecars* sidecars, HybridTime snapshot) {
    SCHECK_EQ(request.client(), YQL_CLIENT_PGSQL, InvalidArgument, "Expected a YSQL request");
    SCHECK(
        request.stmt_type() == PgsqlWriteRequestPB::PGSQL_INSERT ||
            request.stmt_type() == PgsqlWriteRequestPB::PGSQL_UPDATE ||
            request.stmt_type() == PgsqlWriteRequestPB::PGSQL_DELETE ||
            request.stmt_type() == PgsqlWriteRequestPB::PGSQL_UPSERT,
        NotSupported, "Unsupported native SQL write operation");
    SCHECK(
        !request.is_backfill() && !request.is_delete_persist_needed() &&
            !request.skip_intents_write() && !request.read_at_in_txn_limit() &&
            !request.is_ysql_catalog_change_using_protobuf() &&
            !request.force_catalog_modifications() && !request.has_fetch_sequence_params() &&
            request.ignore_after_hybrid_time() == 0 && request.batch_arguments().empty() &&
            request.metrics_capture() == PGSQL_METRICS_CAPTURE_NONE,
        NotSupported, "Unsupported native SQL write mode");
    SCHECK_EQ(request.packed_rows().size() % 2, 0, InvalidArgument, "Unpaired packed rows");
    auto* table = VERIFY_RESULT(Find(request.table_id().ToBuffer()));
    SCHECK_EQ(
        request.schema_version(), table->schema_version, InvalidArgument,
        "Native SQL schema version mismatch");
    ThreadSafeArena arena;
    LWPgsqlWriteRequestPB normalized(&arena);
    normalized.CopyFrom(request);
    if (request.packed_rows().empty()) {
      RETURN_NOT_OK(
          dockv::InitPartitionKey(table->context->schema(), table->partition_schema, &normalized));
    }
    return ExecuteWrite(txn, table, normalized, response, sidecars, snapshot);
  }

  Status ExecuteWrite(
      Txn* txn, Table* table, const PgsqlWriteRequestMsg& request, PgsqlResponseMsg* response,
      rpc::Sidecars* sidecars, HybridTime snapshot) {
    ThreadSafeArena arena;
    auto sub = txn->subtransaction;
    sub.subtransaction_id = txn->current_subtransaction;
    TransactionOperationContext context(txn->metadata.transaction_id, std::move(sub), this);
    docdb::DocOperations operations;
    auto operation =
        std::make_unique<docdb::PgsqlWriteOperation>(request, table->context, context, sidecars);
    RETURN_NOT_OK(operation->Init(response));
    operations.push_back(std::move(operation));
    const auto write_time = VERIFY_RESULT(Tick());
    docdb::LWKeyValueWriteBatchPB pairs(&arena);
    txn->metadata.ToPB(pairs.mutable_transaction());
    pairs.mutable_subtransaction()->set_subtransaction_id(txn->current_subtransaction);
    txn->subtransaction.aborted.ToPB(
        pairs.mutable_subtransaction()->mutable_aborted()->mutable_set());
    auto locks = VERIFY_RESULT(
        docdb::PrepareDocWriteOperation(
            operations, pairs.read_pairs(), metrics_holder_, IsolationLevel::SNAPSHOT_ISOLATION,
            ROW_MARK_ABSENT, true, false, CoarseTimePoint::max(),
            dockv::PartialRangeKeyIntents::kTrue, &locks_));
    RequestScope scope;
    std::optional<Result<HybridTime>> resolution;
    RETURN_NOT_OK(
        docdb::ResolveTransactionConflicts(
            operations, docdb::FAIL_ON_CONFLICT, pairs, scope, write_time,
            ReadTime(*txn, write_time, snapshot).read, txn->metadata.pg_txn_start_us,
            Env::Default()->NowMicros(), request_id_, table->DB(),
            dockv::PartialRangeKeyIntents::kTrue, this, metrics_holder_, &locks.lock_batch, nullptr,
            false, CoarseTimePoint::max(),
            [&resolution](const Result<HybridTime>& result) { resolution.emplace(result); },
            dockv::SkipPrefixLocks::kFalse));
    // FAIL_ON_CONFLICT plus this manager's inline callbacks cannot leave a resolver in flight.
    CHECK(resolution.has_value());
    RETURN_NOT_OK(ResultToStatus(*resolution));
    ScopedRWOperation pending(&operations_, CoarseTimePoint::max());
    RETURN_NOT_OK(pending);
    docdb::DocWriteBatch doc_batch(table->DB(), docdb::InitMarkerBehavior::kOptional, pending);
    ReadRestartData restart;
    docdb::DocOperationApplyData data{
        .doc_write_batch = &doc_batch,
        .read_operation_data =
            docdb::ReadOperationData::FromReadTime(ReadTime(*txn, write_time, snapshot)),
        .read_restart_data = &restart,
        .schema_packing_provider = nullptr,
    };
    RETURN_NOT_OK(operations.front()->Apply(data));
    SCHECK(!restart.is_valid(), NotSupported, "Native write requested a read restart");
    if (response->status() != PgsqlResponsePB::PGSQL_STATUS_OK) return Status::OK();
    doc_batch.MoveToWriteBatchPB(&pairs);
    return WriteIntents(txn, table, pairs, write_time, request.table_id().ToBuffer());
  }

  // Persists data or row-lock intents, prepared and conflict-checked by the caller.
  Status WriteIntents(
      Txn* txn, Table* table, const docdb::LWKeyValueWriteBatchPB& pairs, HybridTime write_time,
      const TableId& table_id) {
    SCHECK_LT(
        pairs.write_pairs().size() + pairs.read_pairs().size(),
        std::numeric_limits<IntraTxnWriteId>::max() - txn->pb.next_write_id(), NotSupported,
        "Transaction write IDs exhausted");
    rocksdb::WriteBatch batch;
    IntentBatch handler(&batch, table->intents);
    docdb::TransactionalWriter writer(
        pairs, write_time, txn->metadata.transaction_id, IsolationLevel::SNAPSHOT_ISOLATION,
        dockv::PartialRangeKeyIntents::kTrue, Slice(), txn->pb.next_write_id(), nullptr);
    if (!txn->touched.contains(table_id)) writer.SetMetadataToStore(&pairs.transaction());
    RETURN_NOT_OK(writer.Apply(handler));
    auto next = txn->pb;
    const auto batch_bytes = batch.GetDataSize();
    const auto catalog_bytes = staged_bytes_ ? staged_bytes_() : 0;
    // Earlier intents are already in RocksDB/WAL, not resident in the catalog overlay.
    SCHECK(
        catalog_bytes <= kMaxResidentBytes && batch_bytes <= kMaxResidentBytes - catalog_bytes,
        NotSupported, "Transaction exceeds native intent staging limit");
    const auto max_intent_bytes = MaxIntentBytes();
    SCHECK(
        next.intent_bytes() <= max_intent_bytes &&
            batch_bytes <= max_intent_bytes - next.intent_bytes(),
        NotSupported, "Transaction exceeds native durable intent limit");
    next.set_intent_bytes(next.intent_bytes() + batch_bytes);
    next.set_next_write_id(writer.intra_txn_write_id());
    next.clear_subtransaction();
    txn->subtransaction.ToPB(next.mutable_subtransaction());
    batch.Put(
        metadata_, MetaKey("tx/" + txn->metadata.transaction_id.ToString()),
        next.SerializeAsString());
    RETURN_NOT_OK(Persist(&batch, false));
    txn->pb = std::move(next);
    // Retained status lets other transactions resolve these intents, including lock-only ones.
    txn->touched.insert(table_id);
    return Status::OK();
  }

  // Mirrors a tserver row-marked read: lock intents come from docdb::GetIntents, are resolved like
  // writes, then persisted before the read executes. Committed lock-only intents stay in the
  // intents family, but conflict resolution treats a committed transaction's locks as released
  // and readers skip them. There is no wait queue, so WAIT_BLOCK fails like WAIT_ERROR.
  Status LockRows(
      Txn* txn, Table* table, const PgsqlReadRequestMsg& request, RowMarkType row_mark,
      HybridTime read_time) {
    ThreadSafeArena arena;
    docdb::LWKeyValueWriteBatchPB pairs(&arena);
    RETURN_NOT_OK(docdb::GetIntents(
        request, table->context->schema(), IsolationLevel::SNAPSHOT_ISOLATION, &pairs));
    if (pairs.read_pairs().empty()) return Status::OK();
    txn->metadata.ToPB(pairs.mutable_transaction());
    pairs.mutable_subtransaction()->set_subtransaction_id(txn->current_subtransaction);
    txn->subtransaction.aborted.ToPB(
        pairs.mutable_subtransaction()->mutable_aborted()->mutable_set());
    pairs.set_row_mark_type(row_mark);
    const auto write_time = VERIFY_RESULT(Tick());
    const docdb::DocOperations no_operations;
    auto locks = VERIFY_RESULT(
        docdb::PrepareDocWriteOperation(
            no_operations, pairs.read_pairs(), metrics_holder_,
            IsolationLevel::SNAPSHOT_ISOLATION, row_mark, true, false, CoarseTimePoint::max(),
            dockv::PartialRangeKeyIntents::kTrue, &locks_));
    const auto policy = pairs.has_wait_policy() && pairs.wait_policy() == WAIT_SKIP
                            ? docdb::SKIP_ON_CONFLICT
                            : docdb::FAIL_ON_CONFLICT;
    RequestScope scope;
    std::optional<Result<HybridTime>> resolution;
    RETURN_NOT_OK(
        docdb::ResolveTransactionConflicts(
            no_operations, policy, pairs, scope, write_time, read_time,
            txn->metadata.pg_txn_start_us, Env::Default()->NowMicros(), request_id_, table->DB(),
            dockv::PartialRangeKeyIntents::kTrue, this, metrics_holder_, &locks.lock_batch, nullptr,
            false, CoarseTimePoint::max(),
            [&resolution](const Result<HybridTime>& result) { resolution.emplace(result); },
            dockv::SkipPrefixLocks::kFalse));
    // Without a wait queue, this manager's inline callbacks cannot leave a resolver in flight.
    CHECK(resolution.has_value());
    RETURN_NOT_OK(ResultToStatus(*resolution));
    return WriteIntents(txn, table, pairs, write_time, request.table_id().ToBuffer());
  }


  Result<TransactionReadResult> Read(
      const TransactionId& id, const PgsqlReadRequestPB& request, size_t tablet,
      HybridTime own_limit) {
    std::lock_guard lock(mutex_);
    auto* txn = VERIFY_RESULT(Active(id));
    RETURN_NOT_OK(detail::CheckFields(request, kReadFields));
    auto* table = VERIFY_RESULT(Find(request.table_id()));
    const auto& schema = table->context->schema();
    SCHECK(
        request.client() == YQL_CLIENT_PGSQL && request.schema_version() == 0, InvalidArgument,
        "Expected native YSQL schema version zero");
    SCHECK_LT(tablet, table->partitions.size(), InvalidArgument, "Invalid logical partition");
    const auto refs = VERIFY_RESULT(detail::CheckReferences(schema, request));
    for (const auto& target : request.targets()) {
      RETURN_NOT_OK(detail::CheckFields(target, {"column_id"}));
      SCHECK(
          target.has_column_id() && refs.contains(target.column_id()), InvalidArgument,
          "Projection requires a referenced ordinary column");
    }
    if (!request.partition_column_values().empty()) {
      RETURN_NOT_OK(
          detail::CheckKeyValues(
              schema, request.partition_column_values(), 0, schema.num_hash_key_columns()));
      SCHECK_LE(
          static_cast<size_t>(request.range_column_values_size()), schema.num_range_key_columns(),
          InvalidArgument, "Too many range components");
      RETURN_NOT_OK(
          detail::CheckKeyValues(
              schema, request.range_column_values(), schema.num_hash_key_columns(),
              request.range_column_values_size()));
      const auto hash = VERIFY_RESULT(
          table->partition_schema.PgsqlHashColumnCompoundValue(request.partition_column_values()));
      SCHECK(
          !request.has_hash_code() || request.hash_code() == hash, InvalidArgument,
          "Hash code does not match canonical YSQL hash");
    } else {
      SCHECK(request.range_column_values().empty(), NotSupported, "Range values without HASH key");
    }
    SCHECK(
        (!request.has_hash_code() || request.hash_code() <= 65535) &&
            (!request.has_max_hash_code() || request.max_hash_code() <= 65535),
        InvalidArgument, "Hash bound exceeds uint16");
    RETURN_NOT_OK(detail::CheckFields(request.lower_bound(), {"key", "is_inclusive"}));
    RETURN_NOT_OK(detail::CheckFields(request.upper_bound(), {"key", "is_inclusive"}));
    const auto& page = request.paging_state();
    RETURN_NOT_OK(
        detail::CheckFields(
            page, {"table_id", "next_partition_key", "next_row_key", "total_num_rows_read",
                   "read_time", "next_tablet_bound"}));
    SCHECK(
        !page.has_table_id() || page.table_id() == request.table_id(), InvalidArgument,
        "Paging table mismatch");
    auto time = ReadTime(*txn, own_limit);
    if (request.paging_state().has_read_time()) {
      auto page_time = ReadHybridTime::FromPB(request.paging_state().read_time());
      SCHECK(
          page_time.read == time.read && page_time.global_limit == time.global_limit &&
              page_time.local_limit == time.local_limit &&
              (!own_limit.is_valid() || page_time.in_txn_limit == own_limit),
          InvalidArgument, "Paging snapshot mismatch");
      time = page_time;
    }
    SCHECK(
        !time.in_txn_limit.is_special() && time.in_txn_limit >= time.read &&
            time.in_txn_limit <= clock_,
        InvalidArgument, "Invalid own-write statement limit");
    auto normalized = request;
    RETURN_NOT_OK(dockv::InitPartitionKey(schema, table->partition_schema, &normalized));
    const auto& partition = table->partitions[tablet];
    const auto lower = VERIFY_RESULT(
        table->partition_schema.GetEncodedPartitionKey(partition.partition_key_start()));
    const auto upper = VERIFY_RESULT(
        table->partition_schema.GetEncodedPartitionKey(partition.partition_key_end()));
    docdb::KeyBounds bounds(lower, upper);
    auto db = table->DB();
    db.key_bounds = &bounds;
    docdb::QLRocksDBStorage storage(
        "native-intent-store", db, {.start_key = KeyBuffer(lower), .end_key = KeyBuffer(upper)});
    ThreadSafeArena arena;
    LWPgsqlReadRequestPB req(&arena);
    req.CopyFrom(normalized);
    LWPgsqlResponsePB response(&arena);
    TransactionOperationContext context(id, SubTransactionMetadata(txn->subtransaction), this);
    ScopedRWOperation pending(&operations_, CoarseTimePoint::max());
    RETURN_NOT_OK(pending);
    docdb::PgsqlReadOperationData data{
        .read_operation_data = docdb::ReadOperationData::FromReadTime(time),
        .is_explicit_request_read_time = true,
        .request = req,
        .doc_read_context = *table->context,
        .index_doc_read_context = nullptr,
        .txn_op_context = context,
        .ql_storage = storage,
        .pending_op = pending,
        .vector_index = nullptr,
    };
    WriteBuffer rows(4096);
    ReadRestartData restart;
    docdb::PgsqlReadOperation op(data, response, &rows, &restart);
    const auto count = VERIFY_RESULT(op.Execute());
    SCHECK(!restart.is_valid(), TryAgain, "Native read requested a snapshot restart");
    response.set_status(PgsqlResponsePB::PGSQL_STATUS_OK);
    return TransactionReadResult{response.ToGoogleProtobuf(), rows.ToBuffer(), count, time};
  }

  Status ReadSql(
      const TransactionId& id, const PgsqlReadRequestMsg& request, PgsqlResponseMsg* response,
      rpc::Sidecars* sidecars, CoarseTimePoint deadline, HybridTime own_limit,
      uint32_t subtransaction, HybridTime snapshot) {
    std::lock_guard lock(mutex_);
    SCHECK(response && sidecars, InvalidArgument, "Missing SQL response or sidecars");
    response->Clear();
    auto* txn = VERIFY_RESULT(Active(id));
    SCHECK(owner_, NotSupported, "SQL execution requires authoritative catalog descriptors");
    RETURN_NOT_OK(StartWrite(txn, subtransaction));
    return FinishStatement(
        txn, DoReadSql(txn, request, response, sidecars, deadline, own_limit, snapshot),
        *response);
  }

  Status DoReadSql(
      Txn* txn, const PgsqlReadRequestMsg& request, PgsqlResponseMsg* response,
      rpc::Sidecars* sidecars, CoarseTimePoint deadline, HybridTime own_limit,
      HybridTime snapshot) {
    SCHECK_EQ(request.client(), YQL_CLIENT_PGSQL, InvalidArgument, "Expected a YSQL request");
    SCHECK(
        !request.has_sampling_state() && request.sample_blocks().empty() &&
            !request.has_vector_idx_options() && !request.has_get_tablet_key_ranges_request() &&
            !request.is_for_backfill() && !request.has_backfill_spec() &&
            !request.skip_intents_read() && !request.read_at_in_txn_limit() &&
            request.metrics_capture() == PGSQL_METRICS_CAPTURE_NONE &&
            !request.has_deprecated_max_partition_key(),
        NotSupported, "Unsupported native SQL scan mode");
    SCHECK(
        !request.has_index_request(), NotSupported,
        "Nested application index reads require separate column-family storage");
    auto* table = VERIFY_RESULT(Find(request.table_id().ToBuffer()));
    SCHECK_EQ(
        request.schema_version(), table->schema_version, InvalidArgument,
        "Native SQL schema version mismatch");
    const auto& page = request.paging_state();
    SCHECK(
        !page.has_table_id() || page.table_id() == request.table_id(), InvalidArgument,
        "Paging table mismatch");
    auto time = ReadTime(*txn, own_limit, snapshot);
    if (page.has_read_time()) {
      const auto page_time = ReadHybridTime::FromPB(page.read_time());
      SCHECK(
          page_time.read == time.read && page_time.global_limit == time.global_limit &&
              page_time.local_limit == time.local_limit &&
              (!own_limit.is_valid() || page_time.in_txn_limit == time.in_txn_limit),
          NotSupported, "Historical native SQL paging is not supported");
      time = page_time;
    }
    SCHECK(
        !time.in_txn_limit.is_special() && time.in_txn_limit >= time.read &&
            time.in_txn_limit <= clock_,
        InvalidArgument, "Invalid own-write statement limit");
    SCHECK(
        (!request.has_hash_code() || request.hash_code() <= 65535) &&
            (!request.has_max_hash_code() || request.max_hash_code() <= 65535),
        InvalidArgument, "Hash bound exceeds uint16");
    ThreadSafeArena arena;
    LWPgsqlReadRequestPB normalized(&arena);
    normalized.CopyFrom(request);
    const auto& schema = table->context->schema();
    const bool unbounded = !request.has_lower_bound() && !request.has_upper_bound();
    SCHECK(
        !unbounded || request.batch_arguments().empty() || table->partitions.size() == 1,
        NotSupported, "Unbounded SQL batches require one partition");
    if (unbounded && request.has_ybctid_column_value() && request.batch_arguments().empty()) {
      const auto& ybctid = request.ybctid_column_value();
      SCHECK(
          ybctid.has_value() && ybctid.value().has_binary_value(), InvalidArgument,
          "SQL ybctid must be a binary constant");
      dockv::DocKey key;
      RETURN_NOT_OK(key.FullyDecodeFrom(ybctid.value().binary_value()));
      SCHECK_EQ(
          key.has_hash(), schema.num_hash_key_columns() > 0, InvalidArgument,
          "SQL ybctid hash does not match table schema");
      if (key.has_hash()) {
        const auto hash = VERIFY_RESULT(dockv::DocKey::DecodeHash(ybctid.value().binary_value()));
        normalized.set_hash_code(hash);
        normalized.set_max_hash_code(hash);
        // Reverse routing uses an exclusive upper partition key, including at hash boundaries.
        if (!request.is_forward_scan() && hash == dockv::PartitionSchema::kMaxPartitionKey) {
          normalized.clear_partition_key();
        } else {
          normalized.dup_partition_key(
              dockv::PartitionSchema::EncodeMultiColumnHashValue(
                  request.is_forward_scan() ? hash : hash + 1));
        }
      } else {
        RETURN_NOT_OK(dockv::InitPartitionKey(schema, table->partition_schema, &normalized));
      }
    } else {
      // PgGate supplies tablet bounds for its scalar and batched ybctid reads.
      RETURN_NOT_OK(dockv::InitPartitionKey(schema, table->partition_schema, &normalized));
    }
    SCHECK(
        request.is_forward_scan() || table->partitions.size() == 1 ||
            (normalized.has_hash_code() && normalized.has_max_hash_code() &&
             normalized.hash_code() == normalized.max_hash_code()),
        NotSupported, "Reverse application scans require one hash code or one partition");
    auto position = table->partitions.end();
    if (request.is_forward_scan()) {
      position = std::upper_bound(
          table->partitions.begin(), table->partitions.end(), normalized.partition_key(),
          [](const auto& key, const auto& partition) {
            return key < partition.partition_key_start();
          });
    } else if (!normalized.partition_key().empty()) {
      position = std::lower_bound(
          table->partitions.begin(), table->partitions.end(), normalized.partition_key(),
          [](const auto& partition, const auto& key) {
            return partition.partition_key_start() < key;
          });
    }
    SCHECK(position != table->partitions.begin(), InvalidArgument, "Invalid scan partition key");
    const auto tablet_index = std::distance(table->partitions.begin(), position) - 1;
    const auto& partition = table->partitions[tablet_index];
    const auto lower = VERIFY_RESULT(
        table->partition_schema.GetEncodedPartitionKey(partition.partition_key_start()));
    const auto upper = VERIFY_RESULT(
        table->partition_schema.GetEncodedPartitionKey(partition.partition_key_end()));
    docdb::KeyBounds bounds(lower, upper);
    auto db = table->DB();
    db.key_bounds = &bounds;
    const auto row_mark = request.has_row_mark_type() ? request.row_mark_type() : ROW_MARK_ABSENT;
    if (IsValidRowMarkType(row_mark)) {
      RETURN_NOT_OK(LockRows(txn, table, normalized, row_mark, time.read));
    }
    docdb::QLRocksDBStorage storage(
        "native-sql", db, {.start_key = KeyBuffer(lower), .end_key = KeyBuffer(upper)});
    auto read_data = docdb::ReadOperationData::FromReadTime(time);
    read_data.deadline = deadline;
    ScopedRWOperation pending(&operations_, deadline);
    RETURN_NOT_OK(pending);
    TransactionOperationContext context(
        txn->metadata.transaction_id, SubTransactionMetadata(txn->subtransaction), this);
    docdb::PgsqlReadOperationData data{
        .read_operation_data = read_data,
        .is_explicit_request_read_time = true,
        .request = normalized,
        .doc_read_context = *table->context,
        .index_doc_read_context = nullptr,
        .txn_op_context = context,
        .ql_storage = storage,
        .pending_op = pending,
        .vector_index = nullptr,
    };
    WriteBuffer rows(4096);
    ReadRestartData restart;
    docdb::PgsqlReadOperation operation(data, *response, &rows, &restart);
    const auto row_count = VERIFY_RESULT(operation.Execute());
    SCHECK(!restart.is_valid(), NotSupported, "Native SQL read restart is not supported");
    if (response->status() != PgsqlResponsePB::PGSQL_STATUS_OK) return Status::OK();
    if (response->has_paging_state()) {
      response->mutable_paging_state()->set_total_num_rows_read(
          page.total_num_rows_read() + row_count);
    }
    if (request.is_forward_scan() && request.return_paging_state() &&
        response->paging_state().next_row_key().empty() &&
        request.partition_column_values().empty() && !request.has_ybctid_column_value()) {
      const auto& end = partition.partition_key_end();
      if (!end.empty() &&
          (!request.has_max_hash_code() ||
           request.max_hash_code() >= dockv::PartitionSchema::DecodeMultiColumnHashValue(end))) {
        auto* paging = response->mutable_paging_state();
        paging->dup_next_partition_key(end);
        paging->dup_next_tablet_bound(end);
        paging->set_total_num_rows_read(page.total_num_rows_read() + row_count);
      }
    }
    if (response->has_paging_state())
      time.ToPB(response->mutable_paging_state()->mutable_read_time());
    response->set_status(PgsqlResponsePB::PGSQL_STATUS_OK);
    sidecars->Start().Take(&rows);
    response->set_rows_data_sidecar(static_cast<int>(sidecars->Complete()));
    return Status::OK();
  }

  // Status is only consulted through intents, so a transaction that wrote none is unreferenced
  // once it finishes. Retaining it would only consume the bounded transaction history.
  bool Unreferenced(const Txn& txn) const {
    return !txn.catalog_owned && txn.touched.empty() && txn.pb.intent_bytes() == 0;
  }

  Status Forget(Txn* txn) {
    const auto id = txn->metadata.transaction_id;
    rocksdb::WriteBatch batch;
    batch.Delete(metadata_, MetaKey("tx/" + id.ToString()));
    // Unsynced: a lost delete leaves a pending record that recovery marks aborted.
    RETURN_NOT_OK(Persist(&batch, false));
    if (txn->pb.status() == TransactionStatus::PENDING) storage_->DataFinished();
    txns_.erase(id);
    return Status::OK();
  }

  Status Commit(const TransactionId& id) {
    std::lock_guard lock(mutex_);
    auto* txn = VERIFY_RESULT(Active(id));
    SCHECK(!txn->catalog_owned, NotSupported, "Finish through the catalog coordinator");
    if (Unreferenced(*txn)) return Forget(txn);
    auto next = txn->pb;
    next.set_commit_ht(VERIFY_RESULT(Tick()).ToUint64());
    next.set_status(TransactionStatus::COMMITTED);
    TEST_SYNC_POINT("PgLocalTxnStore::BeforeCommit");
    // WAL sync covers all earlier intent batches in this DB, across every table CF. The
    // native status manager exposes these unapplied intents only after the durable commit.
    RETURN_NOT_OK(Save(txn, std::move(next), true, true));
    TEST_SYNC_POINT("PgLocalTxnStore::AfterCommit");
    return Status::OK();
  }

  Status AbortTxn(const TransactionId& id, bool catalog_owned = false) {
    RETURN_NOT_OK(Status(failure_));
    const auto it = txns_.find(id);
    SCHECK(it != txns_.end(), NotFound, "Unknown transaction");
    auto& txn = it->second;
    SCHECK(
        !txn.catalog_owned || catalog_owned, NotSupported,
        "Finish through the catalog coordinator");
    SCHECK(
        txn.pb.status() != TransactionStatus::COMMITTED, IllegalState,
        "Cannot abort a committed transaction");
    if (txn.pb.status() == TransactionStatus::ABORTED) return Status::OK();
    if (Unreferenced(txn)) return Forget(&txn);
    auto next = txn.pb;
    next.set_status(TransactionStatus::ABORTED);
    return Save(&txn, std::move(next), true);
  }

  Status RollbackTo(const TransactionId& id, uint32_t subtransaction, bool catalog_owned = false) {
    std::lock_guard lock(mutex_);
    auto* txn = VERIFY_RESULT(Active(id, true));
    SCHECK(
        !txn->catalog_owned || catalog_owned, NotSupported,
        "Rollback through the catalog coordinator");
    SCHECK(subtransaction > 0, InvalidArgument, "Invalid rollback subtransaction");
    SCHECK_LT(txn->pb.rollbacks(), 1024, NotSupported, "Subtransaction rollback limit reached");
    // SQL may roll back to a savepoint this store never saw: no request used it or any later one.
    auto sub = txn->subtransaction;
    sub.subtransaction_id = std::max(sub.subtransaction_id, subtransaction);
    RETURN_NOT_OK(sub.aborted.SetRange(subtransaction, sub.subtransaction_id));
    auto next = txn->pb;
    next.set_rollbacks(next.rollbacks() + 1);
    next.clear_subtransaction();
    sub.ToPB(next.mutable_subtransaction());
    if (next.failed_subtransaction() >= subtransaction) next.clear_failed_subtransaction();
    return Save(txn, std::move(next), true);
  }

  HybridTime LocalCommitTime(const TransactionId& id) override {
    const auto it = txns_.find(id);
    return it != txns_.end() && it->second.pb.status() == TransactionStatus::COMMITTED
               ? HybridTime(it->second.pb.commit_ht())
               : HybridTime::kInvalid;
  }
  std::optional<TransactionLocalState> LocalTxnData(const TransactionId& id) override {
    auto time = LocalCommitTime(id);
    if (!time.is_valid()) return std::nullopt;
    return TransactionLocalState{time, txns_.at(id).subtransaction.aborted};
  }
  void RequestStatusAt(const StatusRequest& request) override {
    if (!failure_.ok()) {
      request.callback(Status(failure_));
      return;
    }
    const auto it = txns_.find(*request.id);
    if (it == txns_.end()) {
      request.callback(STATUS(Corruption, "Intent references unknown local transaction"));
      return;
    }
    if (request.status_tablet_id) *request.status_tablet_id = kStatusLocation;
    const auto& txn = it->second;
    HybridTime time = txn.pb.status() == TransactionStatus::COMMITTED
                          ? HybridTime(txn.pb.commit_ht())
                          : HybridTime::kInvalid;
    if (txn.pb.status() == TransactionStatus::PENDING) {
      const auto promised = std::max(request.read_ht, request.global_limit_ht);
      if (promised.is_special()) {
        request.callback(STATUS(NotSupported, "Unbounded pending-status request"));
        return;
      }
      // All callers use local read points. Do not promise an unpersisted remote horizon.
      if (promised > clock_) {
        request.callback(STATUS(NotSupported, "Future status horizon unsupported"));
        return;
      }
      time = promised;
    }
    request.callback(TransactionStatusResult(
        txn.pb.status(), time, txn.subtransaction.aborted, kStatusLocation));
  }
  Result<TransactionMetadata> PrepareMetadata(const LWTransactionMetadataPB& pb) override {
    RETURN_NOT_OK(Status(failure_));
    const auto id = VERIFY_RESULT(FullyDecodeTransactionId(pb.transaction_id()));
    const auto* txn = VERIFY_RESULT(Active(id));
    if (pb.has_isolation()) {
      SCHECK(
          VERIFY_RESULT(TransactionMetadata::FromPB(pb)) == txn->metadata, InvalidArgument,
          "Transaction metadata mismatch");
    }
    return txn->metadata;
  }
  void Abort(const TransactionId& id, TransactionStatusCallback callback) override {
    auto it = txns_.find(id);
    if (it != txns_.end() && it->second.pb.status() == TransactionStatus::COMMITTED) {
      callback(TransactionStatusResult(
          TransactionStatus::COMMITTED, HybridTime(it->second.pb.commit_ht()),
          it->second.subtransaction.aborted, kStatusLocation));
      return;
    }
    const auto status = AbortTxn(id);
    if (!status.ok())
      callback(status);
    else
      callback(TransactionStatusResult::Aborted());
  }
  Status Cleanup(TransactionIdApplyOpIdMap&&) override {
    return STATUS(NotSupported, "Native intent reclamation is not implemented");
  }
  Status FillPriorities(
      boost::container::small_vector_base<std::pair<TransactionId, uint64_t>>* values) override {
    RETURN_NOT_OK(Status(failure_));
    for (auto& [id, priority] : *values) {
      auto it = txns_.find(id);
      SCHECK(it != txns_.end(), Corruption, "Unknown conflicting transaction");
      priority = it->second.metadata.priority;
    }
    return Status::OK();
  }
  Result<std::optional<TabletId>> FindStatusTablet(const TransactionId& id) override {
    RETURN_NOT_OK(Status(failure_));
    if (txns_.contains(id)) return std::optional<TabletId>(kStatusLocation);
    return std::optional<TabletId>();
  }
  HybridTime MinRunningHybridTime() const override {
    // This WAL-backed shared-CF store does not persist Raft hybrid-time frontiers.
    // kMin disables the native intent SST filter; committed intents remain unapplied.
    return HybridTime::kMin;
  }
  Result<HybridTime> WaitForSafeTime(HybridTime time, CoarseTimePoint) override {
    RETURN_NOT_OK(Status(failure_));
    SCHECK(!time.is_special() && time <= clock_, NotSupported, "Future safe-time wait unsupported");
    return clock_;
  }
  const TabletId& tablet_id() const override { return kStatusLocation; }
  void RecordConflictResolutionKeysScanned(int64_t) override {}
  void RecordConflictResolutionScanLatency(MonoDelta) override {}
  Result<int64_t> RegisterRequest(bool) override {
    RETURN_NOT_OK(Status(failure_));
    SCHECK_LT(
        request_id_, std::numeric_limits<int64_t>::max(), IllegalState, "Request IDs exhausted");
    ++active_requests_;
    return ++request_id_;
  }
  void UnregisterRequest(int64_t) override {
    CHECK_GT(active_requests_, 0);
    --active_requests_;
  }

  std::shared_ptr<PgLocalStorage> storage_;
  std::recursive_mutex& mutex_;
  rocksdb::DB* db_;
  const std::unordered_map<std::string, std::unique_ptr<rocksdb::ColumnFamilyHandle>>& handles_;
  rocksdb::ColumnFamilyHandle* metadata_;
  const void* owner_;
  TableLookup lookup_;
  std::function<Status(uint32_t)> statement_start_;
  std::function<size_t()> staged_bytes_;
  std::function<uint64_t()> catalog_generation_;
  std::unordered_map<TableId, std::unique_ptr<Table>> tables_;
  std::unordered_set<TableId> bindings_;
  std::unordered_map<TransactionId, Txn, TransactionIdHash> txns_;
  HybridTime& clock_;
  Status& failure_;
  int64_t request_id_ = 0;
  int64_t active_requests_ = 0;
  RWOperationCounter operations_{"native-intent-store"};
  dockv::SchemaPackingRegistryPtr packings_ =
      std::make_shared<dockv::SchemaPackingRegistry>("native-intents");
  docdb::SharedLockManager locks_;
  tablet::ScopedTabletMetrics metrics_;
  std::shared_ptr<tablet::TabletMetricsHolder> metrics_holder_ =
      std::make_shared<tablet::TabletMetricsHolder>(false, &metrics_, nullptr);
};

PgLocalTxnStore::PgLocalTxnStore(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
PgLocalTxnStore::~PgLocalTxnStore() = default;
Result<std::unique_ptr<PgLocalTxnStore>> PgLocalTxnStore::Open(const std::string& directory) {
  auto storage = VERIFY_RESULT(PgLocalStorage::Open(directory));
  auto impl = std::make_unique<Impl>(storage, storage->db()->DefaultColumnFamily());
  RETURN_NOT_OK(impl->Initialize());
  return std::unique_ptr<PgLocalTxnStore>(new PgLocalTxnStore(std::move(impl)));
}
Status PgLocalTxnStore::CreateTable(const TableId& id, const Schema& schema, int32_t tablets) {
  return impl_->CreateTable(id, schema, tablets);
}
Result<TransactionId> PgLocalTxnStore::Begin(uint64_t priority) { return impl_->Begin(priority); }
Result<TransactionId> PgLocalTxnStore::BeginSql(IsolationLevel isolation, uint64_t priority) {
  SCHECK(impl_->owner_, NotSupported, "SQL transactions require catalog descriptors");
  // Concurrent snapshot transactions would admit write skew, so do not weaken SERIALIZABLE.
  SCHECK(
      isolation != IsolationLevel::SERIALIZABLE_ISOLATION, NotSupported,
      "SERIALIZABLE isolation is not supported by the local runtime");
  return impl_->Begin(priority);
}
Result<ReadHybridTime> PgLocalTxnStore::ReadPoint(const TransactionId& id) {
  return impl_->ReadPoint(id);
}
Status PgLocalTxnStore::Write(
    const TransactionId& id, const PgsqlWriteRequestPB& request, PgsqlResponsePB* response,
    uint32_t subtransaction) {
  return impl_->Write(id, request, response, subtransaction);
}
Result<TransactionReadResult> PgLocalTxnStore::Read(
    const TransactionId& id, const PgsqlReadRequestPB& request, size_t tablet,
    HybridTime own_limit) {
  return impl_->Read(id, request, tablet, own_limit);
}
Status PgLocalTxnStore::WriteSql(
    const TransactionId& id, const PgsqlWriteRequestMsg& request, PgsqlResponseMsg* response,
    rpc::Sidecars* sidecars, uint32_t subtransaction, HybridTime snapshot) {
  return impl_->WriteSql(id, request, response, sidecars, subtransaction, snapshot);
}
Status PgLocalTxnStore::ReadSql(
    const TransactionId& id, const PgsqlReadRequestMsg& request, PgsqlResponseMsg* response,
    rpc::Sidecars* sidecars, CoarseTimePoint deadline, HybridTime own_limit,
    uint32_t subtransaction, HybridTime snapshot) {
  return impl_->ReadSql(
      id, request, response, sidecars, deadline, own_limit, subtransaction, snapshot);
}
Status PgLocalTxnStore::Commit(const TransactionId& id) { return impl_->Commit(id); }
Status PgLocalTxnStore::Abort(const TransactionId& id) {
  std::lock_guard lock(impl_->mutex_);
  return impl_->AbortTxn(id);
}
Status PgLocalTxnStore::RollbackTo(const TransactionId& id, uint32_t subtransaction) {
  return impl_->RollbackTo(id, subtransaction);
}

Result<std::unique_ptr<PgLocalTxnStore>> PgLocalTxnStore::Attach(
    std::shared_ptr<PgLocalStorage> storage, const void* owner, TableLookup lookup,
    std::function<Status(uint32_t)> statement_start, std::function<size_t()> staged_bytes,
    std::function<uint64_t()> catalog_generation) {
  auto it = storage->families().find(kMetadataFamily);
  std::string initialized;
  const auto marker = storage->db()->Get(
      rocksdb::ReadOptions(), storage->metadata(), MetaKey("native-initialized"), &initialized);
  if (!marker.IsNotFound()) RETURN_NOT_OK(marker);
  SCHECK(
      marker.IsNotFound() || (initialized == "1" && it != storage->families().end()), Corruption,
      "Published native metadata family is missing");
  auto* metadata = it == storage->families().end()
                       ? VERIFY_RESULT(storage->CreateFamily(kMetadataFamily))
                       : it->second.get();
  auto impl = std::make_unique<Impl>(
      storage, metadata, owner, std::move(lookup), std::move(statement_start),
      std::move(staged_bytes), std::move(catalog_generation));
  RETURN_NOT_OK(impl->Initialize());
  return std::unique_ptr<PgLocalTxnStore>(new PgLocalTxnStore(std::move(impl)));
}
Result<TransactionId> PgLocalTxnStore::BeginCatalog(uint64_t priority) {
  return impl_->Begin(priority, true);
}
Status PgLocalTxnStore::AdoptCatalog(const TransactionId& id) {
  auto* txn = VERIFY_RESULT(impl_->Active(id));
  SCHECK(
      !txn->catalog_owned && impl_->storage_->catalog_owner() == impl_->owner_, IllegalState,
      "Catalog adoption requires the catalog owner");
  // Begin already counted this transaction as active data; CatalogCommitted/abort finish it.
  txn->catalog_owned = true;
  return Status::OK();
}
Status PgLocalTxnStore::AppendCatalogCommit(
    const TransactionId& id, HybridTime time, rocksdb::WriteBatch* batch) {
  auto* txn = VERIFY_RESULT(impl_->Active(id));
  SCHECK(
      txn->catalog_owned && impl_->storage_->catalog_owner() == impl_->owner_, IllegalState,
      "Not the catalog transaction owner");
  auto next = txn->pb;
  next.set_status(TransactionStatus::COMMITTED);
  next.set_commit_ht(time.ToUint64());
  batch->Put(impl_->metadata_, MetaKey("tx/" + id.ToString()), next.SerializeAsString());
  return Status::OK();
}
void PgLocalTxnStore::CatalogCommitted(const TransactionId& id, HybridTime time) {
  auto& txn = impl_->txns_.at(id);
  txn.pb.set_status(TransactionStatus::COMMITTED);
  txn.pb.set_commit_ht(time.ToUint64());
  impl_->storage_->DataFinished();
}
Status PgLocalTxnStore::AbortCatalog(const TransactionId& id) { return impl_->AbortTxn(id, true); }
Status PgLocalTxnStore::RollbackCatalog(const TransactionId& id, uint32_t subtransaction) {
  return impl_->RollbackTo(id, subtransaction, true);
}
Status PgLocalTxnStore::RefreshCatalogBindings() { return impl_->RefreshCatalogBindings(); }
Result<size_t> PgLocalTxnStore::IntentBytes(const TransactionId& id) {
  return VERIFY_RESULT(impl_->Active(id))->pb.intent_bytes();
}

Result<uint32_t> PgLocalTxnStore::HighestSubtransaction(const TransactionId& id) {
  return VERIFY_RESULT(impl_->Active(id, true))->subtransaction.subtransaction_id;
}
Status PgLocalTxnStore::SetCatalogSubtransaction(const TransactionId& id, uint32_t subtransaction) {
  auto* txn = VERIFY_RESULT(impl_->Active(id));
  SCHECK(
      subtransaction > 0 && !txn->subtransaction.aborted.Test(subtransaction), InvalidArgument,
      "Aborted catalog subtransaction");
  txn->current_subtransaction = subtransaction;
  if (subtransaction <= txn->subtransaction.subtransaction_id) return Status::OK();
  auto next = txn->pb;
  next.mutable_subtransaction()->set_subtransaction_id(subtransaction);
  return impl_->Save(txn, std::move(next), false);
}

Status PgLocalTxnStore::CheckCatalogHealthy(const TransactionId& id) {
  return ResultToStatus(impl_->Active(id));
}
Result<IntraTxnWriteId> PgLocalTxnStore::ReserveCatalogWriteIds(
    const TransactionId& id, size_t count) {
  auto* txn = VERIFY_RESULT(impl_->Active(id));
  const auto first = txn->pb.next_write_id();
  SCHECK_LT(
      count, std::numeric_limits<IntraTxnWriteId>::max() - first, NotSupported,
      "Transaction write IDs exhausted");
  auto next = txn->pb;
  next.set_next_write_id(first + static_cast<IntraTxnWriteId>(count));
  RETURN_NOT_OK(impl_->Save(txn, std::move(next), false));
  return first;
}

}  // namespace yb::local_runtime
