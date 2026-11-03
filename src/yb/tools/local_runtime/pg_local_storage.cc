// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations
// under the License.

#include "yb/tools/local_runtime/pg_local_storage.h"

#include <charconv>
#include <filesystem>
#include <limits>
#include <map>

#include "yb/common/doc_hybrid_time.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/dockv/doc_key.h"
#include "yb/rocksdb/db.h"
#include "yb/rocksdb/iterator.h"
#include "yb/rocksdb/write_batch.h"
#include "yb/tablet/tablet_options.h"
#include "yb/util/env_util.h"
#include "yb/util/flags.h"
#include "yb/util/logging.h"
#include "yb/util/tostring.h"

DEFINE_NON_RUNTIME_uint64(local_runtime_churn_write_buffer_bytes, 8ULL * 1024 * 1024,
    "Memtable size for intents and metadata families, whose entries are mostly removed or "
    "superseded. 0 keeps the default size, without compaction.");

namespace yb::local_runtime {
namespace {

constexpr auto kMetadataFamily = "local-metadata";

Status CopySeed(const std::string& source, const std::string& destination) {
  auto* env = Env::Default();
  try {
    const auto temporary = destination + ".initializing";
    RETURN_NOT_OK(env->CreateDir(temporary));
    WritableFileOptions options;
    options.sync_on_close = true;
    options.mode = Env::CREATE_NON_EXISTING;
    for (const auto& entry : std::filesystem::directory_iterator(source)) {
      SCHECK(
          !entry.is_symlink() && entry.is_regular_file(), InvalidArgument,
          "Seed RocksDB directory must contain only regular files");
      RETURN_NOT_OK(
          env_util::CopyFile(
              env, entry.path().string(), temporary + "/" + entry.path().filename().string(),
              options));
    }
    RETURN_NOT_OK(env->SyncDir(temporary));
    RETURN_NOT_OK(env->RenameFile(temporary, destination));
    return env->SyncDir(std::filesystem::path(destination).parent_path().string());
  } catch (const std::filesystem::filesystem_error& error) {
    return STATUS(IOError, error.what());
  }
}

Result<HybridTime> ParseTime(const std::string& value) {
  uint64_t number;
  const auto parsed = std::from_chars(value.data(), value.data() + value.size(), number);
  SCHECK(
      parsed.ec == std::errc() && parsed.ptr == value.data() + value.size() &&
          !HybridTime(number).is_special(),
      Corruption, "Invalid shared local clock");
  return HybridTime(number);
}

}  // namespace

PgLocalStorage::~PgLocalStorage() {
  if (!db_) return;
  // Families are never flushed, so their memtables hold the store's resident data.
  std::map<std::string, uint64_t> bytes;
  for (const auto& [name, handle] : families_) {
    uint64_t size = 0;
    if (!db_->GetIntProperty(handle.get(), rocksdb::DB::Properties::kCurSizeAllMemTables, &size))
      continue;
    const bool rows = name.starts_with("table/") || name.starts_with("r/");
    const bool intents = name.starts_with("intents/") || name.starts_with("i/");
    bytes[rows ? "rows" : intents ? "intents" : name] += size;
  }
  LOG(INFO) << "Local storage memtable bytes: " << AsString(bytes) << ", writes: " << writes_
            << ", synced: " << synced_writes_ << ", family changes: " << family_changes_;
}

std::string PgLocalStorage::MetadataKey(const std::string& name) {
  return dockv::DocKey(dockv::KeyEntryValues{dockv::KeyEntryValue(name)})
             .Encode()
             .ToStringBuffer() +
         dockv::KeyEntryValue(DocHybridTime(HybridTime::kInitial, 0)).ToKeyBytes().ToStringBuffer();
}

Result<std::shared_ptr<PgLocalStorage>> PgLocalStorage::Open(
    const std::string& directory, const std::string& seed) {
  auto result = std::make_shared<PgLocalStorage>();
  RETURN_NOT_OK(result->Initialize(directory, seed));
  return result;
}

Status PgLocalStorage::Initialize(const std::string& directory, const std::string& seed) {
  const bool fresh = !Env::Default()->FileExists(directory);
  if (fresh && !seed.empty()) RETURN_NOT_OK(CopySeed(seed, directory));
  std::error_code error;
  SCHECK(
      !std::filesystem::is_symlink(directory, error), InvalidArgument,
      "Local storage path is a symlink");
  tablet::TabletOptions tablet_options;
  tablet_options.block_cache = rocksdb::NewLRUCache(16 * 1024 * 1024);
  docdb::InitRocksDBOptions(
      &regular_options_, "local-shared", "local-shared", nullptr, tablet_options,
      docdb::StorageDbType::kRegular);
  docdb::InitRocksDBOptions(
      &intents_options_, "local-shared", "local-shared", nullptr, tablet_options,
      docdb::StorageDbType::kIntents);
  // Removed intents and superseded clock/status records stay in a memtable, beside their
  // tombstones, until it flushes; with the default 128 MiB buffers they rarely do and come to most
  // of the store's memory. Small buffers let a flush drop them, and universal compaction (the
  // native default) merges what survives. Table and catalog rows keep large buffers and no
  // compaction: without history GC, compacting them would only rewrite the same versions.
  const auto churn_bytes = FLAGS_local_runtime_churn_write_buffer_bytes;
  churn_options_ = regular_options_;
  for (auto* options : {&regular_options_, &intents_options_, &churn_options_}) {
    options->disable_wal = false;
    options->max_manifest_file_size = 128ULL * 1024 * 1024;
    if (options == &regular_options_ || !churn_bytes) {
      options->compaction_style = rocksdb::kCompactionStyleNone;
      options->level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
      options->level0_stop_writes_trigger = std::numeric_limits<int>::max();
    } else {
      options->write_buffer_size = churn_bytes;
      // Flushed intents files are small and mostly cancel out; merge them early.
      options->level0_file_num_compaction_trigger = 2;
      options->compaction_options_universal.min_merge_width = 2;
    }
  }
  regular_options_.create_if_missing = fresh && seed.empty();
  regular_options_.create_missing_column_families = fresh;
  std::vector<std::string> names;
  if (fresh && seed.empty())
    names.push_back(rocksdb::kDefaultColumnFamilyName);
  else
    RETURN_NOT_OK(rocksdb::DB::ListColumnFamilies(regular_options_, directory, &names));
  if (fresh) names.push_back(kMetadataFamily);
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  for (const auto& name : names) descriptors.emplace_back(name, FamilyOptions(name));
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::DB* db;
  RETURN_NOT_OK(rocksdb::DB::Open(regular_options_, directory, descriptors, &handles, &db));
  db_.reset(db);
  for (auto* handle : handles)
    families_.emplace(handle->GetName(), std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle));
  SCHECK(families_.contains(kMetadataFamily), NotSupported, "Missing shared storage format");
  const std::string format = seed.empty() ? "local-native-intents/2" : "local-shared-sql/1";
  if (!fresh) {
    std::string value;
    RETURN_NOT_OK(
        db_->Get(rocksdb::ReadOptions(), metadata(), MetadataKey("storage-format"), &value));
    SCHECK_EQ(value, format, NotSupported, "Incompatible shared local storage format");
    RETURN_NOT_OK(db_->Get(rocksdb::ReadOptions(), metadata(), MetadataKey("clock"), &value));
    clock_ = VERIFY_RESULT(ParseTime(value));
    RETURN_NOT_OK(db_->Get(rocksdb::ReadOptions(), metadata(), MetadataKey("committed"), &value));
    committed_ = VERIFY_RESULT(ParseTime(value));
    SCHECK_LE(committed_, clock_, Corruption, "Committed clock exceeds allocated clock");
    return Status::OK();
  }
  SCHECK_EQ(families_.size(), 2, Corruption, "New store contains unexpected families");
  if (!seed.empty()) {
    std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(rocksdb::ReadOptions()));
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      const auto time = VERIFY_RESULT(DocHybridTime::DecodeFromEnd(iterator->key())).hybrid_time();
      SCHECK(!time.is_special(), Corruption, "Special timestamp in seed");
      clock_.MakeAtLeast(time);
    }
    RETURN_NOT_OK(iterator->status());
  }
  committed_ = clock_;
  rocksdb::WriteBatch batch;
  batch.Put(metadata(), MetadataKey("storage-format"), format);
  return Write(&batch, true);
}

const rocksdb::Options& PgLocalStorage::FamilyOptions(const std::string& name) const {
  if (name.starts_with("i/") || name.starts_with("intents/")) return intents_options_;
  // Shared metadata and the native transaction store's status records.
  if (name == kMetadataFamily || name == "local-native-transactions") return churn_options_;
  return regular_options_;
}

Status PgLocalStorage::FlushFamily(rocksdb::ColumnFamilyHandle* family) {
  rocksdb::FlushOptions options;
  options.wait = false;
  return db_->Flush(options, family);
}

rocksdb::ColumnFamilyHandle* PgLocalStorage::metadata() const {
  return families_.at(kMetadataFamily).get();
}

Result<rocksdb::ColumnFamilyHandle*> PgLocalStorage::CreateFamily(const std::string& name) {
  RETURN_NOT_OK(Status(failure_));
  SCHECK(!families_.contains(name), AlreadyPresent, "Column family already exists");
  rocksdb::ColumnFamilyHandle* handle;
  ++family_changes_;
  failure_ = db_->CreateColumnFamily(FamilyOptions(name), name, &handle);
  RETURN_NOT_OK(Status(failure_));
  families_.emplace(name, std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle));
  return handle;
}

Status PgLocalStorage::DropFamily(const std::string& name) {
  RETURN_NOT_OK(Status(failure_));
  const auto it = families_.find(name);
  SCHECK(it != families_.end(), NotFound, "Missing column family");
  SCHECK(
      name != kMetadataFamily && name != rocksdb::kDefaultColumnFamilyName, InvalidArgument,
      "Cannot drop shared metadata or catalogs");
  ++family_changes_;
  failure_ = db_->DropColumnFamily(it->second.get());
  RETURN_NOT_OK(Status(failure_));
  families_.erase(it);
  return Status::OK();
}

Result<HybridTime> PgLocalStorage::Tick() {
  RETURN_NOT_OK(Status(failure_));
  const auto next = HybridTime(
      std::max(
          clock_.ToUint64() + 1, HybridTime::FromMicros(Env::Default()->NowMicros()).ToUint64()));
  SCHECK(!next.is_special() && next > clock_, IllegalState, "Shared local clock exhausted");
  clock_ = next;
  return next;
}

Status PgLocalStorage::Write(rocksdb::WriteBatch* batch, bool sync, HybridTime commit_time) {
  RETURN_NOT_OK(Status(failure_));
  if (commit_time.is_valid()) {
    SCHECK(
        sync && commit_time > committed_ && commit_time <= clock_ && !commit_time.is_special(),
        IllegalState, "Invalid shared commit time");
  }
  batch->Put(metadata(), MetadataKey("clock"), std::to_string(clock_.ToUint64()));
  batch->Put(
      metadata(), MetadataKey("committed"),
      std::to_string((commit_time.is_valid() ? commit_time : committed_).ToUint64()));
  rocksdb::WriteOptions options;
  options.sync = sync;
  ++writes_;
  if (sync) ++synced_writes_;
  failure_ = db_->Write(options, batch);
  RETURN_NOT_OK(Status(failure_));
  if (commit_time.is_valid()) committed_ = commit_time;
  return Status::OK();
}

Status PgLocalStorage::AcquireCatalog(const void* owner, bool adopting) {
  RETURN_NOT_OK(Status(failure_));
  SCHECK(
      !catalog_owner_ && active_data_ == (adopting ? 1 : 0), NotSupported,
      "Catalog changes require exclusive transaction ownership");
  catalog_owner_ = owner;
  return Status::OK();
}

void PgLocalStorage::ReleaseCatalog(const void* owner) {
  if (catalog_owner_ == owner) catalog_owner_ = nullptr;
}

void PgLocalStorage::DataFinished() {
  CHECK_GT(active_data_, 0);
  --active_data_;
}

}  // namespace yb::local_runtime
