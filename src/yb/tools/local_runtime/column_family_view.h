// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations
// under the License.

#pragma once

#include "yb/rocksdb/utilities/stackable_db.h"
#include "yb/rocksdb/iterator.h"

namespace yb::local_runtime {

// Read-only adapter over a single column family in a shared RocksDB instance.
//
// The supplied DB and ColumnFamilyHandle are owned by the caller; this view only
// exposes the handle as DefaultColumnFamily() and forwards read APIs to it.
// Writers keep using the shared root DB directly, so the destructor must not
// delete the wrapped DB.
class ColumnFamilyView : public rocksdb::StackableDB {
 public:
  ColumnFamilyView(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* column_family)
      : StackableDB(db), column_family_(column_family) {}

  ~ColumnFamilyView() override { db_ = nullptr; }

  rocksdb::DB* GetRootDB() override { return this; }

  rocksdb::ColumnFamilyHandle* DefaultColumnFamily() const override { return column_family_; }

  using rocksdb::DB::Get;
  rocksdb::Status Get(
      const rocksdb::ReadOptions& options, rocksdb::ColumnFamilyHandle* column_family,
      const rocksdb::Slice& key, std::string* value) override {
    if (!IsViewColumnFamily(column_family)) {
      return STATUS(
          InvalidArgument, "ColumnFamilyView can only read from its default column family");
    }
    return db_->Get(options, column_family_, key, value);
  }

  using rocksdb::DB::KeyMayExist;
  bool KeyMayExist(
      const rocksdb::ReadOptions& options, rocksdb::ColumnFamilyHandle* column_family,
      const rocksdb::Slice& key, std::string* value, bool* value_found = nullptr) override {
    if (!IsViewColumnFamily(column_family)) {
      if (value_found != nullptr) {
        *value_found = false;
      }
      return true;  // An unsupported lookup cannot prove absence.
    }
    return db_->KeyMayExist(options, column_family_, key, value, value_found);
  }

  using rocksdb::DB::Put;
  rocksdb::Status Put(
      const rocksdb::WriteOptions&, rocksdb::ColumnFamilyHandle*, const rocksdb::Slice&,
      const rocksdb::Slice&) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  using rocksdb::DB::Delete;
  rocksdb::Status Delete(
      const rocksdb::WriteOptions&, rocksdb::ColumnFamilyHandle*, const rocksdb::Slice&) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  using rocksdb::DB::SingleDelete;
  rocksdb::Status SingleDelete(
      const rocksdb::WriteOptions&, rocksdb::ColumnFamilyHandle*, const rocksdb::Slice&) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  using rocksdb::DB::Merge;
  rocksdb::Status Merge(
      const rocksdb::WriteOptions&, rocksdb::ColumnFamilyHandle*, const rocksdb::Slice&,
      const rocksdb::Slice&) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status Write(const rocksdb::WriteOptions&, rocksdb::WriteBatch*) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status CreateColumnFamily(
      const rocksdb::ColumnFamilyOptions&, const std::string&,
      rocksdb::ColumnFamilyHandle**) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status DropColumnFamily(rocksdb::ColumnFamilyHandle*) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status AddFile(
      rocksdb::ColumnFamilyHandle*, const rocksdb::ExternalSstFileInfo*, bool) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status AddFile(rocksdb::ColumnFamilyHandle*, const std::string&, bool) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status CompactRange(
      const rocksdb::CompactRangeOptions&, rocksdb::ColumnFamilyHandle*, const rocksdb::Slice*,
      const rocksdb::Slice*) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status CompactFiles(
      const rocksdb::CompactionOptions&, rocksdb::ColumnFamilyHandle*,
      const std::vector<std::string>&, const int, const int) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status Flush(const rocksdb::FlushOptions&, rocksdb::ColumnFamilyHandle*) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status WaitForFlush(rocksdb::ColumnFamilyHandle*) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status UpdateFrontiers(const yb::storage::UserFrontiers&) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  void SetAllowCompactionFailures(rocksdb::AllowCompactionFailures) override {
    // Intentionally ignored: compaction behavior is controlled by the owner.
  }

  using rocksdb::DB::NewIterator;
  rocksdb::Iterator* NewIterator(
      const rocksdb::ReadOptions& options, rocksdb::ColumnFamilyHandle* column_family) override {
    if (!IsViewColumnFamily(column_family)) {
      return rocksdb::NewErrorIterator(STATUS(InvalidArgument, "Read outside namespace"));
    }
    return db_->NewIterator(options, column_family_);
  }

  std::unique_ptr<rocksdb::Iterator> NewIndexIterator(
      const rocksdb::ReadOptions& options, rocksdb::SkipLastEntry skip_last_index_entry,
      rocksdb::ColumnFamilyHandle* column_family) override {
    if (!IsViewColumnFamily(column_family)) {
      return std::unique_ptr<rocksdb::Iterator>(
          rocksdb::NewErrorIterator(STATUS(InvalidArgument, "Read outside namespace")));
    }
    return db_->NewIndexIterator(options, skip_last_index_entry, column_family_);
  }

  std::unique_ptr<rocksdb::DataBlockAwareIndexIterator> NewDataBlockAwareIndexIterator(
      const rocksdb::ReadOptions& options, rocksdb::SkipLastEntry skip_last_index_entry,
      rocksdb::ColumnFamilyHandle* column_family) override {
    if (!IsViewColumnFamily(column_family)) {
      return std::make_unique<InvalidDataBlockIterator>();
    }
    return db_->NewDataBlockAwareIndexIterator(options, skip_last_index_entry, column_family_);
  }

  rocksdb::Status NewIterators(
      const rocksdb::ReadOptions& options,
      const std::vector<rocksdb::ColumnFamilyHandle*>& column_families,
      std::vector<rocksdb::Iterator*>* iterators) override {
    for (auto* column_family : column_families) {
      if (!IsViewColumnFamily(column_family)) {
        return STATUS(InvalidArgument, "ColumnFamilyView cannot span multiple column families");
      }
    }
    return db_->NewIterators(options, column_families, iterators);
  }

  bool GetProperty(
      rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& property,
      std::string* value) override {
    if (!IsViewColumnFamily(column_family)) {
      return false;
    }
    return db_->GetProperty(column_family_, property, value);
  }

  bool GetIntProperty(
      rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& property,
      uint64_t* value) override {
    if (!IsViewColumnFamily(column_family)) {
      return false;
    }
    return db_->GetIntProperty(column_family_, property, value);
  }

  void GetApproximateSizes(
      rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Range* r, int n, uint64_t* sizes,
      bool include_memtable = false) override {
    if (!IsViewColumnFamily(column_family)) {
      std::fill_n(sizes, n, 0);
      return;
    }
    db_->GetApproximateSizes(column_family_, r, n, sizes, include_memtable);
  }

  const rocksdb::Options& GetOptions(rocksdb::ColumnFamilyHandle*) const override {
    return db_->GetOptions(column_family_);
  }

  int NumberLevels(rocksdb::ColumnFamilyHandle*) override {
    return db_->NumberLevels(column_family_);
  }

  int MaxMemCompactionLevel(rocksdb::ColumnFamilyHandle*) override {
    return db_->MaxMemCompactionLevel(column_family_);
  }

  int Level0StopWriteTrigger(rocksdb::ColumnFamilyHandle*) override {
    return db_->Level0StopWriteTrigger(column_family_);
  }

  rocksdb::Status SetOptions(
      rocksdb::ColumnFamilyHandle* column_family,
      const std::unordered_map<std::string, std::string>&, bool) override {
    if (!IsViewColumnFamily(column_family)) {
      return STATUS(
          InvalidArgument, "ColumnFamilyView cannot operate on a different column family");
    }
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  std::vector<rocksdb::Status> MultiGet(
      const rocksdb::ReadOptions& options,
      const std::vector<rocksdb::ColumnFamilyHandle*>& families,
      const std::vector<rocksdb::Slice>& keys, std::vector<std::string>* values) override {
    if (families.size() != keys.size()) {
      values->assign(keys.size(), {});
      return std::vector<rocksdb::Status>(
          keys.size(), STATUS(InvalidArgument, "Mismatched key and namespace counts"));
    }
    for (auto* family : families) {
      if (!IsViewColumnFamily(family)) {
        values->resize(keys.size());
        return std::vector<rocksdb::Status>(
            keys.size(), STATUS(InvalidArgument, "Read outside namespace"));
      }
    }
    return db_->MultiGet(options, families, keys, values);
  }

  rocksdb::Status DeleteFile(std::string) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status ModifyFlushedFrontier(
      yb::storage::UserFrontierPtr, rocksdb::FrontierModificationMode) override {
    return STATUS(NotSupported, "ColumnFamilyView is read-only");
  }

  rocksdb::Status PauseBackgroundWork() override {
    return STATUS(NotSupported, "Lifecycle belongs to the shared store");
  }
  rocksdb::Status ContinueBackgroundWork() override {
    return STATUS(NotSupported, "Lifecycle belongs to the shared store");
  }
  rocksdb::Status EnableAutoCompaction(const std::vector<rocksdb::ColumnFamilyHandle*>&) override {
    return STATUS(NotSupported, "Lifecycle belongs to the shared store");
  }
  rocksdb::Status DisableFileDeletions() override {
    return STATUS(NotSupported, "Lifecycle belongs to the shared store");
  }
  rocksdb::Status EnableFileDeletions(bool) override {
    return STATUS(NotSupported, "Lifecycle belongs to the shared store");
  }

 private:
  class InvalidDataBlockIterator : public rocksdb::DataBlockAwareIndexIterator {
   public:
    const rocksdb::KeyValueEntry& Entry() const override {
      return rocksdb::KeyValueEntry::Invalid();
    }
    const rocksdb::KeyValueEntry& Seek(rocksdb::Slice) override { return Entry(); }
    const rocksdb::KeyValueEntry& SeekToFirst() override { return Entry(); }
    const rocksdb::KeyValueEntry& SeekToLast() override { return Entry(); }
    const rocksdb::KeyValueEntry& Next() override { return Entry(); }
    const rocksdb::KeyValueEntry& Prev() override { return Entry(); }
    void UpdateFilterKey(rocksdb::Slice, rocksdb::Slice) override {}
    rocksdb::Status status() const override {
      return STATUS(InvalidArgument, "Read outside namespace");
    }
    Result<std::pair<std::string, std::string>> GetCurrentDataBlockBounds() const override {
      return status();
    }
  };

  bool IsViewColumnFamily(const rocksdb::ColumnFamilyHandle* column_family) const {
    return column_family == column_family_;
  }

  rocksdb::ColumnFamilyHandle* column_family_;
};

}  // namespace yb::local_runtime
