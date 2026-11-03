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

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "yb/common/hybrid_time.h"
#include "yb/rocksdb/options.h"
#include "yb/rocksdb/db.h"
#include "yb/util/result.h"

namespace yb::local_runtime {

// Internal owner shared by the catalog overlay and native-intent engine. Callers hold mutex()
// while using borrowed handles, clocks and the failure status. No participant owns a second WAL.
class PgLocalStorage {
 public:
  static Result<std::shared_ptr<PgLocalStorage>> Open(
      const std::string& directory, const std::string& seed = "");
  ~PgLocalStorage();

  static std::string MetadataKey(const std::string& name);
  rocksdb::DB* db() const { return db_.get(); }
  rocksdb::ColumnFamilyHandle* metadata() const;
  const auto& families() const { return families_; }
  // Starts a background flush of one family; a failure leaves its memtable in place.
  Status FlushFamily(rocksdb::ColumnFamilyHandle* family);
  Result<rocksdb::ColumnFamilyHandle*> CreateFamily(const std::string& name);
  Status DropFamily(const std::string& name);
  std::recursive_mutex& mutex() { return mutex_; }
  Status& failure() { return failure_; }
  HybridTime& clock() { return clock_; }
  HybridTime& committed() { return committed_; }
  Result<HybridTime> Tick();
  Status Write(
      rocksdb::WriteBatch* batch, bool sync, HybridTime commit_time = HybridTime::kInvalid);

  // Catalog ownership excludes independent data transactions. When adopting, the single active
  // data transaction is the one being joined to the catalog transaction.
  Status AcquireCatalog(const void* owner, bool adopting = false);
  void ReleaseCatalog(const void* owner);
  const void* catalog_owner() const { return catalog_owner_; }
  size_t active_data() const { return active_data_; }
  void DataStarted() { ++active_data_; }
  void DataFinished();

 private:
  Status Initialize(const std::string& directory, const std::string& seed);
  std::recursive_mutex mutex_;
  const rocksdb::Options& FamilyOptions(const std::string& name) const;

  rocksdb::Options regular_options_;
  rocksdb::Options intents_options_;
  rocksdb::Options churn_options_;
  // Reported at shutdown: how often a write waits for the WAL to reach the device.
  uint64_t writes_ = 0;
  uint64_t synced_writes_ = 0;
  // Creating or dropping a family also syncs the MANIFEST.
  uint64_t family_changes_ = 0;
  std::unique_ptr<rocksdb::DB> db_;
  std::unordered_map<std::string, std::unique_ptr<rocksdb::ColumnFamilyHandle>> families_;
  HybridTime clock_ = HybridTime::kInitial;
  HybridTime committed_ = HybridTime::kInitial;
  Status failure_;
  const void* catalog_owner_ = nullptr;
  size_t active_data_ = 0;
};

}  // namespace yb::local_runtime
