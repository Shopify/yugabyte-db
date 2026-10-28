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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "yb/tablet/metadata.pb.h"

#include "yb/common/entity_ids_types.h"
#include "yb/common/hybrid_time.h"
#include "yb/common/pgsql_protocol.fwd.h"
#include "yb/common/transaction.h"
#include "yb/docdb/docdb_fwd.h"
#include "yb/rocksdb/rocksdb_fwd.h"
#include "yb/rpc/rpc_fwd.h"
#include "yb/tserver/pg_client.pb.h"
#include "yb/util/result.h"

namespace yb::local_runtime {

class PgLocalTxnStore;

// One exclusive catalog overlay, optionally joined with native data in the same WAL batch.
// Callers serialize borrowed ReadDB views and reject unsupported SQL side effects. Native-bound
// application data must use NativeTransactions(), never the legacy overlay. The runtime holds
// the directory lock and pins seed/engine identity.
class PgCatalogTransaction {
 public:
  static Result<std::unique_ptr<PgCatalogTransaction>> Open(
      const std::string& directory, const std::string& seed_rocksdb_directory);
  ~PgCatalogTransaction();
  // Catalog reads remain available for SQL error reporting after a logical abort, but never
  // after a durability failure. Application reads require a healthy transaction.
  Result<rocksdb::DB*> ReadDB(const TableId& table = "");
  Result<uint32_t> AllocateOid(uint32_t database_oid);
  // Native YSQL sequence tuples are nontransactional and live outside the catalog overlay. Each
  // change is synced before it returns and survives rollback; only the shared durability fence,
  // not a logical catalog abort, rejects them. An I/O error leaves the outcome unknown and fences
  // the store until reopen.
  struct SequenceTuple {
    int64_t last_value;
    bool is_called;
  };
  Status InsertSequence(uint32_t database_oid, uint32_t sequence_oid, const SequenceTuple& value);
  Result<SequenceTuple> ReadSequence(uint32_t database_oid, uint32_t sequence_oid);
  // Returns true when skipped: no tuple, or the current tuple differs from expected.
  Result<bool> UpdateSequence(
      uint32_t database_oid, uint32_t sequence_oid, const SequenceTuple& value,
      const std::optional<SequenceTuple>& expected);
  // Returns the first and last value of a range that is fully reserved on disk.
  Result<std::pair<int64_t, int64_t>> FetchSequence(
      uint32_t database_oid, uint32_t sequence_oid, uint32_t fetch_count, int64_t increment,
      int64_t min_value, int64_t max_value, bool cycle);
  Status DeleteSequence(uint32_t database_oid, uint32_t sequence_oid);
  // Post-commit DROP DATABASE callback. Only a durably retired database is accepted; commit and
  // Open already clean up retired sequences, so this is an idempotent bounded sweep.
  Status DeleteDatabaseSequences(uint32_t database_oid);
  // Clone committed built-in-template catalogs into the active transaction. The caller supplies
  // only nonshared system catalogs; SQL publishes pg_database and its version row in this batch.
  Status CreateDatabase(
      uint32_t oid, const std::string& name,
      const std::vector<tablet::TableInfoPB>& source_catalogs, uint32_t subtransaction);
  // Retire a cloned database atomically with shared SQL database/version/dependency deletes.
  // Caller holds an exclusive SQL lease with no target backend/snapshot. Commit reclaims retired
  // catalog versions in bounded WAL batches before releasing ownership; Open resumes interrupted
  // cleanup. Retired database OIDs cannot be reused, but names can be reused with a fresh OID.
  Status DropDatabase(uint32_t oid, const std::string& name, uint32_t subtransaction);
  Result<std::vector<tablet::TableInfoPB>> CatalogTables();
  Status CreateTable(const tablet::TableInfoPB& metadata, uint32_t subtransaction);
  Status AlterTable(const tserver::PgAlterTableRequestPB& request);
  Status DropTable(const TableId& id, uint32_t subtransaction);
  Status IncrementSchemaVersion(const TableId& id, uint32_t subtransaction);
  Result<std::vector<tablet::TableInfoPB>> Tables();
  Result<tablet::TableInfoPB> GetTable(const TableId& id);
  HybridTime ReadTime() const;
  Status ValidateReadTime(HybridTime time) const;
  bool active() const;
  Status CheckHealthy() const;
  Status Begin();
  // Internal integration gate: no other native transaction may exist. The joined transaction
  // cannot independently commit/abort; Commit/RollbackTo/Abort coordinate both participants.
  Result<TransactionId> BeginWithNative(uint64_t priority = 100);
  PgLocalTxnStore& NativeTransactions();
  Status Apply(
      const PgsqlWriteRequestMsg& request, const docdb::DocReadContextPtr& context,
      PgsqlResponseMsg* response, rpc::Sidecars* sidecars, uint32_t subtransaction);
  Status Commit();
  void Abort();
  Status RollbackTo(uint32_t subtransaction);

 private:
  class Impl;
  explicit PgCatalogTransaction(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace yb::local_runtime
