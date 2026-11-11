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

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "yb/common/doc_hybrid_time.h"
#include "yb/common/pgsql_protocol.fwd.h"
#include "yb/common/pgsql_protocol.pb.h"
#include "yb/common/read_hybrid_time.h"
#include "yb/common/schema.h"
#include "yb/common/transaction.h"
#include "yb/common/transaction.pb.h"
#include "yb/dockv/intent.h"
#include "yb/rocksdb/rocksdb_fwd.h"
#include "yb/rpc/rpc_fwd.h"
#include "yb/tablet/metadata.pb.h"
#include "yb/util/result.h"

namespace yb::local_runtime {

class PgCatalogTransaction;
class PgLocalStorage;

struct TransactionReadResult {
  PgsqlResponsePB response;
  std::string rows;
  size_t row_count;
  ReadHybridTime read_time;
};

// A request's conflict with pending transactions, which the caller waits out as a tablet's wait
// queue does. The intents are the locks it requested, reported by lock status while it waits.
struct LockWait {
  std::vector<TransactionId> blockers;
  TableId table_id;
  uint32_t subtransaction = 0;
  std::vector<std::pair<std::string, dockv::IntentTypeSet>> intents;
};

struct WaitingLocks {
  TransactionId id;
  HybridTime wait_start;
  const LockWait* wait;
};

// Native shared-intent engine with snapshot isolation. A conflict with a pending transaction
// fails the request, or is reported to the caller to wait out when it passes a LockWait; a
// committed conflict always fails. Attached SQL access uses authoritative catalog descriptors;
// the standalone PB API remains restricted to immutable HASH schemas (INT64/INT32/STRING).
// Operations serialize; transactions overlap. Catalog-owned transactions finish through the
// single-WAL catalog coordinator. Independent SQL transactions read and resolve conflicts at the
// backend's read point (statement or transaction snapshot, as PgGate selects); others use their
// begin time. SQL row locks write native lock intents. No serializable execution, retries, or
// history GC. Finished transactions are reclaimed as on a tablet: committed intents are applied
// to regular rows, then intents and the status record are removed. Bounds: 65536 unfinished
// transactions, 64 active, 1024 rollbacks per txn. Cumulative encoded intents: 512 MiB per
// attached SQL txn, 16 MiB per standalone txn. Each encoded intent batch plus resident catalog
// pending bytes is limited to 16 MiB; the catalog coordinator also limits its final WAL batch to
// 16 MiB.
class PgLocalTxnStore {
 public:
  static Result<std::unique_ptr<PgLocalTxnStore>> Open(const std::string& directory);
  ~PgLocalTxnStore();

  Status CreateTable(const TableId& id, const Schema& schema, int32_t tablets);
  Result<TransactionId> Begin(uint64_t priority = 100);
  // Independent attached SQL transaction. SERIALIZABLE is rejected rather than weakened.
  Result<TransactionId> BeginSql(IsolationLevel isolation, uint64_t priority = 100);
  Result<ReadHybridTime> ReadPoint(const TransactionId& id);
  Status Write(
      const TransactionId& id, const PgsqlWriteRequestPB& request, PgsqlResponsePB* response,
      uint32_t subtransaction = 1);
  Result<TransactionReadResult> Read(
      const TransactionId& id, const PgsqlReadRequestPB& request, size_t tablet,
      HybridTime own_write_limit = HybridTime::kInvalid);
  // snapshot, when valid, replaces the begin time as the read point for others' commits. With a
  // wait, a conflict with pending transactions fills it and returns a kConflict status that
  // leaves the statement unfailed, so the caller may repeat the request once they finish.
  Status WriteSql(
      const TransactionId& id, const PgsqlWriteRequestMsg& request, PgsqlResponseMsg* response,
      rpc::Sidecars* sidecars, uint32_t subtransaction,
      HybridTime snapshot = HybridTime::kInvalid, LockWait* wait = nullptr);
  // Row-marked reads wait like writes, except NOWAIT and SKIP LOCKED.
  Status ReadSql(
      const TransactionId& id, const PgsqlReadRequestMsg& request, PgsqlResponseMsg* response,
      rpc::Sidecars* sidecars, CoarseTimePoint deadline, HybridTime own_limit,
      uint32_t subtransaction = 1, HybridTime snapshot = HybridTime::kInvalid,
      LockWait* wait = nullptr);
  Status Commit(const TransactionId& id);
  Status Abort(const TransactionId& id);
  Status RollbackTo(const TransactionId& id, uint32_t subtransaction);

  // Marks a pending transaction aborted, as the deadlock detector does. Its owner's requests fail
  // from then on, and its own abort removes it.
  Status AbortVictim(const TransactionId& id);
  // Begin time in microseconds of each pending transaction, the age lock status filters on.
  std::vector<std::pair<TransactionId, int64_t>> PendingTransactions();
  // One entry per table: granted locks of the listed transactions, read from their intents, and
  // the given waiting locks. max_locks bounds each transaction's locks per table; zero is
  // unbounded.
  Result<std::vector<TabletLockInfoPB>> LockStatus(
      const std::vector<TransactionId>& transactions, const std::vector<WaitingLocks>& waiting,
      uint32_t max_locks);

 private:
  friend class PgCatalogTransaction;
  static constexpr auto kMetadataFamily = "local-native-transactions";
  using TableLookup = std::function<Result<tablet::TableInfoPB>(const TableId&)>;
  static Result<std::unique_ptr<PgLocalTxnStore>> Attach(
      std::shared_ptr<PgLocalStorage> storage, const void* owner, TableLookup lookup,
      std::function<Status(uint32_t)> statement_start, std::function<size_t()> staged_bytes,
      std::function<uint64_t()> catalog_generation);
  Result<TransactionId> BeginCatalog(uint64_t priority);
  // Joins an independent pending transaction to the catalog transaction that now owns storage.
  Status AdoptCatalog(const TransactionId& id);
  Status AppendCatalogCommit(const TransactionId& id, HybridTime time, rocksdb::WriteBatch* batch);
  void CatalogCommitted(const TransactionId& id, HybridTime time);
  Status AbortCatalog(const TransactionId& id);
  Status RollbackCatalog(const TransactionId& id, uint32_t subtransaction);
  Status RefreshCatalogBindings();
  Result<size_t> IntentBytes(const TransactionId& id);
  Result<uint32_t> HighestSubtransaction(const TransactionId& id);
  Status SetCatalogSubtransaction(const TransactionId& id, uint32_t subtransaction);
  Status CheckCatalogHealthy(const TransactionId& id);
  Result<IntraTxnWriteId> ReserveCatalogWriteIds(const TransactionId& id, size_t count);
  class Impl;
  explicit PgLocalTxnStore(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace yb::local_runtime
