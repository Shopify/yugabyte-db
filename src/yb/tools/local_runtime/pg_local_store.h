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
#include <string>
#include <vector>

#include "yb/common/hybrid_time.h"
#include "yb/common/pgsql_protocol.pb.h"
#include "yb/common/schema.h"
#include "yb/dockv/partition.h"
#include "yb/util/result.h"

namespace yb::local_runtime {

struct ReadResult {
  PgsqlResponsePB response;
  std::string rows;  // PgGate wire format, including the row-count header.
  size_t row_count = 0;
  HybridTime read_time;
};

// Request-execution component, NOT a PgClientService or SQL transaction coordinator.
// Single-row autocommit only; callers must check both Status and response.status().
// No retry deduplication: stmt_id is not an idempotency key.
// One physical DB, with namespace column families. No Tablet, TabletPeer, Raft, or pollers.
class PgLocalStore {
 public:
  static Result<std::unique_ptr<PgLocalStore>> Open(const std::string& directory);
  ~PgLocalStore();

  // Version 0 schemas are immutable in this slice. No SQL colocation or key rewriting.
  Status CreateTable(const TableId& id, const Schema& schema, int32_t logical_tablets);
  Result<Schema> GetSchema(const TableId& id) const;
  Result<std::vector<dockv::Partition>> GetPartitions(const TableId& id) const;
  Result<uint16_t> HashCode(
      const TableId& id, const google::protobuf::RepeatedPtrField<PgsqlExpressionPB>& values) const;

  Status ApplyAutocommitWrite(const PgsqlWriteRequestPB& request, PgsqlResponsePB* response);
  // A multi-tablet statement must pass one captured read_time to every call.
  // kInvalid chooses the latest time separately for each request.
  Result<ReadResult> Read(
      const PgsqlReadRequestPB& request, size_t logical_tablet,
      HybridTime read_time = HybridTime::kInvalid);
  HybridTime LatestReadTime() const;

 private:
  class Impl;
  explicit PgLocalStore(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace yb::local_runtime
