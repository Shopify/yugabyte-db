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

#include "yb/util/net/net_util.h"
#include "yb/util/result.h"

namespace yb::local_runtime {

enum class CatalogMode { kFrozen, kSerializedTransactions };

// Requests PostgreSQL backends sent through shared memory, and how many responses outgrew the
// exchange: into a pooled segment, or kept for FetchData.
struct SharedMemoryUse {
  uint64_t requests = 0;
  uint64_t segment_responses = 0;
  uint64_t saved_responses = 0;
};

// Owns real YSQL over native catalogs and DocDB. Frozen by default; SQL transactions serialize.
// No master, tserver, Tablet, TabletPeer, or Raft lifecycle.
// Uses process-global PgWrapper flags; run each instance in its own process.
class ReadonlyPgServer {
 public:
  static Result<std::unique_ptr<ReadonlyPgServer>> Start(
      const std::string& directory, const std::string& catalog_snapshot, uint16_t sql_port,
      CatalogMode mode = CatalogMode::kFrozen);
  ~ReadonlyPgServer();

  static Result<std::string> CatalogFingerprint(const std::string& snapshot);

  size_t CatalogTableCount() const;
  uint64_t ReadCount() const;
  SharedMemoryUse SharedMemory() const;
  HostPort RpcAddress() const;
  uint16_t SqlPort() const;
  const std::string& PgDataDirectory() const;
  Status ValidateConnection(const std::string& reported_directory) const;

 private:
  class Impl;
  explicit ReadonlyPgServer(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace yb::local_runtime
