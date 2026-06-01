//--------------------------------------------------------------------------------------------------
// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
//--------------------------------------------------------------------------------------------------

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "yb/common/pgsql_protocol.fwd.h"
#include "yb/common/read_hybrid_time.h"

#include "yb/util/lw_function.h"
#include "yb/util/memory/arena_fwd.h"

namespace yb::pggate {

struct PgCatalogReadCacheKeyVersionInfo {
  uint64_t version;
  bool is_db_catalog_version_mode;
};

using PgCatalogReadRequestProvider = LWFunction<LWPgsqlReadRequestPB&(size_t)>;

[[nodiscard]] std::string BuildCatalogReadCacheKey(
    ThreadSafeArena* arena,
    const ReadHybridTime& catalog_read_time,
    size_t num_ops,
    PgCatalogReadRequestProvider& read_request_provider,
    PgCatalogReadCacheKeyVersionInfo version_info);

} // namespace yb::pggate
