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

#include "yb/yql/pggate/pg_response_cache_key.h"

#include <limits>
#include <optional>

#include <google/protobuf/io/coded_stream.h>

#include "yb/common/pgsql_protocol.messages.h"
#include "yb/common/pgsql_protocol.pb.h"

#include "yb/gutil/casts.h"

#include "yb/util/memory/arena.h"
#include "yb/util/scope_exit.h"

namespace yb::pggate {
namespace {

using google::protobuf::io::CodedOutputStream;

auto TemporaryClearInsignificantFields(LWPgsqlReadRequestPB& req) {
  const auto stmt_id = req.has_stmt_id() ? std::optional(req.stmt_id()) : std::nullopt;
  req.clear_stmt_id();
  const auto metrics_capture =
      req.has_metrics_capture() ? std::optional(req.metrics_capture()) : std::nullopt;
  req.clear_metrics_capture();
  return ScopeExit([&req, stmt_id, metrics_capture] {
    if (stmt_id) {
      req.set_stmt_id(*stmt_id);
    }
    if (metrics_capture) {
      req.set_metrics_capture(*metrics_capture);
    }
  });
}

template<class PB>
uint8_t* WritePBWithSize(uint8_t* out, const PB* pb) {
  if (!pb) {
    return CodedOutputStream::WriteVarint32ToArray(0U, out);
  }
  out = CodedOutputStream::WriteVarint32ToArray(narrow_cast<uint32_t>(pb->SerializedSize()), out);
  return pb->SerializeToArray(out);
}

class VersionInfoWriter {
 public:
  explicit VersionInfoWriter(PgCatalogReadCacheKeyVersionInfo version_info)
      : version_info_(version_info) {}

  [[nodiscard]] size_t GetSize() const {
    return CodedOutputStream::VarintSize64(version_info_.version) + 1;
  }

  uint8_t* Write(uint8_t* out) const {
    constexpr auto kTrue = '1';
    constexpr auto kFalse = '0';
    out = CodedOutputStream::WriteRawToArray(
        version_info_.is_db_catalog_version_mode ? &kTrue : &kFalse, 1, out);
    return CodedOutputStream::WriteVarint64ToArray(version_info_.version, out);
  }

 private:
  PgCatalogReadCacheKeyVersionInfo version_info_;
};

} // namespace

std::string BuildCatalogReadCacheKey(
    ThreadSafeArena* arena,
    const ReadHybridTime& catalog_read_time,
    size_t num_ops,
    PgCatalogReadRequestProvider& read_request_provider,
    PgCatalogReadCacheKeyVersionInfo version_info) {
  constexpr auto kMaxFieldSize =
      CodedOutputStream::StaticVarintSize32<std::numeric_limits<uint32_t>::max()>::value;
  const VersionInfoWriter version_writer(version_info);
  auto total_size = version_writer.GetSize() + (num_ops + 1) * kMaxFieldSize;
  std::optional<LWReadHybridTimePB> read_time_pb;
  if (catalog_read_time) {
    read_time_pb.emplace(arena);
    catalog_read_time.ToPB(&*read_time_pb);
    total_size += read_time_pb->SerializedSize();
  }
  for (size_t i = 0; i != num_ops; ++i) {
    total_size += read_request_provider(i).SerializedSize();
  }

  std::string result;
  result.resize(total_size);
  auto* start = pointer_cast<uint8_t*>(result.data());
  auto* out = version_writer.Write(start);
  out = WritePBWithSize(out, read_time_pb ? &*read_time_pb : nullptr);
  for (size_t i = 0; i != num_ops; ++i) {
    auto& req = read_request_provider(i);
    auto fields_restorer = TemporaryClearInsignificantFields(req);
    out = WritePBWithSize(out, &req);
  }
  const auto actual_size = out - start;
  DCHECK_LE(actual_size, total_size);
  result.resize(actual_size);
  return result;
}

} // namespace yb::pggate
