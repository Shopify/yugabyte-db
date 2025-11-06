// Copyright (c) YugaByte, Inc.
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

#pragma once

#include "yb/cdc/cdc_service.pb.h"
#include "yb/common/common.pb.h"
#include "yb/util/status.h"

namespace yb {
namespace cdc {

// Evaluates CDC row filters against RowMessage records.
// Filters are applied only to data rows (INSERT/UPDATE/DELETE).
// Transaction markers (BEGIN/COMMIT/SAFEPOINT) and DDL records are always included.
class CDCRowFilter {
 public:
  // Creates a filter from the proto definition.
  // If filter_pb is empty/invalid, all records will match.
  explicit CDCRowFilter(const CDCRowFilterPB& filter_pb);

  // Returns true if the record should be included in the response.
  // Always returns true for:
  //   - Transaction control records (BEGIN/COMMIT/SAFEPOINT)
  //   - DDL records (DDL/TRUNCATE)
  //   - Records when filter is empty
  // For data rows (INSERT/UPDATE/DELETE), returns true if the specified column
  // value is in the filter's value list.
  Result<bool> Matches(const RowMessage& row_message) const;

  // Returns true if the filter is empty (no filtering configured).
  bool IsEmpty() const;

 private:
  // Extracts the int64 value of the specified column from a tuple.
  // Returns Status::NotFound if column doesn't exist.
  // Returns Status::InvalidArgument if column type is not compatible with int64.
  Result<int64_t> GetColumnInt64Value(
      const std::string& column_name,
      const google::protobuf::RepeatedPtrField<DatumMessagePB>& tuple) const;

  // Returns true if the value is in the filter's value list.
  bool ValueInList(int64_t value) const;

  const CDCRowFilterPB filter_pb_;
};

}  // namespace cdc
}  // namespace yb
