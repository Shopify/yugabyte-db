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

#include "yb/cdc/cdc_row_filter.h"
#include "yb/util/result.h"
#include "yb/util/status_macros.h"

#include <algorithm>

#include "yb/util/logging.h"

namespace yb {
namespace cdc {

CDCRowFilter::CDCRowFilter(const CDCRowFilterPB& filter_pb)
    : filter_pb_(filter_pb) {
  if (!IsEmpty()) {
    VLOG(1) << "CDCRowFilter created for column '" << filter_pb_.column_name()
            << "' with " << filter_pb_.values_size() << " values";
  }
}

bool CDCRowFilter::IsEmpty() const {
  return !filter_pb_.has_column_name() || filter_pb_.values_size() == 0;
}

Result<bool> CDCRowFilter::Matches(const RowMessage& row_message) const {
  // Always include transaction control records
  if (row_message.op() == RowMessage::BEGIN ||
      row_message.op() == RowMessage::COMMIT ||
      row_message.op() == RowMessage::SAFEPOINT) {
    return true;
  }

  // Always include DDL records
  if (row_message.op() == RowMessage::DDL ||
      row_message.op() == RowMessage::TRUNCATE) {
    return true;
  }

  // Empty filter matches all data rows
  if (IsEmpty()) {
    return true;
  }

  // For data rows (INSERT/UPDATE/DELETE/READ), apply the filter
  // Filter is based on the new_tuple (the current/new state of the row)
  if (row_message.new_tuple_size() == 0) {
    // No new_tuple data, skip this record (shouldn't happen for data rows)
    VLOG(2) << "Skipping record with no new_tuple data for op: " << row_message.op();
    return false;
  }

  // Extract the column value
  auto column_value = VERIFY_RESULT(GetColumnInt64Value(
      filter_pb_.column_name(), row_message.new_tuple()));

  // Check if value is in the filter list
  bool matches = ValueInList(column_value);

  VLOG(3) << "Filter evaluation for column '" << filter_pb_.column_name()
          << "' value=" << column_value << " matches=" << matches;

  return matches;
}

Result<int64_t> CDCRowFilter::GetColumnInt64Value(
    const std::string& column_name,
    const google::protobuf::RepeatedPtrField<DatumMessagePB>& tuple) const {

  // Find the column in the tuple
  for (const auto& datum : tuple) {
    if (datum.column_name() == column_name) {
      // Extract int64 value based on the datum type
      if (datum.has_datum_int32()) {
        return datum.datum_int32();
      }
      if (datum.has_datum_int64()) {
        return datum.datum_int64();
      }
      if (datum.has_datum_bool()) {
        // Convert bool to int64 (false=0, true=1)
        return datum.datum_bool() ? 1 : 0;
      }
      if (datum.has_datum_missing()) {
        return STATUS_FORMAT(
            InvalidArgument,
            "Column '$0' has NULL value, cannot filter on NULL",
            column_name);
      }
      // Unsupported type for int64 conversion
      return STATUS_FORMAT(
          InvalidArgument,
          "Column '$0' has unsupported type for int64 filtering (must be int32/int64/bool)",
          column_name);
    }
  }

  // Column not found in tuple
  return STATUS_FORMAT(
      NotFound,
      "Column '$0' not found in record tuple",
      column_name);
}

bool CDCRowFilter::ValueInList(int64_t value) const {
  // Check if value exists in the filter's value list
  for (int i = 0; i < filter_pb_.values_size(); ++i) {
    if (filter_pb_.values(i) == value) {
      return true;
    }
  }
  return false;
}

}  // namespace cdc
}  // namespace yb
