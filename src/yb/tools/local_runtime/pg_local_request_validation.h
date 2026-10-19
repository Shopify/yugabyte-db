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

#include <unordered_set>
#include <vector>

#include "yb/common/pgsql_protocol.pb.h"
#include "yb/common/ql_type.h"
#include "yb/common/schema.h"
#include "yb/util/status_format.h"

namespace yb::local_runtime::detail {

inline Status CheckFields(
    const google::protobuf::Message& message, const std::unordered_set<std::string>& allowed) {
  SCHECK(
      message.GetReflection()->GetUnknownFields(message).empty(), NotSupported,
      "Unknown protobuf fields are not supported");
  std::vector<const google::protobuf::FieldDescriptor*> fields;
  message.GetReflection()->ListFields(message, &fields);
  for (const auto* field : fields) {
    SCHECK(
        allowed.contains(field->name()), NotSupported,
        "Local request prototype does not support field: " + field->name());
  }
  return Status::OK();
}

inline Status CheckValue(
    const ColumnSchema& column, const PgsqlExpressionPB& expression, bool key) {
  RETURN_NOT_OK(CheckFields(expression, {"value"}));
  SCHECK(expression.has_value(), NotSupported, "Only constants are supported");
  const auto& value = expression.value();
  if (value.value_case() == QLValuePB::VALUE_NOT_SET) {
    SCHECK(!key && column.is_nullable(), InvalidArgument, "NULL in a non-nullable column");
    return Status::OK();
  }
  const auto type = column.type()->main();
  SCHECK(
      (type == DataType::INT64 && value.has_int64_value()) ||
          (type == DataType::INT32 && value.has_int32_value()) ||
          (type == DataType::STRING && value.has_string_value()),
      InvalidArgument, "Value type does not match schema");
  return Status::OK();
}

inline Status CheckKeyValues(
    const Schema& schema, const google::protobuf::RepeatedPtrField<PgsqlExpressionPB>& values,
    size_t offset, size_t count) {
  SCHECK_EQ(values.size(), count, InvalidArgument, "Incorrect key component count");
  for (size_t i = 0; i < count; ++i) {
    RETURN_NOT_OK(CheckValue(schema.column(offset + i), values.Get(static_cast<int>(i)), true));
  }
  return Status::OK();
}

template <class Request>
Result<std::unordered_set<int32_t>> CheckReferences(const Schema& schema, const Request& request) {
  std::unordered_set<int32_t> references;
  auto add = [&](int32_t id) -> Status {
    SCHECK(
        schema.find_column_by_id(ColumnId(id)) >= 0, InvalidArgument, "Unknown column reference");
    SCHECK(references.insert(id).second, InvalidArgument, "Duplicate column reference");
    return Status::OK();
  };
  SCHECK(
      request.col_refs().empty() || request.column_refs().ids().empty(), InvalidArgument,
      "Mixed column reference formats");
  for (const auto& ref : request.col_refs()) {
    RETURN_NOT_OK(CheckFields(ref, {"column_id"}));
    SCHECK(ref.has_column_id(), InvalidArgument, "Missing column reference ID");
    RETURN_NOT_OK(add(ref.column_id()));
  }
  for (const auto id : request.column_refs().ids()) {
    RETURN_NOT_OK(add(id));
  }
  return references;
}

inline Status CheckColumns(
    const Schema& schema, const google::protobuf::RepeatedPtrField<PgsqlColumnValuePB>& values) {
  std::unordered_set<int32_t> assigned;
  for (const auto& column : values) {
    const auto index = schema.find_column_by_id(ColumnId(column.column_id()));
    SCHECK(
        index >= 0 && static_cast<size_t>(index) >= schema.num_key_columns(), InvalidArgument,
        "Invalid or primary-key assignment column");
    SCHECK(assigned.insert(column.column_id()).second, InvalidArgument, "Duplicate assignment");
    RETURN_NOT_OK(CheckValue(schema.column(index), column.expr(), false));
  }
  return Status::OK();
}

}  // namespace yb::local_runtime::detail
