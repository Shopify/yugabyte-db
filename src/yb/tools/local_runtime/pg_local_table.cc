// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

#include "yb/tools/local_runtime/pg_local_table.h"

#include <limits>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include "yb/client/schema.h"
#include "yb/common/entity_ids.h"
#include "yb/common/pg_system_attr.h"
#include "yb/common/pg_types.h"
#include "yb/common/pgsql_protocol.pb.h"
#include "yb/common/ql_type.h"
#include "yb/common/schema.h"
#include "yb/common/schema_pbutil.h"
#include "yb/common/wire_protocol.h"
#include "yb/dockv/partition.h"

DECLARE_int32(ysql_num_shards_per_tserver);
DECLARE_string(ysql_yb_default_replica_identity);

namespace yb::local_runtime {
namespace {
using client::internal::GetSchema;

Status Validate(const tserver::PgCreateTableRequestPB& req) {
  if (req.table_name().empty() || req.database_name().empty() ||
      req.table_id().database_oid() == 0 || req.table_id().object_oid() < kPgFirstNormalObjectId) {
    return STATUS(InvalidArgument, "Invalid application table identity");
  }
  if (req.GetReflection()->GetUnknownFields(req).field_count() != 0) {
    return STATUS(InvalidArgument, "Unknown fields are not allowed");
  }
  if (!req.use_transaction() || !req.use_regular_transaction_block()) {
    return STATUS(InvalidArgument, "Transactional table creation is required");
  }
  const auto old = req.old_relfilenode_oid().object_oid();
  const auto logical = req.pg_table_oid().object_oid();
  if (old) {
    SCHECK(
        old >= kPgFirstNormalObjectId && old != req.table_id().object_oid() &&
            req.old_relfilenode_oid().database_oid() == req.table_id().database_oid() &&
            logical >= kPgFirstNormalObjectId &&
            req.pg_table_oid().database_oid() == req.table_id().database_oid(),
        InvalidArgument, "Invalid transactional rewrite identity");
  } else if (logical != 0 && logical != req.table_id().object_oid()) {
    // Reindexing an index during a table rewrite sends its new relfilenode without the old one
    // when the index's table properties are not cached (YbIndexSetNewRelfileNode); master
    // accepts that. Table rewrites always name their source.
    SCHECK(
        req.base_table_id().object_oid() != 0 && logical >= kPgFirstNormalObjectId &&
            req.pg_table_oid().database_oid() == req.table_id().database_oid(),
        NotSupported, "A distinct logical table OID requires a rewrite source");
  }
  if (req.is_shared_table() || req.is_pg_catalog_table() || req.is_colocated_via_database() ||
      req.has_colocation_id() || req.tablegroup_oid().object_oid() != 0 ||
      req.tablespace_oid().object_oid() != 0 || req.is_matview() || req.is_truncate() ||
      req.has_vector_idx_options() || req.split_bounds_size() > 0) {
    return STATUS(NotSupported, "Unsupported local table request");
  }
  const bool is_index = req.base_table_id().object_oid() != 0;
  SCHECK(
      !is_index || (req.skip_index_backfill() &&
                    req.base_table_id().database_oid() == req.table_id().database_oid()),
      NotSupported, "Only foreground indexes in the same database are supported");
  SCHECK(
      is_index || (!req.is_unique_index() && !req.skip_index_backfill()), InvalidArgument,
      "Index options require an indexed table");
  if (req.create_columns_size() < 1 || req.create_columns_size() > 65536 ||
      req.num_tablets() < -1 || req.num_tablets() > 65536) {
    return STATUS(InvalidArgument, "Invalid column count");
  }
  return Status::OK();
}

bool AllowedType(int32_t t) {
  switch (static_cast<DataType>(t)) {
    case DataType::INT8:
    case DataType::INT16:
    case DataType::BOOL:
    case DataType::FLOAT:
    case DataType::DOUBLE:
    case DataType::DECIMAL:
    case DataType::INT32:
    case DataType::INT64:
    case DataType::STRING:
    case DataType::BINARY:
      return true;
    default:
      return false;
  }
}
}  // namespace

Status AlterLocalTable(
    const tserver::PgAlterTableRequestPB& req, tablet::TableInfoPB* metadata,
    ColumnId* next_column_id) {
  const auto valid_name = [](const std::string& name) {
    return !name.empty() && name.size() <= 63 && name.find('\0') == std::string::npos;
  };
  // Reflection also rejects unknown fields nested in column/default/transaction messages.
  const auto known_fields = [](const auto& self, const google::protobuf::Message& message) -> bool {
    const auto* reflection = message.GetReflection();
    if (!reflection->GetUnknownFields(message).empty()) return false;
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    reflection->ListFields(message, &fields);
    for (const auto* field : fields) {
      if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) continue;
      if (field->is_repeated()) {
        for (int i = 0; i < reflection->FieldSize(message, field); ++i)
          if (!self(self, reflection->GetRepeatedMessage(message, field, i))) return false;
      } else if (!self(self, reflection->GetMessage(message, field))) {
        return false;
      }
    }
    return true;
  };
  SCHECK(known_fields(known_fields, req), InvalidArgument, "Unknown ALTER fields");
  SCHECK(
      req.use_transaction() && req.use_regular_transaction_block(), InvalidArgument,
      "Transactional ALTER is required");
  SCHECK(
      req.table_id().database_oid() != 0 && req.table_id().object_oid() >= kPgFirstNormalObjectId,
      InvalidArgument, "Invalid application table identity");
  SCHECK(!req.has_replica_identity(), NotSupported, "Replica identity changes are unsupported");
  const bool columns_changed =
      req.add_columns_size() || req.drop_columns_size() || req.rename_columns_size();
  SCHECK(
      !metadata->has_index_info() || !columns_changed, NotSupported,
      "Index column changes are unsupported");
  Schema schema;
  RETURN_NOT_OK(SchemaFromPB(metadata->schema(), &schema));
  SchemaBuilder builder(schema);
  SCHECK(*next_column_id >= builder.next_column_id(), Corruption, "Invalid column ID watermark");
  builder.set_next_column_id(*next_column_id);
  const auto nonkey = [&](const std::string& name) -> Status {
    const auto current = builder.Build();
    const auto index = current.find_column(name);
    SCHECK(index >= 0, NotFound, "Unknown column");
    SCHECK(
        static_cast<size_t>(index) >= current.num_key_columns(), NotSupported,
        "Key column changes are unsupported");
    const auto id = current.column_id(index);
    for (const auto& secondary : metadata->indexes()) {
      for (const auto& column : secondary.columns()) {
        SCHECK(
            column.indexed_column_id() != static_cast<uint32_t>(id.rep()), NotSupported,
            "Indexed column changes are unsupported");
      }
    }
    return Status::OK();
  };
  for (const auto& column : req.add_columns()) {
    SCHECK(
        valid_name(column.attr_name()) && column.attr_num() > 0 &&
            AllowedType(column.attr_ybtype()),
        InvalidArgument, "Invalid added column");
    SCHECK(
        !column.is_hash() && !column.is_range() && column.sorting_type() == 0, NotSupported,
        "Adding key columns is unsupported");
    const auto current = builder.Build();
    SCHECK_LT(current.num_columns(), 65536, InvalidArgument, "Too many columns");
    for (const auto& existing : current.columns()) {
      SCHECK_NE(existing.order(), column.attr_num(), InvalidArgument, "Duplicate PG attribute");
    }
    SCHECK_LT(
        builder.next_column_id().rep(), std::numeric_limits<int32_t>::max() - 1, InvalidArgument,
        "Column IDs exhausted");
    const auto& missing = column.attr_missing_val();
    bool valid_missing = missing.value_case() == QLValuePB::VALUE_NOT_SET;
    switch (static_cast<DataType>(column.attr_ybtype())) {
      case DataType::INT8:
        valid_missing |= missing.has_int8_value();
        break;
      case DataType::INT16:
        valid_missing |= missing.has_int16_value();
        break;
      case DataType::INT32:
        valid_missing |= missing.has_int32_value();
        break;
      case DataType::INT64:
        valid_missing |= missing.has_int64_value();
        break;
      case DataType::BOOL:
        valid_missing |= missing.has_bool_value();
        break;
      case DataType::FLOAT:
        valid_missing |= missing.has_float_value();
        break;
      case DataType::DOUBLE:
        valid_missing |= missing.has_double_value();
        break;
      case DataType::DECIMAL:
        valid_missing |= missing.has_decimal_value();
        break;
      case DataType::STRING:
        valid_missing |= missing.has_string_value();
        break;
      case DataType::BINARY:
        valid_missing |= missing.has_binary_value();
        break;
      default:
        break;
    }
    SCHECK(valid_missing, InvalidArgument, "Missing value has the wrong native type");
    RETURN_NOT_OK(builder.AddColumn(ColumnSchema(
        column.attr_name(), QLType::Create(static_cast<DataType>(column.attr_ybtype())),
        ColumnKind::VALUE, Nullable::kTrue, false, false, column.attr_num(), column.attr_pgoid(),
        false, missing)));
  }
  for (const auto& column : req.rename_columns()) {
    SCHECK(valid_name(column.new_name()), InvalidArgument, "Invalid column name");
    RETURN_NOT_OK(nonkey(column.old_name()));
    RETURN_NOT_OK(builder.RenameColumn(column.old_name(), column.new_name()));
  }
  for (const auto& name : req.drop_columns()) {
    RETURN_NOT_OK(nonkey(name));
    RETURN_NOT_OK(builder.RemoveColumn(name));
  }
  if (!req.rename_table().table_name().empty()) {
    SCHECK(valid_name(req.rename_table().table_name()), InvalidArgument, "Invalid table name");
    metadata->set_table_name(req.rename_table().table_name());
  }
  if (!req.rename_table().schema_name().empty()) {
    SCHECK(valid_name(req.rename_table().schema_name()), InvalidArgument, "Invalid schema name");
    builder.set_pgschema_name(req.rename_table().schema_name());
  }
  SchemaToPB(builder.Build(), metadata->mutable_schema());
  *next_column_id = builder.next_column_id();
  return Status::OK();
}

Result<tablet::TableInfoPB> BuildLocalTable(const tserver::PgCreateTableRequestPB& req) {
  RETURN_NOT_OK(Validate(req));
  client::YBSchemaBuilder builder;
  bool has_hash = false;
  bool has_range = false;
  bool index_columns_added = false;
  const auto add_index_columns = [&] {
    if (index_columns_added || req.base_table_id().object_oid() == 0) return;
    // Match PgCreateTable::EnsureYBbasectidColumnCreated, including unique NULL semantics.
    if (req.is_unique_index()) {
      builder.AddColumn("ybuniqueidxkeysuffix")
          ->Type(QLType::Create(DataType::BINARY))
          ->Order(std::to_underlying(PgSystemAttrNum::kYBUniqueIdxKeySuffix))
          ->PrimaryKey();
    }
    auto* base_id = builder.AddColumn("ybidxbasectid")
                        ->Type(QLType::Create(DataType::BINARY))
                        ->Order(std::to_underlying(PgSystemAttrNum::kYBIdxBaseTupleId));
    if (!req.is_unique_index()) base_id->PrimaryKey();
    has_range = true;
    index_columns_added = true;
  };
  for (const auto& c : req.create_columns()) {
    if (c.attr_name().empty() || c.attr_num() == 0 || !AllowedType(c.attr_ybtype())) {
      return STATUS(InvalidArgument, "Invalid column definition");
    }
    if (!c.is_hash() && !c.is_range()) add_index_columns();
    auto* col = builder.AddColumn(c.attr_name())
                    ->Type(QLType::Create(static_cast<DataType>(c.attr_ybtype())));
    col->Order(c.attr_num())->PgTypeOid(c.attr_pgoid());
    if (c.is_hash()) {
      if (has_range || c.sorting_type() != 0) return STATUS(InvalidArgument, "Invalid hash key");
      col->HashPrimaryKey();
      has_hash = true;
    } else if (c.is_range()) {
      if (c.sorting_type() < 0 || c.sorting_type() > 4) {
        return STATUS(InvalidArgument, "Unknown sorting type");
      }
      col->PrimaryKey(static_cast<SortingType>(c.sorting_type()));
      has_range = true;
    } else if (c.sorting_type() != 0) {
      return STATUS(InvalidArgument, "Sorting requires a key column");
    }
    if (!c.GetReflection()->GetUnknownFields(c).empty())
      return STATUS(NotSupported, "Unknown column fields");
    if (c.has_attr_missing_val()) return STATUS(InvalidArgument, "Missing values are unsupported");
  }
  if (!has_hash && !has_range) return STATUS(InvalidArgument, "Key columns are required");
  if (has_range && !has_hash && req.num_tablets() > 1) {
    return STATUS(InvalidArgument, "Range-only tables must be single-tablet");
  }
  add_index_columns();
  client::YBSchema yb_schema;
  RETURN_NOT_OK(builder.Build(&yb_schema));
  auto schema = GetSchema(yb_schema);
  if (!schema.has_column_ids()) schema.InitColumnIdsByDefault();
  auto properties = schema.table_properties();
  properties.SetTransactional(true);
  // As on a one-tserver universe when ysql_num_shards_per_tserver is set; 4 otherwise.
  const int32_t default_hash_tablets =
      FLAGS_ysql_num_shards_per_tserver > 0 ? FLAGS_ysql_num_shards_per_tserver : 4;
  properties.SetNumTablets(
      req.num_tablets() > 0 ? req.num_tablets() : (has_hash ? default_hash_tablets : 1));
  PgReplicaIdentity identity;
  SCHECK(
      PgReplicaIdentity_Parse(FLAGS_ysql_yb_default_replica_identity, &identity), InvalidArgument,
      "Invalid default replica identity");
  properties.SetReplicaIdentity(identity);
  *schema.mutable_table_properties() = properties;
  schema.SetSchemaName(req.schema_name());
  tablet::TableInfoPB meta;
  meta.set_table_id(PgObjectId::GetYbTableIdFromPB(req.table_id()));
  meta.set_namespace_id(GetPgsqlNamespaceId(req.table_id().database_oid()));
  meta.set_namespace_name(req.database_name());
  meta.set_table_name(req.table_name());
  meta.set_table_type(TableType::PGSQL_TABLE_TYPE);
  meta.set_schema_version(0);
  if (req.pg_table_oid().object_oid())
    meta.set_pg_table_id(PgObjectId::GetYbTableIdFromPB(req.pg_table_oid()));
  SchemaToPB(schema, meta.mutable_schema());
  if (req.base_table_id().object_oid() != 0) {
    auto* index = meta.mutable_index_info();
    index->set_table_id(meta.table_id());
    index->set_indexed_table_id(PgObjectId::GetYbTableIdFromPB(req.base_table_id()));
    index->set_version(0);
    index->set_is_local(false);
    index->set_is_unique(req.is_unique_index());
  }
  auto* p = meta.mutable_partition_schema();
  if (has_hash) p->set_hash_schema(PartitionSchemaPB::PGSQL_HASH_SCHEMA);
  if (has_range && !has_hash) {
    auto* r = p->mutable_range_schema();
    for (size_t i = 0; i < schema.num_key_columns(); ++i)
      r->add_columns()->set_id(schema.column_id(i));
  }
  dockv::PartitionSchema partition;
  RETURN_NOT_OK(dockv::PartitionSchema::FromPB(*p, schema, &partition));
  partition.ToPB(p);
  return meta;
}

}  // namespace yb::local_runtime
