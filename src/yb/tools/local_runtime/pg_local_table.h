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
// License for the specific language governing permissions and limitations under
// the License.

#pragma once

#include "yb/common/pgsql_protocol.pb.h"
#include "yb/common/column_id.h"
#include "yb/tablet/metadata.pb.h"
#include "yb/tserver/pg_client.pb.h"
#include "yb/util/result.h"

namespace yb::local_runtime {

Result<tablet::TableInfoPB> BuildLocalTable(const tserver::PgCreateTableRequestPB& req);
Status AlterLocalTable(
    const tserver::PgAlterTableRequestPB& req, tablet::TableInfoPB* metadata,
    ColumnId* next_column_id);

}  // namespace yb::local_runtime
