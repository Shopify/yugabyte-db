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

#pragma once

#include <string>

#include "yb/master/catalog_entity_base.h"
#include "yb/master/catalog_entity_info.pb.h"
#include "yb/master/sys_catalog.h"

namespace yb::master {

struct PersistentSchemaMigrationInfo : public Persistent<SysSchemaMigrationEntryPB> {};

struct SchemaMigrationInfoHelpers {
  static bool IsTerminal(SysSchemaMigrationEntryPB::State state) {
    return state == SysSchemaMigrationEntryPB::SUCCEEDED ||
           state == SysSchemaMigrationEntryPB::FAILED ||
           state == SysSchemaMigrationEntryPB::CANCELLED;
  }

  static bool IsTerminal(const SysSchemaMigrationEntryPB& pb) { return IsTerminal(pb.state()); }
};

// In-memory representation of a single online-schema-change migration job. The
// migration id (a server-generated UUID string) is the sys-catalog row key.
// See architecture/design/online-schema-changes-async-shadow-roadmap.md Section 0.
class SchemaMigrationInfo : public MetadataCowWrapper<PersistentSchemaMigrationInfo> {
 public:
  explicit SchemaMigrationInfo(std::string migration_id);

  const std::string& id() const override { return migration_id_; }

 private:
  // The ID field is used in the sys_catalog table row key.
  const std::string migration_id_;

  DISALLOW_COPY_AND_ASSIGN(SchemaMigrationInfo);
};

DECLARE_MULTI_INSTANCE_LOADER_CLASS(SchemaMigration, std::string, SysSchemaMigrationEntryPB);

}  // namespace yb::master
