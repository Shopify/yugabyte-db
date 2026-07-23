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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "yb/master/leader_epoch.h"
#include "yb/master/master_fwd.h"
#include "yb/master/schema_migration/schema_migration_entity.h"

#include "yb/util/status_fwd.h"

namespace yb::server {
class Clock;
}

namespace yb::master {

// Master-owned tracker for online-schema-change migration jobs. See
// architecture/design/online-schema-changes-async-shadow-roadmap.md Section 0.
//
// This is the durable, generic state substrate: it admits a job (generating a
// server-owned UUID and replicating the initial row before returning it),
// persists every lifecycle transition, retains terminal jobs for lookup, and
// reloads non-terminal jobs after master-leader failover.
//
// The execution "engine" here is intentionally a skeleton: it advances
// NEW -> RUNNING -> SUCCEEDED without touching the target table, so the durable
// contract and observability surface can be exercised on a test cluster before
// the real copy/replay/cutover backend exists. Job admission is gated behind a
// test flag until that backend lands.
class SchemaMigrationManager {
 public:
  SchemaMigrationManager(
      CatalogManager* catalog_manager, SysCatalogTable* sys_catalog, server::Clock* clock);

  // Durably admit a new migration job and return its server-generated id.
  // The row is replicated before the id is returned. If request_id is set and
  // matches an existing job, that job's id is returned instead of creating a
  // second one (lost-response idempotency).
  Result<std::string> StartSchemaMigration(
      SysSchemaMigrationEntryPB::Kind kind, uint32_t database_oid, uint32_t table_oid,
      uint32_t submitted_by, const std::string& submitted_ddl, const std::string& request_id,
      const LeaderEpoch& epoch);

  // Look up a single job by id.
  Result<SysSchemaMigrationEntryPB> GetSchemaMigration(const std::string& migration_id);

  // List all jobs (optionally filtered by state); pairs of (id, entry).
  std::vector<std::pair<std::string, SysSchemaMigrationEntryPB>> ListSchemaMigrations(
      std::optional<SysSchemaMigrationEntryPB::State> state_filter);

  // Request cancellation of a non-terminal job.
  Status CancelSchemaMigration(const std::string& migration_id, const LeaderEpoch& epoch);

  // Sys-catalog load hook: clears and reloads all jobs from sys.catalog.
  Status ClearAndRunLoaders(const LeaderEpoch& epoch);

  // Post-load hook: called once the full sys-catalog is loaded on a new leader.
  // Resumes non-terminal jobs.
  void SysCatalogLoaded(const LeaderEpoch& epoch);

  // Periodic state-machine tick, driven from the catalog-manager bg loop.
  Status Run(const LeaderEpoch& epoch);

 private:
  Status LoadSchemaMigration(
      const LeaderEpoch& epoch, const std::string& id, const SysSchemaMigrationEntryPB& metadata);

  // Advance one job's state machine by one step; returns true if it mutated.
  Result<bool> AdvanceJob(const SchemaMigrationInfoPtr& job, const LeaderEpoch& epoch);

  // Perform the side-effecting work for `phase` (called without holding the job
  // lock). SHADOW_CREATING creates the hidden shadow generation; other phases
  // are currently no-ops. On success any results are persisted onto the job.
  Status PerformPhaseWork(
      const SchemaMigrationInfoPtr& job, const std::string& phase, const LeaderEpoch& epoch);

  SchemaMigrationInfoPtr FindJobUnlocked(const std::string& migration_id) REQUIRES(mutex_);

  CatalogManager* const catalog_manager_;
  SysCatalogTable* const sys_catalog_;
  server::Clock* const clock_;

  mutable std::mutex mutex_;
  // migration_id -> job.
  std::unordered_map<std::string, SchemaMigrationInfoPtr> jobs_ GUARDED_BY(mutex_);

  DISALLOW_COPY_AND_ASSIGN(SchemaMigrationManager);
};

}  // namespace yb::master
