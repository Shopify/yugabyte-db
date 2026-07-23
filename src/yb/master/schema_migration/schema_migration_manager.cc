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

#include "yb/master/schema_migration/schema_migration_manager.h"

#include "yb/master/catalog_manager.h"
#include "yb/master/master_types.pb.h"
#include "yb/master/sys_catalog.h"

#include "yb/server/clock.h"

#include "yb/util/flags.h"
#include "yb/util/status_format.h"

using namespace std::placeholders;

DEFINE_test_flag(bool, enable_schema_migration_admission, false,
    "When true, allows StartSchemaMigration to admit online-schema-change jobs. The executor is "
    "still a skeleton (NEW -> RUNNING -> SUCCEEDED) that does not modify the target table, so this "
    "is test/preview-only until the real copy/replay/cutover backend lands.");

DEFINE_test_flag(bool, pause_schema_migration_in_running, false,
    "When true, the skeleton schema-migration executor stops advancing jobs once they reach "
    "RUNNING, so tests can observe the RUNNING state and exercise failover/cancel.");

DEFINE_test_flag(bool, fail_schema_migration_in_running, false,
    "When true, the skeleton schema-migration executor moves RUNNING jobs to FAILED instead of "
    "SUCCEEDED, so tests can exercise terminal-error retention.");

namespace yb::master {

namespace {

// Execution phases within the RUNNING state. The executor advances one phase per
// tick and persists each transition, so the status/progress surface reflects a
// real pipeline. These map to the roadmap Section 2.7 state machine; the actual
// DocDB shadow-copy/replay/cutover work is not performed yet (no table is
// mutated) - this is the observable skeleton the real backend plugs into.
const char* kPhasePreflight = "PREFLIGHT";
const char* kPhaseShadowCreating = "SHADOW_CREATING";
const char* kPhaseCopying = "COPYING";
const char* kPhaseCutover = "CUTOVER";

// Returns the next phase after `phase`, or nullptr if `phase` is the last one
// (after which the job succeeds).
const char* NextPhase(const std::string& phase) {
  if (phase == kPhasePreflight) return kPhaseShadowCreating;
  if (phase == kPhaseShadowCreating) return kPhaseCopying;
  if (phase == kPhaseCopying) return kPhaseCutover;
  return nullptr;  // kPhaseCutover is terminal -> SUCCEEDED.
}

}  // namespace

SchemaMigrationManager::SchemaMigrationManager(
    CatalogManager* catalog_manager, SysCatalogTable* sys_catalog, server::Clock* clock)
    : catalog_manager_(catalog_manager), sys_catalog_(sys_catalog), clock_(clock) {}

SchemaMigrationInfoPtr SchemaMigrationManager::FindJobUnlocked(const std::string& migration_id) {
  return FindPtrOrNull(jobs_, migration_id);
}

Result<std::string> SchemaMigrationManager::StartSchemaMigration(
    SysSchemaMigrationEntryPB::Kind kind, uint32_t database_oid, uint32_t table_oid,
    uint32_t submitted_by, const std::string& submitted_ddl, const std::string& request_id,
    const LeaderEpoch& epoch) {
  SCHECK(
      FLAGS_TEST_enable_schema_migration_admission, NotSupported,
      "Online schema change migrations are not enabled");
  SCHECK(
      kind == SysSchemaMigrationEntryPB::ONLINE_TABLE_REWRITE, InvalidArgument,
      "Unsupported schema migration kind: $0", SysSchemaMigrationEntryPB::Kind_Name(kind));

  {
    // Lost-response idempotency: a retried submission with the same request_id
    // resolves to the existing job rather than starting a second one.
    std::lock_guard lock(mutex_);
    if (!request_id.empty()) {
      for (const auto& [id, job] : jobs_) {
        auto l = job->LockForRead();
        if (l->pb.request_id() == request_id) {
          return id;
        }
      }
    }
  }

  const auto migration_id = catalog_manager_->GenerateId(SysRowEntryType::SCHEMA_MIGRATION);
  auto job = std::make_shared<SchemaMigrationInfo>(migration_id);
  const auto now = clock_->Now().ToUint64();
  {
    auto l = job->LockForWrite();
    auto& pb = l.mutable_data()->pb;
    pb.set_kind(kind);
    pb.set_state(SysSchemaMigrationEntryPB::NEW);
    pb.set_state_epoch(epoch.leader_term);
    pb.set_database_oid(database_oid);
    pb.set_table_oid(table_oid);
    pb.set_submitted_by(submitted_by);
    pb.set_submitted_ddl(submitted_ddl);
    pb.set_created_ht(now);
    pb.set_updated_ht(now);
    if (!request_id.empty()) {
      pb.set_request_id(request_id);
    }
    // Durably admit the job BEFORE returning the id, so the id is always
    // resolvable by a subsequent status query (matches snapshot creation).
    RETURN_NOT_OK(sys_catalog_->Upsert(epoch, job.get()));
    l.Commit();
  }

  {
    std::lock_guard lock(mutex_);
    jobs_[migration_id] = job;
  }
  LOG(INFO) << "Admitted schema migration " << migration_id << " for table oid " << table_oid;
  return migration_id;
}

Result<SysSchemaMigrationEntryPB> SchemaMigrationManager::GetSchemaMigration(
    const std::string& migration_id) {
  std::lock_guard lock(mutex_);
  auto job = FindJobUnlocked(migration_id);
  SCHECK(job != nullptr, NotFound, "Schema migration $0 not found", migration_id);
  return job->LockForRead()->pb;
}

std::vector<std::pair<std::string, SysSchemaMigrationEntryPB>>
SchemaMigrationManager::ListSchemaMigrations(
    std::optional<SysSchemaMigrationEntryPB::State> state_filter) {
  std::vector<std::pair<std::string, SysSchemaMigrationEntryPB>> result;
  std::lock_guard lock(mutex_);
  for (const auto& [id, job] : jobs_) {
    auto pb = job->LockForRead()->pb;
    if (state_filter && pb.state() != *state_filter) {
      continue;
    }
    result.emplace_back(id, std::move(pb));
  }
  return result;
}

Status SchemaMigrationManager::CancelSchemaMigration(
    const std::string& migration_id, const LeaderEpoch& epoch) {
  SchemaMigrationInfoPtr job;
  {
    std::lock_guard lock(mutex_);
    job = FindJobUnlocked(migration_id);
  }
  SCHECK(job != nullptr, NotFound, "Schema migration $0 not found", migration_id);

  auto l = job->LockForWrite();
  auto& pb = l.mutable_data()->pb;
  if (SchemaMigrationInfoHelpers::IsTerminal(pb.state())) {
    return STATUS_FORMAT(
        IllegalState, "Schema migration $0 is already terminal ($1)", migration_id,
        SysSchemaMigrationEntryPB::State_Name(pb.state()));
  }
  // Bump the epoch so any in-flight worker callback keyed on the old epoch is
  // fenced. Persist CANCELLING before signalling, so a failover between abort
  // and persistence does not resume the job.
  pb.set_state(SysSchemaMigrationEntryPB::CANCELLING);
  pb.set_state_epoch(epoch.leader_term);
  pb.set_updated_ht(clock_->Now().ToUint64());
  RETURN_NOT_OK(sys_catalog_->Upsert(epoch, job.get()));
  l.Commit();
  LOG(INFO) << "Requested cancellation of schema migration " << migration_id;
  return Status::OK();
}

Status SchemaMigrationManager::ClearAndRunLoaders(const LeaderEpoch& epoch) {
  {
    std::lock_guard lock(mutex_);
    jobs_.clear();
  }
  return sys_catalog_->Load<SchemaMigrationLoader, SysSchemaMigrationEntryPB>(
      "Schema migrations",
      std::function<Status(const std::string&, const SysSchemaMigrationEntryPB&)>(
          std::bind(&SchemaMigrationManager::LoadSchemaMigration, this, epoch, _1, _2)));
}

Status SchemaMigrationManager::LoadSchemaMigration(
    const LeaderEpoch& epoch, const std::string& id, const SysSchemaMigrationEntryPB& metadata) {
  auto job = std::make_shared<SchemaMigrationInfo>(id);
  job->Load(metadata);

  // Bump state_epoch for non-terminal jobs so stale-epoch worker callbacks from
  // the previous leader term are fenced. Terminal jobs are retained as-is for
  // lookup.
  if (!SchemaMigrationInfoHelpers::IsTerminal(metadata.state())) {
    auto l = job->LockForWrite();
    l.mutable_data()->pb.set_state_epoch(epoch.leader_term);
    RETURN_NOT_OK(sys_catalog_->Upsert(epoch, job.get()));
    l.Commit();
  }

  std::lock_guard lock(mutex_);
  jobs_[id] = job;
  return Status::OK();
}

void SchemaMigrationManager::SysCatalogLoaded(const LeaderEpoch& epoch) {
  // Non-terminal jobs resume from the periodic Run() tick; nothing eager needed
  // for the skeleton executor.
  VLOG(1) << "Schema migration manager loaded for leader term " << epoch.leader_term;
}

Result<bool> SchemaMigrationManager::AdvanceJob(
    const SchemaMigrationInfoPtr& job, const LeaderEpoch& epoch) {
  auto l = job->LockForWrite();
  auto& pb = l.mutable_data()->pb;
  const auto now = clock_->Now().ToUint64();
  switch (pb.state()) {
    case SysSchemaMigrationEntryPB::NEW: {
      pb.set_state(SysSchemaMigrationEntryPB::RUNNING);
      pb.set_phase(kPhasePreflight);
      pb.set_updated_ht(now);
      RETURN_NOT_OK(sys_catalog_->Upsert(epoch, job.get()));
      l.Commit();
      return true;
    }
    case SysSchemaMigrationEntryPB::RUNNING: {
      if (FLAGS_TEST_pause_schema_migration_in_running) {
        return false;
      }
      if (FLAGS_TEST_fail_schema_migration_in_running) {
        pb.set_state(SysSchemaMigrationEntryPB::FAILED);
        StatusToPB(
            STATUS(NotSupported, "schema migration executor not implemented (test failure)"),
            pb.mutable_terminal_error());
        pb.set_updated_ht(now);
        pb.set_completed_ht(now);
        RETURN_NOT_OK(sys_catalog_->Upsert(epoch, job.get()));
        l.Commit();
        return true;
      }
      // Advance one execution phase per tick. No DocDB storage work is performed
      // yet (no table is mutated); this drives the observable pipeline that the
      // real shadow-create / copy / replay / cutover backend will implement.
      const char* next = NextPhase(pb.phase());
      if (next != nullptr) {
        pb.set_phase(next);
        pb.set_updated_ht(now);
        RETURN_NOT_OK(sys_catalog_->Upsert(epoch, job.get()));
        l.Commit();
        return true;
      }
      // Reached the end of the phase pipeline.
      pb.set_state(SysSchemaMigrationEntryPB::SUCCEEDED);
      pb.set_updated_ht(now);
      pb.set_completed_ht(now);
      RETURN_NOT_OK(sys_catalog_->Upsert(epoch, job.get()));
      l.Commit();
      return true;
    }
    case SysSchemaMigrationEntryPB::CANCELLING: {
      pb.set_state(SysSchemaMigrationEntryPB::CANCELLED);
      pb.set_updated_ht(now);
      pb.set_completed_ht(now);
      RETURN_NOT_OK(sys_catalog_->Upsert(epoch, job.get()));
      l.Commit();
      return true;
    }
    case SysSchemaMigrationEntryPB::SUCCEEDED: FALLTHROUGH_INTENDED;
    case SysSchemaMigrationEntryPB::FAILED: FALLTHROUGH_INTENDED;
    case SysSchemaMigrationEntryPB::CANCELLED: FALLTHROUGH_INTENDED;
    case SysSchemaMigrationEntryPB::UNKNOWN_STATE:
      return false;
  }
  return false;
}

Status SchemaMigrationManager::Run(const LeaderEpoch& epoch) {
  std::vector<SchemaMigrationInfoPtr> pending;
  {
    std::lock_guard lock(mutex_);
    for (const auto& [id, job] : jobs_) {
      if (!SchemaMigrationInfoHelpers::IsTerminal(job->LockForRead()->pb.state())) {
        pending.push_back(job);
      }
    }
  }
  for (const auto& job : pending) {
    RETURN_NOT_OK(AdvanceJob(job, epoch));
  }
  return Status::OK();
}

}  // namespace yb::master
