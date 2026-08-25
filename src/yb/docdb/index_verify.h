// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>

#include "yb/common/hybrid_time.h"
#include "yb/docdb/docdb_fwd.h"
#include "yb/util/monotime.h"
#include "yb/util/slice.h"
#include "yb/util/status_fwd.h"

namespace yb::docdb {

struct VerifyUniqueIndexChunkOptions {
  // Empty = SeekToFirst. Encoded DocDB key.
  Slice start_key;
  // Empty = no upper bound. Encoded DocDB key (exclusive).
  Slice end_key;
  // Events at HT > verify_time are ignored so we only consider committed writes visible
  // at the caller's chosen point.
  HybridTime verify_time;
  // Work budget: stop scanning (at the next DocKey group boundary) once this deadline is
  // reached. CoarseTimePoint::max() = no deadline. Callers should derive this from the
  // RPC's client deadline minus a safety margin so the response (with a resume key in
  // verified_until) makes it back before the caller gives up and retries from scratch.
  CoarseTimePoint deadline = CoarseTimePoint::max();
  // Work budget: stop scanning (at the next DocKey group boundary) after roughly this
  // many raw entries have been examined. 0 = unlimited. Mainly for tests and for capping
  // per-RPC work independently of wall-clock.
  size_t max_entries = 0;
};

struct VerifyUniqueIndexChunkResult {
  // Encoded DocKey of a duplicated index entry, empty when no duplicate was detected.
  std::string duplicate_key_hint;
  // When the scan stopped early on a work budget (deadline or max_entries), the encoded
  // DocKey the next chunk must resume from (inclusive). Budget stops only happen at
  // DocKey group boundaries so the timeline algorithm never splits a group across
  // chunks. Empty when the whole requested range was consumed.
  std::string verified_until;
  // Scan statistics for measurement/logging.
  size_t entries_seen = 0;
  size_t insert_events = 0;
  size_t update_events = 0;
  size_t delete_events = 0;
};

// Scan a chunk of a unique-index tablet's regular RocksDB for entries whose live intervals
// overlap under the same DocKey. Uses the raw `CreateRocksDBIterator` primitive so retained
// delete markers are visible; requires `retain_delete_markers` to be true on the index
// (as PR 3 arranges when the deferred-uniqueness GFlag is set).
//
// The scan does not read intents. Callers must ensure `verify_time` <= a hybrid time at which
// the tablet's SafeTime has advanced past it, so every committed write with HT <= verify_time
// is already durable in the regular DB.
//
// PROTOTYPE ONLY. Requires the following layout flags to have been true *for every write
// in the verified window*, not merely at scan time. A write that landed while any of these
// was off is persisted in a form the classifier cannot distinguish from a fresh insert, so
// PR 5 must gate deferred uniqueness on these flags at write time (e.g. only take the
// deferred path when they were true at the moment the index entered indislive/indisready,
// and persist that fact on IndexInfoPB so it cannot be revoked by a mid-run flag flip):
//   - `ysql_enable_packed_row = true`: top-level entries must be packed rows (whole-row).
//     Unpacked-row liveness markers are rejected with NotSupported.
//   - `ysql_use_packed_row_v2 = true`: V1 packed rows have no room in their header for an
//     UPDATE marker, so an in-place PK-update rewrite is indistinguishable from a fresh
//     insert. V1 entries are rejected with NotSupported.
//   - `ysql_mark_update_packed_row = true`: without this, UPDATE-produced V2 packed rows
//     do not set the `kIsUpdateFlag` bit and again cannot be distinguished from inserts.
//     An unmarked V2 packed row observed during the scan is ambiguous -- may be a legitimate
//     insert or an update written before the flag was set -- so the classifier trusts the
//     bit and would false-positive if the flag was flipped mid-window.
Result<VerifyUniqueIndexChunkResult> VerifyUniqueIndexChunk(
    const DocDB& doc_db, const VerifyUniqueIndexChunkOptions& options);

} // namespace yb::docdb
