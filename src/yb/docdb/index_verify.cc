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

#include "yb/docdb/index_verify.h"

#include "yb/common/doc_hybrid_time.h"
#include "yb/docdb/bounded_rocksdb_iterator.h"
#include "yb/docdb/docdb.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/docdb/key_bounds.h"
#include "yb/dockv/doc_key.h"
#include "yb/dockv/key_bytes.h"
#include "yb/dockv/key_entry_value.h"
#include "yb/dockv/packed_row.h"
#include "yb/dockv/value.h"
#include "yb/dockv/value_type.h"
#include "yb/util/fast_varint.h"
#include "yb/util/logging.h"
#include "yb/util/result.h"

namespace yb::docdb {

namespace {

using dockv::DocKey;
using dockv::DocKeyPart;
using dockv::KeyBytes;
using dockv::KeyEntryValue;

// The encoded key bytes for the liveness column subkey. YSQL unpacked-row inserts write a single
// entry under [DocKey]/[kLivenessColumn] to record row existence. The deferred-uniqueness scan
// does not support that layout (see header) but recognises it in order to reject early with a
// clear error.
const std::string& LivenessColumnEncoded() {
  static const std::string bytes = [] {
    KeyBytes buf;
    KeyEntryValue::kLivenessColumn.AppendToKey(&buf);
    return buf.ToStringBuffer();
  }();
  return bytes;
}

enum class RowEvent {
  kIgnore,           // Per-column entry or unrecognized value shape.
  kInsertPacked,     // V2 packed row with UPDATE flag clear: fresh insert of a logical row.
  kUpdatePacked,     // V2 packed row with UPDATE flag set: in-place rewrite, not a new row.
  kInsertUnpacked,   // Unpacked-row liveness marker (unsupported by this scan).
  kDelete,           // Top-level tombstone (whole-row delete).
  kPackedV1,         // V1 packed row (unsupported: no in-header UPDATE marker).
};

// Classify a top-level or per-column entry. `payload` must have any ValueControlFields already
// stripped so that its first byte is the ValueEntryType.
Result<RowEvent> ClassifyEntry(Slice subkey_bytes, Slice payload) {
  const bool is_tombstone =
      !payload.empty() && payload[0] == dockv::ValueEntryTypeAsChar::kTombstone;

  if (subkey_bytes.empty()) {
    if (is_tombstone) {
      return RowEvent::kDelete;
    }
    if (payload.empty()) {
      return RowEvent::kIgnore;
    }
    switch (payload[0]) {
      case dockv::ValueEntryTypeAsChar::kPackedRowV2: {
        // V2 header layout: kPackedRowV2 | schema_version_varint | flags_byte | null_mask | data.
        Slice inner = payload;
        inner.consume_byte();
        RETURN_NOT_OK(FastDecodeUnsignedVarInt(&inner));
        SCHECK(!inner.empty(), Corruption,
               "V2 packed row missing flags byte");
        const uint8_t flags = static_cast<uint8_t>(inner[0]);
        return (flags & dockv::RowPackerV2::kIsUpdateFlag) ? RowEvent::kUpdatePacked
                                                           : RowEvent::kInsertPacked;
      }
      case dockv::ValueEntryTypeAsChar::kPackedRowV1:
        return RowEvent::kPackedV1;
      default:
        return RowEvent::kIgnore;
    }
  }

  if (subkey_bytes == LivenessColumnEncoded() && !is_tombstone) {
    return RowEvent::kInsertUnpacked;
  }

  return RowEvent::kIgnore;
}

} // namespace

Result<VerifyUniqueIndexChunkResult> VerifyUniqueIndexChunk(
    const DocDB& doc_db, const VerifyUniqueIndexChunkOptions& options) {
  VerifyUniqueIndexChunkResult result;

  // TODO(deferred-uniqueness prototype): the timeline algorithm assumes all entries for a
  // given DocKey fall within the same chunk. Nothing here enforces that start_key/end_key
  // are DocKey-aligned; a misaligned end_key that splits a DocKey group between chunks would
  // cause a false negative on either side. Master-side chunking must produce DocKey-aligned
  // bounds (matches existing backfill chunking), and this function should defensively snap
  // end_key to a DocKey boundary or SCHECK the caller has done so before the feature ships.
  const bool has_upper_bound = !options.end_key.empty();
  BoundedRocksDbIterator iter = CreateRocksDBIterator(
      doc_db.regular,
      doc_db.key_bounds,
      BloomFilterOptions::Inactive(),
      rocksdb::kDefaultQueryId,
      /* file_filter= */ nullptr,
      has_upper_bound ? &options.end_key : nullptr,
      rocksdb::CacheRestartBlockKeys::kFalse);

  if (options.start_key.empty()) {
    iter.SeekToFirst();
  } else {
    iter.Seek(options.start_key);
  }

  VLOG(1) << "VerifyUniqueIndexChunk: starting scan verify_time="
          << options.verify_time.ToDebugString();

  // Per-DocKey state for the descending-order timeline walk. Within a DocKey the raw iterator
  // yields entries HT-descending. A duplicate is any DocKey that accumulates two or more
  // top-level packed rows that were written as inserts (kIsUpdateFlag clear). Rewrites
  // produced by an in-place PK update or a packed full-row UPDATE are marked with the
  // kIsUpdateFlag bit and do not contribute to `insert_count`. A tombstone resets the count
  // because it ends any live interval opened by earlier inserts within the group.
  std::string current_doc_key;
  size_t insert_count = 0;

  size_t entries_seen = 0;
  size_t entries_after_filter = 0;
  size_t insert_events = 0;
  size_t update_events = 0;
  size_t delete_events = 0;
  while (iter.Valid()) {
    ++entries_seen;

    Slice mutable_key = iter.key();
    const auto ht = VERIFY_RESULT(DocHybridTime::DecodeFromEnd(&mutable_key));
    // DecodeFromEnd strips only the HT-value bytes; the trailing kHybridTime marker
    // is left behind (same pattern handled in IntentAwareIterator::SeekToLatestSubDocKeyInternal).
    mutable_key.remove_suffix(1);
    const auto doc_key_size = VERIFY_RESULT(
        DocKey::EncodedSize(mutable_key, DocKeyPart::kWholeDocKey));
    Slice doc_key_slice(mutable_key.data(), doc_key_size);
    Slice subkey_bytes(mutable_key.data() + doc_key_size, mutable_key.size() - doc_key_size);

    // Reset per-DocKey state on group change.
    if (doc_key_slice.compare(current_doc_key) != 0) {
      current_doc_key.assign(doc_key_slice.cdata(), doc_key_slice.size());
      insert_count = 0;
    }

    // Enforce the verify_time filter: entries above verify_time are ignored.
    if (options.verify_time.is_valid() && ht.hybrid_time() > options.verify_time) {
      iter.Next();
      continue;
    }
    ++entries_after_filter;

    // Strip any leading ValueControlFields (merge_flags/TTL/user_timestamp) before inspecting
    // the value type. Transactional INSERTs applied to the regular DB can carry these; backfill
    // writes typically do not. If none are present the slice is left unchanged.
    Slice payload = iter.value();
    RETURN_NOT_OK(dockv::ValueControlFields::Decode(&payload));

    switch (VERIFY_RESULT(ClassifyEntry(subkey_bytes, payload))) {
      case RowEvent::kIgnore:
        break;
      case RowEvent::kDelete:
        ++delete_events;
        insert_count = 0;
        break;
      case RowEvent::kUpdatePacked:
        ++update_events;
        break;
      case RowEvent::kInsertPacked:
        ++insert_events;
        ++insert_count;
        if (insert_count >= 2) {
          result.duplicate_key_hint.assign(doc_key_slice.cdata(), doc_key_slice.size());
          // Caller receives the encoded DocKey in the response; do not log it here to
          // avoid leaking indexed user data into server logs.
          return result;
        }
        break;
      case RowEvent::kInsertUnpacked:
        return STATUS(
            NotSupported,
            "Deferred-uniqueness verify requires packed rows; encountered unpacked-row "
            "liveness marker. Enable ysql_enable_packed_row.");
      case RowEvent::kPackedV1:
        return STATUS(
            NotSupported,
            "Deferred-uniqueness verify requires V2 packed rows to distinguish inserts "
            "from in-place updates. Enable ysql_use_packed_row_v2.");
    }

    iter.Next();
  }
  RETURN_NOT_OK(iter.status());

  VLOG(1) << "VerifyUniqueIndexChunk: scan done, entries_seen=" << entries_seen
          << " entries_after_filter=" << entries_after_filter
          << " insert_events=" << insert_events
          << " update_events=" << update_events
          << " delete_events=" << delete_events;

  return result;
}

} // namespace yb::docdb
