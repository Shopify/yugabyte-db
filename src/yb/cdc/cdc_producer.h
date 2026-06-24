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

#pragma once

#include <memory>
#include <optional>
#include <string>

#include <boost/functional/hash.hpp>
#include <boost/unordered_map.hpp>

#include "yb/cdc/cdc_service.pb.h"
#include "yb/cdc/cdc_types.h"
#include "yb/client/client_fwd.h"
#include "yb/common/common_fwd.h"
#include "yb/common/doc_hybrid_time.h"
#include "yb/common/opid.h"
#include "yb/common/transaction.h"
#include "yb/consensus/consensus_fwd.h"
#include "yb/dockv/dockv_fwd.h"
#include "yb/tablet/tablet_fwd.h"
#include "yb/util/monotime.h"
#include "yb/master/master_replication.pb.h"
#include "yb/gutil/thread_annotations.h"

namespace yb {

class MemTracker;

namespace cdc {

struct SchemaDetails {
  SchemaVersion schema_version;
  std::shared_ptr<Schema> schema;
};
// We will maintain a map for each stream, tablet pait. The schema details will correspond to the
// the current 'running' schema.
using SchemaDetailsMap = std::map<TableId, SchemaDetails>;
using TableSchemaPackingStorage = std::unordered_map<TableId, dockv::SchemaPackingStorage>;
using consensus::HaveMoreMessages;

struct CDCThroughputMetrics {
  uint64_t records_sent = 0;
  uint64_t bytes_sent = 0;
};

using UpdateOnSplitOpFunc = std::function<Status(const consensus::ReplicateMsg&)>;

class StreamMetadata;

struct XClusterGetChangesContext {
  const xrepl::StreamId& stream_id;
  const TabletId&  tablet_id;
  const OpId& from_op_id;
  const std::shared_ptr<tablet::TabletPeer>& tablet_peer;
  UpdateOnSplitOpFunc update_on_split_op_func;
  const std::shared_ptr<MemTracker>& mem_tracker;
  const CoarseTimePoint& deadline;
  StreamMetadata* stream_metadata;
  consensus::ReplicateMsgsHolder* msgs_holder;
  GetChangesResponsePB* resp;
  HaveMoreMessages* have_more_messages;
  int64_t* last_readable_opid_index;
};

Status GetChangesForCDCSDK(
    const xrepl::StreamId& stream_id,
    const TabletId& tablet_id,
    const CDCSDKCheckpointPB& from_op_id,
    const StreamMetadata& stream_metadata,
    const tablet::TabletPeerPtr& tablet_peer,
    const std::shared_ptr<MemTracker>& mem_tracker,
    const EnumOidLabelMap& enum_oid_label_map,
    const CompositeAttsMap& composite_atts_map,
    CDCSDKRequestSource request_source,
    client::YBClient* client,
    consensus::ReplicateMsgsHolder* msgs_holder,
    GetChangesResponsePB* resp,
    HybridTime* commit_timestamp,
    SchemaDetailsMap* cached_schema_details,
    TableSchemaPackingStorage* schema_packing_storages,
    OpId* last_streamed_op_id,
    int64_t safe_hybrid_time_req,
    std::optional<uint64_t> consistent_snapshot_time,
    int wal_segment_index_req,
    int64_t* last_readable_opid_index = nullptr,
    const TableId& colocated_table_id = "",
    CoarseTimePoint deadline = CoarseTimePoint::max(),
    std::optional<uint64> getchanges_resp_max_size_bytes = std::nullopt,
    CDCThroughputMetrics* throughput_metrics = nullptr);

bool IsReplicationSlotStream(const StreamMetadata& stream_metadata);

Status GetChangesForXCluster(const XClusterGetChangesContext& context);

//
// StreamWAL helpers.
//
// These helpers are additive callsites on top of the existing cdcsdk_producer.cc
// decoder family. They are used exclusively by CDCServiceImpl::StreamWAL (the
// per-tablet, stream-id-less WAL streaming RPC) and do not change any existing
// CDC code path.
//

// Synthesize one DDL CDCSDKProtoRecordPB per active (table_id, schema_version)
// from the tablet's current metadata. Used by StreamWAL to bootstrap the
// client's schema cache when from_op_id is {0, 0} or the skip-to-latest sentinel.
//
// Each emitted record gets cdc_sdk_op_id = {term: 0, index: 0, write_id: i}
// where i is the record's position in `out`, which disambiguates across
// colocated tables.
//
// Parent (tablegroup / colocation) tables are skipped. Sys catalog tablets emit
// no bootstrap DDLs (the decoder resolves their schemas per-row at runtime).
Status PopulateSyntheticBootstrapDDLs(
    const std::shared_ptr<tablet::TabletPeer>& tablet_peer,
    std::vector<CDCSDKProtoRecordPB>* out);

// Build a COMMIT envelope record for an UPDATE_TRANSACTION_OP { APPLYING } WAL
// entry. Populates:
//   row_message.op           = COMMIT
//   row_message.transaction_id
//   row_message.commit_time  = TransactionStatePB.commit_hybrid_time
//   row_message.xrepl_origin_id (if set on TransactionStatePB)
//   cdc_sdk_op_id            = (msg.id.term, msg.id.index, 0)
//   aborted_subtxn_set       = mirrors TransactionStatePB.aborted (when present)
//
// `encoded_start_key` and `encoded_end_key` are populated into the encoded_key
// fields of the (older) CDCRecordPB only by the legacy XCluster decoder; for
// CDCSDKProtoRecordPB they are surfaced via row_message.primary_key on the
// per-row records emitted from PopulateCDCSDKIntentRecord, not here. The COMMIT
// envelope therefore does not carry partition bounds.
Status PopulateStreamWalApplyingRecord(
    const consensus::ReplicateMsgPtr& msg,
    CDCSDKProtoRecordPB* out);

// Build the terminal SPLIT_OP envelope record for a SPLIT_OP WAL entry.
// Populates:
//   row_message.op            = UNKNOWN  (no dedicated SPLIT op in RowMessage)
//   row_message.commit_time   = msg.hybrid_time
//   cdc_sdk_op_id             = (msg.id.term, msg.id.index, 0)
//   split_tablet_request      = msg.split_request  (carries new_tablet1_id,
//                                                   new_tablet2_id,
//                                                   split_partition_key,
//                                                   split_encoded_key)
Status PopulateStreamWalSplitRecord(
    const consensus::ReplicateMsgPtr& msg,
    CDCSDKProtoRecordPB* out);

// Per-call decoder context for StreamWAL. Lives for the duration of one RPC
// handler invocation; not thread-safe.
struct StreamWalDecodeContext {
  std::shared_ptr<tablet::TabletPeer> tablet_peer;
  const StreamMetadata* stream_metadata = nullptr;
  client::YBClient* client = nullptr;
  SchemaDetailsMap* cached_schema_details = nullptr;
  TableSchemaPackingStorage* schema_packing_storages = nullptr;
  const EnumOidLabelMap* enum_map = nullptr;
  const CompositeAttsMap* composite_atts_map = nullptr;
};

// Resume state for a mid-APPLYING StreamWAL call. Round-trips through the
// new StreamWalCursorPB.intent_key / intent_write_id fields.
//
// Mirrors docdb::ApplyTransactionState's resumption fields. The aborted-subtxn
// set is NOT carried here -- it is re-derived from the WAL APPLYING entry on
// every resuming call, which is cheaper than encoding it on the wire and
// authoritative regardless of replica.
struct StreamWalIntentResumeState {
  std::string intent_key;
  IntraTxnWriteId intent_write_id = 0;
};

// Outcome of decoding a single ReplicateMsg via the StreamWAL emission rules.
struct StreamWalDispatchResult {
  enum class Kind {
    // SPLIT_OP for this tablet -> emitted a terminal split record. The caller
    // MUST stop the loop after this op; no further records will arrive on this
    // tablet's stream.
    kSplitTerminal,
    // The op produced one or more CDCSDKProtoRecordPB records appended to the
    // caller's output container.
    kRecordsEmitted,
    // The op was silent (no record emitted). The caller still advances the
    // cursor past it.
    kSilent,
    // Set by DispatchApplyingForStreamWAL when an APPLYING's intents did not
    // fit in one batch. `mid_applying_resume` is populated with the next
    // intent's (key, write_id). The caller MUST stop the loop, MUST NOT advance
    // the cursor past this op, and MUST emit a partial-APPLYING cursor on the
    // response.
    kApplyingSpilled,
  };
  Kind kind = Kind::kSilent;

  // Populated iff kind == kApplyingSpilled.
  std::optional<StreamWalIntentResumeState> mid_applying_resume;
};

// Decode a single ReplicateMsg per the StreamWAL per-OperationType emission
// rules in the data contract. Appends 0..N CDCSDKProtoRecordPB messages to
// *out_records. The decoder may consult / update the schema caches on `ctx`.
//
// This is the ONLY entry point cdc_service.cc::StreamWAL uses to invoke the
// existing decoder family; the decoder functions themselves are private to
// cdcsdk_producer.cc.
//
// Note: when called on an UPDATE_TRANSACTION_OP { APPLYING }, this function
// internally delegates to DispatchApplyingForStreamWAL with resume_state =
// nullptr (i.e. start-of-APPLYING) and may return kApplyingSpilled.
Result<StreamWalDispatchResult> DispatchWalOpForStreamWAL(
    const consensus::ReplicateMsgPtr& msg,
    StreamWalDecodeContext* ctx,
    std::vector<CDCSDKProtoRecordPB>* out_records);

// Emit BEGIN + per-row DMLs + (conditional) COMMIT for an APPLYING WAL entry.
//
// Reads the transaction's intents from IntentsDB via Tablet::GetIntentsForCDC
// (composed inside ProcessIntentsWithInvalidSchemaRetry), filters aborted
// subtxns server-side, stamps commit_hybrid_time on all envelopes and per-row
// records, and appends the resulting CDCSDKProtoRecordPB records to
// *out_records.
//
// If resume_state is null, starts at the beginning of the APPLYING's intent
// scan and emits the BEGIN envelope as the first record.
//
// If resume_state is non-null, skips the BEGIN envelope (already emitted on a
// prior call) and resumes the intent scan one position past
// (resume_state->intent_key, resume_state->intent_write_id).
//
// On a clean drain (intent budget did not spill), the COMMIT envelope is the
// final record appended; *out is set to kRecordsEmitted and
// out->mid_applying_resume is cleared.
//
// On a spill (intent budget exhausted before COMMIT), the COMMIT envelope is
// NOT emitted; *out is set to kApplyingSpilled and out->mid_applying_resume is
// populated with the position of the last intent emitted in this batch (the
// next call resumes one position past it).
//
// On the "intents already GC'd from IntentsDB" condition (i.e. the underlying
// ProcessIntents detection at cdcsdk_producer.cc:1779), returns a status that
// .IsIllegalState() and whose message contains "INTENTS_GC_ERROR". The handler
// maps this to CDCErrorPB::INTENTS_GC_ERROR. All other errors are propagated
// as-is and surface as CDCErrorPB::INTERNAL_ERROR.
Status DispatchApplyingForStreamWAL(
    const consensus::ReplicateMsgPtr& applying_msg,
    StreamWalDecodeContext* ctx,
    const StreamWalIntentResumeState* resume_state,
    std::vector<CDCSDKProtoRecordPB>* out_records,
    StreamWalDispatchResult* out);

//
// StreamWAL consistent-commit-order driver.
//
// Used exclusively by CDCServiceImpl::StreamWAL when a request sets
// consistent_commit_order = true. It produces a batch of COMMITTED records in
// non-decreasing commit_time order, gated behind the per-tablet resolution
// watermark, plus the COMPOSITE resume cursor (WAL floor + commit_ht + optional
// mid-APPLYING spill position).
//
// It REUSES the existing GetChangesForCDCSDK consistent-streaming helpers that
// are INDEPENDENT of the global FLAGS_cdc_enable_consistent_records gflag --
// GetConsistentStreamSafeTime (watermark), GetConsistentWALRecords (segment read
// + wait_for_wal_update + commit sort), SortConsistentWALRecords, and
// ShouldUpdateSafeTime (the tie-group guard) -- plus the StreamWAL decoder
// dispatch above. It does NOT reuse the flag-gated GetChanges checkpoint /
// safe-time helpers (AcknowledgeStreamedMsg / CanUpdateCheckpointOpId /
// UpdateSafetimeForResponse / GetCDCSDKSafeTimeForTarget); their consistent-mode
// logic (WAL floor advance + commit-time frontier advance) is reimplemented
// locally so that consistent_commit_order is a TRUE per-request mode with no
// dependency on the global gflag. It does NOT modify GetChangesForCDCSDK or any
// existing CDC codepath.

struct StreamWalConsistentInput {
  // WAL re-read floor: (term, index) of the composite cursor. {0,0} means
  // "from earliest retained WAL".
  OpId floor;

  // Commit-time dedup frontier from the request cursor (commit_ht). Records
  // with commit_time <= commit_ht_skip are already delivered and are skipped.
  // 0 at bootstrap / first call.
  uint64_t commit_ht_skip = 0;

  // Mid-APPLYING spill resume. When resuming_spilled is true, the floor's
  // (term, index) points at an UPDATE_TRANSACTION_OP { APPLYING } whose intents
  // did not fit in a prior batch; the scan resumes one position past
  // (resume_intent_key, resume_intent_write_id).
  bool resuming_spilled = false;
  std::string resume_intent_key;
  IntraTxnWriteId resume_intent_write_id = 0;

  // Soft batch caps (already defaulted by the handler).
  uint32_t max_records = 1000;
  uint64_t max_bytes = 4 * 1024 * 1024;

  // Leader safe time snapshot taken by the handler; fed to
  // GetConsistentStreamSafeTime as the fallback watermark.
  HybridTime leader_safe_time;

  // WAL-read deadline (already bounded by the handler).
  CoarseTimePoint deadline;
};

struct StreamWalConsistentOutput {
  // Committed records in non-decreasing commit_time order (EXCLUDES synthetic
  // bootstrap DDLs, which the handler prepends).
  std::vector<CDCSDKProtoRecordPB> records;

  // Composite resume cursor.
  OpId next_floor;
  uint64_t next_commit_ht = 0;            // advanced commit-time frontier (0 = unset)
  bool mid_applying = false;              // true iff a spilled APPLYING straddles the batch
  std::string next_intent_key;           // set iff mid_applying
  IntraTxnWriteId next_intent_write_id = 0;

  // The resolution watermark this batch gated on (0 / unset while txns load).
  uint64_t resolution_safe_time = 0;
  bool resolution_safe_time_set = false;

  bool has_more = false;

  // True iff the batch stopped because the next commit-ordered record has
  // commit_time > resolution_safe_time (gated on the watermark, not on a size
  // cap or a spill). The handler uses this to avoid claiming has_more from the
  // leader tip when the only remaining records are watermark-gated.
  bool stopped_on_watermark = false;

  // True iff transactions are still loading on the tablet (W invalid). The
  // handler returns an empty, non-advancing batch with resolution_safe_time
  // unset.
  bool txn_load_in_progress = false;

  // True iff the readable WAL window was empty AND we are not waiting on a
  // pending-but-uncommitted txn (i.e. genuinely nothing to read: a brand-new or
  // fully-GC'd tablet). The handler uses this on a {0,0} from-start request to
  // advance the cursor to the leader tip instead of re-bootstrapping forever.
  bool read_window_empty = false;

  // True iff a SPLIT_OP terminal record for this tablet was emitted. The
  // handler treats this exactly like the WAL-order path (last record on the
  // stream; next call returns TABLET_SPLIT).
  bool saw_split = false;
};

Status GetConsistentChangesForStreamWAL(
    const StreamWalConsistentInput& input,
    StreamWalDecodeContext* ctx,
    StreamWalConsistentOutput* output);

}  // namespace cdc
}  // namespace yb
