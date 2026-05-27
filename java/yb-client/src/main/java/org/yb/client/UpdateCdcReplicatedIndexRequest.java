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

package org.yb.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import org.yb.Opid;
import org.yb.cdc.CdcService;
import org.yb.util.Pair;

/**
 * Retention-heartbeat RPC wrapper, used as a TEMPORARY testing affordance while we wait for
 * server-side time-based intent retention to land.
 *
 * <p><b>This is not the final retention story for the StreamWAL connector.</b> The project's
 * direction is time-based retention for both the WAL ({@code --log_min_seconds_to_retain}) and
 * IntentsDB (a future {@code intents_min_seconds_to_retain} flag), with NO per-consumer
 * heartbeats and NO lease semantics. This class exists only so the embedded connector test can
 * exercise the StreamWAL path end-to-end before that server change ships: without any retention
 * barrier, intents are GC'd immediately post-apply and every transactional record returns
 * INTENTS_GC_ERROR on the wire.
 *
 * <p>One call pins both retention barriers on a single tablet:
 * <ul>
 *   <li><b>WAL barrier</b> (via {@code replicated_indices}) — keeps WAL segments past the cursor
 *       on disk so the next StreamWAL call doesn't see {@code CHECKPOINT_TOO_OLD}.</li>
 *   <li><b>IntentsDB barrier</b> (via {@code cdc_sdk_consumed_ops}) — keeps committed-but-unstreamed
 *       transactions' intents in IntentsDB so the server can read them at APPLYING time without
 *       returning {@code INTENTS_GC_ERROR}.</li>
 * </ul>
 *
 * <p>Leader-only on the current RPC; the connector handles multi-replica fanout itself when
 * multi-replica testing comes online.
 */
public class UpdateCdcReplicatedIndexRequest extends YRpc<UpdateCdcReplicatedIndexResponse> {

  private final String tabletId;
  private final long replicatedTerm;
  private final long replicatedIndex;
  /** Mirrors {@code cdc_sdk_consumed_ops}; the cursor that pins the IntentsDB barrier. */
  private final long cdcSdkOpTerm;
  private final long cdcSdkOpIndex;
  /** Lease TTL in ms; the server's checkpoint is treated as stale past this. */
  private final long cdcSdkOpExpirationMs;

  /**
   * @param table                  YBTable used to anchor the meta-cache lookup for the tablet's
   *                               leader.
   * @param tabletId               32-char hex tablet UUID rendered as UTF-8.
   * @param replicatedTerm         WAL barrier term — pinned to {@code (term, index)} so segments
   *                               past this cursor are retained.
   * @param replicatedIndex        WAL barrier index.
   * @param cdcSdkOpTerm           IntentsDB barrier term — pinned to {@code (term, index)} so
   *                               intents at or after this op stay readable.
   * @param cdcSdkOpIndex          IntentsDB barrier index.
   * @param cdcSdkOpExpirationMs   Lease TTL for the IntentsDB barrier; once this elapses without
   *                               a refresh, the server reverts to {@code OpId::Max()} (GC
   *                               everything) and we'll start hitting INTENTS_GC_ERROR.
   */
  public UpdateCdcReplicatedIndexRequest(YBTable table,
                                         String tabletId,
                                         long replicatedTerm,
                                         long replicatedIndex,
                                         long cdcSdkOpTerm,
                                         long cdcSdkOpIndex,
                                         long cdcSdkOpExpirationMs) {
    super(table);
    this.tabletId = tabletId;
    this.replicatedTerm = replicatedTerm;
    this.replicatedIndex = replicatedIndex;
    this.cdcSdkOpTerm = cdcSdkOpTerm;
    this.cdcSdkOpIndex = cdcSdkOpIndex;
    this.cdcSdkOpExpirationMs = cdcSdkOpExpirationMs;
  }

  public String getTabletId() {
    return tabletId;
  }

  @Override
  ByteBuf serialize(Message header) {
    assert header.isInitialized();
    CdcService.UpdateCdcReplicatedIndexRequestPB.Builder builder =
        CdcService.UpdateCdcReplicatedIndexRequestPB.newBuilder();

    // The repeated-fields shape is the modern one; the legacy single-field shape
    // (tablet_id / replicated_index) is deprecated and intentionally not used.
    builder.addTabletIds(ByteString.copyFromUtf8(this.tabletId));
    builder.addReplicatedIndices(this.replicatedIndex);
    builder.addReplicatedTerms(this.replicatedTerm);
    builder.addCdcSdkConsumedOps(
        Opid.OpIdPB.newBuilder()
            .setTerm(this.cdcSdkOpTerm)
            .setIndex(this.cdcSdkOpIndex)
            .build());
    builder.addCdcSdkOpsExpirationMs(this.cdcSdkOpExpirationMs);

    return toChannelBuffer(header, builder.build());
  }

  @Override
  String serviceName() {
    return CDC_SERVICE_NAME;
  }

  @Override
  String method() {
    return "UpdateCdcReplicatedIndex";
  }

  @Override
  Pair<UpdateCdcReplicatedIndexResponse, Object> deserialize(CallResponse callResponse, String uuid)
      throws Exception {
    CdcService.UpdateCdcReplicatedIndexResponsePB.Builder respBuilder =
        CdcService.UpdateCdcReplicatedIndexResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    UpdateCdcReplicatedIndexResponse response =
        new UpdateCdcReplicatedIndexResponse(deadlineTracker.getElapsedMillis(), uuid);
    return new Pair<>(response, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}
