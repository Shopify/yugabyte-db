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
import org.yb.cdc.CdcService.StreamWalCursorPB;
import org.yb.cdc.CdcService.StreamWalRequestPB;
import org.yb.cdc.CdcService.StreamWalResponsePB;
import org.yb.util.Pair;

/**
 * Per-tablet, leader-only StreamWAL RPC.
 *
 * <p>The cursor passed as {@code from_op_id} has three valid shapes (see {@link StreamWalCursorPB}
 * in the proto):
 * <ul>
 *   <li>{@code (0, 0)} — start from the earliest retained WAL entry; server emits synthetic
 *       bootstrap DDLs first.</li>
 *   <li>{@code (-1, -1)} — skip-to-tip sentinel; server emits bootstrap DDLs only and points
 *       {@code next_op_id} at the live leader tip.</li>
 *   <li>{@code (term >= 1, index >= 0)} — strict resume position; no bootstrap.</li>
 * </ul>
 *
 * <p>Dispatched by {@link AsyncYBClient#sendRpcToTablet(YRpc)} which routes to the tablet leader
 * via the meta-cache; on {@code LEADER_NOT_READY} the response carries
 * {@code tablet_consensus_info} that callers may use to refresh their leader hint without going
 * back to master.
 */
public class StreamWalRequest extends YRpc<StreamWalResponse> {

  private final String tabletId;
  private final long fromTerm;
  private final long fromIndex;
  private final int maxRecords;
  private final long maxBytes;
  private final int deadlineMs;

  /**
   * @param table       the YBTable used to anchor the meta-cache lookup (for colocated parent
   *                    tablets, any table hosted on the tablet will do).
   * @param tabletId    32-char hex tablet UUID rendered as a UTF-8 string, matching how every
   *                    other CDC RPC encodes tablet ids on the wire.
   * @param fromTerm    cursor term; 0 (with index=0) means "from beginning"; -1 (with index=-1)
   *                    means "skip to tip".
   * @param fromIndex   cursor index.
   * @param maxRecords  soft cap on records returned (forwarded as {@code max_records}). Pass 0 to
   *                    omit and let the server default apply.
   * @param maxBytes    soft cap on response payload size in bytes (forwarded as {@code
   *                    max_bytes}). Pass 0 to omit.
   * @param deadlineMs  server-side wait budget in ms when no records are immediately available;
   *                    0 means short-poll.
   */
  public StreamWalRequest(YBTable table,
                          String tabletId,
                          long fromTerm,
                          long fromIndex,
                          int maxRecords,
                          long maxBytes,
                          int deadlineMs) {
    super(table);
    this.tabletId = tabletId;
    this.fromTerm = fromTerm;
    this.fromIndex = fromIndex;
    this.maxRecords = maxRecords;
    this.maxBytes = maxBytes;
    this.deadlineMs = deadlineMs;
  }

  public String getTabletId() {
    return tabletId;
  }

  @Override
  ByteBuf serialize(Message header) {
    assert header.isInitialized();
    final StreamWalRequestPB.Builder builder = StreamWalRequestPB.newBuilder();
    builder.setTabletId(ByteString.copyFromUtf8(this.tabletId));

    StreamWalCursorPB.Builder cursorBuilder = StreamWalCursorPB.newBuilder();
    cursorBuilder.setTerm(this.fromTerm);
    cursorBuilder.setIndex(this.fromIndex);
    builder.setFromOpId(cursorBuilder.build());

    if (this.maxRecords > 0) {
      builder.setMaxRecords(this.maxRecords);
    }
    if (this.maxBytes > 0) {
      builder.setMaxBytes(this.maxBytes);
    }
    // deadline_ms is always meaningful (0 == short-poll), so we always set it.
    builder.setDeadlineMs(this.deadlineMs);

    return toChannelBuffer(header, builder.build());
  }

  @Override
  String serviceName() {
    return CDC_SERVICE_NAME;
  }

  @Override
  String method() {
    return "StreamWAL";
  }

  @Override
  Pair<StreamWalResponse, Object> deserialize(CallResponse callResponse, String uuid)
      throws Exception {
    final StreamWalResponsePB.Builder respBuilder = StreamWalResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    StreamWalResponsePB resp = respBuilder.build();
    StreamWalResponse response =
        new StreamWalResponse(deadlineTracker.getElapsedMillis(), uuid, resp);
    return new Pair<StreamWalResponse, Object>(
        response, resp.hasError() ? resp.getError() : null);
  }
}
