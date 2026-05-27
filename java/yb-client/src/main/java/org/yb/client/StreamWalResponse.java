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
import java.util.List;
import org.yb.annotations.InterfaceAudience;
import org.yb.cdc.CdcService.CDCErrorPB;
import org.yb.cdc.CdcService.CDCSDKProtoRecordPB;
import org.yb.cdc.CdcService.StreamWalCursorPB;
import org.yb.cdc.CdcService.StreamWalResponsePB;
import org.yb.tserver.Tserver.TabletConsensusInfoPB;

/**
 * Java wrapper around {@link StreamWalResponsePB}.
 *
 * <p>Most consumers will care about three things:
 *
 * <ol>
 *   <li>{@link #hasError()} / {@link #getError()} — if set, the records list will be empty and
 *       cursors will be unset. On {@link CDCErrorPB.Code#LEADER_NOT_READY},
 *       {@link #getTabletConsensusInfo()} is populated so callers can refresh their leader hint
 *       without going to master.</li>
 *   <li>{@link #getRecords()} — the decoded WAL records in strict order. Synthetic bootstrap
 *       DDLs (for fresh tablets and skip-to-tip sentinels) come at the front of the batch.</li>
 *   <li>{@link #getNextOpId()} — the cursor to pass back as {@code from_op_id} on the next call.
 *       Required to advance the per-tablet position; never advance the cursor from the records
 *       themselves, the server is authoritative here.</li>
 * </ol>
 */
@InterfaceAudience.Public
public class StreamWalResponse extends YRpcResponse {

  private final StreamWalResponsePB resp;

  StreamWalResponse(long elapsedMillis, String uuid, StreamWalResponsePB resp) {
    super(elapsedMillis, uuid);
    this.resp = resp;
  }

  public StreamWalResponsePB getResp() {
    return resp;
  }

  public boolean hasError() {
    return resp.hasError();
  }

  public CDCErrorPB getError() {
    return resp.getError();
  }

  /**
   * Populated only on {@code LEADER_NOT_READY}. Callers can use this to refresh their leader
   * hint without going back to master.
   */
  public boolean hasTabletConsensusInfo() {
    return resp.hasTabletConsensusInfo();
  }

  public TabletConsensusInfoPB getTabletConsensusInfo() {
    return resp.getTabletConsensusInfo();
  }

  public List<CDCSDKProtoRecordPB> getRecords() {
    return resp.getRecordsList();
  }

  public int getRecordCount() {
    return resp.getRecordsCount();
  }

  public boolean hasNextOpId() {
    return resp.hasNextOpId();
  }

  public StreamWalCursorPB getNextOpId() {
    return resp.getNextOpId();
  }

  public long getNextOpIdTerm() {
    return resp.getNextOpId().getTerm();
  }

  public long getNextOpIdIndex() {
    return resp.getNextOpId().getIndex();
  }

  /**
   * @return {@code true} iff {@link #getNextOpId()} carries mid-APPLYING resumption state —
   *         i.e. the prior batch returned a partial-APPLYING cursor because a transaction's
   *         intents did not all fit. Callers MUST round-trip both {@code intent_key} and
   *         {@code intent_write_id} back as the next call's {@code from_op_id}.
   *
   * <p>The cursor's {@code (term, index)} are the APPLYING op's own OpId; the cursor has NOT
   * advanced past the APPLYING.
   */
  public boolean nextOpIdIsMidApplying() {
    StreamWalCursorPB cursor = resp.getNextOpId();
    return cursor.hasIntentKey() && cursor.hasIntentWriteId();
  }

  /**
   * Opaque docdb reverse-index key for resuming a spilled APPLYING. Returns {@code null} when
   * the cursor is not mid-APPLYING (the common case). Round-trip back as-is on the next call;
   * do not interpret.
   */
  public ByteString getNextOpIdIntentKey() {
    StreamWalCursorPB cursor = resp.getNextOpId();
    return cursor.hasIntentKey() ? cursor.getIntentKey() : null;
  }

  /**
   * IntraTxnWriteId paired with {@link #getNextOpIdIntentKey()}. Returns {@code null} when the
   * cursor is not mid-APPLYING.
   */
  public Integer getNextOpIdIntentWriteId() {
    StreamWalCursorPB cursor = resp.getNextOpId();
    return cursor.hasIntentWriteId() ? cursor.getIntentWriteId() : null;
  }

  public boolean hasLeaderTipOpId() {
    return resp.hasLeaderTipOpId();
  }

  public StreamWalCursorPB getLeaderTipOpId() {
    return resp.getLeaderTipOpId();
  }

  public long getLeaderSafeHybridTime() {
    return resp.getLeaderSafeHybridTime();
  }

  /** True iff more data is immediately available past {@link #getNextOpId()}. */
  public boolean hasMore() {
    return resp.getHasMore();
  }
}
