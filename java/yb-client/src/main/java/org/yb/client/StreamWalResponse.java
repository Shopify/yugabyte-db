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
