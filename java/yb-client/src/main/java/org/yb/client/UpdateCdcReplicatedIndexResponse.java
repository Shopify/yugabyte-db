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

/**
 * Trivial response wrapper for {@link UpdateCdcReplicatedIndexRequest}.
 *
 * <p>The server returns either OK (no payload of interest to the client) or a {@code CDCErrorPB}
 * carried on the underlying response PB. Errors are surfaced via the standard yb-client error
 * dispatch pipeline; callers that care can catch {@link CDCErrorException} on
 * {@link com.stumbleupon.async.Deferred#join}.
 */
public class UpdateCdcReplicatedIndexResponse extends YRpcResponse {
  public UpdateCdcReplicatedIndexResponse(long elapsedMillis, String tsUUID) {
    super(elapsedMillis, tsUUID);
  }
}
