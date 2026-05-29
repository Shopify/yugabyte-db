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

import static org.yb.AssertionWrappers.assertEquals;
import static org.yb.AssertionWrappers.assertFalse;
import static org.yb.AssertionWrappers.assertSame;
import static org.yb.AssertionWrappers.assertTrue;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.YBTestRunner;
import org.yb.consensus.Metadata.ConsensusStatePB;
import org.yb.consensus.Metadata.RaftConfigPB;
import org.yb.tserver.Tserver.TabletConsensusInfoPB;
import org.yb.util.Slice;

/**
 * Unit tests for the consensus-info leader-hint plumbing introduced for the StreamWAL connector.
 *
 * <p>The hint path is:
 *
 * <pre>
 *   TabletClient.decode
 *     -> TabletClient.maybeApplyConsensusHint (guarded by instanceof StreamWalRequest)
 *       -> AsyncYBClient.refreshTabletLeaderFromConsensus
 *         -> AsyncYBClient.RemoteTablet.applyLeaderHint
 * </pre>
 *
 * <p>These tests exercise the bottom two layers in isolation. The top layer
 * ({@code maybeApplyConsensusHint} call-site gating) is asserted by static analysis of the
 * call sites in {@code TabletClient.decode} \u2014 the function is private and we don't reach into
 * it from tests; the {@code instanceof} check guarantees non-StreamWAL responses (notably
 * legacy {@code GetChanges}) never reach the layers tested below.
 *
 * <p>Pure data-structure tests; no live cluster, no Netty I/O. The {@link AsyncYBClient} is
 * built via {@link AsyncYBClient.AsyncYBClientBuilder} which "doesn't block and won't throw an
 * exception if the masters don't exist" (per its own Javadoc), and {@link TabletClient}'s
 * constructor does not open connections.
 */
@RunWith(value = YBTestRunner.class)
public class TestConsensusLeaderHint {

  private static final String TABLE_ID = "fake-table-uuid";
  private static final String TABLET_ID = "fake-tablet-uuid";
  private static final String UUID_A = "tserver-A";
  private static final String UUID_B = "tserver-B";
  private static final String UUID_C = "tserver-C";

  /**
   * Mirrors {@code AsyncYBClient.RemoteTablet.NO_LEADER_INDEX}. We re-declare it here rather
   * than reach into the (private) constant because exposing it would broaden the API surface
   * for a value that's effectively part of an internal protocol between {@code clientFor},
   * {@code demoteLeader}, and {@code applyLeaderHint}.
   */
  private static final int NO_LEADER_INDEX = -1;

  private AsyncYBClient client;
  private AsyncYBClient.RemoteTablet tablet;
  private TabletClient tsA;
  private TabletClient tsB;
  private TabletClient tsC;

  @Before
  public void setUp() {
    // The "host:port" doesn't need to be reachable; we never call .build() through to a
    // master, never open a Netty channel, and never send an RPC.
    client = new AsyncYBClient.AsyncYBClientBuilder("ignored-host:7100").build();

    tsA = new TabletClient(client, UUID_A);
    tsB = new TabletClient(client, UUID_B);
    tsC = new TabletClient(client, UUID_C);

    Partition partition = new Partition(new byte[0], new byte[0], Collections.emptyList());
    tablet = client.new RemoteTablet(
        TABLE_ID, new Slice(TABLET_ID.getBytes()), partition);
  }

  @After
  public void tearDown() throws Exception {
    if (client != null) {
      // Best-effort shutdown of any executors the builder spun up. Exceptions ignored \u2014 the
      // builder may not have created anything that needs closing.
      try {
        client.shutdown().join(1000);
      } catch (Exception ignored) {
      }
    }
  }

  // ---------------------------------------------------------------------------------------
  // RemoteTablet.applyLeaderHint
  // ---------------------------------------------------------------------------------------

  @Test
  public void applyLeaderHint_promotesKnownPeer() {
    // tabletServers = [A, B, C], leaderIndex = 0 (A is the current leader).
    tablet.setTabletClientsForTest(Arrays.asList(tsA, tsB, tsC), 0);
    assertEquals("precondition: A is the current leader",
        0, tablet.leaderIndexForTest());

    // Hint says C is the new leader. Expect leaderIndex to move to 2 and clientFor to
    // route to C from here on.
    assertTrue("hint applied", tablet.applyLeaderHint(UUID_C));
    assertEquals(2, tablet.leaderIndexForTest());
    assertSame(tsC, client.clientFor(tablet));
  }

  @Test
  public void applyLeaderHint_idempotentOnSameLeader() {
    tablet.setTabletClientsForTest(Arrays.asList(tsA, tsB, tsC), 1);
    assertEquals("precondition: B is the current leader",
        1, tablet.leaderIndexForTest());

    // Hint matches current leader. Should be a no-op-but-true so callers can treat
    // "hint accepted" uniformly.
    assertTrue("hint accepted as idempotent", tablet.applyLeaderHint(UUID_B));
    assertEquals("leaderIndex unchanged", 1, tablet.leaderIndexForTest());
    assertSame(tsB, client.clientFor(tablet));

    // Re-applying again is also fine.
    assertTrue(tablet.applyLeaderHint(UUID_B));
    assertEquals(1, tablet.leaderIndexForTest());
  }

  @Test
  public void applyLeaderHint_returnsFalseOnUnknownPeer() {
    // Hint names a peer that's not in our tabletServers list (e.g. a node added since
    // our last master lookup). The contract is to return false so the caller falls
    // back to the existing demote-walk + master path; we do NOT synthesize a new
    // TabletClient here (would require a blocking DNS lookup on the Netty thread).
    tablet.setTabletClientsForTest(Arrays.asList(tsA, tsB), 0);

    assertFalse("unknown peer", tablet.applyLeaderHint("tserver-NEVER-SEEN"));
    assertEquals("leaderIndex unchanged on miss",
        0, tablet.leaderIndexForTest());
    assertSame("clientFor still routes to A", tsA, client.clientFor(tablet));
  }

  @Test
  public void applyLeaderHint_returnsFalseOnEmptyTabletServers() {
    // Tablet exists in the cache but we haven't connected to any replica yet \u2014 nothing
    // we can do, fall back to discovery.
    tablet.setTabletClientsForTest(Collections.emptyList(), NO_LEADER_INDEX);

    assertFalse(tablet.applyLeaderHint(UUID_A));
  }

  @Test
  public void applyLeaderHint_returnsFalseOnNullOrEmptyUuid() {
    tablet.setTabletClientsForTest(Arrays.asList(tsA, tsB, tsC), 0);

    // Defensive: the server-side proto declares leader_uuid as optional string; an empty
    // value means "no current leader" (election in progress). Nothing actionable.
    assertFalse(tablet.applyLeaderHint(null));
    assertFalse(tablet.applyLeaderHint(""));
    assertEquals(0, tablet.leaderIndexForTest());
  }

  @Test
  public void applyLeaderHint_recoversFromNoLeaderIndex() {
    // If a prior demote-walk exhausted the list, leaderIndex is NO_LEADER_INDEX (we'd
    // otherwise be about to do a master lookup). A hint that names a known peer should
    // re-establish leadership without that round-trip.
    tablet.setTabletClientsForTest(Arrays.asList(tsA, tsB, tsC), NO_LEADER_INDEX);

    assertTrue(tablet.applyLeaderHint(UUID_B));
    assertEquals(1, tablet.leaderIndexForTest());
    assertSame(tsB, client.clientFor(tablet));
  }

  // ---------------------------------------------------------------------------------------
  // AsyncYBClient.refreshTabletLeaderFromConsensus
  // ---------------------------------------------------------------------------------------

  @Test
  public void refreshTabletLeaderFromConsensus_returnsFalseWhenTabletNotInCache() {
    TabletConsensusInfoPB info = consensusInfoWithLeader(UUID_C);

    // We never put `tablet` into client.tablet2client, so the lookup misses. This is
    // exactly the state the cache will be in on the very first call to a fresh tablet \u2014
    // we don't want to error in that case, just return false so the normal discovery
    // path runs.
    assertFalse(client.refreshTabletLeaderFromConsensus(TABLET_ID, info));
  }

  @Test
  public void refreshTabletLeaderFromConsensus_returnsFalseOnMissingConsensusState() {
    primeCacheWithTablet();

    TabletConsensusInfoPB info = TabletConsensusInfoPB.newBuilder()
        .setTabletId(ByteString.copyFromUtf8(TABLET_ID))
        // Deliberately no consensus_state.
        .build();

    assertFalse(client.refreshTabletLeaderFromConsensus(TABLET_ID, info));
  }

  @Test
  public void refreshTabletLeaderFromConsensus_returnsFalseOnEmptyLeaderUuid() {
    primeCacheWithTablet();

    TabletConsensusInfoPB info = TabletConsensusInfoPB.newBuilder()
        .setTabletId(ByteString.copyFromUtf8(TABLET_ID))
        .setConsensusState(ConsensusStatePB.newBuilder()
            .setCurrentTerm(7L)
            .setLeaderUuid("")  // election in progress; nothing actionable
            .setConfig(RaftConfigPB.newBuilder().build()))
        .build();

    assertFalse(client.refreshTabletLeaderFromConsensus(TABLET_ID, info));
  }

  @Test
  public void refreshTabletLeaderFromConsensus_returnsFalseOnNullInfo() {
    primeCacheWithTablet();
    assertFalse(client.refreshTabletLeaderFromConsensus(TABLET_ID, null));
  }

  @Test
  public void refreshTabletLeaderFromConsensus_delegatesToApplyLeaderHint() {
    // Wire the tablet into the cache so the lookup hits.
    primeCacheWithTablet();
    tablet.setTabletClientsForTest(Arrays.asList(tsA, tsB, tsC), 0);

    TabletConsensusInfoPB info = consensusInfoWithLeader(UUID_C);
    assertTrue(client.refreshTabletLeaderFromConsensus(TABLET_ID, info));

    // The end-to-end observable effect: clientFor now routes to C.
    assertSame(tsC, client.clientFor(tablet));
    assertEquals(2, tablet.leaderIndexForTest());
  }

  @Test
  public void refreshTabletLeaderFromConsensus_propagatesApplyHintFailure() {
    // Cache has the tablet but the hinted peer isn't in tabletServers. Should pass
    // through applyLeaderHint's false return rather than throwing.
    primeCacheWithTablet();
    tablet.setTabletClientsForTest(Arrays.asList(tsA, tsB), 0);

    TabletConsensusInfoPB info = consensusInfoWithLeader(UUID_C);  // C not in [A, B]
    assertFalse(client.refreshTabletLeaderFromConsensus(TABLET_ID, info));

    // Cache state preserved exactly.
    assertEquals(0, tablet.leaderIndexForTest());
    assertSame(tsA, client.clientFor(tablet));
  }

  // ---------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------

  /**
   * Register {@link #tablet} in the meta-cache so
   * {@link AsyncYBClient#refreshTabletLeaderFromConsensus} can find it by tabletId. Mirrors
   * what {@code discoverTablets} does on a normal master lookup; bypasses the DNS/Netty
   * setup we don't need for these tests.
   */
  private void primeCacheWithTablet() {
    client.putTabletForTest(TABLET_ID, tablet);
  }

  private static TabletConsensusInfoPB consensusInfoWithLeader(String leaderUuid) {
    return TabletConsensusInfoPB.newBuilder()
        .setTabletId(ByteString.copyFromUtf8(TABLET_ID))
        .setConsensusState(ConsensusStatePB.newBuilder()
            .setCurrentTerm(7L)
            .setLeaderUuid(leaderUuid)
            // The real wire format always carries the full raft config alongside the
            // leader_uuid (see FillTabletConsensusInfo in service_util.h). We only need
            // a non-null builder here \u2014 applyLeaderHint never reads it.
            .setConfig(RaftConfigPB.newBuilder().build()))
        .build();
  }
}
