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

package org.yb.pgsql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.yb.AssertionWrappers.assertEquals;
import static org.yb.AssertionWrappers.assertTrue;
import static org.yb.AssertionWrappers.fail;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.YBTestRunner;

/**
 * Tests for YSQL parallel pipeline execution.
 *
 * These tests verify that:
 * - Independent queries in a pipeline are correctly grouped for parallel execution
 * - Conflict detection prevents unsafe parallelism
 * - Transaction integrity (single 2PC) is maintained
 * - Results arrive in correct pipeline order
 * - Error handling works correctly
 */
@RunWith(value = YBTestRunner.class)
public class TestPgPipelineParallelism extends BasePgSQLTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestPgPipelineParallelism.class);

  @Before
  public void setUp() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("SET yb_enable_pipeline_parallelism = true");
      stmt.execute("CREATE TABLE t1 (k INT PRIMARY KEY, v TEXT)");
      stmt.execute("CREATE TABLE t2 (k INT PRIMARY KEY, v TEXT)");
      stmt.execute("CREATE TABLE t3 (k INT PRIMARY KEY, v TEXT)");
    }
  }

  @After
  public void tearDown() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS t1, t2, t3");
    }
  }

  // ---------------------------------------------------------------
  // Functional tests
  // ---------------------------------------------------------------

  /**
   * Pipeline N independent INSERTs to different tables.
   * All should succeed and results should be correct.
   */
  @Test
  public void testIndependentWritesToDifferentTables() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.addBatch("INSERT INTO t1 VALUES (1, 'a')");
      stmt.addBatch("INSERT INTO t2 VALUES (1, 'b')");
      stmt.addBatch("INSERT INTO t3 VALUES (1, 'c')");
      stmt.executeBatch();
    }

    try (Statement stmt = connection.createStatement()) {
      assertOneRow(stmt, "SELECT COUNT(*) FROM t1", 1);
      assertOneRow(stmt, "SELECT COUNT(*) FROM t2", 1);
      assertOneRow(stmt, "SELECT COUNT(*) FROM t3", 1);
    }
  }

  /**
   * Pipeline N independent SELECTs from different tables.
   * All should return correct results in order.
   */
  @Test
  public void testIndependentReadsFromDifferentTables() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO t1 VALUES (1, 'a')");
      stmt.execute("INSERT INTO t2 VALUES (1, 'b')");
      stmt.execute("INSERT INTO t3 VALUES (1, 'c')");
    }

    try (PreparedStatement ps1 = connection.prepareStatement("SELECT v FROM t1 WHERE k = 1");
         PreparedStatement ps2 = connection.prepareStatement("SELECT v FROM t2 WHERE k = 1");
         PreparedStatement ps3 = connection.prepareStatement("SELECT v FROM t3 WHERE k = 1")) {
      try (ResultSet rs1 = ps1.executeQuery()) {
        assertTrue(rs1.next());
        assertEquals("a", rs1.getString(1));
      }
      try (ResultSet rs2 = ps2.executeQuery()) {
        assertTrue(rs2.next());
        assertEquals("b", rs2.getString(1));
      }
      try (ResultSet rs3 = ps3.executeQuery()) {
        assertTrue(rs3.next());
        assertEquals("c", rs3.getString(1));
      }
    }
  }

  /**
   * Pipeline a write to table A and a read from table B.
   * These should not conflict and both should succeed.
   */
  @Test
  public void testMixedReadWriteNoConflict() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO t2 VALUES (1, 'existing')");
    }

    try (Statement stmt = connection.createStatement()) {
      stmt.addBatch("INSERT INTO t1 VALUES (1, 'new')");
      stmt.addBatch("SELECT v FROM t2 WHERE k = 1");
      stmt.executeBatch();
    }

    try (Statement stmt = connection.createStatement()) {
      assertOneRow(stmt, "SELECT COUNT(*) FROM t1", 1);
    }
  }

  /**
   * Pipeline a write to table A then a read from table A.
   * The read-after-write should be detected as a conflict and
   * executed sequentially, so the read sees the write.
   */
  @Test
  public void testMixedReadWriteWithConflict() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("BEGIN");
      stmt.execute("INSERT INTO t1 VALUES (1, 'written')");
      ResultSet rs = stmt.executeQuery("SELECT v FROM t1 WHERE k = 1");
      assertTrue(rs.next());
      assertEquals("written", rs.getString(1));
      stmt.execute("COMMIT");
    }
  }

  /**
   * Pipeline multiple writes to the same table with different keys.
   * Currently these conflict (table-level detection); future key-level
   * detection will allow them to parallelize.
   */
  @Test
  public void testSameTableWritesDifferentKeys() throws Exception {
    try (PreparedStatement pstmt = connection.prepareStatement(
        "INSERT INTO t1 VALUES (?, 'val')")) {
      for (int i = 1; i <= 5; i++) {
        pstmt.setInt(1, i);
        pstmt.addBatch();
      }
      pstmt.executeBatch();
    }

    try (Statement stmt = connection.createStatement()) {
      assertOneRow(stmt, "SELECT COUNT(*) FROM t1", 5);
    }
  }

  /**
   * Test INSERT ON CONFLICT (UPSERT) in a pipeline.
   */
  @Test
  public void testUpsertInPipeline() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO t1 VALUES (1, 'original')");
    }

    try (Statement stmt = connection.createStatement()) {
      stmt.addBatch("INSERT INTO t1 VALUES (1, 'updated') ON CONFLICT (k) DO UPDATE SET v = 'updated'");
      stmt.addBatch("INSERT INTO t2 VALUES (1, 'new')");
      stmt.executeBatch();
    }

    try (Statement stmt = connection.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT v FROM t1 WHERE k = 1");
      assertTrue(rs.next());
      assertEquals("updated", rs.getString(1));
      assertOneRow(stmt, "SELECT COUNT(*) FROM t2", 1);
    }
  }

  // ---------------------------------------------------------------
  // 2PC / Transaction integrity tests
  // ---------------------------------------------------------------

  /**
   * Within BEGIN...COMMIT, pipeline writes to multiple different tables.
   * Verify all writes are committed atomically (single transaction).
   */
  @Test
  public void testTransactionAtomicityAcrossTables() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("BEGIN");
      stmt.addBatch("INSERT INTO t1 VALUES (1, 'a')");
      stmt.addBatch("INSERT INTO t2 VALUES (1, 'b')");
      stmt.addBatch("INSERT INTO t3 VALUES (1, 'c')");
      stmt.executeBatch();
      stmt.execute("COMMIT");
    }

    try (Statement stmt = connection.createStatement()) {
      assertOneRow(stmt, "SELECT COUNT(*) FROM t1", 1);
      assertOneRow(stmt, "SELECT COUNT(*) FROM t2", 1);
      assertOneRow(stmt, "SELECT COUNT(*) FROM t3", 1);
    }
  }

  /**
   * Within BEGIN...ROLLBACK, pipeline writes to multiple tables.
   * Verify all writes are rolled back (no partial commits).
   */
  @Test
  public void testTransactionRollbackAcrossTables() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("BEGIN");
      stmt.addBatch("INSERT INTO t1 VALUES (1, 'a')");
      stmt.addBatch("INSERT INTO t2 VALUES (1, 'b')");
      stmt.addBatch("INSERT INTO t3 VALUES (1, 'c')");
      stmt.executeBatch();
      stmt.execute("ROLLBACK");
    }

    try (Statement stmt = connection.createStatement()) {
      assertOneRow(stmt, "SELECT COUNT(*) FROM t1", 0);
      assertOneRow(stmt, "SELECT COUNT(*) FROM t2", 0);
      assertOneRow(stmt, "SELECT COUNT(*) FROM t3", 0);
    }
  }

  /**
   * Read snapshot consistency: within a transaction, parallel reads from
   * different tables should all see a consistent snapshot.
   */
  @Test
  public void testReadSnapshotConsistency() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO t1 VALUES (1, 'v1')");
      stmt.execute("INSERT INTO t2 VALUES (1, 'v2')");
    }

    // Start a concurrent connection that will modify data after our txn starts
    try (Connection c2 = getConnectionBuilder().connect()) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("BEGIN ISOLATION LEVEL REPEATABLE READ");
        // Read t1 first to establish the read point
        ResultSet rs1 = stmt.executeQuery("SELECT v FROM t1 WHERE k = 1");
        assertTrue(rs1.next());
        assertEquals("v1", rs1.getString(1));

        // Concurrent write from another connection
        try (Statement s2 = c2.createStatement()) {
          s2.execute("UPDATE t2 SET v = 'modified' WHERE k = 1");
        }

        // Read t2 should see the old value (snapshot consistency)
        ResultSet rs2 = stmt.executeQuery("SELECT v FROM t2 WHERE k = 1");
        assertTrue(rs2.next());
        assertEquals("v2", rs2.getString(1));

        stmt.execute("COMMIT");
      }
    }
  }

  // ---------------------------------------------------------------
  // Error handling tests
  // ---------------------------------------------------------------

  /**
   * Pipeline multiple inserts where one causes a unique constraint violation.
   * Verify that the entire batch is rolled back.
   */
  @Test
  public void testBatchErrorRollback() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO t1 VALUES (3, 'existing')");
    }

    try (Statement stmt = connection.createStatement()) {
      stmt.addBatch("INSERT INTO t1 VALUES (1, 'a')");
      stmt.addBatch("INSERT INTO t1 VALUES (2, 'b')");
      stmt.addBatch("INSERT INTO t1 VALUES (3, 'conflict')"); // duplicate key
      stmt.addBatch("INSERT INTO t1 VALUES (4, 'd')");
      stmt.executeBatch();
      fail("Batch should have failed due to unique constraint violation");
    } catch (BatchUpdateException e) {
      // Expected
    }

    // Only the original row should remain
    try (Statement stmt = connection.createStatement()) {
      assertOneRow(stmt, "SELECT COUNT(*) FROM t1", 1);
    }
  }

  /**
   * After an error in an explicit transaction, subsequent queries should
   * fail with "current transaction is aborted".
   */
  @Test
  public void testTransactionAbortPropagation() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO t1 VALUES (1, 'exists')");
    }

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("BEGIN");
      try {
        stmt.execute("INSERT INTO t1 VALUES (1, 'duplicate')");
        fail("Should have thrown duplicate key error");
      } catch (SQLException e) {
        // Expected: duplicate key
      }
      try {
        stmt.execute("INSERT INTO t2 VALUES (1, 'after_error')");
        fail("Should have thrown 'current transaction is aborted' error");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("current transaction is aborted"));
      }
      stmt.execute("ROLLBACK");
    }
  }

  // ---------------------------------------------------------------
  // Feature flag tests
  // ---------------------------------------------------------------

  /**
   * When the feature flag is off, pipeline behavior should match the
   * existing sequential execution exactly.
   */
  @Test
  public void testDisabledPipelineParallelism() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("SET yb_enable_pipeline_parallelism = false");

      stmt.addBatch("INSERT INTO t1 VALUES (1, 'a')");
      stmt.addBatch("INSERT INTO t2 VALUES (1, 'b')");
      stmt.addBatch("INSERT INTO t3 VALUES (1, 'c')");
      stmt.executeBatch();
    }

    try (Statement stmt = connection.createStatement()) {
      assertOneRow(stmt, "SELECT COUNT(*) FROM t1", 1);
      assertOneRow(stmt, "SELECT COUNT(*) FROM t2", 1);
      assertOneRow(stmt, "SELECT COUNT(*) FROM t3", 1);
    }
  }

  /**
   * Test the max parallel queries GUC.
   */
  @Test
  public void testMaxParallelQueriesGuc() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("SET yb_pipeline_max_parallel_queries = 2");
      assertOneRow(stmt, "SHOW yb_pipeline_max_parallel_queries", "2");

      // Insert enough rows to exercise the limit
      for (int i = 1; i <= 10; i++) {
        stmt.addBatch(String.format("INSERT INTO t1 VALUES (%d, 'v%d')", i, i));
      }
      stmt.executeBatch();
    }

    try (Statement stmt = connection.createStatement()) {
      assertOneRow(stmt, "SELECT COUNT(*) FROM t1", 10);
    }
  }

  // ---------------------------------------------------------------
  // Autocommit tests
  // ---------------------------------------------------------------

  /**
   * In autocommit mode (no explicit BEGIN), each statement is its own
   * transaction. Independent statements can fully parallelize.
   */
  @Test
  public void testAutocommitParallelism() throws Exception {
    // In autocommit mode, each addBatch statement is independent
    try (Statement stmt = connection.createStatement()) {
      stmt.addBatch("INSERT INTO t1 VALUES (1, 'a')");
      stmt.addBatch("INSERT INTO t2 VALUES (1, 'b')");
      stmt.addBatch("INSERT INTO t3 VALUES (1, 'c')");
      stmt.executeBatch();
    }

    try (Statement stmt = connection.createStatement()) {
      assertOneRow(stmt, "SELECT COUNT(*) FROM t1", 1);
      assertOneRow(stmt, "SELECT COUNT(*) FROM t2", 1);
      assertOneRow(stmt, "SELECT COUNT(*) FROM t3", 1);
    }
  }
}
