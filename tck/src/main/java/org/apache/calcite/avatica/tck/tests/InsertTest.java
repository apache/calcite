/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.avatica.tck.tests;

import org.junit.Assume;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for <code>INSERT</code>.
 */
public class InsertTest extends BaseTckTest {

  @Test public void simpleInsert() throws Exception {
    final String tableName = getTableName();
    try (Statement stmt = connection.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName
          + " (pk integer not null primary key, col1 varchar(10))";
      assertFalse(stmt.execute(sql));

      for (int i = 0; i < 10; i++) {
        sql = "INSERT INTO " + tableName + " values (" + i + ", '" + i + "')";
        assertEquals(1, stmt.executeUpdate(sql));
      }

      ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName);
      assertNotNull(results);
      for (int i = 0; i < 10; i++) {
        assertTrue(results.next());
        assertEquals(i, results.getInt(1));
        assertEquals(Integer.toString(i), results.getString(1));
      }
      assertFalse(results.next());
      results.close();
    }
  }

  @Test public void preparedStatementInsert() throws Exception {
    final String tableName = getTableName();
    final String insertSql = "INSERT INTO " + tableName + " values(?, ?)";
    try (Statement stmt = connection.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName
          + " (pk integer not null primary key, col1 varchar(10))";
      assertFalse(stmt.execute(sql));

      try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
        for (int i = 0; i < 10; i++) {
          pstmt.setInt(1, i);
          pstmt.setString(2, "a_" + Integer.toString(i));
          assertEquals(1, pstmt.executeUpdate());
        }
      }

      ResultSet results = stmt.executeQuery("SELECT COUNT(pk) from " + tableName);
      assertNotNull(results);
      assertTrue(results.next());
      assertEquals(10, results.getInt(1));

      results = stmt.executeQuery("SELECT * from " + tableName);
      assertNotNull(results);
      for (int i = 0; i < 10; i++) {
        assertTrue(results.next());
        assertEquals(i, results.getInt(1));
        assertEquals("a_" + i, results.getString(2));
      }
      assertFalse(results.next());
      results.close();
    }
  }

  @Test public void batchInsert() throws Exception {
    final String tableName = getTableName();
    try (Statement stmt = connection.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName
          + " (pk integer not null primary key, col1 varchar(10))";
      assertFalse(stmt.execute(sql));

      for (int i = 0; i < 10; i++) {
        sql = "INSERT INTO " + tableName + " values (" + i + ", '" + i + "')";
        try {
          stmt.addBatch(sql);
        } catch (SQLFeatureNotSupportedException e) {
          // batch isn't supported in this version, gracefully ignore,
          Assume.assumeTrue("Batch update is not support by the client", false);
        }
      }

      int[] updateCounts = stmt.executeBatch();
      int[] expectedUpdateCounts = new int[10];
      Arrays.fill(expectedUpdateCounts, 1);
      assertArrayEquals(expectedUpdateCounts, updateCounts);

      ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName);
      assertNotNull(results);
      for (int i = 0; i < 10; i++) {
        assertTrue(results.next());
        assertEquals(i, results.getInt(1));
        assertEquals(Integer.toString(i), results.getString(1));
      }
      assertFalse(results.next());
      results.close();
    }
  }

  @Test public void preparedBatchInsert() throws Exception {
    final String tableName = getTableName();
    final String insertSql = "INSERT INTO " + tableName + " values(?, ?)";
    try (Statement stmt = connection.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName
          + " (pk integer not null primary key, col1 varchar(10))";
      assertFalse(stmt.execute(sql));

      try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
        for (int i = 0; i < 10; i++) {
          pstmt.setInt(1, i);
          pstmt.setString(2, "a_" + Integer.toString(i));
          try {
            pstmt.addBatch();
          } catch (SQLFeatureNotSupportedException e) {
            // batch isn't supported in this version, gracefully ignore,
            Assume.assumeTrue("Batch update is not support by the client", false);
          }
        }

        int[] updateCounts = pstmt.executeBatch();
        int[] expectedUpdateCounts = new int[10];
        Arrays.fill(expectedUpdateCounts, 1);
        assertArrayEquals(expectedUpdateCounts, updateCounts);
      }

      ResultSet results = stmt.executeQuery("SELECT COUNT(pk) from " + tableName);
      assertNotNull(results);
      assertTrue(results.next());
      assertEquals(10, results.getInt(1));

      results = stmt.executeQuery("SELECT * from " + tableName);
      assertNotNull(results);
      for (int i = 0; i < 10; i++) {
        assertTrue(results.next());
        assertEquals(i, results.getInt(1));
        assertEquals("a_" + i, results.getString(2));
      }
      assertFalse(results.next());
      results.close();
    }
  }

  @Test public void commitAndRollback() throws Exception {
    final String tableName = getTableName();

    // Disable autoCommit
    connection.setAutoCommit(false);
    assertFalse(connection.getAutoCommit());

    try (Statement stmt = connection.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName
          + " (pk integer not null primary key, col1 varchar(10))";
      assertFalse(stmt.execute(sql));

      for (int i = 0; i < 10; i++) {
        sql = "INSERT INTO " + tableName + " values (" + i + ", '" + i + "')";
        assertEquals(1, stmt.executeUpdate(sql));
        if (i == 4) {
          // Rollback after the first 5 updates
          connection.rollback();
        }
      }
      connection.commit();

      ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName);
      assertNotNull(results);
      for (int i = 5; i < 10; i++) {
        assertTrue(results.next());
        assertEquals(i, results.getInt(1));
        assertEquals(Integer.toString(i), results.getString(1));
      }
      assertFalse(results.next());
      results.close();
    }
  }
}

// End InsertTest.java
