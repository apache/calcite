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

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * TCK test case to verify binary data can be written and read correctly.
 */
public class BinaryTest extends BaseTckTest {

  @Test public void readWriteBinaryData() throws Exception {
    final String tableName = getTableName();
    try (Statement stmt = connection.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName
          + " (pk integer not null primary key, col1 binary(10))";
      assertFalse(stmt.execute(sql));

      try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO " + tableName
          + " values (?,?)")) {
        for (int i = 0; i < 10; i++) {
          pstmt.setInt(1, i);
          pstmt.setBytes(2, ("bytes" + i).getBytes(UTF_8));
          assertEquals(1, pstmt.executeUpdate());
        }
      }

      ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName);
      assertNotNull(results);
      for (int i = 0; i < 10; i++) {
        assertTrue(results.next());
        assertEquals(i, results.getInt(1));
        byte[] expectedContent = ("bytes" + i).getBytes(UTF_8);
        byte[] expected = new byte[10];
        System.arraycopy(expectedContent, 0, expected, 0, expectedContent.length);
        assertArrayEquals(expected, results.getBytes(2));
      }
      assertFalse(results.next());
      results.close();
    }
  }

  @Test public void selectivelyReadBinaryData() throws Exception {
    final String tableName = getTableName();
    try (Statement stmt = connection.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName
          + " (pk integer not null primary key, col1 binary(10))";
      assertFalse(stmt.execute(sql));

      try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO " + tableName
          + " values (?,?)")) {
        for (int i = 0; i < 10; i++) {
          pstmt.setInt(1, i);
          pstmt.setBytes(2, ("bytes" + i).getBytes(UTF_8));
          assertEquals(1, pstmt.executeUpdate());
        }
      }

      try (PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM " + tableName
          + " WHERE col1 = ?")) {
        byte[] expectedContent = ("bytes" + 4).getBytes(UTF_8);
        byte[] expected = new byte[10];
        System.arraycopy(expectedContent, 0, expected, 0, expectedContent.length);
        pstmt.setBytes(1, expected);
        ResultSet results = pstmt.executeQuery();
        assertNotNull(results);
        assertTrue(results.next());
        assertEquals(4, results.getInt(1));
        assertArrayEquals(expected, results.getBytes(2));
        assertFalse(results.next());
        results.close();
      }
    }
  }
}

// End BinaryTest.java
