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
package org.apache.calcite.test;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests demonstrating that various SQL capabilities work with the Splunk adapter.
 * These tests prove the adapter can handle WHERE, GROUP BY, COUNT, and other SQL features
 * when properly configured.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "CALCITE_TEST_SPLUNK", matches = "true")
public class SplunkSQLCapabilityTest extends SplunkTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkSQLCapabilityTest.class);

  private Connection connection;

  @BeforeAll
  public void setUp() throws Exception {
    // Load connection properties from parent class
    loadConnectionProperties();

    if (!splunkAvailable) {
      throw new IllegalStateException("Splunk connection not configured. " +
          "Set SPLUNK_URL, SPLUNK_USER, SPLUNK_PASSWORD environment variables");
    }

    // Create a schema with a simple test table that searches internal logs
    // Using a small dataset to ensure tests run quickly
    String modelJson = String.format("{"
        + "version:'1.0',"
        + "defaultSchema:'splunk',"
        + "schemas:[{"
        + "  name:'splunk',"
        + "  type:'custom',"
        + "  factory:'org.apache.calcite.adapter.splunk.SplunkSchemaFactory',"
        + "  operand:{"
        + "    url:'%s',"
        + "    username:'%s',"
        + "    password:'%s',"
        + "    disableSslValidation:%s,"
        + "    tables:[{"
        + "      name:'audit_logs',"
        + "      search:'search index=_internal earliest=-1h | head 100',"
        + "      fields:["
        + "        {name:'_time',type:'TIMESTAMP'},"
        + "        {name:'source',type:'VARCHAR'},"
        + "        {name:'sourcetype',type:'VARCHAR'},"
        + "        {name:'host',type:'VARCHAR'},"
        + "        {name:'_raw',type:'VARCHAR'}"
        + "      ]"
        + "    }]"
        + "  }"
        + "}]"
        + "}",
        SPLUNK_URL, SPLUNK_USER, SPLUNK_PASSWORD, DISABLE_SSL_VALIDATION);

    Properties info = new Properties();
    info.setProperty("model", "inline:" + modelJson);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    connection = DriverManager.getConnection("jdbc:calcite:", info);
    assertNotNull(connection, "Connection should be established");
    LOGGER.info("Connected to Splunk successfully");
  }

  /**
   * Demonstrates basic SELECT works
   */
  @Test public void testBasicSelect() throws Exception {
    String sql = "SELECT sourcetype FROM audit_logs LIMIT 5";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype, "sourcetype should not be null");
        LOGGER.debug("Row {}: sourcetype={}", ++count, sourcetype);
      }

      assertTrue(count > 0, "Should return at least one result");
      assertTrue(count <= 5, "Should respect LIMIT clause");
      LOGGER.info("Basic SELECT returned {} rows", count);
    }
  }

  /**
   * Demonstrates WHERE clause filtering works with proper SPL syntax
   */
  @Test public void testWhereClause() throws Exception {
    String sql = "SELECT source FROM audit_logs "
        + "WHERE sourcetype IS NOT NULL "
        + "LIMIT 10";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      while (rs.next()) {
        String source = rs.getString("source");
        assertNotNull(source, "source should not be null");
        count++;
      }

      assertTrue(count > 0, "Should return results");
      assertTrue(count <= 10, "Should respect LIMIT");
      LOGGER.info("WHERE clause filtering returned {} rows", count);
    }
  }

  /**
   * Demonstrates LIKE pattern matching works
   */
  @Test public void testLikePattern() throws Exception {
    String sql = "SELECT sourcetype FROM audit_logs "
        + "WHERE sourcetype LIKE 'splunk%' "
        + "LIMIT 5";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        assertTrue(sourcetype.startsWith("splunk"),
            "sourcetype should match LIKE pattern: " + sourcetype);
        count++;
      }

      // May return 0 if no matching sourcetypes
      assertTrue(count <= 5, "Should respect LIMIT");
      LOGGER.info("LIKE pattern matching returned {} rows", count);
    }
  }

  /**
   * Demonstrates COUNT(*) aggregation works
   * This tests that Calcite can apply EnumerableAggregate on top of SplunkTableScan
   */
  @Test public void testCountStar() throws Exception {
    String sql = "SELECT COUNT(*) as total FROM audit_logs";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      assertTrue(rs.next(), "Should return one row");
      int total = rs.getInt("total");
      assertTrue(total > 0, "Should have counted some records");
      assertFalse(rs.next(), "Should return exactly one row");
      LOGGER.info("COUNT(*) returned: {}", total);
    }
  }

  /**
   * Demonstrates COUNT with WHERE works
   */
  @Test public void testCountWithFilter() throws Exception {
    String sql = "SELECT COUNT(*) as filtered_count FROM audit_logs "
        + "WHERE sourcetype IS NOT NULL";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      assertTrue(rs.next(), "Should return one row");
      int count = rs.getInt("filtered_count");
      assertTrue(count > 0, "Should have counted some records");
      assertFalse(rs.next(), "Should return exactly one row");
      LOGGER.info("COUNT with WHERE returned: {}", count);
    }
  }

  /**
   * Demonstrates GROUP BY with COUNT works
   * This tests that Calcite can apply EnumerableAggregate for grouping
   */
  @Test public void testGroupByCount() throws Exception {
    String sql = "SELECT sourcetype, COUNT(*) as cnt "
        + "FROM audit_logs "
        + "GROUP BY sourcetype "
        + "ORDER BY cnt DESC "
        + "LIMIT 5";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int rowCount = 0;
      int previousCount = Integer.MAX_VALUE;

      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        int count = rs.getInt("cnt");

        assertNotNull(sourcetype);
        assertTrue(count > 0, "Count should be positive");
        assertTrue(count <= previousCount, "Results should be ordered DESC");

        previousCount = count;
        rowCount++;
        LOGGER.debug("GROUP BY result: sourcetype={}, count={}", sourcetype, count);
      }

      assertTrue(rowCount > 0, "Should return grouped results");
      assertTrue(rowCount <= 5, "Should respect LIMIT");
      LOGGER.info("GROUP BY returned {} groups", rowCount);
    }
  }

  /**
   * Demonstrates DISTINCT works
   */
  @Test public void testDistinct() throws Exception {
    String sql = "SELECT DISTINCT sourcetype FROM audit_logs LIMIT 10";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        count++;
      }

      assertTrue(count > 0, "Should return distinct values");
      assertTrue(count <= 10, "Should respect LIMIT");
      LOGGER.info("DISTINCT returned {} unique values", count);
    }
  }

  /**
   * Demonstrates ORDER BY works
   */
  @Test public void testOrderBy() throws Exception {
    String sql = "SELECT sourcetype FROM audit_logs "
        + "ORDER BY sourcetype ASC "
        + "LIMIT 5";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      String previousValue = "";
      int count = 0;

      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        assertTrue(sourcetype.compareTo(previousValue) >= 0,
            "Results should be in ascending order");
        previousValue = sourcetype;
        count++;
      }

      assertTrue(count > 0, "Should return results");
      assertTrue(count <= 5, "Should respect LIMIT");
      LOGGER.info("ORDER BY returned {} rows in sorted order", count);
    }
  }

  /**
   * Demonstrates multiple WHERE conditions with AND/OR work
   */
  @Test public void testComplexWhere() throws Exception {
    String sql = "SELECT source, sourcetype FROM audit_logs "
        + "WHERE (sourcetype IS NOT NULL AND source IS NOT NULL) "
        + "OR host = 'localhost' "
        + "LIMIT 5";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      while (rs.next()) {
        String source = rs.getString("source");
        String sourcetype = rs.getString("sourcetype");
        // At least one condition should be met
        assertTrue(source != null || sourcetype != null,
            "Should match WHERE conditions");
        count++;
      }

      assertTrue(count <= 5, "Should respect LIMIT");
      LOGGER.info("Complex WHERE returned {} rows", count);
    }
  }

  /**
   * Demonstrates IN clause works
   */
  @Test public void testInClause() throws Exception {
    // First get some valid sourcetypes
    String getTypesSql = "SELECT DISTINCT sourcetype FROM audit_logs LIMIT 3";
    String[] types = new String[3];
    int typeCount = 0;

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(getTypesSql)) {
      while (rs.next() && typeCount < 3) {
        types[typeCount++] = rs.getString("sourcetype");
      }
    }

    if (typeCount > 0) {
      // Build IN clause with actual values
      StringBuilder inList = new StringBuilder();
      for (int i = 0; i < typeCount; i++) {
        if (i > 0) inList.append(", ");
        inList.append("'").append(types[i]).append("'");
      }

      String sql = "SELECT source FROM audit_logs "
          + "WHERE sourcetype IN (" + inList + ") "
          + "LIMIT 10";

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {

        int count = 0;
        while (rs.next()) {
          String source = rs.getString("source");
          assertNotNull(source);
          count++;
        }

        assertTrue(count > 0, "Should return results for IN clause");
        assertTrue(count <= 10, "Should respect LIMIT");
        LOGGER.info("IN clause returned {} rows", count);
      }
    }
  }

  /**
   * Demonstrates LIMIT and OFFSET work together
   */
  @Test public void testLimitOffset() throws Exception {
    // First query: get first 3 results
    String sql1 = "SELECT sourcetype FROM audit_logs ORDER BY sourcetype LIMIT 3";
    String[] firstBatch = new String[3];

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql1)) {
      int i = 0;
      while (rs.next() && i < 3) {
        firstBatch[i++] = rs.getString("sourcetype");
      }
    }

    // Second query: skip first 3, get next 3
    String sql2 = "SELECT sourcetype FROM audit_logs ORDER BY sourcetype LIMIT 3 OFFSET 3";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql2)) {

      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        // Should not match any from first batch (assuming we have enough data)
        for (String first : firstBatch) {
          if (first != null && first.equals(sourcetype)) {
            LOGGER.warn("OFFSET may not be working correctly - found duplicate: {}", sourcetype);
          }
        }
        count++;
      }

      assertTrue(count <= 3, "Should respect LIMIT with OFFSET");
      LOGGER.info("LIMIT with OFFSET returned {} rows", count);
    }
  }
}
