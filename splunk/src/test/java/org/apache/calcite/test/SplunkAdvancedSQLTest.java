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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests advanced SQL features that should be handled by Calcite's execution engine
 * on top of SplunkTableScan.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "CALCITE_TEST_SPLUNK", matches = "true")
public class SplunkAdvancedSQLTest extends SplunkTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkAdvancedSQLTest.class);

  private Connection connection;

  @BeforeAll
  public void setUp() throws Exception {
    loadConnectionProperties();

    if (!splunkAvailable) {
      throw new IllegalStateException("Splunk connection not configured");
    }

    // Create test tables
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
        + "    tables:["
        + "      {"
        + "        name:'logs_a',"
        + "        search:'search index=_internal earliest=-1h | head 50',"
        + "        fields:["
        + "          {name:'_time',type:'TIMESTAMP'},"
        + "          {name:'source',type:'VARCHAR'},"
        + "          {name:'sourcetype',type:'VARCHAR'},"
        + "          {name:'host',type:'VARCHAR'}"
        + "        ]"
        + "      },"
        + "      {"
        + "        name:'logs_b',"
        + "        search:'search index=_internal earliest=-1h | head 50',"
        + "        fields:["
        + "          {name:'_time',type:'TIMESTAMP'},"
        + "          {name:'source',type:'VARCHAR'},"
        + "          {name:'sourcetype',type:'VARCHAR'},"
        + "          {name:'host',type:'VARCHAR'}"
        + "        ]"
        + "      }"
        + "    ]"
        + "  }"
        + "}]"
        + "}",
        SPLUNK_URL, SPLUNK_USER, SPLUNK_PASSWORD, DISABLE_SSL_VALIDATION);

    Properties info = new Properties();
    info.setProperty("model", "inline:" + modelJson);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    connection = DriverManager.getConnection("jdbc:calcite:", info);
    LOGGER.info("Connected for advanced SQL tests");
  }

  /**
   * Test self-join - Calcite should handle this with EnumerableJoin
   */
  @Test public void testSelfJoin() throws Exception {
    String sql = "SELECT a.sourcetype as a_type, b.sourcetype as b_type "
        + "FROM logs_a a "
        + "INNER JOIN logs_b b ON a.source = b.source "
        + "LIMIT 5";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      while (rs.next()) {
        String aType = rs.getString("a_type");
        String bType = rs.getString("b_type");
        assertNotNull(aType);
        assertNotNull(bType);
        count++;
        LOGGER.debug("Join result: a_type={}, b_type={}", aType, bType);
      }

      assertTrue(count <= 5, "Should respect LIMIT");
      LOGGER.info("JOIN returned {} rows", count);
    }
  }

  /**
   * Test UNION ALL - Calcite should handle with EnumerableUnion
   */
  @Test public void testUnionAll() throws Exception {
    // Simple UNION ALL test without complex filters
    String sql = "(SELECT sourcetype, 'Part1' as src FROM logs_a) "
        + "UNION ALL "
        + "(SELECT host as sourcetype, 'Part2' as src FROM logs_a) "
        + "LIMIT 10";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int countPart1 = 0;
      int countPart2 = 0;
      int total = 0;

      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String src = rs.getString("src");
        assertNotNull(sourcetype);
        assertNotNull(src);

        if ("Part1".equals(src)) {
          countPart1++;
        } else if ("Part2".equals(src)) {
          countPart2++;
        }
        total++;
      }

      assertTrue(total > 0, "Should return results");
      assertTrue(total <= 10, "Should respect outer LIMIT");
      assertEquals(countPart1 + countPart2, total, "Total should equal sum of both parts");
      LOGGER.info("UNION ALL returned {} + {} = {} rows (Part1: {}, Part2: {})",
          total, countPart1 + countPart2, countPart1, countPart2);
    }
  }

  /**
   * Test window function ROW_NUMBER() - Calcite should handle with EnumerableWindow
   */
  @Test public void testWindowFunctionRowNumber() throws Exception {
    String sql = "SELECT sourcetype, host, "
        + "ROW_NUMBER() OVER (PARTITION BY sourcetype ORDER BY host) as rn "
        + "FROM logs_a "
        + "WHERE sourcetype IS NOT NULL "
        + "LIMIT 10";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      String lastSourceType = null;
      int lastRowNum = 0;

      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String host = rs.getString("host");
        int rowNum = rs.getInt("rn");

        assertNotNull(sourcetype);
        assertTrue(rowNum > 0, "Row number should be positive");

        // Check row numbering resets on partition change
        if (!sourcetype.equals(lastSourceType)) {
          assertEquals(1, rowNum, "Row number should reset to 1 for new partition");
          lastSourceType = sourcetype;
          lastRowNum = 1;
        } else {
          assertEquals(lastRowNum + 1, rowNum, "Row number should increment within partition");
          lastRowNum = rowNum;
        }

        count++;
        LOGGER.debug("Window result: sourcetype={}, host={}, rn={}", sourcetype, host, rowNum);
      }

      assertTrue(count > 0, "Should return results");
      assertTrue(count <= 10, "Should respect LIMIT");
      LOGGER.info("ROW_NUMBER() returned {} rows", count);
    }
  }

  /**
   * Test RANK() window function
   */
  @Test public void testWindowFunctionRank() throws Exception {
    String sql = "SELECT sourcetype, host, "
        + "RANK() OVER (PARTITION BY sourcetype ORDER BY host) as rnk "
        + "FROM logs_a "
        + "WHERE sourcetype IS NOT NULL "
        + "LIMIT 10";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String host = rs.getString("host");
        int rank = rs.getInt("rnk");

        assertNotNull(sourcetype);
        assertTrue(rank > 0, "Rank should be positive");
        count++;
        LOGGER.debug("RANK result: sourcetype={}, host={}, rnk={}", sourcetype, host, rank);
      }

      assertTrue(count > 0, "Should return results");
      assertTrue(count <= 10, "Should respect LIMIT");
      LOGGER.info("RANK() returned {} rows", count);
    }
  }

  /**
   * Test complex nested query with subquery
   */
  @Test public void testSubquery() throws Exception {
    String sql = "SELECT sourcetype, cnt FROM ("
        + "  SELECT sourcetype, COUNT(*) as cnt "
        + "  FROM logs_a "
        + "  GROUP BY sourcetype"
        + ") t "
        + "WHERE cnt > 1 "
        + "ORDER BY cnt DESC";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      int lastCount = Integer.MAX_VALUE;

      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        int cnt = rs.getInt("cnt");

        assertNotNull(sourcetype);
        assertTrue(cnt > 1, "Count should be > 1 due to WHERE clause");
        assertTrue(cnt <= lastCount, "Results should be ordered DESC");
        lastCount = cnt;
        count++;
        LOGGER.debug("Subquery result: sourcetype={}, cnt={}", sourcetype, cnt);
      }

      LOGGER.info("Subquery returned {} rows", count);
    }
  }

  /**
   * Test HAVING clause with GROUP BY
   */
  @Test public void testHavingClause() throws Exception {
    String sql = "SELECT sourcetype, COUNT(*) as cnt "
        + "FROM logs_a "
        + "GROUP BY sourcetype "
        + "HAVING COUNT(*) > 5";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        int cnt = rs.getInt("cnt");

        assertNotNull(sourcetype);
        assertTrue(cnt > 5, "Count should be > 5 due to HAVING clause");
        count++;
        LOGGER.debug("HAVING result: sourcetype={}, cnt={}", sourcetype, cnt);
      }

      LOGGER.info("HAVING clause returned {} groups", count);
    }
  }

  /**
   * Test LAG() and LEAD() window functions
   */
  @Test public void testLagLeadWindowFunctions() throws Exception {
    String sql = "SELECT sourcetype, host, "
        + "LAG(host) OVER (PARTITION BY sourcetype ORDER BY host) as prev_host, "
        + "LEAD(host) OVER (PARTITION BY sourcetype ORDER BY host) as next_host "
        + "FROM logs_a "
        + "WHERE sourcetype IS NOT NULL "
        + "ORDER BY sourcetype, host "
        + "LIMIT 10";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      String lastSourcetype = null;
      String lastHost = null;

      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String host = rs.getString("host");
        String prevHost = rs.getString("prev_host");
        String nextHost = rs.getString("next_host");

        assertNotNull(sourcetype);
        assertNotNull(host);

        // Within same sourcetype partition, verify LAG/LEAD logic
        if (sourcetype.equals(lastSourcetype)) {
          // LAG should return the previous host within the same sourcetype
          assertEquals(lastHost, prevHost,
              "LAG should return previous host in same sourcetype partition");
        } else {
          // First row in new partition should have NULL LAG
          assertNull(prevHost, "First row in partition should have NULL LAG");
        }

        count++;
        lastSourcetype = sourcetype;
        lastHost = host;

        LOGGER.debug("LAG/LEAD result: sourcetype={}, host={}, prev_host={}, next_host={}",
            sourcetype, host, prevHost, nextHost);
      }

      assertTrue(count > 0, "Should return results");
      assertTrue(count <= 10, "Should respect LIMIT");
      LOGGER.info("LAG/LEAD returned {} rows", count);
    }
  }

  /**
   * Test GROUPING SETS - Advanced grouping functionality
   */
  @Test public void testGroupingSets() throws Exception {
    String sql = "SELECT sourcetype, host, COUNT(*) as cnt "
        + "FROM logs_a "
        + "WHERE sourcetype IS NOT NULL AND host IS NOT NULL "
        + "GROUP BY GROUPING SETS ((sourcetype), (host), ()) "
        + "ORDER BY cnt DESC "
        + "LIMIT 15";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      int nullSourcetypeCount = 0;
      int nullHostCount = 0;
      int bothNullCount = 0;

      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String host = rs.getString("host");
        int cnt = rs.getInt("cnt");

        assertTrue(cnt > 0, "Count should be positive");

        // Categorize the grouping sets
        if (sourcetype == null && host == null) {
          bothNullCount++;  // Grand total row
        } else if (sourcetype == null) {
          nullSourcetypeCount++;  // Host-only grouping
        } else if (host == null) {
          nullHostCount++;  // Sourcetype-only grouping
        }

        count++;
        LOGGER.debug("GROUPING SETS result: sourcetype={}, host={}, cnt={}",
            sourcetype, host, cnt);
      }

      assertTrue(count > 0, "Should return results");
      assertTrue(count <= 15, "Should respect LIMIT");
      assertTrue(bothNullCount > 0 || nullSourcetypeCount > 0 || nullHostCount > 0,
          "Should have grouping set aggregations");
      LOGGER.info("GROUPING SETS returned {} rows (nullST={}, nullH={}, bothNull={})",
          count, nullSourcetypeCount, nullHostCount, bothNullCount);
    }
  }

  /**
   * Test ROLLUP - Hierarchical grouping
   */
  @Test public void testRollup() throws Exception {
    String sql = "SELECT sourcetype, host, COUNT(*) as cnt "
        + "FROM logs_a "
        + "WHERE sourcetype IS NOT NULL AND host IS NOT NULL "
        + "GROUP BY ROLLUP (sourcetype, host) "
        + "ORDER BY sourcetype NULLS LAST, host NULLS LAST "
        + "LIMIT 20";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      int subtotalCount = 0;  // Rows with null host but non-null sourcetype
      int grandTotalCount = 0;  // Row with both null

      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String host = rs.getString("host");
        int cnt = rs.getInt("cnt");

        assertTrue(cnt > 0, "Count should be positive");

        if (sourcetype == null && host == null) {
          grandTotalCount++;
        } else if (host == null && sourcetype != null) {
          subtotalCount++;
        }

        count++;
        LOGGER.debug("ROLLUP result: sourcetype={}, host={}, cnt={}",
            sourcetype, host, cnt);
      }

      assertTrue(count > 0, "Should return results");
      assertTrue(count <= 20, "Should respect LIMIT");
      LOGGER.info("ROLLUP returned {} rows (subtotals={}, grand_total={})",
          count, subtotalCount, grandTotalCount);
    }
  }

  /**
   * Test CUBE - All possible grouping combinations
   */
  @Test public void testCube() throws Exception {
    String sql = "SELECT sourcetype, host, COUNT(*) as cnt "
        + "FROM logs_a "
        + "WHERE sourcetype IS NOT NULL AND host IS NOT NULL "
        + "GROUP BY CUBE (sourcetype, host) "
        + "ORDER BY cnt DESC "
        + "LIMIT 25";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int count = 0;
      int detailRows = 0;      // Both sourcetype and host non-null
      int sourcetypeOnlyRows = 0;  // Host null, sourcetype non-null
      int hostOnlyRows = 0;    // Sourcetype null, host non-null
      int grandTotalRows = 0;  // Both null

      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String host = rs.getString("host");
        int cnt = rs.getInt("cnt");

        assertTrue(cnt > 0, "Count should be positive");

        // Categorize CUBE results
        if (sourcetype != null && host != null) {
          detailRows++;
        } else if (sourcetype != null && host == null) {
          sourcetypeOnlyRows++;
        } else if (sourcetype == null && host != null) {
          hostOnlyRows++;
        } else {
          grandTotalRows++;
        }

        count++;
        LOGGER.debug("CUBE result: sourcetype={}, host={}, cnt={}",
            sourcetype, host, cnt);
      }

      assertTrue(count > 0, "Should return results");
      assertTrue(count <= 25, "Should respect LIMIT");
      LOGGER.info("CUBE returned {} rows (detail={}, ST_only={}, host_only={}, grand={})",
          count, detailRows, sourcetypeOnlyRows, hostOnlyRows, grandTotalRows);
    }
  }
}
