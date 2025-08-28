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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests demonstrating SQL:2003 compliance for the Splunk adapter.
 * These tests verify that the adapter supports all major SQL:2003 features
 * through Calcite's execution engine.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "CALCITE_TEST_SPLUNK", matches = "true")
public class SplunkSQL2003ComplianceTest extends SplunkTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkSQL2003ComplianceTest.class);
  
  private Connection connection;

  @BeforeAll
  public void setUp() throws Exception {
    loadConnectionProperties();
    
    if (!splunkAvailable) {
      throw new IllegalStateException("Splunk connection not configured");
    }
    
    // Create a more comprehensive test schema
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
        + "        name:'events',"
        + "        search:'search index=_internal earliest=-1h | head 200',"
        + "        fields:["
        + "          {name:'_time',type:'TIMESTAMP'},"
        + "          {name:'source',type:'VARCHAR'},"
        + "          {name:'sourcetype',type:'VARCHAR'},"
        + "          {name:'host',type:'VARCHAR'},"
        + "          {name:'_raw',type:'VARCHAR'}"
        + "        ]"
        + "      },"
        + "      {"
        + "        name:'metrics',"
        + "        search:'search index=_internal earliest=-30m | head 100',"
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
    LOGGER.info("Connected for SQL:2003 compliance testing");
  }

  // ==================== BASIC SQL:2003 FEATURES ====================

  /**
   * Test basic SELECT with column aliases (SQL:2003 mandatory)
   */
  @Test
  public void testSelectWithAliases() throws Exception {
    String sql = "SELECT sourcetype AS event_type, host AS server_name "
        + "FROM events "
        + "WHERE sourcetype IS NOT NULL "
        + "LIMIT 5";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String eventType = rs.getString("event_type");
        String serverName = rs.getString("server_name");
        assertNotNull(eventType);
        assertNotNull(serverName);
        count++;
      }
      
      assertTrue(count > 0 && count <= 5, "Should return 1-5 results");
      LOGGER.info("Column aliases test passed with {} rows", count);
    }
  }

  /**
   * Test CASE expressions (SQL:2003 mandatory)
   */
  @Test
  public void testCaseExpression() throws Exception {
    String sql = "SELECT sourcetype, "
        + "CASE "
        + "  WHEN sourcetype LIKE 'splunk%' THEN 'Internal' "
        + "  WHEN sourcetype LIKE 'scheduler%' THEN 'Scheduled' "
        + "  ELSE 'Other' "
        + "END AS category "
        + "FROM events "
        + "LIMIT 10";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String category = rs.getString("category");
        assertNotNull(category);
        
        // Verify CASE logic
        if (sourcetype != null) {
          if (sourcetype.startsWith("splunk")) {
            assertEquals("Internal", category);
          } else if (sourcetype.startsWith("scheduler")) {
            assertEquals("Scheduled", category);
          } else {
            assertEquals("Other", category);
          }
        }
        count++;
      }
      
      assertTrue(count > 0, "Should return results");
      LOGGER.info("CASE expression test passed with {} rows", count);
    }
  }

  /**
   * Test COALESCE and NULLIF functions (SQL:2003 mandatory)
   */
  @Test
  public void testNullHandlingFunctions() throws Exception {
    String sql = "SELECT "
        + "COALESCE(sourcetype, 'Unknown') AS sourcetype_safe, "
        + "NULLIF(host, '') AS host_or_null "
        + "FROM events "
        + "LIMIT 5";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String sourcetypeSafe = rs.getString("sourcetype_safe");
        assertNotNull(sourcetypeSafe, "COALESCE should never return null");
        count++;
      }
      
      assertTrue(count > 0, "Should return results");
      LOGGER.info("NULL handling functions test passed");
    }
  }

  // ==================== AGGREGATION FEATURES ====================

  /**
   * Test various aggregate functions (SQL:2003 mandatory)
   */
  @Test
  public void testAggregateFunctions() throws Exception {
    String sql = "SELECT "
        + "COUNT(*) AS total_count, "
        + "COUNT(DISTINCT sourcetype) AS unique_types, "
        + "COUNT(sourcetype) AS non_null_count "
        + "FROM events";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      assertTrue(rs.next(), "Should return one row");
      
      int totalCount = rs.getInt("total_count");
      int uniqueTypes = rs.getInt("unique_types");
      int nonNullCount = rs.getInt("non_null_count");
      
      assertTrue(totalCount > 0, "Should have total count");
      assertTrue(uniqueTypes > 0, "Should have unique types");
      assertTrue(uniqueTypes <= nonNullCount, "Unique <= non-null count");
      assertTrue(nonNullCount <= totalCount, "Non-null <= total count");
      
      assertFalse(rs.next(), "Should return exactly one row");
      LOGGER.info("Aggregate functions: total={}, unique={}, non_null={}", 
          totalCount, uniqueTypes, nonNullCount);
    }
  }

  /**
   * Test GROUP BY with HAVING clause (SQL:2003 mandatory)
   */
  @Test
  public void testGroupByHaving() throws Exception {
    String sql = "SELECT sourcetype, COUNT(*) as cnt, COUNT(DISTINCT host) as host_cnt "
        + "FROM events "
        + "GROUP BY sourcetype "
        + "HAVING COUNT(*) > 2 "
        + "ORDER BY cnt DESC";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int previousCount = Integer.MAX_VALUE;
      int groups = 0;
      
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        int cnt = rs.getInt("cnt");
        int hostCnt = rs.getInt("host_cnt");
        
        assertNotNull(sourcetype);
        assertTrue(cnt > 2, "HAVING clause should filter cnt > 2");
        assertTrue(hostCnt > 0, "Should have at least one host");
        assertTrue(cnt >= previousCount, "Should be ordered DESC");
        
        previousCount = cnt;
        groups++;
      }
      
      LOGGER.info("GROUP BY HAVING returned {} groups", groups);
    }
  }

  /**
   * Test GROUPING SETS (SQL:2003 optional feature T431)
   */
  @Test
  public void testGroupingSets() throws Exception {
    String sql = "SELECT sourcetype, host, COUNT(*) as cnt "
        + "FROM events "
        + "GROUP BY GROUPING SETS ((sourcetype), (host), ())";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int totalRows = 0;
      boolean hasSourcetypeOnly = false;
      boolean hasHostOnly = false;
      boolean hasGrandTotal = false;
      
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String host = rs.getString("host");
        int cnt = rs.getInt("cnt");
        
        if (sourcetype != null && host == null) {
          hasSourcetypeOnly = true;
        } else if (sourcetype == null && host != null) {
          hasHostOnly = true;
        } else if (sourcetype == null && host == null) {
          hasGrandTotal = true;
        }
        
        assertTrue(cnt > 0, "Count should be positive");
        totalRows++;
      }
      
      assertTrue(hasSourcetypeOnly, "Should have sourcetype grouping");
      assertTrue(hasHostOnly, "Should have host grouping");
      assertTrue(hasGrandTotal, "Should have grand total");
      
      LOGGER.info("GROUPING SETS returned {} rows", totalRows);
    }
  }

  // ==================== JOIN OPERATIONS ====================

  /**
   * Test INNER JOIN (SQL:2003 mandatory)
   */
  @Test
  public void testInnerJoin() throws Exception {
    String sql = "SELECT e.sourcetype, m.host "
        + "FROM events e "
        + "INNER JOIN metrics m ON e.source = m.source "
        + "LIMIT 10";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String host = rs.getString("host");
        assertNotNull(sourcetype);
        assertNotNull(host);
        count++;
      }
      
      assertTrue(count <= 10, "Should respect LIMIT");
      LOGGER.info("INNER JOIN returned {} rows", count);
    }
  }

  /**
   * Test LEFT OUTER JOIN (SQL:2003 mandatory)
   */
  @Test
  public void testLeftJoin() throws Exception {
    String sql = "SELECT e.sourcetype, m.host "
        + "FROM events e "
        + "LEFT JOIN metrics m ON e.source = m.source "
        + "WHERE e.sourcetype IS NOT NULL "
        + "LIMIT 10";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype, "Left side should always have value");
        // Right side (m.host) can be null
        count++;
      }
      
      assertTrue(count > 0, "Should return results");
      LOGGER.info("LEFT JOIN returned {} rows", count);
    }
  }

  /**
   * Test CROSS JOIN (SQL:2003 mandatory)
   */
  @Test
  public void testCrossJoin() throws Exception {
    // Use small limits to avoid huge result sets
    String sql = "SELECT e.sourcetype, m.host "
        + "FROM (SELECT DISTINCT sourcetype FROM events LIMIT 2) e "
        + "CROSS JOIN (SELECT DISTINCT host FROM metrics LIMIT 3) m";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String host = rs.getString("host");
        assertNotNull(sourcetype);
        assertNotNull(host);
        count++;
      }
      
      // Should have at most 2 * 3 = 6 rows
      assertTrue(count <= 6, "Cross join should produce at most 6 rows");
      LOGGER.info("CROSS JOIN returned {} rows", count);
    }
  }

  // ==================== SET OPERATIONS ====================

  /**
   * Test UNION (SQL:2003 mandatory)
   */
  @Test
  public void testUnion() throws Exception {
    String sql = "SELECT DISTINCT sourcetype FROM events "
        + "UNION "
        + "SELECT DISTINCT sourcetype FROM metrics";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      Set<String> uniqueSourcetypes = new HashSet<>();
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        // UNION should eliminate duplicates
        assertFalse(uniqueSourcetypes.contains(sourcetype), 
            "UNION should not have duplicates");
        uniqueSourcetypes.add(sourcetype);
      }
      
      assertTrue(uniqueSourcetypes.size() > 0, "Should return results");
      LOGGER.info("UNION returned {} unique sourcetypes", uniqueSourcetypes.size());
    }
  }

  /**
   * Test UNION ALL (SQL:2003 mandatory)
   */
  @Test
  public void testUnionAll() throws Exception {
    // Use parentheses to make precedence clear
    String sql = "(SELECT sourcetype FROM events WHERE sourcetype IS NOT NULL LIMIT 3) "
        + "UNION ALL "
        + "(SELECT sourcetype FROM metrics WHERE sourcetype IS NOT NULL LIMIT 2)";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      List<String> allResults = new ArrayList<>();
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        allResults.add(sourcetype);
      }
      
      // Should have up to 5 results (3 + 2), possibly with duplicates
      assertTrue(allResults.size() <= 5, "Should have at most 5 rows");
      assertTrue(allResults.size() > 0, "Should have at least one row");
      LOGGER.info("UNION ALL returned {} rows", allResults.size());
    }
  }

  /**
   * Test INTERSECT (SQL:2003 mandatory)
   */
  @Test
  public void testIntersect() throws Exception {
    String sql = "SELECT DISTINCT sourcetype FROM events "
        + "INTERSECT "
        + "SELECT DISTINCT sourcetype FROM metrics";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      Set<String> commonSourcetypes = new HashSet<>();
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        commonSourcetypes.add(sourcetype);
      }
      
      LOGGER.info("INTERSECT found {} common sourcetypes", commonSourcetypes.size());
    }
  }

  /**
   * Test EXCEPT (SQL:2003 mandatory)
   */
  @Test
  public void testExcept() throws Exception {
    String sql = "SELECT DISTINCT sourcetype FROM events "
        + "EXCEPT "
        + "SELECT DISTINCT sourcetype FROM metrics";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      Set<String> exclusiveSourcetypes = new HashSet<>();
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        exclusiveSourcetypes.add(sourcetype);
      }
      
      LOGGER.info("EXCEPT found {} exclusive sourcetypes", exclusiveSourcetypes.size());
    }
  }

  // ==================== WINDOW FUNCTIONS ====================

  /**
   * Test ROW_NUMBER() window function (SQL:2003 feature T611)
   */
  @Test
  public void testRowNumber() throws Exception {
    String sql = "SELECT sourcetype, host, "
        + "ROW_NUMBER() OVER (PARTITION BY sourcetype ORDER BY host) as rn "
        + "FROM events "
        + "WHERE sourcetype IS NOT NULL "
        + "LIMIT 20";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      String lastSourcetype = null;
      int expectedRowNum = 1;
      int count = 0;
      
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        int rowNum = rs.getInt("rn");
        
        if (!sourcetype.equals(lastSourcetype)) {
          expectedRowNum = 1;
          lastSourcetype = sourcetype;
        }
        
        assertEquals(expectedRowNum++, rowNum, 
            "Row number should increment within partition");
        count++;
      }
      
      assertTrue(count > 0, "Should return results");
      LOGGER.info("ROW_NUMBER() test passed with {} rows", count);
    }
  }

  /**
   * Test RANK() and DENSE_RANK() window functions (SQL:2003 feature T612)
   */
  @Test
  public void testRankFunctions() throws Exception {
    String sql = "SELECT sourcetype, host, "
        + "RANK() OVER (PARTITION BY sourcetype ORDER BY host) as rnk, "
        + "DENSE_RANK() OVER (PARTITION BY sourcetype ORDER BY host) as dense_rnk "
        + "FROM events "
        + "WHERE sourcetype IS NOT NULL "
        + "LIMIT 20";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        int rank = rs.getInt("rnk");
        int denseRank = rs.getInt("dense_rnk");
        
        assertTrue(rank > 0, "RANK should be positive");
        assertTrue(denseRank > 0, "DENSE_RANK should be positive");
        assertTrue(denseRank <= rank, "DENSE_RANK <= RANK");
        count++;
      }
      
      assertTrue(count > 0, "Should return results");
      LOGGER.info("RANK functions test passed with {} rows", count);
    }
  }

  /**
   * Test LAG() and LEAD() window functions (SQL:2003 feature T614)
   */
  @Test
  public void testLagLead() throws Exception {
    String sql = "SELECT sourcetype, "
        + "LAG(sourcetype, 1) OVER (ORDER BY sourcetype) as prev_type, "
        + "LEAD(sourcetype, 1) OVER (ORDER BY sourcetype) as next_type "
        + "FROM events "
        + "WHERE sourcetype IS NOT NULL "
        + "LIMIT 10";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      String previousSourcetype = null;
      
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        String prevType = rs.getString("prev_type");
        
        if (count > 0) {
          // LAG should return the previous row's value
          assertEquals(previousSourcetype, prevType, 
              "LAG should return previous value");
        }
        
        previousSourcetype = sourcetype;
        count++;
      }
      
      assertTrue(count > 0, "Should return results");
      LOGGER.info("LAG/LEAD test passed with {} rows", count);
    }
  }

  // ==================== SUBQUERIES ====================

  /**
   * Test scalar subquery (SQL:2003 mandatory)
   */
  @Test
  public void testScalarSubquery() throws Exception {
    String sql = "SELECT sourcetype, "
        + "(SELECT COUNT(*) FROM metrics m WHERE m.sourcetype = e.sourcetype) as metric_count "
        + "FROM events e "
        + "WHERE sourcetype IS NOT NULL "
        + "LIMIT 5";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        int metricCount = rs.getInt("metric_count");
        
        assertNotNull(sourcetype);
        assertTrue(metricCount >= 0, "Metric count should be non-negative");
        count++;
      }
      
      assertTrue(count > 0, "Should return results");
      LOGGER.info("Scalar subquery test passed with {} rows", count);
    }
  }

  /**
   * Test EXISTS subquery (SQL:2003 mandatory)
   */
  @Test
  public void testExistsSubquery() throws Exception {
    String sql = "SELECT DISTINCT e.sourcetype "
        + "FROM events e "
        + "WHERE EXISTS ("
        + "  SELECT 1 FROM metrics m "
        + "  WHERE m.sourcetype = e.sourcetype"
        + ") "
        + "LIMIT 10";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        count++;
      }
      
      LOGGER.info("EXISTS subquery returned {} rows", count);
    }
  }

  /**
   * Test IN subquery (SQL:2003 mandatory)
   */
  @Test
  public void testInSubquery() throws Exception {
    String sql = "SELECT sourcetype, host "
        + "FROM events "
        + "WHERE sourcetype IN ("
        + "  SELECT DISTINCT sourcetype FROM metrics"
        + ") "
        + "LIMIT 10";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        count++;
      }
      
      assertTrue(count <= 10, "Should respect LIMIT");
      LOGGER.info("IN subquery returned {} rows", count);
    }
  }

  // ==================== COMMON TABLE EXPRESSIONS (CTEs) ====================

  /**
   * Test WITH clause / CTE (SQL:2003 optional feature T121)
   */
  @Test
  public void testCommonTableExpression() throws Exception {
    String sql = "WITH source_counts AS ("
        + "  SELECT sourcetype, COUNT(*) as cnt "
        + "  FROM events "
        + "  GROUP BY sourcetype"
        + ") "
        + "SELECT sourcetype, cnt "
        + "FROM source_counts "
        + "WHERE cnt > 5 "
        + "ORDER BY cnt DESC";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int previousCount = Integer.MAX_VALUE;
      int count = 0;
      
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        int cnt = rs.getInt("cnt");
        
        assertNotNull(sourcetype);
        assertTrue(cnt > 5, "Should only show counts > 5");
        assertTrue(cnt <= previousCount, "Should be ordered DESC");
        
        previousCount = cnt;
        count++;
      }
      
      LOGGER.info("CTE query returned {} rows", count);
    }
  }

  // ==================== DATA TYPES AND FUNCTIONS ====================

  /**
   * Test string functions (SQL:2003 mandatory)
   */
  @Test
  public void testStringFunctions() throws Exception {
    String sql = "SELECT "
        + "UPPER(sourcetype) as upper_type, "
        + "LOWER(host) as lower_host, "
        + "LENGTH(source) as source_len, "
        + "SUBSTRING(sourcetype, 1, 5) as type_prefix "
        + "FROM events "
        + "WHERE sourcetype IS NOT NULL "
        + "LIMIT 5";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String upperType = rs.getString("upper_type");
        String lowerHost = rs.getString("lower_host");
        int sourceLen = rs.getInt("source_len");
        String typePrefix = rs.getString("type_prefix");
        
        assertNotNull(upperType);
        assertEquals(upperType, upperType.toUpperCase(), "Should be uppercase");
        
        if (lowerHost != null) {
          assertEquals(lowerHost, lowerHost.toLowerCase(), "Should be lowercase");
        }
        
        assertTrue(sourceLen >= 0, "Length should be non-negative");
        
        if (typePrefix != null) {
          assertTrue(typePrefix.length() <= 5, "Substring should be max 5 chars");
        }
        
        count++;
      }
      
      assertTrue(count > 0, "Should return results");
      LOGGER.info("String functions test passed with {} rows", count);
    }
  }

  /**
   * Test date/time functions (SQL:2003 mandatory)
   */
  @Test
  public void testDateTimeFunctions() throws Exception {
    String sql = "SELECT "
        + "EXTRACT(YEAR FROM _time) as year, "
        + "EXTRACT(MONTH FROM _time) as month, "
        + "EXTRACT(DAY FROM _time) as day, "
        + "EXTRACT(HOUR FROM _time) as hour "
        + "FROM events "
        + "LIMIT 5";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        int year = rs.getInt("year");
        int month = rs.getInt("month");
        int day = rs.getInt("day");
        int hour = rs.getInt("hour");
        
        assertTrue(year >= 2000 && year <= 2100, "Year should be reasonable");
        assertTrue(month >= 1 && month <= 12, "Month should be 1-12");
        assertTrue(day >= 1 && day <= 31, "Day should be 1-31");
        assertTrue(hour >= 0 && hour <= 23, "Hour should be 0-23");
        
        count++;
      }
      
      assertTrue(count > 0, "Should return results");
      LOGGER.info("Date/time functions test passed with {} rows", count);
    }
  }

  /**
   * Test CAST and type conversion (SQL:2003 mandatory)
   */
  @Test
  public void testCastAndConversion() throws Exception {
    String sql = "SELECT "
        + "CAST(sourcetype AS VARCHAR(20)) as type_varchar, "
        + "CAST(LENGTH(source) AS DECIMAL(10,2)) as len_decimal, "
        + "CAST('123' AS INTEGER) as str_to_int "
        + "FROM events "
        + "LIMIT 5";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String typeVarchar = rs.getString("type_varchar");
        double lenDecimal = rs.getDouble("len_decimal");
        int strToInt = rs.getInt("str_to_int");
        
        assertNotNull(typeVarchar);
        assertTrue(lenDecimal >= 0, "Length should be non-negative");
        assertEquals(123, strToInt, "String should convert to 123");
        
        count++;
      }
      
      assertTrue(count > 0, "Should return results");
      LOGGER.info("CAST test passed with {} rows", count);
    }
  }

  // ==================== PREPARED STATEMENTS ====================

  /**
   * Test prepared statements with parameters (SQL:2003 mandatory)
   */
  @Test
  public void testPreparedStatements() throws Exception {
    String sql = "SELECT sourcetype, host "
        + "FROM events "
        + "WHERE sourcetype = ? "
        + "LIMIT ?";
    
    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
      // First get a valid sourcetype
      String targetSourcetype = null;
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT DISTINCT sourcetype FROM events WHERE sourcetype IS NOT NULL LIMIT 1")) {
        if (rs.next()) {
          targetSourcetype = rs.getString("sourcetype");
        }
      }
      
      if (targetSourcetype != null) {
        pstmt.setString(1, targetSourcetype);
        pstmt.setInt(2, 5);
        
        try (ResultSet rs = pstmt.executeQuery()) {
          int count = 0;
          while (rs.next()) {
            String sourcetype = rs.getString("sourcetype");
            assertEquals(targetSourcetype, sourcetype, "Should match parameter");
            count++;
          }
          
          assertTrue(count > 0 && count <= 5, "Should return 1-5 results");
          LOGGER.info("Prepared statement test passed with {} rows", count);
        }
      }
    }
  }

  // ==================== OFFSET/FETCH (SQL:2003 feature T611) ====================

  /**
   * Test OFFSET and FETCH for pagination
   */
  @Test
  public void testOffsetFetch() throws Exception {
    // First, get total count
    int totalCount = 0;
    String countSql = "SELECT COUNT(*) as cnt FROM events";
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(countSql)) {
      if (rs.next()) {
        totalCount = rs.getInt("cnt");
      }
    }
    
    // Test pagination
    String sql = "SELECT sourcetype FROM events "
        + "ORDER BY sourcetype "
        + "OFFSET 5 ROWS "
        + "FETCH NEXT 10 ROWS ONLY";
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String sourcetype = rs.getString("sourcetype");
        assertNotNull(sourcetype);
        count++;
      }
      
      assertTrue(count <= 10, "Should fetch at most 10 rows");
      if (totalCount > 5) {
        assertTrue(count > 0, "Should return results when offset < total");
      }
      
      LOGGER.info("OFFSET/FETCH returned {} rows", count);
    }
  }
}