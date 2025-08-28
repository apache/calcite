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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify OFFSET functionality works correctly with Splunk adapter.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "CALCITE_TEST_SPLUNK", matches = "true")
public class SplunkOffsetVerificationTest extends SplunkTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkOffsetVerificationTest.class);
  
  private Connection connection;

  @BeforeAll
  public void setUp() throws Exception {
    loadConnectionProperties();
    
    if (!splunkAvailable) {
      throw new IllegalStateException("Splunk connection not configured");
    }
    
    // Create a test table with more diverse data
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
        + "      name:'test_data',"
        + "      search:'search index=_internal earliest=-1h | head 100',"
        + "      fields:["
        + "        {name:'_time',type:'TIMESTAMP'},"
        + "        {name:'source',type:'VARCHAR'},"
        + "        {name:'sourcetype',type:'VARCHAR'},"
        + "        {name:'host',type:'VARCHAR'}"
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
    LOGGER.info("Connected to Splunk for OFFSET verification");
  }

  @Test
  public void testOffsetWithUniqueColumn() throws Exception {
    // Use _time which should have unique values
    String sql1 = "SELECT _time FROM test_data ORDER BY _time DESC LIMIT 5";
    List<String> firstBatch = new ArrayList<>();
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql1)) {
      while (rs.next()) {
        firstBatch.add(rs.getString("_time"));
      }
    }
    
    assertEquals(5, firstBatch.size(), "Should get 5 rows in first batch");
    
    // Get next 5 with OFFSET
    String sql2 = "SELECT _time FROM test_data ORDER BY _time DESC LIMIT 5 OFFSET 5";
    List<String> secondBatch = new ArrayList<>();
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql2)) {
      while (rs.next()) {
        secondBatch.add(rs.getString("_time"));
      }
    }
    
    assertEquals(5, secondBatch.size(), "Should get 5 rows in second batch");
    
    // Verify no overlap between batches
    for (String time1 : firstBatch) {
      for (String time2 : secondBatch) {
        assertNotEquals(time1, time2, 
            "OFFSET should skip rows - found duplicate: " + time1);
      }
    }
    
    LOGGER.info("OFFSET correctly skipped {} rows", firstBatch.size());
  }

  @Test
  public void testOffsetBeyondResultSet() throws Exception {
    // First count total rows
    String countSql = "SELECT COUNT(*) as total FROM test_data";
    int totalRows = 0;
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(countSql)) {
      if (rs.next()) {
        totalRows = rs.getInt("total");
      }
    }
    
    LOGGER.info("Total rows in dataset: {}", totalRows);
    
    // Try OFFSET beyond total rows
    String sql = String.format("SELECT source FROM test_data LIMIT 10 OFFSET %d", 
        totalRows + 10);
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals(0, count, "OFFSET beyond dataset should return 0 rows");
    }
    
    LOGGER.info("OFFSET beyond dataset correctly returned 0 rows");
  }

  @Test
  public void testOffsetWithDistinctValues() throws Exception {
    // Get distinct sourcetypes ordered
    String sql = "SELECT DISTINCT sourcetype FROM test_data ORDER BY sourcetype";
    List<String> allDistinct = new ArrayList<>();
    
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        allDistinct.add(rs.getString("sourcetype"));
      }
    }
    
    LOGGER.info("Found {} distinct sourcetypes", allDistinct.size());
    
    if (allDistinct.size() >= 2) {
      // Test OFFSET with distinct values
      String sqlOffset = "SELECT DISTINCT sourcetype FROM test_data "
          + "ORDER BY sourcetype LIMIT 1 OFFSET 1";
      
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(sqlOffset)) {
        assertTrue(rs.next(), "Should get one result with OFFSET 1");
        String offsetValue = rs.getString("sourcetype");
        
        assertEquals(allDistinct.get(1), offsetValue, 
            "OFFSET 1 should return second distinct value");
        LOGGER.info("OFFSET correctly returned second distinct value: {}", offsetValue);
      }
    } else {
      LOGGER.warn("Not enough distinct values to properly test OFFSET");
    }
  }
}