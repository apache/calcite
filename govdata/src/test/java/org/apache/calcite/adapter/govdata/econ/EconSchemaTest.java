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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.jdbc.CalciteConnection;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Economic Data Schema with BLS tables.
 */
@Tag("unit")
public class EconSchemaTest {
  
  private static Path tempDir;
  
  @BeforeAll
  public static void setUp() throws Exception {
    tempDir = Files.createTempDirectory("econ-test");
    tempDir.toFile().deleteOnExit();
  }
  
  @Test
  public void testBlsTablesWithComments() throws Exception {
    // Create model with BLS API key
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"econ\","
        + "\"schemas\": [{"
        + "  \"name\": \"econ\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\": {"
        + "    \"dataSource\": \"econ\","
        + "    \"blsApiKey\": \"test-key\","
        + "    \"cacheDirectory\": \"" + tempDir.toString() + "\""
        + "  }"
        + "}]"
        + "}";
    
    File modelFile = new File(tempDir.toFile(), "econ-model.json");
    Files.write(modelFile.toPath(), modelJson.getBytes());
    
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=" + modelFile.getAbsolutePath(), props)) {
      
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      assertNotNull(calciteConn);
      
      DatabaseMetaData meta = conn.getMetaData();
      
      // Check that BLS tables exist
      try (ResultSet rs = meta.getTables(null, "ECON", "%", null)) {
        boolean foundEmployment = false;
        boolean foundInflation = false;
        boolean foundWage = false;
        boolean foundRegional = false;
        
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          if ("EMPLOYMENT_STATISTICS".equals(tableName)) {
            foundEmployment = true;
            String comment = rs.getString("REMARKS");
            assertNotNull(comment);
            assertTrue(comment.contains("employment"));
            assertTrue(comment.contains("unemployment"));
          } else if ("INFLATION_METRICS".equals(tableName)) {
            foundInflation = true;
            String comment = rs.getString("REMARKS");
            assertNotNull(comment);
            assertTrue(comment.contains("CPI"));
            assertTrue(comment.contains("PPI"));
          } else if ("WAGE_GROWTH".equals(tableName)) {
            foundWage = true;
            String comment = rs.getString("REMARKS");
            assertNotNull(comment);
            assertTrue(comment.contains("earnings"));
          } else if ("REGIONAL_EMPLOYMENT".equals(tableName)) {
            foundRegional = true;
            String comment = rs.getString("REMARKS");
            assertNotNull(comment);
            assertTrue(comment.contains("State"));
            assertTrue(comment.contains("metropolitan"));
          }
        }
        
        assertTrue(foundEmployment, "employment_statistics table not found");
        assertTrue(foundInflation, "inflation_metrics table not found");
        assertTrue(foundWage, "wage_growth table not found");
        assertTrue(foundRegional, "regional_employment table not found");
      }
      
      // Check column comments for employment_statistics
      try (ResultSet rs = meta.getColumns(null, "ECON", "EMPLOYMENT_STATISTICS", null)) {
        boolean foundDate = false;
        boolean foundSeriesId = false;
        boolean foundValue = false;
        
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          String comment = rs.getString("REMARKS");
          
          if ("DATE".equals(columnName)) {
            foundDate = true;
            assertNotNull(comment);
            assertTrue(comment.contains("Observation date"));
          } else if ("SERIES_ID".equals(columnName)) {
            foundSeriesId = true;
            assertNotNull(comment);
            assertTrue(comment.contains("BLS series identifier"));
          } else if ("VALUE".equals(columnName)) {
            foundValue = true;
            assertNotNull(comment);
            assertTrue(comment.contains("Metric value"));
          }
        }
        
        assertTrue(foundDate, "date column not found");
        assertTrue(foundSeriesId, "series_id column not found");
        assertTrue(foundValue, "value column not found");
      }
      
      // Test schema comment
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT comment FROM information_schema.schemata WHERE schema_name = 'ECON'")) {
        assertTrue(rs.next());
        String schemaComment = rs.getString("comment");
        assertNotNull(schemaComment);
        assertTrue(schemaComment.contains("economic data"));
        assertTrue(schemaComment.contains("BLS"));
        assertTrue(schemaComment.contains("FRED"));
      }
    }
  }
  
  @Test
  public void testBlsApiClient() throws Exception {
    String apiKey = System.getenv("BLS_API_KEY");
    if (apiKey == null) {
      // Skip test if no API key available
      System.out.println("Skipping BLS API test - no API key configured");
      return;
    }
    
    BlsApiClient client = new BlsApiClient(apiKey);
    
    // Test fetching unemployment rate for 2024
    List<Map<String, Object>> data = client.fetchTimeSeries(BlsApiClient.Series.UNEMPLOYMENT_RATE, 2024, 2024);
    
    assertNotNull(data);
    assertTrue(data.size() > 0, "Should have some data points");
    
    // Verify data structure
    Map<String, Object> firstPoint = data.get(0);
    assertNotNull(firstPoint.get("date"));
    assertNotNull(firstPoint.get("value"));
    assertNotNull(firstPoint.get("series_id"));
    assertEquals(BlsApiClient.Series.UNEMPLOYMENT_RATE, firstPoint.get("series_id"));
  }
}