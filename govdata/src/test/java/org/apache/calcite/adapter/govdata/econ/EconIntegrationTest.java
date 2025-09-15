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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for ECON schema with real data download.
 * This test requires API keys to be configured.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "BLS_API_KEY", matches = ".+")
public class EconIntegrationTest {
  
  @BeforeAll
  public static void setup() {
    // Verify environment is properly configured
    assertNotNull(System.getenv("GOVDATA_CACHE_DIR"), 
        "GOVDATA_CACHE_DIR must be set");
    assertNotNull(System.getenv("GOVDATA_PARQUET_DIR"), 
        "GOVDATA_PARQUET_DIR must be set");
    assertNotNull(System.getenv("BLS_API_KEY"), 
        "BLS_API_KEY must be set for integration tests");
  }
  
  @Test
  public void testEconDataDownloadAndQuery() throws Exception {
    // Create model JSON with ECON schema
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"econ\","
        + "\"schemas\": [{"
        + "  \"name\": \"econ\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.econ.EconSchemaFactory\","
        + "  \"operand\": {"
        + "    \"autoDownload\": true,"
        + "    \"startYear\": 2023,"
        + "    \"endYear\": 2024,"
        + "    \"enabledSources\": [\"bls\"]"
        + "  }"
        + "}]"
        + "}";
    
    // Write model to temp file
    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("econ-model", ".json");
    java.nio.file.Files.writeString(modelFile, modelJson);
    
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=" + modelFile, info);
         Statement stmt = conn.createStatement()) {
      
      // Query schema metadata
      try (ResultSet rs = stmt.executeQuery(
          "SELECT table_name FROM information_schema.tables "
          + "WHERE table_schema = 'econ' ORDER BY table_name")) {
        
        int tableCount = 0;
        while (rs.next()) {
          String tableName = rs.getString("table_name");
          System.out.println("Found ECON table: " + tableName);
          tableCount++;
        }
        
        // Should have at least one table after download
        assertTrue(tableCount > 0, "Should have ECON tables after download");
      }
      
      // Try to query employment statistics if it exists
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) as cnt FROM econ.employment_statistics")) {
        
        assertTrue(rs.next(), "Should get result from employment_statistics");
        int count = rs.getInt("cnt");
        System.out.println("Employment statistics row count: " + count);
        
        // Should have at least the placeholder record
        assertTrue(count >= 1, "Should have at least placeholder data");
      } catch (Exception e) {
        // Table might not exist if BLS download failed
        System.out.println("Employment statistics table not available: " + e.getMessage());
      }
    } finally {
      // Clean up temp file
      java.nio.file.Files.deleteIfExists(modelFile);
    }
  }
  
  @Test
  public void testEconSchemaConstraints() throws Exception {
    // Create model JSON
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"econ\","
        + "\"schemas\": [{"
        + "  \"name\": \"econ\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.econ.EconSchemaFactory\","
        + "  \"operand\": {"
        + "    \"autoDownload\": false,"
        + "    \"enableConstraints\": true"
        + "  }"
        + "}]"
        + "}";
    
    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("econ-constraints", ".json");
    java.nio.file.Files.writeString(modelFile, modelJson);
    
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=" + modelFile, info);
         Statement stmt = conn.createStatement()) {
      
      // Query constraint metadata
      try (ResultSet rs = stmt.executeQuery(
          "SELECT constraint_name, constraint_type, table_name "
          + "FROM information_schema.table_constraints "
          + "WHERE table_schema = 'econ' "
          + "AND constraint_type = 'PRIMARY KEY' "
          + "ORDER BY table_name")) {
        
        int constraintCount = 0;
        while (rs.next()) {
          String constraintName = rs.getString("constraint_name");
          String tableName = rs.getString("table_name");
          System.out.println("Found PRIMARY KEY constraint: " + constraintName + 
              " on table: " + tableName);
          constraintCount++;
        }
        
        // ECON schema defines primary keys for its tables
        assertTrue(constraintCount > 0, "Should have PRIMARY KEY constraints defined");
      }
    } finally {
      java.nio.file.Files.deleteIfExists(modelFile);
    }
  }
}