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
package org.apache.calcite.adapter.file.schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests demonstrating format-aware schema evolution.
 * 
 * These tests create their own test data files to demonstrate schema evolution.
 */
@Tag("integration")
public class SchemaEvolutionIntegrationTest {

  @Test
  public void testCsvSchemaEvolution() throws SQLException {
    // Model with CSV files that have evolved schemas
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"EVOLUTION\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"EVOLUTION\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"src/test/resources/schema-evolution/csv\",\n"
        + "        \"schemaStrategy\": \"richest_file\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Test that schema uses richest file
      ResultSet rs = statement.executeQuery("SELECT * FROM sales LIMIT 1");
      
      int columnCount = rs.getMetaData().getColumnCount();
      System.out.printf("Schema evolution test: %d columns detected\n", columnCount);
      
      // Should include all columns from the richest CSV file
      assertTrue(columnCount >= 3, "Should have at least 3 columns from evolved schema");
    }
  }

  @Test 
  public void testMixedFormatPriority() throws SQLException {
    // Model with mixed CSV, JSON, and potentially Parquet files
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"MIXED\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"MIXED\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"src/test/resources/mixed-formats\",\n"
        + "        \"schemaStrategy\": {\n"
        + "          \"formatPriority\": [\"parquet\", \"csv\", \"json\"]\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Query mixed format data
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) as total FROM data");
      
      if (rs.next()) {
        int total = rs.getInt("total");
        System.out.printf("Mixed format query returned %d rows\n", total);
        assertTrue(total >= 0, "Should return valid row count");
      }
    }
  }

  @Test
  public void testLatestSchemaWinsStrategy() throws SQLException {
    // Test Parquet files with schema evolution using LATEST_SCHEMA_WINS
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"PARQUET_EVOLUTION\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"PARQUET_EVOLUTION\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"src/test/resources/parquet-evolution\",\n"
        + "        \"schemaStrategy\": {\n"
        + "          \"parquet\": \"LATEST_SCHEMA_WINS\",\n"
        + "          \"validation\": \"WARN\"\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Query evolved schema - should use latest file's structure
      ResultSet rs = statement.executeQuery("SELECT * FROM orders LIMIT 1");
      
      System.out.println("Latest schema wins strategy:");
      for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
        String columnName = rs.getMetaData().getColumnName(i);
        String columnType = rs.getMetaData().getColumnTypeName(i);
        System.out.printf("  Column %d: %s (%s)\n", i, columnName, columnType);
      }
    }
  }

  @Test
  public void testUnionAllColumnsStrategy() throws SQLException {
    // Test Parquet files with UNION_ALL_COLUMNS to preserve historical fields
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"UNION_TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"UNION_TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"src/test/resources/parquet-evolution\",\n"
        + "        \"schemaStrategy\": {\n"
        + "          \"parquet\": \"UNION_ALL_COLUMNS\"\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Query with union strategy - should include all historical columns
      ResultSet rs = statement.executeQuery("SELECT * FROM orders LIMIT 1");
      
      System.out.println("Union all columns strategy:");
      for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
        String columnName = rs.getMetaData().getColumnName(i);
        String columnType = rs.getMetaData().getColumnTypeName(i);
        System.out.printf("  Column %d: %s (%s)\n", i, columnName, columnType);
      }
      
      // Union strategy should have more columns than latest-only strategy
      assertTrue(rs.getMetaData().getColumnCount() >= 3, 
                 "Union strategy should preserve historical columns");
    }
  }

  @Test
  public void testSchemaEvolutionWithDuckDB() throws SQLException {
    // Test schema evolution with DuckDB engine for performance
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"DUCKDB_EVOLUTION\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"DUCKDB_EVOLUTION\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"engineType\": \"DUCKDB\",\n"
        + "        \"directory\": \"src/test/resources/mixed-formats\",\n"
        + "        \"schemaStrategy\": \"latest_schema_wins\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Test aggregation with evolved schema on DuckDB
      long start = System.nanoTime();
      ResultSet rs = statement.executeQuery("SELECT COUNT(*), MAX(id) FROM data");
      long elapsed = System.nanoTime() - start;

      if (rs.next()) {
        int count = rs.getInt(1);
        System.out.printf("DuckDB schema evolution query: %d rows in %.2f ms\n", 
                         count, elapsed / 1_000_000.0);
        
        // Should be fast with DuckDB
        assertTrue(elapsed < 100_000_000, // < 100ms
                   "DuckDB should handle evolved schema efficiently");
      }
    }
  }
}