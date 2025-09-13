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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that table and column comments are queryable via SQL
 * through information_schema views, similar to PostgreSQL.
 */
@Tag("unit")
public class SqlCommentsTest {

  @TempDir
  File tempDir;

  @Test
  public void testTableCommentsViaSQL() throws Exception {
    // Create a model with SEC and GEO schemas
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"sec\",\n" +
        "  \"schemas\": [\n" +
        "    {\n" +
        "      \"name\": \"sec\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"dataSource\": \"sec\",\n" +
        "        \"directory\": \"" + tempDir.getAbsolutePath() + "/sec\",\n" +
        "        \"useMockData\": false\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"name\": \"geo\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"dataSource\": \"geo\",\n" +
        "        \"cacheDirectory\": \"" + tempDir.getAbsolutePath() + "/geo\",\n" +
        "        \"autoDownload\": false\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    // Create necessary directories
    new File(tempDir, "sec/sec-parquet").mkdirs();
    new File(tempDir, "geo").mkdirs();

    // Write the model file
    File modelFile = new File(tempDir, "comments-model.json");
    Files.write(modelFile.toPath(), modelJson.getBytes());

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
      
      // Query table comments from information_schema.tables
      String tableQuery = 
          "SELECT \"TABLE_SCHEMA\", \"TABLE_NAME\", \"TABLE_COMMENT\" " +
          "FROM metadata.\"TABLES\" " +
          "WHERE \"TABLE_SCHEMA\" IN ('sec', 'geo') " +
          "  AND \"TABLE_NAME\" IN ('financial_line_items', 'tiger_states') " +
          "ORDER BY \"TABLE_SCHEMA\", \"TABLE_NAME\"";
      
      System.out.println("\n=== Table Comments via SQL ===");
      System.out.println("Query: " + tableQuery);
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(tableQuery)) {
        
        while (rs.next()) {
          String schema = rs.getString("TABLE_SCHEMA");
          String table = rs.getString("TABLE_NAME");
          String comment = rs.getString("TABLE_COMMENT");
          
          System.out.printf("Schema: %s, Table: %s%n", schema, table);
          System.out.printf("  Comment: %s%n", comment);
          
          // Verify we got meaningful comments
          if ("financial_line_items".equalsIgnoreCase(table)) {
            assertNotNull(comment, "financial_line_items should have a comment");
            assertTrue(comment.toLowerCase().contains("financial") || 
                      comment.toLowerCase().contains("xbrl"),
                      "Comment should mention financial or XBRL");
          }
          
          if ("tiger_states".equalsIgnoreCase(table)) {
            assertNotNull(comment, "tiger_states should have a comment");
            assertTrue(comment.toLowerCase().contains("state") || 
                      comment.toLowerCase().contains("census"),
                      "Comment should mention state or census");
          }
        }
      }
    }
  }

  @Test
  public void testColumnCommentsViaSQL() throws Exception {
    // Create a model with SEC schema
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"sec\",\n" +
        "  \"schemas\": [\n" +
        "    {\n" +
        "      \"name\": \"sec\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"dataSource\": \"sec\",\n" +
        "        \"directory\": \"" + tempDir.getAbsolutePath() + "/sec\",\n" +
        "        \"useMockData\": false\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    // Create necessary directories
    new File(tempDir, "sec/sec-parquet").mkdirs();

    // Write the model file
    File modelFile = new File(tempDir, "column-comments-model.json");
    Files.write(modelFile.toPath(), modelJson.getBytes());

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
      
      // Query column comments from information_schema.columns
      String columnQuery = 
          "SELECT \"TABLE_NAME\", \"COLUMN_NAME\", \"COLUMN_COMMENT\" " +
          "FROM metadata.\"COLUMNS\" " +
          "WHERE \"TABLE_SCHEMA\" = 'sec' " +
          "  AND \"TABLE_NAME\" = 'financial_line_items' " +
          "  AND \"COLUMN_NAME\" IN ('cik', 'concept', 'numeric_value') " +
          "ORDER BY \"ORDINAL_POSITION\"";
      
      System.out.println("\n=== Column Comments via SQL ===");
      System.out.println("Query: " + columnQuery);
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(columnQuery)) {
        
        int columnCount = 0;
        while (rs.next()) {
          columnCount++;
          String table = rs.getString("TABLE_NAME");
          String column = rs.getString("COLUMN_NAME");
          String comment = rs.getString("COLUMN_COMMENT");
          
          System.out.printf("Table: %s, Column: %s%n", table, column);
          System.out.printf("  Comment: %s%n", comment);
          
          // Verify specific column comments
          if ("cik".equalsIgnoreCase(column)) {
            assertNotNull(comment, "CIK column should have a comment");
            assertTrue(comment.toLowerCase().contains("central") || 
                      comment.toLowerCase().contains("index") ||
                      comment.toLowerCase().contains("identifier"),
                      "CIK comment should explain what CIK means");
          }
          
          if ("concept".equalsIgnoreCase(column)) {
            assertNotNull(comment, "concept column should have a comment");
            assertTrue(comment.toLowerCase().contains("xbrl") || 
                      comment.toLowerCase().contains("financial") ||
                      comment.toLowerCase().contains("metric"),
                      "concept comment should mention XBRL or financial metric");
          }
        }
        
        assertTrue(columnCount > 0, "Should have found some columns with comments");
      }
    }
  }

  @Test
  public void testCommentsLikePostgreSQL() throws Exception {
    // Create a model with GEO schema
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"geo\",\n" +
        "  \"schemas\": [\n" +
        "    {\n" +
        "      \"name\": \"geo\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"dataSource\": \"geo\",\n" +
        "        \"cacheDirectory\": \"" + tempDir.getAbsolutePath() + "/geo\",\n" +
        "        \"autoDownload\": false\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    // Create necessary directories
    new File(tempDir, "geo").mkdirs();

    // Write the model file
    File modelFile = new File(tempDir, "pg-style-model.json");
    Files.write(modelFile.toPath(), modelJson.getBytes());

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
      
      // Query similar to PostgreSQL style
      // In PostgreSQL: 
      // SELECT obj_description(c.oid, 'pg_class') as table_comment
      // FROM pg_class c WHERE c.relname = 'my_table';
      
      // In Calcite with our enhancement:
      String pgStyleQuery = 
          "SELECT " +
          "  t.\"TABLE_NAME\", " +
          "  t.\"TABLE_COMMENT\", " +
          "  c.\"COLUMN_NAME\", " +
          "  c.\"COLUMN_COMMENT\" " +
          "FROM metadata.\"TABLES\" t " +
          "JOIN metadata.\"COLUMNS\" c " +
          "  ON t.\"TABLE_CATALOG\" = c.\"TABLE_CATALOG\" " +
          "  AND t.\"TABLE_SCHEMA\" = c.\"TABLE_SCHEMA\" " +
          "  AND t.\"TABLE_NAME\" = c.\"TABLE_NAME\" " +
          "WHERE t.\"TABLE_SCHEMA\" = 'geo' " +
          "  AND t.\"TABLE_NAME\" = 'tiger_states' " +
          "  AND c.\"COLUMN_NAME\" IN ('state_fips', 'state_code') " +
          "ORDER BY c.\"ORDINAL_POSITION\"";
      
      System.out.println("\n=== PostgreSQL-style Comment Query ===");
      System.out.println("Query: " + pgStyleQuery);
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(pgStyleQuery)) {
        
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          String tableComment = rs.getString("TABLE_COMMENT");
          String columnName = rs.getString("COLUMN_NAME");
          String columnComment = rs.getString("COLUMN_COMMENT");
          
          System.out.printf("Table: %s%n", tableName);
          System.out.printf("  Table Comment: %s%n", tableComment);
          System.out.printf("  Column: %s%n", columnName);
          System.out.printf("    Column Comment: %s%n", columnComment);
          
          // Verify comments exist and are meaningful
          assertNotNull(tableComment, "Table should have a comment");
          assertNotNull(columnComment, "Column should have a comment");
        }
      }
    }
  }
}