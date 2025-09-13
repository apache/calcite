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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simplified test that table and column comments are queryable via SQL
 * through metadata views without filtering.
 */
@Tag("unit")
public class SimplifiedSqlCommentsTest {

  @TempDir
  File tempDir;

  @Test
  public void testMetadataTablesView() throws Exception {
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
    File modelFile = new File(tempDir, "simplified-model.json");
    Files.write(modelFile.toPath(), modelJson.getBytes());

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
      
      // Simply query all tables from metadata.TABLES  
      String tableQuery = 
          "SELECT * FROM metadata.\"TABLES\" LIMIT 1";
      
      System.out.println("\n=== Metadata Tables View ===");
      System.out.println("Query: " + tableQuery);
      
      boolean canQuery = false;
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(tableQuery)) {
        
        ResultSetMetaData rsMeta = rs.getMetaData();
        System.out.println("Column count: " + rsMeta.getColumnCount());
        for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
          System.out.printf("Column %d: %s (type: %s)%n", 
              i, rsMeta.getColumnName(i), rsMeta.getColumnTypeName(i));
        }
        
        // Check if TABLE_COMMENT column exists
        boolean hasTableComment = false;
        for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
          if ("TABLE_COMMENT".equals(rsMeta.getColumnName(i))) {
            hasTableComment = true;
            break;
          }
        }
        
        assertTrue(hasTableComment, "TABLE_COMMENT column should exist in metadata.TABLES");
        canQuery = true;
      }
      
      assertTrue(canQuery, "Should be able to query metadata.TABLES");
    }
  }

  @Test
  public void testMetadataColumnsView() throws Exception {
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
    File modelFile = new File(tempDir, "columns-model.json");
    Files.write(modelFile.toPath(), modelJson.getBytes());

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
      
      // Simply query columns from metadata.COLUMNS
      String columnQuery = 
          "SELECT \"TABLE_NAME\", \"COLUMN_NAME\", \"COLUMN_COMMENT\" " +
          "FROM metadata.\"COLUMNS\" " +
          "WHERE \"COLUMN_COMMENT\" IS NOT NULL " +
          "LIMIT 10";
      
      System.out.println("\n=== Metadata Columns View ===");
      System.out.println("Query: " + columnQuery);
      
      boolean foundComments = false;
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(columnQuery)) {
        
        while (rs.next()) {
          String table = rs.getString("TABLE_NAME");
          String column = rs.getString("COLUMN_NAME");
          String comment = rs.getString("COLUMN_COMMENT");
          
          System.out.printf("Table: %s, Column: %s%n", table, column);
          System.out.printf("  Comment: %s%n", comment);
          
          if (comment != null && !comment.isEmpty()) {
            foundComments = true;
          }
        }
      }
      
      assertTrue(foundComments, "Should have found some columns with comments");
    }
  }
}