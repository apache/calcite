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
package org.apache.calcite.adapter.sec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for SEC adapter constraint metadata.
 * Verifies that primary key and foreign key definitions are properly exposed
 * through JDBC metadata APIs.
 */
@Tag("unit")
public class SecConstraintMetadataTest {

  @TempDir
  Path tempDir;

  private String modelPath;

  @BeforeEach
  void setUp() throws Exception {
    // Create test model with constraint support enabled
    String model = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"SEC\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"SEC\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"directory\": \"" + tempDir.toString() + "\",\n" +
        "      \"ephemeralCache\": true,\n" +
        "      \"testMode\": true,\n" +
        "      \"useMockData\": true,\n" +
        "      \"enableConstraints\": true,\n" +
        "      \"ciks\": [\"TEST\"],\n" +
        "      \"startYear\": 2023,\n" +
        "      \"endYear\": 2023\n" +
        "    }\n" +
        "  }]\n" +
        "}";

    Path modelFile = tempDir.resolve("model.json");
    Files.writeString(modelFile, model);
    modelPath = modelFile.toString();
  }

  @Test
  void testPrimaryKeyMetadata() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      DatabaseMetaData metadata = conn.getMetaData();
      assertNotNull(metadata);

      // Check primary keys for financial_line_items table
      try (ResultSet rs = metadata.getPrimaryKeys(null, "SEC", "financial_line_items")) {
        List<String> pkColumns = new ArrayList<>();
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          pkColumns.add(columnName);
        }

        // Verify expected primary key columns
        assertTrue(pkColumns.contains("cik"), "PK should include cik");
        assertTrue(pkColumns.contains("filing_type"), "PK should include filing_type");
        assertTrue(pkColumns.contains("year"), "PK should include year");
        assertTrue(pkColumns.contains("filing_date"), "PK should include filing_date");
        assertTrue(pkColumns.contains("concept"), "PK should include concept");
        assertTrue(pkColumns.contains("context_ref"), "PK should include context_ref");
      }
    }
  }

  @Test
  void testForeignKeyMetadata() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      DatabaseMetaData metadata = conn.getMetaData();

      // Check foreign keys for financial_line_items table
      try (ResultSet rs = metadata.getImportedKeys(null, "SEC", "financial_line_items")) {
        boolean foundContextFK = false;
        while (rs.next()) {
          String pkTableName = rs.getString("PKTABLE_NAME");
          String pkColumnName = rs.getString("PKCOLUMN_NAME");
          String fkColumnName = rs.getString("FKCOLUMN_NAME");

          if ("filing_contexts".equals(pkTableName) && 
              "context_id".equals(pkColumnName) &&
              "context_ref".equals(fkColumnName)) {
            foundContextFK = true;
          }
        }

        assertTrue(foundContextFK, 
            "Should find foreign key from financial_line_items.context_ref to filing_contexts.context_id");
      }
    }
  }

  @Test
  void testConstraintsCanBeDisabled() throws Exception {
    // Create model with constraints disabled
    String model = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"SEC\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"SEC\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"directory\": \"" + tempDir.toString() + "\",\n" +
        "      \"ephemeralCache\": true,\n" +
        "      \"testMode\": true,\n" +
        "      \"useMockData\": true,\n" +
        "      \"enableConstraints\": false,\n" +
        "      \"ciks\": [\"TEST\"],\n" +
        "      \"startYear\": 2023,\n" +
        "      \"endYear\": 2023\n" +
        "    }\n" +
        "  }]\n" +
        "}";

    Path modelFile = tempDir.resolve("model-no-constraints.json");
    Files.writeString(modelFile, model);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelFile, props)) {

      DatabaseMetaData metadata = conn.getMetaData();

      // When constraints are disabled, no primary keys should be reported
      try (ResultSet rs = metadata.getPrimaryKeys(null, "SEC", "financial_line_items")) {
        assertFalse(rs.next(), "Should not find primary keys when constraints are disabled");
      }
    }
  }

  @Test
  void testTableMetadataWithConstraints() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      DatabaseMetaData metadata = conn.getMetaData();

      // Check that tables are present
      try (ResultSet rs = metadata.getTables(null, "SEC", "%", new String[]{"TABLE"})) {
        List<String> tableNames = new ArrayList<>();
        while (rs.next()) {
          tableNames.add(rs.getString("TABLE_NAME"));
        }

        // Verify key SEC tables exist
        assertTrue(tableNames.contains("financial_line_items"), "Should have financial_line_items table");
        assertTrue(tableNames.contains("filing_contexts"), "Should have filing_contexts table");
      }

      // Verify column metadata includes partition columns
      try (ResultSet rs = metadata.getColumns(null, "SEC", "financial_line_items", null)) {
        List<String> columnNames = new ArrayList<>();
        while (rs.next()) {
          columnNames.add(rs.getString("COLUMN_NAME"));
        }

        // Check that partition columns are included
        assertTrue(columnNames.contains("cik"), "Should have cik column");
        assertTrue(columnNames.contains("filing_type"), "Should have filing_type column");
        assertTrue(columnNames.contains("year"), "Should have year column");

        // Check data columns
        assertTrue(columnNames.contains("filing_date"), "Should have filing_date column");
        assertTrue(columnNames.contains("concept"), "Should have concept column");
        assertTrue(columnNames.contains("context_ref"), "Should have context_ref column");
      }
    }
  }
}