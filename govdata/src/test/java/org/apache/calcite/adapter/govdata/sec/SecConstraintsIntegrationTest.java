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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for constraint metadata functionality in SEC adapter.
 */
@Tag("integration")
public class SecConstraintsIntegrationTest {

  /**
   * Test that SEC adapter supports constraint metadata in model files.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testConstraintMetadataInModelFile() throws Exception {
    // Create a temporary model file with constraint definitions
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"SEC\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"SEC\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.sec.SecSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"directory\": \"/tmp/test-sec-data\",\n" +
        "      \"executionEngine\": \"DUCKDB\",\n" +
        "      \"ephemeralCache\": true\n" +
        "    },\n" +
        "    \"tables\": [{\n" +
        "      \"name\": \"financial_line_items\",\n" +
        "      \"constraints\": {\n" +
        "        \"primaryKey\": [\"cik\", \"filing_type\", \"year\", \"concept\"],\n" +
        "        \"uniqueKeys\": [\n" +
        "          [\"filing_url\"]\n" +
        "        ],\n" +
        "        \"foreignKeys\": [{\n" +
        "          \"columns\": [\"cik\"],\n" +
        "          \"targetTable\": [\"company_info\"],\n" +
        "          \"targetColumns\": [\"cik\"]\n" +
        "        }]\n" +
        "      }\n" +
        "    }, {\n" +
        "      \"name\": \"company_info\",\n" +
        "      \"constraints\": {\n" +
        "        \"primaryKey\": [\"cik\"],\n" +
        "        \"uniqueKeys\": [\n" +
        "          [\"company_name\"]\n" +
        "        ]\n" +
        "      }\n" +
        "    }]\n" +
        "  }]\n" +
        "}";

    // Create temporary model file
    Path tempModel = Files.createTempFile("sec-constraints-test", ".json");
    Files.write(tempModel, modelJson.getBytes());

    try {
      // Create connection using the model
      String jdbcUrl = "jdbc:calcite:model=" + tempModel.toString();
      Connection connection = DriverManager.getConnection(jdbcUrl);
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

      // Verify that schema factory received constraint metadata
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus secSchemaPlus = rootSchema.getSubSchema("SEC");
      assertNotNull(secSchemaPlus, "SEC schema should be created");
      Schema secSchema = secSchemaPlus.unwrap(Schema.class);

      // Test JDBC metadata for primary keys
      DatabaseMetaData metadata = connection.getMetaData();
      
      try (ResultSet rs = metadata.getPrimaryKeys(null, "SEC", "FINANCIAL_LINE_ITEMS")) {
        int pkColumnCount = 0;
        while (rs.next()) {
          pkColumnCount++;
          String columnName = rs.getString("COLUMN_NAME");
          assertTrue(columnName.matches("(?i)(cik|filing_type|year|concept)"), 
                    "Primary key column should be one of the expected columns: " + columnName);
        }
        assertTrue(pkColumnCount > 0, "Should find primary key columns in metadata");
      }

      // Test foreign key metadata
      try (ResultSet rs = metadata.getImportedKeys(null, "SEC", "FINANCIAL_LINE_ITEMS")) {
        boolean foundForeignKey = false;
        while (rs.next()) {
          foundForeignKey = true;
          String fkColumnName = rs.getString("FKCOLUMN_NAME");
          String pkTableName = rs.getString("PKTABLE_NAME");
          String pkColumnName = rs.getString("PKCOLUMN_NAME");
          
          assertEquals("cik", fkColumnName.toLowerCase(), "Foreign key column should be cik");
          assertEquals("company_info", pkTableName.toLowerCase(), "Referenced table should be company_info");
          assertEquals("cik", pkColumnName.toLowerCase(), "Referenced column should be cik");
        }
        assertTrue(foundForeignKey, "Should find foreign key relationship in metadata");
      }

      connection.close();
    } finally {
      Files.deleteIfExists(tempModel);
    }
  }

  /**
   * Test that SEC adapter supports constraints without breaking backward compatibility.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testBackwardCompatibilityWithoutConstraints() throws Exception {
    // Create a model file WITHOUT constraint definitions (legacy format)
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"SEC\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"SEC\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.sec.SecSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"directory\": \"/tmp/test-sec-data-legacy\",\n" +
        "      \"executionEngine\": \"DUCKDB\",\n" +
        "      \"ephemeralCache\": true\n" +
        "    }\n" +
        "  }]\n" +
        "}";

    // Create temporary model file
    Path tempModel = Files.createTempFile("sec-legacy-test", ".json");
    Files.write(tempModel, modelJson.getBytes());

    try {
      // Create connection using the legacy model
      String jdbcUrl = "jdbc:calcite:model=" + tempModel.toString();
      Connection connection = DriverManager.getConnection(jdbcUrl);
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

      // Verify that schema is created successfully without constraints
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus secSchemaPlus = rootSchema.getSubSchema("SEC");
      assertNotNull(secSchemaPlus, "SEC schema should be created even without constraint definitions");
      Schema secSchema = secSchemaPlus.unwrap(Schema.class);

      // Test that basic queries still work
      try (Statement stmt = connection.createStatement()) {
        // This should not fail even though no constraints are defined
        stmt.execute("SELECT * FROM SEC.financial_line_items LIMIT 0");
      }

      connection.close();
    } finally {
      Files.deleteIfExists(tempModel);
    }
  }

  /**
   * Test that SecSchemaFactory properly implements ConstraintCapableSchemaFactory.
   */
  @Test
  public void testSecSchemaFactoryConstraintCapability() {
    SecSchemaFactory factory = new SecSchemaFactory();
    
    // Test that it implements the constraint capability interface
    assertTrue(factory instanceof org.apache.calcite.schema.ConstraintCapableSchemaFactory, 
               "SecSchemaFactory should implement ConstraintCapableSchemaFactory");
    
    // Test that it supports constraints
    assertTrue(factory.supportsConstraints(), "SecSchemaFactory should support constraints");
  }
}