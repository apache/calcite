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
package org.apache.calcite.adapter.file.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test to verify metadata catalog features work with dynamic file discovery.
 * Tests information_schema, pg_catalog, and JDBC DatabaseMetaData compatibility.
 */
@Tag("integration")
public class MetadataCatalogIntegrationTest {

  @TempDir
  Path tempDir;

  private String modelPath;

  @BeforeEach
  void setUp() throws Exception {
    // Create test data files
    createTestFile("sales.csv",
        "order_id:int,customer:string,amount:double\n"
  +
        "1,John,100.50\n"
  +
        "2,Jane,200.75\n");

    createTestFile("customers.json",
        "[{\"id\": 1, \"name\": \"John\", \"active\": true}," +
        " {\"id\": 2, \"name\": \"Jane\", \"active\": false}]");

    createTestFile("products.csv",
        "product_id:int,name:string,price:double,in_stock:boolean\n"
  +
        "101,Widget,19.99,true\n"
  +
        "102,Gadget,29.99,false\n");

    // Create model with dynamic discovery
    File modelFile = new File(tempDir.toFile(), "model.json");
    modelPath = modelFile.getAbsolutePath();

    try (FileWriter writer = new FileWriter(modelFile, StandardCharsets.UTF_8)) {
      writer.write("{\n");
      writer.write("  \"version\": \"1.0\",\n");
      writer.write("  \"defaultSchema\": \"FILES\",\n");
      writer.write("  \"schemas\": [{\n");
      writer.write("    \"name\": \"FILES\",\n");
      writer.write("    \"type\": \"custom\",\n");
      writer.write("    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n");
      writer.write("    \"operand\": {\n");
      writer.write("      \"directory\": \"" + tempDir.toString().replace("\\", "\\\\") + "\"\n");
      writer.write("    }\n");
      writer.write("  }]\n");
      writer.write("}\n");
    }
  }

  @Test void testMetadataSchemaAccess() throws Exception {
    try (Connection conn = getConnection()) {
      // Test 1: Check if metadata schema exists
      try (Statement stmt = conn.createStatement()) {
        // Test using Calcite's built-in metadata schema
        ResultSet rs = stmt.executeQuery("SELECT * FROM \"metadata\".\"TABLES\"");
        assertTrue(rs.next(), "metadata.TABLES should have results");

        // Count tables discovered
        int tableCount = 1; // Already moved to first row
        while (rs.next()) {
          tableCount++;
        }
        assertTrue(tableCount >= 3, "Should discover at least 3 tables (sales, customers, products)");
      }

      // Test 2: Check columns metadata
      try (Statement stmt = conn.createStatement()) {
        // First, let's see what columns are available in metadata.COLUMNS
        ResultSet rs = stmt.executeQuery("SELECT * FROM \"metadata\".\"COLUMNS\" LIMIT 1");
        ResultSetMetaData rsmd = rs.getMetaData();
        System.out.println("Columns in metadata.COLUMNS:");
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
          System.out.println("  " + rsmd.getColumnName(i));
        }
        rs.close();

        // Now query with correct column names
        rs =
            stmt.executeQuery("SELECT * FROM \"metadata\".\"COLUMNS\" WHERE \"TABLE_NAME\" = 'SALES' AND \"TABLE_SCHEMA\" = 'FILES'");

        List<String> columns = new ArrayList<>();
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          columns.add(columnName);
          System.out.println("Found column: " + columnName);
        }

        assertEquals(3, columns.size(), "SALES table should have 3 columns");
        assertTrue(columns.contains("order_id"), "Should have order_id column");
        assertTrue(columns.contains("customer"), "Should have customer column");
        assertTrue(columns.contains("amount"), "Should have amount column");
      }
    }
  }

  @Test void testInformationSchemaQueries() throws Exception {
    try (Connection conn = getConnection()) {
      // Test if information_schema is available
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs =
            stmt.executeQuery("SELECT \"TABLE_NAME\" FROM \"information_schema\".\"TABLES\" " +
            "WHERE \"TABLE_SCHEMA\" = 'FILES'");

        List<String> tables = new ArrayList<>();
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }

        // Verify results
        assertTrue(tables.contains("SALES"), "Should find SALES table");
        assertTrue(tables.contains("CUSTOMERS"), "Should find CUSTOMERS table");
        assertTrue(tables.contains("PRODUCTS"), "Should find PRODUCTS table");
      }
    }
  }

  @Test void testPgCatalogQueries() throws Exception {
    try (Connection conn = getConnection()) {
      // Test if pg_catalog is available
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs =
            stmt.executeQuery("SELECT \"tablename\" FROM \"pg_catalog\".\"pg_tables\" " +
            "WHERE \"schemaname\" = 'FILES'");

        List<String> tables = new ArrayList<>();
        while (rs.next()) {
          tables.add(rs.getString("tablename"));
        }

        // Verify results
        assertEquals(3, tables.size(), "Should find 3 tables");
        assertTrue(tables.contains("SALES"), "Should find SALES table");
        assertTrue(tables.contains("CUSTOMERS"), "Should find CUSTOMERS table");
        assertTrue(tables.contains("PRODUCTS"), "Should find PRODUCTS table");
      }
    }
  }

  @Test void testJdbcDatabaseMetadata() throws Exception {
    try (Connection conn = getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();
      assertNotNull(metaData, "DatabaseMetaData should not be null");

      // Test 1: Get schemas
      try (ResultSet schemas = metaData.getSchemas()) {
        List<String> schemaNames = new ArrayList<>();
        while (schemas.next()) {
          String schema = schemas.getString("TABLE_SCHEM");
          schemaNames.add(schema);
          System.out.println("Found schema: " + schema);
        }
        System.out.println("All schemas found: " + schemaNames);
        assertTrue(schemaNames.contains("FILES"), "Should have FILES schema");
        assertTrue(schemaNames.contains("metadata"), "Should have metadata schema");
        // Check for our custom metadata schemas (might be case-sensitive)
        boolean hasInfoSchema = schemaNames.contains("information_schema") ||
                               schemaNames.contains("INFORMATION_SCHEMA");
        boolean hasPgCatalog = schemaNames.contains("pg_catalog") ||
                              schemaNames.contains("PG_CATALOG");
        assertTrue(hasInfoSchema, "Should have information_schema, found: " + schemaNames);
        assertTrue(hasPgCatalog, "Should have pg_catalog, found: " + schemaNames);
      }

      // Test 2: Get tables
      try (ResultSet tables = metaData.getTables(null, "FILES", "%", null)) {
        List<String> tableNames = new ArrayList<>();
        while (tables.next()) {
          tableNames.add(tables.getString("TABLE_NAME"));
        }

        assertEquals(4, tableNames.size(), "Should find 4 tables (3 data files + 1 cache)");
        assertTrue(tableNames.contains("SALES"), "Should find SALES table");
        assertTrue(tableNames.contains("CUSTOMERS"), "Should find CUSTOMERS table");
        assertTrue(tableNames.contains("PRODUCTS"), "Should find PRODUCTS table");
      }

      // Test 3: Get columns for a specific table
      try (ResultSet columns = metaData.getColumns(null, "FILES", "SALES", "%")) {
        List<String> columnInfo = new ArrayList<>();
        while (columns.next()) {
          String columnName = columns.getString("COLUMN_NAME");
          String dataType = columns.getString("TYPE_NAME");
          columnInfo.add(columnName + " (" + dataType + ")");
        }

        assertEquals(3, columnInfo.size(), "SALES should have 3 columns");
        System.out.println("SALES columns: " + columnInfo);
      }

      // Test 4: Get primary keys (might be empty for file tables)
      try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, "FILES", "SALES")) {
        if (primaryKeys.next()) {
          System.out.println("Primary key found: " + primaryKeys.getString("COLUMN_NAME"));
        } else {
          System.out.println("No primary keys defined (expected for file tables)");
        }
      }
    }
  }

  @Test void testToolCompatibilityQueries() throws Exception {
    try (Connection conn = getConnection()) {
      // Queries that tools like DBeaver, DataGrip commonly use

      // Test 1: Schema discovery query
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs =
            stmt.executeQuery("SELECT DISTINCT \"TABLE_SCHEMA\" FROM \"metadata\".\"TABLES\" ORDER BY \"TABLE_SCHEMA\"");

        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
          schemas.add(rs.getString("TABLE_SCHEMA"));
        }
        assertTrue(schemas.contains("FILES"), "Should list FILES schema");
      }

      // Test 2: Table listing with metadata
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs =
            stmt.executeQuery("SELECT TABLE_SCHEM, TABLE_NAME, TABLE_TYPE " +
            "FROM \"metadata\".TABLES " +
            "WHERE TABLE_SCHEM = 'FILES' " +
            "ORDER BY TABLE_NAME");

        while (rs.next()) {
          System.out.printf("Table: %s.%s (Type: %s)%n",
              rs.getString("TABLE_SCHEM"),
              rs.getString("TABLE_NAME"),
              rs.getString("TABLE_TYPE"));
        }
      }

      // Test 3: Column details query
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs =
            stmt.executeQuery("SELECT COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, DECIMAL_DIGITS, NULLABLE " +
            "FROM \"metadata\".COLUMNS " +
            "WHERE TABLE_SCHEM = 'FILES' AND TABLE_NAME = 'SALES' " +
            "ORDER BY ORDINAL_POSITION");

        System.out.println("\nSALES table structure:");
        while (rs.next()) {
          System.out.printf("  %s %s(%s,%s) %s%n",
              rs.getString("COLUMN_NAME"),
              rs.getString("TYPE_NAME"),
              rs.getObject("COLUMN_SIZE"),
              rs.getObject("DECIMAL_DIGITS"),
              rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable ? "NULL" : "NOT NULL");
        }
      }
    }
  }

  private void createTestFile(String fileName, String content) throws Exception {
    File file = new File(tempDir.toFile(), fileName);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }

  private Connection getConnection() throws SQLException {
    Properties info = new Properties();
    info.setProperty("model", modelPath);
    return DriverManager.getConnection("jdbc:calcite:", info);
  }
}
