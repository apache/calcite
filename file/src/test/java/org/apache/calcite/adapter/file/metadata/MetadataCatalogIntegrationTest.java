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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test to verify metadata catalog features work with dynamic file discovery.
 * Tests information_schema, pg_catalog, and JDBC DatabaseMetaData compatibility.
 */
@Tag("unit")
public class MetadataCatalogIntegrationTest {

  private File tempDir;
  private String modelPath;

  @BeforeEach
  void setUp() throws Exception {
    // Create temp directory manually
    tempDir = Files.createTempDirectory("metadata-test-").toFile();
    
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

    // Create model with ephemeralCache enabled to avoid lock issues
    File modelFile = new File(tempDir, "model.json");
    modelPath = modelFile.getAbsolutePath();

    try (FileWriter writer = new FileWriter(modelFile, StandardCharsets.UTF_8)) {
      writer.write("{\n");
      writer.write("  \"version\": \"1.0\",\n");
      writer.write("  \"defaultSchema\": \"files\",\n");
      writer.write("  \"schemas\": [{\n");
      writer.write("    \"name\": \"files\",\n");
      writer.write("    \"type\": \"custom\",\n");
      writer.write("    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n");
      writer.write("    \"operand\": {\n");
      writer.write("      \"directory\": \"" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n");
      writer.write("      \"ephemeralCache\": true\n");
      writer.write("    }\n");
      writer.write("  }]\n");
      writer.write("}\n");
    }
  }
  
  @AfterEach
  void tearDown() {
    // Best effort cleanup - don't fail the test if cleanup fails
    if (tempDir != null && tempDir.exists()) {
      try {
        deleteRecursively(tempDir.toPath());
      } catch (Exception e) {
        // Ignore cleanup failures - DuckDB may still have locks
        System.err.println("Warning: Could not clean up temp directory: " + e.getMessage());
      }
    }
  }
  
  private void deleteRecursively(Path path) throws IOException {
    if (Files.exists(path)) {
      Files.walk(path)
          .sorted(Comparator.reverseOrder())
          .forEach(p -> {
            try {
              Files.deleteIfExists(p);
            } catch (IOException e) {
              // Ignore individual file deletion failures
            }
          });
    }
  }

  @Test void testMetadataSchemaAccess() throws Exception {
    try (Connection conn = getConnection()) {
      // Test 1: Check if metadata schema exists
      try (Statement stmt = conn.createStatement()) {
        // metadata schema name is lowercase, table names are uppercase (JDBC standard)
        ResultSet rs = stmt.executeQuery("SELECT * FROM metadata.\"TABLES\"");
        assertTrue(rs.next(), "metadata.TABLES should have results");

        // Count tables discovered
        int tableCount = 1; // Already moved to first row
        while (rs.next()) {
          tableCount++;
        }
        assertTrue(tableCount >= 3, "Should discover at least 3 tables (sales, customers, products)");
        rs.close();
      }

      // Test 2: Check columns metadata
      try (Statement stmt = conn.createStatement()) {
        // Metadata schema uses camelCase column names from Avatica
        ResultSet rs = stmt.executeQuery("SELECT * FROM metadata.\"COLUMNS\" c WHERE c.\"tableName\" = 'sales' AND c.\"tableSchem\" = 'files'");

        List<String> columns = new ArrayList<>();
        while (rs.next()) {
          String columnName = rs.getString("columnName");
          columns.add(columnName);
        }

        assertEquals(3, columns.size(), "sales table should have 3 columns");
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
        // information_schema uses standard SQL uppercase column names
        ResultSet rs = stmt.executeQuery("SELECT \"TABLE_NAME\" FROM information_schema.\"TABLES\" t " +
            "WHERE t.\"TABLE_SCHEMA\" = 'files'");

        List<String> tables = new ArrayList<>();
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }

        // Verify results
        assertTrue(tables.contains("sales"), "Should find SALES table");
        assertTrue(tables.contains("customers"), "Should find CUSTOMERS table");
        assertTrue(tables.contains("products"), "Should find PRODUCTS table");
      }
    }
  }

  @Test void testPgCatalogQueries() throws Exception {
    try (Connection conn = getConnection()) {
      // Test if pg_catalog is available
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs =
            stmt.executeQuery("SELECT tablename FROM pg_catalog.pg_tables " +
            "WHERE schemaname = 'files'");

        List<String> tables = new ArrayList<>();
        while (rs.next()) {
          tables.add(rs.getString("tablename"));
        }

        // Verify results - pg_catalog returns lowercase names
        assertTrue(tables.size() >= 3, "Should find at least 3 tables, found: " + tables.size());
        assertTrue(tables.contains("sales"), "Should find sales table");
        assertTrue(tables.contains("customers"), "Should find customers table");
        assertTrue(tables.contains("products"), "Should find products table");
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
        assertTrue(schemaNames.contains("files"), "Should have files schema");
        assertTrue(schemaNames.contains("metadata"), "Should have metadata schema");
        // Check for our custom metadata schemas (might be case-sensitive)
        boolean hasInfoSchema = schemaNames.contains("information_schema") ||
                               schemaNames.contains("information_schema");
        boolean hasPgCatalog = schemaNames.contains("pg_catalog") ||
                              schemaNames.contains("PG_CATALOG");
        assertTrue(hasInfoSchema, "Should have information_schema, found: " + schemaNames);
        assertTrue(hasPgCatalog, "Should have pg_catalog, found: " + schemaNames);
      }

      // Test 2: Get tables
      try (ResultSet tables = metaData.getTables(null, "files", "%", null)) {
        List<String> tableNames = new ArrayList<>();
        while (tables.next()) {
          tableNames.add(tables.getString("TABLE_NAME"));
        }

        assertEquals(3, tableNames.size(), "Should find 3 tables (3 data files)");
        assertTrue(tableNames.contains("sales"), "Should find sales table");
        assertTrue(tableNames.contains("customers"), "Should find customers table");
        assertTrue(tableNames.contains("products"), "Should find products table");
      }

      // Test 3: Get columns for a specific table
      try (ResultSet columns = metaData.getColumns(null, "files", "sales", "%")) {
        List<String> columnInfo = new ArrayList<>();
        while (columns.next()) {
          String columnName = columns.getString("COLUMN_NAME");
          String dataType = columns.getString("TYPE_NAME");
          columnInfo.add(columnName + " (" + dataType + ")");
        }

        assertEquals(3, columnInfo.size(), "sales should have 3 columns");
        System.out.println("sales columns: " + columnInfo);
      }

      // Test 4: Get primary keys (might be empty for file tables)
      try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, "files", "sales")) {
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
            stmt.executeQuery("SELECT DISTINCT \"tableSchem\" FROM metadata.\"TABLES\" ORDER BY \"tableSchem\"");

        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
          schemas.add(rs.getString("tableSchem"));
        }
        assertTrue(schemas.contains("files"), "Should list files schema");
      }

      // Test 2: Table listing with metadata
      try (Statement stmt = conn.createStatement()) {
        // Use camelCase column names from Avatica metadata
        ResultSet rs =
            stmt.executeQuery("SELECT \"tableSchem\", \"tableName\", \"tableType\" " +
            "FROM metadata.\"TABLES\" t " +
            "WHERE t.\"tableSchem\" = 'files' " +
            "ORDER BY \"tableName\"");

        while (rs.next()) {
          System.out.printf("Table: %s.%s (Type: %s)%n",
              rs.getString("tableSchem"),
              rs.getString("tableName"),
              rs.getString("tableType"));
        }
      }

      // Test 3: Column details query
      try (Statement stmt = conn.createStatement()) {
        // Use camelCase column names from Avatica metadata
        ResultSet rs =
            stmt.executeQuery("SELECT \"columnName\", \"typeName\", \"columnSize\", \"decimalDigits\", \"nullable\" " +
            "FROM metadata.\"COLUMNS\" c " +
            "WHERE c.\"tableSchem\" = 'files' AND c.\"tableName\" = 'sales' " +
            "ORDER BY \"ordinalPosition\"");

        System.out.println("\nsales table structure:");
        while (rs.next()) {
          System.out.printf("  %s %s(%s,%s) %s%n",
              rs.getString("columnName"),
              rs.getString("typeName"),
              rs.getObject("columnSize"),
              rs.getObject("decimalDigits"),
              rs.getInt("nullable") == DatabaseMetaData.columnNullable ? "null" : "NOT NULL");
        }
      }
    }
  }

  private void createTestFile(String fileName, String content) throws Exception {
    File file = new File(tempDir, fileName);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }

  private Connection getConnection() throws SQLException {
    Properties info = new Properties();
    info.setProperty("model", modelPath);
    // Set connection config as per user requirements
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }
}
