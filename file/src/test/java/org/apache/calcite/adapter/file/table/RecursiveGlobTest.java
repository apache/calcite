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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for recursive glob pattern behavior in FileSchema.
 */
@Tag("unit")
public class RecursiveGlobTest {

  @TempDir
  File tempDir;

  @BeforeEach
  void setUp() throws Exception {
    // Clear any static caches that might interfere with test isolation
    Sources.clearFileCache();
    // Force garbage collection to release any file handles
    System.gc();
    // Wait to ensure cleanup completes
    Thread.sleep(200);
  }

  @AfterEach
  void tearDown() throws Exception {
    // Clear caches after each test to prevent contamination
    Sources.clearFileCache();
    System.gc();
    Thread.sleep(200);
  }

  @Test void testRecursiveIncludesRootAndSubdirectories() throws Exception {
    // Create unique temp directory for this test with full UUID to ensure uniqueness
    File uniqueTempDir = new File(tempDir, "recursive_includes_test_" + UUID.randomUUID().toString());
    assertTrue(uniqueTempDir.mkdirs());
    
    // Create test files in root directory with unique names
    createCsvFile(new File(uniqueTempDir, "recursive_root1.csv"), "id,name\n1,Alice\n");
    createCsvFile(new File(uniqueTempDir, "recursive_root2.csv"), "id,value\n1,100\n");

    // Create subdirectory with files
    File subDir = new File(uniqueTempDir, "subdir");
    assertTrue(subDir.mkdir());
    createCsvFile(new File(subDir, "sub1.csv"), "id,dept\n1,Sales\n");
    createCsvFile(new File(subDir, "sub2.csv"), "id,region\n1,North\n");

    // Create nested subdirectory with files
    File nestedDir = new File(subDir, "nested");
    assertTrue(nestedDir.mkdir());
    createCsvFile(new File(nestedDir, "nested1.csv"), "id,country\n1,USA\n");

    // Test using SQL queries through JDBC with proper lexical settings
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + uniqueTempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        recursive: true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:calcite:model=inline:" + model + ";lex=ORACLE;unquotedCasing=TO_LOWER");
         java.sql.Statement stmt = conn.createStatement()) {

      // Test that we can query tables from all directories
      // Use getTables metadata to check what tables are available
      java.sql.ResultSet tables = conn.getMetaData().getTables(null, "TEST", "%", null);
      java.util.Set<String> tableNames = new java.util.HashSet<>();
      while (tables.next()) {
        tableNames.add(tables.getString("TABLE_NAME"));
      }

      // Should find all 5 CSV files - exactly which table names depends on implementation
      assertEquals(5, tableNames.size(), "Should find all CSV files in root and subdirectories");

      // Test querying each table works (use lowercase table names)
      java.sql.ResultSet rs1 = stmt.executeQuery("SELECT COUNT(*) FROM recursive_root1");
      assertTrue(rs1.next());
      assertEquals(1, rs1.getInt(1));

      java.sql.ResultSet rs2 = stmt.executeQuery("SELECT COUNT(*) FROM recursive_root2");
      assertTrue(rs2.next());
      assertEquals(1, rs2.getInt(1));
    }
  }

  @Test void testNonRecursiveOnlyFindsRoot() throws Exception {
    // Create unique temp directory for this test with full UUID to ensure uniqueness
    File uniqueTempDir = new File(tempDir, "nonrecursive_only_test_" + UUID.randomUUID().toString());
    assertTrue(uniqueTempDir.mkdirs());
    
    // Create test files in root directory with unique names
    createCsvFile(new File(uniqueTempDir, "nonrec_root1.csv"), "id,name\n1,Alice\n");
    createCsvFile(new File(uniqueTempDir, "nonrec_root2.csv"), "id,value\n1,100\n");

    // Create subdirectory with files
    File subDir = new File(uniqueTempDir, "subdir");
    assertTrue(subDir.mkdir());
    createCsvFile(new File(subDir, "sub1.csv"), "id,dept\n1,Sales\n");

    // Test using SQL queries through JDBC with non-recursive and proper lexical settings
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + uniqueTempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        recursive: false\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:calcite:model=inline:" + model + ";lex=ORACLE;unquotedCasing=TO_LOWER");
         java.sql.Statement stmt = conn.createStatement()) {

      // Check table count
      java.sql.ResultSet tables = conn.getMetaData().getTables(null, "TEST", "%", null);
      java.util.Set<String> tableNames = new java.util.HashSet<>();
      while (tables.next()) {
        tableNames.add(tables.getString("TABLE_NAME"));
      }

      // Should find only 2 CSV files in root
      assertEquals(2, tableNames.size(), "Should find only CSV files in root directory");

      // Test querying root files works
      java.sql.ResultSet rs1 = stmt.executeQuery("SELECT COUNT(*) FROM nonrec_root1");
      assertTrue(rs1.next());
      assertEquals(1, rs1.getInt(1));

      java.sql.ResultSet rs2 = stmt.executeQuery("SELECT COUNT(*) FROM nonrec_root2");
      assertTrue(rs2.next());
      assertEquals(1, rs2.getInt(1));
    }
  }

  @Test void testCustomGlobPatterns() throws Exception {
    // Create unique temp directory for this test with full UUID to ensure uniqueness
    File uniqueTempDir = new File(tempDir, "customglob_patterns_test_" + UUID.randomUUID().toString());
    assertTrue(uniqueTempDir.mkdirs());
    
    // Create test files
    createCsvFile(new File(uniqueTempDir, "data.csv"), "id,name\n1,Alice\n");

    File subDir = new File(uniqueTempDir, "reports");
    assertTrue(subDir.mkdir());
    createCsvFile(new File(subDir, "q1.csv"), "id,quarter\n1,Q1\n");

    // Test basic glob pattern functionality by creating a schema and verifying it can query the files
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + uniqueTempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        recursive: true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:calcite:model=inline:" + model + ";lex=ORACLE;unquotedCasing=TO_LOWER");
         java.sql.Statement stmt = conn.createStatement()) {

      // Test that both root and subdirectory files are accessible (use lowercase table names)
      java.sql.ResultSet rs1 = stmt.executeQuery("SELECT COUNT(*) FROM data");
      assertTrue(rs1.next());
      assertEquals(1, rs1.getInt(1));

      // Test subdirectory file (table name may vary by implementation)
      java.sql.ResultSet tables = conn.getMetaData().getTables(null, "TEST", "%", null);
      boolean foundQ1 = false;
      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        if (tableName.toLowerCase().contains("q1")) {
          foundQ1 = true;
          break;
        }
      }
      assertTrue(foundQ1, "Should find q1.csv table in subdirectory");
    }
  }

  private void createCsvFile(File file, String content) throws IOException {
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }
}
