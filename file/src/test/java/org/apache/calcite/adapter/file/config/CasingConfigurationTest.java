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
package org.apache.calcite.adapter.file.config;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for table and column name casing configuration in the file adapter.
 */
@SuppressWarnings("deprecation")
public class CasingConfigurationTest {

  @TempDir
  public File tempDir;

  @Test void testUppercaseTableAndColumnNames() throws Exception {
    createTestCsvFile(new File(tempDir, "TestFile.csv"));

    String model = createModelWithCasing("UPPER", "UPPER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      assertTrue(tableNames.contains("TESTFILE"));

      try (Statement stmt = conn.createStatement()) {
        // Table and column names should be uppercase
        try (ResultSet rs = stmt.executeQuery("SELECT \"ID\", \"NAME\" FROM \"TESTFILE\" ORDER BY \"ID\"")) {
          assertTrue(rs.next());
          assertEquals(1, Integer.parseInt(rs.getString("ID")));
          assertEquals("Alice", rs.getString("NAME"));

          assertTrue(rs.next());
          assertEquals(2, Integer.parseInt(rs.getString("ID")));
          assertEquals("Bob", rs.getString("NAME"));
        }
      }
    }
  }

  @Test void testLowercaseTableAndColumnNames() throws Exception {
    createTestCsvFile(new File(tempDir, "TestFile.csv"));

    String model = createModelWithCasing("LOWER", "LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      assertTrue(tableNames.contains("testfile"));

      try (Statement stmt = conn.createStatement()) {
        // Table and column names should be lowercase
        try (ResultSet rs = stmt.executeQuery("SELECT \"id\", \"name\" FROM \"testfile\" ORDER BY \"id\"")) {
          assertTrue(rs.next());
          assertEquals(1, Integer.parseInt(rs.getString("id")));
          assertEquals("Alice", rs.getString("name"));

          assertTrue(rs.next());
          assertEquals(2, Integer.parseInt(rs.getString("id")));
          assertEquals("Bob", rs.getString("name"));
        }
      }
    }
  }

  @Test void testUnchangedTableAndColumnNames() throws Exception {
    createTestCsvFile(new File(tempDir, "TestFile.csv"));

    String model = createModelWithCasing("UNCHANGED", "UNCHANGED");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      assertTrue(tableNames.contains("TestFile"));

      try (Statement stmt = conn.createStatement()) {
        // Table and column names should preserve original casing
        try (ResultSet rs = stmt.executeQuery("SELECT \"ID\", \"Name\" FROM \"TestFile\" ORDER BY \"ID\"")) {
          assertTrue(rs.next());
          assertEquals(1, Integer.parseInt(rs.getString("ID")));
          assertEquals("Alice", rs.getString("Name"));

          assertTrue(rs.next());
          assertEquals(2, Integer.parseInt(rs.getString("ID")));
          assertEquals("Bob", rs.getString("Name"));
        }
      }
    }
  }

  @Test void testMixedCasing() throws Exception {
    createTestCsvFile(new File(tempDir, "TestFile.csv"));

    // Test uppercase table names with lowercase column names
    String model = createModelWithCasing("UPPER", "LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      assertTrue(tableNames.contains("TESTFILE"));

      try (Statement stmt = conn.createStatement()) {
        // Table names uppercase, column names lowercase
        try (ResultSet rs = stmt.executeQuery("SELECT \"id\", \"name\" FROM \"TESTFILE\" ORDER BY \"id\"")) {
          assertTrue(rs.next());
          assertEquals(1, Integer.parseInt(rs.getString("id")));
          assertEquals("Alice", rs.getString("name"));

          assertTrue(rs.next());
          assertEquals(2, Integer.parseInt(rs.getString("id")));
          assertEquals("Bob", rs.getString("name"));
        }
      }
    }
  }

  @Test void testDefaultCasingBehavior() throws Exception {
    createTestCsvFile(new File(tempDir, "TestFile.csv"));

    // Test default behavior (no casing config specified)
    String model = createModelWithoutCasing();

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      // Default table casing should be UPPER
      assertTrue(tableNames.contains("TESTFILE"));

      try (Statement stmt = conn.createStatement()) {
        // Default column casing should be UNCHANGED
        try (ResultSet rs = stmt.executeQuery("SELECT \"ID\", \"Name\" FROM \"TESTFILE\" ORDER BY \"ID\"")) {
          assertTrue(rs.next());
          assertEquals(1, Integer.parseInt(rs.getString("ID")));
          assertEquals("Alice", rs.getString("Name"));
        }
      }
    }
  }

  @Test void testSnakeCaseConfigurationSupport() throws Exception {
    createTestCsvFile(new File(tempDir, "TestFile.csv"));

    // Test snake_case configuration names (as used in JDBC URLs)
    String model = createModelWithSnakeCasing("UPPER", "UPPER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      assertTrue(tableNames.contains("TESTFILE"));

      try (Statement stmt = conn.createStatement()) {
        // Table and column names should be uppercase
        try (ResultSet rs = stmt.executeQuery("SELECT \"ID\", \"NAME\" FROM \"TESTFILE\" ORDER BY \"ID\"")) {
          assertTrue(rs.next());
          assertEquals(1, Integer.parseInt(rs.getString("ID")));
          assertEquals("Alice", rs.getString("NAME"));

          assertTrue(rs.next());
          assertEquals(2, Integer.parseInt(rs.getString("ID")));
          assertEquals("Bob", rs.getString("NAME"));
        }
      }
    }
  }

  private String createModelWithCasing(String tableNameCasing, String columnNameCasing) {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        tableNameCasing: '" + tableNameCasing + "',\n"
        + "        columnNameCasing: '" + columnNameCasing + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  private String createModelWithSnakeCasing(String tableNameCasing, String columnNameCasing) {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        table_name_casing: '" + tableNameCasing + "',\n"
        + "        column_name_casing: '" + columnNameCasing + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  private String createModelWithoutCasing() {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  private void createTestCsvFile(File file) throws IOException {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("ID,Name\n");
      writer.write("1,Alice\n");
      writer.write("2,Bob\n");
      writer.write("3,Charlie\n");
    }
  }
}
