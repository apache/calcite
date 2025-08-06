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
package org.apache.calcite.adapter.file.format;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for YAML file format support in the file adapter.
 */
@Tag("unit")
public class YamlFileTest {

  @TempDir
  public File tempDir;

  @Test void testBasicYamlFile() throws Exception {
    // Create a YAML file
    File yamlFile = new File(tempDir, "employees.yaml");
    createEmployeeYaml(yamlFile);

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'YAML',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'YAML',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Test SELECT *
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"EMPLOYEES\" ORDER BY \"id\"")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("John Doe", rs.getString("name"));
        assertEquals("Engineering", rs.getString("department"));
        assertEquals(75000.0, rs.getDouble("salary"), 0.001);

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("Jane Smith", rs.getString("name"));
        assertEquals("Marketing", rs.getString("department"));
        assertEquals(65000.0, rs.getDouble("salary"), 0.001);

        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        assertEquals("Bob Johnson", rs.getString("name"));
        assertEquals("Sales", rs.getString("department"));
        assertEquals(55000.0, rs.getDouble("salary"), 0.001);

        assertThat(rs.next(), is(false));
      }

      // Test aggregation
      try (ResultSet rs =
          stmt.executeQuery("SELECT \"department\", AVG(\"salary\") as avg_salary "
          + "FROM \"EMPLOYEES\" GROUP BY \"department\" ORDER BY \"department\"")) {
        assertTrue(rs.next());
        assertEquals("Engineering", rs.getString("department"));
        assertEquals(75000.0, rs.getDouble("avg_salary"), 0.001);

        assertTrue(rs.next());
        assertEquals("Marketing", rs.getString("department"));
        assertEquals(65000.0, rs.getDouble("avg_salary"), 0.001);

        assertTrue(rs.next());
        assertEquals("Sales", rs.getString("department"));
        assertEquals(55000.0, rs.getDouble("avg_salary"), 0.001);

        assertThat(rs.next(), is(false));
      }
    }
  }

  @Test void testYmlExtension() throws Exception {
    // Test that .yml extension also works
    File ymlFile = new File(tempDir, "config.yml");
    createConfigYml(ymlFile);

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'YML',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'YML',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"CONFIG\"")) {
        assertTrue(rs.next());
        assertEquals("database", rs.getString("service"));
        assertEquals("localhost", rs.getString("host"));
        assertEquals(5432, rs.getInt("port"));
        assertEquals(true, rs.getBoolean("enabled"));

        assertTrue(rs.next());
        assertEquals("cache", rs.getString("service"));
        assertEquals("redis.local", rs.getString("host"));
        assertEquals(6379, rs.getInt("port"));
        assertEquals(true, rs.getBoolean("enabled"));

        assertThat(rs.next(), is(false));
      }
    }
  }

  @Test void testNestedYaml() throws Exception {
    // Create YAML with nested structure
    File yamlFile = new File(tempDir, "nested.yaml");
    createNestedYaml(yamlFile);

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'YAML',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'YAML',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        flatten: true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // With flattening enabled, nested fields are accessible with double underscore notation
      // (Using __ as separator to be consistent across all engines)
      // Use quoted identifiers to preserve case
      try (ResultSet rs =
          stmt.executeQuery("SELECT \"id\", \"name\", \"address__city\", \"address__zipcode\" "
          + "FROM NESTED WHERE \"address__city\" = 'New York'")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice", rs.getString("name"));
        assertEquals("New York", rs.getString("address__city"));
        assertEquals("10001", rs.getString("address__zipcode"));

        assertThat(rs.next(), is(false));
      }
    }
  }

  @Test void testYamlWithParquetEngine() throws Exception {
    File yamlFile = new File(tempDir, "data.yaml");
    createEmployeeYaml(yamlFile);

    // Test with PARQUET execution engine
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'YAML',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'YAML',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        executionEngine: 'parquet'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Should work with PARQUET engine via auto-conversion
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"DATA\"")) {
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong("cnt"));
      }

      // Check that Parquet cache was created
      File cacheDir = new File(tempDir, ".parquet_cache");
      assertTrue(cacheDir.exists());
    }
  }

  private void createEmployeeYaml(File file) throws IOException {
    String yaml = "- id: 1\n"
        + "  name: John Doe\n"
        + "  department: Engineering\n"
        + "  salary: 75000\n"
        + "- id: 2\n"
        + "  name: Jane Smith\n"
        + "  department: Marketing\n"
        + "  salary: 65000\n"
        + "- id: 3\n"
        + "  name: Bob Johnson\n"
        + "  department: Sales\n"
        + "  salary: 55000\n";

    try (FileWriter writer = new FileWriter(file)) {
      writer.write(yaml);
    }
  }

  private void createConfigYml(File file) throws IOException {
    String yml = "- service: database\n"
        + "  host: localhost\n"
        + "  port: 5432\n"
        + "  enabled: true\n"
        + "- service: cache\n"
        + "  host: redis.local\n"
        + "  port: 6379\n"
        + "  enabled: true\n";

    try (FileWriter writer = new FileWriter(file)) {
      writer.write(yml);
    }
  }

  private void createNestedYaml(File file) throws IOException {
    String yaml = "- id: 1\n"
        + "  name: Alice\n"
        + "  address:\n"
        + "    street: 123 Main St\n"
        + "    city: New York\n"
        + "    zipcode: \"10001\"\n"
        + "- id: 2\n"
        + "  name: Bob\n"
        + "  address:\n"
        + "    street: 456 Oak Ave\n"
        + "    city: Los Angeles\n"
        + "    zipcode: \"90001\"\n";

    try (FileWriter writer = new FileWriter(file)) {
      writer.write(yaml);
    }
  }
}
