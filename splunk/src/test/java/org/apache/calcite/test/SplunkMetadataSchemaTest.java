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
package org.apache.calcite.test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for Splunk adapter metadata schemas (information_schema and pg_catalog).
 * Uses mock connection to test metadata discovery features.
 */
@Tag("unit")
public class SplunkMetadataSchemaTest {

  private Connection connection;

  @BeforeEach
  void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty("model", "inline:" + getTestModel());
    connection = DriverManager.getConnection("jdbc:calcite:", props);
  }

  @Test void testMetadataSchemasExist() throws SQLException {

    // Get list of all schemas
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet schemas = metaData.getSchemas();

    Set<String> schemaNames = new HashSet<>();
    while (schemas.next()) {
      schemaNames.add(schemas.getString("TABLE_SCHEM"));
    }

    // Verify metadata schemas exist
    assertTrue(schemaNames.contains("information_schema"),
        "Should have information_schema");
    assertTrue(schemaNames.contains("pg_catalog"),
        "Should have pg_catalog");
    assertTrue(schemaNames.contains("splunk"),
        "Should have splunk schema");
    assertTrue(schemaNames.contains("metadata"),
        "Should have built-in metadata schema");
  }

  @Test void testInformationSchemaTables() throws SQLException {

    try (Statement stmt = connection.createStatement()) {
      // Query information_schema.tables
      ResultSet rs =
          stmt.executeQuery("SELECT table_schema, table_name, table_type " +
          "FROM \"information_schema\".\"TABLES\" " +
          "WHERE table_schema = 'splunk'");

      List<String> tableNames = new ArrayList<>();
      while (rs.next()) {
        assertEquals("splunk", rs.getString("table_schema"));
        assertEquals("BASE TABLE", rs.getString("table_type"));
        tableNames.add(rs.getString("table_name"));
      }

      // Should have our mock tables
      assertTrue(tableNames.contains("all_email"),
          "Should find all_email table");
      assertTrue(tableNames.contains("authentication"),
          "Should find authentication table");
    }
  }

  @Test void testInformationSchemaColumns() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      // Query information_schema.columns for authentication table
      ResultSet rs =
          stmt.executeQuery("SELECT column_name, data_type, is_nullable, ordinal_position " +
          "FROM \"information_schema\".\"COLUMNS\" " +
          "WHERE table_schema = 'splunk' AND table_name = 'authentication' " +
          "ORDER BY ordinal_position");

      List<String> columns = new ArrayList<>();
      int position = 1;
      while (rs.next()) {
        columns.add(rs.getString("column_name"));
        assertEquals(position++, rs.getInt("ordinal_position"));
      }

      // Verify we have the expected columns
      assertTrue(columns.contains("_time"), "Should have _time column");
      assertTrue(columns.contains("user"), "Should have user column");
      assertTrue(columns.contains("action"), "Should have action column");
      assertTrue(columns.contains("src"), "Should have src column");
      assertTrue(columns.contains("dest"), "Should have dest column");
    }
  }

  @Test void testPgCatalogTables() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      // Query pg_catalog.pg_tables
      ResultSet rs =
          stmt.executeQuery("SELECT \"schemaname\", \"tablename\", \"tableowner\" " +
          "FROM \"pg_catalog\".\"pg_tables\" " +
          "WHERE \"schemaname\" = 'splunk'");

      List<String> tables = new ArrayList<>();
      while (rs.next()) {
        assertEquals("splunk", rs.getString("schemaname"));
        assertEquals("splunk_admin", rs.getString("tableowner"));
        tables.add(rs.getString("tablename"));
      }

      // Should have our mock tables
      assertEquals(2, tables.size(), "Should find 2 tables");
      assertTrue(tables.contains("all_email"),
          "Should find all_email in pg_tables");
      assertTrue(tables.contains("authentication"),
          "Should find authentication in pg_tables");
    }
  }

  @Test void testPgCatalogNamespace() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      // Query pg_catalog.pg_namespace
      ResultSet rs =
          stmt.executeQuery("SELECT \"nspname\" FROM \"pg_catalog\".\"pg_namespace\" " +
          "ORDER BY \"nspname\"");

      List<String> namespaces = new ArrayList<>();
      while (rs.next()) {
        namespaces.add(rs.getString("nspname"));
      }

      // Should have standard schemas plus our splunk schema
      assertTrue(namespaces.contains("information_schema"));
      assertTrue(namespaces.contains("pg_catalog"));
      assertTrue(namespaces.contains("splunk"));
    }
  }

  @Test void testPgCatalogTypes() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      // Query pg_catalog.pg_type for basic types
      ResultSet rs =
          stmt.executeQuery("SELECT \"typname\", \"typlen\" FROM \"pg_catalog\".\"pg_type\" " +
          "WHERE \"typname\" IN ('varchar', 'int4', 'float8', 'bool') " +
          "ORDER BY \"typname\"");

      Set<String> types = new HashSet<>();
      while (rs.next()) {
        types.add(rs.getString("typname"));
      }

      // Should have basic PostgreSQL types
      assertTrue(types.contains("bool"));
      assertTrue(types.contains("float8"));
      assertTrue(types.contains("int4"));
      assertTrue(types.contains("varchar"));
    }
  }

  @Test void testSplunkSpecificMetadata() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      // Query Splunk-specific metadata table
      ResultSet rs =
          stmt.executeQuery("SELECT \"index_name\", \"is_internal\" " +
          "FROM \"pg_catalog\".\"splunk_indexes\" " +
          "ORDER BY \"index_name\"");

      List<String> indexes = new ArrayList<>();
      while (rs.next()) {
        indexes.add(rs.getString("index_name"));

        // Check internal flag
        String indexName = rs.getString("index_name");
        boolean isInternal = rs.getBoolean("is_internal");
        if (indexName.startsWith("_")) {
          assertTrue(isInternal, indexName + " should be marked as internal");
        }
      }

      // Should have at least the common indexes
      assertTrue(indexes.contains("main"), "Should have main index");
      assertTrue(indexes.contains("_internal"), "Should have _internal index");
      assertTrue(indexes.contains("_audit"), "Should have _audit index");
    }
  }

  @Test void testJdbcDatabaseMetadata() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    // Test getTables for splunk schema
    ResultSet tables = metaData.getTables(null, "splunk", "%", null);
    List<String> tableNames = new ArrayList<>();
    while (tables.next()) {
      assertEquals("splunk", tables.getString("TABLE_SCHEM"));
      tableNames.add(tables.getString("TABLE_NAME"));
    }
    assertTrue(tableNames.contains("all_email"));
    assertTrue(tableNames.contains("authentication"));

    // Test getColumns for authentication table
    ResultSet columns = metaData.getColumns(null, "splunk", "authentication", "%");
    List<String> columnNames = new ArrayList<>();
    while (columns.next()) {
      assertEquals("splunk", columns.getString("TABLE_SCHEM"));
      assertEquals("authentication", columns.getString("TABLE_NAME"));
      columnNames.add(columns.getString("COLUMN_NAME"));
    }
    assertTrue(columnNames.size() > 0, "Should have columns");
    assertTrue(columnNames.contains("_time"));
    assertTrue(columnNames.contains("user"));
  }

  @Test void testCrossSchemaMetadataQuery() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      // Query that joins metadata with actual schema info
      ResultSet rs =
          stmt.executeQuery("SELECT s.\"nspname\", COUNT(DISTINCT t.\"TABLE_NAME\") as table_count " +
          "FROM \"pg_catalog\".\"pg_namespace\" s " +
          "LEFT JOIN \"information_schema\".\"TABLES\" t " +
          "ON s.\"nspname\" = t.\"TABLE_SCHEMA\" " +
          "WHERE s.\"nspname\" NOT IN ('pg_catalog', 'information_schema') " +
          "GROUP BY s.\"nspname\" " +
          "ORDER BY s.\"nspname\"");

      boolean foundSplunk = false;
      while (rs.next()) {
        String schema = rs.getString("nspname");
        int tableCount = rs.getInt("table_count");

        if ("splunk".equals(schema)) {
          foundSplunk = true;
          assertEquals(2, tableCount, "Splunk schema should have 2 tables");
        }
      }

      assertTrue(foundSplunk, "Should find splunk schema in results");
    }
  }

  /**
   * Creates a test model with mock Splunk connection.
   */
  private String getTestModel() {
    return "{\n"
  +
        "  \"version\": \"1.0\",\n"
  +
        "  \"defaultSchema\": \"splunk\",\n"
  +
        "  \"schemas\": [\n"
  +
        "    {\n"
  +
        "      \"name\": \"splunk\",\n"
  +
        "      \"type\": \"custom\",\n"
  +
        "      \"factory\": \"org.apache.calcite.adapter.splunk.SplunkSchemaFactory\",\n"
  +
        "      \"operand\": {\n"
  +
        "        \"url\": \"mock\",\n"
  +
        "        \"username\": \"test\",\n"
  +
        "        \"password\": \"test\",\n"
  +
        "        \"tables\": [\n"
  +
        "          {\n"
  +
        "            \"name\": \"all_email\",\n"
  +
        "            \"search\": \"search index=email\",\n"
  +
        "            \"fields\": [\n"
  +
        "              {\"name\": \"_time\", \"type\": \"TIMESTAMP\", \"nullable\": false},\n"
  +
        "              {\"name\": \"sender\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"recipient\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"subject\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"size\", \"type\": \"INTEGER\"}\n"
  +
        "            ]\n"
  +
        "          },\n"
  +
        "          {\n"
  +
        "            \"name\": \"authentication\",\n"
  +
        "            \"search\": \"search index=main sourcetype=auth*\",\n"
  +
        "            \"fields\": [\n"
  +
        "              {\"name\": \"_time\", \"type\": \"TIMESTAMP\", \"nullable\": false},\n"
  +
        "              {\"name\": \"user\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"action\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"src\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"dest\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"result\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"_extra\", \"type\": \"ANY\"}\n"
  +
        "            ]\n"
  +
        "          }\n"
  +
        "        ]\n"
  +
        "      }\n"
  +
        "    }\n"
  +
        "  ]\n"
  +
        "}";
  }
}
