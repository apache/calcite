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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SQL-based unit tests for SharePoint metadata queries.
 * Tests actual SQL queries against pg_catalog and information_schema without requiring SharePoint connectivity.
 */
public class SharePointMetadataSqlTest {

  private Connection connection;

  @BeforeEach
  public void setUp() throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // Create a mock SharePoint schema with metadata schemas
    rootSchema.add("sharepoint_test", new MockSharePointSchemaWithMetadata());
  }

  @Test public void testQueryPgTables() throws SQLException {
    String sql = "SELECT schemaname, tablename, tableowner, tablespace, hasindexes "
        + "FROM sharepoint_test.pg_catalog.pg_tables "
        + "ORDER BY tablename";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);

    // Verify we get results
    assertTrue(rs.next(), "Should have at least one table");

    // Check first row
    assertEquals("sharepoint_test", rs.getString("schemaname"));
    assertNotNull(rs.getString("tablename"));
    assertEquals("sharepoint", rs.getString("tableowner"));

    // Count total rows
    int rowCount = 1;
    while (rs.next()) {
      rowCount++;
    }
    assertEquals(2, rowCount, "Should have exactly 2 tables (documents and tasks)");
  }

  @Test public void testQueryPgColumns() throws SQLException {
    String sql = "SELECT schemaname, tablename, columnname, datatype, columnposition "
        + "FROM sharepoint_test.pg_catalog.pg_columns "
        + "WHERE tablename = 'tasks' "
        + "ORDER BY columnposition";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);

    // Verify columns exist
    assertTrue(rs.next(), "Should have columns for tasks table");
    assertEquals("sharepoint_test", rs.getString("schemaname"));
    assertEquals("tasks", rs.getString("tablename"));
    assertEquals("id", rs.getString("columnname"));
    assertEquals("VARCHAR", rs.getString("datatype"));
    assertEquals(1, rs.getInt("columnposition"));

    // Check we have all expected columns
    List<String> columnNames = new ArrayList<>();
    columnNames.add(rs.getString("columnname"));
    while (rs.next()) {
      columnNames.add(rs.getString("columnname"));
    }

    assertTrue(columnNames.contains("title"), "Should have title column");
    assertTrue(columnNames.contains("status"), "Should have status column");
    assertTrue(columnNames.contains("priority"), "Should have priority column");
    assertTrue(columnNames.contains("zdue_date"), "Should have zdue_date column");
  }

  @Test public void testQuerySharePointLists() throws SQLException {
    String sql = "SELECT list_id, display_name, sql_name, created_date, last_modified "
        + "FROM sharepoint_test.pg_catalog.sharepoint_lists "
        + "ORDER BY sql_name";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);

    // Check first list
    assertTrue(rs.next(), "Should have at least one list");
    assertEquals("list-002", rs.getString("list_id"));
    assertEquals("Documents", rs.getString("display_name"));
    assertEquals("documents", rs.getString("sql_name"));
    assertNotNull(rs.getTimestamp("created_date"));
    assertNotNull(rs.getTimestamp("last_modified"));

    // Check second list
    assertTrue(rs.next(), "Should have second list");
    assertEquals("list-001", rs.getString("list_id"));
    assertEquals("Tasks", rs.getString("display_name"));
    assertEquals("tasks", rs.getString("sql_name"));
  }

  @Test public void testQuerySharePointColumns() throws SQLException {
    String sql = "SELECT list_id, column_id, display_name, sql_name, sharepoint_type, sql_type, is_required "
        + "FROM sharepoint_test.pg_catalog.sharepoint_columns "
        + "WHERE list_id = 'list-001' "
        + "ORDER BY column_id";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);

    // Verify columns metadata
    assertTrue(rs.next(), "Should have columns for list-001");
    assertEquals("list-001", rs.getString("list_id"));
    assertEquals("id", rs.getString("column_id"));
    assertEquals("ID", rs.getString("display_name"));
    assertEquals("id", rs.getString("sql_name"));
    assertEquals("text", rs.getString("sharepoint_type"));
    assertEquals("VARCHAR", rs.getString("sql_type"));
    assertTrue(rs.getBoolean("is_required"));
  }

  @Test public void testQueryInformationSchemaTables() throws SQLException {
    String sql = "SELECT \"TABLE_CATALOG\", \"TABLE_SCHEMA\", \"TABLE_NAME\", \"TABLE_TYPE\" "
        + "FROM sharepoint_test.information_schema.\"TABLES\" "
        + "ORDER BY \"TABLE_NAME\"";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);

    // Verify tables
    assertTrue(rs.next(), "Should have at least one table");
    assertEquals("sharepoint_test", rs.getString("TABLE_CATALOG"));
    assertEquals("sharepoint_test", rs.getString("TABLE_SCHEMA"));
    assertEquals("documents", rs.getString("TABLE_NAME"));
    assertEquals("BASE TABLE", rs.getString("TABLE_TYPE"));

    assertTrue(rs.next(), "Should have second table");
    assertEquals("tasks", rs.getString("TABLE_NAME"));
  }

  @Test public void testQueryInformationSchemaColumns() throws SQLException {
    String sql = "SELECT \"TABLE_CATALOG\", \"TABLE_SCHEMA\", \"TABLE_NAME\", \"COLUMN_NAME\", "
        + "\"ORDINAL_POSITION\", \"IS_NULLABLE\", \"DATA_TYPE\" "
        + "FROM sharepoint_test.information_schema.\"COLUMNS\" "
        + "WHERE \"TABLE_NAME\" = 'documents' "
        + "ORDER BY \"ORDINAL_POSITION\"";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);

    // Check columns
    assertTrue(rs.next(), "Should have columns for documents table");
    assertEquals("sharepoint_test", rs.getString("TABLE_CATALOG"));
    assertEquals("sharepoint_test", rs.getString("TABLE_SCHEMA"));
    assertEquals("documents", rs.getString("TABLE_NAME"));
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("NO", rs.getString("IS_NULLABLE"));
    assertEquals("VARCHAR", rs.getString("DATA_TYPE"));
  }

  @Test public void testJoinPgTablesWithSharePointLists() throws SQLException {
    String sql = "SELECT pt.tablename, sl.list_id, sl.display_name "
        + "FROM sharepoint_test.pg_catalog.pg_tables pt "
        + "JOIN sharepoint_test.pg_catalog.sharepoint_lists sl "
        + "ON pt.tablename = sl.sql_name "
        + "ORDER BY pt.tablename";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);

    // Verify join works
    assertTrue(rs.next(), "Should have joined results");
    assertEquals("documents", rs.getString("tablename"));
    assertEquals("list-002", rs.getString("list_id"));
    assertEquals("Documents", rs.getString("display_name"));

    assertTrue(rs.next(), "Should have second joined result");
    assertEquals("tasks", rs.getString("tablename"));
    assertEquals("list-001", rs.getString("list_id"));
    assertEquals("Tasks", rs.getString("display_name"));
  }

  @Test public void testFilterColumnsWithComplexCondition() throws SQLException {
    String sql = "SELECT c.\"TABLE_NAME\", c.\"COLUMN_NAME\", c.\"DATA_TYPE\", sc.is_required "
        + "FROM sharepoint_test.information_schema.\"COLUMNS\" c "
        + "JOIN sharepoint_test.pg_catalog.sharepoint_columns sc "
        + "ON c.\"COLUMN_NAME\" = sc.sql_name "
        + "WHERE c.\"DATA_TYPE\" IN ('INTEGER', 'TIMESTAMP') "
        + "AND sc.is_required = false "
        + "ORDER BY c.\"TABLE_NAME\", c.\"ORDINAL_POSITION\"";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);

    // Should find priority (INTEGER) and due_date (TIMESTAMP) columns
    int count = 0;
    while (rs.next()) {
      count++;
      String dataType = rs.getString("DATA_TYPE");
      assertTrue(dataType.equals("INTEGER") || dataType.equals("TIMESTAMP"),
          "Should only return INTEGER or TIMESTAMP columns");
      assertFalse(rs.getBoolean("is_required"), "Should only return non-required columns");
    }

    assertTrue(count >= 2, "Should find at least priority and due_date columns");
  }

  @Test public void testCountSharePointObjects() throws SQLException {
    // Count total lists
    String countListsSql = "SELECT COUNT(*) AS list_count "
        + "FROM sharepoint_test.pg_catalog.sharepoint_lists";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(countListsSql);

    assertTrue(rs.next());
    assertEquals(2, rs.getInt("list_count"), "Should have 2 lists");

    // Count total columns across all lists
    String countColumnsSql = "SELECT COUNT(*) AS column_count "
        + "FROM sharepoint_test.pg_catalog.sharepoint_columns";

    rs = stmt.executeQuery(countColumnsSql);
    assertTrue(rs.next());
    assertEquals(9, rs.getInt("column_count"), "Should have 9 total columns");

    // Count columns per list
    String columnsPerListSql = "SELECT list_id, COUNT(*) AS column_count "
        + "FROM sharepoint_test.pg_catalog.sharepoint_columns "
        + "GROUP BY list_id "
        + "ORDER BY list_id";

    rs = stmt.executeQuery(columnsPerListSql);

    assertTrue(rs.next());
    assertEquals("list-001", rs.getString("list_id"));
    assertEquals(5, rs.getInt("column_count"), "Tasks list should have 5 columns");

    assertTrue(rs.next());
    assertEquals("list-002", rs.getString("list_id"));
    assertEquals(4, rs.getInt("column_count"), "Documents list should have 4 columns");
  }

  @Test public void testPreparedStatementWithParameters() throws SQLException {
    String sql = "SELECT tablename, columnname, datatype "
        + "FROM sharepoint_test.pg_catalog.pg_columns "
        + "WHERE tablename = ? AND datatype = ? "
        + "ORDER BY columnposition";

    PreparedStatement pstmt = connection.prepareStatement(sql);
    pstmt.setString(1, "tasks");
    pstmt.setString(2, "VARCHAR");

    ResultSet rs = pstmt.executeQuery();

    // Should find VARCHAR columns in tasks table
    int count = 0;
    while (rs.next()) {
      count++;
      assertEquals("tasks", rs.getString("tablename"));
      assertEquals("VARCHAR", rs.getString("datatype"));
    }

    assertTrue(count >= 2, "Should find at least title and status VARCHAR columns");
  }

  /**
   * Mock SharePoint schema that includes pg_catalog and information_schema.
   */
  private static class MockSharePointSchemaWithMetadata extends AbstractSchema {
    private final Map<String, SharePointListMetadata> availableLists;

    MockSharePointSchemaWithMetadata() {
      this.availableLists = createMockLists();
    }

    private Map<String, SharePointListMetadata> createMockLists() {
      Map<String, SharePointListMetadata> lists = new HashMap<>();

      // Create mock Tasks list
      List<SharePointColumn> tasksColumns = new ArrayList<>();
      tasksColumns.add(new SharePointColumn("id", "ID", "text", true));
      tasksColumns.add(new SharePointColumn("title", "Title", "text", true));
      tasksColumns.add(new SharePointColumn("status", "Status", "choice", false));
      tasksColumns.add(new SharePointColumn("priority", "Priority", "integer", false));
      tasksColumns.add(new SharePointColumn("zdue_date", "Due Date", "datetime", false));
      SharePointListMetadata tasksList = new SharePointListMetadata("list-001", "Tasks", "SP.Data.TasksListItem", tasksColumns);
      lists.put("tasks", tasksList);

      // Create mock Documents list
      List<SharePointColumn> docsColumns = new ArrayList<>();
      docsColumns.add(new SharePointColumn("id", "ID", "text", true));
      docsColumns.add(new SharePointColumn("Name", "Name", "text", true));
      docsColumns.add(new SharePointColumn("FileSize", "File Size", "integer", false));
      docsColumns.add(new SharePointColumn("Modified", "Modified", "datetime", false));
      SharePointListMetadata docsList = new SharePointListMetadata("list-002", "Documents", "SP.Data.DocumentsItem", docsColumns);
      lists.put("documents", docsList);

      return lists;
    }

    @Override protected Map<String, org.apache.calcite.schema.Table> getTableMap() {
      Map<String, org.apache.calcite.schema.Table> tables = new HashMap<>();
      for (Map.Entry<String, SharePointListMetadata> entry : availableLists.entrySet()) {
        tables.put(entry.getKey(), createMockTable(entry.getValue()));
      }
      return tables;
    }

    @Override protected Map<String, org.apache.calcite.schema.Schema> getSubSchemaMap() {
      Map<String, org.apache.calcite.schema.Schema> schemas = new HashMap<>();
      // Create metadata schemas directly without SharePoint connection
      schemas.put("pg_catalog", new MockMetadataSchema(availableLists, "sharepoint_test", "pg_catalog"));
      schemas.put("information_schema", new MockMetadataSchema(availableLists, "sharepoint_test", "information_schema"));
      return schemas;
    }

    public Map<String, SharePointListMetadata> getAvailableLists() {
      return availableLists;
    }

    private org.apache.calcite.schema.Table createMockTable(SharePointListMetadata metadata) {
      return new MockSharePointTable(metadata);
    }
  }

  /**
   * Mock SharePointListSchema for testing metadata functionality.
   */
  private static class MockSharePointListSchema extends SharePointListSchema {
    private final Map<String, SharePointListMetadata> availableLists;

    MockSharePointListSchema(Map<String, SharePointListMetadata> availableLists) {
      // Pass dummy values to parent constructor - this will fail at runtime but compile
      super("https://mock.sharepoint.com", createDummyAuthConfig());
      this.availableLists = availableLists;
    }

    private static Map<String, Object> createDummyAuthConfig() {
      Map<String, Object> config = new HashMap<>();
      // Use CLIENT_CREDENTIALS which is a valid auth type
      config.put("authType", "CLIENT_CREDENTIALS");
      config.put("tenantId", "dummy-tenant");
      config.put("clientId", "dummy-client");
      config.put("clientSecret", "dummy-secret");
      return config;
    }

    @Override public Map<String, org.apache.calcite.schema.Table> getTableMapForMetadata() {
      // Return mock tables without needing actual SharePoint connection
      Map<String, org.apache.calcite.schema.Table> tables = new HashMap<>();
      for (Map.Entry<String, SharePointListMetadata> entry : availableLists.entrySet()) {
        tables.put(entry.getKey(), new MockSharePointTable(entry.getValue()));
      }
      return tables;
    }

    @Override protected Map<String, org.apache.calcite.schema.Table> getTableMap() {
      return getTableMapForMetadata();
    }
  }

  /**
   * Mock metadata schema that directly creates the metadata tables without SharePoint connection.
   */
  private static class MockMetadataSchema extends SharePointMetadataSchema {
    private final Map<String, SharePointListMetadata> availableLists;
    private final String actualSchemaName;

    MockMetadataSchema(Map<String, SharePointListMetadata> availableLists, String catalogName, String schemaName) {
      // Pass null for sourceSchema - we'll override the methods that use it
      super(null, catalogName, schemaName);
      this.availableLists = availableLists;
      this.actualSchemaName = schemaName;
    }

    @Override protected Map<String, org.apache.calcite.schema.Table> getTableMap() {
      // Create the metadata tables directly
      return createMetadataTables();
    }

    private Map<String, org.apache.calcite.schema.Table> createMetadataTables() {
      Map<String, org.apache.calcite.schema.Table> tables = new HashMap<>();

      if (actualSchemaName.equals("pg_catalog")) {
        // pg_catalog tables use lowercase names
        tables.put("pg_tables", new MockPgTablesTable());
        tables.put("pg_columns", new MockPgColumnsTable());
        tables.put("sharepoint_lists", new MockSharePointListsTable());
        tables.put("sharepoint_columns", new MockSharePointColumnsTable());
      } else {
        // information_schema tables use UPPERCASE names
        tables.put("TABLES", new MockInformationSchemaTablesTable());
        tables.put("COLUMNS", new MockInformationSchemaColumnsTable());
      }

      return tables;
    }

    private Map<String, org.apache.calcite.schema.Table> getMockTables() {
      Map<String, org.apache.calcite.schema.Table> tables = new HashMap<>();
      for (Map.Entry<String, SharePointListMetadata> entry : availableLists.entrySet()) {
        tables.put(entry.getKey(), new MockSharePointTable(entry.getValue()));
      }
      return tables;
    }

    // Mock implementations of metadata tables would go here
    // For now, just return empty implementations
    private class MockPgTablesTable extends org.apache.calcite.schema.impl.AbstractTable implements org.apache.calcite.schema.ScannableTable {
      @Override public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("schemaname", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("tablename", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("tableowner", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("tablespace", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("hasindexes", org.apache.calcite.sql.type.SqlTypeName.BOOLEAN)
            .add("hasrules", org.apache.calcite.sql.type.SqlTypeName.BOOLEAN)
            .add("hastriggers", org.apache.calcite.sql.type.SqlTypeName.BOOLEAN)
            .build();
      }

      @Override public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
        List<Object[]> rows = new ArrayList<>();
        for (String tableName : availableLists.keySet()) {
          rows.add(new Object[] {
              "sharepoint_test", tableName, "sharepoint", null, false, false, false
          });
        }
        return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
      }
    }

    private class MockPgColumnsTable extends org.apache.calcite.schema.impl.AbstractTable implements org.apache.calcite.schema.ScannableTable {
      @Override public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("schemaname", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("tablename", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("columnname", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("datatype", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("columnposition", org.apache.calcite.sql.type.SqlTypeName.INTEGER)
            .build();
      }

      @Override public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
        List<Object[]> rows = new ArrayList<>();
        for (Map.Entry<String, SharePointListMetadata> entry : availableLists.entrySet()) {
          String tableName = entry.getKey();
          SharePointListMetadata metadata = entry.getValue();
          int position = 1;
          for (SharePointColumn column : metadata.getColumns()) {
            String sqlType = getSqlTypeName(column.getType());
            rows.add(new Object[] {
                "sharepoint_test", tableName,
                SharePointNameConverter.toSqlName(column.getInternalName()),
                sqlType, position++
            });
          }
        }
        return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
      }

      private String getSqlTypeName(String sharePointType) {
        switch (sharePointType.toLowerCase()) {
          case "text":
          case "choice":
            return "VARCHAR";
          case "integer":
            return "INTEGER";
          case "number":
            return "DOUBLE";
          case "boolean":
            return "BOOLEAN";
          case "datetime":
            return "TIMESTAMP";
          default:
            return "VARCHAR";
        }
      }
    }

    private class MockInformationSchemaTablesTable extends org.apache.calcite.schema.impl.AbstractTable implements org.apache.calcite.schema.ScannableTable {
      @Override public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("TABLE_CATALOG", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("TABLE_SCHEMA", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("TABLE_NAME", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("TABLE_TYPE", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .build();
      }

      @Override public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
        List<Object[]> rows = new ArrayList<>();
        for (String tableName : availableLists.keySet()) {
          rows.add(new Object[] {
              "sharepoint_test", "sharepoint_test", tableName, "BASE TABLE"
          });
        }
        return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
      }
    }

    private class MockInformationSchemaColumnsTable extends org.apache.calcite.schema.impl.AbstractTable implements org.apache.calcite.schema.ScannableTable {
      @Override public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("TABLE_CATALOG", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("TABLE_SCHEMA", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("TABLE_NAME", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("COLUMN_NAME", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("ORDINAL_POSITION", org.apache.calcite.sql.type.SqlTypeName.INTEGER)
            .add("IS_NULLABLE", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("DATA_TYPE", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .build();
      }

      @Override public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
        List<Object[]> rows = new ArrayList<>();
        for (Map.Entry<String, SharePointListMetadata> entry : availableLists.entrySet()) {
          String tableName = entry.getKey();
          SharePointListMetadata metadata = entry.getValue();
          int ordinal = 1;
          for (SharePointColumn column : metadata.getColumns()) {
            String sqlType = getSqlTypeName(column.getType());
            rows.add(new Object[] {
                "sharepoint_test", "sharepoint_test", tableName,
                SharePointNameConverter.toSqlName(column.getInternalName()),
                ordinal++, column.isRequired() ? "NO" : "YES", sqlType
            });
          }
        }
        return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
      }

      private String getSqlTypeName(String sharePointType) {
        switch (sharePointType.toLowerCase()) {
          case "text":
          case "choice":
            return "VARCHAR";
          case "integer":
            return "INTEGER";
          case "number":
            return "DOUBLE";
          case "boolean":
            return "BOOLEAN";
          case "datetime":
            return "TIMESTAMP";
          default:
            return "VARCHAR";
        }
      }
    }

    private class MockSharePointListsTable extends org.apache.calcite.schema.impl.AbstractTable implements org.apache.calcite.schema.ScannableTable {
      @Override public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("list_id", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("display_name", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("sql_name", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("created_date", org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP)
            .add("last_modified", org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP)
            .build();
      }

      @Override public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
        List<Object[]> rows = new ArrayList<>();
        for (Map.Entry<String, SharePointListMetadata> entry : availableLists.entrySet()) {
          SharePointListMetadata metadata = entry.getValue();
          rows.add(new Object[] {
              metadata.getListId(), metadata.getDisplayName(), metadata.getListName(),
              java.sql.Timestamp.valueOf("2023-01-01 10:00:00"),
              java.sql.Timestamp.valueOf("2023-01-15 14:30:00")
          });
        }
        return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
      }
    }

    private class MockSharePointColumnsTable extends org.apache.calcite.schema.impl.AbstractTable implements org.apache.calcite.schema.ScannableTable {
      @Override public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("list_id", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("column_id", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("display_name", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("sql_name", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("sharepoint_type", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("sql_type", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .add("is_required", org.apache.calcite.sql.type.SqlTypeName.BOOLEAN)
            .build();
      }

      @Override public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
        List<Object[]> rows = new ArrayList<>();
        for (Map.Entry<String, SharePointListMetadata> entry : availableLists.entrySet()) {
          SharePointListMetadata metadata = entry.getValue();
          for (SharePointColumn column : metadata.getColumns()) {
            String sqlType = getSqlTypeName(column.getType());
            rows.add(new Object[] {
                metadata.getListId(), column.getInternalName(), column.getDisplayName(),
                SharePointNameConverter.toSqlName(column.getInternalName()),
                column.getType(), sqlType, column.isRequired()
            });
          }
        }
        return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
      }

      private String getSqlTypeName(String sharePointType) {
        switch (sharePointType.toLowerCase()) {
          case "text":
          case "choice":
            return "VARCHAR";
          case "integer":
            return "INTEGER";
          case "number":
            return "DOUBLE";
          case "boolean":
            return "BOOLEAN";
          case "datetime":
            return "TIMESTAMP";
          default:
            return "VARCHAR";
        }
      }
    }
  }

  /**
   * Mock SharePoint table for testing.
   */
  private static class MockSharePointTable extends org.apache.calcite.schema.impl.AbstractTable
      implements org.apache.calcite.schema.ScannableTable {
    private final SharePointListMetadata metadata;

    MockSharePointTable(SharePointListMetadata metadata) {
      this.metadata = metadata;
    }

    @Override public org.apache.calcite.rel.type.RelDataType getRowType(
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
      org.apache.calcite.rel.type.RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (SharePointColumn column : metadata.getColumns()) {
        org.apache.calcite.sql.type.SqlTypeName sqlType = getSqlType(column.getType());
        builder.add(SharePointNameConverter.toSqlName(column.getInternalName()),
                   typeFactory.createSqlType(sqlType));
      }
      return builder.build();
    }

    private org.apache.calcite.sql.type.SqlTypeName getSqlType(String sharePointType) {
      switch (sharePointType.toLowerCase()) {
        case "text":
        case "choice":
          return org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
        case "integer":
          return org.apache.calcite.sql.type.SqlTypeName.INTEGER;
        case "number":
          return org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
        case "boolean":
          return org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
        case "datetime":
          return org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
        default:
          return org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
      }
    }

    @Override public org.apache.calcite.linq4j.Enumerable<Object[]> scan(
        org.apache.calcite.DataContext root) {
      // Return empty data for mock
      return org.apache.calcite.linq4j.Linq4j.emptyEnumerable();
    }
  }
}
