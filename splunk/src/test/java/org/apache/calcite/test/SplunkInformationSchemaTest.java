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

import org.apache.calcite.adapter.splunk.SplunkSchemaFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;

/**
 * Test SQL standard information_schema for Splunk adapter.
 */
@Tag("unit")
class SplunkInformationSchemaTest {

  private Connection createTestConnection() throws SQLException {
    Properties info = new Properties();
    
    // Use inline model JSON
    String model = "inline:{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"splunk\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"splunk\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"" + SplunkSchemaFactory.class.getName() + "\",\n"
        + "      \"operand\": {\n"
        + "        \"url\": \"https://nonexistent.splunk.server:8089\",\n"
        + "        \"user\": \"admin\",\n"
        + "        \"password\": \"changeme\",\n"
        + "        \"disableSslValidation\": true,\n"
        + "        \"searchCacheEnabled\": false\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    
    info.setProperty("model", model);
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  @Test void testInformationSchemaSchemata() throws SQLException {
    try (Connection connection = createTestConnection();
         Statement stmt = connection.createStatement()) {
      
      // Test SCHEMATA table
      String query = "SELECT CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER "
          + "FROM information_schema.SCHEMATA "
          + "ORDER BY SCHEMA_NAME";
      
      try (ResultSet rs = stmt.executeQuery(query)) {
        Set<String> schemas = new HashSet<>();
        while (rs.next()) {
          String catalog = rs.getString("CATALOG_NAME");
          String schema = rs.getString("SCHEMA_NAME");
          String owner = rs.getString("SCHEMA_OWNER");
          
          assertThat("Catalog should be SPLUNK", catalog, is("SPLUNK"));
          assertThat("Schema name should not be null", schema, notNullValue());
          assertThat("Owner should not be null", owner, notNullValue());
          
          schemas.add(schema);
        }
        
        // Should have at least the core schemas
        assertThat("Should have splunk schema", schemas, hasItem("splunk"));
        assertThat("Should have information_schema", schemas, hasItem("information_schema"));
        assertThat("Should have pg_catalog", schemas, hasItem("pg_catalog"));
      }
    }
  }

  @Test void testInformationSchemaTables() throws SQLException {
    try (Connection connection = createTestConnection();
         Statement stmt = connection.createStatement()) {
      
      // Test TABLES table
      String query = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
          + "FROM information_schema.TABLES "
          + "WHERE TABLE_SCHEMA = 'splunk' "
          + "ORDER BY TABLE_NAME";
      
      try (ResultSet rs = stmt.executeQuery(query)) {
        int tableCount = 0;
        while (rs.next()) {
          String catalog = rs.getString("TABLE_CATALOG");
          String schema = rs.getString("TABLE_SCHEMA");
          String table = rs.getString("TABLE_NAME");
          String type = rs.getString("TABLE_TYPE");
          
          assertThat("Catalog should be SPLUNK", catalog, is("SPLUNK"));
          assertThat("Schema should be splunk", schema, is("splunk"));
          assertThat("Table name should not be null", table, notNullValue());
          assertThat("Table type should be BASE TABLE", type, is("BASE TABLE"));
          
          tableCount++;
        }
        
        // Should have at least some default tables
        assertThat("Should have tables in splunk schema", tableCount, greaterThan(0));
      }
    }
  }

  @Test void testInformationSchemaColumns() throws SQLException {
    try (Connection connection = createTestConnection();
         Statement stmt = connection.createStatement()) {
      
      // Test COLUMNS table
      String query = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, "
          + "ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE "
          + "FROM information_schema.COLUMNS "
          + "WHERE TABLE_SCHEMA = 'splunk' "
          + "AND TABLE_NAME = 'web' "
          + "ORDER BY ORDINAL_POSITION";
      
      try (ResultSet rs = stmt.executeQuery(query)) {
        int columnCount = 0;
        int previousPosition = 0;
        
        while (rs.next()) {
          String schema = rs.getString("TABLE_SCHEMA");
          String table = rs.getString("TABLE_NAME");
          String column = rs.getString("COLUMN_NAME");
          int position = rs.getInt("ORDINAL_POSITION");
          String nullable = rs.getString("IS_NULLABLE");
          String dataType = rs.getString("DATA_TYPE");
          
          assertThat("Schema should be splunk", schema, is("splunk"));
          assertThat("Table should be web", table, is("web"));
          assertThat("Column name should not be null", column, notNullValue());
          assertThat("Position should be sequential", position, is(previousPosition + 1));
          assertThat("Nullable should be YES or NO", 
              nullable.equals("YES") || nullable.equals("NO"), is(true));
          assertThat("Data type should not be null", dataType, notNullValue());
          
          previousPosition = position;
          columnCount++;
        }
        
        // Web table should have columns
        assertThat("Should have columns for web table", columnCount, greaterThan(0));
      }
    }
  }

  @Test void testInformationSchemaStandardCompliance() throws SQLException {
    try (Connection connection = createTestConnection();
         Statement stmt = connection.createStatement()) {
      
      // Test that all required SQL standard tables exist
      String query = "SELECT TABLE_NAME "
          + "FROM information_schema.TABLES "
          + "WHERE TABLE_SCHEMA = 'information_schema' "
          + "ORDER BY TABLE_NAME";
      
      try (ResultSet rs = stmt.executeQuery(query)) {
        Set<String> tables = new HashSet<>();
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
        
        // Verify SQL standard required tables exist
        assertThat("Should have SCHEMATA table", tables, hasItem("SCHEMATA"));
        assertThat("Should have TABLES table", tables, hasItem("TABLES"));
        assertThat("Should have COLUMNS table", tables, hasItem("COLUMNS"));
        assertThat("Should have TABLE_CONSTRAINTS table", tables, hasItem("TABLE_CONSTRAINTS"));
        assertThat("Should have KEY_COLUMN_USAGE table", tables, hasItem("KEY_COLUMN_USAGE"));
        assertThat("Should have VIEWS table", tables, hasItem("VIEWS"));
        assertThat("Should have ROUTINES table", tables, hasItem("ROUTINES"));
        assertThat("Should have PARAMETERS table", tables, hasItem("PARAMETERS"));
      }
    }
  }

  @Test void testCaseInsensitivity() throws SQLException {
    try (Connection connection = createTestConnection();
         Statement stmt = connection.createStatement()) {
      
      // Test that column names are case-insensitive (SQL standard)
      String query1 = "SELECT table_catalog, table_schema, table_name "
          + "FROM information_schema.tables "
          + "WHERE table_schema = 'splunk' "
          + "LIMIT 1";
      
      String query2 = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME "
          + "FROM INFORMATION_SCHEMA.TABLES "
          + "WHERE TABLE_SCHEMA = 'splunk' "
          + "LIMIT 1";
      
      // Both queries should work
      try (ResultSet rs1 = stmt.executeQuery(query1)) {
        assertThat("Lowercase query should work", rs1.next(), is(true));
      }
      
      try (ResultSet rs2 = stmt.executeQuery(query2)) {
        assertThat("Uppercase query should work", rs2.next(), is(true));
      }
    }
  }

  @Test void testJdbcCompatibleQuery() throws SQLException {
    try (Connection connection = createTestConnection()) {
      
      // Test a typical JDBC metadata query pattern
      String query = "SELECT "
          + "NULL AS TABLE_CAT, "
          + "TABLE_SCHEMA AS TABLE_SCHEM, "
          + "TABLE_NAME, "
          + "COLUMN_NAME, "
          + "CASE DATA_TYPE "
          + "  WHEN 'VARCHAR' THEN 12 "
          + "  WHEN 'INTEGER' THEN 4 "
          + "  WHEN 'BIGINT' THEN -5 "
          + "  WHEN 'BOOLEAN' THEN 16 "
          + "  WHEN 'TIMESTAMP' THEN 93 "
          + "  ELSE 1111 "
          + "END AS DATA_TYPE, "
          + "DATA_TYPE AS TYPE_NAME, "
          + "CHARACTER_MAXIMUM_LENGTH AS COLUMN_SIZE, "
          + "NULL AS BUFFER_LENGTH, "
          + "NUMERIC_SCALE AS DECIMAL_DIGITS, "
          + "10 AS NUM_PREC_RADIX, "
          + "CASE IS_NULLABLE WHEN 'YES' THEN 1 ELSE 0 END AS NULLABLE, "
          + "NULL AS REMARKS, "
          + "COLUMN_DEFAULT AS COLUMN_DEF, "
          + "0 AS SQL_DATA_TYPE, "
          + "0 AS SQL_DATETIME_SUB, "
          + "CHARACTER_OCTET_LENGTH AS CHAR_OCTET_LENGTH, "
          + "ORDINAL_POSITION, "
          + "IS_NULLABLE "
          + "FROM information_schema.COLUMNS "
          + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
          + "ORDER BY ORDINAL_POSITION";
      
      try (PreparedStatement pstmt = connection.prepareStatement(query)) {
        pstmt.setString(1, "splunk");
        pstmt.setString(2, "web");
        
        try (ResultSet rs = pstmt.executeQuery()) {
          int columnCount = 0;
          while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            String typeName = rs.getString("TYPE_NAME");
            int ordinalPos = rs.getInt("ORDINAL_POSITION");
            
            assertThat("Column name should not be null", columnName, notNullValue());
            assertThat("Type name should not be null", typeName, notNullValue());
            assertThat("Ordinal position should be positive", ordinalPos, greaterThan(0));
            
            columnCount++;
          }
          
          assertThat("Should retrieve columns", columnCount, greaterThan(0));
        }
      }
    }
  }
}