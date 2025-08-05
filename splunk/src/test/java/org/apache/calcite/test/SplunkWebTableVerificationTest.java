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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify the web table structure and basic functionality.
 * Uses mock connection to avoid requiring live Splunk instance.
 */
@Tag("unit")
public class SplunkWebTableVerificationTest {

  @Test void testWebTableExists() throws SQLException {
    Properties props = new Properties();
    props.setProperty("model", "inline:" + getTestModel());
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      DatabaseMetaData metaData = connection.getMetaData();

      // Check that web table exists in splunk schema
      ResultSet tables = metaData.getTables(null, "splunk", "web", null);
      assertTrue(tables.next(), "Web table should exist in splunk schema");
      assertEquals("splunk", tables.getString("TABLE_SCHEM"));
      assertEquals("web", tables.getString("TABLE_NAME"));
      assertEquals("TABLE", tables.getString("TABLE_TYPE"));
    }
  }

  @Test void testWebTableStructure() throws SQLException {
    Properties props = new Properties();
    props.setProperty("model", "inline:" + getTestModel());
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      DatabaseMetaData metaData = connection.getMetaData();

      // Get columns for web table
      ResultSet columns = metaData.getColumns(null, "splunk", "web", null);

      List<String> columnNames = new ArrayList<>();
      while (columns.next()) {
        columnNames.add(columns.getString("COLUMN_NAME"));
      }

      // Verify essential web table columns exist
      assertTrue(columnNames.contains("_time"), "Web table should have _time column");
      assertTrue(columnNames.contains("host"), "Web table should have host column");
      assertTrue(columnNames.contains("source"), "Web table should have source column");
      assertTrue(columnNames.contains("sourcetype"), "Web table should have sourcetype column");
      assertTrue(columnNames.contains("index"), "Web table should have index column");
      assertTrue(columnNames.contains("uri_path"), "Web table should have uri_path column");
      assertTrue(columnNames.contains("status"), "Web table should have status column");
      assertTrue(columnNames.contains("method"), "Web table should have method column");
      assertTrue(columnNames.contains("user"), "Web table should have user column");
      assertTrue(columnNames.contains("src_ip"), "Web table should have src_ip column");
      assertTrue(columnNames.contains("_extra"), "Web table should have _extra column");

      // Verify minimum expected number of columns
      assertTrue(columnNames.size() >= 10,
          "Web table should have at least 10 columns, found: " + columnNames.size());
    }
  }

  @Test void testWebTableQuery() throws SQLException {
    Properties props = new Properties();
    props.setProperty("model", "inline:" + getTestModel());
    // Set PostgreSQL-style lexical conventions
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
         Statement stmt = connection.createStatement()) {

      // Basic SELECT query on web table (should not fail)
      // With PostgreSQL-style settings, unquoted identifiers are lowercase
      // Note: 'method' is a reserved keyword and must be quoted
      String sql = "SELECT uri_path, status, \"method\" FROM web LIMIT 5";
      ResultSet rs = stmt.executeQuery(sql);

      // Verify result set has expected columns
      assertEquals("uri_path", rs.getMetaData().getColumnName(1));
      assertEquals("status", rs.getMetaData().getColumnName(2));
      assertEquals("method", rs.getMetaData().getColumnName(3));

      // Note: With mock data, we won't have actual rows, but the query structure should work
      // This verifies the table schema is correctly set up for querying
    }
  }

  @Test void testWebTableMetadataQueries() throws SQLException {
    Properties props = new Properties();
    props.setProperty("model", "inline:" + getTestModel());
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
         Statement stmt = connection.createStatement()) {

      // Test information_schema query for web table
      // information_schema uses uppercase column names per SQL standard
      String sql = "SELECT \"COLUMN_NAME\", \"DATA_TYPE\" FROM information_schema.\"COLUMNS\" " +
                   "WHERE \"TABLE_SCHEMA\" = 'splunk' AND \"TABLE_NAME\" = 'web' " +
                   "ORDER BY \"ORDINAL_POSITION\"";

      ResultSet rs = stmt.executeQuery(sql);

      List<String> columns = new ArrayList<>();
      while (rs.next()) {
        columns.add(rs.getString("COLUMN_NAME"));
      }

      // Verify web table columns are discoverable through metadata
      assertTrue(columns.contains("_time"), "Web table _time column should be discoverable");
      assertTrue(columns.contains("uri_path"), "Web table uri_path column should be discoverable");
      assertTrue(columns.contains("status"), "Web table status column should be discoverable");
    }
  }

  @Test void testWebTablePgCatalogQueries() throws SQLException {
    Properties props = new Properties();
    props.setProperty("model", "inline:" + getTestModel());
    // Set PostgreSQL-style lexical conventions
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
         Statement stmt = connection.createStatement()) {

      // Test pg_catalog query for web table
      // With PostgreSQL-style settings, pg_catalog can be accessed without quotes
      String sql = "SELECT schemaname, tablename FROM pg_catalog.pg_tables " +
                   "WHERE schemaname = 'splunk' AND tablename = 'web'";

      ResultSet rs = stmt.executeQuery(sql);

      assertTrue(rs.next(), "Web table should be found in pg_catalog.pg_tables");
      assertEquals("splunk", rs.getString("schemaname"));
      assertEquals("web", rs.getString("tablename"));
    }
  }

  /**
   * Creates a test model with web table definition.
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
        "            \"name\": \"web\",\n"
  +
        "            \"search\": \"search index=web sourcetype=access_combined\",\n"
  +
        "            \"fields\": [\n"
  +
        "              {\"name\": \"_time\", \"type\": \"TIMESTAMP\", \"nullable\": false},\n"
  +
        "              {\"name\": \"host\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"source\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"sourcetype\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"index\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"uri_path\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"status\", \"type\": \"INTEGER\"},\n"
  +
        "              {\"name\": \"method\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"user\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"src_ip\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"dest_port\", \"type\": \"INTEGER\"},\n"
  +
        "              {\"name\": \"bytes\", \"type\": \"BIGINT\"},\n"
  +
        "              {\"name\": \"response_time\", \"type\": \"DOUBLE\"},\n"
  +
        "              {\"name\": \"user_agent\", \"type\": \"VARCHAR\"},\n"
  +
        "              {\"name\": \"referer\", \"type\": \"VARCHAR\"},\n"
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
