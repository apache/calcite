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
package org.apache.calcite.adapter.ops;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test case insensitivity for information_schema and pg_catalog tables.
 * Note: Only the metadata schemas (information_schema and pg_catalog) are case insensitive.
 * The main cloud ops tables remain case sensitive with ORACLE lex.
 */
@Tag("unit")
public class MetadataCaseInsensitivityTest {

  @Test public void testInformationSchemaCaseInsensitive() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test various case combinations for information_schema tables
      String[] tableVariations = {
          "information_schema.tables",
          "information_schema.TABLES",
          "information_schema.Tables",
          "INFORMATION_SCHEMA.TABLES",
          "INFORMATION_SCHEMA.tables",
          "Information_Schema.Tables"
      };

      for (String tableRef : tableVariations) {
        String query = "SELECT COUNT(*) FROM " + tableRef + " WHERE 1=0";
        try (ResultSet rs = statement.executeQuery(query)) {
          assertNotNull(rs, "Query should execute successfully for: " + tableRef);
          assertTrue(rs.next(), "Should have result row for: " + tableRef);
          System.out.println("Successfully queried: " + tableRef);
        }
      }
    }
  }

  @Test public void testPgCatalogCaseInsensitive() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test various case combinations for pg_catalog tables
      String[] tableVariations = {
          "pg_catalog.pg_tables",
          "pg_catalog.PG_TABLES",
          "pg_catalog.Pg_Tables",
          "PG_CATALOG.pg_tables",
          "PG_CATALOG.PG_TABLES",
          "Pg_Catalog.Pg_Tables"
      };

      for (String tableRef : tableVariations) {
        String query = "SELECT COUNT(*) FROM " + tableRef + " WHERE 1=0";
        try (ResultSet rs = statement.executeQuery(query)) {
          assertNotNull(rs, "Query should execute successfully for: " + tableRef);
          assertTrue(rs.next(), "Should have result row for: " + tableRef);
          System.out.println("Successfully queried: " + tableRef);
        }
      }
    }
  }

  @Test public void testColumnsCaseInsensitive() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test case insensitive column references
      String[] queries = {
          "SELECT table_name FROM information_schema.tables WHERE 1=0",
          "SELECT TABLE_NAME FROM information_schema.tables WHERE 1=0",
          "SELECT Table_Name FROM information_schema.tables WHERE 1=0",
          "SELECT tABle_NaMe FROM INFORMATION_SCHEMA.TABLES WHERE 1=0"
      };

      for (String query : queries) {
        try (ResultSet rs = statement.executeQuery(query)) {
          assertNotNull(rs, "Query should execute successfully: " + query);
          System.out.println("Successfully executed: " + query);
        }
      }
    }
  }

  @Test public void testMixedCaseQueries() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test complex mixed case queries
      String query =
          "SELECT t.TABLE_NAME, c.COLUMN_NAME " +
          "FROM Information_Schema.Tables t " +
          "JOIN information_schema.COLUMNS c " +
          "ON t.table_name = c.TABLE_NAME " +
          "WHERE t.TABLE_SCHEMA = 'public' " +
          "AND c.data_type = 'VARCHAR' " +
          "LIMIT 5";

      try (ResultSet rs = statement.executeQuery(query)) {
        assertNotNull(rs, "Complex mixed case query should execute");
        while (rs.next()) {
          String tableName = rs.getString(1);
          String columnName = rs.getString(2);
          System.out.println("Table: " + tableName + ", Column: " + columnName);
        }
      }
    }
  }

  @Test public void testMainSchemaTablesCaseSensitive() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Main schema tables should be case sensitive with ORACLE lex
      // With ORACLE lex, we need to use quoted identifiers to preserve lowercase
      String query1 = "SELECT COUNT(*) FROM \"public\".\"kubernetes_clusters\" WHERE 1=0";
      try (ResultSet rs = statement.executeQuery(query1)) {
        assertNotNull(rs, "Quoted lowercase table name should work");
        assertTrue(rs.next());
        System.out.println("Successfully queried: \"public\".\"kubernetes_clusters\"");
      }

      // This should fail (uppercase)
      // With ORACLE lex, unquoted identifiers are uppercased, so public.KUBERNETES_CLUSTERS
      // becomes PUBLIC.KUBERNETES_CLUSTERS, which should fail since we only have "public" schema
      String query2 = "SELECT COUNT(*) FROM public.KUBERNETES_CLUSTERS WHERE 1=0";
      boolean failed = false;
      try (ResultSet rs = statement.executeQuery(query2)) {
        // If we get here, the query succeeded when it shouldn't have
      } catch (SQLException e) {
        failed = true;
        // The error should be about schema not found (PUBLIC vs public)
        assertTrue(e.getMessage().contains("PUBLIC") || e.getMessage().contains("KUBERNETES_CLUSTERS"),
            "Error should mention uppercase identifier");
        System.out.println("Expected failure for uppercase identifier: " + e.getMessage());
      }
      assertTrue(failed, "Uppercase table name should fail with ORACLE lex");
    }
  }

  private Connection createTestConnection() throws SQLException {
    // Create test Cloud Governance configuration
    Map<String, Object> operands = new HashMap<>();
    operands.put("azure.tenantId", "test-tenant");
    operands.put("azure.clientId", "test-client");
    operands.put("azure.clientSecret", "test-secret");
    operands.put("azure.subscriptionIds", "test-sub");

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    CloudOpsSchemaFactory factory = new CloudOpsSchemaFactory();
    rootSchema.add("public", factory.create(rootSchema, "public", operands));

    return connection;
  }
}
