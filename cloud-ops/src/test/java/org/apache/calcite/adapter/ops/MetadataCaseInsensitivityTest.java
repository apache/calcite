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
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test case sensitivity for information_schema and pg_catalog tables.
 * Note: The metadata schemas (information_schema and pg_catalog) use uppercase table names
 * that must be quoted. Schema names are lowercase. The main cloud ops tables use lowercase names.
 */
@Tag("unit")
public class MetadataCaseInsensitivityTest {

  @Test public void testInformationSchemaCorrectCasing() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test correct case combinations for information_schema tables
      String[] correctTableRefs = {
          "information_schema.\"TABLES\"",     // Correct: lowercase schema, quoted uppercase table
          "information_schema.\"COLUMNS\""     // Correct: lowercase schema, quoted uppercase table
      };

      for (String tableRef : correctTableRefs) {
        String query = "SELECT COUNT(*) FROM " + tableRef + " WHERE 1=0";
        try (ResultSet rs = statement.executeQuery(query)) {
          assertNotNull(rs, "Query should execute successfully for: " + tableRef);
          assertTrue(rs.next(), "Should have result row for: " + tableRef);
          System.out.println("Successfully queried: " + tableRef);
        }
      }
    }
  }

  @Test public void testPgCatalogCorrectCasing() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test correct case combinations for pg_catalog tables
      String[] correctTableRefs = {
          "pg_catalog.\"PG_TABLES\"",      // Correct: lowercase schema, quoted uppercase table
          "pg_catalog.\"CLOUD_RESOURCES\"", // Correct: lowercase schema, quoted uppercase table
          "pg_catalog.\"OPS_POLICIES\""    // Correct: lowercase schema, quoted uppercase table
      };

      for (String tableRef : correctTableRefs) {
        String query = "SELECT COUNT(*) FROM " + tableRef + " WHERE 1=0";
        try (ResultSet rs = statement.executeQuery(query)) {
          assertNotNull(rs, "Query should execute successfully for: " + tableRef);
          assertTrue(rs.next(), "Should have result row for: " + tableRef);
          System.out.println("Successfully queried: " + tableRef);
        }
      }
    }
  }

  @Test public void testColumnsCorrectCasing() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test correct column references with proper table quoting
      String[] queries = {
          "SELECT \"TABLE_NAME\" FROM information_schema.\"TABLES\" WHERE 1=0",
          "SELECT \"TABLE_SCHEMA\" FROM information_schema.\"TABLES\" WHERE 1=0",
          "SELECT tablename FROM pg_catalog.\"PG_TABLES\" WHERE 1=0",
          "SELECT table_name FROM pg_catalog.\"CLOUD_RESOURCES\" WHERE 1=0"
      };

      for (String query : queries) {
        try (ResultSet rs = statement.executeQuery(query)) {
          assertNotNull(rs, "Query should execute successfully: " + query);
          System.out.println("Successfully executed: " + query);
        }
      }
    }
  }

  @Test public void testCorrectCaseQueries() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test complex queries with proper case and quoting
      String query =
          "SELECT t.\"TABLE_NAME\", cr.resource_type " +
          "FROM information_schema.\"TABLES\" t " +
          "JOIN pg_catalog.\"CLOUD_RESOURCES\" cr " +
          "ON t.\"TABLE_NAME\" = cr.table_name " +
          "WHERE t.\"TABLE_SCHEMA\" = 'public' " +
          "LIMIT 5";

      try (ResultSet rs = statement.executeQuery(query)) {
        assertNotNull(rs, "Complex cross-schema query should execute");
        while (rs.next()) {
          String tableName = rs.getString(1);
          String resourceType = rs.getString(2);
          System.out.println("Table: " + tableName + ", Type: " + resourceType);
        }
      }
    }
  }

  @Test public void testMainSchemaTablesCaseSensitive() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Main schema tables should be case sensitive with ORACLE lex
      // Cloud ops tables use lowercase names without quoting required
      String query1 = "SELECT COUNT(*) FROM public.kubernetes_clusters WHERE 1=0";
      try (ResultSet rs = statement.executeQuery(query1)) {
        assertNotNull(rs, "Lowercase cloud ops table name should work");
        assertTrue(rs.next());
        System.out.println("Successfully queried: public.kubernetes_clusters");
      }

      // Test that the correct lowercase works
      String query2 = "SELECT COUNT(*) FROM public.storage_resources WHERE 1=0";
      try (ResultSet rs = statement.executeQuery(query2)) {
        assertNotNull(rs, "Lowercase cloud ops table name should work");
        assertTrue(rs.next());
        System.out.println("Successfully queried: public.storage_resources");
      }
    }
  }

  private Connection createTestConnection() throws SQLException {
    CloudOpsConfig config = createTestConfig();

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    CloudOpsSchemaFactory factory = new CloudOpsSchemaFactory();
    rootSchema.add("public", factory.create(rootSchema, "public", configToOperands(config)));

    return connection;
  }

  private CloudOpsConfig createTestConfig() {
    // Only use real credentials from local properties file
    CloudOpsConfig config = CloudOpsTestUtils.loadTestConfig();
    if (config == null) {
      throw new IllegalStateException("Real credentials required from local-test.properties file");
    }
    return config;
  }

  private java.util.Map<String, Object> configToOperands(CloudOpsConfig config) {
    java.util.Map<String, Object> operands = new java.util.HashMap<>();

    if (config.azure != null) {
      operands.put("azure.tenantId", config.azure.tenantId);
      operands.put("azure.clientId", config.azure.clientId);
      operands.put("azure.clientSecret", config.azure.clientSecret);
      operands.put("azure.subscriptionIds", String.join(",", config.azure.subscriptionIds));
    }

    if (config.gcp != null) {
      operands.put("gcp.credentialsPath", config.gcp.credentialsPath);
      operands.put("gcp.projectIds", String.join(",", config.gcp.projectIds));
    }

    if (config.aws != null) {
      operands.put("aws.accessKeyId", config.aws.accessKeyId);
      operands.put("aws.secretAccessKey", config.aws.secretAccessKey);
      operands.put("aws.region", config.aws.region);
      operands.put("aws.accountIds", String.join(",", config.aws.accountIds));
      if (config.aws.roleArn != null) {
        operands.put("aws.roleArn", config.aws.roleArn);
      }
    }

    return operands;
  }
}
