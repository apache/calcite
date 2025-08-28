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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for Cloud Governance metadata queries.
 *
 * Tests PostgreSQL-compatible metadata queries using mock credentials.
 */
@Tag("unit")
public class CloudOpsMetadataQueryTest {

  @Test public void testPgTablesQuery() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT schemaname, tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers " +
          "FROM pg_catalog.\"PG_TABLES\" " +
          "WHERE schemaname = 'public' " +
          "ORDER BY tablename");

      int rowCount = 0;
      while (resultSet.next()) {
        rowCount++;
        String schemaName = resultSet.getString("schemaname");
        String tableName = resultSet.getString("tablename");
        String tableOwner = resultSet.getString("tableowner");

        assertNotNull(schemaName);
        assertNotNull(tableName);
        assertNotNull(tableOwner);
        assertThat("Schema should be public", schemaName, is("public"));
        assertThat("Table owner should be cloud_ops_admin", tableOwner, is("cloud_ops_admin"));

        System.out.println("Found table: " + schemaName + "." + tableName + " owned by " + tableOwner);
      }

      assertTrue(rowCount > 0, "Should find at least one table");
      System.out.println("Total tables found: " + rowCount);
    }
  }

  @Test public void testInformationSchemaTablesQuery() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT \"TABLE_CATALOG\", \"TABLE_SCHEMA\", \"TABLE_NAME\", \"TABLE_TYPE\", \"IS_INSERTABLE_INTO\", \"IS_TYPED\", \"COMMIT_ACTION\" " +
          "FROM information_schema.\"TABLES\" " +
          "WHERE \"TABLE_SCHEMA\" = 'public' " +
          "ORDER BY \"TABLE_NAME\"");

      int tableCount = 0;
      while (resultSet.next()) {
        tableCount++;
        String tableCatalog = resultSet.getString("TABLE_CATALOG");
        String tableSchema = resultSet.getString("TABLE_SCHEMA");
        String tableName = resultSet.getString("TABLE_NAME");
        String tableType = resultSet.getString("TABLE_TYPE");

        assertNotNull(tableCatalog);
        assertNotNull(tableSchema);
        assertNotNull(tableName);
        assertNotNull(tableType);
        assertThat("Table schema should be public", tableSchema, is("public"));
        assertThat("Table type should be BASE TABLE", tableType, is("BASE TABLE"));

        System.out.println("Found table: " + tableCatalog + "." + tableSchema + "." + tableName + " (" + tableType + ")");
      }

      assertTrue(tableCount > 0, "Should find at least one table");
      System.out.println("Total tables found: " + tableCount);
    }
  }

  @Test public void testInformationSchemaColumnsQuery() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT \"TABLE_NAME\", \"COLUMN_NAME\", \"ORDINAL_POSITION\", \"DATA_TYPE\", \"IS_NULLABLE\" " +
          "FROM information_schema.\"COLUMNS\" " +
          "WHERE \"TABLE_SCHEMA\" = 'public' AND \"TABLE_NAME\" = 'kubernetes_clusters' " +
          "ORDER BY \"ORDINAL_POSITION\"");

      int columnCount = 0;
      while (resultSet.next()) {
        columnCount++;
        String tableName = resultSet.getString("TABLE_NAME");
        String columnName = resultSet.getString("COLUMN_NAME");
        int ordinalPosition = resultSet.getInt("ORDINAL_POSITION");
        String dataType = resultSet.getString("DATA_TYPE");
        String isNullable = resultSet.getString("IS_NULLABLE");

        assertThat("Table name should be kubernetes_clusters", tableName, is("kubernetes_clusters"));
        assertTrue(columnName != null && !columnName.isEmpty(), "Column name should not be empty");
        assertTrue(ordinalPosition > 0, "Ordinal position should be positive");
        assertTrue(dataType != null && !dataType.isEmpty(), "Data type should not be empty");
        assertTrue("YES".equals(isNullable) || "NO".equals(isNullable), "Is nullable should be YES or NO");

        System.out.println("Column: " + columnName + " (" + dataType + ", nullable: " + isNullable + ")");
      }

      assertTrue(columnCount > 0, "Should find some columns");
      System.out.println("Total columns found: " + columnCount);
    }
  }

  @Test public void testCloudResourcesMetadataQuery() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT table_name, resource_type, supported_providers, column_count, " +
          "       has_security_fields, has_encryption_fields " +
          "FROM pg_catalog.\"CLOUD_RESOURCES\" " +
          "ORDER BY table_name");

      int resourceCount = 0;
      while (resultSet.next()) {
        resourceCount++;
        String tableName = resultSet.getString("table_name");
        String resourceType = resultSet.getString("resource_type");
        String supportedProviders = resultSet.getString("supported_providers");
        int columnCount = resultSet.getInt("column_count");
        boolean hasSecurityFields = resultSet.getBoolean("has_security_fields");
        boolean hasEncryptionFields = resultSet.getBoolean("has_encryption_fields");

        assertTrue(tableName != null && !tableName.isEmpty(), "Table name should not be empty");
        assertTrue(resourceType != null && !resourceType.isEmpty(), "Resource type should not be empty");
        assertThat("Supported providers should be Azure, AWS, GCP", supportedProviders, is("Azure, AWS, GCP"));
        assertTrue(columnCount > 0, "Column count should be positive");

        System.out.println(
            String.format("Resource: %s (%s) - %d columns, security: %s, encryption: %s",
            tableName, resourceType, columnCount, hasSecurityFields, hasEncryptionFields));
      }

      assertTrue(resourceCount > 0, "Should find some cloud resources");
      System.out.println("Total cloud resources found: " + resourceCount);
    }
  }

  @Test public void testCloudProvidersMetadataQuery() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT provider_name, provider_code, supported_services, authentication_methods " +
          "FROM pg_catalog.\"CLOUD_PROVIDERS\" " +
          "ORDER BY provider_code");

      int providerCount = 0;
      while (resultSet.next()) {
        providerCount++;
        String providerName = resultSet.getString("provider_name");
        String providerCode = resultSet.getString("provider_code");
        String supportedServices = resultSet.getString("supported_services");
        String authMethods = resultSet.getString("authentication_methods");

        assertTrue(providerName != null && !providerName.isEmpty(), "Provider name should not be empty");
        assertTrue(providerCode != null && !providerCode.isEmpty(), "Provider code should not be empty");
        assertTrue(supportedServices != null && !supportedServices.isEmpty(), "Supported services should not be empty");
        assertTrue(authMethods != null && !authMethods.isEmpty(), "Auth methods should not be empty");

        System.out.println(
            String.format("Provider: %s (%s) - Services: %s",
            providerName, providerCode, supportedServices));
      }

      assertThat("Should find exactly 3 cloud providers", providerCount, is(3));
    }
  }

  @Test public void testGovernancePoliciesMetadataQuery() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT policy_name, policy_category, applicable_resources, compliance_level, enforcement_level " +
          "FROM pg_catalog.\"OPS_POLICIES\" " +
          "ORDER BY policy_category, policy_name");

      int policyCount = 0;
      while (resultSet.next()) {
        policyCount++;
        String policyName = resultSet.getString("policy_name");
        String policyCategory = resultSet.getString("policy_category");
        String applicableResources = resultSet.getString("applicable_resources");
        String complianceLevel = resultSet.getString("compliance_level");
        String enforcementLevel = resultSet.getString("enforcement_level");

        assertTrue(policyName != null && !policyName.isEmpty(), "Policy name should not be empty");
        assertTrue(policyCategory != null && !policyCategory.isEmpty(), "Policy category should not be empty");
        assertTrue(applicableResources != null && !applicableResources.isEmpty(), "Applicable resources should not be empty");
        assertTrue(complianceLevel != null && !complianceLevel.isEmpty(), "Compliance level should not be empty");
        assertTrue(enforcementLevel != null && !enforcementLevel.isEmpty(), "Enforcement level should not be empty");

        System.out.println(
            String.format("Policy: %s (%s) - %s compliance, %s enforcement",
            policyName, policyCategory, complianceLevel, enforcementLevel));
      }

      assertTrue(policyCount > 0, "Should find some ops policies");
      System.out.println("Total ops policies found: " + policyCount);
    }
  }

  @Test public void testCrossSchemaMetadataQuery() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT t.\"TABLE_NAME\", cr.resource_type, cr.has_security_fields, cr.column_count " +
          "FROM information_schema.\"TABLES\" t " +
          "JOIN pg_catalog.\"CLOUD_RESOURCES\" cr ON t.\"TABLE_NAME\" = cr.table_name " +
          "WHERE t.\"TABLE_SCHEMA\" = 'public' " +
          "ORDER BY t.\"TABLE_NAME\"");

      int joinCount = 0;
      while (resultSet.next()) {
        joinCount++;
        String tableName = resultSet.getString("TABLE_NAME");
        String resourceType = resultSet.getString("resource_type");
        boolean hasSecurityFields = resultSet.getBoolean("has_security_fields");
        int columnCount = resultSet.getInt("column_count");

        assertTrue(tableName != null && !tableName.isEmpty(), "Table name should not be empty");
        assertTrue(columnCount > 0, "Column count should be positive");
        assertTrue(resourceType != null && !resourceType.isEmpty(), "Resource type should not be empty");

        System.out.println(
            String.format("Table: %s (%s) - %d columns, security fields: %s",
            tableName, resourceType, columnCount, hasSecurityFields));
      }

      assertTrue(joinCount > 0, "Should find some joined results");
      System.out.println("Total joined results: " + joinCount);
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
    rootSchema.add(
        "public", factory.create(rootSchema, "public",
        configToOperands(config)));

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
