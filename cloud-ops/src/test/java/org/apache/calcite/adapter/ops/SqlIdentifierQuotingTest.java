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
import java.util.Arrays;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests verifying SQL identifier quoting rules for Cloud Governance metadata.
 *
 * SQL Identifier Quoting Rules:
 * 1. information_schema uses UPPERCASE identifiers that must be quoted
 * 2. pg_catalog uses lowercase identifiers that don't need quotes
 * 3. Custom cloud ops metadata follows PostgreSQL conventions
 */
@Tag("unit")
public class SqlIdentifierQuotingTest {

  @Test public void testInformationSchemaUppercaseQuotedIdentifiers() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test quoted UPPERCASE identifiers for information_schema - this should work
      ResultSet resultSet =
          statement.executeQuery("SELECT \"TABLE_CATALOG\", \"TABLE_SCHEMA\", \"TABLE_NAME\" " +
          "FROM information_schema.\"TABLES\" " +
          "WHERE \"TABLE_SCHEMA\" = 'public' " +
          "ORDER BY \"TABLE_NAME\"");

      int tableCount = 0;
      while (resultSet.next()) {
        tableCount++;
        String tableCatalog = resultSet.getString("TABLE_CATALOG");
        String tableSchema = resultSet.getString("TABLE_SCHEMA");
        String tableName = resultSet.getString("TABLE_NAME");

        assertNotNull(tableCatalog, "Table catalog should not be null");
        assertNotNull(tableSchema, "Table schema should not be null");
        assertNotNull(tableName, "Table name should not be null");
        assertThat("Table schema should be public", tableSchema, is("public"));

        System.out.println("Found table: " + tableCatalog + "." + tableSchema + "." + tableName);
      }

      assertTrue(tableCount > 0, "Should find at least one table with quoted identifiers");
    }
  }

  @Test public void testPgCatalogLowercaseUnquotedIdentifiers() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test unquoted lowercase identifiers for pg_catalog - this should work
      ResultSet resultSet =
          statement.executeQuery("SELECT schemaname, tablename, tableowner " +
          "FROM pg_catalog.pg_tables " +
          "WHERE schemaname = 'public' " +
          "ORDER BY tablename");

      int tableCount = 0;
      while (resultSet.next()) {
        tableCount++;
        String schemaName = resultSet.getString("schemaname");
        String tableName = resultSet.getString("tablename");
        String tableOwner = resultSet.getString("tableowner");

        assertNotNull(schemaName, "Schema name should not be null");
        assertNotNull(tableName, "Table name should not be null");
        assertNotNull(tableOwner, "Table owner should not be null");
        assertThat("Schema should be public", schemaName, is("public"));

        System.out.println("Found table: " + schemaName + "." + tableName + " owned by " + tableOwner);
      }

      assertTrue(tableCount > 0, "Should find at least one table with unquoted identifiers");
    }
  }

  @Test public void testInformationSchemaColumnsWithQuotedIdentifiers() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test information_schema.COLUMNS with proper quoting
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

        System.out.println("Column " + ordinalPosition + ": " + columnName + " (" + dataType + ", nullable: " + isNullable + ")");
      }

      assertTrue(columnCount > 0, "Should find columns for kubernetes_clusters table");
    }
  }

  @Test public void testPgAttributeWithUnquotedIdentifiers() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test pg_attribute with unquoted lowercase identifiers
      ResultSet resultSet =
          statement.executeQuery("SELECT attname, atttypid, attnum, attnotnull " +
          "FROM pg_catalog.pg_attribute " +
          "WHERE attnum > 0 " +
          "ORDER BY attrelid, attnum " +
          "LIMIT 10");

      int attributeCount = 0;
      while (resultSet.next()) {
        attributeCount++;
        String attname = resultSet.getString("attname");
        int atttypid = resultSet.getInt("atttypid");
        short attnum = resultSet.getShort("attnum");
        boolean attnotnull = resultSet.getBoolean("attnotnull");

        assertNotNull(attname, "Attribute name should not be null");
        assertTrue(atttypid > 0, "Type ID should be positive");
        assertTrue(attnum > 0, "Attribute number should be positive");

        System.out.println("Attribute: " + attname + " (type=" + atttypid + ", num=" + attnum + ", notnull=" + attnotnull + ")");
      }

      assertTrue(attributeCount > 0, "Should find attributes in pg_attribute");
    }
  }

  @Test public void testCustomCloudOpsMetadataUnquoted() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test custom cloud ops metadata tables with unquoted identifiers
      ResultSet resultSet =
          statement.executeQuery("SELECT table_name, resource_type, supported_providers " +
          "FROM pg_catalog.cloud_resources " +
          "ORDER BY table_name");

      int resourceCount = 0;
      while (resultSet.next()) {
        resourceCount++;
        String tableName = resultSet.getString("table_name");
        String resourceType = resultSet.getString("resource_type");
        String supportedProviders = resultSet.getString("supported_providers");

        assertTrue(tableName != null && !tableName.isEmpty(), "Table name should not be empty");
        assertTrue(resourceType != null && !resourceType.isEmpty(), "Resource type should not be empty");
        assertThat("Supported providers should be Azure, AWS, GCP", supportedProviders, is("Azure, AWS, GCP"));

        System.out.println("Cloud resource: " + tableName + " (" + resourceType + ")");
      }

      assertTrue(resourceCount > 0, "Should find cloud resources metadata");
    }
  }

  @Test public void testCrossSchemaJoinWithProperQuoting() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test cross-schema join with proper identifier quoting
      ResultSet resultSet =
          statement.executeQuery("SELECT t.\"TABLE_NAME\", pg.tablename, cr.resource_type " +
          "FROM information_schema.\"TABLES\" t " +
          "JOIN pg_catalog.pg_tables pg ON t.\"TABLE_NAME\" = pg.tablename " +
          "JOIN pg_catalog.cloud_resources cr ON t.\"TABLE_NAME\" = cr.table_name " +
          "WHERE t.\"TABLE_SCHEMA\" = 'public' AND pg.schemaname = 'public' " +
          "ORDER BY t.\"TABLE_NAME\" " +
          "LIMIT 5");

      int joinCount = 0;
      while (resultSet.next()) {
        joinCount++;
        String infoTableName = resultSet.getString("TABLE_NAME");
        String pgTableName = resultSet.getString("tablename");
        String resourceType = resultSet.getString("resource_type");

        assertThat("Table names should match across schemas", infoTableName, is(pgTableName));
        assertTrue(resourceType != null && !resourceType.isEmpty(), "Resource type should not be empty");

        System.out.println("Joined result: " + infoTableName + " -> " + resourceType);
      }

      assertTrue(joinCount > 0, "Should find joined results across schemas");
    }
  }

  @Test public void testGovernancePoliciesMetadata() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test ops policies metadata with unquoted identifiers
      ResultSet resultSet =
          statement.executeQuery("SELECT policy_name, policy_category, compliance_level, enforcement_level " +
          "FROM pg_catalog.ops_policies " +
          "WHERE policy_category = 'Security' " +
          "ORDER BY policy_name");

      int policyCount = 0;
      while (resultSet.next()) {
        policyCount++;
        String policyName = resultSet.getString("policy_name");
        String policyCategory = resultSet.getString("policy_category");
        String complianceLevel = resultSet.getString("compliance_level");
        String enforcementLevel = resultSet.getString("enforcement_level");

        assertTrue(policyName != null && !policyName.isEmpty(), "Policy name should not be empty");
        assertThat("Policy category should be Security", policyCategory, is("Security"));
        assertTrue(complianceLevel != null && !complianceLevel.isEmpty(), "Compliance level should not be empty");
        assertTrue(enforcementLevel != null && !enforcementLevel.isEmpty(), "Enforcement level should not be empty");

        System.out.println("Security policy: " + policyName + " (" + complianceLevel + " compliance, " + enforcementLevel + " enforcement)");
      }

      assertTrue(policyCount > 0, "Should find security policies");
    }
  }

  @Test public void testCloudProvidersMetadata() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();

      // Test cloud providers metadata
      ResultSet resultSet =
          statement.executeQuery("SELECT provider_name, provider_code, supported_services " +
          "FROM pg_catalog.cloud_providers " +
          "ORDER BY provider_code");

      int providerCount = 0;
      String[] expectedProviders = {"aws", "azure", "gcp"};

      while (resultSet.next()) {
        String providerName = resultSet.getString("provider_name");
        String providerCode = resultSet.getString("provider_code");
        String supportedServices = resultSet.getString("supported_services");

        assertTrue(providerName != null && !providerName.isEmpty(), "Provider name should not be empty");
        assertTrue(providerCode != null && !providerCode.isEmpty(), "Provider code should not be empty");
        assertTrue(supportedServices != null && !supportedServices.isEmpty(), "Supported services should not be empty");

        // Verify provider code is one of the expected values
        boolean foundExpected = false;
        for (String expected : expectedProviders) {
          if (expected.equals(providerCode)) {
            foundExpected = true;
            break;
          }
        }
        assertTrue(foundExpected, "Provider code should be one of: aws, azure, gcp");

        System.out.println("Cloud provider: " + providerName + " (" + providerCode + ") - " + supportedServices);
        providerCount++;
      }

      assertThat("Should find exactly 3 cloud providers", providerCount, is(3));
    }
  }

  private Connection createTestConnection() throws SQLException {
    CloudOpsConfig config = createTestConfig();

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");

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
    CloudOpsConfig.AzureConfig azure =
        new CloudOpsConfig.AzureConfig("test-tenant", "test-client", "test-secret", Arrays.asList("sub1", "sub2"));

    CloudOpsConfig.GCPConfig gcp =
        new CloudOpsConfig.GCPConfig(Arrays.asList("project1", "project2"), "/path/to/credentials.json");

    CloudOpsConfig.AWSConfig aws =
        new CloudOpsConfig.AWSConfig(Arrays.asList("account1", "account2"), "us-east-1", "test-key", "test-secret", null);

    return new CloudOpsConfig(
        Arrays.asList("azure", "gcp", "aws"), azure, gcp, aws, true, 15);
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
