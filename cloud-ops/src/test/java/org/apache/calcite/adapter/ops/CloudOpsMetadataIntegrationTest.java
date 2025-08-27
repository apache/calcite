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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for Cloud Governance metadata discovery.
 *
 * Tests PostgreSQL-compatible metadata queries using real connections.
 */
@Tag("integration")
public class CloudOpsMetadataIntegrationTest {

  private Properties testProperties;
  private boolean hasValidCredentials = false;

  @BeforeEach
  public void setUp() {
    testProperties = loadTestProperties();
    hasValidCredentials = validateCredentials();
  }

  @Test public void testPgTablesQuery() throws SQLException {
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
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

        assertThat("Schema should be public", schemaName, is("public"));
        assertTrue(tableName != null && !tableName.isEmpty(), "Table name should not be empty");
        assertThat("Table owner should be cloud_ops_admin", tableOwner, is("cloud_ops_admin"));

        System.out.println("Found table: " + schemaName + "." + tableName + " (owner: " + tableOwner + ")");
      }

      assertTrue(tableCount > 0, "Should find some tables");
      System.out.println("Total tables found: " + tableCount);
    }
  }

  @Test public void testInformationSchemaTablesQuery() throws SQLException {
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT table_catalog, table_schema, table_name, table_type " +
          "FROM information_schema.tables " +
          "WHERE table_schema = 'public' " +
          "ORDER BY table_name");

      int tableCount = 0;
      while (resultSet.next()) {
        tableCount++;
        String tableCatalog = resultSet.getString("table_catalog");
        String tableSchema = resultSet.getString("table_schema");
        String tableName = resultSet.getString("table_name");
        String tableType = resultSet.getString("table_type");

        assertTrue(tableCatalog != null && !tableCatalog.isEmpty(), "Table catalog should not be empty");
        assertThat("Table schema should be public", tableSchema, is("public"));
        assertTrue(tableName != null && !tableName.isEmpty(), "Table name should not be empty");
        assertThat("Table type should be BASE TABLE", tableType, is("BASE TABLE"));

        System.out.println("Found table: " + tableCatalog + "." + tableSchema + "." + tableName + " (" + tableType + ")");
      }

      assertTrue(tableCount > 0, "Should find some tables");
      System.out.println("Total tables found: " + tableCount);
    }
  }

  @Test public void testInformationSchemaColumnsQuery() throws SQLException {
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT table_name, column_name, ordinal_position, data_type, is_nullable " +
          "FROM information_schema.columns " +
          "WHERE table_schema = 'public' AND table_name = 'kubernetes_clusters' " +
          "ORDER BY ordinal_position");

      int columnCount = 0;
      while (resultSet.next()) {
        columnCount++;
        String tableName = resultSet.getString("table_name");
        String columnName = resultSet.getString("column_name");
        int ordinalPosition = resultSet.getInt("ordinal_position");
        String dataType = resultSet.getString("data_type");
        String isNullable = resultSet.getString("is_nullable");

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
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT table_name, resource_type, supported_providers, column_count, " +
          "       has_security_fields, has_encryption_fields " +
          "FROM pg_catalog.cloud_resources " +
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
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT provider_name, provider_code, supported_services, authentication_methods " +
          "FROM pg_catalog.cloud_providers " +
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
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT policy_name, policy_category, applicable_resources, compliance_level, enforcement_level " +
          "FROM pg_catalog.ops_policies " +
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
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT t.table_name, COUNT(c.column_name) as column_count, " +
          "       cr.resource_type, cr.has_security_fields " +
          "FROM information_schema.tables t " +
          "JOIN information_schema.columns c ON t.table_name = c.table_name " +
          "JOIN pg_catalog.cloud_resources cr ON t.table_name = cr.table_name " +
          "WHERE t.table_schema = 'public' " +
          "GROUP BY t.table_name, cr.resource_type, cr.has_security_fields " +
          "ORDER BY t.table_name");

      int joinCount = 0;
      while (resultSet.next()) {
        joinCount++;
        String tableName = resultSet.getString("table_name");
        int columnCount = resultSet.getInt("column_count");
        String resourceType = resultSet.getString("resource_type");
        boolean hasSecurityFields = resultSet.getBoolean("has_security_fields");

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

  private Connection createConnection() throws SQLException {
    CloudOpsConfig config = createConfigFromProperties();

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    CloudOpsSchemaFactory factory = new CloudOpsSchemaFactory();
    rootSchema.add(
        "cloud_ops", factory.create(rootSchema, "cloud_ops",
        configToOperands(config)));

    return connection;
  }

  private Properties loadTestProperties() {
    Properties props = new Properties();
    try (InputStream is = getClass().getClassLoader()
        .getResourceAsStream("local-test.properties")) {
      if (is != null) {
        props.load(is);
      }
    } catch (IOException e) {
      System.out.println("Warning: Could not load local-test.properties: " + e.getMessage());
    }
    return props;
  }

  private boolean validateCredentials() {
    return hasAzureCredentials() || hasGCPCredentials() || hasAWSCredentials();
  }

  private boolean hasAzureCredentials() {
    return testProperties.getProperty("azure.tenantId") != null &&
           testProperties.getProperty("azure.clientId") != null &&
           testProperties.getProperty("azure.clientSecret") != null &&
           testProperties.getProperty("azure.subscriptionIds") != null;
  }

  private boolean hasGCPCredentials() {
    return testProperties.getProperty("gcp.credentialsPath") != null &&
           testProperties.getProperty("gcp.projectIds") != null;
  }

  private boolean hasAWSCredentials() {
    return testProperties.getProperty("aws.accessKeyId") != null &&
           testProperties.getProperty("aws.secretAccessKey") != null &&
           testProperties.getProperty("aws.accountIds") != null;
  }

  private CloudOpsConfig createConfigFromProperties() {
    CloudOpsConfig.AzureConfig azure = null;
    CloudOpsConfig.GCPConfig gcp = null;
    CloudOpsConfig.AWSConfig aws = null;

    // Azure configuration
    if (hasAzureCredentials()) {
      azure =
          new CloudOpsConfig.AzureConfig(testProperties.getProperty("azure.tenantId"),
          testProperties.getProperty("azure.clientId"),
          testProperties.getProperty("azure.clientSecret"),
          Arrays.asList(testProperties.getProperty("azure.subscriptionIds").split(",")));
    }

    // GCP configuration
    if (hasGCPCredentials()) {
      gcp =
          new CloudOpsConfig.GCPConfig(Arrays.asList(testProperties.getProperty("gcp.projectIds").split(",")),
          testProperties.getProperty("gcp.credentialsPath"));
    }

    // AWS configuration
    if (hasAWSCredentials()) {
      aws =
          new CloudOpsConfig.AWSConfig(Arrays.asList(testProperties.getProperty("aws.accountIds").split(",")),
          testProperties.getProperty("aws.region", "us-east-1"),
          testProperties.getProperty("aws.accessKeyId"),
          testProperties.getProperty("aws.secretAccessKey"),
          testProperties.getProperty("aws.roleArn"));
    }

    return new CloudOpsConfig(null, azure, gcp, aws, true, 15, false);
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
