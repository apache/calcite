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
package org.apache.calcite.adapter.governance;

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
 * Integration tests for Cloud Governance adapter.
 *
 * Requires local-test.properties file with actual cloud credentials.
 * Skip tests if properties file is not available.
 */
@Tag("integration")
public class CloudGovernanceIntegrationTest {

  private Properties testProperties;
  private boolean hasValidCredentials = false;

  @BeforeEach
  public void setUp() {
    testProperties = loadTestProperties();
    hasValidCredentials = validateCredentials();
  }

  @Test public void testAzureKubernetesClustersQuery() throws SQLException {
    Assumptions.assumeTrue(hasValidCredentials && hasAzureCredentials(),
        "Azure credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT cloud_provider, cluster_name, application, rbac_enabled " +
          "FROM kubernetes_clusters " +
          "WHERE cloud_provider = 'azure' " +
          "LIMIT 10");

      int rowCount = 0;
      while (resultSet.next()) {
        rowCount++;
        assertThat(resultSet.getString("cloud_provider"), is("azure"));
        // Verify that cluster_name is not null/empty
        assertTrue(resultSet.getString("cluster_name") != null);
        assertTrue(resultSet.getString("cluster_name").length() > 0);
      }

      // Should have some results if Azure is properly configured
      System.out.println("Azure Kubernetes clusters found: " + rowCount);
    }
  }

  @Test public void testMultiCloudStorageQuery() throws SQLException {
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT cloud_provider, COUNT(*) as resource_count " +
          "FROM storage_resources " +
          "GROUP BY cloud_provider " +
          "ORDER BY cloud_provider");

      int providerCount = 0;
      while (resultSet.next()) {
        providerCount++;
        String provider = resultSet.getString("cloud_provider");
        int count = resultSet.getInt("resource_count");

        System.out.println(provider + " storage resources: " + count);
        assertTrue(Arrays.asList("azure", "aws", "gcp").contains(provider),
            "Should have valid provider name");
        assertTrue(count >= 0, "Should have non-negative count");
      }

      System.out.println("Total cloud providers with storage: " + providerCount);
    }
  }

  @Test public void testApplicationTaggingQuery() throws SQLException {
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT application, COUNT(*) as resource_count " +
          "FROM storage_resources " +
          "GROUP BY application " +
          "ORDER BY resource_count DESC " +
          "LIMIT 10");

      int applicationCount = 0;
      while (resultSet.next()) {
        applicationCount++;
        String application = resultSet.getString("application");
        int count = resultSet.getInt("resource_count");

        System.out.println("Application '" + application + "': " + count + " storage resources");
        assertTrue(application != null, "Should have valid application name");
        assertTrue(count > 0, "Should have positive count");
      }

      System.out.println("Total applications found: " + applicationCount);
    }
  }

  @Test public void testSecurityComplianceQuery() throws SQLException {
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT cloud_provider, " +
          "       COUNT(*) as total_clusters, " +
          "       SUM(CASE WHEN rbac_enabled = true THEN 1 ELSE 0 END) as rbac_enabled_count, " +
          "       SUM(CASE WHEN private_cluster = true THEN 1 ELSE 0 END) as private_cluster_count " +
          "FROM kubernetes_clusters " +
          "GROUP BY cloud_provider");

      while (resultSet.next()) {
        String provider = resultSet.getString("cloud_provider");
        int total = resultSet.getInt("total_clusters");
        int rbacEnabled = resultSet.getInt("rbac_enabled_count");
        int privateClusters = resultSet.getInt("private_cluster_count");

        System.out.println(
            String.format("%s: %d clusters, %d with RBAC, %d private",
            provider, total, rbacEnabled, privateClusters));

        assertTrue(Arrays.asList("azure", "aws", "gcp").contains(provider),
            "Should have valid provider");
        assertTrue(rbacEnabled <= total, "RBAC count should not exceed total");
        assertTrue(privateClusters <= total, "Private count should not exceed total");
      }
    }
  }

  @Test public void testCrossProviderJoin() throws SQLException {
    Assumptions.assumeTrue(hasValidCredentials, "Cloud credentials not available");

    try (Connection connection = createConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT k.cloud_provider, k.application, " +
          "       COUNT(k.cluster_name) as cluster_count, " +
          "       COUNT(s.resource_name) as storage_count " +
          "FROM kubernetes_clusters k " +
          "LEFT JOIN storage_resources s ON k.application = s.application " +
          "                              AND k.cloud_provider = s.cloud_provider " +
          "GROUP BY k.cloud_provider, k.application " +
          "ORDER BY cluster_count DESC " +
          "LIMIT 5");

      while (resultSet.next()) {
        String provider = resultSet.getString("cloud_provider");
        String application = resultSet.getString("application");
        int clusterCount = resultSet.getInt("cluster_count");
        int storageCount = resultSet.getInt("storage_count");

        System.out.println(
            String.format("%s/%s: %d clusters, %d storage resources",
            provider, application, clusterCount, storageCount));
      }
    }
  }

  private Connection createConnection() throws SQLException {
    CloudGovernanceConfig config = createConfigFromProperties();

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    CloudGovernanceSchemaFactory factory = new CloudGovernanceSchemaFactory();
    rootSchema.add(
        "cloud_governance", factory.create(rootSchema, "cloud_governance",
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

  private CloudGovernanceConfig createConfigFromProperties() {
    CloudGovernanceConfig.AzureConfig azure = null;
    CloudGovernanceConfig.GCPConfig gcp = null;
    CloudGovernanceConfig.AWSConfig aws = null;

    // Azure configuration
    if (hasAzureCredentials()) {
      azure =
          new CloudGovernanceConfig.AzureConfig(testProperties.getProperty("azure.tenantId"),
          testProperties.getProperty("azure.clientId"),
          testProperties.getProperty("azure.clientSecret"),
          Arrays.asList(testProperties.getProperty("azure.subscriptionIds").split(",")));
    }

    // GCP configuration
    if (hasGCPCredentials()) {
      gcp =
          new CloudGovernanceConfig.GCPConfig(Arrays.asList(testProperties.getProperty("gcp.projectIds").split(",")),
          testProperties.getProperty("gcp.credentialsPath"));
    }

    // AWS configuration
    if (hasAWSCredentials()) {
      aws =
          new CloudGovernanceConfig.AWSConfig(Arrays.asList(testProperties.getProperty("aws.accountIds").split(",")),
          testProperties.getProperty("aws.region", "us-east-1"),
          testProperties.getProperty("aws.accessKeyId"),
          testProperties.getProperty("aws.secretAccessKey"),
          testProperties.getProperty("aws.roleArn"));
    }

    return new CloudGovernanceConfig(null, azure, gcp, aws, true, 15);
  }

  private java.util.Map<String, Object> configToOperands(CloudGovernanceConfig config) {
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
