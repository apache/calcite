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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Live integration tests for sort pushdown optimization.
 * Tests sort pushdown across Azure, AWS, and GCP providers using real cloud credentials.
 * 
 * To run these tests:
 * 1. Configure credentials in src/test/resources/local-test.properties
 * 2. Run with system property: -Dcloud.ops.live.tests=true
 * 
 * Tests demonstrate:
 * - Azure KQL ORDER BY server-side sort pushdown
 * - AWS client-side sorting fallback  
 * - GCP orderBy parameter optimization
 * - Multi-field sorting and direction support
 * - Sort optimization metrics logging
 */
public class SortPushdownIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(SortPushdownIntegrationTest.class);
  
  private static Connection connection;
  private static CloudOpsConfig config;
  
  @BeforeAll
  static void setUp() throws Exception {
    // Load test configuration
    config = CloudOpsTestUtils.loadTestConfig();
    if (config == null) {
      throw new IllegalStateException("Real credentials required from local-test.properties file");
    }
    
    // Set up Calcite connection with cloud-ops schema
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    connection = DriverManager.getConnection("jdbc:calcite:", info);
    org.apache.calcite.jdbc.CalciteConnection calciteConnection = connection.unwrap(org.apache.calcite.jdbc.CalciteConnection.class);
    org.apache.calcite.schema.SchemaPlus rootSchema = calciteConnection.getRootSchema();

    CloudOpsSchemaFactory factory = new CloudOpsSchemaFactory();
    rootSchema.add("cloud_ops", factory.create(rootSchema, "cloud_ops", configToOperands(config)));

    // Set cloud_ops as the default schema
    calciteConnection.setSchema("cloud_ops");
    
    logger.info("Connected to Calcite with cloud ops sort pushdown support");
  }
  
  @Test
  void testSingleFieldSortPushdown() throws Exception {
    
    logger.info("=== Testing Single Field Sort Pushdown ===");
    
    String sql = "SELECT cloud_provider, cluster_name, region " +
                 "FROM kubernetes_clusters " +
                 "ORDER BY cluster_name ASC " +
                 "LIMIT 10";
    
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      
      logger.info("Query: {}", sql);
      logger.info("Sort pushdown optimization should be visible in debug logs");
      
      int rowCount = 0;
      String previousClusterName = null;
      
      while (rs.next() && rowCount < 10) {
        String provider = rs.getString("cloud_provider");
        String clusterName = rs.getString("cluster_name");
        String region = rs.getString("region");
        
        logger.info("Row {}: {} - {} ({})", rowCount + 1, provider, clusterName, region);
        
        // Verify sort order (should be ascending)
        if (previousClusterName != null && clusterName != null) {
          if (clusterName.compareTo(previousClusterName) < 0) {
            logger.warn("Sort order violation: {} should come after {}", clusterName, previousClusterName);
          }
        }
        previousClusterName = clusterName;
        rowCount++;
      }
      
      logger.info("Retrieved {} sorted results", rowCount);
    }
  }
  
  @Test
  void testMultiFieldSortPushdown() throws Exception {
    
    logger.info("=== Testing Multi-Field Sort Pushdown ===");
    
    String sql = "SELECT cloud_provider, cluster_name, region, application " +
                 "FROM kubernetes_clusters " +
                 "ORDER BY cloud_provider ASC, region DESC, cluster_name ASC " +
                 "LIMIT 15";
    
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      
      logger.info("Query: {}", sql);
      logger.info("Multi-field sort pushdown optimization should be visible in debug logs");
      
      int rowCount = 0;
      String previousProvider = null;
      String previousRegion = null;
      String previousClusterName = null;
      
      while (rs.next() && rowCount < 15) {
        String provider = rs.getString("cloud_provider");
        String clusterName = rs.getString("cluster_name");
        String region = rs.getString("region");
        String application = rs.getString("application");
        
        logger.info("Row {}: {} | {} | {} | {}", rowCount + 1, provider, region, clusterName, application);
        
        // Verify multi-field sort order
        if (previousProvider != null && provider != null) {
          int providerCompare = provider.compareTo(previousProvider);
          if (providerCompare < 0) {
            logger.warn("Primary sort violation: provider {} should not come before {}", 
                       provider, previousProvider);
          } else if (providerCompare == 0 && previousRegion != null && region != null) {
            // Same provider, check region (DESC)
            int regionCompare = region.compareTo(previousRegion);
            if (regionCompare > 0) {
              logger.warn("Secondary sort violation: region {} should come before {} (DESC order)", 
                         region, previousRegion);
            }
          }
        }
        
        previousProvider = provider;
        previousRegion = region;
        previousClusterName = clusterName;
        rowCount++;
      }
      
      logger.info("Retrieved {} multi-field sorted results", rowCount);
    }
  }
  
  @Test
  void testSortWithProjectionPushdown() throws Exception {
    
    logger.info("=== Testing Combined Sort + Projection Pushdown ===");
    
    String sql = "SELECT cluster_name, kubernetes_version " +
                 "FROM kubernetes_clusters " +
                 "ORDER BY kubernetes_version DESC, cluster_name ASC " +
                 "LIMIT 8";
    
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      
      logger.info("Query: {}", sql);
      logger.info("Combined sort + projection pushdown should show optimal optimization");
      
      int rowCount = 0;
      while (rs.next() && rowCount < 8) {
        String clusterName = rs.getString("cluster_name");
        String kubernetesVersion = rs.getString("kubernetes_version");
        
        logger.info("Row {}: {} (v{})", rowCount + 1, clusterName, kubernetesVersion);
        rowCount++;
      }
      
      logger.info("Retrieved {} results with combined optimization", rowCount);
    }
  }
  
  @Test
  void testProviderSpecificSortOptimization() throws Exception {
    
    logger.info("=== Testing Provider-Specific Sort Optimization ===");
    
    // Test Azure-specific sort (should use KQL ORDER BY)
    if (config.providers.contains("azure")) {
      logger.info("--- Azure KQL Sort Pushdown ---");
      String azureQuery = "SELECT cluster_name, region, application " +
                          "FROM kubernetes_clusters " +
                          "WHERE cloud_provider = 'azure' " +
                          "ORDER BY application ASC, cluster_name DESC " +
                          "LIMIT 5";
      
      try (PreparedStatement stmt = connection.prepareStatement(azureQuery);
           ResultSet rs = stmt.executeQuery()) {
        
        logger.info("Azure Query: {}", azureQuery);
        int count = 0;
        while (rs.next() && count < 5) {
          logger.info("Azure Result {}: {} | {} | {}", 
                     count + 1, rs.getString("cluster_name"), 
                     rs.getString("region"), rs.getString("application"));
          count++;
        }
      }
    }
    
    // Test AWS-specific sort (should use client-side sorting)
    if (config.providers.contains("aws")) {
      logger.info("--- AWS Client-Side Sort ---");
      String awsQuery = "SELECT cluster_name, region " +
                        "FROM kubernetes_clusters " +
                        "WHERE cloud_provider = 'aws' " +
                        "ORDER BY region ASC, cluster_name DESC " +
                        "LIMIT 5";
      
      try (PreparedStatement stmt = connection.prepareStatement(awsQuery);
           ResultSet rs = stmt.executeQuery()) {
        
        logger.info("AWS Query: {}", awsQuery);
        int count = 0;
        while (rs.next() && count < 5) {
          logger.info("AWS Result {}: {} | {}", 
                     count + 1, rs.getString("cluster_name"), rs.getString("region"));
          count++;
        }
      }
    }
    
    // Test GCP-specific sort (should attempt orderBy parameter)
    if (config.providers.contains("gcp")) {
      logger.info("--- GCP OrderBy Parameter ---");
      String gcpQuery = "SELECT cluster_name, region " +
                        "FROM kubernetes_clusters " +
                        "WHERE cloud_provider = 'gcp' " +
                        "ORDER BY cluster_name ASC " +
                        "LIMIT 5";
      
      try (PreparedStatement stmt = connection.prepareStatement(gcpQuery);
           ResultSet rs = stmt.executeQuery()) {
        
        logger.info("GCP Query: {}", gcpQuery);
        int count = 0;
        while (rs.next() && count < 5) {
          logger.info("GCP Result {}: {} | {}", 
                     count + 1, rs.getString("cluster_name"), rs.getString("region"));
          count++;
        }
      }
    }
  }
  
  @Test
  void testSortOptimizationMetrics() throws Exception {
    
    logger.info("=== Testing Sort Optimization Metrics ===");
    
    String sql = "SELECT cloud_provider, cluster_name, region, application, kubernetes_version " +
                 "FROM kubernetes_clusters " +
                 "ORDER BY cloud_provider ASC, application ASC, cluster_name DESC " +
                 "LIMIT 12";
    
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      
      logger.info("Query: {}", sql);
      logger.info("Check debug logs for sort optimization metrics:");
      logger.info("- Azure: Should show 'Sort: N fields, Server-side pushdown'");
      logger.info("- AWS: Should show 'Sort: N fields, Client-side sorting'");
      logger.info("- GCP: Should show orderBy parameter or fallback strategy");
      
      int rowCount = 0;
      while (rs.next() && rowCount < 12) {
        String provider = rs.getString("cloud_provider");
        String clusterName = rs.getString("cluster_name");
        String region = rs.getString("region");
        String application = rs.getString("application");
        String version = rs.getString("kubernetes_version");
        
        logger.info("Result {}: {} | {} | {} | {} | v{}", 
                   rowCount + 1, provider, application, clusterName, region, version);
        rowCount++;
      }
      
      logger.info("Total results with sort optimization metrics: {}", rowCount);
    }
  }

  private static Map<String, Object> configToOperands(CloudOpsConfig config) {
    Map<String, Object> operands = new HashMap<>();

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