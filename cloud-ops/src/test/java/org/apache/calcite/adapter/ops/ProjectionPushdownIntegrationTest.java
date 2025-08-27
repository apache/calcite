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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for projection pushdown functionality.
 * These tests use real cloud credentials from local-test.properties.
 * 
 * Run with: ./gradlew :cloud-ops:integrationTest
 */
@Tag("integration")
public class ProjectionPushdownIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionPushdownIntegrationTest.class);
  
  private Connection connection;
  private CloudOpsConfig config;
  
  @BeforeEach
  void setUp() throws Exception {
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
    
    LOGGER.info("Integration test setup complete with providers: {}", 
                String.join(", ", config.providers));
  }
  
  @Test
  void testSingleColumnProjection() throws SQLException {
    // Test projecting only cloud_provider column
    String sql = "SELECT cloud_provider FROM cloud_ops.kubernetes_clusters LIMIT 10";
    
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      
      // Verify result set metadata - should only have 1 column
      ResultSetMetaData metadata = rs.getMetaData();
      assertEquals(1, metadata.getColumnCount(), 
                   "Should project only 1 column (cloud_provider)");
      assertEquals("cloud_provider", metadata.getColumnName(1).toLowerCase());
      
      // Verify we get results and they contain valid cloud provider values
      boolean hasResults = false;
      while (rs.next()) {
        hasResults = true;
        String provider = rs.getString(1);
        assertNotNull(provider, "Cloud provider should not be null");
        assertTrue(provider.matches("azure|aws|gcp"), 
                   "Cloud provider should be azure, aws, or gcp: " + provider);
      }
      
      if (hasResults) {
        LOGGER.info("Single column projection test passed - received data with proper projection");
      } else {
        LOGGER.warn("Single column projection test: No data returned from cloud providers");
      }
    }
  }
  
  @Test
  void testMultiColumnProjection() throws SQLException {
    // Test projecting multiple specific columns
    String sql = "SELECT cloud_provider, cluster_name, region " +
                 "FROM cloud_ops.kubernetes_clusters LIMIT 5";
    
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      
      // Verify result set metadata - should have 3 columns
      ResultSetMetaData metadata = rs.getMetaData();
      assertEquals(3, metadata.getColumnCount(), 
                   "Should project exactly 3 columns");
      assertEquals("cloud_provider", metadata.getColumnName(1).toLowerCase());
      assertEquals("cluster_name", metadata.getColumnName(2).toLowerCase());  
      assertEquals("region", metadata.getColumnName(3).toLowerCase());
      
      // Verify data structure
      boolean hasResults = false;
      while (rs.next()) {
        hasResults = true;
        String provider = rs.getString("cloud_provider");
        String clusterName = rs.getString("cluster_name");
        String region = rs.getString("region");
        
        assertNotNull(provider, "Cloud provider should not be null");
        // Note: cluster_name and region might be null for some providers during testing
        LOGGER.debug("Projected data: provider={}, cluster={}, region={}", 
                     provider, clusterName, region);
      }
      
      if (hasResults) {
        LOGGER.info("Multi-column projection test passed - received {} columns with data", 
                    metadata.getColumnCount());
      } else {
        LOGGER.warn("Multi-column projection test: No data returned from cloud providers");
      }
    }
  }
  
  @Test
  void testSelectAllProjection() throws SQLException {
    // Test SELECT * (no projection pushdown)
    String sql = "SELECT * FROM cloud_ops.kubernetes_clusters LIMIT 3";
    
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      
      ResultSetMetaData metadata = rs.getMetaData();
      
      // Verify we get all columns (should be 21 based on schema)
      assertTrue(metadata.getColumnCount() >= 20, 
                 "SELECT * should return all columns (at least 20): " + metadata.getColumnCount());
      
      // Verify key columns exist
      boolean hasCloudProvider = false;
      boolean hasClusterName = false;
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        String colName = metadata.getColumnName(i).toLowerCase();
        if ("cloud_provider".equals(colName)) hasCloudProvider = true;
        if ("cluster_name".equals(colName)) hasClusterName = true;
      }
      assertTrue(hasCloudProvider, "SELECT * should include cloud_provider column");
      assertTrue(hasClusterName, "SELECT * should include cluster_name column");
      
      boolean hasResults = false;
      while (rs.next()) {
        hasResults = true;
        // Just verify we can read the first column
        String provider = rs.getString("cloud_provider");
        assertNotNull(provider, "Cloud provider should not be null in SELECT *");
      }
      
      if (hasResults) {
        LOGGER.info("SELECT * projection test passed - received {} columns with data", 
                    metadata.getColumnCount());
      } else {
        LOGGER.warn("SELECT * projection test: No data returned from cloud providers");
      }
    }
  }
  
  @Test
  void testProjectionWithFilter() throws SQLException {
    // Test projection combined with WHERE clause
    String sql = "SELECT cloud_provider, cluster_name " +
                 "FROM cloud_ops.kubernetes_clusters " +
                 "WHERE cloud_provider = 'azure' LIMIT 5";
    
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      
      ResultSetMetaData metadata = rs.getMetaData();
      assertEquals(2, metadata.getColumnCount(), 
                   "Should project 2 columns with filter");
      
      boolean hasResults = false;
      while (rs.next()) {
        hasResults = true;
        String provider = rs.getString("cloud_provider");
        assertEquals("azure", provider, 
                     "Filter should limit results to Azure only");
      }
      
      LOGGER.info("Projection with filter test: hasResults={}", hasResults);
    }
  }
  
  @Test 
  void testProjectionPerformanceComparison() throws SQLException {
    // This test measures the difference between SELECT * and projected queries
    // to verify projection is working (though actual performance will vary)
    
    // Test 1: SELECT * (baseline)
    String sqlFull = "SELECT * FROM cloud_ops.kubernetes_clusters LIMIT 10";
    long startTime = System.currentTimeMillis();
    
    int fullQueryResultSize = 0;
    try (PreparedStatement stmt = connection.prepareStatement(sqlFull);
         ResultSet rs = stmt.executeQuery()) {
      ResultSetMetaData metadata = rs.getMetaData();
      while (rs.next()) {
        fullQueryResultSize++;
      }
      long fullQueryTime = System.currentTimeMillis() - startTime;
      LOGGER.info("Full query: {} columns, {} rows, {}ms", 
                  metadata.getColumnCount(), fullQueryResultSize, fullQueryTime);
    }
    
    // Test 2: Single column projection
    String sqlProjected = "SELECT cloud_provider FROM cloud_ops.kubernetes_clusters LIMIT 10";
    startTime = System.currentTimeMillis();
    
    int projectedQueryResultSize = 0;
    try (PreparedStatement stmt = connection.prepareStatement(sqlProjected);
         ResultSet rs = stmt.executeQuery()) {
      ResultSetMetaData metadata = rs.getMetaData();
      while (rs.next()) {
        projectedQueryResultSize++;
      }
      long projectedQueryTime = System.currentTimeMillis() - startTime;
      LOGGER.info("Projected query: {} columns, {} rows, {}ms", 
                  metadata.getColumnCount(), projectedQueryResultSize, projectedQueryTime);
      
      // Verify projection worked
      assertEquals(1, metadata.getColumnCount(), 
                   "Projected query should return only 1 column");
    }
    
    // Both queries should return the same number of rows
    assertEquals(fullQueryResultSize, projectedQueryResultSize,
                 "Both queries should return the same number of rows");
  }
  
  @Test
  void testDebugLoggingOutput() throws SQLException {
    // This test is specifically to verify that debug logging shows projection operations
    LOGGER.info("=== Testing Projection Debug Logging ===");
    LOGGER.info("Check the debug output for projection optimization messages");
    
    String sql = "SELECT cloud_provider, region FROM cloud_ops.kubernetes_clusters LIMIT 5";
    
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      
      ResultSetMetaData metadata = rs.getMetaData();
      LOGGER.info("Query executed: {} columns projected", metadata.getColumnCount());
      
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      
      LOGGER.info("Query completed: {} rows returned", rowCount);
      LOGGER.info("=== End Projection Debug Logging Test ===");
    }
    
    // The real test is in the log output - we should see messages like:
    // "CloudOpsProjectionHandler: Projecting 2 out of 21 columns"
    // "Azure KQL projection: ... -> X% data reduction"
    // "AWS EKS querying with client-side projection"
    // "GCP GKE querying with projection"
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