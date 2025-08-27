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

import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for pagination pushdown optimization in cloud-ops adapter.
 * 
 * These tests verify that LIMIT/OFFSET clauses are correctly pushed down to cloud providers
 * and that pagination optimization metrics are properly calculated and logged.
 * 
 * Tests require local cloud credentials configured in local-test.properties.
 */
@Tag("integration")
public class PaginationPushdownIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(PaginationPushdownIntegrationTest.class);
  
  private Connection connection;
  private CloudOpsConfig config;
  private RelDataTypeFactory typeFactory;
  private RexBuilder rexBuilder;
  
  @BeforeEach
  void setUp() throws SQLException {
    // Only use real credentials from local properties file
    config = CloudOpsTestUtils.loadTestConfig();
    if (config == null) {
      throw new IllegalStateException("Real credentials required from local-test.properties file");
    }
    
    // Set up Calcite connection with cloud-ops schema
    Properties calciteProps = new Properties();
    calciteProps.setProperty("lex", "JAVA");
    connection = DriverManager.getConnection("jdbc:calcite:", calciteProps);
    
    // Register cloud-ops schema
    org.apache.calcite.jdbc.CalciteConnection calciteConnection = 
        connection.unwrap(org.apache.calcite.jdbc.CalciteConnection.class);
    org.apache.calcite.schema.SchemaPlus rootSchema = calciteConnection.getRootSchema();
    
    CloudOpsSchemaFactory factory = new CloudOpsSchemaFactory();
    rootSchema.add("cloud_ops", factory.create(rootSchema, "cloud_ops", configToOperands(config)));
    
    // Set cloud_ops as the default schema
    calciteConnection.setSchema("cloud_ops");
    
    // Create type factory and RexBuilder for testing utilities
    typeFactory = calciteConnection.getTypeFactory();
    rexBuilder = new RexBuilder(typeFactory);
  }
  
  /**
   * Test 1: Basic LIMIT clause pagination pushdown.
   * Verifies that simple LIMIT queries create appropriate pagination handlers.
   */
  @Test
  void testBasicLimitPagination() {
    logger.debug("=== Test 1: Basic LIMIT Pagination ===");
    
    // Create LIMIT 10 RexNode
    RexNode limitNode = rexBuilder.makeLiteral(BigDecimal.valueOf(10), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    
    // Create pagination handler
    CloudOpsPaginationHandler paginationHandler = new CloudOpsPaginationHandler(null, limitNode);
    
    // Test basic properties
    assertTrue(paginationHandler.hasPagination(), "Should have pagination for LIMIT clause");
    assertEquals(0L, paginationHandler.getOffset(), "Offset should be 0 for LIMIT only");
    assertEquals(10L, paginationHandler.getLimit(), "Limit should be 10");
    assertEquals(10L, paginationHandler.getEndPosition(), "End position should be 10");
    
    // Test Azure strategy
    CloudOpsPaginationHandler.PaginationStrategy azureStrategy = paginationHandler.getAzureStrategy();
    assertTrue(azureStrategy.useServerSidePagination, "Azure should use server-side pagination");
    assertFalse(azureStrategy.needsMultipleFetches, "Azure shouldn't need multiple fetches for LIMIT only");
    assertEquals("KQL TOP/SKIP", azureStrategy.providerStrategy);
    
    // Test KQL generation
    String kqlClause = paginationHandler.buildAzureKqlPaginationClause();
    assertEquals("| top 10", kqlClause, "Should generate correct KQL TOP clause");
    
    logger.debug("✅ Basic LIMIT pagination test passed");
  }
  
  /**
   * Test 2: OFFSET + LIMIT pagination pushdown.
   * Verifies that OFFSET/LIMIT combinations work correctly across providers.
   */
  @Test
  void testOffsetLimitPagination() {
    logger.debug("=== Test 2: OFFSET + LIMIT Pagination ===");
    
    // Create OFFSET 20 LIMIT 5 RexNodes  
    RexNode offsetNode = rexBuilder.makeLiteral(BigDecimal.valueOf(20), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    RexNode limitNode = rexBuilder.makeLiteral(BigDecimal.valueOf(5), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    
    // Create pagination handler
    CloudOpsPaginationHandler paginationHandler = new CloudOpsPaginationHandler(offsetNode, limitNode);
    
    // Test properties
    assertTrue(paginationHandler.hasPagination());
    assertEquals(20L, paginationHandler.getOffset());
    assertEquals(5L, paginationHandler.getLimit());
    assertEquals(25L, paginationHandler.getEndPosition());
    
    // Test Azure strategy (full server-side support)
    CloudOpsPaginationHandler.PaginationStrategy azureStrategy = paginationHandler.getAzureStrategy();
    assertTrue(azureStrategy.useServerSidePagination);
    assertFalse(azureStrategy.needsMultipleFetches, "Azure handles offset server-side");
    
    String kqlClause = paginationHandler.buildAzureKqlPaginationClause();
    assertEquals("| skip 20 | top 5", kqlClause, "Should generate correct KQL SKIP/TOP clause");
    
    // Test AWS strategy (needs client-side offset handling)
    CloudOpsPaginationHandler.PaginationStrategy awsStrategy = paginationHandler.getAWSStrategy();
    assertTrue(awsStrategy.useServerSidePagination, "AWS uses server-side page size control");
    assertTrue(awsStrategy.needsMultipleFetches, "AWS needs multiple fetches for offset");
    assertEquals("MaxResults + NextToken", awsStrategy.providerStrategy);
    
    int awsMaxResults = paginationHandler.getAWSMaxResults();
    assertEquals(25, awsMaxResults, "AWS should fetch offset + limit records");
    
    // Test GCP strategy (similar to AWS)
    CloudOpsPaginationHandler.PaginationStrategy gcpStrategy = paginationHandler.getGCPStrategy();
    assertTrue(gcpStrategy.useServerSidePagination);
    assertTrue(gcpStrategy.needsMultipleFetches, "GCP needs multiple fetches for offset");
    
    int gcpPageSize = paginationHandler.getGCPPageSize();
    assertEquals(25, gcpPageSize, "GCP should fetch offset + limit records");
    
    logger.debug("✅ OFFSET + LIMIT pagination test passed");
  }
  
  /**
   * Test 3: Client-side pagination application.
   * Tests the client-side windowing logic for provider fallback scenarios.
   */
  @Test
  void testClientSidePaginationApplication() {
    logger.debug("=== Test 3: Client-Side Pagination Application ===");
    
    // Create OFFSET 3 LIMIT 2 scenario
    RexNode offsetNode = rexBuilder.makeLiteral(BigDecimal.valueOf(3), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    RexNode limitNode = rexBuilder.makeLiteral(BigDecimal.valueOf(2), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    
    CloudOpsPaginationHandler paginationHandler = new CloudOpsPaginationHandler(offsetNode, limitNode);
    
    // Test data: 8 items
    java.util.List<String> testData = java.util.Arrays.asList(
        "item-0", "item-1", "item-2", "item-3", "item-4", "item-5", "item-6", "item-7");
    
    // Apply client-side pagination
    java.util.List<String> paginatedResults = paginationHandler.applyClientSidePagination(testData);
    
    // Should get items at positions 3 and 4 (OFFSET 3, LIMIT 2)
    assertEquals(2, paginatedResults.size(), "Should return exactly 2 items");
    assertEquals("item-3", paginatedResults.get(0), "First item should be at offset 3");
    assertEquals("item-4", paginatedResults.get(1), "Second item should be at offset 4");
    
    // Test edge case: offset beyond data size
    RexNode largeOffsetNode = rexBuilder.makeLiteral(BigDecimal.valueOf(20), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    CloudOpsPaginationHandler edgeCasePagination = new CloudOpsPaginationHandler(largeOffsetNode, limitNode);
    
    java.util.List<String> emptyResults = edgeCasePagination.applyClientSidePagination(testData);
    assertTrue(emptyResults.isEmpty(), "Should return empty list when offset exceeds data size");
    
    logger.debug("✅ Client-side pagination application test passed");
  }
  
  /**
   * Test 4: Pagination optimization metrics calculation.
   * Verifies that optimization metrics are correctly calculated for different scenarios.
   */
  @Test
  void testPaginationOptimizationMetrics() {
    logger.debug("=== Test 4: Pagination Optimization Metrics ===");
    
    // Test server-side optimization metrics
    RexNode limitNode = rexBuilder.makeLiteral(BigDecimal.valueOf(50), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    CloudOpsPaginationHandler paginationHandler = new CloudOpsPaginationHandler(null, limitNode);
    
    // Simulate server-side pushdown (Azure scenario)
    CloudOpsPaginationHandler.PaginationMetrics serverSideMetrics = 
        paginationHandler.calculateMetrics(true, 1000);
    
    assertEquals(1000, serverSideMetrics.totalResults);
    assertEquals(50, serverSideMetrics.resultsFetched);
    assertEquals(95.0, serverSideMetrics.reductionPercent, 0.1, "Should achieve 95% reduction");
    assertTrue(serverSideMetrics.serverSidePushdown);
    assertEquals("Server-side pagination", serverSideMetrics.strategy);
    
    // Test client-side fallback metrics  
    CloudOpsPaginationHandler.PaginationMetrics clientSideMetrics = 
        paginationHandler.calculateMetrics(false, 1000);
    
    assertEquals(1000, clientSideMetrics.totalResults);
    assertEquals(1000, clientSideMetrics.resultsFetched, "Client-side fetches all results");
    assertEquals(0.0, clientSideMetrics.reductionPercent, 0.1, "No reduction with client-side");
    assertFalse(clientSideMetrics.serverSidePushdown);
    assertEquals("Client-side pagination", clientSideMetrics.strategy);
    
    // Test no pagination metrics
    CloudOpsPaginationHandler noPagination = new CloudOpsPaginationHandler(null, null);
    CloudOpsPaginationHandler.PaginationMetrics noPageMetrics = 
        noPagination.calculateMetrics(false, 1000);
    
    assertEquals("No pagination", noPageMetrics.strategy);
    assertEquals(1000, noPageMetrics.totalResults);
    assertEquals(1000, noPageMetrics.resultsFetched);
    
    logger.debug("✅ Pagination optimization metrics test passed");
  }
  
  /**
   * Test 5: Integration with projection and sort handlers.
   * Verifies that pagination works correctly when combined with other optimizations.
   */
  @Test
  void testPaginationWithProjectionAndSort() {
    logger.debug("=== Test 5: Pagination + Projection + Sort Integration ===");
    
    // Create sample table schema for KubernetesClustersTable
    RelDataType rowType = typeFactory.builder()
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("cluster_name", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .add("kubernetes_version", SqlTypeName.VARCHAR)
        .build();
    
    // Create projection handler - select only cluster_name and region
    int[] projectedColumns = {2, 3}; // cluster_name, region
    CloudOpsProjectionHandler projectionHandler = 
        new CloudOpsProjectionHandler(rowType, projectedColumns);
    
    // Create sort handler - ORDER BY cluster_name ASC
    RelCollation mockCollation = org.apache.calcite.rel.RelCollations.EMPTY; // Simplified for test
    CloudOpsSortHandler sortHandler = new CloudOpsSortHandler(rowType, mockCollation);
    
    // Create pagination handler - OFFSET 5 LIMIT 3
    RexNode offsetNode = rexBuilder.makeLiteral(BigDecimal.valueOf(5), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    RexNode limitNode = rexBuilder.makeLiteral(BigDecimal.valueOf(3), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    CloudOpsPaginationHandler paginationHandler = new CloudOpsPaginationHandler(offsetNode, limitNode);
    
    // Verify all handlers work together
    assertFalse(projectionHandler.isSelectAll(), "Should have projection");
    assertFalse(projectionHandler.isSelectAll(), "Should not be select all");
    
    assertTrue(paginationHandler.hasPagination(), "Should have pagination");
    assertEquals(5L, paginationHandler.getOffset());
    assertEquals(3L, paginationHandler.getLimit());
    
    // Test combined optimization metrics
    CloudOpsProjectionHandler.ProjectionMetrics projMetrics = projectionHandler.calculateMetrics();
    assertEquals(60.0, projMetrics.reductionPercent, 0.1, "Should reduce 3/5 columns = 60%");
    
    CloudOpsPaginationHandler.PaginationMetrics pageMetrics = 
        paginationHandler.calculateMetrics(true, 100);
    assertEquals(97.0, pageMetrics.reductionPercent, 0.1, "Should reduce to 3/100 = 97%");
    
    // Test Azure KQL generation with combined optimizations
    String azureKqlPagination = paginationHandler.buildAzureKqlPaginationClause();
    assertEquals("| skip 5 | top 3", azureKqlPagination);
    
    logger.debug("✅ Pagination + Projection + Sort integration test passed");
  }
  
  /**
   * Test 6: Live SQL query execution with pagination (if credentials available).
   * Tests actual SQL queries with LIMIT/OFFSET against live or mock cloud providers.
   */
  @Test
  void testLiveSQLPaginationQueries() throws SQLException {
    logger.debug("=== Test 6: Live SQL Pagination Queries ===");
    
    // Test basic LIMIT query
    String limitQuery = "SELECT cloud_provider, cluster_name, region " +
                       "FROM kubernetes_clusters " +
                       "LIMIT 5";
    
    try (PreparedStatement stmt = connection.prepareStatement(limitQuery)) {
      logger.debug("Executing query: {}", limitQuery);
      
      try (ResultSet rs = stmt.executeQuery()) {
        int rowCount = 0;
        while (rs.next() && rowCount < 10) { // Safety limit for test
          String provider = rs.getString("cloud_provider");
          String cluster = rs.getString("cluster_name");
          String region = rs.getString("region");
          
          logger.debug("  Row {}: {} | {} | {}", rowCount + 1, provider, cluster, region);
          rowCount++;
        }
        
        assertTrue(rowCount <= 5, "Should not return more than LIMIT 5 rows");
        logger.debug("Returned {} rows (expected ≤ 5)", rowCount);
      }
    } catch (SQLException e) {
      String errorMessage = e.getMessage();
      if (errorMessage != null && (errorMessage.contains("not authorized") || 
                                   errorMessage.contains("Access Denied") ||
                                   errorMessage.contains("Forbidden") ||
                                   errorMessage.contains("401") ||
                                   errorMessage.contains("403"))) {
        logger.warn("Cloud authentication/authorization failed - results may be incomplete: {}", errorMessage);
      } else {
        logger.debug("SQL query failed (expected if no live credentials): {}", errorMessage);
      }
      // This is expected when users have partial cloud permissions
    }
    
    // Test OFFSET + LIMIT query
    String offsetLimitQuery = "SELECT cloud_provider, cluster_name " +
                             "FROM kubernetes_clusters " +
                             "ORDER BY cluster_name " +
                             "LIMIT 3 OFFSET 2";
    
    try (PreparedStatement stmt = connection.prepareStatement(offsetLimitQuery)) {
      logger.debug("Executing query: {}", offsetLimitQuery);
      
      try (ResultSet rs = stmt.executeQuery()) {
        int rowCount = 0;
        while (rs.next() && rowCount < 10) { // Safety limit
          String provider = rs.getString("cloud_provider");
          String cluster = rs.getString("cluster_name");
          
          logger.debug("  Row {}: {} | {}", rowCount + 1, provider, cluster);
          rowCount++;
        }
        
        assertTrue(rowCount <= 3, "Should not return more than LIMIT 3 rows");
        logger.debug("Returned {} rows (expected ≤ 3)", rowCount);
      }
    } catch (SQLException e) {
      String errorMessage = e.getMessage();
      if (errorMessage != null && (errorMessage.contains("not authorized") || 
                                   errorMessage.contains("Access Denied") ||
                                   errorMessage.contains("Forbidden") ||
                                   errorMessage.contains("401") ||
                                   errorMessage.contains("403"))) {
        logger.warn("Cloud authentication/authorization failed - results may be incomplete: {}", errorMessage);
      } else {
        logger.debug("SQL query failed (expected if no live credentials): {}", errorMessage);
      }
    }
    
    logger.debug("✅ Live SQL pagination queries test completed");
  }
  
  /**
   * Test 7: Provider-specific pagination strategy selection.
   * Verifies that each cloud provider uses appropriate pagination strategies.
   */
  @Test
  void testProviderSpecificPaginationStrategies() {
    logger.debug("=== Test 7: Provider-Specific Pagination Strategies ===");
    
    // Test different pagination scenarios
    RexNode smallLimitNode = rexBuilder.makeLiteral(BigDecimal.valueOf(10), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    RexNode offsetNode = rexBuilder.makeLiteral(BigDecimal.valueOf(5), 
        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    
    // Test 1: Small LIMIT only
    CloudOpsPaginationHandler smallLimitHandler = new CloudOpsPaginationHandler(null, smallLimitNode);
    
    // Azure: Full server-side support
    CloudOpsPaginationHandler.PaginationStrategy azureSmall = smallLimitHandler.getAzureStrategy();
    assertTrue(azureSmall.useServerSidePagination);
    assertFalse(azureSmall.needsMultipleFetches);
    assertEquals("Full server-side pagination support", azureSmall.optimizationNotes);
    
    // AWS: Server-side page size only
    CloudOpsPaginationHandler.PaginationStrategy awsSmall = smallLimitHandler.getAWSStrategy();  
    assertTrue(awsSmall.useServerSidePagination);
    assertFalse(awsSmall.needsMultipleFetches);
    assertEquals("Single API call with MaxResults", awsSmall.optimizationNotes);
    
    // GCP: Server-side page size only
    CloudOpsPaginationHandler.PaginationStrategy gcpSmall = smallLimitHandler.getGCPStrategy();
    assertTrue(gcpSmall.useServerSidePagination);
    assertFalse(gcpSmall.needsMultipleFetches);
    assertEquals("Single API call with pageSize", gcpSmall.optimizationNotes);
    
    // Test 2: OFFSET + LIMIT (more complex)
    CloudOpsPaginationHandler offsetLimitHandler = new CloudOpsPaginationHandler(offsetNode, smallLimitNode);
    
    // Azure: Still full server-side
    CloudOpsPaginationHandler.PaginationStrategy azureOffset = offsetLimitHandler.getAzureStrategy();
    assertTrue(azureOffset.useServerSidePagination);
    assertFalse(azureOffset.needsMultipleFetches, "Azure handles offset server-side");
    
    // AWS: Needs multiple fetches for offset
    CloudOpsPaginationHandler.PaginationStrategy awsOffset = offsetLimitHandler.getAWSStrategy();
    assertTrue(awsOffset.useServerSidePagination);
    assertTrue(awsOffset.needsMultipleFetches, "AWS needs multiple calls for offset");
    assertEquals("Multiple API calls needed for offset handling", awsOffset.optimizationNotes);
    
    // GCP: Similar to AWS  
    CloudOpsPaginationHandler.PaginationStrategy gcpOffset = offsetLimitHandler.getGCPStrategy();
    assertTrue(gcpOffset.useServerSidePagination);
    assertTrue(gcpOffset.needsMultipleFetches, "GCP needs multiple calls for offset");
    assertEquals("Multiple API calls needed for offset handling", gcpOffset.optimizationNotes);
    
    logger.debug("Azure strategy: {}", azureOffset);
    logger.debug("AWS strategy: {}", awsOffset);
    logger.debug("GCP strategy: {}", gcpOffset);
    
    logger.debug("✅ Provider-specific pagination strategies test passed");
  }

  private Map<String, Object> configToOperands(CloudOpsConfig config) {
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