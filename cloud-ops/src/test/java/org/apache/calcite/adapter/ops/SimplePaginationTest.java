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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple demonstration tests for pagination pushdown functionality.
 * These tests verify the core logic without requiring external dependencies.
 */
@Tag("temp")
public class SimplePaginationTest {

  private RelDataTypeFactory typeFactory;
  private RexBuilder rexBuilder;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
  }

  @Test void testBasicPaginationLogic() {
    System.out.println("=== Testing Pagination Pushdown Logic ===\n");

    // Test 1: Basic LIMIT pagination
    System.out.println("--- Test 1: Basic LIMIT Pagination ---");
    RexNode limitNode =
        rexBuilder.makeLiteral(BigDecimal.valueOf(10), typeFactory.createSqlType(SqlTypeName.INTEGER), true);

    CloudOpsPaginationHandler limitHandler = new CloudOpsPaginationHandler(null, limitNode);

    assertTrue(limitHandler.hasPagination(), "Should have pagination for LIMIT clause");
    assertEquals(0L, limitHandler.getOffset(), "Offset should be 0 for LIMIT only");
    assertEquals(10L, limitHandler.getLimit(), "Limit should be 10");

    System.out.println("✅ Basic LIMIT: offset=" + limitHandler.getOffset() + ", limit=" + limitHandler.getLimit());

    // Test 2: OFFSET + LIMIT pagination
    System.out.println("\n--- Test 2: OFFSET + LIMIT Pagination ---");
    RexNode offsetNode =
        rexBuilder.makeLiteral(BigDecimal.valueOf(5), typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    RexNode limitNode2 =
        rexBuilder.makeLiteral(BigDecimal.valueOf(3), typeFactory.createSqlType(SqlTypeName.INTEGER), true);

    CloudOpsPaginationHandler offsetLimitHandler = new CloudOpsPaginationHandler(offsetNode, limitNode2);

    assertEquals(5L, offsetLimitHandler.getOffset());
    assertEquals(3L, offsetLimitHandler.getLimit());
    assertEquals(8L, offsetLimitHandler.getEndPosition());

    System.out.println("✅ OFFSET+LIMIT: offset=" + offsetLimitHandler.getOffset() +
                      ", limit=" + offsetLimitHandler.getLimit() +
                      ", end=" + offsetLimitHandler.getEndPosition());

    // Test 3: Azure KQL generation
    System.out.println("\n--- Test 3: Azure KQL Generation ---");
    String azureKql = offsetLimitHandler.buildAzureKqlPaginationClause();
    assertEquals("| skip 5 | top 3", azureKql);
    System.out.println("✅ Azure KQL: " + azureKql);

    // Test 4: AWS parameter generation
    System.out.println("\n--- Test 4: AWS Parameters ---");
    int awsMaxResults = offsetLimitHandler.getAWSMaxResults();
    boolean needsMultiPage = offsetLimitHandler.needsAWSMultiPageFetch();
    assertEquals(8, awsMaxResults); // offset + limit
    assertTrue(needsMultiPage);
    System.out.println("✅ AWS: MaxResults=" + awsMaxResults + ", needsMultiPage=" + needsMultiPage);

    // Test 5: GCP parameters
    System.out.println("\n--- Test 5: GCP Parameters ---");
    int gcpPageSize = offsetLimitHandler.getGCPPageSize();
    boolean needsGcpMultiPage = offsetLimitHandler.needsGCPMultiPageFetch();
    assertEquals(8, gcpPageSize);
    assertTrue(needsGcpMultiPage);
    System.out.println("✅ GCP: pageSize=" + gcpPageSize + ", needsMultiPage=" + needsGcpMultiPage);

    System.out.println("\n=== All Pagination Logic Tests Passed ===");
  }

  @Test void testClientSidePagination() {
    System.out.println("=== Testing Client-Side Pagination ===\n");

    // Create test data
    List<String> testData =
        Arrays.asList("item-0", "item-1", "item-2", "item-3", "item-4", "item-5", "item-6", "item-7");

    System.out.println("Original data: " + testData);

    // Test OFFSET 3, LIMIT 2
    RexNode offsetNode =
        rexBuilder.makeLiteral(BigDecimal.valueOf(3), typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    RexNode limitNode =
        rexBuilder.makeLiteral(BigDecimal.valueOf(2), typeFactory.createSqlType(SqlTypeName.INTEGER), true);

    CloudOpsPaginationHandler paginationHandler = new CloudOpsPaginationHandler(offsetNode, limitNode);
    List<String> paginatedData = paginationHandler.applyClientSidePagination(testData);

    assertEquals(2, paginatedData.size());
    assertEquals("item-3", paginatedData.get(0));
    assertEquals("item-4", paginatedData.get(1));

    System.out.println("✅ Paginated data (OFFSET 3, LIMIT 2): " + paginatedData);
    System.out.println("✅ Client-side pagination test passed!");
  }

  @Test void testPaginationStrategies() {
    System.out.println("=== Testing Pagination Strategies ===\n");

    RexNode offsetNode =
        rexBuilder.makeLiteral(BigDecimal.valueOf(10), typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    RexNode limitNode =
        rexBuilder.makeLiteral(BigDecimal.valueOf(5), typeFactory.createSqlType(SqlTypeName.INTEGER), true);

    CloudOpsPaginationHandler handler = new CloudOpsPaginationHandler(offsetNode, limitNode);

    // Test Azure strategy
    CloudOpsPaginationHandler.PaginationStrategy azureStrategy = handler.getAzureStrategy();
    System.out.println("✅ Azure strategy: " + azureStrategy);
    assertTrue(azureStrategy.useServerSidePagination);
    assertFalse(azureStrategy.needsMultipleFetches);

    // Test AWS strategy
    CloudOpsPaginationHandler.PaginationStrategy awsStrategy = handler.getAWSStrategy();
    System.out.println("✅ AWS strategy: " + awsStrategy);
    assertTrue(awsStrategy.useServerSidePagination);
    assertTrue(awsStrategy.needsMultipleFetches);

    // Test GCP strategy
    CloudOpsPaginationHandler.PaginationStrategy gcpStrategy = handler.getGCPStrategy();
    System.out.println("✅ GCP strategy: " + gcpStrategy);
    assertTrue(gcpStrategy.useServerSidePagination);
    assertTrue(gcpStrategy.needsMultipleFetches);

    System.out.println("✅ All pagination strategies test passed!");
  }

  @Test void testPaginationMetrics() {
    System.out.println("=== Testing Pagination Metrics ===\n");

    RexNode limitNode =
        rexBuilder.makeLiteral(BigDecimal.valueOf(50), typeFactory.createSqlType(SqlTypeName.INTEGER), true);

    CloudOpsPaginationHandler handler = new CloudOpsPaginationHandler(null, limitNode);

    // Test server-side metrics
    CloudOpsPaginationHandler.PaginationMetrics serverMetrics =
        handler.calculateMetrics(true, 1000);

    assertEquals(1000, serverMetrics.totalResults);
    assertEquals(50, serverMetrics.resultsFetched);
    assertEquals(95.0, serverMetrics.reductionPercent, 0.1);
    assertTrue(serverMetrics.serverSidePushdown);

    System.out.println("✅ Server-side metrics: " + serverMetrics);

    // Test client-side metrics
    CloudOpsPaginationHandler.PaginationMetrics clientMetrics =
        handler.calculateMetrics(false, 1000);

    assertEquals(1000, clientMetrics.totalResults);
    assertEquals(1000, clientMetrics.resultsFetched);
    assertEquals(0.0, clientMetrics.reductionPercent, 0.1);
    assertFalse(clientMetrics.serverSidePushdown);

    System.out.println("✅ Client-side metrics: " + clientMetrics);
    System.out.println("✅ Pagination metrics test passed!");
  }
}
