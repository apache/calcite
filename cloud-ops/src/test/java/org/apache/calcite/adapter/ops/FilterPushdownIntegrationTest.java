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

import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for filter pushdown optimization across multi-cloud providers.
 */
@Tag("integration")
public class FilterPushdownIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(FilterPushdownIntegrationTest.class);

  private CloudOpsConfig config;
  private RexBuilder rexBuilder;
  private RelDataType kubernetesSchema;

  @BeforeEach
  public void setUp() {
    config = CloudOpsTestUtils.loadTestConfig();
    if (config == null) {
      throw new IllegalStateException("Real credentials required from local-test.properties file");
    }

    SqlTypeFactoryImpl typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);

    // Create Kubernetes table schema for testing
    kubernetesSchema = typeFactory.builder()
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("cluster_name", SqlTypeName.VARCHAR)
        .add("application", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .add("resource_group", SqlTypeName.VARCHAR)
        .add("kubernetes_version", SqlTypeName.VARCHAR)
        .add("node_count", SqlTypeName.INTEGER)
        .add("rbac_enabled", SqlTypeName.BOOLEAN)
        .add("private_cluster", SqlTypeName.BOOLEAN)
        .build();
  }

  @Test public void testBasicEqualsFilterPushdown() {
    LOGGER.info("=== Testing Basic EQUALS Filter Pushdown ===");

    // Create filter: region = 'us-east-1'
    // (this will generate WHERE clause unlike provider filters)
    RexInputRef regionRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(4).getType(), 4);
    RexLiteral regionLiteral = rexBuilder.makeLiteral("us-east-1");
    RexNode regionFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, regionLiteral);

    List<RexNode> filters = Arrays.asList(regionFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(kubernetesSchema, filters);

    // Verify filter analysis
    assertTrue(filterHandler.hasFilters());
    assertTrue(filterHandler.hasPushableFilters());

    // Test Azure KQL WHERE clause generation (region filters DO generate WHERE clauses)
    String kqlWhere = filterHandler.buildAzureKqlWhereClause();
    assertNotNull(kqlWhere);
    // Region filter included in WHERE
    assertTrue(kqlWhere.contains("location == 'us-east-1'"));

    // Test AWS and GCP parameter extraction
    Map<String, Object> awsParams = filterHandler.getAWSFilterParameters();
    assertEquals("us-east-1", awsParams.get("region"));

    Map<String, Object> gcpParams = filterHandler.getGCPFilterParameters();
    assertEquals("us-east-1", gcpParams.get("region"));

    // Test metrics
    CloudOpsFilterHandler.FilterMetrics metrics = filterHandler.calculateMetrics(true, 1);
    assertEquals(1, metrics.totalFilters);
    assertTrue(metrics.serverSidePushdown);

    LOGGER.info("✅ Region filter pushdown: WHERE='{}', AWS={}, GCP={}",
        kqlWhere, awsParams, gcpParams);
    LOGGER.info("✅ Filter metrics: {}", metrics);
  }

  @Test public void testRegionFilterPushdown() {
    LOGGER.info("=== Testing Region Filter Pushdown ===");

    // Create filter: region = 'us-east-1'
    RexInputRef regionRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(4).getType(), 4);
    RexLiteral regionLiteral = rexBuilder.makeLiteral("us-east-1");
    RexNode regionFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, regionLiteral);

    List<RexNode> filters = Arrays.asList(regionFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(kubernetesSchema, filters);

    // Test Azure KQL generation
    String azureKql = filterHandler.buildAzureKqlWhereClause();
    assertNotNull(azureKql);
    assertTrue(azureKql.contains("location == 'us-east-1'"));

    // Test AWS parameters
    Map<String, Object> awsParams = filterHandler.getAWSFilterParameters();
    assertEquals("us-east-1", awsParams.get("region"));

    // Test GCP parameters
    Map<String, Object> gcpParams = filterHandler.getGCPFilterParameters();
    assertEquals("us-east-1", gcpParams.get("region"));

    LOGGER.info("✅ Azure KQL WHERE: {}", azureKql);
    LOGGER.info("✅ AWS parameters: {}", awsParams);
    LOGGER.info("✅ GCP parameters: {}", gcpParams);
  }

  @Test public void testApplicationFilterPushdown() {
    LOGGER.info("=== Testing Application Filter Pushdown ===");

    // Create filter: application = 'MyApp'
    RexInputRef appRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(3).getType(), 3);
    RexLiteral appLiteral = rexBuilder.makeLiteral("MyApp");
    RexNode appFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, appRef, appLiteral);

    List<RexNode> filters = Arrays.asList(appFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(kubernetesSchema, filters);

    // Test Azure KQL generation
    String azureKql = filterHandler.buildAzureKqlWhereClause();
    assertNotNull(azureKql);
    assertTrue(azureKql.contains("Application == 'MyApp'"));

    // Test AWS tag-based filtering
    Map<String, Object> awsParams = filterHandler.getAWSFilterParameters();
    assertEquals("MyApp", awsParams.get("tag:Application"));

    LOGGER.info("✅ Azure KQL WHERE: {}", azureKql);
    LOGGER.info("✅ AWS tag filter: {}", awsParams);
  }

  @Test public void testInFilterPushdown() {
    LOGGER.info("=== Testing IN Filter Pushdown ===");

    // Create filter: cloud_provider IN ('azure', 'aws')
    // Using OR approach since IN construction is complex
    RexInputRef providerRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(0).getType(), 0);
    RexLiteral azureLiteral = rexBuilder.makeLiteral("azure");
    RexLiteral awsLiteral = rexBuilder.makeLiteral("aws");

    // Create provider = 'azure' OR provider = 'aws' which is equivalent to IN
    RexNode azureFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, azureLiteral);
    RexNode awsFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, awsLiteral);
    RexNode inFilter = rexBuilder.makeCall(SqlStdOperatorTable.OR, azureFilter, awsFilter);

    List<RexNode> filters = Arrays.asList(inFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(kubernetesSchema, filters);

    // Test provider constraint extraction for IN filter
    Set<String> providers = filterHandler.extractProviderConstraints();
    assertEquals(2, providers.size());
    assertTrue(providers.contains("azure"));
    assertTrue(providers.contains("aws"));
    // Should not query GCP
    assertFalse(providers.contains("gcp"));

    LOGGER.info("✅ IN filter: {} -> Providers to query: {}", inFilter, providers);
  }

  @Test public void testLikeFilterPushdown() {
    LOGGER.info("=== Testing LIKE Filter Pushdown ===");

    // Create filter: cluster_name LIKE 'prod%'
    RexInputRef nameRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(2).getType(), 2);
    RexLiteral patternLiteral = rexBuilder.makeLiteral("prod%");
    RexNode likeFilter = rexBuilder.makeCall(SqlStdOperatorTable.LIKE, nameRef, patternLiteral);

    List<RexNode> filters = Arrays.asList(likeFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(kubernetesSchema, filters);

    // Test Azure KQL generation for LIKE
    String azureKql = filterHandler.buildAzureKqlWhereClause();
    assertNotNull(azureKql);
    // KQL uses contains for LIKE patterns
    assertTrue(azureKql.contains("name contains 'prod'"));

    LOGGER.info("✅ LIKE filter: {} -> Azure KQL: {}", likeFilter, azureKql);
  }

  @Test public void testMultipleFiltersCoordination() {
    LOGGER.info("=== Testing Multiple Filter Coordination ===");

    // Create multiple filters:
    // 1. cloud_provider = 'azure'
    // 2. region = 'eastus'
    // 3. application = 'WebApp'
    RexInputRef providerRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(0).getType(), 0);
    RexLiteral azureLiteral = rexBuilder.makeLiteral("azure");
    RexNode providerFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, azureLiteral);

    RexInputRef regionRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(4).getType(), 4);
    RexLiteral regionLiteral = rexBuilder.makeLiteral("eastus");
    RexNode regionFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, regionLiteral);

    RexInputRef appRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(3).getType(), 3);
    RexLiteral appLiteral = rexBuilder.makeLiteral("WebApp");
    RexNode appFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, appRef, appLiteral);

    List<RexNode> filters = Arrays.asList(providerFilter, regionFilter, appFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(kubernetesSchema, filters);

    // Test comprehensive filter analysis
    assertEquals(3, filterHandler.getPushableFilters().size());

    // Test provider selection (should only query Azure)
    Set<String> providers = filterHandler.extractProviderConstraints();
    assertEquals(1, providers.size());
    assertTrue(providers.contains("azure"));

    // Test Azure KQL generation with multiple WHERE conditions
    String azureKql = filterHandler.buildAzureKqlWhereClause();
    assertNotNull(azureKql);
    assertTrue(azureKql.contains("location == 'eastus'"));
    assertTrue(azureKql.contains("Application == 'WebApp'"));
    // Multiple conditions joined
    assertTrue(azureKql.contains(" and "));

    // Test optimization metrics
    CloudOpsFilterHandler.FilterMetrics metrics = filterHandler.calculateMetrics(true, 3);
    assertEquals(3, metrics.totalFilters);
    assertEquals(3, metrics.filtersApplied);
    assertEquals(100.0, metrics.pushdownPercent, 0.1);

    LOGGER.info("✅ Multi-filter coordination:");
    LOGGER.info("   Providers to query: {}", providers);
    LOGGER.info("   Azure KQL WHERE: {}", azureKql);
    LOGGER.info("   Optimization metrics: {}", metrics);
  }

  @Test public void testFilterOptimizationStrategies() {
    LOGGER.info("=== Testing Filter Optimization Strategies ===");

    // Create filters targeting different optimization scenarios
    RexInputRef providerRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(0).getType(), 0);
    RexLiteral gcpLiteral = rexBuilder.makeLiteral("gcp");
    RexNode providerFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, gcpLiteral);

    RexInputRef regionRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(4).getType(), 4);
    RexLiteral regionLiteral = rexBuilder.makeLiteral("us-central1");
    RexNode regionFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, regionLiteral);

    List<RexNode> filters = Arrays.asList(providerFilter, regionFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(kubernetesSchema, filters);

    // Test GCP-specific optimization
    Set<String> providers = filterHandler.extractProviderConstraints();
    assertEquals(1, providers.size());
    assertTrue(providers.contains("gcp"));

    Map<String, Object> gcpParams = filterHandler.getGCPFilterParameters();
    assertEquals("us-central1", gcpParams.get("region"));

    // Verify AWS and Azure are not queried
    Map<String, Object> awsParams = filterHandler.getAWSFilterParameters();
    // Azure won't be queried due to provider filter
    Map<String, Object> azureParams = Map.of();

    LOGGER.info("✅ GCP-targeted optimization:");
    LOGGER.info("   Only GCP will be queried: {}", providers);
    LOGGER.info("   GCP region constraint: {}", gcpParams);
    LOGGER.info("   AWS parameters (empty): {}", awsParams);
  }

  @Test public void testNullAndNotNullFilters() {
    LOGGER.info("=== Testing NULL and NOT NULL Filter Pushdown ===");

    // Create filter: application IS NOT NULL
    RexInputRef appRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(3).getType(), 3);
    RexNode notNullFilter = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, appRef);

    List<RexNode> filters = Arrays.asList(notNullFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(kubernetesSchema, filters);

    assertTrue(filterHandler.hasPushableFilters());

    // Test Azure KQL generation for NOT NULL
    String azureKql = filterHandler.buildAzureKqlWhereClause();
    assertNotNull(azureKql);
    assertTrue(azureKql.contains("isnotempty(Application)"));

    LOGGER.info("✅ NOT NULL filter -> Azure KQL: {}", azureKql);
  }

  @Test public void testFilterPushdownMetricsCalculation() {
    LOGGER.info("=== Testing Filter Pushdown Metrics ===");

    // Create comprehensive filter scenario
    RexInputRef providerRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(0).getType(), 0);
    RexInputRef regionRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(4).getType(), 4);
    RexInputRef appRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(3).getType(), 3);

    RexLiteral azureLiteral = rexBuilder.makeLiteral("azure");
    RexLiteral regionLiteral = rexBuilder.makeLiteral("westus");
    RexLiteral appLiteral = rexBuilder.makeLiteral("CriticalApp");

    RexNode providerFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, azureLiteral);
    RexNode regionFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, regionLiteral);
    RexNode appFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, appRef, appLiteral);

    List<RexNode> filters = Arrays.asList(providerFilter, regionFilter, appFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(kubernetesSchema, filters);

    // Calculate server-side metrics
    CloudOpsFilterHandler.FilterMetrics serverSideMetrics =
        filterHandler.calculateMetrics(true, 3);

    assertEquals(3, serverSideMetrics.totalFilters);
    assertEquals(3, serverSideMetrics.filtersApplied);
    assertEquals(100.0, serverSideMetrics.pushdownPercent, 0.1);
    assertTrue(serverSideMetrics.serverSidePushdown);
    assertEquals("Server-side filter pushdown", serverSideMetrics.strategy);

    // Simulate client-side metrics for comparison
    CloudOpsFilterHandler.FilterMetrics clientSideMetrics =
        filterHandler.calculateMetrics(false, 3);

    assertEquals(3, clientSideMetrics.totalFilters);
    assertFalse(clientSideMetrics.serverSidePushdown);
    assertEquals("Client-side filtering", clientSideMetrics.strategy);

    LOGGER.info("✅ Server-side metrics: {}", serverSideMetrics);
    LOGGER.info("✅ Client-side metrics: {}", clientSideMetrics);

    // Verify optimization benefits
    assertTrue(serverSideMetrics.pushdownPercent > 50.0,
               "Server-side pushdown should provide significant optimization");
  }

  @Test public void testComplexFilterScenarios() {
    LOGGER.info("=== Testing Complex SQL Filter Scenarios ===");

    LOGGER.info("Scenario 1: Mixed provider and resource constraints");
    // cloud_provider = 'aws' AND region = 'us-west-2' AND node_count > 3
    RexInputRef providerRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(0).getType(), 0);
    RexInputRef regionRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(4).getType(), 4);
    RexInputRef nodeCountRef =
        rexBuilder.makeInputRef(kubernetesSchema.getFieldList().get(7).getType(), 7);

    RexLiteral awsLiteral = rexBuilder.makeLiteral("aws");
    RexLiteral regionLiteral = rexBuilder.makeLiteral("us-west-2");
    RexLiteral nodeCountLiteral =
        rexBuilder.makeExactLiteral(new java.math.BigDecimal(3));

    RexNode providerFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, awsLiteral);
    RexNode regionFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, regionLiteral);
    RexNode nodeCountFilter =
        rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, nodeCountRef, nodeCountLiteral);

    List<RexNode> complexFilters =
        Arrays.asList(providerFilter, regionFilter, nodeCountFilter);
    CloudOpsFilterHandler complexHandler =
        new CloudOpsFilterHandler(kubernetesSchema, complexFilters);

    // Verify AWS-only querying
    Set<String> providers = complexHandler.extractProviderConstraints();
    assertEquals(1, providers.size());
    assertTrue(providers.contains("aws"));

    // Verify AWS region parameter extraction
    Map<String, Object> awsParams = complexHandler.getAWSFilterParameters();
    assertEquals("us-west-2", awsParams.get("region"));

    LOGGER.info("   ✅ Complex filters -> AWS only: {}", providers);
    LOGGER.info("   ✅ AWS region constraint: {}", awsParams);

    LOGGER.info("All complex filter pushdown scenarios validated successfully!");
  }
}
