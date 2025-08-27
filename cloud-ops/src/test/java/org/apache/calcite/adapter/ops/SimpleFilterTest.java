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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple tests for filter pushdown functionality without external dependencies.
 */
@Tag("unit")
public class SimpleFilterTest {
  private static final Logger logger = LoggerFactory.getLogger(SimpleFilterTest.class);

  private RexBuilder rexBuilder;
  private RelDataType testSchema;

  @BeforeEach
  public void setUp() {
    logger.info("=== Setting up Simple Filter Test ===");
    
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(
        org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);

    // Create simple test schema
    testSchema = typeFactory.builder()
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("cluster_name", SqlTypeName.VARCHAR)
        .add("application", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .build();

    logger.debug("Test schema created with {} fields", testSchema.getFieldCount());
  }

  @Test public void testFilterHandlerCreation() {
    logger.info("=== Testing Filter Handler Creation ===");

    // Test with no filters
    CloudOpsFilterHandler emptyHandler = new CloudOpsFilterHandler(testSchema, null);
    assertFalse(emptyHandler.hasFilters());
    assertFalse(emptyHandler.hasPushableFilters());
    
    // Test with empty filter list
    CloudOpsFilterHandler emptyListHandler = new CloudOpsFilterHandler(testSchema, Arrays.asList());
    assertFalse(emptyListHandler.hasFilters());
    
    logger.info("✅ Empty filter handler creation: No filters = {}, No pushable = {}", 
               emptyHandler.hasFilters(), emptyHandler.hasPushableFilters());
  }

  @Test public void testProviderFilterLogic() {
    logger.info("=== Testing Provider Filter Logic ===");

    // Create cloud_provider = 'azure' filter
    RexInputRef providerRef = rexBuilder.makeInputRef(testSchema.getFieldList().get(0).getType(), 0);
    RexLiteral azureLiteral = rexBuilder.makeLiteral("azure");
    RexNode providerFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, azureLiteral);

    List<RexNode> filters = Arrays.asList(providerFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(testSchema, filters);

    // Verify basic filter analysis
    assertTrue(filterHandler.hasFilters());
    assertTrue(filterHandler.hasPushableFilters());
    assertEquals(1, filterHandler.getPushableFilters().size());

    // Test provider constraint extraction
    Set<String> providers = filterHandler.extractProviderConstraints();
    assertEquals(1, providers.size());
    assertTrue(providers.contains("azure"));

    logger.info("✅ Provider filter: cloud_provider = 'azure' -> Only query Azure");
    logger.info("   Pushable filters: {}", filterHandler.getPushableFilters().size());
    logger.info("   Provider constraints: {}", providers);
  }

  @Test public void testRegionFilterGeneration() {
    logger.info("=== Testing Region Filter Generation ===");

    // Create region = 'us-east-1' filter
    RexInputRef regionRef = rexBuilder.makeInputRef(testSchema.getFieldList().get(4).getType(), 4);
    RexLiteral regionLiteral = rexBuilder.makeLiteral("us-east-1");
    RexNode regionFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, regionLiteral);

    List<RexNode> filters = Arrays.asList(regionFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(testSchema, filters);

    // Test Azure KQL generation
    String azureKql = filterHandler.buildAzureKqlWhereClause();
    assertNotNull(azureKql);
    assertTrue(azureKql.contains("| where"));
    assertTrue(azureKql.contains("location == 'us-east-1'"));

    // Test AWS parameter extraction
    Map<String, Object> awsParams = filterHandler.getAWSFilterParameters();
    assertEquals("us-east-1", awsParams.get("region"));

    // Test GCP parameter extraction
    Map<String, Object> gcpParams = filterHandler.getGCPFilterParameters();
    assertEquals("us-east-1", gcpParams.get("region"));

    logger.info("✅ Region filter generation:");
    logger.info("   Azure KQL: {}", azureKql);
    logger.info("   AWS parameters: {}", awsParams);
    logger.info("   GCP parameters: {}", gcpParams);
  }

  @Test public void testApplicationFilterGeneration() {
    logger.info("=== Testing Application Filter Generation ===");

    // Create application = 'WebApp' filter
    RexInputRef appRef = rexBuilder.makeInputRef(testSchema.getFieldList().get(3).getType(), 3);
    RexLiteral appLiteral = rexBuilder.makeLiteral("WebApp");
    RexNode appFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, appRef, appLiteral);

    List<RexNode> filters = Arrays.asList(appFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(testSchema, filters);

    // Test Azure KQL generation
    String azureKql = filterHandler.buildAzureKqlWhereClause();
    assertNotNull(azureKql);
    assertTrue(azureKql.contains("Application == 'WebApp'"));

    // Test AWS tag parameter extraction
    Map<String, Object> awsParams = filterHandler.getAWSFilterParameters();
    assertEquals("WebApp", awsParams.get("tag:Application"));

    logger.info("✅ Application filter generation:");
    logger.info("   Azure KQL: {}", azureKql);
    logger.info("   AWS tag filter: {}", awsParams);
  }

  @Test public void testInFilterHandling() {
    logger.info("=== Testing IN Filter Handling ===");

    // Create cloud_provider IN ('azure', 'gcp') filter - using OR approach since IN construction is complex
    RexInputRef providerRef = rexBuilder.makeInputRef(testSchema.getFieldList().get(0).getType(), 0);
    RexLiteral azureLiteral = rexBuilder.makeLiteral("azure");
    RexLiteral gcpLiteral = rexBuilder.makeLiteral("gcp");
    
    // Create provider = 'azure' OR provider = 'gcp' which is equivalent to IN
    RexNode azureFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, azureLiteral);
    RexNode gcpFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, gcpLiteral);
    RexNode inFilter = rexBuilder.makeCall(SqlStdOperatorTable.OR, azureFilter, gcpFilter);

    List<RexNode> filters = Arrays.asList(inFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(testSchema, filters);

    // Test provider constraint extraction for OR filter (equivalent to IN)
    Set<String> providers = filterHandler.extractProviderConstraints();
    assertEquals(2, providers.size());
    assertTrue(providers.contains("azure"));
    assertTrue(providers.contains("gcp"));
    assertFalse(providers.contains("aws")); // AWS should be excluded

    logger.info("✅ OR filter: cloud_provider = 'azure' OR cloud_provider = 'gcp' (equivalent to IN)");
    logger.info("   Providers to query: {}", providers);
    logger.info("   AWS excluded: {}", !providers.contains("aws"));
  }

  @Test public void testMultipleFilterCoordination() {
    logger.info("=== Testing Multiple Filter Coordination ===");

    // Create multiple filters:
    // 1. cloud_provider = 'aws'
    // 2. region = 'us-west-2'
    // 3. application = 'MyService'
    RexInputRef providerRef = rexBuilder.makeInputRef(testSchema.getFieldList().get(0).getType(), 0);
    RexInputRef regionRef = rexBuilder.makeInputRef(testSchema.getFieldList().get(4).getType(), 4);
    RexInputRef appRef = rexBuilder.makeInputRef(testSchema.getFieldList().get(3).getType(), 3);

    RexLiteral awsLiteral = rexBuilder.makeLiteral("aws");
    RexLiteral regionLiteral = rexBuilder.makeLiteral("us-west-2");
    RexLiteral appLiteral = rexBuilder.makeLiteral("MyService");

    RexNode providerFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, awsLiteral);
    RexNode regionFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, regionLiteral);
    RexNode appFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, appRef, appLiteral);

    List<RexNode> filters = Arrays.asList(providerFilter, regionFilter, appFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(testSchema, filters);

    // Verify all filters are pushable
    assertEquals(3, filterHandler.getPushableFilters().size());
    assertEquals(0, filterHandler.getRemainingFilters().size());

    // Test provider selection (should only query AWS)
    Set<String> providers = filterHandler.extractProviderConstraints();
    assertEquals(1, providers.size());
    assertTrue(providers.contains("aws"));

    // Test AWS parameter extraction
    Map<String, Object> awsParams = filterHandler.getAWSFilterParameters();
    assertEquals("us-west-2", awsParams.get("region"));
    assertEquals("MyService", awsParams.get("tag:Application"));

    logger.info("✅ Multiple filter coordination:");
    logger.info("   Total filters: {}, Pushable: {}, Remaining: {}", 
               filters.size(), filterHandler.getPushableFilters().size(), 
               filterHandler.getRemainingFilters().size());
    logger.info("   Provider selection: {}", providers);
    logger.info("   AWS parameters: {}", awsParams);
  }

  @Test public void testFilterMetricsCalculation() {
    logger.info("=== Testing Filter Metrics Calculation ===");

    // Create sample filters
    RexInputRef providerRef = rexBuilder.makeInputRef(testSchema.getFieldList().get(0).getType(), 0);
    RexInputRef regionRef = rexBuilder.makeInputRef(testSchema.getFieldList().get(4).getType(), 4);

    RexLiteral azureLiteral = rexBuilder.makeLiteral("azure");
    RexLiteral regionLiteral = rexBuilder.makeLiteral("eastus");

    RexNode providerFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, azureLiteral);
    RexNode regionFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, regionLiteral);

    List<RexNode> filters = Arrays.asList(providerFilter, regionFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(testSchema, filters);

    // Test server-side metrics
    CloudOpsFilterHandler.FilterMetrics serverMetrics = 
        filterHandler.calculateMetrics(true, 2);
    
    assertEquals(2, serverMetrics.totalFilters);
    assertEquals(2, serverMetrics.filtersApplied);
    assertEquals(100.0, serverMetrics.pushdownPercent, 0.1);
    assertTrue(serverMetrics.serverSidePushdown);

    // Test client-side metrics
    CloudOpsFilterHandler.FilterMetrics clientMetrics = 
        filterHandler.calculateMetrics(false, 2);
    
    assertEquals(2, clientMetrics.totalFilters);
    assertEquals(2, clientMetrics.filtersApplied);
    assertFalse(clientMetrics.serverSidePushdown);

    logger.info("✅ Filter metrics calculation:");
    logger.info("   Server-side: {}", serverMetrics);
    logger.info("   Client-side: {}", clientMetrics);
    logger.info("   Pushdown efficiency: {:.1f}%", serverMetrics.pushdownPercent);
  }

  @Test public void testFieldFilterExtraction() {
    logger.info("=== Testing Field Filter Extraction ===");

    // Create various filters for testing field extraction
    RexInputRef clusterRef = rexBuilder.makeInputRef(testSchema.getFieldList().get(2).getType(), 2);
    RexLiteral clusterLiteral = rexBuilder.makeLiteral("prod-cluster");
    RexNode clusterFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, clusterRef, clusterLiteral);

    List<RexNode> filters = Arrays.asList(clusterFilter);
    CloudOpsFilterHandler filterHandler = new CloudOpsFilterHandler(testSchema, filters);

    // Test field-specific filter extraction
    List<CloudOpsFilterHandler.FilterInfo> clusterFilters = 
        filterHandler.getFiltersForField("cluster_name");
    
    assertEquals(1, clusterFilters.size());
    CloudOpsFilterHandler.FilterInfo clusterFilterInfo = clusterFilters.get(0);
    assertEquals("cluster_name", clusterFilterInfo.fieldName);
    assertEquals(2, clusterFilterInfo.fieldIndex); // cluster_name is at index 2
    assertEquals("prod-cluster", clusterFilterInfo.value instanceof org.apache.calcite.util.NlsString ? 
        ((org.apache.calcite.util.NlsString) clusterFilterInfo.value).getValue() : 
        clusterFilterInfo.value);

    // Test empty field filter extraction
    List<CloudOpsFilterHandler.FilterInfo> emptyFilters = 
        filterHandler.getFiltersForField("nonexistent_field");
    assertTrue(emptyFilters.isEmpty());

    logger.info("✅ Field filter extraction:");
    logger.info("   cluster_name filters: {}", clusterFilters.size());
    logger.info("   Filter info: {}", clusterFilterInfo);
    logger.info("   Non-existent field filters: {}", emptyFilters.size());
  }
}