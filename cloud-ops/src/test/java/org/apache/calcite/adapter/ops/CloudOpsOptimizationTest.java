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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.adapter.ops.util.CloudOpsQueryOptimizer;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Cloud Ops query optimization and pushdown capabilities.
 */
@Tag("unit")
public class CloudOpsOptimizationTest {

  private CloudOpsConfig config;
  private RexBuilder rexBuilder;
  private DataContext dataContext;

  @BeforeEach
  public void setUp() {
    // Create config with minimal required fields
    config =
        new CloudOpsConfig(Arrays.asList("azure", "aws", "gcp"),
        null,  // Azure config
        null,  // GCP config
        null,  // AWS config
        false, // cacheEnabled
        60,    // cacheTtlMinutes
        false); // cacheDebugMode

    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);

    // Create a simple DataContext implementation for testing
    dataContext = new DataContext() {
      @Override public SchemaPlus getRootSchema() {
        return null;
      }

      @Override public JavaTypeFactory getTypeFactory() {
        return typeFactory;
      }

      @Override public QueryProvider getQueryProvider() {
        return null;
      }

      @Override public Object get(String name) {
        return null;
      }
    };
  }

  @Test public void testProjectionPushdown() {
    // Create a test table
    TestCloudOpsTable table = new TestCloudOpsTable(config);

    // Define projections (select columns 0, 2, 4)
    int[] projections = new int[]{0, 2, 4};

    // Call scan with projections
    List<RexNode> filters = new ArrayList<>();
    table.scan(dataContext, filters, projections);

    // Verify projections were received
    assertNotNull(table.lastProjections);
    assertArrayEquals(projections, table.lastProjections);
  }

  @Test public void testFilterPushdown() {
    TestCloudOpsTable table = new TestCloudOpsTable(config);

    // Create filter: cloud_provider = 'azure'
    RexInputRef providerRef =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);
    RexLiteral azureLiteral = rexBuilder.makeLiteral("azure");
    RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, azureLiteral);

    List<RexNode> filters = Arrays.asList(filter);
    table.scan(dataContext, filters, null);

    // Verify filters were received
    assertNotNull(table.lastFilters);
    assertEquals(1, table.lastFilters.size());
    assertEquals(filter, table.lastFilters.get(0));
  }

  @Test public void testSortPushdown() {
    TestCloudOpsTable table = new TestCloudOpsTable(config);

    // Create sort collation (ORDER BY cluster_name ASC, region DESC)
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(2, RelFieldCollation.Direction.ASCENDING),
        new RelFieldCollation(4, RelFieldCollation.Direction.DESCENDING));

    List<RexNode> filters = new ArrayList<>();
    table.scan(dataContext, filters, null, collation, null, null);

    // Verify sort was received
    assertNotNull(table.lastCollation);
    assertEquals(2, table.lastCollation.getFieldCollations().size());
    assertEquals(2, table.lastCollation.getFieldCollations().get(0).getFieldIndex());
    assertEquals(RelFieldCollation.Direction.ASCENDING,
                table.lastCollation.getFieldCollations().get(0).getDirection());
  }

  @Test public void testPaginationPushdown() {
    TestCloudOpsTable table = new TestCloudOpsTable(config);

    // Create pagination (OFFSET 100 LIMIT 50)
    RexLiteral offset = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100));
    RexLiteral fetch = rexBuilder.makeExactLiteral(BigDecimal.valueOf(50));

    List<RexNode> filters = new ArrayList<>();
    table.scan(dataContext, filters, null, null, offset, fetch);

    // Verify pagination was received
    assertNotNull(table.lastOffset);
    assertNotNull(table.lastFetch);
    assertEquals(offset, table.lastOffset);
    assertEquals(fetch, table.lastFetch);
  }

  @Test public void testCombinedOptimizations() {
    TestCloudOpsTable table = new TestCloudOpsTable(config);

    // Create all optimizations
    int[] projections = new int[]{0, 1, 4};

    RexInputRef regionRef =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 4);
    RexLiteral usEastLiteral = rexBuilder.makeLiteral("us-east-1");
    RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, usEastLiteral);
    List<RexNode> filters = Arrays.asList(filter);

    RelCollation collation =
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING));

    RexLiteral offset = rexBuilder.makeExactLiteral(BigDecimal.valueOf(10));
    RexLiteral fetch = rexBuilder.makeExactLiteral(BigDecimal.valueOf(20));

    // Call scan with all optimizations
    table.scan(dataContext, filters, projections, collation, offset, fetch);

    // Verify all optimizations were received
    assertNotNull(table.lastProjections);
    assertNotNull(table.lastFilters);
    assertNotNull(table.lastCollation);
    assertNotNull(table.lastOffset);
    assertNotNull(table.lastFetch);

    assertArrayEquals(projections, table.lastProjections);
    assertEquals(1, table.lastFilters.size());
    assertEquals(1, table.lastCollation.getFieldCollations().size());
  }

  @Test public void testQueryOptimizerExtraction() {
    // Create filters
    RexInputRef providerRef =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);
    RexLiteral azureLiteral = rexBuilder.makeLiteral("azure");
    RexNode providerFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, providerRef, azureLiteral);

    RexInputRef regionRef =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 4);
    RexLiteral regionLiteral = rexBuilder.makeLiteral("us-west-2");
    RexNode regionFilter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, regionRef, regionLiteral);

    List<RexNode> filters = Arrays.asList(providerFilter, regionFilter);

    // Create projections
    int[] projections = new int[]{0, 1, 2};

    // Create sort
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING));

    // Create pagination
    RexLiteral offset = rexBuilder.makeExactLiteral(BigDecimal.valueOf(50));
    RexLiteral fetch = rexBuilder.makeExactLiteral(BigDecimal.valueOf(25));

    // Create optimizer
    CloudOpsQueryOptimizer optimizer =
        new CloudOpsQueryOptimizer(filters, projections, collation, offset, fetch);

    // Test filter extraction
    List<CloudOpsQueryOptimizer.FilterInfo> providerFilters =
        optimizer.extractFiltersForField("cloud_provider", 0);
    assertEquals(1, providerFilters.size());
    assertTrue(providerFilters.get(0).value.toString().contains("azure"));

    List<CloudOpsQueryOptimizer.FilterInfo> regionFilters =
        optimizer.extractFiltersForField("region", 4);
    assertEquals(1, regionFilters.size());
    assertTrue(regionFilters.get(0).value.toString().contains("us-west-2"));

    // Test sort info
    CloudOpsQueryOptimizer.SortInfo sortInfo = optimizer.getSortInfo();
    assertNotNull(sortInfo);
    assertEquals(1, sortInfo.sortFields.size());
    assertEquals(1, sortInfo.sortFields.get(0).fieldIndex);
    assertEquals(RelFieldCollation.Direction.DESCENDING, sortInfo.sortFields.get(0).direction);

    // Test pagination info
    CloudOpsQueryOptimizer.PaginationInfo paginationInfo = optimizer.getPaginationInfo();
    assertNotNull(paginationInfo);
    assertEquals(50, paginationInfo.offset);
    assertEquals(25, paginationInfo.fetch);

    // Test projection info
    CloudOpsQueryOptimizer.ProjectionInfo projectionInfo = optimizer.getProjectionInfo();
    assertNotNull(projectionInfo);
    assertArrayEquals(projections, projectionInfo.columns);

    // Test optimization checks
    assertTrue(optimizer.hasOptimization(CloudOpsQueryOptimizer.OptimizationType.FILTER));
    assertTrue(optimizer.hasOptimization(CloudOpsQueryOptimizer.OptimizationType.PROJECTION));
    assertTrue(optimizer.hasOptimization(CloudOpsQueryOptimizer.OptimizationType.SORT));
    assertTrue(optimizer.hasOptimization(CloudOpsQueryOptimizer.OptimizationType.PAGINATION));
  }

  /**
   * Test implementation of AbstractCloudOpsTable that captures optimization hints.
   */
  private static class TestCloudOpsTable extends AbstractCloudOpsTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      // Return a simple test schema
      return typeFactory.builder()
          .add("cloud_provider", SqlTypeName.VARCHAR)
          .add("account_id", SqlTypeName.VARCHAR)
          .add("resource_name", SqlTypeName.VARCHAR)
          .add("resource_type", SqlTypeName.VARCHAR)
          .add("region", SqlTypeName.VARCHAR)
          .build();
    }

    public List<RexNode> lastFilters;
    public int[] lastProjections;
    public RelCollation lastCollation;
    public RexNode lastOffset;
    public RexNode lastFetch;

    public TestCloudOpsTable(CloudOpsConfig config) {
      super(config);
    }

    @Override public Enumerable<Object[]> scan(DataContext root,
                                    List<RexNode> filters,
                                    int[] projects,
                                    RelCollation collation,
                                    RexNode offset,
                                    RexNode fetch) {
      // Capture all parameters for verification
      this.lastFilters = filters;
      this.lastProjections = projects;
      this.lastCollation = collation;
      this.lastOffset = offset;
      this.lastFetch = fetch;

      // Call parent to trigger logging
      super.scan(root, filters, projects, collation, offset, fetch);

      // Return empty result for testing
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }

    @Override protected List<Object[]> queryAzure(List<String> subscriptionIds,
                                        org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler projectionHandler,
                                        org.apache.calcite.adapter.ops.util.CloudOpsSortHandler sortHandler,
                                        org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler paginationHandler,
                                        org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler filterHandler) {
      return new ArrayList<>();
    }

    @Override protected List<Object[]> queryGCP(List<String> projectIds,
                                      org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler projectionHandler,
                                      org.apache.calcite.adapter.ops.util.CloudOpsSortHandler sortHandler,
                                      org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler paginationHandler,
                                      org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler filterHandler) {
      return new ArrayList<>();
    }

    @Override protected List<Object[]> queryAWS(List<String> accountIds,
                                      org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler projectionHandler,
                                      org.apache.calcite.adapter.ops.util.CloudOpsSortHandler sortHandler,
                                      org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler paginationHandler,
                                      org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler filterHandler) {
      return new ArrayList<>();
    }
  }
}
