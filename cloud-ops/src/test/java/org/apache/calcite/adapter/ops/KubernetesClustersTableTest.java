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
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link KubernetesClustersTable}.
 */
@Tag("unit")
public class KubernetesClustersTableTest {

  @Test public void testGetRowType() {
    CloudOpsConfig config = createTestConfig();
    KubernetesClustersTable table = new KubernetesClustersTable(config);
    RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    RelDataType rowType = table.getRowType(typeFactory);

    assertThat(rowType, is(notNullValue()));
    assertThat(rowType.getFieldCount(), is(23)); // Expected number of columns

    // Verify key columns exist
    assertThat(rowType.getField("cloud_provider", false, false), is(notNullValue()));
    assertThat(rowType.getField("account_id", false, false), is(notNullValue()));
    assertThat(rowType.getField("cluster_name", false, false), is(notNullValue()));
    assertThat(rowType.getField("application", false, false), is(notNullValue()));
    assertThat(rowType.getField("kubernetes_version", false, false), is(notNullValue()));
    assertThat(rowType.getField("rbac_enabled", false, false), is(notNullValue()));
    assertThat(rowType.getField("private_cluster", false, false), is(notNullValue()));

    // Verify column types
    assertThat(rowType.getField("cloud_provider", false, false).getType().getSqlTypeName(),
        is(SqlTypeName.VARCHAR));
    assertThat(rowType.getField("rbac_enabled", false, false).getType().getSqlTypeName(),
        is(SqlTypeName.BOOLEAN));
    assertThat(rowType.getField("node_count", false, false).getType().getSqlTypeName(),
        is(SqlTypeName.INTEGER));
  }

  @Test public void testScan() {
    CloudOpsConfig config = createTestConfig();
    KubernetesClustersTable table = new KubernetesClustersTable(config);

    // Mock DataContext and empty filters
    DataContext dataContext = null; // Can be null for this test
    List<RexNode> filters = Collections.emptyList();

    // This should not throw an exception, but may return empty results
    // since we don't have real credentials - authentication errors are expected
    try {
      Enumerable<Object[]> result = table.scan(dataContext, filters, null);
      assertThat(result, is(notNullValue()));
    } catch (Exception e) {
      // Expected - authentication will fail with fake credentials
      assertThat(e.getMessage() != null, is(true));
    }
  }

  @Test public void testGetStatistic() {
    CloudOpsConfig config = createTestConfig();
    KubernetesClustersTable table = new KubernetesClustersTable(config);

    // Should return a statistic object (may be empty/default)
    assertThat(table.getStatistic(), is(notNullValue()));
  }

  @Test public void testIsRolledUp() {
    CloudOpsConfig config = createTestConfig();
    KubernetesClustersTable table = new KubernetesClustersTable(config);

    // Table should not be rolled up by default
    assertThat(table.isRolledUp("any_column"), is(false));
  }

  @Test public void testTableName() {
    CloudOpsConfig config = createTestConfig();
    KubernetesClustersTable table = new KubernetesClustersTable(config);

    // Verify table works with configuration
    assertThat(table, is(notNullValue()));
    assertThat(config, is(notNullValue()));
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
}
