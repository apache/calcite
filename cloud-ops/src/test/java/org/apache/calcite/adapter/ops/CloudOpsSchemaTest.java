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

import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link CloudOpsSchema}.
 */
@Tag("unit")
public class CloudOpsSchemaTest {

  @Test public void testSchemaCreation() {
    CloudOpsConfig config = createTestConfig();
    CloudOpsSchema schema = new CloudOpsSchema(config);

    assertThat(schema, is(notNullValue()));
  }

  @Test public void testGetTableMap() {
    CloudOpsConfig config = createTestConfig();
    CloudOpsSchema schema = new CloudOpsSchema(config);

    Map<String, Table> tables = schema.getTableMap();

    // Verify all expected tables are present
    assertThat(tables.containsKey("kubernetes_clusters"), is(true));
    assertThat(tables.containsKey("storage_resources"), is(true));
    assertThat(tables.containsKey("compute_resources"), is(true));
    assertThat(tables.containsKey("network_resources"), is(true));
    assertThat(tables.containsKey("iam_resources"), is(true));
    assertThat(tables.containsKey("database_resources"), is(true));
    assertThat(tables.containsKey("container_registries"), is(true));

    // Verify table types
    assertThat(tables.get("kubernetes_clusters"), instanceOf(KubernetesClustersTable.class));
    assertThat(tables.get("storage_resources"), instanceOf(StorageResourcesTable.class));
    assertThat(tables.get("compute_resources"), instanceOf(ComputeResourcesTable.class));
    assertThat(tables.get("network_resources"), instanceOf(NetworkResourcesTable.class));
    assertThat(tables.get("iam_resources"), instanceOf(IAMResourcesTable.class));
    assertThat(tables.get("database_resources"), instanceOf(DatabaseResourcesTable.class));
    assertThat(tables.get("container_registries"), instanceOf(ContainerRegistriesTable.class));
  }

  // Sub-schema test removed - getSubSchemaMap() is protected

  @Test public void testIsMutable() {
    CloudOpsConfig config = createTestConfig();
    CloudOpsSchema schema = new CloudOpsSchema(config);

    // Schema should be immutable (read-only)
    assertThat(schema.isMutable(), is(false));
  }

  private CloudOpsConfig createTestConfig() {
    // Only use real credentials from local properties file
    CloudOpsConfig config = CloudOpsTestUtils.loadTestConfig();
    if (config == null) {
      throw new IllegalStateException("Real credentials required from local-test.properties file");
    }
    return config;
  }
}
