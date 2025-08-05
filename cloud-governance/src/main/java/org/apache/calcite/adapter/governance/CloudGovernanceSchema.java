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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Schema for Cloud Governance adapter.
 *
 * <p>Provides unified access to cloud resource data across multiple providers
 * (Azure, GCP, AWS) without subjective assessments. Returns raw facts about
 * resource configurations, security settings, and compliance status.
 */
public class CloudGovernanceSchema extends AbstractSchema {
  private final CloudGovernanceConfig config;

  public CloudGovernanceSchema(CloudGovernanceConfig config) {
    this.config = config;
  }

  @Override public boolean isMutable() {
    return false; // Read-only schema
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    // Core resource tables
    builder.put("compute_resources", new ComputeResourcesTable(config));
    builder.put("storage_resources", new StorageResourcesTable(config));
    builder.put("kubernetes_clusters", new KubernetesClustersTable(config));
    builder.put("container_registries", new ContainerRegistriesTable(config));
    builder.put("network_resources", new NetworkResourcesTable(config));
    builder.put("iam_resources", new IAMResourcesTable(config));
    builder.put("database_resources", new DatabaseResourcesTable(config));

    return builder.build();
  }

  /**
   * Provides access to the table map for metadata discovery.
   * Used by the metadata schema to generate information_schema and pg_catalog tables.
   */
  public Map<String, Table> getTableMapForMetadata() {
    return getTableMap();
  }
}
