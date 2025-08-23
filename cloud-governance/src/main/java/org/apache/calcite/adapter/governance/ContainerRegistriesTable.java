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

import org.apache.calcite.adapter.governance.provider.AWSProvider;
import org.apache.calcite.adapter.governance.provider.AzureProvider;
import org.apache.calcite.adapter.governance.provider.CloudProvider;
import org.apache.calcite.adapter.governance.provider.GCPProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table containing container registry information across cloud providers.
 */
public class ContainerRegistriesTable extends AbstractCloudGovernanceTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerRegistriesTable.class);
  public ContainerRegistriesTable(CloudGovernanceConfig config) {
    super(config);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("registry_name", SqlTypeName.VARCHAR)
        .add("application", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .add("resource_group", SqlTypeName.VARCHAR)
        .add("resource_id", SqlTypeName.VARCHAR)
        .add("registry_uri", SqlTypeName.VARCHAR)

        // Configuration facts
        .add("sku", SqlTypeName.VARCHAR)
        .add("admin_user_enabled", SqlTypeName.BOOLEAN)
        .add("public_access", SqlTypeName.VARCHAR)
        .add("image_scanning_enabled", SqlTypeName.BOOLEAN)
        .add("immutable_tags", SqlTypeName.BOOLEAN)

        // Security facts
        .add("encryption_type", SqlTypeName.VARCHAR)
        .add("encryption_key", SqlTypeName.VARCHAR)
        .add("quarantine_policy", SqlTypeName.VARCHAR)
        .add("trust_policy", SqlTypeName.VARCHAR)
        .add("retention_policy", SqlTypeName.VARCHAR)

        // Timestamps
        .add("created_at", SqlTypeName.TIMESTAMP)

        .build();
  }

  @Override protected List<Object[]> queryAzure(List<String> subscriptionIds) {
    List<Object[]> results = new ArrayList<>();

    try {
      CloudProvider azureProvider = new AzureProvider(config.azure);
      List<Map<String, Object>> registryResults = azureProvider.queryContainerRegistries(subscriptionIds);

      for (Map<String, Object> registry : registryResults) {
        results.add(new Object[]{
            "azure",
            registry.get("SubscriptionId"),
            registry.get("RegistryName"),
            registry.get("Application"),
            registry.get("Location"),
            registry.get("ResourceGroup"),
            registry.get("ResourceId"),
            null, // registry URI would need construction
            registry.get("RegistrySKU"),
            registry.get("AdminUserEnabled"),
            registry.get("PublicNetworkAccess"),
            false, // image scanning configured differently in Azure
            false, // immutable tags configured differently in Azure
            registry.get("Encryption"),
            null, // encryption key not in query
            registry.get("QuarantinePolicy"),
            registry.get("TrustPolicy"),
            registry.get("RetentionPolicy"),
            null  // created time not in query
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying Azure container registries: {}", e.getMessage());
    }

    return results;
  }

  @Override protected List<Object[]> queryGCP(List<String> projectIds) {
    List<Object[]> results = new ArrayList<>();

    try {
      CloudProvider gcpProvider = new GCPProvider(config.gcp);
      List<Map<String, Object>> registryResults = gcpProvider.queryContainerRegistries(projectIds);

      for (Map<String, Object> registry : registryResults) {
        results.add(new Object[]{
            "gcp",
            registry.get("ProjectId"),
            registry.get("RegistryName"),
            registry.get("Application"),
            registry.get("Location"),
            null, // resource group not applicable
            registry.get("ResourceId"),
            null, // registry URI would need construction
            registry.get("Format"), // GCP uses format instead of SKU
            false, // admin user not a GCP concept
            null, // public access controlled by IAM
            false, // image scanning configured separately
            "STANDARD_REPOSITORY".equals(registry.get("Mode")),
            registry.get("Encryption"),
            registry.get("KmsKey"),
            null, // quarantine policy not in GCP
            null, // trust policy not in GCP
            registry.get("CleanupPoliciesCount") != null &&
                ((Number) registry.get("CleanupPoliciesCount")).intValue() > 0 ?
                "Enabled" : "Disabled",
            CloudGovernanceDataConverter.convertValue(registry.get("CreateTime"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying GCP container registries: {}", e.getMessage());
    }

    return results;
  }

  @Override protected List<Object[]> queryAWS(List<String> accountIds) {
    List<Object[]> results = new ArrayList<>();

    try {
      CloudProvider awsProvider = new AWSProvider(config.aws);
      List<Map<String, Object>> registryResults = awsProvider.queryContainerRegistries(accountIds);

      for (Map<String, Object> registry : registryResults) {
        results.add(new Object[]{
            "aws",
            registry.get("AccountId"),
            registry.get("RepositoryName"),
            registry.get("Application"),
            registry.get("Region"),
            null, // resource group not applicable
            registry.get("ResourceId"),
            registry.get("RepositoryUri"),
            null, // SKU not applicable to ECR
            false, // admin user not applicable to ECR
            "Private", // ECR is always private
            registry.get("ImageScanningEnabled"),
            "IMMUTABLE".equals(registry.get("ImageTagMutability")),
            registry.get("EncryptionType"),
            registry.get("KmsKey"),
            null, // quarantine policy not in ECR
            null, // trust policy not in ECR
            null, // retention policy configured per lifecycle rules
            CloudGovernanceDataConverter.convertValue(registry.get("CreatedAt"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying AWS container registries: {}", e.getMessage());
    }

    return results;
  }
}
