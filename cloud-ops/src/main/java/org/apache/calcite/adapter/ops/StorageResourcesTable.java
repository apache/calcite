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

import org.apache.calcite.adapter.ops.provider.AWSProvider;
import org.apache.calcite.adapter.ops.provider.AzureProvider;
import org.apache.calcite.adapter.ops.provider.CloudProvider;
import org.apache.calcite.adapter.ops.provider.GCPProvider;
import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table containing storage resource information across cloud providers.
 * Returns raw facts without subjective assessments.
 */
public class StorageResourcesTable extends AbstractCloudOpsTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageResourcesTable.class);
  public StorageResourcesTable(CloudOpsConfig config) {
    super(config);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("resource_name", SqlTypeName.VARCHAR)
        .add("storage_type", SqlTypeName.VARCHAR)
        .add("application", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .add("resource_group", SqlTypeName.VARCHAR)
        .add("resource_id", SqlTypeName.VARCHAR)

        // Configuration facts
        .add("size_bytes", SqlTypeName.BIGINT)
        .add("storage_class", SqlTypeName.VARCHAR)
        .add("replication_type", SqlTypeName.VARCHAR)

        // Security facts
        .add("encryption_enabled", SqlTypeName.BOOLEAN)
        .add("encryption_type", SqlTypeName.VARCHAR)
        .add("encryption_key_type", SqlTypeName.VARCHAR)
        .add("public_access_enabled", SqlTypeName.BOOLEAN)
        .add("public_access_level", SqlTypeName.VARCHAR)
        .add("network_restrictions", SqlTypeName.VARCHAR)
        .add("https_only", SqlTypeName.BOOLEAN)

        // Data protection facts
        .add("versioning_enabled", SqlTypeName.BOOLEAN)
        .add("soft_delete_enabled", SqlTypeName.BOOLEAN)
        .add("soft_delete_retention_days", SqlTypeName.INTEGER)
        .add("backup_enabled", SqlTypeName.BOOLEAN)
        .add("lifecycle_rules_count", SqlTypeName.INTEGER)

        // Access control facts
        .add("access_tier", SqlTypeName.VARCHAR)
        .add("last_access_time", SqlTypeName.TIMESTAMP)
        .add("created_date", SqlTypeName.TIMESTAMP)
        .add("modified_date", SqlTypeName.TIMESTAMP)

        // Metadata
        .add("tags", SqlTypeName.VARCHAR) // JSON string

        .build();
  }

  @Override protected List<Object[]> queryAzure(List<String> subscriptionIds,
                                                CloudOpsProjectionHandler projectionHandler,
                                                CloudOpsSortHandler sortHandler,
                                               CloudOpsPaginationHandler paginationHandler,
                                               CloudOpsFilterHandler filterHandler) {
    List<Object[]> results = new ArrayList<>();

    try {
      // Use native Azure provider
      CloudProvider azureProvider = new AzureProvider(config.azure);
      List<Map<String, Object>> storageResults = azureProvider.queryStorageResources(subscriptionIds);

      // Convert to rows
      for (Map<String, Object> storage : storageResults) {
        String storageType = (String) storage.get("StorageType");
        String encryptionMethod = (String) storage.get("EncryptionMethod");

        results.add(new Object[]{
            "azure",
            storage.get("SubscriptionId"),
            storage.get("StorageResource"),
            storageType,
            storage.get("Application"),
            storage.get("Location"),
            storage.get("ResourceGroup"),
            storage.get("ResourceId"),
            null, // size_bytes - not in current query
            null, // storage_class - Azure specific
            getAzureReplicationType(storageType),
            storage.get("EncryptionEnabled"),
            encryptionMethod,
            encryptionMethod != null && encryptionMethod.contains("Customer") ?
                "customer-managed" : "service-managed",
            false, // public_access_enabled - would need additional query
            null, // public_access_level
            storage.get("NetworkDefaultAction"),
            storage.get("HttpsOnly"),
            null, // versioning_enabled - would need additional query
            null, // soft_delete_enabled
            null, // soft_delete_retention_days
            null, // backup_enabled
            null, // lifecycle_rules_count
            null, // access_tier
            null, // last_access_time
            null, // created_date
            null, // modified_date
            null  // tags
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying Azure storage resources: {}", e.getMessage());
    }

    return results;
  }

  @Override protected List<Object[]> queryGCP(List<String> projectIds,
                                              CloudOpsProjectionHandler projectionHandler,
                                              CloudOpsSortHandler sortHandler,
                                               CloudOpsPaginationHandler paginationHandler,
                                               CloudOpsFilterHandler filterHandler) {
    List<Object[]> results = new ArrayList<>();

    try {
      CloudProvider gcpProvider = new GCPProvider(config.gcp);
      List<Map<String, Object>> storageResults = gcpProvider.queryStorageResources(projectIds);

      for (Map<String, Object> storage : storageResults) {
        results.add(new Object[]{
            "gcp",
            storage.get("ProjectId"),
            storage.get("StorageResource"),
            storage.get("StorageType"),
            storage.get("Application"),
            storage.get("Location"),
            null, // resource_group - GCP doesn't have this concept
            storage.get("ResourceId"),
            null, // size_bytes - not in current query
            storage.get("StorageClass"),
            null, // replication_type
            storage.get("EncryptionEnabled"),
            storage.get("EncryptionEnabled") != null && (Boolean) storage.get("EncryptionEnabled") ?
                (storage.get("EncryptionKeyName") != null ? "customer-managed" : "service-managed") : "none",
            storage.get("EncryptionKeyName") != null ? "customer-managed" : "service-managed",
            false, // public_access_enabled - would need to check IAM
            storage.get("PublicAccessPrevention"),
            null, // network_restrictions
            true, // https_only - GCS always uses HTTPS
            storage.get("VersioningEnabled"),
            null, // soft_delete_enabled
            null, // soft_delete_retention_days
            storage.get("RetentionPolicy") != null && (Boolean) storage.get("RetentionPolicy"),
            storage.get("LifecycleRuleCount"),
            null, // access_tier
            null, // last_access_time
            CloudOpsDataConverter.convertValue(storage.get("TimeCreated"), SqlTypeName.TIMESTAMP),
            CloudOpsDataConverter.convertValue(storage.get("Updated"), SqlTypeName.TIMESTAMP),
            null  // tags - would need to convert labels map to JSON
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying GCP storage resources: {}", e.getMessage());
    }

    return results;
  }

  @Override protected List<Object[]> queryAWS(List<String> accountIds,
                                              CloudOpsProjectionHandler projectionHandler,
                                              CloudOpsSortHandler sortHandler,
                                               CloudOpsPaginationHandler paginationHandler,
                                               CloudOpsFilterHandler filterHandler) {
    List<Object[]> results = new ArrayList<>();

    try {
      // Use the optimized AWS provider with projection support
      AWSProvider awsProvider = new AWSProvider(config.aws);
      List<Map<String, Object>> storageResults = awsProvider.queryStorageResources(accountIds, 
          projectionHandler, sortHandler, paginationHandler, filterHandler);

      for (Map<String, Object> storage : storageResults) {
        // Handle null values gracefully for fields that may not have been fetched
        Boolean publicAccessBlocked = storage.get("PublicAccessBlocked") != null ? 
            (Boolean) storage.get("PublicAccessBlocked") : null;
        
        results.add(new Object[]{
            "aws",
            storage.get("AccountId"),
            storage.get("StorageResource"),
            storage.get("StorageType"),
            storage.get("Application"),
            storage.get("Location"),
            null, // resource_group - AWS doesn't use this concept for S3
            storage.get("ResourceId"),
            null, // size_bytes - would need CloudWatch metrics
            null, // storage_class - in S3 this is per object
            null, // replication_type - would need to check replication rules
            storage.get("EncryptionEnabled"),
            storage.get("EncryptionType"),
            storage.get("KmsKeyId") != null ? "customer-managed" : 
                (storage.get("EncryptionEnabled") != null ? "service-managed" : null),
            publicAccessBlocked != null ? !publicAccessBlocked : null,
            publicAccessBlocked != null ? (publicAccessBlocked ? "blocked" : "allowed") : null,
            null, // network_restrictions - would need to check bucket policy
            true, // https_only - S3 supports both but HTTPS is default
            storage.get("VersioningEnabled"),
            null, // soft_delete_enabled - S3 doesn't have soft delete
            null, // soft_delete_retention_days
            null, // backup_enabled - S3 doesn't have explicit backup
            storage.get("LifecycleRuleCount"),
            null, // access_tier - S3 doesn't have access tiers at bucket level
            null, // last_access_time - would need CloudWatch
            CloudOpsDataConverter.convertValue(storage.get("CreationDate"), SqlTypeName.TIMESTAMP),
            null, // modified_date - S3 doesn't track bucket modification
            null  // tags - would need to convert tag map to JSON
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying AWS storage resources: {}", e.getMessage());
    }

    return results;
  }

  private String getAzureReplicationType(String storageType) {
    // Simplified logic - in reality would parse from SKU
    switch (storageType) {
      case "Storage Account":
        return "LRS"; // Locally Redundant Storage
      case "Managed Disk":
        return "LRS";
      case "SQL Database":
        return "Geo-Replicated";
      case "Cosmos DB":
        return "Multi-Region";
      default:
        return "Unknown";
    }
  }
}
