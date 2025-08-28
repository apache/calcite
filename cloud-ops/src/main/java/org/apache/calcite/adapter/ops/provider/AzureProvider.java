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
package org.apache.calcite.adapter.ops.provider;

import org.apache.calcite.adapter.ops.CloudOpsConfig;
import org.apache.calcite.adapter.ops.util.CloudOpsCacheManager;
import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.resourcemanager.resourcegraph.ResourceGraphManager;
import com.azure.resourcemanager.resourcegraph.models.QueryRequest;
import com.azure.resourcemanager.resourcegraph.models.QueryRequestOptions;
import com.azure.resourcemanager.resourcegraph.models.QueryResponse;
import com.azure.resourcemanager.resourcegraph.models.ResultFormat;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Azure provider implementation using Azure Resource Graph with KQL queries.
 */
public class AzureProvider implements CloudProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzureProvider.class);

  private final CloudOpsConfig.AzureConfig config;
  private final ResourceGraphManager resourceGraphManager;
  private final ObjectMapper objectMapper;
  private final CloudOpsCacheManager cacheManager;

  public AzureProvider(CloudOpsConfig.AzureConfig config) {
    this.config = config;
    this.objectMapper = new ObjectMapper();

    TokenCredential credential = new ClientSecretCredentialBuilder()
        .tenantId(config.tenantId)
        .clientId(config.clientId)
        .clientSecret(config.clientSecret)
        .build();

    AzureProfile profile = new AzureProfile(config.tenantId, null, AzureEnvironment.AZURE);

    this.resourceGraphManager = ResourceGraphManager
        .authenticate(credential, profile);

    // Initialize cache manager with defaults (will be updated to use CloudOpsConfig)
    this.cacheManager = new CloudOpsCacheManager(5, false);
  }

  public AzureProvider(CloudOpsConfig.AzureConfig config, CloudOpsCacheManager cacheManager) {
    this.config = config;
    this.objectMapper = new ObjectMapper();
    this.cacheManager = cacheManager;

    TokenCredential credential = new ClientSecretCredentialBuilder()
        .tenantId(config.tenantId)
        .clientId(config.clientId)
        .clientSecret(config.clientSecret)
        .build();

    AzureProfile profile = new AzureProfile(config.tenantId, null, AzureEnvironment.AZURE);

    this.resourceGraphManager = ResourceGraphManager
        .authenticate(credential, profile);
  }

  private List<Map<String, Object>> executeKqlQuery(String kql, List<String> subscriptionIds) {
    // Build cache key for the query
    String cacheKey =
        CloudOpsCacheManager.buildCacheKey("azure", "kql", kql.hashCode(), subscriptionIds);

    return cacheManager.getOrCompute(cacheKey, () -> {
      List<Map<String, Object>> results = new ArrayList<>();

      try {
        QueryRequestOptions options = new QueryRequestOptions()
            .withResultFormat(ResultFormat.OBJECT_ARRAY)
            .withTop(1000);

        QueryRequest queryRequest = new QueryRequest()
            .withSubscriptions(subscriptionIds)
            .withQuery(kql)
            .withOptions(options);

        QueryResponse response = resourceGraphManager.resourceProviders()
            .resources(queryRequest);

        if (response.data() instanceof List) {
          @SuppressWarnings("unchecked")
          List<Object> dataList = (List<Object>) response.data();
          for (Object item : dataList) {
            if (item instanceof Map) {
              @SuppressWarnings("unchecked")
              Map<String, Object> row = (Map<String, Object>) item;
              results.add(row);
            }
          }
        }
      } catch (Exception e) {
        LOGGER.debug("Error executing KQL query: " + e.getMessage());
      }

      return results;
    });
  }

  @Override public List<Map<String, Object>> queryKubernetesClusters(List<String> subscriptionIds) {
    return queryKubernetesClusters(subscriptionIds, null);
  }

  /**
   * Query Kubernetes clusters with projection support.
   * Azure Resource Graph supports full projection via KQL project clause.
   */
  public List<Map<String, Object>> queryKubernetesClusters(List<String> subscriptionIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler) {
    return queryKubernetesClusters(subscriptionIds, projectionHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> subscriptionIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler) {
    return queryKubernetesClusters(subscriptionIds, projectionHandler, sortHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> subscriptionIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler,
                                                          @Nullable CloudOpsPaginationHandler paginationHandler) {
    return queryKubernetesClusters(subscriptionIds, projectionHandler, sortHandler, paginationHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> subscriptionIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler,
                                                          @Nullable CloudOpsPaginationHandler paginationHandler,
                                                          @Nullable CloudOpsFilterHandler filterHandler) {

    // Build comprehensive cache key including all optimization parameters
    String cacheKey =
        CloudOpsCacheManager.buildComprehensiveCacheKey("azure", "kubernetes_clusters", projectionHandler, sortHandler, paginationHandler, filterHandler, subscriptionIds);

    // Check if caching is beneficial for this query
    boolean shouldCache = CloudOpsCacheManager.shouldCache(filterHandler, paginationHandler);

    if (shouldCache) {
      return cacheManager.getOrCompute(
          cacheKey, () -> executeKubernetesClusterQuery(
          subscriptionIds, projectionHandler, sortHandler, paginationHandler, filterHandler));
    } else {
      // Execute directly without caching for highly specific queries
      return executeKubernetesClusterQuery(
          subscriptionIds, projectionHandler, sortHandler, paginationHandler, filterHandler);
    }
  }

  private List<Map<String, Object>> executeKubernetesClusterQuery(List<String> subscriptionIds,
                                                                 @Nullable CloudOpsProjectionHandler projectionHandler,
                                                                 @Nullable CloudOpsSortHandler sortHandler,
                                                                 @Nullable CloudOpsPaginationHandler paginationHandler,
                                                                 @Nullable CloudOpsFilterHandler filterHandler) {
    String kql = buildKubernetesClusterKql(projectionHandler, sortHandler, paginationHandler, filterHandler);

    if (LOGGER.isDebugEnabled()) {
      if (projectionHandler != null && !projectionHandler.isSelectAll()) {
        CloudOpsProjectionHandler.ProjectionMetrics metrics = projectionHandler.calculateMetrics();
        LOGGER.debug("Azure KQL with projection optimization: {}", metrics);
      }

      if (filterHandler != null && filterHandler.hasPushableFilters()) {
        CloudOpsFilterHandler.FilterMetrics metrics = filterHandler.calculateMetrics(true, 0);
        LOGGER.debug("Azure KQL with filter optimization: {}", metrics);
      }
    }

    return executeKqlQuery(kql, subscriptionIds);
  }

  /**
   * Get cache metrics for monitoring.
   */
  public CloudOpsCacheManager.CacheMetrics getCacheMetrics() {
    return cacheManager.getCacheMetrics();
  }

  /**
   * Invalidate cache entries for a specific subscription.
   */
  public void invalidateSubscriptionCache(String subscriptionId) {
    // Invalidate all cache entries that contain this subscription ID
    // This is a simplified approach - in production, you might want more granular invalidation
    cacheManager.invalidateAll();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Invalidated Azure cache for subscription: {}", subscriptionId);
    }
  }

  /**
   * Invalidate all cache entries.
   */
  public void invalidateAllCache() {
    cacheManager.invalidateAll();

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Invalidated all Azure cache entries");
    }
  }

  /**
   * Build KQL query for Kubernetes clusters with optional projection, sort, pagination, and filtering.
   */
  private String buildKubernetesClusterKql(@Nullable CloudOpsProjectionHandler projectionHandler,
                                          @Nullable CloudOpsSortHandler sortHandler,
                                          @Nullable CloudOpsPaginationHandler paginationHandler) {
    return buildKubernetesClusterKql(projectionHandler, sortHandler, paginationHandler, null);
  }

  private String buildKubernetesClusterKql(@Nullable CloudOpsProjectionHandler projectionHandler,
                                          @Nullable CloudOpsSortHandler sortHandler,
                                          @Nullable CloudOpsPaginationHandler paginationHandler,
                                          @Nullable CloudOpsFilterHandler filterHandler) {
    StringBuilder kql = new StringBuilder();
    kql.append("Resources\n")
       .append("| where type == 'microsoft.containerservice/managedclusters'\n")
       .append("| extend Application = case(\n")
       .append("    isnotempty(tags.Application), tags.Application,\n")
       .append("    isnotempty(tags.app), tags.app,\n")
       .append("    'Untagged/Orphaned'\n")
       .append(")\n")
       .append("| extend ClusterVersion = tostring(properties.kubernetesVersion)\n")
       .append("| extend NodeResourceGroup = tostring(properties.nodeResourceGroup)\n")
       .append("| extend PrivateCluster = tobool(properties.apiServerAccessProfile.enablePrivateCluster)\n")
       .append("| extend NetworkPlugin = tostring(properties.networkProfile.networkPlugin)\n")
       .append("| extend NetworkPolicy = tostring(properties.networkProfile.networkPolicy)\n")
       .append("| extend ServiceCIDR = tostring(properties.networkProfile.serviceCidr)\n")
       .append("| extend PodCIDR = tostring(properties.networkProfile.podCidr)\n")
       .append("| extend RBACEnabled = tobool(properties.enableRBAC)\n")
       .append("| extend AADEnabled = tobool(properties.aadProfile.managed)\n")
       .append("| extend AuthorizedIPRanges = array_length(properties.apiServerAccessProfile.authorizedIPRanges)\n")
       .append("| extend DiskEncryption = case(\n")
       .append("    isnotempty(properties.diskEncryptionSetID), 'Customer Managed Key',\n")
       .append("    'Platform Managed Key'\n")
       .append(")\n")
       .append("| extend NodePoolCount = array_length(properties.agentPoolProfiles)\n");

    // Add filter WHERE clause if specified
    if (filterHandler != null && filterHandler.hasPushableFilters()) {
      String whereClause = filterHandler.buildAzureKqlWhereClause();
      if (whereClause != null) {
        kql.append(whereClause).append("\n");
      }
    }

    // Add projection clause if specified
    if (projectionHandler != null && !projectionHandler.isSelectAll()) {
      String projectionClause = projectionHandler.buildAzureKqlProjectClause();
      if (projectionClause != null) {
        kql.append(projectionClause).append("\n");
      } else {
        // Fallback to default projection
        kql.append(getDefaultKubernetesProjectClause()).append("\n");
      }
    } else {
      // Default projection for SELECT * or no projection handler
      kql.append(getDefaultKubernetesProjectClause()).append("\n");
    }

    // Add sort clause if specified
    if (sortHandler != null && sortHandler.hasSort()) {
      String sortClause = sortHandler.buildAzureKqlOrderByClause();
      if (sortClause != null) {
        kql.append(sortClause);
        if (LOGGER.isDebugEnabled()) {
          CloudOpsSortHandler.SortMetrics metrics = sortHandler.calculateMetrics(true);
          LOGGER.debug("Azure KQL with sort optimization: {}", metrics);
        }
      } else {
        // Fallback to default sort
        kql.append("| order by Application, ClusterName");
      }
    } else {
      // Default sort for no sort handler
      kql.append("| order by Application, ClusterName");
    }

    // Add pagination clause if specified
    if (paginationHandler != null && paginationHandler.hasPagination()) {
      String paginationClause = paginationHandler.buildAzureKqlPaginationClause();
      if (paginationClause != null) {
        kql.append(" ").append(paginationClause);
        if (LOGGER.isDebugEnabled()) {
          CloudOpsPaginationHandler.PaginationMetrics metrics =
              paginationHandler.calculateMetrics(true, 10000); // Assume large dataset
          LOGGER.debug("Azure KQL with pagination optimization: {}", metrics);
        }
      }
    }

    return kql.toString();
  }

  /**
   * Get default project clause for Kubernetes clusters.
   */
  private String getDefaultKubernetesProjectClause() {
    return "| project\n"
  +
           "    SubscriptionId = subscriptionId,\n"
  +
           "    ClusterName = name,\n"
  +
           "    ResourceGroup = resourceGroup,\n"
  +
           "    Location = location,\n"
  +
           "    ResourceId = id,\n"
  +
           "    Application,\n"
  +
           "    ClusterVersion,\n"
  +
           "    NodePoolCount,\n"
  +
           "    RBACEnabled,\n"
  +
           "    AADEnabled,\n"
  +
           "    PrivateCluster,\n"
  +
           "    NetworkPlugin,\n"
  +
           "    NetworkPolicy,\n"
  +
           "    ServiceCidr,\n"
  +
           "    PodCidr,\n"
  +
           "    AuthorizedIPRanges,\n"
  +
           "    DiskEncryption";
  }

  @Override public List<Map<String, Object>> queryStorageResources(List<String> subscriptionIds) {
    String kql = "Resources\n"
  +
        "| where type in (\n"
  +
        "    'microsoft.storage/storageaccounts',\n"
  +
        "    'microsoft.sql/servers/databases',\n"
  +
        "    'microsoft.documentdb/databaseaccounts',\n"
  +
        "    'microsoft.compute/disks'\n"
  +
        ")\n"
  +
        "| extend Application = case(\n"
  +
        "    isnotempty(tags.Application), tags.Application,\n"
  +
        "    isnotempty(tags.app), tags.app,\n"
  +
        "    'Untagged/Orphaned'\n"
  +
        ")\n"
  +
        "| extend StorageType = case(\n"
  +
        "    type == 'microsoft.storage/storageaccounts', 'Storage Account',\n"
  +
        "    type == 'microsoft.sql/servers/databases', 'SQL Database',\n"
  +
        "    type == 'microsoft.documentdb/databaseaccounts', 'Cosmos DB',\n"
  +
        "    type == 'microsoft.compute/disks', 'Managed Disk',\n"
  +
        "    type\n"
  +
        ")\n"
  +
        "| extend EncryptionEnabled = case(\n"
  +
        "    type == 'microsoft.storage/storageaccounts', \n"
  +
        "        isnotnull(properties.encryption.services.blob.enabled),\n"
  +
        "    type == 'microsoft.sql/servers/databases',\n"
  +
        "        properties.transparentDataEncryption.status == 'Enabled',\n"
  +
        "    type == 'microsoft.documentdb/databaseaccounts',\n"
  +
        "        true,\n"
  +
        "    type == 'microsoft.compute/disks',\n"
  +
        "        isnotnull(properties.encryption),\n"
  +
        "    false\n"
  +
        ")\n"
  +
        "| extend EncryptionMethod = case(\n"
  +
        "    type == 'microsoft.storage/storageaccounts' and properties.encryption.keySource == 'Microsoft.Keyvault',\n"
  +
        "        'Customer Managed Key',\n"
  +
        "    type == 'microsoft.storage/storageaccounts',\n"
  +
        "        tostring(properties.encryption.keySource),\n"
  +
        "    type == 'microsoft.compute/disks' and isnotnull(properties.encryption.diskEncryptionSetId),\n"
  +
        "        'Customer Managed Key',\n"
  +
        "    EncryptionEnabled == true,\n"
  +
        "        'Service Managed Key',\n"
  +
        "    'None'\n"
  +
        ")\n"
  +
        "| extend HttpsOnly = case(\n"
  +
        "    type == 'microsoft.storage/storageaccounts',\n"
  +
        "        tobool(properties.supportsHttpsTrafficOnly),\n"
  +
        "    true\n"
  +
        ")\n"
  +
        "| extend MinimumTlsVersion = case(\n"
  +
        "    type == 'microsoft.storage/storageaccounts',\n"
  +
        "        tostring(properties.minimumTlsVersion),\n"
  +
        "    ''\n"
  +
        ")\n"
  +
        "| extend NetworkDefaultAction = case(\n"
  +
        "    type == 'microsoft.storage/storageaccounts',\n"
  +
        "        tostring(properties.networkAcls.defaultAction),\n"
  +
        "    ''\n"
  +
        ")\n"
  +
        "| extend PublicNetworkAccess = case(\n"
  +
        "    type == 'microsoft.storage/storageaccounts',\n"
  +
        "        tostring(properties.publicNetworkAccess),\n"
  +
        "    type == 'microsoft.sql/servers/databases',\n"
  +
        "        tostring(properties.publicNetworkAccess),\n"
  +
        "    ''\n"
  +
        ")\n"
  +
        "| project\n"
  +
        "    SubscriptionId = subscriptionId,\n"
  +
        "    StorageResource = name,\n"
  +
        "    StorageType,\n"
  +
        "    ResourceGroup = resourceGroup,\n"
  +
        "    Location = location,\n"
  +
        "    ResourceId = id,\n"
  +
        "    Application,\n"
  +
        "    EncryptionEnabled,\n"
  +
        "    EncryptionMethod,\n"
  +
        "    HttpsOnly,\n"
  +
        "    MinimumTlsVersion,\n"
  +
        "    NetworkDefaultAction,\n"
  +
        "    PublicNetworkAccess\n"
  +
        "| order by Application, StorageType, StorageResource";

    return executeKqlQuery(kql, subscriptionIds);
  }

  @Override public List<Map<String, Object>> queryComputeInstances(List<String> subscriptionIds) {
    String kql = "Resources\n"
  +
        "| where type == 'microsoft.compute/virtualmachines'\n"
  +
        "| extend Application = case(\n"
  +
        "    isnotempty(tags.Application), tags.Application,\n"
  +
        "    isnotempty(tags.app), tags.app,\n"
  +
        "    'Untagged/Orphaned'\n"
  +
        ")\n"
  +
        "| extend VMSize = tostring(properties.hardwareProfile.vmSize)\n"
  +
        "| extend OSType = tostring(properties.storageProfile.osDisk.osType)\n"
  +
        "| extend PowerState = tostring(properties.extended.instanceView.powerState.displayStatus)\n"
  +
        "| extend DiskEncryption = case(\n"
  +
        "    isnotnull(properties.storageProfile.osDisk.encryptionSettings.enabled) and\n"
  +
        "        tobool(properties.storageProfile.osDisk.encryptionSettings.enabled) == true,\n"
  +
        "        'Enabled',\n"
  +
        "    isnotnull(properties.storageProfile.osDisk.managedDisk.diskEncryptionSet),\n"
  +
        "        'Enabled',\n"
  +
        "    'Disabled'\n"
  +
        ")\n"
  +
        "| extend ManagedDisk = isnotnull(properties.storageProfile.osDisk.managedDisk)\n"
  +
        "| extend BootDiagnostics = tobool(properties.diagnosticsProfile.bootDiagnostics.enabled)\n"
  +
        "| extend HasPublicIP = false  // Would need to join with network interfaces and public IPs\n"
  +
        "| extend AvailabilitySet = tostring(properties.availabilitySet.id)\n"
  +
        "| extend AvailabilityZone = tostring(properties.zones[0])\n"
  +
        "| project\n"
  +
        "    SubscriptionId = subscriptionId,\n"
  +
        "    VMName = name,\n"
  +
        "    ResourceGroup = resourceGroup,\n"
  +
        "    Location = location,\n"
  +
        "    ResourceId = id,\n"
  +
        "    Application,\n"
  +
        "    VMSize,\n"
  +
        "    OSType,\n"
  +
        "    PowerState,\n"
  +
        "    DiskEncryption,\n"
  +
        "    ManagedDiskEnabled = ManagedDisk,\n"
  +
        "    BootDiagnostics,\n"
  +
        "    AvailabilitySet,\n"
  +
        "    AvailabilityZone\n"
  +
        "| order by Application, VMName";

    return executeKqlQuery(kql, subscriptionIds);
  }

  @Override public List<Map<String, Object>> queryNetworkResources(List<String> subscriptionIds) {
    String kql = "Resources\n"
  +
        "| where type in (\n"
  +
        "    'microsoft.network/virtualnetworks',\n"
  +
        "    'microsoft.network/networksecuritygroups',\n"
  +
        "    'microsoft.network/publicipaddresses',\n"
  +
        "    'microsoft.network/loadbalancers',\n"
  +
        "    'microsoft.network/applicationgateways'\n"
  +
        ")\n"
  +
        "| extend Application = case(\n"
  +
        "    isnotempty(tags.Application), tags.Application,\n"
  +
        "    isnotempty(tags.app), tags.app,\n"
  +
        "    'Untagged/Orphaned'\n"
  +
        ")\n"
  +
        "| extend NetworkResourceType = case(\n"
  +
        "    type == 'microsoft.network/virtualnetworks', 'Virtual Network',\n"
  +
        "    type == 'microsoft.network/networksecuritygroups', 'Network Security Group',\n"
  +
        "    type == 'microsoft.network/publicipaddresses', 'Public IP',\n"
  +
        "    type == 'microsoft.network/loadbalancers', 'Load Balancer',\n"
  +
        "    type == 'microsoft.network/applicationgateways', 'Application Gateway',\n"
  +
        "    type\n"
  +
        ")\n"
  +
        "| extend Configuration = case(\n"
  +
        "    type == 'microsoft.network/virtualnetworks',\n"
  +
        "        strcat('Address Space: ', tostring(properties.addressSpace.addressPrefixes)),\n"
  +
        "    type == 'microsoft.network/networksecuritygroups',\n"
  +
        "        strcat('Rules: ', tostring(array_length(properties.securityRules))),\n"
  +
        "    type == 'microsoft.network/publicipaddresses',\n"
  +
        "        strcat('Allocation: ', tostring(properties.publicIPAllocationMethod)),\n"
  +
        "    type == 'microsoft.network/loadbalancers',\n"
  +
        "        strcat('SKU: ', tostring(sku.name)),\n"
  +
        "    ''\n"
  +
        ")\n"
  +
        "| extend SecurityFindings = case(\n"
  +
        "    type == 'microsoft.network/networksecuritygroups' and \n"
  +
        "        array_length(properties.securityRules) == 0,\n"
  +
        "        'No security rules defined',\n"
  +
        "    type == 'microsoft.network/publicipaddresses' and\n"
  +
        "        properties.publicIPAllocationMethod == 'Static',\n"
  +
        "        'Static public IP',\n"
  +
        "    ''\n"
  +
        ")\n"
  +
        "| project\n"
  +
        "    SubscriptionId = subscriptionId,\n"
  +
        "    NetworkResource = name,\n"
  +
        "    NetworkResourceType,\n"
  +
        "    ResourceGroup = resourceGroup,\n"
  +
        "    Location = location,\n"
  +
        "    ResourceId = id,\n"
  +
        "    Application,\n"
  +
        "    Configuration,\n"
  +
        "    SecurityFindings\n"
  +
        "| order by Application, NetworkResourceType, NetworkResource";

    return executeKqlQuery(kql, subscriptionIds);
  }

  @Override public List<Map<String, Object>> queryIAMResources(List<String> subscriptionIds) {
    String kql = "Resources\n"
  +
        "| where type in (\n"
  +
        "    'microsoft.authorization/roleassignments',\n"
  +
        "    'microsoft.managedidentity/userassignedidentities',\n"
  +
        "    'microsoft.keyvault/vaults'\n"
  +
        ")\n"
  +
        "| extend Application = case(\n"
  +
        "    isnotempty(tags.Application), tags.Application,\n"
  +
        "    isnotempty(tags.app), tags.app,\n"
  +
        "    'Untagged/Orphaned'\n"
  +
        ")\n"
  +
        "| extend IAMResourceType = case(\n"
  +
        "    type == 'microsoft.authorization/roleassignments', 'Role Assignment',\n"
  +
        "    type == 'microsoft.managedidentity/userassignedidentities', 'Managed Identity',\n"
  +
        "    type == 'microsoft.keyvault/vaults', 'Key Vault',\n"
  +
        "    type\n"
  +
        ")\n"
  +
        "| extend Configuration = case(\n"
  +
        "    type == 'microsoft.authorization/roleassignments',\n"
  +
        "        strcat('Principal: ', tostring(properties.principalType)),\n"
  +
        "    type == 'microsoft.managedidentity/userassignedidentities',\n"
  +
        "        strcat('ClientId: ', tostring(properties.clientId)),\n"
  +
        "    type == 'microsoft.keyvault/vaults',\n"
  +
        "        strcat('SKU: ', tostring(properties.sku.name)),\n"
  +
        "    ''\n"
  +
        ")\n"
  +
        "| extend SecurityConfiguration = case(\n"
  +
        "    type == 'microsoft.keyvault/vaults',\n"
  +
        "        strcat('Purge Protection: ', \n"
  +
        "            case(tobool(properties.enablePurgeProtection) == true, 'Enabled', 'Disabled'),\n"
  +
        "            ' | Network: ', tostring(properties.networkAcls.defaultAction)),\n"
  +
        "    ''\n"
  +
        ")\n"
  +
        "| project\n"
  +
        "    SubscriptionId = subscriptionId,\n"
  +
        "    IAMResource = name,\n"
  +
        "    IAMResourceType,\n"
  +
        "    ResourceGroup = resourceGroup,\n"
  +
        "    Location = location,\n"
  +
        "    ResourceId = id,\n"
  +
        "    Application,\n"
  +
        "    Configuration,\n"
  +
        "    SecurityConfiguration\n"
  +
        "| order by Application, IAMResourceType, IAMResource";

    return executeKqlQuery(kql, subscriptionIds);
  }

  @Override public List<Map<String, Object>> queryDatabaseResources(List<String> subscriptionIds) {
    String kql = "Resources\n"
  +
        "| where type in (\n"
  +
        "    'microsoft.sql/servers',\n"
  +
        "    'microsoft.sql/servers/databases',\n"
  +
        "    'microsoft.documentdb/databaseaccounts',\n"
  +
        "    'microsoft.dbforpostgresql/servers',\n"
  +
        "    'microsoft.dbformysql/servers',\n"
  +
        "    'microsoft.cache/redis'\n"
  +
        ")\n"
  +
        "| extend Application = case(\n"
  +
        "    isnotempty(tags.Application), tags.Application,\n"
  +
        "    isnotempty(tags.app), tags.app,\n"
  +
        "    'Untagged/Orphaned'\n"
  +
        ")\n"
  +
        "| extend DatabaseType = case(\n"
  +
        "    type == 'microsoft.sql/servers', 'SQL Server',\n"
  +
        "    type == 'microsoft.sql/servers/databases', 'SQL Database',\n"
  +
        "    type == 'microsoft.documentdb/databaseaccounts', 'Cosmos DB',\n"
  +
        "    type == 'microsoft.dbforpostgresql/servers', 'PostgreSQL',\n"
  +
        "    type == 'microsoft.dbformysql/servers', 'MySQL',\n"
  +
        "    type == 'microsoft.cache/redis', 'Redis Cache',\n"
  +
        "    type\n"
  +
        ")\n"
  +
        "| extend SKU = case(\n"
  +
        "    type == 'microsoft.sql/servers/databases',\n"
  +
        "        strcat(tostring(sku.tier), ' - ', tostring(sku.name)),\n"
  +
        "    type == 'microsoft.documentdb/databaseaccounts',\n"
  +
        "        tostring(properties.databaseAccountOfferType),\n"
  +
        "    type == 'microsoft.cache/redis',\n"
  +
        "        strcat(tostring(sku.family), tostring(sku.capacity)),\n"
  +
        "    ''\n"
  +
        ")\n"
  +
        "| extend SecurityConfiguration = case(\n"
  +
        "    type == 'microsoft.sql/servers',\n"
  +
        "        strcat('Min TLS: ', tostring(properties.minimalTlsVersion)),\n"
  +
        "    type == 'microsoft.documentdb/databaseaccounts',\n"
  +
        "        strcat('Firewall: ', tostring(array_length(properties.ipRules))),\n"
  +
        "    type == 'microsoft.cache/redis',\n"
  +
        "        strcat('TLS: ', tostring(properties.minimumTlsVersion)),\n"
  +
        "    ''\n"
  +
        ")\n"
  +
        "| extend BackupConfiguration = case(\n"
  +
        "    type == 'microsoft.sql/servers/databases',\n"
  +
        "        tostring(properties.requestedBackupStorageRedundancy),\n"
  +
        "    type == 'microsoft.documentdb/databaseaccounts',\n"
  +
        "        tostring(properties.backupPolicy.type),\n"
  +
        "    ''\n"
  +
        ")\n"
  +
        "| project\n"
  +
        "    SubscriptionId = subscriptionId,\n"
  +
        "    DatabaseResource = name,\n"
  +
        "    DatabaseType,\n"
  +
        "    ResourceGroup = resourceGroup,\n"
  +
        "    Location = location,\n"
  +
        "    ResourceId = id,\n"
  +
        "    Application,\n"
  +
        "    SKU,\n"
  +
        "    SecurityConfiguration,\n"
  +
        "    BackupConfiguration\n"
  +
        "| order by Application, DatabaseType, DatabaseResource";

    return executeKqlQuery(kql, subscriptionIds);
  }

  @Override public List<Map<String, Object>> queryContainerRegistries(List<String> subscriptionIds) {
    String kql = "Resources\n"
  +
        "| where type == 'microsoft.containerregistry/registries'\n"
  +
        "| extend Application = case(\n"
  +
        "    isnotempty(tags.Application), tags.Application,\n"
  +
        "    isnotempty(tags.app), tags.app,\n"
  +
        "    'Untagged/Orphaned'\n"
  +
        ")\n"
  +
        "| extend RegistrySKU = tostring(sku.name)\n"
  +
        "| extend AdminUserEnabled = tobool(properties.adminUserEnabled)\n"
  +
        "| extend PublicNetworkAccess = tostring(properties.publicNetworkAccess)\n"
  +
        "| extend NetworkRuleSetDefaultAction = tostring(properties.networkRuleSet.defaultAction)\n"
  +
        "| extend ZoneRedundancy = tostring(properties.zoneRedundancy)\n"
  +
        "| extend DataEndpointEnabled = tobool(properties.dataEndpointEnabled)\n"
  +
        "| extend Encryption = case(\n"
  +
        "    isnotnull(properties.encryption.keyVaultProperties),\n"
  +
        "        'Customer Managed Key',\n"
  +
        "    'Service Managed Key'\n"
  +
        ")\n"
  +
        "| extend QuarantinePolicy = tostring(properties.policies.quarantinePolicy.status)\n"
  +
        "| extend TrustPolicy = tostring(properties.policies.trustPolicy.status)\n"
  +
        "| extend RetentionPolicy = tostring(properties.policies.retentionPolicy.status)\n"
  +
        "| extend SecurityConfiguration = strcat(\n"
  +
        "    'Admin User: ', case(AdminUserEnabled == true, 'Enabled', 'Disabled'),\n"
  +
        "    ' | Network: ', case(\n"
  +
        "        PublicNetworkAccess == 'Disabled', 'Private Only',\n"
  +
        "        NetworkRuleSetDefaultAction == 'Deny', 'Restricted',\n"
  +
        "        'Public'\n"
  +
        "),\n"
  +
        "    ' | Encryption: ', Encryption\n"
  +
        ")\n"
  +
        "| project\n"
  +
        "    SubscriptionId = subscriptionId,\n"
  +
        "    RegistryName = name,\n"
  +
        "    ResourceGroup = resourceGroup,\n"
  +
        "    Location = location,\n"
  +
        "    ResourceId = id,\n"
  +
        "    Application,\n"
  +
        "    RegistrySKU,\n"
  +
        "    AdminUserEnabled,\n"
  +
        "    PublicNetworkAccess,\n"
  +
        "    NetworkRuleSetDefaultAction,\n"
  +
        "    ZoneRedundancy,\n"
  +
        "    DataEndpointEnabled,\n"
  +
        "    Encryption,\n"
  +
        "    QuarantinePolicy,\n"
  +
        "    TrustPolicy,\n"
  +
        "    RetentionPolicy,\n"
  +
        "    SecurityConfiguration\n"
  +
        "| order by Application, RegistryName";

    return executeKqlQuery(kql, subscriptionIds);
  }
}
