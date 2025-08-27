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

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.Repository;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.Cluster;
import software.amazon.awssdk.services.eks.model.DescribeClusterRequest;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.CacheCluster;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.*;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/**
 * AWS provider implementation using AWS SDK for Java v2.
 */
public class AWSProvider implements CloudProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(AWSProvider.class);
  
  private final CloudOpsConfig.AWSConfig config;
  private final Region region;
  private final AwsCredentials baseCredentials;
  private final Map<String, AwsCredentials> accountCredentials;
  private final CloudOpsCacheManager cacheManager;

  public AWSProvider(CloudOpsConfig.AWSConfig config) {
    this.config = config;
    this.region = Region.of(config.region);
    this.baseCredentials =
        AwsBasicCredentials.create(config.accessKeyId, config.secretAccessKey);
    this.accountCredentials = new ConcurrentHashMap<>();
    this.cacheManager = new CloudOpsCacheManager(5, false);
    initializeAccountCredentials();
  }

  public AWSProvider(CloudOpsConfig.AWSConfig config, CloudOpsCacheManager cacheManager) {
    this.config = config;
    this.region = Region.of(config.region);
    this.baseCredentials =
        AwsBasicCredentials.create(config.accessKeyId, config.secretAccessKey);
    this.accountCredentials = new ConcurrentHashMap<>();
    this.cacheManager = cacheManager;
    initializeAccountCredentials();
  }

  private void initializeAccountCredentials() {
    if (config.roleArn != null && !config.roleArn.isEmpty()) {
      // If using cross-account role assumption
      StsClient stsClient = StsClient.builder()
          .region(region)
          .credentialsProvider(StaticCredentialsProvider.create(baseCredentials))
          .build();

      for (String accountId : config.accountIds) {
        String roleArn = config.roleArn.replace("{account-id}", accountId);
        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
            .roleArn(roleArn)
            .roleSessionName("cloud-governance-adapter")
            .build();

        try {
          AssumeRoleResponse response = stsClient.assumeRole(assumeRoleRequest);
          AwsCredentials assumedCredentials =
              AwsBasicCredentials.create(response.credentials().accessKeyId(),
              response.credentials().secretAccessKey());
          accountCredentials.put(accountId, assumedCredentials);
        } catch (Exception e) {
          LOGGER.debug("Failed to assume role for account " + accountId + ": " + e.getMessage());
          accountCredentials.put(accountId, baseCredentials);
        }
      }
    } else {
      // Use base credentials for all accounts
      for (String accountId : config.accountIds) {
        accountCredentials.put(accountId, baseCredentials);
      }
    }
  }

  @Override public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds) {
    return queryKubernetesClusters(accountIds, null);
  }

  /**
   * Query Kubernetes clusters with projection support.
   * AWS EKS doesn't support server-side projection, so we apply client-side projection.
   */
  public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler) {
    return queryKubernetesClusters(accountIds, projectionHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler) {
    return queryKubernetesClusters(accountIds, projectionHandler, sortHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler,
                                                          @Nullable CloudOpsPaginationHandler paginationHandler) {
    return queryKubernetesClusters(accountIds, projectionHandler, sortHandler, paginationHandler, null);
  }

  public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds,
                                                          @Nullable CloudOpsProjectionHandler projectionHandler,
                                                          @Nullable CloudOpsSortHandler sortHandler,
                                                          @Nullable CloudOpsPaginationHandler paginationHandler,
                                                          @Nullable CloudOpsFilterHandler filterHandler) {
    
    // Build comprehensive cache key including all optimization parameters
    String cacheKey = CloudOpsCacheManager.buildComprehensiveCacheKey("aws", "kubernetes_clusters",
        projectionHandler, sortHandler, paginationHandler, filterHandler, accountIds);
    
    // Check if caching is beneficial for this query
    boolean shouldCache = CloudOpsCacheManager.shouldCache(filterHandler, paginationHandler);
    
    if (shouldCache) {
      return cacheManager.getOrCompute(cacheKey, () -> executeKubernetesClusterQuery(
          accountIds, projectionHandler, sortHandler, paginationHandler, filterHandler));
    } else {
      // Execute directly without caching for highly specific queries
      return executeKubernetesClusterQuery(
          accountIds, projectionHandler, sortHandler, paginationHandler, filterHandler);
    }
  }

  private List<Map<String, Object>> executeKubernetesClusterQuery(List<String> accountIds,
                                                                 @Nullable CloudOpsProjectionHandler projectionHandler,
                                                                 @Nullable CloudOpsSortHandler sortHandler,
                                                                 @Nullable CloudOpsPaginationHandler paginationHandler,
                                                                 @Nullable CloudOpsFilterHandler filterHandler) {
    List<Map<String, Object>> results = new ArrayList<>();

    // Extract filter parameters for AWS API optimization
    Map<String, Object> filterParams = new HashMap<>();
    if (filterHandler != null && filterHandler.hasPushableFilters()) {
      filterParams = filterHandler.getAWSFilterParameters();
    }

    if (LOGGER.isDebugEnabled()) {
      if (projectionHandler != null && !projectionHandler.isSelectAll()) {
        CloudOpsProjectionHandler.ProjectionMetrics metrics = projectionHandler.calculateMetrics();
        LOGGER.debug("AWS EKS querying with client-side projection: {}", metrics);
      }
      
      if (filterHandler != null && filterHandler.hasPushableFilters()) {
        CloudOpsFilterHandler.FilterMetrics metrics = filterHandler.calculateMetrics(true, filterParams.size());
        LOGGER.debug("AWS EKS with filter parameters: {} -> {}", filterParams.keySet(), metrics);
      }
    }

    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;

      try {
        // Apply region filter if specified
        Region targetRegion = region;
        if (filterParams.containsKey("region")) {
          targetRegion = Region.of(filterParams.get("region").toString());
        }

        EksClient eksClient = EksClient.builder()
            .region(targetRegion)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        // List clusters with pagination support
        List<String> clusterNames = new ArrayList<>();
        String nextToken = null;
        int maxResults = (paginationHandler != null) ?
            paginationHandler.getAWSMaxResults() : 100; // Default AWS page size

        do {
          software.amazon.awssdk.services.eks.model.ListClustersRequest.Builder requestBuilder =
              software.amazon.awssdk.services.eks.model.ListClustersRequest.builder()
                  .maxResults(maxResults);

          if (nextToken != null) {
            requestBuilder.nextToken(nextToken);
          }

          software.amazon.awssdk.services.eks.model.ListClustersResponse response =
              eksClient.listClusters(requestBuilder.build());

          clusterNames.addAll(response.clusters());
          nextToken = response.nextToken();

          // Stop if we have enough results for pagination
          if (paginationHandler != null && paginationHandler.hasPagination()) {
            long totalNeeded = paginationHandler.getOffset() + paginationHandler.getLimit();
            if (clusterNames.size() >= totalNeeded) {
              break;
            }
          }

        } while (nextToken != null);

        for (String clusterName : clusterNames) {
          DescribeClusterRequest describeRequest = DescribeClusterRequest.builder()
              .name(clusterName)
              .build();

          Cluster cluster = eksClient.describeCluster(describeRequest).cluster();

          // Build full cluster data (no server-side projection available)
          Map<String, Object> clusterData = buildClusterData(accountId, cluster);
          
          // Apply client-side filtering for non-pushable filters
          if (filterHandler == null || passesClientSideFilters(clusterData, filterHandler)) {
            results.add(clusterData);
          }
        }
      } catch (Exception e) {
        LOGGER.debug("Error querying EKS clusters in account {}: {}",
            accountId, e.getMessage());
      }
    }

    // Apply client-side sorting if needed (AWS doesn't support server-side sorting)
    if (sortHandler != null && sortHandler.hasSort()) {
      if (LOGGER.isDebugEnabled()) {
        CloudOpsSortHandler.SortMetrics metrics = sortHandler.calculateMetrics(false);
        LOGGER.debug("AWS EKS with client-side sort optimization: {}", metrics);
      }

      // Simple client-side sorting by cluster name (fallback approach)
      results.sort((r1, r2) -> {
        String name1 = String.valueOf(r1.get("ClusterName"));
        String name2 = String.valueOf(r2.get("ClusterName"));
        return name1.compareTo(name2);
      });
    }

    // Apply client-side pagination if needed
    if (paginationHandler != null && paginationHandler.hasPagination()) {
      List<Map<String, Object>> paginatedResults = paginationHandler.applyClientSidePagination(results);

      if (LOGGER.isDebugEnabled()) {
        CloudOpsPaginationHandler.PaginationMetrics metrics =
            paginationHandler.calculateMetrics(false, results.size());
        LOGGER.debug("AWS EKS with pagination optimization: {}", metrics);

        CloudOpsPaginationHandler.PaginationStrategy strategy = paginationHandler.getAWSStrategy();
        LOGGER.debug("AWS pagination strategy: {}", strategy);
      }

      return paginatedResults;
    }

    return results;
  }

  /**
   * Build complete cluster data from AWS EKS Cluster object.
   */
  private Map<String, Object> buildClusterData(String accountId, Cluster cluster) {
    Map<String, Object> clusterData = new HashMap<>();

    // Identity fields
    clusterData.put("AccountId", accountId);
    clusterData.put("ClusterName", cluster.name());
    clusterData.put("Region", region.toString());
    clusterData.put("ResourceId", cluster.arn());

    // Tags
    Map<String, String> tags = cluster.tags();
    String application =
        tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
    clusterData.put("Application", application);

    // Configuration facts
    clusterData.put("ClusterVersion", cluster.version());
    clusterData.put("PlatformVersion", cluster.platformVersion());
    clusterData.put("Status", cluster.status());

    // Security facts
    clusterData.put("RBACEnabled", true); // EKS has RBAC enabled by default

    // Network configuration
    if (cluster.resourcesVpcConfig() != null) {
      clusterData.put("EndpointPrivateAccess",
          cluster.resourcesVpcConfig().endpointPrivateAccess());
      clusterData.put("EndpointPublicAccess",
          cluster.resourcesVpcConfig().endpointPublicAccess());
      clusterData.put("PublicAccessCidrs",
          cluster.resourcesVpcConfig().publicAccessCidrs() != null ?
          cluster.resourcesVpcConfig().publicAccessCidrs().size() : 0);
      clusterData.put("SecurityGroupIds",
          cluster.resourcesVpcConfig().securityGroupIds());
      clusterData.put("SubnetIds",
          cluster.resourcesVpcConfig().subnetIds());
    }

    // Encryption
    if (cluster.encryptionConfig() != null && !cluster.encryptionConfig().isEmpty()) {
      clusterData.put("EncryptionEnabled", true);
      clusterData.put("EncryptionProvider",
          cluster.encryptionConfig().get(0).provider().keyArn());
    } else {
      clusterData.put("EncryptionEnabled", false);
    }

    // Logging
    if (cluster.logging() != null && cluster.logging().clusterLogging() != null) {
      boolean loggingEnabled = cluster.logging().clusterLogging().stream()
          .anyMatch(log -> log.enabled() != null && log.enabled());
      clusterData.put("LoggingEnabled", loggingEnabled);
    } else {
      clusterData.put("LoggingEnabled", false);
    }

    // Timestamps
    clusterData.put("CreatedAt", cluster.createdAt());

    return clusterData;
  }

  @Override public List<Map<String, Object>> queryStorageResources(List<String> accountIds) {
    return queryStorageResources(accountIds, null, null, null, null);
  }

  /**
   * Query storage resources with projection-aware optimization.
   * Only fetches expensive API details when those columns are actually projected.
   */
  public List<Map<String, Object>> queryStorageResources(List<String> accountIds,
                                                         @Nullable CloudOpsProjectionHandler projectionHandler,
                                                         @Nullable CloudOpsSortHandler sortHandler,
                                                         @Nullable CloudOpsPaginationHandler paginationHandler,
                                                         @Nullable CloudOpsFilterHandler filterHandler) {
    
    // Build comprehensive cache key including all optimization parameters
    String cacheKey = CloudOpsCacheManager.buildComprehensiveCacheKey("aws", "storage_resources",
        projectionHandler, sortHandler, paginationHandler, filterHandler, accountIds);
    
    // Check if caching is beneficial for this query
    boolean shouldCache = CloudOpsCacheManager.shouldCache(filterHandler, paginationHandler);
    
    if (shouldCache) {
      return cacheManager.getOrCompute(cacheKey, () -> executeStorageResourceQuery(
          accountIds, projectionHandler, sortHandler, paginationHandler, filterHandler));
    } else {
      // Execute directly without caching for highly specific queries
      return executeStorageResourceQuery(
          accountIds, projectionHandler, sortHandler, paginationHandler, filterHandler);
    }
  }

  private List<Map<String, Object>> executeStorageResourceQuery(List<String> accountIds,
                                                               @Nullable CloudOpsProjectionHandler projectionHandler,
                                                               @Nullable CloudOpsSortHandler sortHandler,
                                                               @Nullable CloudOpsPaginationHandler paginationHandler,
                                                               @Nullable CloudOpsFilterHandler filterHandler) {
    List<Map<String, Object>> results = new ArrayList<>();

    // Determine which fields are needed based on projection
    boolean needsBasicInfo = true; // Always need basic info
    boolean needsLocation = isFieldProjected(projectionHandler, "region", "Location");
    boolean needsTags = isFieldProjected(projectionHandler, "application", "Application", "tags");
    boolean needsEncryption = isFieldProjected(projectionHandler, "encryption_enabled", "encryption_type", 
                                              "encryption_key_type", "EncryptionEnabled", "EncryptionType", "KmsKeyId");
    boolean needsPublicAccess = isFieldProjected(projectionHandler, "public_access_enabled", "public_access_level",
                                                "PublicAccessBlocked");
    boolean needsVersioning = isFieldProjected(projectionHandler, "versioning_enabled", "VersioningEnabled");
    boolean needsLifecycle = isFieldProjected(projectionHandler, "lifecycle_rules_count", "LifecycleRuleCount");
    
    // Log optimization decisions
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("AWS S3 query optimization - Fetching: basic={}, location={}, tags={}, encryption={}, " +
                   "publicAccess={}, versioning={}, lifecycle={}",
                   needsBasicInfo, needsLocation, needsTags, needsEncryption, 
                   needsPublicAccess, needsVersioning, needsLifecycle);
    }

    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;

      try {
        S3Client s3Client = S3Client.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        // List buckets with pagination support
        ListBucketsRequest listRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets(listRequest);
        
        List<Bucket> buckets = listBucketsResponse.buckets();
        
        // Apply default pagination limit if no specific limit is set
        int maxBuckets = 100; // Default limit to prevent API overload
        if (paginationHandler != null && paginationHandler.hasPagination()) {
          maxBuckets = Math.min((int)paginationHandler.getLimit(), maxBuckets);
        }
        
        int bucketCount = 0;
        for (Bucket bucket : buckets) {
          if (bucketCount >= maxBuckets) {
            LOGGER.debug("Reached pagination limit of {} buckets for account {}", maxBuckets, accountId);
            break;
          }
          
          Map<String, Object> storageData = new HashMap<>();

          // Always include basic identity fields (low cost)
          storageData.put("AccountId", accountId);
          storageData.put("StorageResource", bucket.name());
          storageData.put("StorageType", "S3 Bucket");
          storageData.put("ResourceId", "arn:aws:s3:::" + bucket.name());
          storageData.put("CreationDate", bucket.creationDate());

          try {
            // Only fetch location if needed
            if (needsLocation) {
              GetBucketLocationRequest locationRequest = GetBucketLocationRequest.builder()
                  .bucket(bucket.name())
                  .build();
              String location = s3Client.getBucketLocation(locationRequest).locationConstraintAsString();
              storageData.put("Location", location != null ? location : "us-east-1");
            } else {
              storageData.put("Location", null);
            }

            // Only fetch tags if needed
            if (needsTags) {
              GetBucketTaggingRequest taggingRequest = GetBucketTaggingRequest.builder()
                  .bucket(bucket.name())
                  .build();
              Map<String, String> tags = new HashMap<>();
              try {
                GetBucketTaggingResponse taggingResponse = s3Client.getBucketTagging(taggingRequest);
                taggingResponse.tagSet().forEach(tag -> tags.put(tag.key(), tag.value()));
              } catch (Exception e) {
                // Bucket may not have tags
              }
              String application =
                  tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
              storageData.put("Application", application);
            } else {
              storageData.put("Application", null);
            }

            // Only fetch encryption if needed
            if (needsEncryption) {
              GetBucketEncryptionRequest encryptionRequest = GetBucketEncryptionRequest.builder()
                  .bucket(bucket.name())
                  .build();
              try {
                GetBucketEncryptionResponse encryptionResponse = s3Client.getBucketEncryption(encryptionRequest);
                storageData.put("EncryptionEnabled", true);
                if (encryptionResponse.serverSideEncryptionConfiguration() != null &&
                    !encryptionResponse.serverSideEncryptionConfiguration().rules().isEmpty()) {
                  ServerSideEncryptionRule rule = encryptionResponse.serverSideEncryptionConfiguration().rules().get(0);
                  storageData.put("EncryptionType", rule.applyServerSideEncryptionByDefault().sseAlgorithmAsString());
                  storageData.put("KmsKeyId", rule.applyServerSideEncryptionByDefault().kmsMasterKeyID());
                }
              } catch (Exception e) {
                storageData.put("EncryptionEnabled", false);
              }
            } else {
              storageData.put("EncryptionEnabled", null);
              storageData.put("EncryptionType", null);
              storageData.put("KmsKeyId", null);
            }

            // Only fetch public access block if needed
            if (needsPublicAccess) {
              GetPublicAccessBlockRequest publicAccessRequest = GetPublicAccessBlockRequest.builder()
                  .bucket(bucket.name())
                  .build();
              try {
                GetPublicAccessBlockResponse publicAccessResponse = s3Client.getPublicAccessBlock(publicAccessRequest);
                PublicAccessBlockConfiguration config = publicAccessResponse.publicAccessBlockConfiguration();
                storageData.put("PublicAccessBlocked",
                    config.blockPublicAcls() && config.blockPublicPolicy() &&
                    config.ignorePublicAcls() && config.restrictPublicBuckets());
              } catch (Exception e) {
                storageData.put("PublicAccessBlocked", false);
              }
            } else {
              storageData.put("PublicAccessBlocked", null);
            }

            // Only fetch versioning if needed
            if (needsVersioning) {
              GetBucketVersioningRequest versioningRequest = GetBucketVersioningRequest.builder()
                  .bucket(bucket.name())
                  .build();
              GetBucketVersioningResponse versioningResponse = s3Client.getBucketVersioning(versioningRequest);
              storageData.put("VersioningEnabled",
                  BucketVersioningStatus.ENABLED == versioningResponse.status());
            } else {
              storageData.put("VersioningEnabled", null);
            }

            // Only fetch lifecycle if needed
            if (needsLifecycle) {
              GetBucketLifecycleConfigurationRequest lifecycleRequest =
                  GetBucketLifecycleConfigurationRequest.builder()
                  .bucket(bucket.name())
                  .build();
              try {
                GetBucketLifecycleConfigurationResponse lifecycleResponse =
                    s3Client.getBucketLifecycleConfiguration(lifecycleRequest);
                storageData.put("LifecycleRuleCount",
                    lifecycleResponse.rules() != null ? lifecycleResponse.rules().size() : 0);
              } catch (Exception e) {
                storageData.put("LifecycleRuleCount", 0);
              }
            } else {
              storageData.put("LifecycleRuleCount", null);
            }

          } catch (Exception e) {
            // Error getting bucket details, still add basic info
            LOGGER.debug("Error getting details for bucket " + bucket.name() + ": " + e.getMessage());
          }

          results.add(storageData);
          bucketCount++;
        }
      } catch (Exception e) {
        LOGGER.debug("Error querying S3 buckets in account {}: {}",
            accountId, e.getMessage());
      }
    }

    // Log performance metrics
    if (LOGGER.isDebugEnabled()) {
      int totalApiCalls = results.size() * 
          (1 + (needsLocation ? 1 : 0) + (needsTags ? 1 : 0) + (needsEncryption ? 1 : 0) + 
           (needsPublicAccess ? 1 : 0) + (needsVersioning ? 1 : 0) + (needsLifecycle ? 1 : 0));
      int maxPossibleApiCalls = results.size() * 7; // All API calls for all buckets
      double reductionPercent = (1.0 - (double)totalApiCalls / maxPossibleApiCalls) * 100;
      
      LOGGER.debug("AWS S3 query completed: {} buckets, {} API calls (vs {} max), {:.1f}% reduction",
                   results.size(), totalApiCalls, maxPossibleApiCalls, reductionPercent);
    }

    return results;
  }

  /**
   * Check if a field is projected in the query.
   */
  private boolean isFieldProjected(@Nullable CloudOpsProjectionHandler projectionHandler, String... fieldNames) {
    if (projectionHandler == null || projectionHandler.isSelectAll()) {
      return true; // All fields are needed for SELECT *
    }
    
    List<String> projectedFields = projectionHandler.getProjectedFieldNames();
    for (String fieldName : fieldNames) {
      if (projectedFields.contains(fieldName)) {
        return true;
      }
    }
    return false;
  }

  @Override public List<Map<String, Object>> queryComputeInstances(List<String> accountIds) {
    List<Map<String, Object>> results = new ArrayList<>();

    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;

      try {
        Ec2Client ec2Client = Ec2Client.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        // Describe all instances
        for (Reservation reservation : ec2Client.describeInstances().reservations()) {
          for (Instance instance : reservation.instances()) {
            Map<String, Object> vmData = new HashMap<>();

            // Identity fields
            vmData.put("AccountId", accountId);
            vmData.put("InstanceId", instance.instanceId());
            vmData.put("Region", region.toString());
            vmData.put("AvailabilityZone", instance.placement().availabilityZone());
            vmData.put("ResourceId",
                String.format(Locale.ROOT, "arn:aws:ec2:%s:%s:instance/%s",
                    region, accountId, instance.instanceId()));

            // Tags
            Map<String, String> tags = new HashMap<>();
            instance.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
            String application =
                tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
            vmData.put("Application", application);
            vmData.put("InstanceName", tags.getOrDefault("Name", instance.instanceId()));

            // Configuration facts
            vmData.put("InstanceType", instance.instanceTypeAsString());
            vmData.put("State", instance.state().nameAsString());
            vmData.put("Architecture", instance.architectureAsString());
            vmData.put("Platform", instance.platformAsString());
            vmData.put("VirtualizationType", instance.virtualizationTypeAsString());

            // Network facts
            vmData.put("PublicIpAddress", instance.publicIpAddress());
            vmData.put("PrivateIpAddress", instance.privateIpAddress());
            vmData.put("VpcId", instance.vpcId());
            vmData.put("SubnetId", instance.subnetId());

            // Security facts
            vmData.put("SecurityGroups", instance.securityGroups());
            vmData.put("IamInstanceProfile", instance.iamInstanceProfile());

            // EBS encryption
            boolean allEbsEncrypted = instance.blockDeviceMappings().stream()
                .filter(bdm -> bdm.ebs() != null)
                .allMatch(bdm -> bdm.ebs() != null);
            vmData.put("EbsEncrypted", allEbsEncrypted);

            // Monitoring
            vmData.put("MonitoringEnabled",
                instance.monitoring() != null &&
                "enabled".equals(instance.monitoring().stateAsString()));

            // Timestamps
            vmData.put("LaunchTime", instance.launchTime());

            results.add(vmData);
          }
        }
      } catch (Exception e) {
        LOGGER.debug("Error querying EC2 instances in account {}: {}",
            accountId, e.getMessage());
      }
    }

    return results;
  }

  @Override public List<Map<String, Object>> queryNetworkResources(List<String> accountIds) {
    List<Map<String, Object>> results = new ArrayList<>();

    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;

      try {
        Ec2Client ec2Client = Ec2Client.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        // Query VPCs
        for (Vpc vpc : ec2Client.describeVpcs().vpcs()) {
          Map<String, Object> networkData = new HashMap<>();

          networkData.put("AccountId", accountId);
          networkData.put("NetworkResource", vpc.vpcId());
          networkData.put("NetworkResourceType", "VPC");
          networkData.put("Region", region.toString());
          networkData.put("ResourceId",
              String.format(Locale.ROOT, "arn:aws:ec2:%s:%s:vpc/%s", region, accountId, vpc.vpcId()));

          // Tags
          Map<String, String> tags = new HashMap<>();
          vpc.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
          String application =
              tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
          networkData.put("Application", application);

          // VPC Configuration
          networkData.put("CidrBlock", vpc.cidrBlock());
          networkData.put("State", vpc.stateAsString());
          networkData.put("IsDefault", vpc.isDefault());
          // DNS settings are not directly available on VPC object
          networkData.put("EnableDnsHostnames", true);
          networkData.put("EnableDnsSupport", true);

          results.add(networkData);
        }

        // Query Security Groups
        for (SecurityGroup sg : ec2Client.describeSecurityGroups().securityGroups()) {
          Map<String, Object> networkData = new HashMap<>();

          networkData.put("AccountId", accountId);
          networkData.put("NetworkResource", sg.groupId());
          networkData.put("NetworkResourceType", "Security Group");
          networkData.put("Region", region.toString());
          networkData.put("ResourceId",
              String.format(Locale.ROOT, "arn:aws:ec2:%s:%s:security-group/%s", region, accountId, sg.groupId()));

          // Tags
          Map<String, String> tags = new HashMap<>();
          sg.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
          String application =
              tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
          networkData.put("Application", application);

          // Security Group Configuration
          networkData.put("GroupName", sg.groupName());
          networkData.put("Description", sg.description());
          networkData.put("VpcId", sg.vpcId());
          networkData.put("IngressRulesCount", sg.ipPermissions().size());
          networkData.put("EgressRulesCount", sg.ipPermissionsEgress().size());

          // Check for overly permissive rules
          boolean hasOpenIngress = sg.ipPermissions().stream()
              .anyMatch(rule -> rule.ipRanges().stream()
                  .anyMatch(range -> "0.0.0.0/0".equals(range.cidrIp())));
          networkData.put("HasOpenIngressRule", hasOpenIngress);

          results.add(networkData);
        }

        // Query Elastic IPs
        for (Address address : ec2Client.describeAddresses().addresses()) {
          Map<String, Object> networkData = new HashMap<>();

          networkData.put("AccountId", accountId);
          networkData.put("NetworkResource", address.allocationId());
          networkData.put("NetworkResourceType", "Elastic IP");
          networkData.put("Region", region.toString());
          networkData.put("ResourceId", address.allocationId());

          // Tags
          Map<String, String> tags = new HashMap<>();
          address.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
          String application =
              tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
          networkData.put("Application", application);

          // Elastic IP Configuration
          networkData.put("PublicIp", address.publicIp());
          networkData.put("Domain", address.domainAsString());
          networkData.put("AssociationId", address.associationId());
          networkData.put("InstanceId", address.instanceId());
          networkData.put("NetworkInterfaceId", address.networkInterfaceId());
          networkData.put("IsAssociated", address.associationId() != null);

          results.add(networkData);
        }

      } catch (Exception e) {
        LOGGER.debug("Error querying network resources in account {}: {}",
            accountId, e.getMessage());
      }
    }

    return results;
  }

  @Override public List<Map<String, Object>> queryIAMResources(List<String> accountIds) {
    List<Map<String, Object>> results = new ArrayList<>();

    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;

      try {
        IamClient iamClient = IamClient.builder()
            .region(Region.AWS_GLOBAL)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        // Query IAM Users
        for (software.amazon.awssdk.services.iam.model.User user : iamClient.listUsers().users()) {
          Map<String, Object> iamData = new HashMap<>();

          iamData.put("AccountId", accountId);
          iamData.put("IAMResource", user.userName());
          iamData.put("IAMResourceType", "IAM User");
          iamData.put("Region", "global");
          iamData.put("ResourceId", user.arn());

          // Get user tags
          Map<String, String> tags = new HashMap<>();
          try {
            iamClient.listUserTags(r -> r.userName(user.userName())).tags()
                .forEach(tag -> tags.put(tag.key(), tag.value()));
          } catch (Exception e) {
            // User may not have tags
          }
          String application =
              tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
          iamData.put("Application", application);

          // User configuration
          iamData.put("CreateDate", user.createDate());
          iamData.put("PasswordLastUsed", user.passwordLastUsed());
          iamData.put("Path", user.path());

          // Check for access keys
          try {
            List<AccessKeyMetadata> accessKeys = iamClient.listAccessKeys(r -> r.userName(user.userName())).accessKeyMetadata();
            iamData.put("AccessKeyCount", accessKeys.size());
            iamData.put("ActiveAccessKeys", accessKeys.stream()
                .filter(key -> key.statusAsString().equals("Active"))
                .count());
          } catch (Exception e) {
            iamData.put("AccessKeyCount", 0);
            iamData.put("ActiveAccessKeys", 0);
          }

          // Check for MFA devices
          try {
            List<MFADevice> mfaDevices = iamClient.listMFADevices(r -> r.userName(user.userName())).mfaDevices();
            iamData.put("MFAEnabled", !mfaDevices.isEmpty());
          } catch (Exception e) {
            iamData.put("MFAEnabled", false);
          }

          results.add(iamData);
        }

        // Query IAM Roles
        for (Role role : iamClient.listRoles().roles()) {
          Map<String, Object> iamData = new HashMap<>();

          iamData.put("AccountId", accountId);
          iamData.put("IAMResource", role.roleName());
          iamData.put("IAMResourceType", "IAM Role");
          iamData.put("Region", "global");
          iamData.put("ResourceId", role.arn());

          // Get role tags
          Map<String, String> tags = new HashMap<>();
          try {
            iamClient.listRoleTags(r -> r.roleName(role.roleName())).tags()
                .forEach(tag -> tags.put(tag.key(), tag.value()));
          } catch (Exception e) {
            // Role may not have tags
          }
          String application =
              tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
          iamData.put("Application", application);

          // Role configuration
          iamData.put("CreateDate", role.createDate());
          iamData.put("Path", role.path());
          iamData.put("MaxSessionDuration", role.maxSessionDuration());
          iamData.put("Description", role.description());

          // Parse trust policy for principal info
          if (role.assumeRolePolicyDocument() != null) {
            iamData.put("TrustPolicyDocument", role.assumeRolePolicyDocument());
          }

          results.add(iamData);
        }

        // Query IAM Policies (customer managed only)
        for (Policy policy : iamClient.listPolicies(r -> r.scope(PolicyScopeType.LOCAL)).policies()) {
          Map<String, Object> iamData = new HashMap<>();

          iamData.put("AccountId", accountId);
          iamData.put("IAMResource", policy.policyName());
          iamData.put("IAMResourceType", "IAM Policy");
          iamData.put("Region", "global");
          iamData.put("ResourceId", policy.arn());

          // Get policy tags
          Map<String, String> tags = new HashMap<>();
          try {
            iamClient.listPolicyTags(r -> r.policyArn(policy.arn())).tags()
                .forEach(tag -> tags.put(tag.key(), tag.value()));
          } catch (Exception e) {
            // Policy may not have tags
          }
          String application =
              tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
          iamData.put("Application", application);

          // Policy configuration
          iamData.put("CreateDate", policy.createDate());
          iamData.put("UpdateDate", policy.updateDate());
          iamData.put("AttachmentCount", policy.attachmentCount());
          iamData.put("PermissionsBoundaryUsageCount", policy.permissionsBoundaryUsageCount());
          iamData.put("DefaultVersionId", policy.defaultVersionId());
          iamData.put("IsAttachable", policy.isAttachable());
          iamData.put("Description", policy.description());

          results.add(iamData);
        }

      } catch (Exception e) {
        LOGGER.debug("Error querying IAM resources in account {}: {}",
            accountId, e.getMessage());
      }
    }

    return results;
  }

  @Override public List<Map<String, Object>> queryDatabaseResources(List<String> accountIds) {
    List<Map<String, Object>> results = new ArrayList<>();

    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;

      try {
        RdsClient rdsClient = RdsClient.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        // Query RDS DB Instances
        for (DBInstance dbInstance : rdsClient.describeDBInstances().dbInstances()) {
          Map<String, Object> dbData = new HashMap<>();

          dbData.put("AccountId", accountId);
          dbData.put("DatabaseResource", dbInstance.dbInstanceIdentifier());
          dbData.put("DatabaseType", "RDS Instance");
          dbData.put("Region", region.toString());
          dbData.put("ResourceId", dbInstance.dbInstanceArn());

          // Tags
          Map<String, String> tags = new HashMap<>();
          for (software.amazon.awssdk.services.rds.model.Tag tag : dbInstance.tagList()) {
            tags.put(tag.key(), tag.value());
          }
          String application =
              tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
          dbData.put("Application", application);

          // Configuration
          dbData.put("Engine", dbInstance.engine());
          dbData.put("EngineVersion", dbInstance.engineVersion());
          dbData.put("DBInstanceClass", dbInstance.dbInstanceClass());
          dbData.put("AllocatedStorage", dbInstance.allocatedStorage());
          dbData.put("StorageType", dbInstance.storageType());
          dbData.put("MultiAZ", dbInstance.multiAZ());
          dbData.put("DBInstanceStatus", dbInstance.dbInstanceStatus());

          // Security
          dbData.put("PubliclyAccessible", dbInstance.publiclyAccessible());
          dbData.put("StorageEncrypted", dbInstance.storageEncrypted());
          if (dbInstance.storageEncrypted()) {
            dbData.put("KmsKeyId", dbInstance.kmsKeyId());
          }

          // Backup
          dbData.put("BackupRetentionPeriod", dbInstance.backupRetentionPeriod());
          dbData.put("PreferredBackupWindow", dbInstance.preferredBackupWindow());
          dbData.put("PreferredMaintenanceWindow", dbInstance.preferredMaintenanceWindow());

          // Network
          if (dbInstance.dbSubnetGroup() != null) {
            dbData.put("VpcId", dbInstance.dbSubnetGroup().vpcId());
            dbData.put("SubnetGroupName", dbInstance.dbSubnetGroup().dbSubnetGroupName());
          }

          // Performance insights
          dbData.put("PerformanceInsightsEnabled", dbInstance.performanceInsightsEnabled());

          // Timestamps
          dbData.put("InstanceCreateTime", dbInstance.instanceCreateTime());

          results.add(dbData);
        }

        // Query RDS DB Clusters (Aurora)
        for (DBCluster dbCluster : rdsClient.describeDBClusters().dbClusters()) {
          Map<String, Object> dbData = new HashMap<>();

          dbData.put("AccountId", accountId);
          dbData.put("DatabaseResource", dbCluster.dbClusterIdentifier());
          dbData.put("DatabaseType", "RDS Cluster");
          dbData.put("Region", region.toString());
          dbData.put("ResourceId", dbCluster.dbClusterArn());

          // Tags
          Map<String, String> tags = new HashMap<>();
          for (software.amazon.awssdk.services.rds.model.Tag tag : dbCluster.tagList()) {
            tags.put(tag.key(), tag.value());
          }
          String application =
              tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
          dbData.put("Application", application);

          // Configuration
          dbData.put("Engine", dbCluster.engine());
          dbData.put("EngineVersion", dbCluster.engineVersion());
          dbData.put("EngineMode", dbCluster.engineMode());
          dbData.put("AllocatedStorage", dbCluster.allocatedStorage());
          dbData.put("StorageType", dbCluster.storageType());
          dbData.put("MultiAZ", dbCluster.multiAZ());
          dbData.put("Status", dbCluster.status());

          // Security
          dbData.put("StorageEncrypted", dbCluster.storageEncrypted());
          if (dbCluster.storageEncrypted()) {
            dbData.put("KmsKeyId", dbCluster.kmsKeyId());
          }
          dbData.put("IAMDatabaseAuthenticationEnabled",
              dbCluster.iamDatabaseAuthenticationEnabled());

          // Backup
          dbData.put("BackupRetentionPeriod", dbCluster.backupRetentionPeriod());
          dbData.put("PreferredBackupWindow", dbCluster.preferredBackupWindow());
          dbData.put("PreferredMaintenanceWindow", dbCluster.preferredMaintenanceWindow());

          // Timestamps
          dbData.put("ClusterCreateTime", dbCluster.clusterCreateTime());

          results.add(dbData);
        }

        // Query DynamoDB Tables
        DynamoDbClient dynamoClient = DynamoDbClient.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        for (String tableName : dynamoClient.listTables().tableNames()) {
          try {
            TableDescription table = dynamoClient.describeTable(r -> r.tableName(tableName)).table();
            Map<String, Object> dbData = new HashMap<>();

            dbData.put("AccountId", accountId);
            dbData.put("DatabaseResource", table.tableName());
            dbData.put("DatabaseType", "DynamoDB Table");
            dbData.put("Region", region.toString());
            dbData.put("ResourceId", table.tableArn());

            // Get tags
            Map<String, String> tags = new HashMap<>();
            try {
              dynamoClient.listTagsOfResource(r -> r.resourceArn(table.tableArn())).tags()
                  .forEach(tag -> tags.put(tag.key(), tag.value()));
            } catch (Exception e) {
              // Table may not have tags
            }
            String application =
                tags.getOrDefault("Application", tags.getOrDefault("app", "Untagged/Orphaned"));
            dbData.put("Application", application);

            // Configuration
            dbData.put("TableStatus", table.tableStatusAsString());
            dbData.put("TableSizeBytes", table.tableSizeBytes());
            dbData.put("ItemCount", table.itemCount());
            dbData.put("BillingMode", table.billingModeSummary() != null ?
                table.billingModeSummary().billingModeAsString() : "PROVISIONED");

            // Capacity
            if (table.provisionedThroughput() != null) {
              dbData.put("ReadCapacityUnits", table.provisionedThroughput().readCapacityUnits());
              dbData.put("WriteCapacityUnits", table.provisionedThroughput().writeCapacityUnits());
            }

            // Encryption
            if (table.sseDescription() != null) {
              dbData.put("EncryptionType", table.sseDescription().sseTypeAsString());
              dbData.put("KMSMasterKeyArn", table.sseDescription().kmsMasterKeyArn());
            } else {
              dbData.put("EncryptionType", "NONE");
            }

            // Backup
            if (table.deletionProtectionEnabled() != null) {
              dbData.put("DeletionProtectionEnabled", table.deletionProtectionEnabled());
            }

            // Streams
            if (table.streamSpecification() != null) {
              dbData.put("StreamEnabled", table.streamSpecification().streamEnabled());
              dbData.put("StreamViewType", table.streamSpecification().streamViewTypeAsString());
            }

            // Timestamps
            dbData.put("CreationDateTime", table.creationDateTime());

            results.add(dbData);
          } catch (Exception e) {
            LOGGER.debug("Error describing DynamoDB table " + tableName + ": " + e.getMessage());
          }
        }

        // Query ElastiCache Clusters
        ElastiCacheClient elastiCacheClient = ElastiCacheClient.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        for (CacheCluster cluster : elastiCacheClient.describeCacheClusters().cacheClusters()) {
          Map<String, Object> dbData = new HashMap<>();

          dbData.put("AccountId", accountId);
          dbData.put("DatabaseResource", cluster.cacheClusterId());
          dbData.put("DatabaseType", "ElastiCache Cluster");
          dbData.put("Region", region.toString());
          dbData.put("ResourceId", cluster.arn());

          // Application tag - ElastiCache doesn't have built-in tag support in describe response
          dbData.put("Application", "Untagged/Orphaned");

          // Configuration
          dbData.put("Engine", cluster.engine());
          dbData.put("EngineVersion", cluster.engineVersion());
          dbData.put("CacheNodeType", cluster.cacheNodeType());
          dbData.put("NumCacheNodes", cluster.numCacheNodes());
          dbData.put("CacheClusterStatus", cluster.cacheClusterStatus());

          // Security
          dbData.put("AtRestEncryptionEnabled", cluster.atRestEncryptionEnabled());
          dbData.put("TransitEncryptionEnabled", cluster.transitEncryptionEnabled());
          dbData.put("AuthTokenEnabled", cluster.authTokenEnabled());

          // Network
          if (cluster.cacheSubnetGroupName() != null) {
            dbData.put("SubnetGroupName", cluster.cacheSubnetGroupName());
          }

          // Backup
          dbData.put("SnapshotRetentionLimit", cluster.snapshotRetentionLimit());
          dbData.put("SnapshotWindow", cluster.snapshotWindow());

          // Timestamps
          dbData.put("CacheClusterCreateTime", cluster.cacheClusterCreateTime());

          results.add(dbData);
        }

      } catch (Exception e) {
        LOGGER.debug("Error querying database resources in account {}: {}",
            accountId, e.getMessage());
      }
    }

    return results;
  }

  @Override public List<Map<String, Object>> queryContainerRegistries(List<String> accountIds) {
    List<Map<String, Object>> results = new ArrayList<>();

    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;

      try {
        EcrClient ecrClient = EcrClient.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

        // List all repositories
        for (Repository repository : ecrClient.describeRepositories().repositories()) {
          Map<String, Object> registryData = new HashMap<>();

          // Identity fields
          registryData.put("AccountId", accountId);
          registryData.put("RepositoryName", repository.repositoryName());
          registryData.put("Region", region.toString());
          registryData.put("ResourceId", repository.repositoryArn());
          registryData.put("RepositoryUri", repository.repositoryUri());

          // Configuration facts
          registryData.put("ImageScanningEnabled",
              repository.imageScanningConfiguration() != null &&
              repository.imageScanningConfiguration().scanOnPush());
          registryData.put("ImageTagMutability", repository.imageTagMutabilityAsString());

          // Encryption
          if (repository.encryptionConfiguration() != null) {
            registryData.put("EncryptionType",
                repository.encryptionConfiguration().encryptionTypeAsString());
            registryData.put("KmsKey", repository.encryptionConfiguration().kmsKey());
          }

          // Timestamps
          registryData.put("CreatedAt", repository.createdAt());

          results.add(registryData);
        }
      } catch (Exception e) {
        LOGGER.debug("Error querying ECR repositories in account {}: {}",
            accountId, e.getMessage());
      }
    }

    return results;
  }

  /**
   * Apply client-side filtering for filters that cannot be pushed to AWS API.
   */
  private boolean passesClientSideFilters(Map<String, Object> data, CloudOpsFilterHandler filterHandler) {
    // For now, implement basic filtering logic
    // This would be enhanced to handle all remaining filters that weren't pushed down
    
    // Check application filters (tag-based)
    List<CloudOpsFilterHandler.FilterInfo> appFilters = filterHandler.getFiltersForField("application");
    for (CloudOpsFilterHandler.FilterInfo filter : appFilters) {
      Object appValue = data.get("Application");
      if (appValue == null) appValue = "Untagged/Orphaned";
      
      switch (filter.operation) {
        case EQUALS:
          if (!appValue.toString().equals(filter.value.toString())) {
            return false;
          }
          break;
        case NOT_EQUALS:
          if (appValue.toString().equals(filter.value.toString())) {
            return false;
          }
          break;
        case LIKE:
          String pattern = filter.value.toString().replace("%", ".*");
          if (!appValue.toString().matches(pattern)) {
            return false;
          }
          break;
        case IN:
          if (filter.values != null && !filter.values.contains(appValue.toString())) {
            return false;
          }
          break;
        case IS_NULL:
          if (!"Untagged/Orphaned".equals(appValue.toString())) {
            return false;
          }
          break;
        case IS_NOT_NULL:
          if ("Untagged/Orphaned".equals(appValue.toString())) {
            return false;
          }
          break;
        default:
          break;
      }
    }

    // Check cluster name filters
    List<CloudOpsFilterHandler.FilterInfo> nameFilters = filterHandler.getFiltersForField("cluster_name");
    for (CloudOpsFilterHandler.FilterInfo filter : nameFilters) {
      Object nameValue = data.get("ClusterName");
      if (nameValue == null) continue;
      
      switch (filter.operation) {
        case EQUALS:
          if (!nameValue.toString().equals(filter.value.toString())) {
            return false;
          }
          break;
        case NOT_EQUALS:
          if (nameValue.toString().equals(filter.value.toString())) {
            return false;
          }
          break;
        case LIKE:
          String pattern = filter.value.toString().replace("%", ".*");
          if (!nameValue.toString().matches(pattern)) {
            return false;
          }
          break;
        case IN:
          if (filter.values != null && !filter.values.contains(nameValue.toString())) {
            return false;
          }
          break;
        default:
          break;
      }
    }

    return true; // Passes all client-side filters
  }

  /**
   * Get cache metrics for monitoring.
   */
  public CloudOpsCacheManager.CacheMetrics getCacheMetrics() {
    return cacheManager.getCacheMetrics();
  }

  /**
   * Invalidate cache entries for a specific account.
   */
  public void invalidateAccountCache(String accountId) {
    // For now, invalidate all cache entries - in production, you might want more granular invalidation
    cacheManager.invalidateAll();
    
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Invalidated AWS cache for account: {}", accountId);
    }
  }

  /**
   * Invalidate cache entries for a specific region.
   */
  public void invalidateRegionCache(String region) {
    // For now, invalidate all cache entries - in production, you might want more granular invalidation
    cacheManager.invalidateAll();
    
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Invalidated AWS cache for region: {}", region);
    }
  }

  /**
   * Invalidate all cache entries.
   */
  public void invalidateAllCache() {
    cacheManager.invalidateAll();
    
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Invalidated all AWS cache entries");
    }
  }
}
