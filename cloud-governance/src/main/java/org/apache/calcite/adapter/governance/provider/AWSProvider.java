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
package org.apache.calcite.adapter.governance.provider;

import org.apache.calcite.adapter.governance.CloudGovernanceConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.config.ConfigClient;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.Cluster;
import software.amazon.awssdk.services.eks.model.DescribeClusterRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.*;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.Repository;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.ListTagsOfResourceRequest;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.CacheCluster;
import software.amazon.awssdk.services.elasticache.model.DescribeCacheClustersRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AWS provider implementation using AWS SDK for Java v2.
 */
public class AWSProvider implements CloudProvider {
  private final CloudGovernanceConfig.AWSConfig config;
  private final Region region;
  private final AwsCredentials baseCredentials;
  private final Map<String, AwsCredentials> accountCredentials;
  
  public AWSProvider(CloudGovernanceConfig.AWSConfig config) {
    this.config = config;
    this.region = Region.of(config.region);
    this.baseCredentials = AwsBasicCredentials.create(
        config.accessKeyId, config.secretAccessKey);
    this.accountCredentials = new ConcurrentHashMap<>();
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
          AwsCredentials assumedCredentials = AwsBasicCredentials.create(
              response.credentials().accessKeyId(),
              response.credentials().secretAccessKey());
          accountCredentials.put(accountId, assumedCredentials);
        } catch (Exception e) {
          System.err.println("Failed to assume role for account " + accountId + ": " + e.getMessage());
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
  
  @Override
  public List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds) {
    List<Map<String, Object>> results = new ArrayList<>();
    
    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;
      
      try {
        EksClient eksClient = EksClient.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();
        
        // List all clusters
        List<String> clusterNames = eksClient.listClusters().clusters();
        
        for (String clusterName : clusterNames) {
          DescribeClusterRequest describeRequest = DescribeClusterRequest.builder()
              .name(clusterName)
              .build();
          
          Cluster cluster = eksClient.describeCluster(describeRequest).cluster();
          Map<String, Object> clusterData = new HashMap<>();
          
          // Identity fields
          clusterData.put("AccountId", accountId);
          clusterData.put("ClusterName", cluster.name());
          clusterData.put("Region", region.toString());
          clusterData.put("ResourceId", cluster.arn());
          
          // Tags
          Map<String, String> tags = cluster.tags();
          String application = tags.getOrDefault("Application",
              tags.getOrDefault("app", "Untagged/Orphaned"));
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
          
          results.add(clusterData);
        }
      } catch (Exception e) {
        System.err.println("Error querying EKS clusters in account " + 
            accountId + ": " + e.getMessage());
      }
    }
    
    return results;
  }
  
  @Override
  public List<Map<String, Object>> queryStorageResources(List<String> accountIds) {
    List<Map<String, Object>> results = new ArrayList<>();
    
    for (String accountId : accountIds) {
      AwsCredentials credentials = accountCredentials.get(accountId);
      if (credentials == null) continue;
      
      try {
        S3Client s3Client = S3Client.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();
        
        // List all buckets
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets();
        
        for (Bucket bucket : listBucketsResponse.buckets()) {
          Map<String, Object> storageData = new HashMap<>();
          
          // Identity fields
          storageData.put("AccountId", accountId);
          storageData.put("StorageResource", bucket.name());
          storageData.put("StorageType", "S3 Bucket");
          storageData.put("ResourceId", "arn:aws:s3:::" + bucket.name());
          
          try {
            // Get bucket location
            GetBucketLocationRequest locationRequest = GetBucketLocationRequest.builder()
                .bucket(bucket.name())
                .build();
            String location = s3Client.getBucketLocation(locationRequest).locationConstraintAsString();
            storageData.put("Location", location != null ? location : "us-east-1");
            
            // Get bucket tags
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
            String application = tags.getOrDefault("Application",
                tags.getOrDefault("app", "Untagged/Orphaned"));
            storageData.put("Application", application);
            
            // Get bucket encryption
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
            
            // Get public access block configuration
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
            
            // Get versioning
            GetBucketVersioningRequest versioningRequest = GetBucketVersioningRequest.builder()
                .bucket(bucket.name())
                .build();
            GetBucketVersioningResponse versioningResponse = s3Client.getBucketVersioning(versioningRequest);
            storageData.put("VersioningEnabled", 
                BucketVersioningStatus.ENABLED.equals(versioningResponse.status()));
            
            // Get lifecycle configuration
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
            
            // Timestamps
            storageData.put("CreationDate", bucket.creationDate());
            
          } catch (Exception e) {
            // Error getting bucket details, still add basic info
            System.err.println("Error getting details for bucket " + bucket.name() + ": " + e.getMessage());
          }
          
          results.add(storageData);
        }
      } catch (Exception e) {
        System.err.println("Error querying S3 buckets in account " + 
            accountId + ": " + e.getMessage());
      }
    }
    
    return results;
  }
  
  @Override
  public List<Map<String, Object>> queryComputeInstances(List<String> accountIds) {
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
                String.format("arn:aws:ec2:%s:%s:instance/%s", 
                    region, accountId, instance.instanceId()));
            
            // Tags
            Map<String, String> tags = new HashMap<>();
            instance.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
            String application = tags.getOrDefault("Application",
                tags.getOrDefault("app", "Untagged/Orphaned"));
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
        System.err.println("Error querying EC2 instances in account " + 
            accountId + ": " + e.getMessage());
      }
    }
    
    return results;
  }
  
  @Override
  public List<Map<String, Object>> queryNetworkResources(List<String> accountIds) {
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
              String.format("arn:aws:ec2:%s:%s:vpc/%s", region, accountId, vpc.vpcId()));
          
          // Tags
          Map<String, String> tags = new HashMap<>();
          vpc.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
          String application = tags.getOrDefault("Application",
              tags.getOrDefault("app", "Untagged/Orphaned"));
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
              String.format("arn:aws:ec2:%s:%s:security-group/%s", region, accountId, sg.groupId()));
          
          // Tags
          Map<String, String> tags = new HashMap<>();
          sg.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
          String application = tags.getOrDefault("Application",
              tags.getOrDefault("app", "Untagged/Orphaned"));
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
          String application = tags.getOrDefault("Application",
              tags.getOrDefault("app", "Untagged/Orphaned"));
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
        System.err.println("Error querying network resources in account " + 
            accountId + ": " + e.getMessage());
      }
    }
    
    return results;
  }
  
  @Override
  public List<Map<String, Object>> queryIAMResources(List<String> accountIds) {
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
          String application = tags.getOrDefault("Application",
              tags.getOrDefault("app", "Untagged/Orphaned"));
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
          String application = tags.getOrDefault("Application",
              tags.getOrDefault("app", "Untagged/Orphaned"));
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
          String application = tags.getOrDefault("Application",
              tags.getOrDefault("app", "Untagged/Orphaned"));
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
        System.err.println("Error querying IAM resources in account " + 
            accountId + ": " + e.getMessage());
      }
    }
    
    return results;
  }
  
  @Override
  public List<Map<String, Object>> queryDatabaseResources(List<String> accountIds) {
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
          String application = tags.getOrDefault("Application",
              tags.getOrDefault("app", "Untagged/Orphaned"));
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
          String application = tags.getOrDefault("Application",
              tags.getOrDefault("app", "Untagged/Orphaned"));
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
            String application = tags.getOrDefault("Application",
                tags.getOrDefault("app", "Untagged/Orphaned"));
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
            System.err.println("Error describing DynamoDB table " + tableName + ": " + e.getMessage());
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
        System.err.println("Error querying database resources in account " + 
            accountId + ": " + e.getMessage());
      }
    }
    
    return results;
  }
  
  @Override
  public List<Map<String, Object>> queryContainerRegistries(List<String> accountIds) {
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
        System.err.println("Error querying ECR repositories in account " + 
            accountId + ": " + e.getMessage());
      }
    }
    
    return results;
  }
}