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

import org.apache.calcite.adapter.governance.provider.AzureProvider;
import org.apache.calcite.adapter.governance.provider.GCPProvider;
import org.apache.calcite.adapter.governance.provider.AWSProvider;
import org.apache.calcite.adapter.governance.provider.CloudProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table containing compute resource (VM) information across cloud providers.
 */
public class ComputeResourcesTable extends AbstractCloudGovernanceTable {
  
  public ComputeResourcesTable(CloudGovernanceConfig config) {
    super(config);
  }
  
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("instance_id", SqlTypeName.VARCHAR)
        .add("instance_name", SqlTypeName.VARCHAR)
        .add("application", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .add("availability_zone", SqlTypeName.VARCHAR)
        .add("resource_group", SqlTypeName.VARCHAR)
        .add("resource_id", SqlTypeName.VARCHAR)
        
        // Configuration facts
        .add("instance_type", SqlTypeName.VARCHAR)
        .add("state", SqlTypeName.VARCHAR)
        .add("platform", SqlTypeName.VARCHAR)
        .add("architecture", SqlTypeName.VARCHAR)
        .add("virtualization_type", SqlTypeName.VARCHAR)
        
        // Network facts
        .add("public_ip", SqlTypeName.VARCHAR)
        .add("private_ip", SqlTypeName.VARCHAR)
        .add("vpc_id", SqlTypeName.VARCHAR)
        .add("subnet_id", SqlTypeName.VARCHAR)
        
        // Security facts
        .add("iam_role", SqlTypeName.VARCHAR)
        .add("security_groups", SqlTypeName.VARCHAR) // JSON array
        .add("disk_encryption_enabled", SqlTypeName.BOOLEAN)
        .add("monitoring_enabled", SqlTypeName.BOOLEAN)
        
        // Timestamps
        .add("launch_time", SqlTypeName.TIMESTAMP)
        
        .build();
  }
  
  @Override
  protected List<Object[]> queryAzure(List<String> subscriptionIds) {
    List<Object[]> results = new ArrayList<>();
    
    try {
      CloudProvider azureProvider = new AzureProvider(config.azure);
      List<Map<String, Object>> vmResults = azureProvider.queryComputeInstances(subscriptionIds);
      
      for (Map<String, Object> vm : vmResults) {
        results.add(new Object[]{
            "azure",
            vm.get("SubscriptionId"),
            vm.get("VMName"),
            vm.get("VMName"), // Azure doesn't have separate instance name
            vm.get("Application"),
            vm.get("Location"),
            vm.get("AvailabilityZone"),
            vm.get("ResourceGroup"),
            vm.get("ResourceId"),
            vm.get("VMSize"),
            vm.get("PowerState"),
            vm.get("OSType"),
            null, // architecture not in query
            null, // virtualization type not in query
            null, // public IP would need additional query
            null, // private IP would need additional query
            null, // VPC not applicable to Azure
            null, // subnet would need additional query
            null, // IAM role not applicable
            null, // security groups would need additional query
            "Enabled".equals(vm.get("DiskEncryption")),
            vm.get("BootDiagnostics"),
            null  // launch time not in query
        });
      }
    } catch (Exception e) {
      System.err.println("Error querying Azure compute instances: " + e.getMessage());
    }
    
    return results;
  }
  
  @Override
  protected List<Object[]> queryGCP(List<String> projectIds) {
    List<Object[]> results = new ArrayList<>();
    
    try {
      CloudProvider gcpProvider = new GCPProvider(config.gcp);
      List<Map<String, Object>> vmResults = gcpProvider.queryComputeInstances(projectIds);
      
      for (Map<String, Object> vm : vmResults) {
        results.add(new Object[]{
            "gcp",
            vm.get("ProjectId"),
            vm.get("VMName"),
            vm.get("VMName"), // GCP uses same name
            vm.get("Application"),
            vm.get("Zone"),
            vm.get("Zone"),
            null, // resource group not applicable
            vm.get("ResourceId"),
            vm.get("MachineType"),
            vm.get("Status"),
            null, // platform not directly available
            vm.get("CpuPlatform"),
            null, // virtualization type not exposed
            vm.get("HasExternalIP") != null && (Boolean) vm.get("HasExternalIP") ? "assigned" : null,
            null, // private IP would need additional query
            null, // VPC would need additional query
            null, // subnet would need additional query
            null, // IAM role would need additional query
            null, // security groups as JSON
            "Enabled".equals(vm.get("DiskEncryption")),
            false, // monitoring not in basic query
            vm.get("CreationTimestamp")
        });
      }
    } catch (Exception e) {
      System.err.println("Error querying GCP compute instances: " + e.getMessage());
    }
    
    return results;
  }
  
  @Override
  protected List<Object[]> queryAWS(List<String> accountIds) {
    List<Object[]> results = new ArrayList<>();
    
    try {
      CloudProvider awsProvider = new AWSProvider(config.aws);
      List<Map<String, Object>> vmResults = awsProvider.queryComputeInstances(accountIds);
      
      for (Map<String, Object> vm : vmResults) {
        results.add(new Object[]{
            "aws",
            vm.get("AccountId"),
            vm.get("InstanceId"),
            vm.get("InstanceName"),
            vm.get("Application"),
            vm.get("Region"),
            vm.get("AvailabilityZone"),
            null, // resource group not applicable
            vm.get("ResourceId"),
            vm.get("InstanceType"),
            vm.get("State"),
            vm.get("Platform"),
            vm.get("Architecture"),
            vm.get("VirtualizationType"),
            vm.get("PublicIpAddress"),
            vm.get("PrivateIpAddress"),
            vm.get("VpcId"),
            vm.get("SubnetId"),
            vm.get("IamInstanceProfile") != null ? vm.get("IamInstanceProfile").toString() : null,
            vm.get("SecurityGroups") != null ? vm.get("SecurityGroups").toString() : null,
            vm.get("EbsEncrypted"),
            vm.get("MonitoringEnabled"),
            vm.get("LaunchTime")
        });
      }
    } catch (Exception e) {
      System.err.println("Error querying AWS compute instances: " + e.getMessage());
    }
    
    return results;
  }
}