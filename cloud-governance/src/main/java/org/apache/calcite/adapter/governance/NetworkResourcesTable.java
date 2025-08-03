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
 * Table containing network resource information across cloud providers.
 */
public class NetworkResourcesTable extends AbstractCloudGovernanceTable {
  
  public NetworkResourcesTable(CloudGovernanceConfig config) {
    super(config);
  }
  
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("network_resource", SqlTypeName.VARCHAR)
        .add("network_resource_type", SqlTypeName.VARCHAR)
        .add("application", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .add("resource_group", SqlTypeName.VARCHAR)
        .add("resource_id", SqlTypeName.VARCHAR)
        
        // Configuration facts
        .add("configuration", SqlTypeName.VARCHAR)
        .add("cidr_block", SqlTypeName.VARCHAR)
        .add("state", SqlTypeName.VARCHAR)
        .add("is_default", SqlTypeName.BOOLEAN)
        
        // Security facts
        .add("security_findings", SqlTypeName.VARCHAR)
        .add("has_open_ingress", SqlTypeName.BOOLEAN)
        .add("rule_count", SqlTypeName.INTEGER)
        
        // Metadata
        .add("tags", SqlTypeName.VARCHAR) // JSON
        
        .build();
  }
  
  @Override
  protected List<Object[]> queryAzure(List<String> subscriptionIds) {
    List<Object[]> results = new ArrayList<>();
    
    try {
      CloudProvider azureProvider = new AzureProvider(config.azure);
      List<Map<String, Object>> networkResults = azureProvider.queryNetworkResources(subscriptionIds);
      
      for (Map<String, Object> network : networkResults) {
        results.add(new Object[]{
            "azure",
            network.get("SubscriptionId"),
            network.get("NetworkResource"),
            network.get("NetworkResourceType"),
            network.get("Application"),
            network.get("Location"),
            network.get("ResourceGroup"),
            network.get("ResourceId"),
            network.get("Configuration"),
            null, // CIDR block would need parsing from configuration
            null, // state not in query
            null, // is_default not in query
            network.get("SecurityFindings"),
            null, // has_open_ingress would need rule analysis
            null, // rule_count would need parsing
            null  // tags not in query
        });
      }
    } catch (Exception e) {
      System.err.println("Error querying Azure network resources: " + e.getMessage());
    }
    
    return results;
  }
  
  @Override
  protected List<Object[]> queryGCP(List<String> projectIds) {
    List<Object[]> results = new ArrayList<>();
    
    try {
      CloudProvider gcpProvider = new GCPProvider(config.gcp);
      List<Map<String, Object>> networkResults = gcpProvider.queryNetworkResources(projectIds);
      
      for (Map<String, Object> network : networkResults) {
        results.add(new Object[]{
            "gcp",
            network.get("ProjectId"),
            network.get("NetworkResource"),
            network.get("NetworkResourceType"),
            network.get("Application"),
            network.get("Location"),
            null, // resource group not applicable
            network.get("ResourceId"),
            network.get("Configuration"),
            network.get("SourceRanges"), // for firewall rules
            null, // state not applicable
            null, // is_default would need additional info
            null, // security findings not computed
            null, // has_open_ingress would need rule analysis
            null, // rule_count not computed
            null  // tags would need conversion
        });
      }
    } catch (Exception e) {
      System.err.println("Error querying GCP network resources: " + e.getMessage());
    }
    
    return results;
  }
  
  @Override
  protected List<Object[]> queryAWS(List<String> accountIds) {
    List<Object[]> results = new ArrayList<>();
    
    try {
      CloudProvider awsProvider = new AWSProvider(config.aws);
      List<Map<String, Object>> networkResults = awsProvider.queryNetworkResources(accountIds);
      
      for (Map<String, Object> network : networkResults) {
        results.add(new Object[]{
            "aws",
            network.get("AccountId"),
            network.get("NetworkResource"),
            network.get("NetworkResourceType"),
            network.get("Application"),
            network.get("Region"),
            null, // resource group not applicable
            network.get("ResourceId"),
            network.get("GroupName") != null ? 
                "Name: " + network.get("GroupName") + ", Description: " + network.get("Description") : 
                network.get("Configuration"),
            network.get("CidrBlock"),
            network.get("State"),
            network.get("IsDefault"),
            null, // security findings not computed
            network.get("HasOpenIngressRule"),
            network.get("IngressRulesCount") != null ? network.get("IngressRulesCount") : 
                network.get("EgressRulesCount"),
            null  // tags would need conversion
        });
      }
    } catch (Exception e) {
      System.err.println("Error querying AWS network resources: " + e.getMessage());
    }
    
    return results;
  }
}