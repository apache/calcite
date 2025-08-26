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
 * Table containing IAM resource information across cloud providers.
 */
public class IAMResourcesTable extends AbstractCloudOpsTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IAMResourcesTable.class);
  public IAMResourcesTable(CloudOpsConfig config) {
    super(config);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("iam_resource", SqlTypeName.VARCHAR)
        .add("iam_resource_type", SqlTypeName.VARCHAR)
        .add("application", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .add("resource_group", SqlTypeName.VARCHAR)
        .add("resource_id", SqlTypeName.VARCHAR)

        // Configuration facts
        .add("configuration", SqlTypeName.VARCHAR)
        .add("security_configuration", SqlTypeName.VARCHAR)

        // IAM specific facts
        .add("principal_type", SqlTypeName.VARCHAR)
        .add("email", SqlTypeName.VARCHAR)
        .add("is_active", SqlTypeName.BOOLEAN)
        .add("mfa_enabled", SqlTypeName.BOOLEAN)
        .add("access_key_count", SqlTypeName.INTEGER)
        .add("active_access_keys", SqlTypeName.INTEGER)

        // Timestamps
        .add("create_date", SqlTypeName.TIMESTAMP)
        .add("password_last_used", SqlTypeName.TIMESTAMP)

        .build();
  }

  @Override protected List<Object[]> queryAzure(List<String> subscriptionIds,
                                                CloudOpsProjectionHandler projectionHandler,
                                                CloudOpsSortHandler sortHandler,
                                               CloudOpsPaginationHandler paginationHandler,
                                               CloudOpsFilterHandler filterHandler) {
    List<Object[]> results = new ArrayList<>();

    try {
      CloudProvider azureProvider = new AzureProvider(config.azure);
      List<Map<String, Object>> iamResults = azureProvider.queryIAMResources(subscriptionIds);

      for (Map<String, Object> iam : iamResults) {
        results.add(new Object[]{
            "azure",
            iam.get("SubscriptionId"),
            iam.get("IAMResource"),
            iam.get("IAMResourceType"),
            iam.get("Application"),
            iam.get("Location"),
            iam.get("ResourceGroup"),
            iam.get("ResourceId"),
            iam.get("Configuration"),
            iam.get("SecurityConfiguration"),
            null, // principal type parsed from configuration
            null, // email not applicable
            true, // Azure resources are active by default
            null, // MFA not in basic query
            null, // access key count not applicable
            null, // active access keys not applicable
            null, // create date not in query
            null  // password last used not applicable
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying Azure IAM resources: {}", e.getMessage());
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
      List<Map<String, Object>> iamResults = gcpProvider.queryIAMResources(projectIds);

      for (Map<String, Object> iam : iamResults) {
        String resourceType = (String) iam.get("IAMResourceType");
        Boolean isActive = true;
        if ("ServiceAccount".equals(resourceType)) {
          isActive = !(Boolean) iam.getOrDefault("Disabled", false);
        }

        results.add(new Object[]{
            "gcp",
            iam.get("ProjectId"),
            iam.get("IAMResource"),
            iam.get("IAMResourceType"),
            iam.get("Application"),
            iam.get("Location"),
            null, // resource group not applicable
            iam.get("ResourceId"),
            iam.get("DisplayName") != null ? "Display: " + iam.get("DisplayName") : null,
            null, // security configuration not computed
            null, // principal type not exposed
            iam.get("Email"),
            isActive,
            null, // MFA not tracked at resource level
            null, // access key count not applicable
            null, // active access keys not applicable
            CloudOpsDataConverter.convertValue(iam.get("CreateTime"), SqlTypeName.TIMESTAMP),
            null  // password last used not applicable
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying GCP IAM resources: {}", e.getMessage());
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
      CloudProvider awsProvider = new AWSProvider(config.aws);
      List<Map<String, Object>> iamResults = awsProvider.queryIAMResources(accountIds);

      for (Map<String, Object> iam : iamResults) {
        String resourceType = (String) iam.get("IAMResourceType");

        results.add(new Object[]{
            "aws",
            iam.get("AccountId"),
            iam.get("IAMResource"),
            iam.get("IAMResourceType"),
            iam.get("Application"),
            iam.get("Region"),
            null, // resource group not applicable
            iam.get("ResourceId"),
            iam.get("Path") != null ? "Path: " + iam.get("Path") :
                iam.get("Description") != null ? "Description: " + iam.get("Description") : null,
            null, // security configuration not computed
            null, // principal type in users/roles
            null, // email not exposed
            !"IAM Policy".equals(resourceType), // policies aren't active/inactive
            iam.get("MFAEnabled"),
            iam.get("AccessKeyCount") != null ? ((Number) iam.get("AccessKeyCount")).intValue() : null,
            iam.get("ActiveAccessKeys") != null ?
                ((Number) iam.get("ActiveAccessKeys")).intValue() : null,
            CloudOpsDataConverter.convertValue(iam.get("CreateDate"), SqlTypeName.TIMESTAMP),
            CloudOpsDataConverter.convertValue(iam.get("PasswordLastUsed"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying AWS IAM resources: {}", e.getMessage());
    }

    return results;
  }
}
