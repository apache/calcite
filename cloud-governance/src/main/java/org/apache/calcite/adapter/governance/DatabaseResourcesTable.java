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
 * Table containing database resource information across cloud providers.
 */
public class DatabaseResourcesTable extends AbstractCloudGovernanceTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseResourcesTable.class);
  public DatabaseResourcesTable(CloudGovernanceConfig config) {
    super(config);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Identity fields
        .add("cloud_provider", SqlTypeName.VARCHAR)
        .add("account_id", SqlTypeName.VARCHAR)
        .add("database_resource", SqlTypeName.VARCHAR)
        .add("database_type", SqlTypeName.VARCHAR)
        .add("application", SqlTypeName.VARCHAR)
        .add("region", SqlTypeName.VARCHAR)
        .add("resource_group", SqlTypeName.VARCHAR)
        .add("resource_id", SqlTypeName.VARCHAR)

        // Configuration facts
        .add("engine", SqlTypeName.VARCHAR)
        .add("engine_version", SqlTypeName.VARCHAR)
        .add("instance_class", SqlTypeName.VARCHAR)
        .add("allocated_storage", SqlTypeName.INTEGER)
        .add("multi_az", SqlTypeName.BOOLEAN)
        .add("status", SqlTypeName.VARCHAR)

        // Security facts
        .add("publicly_accessible", SqlTypeName.BOOLEAN)
        .add("encrypted", SqlTypeName.BOOLEAN)
        .add("encryption_key", SqlTypeName.VARCHAR)
        .add("tls_version", SqlTypeName.VARCHAR)

        // Backup facts
        .add("backup_retention_days", SqlTypeName.INTEGER)
        .add("backup_window", SqlTypeName.VARCHAR)

        // Timestamps
        .add("create_time", SqlTypeName.TIMESTAMP)

        .build();
  }

  @Override protected List<Object[]> queryAzure(List<String> subscriptionIds) {
    List<Object[]> results = new ArrayList<>();

    try {
      CloudProvider azureProvider = new AzureProvider(config.azure);
      List<Map<String, Object>> dbResults = azureProvider.queryDatabaseResources(subscriptionIds);

      for (Map<String, Object> db : dbResults) {
        results.add(new Object[]{
            "azure",
            db.get("SubscriptionId"),
            db.get("DatabaseResource"),
            db.get("DatabaseType"),
            db.get("Application"),
            db.get("Location"),
            db.get("ResourceGroup"),
            db.get("ResourceId"),
            null, // engine parsed from database type
            null, // engine version not in query
            db.get("SKU"),
            null, // allocated storage not in query
            null, // multi-AZ concept different in Azure
            null, // status not in query
            null, // publicly accessible would need additional query
            null, // encrypted status would need parsing
            null, // encryption key not in query
            parseMinTlsVersion(db.get("SecurityConfiguration")),
            null, // backup retention would need parsing
            db.get("BackupConfiguration"),
            null  // create time not in query
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying Azure database resources: {}", e.getMessage());
    }

    return results;
  }

  @Override protected List<Object[]> queryGCP(List<String> projectIds) {
    List<Object[]> results = new ArrayList<>();

    try {
      CloudProvider gcpProvider = new GCPProvider(config.gcp);
      List<Map<String, Object>> dbResults = gcpProvider.queryDatabaseResources(projectIds);

      for (Map<String, Object> db : dbResults) {
        results.add(new Object[]{
            "gcp",
            db.get("ProjectId"),
            db.get("DatabaseResource"),
            db.get("DatabaseType"),
            db.get("Application"),
            db.get("Location"),
            null, // resource group not applicable
            db.get("ResourceId"),
            db.get("DatabaseVersion"),
            db.get("DatabaseVersion"),
            db.get("Tier"),
            db.get("MemorySizeGb") != null ?
                ((Number) db.get("MemorySizeGb")).intValue() * 1024 : null, // Convert GB to MB
            false, // GCP uses regional replication differently
            db.get("State"),
            db.get("PublicNetworkAccess") != null,
            db.get("RequireSSL") != null && (Boolean) db.get("RequireSSL"),
            null, // encryption key not exposed
            db.get("RequireSSL") != null && (Boolean) db.get("RequireSSL") ? "1.2" : null,
            db.get("BackupEnabled") != null && (Boolean) db.get("BackupEnabled") ? 7 : 0,
            null, // backup window not exposed
            CloudGovernanceDataConverter.convertValue(db.get("CreateTime"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying GCP database resources: {}", e.getMessage());
    }

    return results;
  }

  @Override protected List<Object[]> queryAWS(List<String> accountIds) {
    List<Object[]> results = new ArrayList<>();

    try {
      CloudProvider awsProvider = new AWSProvider(config.aws);
      List<Map<String, Object>> dbResults = awsProvider.queryDatabaseResources(accountIds);

      for (Map<String, Object> db : dbResults) {
        results.add(new Object[]{
            "aws",
            db.get("AccountId"),
            db.get("DatabaseResource"),
            db.get("DatabaseType"),
            db.get("Application"),
            db.get("Region"),
            null, // resource group not applicable
            db.get("ResourceId"),
            db.get("Engine"),
            db.get("EngineVersion"),
            db.get("DBInstanceClass") != null ? db.get("DBInstanceClass") :
                db.get("CacheNodeType"),
            db.get("AllocatedStorage"),
            db.get("MultiAZ"),
            db.get("DBInstanceStatus") != null ? db.get("DBInstanceStatus") :
                db.get("Status"),
            db.get("PubliclyAccessible"),
            db.get("StorageEncrypted") != null ? db.get("StorageEncrypted") :
                db.get("AtRestEncryptionEnabled"),
            db.get("KmsKeyId") != null ? db.get("KmsKeyId") :
                db.get("KMSMasterKeyArn"),
            null, // TLS version not directly exposed
            db.get("BackupRetentionPeriod") != null ?
                ((Number) db.get("BackupRetentionPeriod")).intValue() :
                db.get("SnapshotRetentionLimit") != null ?
                    ((Number) db.get("SnapshotRetentionLimit")).intValue() : null,
            db.get("PreferredBackupWindow") != null ? db.get("PreferredBackupWindow") :
                db.get("SnapshotWindow"),
            CloudGovernanceDataConverter.convertValue(
                db.get("InstanceCreateTime") != null ? db.get("InstanceCreateTime") :
                db.get("ClusterCreateTime") != null ? db.get("ClusterCreateTime") :
                db.get("CreationDateTime"), SqlTypeName.TIMESTAMP)
        });
      }
    } catch (Exception e) {
      LOGGER.debug("Error querying AWS database resources: {}", e.getMessage());
    }

    return results;
  }

  private String parseMinTlsVersion(Object securityConfig) {
    if (securityConfig instanceof String) {
      String config = (String) securityConfig;
      if (config.contains("Min TLS: ")) {
        int start = config.indexOf("Min TLS: ") + 9;
        int end = config.indexOf(",", start);
        if (end == -1) end = config.length();
        return config.substring(start, end).trim();
      }
    }
    return null;
  }
}
