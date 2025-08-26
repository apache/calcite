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
package org.apache.calcite.adapter.ops.util;

import org.apache.calcite.rel.type.RelDataType;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles projection pushdown for multi-cloud query optimization.
 * Converts column indices to provider-specific projections and standardizes result format.
 */
public class CloudOpsProjectionHandler {
  private static final Logger logger = LoggerFactory.getLogger(CloudOpsProjectionHandler.class);

  private final RelDataType rowType;
  private final int @Nullable [] projections;
  private final boolean isSelectAll;

  // Field name mappings for different providers
  private static final Map<String, String> AZURE_FIELD_MAPPING = createAzureFieldMapping();
  private static final Map<String, String> GCP_FIELD_MAPPING = createGcpFieldMapping();

  public CloudOpsProjectionHandler(RelDataType rowType, int @Nullable [] projections) {
    this.rowType = rowType;
    this.projections = projections;
    this.isSelectAll = (projections == null);

    if (logger.isDebugEnabled()) {
      if (isSelectAll) {
        logger.debug("CloudOpsProjectionHandler: SELECT * (all {} columns)",
                    rowType.getFieldCount());
      } else {
        logger.debug("CloudOpsProjectionHandler: Projecting {} out of {} columns: {}",
                    projections.length, rowType.getFieldCount(),
                    getProjectedFieldNames());
      }
    }
  }

  /**
   * Check if this is a SELECT * query (no projection).
   */
  public boolean isSelectAll() {
    return isSelectAll;
  }

  /**
   * Get the projected column indices.
   */
  public int[] getProjections() {
    return projections != null ? projections.clone() : new int[0];
  }

  /**
   * Get projected field names for logging.
   */
  public List<String> getProjectedFieldNames() {
    List<String> fieldNames = new ArrayList<>();
    if (projections != null) {
      for (int index : projections) {
        if (index < rowType.getFieldCount()) {
          fieldNames.add(rowType.getFieldList().get(index).getName());
        }
      }
    }
    return fieldNames;
  }

  /**
   * Build Azure KQL project clause based on projections.
   * Azure Resource Graph supports full projection via KQL.
   */
  public String buildAzureKqlProjectClause() {
    if (isSelectAll) {
      // Return default project for all fields - this could be optimized
      return null; // Let the existing queries handle full projection
    }

    StringBuilder kqlProject = new StringBuilder("| project ");
    List<String> projectedFields = new ArrayList<>();

    for (int i = 0; i < projections.length; i++) {
      int columnIndex = projections[i];
      if (columnIndex < rowType.getFieldCount()) {
        String fieldName = rowType.getFieldList().get(columnIndex).getName();
        String azureFieldName = AZURE_FIELD_MAPPING.getOrDefault(fieldName, fieldName);
        projectedFields.add(azureFieldName);
      }
    }

    kqlProject.append(String.join(", ", projectedFields));

    if (logger.isDebugEnabled()) {
      double reductionPercent = (1.0 - (double) projections.length / rowType.getFieldCount()) * 100;
      logger.debug("Azure KQL projection: {} -> {:.1f}% data reduction",
                  kqlProject.toString(), reductionPercent);
    }

    return kqlProject.toString();
  }

  /**
   * Build GCP fields parameter for partial projection support.
   * GCP APIs support fields parameter for some services.
   */
  public @Nullable String buildGcpFieldsParameter() {
    if (isSelectAll) {
      return null; // No fields parameter = all fields
    }

    List<String> gcpFields = new ArrayList<>();
    for (int columnIndex : projections) {
      if (columnIndex < rowType.getFieldCount()) {
        String fieldName = rowType.getFieldList().get(columnIndex).getName();
        String gcpFieldPath = GCP_FIELD_MAPPING.get(fieldName);
        if (gcpFieldPath != null && !gcpFields.contains(gcpFieldPath)) {
          gcpFields.add(gcpFieldPath);
        }
      }
    }

    if (gcpFields.isEmpty()) {
      return null; // Fallback to full query
    }

    String fieldsParam = "items(" + String.join(",", gcpFields) + ")";

    if (logger.isDebugEnabled()) {
      double reductionPercent = (1.0 - (double) gcpFields.size() / GCP_FIELD_MAPPING.size()) * 100;
      logger.debug("GCP fields parameter: {} -> {:.1f}% estimated data reduction",
                  fieldsParam, reductionPercent);
    }

    return fieldsParam;
  }

  /**
   * Project a result row to only include requested columns.
   * Used for client-side projection (AWS) and result standardization.
   */
  public Object[] projectRow(Object[] fullRow) {
    if (isSelectAll || fullRow == null) {
      return fullRow;
    }

    Object[] projectedRow = new Object[projections.length];
    for (int i = 0; i < projections.length; i++) {
      int sourceIndex = projections[i];
      if (sourceIndex < fullRow.length) {
        projectedRow[i] = fullRow[sourceIndex];
      } else {
        projectedRow[i] = null; // Handle missing fields gracefully
      }
    }

    return projectedRow;
  }

  /**
   * Project a list of result rows.
   */
  public List<Object[]> projectRows(List<Object[]> fullRows) {
    if (isSelectAll) {
      return fullRows;
    }

    List<Object[]> projectedRows = new ArrayList<>();
    for (Object[] row : fullRows) {
      projectedRows.add(projectRow(row));
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Client-side projection: {} rows projected from {} to {} columns",
                  fullRows.size(),
                  fullRows.isEmpty() ? 0 : fullRows.get(0).length,
                  projections.length);
    }

    return projectedRows;
  }

  /**
   * Calculate projection efficiency metrics.
   */
  public ProjectionMetrics calculateMetrics() {
    if (isSelectAll) {
      return new ProjectionMetrics(rowType.getFieldCount(), rowType.getFieldCount(), 0.0);
    }

    int totalFields = rowType.getFieldCount();
    int projectedFields = projections.length;
    double reductionPercent = (1.0 - (double) projectedFields / totalFields) * 100;

    return new ProjectionMetrics(totalFields, projectedFields, reductionPercent);
  }

  /**
   * Azure field name mappings for KQL queries.
   */
  private static Map<String, String> createAzureFieldMapping() {
    Map<String, String> mapping = new HashMap<>();
    // Map Calcite column names to Azure KQL field names
    mapping.put("cloud_provider", "'azure' as cloud_provider");
    mapping.put("account_id", "SubscriptionId");
    mapping.put("cluster_name", "ClusterName");
    mapping.put("application", "Application");
    mapping.put("region", "Location");
    mapping.put("resource_group", "ResourceGroup");
    mapping.put("resource_id", "ResourceId");
    mapping.put("kubernetes_version", "ClusterVersion");
    mapping.put("node_count", "NodePoolCount"); // Approximation for Azure
    mapping.put("node_pools", "NodePoolCount");
    mapping.put("rbac_enabled", "RBACEnabled");
    mapping.put("private_cluster", "PrivateCluster");
    mapping.put("public_endpoint", "case(PrivateCluster == false, true, false)");
    mapping.put("authorized_ip_ranges", "AuthorizedIPRanges");
    mapping.put("network_policy_provider", "NetworkPolicy");
    mapping.put("pod_security_policy_enabled", "false"); // Not supported in Azure
    mapping.put("encryption_at_rest_enabled", "case(DiskEncryption != 'Platform Managed Key', true, false)");
    mapping.put("encryption_key_type", "DiskEncryption");
    mapping.put("logging_enabled", "true"); // Default in AKS
    mapping.put("monitoring_enabled", "true"); // Default in AKS
    mapping.put("created_date", "null"); // Would need additional query
    mapping.put("modified_date", "null"); // Would need additional query
    mapping.put("tags", "null"); // Would need tags extraction
    return mapping;
  }

  /**
   * GCP field path mappings for fields parameter.
   */
  private static Map<String, String> createGcpFieldMapping() {
    Map<String, String> mapping = new HashMap<>();
    // Map Calcite columns to GCP API field paths
    mapping.put("cluster_name", "name");
    mapping.put("region", "location");
    mapping.put("kubernetes_version", "currentMasterVersion");
    mapping.put("node_count", "currentNodeCount");
    mapping.put("rbac_enabled", "legacyAbac.enabled");
    mapping.put("private_cluster", "privateClusterConfig.enablePrivateNodes");
    mapping.put("network_policy_provider", "networkConfig.networkPolicy");
    mapping.put("logging_enabled", "loggingService");
    mapping.put("monitoring_enabled", "monitoringService");
    return mapping;
  }

  /**
   * Metrics about projection efficiency.
   */
  public static class ProjectionMetrics {
    public final int totalFields;
    public final int projectedFields;
    public final double reductionPercent;

    public ProjectionMetrics(int totalFields, int projectedFields, double reductionPercent) {
      this.totalFields = totalFields;
      this.projectedFields = projectedFields;
      this.reductionPercent = reductionPercent;
    }

    @Override public String toString() {
      return String.format("Projection: %d/%d fields (%.1f%% reduction)",
                          projectedFields, totalFields, reductionPercent);
    }
  }
}
