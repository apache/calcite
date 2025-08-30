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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Handles sort pushdown for multi-cloud query optimization.
 * Converts RelCollation to provider-specific sort expressions and handles client-side sorting.
 */
public class CloudOpsSortHandler {
  private static final Logger logger = LoggerFactory.getLogger(CloudOpsSortHandler.class);

  private final RelDataType rowType;
  private final @Nullable RelCollation collation;
  private final boolean hasSort;

  // Field name mappings for different providers
  private static final Map<String, String> AZURE_SORT_MAPPING = createAzureSortMapping();
  private static final Map<String, String> GCP_SORT_MAPPING = createGcpSortMapping();

  public CloudOpsSortHandler(RelDataType rowType, @Nullable RelCollation collation) {
    this.rowType = rowType;
    this.collation = collation;
    this.hasSort = (collation != null && !collation.getFieldCollations().isEmpty());

    if (logger.isDebugEnabled()) {
      if (hasSort) {
        logger.debug("CloudOpsSortHandler: Sort pushdown with {} sort fields: {}",
                    collation.getFieldCollations().size(), getSortFieldNames());
      } else {
        logger.debug("CloudOpsSortHandler: No sort pushdown (no collation)");
      }
    }
  }

  /**
   * Check if sort pushdown is applicable.
   */
  public boolean hasSort() {
    return hasSort;
  }

  /**
   * Get sort field collations.
   */
  public List<RelFieldCollation> getSortFields() {
    return hasSort ? collation.getFieldCollations() : new ArrayList<>();
  }

  /**
   * Get sort field names for logging.
   */
  public List<String> getSortFieldNames() {
    List<String> fieldNames = new ArrayList<>();
    if (hasSort) {
      for (RelFieldCollation fc : collation.getFieldCollations()) {
        int fieldIndex = fc.getFieldIndex();
        if (fieldIndex < rowType.getFieldCount()) {
          String fieldName = rowType.getFieldList().get(fieldIndex).getName();
          String direction = fc.getDirection().name();
          fieldNames.add(fieldName + " " + direction);
        }
      }
    }
    return fieldNames;
  }

  /**
   * Build Azure KQL ORDER BY clause based on sort collation.
   * Azure Resource Graph supports full sort pushdown via KQL.
   */
  public @Nullable String buildAzureKqlOrderByClause() {
    if (!hasSort) {
      return null;
    }

    StringBuilder kqlOrderBy = new StringBuilder("| order by ");
    List<String> sortExpressions = new ArrayList<>();

    for (RelFieldCollation fc : collation.getFieldCollations()) {
      int fieldIndex = fc.getFieldIndex();
      if (fieldIndex < rowType.getFieldCount()) {
        String fieldName = rowType.getFieldList().get(fieldIndex).getName();
        String azureFieldName = AZURE_SORT_MAPPING.getOrDefault(fieldName, fieldName);
        String direction = fc.getDirection() == RelFieldCollation.Direction.DESCENDING ? "desc" : "asc";
        sortExpressions.add(azureFieldName + " " + direction);
      }
    }

    if (sortExpressions.isEmpty()) {
      return null;
    }

    kqlOrderBy.append(String.join(", ", sortExpressions));

    if (logger.isDebugEnabled()) {
      logger.debug("Azure KQL sort pushdown: {}", kqlOrderBy.toString());
    }

    return kqlOrderBy.toString();
  }

  /**
   * Build GCP orderBy parameter for sort pushdown.
   * GCP APIs support orderBy parameter for some services.
   */
  public @Nullable String buildGcpOrderByParameter() {
    if (!hasSort) {
      return null;
    }

    List<String> sortExpressions = new ArrayList<>();
    for (RelFieldCollation fc : collation.getFieldCollations()) {
      int fieldIndex = fc.getFieldIndex();
      if (fieldIndex < rowType.getFieldCount()) {
        String fieldName = rowType.getFieldList().get(fieldIndex).getName();
        String gcpFieldName = GCP_SORT_MAPPING.get(fieldName);
        if (gcpFieldName != null) {
          String direction = fc.getDirection() == RelFieldCollation.Direction.DESCENDING ? " desc" : "";
          sortExpressions.add(gcpFieldName + direction);
        }
      }
    }

    if (sortExpressions.isEmpty()) {
      return null; // Fallback to client-side sorting
    }

    String orderByParam = String.join(",", sortExpressions);

    if (logger.isDebugEnabled()) {
      logger.debug("GCP orderBy parameter: {}", orderByParam);
    }

    return orderByParam;
  }

  /**
   * Sort result rows using client-side sorting.
   * Used for AWS (no API sort support) and fallback for other providers.
   */
  public List<Object[]> sortRows(List<Object[]> rows) {
    if (!hasSort || rows.isEmpty()) {
      return rows;
    }

    List<Object[]> sortedRows = new ArrayList<>(rows);

    // Create comparator chain for multi-field sorting
    Comparator<Object[]> comparator = null;
    for (RelFieldCollation fc : collation.getFieldCollations()) {
      int fieldIndex = fc.getFieldIndex();
      if (fieldIndex < rowType.getFieldCount()) {
        Comparator<Object[]> fieldComparator = createFieldComparator(fieldIndex, fc);
        comparator = (comparator == null) ? fieldComparator : comparator.thenComparing(fieldComparator);
      }
    }

    if (comparator != null) {
      sortedRows.sort(comparator);

      if (logger.isDebugEnabled()) {
        logger.debug("Client-side sort: {} rows sorted by {} fields",
                    rows.size(), collation.getFieldCollations().size());
      }
    }

    return sortedRows;
  }

  /**
   * Create a comparator for a specific field.
   */
  @SuppressWarnings("unchecked")
  private Comparator<Object[]> createFieldComparator(int fieldIndex, RelFieldCollation fc) {
    return (row1, row2) -> {
      Object val1 = (fieldIndex < row1.length) ? row1[fieldIndex] : null;
      Object val2 = (fieldIndex < row2.length) ? row2[fieldIndex] : null;

      // Handle nulls according to null direction
      if (val1 == null && val2 == null) return 0;
      if (val1 == null) return fc.nullDirection == RelFieldCollation.NullDirection.FIRST ? -1 : 1;
      if (val2 == null) return fc.nullDirection == RelFieldCollation.NullDirection.FIRST ? 1 : -1;

      // Compare values
      int comparison = 0;
      if (val1 instanceof Comparable && val2 instanceof Comparable) {
        try {
          comparison = ((Comparable<Object>) val1).compareTo(val2);
        } catch (ClassCastException e) {
          // Fallback to string comparison
          comparison = val1.toString().compareTo(val2.toString());
        }
      } else {
        // String comparison fallback
        comparison = val1.toString().compareTo(val2.toString());
      }

      // Apply direction
      return fc.getDirection() == RelFieldCollation.Direction.DESCENDING ? -comparison : comparison;
    };
  }

  /**
   * Calculate sort optimization metrics.
   */
  public SortMetrics calculateMetrics(boolean serverSidePushdown) {
    if (!hasSort) {
      return new SortMetrics(0, false, "No sort required");
    }

    int sortFields = collation.getFieldCollations().size();
    String strategy = serverSidePushdown ? "Server-side pushdown" : "Client-side sorting";

    return new SortMetrics(sortFields, serverSidePushdown, strategy);
  }

  /**
   * Azure field name mappings for KQL ORDER BY queries.
   */
  private static Map<String, String> createAzureSortMapping() {
    Map<String, String> mapping = new HashMap<>();
    // Map Calcite column names to Azure KQL field names for sorting
    mapping.put("cloud_provider", "'azure'"); // Constant value
    mapping.put("account_id", "SubscriptionId");
    mapping.put("cluster_name", "ClusterName");
    mapping.put("application", "Application");
    mapping.put("region", "Location");
    mapping.put("resource_group", "ResourceGroup");
    mapping.put("resource_id", "ResourceId");
    mapping.put("kubernetes_version", "ClusterVersion");
    mapping.put("node_count", "NodePoolCount");
    mapping.put("rbac_enabled", "RBACEnabled");
    mapping.put("private_cluster", "PrivateCluster");
    mapping.put("created_date", "properties.creationTime");
    mapping.put("modified_date", "properties.lastModifiedTime");
    return mapping;
  }

  /**
   * GCP field path mappings for orderBy parameter.
   */
  private static Map<String, String> createGcpSortMapping() {
    Map<String, String> mapping = new HashMap<>();
    // Map Calcite columns to GCP API orderBy field paths
    mapping.put("cluster_name", "name");
    mapping.put("region", "location");
    mapping.put("kubernetes_version", "currentMasterVersion");
    mapping.put("node_count", "currentNodeCount");
    mapping.put("created_date", "createTime");
    mapping.put("status", "status");
    return mapping;
  }

  /**
   * Metrics about sort optimization.
   */
  public static class SortMetrics {
    public final int sortFields;
    public final boolean serverSidePushdown;
    public final String strategy;

    public SortMetrics(int sortFields, boolean serverSidePushdown, String strategy) {
      this.sortFields = sortFields;
      this.serverSidePushdown = serverSidePushdown;
      this.strategy = strategy;
    }

    @Override public String toString() {
      return String.format(Locale.ROOT, "Sort: %d fields, %s", sortFields, strategy);
    }
  }
}
