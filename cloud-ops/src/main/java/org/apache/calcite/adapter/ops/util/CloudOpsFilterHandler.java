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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles filter pushdown optimization for multi-cloud query optimization.
 * Converts Calcite RexNode filters to provider-specific query constraints.
 */
public class CloudOpsFilterHandler {
  private static final Logger logger = LoggerFactory.getLogger(CloudOpsFilterHandler.class);

  private final RelDataType rowType;
  private final List<RexNode> filters;
  private final Map<String, List<FilterInfo>> fieldFilters;
  private final List<RexNode> pushableFilters;
  private final List<RexNode> remainingFilters;

  // Supported filter operations for pushdown
  private static final Set<SqlKind> PUSHABLE_OPERATIONS = Set.of(
      SqlKind.EQUALS,
      SqlKind.NOT_EQUALS,
      SqlKind.GREATER_THAN,
      SqlKind.GREATER_THAN_OR_EQUAL,
      SqlKind.LESS_THAN,
      SqlKind.LESS_THAN_OR_EQUAL,
      SqlKind.IN,
      SqlKind.LIKE,
      SqlKind.IS_NULL,
      SqlKind.IS_NOT_NULL,
      SqlKind.OR
  );

  // Field name mappings for different cloud providers
  private static final Map<String, ProviderFieldMapping> FIELD_MAPPINGS = Map.of(
      "azure", new ProviderFieldMapping(
          Map.of(
              "cloud_provider", "SubscriptionId", // Special handling needed
              "account_id", "SubscriptionId",
              "region", "location",
              "cluster_name", "name",
              "application", "Application",
              "resource_name", "name",
              "resource_type", "type"
          )
      ),
      "aws", new ProviderFieldMapping(
          Map.of(
              "cloud_provider", "provider", // Logical field
              "account_id", "accountId",
              "region", "region",
              "cluster_name", "name",
              "application", "tags.Application",
              "resource_name", "name",
              "resource_type", "resourceType"
          )
      ),
      "gcp", new ProviderFieldMapping(
          Map.of(
              "cloud_provider", "provider", // Logical field
              "account_id", "projectId",
              "region", "region",
              "cluster_name", "name",
              "application", "labels.application",
              "resource_name", "name",
              "resource_type", "resourceType"
          )
      )
  );

  public CloudOpsFilterHandler(RelDataType rowType, List<RexNode> filters) {
    this.rowType = rowType;
    this.filters = filters != null ? filters : new ArrayList<>();
    this.fieldFilters = new HashMap<>();
    this.pushableFilters = new ArrayList<>();
    this.remainingFilters = new ArrayList<>();

    analyzeFilters();

    if (logger.isDebugEnabled()) {
      if (hasFilters()) {
        logger.debug("CloudOpsFilterHandler: {} filter(s) analyzed, {} pushable, {} remaining",
                    this.filters.size(), pushableFilters.size(), remainingFilters.size());
      } else {
        logger.debug("CloudOpsFilterHandler: No filters (unbounded query)");
      }
    }
  }

  /**
   * Check if any filters are present.
   */
  public boolean hasFilters() {
    return !filters.isEmpty();
  }

  /**
   * Check if any filters can be pushed down.
   */
  public boolean hasPushableFilters() {
    return !pushableFilters.isEmpty();
  }

  /**
   * Get filters for a specific field.
   */
  public List<FilterInfo> getFiltersForField(String fieldName) {
    return fieldFilters.getOrDefault(fieldName, Collections.emptyList());
  }

  /**
   * Get all pushable filters.
   */
  public List<RexNode> getPushableFilters() {
    return new ArrayList<>(pushableFilters);
  }

  /**
   * Get remaining filters that need client-side evaluation.
   */
  public List<RexNode> getRemainingFilters() {
    return new ArrayList<>(remainingFilters);
  }

  /**
   * Extract provider constraints from filters.
   * Returns set of provider names to query, or empty set to query all.
   */
  public Set<String> extractProviderConstraints() {
    List<FilterInfo> providerFilters = getFiltersForField("cloud_provider");
    if (providerFilters.isEmpty()) {
      return Collections.emptySet(); // Query all providers
    }

    return providerFilters.stream()
        .filter(filter -> filter.operation == SqlKind.EQUALS || filter.operation == SqlKind.IN)
        .flatMap(filter -> {
          if (filter.operation == SqlKind.EQUALS) {
            String stringValue = extractStringValue(filter.value);
            return List.of(stringValue).stream();
          } else if (filter.operation == SqlKind.IN && filter.values != null) {
            return filter.values.stream().map(this::extractStringValue);
          }
          return List.<String>of().stream();
        })
        .collect(Collectors.toSet());
  }

  /**
   * Extract account ID constraints from filters.
   */
  public List<String> extractAccountConstraints() {
    List<FilterInfo> accountFilters = getFiltersForField("account_id");
    if (accountFilters.isEmpty()) {
      return Collections.emptyList(); // Use configured defaults
    }

    return accountFilters.stream()
        .filter(filter -> filter.operation == SqlKind.EQUALS || filter.operation == SqlKind.IN)
        .flatMap(filter -> {
          if (filter.operation == SqlKind.EQUALS) {
            String stringValue = extractStringValue(filter.value);
            return List.of(stringValue).stream();
          } else if (filter.operation == SqlKind.IN && filter.values != null) {
            return filter.values.stream().map(this::extractStringValue);
          }
          return List.<String>of().stream();
        })
        .collect(Collectors.toList());
  }

  /**
   * Build Azure KQL WHERE clause from pushable filters.
   */
  public @Nullable String buildAzureKqlWhereClause() {
    if (!hasPushableFilters()) {
      return null;
    }

    List<String> whereConditions = new ArrayList<>();
    ProviderFieldMapping azureMapping = FIELD_MAPPINGS.get("azure");

    for (Map.Entry<String, List<FilterInfo>> entry : fieldFilters.entrySet()) {
      String fieldName = entry.getKey();
      List<FilterInfo> filters = entry.getValue();

      // Skip cloud_provider filters as they're handled by provider selection
      if ("cloud_provider".equals(fieldName)) {
        continue;
      }

      String kqlField = azureMapping.getProviderField(fieldName);
      if (kqlField == null) {
        continue; // Field not mappable to Azure KQL
      }

      for (FilterInfo filter : filters) {
        String condition = buildAzureKqlCondition(kqlField, filter);
        if (condition != null) {
          whereConditions.add(condition);
        }
      }
    }

    if (whereConditions.isEmpty()) {
      return null;
    }

    String whereClause = "| where " + String.join(" and ", whereConditions);

    if (logger.isDebugEnabled()) {
      double reductionEstimate = estimateFilterReduction();
      logger.debug("Azure KQL WHERE clause: {} -> {:.1f}% data reduction estimate",
                  whereClause, reductionEstimate * 100);
    }

    return whereClause;
  }

  /**
   * Get AWS API filter parameters.
   */
  public Map<String, Object> getAWSFilterParameters() {
    Map<String, Object> params = new HashMap<>();
    
    if (!hasPushableFilters()) {
      return params;
    }

    ProviderFieldMapping awsMapping = FIELD_MAPPINGS.get("aws");

    // Extract region filters for AWS region constraint
    List<FilterInfo> regionFilters = getFiltersForField("region");
    if (!regionFilters.isEmpty()) {
      FilterInfo regionFilter = regionFilters.get(0);
      if (regionFilter.operation == SqlKind.EQUALS) {
        params.put("region", extractStringValue(regionFilter.value));
      }
    }

    // Extract tag-based filters
    for (Map.Entry<String, List<FilterInfo>> entry : fieldFilters.entrySet()) {
      String fieldName = entry.getKey();
      List<FilterInfo> filters = entry.getValue();

      if ("cloud_provider".equals(fieldName) || "region".equals(fieldName)) {
        continue; // Already handled
      }

      String awsField = awsMapping.getProviderField(fieldName);
      if (awsField != null && awsField.startsWith("tags.")) {
        String tagName = awsField.substring(5); // Remove "tags." prefix
        for (FilterInfo filter : filters) {
          if (filter.operation == SqlKind.EQUALS) {
            params.put("tag:" + tagName, extractStringValue(filter.value));
          }
        }
      }
    }

    if (logger.isDebugEnabled() && !params.isEmpty()) {
      logger.debug("AWS filter parameters: {} -> estimated data reduction",
                  params.keySet());
    }

    return params;
  }

  /**
   * Get GCP API filter parameters.
   */
  public Map<String, Object> getGCPFilterParameters() {
    Map<String, Object> params = new HashMap<>();
    
    if (!hasPushableFilters()) {
      return params;
    }

    // Extract project ID filters
    List<FilterInfo> projectFilters = getFiltersForField("account_id");
    if (!projectFilters.isEmpty()) {
      FilterInfo projectFilter = projectFilters.get(0);
      if (projectFilter.operation == SqlKind.EQUALS) {
        params.put("projectId", extractStringValue(projectFilter.value));
      }
    }

    // Extract region filters
    List<FilterInfo> regionFilters = getFiltersForField("region");
    if (!regionFilters.isEmpty()) {
      FilterInfo regionFilter = regionFilters.get(0);
      if (regionFilter.operation == SqlKind.EQUALS) {
        params.put("region", extractStringValue(regionFilter.value));
      }
    }

    if (logger.isDebugEnabled() && !params.isEmpty()) {
      logger.debug("GCP filter parameters: {} -> estimated data reduction",
                  params.keySet());
    }

    return params;
  }

  /**
   * Calculate filter optimization metrics.
   */
  public FilterMetrics calculateMetrics(boolean serverSidePushdown, int totalFiltersApplied) {
    if (!hasFilters()) {
      return new FilterMetrics(0, 0, 0.0, "No filters", false);
    }

    int filtersApplied = serverSidePushdown ? pushableFilters.size() : totalFiltersApplied;
    double pushdownPercent = filters.isEmpty() ? 0.0 : 
        (double) pushableFilters.size() / filters.size() * 100;

    String strategy = serverSidePushdown ? 
        "Server-side filter pushdown" : "Client-side filtering";

    return new FilterMetrics(filters.size(), filtersApplied, pushdownPercent,
                            strategy, serverSidePushdown);
  }

  /**
   * Analyze all filters and categorize them.
   */
  private void analyzeFilters() {
    for (RexNode filter : filters) {
      if (isPushableFilter(filter)) {
        pushableFilters.add(filter);
        extractFieldFilters(filter);
      } else {
        remainingFilters.add(filter);
      }
    }
  }

  /**
   * Check if a filter can be pushed down.
   */
  private boolean isPushableFilter(RexNode filter) {
    if (!(filter instanceof RexCall)) {
      return false;
    }

    RexCall call = (RexCall) filter;
    SqlKind kind = call.getKind();

    if (!PUSHABLE_OPERATIONS.contains(kind)) {
      return false;
    }

    // For binary operations, ensure we have field reference and literal
    if (kind == SqlKind.EQUALS || kind == SqlKind.NOT_EQUALS ||
        kind == SqlKind.GREATER_THAN || kind == SqlKind.LESS_THAN ||
        kind == SqlKind.GREATER_THAN_OR_EQUAL || kind == SqlKind.LESS_THAN_OR_EQUAL ||
        kind == SqlKind.LIKE) {
      
      if (call.getOperands().size() != 2) {
        return false;
      }

      RexNode operand0 = call.getOperands().get(0);
      RexNode operand1 = call.getOperands().get(1);

      return (operand0 instanceof RexInputRef && operand1 instanceof RexLiteral);
    }

    // For unary operations (IS NULL, IS NOT NULL)
    if (kind == SqlKind.IS_NULL || kind == SqlKind.IS_NOT_NULL) {
      if (call.getOperands().size() != 1) {
        return false;
      }
      
      return call.getOperands().get(0) instanceof RexInputRef;
    }

    // For IN operations
    if (kind == SqlKind.IN) {
      if (call.getOperands().size() < 2) {
        return false;
      }
      
      // First operand should be field reference, rest should be literals
      RexNode firstOperand = call.getOperands().get(0);
      return firstOperand instanceof RexInputRef;
    }

    // For OR operations - check if both operands are pushable
    if (kind == SqlKind.OR) {
      if (call.getOperands().size() != 2) {
        return false;
      }
      
      // Both operands should be pushable filters
      return isPushableFilter(call.getOperands().get(0)) && 
             isPushableFilter(call.getOperands().get(1));
    }

    return false;
  }

  /**
   * Extract filter information from a pushable filter.
   */
  private void extractFieldFilters(RexNode filter) {
    if (!(filter instanceof RexCall)) {
      return;
    }

    RexCall call = (RexCall) filter;
    SqlKind kind = call.getKind();
    
    // Handle OR operations by recursively processing both operands
    if (kind == SqlKind.OR) {
      for (RexNode operand : call.getOperands()) {
        extractFieldFilters(operand);
      }
      return;
    }
    
    if (call.getOperands().isEmpty()) {
      return;
    }

    RexNode firstOperand = call.getOperands().get(0);
    if (!(firstOperand instanceof RexInputRef)) {
      return;
    }

    RexInputRef inputRef = (RexInputRef) firstOperand;
    int fieldIndex = inputRef.getIndex();
    
    if (fieldIndex >= rowType.getFieldCount()) {
      return;
    }

    String fieldName = rowType.getFieldNames().get(fieldIndex);
    
    FilterInfo filterInfo;
    
    if (kind == SqlKind.IN) {
      // Handle IN operation
      List<Object> values = new ArrayList<>();
      for (int i = 1; i < call.getOperands().size(); i++) {
        RexNode operand = call.getOperands().get(i);
        if (operand instanceof RexLiteral) {
          Object value = ((RexLiteral) operand).getValue();
          values.add(value);
        }
      }
      filterInfo = new FilterInfo(fieldName, fieldIndex, kind, null, values);
    } else if (kind == SqlKind.IS_NULL || kind == SqlKind.IS_NOT_NULL) {
      // Handle unary operations
      filterInfo = new FilterInfo(fieldName, fieldIndex, kind, null, null);
    } else {
      // Handle binary operations
      if (call.getOperands().size() >= 2) {
        RexNode secondOperand = call.getOperands().get(1);
        Object value = null;
        if (secondOperand instanceof RexLiteral) {
          value = ((RexLiteral) secondOperand).getValue();
        }
        filterInfo = new FilterInfo(fieldName, fieldIndex, kind, value, null);
      } else {
        return;
      }
    }

    fieldFilters.computeIfAbsent(fieldName, k -> new ArrayList<>()).add(filterInfo);
  }

  /**
   * Build Azure KQL condition for a field and filter.
   */
  private @Nullable String buildAzureKqlCondition(String kqlField, FilterInfo filter) {
    switch (filter.operation) {
      case EQUALS:
        return kqlField + " == '" + extractStringValue(filter.value) + "'";
      case NOT_EQUALS:
        return kqlField + " != '" + extractStringValue(filter.value) + "'";
      case GREATER_THAN:
        return kqlField + " > " + extractStringValue(filter.value);
      case GREATER_THAN_OR_EQUAL:
        return kqlField + " >= " + extractStringValue(filter.value);
      case LESS_THAN:
        return kqlField + " < " + extractStringValue(filter.value);
      case LESS_THAN_OR_EQUAL:
        return kqlField + " <= " + extractStringValue(filter.value);
      case LIKE:
        String pattern = extractStringValue(filter.value).replace("%", "*");
        return kqlField + " contains '" + pattern.replace("*", "") + "'";
      case IN:
        if (filter.values != null && !filter.values.isEmpty()) {
          String valuesList = filter.values.stream()
              .map(v -> "'" + extractStringValue(v) + "'")
              .collect(Collectors.joining(", "));
          return kqlField + " in (" + valuesList + ")";
        }
        break;
      case IS_NULL:
        return "isempty(" + kqlField + ")";
      case IS_NOT_NULL:
        return "isnotempty(" + kqlField + ")";
      default:
        break;
    }
    return null;
  }

  /**
   * Estimate filter reduction percentage.
   */
  private double estimateFilterReduction() {
    if (!hasPushableFilters()) {
      return 0.0;
    }

    // Conservative estimate based on filter selectivity
    double reduction = 0.0;
    
    if (getFiltersForField("cloud_provider").size() > 0) {
      reduction += 0.3; // Provider filtering can reduce by ~30%
    }
    
    if (getFiltersForField("region").size() > 0) {
      reduction += 0.4; // Region filtering can reduce by ~40%
    }
    
    if (getFiltersForField("application").size() > 0) {
      reduction += 0.2; // Application filtering can reduce by ~20%
    }

    return Math.min(reduction, 0.9); // Cap at 90% reduction
  }

  /**
   * Extract string value from RexLiteral, handling NlsString encoding.
   */
  private String extractStringValue(Object value) {
    if (value instanceof org.apache.calcite.util.NlsString) {
      return ((org.apache.calcite.util.NlsString) value).getValue();
    }
    return value != null ? value.toString() : "";
  }

  /**
   * Filter information for a specific field.
   */
  public static class FilterInfo {
    public final String fieldName;
    public final int fieldIndex;
    public final SqlKind operation;
    public final @Nullable Object value;
    public final @Nullable List<Object> values; // For IN operations

    public FilterInfo(String fieldName, int fieldIndex, SqlKind operation,
                     @Nullable Object value, @Nullable List<Object> values) {
      this.fieldName = fieldName;
      this.fieldIndex = fieldIndex;
      this.operation = operation;
      this.value = value;
      this.values = values;
    }

    @Override public String toString() {
      if (operation == SqlKind.IN && values != null) {
        return String.format("%s[%d] %s %s", fieldName, fieldIndex, operation, values);
      }
      return String.format("%s[%d] %s %s", fieldName, fieldIndex, operation, value);
    }
  }

  /**
   * Provider field mapping configuration.
   */
  private static class ProviderFieldMapping {
    private final Map<String, String> fieldMap;

    public ProviderFieldMapping(Map<String, String> fieldMap) {
      this.fieldMap = fieldMap;
    }

    public @Nullable String getProviderField(String logicalField) {
      return fieldMap.get(logicalField);
    }
  }

  /**
   * Metrics about filter optimization.
   */
  public static class FilterMetrics {
    public final int totalFilters;
    public final int filtersApplied;
    public final double pushdownPercent;
    public final String strategy;
    public final boolean serverSidePushdown;

    public FilterMetrics(int totalFilters, int filtersApplied, double pushdownPercent,
                        String strategy, boolean serverSidePushdown) {
      this.totalFilters = totalFilters;
      this.filtersApplied = filtersApplied;
      this.pushdownPercent = pushdownPercent;
      this.strategy = strategy;
      this.serverSidePushdown = serverSidePushdown;
    }

    @Override public String toString() {
      return String.format("Filters: %d/%d applied (%.1f%% pushdown) via %s",
                          filtersApplied, totalFilters, pushdownPercent, strategy);
    }
  }
}