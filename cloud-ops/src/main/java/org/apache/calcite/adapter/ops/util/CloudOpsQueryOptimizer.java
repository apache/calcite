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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for analyzing and optimizing Cloud Ops queries.
 * Provides methods to extract and analyze query optimization hints.
 */
public class CloudOpsQueryOptimizer {
  private static final Logger logger = LoggerFactory.getLogger(CloudOpsQueryOptimizer.class);

  private final List<RexNode> filters;
  private final int @Nullable [] projections;
  private final @Nullable RelCollation collation;
  private final @Nullable RexNode offset;
  private final @Nullable RexNode fetch;

  public CloudOpsQueryOptimizer(List<RexNode> filters,
                                int @Nullable [] projections,
                                @Nullable RelCollation collation,
                                @Nullable RexNode offset,
                                @Nullable RexNode fetch) {
    this.filters = filters != null ? filters : new ArrayList<>();
    this.projections = projections;
    this.collation = collation;
    this.offset = offset;
    this.fetch = fetch;
  }

  /**
   * Extract filter information for a specific field.
   */
  public List<FilterInfo> extractFiltersForField(String fieldName, int fieldIndex) {
    List<FilterInfo> fieldFilters = new ArrayList<>();

    for (RexNode filter : filters) {
      if (filter instanceof RexCall) {
        RexCall call = (RexCall) filter;
        SqlOperator op = call.getOperator();
        SqlKind kind = op.getKind();

        // Handle different types of operations
        if (kind == SqlKind.IN) {
          // Handle IN operations: field IN (value1, value2, ...)
          if (call.getOperands().size() >= 2) {
            RexNode operand0 = call.getOperands().get(0);
            if (operand0 instanceof RexInputRef) {
              RexInputRef inputRef = (RexInputRef) operand0;
              if (inputRef.getIndex() == fieldIndex) {
                // Extract all IN values
                List<Object> values = new ArrayList<>();
                for (int i = 1; i < call.getOperands().size(); i++) {
                  Object value = extractValue(call.getOperands().get(i));
                  values.add(value);
                }

                FilterInfo filterInfo = new FilterInfo(fieldName, fieldIndex, kind, values);
                fieldFilters.add(filterInfo);

                logger.debug("Found IN filter for field {}: {} IN {}",
                           fieldName, fieldName, values);
              }
            }
          }
        } else if (kind == SqlKind.IS_NULL || kind == SqlKind.IS_NOT_NULL) {
          // Handle unary operations: field IS NULL, field IS NOT NULL
          if (call.getOperands().size() == 1) {
            RexNode operand0 = call.getOperands().get(0);
            if (operand0 instanceof RexInputRef) {
              RexInputRef inputRef = (RexInputRef) operand0;
              if (inputRef.getIndex() == fieldIndex) {
                FilterInfo filterInfo = new FilterInfo(fieldName, fieldIndex, kind, null);
                fieldFilters.add(filterInfo);

                logger.debug("Found null filter for field {}: {} {}",
                           fieldName, fieldName, kind);
              }
            }
          }
        } else {
          // Handle binary operations: field = value, field > value, etc.
          if (call.getOperands().size() >= 2) {
            RexNode operand0 = call.getOperands().get(0);
            if (operand0 instanceof RexInputRef) {
              RexInputRef inputRef = (RexInputRef) operand0;
              if (inputRef.getIndex() == fieldIndex) {
                // This filter applies to our field
                RexNode operand1 = call.getOperands().get(1);
                Object value = extractValue(operand1);

                FilterInfo filterInfo =
                    new FilterInfo(fieldName,
                    fieldIndex,
                    kind,
                    value);
                fieldFilters.add(filterInfo);

                logger.debug("Found filter for field {}: {} {} {}",
                           fieldName, fieldName, kind, value);
              }
            }
          }
        }
      }
    }

    return fieldFilters;
  }

  /**
   * Extract value from a RexNode (typically RexLiteral).
   */
  private Object extractValue(RexNode node) {
    if (node instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) node;
      return literal.getValue();
    }
    return node.toString();
  }

  /**
   * Get sort information if available.
   */
  public @Nullable SortInfo getSortInfo() {
    if (collation == null || collation.getFieldCollations().isEmpty()) {
      return null;
    }

    List<SortField> sortFields = new ArrayList<>();
    for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
      sortFields.add(
          new SortField(
          fieldCollation.getFieldIndex(),
          fieldCollation.getDirection(),
          fieldCollation.nullDirection));
    }

    return new SortInfo(sortFields);
  }

  /**
   * Get pagination information if available.
   */
  public @Nullable PaginationInfo getPaginationInfo() {
    if (offset == null && fetch == null) {
      return null;
    }

    Integer offsetValue = null;
    Integer fetchValue = null;

    if (offset instanceof RexLiteral) {
      offsetValue = ((RexLiteral) offset).getValueAs(Integer.class);
    }

    if (fetch instanceof RexLiteral) {
      fetchValue = ((RexLiteral) fetch).getValueAs(Integer.class);
    }

    return new PaginationInfo(offsetValue, fetchValue);
  }

  /**
   * Get projection information.
   */
  public @Nullable ProjectionInfo getProjectionInfo() {
    if (projections == null) {
      return null;
    }

    return new ProjectionInfo(projections);
  }

  /**
   * Check if a specific optimization is available.
   */
  public boolean hasOptimization(OptimizationType type) {
    switch (type) {
      case FILTER:
        return !filters.isEmpty();
      case PROJECTION:
        return projections != null;
      case SORT:
        return collation != null && !collation.getFieldCollations().isEmpty();
      case PAGINATION:
        return offset != null || fetch != null;
      default:
        return false;
    }
  }

  /**
   * Types of query optimizations.
   */
  public enum OptimizationType {
    FILTER,
    PROJECTION,
    SORT,
    PAGINATION
  }

  /**
   * Filter information for a field.
   */
  public static class FilterInfo {
    public final String fieldName;
    public final int fieldIndex;
    public final SqlKind operator;
    public final Object value;
    public final List<Object> values; // For IN operations

    public FilterInfo(String fieldName, int fieldIndex, SqlKind operator, Object value) {
      this.fieldName = fieldName;
      this.fieldIndex = fieldIndex;
      this.operator = operator;
      this.value = value;
      this.values = null;
    }

    public FilterInfo(String fieldName, int fieldIndex, SqlKind operator, List<Object> values) {
      this.fieldName = fieldName;
      this.fieldIndex = fieldIndex;
      this.operator = operator;
      this.value = null;
      this.values = values;
    }

    @Override public String toString() {
      if (operator == SqlKind.IN && values != null) {
        return String.format("%s[%d] %s %s", fieldName, fieldIndex, operator, values);
      }
      return String.format("%s[%d] %s %s", fieldName, fieldIndex, operator, value);
    }
  }

  /**
   * Sort information.
   */
  public static class SortInfo {
    public final List<SortField> sortFields;

    public SortInfo(List<SortField> sortFields) {
      this.sortFields = sortFields;
    }

    public boolean isEmpty() {
      return sortFields.isEmpty();
    }
  }

  /**
   * Individual sort field.
   */
  public static class SortField {
    public final int fieldIndex;
    public final RelFieldCollation.Direction direction;
    public final RelFieldCollation.NullDirection nullDirection;

    public SortField(int fieldIndex,
                    RelFieldCollation.Direction direction,
                    RelFieldCollation.NullDirection nullDirection) {
      this.fieldIndex = fieldIndex;
      this.direction = direction;
      this.nullDirection = nullDirection;
    }
  }

  /**
   * Pagination information.
   */
  public static class PaginationInfo {
    public final @Nullable Integer offset;
    public final @Nullable Integer fetch;

    public PaginationInfo(@Nullable Integer offset, @Nullable Integer fetch) {
      this.offset = offset;
      this.fetch = fetch;
    }

    public boolean hasOffset() {
      return offset != null;
    }

    public boolean hasFetch() {
      return fetch != null;
    }
  }

  /**
   * Projection information.
   */
  public static class ProjectionInfo {
    public final int[] columns;

    public ProjectionInfo(int[] columns) {
      this.columns = columns;
    }

    public boolean isSelectAll() {
      return false; // If we have a ProjectionInfo, it's not select *
    }
  }
}
