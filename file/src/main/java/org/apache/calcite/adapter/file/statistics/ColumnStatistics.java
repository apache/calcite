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
package org.apache.calcite.adapter.file.statistics;

/**
 * Statistics for a single column including min/max values, null count,
 * and HyperLogLog sketch for cardinality estimation.
 */
public class ColumnStatistics {
  private final Object minValue;
  private final Object maxValue;
  private final long nullCount;
  private final long totalCount;
  private final HyperLogLogSketch hllSketch;
  private final String columnName;

  public ColumnStatistics(String columnName, Object minValue, Object maxValue,
                         long nullCount, long totalCount, HyperLogLogSketch hllSketch) {
    this.columnName = columnName;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.nullCount = nullCount;
    this.totalCount = totalCount;
    this.hllSketch = hllSketch;
  }

  /**
   * Get the minimum value in this column.
   */
  public Object getMinValue() {
    return minValue;
  }

  /**
   * Get the maximum value in this column.
   */
  public Object getMaxValue() {
    return maxValue;
  }

  /**
   * Get the number of null values in this column.
   */
  public long getNullCount() {
    return nullCount;
  }

  /**
   * Get the total number of values (including nulls) in this column.
   */
  public long getTotalCount() {
    return totalCount;
  }

  /**
   * Get the estimated number of distinct values using HyperLogLog.
   */
  public long getDistinctCount() {
    return hllSketch != null ? hllSketch.getEstimate() : Math.min(1000, totalCount);
  }

  /**
   * Get the HyperLogLog sketch for this column.
   */
  public HyperLogLogSketch getHllSketch() {
    return hllSketch;
  }

  /**
   * Get the column name.
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * Calculate selectivity for a predicate on this column.
   *
   * @param operator The comparison operator (=, <, >, <=, >=, !=)
   * @param value The comparison value
   * @return Selectivity estimate between 0.0 and 1.0
   */
  public double getSelectivity(String operator, Object value) {
    if (totalCount == 0) {
      return 0.0;
    }

    // Handle null comparisons
    if (value == null) {
      switch (operator) {
        case "IS NULL":
          return (double) nullCount / totalCount;
        case "IS NOT NULL":
          return 1.0 - ((double) nullCount / totalCount);
        default:
          return 0.0; // Comparing to null with other operators
      }
    }

    switch (operator) {
      case "=":
        // Equality selectivity = 1 / distinct_values
        return 1.0 / Math.max(1, getDistinctCount());

      case "!=":
        // Not-equal selectivity = 1 - equality_selectivity
        return 1.0 - (1.0 / Math.max(1, getDistinctCount()));

      case "<":
      case "<=":
      case ">":
      case ">=":
        // Range selectivity based on min/max values
        return calculateRangeSelectivity(operator, value);

      default:
        return 0.1; // Default selectivity for unknown operators
    }
  }

  @SuppressWarnings("unchecked")
  private double calculateRangeSelectivity(String operator, Object value) {
    if (minValue == null || maxValue == null || !isComparable(value)) {
      return 0.3; // Default for range queries without min/max
    }

    try {
      // Convert to comparable values
      Comparable<Object> min = (Comparable<Object>) minValue;
      Comparable<Object> max = (Comparable<Object>) maxValue;
      Comparable<Object> val = (Comparable<Object>) value;

      // Calculate position of value in range
      double position = calculateRelativePosition(min, max, val);

      switch (operator) {
        case "<":
          return Math.max(0.0, Math.min(1.0, position));
        case "<=":
          return Math.max(0.0, Math.min(1.0, position));
        case ">":
          return Math.max(0.0, Math.min(1.0, 1.0 - position));
        case ">=":
          return Math.max(0.0, Math.min(1.0, 1.0 - position));
        default:
          return 0.3;
      }
    } catch (Exception e) {
      return 0.3; // Fallback for comparison errors
    }
  }

  private boolean isComparable(Object value) {
    return value instanceof Comparable && minValue.getClass().equals(value.getClass());
  }

  @SuppressWarnings("unchecked")
  private double calculateRelativePosition(Object min, Object max, Object value) {
    // Handle different data types
    if (min instanceof Number && max instanceof Number && value instanceof Number) {
      double minNum = ((Number) min).doubleValue();
      double maxNum = ((Number) max).doubleValue();
      double valNum = ((Number) value).doubleValue();

      if (maxNum == minNum) {
        return 0.5; // Single value
      }

      return (valNum - minNum) / (maxNum - minNum);
    }

    if (min instanceof String && max instanceof String && value instanceof String) {
      // Simple lexicographic position estimate
      String minStr = (String) min;
      String maxStr = (String) max;
      String valStr = (String) value;

      if (valStr.compareTo(minStr) <= 0) {
        return 0.0;
      }
      if (valStr.compareTo(maxStr) >= 0) {
        return 1.0;
      }

      // Rough estimate based on string comparison
      return 0.5; // Simplified - could be improved with more sophisticated string positioning
    }

    // For other types, assume middle position
    // In a full implementation, this would handle other Comparable types properly
    return 0.5; // Assume middle position for non-Number, non-String types
  }

  @Override public String toString() {
    return String.format("ColumnStatistics{column='%s', min=%s, max=%s, nulls=%d, "
        + "total=%d, distinct=%d}",
        columnName, minValue, maxValue, nullCount, totalCount, getDistinctCount());
  }
}
