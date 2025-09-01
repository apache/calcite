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

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics for a table including row count, data size, and column statistics.
 * Used by Aperio-db's cost-based optimizer for intelligent query planning.
 */
public class TableStatistics {
  private final long rowCount;
  private final long dataSize;
  private final Map<String, ColumnStatistics> columnStats;
  private final long lastUpdated;
  private final String sourceHash;

  public TableStatistics(long rowCount, long dataSize,
                        Map<String, ColumnStatistics> columnStats,
                        String sourceHash) {
    this.rowCount = rowCount;
    this.dataSize = dataSize;
    this.columnStats = new HashMap<>(columnStats);
    this.lastUpdated = System.currentTimeMillis();
    this.sourceHash = sourceHash;
  }

  /**
   * Get the estimated number of rows in the table.
   */
  public long getRowCount() {
    return rowCount;
  }

  /**
   * Get the estimated data size in bytes.
   */
  public long getDataSize() {
    return dataSize;
  }

  /**
   * Get statistics for a specific column.
   */
  public ColumnStatistics getColumnStatistics(String columnName) {
    return columnStats.get(columnName);
  }

  /**
   * Get all column statistics.
   */
  public Map<String, ColumnStatistics> getColumnStatistics() {
    return new HashMap<>(columnStats);
  }

  /**
   * Get the timestamp when these statistics were last updated.
   */
  public long getLastUpdated() {
    return lastUpdated;
  }

  /**
   * Get a hash of the source data used to generate these statistics.
   * Used for cache invalidation.
   */
  public String getSourceHash() {
    return sourceHash;
  }

  /**
   * Check if these statistics are still valid for the given source hash.
   */
  public boolean isValidFor(String currentSourceHash) {
    return sourceHash != null && sourceHash.equals(currentSourceHash);
  }

  /**
   * Get the selectivity estimate for a column predicate.
   *
   * @param columnName The column name
   * @param operator The comparison operator (=, <, >, etc.)
   * @param value The comparison value
   * @return Selectivity estimate between 0.0 and 1.0
   */
  public double getSelectivity(String columnName, String operator, Object value) {
    ColumnStatistics colStats = columnStats.get(columnName);
    if (colStats == null) {
      return 0.1; // Default selectivity when no statistics available
    }

    return colStats.getSelectivity(operator, value);
  }

  /**
   * Create a simple table statistics object with basic estimates.
   */
  public static TableStatistics createBasicEstimate(long rowCount) {
    return new TableStatistics(rowCount, rowCount * 100, new HashMap<>(), null);
  }

  @Override public String toString() {
    return String.format("TableStatistics{rowCount=%d, dataSize=%d, columns=%d, lastUpdated=%d}",
        rowCount, dataSize, columnStats.size(), lastUpdated);
  }
}
