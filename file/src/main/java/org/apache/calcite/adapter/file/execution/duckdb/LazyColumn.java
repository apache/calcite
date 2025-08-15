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
package org.apache.calcite.adapter.file.execution.duckdb;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A lazy column value that only fetches from ResultSet when actually used.
 * 
 * <p>This allows us to return Object[] where each element is a LazyColumn.
 * The column value is only fetched from the database when:
 * <ul>
 *   <li>toString() is called</li>
 *   <li>equals() is called</li>
 *   <li>hashCode() is called</li>
 *   <li>The value is cast to its actual type</li>
 * </ul>
 * 
 * <p>For queries like COUNT(*), the columns are never accessed, so no data is fetched.
 */
public class LazyColumn {
  private final ResultSet resultSet;
  private final int columnIndex; // 1-based for JDBC
  private Object value;
  private boolean loaded = false;
  private final ColumnAccessTracker tracker;
  
  public LazyColumn(ResultSet resultSet, int columnIndex, ColumnAccessTracker tracker) {
    this.resultSet = resultSet;
    this.columnIndex = columnIndex;
    this.tracker = tracker;
  }
  
  /**
   * Gets the actual value, loading it if necessary.
   */
  private Object getValue() {
    if (!loaded) {
      try {
        value = resultSet.getObject(columnIndex);
        loaded = true;
        
        if (tracker != null) {
          tracker.recordAccess(columnIndex - 1); // Convert to 0-based for tracking
        }
        
      } catch (SQLException e) {
        throw new RuntimeException("Failed to load column " + columnIndex, e);
      }
    }
    return value;
  }
  
  /**
   * Resets this lazy column for reuse with a new row.
   * Called when the ResultSet advances to the next row.
   */
  public void reset() {
    loaded = false;
    value = null;
  }
  
  @Override
  public String toString() {
    Object val = getValue();
    return val != null ? val.toString() : "null";
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LazyColumn) {
      return getValue().equals(((LazyColumn) obj).getValue());
    }
    return getValue().equals(obj);
  }
  
  @Override
  public int hashCode() {
    Object val = getValue();
    return val != null ? val.hashCode() : 0;
  }
  
  /**
   * Provides a way to get the value without triggering toString().
   */
  public Object materialize() {
    return getValue();
  }
  
  /**
   * Checks if this column has been loaded yet.
   */
  public boolean isLoaded() {
    return loaded;
  }
  
  /**
   * Static factory for creating lazy column arrays.
   */
  public static Object[] createLazyRow(ResultSet resultSet, int columnCount, 
                                       LazyColumn[] reusableColumns,
                                       ColumnAccessTracker tracker) {
    Object[] row = new Object[columnCount];
    
    for (int i = 0; i < columnCount; i++) {
      if (reusableColumns[i] == null) {
        // Create lazy column wrapper on first use
        reusableColumns[i] = new LazyColumn(resultSet, i + 1, tracker);
      } else {
        // Reset for reuse
        reusableColumns[i].reset();
      }
      row[i] = reusableColumns[i];
    }
    
    return row;
  }
  
  /**
   * Tracks column access patterns.
   */
  public static class ColumnAccessTracker {
    private final boolean[] accessed;
    private final int[] accessCounts;
    private int totalAccesses = 0;
    
    public ColumnAccessTracker(int columnCount) {
      this.accessed = new boolean[columnCount];
      this.accessCounts = new int[columnCount];
    }
    
    public void recordAccess(int columnIndex) {
      if (!accessed[columnIndex]) {
        accessed[columnIndex] = true;
      }
      accessCounts[columnIndex]++;
      totalAccesses++;
    }
    
    public void logStatistics(String context) {
      int accessedCount = 0;
      int maxAccesses = 0;
      int mostAccessedColumn = -1;
      
      for (int i = 0; i < accessed.length; i++) {
        if (accessed[i]) {
          accessedCount++;
          if (accessCounts[i] > maxAccesses) {
            maxAccesses = accessCounts[i];
            mostAccessedColumn = i;
          }
        }
      }
      
      System.out.println("LazyColumn statistics for: " + context);
      System.out.println("  Columns accessed: " + accessedCount + "/" + accessed.length);
      System.out.println("  Total column accesses: " + totalAccesses);
      
      if (accessedCount == 0) {
        System.out.println("  PERFECT: No columns accessed (pure row counting)");
      } else {
        System.out.println("  Most accessed column: " + mostAccessedColumn + 
                         " (" + maxAccesses + " times)");
        
        if (accessedCount < accessed.length / 2) {
          System.out.println("  OPTIMIZATION: Only " + 
                           (accessedCount * 100 / accessed.length) + 
                           "% of columns used - consider projection pushdown");
        }
      }
    }
  }
}