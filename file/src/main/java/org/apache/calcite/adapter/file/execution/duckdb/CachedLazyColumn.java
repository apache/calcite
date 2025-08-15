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
 * A cached lazy column that maintains values across row iterations.
 * 
 * <p>This implementation optimizes for scenarios where the same columns
 * are accessed repeatedly across different rows. Once a column value is
 * fetched, it remains cached until explicitly invalidated.
 * 
 * <p>Key optimizations:
 * <ul>
 *   <li>Caches values across row iterations</li>
 *   <li>Reuses Object instances to minimize GC pressure</li>
 *   <li>Tracks access patterns for optimization hints</li>
 *   <li>Provides batch invalidation for efficient row advances</li>
 * </ul>
 */
public class CachedLazyColumn {
  private final int columnIndex; // 1-based for JDBC
  private Object value;
  private boolean loaded = false;
  private final ColumnAccessTracker tracker;
  
  // For reusable column instances
  private ResultSet currentResultSet;
  private long rowVersion = 0;
  private long cachedVersion = -1;
  
  public CachedLazyColumn(int columnIndex, ColumnAccessTracker tracker) {
    this.columnIndex = columnIndex;
    this.tracker = tracker;
  }
  
  /**
   * Updates the underlying ResultSet for a new row.
   * This doesn't clear the cache immediately - only invalidates it.
   */
  public void updateResultSet(ResultSet resultSet, long newRowVersion) {
    this.currentResultSet = resultSet;
    this.rowVersion = newRowVersion;
    // Don't clear the cache yet - wait for actual access
  }
  
  /**
   * Gets the value, using cache if valid or fetching if needed.
   */
  private Object getValue() {
    // Check if cache is valid for current row
    if (cachedVersion != rowVersion) {
      // Cache is stale, need to reload
      try {
        value = currentResultSet.getObject(columnIndex);
        cachedVersion = rowVersion;
        loaded = true;
        
        if (tracker != null) {
          tracker.recordAccess(columnIndex - 1); // Convert to 0-based
        }
        
      } catch (SQLException e) {
        throw new RuntimeException("Failed to load column " + columnIndex, e);
      }
    }
    return value;
  }
  
  /**
   * Forces cache invalidation without clearing the value.
   * This allows for lazy reloading on next access.
   */
  public void invalidateCache() {
    cachedVersion = -1;
  }
  
  /**
   * Checks if this column's cache is valid for the current row.
   */
  public boolean isCacheValid() {
    return cachedVersion == rowVersion;
  }
  
  @Override
  public String toString() {
    Object val = getValue();
    return val != null ? val.toString() : "null";
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CachedLazyColumn) {
      return getValue().equals(((CachedLazyColumn) obj).getValue());
    }
    return getValue().equals(obj);
  }
  
  @Override
  public int hashCode() {
    Object val = getValue();
    return val != null ? val.hashCode() : 0;
  }
  
  /**
   * Materializes the value for external use.
   */
  public Object materialize() {
    return getValue();
  }
  
  /**
   * Checks if this column has been accessed in the current row.
   */
  public boolean isLoadedForCurrentRow() {
    return cachedVersion == rowVersion;
  }
  
  /**
   * Factory for creating cached lazy rows with reusable column instances.
   */
  public static class CachedLazyRowFactory {
    private final CachedLazyColumn[] columnCache;
    private final ColumnAccessTracker tracker;
    private long currentRowVersion = 0;
    
    public CachedLazyRowFactory(int columnCount, ColumnAccessTracker tracker) {
      this.columnCache = new CachedLazyColumn[columnCount];
      this.tracker = tracker;
      
      // Pre-create all column instances
      for (int i = 0; i < columnCount; i++) {
        columnCache[i] = new CachedLazyColumn(i + 1, tracker);
      }
    }
    
    /**
     * Creates a lazy row for the current ResultSet position.
     * Returns the same Object[] array with updated column references.
     */
    public Object[] createRow(ResultSet resultSet) {
      currentRowVersion++;
      
      // Update all columns to point to new row
      for (CachedLazyColumn col : columnCache) {
        col.updateResultSet(resultSet, currentRowVersion);
      }
      
      // Return the column cache directly as Object[]
      // Calcite will see these as regular objects
      return columnCache.clone(); // Clone to prevent external modification
    }
    
    /**
     * Gets statistics about column access patterns.
     */
    public void logStatistics(String context) {
      if (tracker != null) {
        tracker.logStatistics(context);
        
        // Additional cache-specific stats
        int validCacheCount = 0;
        for (CachedLazyColumn col : columnCache) {
          if (col.isCacheValid()) {
            validCacheCount++;
          }
        }
        
        System.out.println("  Cache validity: " + validCacheCount + "/" + 
                         columnCache.length + " columns have valid cache");
      }
    }
  }
  
  /**
   * Tracks column access patterns for optimization.
   */
  public static class ColumnAccessTracker {
    private final boolean[] accessed;
    private final int[] accessCounts;
    private int totalAccesses = 0;
    private int rowCount = 0;
    
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
    
    public void recordRow() {
      rowCount++;
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
      
      System.out.println("CachedLazyColumn statistics for: " + context);
      System.out.println("  Rows processed: " + rowCount);
      System.out.println("  Columns accessed: " + accessedCount + "/" + accessed.length);
      System.out.println("  Total column accesses: " + totalAccesses);
      
      if (rowCount > 0) {
        double accessesPerRow = (double) totalAccesses / rowCount;
        System.out.println("  Average accesses per row: " + 
                         String.format("%.2f", accessesPerRow));
      }
      
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
        
        // Cache efficiency metric
        if (rowCount > 1 && totalAccesses > 0) {
          double cacheHitRate = 1.0 - ((double) totalAccesses / 
                                       (rowCount * accessedCount));
          if (cacheHitRate > 0) {
            System.out.println("  Cache efficiency: " + 
                             String.format("%.1f%%", cacheHitRate * 100) + 
                             " reduction in fetches");
          }
        }
      }
    }
    
    public boolean[] getAccessedColumns() {
      return accessed.clone();
    }
    
    public int getTotalAccesses() {
      return totalAccesses;
    }
  }
}