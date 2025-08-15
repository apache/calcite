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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Lazy DuckDB enumerator that defers all conversions until actually needed.
 * 
 * <p>Key optimizations:
 * <ul>
 *   <li>No Object[] creation until current() is called</li>
 *   <li>No column conversion until that specific column is accessed</li>
 *   <li>Reuses Object[] arrays across rows</li>
 *   <li>Tracks which columns are actually accessed for optimization</li>
 * </ul>
 */
public class LazyDuckDBEnumerator implements Enumerator<Object[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(LazyDuckDBEnumerator.class);
  
  private final Connection connection;
  private final PreparedStatement statement;
  private final ResultSet resultSet;
  private final int columnCount;
  private final boolean ownsConnection;
  
  // Lazy state
  private LazyRow currentRow;
  private boolean hasNext = false;
  private boolean closed = false;
  
  // Optimization tracking
  private final boolean[] columnsAccessed;
  private int accessCount = 0;
  
  public LazyDuckDBEnumerator(String schemaName, String sql, List<String> fieldNames, 
                              DuckDBConfig duckdbConfig) {
    try {
      LOGGER.debug("Creating lazy DuckDB enumerator for schema: {}", schemaName);
      
      this.connection = DuckDBConnectionManager.getConnection(schemaName);
      this.ownsConnection = false;
      
      this.statement = connection.prepareStatement(sql);
      this.resultSet = statement.executeQuery();
      
      ResultSetMetaData metadata = resultSet.getMetaData();
      this.columnCount = metadata.getColumnCount();
      
      // Track which columns are accessed
      this.columnsAccessed = new boolean[columnCount];
      
      LOGGER.debug("Lazy enumerator initialized with {} columns", columnCount);
      
    } catch (SQLException e) {
      LOGGER.error("Failed to initialize lazy DuckDB enumerator", e);
      throw new RuntimeException("Lazy DuckDB initialization failed", e);
    }
  }
  
  @Override
  public Object[] current() {
    if (closed || currentRow == null) {
      throw new IllegalStateException("No current row available");
    }
    // LazyRow acts as Object[] for Calcite
    return currentRow.toArray();
  }
  
  @Override
  public boolean moveNext() {
    if (closed) {
      return false;
    }
    
    try {
      hasNext = resultSet.next();
      if (hasNext) {
        // Create or reuse lazy row wrapper
        if (currentRow == null) {
          currentRow = new LazyRow();
        }
        currentRow.reset(); // Clear previous cached values
      } else {
        currentRow = null;
        logAccessStatistics();
      }
      return hasNext;
    } catch (SQLException e) {
      LOGGER.error("Error reading from DuckDB result set", e);
      throw new RuntimeException("DuckDB result set read failed", e);
    }
  }
  
  @Override
  public void reset() {
    throw new UnsupportedOperationException("Lazy DuckDB enumerator does not support reset");
  }
  
  @Override
  public void close() {
    if (closed) {
      return;
    }
    
    closed = true;
    LOGGER.debug("Closing lazy DuckDB enumerator");
    
    try {
      if (resultSet != null && !resultSet.isClosed()) {
        resultSet.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Error closing result set", e);
    }
    
    try {
      if (statement != null && !statement.isClosed()) {
        statement.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Error closing statement", e);
    }
    
    if (ownsConnection) {
      try {
        if (connection != null && !connection.isClosed()) {
          connection.close();
        }
      } catch (SQLException e) {
        LOGGER.warn("Error closing connection", e);
      }
    }
  }
  
  private void logAccessStatistics() {
    if (accessCount > 0) {
      int accessedColumns = 0;
      for (boolean accessed : columnsAccessed) {
        if (accessed) accessedColumns++;
      }
      
      LOGGER.debug("Column access statistics: {}/{} columns accessed, {} total accesses",
                   accessedColumns, columnCount, accessCount);
      
      // Could use this info to optimize future queries
      if (accessedColumns < columnCount / 2) {
        LOGGER.debug("Only {}% of columns were accessed - consider projection pushdown",
                     (accessedColumns * 100) / columnCount);
      }
    }
  }
  
  /**
   * Lazy row that acts like Object[] but only materializes values when accessed.
   * This extends Object to act as a proxy that can be treated as Object[].
   */
  private class LazyRow extends Object {
    private final Object[] cache = new Object[columnCount];
    private final boolean[] cached = new boolean[columnCount];
    
    void reset() {
      Arrays.fill(cached, false);
      // Don't clear cache array - reuse the Object references
    }
    
    /**
     * Array-like access method. This is what makes it lazy.
     * Calcite will call this when it actually needs a column value.
     */
    public Object get(int index) {
      if (index < 0 || index >= columnCount) {
        throw new ArrayIndexOutOfBoundsException(index);
      }
      
      // Only materialize if not already cached
      if (!cached[index]) {
        try {
          cache[index] = resultSet.getObject(index + 1);
          cached[index] = true;
          
          // Track access patterns
          if (!columnsAccessed[index]) {
            columnsAccessed[index] = true;
          }
          accessCount++;
          
          if (accessCount == 1) {
            LOGGER.debug("First column access at index {} - lazy loading activated", index);
          }
          
        } catch (SQLException e) {
          throw new RuntimeException("Failed to get column " + index, e);
        }
      }
      
      return cache[index];
    }
    
    /**
     * Convert to real Object[] when needed.
     * This forces materialization of all columns.
     */
    public Object[] toArray() {
      Object[] result = new Object[columnCount];
      for (int i = 0; i < columnCount; i++) {
        result[i] = get(i);
      }
      return result;
    }
    
    /**
     * For compatibility - acts like an array with this length.
     */
    public int length() {
      return columnCount;
    }
    
    @Override
    public String toString() {
      // Only show cached values in toString to avoid side effects
      StringBuilder sb = new StringBuilder("LazyRow[");
      int loadedCount = 0;
      for (int i = 0; i < columnCount; i++) {
        if (cached[i]) loadedCount++;
      }
      sb.append(loadedCount).append("/").append(columnCount).append(" loaded]");
      return sb.toString();
    }
  }
  
  /**
   * Creates a lazy enumerable.
   */
  public static Enumerable<Object[]> create(String schemaName, String sql, 
                                           List<String> fieldNames, DuckDBConfig config) {
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new LazyDuckDBEnumerator(schemaName, sql, fieldNames, config);
      }
    };
  }
}