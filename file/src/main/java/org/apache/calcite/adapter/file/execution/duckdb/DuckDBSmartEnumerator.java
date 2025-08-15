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
import java.util.List;

/**
 * Smart DuckDB enumerator that chooses the optimal execution strategy.
 * 
 * <p>This enumerator can:
 * <ul>
 *   <li>Return results directly without enumeration for aggregations</li>
 *   <li>Use Arrow vectors for large result sets</li>
 *   <li>Fall back to JDBC for small results or compatibility</li>
 * </ul>
 */
public class DuckDBSmartEnumerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBSmartEnumerator.class);
  
  /**
   * Creates an optimized enumerable based on query analysis.
   * 
   * @param schemaName schema name for the connection
   * @param sql SQL query to execute
   * @param fieldNames expected field names
   * @param config DuckDB configuration
   * @param needsEnumeration whether Calcite needs row-by-row processing
   * @param estimatedRows estimated result size (-1 if unknown)
   * @return optimized enumerable
   */
  public static Enumerable<Object[]> create(String schemaName, String sql, 
                                           List<String> fieldNames,
                                           DuckDBConfig config,
                                           boolean needsEnumeration,
                                           long estimatedRows) {
    
    // Analyze query to determine strategy
    DuckDBQueryAnalyzer.ExecutionStrategy strategy = 
        DuckDBQueryAnalyzer.analyzeQuery(sql, estimatedRows, needsEnumeration);
    
    LOGGER.debug("Query strategy: {} for SQL: {}", strategy, 
                 sql.length() > 100 ? sql.substring(0, 100) + "..." : sql);
    
    switch (strategy) {
      case DIRECT_RESULT:
        return createDirectResult(schemaName, sql);
        
      case FULL_PUSHDOWN:
        return createFullPushdown(schemaName, sql);
        
      case ARROW_ENUMERATION:
        // Arrow has 400+ms overhead - never use it
        LOGGER.debug("Arrow requested but using ultra-lazy JDBC instead (Arrow overhead too high)");
        // Fall through to JDBC
        
      case JDBC_ENUMERATION:
      default:
        LOGGER.debug("Using ultra-lazy JDBC enumeration");
        return UltraLazyDuckDBEnumerator.create(schemaName, sql, fieldNames, config);
    }
  }
  
  /**
   * Creates a direct result enumerable for single-value queries.
   * This avoids enumeration entirely for queries like COUNT(*).
   */
  private static Enumerable<Object[]> createDirectResult(String schemaName, String sql) {
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new DirectResultEnumerator(schemaName, sql);
      }
    };
  }
  
  /**
   * Creates a full pushdown enumerable for complete DuckDB handling.
   */
  private static Enumerable<Object[]> createFullPushdown(String schemaName, String sql) {
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new FullPushdownEnumerator(schemaName, sql);
      }
    };
  }
  
  /**
   * Enumerator for single-result queries (e.g., COUNT(*)).
   */
  private static class DirectResultEnumerator implements Enumerator<Object[]> {
    private final String schemaName;
    private final String sql;
    private Object[] result;
    private boolean hasResult = false;
    private boolean fetched = false;
    
    DirectResultEnumerator(String schemaName, String sql) {
      this.schemaName = schemaName;
      this.sql = sql;
    }
    
    @Override
    public Object[] current() {
      return result;
    }
    
    @Override
    public boolean moveNext() {
      if (fetched) {
        return false; // Already returned the single result
      }
      
      if (!hasResult) {
        // Execute query and get single result
        try {
          Connection conn = DuckDBConnectionManager.getConnection(schemaName);
          try (PreparedStatement stmt = conn.prepareStatement(sql);
               ResultSet rs = stmt.executeQuery()) {
            
            if (rs.next()) {
              ResultSetMetaData meta = rs.getMetaData();
              int columnCount = meta.getColumnCount();
              result = new Object[columnCount];
              
              for (int i = 0; i < columnCount; i++) {
                result[i] = rs.getObject(i + 1);
              }
              hasResult = true;
              
              LOGGER.debug("Direct result query returned: {} columns", columnCount);
            }
          }
        } catch (SQLException e) {
          throw new RuntimeException("Failed to execute direct result query", e);
        }
      }
      
      fetched = true;
      return hasResult;
    }
    
    @Override
    public void reset() {
      fetched = false;
    }
    
    @Override
    public void close() {
      // Nothing to close - using shared connection
    }
  }
  
  /**
   * Enumerator for full DuckDB pushdown with minimal overhead.
   */
  private static class FullPushdownEnumerator implements Enumerator<Object[]> {
    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;
    private final int columnCount;
    private Object[] current;
    
    FullPushdownEnumerator(String schemaName, String sql) {
      try {
        this.connection = DuckDBConnectionManager.getConnection(schemaName);
        this.statement = connection.prepareStatement(sql);
        this.resultSet = statement.executeQuery();
        
        ResultSetMetaData metadata = resultSet.getMetaData();
        this.columnCount = metadata.getColumnCount();
        
        LOGGER.debug("Full pushdown query initialized with {} columns", columnCount);
        
      } catch (SQLException e) {
        throw new RuntimeException("Failed to initialize full pushdown query", e);
      }
    }
    
    @Override
    public Object[] current() {
      return current;
    }
    
    @Override
    public boolean moveNext() {
      try {
        if (resultSet.next()) {
          current = new Object[columnCount];
          for (int i = 0; i < columnCount; i++) {
            current[i] = resultSet.getObject(i + 1);
          }
          return true;
        }
        return false;
      } catch (SQLException e) {
        throw new RuntimeException("Failed to read result", e);
      }
    }
    
    @Override
    public void reset() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void close() {
      try {
        if (resultSet != null) resultSet.close();
        if (statement != null) statement.close();
        // Don't close shared connection
      } catch (SQLException e) {
        LOGGER.warn("Error closing resources", e);
      }
    }
  }
}