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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Enumerator that executes SQL queries against DuckDB.
 * 
 * <p>This enumerator:
 * <ul>
 *   <li>Creates a DuckDB connection</li>
 *   <li>Registers Parquet files as temporary views</li>
 *   <li>Executes pushed-down SQL queries</li>
 *   <li>Streams results efficiently</li>
 * </ul>
 */
public class DuckDBEnumerator implements Enumerator<Object[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBEnumerator.class);

  private final Connection connection;
  private final PreparedStatement statement;
  private final ResultSet resultSet;
  private final int columnCount;
  private Object[] current;
  private boolean hasNext = false;
  private boolean closed = false;
  private final boolean ownsConnection;  // Whether we created the connection

  /**
   * Creates a DuckDB enumerator using the shared connection.
   * 
   * @param schemaName The schema name to get the shared connection
   * @param sql SQL query to execute (already references the correct schema.table)
   * @param fieldNames expected field names for validation
   * @param duckdbConfig DuckDB configuration options
   */
  public DuckDBEnumerator(String schemaName, String sql, List<String> fieldNames, DuckDBConfig duckdbConfig) {
    try {
      LOGGER.debug("Using shared DuckDB connection for schema: {}", schemaName);
      
      // Use shared connection from the connection manager
      this.connection = DuckDBConnectionManager.getConnection(schemaName);
      this.ownsConnection = false;  // Don't close shared connection
      
      // No need to register views - they're already registered in DuckDBConnectionManager
      // No need to rewrite SQL - it already references the correct schema.table
      
      LOGGER.debug("Executing DuckDB SQL: {}", sql);
      
      // Execute the query directly
      this.statement = connection.prepareStatement(sql);
      this.resultSet = statement.executeQuery();
      
      // Get column metadata
      ResultSetMetaData metadata = resultSet.getMetaData();
      this.columnCount = metadata.getColumnCount();
      
      LOGGER.debug("DuckDB query initialized with {} columns", columnCount);
      
    } catch (SQLException e) {
      LOGGER.error("Failed to initialize DuckDB enumerator for schema: {}", schemaName, e);
      throw new RuntimeException("DuckDB query initialization failed", e);
    }
  }

  @Override
  public Object[] current() {
    if (closed) {
      throw new IllegalStateException("Enumerator has been closed");
    }
    return current;
  }

  @Override
  public boolean moveNext() {
    if (closed) {
      return false;
    }
    
    try {
      hasNext = resultSet.next();
      if (hasNext) {
        current = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
          current[i] = resultSet.getObject(i + 1);
        }
        LOGGER.debug("Read row: {}", java.util.Arrays.toString(current));
      } else {
        current = null;
        LOGGER.debug("No more rows available");
      }
      return hasNext;
    } catch (SQLException e) {
      LOGGER.error("Error reading from DuckDB result set", e);
      throw new RuntimeException("DuckDB result set read failed", e);
    }
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("DuckDB enumerator does not support reset");
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    
    closed = true;
    LOGGER.debug("Closing DuckDB enumerator");
    
    // Close resources in reverse order
    try {
      if (resultSet != null && !resultSet.isClosed()) {
        resultSet.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Error closing DuckDB result set", e);
    }
    
    try {
      if (statement != null && !statement.isClosed()) {
        statement.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Error closing DuckDB statement", e);
    }
    
    // Don't close the shared connection
    if (ownsConnection) {
      try {
        if (connection != null && !connection.isClosed()) {
          connection.close();
        }
      } catch (SQLException e) {
        LOGGER.warn("Error closing DuckDB connection", e);
      }
    }
  }

  /**
   * Creates an enumerable that uses DuckDB for query execution with default configuration.
   * 
   * @param schemaName schema name for the shared connection
   * @param sql SQL query to execute
   * @param fieldNames expected field names
   * @return enumerable that yields query results
   */
  public static Enumerable<Object[]> create(String schemaName, String sql, List<String> fieldNames) {
    return create(schemaName, sql, fieldNames, new DuckDBConfig());
  }

  /**
   * Creates an enumerable that uses DuckDB for query execution with custom configuration.
   * 
   * @param schemaName schema name for the shared connection
   * @param sql SQL query to execute
   * @param fieldNames expected field names
   * @param duckdbConfig DuckDB configuration options
   * @return enumerable that yields query results
   */
  public static Enumerable<Object[]> create(String schemaName, String sql, List<String> fieldNames, DuckDBConfig duckdbConfig) {
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new DuckDBEnumerator(schemaName, sql, fieldNames, duckdbConfig);
      }
    };
  }
  
  /**
   * Creates an enumerable for table scans with simplified parameters.
   * This method is called from generated code in DuckDBTableScan.
   * 
   * @param schemaName schema name for the shared connection
   * @param tableName table name (not used directly but kept for consistency)
   * @param sql SQL query to execute
   * @return enumerable that yields query results
   */
  public static Enumerable<Object[]> createEnumerable(String schemaName, String tableName, String sql) {
    LOGGER.debug("Creating enumerable for DuckDB table scan: {}.{} with SQL: {}", schemaName, tableName, sql);
    return create(schemaName, sql, null);
  }
}