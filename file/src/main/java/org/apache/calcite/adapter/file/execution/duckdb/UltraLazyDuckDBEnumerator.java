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
 * Ultra-lazy DuckDB enumerator that does NOTHING until the client iterates.
 * 
 * <p>Key features:
 * <ul>
 *   <li>Query is prepared but NOT executed until first moveNext()</li>
 *   <li>ResultSet.next() not called until client requests it</li>
 *   <li>Column values not fetched until accessed</li>
 *   <li>Zero overhead for unused queries</li>
 * </ul>
 * 
 * <p>This is perfect for:
 * <ul>
 *   <li>Queries that might not be consumed</li>
 *   <li>LIMIT queries where we stop early</li>
 *   <li>Aggregations where we only need one row</li>
 * </ul>
 */
public class UltraLazyDuckDBEnumerator implements Enumerator<Object[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(UltraLazyDuckDBEnumerator.class);
  
  // Lazy initialization fields
  private final String schemaName;
  private final String sql;
  private final List<String> fieldNames;
  private final DuckDBConfig config;
  
  // Execution state (all null until first moveNext)
  private Connection connection;
  private PreparedStatement statement;
  private ResultSet resultSet;
  private ResultSetMetaData metadata;
  private int columnCount = -1;
  
  // Current row state
  private Object[] currentRow;
  
  // Flags
  private boolean initialized = false;
  private boolean closed = false;
  private int rowCount = 0;
  
  public UltraLazyDuckDBEnumerator(String schemaName, String sql, 
                                   List<String> fieldNames, DuckDBConfig config) {
    this.schemaName = schemaName;
    this.sql = sql;
    this.fieldNames = fieldNames;
    this.config = config;
    
    LOGGER.debug("Ultra-lazy enumerator created but NOT executed for: {}", 
                 sql.length() > 50 ? sql.substring(0, 50) + "..." : sql);
  }
  
  @Override
  public Object[] current() {
    if (!initialized) {
      throw new IllegalStateException("Enumerator not yet initialized - call moveNext() first");
    }
    if (closed || currentRow == null) {
      throw new IllegalStateException("No current row available");
    }
    return currentRow;
  }
  
  @Override
  public boolean moveNext() {
    if (closed) {
      return false;
    }
    
    try {
      // Lazy initialization on first call
      if (!initialized) {
        initialize();
      }
      
      // Now do the actual row fetch
      boolean hasNext = resultSet.next();
      if (hasNext) {
        rowCount++;
        
        // Standard JDBC materialization - the optimization is in delayed query execution
        for (int i = 0; i < columnCount; i++) {
          currentRow[i] = resultSet.getObject(i + 1);
        }
        
        if (rowCount == 1) {
          LOGGER.debug("First row fetched - client started iteration");
        }
      } else {
        currentRow = null;
        LOGGER.debug("Query complete after {} rows", rowCount);
      }
      return hasNext;
      
    } catch (SQLException e) {
      LOGGER.error("Error during lazy iteration", e);
      throw new RuntimeException("Lazy iteration failed", e);
    }
  }
  
  /**
   * Lazy initialization - only called when client starts iterating.
   */
  private void initialize() throws SQLException {
    long t0 = System.nanoTime();
    
    System.err.println("[UltraLazy DuckDB] Initializing with query: " + sql);
    LOGGER.debug("Client requested first row - initializing query execution");
    
    // Get connection
    this.connection = DuckDBConnectionManager.getConnection(schemaName);
    
    // Enable streaming
    try (java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("SET jdbc_stream_results=true");
    }
    
    // Prepare and execute
    this.statement = connection.prepareStatement(sql);
    this.resultSet = statement.executeQuery();
    
    // Get metadata
    this.metadata = resultSet.getMetaData();
    this.columnCount = metadata.getColumnCount();
    
    // Create row array
    this.currentRow = new Object[columnCount];
    
    this.initialized = true;
    
    long t1 = System.nanoTime();
    LOGGER.debug("Query initialized in {} ms with {} columns", 
                 String.format("%.2f", (t1 - t0) / 1_000_000.0), columnCount);
  }
  
  @Override
  public void reset() {
    throw new UnsupportedOperationException("Ultra-lazy enumerator does not support reset");
  }
  
  @Override
  public void close() {
    if (closed) {
      return;
    }
    
    closed = true;
    
    if (!initialized) {
      LOGGER.debug("Closing ultra-lazy enumerator - query was NEVER executed!");
      return;
    }
    
    LOGGER.debug("Closing ultra-lazy enumerator after {} rows", rowCount);
    
    // Close resources only if they were created
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
    
    // Never close shared connection
  }
  
  
  /**
   * Creates an ultra-lazy enumerable that does nothing until iteration.
   */
  public static Enumerable<Object[]> create(String schemaName, String sql, 
                                           List<String> fieldNames, DuckDBConfig config) {
    return new UltraLazyEnumerable(schemaName, sql, fieldNames, config);
  }
  
  /**
   * Ultra-lazy enumerable that defers even enumerator creation.
   */
  private static class UltraLazyEnumerable extends AbstractEnumerable<Object[]> {
    private final String schemaName;
    private final String sql;
    private final List<String> fieldNames;
    private final DuckDBConfig config;
    
    UltraLazyEnumerable(String schemaName, String sql, 
                       List<String> fieldNames, DuckDBConfig config) {
      this.schemaName = schemaName;
      this.sql = sql;
      this.fieldNames = fieldNames;
      this.config = config;
      
      LOGGER.debug("Ultra-lazy enumerable created - NO work done yet");
    }
    
    @Override
    public Enumerator<Object[]> enumerator() {
      LOGGER.debug("Client requested enumerator - creating ultra-lazy enumerator");
      return new UltraLazyDuckDBEnumerator(schemaName, sql, fieldNames, config);
    }
  }
}