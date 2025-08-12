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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages DuckDB connections for FileSchema instances.
 * 
 * <p>This manager:
 * <ul>
 *   <li>Creates a shared in-memory DuckDB instance per schema</li>
 *   <li>Initializes schemas and registers Parquet files as views</li>
 *   <li>Provides connections for query execution</li>
 *   <li>Handles cleanup on shutdown</li>
 * </ul>
 */
public class DuckDBConnectionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBConnectionManager.class);
  
  // Map of schema name to DuckDB connection
  private static final Map<String, Connection> SCHEMA_CONNECTIONS = new ConcurrentHashMap<>();
  
  // Map of schema name to initialization status
  private static final Map<String, Boolean> SCHEMA_INITIALIZED = new ConcurrentHashMap<>();
  
  static {
    // Register shutdown hook to close connections
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      for (Map.Entry<String, Connection> entry : SCHEMA_CONNECTIONS.entrySet()) {
        try {
          if (!entry.getValue().isClosed()) {
            entry.getValue().close();
            LOGGER.info("Closed DuckDB connection for schema: {}", entry.getKey());
          }
        } catch (SQLException e) {
          LOGGER.error("Failed to close DuckDB connection for schema: {}", entry.getKey(), e);
        }
      }
    }));
  }
  
  /**
   * Gets or creates a DuckDB connection for the given schema.
   * 
   * @param schemaName The name of the schema
   * @return A DuckDB connection for the schema
   */
  public static synchronized Connection getConnection(String schemaName) {
    return SCHEMA_CONNECTIONS.computeIfAbsent(schemaName, name -> {
      try {
        // Load driver if needed
        try {
          Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("DuckDB driver not found", e);
        }
        
        // Create in-memory database for this schema
        // Each schema gets its own in-memory database to avoid conflicts
        String url = "jdbc:duckdb:";  // In-memory database
        Connection conn = DriverManager.getConnection(url);
        
        LOGGER.info("Created DuckDB connection for schema: {}", name);
        
        // Set optimization options
        try (Statement stmt = conn.createStatement()) {
          // Enable parallel execution
          stmt.execute("SET threads TO 4");
          
          // Enable Parquet optimizations
          stmt.execute("SET enable_object_cache TO true");
          
          // Set memory limit (optional, adjust as needed)
          // stmt.execute("SET memory_limit = '4GB'");
        }
        
        return conn;
        
      } catch (SQLException e) {
        throw new RuntimeException("Failed to create DuckDB connection for schema: " + name, e);
      }
    });
  }
  
  /**
   * Initializes a schema in DuckDB and registers Parquet files as views.
   * 
   * @param schemaName The schema name to create in DuckDB
   * @param parquetFiles Map of table names to Parquet file paths
   */
  public static void initializeSchema(String schemaName, Map<String, String> parquetFiles) {
    // Check if already initialized
    if (SCHEMA_INITIALIZED.getOrDefault(schemaName, false)) {
      LOGGER.debug("Schema {} already initialized", schemaName);
      return;
    }
    
    Connection conn = getConnection(schemaName);
    
    try {
      // Create schema
      try (Statement stmt = conn.createStatement()) {
        String createSchema = "CREATE SCHEMA IF NOT EXISTS " + schemaName;
        stmt.execute(createSchema);
        LOGGER.info("Created DuckDB schema: {}", schemaName);
      }
      
      // Register each Parquet file as a view
      for (Map.Entry<String, String> entry : parquetFiles.entrySet()) {
        String tableName = entry.getKey();
        String parquetPath = entry.getValue();
        
        registerParquetView(conn, schemaName, tableName, parquetPath);
      }
      
      SCHEMA_INITIALIZED.put(schemaName, true);
      LOGGER.info("Initialized schema {} with {} tables", schemaName, parquetFiles.size());
      
    } catch (SQLException e) {
      LOGGER.error("Failed to initialize schema: {}", schemaName, e);
      throw new RuntimeException("Schema initialization failed", e);
    }
  }
  
  /**
   * Registers a single Parquet file as a view in DuckDB.
   * 
   * @param conn The DuckDB connection
   * @param schemaName The schema name
   * @param tableName The table/view name
   * @param parquetPath The path to the Parquet file
   */
  private static void registerParquetView(Connection conn, String schemaName, 
                                         String tableName, String parquetPath) {
    try {
      // Drop view if it exists (for re-registration)
      String dropView = String.format("DROP VIEW IF EXISTS %s.%s", schemaName, tableName);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(dropView);
      }
      
      // Create view from Parquet file
      String createView = String.format(
          "CREATE VIEW %s.%s AS SELECT * FROM read_parquet('%s')",
          schemaName, tableName, parquetPath.replace("'", "''")
      );
      
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(createView);
        LOGGER.debug("Registered Parquet view: {}.{} -> {}", schemaName, tableName, parquetPath);
      }
      
    } catch (SQLException e) {
      LOGGER.error("Failed to register Parquet view: {}.{}", schemaName, tableName, e);
      throw new RuntimeException("Failed to register Parquet file: " + parquetPath, e);
    }
  }
  
  /**
   * Adds a new Parquet file to an existing schema.
   * 
   * @param schemaName The schema name
   * @param tableName The table name
   * @param parquetPath The path to the Parquet file
   */
  public static void addParquetFile(String schemaName, String tableName, String parquetPath) {
    Connection conn = getConnection(schemaName);
    
    // Ensure schema exists
    if (!SCHEMA_INITIALIZED.getOrDefault(schemaName, false)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        SCHEMA_INITIALIZED.put(schemaName, true);
      } catch (SQLException e) {
        throw new RuntimeException("Failed to create schema: " + schemaName, e);
      }
    }
    
    registerParquetView(conn, schemaName, tableName, parquetPath);
  }
  
  /**
   * Removes a schema and its connection.
   * 
   * @param schemaName The schema name to remove
   */
  public static void removeSchema(String schemaName) {
    Connection conn = SCHEMA_CONNECTIONS.remove(schemaName);
    if (conn != null) {
      try {
        if (!conn.isClosed()) {
          conn.close();
        }
        SCHEMA_INITIALIZED.remove(schemaName);
        LOGGER.info("Removed schema: {}", schemaName);
      } catch (SQLException e) {
        LOGGER.error("Failed to close connection for schema: {}", schemaName, e);
      }
    }
  }
  
  /**
   * Checks if DuckDB is available.
   * 
   * @return true if DuckDB driver is available
   */
  public static boolean isAvailable() {
    return DuckDBExecutionEngine.isAvailable();
  }
}