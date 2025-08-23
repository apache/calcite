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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Factory for creating JDBC schema backed by DuckDB.
 * Uses standard JDBC adapter for proper query pushdown.
 */
public class DuckDBJdbcSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBJdbcSchemaFactory.class);
  
  static {
    LOGGER.debug("[DuckDBJdbcSchemaFactory] Class loaded");
  }
  
  /**
   * Creates a JDBC schema for DuckDB with files registered as views.
   * Configures with Oracle Lex and unquoted casing to lower.
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName, File directory) {
    return create(parentSchema, schemaName, directory, false);
  }
  
  /**
   * Creates a JDBC schema for DuckDB with files registered as views.
   * Creates a single persistent connection that lives with the schema.
   * Configures with Oracle Lex and unquoted casing to lower.
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName, 
                                 File directory, boolean recursive) {
    return create(parentSchema, schemaName, directory, recursive, null);
  }
  
  /**
   * Creates a JDBC schema for DuckDB with files registered as views.
   * Creates a single persistent connection that lives with the schema.
   * Configures with Oracle Lex and unquoted casing to lower.
   * @param fileSchema The FileSchema that handles conversions and refreshes (kept alive)
   */
  public static JdbcSchema create(SchemaPlus parentSchema, String schemaName, 
                                 File directory, boolean recursive,
                                 org.apache.calcite.adapter.file.FileSchema fileSchema) {
    LOGGER.debug("[DuckDBJdbcSchemaFactory] create() called with fileSchema for schema: {}", schemaName);
    LOGGER.info("Creating DuckDB JDBC schema for: {} with name: {} (recursive={}, hasFileSchema={})", 
                directory, schemaName, recursive, fileSchema != null);
    
    try {
      Class.forName("org.duckdb.DuckDBDriver");
      
      // Use a named in-memory database that persists across connections
      // The name is based on the schema name and a unique ID to ensure isolation
      String dbName = "calcite_" + schemaName + "_" + System.nanoTime();
      String jdbcUrl = "jdbc:duckdb:" + dbName;
      
      LOGGER.info("Using named DuckDB in-memory database: {}", dbName);
      
      // Create initial connection for setup
      Connection setupConn = DriverManager.getConnection(jdbcUrl);
      
      // Configure DuckDB settings for production use
      setupConn.createStatement().execute("SET threads TO 4");  // Adjust based on workload
      setupConn.createStatement().execute("SET memory_limit = '4GB'");  // Prevent OOM
      setupConn.createStatement().execute("SET max_memory = '4GB'");  // Hard limit
      setupConn.createStatement().execute("SET temp_directory = '" + System.getProperty("java.io.tmpdir") + "'");  // Spill location
      setupConn.createStatement().execute("SET preserve_insertion_order = false");  // Better performance
      setupConn.createStatement().execute("SET enable_progress_bar = false");  // Cleaner output
      setupConn.createStatement().execute("SET enable_object_cache = true");  // Cache parsed files
      
      // Create a schema matching the FileSchema name (in lowercase for DuckDB)
      String duckdbSchema = schemaName.toLowerCase();
      setupConn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + duckdbSchema);
      LOGGER.info("Created DuckDB schema: {}", duckdbSchema);
      
      // Register Parquet files as views
      // FileSchemaFactory has already run conversions via FileSchema
      // Pass the FileSchema to use its unique instance ID for cache lookup
      registerFilesAsViews(setupConn, directory, recursive, duckdbSchema, schemaName, fileSchema);
      
      // Debug: List all registered views
      try (Statement stmt = setupConn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT table_schema, table_name, table_type FROM information_schema.tables")) {
        LOGGER.info("All DuckDB tables and views:");
        while (rs.next()) {
          LOGGER.info("  - Schema: {}, Name: {}, Type: {}", 
                     rs.getString("table_schema"), 
                     rs.getString("table_name"), 
                     rs.getString("table_type"));
        }
      }
      
      // DON'T close the setup connection - keep it alive to maintain the database
      // This connection will be owned by DuckDBJdbcSchema
      
      // Create a DataSource that creates new connections to the named database
      final String finalJdbcUrl = jdbcUrl;
      DataSource dataSource = new DataSource() {
        @Override
        public Connection getConnection() throws SQLException {
          // Create a new connection to the named in-memory database
          return DriverManager.getConnection(finalJdbcUrl);
        }
        
        @Override
        public Connection getConnection(String username, String password) throws SQLException {
          return getConnection();
        }
        
        @Override
        public PrintWriter getLogWriter() { return null; }
        @Override
        public void setLogWriter(PrintWriter out) { }
        @Override
        public void setLoginTimeout(int seconds) { }
        @Override
        public int getLoginTimeout() { return 0; }
        @Override
        public java.util.logging.Logger getParentLogger() { 
          return java.util.logging.Logger.getLogger("DuckDB");
        }
        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
          if (iface.isInstance(this)) return iface.cast(this);
          throw new SQLException("Cannot unwrap to " + iface);
        }
        @Override
        public boolean isWrapperFor(Class<?> iface) {
          return iface.isInstance(this);
        }
      };
      
      SqlDialect dialect = createDuckDBDialectWithCustomLex();
      
      Expression expression = Schemas.subSchemaExpression(parentSchema, schemaName, JdbcSchema.class);
      DuckDBConvention convention = DuckDBConvention.of(dialect, expression, schemaName);
      
      // DuckDB named databases use the database name as catalog and our created schema
      DuckDBJdbcSchema schema = new DuckDBJdbcSchema(dataSource, dialect, convention, 
                                                    dbName, duckdbSchema, directory, recursive, setupConn, fileSchema);
      
      return schema;
      
    } catch (Exception e) {
      throw new RuntimeException("Failed to create DuckDB JDBC schema", e);
    }
  }
  
  /**
   * Creates a Parquet view in DuckDB dynamically.
   * This allows us to register new Parquet files on-the-fly.
   */
  public static void createParquetView(Connection connection, String viewName, String parquetPath) {
    try {
      String sql = String.format("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s')",
                              viewName.toLowerCase(), parquetPath);
      LOGGER.debug("Creating DuckDB Parquet view: {}", sql);
      connection.createStatement().execute(sql);
    } catch (SQLException e) {
      LOGGER.error("Failed to create Parquet view: {}", viewName, e);
      throw new RuntimeException("Failed to create Parquet view", e);
    }
  }
  
  /**
   * Creates a DuckDB dialect with custom lex configuration.
   * This provides scaffolding to handle any lex issues we encounter.
   */
  private static SqlDialect createDuckDBDialectWithCustomLex() {
    SqlDialect.Context context = SqlDialect.EMPTY_CONTEXT
        .withDatabaseProduct(SqlDialect.DatabaseProduct.DUCKDB)
        .withIdentifierQuoteString("\"")
        .withNullCollation(NullCollation.LAST)
        .withDataTypeSystem(DuckDBSqlDialect.TYPE_SYSTEM)
        .withUnquotedCasing(Casing.TO_LOWER)
        .withQuotedCasing(Casing.UNCHANGED)
        .withCaseSensitive(false);
    
    return new DuckDBSqlDialect(context) {
      @Override
      public StringBuilder quoteIdentifier(StringBuilder buf, String name) {
        // Don't quote identifiers that are already lowercase
        // This matches DuckDB's default behavior
        return buf.append(name.toLowerCase());
      }
      
      @Override
      public StringBuilder quoteIdentifier(StringBuilder buf, List<String> names) {
        boolean first = true;
        for (String name : names) {
          if (!first) {
            buf.append('.');
          }
          buf.append(name.toLowerCase());
          first = false;
        }
        return buf;
      }
      
      @Override
      public void unparseCall(org.apache.calcite.sql.SqlWriter writer, 
                              org.apache.calcite.sql.SqlCall call,
                              int leftPrec, int rightPrec) {
        // Use DuckDB function mapping for proper SQL generation
        if (DuckDBFunctionMapping.needsSpecialHandling(call.getOperator())) {
          DuckDBFunctionMapping.unparseCall(writer, call, leftPrec, rightPrec);
        } else {
          super.unparseCall(writer, call, leftPrec, rightPrec);
        }
      }
      
      @Override
      public boolean supportsFunction(org.apache.calcite.sql.SqlOperator operator, 
                                     org.apache.calcite.rel.type.RelDataType type,
                                     List<org.apache.calcite.rel.type.RelDataType> paramTypes) {
        // DuckDB supports most standard SQL functions
        // Plus additional functions for reading files
        String operatorName = operator.getName().toUpperCase();
        
        // DuckDB-specific table functions
        if (operatorName.equals("READ_PARQUET") || 
            operatorName.equals("READ_CSV") ||
            operatorName.equals("READ_CSV_AUTO") ||
            operatorName.equals("READ_JSON") ||
            operatorName.equals("READ_JSON_AUTO")) {
          return true;
        }
        
        // DuckDB supports all aggregation functions
        switch (operator.getKind()) {
        case COUNT:
        case SUM:
        case AVG:
        case MIN:
        case MAX:
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
        case COLLECT:
        case LISTAGG:
        case GROUP_CONCAT:
          return true;
        default:
          // Defer to parent for standard functions
          return super.supportsFunction(operator, type, paramTypes);
        }
      }
      
      @Override
      public boolean supportsAggregateFunction(org.apache.calcite.sql.SqlKind kind) {
        // DuckDB supports all standard aggregate functions
        return true;
      }
    };
  }
  
  /**
   * Gets the SQL parser configuration with Oracle Lex and unquoted to lower.
   */
  public static SqlParser.Config getParserConfig() {
    return SqlParser.config()
        .withLex(Lex.ORACLE)
        .withUnquotedCasing(Casing.TO_LOWER)
        .withQuotedCasing(Casing.UNCHANGED);
  }
  
  /**
   * Registers ONLY Parquet files as DuckDB views.
   * FileSchemaFactory has already handled all conversions by creating a FileSchema first.
   */
  private static void registerFilesAsViews(Connection conn, File directory, boolean recursive, 
                                          String duckdbSchema, String calciteSchemaName,
                                          org.apache.calcite.adapter.file.FileSchema fileSchema) 
      throws SQLException {
    // Get the schema-aware Parquet cache directory using schema name
    // This provides stable, predictable cache paths for proper caching
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(directory, null, calciteSchemaName);
    LOGGER.debug("[DuckDBJdbcSchemaFactory] Looking for Parquet files in cache directory: {} (exists: {})", cacheDir, cacheDir.exists());
    LOGGER.info("Looking for Parquet files in cache directory: {} (exists: {})", 
                cacheDir, cacheDir.exists());
    
    // Register both original Parquet files and cached Parquet files
    registerParquetFiles(conn, directory, recursive, duckdbSchema);
    
    // Also register Parquet files from the schema-aware cache directory
    if (cacheDir.exists()) {
      LOGGER.info("Registering Parquet files from schema-aware cache: {}", cacheDir);
      registerParquetFiles(conn, cacheDir, false, duckdbSchema);
    } else {
      LOGGER.warn("Schema-aware cache directory does not exist: {}", cacheDir);
    }
  }
  
  private static void registerParquetFiles(Connection conn, File directory, boolean recursive, String schema) 
      throws SQLException {
    LOGGER.debug("[DuckDBJdbcSchemaFactory] Scanning directory: {}", directory);
    File[] files = directory.listFiles();
    
    if (files != null) {
      LOGGER.debug("[DuckDBJdbcSchemaFactory] Found {} files in {}", files.length, directory);
      for (File file : files) {
        if (file.isDirectory() && recursive) {
          registerParquetFiles(conn, file, recursive, schema);
        } else if (file.isFile() && file.getName().endsWith(".parquet")) {
          String fileName = file.getName();
          
          // Skip temporary and hidden files
          if (fileName.startsWith(".") || fileName.startsWith("~")) {
            continue;
          }
          
          // Register Parquet file
          String tableName = fileName.replaceAll("\\.parquet$", "").toLowerCase();
          String sql = String.format("CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM read_parquet('%s')",
                                   schema, tableName, file.getAbsolutePath());
          LOGGER.info("Registering DuckDB view: {}.{} from file: {}", 
                      schema, tableName, file.getAbsolutePath());
          conn.createStatement().execute(sql);
          LOGGER.info("Successfully registered view: {}.{}", schema, tableName);
        }
      }
    }
  }
  
}