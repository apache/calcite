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

import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;

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

      // Disable object cache if schema has refreshable tables to ensure fresh reads after refresh
      // When files are updated, DuckDB's object cache would serve stale metadata
//      boolean hasRefreshableTables = fileSchema != null && fileSchema.hasRefreshableTables();
//      if (hasRefreshableTables) {
//        setupConn.createStatement().execute("SET enable_object_cache = false");  // Disable cache for refreshable tables
//        LOGGER.info("Disabled DuckDB object cache for schema '{}' with refreshable tables", schemaName);
//      } else {
//        setupConn.createStatement().execute("SET enable_object_cache = true");  // Cache parsed files for better performance
//      }

      setupConn.createStatement().execute("SET scalar_subquery_error_on_multiple_rows = false");  // Allow Calcite's scalar subquery rewriting

      // Create a schema matching the FileSchema name
      // ALWAYS quote the schema name to preserve casing as-is
      String duckdbSchema = schemaName;
      String createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS \"" + duckdbSchema + "\"";
      LOGGER.info("Creating DuckDB schema with preserved casing: \"{}\"", duckdbSchema);
      setupConn.createStatement().execute(createSchemaSQL);
      LOGGER.info("Created DuckDB schema: \"{}\"", duckdbSchema);

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
        @Override public Connection getConnection() throws SQLException {
          // Create a new connection to the named in-memory database
          Connection conn = DriverManager.getConnection(finalJdbcUrl);
          // Apply critical settings to new connections
          try (Statement stmt = conn.createStatement()) {
            stmt.execute("SET scalar_subquery_error_on_multiple_rows = false");
          }
          return conn;
        }

        @Override public Connection getConnection(String username, String password) throws SQLException {
          return getConnection();
        }

        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public java.util.logging.Logger getParentLogger() {
          return java.util.logging.Logger.getLogger("DuckDB");
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
          if (iface.isInstance(this)) return iface.cast(this);
          throw new SQLException("Cannot unwrap to " + iface);
        }
        @Override public boolean isWrapperFor(Class<?> iface) {
          return iface.isInstance(this);
        }
      };

      SqlDialect dialect = createDuckDBDialectWithCustomLex();

      Expression expression = Schemas.subSchemaExpression(parentSchema, schemaName, JdbcSchema.class);
      DuckDBConvention convention = DuckDBConvention.of(dialect, expression, schemaName);

      // DuckDB named databases use the database name as catalog and our created schema
      DuckDBJdbcSchema schema =
                                                    new DuckDBJdbcSchema(dataSource, dialect, convention, dbName, duckdbSchema, directory, recursive, setupConn, fileSchema);

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
      // Preserve the original casing of the view name by properly quoting it
      // DuckDB preserves casing when identifiers are quoted
      String sql =
                              String.format("CREATE OR REPLACE VIEW \"%s\" AS SELECT * FROM read_parquet('%s')", viewName, parquetPath);
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
      // Don't override quoteIdentifier - let the base class handle it properly
      // The base SqlDialect already handles quoting correctly based on the context settings

      @Override public void unparseCall(org.apache.calcite.sql.SqlWriter writer,
                              org.apache.calcite.sql.SqlCall call,
                              int leftPrec, int rightPrec) {
        // Use DuckDB function mapping for proper SQL generation
        if (DuckDBFunctionMapping.needsSpecialHandling(call.getOperator())) {
          DuckDBFunctionMapping.unparseCall(writer, call, leftPrec, rightPrec);
        } else {
          super.unparseCall(writer, call, leftPrec, rightPrec);
        }
      }

      @Override public boolean supportsFunction(org.apache.calcite.sql.SqlOperator operator,
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

      @Override public boolean supportsAggregateFunction(org.apache.calcite.sql.SqlKind kind) {
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
   * Registers tables from the FileSchema's conversion registry as DuckDB views.
   * This ensures all tables discovered by FileSchema are available in DuckDB.
   */
  private static void registerFilesAsViews(Connection conn, File directory, boolean recursive,
                                          String duckdbSchema, String calciteSchemaName,
                                          org.apache.calcite.adapter.file.FileSchema fileSchema)
      throws SQLException {
    LOGGER.info("=== Starting DuckDB table registration from FileSchema registry for schema '{}' ===", calciteSchemaName);

    // Use FileSchema's metadata directly - NO FALLBACKS!
    if (fileSchema == null) {
      LOGGER.error("No FileSchema available - this is a configuration error");
      throw new SQLException("DuckDB engine requires FileSchema to be available for table discovery");
    }

    // Get all table records directly from FileSchema
    java.util.Map<String, ConversionMetadata.ConversionRecord> records = fileSchema.getAllTableRecords();
    LOGGER.info("Found {} entries in FileSchema's conversion registry", records.size());

    // Log detailed information about each conversion record for DuckDB
    LOGGER.info("=== DUCKDB REGISTRATION: CONVERSION RECORDS ===");
    for (java.util.Map.Entry<String, ConversionMetadata.ConversionRecord> entry : records.entrySet()) {
      ConversionMetadata.ConversionRecord record = entry.getValue();
      LOGGER.info("DuckDB: Table '{}' -> sourceFile='{}', convertedFile='{}', parquetCacheFile='{}', conversionType='{}'",
          record.tableName, record.sourceFile, record.convertedFile, record.getParquetCacheFile(), record.conversionType);
    }

    // Debug why records might be empty
    if (records.isEmpty()) {
      LOGGER.warn("DuckDB: FileSchema.getAllTableRecords() returned empty - checking details");
      ConversionMetadata metadata = fileSchema.getConversionMetadata();
      if (metadata == null) {
        LOGGER.warn("DuckDB: FileSchema.getConversionMetadata() returned null");
      } else {
        LOGGER.warn("DuckDB: ConversionMetadata exists but getAllConversions() returned empty");
      }

      // Also check what tables FileSchema knows about
      // Note: getTableMap() is protected, so we can't call it directly
      LOGGER.info("DuckDB: FileSchema reports it has tables but registry is empty - check conversion process");
    }

    // Process each table from the registry
    int viewCount = 0;
    for (java.util.Map.Entry<String, ConversionMetadata.ConversionRecord> entry : records.entrySet()) {
      String key = entry.getKey();
      ConversionMetadata.ConversionRecord record = entry.getValue();

      // Get the Parquet file path - either from cache or original if already Parquet
      String parquetPath = null;
      String tableName = null;

      // Check if this record has table metadata (newer records)
      if (record.getTableName() != null && !record.getTableName().isEmpty()) {
        tableName = record.getTableName();
        LOGGER.debug("Processing table '{}' from registry (original casing)", tableName);

        // Check if this is an Iceberg table
        if ("ICEBERG_PARQUET".equals(record.getConversionType())) {
          // For Iceberg tables, use DuckDB's native Iceberg support
          LOGGER.debug("Table '{}' is an Iceberg table, will use native DuckDB Iceberg support", tableName);
          // We'll handle this below with iceberg_scan
          parquetPath = null; // Will be handled specially
        } else {
          // Determine the Parquet file path for non-Iceberg tables
          if (record.getParquetCacheFile() != null) {
            parquetPath = record.getParquetCacheFile();
            LOGGER.debug("Table '{}' has cached Parquet file: {}", tableName, parquetPath);
          } else if (record.getSourceFile() != null && record.getSourceFile().endsWith(".parquet")) {
            parquetPath = record.getSourceFile();
            LOGGER.debug("Table '{}' is native Parquet: {}", tableName, parquetPath);
          } else if (record.getConvertedFile() != null) {
            // Check if it's a single parquet file or a glob pattern
            if (record.getConvertedFile().endsWith(".parquet")) {
              parquetPath = record.getConvertedFile();
              LOGGER.debug("Table '{}' has converted Parquet: {}", tableName, parquetPath);
            } else if (record.getConvertedFile().startsWith("{") && record.getConvertedFile().endsWith("}")) {
              // This is a glob pattern for multiple parquet files (e.g., from Iceberg tables)
              parquetPath = record.getConvertedFile();
              LOGGER.debug("Table '{}' has multiple Parquet files (glob pattern): {}", tableName, parquetPath);
            }
          }
        }
      } else {
        // Legacy record format - try to extract table name from file path
        LOGGER.debug("Processing legacy record with key: {}", key);

        // If the key is a simple table name (not a path), use it
        if (!key.contains("/") && !key.contains("\\")) {
          tableName = key;
        } else {
          // Extract table name from file path
          File file = new File(key);
          String fileName = file.getName();
          if (fileName.endsWith(".parquet")) {
            tableName = fileName.substring(0, fileName.length() - 8);
          } else if (fileName.endsWith(".json")) {
            tableName = fileName.substring(0, fileName.length() - 5);
          } else {
            tableName = fileName;
          }
        }

        // For legacy records, check parquet cache file first
        if (record.getParquetCacheFile() != null) {
          parquetPath = record.getParquetCacheFile();
        } else if (key.endsWith(".parquet")) {
          parquetPath = key;
        } else if (key.endsWith(".json")) {
          // For JSON files, try to find corresponding Parquet cache file
          // Schema-aware cache uses pattern: baseDirectory/.parquet_cache/tableName.parquet
          File baseDir = fileSchema.getBaseDirectory();
          File parquetCacheDir = new File(baseDir, ".parquet_cache");
          File parquetFile = new File(parquetCacheDir, tableName + ".parquet");
          if (parquetFile.exists()) {
            parquetPath = parquetFile.getAbsolutePath();
            LOGGER.debug("Found Parquet cache for legacy JSON table '{}': {}", tableName, parquetFile.getName());
          } else {
            // Try original ParquetConversionUtil location pattern
            File conversionDir = ParquetConversionUtil.getParquetCacheDir(new File(key).getParentFile(), null, calciteSchemaName);
            File altParquetFile = new File(conversionDir, tableName + ".parquet");
            if (altParquetFile.exists()) {
              parquetPath = altParquetFile.getAbsolutePath();
              LOGGER.debug("Found Parquet cache for legacy JSON table '{}' in alt location: {}", tableName, altParquetFile.getName());
            }
          }
        }
      }

      // Create view if we have a table name and either a Parquet path or it's an Iceberg table
      if (tableName != null) {
        // Check if this is an Iceberg table that needs special handling
        boolean isIcebergTable = "ICEBERG_PARQUET".equals(record.getConversionType());

        if (isIcebergTable && record.getSourceFile() != null) {
          // Use DuckDB's native Iceberg support
          // First, ensure the iceberg extension is installed and loaded
          try {
            // Install and load iceberg extension if not already done
            try {
              conn.createStatement().execute("INSTALL iceberg");
            } catch (SQLException e) {
              // Extension might already be installed
              LOGGER.debug("Iceberg extension may already be installed: {}", e.getMessage());
            }

            try {
              conn.createStatement().execute("LOAD iceberg");
            } catch (SQLException e) {
              // Extension might already be loaded
              LOGGER.debug("Iceberg extension may already be loaded: {}", e.getMessage());
            }

            // For Iceberg tables, try iceberg_scan
            // If it fails (e.g., empty table), create an empty view as a fallback
            String sql =
                                     String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM iceberg_scan('%s')", duckdbSchema, tableName, record.getSourceFile());
            LOGGER.info("Creating DuckDB view for Iceberg table: \"{}.{}\" -> {}",
                       duckdbSchema, tableName, record.getSourceFile());

            try {
              conn.createStatement().execute(sql);
              viewCount++;
              LOGGER.debug("Successfully created Iceberg view: {}.{}", duckdbSchema, tableName);
            } catch (SQLException scanError) {
              // iceberg_scan failed - probably an empty table
              LOGGER.debug("iceberg_scan failed for table '{}': {}", tableName, scanError.getMessage());

              // Create an empty view as a fallback
              // This ensures the table is available even if it's empty
              // We need to include common Iceberg columns
              try {
                // Create empty view with common Iceberg table columns
                // This is a workaround for empty Iceberg tables where iceberg_scan fails
                // TODO: Get actual schema from FileSchema's table instance
                String emptyViewSql =
                    String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS " +
                    "SELECT " +
                    "NULL::INT AS order_id, " +
                    "NULL::VARCHAR AS customer_id, " +
                    "NULL::VARCHAR AS product_id, " +
                    "NULL::DOUBLE AS amount, " +
                    "NULL::TIMESTAMP AS snapshot_time " +
                    "WHERE 1=0",
                    duckdbSchema, tableName);

                LOGGER.info("Creating empty view for Iceberg table '{}' (fallback)", tableName);
                conn.createStatement().execute(emptyViewSql);
                viewCount++;
                LOGGER.debug("Successfully created empty view for table: {}.{}", duckdbSchema, tableName);
              } catch (SQLException fallbackError) {
                LOGGER.warn("Failed to create fallback empty view for table '{}': {}",
                           tableName, fallbackError.getMessage());
                throw fallbackError; // Re-throw to maintain original behavior
              }
            }
          } catch (SQLException e) {
            LOGGER.warn("Failed to create Iceberg view for table '{}': {}", tableName, e.getMessage());
          }
        } else if (parquetPath != null) {
          // Check if it's a glob pattern or single file
          boolean isMultiFileList = (parquetPath.startsWith("[") && parquetPath.endsWith("]")) ||
                                   (parquetPath.startsWith("{") && parquetPath.endsWith("}"));
          boolean isGlobPattern = parquetPath.contains("**") || (parquetPath.contains("*") && !isMultiFileList);
          String sql = null;

          if (isMultiFileList) {
            // For multiple files specified as [file1,file2,file3] or {file1,file2,file3}
            // Remove the brackets and split by comma
            String fileList = parquetPath.substring(1, parquetPath.length() - 1);
            String[] files = fileList.split(",");

            // Build a list of file paths for DuckDB's read_parquet function
            // DuckDB can read multiple files using: read_parquet(['file1', 'file2', ...])
            StringBuilder fileArray = new StringBuilder("[");
            boolean first = true;
            for (String file : files) {
              if (!first) fileArray.append(", ");
              fileArray.append("'").append(file.trim()).append("'");
              first = false;
            }
            fileArray.append("]");

            sql =
                              String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM parquet_scan(%s)", duckdbSchema, tableName, fileArray.toString());
            LOGGER.info("Creating DuckDB view for multiple files: \"{}.{}\" -> {} files", duckdbSchema, tableName, files.length);
          } else if (isGlobPattern) {
            // Glob pattern - DuckDB's parquet_scan supports glob patterns directly
            // For glob patterns with **, always enable hive_partitioning to let DuckDB auto-detect
            // DuckDB will automatically detect if the data is actually Hive-partitioned
            if (parquetPath.contains("**")) {
              // Enable hive_partitioning for recursive glob patterns - DuckDB will auto-detect if needed
              sql =
                                 String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM parquet_scan('%s', hive_partitioning = true)", duckdbSchema, tableName, parquetPath);
              LOGGER.info("Creating DuckDB view with glob pattern and Hive partitioning auto-detection: \"{}.{}\" -> {}", duckdbSchema, tableName, parquetPath);
            } else {
              sql =
                                 String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM parquet_scan('%s')", duckdbSchema, tableName, parquetPath);
              LOGGER.info("Creating DuckDB view with glob pattern: \"{}.{}\" -> {}", duckdbSchema, tableName, parquetPath);
            }
          } else {
            // Single file
            File parquetFile = new File(parquetPath);
            if (parquetFile.exists()) {
              sql =
                                String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM parquet_scan('%s')", duckdbSchema, tableName, parquetFile.getAbsolutePath());
              LOGGER.info("Creating DuckDB view: \"{}.{}\" -> {}", duckdbSchema, tableName, parquetFile.getName());
            } else {
              LOGGER.warn("Parquet file does not exist for table '{}': {}", tableName, parquetPath);
            }
          }

          if (sql != null) {
            try {
              conn.createStatement().execute(sql);
              viewCount++;
              LOGGER.debug("Successfully created view: {}.{}", duckdbSchema, tableName);

              // Add diagnostic logging to see what DuckDB interprets from the Parquet file
              try (Statement debugStmt = conn.createStatement();
                   ResultSet schemaInfo =
                     debugStmt.executeQuery(String.format("DESCRIBE \"%s\".\"%s\"", duckdbSchema, tableName))) {
                LOGGER.debug("=== DuckDB Schema for {}.{} ===", duckdbSchema, tableName);
                while (schemaInfo.next()) {
                  String colName = schemaInfo.getString("column_name");
                  String colType = schemaInfo.getString("column_type");
                  String nullable = schemaInfo.getString("null");
                  LOGGER.debug("  Column: {} | Type: {} | Nullable: {}", colName, colType, nullable);
                }
              } catch (SQLException debugE) {
                LOGGER.warn("Failed to get schema info for table '{}': {}", tableName, debugE.getMessage());
              }
            } catch (SQLException e) {
              LOGGER.warn("Failed to create view for table '{}': {}", tableName, e.getMessage());
            }
          }
        } else {
          LOGGER.debug("Skipping registry entry - no table name or suitable path. Table: {}, Path: {}",
                      tableName, parquetPath);
        }
      }
    }

    LOGGER.info("=== Created {} DuckDB views from registry ===", viewCount);

    if (viewCount == 0) {
      LOGGER.warn("No DuckDB views created from registry - this may indicate missing Parquet cache files");
      LOGGER.warn("Tables found in registry: {}", records.keySet());
    }
  }

  /**
   * Legacy method: Scans directories for Parquet files.
   * Used as fallback when registry is not available or empty.
   */
  private static void registerParquetFilesFromDirectory(Connection conn, File directory,
                                                       boolean recursive, String duckdbSchema)
      throws SQLException {
    LOGGER.info("Using directory scanning for Parquet files in: {}", directory);

    // Get the schema-aware Parquet cache directory
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(directory, null, duckdbSchema);

    // Register both original Parquet files and cached Parquet files
    registerParquetFiles(conn, directory, recursive, duckdbSchema);

    // Also register Parquet files from the cache directory
    if (cacheDir.exists()) {
      LOGGER.info("Registering Parquet files from cache: {}", cacheDir);
      registerParquetFiles(conn, cacheDir, false, duckdbSchema);
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

          // Register Parquet file - preserve original casing since we use quoted identifiers
          String tableName = fileName.replaceAll("\\.parquet$", "");
          String sql =
                                   String.format("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM read_parquet('%s')", schema, tableName, file.getAbsolutePath());
          LOGGER.info("Registering DuckDB view: {}.{} from file: {}",
                      schema, tableName, file.getAbsolutePath());
          conn.createStatement().execute(sql);
          LOGGER.info("Successfully registered view: {}.{}", schema, tableName);
        }
      }
    }
  }

}
