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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * JDBC Driver for the Calcite File Adapter.
 *
 * <p>This driver extends the standard Calcite JDBC driver to provide:
 * <ul>
 *   <li>Automatic file schema registration on connection</li>
 *   <li>Support for various file formats (CSV, JSON, Excel, Parquet, etc.)</li>
 *   <li>Configurable execution engines (PARQUET, ARROW, VECTORIZED, LINQ4J)</li>
 *   <li>Materialized view support with persistence</li>
 *   <li>Partitioned and refreshable table support</li>
 * </ul>
 *
 * <p>Connection URL format:
 * {@code jdbc:calcite:schema=file;data_path=/path/to/data;storage_path=/path/to/storage;engine=parquet}
 *
 * <p>Example usage:
 * <pre>{@code
 * String url = "jdbc:calcite:schema=file;data_path=/data/csv;engine=parquet";
 * Connection conn = DriverManager.getConnection(url);
 * }</pre>
 */
public class FileJdbcDriver extends Driver {

  private static final Logger LOGGER = Logger.getLogger(FileJdbcDriver.class.getName());

  // Track initialized schemas to avoid duplicate setup
  private static final Set<String> INITIALIZED_SCHEMAS = ConcurrentHashMap.newKeySet();

  static {
    new FileJdbcDriver().register();
  }

  @Override public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }

    LOGGER.info("Creating File Adapter Calcite connection: " + url);

    // Get underlying Calcite connection
    Connection connection = super.connect(url, info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

    // Automatically setup file schemas
    setupFileSchemas(calciteConnection, url, info);

    return connection;
  }

  /**
   * Automatically setup file schemas based on connection configuration.
   */
  private void setupFileSchemas(CalciteConnection connection,
                                String url, Properties info) {
    try {
      SchemaPlus rootSchema = connection.getRootSchema();

      // Parse configuration from URL and properties
      FileAdapterConfig config = parseConfiguration(url, info);

      // Setup each configured schema
      for (SchemaConfig schemaConfig : config.getSchemas()) {
        setupSchema(rootSchema, schemaConfig);
      }

    } catch (Exception e) {
      LOGGER.severe("Failed to setup file schemas: " + e.getMessage());
      throw new RuntimeException("Schema setup failed", e);
    }
  }

  /**
   * Setup a single file schema.
   */
  private void setupSchema(SchemaPlus rootSchema, SchemaConfig schemaConfig) {
    String schemaKey = schemaConfig.getName() + ":" + schemaConfig.getDataPath();

    // Avoid duplicate initialization
    if (INITIALIZED_SCHEMAS.contains(schemaKey)) {
      LOGGER.info("Schema already initialized: " + schemaConfig.getName());
      return;
    }

    try {
      LOGGER.info("Setting up file schema: " + schemaConfig.getName());
      LOGGER.info("  Data path: " + schemaConfig.getDataPath());
      LOGGER.info("  Storage path: " + schemaConfig.getStoragePath());
      LOGGER.info("  Engine: " + schemaConfig.getEngineType());

      // Create execution engine config
      ExecutionEngineConfig engineConfig =
          new ExecutionEngineConfig(schemaConfig.getEngineType(),
          schemaConfig.getBatchSize(),
          schemaConfig.getStoragePath());

      // Create file schema
      File dataDirectory = new File(schemaConfig.getDataPath());
      FileSchema schema =
          new FileSchema(rootSchema, schemaConfig.getName(),
          dataDirectory,
          null,
          engineConfig);

      // Register schema
      rootSchema.add(schemaConfig.getName(), schema);

      // Log successful schema creation
      LOGGER.info("  Schema '" + schemaConfig.getName() + "' created from directory: "
          + dataDirectory.getAbsolutePath());

      INITIALIZED_SCHEMAS.add(schemaKey);
      LOGGER.info("✓ File schema setup complete: " + schemaConfig.getName());

    } catch (Exception e) {
      LOGGER.severe("Failed to setup schema " + schemaConfig.getName() + ": " + e.getMessage());
      throw new RuntimeException("Schema setup failed: " + schemaConfig.getName(), e);
    }
  }

  /**
   * Parse configuration from JDBC URL and properties.
   */
  private FileAdapterConfig parseConfiguration(String url, Properties info) {
    FileAdapterConfig config = new FileAdapterConfig();

    // Parse URL parameters
    // Format: jdbc:calcite:schema=file;data_path=/data;storage_path=/storage;engine=parquet
    String[] urlParts = url.split(";");
    String schemaType = null;
    String storagePath = null;
    String engineType = "parquet";
    String dataPath = null;
    int batchSize = 2048;

    for (String part : urlParts) {
      if (part.contains("=")) {
        String[] keyValue = part.split("=", 2);
        String key = keyValue[0];
        String value = keyValue.length > 1 ? keyValue[1] : "";

        switch (key) {
        case "schema":
          schemaType = value;
          break;
        case "storage_path":
          storagePath = value;
          break;
        case "engine":
          engineType = value;
          break;
        case "data_path":
          dataPath = value;
          break;
        case "batch_size":
          try {
            batchSize = Integer.parseInt(value);
          } catch (NumberFormatException e) {
            LOGGER.warning("Invalid batch_size: " + value + ", using default: " + batchSize);
          }
          break;
        }
      }
    }

    // Also check properties for overrides
    storagePath = info.getProperty("storage_path", storagePath);
    engineType = info.getProperty("engine", engineType);
    dataPath = info.getProperty("data_path", dataPath);

    // Support legacy materialized_view properties for backward compatibility
    storagePath = info.getProperty("materialized_view.storage_path", storagePath);
    engineType = info.getProperty("materialized_view.engine", engineType);

    // Set defaults if not specified
    if (storagePath == null) {
      storagePath = System.getProperty("java.io.tmpdir") + "/calcite_file_storage";
      LOGGER.info("Using default storage path: " + storagePath);
    }

    if (dataPath == null) {
      dataPath = System.getProperty("user.dir") + "/data";
      LOGGER.info("Using default data path: " + dataPath);
    }

    // Create schema config
    if ("file".equals(schemaType)) {
      SchemaConfig schemaConfig =
          new SchemaConfig("files",  // schema name
          dataPath,
          storagePath,
          engineType,
          batchSize);
      config.addSchema(schemaConfig);
    }

    // Support multiple schemas from properties
    addConfiguredSchemas(config, info);

    return config;
  }

  /**
   * Add additional schemas from properties configuration.
   */
  private void addConfiguredSchemas(FileAdapterConfig config, Properties info) {
    // Support configuration like:
    // schema.sales.data_path=/data/sales
    // schema.sales.storage_path=/storage/sales
    // schema.sales.engine=parquet

    Set<String> schemaNames = new java.util.HashSet<>();

    // Find all schema.* properties
    for (Object key : info.keySet()) {
      String keyStr = key.toString();
      if (keyStr.startsWith("schema.") && keyStr.contains(".")) {
        String[] parts = keyStr.split("\\.", 3);
        if (parts.length >= 2) {
          schemaNames.add(parts[1]);
        }
      }
    }

    // Create schema config for each found schema
    for (String schemaName : schemaNames) {
      String dataPath = info.getProperty("schema." + schemaName + ".data_path");
      String storagePath = info.getProperty("schema." + schemaName + ".storage_path");
      String engine = info.getProperty("schema." + schemaName + ".engine", "parquet");
      String batchSizeStr = info.getProperty("schema." + schemaName + ".batch_size", "2048");

      if (dataPath != null && storagePath != null) {
        try {
          int batchSize = Integer.parseInt(batchSizeStr);
          SchemaConfig schemaConfig =
              new SchemaConfig(schemaName, dataPath, storagePath, engine, batchSize);
          config.addSchema(schemaConfig);
          LOGGER.info("Configured schema from properties: " + schemaName);
        } catch (NumberFormatException e) {
          LOGGER.warning("Invalid batch_size for schema " + schemaName + ": " + batchSizeStr);
        }
      }
    }
  }

  @Override public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith("jdbc:calcite:")
        && (url.contains("schema=file") || url.contains("materialized_view"));
  }

  @Override protected DriverVersion createDriverVersion() {
    return new DriverVersion(
        "Calcite File Adapter JDBC Driver",
        "1.0.0",
        "Apache Calcite",
        "1.41.0-SNAPSHOT",
        true,
        1, 0, 1, 41);
  }

  /**
   * Configuration holder for file adapter setup.
   */
  private static class FileAdapterConfig {
    private final java.util.List<SchemaConfig> schemas = new java.util.ArrayList<>();

    public void addSchema(SchemaConfig schema) {
      schemas.add(schema);
    }

    public java.util.List<SchemaConfig> getSchemas() {
      return schemas;
    }
  }

  /**
   * Configuration for a single file schema.
   */
  private static class SchemaConfig {
    private final String name;
    private final String dataPath;
    private final String storagePath;
    private final String engineType;
    private final int batchSize;

    SchemaConfig(String name, String dataPath, String storagePath,
                String engineType, int batchSize) {
      this.name = name;
      this.dataPath = dataPath;
      this.storagePath = storagePath;
      this.engineType = engineType;
      this.batchSize = batchSize;
    }

    public String getName() {
      return name;
    }
    public String getDataPath() {
      return dataPath;
    }
    public String getStoragePath() {
      return storagePath;
    }
    public String getEngineType() {
      return engineType;
    }
    public int getBatchSize() {
      return batchSize;
    }
  }
}
