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

import org.apache.calcite.adapter.file.duckdb.DuckDBJdbcSchemaFactory;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.execution.duckdb.DuckDBConfig;
import org.apache.calcite.adapter.file.metadata.InformationSchema;
import org.apache.calcite.adapter.file.metadata.PostgresMetadataSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Factory that creates a {@link FileSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 * See <a href="http://calcite.apache.org/docs/file_adapter.html">File adapter</a>.
 */
@SuppressWarnings("UnusedDeclaration")
public class FileSchemaFactory implements SchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSchemaFactory.class);

  static {
    LOGGER.debug("[FileSchemaFactory] Class loaded and static initializer running");
  }

  /** Public singleton, per factory contract. */
  public static final FileSchemaFactory INSTANCE = new FileSchemaFactory();

  /** Name of the column that is implicitly created in a CSV stream table
   * to hold the data arrival time. */
  public static final String ROWTIME_COLUMN_NAME = "ROWTIME";

  private FileSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    LOGGER.info("[FileSchemaFactory] ==> create() called for schema: '{}'", name);
    LOGGER.info("[FileSchemaFactory] ==> Parent schema: '{}'", parentSchema != null ? parentSchema.getName() : "null");
    LOGGER.info("[FileSchemaFactory] ==> Operand keys: {}", operand.keySet());
    LOGGER.info("[FileSchemaFactory] ==> Thread: {}", Thread.currentThread().getName());
    @SuppressWarnings("unchecked") List<Map<String, Object>> tables =
        (List) operand.get("tables");
    final File baseDirectory =
        (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    final String directory = (String) operand.get("directory");
    LOGGER.debug("[FileSchemaFactory] directory from operand: '{}'", directory);
    LOGGER.debug("[FileSchemaFactory] baseDirectory: '{}'", baseDirectory);

    // Execution engine configuration
    final String executionEngine =
        (String) operand.getOrDefault("executionEngine",
            ExecutionEngineConfig.DEFAULT_EXECUTION_ENGINE);
    LOGGER.info("[FileSchemaFactory] ==> executionEngine from operand: '{}' for schema: '{}'", executionEngine, name);
    LOGGER.info("[FileSchemaFactory] ==> Raw executionEngine value: '{}'", operand.get("executionEngine"));
    LOGGER.info("[FileSchemaFactory] ==> Default engine: '{}'", ExecutionEngineConfig.DEFAULT_EXECUTION_ENGINE);
    final Object batchSizeObj = operand.get("batchSize");
    final int batchSize = batchSizeObj instanceof Number
        ? ((Number) batchSizeObj).intValue()
        : ExecutionEngineConfig.DEFAULT_BATCH_SIZE;
    final Object memoryThresholdObj = operand.get("memoryThreshold");
    final long memoryThreshold = memoryThresholdObj instanceof Number
        ? ((Number) memoryThresholdObj).longValue()
        : ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD;

    // Get DuckDB configuration if provided
    @SuppressWarnings("unchecked") Map<String, Object> duckdbConfigMap =
        (Map<String, Object>) operand.get("duckdbConfig");
    final DuckDBConfig duckdbConfig = duckdbConfigMap != null
        ? new DuckDBConfig(duckdbConfigMap)
        : null;

    // Get custom Parquet cache directory if provided
    final String parquetCacheDirectory = (String) operand.get("parquetCacheDirectory");

    final ExecutionEngineConfig engineConfig =
        new ExecutionEngineConfig(executionEngine, batchSize, memoryThreshold, null, duckdbConfig, parquetCacheDirectory);

    // Get recursive parameter (default to false for backward compatibility)
    final boolean recursive = operand.get("recursive") == Boolean.TRUE;

    // Get directory pattern for glob-based file discovery
    final String directoryPattern = (String) operand.get("directoryPattern");

    // Get materialized views configuration
    @SuppressWarnings("unchecked") List<Map<String, Object>> materializations =
        (List<Map<String, Object>>) operand.get("materializations");

    // Get views configuration
    @SuppressWarnings("unchecked") List<Map<String, Object>> views =
        (List<Map<String, Object>>) operand.get("views");

    // Get partitioned tables configuration
    @SuppressWarnings("unchecked") List<Map<String, Object>> partitionedTables =
        (List<Map<String, Object>>) operand.get("partitionedTables");

    // Get storage provider configuration
    final String storageType = (String) operand.get("storageType");
    @SuppressWarnings("unchecked") Map<String, Object> storageConfig =
        (Map<String, Object>) operand.get("storageConfig");

    // Get refresh interval for schema (default for all tables)
    final String refreshInterval = (String) operand.get("refreshInterval");
    LOGGER.info("FileSchemaFactory: refreshInterval from operand: '{}'", refreshInterval);

    // Get flatten option for JSON/YAML files
    final Boolean flatten = (Boolean) operand.get("flatten");

    // Get table name casing configuration (default to SMART_CASING)
    // Support both camelCase (model.json) and snake_case (JDBC URL) naming conventions
    String tableNameCasing = (String) operand.get("tableNameCasing");
    if (tableNameCasing == null) {
      tableNameCasing = (String) operand.getOrDefault("table_name_casing", "SMART_CASING");
    }

    // Get column name casing configuration (default to SMART_CASING)
    // Support both camelCase (model.json) and snake_case (JDBC URL) naming conventions
    String columnNameCasing = (String) operand.get("columnNameCasing");
    if (columnNameCasing == null) {
      columnNameCasing = (String) operand.getOrDefault("column_name_casing", "SMART_CASING");
    }

    // Get CSV type inference configuration
    @SuppressWarnings("unchecked") Map<String, Object> csvTypeInference =
        (Map<String, Object>) operand.get("csvTypeInference");

    // Get prime_cache option (default to true for optimal performance)
    final Boolean primeCache = operand.get("primeCache") != null
        ? (Boolean) operand.get("primeCache")
        : operand.get("prime_cache") != null
            ? (Boolean) operand.get("prime_cache")
            : Boolean.TRUE;  // Default to true

    File directoryFile = null;
    // Only create File objects for local storage, not for cloud storage providers
    if (storageType == null || "local".equals(storageType)) {
      if (directory != null) {
        directoryFile = new File(directory);
      }
      if (baseDirectory != null) {
        if (directoryFile == null) {
          directoryFile = baseDirectory;
        } else if (!directoryFile.isAbsolute()) {
          directoryFile = new File(baseDirectory, directory);
        }
      }
    } else if (directory != null && storageType != null) {
      // For cloud storage, use the directory as-is (it's a URI like s3://bucket/path)
      // Create a fake File object that just holds the path
      directoryFile = new File(directory) {
        @Override public String getPath() {
          return directory;
        }
        @Override public String getAbsolutePath() {
          return directory;
        }
      };
    }

    // If DuckDB engine is selected, first create FileSchema with PARQUET engine for conversions
    LOGGER.debug("FileSchemaFactory: Checking DuckDB conditions for schema '{}': engineConfig.getEngineType()={}, directoryFile={}, exists={}, isDirectory={}, storageType={}", 
                name, engineConfig.getEngineType(), directoryFile, 
                directoryFile != null ? directoryFile.exists() : false,
                directoryFile != null ? directoryFile.isDirectory() : false,
                storageType);
    
    // Check if we're using DuckDB engine
    boolean isDuckDB = engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.DUCKDB;
    LOGGER.info("[FileSchemaFactory] ==> DuckDB analysis for schema '{}': ", name);
    LOGGER.info("[FileSchemaFactory] ==> - engineConfig.getEngineType(): {}", engineConfig.getEngineType());
    LOGGER.info("[FileSchemaFactory] ==> - ExecutionEngineType.DUCKDB: {}", ExecutionEngineConfig.ExecutionEngineType.DUCKDB);
    LOGGER.info("[FileSchemaFactory] ==> - isDuckDB: {}", isDuckDB);
    LOGGER.info("[FileSchemaFactory] ==> - directoryFile != null: {}", directoryFile != null);
    LOGGER.info("[FileSchemaFactory] ==> - storageType: '{}'", storageType);
    LOGGER.info("[FileSchemaFactory] ==> - Full condition: {}", isDuckDB && directoryFile != null && storageType == null);
    
    if (isDuckDB && directoryFile != null && storageType == null) {
      LOGGER.info("[FileSchemaFactory] ==> *** ENTERING DUCKDB PATH FOR SCHEMA: {} ***", name);
      // Create directory if it doesn't exist yet (common in tests)
      if (!directoryFile.exists()) {
        LOGGER.info("Creating directory as it doesn't exist: {}", directoryFile);
        directoryFile.mkdirs();
      }
      
      LOGGER.info("Using DuckDB: Running conversions first, then creating JDBC adapter for schema: {}", name);
      
      // Step 1: Create FileSchema with PARQUET engine to handle all conversions
      // DuckDB always uses Parquet for consistent performance and functionality
      ExecutionEngineConfig conversionConfig = new ExecutionEngineConfig("PARQUET", 
          engineConfig.getBatchSize(), engineConfig.getMemoryThreshold(), 
          engineConfig.getMaterializedViewStoragePath(), engineConfig.getDuckDBConfig(),
          engineConfig.getParquetCacheDirectory());
      
      FileSchema fileSchema = new FileSchema(parentSchema, name, directoryFile, 
          directoryPattern, tables, conversionConfig, recursive, materializations, views, 
          partitionedTables, refreshInterval, tableNameCasing, columnNameCasing, 
          storageType, storageConfig, flatten, csvTypeInference, primeCache);
      
      // Force initialization to run conversions
      LOGGER.debug("FileSchemaFactory: About to call fileSchema.getTableMap() for table discovery");
      Map<String, Table> tableMap = fileSchema.getTableMap();
      LOGGER.info("FileSchemaFactory: FileSchema discovered {} tables: {}", tableMap.size(), tableMap.keySet());
      
      // Parquet conversion should happen automatically when tables are accessed
      LOGGER.debug("FileSchemaFactory: Parquet conversion will happen on-demand via FileSchema");
      
      // Step 2: Now create DuckDB JDBC schema that reads the files
      // Pass the FileSchema so it stays alive for refresh handling
      LOGGER.debug("FileSchemaFactory: Now creating DuckDB JDBC schema");
      JdbcSchema duckdbSchema = DuckDBJdbcSchemaFactory.create(parentSchema, name, directoryFile, recursive, fileSchema);
      LOGGER.info("FileSchemaFactory: DuckDB JDBC schema created successfully");

      // Add metadata schemas as sibling schemas
      addMetadataSchemas(parentSchema);

      return duckdbSchema;
    }

    // Otherwise use regular FileSchema
    LOGGER.info("[FileSchemaFactory] ==> *** USING REGULAR FILESCHEMA FOR SCHEMA: {} ***", name);
    LOGGER.info("[FileSchemaFactory] ==> - Reason: isDuckDB={}, directoryFile != null={}, storageType='{}'", 
               isDuckDB, directoryFile != null, storageType);
    FileSchema fileSchema =
        new FileSchema(parentSchema, name, directoryFile, directoryPattern, tables, engineConfig, recursive,
        materializations, views, partitionedTables, refreshInterval, tableNameCasing,
        columnNameCasing, storageType, storageConfig, flatten, csvTypeInference, primeCache);

    // Add metadata schemas as sibling schemas (not sub-schemas)
    // This makes them available at the same level as the file schema
    // Get the root schema to access all schemas for metadata
    SchemaPlus rootSchema = parentSchema;
    while (rootSchema.getParentSchema() != null) {
      rootSchema = rootSchema.getParentSchema();
    }

    // Only add metadata schemas if they don't already exist
    if (rootSchema.subSchemas().get("information_schema") == null) {
      InformationSchema infoSchema = new InformationSchema(rootSchema, "CALCITE");
      parentSchema.add("information_schema", infoSchema);
    }

    if (rootSchema.subSchemas().get("pg_catalog") == null) {
      PostgresMetadataSchema pgSchema = new PostgresMetadataSchema(rootSchema, "CALCITE");
      parentSchema.add("pg_catalog", pgSchema);
    }

    // Ensure the standard Calcite metadata schema is preserved
    // It should already exist at the root level from CalciteConnectionImpl
    if (rootSchema.subSchemas().get("metadata") != null && parentSchema.subSchemas().get("metadata") == null) {
      // The metadata schema exists at root but not at current level, so reference it
      SchemaPlus metadataSchema = rootSchema.subSchemas().get("metadata");
      parentSchema.add("metadata", metadataSchema.unwrap(Schema.class));
    }

    return fileSchema;
  }

  private static void addMetadataSchemas(SchemaPlus parentSchema) {
    // Get the root schema to access all schemas for metadata
    SchemaPlus rootSchema = parentSchema;
    while (rootSchema.getParentSchema() != null) {
      rootSchema = rootSchema.getParentSchema();
    }

    // Only add metadata schemas if they don't already exist
    if (rootSchema.subSchemas().get("information_schema") == null) {
      InformationSchema infoSchema = new InformationSchema(rootSchema, "CALCITE");
      parentSchema.add("information_schema", infoSchema);
    }

    if (rootSchema.subSchemas().get("pg_catalog") == null) {
      PostgresMetadataSchema pgSchema = new PostgresMetadataSchema(rootSchema, "CALCITE");
      parentSchema.add("pg_catalog", pgSchema);
    }

    // Ensure the standard Calcite metadata schema is preserved
    if (rootSchema.subSchemas().get("metadata") != null && parentSchema.subSchemas().get("metadata") == null) {
      SchemaPlus metadataSchema = rootSchema.subSchemas().get("metadata");
      parentSchema.add("metadata", metadataSchema.unwrap(Schema.class));
    }
  }
}
