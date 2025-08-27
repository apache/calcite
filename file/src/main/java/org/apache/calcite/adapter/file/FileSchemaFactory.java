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
import org.apache.calcite.schema.lookup.LikePattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
    // Validate that schema name is unique within parent schema
    validateUniqueSchemaName(parentSchema, name);
    LOGGER.info("[FileSchemaFactory] ==> create() called for schema: '{}'", name);
    LOGGER.info("[FileSchemaFactory] ==> Parent schema: '{}'", parentSchema != null ? parentSchema.getName() : "null");
    LOGGER.info("[FileSchemaFactory] ==> Operand keys: {}", operand.keySet());
    LOGGER.info("[FileSchemaFactory] ==> Thread: {}", Thread.currentThread().getName());
    @SuppressWarnings("unchecked") List<Map<String, Object>> tables =
        (List) operand.get("tables");
    
    // Model file location (automatically set by Calcite's ModelHandler)
    // This can be null for inline models (model provided as a string)
    Object baseDirectoryObj = operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    File modelFileDirectory = null;
    if (baseDirectoryObj instanceof File) {
      modelFileDirectory = (File) baseDirectoryObj;
    } else if (baseDirectoryObj instanceof String) {
      modelFileDirectory = new File((String) baseDirectoryObj);
    }
    
    // Get ephemeralCache option (default to false for backward compatibility)
    final Boolean ephemeralCache = parseBooleanValue(operand.get("ephemeralCache"))
        != null ? parseBooleanValue(operand.get("ephemeralCache"))
        : parseBooleanValue(operand.get("ephemeral_cache")) != null  // Support snake_case too
            ? parseBooleanValue(operand.get("ephemeral_cache"))
            : Boolean.FALSE;  // Default to persistent cache
    
    // Handle ephemeralCache and baseDirectory configuration
    // baseConfigDirectory is the root directory (without .aperio/<schema>)
    // baseDirectory is the full path including .aperio/<schema>
    File baseConfigDirectory = null;
    File baseDirectory = null;
    
    if (ephemeralCache) {
      // Create a temp directory for this test instance
      try {
        String tempDir = System.getProperty("java.io.tmpdir");
        String uniqueId = UUID.randomUUID().toString();
        baseConfigDirectory = new File(tempDir, uniqueId);
        baseConfigDirectory.mkdirs();
        LOGGER.info("Using ephemeral cache directory for schema '{}': {}", 
            name, baseConfigDirectory.getAbsolutePath());
        // baseDirectory will be computed from baseConfigDirectory later
      } catch (Exception e) {
        LOGGER.error("Failed to create ephemeral cache directory", e);
      }
    } else {
      // User-configurable baseDirectory for cache/conversions (optional)
      // Handle both String and File types for baseDirectory
      final Object baseDirObj = operand.get("baseDirectory");
      final String baseDirConfig;
      if (baseDirObj instanceof String) {
        baseDirConfig = (String) baseDirObj;
      } else if (baseDirObj instanceof File) {
        baseDirConfig = ((File) baseDirObj).getPath();
      } else {
        baseDirConfig = null;
      }
      
      if (baseDirConfig != null && !baseDirConfig.isEmpty()) {
        // User explicitly configured baseDirectory - respect their choice
        baseConfigDirectory = new File(baseDirConfig);
        if (!baseConfigDirectory.isAbsolute() && modelFileDirectory != null) {
          // If relative path, resolve against model.json location
          baseConfigDirectory = new File(modelFileDirectory, baseDirConfig);
        }
        baseConfigDirectory = baseConfigDirectory.getAbsoluteFile();
        LOGGER.info("Using user-configured baseConfigDirectory: {}", baseConfigDirectory.getAbsolutePath());
      }
      // If no explicit config and not ephemeral, baseConfigDirectory remains null
      // and FileSchema will use its default (working directory)
    }
    
    // Compute the full baseDirectory path (with .aperio/<schema>) if baseConfigDirectory is set
    if (baseConfigDirectory != null) {
      baseDirectory = baseConfigDirectory;  // For now, keep it as the root for backward compatibility
      LOGGER.debug("baseConfigDirectory={}, baseDirectory={}", 
          baseConfigDirectory.getAbsolutePath(), baseDirectory.getAbsolutePath());
    }
    
    // Schema-specific sourceDirectory operand (for reading source files)
    // Support both "directory" and "sourceDirectory" for backward compatibility
    final String directory = (String) operand.get("directory") != null 
        ? (String) operand.get("directory") 
        : (String) operand.get("sourceDirectory");
    File sourceDirectory = null;
    LOGGER.debug("[FileSchemaFactory] directory from operand: '{}' (checked both 'directory' and 'sourceDirectory')", directory);
    LOGGER.debug("[FileSchemaFactory] modelFileDirectory: '{}'", modelFileDirectory);
    LOGGER.debug("[FileSchemaFactory] ephemeralCache: {}, baseDirectory: {}", ephemeralCache, baseDirectory);

    // Execution engine configuration
    // Priority: 1. Schema-specific operand, 2. Environment variable, 3. System property, 4. Default
    String executionEngine = (String) operand.get("executionEngine");
    String source = "schema operand";
    
    if (executionEngine == null || executionEngine.isEmpty()) {
      executionEngine = System.getenv("CALCITE_FILE_ENGINE_TYPE");
      source = "environment variable";
    }
    if (executionEngine == null || executionEngine.isEmpty()) {
      executionEngine = System.getProperty("calcite.file.engine.type");
      source = "system property";
    }
    if (executionEngine == null || executionEngine.isEmpty()) {
      executionEngine = ExecutionEngineConfig.DEFAULT_EXECUTION_ENGINE;
      source = "default";
    }
    
    LOGGER.info("[FileSchemaFactory] ==> executionEngine: '{}' for schema: '{}' (source: {})", 
        executionEngine, name, source);
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
    // Support both "directoryPattern" and "glob" for backward compatibility
    final String directoryPattern = (String) operand.get("directoryPattern") != null 
        ? (String) operand.get("directoryPattern")
        : (String) operand.get("glob");

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
    // Determine sourceDirectory for reading files
    // Only create File objects for local storage, not for cloud storage providers
    if (storageType == null || "local".equals(storageType)) {
      if (directory != null) {
        sourceDirectory = new File(directory);
        // If sourceDirectory is relative and we have a modelFileDirectory context, resolve it
        if (!sourceDirectory.isAbsolute() && modelFileDirectory != null) {
          // For relative paths, resolve against modelFileDirectory (which is the model file's parent directory)
          // This ensures we find test resources at the correct location
          sourceDirectory = new File(modelFileDirectory, directory);
        }
      } else if (modelFileDirectory != null) {
        // If no directory specified but modelFileDirectory exists, use modelFileDirectory itself
        sourceDirectory = modelFileDirectory;
      } else {
        // Default to current working directory
        sourceDirectory = new File(System.getProperty("user.dir"));
      }
      directoryFile = sourceDirectory;
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
      
      // Step 1: Create FileSchema with PARQUET engine
      // Use the same baseDirectory that was computed for DuckDB (ephemeral or not)
      ExecutionEngineConfig conversionConfig = new ExecutionEngineConfig("PARQUET", 
          engineConfig.getBatchSize(), engineConfig.getMemoryThreshold(), 
          engineConfig.getMaterializedViewStoragePath(), engineConfig.getDuckDBConfig(),
          engineConfig.getParquetCacheDirectory());
      
      LOGGER.info("DuckDB: Creating internal Parquet FileSchema with baseConfigDirectory: {}", baseConfigDirectory);
      
      // Create internal FileSchema for DuckDB processing
      FileSchema fileSchema = new FileSchema(parentSchema, name, directoryFile, baseConfigDirectory,
          directoryPattern, tables, conversionConfig, recursive, materializations, views, 
          partitionedTables, refreshInterval, tableNameCasing, columnNameCasing, 
          storageType, storageConfig, flatten, csvTypeInference, primeCache);
      
      // Force initialization to run conversions and populate the FileSchema for DuckDB
      LOGGER.info("DuckDB: About to call fileSchema.getTableMap() for table discovery");
      LOGGER.info("DuckDB: Internal FileSchema created successfully: {}", fileSchema.getClass().getSimpleName());
      LOGGER.info("DuckDB: Internal FileSchema directory: {}", directoryFile);
      
      Map<String, Table> tableMap = fileSchema.getTableMap();
      LOGGER.info("DuckDB: Internal FileSchema discovered {} tables: {}", tableMap.size(), tableMap.keySet());
      
      if (tableMap.containsKey("sales_custom")) {
        LOGGER.info("DuckDB: Found sales_custom table in internal FileSchema!");
      } else {
        LOGGER.warn("DuckDB: sales_custom table NOT found in internal FileSchema");
      }
      
      // Check the conversion metadata immediately after table discovery
      if (fileSchema.getConversionMetadata() != null) {
        java.util.Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord> records = 
            fileSchema.getAllTableRecords();
        LOGGER.info("FileSchemaFactory: After getTableMap(), conversion metadata has {} records", records.size());
        for (String key : records.keySet()) {
          LOGGER.debug("FileSchemaFactory: Conversion record key: {}", key);
        }
      } else {
        LOGGER.warn("FileSchemaFactory: FileSchema has no conversion metadata!");
      }
      
      // Check if any JSON files were processed by the internal FileSchema
      LOGGER.debug("FileSchemaFactory: Checking if internal FileSchema processed JSON files from HTML conversion...");
      
      // Parquet conversion should happen automatically when tables are accessed
      LOGGER.debug("FileSchemaFactory: Parquet conversion will happen on-demand via FileSchema");
      
      // Step 2: Now create DuckDB JDBC schema that reads the files
      // Pass the FileSchema so it stays alive for refresh handling
      LOGGER.debug("FileSchemaFactory: Now creating DuckDB JDBC schema");
      JdbcSchema duckdbSchema = DuckDBJdbcSchemaFactory.create(parentSchema, name, directoryFile, recursive, fileSchema);
      LOGGER.info("FileSchemaFactory: DuckDB JDBC schema created successfully");

      // Register the DuckDB JDBC schema with the parent so SQL queries can find the tables
      // This is critical for Calcite's SQL validator to see the tables
      // Note: Schema uniqueness already validated at method start
      parentSchema.add(name, duckdbSchema);
      LOGGER.info("FileSchemaFactory: Registered DuckDB JDBC schema '{}' with parent schema for SQL validation", name);

      // Add metadata schemas as sibling schemas
      addMetadataSchemas(parentSchema);

      return duckdbSchema;
    }

    // Otherwise use regular FileSchema
    LOGGER.info("[FileSchemaFactory] ==> *** USING REGULAR FILESCHEMA FOR SCHEMA: {} ***", name);
    LOGGER.info("[FileSchemaFactory] ==> - Reason: isDuckDB={}, directoryFile != null={}, storageType='{}'", 
               isDuckDB, directoryFile != null, storageType);
    // Pass user-configured baseDirectory or null to let FileSchema use its default
    // FileSchema will default to {working_directory}/.aperio/<schema_name> if null
    FileSchema fileSchema =
        new FileSchema(parentSchema, name, directoryFile, baseDirectory, directoryPattern, tables, engineConfig, recursive,
        materializations, views, partitionedTables, refreshInterval, tableNameCasing,
        columnNameCasing, storageType, storageConfig, flatten, csvTypeInference, primeCache);

    // Force table discovery to populate the schema before creating metadata schemas
    LOGGER.debug("FileSchemaFactory: About to call fileSchema.getTableMap() for table discovery");
    Map<String, Table> tableMap = fileSchema.getTableMap();
    LOGGER.info("FileSchemaFactory: FileSchema discovered {} tables: {}", tableMap.size(), tableMap.keySet());

    // Register the FileSchema with the parent so metadata queries can find the tables
    // This is critical for DatabaseMetaData.getColumns() to work
    // Note: Schema uniqueness already validated at method start
    parentSchema.add(name, fileSchema);
    LOGGER.info("FileSchemaFactory: Registered FileSchema '{}' with parent schema for metadata visibility", name);

    // Add metadata schemas as sibling schemas (not sub-schemas)
    // This makes them available at the same level as the file schema
    // Get the root schema to access all schemas for metadata
    SchemaPlus rootSchema = parentSchema;
    while (rootSchema.getParentSchema() != null) {
      rootSchema = rootSchema.getParentSchema();
    }

    // Only add metadata schemas if they don't already exist
    if (parentSchema.subSchemas().get("information_schema") == null) {
      LOGGER.info("FileSchemaFactory: Creating InformationSchema with parentSchema containing tables: {}", 
                  parentSchema.tables().getNames(LikePattern.any()));
      InformationSchema infoSchema = new InformationSchema(parentSchema, "CALCITE");
      parentSchema.add("information_schema", infoSchema);
      LOGGER.info("FileSchemaFactory: Added InformationSchema to parent schema");
    } else {
      LOGGER.info("FileSchemaFactory: InformationSchema already exists, not creating new one");
    }

    if (parentSchema.subSchemas().get("pg_catalog") == null) {
      PostgresMetadataSchema pgSchema = new PostgresMetadataSchema(parentSchema, "CALCITE");
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

  /**
   * Validates that a schema with the given name does not already exist in the parent schema.
   * This prevents configuration errors and silent schema replacement.
   *
   * @param parentSchema the parent schema to check
   * @param name the schema name to validate
   * @throws IllegalArgumentException if a schema with the same name already exists
   */
  private static void validateUniqueSchemaName(SchemaPlus parentSchema, String name) {
    if (parentSchema == null || name == null) {
      return;
    }

    // Check if schema with this name already exists
    if (parentSchema.subSchemas().get(name) != null) {
      String errorMessage = String.format(
          "Schema with name '%s' already exists in parent schema. " +
          "Each schema must have a unique name within the same connection. " +
          "Existing schemas: %s",
          name,
          parentSchema.subSchemas().getNames(LikePattern.any()));
      LOGGER.error("Duplicate schema name detected: {}", errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    LOGGER.debug("Schema name '{}' is unique within parent schema", name);
  }

  private static void addMetadataSchemas(SchemaPlus parentSchema) {
    // Get the root schema to access all schemas for metadata
    SchemaPlus rootSchema = parentSchema;
    while (rootSchema.getParentSchema() != null) {
      rootSchema = rootSchema.getParentSchema();
    }

    // Only add metadata schemas if they don't already exist
    if (parentSchema.subSchemas().get("information_schema") == null) {
      InformationSchema infoSchema = new InformationSchema(parentSchema, "CALCITE");
      parentSchema.add("information_schema", infoSchema);
    }

    if (parentSchema.subSchemas().get("pg_catalog") == null) {
      PostgresMetadataSchema pgSchema = new PostgresMetadataSchema(parentSchema, "CALCITE");
      parentSchema.add("pg_catalog", pgSchema);
    }

    // Ensure the standard Calcite metadata schema is preserved
    if (rootSchema.subSchemas().get("metadata") != null && parentSchema.subSchemas().get("metadata") == null) {
      SchemaPlus metadataSchema = rootSchema.subSchemas().get("metadata");
      parentSchema.add("metadata", metadataSchema.unwrap(Schema.class));
    }
  }

  /**
   * Parse boolean value from operand map, handling both Boolean and String types.
   * This is needed because environment variable substitution can produce either type.
   */
  private static Boolean parseBooleanValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    return null;
  }
}
