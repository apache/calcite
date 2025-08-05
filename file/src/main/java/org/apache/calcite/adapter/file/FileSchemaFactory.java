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

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

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
  /** Public singleton, per factory contract. */
  public static final FileSchemaFactory INSTANCE = new FileSchemaFactory();

  /** Name of the column that is implicitly created in a CSV stream table
   * to hold the data arrival time. */
  static final String ROWTIME_COLUMN_NAME = "ROWTIME";

  private FileSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    @SuppressWarnings("unchecked") List<Map<String, Object>> tables =
        (List) operand.get("tables");
    final File baseDirectory =
        (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    final String directory = (String) operand.get("directory");

    // Execution engine configuration
    final String executionEngine =
        (String) operand.getOrDefault("executionEngine",
            ExecutionEngineConfig.DEFAULT_EXECUTION_ENGINE);
    final Object batchSizeObj = operand.get("batchSize");
    final int batchSize = batchSizeObj instanceof Number
        ? ((Number) batchSizeObj).intValue()
        : ExecutionEngineConfig.DEFAULT_BATCH_SIZE;
    final Object memoryThresholdObj = operand.get("memoryThreshold");
    final long memoryThreshold = memoryThresholdObj instanceof Number
        ? ((Number) memoryThresholdObj).longValue()
        : ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD;

    final ExecutionEngineConfig engineConfig =
        new ExecutionEngineConfig(executionEngine, batchSize, memoryThreshold, null);

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

    // Get flatten option for JSON/YAML files
    final Boolean flatten = (Boolean) operand.get("flatten");

    // Get table name casing configuration (default to UPPER for backward compatibility)
    // Support both camelCase (model.json) and snake_case (JDBC URL) naming conventions
    String tableNameCasing = (String) operand.get("tableNameCasing");
    if (tableNameCasing == null) {
      tableNameCasing = (String) operand.getOrDefault("table_name_casing", "UPPER");
    }

    // Get column name casing configuration (default to UNCHANGED for backward compatibility)
    // Support both camelCase (model.json) and snake_case (JDBC URL) naming conventions
    String columnNameCasing = (String) operand.get("columnNameCasing");
    if (columnNameCasing == null) {
      columnNameCasing = (String) operand.getOrDefault("column_name_casing", "UNCHANGED");
    }

    File directoryFile = null;
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
    FileSchema fileSchema =
        new FileSchema(parentSchema, name, directoryFile, directoryPattern, tables, engineConfig, recursive,
        materializations, views, partitionedTables, refreshInterval, tableNameCasing,
        columnNameCasing, storageType, storageConfig, flatten);

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
}
