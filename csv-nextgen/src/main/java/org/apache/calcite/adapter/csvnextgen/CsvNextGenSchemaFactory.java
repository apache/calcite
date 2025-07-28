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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Factory that creates a {@link CsvNextGenSchema} with configurable execution engines.
 *
 * <p>Allows a custom CSV schema with pluggable execution engines to be included
 * in a model.json file.
 *
 * <p>Configuration options:
 * <ul>
 *   <li><b>directory</b>: Base directory containing CSV files (for auto-discovery)</li>
 *   <li><b>file</b>: Single CSV file path (alternative to directory)</li>
 *   <li><b>tableName</b>: Table name when using single file (default: filename without
 *       extension)</li>
 *   <li><b>executionEngine</b>: "linq4j", "arrow", or "vectorized" (default: "vectorized")</li>
 *   <li><b>batchSize</b>: Number of rows per batch for columnar engines (default: 2048)</li>
 *   <li><b>tables</b>: Optional explicit table definitions</li>
 * </ul>
 *
 * <p>Example model.json:
 * <pre>{@code
 * {
 *   "version": "1.0",
 *   "defaultSchema": "csvnextgen",
 *   "schemas": [
 *     {
 *       "name": "csvnextgen",
 *       "type": "custom",
 *       "factory": "org.apache.calcite.adapter.csvnextgen.CsvNextGenSchemaFactory",
 *       "operand": {
 *         "directory": "data/",
 *         "executionEngine": "vectorized",
 *         "batchSize": 4096
 *       }
 *     }
 *   ]
 * }
 * }</pre>
 */
@SuppressWarnings("UnusedDeclaration")
public class CsvNextGenSchemaFactory implements SchemaFactory {
  /** Public singleton, per factory contract. */
  public static final CsvNextGenSchemaFactory INSTANCE = new CsvNextGenSchemaFactory();

  /** Default execution engine if not specified. */
  public static final String DEFAULT_EXECUTION_ENGINE = "vectorized";

  /** Default batch size for columnar engines. */
  public static final int DEFAULT_BATCH_SIZE = 2048;

  private CsvNextGenSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    // Extract configuration parameters
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> tables = (List) operand.get("tables");

    final File baseDirectory =
        (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    final String directory = (String) operand.get("directory");
    final String singleFile = (String) operand.get("file");
    final String tableName = (String) operand.get("tableName");

    // Execution engine configuration
    final String executionEngine =
        (String) operand.getOrDefault("executionEngine", DEFAULT_EXECUTION_ENGINE);
    final Object batchSizeObj = operand.get("batchSize");
    final int batchSize = batchSizeObj instanceof Number
        ? ((Number) batchSizeObj).intValue()
        : DEFAULT_BATCH_SIZE;

    // Handle single file mode
    if (singleFile != null) {
      return createSingleFileSchema(parentSchema, name, baseDirectory, singleFile, tableName,
          executionEngine, batchSize);
    }

    // Resolve directory path for multi-file mode
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

    // Validate execution engine
    ExecutionEngineType engineType;
    try {
      engineType = ExecutionEngineType.valueOf(executionEngine.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid execution engine: " + executionEngine
          + ". Valid options: linq4j, arrow, vectorized", e);
    }

    return new CsvNextGenSchema(parentSchema, name, directoryFile, tables, engineType, batchSize);
  }

  /**
   * Creates a schema for a single CSV file.
   */
  private Schema createSingleFileSchema(SchemaPlus parentSchema, String name,
      @Nullable File baseDirectory, String fileName, @Nullable String tableName,
      String executionEngine, int batchSize) {

    // Resolve file path
    File file = new File(fileName);
    if (baseDirectory != null && !file.isAbsolute()) {
      file = new File(baseDirectory, fileName);
    }

    if (!file.exists()) {
      throw new IllegalArgumentException("File does not exist: " + file.getAbsolutePath());
    }

    // Generate table name if not provided
    String resolvedTableName = tableName;
    if (resolvedTableName == null) {
      String name_temp = file.getName();

      // Remove .gz suffix if present
      if (name_temp.toLowerCase(Locale.ROOT).endsWith(".gz")) {
        name_temp = name_temp.substring(0, name_temp.length() - 3);
      }

      // Remove .csv or .tsv extension
      if (name_temp.toLowerCase(Locale.ROOT).endsWith(".csv")) {
        name_temp = name_temp.substring(0, name_temp.length() - 4);
      } else if (name_temp.toLowerCase(Locale.ROOT).endsWith(".tsv")) {
        name_temp = name_temp.substring(0, name_temp.length() - 4);
      }

      resolvedTableName = name_temp;
    }

    // Create table definition for the single file
    Map<String, Object> tableDef = new java.util.HashMap<>();
    tableDef.put("name", resolvedTableName);
    tableDef.put("file", file.getName());
    tableDef.put("header", true); // Default to true for single file mode

    List<Map<String, Object>> tablesList = java.util.Collections.singletonList(tableDef);

    // Validate execution engine
    ExecutionEngineType engineType;
    try {
      engineType = ExecutionEngineType.valueOf(executionEngine.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid execution engine: " + executionEngine
          + ". Valid options: linq4j, arrow, vectorized", e);
    }

    return new CsvNextGenSchema(parentSchema, name, file.getParentFile(), tablesList, engineType,
        batchSize);
  }

  /**
   * Supported execution engine types.
   */
  public enum ExecutionEngineType {
    /**
     * Traditional row-by-row processing using Linq4j enumerables.
     * Best for: OLTP workloads, small datasets, row-wise operations.
     */
    LINQ4J,

    /**
     * Arrow-based columnar processing with standard enumerable interface.
     * Best for: Medium datasets, mixed workloads.
     */
    ARROW,

    /**
     * Vectorized Arrow processing with optimized columnar operations.
     * Best for: OLAP workloads, large datasets, analytical queries.
     */
    VECTORIZED
  }
}
