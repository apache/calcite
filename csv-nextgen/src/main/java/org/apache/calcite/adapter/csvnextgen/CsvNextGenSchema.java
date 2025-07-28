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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Schema for CSV files with configurable execution engines.
 *
 * <p>This schema automatically discovers CSV and TSV files in a directory
 * and creates tables with the specified execution engine (Linq4j, Arrow, or Vectorized).
 */
class CsvNextGenSchema extends AbstractSchema {
  private final ImmutableList<Map<String, Object>> tables;
  private final @Nullable File baseDirectory;
  private final CsvNextGenSchemaFactory.ExecutionEngineType executionEngine;
  private final int batchSize;

  /**
   * Creates a CSV NextGen schema.
   *
   * @param parentSchema   Parent schema
   * @param name          Schema name
   * @param baseDirectory Base directory to look for CSV files, or null
   * @param tables        List containing explicit table definitions, or null
   * @param executionEngine Type of execution engine to use
   * @param batchSize     Batch size for columnar engines
   */
  CsvNextGenSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
      @Nullable List<Map<String, Object>> tables,
      CsvNextGenSchemaFactory.ExecutionEngineType executionEngine,
      int batchSize) {
    this.tables = tables == null ? ImmutableList.of() : ImmutableList.copyOf(tables);
    this.baseDirectory = baseDirectory;
    this.executionEngine = executionEngine;
    this.batchSize = batchSize;
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    // Add explicitly defined tables
    for (Map<String, Object> tableDef : tables) {
      createTableFromDefinition(builder, tableDef);
    }

    // Auto-discover CSV files in directory
    if (baseDirectory != null && baseDirectory.isDirectory()) {
      discoverCsvFiles(builder, baseDirectory);
    }

    return builder.build();
  }

  private void createTableFromDefinition(ImmutableMap.Builder<String, Table> builder,
      Map<String, Object> tableDef) {
    final String tableName = (String) tableDef.get("name");
    final String fileName = (String) tableDef.get("file");

    if (tableName == null || fileName == null) {
      return; // Skip invalid table definitions
    }

    final Source source = createSource(fileName);
    if (source != null) {
      final CsvNextGenTable table = createTable(source, tableDef);
      builder.put(tableName.toUpperCase(Locale.ROOT), table);
    }
  }

  private void discoverCsvFiles(ImmutableMap.Builder<String, Table> builder, File directory) {
    final File[] files = directory.listFiles();
    if (files == null) {
      return;
    }

    for (File file : files) {
      if (file.isDirectory()) {
        // Recursively scan subdirectories
        discoverCsvFiles(builder, file);
      } else if (isCsvFile(file)) {
        // Create table for CSV file
        final String tableName = getTableNameFromFile(file);
        final Source source = Sources.of(file);
        final CsvNextGenTable table = createTable(source, null);
        builder.put(tableName.toUpperCase(Locale.ROOT), table);
      }
    }
  }

  private boolean isCsvFile(File file) {
    String name = file.getName().toLowerCase(Locale.ROOT);
    // Remove .gz suffix if present
    if (name.endsWith(".gz")) {
      name = name.substring(0, name.length() - 3);
    }
    return name.endsWith(".csv") || name.endsWith(".tsv");
  }

  private String getTableNameFromFile(File file) {
    String name = file.getName();

    // Remove .gz suffix if present
    if (name.toLowerCase(Locale.ROOT).endsWith(".gz")) {
      name = name.substring(0, name.length() - 3);
    }

    // Remove .csv or .tsv extension
    if (name.toLowerCase(Locale.ROOT).endsWith(".csv")) {
      name = name.substring(0, name.length() - 4);
    } else if (name.toLowerCase(Locale.ROOT).endsWith(".tsv")) {
      name = name.substring(0, name.length() - 4);
    }

    return name;
  }

  private @Nullable Source createSource(String fileName) {
    if (baseDirectory == null) {
      return Sources.of(new File(fileName));
    }

    final File file = new File(fileName);
    final File resolvedFile = file.isAbsolute()
        ? file
        : new File(baseDirectory, fileName);

    return resolvedFile.exists() ? Sources.of(resolvedFile) : null;
  }

  private CsvNextGenTable createTable(Source source, @Nullable Map<String, Object> tableDef) {
    // Extract table-specific configuration if provided
    Boolean hasHeader = null;
    if (tableDef != null) {
      Object headerObj = tableDef.get("header");
      if (headerObj instanceof Boolean) {
        hasHeader = (Boolean) headerObj;
      }
    }

    // Default to true for header detection if not specified
    if (hasHeader == null) {
      hasHeader = true;
    }

    return new CsvNextGenTable(source, hasHeader, executionEngine, batchSize);
  }

  /**
   * Gets the configured execution engine type.
   */
  public CsvNextGenSchemaFactory.ExecutionEngineType getExecutionEngine() {
    return executionEngine;
  }

  /**
   * Gets the configured batch size.
   */
  public int getBatchSize() {
    return batchSize;
  }
}
