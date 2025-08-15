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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.format.parquet.ConcurrentParquetCache;
import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.table.CsvTranslatableTable;
import org.apache.calcite.adapter.file.table.JsonTable;
import org.apache.calcite.adapter.file.table.JsonScannableTable;
import org.apache.calcite.adapter.file.table.ParquetTranslatableTable;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.table.DuckDBParquetTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.time.Instant;

/**
 * Refreshable table that maintains a Parquet cache for CSV/JSON source files.
 * Provides atomic updates when refresh intervals expire and source files change.
 */
public class RefreshableParquetCacheTable extends AbstractRefreshableTable 
    implements TranslatableTable {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshableParquetCacheTable.class);

  private final Source source;
  private final File cacheDir;
  private volatile File parquetFile;
  private final boolean typeInferenceEnabled;
  private final String columnNameCasing;
  private final @Nullable RelProtoDataType protoRowType;
  private final ExecutionEngineConfig.ExecutionEngineType engineType;
  protected volatile Table delegateTable;
  private final @Nullable SchemaPlus parentSchema;
  private final String fileSchemaName;
  
  // For DuckDB integration
  private @Nullable String schemaName;
  private @Nullable String tableName;

  public RefreshableParquetCacheTable(Source source, File initialParquetFile, 
      File cacheDir, @Nullable Duration refreshInterval, boolean typeInferenceEnabled,
      String columnNameCasing, @Nullable RelProtoDataType protoRowType,
      ExecutionEngineConfig.ExecutionEngineType engineType, @Nullable SchemaPlus parentSchema,
      String fileSchemaName) {
    super(source.path(), refreshInterval);
    this.source = source;
    this.parquetFile = initialParquetFile;
    this.cacheDir = cacheDir;
    this.typeInferenceEnabled = typeInferenceEnabled;
    this.columnNameCasing = columnNameCasing;
    this.protoRowType = protoRowType;
    this.engineType = engineType;
    this.parentSchema = parentSchema;
    this.fileSchemaName = fileSchemaName;
    
    // Initialize delegate table
    updateDelegateTable();
  }

  /**
   * Sets the schema and table names for DuckDB queries.
   */
  public void setDuckDBNames(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    
    // Update delegate if it's a DuckDB table
    if (delegateTable instanceof DuckDBParquetTable) {
      ((DuckDBParquetTable) delegateTable).setDuckDBNames(schemaName, tableName);
    }
  }

  @Override
  protected void doRefresh() {
    File sourceFile = new File(source.path());
    
    // Check if source file has been modified since our last refresh
    if (isFileModified(sourceFile)) {
      LOGGER.debug("Source file {} has been modified, updating parquet cache", source.path());
      
      try {
        // Atomically update the cache using the standard conversion method
        this.parquetFile = ParquetConversionUtil.convertToParquet(source, sourceFile.getName(), 
            createSourceTable(), cacheDir, parentSchema, fileSchemaName);
        
        // Update delegate table to use new parquet file
        updateDelegateTable();
        
        // Update our tracking of when the source file was last seen
        updateLastModified(sourceFile);
        
        LOGGER.info("Updated parquet cache for {}", source.path());
      } catch (Exception e) {
        LOGGER.error("Failed to update parquet cache for {}: {}", source.path(), e.getMessage(), e);
        // Continue with existing cache file
      }
    }
  }

  private Table createSourceTable() {
    String path = source.path().toLowerCase();
    if (path.endsWith(".csv") || path.endsWith(".csv.gz")) {
      return new CsvTranslatableTable(source, protoRowType, columnNameCasing, null);
    } else if (path.endsWith(".json") || path.endsWith(".json.gz")) {
      // Use JsonScannableTable instead of JsonTable for proper scanning support
      return new JsonScannableTable(source);
    } else {
      throw new IllegalArgumentException("Unsupported file type for refresh: " + source.path());
    }
  }

  private void updateDelegateTable() {
    if (engineType == ExecutionEngineConfig.ExecutionEngineType.DUCKDB) {
      DuckDBParquetTable duckTable = new DuckDBParquetTable(
          Sources.of(parquetFile), null, columnNameCasing);
      
      // Set DuckDB names if available
      if (schemaName != null && tableName != null) {
        duckTable.setDuckDBNames(schemaName, tableName);
      }
      
      this.delegateTable = duckTable;
    } else {
      // For PARQUET engine, use ParquetScannableTable which properly implements ScannableTable
      // This avoids the type mismatch issues with ParquetTranslatableTable
      this.delegateTable = new org.apache.calcite.adapter.file.table.ParquetScannableTable(parquetFile);
    }
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    refresh(); // Check if refresh needed before query
    if (delegateTable instanceof TranslatableTable) {
      return ((TranslatableTable) delegateTable).toRel(context, relOptTable);
    } else {
      // For ScannableTable delegates, use LogicalTableScan
      return org.apache.calcite.rel.logical.LogicalTableScan.create(context.getCluster(), relOptTable, 
                                                                     context.getTableHints());
    }
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    // Ensure we have the latest delegate table
    if (delegateTable == null) {
      updateDelegateTable();
    }
    return delegateTable.getRowType(typeFactory);
  }



  @Override
  public RefreshBehavior getRefreshBehavior() {
    return RefreshBehavior.SINGLE_FILE;
  }

  /**
   * Get the current parquet file being used.
   */
  public File getParquetFile() {
    return parquetFile;
  }

  /**
   * Get the source file being monitored.
   */
  public Source getSource() {
    return source;
  }


  @Override
  public String toString() {
    return "RefreshableParquetCacheTable(" + source.path() + " -> " + parquetFile.getName() + ")";
  }
}