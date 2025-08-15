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
    implements TranslatableTable, ScannableTable {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshableParquetCacheTable.class);

  private final Source source;
  private final Source originalSource; // The original file if source is a converted file
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
    this(source, null, initialParquetFile, cacheDir, refreshInterval, typeInferenceEnabled,
        columnNameCasing, protoRowType, engineType, parentSchema, fileSchemaName);
  }
  
  /**
   * Constructor with original source tracking for converted files.
   * 
   * @param source The source to read data from (may be converted JSON)
   * @param originalSource The original source file to monitor (e.g., Excel file), or null if source is original
   * @param initialParquetFile Initial Parquet file
   * @param cacheDir Cache directory
   * @param refreshInterval Refresh interval
   * @param typeInferenceEnabled Whether type inference is enabled
   * @param columnNameCasing Column name casing
   * @param protoRowType Proto row type
   * @param engineType Engine type
   * @param parentSchema Parent schema
   * @param fileSchemaName File schema name
   */
  public RefreshableParquetCacheTable(Source source, @Nullable Source originalSource,
      File initialParquetFile, File cacheDir, @Nullable Duration refreshInterval, 
      boolean typeInferenceEnabled, String columnNameCasing, @Nullable RelProtoDataType protoRowType,
      ExecutionEngineConfig.ExecutionEngineType engineType, @Nullable SchemaPlus parentSchema,
      String fileSchemaName) {
    // Monitor the original source if provided, otherwise monitor the direct source
    super(originalSource != null ? originalSource.path() : source.path(), refreshInterval);
    this.source = source;
    this.originalSource = originalSource;
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
    
    // No longer needed since DuckDB uses JDBC adapter
    // The DuckDB JDBC schema handles all table registration
  }

  @Override
  protected void doRefresh() {
    // Check the original source if we have one (for converted files), otherwise check direct source
    File fileToMonitor = originalSource != null ? 
        new File(originalSource.path()) : new File(source.path());
    
    // Check if monitored file has been modified since our last refresh
    if (isFileModified(fileToMonitor)) {
      LOGGER.debug("Source file {} has been modified, updating parquet cache", fileToMonitor.getPath());
      
      try {
        // If we have an original source, re-run the conversion pipeline
        if (originalSource != null) {
          rerunConversionsIfNeeded(fileToMonitor);
        }
        
        // Atomically update the cache using the standard conversion method
        // Note: createSourceTable() reads from 'source' which may be the converted JSON
        this.parquetFile = ParquetConversionUtil.convertToParquet(source, 
            new File(source.path()).getName(), 
            createSourceTable(), cacheDir, parentSchema, fileSchemaName);
        
        // Update delegate table to use new parquet file
        updateDelegateTable();
        
        // Update our tracking of when the monitored file was last seen
        updateLastModified(fileToMonitor);
        
        LOGGER.info("Updated parquet cache for {} (monitoring {})", 
            source.path(), fileToMonitor.getPath());
      } catch (Exception e) {
        LOGGER.error("Failed to update parquet cache for {}: {}", source.path(), e.getMessage(), e);
        // Continue with existing cache file
      }
    }
  }
  
  /**
   * Re-runs any necessary file conversions if the source requires it.
   * This handles Excel→JSON, HTML→JSON, XML→JSON, etc.
   */
  private void rerunConversionsIfNeeded(File sourceFile) {
    String path = sourceFile.getPath().toLowerCase();
    
    try {
      if (path.endsWith(".xlsx") || path.endsWith(".xls")) {
        // Re-convert Excel to JSON if needed
        org.apache.calcite.adapter.file.converters.SafeExcelToJsonConverter.convertIfNeeded(sourceFile, true);
        LOGGER.debug("Re-converted Excel file to JSON: {}", sourceFile.getName());
      } else if (path.endsWith(".html") || path.endsWith(".htm")) {
        // HTML conversion - the converter should check if re-conversion is needed
        // TODO: Implement HTML re-conversion check
        LOGGER.debug("HTML file detected, re-conversion check not yet implemented: {}", sourceFile.getName());
      } else if (path.endsWith(".xml")) {
        // XML conversion - the converter should check if re-conversion is needed
        // TODO: Implement XML re-conversion check
        LOGGER.debug("XML file detected, re-conversion check not yet implemented: {}", sourceFile.getName());
      }
      // For JSON files, no conversion needed - they are the source
      // For CSV files, no conversion needed - they are the source
    } catch (Exception e) {
      LOGGER.error("Failed to re-run conversions for {}: {}", sourceFile.getName(), e.getMessage(), e);
    }
  }

  private Table createSourceTable() {
    String path = source.path().toLowerCase();
    if (path.endsWith(".csv") || path.endsWith(".csv.gz")) {
      return new CsvTranslatableTable(source, protoRowType, columnNameCasing, null);
    } else if (path.endsWith(".json") || path.endsWith(".json.gz")) {
      // Use JsonScannableTable instead of JsonTable for proper scanning support
      return new JsonScannableTable(source);
    } else if (path.endsWith(".xlsx") || path.endsWith(".xls")) {
      // Excel files need conversion to JSON first
      try {
        File excelFile = new File(source.path());
        // Convert Excel to JSON
        org.apache.calcite.adapter.file.converters.SafeExcelToJsonConverter.convertIfNeeded(excelFile, true);
        
        // Find the generated JSON file - this is tricky because Excel creates multiple JSON files
        // We need to know which sheet/table this refreshable table is for
        // For now, just re-convert and let the cache handle it
        // The actual JSON file should already exist from initial conversion
        
        // This is a limitation - we're re-converting the whole Excel file
        // but only one sheet's table is being refreshed
        // TODO: Track which sheet this table represents
        
        return new JsonScannableTable(source);
      } catch (Exception e) {
        throw new RuntimeException("Failed to convert Excel file for refresh: " + source.path(), e);
      }
    } else if (path.endsWith(".html") || path.endsWith(".htm")) {
      // HTML files need conversion to JSON
      try {
        File htmlFile = new File(source.path());
        // TODO: HTML conversion would go here
        // For now, treat as unsupported
        throw new IllegalArgumentException("HTML refresh not yet implemented: " + source.path());
      } catch (Exception e) {
        throw new RuntimeException("Failed to convert HTML file for refresh: " + source.path(), e);
      }
    } else if (path.endsWith(".xml")) {
      // XML files need conversion to JSON
      try {
        File xmlFile = new File(source.path());
        // TODO: XML conversion would go here
        // For now, treat as unsupported
        throw new IllegalArgumentException("XML refresh not yet implemented: " + source.path());
      } catch (Exception e) {
        throw new RuntimeException("Failed to convert XML file for refresh: " + source.path(), e);
      }
    } else {
      throw new IllegalArgumentException("Unsupported file type for refresh: " + source.path());
    }
  }

  private void updateDelegateTable() {
    // DuckDB now uses JDBC adapter, so always use ParquetScannableTable
    // For PARQUET engine, use ParquetScannableTable which properly implements ScannableTable
    // This avoids the type mismatch issues with ParquetTranslatableTable
    this.delegateTable = new org.apache.calcite.adapter.file.table.ParquetScannableTable(parquetFile);
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
  public Enumerable<@Nullable Object[]> scan(DataContext root) {
    refresh(); // Check if refresh needed before scanning
    if (delegateTable instanceof ScannableTable) {
      return ((ScannableTable) delegateTable).scan(root);
    } else {
      throw new RuntimeException("Delegate table does not support scanning: " + delegateTable.getClass());
    }
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