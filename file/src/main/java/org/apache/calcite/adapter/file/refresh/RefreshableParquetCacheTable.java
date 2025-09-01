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
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.table.CsvTranslatableTable;
import org.apache.calcite.adapter.file.table.JsonScannableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;

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
  private final String tableNameCasing;
  private final @Nullable RelProtoDataType protoRowType;
  private final ExecutionEngineConfig.ExecutionEngineType engineType;
  protected volatile Table delegateTable;
  private final @Nullable SchemaPlus parentSchema;
  private final String fileSchemaName;

  // For DuckDB integration and refresh notifications
  private @Nullable String schemaName;
  private @Nullable String tableName;
  private org.apache.calcite.adapter.file.@Nullable FileSchema fileSchema;

  public RefreshableParquetCacheTable(Source source, File initialParquetFile,
      File cacheDir, @Nullable Duration refreshInterval, boolean typeInferenceEnabled,
      String columnNameCasing, String tableNameCasing, @Nullable RelProtoDataType protoRowType,
      ExecutionEngineConfig.ExecutionEngineType engineType, @Nullable SchemaPlus parentSchema,
      String fileSchemaName) {
    this(source, null, initialParquetFile, cacheDir, refreshInterval, typeInferenceEnabled,
        columnNameCasing, tableNameCasing, protoRowType, engineType, parentSchema, fileSchemaName);
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
   * @param tableNameCasing Table name casing
   * @param protoRowType Proto row type
   * @param engineType Engine type
   * @param parentSchema Parent schema
   * @param fileSchemaName File schema name
   */
  public RefreshableParquetCacheTable(Source source, @Nullable Source originalSource,
      File initialParquetFile, File cacheDir, @Nullable Duration refreshInterval,
      boolean typeInferenceEnabled, String columnNameCasing, String tableNameCasing, @Nullable RelProtoDataType protoRowType,
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
    this.tableNameCasing = tableNameCasing;
    this.protoRowType = protoRowType;
    this.engineType = engineType;
    this.parentSchema = parentSchema;
    this.fileSchemaName = fileSchemaName;

    // Initialize the last modified time
    File fileToMonitor = originalSource != null ?
        new File(originalSource.path()) : new File(source.path());
    if (fileToMonitor.exists()) {
      // For refresh scenarios, start with 0 so first refresh will detect changes
      this.lastModifiedTime = refreshInterval != null ? 0 : fileToMonitor.lastModified();
      LOGGER.info("RefreshableParquetCacheTable initialized for {} - monitoring {} (originalSource={}) with lastModifiedTime={} (refresh={})",
                   source.path(), fileToMonitor.getPath(), originalSource != null ? originalSource.path() : "null",
                   this.lastModifiedTime, refreshInterval != null ? "enabled" : "disabled");
    } else {
      LOGGER.warn("RefreshableParquetCacheTable initialized for {} - file to monitor does not exist: {} (originalSource={})",
                  source.path(), fileToMonitor.getPath(), originalSource != null ? originalSource.path() : "null");
    }

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

  /**
   * Sets the FileSchema and table name for refresh notifications.
   */
  public void setRefreshContext(org.apache.calcite.adapter.file.FileSchema fileSchema, String tableName) {
    this.fileSchema = fileSchema;
    this.tableName = tableName;
  }

  @Override protected void doRefresh() {
    LOGGER.debug("doRefresh() called for RefreshableParquetCacheTable: {}", source.path());

    // Check the original source if we have one (for converted files), otherwise check direct source
    File fileToMonitor = originalSource != null ?
        new File(originalSource.path()) : new File(source.path());

    LOGGER.debug("Monitoring file: {} (exists: {}, lastModified: {}, lastTracked: {})",
                 fileToMonitor.getPath(), fileToMonitor.exists(), fileToMonitor.lastModified(), lastModifiedTime);

    // Check if monitored file has been modified since our last refresh
    if (isFileModified(fileToMonitor)) {
      LOGGER.info("Source file {} has been modified, updating parquet cache", fileToMonitor.getPath());

      try {
        // If we have an original source, re-run the conversion pipeline
        if (originalSource != null) {
          rerunConversionsIfNeeded(fileToMonitor);

          // IMPORTANT: Update the source file's timestamp to trigger parquet re-conversion
          // The JSON file needs to be newer than the parquet file for conversion to happen
          File jsonFile = new File(source.path());
          if (jsonFile.exists()) {
            jsonFile.setLastModified(System.currentTimeMillis());
            LOGGER.debug("Updated JSON file timestamp to trigger parquet re-conversion");
          }
        }

        // Force delete the old parquet file to ensure a fresh conversion
        if (parquetFile != null && parquetFile.exists()) {
          long oldSize = parquetFile.length();
          long oldModified = parquetFile.lastModified();
          boolean deleted = parquetFile.delete();
          LOGGER.info("Deleted old parquet cache file: {} (size={}, modified={}, deleted={})",
                      parquetFile.getAbsolutePath(), oldSize, oldModified, deleted);
        }

        // Also delete any temporary parquet files that might be lingering
        File[] tempFiles = cacheDir.listFiles((dir, name) ->
            name.contains(".tmp.") && name.endsWith(".parquet"));
        if (tempFiles != null) {
          for (File tempFile : tempFiles) {
            tempFile.delete();
            LOGGER.debug("Deleted temp parquet file: {}", tempFile.getName());
          }
        }

        // Atomically update the cache using the standard conversion method
        // Note: createSourceTable() reads from 'source' which may be the converted JSON
        this.parquetFile =
            ParquetConversionUtil.convertToParquet(source, new File(source.path()).getName(),
            createSourceTable(), cacheDir, parentSchema, fileSchemaName, this.tableNameCasing);

        // Update delegate table to use new parquet file
        updateDelegateTable();

        // Update our tracking of when the monitored file was last seen
        updateLastModified(fileToMonitor);

        LOGGER.info("Updated parquet cache for {} (monitoring {}). New file: {} (size={}, modified={})",
            source.path(), fileToMonitor.getPath(),
            parquetFile.getAbsolutePath(), parquetFile.length(), parquetFile.lastModified());

        // Notify listeners (e.g., DUCKDB) that the table has been refreshed
        if (fileSchema != null && tableName != null) {
          fileSchema.notifyTableRefreshed(tableName, parquetFile);
          LOGGER.debug("Notified listeners of refresh for table '{}'", tableName);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to update parquet cache for {}: {}", source.path(), e.getMessage(), e);
        // Continue with existing cache file
      }
    } else {
      LOGGER.debug("No refresh needed for {} - file not modified", fileToMonitor.getPath());
    }
  }

  /**
   * Re-runs any necessary file conversions if the source requires it.
   * Uses the centralized FileConversionManager for all conversion logic.
   *
   * For JSONPath extractions, this method also finds all derived files that were
   * extracted from the source and re-runs their extractions.
   */
  private void rerunConversionsIfNeeded(File sourceFile) {
    try {
      File outputDir = sourceFile.getParentFile();

      // First, try standard conversion (for format conversions like HTML->JSON)
      boolean converted =
          org.apache.calcite.adapter.file.converters.FileConversionManager.convertIfNeeded(sourceFile, outputDir, columnNameCasing, tableNameCasing);

      if (converted) {
        LOGGER.debug("Re-converted source file: {}", sourceFile.getName());
      }

      // Additionally, for JSON source files, find and refresh any JSONPath extractions
      if (sourceFile.getName().toLowerCase().endsWith(".json")) {
        refreshJsonPathExtractions(sourceFile);
      }

    } catch (Exception e) {
      LOGGER.error("Failed to re-run conversions for {}: {}", sourceFile.getName(), e.getMessage(), e);
    }
  }

  /**
   * Finds and refreshes all JSONPath extractions that were derived from the given source JSON file.
   */
  private void refreshJsonPathExtractions(File sourceFile) {
    try {
      // Create metadata instance using the same directory structure as the original extraction
      // This will automatically use the central metadata directory if it was configured
      File metadataDir = sourceFile.getParentFile();
      org.apache.calcite.adapter.file.metadata.ConversionMetadata metadata =
          new org.apache.calcite.adapter.file.metadata.ConversionMetadata(metadataDir);

      // Find all files that were derived from this source via JSONPath extraction
      java.util.List<java.io.File> derivedFiles = metadata.findDerivedFiles(sourceFile);


      for (File derivedFile : derivedFiles) {
        org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record =
            metadata.getConversionRecord(derivedFile);

        if (record != null && record.getConversionType() != null &&
            record.getConversionType().startsWith("JSONPATH_EXTRACTION")) {
          // Extract the JSONPath from the conversion type
          String conversionType = record.getConversionType();
          String jsonPath = extractJsonPath(conversionType);

          if (jsonPath != null) {
            LOGGER.info("Re-running JSONPath extraction: {} -> {} using path {}",
                sourceFile.getName(), derivedFile.getName(), jsonPath);

            // Re-run the JSONPath extraction
            org.apache.calcite.adapter.file.converters.JsonPathConverter.extract(
                sourceFile, derivedFile, jsonPath, metadataDir);

            LOGGER.info("Refreshed JSONPath extraction: {} -> {} (path: {})",
                sourceFile.getName(), derivedFile.getName(), jsonPath);
          } else {
            LOGGER.warn("Could not extract JSONPath from conversion type: {}", conversionType);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to refresh JSONPath extractions for {}: {}", sourceFile.getName(), e.getMessage(), e);
    }
  }

  /**
   * Extracts the JSONPath expression from a conversion type string.
   * E.g., "JSONPATH_EXTRACTION[$.data.users]" -> "$.data.users"
   */
  private String extractJsonPath(String conversionType) {
    if (conversionType != null && conversionType.contains("[") && conversionType.contains("]")) {
      int start = conversionType.indexOf('[') + 1;
      int end = conversionType.lastIndexOf(']');
      if (start < end) {
        return conversionType.substring(start, end);
      }
    }
    return null;
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
        // Get the proper output directory from the parent directory structure
        File outputDir = cacheDir != null ? cacheDir : excelFile.getParentFile();
        org.apache.calcite.adapter.file.converters.SafeExcelToJsonConverter.convertIfNeeded(
            excelFile, outputDir, true, tableNameCasing, columnNameCasing, outputDir.getParentFile());

        // The source should already point to the correct JSON file
        // as it was set up during initial table discovery
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

    LOGGER.debug("Updating delegate table with parquet file: {}",
                 parquetFile != null ? parquetFile.getName() : "null");

    this.delegateTable = new org.apache.calcite.adapter.file.table.ParquetScannableTable(parquetFile);
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    LOGGER.debug("toRel() called for RefreshableParquetCacheTable: {}", source.path());
    refresh(); // Check if refresh needed before query
    LOGGER.debug("toRel() refresh complete for: {}", source.path());
    if (delegateTable instanceof TranslatableTable) {
      return ((TranslatableTable) delegateTable).toRel(context, relOptTable);
    } else {
      // For ScannableTable delegates, use LogicalTableScan
      return org.apache.calcite.rel.logical.LogicalTableScan.create(context.getCluster(), relOptTable,
                                                                     context.getTableHints());
    }
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    // Ensure we have the latest delegate table
    if (delegateTable == null) {
      updateDelegateTable();
    }
    return delegateTable.getRowType(typeFactory);
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    LOGGER.debug("scan() called for RefreshableParquetCacheTable: {}", source.path());
    refresh(); // Check if refresh needed before scanning
    LOGGER.debug("scan() refresh complete for: {}", source.path());
    if (delegateTable instanceof ScannableTable) {
      return ((ScannableTable) delegateTable).scan(root);
    } else {
      throw new RuntimeException("Delegate table does not support scanning: " + delegateTable.getClass());
    }
  }



  @Override public RefreshBehavior getRefreshBehavior() {
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


  @Override public String toString() {
    return "RefreshableParquetCacheTable(" + source.path() + " -> " + parquetFile.getName() + ")";
  }
}
