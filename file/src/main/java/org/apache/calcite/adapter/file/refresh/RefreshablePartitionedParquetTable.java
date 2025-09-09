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
import org.apache.calcite.adapter.file.partition.PartitionDetector;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.table.PartitionedParquetTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Refreshable partitioned Parquet table that can discover new partitions.
 */
public class RefreshablePartitionedParquetTable extends AbstractTable
    implements ScannableTable, RefreshableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshablePartitionedParquetTable.class);

  private final String tableName;
  private final File directory;
  private final String pattern;
  private final PartitionedTableConfig config;
  private final ExecutionEngineConfig engineConfig;
  private final @Nullable Duration refreshInterval;
  private @Nullable Instant lastRefreshTime;
  private final @Nullable Map<String, Object> constraintConfig;
  private final @Nullable String schemaName;

  private volatile PartitionedParquetTable currentTable;
  private volatile List<String> lastDiscoveredFiles;

  // For refresh notifications to DuckDB
  private org.apache.calcite.adapter.file.@Nullable FileSchema fileSchema;
  private @Nullable String tableNameForNotification;

  public RefreshablePartitionedParquetTable(String tableName, File directory,
      String pattern, PartitionedTableConfig config,
      ExecutionEngineConfig engineConfig, @Nullable Duration refreshInterval) {
    this(tableName, directory, pattern, config, engineConfig, refreshInterval, null, null);
  }

  public RefreshablePartitionedParquetTable(String tableName, File directory,
      String pattern, PartitionedTableConfig config,
      ExecutionEngineConfig engineConfig, @Nullable Duration refreshInterval,
      @Nullable Map<String, Object> constraintConfig, @Nullable String schemaName) {
    this.tableName = tableName;
    this.directory = directory;
    this.pattern = pattern;
    this.config = config;
    this.engineConfig = engineConfig;
    this.refreshInterval = refreshInterval;
    this.constraintConfig = constraintConfig;
    this.schemaName = schemaName;

    // Initial discovery
    refreshTableDefinition();
  }

  /**
   * Sets the FileSchema and table name for refresh notifications.
   */
  public void setRefreshContext(org.apache.calcite.adapter.file.FileSchema fileSchema, String tableName) {
    this.fileSchema = fileSchema;
    this.tableNameForNotification = tableName;
  }

  /**
   * Get the list of parquet file paths for this partitioned table.
   * Used by conversion metadata to register with DuckDB.
   */
  public List<String> getFilePaths() {
    if (currentTable != null) {
      return currentTable.getFilePaths();
    }
    return lastDiscoveredFiles != null ? lastDiscoveredFiles : java.util.Collections.emptyList();
  }

  @Override public @Nullable Duration getRefreshInterval() {
    return refreshInterval;
  }

  @Override public @Nullable Instant getLastRefreshTime() {
    return lastRefreshTime;
  }

  @Override public boolean needsRefresh() {
    if (refreshInterval == null) {
      return false;
    }

    // First time - always refresh
    if (lastRefreshTime == null) {
      return true;
    }

    // Check if interval has elapsed
    return Instant.now().isAfter(lastRefreshTime.plus(refreshInterval));
  }

  @Override public void refresh() {
    if (!needsRefresh()) {
      return;
    }

    refreshTableDefinition();
    lastRefreshTime = Instant.now();
  }

  @Override public RefreshBehavior getRefreshBehavior() {
    return RefreshBehavior.PARTITIONED_TABLE;
  }

  private synchronized void refreshTableDefinition() {
    try {
      // Discover matching files
      List<String> matchingFiles = discoverFiles();

      // Only update if files have changed
      if (!matchingFiles.equals(lastDiscoveredFiles)) {
        // Detect partitions based on configuration
        PartitionDetector.PartitionInfo partitionInfo = null;
        if (!matchingFiles.isEmpty() && config.getPartitions() != null) {
          PartitionedTableConfig.PartitionConfig partConfig = config.getPartitions();
          String style = partConfig.getStyle();

          if (style == null || "auto".equals(style)) {
            // Auto-detect partition scheme
            partitionInfo = PartitionDetector.detectPartitionScheme(matchingFiles);
          } else if ("hive".equals(style)) {
            // Hive-style partitioning (key=value)
            partitionInfo = PartitionDetector.extractHivePartitions(matchingFiles.get(0));
          } else if ("directory".equals(style) && partConfig.getColumns() != null) {
            // Directory-based partitioning
            partitionInfo =
                PartitionDetector.extractDirectoryPartitions(
                    matchingFiles.get(0), partConfig.getColumns());
          } else if ("custom".equals(style) && partConfig.getRegex() != null) {
            // Custom regex partitioning
            partitionInfo =
                PartitionDetector.extractCustomPartitions(
                    matchingFiles.get(0), partConfig.getRegex(),
                    partConfig.getColumnMappings());
          }
        }

        // Get column types if configured
        Map<String, String> columnTypes = null;
        if (config.getPartitions() != null) {
          // Try to get typed column definitions first
          if (config.getPartitions().getColumnDefinitions() != null) {
            columnTypes = new java.util.HashMap<>();
            for (PartitionedTableConfig.ColumnDefinition colDef
                 : config.getPartitions().getColumnDefinitions()) {
              columnTypes.put(colDef.getName(), colDef.getType());
            }
          } else if (config.getPartitions().getColumns() != null) {
            // Fall back to simple string columns (all VARCHAR)
            columnTypes = new java.util.HashMap<>();
            for (String col : config.getPartitions().getColumns()) {
              columnTypes.put(col, "VARCHAR");
            }
          }
        }

        // Extract custom regex info if available
        String regex = null;
        List<PartitionedTableConfig.ColumnMapping> colMappings = null;
        if (config.getPartitions() != null && "custom".equals(config.getPartitions().getStyle())) {
          regex = config.getPartitions().getRegex();
          colMappings = config.getPartitions().getColumnMappings();
        }

        // Create new table instance with constraint config and qualified name
        currentTable =
            new PartitionedParquetTable(matchingFiles, partitionInfo,
                engineConfig, columnTypes, regex, colMappings, constraintConfig, schemaName, tableName);
        lastDiscoveredFiles = matchingFiles;

        LOGGER.debug("[RefreshablePartitionedParquetTable] Discovered {} files for table: {}", matchingFiles.size(), tableName);

        // Notify listeners (e.g., DUCKDB) that the table has been refreshed
        // For partitioned tables, we notify with the first file as a representative
        if (fileSchema != null && tableNameForNotification != null && !matchingFiles.isEmpty()) {
          fileSchema.notifyTableRefreshed(tableNameForNotification, new File(matchingFiles.get(0)));
          LOGGER.debug("Notified listeners of refresh for partitioned table '{}'", tableNameForNotification);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to refresh partitioned table: {}", e.getMessage());
    }
  }

  private List<String> discoverFiles() {
    List<String> files = new ArrayList<>();
    PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);

    discoverFilesRecursive(directory, matcher, files);
    return files;
  }

  private void discoverFilesRecursive(File dir, PathMatcher matcher, List<String> files) {
    if (!dir.exists() || !dir.isDirectory()) {
      return;
    }

    File[] children = dir.listFiles();
    if (children == null) {
      return;
    }

    for (File child : children) {
      if (child.isDirectory()) {
        discoverFilesRecursive(child, matcher, files);
      } else if (child.isFile()) {
        // Check if file matches pattern
        java.nio.file.Path relativePath = directory.toPath().relativize(child.toPath());
        if (matcher.matches(relativePath)) {
          files.add(child.getAbsolutePath());
        }
      }
    }
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    refresh(); // Check for updates
    return currentTable.getRowType(typeFactory);
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    refresh(); // Check for updates
    return currentTable.scan(root);
  }

  @Override public String toString() {
    return "RefreshablePartitionedParquetTable(" + tableName + ")";
  }
}
