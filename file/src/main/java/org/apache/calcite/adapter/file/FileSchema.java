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

import org.apache.calcite.adapter.file.converters.DocxTableScanner;
import org.apache.calcite.adapter.file.converters.MarkdownTableScanner;
import org.apache.calcite.adapter.file.converters.PptxTableScanner;
import org.apache.calcite.adapter.file.converters.SafeExcelToJsonConverter;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.cache.StorageCacheManager;
import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.adapter.file.format.json.JsonMultiTableFactory;
import org.apache.calcite.adapter.file.format.json.JsonSearchConfig;
import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.iceberg.IcebergMetadataTables;
import org.apache.calcite.adapter.file.iceberg.IcebergTable;
import org.apache.calcite.adapter.file.materialized.MaterializedViewTable;
import org.apache.calcite.adapter.file.partition.PartitionDetector;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.refresh.RefreshInterval;
import org.apache.calcite.adapter.file.refresh.RefreshableCsvTable;
import org.apache.calcite.adapter.file.refresh.RefreshableJsonTable;
import org.apache.calcite.adapter.file.refresh.RefreshableParquetCacheTable;
import org.apache.calcite.adapter.file.refresh.RefreshablePartitionedParquetTable;
import org.apache.calcite.adapter.file.statistics.CachePrimer;
import org.apache.calcite.adapter.file.statistics.TableStatistics;
import org.apache.calcite.adapter.file.storage.HttpStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.file.storage.StorageProviderFile;
import org.apache.calcite.adapter.file.storage.StorageProviderSource;
import org.apache.calcite.adapter.file.table.CsvTranslatableTable;
import org.apache.calcite.adapter.file.table.EnhancedCsvTranslatableTable;
import org.apache.calcite.adapter.file.table.EnhancedJsonScannableTable;
import org.apache.calcite.adapter.file.table.FileTable;
import org.apache.calcite.adapter.file.table.GlobParquetTable;
import org.apache.calcite.adapter.file.table.JsonScannableTable;
import org.apache.calcite.adapter.file.table.ParquetJsonScannableTable;
import org.apache.calcite.adapter.file.table.ParquetTranslatableTable;
import org.apache.calcite.adapter.file.table.PartitionedParquetTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Schema mapped onto a set of URLs / HTML tables. Each table in the schema
 * is an HTML table on a URL.
 */
public class FileSchema extends AbstractSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSchema.class);
  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");

  private final ImmutableList<Map<String, Object>> tables;
  private final @Nullable File baseDirectory;
  private final @Nullable String directoryPattern;
  private final ExecutionEngineConfig engineConfig;
  private final boolean recursive;
  private final @Nullable List<Map<String, Object>> materializations;
  private final @Nullable List<Map<String, Object>> views;
  private final @Nullable List<Map<String, Object>> partitionedTables;
  private final @Nullable String refreshInterval;
  private final String tableNameCasing;
  private final String columnNameCasing;
  private final @Nullable String storageType;
  private final @Nullable Map<String, Object> storageConfig;
  private final @Nullable StorageProvider storageProvider;
  private final @Nullable Boolean flatten;
  private final CsvTypeInferrer.TypeInferenceConfig csvTypeInferenceConfig;
  private final boolean primeCache;

  /**
   * Creates a file schema with all features including storage provider support.
   *
   * @param parentSchema    Parent schema
   * @param name            Schema name
   * @param baseDirectory   Base directory to look for relative files, or null
   * @param directoryPattern Directory pattern for file discovery, or null
   * @param tables          List containing table identifiers, or null
   * @param engineConfig    Execution engine configuration
   * @param recursive       Whether to recursively scan subdirectories
   * @param materializations List of materialized view definitions, or null
   * @param views           List of view definitions, or null
   * @param partitionedTables List of partitioned table definitions, or null
   * @param refreshInterval Default refresh interval for tables (e.g., "5 minutes"), or null
   * @param tableNameCasing Table name casing: "UPPER", "LOWER", or "UNCHANGED"
   * @param columnNameCasing Column name casing: "UPPER", "LOWER", or "UNCHANGED"
   * @param storageType     Storage type (e.g., "local", "s3", "sharepoint"), or null
   * @param storageConfig   Storage-specific configuration, or null
   * @param flatten         Whether to flatten JSON/YAML structures, or null
   * @param csvTypeInference CSV type inference configuration, or null
   * @param primeCache      Whether to prime statistics cache on initialization (default true)
   */
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
      @Nullable String directoryPattern,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig,
      boolean recursive,
      @Nullable List<Map<String, Object>> materializations,
      @Nullable List<Map<String, Object>> views,
      @Nullable List<Map<String, Object>> partitionedTables,
      @Nullable String refreshInterval,
      String tableNameCasing,
      String columnNameCasing,
      @Nullable String storageType,
      @Nullable Map<String, Object> storageConfig,
      @Nullable Boolean flatten,
      @Nullable Map<String, Object> csvTypeInference,
      boolean primeCache) {
    this.tables =
        tables == null ? ImmutableList.of()
            : ImmutableList.copyOf(tables);
    this.baseDirectory = baseDirectory;
    this.directoryPattern = directoryPattern;
    this.engineConfig = engineConfig;
    this.recursive = recursive;
    this.materializations = materializations;
    this.views = views;
    this.partitionedTables = partitionedTables;
    this.refreshInterval = refreshInterval;
    this.parentSchema = parentSchema;
    this.name = name;
    this.tableNameCasing = tableNameCasing;
    this.columnNameCasing = columnNameCasing;
    this.storageType = storageType;
    this.storageConfig = storageConfig;
    this.flatten = flatten;
    this.csvTypeInferenceConfig = CsvTypeInferrer.TypeInferenceConfig.fromMap(csvTypeInference);
    this.primeCache = primeCache;

    // Initialize central metadata directory and storage cache if parquetCacheDirectory is configured
    if (engineConfig.getParquetCacheDirectory() != null) {
      File cacheDir = new File(engineConfig.getParquetCacheDirectory());
      String metadataStorageType = storageType != null ? storageType : "local";
      
      // Initialize conversion metadata
      ConversionMetadata.setCentralMetadataDirectory(cacheDir, metadataStorageType);
      LOGGER.debug("Initialized central metadata directory under: {}/metadata/{}", 
          cacheDir, metadataStorageType);
      
      // Initialize storage cache manager
      StorageCacheManager.initialize(cacheDir);
      LOGGER.debug("Initialized storage cache manager under: {}/storage_cache", cacheDir);
    }

    // Create storage provider if configured
    if (storageType != null) {
      LOGGER.debug("[FileSchema] Creating storage provider of type: {}", storageType);
      this.storageProvider = StorageProviderFactory.createFromType(storageType, storageConfig);
      LOGGER.debug("[FileSchema] Storage provider created: {}", this.storageProvider);
    } else {
      LOGGER.debug("[FileSchema] No storage provider configured");
      this.storageProvider = null;
    }

    // Prime cache if enabled (after schema is fully initialized)
    if (primeCache) {
      // Schedule cache priming to run after the schema is fully registered
      // This is done asynchronously to avoid blocking schema initialization
      primeCacheAsync();
    }
  }

  /**
   * Asynchronously prime the statistics cache for all tables in this schema.
   * This improves query performance by pre-loading table statistics.
   */
  private void primeCacheAsync() {
    // Create a background thread to prime the cache
    Thread primingThread = new Thread(() -> {
      try {
        // Wait a moment for schema registration to complete
        Thread.sleep(100);

        LOGGER.info("Starting cache priming for schema: {}", name);
        long startTime = System.currentTimeMillis();

        // Collect all tables in this schema
        Map<String, Table> tableMap = getTableMap();
        List<CachePrimer.TableInfo> tablesToPrime = new ArrayList<>();

        for (Map.Entry<String, Table> entry : tableMap.entrySet()) {
          String tableName = entry.getKey();
          Table table = entry.getValue();

          // Determine file for size-based sorting if possible
          File tableFile = null;
          if (table instanceof ParquetTranslatableTable) {
            // Extract file from ParquetTranslatableTable if possible
            try {
              java.lang.reflect.Field fileField =
                  ParquetTranslatableTable.class.getDeclaredField("parquetFile");
              fileField.setAccessible(true);
              tableFile = (File) fileField.get(table);
            } catch (Exception e) {
              // Ignore, will use null file
            }
          }

          tablesToPrime.add(new CachePrimer.TableInfo(name, tableName, table, tableFile));
        }

        // Sort by file size (smallest first) for optimal cache usage
        tablesToPrime.sort((a, b) -> {
          if (a.file == null && b.file == null) return 0;
          if (a.file == null) return -1;
          if (b.file == null) return 1;
          return Long.compare(a.file.length(), b.file.length());
        });

        // Prime each table's statistics
        int successCount = 0;
        for (CachePrimer.TableInfo tableInfo : tablesToPrime) {
          try {
            if (tableInfo.table instanceof org.apache.calcite.adapter.file.statistics.StatisticsProvider) {
              org.apache.calcite.adapter.file.statistics.StatisticsProvider provider =
                  (org.apache.calcite.adapter.file.statistics.StatisticsProvider) tableInfo.table;
              TableStatistics stats = provider.getTableStatistics(null);  // This loads and caches the statistics

              // CRITICAL: Load HLL sketches into the global cache with the correct table name
              if (stats != null && stats.getColumnStatistics() != null) {
                org.apache.calcite.adapter.file.statistics.HLLSketchCache cache =
                    org.apache.calcite.adapter.file.statistics.HLLSketchCache.getInstance();
                for (Map.Entry<String, org.apache.calcite.adapter.file.statistics.ColumnStatistics> entry :
                     stats.getColumnStatistics().entrySet()) {
                  String columnName = entry.getKey();
                  org.apache.calcite.adapter.file.statistics.ColumnStatistics colStats = entry.getValue();
                  if (colStats != null && colStats.getHllSketch() != null) {
                    // Use fully qualified name (schema.table.column) to prevent collisions
                    cache.putSketch(this.name, tableInfo.tableName, columnName, colStats.getHllSketch());
                    LOGGER.debug("Loaded HLL sketch for {}.{}.{} with estimate: {}",
                        this.name, tableInfo.tableName, columnName, colStats.getHllSketch().getEstimate());
                  }
                }
              }

              successCount++;
            }
          } catch (Exception e) {
            LOGGER.debug("Failed to prime cache for table {}: {}",
                tableInfo.tableName, e.getMessage());
          }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        LOGGER.info("Cache priming completed for schema {} in {}ms ({}/{} tables primed)",
            name, elapsed, successCount, tablesToPrime.size());

      } catch (Exception e) {
        LOGGER.warn("Cache priming failed for schema {}: {}", name, e.getMessage());
      }
    }, "CachePrimer-" + name);

    primingThread.setDaemon(true);  // Don't prevent JVM shutdown
    primingThread.start();
  }

  /**
   * Creates a file schema with all features including partitioned tables, refresh support,
   * and configurable identifier casing.
   *
   * @param parentSchema    Parent schema
   * @param name            Schema name
   * @param baseDirectory   Base directory to look for relative files, or null
   * @param directoryPattern Directory pattern for file discovery, or null
   * @param tables          List containing table identifiers, or null
   * @param engineConfig    Execution engine configuration
   * @param recursive       Whether to recursively scan subdirectories
   * @param materializations List of materialized view definitions, or null
   * @param views           List of view definitions, or null
   * @param partitionedTables List of partitioned table definitions, or null
   * @param refreshInterval Default refresh interval for tables (e.g., "5 minutes"), or null
   * @param tableNameCasing Table name casing: "UPPER", "LOWER", or "UNCHANGED"
   * @param columnNameCasing Column name casing: "UPPER", "LOWER", or "UNCHANGED"
   */
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
      @Nullable String directoryPattern,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig,
      boolean recursive,
      @Nullable List<Map<String, Object>> materializations,
      @Nullable List<Map<String, Object>> views,
      @Nullable List<Map<String, Object>> partitionedTables,
      @Nullable String refreshInterval,
      String tableNameCasing,
      String columnNameCasing) {
    this.tables =
        tables == null ? ImmutableList.of()
            : ImmutableList.copyOf(tables);
    this.baseDirectory = baseDirectory;
    this.directoryPattern = directoryPattern;
    this.engineConfig = engineConfig;
    this.recursive = recursive;
    this.materializations = materializations;
    this.views = views;
    this.partitionedTables = partitionedTables;
    this.refreshInterval = refreshInterval;
    this.parentSchema = parentSchema;
    this.name = name;
    this.tableNameCasing = tableNameCasing;
    this.columnNameCasing = columnNameCasing;
    this.storageType = null;
    this.storageConfig = null;
    this.storageProvider = null;
    this.flatten = null;
    this.csvTypeInferenceConfig = CsvTypeInferrer.TypeInferenceConfig.disabled();
    this.primeCache = true;  // Default to true for backward compatibility
  }

  /**
   * Creates a file schema with all features including partitioned tables and refresh support.
   * Uses default casing (UPPER for tables, UNCHANGED for columns).
   *
   * @param parentSchema    Parent schema
   * @param name            Schema name
   * @param baseDirectory   Base directory to look for relative files, or null
   * @param tables          List containing table identifiers, or null
   * @param engineConfig    Execution engine configuration
   * @param recursive       Whether to recursively scan subdirectories
   * @param materializations List of materialized view definitions, or null
   * @param views           List of view definitions, or null
   * @param partitionedTables List of partitioned table definitions, or null
   * @param refreshInterval Default refresh interval for tables (e.g., "5 minutes"), or null
   */
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
      @Nullable String directoryPattern,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig,
      boolean recursive,
      @Nullable List<Map<String, Object>> materializations,
      @Nullable List<Map<String, Object>> views,
      @Nullable List<Map<String, Object>> partitionedTables,
      @Nullable String refreshInterval) {
    this(parentSchema, name, baseDirectory, directoryPattern, tables, engineConfig, recursive,
        materializations, views, partitionedTables, refreshInterval, "UPPER", "UNCHANGED");
  }

  /**
   * Creates a file schema with execution engine support, recursive directory scanning,
   * multi-table Excel support, multi-table HTML support, and materialized views.
   *
   * @param parentSchema    Parent schema
   * @param name            Schema name
   * @param baseDirectory   Base directory to look for relative files, or null
   * @param tables          List containing table identifiers, or null
   * @param engineConfig    Execution engine configuration
   * @param recursive       Whether to recursively scan subdirectories
   * @param materializations List of materialized view definitions, or null
   * @param views           List of view definitions, or null
   */
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig,
      boolean recursive,
      @Nullable List<Map<String, Object>> materializations,
      @Nullable List<Map<String, Object>> views) {
    this(parentSchema, name, baseDirectory, null, tables, engineConfig, recursive,
        materializations, views, null, null);
  }

  private final SchemaPlus parentSchema;
  private final String name;

  /**
   * Creates a file schema with execution engine support and recursive directory scanning.
   *
   * @param parentSchema  Parent schema
   * @param name          Schema name
   * @param baseDirectory Base directory to look for relative files, or null
   * @param tables        List containing table identifiers, or null
   * @param engineConfig  Execution engine configuration
   * @param recursive     Whether to recursively scan subdirectories
   */
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig,
      boolean recursive) {
    this(parentSchema, name, baseDirectory, null, tables, engineConfig, recursive, null,
        null, null, null);
  }

  /**
   * Creates a file schema with execution engine support (non-recursive).
   *
   * @param parentSchema  Parent schema
   * @param name          Schema name
   * @param baseDirectory Base directory to look for relative files, or null
   * @param tables        List containing table identifiers, or null
   * @param engineConfig  Execution engine configuration
   */
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig) {
    this(parentSchema, name, baseDirectory, null, tables, engineConfig, false, null, null,
        null, null);
  }

  /**
   * Creates a file schema with default execution engine (for backward compatibility).
   *
   * @param parentSchema  Parent schema
   * @param name          Schema name
   * @param baseDirectory Base directory to look for relative files, or null
   * @param tables        List containing table identifiers, or null
   */
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
      @Nullable List<Map<String, Object>> tables) {
    this(parentSchema, name, baseDirectory, null, tables, new ExecutionEngineConfig(), false,
        null, null, null, null);
  }

  /**
   * Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or the original string.
   */
  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  /**
   * Applies the configured casing transformation to an identifier.
   */
  private String applyCasing(String identifier, String casing) {
    return org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(identifier, casing);
  }

  /**
   * Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or null.
   */
  private static @Nullable String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }


  private void convertExcelFilesToJson(File baseDirectory) {

    // Get the list of all files and directories
    File[] files = baseDirectory.listFiles();

    if (files != null) {
      for (File file : files) {
        // If it's a directory and recursive is enabled, recurse into it
        if (file.isDirectory() && recursive) {
          convertExcelFilesToJson(file);
        } else if ((file.getName().endsWith(".xlsx") || file.getName().endsWith(".xls"))
            && !file.getName().startsWith("~")) {
          // If it's a file ending in .xlsx, convert it
          try {
            // Always extract all sheets from Excel files
            SafeExcelToJsonConverter.convertIfNeeded(file, true);
          } catch (Exception e) {
            e.printStackTrace();
            LOGGER.debug("!");
          }
        }
      }
    }
  }

  private void convertHtmlFilesToJson(File baseDirectory) {
    // Get the list of all files and directories
    File[] files = baseDirectory.listFiles();

    if (files != null) {
      for (File file : files) {
        // If it's a directory and recursive is enabled, recurse into it
        if (file.isDirectory() && recursive) {
          convertHtmlFilesToJson(file);
        } else if ((file.getName().endsWith(".html") || file.getName().endsWith(".htm"))
            && !file.getName().startsWith("~")) {
          // If it's an HTML file, convert its tables to JSON
          try {
            org.apache.calcite.adapter.file.converters.HtmlToJsonConverter.convert(
                file, file.getParentFile(), columnNameCasing, tableNameCasing);
          } catch (Exception e) {
            LOGGER.error("Error converting HTML file: {} - {}", file.getName(), e.getMessage());
          }
        }
      }
    }
  }

  private void convertMarkdownFilesToJson(File baseDirectory) {
    // Get the list of all files and directories
    File[] files = baseDirectory.listFiles();

    if (files != null) {
      for (File file : files) {
        // If it's a directory and recursive is enabled, recurse into it
        if (file.isDirectory() && recursive) {
          convertMarkdownFilesToJson(file);
        } else if ((file.getName().endsWith(".md") || file.getName().endsWith(".markdown"))
            && !file.getName().startsWith("~")) {
          // If it's a Markdown file, scan it for tables
          try {
            MarkdownTableScanner.scanAndConvertTables(file);
          } catch (Exception e) {
            LOGGER.error("Error scanning Markdown file: {} - {}", file.getName(), e.getMessage());
          }
        }
      }
    }
  }

  private void convertDocxFilesToJson(File baseDirectory) {
    // Get the list of all files and directories
    File[] files = baseDirectory.listFiles();

    if (files != null) {
      for (File file : files) {
        // If it's a directory and recursive is enabled, recurse into it
        if (file.isDirectory() && recursive) {
          convertDocxFilesToJson(file);
        } else if (file.getName().endsWith(".docx") && !file.getName().startsWith("~")) {
          // If it's a DOCX file, scan it for tables
          try {
            DocxTableScanner.scanAndConvertTables(file);
          } catch (Exception e) {
            LOGGER.error("Error scanning DOCX file: {} - {}", file.getName(), e.getMessage());
          }
        }
      }
    }
  }

  private void convertPptxFilesToJson(File baseDirectory) {
    // Get the list of all files and directories
    File[] files = baseDirectory.listFiles();

    if (files != null) {
      for (File file : files) {
        // If it's a directory and recursive is enabled, recurse into it
        if (file.isDirectory() && recursive) {
          convertPptxFilesToJson(file);
        } else if (file.getName().endsWith(".pptx") && !file.getName().startsWith("~")) {
          // If it's a PPTX file, scan it for tables
          try {
            PptxTableScanner.scanAndConvertTables(file);
          } catch (Exception e) {
            LOGGER.error("Error scanning PPTX file: {} - {}", file.getName(), e.getMessage());
          }
        }
      }
    }
  }

  private File[] getFilesInDir(File dir) {
    List<File> files = new ArrayList<>();
    File[] fileArr = dir.listFiles();

    if (fileArr == null) {
      return new File[0];
    }

    // Debug logging
    if (recursive) {
      LOGGER.debug("[FileSchema] Scanning directory (recursive={}): {}", recursive, dir);
    }

    for (File file : fileArr) {
      if (!file.getName().startsWith("._")) {
        if (file.isDirectory() && recursive) {
          // Only recurse if recursive flag is set
          files.addAll(Arrays.asList(getFilesInDir(file)));
        } else if (file.isFile()) {
          final String nameSansGz = trim(file.getName(), ".gz");
          if (nameSansGz.endsWith(".csv")
              || nameSansGz.endsWith(".tsv")
              || nameSansGz.endsWith(".json")
              || nameSansGz.endsWith(".yaml")
              || nameSansGz.endsWith(".yml")
              || nameSansGz.endsWith(".arrow")
              || nameSansGz.endsWith(".parquet")) {
            files.add(file);
          }
        }
      }
    }

    return files.toArray(new File[0]);
  }

  // Make volatile for thread visibility
  private volatile Map<String, Table> tableCache = null;
  
  /**
   * Finds the original source file for a converted JSON file using metadata.
   * Returns null if this JSON file is not a conversion result.
   * 
   * @param jsonFile The JSON file that might be a conversion result
   * @return The original source, or null if this is not a converted file
   */
  private Source findOriginalSource(File jsonFile) {
    if (baseDirectory != null) {
      try {
        org.apache.calcite.adapter.file.metadata.ConversionMetadata metadata = 
            new org.apache.calcite.adapter.file.metadata.ConversionMetadata(baseDirectory);
        File originalFile = metadata.findOriginalSource(jsonFile);
        if (originalFile != null) {
          LOGGER.debug("Found original source {} for {} from metadata", 
              originalFile.getName(), jsonFile.getName());
          return Sources.of(originalFile);
        }
      } catch (Exception e) {
        LOGGER.debug("Failed to check conversion metadata: {}", e.getMessage());
      }
    }
    
    // No metadata found - this is either not a converted file or metadata is missing
    return null;
  }

  /**
   * Clear the table cache. This is primarily for testing to ensure
   * test isolation. In production, each connection typically gets its
   * own FileSchema instance so cache clearing isn't needed.
   */
  public synchronized void clearTableCache() {
    tableCache = null;
    LOGGER.debug("[FileSchema] Table cache cleared");
  }

  @Override protected synchronized Map<String, Table> getTableMap() {
    try {
      // Use cached tables if already computed
      Map<String, Table> cached = tableCache;
      if (cached != null) {
        LOGGER.debug("[FileSchema.getTableMap] Returning cached tables: {}", cached.size());
        LOGGER.debug("Returning cached tables: size={}, engine={}", cached.size(), engineConfig.getEngineType());
        return cached;
      }

      LOGGER.debug("[FileSchema.getTableMap] Computing tables! baseDirectory={}, storageProvider={}, storageType={}",
                   baseDirectory, storageProvider, storageType);
      LOGGER.debug("Computing tables: engine={}, baseDirectory={}", engineConfig.getEngineType(), baseDirectory);

    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    // Track table names to handle duplicates
    final Map<String, Integer> tableNameCounts = new HashMap<>();

    for (Map<String, Object> tableDef : this.tables) {
      addTable(builder, tableDef);
    }

    // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
    // ".json.gz".
    // If storage provider is configured, skip local file operations
    LOGGER.debug("[FileSchema.getTableMap] Checking conditions: baseDirectory={}, storageProvider={}",
                 baseDirectory, storageProvider);
    if (baseDirectory != null && storageProvider == null) {
      LOGGER.debug("[FileSchema.getTableMap] Using local file system");

      convertExcelFilesToJson(baseDirectory);

      // Convert Markdown files to JSON
      convertMarkdownFilesToJson(baseDirectory);

      // Convert DOCX files to JSON
      convertDocxFilesToJson(baseDirectory);

      // Convert PPTX files to JSON
      convertPptxFilesToJson(baseDirectory);

      // Convert HTML files to JSON
      convertHtmlFilesToJson(baseDirectory);

      final Source baseSource = Sources.of(baseDirectory);
      // Get files using glob or regular directory scanning
      File[] files = getFilesForProcessing();
      LOGGER.debug("[FileSchema] Found {} files for processing", files.length);
      // Track table names to handle conflicts
      Map<String, String> tableNameToFileType = new HashMap<>();

      // Build a map from table name to table; each file becomes a table.
      for (File file : files) {
        // Use DirectFileSource for PARQUET engine to bypass Sources cache, but handle gzip properly
        Source source;
        // Check if this is a StorageProviderFile
        if (file instanceof StorageProviderFile) {
          StorageProviderFile spFile = (StorageProviderFile) file;
          source = new StorageProviderSource(spFile.getFileEntry(), spFile.getStorageProvider());
        } else if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.PARQUET) {
          // For gzipped files, we still need Sources.of() to handle decompression
          if (file.getName().endsWith(".gz")) {
            source = Sources.of(file);
          } else {
            source = new DirectFileSource(file);
          }
        } else {
          source = Sources.of(file);
        }
        Source sourceSansGz = source.trim(".gz");
        Source sourceSansJson = sourceSansGz.trimOrNull(".json");
        if (sourceSansJson == null) {
          sourceSansJson = sourceSansGz.trimOrNull(".yaml");
        }
        if (sourceSansJson == null) {
          sourceSansJson = sourceSansGz.trimOrNull(".yml");
        }
        if (sourceSansJson != null) {
          String rawName = WHITESPACE_PATTERN.matcher(sourceSansJson.relative(baseSource).path()
              .replace(File.separator, "_"))
              .replaceAll("_");
          String baseName = applyCasing(rawName, tableNameCasing);
          LOGGER.debug("Converting table name: '{}' -> '{}' (casing={})", rawName, baseName, tableNameCasing);
          // Handle duplicate table names by adding extension suffix
          String tableName = baseName;
          if (tableNameCounts.containsKey(baseName)) {
            // Add file extension to disambiguate
            String ext = source.path().endsWith(".yaml")
                || source.path().endsWith(".yaml.gz") ? "_YAML"
                       : source.path().endsWith(".yml")
                       || source.path().endsWith(".yml.gz") ? "_YML"
                       : "_JSON";
            tableName = baseName + ext;
          }
          tableNameCounts.put(baseName, tableNameCounts.getOrDefault(baseName, 0) + 1);
          // Skip Calcite model files (they contain schema definitions, not data)
          if (isCalciteModelFile(source)) {
            LOGGER.debug("Skipping Calcite model file: {}", source.path());
            continue;
          }

          // Create a table definition with schema-level refresh interval for JSON files
          Map<String, Object> autoTableDef = null;
          if (this.refreshInterval != null) {
            autoTableDef = new HashMap<>();
            autoTableDef.put("refreshInterval", this.refreshInterval);
          }
          addTable(builder, source, tableName, autoTableDef);
        }
        final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
        if (sourceSansCsv != null) {
          String tableName =
              applyCasing(
                  WHITESPACE_PATTERN.matcher(sourceSansCsv.relative(baseSource).path()
              .replace(File.separator, "_"))
              .replaceAll("_"), tableNameCasing);
          tableNameToFileType.put(tableName, "csv");
          addTable(builder, source, tableName, null);
        }
        final Source sourceSansTsv = sourceSansGz.trimOrNull(".tsv");
        if (sourceSansTsv != null) {
          String tableName =
              applyCasing(
                  WHITESPACE_PATTERN.matcher(sourceSansTsv.relative(baseSource).path()
              .replace(File.separator, "_"))
              .replaceAll("_"), tableNameCasing);
          addTable(builder, source, tableName, null);
        }
        final Source sourceSansParquet =
            sourceSansGz.trimOrNull(".parquet");
        if (sourceSansParquet != null) {
          String tableName = applyCasing(WHITESPACE_PATTERN
              .matcher(sourceSansParquet.relative(baseSource).path()
              .replace(File.separator, "_"))
              .replaceAll("_"), tableNameCasing);
          LOGGER.debug("Found Parquet file in directory scan: {} -> table: {}", source.path(), tableName);
          try {
            // Add Parquet file using appropriate table type based on execution engine
            // DuckDB engine uses JDBC adapter, not FileSchema
            if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.DUCKDB) {
              throw new IllegalStateException("DuckDB engine should use JDBC adapter, not FileSchema");
            }
            // Use standard Parquet table
            Table table = new ParquetTranslatableTable(new java.io.File(source.path()));
            builder.put(tableName, table);
          } catch (Exception e) {
            LOGGER.error("Failed to add Parquet table: {}", e.getMessage());
          }
        }
        final Source sourceSansArrow = sourceSansGz.trimOrNull(".arrow");
        if (sourceSansArrow != null) {
          String tableName = applyCasing(WHITESPACE_PATTERN
              .matcher(sourceSansArrow.relative(baseSource).path()
              .replace(File.separator, "_"))
              .replaceAll("_"), tableNameCasing);
          try {
            Table arrowTable = createArrowTable(new java.io.File(source.path()));
            if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.PARQUET) {
              // Try to convert Arrow file to Parquet, but fall back to ArrowTable if conversion fails
              try {
                File cacheDir =
                    ParquetConversionUtil.getParquetCacheDir(baseDirectory, engineConfig.getParquetCacheDirectory());
                File parquetFile =
                    ParquetConversionUtil.convertToParquet(source, tableName, arrowTable, cacheDir, parentSchema, "FILE");
                Table table = new ParquetTranslatableTable(parquetFile);
                builder.put(tableName, table);
              } catch (Exception conversionException) {
                LOGGER.warn("Parquet conversion failed for {}, using Arrow table: {}", tableName, conversionException.getMessage());
                // Fall back to using the original Arrow table
                builder.put(tableName, arrowTable);
              }
            } else {
              // Add Arrow file as an ArrowTable
              builder.put(tableName, arrowTable);
            }
          } catch (Exception e) {
            LOGGER.error("Failed to add Arrow table: {}", e.getMessage());
          }
        }
        // HTML files are always scanned for tables in directory mode
        // Explicit HTML tables with URL fragments are handled via table definitions
      }

      // HTML tables are now handled as JSON files created by convertHtmlFilesToJson
      // The JSON files will be picked up in the regular file scanning below
    } else if (storageProvider != null) {
      // Process files from storage provider
      LOGGER.debug("[FileSchema.getTableMap] Using storage provider to process files");
      processStorageProviderFiles(builder, tableNameCounts);
    }

    // Process materialized views (only works with Parquet engine)
    if (materializations != null
        && engineConfig.getEngineType()
        == ExecutionEngineConfig.ExecutionEngineType.PARQUET) {
      for (Map<String, Object> materialization : materializations) {
        String viewName = (String) materialization.get("view");
        String tableName = (String) materialization.get("table");
        String sql = (String) materialization.get("sql");

        if (viewName != null && tableName != null && sql != null) {
          try {
            File mvParquetFile = null;
            if (baseDirectory != null) {
              File mvDir = new File(baseDirectory, ".materialized_views");
              if (!mvDir.exists()) {
                mvDir.mkdirs();
              }
              mvParquetFile = new File(mvDir, tableName + ".parquet");

              if (mvParquetFile.exists()) {
                // If the Parquet file exists, use appropriate table based on execution engine
                LOGGER.debug("Found existing materialized view: {}", mvParquetFile.getPath());
                // DuckDB engine uses JDBC adapter, not FileSchema
                if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.DUCKDB) {
                  throw new IllegalStateException("DuckDB engine should use JDBC adapter, not FileSchema");
                }
                // Use standard Parquet table
                LOGGER.debug("Using ParquetTranslatableTable to read materialized view");
                Table mvTable = new ParquetTranslatableTable(mvParquetFile);
                builder.put(applyCasing(viewName, tableNameCasing), mvTable);
                LOGGER.debug("Successfully added table for materialized view: {}", viewName);
              } else {
                // Create a MaterializedViewTable that will execute the SQL when queried
                // and cache results to Parquet on first access
                LOGGER.debug("Creating materialized view: {}", viewName);
                LOGGER.debug("Will materialize to: {}", mvParquetFile.getPath());

                // Create a view using ViewTable
                String modifiedSql = sql;
                // Ensure the SQL references tables with proper schema prefix
                if (!sql.toUpperCase(Locale.ROOT).contains("FROM " + name + ".")) {
                  // Simple heuristic: replace unqualified table names
                  modifiedSql =
                    sql.replace("FROM ", "FROM " + name + ".");
                }

                final String finalSql = modifiedSql;
                final File finalMvFile = mvParquetFile;
                final String finalViewName = viewName;

                // Create a MaterializedViewTable that executes SQL on first access
                Table mvTable =
                    new MaterializedViewTable(parentSchema, name,
                        finalViewName, finalSql, finalMvFile, builder.build());
                builder.put(applyCasing(viewName, tableNameCasing), mvTable);
              }
            }
          } catch (Exception e) {
            LOGGER.error("Failed to create materialized view {}: {}", viewName, e.getMessage());
            e.printStackTrace();
          }
        }
      }
    } else if (materializations != null) {
      LOGGER.error("ERROR: Materialized views are only supported with Parquet execution engine");
      LOGGER.error("Current engine: {}", engineConfig.getEngineType());
      LOGGER.error("To use materialized views, set executionEngine to 'parquet' in your schema configuration");
    }

    // Process views
    if (views != null) {
      for (Map<String, Object> view : views) {
        String viewName = (String) view.get("name");
        String sql = (String) view.get("sql");

        if (viewName != null && sql != null) {
          // For now, just log the view registration
          // In a full implementation, we would create a proper view table
          LOGGER.debug("View registered: {}", viewName);
          // TODO: Implement actual view creation
        }
      }
    }

    // Process partitioned tables
    if (partitionedTables != null) {
      processPartitionedTables(builder);
    }

    tableCache = builder.build();
    LOGGER.debug("[FileSchema.getTableMap] Computed {} tables: {}", tableCache.size(), tableCache.keySet());
    return tableCache;
    } catch (Exception e) {
      LOGGER.error("[FileSchema.getTableMap] Error computing tables: {}", e.getMessage());
      e.printStackTrace();
      return ImmutableMap.of();
    }
  }

  private boolean addTable(ImmutableMap.Builder<String, Table> builder,
      Map<String, Object> tableDef) {
    final String tableName = (String) tableDef.get("name");
    final String url = (String) tableDef.get("url");

    // Check if URL contains glob patterns
    if (isGlobPattern(url)) {
      // Create GlobParquetTable for glob patterns
      String refreshInterval = (String) tableDef.get("refreshInterval");
      Duration refreshDuration = RefreshInterval.parse(refreshInterval);

      // Get cache directory for glob results
      File cacheDir = baseDirectory != null
          ? new File(baseDirectory, ".glob_cache")
          : new File(System.getProperty("java.io.tmpdir"), "calcite_glob_cache");

      Table globTable = new GlobParquetTable(url, tableName, cacheDir, refreshDuration);
      builder.put(applyCasing(tableName, tableNameCasing), globTable);
      return true;
    }

    // Check if HTTP URL with custom configuration (POST, headers, etc.)
    if ((url.startsWith("http://") || url.startsWith("https://"))
        && hasHttpConfiguration(tableDef)) {
      final Source source = resolveSourceWithHttpConfig(url, tableDef);
      LOGGER.debug("Creating HTTP table '{}' with source: {} (type: {})", tableName, source.path(), source.getClass().getName());
      return addTable(builder, source, tableName, tableDef);
    }

    // Regular processing for URLs
    final Source source = resolveSource(url);
    return addTable(builder, source, tableName, tableDef);
  }

  /**
   * Checks if a URL contains glob patterns like *, ?, or [].
   */
  private boolean isGlobPattern(String url) {
    // URLs with http/https protocols are never glob patterns
    if (url.startsWith("http://") || url.startsWith("https://")) {
      return false;
    }

    // Remove protocol prefix if present
    String path = url;
    if (url.contains("://")) {
      int idx = url.indexOf("://");
      path = url.substring(idx + 3);
    }

    // Check for glob characters
    return path.contains("*") || path.contains("?")
           || (path.contains("[") && path.contains("]"));
  }

  /**
   * Checks if a table definition has HTTP-specific configuration.
   */
  private boolean hasHttpConfiguration(Map<String, Object> tableDef) {
    return tableDef != null && (
        tableDef.containsKey("method") ||
        tableDef.containsKey("body") ||
        tableDef.containsKey("headers") ||
        tableDef.containsKey("mimeType"));
  }

  /**
   * Resolves a Source with HTTP configuration for POST, headers, etc.
   */
  private Source resolveSourceWithHttpConfig(String url, Map<String, Object> tableDef) {
    // Extract HTTP configuration from table definition
    String method = (String) tableDef.getOrDefault("method", "GET");
    String body = (String) tableDef.get("body");
    String mimeType = (String) tableDef.get("mimeType");

    @SuppressWarnings("unchecked")
    Map<String, String> headers = (Map<String, String>) tableDef.get("headers");
    if (headers == null) {
      headers = new HashMap<>();
    }

    // Create HTTP storage provider with configuration
    HttpStorageProvider httpProvider = new HttpStorageProvider(method, body, headers, mimeType);

    // Try to get metadata for the file
    StorageProvider.FileMetadata metadata;
    try {
      metadata = httpProvider.getMetadata(url);
    } catch (Exception e) {
      // If metadata can't be retrieved, use defaults
      metadata =
                                                   new StorageProvider.FileMetadata(url, -1, System.currentTimeMillis(), "application/octet-stream", null);
    }

    // Extract just the filename from the URL for the name field
    String name = url.substring(url.lastIndexOf('/') + 1);
    if (name.contains("?")) {
      name = name.substring(0, name.indexOf('?'));
    }

    // Create a FileEntry for the URL
    StorageProvider.FileEntry fileEntry =
        new StorageProvider.FileEntry(url,           // path
        name,          // name
        false,         // isDirectory
        metadata.getSize(),         // size
        metadata.getLastModified());  // lastModified

    // Create a StorageProviderSource that wraps the HTTP provider
    return new StorageProviderSource(fileEntry, httpProvider);
  }

  /**
   * Resolves a URI string to a Source object, supporting:
   * - file:// - Local file with explicit protocol
   * - s3:// - S3 resources
   * - http:// or https:// - Web resources
   * - ftp:// - FTP resources
   * - / - Absolute local file path
   * - relative/path - Relative path (resolved against baseDirectory if present).
   */
  private Source resolveSource(String uri) {
    // Let Sources.of handle protocol detection
    Source source0 = Sources.of(uri);

    // Apply base directory for relative paths
    if (baseDirectory != null && !isAbsoluteUri(uri)) {
      return Sources.of(baseDirectory).append(source0);
    }

    return source0;
  }

  /**
   * Checks if a URI is absolute (has protocol or starts with /).
   */
  private boolean isAbsoluteUri(String uri) {
    return uri.startsWith("s3://")
        || uri.startsWith("http://")
        || uri.startsWith("https://")
        || uri.startsWith("ftp://")
        || uri.startsWith("file://")
        || uri.startsWith("/")
        || (uri.length() > 2 && uri.charAt(1) == ':'); // Windows absolute path
  }

  private boolean addTable(ImmutableMap.Builder<String, Table> builder,
      Source source, String tableName, @Nullable Map<String, Object> tableDef) {
    LOGGER.debug("FileSchema.addTable: schemaName={}, tableName='{}', source={}, csvTypeInferenceConfig={}",
        name, tableName, source.path(),
        csvTypeInferenceConfig != null ? "enabled=" + csvTypeInferenceConfig.isEnabled() : "null");

    // Check if refresh is configured
    String refreshIntervalStr = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
    Duration refreshInterval = RefreshInterval.parse(refreshIntervalStr);
    boolean hasRefresh = refreshInterval != null;

    String path = source.path().toLowerCase(Locale.ROOT);
    boolean isCSVorJSON = path.endsWith(".csv") || path.endsWith(".csv.gz") ||
                          path.endsWith(".json") || path.endsWith(".json.gz");

    // Handle refreshable CSV/JSON files with Parquet caching
    if (hasRefresh && isCSVorJSON &&
        engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.PARQUET &&
        baseDirectory != null) {

      try {
        File cacheDir =
            ParquetConversionUtil.getParquetCacheDir(baseDirectory, engineConfig.getParquetCacheDirectory());
        File sourceFile = new File(source.path());
        boolean typeInferenceEnabled = csvTypeInferenceConfig != null && csvTypeInferenceConfig.isEnabled();
        File parquetFile = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, typeInferenceEnabled);

        // Check if we need initial conversion/update
        if (!parquetFile.exists() || sourceFile.lastModified() > parquetFile.lastModified()) {
          LOGGER.debug("Creating/updating parquet cache for refreshable table: {}", tableName);

          // Convert/update the cache file using standard conversion method
          Table sourceTable = path.endsWith(".csv") || path.endsWith(".csv.gz")
              ? new CsvTranslatableTable(source, null, columnNameCasing, csvTypeInferenceConfig)
              : createTableFromSource(source, tableDef);

          if (sourceTable != null) {
            parquetFile =
                ParquetConversionUtil.convertToParquet(source, tableName, sourceTable, cacheDir, parentSchema, name);
          } else {
            throw new RuntimeException("Failed to create source table for: " + source.path());
          }
        } else {
          LOGGER.debug("Using existing parquet cache for refreshable table: {}", tableName);
        }

        // Create refreshable table that monitors the source and updates cache
        RefreshableParquetCacheTable refreshableTable =
            new RefreshableParquetCacheTable(source, parquetFile, cacheDir, refreshInterval, typeInferenceEnabled,
            columnNameCasing, null, engineConfig.getEngineType(), parentSchema, name);

        String finalName = applyCasing(tableName, tableNameCasing);
        builder.put(finalName, refreshableTable);
        return true;

      } catch (Exception e) {
        LOGGER.error("Failed to create refreshable parquet cache table for {}: {}", tableName, e.getMessage(), e);
        // Fall through to regular processing
      }
    }

    // Check if we should convert to Parquet (for PARQUET engine)
    if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.PARQUET
        && baseDirectory != null
        && !source.path().toLowerCase(Locale.ROOT).endsWith(".parquet")) {

      // Check if it's a file type we can convert
      boolean canConvert = path.endsWith(".csv") || path.endsWith(".tsv")
          || path.endsWith(".json") || path.endsWith(".yaml") || path.endsWith(".yml")
          || path.endsWith(".csv.gz") || path.endsWith(".tsv.gz")
          || path.endsWith(".json.gz") || path.endsWith(".yaml.gz")
          || path.endsWith(".yml.gz");

      if (canConvert) {
        // Skip Parquet conversion for JSON/YAML files with flatten option
        // because flattening happens at scan time, not schema time
        boolean skipConversion = false;
        if (tableDef != null && Boolean.TRUE.equals(tableDef.get("flatten"))
            && (path.endsWith(".json") || path.endsWith(".yaml") || path.endsWith(".yml")
                || path.endsWith(".json.gz") || path.endsWith(".yaml.gz") || path.endsWith(".yml.gz"))) {
          skipConversion = true;
        }

        if (!skipConversion) {
          try {
            // Create the original table first - use standard table for conversion
            // to avoid convention issues during the conversion process
            Table originalTable = path.endsWith(".csv") || path.endsWith(".csv.gz")
                ? new CsvTranslatableTable(source, null, columnNameCasing, csvTypeInferenceConfig)
                : createTableFromSource(source, tableDef);
            if (originalTable != null) {
            // Get cache directory
            File cacheDir =
                ParquetConversionUtil.getParquetCacheDir(baseDirectory, engineConfig.getParquetCacheDirectory());

            // Convert to Parquet
            File parquetFile =
                ParquetConversionUtil.convertToParquet(source, tableName,
                    originalTable, cacheDir, parentSchema, name);

            // Check if refresh is configured
            String tableRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
            Duration effectiveInterval = RefreshInterval.getEffectiveInterval(tableRefreshInterval, this.refreshInterval);

            // DuckDB uses JDBC adapter, not FileSchema
            if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.DUCKDB) {
              throw new IllegalStateException("DuckDB engine should use JDBC adapter, not FileSchema");
            }
            
            // Create appropriate table based on refresh configuration
            Table parquetTable;
            if (effectiveInterval != null) {
              // Create refreshable table with Parquet cache
              boolean typeInferenceEnabled = path.endsWith(".csv") || path.endsWith(".csv.gz")
                  ? (csvTypeInferenceConfig != null && csvTypeInferenceConfig.isEnabled())
                  : false;

              parquetTable =
                  new RefreshableParquetCacheTable(source, parquetFile, cacheDir, effectiveInterval, typeInferenceEnabled,
                  columnNameCasing, null, engineConfig.getEngineType(), parentSchema, name);
            } else {
              // Use standard Parquet table
              parquetTable = new ParquetTranslatableTable(parquetFile);
            }
            builder.put(
                applyCasing(Util.first(tableName, source.path()),
                tableNameCasing), parquetTable);

            return true;
          }
        } catch (Exception e) {
          LOGGER.error("Failed to convert {} to Parquet: {}", tableName, e.getMessage());
          // Fall through to regular processing - don't return here!
          // We need to register the table even if Parquet conversion fails
        }
        }
      }
    }

    final Source sourceSansGz = source.trim(".gz");

    // Check for custom format override
    String forcedFormat = null;
    if (tableDef != null) {
      forcedFormat = (String) tableDef.get("format");
    }

    // If format is forced, use it directly
    if (forcedFormat != null) {
      switch (forcedFormat.toLowerCase(Locale.ROOT)) {
      case "json":
        // Check if JSONPath extraction is configured
        if (tableDef != null && tableDef.containsKey("jsonSearchPaths")) {
          LOGGER.info("Using JSONPath extraction for source: {}", source.path());
          
          // Get refresh interval if configured
          String jsonRefreshInterval = (String) tableDef.get("refreshInterval");
          Duration effectiveInterval = RefreshInterval.getEffectiveInterval(jsonRefreshInterval, this.refreshInterval);
          
          // JSON table extraction would go here when JsonMultiTableFactory is available
          // For now, skip JSONPath extraction as dependencies are not available
          LOGGER.debug("Skipping JSONPath extraction - dependencies not available");
          // Continue to single table creation below
        }
        
        // Single table creation (existing code)
        String jsonRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
        Map<String, Object> options = null;
        if (tableDef != null && tableDef.containsKey("flatten")) {
          options = new HashMap<>();
          options.put("flatten", tableDef.get("flatten"));
          if (tableDef.containsKey("flattenSeparator")) {
            options.put("flattenSeparator", tableDef.get("flattenSeparator"));
          }
        }
        final Table jsonTable =
            createEnhancedJsonTable(source, tableName, jsonRefreshInterval, options);
        String finalTableName = applyCasing(Util.first(tableName, source.path()), tableNameCasing);
        LOGGER.debug("Created JSON table of type: {} for table '{}' -> registered as '{}'", jsonTable.getClass().getName(), tableName, finalTableName);
        builder.put(finalTableName, jsonTable);
        return true;
      case "csv":
        String csvRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
        final Table csvTable = createEnhancedCsvTable(source, null, tableName, csvRefreshInterval);
        builder.put(applyCasing(Util.first(tableName, source.path()), tableNameCasing), csvTable);
        return true;
      case "tsv":
        String tsvRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
        final Table tsvTable = createEnhancedCsvTable(source, null, tableName, tsvRefreshInterval);
        builder.put(applyCasing(Util.first(tableName, source.path()), tableNameCasing), tsvTable);
        return true;
      case "yaml":
      case "yml":
        String yamlRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
        options = null;
        if (tableDef != null && tableDef.containsKey("flatten")) {
          options = new HashMap<>();
          options.put("flatten", tableDef.get("flatten"));
          if (tableDef.containsKey("flattenSeparator")) {
            options.put("flattenSeparator", tableDef.get("flattenSeparator"));
          }
        }
        final Table yamlTable =
            createEnhancedJsonTable(source, tableName, yamlRefreshInterval, options);
        builder.put(applyCasing(Util.first(tableName, source.path()), tableNameCasing), yamlTable);
        return true;
      case "html":
      case "htm":
        // For backward compatibility, allow local HTML files without fragments
        // This supports existing tests and configurations
        final Table htmlTable = FileTable.create(source, tableDef);
        builder.put(applyCasing(Util.first(tableName, source.path()), tableNameCasing), htmlTable);
        return true;
      case "parquet":
        // DuckDB uses JDBC adapter, not FileSchema
        if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.DUCKDB) {
          throw new IllegalStateException("DuckDB engine should use JDBC adapter, not FileSchema");
        }
        // Use standard Parquet table
        final Table parquetTable = new ParquetTranslatableTable(new java.io.File(source.path()));
        builder.put(
            applyCasing(Util.first(tableName, source.path()),
            tableNameCasing), parquetTable);
        return true;
      case "excel":
      case "xlsx":
      case "xls":
        // Excel files in explicit table mappings are problematic because:
        // 1. Excel files naturally contain multiple tables (sheets)
        // 2. Mapping one table name to entire Excel file loses sheet information
        // 3. Better to use directory discovery mode for Excel files
        throw new RuntimeException("Excel files cannot be used in explicit table definitions "
            + "because they contain multiple sheets. Each sheet becomes a separate table. "
            + "Place the Excel file in your schema directory and it will be automatically "
            + "processed, with each sheet accessible as 'filename__sheetname'.");
      case "md":
      case "markdown":
        // Markdown files in explicit table mappings are problematic because:
        // 1. Markdown files can contain multiple tables
        // 2. Mapping one table name to entire Markdown file loses table information
        // 3. Better to use directory discovery mode for Markdown files
        throw new RuntimeException("Markdown files cannot be used in explicit table definitions "
            + "because they can contain multiple tables. "
            + "Place the Markdown file in your schema directory and it will be automatically "
            + "processed, with each table accessible as 'filename__tablename'.");
      case "docx":
        // DOCX files in explicit table mappings are problematic because:
        // 1. DOCX files can contain multiple tables
        // 2. Mapping one table name to entire DOCX file loses table information
        // 3. Better to use directory discovery mode for DOCX files
        throw new RuntimeException("DOCX files cannot be used in explicit table definitions "
            + "because they can contain multiple tables. "
            + "Place the DOCX file in your schema directory and it will be automatically "
            + "processed, with each table accessible as 'filename__tablename'.");
      case "iceberg":
        // Create Iceberg table
        Map<String, Object> icebergConfig = tableDef != null
            ? new HashMap<>(tableDef)
            : new HashMap<>();
        // Add storage config if available
        if (this.storageConfig != null) {
          icebergConfig.putAll(this.storageConfig);
        }
        final Table icebergTable = createIcebergTable(source, tableName, icebergConfig);
        String icebergTableName = applyCasing(Util.first(tableName, source.path()), tableNameCasing);
        builder.put(icebergTableName, icebergTable);

        // Add metadata tables if this is an Iceberg table
        addIcebergMetadataTables(builder, icebergTableName, icebergTable);
        return true;
      default:
        throw new RuntimeException("Unsupported format override: " + forcedFormat
            + " for table: " + tableName);
      }
    }

    // Default extension-based detection
    Source sourceSansJson = sourceSansGz.trimOrNull(".json");
    if (sourceSansJson == null) {
      sourceSansJson = sourceSansGz.trimOrNull(".yaml");
    }
    if (sourceSansJson == null) {
      sourceSansJson = sourceSansGz.trimOrNull(".yml");
    }
    if (sourceSansJson != null) {
      String jsonAutoRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
      Map<String, Object> options = null;
      // Check table-level flatten first, then schema-level flatten
      if (tableDef != null && tableDef.containsKey("flatten")) {
        options = new HashMap<>();
        options.put("flatten", tableDef.get("flatten"));
        if (tableDef.containsKey("flattenSeparator")) {
          options.put("flattenSeparator", tableDef.get("flattenSeparator"));
        }
      } else if (this.flatten != null && this.flatten) {
        // Use schema-level flatten option for auto-discovered JSON/YAML files
        options = new HashMap<>();
        options.put("flatten", this.flatten);
      }
      final Table table =
          createEnhancedJsonTable(source, tableName, jsonAutoRefreshInterval, options);
      builder.put(
          applyCasing(Util.first(tableName, sourceSansJson.path()),
          tableNameCasing), table);
      return true;
    }
    final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
    if (sourceSansCsv != null) {
      String csvAutoRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
      final Table table = createEnhancedCsvTable(source, null, tableName, csvAutoRefreshInterval);
      String tableNameKey = applyCasing(Util.first(tableName, sourceSansCsv.path()), tableNameCasing);
      builder.put(tableNameKey, table);
      return true;
    }
    final Source sourceSansTsv = sourceSansGz.trimOrNull(".tsv");
    if (sourceSansTsv != null) {
      String tsvAutoRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
      final Table table = createEnhancedCsvTable(source, null, tableName, tsvAutoRefreshInterval);
      builder.put(applyCasing(Util.first(tableName, sourceSansTsv.path()), tableNameCasing), table);
      return true;
    }
    final Source sourceSansArrow = sourceSansGz.trimOrNull(".arrow");
    if (sourceSansArrow != null) {
      try {
        final Table table = createArrowTable(new java.io.File(source.path()));
        builder.put(
            applyCasing(Util.first(tableName, sourceSansArrow.path()),
            tableNameCasing), table);
        return true;
      } catch (Exception e) {
        // Fall back to generic FileTable if Arrow creation fails
        LOGGER.warn("Warning: Failed to create Arrow table for {}: {}", source.path(), e.getMessage());
      }
    }
    final Source sourceSansParquet = sourceSansGz.trimOrNull(".parquet");
    if (sourceSansParquet != null) {
      try {
        // DuckDB uses JDBC adapter, not FileSchema
        if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.DUCKDB) {
          throw new IllegalStateException("DuckDB engine should use JDBC adapter, not FileSchema");
        }
        // Use our custom ParquetTranslatableTable instead of arrow's ParquetTable
        final Table table = new ParquetTranslatableTable(new java.io.File(source.path()));
        builder.put(
            applyCasing(Util.first(tableName, sourceSansParquet.path()),
            tableNameCasing), table);
        return true;
      } catch (Exception e) {
        // Fall back to generic FileTable if Parquet creation fails
        LOGGER.warn("Warning: Failed to create Parquet table for {}: {}", source.path(), e.getMessage());
      }
    }
    Source sourceSansXlsx = sourceSansGz.trimOrNull(".xlsx");
    if (sourceSansXlsx == null) {
      sourceSansXlsx = sourceSansGz.trimOrNull(".xls");
    }
    if (sourceSansXlsx != null) {
      // Only throw error if this is from an explicit table definition
      if (tableDef != null) {
        throw new RuntimeException("Excel files should not be used in explicit table mappings. "
            + "Excel files contain multiple sheets (tables). "
            + "Use directory discovery mode or direct file URL instead.");
      }
      // For directory/file discovery mode, convert Excel to JSON
      try {
        java.io.File file = new java.io.File(source.path());
        // Always extract all sheets from Excel files
        SafeExcelToJsonConverter.convertIfNeeded(file, true);
        // Return false to let directory scanning pick up the converted JSON files
        return false;
      } catch (Exception e) {
        LOGGER.warn("Warning: Failed to convert Excel file {}: {}", source.path(), e.getMessage());
      }
    }

    // Check for HTML files
    Source sourceSansHtml = sourceSansGz.trimOrNull(".html");
    if (sourceSansHtml == null) {
      sourceSansHtml = sourceSansGz.trimOrNull(".htm");
    }
    if (sourceSansHtml != null) {
      // For directory discovery mode, skip individual processing
      // These files are handled in getTableMap() where all tables are extracted
      if (tableDef == null) {
        return false;
      }

      // For explicit table definitions or single-table mode
      if (tableDef != null) {
        // For backward compatibility, allow local HTML files without fragments
        // This supports existing tests and configurations
        try {
          FileTable table = FileTable.create(source, tableDef);
          builder.put(applyCasing(Util.first(tableName, source.path()), tableNameCasing), table);
          return true;
        } catch (Exception e) {
          throw new RuntimeException("Unable to instantiate HTML table for: "
              + tableName + ". Error: " + e.getMessage());
        }
      }
    }

    if (tableDef != null) {
      // If file type not recognized by extension, try the generic FileTable approach
      try {
        FileTable table = FileTable.create(source, tableDef);
        builder.put(applyCasing(Util.first(tableName, source.path()), tableNameCasing), table);
        return true;
      } catch (Exception e) {
        throw new RuntimeException("Unable to instantiate table for: "
            + tableName + ". Error: " + e.getMessage());
      }
    }

    return false;
  }

  /**
   * Creates an Arrow table from the given file.
   * This method attempts to use the Arrow adapter if available.
   */
  private static org.apache.calcite.schema.Table createArrowTable(java.io.File file) {
    try {
      // Try to use reflection to create Arrow table to avoid hard dependency
      Class<?> arrowTableClass = Class.forName("org.apache.calcite.adapter.arrow.ArrowTable");
      Class<?> arrowFileReaderClass = Class.forName("org.apache.arrow.vector.ipc.ArrowFileReader");
      Class<?> seekableReadChannelClass =
          Class.forName("org.apache.arrow.vector.ipc.SeekableReadChannel");
      Class<?> bufferAllocatorClass = Class.forName("org.apache.arrow.memory.BufferAllocator");
      Class<?> rootAllocatorClass = Class.forName("org.apache.arrow.memory.RootAllocator");

      java.io.FileInputStream fileInputStream = new java.io.FileInputStream(file);
      Object seekableReadChannel = seekableReadChannelClass
          .getConstructor(java.nio.channels.SeekableByteChannel.class)
          .newInstance(fileInputStream.getChannel());
      Object allocator = rootAllocatorClass.getConstructor().newInstance();
      Object arrowFileReader = arrowFileReaderClass
          .getConstructor(seekableReadChannelClass, bufferAllocatorClass)
          .newInstance(seekableReadChannel, allocator);

      // Try the constructor with file parameter first, fall back to the old one if not available
      try {
        return (org.apache.calcite.schema.Table) arrowTableClass
            .getConstructor(org.apache.calcite.rel.type.RelProtoDataType.class,
                arrowFileReaderClass, java.io.File.class)
            .newInstance(null, arrowFileReader, file);
      } catch (NoSuchMethodException e) {
        // Fall back to old constructor for compatibility
        return (org.apache.calcite.schema.Table) arrowTableClass
            .getConstructor(org.apache.calcite.rel.type.RelProtoDataType.class, arrowFileReaderClass)
            .newInstance(null, arrowFileReader);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Arrow table: " + e.getMessage(), e);
    }
  }


  /**
   * Creates a table from a source file based on its type.
   * This is used for Parquet conversion.
   */
  private Table createTableFromSource(Source source, @Nullable Map<String, Object> tableDef) {
    final Source sourceSansGz = source.trim(".gz");

    // Check for custom format override
    String forcedFormat = null;
    if (tableDef != null) {
      forcedFormat = (String) tableDef.get("format");
    }

    // If format is forced, use it directly
    if (forcedFormat != null) {
      switch (forcedFormat.toLowerCase(Locale.ROOT)) {
      case "json":
        // Use simple table for conversion to avoid Arrow issues
        Map<String, Object> options = null;
        if (tableDef != null && tableDef.containsKey("flatten")) {
          options = new HashMap<>();
          options.put("flatten", tableDef.get("flatten"));
          if (tableDef.containsKey("flattenSeparator")) {
            options.put("flattenSeparator", tableDef.get("flattenSeparator"));
          }
        }
        return new JsonScannableTable(source, options, columnNameCasing);
      case "csv":
        // Use simple table for conversion to avoid Arrow issues
        return new CsvTranslatableTable(source, null, columnNameCasing, csvTypeInferenceConfig);
      case "tsv":
        // Use simple table for conversion to avoid Arrow issues
        return new CsvTranslatableTable(source, null, columnNameCasing, csvTypeInferenceConfig);
      case "yaml":
      case "yml":
        // Use simple table for conversion to avoid Arrow issues
        options = null;
        if (tableDef != null && tableDef.containsKey("flatten")) {
          options = new HashMap<>();
          options.put("flatten", tableDef.get("flatten"));
          if (tableDef.containsKey("flattenSeparator")) {
            options.put("flattenSeparator", tableDef.get("flattenSeparator"));
          }
        }
        return new JsonScannableTable(source, options, columnNameCasing);
      default:
        return null;
      }
    }

    // Default extension-based detection
    Source sourceSansJson = sourceSansGz.trimOrNull(".json");
    if (sourceSansJson == null) {
      sourceSansJson = sourceSansGz.trimOrNull(".yaml");
    }
    if (sourceSansJson == null) {
      sourceSansJson = sourceSansGz.trimOrNull(".yml");
    }
    if (sourceSansJson != null) {
      // Use simple table for conversion to avoid Arrow issues
      // Apply schema-level flatten option if no table definition is provided
      Map<String, Object> options = null;
      if (tableDef == null && this.flatten != null && this.flatten) {
        options = new HashMap<>();
        options.put("flatten", this.flatten);
      }
      return new JsonScannableTable(source, options, columnNameCasing);
    }

    final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
    if (sourceSansCsv != null) {
      // Use simple table for conversion to avoid Arrow issues
      return new CsvTranslatableTable(source, null, columnNameCasing, csvTypeInferenceConfig);
    }

    final Source sourceSansTsv = sourceSansGz.trimOrNull(".tsv");
    if (sourceSansTsv != null) {
      // Use simple table for conversion to avoid Arrow issues
      return new CsvTranslatableTable(source, null, columnNameCasing, csvTypeInferenceConfig);
    }

    return null;
  }

  /**
   * Creates an enhanced CSV table with execution engine support.
   */
  private org.apache.calcite.schema.Table createEnhancedCsvTable(
      Source source,
      org.apache.calcite.rel.type.@Nullable RelProtoDataType protoRowType) {
    return createEnhancedCsvTable(source, protoRowType, null, null);
  }

  /**
   * Creates an enhanced CSV table with execution engine and refresh support.
   */
  private org.apache.calcite.schema.Table createEnhancedCsvTable(
      Source source,
      org.apache.calcite.rel.type.@Nullable RelProtoDataType protoRowType,
      String tableName, @Nullable String tableRefreshInterval) {
    // Get effective refresh interval (table level or schema default)
    Duration effectiveInterval =
        RefreshInterval.getEffectiveInterval(tableRefreshInterval,
            this.refreshInterval);

    // Handle refresh based on execution engine
    switch (engineConfig.getEngineType()) {
    case VECTORIZED:
      if (effectiveInterval != null && tableName != null) {
        return new RefreshableCsvTable(source, tableName, protoRowType, effectiveInterval);
      }
      return new EnhancedCsvTranslatableTable(source, protoRowType, engineConfig, columnNameCasing);
    case PARQUET:
      if (effectiveInterval != null && tableName != null) {
        try {
          // For Parquet engine with refresh, use RefreshableParquetCacheTable
          File cacheDir =
              ParquetConversionUtil.getParquetCacheDir(baseDirectory, engineConfig.getParquetCacheDirectory());
          File parquetFile =
              ParquetConversionUtil.convertToParquet(source, tableName, new CsvTranslatableTable(source, protoRowType, columnNameCasing, csvTypeInferenceConfig),
              cacheDir, parentSchema, name);
          boolean typeInferenceEnabled = csvTypeInferenceConfig != null && csvTypeInferenceConfig.isEnabled();
          // Use standard RefreshableParquetCacheTable for PARQUET engine
          return new RefreshableParquetCacheTable(source, parquetFile, cacheDir,
              effectiveInterval, typeInferenceEnabled, columnNameCasing, protoRowType,
              engineConfig.getEngineType(), parentSchema, name);
        } catch (Exception e) {
          throw new RuntimeException("Failed to create refreshable Parquet table for " + source.path(), e);
        }
      }
      // For PARQUET engine, return a regular CSV table that will be converted to Parquet
      // The conversion happens in addTable() method
      return new CsvTranslatableTable(source, protoRowType, columnNameCasing, csvTypeInferenceConfig);
    case DUCKDB:
      // DuckDB uses JDBC adapter, not FileSchema
      throw new IllegalStateException("DuckDB engine should use JDBC adapter, not FileSchema");
    case ARROW:
    case LINQ4J:
    default:
      if (effectiveInterval != null && tableName != null) {
        return new RefreshableCsvTable(source, tableName, protoRowType, effectiveInterval);
      }
      return new CsvTranslatableTable(source, protoRowType, columnNameCasing, csvTypeInferenceConfig);
    }
  }

  /**
   * Creates an enhanced JSON table with execution engine support.
   */
  private org.apache.calcite.schema.Table createEnhancedJsonTable(
      Source source) {
    return createEnhancedJsonTable(source, null, null, null);
  }

  /**
   * Creates an enhanced JSON table with execution engine and refresh support.
   */
  private org.apache.calcite.schema.Table createEnhancedJsonTable(
      Source source,
      String tableName, @Nullable String tableRefreshInterval) {
    return createEnhancedJsonTable(source, tableName, tableRefreshInterval, null);
  }

  /**
   * Creates an enhanced JSON table with execution engine, refresh support, and options.
   */
  private org.apache.calcite.schema.Table createEnhancedJsonTable(
      Source source,
      String tableName, @Nullable String tableRefreshInterval, Map<String, Object> options) {
    // Get effective refresh interval (table level or schema default)
    Duration effectiveInterval =
        RefreshInterval.getEffectiveInterval(tableRefreshInterval,
            this.refreshInterval);

    // Create refreshable or standard tables based on engine type
    switch (engineConfig.getEngineType()) {
    case VECTORIZED:
      if (effectiveInterval != null && tableName != null) {
        // TODO: Create RefreshableEnhancedJsonTable when needed
        return new EnhancedJsonScannableTable(source, engineConfig, options, columnNameCasing);
      }
      return new EnhancedJsonScannableTable(source, engineConfig, options, columnNameCasing);
    case PARQUET:
      if (effectiveInterval != null && tableName != null) {
        try {
          // For Parquet engine with refresh, we need to use RefreshableParquetCacheTable
          // First convert JSON to Parquet
          File cacheDir =
              ParquetConversionUtil.getParquetCacheDir(baseDirectory, engineConfig.getParquetCacheDirectory());
          File parquetFile =
              ParquetConversionUtil.convertToParquet(source, tableName, new JsonScannableTable(source, options, columnNameCasing), cacheDir, parentSchema, name);
          
          // Check if this JSON file was converted from another source (HTML, Excel, XML)
          Source originalSource = null;
          try {
            File jsonFile = source.file();
            if (jsonFile != null) {
              org.apache.calcite.adapter.file.metadata.ConversionMetadata metadata = 
                  new org.apache.calcite.adapter.file.metadata.ConversionMetadata(jsonFile.getParentFile());
              File origFile = metadata.findOriginalSource(jsonFile);
              if (origFile != null && origFile.exists()) {
                originalSource = Sources.of(origFile);
                LOGGER.debug("Found original source {} for JSON file {}", 
                    origFile.getName(), jsonFile.getName());
              }
            }
          } catch (Exception e) {
            LOGGER.debug("Could not find original source for {}: {}", source.path(), e.getMessage());
          }
          
          // Use RefreshableParquetCacheTable with original source if found
          return new RefreshableParquetCacheTable(source, originalSource, parquetFile, cacheDir,
              effectiveInterval, false, columnNameCasing, null,
              engineConfig.getEngineType(), parentSchema, name);
        } catch (Exception e) {
          throw new RuntimeException("Failed to create refreshable Parquet table for " + source.path(), e);
        }
      }
      return new ParquetJsonScannableTable(source, engineConfig, options, columnNameCasing);
    case DUCKDB:
      // DuckDB uses JDBC adapter, not FileSchema
      throw new IllegalStateException("DuckDB engine should use JDBC adapter, not FileSchema");
    case ARROW:
    case LINQ4J:
    default:
      if (effectiveInterval != null && tableName != null) {
        return new RefreshableJsonTable(source, tableName, effectiveInterval, columnNameCasing);
      }
      return new JsonScannableTable(source, options, columnNameCasing);
    }
  }

  /**
   * Process partitioned table configurations.
   */
  private void processPartitionedTables(ImmutableMap.Builder<String, Table> builder) {
    if (partitionedTables == null || baseDirectory == null) {
      return;
    }

    for (Map<String, Object> partTableConfig : partitionedTables) {
      try {
        PartitionedTableConfig config = PartitionedTableConfig.fromMap(partTableConfig);

        // Find all files matching the pattern
        List<String> matchingFiles = findMatchingFiles(config.getPattern());

        if (matchingFiles.isEmpty()) {
          LOGGER.debug("No files found matching pattern: {}", config.getPattern());
          continue;
        }

        LOGGER.debug("Found {} files for partitioned table: {}", matchingFiles.size(), config.getName());

        // Detect or apply partition scheme
        PartitionDetector.PartitionInfo partitionInfo = null;

        if (config.getPartitions() == null
            || "auto".equals(config.getPartitions().getStyle())) {
          // Auto-detect partition scheme
          partitionInfo =
              PartitionDetector.detectPartitionScheme(matchingFiles);
          if (partitionInfo == null) {
            LOGGER.warn("WARN: No partition scheme detected for table '{}'. Treating as unpartitioned. Query performance may be impacted.", config.getName());
          }
        } else {
          // Use configured partition scheme
          PartitionedTableConfig.PartitionConfig partConfig = config.getPartitions();
          String style = partConfig.getStyle();

          if ("hive".equals(style)) {
            // Force Hive-style interpretation
            partitionInfo =
                PartitionDetector.extractHivePartitions(matchingFiles.get(0));
          } else if ("directory".equals(style)
              && partConfig.getColumns() != null) {
            // Directory-based partitioning
            partitionInfo =
                PartitionDetector.extractDirectoryPartitions(
                    matchingFiles.get(0), partConfig.getColumns());
          } else if ("custom".equals(style)
              && partConfig.getRegex() != null) {
            // Custom regex partitioning
            partitionInfo =
                PartitionDetector.extractCustomPartitions(
                    matchingFiles.get(0), partConfig.getRegex(),
                    partConfig.getColumnMappings());
          }
        }

        // Create map of column types if defined
        Map<String, String> columnTypes = null;
        if (config.getPartitions() != null
            && config.getPartitions().getColumnDefinitions() != null) {
          columnTypes = new HashMap<>();
          for (PartitionedTableConfig.ColumnDefinition colDef
               : config.getPartitions().getColumnDefinitions()) {
            columnTypes.put(colDef.getName(), colDef.getType());
          }
        }

        // Create the partitioned table - use refreshable version if interval configured
        Table table;
        if (this.refreshInterval != null) {
          // Use refreshable version that can discover new partitions
          table =
              new RefreshablePartitionedParquetTable(config.getName(),
                  baseDirectory, config.getPattern(), config,
                  engineConfig, RefreshInterval.parse(this.refreshInterval));
        } else {
          // Use standard version
          table =
              new PartitionedParquetTable(matchingFiles, partitionInfo,
                  engineConfig, columnTypes);
        }
        builder.put(applyCasing(config.getName(), tableNameCasing), table);

      } catch (Exception e) {
        LOGGER.error("Failed to process partitioned table: {}", e.getMessage());
        e.printStackTrace();
      }
    }
  }

  /**
   * Find all files matching a glob pattern relative to baseDirectory.
   */
  private List<String> findMatchingFiles(String pattern) {
    List<String> result = new ArrayList<>();

    if (baseDirectory == null || pattern == null) {
      return result;
    }

    try {
      Path basePath = baseDirectory.toPath();
      PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);

      Files.walkFileTree(basePath, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
        @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
          Path relativePath = basePath.relativize(file);
          if (matcher.matches(relativePath)) {
            result.add(file.toString());
          }
          return FileVisitResult.CONTINUE;
        }
      });

    } catch (Exception e) {
      LOGGER.error("Error scanning for files with pattern: {}", pattern);
      e.printStackTrace();
    }

    return result;
  }

  /**
   * Gets files for processing, using storage provider if configured,
   * otherwise falls back to local file system.
   */
  private File[] getFilesForProcessing() {
    // If storage provider is configured, use it for file discovery
    if (storageProvider != null) {
      return getFilesFromStorageProvider();
    }

    // Otherwise use local file system
    if (baseDirectory == null) {
      return new File[0];
    }

    // Determine the glob pattern to use
    String pattern;

    if (directoryPattern != null && isGlobPattern(directoryPattern)) {
      // Directory pattern itself contains glob characters - use as-is
      pattern = directoryPattern;
    } else if (recursive) {
      // Use recursive glob pattern that includes both root and subdirectory files
      pattern = "**";
    } else {
      // Use non-recursive glob pattern for plain directories
      pattern = "*";
    }

    // Always use glob-based file discovery for consistency
    List<String> matchingFiles = findMatchingFiles(pattern);
    File[] sortedFiles = matchingFiles.stream()
        .map(File::new)
        .filter(this::isFileTypeSupported)
        .sorted((f1, f2) -> {
          // Sort files to ensure CSV files are processed after HTML files
          // This ensures CSV tables take precedence over HTML tables when they have the same name
          String ext1 = getFileExtension(f1.getName());
          String ext2 = getFileExtension(f2.getName());

          // CSV/TSV files should be processed last (higher priority)
          boolean isCsv1 = "csv".equals(ext1) || "tsv".equals(ext1);
          boolean isCsv2 = "csv".equals(ext2) || "tsv".equals(ext2);

          if (isCsv1 && !isCsv2) return 1;  // CSV after non-CSV
          if (!isCsv1 && isCsv2) return -1; // non-CSV before CSV

          // For same priority types, sort alphabetically
          return f1.getName().compareTo(f2.getName());
        })
        .toArray(File[]::new);


    return sortedFiles;
  }

  /**
   * Gets the file extension without .gz suffix.
   */
  private String getFileExtension(String fileName) {
    String nameSansGz = trim(fileName, ".gz");
    int lastDot = nameSansGz.lastIndexOf('.');
    return lastDot > 0 ? nameSansGz.substring(lastDot + 1) : "";
  }

  /**
   * Checks if a file type is supported based on its extension.
   */
  private boolean isFileTypeSupported(File file) {
    if (!file.isFile()) {
      return false;
    }
    final String nameSansGz = trim(file.getName(), ".gz");
    return nameSansGz.endsWith(".csv")
        || nameSansGz.endsWith(".tsv")
        || nameSansGz.endsWith(".json")
        || nameSansGz.endsWith(".yaml")
        || nameSansGz.endsWith(".yml")
        || nameSansGz.endsWith(".html")
        || nameSansGz.endsWith(".arrow")
        || nameSansGz.endsWith(".parquet");
  }

  /**
   * Gets files from the configured storage provider.
   * Traverses the directory structure recursively if needed.
   */
  private File[] getFilesFromStorageProvider() {
    if (storageProvider == null) {
      return new File[0];
    }

    // Determine the base path to start from
    String basePath = "/";
    if (baseDirectory != null) {
      basePath = baseDirectory.getPath();
      // Don't normalize S3 or other cloud storage URIs
      if (!basePath.startsWith("s3://") && !basePath.startsWith("gs://")
          && !basePath.startsWith("azure://") && !basePath.startsWith("http")) {
        // Only normalize local paths
        if (!basePath.startsWith("/")) {
          basePath = "/" + basePath;
        }
      }
    }

    List<File> files = new ArrayList<>();

    try {
      // Get files from storage provider recursively
      LOGGER.debug("[FileSchema] Using storage provider to list files from: {}", basePath);
      List<StorageProvider.FileEntry> entries = listFilesRecursively(basePath, recursive);
      LOGGER.debug("[FileSchema] Storage provider found {} entries", entries.size());

      // Convert FileEntry objects to temporary File objects for compatibility
      // Note: These File objects won't actually exist on disk, but we create them
      // for compatibility with existing code that expects File objects
      for (StorageProvider.FileEntry entry : entries) {
        if (!entry.isDirectory() && isFileNameSupported(entry.getName())) {
          // Create a virtual File object that represents the remote file
          // The actual content will be fetched through the storage provider when needed
          files.add(new StorageProviderFile(entry, storageProvider));
        }
      }

      LOGGER.debug("[FileSchema] Found {} supported files via storage provider", files.size());

    } catch (Exception e) {
      LOGGER.error("Error listing files from storage provider: {}", e.getMessage());
      e.printStackTrace();
    }

    // Sort files for consistent processing order
    File[] sortedFiles = files.stream()
        .sorted((f1, f2) -> {
          // Sort files to ensure CSV files are processed after HTML files
          String ext1 = getFileExtension(f1.getName());
          String ext2 = getFileExtension(f2.getName());

          // CSV/TSV files should be processed last (higher priority)
          boolean isCsv1 = "csv".equals(ext1) || "tsv".equals(ext1);
          boolean isCsv2 = "csv".equals(ext2) || "tsv".equals(ext2);

          if (isCsv1 && !isCsv2) return 1;  // CSV after non-CSV
          if (!isCsv1 && isCsv2) return -1; // non-CSV before CSV

          // For same priority types, sort alphabetically
          return f1.getName().compareTo(f2.getName());
        })
        .toArray(File[]::new);

    return sortedFiles;
  }

  /**
   * Recursively lists files from storage provider.
   */
  private List<StorageProvider.FileEntry> listFilesRecursively(String path, boolean recursive) throws IOException {
    List<StorageProvider.FileEntry> allFiles = new ArrayList<>();

    // List files at current path
    List<StorageProvider.FileEntry> entries = storageProvider.listFiles(path, false);

    for (StorageProvider.FileEntry entry : entries) {
      if (entry.isDirectory() && recursive) {
        // Recursively list files in subdirectory
        try {
          List<StorageProvider.FileEntry> subEntries = listFilesRecursively(entry.getPath(), true);
          allFiles.addAll(subEntries);
        } catch (Exception e) {
          LOGGER.error("Error listing files in directory {}: {}", entry.getPath(), e.getMessage());
        }
      } else if (!entry.isDirectory()) {
        // Add file to list
        allFiles.add(entry);
      }
    }

    return allFiles;
  }

  /**
   * Checks if a file name is supported based on its extension.
   */
  private boolean isFileNameSupported(String fileName) {
    final String nameSansGz = trim(fileName, ".gz");
    return nameSansGz.endsWith(".csv")
        || nameSansGz.endsWith(".tsv")
        || nameSansGz.endsWith(".json")
        || nameSansGz.endsWith(".yaml")
        || nameSansGz.endsWith(".yml")
        || nameSansGz.endsWith(".html")
        || nameSansGz.endsWith(".arrow")
        || nameSansGz.endsWith(".parquet");
  }

  /**
   * Checks if a JSON Source is a Calcite model file (should not be treated as data).
   */
  private boolean isCalciteModelFile(Source source) {
    if (!source.path().toLowerCase().endsWith(".json") &&
        !source.path().toLowerCase().endsWith(".json.gz")) {
      return false;
    }

    try {
      com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
      com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(source.reader());

      // Check for typical Calcite model structure
      return root.has("version") && root.has("schemas");
    } catch (Exception e) {
      // If we can't read it, assume it's not a model file
      return false;
    }
  }

  /**
   * Process files from the configured storage provider.
   */
  private void processStorageProviderFiles(ImmutableMap.Builder<String, Table> builder,
                                           Map<String, Integer> tableNameCounts) {
    if (storageProvider == null) {
      return;
    }

    // Determine the base path to start from
    String basePath = "/";
    if (baseDirectory != null) {
      basePath = baseDirectory.getPath();
      // Don't normalize S3 or other cloud storage URIs
      if (!basePath.startsWith("s3://") && !basePath.startsWith("gs://")
          && !basePath.startsWith("azure://") && !basePath.startsWith("http")) {
        // Only normalize local paths
        if (!basePath.startsWith("/")) {
          basePath = "/" + basePath;
        }
      }
    }

    try {
      LOGGER.debug("[FileSchema] Processing files from storage provider at: {}", basePath);
      List<StorageProvider.FileEntry> entries = listFilesRecursively(basePath, recursive);
      LOGGER.debug("[FileSchema] Found {} entries from storage provider", entries.size());

      // Create a virtual base source for relative path calculations
      final Source baseSource =
          new StorageProviderSource(new StorageProvider.FileEntry(basePath, "", true, 0, 0), storageProvider);

      // Process each file
      for (StorageProvider.FileEntry entry : entries) {
        if (!entry.isDirectory() && isFileNameSupported(entry.getName())) {
          // Create source for this file
          Source source = new StorageProviderSource(entry, storageProvider);
          Source sourceSansGz = source.trim(".gz");

          // Check for JSON/YAML files
          Source sourceSansJson = sourceSansGz.trimOrNull(".json");
          if (sourceSansJson == null) {
            sourceSansJson = sourceSansGz.trimOrNull(".yaml");
          }
          if (sourceSansJson == null) {
            sourceSansJson = sourceSansGz.trimOrNull(".yml");
          }
          if (sourceSansJson != null) {
            String baseName =
                applyCasing(
                    WHITESPACE_PATTERN.matcher(sourceSansJson.relative(baseSource).path()
                    .replace("/", "__"))
                    .replaceAll("_"), tableNameCasing);
            // Handle duplicate table names
            String tableName = baseName;
            if (tableNameCounts.containsKey(baseName)) {
              String ext = source.path().endsWith(".yaml")
                  || source.path().endsWith(".yaml.gz") ? "_YAML"
                  : source.path().endsWith(".yml")
                  || source.path().endsWith(".yml.gz") ? "_YML"
                  : "_JSON";
              tableName = baseName + ext;
            }
            tableNameCounts.put(baseName, tableNameCounts.getOrDefault(baseName, 0) + 1);
            // Skip Calcite model files (they contain schema definitions, not data)
            if (isCalciteModelFile(source)) {
              LOGGER.debug("Skipping Calcite model file: {}", source.path());
              continue;
            }

            // Create table with refresh interval if configured
            Map<String, Object> autoTableDef = null;
            if (this.refreshInterval != null) {
              autoTableDef = new HashMap<>();
              autoTableDef.put("refreshInterval", this.refreshInterval);
            }
            addTable(builder, source, tableName, autoTableDef);
            continue;
          }

          // Check for CSV files
          final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
          if (sourceSansCsv != null) {
            String tableName =
                applyCasing(
                    WHITESPACE_PATTERN.matcher(sourceSansCsv.relative(baseSource).path()
                    .replace("/", "__"))
                    .replaceAll("_"), tableNameCasing);
            addTable(builder, source, tableName, null);
            continue;
          }

          // Check for TSV files
          final Source sourceSansTsv = sourceSansGz.trimOrNull(".tsv");
          if (sourceSansTsv != null) {
            String tableName =
                applyCasing(
                    WHITESPACE_PATTERN.matcher(sourceSansTsv.relative(baseSource).path()
                    .replace("/", "__"))
                    .replaceAll("_"), tableNameCasing);
            addTable(builder, source, tableName, null);
            continue;
          }

          // Check for Parquet files
          final Source sourceSansParquet = sourceSansGz.trimOrNull(".parquet");
          if (sourceSansParquet != null) {
            String tableName =
                applyCasing(
                    WHITESPACE_PATTERN.matcher(sourceSansParquet.relative(baseSource).path()
                    .replace("/", "__"))
                    .replaceAll("_"), tableNameCasing);
            // Note: Parquet files from storage providers might need special handling
            // For now, skip them or use FileTable
            try {
              FileTable table = FileTable.create(source, null);
              builder.put(tableName, table);
            } catch (Exception e) {
              LOGGER.error("Failed to add Parquet table from storage provider: {}", e.getMessage());
            }
            continue;
          }

          // Check for Arrow files
          final Source sourceSansArrow = sourceSansGz.trimOrNull(".arrow");
          if (sourceSansArrow != null) {
            String tableName =
                applyCasing(
                    WHITESPACE_PATTERN.matcher(sourceSansArrow.relative(baseSource).path()
                    .replace("/", "__"))
                    .replaceAll("_"), tableNameCasing);
            // Note: Arrow files from storage providers might need special handling
            // For now, skip them or use FileTable
            try {
              FileTable table = FileTable.create(source, null);
              builder.put(tableName, table);
            } catch (Exception e) {
              LOGGER.error("Failed to add Arrow table from storage provider: {}", e.getMessage());
            }
            continue;
          }
        }
      }

      LOGGER.debug("[FileSchema] Processed {} files from storage provider", entries.size());

    } catch (Exception e) {
      LOGGER.error("Error processing files from storage provider: {}", e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Creates an Iceberg table from the given source and configuration.
   */
  private Table createIcebergTable(Source source, String tableName, Map<String, Object> config) {
    try {
      // Create Iceberg table with source
      return new IcebergTable(source, config);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Iceberg table: " + tableName, e);
    }
  }

  /**
   * Adds Iceberg metadata tables for a given Iceberg table.
   */
  private void addIcebergMetadataTables(ImmutableMap.Builder<String, Table> builder,
                                        String baseTableName, Table icebergTable) {
    if (!(icebergTable instanceof IcebergTable)) {
      return;
    }

    IcebergTable table = (IcebergTable) icebergTable;

    // Add metadata tables
    Map<String, Table> metadataTables = IcebergMetadataTables.createMetadataTables(table);
    for (Map.Entry<String, Table> entry : metadataTables.entrySet()) {
      String metadataTableName = baseTableName + "$" + entry.getKey();
      builder.put(metadataTableName, entry.getValue());
    }
  }

}
