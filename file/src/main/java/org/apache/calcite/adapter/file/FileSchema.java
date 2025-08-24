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
import org.apache.calcite.adapter.file.converters.FileConversionManager;
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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Schema mapped onto a set of URLs / HTML tables. Each table in the schema
 * is an HTML table on a URL.
 *
 * <p><strong>Cache directory behavior:</strong>
 * <ul>
 *   <li>Multiple JVM instances can safely share the same cache directory on shared storage
 *       (e.g., NFS, shared filesystem) - file locking ensures proper synchronization</li>
 *   <li>This enables horizontal scaling of query processing across multiple servers</li>
 *   <li>Within a single JVM, using the same schema name with the same cache directory
 *       across multiple FileSchema instances may cause conflicts</li>
 * </ul>
 */
public class FileSchema extends AbstractSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSchema.class);
  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
  
  /** Brand name for cache and metadata directory */
  public static final String BRAND = "aperio";

  /**
   * Set of file extensions that need conversion to JSON for table creation.
   * These include spreadsheets, documents, and markup files.
   */
  private static final Set<String> CONVERTIBLE_EXTENSIONS = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          ".xlsx", ".xls",        // Excel spreadsheets
          ".md", ".markdown",     // Markdown files
          ".docx",                 // Word documents
          ".pptx",                 // PowerPoint presentations
          ".html", ".htm"         // HTML files
      ))
  );

  /**
   * Set of file extensions that are native table source primitives.
   * These files can be directly used as tables without conversion.
   * Note: Compression extensions are handled separately.
   */
  private static final Set<String> TABLE_SOURCE_EXTENSIONS = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          ".csv",                  // Comma-separated values
          ".tsv",                  // Tab-separated values
          ".json",                 // JSON data
          ".yaml", ".yml",        // YAML data
          ".arrow",                // Apache Arrow format
          ".parquet"               // Apache Parquet format
      ))
  );

  /**
   * Set of supported compressed file extensions.
   * Files with these extensions will be automatically decompressed.
   */
  private static final Set<String> COMPRESSED_EXTENSIONS = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          ".gz",                   // Gzip compression
          ".gzip",                 // Alternative gzip extension
          ".bz2",                  // Bzip2 compression
          ".xz",                   // XZ compression
          ".zip"                   // ZIP compression
      ))
  );

  /**
   * Gets the table name to use for registration.
   * If an explicit name is provided, use it as-is.
   * If the name is derived from a file path, apply casing transformations.
   */
  private String getTableName(String explicitName, String derivedName, String casing) {
    if (explicitName != null) {
      // Explicit name - use as-is without casing transformation
      return explicitName;
    } else {
      // Derived name - apply casing transformation
      return applyCasing(derivedName, casing);
    }
  }

  private final ImmutableList<Map<String, Object>> tables;
  private final @Nullable File baseDirectory;
  private final @Nullable File sourceDirectory; // Original directory for reading source files
  private final @Nullable String directoryPattern;
  private final ExecutionEngineConfig engineConfig;
  private final @Nullable ConversionMetadata conversionMetadata;
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
  // Cache directories use schema name for stable, predictable paths

  /**
   * Creates a file schema with all features including storage provider support.
   *
   * @param parentSchema    Parent schema
   * @param name            Schema name
   * @param sourceDirectory Source directory to look for files, or null
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
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File sourceDirectory,
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
    this(parentSchema, name, sourceDirectory, null, directoryPattern, tables, engineConfig,
        recursive, materializations, views, partitionedTables, refreshInterval,
        tableNameCasing, columnNameCasing, storageType, storageConfig, flatten,
        csvTypeInference, primeCache);
  }

  /**
   * Creates a file schema with all features including storage provider support.
   * This constructor accepts both sourceDirectory and userConfiguredBaseDirectory.
   *
   * @param parentSchema    Parent schema
   * @param name            Schema name
   * @param sourceDirectory Source directory to look for files, or null
   * @param userConfiguredBaseDirectory User-configured base directory for .aperio location from model.json, or null
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
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File sourceDirectory,
      @Nullable File userConfiguredBaseDirectory,
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

    // Handle sourceDirectory - convert to fully qualified path
    if (sourceDirectory != null) {
      // If sourceDirectory is provided, ensure it's a fully qualified path
      this.sourceDirectory = sourceDirectory.getAbsoluteFile();
    } else {
      // Fallback to current working directory
      String workingDir = System.getProperty("user.dir");
      LOGGER.warn("sourceDirectory is null after initialization. Falling back to current working directory: {}. " +
          "This is not fatal if not using local storage for file discovery.", workingDir);
      this.sourceDirectory = new File(workingDir).getAbsoluteFile();
    }

    // Determine the root directory for .aperio/<schema>
    File aperioRoot;
    if (userConfiguredBaseDirectory != null) {
      // Use the baseDirectory passed from FileSchemaFactory
      // (could be user-configured or ephemeral)
      aperioRoot = userConfiguredBaseDirectory.getAbsoluteFile();
    } else {
      // Default to current working directory
      String userDir = System.getProperty("user.dir");
      
      // Safety check: if user.dir is root, use temp directory instead
      if ("/".equals(userDir) || userDir == null || userDir.isEmpty()) {
        LOGGER.warn("Working directory is root or invalid ('{}'), falling back to temp directory", userDir);
        userDir = System.getProperty("java.io.tmpdir");
      }
      aperioRoot = new File(userDir).getAbsoluteFile();
    }
    
    // Always use fully qualified path for .aperio/<schema> as the baseDirectory for cache/conversion operations
    this.baseDirectory = new File(aperioRoot, "." + BRAND + "/" + name);
    // Ensure the aperio directory exists
    if (!this.baseDirectory.exists()) {
      this.baseDirectory.mkdirs();
      LOGGER.info("Created {} cache directory: {}", BRAND, this.baseDirectory.getAbsolutePath());
    }
    LOGGER.debug("FileSchema baseDirectory setup: aperioRoot={}, baseDirectory={}", 
        aperioRoot.getAbsolutePath(), this.baseDirectory.getAbsolutePath());
    
    // Initialize conversion metadata for comprehensive table tracking
    this.conversionMetadata = this.baseDirectory != null ? new ConversionMetadata(this.baseDirectory) : null;
    this.directoryPattern = directoryPattern;
    this.engineConfig = engineConfig;
    this.recursive = recursive;
    this.materializations = materializations;
    this.views = views;
    this.partitionedTables = partitionedTables;
    this.refreshInterval = refreshInterval;
    LOGGER.info("FileSchema constructor: refreshInterval set to '{}'", refreshInterval);
    this.parentSchema = parentSchema;
    this.name = name;
    this.tableNameCasing = tableNameCasing != null ? tableNameCasing : "SMART_CASING";
    this.columnNameCasing = columnNameCasing != null ? columnNameCasing : "SMART_CASING";
    this.storageType = storageType;
    this.storageConfig = storageConfig;
    this.flatten = flatten;
    this.csvTypeInferenceConfig = CsvTypeInferrer.TypeInferenceConfig.fromMap(csvTypeInference);
    this.primeCache = primeCache;

    // Schema name is used for cache directory naming to ensure stable, predictable paths
    LOGGER.debug("FileSchema created with name: {}", name);

    // Initialize storage cache manager if parquetCacheDirectory is explicitly configured
    if (engineConfig.getParquetCacheDirectory() != null) {
      StorageCacheManager.initialize(new File(engineConfig.getParquetCacheDirectory()));
      LOGGER.debug("Initialized storage cache manager under: {}/storage_cache", 
          engineConfig.getParquetCacheDirectory());
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
   * @param sourceDirectory Source directory to look for files, or null
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
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File sourceDirectory,
      @Nullable String directoryPattern,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig,
      boolean recursive,
      @Nullable List<Map<String, Object>> materializations,
      @Nullable List<Map<String, Object>> views,
      @Nullable List<Map<String, Object>> partitionedTables,
      @Nullable String refreshInterval,
      String tableNameCasing,
      String columnNameCasing) {
    this(parentSchema, name, sourceDirectory, null, directoryPattern, tables, engineConfig,
        recursive, materializations, views, partitionedTables, refreshInterval,
        tableNameCasing, columnNameCasing, null, null, null, null, true);
  }


  /**
   * Creates a file schema with all features including partitioned tables and refresh support.
   * Uses default casing (SMART_CASING for both tables and columns).
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
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File sourceDirectory,
      @Nullable String directoryPattern,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig,
      boolean recursive,
      @Nullable List<Map<String, Object>> materializations,
      @Nullable List<Map<String, Object>> views,
      @Nullable List<Map<String, Object>> partitionedTables,
      @Nullable String refreshInterval) {
    this(parentSchema, name, sourceDirectory, directoryPattern, tables, engineConfig, recursive,
        materializations, views, partitionedTables, refreshInterval, "SMART_CASING", "SMART_CASING");
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
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File sourceDirectory,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig,
      boolean recursive,
      @Nullable List<Map<String, Object>> materializations,
      @Nullable List<Map<String, Object>> views) {
    this(parentSchema, name, sourceDirectory, null, tables, engineConfig, recursive,
        materializations, views, null, null);
  }

  private final SchemaPlus parentSchema;
  private final String name;

  /**
   * Creates a file schema with execution engine support and recursive directory scanning.
   *
   * @param parentSchema    Parent schema
   * @param name            Schema name
   * @param sourceDirectory Source directory to look for files, or null
   * @param tables          List containing table identifiers, or null
   * @param engineConfig    Execution engine configuration
   * @param recursive       Whether to recursively scan subdirectories
   */
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File sourceDirectory,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig,
      boolean recursive) {
    this(parentSchema, name, sourceDirectory, null, tables, engineConfig, recursive, null,
        null, null, null);
  }

  /**
   * Creates a file schema with execution engine support (non-recursive).
   *
   * @param parentSchema    Parent schema
   * @param name            Schema name
   * @param sourceDirectory Source directory to look for files, or null
   * @param tables          List containing table identifiers, or null
   * @param engineConfig    Execution engine configuration
   */
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File sourceDirectory,
      @Nullable List<Map<String, Object>> tables, ExecutionEngineConfig engineConfig) {
    this(parentSchema, name, sourceDirectory, null, tables, engineConfig, false, null, null,
        null, null);
  }

  /**
   * Creates a file schema with default execution engine (for backward compatibility).
   *
   * @param parentSchema    Parent schema
   * @param name            Schema name
   * @param sourceDirectory Source directory to look for files, or null
   * @param tables          List containing table identifiers, or null
   */
  public FileSchema(SchemaPlus parentSchema, String name, @Nullable File sourceDirectory,
      @Nullable List<Map<String, Object>> tables) {
    this(parentSchema, name, sourceDirectory, null, tables, new ExecutionEngineConfig(), false,
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
   * Applies the configured casing transformation and sanitization to an identifier.
   */
  private String applyCasing(String identifier, String casing) {
    String casedName = org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(identifier, casing);
    // Sanitize the name to ensure consistent identifier format
    return org.apache.calcite.adapter.file.converters.ConverterUtils.sanitizeIdentifier(casedName);
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


  /**
   * Converts all supported file types to JSON using a glob pattern approach.
   * This consolidates conversion of Excel, Markdown, DOCX, PPTX, and HTML files.
   * Now also handles files from storage providers (S3, FTP, etc.).
   */
  private void convertSupportedFilesToJson(File sourceDir) {
    // Process local files if source directory exists
    if (sourceDir != null && sourceDir.exists() && sourceDir.isDirectory()) {
      convertLocalSupportedFilesToJson(sourceDir);
    }

    // Also process files from storage provider if configured
    if (storageProvider != null) {
      convertStorageProviderFilesToJson();
    }
  }

  /**
   * Converts local supported file types to JSON.
   */
  private void convertLocalSupportedFilesToJson(File sourceDir) {
    // Build glob pattern for all convertible file types
    // We need to search separately for root and recursive patterns due to glob syntax limitations
    List<String> matchingFiles = new ArrayList<>();
    
    if (recursive) {
      // Search for root-level files first
      String rootPattern = buildConvertibleFilesGlobPattern(false);
      matchingFiles.addAll(findMatchingFiles(rootPattern));
      
      // Then search for subdirectory files
      String recursivePattern = buildConvertibleFilesGlobPatternRecursive();
      matchingFiles.addAll(findMatchingFiles(recursivePattern));
    } else {
      // Just search for root-level files
      String pattern = buildConvertibleFilesGlobPattern(false);
      matchingFiles = findMatchingFiles(pattern);
    }
    
    // Determine output directory for converted files
    File conversionDir = null;
    if (baseDirectory != null) {
      // Use baseDirectory/conversions/ for converted files
      conversionDir = new File(baseDirectory, "conversions");
      if (!conversionDir.exists()) {
        conversionDir.mkdirs();
        LOGGER.debug("Created conversions directory: {}", conversionDir.getAbsolutePath());
      }
    }

    // Convert each file using FileConversionManager
    for (String filePath : matchingFiles) {
      File file = new File(filePath);

      // Skip temporary files (starting with ~)
      if (file.getName().startsWith("~")) {
        continue;
      }

      try {
        // Calculate relative path from sourceDirectory to preserve directory structure
        String relativePath = null;
        if (sourceDirectory != null) {
          relativePath = sourceDirectory.toPath().relativize(file.toPath()).toString();
        }
        
        // Use FileConversionManager for centralized conversion logic
        // Output to conversions directory if available, otherwise fallback to source directory
        File outputDir = conversionDir != null ? conversionDir : file.getParentFile();
        // Pass baseDirectory for metadata storage and relativePath for directory preservation
        boolean converted = FileConversionManager.convertIfNeeded(
            file, outputDir, columnNameCasing, tableNameCasing, baseDirectory, relativePath);
        if (converted) {
          LOGGER.debug("Converted file: {} to directory: {}", file.getName(), outputDir.getAbsolutePath());
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to convert file {}: {}", file.getName(), e.getMessage());
      }
    }
  }

  /**
   * Converts complex file types from storage provider to JSON.
   * Downloads convertible files (Excel, HTML, DOCX, etc.) to cache and converts them.
   */
  private void convertStorageProviderFilesToJson() {
    if (storageProvider == null) {
      return;
    }

    // Determine the base path for storage provider
    String basePath = "/";
    if (sourceDirectory != null) {
      basePath = sourceDirectory.getPath();
      // Handle cloud storage URIs
      if (!basePath.startsWith("s3://") && !basePath.startsWith("gs://")
          && !basePath.startsWith("azure://") && !basePath.startsWith("http")) {
        // Only normalize local paths
        if (!basePath.startsWith("/")) {
          basePath = "/" + basePath;
        }
      }
    }

    try {
      LOGGER.debug("Checking storage provider for convertible files at: {}", basePath);
      List<StorageProvider.FileEntry> entries = listFilesRecursively(basePath, recursive);

      for (StorageProvider.FileEntry entry : entries) {
        if (!entry.isDirectory() && isConvertibleFile(entry.getName())) {
          // Skip temporary files
          if (entry.getName().startsWith("~")) {
            continue;
          }

          try {
            // Download file to cache
            File cachedFile = downloadToCache(entry);

            // Determine output directory for converted JSON files
            // Use baseDirectory/.download-cache/conversions/
            File conversionDir = new File(baseDirectory, ".download-cache/conversions");
            if (!conversionDir.exists()) {
              conversionDir.mkdirs();
            }

            // Calculate relative path to preserve directory structure
            String relativePath = entry.getName();
            if (basePath != null && !basePath.equals("/") && relativePath.startsWith(basePath)) {
              relativePath = relativePath.substring(basePath.length());
              if (relativePath.startsWith("/")) {
                relativePath = relativePath.substring(1);
              }
            }
            
            // Convert the cached file
            // Pass baseDirectory for metadata storage and relativePath for directory preservation
            boolean converted = FileConversionManager.convertIfNeeded(
                cachedFile,
                conversionDir,
                columnNameCasing,
                tableNameCasing,
                baseDirectory,
                relativePath
            );

            if (converted) {
              LOGGER.debug("Converted storage provider file: {}", entry.getName());
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to convert storage provider file {}: {}",
                entry.getName(), e.getMessage());
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error processing storage provider files for conversion: {}", e.getMessage());
    }
  }

  /**
   * Downloads a file from storage provider to local cache.
   * Files are cached in .download-cache/originals/ to avoid re-downloading.
   */
  private File downloadToCache(StorageProvider.FileEntry entry) throws IOException {
    // Create cache directory structure
    File cacheDir = new File(baseDirectory, ".download-cache/originals");
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }

    // Create cached file path preserving directory structure
    String relativePath = entry.getName();
    if (relativePath.contains("/")) {
      // Create subdirectories if needed
      String subPath = relativePath.substring(0, relativePath.lastIndexOf("/"));
      File subDir = new File(cacheDir, subPath);
      if (!subDir.exists()) {
        subDir.mkdirs();
      }
    }

    File cachedFile = new File(cacheDir, relativePath);

    // Check if we need to download (file doesn't exist or is stale)
    boolean needsDownload = !cachedFile.exists();

    if (!needsDownload && storageProvider.getMetadata(entry.getPath()) != null) {
      // Check if cached file is stale
      StorageProvider.FileMetadata metadata = storageProvider.getMetadata(entry.getPath());
      if (cachedFile.lastModified() < metadata.getLastModified()) {
        needsDownload = true;
        LOGGER.debug("Cached file is stale, will re-download: {}", entry.getName());
      }
    }

    if (needsDownload) {
      LOGGER.debug("Downloading file from storage provider: {} to {}",
          entry.getPath(), cachedFile.getAbsolutePath());

      // Download the file
      try (InputStream in = storageProvider.openInputStream(entry.getPath());
           FileOutputStream out = new FileOutputStream(cachedFile)) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) != -1) {
          out.write(buffer, 0, bytesRead);
        }
      }

      // Set the last modified time to match source
      if (entry.getLastModified() > 0) {
        cachedFile.setLastModified(entry.getLastModified());
      }
    } else {
      LOGGER.debug("Using cached file: {}", cachedFile.getAbsolutePath());
    }

    return cachedFile;
  }

  /**
   * Checks if a file needs conversion based on its extension.
   */
  private boolean isConvertibleFile(String fileName) {
    if (fileName == null) {
      return false;
    }

    String lowerName = fileName.toLowerCase();
    for (String extension : CONVERTIBLE_EXTENSIONS) {
      if (lowerName.endsWith(extension)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Builds a glob pattern for finding all convertible file types.
   * @param recursive Whether to search recursively
   * @return Glob pattern string
   */
  private String buildConvertibleFilesGlobPattern(boolean recursive) {
    // Remove dots from extensions and join with commas
    StringBuilder extensions = new StringBuilder();
    boolean first = true;
    for (String ext : CONVERTIBLE_EXTENSIONS) {
      if (!first) {
        extensions.append(",");
      }
      // Remove the leading dot from extension
      extensions.append(ext.substring(1));
      first = false;
    }

    // Note: for recursive mode, this now only returns the root pattern
    // The recursive pattern is handled by buildConvertibleFilesGlobPatternRecursive()
    return "*.{" + extensions.toString() + "}";
  }
  
  /**
   * Builds a glob pattern for finding convertible files in subdirectories.
   * This is separate from buildConvertibleFilesGlobPattern to avoid nested brace syntax issues.
   * @return Glob pattern string for recursive search
   */
  private String buildConvertibleFilesGlobPatternRecursive() {
    // Remove dots from extensions and join with commas
    StringBuilder extensions = new StringBuilder();
    boolean first = true;
    for (String ext : CONVERTIBLE_EXTENSIONS) {
      if (!first) {
        extensions.append(",");
      }
      // Remove the leading dot from extension
      extensions.append(ext.substring(1));
      first = false;
    }
    
    return "**/*.{" + extensions.toString() + "}";
  }

  /**
   * Checks if a file is a native table source primitive.
   * @param fileName File name (without .gz extension if compressed)
   * @return true if this is a table source file
   */
  private boolean isTableSourceFile(String fileName) {
    if (fileName == null) {
      return false;
    }

    String lowerName = fileName.toLowerCase();
    for (String extension : TABLE_SOURCE_EXTENSIONS) {
      if (lowerName.endsWith(extension)) {
        return true;
      }
    }
    return false;
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
          final String nameSansCompression = trimCompressedExtensions(file.getName());
          if (isTableSourceFile(nameSansCompression)) {
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

      LOGGER.info("[FileSchema.getTableMap] Computing tables! baseDirectory={}, storageProvider={}, storageType={}, engine={}",
                   baseDirectory, storageProvider, storageType, engineConfig != null ? engineConfig.getEngineType() : "null");
      LOGGER.info("Schema name: {}, CSV type inference enabled: {}", name,
                  csvTypeInferenceConfig != null ? csvTypeInferenceConfig.isEnabled() : false);

    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    // Track table names to handle duplicates
    final Map<String, Integer> tableNameCounts = new HashMap<>();

    for (Map<String, Object> tableDef : this.tables) {
      addTable(builder, tableDef);
    }

    // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
    // ".json.gz".
    // If storage provider is configured, skip local file operations
    LOGGER.debug("[FileSchema.getTableMap] Checking conditions: sourceDirectory={}, baseDirectory={}, storageProvider={}, refreshInterval={}",
                 sourceDirectory, baseDirectory, storageProvider, refreshInterval);
    if (sourceDirectory != null && storageProvider == null) {
      LOGGER.debug("[FileSchema.getTableMap] Using local file system with sourceDirectory: {}", sourceDirectory);

      // Convert all supported file types to JSON using a consolidated approach
      convertSupportedFilesToJson(sourceDirectory);

      final Source baseSource = Sources.of(sourceDirectory);
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
          // For compressed files, we still need Sources.of() to handle decompression
          if (hasCompressedExtension(file.getName())) {
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
          // For files in the conversions directory, use just the filename as the table name
          String rawName;
          if (baseDirectory != null && file.getAbsolutePath().startsWith(
              new File(baseDirectory, "conversions").getAbsolutePath())) {
            // This is a converted file - use just the filename without path
            rawName = file.getName();
            // Remove extension
            if (rawName.endsWith(".json")) {
              rawName = rawName.substring(0, rawName.lastIndexOf(".json"));
            } else if (rawName.endsWith(".yaml")) {
              rawName = rawName.substring(0, rawName.lastIndexOf(".yaml"));
            } else if (rawName.endsWith(".yml")) {
              rawName = rawName.substring(0, rawName.lastIndexOf(".yml"));
            }
            rawName = WHITESPACE_PATTERN.matcher(rawName.replace(File.separator, "_"))
                .replaceAll("_");
          } else {
            // Regular file - use path relative to base source
            rawName = WHITESPACE_PATTERN.matcher(sourceSansJson.relative(baseSource).path()
                .replace(File.separator, "_"))
                .replaceAll("_");
          }
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
            LOGGER.debug("Creating JSON table {} with refresh interval: {}", tableName, this.refreshInterval);
          } else {
            LOGGER.debug("Creating JSON table {} with NO refresh interval", tableName);
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
            // DuckDB engine will be handled same as PARQUET for initial setup
            // FileSchemaFactory will create the actual DuckDB JDBC adapter later
            // Use standard Parquet table
            Table table = new ParquetTranslatableTable(new java.io.File(source.path()), name);
            builder.put(tableName, table);
            recordTableMetadata(tableName, table, source, null);
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
                    ParquetConversionUtil.getParquetCacheDir(baseDirectory, engineConfig.getParquetCacheDirectory(), name);
                File parquetFile =
                    ParquetConversionUtil.convertToParquet(source, tableName, arrowTable, cacheDir, parentSchema, this.name, tableNameCasing);
                Table table = new ParquetTranslatableTable(parquetFile, name);
                builder.put(tableName, table);
                recordTableMetadata(tableName, table, source, null);
              } catch (Exception conversionException) {
                LOGGER.warn("Parquet conversion failed for {}, using Arrow table: {}", tableName, conversionException.getMessage());
                // Fall back to using the original Arrow table
                builder.put(tableName, arrowTable);
                recordTableMetadata(tableName, arrowTable, source, null);
              }
            } else {
              // Add Arrow file as an ArrowTable
              builder.put(tableName, arrowTable);
              recordTableMetadata(tableName, arrowTable, source, null);
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

    // Process partitioned tables BEFORE views/materialized views
    if (partitionedTables != null) {
      processPartitionedTables(builder);
    }

    // Now process views and materialized views (they must come AFTER tables)
    // Process views first
    if (views != null) {
      for (Map<String, Object> view : views) {
        String viewName = (String) view.get("name");
        String sql = (String) view.get("sql");

        if (viewName != null && sql != null) {
          // For now, just log the view registration
          // In a full implementation, we would create a proper view table
          LOGGER.debug("View registered: {}", viewName);
          // TODO: Implement actual view creation
          // NOTE: When implementing, use explicit viewName without casing transformation
          // (same as materialized views)
        }
      }
    }

    // Process materialized views LAST (only works with Parquet engine)
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
              // Use schema-aware cache directory for materialized views
              File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDirectory,
                  engineConfig.getParquetCacheDirectory(), name);
              File mvDir = new File(cacheDir, ".materialized_views");
              if (!mvDir.exists()) {
                mvDir.mkdirs();
              }
              mvParquetFile = new File(mvDir, tableName + ".parquet");

              if (mvParquetFile.exists()) {
                // If the Parquet file exists, use appropriate table based on execution engine
                LOGGER.debug("Found existing materialized view: {}", mvParquetFile.getPath());
                // DuckDB engine will be handled same as PARQUET for initial setup
                // FileSchemaFactory will create the actual DuckDB JDBC adapter later
                // Use standard Parquet table
                LOGGER.debug("Using ParquetTranslatableTable to read materialized view");
                Table mvTable = new ParquetTranslatableTable(mvParquetFile, name);
                // Register by both view name and table name - use explicit names without casing transformation
                builder.put(viewName, mvTable);
                builder.put(tableName, mvTable);
                LOGGER.debug("Successfully added table for materialized view: {} (also as {})", viewName, tableName);
              } else {
                // Create a MaterializedViewTable that will execute the SQL when queried
                // and cache results to Parquet on first access
                LOGGER.debug("Creating materialized view: {}", viewName);
                LOGGER.debug("Will materialize to: {}", mvParquetFile.getPath());

                // Don't modify the SQL - MaterializedViewTable handles schema context
                // The MaterializedViewTable creates its own connection and registers
                // the schema with the existing tables, so we don't need schema prefixes
                final String finalSql = sql;
                final File finalMvFile = mvParquetFile;
                final String finalViewName = viewName;

                // Create a MaterializedViewTable that executes SQL on first access
                // Build a snapshot of current tables for the MV to use
                final Map<String, Table> currentTables = builder.build();
                LOGGER.debug("Creating MV '{}' with {} available tables: {}",
                    viewName, currentTables.size(), currentTables.keySet());
                Table mvTable =
                    new MaterializedViewTable(parentSchema, name,
                        finalViewName, finalSql, finalMvFile, currentTables);
                // Register by both view name and table name - use explicit names without casing transformation
                builder.put(viewName, mvTable);
                builder.put(tableName, mvTable);
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

    tableCache = builder.build();
    LOGGER.info("[FileSchema.getTableMap] COMPLETED - Computed {} tables for schema '{}': {}",
                tableCache.size(), name, tableCache.keySet());
    if (tableCache.isEmpty()) {
      LOGGER.warn("[FileSchema.getTableMap] WARNING: No tables were registered for schema '{}'!", name);
    }

    // Generate Calcite model file in baseDirectory
    generateModelFile(tableCache);

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

      Table globTable = new GlobParquetTable(url, tableName, cacheDir, refreshDuration, csvTypeInferenceConfig);
      // Use explicit table name as-is, without casing transformation
      builder.put(tableName, globTable);
      return true;
    }

    // Check if HTTP URL with custom configuration (POST, headers, etc.)
    if (url != null && (url.startsWith("http://") || url.startsWith("https://"))
        && hasHttpConfiguration(tableDef)) {
      final Source source = resolveSourceWithHttpConfig(url, tableDef);
      LOGGER.debug("Creating HTTP table '{}' with source: {} (type: {})", tableName, source.path(), source.getClass().getName());
      return addTable(builder, source, tableName, tableDef);
    }

    // Regular processing for URLs
    if (url == null) {
      LOGGER.warn("Skipping table '{}' due to null URL", tableName);
      return false;
    }
    final Source source = resolveSource(url);
    return addTable(builder, source, tableName, tableDef);
  }

  /**
   * Checks if a URL contains glob patterns like *, ?, or [].
   */
  private boolean isGlobPattern(String url) {
    // Null URLs are not glob patterns
    if (url == null) {
      return false;
    }

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

    // Apply source directory for relative paths (use original directory for reading files)
    if (sourceDirectory != null && !isAbsoluteUri(uri)) {
      return Sources.of(sourceDirectory).append(source0);
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
    LOGGER.info("FileSchema.addTable START: schemaName={}, tableName='{}', source={}, csvTypeInferenceConfig={}, engineType={}",
        name, tableName, source.path(),
        csvTypeInferenceConfig != null ? "enabled=" + csvTypeInferenceConfig.isEnabled() : "null",
        engineConfig != null ? engineConfig.getEngineType() : "null");

    // Check if refresh is configured
    String refreshIntervalStr = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
    Duration refreshInterval = RefreshInterval.parse(refreshIntervalStr);
    boolean hasRefresh = refreshInterval != null;
    LOGGER.info("FileSchema.addTable: tableName={}, source={}, refreshIntervalStr={}, hasRefresh={}",
        tableName, source.path(), refreshIntervalStr, hasRefresh);

    String path = source.path().toLowerCase(Locale.ROOT);
    boolean isCSVorJSON = path.endsWith(".csv") || path.endsWith(".csv.gz") ||
                          path.endsWith(".json") || path.endsWith(".json.gz");

    // Handle refreshable CSV/JSON files with Parquet caching
    LOGGER.info("Refresh check: hasRefresh={}, isCSVorJSON={}, engineType={}, baseDirectory!=null={}",
        hasRefresh, isCSVorJSON, engineConfig.getEngineType(), baseDirectory != null);

    if (hasRefresh && isCSVorJSON &&
        engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.PARQUET &&
        baseDirectory != null) {
      LOGGER.info("CREATING RefreshableParquetCacheTable for: {}", tableName);

      try {
        File cacheDir =
            ParquetConversionUtil.getParquetCacheDir(baseDirectory, engineConfig.getParquetCacheDirectory(), name);
        File sourceFile = new File(source.path());
        boolean typeInferenceEnabled = csvTypeInferenceConfig != null && csvTypeInferenceConfig.isEnabled();
        File parquetFile = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, typeInferenceEnabled, this.tableNameCasing);

        // Check if we need initial conversion/update
        if (!parquetFile.exists() || sourceFile.lastModified() > parquetFile.lastModified()) {
          LOGGER.debug("Creating/updating parquet cache for refreshable table: {}", tableName);

          // Convert/update the cache file using standard conversion method
          Table sourceTable = path.endsWith(".csv") || path.endsWith(".csv.gz")
              ? new CsvTranslatableTable(source, null, columnNameCasing, csvTypeInferenceConfig)
              : createTableFromSource(source, tableDef);

          if (sourceTable != null) {
            parquetFile =
                ParquetConversionUtil.convertToParquet(source, tableName, sourceTable, cacheDir, parentSchema, name, tableNameCasing);
          } else {
            throw new RuntimeException("Failed to create source table for: " + source.path());
          }
        } else {
          LOGGER.debug("Using existing parquet cache for refreshable table: {}", tableName);
        }

        // Create refreshable table that monitors the source and updates cache
        // Find original source file for converted files (e.g., HTML->JSON)
        Source originalSource = findOriginalSource(new File(source.path()));
        LOGGER.debug("RefreshableParquetCacheTable - source: {}, originalSource: {}", source.path(), (originalSource != null ? originalSource.path() : "null"));

        RefreshableParquetCacheTable refreshableTable =
            new RefreshableParquetCacheTable(source, originalSource, parquetFile, cacheDir, refreshInterval, typeInferenceEnabled,
            columnNameCasing, tableNameCasing, null, engineConfig.getEngineType(), parentSchema, name);

        String finalName = applyCasing(tableName, tableNameCasing);
        builder.put(finalName, refreshableTable);
        return true;

      } catch (Exception e) {
        LOGGER.error("Failed to create refreshable parquet cache table for {}: {}", tableName, e.getMessage(), e);
        // Fall through to regular processing
      }
    }

    // Check if we should convert to Parquet (for PARQUET engine)
    String tableRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
    Duration effectiveInterval = RefreshInterval.getEffectiveInterval(tableRefreshInterval, this.refreshInterval);

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
                ParquetConversionUtil.getParquetCacheDir(baseDirectory, engineConfig.getParquetCacheDirectory(), name);

            // Convert to Parquet
            File parquetFile =
                ParquetConversionUtil.convertToParquet(source, tableName,
                    originalTable, cacheDir, parentSchema, name, tableNameCasing);

            // DuckDB engine will be handled same as PARQUET for initial setup
            // FileSchemaFactory will create the actual DuckDB JDBC adapter later

            // Create appropriate table based on refresh configuration
            Table parquetTable;
            if (effectiveInterval != null) {
              // Create refreshable table with Parquet cache
              boolean typeInferenceEnabled = path.endsWith(".csv") || path.endsWith(".csv.gz")
                  ? (csvTypeInferenceConfig != null && csvTypeInferenceConfig.isEnabled())
                  : false;

              parquetTable =
                  new RefreshableParquetCacheTable(source, parquetFile, cacheDir, effectiveInterval, typeInferenceEnabled,
                  columnNameCasing, tableNameCasing, null, engineConfig.getEngineType(), parentSchema, name);
            } else {
              // Use standard Parquet table for non-refresh cases
              parquetTable = new ParquetTranslatableTable(parquetFile, name);
            }

            builder.put(
                applyCasing(Util.first(tableName, source.path()),
                tableNameCasing), parquetTable);

            return true;
          }
        } catch (Exception e) {
          LOGGER.error("Failed to convert {} to Parquet: {}", tableName, e.getMessage(), e);
          LOGGER.error("Parquet conversion stacktrace:", e);
          throw new RuntimeException("PARQUET engine requires successful conversion. Failed to convert " + tableName + " to Parquet: " + e.getMessage(), e);
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
          Duration jsonEffectiveInterval = RefreshInterval.getEffectiveInterval(jsonRefreshInterval, this.refreshInterval);

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
        String finalTableName = getTableName(tableName, source.path(), tableNameCasing);
        LOGGER.debug("Created JSON table of type: {} for table '{}' -> registered as '{}'", jsonTable.getClass().getName(), tableName, finalTableName);
        builder.put(finalTableName, jsonTable);
        recordTableMetadata(finalTableName, jsonTable, source, tableDef);
        return true;
      case "csv":
        String csvRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
        final Table csvTable = createEnhancedCsvTable(source, null, tableName, csvRefreshInterval);
        String csvFinalName = getTableName(tableName, source.path(), tableNameCasing);
        builder.put(csvFinalName, csvTable);
        recordTableMetadata(csvFinalName, csvTable, source, tableDef);
        LOGGER.info("FileSchema.addTable SUCCESS: Added CSV table '{}' to schema '{}'", csvFinalName, name);
        return true;
      case "tsv":
        String tsvRefreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
        final Table tsvTable = createEnhancedCsvTable(source, null, tableName, tsvRefreshInterval);
        String tsvFinalName = getTableName(tableName, source.path(), tableNameCasing);
        builder.put(tsvFinalName, tsvTable);
        LOGGER.info("FileSchema.addTable SUCCESS: Added TSV table '{}' to schema '{}'", tsvFinalName, name);
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
        builder.put(getTableName(tableName, source.path(), tableNameCasing), yamlTable);
        return true;
      case "html":
      case "htm":
        // For backward compatibility, allow local HTML files without fragments
        // This supports existing tests and configurations
        final Table htmlTable = FileTable.create(source, tableDef);
        builder.put(getTableName(tableName, source.path(), tableNameCasing), htmlTable);
        return true;
      case "parquet":
        // DuckDB engine will be handled same as PARQUET for initial setup
        // FileSchemaFactory will create the actual DuckDB JDBC adapter later
        // Use standard Parquet table
        final Table parquetTable = new ParquetTranslatableTable(new java.io.File(source.path()), name);
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
      // For PARQUET engine, CSV files should have been converted above - don't create fallback CSV tables
      if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.PARQUET) {
        throw new RuntimeException("PARQUET engine: CSV file " + tableName + " should have been converted to Parquet above. This fallback should not execute.");
      }
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
      builder.put(getTableName(tableName, sourceSansTsv.path(), tableNameCasing), table);
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
        // DuckDB engine will be handled same as PARQUET for initial setup
        // FileSchemaFactory will create the actual DuckDB JDBC adapter later
        // Use our custom ParquetTranslatableTable instead of arrow's ParquetTable
        final Table table = new ParquetTranslatableTable(new java.io.File(source.path()), name);
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
      // For directory/file discovery mode, return false to let convertSupportedFilesToJson handle it
      return false;
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
          builder.put(getTableName(tableName, source.path(), tableNameCasing), table);
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
        builder.put(getTableName(tableName, source.path(), tableNameCasing), table);
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
              ParquetConversionUtil.getParquetCacheDir(baseDirectory, engineConfig.getParquetCacheDirectory(), name);
          File parquetFile =
              ParquetConversionUtil.convertToParquet(source, tableName, new CsvTranslatableTable(source, protoRowType, columnNameCasing, csvTypeInferenceConfig),
              cacheDir, parentSchema, name, tableNameCasing);
          boolean typeInferenceEnabled = csvTypeInferenceConfig != null && csvTypeInferenceConfig.isEnabled();
          // Use standard RefreshableParquetCacheTable for PARQUET engine
          return new RefreshableParquetCacheTable(source, parquetFile, cacheDir,
              effectiveInterval, typeInferenceEnabled, columnNameCasing, tableNameCasing, protoRowType,
              engineConfig.getEngineType(), parentSchema, name);
        } catch (Exception e) {
          throw new RuntimeException("Failed to create refreshable Parquet table for " + source.path(), e);
        }
      }
      // For PARQUET engine, return a regular CSV table that will be converted to Parquet
      // The conversion happens in addTable() method
      return new CsvTranslatableTable(source, protoRowType, columnNameCasing, csvTypeInferenceConfig);
    case DUCKDB:
      // DuckDB engine will be handled same as PARQUET for initial setup
      // FileSchemaFactory will create the actual DuckDB JDBC adapter later
      // Fall through to PARQUET handling
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
        // Check if this JSON file was extracted via JSONPath
        boolean isJsonPathExtraction = false;
        File jsonFile = source.file();
        if (jsonFile != null) {
          try {
            org.apache.calcite.adapter.file.metadata.ConversionMetadata metadata =
                new org.apache.calcite.adapter.file.metadata.ConversionMetadata(jsonFile.getParentFile());
            org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record =
                metadata.getConversionRecord(jsonFile);
            if (record != null && record.getConversionType() != null
                && record.getConversionType().startsWith("JSONPATH_EXTRACTION")) {
              isJsonPathExtraction = true;
              LOGGER.debug("JSON file {} is a JSONPath extraction, using regular JSON table", jsonFile.getName());
            }
          } catch (Exception e) {
            LOGGER.debug("Could not check if {} is a JSONPath extraction: {}", source.path(), e.getMessage());
          }
        }

        // JSONPath extractions should use regular JSON tables that can be refreshed via re-extraction
        if (isJsonPathExtraction) {
          return new RefreshableJsonTable(source, tableName, effectiveInterval, columnNameCasing);
        }

        try {
          // For Parquet engine with refresh, we need to use RefreshableParquetCacheTable
          // First convert JSON to Parquet
          File cacheDir =
              ParquetConversionUtil.getParquetCacheDir(baseDirectory, engineConfig.getParquetCacheDirectory(), name);
          File parquetFile =
              ParquetConversionUtil.convertToParquet(source, tableName, new JsonScannableTable(source, options, columnNameCasing), cacheDir, parentSchema, name, tableNameCasing);

          // Check if this JSON file was converted from another source (HTML, Excel, XML)
          Source originalSource = null;
          try {
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
              effectiveInterval, false, columnNameCasing, tableNameCasing, null,
              engineConfig.getEngineType(), parentSchema, name);
        } catch (Exception e) {
          throw new RuntimeException("Failed to create refreshable Parquet table for " + source.path(), e);
        }
      }
      // For PARQUET engine without refresh, just use regular JSON table
      // The ParquetJsonScannableTable expects actual parquet files, not JSON
      return new JsonScannableTable(source, options, columnNameCasing);
    case DUCKDB:
      // DuckDB engine will be handled same as PARQUET for initial setup
      // FileSchemaFactory will create the actual DuckDB JDBC adapter later
      // Fall through to PARQUET handling
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
        // Storage provider config explicitly defines table name - use as-is
        builder.put(config.getName(), table);

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

    if (sourceDirectory == null || pattern == null) {
      return result;
    }

    try {
      Path basePath = sourceDirectory.toPath();
      PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
      
      // Special handling for patterns starting with "**/": also match root files
      // For example, "**/*.csv" should also match "*.csv" in the root directory
      PathMatcher rootMatcher = null;
      if (pattern.startsWith("**/")) {
        String rootPattern = pattern.substring(3); // Get pattern after "**/"
        rootMatcher = FileSystems.getDefault().getPathMatcher("glob:" + rootPattern);
      }

      final PathMatcher finalRootMatcher = rootMatcher;
      Files.walkFileTree(basePath, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
        @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
          // Skip the .aperio directory to avoid finding converted files through source directory scan
          if (dir.getFileName() != null && dir.getFileName().toString().equals(".aperio")) {
            return FileVisitResult.SKIP_SUBTREE;
          }
          return FileVisitResult.CONTINUE;
        }
        
        @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
          Path relativePath = basePath.relativize(file);
          // Match against main pattern OR root pattern (if applicable)
          if (matcher.matches(relativePath) || 
              (finalRootMatcher != null && finalRootMatcher.matches(relativePath))) {
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

    // Otherwise use local file system from sourceDirectory
    if (sourceDirectory == null) {
      LOGGER.warn("[FileSchema] sourceDirectory is null, returning empty file array");
      return new File[0];
    }

    LOGGER.debug("[FileSchema] getFilesForProcessing - sourceDirectory: {}, exists: {}, isDirectory: {}",
        sourceDirectory.getAbsolutePath(), sourceDirectory.exists(), sourceDirectory.isDirectory());

    // Determine the glob pattern to use
    String pattern;

    if (directoryPattern != null && isGlobPattern(directoryPattern)) {
      // Directory pattern itself contains glob characters - use as-is
      // The findMatchingFiles method will handle **/ patterns specially
      pattern = directoryPattern;
    } else if (recursive) {
      // Use recursive glob pattern that includes both root and subdirectory files
      pattern = "**";
    } else {
      // Use non-recursive glob pattern for plain directories
      pattern = "*";
    }

    // Collect files from source directory
    List<String> matchingFiles = findMatchingFiles(pattern);
    List<File> allFiles = new ArrayList<>();
    
    // Add files from source directory
    matchingFiles.stream()
        .map(File::new)
        .filter(this::isFileTypeSupported)
        .forEach(allFiles::add);
    
    // Also include converted files from base directory if it exists
    if (baseDirectory != null) {
      // Check for converted files in baseDirectory/conversions/
      File conversionsDir = new File(baseDirectory, "conversions");
      if (conversionsDir.exists() && conversionsDir.isDirectory()) {
        LOGGER.debug("[FileSchema] Including converted files from: {}", conversionsDir.getAbsolutePath());
        File[] convertedFiles = conversionsDir.listFiles(file -> isFileTypeSupported(file));
        if (convertedFiles != null) {
          for (File file : convertedFiles) {
            allFiles.add(file);
            LOGGER.debug("[FileSchema] Added converted file: {}", file.getName());
          }
        }
      }
      
      // Check for converted files from storage providers in .download-cache/conversions/
      File downloadCacheConversionsDir = new File(new File(baseDirectory, ".download-cache"), "conversions");
      if (downloadCacheConversionsDir.exists() && downloadCacheConversionsDir.isDirectory()) {
        LOGGER.debug("[FileSchema] Including storage provider converted files from: {}", 
            downloadCacheConversionsDir.getAbsolutePath());
        File[] storageConvertedFiles = downloadCacheConversionsDir.listFiles(file -> isFileTypeSupported(file));
        if (storageConvertedFiles != null) {
          for (File file : storageConvertedFiles) {
            allFiles.add(file);
            LOGGER.debug("[FileSchema] Added storage provider converted file: {}", file.getName());
          }
        }
      }
    }
    
    // Deduplicate files based on absolute path and sort with the same logic
    // Use a Set to track unique paths for deduplication
    Set<String> seenPaths = new HashSet<>();
    File[] sortedFiles = allFiles.stream()
        .filter(file -> seenPaths.add(file.getAbsolutePath())) // Only keep files with unique absolute paths
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

    LOGGER.debug("[FileSchema] Total files for processing: {} (source: {}, converted: {})", 
        sortedFiles.length, matchingFiles.size(), allFiles.size() - matchingFiles.size());

    return sortedFiles;
  }

  /**
   * Gets the file extension without compression suffix.
   */
  private String getFileExtension(String fileName) {
    String nameSansCompression = trimCompressedExtensions(fileName);
    int lastDot = nameSansCompression.lastIndexOf('.');
    return lastDot > 0 ? nameSansCompression.substring(lastDot + 1) : "";
  }

  /**
   * Removes known compression extensions from a file name.
   * @param fileName The file name to process
   * @return The file name without compression extensions
   */
  private String trimCompressedExtensions(String fileName) {
    if (fileName == null) {
      return null;
    }

    String result = fileName;
    for (String ext : COMPRESSED_EXTENSIONS) {
      if (result.toLowerCase().endsWith(ext)) {
        result = result.substring(0, result.length() - ext.length());
        break; // Only remove one compression extension
      }
    }
    return result;
  }

  /**
   * Checks if a file has a compressed extension.
   * @param fileName The file name to check
   * @return true if the file has a compressed extension
   */
  private boolean hasCompressedExtension(String fileName) {
    if (fileName == null) {
      return false;
    }

    String lowerName = fileName.toLowerCase();
    for (String ext : COMPRESSED_EXTENSIONS) {
      if (lowerName.endsWith(ext)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if a file type is supported based on its extension.
   */
  private boolean isFileTypeSupported(File file) {
    if (!file.isFile()) {
      return false;
    }
    // Exclude hidden files and metadata files (like .calcite_conversions)
    if (file.getName().startsWith(".")) {
      return false;
    }
    final String nameSansCompression = trimCompressedExtensions(file.getName());
    // HTML is a special case - it's both a potential table source and a convertible file
    return isTableSourceFile(nameSansCompression) || nameSansCompression.toLowerCase().endsWith(".html");
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
    final String nameSansCompression = trimCompressedExtensions(fileName);
    // HTML is a special case - it's both a potential table source and a convertible file
    return isTableSourceFile(nameSansCompression) || nameSansCompression.toLowerCase().endsWith(".html");
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

  /**
   * Generates a Calcite model JSON file documenting the discovered schema and tables.
   * The file is saved to baseDirectory/generated-model.json
   */
  private void generateModelFile(Map<String, Table> tables) {
    if (baseDirectory == null || !baseDirectory.exists()) {
      LOGGER.debug("Cannot generate model file - baseDirectory doesn't exist");
      return;
    }

    File modelFile = new File(baseDirectory, ".generated-model.json");

    try (FileWriter writer = new FileWriter(modelFile)) {
      writer.write("{\n");
      writer.write("  \"version\": \"1.0\",\n");
      writer.write("  \"defaultSchema\": \"" + name + "\",\n");
      writer.write("  \"schemas\": [\n");
      writer.write("    {\n");
      writer.write("      \"name\": \"" + name + "\",\n");
      writer.write("      \"type\": \"custom\",\n");
      writer.write("      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n");
      writer.write("      \"operand\": {\n");

      // Write source directory if available
      if (sourceDirectory != null) {
        writer.write("        \"directory\": \"" + sourceDirectory.getAbsolutePath().replace("\\", "\\\\") + "\",\n");
      }

      // Write execution engine
      writer.write("        \"executionEngine\": \"" + engineConfig.getEngineType() + "\",\n");

      // Write storage type if configured
      if (storageType != null) {
        writer.write("        \"storageType\": \"" + storageType + "\",\n");
      }

      // Write refresh interval if configured
      if (refreshInterval != null) {
        writer.write("        \"refreshInterval\": \"" + refreshInterval + "\",\n");
      }

      // Write table and column casing
      writer.write("        \"tableNameCasing\": \"" + tableNameCasing + "\",\n");
      writer.write("        \"columnNameCasing\": \"" + columnNameCasing + "\",\n");

      // Write discovered tables
      writer.write("        \"tables\": [\n");
      boolean firstTable = true;
      for (Map.Entry<String, Table> entry : tables.entrySet()) {
        if (!firstTable) {
          writer.write(",\n");
        }
        writer.write("          {\n");
        writer.write("            \"name\": \"" + entry.getKey() + "\",\n");

        // Add table type info
        Table table = entry.getValue();
        String tableType = table.getClass().getSimpleName();
        writer.write("            \"type\": \"" + tableType + "\"");

        // Add source file if we can determine it
        if (table instanceof FileTable) {
          try {
            // Use reflection to get the source file
            java.lang.reflect.Field sourceField = table.getClass().getDeclaredField("source");
            sourceField.setAccessible(true);
            Source source = (Source) sourceField.get(table);
            if (source != null) {
              writer.write(",\n            \"source\": \"" + source.path().replace("\\", "\\\\") + "\"");
            }
          } catch (Exception e) {
            // Ignore - not all tables have a source field
          }
        }

        writer.write("\n          }");
        firstTable = false;
      }
      writer.write("\n        ]\n");
      writer.write("      }\n");
      writer.write("    }\n");
      writer.write("  ]\n");
      writer.write("}\n");

      LOGGER.info("Generated Calcite model file: {}", modelFile.getAbsolutePath());
    } catch (IOException e) {
      LOGGER.warn("Failed to generate model file: {}", e.getMessage());
    }
  }

  /**
   * Records table metadata for comprehensive tracking.
   * This captures the complete lineage from original source to final table.
   */
  private void recordTableMetadata(String tableName, Table table, Source source, 
      @Nullable Map<String, Object> tableDef) {
    if (conversionMetadata != null) {
      conversionMetadata.recordTable(tableName, table, source, tableDef);
    }
  }

}
