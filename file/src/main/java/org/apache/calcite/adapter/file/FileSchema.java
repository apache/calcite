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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Schema mapped onto a set of URLs / HTML tables. Each table in the schema
 * is an HTML table on a URL.
 */
class FileSchema extends AbstractSchema {
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
   */
  FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
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
      @Nullable Boolean flatten) {
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

    // Create storage provider if configured
    if (storageType != null) {
      this.storageProvider = StorageProviderFactory.createFromType(storageType, storageConfig);
    } else {
      this.storageProvider = null;
    }
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
  FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
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
  FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
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
  FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
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
  FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
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
  FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
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
  FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
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
    if (identifier == null) {
      return null;
    }
    switch (casing) {
    case "UPPER":
      return identifier.toUpperCase(Locale.ROOT);
    case "LOWER":
      return identifier.toLowerCase(Locale.ROOT);
    case "UNCHANGED":
    default:
      return identifier;
    }
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
            System.out.println("!");
          }
        }
      }
    }
  }

  private Map<String, List<HtmlTableScanner.TableInfo>> scanHtmlFiles(File baseDirectory) {
    Map<String, List<HtmlTableScanner.TableInfo>> htmlTables = new LinkedHashMap<>();

    // Get the list of all files and directories
    File[] files = baseDirectory.listFiles();

    if (files != null) {
      for (File file : files) {
        // If it's a directory and recursive is enabled, recurse into it
        if (file.isDirectory() && recursive) {
          htmlTables.putAll(scanHtmlFiles(file));
        } else if ((file.getName().endsWith(".html") || file.getName().endsWith(".htm"))
            && !file.getName().startsWith("~")) {
          // If it's a file ending in .html or .htm, scan it for tables
          try {
            Source source = Sources.of(file);
            List<HtmlTableScanner.TableInfo> tables = HtmlTableScanner.scanTables(source);
            if (!tables.isEmpty()) {
              htmlTables.put(file.getPath(), tables);
            }
          } catch (Exception e) {
            System.err.println("Error scanning HTML file: " + file.getName() + " - "
                + e.getMessage());
          }
        }
      }
    }

    return htmlTables;
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
            System.err.println("Error scanning Markdown file: " + file.getName()
                + " - " + e.getMessage());
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
            System.err.println("Error scanning DOCX file: " + file.getName()
                + " - " + e.getMessage());
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
      System.out.println("[FileSchema] Scanning directory (recursive=" + recursive + "): " + dir);
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
              || nameSansGz.endsWith(".html")
              || nameSansGz.endsWith(".arrow")
              || nameSansGz.endsWith(".parquet")) {
            files.add(file);
          }
        }
      }
    }

    return files.toArray(new File[0]);
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    // Track table names to handle duplicates
    final Map<String, Integer> tableNameCounts = new HashMap<>();

    for (Map<String, Object> tableDef : this.tables) {
      addTable(builder, tableDef);
    }

    // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
    // ".json.gz".
    if (baseDirectory != null) {

      convertExcelFilesToJson(baseDirectory);

      // Convert Markdown files to JSON
      convertMarkdownFilesToJson(baseDirectory);

      // Convert DOCX files to JSON
      convertDocxFilesToJson(baseDirectory);

      // Always scan HTML files for tables
      Map<String, List<HtmlTableScanner.TableInfo>> htmlTables = scanHtmlFiles(baseDirectory);

      final Source baseSource = Sources.of(baseDirectory);
      // Get files using glob or regular directory scanning
      File[] files = getFilesForProcessing();
      System.out.println("[FileSchema] Found " + files.length + " files for processing");
      // Track table names to handle conflicts
      Map<String, String> tableNameToFileType = new HashMap<>();

      // Build a map from table name to table; each file becomes a table.
      for (File file : files) {
        // Use DirectFileSource for PARQUET engine to bypass Sources cache, but handle gzip properly
        Source source;
        if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.PARQUET) {
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
          String baseName =
              applyCasing(
                  WHITESPACE_PATTERN.matcher(sourceSansJson.relative(baseSource).path()
              .replace(File.separator, "_"))
              .replaceAll("_"), tableNameCasing);
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
          try {
            // Add Parquet file as a ParquetTranslatableTable
            Table table = new ParquetTranslatableTable(new java.io.File(source.path()));
            builder.put(tableName, table);
          } catch (Exception e) {
            System.err.println("Failed to add Parquet table: " + e.getMessage());
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
                File cacheDir = new File(baseDirectory, ".parquet_cache");
                File parquetFile = ParquetConversionUtil.convertToParquet(source, tableName, arrowTable, cacheDir, 
                    parentSchema, "FILE");
                Table table = new ParquetTranslatableTable(parquetFile);
                builder.put(tableName, table);
              } catch (Exception conversionException) {
                System.err.println("Parquet conversion failed for " + tableName + ", using Arrow table: " + conversionException.getMessage());
                // Fall back to using the original Arrow table
                builder.put(tableName, arrowTable);
              }
            } else {
              // Add Arrow file as an ArrowTable
              builder.put(tableName, arrowTable);
            }
          } catch (Exception e) {
            System.err.println("Failed to add Arrow table: " + e.getMessage());
          }
        }
        // HTML files are always scanned for tables in directory mode
        // Explicit HTML tables with URL fragments are handled via table definitions
      }

      // Add HTML tables discovered during scanning
      if (htmlTables != null && !htmlTables.isEmpty()) {
        for (Map.Entry<String, List<HtmlTableScanner.TableInfo>> entry : htmlTables.entrySet()) {
          String htmlFilePath = entry.getKey();
          Source htmlSource = Sources.of(new File(htmlFilePath));
          String baseFileName = new File(htmlFilePath).getName();

          // Remove extension
          if (baseFileName.toLowerCase(Locale.ROOT).endsWith(".html")) {
            baseFileName = baseFileName.substring(0, baseFileName.length() - 5);
          } else if (baseFileName.toLowerCase(Locale.ROOT).endsWith(".htm")) {
            baseFileName = baseFileName.substring(0, baseFileName.length() - 4);
          }

          // Create a FileTable for each table in the HTML
          for (HtmlTableScanner.TableInfo tableInfo : entry.getValue()) {
            String fullTableName = applyCasing(baseFileName + "__"
                + tableInfo.name, tableNameCasing);

            // Create table definition with selector
            Map<String, Object> htmlTableDef = new LinkedHashMap<>();
            htmlTableDef.put("selector", tableInfo.selector);
            // Don't pass index when we have a specific selector
            // The selector should already target the specific table

            try {
              FileTable table = FileTable.create(htmlSource, htmlTableDef);
              builder.put(fullTableName, table);
            } catch (Exception e) {
              System.err.println("Failed to create table " + fullTableName + " from "
                  + htmlFilePath + ": " + e.getMessage());
            }
          }
        }
      }
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
                // If the Parquet file exists, use our ParquetTranslatableTable
                System.out.println("Found existing materialized view: "
                    + mvParquetFile.getPath());
                System.out.println("Using ParquetTranslatableTable to read it");
                Table mvTable = new ParquetTranslatableTable(mvParquetFile);
                builder.put(applyCasing(viewName, tableNameCasing), mvTable);
                System.out.println(
                    "Successfully added ParquetTranslatableTable for view: "
                    + viewName);
              } else {
                // Create a MaterializedViewTable that will execute the SQL when queried
                // and cache results to Parquet on first access
                System.out.println("Creating materialized view: " + viewName);
                System.out.println("Will materialize to: " + mvParquetFile.getPath());

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
            System.err.println("Failed to create materialized view "
                + viewName + ": " + e.getMessage());
            e.printStackTrace();
          }
        }
      }
    } else if (materializations != null) {
      System.err.println(
          "ERROR: Materialized views are only supported with Parquet execution engine");
      System.err.println("Current engine: " + engineConfig.getEngineType());
      System.err.println(
          "To use materialized views, set executionEngine to 'parquet' in your schema configuration");
    }

    // Process views
    if (views != null) {
      for (Map<String, Object> view : views) {
        String viewName = (String) view.get("name");
        String sql = (String) view.get("sql");

        if (viewName != null && sql != null) {
          // For now, just log the view registration
          // In a full implementation, we would create a proper view table
          System.out.println("View registered: " + viewName);
          // TODO: Implement actual view creation
        }
      }
    }

    // Process partitioned tables
    if (partitionedTables != null) {
      processPartitionedTables(builder);
    }

    return builder.build();
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

    // Regular processing for non-glob URLs
    final Source source = resolveSource(url);
    return addTable(builder, source, tableName, tableDef);
  }

  /**
   * Checks if a URL contains glob patterns like *, ?, or [].
   */
  private boolean isGlobPattern(String url) {
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
    // Check if refresh is configured - if so, use enhanced table creation instead of direct Parquet conversion
    String refreshIntervalStr = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
    boolean hasRefresh = RefreshInterval.parse(refreshIntervalStr) != null;
    
    // Check if we should convert to Parquet (but not if refresh is enabled)
    if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.PARQUET
        && baseDirectory != null
        && !hasRefresh  // Don't bypass enhanced table creation if refresh is configured
        && !source.path().toLowerCase(Locale.ROOT).endsWith(".parquet")) {

      // Check if it's a file type we can convert
      String path = source.path().toLowerCase(Locale.ROOT);
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
                ? new CsvTranslatableTable(source, null, columnNameCasing)
                : createTableFromSource(source, tableDef);
            if (originalTable != null) {
            // Get cache directory
            File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDirectory);

            // Convert to Parquet
            File parquetFile =
                ParquetConversionUtil.convertToParquet(source, tableName,
                    originalTable, cacheDir, parentSchema, name);

            // Create a ParquetTranslatableTable for the converted file
            Table parquetTable = new ParquetTranslatableTable(parquetFile);
            builder.put(
                applyCasing(Util.first(tableName, source.path()),
                tableNameCasing), parquetTable);

            return true;
          }
        } catch (Exception e) {
          System.err.println("Failed to convert " + tableName + " to Parquet: " + e.getMessage());
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
        String refreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
        Map<String, Object> options = null;
        if (tableDef != null && tableDef.containsKey("flatten")) {
          options = new HashMap<>();
          options.put("flatten", tableDef.get("flatten"));
          if (tableDef.containsKey("flattenSeparator")) {
            options.put("flattenSeparator", tableDef.get("flattenSeparator"));
          }
        }
        final Table jsonTable =
            createEnhancedJsonTable(source, tableName, refreshInterval, options);
        builder.put(applyCasing(Util.first(tableName, source.path()), tableNameCasing), jsonTable);
        return true;
      case "csv":
        refreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
        final Table csvTable = createEnhancedCsvTable(source, null, tableName, refreshInterval);
        builder.put(applyCasing(Util.first(tableName, source.path()), tableNameCasing), csvTable);
        return true;
      case "tsv":
        refreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
        final Table tsvTable = createEnhancedCsvTable(source, null, tableName, refreshInterval);
        builder.put(applyCasing(Util.first(tableName, source.path()), tableNameCasing), tsvTable);
        return true;
      case "yaml":
      case "yml":
        refreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
        options = null;
        if (tableDef != null && tableDef.containsKey("flatten")) {
          options = new HashMap<>();
          options.put("flatten", tableDef.get("flatten"));
          if (tableDef.containsKey("flattenSeparator")) {
            options.put("flattenSeparator", tableDef.get("flattenSeparator"));
          }
        }
        final Table yamlTable =
            createEnhancedJsonTable(source, tableName, refreshInterval, options);
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
      String refreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
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
          createEnhancedJsonTable(source, tableName, refreshInterval, options);
      builder.put(
          applyCasing(Util.first(tableName, sourceSansJson.path()),
          tableNameCasing), table);
      return true;
    }
    final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
    if (sourceSansCsv != null) {
      String refreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
      final Table table = createEnhancedCsvTable(source, null, tableName, refreshInterval);
      String tableNameKey = applyCasing(Util.first(tableName, sourceSansCsv.path()), tableNameCasing);
      builder.put(tableNameKey, table);
      return true;
    }
    final Source sourceSansTsv = sourceSansGz.trimOrNull(".tsv");
    if (sourceSansTsv != null) {
      String refreshInterval = tableDef != null ? (String) tableDef.get("refreshInterval") : null;
      final Table table = createEnhancedCsvTable(source, null, tableName, refreshInterval);
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
        System.err.println("Warning: Failed to create Arrow table for " + source.path()
            + ": " + e.getMessage());
      }
    }
    final Source sourceSansParquet = sourceSansGz.trimOrNull(".parquet");
    if (sourceSansParquet != null) {
      try {
        // Use our custom ParquetTranslatableTable instead of arrow's ParquetTable
        final Table table = new ParquetTranslatableTable(new java.io.File(source.path()));
        builder.put(
            applyCasing(Util.first(tableName, sourceSansParquet.path()),
            tableNameCasing), table);
        return true;
      } catch (Exception e) {
        // Fall back to generic FileTable if Parquet creation fails
        System.err.println("Warning: Failed to create Parquet table for " + source.path()
            + ": " + e.getMessage());
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
        System.err.println("Warning: Failed to convert Excel file " + source.path()
            + ": " + e.getMessage());
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

      return (org.apache.calcite.schema.Table) arrowTableClass
          .getConstructor(org.apache.calcite.rel.type.RelProtoDataType.class, arrowFileReaderClass)
          .newInstance(null, arrowFileReader);
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
        return new JsonScannableTable(source, options);
      case "csv":
        // Use simple table for conversion to avoid Arrow issues
        return new CsvTranslatableTable(source, null);
      case "tsv":
        // Use simple table for conversion to avoid Arrow issues
        return new CsvTranslatableTable(source, null);
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
        return new JsonScannableTable(source, options);
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
      return new JsonScannableTable(source, options);
    }

    final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
    if (sourceSansCsv != null) {
      // Use simple table for conversion to avoid Arrow issues
      return new CsvTranslatableTable(source, null);
    }

    final Source sourceSansTsv = sourceSansGz.trimOrNull(".tsv");
    if (sourceSansTsv != null) {
      // Use simple table for conversion to avoid Arrow issues
      return new CsvTranslatableTable(source, null);
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

    // If refresh is configured, use refreshable table
    if (effectiveInterval != null && tableName != null) {
      return new RefreshableCsvTable(source, tableName, protoRowType, effectiveInterval);
    }

    // Otherwise use standard tables
    switch (engineConfig.getEngineType()) {
    case VECTORIZED:
      return new EnhancedCsvTranslatableTable(source, protoRowType, engineConfig, columnNameCasing);
    case PARQUET:
      // For PARQUET engine, return a regular CSV table that will be converted to Parquet
      // The conversion happens in addTable() method
      return new CsvTranslatableTable(source, protoRowType, columnNameCasing);
    case ARROW:
    case LINQ4J:
    default:
      return new CsvTranslatableTable(source, protoRowType, columnNameCasing);
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
        return new EnhancedJsonScannableTable(source, engineConfig, options);
      }
      return new EnhancedJsonScannableTable(source, engineConfig, options);
    case PARQUET:
      if (effectiveInterval != null && tableName != null) {
        return new RefreshableJsonTable(source, tableName, effectiveInterval);
      }
      return new ParquetJsonScannableTable(source, engineConfig, options);
    case ARROW:
    case LINQ4J:
    default:
      if (effectiveInterval != null && tableName != null) {
        return new RefreshableJsonTable(source, tableName, effectiveInterval);
      }
      return new JsonScannableTable(source, options);
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
          System.out.println("No files found matching pattern: " + config.getPattern());
          continue;
        }

        System.out.println("Found " + matchingFiles.size() + " files for partitioned table: "
            + config.getName());

        // Detect or apply partition scheme
        PartitionDetector.PartitionInfo partitionInfo = null;

        if (config.getPartitions() == null
            || "auto".equals(config.getPartitions().getStyle())) {
          // Auto-detect partition scheme
          partitionInfo =
              PartitionDetector.detectPartitionScheme(matchingFiles);
          if (partitionInfo == null) {
            System.out.println(
                "WARN: No partition scheme detected for table '"
                + config.getName() + "'. Treating as unpartitioned. "
                + "Query performance may be impacted.");
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
        System.err.println("Failed to process partitioned table: " + e.getMessage());
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
      System.err.println("Error scanning for files with pattern: " + pattern);
      e.printStackTrace();
    }

    return result;
  }

  /**
   * Gets files for processing, always using glob patterns for consistency.
   * Converts plain directories to appropriate glob patterns.
   */
  private File[] getFilesForProcessing() {
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
}
