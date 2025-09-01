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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.converters.HtmlToJsonConverter;
import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.refresh.RefreshableTable;
import org.apache.calcite.adapter.file.refresh.RefreshableTable.RefreshBehavior;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Sources;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Table implementation that reads files matching a glob pattern and caches
 * the result as a Parquet file for efficient querying.
 *
 * <p>Supports preprocessing of Excel and HTML files to JSON before
 * consolidation into Parquet.
 */
public class GlobParquetTable extends AbstractTable
    implements RefreshableTable, ScannableTable {

  private static final Logger LOGGER = Logger.getLogger(GlobParquetTable.class.getName());

  private final String globPattern;
  private final String tableName;
  private final File cacheDir;
  private final @Nullable Duration refreshInterval;
  private final BufferAllocator allocator;
  private final String columnNameCasing;

  private File parquetCacheFile;
  private @Nullable Instant lastRefreshTime;
  private final AtomicBoolean cacheValid = new AtomicBoolean(false);
  private @Nullable RelProtoDataType protoRowType;
  private final CsvTypeInferrer.TypeInferenceConfig csvTypeInferenceConfig;

  /**
   * Cache metadata for glob pattern matching results.
   */
  private static class CacheMetadata {
    final String globPattern;
    final List<FileInfo> sourceFiles;
    final Instant created;

    /**
     * Information about a single file.
     */
    static class FileInfo {
      final String path;
      final long size;
      final long lastModified;

      FileInfo(File file) {
        this.path = file.getAbsolutePath();
        this.size = file.length();
        this.lastModified = file.lastModified();
      }

      boolean hasChanged(File file) {
        return file.length() != size || file.lastModified() != lastModified;
      }
    }

    CacheMetadata(String pattern, List<File> files) {
      this.globPattern = pattern;
      this.sourceFiles = files.stream()
          .map(FileInfo::new)
          .collect(Collectors.toList());
      this.created = Instant.now();
    }

    boolean needsUpdate(List<File> currentFiles) {
      if (currentFiles.size() != sourceFiles.size()) {
        return true;
      }

      // Check if any file has changed
      for (File file : currentFiles) {
        boolean found = false;
        for (FileInfo info : sourceFiles) {
          if (info.path.equals(file.getAbsolutePath())) {
            if (info.hasChanged(file)) {
              return true;
            }
            found = true;
            break;
          }
        }
        if (!found) {
          return true; // New file
        }
      }

      return false;
    }
  }

  private @Nullable CacheMetadata cacheMetadata;

  public GlobParquetTable(String globPattern, String tableName, File cacheDir,
      @Nullable Duration refreshInterval, CsvTypeInferrer.TypeInferenceConfig csvTypeInferenceConfig,
      String columnNameCasing) {
    this.globPattern = globPattern;
    this.tableName = tableName;
    this.cacheDir = cacheDir;
    this.refreshInterval = refreshInterval;
    this.csvTypeInferenceConfig = csvTypeInferenceConfig;
    this.columnNameCasing = columnNameCasing;
    this.allocator = new RootAllocator();

    // Initialize cache file path
    String cacheFileName = generateCacheFileName(globPattern);
    this.parquetCacheFile = new File(cacheDir, cacheFileName);
  }

  private String generateCacheFileName(String pattern) {
    try {
      // Use hash of pattern for filename
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] digest = md.digest(pattern.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (byte b : digest) {
        sb.append(String.format(Locale.ROOT, "%02x", b));
      }
      return tableName + "_" + sb.substring(0, 8) + ".parquet";
    } catch (NoSuchAlgorithmException e) {
      // Fallback to simple name
      return tableName + "_cache.parquet";
    }
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

    if (lastRefreshTime == null || !cacheValid.get()) {
      return true;
    }

    return Instant.now().isAfter(lastRefreshTime.plus(refreshInterval));
  }

  @Override public void refresh() {
    if (!needsRefresh()) {
      return;
    }

    try {
      // Check if source files have changed
      List<File> currentFiles = findMatchingFiles();

      if (cacheMetadata == null || cacheMetadata.needsUpdate(currentFiles)) {
        regenerateCache();
      }

      lastRefreshTime = Instant.now();
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Failed to refresh glob cache", e);
      cacheValid.set(false);
    }
  }

  @Override public RefreshBehavior getRefreshBehavior() {
    return RefreshBehavior.DIRECTORY_SCAN;
  }

  private void regenerateCache() throws IOException {
    LOGGER.info("Regenerating Parquet cache for glob: " + globPattern);

    // Step 1: Find all matching files
    List<File> matchingFiles = findMatchingFiles();
    LOGGER.info("Found " + matchingFiles.size() + " files matching pattern");

    // Step 2: Preprocess Excel and HTML files
    List<File> allFiles = preprocessFiles(matchingFiles);
    LOGGER.info("Total files after preprocessing: " + allFiles.size());

    // Step 3: Create Arrow Dataset from all files
    List<String> filePaths = allFiles.stream()
        .map(File::getAbsolutePath)
        .collect(Collectors.toList());

    if (filePaths.isEmpty()) {
      LOGGER.warning("No files to process for glob: " + globPattern);
      return;
    }

    // Step 4: Use Arrow to read and write to Parquet
    writeToParquet(filePaths);

    // Update metadata
    cacheMetadata = new CacheMetadata(globPattern, matchingFiles);
    cacheValid.set(true);

    LOGGER.info("Successfully generated Parquet cache: " + parquetCacheFile.getAbsolutePath());
  }

  private List<File> findMatchingFiles() throws IOException {
    java.nio.file.Path basePath;
    String pattern;

    // Extract base path and pattern
    int lastSeparator = globPattern.lastIndexOf(File.separator);
    if (lastSeparator >= 0) {
      basePath = Paths.get(globPattern.substring(0, lastSeparator));
      pattern = globPattern.substring(lastSeparator + 1);
    } else {
      basePath = Paths.get(".");
      pattern = globPattern;
    }

    // Convert glob pattern to regex
    String regex = pattern.replace(".", "\\.")
                         .replace("*", ".*")
                         .replace("?", ".");

    List<File> files = new ArrayList<>();

    // Walk directory tree for ** patterns
    if (globPattern.contains("**")) {
      Files.walk(basePath)
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().matches(regex))
          .forEach(path -> files.add(path.toFile()));
    } else {
      // Simple directory listing
      File dir = basePath.toFile();
      if (dir.exists() && dir.isDirectory()) {
        File[] matchingFiles = dir.listFiles((d, name) -> name.matches(regex));
        if (matchingFiles != null) {
          files.addAll(Arrays.asList(matchingFiles));
        }
      }
    }

    return files;
  }

  private List<File> preprocessFiles(List<File> files) throws IOException {
    List<File> allFiles = new ArrayList<>();

    for (File file : files) {
      String fileName = file.getName().toLowerCase(Locale.ROOT);

      if (fileName.endsWith(".xlsx") || fileName.endsWith(".xls")) {
        // Excel files - use existing converter (placeholder for now)
        LOGGER.fine("Excel file found: " + file.getName());
        allFiles.add(file);
      } else if (fileName.endsWith(".html") || fileName.endsWith(".htm")) {
        // HTML files - use new converter
        LOGGER.fine("Preprocessing HTML file: " + file.getName());
        // Use conversions subdirectory under cacheDir, not source directory
        File conversionsDir = new File(cacheDir, "conversions");
        if (!conversionsDir.exists()) {
          conversionsDir.mkdirs();
        }
        List<File> jsonFiles =
            HtmlToJsonConverter.convert(file, conversionsDir, columnNameCasing, cacheDir);
        allFiles.addAll(jsonFiles);
      } else {
        // Other files (CSV, JSON, Parquet) - add directly
        allFiles.add(file);
      }
    }

    return allFiles;
  }

  private void writeToParquet(List<String> filePaths) throws IOException {
    // Ensure cache directory exists
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }

    // Create temporary file for atomic write
    File tempFile = new File(cacheDir, parquetCacheFile.getName() + ".tmp");

    try {
      if (filePaths.size() == 1) {
        // Single file processing
        File singleFile = new File(filePaths.get(0));
        if (singleFile.getName().endsWith(".parquet")) {
          // Single Parquet file - just copy
          Files.copy(singleFile.toPath(), tempFile.toPath());
        } else if (singleFile.getName().endsWith(".json")) {
          // Convert JSON to Parquet
          convertJsonToParquet(singleFile, tempFile);
        } else if (singleFile.getName().endsWith(".csv")) {
          // Convert CSV to Parquet
          convertCsvToParquet(singleFile, tempFile);
        } else {
          // Unknown format
          throw new IOException("Unsupported file format: " + singleFile.getName());
        }
      } else {
        // Multiple files - need to merge them
        mergeFilesToParquet(filePaths, tempFile);
      }

      // Atomic rename
      if (parquetCacheFile.exists()) {
        parquetCacheFile.delete();
      }
      tempFile.renameTo(parquetCacheFile);

    } catch (Exception e) {
      // Clean up temp file on error
      if (tempFile.exists()) {
        tempFile.delete();
      }
      throw new IOException("Failed to write Parquet cache", e);
    }
  }

  private void convertJsonToParquet(File jsonFile, File parquetFile) throws IOException {
    LOGGER.info("Converting JSON to Parquet: " + jsonFile.getName());
    try {
      // Create a temporary JSON table to read from
      JsonScannableTable jsonTable = new JsonScannableTable(Sources.of(jsonFile));
      String tableName = Pattern.compile("\\.json$")
          .matcher(jsonFile.getName()).replaceAll("")
          .toUpperCase(Locale.ROOT);

      // Convert using Calcite's query engine
      try (Connection conn = DriverManager.getConnection("jdbc:calcite:");
           CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

        SchemaPlus rootSchema = calciteConn.getRootSchema();
        SchemaPlus tempSchema = rootSchema.add("TEMP_CONVERT", new AbstractSchema() {
          @Override protected Map<String, org.apache.calcite.schema.Table> getTableMap() {
            return Collections.singletonMap(tableName, jsonTable);
          }
        });

        // Use ParquetConversionUtil's conversion directly
        File result =
            ParquetConversionUtil.convertToParquet(Sources.of(jsonFile),
            tableName,
            jsonTable,
            parquetFile.getParentFile(),
            tempSchema,
            "TEMP_CONVERT",
            "TO_LOWER");

        // Move the result to our target location if different
        if (!result.equals(parquetFile)) {
          Files.move(result.toPath(), parquetFile.toPath(),
              java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
      }
    } catch (Exception e) {
      throw new IOException("Failed to convert JSON to Parquet", e);
    }
  }

  private void convertCsvToParquet(File csvFile, File parquetFile) throws IOException {
    LOGGER.info("Converting CSV to Parquet: " + csvFile.getName());
    try {
      // Create a CSV table to read from with proper CSV configuration
      CsvTranslatableTable csvTable = new CsvTranslatableTable(Sources.of(csvFile), null, "TO_LOWER", csvTypeInferenceConfig);
      String tableName = Pattern.compile("\\.csv$")
          .matcher(csvFile.getName()).replaceAll("")
          .toUpperCase(Locale.ROOT);

      // Convert using Calcite's query engine
      try (Connection conn = DriverManager.getConnection("jdbc:calcite:");
           CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

        SchemaPlus rootSchema = calciteConn.getRootSchema();
        SchemaPlus tempSchema = rootSchema.add("TEMP_CONVERT", new AbstractSchema() {
          @Override protected Map<String, org.apache.calcite.schema.Table> getTableMap() {
            return Collections.singletonMap(tableName, csvTable);
          }
        });

        // Use ParquetConversionUtil's conversion directly
        File result =
            ParquetConversionUtil.convertToParquet(Sources.of(csvFile),
            tableName,
            csvTable,
            parquetFile.getParentFile(),
            tempSchema,
            "TEMP_CONVERT",
            "TO_LOWER");

        // Move the result to our target location if different
        if (!result.equals(parquetFile)) {
          Files.move(result.toPath(), parquetFile.toPath(),
              java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
      }
    } catch (Exception e) {
      throw new IOException("Failed to convert CSV to Parquet", e);
    }
  }

  private void mergeFilesToParquet(List<String> filePaths, File parquetFile) throws IOException {
    LOGGER.info("Merging " + filePaths.size() + " files to Parquet");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn.getRootSchema();

      // Create a custom table that represents the UNION ALL of all files
      org.apache.calcite.schema.Table unionTable = new AbstractTable() {
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          // Use the first file to determine schema
          File firstFile = new File(filePaths.get(0));
          if (firstFile.getName().endsWith(".csv")) {
            return new CsvTranslatableTable(Sources.of(firstFile), null).getRowType(typeFactory);
          } else if (firstFile.getName().endsWith(".json")) {
            return new JsonScannableTable(Sources.of(firstFile)).getRowType(typeFactory);
          } else {
            throw new RuntimeException("Unsupported file type: " + firstFile.getName());
          }
        }
      };

      // Create temporary schema with union table
      SchemaPlus tempSchema = rootSchema.add("TEMP_MERGE", new AbstractSchema() {
        @Override protected Map<String, org.apache.calcite.schema.Table> getTableMap() {
          // Add each file as a separate table
          Map<String, org.apache.calcite.schema.Table> tables = new HashMap<>();
          for (int i = 0; i < filePaths.size(); i++) {
            File file = new File(filePaths.get(i));
            String tableName = "FILE_" + i;

            if (file.getName().endsWith(".csv")) {
              tables.put(tableName, new CsvTranslatableTable(Sources.of(file), null));
            } else if (file.getName().endsWith(".json")) {
              tables.put(tableName, new JsonScannableTable(Sources.of(file)));
            }
          }

          // Add a combined view
          tables.put("ALL_FILES", unionTable);
          return tables;
        }
      });

      // Build UNION ALL query to merge all tables
      StringBuilder unionQuery = new StringBuilder();
      unionQuery.append("SELECT * FROM (");
      for (int i = 0; i < filePaths.size(); i++) {
        if (i > 0) {
          unionQuery.append(" UNION ALL ");
        }
        unionQuery.append("SELECT * FROM \"TEMP_MERGE\".\"FILE_").append(i).append("\"");
      }
      unionQuery.append(") AS MERGED");

      // Use ParquetConversionUtil to convert the query result to Parquet
      String mergedTableName = "MERGED_" + System.currentTimeMillis();

      /**
       * Merged table that executes union query over multiple files.
       */
      class MergedTable extends AbstractTable implements ScannableTable {

        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return unionTable.getRowType(typeFactory);
        }

        @Override public Enumerable<Object[]> scan(DataContext root) {
          try (Statement stmt = calciteConn.createStatement();
               ResultSet rs = stmt.executeQuery(unionQuery.toString())) {
            List<Object[]> rows = new ArrayList<>();
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
              Object[] row = new Object[columnCount];
              for (int i = 0; i < columnCount; i++) {
                row[i] = rs.getObject(i + 1);
              }
              rows.add(row);
            }
            return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
          } catch (Exception e) {
            throw new RuntimeException("Failed to execute union query", e);
          }
        }
      }

      org.apache.calcite.schema.Table mergedTable = new MergedTable();

      // Use ParquetConversionUtil to convert the merged table
      File result =
          ParquetConversionUtil.convertToParquet(
              Sources.of(parquetFile), // Dummy source, not used for path
              mergedTableName,
              mergedTable,
              parquetFile.getParentFile(),
              tempSchema,
              "TEMP_MERGE",
              "TO_LOWER");

      // Move result if needed
      if (!result.equals(parquetFile)) {
        Files.move(result.toPath(), parquetFile.toPath(),
            java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      }

    } catch (Exception e) {
      throw new IOException("Failed to merge files to Parquet", e);
    }
  }


  @Override public Enumerable<Object[]> scan(DataContext root) {
    ensureCacheValid();

    // Create a ParquetScannableTable to read the cache file
    ParquetScannableTable parquetTable = new ParquetScannableTable(parquetCacheFile);

    // Return the full enumerable from the Parquet table
    return parquetTable.scan(root);
  }

  public Enumerable<Object> project(final DataContext root, final int[] projects) {
    // Use scan method to get the base enumerable
    Enumerable<Object[]> fullEnumerable = scan(root);

    // Convert to Enumerable<Object> and apply projection if needed
    return new AbstractEnumerable<Object>() {
      @Override public Enumerator<Object> enumerator() {
        final Enumerator<Object[]> arrayEnumerator = fullEnumerable.enumerator();

        return new Enumerator<Object>() {
          @Override public Object current() {
            Object[] row = arrayEnumerator.current();
            if (row == null) {
              return null;
            }

            // Apply projection if specified
            if (projects != null && projects.length > 0) {
              Object[] projected = new Object[projects.length];
              for (int i = 0; i < projects.length; i++) {
                if (projects[i] < row.length) {
                  projected[i] = row[projects[i]];
                }
              }
              return projected;
            }

            return row;
          }

          @Override public boolean moveNext() {
            return arrayEnumerator.moveNext();
          }

          @Override public void reset() {
            arrayEnumerator.reset();
          }

          @Override public void close() {
            arrayEnumerator.close();
          }
        };
      }
    };
  }

  private void ensureCacheValid() {
    refresh();

    if (!cacheValid.get() || !parquetCacheFile.exists()) {
      try {
        regenerateCache();
      } catch (IOException e) {
        throw new RuntimeException("Failed to generate Parquet cache", e);
      }
    }
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }

    // Ensure cache exists to infer schema
    ensureCacheValid();

    // Use ParquetScannableTable to get the proper schema
    ParquetScannableTable parquetTable = new ParquetScannableTable(parquetCacheFile);
    return parquetTable.getRowType(typeFactory);
  }

  @Override public String toString() {
    return "GlobParquetTable{" + globPattern + "}";
  }
}
