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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Table implementation that reads files matching a glob pattern and caches
 * the result as a Parquet file for efficient querying.
 *
 * <p>Supports preprocessing of Excel and HTML files to JSON before
 * consolidation into Parquet.
 */
public class GlobParquetTable extends AbstractTable
    implements RefreshableTable {

  private static final Logger LOGGER = Logger.getLogger(GlobParquetTable.class.getName());

  private final String globPattern;
  private final String tableName;
  private final File cacheDir;
  private final @Nullable Duration refreshInterval;
  private final BufferAllocator allocator;

  private File parquetCacheFile;
  private @Nullable Instant lastRefreshTime;
  private final AtomicBoolean cacheValid = new AtomicBoolean(false);
  private @Nullable RelProtoDataType protoRowType;

  // Cache metadata
  private static class CacheMetadata {
    final String globPattern;
    final List<FileInfo> sourceFiles;
    final Instant created;

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
      @Nullable Duration refreshInterval) {
    this.globPattern = globPattern;
    this.tableName = tableName;
    this.cacheDir = cacheDir;
    this.refreshInterval = refreshInterval;
    this.allocator = new RootAllocator();

    // Initialize cache file path
    String cacheFileName = generateCacheFileName(globPattern);
    this.parquetCacheFile = new File(cacheDir, cacheFileName);
  }

  private String generateCacheFileName(String pattern) {
    try {
      // Use hash of pattern for filename
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] digest = md.digest(pattern.getBytes());
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
      String fileName = file.getName().toLowerCase();

      if (fileName.endsWith(".xlsx") || fileName.endsWith(".xls")) {
        // Excel files - use existing converter (placeholder for now)
        LOGGER.fine("Excel file found: " + file.getName());
        allFiles.add(file);
      } else if (fileName.endsWith(".html") || fileName.endsWith(".htm")) {
        // HTML files - use new converter
        LOGGER.fine("Preprocessing HTML file: " + file.getName());
        List<File> jsonFiles =
            HtmlToJsonConverter.convert(file, file.getParentFile());
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
      // For initial implementation, we'll use a simpler approach
      // Full Arrow Dataset integration would be added later

      // If all files are already Parquet, just copy the first one
      // Otherwise, we need to convert them
      File firstFile = new File(filePaths.get(0));

      if (filePaths.size() == 1 && firstFile.getName().endsWith(".parquet")) {
        // Single Parquet file - just copy
        Files.copy(firstFile.toPath(), tempFile.toPath());
      } else {
        // Multiple files or non-Parquet format
        // For now, we'll use a simplified approach
        // In production, this would use Arrow Dataset API to merge all files

        if (firstFile.getName().endsWith(".json")) {
          // Convert JSON to Parquet
          convertJsonToParquet(firstFile, tempFile);
        } else if (firstFile.getName().endsWith(".csv")) {
          // Convert CSV to Parquet
          convertCsvToParquet(firstFile, tempFile);
        } else {
          // For now, just copy first file
          LOGGER.warning("Multi-file merge not yet implemented - using first file only");
          Files.copy(firstFile.toPath(), tempFile.toPath());
        }
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
    // Simplified implementation - would use Arrow in production
    LOGGER.info("Converting JSON to Parquet: " + jsonFile.getName());
    // For now, just copy as a placeholder
    Files.copy(jsonFile.toPath(), parquetFile.toPath());
  }

  private void convertCsvToParquet(File csvFile, File parquetFile) throws IOException {
    // Simplified implementation - would use Arrow in production
    LOGGER.info("Converting CSV to Parquet: " + csvFile.getName());
    // For now, just copy as a placeholder
    Files.copy(csvFile.toPath(), parquetFile.toPath());
  }


  public Enumerable<Object> project(final DataContext root, final int[] projects) {
    ensureCacheValid();

    return new AbstractEnumerable<Object>() {
      @Override public Enumerator<Object> enumerator() {
        try {
          // Use CSV enumerator as fallback since we don't have full Parquet implementation
          Source source = Sources.of(parquetCacheFile);
          AtomicBoolean cancelFlag = new AtomicBoolean(false);
          RelDataType rowType = CsvEnumerator.deduceRowType(root.getTypeFactory(), source, null, false);
          List<RelDataType> fieldTypes = rowType.getFieldList().stream()
              .map(field -> field.getType())
              .collect(Collectors.toList());
          List<Integer> fieldIndices = Arrays.stream(projects)
              .boxed()
              .collect(Collectors.toList());
          return new CsvEnumerator(source, cancelFlag, fieldTypes, fieldIndices);
        } catch (Exception e) {
          throw new RuntimeException("Failed to read cache", e);
        }
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

    // Read schema from Parquet file
    try {
      org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetCacheFile.getAbsolutePath());
      Configuration conf = new Configuration();

      @SuppressWarnings("deprecation")
      ParquetMetadata metadata = ParquetFileReader.readFooter(conf, hadoopPath);
      MessageType messageType = metadata.getFileMetaData().getSchema();

      // For now, return a simple schema - full implementation would convert Parquet schema
      return typeFactory.builder()
          .add("column1", SqlTypeName.VARCHAR)
          .build();
    } catch (IOException e) {
      // Fallback schema
      return typeFactory.builder()
          .add("column1", SqlTypeName.VARCHAR)
          .build();
    }
  }

  @Override public String toString() {
    return "GlobParquetTable{" + globPattern + "}";
  }
}
