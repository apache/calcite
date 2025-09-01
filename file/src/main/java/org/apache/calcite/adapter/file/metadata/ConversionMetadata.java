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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.converters.FileConversionManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Tracks the lineage of converted files (Excel→JSON, HTML→JSON, etc.).
 * This metadata persists across restarts so RefreshableTable can monitor
 * the correct original source files.
 */
public class ConversionMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConversionMetadata.class);
  private static final String METADATA_FILE = ".conversions.json";
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT);

  // Intra-JVM synchronization: map of locks per metadata file path
  // This ensures threads within the same JVM coordinate properly
  private static final ConcurrentHashMap<String, ReentrantReadWriteLock> JVM_LOCKS =
      new ConcurrentHashMap<>();

  // Use FileConversionManager directly instead of static list

  /**
   * Generates the complete file extensions list by parsing FileConversionManager methods.
   * This uses the actual source of truth and includes all compression variants.
   */
  private static java.util.List<java.util.Map.Entry<String, String>> generateFileExtensions() {
    java.util.List<java.util.Map.Entry<String, String>> extensions = new java.util.ArrayList<>();

    // Generate test filenames dynamically from FileConversionManager
    java.util.List<String> testExtensions = new java.util.ArrayList<>();

    // Test common file types to see which ones are recognized
    String[] candidateExtensions = {
        "csv", "csv.gz", "tsv", "tsv.gz", "json", "json.gz", "parquet",
        "yaml", "yml", "arrow", "xlsx", "xls", "html", "htm",
        "xml", "md", "docx", "pptx", "gz", "gzip", "bz2", "xz", "zip"
    };

    for (String ext : candidateExtensions) {
      testExtensions.add("test." + ext);
    }

    for (String testFile : testExtensions) {
      String fileType = detectTypeFromTestFile(testFile);
      if (!fileType.equals("unknown")) {
        // Extract the extension part
        String extension = extractExtension(testFile);
        if (extension != null) {
          extensions.add(java.util.Map.entry(extension, fileType));
        }
      }
    }

    // Add compressed variants for directly usable types
    String[] baseExtensions = {"csv", "tsv", "json", "yaml", "yml", "arrow"};
    String[] compressionSuffixes = {"gz", "gzip", "bz2", "xz", "zip"};

    for (String base : baseExtensions) {
      String baseType = detectTypeFromExtension(base);
      if (!baseType.equals("unknown")) {
        for (String compression : compressionSuffixes) {
          extensions.add(java.util.Map.entry(base + "." + compression, baseType));
        }
      }
    }

    return extensions;
  }

  /**
   * Detects file type by using FileConversionManager methods.
   */
  private static String detectTypeFromTestFile(String filename) {
    // Use FileConversionManager's actual logic
    if (org.apache.calcite.adapter.file.converters.FileConversionManager.requiresConversion(filename)) {
      return detectConvertibleType(filename);
    } else if (org.apache.calcite.adapter.file.converters.FileConversionManager.isDirectlyUsable(filename)) {
      return detectDirectType(filename);
    }
    return "unknown";
  }

  private static String detectConvertibleType(String filename) {
    String lower = filename.toLowerCase();
    if (lower.contains(".xlsx")) return "excel";
    if (lower.contains(".xls")) return "excel";
    if (lower.contains(".html")) return "html";
    if (lower.contains(".htm")) return "html";
    if (lower.contains(".xml")) return "xml";
    if (lower.contains(".md")) return "markdown";
    if (lower.contains(".docx")) return "docx";
    if (lower.contains(".pptx")) return "pptx";
    return "unknown";
  }

  private static String detectDirectType(String filename) {
    String lower = filename.toLowerCase();
    if (lower.contains(".csv")) return "csv";
    if (lower.contains(".tsv")) return "tsv";
    if (lower.contains(".json")) return "json";
    if (lower.contains(".parquet")) return "parquet";
    if (lower.contains(".yaml")) return "yaml";
    if (lower.contains(".yml")) return "yaml";
    if (lower.contains(".arrow")) return "arrow";
    return "unknown";
  }

  private static String detectTypeFromExtension(String extension) {
    return detectTypeFromTestFile("test." + extension);
  }

  private static String extractExtension(String filename) {
    // Remove "test." prefix and return the extension part
    if (filename.startsWith("test.")) {
      return filename.substring(5);
    }
    return null;
  }

  private final File metadataFile;
  private final Map<String, ConversionRecord> conversions = new ConcurrentHashMap<>();

  /**
   * Comprehensive record of a table in the schema, tracking everything from original source to final table.
   * Despite the name "ConversionRecord", this now tracks ALL tables, not just converted ones.
   */
  public static class ConversionRecord {
    // === CORE IDENTIFICATION ===
    public String tableName;              // Table name as it appears in schema
    public String tableType;              // Table implementation class name (ParquetTranslatableTable, etc.)
    public String sourceFile;            // Direct source file used by table
    public String sourceType;            // File type: "csv", "json", "parquet", "excel", etc.

    // === LINEAGE CHAIN (for converted files) ===
    public String originalFile;          // Ultimate original source (e.g., Excel file)
    public String convertedFile;         // Intermediate converted file (e.g., JSON from Excel)
    public String conversionType;        // Conversion performed (EXCEL_TO_JSON, HTML_TO_JSON, DIRECT, etc.)

    // === CACHING AND PERFORMANCE ===
    public String parquetCacheFile;      // Parquet cache file path (when using PARQUET engine)
    public Boolean refreshEnabled;       // Whether table has refresh capability
    public String refreshInterval;       // Refresh interval if applicable

    // === CHANGE DETECTION METADATA ===
    public long timestamp;               // Last update timestamp
    public String etag;                  // ETag for HTTP/S3 sources
    public Long contentLength;           // File size for change detection
    public String contentType;           // MIME type

    // === CONFIGURATION ===
    public java.util.Map<String, Object> tableConfig; // Original table definition from model.json

    public ConversionRecord() {} // For Jackson

    public ConversionRecord(String originalFile, String convertedFile,
        String conversionType) {
      this.originalFile = originalFile;
      this.convertedFile = convertedFile;
      this.conversionType = conversionType;
      this.timestamp = System.currentTimeMillis();
    }

    public ConversionRecord(String originalFile, String convertedFile,
        String conversionType, String parquetCacheFile) {
      this.originalFile = originalFile;
      this.convertedFile = convertedFile;
      this.conversionType = conversionType;
      this.parquetCacheFile = parquetCacheFile;
      this.timestamp = System.currentTimeMillis();
    }

    /**
     * Constructor with HTTP metadata for change detection.
     */
    public ConversionRecord(String originalFile, String convertedFile,
        String conversionType, String parquetCacheFile,
        String etag, Long contentLength, String contentType) {
      this.originalFile = originalFile;
      this.convertedFile = convertedFile;
      this.conversionType = conversionType;
      this.parquetCacheFile = parquetCacheFile;
      this.timestamp = System.currentTimeMillis();
      this.etag = etag;
      this.contentLength = contentLength;
      this.contentType = contentType;
    }

    /**
     * Comprehensive constructor for table tracking.
     */
    public ConversionRecord(String tableName, String tableType, String sourceFile, String sourceType,
        String originalFile, String convertedFile, String conversionType,
        String parquetCacheFile, Boolean refreshEnabled, String refreshInterval,
        String etag, Long contentLength, String contentType,
        java.util.Map<String, Object> tableConfig) {
      this.tableName = tableName;
      this.tableType = tableType;
      this.sourceFile = sourceFile;
      this.sourceType = sourceType;
      this.originalFile = originalFile;
      this.convertedFile = convertedFile;
      this.conversionType = conversionType;
      this.parquetCacheFile = parquetCacheFile;
      this.refreshEnabled = refreshEnabled;
      this.refreshInterval = refreshInterval;
      this.timestamp = System.currentTimeMillis();
      this.etag = etag;
      this.contentLength = contentLength;
      this.contentType = contentType;
      this.tableConfig = tableConfig;
    }

    @com.fasterxml.jackson.annotation.JsonIgnore
    public String getOriginalPath() {
      return originalFile;
    }

    public String getConversionType() {
      return conversionType;
    }

    public String getTableName() {
      return tableName;
    }

    public String getSourceFile() {
      return sourceFile;
    }

    public String getParquetCacheFile() {
      return parquetCacheFile;
    }

    public String getConvertedFile() {
      return convertedFile;
    }

    /**
     * Checks if the original file has changed since this conversion record was created.
     * Uses appropriate change detection method based on file type (ETag vs timestamp).
     */
    @com.fasterxml.jackson.annotation.JsonIgnore
    public boolean hasChanged() {
      // For local files, use timestamp-based detection
      if (isLocalFile(originalFile)) {
        return hasChangedViaTimestamp();
      }

      // For remote files, we need metadata comparison - this requires StorageProvider
      // This method will be called from FileConversionManager with StorageProvider access
      return true; // Conservative: assume changed if we can't check properly
    }

    /**
     * Checks if file has changed using StorageProvider metadata (HTTP ETags, S3 ETags, etc.).
     */
    @com.fasterxml.jackson.annotation.JsonIgnore
    public boolean hasChangedViaMetadata(org.apache.calcite.adapter.file.storage.StorageProvider.FileMetadata currentMetadata) {
      if (currentMetadata == null) {
        return true; // No current metadata, assume changed
      }

      // Check ETags first (most reliable for HTTP, S3)
      if (etag != null && currentMetadata.getEtag() != null) {
        boolean etagChanged = !etag.equals(currentMetadata.getEtag());
        LOGGER.debug("ETag comparison for {}: stored={}, current={}, changed={}",
            originalFile, etag, currentMetadata.getEtag(), etagChanged);
        return etagChanged;
      }

      // Fallback to size + timestamp comparison
      if (contentLength != null && contentLength != currentMetadata.getSize()) {
        LOGGER.debug("Size changed for {}: stored={}, current={}",
            originalFile, contentLength, currentMetadata.getSize());
        return true;
      }

      // Compare timestamps (allow small differences for filesystem precision)
      long timeDiff = Math.abs(currentMetadata.getLastModified() - timestamp);
      boolean timestampChanged = timeDiff > 1000; // 1 second tolerance
      LOGGER.debug("Timestamp comparison for {}: stored={}, current={}, diff={}ms, changed={}",
          originalFile, timestamp, currentMetadata.getLastModified(), timeDiff, timestampChanged);

      return timestampChanged;
    }

    /**
     * Updates this record with fresh metadata from StorageProvider.
     */
    @com.fasterxml.jackson.annotation.JsonIgnore
    public void updateMetadata(org.apache.calcite.adapter.file.storage.StorageProvider.FileMetadata metadata) {
      if (metadata != null) {
        this.etag = metadata.getEtag();
        this.contentLength = metadata.getSize();
        this.contentType = metadata.getContentType();
        this.timestamp = metadata.getLastModified();
        LOGGER.debug("Updated metadata for {}: etag={}, size={}, timestamp={}",
            originalFile, etag, contentLength, timestamp);
      }
    }

    /**
     * Timestamp-based change detection for local files.
     */
    private boolean hasChangedViaTimestamp() {
      try {
        File file = new File(originalFile);
        if (!file.exists()) {
          return false; // File doesn't exist, no change possible
        }
        long currentTimestamp = file.lastModified();
        boolean changed = currentTimestamp > timestamp;
        LOGGER.debug("Local file timestamp check for {}: stored={}, current={}, changed={}",
            originalFile, timestamp, currentTimestamp, changed);
        return changed;
      } catch (Exception e) {
        LOGGER.warn("Failed to check timestamp for {}: {}", originalFile, e.getMessage());
        return true; // Conservative: assume changed on error
      }
    }

    private boolean isLocalFile(String path) {
      return path != null && !path.startsWith("http://") && !path.startsWith("https://")
          && !path.startsWith("s3://") && !path.startsWith("ftp://") && !path.startsWith("sftp://");
    }
  }

  // Removed centralized metadata directory methods - metadata is now stored directly in baseDirectory

  /**
   * Creates a metadata tracker for the given directory.
   * Always stores the .conversions.json file directly in the provided directory.
   */
  public ConversionMetadata(File directory) {
    // Always store metadata directly in the provided directory
    this.metadataFile = new File(directory, METADATA_FILE);
    LOGGER.debug("Using metadata file: {}", metadataFile);
    loadMetadata();
  }

  /**
   * Records a file conversion.
   *
   * @param originalFile The source file (e.g., Excel)
   * @param convertedFile The converted file (e.g., JSON)
   * @param conversionType The type of conversion
   */
  public void recordConversion(File originalFile, File convertedFile, String conversionType) {
    try {
      String key = convertedFile.getCanonicalPath();
      ConversionRecord record =
          new ConversionRecord(originalFile.getCanonicalPath(),
          convertedFile.getCanonicalPath(),
          conversionType);

      conversions.put(key, record);
      saveMetadata();

      LOGGER.debug("Recorded conversion: {} -> {} ({})",
          originalFile.getName(), convertedFile.getName(), conversionType);
    } catch (IOException e) {
      LOGGER.error("Failed to record conversion metadata", e);
    }
  }

  /**
   * Records a file conversion with cached file information.
   *
   * @param originalFile The source file (e.g., Excel)
   * @param convertedFile The converted file (e.g., JSON)
   * @param conversionType The type of conversion
   * @param parquetCacheFile The cached file (e.g., Parquet)
   */
  public void recordConversion(File originalFile, File convertedFile, String conversionType, File parquetCacheFile) {
    try {
      String key = convertedFile.getCanonicalPath();
      ConversionRecord record =
          new ConversionRecord(originalFile.getCanonicalPath(),
          convertedFile.getCanonicalPath(),
          conversionType,
          parquetCacheFile != null ? parquetCacheFile.getCanonicalPath() : null);

      conversions.put(key, record);
      saveMetadata();

      LOGGER.debug("Recorded conversion: {} -> {} -> {} ({})",
          originalFile.getName(), convertedFile.getName(),
          parquetCacheFile != null ? parquetCacheFile.getName() : "null", conversionType);
    } catch (IOException e) {
      LOGGER.error("Failed to record conversion metadata", e);
    }
  }

  /**
   * Records a conversion using a pre-built ConversionRecord.
   * This allows for storing HTTP metadata and other advanced information.
   *
   * @param convertedFile The converted file (used as the key)
   * @param record The complete conversion record with metadata
   */
  public void recordConversion(File convertedFile, ConversionRecord record) {
    try {
      String key = convertedFile.getCanonicalPath();
      conversions.put(key, record);
      saveMetadata();

      LOGGER.debug("Recorded conversion record: {} -> {} (type: {}, etag: {})",
          record.originalFile, record.convertedFile, record.conversionType, record.etag);
    } catch (IOException e) {
      LOGGER.error("Failed to record conversion record", e);
    }
  }

  /**
   * Records a comprehensive table with all metadata.
   * This is the main method for tracking ALL tables in the schema.
   *
   * @param tableName Table name as it appears in the schema
   * @param table The Table instance
   * @param source The Source used to create the table
   * @param tableDef The table definition from model.json (can be null)
   */
  public void recordTable(String tableName, org.apache.calcite.schema.Table table,
      org.apache.calcite.util.Source source, java.util.Map<String, Object> tableDef) {
    try {
      LOGGER.info("=== CONVERSION RECORD UPDATE TRACE === recordTable({})", tableName);
      LOGGER.info("Before update: conversions map size = {}", conversions.size());

      // Extract metadata from table and source
      TableMetadata metadata = extractTableMetadata(tableName, table, source, tableDef);

      LOGGER.info("Extracted table metadata: tableName='{}', tableType='{}', sourceFile='{}', convertedFile='{}'",
          metadata.tableName, metadata.tableType, metadata.sourceFile, metadata.convertedFile);

      // Create comprehensive record
      ConversionRecord record =
          new ConversionRecord(metadata.tableName,
          metadata.tableType,
          metadata.sourceFile,
          metadata.sourceType,
          metadata.originalFile,
          metadata.convertedFile,
          metadata.conversionType,
          metadata.parquetCacheFile,
          metadata.refreshEnabled,
          metadata.refreshInterval,
          metadata.etag,
          metadata.contentLength,
          metadata.contentType,
          metadata.tableConfig);

      LOGGER.info("Created new record: {}", formatRecord(record));

      // Check if record already exists for this table name
      ConversionRecord existingRecord = conversions.get(tableName);
      LOGGER.info("Existing record with tableName key '{}': {}", tableName,
          existingRecord != null ? formatRecord(existingRecord) : "null");

      if (existingRecord != null) {
        LOGGER.info("Updating existing record fields...");

        // IMPORTANT: Check if this is a conversion record (has convertedFile set)
        // If so, don't overwrite the conversion information
        boolean isConversionRecord = existingRecord.convertedFile != null &&
                                     existingRecord.conversionType != null &&
                                     !"DIRECT".equals(existingRecord.conversionType);

        if (isConversionRecord) {
          LOGGER.info("Preserving conversion record for table '{}' (conversionType={}, sourceFile={}, convertedFile={})",
              tableName, existingRecord.conversionType, existingRecord.sourceFile, existingRecord.convertedFile);
          // Only update table type if not set
          if (existingRecord.tableType == null) {
            LOGGER.info("Setting null tableType: null -> '{}'", metadata.tableType);
            existingRecord.tableType = metadata.tableType;
          }
          // Don't update sourceFile, originalFile, convertedFile, or conversionType for conversion records
        } else {
          // Normal update for non-conversion records
          // Only update basic table info if not already set
          if (existingRecord.tableType == null) {
            LOGGER.info("Setting null tableType: null -> '{}'", metadata.tableType);
            existingRecord.tableType = metadata.tableType;
          }
          if (existingRecord.sourceFile == null) {
            LOGGER.info("Setting null sourceFile: null -> '{}'", metadata.sourceFile);
            existingRecord.sourceFile = metadata.sourceFile;
          }
          if (existingRecord.sourceType == null) {
            LOGGER.info("Setting null sourceType: null -> '{}'", metadata.sourceType);
            existingRecord.sourceType = metadata.sourceType;
          }

          // Only update these fields if they're not already set (preserve existing values)
          if (existingRecord.originalFile == null) {
            LOGGER.info("Setting null originalFile: null -> '{}'", metadata.originalFile);
            existingRecord.originalFile = metadata.originalFile;
          }
          if (existingRecord.convertedFile == null) {
            LOGGER.info("Setting null convertedFile: null -> '{}'", metadata.convertedFile);
            existingRecord.convertedFile = metadata.convertedFile;
          }
          if (existingRecord.conversionType == null) {
            LOGGER.info("Setting null conversionType: null -> '{}'", metadata.conversionType);
            existingRecord.conversionType = metadata.conversionType;
          }
        }
        if (existingRecord.parquetCacheFile == null) {
          LOGGER.info("Setting null parquetCacheFile: null -> '{}'", metadata.parquetCacheFile);
          existingRecord.parquetCacheFile = metadata.parquetCacheFile;
        }
        if (existingRecord.etag == null) {
          LOGGER.info("Setting null etag: null -> '{}'", metadata.etag);
          existingRecord.etag = metadata.etag;
        }
        if (existingRecord.contentLength == null) {
          LOGGER.info("Setting null contentLength: null -> '{}'", metadata.contentLength);
          existingRecord.contentLength = metadata.contentLength;
        }
        if (existingRecord.contentType == null) {
          LOGGER.info("Setting null contentType: null -> '{}'", metadata.contentType);
          existingRecord.contentType = metadata.contentType;
        }

        // Always update refresh settings and table config as these are current operational settings
        LOGGER.info("Updating refreshEnabled: '{}' -> '{}'", existingRecord.refreshEnabled, metadata.refreshEnabled);
        LOGGER.info("Updating refreshInterval: '{}' -> '{}'", existingRecord.refreshInterval, metadata.refreshInterval);
        existingRecord.refreshEnabled = metadata.refreshEnabled;
        existingRecord.refreshInterval = metadata.refreshInterval;
        existingRecord.tableConfig = metadata.tableConfig;

        LOGGER.info("Updated existing record: {}", formatRecord(existingRecord));
      } else {
        // Create new record for comprehensive table tracking
        conversions.put(tableName, record);
        LOGGER.info("Stored new record under tableName key '{}'", tableName);
      }
      saveMetadata();

      LOGGER.info("After update: conversions map size = {}", conversions.size());
      LOGGER.info("Final record under tableName '{}': {}", tableName, formatRecord(conversions.get(tableName)));
      LOGGER.info("=== END CONVERSION RECORD UPDATE TRACE ===\\n");

    } catch (Exception e) {
      LOGGER.warn("Failed to record table metadata for {}: {}", tableName, e.getMessage());
      // Don't fail table creation just because metadata recording failed
    }
  }

  /**
   * Helper class to hold extracted table metadata.
   */
  private static class TableMetadata {
    String tableName;
    String tableType;
    String sourceFile;
    String sourceType;
    String originalFile;
    String convertedFile;
    String conversionType;
    String parquetCacheFile;
    Boolean refreshEnabled;
    String refreshInterval;
    String etag;
    Long contentLength;
    String contentType;
    java.util.Map<String, Object> tableConfig;
  }

  /**
   * Extracts comprehensive metadata from a table instance.
   */
  private TableMetadata extractTableMetadata(String tableName, org.apache.calcite.schema.Table table,
      org.apache.calcite.util.Source source, java.util.Map<String, Object> tableDef) {

    TableMetadata metadata = new TableMetadata();
    metadata.tableName = tableName;
    metadata.tableType = table.getClass().getSimpleName();
    metadata.sourceFile = source.path();
    metadata.sourceType = detectSourceType(source.path());
    metadata.tableConfig = tableDef;

    // Default values - will be overridden for converted tables
    metadata.originalFile = source.path();
    metadata.conversionType = "DIRECT";

    // Detect refresh capability
    metadata.refreshEnabled = table.getClass().getSimpleName().startsWith("Refreshable");
    if (metadata.refreshEnabled && tableDef != null) {
      metadata.refreshInterval = (String) tableDef.get("refreshInterval");
    }

    // Extract parquet cache file using reflection for ParquetTranslatableTable
    if (table.getClass().getSimpleName().equals("ParquetTranslatableTable")) {
      try {
        java.lang.reflect.Field fileField = table.getClass().getDeclaredField("parquetFile");
        fileField.setAccessible(true);
        java.io.File parquetFile = (java.io.File) fileField.get(table);
        if (parquetFile != null) {
          metadata.parquetCacheFile = parquetFile.getAbsolutePath();
          metadata.sourceFile = parquetFile.getAbsolutePath(); // Table reads from parquet, not original
          metadata.sourceType = "parquet";
        }
      } catch (Exception e) {
        LOGGER.debug("Could not extract parquet file from ParquetTranslatableTable: {}", e.getMessage());
      }
    }

    // Extract refresh interval from RefreshableParquetCacheTable
    if (table.getClass().getSimpleName().equals("RefreshableParquetCacheTable")) {
      try {
        java.lang.reflect.Method getRefreshIntervalMethod = table.getClass().getMethod("getRefreshInterval");
        Object interval = getRefreshIntervalMethod.invoke(table);
        if (interval != null) {
          metadata.refreshInterval = interval.toString();
        }
      } catch (Exception e) {
        LOGGER.debug("Could not extract refresh interval: {}", e.getMessage());
      }
    }

    // Extract parquet file information from partitioned tables
    if (table.getClass().getSimpleName().equals("RefreshablePartitionedParquetTable") ||
        table.getClass().getSimpleName().equals("PartitionedParquetTable")) {
      try {
        // For partitioned tables, get the file paths to use as parquet cache files
        // This allows DuckDB to discover the table structure and create views
        java.lang.reflect.Method getFilePathsMethod = table.getClass().getMethod("getFilePaths");

        @SuppressWarnings("unchecked")
        java.util.List<String> partitionFiles = (java.util.List<String>) getFilePathsMethod.invoke(table);
        if (partitionFiles != null && !partitionFiles.isEmpty()) {
          // For partitioned tables with multiple files, store as a glob pattern
          // DuckDB's parquet_scan() supports glob patterns directly
          if (partitionFiles.size() > 1) {
            // Find the common directory path
            java.io.File firstFile = new java.io.File(partitionFiles.get(0));
            java.io.File parentDir = firstFile.getParentFile();

            // Go up to find the root directory containing all partitions
            while (parentDir != null && !parentDir.getName().matches(".*sales.*|.*data.*")) {
              parentDir = parentDir.getParentFile();
            }

            if (parentDir == null) {
              // Fallback: use the parent of the first file's parent
              parentDir = firstFile.getParentFile().getParentFile();
            }

            // Create a glob pattern for all parquet files
            String globPattern = parentDir.getAbsolutePath() + "/**/*.parquet";
            metadata.parquetCacheFile = globPattern;
            LOGGER.info("Using glob pattern for partitioned table '{}': {}", tableName, globPattern);
          } else {
            // Single partition file
            metadata.parquetCacheFile = partitionFiles.get(0);
            LOGGER.debug("Extracted single parquet file for partitioned table '{}': {}", tableName, partitionFiles.get(0));
          }
        }

        // Extract refresh interval if it's a refreshable partitioned table
        if (table.getClass().getSimpleName().equals("RefreshablePartitionedParquetTable")) {
          try {
            java.lang.reflect.Method getRefreshIntervalMethod = table.getClass().getMethod("getRefreshInterval");
            Object interval = getRefreshIntervalMethod.invoke(table);
            if (interval != null) {
              metadata.refreshInterval = interval.toString();
            }
          } catch (Exception e) {
            LOGGER.debug("Could not extract refresh interval from partitioned table: {}", e.getMessage());
          }
        }
      } catch (Exception e) {
        LOGGER.debug("Could not extract parquet file from partitioned table '{}': {}", tableName, e.getMessage());
      }
    }

    // Try to get HTTP metadata for remote sources
    if (isRemoteFile(source.path())) {
      try {
        org.apache.calcite.adapter.file.storage.StorageProvider provider =
            org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(source.path());
        org.apache.calcite.adapter.file.storage.StorageProvider.FileMetadata fileMetadata =
            provider.getMetadata(source.path());
        metadata.etag = fileMetadata.getEtag();
        metadata.contentLength = fileMetadata.getSize();
        metadata.contentType = fileMetadata.getContentType();
      } catch (Exception e) {
        LOGGER.debug("Could not get remote file metadata for {}: {}", source.path(), e.getMessage());
      }
    }

    // Check for existing conversion record to get original file and conversion info
    ConversionRecord existingRecord = getConversionRecordBySourceFile(source.path());
    if (existingRecord != null) {
      metadata.originalFile = existingRecord.originalFile;
      metadata.convertedFile = existingRecord.convertedFile;
      metadata.conversionType = existingRecord.conversionType;
      // Preserve existing HTTP metadata
      if (metadata.etag == null) metadata.etag = existingRecord.etag;
      if (metadata.contentLength == null) metadata.contentLength = existingRecord.contentLength;
      if (metadata.contentType == null) metadata.contentType = existingRecord.contentType;
    }

    return metadata;
  }

  /**
   * Detects source file type using FileConversionManager as single source of truth.
   */
  private String detectSourceType(String filePath) {
    if (filePath == null) return "unknown";

    // Use FileConversionManager as the single source of truth
    if (FileConversionManager.requiresConversion(filePath)) {
      return detectConvertibleType(filePath);
    } else if (FileConversionManager.isDirectlyUsable(filePath)) {
      return detectDirectType(filePath);
    }

    return "unknown";
  }

  /**
   * Gets conversion record by source file path (for tracking lineage).
   */
  private ConversionRecord getConversionRecordBySourceFile(String sourceFile) {
    for (ConversionRecord record : conversions.values()) {
      if (sourceFile.equals(record.convertedFile)) {
        return record;
      }
    }
    return null;
  }

  /**
   * Determines if a file path is a remote URL.
   */
  private boolean isRemoteFile(String path) {
    return path != null && (
        path.startsWith("http://") || path.startsWith("https://") ||
        path.startsWith("s3://") || path.startsWith("ftp://") ||
        path.startsWith("sftp://"));
  }

  /**
   * Updates an existing conversion record to add cached file information.
   *
   * @param convertedFile The converted file that was cached
   * @param parquetCacheFile The cached file (e.g., Parquet)
   */
  public void updateCachedFile(File convertedFile, File parquetCacheFile) {
    try {
      String key = convertedFile.getCanonicalPath();
      ConversionRecord record = conversions.get(key);

      if (record != null) {
        record.parquetCacheFile = parquetCacheFile != null ? parquetCacheFile.getCanonicalPath() : null;
        saveMetadata();

        LOGGER.debug("Updated cached file for {}: {}",
            convertedFile.getName(), parquetCacheFile != null ? parquetCacheFile.getName() : "null");
      }
    } catch (IOException e) {
      LOGGER.error("Failed to update cached file metadata", e);
    }
  }

  /**
   * Builds a comprehensive mapping of all conversions across all schemas in a base directory.
   * This scans all .aperio/<schema>/.conversions.json files and creates a unified view.
   *
   * @param baseDirectory The base directory containing .aperio subdirectories
   * @param htmlFileToTableName Map of HTML filenames to explicit table names from model definitions
   * @return Map from generated file paths to explicit table names (where applicable)
   */
  public static Map<String, String> buildComprehensiveMapping(File baseDirectory, Map<String, String> htmlFileToTableName) {
    Map<String, String> fileToTableName = new HashMap<>();

    if (baseDirectory == null || !baseDirectory.exists()) {
      return fileToTableName;
    }

    File aperioDir = new File(baseDirectory, "." + FileSchema.BRAND);
    if (!aperioDir.exists() || !aperioDir.isDirectory()) {
      return fileToTableName;
    }

    LOGGER.info("Building comprehensive conversion mapping from: {}", aperioDir.getAbsolutePath());

    // Scan all schema directories under .aperio
    File[] schemaDirs = aperioDir.listFiles(File::isDirectory);
    LOGGER.info("Found {} schema directories under .{}", schemaDirs != null ? schemaDirs.length : 0, FileSchema.BRAND);

    if (schemaDirs != null) {
      for (File schemaDir : schemaDirs) {
        String schemaName = schemaDir.getName();
        LOGGER.info("Scanning schema directory: {}", schemaName);

        // Load conversion metadata for this schema
        ConversionMetadata metadata = new ConversionMetadata(schemaDir);
        LOGGER.info("Loaded {} conversion records from schema directory: {}", metadata.conversions.size(), schemaName);

        // Process each conversion record
        for (ConversionRecord record : metadata.conversions.values()) {
          if ("HTML_TO_JSON".equals(record.conversionType)) {
            // Extract table name from JSON file name pattern: htmlFileName__tableName.json
            File convertedFile = new File(record.convertedFile);
            String jsonFileName = convertedFile.getName();

            // First check if this JSON file name matches an explicit table name directly (like T1.json)
            if (jsonFileName.endsWith(".json")) {
              String jsonBaseName = jsonFileName.substring(0, jsonFileName.length() - 5); // remove .json

              // Check if this is an explicit table name from the HTML mapping
              for (Map.Entry<String, String> htmlEntry : htmlFileToTableName.entrySet()) {
                String explicitTableName = htmlEntry.getValue();
                if (jsonBaseName.equals(explicitTableName)) {
                  // This JSON file uses the explicit table name directly
                  LOGGER.info("=== DIRECT TABLE NAME MATCH === JSON file '{}' matches explicit table name '{}'",
                             jsonFileName, explicitTableName);

                  // Map the JSON file to explicit table name
                  fileToTableName.put(record.convertedFile, explicitTableName);

                  // Map cached Parquet file if it exists
                  if (record.parquetCacheFile != null) {
                    fileToTableName.put(record.parquetCacheFile, explicitTableName);
                    LOGGER.info("Mapped conversion files for explicit table '{}': JSON={}, Cached={}",
                               explicitTableName, record.convertedFile, record.parquetCacheFile);
                  } else {
                    // Map potential Parquet file
                    File parquetCacheDir = new File(schemaDir, ".parquet_cache");
                    File parquetFile = new File(parquetCacheDir, jsonBaseName + ".parquet");

                    try {
                      String parquetPath = parquetFile.getCanonicalPath();
                      fileToTableName.put(parquetPath, explicitTableName);
                      LOGGER.info("Mapped conversion files for explicit table '{}': JSON={}, Parquet={}",
                                 explicitTableName, record.convertedFile, parquetPath);
                    } catch (IOException e) {
                      LOGGER.warn("Failed to get canonical path for Parquet file: {}", parquetFile.getName(), e);
                    }
                  }
                  break; // Found a match, no need to check other patterns
                }
              }
            }

            // Also check for old pattern: htmlFileName__tableName.json
            if (jsonFileName.contains("__")) {
              String baseName = jsonFileName.substring(0, jsonFileName.length() - 5); // remove .json
              int underscoreIndex = baseName.lastIndexOf("__");
              if (underscoreIndex > 0) {
                String htmlFileName = baseName.substring(0, underscoreIndex);

                // Check if this HTML file has an explicit table name
                LOGGER.info("=== HTML MAPPING CHECK === Checking if htmlFileToTableName contains '{}' (size={})",
                           htmlFileName, htmlFileToTableName.size());
                for (Map.Entry<String, String> entry : htmlFileToTableName.entrySet()) {
                  LOGGER.info("=== HTML MAPPING ENTRY === '{}' -> '{}'", entry.getKey(), entry.getValue());
                }

                if (htmlFileToTableName.containsKey(htmlFileName)) {
                  String explicitTableName = htmlFileToTableName.get(htmlFileName);
                  LOGGER.info("=== FOUND EXPLICIT MAPPING === '{}' -> '{}'", htmlFileName, explicitTableName);

                  // Map the JSON file to explicit table name
                  fileToTableName.put(record.convertedFile, explicitTableName);

                  // Map cached Parquet file if it exists
                  if (record.parquetCacheFile != null) {
                    fileToTableName.put(record.parquetCacheFile, explicitTableName);
                    LOGGER.info("Mapped conversion files for explicit table '{}': JSON={}, Cached={}",
                               explicitTableName, record.convertedFile, record.parquetCacheFile);
                  } else {
                    // Fallback: map potential Parquet files (they follow similar naming)
                    // Parquet files are created with SMART_CASING which converts to snake_case
                    // Convert camelCase to snake_case for the parquet file name
                    String parquetName = baseName.replaceAll("([a-z])([A-Z]+)", "$1_$2").toLowerCase() + ".parquet";
                    File parquetCacheDir = new File(schemaDir, ".parquet_cache");
                    File parquetFile = new File(parquetCacheDir, parquetName);

                    try {
                      String parquetPath = parquetFile.getCanonicalPath();
                      fileToTableName.put(parquetPath, explicitTableName);
                      LOGGER.info("Mapped conversion files for explicit table '{}': JSON={}, Parquet={}",
                                 explicitTableName, record.convertedFile, parquetPath);
                    } catch (IOException e) {
                      LOGGER.warn("Failed to get canonical path for Parquet file: {}", parquetFile.getName(), e);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    return fileToTableName;
  }

  /**
   * Finds the original source file for a converted file.
   *
   * @param convertedFile The converted file (e.g., JSON)
   * @return The original source file, or null if not found
   */
  public File findOriginalSource(File convertedFile) {
    try {
      String key = convertedFile.getCanonicalPath();
      ConversionRecord record = conversions.get(key);

      if (record != null) {
        File originalFile = new File(record.originalFile);
        if (originalFile.exists()) {
          return originalFile;
        } else {
          // Original file was deleted or moved
          LOGGER.debug("Original source {} no longer exists", record.originalFile);
          conversions.remove(key);
          saveMetadata();
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to find original source", e);
    }

    return null;
  }

  /**
   * Finds all files that were derived from a given source file.
   * This is useful for finding JSONPath extractions that need to be refreshed
   * when the source file changes.
   *
   * @param sourceFile The source file to find derivatives for
   * @return List of derived files, empty if none found
   */
  public java.util.List<File> findDerivedFiles(File sourceFile) {
    java.util.List<File> derivedFiles = new java.util.ArrayList<>();

    try {
      String sourcePath = sourceFile.getCanonicalPath();

      for (ConversionRecord record : conversions.values()) {
        if (sourcePath.equals(record.originalFile)) {
          File derivedFile = new File(record.convertedFile);
          if (derivedFile.exists()) {
            derivedFiles.add(derivedFile);
          } else {
            // Clean up metadata for non-existent files
            LOGGER.debug("Derived file {} no longer exists", record.convertedFile);
          }
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to find derived files for {}", sourceFile.getName(), e);
    }

    return derivedFiles;
  }

  /**
   * Gets the conversion record for a converted file.
   *
   * @param convertedFile The converted file
   * @return The conversion record, or null if not found
   */
  public ConversionRecord getConversionRecord(File convertedFile) {
    try {
      String key = convertedFile.getCanonicalPath();
      return conversions.get(key);
    } catch (IOException e) {
      LOGGER.error("Failed to get conversion record", e);
      return null;
    }
  }

  /**
   * Finds a conversion record by source file path.
   * This is used by bulk conversion to find existing records to update.
   * Searches through all records regardless of key (table name, converted file, etc).
   *
   * @param sourceFile The original source file to look for
   * @return The conversion record, or null if not found
   */
  public ConversionRecord findRecordBySourceFile(File sourceFile) {
    try {
      String sourcePath = sourceFile.getCanonicalPath();
      LOGGER.debug("Looking for record with sourceFile: {}", sourcePath);

      // Search through all records to find one with matching source file
      // Records can be keyed by table name, converted file path, or source file path
      for (Map.Entry<String, ConversionRecord> entry : conversions.entrySet()) {
        ConversionRecord record = entry.getValue();
        LOGGER.debug("Checking record key='{}': originalFile='{}', sourceFile='{}'",
            entry.getKey(), record.originalFile, record.sourceFile);
        if (sourcePath.equals(record.originalFile) || sourcePath.equals(record.sourceFile)) {
          LOGGER.debug("Found matching record with key='{}'", entry.getKey());
          return record;
        }
      }

      LOGGER.debug("No matching record found for sourceFile: {}", sourcePath);
      return null;
    } catch (IOException e) {
      LOGGER.error("Failed to find conversion record by source file", e);
      return null;
    }
  }

  /**
   * Updates an existing conversion record, setting table name if null but preserving existing names.
   * This is used by bulk conversion to update records without losing explicit names.
   *
   * @param sourceFile The source file that was converted
   * @param convertedFile The new converted file
   * @param conversionType The type of conversion performed
   * @param baseDirectory The base directory for metadata storage
   * @param generatedTableName The generated table name to use if existing record has null table name
   */
  public void updateExistingRecord(File sourceFile, File convertedFile, String conversionType, File baseDirectory, String generatedTableName) {
    try {
      LOGGER.info("=== CONVERSION RECORD UPDATE TRACE === updateExistingRecord(sourceFile={}, generatedTableName={})", sourceFile.getName(), generatedTableName);
      LOGGER.info("Before update: conversions map size = {}", conversions.size());

      ConversionRecord existingRecord = findRecordBySourceFile(sourceFile);
      LOGGER.info("Found existing record by sourceFile: {}", existingRecord != null ? formatRecord(existingRecord) : "null");

      if (existingRecord != null) {
        String oldConvertedFile = existingRecord.convertedFile;
        String oldConversionType = existingRecord.conversionType;
        String oldTableName = existingRecord.tableName;

        // Update the existing record with new conversion information
        existingRecord.convertedFile = convertedFile.getCanonicalPath();
        existingRecord.conversionType = conversionType;

        LOGGER.info("Updated convertedFile: '{}' -> '{}'", oldConvertedFile, existingRecord.convertedFile);
        LOGGER.info("Updated conversionType: '{}' -> '{}'", oldConversionType, existingRecord.conversionType);

        // If table name is null, update it with the generated name; otherwise preserve existing name
        if (existingRecord.tableName == null) {
          existingRecord.tableName = generatedTableName;
          LOGGER.info("Updated null tableName: null -> '{}'", generatedTableName);
        } else {
          LOGGER.info("Preserved existing tableName: '{}'", existingRecord.tableName);
        }

        saveMetadata();

        LOGGER.info("Updated existing record: {}", formatRecord(existingRecord));
      } else {
        // No existing record found - create a new one with explicit table name
        if (generatedTableName != null) {
          LOGGER.info("Creating new conversion record with table name '{}'", generatedTableName);
          recordConversionWithTableName(generatedTableName, sourceFile, convertedFile, conversionType);
        } else {
          LOGGER.info("Creating new conversion record without table name");
          recordConversion(sourceFile, convertedFile, conversionType);
        }
      }

      LOGGER.info("After update: conversions map size = {}", conversions.size());
      LOGGER.info("=== END CONVERSION RECORD UPDATE TRACE ===\\n");

    } catch (IOException e) {
      LOGGER.error("Failed to update existing conversion record", e);
    }
  }

  /**
   * Records a conversion with an explicit table name, using the table name as the key.
   * This is used when converting HTML to JSON with explicit table definitions.
   *
   * @param tableName The explicit table name to use as the key
   * @param sourceFile The original source file
   * @param convertedFile The converted file
   * @param conversionType The type of conversion
   */
  public void recordConversionWithTableName(String tableName, File sourceFile, File convertedFile, String conversionType) {
    try {
      LOGGER.info("=== CONVERSION RECORD UPDATE TRACE === recordConversionWithTableName({})", tableName);
      LOGGER.info("Before update: conversions map size = {}", conversions.size());

      String convertedPath = convertedFile.getCanonicalPath();
      ConversionRecord existingTableNameRecord = conversions.get(tableName);
      ConversionRecord existingConvertedFileRecord = conversions.get(convertedPath);

      LOGGER.info("Existing record with tableName key '{}': {}", tableName,
          existingTableNameRecord != null ? formatRecord(existingTableNameRecord) : "null");
      LOGGER.info("Existing record with convertedFile key '{}': {}", convertedPath,
          existingConvertedFileRecord != null ? formatRecord(existingConvertedFileRecord) : "null");

      if (existingTableNameRecord != null) {
        // Update existing record with conversion information, preserving existing values
        LOGGER.info("Updating existing record with tableName key '{}'", tableName);

        String oldOriginalFile = existingTableNameRecord.originalFile;
        String oldSourceFile = existingTableNameRecord.sourceFile;
        String oldConvertedFile = existingTableNameRecord.convertedFile;
        String oldConversionType = existingTableNameRecord.conversionType;

        // Update conversion-related fields
        existingTableNameRecord.originalFile = sourceFile.getCanonicalPath();
        existingTableNameRecord.sourceFile = sourceFile.getCanonicalPath();
        existingTableNameRecord.convertedFile = convertedPath;
        existingTableNameRecord.conversionType = conversionType;

        LOGGER.info("Updated originalFile: '{}' -> '{}'", oldOriginalFile, existingTableNameRecord.originalFile);
        LOGGER.info("Updated sourceFile: '{}' -> '{}'", oldSourceFile, existingTableNameRecord.sourceFile);
        LOGGER.info("Updated convertedFile: '{}' -> '{}'", oldConvertedFile, existingTableNameRecord.convertedFile);
        LOGGER.info("Updated conversionType: '{}' -> '{}'", oldConversionType, existingTableNameRecord.conversionType);

        // Store under convertedFile key as an alternate key
        // convertedFile is a valid surrogate key when sourceFile is not null (which it is here)
        conversions.put(convertedPath, existingTableNameRecord);
        LOGGER.info("Stored updated record under convertedFile key '{}' (alternate key for conversion lookups)", convertedPath);

        LOGGER.info("Updated existing record: {}", formatRecord(existingTableNameRecord));
      } else {
        // Create new record
        LOGGER.info("Creating new record for tableName '{}'", tableName);

        ConversionRecord record =
            new ConversionRecord(sourceFile.getCanonicalPath(),
            convertedFile.getCanonicalPath(),
            conversionType);
        record.tableName = tableName;
        record.sourceFile = sourceFile.getCanonicalPath();

        LOGGER.info("Created new record: {}", formatRecord(record));

        // Store under tableName key (primary surrogate key)
        conversions.put(tableName, record);
        LOGGER.info("Stored record under tableName key '{}' (primary key)", tableName);

        // Store under convertedFile key as an alternate key
        // convertedFile is a valid surrogate key when sourceFile is not null (which it is here)
        conversions.put(convertedPath, record);
        LOGGER.info("Stored record under convertedFile key '{}' (alternate key for conversion lookups)", convertedPath);
      }

      saveMetadata();

      LOGGER.info("After update: conversions map size = {}", conversions.size());
      LOGGER.info("Final record under tableName '{}': {}", tableName, formatRecord(conversions.get(tableName)));
      LOGGER.info("Final record under convertedFile '{}': {}", convertedPath, formatRecord(conversions.get(convertedPath)));
      LOGGER.info("=== END CONVERSION RECORD UPDATE TRACE ===\\n");

    } catch (IOException e) {
      LOGGER.error("Failed to record conversion with table name", e);
    }
  }

  /**
   * Reloads the metadata from disk.
   * This is needed when another process or instance has modified the metadata file.
   */
  public void reload() {
    loadMetadata();
  }

  /**
   * Gets a conversion record by converted file path.
   * This is used to find records when auto-discovering converted files.
   *
   * @param convertedFilePath The path to the converted file (e.g., JSON file)
   * @return The conversion record, or null if not found
   */
  public ConversionRecord getConversionRecordByConvertedFile(String convertedFilePath) {
    try {
      String canonicalPath = new File(convertedFilePath).getCanonicalPath();
      LOGGER.debug("Looking for conversion record by converted file: {}", canonicalPath);

      // First try direct lookup
      ConversionRecord record = conversions.get(canonicalPath);
      if (record != null) {
        LOGGER.debug("Found record by direct lookup with key: {}", canonicalPath);
        return record;
      }

      // Search through all records to find one with matching converted file
      for (Map.Entry<String, ConversionRecord> entry : conversions.entrySet()) {
        ConversionRecord r = entry.getValue();
        LOGGER.debug("Checking record key='{}': convertedFile='{}'", entry.getKey(), r.convertedFile);
        if (canonicalPath.equals(r.convertedFile)) {
          LOGGER.debug("Found matching record with key='{}', tableName='{}'", entry.getKey(), r.tableName);
          return r;
        }
      }

      LOGGER.debug("No conversion record found for converted file: {}", canonicalPath);
      return null;
    } catch (IOException e) {
      LOGGER.error("Failed to get conversion record by converted file", e);
      return null;
    }
  }

  /**
   * Updates an existing record with parquet cache file information.
   * This is used when a table is converted to Parquet for caching.
   *
   * @param tableName The table name (key to find the record)
   * @param parquetFile The parquet cache file
   */
  public void updateRecordWithParquetFile(String tableName, File parquetFile) {
    ConversionRecord record = conversions.get(tableName);
    if (record != null) {
      try {
        record.parquetCacheFile = parquetFile.getCanonicalPath();
        record.tableType = "ParquetTranslatableTable";
        saveMetadata();
        LOGGER.debug("Updated record '{}' with parquet cache file: {}", tableName, parquetFile.getName());
      } catch (IOException e) {
        LOGGER.error("Failed to update record with parquet file", e);
      }
    } else {
      LOGGER.warn("No record found with table name '{}' to update with parquet file", tableName);
    }
  }

  /**
   * Updates an existing conversion record while preserving the table name.
   * This is used by bulk conversion to update records without losing explicit names.
   *
   * @param sourceFile The source file that was converted
   * @param convertedFile The new converted file
   * @param conversionType The type of conversion performed
   * @param baseDirectory The base directory for metadata storage
   */
  public void updateExistingRecord(File sourceFile, File convertedFile, String conversionType, File baseDirectory) {
    updateExistingRecord(sourceFile, convertedFile, conversionType, baseDirectory, null);
  }

  /**
   * Gets all conversion records from the registry.
   * This provides access to all table metadata including conversions and cache locations.
   *
   * @return Unmodifiable map of all conversion records
   */
  public Map<String, ConversionRecord> getAllConversions() {
    return java.util.Collections.unmodifiableMap(conversions);
  }

  /**
   * Loads metadata from disk with file locking for concurrent access.
   */
  private void loadMetadata() {
    if (!metadataFile.exists()) {
      return;
    }

    File lockFile = new File(metadataFile.getParentFile(), metadataFile.getName() + ".lock");

    try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
         FileChannel channel = raf.getChannel()) {

      // Acquire shared lock for reading
      try (FileLock lock = channel.lock(0, Long.MAX_VALUE, true)) {
        @SuppressWarnings("unchecked")
        Map<String, ConversionRecord> loaded =
            MAPPER.readValue(
                metadataFile, MAPPER.getTypeFactory().constructMapType(HashMap.class,
                String.class, ConversionRecord.class));

        conversions.putAll(loaded);
        LOGGER.debug("Loaded {} conversion records from metadata", loaded.size());

        // Clean up entries for files that no longer exist
        boolean needsCleanup = false;
        for (Map.Entry<String, ConversionRecord> entry : loaded.entrySet()) {
          File convertedFile = new File(entry.getKey());
          File originalFile = new File(entry.getValue().originalFile);
          if (convertedFile.exists() && originalFile.exists()) {
            conversions.put(entry.getKey(), entry.getValue());
          } else {
            LOGGER.debug("Skipping stale conversion record: {}", entry.getKey());
            needsCleanup = true;
          }
        }

        // Save cleaned version if needed (will use exclusive lock)
        if (needsCleanup) {
          // Release shared lock before acquiring exclusive lock
          lock.release();
          saveMetadata();
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to load conversion metadata", e);
    }
  }

  /**
   * Saves metadata to disk with file locking for concurrent access.
   */
  private void saveMetadata() {
    File tempFile = new File(metadataFile.getParentFile(), metadataFile.getName() + ".tmp");
    File lockFile = new File(metadataFile.getParentFile(), metadataFile.getName() + ".lock");

    try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
         FileChannel channel = raf.getChannel()) {

      // Acquire exclusive lock
      try (FileLock lock = channel.lock()) {
        // Write to temp file first
        MAPPER.writeValue(tempFile, conversions);

        // Atomically move temp file to actual file
        Files.move(tempFile.toPath(), metadataFile.toPath(),
                   StandardCopyOption.REPLACE_EXISTING,
                   StandardCopyOption.ATOMIC_MOVE);

        LOGGER.debug("Saved {} conversion records to metadata", conversions.size());
      }
    } catch (IOException e) {
      LOGGER.error("Failed to save conversion metadata", e);
      // Clean up temp file if it exists
      if (tempFile.exists()) {
        tempFile.delete();
      }
    }
  }

  /**
   * Clears all metadata (mainly for testing).
   */
  public void clear() {
    conversions.clear();
    if (metadataFile.exists()) {
      metadataFile.delete();
    }
  }

  /**
   * Formats a conversion record for detailed logging.
   */
  private String formatRecord(ConversionRecord record) {
    if (record == null) return "null";
    return String.format(
        "ConversionRecord{tableName='%s', tableType='%s', sourceFile='%s', originalFile='%s', convertedFile='%s', conversionType='%s', parquetCacheFile='%s'}",
        record.tableName, record.tableType, record.sourceFile, record.originalFile, record.convertedFile, record.conversionType, record.parquetCacheFile);
  }
}
