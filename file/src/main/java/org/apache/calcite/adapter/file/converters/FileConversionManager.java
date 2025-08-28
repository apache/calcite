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
package org.apache.calcite.adapter.file.converters;

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Centralized manager for file conversions.
 * Handles all types of conversions (Excel→JSON, HTML→JSON, XML→JSON, JSON→JSON via JSONPath)
 * and tracks conversion metadata for refresh functionality.
 */
public class FileConversionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileConversionManager.class);

  /**
   * Identifies the source file type and runs the appropriate conversion if needed.
   * This method is smart enough to:
   * - Handle direct source files (CSV, Parquet) that don't need conversion
   * - Handle files that need conversion (Excel, HTML, XML)
   * - Handle derived files (JSON from JSONPath extraction)
   * - Track the original source through conversion chains
   *
   * @param sourceFile The file to potentially convert
   * @param outputDir The directory for output files
   * @param columnNameCasing Column name casing strategy
   * @return true if conversion was performed, false if no conversion needed
   */
  public static boolean convertIfNeeded(File sourceFile, File outputDir, String columnNameCasing) {
    return convertIfNeeded(sourceFile, outputDir, columnNameCasing, "SMART_CASING");
  }

  /**
   * Identifies the source file type and runs the appropriate conversion if needed.
   *
   * @param sourceFile The file to potentially convert
   * @param outputDir The directory for output files
   * @param columnNameCasing Column name casing strategy
   * @param tableNameCasing Table name casing strategy
   * @return true if conversion was performed, false if no conversion needed
   */
  public static boolean convertIfNeeded(File sourceFile, File outputDir, String columnNameCasing, String tableNameCasing) {
    return convertIfNeeded(sourceFile, outputDir, columnNameCasing, tableNameCasing, null);
  }

  /**
   * Identifies the source file type and runs the appropriate conversion if needed.
   *
   * @param sourceFile The file to potentially convert
   * @param outputDir The directory for output files
   * @param columnNameCasing Column name casing strategy
   * @param tableNameCasing Table name casing strategy
   * @param baseDirectory The base directory for metadata storage (if null, uses outputDir)
   * @return true if conversion was performed, false if no conversion needed
   */
  public static boolean convertIfNeeded(File sourceFile, File outputDir, String columnNameCasing, String tableNameCasing, File baseDirectory) {
    return convertIfNeeded(sourceFile, outputDir, columnNameCasing, tableNameCasing, baseDirectory, null);
  }

  /**
   * Identifies the source file type and runs the appropriate conversion if needed.
   *
   * @param sourceFile The file to potentially convert
   * @param outputDir The directory for output files
   * @param columnNameCasing Column name casing strategy
   * @param tableNameCasing Table name casing strategy
   * @param baseDirectory The base directory for metadata storage (if null, uses outputDir)
   * @param relativePath The relative path from source directory to the file (for preserving directory structure)
   * @return true if conversion was performed, false if no conversion needed
   */
  public static boolean convertIfNeeded(File sourceFile, File outputDir, String columnNameCasing, String tableNameCasing, File baseDirectory, String relativePath) {
    String path = sourceFile.getPath().toLowerCase();

    // First check if conversion is needed using metadata-based change detection
    if (baseDirectory != null && !isConversionNeeded(sourceFile, baseDirectory)) {
      LOGGER.debug("File {} has not changed, skipping conversion", sourceFile.getName());
      return false;
    }

    try {
      // Excel files
      if (path.endsWith(".xlsx") || path.endsWith(".xls")) {
        boolean converted = convertExcelFile(sourceFile, outputDir, baseDirectory, tableNameCasing, columnNameCasing, relativePath);
        LOGGER.debug("Converted Excel file to JSON: {}", sourceFile.getName());
        return converted;
      }

      // HTML files
      if (path.endsWith(".html") || path.endsWith(".htm")) {
        // Check if there's an existing conversion record for this source file
        // This preserves explicit table names from tableDef processing
        String existingTableName = null;
        if (baseDirectory != null) {
          try {
            ConversionMetadata metadata = new ConversionMetadata(baseDirectory);
            ConversionMetadata.ConversionRecord existingRecord = metadata.findRecordBySourceFile(sourceFile);
            if (existingRecord != null) {
              existingTableName = existingRecord.tableName;
              LOGGER.debug("Found existing conversion record for {}, preserving table name: {}",
                  sourceFile.getName(), existingTableName);
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to check for existing conversion record: {}", e.getMessage());
          }
        }

        List<File> jsonFiles = HtmlToJsonConverter.convert(sourceFile, outputDir, columnNameCasing, tableNameCasing, baseDirectory, relativePath, existingTableName);
        LOGGER.debug("Converted HTML file to {} JSON files: {}", jsonFiles.size(), sourceFile.getName());
        return true;
      }

      // XML files
      if (path.endsWith(".xml")) {
        List<File> jsonFiles = XmlToJsonConverter.convert(sourceFile, outputDir, null, columnNameCasing, baseDirectory, relativePath);
        LOGGER.debug("Converted XML file to {} JSON files: {}", jsonFiles.size(), sourceFile.getName());
        return true;
      }

      // Markdown files
      if (path.endsWith(".md")) {
        MarkdownTableScanner.scanAndConvertTables(sourceFile, outputDir, relativePath);
        LOGGER.debug("Converted Markdown file to JSON: {}", sourceFile.getName());
        return true;
      }

      // DOCX files
      if (path.endsWith(".docx")) {
        DocxTableScanner.scanAndConvertTables(sourceFile, outputDir, relativePath);
        LOGGER.debug("Converted DOCX file to JSON: {}", sourceFile.getName());
        return true;
      }

      // PPTX files
      if (path.endsWith(".pptx")) {
        PptxTableScanner.scanAndConvertTables(sourceFile, outputDir, relativePath);
        LOGGER.debug("Converted PPTX file to JSON: {}", sourceFile.getName());
        return true;
      }

      // JSON files - check if they're derived via JSONPath
      if (path.endsWith(".json") || path.endsWith(".json.gz")) {
        return handleJsonFile(sourceFile, outputDir, baseDirectory);
      }

      // YAML files - check if they're derived via JSONPath
      if (path.endsWith(".yaml") || path.endsWith(".yml")) {
        return handleYamlFile(sourceFile, outputDir, baseDirectory);
      }

      // CSV, Parquet, and other direct-use files don't need conversion
      return false;

    } catch (Exception e) {
      LOGGER.error("Failed to convert file {}: {}", sourceFile.getName(), e.getMessage(), e);
      return false;
    }
  }

  /**
   * Handles JSON files which might be derived via JSONPath extraction.
   */
  private static boolean handleJsonFile(File jsonFile, File outputDir, File baseDirectory) {
    try {
      File metadataDir = baseDirectory != null ? baseDirectory : jsonFile.getParentFile();
      ConversionMetadata metadata = new ConversionMetadata(metadataDir);
      ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(jsonFile);

      if (record != null && record.getConversionType().startsWith("JSONPATH_EXTRACTION")) {
        // This JSON was extracted from another JSON via JSONPath
        String jsonPath = extractJsonPath(record.getConversionType());
        File originalJson = new File(record.getOriginalPath());

        if (originalJson.exists()) {
          JsonPathConverter.extract(originalJson, jsonFile, jsonPath, baseDirectory);
          LOGGER.debug("Re-extracted JSON via JSONPath {} from {}",
              jsonPath, originalJson.getName());
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.debug("JSON file is not a JSONPath extraction: {}", jsonFile.getName());
    }

    return false;
  }

  /**
   * Handles YAML files which might be derived via JSONPath extraction.
   * YAML is a superset of JSON, so JSONPath queries work with YAML data.
   */
  private static boolean handleYamlFile(File yamlFile, File outputDir, File baseDirectory) {
    try {
      File metadataDir = baseDirectory != null ? baseDirectory : yamlFile.getParentFile();
      ConversionMetadata metadata = new ConversionMetadata(metadataDir);
      ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(yamlFile);

      if (record != null && record.getConversionType().startsWith("JSONPATH_EXTRACTION")) {
        // This YAML was extracted from another YAML/JSON via JSONPath
        String jsonPath = extractJsonPath(record.getConversionType());
        File originalFile = new File(record.getOriginalPath());

        if (originalFile.exists()) {
          YamlPathConverter.extract(originalFile, yamlFile, jsonPath, baseDirectory);
          LOGGER.debug("Re-extracted YAML via JSONPath {} from {}",
              jsonPath, originalFile.getName());
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.debug("YAML file is not a JSONPath extraction: {}", yamlFile.getName());
    }

    return false;
  }

  /**
   * Extracts the JSONPath from a conversion type string.
   */
  private static String extractJsonPath(String conversionType) {
    // Format: "JSONPATH_EXTRACTION[$.path.to.data]"
    return conversionType
        .replace("JSONPATH_EXTRACTION[", "")
        .replace("]", "");
  }

  /**
   * Finds the ultimate original source file for a converted file.
   * This follows the conversion chain back to the original source.
   * For example: HTML → JSON → JSON (via JSONPath) would return the HTML file.
   *
   * @param file The file to trace back
   * @return The original source file, or the input file if no conversion history
   */
  public static File findOriginalSource(File file) {
    try {
      ConversionMetadata metadata = new ConversionMetadata(file.getParentFile());
      File current = file;
      File original = null;

      // Follow the chain back to the original source
      while ((original = metadata.findOriginalSource(current)) != null) {
        current = original;
      }

      return current;
    } catch (Exception e) {
      LOGGER.debug("Could not find original source for {}: {}", file.getName(), e.getMessage());
      return file;
    }
  }

  /**
   * Determines if a file type requires conversion to JSON.
   */
  public static boolean requiresConversion(String filename) {
    String lower = filename.toLowerCase();
    return lower.endsWith(".xlsx") || lower.endsWith(".xls") ||
           lower.endsWith(".html") || lower.endsWith(".htm") ||
           lower.endsWith(".xml") ||
           lower.endsWith(".md") ||
           lower.endsWith(".docx") ||
           lower.endsWith(".pptx");
  }

  /**
   * Determines if a file is directly usable without conversion.
   */
  public static boolean isDirectlyUsable(String filename) {
    String lower = filename.toLowerCase();
    return lower.endsWith(".csv") || lower.endsWith(".csv.gz") ||
           lower.endsWith(".tsv") || lower.endsWith(".tsv.gz") ||
           lower.endsWith(".json") || lower.endsWith(".json.gz") ||
           lower.endsWith(".parquet") ||
           lower.endsWith(".arrow") ||
           lower.endsWith(".yaml") || lower.endsWith(".yml");
  }

  /**
   * Checks if conversion is needed by comparing current file state with stored metadata.
   * Uses appropriate change detection (ETag for HTTP/S3, timestamp for local files).
   */
  private static boolean isConversionNeeded(File sourceFile, File baseDirectory) {
    try {
      ConversionMetadata metadata = new ConversionMetadata(baseDirectory);
      ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(sourceFile);

      if (record == null) {
        LOGGER.debug("No conversion record found for {}, conversion needed", sourceFile.getName());
        return true; // No record exists, need to convert
      }

      // Check if source file has changed
      boolean hasChanged = checkFileChanged(record, sourceFile);
      LOGGER.debug("Change detection for {}: hasChanged={}", sourceFile.getName(), hasChanged);
      return hasChanged;

    } catch (Exception e) {
      LOGGER.warn("Failed to check conversion metadata for {}: {}", sourceFile.getName(), e.getMessage());
      return true; // Conservative: convert if we can't check
    }
  }

  /**
   * Checks if a file has changed using the appropriate detection method.
   */
  private static boolean checkFileChanged(ConversionMetadata.ConversionRecord record, File sourceFile) {
    String filePath = sourceFile.getAbsolutePath();

    // For remote files (HTTP, S3, etc.), use StorageProvider metadata comparison
    if (isRemoteFile(filePath)) {
      try {
        StorageProvider provider = StorageProviderFactory.createFromUrl(filePath);
        StorageProvider.FileMetadata currentMetadata = provider.getMetadata(filePath);
        return record.hasChangedViaMetadata(currentMetadata);
      } catch (Exception e) {
        LOGGER.warn("Failed to get metadata for remote file {}: {}", filePath, e.getMessage());
        return true; // Conservative: assume changed if we can't check
      }
    } else {
      // For local files, use built-in timestamp-based detection
      return record.hasChanged();
    }
  }

  /**
   * Records conversion metadata with appropriate metadata for change detection.
   */
  private static void recordConversion(File sourceFile, File convertedFile, String conversionType, File baseDirectory) {
    try {
      ConversionMetadata metadata = new ConversionMetadata(baseDirectory);
      String filePath = sourceFile.getAbsolutePath();

      if (isRemoteFile(filePath)) {
        // For remote files, capture StorageProvider metadata
        try {
          StorageProvider provider = StorageProviderFactory.createFromUrl(filePath);
          StorageProvider.FileMetadata fileMetadata = provider.getMetadata(filePath);

          ConversionMetadata.ConversionRecord record =
              new ConversionMetadata.ConversionRecord(filePath,
              convertedFile.getAbsolutePath(),
              conversionType,
              null, // cachedFile
              fileMetadata.getEtag(),
              fileMetadata.getSize(),
              fileMetadata.getContentType());

          metadata.recordConversion(convertedFile, record);
          LOGGER.debug("Recorded conversion with remote metadata for {}: etag={}, size={}",
              sourceFile.getName(), fileMetadata.getEtag(), fileMetadata.getSize());

        } catch (Exception e) {
          LOGGER.warn("Failed to get remote metadata for {}, using basic record: {}", filePath, e.getMessage());
          // Fallback to basic record
          metadata.recordConversion(
              convertedFile, new ConversionMetadata.ConversionRecord(
              filePath, convertedFile.getAbsolutePath(), conversionType));
        }
      } else {
        // For local files, use basic record (timestamp-based)
        metadata.recordConversion(
            convertedFile, new ConversionMetadata.ConversionRecord(
            filePath, convertedFile.getAbsolutePath(), conversionType));
        LOGGER.debug("Recorded conversion with timestamp metadata for {}", sourceFile.getName());
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to record conversion metadata for {}: {}", sourceFile.getName(), e.getMessage());
    }
  }

  /**
   * Converts an Excel file and records metadata.
   */
  private static boolean convertExcelFile(File sourceFile, File outputDir, File baseDirectory, String tableNameCasing, String columnNameCasing, String relativePath) {
    try {
      // Convert Excel file to the specified output directory with relative path for directory preservation
      MultiTableExcelToJsonConverter.convertFileToJson(sourceFile, outputDir, true, tableNameCasing, columnNameCasing, baseDirectory, relativePath);

      // Record conversion for each sheet that was converted
      // Excel converter creates files like "filename__sheetname.json" or "dir_filename__sheetname.json" if relativePath is provided
      String fileName = sourceFile.getName();
      String baseName = fileName.contains(".") ?
          fileName.substring(0, fileName.lastIndexOf('.')) : fileName;

      // If relativePath is provided, include directory prefix in the pattern
      if (relativePath != null && relativePath.contains(File.separator)) {
        String dirPrefix = relativePath.substring(0, relativePath.lastIndexOf(File.separator))
            .replace(File.separator, "_");
        baseName = dirPrefix + "_" + baseName;
      }

      // Make baseName final for use in lambda
      final String filePrefix = baseName;

      // Find generated JSON files in the output directory
      File[] jsonFiles = outputDir.listFiles((dir, name) ->
          name.startsWith(filePrefix + "__") && name.endsWith(".json"));

      if (jsonFiles != null && baseDirectory != null) {
        for (File jsonFile : jsonFiles) {
          recordConversion(sourceFile, jsonFile, "EXCEL_TO_JSON", baseDirectory);
        }
      }

      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to convert Excel file {}: {}", sourceFile.getName(), e.getMessage());
      return false;
    }
  }

  /**
   * Determines if a file path is a remote URL.
   */
  private static boolean isRemoteFile(String path) {
    return path != null && (
        path.startsWith("http://") || path.startsWith("https://") ||
        path.startsWith("s3://") || path.startsWith("ftp://") ||
        path.startsWith("sftp://"));
  }
}
