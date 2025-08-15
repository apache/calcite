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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
    String path = sourceFile.getPath().toLowerCase();
    
    try {
      // Excel files
      if (path.endsWith(".xlsx") || path.endsWith(".xls")) {
        SafeExcelToJsonConverter.convertIfNeeded(sourceFile, true);
        LOGGER.debug("Converted Excel file to JSON: {}", sourceFile.getName());
        return true;
      }
      
      // HTML files
      if (path.endsWith(".html") || path.endsWith(".htm")) {
        List<File> jsonFiles = HtmlToJsonConverter.convert(sourceFile, outputDir, columnNameCasing);
        LOGGER.debug("Converted HTML file to {} JSON files: {}", jsonFiles.size(), sourceFile.getName());
        return true;
      }
      
      // XML files
      if (path.endsWith(".xml")) {
        List<File> jsonFiles = XmlToJsonConverter.convert(sourceFile, outputDir, columnNameCasing);
        LOGGER.debug("Converted XML file to {} JSON files: {}", jsonFiles.size(), sourceFile.getName());
        return true;
      }
      
      // Markdown files
      if (path.endsWith(".md")) {
        MarkdownTableScanner.scanAndConvertTables(sourceFile);
        LOGGER.debug("Converted Markdown file to JSON: {}", sourceFile.getName());
        return true;
      }
      
      // DOCX files
      if (path.endsWith(".docx")) {
        DocxTableScanner.scanAndConvertTables(sourceFile);
        LOGGER.debug("Converted DOCX file to JSON: {}", sourceFile.getName());
        return true;
      }
      
      // PPTX files
      if (path.endsWith(".pptx")) {
        PptxTableScanner.scanAndConvertTables(sourceFile);
        LOGGER.debug("Converted PPTX file to JSON: {}", sourceFile.getName());
        return true;
      }
      
      // JSON files - check if they're derived via JSONPath
      if (path.endsWith(".json") || path.endsWith(".json.gz")) {
        return handleJsonFile(sourceFile, outputDir);
      }
      
      // YAML files - check if they're derived via JSONPath
      if (path.endsWith(".yaml") || path.endsWith(".yml")) {
        return handleYamlFile(sourceFile, outputDir);
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
  private static boolean handleJsonFile(File jsonFile, File outputDir) {
    try {
      ConversionMetadata metadata = new ConversionMetadata(jsonFile.getParentFile());
      ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(jsonFile);
      
      if (record != null && record.getConversionType().startsWith("JSONPATH_EXTRACTION")) {
        // This JSON was extracted from another JSON via JSONPath
        String jsonPath = extractJsonPath(record.getConversionType());
        File originalJson = new File(record.getOriginalPath());
        
        if (originalJson.exists()) {
          JsonPathConverter.extract(originalJson, jsonFile, jsonPath);
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
  private static boolean handleYamlFile(File yamlFile, File outputDir) {
    try {
      ConversionMetadata metadata = new ConversionMetadata(yamlFile.getParentFile());
      ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(yamlFile);
      
      if (record != null && record.getConversionType().startsWith("JSONPATH_EXTRACTION")) {
        // This YAML was extracted from another YAML/JSON via JSONPath
        String jsonPath = extractJsonPath(record.getConversionType());
        File originalFile = new File(record.getOriginalPath());
        
        if (originalFile.exists()) {
          YamlPathConverter.extract(originalFile, yamlFile, jsonPath);
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
           lower.endsWith(".yaml") || lower.endsWith(".yml");
  }
}