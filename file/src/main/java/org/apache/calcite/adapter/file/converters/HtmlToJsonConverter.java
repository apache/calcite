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

import org.apache.calcite.adapter.file.converters.ConversionRecorder;
import org.apache.calcite.adapter.file.converters.HtmlCrawler.CrawlResult;
import org.apache.calcite.adapter.file.converters.HtmlTableScanner.TableInfo;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Converts HTML tables to JSON files for processing by Arrow Dataset.
 * Similar to MultiTableExcelToJsonConverter but for HTML files.
 */
public class HtmlToJsonConverter {
  private static final Logger LOGGER = Logger.getLogger(HtmlToJsonConverter.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private HtmlToJsonConverter() {
    // Utility class should not be instantiated
  }

  /**
   * Converts all tables in an HTML file to separate JSON files.
   *
   * @param htmlFile The HTML file to convert
   * @param outputDir The directory to write JSON files to
   * @return List of generated JSON files
   * @throws IOException if conversion fails
   */
  public static List<File> convert(File htmlFile, File outputDir, File baseDirectory) throws IOException {
    return convert(htmlFile, outputDir, "UNCHANGED", baseDirectory);
  }

  /**
   * Converts all tables in an HTML file to separate JSON files with column name casing.
   *
   * @param htmlFile The HTML file to convert
   * @param outputDir The directory to write JSON files to
   * @param columnNameCasing The casing strategy for column names
   * @return List of generated JSON files
   * @throws IOException if conversion fails
   */
  public static List<File> convert(File htmlFile, File outputDir, String columnNameCasing, File baseDirectory) throws IOException {
    return convert(htmlFile, outputDir, columnNameCasing, "SMART_CASING", baseDirectory);
  }

  /**
   * Converts all tables in an HTML file to separate JSON files with column and table name casing.
   *
   * @param htmlFile The HTML file to convert
   * @param outputDir The directory to write JSON files to
   * @param columnNameCasing The casing strategy for column names
   * @param tableNameCasing The casing strategy for table names
   * @return List of generated JSON files
   * @throws IOException if conversion fails
   */
  /**
   * Converts HTML file to JSON using explicit table name.
   * 
   * @param htmlFile The HTML file to convert
   * @param outputDir The directory to write JSON files to
   * @param columnNameCasing The casing strategy for column names
   * @param tableNameCasing The casing strategy for table names
   * @param explicitTableName The explicit name to use for the table (overrides auto-generated name)
   * @return List of generated JSON files
   * @throws IOException if conversion fails
   */
  public static List<File> convert(File htmlFile, File outputDir, String columnNameCasing, 
                                   String tableNameCasing, String explicitTableName, File baseDirectory, String relativePath) throws IOException {
    List<File> jsonFiles = new ArrayList<>();
    // Use HtmlTableScanner to find tables - make sure we re-scan each time
    Source source = Sources.of(htmlFile);
    List<HtmlTableScanner.TableInfo> tableInfos = HtmlTableScanner.scanTables(source, tableNameCasing);
    LOGGER.info("Found " + tableInfos.size() + " tables in " + htmlFile.getName());
    
    // Ensure output directory exists
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }
    
    // Parse HTML to get actual table elements
    Document doc = Jsoup.parse(htmlFile, "UTF-8");
    Elements tables = doc.select("table");
    
    // If explicit table name is provided, select appropriate table and use the explicit name
    if (explicitTableName != null && tables.size() > 0) {
      Element selectedTable;
      
      if (tables.size() == 1) {
        // Single table - use it
        selectedTable = tables.get(0);
      } else {
        // Multiple tables - select the largest one (most columns)
        selectedTable = tables.get(0);
        int maxColumns = selectedTable.select("tr").stream()
            .mapToInt(row -> row.select("td, th").size())
            .max()
            .orElse(0);
        
        for (int i = 1; i < tables.size(); i++) {
          Element table = tables.get(i);
          int columns = table.select("tr").stream()
              .mapToInt(row -> row.select("td, th").size())
              .max()
              .orElse(0);
          if (columns > maxColumns) {
            maxColumns = columns;
            selectedTable = table;
          }
        }
        LOGGER.info("Selected table with " + maxColumns + " columns from " + tables.size() 
            + " tables for explicit name '" + explicitTableName + "'");
      }
      
      File jsonFile = new File(outputDir, explicitTableName + ".json");
      writeTableAsJson(selectedTable, jsonFile, columnNameCasing);
      jsonFiles.add(jsonFile);
      
      // Update or create conversion record with explicit table name
      if (baseDirectory != null) {
        ConversionMetadata metadata = new ConversionMetadata(baseDirectory);
        metadata.updateExistingRecord(htmlFile, jsonFile, "HTML_TO_JSON", baseDirectory, explicitTableName);
      } else {
        ConversionRecorder.recordConversion(htmlFile, jsonFile, "HTML_TO_JSON", baseDirectory, explicitTableName);
      }
      
      LOGGER.info("Wrote table to " + jsonFile.getAbsolutePath() + " with explicit name: " + explicitTableName);
    } else if (explicitTableName == null) {
      // Use the original logic for multiple tables or no explicit name
      for (int i = 0; i < tableInfos.size() && i < tables.size(); i++) {
        HtmlTableScanner.TableInfo tableInfo = tableInfos.get(i);
        Element table = tables.get(i);
        String tableName = tableInfo.name;
        
        // Generate JSON filename
        String baseFileName = ConverterUtils.getBaseFileName(htmlFile.getName(), ".html", ".htm");
        
        // Include directory structure in the filename if relativePath is provided
        if (relativePath != null && relativePath.contains(File.separator)) {
          String dirPrefix = relativePath.substring(0, relativePath.lastIndexOf(File.separator))
              .replace(File.separator, "_");
          baseFileName = dirPrefix + "_" + baseFileName;
        }
        
        String jsonFileName = baseFileName + "__" + tableName + ".json";
        File jsonFile = new File(outputDir, jsonFileName);
        
        writeTableAsJson(table, jsonFile, columnNameCasing);
        jsonFiles.add(jsonFile);
        
        // Update existing conversion record if one exists, otherwise create new one
        // Use the generated table name from the JSON filename
        String generatedTableName = jsonFileName.substring(0, jsonFileName.lastIndexOf(".json"));
        
        // Create conversion record with generated table name
        ConversionRecorder.recordConversion(htmlFile, jsonFile, "HTML_TO_JSON", baseDirectory, generatedTableName);
        
        LOGGER.info("Wrote table to " + jsonFile.getAbsolutePath());
      }
    }
    
    return jsonFiles;
  }
  
  public static List<File> convert(File htmlFile, File outputDir, String columnNameCasing, String tableNameCasing, File baseDirectory) throws IOException {
    return convert(htmlFile, outputDir, columnNameCasing, tableNameCasing, null, baseDirectory, null);
  }
  
  public static List<File> convert(File htmlFile, File outputDir, String columnNameCasing, String tableNameCasing, File baseDirectory, String relativePath) throws IOException {
    return convert(htmlFile, outputDir, columnNameCasing, tableNameCasing, null, baseDirectory, relativePath);
  }
  
  /**
   * Converts a specific table in an HTML file to JSON using selector and index.
   * This is for explicit table definitions with selector and index parameters.
   *
   * @param htmlFile The HTML file to convert
   * @param outputDir The directory to write JSON files to
   * @param columnNameCasing The casing strategy for column names
   * @param tableNameCasing The casing strategy for table names
   * @param selector The CSS selector to find tables (e.g., "table.wikitable.sortable")
   * @param index The index of the table to select (0-based)
   * @param explicitTableName The explicit name to use for the table
   * @param baseDirectory The base directory for metadata recording
   * @return List containing the single generated JSON file
   * @throws IOException if conversion fails
   */
  public static List<File> convertWithSelector(File htmlFile, File outputDir, String columnNameCasing, 
                                               String tableNameCasing, String selector, Integer index, 
                                               String explicitTableName, File baseDirectory) throws IOException {
    return convertWithSelector(htmlFile, outputDir, columnNameCasing, tableNameCasing, selector, index, 
                              explicitTableName, baseDirectory, null);
  }

  /**
   * Converts a specific table in an HTML file to JSON using selector and index with field mappings.
   * This version supports field definitions for mapping HTML headers to different column names.
   *
   * @param htmlFile The HTML file to convert
   * @param outputDir The directory to write JSON files to
   * @param columnNameCasing The casing strategy for column names
   * @param tableNameCasing The casing strategy for table names
   * @param selector The CSS selector to find tables (e.g., "table.wikitable.sortable")
   * @param index The index of the table to select (0-based)
   * @param explicitTableName The explicit name to use for the table
   * @param baseDirectory The base directory for metadata recording
   * @param fieldConfigs The field definitions for mapping HTML headers to column names
   * @return List containing the single generated JSON file
   * @throws IOException if conversion fails
   */
  public static List<File> convertWithSelector(File htmlFile, File outputDir, String columnNameCasing, 
                                               String tableNameCasing, String selector, Integer index, 
                                               String explicitTableName, File baseDirectory,
                                               List<Map<String, Object>> fieldConfigs) throws IOException {
    List<File> jsonFiles = new ArrayList<>();
    
    if (selector == null || index == null) {
      LOGGER.warning("Selector or index is null, falling back to regular conversion");
      return convert(htmlFile, outputDir, columnNameCasing, tableNameCasing, explicitTableName, baseDirectory, null);
    }
    
    // Ensure output directory exists
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }
    
    // Parse HTML and find the specific table using selector and index
    Document doc = Jsoup.parse(htmlFile, "UTF-8");
    Elements selectedTables = doc.select(selector);
    
    if (selectedTables.size() == 0) {
      LOGGER.warning("No tables found for selector: " + selector);
      return jsonFiles;
    }
    
    if (index >= selectedTables.size()) {
      LOGGER.warning("Index " + index + " is out of bounds, only " + selectedTables.size() + " tables found for selector: " + selector);
      return jsonFiles;
    }
    
    // Get the specific table at the requested index
    Element table = selectedTables.get(index);
    
    // Create JSON file with explicit table name
    File jsonFile = new File(outputDir, explicitTableName + ".json");
    writeTableAsJson(table, jsonFile, columnNameCasing, fieldConfigs);
    jsonFiles.add(jsonFile);
    
    // Update or create conversion record with explicit table name
    if (baseDirectory != null) {
      ConversionMetadata metadata = new ConversionMetadata(baseDirectory);
      metadata.updateExistingRecord(htmlFile, jsonFile, "HTML_TO_JSON", baseDirectory, explicitTableName);
    } else {
      ConversionRecorder.recordConversion(htmlFile, jsonFile, "HTML_TO_JSON", baseDirectory, explicitTableName);
    }
    
    LOGGER.info("Converted table at selector '" + selector + "' index " + index + " to " + jsonFile.getAbsolutePath() + " with explicit name: " + explicitTableName);
    
    return jsonFiles;
  }

  /**
   * Converts all tables in an HTML file to separate JSON files with explicit table name support.
   * This overload is used by bulk conversion to preserve explicit table names from tableDef processing.
   * 
   * @param htmlFile The HTML file to convert
   * @param outputDir The directory to write JSON files to
   * @param columnNameCasing The casing strategy for column names
   * @param tableNameCasing The casing strategy for table names
   * @param baseDirectory The base directory for metadata storage
   * @param relativePath The relative path from source directory to the file
   * @param existingTableName The existing table name from previous conversion record (or null)
   * @return List of generated JSON files
   * @throws IOException if conversion fails
   */
  public static List<File> convert(File htmlFile, File outputDir, String columnNameCasing, String tableNameCasing, File baseDirectory, String relativePath, String existingTableName) throws IOException {
    return convert(htmlFile, outputDir, columnNameCasing, tableNameCasing, existingTableName, baseDirectory, relativePath);
  }
  


  /**
   * Writes a table as a JSON array file.
   */
  private static void writeTableAsJson(Element table, File jsonFile, String columnNameCasing)
      throws IOException {
    writeTableAsJson(table, jsonFile, columnNameCasing, null);
  }

  /**
   * Converts an HTML table element to JSON with field mappings support.
   */
  private static void writeTableAsJson(Element table, File jsonFile, String columnNameCasing, 
                                      List<Map<String, Object>> fieldConfigs) throws IOException {
    ArrayNode jsonArray = MAPPER.createArrayNode();

    // Extract raw headers from HTML first (without casing applied)
    List<String> rawHeaders = extractRawHeaders(table);
    
    // Create header mapping based on field definitions
    Map<String, String> headerMapping = new LinkedHashMap<>();
    Set<String> skipHeaders = new HashSet<>();
    
    if (fieldConfigs != null) {
      // Build mapping from raw HTML headers to desired column names
      for (Map<String, Object> fieldConfig : fieldConfigs) {
        String thName = (String) fieldConfig.get("th");  // Raw HTML header text
        String name = (String) fieldConfig.get("name");  // Desired column name
        String skip = (String) fieldConfig.get("skip");
        
        
        if (thName != null) {
          if (skip != null && "true".equalsIgnoreCase(skip)) {
            skipHeaders.add(thName);
          } else {
            // Use explicit name if provided, otherwise apply casing to th name
            String finalName = name != null ? name : 
                org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(thName, columnNameCasing);
            headerMapping.put(thName, finalName);
          }
        }
      }
    }
    
    // Create final headers list using mappings
    List<String> headers = new ArrayList<>();
    for (String rawHeader : rawHeaders) {
      if (skipHeaders.contains(rawHeader)) {
        continue; // Skip this header entirely
      }
      
      if (headerMapping.containsKey(rawHeader)) {
        headers.add(headerMapping.get(rawHeader));
      } else {
        // No field config for this header, apply default casing
        headers.add(org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(rawHeader, columnNameCasing));
      }
    }
    
    // Process data rows
    Elements rows = table.select("tr");
    boolean skipFirstRow = shouldSkipFirstRow(table, headers);
    
    LOGGER.fine("Processing table with " + rows.size() + " rows, skipFirstRow=" + skipFirstRow);
    
    for (int rowIndex = skipFirstRow ? 1 : 0; rowIndex < rows.size(); rowIndex++) {
      Element row = rows.get(rowIndex);
      // Select both th and td elements to capture all cell data
      // Some tables use th elements for row headers (first column)
      Elements cells = row.select("th, td");
      
      if (cells.isEmpty()) {
        continue;
      }
      
      ObjectNode jsonRow = MAPPER.createObjectNode();
      
      // Process cells based on raw headers, applying mappings and skips
      int headerIndex = 0;
      for (int cellIndex = 0; cellIndex < Math.min(rawHeaders.size(), cells.size()); cellIndex++) {
        String rawHeader = rawHeaders.get(cellIndex);
        
        // Skip this column if it's in the skip set
        if (skipHeaders.contains(rawHeader)) {
          continue;
        }
        
        // Get the mapped header name
        String finalHeader;
        if (headerMapping.containsKey(rawHeader)) {
          finalHeader = headerMapping.get(rawHeader);
        } else {
          // No field config for this header, apply default casing
          finalHeader = org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(rawHeader, columnNameCasing);
        }
        
        String value = extractTextFromElement(cells.get(cellIndex));
        ConverterUtils.setJsonValueWithTypeInference(jsonRow, finalHeader, value);
      }
      
      if (jsonRow.size() > 0) {
        jsonArray.add(jsonRow);
      }
    }

    LOGGER.info("Writing " + jsonArray.size() + " rows to " + jsonFile.getAbsolutePath());
    
    // Convert to JSON string first to verify content
    String jsonContent = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonArray);
    LOGGER.info("JSON content to write (first 200 chars): " + jsonContent.substring(0, Math.min(200, jsonContent.length())));
    
    // Write to temporary file first for atomic operation
    File tempFile = new File(jsonFile.getAbsolutePath() + ".tmp." + Thread.currentThread().threadId());
    
    try (FileWriter writer = new FileWriter(tempFile, StandardCharsets.UTF_8)) {
      writer.write(jsonContent);
      writer.flush();
    }
    
    // Atomic rename (on most filesystems)
    java.nio.file.Files.move(tempFile.toPath(), jsonFile.toPath(),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING,
        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    
    // Force file system sync and set timestamp
    jsonFile.setLastModified(System.currentTimeMillis());
    
    // Verify write by reading back
    String readBack = Files.readString(jsonFile.toPath());
    LOGGER.info("Successfully wrote " + jsonArray.size() + " rows to " + jsonFile.getAbsolutePath() + 
                " (size: " + jsonFile.length() + " bytes, verified: " + readBack.substring(0, Math.min(100, readBack.length())) + ")");
    
    // Note: Conversion recording is handled by the calling method
    // which has access to the original HTML source file
  }

  /**
   * Extracts clean text from an HTML element, handling special cases:
   * - Converts <br> elements to spaces
   * - Lets JSoup handle footnotes naturally
   * @param element The HTML element to extract text from
   * @return Clean text string
   */
  private static String extractTextFromElement(Element element) {
    // Clone the element to avoid modifying the original
    Element clone = element.clone();
    
    // Replace <br> tags with spaces
    clone.select("br").append(" ");
    
    // Get the text content - JSoup handles footnotes naturally
    String text = clone.text().trim();
    
    // Clean up multiple spaces
    text = text.replaceAll("\\s+", " ");
    
    return text;
  }
  
  /**
   * Extracts headers from an HTML table.
   * First tries th elements, then falls back to first row td elements.
   */
  /**
   * Extracts raw headers from HTML table without applying any casing transformations.
   * Used for field mapping where we need to match against exact HTML text.
   */
  private static List<String> extractRawHeaders(Element table) {
    // Try th elements first (in thead or first row)
    Elements headerElements = table.select("thead th, tr:first-child th");
    if (!headerElements.isEmpty()) {
      return headerElements.stream()
          .map(HtmlToJsonConverter::extractTextFromElement)
          .collect(Collectors.toList());
    }
    
    // When no th elements found, generate default headers
    // DO NOT use first row td elements as headers - they are data!
    List<String> defaultHeaders = new ArrayList<>();
    int maxCells = table.select("tr").stream()
        .mapToInt(row -> row.select("td, th").size())
        .max()
        .orElse(0);
    for (int i = 0; i < maxCells; i++) {
      // Use col0, col1, col2 format to match expected behavior
      defaultHeaders.add("col" + i);
    }
    return defaultHeaders;
  }

  private static List<String> extractHeaders(Element table, String columnNameCasing) {
    // Try th elements first (in thead or first row)
    Elements headerElements = table.select("thead th, tr:first-child th");
    if (!headerElements.isEmpty()) {
      return headerElements.stream()
          .map(element -> extractTextFromElement(element))
          .map(header -> ConverterUtils.sanitizeIdentifier(org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(header, columnNameCasing)))
          .collect(Collectors.toList());
    }
    
    // No th elements found - generate default headers
    // Use col0, col1, col2 format to match expected behavior
    List<String> defaultHeaders = new ArrayList<>();
    int maxCells = table.select("tr").stream()
        .mapToInt(row -> row.select("td, th").size())
        .max()
        .orElse(0);
    for (int i = 0; i < maxCells; i++) {
      String defaultHeader = "col" + i;
      defaultHeaders.add(ConverterUtils.sanitizeIdentifier(org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(defaultHeader, columnNameCasing)));
    }
    return defaultHeaders;
  }
  
  /**
   * Determines if the first row should be skipped (used as headers).
   */
  private static boolean shouldSkipFirstRow(Element table, List<String> headers) {
    // Only skip first row if it contains th elements (actual header row)
    // Do NOT skip first row when it contains td elements (data row)
    Elements firstRowTh = table.select("tr:first-child th");
    return !firstRowTh.isEmpty();
  }

  /**
   * Checks if extracted JSON files already exist for an HTML file.
   */
  public static boolean hasExtractedFiles(File htmlFile, File outputDir) {
    String baseFileName = ConverterUtils.getBaseFileName(htmlFile.getName(), ".html", ".htm");

    // Check if any files matching the pattern exist
    final String finalBaseFileName = baseFileName;
    File[] files = outputDir.listFiles((dir, name) ->
        name.startsWith(finalBaseFileName + "__") && name.endsWith(".json"));

    return files != null && files.length > 0;
  }
  
  /**
   * Converts HTML tables to JSON with crawling support.
   * Discovers and processes tables from the starting URL and linked pages/files.
   *
   * @param startUrl The starting URL to crawl from
   * @param outputDir The directory to write JSON files to
   * @param config The crawler configuration
   * @return Map of generated JSON files (URL -> List of files)
   * @throws IOException if conversion fails
   */
  public static Map<String, List<File>> convertWithCrawling(String startUrl, File outputDir, 
                                                            CrawlerConfiguration config) throws IOException {
    return convertWithCrawling(startUrl, outputDir, config, "UNCHANGED", "SMART_CASING", outputDir.getParentFile());
  }
  
  /**
   * Converts HTML tables to JSON with crawling support and column name casing.
   *
   * @param startUrl The starting URL to crawl from
   * @param outputDir The directory to write JSON files to
   * @param config The crawler configuration
   * @param columnNameCasing The casing strategy for column names
   * @return Map of generated JSON files (URL -> List of files)
   * @throws IOException if conversion fails
   */
  public static Map<String, List<File>> convertWithCrawling(String startUrl, File outputDir,
                                                            CrawlerConfiguration config,
                                                            String columnNameCasing) throws IOException {
    return convertWithCrawling(startUrl, outputDir, config, columnNameCasing, "SMART_CASING", outputDir.getParentFile());
  }

  /**
   * Converts HTML tables to JSON with crawling support and column/table name casing.
   *
   * @param startUrl The starting URL to crawl from
   * @param outputDir The directory to write JSON files to
   * @param config The crawler configuration
   * @param columnNameCasing The casing strategy for column names
   * @param tableNameCasing The casing strategy for table names
   * @return Map of generated JSON files (URL -> List of files)
   * @throws IOException if conversion fails
   */
  public static Map<String, List<File>> convertWithCrawling(String startUrl, File outputDir,
                                                            CrawlerConfiguration config,
                                                            String columnNameCasing,
                                                            String tableNameCasing, File baseDirectory) throws IOException {
    Map<String, List<File>> allJsonFiles = new HashMap<>();
    
    // Ensure output directory exists
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }
    
    // Perform crawl
    HtmlCrawler crawler = new HtmlCrawler(config);
    CrawlResult crawlResult = crawler.crawl(startUrl);
    
    try {
      LOGGER.info("Crawl complete. Found " + crawlResult.getTotalTablesFound() + " HTML tables and " 
                  + crawlResult.getTotalDataFilesFound() + " data files");
      
      // Process HTML tables from all crawled pages
      for (Map.Entry<String, List<TableInfo>> entry : crawlResult.getHtmlTables().entrySet()) {
        String url = entry.getKey();
        List<TableInfo> tables = entry.getValue();
        
        List<File> jsonFiles = processHtmlTables(url, tables, outputDir, columnNameCasing, tableNameCasing, baseDirectory);
        if (!jsonFiles.isEmpty()) {
          allJsonFiles.put(url, jsonFiles);
        }
      }
      
      // Process downloaded data files
      for (Map.Entry<String, File> entry : crawlResult.getDataFiles().entrySet()) {
        String url = entry.getKey();
        File dataFile = entry.getValue();
        
        List<File> jsonFiles = processDataFile(url, dataFile, outputDir, columnNameCasing, tableNameCasing, baseDirectory);
        if (!jsonFiles.isEmpty()) {
          allJsonFiles.put(url, jsonFiles);
        }
      }
      
      LOGGER.info("Conversion complete. Generated JSON files for " + allJsonFiles.size() + " sources");
      
    } finally {
      crawler.cleanup();
    }
    
    return allJsonFiles;
  }
  
  /**
   * Processes HTML tables from a crawled page.
   */
  private static List<File> processHtmlTables(String url, List<TableInfo> tables, 
                                             File outputDir, String columnNameCasing, String tableNameCasing, File baseDirectory) throws IOException {
    List<File> jsonFiles = new ArrayList<>();
    
    // Create a safe filename from URL
    String urlFileName = sanitizeUrlForFileName(url);
    
    // Download and parse HTML to extract table data
    Document doc = Jsoup.connect(url).get();
    Elements tableElements = doc.select("table");
    
    for (TableInfo tableInfo : tables) {
      try {
        Element table = null;
        if (tableInfo.selector.startsWith("table[index=")) {
          // Handle index-based selection
          if (tableInfo.index < tableElements.size()) {
            table = tableElements.get(tableInfo.index);
          }
        } else {
          table = doc.selectFirst(tableInfo.selector);
        }
        
        if (table != null) {
          File jsonFile = new File(outputDir, urlFileName + "__" + tableInfo.name + ".json");
          writeTableAsJson(table, jsonFile, columnNameCasing);
          jsonFiles.add(jsonFile);
          
          // Record the conversion for refresh tracking (using URL as source identifier)
          File sourceFile = new File(url); // Pseudo-file for URL tracking
          ConversionRecorder.recordConversion(sourceFile, jsonFile, "HTML_TO_JSON", baseDirectory);
          
          LOGGER.fine("Wrote table from " + url + " to " + jsonFile.getName());
        }
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "Failed to process table " + tableInfo.name + " from " + url, e);
      }
    }
    
    return jsonFiles;
  }
  
  /**
   * Processes a downloaded data file (CSV, Excel, etc.).
   */
  private static List<File> processDataFile(String url, File dataFile, 
                                           File outputDir, String columnNameCasing, String tableNameCasing, File baseDirectory) throws IOException {
    List<File> jsonFiles = new ArrayList<>();
    String fileName = dataFile.getName().toLowerCase();
    
    try {
      if (fileName.endsWith(".csv") || fileName.endsWith(".tsv")) {
        // Process as CSV - create a JSON representation
        File jsonFile = new File(outputDir, sanitizeUrlForFileName(url) + ".json");
        // TODO: Implement CSV to JSON conversion
        LOGGER.info("CSV conversion not yet implemented for: " + dataFile.getName());
        
      } else if (fileName.endsWith(".xlsx") || fileName.endsWith(".xls")) {
        // Use Excel converter - convert to directory and find generated files
        File parentDir = dataFile.getParentFile();
        MultiTableExcelToJsonConverter.convertFileToJson(dataFile, parentDir, true, "SMART_CASING", "SMART_CASING", baseDirectory);
        // Find the generated JSON files
        String baseName = dataFile.getName().replaceFirst("\\.[^.]+$", "");
        File[] generated = parentDir.listFiles((dir, name) -> 
            name.startsWith(baseName) && name.endsWith(".json"));
        if (generated != null) {
          for (File f : generated) {
            jsonFiles.add(f);
          }
        }
        LOGGER.info("Converted Excel file " + dataFile.getName() + " to " + jsonFiles.size() + " JSON files");
        
      } else if (fileName.endsWith(".json")) {
        // Copy JSON file directly
        File targetFile = new File(outputDir, sanitizeUrlForFileName(url) + ".json");
        java.nio.file.Files.copy(dataFile.toPath(), targetFile.toPath(), 
                                 java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        jsonFiles.add(targetFile);
        LOGGER.info("Copied JSON file to " + targetFile.getName());
        
      } else if (fileName.endsWith(".parquet")) {
        // Parquet files can be used directly, no conversion needed
        LOGGER.info("Parquet file " + dataFile.getName() + " can be used directly");
        
      } else {
        LOGGER.warning("Unsupported file type: " + fileName);
      }
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Failed to process data file: " + dataFile.getName(), e);
    }
    
    return jsonFiles;
  }
  
  /**
   * Sanitizes a URL to create a safe filename.
   */
  private static String sanitizeUrlForFileName(String url) {
    // Remove protocol
    String name = url.replaceFirst("^https?://", "");
    
    // Replace special characters
    name = name.replaceAll("[^a-zA-Z0-9.-]", "_");
    
    // Limit length
    if (name.length() > 100) {
      name = name.substring(0, 100);
    }
    
    // Remove trailing underscores
    name = name.replaceAll("_+$", "");
    
    return name;
  }
}
