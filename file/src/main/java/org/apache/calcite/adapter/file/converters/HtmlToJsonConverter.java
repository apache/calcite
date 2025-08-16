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
import java.util.List;
import java.util.Map;
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
  public static List<File> convert(File htmlFile, File outputDir) throws IOException {
    return convert(htmlFile, outputDir, "UNCHANGED");
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
  public static List<File> convert(File htmlFile, File outputDir, String columnNameCasing) throws IOException {
    return convert(htmlFile, outputDir, columnNameCasing, "SMART_CASING");
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
  public static List<File> convert(File htmlFile, File outputDir, String columnNameCasing, String tableNameCasing) throws IOException {
    List<File> jsonFiles = new ArrayList<>();

    // Use HtmlTableScanner to find tables - make sure we re-scan each time
    Source source = Sources.of(htmlFile);
    List<HtmlTableScanner.TableInfo> tableInfos = HtmlTableScanner.scanTables(source, tableNameCasing);

    LOGGER.info("Found " + tableInfos.size() + " tables in " + htmlFile.getName());

    // Ensure output directory exists
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }

    // Parse HTML to extract table data - ensure we re-read the file
    // Force re-reading by not using any cached document
    Document doc = Jsoup.parse(htmlFile, "UTF-8");
    LOGGER.info("Parsed HTML file: " + htmlFile.getName() + " (size: " + htmlFile.length() + " bytes, lastModified: " + htmlFile.lastModified() + ")");
    Elements tables = doc.select("table");
    LOGGER.info("Found " + tables.size() + " table elements in DOM");

    // Convert each table to a JSON file
    for (int i = 0; i < tableInfos.size(); i++) {
      HtmlTableScanner.TableInfo tableInfo = tableInfos.get(i);
      String tableName = tableInfo.name;

      // Create filename: original_tablename.json
      String baseFileName = ConverterUtils.getBaseFileName(htmlFile.getName(), ".html", ".htm");

      File jsonFile = new File(outputDir, baseFileName + "__" + tableName + ".json");

      try {
        // Get the actual table element using the selector or index
        Element table = null;
        if (tableInfo.selector.startsWith("table[index=")) {
          // Handle index-based selection directly
          if (tableInfo.index < tables.size()) {
            table = tables.get(tableInfo.index);
          }
        } else {
          // Use regular CSS selector
          table = doc.selectFirst(tableInfo.selector);
        }
        
        if (table != null) {
          // Delete existing file to ensure fresh write
          if (jsonFile.exists()) {
            jsonFile.delete();
            LOGGER.info("Deleted existing JSON file for refresh: " + jsonFile.getName());
          }
          
          // Log table content to debug
          LOGGER.info("Table HTML content: " + table.html().substring(0, Math.min(200, table.html().length())));
          
          writeTableAsJson(table, jsonFile, columnNameCasing);
          jsonFiles.add(jsonFile);
          
          // Record the conversion for refresh tracking
          ConversionRecorder.recordConversion(htmlFile, jsonFile, "HTML_TO_JSON");
          
          LOGGER.info("Wrote table to " + jsonFile.getAbsolutePath());
        }
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "Failed to write table " + tableName + " to JSON", e);
        // Continue with other tables
      }
    }

    return jsonFiles;
  }


  /**
   * Writes a table as a JSON array file.
   */
  private static void writeTableAsJson(Element table, File jsonFile, String columnNameCasing)
      throws IOException {
    ArrayNode jsonArray = MAPPER.createArrayNode();

    // Extract headers and apply column name casing
    List<String> headers = extractHeaders(table, columnNameCasing);
    
    // Process data rows
    Elements rows = table.select("tr");
    boolean skipFirstRow = shouldSkipFirstRow(table, headers);
    
    LOGGER.fine("Processing table with " + rows.size() + " rows, skipFirstRow=" + skipFirstRow);
    
    for (int rowIndex = skipFirstRow ? 1 : 0; rowIndex < rows.size(); rowIndex++) {
      Element row = rows.get(rowIndex);
      Elements cells = row.select("td");
      
      if (cells.isEmpty()) {
        continue;
      }
      
      ObjectNode jsonRow = MAPPER.createObjectNode();
      for (int i = 0; i < Math.min(headers.size(), cells.size()); i++) {
        String header = headers.get(i);
        String value = cells.get(i).text();
        ConverterUtils.setJsonValueWithTypeInference(jsonRow, header, value);
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
   * Extracts headers from an HTML table.
   * First tries th elements, then falls back to first row td elements.
   */
  private static List<String> extractHeaders(Element table, String columnNameCasing) {
    // Try th elements first (in thead or first row)
    Elements headerElements = table.select("thead th, tr:first-child th");
    if (!headerElements.isEmpty()) {
      return headerElements.stream()
          .map(Element::text)
          .map(header -> org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(header, columnNameCasing))
          .collect(Collectors.toList());
    }
    
    // Fall back to first row td elements
    Elements firstRowCells = table.select("tr:first-child td");
    if (!firstRowCells.isEmpty()) {
      return firstRowCells.stream()
          .map(Element::text)
          .map(header -> org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(header, columnNameCasing))
          .collect(Collectors.toList());
    }
    
    // Default headers if no headers found
    List<String> defaultHeaders = new ArrayList<>();
    int maxCells = table.select("tr").stream()
        .mapToInt(row -> row.select("td").size())
        .max()
        .orElse(0);
    for (int i = 0; i < maxCells; i++) {
      String defaultHeader = "column" + (i + 1);
      defaultHeaders.add(org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(defaultHeader, columnNameCasing));
    }
    return defaultHeaders;
  }
  
  /**
   * Determines if the first row should be skipped (used as headers).
   */
  private static boolean shouldSkipFirstRow(Element table, List<String> headers) {
    // Skip first row if we got headers from td elements in first row
    Elements firstRowTh = table.select("tr:first-child th");
    if (!firstRowTh.isEmpty()) {
      return false; // Headers came from th, don't skip
    }
    
    Elements firstRowTd = table.select("tr:first-child td");
    return !firstRowTd.isEmpty() && !headers.isEmpty();
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
    return convertWithCrawling(startUrl, outputDir, config, "UNCHANGED", "SMART_CASING");
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
    return convertWithCrawling(startUrl, outputDir, config, columnNameCasing, "SMART_CASING");
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
                                                            String tableNameCasing) throws IOException {
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
        
        List<File> jsonFiles = processHtmlTables(url, tables, outputDir, columnNameCasing, tableNameCasing);
        if (!jsonFiles.isEmpty()) {
          allJsonFiles.put(url, jsonFiles);
        }
      }
      
      // Process downloaded data files
      for (Map.Entry<String, File> entry : crawlResult.getDataFiles().entrySet()) {
        String url = entry.getKey();
        File dataFile = entry.getValue();
        
        List<File> jsonFiles = processDataFile(url, dataFile, outputDir, columnNameCasing, tableNameCasing);
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
                                             File outputDir, String columnNameCasing, String tableNameCasing) throws IOException {
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
          ConversionRecorder.recordConversion(sourceFile, jsonFile, "HTML_TO_JSON");
          
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
                                           File outputDir, String columnNameCasing, String tableNameCasing) throws IOException {
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
        MultiTableExcelToJsonConverter.convertFileToJson(dataFile, true);
        // Find the generated JSON files
        File parentDir = dataFile.getParentFile();
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
