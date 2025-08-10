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
import java.util.ArrayList;
import java.util.List;
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
    List<File> jsonFiles = new ArrayList<>();

    // Use HtmlTableScanner to find tables
    Source source = Sources.of(htmlFile);
    List<HtmlTableScanner.TableInfo> tableInfos = HtmlTableScanner.scanTables(source);

    LOGGER.info("Found " + tableInfos.size() + " tables in " + htmlFile.getName());

    // Ensure output directory exists
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }

    // Parse HTML to extract table data
    Document doc = Jsoup.parse(htmlFile, "UTF-8");
    Elements tables = doc.select("table");

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
          writeTableAsJson(table, jsonFile, columnNameCasing);
          jsonFiles.add(jsonFile);
          LOGGER.fine("Wrote table to " + jsonFile.getAbsolutePath());
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

    // Write to file
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      MAPPER.writerWithDefaultPrettyPrinter().writeValue(writer, jsonArray);
    }
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
}
