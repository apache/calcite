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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Converts HTML tables to JSON files for processing by Arrow Dataset.
 * Similar to MultiTableExcelToJsonConverter but for HTML files.
 */
public class HtmlToJsonConverter {
  private static final Logger LOGGER = Logger.getLogger(HtmlToJsonConverter.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Converts all tables in an HTML file to separate JSON files.
   *
   * @param htmlFile The HTML file to convert
   * @param outputDir The directory to write JSON files to
   * @return List of generated JSON files
   * @throws IOException if conversion fails
   */
  public static List<File> convert(File htmlFile, File outputDir) throws IOException {
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
      String baseFileName = htmlFile.getName();
      if (baseFileName.toLowerCase().endsWith(".html")) {
        baseFileName = baseFileName.substring(0, baseFileName.length() - 5);
      } else if (baseFileName.toLowerCase().endsWith(".htm")) {
        baseFileName = baseFileName.substring(0, baseFileName.length() - 4);
      }

      File jsonFile = new File(outputDir, baseFileName + "_" + tableName + ".json");

      try {
        // Get the actual table element using the selector
        Element table = doc.selectFirst(tableInfo.selector);
        if (table != null) {
          writeTableAsJson(table, jsonFile);
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
  private static void writeTableAsJson(Element table, File jsonFile)
      throws IOException {
    ArrayNode jsonArray = MAPPER.createArrayNode();

    // Get headers from th elements
    List<String> headers = new ArrayList<>();
    Elements headerElements = table.select("th");
    if (!headerElements.isEmpty()) {
      for (Element th : headerElements) {
        headers.add(th.text());
      }
    }

    // Get rows
    Elements rows = table.select("tr");
    boolean firstRowAsHeader = headers.isEmpty();

    for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
      Element row = rows.get(rowIndex);
      Elements cells = row.select("td");

      // Skip header row if we already have headers
      if (rowIndex == 0 && !firstRowAsHeader && cells.isEmpty()) {
        continue;
      }

      // Use first row as headers if needed
      if (firstRowAsHeader && rowIndex == 0 && !cells.isEmpty()) {
        for (Element cell : cells) {
          headers.add(cell.text());
        }
        continue;
      }

      // Skip empty rows
      if (cells.isEmpty()) {
        continue;
      }
      ObjectNode jsonRow = MAPPER.createObjectNode();

      for (int i = 0; i < Math.min(headers.size(), cells.size()); i++) {
        String header = headers.get(i);
        String value = cells.get(i).text();

        // Try to infer type
        if (value == null || value.isEmpty()) {
          jsonRow.putNull(header);
        } else if (isNumeric(value)) {
          if (value.contains(".")) {
            jsonRow.put(header, Double.parseDouble(value));
          } else {
            jsonRow.put(header, Long.parseLong(value));
          }
        } else if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
          jsonRow.put(header, Boolean.parseBoolean(value));
        } else {
          jsonRow.put(header, value);
        }
      }

      if (jsonRow.size() > 0) {
        jsonArray.add(jsonRow);
      }
    }

    // Write to file
    try (FileWriter writer = new FileWriter(jsonFile)) {
      MAPPER.writerWithDefaultPrettyPrinter().writeValue(writer, jsonArray);
    }
  }

  private static boolean isNumeric(String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    try {
      Double.parseDouble(str);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  /**
   * Checks if extracted JSON files already exist for an HTML file.
   */
  public static boolean hasExtractedFiles(File htmlFile, File outputDir) {
    String baseFileName = htmlFile.getName();
    if (baseFileName.toLowerCase().endsWith(".html")) {
      baseFileName = baseFileName.substring(0, baseFileName.length() - 5);
    } else if (baseFileName.toLowerCase().endsWith(".htm")) {
      baseFileName = baseFileName.substring(0, baseFileName.length() - 4);
    }

    // Check if any files matching the pattern exist
    final String finalBaseFileName = baseFileName;
    File[] files = outputDir.listFiles((dir, name) ->
        name.startsWith(finalBaseFileName + "_") && name.endsWith(".json"));

    return files != null && files.length > 0;
  }
}
