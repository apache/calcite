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

import org.apache.calcite.adapter.file.cache.SourceFileLockManager;
import org.apache.calcite.util.trace.CalciteLogger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Scanner that extracts tables from Markdown files.
 * Supports:
 * - Standard Markdown tables with pipe separators
 * - Optional table titles (heading above table)
 * - Group headers (spanning cells using multiple dashes)
 * - Multiple tables per file
 * - GFM (GitHub Flavored Markdown) table extensions
 */
public final class MarkdownTableScanner {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(MarkdownTableScanner.class));

  // Pattern to match markdown table rows
  private static final Pattern TABLE_ROW_PATTERN = Pattern.compile("^\\s*\\|(.+)\\|\\s*$");
  private static final Pattern SEPARATOR_ROW_PATTERN =
      Pattern.compile("^\\s*\\|[\\s\\-:|]+\\|\\s*$");
  private static final Pattern HEADING_PATTERN = Pattern.compile("^#+\\s+(.+)$");

  private MarkdownTableScanner() {
    // Prevent instantiation
  }

  /**
   * Scans a Markdown file and extracts all tables, converting them to JSON files.
   * 
   * @param inputFile The Markdown file to scan
   * @param outputDir The directory to write JSON files to
   */
  public static void scanAndConvertTables(File inputFile, File outputDir) throws IOException {
    scanAndConvertTables(inputFile, outputDir, null);
  }
  
  public static void scanAndConvertTables(File inputFile, File outputDir, String relativePath) throws IOException {
    LOGGER.debug("Scanning Markdown file for tables: " + inputFile.getName());

    // Acquire read lock on source file
    SourceFileLockManager.LockHandle lockHandle = null;
    try {
      lockHandle = SourceFileLockManager.acquireReadLock(inputFile);
      LOGGER.debug("Acquired read lock on Markdown file: " + inputFile.getPath());
    } catch (IOException e) {
      LOGGER.warn("Could not acquire lock on file: " + inputFile.getPath()
          + " - proceeding without lock");
    }

    try {
      List<MarkdownTable> tables = extractTables(inputFile);

      if (tables.isEmpty()) {
        LOGGER.debug("No tables found in Markdown file: " + inputFile.getName());
        return;
      }

      LOGGER.debug("Found " + tables.size() + " tables in Markdown file");

      // Convert each table to JSON
      String baseName = toPascalCase(inputFile.getName().replaceFirst("\\.[^.]+$", ""));
      
      // Include directory structure in the basename if relativePath is provided
      if (relativePath != null && relativePath.contains(File.separator)) {
        String dirPrefix = relativePath.substring(0, relativePath.lastIndexOf(File.separator))
            .replace(File.separator, "_");
        baseName = dirPrefix + "_" + baseName;
      }
      
      ObjectMapper mapper = new ObjectMapper();

      for (int i = 0; i < tables.size(); i++) {
        MarkdownTable table = tables.get(i);
        String jsonFileName = generateFileName(baseName, table.title, i, tables.size());

        File jsonFile = new File(outputDir, jsonFileName);
        LOGGER.debug("Writing JSON file: " + jsonFileName);

        try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
          mapper.writerWithDefaultPrettyPrinter().writeValue(writer, table.data);
          
          // Record the conversion for refresh tracking - use schema directory for metadata
          ConversionRecorder.recordConversion(inputFile, jsonFile, "MARKDOWN_TO_JSON", outputDir.getParentFile());
        }
      }
    } finally {
      if (lockHandle != null) {
        lockHandle.close();
        LOGGER.debug("Released read lock on Markdown file");
      }
    }
  }

  /**
   * Extracts all tables from a Markdown file.
   */
  private static List<MarkdownTable> extractTables(File inputFile) throws IOException {
    List<MarkdownTable> tables = new ArrayList<>();

    try (BufferedReader reader =
        new BufferedReader(new FileReader(inputFile, StandardCharsets.UTF_8))) {

      String line;
      String lastHeading = null;
      List<String> currentTableLines = new ArrayList<>();
      boolean inTable = false;

      while ((line = reader.readLine()) != null) {
        // Check for headings
        Matcher headingMatcher = HEADING_PATTERN.matcher(line);
        if (headingMatcher.matches() && !inTable) {
          lastHeading = headingMatcher.group(1).trim();
          continue;
        }

        // Check if line is a table row
        if (TABLE_ROW_PATTERN.matcher(line).matches()) {
          if (!inTable) {
            inTable = true;
            currentTableLines.clear();
          }
          currentTableLines.add(line);
        } else if (inTable) {
          // End of table
          MarkdownTable table = parseTable(currentTableLines, lastHeading);
          if (table != null && !table.data.isEmpty()) {
            tables.add(table);
          }
          inTable = false;
          currentTableLines.clear();

          // Check if this line is a new heading
          if (headingMatcher.matches()) {
            lastHeading = headingMatcher.group(1).trim();
          } else {
            lastHeading = null;
          }
        } else if (!line.trim().isEmpty()) {
          // Non-empty line that's not a table - clear heading
          lastHeading = null;
        }
      }

      // Handle table at end of file
      if (inTable && !currentTableLines.isEmpty()) {
        MarkdownTable table = parseTable(currentTableLines, lastHeading);
        if (table != null && !table.data.isEmpty()) {
          tables.add(table);
        }
      }
    }

    return tables;
  }

  /**
   * Parses a Markdown table from raw lines.
   */
  private static MarkdownTable parseTable(List<String> lines, String title) {
    if (lines.size() < 3) {
      // Minimum: header, separator, one data row
      return null;
    }

    // Find the separator row
    int separatorIndex = -1;
    for (int i = 0; i < lines.size(); i++) {
      if (SEPARATOR_ROW_PATTERN.matcher(lines.get(i)).matches()) {
        separatorIndex = i;
        break;
      }
    }

    if (separatorIndex == -1 || separatorIndex == 0) {
      return null;
    }

    // Extract header rows (all rows before separator)
    List<List<String>> headerRows = new ArrayList<>();
    for (int i = 0; i < separatorIndex; i++) {
      List<String> cells = parseCells(lines.get(i));
      if (!cells.isEmpty()) {
        headerRows.add(cells);
      }
    }

    if (headerRows.isEmpty()) {
      return null;
    }

    // Build column headers (handle group headers if multiple header rows)
    List<String> columnHeaders = buildColumnHeaders(headerRows);

    // Parse data rows
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode data = mapper.createArrayNode();

    for (int i = separatorIndex + 1; i < lines.size(); i++) {
      List<String> cells = parseCells(lines.get(i));
      if (cells.isEmpty()) {
        continue;
      }

      ObjectNode row = mapper.createObjectNode();
      for (int j = 0; j < Math.min(cells.size(), columnHeaders.size()); j++) {
        String value = cells.get(j).trim();
        if (!value.isEmpty()) {
          row.put(columnHeaders.get(j), value);
        }
      }

      if (row.size() > 0) {
        data.add(row);
      }
    }

    MarkdownTable table = new MarkdownTable();
    table.title = title;
    table.data = data;
    return table;
  }

  /**
   * Parses cells from a table row.
   */
  private static List<String> parseCells(String line) {
    List<String> cells = new ArrayList<>();

    // Remove leading and trailing pipes and spaces
    line = line.trim();
    if (line.startsWith("|")) {
      line = line.substring(1);
    }
    if (line.endsWith("|")) {
      line = line.substring(0, line.length() - 1);
    }

    // Split by pipe, handling escaped pipes
    String[] parts = line.split("(?<!\\\\)\\|");
    for (String part : parts) {
      // Unescape pipes and trim
      String cell = part.replace("\\|", "|").trim();
      cells.add(cell);
    }

    return cells;
  }

  /**
   * Builds column headers from potentially multiple header rows.
   */
  private static List<String> buildColumnHeaders(List<List<String>> headerRows) {
    if (headerRows.size() == 1) {
      // Simple case: single header row
      return headerRows.get(0);
    }

    // Multiple header rows: assume last row is detail headers, others are groups
    List<String> detailHeaders = headerRows.get(headerRows.size() - 1);
    List<String> combinedHeaders = new ArrayList<>();

    // Build group prefixes for each column
    Map<Integer, String> groupPrefixes = new HashMap<>();

    for (int i = 0; i < headerRows.size() - 1; i++) {
      List<String> groupRow = headerRows.get(i);
      String currentGroup = null;
      int groupStart = 0;

      for (int col = 0; col <= groupRow.size(); col++) {
        String cellValue = (col < groupRow.size()) ? groupRow.get(col).trim() : null;

        if (cellValue != null && !cellValue.isEmpty() && !cellValue.equals("-")) {
          // New group starts
          if (currentGroup != null) {
            // Apply previous group to its columns
            for (int c = groupStart; c < col; c++) {
              String existing = groupPrefixes.get(c);
              groupPrefixes.put(c, existing == null ? currentGroup : existing + "_" + currentGroup);
            }
          }
          currentGroup = cellValue;
          groupStart = col;
        }

        // End of row - apply last group
        if (col == groupRow.size() && currentGroup != null) {
          for (int c = groupStart; c < detailHeaders.size(); c++) {
            String existing = groupPrefixes.get(c);
            groupPrefixes.put(c, existing == null ? currentGroup : existing + "_" + currentGroup);
          }
        }
      }
    }

    // Combine group headers with detail headers
    for (int col = 0; col < detailHeaders.size(); col++) {
      String header = detailHeaders.get(col);
      String prefix = groupPrefixes.get(col);
      if (prefix != null && !prefix.isEmpty()) {
        header = prefix + "_" + header;
      }
      combinedHeaders.add(header);
    }

    return combinedHeaders;
  }

  /**
   * Generates a file name for the JSON output.
   */
  private static String generateFileName(String baseName, String tableTitle,
      int tableIndex, int totalTables) {
    StringBuilder fileName = new StringBuilder(baseName);

    if (tableTitle != null && !tableTitle.isEmpty()) {
      fileName.append("__").append(ConverterUtils.sanitizeIdentifier(tableTitle));
    } else if (totalTables > 1) {
      fileName.append("__Table").append(tableIndex + 1);
    }

    fileName.append(".json");
    return fileName.toString();
  }

  private static String toPascalCase(String input) {
    StringBuilder result = new StringBuilder();
    boolean capitalizeNext = true;
    for (char c : input.toCharArray()) {
      if (Character.isWhitespace(c) || c == '_' || c == '-') {
        capitalizeNext = true;
      } else if (capitalizeNext) {
        result.append(Character.toUpperCase(c));
        capitalizeNext = false;
      } else {
        result.append(Character.toLowerCase(c));
      }
    }
    return result.toString();
  }


  /**
   * Container for a parsed Markdown table.
   */
  private static class MarkdownTable {
    String title;
    ArrayNode data;
  }
}
