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

import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Scanner that extracts tables from Word (DOCX) documents.
 * Supports:
 * - Standard Word tables with headers
 * - Optional table titles (paragraph above table)
 * - Group headers (merged cells spanning columns)
 * - Multiple tables per document
 * - Complex table structures with merged cells
 */
public final class DocxTableScanner {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(DocxTableScanner.class));

  private DocxTableScanner() {
    // Prevent instantiation
  }

  /**
   * Scans a DOCX file and extracts all tables, converting them to JSON files.
   */
  public static void scanAndConvertTables(File inputFile) throws IOException {
    LOGGER.debug("Scanning DOCX file for tables: " + inputFile.getName());

    // Acquire read lock on source file
    SourceFileLockManager.LockHandle lockHandle = null;
    try {
      lockHandle = SourceFileLockManager.acquireReadLock(inputFile);
      LOGGER.debug("Acquired read lock on DOCX file: " + inputFile.getPath());
    } catch (IOException e) {
      LOGGER.warn("Could not acquire lock on file: "
          + inputFile.getPath()
          + " - proceeding without lock");
    }

    try {
      List<DocxTable> tables = extractTables(inputFile);

      if (tables.isEmpty()) {
        LOGGER.debug("No tables found in DOCX file: " + inputFile.getName());
        return;
      }

      LOGGER.debug("Found " + tables.size() + " tables in DOCX file");

      // Convert each table to JSON
      String baseName = inputFile.getName().replaceFirst("\\.[^.]+$", "").toLowerCase()
          .replaceAll("[^a-z0-9_]", "_");
      ObjectMapper mapper = new ObjectMapper();

      for (int i = 0; i < tables.size(); i++) {
        DocxTable table = tables.get(i);
        String jsonFileName = generateFileName(baseName, table.title, i, tables.size());

        File jsonFile = new File(inputFile.getParent(), jsonFileName);
        LOGGER.debug("Writing JSON file: " + jsonFileName);

        try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
          mapper.writerWithDefaultPrettyPrinter().writeValue(writer, table.data);
        }
      }
    } finally {
      if (lockHandle != null) {
        lockHandle.close();
        LOGGER.debug("Released read lock on DOCX file");
      }
    }
  }

  /**
   * Extracts all tables from a DOCX file.
   */
  private static List<DocxTable> extractTables(File inputFile) throws IOException {
    List<DocxTable> tables = new ArrayList<>();

    try (FileInputStream fis = new FileInputStream(inputFile);
         XWPFDocument document = new XWPFDocument(fis)) {

      List<XWPFTable> wordTables = document.getTables();
      List<XWPFParagraph> paragraphs = document.getParagraphs();

      for (int i = 0; i < wordTables.size(); i++) {
        XWPFTable wordTable = wordTables.get(i);

        // Try to find a title from the paragraph immediately before this table
        String title = findTableTitle(document, wordTable, paragraphs);

        DocxTable table = parseTable(wordTable, title, i);
        if (table != null && !table.data.isEmpty()) {
          tables.add(table);
        }
      }
    }

    return tables;
  }

  /**
   * Finds a title for the table by looking at preceding paragraphs.
   */
  private static String findTableTitle(XWPFDocument document, XWPFTable table,
      List<XWPFParagraph> paragraphs) {
    // Get body elements in document order (paragraphs and tables interspersed)
    List<Object> bodyElements = new ArrayList<>();

    // Use the document body elements to preserve order
    for (org.apache.poi.xwpf.usermodel.IBodyElement element : document.getBodyElements()) {
      bodyElements.add(element);
    }

    // Find our table in the body elements
    int tableIndex = -1;
    for (int i = 0; i < bodyElements.size(); i++) {
      if (bodyElements.get(i) == table) {
        tableIndex = i;
        break;
      }
    }

    // Look for the closest preceding paragraph with text
    if (tableIndex > 0) {
      for (int i = tableIndex - 1; i >= 0; i--) {
        Object element = bodyElements.get(i);
        if (element instanceof XWPFParagraph) {
          XWPFParagraph para = (XWPFParagraph) element;
          String text = para.getText().trim();
          if (!text.isEmpty()) {
            // Remove common heading markers
            // Remove markdown-style headers
            text = Pattern.compile("^#+\\s*").matcher(text).replaceAll("");
            // Remove numbered headers
            text = Pattern.compile("^\\d+\\.\\s*").matcher(text).replaceAll("");
            return text;
          }
        } else if (element instanceof XWPFTable) {
          // Stop if we hit another table
          break;
        }
      }
    }

    return null;
  }

  /**
   * Parses a Word table into our internal representation.
   */
  private static DocxTable parseTable(XWPFTable wordTable, String title, int index) {
    List<XWPFTableRow> rows = wordTable.getRows();
    if (rows.isEmpty()) {
      return null;
    }

    // Determine header rows by analyzing first few rows
    int headerRowCount = determineHeaderRowCount(rows);
    if (headerRowCount == 0) {
      LOGGER.warn("No header rows detected in table " + index);
      return null;
    }

    // Extract header rows
    List<List<String>> headerRows = new ArrayList<>();
    for (int i = 0; i < headerRowCount; i++) {
      List<String> headerRow = extractRowCells(rows.get(i));
      if (!headerRow.isEmpty()) {
        headerRows.add(headerRow);
      }
    }

    // Build column headers (handle merged cells and group headers)
    List<String> columnHeaders = buildColumnHeaders(headerRows);

    // Parse data rows
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode data = mapper.createArrayNode();

    for (int i = headerRowCount; i < rows.size(); i++) {
      List<String> cells = extractRowCells(rows.get(i));
      if (cells.isEmpty() || isEmptyRow(cells)) {
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

    DocxTable table = new DocxTable();
    table.title = title;
    table.data = data;
    return table;
  }

  /**
   * Determines how many rows at the beginning are headers.
   */
  private static int determineHeaderRowCount(List<XWPFTableRow> rows) {
    if (rows.isEmpty()) {
      return 0;
    }

    // Simple heuristic: assume first row is always header
    // TODO: Could be enhanced to detect multiple header rows based on:
    // - Cell formatting (bold, different background)
    // - Content analysis (text vs numbers)
    // - Merged cells pattern

    int headerCount = 1;

    // Check if second row also looks like a header (for group headers)
    if (rows.size() > 1) {
      List<String> firstRow = extractRowCells(rows.get(0));
      List<String> secondRow = extractRowCells(rows.get(1));

      // If first row has fewer cells than second, it might be a group header
      if (firstRow.size() < secondRow.size()) {
        headerCount = 2;
      }

      // If second row is mostly text and third row has numbers, second might be header
      if (rows.size() > 2 && looksLikeHeader(secondRow)
          && !looksLikeHeader(extractRowCells(rows.get(2)))) {
        headerCount = Math.max(headerCount, 2);
      }
    }

    return Math.min(headerCount, 3); // Max 3 header rows
  }

  /**
   * Extracts text content from all cells in a row.
   */
  private static List<String> extractRowCells(XWPFTableRow row) {
    List<String> cells = new ArrayList<>();
    for (XWPFTableCell cell : row.getTableCells()) {
      String cellText = cell.getText().trim();
      cells.add(cellText);
    }
    return cells;
  }

  /**
   * Builds column headers from potentially multiple header rows.
   */
  private static List<String> buildColumnHeaders(List<List<String>> headerRows) {
    if (headerRows.isEmpty()) {
      return new ArrayList<>();
    }

    if (headerRows.size() == 1) {
      // Simple case: single header row - convert to lowercase
      List<String> lowercaseHeaders = new ArrayList<>();
      for (String header : headerRows.get(0)) {
        lowercaseHeaders.add(header.toLowerCase().replaceAll("[^a-z0-9_]", "_"));
      }
      return lowercaseHeaders;
    }

    // Multiple header rows: combine group headers with detail headers
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

        if (cellValue != null && !cellValue.isEmpty()) {
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

    // Combine group headers with detail headers - convert to lowercase
    for (int col = 0; col < detailHeaders.size(); col++) {
      String header = detailHeaders.get(col);
      String prefix = groupPrefixes.get(col);
      if (prefix != null && !prefix.isEmpty()) {
        header = prefix + "_" + header;
      }
      // Convert to lowercase and sanitize for use as column name
      header = header.toLowerCase().replaceAll("[^a-z0-9_]", "_");
      combinedHeaders.add(header);
    }

    return combinedHeaders;
  }

  /**
   * Checks if a row looks like a header (mostly text, not numbers).
   */
  private static boolean looksLikeHeader(List<String> cells) {
    if (cells.isEmpty()) {
      return false;
    }

    int textCells = 0;
    int numericCells = 0;

    for (String cell : cells) {
      cell = cell.trim();
      if (cell.isEmpty()) {
        continue;
      }

      if (cell.matches("^\\d+(\\.\\d+)?$")) {
        numericCells++;
      } else {
        textCells++;
      }
    }

    // Headers should be mostly text
    return textCells > numericCells;
  }

  /**
   * Checks if a row is empty (all cells are empty).
   */
  private static boolean isEmptyRow(List<String> cells) {
    for (String cell : cells) {
      if (!cell.trim().isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Generates a file name for the JSON output.
   */
  private static String generateFileName(String baseName, String tableTitle,
      int tableIndex, int totalTables) {
    StringBuilder fileName = new StringBuilder(baseName);

    if (tableTitle != null && !tableTitle.isEmpty()) {
      fileName.append("__").append(sanitizeIdentifier(tableTitle).toLowerCase());
    } else if (totalTables > 1) {
      fileName.append("__table").append(tableIndex + 1);
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

  private static String sanitizeIdentifier(String identifier) {
    // Replace non-alphanumeric characters with underscore
    StringBuilder result = new StringBuilder();
    for (char c : identifier.toCharArray()) {
      if (Character.isLetterOrDigit(c) || c == '_' || c == '-') {
        result.append(c);
      } else {
        result.append('_');
      }
    }

    // Collapse multiple underscores
    String str = result.toString();
    while (str.contains("__")) {
      str = str.replace("__", "_");
    }

    // Remove leading/trailing underscores
    str = Pattern.compile("^_+|_+$").matcher(str).replaceAll("");

    return str;
  }

  /**
   * Container for a parsed DOCX table.
   */
  private static class DocxTable {
    String title;
    ArrayNode data;
  }
}
