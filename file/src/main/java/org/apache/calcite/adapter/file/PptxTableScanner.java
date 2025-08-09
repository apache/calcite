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

import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTable;
import org.apache.poi.xslf.usermodel.XSLFTableCell;
import org.apache.poi.xslf.usermodel.XSLFTableRow;
import org.apache.poi.xslf.usermodel.XSLFTextShape;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
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
 * Scanner that extracts tables from PowerPoint (PPTX) presentations.
 * Supports:
 * - Standard PowerPoint tables with headers
 * - Optional table titles (text shape above table on same slide)
 * - Multiple tables per slide
 * - Multiple slides with tables
 * - Complex table structures with merged cells
 * - Slide titles as table context
 */
public final class PptxTableScanner {
  private static final Logger LOGGER = LoggerFactory.getLogger(PptxTableScanner.class);

  private PptxTableScanner() {
    // Prevent instantiation
  }

  /**
   * Scans a PPTX file and extracts all tables, converting them to JSON files.
   */
  public static void scanAndConvertTables(File inputFile) throws IOException {
    LOGGER.debug("Scanning PPTX file for tables: {}", inputFile.getName());

    // Acquire read lock on source file
    SourceFileLockManager.LockHandle lockHandle = null;
    try {
      lockHandle = SourceFileLockManager.acquireReadLock(inputFile);
      LOGGER.debug("Acquired read lock on PPTX file: {}", inputFile.getPath());
    } catch (IOException e) {
      LOGGER.warn("Could not acquire lock on file: {} - proceeding without lock", 
          inputFile.getPath());
    }

    try {
      List<PptxTable> tables = extractTables(inputFile);

      if (tables.isEmpty()) {
        LOGGER.debug("No tables found in PPTX file: {}", inputFile.getName());
        return;
      }

      LOGGER.debug("Found {} tables in PPTX file", tables.size());

      // Convert each table to JSON
      String baseName = inputFile.getName().replaceFirst("\\.[^.]+$", "").toLowerCase()
          .replaceAll("[^a-z0-9_]", "_");
      ObjectMapper mapper = new ObjectMapper();
      
      // First pass: generate base filenames for all tables
      Map<String, Integer> fileNameCounts = new HashMap<>();
      List<String> baseFileNames = new ArrayList<>();
      for (PptxTable table : tables) {
        String baseFileName = generateBaseFileName(baseName, table.title, table.slideTitle, 
            table.slideNumber);
        baseFileNames.add(baseFileName);
        fileNameCounts.put(baseFileName, fileNameCounts.getOrDefault(baseFileName, 0) + 1);
      }
      
      // Second pass: add indices only where there are duplicates
      Map<String, Integer> currentIndices = new HashMap<>();
      for (int i = 0; i < tables.size(); i++) {
        PptxTable table = tables.get(i);
        String baseFileName = baseFileNames.get(i);
        String jsonFileName;
        
        if (fileNameCounts.get(baseFileName) > 1) {
          // Duplicate name, add index
          int index = currentIndices.getOrDefault(baseFileName, 0) + 1;
          currentIndices.put(baseFileName, index);
          jsonFileName = baseFileName + "_" + index + ".json";
        } else {
          // Unique name, no index needed
          jsonFileName = baseFileName + ".json";
        }

        File jsonFile = new File(inputFile.getParent(), jsonFileName);
        LOGGER.debug("Writing JSON file: {}", jsonFileName);

        try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
          mapper.writerWithDefaultPrettyPrinter().writeValue(writer, table.data);
        }
      }
    } finally {
      if (lockHandle != null) {
        lockHandle.close();
        LOGGER.debug("Released read lock on PPTX file");
      }
    }
  }

  /**
   * Extracts all tables from a PPTX file.
   */
  private static List<PptxTable> extractTables(File inputFile) throws IOException {
    List<PptxTable> tables = new ArrayList<>();

    try (FileInputStream fis = new FileInputStream(inputFile);
         XMLSlideShow ppt = new XMLSlideShow(fis)) {

      List<XSLFSlide> slides = ppt.getSlides();
      
      for (int slideIdx = 0; slideIdx < slides.size(); slideIdx++) {
        XSLFSlide slide = slides.get(slideIdx);
        String slideTitle = getSlideTitle(slide);
        
        // Find all tables on this slide
        List<XSLFTable> slideTables = findTablesOnSlide(slide);
        
        for (int tableIdx = 0; tableIdx < slideTables.size(); tableIdx++) {
          XSLFTable slideTable = slideTables.get(tableIdx);
          // Try to find a title for this specific table
          String tableTitle = findTableTitle(slide, slideTable);
          
          PptxTable table = parseTable(slideTable, tableTitle, slideTitle, slideIdx + 1);
          if (table != null && !table.data.isEmpty()) {
            table.tableIndexOnSlide = tableIdx;
            table.totalTablesOnSlide = slideTables.size();
            tables.add(table);
          }
        }
      }
    }

    return tables;
  }

  /**
   * Gets the title of a slide if it exists.
   */
  private static String getSlideTitle(XSLFSlide slide) {
    // Try to get the slide title from the title placeholder
    try {
      XSLFTextShape titleShape = slide.getPlaceholder(0);
      if (titleShape != null && titleShape.getText() != null) {
        String title = titleShape.getText().trim();
        if (!title.isEmpty()) {
          return title;
        }
      }
    } catch (IndexOutOfBoundsException e) {
      // No placeholder at index 0, continue to look for text shapes
    }

    // If no title placeholder, look for the first text shape that looks like a title
    for (XSLFShape shape : slide.getShapes()) {
      if (shape instanceof XSLFTextShape) {
        XSLFTextShape textShape = (XSLFTextShape) shape;
        String text = textShape.getText();
        if (text != null && !text.trim().isEmpty()) {
          // Check if it looks like a title (relatively short, not a paragraph)
          String trimmed = text.trim();
          if (trimmed.length() < 100 && !trimmed.contains("\n\n")) {
            return trimmed;
          }
        }
      }
    }

    return null;
  }

  /**
   * Finds all tables on a slide.
   */
  private static List<XSLFTable> findTablesOnSlide(XSLFSlide slide) {
    List<XSLFTable> tables = new ArrayList<>();
    
    for (XSLFShape shape : slide.getShapes()) {
      if (shape instanceof XSLFTable) {
        tables.add((XSLFTable) shape);
      }
    }
    
    return tables;
  }

  /**
   * Finds a title for the table by looking at text shapes above it on the slide.
   */
  private static String findTableTitle(XSLFSlide slide, XSLFTable table) {
    // Get the position of the table
    double tableY = table.getAnchor().getY();
    double tableX = table.getAnchor().getX();
    double tableWidth = table.getAnchor().getWidth();
    
    String closestTitle = null;
    double closestDistance = Double.MAX_VALUE;
    
    // Look for text shapes above the table
    for (XSLFShape shape : slide.getShapes()) {
      if (shape instanceof XSLFTextShape && !(shape instanceof XSLFTable)) {
        XSLFTextShape textShape = (XSLFTextShape) shape;
        double textY = textShape.getAnchor().getY();
        double textX = textShape.getAnchor().getX();
        double textWidth = textShape.getAnchor().getWidth();
        
        // Check if the text is above the table
        if (textY < tableY) {
          // Check for horizontal alignment - text should be roughly aligned with the table
          double textCenterX = textX + textWidth / 2;
          double tableCenterX = tableX + tableWidth / 2;
          double horizontalDistance = Math.abs(textCenterX - tableCenterX);
          
          // Only consider text that is horizontally aligned with the table
          if (horizontalDistance < tableWidth) {
            String text = textShape.getText();
            if (text != null && !text.trim().isEmpty()) {
              double verticalDistance = tableY - textY;
              double totalDistance = verticalDistance + horizontalDistance / 10; // Weight vertical distance more
              
              // Find the closest text above the table
              if (totalDistance < closestDistance && verticalDistance < 100) { // Within 100 points vertically
                String trimmed = text.trim();
                // Avoid using very long text as title
                if (trimmed.length() < 200) {
                  closestTitle = trimmed;
                  closestDistance = totalDistance;
                }
              }
            }
          }
        }
      }
    }
    
    if (closestTitle != null) {
      // Clean up the title
      closestTitle = Pattern.compile("^#+\\s*").matcher(closestTitle).replaceAll("");
      closestTitle = Pattern.compile("^\\d+\\.\\s*").matcher(closestTitle).replaceAll("");
      closestTitle = Pattern.compile("^[•·-]\\s*").matcher(closestTitle).replaceAll("");
    }
    
    return closestTitle;
  }

  /**
   * Parses a PowerPoint table into our internal representation.
   */
  private static PptxTable parseTable(XSLFTable pptTable, String tableTitle, 
      String slideTitle, int slideNumber) {
    List<XSLFTableRow> rows = pptTable.getRows();
    if (rows.isEmpty()) {
      return null;
    }

    // Determine header rows by analyzing first few rows
    int headerRowCount = determineHeaderRowCount(rows);
    if (headerRowCount == 0) {
      LOGGER.warn("No header rows detected in table on slide {}", slideNumber);
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

    PptxTable table = new PptxTable();
    table.title = tableTitle;
    table.slideTitle = slideTitle;
    table.slideNumber = slideNumber;
    table.data = data;
    return table;
  }

  /**
   * Determines how many rows at the beginning are headers.
   */
  private static int determineHeaderRowCount(List<XSLFTableRow> rows) {
    if (rows.isEmpty()) {
      return 0;
    }

    // Simple heuristic: assume first row is always header
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
  private static List<String> extractRowCells(XSLFTableRow row) {
    List<String> cells = new ArrayList<>();
    for (XSLFTableCell cell : row.getCells()) {
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

      if (cell.matches("^-?\\d+(\\.\\d+)?$")) {
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
   * Generates a base file name for the JSON output (without extension and index).
   */
  private static String generateBaseFileName(String baseName, String tableTitle, 
      String slideTitle, int slideNumber) {
    StringBuilder fileName = new StringBuilder(baseName);

    // Add slide context (in lowercase)
    // Only add slide number if there's no clear slide title
    if (slideTitle != null && !slideTitle.isEmpty()) {
      fileName.append("__").append(sanitizeIdentifier(slideTitle).toLowerCase());
    } else {
      fileName.append("__slide").append(slideNumber);
    }

    // Add table title if available (in lowercase)
    if (tableTitle != null && !tableTitle.isEmpty()) {
      fileName.append("__").append(sanitizeIdentifier(tableTitle).toLowerCase());
    } else {
      fileName.append("__table");
    }

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
   * Container for a parsed PPTX table.
   */
  private static class PptxTable {
    String title;
    String slideTitle;
    int slideNumber;
    int tableIndexOnSlide;
    int totalTablesOnSlide;
    ArrayNode data;
  }
}