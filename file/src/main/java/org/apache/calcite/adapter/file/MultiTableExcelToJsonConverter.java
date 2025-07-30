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

import org.apache.calcite.util.trace.CalciteLogger;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

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

/**
 * Enhanced Excel to JSON converter that supports detecting multiple tables within a sheet.
 * This converter can identify:
 * - Optional table title row (single cell value above headers)
 * - Up to 2 group header rows followed by 1 detail header row
 * - Multiple tables separated by empty rows
 * - Embedded tables at arbitrary positions
 *
 * Table structure:
 * - [Optional] Title row: single cell containing table name/identifier
 * - [Optional] Group header row 1: first level of column grouping
 * - [Optional] Group header row 2: second level of column grouping
 * - [Required] Header row: actual column headers
 * - [Required] Data rows: the table data
 */
public final class MultiTableExcelToJsonConverter {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(MultiTableExcelToJsonConverter.class));
  private static final int MIN_EMPTY_ROWS_BETWEEN_TABLES = 2;
  private static final int MAX_HEADER_ROWS = 3;

  private MultiTableExcelToJsonConverter() {
    // Prevent instantiation
  }

  /**
   * Converts an Excel file to JSON with multi-table detection.
   */
  public static void convertFileToJson(File inputFile, boolean detectMultipleTables)
      throws IOException {
    if (!detectMultipleTables) {
      // Fall back to standard conversion
      ExcelToJsonConverter.convertFileToJson(inputFile);
      return;
    }

    LOGGER.debug("Converting file with multi-table detection: " + inputFile.getName());

    // Acquire read lock on source file
    SourceFileLockManager.LockHandle lockHandle = null;
    try {
      lockHandle = SourceFileLockManager.acquireReadLock(inputFile);
      LOGGER.debug("Acquired read lock on Excel file: " + inputFile.getPath());
    } catch (IOException e) {
      LOGGER.warn("Could not acquire lock on file: "
          + inputFile.getPath()
          + " - proceeding without lock");
      // Continue without lock
    }

    try (FileInputStream file = new FileInputStream(inputFile)) {
      Workbook workbook = WorkbookFactory.create(file);
      ObjectMapper mapper = new ObjectMapper();
      FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
      String fileName = inputFile.getName();
      String baseName = toPascalCase(fileName.substring(0, fileName.lastIndexOf('.')));

      for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
        Sheet sheet = workbook.getSheetAt(i);
        List<TableRegion> tables = detectTables(sheet, evaluator);

        // Track filenames to detect conflicts
        Map<String, Integer> filenameCount = new HashMap<>();
        List<String> plannedFilenames = new ArrayList<>();

        // Filter out empty tables first
        List<TableRegion> validTables = new ArrayList<>();
        for (TableRegion table : tables) {
          ArrayNode tableData = convertTableToJson(sheet, table, evaluator, mapper);
          LOGGER.trace("Table has " + tableData.size() + " rows of data");
          if (tableData.size() > 0) {
            table.jsonData = tableData; // Store for reuse
            validTables.add(table);
          }
        }

        // Skip processing if no valid tables found
        if (validTables.isEmpty()) {
          LOGGER.debug("No valid tables found in sheet: " + sheet.getSheetName());
          continue;
        }

        LOGGER.debug("Found " + validTables.size()
            + " valid tables in sheet: " + sheet.getSheetName());

        // First pass: collect all planned filenames from valid tables only
        for (TableRegion table : validTables) {
          String sheetName = toPascalCase(sheet.getSheetName());
          String baseFilename = baseName + "__" + sheetName;

          // For single table with no identifier, just use sheet name
          // For multiple tables or tables with identifiers, add identifier
          if (validTables.size() > 1
              || (table.identifier != null && !table.identifier.trim().isEmpty())) {
            if (table.identifier != null && !table.identifier.trim().isEmpty()) {
              baseFilename += "_" + sanitizeIdentifier(table.identifier);
            }
          }

          plannedFilenames.add(baseFilename);
          filenameCount.put(baseFilename, filenameCount.getOrDefault(baseFilename, 0) + 1);
        }

        // Second pass: generate files with conflict resolution
        int tableIndex = 0;
        int conflictIndex = 1;
        for (TableRegion table : validTables) {
          String plannedName = plannedFilenames.get(tableIndex);
          String jsonFileName;

          // Add suffix only if there's a naming conflict
          if (filenameCount.get(plannedName) > 1) {
            jsonFileName = plannedName + "_T" + conflictIndex + ".json";
            conflictIndex++;
          } else {
            jsonFileName = plannedName + ".json";
          }

          LOGGER.debug("Writing JSON file: "
              + jsonFileName);
          try (FileWriter fileWriter =
              new FileWriter(new File(inputFile.getParent(), jsonFileName),
                  StandardCharsets.UTF_8)) {
            mapper.writerWithDefaultPrettyPrinter()
                .writeValue(fileWriter, table.jsonData);
          }
          tableIndex++;
        }
      }
      workbook.close();
    } finally {
      // Release the lock
      if (lockHandle != null) {
        lockHandle.close();
        LOGGER.debug("Released read lock on Excel file");
      }
    }
  }

  /**
   * Detects multiple tables within a sheet.
   */
  private static List<TableRegion> detectTables(Sheet sheet, FormulaEvaluator evaluator) {
    List<TableRegion> tables = new ArrayList<>();
    int lastRowNum = sheet.getLastRowNum();
    int currentRow = 0;
    LOGGER.debug("Detecting tables in sheet: " + sheet.getSheetName()
        + " (rows: 0-" + lastRowNum + ")");

    while (currentRow <= lastRowNum) {
      // Skip empty rows
      while (currentRow <= lastRowNum && isEmptyRow(sheet.getRow(currentRow))) {
        currentRow++;
      }

      if (currentRow > lastRowNum) {
        break;
      }

      // Try to detect a table starting at currentRow
      TableRegion table = detectTableAt(sheet, currentRow, evaluator);
      if (table != null) {
        LOGGER.trace("Found table at row " + currentRow + ", ending at row " + table.endRow);
        tables.add(table);
        currentRow = table.endRow + 1;
      } else {
        currentRow++;
      }
    }

    return tables;
  }

  /**
   * Detects a single table starting at the given row.
   */
  private static TableRegion detectTableAt(Sheet sheet, int startRow,
      FormulaEvaluator evaluator) {
    TableRegion table = new TableRegion();
    table.startRow = startRow;

    // Check for optional table title row (single cell value)
    String potentialIdentifier = null;
    int headerStartRow = startRow;

    Row firstRow = sheet.getRow(startRow);
    if (firstRow != null && countNonEmptyCells(firstRow) == 1) {
      // Found table title row
      Cell identifierCell = getFirstNonEmptyCell(firstRow);
      if (identifierCell != null) {
        potentialIdentifier = getCellValue(identifierCell, evaluator);
        headerStartRow = startRow + 1;
      }
    }

    // Find headers (up to 2 group header rows + 1 detail header row)
    List<Row> headerRows = new ArrayList<>();
    int row = headerStartRow;
    int emptyRowCount = 0;

    while (row <= sheet.getLastRowNum() && headerRows.size() < MAX_HEADER_ROWS) {
      Row currentRow = sheet.getRow(row);
      if (isEmptyRow(currentRow)) {
        emptyRowCount++;
        if (emptyRowCount >= MIN_EMPTY_ROWS_BETWEEN_TABLES) {
          break;
        }
      } else {
        emptyRowCount = 0;
        // For multi-header situations, be more lenient
        boolean isLikelyHeader = headerRows.isEmpty()
            ? looksLikeHeader(currentRow)
            : looksLikeHeaderInContext(currentRow, headerRows);

        if (isLikelyHeader) {
          headerRows.add(currentRow);
        } else if (!headerRows.isEmpty()) {
          // Found data row after headers
          break;
        }
      }
      row++;
    }

    if (headerRows.isEmpty()) {
      return null;
    }

    table.identifier = potentialIdentifier;
    table.headerRows = headerRows;
    table.dataStartRow = row;
    LOGGER.trace("detectTableAt: headerRows=" + headerRows.size()
        + ", dataStartRow=" + table.dataStartRow);

    // Find end of table (consecutive empty rows or end of sheet)
    int dataRow = table.dataStartRow;
    int consecutiveEmptyRows = 0;
    table.endRow = dataRow;

    while (dataRow <= sheet.getLastRowNum()) {
      Row currentRow = sheet.getRow(dataRow);
      if (isEmptyRow(currentRow)) {
        consecutiveEmptyRows++;
        if (consecutiveEmptyRows >= MIN_EMPTY_ROWS_BETWEEN_TABLES) {
          break;
        }
      } else {
        consecutiveEmptyRows = 0;
        table.endRow = dataRow;
      }
      dataRow++;
    }

    // Determine column range
    table.startCol = Integer.MAX_VALUE;
    table.endCol = 0;
    for (Row headerRow : headerRows) {
      if (headerRow != null) {
        table.startCol = Math.min(table.startCol, headerRow.getFirstCellNum());
        table.endCol = Math.max(table.endCol, headerRow.getLastCellNum() - 1);
      }
    }

    return table;
  }

  /**
   * Converts a table region to JSON.
   */
  private static ArrayNode convertTableToJson(Sheet sheet, TableRegion table,
      FormulaEvaluator evaluator, ObjectMapper mapper) {
    ArrayNode tableData = mapper.createArrayNode();
    LOGGER.trace("Converting table: dataStartRow=" + table.dataStartRow
        + ", endRow=" + table.endRow);

    // Build column headers from all header rows
    Map<Integer, String> columnHeaders = buildColumnHeaders(table, evaluator);

    // Process data rows
    for (int rowNum = table.dataStartRow; rowNum <= table.endRow; rowNum++) {
      Row row = sheet.getRow(rowNum);
      if (row != null && !isEmptyRow(row)) {
        ObjectNode rowData = mapper.createObjectNode();
        boolean hasData = false;

        for (int colNum = table.startCol; colNum <= table.endCol; colNum++) {
          Cell cell = row.getCell(colNum);
          String header = columnHeaders.get(colNum);
          if (header != null && cell != null && cell.getCellType() != CellType.BLANK) {
            String value = getCellValue(cell, evaluator);
            if (!value.isEmpty()) {
              rowData.put(header, value);
              hasData = true;
            }
          }
        }

        if (hasData) {
          tableData.add(rowData);
        }
      }
    }

    return tableData;
  }

  /**
   * Builds column headers from potentially multiple header rows.
   */
  private static Map<Integer, String> buildColumnHeaders(TableRegion table,
      FormulaEvaluator evaluator) {
    Map<Integer, String> headers = new HashMap<>();

    if (table.headerRows.size() == 1) {
      // Simple case: single header row
      Row headerRow = table.headerRows.get(0);
      for (int col = table.startCol; col <= table.endCol; col++) {
        Cell cell = headerRow.getCell(col);
        if (cell != null && cell.getCellType() != CellType.BLANK) {
          headers.put(col, getCellValue(cell, evaluator));
        }
      }
    } else {
      // Multiple header rows: need to handle sparse group headers
      // First, get the detail headers from the last row
      Row detailHeaderRow = table.headerRows.get(table.headerRows.size() - 1);
      Map<Integer, String> detailHeaders = new HashMap<>();
      for (int col = table.startCol; col <= table.endCol; col++) {
        Cell cell = detailHeaderRow.getCell(col);
        if (cell != null && cell.getCellType() != CellType.BLANK) {
          detailHeaders.put(col, getCellValue(cell, evaluator));
        }
      }

      // Then, process group headers (all rows except the last)
      Map<Integer, String> groupHeaders = new HashMap<>();
      for (int i = 0; i < table.headerRows.size() - 1; i++) {
        Row groupRow = table.headerRows.get(i);
        String lastGroupValue = null;
        int lastGroupStart = -1;

        for (int col = table.startCol; col <= table.endCol + 1; col++) {
          Cell cell = (col <= table.endCol) ? groupRow.getCell(col) : null;
          String value = (cell != null && cell.getCellType() != CellType.BLANK)
              ? getCellValue(cell, evaluator) : null;

          if (value != null && !value.isEmpty()) {
            // Found a new group header
            if (lastGroupValue != null) {
              // Apply previous group to columns
              for (int c = lastGroupStart; c < col; c++) {
                if (groupHeaders.containsKey(c)) {
                  groupHeaders.put(c, groupHeaders.get(c) + "_" + lastGroupValue);
                } else {
                  groupHeaders.put(c, lastGroupValue);
                }
              }
            }
            lastGroupValue = value;
            lastGroupStart = col;
          }

          // End of row or end of columns
          if (col > table.endCol && lastGroupValue != null) {
            // Apply last group to remaining columns
            for (int c = lastGroupStart; c <= table.endCol; c++) {
              if (groupHeaders.containsKey(c)) {
                groupHeaders.put(c, groupHeaders.get(c) + "_" + lastGroupValue);
              } else {
                groupHeaders.put(c, lastGroupValue);
              }
            }
          }
        }
      }

      // Combine group headers with detail headers
      for (int col = table.startCol; col <= table.endCol; col++) {
        String header = "";
        if (groupHeaders.containsKey(col)) {
          header = groupHeaders.get(col);
        }
        if (detailHeaders.containsKey(col)) {
          if (!header.isEmpty()) {
            header += "_";
          }
          header += detailHeaders.get(col);
        }
        if (!header.isEmpty()) {
          headers.put(col, header);
        }
      }
    }

    return headers;
  }

  /**
   * Checks if a row looks like a header row.
   */
  private static boolean looksLikeHeader(Row row) {
    if (row == null) {
      return false;
    }

    int nonEmptyCells = countNonEmptyCells(row);
    if (nonEmptyCells < 2) {
      return false;
    }

    // For small rows (2-3 cells), check if all cells are text
    boolean allText = true;

    // Headers typically have more text cells than numeric cells
    int textCells = 0;
    int numericCells = 0;
    boolean hasLongNumbers = false;

    for (Cell cell : row) {
      if (cell != null && cell.getCellType() != CellType.BLANK) {
        if (cell.getCellType() == CellType.STRING) {
          // allText already true, no need to update
          String value = cell.getStringCellValue().trim();
          // Check if it's a typical header value (short, no special chars)
          if (value.length() > 50 || value.contains("\n")) {
            // Long text or multiline - probably not a header
            return false;
          }
          // Check for patterns that indicate data, not headers
          // Names with spaces (like "John Doe") are typically data
          if (value.matches(".*\\s+.*") && value.matches("^[A-Z][a-z]+\\s+[A-Z][a-z]+.*")) {
            return false;
          }
          // Single digit strings are often IDs, not headers
          if (value.matches("^\\d{1,2}$")) {
            return false;
          }
          textCells++;
        } else if (cell.getCellType() == CellType.NUMERIC) {
          allText = false;
          double value = cell.getNumericCellValue();
          // Large numbers (like salaries) are unlikely to be headers
          if (value > 1000) {
            hasLongNumbers = true;
          }
          numericCells++;
        }
      }
    }

    // If we have large numbers, it's probably data, not headers
    if (hasLongNumbers && numericCells > 0) {
      return false;
    }

    // For small tables (2-3 columns), header rows should be all text
    if (nonEmptyCells <= 3 && allText) {
      return true;
    }

    // For larger tables, headers should be mostly text
    if (numericCells > 0) {
      return textCells > numericCells * 2; // Text cells should be at least twice the numeric cells
    }

    return textCells > 0; // Pure text row is likely a header
  }

  /**
   * Checks if a row looks like a header when we already have header rows.
   * This is more lenient because detail headers often follow group headers.
   */
  private static boolean looksLikeHeaderInContext(Row row, List<Row> existingHeaders) {
    if (row == null) {
      return false;
    }

    int nonEmptyCells = countNonEmptyCells(row);
    if (nonEmptyCells < 2) {
      return false;
    }

    // If we already have sparse headers (group headers), the next row
    // with more cells is likely the detail header
    Row lastHeader = existingHeaders.get(existingHeaders.size() - 1);
    int lastHeaderCells = countNonEmptyCells(lastHeader);

    // Detail headers typically have more cells than group headers
    if (nonEmptyCells > lastHeaderCells) {
      return true;
    }

    // Otherwise apply the standard header check
    return looksLikeHeader(row);
  }

  private static boolean isEmptyRow(Row row) {
    if (row == null) {
      return true;
    }
    for (Cell cell : row) {
      if (cell != null && cell.getCellType() != CellType.BLANK) {
        return false;
      }
    }
    return true;
  }

  private static int countNonEmptyCells(Row row) {
    if (row == null) {
      return 0;
    }
    int count = 0;
    for (Cell cell : row) {
      if (cell != null && cell.getCellType() != CellType.BLANK) {
        count++;
      }
    }
    return count;
  }

  private static Cell getFirstNonEmptyCell(Row row) {
    if (row == null) {
      return null;
    }
    for (Cell cell : row) {
      if (cell != null && cell.getCellType() != CellType.BLANK) {
        return cell;
      }
    }
    return null;
  }

  private static String getCellValue(Cell cell, FormulaEvaluator evaluator) {
    if (cell == null) {
      return "";
    }
    switch (cell.getCellType()) {
    case STRING:
      return cell.getStringCellValue();
    case NUMERIC:
      if (DateUtil.isCellDateFormatted(cell)) {
        return cell.getDateCellValue().toString();
      } else {
        return String.valueOf(cell.getNumericCellValue());
      }
    case BOOLEAN:
      return String.valueOf(cell.getBooleanCellValue());
    case FORMULA:
      return getCellValue(evaluator.evaluateInCell(cell), evaluator);
    default:
      return "";
    }
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
    if (str.startsWith("_")) {
      str = str.substring(1);
    }
    if (str.endsWith("_")) {
      str = str.substring(0, str.length() - 1);
    }

    return str;
  }

  /**
   * Represents a detected table region within a sheet.
   */
  private static class TableRegion {
    String identifier;
    List<Row> headerRows = new ArrayList<>();
    int startRow;
    int endRow;
    int dataStartRow;
    int startCol;
    int endCol;
    ArrayNode jsonData; // Cache converted JSON data to avoid double conversion
  }
}
