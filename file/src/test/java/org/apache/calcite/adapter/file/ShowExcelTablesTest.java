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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Test to show tables extracted from multi-table Excel file.
 */
public class ShowExcelTablesTest {

  @Test public void showExtractedTables() throws Exception {
    // Disable POI logging
    System.setProperty("org.apache.poi.util.POILogger", "org.apache.poi.util.NullLogger");

    String fileName = "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/complex tables/lots_of_tables.xlsx";
    File file = new File(fileName);

    if (!file.exists()) {
      System.out.println("File not found: " + fileName);
      System.out.println("Trying relative path...");
      fileName = "src/test/resources/testdata/lots_of_tables.xlsx";
      file = new File(fileName);
    }

    if (!file.exists()) {
      System.out.println("File not found at either location");
      return;
    }

    System.out.println("\n=== EXTRACTED TABLES FROM: " + file.getName() + " ===\n");

    try (FileInputStream fis = new FileInputStream(file);
         Workbook workbook = WorkbookFactory.create(fis)) {

      for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
        Sheet sheet = workbook.getSheetAt(i);
        String sheetName = sheet.getSheetName();

        System.out.println("SHEET: " + sheetName);
        System.out.println("----------------------------------------");

        List<Map<String, Integer>> tableRegions = detectTableRegions(sheet);

        if (tableRegions.isEmpty()) {
          System.out.println("  No tables detected\n");
          continue;
        }

        int tableIndex = 1;
        for (Map<String, Integer> region : tableRegions) {
          String baseFileName = file.getName().replace(".xlsx", "").replace(".xls", "");

          // Check for table identifier in the row above headers
          String tableIdentifier = null;
          int headerStartRow = region.get("startRow");

          // Look for title row (single cell above headers)
          if (headerStartRow > 0) {
            Row titleRow = sheet.getRow(headerStartRow - 1);
            if (titleRow != null && countNonEmptyCells(titleRow) == 1) {
              Cell titleCell = getFirstNonEmptyCell(titleRow);
              if (titleCell != null) {
                tableIdentifier = getCellValue(titleCell);
              }
            }
          }

          // For second table, check row 14 specifically (row 15 in Excel, 0-based index 14)
          if (tableIndex == 2 && headerStartRow != 14) {
            Row possibleTitleRow = sheet.getRow(14);
            if (possibleTitleRow != null && countNonEmptyCells(possibleTitleRow) == 1) {
              Cell titleCell = getFirstNonEmptyCell(possibleTitleRow);
              if (titleCell != null) {
                String possibleTitle = getCellValue(titleCell);
                System.out.println("  DEBUG: Found possible title at row 15: " + possibleTitle);
                if ("table2".equalsIgnoreCase(possibleTitle.trim())) {
                  tableIdentifier = possibleTitle;
                }
              }
            }
          }

          String tableName;
          if (tableIdentifier != null && !tableIdentifier.isEmpty()) {
            tableName = sanitizeTableName(baseFileName) + "__" +
                       sanitizeTableName(sheetName) + "_" +
                       sanitizeTableName(tableIdentifier) +
                       (tableRegions.size() > 1 ? "_T" + tableIndex : "");
          } else {
            tableName = sanitizeTableName(baseFileName) + "__" +
                       sanitizeTableName(sheetName) + "_Table_T" + tableIndex;
          }

          System.out.println("\n  TABLE NAME: " + tableName);
          if (tableIdentifier != null) {
            System.out.println("  TABLE IDENTIFIER: " + tableIdentifier);
          }

          // Check for group headers
          int startRow = region.get("startRow");
          int startCol = region.get("startCol");
          int endCol = region.get("endCol");

          // Look for group headers above the main header
          List<String> groupHeaders1 = new ArrayList<>();
          List<String> groupHeaders2 = new ArrayList<>();

          if (startRow >= 2) {
            Row groupRow1 = sheet.getRow(startRow - 2);
            if (groupRow1 != null && !isEmptyRow(groupRow1)) {
              for (int col = startCol; col <= endCol; col++) {
                Cell cell = groupRow1.getCell(col);
                groupHeaders1.add(cell != null ? getCellValue(cell) : "");
              }
              if (!groupHeaders1.stream().allMatch(String::isEmpty)) {
                System.out.println("  GROUP HEADER 1: " + String.join(", ", groupHeaders1));
              }
            }

            Row groupRow2 = sheet.getRow(startRow - 1);
            if (groupRow2 != null && !isEmptyRow(groupRow2) && countNonEmptyCells(groupRow2) > 1) {
              for (int col = startCol; col <= endCol; col++) {
                Cell cell = groupRow2.getCell(col);
                groupHeaders2.add(cell != null ? getCellValue(cell) : "");
              }
              if (!groupHeaders2.stream().allMatch(String::isEmpty)) {
                System.out.println("  GROUP HEADER 2: " + String.join(", ", groupHeaders2));
              }
            }
          }

          Row headerRow = sheet.getRow(startRow);
          if (headerRow != null) {
            System.out.print("  COLUMNS: ");
            List<String> columns = new ArrayList<>();
            for (int col = startCol; col <= endCol; col++) {
              Cell cell = headerRow.getCell(col);
              if (cell != null) {
                String value = getCellValue(cell);
                if (!value.isEmpty()) {
                  columns.add(value);
                }
              }
            }
            System.out.println(String.join(", ", columns));

            // Show region info
            System.out.println("  LOCATION: Rows " + (startRow + 1) + "-" + (region.get("endRow") + 1) +
                             ", Columns " + getColumnLetter(startCol) + "-" + getColumnLetter(endCol));
            System.out.println("  DATA ROWS: " + (region.get("endRow") - startRow));
          }

          tableIndex++;
        }
        System.out.println();
      }
    }
  }

  private static List<Map<String, Integer>> detectTableRegions(Sheet sheet) {
    List<Map<String, Integer>> regions = new ArrayList<>();
    boolean[][] visited = new boolean[sheet.getLastRowNum() + 1][256];

    for (int row = 0; row <= sheet.getLastRowNum(); row++) {
      Row currentRow = sheet.getRow(row);
      if (currentRow == null) continue;

      for (int col = 0; col < 256; col++) {
        if (visited[row][col]) continue;

        Cell cell = currentRow.getCell(col);
        if (cell == null || cell.getCellType() == CellType.BLANK) continue;

        // Found a non-empty cell, check if it's a potential header
        String value = getCellValue(cell);
        if (value.isEmpty()) continue;

        // Detect table region starting from this cell
        Map<String, Integer> region = detectTableFromCell(sheet, row, col, visited);
        if (region != null && isValidTableRegion(region)) {
          regions.add(region);
        }
      }
    }

    return regions;
  }

  private static Map<String, Integer> detectTableFromCell(Sheet sheet, int startRow, int startCol,
                                                         boolean[][] visited) {
    // Find the extent of the table
    int endCol = startCol;
    int endRow = startRow;

    // Find the right boundary
    Row headerRow = sheet.getRow(startRow);
    if (headerRow != null) {
      for (int col = startCol + 1; col < 256; col++) {
        Cell cell = headerRow.getCell(col);
        if (cell == null || cell.getCellType() == CellType.BLANK) {
          break;
        }
        endCol = col;
      }
    }

    // Find the bottom boundary
    for (int row = startRow + 1; row <= sheet.getLastRowNum(); row++) {
      Row currentRow = sheet.getRow(row);
      if (currentRow == null) break;

      boolean hasData = false;
      for (int col = startCol; col <= endCol; col++) {
        Cell cell = currentRow.getCell(col);
        if (cell != null && cell.getCellType() != CellType.BLANK) {
          hasData = true;
          break;
        }
      }

      if (!hasData) break;
      endRow = row;
    }

    // Mark cells as visited
    for (int row = startRow; row <= endRow; row++) {
      for (int col = startCol; col <= endCol; col++) {
        visited[row][col] = true;
      }
    }

    Map<String, Integer> region = new LinkedHashMap<>();
    region.put("startRow", startRow);
    region.put("startCol", startCol);
    region.put("endRow", endRow);
    region.put("endCol", endCol);

    return region;
  }

  private static boolean isValidTableRegion(Map<String, Integer> region) {
    // Table must have at least 2 rows (header + 1 data) and 2 columns
    int rows = region.get("endRow") - region.get("startRow") + 1;
    int cols = region.get("endCol") - region.get("startCol") + 1;
    return rows >= 2 && cols >= 2;
  }

  private static String getCellValue(Cell cell) {
    if (cell == null) return "";

    switch (cell.getCellType()) {
      case STRING:
        return cell.getStringCellValue().trim();
      case NUMERIC:
        return String.valueOf((int) cell.getNumericCellValue());
      case BOOLEAN:
        return String.valueOf(cell.getBooleanCellValue());
      case FORMULA:
        try {
          return cell.getStringCellValue().trim();
        } catch (Exception e) {
          return String.valueOf(cell.getNumericCellValue());
        }
      default:
        return "";
    }
  }

  private static final Pattern INVALID_TABLE_NAME_CHARS = Pattern.compile("[^a-zA-Z0-9_]");

  private static String sanitizeTableName(String name) {
    return INVALID_TABLE_NAME_CHARS.matcher(name).replaceAll("_");
  }

  private static String getColumnLetter(int col) {
    StringBuilder result = new StringBuilder();
    while (col >= 0) {
      result.insert(0, (char) ('A' + (col % 26)));
      col = col / 26 - 1;
    }
    return result.toString();
  }

  private static int countNonEmptyCells(Row row) {
    if (row == null) return 0;
    int count = 0;
    for (Cell cell : row) {
      if (cell != null && cell.getCellType() != CellType.BLANK) {
        count++;
      }
    }
    return count;
  }

  private static Cell getFirstNonEmptyCell(Row row) {
    if (row == null) return null;
    for (Cell cell : row) {
      if (cell != null && cell.getCellType() != CellType.BLANK) {
        return cell;
      }
    }
    return null;
  }

  private static boolean isEmptyRow(Row row) {
    if (row == null) return true;
    for (Cell cell : row) {
      if (cell != null && cell.getCellType() != CellType.BLANK) {
        String value = getCellValue(cell);
        if (!value.isEmpty()) {
          return false;
        }
      }
    }
    return true;
  }
}
