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

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Simple program to show tables extracted from multi-table Excel file.
 */
public class ShowExcelTables {
  public static void main(String[] args) throws Exception {
    // Disable POI logging to avoid conflicts
    System.setProperty("org.apache.poi.util.POILogger", "org.apache.poi.util.NullLogger");

    String fileName = "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/complex tables/lots_of_tables.xlsx";
    File file = new File(fileName);

    if (!file.exists()) {
      System.out.println("File not found: " + fileName);
      return;
    }

    System.out.println("Extracting tables from: " + file.getName());
    System.out.println("=========================================");

    try (FileInputStream fis = new FileInputStream(file);
         Workbook workbook = WorkbookFactory.create(fis)) {

      for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
        Sheet sheet = workbook.getSheetAt(i);
        String sheetName = sheet.getSheetName();

        System.out.println("\nSheet: " + sheetName);
        System.out.println("-----------------");

        List<Map<String, Integer>> tableRegions = detectTableRegions(sheet);

        if (tableRegions.isEmpty()) {
          System.out.println("  No tables detected");
          continue;
        }

        int tableIndex = 1;
        for (Map<String, Integer> region : tableRegions) {
          String tableName = "LotsOfTables__" + sanitizeTableName(sheetName) + "_Table_T" + tableIndex;
          System.out.println("\n  Table: " + tableName);

          // Extract column names
          int startRow = region.get("startRow");
          int startCol = region.get("startCol");
          int endCol = region.get("endCol");

          Row headerRow = sheet.getRow(startRow);
          if (headerRow != null) {
            System.out.print("  Columns: ");
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
            System.out.println("  Region: Row " + (startRow + 1) + "-" + (region.get("endRow") + 1) +
                             ", Col " + getColumnLetter(startCol) + "-" + getColumnLetter(endCol));
            System.out.println("  Data rows: " + (region.get("endRow") - startRow));
          }

          tableIndex++;
        }
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
}
