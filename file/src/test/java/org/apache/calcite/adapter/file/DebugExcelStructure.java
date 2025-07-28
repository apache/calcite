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
import java.util.Locale;

/**
 * Debug the actual Excel file structure.
 */
public class DebugExcelStructure {

  @Test public void debugExcelStructure() throws Exception {
    File file = new File("src/test/resources/testdata/lots_of_tables.xlsx");

    try (FileInputStream fis = new FileInputStream(file);
         Workbook workbook = WorkbookFactory.create(fis)) {

      Sheet sheet = workbook.getSheetAt(0);
      System.out.println("Sheet: " + sheet.getSheetName());
      System.out.println("=====================================\n");

      // Print first 30 rows to understand structure
      for (int rowNum = 0; rowNum <= Math.min(29, sheet.getLastRowNum()); rowNum++) {
        Row row = sheet.getRow(rowNum);
        System.out.printf(Locale.ROOT, "Row %2d: ", rowNum + 1);

        if (row == null) {
          System.out.println("[empty]");
          continue;
        }

        int nonEmpty = 0;
        for (int col = 0; col < 10; col++) {
          Cell cell = row.getCell(col);
          if (cell != null && cell.getCellType() != CellType.BLANK) {
            nonEmpty++;
            String value = getCellValue(cell);
            System.out.printf(Locale.ROOT, "[%s:%s] ", getColumnLetter(col), value);
          }
        }

        if (nonEmpty == 0) {
          System.out.println("[empty]");
        } else {
          System.out.println(" (cells: " + nonEmpty + ")");
        }
      }
    }
  }

  private static String getCellValue(Cell cell) {
    if (cell == null) return "";

    switch (cell.getCellType()) {
      case STRING:
        return cell.getStringCellValue().trim();
      case NUMERIC:
        double num = cell.getNumericCellValue();
        if (num == (int) num) {
          return String.valueOf((int) num);
        }
        return String.valueOf(num);
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

  private static String getColumnLetter(int col) {
    StringBuilder result = new StringBuilder();
    while (col >= 0) {
      result.insert(0, (char) ('A' + (col % 26)));
      col = col / 26 - 1;
    }
    return result.toString();
  }
}
