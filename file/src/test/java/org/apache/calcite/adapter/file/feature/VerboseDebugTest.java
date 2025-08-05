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
package org.apache.calcite.adapter.file.feature;

import org.apache.calcite.adapter.file.MultiTableExcelToJsonConverter;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;

public class VerboseDebugTest {

  @TempDir
  public File tempDir;

  @Test void verboseDebug() throws Exception {
    // Create Excel file exactly like SpuriousTableComprehensiveTest
    File file = new File(tempDir, "embedded_tables.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
         FileOutputStream fos = new FileOutputStream(file)) {

      Sheet sheet = workbook.createSheet("Sheet1");

      System.out.println("Creating Excel structure:");

      // First table at A1
      Row header1 = sheet.createRow(0);
      header1.createCell(0).setCellValue("Employee ID");
      header1.createCell(1).setCellValue("Name");
      header1.createCell(2).setCellValue("Department");
      printRow("Row 0 (header)", header1);

      Row data1 = sheet.createRow(1);
      data1.createCell(0).setCellValue(1);
      data1.createCell(1).setCellValue("Alice");
      data1.createCell(2).setCellValue("Engineering");
      printRow("Row 1 (data)", data1);

      Row data2 = sheet.createRow(2);
      data2.createCell(0).setCellValue(2);
      data2.createCell(1).setCellValue("Bob");
      data2.createCell(2).setCellValue("Sales");
      printRow("Row 2 (data)", data2);

      // Gap rows
      sheet.createRow(3); // Empty row
      System.out.println("Row 3: (empty)");
      sheet.createRow(4); // Empty row
      System.out.println("Row 4: (empty)");

      // Second table at A6
      Row header2 = sheet.createRow(5);
      header2.createCell(0).setCellValue("Product ID");
      header2.createCell(1).setCellValue("Product Name");
      header2.createCell(2).setCellValue("Price");
      printRow("Row 5 (header)", header2);

      Row prod1 = sheet.createRow(6);
      prod1.createCell(0).setCellValue(101);
      prod1.createCell(1).setCellValue("Widget");
      prod1.createCell(2).setCellValue(19.99);
      printRow("Row 6 (data)", prod1);

      Row prod2 = sheet.createRow(7);
      prod2.createCell(0).setCellValue(102);
      prod2.createCell(1).setCellValue("Gadget");
      prod2.createCell(2).setCellValue(29.99);
      printRow("Row 7 (data)", prod2);

      workbook.write(fos);
    }

    System.out.println("\nConverting with multi-table detection...");

    // Enable trace logging programmatically
    System.setProperty("org.slf4j.simpleLogger.log.org.apache.calcite.adapter.file.MultiTableExcelToJsonConverter", "TRACE");

    MultiTableExcelToJsonConverter.convertFileToJson(file, true);

    // Check results
    File[] jsonFiles = tempDir.listFiles((dir, name) -> name.endsWith(".json"));
    System.out.println("\nJSON files created: " + jsonFiles.length);
    for (File f : jsonFiles) {
      System.out.println("  " + f.getName());
    }

    System.out.println("\nExpected: 2 JSON files");
    System.out.println("Actual: " + jsonFiles.length + " JSON files");
  }

  private void printRow(String label, Row row) {
    System.out.print(label + ": ");
    for (int i = 0; i <= 2; i++) {
      Cell cell = row.getCell(i);
      if (cell != null) {
        switch (cell.getCellType()) {
          case STRING:
            System.out.print("'" + cell.getStringCellValue() + "'");
            break;
          case NUMERIC:
            System.out.print(cell.getNumericCellValue());
            break;
          default:
            System.out.print("null");
        }
      } else {
        System.out.print("null");
      }
      if (i < 2) System.out.print(", ");
    }
    System.out.println();
  }
}
