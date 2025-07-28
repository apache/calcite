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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Debug test to investigate what's wrong with multi-table Excel conversion.
 */
public class ExcelDebugTest {
  @TempDir
  Path tempDir;

  @Test public void debugMultiTableIssue() throws Exception {
    System.out.println("\n=== DEBUGGING MULTI-TABLE EXCEL ISSUE ===");

    // Create a simple Excel file with 2 clear tables
    File excelFile = createDebugExcelFile();

    System.out.println("Created Excel file: " + excelFile.getAbsolutePath());
    System.out.println("File size: " + excelFile.length() + " bytes");

    // Convert it
    System.out.println("\nConverting...");
    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    // List all JSON files created
    System.out.println("\nJSON files created:");
    File[] files = tempDir.toFile().listFiles((dir, name) -> name.endsWith(".json"));
    if (files != null) {
      for (File jsonFile : files) {
        System.out.println("\n--- " + jsonFile.getName() + " ---");
        String content = Files.readString(jsonFile.toPath());
        System.out.println(content);
      }
    } else {
      System.out.println("No JSON files created!");
    }
  }

  private File createDebugExcelFile() throws Exception {
    File excelFile = new File(tempDir.toFile(), "debug_multi_table.xlsx");

    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("TestSheet");

      // Table 1: Simple Products table
      System.out.println("Creating Table 1:");
      Row id1 = sheet.createRow(0);
      id1.createCell(0).setCellValue("Products");
      System.out.println("  Row 0: Identifier = 'Products'");

      Row header1 = sheet.createRow(2);
      header1.createCell(0).setCellValue("Name");
      header1.createCell(1).setCellValue("Price");
      System.out.println("  Row 2: Headers = 'Name', 'Price'");

      Row data1 = sheet.createRow(3);
      data1.createCell(0).setCellValue("Widget");
      data1.createCell(1).setCellValue("10.00");
      System.out.println("  Row 3: Data = 'Widget', '10.00'");

      Row data2 = sheet.createRow(4);
      data2.createCell(0).setCellValue("Gadget");
      data2.createCell(1).setCellValue("20.00");
      System.out.println("  Row 4: Data = 'Gadget', '20.00'");

      // Empty separation
      sheet.createRow(5);
      sheet.createRow(6);
      System.out.println("  Rows 5-6: Empty (separator)");

      // Table 2: Simple Employees table
      System.out.println("Creating Table 2:");
      Row id2 = sheet.createRow(7);
      id2.createCell(0).setCellValue("Employees");
      System.out.println("  Row 7: Identifier = 'Employees'");

      Row header2 = sheet.createRow(9);
      header2.createCell(0).setCellValue("Employee");
      header2.createCell(1).setCellValue("Department");
      System.out.println("  Row 9: Headers = 'Employee', 'Department'");

      Row emp1 = sheet.createRow(10);
      emp1.createCell(0).setCellValue("John");
      emp1.createCell(1).setCellValue("Sales");
      System.out.println("  Row 10: Data = 'John', 'Sales'");

      Row emp2 = sheet.createRow(11);
      emp2.createCell(0).setCellValue("Jane");
      emp2.createCell(1).setCellValue("Engineering");
      System.out.println("  Row 11: Data = 'Jane', 'Engineering'");

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }

    System.out.println("\nExpected result:");
    System.out.println("  2 JSON files should be created:");
    System.out.println("  1. DebugMultiTable__TestSheet_Products.json with Name/Price columns");
    System.out.println("  2. DebugMultiTable__TestSheet_Employees.json with Employee/Department columns");

    return excelFile;
  }
}
