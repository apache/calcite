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
 * Try to reproduce the spurious table issue based on the user's clues.
 */
public class ReproduceSpuriousTest {
  @TempDir
  Path tempDir;

  @Test public void testWithTableIdentifiers() throws Exception {
    System.out.println("\n=== TEST: Multiple tables WITH identifiers ===");

    File excelFile = new File(tempDir.toFile(), "with_identifiers.xlsx");
    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("Data");

      // Recreate something closer to the working MultiTableExcelTest

      // Table 1: Sales Report (with identifier)
      Row identifierRow = sheet.createRow(0);
      identifierRow.createCell(0).setCellValue("Sales Report");

      // Headers for table 1
      Row header1 = sheet.createRow(2);
      header1.createCell(0).setCellValue("Product");
      header1.createCell(1).setCellValue("Q1");
      header1.createCell(2).setCellValue("Q2");

      // Data for table 1
      Row data1 = sheet.createRow(3);
      data1.createCell(0).setCellValue("Widget");
      data1.createCell(1).setCellValue(100);
      data1.createCell(2).setCellValue(150);

      Row data2 = sheet.createRow(4);
      data2.createCell(0).setCellValue("Gadget");
      data2.createCell(1).setCellValue(200);
      data2.createCell(2).setCellValue(250);

      // Empty rows
      sheet.createRow(5);
      sheet.createRow(6);

      // Table 2: Employee Data
      Row identifier2 = sheet.createRow(7);
      identifier2.createCell(0).setCellValue("Employee Data");

      Row header2 = sheet.createRow(9);
      header2.createCell(0).setCellValue("Name");
      header2.createCell(1).setCellValue("Department");
      header2.createCell(2).setCellValue("Salary");

      Row emp1 = sheet.createRow(10);
      emp1.createCell(0).setCellValue("John");
      emp1.createCell(1).setCellValue("Sales");
      emp1.createCell(2).setCellValue(50000);

      Row emp2 = sheet.createRow(11);
      emp2.createCell(0).setCellValue("Jane");
      emp2.createCell(1).setCellValue("Engineering");
      emp2.createCell(2).setCellValue(75000);

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }

    System.out.println("Converting Excel with table identifiers...");
    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);
    analyzeResults();
  }

  @Test public void testWithoutIdentifiers() throws Exception {
    System.out.println("\n=== TEST: Single table WITHOUT identifier ===");

    File excelFile = new File(tempDir.toFile(), "no_identifier.xlsx");
    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("Data");

      // Just headers and data - no identifier
      Row header = sheet.createRow(0);
      header.createCell(0).setCellValue("Name");
      header.createCell(1).setCellValue("Value");

      Row data1 = sheet.createRow(1);
      data1.createCell(0).setCellValue("Test");
      data1.createCell(1).setCellValue(123);

      Row data2 = sheet.createRow(2);
      data2.createCell(0).setCellValue("Demo");
      data2.createCell(1).setCellValue(456);

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }

    System.out.println("Converting Excel without identifiers...");
    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);
    analyzeResults();
  }

  private void analyzeResults() throws Exception {
    File[] files = tempDir.toFile().listFiles((dir, name) -> name.endsWith(".json"));

    if (files != null) {
      System.out.println("JSON files: " + files.length);

      for (File jsonFile : files) {
        System.out.println("\n--- " + jsonFile.getName() + " ---");
        String content = Files.readString(jsonFile.toPath());
        System.out.println(content);

        // Look for signs of spurious table
        if (content.contains("Sales Report") || content.contains("Employee Data") ||
            content.contains("table 1") || content.contains("Product")) {
          System.out.println(">>> This file contains identifier/header text - could be spurious");
        }
      }

      // Clean up for next test
      for (File f : files) {
        f.delete();
      }
    } else {
      System.out.println("No JSON files created");
    }
  }
}
