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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify Excel table naming behavior - no _T1 suffix unless there are conflicts.
 */
public class ExcelNamingTest {
  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    createTestExcelFiles();
  }

  private void createTestExcelFiles() throws Exception {
    // Test 1: Single table with identifier - should NOT get _T1 suffix
    createSingleTableWithIdentifier();

    // Test 2: Single table without identifier - should NOT get _T1 suffix
    createSingleTableWithoutIdentifier();

    // Test 3: Multiple tables - should get _T1, _T2 suffixes
    createMultipleTablesWithConflicts();
  }

  private void createSingleTableWithIdentifier() throws Exception {
    File excelFile = new File(tempDir.toFile(), "single_with_id.xlsx");

    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("Sales");

      // Table identifier
      Row identifierRow = sheet.createRow(0);
      identifierRow.createCell(0).setCellValue("Q1 Results");

      // Headers
      Row headerRow = sheet.createRow(2);
      headerRow.createCell(0).setCellValue("Product");
      headerRow.createCell(1).setCellValue("Revenue");

      // Data
      Row dataRow1 = sheet.createRow(3);
      dataRow1.createCell(0).setCellValue("Widget");
      dataRow1.createCell(1).setCellValue(1000);

      Row dataRow2 = sheet.createRow(4);
      dataRow2.createCell(0).setCellValue("Gadget");
      dataRow2.createCell(1).setCellValue(2000);

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }
  }

  private void createSingleTableWithoutIdentifier() throws Exception {
    File excelFile = new File(tempDir.toFile(), "single_no_id.xlsx");

    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("Data");

      // Headers (no identifier)
      Row headerRow = sheet.createRow(0);
      headerRow.createCell(0).setCellValue("Name");
      headerRow.createCell(1).setCellValue("Value");

      // Data
      Row dataRow1 = sheet.createRow(1);
      dataRow1.createCell(0).setCellValue("Test");
      dataRow1.createCell(1).setCellValue(500);

      Row dataRow2 = sheet.createRow(2);
      dataRow2.createCell(0).setCellValue("Demo");
      dataRow2.createCell(1).setCellValue(600);

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }
  }

  private void createMultipleTablesWithConflicts() throws Exception {
    File excelFile = new File(tempDir.toFile(), "multiple_tables.xlsx");

    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("Report");

      // Table 1
      Row id1Row = sheet.createRow(0);
      id1Row.createCell(0).setCellValue("Sales Data");
      Row header1Row = sheet.createRow(2);
      header1Row.createCell(0).setCellValue("Product");
      header1Row.createCell(1).setCellValue("Q1");
      Row data1Row = sheet.createRow(3);
      data1Row.createCell(0).setCellValue("Widget");
      data1Row.createCell(1).setCellValue(100);
      Row data1Row2 = sheet.createRow(4);
      data1Row2.createCell(0).setCellValue("Gadget");
      data1Row2.createCell(1).setCellValue(150);

      // Empty rows
      sheet.createRow(5);
      sheet.createRow(6);

      // Table 2 (same base name - should cause conflict)
      Row id2Row = sheet.createRow(7);
      id2Row.createCell(0).setCellValue("Sales Data");
      Row header2Row = sheet.createRow(9);
      header2Row.createCell(0).setCellValue("Product");
      header2Row.createCell(1).setCellValue("Q2");
      Row data2Row = sheet.createRow(10);
      data2Row.createCell(0).setCellValue("Widget");
      data2Row.createCell(1).setCellValue(200);
      Row data2Row2 = sheet.createRow(11);
      data2Row2.createCell(0).setCellValue("Gadget");
      data2Row2.createCell(1).setCellValue(250);

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }
  }

  @Test public void testSingleTableWithIdentifierNoSuffix() throws Exception {
    // Convert Excel file
    File inputFile = new File(tempDir.toFile(), "single_with_id.xlsx");

    System.out.println("Converting file: " + inputFile.getAbsolutePath());
    System.out.println("File exists: " + inputFile.exists());
    System.out.println("File size: " + inputFile.length() + " bytes");

    try {
      MultiTableExcelToJsonConverter.convertFileToJson(inputFile, true);
      System.out.println("Conversion completed successfully");
    } catch (Exception e) {
      System.out.println("Conversion failed: " + e.getMessage());
      e.printStackTrace();
      throw e;
    }

    // Debug: List all files created
    System.out.println("Files in temp directory after conversion:");
    File[] files = tempDir.toFile().listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.getName().endsWith(".json")) {
          System.out.println("  JSON: " + f.getName());
        } else {
          System.out.println("  OTHER: " + f.getName());
        }
      }
    }

    // Small delay to ensure file system operations complete
    Thread.sleep(100);

    // Should create: SingleWithId__Sales_Q1_Results.json (NO _T1 suffix)
    File expectedFile = new File(tempDir.toFile(), "SingleWithId__Sales_Q1_Results.json");
    File unexpectedFile = new File(tempDir.toFile(), "SingleWithId__Sales_Q1_Results_T1.json");

    System.out.println("Testing single table with identifier:");
    System.out.println("  Expected file: " + expectedFile.getName() + " exists=" + expectedFile.exists());
    System.out.println("  Unexpected file: " + unexpectedFile.getName() + " exists=" + unexpectedFile.exists());

    assertTrue(expectedFile.exists(), "Should create file without _T1 suffix");
    assertFalse(unexpectedFile.exists(), "Should NOT create file with _T1 suffix");
  }

  @Test public void testSingleTableWithoutIdentifierNoSuffix() throws Exception {
    // Convert Excel file
    File inputFile = new File(tempDir.toFile(), "single_no_id.xlsx");

    System.out.println("Converting file: " + inputFile.getAbsolutePath());
    System.out.println("File exists: " + inputFile.exists());
    System.out.println("File size: " + inputFile.length() + " bytes");

    MultiTableExcelToJsonConverter.convertFileToJson(inputFile, true);

    // List all files in directory after conversion
    System.out.println("Files in temp directory after conversion:");
    File[] files = tempDir.toFile().listFiles();
    if (files != null) {
      for (File f : files) {
        System.out.println("  " + f.getName());
      }
    }

    // Should create: SingleNoId__Data.json (NO _T1 suffix)
    File expectedFile = new File(tempDir.toFile(), "SingleNoId__Data.json");
    File unexpectedFile = new File(tempDir.toFile(), "SingleNoId__Data_T1.json");

    System.out.println("Testing single table without identifier:");
    System.out.println("  Expected file: " + expectedFile.getName() + " exists=" + expectedFile.exists());
    System.out.println("  Unexpected file: " + unexpectedFile.getName() + " exists=" + unexpectedFile.exists());

    assertTrue(expectedFile.exists(), "Should create file without _T1 suffix");
    assertFalse(unexpectedFile.exists(), "Should NOT create file with _T1 suffix");
  }

  @Test public void testMultipleTablesWithSuffix() throws Exception {
    // Convert Excel file
    File inputFile = new File(tempDir.toFile(), "multiple_tables.xlsx");

    System.out.println("Converting file: " + inputFile.getAbsolutePath());
    System.out.println("File exists: " + inputFile.exists());
    System.out.println("File size: " + inputFile.length() + " bytes");

    MultiTableExcelToJsonConverter.convertFileToJson(inputFile, true);

    // List all files in directory after conversion
    System.out.println("Files in temp directory after conversion:");
    File[] files = tempDir.toFile().listFiles();
    if (files != null) {
      for (File f : files) {
        System.out.println("  " + f.getName());
      }
    }

    // Should create: MultipleTables__Report_Sales_Data_T1.json and _T2.json (WITH suffixes)
    File file1 = new File(tempDir.toFile(), "MultipleTables__Report_Sales_Data_T1.json");
    File file2 = new File(tempDir.toFile(), "MultipleTables__Report_Sales_Data_T2.json");
    File unexpectedFile = new File(tempDir.toFile(), "MultipleTables__Report_Sales_Data.json");

    System.out.println("Testing multiple tables with conflicts:");
    System.out.println("  File 1: " + file1.getName() + " exists=" + file1.exists());
    System.out.println("  File 2: " + file2.getName() + " exists=" + file2.exists());
    System.out.println("  Unexpected file: " + unexpectedFile.getName() + " exists=" + unexpectedFile.exists());

    assertTrue(file1.exists(), "Should create first file with _T1 suffix");
    assertTrue(file2.exists(), "Should create second file with _T2 suffix");
    assertFalse(unexpectedFile.exists(), "Should NOT create file without suffix when there are conflicts");
  }

  // Removed testAllNamingBehaviors as it improperly calls other test methods
  // Each test method should run in isolation with its own @TempDir
}
