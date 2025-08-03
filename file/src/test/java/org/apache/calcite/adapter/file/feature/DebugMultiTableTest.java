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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class DebugMultiTableTest {

  @TempDir
  public File tempDir;

  @Test void debugMultiTableDetection() throws Exception {
    // Create Excel file with embedded tables
    File excelFile = new File(tempDir, "debug_tables.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
         FileOutputStream fos = new FileOutputStream(excelFile)) {

      Sheet sheet = workbook.createSheet("Sheet1");

      // First table at A1
      System.out.println("Creating first table (Employee):");
      Row header1 = sheet.createRow(0);
      header1.createCell(0).setCellValue("Employee ID");
      header1.createCell(1).setCellValue("Name");
      header1.createCell(2).setCellValue("Department");
      System.out.println("  Row 0: Employee ID, Name, Department");

      Row data1 = sheet.createRow(1);
      data1.createCell(0).setCellValue(1);
      data1.createCell(1).setCellValue("Alice");
      data1.createCell(2).setCellValue("Engineering");
      System.out.println("  Row 1: 1, Alice, Engineering");

      Row data2 = sheet.createRow(2);
      data2.createCell(0).setCellValue(2);
      data2.createCell(1).setCellValue("Bob");
      data2.createCell(2).setCellValue("Sales");
      System.out.println("  Row 2: 2, Bob, Sales");

      // Gap rows that might create spurious detection
      sheet.createRow(3); // Empty row
      sheet.createRow(4); // Empty row
      System.out.println("  Row 3: (empty)");
      System.out.println("  Row 4: (empty)");

      // Second table at A6
      System.out.println("Creating second table (Product):");
      Row header2 = sheet.createRow(5);
      header2.createCell(0).setCellValue("Product ID");
      header2.createCell(1).setCellValue("Product Name");
      header2.createCell(2).setCellValue("Price");
      System.out.println("  Row 5: Product ID, Product Name, Price");

      Row prod1 = sheet.createRow(6);
      prod1.createCell(0).setCellValue(101);
      prod1.createCell(1).setCellValue("Widget");
      prod1.createCell(2).setCellValue(19.99);
      System.out.println("  Row 6: 101, Widget, 19.99");

      Row prod2 = sheet.createRow(7);
      prod2.createCell(0).setCellValue(102);
      prod2.createCell(1).setCellValue("Gadget");
      prod2.createCell(2).setCellValue(29.99);
      System.out.println("  Row 7: 102, Gadget, 29.99");

      workbook.write(fos);
    }

    System.out.println("\nConverting Excel to JSON with multi-table detection...");
    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    // List all JSON files created
    File[] jsonFiles = tempDir.listFiles((dir, name) -> name.endsWith(".json"));
    System.out.println("\nJSON files created: " + jsonFiles.length);
    for (File f : jsonFiles) {
      System.out.println("  " + f.getName());
    }
  }
}