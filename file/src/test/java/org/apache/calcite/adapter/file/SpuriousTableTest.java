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
 * Test to reproduce and identify the spurious table containing headers.
 */
public class SpuriousTableTest {
  @TempDir
  Path tempDir;

  @Test public void reproduceSpuriousTable() throws Exception {
    System.out.println("\n=== REPRODUCING SPURIOUS TABLE ISSUE ===");

    // Create Excel file with 2 embedded tables like the user described
    File excelFile = createTwoEmbeddedTablesExcel();

    System.out.println("Converting Excel file...");
    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    // List all JSON files and their contents
    System.out.println("\nJSON files created:");
    File[] files = tempDir.toFile().listFiles((dir, name) -> name.endsWith(".json"));

    if (files != null) {
      System.out.println("Total JSON files: " + files.length);

      for (int i = 0; i < files.length; i++) {
        File jsonFile = files[i];
        System.out.println("\n--- FILE " + (i+1) + ": " + jsonFile.getName() + " ---");

        String content = Files.readString(jsonFile.toPath());
        System.out.println("Content: " + content);

        // Check if this looks like the spurious file with headers
        if (content.contains("table 1") || content.contains("id") || content.contains("Company")) {
          System.out.println(">>> POTENTIAL SPURIOUS FILE - contains header-like content");
        }
      }

      System.out.println("\nExpected: 2 files (one per table)");
      System.out.println("Actual: " + files.length + " files");
      if (files.length > 2) {
        System.out.println(">>> SPURIOUS FILES DETECTED");
      }
    } else {
      System.out.println("No JSON files created!");
    }
  }

  private File createTwoEmbeddedTablesExcel() throws Exception {
    File excelFile = new File(tempDir.toFile(), "two_tables.xlsx");

    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("Data");

      // Table 1: Company data
      Row id1Row = sheet.createRow(0);
      id1Row.createCell(0).setCellValue("table 1");

      Row header1Row = sheet.createRow(1);
      header1Row.createCell(0).setCellValue("id");
      header1Row.createCell(1).setCellValue("Company");

      Row data1Row1 = sheet.createRow(2);
      data1Row1.createCell(0).setCellValue("1");
      data1Row1.createCell(1).setCellValue("Acme Corp");

      Row data1Row2 = sheet.createRow(3);
      data1Row2.createCell(0).setCellValue("2");
      data1Row2.createCell(1).setCellValue("Widget Inc");

      // Empty separator - need at least 2 empty rows
      sheet.createRow(4);
      sheet.createRow(5);
      sheet.createRow(6);

      // Table 2: Department data
      Row id2Row = sheet.createRow(7);
      id2Row.createCell(0).setCellValue("table 2");

      Row header2Row = sheet.createRow(9);
      header2Row.createCell(0).setCellValue("Department");
      header2Row.createCell(1).setCellValue("Head");

      Row data2Row1 = sheet.createRow(10);
      data2Row1.createCell(0).setCellValue("Engineering");
      data2Row1.createCell(1).setCellValue("Alice");

      Row data2Row2 = sheet.createRow(11);
      data2Row2.createCell(0).setCellValue("Sales");
      data2Row2.createCell(1).setCellValue("Bob");

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }

    System.out.println("Created Excel with structure:");
    System.out.println("  Table 1: 'table 1' identifier, 'id'/'Company' headers, 2 data rows");
    System.out.println("  Table 2: 'table 2' identifier, 'Department'/'Head' headers, 2 data rows");

    return excelFile;
  }
}
