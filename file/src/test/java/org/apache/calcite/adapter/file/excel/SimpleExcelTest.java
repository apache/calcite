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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Simplest possible test for Excel table detection.
 */
@Tag("unit")
public class SimpleExcelTest {
  @TempDir
  Path tempDir;

  @Test public void testSingleSimpleTable() throws Exception {
    System.out.println("\n=== TESTING SINGLE SIMPLE TABLE ===");

    // Create the absolute simplest Excel file possible
    File excelFile = new File(tempDir.toFile(), "simple.xlsx");

    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("Sheet1");

      // Just headers and data - no identifier
      Row header = sheet.createRow(0);
      header.createCell(0).setCellValue("Name");
      header.createCell(1).setCellValue("Value");
      System.out.println("Row 0: 'Name', 'Value'");

      Row data1 = sheet.createRow(1);
      data1.createCell(0).setCellValue("Test");
      data1.createCell(1).setCellValue("123");
      System.out.println("Row 1: 'Test', '123'");

      Row data2 = sheet.createRow(2);
      data2.createCell(0).setCellValue("Demo");
      data2.createCell(1).setCellValue("456");
      System.out.println("Row 2: 'Demo', '456'");

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }

    System.out.println("\nConverting...");
    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    // List results
    System.out.println("\nFiles created:");
    File[] files = tempDir.toFile().listFiles();
    if (files != null) {
      for (File f : files) {
        System.out.println("  " + f.getName());
        if (f.getName().endsWith(".json")) {
          String content = Files.readString(f.toPath());
          System.out.println("    Content: " + content);
        }
      }
    }

    System.out.println("\nThis should have created: Simple__Sheet1.json");
  }
}
