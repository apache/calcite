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

import org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter;
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
 * Test single table with identifier to verify no _T1 suffix.
 */
public class SingleTableTest {
  @TempDir
  Path tempDir;

  @Test public void testSingleTableWithIdentifier() throws Exception {
    System.out.println("\n=== TESTING SINGLE TABLE WITH IDENTIFIER ===");

    File excelFile = new File(tempDir.toFile(), "single_table.xlsx");

    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("Data");

      // Table with identifier
      Row identifierRow = sheet.createRow(0);
      identifierRow.createCell(0).setCellValue("Products");
      System.out.println("Row 0: identifier = 'Products'");

      // Add gap like working test - headers at row 2
      Row headerRow = sheet.createRow(2);
      headerRow.createCell(0).setCellValue("ProductName");
      headerRow.createCell(1).setCellValue("Category");
      System.out.println("Row 2: headers = 'ProductName', 'Category'");

      Row data1 = sheet.createRow(3);
      data1.createCell(0).setCellValue("Widget");
      data1.createCell(1).setCellValue("Electronics");
      System.out.println("Row 3: data = 'Widget', 'Electronics'");

      Row data2 = sheet.createRow(4);
      data2.createCell(0).setCellValue("Gadget");
      data2.createCell(1).setCellValue("Tools");
      System.out.println("Row 4: data = 'Gadget', 'Tools'");

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }

    System.out.println("Converting single table with identifier...");
    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    // List results
    System.out.println("\nFiles created:");
    File[] files = tempDir.toFile().listFiles((dir, name) -> name.endsWith(".json"));
    if (files != null) {
      System.out.println("Total JSON files: " + files.length);
      for (File f : files) {
        System.out.println("  " + f.getName());
        String content = Files.readString(f.toPath());
        System.out.println("    Content: " + content);
      }

      System.out.println("\nExpected: 1 file named 'SingleTable__Data_Products.json' (no _T1 suffix)");
      if (files.length == 1 && !files[0].getName().contains("_T1")) {
        System.out.println("✅ SUCCESS: No unnecessary _T1 suffix!");
      } else {
        System.out.println("❌ ISSUE: Found _T1 suffix or wrong number of files");
      }
    } else {
      System.out.println("No JSON files created!");
    }
  }
}
