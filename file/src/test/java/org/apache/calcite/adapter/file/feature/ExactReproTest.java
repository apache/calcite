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

import org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter;

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

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
public class ExactReproTest {

  @TempDir
  public File tempDir;

  @Test void testExactRepro() throws Exception {
    // Create Excel file exactly like SpuriousTableComprehensiveTest
    File file = new File(tempDir, "embedded_tables.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
         FileOutputStream fos = new FileOutputStream(file)) {

      Sheet sheet = workbook.createSheet("Sheet1");

      // First table at A1
      Row header1 = sheet.createRow(0);
      header1.createCell(0).setCellValue("Employee ID");
      header1.createCell(1).setCellValue("Name");
      header1.createCell(2).setCellValue("department");

      Row data1 = sheet.createRow(1);
      data1.createCell(0).setCellValue(1);
      data1.createCell(1).setCellValue("Alice");
      data1.createCell(2).setCellValue("Engineering");

      Row data2 = sheet.createRow(2);
      data2.createCell(0).setCellValue(2);
      data2.createCell(1).setCellValue("Bob");
      data2.createCell(2).setCellValue("Sales");

      // Gap rows that might create spurious detection
      sheet.createRow(3); // Empty row
      sheet.createRow(4); // Empty row

      // Second table at A6
      Row header2 = sheet.createRow(5);
      header2.createCell(0).setCellValue("Product ID");
      header2.createCell(1).setCellValue("Product Name");
      header2.createCell(2).setCellValue("price");

      Row prod1 = sheet.createRow(6);
      prod1.createCell(0).setCellValue(101);
      prod1.createCell(1).setCellValue("Widget");
      prod1.createCell(2).setCellValue(19.99);

      Row prod2 = sheet.createRow(7);
      prod2.createCell(0).setCellValue(102);
      prod2.createCell(1).setCellValue("Gadget");
      prod2.createCell(2).setCellValue(29.99);

      workbook.write(fos);
    }

    System.out.println("Created Excel file: " + file.getAbsolutePath());
    MultiTableExcelToJsonConverter.convertFileToJson(file, tempDir, true, "SMART_CASING", "SMART_CASING", tempDir);

    // List all JSON files created
    File[] jsonFiles = tempDir.listFiles((dir, name) -> name.endsWith(".json"));
    System.out.println("JSON files created: " + jsonFiles.length);
    for (File f : jsonFiles) {
      System.out.println("  " + f.getName());
      String content = Files.readString(f.toPath());
      System.out.println("  Size: " + content.length() + " bytes");
      System.out.println("  First 200 chars: " + content.substring(0, Math.min(200, content.length())));
    }

    assertTrue(jsonFiles.length >= 2, "Should create at least 2 JSON files, but created " + jsonFiles.length);
  }
}
