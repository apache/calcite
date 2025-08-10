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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;

public class DetailedDebugTest {

  @TempDir
  public File tempDir;

  @Test void testDetailedDebug() throws Exception {
    // Create Excel file
    File excelFile = new File(tempDir, "test.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
         FileOutputStream fos = new FileOutputStream(excelFile)) {

      Sheet sheet = workbook.createSheet("Sheet1");

      // First table
      Row h1 = sheet.createRow(0);
      h1.createCell(0).setCellValue("id");
      h1.createCell(1).setCellValue("Name");

      Row d1 = sheet.createRow(1);
      d1.createCell(0).setCellValue(1);
      d1.createCell(1).setCellValue("Alice");

      // Empty rows
      sheet.createRow(2); // Empty
      sheet.createRow(3); // Empty

      // Second table
      Row h2 = sheet.createRow(4);
      h2.createCell(0).setCellValue("product");
      h2.createCell(1).setCellValue("price");

      Row d2 = sheet.createRow(5);
      d2.createCell(0).setCellValue("Widget");
      d2.createCell(1).setCellValue(10.0);

      workbook.write(fos);
    }

    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    // Check results
    File[] jsonFiles = tempDir.listFiles((dir, name) -> name.endsWith(".json"));
    System.out.println("\nTotal JSON files: " + jsonFiles.length);
    for (File f : jsonFiles) {
      System.out.println("\nFile: " + f.getName());
      String content = Files.readString(f.toPath());
      System.out.println("Content: " + content);
    }
  }
}
