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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Test with the actual Excel file that creates spurious JSON.
 */
public class RealSpuriousTest {
  @TempDir
  Path tempDir;

  @Test public void testRealExcelFile() throws Exception {
    System.out.println("\n=== TESTING REAL EXCEL FILE: company_data.xlsx ===");

    // Copy the real Excel file to temp directory
    InputStream excelStream = getClass().getResourceAsStream("/company_data.xlsx");
    if (excelStream == null) {
      System.out.println("❌ Excel file not found in resources");
      return;
    }

    File excelFile = new File(tempDir.toFile(), "company_data.xlsx");
    Files.copy(excelStream, excelFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

    System.out.println("Converting real Excel file...");
    System.out.println("File path: " + excelFile.getAbsolutePath());
    System.out.println("File size: " + excelFile.length() + " bytes");

    // Convert using multi-table detection
    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    // List all JSON files created
    System.out.println("\nJSON files created:");
    File[] files = tempDir.toFile().listFiles((dir, name) -> name.endsWith(".json"));

    if (files != null && files.length > 0) {
      System.out.println("Total JSON files: " + files.length);

      for (int i = 0; i < files.length; i++) {
        File jsonFile = files[i];
        System.out.println("\n--- FILE " + (i+1) + ": " + jsonFile.getName() + " ---");

        String content = Files.readString(jsonFile.toPath());
        System.out.println("Size: " + content.length() + " characters");
        System.out.println("Content preview: " + content.substring(0, Math.min(200, content.length())));

        // Check if this looks like a spurious file
        if (content.length() < 50 || content.equals("[ ]") || content.trim().isEmpty()) {
          System.out.println(">>> SPURIOUS FILE - Empty or near-empty content!");
        }

        // Check for header-like content that shouldn't be a separate table
        if (content.contains("table") || content.contains("header") || content.contains("column")) {
          System.out.println(">>> POTENTIAL SPURIOUS FILE - Contains metadata-like content");
        }
      }
    } else {
      System.out.println("No JSON files created!");
    }
  }
}
