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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test showing what tables are extracted from multi-table Excel files.
 */
@Tag("unit")
public class MultiTableExtractorTest {
  @TempDir
  Path tempDir;

  @Test public void testExtractTablesFromComplexExcel() throws Exception {
    // Copy the lots_of_tables.xlsx to temp directory
    File targetFile = new File(tempDir.toFile(), "lots_of_tables.xlsx");
    try (InputStream in = getClass().getResourceAsStream("/lots_of_tables.xlsx")) {
      if (in == null) {
        System.out.println("Test file /lots_of_tables.xlsx not found, skipping test");
        return;
      }
      Files.copy(in, targetFile.toPath());
    }

    // Convert with multi-table detection
    System.out.println("Converting " + targetFile.getName() + " with multi-table detection...");
    MultiTableExcelToJsonConverter.convertFileToJson(targetFile, true);

    // List all JSON files created
    File[] jsonFiles = tempDir.toFile().listFiles((dir, name) -> name.endsWith(".json"));

    System.out.println("\nTables extracted from lots_of_tables.xlsx:");
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        System.out.println("  - " + jsonFile.getName());

        // Show first few lines of content
        String content = Files.readString(jsonFile.toPath());
        String[] lines = content.split("\n");
        System.out.println("    Content preview:");
        for (int i = 0; i < Math.min(3, lines.length); i++) {
          System.out.println("      " + lines[i]);
        }
      }

      System.out.println("\nTotal tables found: " + jsonFiles.length);
      assertTrue(jsonFiles.length > 0, "Should extract at least one table");
    } else {
      System.out.println("No JSON files created");
    }
  }
}
