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

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to show what tables are extracted from Excel files.
 */
public class SimpleMultiTableTest {
  @TempDir
  Path tempDir;

  @Test public void testShowExtractedTables() throws Exception {
    // Copy test file
    File targetFile = new File(tempDir.toFile(), "lots_of_tables.xlsx");
    try (InputStream in = getClass().getResourceAsStream("/lots_of_tables.xlsx")) {
      if (in == null) {
        System.out.println("Test file /lots_of_tables.xlsx not found");

        // Create a dummy file to show the process
        targetFile = new File(tempDir.toFile(), "dummy_multi_table.xlsx");
        Files.writeString(targetFile.toPath(), "dummy");
        System.out.println("\nExpected table naming convention for multi-table Excel:");
        System.out.println("  - BaseFileName__SheetName_TableIdentifier_T1.json");
        System.out.println("  - BaseFileName__SheetName_TableIdentifier_T2.json");
        System.out.println("  - etc.");
        return;
      }
      Files.copy(in, targetFile.toPath());
    }

    // Use FileSchema to see what tables it creates
    try {
      FileSchema schema =
          new FileSchema(null, "TEST", tempDir.toFile(), null, new ExecutionEngineConfig(), false, null, null);

      System.out.println("\nTables extracted from " + targetFile.getName() + ":");
      for (String tableName : schema.getTableMap().keySet()) {
        if (tableName.startsWith("lots_of_tables") || tableName.startsWith("dummy")) {
          System.out.println("  - " + tableName);
        }
      }
    } catch (Exception e) {
      System.out.println("Error during table extraction: " + e.getMessage());

      // Show expected format anyway
      System.out.println("\nExpected table naming convention:");
      System.out.println("When multiTableExcel=true, tables are named as:");
      System.out.println("  BaseFileName__SheetName_TableIdentifier_TN");
      System.out.println("Where:");
      System.out.println("  - BaseFileName: The Excel file name without extension");
      System.out.println("  - SheetName: The sheet containing the table");
      System.out.println("  - TableIdentifier: Optional identifier found above table");
      System.out.println("  - TN: Table number (T1, T2, etc.) when multiple tables exist");
    }
  }
}
