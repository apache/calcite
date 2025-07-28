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

import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Demonstration test for recursive directory scanning feature.
 */
public class RecursiveDemoTest {
  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    // Create nested directory structure with CSV files
    createNestedDirectoryStructure();
  }

  private void createNestedDirectoryStructure() throws Exception {
    // Root level file
    File rootCsv = new File(tempDir.toFile(), "root_data.csv");
    try (FileWriter writer = new FileWriter(rootCsv, StandardCharsets.UTF_8)) {
      writer.write("id,name,level\n");
      writer.write("1,Root Item,0\n");
    }

    // Level 1 directory
    File level1Dir = new File(tempDir.toFile(), "level1");
    level1Dir.mkdir();

    File level1Csv = new File(level1Dir, "level1_data.csv");
    try (FileWriter writer = new FileWriter(level1Csv, StandardCharsets.UTF_8)) {
      writer.write("id,name,level\n");
      writer.write("2,Level 1 Item,1\n");
    }

    // Level 2 directory
    File level2Dir = new File(level1Dir, "level2");
    level2Dir.mkdir();

    File level2Csv = new File(level2Dir, "level2_data.csv");
    try (FileWriter writer = new FileWriter(level2Csv, StandardCharsets.UTF_8)) {
      writer.write("id,name,level\n");
      writer.write("3,Level 2 Item,2\n");
    }

    // Another branch
    File branchDir = new File(tempDir.toFile(), "branch");
    branchDir.mkdir();

    File branchCsv = new File(branchDir, "branch_data.csv");
    try (FileWriter writer = new FileWriter(branchCsv, StandardCharsets.UTF_8)) {
      writer.write("id,name,level\n");
      writer.write("4,Branch Item,1\n");
    }
  }

  @Test public void testRecursiveDirectoryTableNames() throws Exception {
    System.out.println("\n=== TESTING RECURSIVE DIRECTORY SCANNING ===");
    System.out.println("Test setup:");
    System.out.println("  tempDir/");
    System.out.println("    ├── root_data.csv");
    System.out.println("    ├── level1/");
    System.out.println("    │   ├── level1_data.csv");
    System.out.println("    │   └── level2/");
    System.out.println("    │       └── level2_data.csv");
    System.out.println("    └── branch/");
    System.out.println("        └── branch_data.csv");

    // Test with recursive=true
    FileSchema recursiveSchema =
        new FileSchema(null, "files", tempDir.toFile(), null, new ExecutionEngineConfig(), true, null, null);

    Map<String, Table> recursiveTables = recursiveSchema.getTableMap();

    System.out.println("\nTables found with recursive=true:");
    for (String tableName : recursiveTables.keySet()) {
      System.out.println("  - " + tableName);
    }

    // Test with recursive=false
    FileSchema nonRecursiveSchema =
        new FileSchema(null, "files", tempDir.toFile(), null, new ExecutionEngineConfig(), false, null, null);

    Map<String, Table> nonRecursiveTables = nonRecursiveSchema.getTableMap();

    System.out.println("\nTables found with recursive=false:");
    for (String tableName : nonRecursiveTables.keySet()) {
      System.out.println("  - " + tableName);
    }

    // Verify recursive found more tables
    assertTrue(recursiveTables.size() > nonRecursiveTables.size(),
        "Recursive mode should find more tables than non-recursive");

    // Verify specific table names with dot notation
    assertTrue(recursiveTables.containsKey("root_data"),
        "Should find root level file");
    assertTrue(recursiveTables.containsKey("level1.level1_data"),
        "Should find level1/level1_data.csv as 'level1.level1_data'");
    assertTrue(recursiveTables.containsKey("level1.level2.level2_data"),
        "Should find level1/level2/level2_data.csv as 'level1.level2.level2_data'");
    assertTrue(recursiveTables.containsKey("branch.branch_data"),
        "Should find branch/branch_data.csv as 'branch.branch_data'");

    System.out.println("\n✅ RECURSIVE DIRECTORY SCANNING WORKS!");
    System.out.println("✅ Subdirectory files get dot notation table names!");
    System.out.println("✅ Total tables found: " + recursiveTables.size());
  }
}
