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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for recursive glob pattern behavior in FileSchema.
 */
public class RecursiveGlobTest {

  @TempDir
  File tempDir;

  @Test void testRecursiveIncludesRootAndSubdirectories() throws IOException {
    // Create test files in root directory
    createCsvFile(new File(tempDir, "root1.csv"), "id,name\n1,Alice\n");
    createCsvFile(new File(tempDir, "root2.csv"), "id,value\n1,100\n");

    // Create subdirectory with files
    File subDir = new File(tempDir, "subdir");
    assertTrue(subDir.mkdir());
    createCsvFile(new File(subDir, "sub1.csv"), "id,dept\n1,Sales\n");
    createCsvFile(new File(subDir, "sub2.csv"), "id,region\n1,North\n");

    // Create nested subdirectory with files
    File nestedDir = new File(subDir, "nested");
    assertTrue(nestedDir.mkdir());
    createCsvFile(new File(nestedDir, "nested1.csv"), "id,country\n1,USA\n");

    // Create schema with recursive=true
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("recursive", true);

    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    FileSchema fileSchema = (FileSchema) FileSchemaFactory.INSTANCE
        .create(rootSchema, "TEST", operand);

    // Get all tables
    Map<String, Table> tables = fileSchema.getTableMap();

    // Should find all 5 CSV files
    assertEquals(5, tables.size(), "Should find all CSV files in root and subdirectories");

    // Verify all files are found - table names include directory path
    assertTrue(tables.containsKey("ROOT1"), "Should find root1.csv");
    assertTrue(tables.containsKey("ROOT2"), "Should find root2.csv");
    assertTrue(tables.containsKey("SUBDIR_SUB1"), "Should find subdir/sub1.csv");
    assertTrue(tables.containsKey("SUBDIR_SUB2"), "Should find subdir/sub2.csv");
    assertTrue(tables.containsKey("SUBDIR_NESTED_NESTED1"), "Should find subdir/nested/nested1.csv");
  }

  @Test void testNonRecursiveOnlyFindsRoot() throws IOException {
    // Create test files in root directory
    createCsvFile(new File(tempDir, "root1.csv"), "id,name\n1,Alice\n");
    createCsvFile(new File(tempDir, "root2.csv"), "id,value\n1,100\n");

    // Create subdirectory with files
    File subDir = new File(tempDir, "subdir");
    assertTrue(subDir.mkdir());
    createCsvFile(new File(subDir, "sub1.csv"), "id,dept\n1,Sales\n");

    // Create schema with recursive=false
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("recursive", false);

    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    FileSchema fileSchema = (FileSchema) FileSchemaFactory.INSTANCE
        .create(rootSchema, "TEST", operand);

    // Get all tables
    Map<String, Table> tables = fileSchema.getTableMap();

    // Should find only 2 CSV files in root
    assertEquals(2, tables.size(), "Should find only CSV files in root directory");

    // Verify only root files are found
    assertTrue(tables.containsKey("ROOT1"), "Should find root1.csv");
    assertTrue(tables.containsKey("ROOT2"), "Should find root2.csv");
    assertFalse(tables.containsKey("SUB1"), "Should not find sub1.csv");
  }

  @Test void testCustomGlobPatterns() throws IOException {
    // Create test files
    createCsvFile(new File(tempDir, "data.csv"), "id,name\n1,Alice\n");
    createCsvFile(new File(tempDir, "report.json"), "{\"id\":1,\"type\":\"monthly\"}");

    File subDir = new File(tempDir, "reports");
    assertTrue(subDir.mkdir());
    createCsvFile(new File(subDir, "q1.csv"), "id,quarter\n1,Q1\n");
    createCsvFile(new File(subDir, "annual.json"), "{\"id\":2,\"type\":\"yearly\"}");

    // Test 1: Only CSV files in subdirectories (not root)
    Map<String, Object> operand1 = new HashMap<>();
    operand1.put("directory", tempDir.getAbsolutePath());
    operand1.put("directoryPattern", "**/*.csv");

    SchemaPlus rootSchema1 = CalciteSchema.createRootSchema(false).plus();
    FileSchema fileSchema1 = (FileSchema) FileSchemaFactory.INSTANCE
        .create(rootSchema1, "TEST1", operand1);

    Map<String, Table> tables1 = fileSchema1.getTableMap();
    assertEquals(1, tables1.size(), "Should find only CSV files in subdirectories");
    assertTrue(tables1.containsKey("REPORTS_Q1"));

    // Test 2: Only files in reports subdirectory
    Map<String, Object> operand2 = new HashMap<>();
    operand2.put("directory", tempDir.getAbsolutePath());
    operand2.put("directoryPattern", "reports/*");

    SchemaPlus rootSchema2 = CalciteSchema.createRootSchema(false).plus();
    FileSchema fileSchema2 = (FileSchema) FileSchemaFactory.INSTANCE
        .create(rootSchema2, "TEST2", operand2);

    Map<String, Table> tables2 = fileSchema2.getTableMap();
    assertEquals(2, tables2.size(), "Should find only files in reports directory");
    assertTrue(tables2.containsKey("REPORTS_Q1"));
    assertTrue(tables2.containsKey("REPORTS_ANNUAL"));

    // Test 3: All CSV files including root - using composite pattern
    Map<String, Object> operand3 = new HashMap<>();
    operand3.put("directory", tempDir.getAbsolutePath());
    operand3.put("directoryPattern", "{*.csv,**/*.csv}");

    SchemaPlus rootSchema3 = CalciteSchema.createRootSchema(false).plus();
    FileSchema fileSchema3 = (FileSchema) FileSchemaFactory.INSTANCE
        .create(rootSchema3, "TEST3", operand3);

    Map<String, Table> tables3 = fileSchema3.getTableMap();
    assertEquals(2, tables3.size(), "Should find all CSV files in root and subdirectories");
    assertTrue(tables3.containsKey("DATA"));
    assertTrue(tables3.containsKey("REPORTS_Q1"));
  }

  private void createCsvFile(File file, String content) throws IOException {
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }
}
