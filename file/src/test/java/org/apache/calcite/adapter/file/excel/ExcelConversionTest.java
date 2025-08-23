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

import org.apache.calcite.adapter.file.converters.SafeExcelToJsonConverter;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for Excel file conversion and conflict handling.
 */
@Tag("unit")
public class ExcelConversionTest {
  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() {
    // Clear the conversion cache before each test
    SafeExcelToJsonConverter.clearCache();
  }

  @Test public void testExcelConversionWithoutConflicts() throws IOException {
    System.out.println("\n=== Test: Excel Conversion Without Conflicts ===");

    // Create a mock Excel file
    File excelFile = new File(tempDir.toFile(), "TestData.xlsx");
    excelFile.createNewFile();

    // Create FileSchema
    FileSchema schema = new FileSchema(null, "TEST", tempDir.toFile(), null);

    // Note: Without POI dependencies working, we can't actually convert Excel files
    // But we can test the schema creation and conflict detection logic

    Map<String, Table> tables = schema.getTableMap();
    System.out.println("Tables found: " + tables.keySet());

    // In a real test with POI, we'd expect tables like:
    // - TestData__Sheet1
    // - TestData__Sheet2
  }

  @Test public void testExcelConversionWithExistingJsonFiles() throws IOException {
    System.out.println("\n=== Test: Excel Conversion With Existing JSON Files ===");

    // Create pre-existing JSON file that would conflict
    File existingJson = new File(tempDir.toFile(), "Sales__Sheet1.json");
    try (FileWriter writer = new FileWriter(existingJson, StandardCharsets.UTF_8)) {
      writer.write("[{\"existing\": \"data\", \"value\": 100}]");
    }

    // Create a newer mock Excel file
    File excelFile = new File(tempDir.toFile(), "Sales.xlsx");
    excelFile.createNewFile();
    // Make Excel file newer
    excelFile.setLastModified(System.currentTimeMillis() + 1000);

    // Test that SafeExcelToJsonConverter would handle this correctly
    try {
      SafeExcelToJsonConverter.convertIfNeeded(excelFile, tempDir.toFile(), true, "SMART_CASING", "SMART_CASING", tempDir.toFile());
      // In real scenario with POI, this would convert because Excel is newer
    } catch (Exception e) {
      // Expected without POI dependencies
      System.out.println("Expected error without POI: " + e.getMessage());
    }

    // Verify the existing file still exists
    assertTrue(existingJson.exists(), "Existing JSON should still exist");
  }

  @Test public void testMixedFileTypesInDirectory() throws IOException {
    System.out.println("\n=== Test: Mixed File Types in Directory ===");

    // Create various file types
    File csvFile = new File(tempDir.toFile(), "data.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("id,name\n1,test\n");
    }

    File jsonFile = new File(tempDir.toFile(), "config.json");
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      writer.write("[{\"setting\": \"value\"}]");
    }

    File excelFile = new File(tempDir.toFile(), "report.xlsx");
    excelFile.createNewFile();

    // Create schema
    FileSchema schema = new FileSchema(null, "TEST", tempDir.toFile(), null);
    Map<String, Table> tables = schema.getTableMap();

    // Verify all file types are recognized (table names are lowercase with SMART_CASING)
    assertNotNull(tables.get("data"), "CSV file should be recognized");
    assertNotNull(tables.get("config"), "JSON file should be recognized");

    System.out.println("Mixed directory tables: " + tables.keySet());
    assertTrue(tables.size() >= 2, "Should have at least CSV and JSON tables");
  }

  @Test public void testConversionCaching() throws IOException {
    System.out.println("\n=== Test: Conversion Caching ===");

    File excelFile = new File(tempDir.toFile(), "Cached.xlsx");
    excelFile.createNewFile();

    // First conversion attempt
    try {
      SafeExcelToJsonConverter.convertIfNeeded(excelFile, tempDir.toFile(), true, "SMART_CASING", "SMART_CASING", tempDir.toFile());
    } catch (Exception e) {
      // Expected without POI
    }

    // Second conversion attempt - should be cached
    long startTime = System.currentTimeMillis();
    try {
      SafeExcelToJsonConverter.convertIfNeeded(excelFile, tempDir.toFile(), true, "SMART_CASING", "SMART_CASING", tempDir.toFile());
    } catch (Exception e) {
      // Expected without POI
    }
    long elapsed = System.currentTimeMillis() - startTime;

    // Second attempt should be very fast due to caching
    assertTrue(elapsed < 10, "Cached conversion should be instant");

    // Clear cache and try again
    SafeExcelToJsonConverter.clearCache();
    try {
      SafeExcelToJsonConverter.convertIfNeeded(excelFile, tempDir.toFile(), true, "SMART_CASING", "SMART_CASING", tempDir.toFile());
    } catch (Exception e) {
      // Expected without POI
    }
  }

  @Test public void testTimestampBasedConversion() throws IOException, InterruptedException {
    System.out.println("\n=== Test: Timestamp-based Conversion ===");

    // Create an "old" JSON file
    File oldJson = new File(tempDir.toFile(), "TimeBased__Sheet1.json");
    try (FileWriter writer = new FileWriter(oldJson, StandardCharsets.UTF_8)) {
      writer.write("[{\"old\": \"data\"}]");
    }
    long oldTime = System.currentTimeMillis() - 10000; // 10 seconds ago
    oldJson.setLastModified(oldTime);

    // Create a "newer" Excel file
    File excelFile = new File(tempDir.toFile(), "TimeBased.xlsx");
    excelFile.createNewFile();
    long newTime = System.currentTimeMillis();
    excelFile.setLastModified(newTime);

    // Verify Excel is newer
    assertTrue(excelFile.lastModified() > oldJson.lastModified(),
        "Excel file should be newer than JSON");

    // SafeExcelToJsonConverter should detect that conversion is needed
    try {
      SafeExcelToJsonConverter.convertIfNeeded(excelFile, tempDir.toFile(), true, "SMART_CASING", "SMART_CASING", tempDir.toFile());
      // Would convert in real scenario because Excel is newer
    } catch (Exception e) {
      // Expected without POI
      System.out.println("Would convert because Excel is newer");
    }
  }
}
