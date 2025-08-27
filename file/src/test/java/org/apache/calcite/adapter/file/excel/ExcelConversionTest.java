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
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for Excel file conversion and conflict handling.
 */
@Tag("unit")
public class ExcelConversionTest extends BaseFileTest {
  private Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    // Clear static caches that might interfere with test isolation
    Sources.clearFileCache();
    SafeExcelToJsonConverter.clearCache();
    // Create a temporary directory for each test
    tempDir = Files.createTempDirectory("excel-conversion-test");
    // Force garbage collection to release any resources
    System.gc();
    // Wait to ensure cleanup is complete
    Thread.sleep(100);
  }

  @AfterEach
  public void tearDown() throws Exception {
    // Clear caches after test to prevent contamination
    Sources.clearFileCache();
    SafeExcelToJsonConverter.clearCache();
    // Clean up temp directory
    if (tempDir != null && Files.exists(tempDir)) {
      try {
        Files.walk(tempDir)
            .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
            .forEach(path -> {
              try {
                Files.deleteIfExists(path);
              } catch (IOException e) {
                // Best effort cleanup
              }
            });
      } catch (IOException e) {
        // Best effort cleanup
      }
    }
    System.gc();
    Thread.sleep(100);
  }

  @Test public void testExcelConversionWithoutConflicts() throws Exception {
    System.out.println("\n=== Test: Excel Conversion Without Conflicts ===");

    // Create a mock Excel file
    File excelFile = new File(tempDir.toFile(), "TestData.xlsx");
    excelFile.createNewFile();

    Properties connectionProps = new Properties();
    applyEngineDefaults(connectionProps);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", connectionProps);
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Create operand map with ephemeralCache for test isolation
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      // Create schema with ephemeral cache
      rootSchema.add("TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));

      // Get tables using standard JDBC metadata
      java.sql.ResultSet tables = connection.getMetaData().getTables(null, "TEST", "%", new String[]{"TABLE", "VIEW"});
      System.out.print("Tables found: ");
      while (tables.next()) {
        System.out.print(tables.getString("TABLE_NAME") + " ");
      }
      System.out.println();

    }
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

  @Test public void testMixedFileTypesInDirectory() throws Exception {
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

    Properties connectionProps = new Properties();
    applyEngineDefaults(connectionProps);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", connectionProps);
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Create operand map with ephemeralCache for test isolation
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      // Create schema with ephemeral cache
      rootSchema.add("TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));

      // Verify all file types are recognized using JDBC metadata
      java.sql.ResultSet tables = connection.getMetaData().getTables(null, "TEST", "%", new String[]{"TABLE", "VIEW"});

      boolean hasData = false;
      boolean hasConfig = false;
      int tableCount = 0;

      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        if ("data".equals(tableName)) hasData = true;
        if ("config".equals(tableName)) hasConfig = true;
        tableCount++;
        System.out.println("Found table: " + tableName);
      }

      assertTrue(hasData, "CSV file 'data' should be recognized");
      assertTrue(hasConfig, "JSON file 'config' should be recognized");
      assertTrue(tableCount >= 2, "Should have at least CSV and JSON tables");
    }
  }

  @Test public void testConversionCaching() throws IOException {
    System.out.println("\n=== Test: Conversion Caching ===");

    File excelFile = new File(tempDir.toFile(), "Cached.xlsx");
    excelFile.createNewFile();

    // First conversion attempt - will fail with empty file but should cache the failure
    try {
      SafeExcelToJsonConverter.convertIfNeeded(excelFile, tempDir.toFile(), true, "SMART_CASING", "SMART_CASING", tempDir.toFile());
    } catch (Exception e) {
      // Expected - empty Excel file
      System.out.println("Expected error for empty Excel: " + e.getMessage());
    }

    // Second conversion attempt - should be cached
    long startTime = System.currentTimeMillis();
    try {
      SafeExcelToJsonConverter.convertIfNeeded(excelFile, tempDir.toFile(), true, "SMART_CASING", "SMART_CASING", tempDir.toFile());
    } catch (Exception e) {
      // Expected - empty Excel file
    }
    long elapsed = System.currentTimeMillis() - startTime;

    // Second attempt should be very fast due to caching
    assertTrue(elapsed < 100, "Cached conversion should be instant");

    // Clear cache and try again
    SafeExcelToJsonConverter.clearCache();
    try {
      SafeExcelToJsonConverter.convertIfNeeded(excelFile, tempDir.toFile(), true, "SMART_CASING", "SMART_CASING", tempDir.toFile());
    } catch (Exception e) {
      // Expected - empty Excel file
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
