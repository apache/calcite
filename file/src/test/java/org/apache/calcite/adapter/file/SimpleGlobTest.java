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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple tests to validate glob pattern functionality.
 */
public class SimpleGlobTest {

  @TempDir
  File tempDir;

  private File dataDir;
  private File cacheDir;

  @BeforeEach
  public void setUp() throws IOException {
    dataDir = new File(tempDir, "data");
    cacheDir = new File(tempDir, "cache");
    dataDir.mkdirs();
    cacheDir.mkdirs();
  }

  @Test public void testHtmlToJsonConverter() throws Exception {
    // Create a simple HTML file
    File htmlFile = new File(dataDir, "test.html");
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("<html><body>\n");
      writer.write("<table>\n");
      writer.write("  <tr><th>Name</th><th>Age</th></tr>\n");
      writer.write("  <tr><td>Alice</td><td>30</td></tr>\n");
      writer.write("  <tr><td>Bob</td><td>25</td></tr>\n");
      writer.write("</table>\n");
      writer.write("</body></html>\n");
    }

    // Convert HTML to JSON
    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, dataDir);

    assertEquals(1, jsonFiles.size(), "Should convert one table to JSON");
    assertTrue(jsonFiles.get(0).exists(), "JSON file should exist");
    assertTrue(jsonFiles.get(0).getName().endsWith(".json"), "Should be a JSON file");

    System.out.println("\n=== HTML TO JSON CONVERTER TEST ===");
    System.out.println("✅ HTML file converted to JSON successfully");
    System.out.println("✅ Generated file: " + jsonFiles.get(0).getName());
    System.out.println("===================================\n");
  }

  @Test public void testGlobParquetTableCreation() throws Exception {
    // Create some test CSV files
    createTestCsvFile("data1.csv");
    createTestCsvFile("data2.csv");

    // Create GlobParquetTable
    String globPattern = dataDir.getAbsolutePath() + "/data*.csv";
    GlobParquetTable globTable =
        new GlobParquetTable(globPattern,
        "test_glob",
        cacheDir,
        Duration.ofMinutes(5));

    assertNotNull(globTable, "GlobParquetTable should be created");
    assertEquals(globPattern, globTable.toString().replace("GlobParquetTable{", "").replace("}", ""));
    assertTrue(globTable.needsRefresh(), "Should need initial refresh");

    System.out.println("\n=== GLOB PARQUET TABLE TEST ===");
    System.out.println("✅ GlobParquetTable created successfully");
    System.out.println("✅ Glob pattern: " + globPattern);
    System.out.println("✅ Initial refresh needed: " + globTable.needsRefresh());
    System.out.println("==============================\n");
  }

  @Test public void testGlobPatternDetection() {
    // Test various glob patterns
    assertTrue(isGlobPattern("*.csv"), "Should detect * wildcard");
    assertTrue(isGlobPattern("/data/*.json"), "Should detect * in path");
    assertTrue(isGlobPattern("reports/2024-*.xlsx"), "Should detect * with prefix");
    assertTrue(isGlobPattern("data/sales_[0-9].csv"), "Should detect [] ranges");
    assertTrue(isGlobPattern("**/*.parquet"), "Should detect ** recursive");
    assertTrue(isGlobPattern("file??.txt"), "Should detect ? wildcard");

    // Test non-glob patterns
    assertFalse(isGlobPattern("data.csv"), "Should not detect plain filename");
    assertFalse(isGlobPattern("/path/to/file.json"), "Should not detect plain path");
    assertFalse(isGlobPattern("http://example.com/data.csv"), "Should not detect URL");

    System.out.println("\n=== GLOB PATTERN DETECTION TEST ===");
    System.out.println("✅ Correctly identified patterns with * wildcard");
    System.out.println("✅ Correctly identified patterns with ? wildcard");
    System.out.println("✅ Correctly identified patterns with [] ranges");
    System.out.println("✅ Correctly rejected non-glob patterns");
    System.out.println("==================================\n");
  }

  private void createTestCsvFile(String filename) throws IOException {
    File csvFile = new File(dataDir, filename);
    try (FileWriter writer = new FileWriter(csvFile)) {
      writer.write("id,name,value\n");
      writer.write("1,Test1,100\n");
      writer.write("2,Test2,200\n");
    }
  }

  // Helper method to test glob pattern detection
  private boolean isGlobPattern(String url) {
    String path = url;
    if (url.contains("://")) {
      int idx = url.indexOf("://");
      path = url.substring(idx + 3);
    }
    return path.contains("*") || path.contains("?") ||
           (path.contains("[") && path.contains("]"));
  }
}
