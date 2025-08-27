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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.converters.HtmlToJsonConverter;

import org.apache.calcite.adapter.file.BaseFileTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * End-to-end tests for file conversion refresh functionality.
 * Tests that converters automatically record metadata for refresh tracking.
 */
@Tag("unit")
@Isolated  // Needs isolation due to static ConversionMetadata state
public class RefreshEndToEndTest extends BaseFileTest {
  
  /**
   * Checks if refresh functionality is supported by the current engine.
   * Refresh only works with PARQUET and DUCKDB engines.
   */
  private boolean isRefreshSupported() {
    String engine = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    if (engine == null || engine.isEmpty()) {
      return true; // Default engine supports refresh
    }
    String engineUpper = engine.toUpperCase();
    return "PARQUET".equals(engineUpper) || "DUCKDB".equals(engineUpper);
  }
  
  /**
   * Checks if Parquet-specific functionality is supported by the current engine.
   * Parquet-specific tests only work with PARQUET and DUCKDB engines.
   */
  private boolean isParquetSupported() {
    String engine = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    if (engine == null || engine.isEmpty()) {
      return true; // Default engine supports Parquet functionality
    }
    String engineUpper = engine.toUpperCase();
    return "PARQUET".equals(engineUpper) || "DUCKDB".equals(engineUpper);
  }

  private File schemaDir;
  
  @BeforeEach
  public void setupTestFiles() throws Exception {
    schemaDir = Files.createTempDirectory("refresh-test-").toFile();
  }
  
  @AfterEach
  public void cleanup() throws Exception {
    if (schemaDir != null && schemaDir.exists()) {
      deleteDirectory(schemaDir);
    }
  }
  
  private void deleteDirectory(File dir) {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    dir.delete();
  }
  
  @Test
  public void testHtmlToJsonConversionRecordsMetadata() throws Exception {
    System.out.println("\n=== TEST: HTML to JSON Conversion Metadata Recording ===");
    
    // Create an HTML file with a table
    File htmlFile = new File(schemaDir, "test.html");
    try (FileWriter writer = new FileWriter(htmlFile, StandardCharsets.UTF_8)) {
      writer.write("<html><body>\n");
      writer.write("<table>\n");
      writer.write("<tr><th>name</th><th>value</th></tr>\n");
      writer.write("<tr><td>item1</td><td>100</td></tr>\n");
      writer.write("<tr><td>item2</td><td>200</td></tr>\n");
      writer.write("</table>\n");
      writer.write("</body></html>\n");
    }
    
    // Convert HTML to JSON using the converter directly
    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, schemaDir, schemaDir);
    assertFalse(jsonFiles.isEmpty(), "Should create at least one JSON file");
    
    File jsonFile = jsonFiles.get(0);
    assertTrue(jsonFile.exists(), "JSON file should exist");
    System.out.println("Created JSON file: " + jsonFile.getName());
    
    // Verify conversion was recorded automatically
    ConversionMetadata metadata = new ConversionMetadata(schemaDir);
    File foundSource = metadata.findOriginalSource(jsonFile);
    assertNotNull(foundSource, "Conversion metadata should be recorded automatically by converter");
    assertEquals(htmlFile.getCanonicalPath(), foundSource.getCanonicalPath(), 
                "Should find correct original source");
    
    System.out.println("✅ HTML to JSON conversion automatically recorded metadata");
    System.out.println("  Source: " + htmlFile.getName());
    System.out.println("  Converted: " + jsonFile.getName());
    System.out.println("  Metadata: Found source " + foundSource.getName());
  }
  
  @Test
  public void testConversionMetadataPersistence() throws Exception {
    System.out.println("\n=== TEST: Conversion Metadata Persistence ===");
    
    // Create test files
    File htmlFile = new File(schemaDir, "persist_test.html");
    File jsonFile = new File(schemaDir, "persist_test.json");
    
    try (FileWriter writer = new FileWriter(htmlFile, StandardCharsets.UTF_8)) {
      writer.write("<html><body><table><tr><th>test</th></tr><tr><td>data</td></tr></table></body></html>");
    }
    
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      writer.write("[{\"test\": \"data\"}]");
    }
    
    // Record a conversion
    ConversionMetadata metadata1 = new ConversionMetadata(schemaDir);
    metadata1.recordConversion(htmlFile, jsonFile, "html-to-json");
    
    // Create new instance (simulates restart)
    ConversionMetadata metadata2 = new ConversionMetadata(schemaDir);
    
    // Should still find the conversion
    File foundSource = metadata2.findOriginalSource(jsonFile);
    assertNotNull(foundSource, "Conversion should persist across metadata instances");
    assertEquals(htmlFile.getCanonicalPath(), foundSource.getCanonicalPath(),
                "Should find same source file after restart simulation");
    
    System.out.println("✅ Conversion metadata persists across restarts");
    System.out.println("  Recorded: " + htmlFile.getName() + " -> " + jsonFile.getName());
    System.out.println("  Retrieved after restart: " + foundSource.getName());
  }
  
  @Test
  public void testQueryWithParquetEngine() throws Exception {
    assumeFalse(!isParquetSupported(), "Parquet-specific functionality only supported by PARQUET and DUCKDB engines");
    System.out.println("\n=== TEST: Query with Parquet Engine ===");
    
    // Create a simple JSON file directly for querying
    File jsonFile = new File(schemaDir, "simple_data.json");
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      writer.write("[\n");
      writer.write("  {\"name\": \"item1\", \"value\": 100},\n");
      writer.write("  {\"name\": \"item2\", \"value\": 200}\n");
      writer.write("]\n");
    }
    
    // Create connection with parquet engine
    Properties info = new Properties();
    info.setProperty("directory", schemaDir.getAbsolutePath());
    info.setProperty("engine", "parquet");
    
    try (Connection connection = createConnection(info)) {
      Statement statement = connection.createStatement();
      
      // Query using lowercase unquoted identifiers and uppercase keywords
      // VALUE is a reserved word in ORACLE lexer, so we need to quote it
      ResultSet rs = statement.executeQuery("SELECT name, \"value\" FROM simple_data ORDER BY name");
      
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        String name = rs.getString("name");
        int value = rs.getInt("value");
        System.out.println("  Row: " + name + " = " + value);
      }
      rs.close();
      
      assertEquals(2, rowCount, "Should have 2 rows");
      System.out.println("✅ Parquet engine query successful");
      
      statement.close();
    }
  }
  
  @Test
  public void testHtmlRefreshOnSourceChange() throws Exception {
    assumeFalse(!isRefreshSupported(), "Refresh functionality only supported by PARQUET and DUCKDB engines");
    System.out.println("\n=== TEST: HTML Refresh on Source Change ===");
    
    // Create an HTML file with initial data
    File htmlFile = new File(schemaDir, "refresh_test.html");
    try (FileWriter writer = new FileWriter(htmlFile, StandardCharsets.UTF_8)) {
      writer.write("<html><body>\n");
      writer.write("<table>\n");
      writer.write("<tr><th>id</th><th>name</th></tr>\n");
      writer.write("<tr><td>1</td><td>Alice</td></tr>\n");
      writer.write("</table>\n");
      writer.write("</body></html>\n");
    }
    
    // Convert HTML to JSON
    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, schemaDir, schemaDir);
    assertFalse(jsonFiles.isEmpty(), "Should create at least one JSON file");
    File jsonFile = jsonFiles.get(0);
    System.out.println("Generated JSON file: " + jsonFile.getName());
    
    // Get the table name from the JSON file name (removing .json extension)
    String tableName = jsonFile.getName().replaceFirst("\\.json$", "");
    System.out.println("Table name: " + tableName);
    
    // Verify initial conversion was recorded
    ConversionMetadata metadata = new ConversionMetadata(schemaDir);
    File foundSource = metadata.findOriginalSource(jsonFile);
    assertNotNull(foundSource, "Conversion should be recorded");
    
    // Create connection with refresh enabled (parquet engine)
    Properties info = new Properties();
    info.setProperty("directory", schemaDir.getAbsolutePath());
    info.setProperty("engine", "parquet");
    info.setProperty("refreshInterval", "1 second");
    
    try (Connection connection = createConnection(info)) {
      Statement statement = connection.createStatement();
      
      // Query initial data
      ResultSet rs = statement.executeQuery("SELECT id, name FROM " + tableName + " ORDER BY id");
      assertTrue(rs.next());
      assertEquals("1", rs.getString("id"));
      assertEquals("Alice", rs.getString("name"));
      assertFalse(rs.next());
      rs.close();
      
      System.out.println("Initial data: id=1, name=Alice");
      
      // Modify the HTML source file
      Thread.sleep(1100); // Ensure file timestamp changes
      try (FileWriter writer = new FileWriter(htmlFile, StandardCharsets.UTF_8)) {
        writer.write("<html><body>\n");
        writer.write("<table>\n");
        writer.write("<tr><th>id</th><th>name</th></tr>\n");
        writer.write("<tr><td>2</td><td>Bob</td></tr>\n");
        writer.write("<tr><td>3</td><td>Charlie</td></tr>\n");
        writer.write("</table>\n");
        writer.write("</body></html>\n");
      }
      
      // Force file timestamp update
      htmlFile.setLastModified(System.currentTimeMillis());
      
      // The refresh mechanism should detect the change and re-convert
      // Wait for refresh interval
      Thread.sleep(1500);
      
      // Query again - should see updated data
      rs = statement.executeQuery("SELECT id, name FROM " + tableName + " ORDER BY id");
      assertTrue(rs.next());
      assertEquals("2", rs.getString("id"));
      assertEquals("Bob", rs.getString("name"));
      assertTrue(rs.next());
      assertEquals("3", rs.getString("id"));
      assertEquals("Charlie", rs.getString("name"));
      assertFalse(rs.next());
      rs.close();
      
      System.out.println("✅ HTML source change detected and JSON re-converted");
      System.out.println("  Updated data: id=2, name=Bob; id=3, name=Charlie");
      
      statement.close();
    }
  }
  
  private Connection createConnection(Properties info) throws Exception {
    String url = "jdbc:calcite:";
    Properties connectionProperties = new Properties();
    applyEngineDefaults(connectionProperties);
    
    // File adapter configuration
    StringBuilder model = new StringBuilder();
    model.append("{\n");
    model.append("  \"version\": \"1.0\",\n");
    model.append("  \"defaultSchema\": \"files\",\n");
    model.append("  \"schemas\": [\n");
    model.append("    {\n");
    model.append("      \"name\": \"files\",\n");
    model.append("      \"type\": \"custom\",\n");
    model.append("      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n");
    model.append("      \"operand\": {\n");
    model.append("        \"directory\": \"").append(info.getProperty("directory").replace("\\", "\\\\")).append("\",\n");
    model.append("        \"ephemeralCache\": true,\n");
    
    if (info.containsKey("refreshInterval")) {
      model.append("        \"refreshInterval\": \"").append(info.getProperty("refreshInterval")).append("\",\n");
    }
    
    if (info.containsKey("engine")) {
      model.append("        \"executionEngine\": \"").append(info.getProperty("engine")).append("\",\n");
    }
    
    model.append("        \"caseSensitive\": false\n");
    model.append("      }\n");
    model.append("    }\n");
    model.append("  ]\n");
    model.append("}\n");
    
    connectionProperties.setProperty("model", "inline:" + model.toString());
    
    return DriverManager.getConnection(url, connectionProperties);
  }
}