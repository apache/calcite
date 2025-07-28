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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for HtmlToJsonConverter.
 */
public class HtmlToJsonConverterTest {

  @TempDir
  File tempDir;

  private ObjectMapper mapper = new ObjectMapper();

  /**
   * Creates a simple HTML file with one table.
   */
  private File createSimpleHtmlFile() throws IOException {
    File htmlFile = new File(tempDir, "simple.html");
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("<html><body>\n");
      writer.write("<table>\n");
      writer.write("  <tr><th>Name</th><th>Age</th><th>City</th></tr>\n");
      writer.write("  <tr><td>Alice</td><td>30</td><td>New York</td></tr>\n");
      writer.write("  <tr><td>Bob</td><td>25</td><td>San Francisco</td></tr>\n");
      writer.write("  <tr><td>Charlie</td><td>35</td><td>Chicago</td></tr>\n");
      writer.write("</table>\n");
      writer.write("</body></html>\n");
    }
    return htmlFile;
  }

  /**
   * Creates an HTML file with multiple named tables.
   */
  private File createMultiTableHtmlFile() throws IOException {
    File htmlFile = new File(tempDir, "multi.html");
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("<html><body>\n");
      writer.write("<h2>Sales Data</h2>\n");
      writer.write("<table id=\"sales\">\n");
      writer.write("  <tr><th>Product</th><th>Quantity</th><th>Price</th></tr>\n");
      writer.write("  <tr><td>Laptop</td><td>5</td><td>1200.00</td></tr>\n");
      writer.write("  <tr><td>Mouse</td><td>20</td><td>25.00</td></tr>\n");
      writer.write("</table>\n");
      writer.write("<h2>Inventory</h2>\n");
      writer.write("<table id=\"inventory\">\n");
      writer.write("  <tr><th>Item</th><th>Stock</th><th>Location</th></tr>\n");
      writer.write("  <tr><td>Laptop</td><td>50</td><td>Warehouse A</td></tr>\n");
      writer.write("  <tr><td>Mouse</td><td>200</td><td>Warehouse B</td></tr>\n");
      writer.write("  <tr><td>Keyboard</td><td>150</td><td>Warehouse A</td></tr>\n");
      writer.write("</table>\n");
      writer.write("</body></html>\n");
    }
    return htmlFile;
  }

  /**
   * Creates an HTML file with numeric data for type inference testing.
   */
  private File createNumericHtmlFile() throws IOException {
    File htmlFile = new File(tempDir, "numeric.html");
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("<html><body>\n");
      writer.write("<table>\n");
      writer.write("  <tr><th>ID</th><th>Score</th><th>Percentage</th><th>Active</th></tr>\n");
      writer.write("  <tr><td>1</td><td>95</td><td>87.5</td><td>true</td></tr>\n");
      writer.write("  <tr><td>2</td><td>82</td><td>75.25</td><td>false</td></tr>\n");
      writer.write("  <tr><td>3</td><td>91</td><td>88.0</td><td>true</td></tr>\n");
      writer.write("</table>\n");
      writer.write("</body></html>\n");
    }
    return htmlFile;
  }

  @Test public void testSimpleHtmlConversion() throws Exception {
    File htmlFile = createSimpleHtmlFile();
    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, tempDir);

    assertEquals(1, jsonFiles.size());
    File jsonFile = jsonFiles.get(0);
    assertTrue(jsonFile.exists());
    assertEquals("simple_table1.json", jsonFile.getName());

    // Read and verify JSON content
    JsonNode root = mapper.readTree(jsonFile);
    assertTrue(root.isArray());
    assertEquals(3, root.size());

    JsonNode firstRow = root.get(0);
    assertEquals("Alice", firstRow.get("Name").asText());
    assertEquals("30", firstRow.get("Age").asText());
    assertEquals("New York", firstRow.get("City").asText());

    System.out.println("\n=== SIMPLE HTML CONVERSION TEST ===");
    System.out.println("✅ Single table extracted from HTML");
    System.out.println("✅ Generated file: simple_table1.json");
    System.out.println("✅ 3 rows with correct data");
    System.out.println("✅ Column headers used as JSON keys");
    System.out.println("===================================\n");
  }

  @Test public void testMultiTableHtmlConversion() throws Exception {
    File htmlFile = createMultiTableHtmlFile();
    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, tempDir);

    assertEquals(2, jsonFiles.size());

    // Find the sales and inventory files
    File salesFile = null;
    File inventoryFile = null;
    for (File f : jsonFiles) {
      if (f.getName().contains("sales")) {
        salesFile = f;
      } else if (f.getName().contains("inventory")) {
        inventoryFile = f;
      }
    }

    assertNotNull(salesFile);
    assertNotNull(inventoryFile);

    // Verify sales table
    JsonNode salesData = mapper.readTree(salesFile);
    assertEquals(2, salesData.size());
    assertEquals("Laptop", salesData.get(0).get("Product").asText());
    assertEquals("5", salesData.get(0).get("Quantity").asText());

    // Verify inventory table
    JsonNode inventoryData = mapper.readTree(inventoryFile);
    assertEquals(3, inventoryData.size());
    assertEquals("Keyboard", inventoryData.get(2).get("Item").asText());
    assertEquals("Warehouse A", inventoryData.get(2).get("Location").asText());

    System.out.println("\n=== MULTI-TABLE HTML CONVERSION TEST ===");
    System.out.println("✅ 2 tables extracted from single HTML file");
    System.out.println("✅ Tables named using id attributes");
    System.out.println("✅ Sales table: 2 rows");
    System.out.println("✅ Inventory table: 3 rows");
    System.out.println("========================================\n");
  }

  @Test public void testNumericTypeInference() throws Exception {
    File htmlFile = createNumericHtmlFile();
    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, tempDir);

    assertEquals(1, jsonFiles.size());
    JsonNode data = mapper.readTree(jsonFiles.get(0));

    // Check first row
    JsonNode firstRow = data.get(0);
    assertTrue(firstRow.get("ID").isNumber());
    assertEquals(1, firstRow.get("ID").asInt());

    assertTrue(firstRow.get("Score").isNumber());
    assertEquals(95, firstRow.get("Score").asInt());

    assertTrue(firstRow.get("Percentage").isNumber());
    assertEquals(87.5, firstRow.get("Percentage").asDouble(), 0.01);

    assertTrue(firstRow.get("Active").isBoolean());
    assertTrue(firstRow.get("Active").asBoolean());

    System.out.println("\n=== NUMERIC TYPE INFERENCE TEST ===");
    System.out.println("✅ Integer values detected and converted");
    System.out.println("✅ Double values detected and converted");
    System.out.println("✅ Boolean values detected and converted");
    System.out.println("✅ Proper JSON types in output");
    System.out.println("===================================\n");
  }

  @Test public void testHasExtractedFiles() throws Exception {
    File htmlFile = createSimpleHtmlFile();

    // Initially no extracted files
    assertFalse(HtmlToJsonConverter.hasExtractedFiles(htmlFile, tempDir));

    // Convert
    HtmlToJsonConverter.convert(htmlFile, tempDir);

    // Now should have extracted files
    assertTrue(HtmlToJsonConverter.hasExtractedFiles(htmlFile, tempDir));

    System.out.println("\n=== EXTRACTED FILES CHECK TEST ===");
    System.out.println("✅ Correctly detects no extracted files initially");
    System.out.println("✅ Correctly detects extracted files after conversion");
    System.out.println("==================================\n");
  }

  @Test public void testEmptyHtml() throws Exception {
    File htmlFile = new File(tempDir, "empty.html");
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("<html><body><p>No tables here</p></body></html>");
    }

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, tempDir);
    assertEquals(0, jsonFiles.size());

    System.out.println("\n=== EMPTY HTML TEST ===");
    System.out.println("✅ HTML with no tables produces no JSON files");
    System.out.println("✅ Converter handles empty case gracefully");
    System.out.println("=======================\n");
  }

  @Test public void testSpecialCharactersInTableName() throws Exception {
    File htmlFile = new File(tempDir, "special.html");
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("<html><body>\n");
      writer.write("<table id=\"sales-data (2024)\">\n");
      writer.write("  <tr><th>Month</th><th>Revenue</th></tr>\n");
      writer.write("  <tr><td>Jan</td><td>10000</td></tr>\n");
      writer.write("</table>\n");
      writer.write("</body></html>\n");
    }

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, tempDir);
    assertEquals(1, jsonFiles.size());

    String fileName = jsonFiles.get(0).getName();
    assertTrue(fileName.startsWith("special_"));
    assertTrue(fileName.contains("sales_data__2024_"));
    assertTrue(fileName.endsWith(".json"));

    System.out.println("\n=== SPECIAL CHARACTERS TEST ===");
    System.out.println("✅ Special characters in table ID sanitized");
    System.out.println("✅ Generated filename: " + fileName);
    System.out.println("✅ File created successfully");
    System.out.println("===============================\n");
  }
}
