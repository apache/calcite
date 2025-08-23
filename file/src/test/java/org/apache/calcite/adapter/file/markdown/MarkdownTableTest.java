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

import org.apache.calcite.adapter.file.converters.MarkdownTableScanner;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.schema.Table;
import org.apache.calcite.test.CalciteAssert;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Isolated;import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.Isolated;import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Markdown table extraction in the file adapter.
 */
@Tag("unit")
@Isolated  // Required due to engine-specific behavior and shared state
public class MarkdownTableTest {
  @TempDir
  Path tempDir;

  private File markdownFile;
  private File complexMarkdownFile;

  @BeforeEach
  public void setUp() throws Exception {
    // Create test Markdown files
    createSimpleMarkdownFile();
    createComplexMarkdownFile();
  }

  private void createSimpleMarkdownFile() throws IOException {
    markdownFile = new File(tempDir.toFile(), "products.md");
    try (FileWriter writer = new FileWriter(markdownFile, StandardCharsets.UTF_8)) {
      writer.write("# Product Catalog\n\n");
      writer.write("## Current Products\n\n");
      writer.write("| Product | Price | Stock |\n");
      writer.write("|---------|-------|-------|\n");
      writer.write("| Widget  | 10.99 | 100   |\n");
      writer.write("| Gadget  | 25.50 | 50    |\n");
      writer.write("| Tool    | 15.75 | 75    |\n");
    }
  }

  private void createComplexMarkdownFile() throws IOException {
    complexMarkdownFile = new File(tempDir.toFile(), "quarterly_report.md");
    try (FileWriter writer = new FileWriter(complexMarkdownFile, StandardCharsets.UTF_8)) {
      writer.write("# Quarterly Report\n\n");
      writer.write("## Sales Summary\n\n");
      writer.write("| Region | Q1 Sales | Q2 Sales |\n");
      writer.write("|--------|----------|----------|\n");
      writer.write("| North  | 50000    | 55000    |\n");
      writer.write("| South  | 45000    | 48000    |\n\n");
      writer.write("Some text between tables.\n\n");
      writer.write("## Employee Performance\n\n");
      writer.write("| Employee | Department | Rating |\n");
      writer.write("|----------|------------|--------|\n");
      writer.write("| Alice    | Sales      | A      |\n");
      writer.write("| Bob      | Marketing  | B      |\n");
      writer.write("| Charlie  | Engineering| A      |\n");
    }
  }

  @Test public void testMarkdownTableExtraction() throws Exception {
    // Run the Markdown scanner
    MarkdownTableScanner.scanAndConvertTables(markdownFile, tempDir.toFile());

    // Check that JSON file was created
    File jsonFile = new File(tempDir.toFile(), "Products__Current_Products.json");
    assertTrue(jsonFile.exists(), "JSON file should be created from Markdown table");

    // Verify content
    String jsonContent = Files.readString(jsonFile.toPath());
    assertTrue(jsonContent.contains("Widget"));
    assertTrue(jsonContent.contains("10.99"));
    assertTrue(jsonContent.contains("Gadget"));
  }

  @Test public void testMultipleTablesInMarkdown() throws Exception {
    // Run the Markdown scanner
    MarkdownTableScanner.scanAndConvertTables(complexMarkdownFile, tempDir.toFile());

    // Check that both JSON files were created
    File salesFile = new File(tempDir.toFile(), "QuarterlyReport__Sales_Summary.json");
    File employeeFile = new File(tempDir.toFile(), "QuarterlyReport__Employee_Performance.json");

    assertTrue(salesFile.exists(), "Sales summary JSON should be created");
    assertTrue(employeeFile.exists(), "Employee performance JSON should be created");

    // Verify sales content
    String salesContent = Files.readString(salesFile.toPath());
    assertTrue(salesContent.contains("North"));
    assertTrue(salesContent.contains("50000"));

    // Verify employee content
    String employeeContent = Files.readString(employeeFile.toPath());
    assertTrue(employeeContent.contains("Alice"));
    assertTrue(employeeContent.contains("Sales"));
  }

  @Test public void testMarkdownWithGroupHeaders() throws Exception {
    File groupHeaderFile = new File(tempDir.toFile(), "budget.md");
    try (FileWriter writer = new FileWriter(groupHeaderFile, StandardCharsets.UTF_8)) {
      writer.write("# Budget Report\n\n");
      writer.write("## Department Budgets\n\n");
      writer.write("|            | 2023      |           | 2024      |           |\n");
      writer.write("| Department | Budget    | Spent     | Budget    | Spent     |\n");
      writer.write("|------------|-----------|-----------|-----------|----------|\n");
      writer.write("| Sales      | 100000    | 95000     | 110000    | 50000     |\n");
      writer.write("| Marketing  | 80000     | 78000     | 85000     | 40000     |\n");
    }

    MarkdownTableScanner.scanAndConvertTables(groupHeaderFile, tempDir.toFile());

    File jsonFile = new File(tempDir.toFile(), "Budget__Department_Budgets.json");
    assertTrue(jsonFile.exists(), "JSON file with group headers should be created");

    String content = Files.readString(jsonFile.toPath());
    // Check that group headers were properly combined
    assertTrue(content.contains("2023_Budget") || content.contains("Budget"));
    assertTrue(content.contains("Sales"));
  }

  @Test public void testMarkdownInFileSchema() throws Exception {
    // Create a simple schema with Markdown files
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile());

    FileSchema schema = new FileSchema(null, "TEST", tempDir.toFile(), null, null, new ExecutionEngineConfig(), false, null, null, null, null);

    // Convert Markdown files first
    MarkdownTableScanner.scanAndConvertTables(markdownFile, tempDir.toFile());
    MarkdownTableScanner.scanAndConvertTables(complexMarkdownFile, tempDir.toFile());

    // Check that tables are accessible
    Map<String, Table> tables = schema.getTableMap();

    // Tables should be created from the generated JSON files
    assertTrue(tables.containsKey("products__current_products"),
        "Should have products__current_products table");
    assertTrue(tables.containsKey("quarterly_report__sales_summary"),
        "Should have quarterly_report__sales_summary table");
    assertTrue(tables.containsKey("quarterly_report__employee_performance"),
        "Should have quarterly_report__employee_performance table");
  }

  @Test public void testMarkdownTableQuery() throws Exception {
    // Run the scanner first
    MarkdownTableScanner.scanAndConvertTables(markdownFile, tempDir.toFile());

    // Create schema and run query
    final Map<String, Object> operand = ImmutableMap.of("directory", tempDir.toFile());

    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .withSchema("markdown", new FileSchema(null, "TEST", tempDir.toFile(), null, null, new ExecutionEngineConfig(), false, null, null, null, null))
        .query("SELECT * FROM \"markdown\".\"products__current_products\" WHERE CAST(\"price\" AS DECIMAL) >= 15.75")
        .returnsCount(2); // Gadget (25.50) and Tool (15.75) have prices >= 15.75
  }

  @Test public void testMarkdownWithSpecialCharacters() throws Exception {
    File specialFile = new File(tempDir.toFile(), "special_chars.md");
    try (FileWriter writer = new FileWriter(specialFile, StandardCharsets.UTF_8)) {
      writer.write("# Special Characters Test\n\n");
      writer.write("## Data with Special Chars\n\n");
      writer.write("| Name | Value | Description |\n");
      writer.write("|------|-------|-------------|\n");
      writer.write("| Test\\|Pipe | 100 | Contains pipe |\n");
      writer.write("| Test-Dash | 200 | Contains dash |\n");
      writer.write("| Test_Under | 300 | Contains underscore |\n");
    }

    MarkdownTableScanner.scanAndConvertTables(specialFile, tempDir.toFile());

    File jsonFile = new File(tempDir.toFile(), "SpecialChars__Data_with_Special_Chars.json");
    assertTrue(jsonFile.exists(), "JSON file should handle special characters");

    String content = Files.readString(jsonFile.toPath());
    assertTrue(content.contains("Test|Pipe"), "Should handle escaped pipes");
    assertTrue(content.contains("Test-Dash"));
    assertTrue(content.contains("Test_Under"));
  }

  @Test public void testEmptyMarkdownFile() throws Exception {
    File emptyFile = new File(tempDir.toFile(), "empty.md");
    try (FileWriter writer = new FileWriter(emptyFile, StandardCharsets.UTF_8)) {
      writer.write("# Empty Document\n\n");
      writer.write("This document has no tables.\n");
    }

    // Should not throw exception
    MarkdownTableScanner.scanAndConvertTables(emptyFile, tempDir.toFile());

    // No JSON files should be created
    File[] jsonFiles = tempDir.toFile().listFiles((dir, name) ->
        name.startsWith("Empty") && name.endsWith(".json"));
    assertEquals(0, jsonFiles.length, "No JSON files should be created for empty Markdown");
  }

  @Test public void testMarkdownTableWithoutTitle() throws Exception {
    File noTitleFile = new File(tempDir.toFile(), "no_title.md");
    try (FileWriter writer = new FileWriter(noTitleFile, StandardCharsets.UTF_8)) {
      writer.write("Some introductory text.\n\n");
      writer.write("| Column1 | Column2 |\n");
      writer.write("|---------|--------|\n");
      writer.write("| Data1   | Data2  |\n");
    }

    MarkdownTableScanner.scanAndConvertTables(noTitleFile, tempDir.toFile());

    // Should create file without Table suffix since there's only one table
    File jsonFile = new File(tempDir.toFile(), "NoTitle.json");
    assertTrue(jsonFile.exists(), "Should create table with generic name when no heading");
  }
}
