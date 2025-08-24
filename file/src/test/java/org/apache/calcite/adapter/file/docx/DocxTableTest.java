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

import org.apache.calcite.adapter.file.converters.DocxTableScanner;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.schema.Table;
import org.apache.calcite.test.CalciteAssert;

import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for DOCX table extraction in the file adapter.
 */
@Tag("unit")
public class DocxTableTest {
  private File tempDir;
  private File simpleDocxFile;
  private File complexDocxFile;

  @BeforeEach
  public void setUp() throws Exception {
    // Create temporary directory manually
    tempDir = Files.createTempDirectory("docx-test").toFile();
    
    // Create test DOCX files
    createSimpleDocxFile();
    createComplexDocxFile();
  }

  @AfterEach
  public void tearDown() {
    // Clean up temporary directory - non-fatal
    if (tempDir != null && tempDir.exists()) {
      try {
        deleteDirectory(tempDir);
      } catch (Exception e) {
        // Cleanup failure should not fail the test
        System.err.println("Warning: Failed to clean up temp directory: " + e.getMessage());
      }
    }
  }

  private void deleteDirectory(File directory) {
    try {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
      directory.delete();
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  private void createSimpleDocxFile() throws IOException {
    simpleDocxFile = new File(tempDir, "products.docx");

    try (XWPFDocument document = new XWPFDocument()) {
      // Add title
      XWPFParagraph title = document.createParagraph();
      XWPFRun titleRun = title.createRun();
      titleRun.setText("Product Catalog");
      titleRun.setBold(true);
      titleRun.setFontSize(16);

      // Add some text before table
      XWPFParagraph intro = document.createParagraph();
      intro.createRun().setText("Current inventory of products:");

      // Add table title
      XWPFParagraph tableTitle = document.createParagraph();
      XWPFRun tableTitleRun = tableTitle.createRun();
      tableTitleRun.setText("Current Products");
      tableTitleRun.setBold(true);

      // Create table
      XWPFTable table = document.createTable();

      // Header row
      XWPFTableRow headerRow = table.getRow(0);
      headerRow.getCell(0).setText("product");
      headerRow.addNewTableCell().setText("price");
      headerRow.addNewTableCell().setText("Stock");

      // Data rows
      XWPFTableRow row1 = table.createRow();
      row1.getCell(0).setText("Widget");
      row1.getCell(1).setText("10.99");
      row1.getCell(2).setText("100");

      XWPFTableRow row2 = table.createRow();
      row2.getCell(0).setText("Gadget");
      row2.getCell(1).setText("25.50");
      row2.getCell(2).setText("50");

      XWPFTableRow row3 = table.createRow();
      row3.getCell(0).setText("Tool");
      row3.getCell(1).setText("15.75");
      row3.getCell(2).setText("75");

      try (FileOutputStream out = new FileOutputStream(simpleDocxFile)) {
        document.write(out);
      }
    }
  }

  private void createComplexDocxFile() throws IOException {
    complexDocxFile = new File(tempDir, "quarterly_report.docx");

    try (XWPFDocument document = new XWPFDocument()) {
      // Document title
      XWPFParagraph docTitle = document.createParagraph();
      XWPFRun docTitleRun = docTitle.createRun();
      docTitleRun.setText("Quarterly Business Report");
      docTitleRun.setBold(true);
      docTitleRun.setFontSize(18);

      // First table - Sales Summary
      XWPFParagraph salesHeader = document.createParagraph();
      XWPFRun salesHeaderRun = salesHeader.createRun();
      salesHeaderRun.setText("Regional Sales Summary");
      salesHeaderRun.setBold(true);
      salesHeaderRun.setFontSize(14);

      XWPFTable salesTable = document.createTable();
      XWPFTableRow salesHeaderRow = salesTable.getRow(0);
      salesHeaderRow.getCell(0).setText("Region");
      salesHeaderRow.addNewTableCell().setText("Q1 Sales");
      salesHeaderRow.addNewTableCell().setText("Q2 Sales");

      XWPFTableRow northRow = salesTable.createRow();
      northRow.getCell(0).setText("North");
      northRow.getCell(1).setText("50000");
      northRow.getCell(2).setText("55000");

      XWPFTableRow southRow = salesTable.createRow();
      southRow.getCell(0).setText("South");
      southRow.getCell(1).setText("45000");
      southRow.getCell(2).setText("48000");

      // Add some text between tables
      XWPFParagraph separator = document.createParagraph();
      separator.createRun().setText("Employee performance metrics are shown below:");

      // Second table - Employee Performance
      XWPFParagraph empHeader = document.createParagraph();
      XWPFRun empHeaderRun = empHeader.createRun();
      empHeaderRun.setText("Employee Performance");
      empHeaderRun.setBold(true);
      empHeaderRun.setFontSize(14);

      XWPFTable empTable = document.createTable();
      XWPFTableRow empHeaderRow = empTable.getRow(0);
      empHeaderRow.getCell(0).setText("Employee");
      empHeaderRow.addNewTableCell().setText("department");
      empHeaderRow.addNewTableCell().setText("Rating");

      XWPFTableRow aliceRow = empTable.createRow();
      aliceRow.getCell(0).setText("Alice");
      aliceRow.getCell(1).setText("Sales");
      aliceRow.getCell(2).setText("a");

      XWPFTableRow bobRow = empTable.createRow();
      bobRow.getCell(0).setText("Bob");
      bobRow.getCell(1).setText("Marketing");
      bobRow.getCell(2).setText("b");

      XWPFTableRow charlieRow = empTable.createRow();
      charlieRow.getCell(0).setText("Charlie");
      charlieRow.getCell(1).setText("Engineering");
      charlieRow.getCell(2).setText("a");

      try (FileOutputStream out = new FileOutputStream(complexDocxFile)) {
        document.write(out);
      }
    }
  }

  @Test public void testDocxTableExtraction() throws Exception {
    // Run the DOCX scanner
    DocxTableScanner.scanAndConvertTables(simpleDocxFile, tempDir);

    // Check that JSON file was created
    File jsonFile = new File(tempDir, "products__current_products.json");
    assertTrue(jsonFile.exists(), "JSON file should be created from DOCX table");

    // Verify content
    String jsonContent = Files.readString(jsonFile.toPath());
    assertTrue(jsonContent.contains("Widget"));
    assertTrue(jsonContent.contains("10.99"));
    assertTrue(jsonContent.contains("Gadget"));
  }

  @Test public void testMultipleTablesInDocx() throws Exception {
    // Run the DOCX scanner
    DocxTableScanner.scanAndConvertTables(complexDocxFile, tempDir);

    // Check that both JSON files were created
    File salesFile = new File(tempDir, "quarterly_report__regional_sales_summary.json");
    File employeeFile = new File(tempDir, "quarterly_report__employee_performance.json");

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

  @Test public void testDocxWithGroupHeaders() throws Exception {
    File groupHeaderFile = new File(tempDir, "budget.docx");

    try (XWPFDocument document = new XWPFDocument()) {
      // Document title
      XWPFParagraph title = document.createParagraph();
      title.createRun().setText("Budget Report");

      // Table title
      XWPFParagraph tableTitle = document.createParagraph();
      XWPFRun titleRun = tableTitle.createRun();
      titleRun.setText("Department Budgets");
      titleRun.setBold(true);

      // Create table with group headers
      XWPFTable table = document.createTable();

      // Group header row
      XWPFTableRow groupRow = table.getRow(0);
      groupRow.getCell(0).setText("");
      groupRow.addNewTableCell().setText("2023");
      groupRow.addNewTableCell().setText("");
      groupRow.addNewTableCell().setText("2024");
      groupRow.addNewTableCell().setText("");

      // Detail header row
      XWPFTableRow headerRow = table.createRow();
      headerRow.getCell(0).setText("department");
      headerRow.getCell(1).setText("Budget");
      headerRow.getCell(2).setText("Spent");
      headerRow.getCell(3).setText("Budget");
      headerRow.getCell(4).setText("Spent");

      // Data row
      XWPFTableRow dataRow = table.createRow();
      dataRow.getCell(0).setText("Sales");
      dataRow.getCell(1).setText("100000");
      dataRow.getCell(2).setText("95000");
      dataRow.getCell(3).setText("110000");
      dataRow.getCell(4).setText("50000");

      try (FileOutputStream out = new FileOutputStream(groupHeaderFile)) {
        document.write(out);
      }
    }

    DocxTableScanner.scanAndConvertTables(groupHeaderFile, tempDir);

    File jsonFile = new File(tempDir, "budget__department_budgets.json");
    assertTrue(jsonFile.exists(), "JSON file with group headers should be created");

    String content = Files.readString(jsonFile.toPath());
    // Check that group headers were properly combined
    assertTrue(content.contains("Sales"));
    assertTrue(content.contains("100000"));
  }

  @Test public void testDocxInFileSchema() throws Exception {
    // modelBaseDirectory is the parent directory where .aperio will be created
    // FileSchema will create tempDir/.aperio/TEST as the actual base directory
    File sourceDir = tempDir;

    // Create FileSchema with proper source and base directory configuration
    FileSchema schema = new FileSchema(null, "TEST", sourceDir, tempDir, null, null, 
        new ExecutionEngineConfig(), false, null, null, null, null, 
        "SMART_CASING", "SMART_CASING", null, null, null, null, true);

    // FileSchema should automatically detect and convert DOCX files via FileConversionManager
    // No manual conversion needed - it will happen when getTableMap() is called

    // Check that tables are accessible
    Map<String, Table> tables = schema.getTableMap();

    // Tables should be created from the generated JSON files in base directory
    assertTrue(tables.containsKey("products__current_products"),
        "Should have products__current_products table");
    assertTrue(tables.containsKey("quarterly_report__regional_sales_summary"),
        "Should have quarterly_report__regional_sales_summary table");
    assertTrue(tables.containsKey("quarterly_report__employee_performance"),
        "Should have quarterly_report__employee_performance table");
  }

  @Test public void testDocxTableQuery() throws Exception {
    // Run the scanner first
    DocxTableScanner.scanAndConvertTables(simpleDocxFile, tempDir);

    // modelBaseDirectory is the parent directory where .aperio will be created
    // FileSchema will create tempDir/.aperio/TEST as the actual base directory

    // Create schema and run query
    final Map<String, Object> operand = ImmutableMap.of("directory", tempDir);

    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .withSchema("docx", new FileSchema(null, "TEST", tempDir, tempDir, null, null, 
            new ExecutionEngineConfig(), false, null, null, null, null, 
            "SMART_CASING", "SMART_CASING", null, null, null, null, true))
        .query("SELECT * FROM \"docx\".\"products__current_products\" WHERE CAST(\"price\" AS DECIMAL) >= 15.75")
        .returnsCount(2); // Gadget (25.50) and Tool (15.75) have prices >= 15.75
  }

  @Test public void testEmptyDocxFile() throws Exception {
    File emptyFile = new File(tempDir, "empty.docx");

    try (XWPFDocument document = new XWPFDocument()) {
      XWPFParagraph para = document.createParagraph();
      para.createRun().setText("This document has no tables.");

      try (FileOutputStream out = new FileOutputStream(emptyFile)) {
        document.write(out);
      }
    }

    // Should not throw exception
    DocxTableScanner.scanAndConvertTables(emptyFile, tempDir);

    // No JSON files should be created
    File[] jsonFiles = tempDir.listFiles((dir, name) ->
        name.startsWith("Empty") && name.endsWith(".json"));
    assertEquals(0, jsonFiles.length, "No JSON files should be created for empty DOCX");
  }

  @Test public void testDocxTableWithoutTitle() throws Exception {
    File noTitleFile = new File(tempDir, "no_title.docx");

    try (XWPFDocument document = new XWPFDocument()) {
      // Just add a table without any preceding title
      XWPFTable table = document.createTable();

      XWPFTableRow headerRow = table.getRow(0);
      headerRow.getCell(0).setText("Column1");
      headerRow.addNewTableCell().setText("Column2");

      XWPFTableRow dataRow = table.createRow();
      dataRow.getCell(0).setText("Data1");
      dataRow.getCell(1).setText("Data2");

      try (FileOutputStream out = new FileOutputStream(noTitleFile)) {
        document.write(out);
      }
    }

    DocxTableScanner.scanAndConvertTables(noTitleFile, tempDir);

    // Should create file without Table suffix since there's only one table
    File jsonFile = new File(tempDir, "no_title.json");
    assertTrue(jsonFile.exists(), "Should create table with generic name when no heading");
  }
}
