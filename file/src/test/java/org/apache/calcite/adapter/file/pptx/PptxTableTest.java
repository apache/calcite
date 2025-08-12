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

import org.apache.calcite.adapter.file.converters.PptxTableScanner;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.schema.Table;
import org.apache.calcite.test.CalciteAssert;

import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTable;
import org.apache.poi.xslf.usermodel.XSLFTableCell;
import org.apache.poi.xslf.usermodel.XSLFTableRow;
import org.apache.poi.xslf.usermodel.XSLFTextBox;
import org.apache.poi.xslf.usermodel.XSLFTextParagraph;
import org.apache.poi.xslf.usermodel.XSLFTextRun;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.awt.geom.Rectangle2D;
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
 * Tests for PPTX table extraction in the file adapter.
 */
@Tag("unit")
public class PptxTableTest {
  @TempDir
  Path tempDir;

  private File simplePptxFile;
  private File complexPptxFile;

  @BeforeEach
  public void setUp() throws Exception {
    // Create test PPTX files
    createSimplePptxFile();
    createComplexPptxFile();
  }

  private void createSimplePptxFile() throws IOException {
    simplePptxFile = new File(tempDir.toFile(), "sales_presentation.pptx");

    try (XMLSlideShow ppt = new XMLSlideShow()) {
      // Create first slide with title
      XSLFSlide slide = ppt.createSlide();

      // Add slide title
      XSLFTextBox titleBox = slide.createTextBox();
      titleBox.setAnchor(new Rectangle2D.Double(50, 20, 600, 50));
      XSLFTextParagraph titlePara = titleBox.addNewTextParagraph();
      XSLFTextRun titleRun = titlePara.addNewTextRun();
      titleRun.setText("Q4 Sales Results");
      titleRun.setFontSize(32.0);
      titleRun.setBold(true);

      // Add table title
      XSLFTextBox tableTitleBox = slide.createTextBox();
      tableTitleBox.setAnchor(new Rectangle2D.Double(50, 100, 600, 30));
      XSLFTextParagraph tableTitlePara = tableTitleBox.addNewTextParagraph();
      XSLFTextRun tableTitleRun = tableTitlePara.addNewTextRun();
      tableTitleRun.setText("Regional Performance");
      tableTitleRun.setFontSize(18.0);
      tableTitleRun.setBold(true);

      // Create table
      XSLFTable table = slide.createTable();
      table.setAnchor(new Rectangle2D.Double(50, 150, 600, 200));

      // Header row
      XSLFTableRow headerRow = table.addRow();
      XSLFTableCell cell1 = headerRow.addCell();
      cell1.setText("Region");
      XSLFTableCell cell2 = headerRow.addCell();
      cell2.setText("Sales");
      XSLFTableCell cell3 = headerRow.addCell();
      cell3.setText("Growth");

      // Data rows
      XSLFTableRow row1 = table.addRow();
      row1.addCell().setText("North");
      row1.addCell().setText("120000");
      row1.addCell().setText("15%");

      XSLFTableRow row2 = table.addRow();
      row2.addCell().setText("South");
      row2.addCell().setText("98000");
      row2.addCell().setText("12%");

      XSLFTableRow row3 = table.addRow();
      row3.addCell().setText("East");
      row3.addCell().setText("145000");
      row3.addCell().setText("20%");

      try (FileOutputStream out = new FileOutputStream(simplePptxFile)) {
        ppt.write(out);
      }
    }
  }

  private void createComplexPptxFile() throws IOException {
    complexPptxFile = new File(tempDir.toFile(), "company_overview.pptx");

    try (XMLSlideShow ppt = new XMLSlideShow()) {
      // Slide 1 - Multiple tables
      XSLFSlide slide1 = ppt.createSlide();

      // Slide title
      XSLFTextBox slide1Title = slide1.createTextBox();
      slide1Title.setAnchor(new Rectangle2D.Double(50, 20, 600, 50));
      XSLFTextParagraph slide1TitlePara = slide1Title.addNewTextParagraph();
      slide1TitlePara.addNewTextRun().setText("Financial Overview");

      // First table - Revenue
      XSLFTextBox revenueTitle = slide1.createTextBox();
      revenueTitle.setAnchor(new Rectangle2D.Double(50, 80, 250, 30));
      revenueTitle.addNewTextParagraph().addNewTextRun().setText("Revenue by Quarter");

      XSLFTable revenueTable = slide1.createTable();
      revenueTable.setAnchor(new Rectangle2D.Double(50, 120, 250, 150));

      XSLFTableRow revHeader = revenueTable.addRow();
      revHeader.addCell().setText("Quarter");
      revHeader.addCell().setText("Revenue");

      XSLFTableRow q1Row = revenueTable.addRow();
      q1Row.addCell().setText("Q1");
      q1Row.addCell().setText("500000");

      XSLFTableRow q2Row = revenueTable.addRow();
      q2Row.addCell().setText("Q2");
      q2Row.addCell().setText("550000");

      // Second table - Expenses
      XSLFTextBox expenseTitle = slide1.createTextBox();
      expenseTitle.setAnchor(new Rectangle2D.Double(380, 80, 300, 30));
      expenseTitle.addNewTextParagraph().addNewTextRun().setText("Operating Expenses");

      XSLFTable expenseTable = slide1.createTable();
      expenseTable.setAnchor(new Rectangle2D.Double(380, 120, 300, 150));

      XSLFTableRow expHeader = expenseTable.addRow();
      expHeader.addCell().setText("Category");
      expHeader.addCell().setText("amount");

      XSLFTableRow salaryRow = expenseTable.addRow();
      salaryRow.addCell().setText("Salaries");
      salaryRow.addCell().setText("200000");

      XSLFTableRow rentRow = expenseTable.addRow();
      rentRow.addCell().setText("Rent");
      rentRow.addCell().setText("50000");

      // Slide 2 - Employee data
      XSLFSlide slide2 = ppt.createSlide();

      XSLFTextBox slide2Title = slide2.createTextBox();
      slide2Title.setAnchor(new Rectangle2D.Double(50, 20, 600, 50));
      slide2Title.addNewTextParagraph().addNewTextRun().setText("Team Structure");

      XSLFTextBox deptTitle = slide2.createTextBox();
      deptTitle.setAnchor(new Rectangle2D.Double(50, 80, 600, 30));
      deptTitle.addNewTextParagraph().addNewTextRun().setText("Department Headcount");

      XSLFTable deptTable = slide2.createTable();
      deptTable.setAnchor(new Rectangle2D.Double(50, 120, 600, 200));

      XSLFTableRow deptHeader = deptTable.addRow();
      deptHeader.addCell().setText("department");
      deptHeader.addCell().setText("Employees");
      deptHeader.addCell().setText("Manager");

      XSLFTableRow engRow = deptTable.addRow();
      engRow.addCell().setText("Engineering");
      engRow.addCell().setText("25");
      engRow.addCell().setText("Alice");

      XSLFTableRow salesRow = deptTable.addRow();
      salesRow.addCell().setText("Sales");
      salesRow.addCell().setText("15");
      salesRow.addCell().setText("Bob");

      try (FileOutputStream out = new FileOutputStream(complexPptxFile)) {
        ppt.write(out);
      }
    }
  }

  @Test public void testPptxTableExtraction() throws Exception {
    // Run the PPTX scanner
    PptxTableScanner.scanAndConvertTables(simplePptxFile);

    // Check that JSON file was created with slide context (now lowercase)
    File jsonFile = new File(tempDir.toFile(),
        "sales_presentation__q4_sales_results__regional_performance.json");
    assertTrue(jsonFile.exists(), "JSON file should be created from PPTX table");

    // Verify content (column names should be lowercase now)
    String jsonContent = Files.readString(jsonFile.toPath());
    assertTrue(jsonContent.contains("\"region\""));
    assertTrue(jsonContent.contains("SALES"));
    assertTrue(jsonContent.contains("\"growth\""));
    assertTrue(jsonContent.contains("North"));
    assertTrue(jsonContent.contains("120000"));
    assertTrue(jsonContent.contains("East"));
    assertTrue(jsonContent.contains("145000"));
  }

  @Test public void testMultipleTablesInPptx() throws Exception {
    // Run the PPTX scanner
    PptxTableScanner.scanAndConvertTables(complexPptxFile);

    // Debug: List all JSON files created
    File[] jsonFiles = tempDir.toFile().listFiles((dir, name) -> name.endsWith(".json"));
    System.out.println("JSON files created:");
    if (jsonFiles != null) {
      for (File f : jsonFiles) {
        System.out.println("  - " + f.getName());
      }
    }

    // Check that all JSON files were created (now lowercase)
    // Note: Indices are only added when there are duplicate names
    File revenueFile = new File(tempDir.toFile(),
        "company_overview__financial_overview__revenue_by_quarter.json");
    File expenseFile = new File(tempDir.toFile(),
        "company_overview__financial_overview__operating_expenses.json");
    File deptFile = new File(tempDir.toFile(),
        "company_overview__team_structure__department_headcount.json");

    assertTrue(revenueFile.exists(), "Revenue table JSON should be created");
    assertTrue(expenseFile.exists(), "Expense table JSON should be created");
    assertTrue(deptFile.exists(), "Department table JSON should be created");

    // Verify revenue content
    String revenueContent = Files.readString(revenueFile.toPath());
    assertTrue(revenueContent.contains("Q1"));
    assertTrue(revenueContent.contains("500000"));

    // Verify expense content
    String expenseContent = Files.readString(expenseFile.toPath());
    assertTrue(expenseContent.contains("Salaries"));
    assertTrue(expenseContent.contains("200000"));

    // Verify department content
    String deptContent = Files.readString(deptFile.toPath());
    assertTrue(deptContent.contains("Engineering"));
    assertTrue(deptContent.contains("Alice"));
  }

  @Test public void testPptxWithoutTableTitle() throws Exception {
    File noTitleFile = new File(tempDir.toFile(), "no_title.pptx");

    try (XMLSlideShow ppt = new XMLSlideShow()) {
      XSLFSlide slide = ppt.createSlide();

      // Just slide title, no table title
      XSLFTextBox slideTitle = slide.createTextBox();
      slideTitle.setAnchor(new Rectangle2D.Double(50, 20, 600, 50));
      slideTitle.addNewTextParagraph().addNewTextRun().setText("Data Slide");

      // Table without preceding text
      XSLFTable table = slide.createTable();
      table.setAnchor(new Rectangle2D.Double(50, 150, 600, 200));

      XSLFTableRow header = table.addRow();
      header.addCell().setText("Item");
      header.addCell().setText("value");

      XSLFTableRow data = table.addRow();
      data.addCell().setText("Test");
      data.addCell().setText("123");

      try (FileOutputStream out = new FileOutputStream(noTitleFile)) {
        ppt.write(out);
      }
    }

    PptxTableScanner.scanAndConvertTables(noTitleFile);

    // Should create file with slide info but no table title - adds __table when no title
    File jsonFile = new File(tempDir.toFile(), "no_title__data_slide__table.json");
    assertTrue(jsonFile.exists(), "Should create table with slide context when no table title");
  }

  @Test public void testEmptyPptxFile() throws Exception {
    File emptyFile = new File(tempDir.toFile(), "empty.pptx");

    try (XMLSlideShow ppt = new XMLSlideShow()) {
      XSLFSlide slide = ppt.createSlide();

      XSLFTextBox text = slide.createTextBox();
      text.setAnchor(new Rectangle2D.Double(50, 50, 600, 100));
      text.addNewTextParagraph().addNewTextRun().setText("This presentation has no tables.");

      try (FileOutputStream out = new FileOutputStream(emptyFile)) {
        ppt.write(out);
      }
    }

    // Should not throw exception
    PptxTableScanner.scanAndConvertTables(emptyFile);

    // No JSON files should be created
    File[] jsonFiles = tempDir.toFile().listFiles((dir, name) ->
        name.startsWith("Empty") && name.endsWith(".json"));
    assertEquals(0, jsonFiles.length, "No JSON files should be created for PPTX without tables");
  }

  @Test public void testPptxInFileSchema() throws Exception {
    // Create a simple schema with PPTX files
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile());

    FileSchema schema = new FileSchema(null, "TEST", tempDir.toFile(), null, null,
        new ExecutionEngineConfig(), false, null, null, null, null);

    // Convert PPTX files first
    PptxTableScanner.scanAndConvertTables(simplePptxFile);
    PptxTableScanner.scanAndConvertTables(complexPptxFile);

    // Check that tables are accessible
    Map<String, Table> tables = schema.getTableMap();

    // Tables should be created from the generated JSON files (uppercase table names in schema)
    // Note: Indices are only added when there are duplicate names
    assertTrue(tables.containsKey("SALES_PRESENTATION__Q4_SALES_RESULTS__REGIONAL_PERFORMANCE"),
        "Should have sales presentation table");
    assertTrue(tables.containsKey("COMPANY_OVERVIEW__FINANCIAL_OVERVIEW__REVENUE_BY_QUARTER"),
        "Should have revenue table");
    assertTrue(tables.containsKey("COMPANY_OVERVIEW__FINANCIAL_OVERVIEW__OPERATING_EXPENSES"),
        "Should have expense table");
    assertTrue(tables.containsKey("COMPANY_OVERVIEW__TEAM_STRUCTURE__DEPARTMENT_HEADCOUNT"),
        "Should have department table");
  }

  @Test public void testPptxTableQuery() throws Exception {
    // Run the scanner first
    PptxTableScanner.scanAndConvertTables(simplePptxFile);

    // Create schema and run query
    final Map<String, Object> operand = ImmutableMap.of("directory", tempDir.toFile());

    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .withSchema("pptx", new FileSchema(null, "TEST", tempDir.toFile(), null, null,
            new ExecutionEngineConfig(), false, null, null, null, null))
        .query("SELECT * FROM \"pptx\".\"SALES_PRESENTATION__Q4_SALES_RESULTS__REGIONAL_PERFORMANCE\" " +
               "WHERE CAST(\"sales\" AS INTEGER) > 100000")
        .returnsCount(2); // North (120000) and East (145000) have sales > 100000
  }

  @Test public void testPptxWithViewSimplification() throws Exception {
    // Run the scanner first to generate the complex named tables
    PptxTableScanner.scanAndConvertTables(complexPptxFile);

    // Create a model file with views that simplify the complex PPTX table names
    String modelContent = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"presentations\",\n" +
        "  \"schemas\": [\n" +
        "    {\n" +
        "      \"name\": \"raw_pptx\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"directory\": \"" + tempDir.toFile().getAbsolutePath().replace("\\", "\\\\") + "\"\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"name\": \"presentations\",\n" +
        "      \"tables\": [\n" +
        "        {\n" +
        "          \"name\": \"revenue\",\n" +
        "          \"type\": \"view\",\n" +
        "          \"sql\": \"SELECT * FROM \\\"raw_pptx\\\".\\\"company_overview__financial_overview__revenue_by_quarter\\\"\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"name\": \"expenses\",\n" +
        "          \"type\": \"view\",\n" +
        "          \"sql\": \"SELECT * FROM \\\"raw_pptx\\\".\\\"company_overview__financial_overview__operating_expenses\\\"\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"name\": \"departments\",\n" +
        "          \"type\": \"view\",\n" +
        "          \"sql\": \"SELECT * FROM \\\"raw_pptx\\\".\\\"company_overview__team_structure__department_headcount\\\"\"\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    // Now query the views with simple names - Oracle lex with TO_LOWER casing, no quotes needed for lowercase
    CalciteAssert.model(modelContent)
        .with(Lex.ORACLE)
        .with(CalciteConnectionProperty.UNQUOTED_CASING, Casing.TO_LOWER)
        .query("SELECT * FROM revenue WHERE quarter = 'Q1'")
        .returnsCount(1)
        .returns("quarter=Q1; revenue=500000\n");

    CalciteAssert.model(modelContent)
        .with(Lex.ORACLE)
        .with(CalciteConnectionProperty.UNQUOTED_CASING, Casing.TO_LOWER)
        .query("SELECT * FROM expenses WHERE category = 'Salaries'")
        .returnsCount(1)
        .returns("category=Salaries; amount=200000\n");

    CalciteAssert.model(modelContent)
        .with(Lex.ORACLE)
        .with(CalciteConnectionProperty.UNQUOTED_CASING, Casing.TO_LOWER)
        .query("SELECT * FROM departments WHERE department = 'Engineering'")
        .returnsCount(1)
        .returns("department=Engineering; employees=25; manager=Alice\n");

    // Join the simplified views
    CalciteAssert.model(modelContent)
        .with(Lex.ORACLE)
        .with(CalciteConnectionProperty.UNQUOTED_CASING, Casing.TO_LOWER)
        .query("SELECT r.quarter, r.revenue, e.category, e.amount " +
               "FROM revenue r, expenses e " +
               "WHERE r.quarter = 'Q2' AND e.category = 'Rent'")
        .returnsCount(1)
        .returns("quarter=Q2; revenue=550000; category=Rent; amount=50000\n");
  }

  @Test public void testPptxNameSimplificationDemo() throws Exception {
    // This test demonstrates how PPTX tables get complex names based on their structure
    // and shows how queries can work with these names

    // Run the scanner first to generate the complex named tables
    PptxTableScanner.scanAndConvertTables(complexPptxFile);

    // Create schema with the PPTX files
    FileSchema schema = new FileSchema(null, "TEST", tempDir.toFile(), null, null,
        new ExecutionEngineConfig(), false, null, null, null, null);

    // Test querying with the complex names (now lowercase columns, uppercase table names in schema)
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .withSchema("pptx", schema)
        .query("SELECT * FROM \"pptx\".\"COMPANY_OVERVIEW__FINANCIAL_OVERVIEW__REVENUE_BY_QUARTER\" " +
               "WHERE \"quarter\" = 'Q1'")
        .returnsCount(1)
        .returns("quarter=Q1; revenue=500000\n");

    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .withSchema("pptx", schema)
        .query("SELECT * FROM \"pptx\".\"COMPANY_OVERVIEW__FINANCIAL_OVERVIEW__OPERATING_EXPENSES\" " +
               "WHERE \"category\" = 'Salaries'")
        .returnsCount(1)
        .returns("category=Salaries; amount=200000\n");

    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .withSchema("pptx", schema)
        .query("SELECT * FROM \"pptx\".\"COMPANY_OVERVIEW__TEAM_STRUCTURE__DEPARTMENT_HEADCOUNT\" " +
               "WHERE \"department\" = 'Engineering'")
        .returnsCount(1)
        .returns("department=Engineering; employees=25; manager=Alice\n");

    // Demonstrate how views could simplify these names in a model file
    // NOTE: In a real deployment, you could create a model file with views like:
    //
    // {
    //   "version": "1.0",
    //   "defaultSchema": "presentations",
    //   "schemas": [{
    //     "name": "raw_pptx",
    //     "type": "custom",
    //     "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    //     "operand": {
    //       "directory": "/path/to/pptx/json/files"
    //     }
    //   }, {
    //     "name": "presentations",
    //     "type": "custom",
    //     "factory": "org.apache.calcite.schema.impl.AbstractSchema$Factory",
    //     "tables": [
    //       {
    //         "name": "revenue",
    //         "type": "view",
    //         "sql": "SELECT * FROM raw_pptx.COMPANYOVERVIEW__SLIDE1_FINANCIAL_OVERVIEW__REVENUE_BY_QUARTER"
    //       },
    //       {
    //         "name": "expenses",
    //         "type": "view",
    //         "sql": "SELECT * FROM raw_pptx.COMPANYOVERVIEW__SLIDE1_FINANCIAL_OVERVIEW__OPERATING_EXPENSES"
    //       },
    //       {
    //         "name": "departments",
    //         "type": "view",
    //         "sql": "SELECT * FROM raw_pptx.COMPANYOVERVIEW__SLIDE2_TEAM_STRUCTURE__DEPARTMENT_HEADCOUNT"
    //       }
    //     ]
    //   }]
    // }
    //
    // This would allow users to query with simple names like:
    // SELECT * FROM presentations.revenue WHERE "Quarter" = 'Q1'
    // Instead of the complex auto-generated names
  }
}
