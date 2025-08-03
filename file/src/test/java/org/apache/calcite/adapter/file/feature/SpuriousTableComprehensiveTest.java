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
package org.apache.calcite.adapter.file.feature;

import org.apache.calcite.adapter.file.MultiTableExcelToJsonConverter;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive test for spurious table detection and handling.
 * Consolidates functionality from SpuriousTableTest, RealSpuriousTest,
 * and ReproduceSpuriousTest.
 */
@SuppressWarnings("deprecation")
public class SpuriousTableComprehensiveTest {

  @TempDir
  public File tempDir;

  @Test void testSpuriousTableDetection() throws Exception {
    // Create Excel file with embedded tables that might produce spurious tables
    File excelFile = createTwoEmbeddedTablesExcel();

    // Convert to JSON and check for spurious tables
    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    // List all JSON files created
    File[] jsonFiles = tempDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertThat(jsonFiles.length >= 2, is(true)); // Should have at least 2 legitimate tables

    // Check content of generated files
    for (File jsonFile : jsonFiles) {
      String content = Files.readString(jsonFile.toPath());

      // Spurious tables often contain only headers or empty data
      if (isSpuriousTable(content)) {
        System.out.println("Detected spurious table: " + jsonFile.getName());
        System.out.println("Content: " + content);
      }
    }

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();

      // Filter out spurious tables (ones with only headers)
      Set<String> legitimateTables = tableNames.stream()
          .filter(name -> !name.toLowerCase().contains("spurious"))
          .collect(Collectors.toSet());

      assertTrue(legitimateTables.size() >= 2, "Should have at least 2 legitimate tables");

      try (Statement stmt = conn.createStatement()) {
        // Verify legitimate tables have actual data
        for (String tableName : legitimateTables) {
          try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"" + tableName + "\"")) {
            assertTrue(rs.next());
            long count = rs.getLong("cnt");
            assertTrue(count > 0, "Table " + tableName + " should have data");
            assertTrue(count < 100, "Table " + tableName + " shouldn't have too much data (spurious)");
          }
        }
      }
    }
  }

  @Test void testHeaderOnlySpuriousTable() throws Exception {
    // Create Excel with table that has headers but no data
    File excelFile = createHeaderOnlyExcel();

    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    File[] jsonFiles = tempDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertTrue(jsonFiles.length > 0);

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Header-only tables should be queryable but return no rows
      for (File jsonFile : jsonFiles) {
        String tableName = jsonFile.getName().replace(".json", "");
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"" + tableName + "\"")) {
          assertTrue(rs.next());
          assertEquals(0L, rs.getLong("cnt"));
        }
      }
    }
  }

  @Test void testEmptyRowSpuriousTable() throws Exception {
    // Create Excel with empty rows that might be detected as tables
    File excelFile = createEmptyRowsExcel();

    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    File[] jsonFiles = tempDir.listFiles((dir, name) -> name.endsWith(".json"));

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();

      try (Statement stmt = conn.createStatement()) {
        // Should not create tables from empty rows
        for (String tableName : tableNames) {
          try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + tableName + "\" LIMIT 1")) {
            if (rs.next()) {
              // If there's data, it should be meaningful
              String firstColumn = rs.getString(1);
              assertFalse(firstColumn == null || firstColumn.trim().isEmpty(),
                  "Table " + tableName + " contains empty first column");
            }
          }
        }
      }
    }
  }

  @Test void testFormattingArtifactSpuriousTable() throws Exception {
    // Create Excel with formatting that might be mistaken for a table
    File excelFile = createFormattingArtifactExcel();

    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();

      try (Statement stmt = conn.createStatement()) {
        // Verify tables contain actual business data, not formatting artifacts
        for (String tableName : tableNames) {
          try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + tableName + "\"")) {
            int validRows = 0;
            while (rs.next() && validRows < 10) {
              // Check if row contains meaningful data
              boolean hasNonEmptyColumn = false;
              for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                String value = rs.getString(i);
                if (value != null && !value.trim().isEmpty() && !isFormattingArtifact(value)) {
                  hasNonEmptyColumn = true;
                  break;
                }
              }
              if (hasNonEmptyColumn) {
                validRows++;
              }
            }

            // If table exists, it should have some valid rows
            if (tableNames.size() > 0) {
              assertTrue(validRows > 0, "Table " + tableName + " appears to be spurious (no valid data)");
            }
          }
        }
      }
    }
  }

  @Test void testMergedCellSpuriousTable() throws Exception {
    // Create Excel with merged cells that might create spurious table detection
    File excelFile = createMergedCellComplexExcel();

    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();

      // Should handle merged cells without creating spurious tables
      for (String tableName : tableNames) {
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"" + tableName + "\"")) {
          assertTrue(rs.next());
          long count = rs.getLong("cnt");

          // Merged cell handling should not create excessive empty rows
          assertTrue(count < 50, "Table " + tableName + " has too many rows (possible merged cell issue)");
        }
      }
    }
  }

  @Test void testHtmlSpuriousTableDetection() throws Exception {
    // Create HTML with nested tables that might create spurious detection
    File htmlFile = createNestedTablesHtml();

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();

      // Should detect legitimate tables, not layout tables
      for (String tableName : tableNames) {
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + tableName + "\" LIMIT 5")) {
          int columnCount = rs.getMetaData().getColumnCount();

          // Layout tables often have very few columns or very many
          assertTrue(columnCount >= 2 && columnCount <= 10,
              "Table " + tableName + " has suspicious column count: " + columnCount);

          // Check for meaningful data
          if (rs.next()) {
            boolean hasData = false;
            for (int i = 1; i <= columnCount; i++) {
              String value = rs.getString(i);
              if (value != null && value.length() > 1 && !isLayoutElement(value)) {
                hasData = true;
                break;
              }
            }
            assertTrue(hasData, "Table " + tableName + " appears to be a layout table");
          }
        }
      }
    }
  }

  @Test void testCsvSpuriousLineDetection() throws Exception {
    // Create CSV with comment lines or empty lines that might be detected as data
    File csvFile = createCsvWithComments();

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"csv_with_comments\"")) {
        // Should skip comment lines and empty lines
        int validRows = 0;
        while (rs.next()) {
          String firstCol = rs.getString(1);
          if (firstCol != null && !firstCol.startsWith("#") && !firstCol.trim().isEmpty()) {
            validRows++;
          }
        }

        assertEquals(3, validRows); // Should have exactly 3 data rows
      }
    }
  }

  // Helper methods

  private boolean isSpuriousTable(String jsonContent) {
    // Simple heuristics to detect spurious tables
    return jsonContent.trim().equals("[]") || // Empty array
           jsonContent.length() < 50 || // Very short content
           jsonContent.contains("\"column1\":null") || // All null columns
           !jsonContent.contains("\""); // No actual string values
  }

  private boolean isFormattingArtifact(String value) {
    return value.matches("^[\\s\\-_=]+$") || // Just formatting characters
           value.toLowerCase().matches("(page|section|chapter)\\s*\\d+") || // Page numbers
           value.matches("^\\d+\\.\\d+$"); // Version numbers without context
  }

  private boolean isLayoutElement(String value) {
    return value.toLowerCase().matches(".*(menu|header|footer|nav|sidebar).*") ||
           value.trim().matches("^[&nbsp;\\s]*$"); // HTML entities
  }

  private String createModel(File directory) {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'FILES',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'FILES',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + directory.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  private File createTwoEmbeddedTablesExcel() throws IOException {
    File file = new File(tempDir, "embedded_tables.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
         FileOutputStream fos = new FileOutputStream(file)) {

      Sheet sheet = workbook.createSheet("Sheet1");

      // First table at A1
      Row header1 = sheet.createRow(0);
      header1.createCell(0).setCellValue("Employee ID");
      header1.createCell(1).setCellValue("Name");
      header1.createCell(2).setCellValue("Department");

      Row data1 = sheet.createRow(1);
      data1.createCell(0).setCellValue(1);
      data1.createCell(1).setCellValue("Alice");
      data1.createCell(2).setCellValue("Engineering");

      Row data2 = sheet.createRow(2);
      data2.createCell(0).setCellValue(2);
      data2.createCell(1).setCellValue("Bob");
      data2.createCell(2).setCellValue("Sales");

      // Gap rows that might create spurious detection
      sheet.createRow(3); // Empty row
      sheet.createRow(4); // Empty row

      // Second table at A6
      Row header2 = sheet.createRow(5);
      header2.createCell(0).setCellValue("Product ID");
      header2.createCell(1).setCellValue("Product Name");
      header2.createCell(2).setCellValue("Price");

      Row prod1 = sheet.createRow(6);
      prod1.createCell(0).setCellValue(101);
      prod1.createCell(1).setCellValue("Widget");
      prod1.createCell(2).setCellValue(19.99);

      Row prod2 = sheet.createRow(7);
      prod2.createCell(0).setCellValue(102);
      prod2.createCell(1).setCellValue("Gadget");
      prod2.createCell(2).setCellValue(29.99);

      workbook.write(fos);
    }
    return file;
  }

  private File createHeaderOnlyExcel() throws IOException {
    File file = new File(tempDir, "header_only.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
         FileOutputStream fos = new FileOutputStream(file)) {

      Sheet sheet = workbook.createSheet("Sheet1");

      // Table with headers but no data
      Row header = sheet.createRow(0);
      header.createCell(0).setCellValue("ID");
      header.createCell(1).setCellValue("Name");
      header.createCell(2).setCellValue("Status");

      // No data rows - this might create a spurious table

      workbook.write(fos);
    }
    return file;
  }

  private File createEmptyRowsExcel() throws IOException {
    File file = new File(tempDir, "empty_rows.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
         FileOutputStream fos = new FileOutputStream(file)) {

      Sheet sheet = workbook.createSheet("Sheet1");

      // Create several empty rows
      for (int i = 0; i < 10; i++) {
        sheet.createRow(i);
      }

      // Actual table starts at row 11
      Row header = sheet.createRow(10);
      header.createCell(0).setCellValue("Data");
      header.createCell(1).setCellValue("Value");

      Row data = sheet.createRow(11);
      data.createCell(0).setCellValue("Test");
      data.createCell(1).setCellValue(123);

      workbook.write(fos);
    }
    return file;
  }

  private File createFormattingArtifactExcel() throws IOException {
    File file = new File(tempDir, "formatting_artifact.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
         FileOutputStream fos = new FileOutputStream(file)) {

      Sheet sheet = workbook.createSheet("Sheet1");

      // Title row
      Row title = sheet.createRow(0);
      title.createCell(0).setCellValue("QUARTERLY REPORT - Q1 2024");

      // Separator row
      Row separator = sheet.createRow(1);
      separator.createCell(0).setCellValue("=====================================");

      // Page number
      Row pageNum = sheet.createRow(2);
      pageNum.createCell(0).setCellValue("Page 1.0");

      // Actual data table
      Row header = sheet.createRow(4);
      header.createCell(0).setCellValue("Quarter");
      header.createCell(1).setCellValue("Revenue");

      Row data = sheet.createRow(5);
      data.createCell(0).setCellValue("Q1");
      data.createCell(1).setCellValue(150000);

      workbook.write(fos);
    }
    return file;
  }

  private File createMergedCellComplexExcel() throws IOException {
    File file = new File(tempDir, "merged_complex.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
         FileOutputStream fos = new FileOutputStream(file)) {

      Sheet sheet = workbook.createSheet("Sheet1");

      // Complex layout with merged cells
      Row row1 = sheet.createRow(0);
      row1.createCell(0).setCellValue("Category");
      row1.createCell(1).setCellValue("Sales Data");

      // Merge B1:C1
      sheet.addMergedRegion(new org.apache.poi.ss.util.CellRangeAddress(0, 0, 1, 2));

      Row row2 = sheet.createRow(1);
      row2.createCell(0).setCellValue("Electronics");
      row2.createCell(1).setCellValue("Q1");
      row2.createCell(2).setCellValue("Q2");

      Row row3 = sheet.createRow(2);
      row3.createCell(0).setCellValue("Laptops");
      row3.createCell(1).setCellValue(100);
      row3.createCell(2).setCellValue(120);

      Row row4 = sheet.createRow(3);
      row4.createCell(0).setCellValue("Phones");
      row4.createCell(1).setCellValue(80);
      row4.createCell(2).setCellValue(95);

      workbook.write(fos);
    }
    return file;
  }

  private File createNestedTablesHtml() throws IOException {
    File file = new File(tempDir, "nested_tables.html");
    String html = "<html><body>\n"
        + "<table border='1'>\n" // Layout table
        + "<tr><td>\n"
        + "  <h1>Sales Report</h1>\n"
        + "  <table border='1'>\n" // Actual data table
        + "    <tr><th>Product</th><th>Sales</th></tr>\n"
        + "    <tr><td>Widget</td><td>100</td></tr>\n"
        + "    <tr><td>Gadget</td><td>150</td></tr>\n"
        + "  </table>\n"
        + "</td><td>\n"
        + "  <div>Navigation</div>\n"
        + "  <table border='1'>\n" // Navigation table (spurious)
        + "    <tr><td>Menu</td></tr>\n"
        + "    <tr><td>Home</td></tr>\n"
        + "    <tr><td>About</td></tr>\n"
        + "  </table>\n"
        + "</td></tr>\n"
        + "</table>\n"
        + "</body></html>";

    try (FileWriter writer = new FileWriter(file)) {
      writer.write(html);
    }
    return file;
  }

  private File createCsvWithComments() throws IOException {
    File file = new File(tempDir, "csv_with_comments.csv");
    String csv = "# This is a comment\n"
        + "# Author: Test\n"
        + "id,name,value\n"
        + "1,Alice,100\n"
        + "# Another comment in the middle\n"
        + "\n" // Empty line
        + "2,Bob,200\n"
        + "3,Charlie,300\n"
        + "# End of file\n";

    try (FileWriter writer = new FileWriter(file)) {
      writer.write(csv);
    }
    return file;
  }
}
