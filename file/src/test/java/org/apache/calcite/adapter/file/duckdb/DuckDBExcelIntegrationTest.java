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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.BaseFileTest;
import org.apache.calcite.test.CalciteAssert;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify DuckDB can handle pre-converted Parquet files.
 * NOTE: DuckDB cannot discover Excel files that are converted after connection creation.
 * All files must exist as Parquet before the DuckDB connection is established.
 */
public class DuckDBExcelIntegrationTest extends BaseFileTest {

  @TempDir
  static Path tempDir;

  /**
   * Creates a simple Excel file with employee data for testing.
   */
  private static void createTestExcelFile(File file) throws Exception {
    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("employees");

      // Create header row
      Row headerRow = sheet.createRow(0);
      headerRow.createCell(0).setCellValue("id");
      headerRow.createCell(1).setCellValue("name");
      headerRow.createCell(2).setCellValue("department");
      headerRow.createCell(3).setCellValue("salary");

      // Add sample data
      Object[][] data = {
          {1, "Alice", "Engineering", 95000},
          {2, "Bob", "Sales", 75000},
          {3, "Charlie", "Engineering", 105000},
          {4, "Diana", "HR", 65000},
          {5, "Eve", "Sales", 80000}
      };

      int rowNum = 1;
      for (Object[] rowData : data) {
        Row row = sheet.createRow(rowNum++);
        row.createCell(0).setCellValue((Integer) rowData[0]);
        row.createCell(1).setCellValue((String) rowData[1]);
        row.createCell(2).setCellValue((String) rowData[2]);
        row.createCell(3).setCellValue((Integer) rowData[3]);
      }

      // Write to file
      try (FileOutputStream fos = new FileOutputStream(file)) {
        workbook.write(fos);
      }
    }

    assertTrue(file.exists(), "Test Excel file should exist");
    assertTrue(file.length() > 0, "Test Excel file should not be empty");
  }

  @Test public void testDuckDBWithExcelFile() throws Exception {
    // Skip test if not using DuckDB engine
    String engineType = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    if (!"DUCKDB".equals(engineType)) {
      return; // Skip test for non-DuckDB engines
    }

    // Create Excel file first
    File testExcelFile = new File(tempDir.toFile(), "test_data.xlsx");
    createTestExcelFile(testExcelFile);

    // Now create DuckDB connection - FileSchema will handle conversions
    // and DuckDB will use the converted Parquet files
    // Use ephemeralCache for proper test isolation
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + tempDir.toString().replace("\\", "\\\\") + "\",\n"
        + "        \"executionEngine\": \"DUCKDB\",\n"
        + "        \"ephemeralCache\": true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = CalciteAssert.that()
        .withModel(model)
        .connect()) {
      // Query the data through DuckDB
      try (Statement stmt = conn.createStatement()) {
        // The table name will be test_data__employees (Excel filename + sheet name)
        String query = "SELECT COUNT(*) as cnt FROM \"test_data__employees\"";

        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next(), "Should have result");
          assertEquals(5, rs.getInt("cnt"), "Should have 5 rows from Excel");
        }

        // Test a more complex query
        query = "SELECT \"department\", COUNT(*) as emp_count, AVG(\"salary\") as avg_salary "
              + "FROM \"test_data__employees\" "
              + "GROUP BY \"department\" "
              + "ORDER BY \"department\"";

        try (ResultSet rs = stmt.executeQuery(query)) {
          // Verify Engineering department
          assertTrue(rs.next());
          assertEquals("Engineering", rs.getString("department"));
          assertEquals(2, rs.getInt("emp_count"));
          assertEquals(100000.0, rs.getDouble("avg_salary"), 0.01);

          // Verify HR department
          assertTrue(rs.next());
          assertEquals("HR", rs.getString("department"));
          assertEquals(1, rs.getInt("emp_count"));
          assertEquals(65000.0, rs.getDouble("avg_salary"), 0.01);

          // Verify Sales department
          assertTrue(rs.next());
          assertEquals("Sales", rs.getString("department"));
          assertEquals(2, rs.getInt("emp_count"));
          assertEquals(77500.0, rs.getDouble("avg_salary"), 0.01);
        }
      }
    }
  }

  @Test public void testDuckDBWithMultipleSheets() throws Exception {
    // Skip test if not using DuckDB engine
    String engineType = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    if (!"DUCKDB".equals(engineType)) {
      return; // Skip test for non-DuckDB engines
    }

    // This test verifies DuckDB can handle Excel files with multiple sheets
    // Create Excel file with multiple sheets BEFORE connection
    File excelFile = new File(tempDir.toFile(), "multi_sheet.xlsx");

    // Create Excel file with multiple sheets
    try (Workbook workbook = new XSSFWorkbook()) {
      // First sheet - employees
      Sheet empSheet = workbook.createSheet("employees");
      Row empHeader = empSheet.createRow(0);
      empHeader.createCell(0).setCellValue("id");
      empHeader.createCell(1).setCellValue("name");
      empHeader.createCell(2).setCellValue("dept_id");

      Object[][] empData = {
          {1, "Alice", 10},
          {2, "Bob", 20},
          {3, "Charlie", 10}
      };

      int rowNum = 1;
      for (Object[] rowData : empData) {
        Row row = empSheet.createRow(rowNum++);
        row.createCell(0).setCellValue((Integer) rowData[0]);
        row.createCell(1).setCellValue((String) rowData[1]);
        row.createCell(2).setCellValue((Integer) rowData[2]);
      }

      // Second sheet - departments
      Sheet deptSheet = workbook.createSheet("departments");
      Row deptHeader = deptSheet.createRow(0);
      deptHeader.createCell(0).setCellValue("dept_id");
      deptHeader.createCell(1).setCellValue("dept_name");

      Object[][] deptData = {
          {10, "Engineering"},
          {20, "Sales"}
      };

      rowNum = 1;
      for (Object[] rowData : deptData) {
        Row row = deptSheet.createRow(rowNum++);
        row.createCell(0).setCellValue((Integer) rowData[0]);
        row.createCell(1).setCellValue((String) rowData[1]);
      }

      // Write to file
      try (FileOutputStream fos = new FileOutputStream(excelFile)) {
        workbook.write(fos);
      }
    }

    // Now create DuckDB connection
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + tempDir.toString().replace("\\", "\\\\") + "\",\n"
        + "        \"executionEngine\": \"DUCKDB\",\n"
        + "        \"ephemeralCache\": true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = CalciteAssert.that()
        .withModel(model)
        .connect()) {

      // Query data from both sheets
      try (Statement stmt = conn.createStatement()) {
        // Test employees table
        String query = "SELECT COUNT(*) as cnt FROM \"multi_sheet__employees\"";
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          assertEquals(3, rs.getInt("cnt"), "Should have 3 employees");
        }

        // Test departments table
        query = "SELECT COUNT(*) as cnt FROM \"multi_sheet__departments\"";
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          assertEquals(2, rs.getInt("cnt"), "Should have 2 departments");
        }

        // Test join between tables from different sheets
        query = "SELECT e.\"name\", d.\"dept_name\" " +
                "FROM \"multi_sheet__employees\" e " +
                "JOIN \"multi_sheet__departments\" d ON e.\"dept_id\" = d.\"dept_id\" " +
                "ORDER BY e.\"name\"";
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          assertEquals("Alice", rs.getString("name"));
          assertEquals("Engineering", rs.getString("dept_name"));

          assertTrue(rs.next());
          assertEquals("Bob", rs.getString("name"));
          assertEquals("Sales", rs.getString("dept_name"));

          assertTrue(rs.next());
          assertEquals("Charlie", rs.getString("name"));
          assertEquals("Engineering", rs.getString("dept_name"));

          assertFalse(rs.next());
        }
      }
    }
  }
}
