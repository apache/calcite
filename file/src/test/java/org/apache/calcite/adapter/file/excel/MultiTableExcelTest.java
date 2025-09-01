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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.test.CalciteAssert;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for multi-table Excel detection feature.
 */
@Tag("unit")
public class MultiTableExcelTest extends BaseFileTest {
  private File tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    // Create temporary directory manually
    tempDir = Files.createTempDirectory("excel-test").toFile();

    // Create a test Excel file with multiple tables
    createMultiTableExcelFile();
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
    if (directory.exists()) {
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
    }
  }

  private void createMultiTableExcelFile() throws Exception {
    File excelFile = new File(tempDir, "multi_table_test.xlsx");

    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("Data");

      // Table 1: Sales Report (with identifier)
      Row identifierRow = sheet.createRow(0);
      Cell identifierCell = identifierRow.createCell(0);
      identifierCell.setCellValue("Sales Report");

      // Headers for table 1
      Row header1 = sheet.createRow(2);
      header1.createCell(0).setCellValue("product");
      header1.createCell(1).setCellValue("Q1");
      header1.createCell(2).setCellValue("Q2");

      // Data for table 1
      Row data1 = sheet.createRow(3);
      data1.createCell(0).setCellValue("Widget");
      data1.createCell(1).setCellValue(100);
      data1.createCell(2).setCellValue(150);

      Row data2 = sheet.createRow(4);
      data2.createCell(0).setCellValue("Gadget");
      data2.createCell(1).setCellValue(200);
      data2.createCell(2).setCellValue(250);

      // Empty rows
      sheet.createRow(5);
      sheet.createRow(6);

      // Table 2: Employee Data
      Row identifier2 = sheet.createRow(7);
      identifier2.createCell(0).setCellValue("Employee Data");

      Row header2 = sheet.createRow(9);
      header2.createCell(0).setCellValue("Name");
      header2.createCell(1).setCellValue("department");
      header2.createCell(2).setCellValue("Salary");

      Row emp1 = sheet.createRow(10);
      emp1.createCell(0).setCellValue("John");
      emp1.createCell(1).setCellValue("Sales");
      emp1.createCell(2).setCellValue(50000);

      Row emp2 = sheet.createRow(11);
      emp2.createCell(0).setCellValue("Jane");
      emp2.createCell(1).setCellValue("Engineering");
      emp2.createCell(2).setCellValue(75000);

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }
  }

  @Test public void testMultiTableExcelDetection() throws Exception {
    String model = buildTestModel("excel", tempDir.getAbsolutePath(), "multiTableExcel", "true");

    CalciteAssert.model(model)
        .with(Lex.ORACLE)
        .with(CalciteConnectionProperty.UNQUOTED_CASING, Casing.TO_LOWER)
        .query("SELECT * FROM \"excel\".\"multi_table_test__data_sales_report\" ORDER BY product")
        .returnsUnordered("product=Gadget; q1=200; q2=250",
                         "product=Widget; q1=100; q2=150");

    CalciteAssert.model(model)
        .with(Lex.ORACLE)
        .with(CalciteConnectionProperty.UNQUOTED_CASING, Casing.TO_LOWER)
        .query("SELECT * FROM \"excel\".\"multi_table_test__data_employee_data\" ORDER BY name")
        .returnsUnordered("name=Jane; department=Engineering; salary=75000",
                         "name=John; department=Sales; salary=50000");
  }


  @Test public void testComplexExcelFile() throws Exception {
    // Copy the lots_of_tables.xlsx file to temp directory
    File targetFile = new File(tempDir, "lots_of_tables.xlsx");
    try (InputStream in = getClass().getResourceAsStream("/lots_of_tables.xlsx")) {
      if (in != null) {
        Files.copy(in, targetFile.toPath());
      }
    }

    String model = buildTestModel("excel", tempDir.getAbsolutePath(), "multiTableExcel", "true");

    // Test that multiple tables are detected
    CalciteAssert.model(model)
        .with(Lex.ORACLE)
        .with(CalciteConnectionProperty.UNQUOTED_CASING, Casing.TO_LOWER)
        .doWithConnection(connection -> {
          try {
            // Count tables found
            ResultSet tables = connection.getMetaData().getTables(null, "excel", "%", null);
            int tableCount = 0;
            while (tables.next()) {
              String tableName = tables.getString("TABLE_NAME");
              if (tableName.startsWith("lots_of_tables")) {
                tableCount++;
              }
            }
            // Should find multiple tables
            assertTrue(tableCount > 1, "Should find multiple tables in lots_of_tables.xlsx, found: " + tableCount);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }
}
