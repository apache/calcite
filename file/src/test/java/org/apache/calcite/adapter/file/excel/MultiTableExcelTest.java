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

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for multi-table Excel detection feature.
 */
@Tag("unit")
public class MultiTableExcelTest {
  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    // Create a test Excel file with multiple tables
    createMultiTableExcelFile();
  }

  private void createMultiTableExcelFile() throws Exception {
    File excelFile = new File(tempDir.toFile(), "multi_table_test.xlsx");

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
    Properties props = new Properties();
    props.setProperty("multiTableExcel", "true");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      rootSchema.add("excel",
          new FileSchema(rootSchema, "excel", tempDir.toFile(), null,
              new ExecutionEngineConfig(), false, null, null));

      try (Statement statement = connection.createStatement()) {
        // Query the first table
        ResultSet rs1 =
            statement.executeQuery("SELECT * FROM \"excel\".\"MULTI_TABLE_TEST__DATA_SALES_REPORT\" "
            + "ORDER BY \"product\"");

        assertTrue(rs1.next());
        assertThat(rs1.getString("product"), is("Gadget"));
        assertThat((int) Double.parseDouble(rs1.getString("q1")), is(200));

        assertTrue(rs1.next());
        assertThat(rs1.getString("product"), is("Widget"));
        assertThat((int) Double.parseDouble(rs1.getString("q1")), is(100));

        // Query the second table
        ResultSet rs2 =
            statement.executeQuery("SELECT * FROM \"excel\".\"MULTI_TABLE_TEST__DATA_EMPLOYEE_DATA\" "
            + "ORDER BY \"name\"");

        assertTrue(rs2.next());
        assertThat(rs2.getString("name"), is("Jane"));
        assertThat(rs2.getString("department"), is("Engineering"));

        assertTrue(rs2.next());
        assertThat(rs2.getString("name"), is("John"));
        assertThat(rs2.getString("department"), is("Sales"));
      }
    }
  }


  @Test public void testComplexExcelFile() throws Exception {
    // Copy the lots_of_tables.xlsx file to temp directory
    File targetFile = new File(tempDir.toFile(), "lots_of_tables.xlsx");
    try (InputStream in = getClass().getResourceAsStream("/lots_of_tables.xlsx")) {
      if (in != null) {
        Files.copy(in, targetFile.toPath());
      }
    }

    // Test with multi-table detection enabled
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      rootSchema.add("excel",
          new FileSchema(rootSchema, "excel", tempDir.toFile(), null,
              new ExecutionEngineConfig(), false, null, null));

      try (Statement statement = connection.createStatement()) {
        // Count tables found
        ResultSet tables = connection.getMetaData().getTables(null, "excel", "%", null);
        int tableCount = 0;
        System.out.println("Tables from lots_of_tables.xlsx:");
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          if (tableName.startsWith("LOTS_OF_TABLES")) {
            tableCount++;
            System.out.println("  - " + tableName);
          }
        }
        // Should find multiple tables
        assertTrue(tableCount > 1, "Should find multiple tables in lots_of_tables.xlsx");
        System.out.println("Total tables found: " + tableCount);
      }
    }
  }
}
