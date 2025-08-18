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
import org.apache.calcite.schema.Table;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for Excel file processing in FileSchema.
 */
@Tag("unit")
public class ExcelFileTest {

  @Test public void testExcelFileConversion(@TempDir Path tempDir) throws IOException {
    // Create a test Excel file with two sheets
    File excelFile = new File(tempDir.toFile(), "TestData.xlsx");
    createTestExcelFile(excelFile);

    // Create FileSchema pointing to the temp directory with engine configuration if set
    String engineType = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    ExecutionEngineConfig engineConfig = null;
    if (engineType != null && !engineType.isEmpty()) {
      engineConfig = new ExecutionEngineConfig(engineType, ExecutionEngineConfig.DEFAULT_BATCH_SIZE);
    }
    
    FileSchema schema = new FileSchema(null, "TEST", tempDir.toFile(),
        com.google.common.collect.ImmutableList.of(), engineConfig);

    // Get table map which should trigger Excel conversion
    Map<String, Table> tables = schema.getTableMap();

    // Verify that JSON files were created for each sheet
    File sheet1Json = new File(tempDir.toFile(), "TestData__Sheet1.json");
    File sheet2Json = new File(tempDir.toFile(), "TestData__Orders.json");

    assertTrue(sheet1Json.exists(), "Sheet1 JSON file should exist");
    assertTrue(sheet2Json.exists(), "Orders JSON file should exist");

    // Verify that tables were created for the converted files
    assertNotNull(tables.get("TESTDATA__SHEET1"), "Should have table for Sheet1");
    assertNotNull(tables.get("TESTDATA__ORDERS"), "Should have table for Orders");

    // Verify we can query the tables
    Table sheet1Table = tables.get("TESTDATA__SHEET1");
    assertNotNull(sheet1Table, "Sheet1 table should exist");
  }

  @Test public void testExcelFileWithSubdirectory(@TempDir Path tempDir) throws IOException {
    // Create a subdirectory
    File subDir = new File(tempDir.toFile(), "data");
    subDir.mkdir();

    // Create Excel file in subdirectory
    File excelFile = new File(subDir, "Sales.xlsx");
    createTestExcelFile(excelFile);

    // Create FileSchema with recursive=true to scan subdirectories and engine configuration if set
    String engineType = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig();
    if (engineType != null && !engineType.isEmpty()) {
      engineConfig = new ExecutionEngineConfig(engineType, ExecutionEngineConfig.DEFAULT_BATCH_SIZE);
    }
    
    FileSchema schema = new FileSchema(null, "TEST", tempDir.toFile(),
        com.google.common.collect.ImmutableList.of(), engineConfig, true);

    Map<String, Table> tables = schema.getTableMap();

    // Verify tables are created with correct names including subdirectory
    assertNotNull(tables.get("DATA_SALES__SHEET1"), "Should have table for data/Sales__Sheet1");
    assertNotNull(tables.get("DATA_SALES__ORDERS"), "Should have table for data/Sales__Orders");
  }

  @Test public void testExcelFileDirectoryProcessing(@TempDir Path tempDir) throws IOException {
    // Create test Excel file in directory (this is the supported approach)
    File excelFile = new File(tempDir.toFile(), "DirectTest.xlsx");
    createTestExcelFile(excelFile);

    // Use directory-based schema with engine configuration if set
    String engineType = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    ExecutionEngineConfig engineConfig = null;
    if (engineType != null && !engineType.isEmpty()) {
      engineConfig = new ExecutionEngineConfig(engineType, ExecutionEngineConfig.DEFAULT_BATCH_SIZE);
    }
    
    FileSchema schema = new FileSchema(null, "TEST", tempDir.toFile(), 
        com.google.common.collect.ImmutableList.of(), engineConfig);
    Map<String, Table> tableMap = schema.getTableMap();

    // After processing, the Excel file should be converted and tables created with proper names
    // Verify we have the expected tables (Excel sheets become separate tables)
    assertNotNull(tableMap.get("DIRECTTEST__SHEET1"), "Should have converted Sheet1 table");
    assertNotNull(tableMap.get("DIRECTTEST__ORDERS"), "Should have converted Orders table");
  }

  private void createTestExcelFile(File file) throws IOException {
    try (Workbook workbook = new XSSFWorkbook()) {
      // Create first sheet with sample data
      Sheet sheet1 = workbook.createSheet("Sheet1");
      Row header1 = sheet1.createRow(0);
      header1.createCell(0).setCellValue("id");
      header1.createCell(1).setCellValue("Name");
      header1.createCell(2).setCellValue("value");

      Row data1 = sheet1.createRow(1);
      data1.createCell(0).setCellValue(1);
      data1.createCell(1).setCellValue("Item A");
      data1.createCell(2).setCellValue(100.5);

      Row data2 = sheet1.createRow(2);
      data2.createCell(0).setCellValue(2);
      data2.createCell(1).setCellValue("Item B");
      data2.createCell(2).setCellValue(200.75);

      // Create second sheet
      Sheet sheet2 = workbook.createSheet("Orders");
      Row header2 = sheet2.createRow(0);
      header2.createCell(0).setCellValue("OrderID");
      header2.createCell(1).setCellValue("Customer");
      header2.createCell(2).setCellValue("amount");

      Row order1 = sheet2.createRow(1);
      order1.createCell(0).setCellValue("ORD001");
      order1.createCell(1).setCellValue("Customer A");
      order1.createCell(2).setCellValue(1500.0);

      // Write to file
      try (FileOutputStream out = new FileOutputStream(file)) {
        workbook.write(out);
      }
    }
  }
}
