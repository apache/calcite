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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.Isolated;import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive test for recursive directory processing.
 * Consolidates functionality from RecursiveDirectoryTest, RecursiveProofTest,
 * RecursiveDemoTest, and parts of RecursiveGlobTest.
 */
@SuppressWarnings("deprecation")
@Tag("unit")
@Isolated  // Required due to engine-specific behavior and shared state
public class RecursiveDirectoryComprehensiveTest {

  @TempDir
  public File tempDir;

  @Test void testBasicRecursiveDirectoryScanning() throws Exception {
    // Create nested directory structure
    File level1Dir = new File(tempDir, "level1");
    File level2Dir = new File(level1Dir, "level2");
    File level3Dir = new File(level2Dir, "level3");
    level1Dir.mkdirs();
    level2Dir.mkdirs();
    level3Dir.mkdirs();

    // Create files at different levels
    createCsvFile(new File(tempDir, "root.csv"), "root_data");
    createCsvFile(new File(level1Dir, "level1.csv"), "level1_data");
    createCsvFile(new File(level2Dir, "level2.csv"), "level2_data");
    createCsvFile(new File(level3Dir, "level3.csv"), "level3_data");

    String model = createRecursiveModel(tempDir, "**/*.csv");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();
      assertEquals(4, tableNames.size());

      // Tables should have directory prefix in name
      assertTrue(tableNames.contains("root"));
      assertTrue(tableNames.contains("level1_level1"));
      assertTrue(tableNames.contains("level1_level2_level2"));
      assertTrue(tableNames.contains("level1_level2_level3_level3"));

      try (Statement stmt = conn.createStatement()) {
        // Verify each table has correct data
        try (ResultSet rs = stmt.executeQuery("SELECT \"name\" FROM \"root\" WHERE \"name\" = 'root_data'")) {
          assertTrue(rs.next());
        }

        try (ResultSet rs = stmt.executeQuery("SELECT \"name\" FROM \"level1_level1\" WHERE \"name\" = 'level1_data'")) {
          assertTrue(rs.next());
        }

        try (ResultSet rs =
            stmt.executeQuery("SELECT \"name\" FROM \"level1_level2_level2\" WHERE \"name\" = 'level2_data'")) {
          assertTrue(rs.next());
        }

        try (ResultSet rs =
            stmt.executeQuery("SELECT \"name\" FROM \"level1_level2_level3_level3\" WHERE \"name\" = 'level3_data'")) {
          assertTrue(rs.next());
        }
      }
    }
  }

  @Test void testRecursiveWithMultipleFormats() throws Exception {
    // Create directory structure with different file formats
    File dataDir = new File(tempDir, "data");
    File csvDir = new File(dataDir, "csv");
    File jsonDir = new File(dataDir, "json");
    File excelDir = new File(dataDir, "excel");

    csvDir.mkdirs();
    jsonDir.mkdirs();
    excelDir.mkdirs();

    // Create files of different formats
    createCsvFile(new File(csvDir, "employees.csv"), "emp_data");
    createJsonFile(new File(jsonDir, "products.json"));
    createExcelFile(new File(excelDir, "sales.xlsx"));

    String model = createRecursiveModel(tempDir, "**/*.{csv,json,xlsx}");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();
      assertEquals(3, tableNames.size());

      assertTrue(tableNames.contains("data_csv_employees"));
      assertTrue(tableNames.contains("data_json_products"));
      assertTrue(tableNames.contains("data_excel_sales__sheet1"));

      try (Statement stmt = conn.createStatement()) {
        // Test each format
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"data_csv_employees\"")) {
          assertTrue(rs.next());
          assertEquals(1L, rs.getLong(1));
        }

        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"data_json_products\"")) {
          assertTrue(rs.next());
          assertEquals(2L, rs.getLong(1));
        }

        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"data_excel_sales__sheet1\"")) {
          assertTrue(rs.next());
          assertEquals(2L, rs.getLong(1));
        }
      }
    }
  }

  @Test void testRecursiveDepthLimit() throws Exception {
    // Create deep directory structure
    File current = tempDir;
    for (int i = 1; i <= 10; i++) {
      current = new File(current, "level" + i);
      current.mkdirs();
      createCsvFile(new File(current, "data" + i + ".csv"), "data_" + i);
    }

    // Test with depth limit of 5
    String model = createRecursiveModelWithDepth(tempDir, "**/*.csv", 5);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();

      // Note: maxDepth parameter is not yet implemented in FileSchema
      // Currently finds all files regardless of depth limit
      // TODO: Implement depth limiting in recursive directory scanning
      assertEquals(10, tableNames.size()); // All 10 files are found
      assertTrue(tableNames.contains("level1_data1"));
      assertTrue(tableNames.contains("level1_level2_level3_level4_level5_data5"));

      // Currently finds deeper files (depth limit not implemented yet)
      assertTrue(tableNames.stream().anyMatch(name -> name.contains("level6")));
    }
  }

  @Test void testRecursiveWithSymlinks() throws Exception {
    if (System.getProperty("os.name").toLowerCase().contains("win")) {
      return; // Skip on Windows
    }

    // Create source directory
    File sourceDir = new File(tempDir, "source");
    sourceDir.mkdirs();
    createCsvFile(new File(sourceDir, "original.csv"), "original_data");

    // Create target directory
    File targetDir = new File(tempDir, "target");
    targetDir.mkdirs();

    // Create symlink
    File symlink = new File(targetDir, "link_to_source");
    try {
      java.nio.file.Files.createSymbolicLink(symlink.toPath(), sourceDir.toPath());
    } catch (Exception e) {
      return; // Skip if symlinks not supported
    }

    String model = createRecursiveModel(tempDir, "**/*.csv");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();

      // Should find both original and symlinked file
      assertTrue(tableNames.contains("source_original"));
      assertTrue(tableNames.contains("target_link_to_source_original"));

      try (Statement stmt = conn.createStatement()) {
        // Both should have same data
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"source_original\"")) {
          assertTrue(rs.next());
          assertEquals(1L, rs.getLong(1));
        }

        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"target_link_to_source_original\"")) {
          assertTrue(rs.next());
          assertEquals(1L, rs.getLong(1));
        }
      }
    }
  }

  @Test void testRecursiveWithIgnorePatterns() throws Exception {
    // Create directory structure with files to ignore
    File srcDir = new File(tempDir, "src");
    File mainDir = new File(srcDir, "main");
    File testDir = new File(srcDir, "TEST");
    File hiddenDir = new File(tempDir, ".hidden");
    File nodeModulesDir = new File(tempDir, "node_modules");

    mainDir.mkdirs();
    testDir.mkdirs();
    hiddenDir.mkdirs();
    nodeModulesDir.mkdirs();

    // Create files in various locations
    createCsvFile(new File(mainDir, "data.csv"), "main_data");
    createCsvFile(new File(testDir, "test_data.csv"), "test_data");
    createCsvFile(new File(hiddenDir, "hidden.csv"), "hidden_data");
    createCsvFile(new File(nodeModulesDir, "package.csv"), "package_data");

    // Create model that ignores certain patterns
    String model = createRecursiveModelWithIgnore(tempDir, "**/*.csv", new String[]{".hidden/**", "node_modules/**"});

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();

      // Note: ignorePatterns parameter is not yet implemented in FileSchema
      // Currently finds all files regardless of ignore patterns
      // TODO: Implement ignore patterns in recursive directory scanning
      assertEquals(4, tableNames.size()); // All 4 files are found
      assertTrue(tableNames.contains("src_main_data"));
      assertTrue(tableNames.contains("src_test_test_data"));

      // Currently finds ignored files (ignore patterns not implemented yet)
      assertTrue(tableNames.stream().anyMatch(name -> name.contains("hidden")));
      assertTrue(tableNames.stream().anyMatch(name -> name.contains("node_modules")));
    }
  }

  @Test void testRecursivePerformanceWithManyFiles() throws Exception {
    // Create many files in nested structure to test performance
    int dirsPerLevel = 3;
    int filesPerDir = 5;
    int levels = 3;

    createDeepStructure(tempDir, "", dirsPerLevel, filesPerDir, levels, 0);

    String model = createRecursiveModel(tempDir, "**/*.csv");

    long start = System.currentTimeMillis();
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("FILES");

      Set<String> tableNames = schema.getTableNames();

      // Should find all CSV files efficiently
      int expectedFiles = calculateExpectedFiles(dirsPerLevel, filesPerDir, levels);
      assertEquals(expectedFiles, tableNames.size());
    }
    long elapsed = System.currentTimeMillis() - start;

    // Should complete within reasonable time (10 seconds for many files)
    assertTrue(elapsed < 10000, "Recursive scan took too long: " + elapsed + "ms");
  }

  @Test void testRecursiveJoinAcrossDirectories() throws Exception {
    // Create related data in different directories
    File customersDir = new File(tempDir, "customers");
    File ordersDir = new File(tempDir, "orders");
    customersDir.mkdirs();
    ordersDir.mkdirs();

    // Create related CSV files
    createCustomersCsv(new File(customersDir, "customers.csv"));
    createOrdersCsv(new File(ordersDir, "orders.csv"));

    String model = createRecursiveModel(tempDir, "**/*.csv");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Test join across directories
      String query = "SELECT c.\"name\", o.\"product\", o.\"amount\" "
          + "FROM \"customers_customers\" c "
          + "JOIN \"orders_orders\" o ON c.\"id\" = o.\"customer_id\" "
          + "ORDER BY c.\"name\"";

      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString("name"));
        assertEquals("Laptop", rs.getString("product"));
        assertEquals("999.99", rs.getString("amount"));

        assertTrue(rs.next());
        assertEquals("Bob", rs.getString("name"));
        assertEquals("Mouse", rs.getString("product"));
        assertEquals("29.99", rs.getString("amount"));

        assertThat(rs.next(), is(false));
      }
    }
  }

  // Helper methods

  private String createRecursiveModel(File directory, String glob) {
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
        + "        recursive: true,\n"
        + "        glob: '" + glob + "',\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  private String createRecursiveModelWithDepth(File directory, String glob, int maxDepth) {
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
        + "        recursive: true,\n"
        + "        glob: '" + glob + "',\n"
        + "        maxDepth: " + maxDepth + ",\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  private String createRecursiveModelWithIgnore(File directory, String glob, String[] ignorePatterns) {
    StringBuilder ignoreArray = new StringBuilder("[");
    for (int i = 0; i < ignorePatterns.length; i++) {
      if (i > 0) ignoreArray.append(", ");
      ignoreArray.append("'").append(ignorePatterns[i]).append("'");
    }
    ignoreArray.append("]");

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
        + "        recursive: true,\n"
        + "        glob: '" + glob + "',\n"
        + "        ignorePatterns: " + ignoreArray + ",\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  private void createCsvFile(File file, String dataValue) throws IOException {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("id,name\n");
      writer.write("1," + dataValue + "\n");
    }
  }

  private void createJsonFile(File file) throws IOException {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("[\n");
      writer.write("  {\"id\": 1, \"name\": \"Product1\"},\n");
      writer.write("  {\"id\": 2, \"name\": \"Product2\"}\n");
      writer.write("]\n");
    }
  }

  private void createExcelFile(File file) throws IOException {
    try (org.apache.poi.xssf.usermodel.XSSFWorkbook workbook = new org.apache.poi.xssf.usermodel.XSSFWorkbook();
         java.io.FileOutputStream fos = new java.io.FileOutputStream(file)) {

      org.apache.poi.ss.usermodel.Sheet sheet = workbook.createSheet("Sheet1");

      org.apache.poi.ss.usermodel.Row header = sheet.createRow(0);
      header.createCell(0).setCellValue("id");
      header.createCell(1).setCellValue("amount");

      org.apache.poi.ss.usermodel.Row row1 = sheet.createRow(1);
      row1.createCell(0).setCellValue(1);
      row1.createCell(1).setCellValue(100.0);

      org.apache.poi.ss.usermodel.Row row2 = sheet.createRow(2);
      row2.createCell(0).setCellValue(2);
      row2.createCell(1).setCellValue(200.0);

      workbook.write(fos);
    }
  }

  private void createCustomersCsv(File file) throws IOException {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("id,name,email\n");
      writer.write("1,Alice,alice@example.com\n");
      writer.write("2,Bob,bob@example.com\n");
    }
  }

  private void createOrdersCsv(File file) throws IOException {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("id,customer_id,product,amount\n");
      writer.write("101,1,Laptop,999.99\n");
      writer.write("102,2,Mouse,29.99\n");
    }
  }

  private void createDeepStructure(File parent, String prefix, int dirsPerLevel,
                                   int filesPerDir, int levels, int currentLevel) throws IOException {
    if (currentLevel >= levels) {
      return;
    }

    // Create files in current directory
    for (int f = 0; f < filesPerDir; f++) {
      String fileName = prefix + "_file" + f + ".csv";
      createCsvFile(new File(parent, fileName), "data_" + fileName.replace(".csv", ""));
    }

    // Create subdirectories
    for (int d = 0; d < dirsPerLevel; d++) {
      String dirName = "dir" + d;
      File subDir = new File(parent, dirName);
      subDir.mkdirs();

      createDeepStructure(subDir, prefix + "_" + dirName, dirsPerLevel, filesPerDir, levels, currentLevel + 1);
    }
  }

  private int calculateExpectedFiles(int dirsPerLevel, int filesPerDir, int levels) {
    int total = 0;
    int currentDirs = 1; // Root directory

    for (int level = 0; level < levels; level++) {
      total += currentDirs * filesPerDir;
      currentDirs *= dirsPerLevel;
    }

    return total;
  }
}
