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
package org.apache.calcite.adapter.file.html;

import org.apache.calcite.adapter.file.BaseFileTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for multi-table HTML support.
 */
@Tag("unit")
public class MultiTableHtmlTest extends BaseFileTest {
  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    // HTML files will be created by individual tests as needed
  }

  private void createMultiTableHtmlFile() throws Exception {
    File htmlFile = new File(tempDir.toFile(), "multi_table_test.html");

    try (FileWriter writer = new FileWriter(htmlFile, StandardCharsets.UTF_8)) {
      writer.write("<!DOCTYPE html>\n");
      writer.write("<html>\n");
      writer.write("<head><title>Test Page</title></head>\n");
      writer.write("<body>\n");

      // Table 1 with id
      writer.write("<h2>Sales Report</h2>\n");
      writer.write("<table id=\"sales_data\">\n");
      writer.write("  <caption>Q1-Q2 Sales</caption>\n");
      writer.write("  <tr><th>Product</th><th>Q1</th><th>Q2</th></tr>\n");
      writer.write("  <tr><td>Widget</td><td>100</td><td>150</td></tr>\n");
      writer.write("  <tr><td>Gadget</td><td>200</td><td>250</td></tr>\n");
      writer.write("</table>\n");

      writer.write("<br><br>\n");

      // Table 2 without id, but with preceding heading
      writer.write("<h3>Employee Data</h3>\n");
      writer.write("<table>\n");
      writer.write("  <tr><th>Name</th><th>Department</th><th>Salary</th></tr>\n");
      writer.write("  <tr><td>John</td><td>Sales</td><td>50000</td></tr>\n");
      writer.write("  <tr><td>Jane</td><td>Engineering</td><td>75000</td></tr>\n");
      writer.write("</table>\n");

      writer.write("<br><br>\n");

      // Table 3 with only caption
      writer.write("<table>\n");
      writer.write("  <caption>Inventory Status</caption>\n");
      writer.write("  <tr><th>Item</th><th>Stock</th><th>Reorder</th></tr>\n");
      writer.write("  <tr><td>Pencils</td><td>500</td><td>No</td></tr>\n");
      writer.write("  <tr><td>Paper</td><td>50</td><td>Yes</td></tr>\n");
      writer.write("</table>\n");

      writer.write("</body>\n");
      writer.write("</html>\n");
    }
  }

  private void createComplexHtmlFile() throws Exception {
    File htmlFile = new File(tempDir.toFile(), "complex_tables.html");

    try (FileWriter writer = new FileWriter(htmlFile, StandardCharsets.UTF_8)) {
      writer.write("<!DOCTYPE html>\n");
      writer.write("<html>\n");
      writer.write("<head><title>Complex Tables</title></head>\n");
      writer.write("<body>\n");

      // Multiple tables with same heading
      writer.write("<h2>Financial Data</h2>\n");
      writer.write("<table id=\"revenue\">\n");
      writer.write("  <tr><th>Year</th><th>Revenue</th></tr>\n");
      writer.write("  <tr><td>2022</td><td>1000000</td></tr>\n");
      writer.write("  <tr><td>2023</td><td>1200000</td></tr>\n");
      writer.write("</table>\n");

      writer.write("<br>\n");

      writer.write("<h2>Financial Data</h2>\n");
      writer.write("<table id=\"expenses\">\n");
      writer.write("  <tr><th>Year</th><th>Expenses</th></tr>\n");
      writer.write("  <tr><td>2022</td><td>800000</td></tr>\n");
      writer.write("  <tr><td>2023</td><td>900000</td></tr>\n");
      writer.write("</table>\n");

      // Table without any identifier
      writer.write("<br><br>\n");
      writer.write("<table>\n");
      writer.write("  <tr><th>A</th><th>B</th></tr>\n");
      writer.write("  <tr><td>1</td><td>2</td></tr>\n");
      writer.write("</table>\n");

      writer.write("</body>\n");
      writer.write("</html>\n");
    }
  }

  @Test public void testMultiTableHtmlDetection() throws Exception {
    createMultiTableHtmlFile(); // Create only the file this test needs

    // Build model with ephemeralCache
    String model = buildTestModel("html", tempDir.toFile().getAbsolutePath());

    Properties info = new Properties();
    info.put("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {
      // Query the first table (identified by id)
      ResultSet rs1 =
          statement.executeQuery("SELECT * FROM html.multi_table_test__sales_data ORDER BY product");

      assertTrue(rs1.next());
      assertThat(rs1.getString("product"), is("Gadget"));
      assertThat(rs1.getString("q1"), is("200"));

      assertTrue(rs1.next());
      assertThat(rs1.getString("product"), is("Widget"));
      assertThat(rs1.getString("q1"), is("100"));

      // Query the second table (identified by heading)
      ResultSet rs2 =
          statement.executeQuery("SELECT * FROM html.multi_table_test__employee_data ORDER BY name");

      assertTrue(rs2.next());
      assertThat(rs2.getString("name"), is("Jane"));
      assertThat(rs2.getString("department"), is("Engineering"));

      assertTrue(rs2.next());
      assertThat(rs2.getString("name"), is("John"));
      assertThat(rs2.getString("department"), is("Sales"));

      // Query the third table (identified by caption)
      ResultSet rs3 =
          statement.executeQuery("SELECT * FROM html.multi_table_test__inventory_status ORDER BY item");

      assertTrue(rs3.next());
      assertThat(rs3.getString("item"), is("Paper"));
      assertThat(rs3.getString("reorder"), is("Yes"));

      assertTrue(rs3.next());
      assertThat(rs3.getString("item"), is("Pencils"));
      assertThat(rs3.getString("reorder"), is("No"));
    }
  }

  @Test public void testNonRecursiveDirectoryScan() throws Exception {
    createMultiTableHtmlFile(); // Create only the file this test needs
    createComplexHtmlFile(); // This test expects both files for the count

    // Build model with recursive=false and ephemeralCache
    String model =
        buildTestModel("html", tempDir.toFile().getAbsolutePath(), "recursive", "false");

    Properties info = new Properties();
    info.put("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {
      // HTML files should be processed even when recursive=false
      ResultSet tables = connection.getMetaData().getTables(null, "html", "%", null);
      int htmlTableCount = 0;
      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        if (tableName.contains("multi_table_test") || tableName.contains("complex_tables")) {
          htmlTableCount++;
        }
      }
      // HTML files are discovered regardless of recursive setting
      assertThat(htmlTableCount, is(6)); // 3 tables from each HTML file
    }
  }

  @Test public void testComplexHtmlFile() throws Exception {
    createComplexHtmlFile(); // Create only the complex HTML file for this test

    // Build model with ephemeralCache
    String model = buildTestModel("html", tempDir.toFile().getAbsolutePath());

    Properties info = new Properties();
    info.put("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {
      // Count tables found
      ResultSet tableList = connection.getMetaData().getTables(null, "html", "%", null);
      int tableCount = 0;
      System.out.println("Tables found in complex_tables.html:");
      while (tableList.next()) {
        String tableName = tableList.getString("TABLE_NAME");
        if (tableName.toLowerCase(Locale.ROOT).startsWith("complex_tables")) {
          tableCount++;
          System.out.println("  - " + tableName);
        }
      }
      // Should find 3 tables: revenue (id), expenses (id), and T3 (no identifier)
      assertThat(tableCount, is(3));

      // Query the revenue table
      ResultSet rs1 =
          statement.executeQuery("SELECT * FROM html.complex_tables__revenue WHERE \"year\" = 2023");
      assertTrue(rs1.next());
      assertThat(rs1.getString("revenue"), is("1200000"));

      // Query the expenses table
      ResultSet rs2 =
          statement.executeQuery("SELECT * FROM html.complex_tables__expenses WHERE \"year\" = 2023");
      assertTrue(rs2.next());
      assertThat(rs2.getString("expenses"), is("900000"));
    }
  }

  @Test public void testExplicitHtmlTableWithSelector() throws Exception {
    createMultiTableHtmlFile(); // Create the multi-table file for selector testing

    // Build model with explicit table definition and selector
    String model =
        buildTestModel("html", tempDir.toFile().getAbsolutePath(), "ephemeralCache", "true",
        "tables", "[{\"name\": \"sales_only\", \"url\": \"file://" +
            new File(tempDir.toFile(), "multi_table_test.html").getAbsolutePath().replace("\\", "\\\\") +
            "\", \"selector\": \"#sales_data\"}]");

    Properties info = new Properties();
    info.put("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Should only find the explicitly defined table
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) as cnt FROM html.sales_only");
      assertTrue(rs.next());
      assertThat(rs.getLong("cnt"), is(2L)); // 2 rows in sales table
    }
  }
}
