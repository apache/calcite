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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.BeforeEach;
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
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for multi-table HTML support.
 */
public class MultiTableHtmlTest {
  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    // Create test HTML files with multiple tables
    createMultiTableHtmlFile();
    createComplexHtmlFile();
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
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      FileSchema fileSchema =
          new FileSchema(rootSchema, "html", tempDir.toFile(), null, new ExecutionEngineConfig(), false, null, null);
      rootSchema.add("html", fileSchema);

      // Force table discovery
      Map<String, Table> tables = fileSchema.getTableMap();

      // Debug: List all discovered tables
      System.out.println("Discovered tables:");
      for (String tableName : tables.keySet()) {
        System.out.println("  - " + tableName);
      }

      try (Statement statement = connection.createStatement()) {
        // Query the first table (identified by id)
        ResultSet rs1 =
            statement.executeQuery("SELECT * FROM \"html\".\"MULTI_TABLE_TEST__SALES_DATA\" ORDER BY \"Product\"");

        assertTrue(rs1.next());
        assertThat(rs1.getString("Product"), is("Gadget"));
        assertThat(rs1.getString("Q1"), is("200"));

        assertTrue(rs1.next());
        assertThat(rs1.getString("Product"), is("Widget"));
        assertThat(rs1.getString("Q1"), is("100"));

        // Query the second table (identified by heading)
        ResultSet rs2 =
            statement.executeQuery("SELECT * FROM \"html\".\"MULTI_TABLE_TEST__EMPLOYEE_DATA\" ORDER BY \"Name\"");

        assertTrue(rs2.next());
        assertThat(rs2.getString("Name"), is("Jane"));
        assertThat(rs2.getString("Department"), is("Engineering"));

        assertTrue(rs2.next());
        assertThat(rs2.getString("Name"), is("John"));
        assertThat(rs2.getString("Department"), is("Sales"));

        // Query the third table (identified by caption)
        ResultSet rs3 =
            statement.executeQuery("SELECT * FROM \"html\".\"MULTI_TABLE_TEST__INVENTORY_STATUS\" ORDER BY \"Item\"");

        assertTrue(rs3.next());
        assertThat(rs3.getString("Item"), is("Paper"));
        assertThat(rs3.getString("Reorder"), is("Yes"));

        assertTrue(rs3.next());
        assertThat(rs3.getString("Item"), is("Pencils"));
        assertThat(rs3.getString("Reorder"), is("No"));
      }
    }
  }

  @Test public void testSingleTableHtmlMode() throws Exception {
    // Test that without multiTableHtml flag, HTML files are not processed in directory scan
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      rootSchema.add("html",
          new FileSchema(rootSchema, "html", tempDir.toFile(), null,
              new ExecutionEngineConfig(), false, null, null));

      try (Statement statement = connection.createStatement()) {
        // Should not find any HTML tables in directory scan mode
        ResultSet tables = connection.getMetaData().getTables(null, "html", "%", null);
        int htmlTableCount = 0;
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          if (tableName.contains("multi_table_test") || tableName.contains("complex_tables")) {
            htmlTableCount++;
          }
        }
        // In single table mode without explicit definitions, HTML files are not discovered
        assertThat(htmlTableCount, is(0));
      }
    }
  }

  @Test public void testComplexHtmlFile() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      FileSchema fileSchema =
          new FileSchema(rootSchema, "html", tempDir.toFile(), null, new ExecutionEngineConfig(), true, null, null);
      rootSchema.add("html", fileSchema);

      // Force table discovery
      Map<String, Table> tables = fileSchema.getTableMap();

      try (Statement statement = connection.createStatement()) {
        // Count tables found
        ResultSet tableList = connection.getMetaData().getTables(null, "html", "%", null);
        int tableCount = 0;
        System.out.println("Tables found in complex_tables.html:");
        while (tableList.next()) {
          String tableName = tableList.getString("TABLE_NAME");
          if (tableName.toUpperCase(Locale.ROOT).startsWith("COMPLEX_TABLES")) {
            tableCount++;
            System.out.println("  - " + tableName);
          }
        }
        // Should find 3 tables: revenue (id), expenses (id), and T3 (no identifier)
        assertThat(tableCount, is(3));

        // Query the revenue table
        ResultSet rs1 =
            statement.executeQuery("SELECT * FROM \"html\".\"COMPLEX_TABLES__REVENUE\" WHERE \"Year\" = '2023'");
        assertTrue(rs1.next());
        assertThat(rs1.getString("Revenue"), is("1200000"));

        // Query the expenses table
        ResultSet rs2 =
            statement.executeQuery("SELECT * FROM \"html\".\"COMPLEX_TABLES__EXPENSES\" WHERE \"Year\" = '2023'");
        assertTrue(rs2.next());
        assertThat(rs2.getString("Expenses"), is("900000"));
      }
    }
  }

  @Test public void testExplicitHtmlTableWithSelector() throws Exception {
    // Test explicit table definition with selector
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Create schema with explicit table definition
      java.util.List<java.util.Map<String, Object>> tableDefs =
          java.util.Collections.singletonList(
              java.util.Map.of(
                  "name", "SALES_ONLY",
                  "url", "file://" + new File(tempDir.toFile(), "multi_table_test.html").getAbsolutePath(),
                  "selector", "#sales_data"));

      rootSchema.add("HTML",
          new FileSchema(rootSchema, "HTML", null, tableDefs,
              new ExecutionEngineConfig(), false, null, null));

      try (Statement statement = connection.createStatement()) {
        // Should only find the explicitly defined table
        ResultSet rs =
            statement.executeQuery("SELECT COUNT(*) as cnt FROM HTML.SALES_ONLY");
        assertTrue(rs.next());
        assertThat(rs.getLong("cnt"), is(2L)); // 2 rows in sales table
      }
    }
  }
}
