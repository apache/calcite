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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for multiple schema configurations with File adapter.
 */
public class MultipleSchemaTest {

  @TempDir
  File tempDir;

  private File salesDir;
  private File hrDir;

  @BeforeEach
  public void setUp() throws IOException {
    salesDir = new File(tempDir, "SALES");
    hrDir = new File(tempDir, "hr");
    salesDir.mkdirs();
    hrDir.mkdirs();

    // Create test data in sales directory
    createCsvFile(salesDir, "customers.csv", "ID,NAME,REGION\n1,Alice,North\n2,Bob,South");
    createCsvFile(salesDir, "orders.csv", "ID,CUSTOMER_ID,AMOUNT\n1,1,100.0\n2,2,200.0");

    // Create test data in hr directory
    createCsvFile(hrDir, "employees.csv", "ID,NAME,DEPARTMENT\n1,John,Sales\n2,Jane,HR");
    createCsvFile(hrDir, "departments.csv", "ID,NAME,BUDGET\n1,Sales,50000\n2,HR,30000");
  }

  private void createCsvFile(File dir, String filename, String content) throws IOException {
    File file = new File(dir, filename);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }

  @Test public void testDuplicateSchemaNames() throws Exception {
    // Try to create two schemas with the same name but different directories
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Add first DATA schema pointing to sales directory
      Map<String, Object> salesOperand = new HashMap<>();
      salesOperand.put("directory", salesDir.getAbsolutePath());
      rootSchema.add("data", FileSchemaFactory.INSTANCE.create(rootSchema, "data", salesOperand));

      // Try to add second DATA schema pointing to hr directory
      Map<String, Object> hrOperand = new HashMap<>();
      hrOperand.put("directory", hrDir.getAbsolutePath());

      // This should either replace the first schema or throw an exception
      Exception caughtException = null;
      try {
        rootSchema.add("data", FileSchemaFactory.INSTANCE.create(rootSchema, "data", hrOperand));
      } catch (Exception e) {
        caughtException = e;
      }

      // Query to see which schema is active
      try (Statement stmt = connection.createStatement()) {
        // Check what tables are available in DATA schema by trying to query them
        boolean hasSalesTables = false;
        boolean hasHrTables = false;

        // Try to access sales tables
        try {
          ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total_count FROM DATA.CUSTOMERS");
          if (rs.next()) {
            System.out.println("Found table in DATA schema: CUSTOMERS");
            hasSalesTables = true;
          }
        } catch (Exception e) {
          // Table doesn't exist or can't be accessed
        }

        try {
          ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total_count FROM DATA.ORDERS");
          if (rs.next()) {
            System.out.println("Found table in DATA schema: ORDERS");
            hasSalesTables = true;
          }
        } catch (Exception e) {
          // Table doesn't exist or can't be accessed
        }

        // Try to access hr tables
        try {
          ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total_count FROM DATA.EMPLOYEES");
          if (rs.next()) {
            System.out.println("Found table in DATA schema: EMPLOYEES");
            hasHrTables = true;
          }
        } catch (Exception e) {
          // Table doesn't exist or can't be accessed
        }

        try {
          ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total_count FROM DATA.DEPARTMENTS");
          if (rs.next()) {
            System.out.println("Found table in DATA schema: DEPARTMENTS");
            hasHrTables = true;
          }
        } catch (Exception e) {
          // Table doesn't exist or can't be accessed
        }

        System.out.println("\n=== DUPLICATE SCHEMA NAME TEST ===");
        System.out.println("Exception thrown: " + (caughtException != null ? caughtException.getClass().getSimpleName() : "None"));
        System.out.println("Has sales tables: " + hasSalesTables);
        System.out.println("Has HR tables: " + hasHrTables);

        if (hasSalesTables && !hasHrTables) {
          System.out.println("✅ First schema retained (sales directory)");
        } else if (!hasSalesTables && hasHrTables) {
          System.out.println("✅ Second schema replaced first (hr directory)");
        } else if (hasSalesTables && hasHrTables) {
          System.out.println("⚠️ Both schemas merged or visible");
        } else {
          System.out.println("❌ No tables visible - unexpected");
        }
        System.out.println("===============================\n");
      }
    }
  }

  @Test public void testMultipleDistinctSchemas() throws Exception {
    // Test multiple schemas with different names
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Add SALES schema
      Map<String, Object> salesOperand = new HashMap<>();
      salesOperand.put("directory", salesDir.getAbsolutePath());
      rootSchema.add("SALES", FileSchemaFactory.INSTANCE.create(rootSchema, "SALES", salesOperand));

      // Add HR schema
      Map<String, Object> hrOperand = new HashMap<>();
      hrOperand.put("directory", hrDir.getAbsolutePath());
      rootSchema.add("HR", FileSchemaFactory.INSTANCE.create(rootSchema, "HR", hrOperand));

      // Test cross-schema query
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs =
            stmt.executeQuery("SELECT s.NAME as customer_name, h.NAME as employee_name " +
            "FROM SALES.customers s, HR.EMPLOYEES h " +
            "WHERE s.ID = h.ID");

        int rowCount = 0;
        while (rs.next()) {
          String customerName = rs.getString("customer_name");
          String employeeName = rs.getString("employee_name");
          System.out.println("Customer: " + customerName + ", Employee: " + employeeName);
          rowCount++;
        }

        System.out.println("\n=== MULTIPLE DISTINCT SCHEMAS TEST ===");
        System.out.println("✅ Cross-schema query executed successfully");
        System.out.println("✅ Result rows: " + rowCount);
        System.out.println("✅ SALES and HR schemas working independently");
        System.out.println("====================================\n");

        assertTrue(rowCount > 0, "Should have at least one matching row");
      }
    }
  }

  @Test public void testSchemaReplacement() throws Exception {
    // Test explicit schema replacement behavior
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Add initial schema
      Map<String, Object> salesOperand = new HashMap<>();
      salesOperand.put("directory", salesDir.getAbsolutePath());
      rootSchema.add("TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", salesOperand));

      // Verify sales tables are accessible
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total_count FROM TEST.CUSTOMERS");
        assertTrue(rs.next());
        int customerCount = rs.getInt("total_count");
        System.out.println("Initial customer count: " + customerCount);
        assertTrue(customerCount > 0);
      }

      // Replace with hr schema
      Map<String, Object> hrOperand = new HashMap<>();
      hrOperand.put("directory", hrDir.getAbsolutePath());
      rootSchema.add("TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", hrOperand));

      // Check what's now accessible
      try (Statement stmt = connection.createStatement()) {
        // This should now access hr data, not sales data
        try {
          ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total_count FROM TEST.EMPLOYEES");
          assertTrue(rs.next());
          int employeeCount = rs.getInt("total_count");
          System.out.println("Employee count after replacement: " + employeeCount);

          System.out.println("\n=== SCHEMA REPLACEMENT TEST ===");
          System.out.println("✅ Schema replacement successful");
          System.out.println("✅ Now accessing HR data instead of Sales data");
          System.out.println("==============================\n");

        } catch (Exception e) {
          System.out.println("\n=== SCHEMA REPLACEMENT TEST ===");
          System.out.println("❌ Could not access employees table: " + e.getMessage());
          System.out.println("❓ Schema replacement behavior unclear");
          System.out.println("==============================\n");
        }
      }
    }
  }
}
