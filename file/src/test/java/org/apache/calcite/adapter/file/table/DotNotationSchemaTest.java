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
 * Tests for dot notation in schema names to simulate hierarchy.
 */
public class DotNotationSchemaTest {

  @TempDir
  File tempDir;

  private File salesDir;
  private File hrDir;
  private File financeDir;

  @BeforeEach
  public void setUp() throws IOException {
    salesDir = new File(tempDir, "sales");
    hrDir = new File(tempDir, "hr");
    financeDir = new File(tempDir, "finance");
    salesDir.mkdirs();
    hrDir.mkdirs();
    financeDir.mkdirs();

    // Create test data
    createCsvFile(salesDir, "customers.csv", "id,name,region\n1,Alice,North\n2,Bob,South");
    createCsvFile(hrDir, "employees.csv", "id,name,department\n1,John,Sales\n2,Jane,HR");
    createCsvFile(financeDir, "budgets.csv", "department,amount\nSales,50000\nHR,30000");
  }

  private void createCsvFile(File dir, String filename, String content) throws IOException {
    File file = new File(dir, filename);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }

  @Test public void testDotNotationInSchemaNames() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Try dot notation in schema names
      Map<String, Object> salesOperand = new HashMap<>();
      salesOperand.put("directory", salesDir.getAbsolutePath());
      rootSchema.add("COMPANY.SALES", FileSchemaFactory.INSTANCE.create(rootSchema, "COMPANY.SALES", salesOperand));

      Map<String, Object> hrOperand = new HashMap<>();
      hrOperand.put("directory", hrDir.getAbsolutePath());
      rootSchema.add("COMPANY.HR", FileSchemaFactory.INSTANCE.create(rootSchema, "COMPANY.HR", hrOperand));

      Map<String, Object> financeOperand = new HashMap<>();
      financeOperand.put("directory", financeDir.getAbsolutePath());
      rootSchema.add("COMPANY.FINANCE", FileSchemaFactory.INSTANCE.create(rootSchema, "COMPANY.FINANCE", financeOperand));

      System.out.println("\n=== DOT NOTATION SCHEMA TEST ===");

      // Test if we can query using dot notation
      try (Statement stmt = connection.createStatement()) {

        // Test 1: Direct dot notation access
        try {
          ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM \"COMPANY.SALES\".customers");
          if (rs.next()) {
            int count = rs.getInt("count");
            System.out.println("✅ Dot notation query successful: " + count + " customers");
          }
        } catch (Exception e) {
          System.out.println("❌ Dot notation query failed: " + e.getMessage());
        }

        // Test 2: Cross-schema query with dot notation
        try {
          ResultSet rs =
              stmt.executeQuery("SELECT s.\"name\" as customer, h.\"name\" as employee " +
              "FROM \"COMPANY.SALES\".customers s, \"COMPANY.HR\".employees h " +
              "WHERE s.\"id\" = h.\"id\"");

          int rowCount = 0;
          while (rs.next()) {
            String customer = rs.getString("customer");
            String employee = rs.getString("employee");
            System.out.println("✅ Cross-schema result: " + customer + " <-> " + employee);
            rowCount++;
          }

          if (rowCount > 0) {
            System.out.println("✅ Cross-schema dot notation queries work!");
          }

        } catch (Exception e) {
          System.out.println("❌ Cross-schema dot notation failed: " + e.getMessage());
        }

        // Test 3: List available schemas to see how they appear
        try {
          ResultSet rs = stmt.executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA");
          System.out.println("Available schemas:");
          while (rs.next()) {
            String schemaName = rs.getString("SCHEMA_NAME");
            System.out.println("  - " + schemaName);
          }
        } catch (Exception e) {
          System.out.println("Could not list schemas: " + e.getMessage());
        }
      }

      System.out.println("==============================\n");
    }
  }

  @Test public void testUnderscoreVsDotNotation() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Compare different naming strategies
      Map<String, Object> salesOperand = new HashMap<>();
      salesOperand.put("directory", salesDir.getAbsolutePath());

      Map<String, Object> hrOperand = new HashMap<>();
      hrOperand.put("directory", hrDir.getAbsolutePath());

      // Strategy 1: Dot notation
      rootSchema.add("COMPANY.SALES", FileSchemaFactory.INSTANCE.create(rootSchema, "COMPANY.SALES", salesOperand));

      // Strategy 2: Underscore notation
      rootSchema.add("COMPANY_HR", FileSchemaFactory.INSTANCE.create(rootSchema, "COMPANY_HR", hrOperand));

      System.out.println("\n=== NAMING STRATEGY COMPARISON ===");

      try (Statement stmt = connection.createStatement()) {

        // Test dot notation
        try {
          ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM \"COMPANY.SALES\".customers");
          if (rs.next()) {
            System.out.println("✅ Dot notation (COMPANY.SALES): " + rs.getInt("count") + " records");
          }
        } catch (Exception e) {
          System.out.println("❌ Dot notation failed: " + e.getMessage());
        }

        // Test underscore notation
        try {
          ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM COMPANY_HR.employees");
          if (rs.next()) {
            System.out.println("✅ Underscore notation (COMPANY_HR): " + rs.getInt("count") + " records");
          }
        } catch (Exception e) {
          System.out.println("❌ Underscore notation failed: " + e.getMessage());
        }

        // Test mixed query
        try {
          ResultSet rs =
              stmt.executeQuery("SELECT s.\"region\", h.\"department\" " +
              "FROM \"COMPANY.SALES\".customers s, COMPANY_HR.employees h " +
              "WHERE s.\"id\" = h.\"id\"");

          if (rs.next()) {
            System.out.println("✅ Mixed notation query works: " + rs.getString("region") + " / " + rs.getString("department"));
          }

        } catch (Exception e) {
          System.out.println("❌ Mixed notation query failed: " + e.getMessage());
        }
      }

      System.out.println("================================\n");
    }
  }

  @Test public void testHierarchicalSchemaSimulation() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Create hierarchical-looking schema names
      Map<String, Object> salesOperand = new HashMap<>();
      salesOperand.put("directory", salesDir.getAbsolutePath());

      Map<String, Object> hrOperand = new HashMap<>();
      hrOperand.put("directory", hrDir.getAbsolutePath());

      Map<String, Object> financeOperand = new HashMap<>();
      financeOperand.put("directory", financeDir.getAbsolutePath());

      // Simulate nested hierarchy with dot notation
      rootSchema.add("ORG.DEPT.SALES", FileSchemaFactory.INSTANCE.create(rootSchema, "ORG.DEPT.SALES", salesOperand));
      rootSchema.add("ORG.DEPT.HR", FileSchemaFactory.INSTANCE.create(rootSchema, "ORG.DEPT.HR", hrOperand));
      rootSchema.add("ORG.FINANCE", FileSchemaFactory.INSTANCE.create(rootSchema, "ORG.FINANCE", financeOperand));

      System.out.println("\n=== HIERARCHICAL SCHEMA SIMULATION ===");

      try (Statement stmt = connection.createStatement()) {

        // Test different "levels" of hierarchy
        String[] testQueries = {
            "SELECT 'SALES' as dept, COUNT(*) as count FROM \"ORG.DEPT.SALES\".customers",
            "SELECT 'HR' as dept, COUNT(*) as count FROM \"ORG.DEPT.HR\".employees",
            "SELECT 'FINANCE' as dept, COUNT(*) as count FROM \"ORG.FINANCE\".budgets"
        };

        for (String query : testQueries) {
          try {
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
              String dept = rs.getString("dept");
              int count = rs.getInt("count");
              System.out.println("✅ " + dept + " department: " + count + " records");
            }
          } catch (Exception e) {
            System.out.println("❌ Query failed: " + e.getMessage());
          }
        }

        // Test "cross-hierarchy" join
        try {
          ResultSet rs =
              stmt.executeQuery("SELECT s.\"region\", f.\"amount\" " +
              "FROM \"ORG.DEPT.SALES\".customers s, \"ORG.FINANCE\".budgets f " +
              "WHERE f.\"department\" = 'Sales'");

          while (rs.next()) {
            String region = rs.getString("region");
            int amount = rs.getInt("amount");
            System.out.println("✅ Cross-hierarchy join: " + region + " region, budget: $" + amount);
          }

        } catch (Exception e) {
          System.out.println("❌ Cross-hierarchy join failed: " + e.getMessage());
        }
      }

      System.out.println("====================================\n");
    }
  }
}
