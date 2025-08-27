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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.file.FileSchemaFactory;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
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
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for multiple schema configurations with File adapter.
 */
@Tag("unit")
public class MultipleSchemaTest extends org.apache.calcite.adapter.file.BaseFileTest {

  @TempDir
  File tempDir;

  private File salesDir;
  private File hrDir;

  @BeforeEach
  public void setUp() throws IOException {
    salesDir = new File(tempDir, "sales");
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
    // Try to create two schemas with the same name - this should now throw an exception
    Properties connectionProps = new Properties();
    applyEngineDefaults(connectionProps);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", connectionProps);
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Add first DATA schema pointing to sales directory
      Map<String, Object> salesOperand = new HashMap<>();
      salesOperand.put("directory", salesDir.getAbsolutePath());
      salesOperand.put("ephemeralCache", true);  // Use ephemeral cache for test isolation
      String engine = getExecutionEngine();
      if (engine != null && !engine.isEmpty()) {
        salesOperand.put("executionEngine", engine.toLowerCase());
      }
      rootSchema.add("data", FileSchemaFactory.INSTANCE.create(rootSchema, "data", salesOperand));

      // Try to add second DATA schema pointing to hr directory - this should throw an exception
      Map<String, Object> hrOperand = new HashMap<>();
      hrOperand.put("directory", hrDir.getAbsolutePath());
      hrOperand.put("ephemeralCache", true);  // Use ephemeral cache for test isolation
      if (engine != null && !engine.isEmpty()) {
        hrOperand.put("executionEngine", engine.toLowerCase());
      }

      // This should now throw an IllegalArgumentException due to duplicate schema name
      IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
        FileSchemaFactory.INSTANCE.create(rootSchema, "data", hrOperand);
      });

      // Verify the error message is descriptive
      String expectedMessage = "Schema with name 'data' already exists in parent schema";
      assertTrue(exception.getMessage().contains(expectedMessage), 
          "Expected error message to contain: " + expectedMessage + ", but got: " + exception.getMessage());
      
      // Verify the original schema still works
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total_count FROM data.customers");
        assertTrue(rs.next(), "Should be able to query original schema");
        assertTrue(rs.getInt("total_count") > 0, "Should have data in customers table");
      }
    }
  }

  @Test public void testMultipleDistinctSchemas() throws Exception {
    // Test multiple schemas with different names
    Properties connectionProps = new Properties();
    applyEngineDefaults(connectionProps);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", connectionProps);
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Add SALES schema
      Map<String, Object> salesOperand = new HashMap<>();
      salesOperand.put("directory", salesDir.getAbsolutePath());
      salesOperand.put("ephemeralCache", true);  // Use ephemeral cache for test isolation
      String engine = getExecutionEngine();
      if (engine != null && !engine.isEmpty()) {
        salesOperand.put("executionEngine", engine.toLowerCase());
      }
      rootSchema.add("sales", FileSchemaFactory.INSTANCE.create(rootSchema, "sales", salesOperand));

      // Add HR schema
      Map<String, Object> hrOperand = new HashMap<>();
      hrOperand.put("directory", hrDir.getAbsolutePath());
      hrOperand.put("ephemeralCache", true);  // Use ephemeral cache for test isolation
      if (engine != null && !engine.isEmpty()) {
        hrOperand.put("executionEngine", engine.toLowerCase());
      }
      rootSchema.add("hr", FileSchemaFactory.INSTANCE.create(rootSchema, "hr", hrOperand));

      // Test cross-schema query
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs =
            stmt.executeQuery("SELECT s.name as customer_name, h.name as employee_name " +
            "FROM sales.customers s, hr.employees h " +
            "WHERE s.id = h.id");

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
    // Test that schema replacement is now prevented by duplicate detection
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Add initial schema
      Map<String, Object> salesOperand = new HashMap<>();
      salesOperand.put("directory", salesDir.getAbsolutePath());
      rootSchema.add("test", FileSchemaFactory.INSTANCE.create(rootSchema, "test", salesOperand));

      // Verify sales tables are accessible
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total_count FROM test.customers");
        assertTrue(rs.next());
        int customerCount = rs.getInt("total_count");
        System.out.println("Initial customer count: " + customerCount);
        assertTrue(customerCount > 0);
      }

      // Attempt to replace with hr schema - this should now throw an exception
      Map<String, Object> hrOperand = new HashMap<>();
      hrOperand.put("directory", hrDir.getAbsolutePath());

      IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
        FileSchemaFactory.INSTANCE.create(rootSchema, "test", hrOperand);
      });
      
      // Verify the exception message
      assertTrue(exception.getMessage().contains("Schema with name 'test' already exists"),
          "Exception should mention the duplicate schema name");
      
      // Verify the original schema is still accessible and unchanged
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total_count FROM test.customers");
        assertTrue(rs.next());
        int customerCount = rs.getInt("total_count");
        assertTrue(customerCount > 0, "Original schema should still be accessible");
        
        // Verify we can't access HR tables (since replacement was prevented)
        try {
          stmt.executeQuery("SELECT COUNT(*) as total_count FROM test.employees");
          fail("Should not be able to access HR tables since replacement was prevented");
        } catch (Exception e) {
          // Expected - HR tables should not be accessible
        }
      }
    }
  }
  
  @Test public void testDuplicateSchemaDetectionWithDescriptiveError() throws Exception {
    // Test that duplicate schema detection provides clear, descriptive error messages
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Add multiple schemas first
      Map<String, Object> salesOperand = new HashMap<>();
      salesOperand.put("directory", salesDir.getAbsolutePath());
      rootSchema.add("sales", FileSchemaFactory.INSTANCE.create(rootSchema, "sales", salesOperand));
      
      Map<String, Object> hrOperand = new HashMap<>();
      hrOperand.put("directory", hrDir.getAbsolutePath());
      rootSchema.add("hr", FileSchemaFactory.INSTANCE.create(rootSchema, "hr", hrOperand));

      // Try to add a duplicate of the 'sales' schema
      Map<String, Object> duplicateOperand = new HashMap<>();
      duplicateOperand.put("directory", "/some/other/path");

      IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
        FileSchemaFactory.INSTANCE.create(rootSchema, "sales", duplicateOperand);
      });

      // Verify the error message contains expected elements
      String errorMessage = exception.getMessage();
      assertTrue(errorMessage.contains("Schema with name 'sales' already exists"), 
          "Error message should mention the duplicate schema name");
      assertTrue(errorMessage.contains("unique name within the same connection"), 
          "Error message should explain uniqueness requirement");
      assertTrue(errorMessage.contains("sales") && errorMessage.contains("hr"), 
          "Error message should list existing schemas for reference");
    }
  }
}