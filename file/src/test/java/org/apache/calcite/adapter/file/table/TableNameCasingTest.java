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
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for table name casing configuration in FileSchema.
 */
public class TableNameCasingTest {

  @TempDir
  File tempDir;

  @Test void testDefaultUpperCasing() throws Exception {
    // Create a CSV file with lowercase name
    File csvFile = new File(tempDir, "test_data.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("id:int,name:string\n");
      writer.write("1,Alice\n");
      writer.write("2,Bob\n");
    }

    // Create schema with default settings (should uppercase table names)
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());

    try (Connection connection = createConnection()) {
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Schema schema = FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand);
      rootSchema.add("TEST", schema);

      // Query should work with uppercase table name
      try (Statement statement = connection.createStatement()) {
        // First, let's see what tables are available
        ResultSet tables = connection.getMetaData().getTables(null, "TEST", "%", null);
        System.out.println("Available tables in schema 'test':");
        while (tables.next()) {
          System.out.println("  Table: " + tables.getString("TABLE_NAME"));
        }
        tables.close();

        ResultSet rs = statement.executeQuery("SELECT * FROM test.test_data");
        System.out.println("Query executed successfully");
        boolean hasNext = rs.next();
        System.out.println("ResultSet.next() = " + hasNext);
        if (hasNext) {
          System.out.println("id = " + rs.getInt("id"));
          System.out.println("name = " + rs.getString("name"));
        } else {
          System.out.println("No rows returned");
        }
        assertTrue(hasNext);
        assertThat(rs.getInt("id"), equalTo(1));
        assertThat(rs.getString("name"), equalTo("Alice"));
      }

      // Query with lowercase should fail
      try (Statement statement = connection.createStatement()) {
        statement.executeQuery("SELECT * FROM test.test_data");
        fail("Expected query to fail with lowercase table name");
      } catch (SQLException e) {
        System.out.println("Exception message: " + e.getMessage());
        assertTrue(e.getMessage().contains("test_data") || e.getMessage().contains("TEST.test_data"));
      }
    }
  }

  @Test void testLowerCasing() throws Exception {
    // Create a CSV file
    File csvFile = new File(tempDir, "Test_Data.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("id:int,name:string\n");
      writer.write("1,Alice\n");
      writer.write("2,Bob\n");
    }

    // Create schema with lowercase table name casing
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("tableNameCasing", "LOWER");

    try (Connection connection = createConnection()) {
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Schema schema = FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand);
      rootSchema.add("TEST", schema);

      // Query should work with lowercase table name
      try (Statement statement = connection.createStatement()) {
        ResultSet rs = statement.executeQuery("SELECT * FROM test.test_data");
        assertTrue(rs.next());
        assertThat(rs.getInt("id"), equalTo(1));
        assertThat(rs.getString("name"), equalTo("Alice"));
      }

      // Query with uppercase should fail
      try (Statement statement = connection.createStatement()) {
        statement.executeQuery("SELECT * FROM test.TEST_DATA");
        fail("Expected query to fail with uppercase table name");
      } catch (SQLException e) {
        System.out.println("Exception message: " + e.getMessage());
        assertTrue(e.getMessage().contains("TEST_DATA") || e.getMessage().contains("TEST.TEST_DATA"));
      }
    }
  }

  @Test void testUnchangedCasing() throws Exception {
    // Create a CSV file with mixed case name
    File csvFile = new File(tempDir, "Test_Data.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("id:int,name:string\n");
      writer.write("1,Alice\n");
      writer.write("2,Bob\n");
    }

    // Create schema with unchanged table name casing
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("tableNameCasing", "UNCHANGED");

    try (Connection connection = createConnection()) {
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Schema schema = FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand);
      rootSchema.add("TEST", schema);

      // Query should work with exact case match
      try (Statement statement = connection.createStatement()) {
        ResultSet rs = statement.executeQuery("SELECT * FROM test.Test_Data");
        assertTrue(rs.next());
        assertThat(rs.getInt("id"), equalTo(1));
        assertThat(rs.getString("name"), equalTo("Alice"));
      }

      // Query with different case should fail
      try (Statement statement = connection.createStatement()) {
        statement.executeQuery("SELECT * FROM test.test_data");
        fail("Expected query to fail with incorrect case");
      } catch (SQLException e) {
        System.out.println("Exception message: " + e.getMessage());
        assertTrue(e.getMessage().contains("test_data") || e.getMessage().contains("TEST.test_data"));
      }
    }
  }

  @Test void testColumnNameCasing() throws Exception {
    // Create a CSV file with mixed case column names
    File csvFile = new File(tempDir, "test_columns.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("Customer_ID:int,First_Name:string,Last_Name:string\n");
      writer.write("1,John,Doe\n");
      writer.write("2,Jane,Smith\n");
    }

    // Test LOWER column casing
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("columnNameCasing", "LOWER");

    try (Connection connection = createConnection()) {
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Schema schema = FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand);
      rootSchema.add("TEST", schema);

      // Query should work with lowercase column names
      try (Statement statement = connection.createStatement()) {
        ResultSet rs = statement.executeQuery("SELECT customer_id, first_name FROM test.test_columns");
        assertTrue(rs.next());
        assertThat(rs.getInt("customer_id"), equalTo(1));
        assertThat(rs.getString("first_name"), equalTo("John"));
      }
    }
  }

  @Test void testMixedCasing() throws Exception {
    // Create a CSV file
    File csvFile = new File(tempDir, "Mixed_Case_Table.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("Product_ID:int,Product_Name:string,Unit_Price:double\n");
      writer.write("1,Widget,19.99\n");
      writer.write("2,Gadget,29.99\n");
    }

    // Test PostgreSQL-style (lowercase everything)
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("tableNameCasing", "LOWER");
    operand.put("columnNameCasing", "LOWER");

    try (Connection connection = createConnection()) {
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Schema schema = FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand);
      rootSchema.add("TEST", schema);

      // Everything should be lowercase
      try (Statement statement = connection.createStatement()) {
        ResultSet rs = statement.executeQuery("SELECT product_id, product_name, unit_price FROM test.mixed_case_table");
        assertTrue(rs.next());
        assertThat(rs.getInt("product_id"), equalTo(1));
        assertThat(rs.getString("product_name"), equalTo("Widget"));
        assertThat(rs.getDouble("unit_price"), equalTo(19.99));
      }
    }
  }

  private Connection createConnection() throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }
}
