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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.adapter.file.converters.JsonPathConverter;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that JSONPath extractions work correctly with the refresh mechanism.
 * Tests that when source JSON files change, extracted JSON files are re-converted
 * and queries return updated data.
 */
@Tag("unit")
@Isolated  // Needs isolation due to static ConversionMetadata state
public class JsonPathRefreshTest {

  @TempDir
  Path tempDir;
  
  private File schemaDir;
  
  @BeforeEach
  public void setupTestFiles() throws Exception {
    schemaDir = tempDir.toFile();
    
    // Metadata now stored directly in the test directory
    // This avoids conflicts when tests run in parallel
  }
  
  @AfterEach
  public void cleanup() throws Exception {
    // No longer need to reset central metadata directory
    
    // Clear various static caches that might interfere between tests
    try {
      org.apache.calcite.adapter.file.storage.StorageProviderFactory.clearCache();
    } catch (Exception e) {
      // Ignore cleanup errors
    }
    
    try {
      org.apache.calcite.adapter.file.converters.SafeExcelToJsonConverter.clearCache();
    } catch (Exception e) {
      // Ignore cleanup errors  
    }
    
    try {
      org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager.clearCache();
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }
  
  @Test
  public void testJsonPathExtractionRefresh() throws Exception {
    
    // Create a source JSON file with nested data
    File sourceJsonFile = new File(schemaDir, "api_data.json");
    try (FileWriter writer = new FileWriter(sourceJsonFile, StandardCharsets.UTF_8)) {
      writer.write("{\n");
      writer.write("  \"status\": \"success\",\n");
      writer.write("  \"data\": {\n");
      writer.write("    \"users\": [\n");
      writer.write("      {\"id\": 1, \"name\": \"Alice\", \"role\": \"admin\"},\n");
      writer.write("      {\"id\": 2, \"name\": \"Bob\", \"role\": \"user\"}\n");
      writer.write("    ],\n");
      writer.write("    \"products\": [\n");
      writer.write("      {\"id\": \"P001\", \"name\": \"Laptop\", \"price\": 999.99}\n");
      writer.write("    ]\n");
      writer.write("  }\n");
      writer.write("}\n");
    }
    
    // Extract users using JSONPath
    File usersFile = new File(schemaDir, "users.json");
    JsonPathConverter.extract(sourceJsonFile, usersFile, "$.data.users");
    
    // Verify metadata was recorded
    ConversionMetadata metadata = new ConversionMetadata(schemaDir);
    File foundSource = metadata.findOriginalSource(usersFile);
    assertNotNull(foundSource, "Conversion metadata should be recorded");
    assertEquals(sourceJsonFile.getCanonicalPath(), foundSource.getCanonicalPath());
    
    
    // Create connection with refresh enabled (parquet engine)
    Properties info = new Properties();
    info.setProperty("directory", schemaDir.getAbsolutePath());
    info.setProperty("executionEngine", "parquet");
    info.setProperty("refreshInterval", "1 second");
    
    
    try (Connection connection = createConnection(info)) {
      Statement statement = connection.createStatement();
      
      // Check what type of table was created for 'users'
      java.sql.DatabaseMetaData meta = connection.getMetaData();
      java.sql.ResultSet tables = meta.getTables(null, null, "users", null);
      while (tables.next()) {
      }
      tables.close();
      
      // Query initial data
      ResultSet rs = statement.executeQuery("SELECT id, name, role FROM users ORDER BY id");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      assertEquals("Alice", rs.getString("name"));
      assertEquals("admin", rs.getString("role"));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt("id"));
      assertEquals("Bob", rs.getString("name"));
      assertEquals("user", rs.getString("role"));
      assertFalse(rs.next());
      rs.close();
      
      
      // Modify the source JSON file to add new users
      Thread.sleep(1100); // Ensure file timestamp changes
      try (FileWriter writer = new FileWriter(sourceJsonFile, StandardCharsets.UTF_8)) {
        writer.write("{\n");
        writer.write("  \"status\": \"success\",\n");
        writer.write("  \"data\": {\n");
        writer.write("    \"users\": [\n");
        writer.write("      {\"id\": 1, \"name\": \"Alice\", \"role\": \"admin\"},\n");
        writer.write("      {\"id\": 2, \"name\": \"Bob\", \"role\": \"user\"},\n");
        writer.write("      {\"id\": 3, \"name\": \"Charlie\", \"role\": \"manager\"},\n");
        writer.write("      {\"id\": 4, \"name\": \"Diana\", \"role\": \"developer\"}\n");
        writer.write("    ],\n");
        writer.write("    \"products\": [\n");
        writer.write("      {\"id\": \"P001\", \"name\": \"Laptop\", \"price\": 999.99},\n");
        writer.write("      {\"id\": \"P002\", \"name\": \"Mouse\", \"price\": 29.99}\n");
        writer.write("    ]\n");
        writer.write("  }\n");
        writer.write("}\n");
      }
      
      // Force file timestamp update - use canonical path to handle symlinks
      File canonicalSourceFile = sourceJsonFile.getCanonicalFile();
      long newTimestamp = System.currentTimeMillis();
      canonicalSourceFile.setLastModified(newTimestamp);
      
      // Also update the non-canonical file in case the watcher is using it
      sourceJsonFile.setLastModified(newTimestamp);
      
      // The refresh mechanism should detect the change and re-extract
      Thread.sleep(2000); // Wait longer for refresh interval to ensure it triggers
      
      
      // Query again - should see updated data
      rs = statement.executeQuery("SELECT COUNT(*) as user_count FROM users");
      assertTrue(rs.next());
      assertEquals(4, rs.getInt("user_count"));
      rs.close();
      
      // Verify new users are present
      rs = statement.executeQuery("SELECT id, name, role FROM users WHERE id > 2 ORDER BY id");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("id"));
      assertEquals("Charlie", rs.getString("name"));
      assertEquals("manager", rs.getString("role"));
      assertTrue(rs.next());
      assertEquals(4, rs.getInt("id"));
      assertEquals("Diana", rs.getString("name"));
      assertEquals("developer", rs.getString("role"));
      assertFalse(rs.next());
      rs.close();
      
      
      statement.close();
    }
  }
  
  @Test
  public void testMultipleJsonPathExtractions() throws Exception {
    
    // Create a source JSON file with multiple extractable sections
    File sourceJsonFile = new File(schemaDir, "multi_data.json");
    try (FileWriter writer = new FileWriter(sourceJsonFile, StandardCharsets.UTF_8)) {
      writer.write("{\n");
      writer.write("  \"company\": {\n");
      writer.write("    \"employees\": [\n");
      writer.write("      {\"id\": 1, \"name\": \"John\", \"dept\": \"Engineering\"}\n");
      writer.write("    ],\n");
      writer.write("    \"departments\": [\n");
      writer.write("      {\"id\": \"ENG\", \"name\": \"Engineering\", \"budget\": 1000000}\n");
      writer.write("    ]\n");
      writer.write("  }\n");
      writer.write("}\n");
    }
    
    // Extract both employees and departments using different JSONPath expressions
    File employeesFile = new File(schemaDir, "employees.json");
    File departmentsFile = new File(schemaDir, "departments.json");
    
    JsonPathConverter.extract(sourceJsonFile, employeesFile, "$.company.employees");
    JsonPathConverter.extract(sourceJsonFile, departmentsFile, "$.company.departments");
    
    
    // Create connection with refresh enabled
    Properties info = new Properties();
    info.setProperty("directory", schemaDir.getAbsolutePath());
    info.setProperty("executionEngine", "parquet");
    info.setProperty("refreshInterval", "1 second");
    
    try (Connection connection = createConnection(info)) {
      Statement statement = connection.createStatement();
      
      // Query initial employees
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) as emp_count FROM employees");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("emp_count"));
      rs.close();
      
      // Query initial departments
      rs = statement.executeQuery("SELECT COUNT(*) as dept_count FROM departments");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("dept_count"));
      rs.close();
      
      
      // Update source JSON to add more data to both sections
      Thread.sleep(1100);
      try (FileWriter writer = new FileWriter(sourceJsonFile, StandardCharsets.UTF_8)) {
        writer.write("{\n");
        writer.write("  \"company\": {\n");
        writer.write("    \"employees\": [\n");
        writer.write("      {\"id\": 1, \"name\": \"John\", \"dept\": \"Engineering\"},\n");
        writer.write("      {\"id\": 2, \"name\": \"Jane\", \"dept\": \"Sales\"},\n");
        writer.write("      {\"id\": 3, \"name\": \"Mike\", \"dept\": \"Marketing\"}\n");
        writer.write("    ],\n");
        writer.write("    \"departments\": [\n");
        writer.write("      {\"id\": \"ENG\", \"name\": \"Engineering\", \"budget\": 1200000},\n");
        writer.write("      {\"id\": \"SALES\", \"name\": \"Sales\", \"budget\": 800000},\n");
        writer.write("      {\"id\": \"MKT\", \"name\": \"Marketing\", \"budget\": 600000}\n");
        writer.write("    ]\n");
        writer.write("  }\n");
        writer.write("}\n");
      }
      
      long newTimestamp2 = System.currentTimeMillis();
      sourceJsonFile.setLastModified(newTimestamp2);
      sourceJsonFile.getCanonicalFile().setLastModified(newTimestamp2);
      Thread.sleep(2000);
      
      // Verify both extractions were refreshed
      rs = statement.executeQuery("SELECT COUNT(*) as emp_count FROM employees");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("emp_count"));
      rs.close();
      
      rs = statement.executeQuery("SELECT COUNT(*) as dept_count FROM departments");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("dept_count"));
      rs.close();
      
      // Check specific new data
      rs = statement.executeQuery("SELECT name FROM employees WHERE dept = 'Sales'");
      assertTrue(rs.next());
      assertEquals("Jane", rs.getString("name"));
      rs.close();
      
      rs = statement.executeQuery("SELECT budget FROM departments WHERE id = 'MKT'");
      assertTrue(rs.next());
      assertEquals(600000, rs.getInt("budget"));
      rs.close();
      
      
      statement.close();
    }
  }
  
  private Connection createConnection(Properties info) throws Exception {
    String url = "jdbc:calcite:";
    Properties connectionProperties = new Properties();
    connectionProperties.setProperty("lex", "ORACLE");
    connectionProperties.setProperty("unquotedCasing", "TO_LOWER");
    
    // File adapter configuration
    StringBuilder model = new StringBuilder();
    model.append("{\n");
    model.append("  \"version\": \"1.0\",\n");
    model.append("  \"defaultSchema\": \"files\",\n");
    model.append("  \"schemas\": [\n");
    model.append("    {\n");
    model.append("      \"name\": \"files\",\n");
    model.append("      \"type\": \"custom\",\n");
    model.append("      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n");
    model.append("      \"operand\": {\n");
    model.append("        \"directory\": \"").append(info.getProperty("directory").replace("\\", "\\\\")).append("\",\n");
    
    if (info.containsKey("refreshInterval")) {
      model.append("        \"refreshInterval\": \"").append(info.getProperty("refreshInterval")).append("\",\n");
    }
    
    if (info.containsKey("executionEngine")) {
      model.append("        \"executionEngine\": \"").append(info.getProperty("executionEngine")).append("\",\n");
    }
    
    model.append("        \"caseSensitive\": false\n");
    model.append("      }\n");
    model.append("    }\n");
    model.append("  ]\n");
    model.append("}\n");
    
    connectionProperties.setProperty("model", "inline:" + model.toString());
    
    return DriverManager.getConnection(url, connectionProperties);
  }
}