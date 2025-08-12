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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;
import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify vectorized reading and cache priming configuration works correctly.
 */
@Tag("performance")public class VectorizedAndCachePrimingTest {

  @TempDir
  File tempDir;

  @Test
  void testVectorizedReadingConfiguration() throws Exception {
    // Create test CSV file
    File csvFile = new File(tempDir, "test_data.csv");
    try (PrintWriter writer = new PrintWriter(csvFile)) {
      writer.println("id,name,value");
      for (int i = 1; i <= 1000; i++) {
        writer.println(i + ",Name" + i + "," + (i * 10));
      }
    }
    
    // Test with vectorized reading enabled
    System.setProperty("parquet.enable.vectorized.reader", "true");
    
    Properties props = new Properties();
    props.put("model", "inline:"
        + "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        executionEngine: 'parquet',\n"
        + "        primeCache: false\n"  // Disable for this test
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_data")) {
      
      assertTrue(rs.next());
      assertEquals(1000, rs.getInt(1));
      
      System.out.println("Vectorized reading test passed");
    } finally {
      System.clearProperty("parquet.enable.vectorized.reader");
    }
  }
  
  @Test
  void testCachePrimingConfiguration() throws Exception {
    // Create test CSV files
    for (int i = 1; i <= 3; i++) {
      File csvFile = new File(tempDir, "table" + i + ".csv");
      try (PrintWriter writer = new PrintWriter(csvFile)) {
        writer.println("id,value");
        for (int j = 1; j <= 100; j++) {
          writer.println(j + "," + (j * i));
        }
      }
    }
    
    // Test with cache priming enabled (default)
    Properties props = new Properties();
    props.put("model", "inline:"
        + "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        primeCache: true\n"  // Explicitly enable
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Wait for cache priming to complete
      Thread.sleep(500);
      
      // Run queries - should use warm cache
      try (Statement stmt = conn.createStatement()) {
        for (int i = 1; i <= 3; i++) {
          try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM table" + i)) {
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
          }
        }
      }
      
      System.out.println("Cache priming test passed");
    }
  }
  
  @Test
  void testCombinedVectorizedAndCachePriming() throws Exception {
    // Create larger test CSV file
    File csvFile = new File(tempDir, "large_table.csv");
    try (PrintWriter writer = new PrintWriter(csvFile)) {
      writer.println("id,category,value");
      for (int i = 1; i <= 10000; i++) {
        writer.println(i + ",Cat" + (i % 10) + "," + (i * 100));
      }
    }
    
    // Enable both vectorized reading and cache priming
    System.setProperty("parquet.enable.vectorized.reader", "true");
    
    Properties props = new Properties();
    props.put("model", "inline:"
        + "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        executionEngine: 'parquet',\n"
        + "        primeCache: true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Wait for cache priming
      Thread.sleep(500);
      
      // Run aggregation query - should benefit from both optimizations
      String query = "SELECT category, COUNT(*), SUM(value) " +
                    "FROM large_table " +
                    "GROUP BY category";
      
      long start = System.currentTimeMillis();
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        
        int count = 0;
        while (rs.next()) {
          count++;
          assertNotNull(rs.getString(1));
          assertTrue(rs.getInt(2) > 0);
          assertTrue(rs.getLong(3) > 0);
        }
        assertEquals(10, count);  // 10 categories
      }
      
      long elapsed = System.currentTimeMillis() - start;
      System.out.println("Combined optimization query completed in " + elapsed + " ms");
      
      // Second query should be even faster (fully warm cache)
      start = System.currentTimeMillis();
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT category) FROM large_table")) {
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
      }
      long elapsed2 = System.currentTimeMillis() - start;
      
      System.out.println("Second query (warm cache) completed in " + elapsed2 + " ms");
      assertTrue(elapsed2 <= elapsed, "Second query should be as fast or faster");
      
    } finally {
      System.clearProperty("parquet.enable.vectorized.reader");
    }
  }
}