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
package org.apache.calcite.adapter.file.debug;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test to verify optimizations are actually working.
 */
public class OptimizationDebugTest {
  
  @TempDir
  Path tempDir;
  
  private File testCsvFile;
  
  @BeforeEach
  public void setUp() throws Exception {
    // Create a simple test CSV file
    testCsvFile = tempDir.resolve("test_data.csv").toFile();
    try (PrintWriter writer = new PrintWriter(testCsvFile)) {
      writer.println("customer_id,amount,category");
      for (int i = 1; i <= 1000; i++) {
        writer.println(i + "," + (i * 100) + ",cat" + (i % 10));
      }
    }
    
    System.out.println("Created test file: " + testCsvFile.getAbsolutePath());
    System.out.println("File size: " + testCsvFile.length() + " bytes");
  }
  
  @Test
  public void testHLLPropertyActivation() {
    System.out.println("\n=== Testing HLL Property Activation ===");
    
    // Clear and set property
    System.clearProperty("calcite.file.statistics.hll.enabled");
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    
    String value = System.getProperty("calcite.file.statistics.hll.enabled");
    System.out.println("HLL property value: " + value);
    assertEquals("true", value);
    
    // Check if HLL cache has any sketches
    HLLSketchCache cache = HLLSketchCache.getInstance();
    System.out.println("HLL Cache instance: " + cache);
    
    // Try to create a sketch manually
    HyperLogLogSketch sketch = new HyperLogLogSketch(12);
    for (int i = 1; i <= 100; i++) {
      sketch.add(String.valueOf(i));
    }
    long estimate = sketch.getEstimate();
    System.out.println("Manual HLL sketch estimate for 100 unique values: " + estimate);
    assertTrue(estimate >= 90 && estimate <= 110, "HLL estimate should be close to 100");
  }
  
  @Test
  public void testVectorizedPropertyActivation() {
    System.out.println("\n=== Testing Vectorized Property Activation ===");
    
    System.clearProperty("parquet.enable.vectorized.reader");
    System.setProperty("parquet.enable.vectorized.reader", "true");
    
    String value = System.getProperty("parquet.enable.vectorized.reader");
    System.out.println("Vectorized property value: " + value);
    assertEquals("true", value);
  }
  
  @Test
  public void testSchemaCreationAndQuery() throws Exception {
    System.out.println("\n=== Testing Schema Creation and Query ===");
    
    // Set properties
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("parquet.enable.vectorized.reader", "false"); // Use CSV first
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      // Create file schema
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "linq4j");  // Start with simple engine
      operand.put("primeCache", false);  // No cache priming for debug
      
      SchemaPlus fileSchema = rootSchema.add("TEST_SCHEMA", 
          FileSchemaFactory.INSTANCE.create(rootSchema, "TEST_SCHEMA", operand));
      
      System.out.println("✓ Schema created successfully");
      
      // Test basic query
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST_SCHEMA.\"test_data\"")) {
        
        if (rs.next()) {
          int count = rs.getInt(1);
          System.out.println("Row count: " + count);
          assertEquals(1000, count, "Should have 1000 rows");
        }
      }
      
      // Test COUNT(DISTINCT) query
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT customer_id) FROM TEST_SCHEMA.\"test_data\"")) {
        
        if (rs.next()) {
          int distinctCount = rs.getInt(1);
          System.out.println("Distinct customer count: " + distinctCount);
          assertEquals(1000, distinctCount, "Should have 1000 distinct customers");
        }
      }
      
      System.out.println("✓ Basic queries work");
    }
  }
  
  @Test 
  public void testPerformanceDifference() throws Exception {
    System.out.println("\n=== Testing Performance Difference ===");
    
    // Test with HLL disabled
    System.setProperty("calcite.file.statistics.hll.enabled", "false");
    long timeWithoutHLL = measureCountDistinct();
    System.out.println("Time without HLL: " + timeWithoutHLL + "ms");
    
    // Test with HLL enabled  
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    long timeWithHLL = measureCountDistinct();
    System.out.println("Time with HLL: " + timeWithHLL + "ms");
    
    System.out.println("Performance difference: " + ((double)timeWithoutHLL / timeWithHLL) + "x");
    
    // If HLL is working, we should see some difference (even if small on this test data)
    // But if times are identical, HLL definitely isn't working
    if (timeWithoutHLL == timeWithHLL) {
      System.out.println("⚠️  WARNING: Times are identical - HLL optimization not working!");
    } else {
      System.out.println("✓ Performance difference detected - HLL may be working");
    }
  }
  
  private long measureCountDistinct() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "linq4j");
      operand.put("primeCache", false);
      
      SchemaPlus fileSchema = rootSchema.add("PERF_TEST", 
          FileSchemaFactory.INSTANCE.create(rootSchema, "PERF_TEST", operand));
      
      // Warmup
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT customer_id) FROM PERF_TEST.\"test_data\"")) {
        rs.next();
      }
      
      // Actual measurement
      long start = System.currentTimeMillis();
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT customer_id) FROM PERF_TEST.\"test_data\"")) {
        rs.next();
      }
      long end = System.currentTimeMillis();
      
      return end - start;
    }
  }
}