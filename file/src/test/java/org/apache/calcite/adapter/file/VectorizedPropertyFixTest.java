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
import java.io.PrintWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that the vectorized reading system property fix is working.
 */
public class VectorizedPropertyFixTest {
  
  @TempDir
  Path tempDir;
  
  private File csvFile;
  
  @BeforeEach
  public void setUp() throws Exception {
    csvFile = tempDir.resolve("test.csv").toFile();
    try (PrintWriter writer = new PrintWriter(csvFile)) {
      writer.println("customer_id,amount");
      for (int i = 1; i <= 5000; i++) {
        writer.println(i + "," + (i * 100));
      }
    }
  }
  
  @Test
  public void testVectorizedPropertyIsRespected() throws Exception {
    System.out.println("=== Testing Engine Performance Differences ===");
    
    // Test with LINQ4J engine (baseline)
    System.out.println("\\n1. Testing with LINQ4J engine:");
    System.clearProperty("parquet.enable.vectorized.reader");
    long timeLinq4j = runQuery("linq4j");
    System.out.println("Time with LINQ4J: " + timeLinq4j + "ms");
    
    // Clear and force new schema creation
    Thread.sleep(100);
    System.gc();
    
    // Test with Parquet + vectorized disabled
    System.out.println("\\n2. Testing with Parquet (non-vectorized):");
    System.setProperty("parquet.enable.vectorized.reader", "false");
    long timeParquetNonVec = runQuery("parquet");
    System.out.println("Time with Parquet (non-vectorized): " + timeParquetNonVec + "ms");
    
    // Clear and force new schema creation
    Thread.sleep(100);
    System.gc();
    
    // Test with Parquet + vectorized enabled
    System.out.println("\\n3. Testing with Parquet (vectorized):");
    System.setProperty("parquet.enable.vectorized.reader", "true");
    long timeParquetVec = runQuery("parquet");
    System.out.println("Time with Parquet (vectorized): " + timeParquetVec + "ms");
    
    // Calculate ratios
    double linq4jToParquetNonVec = (double) timeLinq4j / timeParquetNonVec;
    double linq4jToParquetVec = (double) timeLinq4j / timeParquetVec;
    double nonVecToVec = (double) timeParquetNonVec / timeParquetVec;
    
    System.out.println("\\n=== Performance Analysis ===");
    System.out.println("LINQ4J vs Parquet (non-vectorized): " + String.format("%.2fx", linq4jToParquetNonVec));
    System.out.println("LINQ4J vs Parquet (vectorized): " + String.format("%.2fx", linq4jToParquetVec));
    System.out.println("Parquet non-vectorized vs vectorized: " + String.format("%.2fx", nonVecToVec));
    
    // Test expectations
    assertTrue(linq4jToParquetVec > 1.5, 
        "LINQ4J should be significantly slower than vectorized Parquet. " +
        "LINQ4J: " + timeLinq4j + "ms, Parquet vectorized: " + timeParquetVec + "ms");
    
    // Vectorized should be different from non-vectorized (even if not necessarily faster)
    boolean vectorizedDifferent = Math.abs(timeParquetNonVec - timeParquetVec) > 10;
    assertTrue(vectorizedDifferent, 
        "Vectorized and non-vectorized should have different performance. " +
        "Non-vec: " + timeParquetNonVec + "ms, Vectorized: " + timeParquetVec + "ms");
    
    System.out.println("\\n✓ All performance differences are as expected!");
    
    System.clearProperty("parquet.enable.vectorized.reader");
  }
  
  private long runQuery(String engine) throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", engine);
      operand.put("primeCache", false);
      
      // Create unique schema name to force new schema creation
      String schemaName = "TEST_" + System.currentTimeMillis() + "_" + Thread.currentThread().getName().hashCode();
      SchemaPlus fileSchema = rootSchema.add(schemaName, 
          FileSchemaFactory.INSTANCE.create(rootSchema, schemaName, operand));
      
      // Warmup
      try (PreparedStatement stmt = connection.prepareStatement(
              "SELECT COUNT(*) FROM " + schemaName + ".\"test\"")) {
        ResultSet rs = stmt.executeQuery();
        rs.next();
      }
      
      // Actual test - multiple operations to amplify differences
      long start = System.currentTimeMillis();
      for (int i = 0; i < 3; i++) {
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT COUNT(*), SUM(\"amount\"), AVG(\"amount\"), MIN(\"customer_id\"), MAX(\"customer_id\") FROM " + schemaName + ".\"test\"")) {
          ResultSet rs = stmt.executeQuery();
          if (rs.next() && i == 0) {
            System.out.println("  Results: count=" + rs.getInt(1) + ", sum=" + rs.getLong(2));
          }
        }
      }
      long end = System.currentTimeMillis();
      
      return end - start;
    }
  }
}