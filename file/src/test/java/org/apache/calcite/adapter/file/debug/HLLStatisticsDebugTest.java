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

import org.apache.calcite.adapter.file.DirectFileSource;
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsBuilder;
import org.apache.calcite.adapter.file.statistics.TableStatistics;
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
 * Debug test to verify HLL statistics generation and rule matching.
 */
public class HLLStatisticsDebugTest {
  
  @TempDir
  Path tempDir;
  
  private File csvFile;
  
  @BeforeEach
  public void setUp() throws Exception {
    csvFile = tempDir.resolve("test.csv").toFile();
    try (PrintWriter writer = new PrintWriter(csvFile)) {
      writer.println("customer_id,amount");
      for (int i = 1; i <= 1000; i++) {
        writer.println(i + "," + (i * 100));
      }
    }
  }
  
  @Test
  public void debugHLLStatisticsGeneration() throws Exception {
    System.out.println("=== HLL Statistics Debug ===");
    
    // Test HLL Cache
    HLLSketchCache cache = HLLSketchCache.getInstance();
    System.out.println("1. HLL Cache instance: " + cache);
    
    // Test manual HLL sketch
    HyperLogLogSketch sketch = new HyperLogLogSketch(12);
    for (int i = 1; i <= 1000; i++) {
      sketch.add(String.valueOf(i));
    }
    long estimate = sketch.getEstimate();
    System.out.println("2. Manual HLL sketch estimate for 1000 unique values: " + estimate);
    assertTrue(estimate >= 900 && estimate <= 1100, "HLL estimate should be close to 1000");
    
    // Test statistics generation
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    
    try {
      DirectFileSource source = new DirectFileSource(csvFile);
      StatisticsBuilder builder = new StatisticsBuilder();
      TableStatistics stats = builder.buildStatistics(source, cacheDir);
      
      System.out.println("3. Table statistics generated:");
      System.out.println("   Row count: " + stats.getRowCount());
      assertEquals(1000, stats.getRowCount());
      
      // Check HLL statistics for customer_id column
      org.apache.calcite.adapter.file.statistics.ColumnStatistics columnStats = stats.getColumnStatistics("customer_id");
      if (columnStats != null) {
        System.out.println("   customer_id distinct count: " + columnStats.getDistinctCount());
        assertTrue(columnStats.getDistinctCount() > 0, "Should have positive distinct count");
      } else {
        System.out.println("   customer_id column stats: null");
      }
      
    } catch (Exception e) {
      System.out.println("3. Statistics generation failed: " + e.getMessage());
      e.printStackTrace();
      fail("Statistics generation should not fail");
    }
    
    // Test with SQL query to see if HLL rules activate
    System.out.println("4. Testing SQL query with HLL rules:");
    testHLLQueryOptimization();
    
    System.clearProperty("calcite.file.statistics.hll.enabled");
  }
  
  private void testHLLQueryOptimization() throws Exception {
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    File cacheDir = tempDir.resolve("cache").toFile();
    System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      operand.put("primeCache", false);
      
      SchemaPlus fileSchema = rootSchema.add("HLL_TEST", 
          FileSchemaFactory.INSTANCE.create(rootSchema, "HLL_TEST", operand));
      
      System.out.println("   Schema created successfully");
      
      // Test regular COUNT(DISTINCT)
      long start = System.currentTimeMillis();
      try (PreparedStatement stmt = connection.prepareStatement(
              "SELECT COUNT(DISTINCT \"customer_id\") FROM HLL_TEST.\"test\"")) {
        ResultSet rs = stmt.executeQuery();
        if (rs.next()) {
          int result = rs.getInt(1);
          long elapsed = System.currentTimeMillis() - start;
          System.out.println("   COUNT(DISTINCT) result: " + result + " (took " + elapsed + "ms)");
          assertEquals(1000, result, "Should have 1000 distinct customers");
        }
      }
      
      // Test APPROX_COUNT_DISTINCT
      start = System.currentTimeMillis();
      try (PreparedStatement stmt = connection.prepareStatement(
              "SELECT APPROX_COUNT_DISTINCT(\"customer_id\") FROM HLL_TEST.\"test\"")) {
        ResultSet rs = stmt.executeQuery();
        if (rs.next()) {
          int result = rs.getInt(1);
          long elapsed = System.currentTimeMillis() - start;
          System.out.println("   APPROX_COUNT_DISTINCT result: " + result + " (took " + elapsed + "ms)");
          assertTrue(result >= 900 && result <= 1100, "APPROX result should be close to 1000");
        }
      } catch (Exception e) {
        System.out.println("   APPROX_COUNT_DISTINCT failed: " + e.getMessage());
      }
    }
  }
}