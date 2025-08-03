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
package org.apache.calcite.adapter.file.performance;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Performance test that separates cold start, warm start, and pure query performance.
 */
@Tag("performance")
public class SeparatedPerformanceTest {
  @TempDir
  java.nio.file.Path tempDir;

  private static final int DATASET_SIZE = 100_000;
  private static final int WARMUP_RUNS = 2;
  private static final int TEST_RUNS = 5;

  @BeforeEach
  public void setUp() throws Exception {
    // Skip performance tests unless explicitly enabled
    assumeTrue(Boolean.getBoolean("enablePerformanceTests"), 
        "Performance tests disabled - use -DenablePerformanceTests=true to enable");
    
    System.out.println("\n=== SEPARATED PERFORMANCE ANALYSIS ===");
    createTestDataset();
  }

  @Test public void testSeparatedPerformance() throws Exception {
    System.out.println("\nAnalyzing performance with proper separation...");
    
    // Test queries
    String[] queries = {
        "SELECT COUNT(*) FROM TEST.\"SALES\"",
        "SELECT \"region\", COUNT(*), SUM(\"total\") FROM TEST.\"SALES\" GROUP BY \"region\"",
        "SELECT \"order_id\", \"total\" FROM TEST.\"SALES\" WHERE \"total\" > 500 ORDER BY \"total\" DESC LIMIT 100"
    };
    
    String[] queryNames = {"COUNT Query", "GROUP BY Query", "TOP-N Query"};
    
    for (int i = 0; i < queries.length; i++) {
      System.out.println("\n" + "=".repeat(60));
      System.out.println("QUERY: " + queryNames[i]);
      System.out.println("=".repeat(60));
      
      analyzeQueryPerformance(queries[i]);
    }
    
    printSummaryInsights();
  }

  private void analyzeQueryPerformance(String query) throws Exception {
    
    // 1. COLD START PERFORMANCE (includes conversion for Parquet)
    System.out.println("\n## 1. COLD START PERFORMANCE ##");
    System.out.println("(Fresh connections + cache cleared)");
    
    Map<String, PerformanceResult> coldStartResults = new HashMap<>();
    
    // Clear any existing cache
    clearParquetCache();
    coldStartResults.put("LINQ4J", measureColdStart("linq4j", query));
    
    clearParquetCache();
    coldStartResults.put("PARQUET", measureColdStart("parquet", query));
    
    clearParquetCache();
    coldStartResults.put("VECTORIZED", measureColdStart("vectorized", query));
    
    displayResults("Cold Start", coldStartResults);
    
    // 2. WARM START PERFORMANCE (cache exists, fresh connections)
    System.out.println("\n## 2. WARM START PERFORMANCE ##");
    System.out.println("(Fresh connections + cache pre-built)");
    
    // Pre-build cache
    prebuildParquetCache();
    
    Map<String, PerformanceResult> warmStartResults = new HashMap<>();
    warmStartResults.put("LINQ4J", measureWarmStart("linq4j", query));
    warmStartResults.put("PARQUET", measureWarmStart("parquet", query));
    warmStartResults.put("VECTORIZED", measureWarmStart("vectorized", query));
    
    displayResults("Warm Start", warmStartResults);
    
    // 3. PURE QUERY PERFORMANCE (persistent connection, cache pre-built)
    System.out.println("\n## 3. PURE QUERY PERFORMANCE ##");
    System.out.println("(Persistent connection + cache pre-built)");
    
    Map<String, PerformanceResult> queryResults = new HashMap<>();
    queryResults.put("LINQ4J", measurePureQuery("linq4j", query));
    queryResults.put("PARQUET", measurePureQuery("parquet", query));
    queryResults.put("VECTORIZED", measurePureQuery("vectorized", query));
    
    displayResults("Pure Query", queryResults);
  }

  private PerformanceResult measureColdStart(String engine, String query) throws Exception {
    System.out.printf(Locale.ROOT, "\nTesting %s cold start:%n", engine.toUpperCase(Locale.ROOT));
    
    long totalTime = 0;
    int successfulRuns = 0;
    
    for (int i = 0; i < TEST_RUNS; i++) {
      try {
        Thread.sleep(100); // Allow GC
        
        long startTime = System.currentTimeMillis();
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
          
          SchemaPlus rootSchema = calciteConnection.getRootSchema();
          
          Map<String, Object> operand = new HashMap<>();
          operand.put("directory", tempDir.toString());
          operand.put("executionEngine", engine);
          
          SchemaPlus fileSchema = 
              rootSchema.add("TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));
          
          try (Statement stmt = connection.createStatement();
               ResultSet rs = stmt.executeQuery(query)) {
            
            int rowCount = 0;
            while (rs.next()) {
              rowCount++;
              rs.getObject(1); // Force materialization
            }
          }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        totalTime += duration;
        successfulRuns++;
        
        System.out.printf(Locale.ROOT, "  Run %d: %d ms%n", i + 1, duration);
        
      } catch (Exception e) {
        System.out.printf(Locale.ROOT, "  Run %d: FAILED - %s%n", i + 1, e.getMessage());
      }
    }
    
    if (successfulRuns == 0) {
      return new PerformanceResult(false, 0, "All runs failed");
    }
    
    long avgTime = totalTime / successfulRuns;
    System.out.printf(Locale.ROOT, "  Average: %d ms%n", avgTime);
    
    return new PerformanceResult(true, avgTime, null);
  }

  private PerformanceResult measureWarmStart(String engine, String query) throws Exception {
    System.out.printf(Locale.ROOT, "\nTesting %s warm start:%n", engine.toUpperCase(Locale.ROOT));
    
    long totalTime = 0;
    int successfulRuns = 0;
    
    for (int i = 0; i < TEST_RUNS; i++) {
      try {
        Thread.sleep(100); // Allow GC
        
        long startTime = System.currentTimeMillis();
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
          
          SchemaPlus rootSchema = calciteConnection.getRootSchema();
          
          Map<String, Object> operand = new HashMap<>();
          operand.put("directory", tempDir.toString());
          operand.put("executionEngine", engine);
          
          SchemaPlus fileSchema = 
              rootSchema.add("TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));
          
          try (Statement stmt = connection.createStatement();
               ResultSet rs = stmt.executeQuery(query)) {
            
            int rowCount = 0;
            while (rs.next()) {
              rowCount++;
              rs.getObject(1); // Force materialization
            }
          }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        totalTime += duration;
        successfulRuns++;
        
        System.out.printf(Locale.ROOT, "  Run %d: %d ms%n", i + 1, duration);
        
      } catch (Exception e) {
        System.out.printf(Locale.ROOT, "  Run %d: FAILED - %s%n", i + 1, e.getMessage());
      }
    }
    
    if (successfulRuns == 0) {
      return new PerformanceResult(false, 0, "All runs failed");
    }
    
    long avgTime = totalTime / successfulRuns;
    System.out.printf(Locale.ROOT, "  Average: %d ms%n", avgTime);
    
    return new PerformanceResult(true, avgTime, null);
  }

  private PerformanceResult measurePureQuery(String engine, String query) throws Exception {
    System.out.printf(Locale.ROOT, "\nTesting %s pure query:%n", engine.toUpperCase(Locale.ROOT));
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", engine);
      
      SchemaPlus fileSchema = 
          rootSchema.add("TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));
      
      // Warmup runs
      for (int i = 0; i < WARMUP_RUNS; i++) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
          while (rs.next()) {
            rs.getObject(1);
          }
        }
      }
      
      // Test runs
      long totalTime = 0;
      for (int i = 0; i < TEST_RUNS; i++) {
        Thread.sleep(50); // Brief pause
        
        long startTime = System.currentTimeMillis();
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
          
          int rowCount = 0;
          while (rs.next()) {
            rowCount++;
            rs.getObject(1); // Force materialization
          }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        totalTime += duration;
        
        System.out.printf(Locale.ROOT, "  Run %d: %d ms%n", i + 1, duration);
      }
      
      long avgTime = totalTime / TEST_RUNS;
      System.out.printf(Locale.ROOT, "  Average: %d ms%n", avgTime);
      
      return new PerformanceResult(true, avgTime, null);
      
    } catch (Exception e) {
      System.out.printf(Locale.ROOT, "  FAILED: %s%n", e.getMessage());
      return new PerformanceResult(false, 0, e.getMessage());
    }
  }

  private void displayResults(String phase, Map<String, PerformanceResult> results) {
    System.out.printf(Locale.ROOT, "\n### %s Results ###%n", phase);
    System.out.println("Engine       Time(ms)  Speedup   Status");
    System.out.println("---------    --------  -------   ------");
    
    PerformanceResult baseline = results.get("LINQ4J");
    
    for (Map.Entry<String, PerformanceResult> entry : results.entrySet()) {
      String engine = entry.getKey();
      PerformanceResult result = entry.getValue();
      
      if (!result.success) {
        System.out.printf(Locale.ROOT, "%-12s   FAILED    -       %s%n", engine, result.error);
        continue;
      }
      
      if (baseline != null && baseline.success) {
        double speedup = (double) baseline.avgTime / result.avgTime;
        System.out.printf(Locale.ROOT, "%-12s   %,6d    %.1fx    OK%n", 
            engine, result.avgTime, speedup);
      } else {
        System.out.printf(Locale.ROOT, "%-12s   %,6d    -       OK%n", 
            engine, result.avgTime);
      }
    }
  }

  private void clearParquetCache() {
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache");
    if (cacheDir.exists()) {
      File[] files = cacheDir.listFiles();
      if (files != null) {
        for (File file : files) {
          file.delete();
        }
      }
    }
  }

  private void prebuildParquetCache() throws Exception {
    // Run a quick query with parquet engine to build cache
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      
      SchemaPlus fileSchema = 
          rootSchema.add("CACHE", FileSchemaFactory.INSTANCE.create(rootSchema, "CACHE", operand));
      
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM CACHE.\"SALES\"")) {
        rs.next();
      }
    }
  }

  private void createTestDataset() throws IOException {
    File file = new File(tempDir.toFile(), "sales.csv");
    
    System.out.printf(Locale.ROOT, "Creating test dataset: %,d rows%n", DATASET_SIZE);
    
    Random rand = new Random(42);
    
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      // Header
      writer.write("order_id:int,customer_id:int,product_category:string,");
      writer.write("region:string,quantity:int,unit_price:double,total:double,");
      writer.write("order_date:string,status:string\n");
      
      String[] categories = {"Electronics", "Clothing", "Books", "Home", "Sports"};
      String[] regions = {"North America", "Europe", "Asia Pacific"};
      String[] statuses = {"pending", "completed", "shipped"};
      
      for (int i = 0; i < DATASET_SIZE; i++) {
        int orderId = 1000000 + i;
        int customerId = 10000 + rand.nextInt(1000);
        String category = categories[rand.nextInt(categories.length)];
        String region = regions[rand.nextInt(regions.length)];
        int quantity = 1 + rand.nextInt(10);
        double unitPrice = 10.0 + rand.nextDouble() * 90.0;
        double total = quantity * unitPrice;
        String date = String.format(Locale.ROOT, "2024-%02d-%02d", 
            1 + rand.nextInt(12), 1 + rand.nextInt(28));
        String status = statuses[rand.nextInt(statuses.length)];
        
        writer.write(String.format(Locale.ROOT,
            "%d,%d,%s,%s,%d,%.2f,%.2f,%s,%s\n",
            orderId, customerId, category, region, quantity, 
            unitPrice, total, date, status));
      }
    }
    
    System.out.printf(Locale.ROOT, "Created: %s (%.1f MB)%n", 
        file.getName(), file.length() / 1024.0 / 1024.0);
  }

  private void printSummaryInsights() {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("PERFORMANCE ANALYSIS SUMMARY");
    System.out.println("=".repeat(80));
    
    System.out.println("\n## Key Insights ##");
    System.out.println("1. COLD START: Includes schema setup + first-time cache conversion");
    System.out.println("   - LINQ4J: Direct CSV parsing (consistent times)");
    System.out.println("   - PARQUET: Conversion overhead dominates performance");
    System.out.println("   - VECTORIZED: Similar to LINQ4J for cold start");
    
    System.out.println("\n2. WARM START: Schema setup with pre-built cache");
    System.out.println("   - LINQ4J: Still parses CSV (similar to cold start)");
    System.out.println("   - PARQUET: Uses cache (should be much faster)");
    System.out.println("   - VECTORIZED: Consistent with cold start");
    
    System.out.println("\n3. PURE QUERY: Only query execution time");
    System.out.println("   - Shows true engine performance differences");
    System.out.println("   - Eliminates connection and schema setup overhead");
    System.out.println("   - Best measure of actual query processing speed");
    
    System.out.println("\n## Recommendations ##");
    System.out.println("• Use COLD START times for: First-time usage scenarios");
    System.out.println("• Use WARM START times for: Application restart scenarios");
    System.out.println("• Use PURE QUERY times for: Production query performance");
  }

  static class PerformanceResult {
    final boolean success;
    final long avgTime;
    final String error;
    
    PerformanceResult(boolean success, long avgTime, String error) {
      this.success = success;
      this.avgTime = avgTime;
      this.error = error;
    }
  }
}