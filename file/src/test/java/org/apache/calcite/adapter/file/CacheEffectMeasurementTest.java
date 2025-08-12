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

import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.table.CsvScannableTable;
import org.apache.calcite.adapter.file.table.ParquetTranslatableTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to measure the actual effect of statistics caching on performance.
 * Demonstrates the importance of cache priming strategy.
 */
public class CacheEffectMeasurementTest {

  @TempDir
  static File tempDir;
  
  static File smallTable1;
  static File smallTable2;
  static File mediumTable;
  static File largeTable;
  static Map<String, Table> tables;
  
  @BeforeAll
  static void createTestTables() throws Exception {
    System.out.println("\n=== Creating Test Tables ===");
    
    // Create different sized tables
    smallTable1 = createTable("small1", 100);      // ~10 KB
    smallTable2 = createTable("small2", 500);      // ~50 KB
    mediumTable = createTable("medium", 5000);     // ~500 KB
    largeTable = createTable("large", 50000);      // ~5 MB
    
    // Convert to Parquet
    File cacheDir = new File(tempDir, ".parquet_cache");
    cacheDir.mkdirs();
    
    tables = new HashMap<>();
    tables.put("small1", new ParquetTranslatableTable(
        convertToParquet(smallTable1, "small1", cacheDir)));
    tables.put("small2", new ParquetTranslatableTable(
        convertToParquet(smallTable2, "small2", cacheDir)));
    tables.put("medium", new ParquetTranslatableTable(
        convertToParquet(mediumTable, "medium", cacheDir)));
    tables.put("large", new ParquetTranslatableTable(
        convertToParquet(largeTable, "large", cacheDir)));
    
    System.out.println("Created tables:");
    System.out.println("  small1: " + formatSize(smallTable1.length()));
    System.out.println("  small2: " + formatSize(smallTable2.length()));
    System.out.println("  medium: " + formatSize(mediumTable.length()));
    System.out.println("  large:  " + formatSize(largeTable.length()));
  }
  
  @Test
  void measureColdVsWarmCache() throws Exception {
    System.out.println("\n=== Test 1: Cold vs Warm Cache Performance ===");
    
    try (Connection conn = createConnection()) {
      String query = "SELECT COUNT(*), SUM(value), AVG(value) FROM test.large";
      
      // Measure cold cache (first query)
      clearAllCaches();
      long coldStart = System.nanoTime();
      int coldCount = executeQuery(conn, query);
      long coldTime = (System.nanoTime() - coldStart) / 1_000_000;
      
      // Measure warm cache (second query)
      long warmStart = System.nanoTime();
      int warmCount = executeQuery(conn, query);
      long warmTime = (System.nanoTime() - warmStart) / 1_000_000;
      
      // Measure hot cache (multiple queries)
      long totalHotTime = 0;
      for (int i = 0; i < 10; i++) {
        long start = System.nanoTime();
        executeQuery(conn, query);
        totalHotTime += (System.nanoTime() - start) / 1_000_000;
      }
      long avgHotTime = totalHotTime / 10;
      
      // Results
      System.out.println("\nResults for large table query:");
      System.out.println("  Cold cache (1st query):  " + coldTime + " ms");
      System.out.println("  Warm cache (2nd query):  " + warmTime + " ms");
      System.out.println("  Hot cache (avg of 10):   " + avgHotTime + " ms");
      System.out.println("\nCache loading overhead:    " + (coldTime - warmTime) + " ms");
      System.out.println("Cache speedup:             " + 
          String.format("%.2fx", (double) coldTime / warmTime));
      
      // Verify cache is working
      assertTrue(warmTime < coldTime, 
          "Warm cache should be faster than cold cache");
      assertTrue(avgHotTime <= warmTime, 
          "Hot cache should be at least as fast as warm cache");
    }
  }
  
  @Test
  void measureSmallToLargeVsLargeToSmall() throws Exception {
    System.out.println("\n=== Test 2: Cache Priming Order Effect ===");
    
    // Test 1: Prime small to large
    System.out.println("\n--- Strategy 1: Small to Large ---");
    long smallToLargeTime = measureWithPrimingStrategy(true);
    
    // Test 2: Prime large to small  
    System.out.println("\n--- Strategy 2: Large to Small ---");
    long largeToSmallTime = measureWithPrimingStrategy(false);
    
    // Compare results
    System.out.println("\n=== Comparison ===");
    System.out.println("Small→Large total time: " + smallToLargeTime + " ms");
    System.out.println("Large→Small total time: " + largeToSmallTime + " ms");
    
    if (smallToLargeTime < largeToSmallTime) {
      System.out.println("Winner: Small→Large is " + 
          String.format("%.1f%% faster", 
              100.0 * (largeToSmallTime - smallToLargeTime) / largeToSmallTime));
    } else {
      System.out.println("Winner: Large→Small is " + 
          String.format("%.1f%% faster",
              100.0 * (smallToLargeTime - largeToSmallTime) / smallToLargeTime));
    }
  }
  
  @Test
  void measureStatisticsLoadingCost() throws Exception {
    System.out.println("\n=== Test 3: Statistics Loading Cost by Table Size ===");
    
    Map<String, Long> loadingTimes = new LinkedHashMap<>();
    
    try (Connection conn = createConnection()) {
      for (String tableName : Arrays.asList("small1", "small2", "medium", "large")) {
        clearAllCaches();
        
        // First query loads statistics
        String query = "SELECT COUNT(*) FROM test." + tableName;
        long start = System.nanoTime();
        executeQuery(conn, query);
        long firstTime = (System.nanoTime() - start) / 1_000_000;
        
        // Second query uses cache
        start = System.nanoTime();
        executeQuery(conn, query);
        long secondTime = (System.nanoTime() - start) / 1_000_000;
        
        long loadingTime = firstTime - secondTime;
        loadingTimes.put(tableName, loadingTime);
        
        System.out.println(tableName + ":");
        System.out.println("  First query:  " + firstTime + " ms");
        System.out.println("  Second query: " + secondTime + " ms");
        System.out.println("  Loading cost: " + loadingTime + " ms");
      }
    }
    
    // Verify larger tables have higher loading cost
    assertTrue(loadingTimes.get("large") >= loadingTimes.get("small1"),
        "Large tables should have higher statistics loading cost");
  }
  
  @Test
  void measureMultipleQueriesBenefit() throws Exception {
    System.out.println("\n=== Test 4: Cumulative Benefit Over Multiple Queries ===");
    
    String[] queries = {
        "SELECT COUNT(*) FROM test.large",
        "SELECT SUM(value) FROM test.large WHERE category = 'A'",
        "SELECT category, COUNT(*) FROM test.large GROUP BY category",
        "SELECT AVG(value) FROM test.large WHERE id > 25000",
        "SELECT MAX(value) - MIN(value) FROM test.large"
    };
    
    try (Connection conn = createConnection()) {
      // Cold cache run
      clearAllCaches();
      long coldTotal = 0;
      System.out.println("\nCold cache runs:");
      for (int i = 0; i < queries.length; i++) {
        if (i > 0) clearAllCaches(); // Clear cache between queries
        long start = System.nanoTime();
        executeQuery(conn, queries[i]);
        long time = (System.nanoTime() - start) / 1_000_000;
        coldTotal += time;
        System.out.println("  Query " + (i+1) + ": " + time + " ms");
      }
      
      // Warm cache run (prime once, run all)
      clearAllCaches();
      executeQuery(conn, "SELECT 1 FROM test.large LIMIT 1"); // Prime cache
      
      long warmTotal = 0;
      System.out.println("\nWarm cache runs:");
      for (int i = 0; i < queries.length; i++) {
        long start = System.nanoTime();
        executeQuery(conn, queries[i]);
        long time = (System.nanoTime() - start) / 1_000_000;
        warmTotal += time;
        System.out.println("  Query " + (i+1) + ": " + time + " ms");
      }
      
      System.out.println("\n=== Summary ===");
      System.out.println("Total time (cold cache): " + coldTotal + " ms");
      System.out.println("Total time (warm cache): " + warmTotal + " ms");
      System.out.println("Time saved: " + (coldTotal - warmTotal) + " ms");
      System.out.println("Speedup: " + String.format("%.2fx", (double) coldTotal / warmTotal));
      System.out.println("\nFor " + queries.length + " queries:");
      System.out.println("  Average savings per query: " + 
          ((coldTotal - warmTotal) / queries.length) + " ms");
      System.out.println("  Cache ROI: " + 
          String.format("%.0f%%", 100.0 * (coldTotal - warmTotal) / coldTotal));
    }
  }
  
  @Test
  void measureVectorizedWithCacheEffect() throws Exception {
    System.out.println("\n=== Test 5: Vectorized Reading + Cache Effect ===");
    
    String query = "SELECT COUNT(*), SUM(value), AVG(value) FROM test.large WHERE value > 50";
    
    try (Connection conn = createConnection()) {
      // Test all 4 combinations
      Map<String, Long> results = new LinkedHashMap<>();
      
      // 1. Cold + Non-vectorized
      clearAllCaches();
      System.setProperty("parquet.enable.vectorized.reader", "false");
      long start = System.nanoTime();
      executeQuery(conn, query);
      results.put("Cold + Record-by-record", (System.nanoTime() - start) / 1_000_000);
      
      // 2. Warm + Non-vectorized
      start = System.nanoTime();
      executeQuery(conn, query);
      results.put("Warm + Record-by-record", (System.nanoTime() - start) / 1_000_000);
      
      // 3. Cold + Vectorized
      clearAllCaches();
      System.setProperty("parquet.enable.vectorized.reader", "true");
      start = System.nanoTime();
      executeQuery(conn, query);
      results.put("Cold + Vectorized", (System.nanoTime() - start) / 1_000_000);
      
      // 4. Warm + Vectorized
      start = System.nanoTime();
      executeQuery(conn, query);
      results.put("Warm + Vectorized", (System.nanoTime() - start) / 1_000_000);
      
      // Display results
      System.out.println("\nResults:");
      for (Map.Entry<String, Long> entry : results.entrySet()) {
        System.out.println("  " + entry.getKey() + ": " + entry.getValue() + " ms");
      }
      
      // Calculate improvements
      long cacheImprovement = results.get("Cold + Record-by-record") - 
                              results.get("Warm + Record-by-record");
      long vectorizedImprovement = results.get("Warm + Record-by-record") - 
                                   results.get("Warm + Vectorized");
      long totalImprovement = results.get("Cold + Record-by-record") - 
                             results.get("Warm + Vectorized");
      
      System.out.println("\n=== Improvement Breakdown ===");
      System.out.println("Cache contribution:      " + cacheImprovement + " ms");
      System.out.println("Vectorized contribution: " + vectorizedImprovement + " ms");
      System.out.println("Total improvement:       " + totalImprovement + " ms");
      System.out.println("\nBest case speedup: " + 
          String.format("%.2fx", 
              (double) results.get("Cold + Record-by-record") / 
                      results.get("Warm + Vectorized")));
    }
  }
  
  // Helper methods
  
  private static File createTable(String name, int rows) throws Exception {
    File csvFile = new File(tempDir, name + ".csv");
    StringBuilder csv = new StringBuilder();
    csv.append("id,name,category,value,timestamp\n");
    
    Random rand = new Random(42); // Consistent seed
    String[] categories = {"A", "B", "C", "D", "E"};
    
    for (int i = 1; i <= rows; i++) {
      csv.append(i).append(",");
      csv.append("Name_").append(i).append(",");
      csv.append(categories[rand.nextInt(categories.length)]).append(",");
      csv.append(rand.nextInt(100)).append(",");
      csv.append("2024-01-").append(String.format("%02d", (i % 28) + 1));
      csv.append(" 12:00:00\n");
    }
    
    Files.write(csvFile.toPath(), csv.toString().getBytes());
    return csvFile;
  }
  
  private static File convertToParquet(File csvFile, String tableName, File cacheDir) 
      throws Exception {
    Source source = Sources.of(csvFile);
    Table csvTable = new CsvScannableTable(source, null);
    return ParquetConversionUtil.convertToParquet(
        source, tableName, csvTable, cacheDir, null, "test");
  }
  
  private Connection createConnection() throws Exception {
    Connection conn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConn.getRootSchema();
    
    rootSchema.add("test", new org.apache.calcite.schema.impl.AbstractSchema() {
      @Override
      protected Map<String, Table> getTableMap() {
        return tables;
      }
    });
    
    return conn;
  }
  
  private int executeQuery(Connection conn, String query) throws Exception {
    int count = 0;
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        count++;
      }
    }
    return count;
  }
  
  private void clearAllCaches() {
    // Clear in-memory statistics cache
    for (Table table : tables.values()) {
      if (table instanceof ParquetTranslatableTable) {
        try {
          java.lang.reflect.Field cacheField = 
              ParquetTranslatableTable.class.getDeclaredField("cachedStatistics");
          cacheField.setAccessible(true);
          cacheField.set(table, null);
        } catch (Exception e) {
          // Ignore
        }
      }
    }
    System.gc(); // Suggest GC
  }
  
  private long measureWithPrimingStrategy(boolean smallToLarge) throws Exception {
    clearAllCaches();
    
    List<String> tableOrder = smallToLarge ?
        Arrays.asList("small1", "small2", "medium", "large") :
        Arrays.asList("large", "medium", "small2", "small1");
    
    long totalTime = 0;
    
    try (Connection conn = createConnection()) {
      // Prime caches in specified order
      System.out.println("Priming order: " + tableOrder);
      for (String table : tableOrder) {
        long start = System.nanoTime();
        executeQuery(conn, "SELECT COUNT(*) FROM test." + table);
        long time = (System.nanoTime() - start) / 1_000_000;
        System.out.println("  Prime " + table + ": " + time + " ms");
      }
      
      // Now run queries on all tables
      System.out.println("\nRunning queries after priming:");
      for (String table : Arrays.asList("small1", "small2", "medium", "large")) {
        String query = "SELECT COUNT(*), SUM(value) FROM test." + table;
        long start = System.nanoTime();
        executeQuery(conn, query);
        long time = (System.nanoTime() - start) / 1_000_000;
        totalTime += time;
        System.out.println("  Query " + table + ": " + time + " ms");
      }
    }
    
    return totalTime;
  }
  
  private static String formatSize(long bytes) {
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return (bytes / 1024) + " KB";
    return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
  }
}