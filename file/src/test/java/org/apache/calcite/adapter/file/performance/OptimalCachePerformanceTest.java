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

import org.apache.calcite.adapter.file.FileAdapterTests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance test demonstrating optimal cache priming strategy.
 * Shows the difference between cold and warm cache performance.
 */
public class OptimalCachePerformanceTest extends PerformanceTestBase {
  
  @Override
  protected String getJdbcUrl() {
    return "jdbc:calcite:";
  }
  
  @Override
  protected String[] getSchemaNames() {
    return new String[] { "sales", "products", "customers" };
  }
  
  @BeforeAll
  static void setup() throws Exception {
    // Create test instance to initialize connection
    OptimalCachePerformanceTest test = new OptimalCachePerformanceTest();
    test.initializeConnection();
  }
  
  @Test
  void testVectorizedReaderWithOptimalCache() throws Exception {
    printTestHeader("Vectorized Reader with Optimal Cache");
    
    String query = "SELECT COUNT(*), SUM(amount) FROM sales.transactions WHERE year = 2024";
    
    // Compare vectorized vs non-vectorized with warm cache
    compareConfigurations(
        query,
        "Record-by-record", 
        () -> System.setProperty("parquet.enable.vectorized.reader", "false"),
        "Vectorized", 
        () -> System.setProperty("parquet.enable.vectorized.reader", "true")
    );
  }
  
  @Test
  void testComplexAggregationWithCache() throws Exception {
    printTestHeader("Complex Aggregation with Cache");
    
    String query = 
        "SELECT category, " +
        "       COUNT(DISTINCT customer_id) as unique_customers, " +
        "       SUM(amount) as total_sales, " +
        "       AVG(amount) as avg_sale " +
        "FROM sales.transactions " +
        "GROUP BY category " +
        "HAVING COUNT(*) > 100";
    
    QueryTiming timing = measureQuery(query);
    timing.print();
    
    // Verify cache benefit
    assertTrue(timing.warmTimeMs < timing.coldTimeMs, 
        "Warm cache should be faster than cold");
    
    System.out.println("\nCache benefit: " + 
        (timing.coldTimeMs - timing.warmTimeMs) + " ms saved per query");
  }
  
  @Test
  void testJoinPerformanceWithCache() throws Exception {
    printTestHeader("Join Performance with Cache");
    
    String query = 
        "SELECT p.name, SUM(t.amount) as revenue " +
        "FROM sales.transactions t " +
        "JOIN products.catalog p ON t.product_id = p.id " +
        "WHERE t.year = 2024 " +
        "GROUP BY p.name " +
        "ORDER BY revenue DESC " +
        "LIMIT 10";
    
    // Measure with warm cache (typical production scenario)
    long warmTime = measureWarmQuery(query);
    
    System.out.println("Join query execution time (warm cache): " + warmTime + " ms");
    
    // Now test cold cache for comparison
    clearAllStatisticsCaches();
    long coldTime = measureWarmQuery(query); // First execution loads cache
    
    System.out.println("Join query execution time (cold cache): " + coldTime + " ms");
    System.out.println("Statistics loading overhead: " + (coldTime - warmTime) + " ms");
    System.out.println("Performance improvement with cache: " + 
        String.format("%.1f%%", 100.0 * (coldTime - warmTime) / coldTime));
  }
  
  @Test
  void testCachePrimingEffectiveness() throws Exception {
    printTestHeader("Cache Priming Effectiveness");
    
    // Test queries of different complexities
    String[] queries = {
        "SELECT COUNT(*) FROM sales.transactions",
        "SELECT product_id, SUM(amount) FROM sales.transactions GROUP BY product_id",
        "SELECT * FROM sales.transactions WHERE amount > 1000 LIMIT 100",
        "SELECT t.*, p.name FROM sales.transactions t " +
        "JOIN products.catalog p ON t.product_id = p.id LIMIT 50"
    };
    
    System.out.println("Testing " + queries.length + " queries with primed cache:\n");
    
    long totalWarmTime = 0;
    long totalColdTime = 0;
    
    for (String query : queries) {
      QueryTiming timing = measureQuery(query);
      totalWarmTime += timing.avgWarmTimeMs;
      totalColdTime += timing.coldTimeMs;
      
      System.out.println("Query: " + 
          (query.length() > 50 ? query.substring(0, 50) + "..." : query));
      System.out.println("  Cold: " + timing.coldTimeMs + 
                        " ms, Warm: " + timing.avgWarmTimeMs + " ms");
    }
    
    System.out.println("\n=== Summary ===");
    System.out.println("Total cold time: " + totalColdTime + " ms");
    System.out.println("Total warm time: " + totalWarmTime + " ms");
    System.out.println("Cache benefit: " + (totalColdTime - totalWarmTime) + " ms");
    System.out.println("Overall speedup: " + 
        String.format("%.2fx", (double) totalColdTime / totalWarmTime));
  }
  
  @Test
  void testLargeTableBenefit() throws Exception {
    printTestHeader("Large Table Cache Benefit");
    
    // Assuming we have tables of different sizes
    String[] tableSizes = {"small", "medium", "large"};
    
    for (String size : tableSizes) {
      String query = String.format(
          "SELECT COUNT(*), AVG(amount) FROM sales.transactions_%s", size);
      
      try {
        QueryTiming timing = measureQuery(query);
        
        System.out.println("\nTable size: " + size);
        System.out.println("  Cold time: " + timing.coldTimeMs + " ms");
        System.out.println("  Warm time: " + timing.avgWarmTimeMs + " ms");
        System.out.println("  Cache benefit: " + 
            (timing.coldTimeMs - timing.avgWarmTimeMs) + " ms");
        
        // Larger tables should show more cache benefit
        if (size.equals("large")) {
          assertTrue(timing.getSpeedup() > 1.5, 
              "Large tables should show significant cache benefit");
        }
      } catch (Exception e) {
        System.out.println("  Table not found: " + e.getMessage());
      }
    }
  }
  
  /**
   * Helper to clear all statistics caches for cold-cache testing.
   */
  private void clearAllStatisticsCaches() throws Exception {
    org.apache.calcite.adapter.file.statistics.CachePrimer.clearAllCaches(connection);
    cachesPrimed = false;
  }
}