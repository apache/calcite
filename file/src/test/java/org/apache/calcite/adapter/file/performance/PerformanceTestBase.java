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

import org.apache.calcite.adapter.file.statistics.CachePrimer;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Base class for performance tests that properly handles cache priming.
 * Ensures consistent and realistic performance measurements.
 */
public abstract class PerformanceTestBase {
  
  protected static Connection connection;
  protected static CalciteConnection calciteConnection;
  protected static boolean cachesPrimed = false;
  
  /**
   * Performance measurement result.
   */
  public static class QueryTiming {
    public final String query;
    public final long coldTimeMs;    // First execution (may include cache loading)
    public final long warmTimeMs;    // Second execution (cached)
    public final long avgWarmTimeMs; // Average of multiple warm executions
    public final int rowCount;
    
    public QueryTiming(String query, long coldTimeMs, long warmTimeMs, 
                       long avgWarmTimeMs, int rowCount) {
      this.query = query;
      this.coldTimeMs = coldTimeMs;
      this.warmTimeMs = warmTimeMs;
      this.avgWarmTimeMs = avgWarmTimeMs;
      this.rowCount = rowCount;
    }
    
    public double getSpeedup() {
      return warmTimeMs > 0 ? (double) coldTimeMs / warmTimeMs : 0;
    }
    
    public void print() {
      System.out.println("\nQuery: " + query);
      System.out.println("  Cold time:     " + coldTimeMs + " ms");
      System.out.println("  Warm time:     " + warmTimeMs + " ms");
      System.out.println("  Avg warm time: " + avgWarmTimeMs + " ms");
      System.out.println("  Rows returned: " + rowCount);
      System.out.println("  Cache speedup: " + String.format("%.2fx", getSpeedup()));
    }
  }
  
  /**
   * Override to provide the JDBC URL for testing.
   */
  protected abstract String getJdbcUrl();
  
  /**
   * Override to provide the schema name(s) to test.
   */
  protected abstract String[] getSchemaNames();
  
  /**
   * Override to enable/disable cache priming.
   * Default is true for realistic performance testing.
   */
  protected boolean shouldPrimeCaches() {
    return true;
  }
  
  /**
   * Override to set the number of warm-up iterations.
   * Default is 3.
   */
  protected int getWarmupIterations() {
    return 3;
  }
  
  /**
   * Override to set the number of measurement iterations.
   * Default is 5.
   */
  protected int getMeasurementIterations() {
    return 5;
  }
  
  @BeforeAll
  static void setupConnection() throws Exception {
    // Setup will be called by concrete test classes
  }
  
  protected void initializeConnection() throws Exception {
    Properties props = new Properties();
    props.setProperty("caseSensitive", "false");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("lex", "ORACLE");
    
    connection = DriverManager.getConnection(getJdbcUrl(), props);
    calciteConnection = connection.unwrap(CalciteConnection.class);
    
    // Prime caches if not already done
    if (shouldPrimeCaches() && !cachesPrimed) {
      primeAllCaches();
      cachesPrimed = true;
    }
  }
  
  @BeforeEach
  void ensureCachesPrimed() throws Exception {
    if (connection == null || connection.isClosed()) {
      initializeConnection();
    }
    
    // Ensure caches are primed for each test
    if (shouldPrimeCaches() && !cachesPrimed) {
      primeAllCaches();
      cachesPrimed = true;
    }
  }
  
  /**
   * Prime all statistics caches using the optimal strategy:
   * smallest to largest tables.
   */
  protected void primeAllCaches() throws Exception {
    System.out.println("\n=== Priming Statistics Caches ===");
    System.out.println("Strategy: Loading statistics from smallest to largest tables");
    System.out.println("This ensures larger tables remain in cache for testing\n");
    
    long startTime = System.currentTimeMillis();
    
    // Prime all specified schemas
    CachePrimer.PrimingResult result = 
        CachePrimer.primeSchemas(connection, getSchemaNames());
    
    long duration = System.currentTimeMillis() - startTime;
    
    result.printSummary();
    System.out.println("Total priming time: " + duration + " ms\n");
    
    // Additional warm-up queries to ensure everything is ready
    runWarmupQueries();
  }
  
  /**
   * Run warm-up queries to ensure JIT compilation and cache warming.
   */
  protected void runWarmupQueries() throws Exception {
    System.out.println("Running warm-up queries...");
    
    for (String schema : getSchemaNames()) {
      // Simple query to warm up each schema
      String warmupQuery = String.format(
          "SELECT COUNT(*) FROM %s.%s", 
          schema, getFirstTableName(schema)
      );
      
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(warmupQuery)) {
        rs.next();
      }
    }
    
    System.out.println("Warm-up complete\n");
  }
  
  /**
   * Measure query performance with proper cache handling.
   * 
   * @param query The query to measure
   * @return Timing information including cold and warm performance
   */
  protected QueryTiming measureQuery(String query) throws Exception {
    // Clear query result cache (but not statistics cache)
    clearQueryCache();
    
    // Cold execution (first run, may load statistics if not primed)
    long coldStart = System.currentTimeMillis();
    int rowCount = executeCountQuery(query);
    long coldTime = System.currentTimeMillis() - coldStart;
    
    // Warm execution (second run, uses cached statistics)
    long warmStart = System.currentTimeMillis();
    executeCountQuery(query);
    long warmTime = System.currentTimeMillis() - warmStart;
    
    // Multiple warm executions for average
    long totalWarmTime = 0;
    for (int i = 0; i < getMeasurementIterations(); i++) {
      long start = System.currentTimeMillis();
      executeCountQuery(query);
      totalWarmTime += System.currentTimeMillis() - start;
    }
    long avgWarmTime = totalWarmTime / getMeasurementIterations();
    
    return new QueryTiming(query, coldTime, warmTime, avgWarmTime, rowCount);
  }
  
  /**
   * Measure query performance with statistics already primed.
   * This is the recommended approach for most performance tests.
   * 
   * @param query The query to measure
   * @return Average execution time with warm cache
   */
  protected long measureWarmQuery(String query) throws Exception {
    // Warm-up iterations
    for (int i = 0; i < getWarmupIterations(); i++) {
      executeCountQuery(query);
    }
    
    // Measurement iterations
    long totalTime = 0;
    for (int i = 0; i < getMeasurementIterations(); i++) {
      long start = System.currentTimeMillis();
      executeCountQuery(query);
      totalTime += System.currentTimeMillis() - start;
    }
    
    return totalTime / getMeasurementIterations();
  }
  
  /**
   * Compare performance between two configurations.
   * Both measurements use warm cache for fair comparison.
   * 
   * @param query The query to test
   * @param config1Name Name of first configuration
   * @param config1Setup Setup for first configuration
   * @param config2Name Name of second configuration  
   * @param config2Setup Setup for second configuration
   */
  protected void compareConfigurations(String query, 
                                      String config1Name, Runnable config1Setup,
                                      String config2Name, Runnable config2Setup) 
      throws Exception {
    System.out.println("\n=== Configuration Comparison ===");
    System.out.println("Query: " + query);
    
    // Test configuration 1
    config1Setup.run();
    long time1 = measureWarmQuery(query);
    System.out.println(config1Name + ": " + time1 + " ms");
    
    // Test configuration 2
    config2Setup.run();
    long time2 = measureWarmQuery(query);
    System.out.println(config2Name + ": " + time2 + " ms");
    
    // Compare
    if (time1 > 0 && time2 > 0) {
      double improvement = (double) time1 / time2;
      if (improvement > 1) {
        System.out.println(config2Name + " is " + 
            String.format("%.2fx faster", improvement));
      } else {
        System.out.println(config1Name + " is " + 
            String.format("%.2fx faster", 1 / improvement));
      }
    }
  }
  
  /**
   * Execute query and count results.
   */
  protected int executeCountQuery(String query) throws Exception {
    int count = 0;
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        count++;
      }
    }
    return count;
  }
  
  /**
   * Clear query result cache without clearing statistics cache.
   */
  protected void clearQueryCache() {
    // This would clear any query-level caching
    // Statistics cache remains intact
    System.gc(); // Suggest GC to clear soft references
  }
  
  /**
   * Get the first table name in a schema (for warm-up queries).
   */
  @SuppressWarnings("deprecation")
  private String getFirstTableName(String schemaName) throws Exception {
    SchemaPlus schema = calciteConnection.getRootSchema().getSubSchema(schemaName);
    if (schema != null && !schema.getTableNames().isEmpty()) {
      return schema.getTableNames().iterator().next();
    }
    return "dual"; // fallback
  }
  
  /**
   * Print performance test header.
   */
  protected void printTestHeader(String testName) {
    System.out.println("\n" + "=".repeat(60));
    System.out.println("Performance Test: " + testName);
    System.out.println("=".repeat(60));
    System.out.println("Cache Strategy: " + 
        (shouldPrimeCaches() ? "Statistics pre-loaded (warm)" : "Cold start"));
    System.out.println("Warmup iterations: " + getWarmupIterations());
    System.out.println("Measurement iterations: " + getMeasurementIterations());
    System.out.println();
  }
}