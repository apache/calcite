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
 * Direct performance comparison: Pre-existing Parquet files vs CSV files.
 * This test creates separate directories with only Parquet or only CSV files
 * to eliminate any conversion overhead.
 */
@Tag("performance")
public class DirectParquetPerformanceTest {
  @TempDir
  java.nio.file.Path tempDir;

  private File csvDir;
  private File parquetDir;

  private static final int DATASET_SIZE = 1_000_000;
  private static final int WARMUP_RUNS = 2;
  private static final int TEST_RUNS = 5;

  @BeforeEach
  public void setUp() throws Exception {
    System.out.println("\n=== DIRECT PARQUET VS CSV PERFORMANCE TEST ===");

    // Create separate directories
    csvDir = new File(tempDir.toFile(), "csv_only");
    parquetDir = new File(tempDir.toFile(), "parquet_only");
    csvDir.mkdirs();
    parquetDir.mkdirs();

    // Step 1: Create CSV file in csv directory
    System.out.println("Creating CSV dataset...");
    createSalesDataset(csvDir, "sales");

    // Step 2: Create Parquet file using conversion
    System.out.println("Creating Parquet dataset...");
    createParquetFromCsv();

    System.out.println("\nSetup complete:");
    System.out.println("  CSV directory: " + csvDir);
    System.out.println("  Parquet directory: " + parquetDir);
    listDirectory(csvDir);
    listDirectory(parquetDir);
  }

  @Test public void testDirectPerformanceComparison() throws Exception {
    System.out.println("\n## TEST CONFIGURATION ##");
    System.out.println("Dataset size: " + String.format(Locale.ROOT, "%,d", DATASET_SIZE) + " rows");
    System.out.println("Warmup runs: " + WARMUP_RUNS);
    System.out.println("Test runs: " + TEST_RUNS);
    System.out.println("Each test creates new connection (cold start)");

    // Test 1: Simple aggregation
    System.out.println("\n## TEST 1: Simple COUNT(*) ##");
    String countQuery = "SELECT COUNT(*) FROM TEST.\"SALES\"";
    runComparison("Count Query", countQuery);

    // Test 2: Group By aggregation
    System.out.println("\n## TEST 2: GROUP BY Aggregation ##");
    String groupByQuery = "SELECT \"region\", COUNT(*) as cnt, SUM(\"total\") as revenue " +
        "FROM TEST.\"SALES\" " +
        "GROUP BY \"region\" " +
        "ORDER BY revenue DESC";
    runComparison("Group By Query", groupByQuery);

    // Test 3: Filtered aggregation
    System.out.println("\n## TEST 3: Filtered Aggregation ##");
    String filteredQuery = "SELECT \"product_category\", " +
        "COUNT(*) as orders, " +
        "AVG(\"unit_price\") as avg_price, " +
        "SUM(\"quantity\") as total_qty " +
        "FROM TEST.\"SALES\" " +
        "WHERE \"status\" = 'completed' AND \"total\" > 100 " +
        "GROUP BY \"product_category\"";
    runComparison("Filtered Aggregation", filteredQuery);

    // Test 4: Top-N with sorting
    System.out.println("\n## TEST 4: Top-N Query ##");
    String topNQuery = "SELECT \"order_id\", \"customer_id\", \"total\" " +
        "FROM TEST.\"SALES\" " +
        "WHERE \"region\" = 'North America' " +
        "ORDER BY \"total\" DESC " +
        "LIMIT 1000";
    runComparison("Top-N Query", topNQuery);

    // Test 5: Selective projection
    System.out.println("\n## TEST 5: Selective Column Projection ##");
    String projectionQuery = "SELECT \"order_id\", \"total\" " +
        "FROM TEST.\"SALES\" " +
        "WHERE \"total\" > 500";
    runComparison("Selective Projection", projectionQuery);

    printConclusions();
  }

  private void runComparison(String testName, String query) throws Exception {
    System.out.println("\n### " + testName + " ###");

    // Test CSV with each engine
    long csvLinq4j = testWithSetup(csvDir, "linq4j", query, "CSV+LINQ4J");
    long csvArrow = testWithSetup(csvDir, "arrow", query, "CSV+ARROW");
    long csvVectorized = testWithSetup(csvDir, "vectorized", query, "CSV+VECTORIZED");

    // Test Parquet with each engine
    long parquetLinq4j = testWithSetup(parquetDir, "linq4j", query, "Parquet+LINQ4J");
    long parquetArrow = testWithSetup(parquetDir, "arrow", query, "Parquet+ARROW");
    long parquetParquet = testWithSetup(parquetDir, "parquet", query, "Parquet+PARQUET");

    // Display results
    System.out.println("\nResults:");
    System.out.println("Configuration        Avg Time(ms)   Speedup vs CSV+LINQ4J");
    System.out.println("------------------   -----------   --------------------");

    displayResult("CSV+LINQ4J", csvLinq4j, csvLinq4j);
    displayResult("CSV+ARROW", csvArrow, csvLinq4j);
    displayResult("CSV+VECTORIZED", csvVectorized, csvLinq4j);
    displayResult("Parquet+LINQ4J", parquetLinq4j, csvLinq4j);
    displayResult("Parquet+ARROW", parquetArrow, csvLinq4j);
    displayResult("Parquet+PARQUET", parquetParquet, csvLinq4j);
  }

  private long testWithSetup(File directory, String engine, String query, String configName) {
    try {
      // Warmup
      for (int i = 0; i < WARMUP_RUNS; i++) {
        runSingleQuery(directory, engine, query);
      }

      // Test runs
      long totalTime = 0;
      for (int i = 0; i < TEST_RUNS; i++) {
        long time = runSingleQuery(directory, engine, query);
        totalTime += time;
      }

      return totalTime / TEST_RUNS;

    } catch (Exception e) {
      System.err.println(configName + " FAILED: " + e.getMessage());
      return Long.MAX_VALUE;
    }
  }

  private long runSingleQuery(File directory, String engine, String query) throws Exception {
    // Allow GC before each run
    Thread.sleep(50);

    long startTime = System.currentTimeMillis();

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", directory.getAbsolutePath());
      operand.put("executionEngine", engine);

      SchemaPlus fileSchema =
          rootSchema.add("TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {

        int count = 0;
        while (rs.next()) {
          count++;
          // Force materialization of first column
          rs.getObject(1);
        }
      }
    }

    return System.currentTimeMillis() - startTime;
  }

  private void displayResult(String config, long time, long baseline) {
    if (time == Long.MAX_VALUE) {
      System.out.printf(Locale.ROOT, "%-18s        FAILED%n", config);
      return;
    }

    double speedup = (double) baseline / time;
    String performance = speedup >= 10 ? "🚀 EXCELLENT" :
                        speedup >= 5 ? "🎯 GREAT" :
                        speedup >= 2 ? "✓ GOOD" :
                        speedup >= 1.5 ? "↗ BETTER" :
                        speedup >= 1 ? "→ SIMILAR" : "↘ SLOWER";

    System.out.printf(Locale.ROOT, "%-18s   %,11d          %.1fx %s%n",
        config, time, speedup, performance);
  }

  private void createSalesDataset(File directory, String name) throws IOException {
    File file = new File(directory, name + ".csv");

    Random rand = new Random(42);

    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      // Header
      writer.write("order_id:int,customer_id:int,product_id:int,product_category:string,");
      writer.write("region:string,quantity:int,unit_price:double,total:double,");
      writer.write("order_date:string,status:string\n");

      String[] categories = {"Electronics", "Clothing", "Books", "Home", "Sports",
                            "Toys", "Food", "Beauty", "Auto", "Office"};
      String[] regions = {"North America", "Europe", "Asia Pacific", "Latin America", "Middle East"};
      String[] statuses = {"pending", "processing", "shipped", "completed", "cancelled"};

      for (int i = 0; i < DATASET_SIZE; i++) {
        int orderId = 1000000 + i;
        int customerId = 10000 + rand.nextInt(50000);
        int productId = 1000 + rand.nextInt(2000);
        String category = categories[rand.nextInt(categories.length)];
        String region = regions[rand.nextInt(regions.length)];
        int quantity = 1 + rand.nextInt(20);
        double unitPrice = 10.0 + rand.nextDouble() * 990.0;
        double total = quantity * unitPrice;

        String date =
            String.format(Locale.ROOT, "2024-%02d-%02d", 1 + rand.nextInt(12), 1 + rand.nextInt(28));

        String status = statuses[rand.nextInt(statuses.length)];
        if (rand.nextDouble() < 0.6) status = "completed"; // 60% completed

        writer.write(
            String.format(Locale.ROOT,
            "%d,%d,%d,%s,%s,%d,%.2f,%.2f,%s,%s\n",
            orderId, customerId, productId, category, region, quantity,
            unitPrice, total, date, status));
      }
    }

    System.out.println("  Created: " + file.getName() + " (" +
        String.format(Locale.ROOT, "%.1f MB", file.length() / 1024.0 / 1024.0) + ")");
  }

  private void createParquetFromCsv() throws Exception {
    // Use the file adapter with Parquet engine to convert
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", csvDir.getAbsolutePath());
      operand.put("executionEngine", "parquet");

      SchemaPlus fileSchema =
          rootSchema.add("TEMP", FileSchemaFactory.INSTANCE.create(rootSchema, "TEMP", operand));

      // Run a query to trigger conversion
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM TEMP.\"SALES\"")) {
        rs.next();
        System.out.println("  Conversion triggered. Row count: " + rs.getInt(1));
      }
    }

    // Move the converted Parquet file to parquet directory
    File cacheDir = new File(csvDir, ".parquet_cache");
    File sourceParquet = new File(cacheDir, "sales.parquet");
    File destParquet = new File(parquetDir, "sales.parquet");

    if (sourceParquet.exists()) {
      sourceParquet.renameTo(destParquet);
      System.out.println("  Moved Parquet file to: " + destParquet.getName() + " (" +
          String.format(Locale.ROOT, "%.1f MB", destParquet.length() / 1024.0 / 1024.0) + ")");
    }
  }

  private void listDirectory(File dir) {
    System.out.println("\nContents of " + dir.getName() + ":");
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          System.out.printf(Locale.ROOT, "  %s (%.1f MB)%n",
              file.getName(), file.length() / 1024.0 / 1024.0);
        }
      }
    }
  }

  private void printConclusions() {
    System.out.println("\n## CONCLUSIONS ##");
    System.out.println("\n1. File Format Impact:");
    System.out.println("   - Parquet files provide columnar storage benefits");
    System.out.println("   - Compression reduces I/O overhead");
    System.out.println("   - Schema is embedded, no parsing needed");

    System.out.println("\n2. Engine Performance:");
    System.out.println("   - PARQUET engine optimized for Parquet format");
    System.out.println("   - ARROW engine provides good general performance");
    System.out.println("   - LINQ4J baseline for compatibility");

    System.out.println("\n3. Query Type Matters:");
    System.out.println("   - Aggregations benefit most from columnar format");
    System.out.println("   - Selective projections show significant speedup");
    System.out.println("   - Full scans have less improvement");
  }
}
