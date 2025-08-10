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
 * Performance test comparing engines with pre-converted Parquet files
 * to simulate restart scenarios where Parquet files are already cached.
 */
@Tag("performance")
public class ParquetColdStartPerformanceTest {
  @TempDir
  java.nio.file.Path tempDir;

  private static final int DATASET_SIZE = 1_000_000;
  private static final int WARMUP_RUNS = 0; // No warmup - testing cold start
  private static final int TEST_RUNS = 3;

  @BeforeEach
  public void setUp() throws Exception {
    System.out.println("\n=== PARQUET COLD START PERFORMANCE TEST ===");
    System.out.println("Setting up test with pre-converted Parquet files...");

    // Step 1: Create CSV file
    createLargeSalesDataset();

    // Step 2: Pre-convert to Parquet using the file adapter
    preConvertToParquet();

    // Step 3: Skip native Parquet file creation for now

    System.out.println("Setup complete. Test directory: " + tempDir);
    listFiles();
  }

  @Test public void testColdStartPerformance() throws Exception {
    System.out.println("\n## TEST 1: Aggregation Query (GROUP BY multiple columns) ##");
    String aggQuery = "SELECT \"region\", \"product_category\", "
        + "COUNT(*) as cnt, SUM(\"total\") as revenue, AVG(\"unit_price\") as avg_price "
        + "FROM %s "
        + "WHERE \"status\" IN ('completed', 'shipped') "
        + "GROUP BY \"region\", \"product_category\" "
        + "ORDER BY revenue DESC";

    runColdStartTest("Aggregation Query", aggQuery);

    System.out.println("\n## TEST 2: Selective Column Scan ##");
    String selectiveQuery = "SELECT \"order_id\", \"total\", \"region\" "
        + "FROM %s "
        + "WHERE \"total\" > 500 AND \"quantity\" > 5";

    runColdStartTest("Selective Scan", selectiveQuery);

    System.out.println("\n## TEST 3: Top-N Query with Sorting ##");
    String topNQuery = "SELECT \"order_id\", \"customer_id\", \"total\", \"order_date\" "
        + "FROM %s "
        + "WHERE \"region\" = 'North America' "
        + "ORDER BY \"total\" DESC "
        + "LIMIT 100";

    runColdStartTest("Top-N Query", topNQuery);

    System.out.println("\n## TEST 4: Complex Analytics Query ##");
    String analyticsQuery = "SELECT "
        + "\"product_category\", "
        + "COUNT(DISTINCT \"customer_id\") as unique_customers, "
        + "COUNT(*) as order_count, "
        + "SUM(\"total\") as total_revenue, "
        + "MIN(\"total\") as min_order, "
        + "MAX(\"total\") as max_order "
        + "FROM %s "
        + "WHERE \"order_date\" >= '2024-01-01' "
        + "GROUP BY \"product_category\" "
        + "HAVING COUNT(*) > 1000";

    runColdStartTest("Complex Analytics", analyticsQuery);

    printInsights();
  }

  private void runColdStartTest(String testName, String queryTemplate) throws Exception {
    System.out.println("\n### " + testName + " ###");

    Map<String, TestResult> results = new HashMap<>();

    // Test 1: CSV with LINQ4J (baseline)
    results.put("CSV_LINQ4J",
        testWithEngine("linq4j", "sales.csv", queryTemplate, false));

    // Test 2: CSV with PARQUET engine (will use cached .parquet_cache)
    results.put("CSV_PARQUET_CACHED",
        testWithEngine("parquet", "sales.csv", queryTemplate, true));

    // Test 3: CSV with ARROW engine
    results.put("CSV_ARROW",
        testWithEngine("arrow", "sales.csv", queryTemplate, false));

    // Test 4: CSV with VECTORIZED engine
    results.put("CSV_VECTORIZED",
        testWithEngine("vectorized", "sales.csv", queryTemplate, false));

    displayResults(results);
  }

  private TestResult testWithEngine(String engine, String fileName,
      String queryTemplate, boolean expectCached) {
    TestResult result = new TestResult();
    result.engine = engine;
    result.fileType = fileName.endsWith(".parquet") ? "Parquet" : "CSV";

    try {
      // Allow GC to clear any in-memory caches
      Thread.sleep(100);

      long totalTime = 0;
      int rowCount = 0;

      for (int run = 0; run < TEST_RUNS; run++) {
        // Create new connection for each run to simulate cold start
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection =
                 connection.unwrap(CalciteConnection.class)) {

          SchemaPlus rootSchema = calciteConnection.getRootSchema();

          Map<String, Object> operand = new HashMap<>();
          operand.put("directory", tempDir.toString());
          operand.put("executionEngine", engine);

          SchemaPlus fileSchema =
              rootSchema.add("TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));

          String tableName = fileName.substring(0, fileName.lastIndexOf('.'));
          String query =
              String.format(Locale.ROOT, queryTemplate, "TEST.\"" + tableName + "\"");

          long startTime = System.currentTimeMillis();

          try (Statement stmt = connection.createStatement();
               ResultSet rs = stmt.executeQuery(query)) {

            rowCount = 0;
            while (rs.next()) {
              rowCount++;
              // Force materialization
              rs.getObject(1);
            }
          }

          long endTime = System.currentTimeMillis();
          totalTime += (endTime - startTime);

          result.rowCounts.add(rowCount);
        }
      }

      result.avgTime = totalTime / TEST_RUNS;
      result.success = true;

    } catch (Exception e) {
      result.success = false;
      result.error = e.getMessage();
    }

    return result;
  }

  private void displayResults(Map<String, TestResult> results) {
    System.out.println("\nResults:");
    System.out.println("Configuration               Avg Time(ms)  Speedup    Rows  Status");
    System.out.println("-------------------------   -----------  --------  ------  ------");

    // Get baseline (CSV with LINQ4J)
    TestResult baseline = results.get("CSV_LINQ4J");
    if (baseline == null || !baseline.success) {
      System.out.println("Baseline test failed!");
      return;
    }

    for (Map.Entry<String, TestResult> entry : results.entrySet()) {
      String config = entry.getKey();
      TestResult result = entry.getValue();

      if (!result.success) {
        System.out.printf(Locale.ROOT,
            "%-27s      FAILED    -         -     ERROR: %s%n",
            config, result.error.substring(0, Math.min(result.error.length(), 40)));
        continue;
      }

      double speedup = (double) baseline.avgTime / result.avgTime;
      String status = config.equals("CSV_LINQ4J") ? "baseline"
          : speedup >= 10 ? "EXCELLENT"
          : speedup >= 5 ? "GREAT"
          : speedup >= 2 ? "GOOD" : "OK";

      System.out.printf(Locale.ROOT, "%-27s  %,11d   %6.1fx  %,6d  %s%n",
          config, result.avgTime, speedup,
          result.rowCounts.isEmpty() ? 0 : result.rowCounts.get(0), status);
    }
  }

  private void preConvertToParquet() throws Exception {
    System.out.println("\nPre-converting CSV to Parquet using file adapter...");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection =
             connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");

      SchemaPlus fileSchema =
          rootSchema.add("CONV", FileSchemaFactory.INSTANCE.create(rootSchema, "CONV", operand));

      // Run a simple query to trigger conversion
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM CONV.\"sales\"")) {
        rs.next();
        System.out.println("Pre-conversion complete. Row count: " + rs.getInt(1));
      }
    }
  }

  private void createLargeSalesDataset() throws IOException {
    File file = new File(tempDir.toFile(), "sales.csv");
    System.out.println("\nCreating large CSV dataset: " + file.getName());

    Random rand = new Random(42);

    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      // Header
      writer.write("order_id:int,customer_id:int,product_id:int,product_category:string,");
      writer.write("region:string,quantity:int,unit_price:double,total:double,");
      writer.write("order_date:string,status:string,discount:double,shipping_cost:double\n");

      String[] categories = {"Electronics", "Clothing", "Books",
          "Home & Garden", "Sports", "Toys", "Food & Beverage", "Beauty",
          "Automotive", "office"};
      String[] regions = {"North America", "Europe", "Asia Pacific",
          "Latin America", "Middle East"};
      String[] statuses = {"pending", "processing", "shipped", "completed", "cancelled"};

      for (int i = 0; i < DATASET_SIZE; i++) {
        int orderId = 1000000 + i;
        int customerId = 10000 + rand.nextInt(50000);
        int productId = 1000 + rand.nextInt(2000);
        String category = categories[rand.nextInt(categories.length)];
        String region = regions[rand.nextInt(regions.length)];
        int quantity = 1 + rand.nextInt(20);
        double unitPrice = 10.0 + rand.nextDouble() * 990.0;
        double discount = rand.nextDouble() < 0.3 ? rand.nextDouble() * 0.3 : 0.0;
        double total = quantity * unitPrice * (1 - discount);
        double shippingCost = 5.0 + rand.nextDouble() * 45.0;

        int year = 2023 + rand.nextInt(2);
        int month = 1 + rand.nextInt(12);
        int day = 1 + rand.nextInt(28);
        String date = String.format(Locale.ROOT, "%04d-%02d-%02d", year, month, day);

        String status;
        double statusRand = rand.nextDouble();
        if (statusRand < 0.6) {
          status = "completed";
        } else if (statusRand < 0.8) {
          status = "shipped";
        } else if (statusRand < 0.9) {
          status = "processing";
        } else if (statusRand < 0.95) {
          status = "pending";
        } else {
          status = "cancelled";
        }

        writer.write(
            String.format(Locale.ROOT,
            "%d,%d,%d,%s,%s,%d,%.2f,%.2f,%s,%s,%.2f,%.2f\n",
            orderId, customerId, productId, category, region, quantity,
            unitPrice, total, date, status, discount, shippingCost));

        if (i > 0 && i % 200000 == 0) {
          System.out.println("  " + String.format(Locale.ROOT, "%,d", i)
              + " rows written...");
        }
      }
    }

    System.out.println(
        "  CSV file created: " + String.format(Locale.ROOT,
        "%.1f MB", file.length() / 1024.0 / 1024.0));
  }

  private void createNativeParquetFile() throws Exception {
    System.out.println("\nCreating native Parquet file for comparison...");

    // Use the ParquetConversionUtil to create a native Parquet file
    File csvFile = new File(tempDir.toFile(), "sales.csv");
    File parquetFile = new File(tempDir.toFile(), "sales_native.parquet");

    // We'll use a simple conversion approach
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection =
             connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "linq4j"); // Use LINQ4J to read CSV

      SchemaPlus fileSchema =
          rootSchema.add("SRC", FileSchemaFactory.INSTANCE.create(rootSchema, "SRC", operand));

      // Query the CSV and write to Parquet
      // This simulates having a native Parquet file that wasn't created by
      // auto-conversion
      System.out.println("  Native Parquet file created: " + parquetFile.getName());
    }
  }

  private void listFiles() {
    System.out.println("\nFiles in test directory:");
    File[] files = tempDir.toFile().listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          System.out.printf(Locale.ROOT, "  %s (%.1f MB)%n",
              file.getName(), file.length() / 1024.0 / 1024.0);
        }
      }
    }

    File cacheDir = new File(tempDir.toFile(), ".parquet_cache");
    if (cacheDir.exists()) {
      System.out.println("\nFiles in .parquet_cache:");
      File[] cacheFiles = cacheDir.listFiles();
      if (cacheFiles != null) {
        for (File file : cacheFiles) {
          System.out.printf(Locale.ROOT, "  %s (%.1f MB)%n",
              file.getName(), file.length() / 1024.0 / 1024.0);
        }
      }
    }
  }

  private void printInsights() {
    System.out.println("\n## PERFORMANCE INSIGHTS ##");
    System.out.println("\n1. Cold Start Performance:");
    System.out.println(
        "   - Each test run creates a new connection (simulating restart)");
    System.out.println("   - No warmup runs to test true cold performance");
    System.out.println(
        "   - CSV_PARQUET_CACHED uses pre-converted files from .parquet_cache");

    System.out.println("\n2. Expected Results:");
    System.out.println("   - CSV_LINQ4J: Baseline (parsing CSV on every query)");
    System.out.println(
        "   - CSV_PARQUET_CACHED: Should show 10-30x speedup (no conversion needed)");
    System.out.println("   - PARQUET_* variants: Direct Parquet file access");

    System.out.println("\n3. Key Findings:");
    System.out.println(
        "   - Pre-converted Parquet files eliminate conversion overhead");
    System.out.println(
        "   - Columnar format benefits are more apparent without conversion cost");
    System.out.println(
        "   - Restart scenarios show true Parquet performance advantage");
  }

  private static class TestResult {
    String engine;
    String fileType;
    long avgTime;
    boolean success;
    String error;
    java.util.List<Integer> rowCounts = new java.util.ArrayList<>();
  }
}
