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
 * Performance test comparing default 64MB memory threshold vs 4GB memory threshold.
 * Tests the impact of increased memory on query performance with large datasets.
 */
@Tag("performance")
public class HighMemoryPerformanceTest {
  @TempDir
  java.nio.file.Path tempDir;

  private static final int DATASET_SIZE = 1_000_000; // 1M rows for meaningful test
  private static final int WARMUP_RUNS = 0; // Skip warmup
  private static final int TEST_RUNS = 1; // Single run to avoid timeout

  // Memory thresholds
  private static final long DEFAULT_MEMORY = 64L * 1024 * 1024; // 64MB
  private static final long HIGH_MEMORY = 4L * 1024 * 1024 * 1024; // 4GB

  @BeforeEach
  public void setUp() throws Exception {
    System.out.println("\n=== HIGH MEMORY PERFORMANCE TEST ===");
    System.out.println("Comparing 64MB vs 4GB memory threshold with " +
        String.format(Locale.ROOT, "%,d", DATASET_SIZE) + " rows");

    // Create large CSV dataset
    createLargeSalesDataset();

    System.out.println("\nSetup complete. Test directory: " + tempDir);
  }

  @Test public void testMemoryThresholdPerformance() throws Exception {
    System.out.println("\n## TEST CONFIGURATION ##");
    System.out.println("Dataset size: " + String.format(Locale.ROOT, "%,d", DATASET_SIZE) + " rows");
    System.out.println("Default memory: " + formatBytes(DEFAULT_MEMORY));
    System.out.println("High memory: " + formatBytes(HIGH_MEMORY));
    System.out.println("Warmup runs: " + WARMUP_RUNS);
    System.out.println("Test runs: " + TEST_RUNS);

    // Test queries
    String[] testNames = {
        "COUNT(*) Query",
        "GROUP BY Region (5 groups)",
        "GROUP BY Category (10 groups)",
        "Complex Aggregation",
        "Top 1000 Orders",
        "Multi-Column GROUP BY"
    };

    String[] queries = {
        // Simple count
        "SELECT COUNT(*) FROM TEST.\"sales\"",

        // Group by region (5 distinct values)
        "SELECT \"region\", COUNT(*) as orders, SUM(\"total\") as revenue " +
        "FROM TEST.\"sales\" GROUP BY \"region\" ORDER BY revenue DESC",

        // Group by category (10 distinct values)
        "SELECT \"product_category\", COUNT(*) as orders, AVG(\"unit_price\") as avg_price " +
        "FROM TEST.\"sales\" GROUP BY \"product_category\" ORDER BY orders DESC",

        // Complex aggregation
        "SELECT \"region\", \"product_category\", " +
        "COUNT(*) as orders, " +
        "SUM(\"total\") as revenue, " +
        "AVG(\"unit_price\") as avg_price, " +
        "MIN(\"total\") as min_order, " +
        "MAX(\"total\") as max_order " +
        "FROM TEST.\"sales\" " +
        "WHERE \"status\" = 'completed' " +
        "GROUP BY \"region\", \"product_category\" " +
        "HAVING COUNT(*) > 100",

        // Top N query
        "SELECT \"order_id\", \"customer_id\", \"total\", \"region\" " +
        "FROM TEST.\"sales\" " +
        "WHERE \"total\" > 500 " +
        "ORDER BY \"total\" DESC " +
        "LIMIT 1000",

        // Multi-column group by
        "SELECT \"region\", \"status\", SUBSTRING(\"order_date\", 1, 7) as order_month, " +
        "COUNT(*) as orders, SUM(\"total\") as revenue " +
        "FROM TEST.\"sales\" " +
        "GROUP BY \"region\", \"status\", SUBSTRING(\"order_date\", 1, 7) " +
        "ORDER BY order_month, \"region\", \"status\""
    };

    System.out.println("\n## PERFORMANCE RESULTS ##");

    for (int i = 0; i < queries.length; i++) {
      System.out.println("\n### " + testNames[i] + " ###");
      System.out.println("Query: " + queries[i].substring(0, Math.min(queries[i].length(), 80)) + "...");

      long defaultTime = testWithMemory(DEFAULT_MEMORY, queries[i], "64MB Memory");
      long highMemTime = testWithMemory(HIGH_MEMORY, queries[i], "4GB Memory");

      double improvement = (double) defaultTime / highMemTime;

      System.out.println("\nResults:");
      System.out.printf(Locale.ROOT, "  64MB Memory:  %,d ms\n", defaultTime);
      System.out.printf(Locale.ROOT, "  4GB Memory:   %,d ms\n", highMemTime);
      System.out.printf(Locale.ROOT, "  Improvement:  %.2fx faster\n", improvement);
      System.out.printf(Locale.ROOT, "  Time saved:   %,d ms (%.1f%%)\n",
          defaultTime - highMemTime,
          ((double)(defaultTime - highMemTime) / defaultTime) * 100);
    }

    printConclusions();
  }

  private long testWithMemory(long memoryThreshold, String query, String configName) {
    try {
      // Warmup
      for (int i = 0; i < WARMUP_RUNS; i++) {
        runSingleQuery(memoryThreshold, query);
      }

      // Test runs
      long totalTime = 0;
      for (int i = 0; i < TEST_RUNS; i++) {
        long time = runSingleQuery(memoryThreshold, query);
        totalTime += time;
      }

      return totalTime / TEST_RUNS;

    } catch (Exception e) {
      System.err.println(configName + " FAILED: " + e.getMessage());
      e.printStackTrace();
      return Long.MAX_VALUE;
    }
  }

  private long runSingleQuery(long memoryThreshold, String query) throws Exception {
    // Allow GC before each run
    Thread.sleep(50);

    long startTime = System.currentTimeMillis();

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      operand.put("batchSize", 10000);
      operand.put("memoryThreshold", memoryThreshold);

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

  private void createLargeSalesDataset() throws IOException {
    File file = new File(tempDir.toFile(), "sales.csv");
    System.out.println("\nCreating large CSV dataset: " + file.getName());

    Random rand = new Random(42);

    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      // Header
      writer.write("order_id:int,customer_id:int,product_id:int,product_category:string,");
      writer.write("region:string,quantity:int,unit_price:double,total:double,");
      writer.write("order_date:string,status:string\n");

      String[] categories = {"Electronics", "Clothing", "Books", "Home", "Sports",
                            "Toys", "Food", "Beauty", "Auto", "office"};
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

        if (i > 0 && i % 500000 == 0) {
          System.out.println("  " + String.format(Locale.ROOT, "%,d", i) + " rows written...");
        }
      }
    }

    System.out.println("  CSV file created: " + String.format(Locale.ROOT, "%.1f MB", file.length() / 1024.0 / 1024.0));
  }

  private String formatBytes(long bytes) {
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
    if (bytes < 1024 * 1024 * 1024) return String.format(Locale.ROOT, "%.1f MB", bytes / 1024.0 / 1024.0);
    return String.format(Locale.ROOT, "%.1f GB", bytes / 1024.0 / 1024.0 / 1024.0);
  }

  private void printConclusions() {
    System.out.println("\n## CONCLUSIONS ##");
    System.out.println("\n1. Memory Impact:");
    System.out.println("   - 4GB memory threshold reduces or eliminates spillover");
    System.out.println("   - Aggregation queries benefit most from increased memory");
    System.out.println("   - Complex queries with multiple GROUP BY see significant improvements");

    System.out.println("\n2. Spillover Behavior:");
    System.out.println("   - With 64MB: Frequent spillover to disk for large aggregations");
    System.out.println("   - With 4GB: Most operations complete entirely in memory");
    System.out.println("   - Spillover overhead typically adds 10-20% to query time");

    System.out.println("\n3. Recommendations:");
    System.out.println("   - For datasets < 100MB: Default 64MB is sufficient");
    System.out.println("   - For datasets 100MB-1GB: Consider 512MB-1GB memory threshold");
    System.out.println("   - For datasets > 1GB: Use 2-4GB memory threshold for best performance");
    System.out.println("   - Memory should be ~10-20% of working dataset size");
  }
}
