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

/**
 * Comprehensive performance test comparing all execution engines:
 * LINQ4J, PARQUET, ARROW, and VECTORIZED.
 */
public class ComprehensiveEnginePerformanceTest {
  @TempDir
  java.nio.file.Path tempDir;

  private static final int WARMUP_RUNS = 2;
  private static final int TEST_RUNS = 5;

  @BeforeEach
  public void setUp() throws Exception {
    // Create test datasets
    createTestCsvFile(1000);
    createTestCsvFile(10000);
    createTestCsvFile(50000);
    createTestJsonFile(1000);
    createTestJsonFile(5000);
  }

  @Test public void testAllEnginesPerformance() throws Exception {
    System.out.println("\n=== COMPREHENSIVE FILE ADAPTER EXECUTION ENGINE PERFORMANCE TEST ===");
    System.out.println("Date: " + new java.util.Date());
    System.out.println("JVM: " + System.getProperty("java.version"));
    System.out.println("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.arch"));
    System.out.println("\nComparing: LINQ4J, PARQUET, ARROW, VECTORIZED");
    System.out.println("Warmup runs: " + WARMUP_RUNS);
    System.out.println("Test runs: " + TEST_RUNS + " (average time reported)");

    // Test CSV files of different sizes
    System.out.println("\n## CSV FILE PERFORMANCE ##");
    testEnginesOnFile("sales_1000.csv", "CSV", 1000);
    testEnginesOnFile("sales_10000.csv", "CSV", 10000);
    testEnginesOnFile("sales_50000.csv", "CSV", 50000);

    // Test JSON files
    System.out.println("\n## JSON FILE PERFORMANCE ##");
    testEnginesOnFile("orders_1000.json", "JSON", 1000);
    testEnginesOnFile("orders_5000.json", "JSON", 5000);

    // Summary
    System.out.println("\n## PERFORMANCE SUMMARY ##");
    System.out.println("1. PARQUET engine shows excellent performance due to:");
    System.out.println("   - Columnar storage format optimized for analytics");
    System.out.println("   - Built-in compression and encoding");
    System.out.println("   - Efficient data skipping and predicate pushdown");
    System.out.println("   - Auto-conversion caching for non-Parquet files");

    System.out.println("\n2. VECTORIZED engine provides good performance through:");
    System.out.println("   - Batch processing (default 2048 rows)");
    System.out.println("   - Reduced per-row overhead");
    System.out.println("   - Better CPU cache utilization");

    System.out.println("\n3. ARROW engine (when available) offers:");
    System.out.println("   - In-memory columnar format");
    System.out.println("   - Zero-copy reads");
    System.out.println("   - SIMD optimizations");

    System.out.println("\n4. LINQ4J (default) provides:");
    System.out.println("   - Simple row-by-row processing");
    System.out.println("   - Good compatibility");
    System.out.println("   - Baseline performance");
  }

  private void testEnginesOnFile(String fileName, String fileType, int rowCount) throws Exception {
    System.out.println("\n### " + fileType + " - " + rowCount + " rows (" + fileName + ")");

    Map<String, Long> engineTimes = new HashMap<>();

    // Test each engine
    String[] engines = {"linq4j", "parquet", "vectorized"}; // Arrow tested separately if available

    for (String engine : engines) {
      try {
        long avgTime = measureEnginePerformance(engine, fileName);
        engineTimes.put(engine, avgTime);
      } catch (Exception e) {
        System.out.println("  " + engine.toUpperCase() + ": FAILED - " + e.getMessage());
      }
    }

    // Try Arrow if available
    try {
      long avgTime = measureEnginePerformance("arrow", fileName);
      engineTimes.put("arrow", avgTime);
    } catch (Exception e) {
      // Arrow not available, skip
    }

    // Display results
    Long linq4jTime = engineTimes.get("linq4j");
    if (linq4jTime != null) {
      System.out.println("\nResults:");
      for (Map.Entry<String, Long> entry : engineTimes.entrySet()) {
        String engine = entry.getKey();
        Long time = entry.getValue();
        double speedup = (double) linq4jTime / time;
        System.out.printf(Locale.ROOT, "  %-12s: %,6d ms (%.2fx speedup vs LINQ4J)\n",
            engine.toUpperCase(), time, speedup);
      }
    }
  }

  private long measureEnginePerformance(String engine, String fileName) throws Exception {
    // Warmup runs
    for (int i = 0; i < WARMUP_RUNS; i++) {
      runQuery(engine, fileName);
    }

    // Test runs
    long totalTime = 0;
    for (int i = 0; i < TEST_RUNS; i++) {
      long startTime = System.currentTimeMillis();
      int rowCount = runQuery(engine, fileName);
      long endTime = System.currentTimeMillis();
      totalTime += (endTime - startTime);
    }

    return totalTime / TEST_RUNS;
  }

  private int runQuery(String engine, String fileName) throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Configure file schema with specified execution engine
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", engine);

      SchemaPlus fileSchema =
          rootSchema.add("TEST_" + engine.toUpperCase(), FileSchemaFactory.INSTANCE.create(rootSchema, "TEST_" + engine.toUpperCase(), operand));

      String tableName = fileName.substring(0, fileName.lastIndexOf('.'));
      String query;

      if (fileName.endsWith(".csv")) {
        // Aggregate query for CSV
        query = String.format("SELECT \"category\", COUNT(*) as cnt, " +
            "SUM(\"quantity\") as total_qty, AVG(\"unit_price\") as avg_price " +
            "FROM TEST_%s.\"%s\" " +
            "GROUP BY \"category\" " +
            "ORDER BY cnt DESC", engine.toUpperCase(), tableName);
      } else {
        // Filter query for JSON
        query = String.format("SELECT * FROM TEST_%s.\"%s\" " +
            "WHERE \"status\" = 'shipped' AND \"amount\" > 100",
            engine.toUpperCase(), tableName);
      }

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {

        int count = 0;
        while (rs.next()) {
          count++;
          // Force materialization of results
          if (fileName.endsWith(".csv")) {
            rs.getString("category");
            rs.getInt("cnt");
            rs.getDouble("total_qty");
            rs.getDouble("avg_price");
          } else {
            rs.getInt("order_id");
            rs.getDouble("amount");
          }
        }
        return count;
      }
    }
  }

  private void createTestCsvFile(int rows) throws IOException {
    File file = new File(tempDir.toFile(), "sales_" + rows + ".csv");

    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write("order_id:int,customer_id:int,product_id:int,category:string,");
      writer.write("quantity:int,unit_price:double,total:double,order_date:string,status:string\n");

      String[] categories = {"Electronics", "Clothing", "Books", "Home", "Sports", "Toys", "Food", "Beauty"};
      String[] statuses = {"pending", "shipped", "delivered", "cancelled"};

      for (int i = 0; i < rows; i++) {
        int customerId = 1000 + (i % 500);
        int productId = 1 + (i % 100);
        String category = categories[i % categories.length];
        int quantity = 1 + (i % 10);
        double unitPrice = 10.0 + (i % 90);
        double total = quantity * unitPrice;
        String date = String.format("2024-01-%02d", (i % 28) + 1);
        String status = statuses[i % statuses.length];

        writer.write(
            String.format(Locale.ROOT, "%d,%d,%d,%s,%d,%.2f,%.2f,%s,%s\n",
            i, customerId, productId, category, quantity, unitPrice, total, date, status));
      }
    }
  }

  private void createTestJsonFile(int rows) throws IOException {
    File file = new File(tempDir.toFile(), "orders_" + rows + ".json");

    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write("[\n");

      String[] statuses = {"pending", "shipped", "delivered", "cancelled"};

      for (int i = 0; i < rows; i++) {
        if (i > 0) writer.write(",\n");

        int orderId = 10000 + i;
        int customerId = 1000 + (i % 500);
        double amount = 50.0 + (i % 200) * 5;
        String status = statuses[i % statuses.length];
        String date = String.format("2024-01-%02d", (i % 28) + 1);

        writer.write(
            String.format(Locale.ROOT,
            "  {\"order_id\": %d, \"customer_id\": %d, \"amount\": %.2f, " +
            "\"status\": \"%s\", \"order_date\": \"%s\"}",
            orderId, customerId, amount, status, date));
      }

      writer.write("\n]\n");
    }
  }
}
