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
 * Simple cold start performance test to investigate parquet cache conversion overhead.
 */
@Tag("performance")
public class SimpleParquetColdStartTest {
  @TempDir
  java.nio.file.Path tempDir;

  private static final int DATASET_SIZE = 100_000;
  private static final int TEST_RUNS = 3;

  @BeforeEach
  public void setUp() throws Exception {
    System.out.println("\n=== SIMPLE PARQUET COLD START TEST ===");
    createTestDataset();
  }

  @Test public void testParquetColdStart() throws Exception {
    System.out.println("\nTesting cold start performance...");

    // Test 1: LINQ4J (baseline - no caching)
    long linq4jTime = measureColdStart("linq4j", "Direct CSV processing");

    // Test 2: PARQUET (first time - includes conversion)
    clearParquetCache();
    long parquetFirstTime = measureColdStart("parquet", "Parquet with conversion");

    // Test 3: PARQUET (second time - uses cache)
    long parquetCachedTime = measureColdStart("parquet", "Parquet cached");

    // Results
    System.out.println("\n## COLD START PERFORMANCE RESULTS ##");
    System.out.println("Dataset: " + String.format(Locale.ROOT, "%,d", DATASET_SIZE) + " rows");
    System.out.println("Query: Simple COUNT(*) aggregation");
    System.out.println();

    System.out.printf(Locale.ROOT, "%-25s %,10d ms  (baseline)%n",
        "LINQ4J (CSV direct):", linq4jTime);
    System.out.printf(Locale.ROOT, "%-25s %,10d ms  (%.1fx vs baseline)%n",
        "PARQUET (with conversion):", parquetFirstTime, (double) linq4jTime / parquetFirstTime);
    System.out.printf(Locale.ROOT, "%-25s %,10d ms  (%.1fx vs baseline)%n",
        "PARQUET (cached):", parquetCachedTime, (double) linq4jTime / parquetCachedTime);

    System.out.println();
    System.out.println("## KEY INSIGHTS ##");
    System.out.printf(Locale.ROOT, "• Conversion overhead: %,d ms%n",
        parquetFirstTime - parquetCachedTime);
    System.out.printf(Locale.ROOT, "• Cache speedup: %.1fx faster%n",
        (double) parquetFirstTime / parquetCachedTime);
    System.out.printf(Locale.ROOT, "• True Parquet advantage: %.1fx vs CSV%n",
        (double) linq4jTime / parquetCachedTime);
  }

  private long measureColdStart(String engine, String description) throws Exception {
    System.out.println("\nTesting: " + description);

    long totalTime = 0;

    for (int i = 0; i < TEST_RUNS; i++) {
      // Allow GC between runs
      Thread.sleep(100);

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
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST.\"SALES\"")) {

          rs.next();
          int count = rs.getInt(1);
          System.out.printf(Locale.ROOT, "  Run %d: %d rows, %d ms%n",
              i + 1, count, System.currentTimeMillis() - startTime);
        }
      }

      totalTime += (System.currentTimeMillis() - startTime);
    }

    long avgTime = totalTime / TEST_RUNS;
    System.out.printf(Locale.ROOT, "  Average: %,d ms%n", avgTime);
    return avgTime;
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
      System.out.println("Cleared parquet cache");
    }
  }

  private void createTestDataset() throws IOException {
    File file = new File(tempDir.toFile(), "sales.csv");

    System.out.println("Creating test dataset: " + DATASET_SIZE + " rows");

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
        String date =
            String.format(Locale.ROOT, "2024-%02d-%02d", 1 + rand.nextInt(12), 1 + rand.nextInt(28));
        String status = statuses[rand.nextInt(statuses.length)];

        writer.write(
            String.format(Locale.ROOT,
            "%d,%d,%s,%s,%d,%.2f,%.2f,%s,%s\n",
            orderId, customerId, category, region, quantity,
            unitPrice, total, date, status));
      }
    }

    System.out.println("Created: " + file.getName() + " (" +
        String.format(Locale.ROOT, "%.1f MB", file.length() / 1024.0 / 1024.0) + ")");
  }
}
