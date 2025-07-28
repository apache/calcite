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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

/**
 * Quick performance test for CsvNextGen adapter.
 * Generates realistic test data and measures key operations.
 */
public class QuickPerformanceTest {

  @Test public void runQuickPerformanceBenchmark(@TempDir File tempDir) throws IOException, SQLException {
    System.out.println("=".repeat(60));
    System.out.println("CSV NEXTGEN ADAPTER - PERFORMANCE BENCHMARK");
    System.out.println("=".repeat(60));

    // Create test data
    File testCsv = createRealisticTestData(tempDir, 50_000); // 50K rows

    // Setup connection
    Connection connection = setupConnection(tempDir);

    // Run benchmark tests
    runBenchmarkSuite(connection, testCsv);

    connection.close();
    System.out.println("\nBenchmark completed successfully!");
  }

  private File createRealisticTestData(File dir, int rowCount) throws IOException {
    File csvFile = new File(dir, "benchmark_data.csv");
    Random random = new Random(42); // Fixed seed for reproducible results

    System.out.format(Locale.ROOT, "Generating %d rows of test data...\n", rowCount);
    long startTime = System.currentTimeMillis();

    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      // Write header - realistic e-commerce data
      writer.write("order_id,customer_id,product_name,category,price,quantity,order_date,status," +
          "region\n");

      String[] products = {
          "Laptop Pro 15", "Wireless Mouse", "USB Cable", "Monitor 27in", "Keyboard Mechanical",
          "Phone Case", "Tablet 10in", "Headphones", "Webcam HD", "Speaker Bluetooth",
          "Charger Fast", "Memory Card", "External Drive", "Gaming Chair", "Desk Lamp"
      };

      String[] categories = {"Electronics", "Accessories", "Computers", "Audio", "Furniture"};
      String[] statuses = {"pending", "shipped", "delivered", "cancelled"};
      String[] regions = {"North", "South", "East", "West", "Central"};

      for (int i = 1; i <= rowCount; i++) {
        int customerId = 1000 + random.nextInt(5000);
        String product = products[random.nextInt(products.length)];
        String category = categories[random.nextInt(categories.length)];
        double price = 10.0 + (random.nextDouble() * 1000.0);
        int quantity = 1 + random.nextInt(5);
        String status = statuses[random.nextInt(statuses.length)];
        String region = regions[random.nextInt(regions.length)];
        int month = 1 + random.nextInt(12);
        int day = 1 + random.nextInt(28);

        writer.write(
            String.format(Locale.ROOT, "%d,%d,%s,%s,%.2f,%d,2023-%02d-%02d,%s,%s\n",
                i, customerId, product, category, price, quantity, month, day, status, region));

        if (i % 10000 == 0) {
          System.out.format(Locale.ROOT, "  Generated %d/%d rows\n", i, rowCount);
        }
      }
    }

    long duration = System.currentTimeMillis() - startTime;
    long fileSizeBytes = csvFile.length();
    double fileSizeMB = fileSizeBytes / 1024.0 / 1024.0;

    System.out.format(Locale.ROOT, "Created test file: %.2f MB (%d bytes) in %d ms\n",
        fileSizeMB, fileSizeBytes, duration);
    System.out.format(Locale.ROOT, "Generation rate: %.0f rows/sec\n\n",
        (rowCount * 1000.0) / duration);

    return csvFile;
  }

  private Connection setupConnection(File testDir) throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // Add CsvNextGen schema
    CsvNextGenSchemaSimple schema =
        new CsvNextGenSchemaSimple(testDir, "linq4j", 1024, true);
    rootSchema.add("csvnextgen", schema);

    System.out.println("Calcite connection established with CsvNextGen schema");
    return connection;
  }

  private void runBenchmarkSuite(Connection connection, File testFile) throws SQLException {
    System.out.println("BENCHMARK RESULTS");
    System.out.println("-".repeat(60));

    // 1. Basic table scan
    benchmarkQuery(connection,
        "SELECT COUNT(*) FROM BENCHMARK_DATA",
        "Full table scan (COUNT)");

    // 2. Simple projection
    benchmarkQuery(connection,
        "SELECT order_id, customer_id, price FROM BENCHMARK_DATA",
        "Simple projection (3 columns)");

    // 3. Filtering
    benchmarkQuery(connection,
        "SELECT COUNT(*) FROM BENCHMARK_DATA WHERE price > 100.0",
        "Filter by price > 100");

    // 4. String filtering
    benchmarkQuery(connection,
        "SELECT COUNT(*) FROM BENCHMARK_DATA WHERE category = 'Electronics'",
        "Filter by category");

    // 5. Complex filtering
    benchmarkQuery(connection,
        "SELECT COUNT(*) FROM BENCHMARK_DATA WHERE price BETWEEN 50.0 AND 500.0 AND quantity > 1",
        "Complex filter (price range + quantity)");

    // 6. Aggregation by group
    benchmarkQuery(connection,
        "SELECT category, COUNT(*), AVG(price) FROM BENCHMARK_DATA GROUP BY category",
        "GROUP BY category with aggregates");

    // 7. Sorting
    benchmarkQuery(connection,
        "SELECT * FROM BENCHMARK_DATA ORDER BY price DESC LIMIT 100",
        "ORDER BY price DESC LIMIT 100");

    // 8. Complex analytical query
    benchmarkQuery(connection,
        "SELECT region, category, COUNT(*) as orders, SUM(price * quantity) as revenue " +
            "FROM BENCHMARK_DATA " +
            "WHERE status IN ('shipped', 'delivered') " +
            "GROUP BY region, category " +
            "HAVING COUNT(*) > 10 " +
            "ORDER BY revenue DESC",
        "Complex analytical query");

    // 9. Memory efficiency test
    measureMemoryUsage(connection);

    System.out.println("-".repeat(60));
  }

  private void benchmarkQuery(Connection connection, String sql, String description) throws SQLException {
    System.out.format(Locale.ROOT, "%-35s: ", description);

    // Warm-up run
    executeQuery(connection, sql, false);

    // Measured runs
    long[] times = new long[3];
    long totalRows = 0;

    for (int i = 0; i < 3; i++) {
      long startTime = System.nanoTime();
      totalRows = executeQuery(connection, sql, true);
      long endTime = System.nanoTime();
      times[i] = (endTime - startTime) / 1_000_000; // Convert to milliseconds
    }

    // Calculate statistics
    long avgTime = (times[0] + times[1] + times[2]) / 3;
    long minTime = Math.min(times[0], Math.min(times[1], times[2]));
    long maxTime = Math.max(times[0], Math.max(times[1], times[2]));

    System.out.format(Locale.ROOT, "%4d ms (min: %d, max: %d)", avgTime, minTime, maxTime);

    if (totalRows > 0 && avgTime > 0) {
      long rowsPerSecond = (totalRows * 1000) / avgTime;
      System.out.format(Locale.ROOT, " | %d rows | %,.0f rows/sec", totalRows,
          (double) rowsPerSecond);
    }

    System.out.println();
  }

  private long executeQuery(Connection connection, String sql, boolean consumeResults) throws SQLException {
    long totalRows = 0;

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      if (consumeResults) {
        int columnCount = rs.getMetaData().getColumnCount();

        while (rs.next()) {
          totalRows++;
          // Access all columns to ensure full processing
          for (int i = 1; i <= columnCount; i++) {
            rs.getObject(i);
          }
        }
      } else {
        // Just consume the result set without counting
        while (rs.next()) {
          totalRows++;
        }
      }
    }

    return totalRows;
  }

  private void measureMemoryUsage(Connection connection) throws SQLException {
    Runtime runtime = Runtime.getRuntime();

    // Measure baseline memory
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }
    long baselineMemory = runtime.totalMemory() - runtime.freeMemory();

    // Execute memory-intensive query
    String sql = "SELECT * FROM BENCHMARK_DATA WHERE price > 100.0";
    executeQuery(connection, sql, true);

    // Measure memory after query
    long afterQueryMemory = runtime.totalMemory() - runtime.freeMemory();
    long memoryUsed = afterQueryMemory - baselineMemory;

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }
    long afterGcMemory = runtime.totalMemory() - runtime.freeMemory();
    long memoryRetained = afterGcMemory - baselineMemory;

    System.out.format(Locale.ROOT, "%-35s: %.2f MB used, %.2f MB retained\n",
        "Memory efficiency test",
        memoryUsed / 1024.0 / 1024.0,
        memoryRetained / 1024.0 / 1024.0);
  }
}
