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
 * Large-scale performance test for CsvNextGen adapter.
 * Tests with datasets of varying sizes to quantify scalability.
 */
public class LargePerformanceTest {

  @Test public void runLargeScalePerformanceBenchmark(@TempDir File tempDir) throws IOException, SQLException {
    System.out.println("=".repeat(80));
    System.out.println("CSV NEXTGEN ADAPTER - LARGE SCALE PERFORMANCE BENCHMARK");
    System.out.println("=".repeat(80));

    // Test different data sizes
    int[] dataSizes = {10_000, 50_000, 100_000, 500_000};

    for (int dataSize : dataSizes) {
      System.out.format(Locale.ROOT, "\n"
  + "=".repeat(80) + "\n");
      System.out.format(Locale.ROOT, "TESTING WITH %,d ROWS\n", dataSize);
      System.out.format(Locale.ROOT, "=".repeat(80) + "\n");

      File testCsv = createTestData(tempDir, dataSize, "large_test_" + dataSize + ".csv");
      Connection connection = setupConnection(tempDir);

      String tableName = "csvnextgen.LARGE_TEST_" + dataSize;
      runPerformanceTests(connection, tableName, dataSize);

      connection.close();

      // Clean up to save space
      testCsv.delete();
    }

    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("LARGE SCALE BENCHMARK COMPLETED");
    System.out.println("=".repeat(80));
  }

  private File createTestData(File dir, int rowCount, String filename) throws IOException {
    File csvFile = new File(dir, filename);
    Random random = new Random(42); // Fixed seed for reproducible results

    System.out.format(Locale.ROOT, "Generating %,d rows of test data...\n", rowCount);
    long startTime = System.currentTimeMillis();

    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      // Write header
      writer.write("id,customer_id,product_name,category,price,quantity,order_date,status,region,sales_rep\n");

      String[] products = {
          "Laptop Pro", "Desktop PC", "Monitor 4K", "Keyboard RGB", "Mouse Wireless",
          "Tablet Air", "Phone X1", "Headphones", "Speaker", "Webcam HD",
          "Router WiFi", "Switch 24P", "Cable USB-C", "Adapter HDMI", "Drive SSD"
      };

      String[] categories = {"Electronics", "Computers", "Audio", "Networking", "Accessories"};
      String[] statuses = {"pending", "processing", "shipped", "delivered", "cancelled", "returned"};
      String[] regions = {"North America", "Europe", "Asia Pacific", "Latin America", "Middle East", "Africa"};
      String[] salesReps = {"Alice Johnson", "Bob Smith", "Carol Williams", "David Brown", "Eva Davis", "Frank Wilson"};

      for (int i = 1; i <= rowCount; i++) {
        int customerId = 10000 + random.nextInt(50000);
        String product = products[random.nextInt(products.length)];
        String category = categories[random.nextInt(categories.length)];
        double price = 19.99 + (random.nextDouble() * 2000.0);
        int quantity = 1 + random.nextInt(10);
        String status = statuses[random.nextInt(statuses.length)];
        String region = regions[random.nextInt(regions.length)];
        String salesRep = salesReps[random.nextInt(salesReps.length)];

        // Generate realistic date range (last 2 years)
        int year = 2022 + random.nextInt(2);
        int month = 1 + random.nextInt(12);
        int day = 1 + random.nextInt(28);

        writer.write(
            String.format(Locale.ROOT, "%d,%d,%s,%s,%.2f,%d,%d-%02d-%02d,%s,%s,%s\n",
            i, customerId, product, category, price, quantity, year, month, day,
            status, region, salesRep));

        if (i % 50000 == 0) {
          System.out.format(Locale.ROOT, "  Generated %,d/%,d rows (%.1f%%)\n",
              i, rowCount, (i * 100.0) / rowCount);
        }
      }
    }

    long duration = System.currentTimeMillis() - startTime;
    long fileSizeBytes = csvFile.length();
    double fileSizeMB = fileSizeBytes / 1024.0 / 1024.0;

    System.out.format(Locale.ROOT, "Created: %.2f MB in %,d ms (%.0f rows/sec)\n",
        fileSizeMB, duration, (rowCount * 1000.0) / duration);

    return csvFile;
  }

  private Connection setupConnection(File testDir) throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    CsvNextGenSchemaSimple schema =
        new CsvNextGenSchemaSimple(testDir, "linq4j", 1024, true);
    rootSchema.add("csvnextgen", schema);

    return connection;
  }

  private void runPerformanceTests(Connection connection, String tableName, int expectedRowCount) throws SQLException {
    System.out.println("\nPERFORMANCE RESULTS");
    System.out.println("-".repeat(80));

    // 1. Full table scan
    Result result =
        benchmarkQuery(connection, "SELECT COUNT(*) FROM " + tableName,
        "Full table scan (COUNT)", 1);
    System.out.format(Locale.ROOT, "%-35s: %6d ms | %10s | %12s/sec\n",
        "Full table scan (COUNT)", result.avgTime,
        formatNumber(result.totalRows), formatNumber(result.rowsPerSecond));

    // 2. Projection tests
    result =
        benchmarkQuery(connection, "SELECT id, customer_id, price FROM " + tableName,
        "Simple projection (3 cols)", expectedRowCount);
    System.out.format(Locale.ROOT, "%-35s: %6d ms | %10s | %12s/sec\n",
        "Simple projection (3 cols)", result.avgTime,
        formatNumber(result.totalRows), formatNumber(result.rowsPerSecond));

    result =
        benchmarkQuery(connection, "SELECT * FROM " + tableName,
        "Full projection (10 cols)", expectedRowCount);
    System.out.format(Locale.ROOT, "%-35s: %6d ms | %10s | %12s/sec\n",
        "Full projection (10 cols)", result.avgTime,
        formatNumber(result.totalRows), formatNumber(result.rowsPerSecond));

    // 3. Filtering tests
    result =
        benchmarkQuery(connection, "SELECT COUNT(*) FROM " + tableName + " WHERE price > 500.0",
        "Filter by price > 500", -1);
    System.out.format(Locale.ROOT, "%-35s: %6d ms | %10s | %12s/sec\n",
        "Filter by price > 500", result.avgTime,
        formatNumber(result.totalRows), formatNumber(result.rowsPerSecond));

    result =
        benchmarkQuery(connection, "SELECT COUNT(*) FROM " + tableName + " WHERE category = 'Electronics'",
        "Filter by category", -1);
    System.out.format(Locale.ROOT, "%-35s: %6d ms | %10s | %12s/sec\n",
        "Filter by category", result.avgTime,
        formatNumber(result.totalRows), formatNumber(result.rowsPerSecond));

    result =
        benchmarkQuery(connection, "SELECT COUNT(*) FROM " + tableName + " WHERE price BETWEEN 100.0 AND 1000.0 AND quantity > 2",
        "Complex filter", -1);
    System.out.format(Locale.ROOT, "%-35s: %6d ms | %10s | %12s/sec\n",
        "Complex filter", result.avgTime,
        formatNumber(result.totalRows), formatNumber(result.rowsPerSecond));

    // 4. Aggregation tests
    result =
        benchmarkQuery(connection, "SELECT category, COUNT(*) FROM " + tableName + " GROUP BY category",
        "GROUP BY category", -1);
    System.out.format(Locale.ROOT, "%-35s: %6d ms | %10s | %12s/sec\n",
        "GROUP BY category", result.avgTime,
        formatNumber(result.totalRows), formatNumber(result.rowsPerSecond));

    result =
        benchmarkQuery(connection, "SELECT region, AVG(price), SUM(quantity) FROM " + tableName + " GROUP BY region",
        "GROUP BY region with agg", -1);
    System.out.format(Locale.ROOT, "%-35s: %6d ms | %10s | %12s/sec\n",
        "GROUP BY region with agg", result.avgTime,
        formatNumber(result.totalRows), formatNumber(result.rowsPerSecond));

    // 5. Sorting test
    result =
        benchmarkQuery(connection, "SELECT * FROM " + tableName + " ORDER BY price DESC LIMIT 1000",
        "ORDER BY price LIMIT 1000", 1000);
    System.out.format(Locale.ROOT, "%-35s: %6d ms | %10s | %12s/sec\n",
        "ORDER BY price LIMIT 1000", result.avgTime,
        formatNumber(result.totalRows), formatNumber(result.rowsPerSecond));

    // 6. Complex analytical query
    result =
        benchmarkQuery(connection, "SELECT region, category, COUNT(*) as orders, AVG(price) as avg_price, SUM(price * quantity) as revenue " +
        "FROM " + tableName + " " +
        "WHERE status IN ('delivered', 'shipped') AND price > 50.0 " +
        "GROUP BY region, category " +
        "HAVING COUNT(*) > " + Math.max(1, expectedRowCount / 1000) + " " +
        "ORDER BY revenue DESC",
        "Complex analytical query", -1);
    System.out.format(Locale.ROOT, "%-35s: %6d ms | %10s | %12s/sec\n",
        "Complex analytical query", result.avgTime,
        formatNumber(result.totalRows), formatNumber(result.rowsPerSecond));

    System.out.println("-".repeat(80));

    // Calculate throughput metrics
    double dataProcessingMB = getFileSize(expectedRowCount) / 1024.0 / 1024.0;
    long scanTime = benchmarkQuery(connection, "SELECT * FROM " + tableName, "Full scan", expectedRowCount).avgTime;
    double throughputMBPerSec = dataProcessingMB / (scanTime / 1000.0);

    System.out.format(Locale.ROOT, "Data size: %.2f MB, Full scan throughput: %.2f MB/sec\n",
        dataProcessingMB, throughputMBPerSec);
  }

  private long getFileSize(int rowCount) {
    // Estimate file size based on row count
    // Average row is about 70 bytes based on our data format
    return rowCount * 70L + 100; // +100 for header
  }

  private String formatNumber(long number) {
    if (number >= 1_000_000) {
      return String.format(Locale.ROOT, "%.2fM", number / 1_000_000.0);
    } else if (number >= 1_000) {
      return String.format(Locale.ROOT, "%.1fK", number / 1_000.0);
    } else {
      return String.valueOf(number);
    }
  }

  private Result benchmarkQuery(Connection connection, String sql, String description, int expectedRows) throws SQLException {
    // Warm-up run
    executeQuery(connection, sql, false);

    // Measured runs (3 iterations)
    long[] times = new long[3];
    long totalRows = 0;

    for (int i = 0; i < 3; i++) {
      long startTime = System.nanoTime();
      totalRows = executeQuery(connection, sql, true);
      long endTime = System.nanoTime();
      times[i] = (endTime - startTime) / 1_000_000; // Convert to ms
    }

    // Calculate statistics
    long avgTime = (times[0] + times[1] + times[2]) / 3;
    long rowsPerSecond = totalRows > 0 && avgTime > 0 ? (totalRows * 1000) / avgTime : 0;

    return new Result(avgTime, totalRows, rowsPerSecond);
  }

  private long executeQuery(Connection connection, String sql, boolean consumeAll) throws SQLException {
    long totalRows = 0;

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      if (consumeAll) {
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
          totalRows++;
          // Access all columns to ensure full processing
          for (int i = 1; i <= columnCount; i++) {
            rs.getObject(i);
          }
        }
      } else {
        while (rs.next()) {
          totalRows++;
        }
      }
    }

    return totalRows;
  }

  private static class Result {
    final long avgTime;
    final long totalRows;
    final long rowsPerSecond;

    Result(long avgTime, long totalRows, long rowsPerSecond) {
      this.avgTime = avgTime;
      this.totalRows = totalRows;
      this.rowsPerSecond = rowsPerSecond;
    }
  }
}
