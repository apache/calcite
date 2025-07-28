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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

/**
 * Performance test suite for CsvNextGen adapter.
 * Tests various operations with large CSV files to quantify performance.
 */
public class CsvNextGenPerformanceTest {

  private File testDir;
  private File smallCsv;   // 1K rows
  private File mediumCsv;  // 10K rows
  private File largeCsv;   // 100K rows
  private File xlargeCsv;  // 1M rows
  private Connection connection;

  @BeforeEach
  public void setUp(@TempDir File tempDir) throws IOException, SQLException {
    this.testDir = tempDir;

    // Create test CSV files of various sizes
    System.out.println("Creating test CSV files...");
    smallCsv = createTestCsv("small_data.csv", 1_000);
    mediumCsv = createTestCsv("medium_data.csv", 10_000);
    largeCsv = createTestCsv("large_data.csv", 100_000);
    xlargeCsv = createTestCsv("xlarge_data.csv", 1_000_000);

    // Set up Calcite connection
    setupConnection();
    System.out.println("Setup complete. Test files created in: " + testDir.getAbsolutePath());
  }

  private File createTestCsv(String filename, int rowCount) throws IOException {
    File csvFile = new File(testDir, filename);
    Random random = new Random(42); // Fixed seed for reproducible results

    long startTime = System.currentTimeMillis();

    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      // Write header
      writer.write("id,name,email,age,salary,department,hire_date,active\n");

      String[] departments = {"Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"};
      String[] domains = {"gmail.com", "yahoo.com", "company.com", "outlook.com"};

      for (int i = 1; i <= rowCount; i++) {
        int age = 22 + random.nextInt(40);
        int salary = 30000 + random.nextInt(120000);
        String department = departments[random.nextInt(departments.length)];
        String domain = domains[random.nextInt(domains.length)];
        boolean active = random.nextBoolean();

        writer.write(
            String.format(Locale.ROOT, "%d,User_%d,user_%d@%s,%d,%d,%s,2020-01-%02d,%s\n",
            i, i, i, domain, age, salary, department,
            1 + random.nextInt(28), active));

        if (i % 100000 == 0) {
          System.out.format(Locale.ROOT, "Generated %d rows for %s\n", i, filename);
        }
      }
    }

    long duration = System.currentTimeMillis() - startTime;
    long fileSize = csvFile.length();
    System.out.format(Locale.ROOT, "Created %s: %d rows, %.2f MB, took %d ms\n",
        filename, rowCount, fileSize / 1024.0 / 1024.0, duration);

    return csvFile;
  }

  private void setupConnection() throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    connection = DriverManager.getConnection("jdbc:calcite:", info);

    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // Add CsvNextGen schema
    CsvNextGenSchemaSimple schema =
        new CsvNextGenSchemaSimple(testDir, "linq4j", 1024, true);
    rootSchema.add("csvnextgen", schema);
  }

  @Test public void testFullTableScanPerformance() throws SQLException {
    System.out.println("\n=== FULL TABLE SCAN PERFORMANCE ===");

    testQuery("SELECT COUNT(*) FROM SMALL_DATA", "Small (1K rows) - COUNT");
    testQuery("SELECT COUNT(*) FROM MEDIUM_DATA", "Medium (10K rows) - COUNT");
    testQuery("SELECT COUNT(*) FROM LARGE_DATA", "Large (100K rows) - COUNT");
    testQuery("SELECT COUNT(*) FROM XLARGE_DATA", "XLarge (1M rows) - COUNT");
  }

  @Test public void testProjectionPerformance() throws SQLException {
    System.out.println("\n=== PROJECTION PERFORMANCE ===");

    testQuery("SELECT id, name FROM LARGE_DATA", "Large - Simple projection (2 cols)");
    testQuery("SELECT id, name, email, department FROM LARGE_DATA", "Large - Medium projection (4 cols)");
    testQuery("SELECT * FROM LARGE_DATA", "Large - Full projection (8 cols)");
  }

  @Test public void testFilteringPerformance() throws SQLException {
    System.out.println("\n=== FILTERING PERFORMANCE ===");

    testQuery("SELECT COUNT(*) FROM LARGE_DATA WHERE age > 30",
        "Large - Simple filter (age > 30)");
    testQuery("SELECT COUNT(*) FROM LARGE_DATA WHERE salary BETWEEN 50000 AND 80000",
        "Large - Range filter (salary)");
    testQuery("SELECT COUNT(*) FROM LARGE_DATA WHERE department = 'Engineering'",
        "Large - String equality filter");
    testQuery("SELECT COUNT(*) FROM LARGE_DATA WHERE age > 30 AND salary > 60000",
        "Large - Compound AND filter");
    testQuery("SELECT COUNT(*) FROM LARGE_DATA WHERE department = 'Engineering' OR department = 'Sales'",
        "Large - Compound OR filter");
  }

  @Test public void testAggregationPerformance() throws SQLException {
    System.out.println("\n=== AGGREGATION PERFORMANCE ===");

    testQuery("SELECT AVG(salary) FROM LARGE_DATA", "Large - AVG(salary)");
    testQuery("SELECT MIN(age), MAX(age) FROM LARGE_DATA", "Large - MIN/MAX(age)");
    testQuery("SELECT SUM(salary) FROM LARGE_DATA", "Large - SUM(salary)");
    testQuery("SELECT department, COUNT(*) FROM LARGE_DATA GROUP BY department",
        "Large - GROUP BY department");
    testQuery("SELECT department, AVG(salary) FROM LARGE_DATA GROUP BY department",
        "Large - GROUP BY with AVG");
    testQuery("SELECT department, COUNT(*), AVG(salary), MIN(age), MAX(age) FROM LARGE_DATA GROUP BY department",
        "Large - GROUP BY with multiple aggregates");
  }

  @Test public void testSortingPerformance() throws SQLException {
    System.out.println("\n=== SORTING PERFORMANCE ===");

    testQuery("SELECT * FROM MEDIUM_DATA ORDER BY id LIMIT 100",
        "Medium - ORDER BY id (natural order) LIMIT 100");
    testQuery("SELECT * FROM MEDIUM_DATA ORDER BY salary DESC LIMIT 100",
        "Medium - ORDER BY salary DESC LIMIT 100");
    testQuery("SELECT * FROM MEDIUM_DATA ORDER BY department, salary DESC LIMIT 100",
        "Medium - ORDER BY department, salary LIMIT 100");
  }

  @Test public void testComplexQueryPerformance() throws SQLException {
    System.out.println("\n=== COMPLEX QUERY PERFORMANCE ===");

    String complexQuery1 =
        "SELECT department, " +
        "       COUNT(*) as employee_count, " +
        "       AVG(salary) as avg_salary, " +
        "       MIN(age) as min_age, " +
        "       MAX(age) as max_age " +
        "FROM LARGE_DATA " +
        "WHERE age BETWEEN 25 AND 50 AND salary > 40000 " +
        "GROUP BY department " +
        "HAVING COUNT(*) > 1000 " +
        "ORDER BY avg_salary DESC";

    testQuery(complexQuery1, "Large - Complex analytical query");

    String complexQuery2 =
        "SELECT d1.department, " +
        "       d1.avg_salary, " +
        "       d2.employee_count " +
        "FROM (SELECT department, AVG(salary) as avg_salary FROM LARGE_DATA GROUP BY department) d1 " +
        "JOIN (SELECT department, COUNT(*) as employee_count FROM LARGE_DATA GROUP BY department) d2 " +
        "  ON d1.department = d2.department " +
        "ORDER BY d1.avg_salary DESC";

    testQuery(complexQuery2, "Large - JOIN with subqueries");
  }

  @Test public void testMemoryUsagePatterns() throws SQLException {
    System.out.println("\n=== MEMORY USAGE PATTERNS ===");

    // Test memory efficiency with different result set sizes
    Runtime runtime = Runtime.getRuntime();

    runtime.gc();
    long beforeMemory = runtime.totalMemory() - runtime.freeMemory();

    testQuery("SELECT * FROM SMALL_DATA", "Small - Full table scan (memory baseline)");

    runtime.gc();
    long afterSmallMemory = runtime.totalMemory() - runtime.freeMemory();
    long smallMemoryUsed = afterSmallMemory - beforeMemory;

    testQuery("SELECT * FROM MEDIUM_DATA", "Medium - Full table scan (memory test)");

    runtime.gc();
    long afterMediumMemory = runtime.totalMemory() - runtime.freeMemory();
    long mediumMemoryUsed = afterMediumMemory - beforeMemory;

    System.out.format(Locale.ROOT, "Memory usage - Small: %.2f MB, Medium: %.2f MB\n",
        smallMemoryUsed / 1024.0 / 1024.0,
        mediumMemoryUsed / 1024.0 / 1024.0);
  }

  @Test public void testBatchSizeImpact() throws SQLException {
    System.out.println("\n=== BATCH SIZE IMPACT ===");

    // Test with different batch sizes (would require modifying the adapter)
    // For now, just document the current batch size performance
    testQuery("SELECT COUNT(*) FROM LARGE_DATA", "Large - Current batch size (1024)");

    // Note: To truly test different batch sizes, we'd need to modify the adapter
    // to accept batch size as a parameter and create multiple schema instances
  }

  private void testQuery(String sql, String description) throws SQLException {
    System.out.format(Locale.ROOT, "\nTesting: %s\n", description);
    System.out.format(Locale.ROOT, "SQL: %s\n", sql);

    long startTime = System.nanoTime();
    long totalRows = 0;

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int columnCount = rs.getMetaData().getColumnCount();

      while (rs.next()) {
        totalRows++;
        // Access all columns to ensure full processing
        for (int i = 1; i <= columnCount; i++) {
          rs.getObject(i);
        }
      }
    }

    long endTime = System.nanoTime();
    long durationMs = (endTime - startTime) / 1_000_000;

    System.out.format(Locale.ROOT, "Results: %d rows in %d ms", totalRows, durationMs);

    if (totalRows > 0 && durationMs > 0) {
      long rowsPerSecond = (totalRows * 1000) / durationMs;
      System.out.format(Locale.ROOT, " (%.0f rows/sec)", (double) rowsPerSecond);
    }

    System.out.println();
  }

  @Test public void testComparisonWithOriginalFileAdapter() throws SQLException {
    System.out.println("\n=== COMPARISON WITH ORIGINAL FILE ADAPTER ===");

    // Add original file adapter for comparison
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    try {
      // Try to add original file adapter schema
      Class<?> fileSchemaFactory = Class.forName("org.apache.calcite.adapter.file.FileSchemaFactory");
      Object fileSchema = fileSchemaFactory.getDeclaredConstructor().newInstance();

      java.util.Map<String, Object> operand = new java.util.HashMap<>();
      operand.put("directory", testDir.getAbsolutePath());

      org.apache.calcite.schema.Schema originalSchema =
          ((org.apache.calcite.schema.SchemaFactory) fileSchema).create(rootSchema, "file", operand);
      rootSchema.add("fileadapter", originalSchema);

      System.out.println("Added original file adapter for comparison");

      // Compare performance on same dataset
      testQuery("SELECT COUNT(*) FROM csvnextgen.MEDIUM_DATA", "CsvNextGen - Medium COUNT");
      testQuery("SELECT COUNT(*) FROM fileadapter.MEDIUM_DATA", "Original File - Medium COUNT");

    } catch (Exception e) {
      System.out.println("Could not load original file adapter for comparison: " + e.getMessage());
    }
  }

  @Test public void printPerformanceSummary() {
    System.out.println("\n"
  + "=".repeat(60));
    System.out.println("PERFORMANCE TEST SUMMARY");
    System.out.println("=".repeat(60));
    System.out.println("Test Environment:");
    System.out.println("- Java Version: " + System.getProperty("java.version"));
    System.out.println("- Available Processors: " + Runtime.getRuntime().availableProcessors());
    System.out.println("- Max Memory: " + (Runtime.getRuntime().maxMemory() / 1024 / 1024) + " MB");
    System.out.println("- Test Data Directory: " + testDir.getAbsolutePath());

    System.out.println("\nFile Sizes:");
    if (smallCsv.exists()) System.out.format(Locale.ROOT, "- Small (1K rows): %.2f MB\n", smallCsv.length() / 1024.0 / 1024.0);
    if (mediumCsv.exists()) System.out.format(Locale.ROOT, "- Medium (10K rows): %.2f MB\n", mediumCsv.length() / 1024.0 / 1024.0);
    if (largeCsv.exists()) System.out.format(Locale.ROOT, "- Large (100K rows): %.2f MB\n", largeCsv.length() / 1024.0 / 1024.0);
    if (xlargeCsv.exists()) System.out.format(Locale.ROOT, "- XLarge (1M rows): %.2f MB\n", xlargeCsv.length() / 1024.0 / 1024.0);

    System.out.println("\nAdapter Configuration:");
    System.out.println("- Engine Type: linq4j");
    System.out.println("- Batch Size: 1024");
    System.out.println("- Header Detection: enabled");

    System.out.println("=".repeat(60));
  }
}
