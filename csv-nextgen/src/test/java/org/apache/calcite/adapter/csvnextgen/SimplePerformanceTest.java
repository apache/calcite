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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

/**
 * Simple performance test for CsvNextGen adapter.
 */
public class SimplePerformanceTest {

  @Test public void runSimplePerformanceBenchmark(@TempDir File tempDir) throws IOException, SQLException {
    System.out.println("=".repeat(60));
    System.out.println("CSV NEXTGEN ADAPTER - PERFORMANCE BENCHMARK");
    System.out.println("=".repeat(60));

    // Create test data
    File testCsv = createTestData(tempDir, 25_000); // 25K rows for faster test

    // Setup connection
    Connection connection = setupConnection(tempDir);

    // List available tables first
    listAvailableTables(connection);

    // Run benchmark tests
    runBenchmarkSuite(connection);

    connection.close();
    System.out.println("\nBenchmark completed successfully!");
  }

  private File createTestData(File dir, int rowCount) throws IOException {
    File csvFile = new File(dir, "perf_test.csv");
    Random random = new Random(42);

    System.out.format(Locale.ROOT, "Generating %d rows of test data...\n", rowCount);
    long startTime = System.currentTimeMillis();

    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("id,name,email,age,salary,department,active\n");

      String[] departments = {"Engineering", "Sales", "Marketing", "HR", "Finance"};
      String[] domains = {"gmail.com", "company.com", "yahoo.com"};

      for (int i = 1; i <= rowCount; i++) {
        int age = 22 + random.nextInt(40);
        int salary = 30000 + random.nextInt(120000);
        String department = departments[random.nextInt(departments.length)];
        String domain = domains[random.nextInt(domains.length)];
        boolean active = random.nextBoolean();

        writer.write(
            String.format(Locale.ROOT, "%d,User_%d,user_%d@%s,%d,%d,%s,%s\n",
            i, i, i, domain, age, salary, department, active));
      }
    }

    long duration = System.currentTimeMillis() - startTime;
    long fileSizeBytes = csvFile.length();
    double fileSizeMB = fileSizeBytes / 1024.0 / 1024.0;

    System.out.format(Locale.ROOT, "Created: %.2f MB in %d ms (%.0f rows/sec)\n",
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

  private void listAvailableTables(Connection connection) throws SQLException {
    System.out.println("\nAvailable tables:");
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet tables = metaData.getTables(null, "csvnextgen", null, new String[]{"TABLE"})) {
      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        System.out.println("  - " + tableName);
      }
    }
    System.out.println();
  }

  private void runBenchmarkSuite(Connection connection) throws SQLException {
    System.out.println("BENCHMARK RESULTS");
    System.out.println("-".repeat(60));

    String tableName = "csvnextgen.PERF_TEST"; // Use fully qualified name

    // Basic operations
    benchmarkQuery(connection,
        "SELECT COUNT(*) FROM " + tableName,
        "Full table scan (COUNT)");

    benchmarkQuery(connection,
        "SELECT id, name, salary FROM " + tableName,
        "Simple projection (3 cols)");

    benchmarkQuery(connection,
        "SELECT COUNT(*) FROM " + tableName + " WHERE salary > 75000",
        "Filter by salary > 75K");

    benchmarkQuery(connection,
        "SELECT department, COUNT(*) FROM " + tableName + " GROUP BY department",
        "GROUP BY department");

    benchmarkQuery(connection,
        "SELECT department, AVG(salary) FROM " + tableName + " GROUP BY department",
        "GROUP BY with AVG(salary)");

    benchmarkQuery(connection,
        "SELECT * FROM " + tableName + " ORDER BY salary DESC LIMIT 100",
        "ORDER BY salary LIMIT 100");

    benchmarkQuery(connection,
        "SELECT department, COUNT(*), AVG(salary) FROM " + tableName +
        " WHERE age BETWEEN 25 AND 45 GROUP BY department HAVING COUNT(*) > 100",
        "Complex analytical query");

    System.out.println("-".repeat(60));
  }

  private void benchmarkQuery(Connection connection, String sql, String description) throws SQLException {
    System.out.format(Locale.ROOT, "%-30s: ", description);

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

    // Calculate average
    long avgTime = (times[0] + times[1] + times[2]) / 3;
    long minTime = Math.min(times[0], Math.min(times[1], times[2]));

    System.out.format(Locale.ROOT, "%4d ms", avgTime);

    if (totalRows > 0 && avgTime > 0) {
      long rowsPerSecond = (totalRows * 1000) / avgTime;
      System.out.format(Locale.ROOT, " | %,6d rows | %,8d rows/sec", totalRows, rowsPerSecond);
    }

    System.out.println();
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
}
