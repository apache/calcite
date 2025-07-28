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

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Random;

/**
 * Performance comparison test between Arrow and Linq4j execution engines.
 * Quantifies the performance differences across various operation types.
 */
public class ArrowVsLinq4jPerformanceTest {

  @Test public void compareArrowVsLinq4jPerformance(@TempDir File tempDir) throws IOException {
    System.out.println("=".repeat(80));
    System.out.println("ARROW vs LINQ4J PERFORMANCE COMPARISON");
    System.out.println("=".repeat(80));

    // Test with different dataset sizes including larger datasets
    int[] dataSizes = {50_000, 100_000, 500_000};

    for (int dataSize : dataSizes) {
      System.out.format(Locale.ROOT, "\n"
  + "=".repeat(80) + "\n");
      System.out.format(Locale.ROOT, "DATASET SIZE: %,d ROWS\n", dataSize);
      System.out.format(Locale.ROOT, "=".repeat(80) + "\n");

      File testCsv = createNumericTestData(tempDir, dataSize);
      Source source = Sources.of(testCsv);
      RelDataType rowType = createRowType();

      // Initialize engines
      ArrowExecutionEngine arrowEngine = new ArrowExecutionEngine(1024);
      Linq4jExecutionEngine linq4jEngine = new Linq4jExecutionEngine(1024);

      // Run comparison tests
      compareFullTableScan(source, rowType, arrowEngine, linq4jEngine, dataSize);
      compareProjection(source, rowType, arrowEngine, linq4jEngine, dataSize);
      compareFiltering(source, rowType, arrowEngine, linq4jEngine, dataSize);
      compareAggregation(source, rowType, arrowEngine, linq4jEngine, dataSize);

      // Clean up
      testCsv.delete();
    }

    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("PERFORMANCE COMPARISON COMPLETED");
    System.out.println("=".repeat(80));
  }

  private File createNumericTestData(File dir, int rowCount) throws IOException {
    File csvFile = new File(dir, "numeric_test_" + rowCount + ".csv");
    Random random = new Random(42);

    System.out.format(Locale.ROOT, "Generating %,d rows of numeric test data...\n", rowCount);

    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      // Header with numeric columns for better testing
      writer.write("id,value1,value2,value3,category\n");

      String[] categories = {"A", "B", "C", "D", "E"};

      for (int i = 1; i <= rowCount; i++) {
        double value1 = random.nextDouble() * 1000.0;
        double value2 = random.nextDouble() * 100.0;
        double value3 = random.nextDouble() * 10.0;
        String category = categories[random.nextInt(categories.length)];

        writer.write(
            String.format(Locale.ROOT, "%d,%.2f,%.2f,%.2f,%s\n",
            i, value1, value2, value3, category));
      }
    }

    System.out.format(Locale.ROOT, "Created test file: %.2f MB\n", csvFile.length() / 1024.0 / 1024.0);
    return csvFile;
  }

  private RelDataType createRowType() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    builder.add("id", SqlTypeName.INTEGER);
    builder.add("value1", SqlTypeName.DOUBLE);
    builder.add("value2", SqlTypeName.DOUBLE);
    builder.add("value3", SqlTypeName.DOUBLE);
    builder.add("category", SqlTypeName.VARCHAR);
    return builder.build();
  }

  private void compareFullTableScan(Source source, RelDataType rowType,
      ArrowExecutionEngine arrowEngine, Linq4jExecutionEngine linq4jEngine, int expectedRows) {

    System.out.println("\n1. FULL TABLE SCAN COMPARISON");
    System.out.println("-".repeat(50));

    // Arrow scan
    long arrowTime = benchmarkOperation("Arrow Full Scan", () -> {
      Enumerable<Object[]> enumerable = arrowEngine.scan(source, rowType, true);
      return consumeEnumerable(enumerable);
    });

    // Linq4j scan
    long linq4jTime = benchmarkOperation("Linq4j Full Scan", () -> {
      Enumerable<Object[]> enumerable = linq4jEngine.scan(source, rowType, true);
      return consumeEnumerable(enumerable);
    });

    printComparison("Full Table Scan", arrowTime, linq4jTime, expectedRows);
  }

  private void compareProjection(Source source, RelDataType rowType,
      ArrowExecutionEngine arrowEngine, Linq4jExecutionEngine linq4jEngine, int expectedRows) {

    System.out.println("\n2. PROJECTION COMPARISON");
    System.out.println("-".repeat(50));

    // Arrow projection (simulated by full scan - in real impl would have projection pushdown)
    long arrowTime = benchmarkOperation("Arrow Projection", () -> {
      Enumerable<Object[]> enumerable = arrowEngine.scan(source, rowType, true);
      // Simulate projection by accessing only specific columns
      return countWithProjection(enumerable, new int[]{0, 1, 2}); // id, value1, value2
    });

    // Linq4j projection
    long linq4jTime = benchmarkOperation("Linq4j Projection", () -> {
      Enumerable<Object[]> enumerable = linq4jEngine.scan(source, rowType, true);
      return countWithProjection(enumerable, new int[]{0, 1, 2}); // id, value1, value2
    });

    printComparison("Projection (3 cols)", arrowTime, linq4jTime, expectedRows);
  }

  private void compareFiltering(Source source, RelDataType rowType,
      ArrowExecutionEngine arrowEngine, Linq4jExecutionEngine linq4jEngine, int expectedRows) {

    System.out.println("\n3. FILTERING COMPARISON");
    System.out.println("-".repeat(50));

    // Define filter predicate (value1 > 500.0)
    ArrowExecutionEngine.FilterPredicate arrowPredicate = row -> {
      Object value = row[1]; // value1 column
      return value instanceof Number && ((Number) value).doubleValue() > 500.0;
    };

    Linq4jExecutionEngine.FilterPredicate linq4jPredicate = row -> {
      Object value = row[1]; // value1 column
      return value instanceof Number && ((Number) value).doubleValue() > 500.0;
    };

    // Arrow filtering
    long arrowTime = benchmarkOperation("Arrow Filtering", () -> {
      Enumerable<Object[]> enumerable = arrowEngine.filter(source, rowType, true, arrowPredicate);
      return consumeEnumerable(enumerable);
    });

    // Linq4j filtering
    long linq4jTime = benchmarkOperation("Linq4j Filtering", () -> {
      Enumerable<Object[]> enumerable = linq4jEngine.filter(source, rowType, true, linq4jPredicate);
      return consumeEnumerable(enumerable);
    });

    printComparison("Filtering (value1 > 500)", arrowTime, linq4jTime, -1); // Unknown result count
  }

  private void compareAggregation(Source source, RelDataType rowType,
      ArrowExecutionEngine arrowEngine, Linq4jExecutionEngine linq4jEngine, int expectedRows) {

    System.out.println("\n4. AGGREGATION COMPARISON");
    System.out.println("-".repeat(50));

    // Arrow aggregation
    long arrowTime = benchmarkOperation("Arrow SUM", () -> {
      Enumerable<Object[]> enumerable = arrowEngine.scan(source, rowType, true);
      double sum = 0.0;
      try (Enumerator<Object[]> enumerator = enumerable.enumerator()) {
        while (enumerator.moveNext()) {
          Object[] row = enumerator.current();
          Object value = row[1]; // value1 column
          if (value instanceof Number) {
            sum += ((Number) value).doubleValue();
          }
        }
      }
      return (long) sum; // Return as long for consistency
    });

    // Linq4j aggregation
    long linq4jTime = benchmarkOperation("Linq4j SUM", () -> {
      Enumerable<Object[]> enumerable = linq4jEngine.scan(source, rowType, true);
      double sum = 0.0;
      try (Enumerator<Object[]> enumerator = enumerable.enumerator()) {
        while (enumerator.moveNext()) {
          Object[] row = enumerator.current();
          Object value = row[1]; // value1 column
          if (value instanceof Number) {
            sum += ((Number) value).doubleValue();
          }
        }
      }
      return (long) sum; // Return as long for consistency
    });

    printComparison("SUM(value1)", arrowTime, linq4jTime, 1); // Single result
  }

  private long benchmarkOperation(String operationName, BenchmarkOperation operation) {
    // Warm-up
    operation.execute();

    // Measured runs
    long[] times = new long[3];
    for (int i = 0; i < 3; i++) {
      long startTime = System.nanoTime();
      operation.execute();
      long endTime = System.nanoTime();
      times[i] = (endTime - startTime) / 1_000_000; // Convert to ms
    }

    // Return average time
    return (times[0] + times[1] + times[2]) / 3;
  }

  private long consumeEnumerable(Enumerable<Object[]> enumerable) {
    long count = 0;
    try (Enumerator<Object[]> enumerator = enumerable.enumerator()) {
      while (enumerator.moveNext()) {
        count++;
        // Access all columns to ensure full processing
        Object[] row = enumerator.current();
        for (Object value : row) {
          // Touch the value to ensure it's materialized
          value.toString();
        }
      }
    }
    return count;
  }

  private long countWithProjection(Enumerable<Object[]> enumerable, int[] projectedColumns) {
    long count = 0;
    try (Enumerator<Object[]> enumerator = enumerable.enumerator()) {
      while (enumerator.moveNext()) {
        count++;
        // Access only projected columns
        Object[] row = enumerator.current();
        for (int col : projectedColumns) {
          if (col < row.length) {
            row[col].toString(); // Touch the value
          }
        }
      }
    }
    return count;
  }

  private void printComparison(String operation, long arrowTime, long linq4jTime, int rows) {
    double speedup = (double) linq4jTime / arrowTime;
    String winner = speedup > 1.0 ? "Arrow" : "Linq4j";

    System.out.format(Locale.ROOT, "%-25s: Arrow=%4d ms, Linq4j=%4d ms", operation, arrowTime, linq4jTime);
    System.out.format(Locale.ROOT, " | Speedup: %.2fx (%s)", Math.abs(speedup), winner);

    if (rows > 0) {
      long arrowThroughput = (rows * 1000L) / arrowTime;
      long linq4jThroughput = (rows * 1000L) / linq4jTime;
      System.out.format(Locale.ROOT, " | Throughput: A=%,d/s, L=%,d/s", arrowThroughput, linq4jThroughput);
    }

    System.out.println();
  }

  @FunctionalInterface
  private interface BenchmarkOperation {
    long execute();
  }
}
