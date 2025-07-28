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
 * Comprehensive performance test comparing Linq4j, Arrow, and Vectorized Arrow implementations.
 * Focuses on projection, filtering, and aggregation operations where vectorization provides
 * the greatest benefits.
 */
public class VectorizedPerformanceTest {

  @Test public void compareAllExecutionEngines(@TempDir File tempDir) throws IOException {
    System.out.println("=".repeat(80));
    System.out.println("COMPREHENSIVE EXECUTION ENGINE PERFORMANCE COMPARISON");
    System.out.println("=".repeat(80));

    // Test with different dataset sizes to show vectorization scaling benefits
    int[] dataSizes = {100_000, 500_000, 1_000_000};

    for (int dataSize : dataSizes) {
      System.out.format(Locale.ROOT, "\n"
          + "=".repeat(80) + "\n");
      System.out.format(Locale.ROOT, "DATASET SIZE: %,d ROWS\n", dataSize);
      System.out.format(Locale.ROOT, "=".repeat(80) + "\n");

      File testCsv = createAnalyticalTestData(tempDir, dataSize);
      Source source = Sources.of(testCsv);
      RelDataType rowType = createRowType();

      // Initialize all engines
      VectorizedArrowExecutionEngine vectorizedEngine = new VectorizedArrowExecutionEngine(2048);
      ArrowExecutionEngine arrowEngine = new ArrowExecutionEngine(2048);
      Linq4jExecutionEngine linq4jEngine = new Linq4jExecutionEngine(2048);

      try {
        // Test operations where vectorization provides maximum benefit
        compareProjectionPerformance(source, rowType, vectorizedEngine,
            arrowEngine, linq4jEngine, dataSize);
        compareFilteringPerformance(source, rowType, vectorizedEngine,
            arrowEngine, linq4jEngine, dataSize);
        compareAggregationPerformance(source, rowType, vectorizedEngine,
            arrowEngine, linq4jEngine, dataSize);

      } finally {
        vectorizedEngine.close();
        testCsv.delete();
      }
    }

    System.out.println("\n"
        + "=".repeat(80));
    System.out.println("PERFORMANCE ANALYSIS SUMMARY");
    printVectorizationBenefits();
    System.out.println("=".repeat(80));
  }

  private File createAnalyticalTestData(File dir, int rowCount) throws IOException {
    File csvFile = new File(dir, "analytical_test_" + rowCount + ".csv");
    Random random = new Random(42);

    System.out.format(Locale.ROOT, "Generating %,d rows of analytical test data...\n",
        rowCount);

    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      // Schema optimized for analytical workloads
      writer.write("id,revenue,cost,profit_margin,region,quarter,year\n");

      String[] regions = {"North", "South", "East", "West", "Central"};
      int[] quarters = {1, 2, 3, 4};
      int[] years = {2020, 2021, 2022, 2023, 2024};

      for (int i = 1; i <= rowCount; i++) {
        double revenue = 1000.0 + (random.nextDouble() * 50000.0);
        double cost = revenue * (0.3 + random.nextDouble() * 0.4); // 30-70% of revenue
        double profitMargin = ((revenue - cost) / revenue) * 100.0;
        String region = regions[random.nextInt(regions.length)];
        int quarter = quarters[random.nextInt(quarters.length)];
        int year = years[random.nextInt(years.length)];

        writer.write(
            String.format(Locale.ROOT, "%d,%.2f,%.2f,%.2f,%s,%d,%d\n",
            i, revenue, cost, profitMargin, region, quarter, year));
      }
    }

    System.out.format(Locale.ROOT, "Created test file: %.2f MB\n",
        csvFile.length() / 1024.0 / 1024.0);
    return csvFile;
  }

  private RelDataType createRowType() {
    RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    builder.add("id", SqlTypeName.INTEGER);
    builder.add("revenue", SqlTypeName.DOUBLE);
    builder.add("cost", SqlTypeName.DOUBLE);
    builder.add("profit_margin", SqlTypeName.DOUBLE);
    builder.add("region", SqlTypeName.VARCHAR);
    builder.add("quarter", SqlTypeName.INTEGER);
    builder.add("year", SqlTypeName.INTEGER);
    return builder.build();
  }

  private void compareProjectionPerformance(Source source, RelDataType rowType,
      VectorizedArrowExecutionEngine vectorizedEngine, ArrowExecutionEngine arrowEngine,
      Linq4jExecutionEngine linq4jEngine, int expectedRows) {

    System.out.println("\n1. PROJECTION PERFORMANCE (3 numeric columns)");
    System.out.println("-".repeat(60));

    int[] projectedColumns = {1, 2, 3}; // revenue, cost, profit_margin

    // Vectorized Arrow projection (true columnar)
    long vectorizedTime = benchmarkOperation("Vectorized Projection", () -> {
      try (VectorizedArrowExecutionEngine.VectorizedResult result =
          vectorizedEngine.project(source, rowType, true, projectedColumns)) {
        return result.getTotalRowCount();
      }
    });

    // Standard Arrow projection (via enumerable)
    long arrowTime = benchmarkOperation("Arrow Projection", () -> {
      Enumerable<Object[]> enumerable = arrowEngine.scan(source, rowType, true);
      return countWithProjection(enumerable, projectedColumns);
    });

    // Linq4j projection
    long linq4jTime = benchmarkOperation("Linq4j Projection", () -> {
      Enumerable<Object[]> enumerable = linq4jEngine.scan(source, rowType, true);
      return countWithProjection(enumerable, projectedColumns);
    });

    printThreeWayComparison("Projection", vectorizedTime, arrowTime, linq4jTime, expectedRows);
  }

  private void compareFilteringPerformance(Source source, RelDataType rowType,
      VectorizedArrowExecutionEngine vectorizedEngine, ArrowExecutionEngine arrowEngine,
      Linq4jExecutionEngine linq4jEngine, int expectedRows) {

    System.out.println("\n2. FILTERING PERFORMANCE (profit_margin > 15.0)");
    System.out.println("-".repeat(60));

    // Vectorized Arrow filtering (true vectorized predicates)
    long vectorizedTime = benchmarkOperation("Vectorized Filtering", () -> {
      VectorizedArrowExecutionEngine.VectorizedPredicate predicate =
          VectorizedArrowExecutionEngine.VectorizedPredicates
              .greaterThan(3, 15.0); // profit_margin > 15
      try (VectorizedArrowExecutionEngine.VectorizedResult result =
          vectorizedEngine.filter(source, rowType, true, predicate)) {
        return result.getTotalRowCount();
      }
    });

    // Standard Arrow filtering
    ArrowExecutionEngine.FilterPredicate arrowPredicate = row -> {
      Object value = row[3]; // profit_margin column
      return value instanceof Number && ((Number) value).doubleValue() > 15.0;
    };

    long arrowTime = benchmarkOperation("Arrow Filtering", () -> {
      Enumerable<Object[]> enumerable = arrowEngine.filter(source, rowType, true, arrowPredicate);
      return consumeEnumerable(enumerable);
    });

    // Linq4j filtering
    Linq4jExecutionEngine.FilterPredicate linq4jPredicate = row -> {
      Object value = row[3]; // profit_margin column
      return value instanceof Number && ((Number) value).doubleValue() > 15.0;
    };

    long linq4jTime = benchmarkOperation("Linq4j Filtering", () -> {
      Enumerable<Object[]> enumerable = linq4jEngine.filter(source, rowType, true, linq4jPredicate);
      return consumeEnumerable(enumerable);
    });

    printThreeWayComparison("Filtering", vectorizedTime, arrowTime, linq4jTime, -1);
  }

  private void compareAggregationPerformance(Source source, RelDataType rowType,
      VectorizedArrowExecutionEngine vectorizedEngine, ArrowExecutionEngine arrowEngine,
      Linq4jExecutionEngine linq4jEngine, int expectedRows) {

    System.out.println("\n3. AGGREGATION PERFORMANCE");
    System.out.println("-".repeat(60));

    // SUM aggregation comparison
    System.out.println("\nSUM(revenue) comparison:");

    // Vectorized Arrow SUM (true columnar aggregation)
    long vectorizedSumTime = benchmarkOperation("Vectorized SUM", () -> {
      double sum = vectorizedEngine.aggregateSum(source, rowType, true, 1); // revenue column
      return (long) sum;
    });

    // Standard Arrow SUM
    long arrowSumTime = benchmarkOperation("Arrow SUM", () -> {
      Enumerable<Object[]> enumerable = arrowEngine.scan(source, rowType, true);
      double sum = 0.0;
      try (Enumerator<Object[]> enumerator = enumerable.enumerator()) {
        while (enumerator.moveNext()) {
          Object[] row = enumerator.current();
          Object value = row[1]; // revenue column
          if (value instanceof Number) {
            sum += ((Number) value).doubleValue();
          }
        }
      }
      return (long) sum;
    });

    // Linq4j SUM
    long linq4jSumTime = benchmarkOperation("Linq4j SUM", () -> {
      Enumerable<Object[]> enumerable = linq4jEngine.scan(source, rowType, true);
      return (long) Linq4jExecutionEngine.Linq4jAggregator.sum(enumerable, 1);
    });

    printThreeWayComparison("SUM(revenue)", vectorizedSumTime, arrowSumTime, linq4jSumTime, 1);

    // COUNT aggregation comparison
    System.out.println("\nCOUNT(*) comparison:");

    long vectorizedCountTime = benchmarkOperation("Vectorized COUNT", () -> {
      return vectorizedEngine.aggregateCount(source, rowType, true);
    });

    long arrowCountTime = benchmarkOperation("Arrow COUNT", () -> {
      Enumerable<Object[]> enumerable = arrowEngine.scan(source, rowType, true);
      return consumeEnumerable(enumerable);
    });

    long linq4jCountTime = benchmarkOperation("Linq4j COUNT", () -> {
      Enumerable<Object[]> enumerable = linq4jEngine.scan(source, rowType, true);
      return Linq4jExecutionEngine.Linq4jAggregator.count(enumerable);
    });

    printThreeWayComparison("COUNT(*)", vectorizedCountTime, arrowCountTime,
        linq4jCountTime, expectedRows);

    // MIN/MAX aggregation comparison
    System.out.println("\nMIN(profit_margin) comparison:");

    long vectorizedMinTime = benchmarkOperation("Vectorized MIN", () -> {
      double min =
          vectorizedEngine.aggregateMinMax(source, rowType, true, 3, true); // profit_margin column
      return (long) (min * 100); // Convert to long for consistency
    });

    long arrowMinTime = benchmarkOperation("Arrow MIN", () -> {
      Enumerable<Object[]> enumerable = arrowEngine.scan(source, rowType, true);
      return (long) (Linq4jExecutionEngine.Linq4jAggregator.min(enumerable, 3) * 100);
    });

    long linq4jMinTime = benchmarkOperation("Linq4j MIN", () -> {
      Enumerable<Object[]> enumerable = linq4jEngine.scan(source, rowType, true);
      return (long) (Linq4jExecutionEngine.Linq4jAggregator.min(enumerable, 3) * 100);
    });

    printThreeWayComparison("MIN(profit_margin)",
        vectorizedMinTime, arrowMinTime, linq4jMinTime, 1);
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
        // Touch the values to ensure they're materialized
        Object[] row = enumerator.current();
        for (Object value : row) {
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

  private void printThreeWayComparison(String operation, long vectorizedTime,
      long arrowTime, long linq4jTime, int rows) {
    System.out.format(Locale.ROOT, "%-20s: Vectorized=%4d ms, Arrow=%4d ms, Linq4j=%4d ms",
        operation, vectorizedTime, arrowTime, linq4jTime);

    // Calculate speedups
    double vectorizedVsLinq4j = (double) linq4jTime / vectorizedTime;
    double vectorizedVsArrow = (double) arrowTime / vectorizedTime;
    double arrowVsLinq4j = (double) linq4jTime / arrowTime;

    System.out.format(Locale.ROOT, " | Speedup: Vec/L4j=%.2fx, Vec/Arr=%.2fx, Arr/L4j=%.2fx",
        vectorizedVsLinq4j, vectorizedVsArrow, arrowVsLinq4j);

    if (rows > 0) {
      long vectorizedThroughput = (rows * 1000L) / vectorizedTime;
      long arrowThroughput = (rows * 1000L) / arrowTime;
      long linq4jThroughput = (rows * 1000L) / linq4jTime;
      System.out.format(Locale.ROOT,
          "\n%20s  Throughput: V=%,d/s, A=%,d/s, L=%,d/s",
          "", vectorizedThroughput, arrowThroughput, linq4jThroughput);
    }

    System.out.println();
  }

  private void printVectorizationBenefits() {
    System.out.println("VECTORIZATION PERFORMANCE ANALYSIS:");
    System.out.println("-".repeat(80));
    System.out.println();

    System.out.println("Expected Benefits from True Vectorization:");
    System.out.println("• Projection: 2-4x faster (columnar access, cache efficiency)");
    System.out.println("• Filtering:  3-6x faster (SIMD predicates, batch processing)");
    System.out.println("• Aggregation: 4-10x faster (vectorized math, reduced overhead)");
    System.out.println();

    System.out.println("Key Optimizations Implemented:");
    System.out.println("✓ Batch-oriented processing (2048 rows per batch)");
    System.out.println("✓ Columnar data access patterns");
    System.out.println("✓ Vectorized predicate evaluation");
    System.out.println("✓ Chunked aggregation with unrolled loops");
    System.out.println("✓ Zero-copy data projection");
    System.out.println("✓ Cache-friendly memory access patterns");
    System.out.println();

    System.out.println("Scaling Characteristics:");
    System.out.println("• Vectorized benefits increase with dataset size");
    System.out.println("• Analytical workloads see maximum improvement");
    System.out.println("• Numeric operations benefit most from vectorization");
    System.out.println("• String operations see moderate improvements");
  }

  /** Functional interface for benchmark operations. */
  @FunctionalInterface
  private interface BenchmarkOperation {
    long execute();
  }
}
