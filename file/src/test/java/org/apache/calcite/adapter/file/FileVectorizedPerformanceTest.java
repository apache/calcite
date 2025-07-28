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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Performance test comparing LINQ4J vs Vectorized execution engines for file processing.
 *
 * <p>To run performance tests, remove @Disabled annotations and run:
 * <pre>
 * ./gradlew :file:test --tests FileVectorizedPerformanceTest
 * </pre>
 */
@Disabled("Performance tests are disabled by default to avoid slow builds")
public class FileVectorizedPerformanceTest {
  private static final int WARMUP_ITERATIONS = 3;
  private static final int TEST_ITERATIONS = 5;
  private Path tempDir;
  private JavaTypeFactory typeFactory;

  @BeforeEach
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("file-perf-test");
    typeFactory = new JavaTypeFactoryImpl();
  }

  @Test public void testCsvPerformance() throws Exception {
    System.out.println("=== CSV File Performance Test ===");

    // Test different dataset sizes
    int[] rowCounts = {10_000, 50_000, 100_000};

    for (int rowCount : rowCounts) {
      System.out.println("\nDataset size: " + rowCount + " rows");

      File csvFile = createTestCsvFile(rowCount);
      Source source = Sources.of(csvFile);

      // Test LINQ4J engine
      ExecutionEngineConfig linq4jConfig = new ExecutionEngineConfig("linq4j", 2048);
      long linq4jTime = measureCsvScanTime(source, linq4jConfig);

      // Test Vectorized engine
      ExecutionEngineConfig vectorizedConfig = new ExecutionEngineConfig("vectorized", 2048);
      long vectorizedTime = measureCsvScanTime(source, vectorizedConfig);

      // Calculate improvement
      double improvement = (double) linq4jTime / vectorizedTime;

      System.out.printf(Locale.ROOT, "LINQ4J:     %,d ms%n", linq4jTime);
      System.out.printf(Locale.ROOT, "Vectorized: %,d ms%n", vectorizedTime);
      System.out.printf(Locale.ROOT, "Improvement: %.2fx%n", improvement);
    }
  }

  @Test public void testJsonPerformance() throws Exception {
    System.out.println("\n=== JSON File Performance Test ===");

    int[] rowCounts = {5_000, 25_000, 50_000};

    for (int rowCount : rowCounts) {
      System.out.println("\nDataset size: " + rowCount + " rows");

      File jsonFile = createTestJsonFile(rowCount);
      Source source = Sources.of(jsonFile);

      // Test LINQ4J engine
      ExecutionEngineConfig linq4jConfig = new ExecutionEngineConfig("linq4j", 2048);
      long linq4jTime = measureJsonScanTime(source, linq4jConfig);

      // Test Vectorized engine
      ExecutionEngineConfig vectorizedConfig = new ExecutionEngineConfig("vectorized", 2048);
      long vectorizedTime = measureJsonScanTime(source, vectorizedConfig);

      // Calculate improvement
      double improvement = (double) linq4jTime / vectorizedTime;

      System.out.printf(Locale.ROOT, "LINQ4J:     %,d ms%n", linq4jTime);
      System.out.printf(Locale.ROOT, "Vectorized: %,d ms%n", vectorizedTime);
      System.out.printf(Locale.ROOT, "Improvement: %.2fx%n", improvement);
    }
  }

  @Test public void testBatchSizeImpact() throws Exception {
    System.out.println("\n=== Batch Size Impact Test ===");

    File csvFile = createTestCsvFile(100_000);
    Source source = Sources.of(csvFile);

    int[] batchSizes = {512, 1024, 2048, 4096, 8192};

    for (int batchSize : batchSizes) {
      ExecutionEngineConfig config = new ExecutionEngineConfig("vectorized", batchSize);
      long time = measureCsvScanTime(source, config);
      System.out.printf(Locale.ROOT, "Batch size %,5d: %,d ms%n", batchSize, time);
    }
  }

  private File createTestCsvFile(int rows) throws IOException {
    File file = new File(tempDir.toFile(), "test_" + rows + ".csv");
    Random rand = new Random(42);

    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      // Header
      writer.write("id:int,name:string,value:double,active:boolean,category:string\n");

      // Data
      for (int i = 0; i < rows; i++) {
        writer.write(
            String.format(Locale.ROOT, "%d,Product_%d,%.2f,%b,Category_%d\n",
            i,
            i,
            rand.nextDouble() * 1000,
            rand.nextBoolean(),
            rand.nextInt(10)));
      }
    }

    return file;
  }

  private File createTestJsonFile(int rows) throws IOException {
    File file = new File(tempDir.toFile(), "test_" + rows + ".json");
    Random rand = new Random(42);

    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write("[\n");

      for (int i = 0; i < rows; i++) {
        if (i > 0) writer.write(",\n");
        writer.write(
            String.format(Locale.ROOT,
            "  {\"id\": %d, \"name\": \"Product_%d\", \"value\": %.2f, " +
            "\"active\": %b, \"category\": \"Category_%d\"}",
            i, i, rand.nextDouble() * 1000, rand.nextBoolean(), rand.nextInt(10)));
      }

      writer.write("\n]\n");
    }

    return file;
  }

  private long measureCsvScanTime(Source source, ExecutionEngineConfig config) {
    EnhancedCsvTranslatableTable table = new EnhancedCsvTranslatableTable(source, null, config);

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      scanTable(table);
    }

    // Measure
    long totalTime = 0;
    for (int i = 0; i < TEST_ITERATIONS; i++) {
      long start = System.nanoTime();
      int rowCount = scanTable(table);
      long end = System.nanoTime();
      totalTime += end - start;
    }

    return TimeUnit.NANOSECONDS.toMillis(totalTime / TEST_ITERATIONS);
  }

  private long measureJsonScanTime(Source source, ExecutionEngineConfig config) {
    EnhancedJsonScannableTable table = new EnhancedJsonScannableTable(source, config);

    // Create a mock DataContext
    TestDataContext context = new TestDataContext(typeFactory);

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      scanJsonTable(table, context);
    }

    // Measure
    long totalTime = 0;
    for (int i = 0; i < TEST_ITERATIONS; i++) {
      long start = System.nanoTime();
      int rowCount = scanJsonTable(table, context);
      long end = System.nanoTime();
      totalTime += end - start;
    }

    return TimeUnit.NANOSECONDS.toMillis(totalTime / TEST_ITERATIONS);
  }

  private int scanTable(EnhancedCsvTranslatableTable table) {
    // Create a mock DataContext
    TestDataContext context = new TestDataContext(typeFactory);

    // Project all fields
    int[] fields = {0, 1, 2, 3, 4};
    int count = 0;

    for (Object row : table.project(context, fields)) {
      count++;
    }

    return count;
  }

  private int scanJsonTable(EnhancedJsonScannableTable table, TestDataContext context) {
    int count = 0;

    for (Object[] row : table.scan(context)) {
      count++;
    }

    return count;
  }

  /**
   * Simple test implementation of DataContext.
   */
  private static class TestDataContext implements org.apache.calcite.DataContext {
    private final JavaTypeFactory typeFactory;

    TestDataContext(JavaTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
    }

    @Override public org.apache.calcite.schema.SchemaPlus getRootSchema() {
      return null;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return typeFactory;
    }


    @Override public org.apache.calcite.linq4j.QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(String name) {
      if (name.equals("cancelFlag")) {
        return new java.util.concurrent.atomic.AtomicBoolean(false);
      }
      return null;
    }
  }
}
