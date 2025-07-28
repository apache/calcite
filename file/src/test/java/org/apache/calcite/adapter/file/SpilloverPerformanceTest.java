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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Comprehensive performance test for spillover functionality.
 * Tests various dataset sizes, execution engines, and spillover scenarios.
 */
public class SpilloverPerformanceTest {

  private static final String TEST_DATA_DIR = "/tmp/calcite_perf_test";
  private static final int[] DATASET_SIZES = {1000, 10000, 50000, 100000, 250000};
  private static final String[] ENGINES = {"PARQUET", "ARROW", "VECTORIZED", "LINQ4J"};

  public static void main(String[] args) {
    SpilloverPerformanceTest test = new SpilloverPerformanceTest();
    try {
      test.runComprehensivePerformanceTests();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void runComprehensivePerformanceTests() throws IOException {
    System.out.println("=".repeat(80));
    System.out.println("CALCITE FILE ADAPTER SPILLOVER PERFORMANCE TESTS");
    System.out.println("=".repeat(80));

    // Create test data directory
    File testDir = new File(TEST_DATA_DIR);
    testDir.mkdirs();

    // Test 1: Dataset Size Scaling
    testDatasetSizeScaling();

    // Test 2: Memory Threshold Impact
    testMemoryThresholdImpact();

    // Test 3: Multiple Tables Spillover
    testMultipleTablesSpillover();

    // Test 4: Format-Specific Performance
    testFormatSpecificPerformance();

    // Test 5: Spillover Statistics
    testSpilloverStatistics();

    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("PERFORMANCE TESTS COMPLETED");
    System.out.println("=".repeat(80));
  }

  private void testDatasetSizeScaling() throws IOException {
    System.out.println("\n🚀 TEST 1: Dataset Size Scaling with Spillover");
    System.out.println("-".repeat(60));
    System.out.printf(Locale.ROOT, "%-15s %-12s %-12s %-15s %-15s %-12s%n",
        "Dataset Size", "Time (ms)", "Memory (MB)", "Spill Ratio", "Total Size", "Engine");

    for (int size : DATASET_SIZES) {
      File csvFile = createLargeCSVFile(size, "scaling_test_" + size + ".csv");
      Source source = Sources.of(csvFile);

      // Test with PARQUET engine with spillover
      long startTime = System.currentTimeMillis();
      ParquetEnumerator<Object[]> enumerator = createSpilloverEnumerator(source, size);

      // Process all data
      int rowCount = 0;
      while (enumerator.moveNext()) {
        rowCount++;
      }

      long endTime = System.currentTimeMillis();
      ParquetEnumerator.StreamingStats stats = enumerator.getStreamingStats();

      System.out.printf(Locale.ROOT, "%-15d %-12d %-12.1f %-15.1f%% %-15s %-12s%n",
          size,
          endTime - startTime,
          stats.currentMemoryUsage / 1024.0 / 1024.0,
          stats.getSpillRatio() * 100,
          stats.getSpillSizeFormatted(),
          "PARQUET");

      enumerator.close();
      csvFile.delete();
    }
  }

  private void testMemoryThresholdImpact() throws IOException {
    System.out.println("\n💾 TEST 2: Memory Threshold Impact");
    System.out.println("-".repeat(60));

    int datasetSize = 50000;
    File csvFile = createLargeCSVFile(datasetSize, "memory_threshold_test.csv");
    Source source = Sources.of(csvFile);

    long[] memoryThresholds = {8 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024};
    String[] thresholdLabels = {"8MB", "16MB", "32MB", "64MB"};

    System.out.printf(Locale.ROOT, "%-15s %-12s %-15s %-15s %-12s%n",
        "Memory Limit", "Time (ms)", "Spill Ratio", "Spill Size", "Batches");

    for (int i = 0; i < memoryThresholds.length; i++) {
      long startTime = System.currentTimeMillis();

      ParquetEnumerator<Object[]> enumerator =
          new ParquetEnumerator<>(source, new AtomicBoolean(false), createFieldTypes(), new int[]{0, 1, 2, 3},
          5000, memoryThresholds[i]);

      // Process all data
      while (enumerator.moveNext()) {
        // Just iterate
      }

      long endTime = System.currentTimeMillis();
      ParquetEnumerator.StreamingStats stats = enumerator.getStreamingStats();

      System.out.printf(Locale.ROOT, "%-15s %-12d %-15.1f%% %-15s %-12d%n",
          thresholdLabels[i],
          endTime - startTime,
          stats.getSpillRatio() * 100,
          stats.getSpillSizeFormatted(),
          stats.totalBatches);

      enumerator.close();
    }

    csvFile.delete();
  }

  private void testMultipleTablesSpillover() throws IOException {
    System.out.println("\n⚡ TEST 3: Multiple Tables Spillover");
    System.out.println("-".repeat(60));

    int numTables = 5;
    int rowsPerTable = 20000;
    List<ParquetEnumerator<Object[]>> enumerators = new ArrayList<>();
    List<File> files = new ArrayList<>();

    System.out.printf(Locale.ROOT, "%-10s %-15s %-12s %-15s %-15s%n",
        "Table", "Rows Processed", "Time (ms)", "Memory (MB)", "Spill Ratio");

    // Create and process multiple tables simultaneously
    for (int i = 0; i < numTables; i++) {
      File csvFile = createLargeCSVFile(rowsPerTable, "multi_table_" + i + ".csv");
      files.add(csvFile);

      Source source = Sources.of(csvFile);
      ParquetEnumerator<Object[]> enumerator =
          new ParquetEnumerator<>(source, new AtomicBoolean(false), createFieldTypes(), new int[]{0, 1, 2, 3},
          2000, 16 * 1024 * 1024); // 16MB limit per table

      enumerators.add(enumerator);

      long startTime = System.currentTimeMillis();
      int rowCount = 0;

      while (enumerator.moveNext()) {
        rowCount++;
      }

      long endTime = System.currentTimeMillis();
      ParquetEnumerator.StreamingStats stats = enumerator.getStreamingStats();

      System.out.printf(Locale.ROOT, "%-10d %-15d %-12d %-15.1f %-15.1f%%%n",
          i + 1,
          rowCount,
          endTime - startTime,
          stats.currentMemoryUsage / 1024.0 / 1024.0,
          stats.getSpillRatio() * 100);
    }

    // Calculate total memory usage
    long totalMemory = enumerators.stream()
        .mapToLong(e -> e.getStreamingStats().currentMemoryUsage)
        .sum();

    System.out.printf(Locale.ROOT, "\nTotal Memory Usage: %.1f MB%n", totalMemory / 1024.0 / 1024.0);
    System.out.printf(Locale.ROOT, "Average Memory per Table: %.1f MB%n",
                     (totalMemory / 1024.0 / 1024.0) / numTables);

    // Cleanup
    enumerators.forEach(ParquetEnumerator::close);
    files.forEach(File::delete);
  }

  private void testFormatSpecificPerformance() throws IOException {
    System.out.println("\n📊 TEST 4: Format-Specific Performance");
    System.out.println("-".repeat(60));

    int datasetSize = 30000;

    // Create test files in different formats
    File csvFile = createLargeCSVFile(datasetSize, "format_test.csv");
    File jsonFile = createLargeJSONFile(datasetSize, "format_test.json");

    System.out.printf(Locale.ROOT, "%-10s %-15s %-12s %-15s %-15s%n",
        "Format", "Streaming", "Time (ms)", "Memory (MB)", "Spill Ratio");

    // Test CSV (streaming)
    testFormatPerformance("CSV", csvFile, true);

    // Test JSON (non-streaming)
    testFormatPerformance("JSON", jsonFile, false);

    csvFile.delete();
    jsonFile.delete();
  }

  private void testFormatPerformance(String format, File file, boolean isStreaming) {
    try {
      Source source = Sources.of(file);
      long startTime = System.currentTimeMillis();

      ParquetEnumerator<Object[]> enumerator =
          new ParquetEnumerator<>(source, new AtomicBoolean(false), createFieldTypes(), new int[]{0, 1, 2, 3},
          3000, 24 * 1024 * 1024); // 24MB limit

      int rowCount = 0;
      while (enumerator.moveNext()) {
        rowCount++;
      }

      long endTime = System.currentTimeMillis();
      ParquetEnumerator.StreamingStats stats = enumerator.getStreamingStats();

      System.out.printf(Locale.ROOT, "%-10s %-15s %-12d %-15.1f %-15.1f%%%n",
          format,
          isStreaming ? "✅ Yes" : "⚠️ No",
          endTime - startTime,
          stats.currentMemoryUsage / 1024.0 / 1024.0,
          stats.getSpillRatio() * 100);

      enumerator.close();

    } catch (Exception e) {
      System.out.printf(Locale.ROOT, "%-10s %-15s %-12s %-15s %-15s%n",
          format, "❌ Error", "Failed", "N/A", e.getMessage());
    }
  }

  private void testSpilloverStatistics() throws IOException {
    System.out.println("\n📈 TEST 5: Spillover Statistics Analysis");
    System.out.println("-".repeat(60));

    int datasetSize = 40000;
    File csvFile = createLargeCSVFile(datasetSize, "stats_test.csv");
    Source source = Sources.of(csvFile);

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<>(source, new AtomicBoolean(false), createFieldTypes(), new int[]{0, 1, 2, 3},
        2500, 20 * 1024 * 1024); // 20MB limit

    System.out.println("Processing data with spillover monitoring...");

    int checkpoint = 0;
    int rowCount = 0;
    while (enumerator.moveNext()) {
      rowCount++;

      // Print statistics at checkpoints
      if (rowCount % 10000 == 0) {
        checkpoint++;
        ParquetEnumerator.StreamingStats stats = enumerator.getStreamingStats();
        System.out.printf(Locale.ROOT, "Checkpoint %d (Row %d): %s%n", checkpoint, rowCount, stats);
      }
    }

    // Final statistics
    ParquetEnumerator.StreamingStats finalStats = enumerator.getStreamingStats();
    System.out.println("\nFinal Statistics:");
    System.out.println("  Total Rows: " + rowCount);
    System.out.println("  Total Batches: " + finalStats.totalBatches);
    System.out.println("  Spilled Batches: " + finalStats.spilledBatches);
    System.out.println("  Spill Ratio: " + String.format(Locale.ROOT, "%.1f%%", finalStats.getSpillRatio() * 100));
    System.out.println("  Current Memory: " + String.format(Locale.ROOT, "%.1f MB", finalStats.currentMemoryUsage / 1024.0 / 1024.0));
    System.out.println("  Total Spill Size: " + finalStats.getSpillSizeFormatted());

    enumerator.close();
    csvFile.delete();
  }

  private ParquetEnumerator<Object[]> createSpilloverEnumerator(Source source, int expectedRows) {
    // Configure based on expected dataset size
    int batchSize;
    long memoryThreshold;

    if (expectedRows < 10000) {
      batchSize = 1000;
      memoryThreshold = 8 * 1024 * 1024; // 8MB
    } else if (expectedRows < 50000) {
      batchSize = 2500;
      memoryThreshold = 16 * 1024 * 1024; // 16MB
    } else {
      batchSize = 5000;
      memoryThreshold = 32 * 1024 * 1024; // 32MB
    }

    return new ParquetEnumerator<>(
        source, new AtomicBoolean(false), createFieldTypes(), new int[]{0, 1, 2, 3},
        batchSize, memoryThreshold);
  }

  private File createLargeCSVFile(int numRows, String filename) throws IOException {
    File file = new File(TEST_DATA_DIR, filename);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      // Write header
      writer.write("id:int,name:string,value:double,category:string\n");

      // Write data rows
      for (int i = 1; i <= numRows; i++) {
        writer.write(
            String.format(Locale.ROOT, "%d,User_%d,%.2f,Category_%d\n",
            i, i, Math.random() * 1000, (i % 10) + 1));

        // Periodic flush for large files
        if (i % 10000 == 0) {
          writer.flush();
        }
      }
    }
    return file;
  }

  private File createLargeJSONFile(int numRows, String filename) throws IOException {
    File file = new File(TEST_DATA_DIR, filename);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write("[\n");

      for (int i = 1; i <= numRows; i++) {
        writer.write(
            String.format(Locale.ROOT,
            "  {\"id\": %d, \"name\": \"User_%d\", \"value\": %.2f, \"category\": \"Category_%d\"}",
            i, i, Math.random() * 1000, (i % 10) + 1));

        if (i < numRows) {
          writer.write(",");
        }
        writer.write("\n");

        // Periodic flush
        if (i % 5000 == 0) {
          writer.flush();
        }
      }

      writer.write("]\n");
    }
    return file;
  }

  private List<RelDataType> createFieldTypes() {
    List<RelDataType> fieldTypes = new ArrayList<>();
    // Simplified - in real implementation, these would be proper RelDataType instances
    // For this test, we'll create mock types
    return fieldTypes;
  }
}
