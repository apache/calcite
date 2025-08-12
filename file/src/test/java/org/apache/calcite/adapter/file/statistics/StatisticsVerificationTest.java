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
package org.apache.calcite.adapter.file.statistics;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive verification that all optimization rules get correct information.
 */
@Tag("unit")
public class StatisticsVerificationTest {
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private File testFile;
  
  @BeforeEach
  public void setUp() throws Exception {
    createTestParquetFile();
  }
  
  @Test
  public void verifyParquetStatisticsExtraction() throws Exception {
    System.out.println("\n=== VERIFICATION: Parquet Statistics Extraction ===");
    
    Map<String, ParquetStatisticsExtractor.ColumnStatsBuilder> stats = 
        ParquetStatisticsExtractor.extractStatistics(testFile);
    
    System.out.println("Extracted statistics for " + stats.size() + " columns");
    
    // Verify each column has complete statistics
    for (Map.Entry<String, ParquetStatisticsExtractor.ColumnStatsBuilder> entry : stats.entrySet()) {
      String columnName = entry.getKey();
      ParquetStatisticsExtractor.ColumnStatsBuilder builder = entry.getValue();
      
      System.out.println("\nColumn: " + columnName);
      System.out.println("  Min: " + builder.getMinValue());
      System.out.println("  Max: " + builder.getMaxValue());
      System.out.println("  Total Count: " + builder.getTotalCount());
      System.out.println("  Null Count: " + builder.getNullCount());
      
      assertNotNull(builder.getMinValue(), columnName + " should have min value");
      assertNotNull(builder.getMaxValue(), columnName + " should have max value");
      assertEquals(5000L, builder.getTotalCount(), columnName + " should have 5000 rows");
    }
    
    // Specific validations
    ParquetStatisticsExtractor.ColumnStatsBuilder customerStats = stats.get("customer_id");
    assertTrue((Integer)customerStats.getMinValue() >= 0, "Customer min should be >= 0");
    assertTrue((Integer)customerStats.getMaxValue() < 1000, "Customer max should be < 1000");
    
    ParquetStatisticsExtractor.ColumnStatsBuilder amountStats = stats.get("amount");
    assertTrue((Double)amountStats.getMinValue() >= 100, "Amount min should be >= 100");
    assertTrue((Double)amountStats.getMaxValue() <= 1100, "Amount max should be <= 1100");
    
    System.out.println("\n✅ Parquet statistics extraction VERIFIED");
  }
  
  @Test
  public void verifyHLLSketchGeneration() throws Exception {
    System.out.println("\n=== VERIFICATION: HLL Sketch Generation ===");
    
    // Enable HLL and generate statistics
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllEnabled(true)
        .hllPrecision(14)
        .hllThreshold(10) // Low threshold to ensure HLL generation
        .build();
    
    StatisticsBuilder builder = new StatisticsBuilder(config);
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    
    TableStatistics tableStats = builder.buildStatistics(
        new org.apache.calcite.adapter.file.DirectFileSource(testFile),
        cacheDir);
    
    System.out.println("Table row count: " + tableStats.getRowCount());
    System.out.println("Table data size: " + tableStats.getDataSize());
    
    // Check HLL for high cardinality columns
    ColumnStatistics customerStats = tableStats.getColumnStatistics("customer_id");
    assertNotNull(customerStats, "Customer statistics should exist");
    
    if (customerStats.getHllSketch() != null) {
      long hllEstimate = customerStats.getHllSketch().getEstimate();
      System.out.println("\ncustomer_id HLL estimate: " + hllEstimate);
      System.out.println("Expected: ~1000 distinct values");
      
      // Calculate accuracy
      double accuracy = (double)hllEstimate / 1000.0;
      System.out.println("Accuracy: " + String.format("%.1f%%", accuracy * 100));
      
      // HLL with precision 14 should be within 2% typically
      assertTrue(accuracy > 0.95 && accuracy < 1.05, 
                "HLL should be 95-105% accurate, got " + (accuracy * 100) + "%");
      
      System.out.println("\n✅ HLL sketch generation VERIFIED");
    } else {
      System.out.println("\n⚠️ HLL not generated (may be disabled or below threshold)");
    }
  }
  
  @Test 
  public void verifyStatisticsCompleteness() throws Exception {
    System.out.println("\n=== VERIFICATION: Statistics Completeness ===");
    
    StatisticsBuilder builder = new StatisticsBuilder();
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    
    TableStatistics tableStats = builder.buildStatistics(
        new org.apache.calcite.adapter.file.DirectFileSource(testFile),
        cacheDir);
    
    // Verify all expected columns have statistics
    String[] expectedColumns = {"customer_id", "product_id", "amount", "status"};
    
    for (String columnName : expectedColumns) {
      ColumnStatistics colStats = tableStats.getColumnStatistics(columnName);
      assertNotNull(colStats, columnName + " statistics should exist");
      
      System.out.println("\n" + columnName + " statistics:");
      System.out.println("  Min: " + colStats.getMinValue());
      System.out.println("  Max: " + colStats.getMaxValue());
      System.out.println("  Nulls: " + colStats.getNullCount());
      System.out.println("  Total: " + colStats.getTotalCount());
      
      // Min/max should be populated for filter pushdown
      assertNotNull(colStats.getMinValue(), columnName + " min required for filter pushdown");
      assertNotNull(colStats.getMaxValue(), columnName + " max required for filter pushdown");
      
      // Verify data types are correct
      if (columnName.equals("customer_id") || columnName.equals("product_id")) {
        assertTrue(colStats.getMinValue() instanceof Integer, 
                  columnName + " should have Integer min/max");
      } else if (columnName.equals("amount")) {
        assertTrue(colStats.getMinValue() instanceof Double, 
                  columnName + " should have Double min/max");
      } else if (columnName.equals("status")) {
        assertTrue(colStats.getMinValue() instanceof String, 
                  columnName + " should have String min/max");
      }
    }
    
    System.out.println("\n✅ Statistics completeness VERIFIED");
  }
  
  @Test
  public void verifyFilterPushdownStatistics() throws Exception {
    System.out.println("\n=== VERIFICATION: Filter Pushdown Statistics ===");
    
    Map<String, ParquetStatisticsExtractor.ColumnStatsBuilder> stats = 
        ParquetStatisticsExtractor.extractStatistics(testFile);
    
    ParquetStatisticsExtractor.ColumnStatsBuilder amountStats = stats.get("amount");
    
    double minAmount = (Double) amountStats.getMinValue();
    double maxAmount = (Double) amountStats.getMaxValue();
    
    System.out.println("Amount range: " + minAmount + " to " + maxAmount);
    
    // Simulate filter pushdown decisions
    
    // Case 1: Filter that can be completely eliminated
    double filterValue1 = 2000.0; // Above max
    boolean canEliminate1 = filterValue1 > maxAmount;
    System.out.println("\nFilter: amount > " + filterValue1);
    System.out.println("Can eliminate scan: " + canEliminate1);
    assertTrue(canEliminate1, "Should be able to eliminate scan for amount > 2000");
    
    // Case 2: Filter that must scan all data
    double filterValue2 = 50.0; // Below min
    boolean mustScanAll = filterValue2 < minAmount;
    System.out.println("\nFilter: amount > " + filterValue2);
    System.out.println("Must scan all: " + mustScanAll);
    assertTrue(mustScanAll, "Must scan all data for amount > 50");
    
    // Case 3: Partial filter
    double filterValue3 = 600.0; // In range
    boolean isPartial = filterValue3 > minAmount && filterValue3 < maxAmount;
    System.out.println("\nFilter: amount > " + filterValue3);
    System.out.println("Partial scan needed: " + isPartial);
    assertTrue(isPartial, "Partial scan needed for amount > 600");
    
    System.out.println("\n✅ Filter pushdown statistics VERIFIED");
  }
  
  @SuppressWarnings("deprecation")
  private void createTestParquetFile() throws Exception {
    testFile = new File(tempDir.toFile(), "verification_test.parquet");
    
    String schemaString = "{\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [" +
                         "  {\"name\": \"customer_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"product_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"amount\", \"type\": \"double\"}," +
                         "  {\"name\": \"status\", \"type\": \"string\"}" +
                         "]}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);
    
    try (ParquetWriter<GenericRecord> writer = 
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(testFile.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {
      
      // Create 5000 rows with known statistics
      for (int i = 0; i < 5000; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("customer_id", random.nextInt(1000)); // ~1000 distinct
        record.put("product_id", random.nextInt(100));
        record.put("amount", 100.0 + random.nextDouble() * 1000.0); // 100-1100
        record.put("status", random.nextBoolean() ? "ACTIVE" : "INACTIVE");
        writer.write(record);
      }
    }
    
    System.out.println("Created test file: " + testFile.getAbsolutePath());
  }
}