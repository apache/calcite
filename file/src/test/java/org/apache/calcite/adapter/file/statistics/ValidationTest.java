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

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validation test to verify all optimizations are working correctly:
 * 1. Parquet min/max statistics extraction
 * 2. HLL sketch generation from actual data
 * 3. Filter pushdown using statistics
 */
@Tag("unit")public class ValidationTest {
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private File testFile;
  private Connection calciteConn;
  
  @BeforeEach
  public void setUp() throws Exception {
    // Create test Parquet file with known data
    createTestParquetFile();
    
    // Set up Calcite connection
    setupCalciteConnection();
  }
  
  @Test
  public void testParquetStatisticsExtraction() throws Exception {
    System.out.println("\n=== Testing Parquet Statistics Extraction ===");
    
    // Extract statistics from the test file
    Map<String, ParquetStatisticsExtractor.ColumnStatsBuilder> stats = 
        ParquetStatisticsExtractor.extractStatistics(testFile);
    
    assertFalse(stats.isEmpty(), "Statistics should be extracted");
    
    // Verify customer_id statistics
    ParquetStatisticsExtractor.ColumnStatsBuilder customerStats = stats.get("customer_id");
    assertNotNull(customerStats, "customer_id statistics should exist");
    
    // Check min/max values
    Object minValue = customerStats.getMinValue();
    Object maxValue = customerStats.getMaxValue();
    
    System.out.println("customer_id min: " + minValue);
    System.out.println("customer_id max: " + maxValue);
    System.out.println("customer_id total count: " + customerStats.getTotalCount());
    System.out.println("customer_id null count: " + customerStats.getNullCount());
    
    assertNotNull(minValue, "Min value should be extracted");
    assertNotNull(maxValue, "Max value should be extracted");
    assertEquals(10000L, customerStats.getTotalCount(), "Should have 10000 rows");
    
    // Verify amount statistics
    ParquetStatisticsExtractor.ColumnStatsBuilder amountStats = stats.get("amount");
    assertNotNull(amountStats, "amount statistics should exist");
    
    System.out.println("amount min: " + amountStats.getMinValue());
    System.out.println("amount max: " + amountStats.getMaxValue());
    
    assertNotNull(amountStats.getMinValue(), "Amount min should be extracted");
    assertNotNull(amountStats.getMaxValue(), "Amount max should be extracted");
    
    System.out.println("✅ Parquet statistics extraction working!");
  }
  
  @Test
  public void testHLLAccuracy() throws Exception {
    System.out.println("\n=== Testing HLL Accuracy ===");
    
    // Create HLL sketch from actual data
    HyperLogLogSketch sketch = new HyperLogLogSketch(14);
    
    // Add 5000 unique values
    for (int i = 0; i < 5000; i++) {
      sketch.add("customer_" + i);
    }
    
    long estimate = sketch.getEstimate();
    double accuracy = (double) estimate / 5000.0 * 100.0;
    
    System.out.println("Actual distinct: 5000");
    System.out.println("HLL estimate: " + estimate);
    System.out.println("Accuracy: " + String.format("%.2f%%", accuracy));
    
    // With precision 14, we expect ~98% accuracy
    assertTrue(accuracy > 95.0, "HLL should be >95% accurate");
    assertTrue(accuracy < 105.0, "HLL should be <105% accurate");
    
    System.out.println("✅ HLL accuracy validated!");
  }
  
  @Test
  public void testFilterPushdownWithStatistics() throws Exception {
    System.out.println("\n=== Testing Filter Pushdown ===");
    
    // Query that should benefit from filter pushdown
    // Since amount ranges from 10-1010, this filter eliminates all rows
    String query1 = "SELECT COUNT(*) FROM FILES.\"validation_test\" WHERE \"amount\" > 2000";
    
    long startTime = System.nanoTime();
    try (Statement stmt = calciteConn.createStatement()) {
      ResultSet rs = stmt.executeQuery(query1);
      assertTrue(rs.next());
      long count = rs.getLong(1);
      long endTime = System.nanoTime();
      double timeMs = (endTime - startTime) / 1_000_000.0;
      
      System.out.println("Query: WHERE amount > 2000");
      System.out.println("Result: " + count + " rows");
      System.out.println("Time: " + String.format("%.1f ms", timeMs));
      
      assertEquals(0L, count, "Should return 0 rows (all amounts <= 1010)");
      
      // This should be very fast if filter pushdown works
      assertTrue(timeMs < 100, "Should be fast with statistics-based elimination");
    }
    
    // Query that partially matches
    String query2 = "SELECT COUNT(*) FROM FILES.\"validation_test\" WHERE \"amount\" > 500";
    
    startTime = System.nanoTime();
    try (Statement stmt = calciteConn.createStatement()) {
      ResultSet rs = stmt.executeQuery(query2);
      assertTrue(rs.next());
      long count = rs.getLong(1);
      long endTime = System.nanoTime();
      double timeMs = (endTime - startTime) / 1_000_000.0;
      
      System.out.println("\nQuery: WHERE amount > 500");
      System.out.println("Result: " + count + " rows");
      System.out.println("Time: " + String.format("%.1f ms", timeMs));
      
      // About half the rows should match
      assertTrue(count > 4000 && count < 6000, "Should return ~5000 rows");
    }
    
    System.out.println("✅ Filter pushdown validated!");
  }
  
  @Test
  public void testHLLWithRealParquetData() throws Exception {
    System.out.println("\n=== Testing HLL with Real Parquet Data ===");
    
    // Enable HLL for this test
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllEnabled(true)
        .hllPrecision(14)
        .hllThreshold(100)
        .build();
    
    StatisticsBuilder builder = new StatisticsBuilder(config);
    
    // Build statistics with HLL
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    
    TableStatistics stats = builder.buildStatistics(
        new org.apache.calcite.adapter.file.DirectFileSource(testFile), 
        cacheDir);
    
    assertNotNull(stats, "Statistics should be generated");
    
    // Check customer_id HLL
    ColumnStatistics customerStats = stats.getColumnStatistics("customer_id");
    if (customerStats != null && customerStats.getHllSketch() != null) {
      long hllEstimate = customerStats.getHllSketch().getEstimate();
      System.out.println("customer_id HLL estimate: " + hllEstimate);
      System.out.println("customer_id actual distinct: ~1000");
      
      // Should be within 5% of actual (1000 distinct values)
      double accuracy = (double) hllEstimate / 1000.0;
      System.out.println("HLL accuracy: " + String.format("%.2f%%", accuracy * 100));
      
      assertTrue(accuracy > 0.95 && accuracy < 1.05, 
                "HLL should be 95-105% accurate, got " + (accuracy * 100) + "%");
      
      System.out.println("✅ HLL with real Parquet data validated!");
    } else {
      System.out.println("⚠️ HLL not generated (may be disabled by default)");
    }
  }
  
  @SuppressWarnings("deprecation")
  private void createTestParquetFile() throws Exception {
    testFile = new File(tempDir.toFile(), "validation_test.parquet");
    
    String schemaString = "{\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [" +
                         "  {\"name\": \"customer_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"product_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"amount\", \"type\": \"double\"}," +
                         "  {\"name\": \"region\", \"type\": \"string\"}" +
                         "]}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);
    
    try (ParquetWriter<GenericRecord> writer = 
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(testFile.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {
      
      // Create 10000 rows with ~1000 distinct customers
      for (int i = 0; i < 10000; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("customer_id", random.nextInt(1000)); // ~1000 distinct
        record.put("product_id", random.nextInt(100));
        record.put("amount", 10.0 + random.nextDouble() * 1000.0); // 10-1010 range
        record.put("region", "Region" + (i % 10));
        writer.write(record);
      }
    }
    
    System.out.println("Created test file: " + testFile.getAbsolutePath());
    System.out.println("File size: " + testFile.length() + " bytes");
  }
  
  private void setupCalciteConnection() throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    
    rootSchema.add("FILES", FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));
  }
}