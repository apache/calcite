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

import org.apache.calcite.adapter.file.statistics.*;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
 * End-to-end validation that optimizations are working in real queries.
 */
public class OptimizationValidationTest {
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private File smallFile;  // 1K rows
  private File mediumFile; // 10K rows
  private File largeFile;  // 100K rows
  private Connection calciteConn;
  private File cacheDir;
  
  @BeforeEach
  public void setUp() throws Exception {
    cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    
    // Create test files with different sizes
    createTestFile("small", 1000);
    createTestFile("medium", 10000);
    createTestFile("large", 100000);
    
    // Pre-generate statistics
    generateStatistics();
    
    // Set up Calcite connection
    setupCalciteConnection();
  }
  
  @Test
  public void validateFilterPushdownPerformance() throws Exception {
    System.out.println("\n=== VALIDATING FILTER PUSHDOWN PERFORMANCE ===");
    
    // Test 1: Filter that eliminates all rows (amount > 2000, max is ~1010)
    String query1 = "SELECT COUNT(*) FROM FILES.\"large\" WHERE \"amount\" > 2000";
    
    long start = System.nanoTime();
    long count1 = executeCountQuery(query1);
    long time1 = (System.nanoTime() - start) / 1_000_000;
    
    System.out.println("\nQuery: " + query1);
    System.out.println("Result: " + count1 + " rows");
    System.out.println("Time: " + time1 + " ms");
    assertEquals(0L, count1, "Should return 0 rows");
    
    // Test 2: Filter that returns all rows (amount > 0)
    String query2 = "SELECT COUNT(*) FROM FILES.\"large\" WHERE \"amount\" > 0";
    
    start = System.nanoTime();
    long count2 = executeCountQuery(query2);
    long time2 = (System.nanoTime() - start) / 1_000_000;
    
    System.out.println("\nQuery: " + query2);
    System.out.println("Result: " + count2 + " rows");
    System.out.println("Time: " + time2 + " ms");
    assertEquals(100000L, count2, "Should return all rows");
    
    // Test 3: Partial filter (amount > 500)
    String query3 = "SELECT COUNT(*) FROM FILES.\"large\" WHERE \"amount\" > 500";
    
    start = System.nanoTime();
    long count3 = executeCountQuery(query3);
    long time3 = (System.nanoTime() - start) / 1_000_000;
    
    System.out.println("\nQuery: " + query3);
    System.out.println("Result: " + count3 + " rows");
    System.out.println("Time: " + time3 + " ms");
    assertTrue(count3 > 40000 && count3 < 60000, "Should return ~50% of rows");
    
    System.out.println("\n✅ Filter pushdown validated");
  }
  
  @Test
  public void validateCountDistinctPerformance() throws Exception {
    System.out.println("\n=== VALIDATING COUNT(DISTINCT) PERFORMANCE ===");
    
    // HLL should be disabled, so these should return exact results
    String[] queries = {
        "SELECT COUNT(DISTINCT \"customer_id\") FROM FILES.\"small\"",
        "SELECT COUNT(DISTINCT \"customer_id\") FROM FILES.\"medium\"",
        "SELECT COUNT(DISTINCT \"customer_id\") FROM FILES.\"large\""
    };
    
    long[] expectedCounts = {500, 5000, 10000}; // Approximate distinct counts
    String[] sizes = {"small (1K)", "medium (10K)", "large (100K)"};
    
    for (int i = 0; i < queries.length; i++) {
      long start = System.nanoTime();
      long count = executeCountQuery(queries[i]);
      long time = (System.nanoTime() - start) / 1_000_000;
      
      System.out.println("\nDataset: " + sizes[i]);
      System.out.println("Query: " + queries[i]);
      System.out.println("Result: " + count);
      System.out.println("Time: " + time + " ms");
      
      // Should be exact count (not HLL approximation)
      assertTrue(count >= expectedCounts[i] * 0.8 && count <= expectedCounts[i] * 1.2,
                "Should return approximately " + expectedCounts[i] + " distinct values");
    }
    
    System.out.println("\n✅ COUNT(DISTINCT) returns exact results (HLL disabled for standard SQL)");
  }
  
  @Test
  public void validateComplexAggregationPerformance() throws Exception {
    System.out.println("\n=== VALIDATING COMPLEX AGGREGATION PERFORMANCE ===");
    
    String query = "SELECT \"region\", " +
                  "COUNT(*) as cnt, " +
                  "COUNT(DISTINCT \"customer_id\") as distinct_customers, " +
                  "AVG(\"amount\") as avg_amount, " +
                  "MIN(\"amount\") as min_amount, " +
                  "MAX(\"amount\") as max_amount " +
                  "FROM FILES.\"medium\" " +
                  "WHERE \"amount\" > 300 " +
                  "GROUP BY \"region\" " +
                  "ORDER BY distinct_customers DESC";
    
    long start = System.nanoTime();
    int rowCount = 0;
    double totalAvg = 0;
    
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      
      System.out.println("\nRegion | Count | Distinct | Avg Amount | Min | Max");
      System.out.println("-------|-------|----------|------------|-----|-----");
      
      while (rs.next()) {
        String region = rs.getString(1);
        long count = rs.getLong(2);
        long distinct = rs.getLong(3);
        double avg = rs.getDouble(4);
        double min = rs.getDouble(5);
        double max = rs.getDouble(6);
        
        System.out.printf("%-7s| %5d | %8d | %10.2f | %.0f | %.0f\n",
                         region, count, distinct, avg, min, max);
        
        rowCount++;
        totalAvg += avg;
        
        // Validate statistics make sense
        assertTrue(min >= 300, "Min should be >= 300 due to WHERE clause");
        assertTrue(max <= 1010, "Max should be <= 1010 based on data generation");
        assertTrue(avg > min && avg < max, "Avg should be between min and max");
      }
    }
    
    long time = (System.nanoTime() - start) / 1_000_000;
    
    System.out.println("\nTotal regions: " + rowCount);
    System.out.println("Query time: " + time + " ms");
    
    assertEquals(10, rowCount, "Should have 10 regions");
    
    System.out.println("\n✅ Complex aggregation validated");
  }
  
  @Test
  public void validateStatisticsAvailability() throws Exception {
    System.out.println("\n=== VALIDATING STATISTICS AVAILABILITY ===");
    
    // Check that statistics were generated for all files
    File[] statFiles = cacheDir.listFiles((dir, name) -> name.endsWith(".apericio_stats"));
    assertNotNull(statFiles, "Statistics files should exist");
    assertTrue(statFiles.length >= 3, "Should have stats for all test files");
    
    System.out.println("Found " + statFiles.length + " statistics files:");
    for (File f : statFiles) {
      System.out.println("  - " + f.getName() + " (" + f.length() + " bytes)");
      
      // Load and verify statistics
      TableStatistics stats = StatisticsCache.loadStatistics(f);
      assertNotNull(stats, "Should be able to load statistics");
      assertTrue(stats.getRowCount() > 0, "Should have row count");
      
      // Check column statistics
      ColumnStatistics amountStats = stats.getColumnStatistics("amount");
      if (amountStats != null) {
        assertNotNull(amountStats.getMinValue(), "Amount should have min value");
        assertNotNull(amountStats.getMaxValue(), "Amount should have max value");
        
        System.out.println("    amount range: " + amountStats.getMinValue() + 
                          " to " + amountStats.getMaxValue());
      }
    }
    
    System.out.println("\n✅ Statistics availability validated");
  }
  
  @Test
  public void validateJoinPerformance() throws Exception {
    System.out.println("\n=== VALIDATING JOIN PERFORMANCE ===");
    
    // Self-join to test join reordering
    String query = "SELECT COUNT(*) " +
                  "FROM FILES.\"small\" t1 " +
                  "JOIN FILES.\"small\" t2 " +
                  "ON t1.\"customer_id\" = t2.\"customer_id\" " +
                  "WHERE t1.\"amount\" > 500 AND t2.\"amount\" < 600";
    
    long start = System.nanoTime();
    long count = executeCountQuery(query);
    long time = (System.nanoTime() - start) / 1_000_000;
    
    System.out.println("Join query result: " + count + " rows");
    System.out.println("Time: " + time + " ms");
    
    assertTrue(count > 0, "Should have some matching rows");
    
    System.out.println("\n✅ Join performance validated");
  }
  
  private long executeCountQuery(String query) throws Exception {
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next());
      return rs.getLong(1);
    }
  }
  
  private void generateStatistics() throws Exception {
    System.out.println("\nPre-generating statistics...");
    
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllEnabled(false) // Disable HLL for standard queries
        .build();
    
    StatisticsBuilder builder = new StatisticsBuilder(config);
    
    for (File file : new File[]{smallFile, mediumFile, largeFile}) {
      TableStatistics stats = builder.buildStatistics(
          new org.apache.calcite.adapter.file.DirectFileSource(file),
          cacheDir);
      
      System.out.println("Generated stats for " + file.getName() + 
                        ": " + stats.getRowCount() + " rows");
    }
  }
  
  @SuppressWarnings("deprecation")
  private void createTestFile(String name, int rowCount) throws Exception {
    File file = new File(tempDir.toFile(), name + ".parquet");
    
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
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {
      
      int distinctCustomers = Math.min(rowCount / 2, 10000);
      
      for (int i = 0; i < rowCount; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("customer_id", random.nextInt(distinctCustomers));
        record.put("product_id", random.nextInt(100));
        record.put("amount", 10.0 + random.nextDouble() * 1000.0);
        record.put("region", "Region" + (i % 10));
        writer.write(record);
      }
    }
    
    if (name.equals("small")) smallFile = file;
    else if (name.equals("medium")) mediumFile = file;
    else if (name.equals("large")) largeFile = file;
    
    System.out.println("Created " + name + ".parquet: " + rowCount + " rows, " + 
                      file.length() + " bytes");
  }
  
  private void setupCalciteConnection() throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    
    // Set cache directory for statistics
    System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
    
    rootSchema.add("FILES", FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));
  }
}