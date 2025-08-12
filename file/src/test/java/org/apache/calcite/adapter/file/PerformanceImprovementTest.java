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
 * Test to measure actual performance improvements from optimizations.
 */
public class PerformanceImprovementTest {
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private File testFile;
  private Connection withStatsConn;
  private Connection withoutStatsConn;
  
  @BeforeEach
  public void setUp() throws Exception {
    // Create a reasonably large test file
    createTestFile(50000); // 50K rows
    
    // Set up connection WITH statistics
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();
    generateStatistics(cacheDir);
    withStatsConn = createConnection(cacheDir);
    
    // Set up connection WITHOUT statistics (for comparison)
    File emptyCacheDir = tempDir.resolve("empty_cache").toFile();
    emptyCacheDir.mkdirs();
    withoutStatsConn = createConnection(emptyCacheDir);
  }
  
  @Test
  public void measureFilterPushdownImprovement() throws Exception {
    System.out.println("\n=== FILTER PUSHDOWN PERFORMANCE COMPARISON ===");
    
    // Query that should benefit from min/max statistics
    String[] queries = {
        "SELECT COUNT(*) FROM FILES.\"perf_test\" WHERE \"amount\" > 1500", // No rows match
        "SELECT COUNT(*) FROM FILES.\"perf_test\" WHERE \"amount\" < 5",    // No rows match
        "SELECT COUNT(*) FROM FILES.\"perf_test\" WHERE \"customer_id\" < 0", // No rows match
        "SELECT AVG(\"amount\") FROM FILES.\"perf_test\" WHERE \"amount\" > 800" // ~20% match
    };
    
    String[] descriptions = {
        "amount > 1500 (eliminates all)",
        "amount < 5 (eliminates all)",
        "customer_id < 0 (eliminates all)",
        "amount > 800 (partial scan)"
    };
    
    System.out.println("\n| Query | Without Stats (ms) | With Stats (ms) | Speedup |");
    System.out.println("|-------|-------------------|-----------------|---------|");
    
    for (int i = 0; i < queries.length; i++) {
      // Run without statistics
      long timeWithout = timeQuery(withoutStatsConn, queries[i]);
      
      // Run with statistics  
      long timeWith = timeQuery(withStatsConn, queries[i]);
      
      double speedup = (double) timeWithout / timeWith;
      
      System.out.printf("| %-30s | %17d | %15d | %6.2fx |\n",
                       descriptions[i], timeWithout, timeWith, speedup);
    }
    
    System.out.println("\n✅ Filter pushdown performance measured");
  }
  
  @Test
  public void measureComplexQueryImprovement() throws Exception {
    System.out.println("\n=== COMPLEX QUERY PERFORMANCE COMPARISON ===");
    
    String complexQuery = 
        "SELECT \"region\", " +
        "       COUNT(*) as total, " +
        "       COUNT(DISTINCT \"customer_id\") as customers, " +
        "       AVG(\"amount\") as avg_amount, " +
        "       MAX(\"amount\") - MIN(\"amount\") as range " +
        "FROM FILES.\"perf_test\" " +
        "WHERE \"amount\" BETWEEN 200 AND 800 " +
        "GROUP BY \"region\" " +
        "HAVING COUNT(*) > 100 " +
        "ORDER BY customers DESC";
    
    System.out.println("\nComplex aggregation with GROUP BY, HAVING, ORDER BY:");
    
    // Warm up
    executeQuery(withStatsConn, complexQuery);
    executeQuery(withoutStatsConn, complexQuery);
    
    // Measure
    long timeWithout = timeQuery(withoutStatsConn, complexQuery);
    long timeWith = timeQuery(withStatsConn, complexQuery);
    
    System.out.println("Without statistics: " + timeWithout + " ms");
    System.out.println("With statistics: " + timeWith + " ms");
    System.out.println("Speedup: " + String.format("%.2fx", (double) timeWithout / timeWith));
    
    System.out.println("\n✅ Complex query performance measured");
  }
  
  @Test
  public void verifyStatisticsCorrectness() throws Exception {
    System.out.println("\n=== VERIFYING STATISTICS CORRECTNESS ===");
    
    // Load generated statistics
    File cacheDir = tempDir.resolve("cache").toFile();
    File statsFile = new File(cacheDir, "perf_test.apericio_stats");
    
    assertTrue(statsFile.exists(), "Statistics file should exist");
    
    TableStatistics stats = StatisticsCache.loadStatistics(statsFile);
    assertNotNull(stats, "Should load statistics");
    
    System.out.println("\nTable Statistics:");
    System.out.println("  Row count: " + stats.getRowCount());
    System.out.println("  Data size: " + stats.getDataSize() + " bytes");
    
    // Verify column statistics
    String[] columns = {"customer_id", "product_id", "amount", "region"};
    for (String col : columns) {
      ColumnStatistics colStats = stats.getColumnStatistics(col);
      assertNotNull(colStats, col + " statistics should exist");
      
      System.out.println("\n" + col + ":");
      System.out.println("  Min: " + colStats.getMinValue());
      System.out.println("  Max: " + colStats.getMaxValue());
      System.out.println("  Nulls: " + colStats.getNullCount());
      
      // Verify min/max are present for filter pushdown
      assertNotNull(colStats.getMinValue(), col + " should have min");
      assertNotNull(colStats.getMaxValue(), col + " should have max");
    }
    
    // Verify specific ranges based on our data generation
    ColumnStatistics amountStats = stats.getColumnStatistics("amount");
    double minAmount = (Double) amountStats.getMinValue();
    double maxAmount = (Double) amountStats.getMaxValue();
    
    assertTrue(minAmount >= 10 && minAmount < 20, "Amount min should be 10-20");
    assertTrue(maxAmount >= 1000 && maxAmount < 1020, "Amount max should be 1000-1020");
    
    ColumnStatistics customerStats = stats.getColumnStatistics("customer_id");
    int minCustomer = (Integer) customerStats.getMinValue();
    int maxCustomer = (Integer) customerStats.getMaxValue();
    
    assertTrue(minCustomer >= 0, "Customer min should be >= 0");
    assertTrue(maxCustomer < 10000, "Customer max should be < 10000");
    
    System.out.println("\n✅ Statistics correctness verified");
  }
  
  private long timeQuery(Connection conn, String query) throws Exception {
    // Run query 3 times and take average
    long totalTime = 0;
    for (int i = 0; i < 3; i++) {
      long start = System.nanoTime();
      executeQuery(conn, query);
      totalTime += (System.nanoTime() - start) / 1_000_000;
    }
    return totalTime / 3;
  }
  
  private void executeQuery(Connection conn, String query) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        // Consume results
      }
    }
  }
  
  private void generateStatistics(File cacheDir) throws Exception {
    StatisticsBuilder builder = new StatisticsBuilder();
    TableStatistics stats = builder.buildStatistics(
        new org.apache.calcite.adapter.file.DirectFileSource(testFile),
        cacheDir);
    
    System.out.println("Generated statistics: " + stats.getRowCount() + " rows");
  }
  
  private Connection createConnection(File cacheDir) throws Exception {
    // Set cache directory
    System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
    
    Connection conn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    
    rootSchema.add("FILES", FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));
    
    return conn;
  }
  
  @SuppressWarnings("deprecation")
  private void createTestFile(int rowCount) throws Exception {
    testFile = new File(tempDir.toFile(), "perf_test.parquet");
    
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
      
      for (int i = 0; i < rowCount; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("customer_id", random.nextInt(10000));
        record.put("product_id", random.nextInt(100));
        record.put("amount", 10.0 + random.nextDouble() * 1000.0);
        record.put("region", "Region" + (i % 20));
        writer.write(record);
      }
    }
    
    System.out.println("Created test file: " + rowCount + " rows, " + testFile.length() + " bytes");
  }
}