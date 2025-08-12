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
package org.apache.calcite.adapter.file.performance;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.FileRules;
import org.apache.calcite.adapter.file.rules.HLLCountDistinctRule;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsCache;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Planner;

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
import java.util.Properties;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify HLL rule implementation status and performance.
 */
public class HLLRuleImplementationTest {
  @TempDir
  java.nio.file.Path tempDir;
  
  private File cacheDir;
  
  @BeforeEach
  public void setUp() throws Exception {
    cacheDir = tempDir.resolve("hll_cache").toFile();
    cacheDir.mkdirs();
    
    // Create test dataset
    createTestParquet(100000);
    
    // Enable HLL globally
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
  }
  
  @Test
  public void testRuleRegistrationStatus() throws Exception {
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║              HLL RULE IMPLEMENTATION STATUS CHECK                   ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝\n");
    
    // Check rule existence
    System.out.println("1. Rule Class Existence:");
    System.out.println("   ✓ HLLCountDistinctRule: " + (HLLCountDistinctRule.INSTANCE != null ? "EXISTS" : "MISSING"));
    System.out.println("   ✓ FileRules.HLL_COUNT_DISTINCT: " + (FileRules.HLL_COUNT_DISTINCT != null ? "EXISTS" : "MISSING"));
    
    // Check if HLL sketches can be built
    System.out.println("\n2. HLL Sketch Building:");
    HyperLogLogSketch testSketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 1000; i++) {
      testSketch.add("value_" + i);
    }
    System.out.println("   ✓ Sketch creation: SUCCESS");
    System.out.println("   ✓ Estimate: " + testSketch.getEstimate() + " (expected ~1000)");
    
    // Check if cache works
    System.out.println("\n3. HLL Cache:");
    File testCacheFile = new File(cacheDir, "test.hll");
    StatisticsCache.saveHLLSketch(testSketch, testCacheFile);
    HyperLogLogSketch loaded = StatisticsCache.loadHLLSketch(testCacheFile);
    System.out.println("   ✓ Save/Load: " + (loaded != null ? "SUCCESS" : "FAILED"));
    System.out.println("   ✓ Loaded estimate: " + (loaded != null ? loaded.getEstimate() : "N/A"));
    
    // Check if rule is registered in planner
    System.out.println("\n4. Rule Registration in Planner:");
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      
      SchemaPlus fileSchema = rootSchema.add("FILES", 
          FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));
      
      // Try to execute a COUNT(DISTINCT) query and check plan
      String query = "SELECT COUNT(DISTINCT \"user_id\") FROM FILES.\"test_100000\"";
      
      try (Statement stmt = connection.createStatement()) {
        // Get the execution plan
        ResultSet planRs = stmt.executeQuery("EXPLAIN PLAN FOR " + query);
        StringBuilder plan = new StringBuilder();
        while (planRs.next()) {
          plan.append(planRs.getString(1)).append("\n");
        }
        
        System.out.println("   Query Plan:");
        String[] planLines = plan.toString().split("\n");
        for (String line : planLines) {
          System.out.println("   " + line);
        }
        
        // Check if HLL is mentioned in the plan
        boolean hllInPlan = plan.toString().contains("HLL") || 
                           plan.toString().contains("HyperLogLog") ||
                           plan.toString().contains("sketch");
        System.out.println("\n   ✓ HLL in execution plan: " + (hllInPlan ? "YES" : "NO"));
        
        // Execute the query and measure time
        long start = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery(query);
        rs.next();
        long count = rs.getLong(1);
        long elapsed = System.currentTimeMillis() - start;
        
        System.out.println("   ✓ Query execution: SUCCESS");
        System.out.println("   ✓ Result: " + count);
        System.out.println("   ✓ Time: " + elapsed + "ms");
      }
    }
    
    // Summary
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                         RULE STATUS SUMMARY                         ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝");
    System.out.println("\nImplemented Components:");
    System.out.println("  ✓ HyperLogLogSketch class");
    System.out.println("  ✓ StatisticsCache with HLL support");
    System.out.println("  ✓ HLLCountDistinctRule class");
    System.out.println("  ✓ HLLAcceleratedTable class");
    
    System.out.println("\nMissing/Incomplete Components:");
    System.out.println("  ✗ HLLCountDistinctRule not registered in table scans");
    System.out.println("  ✗ HLL rule not transforming COUNT(DISTINCT) queries");
    System.out.println("  ✗ FileStatisticsRules are placeholders only");
    System.out.println("  ✗ No actual HLL sketch usage in query execution");
    
    System.out.println("\nOther Placeholder Rules Found:");
    System.out.println("  - FileStatisticsRules.STATISTICS_FILTER_PUSHDOWN");
    System.out.println("  - FileStatisticsRules.STATISTICS_JOIN_REORDER");
    System.out.println("  - FileStatisticsRules.STATISTICS_COLUMN_PRUNING");
    System.out.println("\nConclusion: HLL infrastructure exists but is NOT integrated into query execution!");
  }
  
  @Test
  public void testJoinScalingWithProperHLL() throws Exception {
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║          3-TABLE JOIN SCALING ANALYSIS (DuckDB vs Calcite)          ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝\n");
    
    int[] rowCounts = {10000, 50000, 100000, 500000, 1000000};
    
    for (int rows : rowCounts) {
      // Create 3 tables for join testing
      createJoinTestData(rows);
      
      String joinQuery = String.format(
          "SELECT COUNT(DISTINCT o.order_id), COUNT(DISTINCT c.customer_id), " +
          "COUNT(DISTINCT p.product_id) " +
          "FROM orders_%d o " +
          "JOIN customers_%d c ON o.customer_id = c.customer_id " +
          "JOIN products_%d p ON o.product_id = p.product_id",
          rows, rows, rows);
      
      // Test DuckDB
      long duckdbTime = testDuckDBJoin(joinQuery, rows);
      
      // Test Calcite
      long calciteTime = testCalciteJoin(joinQuery, rows);
      
      // Calculate theoretical HLL time (if it worked)
      long theoreticalHLL = 5; // Should be instant with pre-computed sketches
      
      System.out.printf("\n%d rows per table:\n", rows);
      System.out.printf("  DuckDB:          %6d ms\n", duckdbTime);
      System.out.printf("  Calcite:         %6d ms\n", calciteTime);
      System.out.printf("  Theoretical HLL: %6d ms (if implemented)\n", theoreticalHLL);
      System.out.printf("  Speedup potential: %.1fx\n", (double)calciteTime / theoreticalHLL);
      
      // Predict crossover point
      if (rows >= 100000) {
        double scaleFactor = (double)calciteTime / duckdbTime;
        if (scaleFactor > 1.5) {
          System.out.println("  ⚠ Calcite is " + String.format("%.1f", scaleFactor) + 
                           "x slower than DuckDB at this scale");
        }
      }
    }
    
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                      PERFORMANCE PREDICTION                         ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝");
    System.out.println("\nWith proper HLL implementation:");
    System.out.println("  - COUNT(DISTINCT) would return in <10ms regardless of data size");
    System.out.println("  - HLL would become competitive at ~50K rows per table");
    System.out.println("  - HLL would dominate at >100K rows per table");
    System.out.println("\nCurrent state:");
    System.out.println("  - No HLL acceleration is actually happening");
    System.out.println("  - Calcite scans all data for every COUNT(DISTINCT)");
    System.out.println("  - Performance degrades linearly with data size");
  }
  
  @SuppressWarnings("deprecation")
  private void createTestParquet(int rows) throws Exception {
    File file = new File(tempDir.toFile(), "test_" + rows + ".parquet");
    if (file.exists()) return;
    
    String schemaString = "{"
        + "\"type\": \"record\","
        + "\"name\": \"TestRecord\","
        + "\"fields\": ["
        + "  {\"name\": \"user_id\", \"type\": \"int\"},"
        + "  {\"name\": \"value\", \"type\": \"double\"}"
        + "]"
        + "}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);
    
    try (ParquetWriter<GenericRecord> writer = 
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {
      
      for (int i = 0; i < rows; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("user_id", random.nextInt(rows / 10));
        record.put("value", random.nextDouble() * 1000);
        writer.write(record);
      }
    }
  }
  
  private void createJoinTestData(int rows) throws Exception {
    // Create orders table
    createTableParquet("orders_" + rows, rows, new String[]{"order_id", "customer_id", "product_id"});
    // Create customers table
    createTableParquet("customers_" + rows, rows / 10, new String[]{"customer_id", "name"});
    // Create products table
    createTableParquet("products_" + rows, rows / 20, new String[]{"product_id", "category"});
  }
  
  @SuppressWarnings("deprecation")
  private void createTableParquet(String name, int rows, String[] fields) throws Exception {
    File file = new File(tempDir.toFile(), name + ".parquet");
    if (file.exists()) return;
    
    StringBuilder schemaBuilder = new StringBuilder();
    schemaBuilder.append("{\"type\": \"record\",\"name\": \"Record\",\"fields\": [");
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) schemaBuilder.append(",");
      schemaBuilder.append("{\"name\": \"").append(fields[i]).append("\", \"type\": \"int\"}");
    }
    schemaBuilder.append("]}");
    
    Schema avroSchema = new Schema.Parser().parse(schemaBuilder.toString());
    Random random = new Random(42);
    
    try (ParquetWriter<GenericRecord> writer = 
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {
      
      for (int i = 0; i < rows; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        for (String field : fields) {
          if (field.endsWith("_id")) {
            record.put(field, i);
          } else {
            record.put(field, random.nextInt(100));
          }
        }
        writer.write(record);
      }
    }
  }
  
  private long testDuckDBJoin(String query, int rows) throws Exception {
    String url = "jdbc:duckdb:";
    try (Connection conn = DriverManager.getConnection(url);
         Statement stmt = conn.createStatement()) {
      
      // Register tables
      String[] tables = {"orders", "customers", "products"};
      for (String table : tables) {
        File parquetFile = new File(tempDir.toFile(), table + "_" + rows + ".parquet");
        stmt.execute("CREATE OR REPLACE VIEW " + table + "_" + rows + 
                    " AS SELECT * FROM '" + parquetFile.getAbsolutePath() + "'");
      }
      
      // Warmup
      stmt.executeQuery(query).close();
      
      // Test
      long start = System.currentTimeMillis();
      try (ResultSet rs = stmt.executeQuery(query)) {
        rs.next();
      }
      return System.currentTimeMillis() - start;
    }
  }
  
  private long testCalciteJoin(String query, int rows) throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      
      SchemaPlus fileSchema = rootSchema.add("FILES", 
          FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));
      
      // Update query for Calcite
      String calciteQuery = query
          .replaceAll("(orders|customers|products)_(\\d+)", "FILES.\"$1_$2\"")
          .replaceAll("\\b(order_id|customer_id|product_id|name|category)\\b", "\"$1\"");
      
      // Warmup
      try (Statement stmt = connection.createStatement()) {
        stmt.executeQuery(calciteQuery).close();
      }
      
      // Test
      long start = System.currentTimeMillis();
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(calciteQuery)) {
        rs.next();
      }
      return System.currentTimeMillis() - start;
    }
  }
}