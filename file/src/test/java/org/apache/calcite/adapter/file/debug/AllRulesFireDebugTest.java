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
package org.apache.calcite.adapter.file.debug;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsCache;
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Comprehensive test to verify that ALL HLL and statistics rules are firing.
 * Tests each rule individually and in combination to ensure complete coverage.
 */
public class AllRulesFireDebugTest {
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private File cacheDir;
  private Connection calciteConn;
  
  @BeforeEach
  public void setUp() throws Exception {
    cacheDir = tempDir.resolve("hll_cache").toFile();
    cacheDir.mkdirs();
    
    // Enable ALL optimizations
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("calcite.file.statistics.filter.enabled", "true"); 
    System.setProperty("calcite.file.statistics.join.reorder.enabled", "true");
    System.setProperty("calcite.file.statistics.column.pruning.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
    
    // Create multiple test datasets for different rule scenarios
    createHLLTestData();
    createFilterTestData(); 
    createJoinTestData();
    
    // Pre-compute all HLL sketches
    precomputeAllHLLSketches();
    
    // Setup Calcite connection
    setupCalciteConnection();
  }
  
  @Test
  public void testAllRulesFiring() throws Exception {
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                            COMPREHENSIVE RULE FIRING TEST                                  ║");
    System.out.println("║                         Testing ALL HLL and Statistics Rules                               ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════════╝\n");
    
    // Test each rule category
    testHLLCountDistinctRule();
    testFilterPushdownRule();
    testJoinReorderRule(); 
    testColumnPruningRule();
    testCombinedRules();
    
    // Final summary
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                                RULE FIRING SUMMARY                                          ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════════╝");
    
    System.out.println("\n📊 RULE ACTIVATION STATUS:");
    System.out.println("   [?] HLLCountDistinctRule - Loads sketches, needs plan transformation");
    System.out.println("   [?] FileFilterPushdownRule - May need selective queries to trigger");
    System.out.println("   [?] FileJoinReorderRule - May need multi-table joins to trigger");
    System.out.println("   [?] FileColumnPruningRule - May need projection queries to trigger");
  }
  
  private void testHLLCountDistinctRule() throws Exception {
    System.out.println("Testing HLLCountDistinctRule:");
    System.out.println("═══════════════════════════");
    
    String query = "SELECT " +
                  "COUNT(DISTINCT \"order_id\") as distinct_orders, " +
                  "COUNT(DISTINCT \"customer_id\") as distinct_customers " +
                  "FROM FILES.\"hll_test\"";
    
    String ruleTrace = executeQueryWithTrace(query, "HLL COUNT(DISTINCT)");
    analyzeRuleTrace(ruleTrace, "HLL");
  }
  
  private void testFilterPushdownRule() throws Exception {
    System.out.println("\nTesting FileFilterPushdownRule:");
    System.out.println("══════════════════════════════");
    
    // Query with selective filter that should benefit from statistics
    String query = "SELECT COUNT(*) " +
                  "FROM FILES.\"filter_test\" " +
                  "WHERE \"amount\" > 500 AND \"amount\" < 750";
    
    String ruleTrace = executeQueryWithTrace(query, "Filter pushdown");
    analyzeRuleTrace(ruleTrace, "FilterPushdown|STATISTICS_FILTER");
  }
  
  private void testJoinReorderRule() throws Exception {
    System.out.println("\nTesting FileJoinReorderRule:");
    System.out.println("═══════════════════════════");
    
    // Multi-table join that should benefit from statistics-based reordering
    String query = "SELECT o.\"order_id\", c.\"customer_id\", p.\"product_id\" " +
                  "FROM FILES.\"orders_join\" o " +
                  "JOIN FILES.\"customers_join\" c ON o.\"customer_id\" = c.\"customer_id\" " +
                  "JOIN FILES.\"products_join\" p ON o.\"product_id\" = p.\"product_id\" " +
                  "LIMIT 10";
    
    String ruleTrace = executeQueryWithTrace(query, "Join reorder");
    analyzeRuleTrace(ruleTrace, "JoinReorder|STATISTICS_JOIN");
  }
  
  private void testColumnPruningRule() throws Exception {
    System.out.println("\nTesting FileColumnPruningRule:");
    System.out.println("═════════════════════════════");
    
    // Query that only uses subset of columns - should trigger pruning
    String query = "SELECT \"order_id\", \"amount\" " +
                  "FROM FILES.\"hll_test\" " +
                  "WHERE \"order_id\" < 100";
    
    String ruleTrace = executeQueryWithTrace(query, "Column pruning");
    analyzeRuleTrace(ruleTrace, "ColumnPruning|STATISTICS_COLUMN");
  }
  
  private void testCombinedRules() throws Exception {
    System.out.println("\nTesting Combined Rules:");
    System.out.println("══════════════════════");
    
    // Complex query that should trigger multiple rules
    String query = "SELECT " +
                  "p.\"product_id\", " +
                  "COUNT(DISTINCT o.\"order_id\") as distinct_orders, " +
                  "AVG(o.\"amount\") as avg_amount " +
                  "FROM FILES.\"orders_join\" o " +
                  "JOIN FILES.\"products_join\" p ON o.\"product_id\" = p.\"product_id\" " +
                  "WHERE o.\"amount\" > 100 " +
                  "GROUP BY p.\"product_id\" " +
                  "HAVING COUNT(*) > 5";
    
    String ruleTrace = executeQueryWithTrace(query, "Combined optimization");
    
    // Check for multiple rule types
    System.out.println("  Looking for multiple rule activations:");
    if (ruleTrace.contains("HLL") || ruleTrace.contains("sketch")) {
      System.out.println("    ✓ HLL rules detected");
    }
    if (ruleTrace.contains("Filter") || ruleTrace.contains("pushdown")) {
      System.out.println("    ✓ Filter pushdown detected");
    }
    if (ruleTrace.contains("Join") || ruleTrace.contains("reorder")) {
      System.out.println("    ✓ Join reorder detected");
    }
    if (ruleTrace.contains("Column") || ruleTrace.contains("pruning")) {
      System.out.println("    ✓ Column pruning detected");
    }
  }
  
  private String executeQueryWithTrace(String query, String description) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    
    try {
      System.setOut(new PrintStream(baos));
      
      try (Statement stmt = calciteConn.createStatement()) {
        // Execute query and capture any debug output
        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()) {
          // Just consume first row
        }
        rs.close();
      }
    } catch (Exception e) {
      System.setOut(originalOut);
      System.out.println("  ⚠️ Query failed: " + e.getMessage());
      return "";
    } finally {
      System.setOut(originalOut);
    }
    
    String trace = baos.toString();
    System.out.println("  Query: " + description);
    return trace;
  }
  
  private void analyzeRuleTrace(String trace, String rulePattern) {
    String[] patterns = rulePattern.split("\\|");
    boolean foundRule = false;
    
    for (String pattern : patterns) {
      if (trace.toLowerCase().contains(pattern.toLowerCase())) {
        System.out.println("    ✓ " + pattern + " rule activated");
        foundRule = true;
        
        // Show specific trace lines
        String[] lines = trace.split("\n");
        for (String line : lines) {
          if (line.toLowerCase().contains(pattern.toLowerCase())) {
            System.out.println("      └─ " + line.trim());
          }
        }
      }
    }
    
    if (!foundRule) {
      System.out.println("    ❌ No " + rulePattern + " rule activation detected");
      // Show trace for debugging if short
      if (trace.length() > 0 && trace.length() < 500) {
        System.out.println("    Debug trace: " + trace.trim());
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private void createHLLTestData() throws Exception {
    File file = new File(tempDir.toFile(), "hll_test.parquet");
    
    String schemaString = "{\"type\": \"record\",\"name\": \"HLLRecord\",\"fields\": [" +
                         "  {\"name\": \"order_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"customer_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"product_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"amount\", \"type\": \"double\"}" +
                         "]}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);
    
    try (ParquetWriter<GenericRecord> writer = 
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {
      
      for (int i = 0; i < 1000; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("order_id", i);
        record.put("customer_id", i % 100);
        record.put("product_id", i % 50);
        record.put("amount", 100.0 + random.nextDouble() * 900.0);
        writer.write(record);
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private void createFilterTestData() throws Exception {
    File file = new File(tempDir.toFile(), "filter_test.parquet");
    
    String schemaString = "{\"type\": \"record\",\"name\": \"FilterRecord\",\"fields\": [" +
                         "  {\"name\": \"id\", \"type\": \"int\"}," +
                         "  {\"name\": \"amount\", \"type\": \"double\"}," +
                         "  {\"name\": \"category\", \"type\": \"string\"}" +
                         "]}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);
    String[] categories = {"A", "B", "C", "D", "E"};
    
    try (ParquetWriter<GenericRecord> writer = 
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {
      
      for (int i = 0; i < 1000; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", i);
        record.put("amount", random.nextDouble() * 1000.0);
        record.put("category", categories[random.nextInt(categories.length)]);
        writer.write(record);
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private void createJoinTestData() throws Exception {
    // Create orders table
    createJoinTable("orders_join", 1000, new String[]{"order_id", "customer_id", "product_id", "amount"});
    
    // Create customers table  
    createJoinTable("customers_join", 100, new String[]{"customer_id", "customer_name", "region"});
    
    // Create products table
    createJoinTable("products_join", 50, new String[]{"product_id", "product_name", "category"});
  }
  
  @SuppressWarnings("deprecation")
  private void createJoinTable(String tableName, int rows, String[] fields) throws Exception {
    File file = new File(tempDir.toFile(), tableName + ".parquet");
    
    StringBuilder schemaBuilder = new StringBuilder();
    schemaBuilder.append("{\"type\": \"record\",\"name\": \"JoinRecord\",\"fields\": [");
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) schemaBuilder.append(",");
      String type = fields[i].contains("name") || fields[i].contains("region") || fields[i].contains("category") 
          ? "string" : (fields[i].equals("amount") ? "double" : "int");
      schemaBuilder.append("{\"name\": \"").append(fields[i]).append("\", \"type\": \"").append(type).append("\"}");
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
            if (field.equals("customer_id") && tableName.equals("orders_join")) {
              record.put(field, random.nextInt(100)); // FK to customers
            } else if (field.equals("product_id") && tableName.equals("orders_join")) {
              record.put(field, random.nextInt(50));  // FK to products
            } else {
              record.put(field, i); // PK
            }
          } else if (field.equals("amount")) {
            record.put(field, 10.0 + random.nextDouble() * 990.0);
          } else if (field.contains("name") || field.contains("region") || field.contains("category")) {
            record.put(field, field.replace("_name", "").replace("_", "") + "_" + (i % 10));
          }
        }
        writer.write(record);
      }
    }
  }
  
  private void precomputeAllHLLSketches() throws Exception {
    System.out.println("Pre-computing HLL sketches for all test tables...");
    
    // HLL test sketches
    precomputeTableSketches("hll_test", new String[]{"order_id", "customer_id", "product_id"});
    
    // Join test sketches  
    precomputeTableSketches("orders_join", new String[]{"order_id", "customer_id", "product_id"});
    precomputeTableSketches("customers_join", new String[]{"customer_id"});
    precomputeTableSketches("products_join", new String[]{"product_id"});
    
    System.out.println("All HLL sketches pre-computed and cached.");
  }
  
  private void precomputeTableSketches(String tableName, String[] columns) throws Exception {
    for (String column : columns) {
      HyperLogLogSketch sketch = new HyperLogLogSketch(14);
      
      // Add synthetic distinct values based on expected cardinality
      int distinctCount;
      if (column.equals("order_id")) {
        distinctCount = tableName.equals("orders_join") ? 1000 : 1000;
      } else if (column.equals("customer_id")) {
        distinctCount = 100;
      } else if (column.equals("product_id")) {
        distinctCount = 50;
      } else {
        distinctCount = 100; // Default
      }
      
      for (int i = 0; i < distinctCount; i++) {
        sketch.add(column + "_" + i);
      }
      
      File sketchFile = new File(cacheDir, tableName + "_" + column + ".hll");
      StatisticsCache.saveHLLSketch(sketch, sketchFile);
    }
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