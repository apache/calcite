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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Aggressive test to verify that all rules actually fire by adding system output to the rules.
 */
public class RuleFiringVerificationTest {
  
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
    
    createTestData();
    precomputeHLLSketches();
    setupCalciteConnection();
  }
  
  @Test
  public void testRulesActuallyFire() throws Exception {
    System.out.println("\n╔════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                          RULE FIRING VERIFICATION TEST                                    ║");
    System.out.println("║                    Testing if rules ACTUALLY fire (not just match)                      ║");
    System.out.println("╚════════════════════════════════════════════════════════════════════════════════════════╝\n");
    
    // Test 1: HLL COUNT DISTINCT
    testHLLRule();
    
    // Test 2: Filter Pushdown with EnumerableCalc
    testFilterRule();
    
    // Test 3: Column Pruning with EnumerableCalc
    testColumnPruningRule();
    
    // Test 4: Join Reorder with EnumerableMergeJoin
    testJoinRule();
    
    System.out.println("\n🎯 CONCLUSION:");
    System.out.println("   - If you see rule output above, rules are firing");
    System.out.println("   - If no rule output, rules are not firing despite pattern matches");
  }
  
  private void testHLLRule() throws Exception {
    System.out.println("Testing HLL Rule Firing:");
    System.out.println("========================");
    
    String query = "SELECT COUNT(DISTINCT \"order_id\") FROM FILES.\"test_table\"";
    
    try (Statement stmt = calciteConn.createStatement()) {
      System.out.println("Executing: " + query);
      ResultSet rs = stmt.executeQuery(query);
      if (rs.next()) {
        System.out.println("Result: " + rs.getLong(1));
      }
      rs.close();
    }
    System.out.println();
  }
  
  private void testFilterRule() throws Exception {
    System.out.println("Testing Filter Rule Firing:");
    System.out.println("===========================");
    
    String query = "SELECT * FROM FILES.\"test_table\" WHERE \"amount\" > 500";
    
    try (Statement stmt = calciteConn.createStatement()) {
      System.out.println("Executing: " + query);
      ResultSet rs = stmt.executeQuery(query);
      int count = 0;
      while (rs.next() && count < 5) {
        count++;
      }
      System.out.println("Returned " + count + " rows");
      rs.close();
    }
    System.out.println();
  }
  
  private void testColumnPruningRule() throws Exception {
    System.out.println("Testing Column Pruning Rule Firing:");
    System.out.println("===================================");
    
    String query = "SELECT \"order_id\", \"amount\" FROM FILES.\"test_table\" LIMIT 10";
    
    try (Statement stmt = calciteConn.createStatement()) {
      System.out.println("Executing: " + query);
      ResultSet rs = stmt.executeQuery(query);
      int count = 0;
      while (rs.next() && count < 3) {
        System.out.println("  Row " + (count + 1) + ": order_id=" + rs.getInt(1) + ", amount=" + rs.getDouble(2));
        count++;
      }
      rs.close();
    }
    System.out.println();
  }
  
  private void testJoinRule() throws Exception {
    System.out.println("Testing Join Rule Firing:");
    System.out.println("=========================");
    
    String query = "SELECT o.\"order_id\", c.\"customer_name\" " +
                  "FROM FILES.\"orders\" o " +
                  "JOIN FILES.\"customers\" c ON o.\"customer_id\" = c.\"customer_id\" " +
                  "LIMIT 5";
    
    try (Statement stmt = calciteConn.createStatement()) {
      System.out.println("Executing: " + query);
      ResultSet rs = stmt.executeQuery(query);
      int count = 0;
      while (rs.next() && count < 3) {
        System.out.println("  Join result " + (count + 1) + ": order=" + rs.getInt(1) + ", customer=" + rs.getString(2));
        count++;
      }
      rs.close();
    }
    System.out.println();
  }
  
  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    // Create main test table
    createTable("test_table", 1000, new String[]{"order_id", "customer_id", "product_id", "amount"});
    
    // Create join tables
    createTable("orders", 500, new String[]{"order_id", "customer_id", "amount"});
    createTable("customers", 100, new String[]{"customer_id", "customer_name"});
  }
  
  @SuppressWarnings("deprecation")
  private void createTable(String tableName, int rows, String[] fields) throws Exception {
    File file = new File(tempDir.toFile(), tableName + ".parquet");
    
    StringBuilder schemaBuilder = new StringBuilder();
    schemaBuilder.append("{\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [");
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) schemaBuilder.append(",");
      String type = fields[i].contains("name") ? "string" : 
                   fields[i].equals("amount") ? "double" : "int";
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
          if (field.equals("order_id")) {
            record.put(field, i);
          } else if (field.equals("customer_id")) {
            record.put(field, i % 100);
          } else if (field.equals("product_id")) {
            record.put(field, i % 50);
          } else if (field.equals("amount")) {
            record.put(field, 100.0 + random.nextDouble() * 900.0);
          } else if (field.equals("customer_name")) {
            record.put(field, "Customer_" + i);
          }
        }
        writer.write(record);
      }
    }
  }
  
  private void precomputeHLLSketches() throws Exception {
    // Create sketches for test tables
    String[] tables = {"test_table", "orders", "customers"};
    String[] columns = {"order_id", "customer_id", "product_id"};
    
    for (String table : tables) {
      for (String column : columns) {
        HyperLogLogSketch sketch = new HyperLogLogSketch(14);
        
        int distinctCount = column.equals("order_id") ? 
                           (table.equals("orders") ? 500 : 1000) :
                           column.equals("customer_id") ? 100 : 50;
        
        for (int i = 0; i < distinctCount; i++) {
          sketch.add(column + "_" + i);
        }
        
        File sketchFile = new File(cacheDir, table + "_" + column + ".hll");
        StatisticsCache.saveHLLSketch(sketch, sketchFile);
      }
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