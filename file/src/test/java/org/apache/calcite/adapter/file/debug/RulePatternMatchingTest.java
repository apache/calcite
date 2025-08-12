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
 * Test specifically designed to match the exact query patterns that each rule expects.
 * This will determine if the rules are not firing due to pattern mismatches.
 */
public class RulePatternMatchingTest {
  
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
    
    // Create test datasets
    createTestData();
    precomputeHLLSketches();
    setupCalciteConnection();
  }
  
  @Test
  public void testExactRulePatternMatching() throws Exception {
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                          RULE PATTERN MATCHING TEST                                         ║");
    System.out.println("║                    Testing Exact Patterns Each Rule Expects                                ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════════╝\n");
    
    testFilterPushdownPattern();
    testJoinReorderPattern();
    testColumnPruningPattern();
    testActualQueryPlans();
  }
  
  private void testFilterPushdownPattern() throws Exception {
    System.out.println("Testing Filter Pushdown Pattern: Filter -> TableScan");
    System.out.println("═════════════════════════════════════════════════════");
    
    // Rule expects: Filter(TableScan)
    String query = "SELECT * FROM FILES.\"pattern_test\" WHERE \"amount\" > 500";
    
    System.out.println("  Expected pattern: Filter -> TableScan");
    System.out.println("  Query: " + query);
    
    String trace = executeWithTrace(query);
    String plan = getExecutionPlan(query);
    
    System.out.println("  Actual plan:");
    for (String line : plan.split("\n")) {
      System.out.println("    " + line);
    }
    
    // Check for filter pushdown activation
    boolean filterRuleActivated = trace.contains("FileFilterPushdownRule") || 
                                 trace.contains("filter") && trace.contains("selectivity");
    
    System.out.println("  Rule activation: " + (filterRuleActivated ? "✓ ACTIVATED" : "❌ NOT ACTIVATED"));
    
    if (!filterRuleActivated) {
      System.out.println("  Analysis: Filter rule requires Filter->TableScan pattern");
      System.out.println("           Plan shows: " + getTopLevelOperator(plan));
    }
  }
  
  private void testJoinReorderPattern() throws Exception {
    System.out.println("\nTesting Join Reorder Pattern: Join(RelNode, RelNode)");
    System.out.println("═══════════════════════════════════════════════════");
    
    // Rule expects: Join with two RelNode inputs
    String query = "SELECT * FROM FILES.\"table1\" t1 " +
                  "JOIN FILES.\"table2\" t2 ON t1.\"id\" = t2.\"ref_id\"";
    
    System.out.println("  Expected pattern: Join -> (RelNode, RelNode)");
    System.out.println("  Query: " + query);
    
    String trace = executeWithTrace(query);
    String plan = getExecutionPlan(query);
    
    System.out.println("  Actual plan:");
    for (String line : plan.split("\n")) {
      System.out.println("    " + line);
    }
    
    boolean joinRuleActivated = trace.contains("FileJoinReorderRule") || 
                               trace.contains("join") && trace.contains("reorder");
    
    System.out.println("  Rule activation: " + (joinRuleActivated ? "✓ ACTIVATED" : "❌ NOT ACTIVATED"));
    
    if (!joinRuleActivated) {
      System.out.println("  Analysis: Join rule requires explicit Join node");
      System.out.println("           Plan shows: " + getTopLevelOperator(plan));
    }
  }
  
  private void testColumnPruningPattern() throws Exception {
    System.out.println("\nTesting Column Pruning Pattern: Project -> TableScan");
    System.out.println("══════════════════════════════════════════════════");
    
    // Rule expects: Project(TableScan) - subset of columns selected
    String query = "SELECT \"id\", \"amount\" FROM FILES.\"pattern_test\"";
    
    System.out.println("  Expected pattern: Project -> TableScan");
    System.out.println("  Query: " + query);
    
    String trace = executeWithTrace(query);
    String plan = getExecutionPlan(query);
    
    System.out.println("  Actual plan:");
    for (String line : plan.split("\n")) {
      System.out.println("    " + line);
    }
    
    boolean pruningRuleActivated = trace.contains("FileColumnPruningRule") || 
                                  trace.contains("column") && trace.contains("prun");
    
    System.out.println("  Rule activation: " + (pruningRuleActivated ? "✓ ACTIVATED" : "❌ NOT ACTIVATED"));
    
    if (!pruningRuleActivated) {
      System.out.println("  Analysis: Column pruning rule requires Project->TableScan pattern");
      System.out.println("           Plan shows: " + getTopLevelOperator(plan));
    }
  }
  
  private void testActualQueryPlans() throws Exception {
    System.out.println("\nActual Query Plan Analysis:");
    System.out.println("═══════════════════════════");
    
    String[] queries = {
        "SELECT COUNT(*) FROM FILES.\"pattern_test\"",
        "SELECT * FROM FILES.\"pattern_test\" WHERE \"amount\" > 500", 
        "SELECT \"id\", \"amount\" FROM FILES.\"pattern_test\"",
        "SELECT COUNT(DISTINCT \"id\") FROM FILES.\"pattern_test\""
    };
    
    for (String query : queries) {
      System.out.println("\n  Query: " + query);
      String plan = getExecutionPlan(query);
      System.out.println("  Plan structure: " + analyzePlanStructure(plan));
    }
  }
  
  private String executeWithTrace(String query) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    
    try {
      System.setOut(new PrintStream(baos));
      
      try (Statement stmt = calciteConn.createStatement()) {
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
          // Consume results
        }
        rs.close();
      }
    } catch (Exception e) {
      System.setOut(originalOut);
      System.out.println("    Query failed: " + e.getMessage());
    } finally {
      System.setOut(originalOut);
    }
    
    return baos.toString();
  }
  
  private String getExecutionPlan(String query) {
    try (Statement stmt = calciteConn.createStatement()) {
      ResultSet rs = stmt.executeQuery("EXPLAIN PLAN FOR " + query);
      StringBuilder plan = new StringBuilder();
      while (rs.next()) {
        plan.append(rs.getString(1)).append("\n");
      }
      rs.close();
      return plan.toString().trim();
    } catch (Exception e) {
      return "Plan generation failed: " + e.getMessage();
    }
  }
  
  private String getTopLevelOperator(String plan) {
    String[] lines = plan.split("\n");
    if (lines.length > 0) {
      String firstLine = lines[0].trim();
      if (firstLine.contains("(")) {
        return firstLine.substring(0, firstLine.indexOf("("));
      }
      return firstLine;
    }
    return "Unknown";
  }
  
  private String analyzePlanStructure(String plan) {
    String[] lines = plan.split("\n");
    StringBuilder structure = new StringBuilder();
    
    for (String line : lines) {
      String trimmed = line.trim();
      if (trimmed.contains("(")) {
        String operator = trimmed.substring(0, trimmed.indexOf("("));
        structure.append(operator).append(" -> ");
      }
    }
    
    String result = structure.toString();
    return result.length() > 4 ? result.substring(0, result.length() - 4) : result;
  }
  
  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    // Create main test table
    createTable("pattern_test", 1000, new String[]{"id", "amount", "category", "date_val"});
    
    // Create join tables
    createTable("table1", 500, new String[]{"id", "value", "type"});
    createTable("table2", 200, new String[]{"ref_id", "name", "status"});
  }
  
  @SuppressWarnings("deprecation")
  private void createTable(String tableName, int rows, String[] fields) throws Exception {
    File file = new File(tempDir.toFile(), tableName + ".parquet");
    
    StringBuilder schemaBuilder = new StringBuilder();
    schemaBuilder.append("{\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [");
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) schemaBuilder.append(",");
      String type = fields[i].contains("name") || fields[i].contains("category") || 
                   fields[i].contains("type") || fields[i].contains("status") ? 
                   "string" : (fields[i].contains("amount") || fields[i].contains("value") ? "double" : "int");
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
          if (field.equals("id") || field.equals("ref_id")) {
            record.put(field, i);
          } else if (field.equals("amount") || field.equals("value")) {
            record.put(field, 100.0 + random.nextDouble() * 900.0);
          } else if (field.contains("name") || field.contains("category") || 
                    field.contains("type") || field.contains("status")) {
            record.put(field, field.substring(0, 1).toUpperCase() + "_" + (i % 10));
          } else {
            record.put(field, random.nextInt(1000));
          }
        }
        writer.write(record);
      }
    }
  }
  
  private void precomputeHLLSketches() throws Exception {
    // Create sketches for main table
    String[] columns = {"id", "amount", "category"};
    for (String column : columns) {
      HyperLogLogSketch sketch = new HyperLogLogSketch(14);
      
      int distinctCount = column.equals("id") ? 1000 : 
                         column.equals("category") ? 10 : 100;
      
      for (int i = 0; i < distinctCount; i++) {
        sketch.add(column + "_" + i);
      }
      
      File sketchFile = new File(cacheDir, "pattern_test_" + column + ".hll");
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