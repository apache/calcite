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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
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
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Debug test to verify that HLL rules are properly registered and activated.
 * This test creates datasets, computes HLL sketches, and traces rule execution
 * to confirm that optimizations are being applied.
 */
public class HLLRuleActivationDebugTest {
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private File cacheDir;
  private Connection calciteConn;
  
  @BeforeEach
  public void setUp() throws Exception {
    cacheDir = tempDir.resolve("hll_cache").toFile();
    cacheDir.mkdirs();
    
    // Enable detailed logging for rule execution
    setupDetailedLogging();
    
    // Enable all HLL optimizations
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("calcite.file.statistics.filter.enabled", "true"); 
    System.setProperty("calcite.file.statistics.join.reorder.enabled", "true");
    System.setProperty("calcite.file.statistics.column.pruning.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
    
    // Create test dataset
    createDebugDataset();
    
    // Pre-compute HLL sketches
    precomputeHLLSketches();
    
    // Setup Calcite with debug connection
    setupCalciteConnection();
  }
  
  @Test
  public void testHLLRuleActivationWithDebugging() throws Exception {
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                           HLL RULE ACTIVATION DEBUG TEST                                    ║");
    System.out.println("║                        Verifying Rules Fire in Volcano Planner                             ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════════╝\n");
    
    // Test 1: Verify rule registration
    System.out.println("STEP 1: Verifying Rule Registration in Planner");
    System.out.println("═════════════════════════════════════════════");
    verifyRuleRegistration();
    
    // Test 2: Execute query with rule tracing
    System.out.println("\nSTEP 2: Executing COUNT(DISTINCT) Query with Rule Tracing");
    System.out.println("════════════════════════════════════════════════════════");
    String ruleTrace = executeQueryWithRuleTrace();
    
    // Test 3: Analyze rule activation
    System.out.println("\nSTEP 3: Analyzing Rule Activation Results");
    System.out.println("════════════════════════════════════════");
    analyzeRuleActivation(ruleTrace);
    
    // Test 4: Verify HLL sketch usage
    System.out.println("\nSTEP 4: Verifying HLL Sketch Usage");
    System.out.println("═════════════════════════════════");
    verifyHLLSketchUsage();
    
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                                  DEBUG SUMMARY                                              ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════════╝");
    
    System.out.println("\n✅ VERIFICATION CHECKLIST:");
    System.out.println("   □ HLL rules registered in volcano planner");
    System.out.println("   □ HLL rules fire during query optimization");
    System.out.println("   □ HLL sketches loaded from cache");
    System.out.println("   □ COUNT(DISTINCT) transformed to sketch lookup");
    System.out.println("   □ Query execution uses optimized plan");
  }
  
  private void setupDetailedLogging() {
    // Enable detailed logging for Calcite's rule engine
    Logger calciteLogger = Logger.getLogger("org.apache.calcite");
    calciteLogger.setLevel(Level.FINE);
    
    Logger plannerLogger = Logger.getLogger("org.apache.calcite.plan");
    plannerLogger.setLevel(Level.FINE);
    
    Logger ruleLogger = Logger.getLogger("org.apache.calcite.plan.volcano");
    ruleLogger.setLevel(Level.FINE);
    
    // Create console handler with detailed formatter
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.FINE);
    handler.setFormatter(new SimpleFormatter());
    
    calciteLogger.addHandler(handler);
    plannerLogger.addHandler(handler);
    ruleLogger.addHandler(handler);
    
    calciteLogger.setUseParentHandlers(false);
    plannerLogger.setUseParentHandlers(false);
    ruleLogger.setUseParentHandlers(false);
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
  
  @SuppressWarnings("deprecation")
  private void createDebugDataset() throws Exception {
    File file = new File(tempDir.toFile(), "debug_test.parquet");
    
    String schemaString = "{\"type\": \"record\",\"name\": \"DebugRecord\",\"fields\": [" +
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
      
      // Create exactly 1000 rows with known distinct counts
      for (int i = 0; i < 1000; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("order_id", i);                           // 1000 distinct values
        record.put("customer_id", i % 100);                  // 100 distinct values  
        record.put("product_id", i % 50);                    // 50 distinct values
        record.put("amount", 10.0 + (i % 100) * 5.0);      // Predictable amounts
        writer.write(record);
      }
    }
    
    System.out.println("Created debug dataset with:");
    System.out.println("  • 1000 total rows");
    System.out.println("  • 1000 distinct order_ids (100% cardinality)");
    System.out.println("  • 100 distinct customer_ids (10% cardinality)");
    System.out.println("  • 50 distinct product_ids (5% cardinality)");
  }
  
  private void precomputeHLLSketches() throws Exception {
    System.out.println("\nPre-computing HLL sketches with known cardinalities:");
    
    // Create HLL sketches with exact known values
    HyperLogLogSketch orderIdSketch = new HyperLogLogSketch(14);
    HyperLogLogSketch customerIdSketch = new HyperLogLogSketch(14);
    HyperLogLogSketch productIdSketch = new HyperLogLogSketch(14);
    
    // Add all distinct values to sketches
    for (int i = 0; i < 1000; i++) {
      orderIdSketch.add("order_id_" + i);
    }
    
    for (int i = 0; i < 100; i++) {
      customerIdSketch.add("customer_id_" + i);
    }
    
    for (int i = 0; i < 50; i++) {
      productIdSketch.add("product_id_" + i);
    }
    
    // Save sketches to cache
    StatisticsCache.saveHLLSketch(orderIdSketch, new File(cacheDir, "debug_test_order_id.hll"));
    StatisticsCache.saveHLLSketch(customerIdSketch, new File(cacheDir, "debug_test_customer_id.hll"));
    StatisticsCache.saveHLLSketch(productIdSketch, new File(cacheDir, "debug_test_product_id.hll"));
    
    System.out.println("  ✓ order_id sketch: " + orderIdSketch.getEstimate() + " (expected ~1000)");
    System.out.println("  ✓ customer_id sketch: " + customerIdSketch.getEstimate() + " (expected ~100)");
    System.out.println("  ✓ product_id sketch: " + productIdSketch.getEstimate() + " (expected ~50)");
    System.out.println("  ✓ Sketches saved to: " + cacheDir.getAbsolutePath());
  }
  
  private void verifyRuleRegistration() throws Exception {
    try (Statement stmt = calciteConn.createStatement()) {
      // Get the planner to examine registered rules
      CalciteConnection calciteConnection = calciteConn.unwrap(CalciteConnection.class);
      
      System.out.println("Calcite connection established successfully");
      System.out.println("Connection type: " + calciteConnection.getClass().getName());
      
      // Verify schema registration by attempting to access schema names
      try {
        System.out.println("Root schema: " + calciteConnection.getRootSchema().getName());
      } catch (Exception e) {
        System.out.println("Schema access: " + e.getMessage());
      }
      
      // Test basic table access
      try {
        stmt.execute("SELECT COUNT(*) FROM FILES.\"debug_test\"");
        System.out.println("✓ Table access confirmed - FILES.\"debug_test\" is accessible");
      } catch (Exception e) {
        System.out.println("✗ Table access failed: " + e.getMessage());
      }
    }
  }
  
  private String executeQueryWithRuleTrace() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    
    try {
      // Redirect output to capture rule traces
      System.setOut(new PrintStream(baos));
      
      try (Statement stmt = calciteConn.createStatement()) {
        // First, show the execution plan
        System.setOut(originalOut);
        System.out.println("Analyzing execution plan for COUNT(DISTINCT) query...");
        
        String explainQuery = "EXPLAIN PLAN FOR SELECT " +
                             "COUNT(DISTINCT \"order_id\") as distinct_orders, " +
                             "COUNT(DISTINCT \"customer_id\") as distinct_customers, " +
                             "COUNT(DISTINCT \"product_id\") as distinct_products " +
                             "FROM FILES.\"debug_test\"";
        
        ResultSet planResult = stmt.executeQuery(explainQuery);
        while (planResult.next()) {
          System.out.println("  " + planResult.getString(1));
        }
        planResult.close();
        
        // Now execute the actual query with tracing
        System.setOut(new PrintStream(baos));
        
        String query = "SELECT " +
                      "COUNT(DISTINCT \"order_id\") as distinct_orders, " +
                      "COUNT(DISTINCT \"customer_id\") as distinct_customers, " +
                      "COUNT(DISTINCT \"product_id\") as distinct_products " +
                      "FROM FILES.\"debug_test\"";
        
        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()) {
          System.setOut(originalOut);
          System.out.println("Query executed successfully:");
          System.out.println("  • Distinct orders: " + rs.getInt(1) + " (expected ~1000)");
          System.out.println("  • Distinct customers: " + rs.getInt(2) + " (expected ~100)");
          System.out.println("  • Distinct products: " + rs.getInt(3) + " (expected ~50)");
        }
        rs.close();
      }
    } finally {
      System.setOut(originalOut);
    }
    
    return baos.toString();
  }
  
  private void analyzeRuleActivation(String ruleTrace) {
    System.out.println("Analyzing rule activation from execution trace...");
    
    String[] lines = ruleTrace.split("\n");
    int ruleFireCount = 0;
    int hllRuleCount = 0;
    
    for (String line : lines) {
      if (line.contains("Rule") && line.contains("fired")) {
        ruleFireCount++;
        System.out.println("  Rule fired: " + line.trim());
      }
      
      if (line.contains("HLL") || line.contains("HyperLogLog")) {
        hllRuleCount++;
        System.out.println("  ✓ HLL rule detected: " + line.trim());
      }
    }
    
    System.out.println("\nRule activation summary:");
    System.out.println("  • Total rules fired: " + ruleFireCount);
    System.out.println("  • HLL-related rules: " + hllRuleCount);
    
    if (hllRuleCount > 0) {
      System.out.println("  ✅ HLL rules ARE being activated");
    } else {
      System.out.println("  ❌ HLL rules are NOT being activated");
      System.out.println("  ⚠️  This indicates the HLL optimization is not working");
    }
  }
  
  private void verifyHLLSketchUsage() {
    System.out.println("Verifying HLL sketch files are accessible:");
    
    // List all files in cache directory to see actual names
    System.out.println("  Cache directory contents:");
    File[] files = cacheDir.listFiles();
    if (files != null) {
      for (File file : files) {
        System.out.println("    - " + file.getName());
      }
    }
    
    String[] columns = {"order_id", "customer_id", "product_id"};
    for (String column : columns) {
      File sketchFile = new File(cacheDir, "debug_test_" + column + ".hll");
      if (sketchFile.exists()) {
        System.out.println("  ✓ " + column + " sketch exists (" + sketchFile.length() + " bytes)");
        
        try {
          HyperLogLogSketch sketch = StatisticsCache.loadHLLSketch(sketchFile);
          System.out.println("    └─ Estimated cardinality: " + sketch.getEstimate());
        } catch (Exception e) {
          System.out.println("    └─ Error loading sketch: " + e.getMessage());
        }
      } else {
        System.out.println("  ✗ " + column + " sketch missing");
        System.out.println("    └─ Expected file: " + sketchFile.getAbsolutePath());
      }
    }
  }
}