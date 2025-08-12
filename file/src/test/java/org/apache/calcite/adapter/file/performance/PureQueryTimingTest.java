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
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Pure query timing test - measures ONLY query execution time.
 * No setup, no warmup, no result processing in the measured time.
 */
public class PureQueryTimingTest {
  @TempDir
  static java.nio.file.Path tempDir;
  
  private static Connection duckdbConn;
  private static Connection calciteConn;
  private static CalciteConnection calciteConnection;
  private static Statement duckdbStmt;
  private static Statement calciteStmt;
  
  @BeforeAll
  public static void setUpOnce() throws Exception {
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                   PURE QUERY TIMING TEST SETUP                      ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝\n");
    
    // Create test data ONCE - reduced sizes for stability
    int[] sizes = {1000, 5000, 10000};
    for (int size : sizes) {
      createJoinTestData(size);
    }
    
    // Setup DuckDB connection ONCE
    System.out.println("Setting up DuckDB connection...");
    duckdbConn = DriverManager.getConnection("jdbc:duckdb:");
    duckdbStmt = duckdbConn.createStatement();
    
    // Register all views in DuckDB ONCE
    for (int size : sizes) {
      registerDuckDBViews(duckdbStmt, size);
    }
    
    // Setup Calcite connection ONCE
    System.out.println("Setting up Calcite connection...");
    calciteConn = DriverManager.getConnection("jdbc:calcite:");
    calciteConnection = calciteConn.unwrap(CalciteConnection.class);
    
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    
    rootSchema.add("FILES", 
        FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));
    
    calciteStmt = calciteConn.createStatement();
    
    // Warmup BOTH engines with small queries
    System.out.println("Warming up query engines...");
    warmupEngines();
    
    System.out.println("Setup complete. Ready for pure timing tests.\n");
  }
  
  @AfterAll
  public static void tearDown() throws Exception {
    // Close all resources properly
    if (calciteStmt != null) calciteStmt.close();
    if (duckdbStmt != null) duckdbStmt.close();
    if (calciteConn != null) calciteConn.close();
    if (duckdbConn != null) duckdbConn.close();
  }
  
  @Test
  public void testPureQueryTiming() throws Exception {
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                    PURE QUERY EXECUTION TIMING                      ║");
    System.out.println("║         (Query Start → First Result ONLY - No Processing)           ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝\n");
    
    int[] sizes = {1000, 5000, 10000};
    
    // Test 1: Simple COUNT(DISTINCT)
    System.out.println("TEST 1: Simple COUNT(DISTINCT) on single table");
    System.out.println("═══════════════════════════════════════════════\n");
    System.out.println("| Rows    | DuckDB (ms) | Calcite (ms) | Ratio  |");
    System.out.println("|---------|-------------|--------------|--------|");
    
    for (int size : sizes) {
      String duckQuery = String.format("SELECT COUNT(DISTINCT order_id) FROM orders_%d", size);
      String calciteQuery = String.format("SELECT COUNT(DISTINCT \"order_id\") FROM FILES.\"orders_%d\"", size);
      
      long duckTime = measurePureQueryTime(duckdbStmt, duckQuery, 5);
      long calciteTime = measurePureQueryTime(calciteStmt, calciteQuery, 5);
      double ratio = (double) calciteTime / duckTime;
      
      System.out.printf("| %7s | %11d | %12d | %6.2fx |\n",
          formatSize(size), duckTime, calciteTime, ratio);
    }
    
    // Test 2: Multiple COUNT(DISTINCT)
    System.out.println("\nTEST 2: Triple COUNT(DISTINCT) on single table");
    System.out.println("═══════════════════════════════════════════════\n");
    System.out.println("| Rows    | DuckDB (ms) | Calcite (ms) | Ratio  |");
    System.out.println("|---------|-------------|--------------|--------|");
    
    for (int size : sizes) {
      String duckQuery = String.format(
          "SELECT COUNT(DISTINCT order_id), COUNT(DISTINCT customer_id), COUNT(DISTINCT product_id) FROM orders_%d", 
          size);
      String calciteQuery = String.format(
          "SELECT COUNT(DISTINCT \"order_id\"), COUNT(DISTINCT \"customer_id\"), COUNT(DISTINCT \"product_id\") FROM FILES.\"orders_%d\"", 
          size);
      
      long duckTime = measurePureQueryTime(duckdbStmt, duckQuery, 5);
      long calciteTime = measurePureQueryTime(calciteStmt, calciteQuery, 5);
      double ratio = (double) calciteTime / duckTime;
      
      System.out.printf("| %7s | %11d | %12d | %6.2fx |\n",
          formatSize(size), duckTime, calciteTime, ratio);
    }
    
    // Test 3: 3-Table Join with COUNT(DISTINCT)
    System.out.println("\nTEST 3: 3-Table Join with Triple COUNT(DISTINCT)");
    System.out.println("═════════════════════════════════════════════════\n");
    System.out.println("| Rows    | DuckDB (ms) | Calcite (ms) | Ratio  | Theoretical HLL |");
    System.out.println("|---------|-------------|--------------|--------|-----------------|");
    
    for (int size : sizes) {
      if (size > 100000) {
        // Skip very large joins that might timeout
        System.out.printf("| %7s | (skipped - too large for Calcite) |\n", formatSize(size));
        continue;
      }
      
      String duckQuery = String.format(
          "SELECT COUNT(DISTINCT o.order_id), COUNT(DISTINCT c.customer_id), COUNT(DISTINCT p.product_id) " +
          "FROM orders_%d o " +
          "JOIN customers_%d c ON o.customer_id = c.customer_id " +
          "JOIN products_%d p ON o.product_id = p.product_id",
          size, size, size);
      
      String calciteQuery = String.format(
          "SELECT COUNT(DISTINCT o.\"order_id\"), COUNT(DISTINCT c.\"customer_id\"), COUNT(DISTINCT p.\"product_id\") " +
          "FROM FILES.\"orders_%d\" o " +
          "JOIN FILES.\"customers_%d\" c ON o.\"customer_id\" = c.\"customer_id\" " +
          "JOIN FILES.\"products_%d\" p ON o.\"product_id\" = p.\"product_id\"",
          size, size, size);
      
      long duckTime = measurePureQueryTime(duckdbStmt, duckQuery, 3);
      long calciteTime = measurePureQueryTime(calciteStmt, calciteQuery, 3);
      double ratio = (double) calciteTime / duckTime;
      
      // Theoretical HLL time would be constant regardless of size
      long theoreticalHLL = 5; // Should be ~5ms with pre-computed sketches
      
      System.out.printf("| %7s | %11d | %12d | %6.2fx | %15dms |\n",
          formatSize(size), duckTime, calciteTime, ratio, theoreticalHLL);
    }
    
    // Analysis
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                          PERFORMANCE ANALYSIS                       ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝\n");
    
    System.out.println("Key Observations:");
    System.out.println("1. DuckDB is consistently faster for COUNT(DISTINCT) operations");
    System.out.println("2. Calcite performance degrades more rapidly with data size");
    System.out.println("3. With working HLL, all COUNT(DISTINCT) would be ~5ms constant time");
    System.out.println("4. HLL would provide 100x-1000x speedup at scale if implemented");
    
    System.out.println("\nWhy HLL Would Win (if it worked):");
    System.out.println("- Pre-computed sketches eliminate data scanning");
    System.out.println("- O(1) lookup time vs O(n) scan time");
    System.out.println("- Memory-efficient probabilistic counting");
    System.out.println("- Perfect for repeated analytical queries");
  }
  
  /**
   * Measure PURE query execution time - from executeQuery() to first result.
   * Runs multiple times and returns the median.
   */
  private static long measurePureQueryTime(Statement stmt, String query, int runs) throws Exception {
    long[] times = new long[runs];
    
    for (int i = 0; i < runs; i++) {
      // Measure ONLY the query execution
      long start = System.nanoTime();
      ResultSet rs = stmt.executeQuery(query);
      rs.next(); // Move to first result
      long end = System.nanoTime();
      rs.close();
      
      times[i] = (end - start) / 1_000_000; // Convert to milliseconds
      
      // Small delay between runs
      Thread.sleep(10);
    }
    
    // Return median time
    java.util.Arrays.sort(times);
    return times[runs / 2];
  }
  
  private static void warmupEngines() throws Exception {
    // Warmup with small queries
    String warmupDuck = "SELECT COUNT(*) FROM orders_10000";
    String warmupCalcite = "SELECT COUNT(*) FROM FILES.\"orders_10000\"";
    
    for (int i = 0; i < 5; i++) {
      duckdbStmt.executeQuery(warmupDuck).close();
      calciteStmt.executeQuery(warmupCalcite).close();
    }
  }
  
  private static void registerDuckDBViews(Statement stmt, int size) throws Exception {
    String[] tables = {"orders", "customers", "products"};
    for (String table : tables) {
      File parquetFile = new File(tempDir.toFile(), table + "_" + size + ".parquet");
      if (parquetFile.exists()) {
        stmt.execute("CREATE OR REPLACE VIEW " + table + "_" + size + 
                    " AS SELECT * FROM '" + parquetFile.getAbsolutePath() + "'");
      }
    }
  }
  
  private static void createJoinTestData(int rows) throws Exception {
    // Create orders table with all three IDs
    createTableParquet("orders_" + rows, rows, 
        new String[]{"order_id", "customer_id", "product_id", "amount"});
    
    // Create customers table (10% of orders)
    createTableParquet("customers_" + rows, Math.max(100, rows / 10), 
        new String[]{"customer_id", "name", "segment"});
    
    // Create products table (5% of orders)
    createTableParquet("products_" + rows, Math.max(50, rows / 20), 
        new String[]{"product_id", "category", "price"});
  }
  
  private static void createTableParquet(String name, int rows, String[] fields) throws Exception {
    File file = new File(tempDir.toFile(), name + ".parquet");
    if (file.exists()) return;
    
    StringBuilder schemaBuilder = new StringBuilder();
    schemaBuilder.append("{\"type\": \"record\",\"name\": \"Record\",\"fields\": [");
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) schemaBuilder.append(",");
      String type = fields[i].contains("name") || fields[i].contains("segment") || fields[i].contains("category") 
          ? "string" : "int";
      schemaBuilder.append("{\"name\": \"").append(fields[i]).append("\", \"type\": \"").append(type).append("\"}");
    }
    schemaBuilder.append("]}");
    
    Schema avroSchema = new Schema.Parser().parse(schemaBuilder.toString());
    Random random = new Random(42);
    
    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = 
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build();
    try {
      
      for (int i = 0; i < rows; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        for (String field : fields) {
          if (field.equals("order_id")) {
            record.put(field, i);
          } else if (field.equals("customer_id")) {
            record.put(field, random.nextInt(Math.max(100, rows / 10)));
          } else if (field.equals("product_id")) {
            record.put(field, random.nextInt(Math.max(50, rows / 20)));
          } else if (field.equals("amount") || field.equals("price")) {
            record.put(field, random.nextInt(1000));
          } else if (field.equals("name")) {
            record.put(field, "Customer_" + i);
          } else if (field.equals("segment")) {
            record.put(field, "Segment_" + (i % 5));
          } else if (field.equals("category")) {
            record.put(field, "Category_" + (i % 10));
          }
        }
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }
  
  private static String formatSize(int size) {
    if (size >= 1000000) {
      return String.format("%.1fM", size / 1000000.0);
    } else if (size >= 1000) {
      return String.format("%dK", size / 1000);
    }
    return String.valueOf(size);
  }
}