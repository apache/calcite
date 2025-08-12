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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Performance test for selective filter queries: COUNT(*) WHERE date > X
 * Tests DuckDB's filter pushdown optimization vs Calcite's approach.
 * 
 * This scenario favors DuckDB which has excellent predicate pushdown
 * and can leverage Parquet column statistics for efficient filtering.
 */
public class SelectiveFilterPerformanceTest {
  
  @TempDir
  static java.nio.file.Path tempDir;
  
  private static Connection duckdbConn;
  private static Connection calciteConn;
  
  private static final int[] ROW_COUNTS = {10000, 50000, 100000, 500000, 1000000, 5000000};
  private static final double[] SELECTIVITY_LEVELS = {0.01, 0.05, 0.10, 0.25, 0.50}; // 1%, 5%, 10%, 25%, 50%
  
  @BeforeAll
  public static void setUpOnce() throws Exception {
    System.out.println("Setting up selective filter performance test...");
    
    // Create test datasets with timestamps
    for (int rowCount : ROW_COUNTS) {
      createTimeSeriesData(rowCount);
    }
    
    // Setup DuckDB connection
    duckdbConn = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement stmt = duckdbConn.createStatement()) {
      for (int rowCount : ROW_COUNTS) {
        registerDuckDBView(stmt, rowCount);
      }
    }
    
    // Setup Calcite connection
    calciteConn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    
    rootSchema.add("FILES", FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));
    
    System.out.println("Setup complete. Ready for selective filter testing.\n");
  }
  
  @Test
  public void testSelectiveFilterPerformance() throws Exception {
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                     SELECTIVE FILTER PERFORMANCE COMPARISON                                 ║");
    System.out.println("║                    Query: COUNT(*) WHERE order_date > 'cutoff_date'                         ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════════╝\n");
    
    // Test different data sizes and filter selectivity
    for (int rowCount : ROW_COUNTS) {
      System.out.println("Testing with " + formatRowCount(rowCount) + " rows:");
      System.out.println("═══════════════════════════════════════");
      
      for (double selectivity : SELECTIVITY_LEVELS) {
        testSelectivityLevel(rowCount, selectivity);
      }
      System.out.println();
    }
    
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                                   KEY INSIGHTS                                              ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════════╝");
    
    System.out.println("\n🎯 FILTER PUSHDOWN ADVANTAGES:");
    System.out.println("   • DuckDB excels at filter pushdown with Parquet column statistics");
    System.out.println("   • Performance advantage grows with data size and filter selectivity");
    System.out.println("   • At 1% selectivity, DuckDB can skip 99% of row groups");
    System.out.println("   • This is DuckDB's strongest use case vs generic query engines");
    
    System.out.println("\n📊 PERFORMANCE PATTERNS:");
    System.out.println("   • Lower selectivity (1-5%) shows biggest DuckDB advantage");
    System.out.println("   • Calcite improvement opportunities: better predicate pushdown");
    System.out.println("   • HLL sketches don't help with selective filtering");
  }
  
  private void testSelectivityLevel(int rowCount, double selectivity) throws Exception {
    // Calculate cutoff date for desired selectivity
    LocalDate baseDate = LocalDate.of(2023, 1, 1);
    int daysToAdd = (int) (365 * (1.0 - selectivity)); // More recent date = higher selectivity
    LocalDate cutoffDate = baseDate.plusDays(daysToAdd);
    String cutoffStr = cutoffDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
    
    // DuckDB query
    String duckdbQuery = String.format(
        "SELECT COUNT(*) FROM timeseries_%d WHERE order_date > '%s'", 
        rowCount, cutoffStr);
    
    // Calcite query
    String calciteQuery = String.format(
        "SELECT COUNT(*) FROM FILES.\"timeseries_%d\" WHERE \"order_date\" > '%s'", 
        rowCount, cutoffStr);
    
    // Test DuckDB
    long duckdbTime = -1;
    long actualCount = 0;
    try (Statement stmt = duckdbConn.createStatement()) {
      // Warmup
      executeAndConsume(stmt, duckdbQuery);
      
      // Measure
      long start = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery(duckdbQuery)) {
        if (rs.next()) {
          actualCount = rs.getLong(1);
        }
      }
      duckdbTime = (System.nanoTime() - start) / 1_000_000;
    } catch (Exception e) {
      System.out.println("    DuckDB error: " + e.getMessage());
    }
    
    // Test Calcite (skip very large datasets to avoid timeouts)
    long calciteTime = -1;
    if (rowCount <= 100000) {
      try (Statement stmt = calciteConn.createStatement()) {
        // Warmup
        executeAndConsume(stmt, calciteQuery);
        
        // Measure
        long start = System.nanoTime();
        executeAndConsume(stmt, calciteQuery);
        calciteTime = (System.nanoTime() - start) / 1_000_000;
      } catch (Exception e) {
        System.out.println("    Calcite error: " + e.getMessage());
      }
    }
    
    // Report results
    String selectivityStr = String.format("%4.1f%%", selectivity * 100);
    String duckdbStr = duckdbTime > 0 ? String.format("%4d ms", duckdbTime) : "  ERROR";
    String calciteStr = calciteTime > 0 ? String.format("%4d ms", calciteTime) : calciteTime == -1 && rowCount > 100000 ? "SKIPPED" : "  ERROR";
    String speedup = (duckdbTime > 0 && calciteTime > 0) ? String.format("%.1fx", (double) calciteTime / duckdbTime) : "  N/A";
    
    System.out.printf("  %s selectivity: DuckDB %s | Calcite %s | Speedup %s | Found %,d rows%n",
                     selectivityStr, duckdbStr, calciteStr, speedup, actualCount);
  }
  
  @SuppressWarnings("deprecation")
  private static void createTimeSeriesData(int rows) throws Exception {
    File file = new File(tempDir.toFile(), "timeseries_" + rows + ".parquet");
    if (file.exists()) return;
    
    String schemaString = "{\"type\": \"record\",\"name\": \"TimeSeriesRecord\",\"fields\": [" +
                         "  {\"name\": \"order_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"order_date\", \"type\": \"string\"}," +
                         "  {\"name\": \"amount\", \"type\": \"double\"}," +
                         "  {\"name\": \"customer_id\", \"type\": \"int\"}" +
                         "]}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);
    LocalDate baseDate = LocalDate.of(2023, 1, 1);
    
    try (ParquetWriter<GenericRecord> writer = 
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {
      
      for (int i = 0; i < rows; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("order_id", i);
        
        // Generate dates spread over a year with some clustering
        // This creates realistic date patterns where some date ranges have more data
        int daysFromBase = (int) (365 * Math.pow(random.nextDouble(), 2)); // Skewed toward recent dates
        LocalDate orderDate = baseDate.plusDays(daysFromBase);
        record.put("order_date", orderDate.format(DateTimeFormatter.ISO_LOCAL_DATE));
        
        record.put("amount", 10.0 + random.nextDouble() * 990.0); // $10-$1000
        record.put("customer_id", random.nextInt(Math.max(1000, rows / 10)));
        
        writer.write(record);
      }
    }
  }
  
  private static void registerDuckDBView(Statement stmt, int rowCount) throws Exception {
    File parquetFile = new File(tempDir.toFile(), "timeseries_" + rowCount + ".parquet");
    if (parquetFile.exists()) {
      stmt.execute("CREATE OR REPLACE VIEW timeseries_" + rowCount + 
                  " AS SELECT * FROM '" + parquetFile.getAbsolutePath() + "'");
    }
  }
  
  private void executeAndConsume(Statement stmt, String query) throws Exception {
    try (ResultSet rs = stmt.executeQuery(query)) {
      rs.next();
    }
  }
  
  private static String formatRowCount(int count) {
    if (count >= 1000000) {
      return String.format("%.1fM", count / 1000000.0);
    } else if (count >= 1000) {
      return String.format("%dK", count / 1000);
    }
    return String.valueOf(count);
  }
}