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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Real comparison test to show actual HLL vs non-HLL performance.
 * Tests cold start vs warm start with pre-computed statistics.
 */
public class RealHLLComparisonTest {
  @TempDir
  java.nio.file.Path tempDir;
  
  private File cacheDir;
  private static final int[] ROW_COUNTS = {100000, 500000, 1000000, 5000000};
  
  @BeforeEach
  public void setUp() throws Exception {
    // Setup cache directory for HLL statistics
    cacheDir = tempDir.resolve("hll_cache").toFile();
    cacheDir.mkdirs();
    
    // Create test datasets
    for (int rowCount : ROW_COUNTS) {
      createHighCardinalityParquet(rowCount);
    }
  }
  
  @Test
  public void compareActualHLLPerformance() throws Exception {
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║              REAL HLL PERFORMANCE COMPARISON                        ║");
    System.out.println("║         Cold Start vs Warm Start vs DuckDB                          ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝");
    
    String[] queries = {
        "SELECT COUNT(DISTINCT unique_id) FROM high_card_%d",  // Every row unique
        "SELECT COUNT(DISTINCT user_id) FROM high_card_%d",    // ~20% cardinality
        "SELECT COUNT(DISTINCT group_id) FROM high_card_%d",   // ~1% cardinality
        "SELECT COUNT(DISTINCT unique_id), COUNT(DISTINCT user_id), COUNT(DISTINCT group_id) FROM high_card_%d"
    };
    
    String[] queryNames = {
        "COUNT(DISTINCT) - 100% unique",
        "COUNT(DISTINCT) - 20% unique",
        "COUNT(DISTINCT) - 1% unique", 
        "Triple COUNT(DISTINCT)"
    };
    
    for (int q = 0; q < queries.length; q++) {
      System.out.println("\n┌─────────────────────────────────────────────────────────────────────┐");
      System.out.println("│ " + String.format("%-67s", queryNames[q]) + " │");
      System.out.println("└─────────────────────────────────────────────────────────────────────┘");
      System.out.println("\n| Rows    | DuckDB | Calcite Cold | Calcite Warm | Calcite+HLL Cold | Calcite+HLL Warm | Winner |");
      System.out.println("|---------|--------|--------------|--------------|------------------|------------------|--------|");
      
      for (int rowCount : ROW_COUNTS) {
        if (rowCount > 1000000 && q < 3) continue; // Skip simple queries for very large datasets
        
        String query = String.format(Locale.ROOT, queries[q], rowCount);
        Map<String, Long> times = new LinkedHashMap<>();
        
        // Test DuckDB
        times.put("DuckDB", testDuckDB(query, rowCount));
        
        // Test Calcite Cold Start (clear cache first)
        clearCache();
        times.put("Calcite_Cold", testCalciteColdStart(query, rowCount));
        
        // Test Calcite Warm Start (cache should be populated)
        times.put("Calcite_Warm", testCalciteWarmStart(query, rowCount));
        
        // Test Calcite+HLL Cold Start
        clearCache();
        System.setProperty("calcite.file.statistics.hll.enabled", "true");
        System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
        times.put("HLL_Cold", testCalciteColdStart(query, rowCount));
        
        // Test Calcite+HLL Warm Start (HLL sketches should be cached)
        times.put("HLL_Warm", testCalciteWarmStart(query, rowCount));
        System.clearProperty("calcite.file.statistics.hll.enabled");
        
        // Display results
        displayResults(rowCount, times);
      }
    }
    
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                          KEY INSIGHTS                               ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝");
    System.out.println("\n1. COLD START: First query execution - must scan Parquet files");
    System.out.println("2. WARM START: Subsequent queries - should use cached statistics/results");
    System.out.println("3. If HLL WARM = HLL COLD: HLL sketches are NOT being reused");
    System.out.println("4. If HLL WARM << HLL COLD: HLL sketches ARE being reused effectively");
  }
  
  private long testDuckDB(String query, int rowCount) throws Exception {
    String url = "jdbc:duckdb:";
    try (Connection conn = DriverManager.getConnection(url);
         Statement stmt = conn.createStatement()) {
      
      // Register Parquet file as view
      File parquetFile = new File(tempDir.toFile(), "high_card_" + rowCount + ".parquet");
      stmt.execute("CREATE OR REPLACE VIEW high_card_" + rowCount + " AS SELECT * FROM '" + 
                  parquetFile.getAbsolutePath() + "'");
      
      // Warmup
      stmt.executeQuery(query).close();
      
      // Test
      long start = System.currentTimeMillis();
      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            rs.getObject(i);
          }
        }
      }
      return System.currentTimeMillis() - start;
    }
  }
  
  private long testCalciteColdStart(String query, int rowCount) throws Exception {
    // First execution - no cache
    return runCalciteQuery(query, false);
  }
  
  private long testCalciteWarmStart(String query, int rowCount) throws Exception {
    // Second execution - should use cache
    return runCalciteQuery(query, true);
  }
  
  private long runCalciteQuery(String query, boolean isWarm) throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      
      SchemaPlus fileSchema = rootSchema.add("FILES", 
          FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));
      
      // Update query to use FILES schema and quote identifiers
      String calciteQuery = query.replaceAll("high_card_(\\d+)", "FILES.\"high_card_$1\"");
      calciteQuery = calciteQuery.replaceAll("\\b(unique_id|user_id|group_id)\\b", "\"$1\"");
      
      if (!isWarm) {
        // Cold start - just run once
        long start = System.currentTimeMillis();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(calciteQuery)) {
          while (rs.next()) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
              rs.getObject(i);
            }
          }
        }
        return System.currentTimeMillis() - start;
      } else {
        // Warm start - run multiple times and average
        long total = 0;
        for (int i = 0; i < 3; i++) {
          long start = System.currentTimeMillis();
          try (Statement stmt = connection.createStatement();
               ResultSet rs = stmt.executeQuery(calciteQuery)) {
            while (rs.next()) {
              for (int i2 = 1; i2 <= rs.getMetaData().getColumnCount(); i2++) {
                rs.getObject(i2);
              }
            }
          }
          total += System.currentTimeMillis() - start;
        }
        return total / 3;
      }
    }
  }
  
  private void clearCache() throws Exception {
    if (cacheDir.exists()) {
      Files.walk(cacheDir.toPath())
          .filter(Files::isRegularFile)
          .forEach(p -> {
            try {
              Files.delete(p);
            } catch (Exception e) {
              // Ignore
            }
          });
    }
    System.gc();
    Thread.sleep(50);
  }
  
  private void displayResults(int rowCount, Map<String, Long> times) {
    String rows = rowCount >= 1000000 ? 
        String.format("%.1fM", rowCount / 1000000.0) : 
        String.format("%dK", rowCount / 1000);
    
    System.out.printf("| %7s | %6d | %12d | %12d | %16d | %16d | ", 
        rows,
        times.get("DuckDB"),
        times.get("Calcite_Cold"),
        times.get("Calcite_Warm"),
        times.get("HLL_Cold"),
        times.get("HLL_Warm"));
    
    // Determine winner and insight
    long min = times.values().stream().min(Long::compare).get();
    String winner = times.entrySet().stream()
        .filter(e -> e.getValue().equals(min))
        .findFirst().get().getKey();
    
    // Check if HLL is actually helping
    boolean hllHelps = times.get("HLL_Warm") < times.get("HLL_Cold") * 0.5;
    if (hllHelps) {
      System.out.println(winner + " (HLL effective!)");
    } else {
      System.out.println(winner);
    }
  }
  
  @SuppressWarnings("deprecation")
  private void createHighCardinalityParquet(int rows) throws Exception {
    File file = new File(tempDir.toFile(), "high_card_" + rows + ".parquet");
    if (file.exists()) return;
    
    String schemaString = "{"
        + "\"type\": \"record\","
        + "\"name\": \"HighCardRecord\","
        + "\"fields\": ["
        + "  {\"name\": \"unique_id\", \"type\": \"string\"},"  // 100% unique
        + "  {\"name\": \"user_id\", \"type\": \"int\"},"      // ~20% of rows unique
        + "  {\"name\": \"group_id\", \"type\": \"int\"},"     // ~1% of rows unique
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
        record.put("unique_id", "ID_" + i);  // Every row unique
        record.put("user_id", random.nextInt(rows / 5));  // ~20% cardinality
        record.put("group_id", random.nextInt(rows / 100));  // ~1% cardinality
        record.put("value", random.nextDouble() * 1000);
        writer.write(record);
      }
    }
    
    System.out.println("Created: " + file.getName() + " (" + file.length() + " bytes)");
  }
}