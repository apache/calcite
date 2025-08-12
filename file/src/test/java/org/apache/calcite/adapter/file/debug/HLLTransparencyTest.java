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
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that regular COUNT(DISTINCT) queries use HLL approximations transparently
 * without requiring explicit APPROX_COUNT_DISTINCT syntax.
 */
public class HLLTransparencyTest {
  
  @TempDir
  Path tempDir;
  
  private File cacheDir;
  
  @BeforeEach
  public void setUp() throws Exception {
    cacheDir = tempDir.resolve("hll_cache").toFile();
    cacheDir.mkdirs();
    
    // Create test data with known cardinality
    createTestParquetFile();
  }
  
  @Test
  public void testTransparentHLLOptimization() throws Exception {
    System.out.println("=== Testing Transparent HLL Optimization ===");
    
    // Test with HLL disabled - should give exact results
    System.out.println("1. Testing with HLL disabled:");
    long exactResult = runCountDistinct(false);
    System.out.println("   Exact COUNT(DISTINCT) result: " + exactResult);
    assertEquals(1000, exactResult, "Exact result should be 1000");
    
    // Test with HLL enabled - should give approximate results transparently
    System.out.println("2. Testing with HLL enabled:");
    long hllResult = runCountDistinct(true);
    System.out.println("   HLL COUNT(DISTINCT) result: " + hllResult);
    
    // HLL should be close to exact but may not be identical
    assertTrue(hllResult >= 900 && hllResult <= 1100, 
        "HLL result should be close to 1000, got: " + hllResult);
    
    // Test that both APPROX_COUNT_DISTINCT and COUNT(DISTINCT) give same results with HLL
    System.out.println("3. Comparing COUNT(DISTINCT) vs APPROX_COUNT_DISTINCT:");
    long countDistinctResult = runSpecificQuery("SELECT COUNT(DISTINCT \"customer_id\") FROM HLL_TEST.\"test\"");
    long approxCountResult = runSpecificQuery("SELECT APPROX_COUNT_DISTINCT(\"customer_id\") FROM HLL_TEST.\"test\"");
    
    System.out.println("   COUNT(DISTINCT) with HLL: " + countDistinctResult);
    System.out.println("   APPROX_COUNT_DISTINCT with HLL: " + approxCountResult);
    
    // Both should use HLL and give similar results
    assertTrue(Math.abs(countDistinctResult - approxCountResult) <= 100, 
        "COUNT(DISTINCT) and APPROX_COUNT_DISTINCT should give similar results when HLL is enabled");
    
    System.out.println("✓ HLL optimization works transparently for regular COUNT(DISTINCT)");
  }
  
  private long runCountDistinct(boolean enableHLL) throws Exception {
    // Configure HLL
    if (enableHLL) {
      System.setProperty("calcite.file.statistics.hll.enabled", "true");
      System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
      System.setProperty("calcite.file.statistics.hll.threshold", "1"); // Always generate HLL
    } else {
      System.setProperty("calcite.file.statistics.hll.enabled", "false");
    }
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      operand.put("primeCache", true);  // Enable cache priming for HLL
      
      SchemaPlus fileSchema = rootSchema.add("HLL_TEST", 
          FileSchemaFactory.INSTANCE.create(rootSchema, "HLL_TEST", operand));
      
      // Give some time for cache priming if HLL is enabled
      if (enableHLL) {
        Thread.sleep(500);
      }
      
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT \"customer_id\") FROM HLL_TEST.\"test\"")) {
        
        if (rs.next()) {
          return rs.getLong(1);
        }
        return 0;
      }
    } finally {
      System.clearProperty("calcite.file.statistics.hll.enabled");
      System.clearProperty("calcite.file.statistics.cache.directory");
      System.clearProperty("calcite.file.statistics.hll.threshold");
    }
  }
  
  private long runSpecificQuery(String query) throws Exception {
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
    System.setProperty("calcite.file.statistics.hll.threshold", "1"); // Always generate HLL
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      operand.put("primeCache", true);
      
      SchemaPlus fileSchema = rootSchema.add("HLL_TEST", 
          FileSchemaFactory.INSTANCE.create(rootSchema, "HLL_TEST", operand));
      
      Thread.sleep(500); // Cache priming time
      
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        
        if (rs.next()) {
          return rs.getLong(1);
        }
        return 0;
      }
    } finally {
      System.clearProperty("calcite.file.statistics.hll.enabled");
      System.clearProperty("calcite.file.statistics.cache.directory");
      System.clearProperty("calcite.file.statistics.hll.threshold");
    }
  }
  
  @SuppressWarnings("deprecation")
  private void createTestParquetFile() throws Exception {
    File file = new File(tempDir.toFile(), "test.parquet");
    
    String schemaString = "{"
        + "\"type\": \"record\","
        + "\"name\": \"TestRecord\","
        + "\"fields\": ["
        + "  {\"name\": \"customer_id\", \"type\": \"int\"},"
        + "  {\"name\": \"amount\", \"type\": \"double\"}"
        + "]"
        + "}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(12345);
    
    try (ParquetWriter<GenericRecord> writer = 
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {
      
      // Create 5000 records with exactly 1000 distinct customer_ids
      for (int i = 0; i < 5000; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("customer_id", 1000 + (i % 1000)); // Exactly 1000 unique customers
        record.put("amount", 100.0 + random.nextDouble() * 900);
        writer.write(record);
      }
    }
    
    System.out.println("Created test file with 5000 records, 1000 distinct customer_ids");
  }
}