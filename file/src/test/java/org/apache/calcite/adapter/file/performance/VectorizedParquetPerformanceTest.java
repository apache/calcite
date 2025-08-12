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

import org.apache.calcite.adapter.file.FileAdapterTests;
import org.apache.calcite.adapter.file.execution.parquet.VectorizedParquetReader;
import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.table.CsvScannableTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.avro.generic.GenericRecord;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test vectorized Parquet reading performance and correctness.
 */
public class VectorizedParquetPerformanceTest {

  @TempDir
  static File tempDir;
  static File parquetFile;
  static File csvFile;

  @BeforeAll
  static void setupTestData() throws Exception {
    // Create a CSV file with test data
    csvFile = new File(tempDir, "test_data.csv");
    StringBuilder csv = new StringBuilder();
    csv.append("id,name,value,category,timestamp\n");
    
    // Generate 10,000 rows of test data
    for (int i = 1; i <= 10000; i++) {
      csv.append(i).append(",");
      csv.append("Name_").append(i).append(",");
      csv.append(i * 1.5).append(",");
      csv.append("Category_").append(i % 10).append(",");
      csv.append("2024-01-").append(String.format("%02d", (i % 28) + 1)).append(" 12:00:00\n");
    }
    
    Files.write(csvFile.toPath(), csv.toString().getBytes());
    
    // Convert CSV to Parquet
    Source source = Sources.of(csvFile);
    Table csvTable = new CsvScannableTable(source, null);
    
    File cacheDir = new File(tempDir, ".parquet_cache");
    cacheDir.mkdirs();
    
    parquetFile = ParquetConversionUtil.convertToParquet(
        source, "test_data", csvTable, cacheDir, null, "test");
    
    assertTrue(parquetFile.exists(), "Parquet file should be created");
  }

  @Test
  void testVectorizedReaderCorrectness() throws Exception {
    // Read using vectorized reader
    VectorizedParquetReader vectorizedReader = new VectorizedParquetReader(parquetFile.getAbsolutePath());
    
    int rowCount = 0;
    double sumValues = 0;
    
    List<Object[]> batch;
    while ((batch = vectorizedReader.readBatch()) != null) {
      for (Object[] row : batch) {
        rowCount++;
        // Check row structure
        assertEquals(5, row.length, "Row should have 5 columns");
        
        // Verify data types
        assertTrue(row[0] instanceof Long, "ID should be Long");
        assertTrue(row[1] instanceof String, "Name should be String");
        assertTrue(row[2] instanceof Double, "Value should be Double");
        assertTrue(row[3] instanceof String, "Category should be String");
        
        // Sum values for verification
        sumValues += (Double) row[2];
      }
    }
    
    vectorizedReader.close();
    
    // Verify results
    assertEquals(10000, rowCount, "Should read all 10,000 rows");
    assertEquals(75007500.0, sumValues, 0.01, "Sum of values should match expected");
  }

  @Test
  void testVectorizedVsRecordByRecordPerformance() throws Exception {
    // Warm up both readers
    warmupReaders();
    
    // Test record-by-record reading
    long recordByRecordTime = timeRecordByRecordReading();
    
    // Test vectorized reading
    long vectorizedTime = timeVectorizedReading();
    
    // Print results
    System.out.println("Performance Comparison:");
    System.out.println("  Record-by-record: " + recordByRecordTime + " ms");
    System.out.println("  Vectorized:       " + vectorizedTime + " ms");
    System.out.println("  Speedup:          " + String.format("%.2fx", 
        (double) recordByRecordTime / vectorizedTime));
    
    // Vectorized should be faster (allow some margin for test variability)
    assertTrue(vectorizedTime <= recordByRecordTime * 1.2, 
        "Vectorized reading should not be significantly slower than record-by-record");
  }

  @Test
  void testBatchSizeImpact() throws Exception {
    int[] batchSizes = {64, 256, 1024, 4096};
    
    System.out.println("\nBatch Size Performance:");
    for (int batchSize : batchSizes) {
      long time = timeVectorizedReadingWithBatchSize(batchSize);
      System.out.println("  Batch size " + batchSize + ": " + time + " ms");
    }
  }

  @Test
  void testSqlQueryWithVectorizedReading() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      
      SchemaPlus rootSchema = calciteConn.getRootSchema();
      
      // Create schema with Parquet table
      Map<String, Table> tableMap = new HashMap<>();
      tableMap.put("test_data", new org.apache.calcite.adapter.file.table.ParquetTranslatableTable(parquetFile));
      
      rootSchema.add("test", new org.apache.calcite.schema.impl.AbstractSchema() {
        @Override
        protected Map<String, Table> getTableMap() {
          return tableMap;
        }
      });
      
      // Execute aggregation query
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT category, COUNT(*) as cnt, SUM(value) as total " +
               "FROM test.test_data " +
               "GROUP BY category " +
               "ORDER BY category")) {
        
        int categoryCount = 0;
        double totalSum = 0;
        
        while (rs.next()) {
          categoryCount++;
          totalSum += rs.getDouble("total");
        }
        
        assertEquals(10, categoryCount, "Should have 10 categories");
        assertEquals(75007500.0, totalSum, 0.01, "Total sum should match");
      }
    }
  }

  @Test
  void testFilteredVectorizedReading() throws Exception {
    VectorizedParquetReader reader = new VectorizedParquetReader(parquetFile.getAbsolutePath());
    
    int countCategory5 = 0;
    List<Object[]> batch;
    
    while ((batch = reader.readBatch()) != null) {
      for (Object[] row : batch) {
        String category = (String) row[3];
        if ("Category_5".equals(category)) {
          countCategory5++;
        }
      }
    }
    
    reader.close();
    
    assertEquals(1000, countCategory5, "Should have 1000 rows with Category_5");
  }

  @Test
  void testVectorizedReaderWithNulls() throws Exception {
    // Create CSV with null values
    File csvWithNulls = new File(tempDir, "test_nulls.csv");
    StringBuilder csv = new StringBuilder();
    csv.append("id,name,value\n");
    csv.append("1,Alice,100.0\n");
    csv.append("2,,200.0\n");  // null name
    csv.append("3,Charlie,\n"); // null value
    csv.append("4,David,400.0\n");
    
    Files.write(csvWithNulls.toPath(), csv.toString().getBytes());
    
    // Convert to Parquet
    Source source = Sources.of(csvWithNulls);
    Table csvTable = new CsvScannableTable(source, null);
    File cacheDir = new File(tempDir, ".parquet_cache");
    File parquetWithNulls = ParquetConversionUtil.convertToParquet(
        source, "test_nulls", csvTable, cacheDir, null, "test");
    
    // Read with vectorized reader
    VectorizedParquetReader reader = new VectorizedParquetReader(parquetWithNulls.getAbsolutePath());
    
    List<Object[]> batch = reader.readBatch();
    assertNotNull(batch);
    assertEquals(4, batch.size());
    
    // Check null handling
    assertNull(batch.get(1)[1], "Second row name should be null");
    assertNull(batch.get(2)[2], "Third row value should be null");
    
    reader.close();
  }

  private void warmupReaders() throws Exception {
    // Warm up vectorized reader
    for (int i = 0; i < 3; i++) {
      try (VectorizedParquetReader reader = new VectorizedParquetReader(parquetFile.getAbsolutePath())) {
        while (reader.readBatch() != null) {
          // Just read through
        }
      }
    }
    
    // Warm up record-by-record reader
    for (int i = 0; i < 3; i++) {
      Path hadoopPath = new Path(parquetFile.getAbsolutePath());
      Configuration conf = new Configuration();
      
      @SuppressWarnings("deprecation")
      ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(hadoopPath)
          .withConf(conf)
          .build();
      
      while (reader.read() != null) {
        // Just read through
      }
      reader.close();
    }
  }

  private long timeRecordByRecordReading() throws Exception {
    long startTime = System.currentTimeMillis();
    
    Path hadoopPath = new Path(parquetFile.getAbsolutePath());
    Configuration conf = new Configuration();
    
    @SuppressWarnings("deprecation")
    ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(hadoopPath)
        .withConf(conf)
        .build();
    
    int count = 0;
    while (reader.read() != null) {
      count++;
    }
    reader.close();
    
    long endTime = System.currentTimeMillis();
    assertEquals(10000, count, "Should read all rows");
    
    return endTime - startTime;
  }

  private long timeVectorizedReading() throws Exception {
    long startTime = System.currentTimeMillis();
    
    VectorizedParquetReader reader = new VectorizedParquetReader(parquetFile.getAbsolutePath());
    
    int count = 0;
    List<Object[]> batch;
    while ((batch = reader.readBatch()) != null) {
      count += batch.size();
    }
    reader.close();
    
    long endTime = System.currentTimeMillis();
    assertEquals(10000, count, "Should read all rows");
    
    return endTime - startTime;
  }

  private long timeVectorizedReadingWithBatchSize(int batchSize) throws Exception {
    long startTime = System.currentTimeMillis();
    
    VectorizedParquetReader reader = new VectorizedParquetReader(parquetFile.getAbsolutePath(), batchSize);
    
    int count = 0;
    List<Object[]> batch;
    while ((batch = reader.readBatch()) != null) {
      count += batch.size();
    }
    reader.close();
    
    long endTime = System.currentTimeMillis();
    assertEquals(10000, count, "Should read all rows");
    
    return endTime - startTime;
  }
}