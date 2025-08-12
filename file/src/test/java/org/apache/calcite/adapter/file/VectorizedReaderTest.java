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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.execution.parquet.VectorizedParquetReader;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for VectorizedParquetReader.
 */
@Tag("performance")public class VectorizedReaderTest {

  @TempDir
  File tempDir;

  @Test
  void testBasicVectorizedReading() throws Exception {
    // Create test Parquet file
    File parquetFile = createTestParquetFile(1000);
    
    // Read using vectorized reader
    VectorizedParquetReader reader = new VectorizedParquetReader(parquetFile.getAbsolutePath());
    
    int totalRows = 0;
    List<Object[]> batch;
    
    while ((batch = reader.readBatch()) != null) {
      assertNotNull(batch);
      assertFalse(batch.isEmpty(), "Batch should not be empty");
      
      for (Object[] row : batch) {
        assertNotNull(row);
        assertEquals(3, row.length, "Each row should have 3 columns");
        
        // Verify data types
        assertTrue(row[0] instanceof Long, "First column should be Long");
        assertTrue(row[1] instanceof String, "Second column should be String");
        assertTrue(row[2] instanceof Double, "Third column should be Double");
        
        totalRows++;
      }
    }
    
    reader.close();
    
    assertEquals(1000, totalRows, "Should read all 1000 rows");
  }

  @Test
  void testBatchSizes() throws Exception {
    File parquetFile = createTestParquetFile(5000);
    
    // Test different batch sizes
    int[] batchSizes = {100, 500, 1024, 2000};
    
    for (int batchSize : batchSizes) {
      VectorizedParquetReader reader = new VectorizedParquetReader(
          parquetFile.getAbsolutePath(), batchSize);
      
      int totalRows = 0;
      int batchCount = 0;
      List<Object[]> batch;
      
      while ((batch = reader.readBatch()) != null) {
        batchCount++;
        totalRows += batch.size();
        
        // Verify batch size (except possibly the last batch)
        if (totalRows < 5000) {
          assertTrue(batch.size() <= batchSize, 
              "Batch size should not exceed " + batchSize);
        }
      }
      
      reader.close();
      
      assertEquals(5000, totalRows, 
          "Should read all 5000 rows with batch size " + batchSize);
      
      System.out.println("Batch size " + batchSize + ": " + batchCount + " batches");
    }
  }

  @Test
  void testComparisonWithAvroReader() throws Exception {
    File parquetFile = createTestParquetFile(500);
    
    // Read with Avro reader (record-by-record)
    List<Object[]> avroRows = new ArrayList<>();
    Path hadoopPath = new Path(parquetFile.getAbsolutePath());
    Configuration conf = new Configuration();
    
    @SuppressWarnings("deprecation")
    ParquetReader<GenericRecord> avroReader = 
        AvroParquetReader.<GenericRecord>builder(hadoopPath)
        .withConf(conf)
        .build();
    
    GenericRecord record;
    while ((record = avroReader.read()) != null) {
      Object[] row = new Object[3];
      row[0] = record.get(0);
      row[1] = record.get(1).toString();
      row[2] = record.get(2);
      avroRows.add(row);
    }
    avroReader.close();
    
    // Read with vectorized reader
    List<Object[]> vectorizedRows = new ArrayList<>();
    VectorizedParquetReader vectorizedReader = 
        new VectorizedParquetReader(parquetFile.getAbsolutePath());
    
    List<Object[]> batch;
    while ((batch = vectorizedReader.readBatch()) != null) {
      vectorizedRows.addAll(batch);
    }
    vectorizedReader.close();
    
    // Compare results
    assertEquals(avroRows.size(), vectorizedRows.size(), 
        "Both readers should read the same number of rows");
    
    for (int i = 0; i < avroRows.size(); i++) {
      Object[] avroRow = avroRows.get(i);
      Object[] vectorizedRow = vectorizedRows.get(i);
      
      assertEquals(avroRow[0], vectorizedRow[0], 
          "ID should match at row " + i);
      assertEquals(avroRow[1], vectorizedRow[1], 
          "Name should match at row " + i);
      assertEquals(avroRow[2], vectorizedRow[2], 
          "Value should match at row " + i);
    }
  }

  @Test
  void testEmptyFile() throws Exception {
    File parquetFile = createTestParquetFile(0);
    
    VectorizedParquetReader reader = 
        new VectorizedParquetReader(parquetFile.getAbsolutePath());
    
    List<Object[]> batch = reader.readBatch();
    assertNull(batch, "Should return null for empty file");
    
    reader.close();
  }

  @Test
  void testPerformanceComparison() throws Exception {
    File parquetFile = createTestParquetFile(10000);
    
    // Warm up
    for (int i = 0; i < 3; i++) {
      readWithVectorized(parquetFile);
      readWithAvro(parquetFile);
    }
    
    // Time Avro reader
    long avroStart = System.currentTimeMillis();
    int avroCount = readWithAvro(parquetFile);
    long avroTime = System.currentTimeMillis() - avroStart;
    
    // Time vectorized reader
    long vectorizedStart = System.currentTimeMillis();
    int vectorizedCount = readWithVectorized(parquetFile);
    long vectorizedTime = System.currentTimeMillis() - vectorizedStart;
    
    assertEquals(avroCount, vectorizedCount, "Both should read same number of rows");
    
    System.out.println("\nPerformance Results:");
    System.out.println("  Avro (record-by-record): " + avroTime + " ms");
    System.out.println("  Vectorized (batch):      " + vectorizedTime + " ms");
    
    double speedup = (double) avroTime / vectorizedTime;
    System.out.println("  Speedup:                 " + String.format("%.2fx", speedup));
  }

  private File createTestParquetFile(int numRows) throws IOException {
    File parquetFile = new File(tempDir, "test_" + numRows + ".parquet");
    
    // Define schema
    Schema schema = SchemaBuilder.record("TestRecord")
        .namespace("test")
        .fields()
        .name("id").type().longType().noDefault()
        .name("name").type().stringType().noDefault()
        .name("value").type().doubleType().noDefault()
        .endRecord();
    
    // Write test data
    Path hadoopPath = new Path(parquetFile.getAbsolutePath());
    Configuration conf = new Configuration();
    
    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = 
        AvroParquetWriter.<GenericRecord>builder(hadoopPath)
        .withConf(conf)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();
    
    for (int i = 0; i < numRows; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("id", (long) i);
      record.put("name", "Name_" + i);
      record.put("value", i * 1.5);
      writer.write(record);
    }
    
    writer.close();
    
    return parquetFile;
  }

  private int readWithAvro(File parquetFile) throws IOException {
    Path hadoopPath = new Path(parquetFile.getAbsolutePath());
    Configuration conf = new Configuration();
    
    @SuppressWarnings("deprecation")
    ParquetReader<GenericRecord> reader = 
        AvroParquetReader.<GenericRecord>builder(hadoopPath)
        .withConf(conf)
        .build();
    
    int count = 0;
    while (reader.read() != null) {
      count++;
    }
    reader.close();
    
    return count;
  }

  private int readWithVectorized(File parquetFile) throws IOException {
    VectorizedParquetReader reader = 
        new VectorizedParquetReader(parquetFile.getAbsolutePath());
    
    int count = 0;
    List<Object[]> batch;
    while ((batch = reader.readBatch()) != null) {
      count += batch.size();
    }
    reader.close();
    
    return count;
  }
}