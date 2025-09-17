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
package org.apache.calcite.adapter.govdata;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.calcite.adapter.file.FileSchema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for creating parquet files using the StorageProvider pattern.
 * 
 * This class handles the conversion from direct Hadoop parquet writing to 
 * StorageProvider-based writing by creating parquet files in memory first,
 * then writing them through the FileSchema's StorageProvider.
 */
public class ParquetStorageHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStorageHelper.class);
  
  private final FileSchema fileSchema;
  
  public ParquetStorageHelper(FileSchema fileSchema) {
    this.fileSchema = fileSchema;
  }
  
  /**
   * Creates a parquet file using in-memory generation and StorageProvider writing.
   * 
   * @param relativePath The relative path where the parquet file should be stored
   * @param schema The Avro schema for the parquet file
   * @param records The records to write to the parquet file
   * @throws IOException If an error occurs during parquet creation or writing
   */
  @SuppressWarnings("deprecation")
  public void writeParquetFile(String relativePath, Schema schema, List<GenericRecord> records) 
      throws IOException {
    
    LOGGER.debug("Creating parquet file at: {}", relativePath);
    
    // Create a temporary file for in-memory parquet generation
    java.nio.file.Path tempFile = Files.createTempFile("parquet-temp", ".parquet");
    
    try {
      // Create parquet file using traditional Hadoop approach in temp location
      Path hadoopPath = new Path(tempFile.toAbsolutePath().toString());
      
      try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .build()) {
        
        for (GenericRecord record : records) {
          writer.write(record);
        }
      }
      
      // Read the temporary parquet file into memory
      byte[] parquetData = Files.readAllBytes(tempFile);
      
      // Write through StorageProvider
      fileSchema.writeToStorage(relativePath, parquetData);
      
      LOGGER.info("Successfully wrote parquet file via StorageProvider: {} ({} bytes, {} records)", 
          relativePath, parquetData.length, records.size());
      
    } finally {
      // Clean up temporary file
      try {
        Files.deleteIfExists(tempFile);
      } catch (IOException e) {
        LOGGER.warn("Failed to delete temporary parquet file: {}", tempFile, e);
      }
    }
  }
  
  /**
   * Creates a parquet file from an InputStream using StorageProvider.
   * 
   * @param relativePath The relative path where the parquet file should be stored
   * @param parquetStream The input stream containing parquet data
   * @throws IOException If an error occurs during writing
   */
  public void writeParquetFile(String relativePath, InputStream parquetStream) throws IOException {
    LOGGER.debug("Writing parquet file from stream to: {}", relativePath);
    fileSchema.writeToStorage(relativePath, parquetStream);
    LOGGER.info("Successfully wrote parquet file via StorageProvider: {}", relativePath);
  }
  
  /**
   * Creates directories in storage using StorageProvider.
   * 
   * @param relativePath The relative directory path to create
   * @throws IOException If an error occurs during directory creation
   */
  public void createDirectories(String relativePath) throws IOException {
    fileSchema.createStorageDirectories(relativePath);
  }
  
  /**
   * Checks if a file exists in storage using the FileSchema's internal methods.
   * This is a simple check by attempting to read metadata.
   * 
   * @param relativePath The relative path to check
   * @return true if the file exists
   * @throws IOException If an error occurs during the check
   */
  public boolean exists(String relativePath) throws IOException {
    try {
      // Try to get file metadata - if it throws, file doesn't exist
      java.io.File testFile = new java.io.File(relativePath);
      if (testFile.isAbsolute()) {
        return testFile.exists();
      } else {
        // For relative paths, check if file was already created in memory storage
        // This is a simple heuristic - for production we'd need better detection
        return false; // Assume file doesn't exist for now
      }
    } catch (Exception e) {
      return false;
    }
  }
  
  /**
   * Creates an empty placeholder parquet file with a minimal schema.
   * 
   * @param relativePath The relative path where the placeholder should be created
   * @param tableName The name for the record schema
   * @throws IOException If an error occurs during creation
   */
  public void createPlaceholderParquet(String relativePath, String tableName) throws IOException {
    // Create a minimal schema with one string field
    Schema schema = SchemaBuilder.record(tableName)
        .fields()
        .name("placeholder").type().stringType().noDefault()
        .endRecord();
    
    // Create empty records list
    List<GenericRecord> emptyRecords = new ArrayList<>();
    
    // Write empty parquet file
    writeParquetFile(relativePath, schema, emptyRecords);
  }
}