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
package org.apache.calcite.adapter.file.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Tag;
/**
 * Tests for FormatAwareSchemaResolver.
 */
@Tag("unit")public class FormatAwareSchemaResolverTest {

  @TempDir
  Path tempDir;

  private FormatAwareSchemaResolver resolver;
  private RelDataTypeFactory typeFactory;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    resolver = new FormatAwareSchemaResolver(typeFactory);
  }

  @Test
  public void testSchemaStrategyDefaults() {
    SchemaStrategy defaultStrategy = SchemaStrategy.PARQUET_DEFAULT;
    
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS, defaultStrategy.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.RICHEST_FILE, defaultStrategy.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.LATEST_FILE, defaultStrategy.getJsonStrategy());
  }

  @Test
  public void testCsvSchemaResolution() throws IOException {
    // Create CSV files with different column counts
    File csv1 = createCsvFile("test1.csv", "id,name");
    File csv2 = createCsvFile("test2.csv", "id,name,email,phone");  // Richest file
    File csv3 = createCsvFile("test3.csv", "id,name,email");
    
    List<File> files = Arrays.asList(csv1, csv2, csv3);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    
    // Should use richest file (csv2 with 4 columns)
    assertEquals(4, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("id"));
    assertTrue(schema.getFieldNames().contains("name"));
    assertTrue(schema.getFieldNames().contains("email"));
    assertTrue(schema.getFieldNames().contains("phone"));
  }

  @Test
  public void testJsonSchemaResolution() throws IOException {
    // Create JSON files with different structures
    File json1 = createJsonFile("old.json", "{\"id\": 1, \"name\": \"old\"}");
    
    // Make json2 newer by setting last modified time
    File json2 = createJsonFile("new.json", "{\"id\": 1, \"name\": \"new\", \"email\": \"test@example.com\"}");
    json2.setLastModified(json1.lastModified() + 1000); // 1 second newer
    
    List<File> files = Arrays.asList(json1, json2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    
    // Should use latest file (json2 with email field)
    assertEquals(3, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("id"));
    assertTrue(schema.getFieldNames().contains("name"));
    assertTrue(schema.getFieldNames().contains("email"));
  }

  @Test
  public void testMixedFormatResolution() throws IOException {
    // Create mixed format files
    File csvFile = createCsvFile("data.csv", "id,name,amount");
    File jsonFile = createJsonFile("data.json", "{\"id\": 1, \"title\": \"test\"}");
    
    List<File> files = Arrays.asList(csvFile, jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    
    // Should prioritize CSV over JSON (based on format priority)
    assertEquals(3, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("id"));
    assertTrue(schema.getFieldNames().contains("name"));
    assertTrue(schema.getFieldNames().contains("amount"));
  }

  @Test
  public void testEmptyFilesList() {
    List<File> emptyFiles = Arrays.asList();
    RelDataType schema = resolver.resolveSchema(emptyFiles, SchemaStrategy.PARQUET_DEFAULT);
    
    assertEquals(0, schema.getFieldCount());
  }

  @Test
  public void testSchemaStrategyParsing() {
    SchemaStrategy conservative = SchemaStrategy.CONSERVATIVE;
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_FILE, conservative.getParquetStrategy());
    
    SchemaStrategy aggressive = SchemaStrategy.AGGRESSIVE_UNION;
    assertEquals(SchemaStrategy.ParquetStrategy.UNION_ALL_COLUMNS, aggressive.getParquetStrategy());
  }

  @Test
  public void testValidationLevels() {
    SchemaStrategy strategy = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS,
        SchemaStrategy.CsvStrategy.RICHEST_FILE,
        SchemaStrategy.JsonStrategy.LATEST_FILE,
        Arrays.asList("parquet", "csv", "json"),
        SchemaStrategy.ValidationLevel.ERROR
    );
    
    assertEquals(SchemaStrategy.ValidationLevel.ERROR, strategy.getValidationLevel());
  }

  // Helper methods

  private File createCsvFile(String name, String header) throws IOException {
    File file = tempDir.resolve(name).toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(header + "\n");
      writer.write("1,test\n"); // Add a data row
    }
    return file;
  }

  private File createJsonFile(String name, String content) throws IOException {
    File file = tempDir.resolve(name).toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }
}