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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Frameworks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CsvNextGen schema factory and configuration.
 */
@SuppressWarnings("deprecation")
public class CsvNextGenSchemaTest {

  @Test public void testSchemaFactoryWithDefaultConfig(@TempDir File tempDir) throws IOException {
    // Create test CSV file
    File csvFile = new File(tempDir, "test.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("id,name,value\n");
      writer.write("1,Alice,10.5\n");
      writer.write("2,Bob,20.0\n");
    }

    // Configure schema
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    // Don't specify executionEngine - should default to vectorized

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Schema schema = CsvNextGenSchemaFactory.INSTANCE.create(rootSchema, "test", operand);

    assertNotNull(schema);
    assertTrue(schema instanceof CsvNextGenSchema);

    CsvNextGenSchema csvSchema = (CsvNextGenSchema) schema;
    assertEquals(CsvNextGenSchemaFactory.ExecutionEngineType.VECTORIZED, csvSchema.getExecutionEngine());
    assertEquals(CsvNextGenSchemaFactory.DEFAULT_BATCH_SIZE, csvSchema.getBatchSize());

    // Check that table was discovered
    Table table = schema.getTable("TEST"); // Should be uppercase
    assertNotNull(table);
    assertTrue(table instanceof CsvNextGenTable);

    CsvNextGenTable csvTable = (CsvNextGenTable) table;
    assertEquals(CsvNextGenSchemaFactory.ExecutionEngineType.VECTORIZED, csvTable.getExecutionEngine());
  }

  @Test public void testSchemaFactoryWithLinq4jEngine(@TempDir File tempDir) throws IOException {
    // Create test TSV file
    File tsvFile = new File(tempDir, "data.tsv");
    try (FileWriter writer = new FileWriter(tsvFile, StandardCharsets.UTF_8)) {
      writer.write("col1\tcol2\tcol3\n");
      writer.write("100\tTest\t3.14\n");
    }

    // Configure schema with Linq4j engine
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("executionEngine", "linq4j");
    operand.put("batchSize", 512);

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Schema schema = CsvNextGenSchemaFactory.INSTANCE.create(rootSchema, "test", operand);

    CsvNextGenSchema csvSchema = (CsvNextGenSchema) schema;
    assertEquals(CsvNextGenSchemaFactory.ExecutionEngineType.LINQ4J, csvSchema.getExecutionEngine());
    assertEquals(512, csvSchema.getBatchSize());

    // Check TSV table
    Table table = schema.getTable("DATA");
    assertNotNull(table);
    assertTrue(table instanceof CsvNextGenTable);

    CsvNextGenTable csvTable = (CsvNextGenTable) table;
    assertEquals(CsvNextGenSchemaFactory.ExecutionEngineType.LINQ4J, csvTable.getExecutionEngine());
    assertEquals(512, csvTable.getBatchSize());
  }

  @Test public void testSchemaFactoryWithArrowEngine(@TempDir File tempDir) throws IOException {
    // Create test CSV file
    File csvFile = new File(tempDir, "metrics.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("timestamp,cpu_usage,memory_usage\n");
      writer.write("2024-01-01 10:00:00,45.2,67.8\n");
      writer.write("2024-01-01 10:01:00,52.1,71.3\n");
    }

    // Configure schema with Arrow engine
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("executionEngine", "arrow");
    operand.put("batchSize", 8192);

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Schema schema = CsvNextGenSchemaFactory.INSTANCE.create(rootSchema, "test", operand);

    CsvNextGenSchema csvSchema = (CsvNextGenSchema) schema;
    assertEquals(CsvNextGenSchemaFactory.ExecutionEngineType.ARROW, csvSchema.getExecutionEngine());
    assertEquals(8192, csvSchema.getBatchSize());
  }

  @Test public void testInvalidExecutionEngine(@TempDir File tempDir) {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("executionEngine", "invalid_engine");

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      CsvNextGenSchemaFactory.INSTANCE.create(rootSchema, "test", operand);
    });

    assertTrue(exception.getMessage().contains("Invalid execution engine: invalid_engine"));
    assertTrue(exception.getMessage().contains("Valid options: linq4j, arrow, vectorized"));
  }

  @Test public void testSubdirectoryScanning(@TempDir File tempDir) throws IOException {
    // Create subdirectory structure
    File subDir = new File(tempDir, "subdir");
    subDir.mkdirs();

    // Create CSV in subdirectory
    File csvFile = new File(subDir, "nested.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("a,b,c\n");
      writer.write("1,2,3\n");
    }

    // Create CSV in root directory
    File rootCsv = new File(tempDir, "root.csv");
    try (FileWriter writer = new FileWriter(rootCsv, StandardCharsets.UTF_8)) {
      writer.write("x,y\n");
      writer.write("10,20\n");
    }

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("executionEngine", "vectorized");

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Schema schema = CsvNextGenSchemaFactory.INSTANCE.create(rootSchema, "test", operand);

    // Should find both CSV files
    assertNotNull(schema.getTable("ROOT"));
    assertNotNull(schema.getTable("NESTED"));
  }

  @Test public void testExecutionEngineTypes() {
    // Test enum values
    assertEquals("LINQ4J", CsvNextGenSchemaFactory.ExecutionEngineType.LINQ4J.name());
    assertEquals("ARROW", CsvNextGenSchemaFactory.ExecutionEngineType.ARROW.name());
    assertEquals("VECTORIZED", CsvNextGenSchemaFactory.ExecutionEngineType.VECTORIZED.name());

    // Test case insensitive parsing
    assertEquals(CsvNextGenSchemaFactory.ExecutionEngineType.LINQ4J,
        CsvNextGenSchemaFactory.ExecutionEngineType.valueOf("LINQ4J"));
    assertEquals(CsvNextGenSchemaFactory.ExecutionEngineType.ARROW,
        CsvNextGenSchemaFactory.ExecutionEngineType.valueOf("ARROW"));
    assertEquals(CsvNextGenSchemaFactory.ExecutionEngineType.VECTORIZED,
        CsvNextGenSchemaFactory.ExecutionEngineType.valueOf("VECTORIZED"));
  }
}
