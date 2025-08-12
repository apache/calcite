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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for partitioned table functionality.
 * Covers three scenarios:
 * 1. Hive-style partitioned tables (auto-detected)
 * 2. Unconfigured partitioned tables (warning but works)
 * 3. Configured non-Hive partitioned tables
 */
public class PartitionedTableTest {
  @TempDir
  java.nio.file.Path tempDir;

  private static final String AVRO_SCHEMA_STRING = "{"
      + "\"type\": \"record\","
      + "\"name\": \"Sales\","
      + "\"fields\": ["
      + "  {\"name\": \"order_id\", \"type\": \"int\"},"
      + "  {\"name\": \"product\", \"type\": \"string\"},"
      + "  {\"name\": \"amount\", \"type\": \"double\"}"
      + "]"
      + "}";

  private Schema avroSchema;

  @BeforeEach
  public void setUp() {
    avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
  }

  @Test public void testHiveStylePartitionedTable() throws Exception {
    // Create Hive-style partitioned data
    createHivePartitionedData();

    // Create schema configuration
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put(
        "partitionedTables", java.util.Arrays.asList(
        createPartitionedTableConfig("sales", "sales/**/*.parquet", null)));

    // Run queries
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus fileSchema =
          rootSchema.add("test", FileSchemaFactory.INSTANCE.create(rootSchema, "test", operand));

      // Test 1: Query all data
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"test\".\"sales\"")) {
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1)); // 3 years * 2 months each
      }

      // Test 2: Query with partition filter
      try (Statement stmt = connection.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) FROM \"test\".\"sales\" WHERE \"year\" = '2024'")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1)); // 2 months in 2024
      }

      // Test 3: Verify partition columns are available
      try (Statement stmt = connection.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT DISTINCT \"year\", \"month\" FROM \"test\".\"sales\" ORDER BY \"year\", \"month\"")) {
        // Should have 6 rows: 2022/01, 2022/02, 2023/01, 2023/02, 2024/01, 2024/02
        int count = 0;
        while (rs.next()) {
          String year = rs.getString("year");
          String month = rs.getString("month");
          assertTrue(year.matches("202[234]"));
          assertTrue(month.matches("0[12]"));
          count++;
        }
        assertEquals(6, count);
      }
    }
  }

  @Test public void testUnconfiguredPartitionedTable() throws Exception {
    // Create directory-style partitioned data (no key=value pattern)
    createDirectoryPartitionedData();

    // Create schema configuration with no partition config
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put(
        "partitionedTables", java.util.Arrays.asList(
        createPartitionedTableConfig("events", "events/**/*.parquet", null)));

    // Run queries - should work but without partition columns
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus fileSchema =
          rootSchema.add("test", FileSchemaFactory.INSTANCE.create(rootSchema, "test", operand));

      // Query should work but no partition columns available
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"test\".\"events\"")) {
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1)); // All 6 files
      }

      // Verify no partition columns (should fail)
      try (Statement stmt = connection.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT \"year\" FROM \"test\".\"events\" LIMIT 1")) {
        // This should fail as 'year' column doesn't exist
      } catch (Exception e) {
        // Expected - no partition columns
        assertTrue(e.getMessage().contains("Column 'year' not found") ||
                   e.getMessage().contains("Column 'YEAR' not found"));
      }
    }
  }

  @Test public void testTypedPartitionColumns() throws Exception {
    // Create directory-style partitioned data
    createDirectoryPartitionedData();

    // Create schema configuration with typed partition columns
    Map<String, Object> partitionConfig = new HashMap<>();
    partitionConfig.put("style", "directory");
    partitionConfig.put(
        "columns", java.util.Arrays.asList(
        new HashMap<String, Object>() {{
          put("name", "year");
          put("type", "INTEGER");
        }},
        new HashMap<String, Object>() {{
          put("name", "month");
          put("type", "INTEGER");
        }}));

    Map<String, Object> tableConfig =
        createPartitionedTableConfig("events", "events/**/*.parquet", null);
    tableConfig.put("partitions", partitionConfig);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put("partitionedTables", java.util.Arrays.asList(tableConfig));

    // Run queries with typed partition columns
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus fileSchema =
          rootSchema.add("test", FileSchemaFactory.INSTANCE.create(rootSchema, "test", operand));

      // Test 1: Numeric comparison should work with INTEGER type
      try (Statement stmt = connection.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) FROM \"test\".\"events\" WHERE \"year\" > 2022")) {
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1)); // 2023 and 2024 data
      }

      // Test 2: Verify partition columns are integers
      try (Statement stmt = connection.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT \"year\", \"month\" FROM \"test\".\"events\" LIMIT 1")) {
        assertTrue(rs.next());
        // Should be able to get as int
        int year = rs.getInt("year");
        int month = rs.getInt("month");
        assertTrue(year >= 2022 && year <= 2024);
        assertTrue(month >= 1 && month <= 2);
      }

      // Test 3: Aggregation with numeric partition columns
      try (Statement stmt = connection.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT \"year\", SUM(\"month\") as total_months " +
                                "FROM \"test\".\"events\" GROUP BY \"year\" ORDER BY \"year\"")) {
        assertTrue(rs.next());
        assertEquals(2022, rs.getInt("year"));
        assertEquals(3, rs.getInt("total_months")); // 1 + 2

        assertTrue(rs.next());
        assertEquals(2023, rs.getInt("year"));
        assertEquals(3, rs.getInt("total_months"));

        assertTrue(rs.next());
        assertEquals(2024, rs.getInt("year"));
        assertEquals(3, rs.getInt("total_months"));
      }
    }
  }

  @Test public void testConfiguredNonHivePartitionedTable() throws Exception {
    // Create directory-style partitioned data
    createDirectoryPartitionedData();

    // Create schema configuration with explicit partition config
    Map<String, Object> partitionConfig = new HashMap<>();
    partitionConfig.put("style", "directory");
    partitionConfig.put("columns", java.util.Arrays.asList("year", "month"));

    Map<String, Object> tableConfig =
        createPartitionedTableConfig("events", "events/**/*.parquet", null);
    tableConfig.put("partitions", partitionConfig);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put("partitionedTables", java.util.Arrays.asList(tableConfig));

    // Run queries with partition awareness
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus fileSchema =
          rootSchema.add("test", FileSchemaFactory.INSTANCE.create(rootSchema, "test", operand));

      // Test 1: Query with partition columns available
      try (Statement stmt = connection.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) FROM \"test\".\"events\" WHERE \"year\" = '2024'")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1)); // 2 months in 2024
      }

      // Test 2: Verify partition columns
      try (Statement stmt = connection.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT DISTINCT \"year\", \"month\" FROM \"test\".\"events\" ORDER BY \"year\", \"month\"")) {
        int count = 0;
        while (rs.next()) {
          String year = rs.getString("year");
          String month = rs.getString("month");
          assertTrue(year.matches("202[234]"));
          assertTrue(month.matches("0[12]"));
          count++;
        }
        assertEquals(6, count);
      }
    }
  }

  private void createHivePartitionedData() throws IOException {
    // Create Hive-style directory structure: year=YYYY/month=MM/
    for (int year = 2022; year <= 2024; year++) {
      for (int month = 1; month <= 2; month++) {
        File partDir =
            new File(tempDir.toFile(), String.format(Locale.ROOT, "sales/year=%d/month=%02d", year, month));
        partDir.mkdirs();

        File parquetFile = new File(partDir, "data.parquet");
        writeParquetFile(
            parquetFile, createSalesRecord(year * 100 + month,
            "Product-" + year + month, 100.0 * month));
      }
    }
  }

  private void createDirectoryPartitionedData() throws IOException {
    // Create directory-style structure: YYYY/MM/
    for (int year = 2022; year <= 2024; year++) {
      for (int month = 1; month <= 2; month++) {
        File partDir =
            new File(tempDir.toFile(), String.format(Locale.ROOT, "events/%d/%02d", year, month));
        partDir.mkdirs();

        File parquetFile = new File(partDir, "events.parquet");
        writeParquetFile(
            parquetFile, createSalesRecord(year * 100 + month,
            "Event-" + year + month, 200.0 * month));
      }
    }
  }

  private Map<String, Object> createPartitionedTableConfig(String name,
                                                           String pattern,
                                                           String type) {
    Map<String, Object> config = new HashMap<>();
    config.put("name", name);
    config.put("pattern", pattern);
    if (type != null) {
      config.put("type", type);
    }
    return config;
  }

  private GenericRecord createSalesRecord(int orderId, String product, double amount) {
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("order_id", orderId);
    record.put("product", product);
    record.put("amount", amount);
    return record;
  }

  private void writeParquetFile(File file, GenericRecord... records) throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(file.getAbsolutePath());
    OutputFile outputFile = HadoopOutputFile.fromPath(path, conf);

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
        .withSchema(avroSchema)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {

      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
  }
}
