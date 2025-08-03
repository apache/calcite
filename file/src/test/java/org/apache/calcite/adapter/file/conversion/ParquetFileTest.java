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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
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
 * Test that Parquet files work as regular input files in the file adapter.
 */
public class ParquetFileTest {
  @TempDir
  java.nio.file.Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    // Create a Parquet file with test data
    File parquetFile = new File(tempDir.toFile(), "employees.parquet");
    createTestParquetFile(parquetFile);
  }

  private void createTestParquetFile(File parquetFile) throws Exception {
    // Define Avro schema
    Schema schema = SchemaBuilder.record("Employee")
        .namespace("org.apache.calcite.adapter.file.test")
        .fields()
        .name("id").type().intType().noDefault()
        .name("name").type().stringType().noDefault()
        .name("department").type().stringType().noDefault()
        .name("salary").type().doubleType().noDefault()
        .endRecord();

    // Create Parquet writer
    Path hadoopPath = new Path(parquetFile.getAbsolutePath());
    Configuration conf = new Configuration();

    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
        .withConf(conf)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();

    try {
      // Write test data
      GenericRecord record = new GenericData.Record(schema);

      record.put("id", 1);
      record.put("name", "Alice");
      record.put("department", "Engineering");
      record.put("salary", 120000.0);
      writer.write(record);

      record.put("id", 2);
      record.put("name", "Bob");
      record.put("department", "Sales");
      record.put("salary", 95000.0);
      writer.write(record);

      record.put("id", 3);
      record.put("name", "Charlie");
      record.put("department", "Engineering");
      record.put("salary", 110000.0);
      writer.write(record);

      record.put("id", 4);
      record.put("name", "Diana");
      record.put("department", "HR");
      record.put("salary", 85000.0);
      writer.write(record);

    } finally {
      writer.close();
    }

    System.out.println("Created test Parquet file: "
        + parquetFile.getAbsolutePath());
  }

  @Test public void testQueryParquetFileDirectly() throws Exception {
    System.out.println("\n=== TESTING PARQUET FILE AS INPUT ===");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection =
             connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Configure file schema to read Parquet files
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());

      System.out.println(
          "\n1. Creating schema with directory containing Parquet file");
      SchemaPlus fileSchema =
          rootSchema.add("PARQUET_TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "PARQUET_TEST", operand));

      try (Statement stmt = connection.createStatement()) {
        // List all available tables
        System.out.println("\n2. Listing all tables in schema:");
        ResultSet tables =
            connection.getMetaData().getTables(null, "PARQUET_TEST", "%", null);

        System.out.println("   Available tables:");
        boolean foundEmployees = false;
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          System.out.println("   - " + tableName);
          if (tableName.equals("EMPLOYEES")) {
            foundEmployees = true;
          }
        }

        assertTrue(foundEmployees, "Employees table should be found");
        System.out.println(
            "\n   ✓ SUCCESS: Parquet file 'employees' is registered as a table!");

        // Query the Parquet file
        System.out.println("\n3. Querying the Parquet file:");
        ResultSet rs =
            stmt.executeQuery("SELECT * FROM PARQUET_TEST.\"EMPLOYEES\" ORDER BY \"id\"");

        System.out.println("   ID | Name     | Department  | Salary");
        System.out.println("   ---|----------|-------------|--------");

        int rowCount = 0;
        while (rs.next()) {
          System.out.printf(Locale.ROOT, "   %2d | %-8s | %-11s | %.2f%n",
              rs.getInt("id"),
              rs.getString("name"),
              rs.getString("department"),
              rs.getDouble("salary"));
          rowCount++;
        }

        assertEquals(4, rowCount, "Should have 4 rows in Parquet file");
        System.out.println("\n   ✓ Successfully queried Parquet file with "
            + rowCount + " rows!");

        // Test aggregation query
        System.out.println("\n4. Testing aggregation query on Parquet file:");
        ResultSet aggRs =
            stmt.executeQuery("SELECT \"department\", COUNT(*) as emp_count, AVG(\"salary\") as avg_salary "
            + "FROM PARQUET_TEST.\"EMPLOYEES\" "
            + "GROUP BY \"department\" "
            + "ORDER BY \"department\"");

        System.out.println("   Department  | Count | Avg Salary");
        System.out.println("   ------------|-------|------------");

        while (aggRs.next()) {
          System.out.printf(Locale.ROOT, "   %-11s | %5d | %.2f%n",
              aggRs.getString("department"),
              aggRs.getInt("emp_count"),
              aggRs.getDouble("avg_salary"));
        }

        System.out.println("\n   ✓ Aggregation queries work on Parquet files!");
      }
    }
  }

  @Test public void testParquetFileWithExplicitFormat() throws Exception {
    System.out.println("\n=== TESTING PARQUET FILE WITH EXPLICIT FORMAT ===");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection =
             connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Configure file schema with explicit table mapping
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());

      // Create explicit table mapping
      Map<String, Object> tableMapping = new HashMap<>();
      tableMapping.put("name", "emp_data");
      tableMapping.put("url",
          new File(tempDir.toFile(), "employees.parquet").getAbsolutePath());
      tableMapping.put("format", "parquet");

      operand.put("tables", java.util.Arrays.asList(tableMapping));

      System.out.println(
          "\n1. Creating schema with explicit Parquet table mapping");
      SchemaPlus fileSchema =
          rootSchema.add("PARQUET_EXPLICIT", FileSchemaFactory.INSTANCE.create(rootSchema, "PARQUET_EXPLICIT", operand));

      try (Statement stmt = connection.createStatement()) {
        // Query the explicitly mapped Parquet file
        System.out.println("\n2. Querying the explicitly mapped Parquet file:");
        ResultSet rs =
            stmt.executeQuery("SELECT * FROM PARQUET_EXPLICIT.\"EMP_DATA\" "
            + "WHERE \"salary\" > 100000 ORDER BY \"salary\" DESC");

        System.out.println("   High earners (salary > 100k):");
        System.out.println("   Name     | Department  | Salary");
        System.out.println("   ---------|-------------|--------");

        int rowCount = 0;
        while (rs.next()) {
          System.out.printf(Locale.ROOT, "   %-8s | %-11s | %.2f%n",
              rs.getString("name"),
              rs.getString("department"),
              rs.getDouble("salary"));
          rowCount++;
        }

        assertEquals(2, rowCount, "Should have 2 employees with salary > 100k");
        System.out.println(
            "\n   ✓ Successfully queried explicitly mapped Parquet file!");
      }
    }
  }
}
