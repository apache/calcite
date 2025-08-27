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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.BaseFileTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for Iceberg tables with actual data to ensure DuckDB's iceberg_scan works properly.
 */
public class IcebergNonEmptyTableTest extends BaseFileTest {
  
  @TempDir
  Path tempDir;
  
  private String warehousePath;
  private String ordersTablePath;

  @BeforeEach
  public void setUp() throws Exception {
    warehousePath = tempDir.resolve("warehouse").toString();
    
    // Create Iceberg catalog
    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    
    // Create schema for orders table
    Schema ordersSchema = new Schema(
        Types.NestedField.required(1, "order_id", Types.IntegerType.get()),
        Types.NestedField.required(2, "customer_id", Types.StringType.get()),
        Types.NestedField.required(3, "product_id", Types.StringType.get()),
        Types.NestedField.required(4, "amount", Types.DoubleType.get()),
        Types.NestedField.required(5, "order_date", Types.TimestampType.withZone())
    );
    
    // Create orders table
    Table ordersTable = catalog.createTable(
        TableIdentifier.of("orders"), 
        ordersSchema,
        PartitionSpec.unpartitioned()
    );
    ordersTablePath = ordersTable.location();
    
    // Add some data to the table
    addDataToTable(ordersTable, ordersSchema);
  }

  private void addDataToTable(Table table, Schema schema) throws Exception {
    // Create a data file writer
    OutputFile outputFile = table.io().newOutputFile(
        table.location() + "/data/orders-" + UUID.randomUUID() + ".parquet");
    
    DataWriter<Record> dataWriter = Parquet.writeData(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();
    
    // Add some sample records
    OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
    
    GenericRecord record1 = GenericRecord.create(schema);
    record1.setField("order_id", 1);
    record1.setField("customer_id", "CUST001");
    record1.setField("product_id", "PROD001");
    record1.setField("amount", 100.50);
    record1.setField("order_date", now);
    dataWriter.write(record1);
    
    GenericRecord record2 = GenericRecord.create(schema);
    record2.setField("order_id", 2);
    record2.setField("customer_id", "CUST002");
    record2.setField("product_id", "PROD002");
    record2.setField("amount", 250.75);
    record2.setField("order_date", now.plusDays(1));
    dataWriter.write(record2);
    
    GenericRecord record3 = GenericRecord.create(schema);
    record3.setField("order_id", 3);
    record3.setField("customer_id", "CUST001");
    record3.setField("product_id", "PROD003");
    record3.setField("amount", 75.00);
    record3.setField("order_date", now.plusDays(2));
    dataWriter.write(record3);
    
    // Close the writer and commit the data file
    dataWriter.close();
    
    // Commit the new data file to the table
    table.newAppend()
        .appendFile(dataWriter.toDataFile())
        .commit();
  }

  @Test
  public void testNonEmptyIcebergTableWithDuckDB() throws Exception {
    // This test verifies that DuckDB can properly query non-empty Iceberg tables
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"ephemeralCache\": true,\n"
        + "        \"tables\": [\n"
        + "          {\n"
        + "            \"name\": \"orders\",\n"
        + "            \"url\": \"" + ordersTablePath + "\",\n"
        + "            \"format\": \"iceberg\"\n"
        + "          }\n"
        + "        ]\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("quotedCasing", "UNCHANGED");
    info.setProperty("caseSensitive", "false");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Test 1: Basic count query
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders");
      assertTrue(rs.next(), "Should have a result row");
      int count = rs.getInt(1);
      assertEquals(3, count, "Should have 3 rows in the table");
      rs.close();
      
      // Test 2: Select all columns
      rs = statement.executeQuery("SELECT * FROM orders ORDER BY order_id");
      
      // First row
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("order_id"));
      assertEquals("CUST001", rs.getString("customer_id"));
      assertEquals("PROD001", rs.getString("product_id"));
      assertEquals(100.50, rs.getDouble("amount"), 0.01);
      
      // Second row
      assertTrue(rs.next());
      assertEquals(2, rs.getInt("order_id"));
      assertEquals("CUST002", rs.getString("customer_id"));
      assertEquals("PROD002", rs.getString("product_id"));
      assertEquals(250.75, rs.getDouble("amount"), 0.01);
      
      // Third row
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("order_id"));
      assertEquals("CUST001", rs.getString("customer_id"));
      assertEquals("PROD003", rs.getString("product_id"));
      assertEquals(75.00, rs.getDouble("amount"), 0.01);
      
      rs.close();
      
      // Test 3: Aggregation query
      rs = statement.executeQuery("SELECT customer_id, SUM(amount) as total_amount " +
                                  "FROM orders GROUP BY customer_id ORDER BY customer_id");
      
      assertTrue(rs.next());
      assertEquals("CUST001", rs.getString("customer_id"));
      assertEquals(175.50, rs.getDouble("total_amount"), 0.01); // 100.50 + 75.00
      
      assertTrue(rs.next());
      assertEquals("CUST002", rs.getString("customer_id"));
      assertEquals(250.75, rs.getDouble("total_amount"), 0.01);
      
      rs.close();
      
      // Test 4: Filter query
      rs = statement.executeQuery("SELECT * FROM orders WHERE amount > 100");
      int largeOrderCount = 0;
      while (rs.next()) {
        assertTrue(rs.getDouble("amount") > 100);
        largeOrderCount++;
      }
      assertEquals(2, largeOrderCount, "Should have 2 orders with amount > 100");
      rs.close();
    }
  }
}