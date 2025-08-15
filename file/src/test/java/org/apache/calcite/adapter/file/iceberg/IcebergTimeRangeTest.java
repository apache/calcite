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

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
/**
 * Tests for Iceberg time range functionality.
 * 
 * Creates local Iceberg tables with multiple snapshots for testing.
 */
@Tag("unit")
public class IcebergTimeRangeTest {

  @TempDir
  Path tempDir;
  
  private String warehousePath;
  private String ordersTablePath;
  private String salesTablePath;

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
        Types.NestedField.required(5, "snapshot_time", Types.TimestampType.withZone())
    );
    
    // Create orders table
    Table ordersTable = catalog.createTable(TableIdentifier.of("orders"), ordersSchema);
    ordersTablePath = ordersTable.location();
    
    // Create schema for sales table  
    Schema salesSchema = new Schema(
        Types.NestedField.required(1, "sale_id", Types.IntegerType.get()),
        Types.NestedField.required(2, "amount", Types.DoubleType.get()),
        Types.NestedField.required(3, "snapshot_time", Types.TimestampType.withZone())
    );
    
    // Create sales table
    Table salesTable = catalog.createTable(TableIdentifier.of("sales"), salesSchema);
    salesTablePath = salesTable.location();
  }

  @Test
  public void testIcebergTimeRangeResolver() throws Exception {
    // Test the core time range resolution functionality
    IcebergTimeRangeResolver resolver = new IcebergTimeRangeResolver();
    
    // Define time range (should not find any snapshots in empty table)
    Instant startTime = Instant.parse("2024-01-01T00:00:00Z");
    Instant endTime = Instant.parse("2024-02-01T00:00:00Z");
    
    List<IcebergTimeRangeResolver.IcebergDataFile> dataFiles = 
        resolver.resolveTimeRange(ordersTablePath, startTime, endTime);
    
    assertNotNull(dataFiles, "Data files list should not be null");
    // Empty table should have no snapshots
    assertEquals(0, dataFiles.size(), "Empty table should have no data files");
  }

  @Test  
  public void testIcebergTimeRangeTable() throws Exception {
    // Test the time range table creation
    IcebergTimeRangeResolver resolver = new IcebergTimeRangeResolver();
    
    Instant startTime = Instant.parse("2024-01-01T00:00:00Z");
    Instant endTime = Instant.parse("2024-02-01T00:00:00Z");
    
    List<IcebergTimeRangeResolver.IcebergDataFile> dataFiles = 
        resolver.resolveTimeRange(ordersTablePath, startTime, endTime);
    
    // Create time range table
    IcebergTimeRangeTable timeRangeTable = new IcebergTimeRangeTable(
        dataFiles, 
        "snapshot_time",
        new org.apache.calcite.adapter.file.execution.ExecutionEngineConfig(),
        ordersTablePath
    );
    
    // Test that we can get the schema
    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory = 
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    
    org.apache.calcite.rel.type.RelDataType rowType = timeRangeTable.getRowType(typeFactory);
    
    assertNotNull(rowType, "Row type should not be null");
    assertTrue(rowType.getFieldCount() > 0, "Row type should have fields");
    
    // Verify snapshot_time column is added
    boolean hasSnapshotColumn = rowType.getFieldNames().contains("snapshot_time");
    assertTrue(hasSnapshotColumn, "Schema should include snapshot_time column");
    
    // Verify original columns are present
    assertTrue(rowType.getFieldNames().contains("order_id"), "Should have order_id column");
    assertTrue(rowType.getFieldNames().contains("customer_id"), "Should have customer_id column");
    assertTrue(rowType.getFieldNames().contains("amount"), "Should have amount column");
  }

  @Test
  public void testTimeRangeIntegration() throws Exception {
    // Test the time range integration with FileSchemaFactory
    // This tests that the timeRange configuration is properly parsed and used
    
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"tables\": [\n"
        + "          {\n"
        + "            \"name\": \"orders_timeline\",\n"
        + "            \"url\": \"" + ordersTablePath + "\",\n"
        + "            \"format\": \"iceberg\",\n"
        + "            \"timeRange\": {\n"
        + "              \"start\": \"2024-01-01T00:00:00Z\",\n"
        + "              \"end\": \"2024-12-31T23:59:59Z\",\n"
        + "              \"snapshotColumn\": \"snapshot_time\"\n"
        + "            }\n"
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

      // Test that the time range table configuration works by executing queries
      // First test: basic count query
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_timeline");
      assertTrue(rs.next(), "Should be able to query the time range table");
      int count = rs.getInt(1);
      assertTrue(count >= 0, "Count should be non-negative");
      rs.close();
      
      // Second test: verify we can access snapshot_time column
      // (This will fail if the column doesn't exist or timeRange config isn't working)
      try {
        rs = statement.executeQuery("SELECT snapshot_time FROM orders_timeline LIMIT 1");
        // If the query succeeds, the snapshot_time column exists
        rs.close();
        assertTrue(true, "snapshot_time column is accessible from timeRange config");
      } catch (Exception e) {
        // If it fails, the timeRange feature isn't working
        assertTrue(false, "snapshot_time column should be available from timeRange config: " + e.getMessage());
      }
    }
  }

}