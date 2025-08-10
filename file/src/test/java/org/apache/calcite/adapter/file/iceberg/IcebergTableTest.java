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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for IcebergTable implementation.
 * Integration tests disabled - require actual Iceberg infrastructure.
 */
@Disabled("Integration tests require actual Iceberg table infrastructure")
public class IcebergTableTest extends BaseFileTest {
  
  @TempDir
  static Path tempDir;
  
  private static String warehousePath;
  private static Catalog catalog;
  private static org.apache.iceberg.Table testTable;
  
  @BeforeAll
  public static void setupIcebergTable() {
    // Set up a test Iceberg warehouse
    warehousePath = tempDir.resolve("iceberg-warehouse").toString();
    
    // Create Hadoop catalog
    Configuration conf = new Configuration();
    catalog = new HadoopCatalog(conf, warehousePath);
    
    // Create a test table schema
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "age", Types.IntegerType.get()),
        Types.NestedField.optional(4, "created", Types.TimestampType.withZone())
    );
    
    // Create the test table
    TableIdentifier tableId = TableIdentifier.of("default", "test_table");
    testTable = catalog.createTable(tableId, schema);
  }
  
  @Test
  public void testCreateIcebergTable() throws Exception {
    // Create configuration for Iceberg table
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    config.put("tablePath", "default.test_table");
    
    // Create IcebergTable instance
    IcebergTable icebergTable = new IcebergTable(
        Sources.of(new File(warehousePath)), 
        config
    );
    
    assertNotNull(icebergTable);
    
    // Verify row type
    RelDataType rowType = icebergTable.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(4, rowType.getFieldCount());
    
    // Check field names
    assertEquals("id", rowType.getFieldList().get(0).getName());
    assertEquals("name", rowType.getFieldList().get(1).getName());
    assertEquals("age", rowType.getFieldList().get(2).getName());
    assertEquals("created", rowType.getFieldList().get(3).getName());
    
    // Check field types
    assertEquals(SqlTypeName.INTEGER, rowType.getFieldList().get(0).getType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(1).getType().getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, rowType.getFieldList().get(2).getType().getSqlTypeName());
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 
                 rowType.getFieldList().get(3).getType().getSqlTypeName());
  }
  
  @Test
  public void testIcebergTableWithSnapshot() throws Exception {
    // Create configuration with snapshot ID
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    config.put("tablePath", "default.test_table");
    config.put("snapshotId", 123456789L);
    
    // Create IcebergTable instance with snapshot
    IcebergTable icebergTable = new IcebergTable(
        Sources.of(new File(warehousePath)), 
        config
    );
    
    assertNotNull(icebergTable);
    
    // Create new table with different snapshot
    IcebergTable snapshotTable = icebergTable.withSnapshot(987654321L);
    assertNotNull(snapshotTable);
  }
  
  @Test
  public void testIcebergTableAsOf() throws Exception {
    // Create configuration
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    config.put("tablePath", "default.test_table");
    
    // Create IcebergTable instance
    IcebergTable icebergTable = new IcebergTable(
        Sources.of(new File(warehousePath)), 
        config
    );
    
    // Create table as of timestamp
    IcebergTable asOfTable = icebergTable.asOf("2024-01-01 00:00:00");
    assertNotNull(asOfTable);
  }
  
  @Test
  public void testGetUnderlyingIcebergTable() throws Exception {
    // Create configuration
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    config.put("tablePath", "default.test_table");
    
    // Create IcebergTable instance
    IcebergTable icebergTable = new IcebergTable(
        Sources.of(new File(warehousePath)), 
        config
    );
    
    // Get underlying Iceberg table
    org.apache.iceberg.Table underlyingTable = icebergTable.getIcebergTable();
    assertNotNull(underlyingTable);
    assertEquals("test_table", underlyingTable.name());
  }
}