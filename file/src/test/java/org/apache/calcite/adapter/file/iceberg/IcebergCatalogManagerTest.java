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

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ConnectException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for IcebergCatalogManager.
 */
public class IcebergCatalogManagerTest {

  @TempDir
  Path tempDir;

  @Test
  public void testCreateHadoopCatalog() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", tempDir.toString());
    
    Catalog catalog = IcebergCatalogManager.getCatalogForProvider("hadoop", config);
    assertNotNull(catalog);
  }

  @Test
  @Disabled("Requires actual Iceberg table infrastructure")
  public void testLoadTableFromHadoopCatalog() throws Exception {
    // Setup
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    // Create catalog and table
    Catalog catalog = IcebergCatalogManager.getCatalogForProvider("hadoop", config);
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "data", Types.StringType.get())
    );
    
    TableIdentifier tableId = TableIdentifier.of("test_table");
    Table createdTable = catalog.createTable(tableId, schema);
    assertNotNull(createdTable);
    
    // Load table using catalog manager
    config.put("tablePath", "test_table");
    Table loadedTable = IcebergCatalogManager.loadTable(config, "test_table");
    assertNotNull(loadedTable);
    assertEquals("test_table", loadedTable.name());
  }

  @Test
  @Disabled("Requires actual Iceberg table infrastructure")
  public void testDirectPathLoading() throws Exception {
    // Setup - create a table first
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    Catalog catalog = IcebergCatalogManager.getCatalogForProvider("hadoop", config);
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );
    
    TableIdentifier tableId = TableIdentifier.of("direct_table");
    Table createdTable = catalog.createTable(tableId, schema);
    String tablePath = createdTable.location();
    
    // Load table directly by path
    Map<String, Object> directConfig = new HashMap<>();
    Table directTable = IcebergCatalogManager.loadTable(directConfig, tablePath);
    assertNotNull(directTable);
    assertEquals(tablePath, directTable.location());
  }

  @Test
  public void testCatalogCaching() {
    Map<String, Object> config1 = new HashMap<>();
    config1.put("catalogType", "hadoop");
    config1.put("warehousePath", tempDir.resolve("warehouse1").toString());
    
    Map<String, Object> config2 = new HashMap<>();
    config2.put("catalogType", "hadoop");
    config2.put("warehousePath", tempDir.resolve("warehouse2").toString());
    
    // Get catalogs
    Catalog catalog1 = IcebergCatalogManager.getCatalogForProvider("hadoop", config1);
    Catalog catalog2 = IcebergCatalogManager.getCatalogForProvider("hadoop", config2);
    
    assertNotNull(catalog1);
    assertNotNull(catalog2);
    
    // Get same catalog again - should be cached
    Catalog catalog1Again = IcebergCatalogManager.getCatalogForProvider("hadoop", config1);
    assertEquals(catalog1, catalog1Again);
  }

  @Test
  public void testRestCatalogCreation() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "rest");
    config.put("uri", "http://localhost:8181");
    
    // Just test that we can create the catalog object without connecting
    try {
      Catalog catalog = IcebergCatalogManager.getCatalogForProvider("rest", config);
      assertNotNull(catalog);
      // Don't test actual connectivity since there's no real REST server
    } catch (Exception e) {
      // Expected - REST catalog creation may fail without a real server
      assertTrue(e.getMessage().contains("Connection refused") || 
                 e.getMessage().contains("refused") ||
                 e.getCause() instanceof java.net.ConnectException);
    }
  }

  @Test
  public void testInvalidCatalogType() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "invalid");
    
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      IcebergCatalogManager.getCatalogForProvider("invalid", config);
    });
    
    assertTrue(exception.getMessage().contains("Unknown catalog type"));
  }

  @Test
  public void testMissingWarehousePathForHadoop() {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    // Missing warehousePath
    
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      IcebergCatalogManager.getCatalogForProvider("hadoop", config);
    });
    
    assertTrue(exception.getMessage().contains("warehouse"));
  }

  @Test
  @Disabled("Requires actual Iceberg table infrastructure")
  public void testParseTablePath() throws Exception {
    // Setup catalog
    String warehousePath = tempDir.resolve("warehouse").toString();
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    Catalog catalog = IcebergCatalogManager.getCatalogForProvider("hadoop", config);
    
    // Create tables in different namespaces
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );
    
    // Single level table
    catalog.createTable(TableIdentifier.of("simple_table"), schema);
    
    // Namespace.table format
    catalog.createTable(TableIdentifier.of("ns1", "table1"), schema);
    
    // Load tables with different path formats
    config.put("tablePath", "simple_table");
    Table table1 = IcebergCatalogManager.loadTable(config, "simple_table");
    assertNotNull(table1);
    
    config.put("tablePath", "ns1.table1");
    Table table2 = IcebergCatalogManager.loadTable(config, "ns1.table1");
    assertNotNull(table2);
  }
}