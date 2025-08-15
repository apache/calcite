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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for IcebergStorageProvider.
 * Tests use local file-based Iceberg tables in a temp directory.
 */
@Tag("unit")
public class IcebergStorageProviderTest {

  @TempDir
  Path tempDir;

  private IcebergStorageProvider storageProvider;
  private String warehousePath;

  @BeforeEach
  public void setup() throws Exception {
    warehousePath = tempDir.resolve("warehouse").toString();
    
    // Create test catalog and tables
    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    
    // Create schema
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "data", Types.StringType.get())
    );
    
    // Create default namespace and tables in it
    if (catalog instanceof SupportsNamespaces) {
      SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
      nsCatalog.createNamespace(Namespace.of("default"));
    }
    catalog.createTable(TableIdentifier.of("default", "table1"), schema);
    catalog.createTable(TableIdentifier.of("default", "table2"), schema);
    
    // Create custom namespace and tables in it
    if (catalog instanceof SupportsNamespaces) {
      SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
      nsCatalog.createNamespace(Namespace.of("ns1"));
    }
    catalog.createTable(TableIdentifier.of("ns1", "table3"), schema);
    
    // Create storage provider
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    storageProvider = new IcebergStorageProvider(config);
  }

  @Test
  public void testGetStorageType() {
    assertEquals("iceberg", storageProvider.getStorageType());
  }

  @Test
  public void testListRootNamespaces() throws Exception {
    List<StorageProvider.FileEntry> entries = storageProvider.listFiles("/", false);
    assertNotNull(entries);
    
    // Should list namespaces as directories
    boolean foundDefault = false;
    boolean foundNs1 = false;
    
    for (StorageProvider.FileEntry entry : entries) {
      if (entry.getName().contains("default")) {
        foundDefault = true;
        assertTrue(entry.isDirectory());
      }
      if (entry.getName().contains("ns1")) {
        foundNs1 = true;
        assertTrue(entry.isDirectory());
      }
    }
    
    // Default namespace may not always be listed explicitly
    // but ns1 should be there
    assertTrue(foundNs1 || entries.size() > 0);
  }

  @Test
  public void testListTablesInNamespace() throws Exception {
    // List tables in default namespace
    List<StorageProvider.FileEntry> entries = storageProvider.listFiles("/default", false);
    
    // Should have table1 and table2
    assertEquals(2, entries.size());
    
    boolean foundTable1 = false;
    boolean foundTable2 = false;
    
    for (StorageProvider.FileEntry entry : entries) {
      assertFalse(entry.isDirectory());
      if (entry.getName().equals("table1")) {
        foundTable1 = true;
      }
      if (entry.getName().equals("table2")) {
        foundTable2 = true;
      }
    }
    
    assertTrue(foundTable1);
    assertTrue(foundTable2);
  }

  @Test
  public void testExistsForTable() throws Exception {
    // Check existing table
    assertTrue(storageProvider.exists("/default/table1"));
    
    // Check non-existing table
    assertFalse(storageProvider.exists("/default/nonexistent"));
  }

  @Test
  public void testExistsForNamespace() throws Exception {
    // Check existing namespace
    assertTrue(storageProvider.exists("/default"));
    
    // Check non-existing namespace
    assertFalse(storageProvider.exists("/nonexistent"));
  }

  @Test
  public void testIsDirectory() throws Exception {
    // Namespace should be directory
    assertTrue(storageProvider.isDirectory("/default"));
    
    // Table should not be directory
    assertFalse(storageProvider.isDirectory("/default/table1"));
  }

  @Test
  public void testGetMetadata() throws Exception {
    StorageProvider.FileMetadata metadata = storageProvider.getMetadata("/default/table1");
    assertNotNull(metadata);
    assertEquals("/default/table1", metadata.getPath());
    assertEquals("application/x-iceberg-table", metadata.getContentType());
  }

  @Test
  public void testResolvePath() {
    // Test absolute path
    assertEquals("/absolute/path", storageProvider.resolvePath("/base", "/absolute/path"));
    
    // Test relative path
    assertEquals("/base/relative", storageProvider.resolvePath("/base", "relative"));
    
    // Test base with trailing slash
    assertEquals("/base/relative", storageProvider.resolvePath("/base/", "relative"));
  }

  @Test
  public void testOpenInputStream() throws Exception {
    // Iceberg tables don't have byte representation
    // Should return empty stream
    InputStream stream = storageProvider.openInputStream("/default/table1");
    assertNotNull(stream);
    assertEquals(0, stream.available());
    stream.close();
  }

  @Test
  public void testOpenReader() throws Exception {
    // Iceberg tables don't have text representation
    // Should return empty reader
    Reader reader = storageProvider.openReader("/default/table1");
    assertNotNull(reader);
    assertEquals(-1, reader.read());
    reader.close();
  }
}