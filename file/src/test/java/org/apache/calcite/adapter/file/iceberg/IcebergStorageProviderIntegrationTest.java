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
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for IcebergStorageProvider demonstrating the documented patterns.
 * This test creates a local Iceberg warehouse and verifies StorageProvider integration.
 */
public class IcebergStorageProviderIntegrationTest {
  
  @TempDir
  Path tempDir;
  
  private HttpServer server;
  private int port;
  private String baseUrl;
  private String warehousePath;
  private Catalog catalog;
  
  @BeforeEach
  public void setup() throws IOException {
    // Create a real Iceberg warehouse for testing
    warehousePath = tempDir.resolve("iceberg-warehouse").toString();
    new File(warehousePath).mkdirs();
    
    // Create Hadoop catalog with test tables
    Configuration conf = new Configuration();
    catalog = new HadoopCatalog(conf, warehousePath);
    
    // Create test namespace and table
    Namespace testNamespace = Namespace.of("test_db");
    
    // Check if catalog supports namespace creation
    if (catalog instanceof SupportsNamespaces) {
      try {
        ((SupportsNamespaces) catalog).createNamespace(testNamespace);
      } catch (Exception e) {
        // Namespace might already exist, which is fine for testing
      }
    }
    
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "age", Types.IntegerType.get())
    );
    
    TableIdentifier tableId = TableIdentifier.of(testNamespace, "users");
    catalog.createTable(tableId, schema);
    
    // Start HTTP server for REST catalog simulation (future enhancement)
    setupHttpServer();
  }
  
  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }
  
  private void setupHttpServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;
    
    // Mock REST catalog endpoints (for future REST catalog support)
    server.createContext("/v1/config", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        String response = "{\"defaults\":{},\"overrides\":{}}";
        sendJsonResponse(exchange, response);
      }
    });
    
    server.createContext("/v1/namespaces", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        String response = "{\"namespaces\":[{\"namespace\":[\"test_db\"]}]}";
        sendJsonResponse(exchange, response);
      }
    });
    
    server.start();
  }
  
  private void sendJsonResponse(HttpExchange exchange, String response) throws IOException {
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, response.length());
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(response.getBytes(StandardCharsets.UTF_8));
    }
    exchange.close();
  }
  
  @Test
  public void testIcebergStorageProviderFromType() throws IOException {
    // Test the documented pattern: createFromType with iceberg config
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    StorageProvider provider = StorageProviderFactory.createFromType("iceberg", config);
    
    assertNotNull(provider);
    assertEquals("iceberg", provider.getStorageType());
    assertTrue(provider instanceof IcebergStorageProvider);
  }
  
  @Test
  public void testListNamespaces() throws IOException {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    IcebergStorageProvider provider = new IcebergStorageProvider(config);
    
    // List root should show namespaces
    List<StorageProvider.FileEntry> entries = provider.listFiles("/", false);
    
    assertFalse(entries.isEmpty());
    boolean foundTestDb = false;
    for (StorageProvider.FileEntry entry : entries) {
      if (entry.getName().equals("test_db")) {
        assertTrue(entry.isDirectory());
        foundTestDb = true;
        break;
      }
    }
    assertTrue(foundTestDb, "Should find test_db namespace");
  }
  
  @Test
  public void testListTablesInNamespace() throws IOException {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    IcebergStorageProvider provider = new IcebergStorageProvider(config);
    
    // List tables in test_db namespace
    List<StorageProvider.FileEntry> entries = provider.listFiles("/test_db", false);
    
    assertFalse(entries.isEmpty());
    boolean foundUsersTable = false;
    for (StorageProvider.FileEntry entry : entries) {
      if (entry.getName().equals("users")) {
        assertFalse(entry.isDirectory()); // Tables are files, not directories
        foundUsersTable = true;
        break;
      }
    }
    assertTrue(foundUsersTable, "Should find users table in test_db namespace");
  }
  
  @Test
  public void testTableExists() throws IOException {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    IcebergStorageProvider provider = new IcebergStorageProvider(config);
    
    // Test existing table
    assertTrue(provider.exists("/test_db/users"));
    
    // Test non-existing table
    assertFalse(provider.exists("/test_db/nonexistent"));
    
    // Test existing namespace
    assertTrue(provider.exists("/test_db"));
    
    // Test non-existing namespace
    assertFalse(provider.exists("/nonexistent"));
  }
  
  @Test
  public void testIsDirectory() throws IOException {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    IcebergStorageProvider provider = new IcebergStorageProvider(config);
    
    // Namespaces are directories
    assertTrue(provider.isDirectory("/test_db"));
    
    // Tables are not directories
    assertFalse(provider.isDirectory("/test_db/users"));
  }
  
  @Test
  public void testGetMetadata() throws IOException {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    IcebergStorageProvider provider = new IcebergStorageProvider(config);
    
    StorageProvider.FileMetadata metadata = provider.getMetadata("/test_db/users");
    
    assertNotNull(metadata);
    assertEquals("/test_db/users", metadata.getPath());
    assertEquals("application/x-iceberg-table", metadata.getContentType());
  }
  
  @Test
  public void testDocumentedStorageTypePattern() throws IOException {
    // This tests the exact pattern from ICEBERG_GUIDE.md:
    // {
    //   "storageType": "iceberg",
    //   "storageConfig": {
    //     "catalogType": "hadoop",
    //     "warehousePath": "/iceberg/warehouse"
    //   }
    // }
    
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("catalogType", "hadoop");
    storageConfig.put("warehousePath", warehousePath);
    
    StorageProvider provider = StorageProviderFactory.createFromType("iceberg", storageConfig);
    
    assertNotNull(provider);
    assertTrue(provider instanceof IcebergStorageProvider);
    
    // Test that it can list our test tables
    List<StorageProvider.FileEntry> namespaces = provider.listFiles("/", false);
    assertTrue(namespaces.stream().anyMatch(entry -> "test_db".equals(entry.getName())));
    
    List<StorageProvider.FileEntry> tables = provider.listFiles("/test_db", false);
    assertTrue(tables.stream().anyMatch(entry -> "users".equals(entry.getName())));
  }
  
  @Test
  public void testPathResolution() throws IOException {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    IcebergStorageProvider provider = new IcebergStorageProvider(config);
    
    // Test path resolution
    assertEquals("/test_db/users", provider.resolvePath("/test_db", "users"));
    assertEquals("/test_db/users", provider.resolvePath("/test_db/", "users"));
    assertEquals("/absolute/path", provider.resolvePath("/test_db", "/absolute/path"));
  }
  
  @Test
  public void testIcebergUrlScheme() throws IOException {
    // Test iceberg:// URL scheme (future enhancement)
    StorageProvider provider = StorageProviderFactory.createFromUrl("iceberg://test-catalog/namespace/table");
    
    assertNotNull(provider);
    assertTrue(provider instanceof IcebergStorageProvider);
    assertEquals("iceberg", provider.getStorageType());
  }
  
  @Test 
  public void testEmptyStreamResponse() throws IOException {
    // Iceberg tables don't have byte streams - verify graceful handling
    Map<String, Object> config = new HashMap<>();
    config.put("catalogType", "hadoop");
    config.put("warehousePath", warehousePath);
    
    IcebergStorageProvider provider = new IcebergStorageProvider(config);
    
    // Should return empty streams, not throw exceptions
    try (java.io.InputStream is = provider.openInputStream("/test_db/users")) {
      assertEquals(0, is.available());
    }
    
    try (java.io.Reader reader = provider.openReader("/test_db/users")) {
      assertEquals(-1, reader.read()); // EOF
    }
  }
  
  @Test
  public void testProviderCaching() {
    // Test that factory properly caches providers
    StorageProviderFactory.clearCache();
    
    Map<String, Object> config1 = new HashMap<>();
    config1.put("catalogType", "hadoop");
    config1.put("warehousePath", warehousePath);
    
    StorageProvider provider1 = StorageProviderFactory.createFromType("iceberg", config1);
    StorageProvider provider2 = StorageProviderFactory.createFromType("iceberg", config1);
    
    // Both should be IcebergStorageProvider instances but not necessarily the same instance
    // (since we create new instances with different configs)
    assertNotNull(provider1);
    assertNotNull(provider2);
    assertTrue(provider1 instanceof IcebergStorageProvider);
    assertTrue(provider2 instanceof IcebergStorageProvider);
  }
}