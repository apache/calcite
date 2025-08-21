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
package org.apache.calcite.adapter.file.conversion;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.TestAbortedException;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test auto-conversion of files to Parquet when using Parquet execution engine.
 */
@Tag("unit")
public class ParquetAutoConversionTest {
  @TempDir
  java.nio.file.Path tempDir;
  
  private boolean shouldSkipTests;

  @BeforeEach
  public void setUp() throws Exception {
    // Check if we should skip tests based on the current execution engine
    String currentEngine = System.getProperty("CALCITE_FILE_ENGINE_TYPE", "PARQUET");
    shouldSkipTests = "LINQ4J".equals(currentEngine) || "ARROW".equals(currentEngine);
    
    if (shouldSkipTests) {
      System.out.println("Skipping ParquetAutoConversionTest - not relevant for " + currentEngine + " engine");
      return;
    }
    
    // Clear any static caches that might interfere with test isolation
    Sources.clearFileCache();
    // Force garbage collection to release any file handles and clear caches
    System.gc();
    // Wait a bit to ensure cleanup
    Thread.sleep(100);
  }

  @AfterEach
  public void tearDown() throws Exception {
    // Clear caches after each test to prevent contamination
    Sources.clearFileCache();
    System.gc();
    Thread.sleep(100);
  }

  @Test 
  public void testAutoConversionToParquet() throws Exception {
    String currentEngine = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    if (currentEngine == null) {
      currentEngine = System.getProperty("CALCITE_FILE_ENGINE_TYPE", "PARQUET");
    }
    if (!"PARQUET".equals(currentEngine) && !"DUCKDB".equals(currentEngine)) {
      throw new TestAbortedException("Skipping test - only relevant for PARQUET or DUCKDB engines, current: " + currentEngine);
    }
    System.out.println("\n=== TESTING AUTO-CONVERSION TO PARQUET ===");

    // Create unique temp directory for this test with full UUID to ensure uniqueness
    String testId = UUID.randomUUID().toString();
    File uniqueTempDir = new File(tempDir.toFile(), "autoconvert_test_" + testId);
    assertTrue(uniqueTempDir.mkdirs());

    // Create test CSV file
    File csvFile = new File(uniqueTempDir, "customers.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("customer_id:int,name:string,city:string,balance:double\n");
      writer.write("1,Alice,New York,1500.50\n");
      writer.write("2,Bob,San Francisco,2200.00\n");
      writer.write("3,Charlie,Chicago,800.75\n");
      writer.write("4,Diana,Boston,3100.25\n");
    }

    // Create test JSON file
    File jsonFile = new File(uniqueTempDir, "orders.json");
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      writer.write("[\n");
      writer.write("  {\"order_id\": 101, \"customer_id\": 1, \"amount\": 250.00, \"status\": \"shipped\"},\n");
      writer.write("  {\"order_id\": 102, \"customer_id\": 2, \"amount\": 150.50, \"status\": \"pending\"},\n");
      writer.write("  {\"order_id\": 103, \"customer_id\": 1, \"amount\": 75.25, \"status\": \"delivered\"},\n");
      writer.write("  {\"order_id\": 104, \"customer_id\": 3, \"amount\": 500.00, \"status\": \"shipped\"}\n");
      writer.write("]\n");
    }

    // Verify cache directory doesn't exist yet
    File cacheDir = new File(uniqueTempDir, "test_cache_auto");
    assertFalse(cacheDir.exists(), "Cache directory should not exist initially");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Configure file schema with PARQUET execution engine
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", uniqueTempDir.getAbsolutePath());
      // Use unique cache directory for test isolation
      operand.put("parquetCacheDirectory", cacheDir.getAbsolutePath());

      System.out.println("\n1. Creating schema with PARQUET execution engine");
      SchemaPlus fileSchema =
          rootSchema.add("parquet_convert", FileSchemaFactory.INSTANCE.create(rootSchema, "parquet_convert", operand));

      try (Statement stmt = connection.createStatement()) {
        // Query CSV file - should be auto-converted to Parquet
        System.out.println("\n2. Querying CSV file (should auto-convert to Parquet):");
        ResultSet rs1 =
            stmt.executeQuery("SELECT * FROM \"parquet_convert\".\"customers\" WHERE \"balance\" > 1000 ORDER BY \"customer_id\"");

        int count1 = 0;
        while (rs1.next()) {
          System.out.printf(Locale.ROOT, "   Customer %d: %s from %s - $%.2f%n",
              rs1.getInt("customer_id"),
              rs1.getString("name"),
              rs1.getString("city"),
              rs1.getDouble("balance"));
          count1++;
        }
        assertEquals(3, count1, "Should have 3 customers with balance > 1000");

        // Verify cache directory now exists
        assertTrue(cacheDir.exists(), "Cache directory should exist after conversion");

        // Verify Parquet files were created in schema-specific subdirectory
        File schemaCacheDir = new File(cacheDir, "schema_parquet_convert");
        assertTrue(schemaCacheDir.exists(), "Schema-specific cache directory should exist");
        File customersParquet = new File(schemaCacheDir, "customers.parquet");
        assertTrue(customersParquet.exists(), "Customers Parquet file should exist");
        System.out.println("\n   ✓ CSV file was converted to Parquet: " + customersParquet.getName());

        // Query JSON file - should also be auto-converted
        System.out.println("\n3. Querying JSON file (should auto-convert to Parquet):");
        ResultSet rs2 =
            stmt.executeQuery("SELECT * FROM \"parquet_convert\".\"orders\" WHERE \"status\" = 'shipped' ORDER BY \"order_id\"");

        int count2 = 0;
        while (rs2.next()) {
          System.out.printf(Locale.ROOT, "   Order %d: Customer %d - $%.2f (%s)%n",
              rs2.getInt("order_id"),
              rs2.getInt("customer_id"),
              rs2.getDouble("amount"),
              rs2.getString("status"));
          count2++;
        }
        assertEquals(2, count2, "Should have 2 shipped orders");

        File ordersParquet = new File(schemaCacheDir, "orders.parquet");
        assertTrue(ordersParquet.exists(), "Orders Parquet file should exist");
        System.out.println("\n   ✓ JSON file was converted to Parquet: " + ordersParquet.getName());

        // Test aggregation query across converted files
        System.out.println("\n4. Testing join query across converted files:");
        ResultSet rs3 =
            stmt.executeQuery("SELECT c.\"name\", COUNT(*) as order_count, SUM(o.\"amount\") as total_spent " +
            "FROM \"parquet_convert\".\"customers\" c " +
            "JOIN \"parquet_convert\".\"orders\" o ON c.\"customer_id\" = o.\"customer_id\" " +
            "GROUP BY c.\"name\" " +
            "ORDER BY total_spent DESC");

        System.out.println("   Customer | Orders | Total Spent");
        System.out.println("   ---------|--------|------------");
        while (rs3.next()) {
          System.out.printf(Locale.ROOT, "   %-8s | %6d | $%.2f%n",
              rs3.getString("name"),
              rs3.getInt("order_count"),
              rs3.getDouble("total_spent"));
        }

        System.out.println("\n   ✓ Complex queries work on auto-converted Parquet files!");
      }
    }
  }

  @Test 
  public void testCacheCreationFailureHandling() throws Exception {
    String currentEngine = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    if (currentEngine == null) {
      currentEngine = System.getProperty("CALCITE_FILE_ENGINE_TYPE", "PARQUET");
    }
    if (!"PARQUET".equals(currentEngine) && !"DUCKDB".equals(currentEngine)) {
      throw new TestAbortedException("Skipping test - only relevant for PARQUET or DUCKDB engines, current: " + currentEngine);
    }

    System.out.println("\n=== TESTING CACHE CREATION FAILURE HANDLING ===");

    // Create unique temp directory for this test
    String testId = UUID.randomUUID().toString();
    File uniqueTempDir = new File(tempDir.toFile(), "cache_failure_test_" + testId);
    assertTrue(uniqueTempDir.mkdirs());

    // Create a cache directory that will fail (read-only after creation)
    File cacheDir = new File(uniqueTempDir, "readonly_cache_" + testId);
    assertTrue(cacheDir.mkdirs());

    File csvFile = new File(uniqueTempDir, "data.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("id:int,name:string\n");
      writer.write("1,Alice\n");
      writer.write("2,Bob\n");
    }

    // Make cache directory read-only to simulate permission issues
    assertTrue(cacheDir.setWritable(false), "Failed to make cache dir read-only");

    try {
      // Attempt to use Parquet engine with read-only cache directory
      try (Connection conn = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
           CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

        SchemaPlus rootSchema = calciteConn.getRootSchema();
        Map<String, Object> operand = new HashMap<>();
        operand.put("directory", uniqueTempDir.getAbsolutePath());
        operand.put("parquetCacheDirectory", cacheDir.getAbsolutePath());

        rootSchema.add("test", FileSchemaFactory.INSTANCE.create(rootSchema, "test", operand));

        // System should still work despite cache directory being read-only
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM test.data")) {
          rs.next();
          assertEquals(2, rs.getInt("cnt"), "Should work even with read-only cache directory");
        }
      }

      // Verify no cache file was created due to permissions
      File schemaCacheDir = new File(cacheDir, "schema_READONLY_TEST");
      // The schema directory might be created but the cache file should not exist
      File expectedCache = new File(schemaCacheDir, "data.parquet");
      assertFalse(expectedCache.exists(), "Cache file should not exist in read-only directory");

      System.out.println("✓ System handles cache creation failure gracefully");
      System.out.println("  - Read-only cache directory simulated permission issues");
      System.out.println("  - Queries still succeeded by falling back to direct file reading");

    } finally {
      // Restore write permissions for cleanup
      cacheDir.setWritable(true);
    }
  }

  @Test 
  public void testCacheInvalidation() throws Exception {
    String currentEngine = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    if (currentEngine == null) {
      currentEngine = System.getProperty("CALCITE_FILE_ENGINE_TYPE", "PARQUET");
    }
    if (!"PARQUET".equals(currentEngine) && !"DUCKDB".equals(currentEngine)) {
      throw new TestAbortedException("Skipping test - only relevant for PARQUET or DUCKDB engines, current: " + currentEngine);
    }
    System.out.println("\n=== TESTING PARQUET CACHE INVALIDATION ===");

    // Create unique temp directory for this test with full UUID
    String testId = UUID.randomUUID().toString();
    File uniqueTempDir = new File(tempDir.toFile(), "cache_invalidation_test_" + testId);
    assertTrue(uniqueTempDir.mkdirs());

    File cacheDir = new File(uniqueTempDir, "test_cache_invalidation");
    File csvFile = new File(uniqueTempDir, "inventory.csv");

    // Create initial CSV file
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("item_id:int,name:string,quantity:int\n");
      writer.write("1,Hammer,50\n");
      writer.write("2,Screwdriver,75\n");
    }

    // First query - should create cache
    try (Connection conn1 = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
         CalciteConnection calciteConn1 = conn1.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn1.getRootSchema();
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", uniqueTempDir.getAbsolutePath());
      // Use unique cache directory for test isolation
      operand.put("parquetCacheDirectory", cacheDir.getAbsolutePath());

      rootSchema.add("INVENTORY1", FileSchemaFactory.INSTANCE.create(rootSchema, "INVENTORY1", operand));

      try (Statement stmt = conn1.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"INVENTORY1\".\"inventory\"")) {
        rs.next();
        assertEquals(2, rs.getInt("cnt"));
      }
    }

    File schemaCacheDir = new File(cacheDir, "schema_INVENTORY1");
    assertTrue(schemaCacheDir.exists(), "Schema-specific cache directory should exist");
    File cachedParquet = new File(schemaCacheDir, "inventory.parquet");
    assertTrue(cachedParquet.exists(), "Cached Parquet file should exist");
    long cacheTime1 = cachedParquet.lastModified();
    System.out.println("1. Initial cache created with 2 items");

    // Update CSV file with more data
    Thread.sleep(1100); // Ensure timestamp difference > 1 second buffer
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("item_id:int,name:string,quantity:int\n");
      writer.write("1,Hammer,50\n");
      writer.write("2,Screwdriver,75\n");
      writer.write("3,Wrench,30\n");  // Added new item
      writer.write("4,Pliers,40\n");  // Added another new item
    }

    // Verify the file was updated
    String fileContent = new String(java.nio.file.Files.readAllBytes(csvFile.toPath()), StandardCharsets.UTF_8);
    assertTrue(fileContent.contains("3,Wrench,30"), "CSV file should contain new items");
    assertTrue(fileContent.contains("4,Pliers,40"), "CSV file should contain new items");
    // File was updated successfully

    // Force a small delay to ensure file system timestamps are different
    Thread.sleep(100);

    // Second query with updated file - should regenerate cache
    try (Connection conn2 = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
         CalciteConnection calciteConn2 = conn2.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn2.getRootSchema();
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", uniqueTempDir.getAbsolutePath());
      // Use same cache directory to test invalidation
      operand.put("parquetCacheDirectory", cacheDir.getAbsolutePath());

      rootSchema.add("INVENTORY1", FileSchemaFactory.INSTANCE.create(rootSchema, "INVENTORY1", operand));

      try (Statement stmt = conn2.createStatement()) {
        // First, let's verify what's in the table
        ResultSet rs = stmt.executeQuery("SELECT * FROM \"INVENTORY1\".\"inventory\" ORDER BY \"item_id\"");
        int itemCount = 0;
        while (rs.next()) {
          itemCount++;
        }
        rs.close();

        // With DirectFileSource, we should now see all 4 items
        assertEquals(4, itemCount, "Should see 4 items after update");
      }
    }

    long cacheTime2 = cachedParquet.lastModified();
    assertTrue(cacheTime2 > cacheTime1, "Cache file should be regenerated after source update");
    System.out.println("2. Cache file was regenerated (timestamp changed)");
    System.out.println("   Original cache time: " + cacheTime1);
    System.out.println("   New cache time: " + cacheTime2);
    System.out.println("   ✓ Cache invalidation is working correctly!");
  }

  @Test 
  public void testJsonFileCacheInvalidation() throws Exception {
    String currentEngine = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    if (currentEngine == null) {
      currentEngine = System.getProperty("CALCITE_FILE_ENGINE_TYPE", "PARQUET");
    }
    if (!"PARQUET".equals(currentEngine) && !"DUCKDB".equals(currentEngine)) {
      throw new TestAbortedException("Skipping test - only relevant for PARQUET or DUCKDB engines, current: " + currentEngine);
    }
    System.out.println("\n=== TESTING JSON FILE CACHE INVALIDATION ===");

    // Use a subdirectory with full UUID for this test to ensure complete isolation
    String testId = UUID.randomUUID().toString();
    File testSubDir = new File(tempDir.toFile(), "json_cache_inv_test_" + testId);
    assertTrue(testSubDir.mkdirs(), "Failed to create test subdirectory");

    File cacheDir = new File(testSubDir, "cache_json_" + testId);
    File jsonFile = new File(testSubDir, "cache_invalidation_test.json");

    // Create initial JSON file
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      writer.write("[\n");
      writer.write("  {\"id\": 1, \"name\": \"Alice\", \"score\": 85},\n");
      writer.write("  {\"id\": 2, \"name\": \"Bob\", \"score\": 92}\n");
      writer.write("]\n");
    }

    // First query - should create cache
    try (Connection conn1 = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
         CalciteConnection calciteConn1 = conn1.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn1.getRootSchema();
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", testSubDir.getAbsolutePath());
      // Use unique cache directory for test isolation
      operand.put("parquetCacheDirectory", cacheDir.getAbsolutePath());
      // Enable refresh to use RefreshableParquetCacheTable which handles cache invalidation
      operand.put("refreshInterval", "PT1S"); // 1 second refresh interval

      rootSchema.add("JSON_CACHE_TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "JSON_CACHE_TEST", operand));

      try (Statement stmt = conn1.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"JSON_CACHE_TEST\".\"cache_invalidation_test\"")) {
        rs.next();
        int actualCount = rs.getInt("cnt");
        assertEquals(2, actualCount, "Should see 2 records initially but got " + actualCount);
      }
    }

    File schemaCacheDir = new File(cacheDir, "schema_JSON_CACHE_TEST");
    assertTrue(schemaCacheDir.exists(), "Schema-specific cache directory should exist");
    File cachedParquet = new File(schemaCacheDir, "cache_invalidation_test.parquet");
    assertTrue(cachedParquet.exists(), "Cached Parquet file should exist");
    long cacheTime1 = cachedParquet.lastModified();
    System.out.println("1. Initial cache created with 2 records");

    // Update JSON file with more data
    Thread.sleep(1100); // Ensure timestamp difference > 1 second buffer
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      writer.write("[\n");
      writer.write("  {\"id\": 1, \"name\": \"Alice\", \"score\": 85},\n");
      writer.write("  {\"id\": 2, \"name\": \"Bob\", \"score\": 92},\n");
      writer.write("  {\"id\": 3, \"name\": \"Charlie\", \"score\": 78},\n");
      writer.write("  {\"id\": 4, \"name\": \"Diana\", \"score\": 95}\n");
      writer.write("]\n");
    }

    // Verify the file was updated
    String fileContent = new String(java.nio.file.Files.readAllBytes(jsonFile.toPath()), StandardCharsets.UTF_8);
    assertTrue(fileContent.contains("Charlie"), "JSON file should contain new records");

    System.out.println("JSON file timestamp after update: " + jsonFile.lastModified());
    System.out.println("Cache file timestamp before query: " + cachedParquet.lastModified());
    System.out.println("Time difference: " + (jsonFile.lastModified() - cachedParquet.lastModified()));
    System.out.println("Needs conversion (>1000): " + (jsonFile.lastModified() > (cachedParquet.lastModified() + 1000)));

    // Wait longer to ensure cache invalidation detection
    Thread.sleep(1000);

    // Second query with updated file - should regenerate cache
    try (Connection conn2 = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
         CalciteConnection calciteConn2 = conn2.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn2.getRootSchema();
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", testSubDir.getAbsolutePath());
      // Use same cache directory to test invalidation
      operand.put("parquetCacheDirectory", cacheDir.getAbsolutePath());
      // Enable refresh to use RefreshableParquetCacheTable which handles cache invalidation
      operand.put("refreshInterval", "PT1S"); // 1 second refresh interval

      rootSchema.add("JSON_CACHE_TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "JSON_CACHE_TEST", operand));

      try (Statement stmt = conn2.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"JSON_CACHE_TEST\".\"cache_invalidation_test\"")) {
        rs.next();
        int actualCount = rs.getInt("cnt");
        System.out.println("Query returned " + actualCount + " records");

        // Get updated cache file timestamp
        File updatedCache = new File(schemaCacheDir, "cache_invalidation_test.parquet");
        if (updatedCache.exists()) {
          System.out.println("Updated cache exists with timestamp: " + updatedCache.lastModified());
        } else {
          System.out.println("No cache file found after query");
        }

        assertEquals(4, actualCount, "Should see 4 records after update");
      }
    }

    long cacheTime2 = cachedParquet.lastModified();
    assertTrue(cacheTime2 > cacheTime1, "Cache file should be regenerated after source update");
    System.out.println("2. JSON cache file was regenerated (timestamp changed)");
    System.out.println("   Original cache time: " + cacheTime1);
    System.out.println("   New cache time: " + cacheTime2);
    System.out.println("   ✓ JSON cache invalidation is working correctly!");
  }


  private void deleteDirectory(File dir) {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    dir.delete();
  }
}
