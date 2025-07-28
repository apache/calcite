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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test auto-conversion of files to Parquet when using Parquet execution engine.
 */
public class ParquetAutoConversionTest {
  @TempDir
  java.nio.file.Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    // Create test CSV file
    File csvFile = new File(tempDir.toFile(), "customers.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("customer_id:int,name:string,city:string,balance:double\n");
      writer.write("1,Alice,New York,1500.50\n");
      writer.write("2,Bob,San Francisco,2200.00\n");
      writer.write("3,Charlie,Chicago,800.75\n");
      writer.write("4,Diana,Boston,3100.25\n");
    }

    // Create test JSON file
    File jsonFile = new File(tempDir.toFile(), "orders.json");
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      writer.write("[\n");
      writer.write("  {\"order_id\": 101, \"customer_id\": 1, \"amount\": 250.00, \"status\": \"shipped\"},\n");
      writer.write("  {\"order_id\": 102, \"customer_id\": 2, \"amount\": 150.50, \"status\": \"pending\"},\n");
      writer.write("  {\"order_id\": 103, \"customer_id\": 1, \"amount\": 75.25, \"status\": \"delivered\"},\n");
      writer.write("  {\"order_id\": 104, \"customer_id\": 3, \"amount\": 500.00, \"status\": \"shipped\"}\n");
      writer.write("]\n");
    }
  }

  @Test public void testAutoConversionToParquet() throws Exception {
    System.out.println("\n=== TESTING AUTO-CONVERSION TO PARQUET ===");

    // Verify cache directory doesn't exist yet
    File cacheDir = new File(tempDir.toFile(), ".parquet_cache");
    assertFalse(cacheDir.exists(), "Cache directory should not exist initially");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Configure file schema with PARQUET execution engine
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");  // This triggers auto-conversion

      System.out.println("\n1. Creating schema with PARQUET execution engine");
      SchemaPlus fileSchema =
          rootSchema.add("PARQUET_CONVERT", FileSchemaFactory.INSTANCE.create(rootSchema, "PARQUET_CONVERT", operand));

      try (Statement stmt = connection.createStatement()) {
        // Query CSV file - should be auto-converted to Parquet
        System.out.println("\n2. Querying CSV file (should auto-convert to Parquet):");
        ResultSet rs1 =
            stmt.executeQuery("SELECT * FROM PARQUET_CONVERT.\"customers\" WHERE \"balance\" > 1000 ORDER BY \"customer_id\"");

        int count1 = 0;
        while (rs1.next()) {
          System.out.printf("   Customer %d: %s from %s - $%.2f%n",
              rs1.getInt("customer_id"),
              rs1.getString("name"),
              rs1.getString("city"),
              rs1.getDouble("balance"));
          count1++;
        }
        assertEquals(3, count1, "Should have 3 customers with balance > 1000");

        // Verify cache directory now exists
        assertTrue(cacheDir.exists(), "Cache directory should exist after conversion");

        // Verify Parquet files were created
        File customersParquet = new File(cacheDir, "customers.parquet");
        assertTrue(customersParquet.exists(), "Customers Parquet file should exist");
        System.out.println("\n   ✓ CSV file was converted to Parquet: " + customersParquet.getName());

        // Query JSON file - should also be auto-converted
        System.out.println("\n3. Querying JSON file (should auto-convert to Parquet):");
        ResultSet rs2 =
            stmt.executeQuery("SELECT * FROM PARQUET_CONVERT.\"orders\" WHERE \"status\" = 'shipped' ORDER BY \"order_id\"");

        int count2 = 0;
        while (rs2.next()) {
          System.out.printf("   Order %d: Customer %d - $%.2f (%s)%n",
              rs2.getInt("order_id"),
              rs2.getInt("customer_id"),
              rs2.getDouble("amount"),
              rs2.getString("status"));
          count2++;
        }
        assertEquals(2, count2, "Should have 2 shipped orders");

        File ordersParquet = new File(cacheDir, "orders.parquet");
        assertTrue(ordersParquet.exists(), "Orders Parquet file should exist");
        System.out.println("\n   ✓ JSON file was converted to Parquet: " + ordersParquet.getName());

        // Test aggregation query across converted files
        System.out.println("\n4. Testing join query across converted files:");
        ResultSet rs3 =
            stmt.executeQuery("SELECT c.\"name\", COUNT(*) as order_count, SUM(o.\"amount\") as total_spent " +
            "FROM PARQUET_CONVERT.\"customers\" c " +
            "JOIN PARQUET_CONVERT.\"orders\" o ON c.\"customer_id\" = o.\"customer_id\" " +
            "GROUP BY c.\"name\" " +
            "ORDER BY total_spent DESC");

        System.out.println("   Customer | Orders | Total Spent");
        System.out.println("   ---------|--------|------------");
        while (rs3.next()) {
          System.out.printf("   %-8s | %6d | $%.2f%n",
              rs3.getString("name"),
              rs3.getInt("order_count"),
              rs3.getDouble("total_spent"));
        }

        System.out.println("\n   ✓ Complex queries work on auto-converted Parquet files!");
      }
    }
  }

  @Test public void testCacheReuse() throws Exception {
    System.out.println("\n=== TESTING PARQUET CACHE REUSE ===");

    File cacheDir = new File(tempDir.toFile(), ".parquet_cache");
    File csvFile = new File(tempDir.toFile(), "products.csv");

    // Create initial CSV file
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("product_id:int,name:string,price:double\n");
      writer.write("1,Widget,25.50\n");
      writer.write("2,Gadget,50.00\n");
    }

    long originalTimestamp = csvFile.lastModified();

    // First query - should create cache
    try (Connection conn1 = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConn1 = conn1.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn1.getRootSchema();
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");

      rootSchema.add("TEST1", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST1", operand));

      try (Statement stmt = conn1.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM TEST1.\"products\"")) {
        rs.next();
        assertEquals(2, rs.getInt("cnt"));
      }
    }

    File cachedParquet = new File(cacheDir, "products.parquet");
    assertTrue(cachedParquet.exists(), "Cached Parquet file should exist");
    long cacheTime1 = cachedParquet.lastModified();

    System.out.println("1. Initial conversion created cache file");

    // Second query with unchanged file - should reuse cache
    Thread.sleep(100); // Ensure time difference

    try (Connection conn2 = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConn2 = conn2.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn2.getRootSchema();
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");

      rootSchema.add("TEST2", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST2", operand));

      try (Statement stmt = conn2.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM TEST2.\"products\"")) {
        rs.next();
        assertEquals(2, rs.getInt("cnt"));
      }
    }

    long cacheTime2 = cachedParquet.lastModified();
    assertEquals(cacheTime1, cacheTime2, "Cache file should not be regenerated for unchanged source");
    System.out.println("2. Second query reused existing cache (timestamps match)");

    // Update CSV file
    Thread.sleep(100); // Ensure time difference
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("product_id:int,name:string,price:double\n");
      writer.write("1,Widget,25.50\n");
      writer.write("2,Gadget,50.00\n");
      writer.write("3,Gizmo,75.00\n");  // Added new product
    }

    // Ensure the file timestamp is updated
    long newTime = System.currentTimeMillis() + 2000; // Add 2 seconds to ensure it's newer
    csvFile.setLastModified(newTime);

    // Third query with updated file - should regenerate cache
    Thread.sleep(100); // Ensure time difference

    // Verify the file was actually updated
    File updatedCsvFile = new File(tempDir.toFile(), "products.csv");
    assertTrue(updatedCsvFile.exists(), "Updated CSV file should exist");
    long updatedFileTime = updatedCsvFile.lastModified();
    System.out.println("3. Updated CSV file timestamp: " + updatedFileTime);

    try (Connection conn3 = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConn3 = conn3.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn3.getRootSchema();
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");

      rootSchema.add("TEST3", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST3", operand));

      try (Statement stmt = conn3.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM TEST3.\"products\"")) {
        rs.next();
        assertEquals(3, rs.getInt("cnt"), "Should see 3 products after update");
      }
    }

    long cacheTime3 = cachedParquet.lastModified();
    assertTrue(cacheTime3 > cacheTime2, "Cache file should be regenerated after source update");
    System.out.println("3. Updated CSV triggered cache regeneration");
    System.out.println("   Cache time 1: " + cacheTime1);
    System.out.println("   Cache time 2: " + cacheTime2 + " (should equal time 1)");
    System.out.println("   Cache time 3: " + cacheTime3 + " (should be > time 2)");
    System.out.println("   ✓ Cache reuse optimization is working correctly!");
  }
}
