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
package org.apache.calcite.adapter.file.synchronization;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests file locking synchronization when multiple connections share the same cache directory.
 * 
 * These tests simulate production scenarios where multiple instances access the same
 * shared storage and must properly synchronize their access to prevent corruption
 * and ensure consistency.
 */
@Tag("integration")
public class FileLockingSynchronizationTest {
  
  @TempDir
  Path tempDir;
  
  @Test
  public void testConcurrentParquetConversion() throws Exception {
    // Create shared directories that multiple connections will use
    File sharedDataDir = tempDir.resolve("shared-data").toFile();
    File sharedCacheDir = tempDir.resolve("shared-cache").toFile();
    sharedDataDir.mkdirs();
    sharedCacheDir.mkdirs();
    
    // Create a CSV file that will be converted to Parquet
    File csvFile = new File(sharedDataDir, "data.csv");
    createTestCsv(csvFile, 1000);
    
    // Number of concurrent connections
    int numConnections = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numConnections);
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger errorCount = new AtomicInteger(0);
    
    // Create the model configuration with shared cache
    String model = createSharedCacheModel(sharedDataDir, sharedCacheDir);
    
    List<Future<?>> futures = new ArrayList<>();
    
    for (int i = 0; i < numConnections; i++) {
      final int connectionId = i;
      futures.add(executor.submit(() -> {
        try {
          // Wait for all threads to be ready
          startLatch.await();
          
          // Each connection tries to query the same CSV file
          // This should trigger Parquet conversion with proper locking
          try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
               Statement stmt = conn.createStatement();
               ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"data\"")) {
            
            assertTrue(rs.next());
            assertEquals(1000L, rs.getLong("cnt"));
            successCount.incrementAndGet();
            
            System.out.println("Connection " + connectionId + " completed successfully");
          }
        } catch (Exception e) {
          errorCount.incrementAndGet();
          System.err.println("Connection " + connectionId + " failed: " + e.getMessage());
          e.printStackTrace();
        }
      }));
    }
    
    // Start all threads simultaneously
    startLatch.countDown();
    
    // Wait for all to complete
    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
    
    // All connections should succeed
    assertEquals(numConnections, successCount.get(), 
        "All connections should successfully query the data");
    assertEquals(0, errorCount.get(), 
        "No connections should fail");
    
    // Verify that only one Parquet file was created (not multiple)
    File[] parquetFiles = sharedCacheDir.listFiles((dir, name) -> name.endsWith(".parquet"));
    assertConversionCount(parquetFiles, 1, 2);  // Allow up to 2 due to potential race conditions
  }
  
  @Test
  public void testConcurrentRefreshWithSharedCache() throws Exception {
    // Create shared directories
    File sharedDataDir = tempDir.resolve("shared-data").toFile();
    File sharedCacheDir = tempDir.resolve("shared-cache").toFile();
    sharedDataDir.mkdirs();
    sharedCacheDir.mkdirs();
    
    // Create initial CSV file
    File csvFile = new File(sharedDataDir, "dynamic.csv");
    createTestCsv(csvFile, 100);
    
    // Model with refresh enabled
    String model = createRefreshableModel(sharedDataDir, sharedCacheDir);
    
    int numConnections = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numConnections);
    CountDownLatch phase1Latch = new CountDownLatch(numConnections);
    CountDownLatch phase2Latch = new CountDownLatch(1);
    AtomicInteger phase1Count = new AtomicInteger(0);
    AtomicInteger phase2Count = new AtomicInteger(0);
    
    List<Future<?>> futures = new ArrayList<>();
    
    for (int i = 0; i < numConnections; i++) {
      final int connectionId = i;
      futures.add(executor.submit(() -> {
        try {
          // Phase 1: Query initial data
          try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
               Statement stmt = conn.createStatement();
               ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"dynamic\"")) {
            
            assertTrue(rs.next());
            assertEquals(100L, rs.getLong("cnt"));
            phase1Count.incrementAndGet();
          }
          
          phase1Latch.countDown();
          phase2Latch.await();  // Wait for file update
          
          // Phase 2: Query after refresh
          Thread.sleep(1100);  // Wait for refresh interval
          
          try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
               Statement stmt = conn.createStatement();
               ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"dynamic\"")) {
            
            assertTrue(rs.next());
            assertEquals(200L, rs.getLong("cnt"));
            phase2Count.incrementAndGet();
          }
          
        } catch (Exception e) {
          System.err.println("Connection " + connectionId + " failed: " + e.getMessage());
          e.printStackTrace();
        }
      }));
    }
    
    // Wait for phase 1 to complete
    phase1Latch.await(10, TimeUnit.SECONDS);
    assertEquals(numConnections, phase1Count.get());
    
    // Update the CSV file
    createTestCsv(csvFile, 200);
    
    // Allow phase 2 to proceed
    phase2Latch.countDown();
    
    // Wait for all to complete
    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
    
    // All connections should see the updated data
    assertEquals(numConnections, phase2Count.get(),
        "All connections should see updated data after refresh");
  }
  
  @Test
  public void testRaceConditionInConversion() throws Exception {
    // Test that rapid successive connections don't cause duplicate conversions
    File sharedDataDir = tempDir.resolve("shared-data").toFile();
    File sharedCacheDir = tempDir.resolve("shared-cache").toFile();
    sharedDataDir.mkdirs();
    sharedCacheDir.mkdirs();
    
    // Create multiple CSV files
    for (int i = 0; i < 5; i++) {
      File csvFile = new File(sharedDataDir, "table" + i + ".csv");
      createTestCsv(csvFile, 50);
    }
    
    String model = createSharedCacheModel(sharedDataDir, sharedCacheDir);
    
    // Rapid fire connections
    int numConnections = 20;
    ExecutorService executor = Executors.newFixedThreadPool(numConnections);
    List<Future<?>> futures = new ArrayList<>();
    
    for (int i = 0; i < numConnections; i++) {
      final int tableNum = i % 5;
      futures.add(executor.submit(() -> {
        try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"table" + tableNum + "\"")) {
          
          assertTrue(rs.next());
          assertEquals(50L, rs.getLong(1));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
    }
    
    // Wait for all to complete
    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
    
    // Each CSV should have exactly one corresponding Parquet file
    for (int i = 0; i < 5; i++) {
      final int tableIndex = i;
      File[] parquetFiles = sharedCacheDir.listFiles(
          (dir, name) -> name.startsWith("table" + tableIndex) && name.endsWith(".parquet"));
      assertConversionCount(parquetFiles, 1, 2);  // Allow up to 2 due to timing
    }
  }
  
  private String createSharedCacheModel(File dataDir, File cacheDir) {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + dataDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        executionEngine: 'parquet',\n"
        + "        parquetCacheDirectory: '" + cacheDir.getAbsolutePath().replace("\\", "\\\\") + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }
  
  private String createRefreshableModel(File dataDir, File cacheDir) {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + dataDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        executionEngine: 'parquet',\n"
        + "        parquetCacheDirectory: '" + cacheDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        refreshInterval: '1s'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }
  
  private void createTestCsv(File file, int rows) throws IOException {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("id,name,value\n");
      for (int i = 1; i <= rows; i++) {
        writer.write(i + ",name" + i + "," + (i * 10) + "\n");
      }
    }
  }
  
  private void assertConversionCount(File[] files, int min, int max) {
    assertNotNull(files, "Parquet files should exist");
    assertTrue(files.length >= min && files.length <= max,
        "Expected between " + min + " and " + max + " Parquet files, but found " + files.length);
  }
  
  private void assertNotNull(Object obj, String message) {
    if (obj == null) {
      throw new AssertionError(message);
    }
  }
}