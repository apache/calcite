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
package org.apache.calcite.test;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

/**
 * Performance tests for Splunk adapter.
 * These tests measure query performance, memory usage, and concurrent access.
 */
@Tag("performance")
class SplunkPerformanceTest {
  private static String SPLUNK_URL = null;
  private static String SPLUNK_USER = null;
  private static String SPLUNK_PASSWORD = null;
  private static boolean DISABLE_SSL_VALIDATION = false;
  private static boolean PROPERTIES_LOADED = false;

  @BeforeAll
  static void loadConnectionProperties() {
    File[] possibleLocations = {
        new File("local-properties.settings"),
        new File("splunk/local-properties.settings"),
        new File("../splunk/local-properties.settings")
    };

    File propsFile = null;
    for (File location : possibleLocations) {
      if (location.exists()) {
        propsFile = location;
        break;
      }
    }

    if (propsFile != null) {
      Properties props = new Properties();
      try (FileInputStream fis = new FileInputStream(propsFile)) {
        props.load(fis);
        
        if (props.containsKey("splunk.url")) {
          SPLUNK_URL = props.getProperty("splunk.url");
        }
        if (props.containsKey("splunk.username")) {
          SPLUNK_USER = props.getProperty("splunk.username");
        }
        if (props.containsKey("splunk.password")) {
          SPLUNK_PASSWORD = props.getProperty("splunk.password");
        }
        if (props.containsKey("splunk.ssl.insecure")) {
          DISABLE_SSL_VALIDATION = Boolean.parseBoolean(props.getProperty("splunk.ssl.insecure"));
        }
        
        PROPERTIES_LOADED = true;
        System.out.println("Loaded Splunk connection for performance tests from " + propsFile.getPath());
      } catch (IOException e) {
        throw new RuntimeException("Failed to load local-properties.settings: " + e.getMessage(), e);
      }
    } else {
      throw new IllegalStateException(
          "Performance tests require local-properties.settings with Splunk credentials. " +
          "Create the file with splunk.url, splunk.username, and splunk.password properties.");
    }
  }

  private void validateConfiguration() {
    if (!PROPERTIES_LOADED || SPLUNK_URL == null || SPLUNK_USER == null || SPLUNK_PASSWORD == null) {
      throw new IllegalStateException(
          "Splunk connection not configured. Check local-properties.settings file.");
    }
  }

  private Connection createConnection() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    if (DISABLE_SSL_VALIDATION) {
      info.put("disableSslValidation", "true");
    }
    return DriverManager.getConnection("jdbc:splunk:", info);
  }

  @Test void testSimpleQueryPerformance() throws SQLException {
    validateConfiguration();
    
    try (Connection connection = createConnection();
         Statement statement = connection.createStatement()) {
      
      // Warm up
      statement.executeQuery("SELECT * FROM splunk.splunk_audit LIMIT 1");
      
      // Measure query time
      Instant start = Instant.now();
      ResultSet rs = statement.executeQuery("SELECT * FROM splunk.splunk_audit LIMIT 100");
      
      int count = 0;
      while (rs.next()) {
        count++;
      }
      
      Duration duration = Duration.between(start, Instant.now());
      System.out.println("Query returned " + count + " rows in " + duration.toMillis() + "ms");
      
      // Assert performance threshold
      assertThat("Query should complete within 5 seconds", 
          duration.getSeconds(), lessThan(5L));
    }
  }

  @Test void testLargeResultSetPerformance() throws SQLException {
    validateConfiguration();
    
    try (Connection connection = createConnection();
         Statement statement = connection.createStatement()) {
      
      // Test fetching larger result set
      Instant start = Instant.now();
      ResultSet rs = statement.executeQuery("SELECT * FROM splunk.splunk_audit LIMIT 1000");
      
      int count = 0;
      while (rs.next()) {
        count++;
      }
      rs.close();
      
      Duration duration = Duration.between(start, Instant.now());
      double rowsPerSecond = count / (duration.toMillis() / 1000.0);
      
      System.out.println("Fetched " + count + " rows in " + duration.toMillis() + "ms");
      System.out.println("Throughput: " + String.format("%.2f", rowsPerSecond) + " rows/second");
      
      // Assert minimum throughput
      assertThat("Should process at least 100 rows per second", 
          rowsPerSecond > 100.0, is(true));
    }
  }

  @Test void testConcurrentConnectionPerformance() throws Exception {
    validateConfiguration();
    
    int concurrentConnections = 5;
    ExecutorService executor = Executors.newFixedThreadPool(concurrentConnections);
    
    Instant start = Instant.now();
    
    CompletableFuture<?>[] futures = new CompletableFuture[concurrentConnections];
    for (int i = 0; i < concurrentConnections; i++) {
      final int threadId = i;
      futures[i] = CompletableFuture.runAsync(() -> {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
          
          ResultSet rs = stmt.executeQuery("SELECT * FROM splunk.splunk_audit LIMIT 10");
          int count = 0;
          while (rs.next()) {
            count++;
          }
          System.out.println("Thread " + threadId + " fetched " + count + " rows");
          
        } catch (SQLException e) {
          throw new RuntimeException("Thread " + threadId + " failed", e);
        }
      }, executor);
    }
    
    CompletableFuture.allOf(futures).get(30, TimeUnit.SECONDS);
    
    Duration duration = Duration.between(start, Instant.now());
    executor.shutdown();
    
    System.out.println("Concurrent test with " + concurrentConnections + 
        " connections completed in " + duration.toMillis() + "ms");
    
    // Assert reasonable concurrent performance
    assertThat("Concurrent queries should complete within 10 seconds", 
        duration.getSeconds(), lessThan(10L));
  }

  @Test void testMemoryEfficiencyWithLargeResults() throws SQLException {
    validateConfiguration();
    
    Runtime runtime = Runtime.getRuntime();
    runtime.gc(); // Suggest GC before measurement
    long memoryBefore = runtime.totalMemory() - runtime.freeMemory();
    
    try (Connection connection = createConnection();
         Statement statement = connection.createStatement()) {
      
      statement.setFetchSize(100); // Use reasonable fetch size
      
      ResultSet rs = statement.executeQuery("SELECT * FROM splunk.web LIMIT 5000");
      
      int count = 0;
      while (rs.next()) {
        count++;
        // Process row without storing
        rs.getString(1);
      }
      
      runtime.gc(); // Suggest GC after processing
      long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
      long memoryUsed = (memoryAfter - memoryBefore) / (1024 * 1024); // Convert to MB
      
      System.out.println("Processed " + count + " rows");
      System.out.println("Memory used: ~" + memoryUsed + " MB");
      
      // Assert reasonable memory usage (less than 100MB for 5000 rows)
      assertThat("Memory usage should be reasonable", memoryUsed < 100, is(true));
    }
  }

  @Test void testQueryOptimizationWithProjection() throws SQLException {
    validateConfiguration();
    
    try (Connection connection = createConnection();
         Statement statement = connection.createStatement()) {
      
      // Test 1: Full row fetch
      Instant start1 = Instant.now();
      ResultSet rs1 = statement.executeQuery("SELECT * FROM splunk.web LIMIT 100");
      while (rs1.next()) {
        // Process
      }
      Duration fullFetchDuration = Duration.between(start1, Instant.now());
      
      // Test 2: Projected columns only
      Instant start2 = Instant.now();
      ResultSet rs2 = statement.executeQuery("SELECT status, action FROM splunk.web LIMIT 100");
      while (rs2.next()) {
        // Process
      }
      Duration projectedDuration = Duration.between(start2, Instant.now());
      
      System.out.println("Full fetch: " + fullFetchDuration.toMillis() + "ms");
      System.out.println("Projected fetch: " + projectedDuration.toMillis() + "ms");
      
      // Projected query should be faster or at least not significantly slower
      double improvement = (fullFetchDuration.toMillis() - projectedDuration.toMillis()) 
          / (double) fullFetchDuration.toMillis();
      System.out.println("Projection improvement: " + 
          String.format("%.2f%%", improvement * 100));
    }
  }
}