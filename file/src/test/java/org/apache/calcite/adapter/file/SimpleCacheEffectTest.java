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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Simple test to demonstrate cache effect measurements.
 */
public class SimpleCacheEffectTest {

  @TempDir
  File tempDir;

  @Test
  void measureSimpleCacheEffect() throws Exception {
    System.out.println("\n=== Simple Cache Effect Measurement ===\n");
    
    // Create a test CSV file
    File csvFile = new File(tempDir, "test_data.csv");
    try (PrintWriter writer = new PrintWriter(csvFile)) {
      writer.println("id,name,amount");
      for (int i = 1; i <= 10000; i++) {
        writer.println(i + ",Name" + i + "," + (i * 10));
      }
    }
    
    // Create connection with file schema
    Properties props = new Properties();
    props.put("model", "inline:"
        + "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      String query = "SELECT COUNT(*), SUM(\"amount\"), AVG(\"amount\") FROM \"test_data\"";
      
      // First query (cold cache)
      long coldStart = System.currentTimeMillis();
      int coldCount = executeQuery(conn, query);
      long coldTime = System.currentTimeMillis() - coldStart;
      
      // Second query (warm cache)
      long warmStart = System.currentTimeMillis();
      int warmCount = executeQuery(conn, query);
      long warmTime = System.currentTimeMillis() - warmStart;
      
      // Multiple warm queries for average
      long totalWarmTime = 0;
      for (int i = 0; i < 5; i++) {
        long start = System.currentTimeMillis();
        executeQuery(conn, query);
        totalWarmTime += System.currentTimeMillis() - start;
      }
      long avgWarmTime = totalWarmTime / 5;
      
      // Print results
      System.out.println("Results:");
      System.out.println("  Cold cache (1st query): " + coldTime + " ms");
      System.out.println("  Warm cache (2nd query): " + warmTime + " ms");
      System.out.println("  Average warm cache:     " + avgWarmTime + " ms");
      System.out.println("\nCache Effect:");
      System.out.println("  Loading overhead: " + (coldTime - warmTime) + " ms");
      if (warmTime > 0) {
        System.out.println("  Speedup: " + String.format("%.2fx", (double) coldTime / warmTime));
      }
      
      // Test with vectorized reading enabled
      System.setProperty("parquet.enable.vectorized.reader", "true");
      System.out.println("\n--- With Vectorized Reading ---");
      
      // Clear any in-memory cache by forcing GC
      System.gc();
      Thread.sleep(100);
      
      long vectorizedStart = System.currentTimeMillis();
      executeQuery(conn, query);
      long vectorizedTime = System.currentTimeMillis() - vectorizedStart;
      
      System.out.println("  Vectorized (warm): " + vectorizedTime + " ms");
      if (warmTime > 0 && vectorizedTime > 0) {
        System.out.println("  Vectorized improvement: " + 
            String.format("%.2fx", (double) warmTime / vectorizedTime));
      }
    }
  }
  
  private int executeQuery(Connection conn, String query) throws Exception {
    int count = 0;
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        count++;
      }
    }
    return count;
  }
}