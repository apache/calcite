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
package org.apache.calcite.adapter.file.debug;

import org.apache.calcite.adapter.file.FileAdapterTests;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * Debug test to trace timestamp handling through the parquet engine.
 */
@Tag("unit")
public class TimestampDebugTest {

  @Test void debugTimestampHandling() throws Exception {
    System.out.println("\n=== TIMESTAMP DEBUG TEST ===");
    System.out.println("JVM Timezone: " + java.util.TimeZone.getDefault().getID());
    
    // First, test with linq4j engine to see expected value
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug-linq4j"));
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery(
          "SELECT \"jointimes\" FROM \"date\" WHERE \"empno\" = 100");
      rs.next();
      
      Timestamp ts = rs.getTimestamp(1);
      long linq4jMillis = ts.getTime();
      System.out.println("\nLINQ4J Engine:");
      System.out.println("  Timestamp object: " + ts);
      System.out.println("  Milliseconds: " + linq4jMillis);
      System.out.println("  Expected: 838958462000");
      System.out.println("  Difference: " + (linq4jMillis - 838958462000L) + " ms");
    }
    
    // Now test with parquet engine
    info.put("model", FileAdapterTests.jsonPath("bug-parquet"));
    
    // Clear cache first
    java.io.File cacheDir = new java.io.File("build/test-cache/bug-parquet");
    if (cacheDir.exists()) {
      for (java.io.File file : cacheDir.listFiles()) {
        file.delete();
      }
    }
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      Statement statement = connection.createStatement();
      
      // First query triggers conversion
      ResultSet rs = statement.executeQuery(
          "SELECT \"jointimes\" FROM \"date\" WHERE \"empno\" = 100");
      rs.next();
      
      Timestamp ts = rs.getTimestamp(1);
      long parquetMillis = ts.getTime();
      System.out.println("\nPARQUET Engine (after conversion):");
      System.out.println("  Timestamp object: " + ts);
      System.out.println("  Timestamp class: " + ts.getClass().getName());
      System.out.println("  Milliseconds: " + parquetMillis);
      System.out.println("  Expected: 838958462000");
      System.out.println("  Difference: " + (parquetMillis - 838958462000L) + " ms");
      
      if (parquetMillis - 838958462000L != 0) {
        long diffHours = (parquetMillis - 838958462000L) / 3600000;
        System.out.println("  Difference in hours: " + diffHours);
      }
    }
    
    // Also check what getString returns
    info.put("model", FileAdapterTests.jsonPath("bug-parquet"));
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery(
          "SELECT \"jointimes\" FROM \"date\" WHERE \"empno\" = 100");
      rs.next();
      
      String strValue = rs.getString(1);
      System.out.println("\nPARQUET Engine getString:");
      System.out.println("  String value: " + strValue);
    }
    
    System.out.println("\n=== END DEBUG TEST ===\n");
  }
}