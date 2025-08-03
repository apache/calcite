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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.util.Sources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Splunk adapter using model JSON file.
 * Run with: -Dcalcite.test.splunk=true
 */
@EnabledIfSystemProperty(named = "calcite.test.splunk", matches = "true")
class SplunkModelTest {
  
  @Test
  void testWithModelFile() throws Exception {
    System.out.println("\n=== Testing with Model File ===");
    
    Properties info = new Properties();
    info.put("model", Sources.of(SplunkModelTest.class.getResource("/test-splunk-model.json")).file().getAbsolutePath());
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      DatabaseMetaData metaData = calciteConn.getMetaData();
      
      System.out.println("\nSchemas:");
      try (ResultSet rs = metaData.getSchemas()) {
        while (rs.next()) {
          String schemaName = rs.getString("TABLE_SCHEM");
          System.out.println("  - " + schemaName);
        }
      }
      
      System.out.println("\nTables in 'splunk' schema:");
      int count = 0;
      
      try (ResultSet rs = metaData.getTables(null, "splunk", "%", null)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          System.out.println("  - " + tableName);
          count++;
        }
      }
      
      System.out.println("\nTotal tables discovered: " + count);
      assertTrue(count > 0, "Should discover some tables");
    }
  }
}