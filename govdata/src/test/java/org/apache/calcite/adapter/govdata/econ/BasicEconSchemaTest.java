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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Basic test for ECON schema factory.
 */
@Tag("unit")
public class BasicEconSchemaTest {
  
  @BeforeAll
  public static void setup() {
    // Set system properties for test if environment variables are not set
    if (System.getenv("GOVDATA_CACHE_DIR") == null) {
      System.setProperty("GOVDATA_CACHE_DIR", "/tmp/govdata-test-cache");
    }
    if (System.getenv("GOVDATA_PARQUET_DIR") == null) {
      System.setProperty("GOVDATA_PARQUET_DIR", "/tmp/govdata-test-parquet");
    }
  }
  
  @Test
  public void testBasicSchemaCreation() throws Exception {
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      // Create ECON schema using factory
      Map<String, Object> operand = new HashMap<>();
      operand.put("autoDownload", false); // Don't download data in unit test
      operand.put("startYear", 2023);
      operand.put("endYear", 2024);
      
      EconSchemaFactory factory = new EconSchemaFactory();
      rootSchema.add("econ", factory.create(rootSchema, "econ", operand));
      
      // Check if we can query metadata
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet schemas = metaData.getSchemas()) {
        boolean foundEcon = false;
        while (schemas.next()) {
          if ("econ".equalsIgnoreCase(schemas.getString("TABLE_SCHEM"))) {
            foundEcon = true;
            break;
          }
        }
        assertTrue(foundEcon, "ECON schema should be present");
      }
    }
  }
  
  @Test
  public void testEconTableDefinitions() throws Exception {
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      // Create ECON schema
      Map<String, Object> operand = new HashMap<>();
      operand.put("autoDownload", false);
      operand.put("startYear", 2023);
      operand.put("endYear", 2024);
      
      EconSchemaFactory factory = new EconSchemaFactory();
      rootSchema.add("econ", factory.create(rootSchema, "econ", operand));
      
      // Check for expected tables
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet tables = metaData.getTables(null, "ECON", "%", null)) {
        int tableCount = 0;
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          System.out.println("Found ECON table: " + tableName);
          tableCount++;
        }
        // We expect at least some tables to be defined
        assertTrue(tableCount > 0, "ECON schema should have tables defined");
      }
    }
  }
  
  @Test
  public void testConstraintSupport() {
    EconSchemaFactory factory = new EconSchemaFactory();
    assertTrue(factory.supportsConstraints(), "ECON schema factory should support constraints");
  }
}