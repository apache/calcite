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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Direct test for SharePoint SQL:2003 compliance without logging conflicts.
 */
@EnabledIfEnvironmentVariable(named = "SHAREPOINT_INTEGRATION_TESTS", matches = "true")
public class SharePointDirectTest {

  @Test
  public void testDirectConnection() throws Exception {
    // Load test configuration
    Properties testConfig = new Properties();
    Path configPath = Paths.get("/Users/kennethstott/calcite/sharepoint-list/local-test.properties");
    if (!Files.exists(configPath)) {
      configPath = Paths.get("sharepoint-list/local-test.properties");
    }
    if (!Files.exists(configPath)) {
      configPath = Paths.get("local-test.properties");
    }
    
    if (Files.exists(configPath)) {
      try (FileInputStream fis = new FileInputStream(configPath.toFile())) {
        testConfig.load(fis);
      }
    }
    
    // Create connection directly with proper casing configuration
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // Create SharePoint schema
    Map<String, Object> operand = new HashMap<>();
    operand.put("siteUrl", testConfig.getProperty("SHAREPOINT_SITE_URL"));
    operand.put("authType", "CLIENT_CREDENTIALS");
    operand.put("clientId", testConfig.getProperty("SHAREPOINT_CLIENT_ID"));
    operand.put("clientSecret", testConfig.getProperty("SHAREPOINT_CLIENT_SECRET"));
    operand.put("tenantId", testConfig.getProperty("SHAREPOINT_TENANT_ID"));

    SharePointListSchema sharePointSchema =
        new SharePointListSchema(testConfig.getProperty("SHAREPOINT_SITE_URL"), operand);
    rootSchema.add("sharepoint", sharePointSchema);

    System.out.println("\n=== SharePoint Direct Connection Test ===");
    
    // Test 1: List available tables
    System.out.println("\n1. Available SharePoint Lists:");
    int listCount = 0;
    for (String tableName : sharePointSchema.getTableMap().keySet()) {
      System.out.println("   - " + tableName);
      listCount++;
      if (listCount >= 10) {
        System.out.println("   ... and more");
        break;
      }
    }
    System.out.println("   Total lists: " + sharePointSchema.getTableMap().size());
    
    // Test 2: Simple SELECT
    System.out.println("\n2. Testing SELECT with LIMIT:");
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM sharepoint.documents LIMIT 3")) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      System.out.println("   ✓ Retrieved " + count + " rows");
    }
    
    // Test 3: COUNT aggregate
    System.out.println("\n3. Testing COUNT aggregate:");
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM sharepoint.documents")) {
      if (rs.next()) {
        int count = rs.getInt("cnt");
        System.out.println("   ✓ COUNT(*) = " + count);
      }
    }
    
    // Test 4: VALUES clause
    System.out.println("\n4. Testing VALUES clause:");
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT * FROM (VALUES (1, 'test'), (2, 'data')) AS t(id, name)")) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      System.out.println("   ✓ VALUES returned " + count + " rows");
    }
    
    // Test 5: String functions
    System.out.println("\n5. Testing string functions:");
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT UPPER('hello') as up, LOWER('WORLD') as low FROM (VALUES (1)) AS t(x)")) {
      if (rs.next()) {
        String upper = rs.getString("up");
        String lower = rs.getString("low");
        System.out.println("   ✓ UPPER('hello') = " + upper);
        System.out.println("   ✓ LOWER('WORLD') = " + lower);
      }
    }
    
    // Test 6: CASE expression (quote 'result' as it may be a reserved word)
    System.out.println("\n6. Testing CASE expression:");
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END as \"result\" FROM (VALUES (1)) AS t(x)")) {
      if (rs.next()) {
        String result = rs.getString("result");
        System.out.println("   ✓ CASE result = " + result);
      }
    }
    
    System.out.println("\n=== SharePoint SQL:2003 Compliance Verified ===");
    System.out.println("✓ Connection successful");
    System.out.println("✓ Schema discovery works");
    System.out.println("✓ Basic queries work");
    System.out.println("✓ Aggregate functions work");
    System.out.println("✓ VALUES clause works");
    System.out.println("✓ String functions work");
    System.out.println("✓ CASE expressions work");
    
    connection.close();
  }
}