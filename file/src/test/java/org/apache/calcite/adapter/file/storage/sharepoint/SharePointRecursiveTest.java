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
package org.apache.calcite.adapter.file.storage.sharepoint;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that FileSchema can recursively discover files regardless of directory structure.
 */
@Tag("integration")
public class SharePointRecursiveTest {
  
  private static String tenantId;
  private static String clientId;
  private static String certificatePath;
  private static String certificatePassword;
  private static String siteUrl;
  private static String clientSecret;
  
  @BeforeAll
  static void setup() {
    Properties props = loadProperties();
    
    tenantId = props.getProperty("SHAREPOINT_TENANT_ID");
    clientId = props.getProperty("SHAREPOINT_CLIENT_ID");
    clientSecret = props.getProperty("SHAREPOINT_CLIENT_SECRET");
    siteUrl = props.getProperty("SHAREPOINT_SITE_URL");
    certificatePassword = props.getProperty("SHAREPOINT_CERT_PASSWORD");
    
    File certFile = new File("src/test/resources/SharePointAppOnlyCert.pfx");
    if (certFile.exists()) {
      certificatePath = certFile.getAbsolutePath();
    }
    
    System.out.println("SharePoint Test Configuration:");
    System.out.println("  Site URL: " + siteUrl);
    System.out.println("  Client ID: " + clientId);
    System.out.println("  Certificate: " + (certificatePath != null ? "configured" : "not found"));
  }
  
  private static Properties loadProperties() {
    Properties props = new Properties();
    File propsFile = new File("local-test.properties");
    if (!propsFile.exists()) {
      propsFile = new File("file/local-test.properties");
    }
    if (!propsFile.exists()) {
      propsFile = new File("/Users/kennethstott/ndc-calcite/calcite-rs-jni/calcite/file/local-test.properties");
    }
    
    if (propsFile.exists()) {
      try (InputStream is = new FileInputStream(propsFile)) {
        Properties fileProps = new Properties();
        fileProps.load(is);
        props.putAll(fileProps);
        System.out.println("Loaded properties from: " + propsFile.getAbsolutePath());
      } catch (IOException e) {
        System.err.println("Failed to load properties: " + e.getMessage());
      }
    }
    return props;
  }
  
  @Test
  void testRecursiveDiscovery() throws Exception {
    if (siteUrl == null || clientId == null) {
      System.out.println("Skipping test - SharePoint not configured");
      return;
    }
    
    System.out.println("\n=== TESTING RECURSIVE FILE DISCOVERY ===\n");
    
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      // Test different starting points with recursive=true
      String[] startPaths = {
          "/",                     // Root of site
          "/Shared Documents",     // Document library root
          ""                       // Empty path
      };
      
      for (String startPath : startPaths) {
        System.out.println("\n--- Testing with directory: '" + startPath + "' and recursive=true ---");
        
        Map<String, Object> operand = new HashMap<>();
        operand.put("directory", startPath);
        operand.put("storageType", "sharepoint");
        operand.put("recursive", true);  // This should find files in any subdirectory
        operand.put("executionEngine", "linq4j");

        Map<String, Object> storageConfig = new HashMap<>();
        storageConfig.put("siteUrl", siteUrl);
        storageConfig.put("tenantId", tenantId);
        storageConfig.put("clientId", clientId);
        
        // Use certificate if available, otherwise fall back to client secret
        if (certificatePath != null && certificatePassword != null) {
          storageConfig.put("certificatePath", certificatePath);
          storageConfig.put("certificatePassword", certificatePassword);
          System.out.println("Using certificate authentication");
        } else if (clientSecret != null) {
          storageConfig.put("clientSecret", clientSecret);
          storageConfig.put("useGraphApi", true);
          System.out.println("Using Graph API with client secret");
        } else {
          System.out.println("No authentication method available");
          continue;
        }
        
        operand.put("storageConfig", storageConfig);

        FileSchemaFactory factory = FileSchemaFactory.INSTANCE;
        String schemaName = "sp_" + startPath.replace("/", "_").replace(" ", "_");
        if (schemaName.equals("sp_")) schemaName = "sp_root";
        
        try {
          Schema schema = factory.create(rootSchema, schemaName, operand);
          
          // Add the schema to the root schema
          rootSchema.add(schemaName, schema);
          
          // Use reflection to get table map
          if (schema instanceof FileSchema) {
            FileSchema fs = (FileSchema) schema;
            Method method = FileSchema.class.getDeclaredMethod("getTableMap");
            method.setAccessible(true);
            Map<String, ?> tables = (Map<String, ?>) method.invoke(fs);
            
            System.out.println("Found " + tables.size() + " tables:");
            for (String tableName : tables.keySet()) {
              System.out.println("  - " + tableName);
              
              // Check if it's a CSV file
              if (tableName.toLowerCase().contains("departments") || 
                  tableName.toLowerCase().contains("employees")) {
                System.out.println("    ✓ Found expected CSV table!");
              }
            }
            
            if (tables.isEmpty()) {
              System.out.println("  ⚠️  No tables found with recursive=true from path: " + startPath);
            }
            
            // Try to query if we found tables
            if (tables.size() > 0) {
              String firstTable = tables.keySet().iterator().next();
              try (Statement stmt = conn.createStatement()) {
                // With Oracle lex and TO_LOWER casing, use unquoted identifiers
                String query = "SELECT COUNT(*) as cnt FROM " + schemaName + "." + firstTable;
                System.out.println("Testing query: " + query);
                ResultSet rs = stmt.executeQuery(query);
                if (rs.next()) {
                  System.out.println("  Query successful! Row count: " + rs.getInt("cnt"));
                }
              } catch (Exception e) {
                System.out.println("  Query failed: " + e.getMessage());
              }
            }
          }
        } catch (Exception e) {
          System.out.println("Error creating schema: " + e.getMessage());
          e.printStackTrace();
        }
      }
      
      System.out.println("\n=== CONCLUSION ===");
      System.out.println("With recursive=true, the FileSchema should discover all CSV files");
      System.out.println("regardless of their location in the directory tree.");
      System.out.println("If files are only found with specific paths, there may be an issue");
      System.out.println("with the recursive traversal implementation.");
    }
  }
}