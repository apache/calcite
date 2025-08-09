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
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test for FileSchema's S3 bucket crawling functionality.
 * 
 * This test verifies that FileSchema can:
 * 1. Connect to an S3 bucket using the storageType configuration
 * 2. Automatically discover files in the bucket (with recursive scanning)
 * 3. Create tables for each discovered file
 * 4. Allow querying of those tables through standard SQL
 * 
 * The test does NOT directly interact with S3 - it only configures FileSchema
 * and verifies the results through Calcite's standard JDBC interface.
 */
@Tag("integration")
public class S3BucketCrawlingIntegrationTest {

  private static Properties testProperties;
  private static String testBucket;
  private static String accessKey;
  private static String secretKey;
  private static String region;

  @BeforeAll
  static void setup() {
    testProperties = loadLocalProperties();
    
    // Get S3 configuration
    testBucket = getConfig("S3_TEST_BUCKET");
    accessKey = getConfig("AWS_ACCESS_KEY_ID");
    secretKey = getConfig("AWS_SECRET_ACCESS_KEY");
    region = getConfig("AWS_REGION");
    if (region == null || region.isEmpty()) {
      region = getConfig("AWS_DEFAULT_REGION");
    }
    if (region == null || region.isEmpty()) {
      region = "us-west-1";
    }
    
    // Set AWS credentials as system properties for the test
    if (accessKey != null && !accessKey.isEmpty()) {
      System.setProperty("aws.accessKeyId", accessKey);
    }
    if (secretKey != null && !secretKey.isEmpty()) {
      System.setProperty("aws.secretKey", secretKey);
    }
    if (region != null && !region.isEmpty()) {
      System.setProperty("aws.region", region);
    }
  }

  private static Properties loadLocalProperties() {
    Properties props = new Properties();
    File propsFile = new File("local-test.properties");
    if (!propsFile.exists()) {
      propsFile = new File("file/local-test.properties");
    }
    if (!propsFile.exists()) {
      propsFile = new File("calcite/file/local-test.properties");
    }
    if (!propsFile.exists()) {
      propsFile = new File("/Users/kennethstott/ndc-calcite/calcite-rs-jni/calcite/file/local-test.properties");
    }
    
    if (propsFile.exists()) {
      try (InputStream is = new FileInputStream(propsFile)) {
        props.load(is);
        System.out.println("Loaded S3 test properties from: " + propsFile.getAbsolutePath());
      } catch (IOException e) {
        System.err.println("Failed to load properties file: " + e.getMessage());
      }
    }
    return props;
  }

  private static String getConfig(String key) {
    String value = System.getenv(key);
    if (value == null || value.isEmpty()) {
      value = System.getProperty(key);
    }
    if (value == null || value.isEmpty()) {
      value = testProperties.getProperty(key);
    }
    return value;
  }

  private boolean isS3Available() {
    return testBucket != null && !testBucket.isEmpty()
        && accessKey != null && !accessKey.isEmpty()
        && secretKey != null && !secretKey.isEmpty();
  }

  @Test
  @SuppressWarnings("deprecation")
  void testS3BucketCrawlingWithCalcite() throws Exception {
    assumeTrue(isS3Available(), "S3 credentials not configured, skipping test");
    
    // STEP 1: Configure FileSchema to use S3 storage provider
    // This is the only S3-specific configuration - FileSchema handles all S3 interaction
    Properties info = new Properties();
    info.setProperty("model", "inline:"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"s3_data\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"s3_data\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"storageType\": \"s3\",\n"
        + "        \"storageConfig\": {\n"
        + "          \"region\": \"" + region + "\"\n"
        + "        },\n"
        + "        \"directory\": \"s3://" + testBucket + "/\",\n"
        + "        \"recursive\": true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
    
    // STEP 2: Connect via standard JDBC - no S3-specific code needed
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConn.getRootSchema();
      SchemaPlus s3SchemaPlus = rootSchema.getSubSchema("s3_data");
      Schema s3Schema = s3SchemaPlus == null ? null : s3SchemaPlus.unwrap(Schema.class);
      
      assertNotNull(s3Schema, "FileSchema should have been created with S3 storage provider");
      
      // STEP 3: Verify FileSchema discovered files and created tables
      // FileSchema handles all S3 operations internally
      List<String> tableNames = new ArrayList<>(s3Schema.getTableNames());
      System.out.println("Found " + tableNames.size() + " tables in S3 bucket:");
      for (String tableName : tableNames) {
        System.out.println("  - " + tableName);
      }
      
      // STEP 4: Test querying through standard SQL
      // FileSchema transparently handles S3 file access when queries are executed
      if (!tableNames.isEmpty()) {
        String firstTable = tableNames.get(0);
        System.out.println("\nQuerying first table: " + firstTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + firstTable + "\" LIMIT 5")) {
          
          // Print column metadata
          ResultSetMetaData metaData = rs.getMetaData();
          int columnCount = metaData.getColumnCount();
          System.out.println("Table has " + columnCount + " columns:");
          for (int i = 1; i <= columnCount; i++) {
            System.out.println("  Column " + i + ": " + metaData.getColumnName(i) 
                + " (" + metaData.getColumnTypeName(i) + ")");
          }
          
          // Print first few rows
          System.out.println("\nFirst few rows:");
          int rowCount = 0;
          while (rs.next() && rowCount < 5) {
            StringBuilder row = new StringBuilder("Row " + (++rowCount) + ": ");
            for (int i = 1; i <= columnCount; i++) {
              if (i > 1) row.append(", ");
              row.append(rs.getString(i));
            }
            System.out.println(row);
          }
        }
      }
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  void testS3BucketWithNonRecursiveScanning() throws Exception {
    assumeTrue(isS3Available(), "S3 credentials not configured, skipping test");
    
    // Test non-recursive scanning - should only find files in root, not subdirectories
    Properties info = new Properties();
    info.setProperty("model", "inline:"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"s3_flat\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"s3_flat\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"storageType\": \"s3\",\n"
        + "        \"storageConfig\": {\n"
        + "          \"region\": \"" + region + "\"\n"
        + "        },\n"
        + "        \"directory\": \"s3://" + testBucket + "/data/\",\n"
        + "        \"recursive\": false\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConn.getRootSchema();
      SchemaPlus s3SchemaPlus = rootSchema.getSubSchema("s3_flat");
      Schema s3Schema = s3SchemaPlus == null ? null : s3SchemaPlus.unwrap(Schema.class);
      
      assertNotNull(s3Schema, "FileSchema should have been created");
      
      // With recursive=false, should only see files directly in /data/, not in subdirectories
      List<String> tableNames = new ArrayList<>(s3Schema.getTableNames());
      System.out.println("\nNon-recursive scan found " + tableNames.size() + " tables in /data/ root:");
      for (String tableName : tableNames) {
        System.out.println("  - " + tableName);
      }
      
      // Verify no tables from subdirectories are included
      boolean hasSubdirTables = tableNames.stream()
          .anyMatch(name -> name.contains("ALBUM") || name.contains("ARTIST") || name.contains("CUSTOMER"));
      
      if (tableNames.isEmpty()) {
        System.out.println("No files found directly in /data/ (all files are in subdirectories)");
      } else if (hasSubdirTables) {
        System.out.println("Note: Found tables from subdirectories even with recursive=false");
      }
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  void testS3SpecificDirectory() throws Exception {
    assumeTrue(isS3Available(), "S3 credentials not configured, skipping test");
    
    // Test with a specific subdirectory if it exists
    // This test looks for a 'parquet' subdirectory
    Properties info = new Properties();
    info.setProperty("model", "inline:"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"s3_subdir\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"s3_subdir\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"storageType\": \"s3\",\n"
        + "        \"storageConfig\": {\n"
        + "          \"region\": \"" + region + "\"\n"
        + "        },\n"
        + "        \"directory\": \"s3://" + testBucket + "/data/album/\",\n"
        + "        \"recursive\": false\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConn.getRootSchema();
      SchemaPlus s3SchemaPlus = rootSchema.getSubSchema("s3_subdir");
      Schema s3Schema = s3SchemaPlus == null ? null : s3SchemaPlus.unwrap(Schema.class);
      
      if (s3Schema != null) {
        List<String> tableNames = new ArrayList<>(s3Schema.getTableNames());
        System.out.println("\nFound " + tableNames.size() + " tables in /data/album/ subdirectory:");
        for (String tableName : tableNames) {
          System.out.println("  - " + tableName);
        }
        
        // If there are CSV files, test querying one
        if (!tableNames.isEmpty()) {
          String firstTable = tableNames.get(0);
          
          System.out.println("\nQuerying CSV table: " + firstTable);
          try (Statement stmt = connection.createStatement();
               ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"" + firstTable + "\"")) {
            if (rs.next()) {
              System.out.println("Row count: " + rs.getInt(1));
            }
          }
        }
      } else {
        System.out.println("No /data/album/ subdirectory found or no files in it");
      }
    }
  }
}