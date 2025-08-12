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

import org.apache.calcite.adapter.file.storage.SharePointCertificateTokenManager;
import org.apache.calcite.adapter.file.storage.SharePointRestStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderSource;
import org.apache.calcite.util.Source;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct test for SharePoint file reading through storage provider.
 */
@Tag("integration")
public class SharePointDirectReadTest {
  
  private static String tenantId;
  private static String clientId;
  private static String certificatePath;
  private static String certificatePassword;
  private static String siteUrl;
  
  @BeforeAll
  static void setup() {
    Properties props = loadProperties();
    
    tenantId = props.getProperty("SHAREPOINT_TENANT_ID");
    clientId = props.getProperty("SHAREPOINT_CLIENT_ID");
    siteUrl = props.getProperty("SHAREPOINT_SITE_URL");
    certificatePassword = props.getProperty("SHAREPOINT_CERT_PASSWORD");
    
    // Check if certificate file exists
    File certFile = new File("src/test/resources/SharePointAppOnlyCert.pfx");
    if (certFile.exists()) {
      certificatePath = certFile.getAbsolutePath();
      System.out.println("Found certificate at: " + certificatePath);
    }
    
    if (tenantId == null || clientId == null || siteUrl == null || 
        certificatePath == null || certificatePassword == null) {
      System.out.println("SharePoint certificate authentication not fully configured.");
      return;
    }
    
    System.out.println("SharePoint Test Configuration:");
    System.out.println("  Site URL: " + siteUrl);
    System.out.println("  Client ID: " + clientId);
    System.out.println("  Tenant ID: " + tenantId);
    System.out.println("  Certificate: " + certificatePath);
  }
  
  private static Properties loadProperties() {
    Properties props = new Properties();
    
    // Load from properties file
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
        System.out.println("Loaded configuration from: " + propsFile.getAbsolutePath());
      } catch (IOException e) {
        System.err.println("Failed to load properties: " + e.getMessage());
      }
    }
    
    return props;
  }
  
  @Test
  void testDirectFileRead() throws Exception {
    if (certificatePath == null || certificatePassword == null) {
      System.out.println("Skipping test - certificate not configured");
      return;
    }
    
    System.out.println("\n=== Testing Direct File Read ===");
    
    // Create token manager with certificate
    SharePointCertificateTokenManager tokenManager = 
        new SharePointCertificateTokenManager(tenantId, clientId, 
            certificatePath, certificatePassword, siteUrl);
    
    // Create storage provider
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);
    
    // First list files to find a CSV
    System.out.println("Listing files in /Shared Documents/Shared Documents...");
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents/Shared Documents", false);
    
    assertNotNull(entries, "Should be able to list files");
    System.out.println("Found " + entries.size() + " files");
    
    StorageProvider.FileEntry csvFile = null;
    for (StorageProvider.FileEntry entry : entries) {
      if (!entry.isDirectory() && entry.getName().endsWith(".csv")) {
        csvFile = entry;
        System.out.println("Found CSV file: " + entry.getName() + " at path: " + entry.getPath());
        break;
      }
    }
    
    assertNotNull(csvFile, "Should find at least one CSV file");
    
    // Test 1: Read file using openInputStream
    System.out.println("\n--- Test 1: openInputStream ---");
    try (InputStream stream = provider.openInputStream(csvFile.getPath())) {
      assertNotNull(stream, "Stream should not be null");
      byte[] buffer = new byte[100];
      int bytesRead = stream.read(buffer);
      assertTrue(bytesRead > 0, "Should read some bytes");
      String content = new String(buffer, 0, bytesRead);
      System.out.println("First 100 bytes: " + content);
      assertTrue(content.contains(","), "CSV should contain commas");
    }
    
    // Test 2: Read file using openReader
    System.out.println("\n--- Test 2: openReader ---");
    try (Reader reader = provider.openReader(csvFile.getPath())) {
      assertNotNull(reader, "Reader should not be null");
      char[] buffer = new char[100];
      int charsRead = reader.read(buffer);
      assertTrue(charsRead > 0, "Should read some characters");
      String content = new String(buffer, 0, charsRead);
      System.out.println("First 100 chars: " + content);
      assertTrue(content.contains(","), "CSV should contain commas");
    }
    
    // Test 3: Create Source and read through it
    System.out.println("\n--- Test 3: StorageProviderSource ---");
    StorageProviderSource source = new StorageProviderSource(csvFile, provider);
    assertNotNull(source, "Source should not be null");
    assertEquals(csvFile.getPath(), source.path(), "Path should match");
    
    try (Reader reader = source.reader()) {
      assertNotNull(reader, "Reader from source should not be null");
      BufferedReader br = new BufferedReader(reader);
      String firstLine = br.readLine();
      assertNotNull(firstLine, "Should read first line");
      System.out.println("First line from Source: " + firstLine);
      assertTrue(firstLine.contains(","), "First line should contain commas");
      
      // Read a few more lines
      for (int i = 0; i < 3; i++) {
        String line = br.readLine();
        if (line != null) {
          System.out.println("Line " + (i+2) + ": " + line);
        }
      }
    }
    
    System.out.println("\n✅ All direct read tests passed!");
  }
  
  @Test
  void testPathNormalization() throws Exception {
    if (certificatePath == null || certificatePassword == null) {
      System.out.println("Skipping test - certificate not configured");
      return;
    }
    
    System.out.println("\n=== Testing Path Normalization ===");
    
    SharePointCertificateTokenManager tokenManager = 
        new SharePointCertificateTokenManager(tenantId, clientId, 
            certificatePath, certificatePassword, siteUrl);
    
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);
    
    // Test different path formats
    String[] testPaths = {
        "Shared Documents/Shared Documents/departments.csv",
        "/Shared Documents/Shared Documents/departments.csv",
        "shared_documents_departments",  // Table name format
        "departments.csv"  // Just filename
    };
    
    for (String path : testPaths) {
      System.out.println("\nTrying path: " + path);
      try {
        boolean exists = provider.exists(path);
        System.out.println("  Exists: " + exists);
        if (exists) {
          try (InputStream stream = provider.openInputStream(path)) {
            byte[] buffer = new byte[50];
            int bytesRead = stream.read(buffer);
            if (bytesRead > 0) {
              System.out.println("  Successfully read " + bytesRead + " bytes");
            }
          }
        }
      } catch (Exception e) {
        System.out.println("  Error: " + e.getMessage());
      }
    }
  }
}