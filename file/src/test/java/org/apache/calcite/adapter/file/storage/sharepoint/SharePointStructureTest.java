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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to diagnose SharePoint structure and verify if the nested
 * "Shared Documents/Shared Documents" is normal or a configuration issue.
 */
@Tag("integration")
public class SharePointStructureTest {
  
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
    
    File certFile = new File("src/test/resources/SharePointAppOnlyCert.pfx");
    if (certFile.exists()) {
      certificatePath = certFile.getAbsolutePath();
    }
    
    if (tenantId == null || clientId == null || siteUrl == null || 
        certificatePath == null || certificatePassword == null) {
      System.out.println("SharePoint certificate authentication not fully configured.");
      return;
    }
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
      } catch (IOException e) {
        System.err.println("Failed to load properties: " + e.getMessage());
      }
    }
    return props;
  }
  
  @Test
  void analyzeSharePointStructure() throws Exception {
    if (certificatePath == null || certificatePassword == null) {
      System.out.println("Skipping test - certificate not configured");
      return;
    }
    
    System.out.println("\n=== SHAREPOINT STRUCTURE ANALYSIS ===\n");
    
    SharePointCertificateTokenManager tokenManager = 
        new SharePointCertificateTokenManager(tenantId, clientId, 
            certificatePath, certificatePassword, siteUrl);
    
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);
    
    // Test 1: List root of site
    System.out.println("1. ROOT OF SITE (empty path):");
    System.out.println("--------------------------------");
    try {
      List<StorageProvider.FileEntry> rootEntries = provider.listFiles("", false);
      for (StorageProvider.FileEntry entry : rootEntries) {
        System.out.println("  " + (entry.isDirectory() ? "[DIR] " : "[FILE]") + 
                          " " + entry.getName() + " (path: " + entry.getPath() + ")");
      }
    } catch (Exception e) {
      System.out.println("  Error: " + e.getMessage());
    }
    
    // Test 2: List "Shared Documents" (the document library)
    System.out.println("\n2. DOCUMENT LIBRARY LEVEL (/Shared Documents):");
    System.out.println("------------------------------------------------");
    try {
      List<StorageProvider.FileEntry> libEntries = provider.listFiles("/Shared Documents", false);
      for (StorageProvider.FileEntry entry : libEntries) {
        System.out.println("  " + (entry.isDirectory() ? "[DIR] " : "[FILE]") + 
                          " " + entry.getName() + " (path: " + entry.getPath() + ")");
        
        // Check if there's a nested "Shared Documents" folder
        if (entry.isDirectory() && entry.getName().equals("Shared Documents")) {
          System.out.println("  ⚠️  WARNING: Found nested 'Shared Documents' folder inside the library!");
          System.out.println("      This is NOT normal SharePoint structure.");
        }
      }
    } catch (Exception e) {
      System.out.println("  Error: " + e.getMessage());
    }
    
    // Test 3: Try different paths to understand the structure
    System.out.println("\n3. TESTING VARIOUS PATH FORMATS:");
    System.out.println("----------------------------------");
    String[] testPaths = {
        "Shared Documents",
        "/Shared Documents",
        "Shared Documents/Shared Documents",
        "/Shared Documents/Shared Documents",
        "Documents",
        "/Documents"
    };
    
    for (String path : testPaths) {
      System.out.println("\nPath: '" + path + "'");
      try {
        List<StorageProvider.FileEntry> entries = provider.listFiles(path, false);
        System.out.println("  ✓ Success - Found " + entries.size() + " items");
        
        // Count CSV files
        long csvCount = entries.stream()
            .filter(e -> !e.isDirectory() && e.getName().endsWith(".csv"))
            .count();
        if (csvCount > 0) {
          System.out.println("    → Contains " + csvCount + " CSV files");
        }
        
        // Show first few entries
        entries.stream().limit(3).forEach(e -> 
            System.out.println("      " + (e.isDirectory() ? "[D]" : "[F]") + " " + e.getName()));
            
      } catch (Exception e) {
        System.out.println("  ✗ Failed: " + e.getMessage());
      }
    }
    
    // Test 4: Check if files can be accessed at root level
    System.out.println("\n4. ATTEMPTING DIRECT FILE ACCESS:");
    System.out.println("-----------------------------------");
    String[] filePaths = {
        "departments.csv",
        "Shared Documents/departments.csv", 
        "/Shared Documents/departments.csv",
        "Shared Documents/Shared Documents/departments.csv",
        "/Shared Documents/Shared Documents/departments.csv"
    };
    
    for (String filePath : filePaths) {
      System.out.print("File: '" + filePath + "' - ");
      try {
        boolean exists = provider.exists(filePath);
        System.out.println(exists ? "✓ EXISTS" : "✗ NOT FOUND");
      } catch (Exception e) {
        System.out.println("✗ ERROR: " + e.getMessage());
      }
    }
    
    System.out.println("\n=== DIAGNOSIS ===");
    System.out.println("If your files are ONLY accessible at 'Shared Documents/Shared Documents',");
    System.out.println("this indicates someone created a 'Shared Documents' folder inside the ");
    System.out.println("document library. This is NOT standard SharePoint configuration.");
    System.out.println("\nRECOMMENDATION: Move the files from the nested folder to the root of");
    System.out.println("the document library for cleaner structure.");
  }
}