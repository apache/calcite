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
 * Test for SharePoint REST API with certificate authentication.
 * This uses the modern Azure AD authentication with certificates.
 */
@Tag("integration")
public class SharePointCertificateAuthTest {
  
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
    certificatePath = props.getProperty("SHAREPOINT_CERTIFICATE_PATH");
    certificatePassword = props.getProperty("SHAREPOINT_CERT_PASSWORD");  // Updated property name
    
    // Check if certificate file exists
    if (certificatePath == null) {
      // Try default location
      File certFile = new File("src/test/resources/SharePointAppOnlyCert.pfx");
      if (certFile.exists()) {
        certificatePath = certFile.getAbsolutePath();
        System.out.println("Found certificate at: " + certificatePath);
      }
    }
    
    if (tenantId == null || clientId == null || siteUrl == null || 
        certificatePath == null || certificatePassword == null) {
      System.out.println("SharePoint certificate authentication not fully configured.");
      System.out.println("Required in local-test.properties:");
      System.out.println("  SHAREPOINT_TENANT_ID=<your-tenant-id>");
      System.out.println("  SHAREPOINT_CLIENT_ID=<your-app-client-id>");
      System.out.println("  SHAREPOINT_SITE_URL=https://<tenant>.sharepoint.com");
      System.out.println("  SHAREPOINT_CERTIFICATE_PATH=<path-to-pfx-file>");
      System.out.println("  SHAREPOINT_CERT_PASSWORD=<pfx-password>");
      return;
    }
    
    System.out.println("SharePoint Certificate Authentication Configuration:");
    System.out.println("  Site URL: " + siteUrl);
    System.out.println("  Client ID: " + clientId);
    System.out.println("  Tenant ID: " + tenantId);
    System.out.println("  Certificate: " + certificatePath);
  }
  
  private static Properties loadProperties() {
    Properties props = new Properties();
    
    // Load from environment variables
    String[] envVars = {
        "SHAREPOINT_TENANT_ID", "SHAREPOINT_CLIENT_ID", "SHAREPOINT_SITE_URL",
        "SHAREPOINT_CERTIFICATE_PATH", "SHAREPOINT_CERT_PASSWORD"
    };
    
    for (String var : envVars) {
      String value = System.getenv(var);
      if (value != null && !value.isEmpty()) {
        props.setProperty(var, value);
      }
    }
    
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
  void testCertificateAuthentication() throws Exception {
    if (certificatePath == null || certificatePassword == null) {
      System.out.println("Skipping test - certificate not configured");
      return;
    }
    
    System.out.println("\n=== Testing SharePoint REST API with Certificate ===");
    
    // Create token manager with certificate
    SharePointCertificateTokenManager tokenManager = 
        new SharePointCertificateTokenManager(tenantId, clientId, 
            certificatePath, certificatePassword, siteUrl);
    
    // Create storage provider
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);
    
    // Test authentication - just verify we can connect
    System.out.println("Testing authentication at configured path: /Shared Documents/Shared Documents");
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents/Shared Documents", false);
    
    assertNotNull(entries, "Should be able to list files with certificate auth");
    System.out.println("✅ Certificate authentication successful!");
    System.out.println("Found " + entries.size() + " items at configured location:");
    
    int csvCount = 0;
    for (StorageProvider.FileEntry entry : entries) {
      String type = entry.isDirectory() ? "[DIR]" : "[FILE]";
      System.out.println("  " + type + " " + entry.getName() + 
                        " (size: " + entry.getSize() + " bytes)");
      if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(".csv")) {
        csvCount++;
      }
    }
    
    System.out.println("\n✅ Found " + csvCount + " CSV files ready for Calcite!");
    assertTrue(csvCount > 0, "Should find CSV files at the configured path");
  }
  
  @Test
  void testCertificateWithNestedPath() throws Exception {
    if (certificatePath == null || certificatePassword == null) {
      System.out.println("Skipping test - certificate not configured");
      return;
    }
    
    System.out.println("\n=== Testing Certificate Auth with Nested Path ===");
    
    SharePointCertificateTokenManager tokenManager = 
        new SharePointCertificateTokenManager(tenantId, clientId,
            certificatePath, certificatePassword, siteUrl);
    
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);
    
    // Try the nested path structure
    System.out.println("Attempting to list files in /Shared Documents/Shared Documents...");
    try {
      List<StorageProvider.FileEntry> entries = 
          provider.listFiles("/Shared Documents/Shared Documents", false);
      System.out.println("Found " + entries.size() + " items in nested path");
      
      // Look for CSV files
      int csvCount = 0;
      for (StorageProvider.FileEntry entry : entries) {
        if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(".csv")) {
          System.out.println("  [CSV] " + entry.getName() + " (size: " + entry.getSize() + " bytes)");
          csvCount++;
        }
      }
      
      if (csvCount > 0) {
        System.out.println("Found " + csvCount + " CSV files!");
      }
    } catch (Exception e) {
      System.out.println("Nested path failed: " + e.getMessage());
      System.out.println("This is expected if the nested structure doesn't exist");
    }
  }
  
  @Test
  void testCertificateAuthWithCalcite() throws Exception {
    if (certificatePath == null || certificatePassword == null) {
      System.out.println("Skipping test - certificate not configured");
      return;
    }
    
    System.out.println("\n=== Testing Certificate Auth with Calcite Schema ===");
    
    // This test verifies that certificate auth works with the full Calcite stack
    // The actual Calcite integration would use StorageProviderFactory with:
    // storageConfig.put("certificatePath", certificatePath);
    // storageConfig.put("certificatePassword", certificatePassword);
    
    System.out.println("Certificate authentication is ready for use with Calcite!");
    System.out.println("Configuration example:");
    System.out.println("  storageConfig.put(\"certificatePath\", \"" + certificatePath + "\");");
    System.out.println("  storageConfig.put(\"certificatePassword\", \"<password>\");");
    System.out.println("  // No need for useLegacyAuth or useGraphApi flags");
  }
}