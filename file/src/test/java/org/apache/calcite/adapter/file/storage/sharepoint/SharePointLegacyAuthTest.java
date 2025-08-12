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

import org.apache.calcite.adapter.file.storage.SharePointLegacyTokenManager;
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
 * Test for SharePoint legacy authentication using client secret.
 * This uses the Azure ACS endpoint which supports client secrets.
 */
@Tag("integration")
public class SharePointLegacyAuthTest {
  
  private static String clientId;
  private static String clientSecret;
  private static String siteUrl;
  private static String realm;
  private static boolean skipTest = false;
  
  @BeforeAll
  static void setup() {
    Properties props = loadProperties();
    
    // Check for legacy SharePoint app credentials
    clientId = props.getProperty("SHAREPOINT_LEGACY_CLIENT_ID");
    clientSecret = props.getProperty("SHAREPOINT_LEGACY_CLIENT_SECRET");
    siteUrl = props.getProperty("SHAREPOINT_SITE_URL");
    realm = props.getProperty("SHAREPOINT_REALM");
    
    if (clientId == null || clientSecret == null || siteUrl == null) {
      System.out.println("Legacy SharePoint credentials not found. To test legacy auth:");
      System.out.println("1. Register app at: https://[tenant].sharepoint.com/_layouts/15/appregnew.aspx");
      System.out.println("2. Grant permissions at: https://[tenant].sharepoint.com/_layouts/15/appinv.aspx");
      System.out.println("   Use this XML for full control:");
      System.out.println("   <AppPermissionRequests AllowAppOnlyPolicy=\"true\">");
      System.out.println("     <AppPermissionRequest Scope=\"http://sharepoint/content/tenant\" Right=\"FullControl\" />");
      System.out.println("   </AppPermissionRequests>");
      System.out.println("3. Add to local-test.properties:");
      System.out.println("   SHAREPOINT_LEGACY_CLIENT_ID=<your-client-id>");
      System.out.println("   SHAREPOINT_LEGACY_CLIENT_SECRET=<your-client-secret>");
      System.out.println("   SHAREPOINT_REALM=<optional-realm-id>");
      skipTest = true;
      return;
    }
    
    System.out.println("Legacy SharePoint test configuration:");
    System.out.println("  Site URL: " + siteUrl);
    System.out.println("  Client ID: " + clientId);
    System.out.println("  Realm: " + (realm != null ? realm : "will be auto-discovered"));
  }
  
  private static Properties loadProperties() {
    Properties props = new Properties();
    
    // Try environment variables first
    props.setProperty("SHAREPOINT_LEGACY_CLIENT_ID", 
        System.getenv().getOrDefault("SHAREPOINT_LEGACY_CLIENT_ID", ""));
    props.setProperty("SHAREPOINT_LEGACY_CLIENT_SECRET",
        System.getenv().getOrDefault("SHAREPOINT_LEGACY_CLIENT_SECRET", ""));
    props.setProperty("SHAREPOINT_SITE_URL",
        System.getenv().getOrDefault("SHAREPOINT_SITE_URL", ""));
    props.setProperty("SHAREPOINT_REALM",
        System.getenv().getOrDefault("SHAREPOINT_REALM", ""));
    
    // Then try local properties file
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
        // File properties override environment variables
        props.putAll(fileProps);
        System.out.println("Loaded legacy config from: " + propsFile.getAbsolutePath());
      } catch (IOException e) {
        System.err.println("Failed to load properties: " + e.getMessage());
      }
    }
    
    // Remove empty values
    props.entrySet().removeIf(e -> e.getValue().toString().isEmpty());
    
    return props;
  }
  
  @Test
  void testLegacyAuthentication() throws Exception {
    if (skipTest) {
      System.out.println("Skipping test - legacy credentials not configured");
      return;
    }
    
    System.out.println("\n=== Testing Legacy SharePoint Authentication ===");
    
    // Create token manager with or without realm
    SharePointLegacyTokenManager tokenManager = realm != null
        ? new SharePointLegacyTokenManager(clientId, clientSecret, siteUrl, realm)
        : new SharePointLegacyTokenManager(clientId, clientSecret, siteUrl);
    
    // Create storage provider
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);
    
    // Test authentication by listing files
    System.out.println("Attempting to list files in /Shared Documents...");
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents", false);
    
    assertNotNull(entries, "Should be able to list files");
    System.out.println("Success! Found " + entries.size() + " items:");
    
    for (StorageProvider.FileEntry entry : entries) {
      String type = entry.isDirectory() ? "[DIR]" : "[FILE]";
      System.out.println("  " + type + " " + entry.getName() + 
                        " (size: " + entry.getSize() + " bytes)");
    }
  }
  
  @Test
  void testLegacyWithNestedPath() throws Exception {
    if (skipTest) {
      return;
    }
    
    System.out.println("\n=== Testing Legacy Auth with Nested Path ===");
    
    SharePointLegacyTokenManager tokenManager = realm != null
        ? new SharePointLegacyTokenManager(clientId, clientSecret, siteUrl, realm)
        : new SharePointLegacyTokenManager(clientId, clientSecret, siteUrl);
    
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);
    
    // Try the nested path that Graph API uses
    System.out.println("Attempting to list files in /Shared Documents/Shared Documents...");
    try {
      List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents/Shared Documents", false);
      System.out.println("Found " + entries.size() + " items in nested path");
      
      for (StorageProvider.FileEntry entry : entries) {
        if (entry.getName().toLowerCase().endsWith(".csv")) {
          System.out.println("  [CSV] " + entry.getName());
        }
      }
    } catch (Exception e) {
      System.out.println("Nested path failed: " + e.getMessage());
      System.out.println("This is expected if the nested structure doesn't exist");
    }
  }
}