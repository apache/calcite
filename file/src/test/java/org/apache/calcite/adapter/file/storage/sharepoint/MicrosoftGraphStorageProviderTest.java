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

import org.apache.calcite.adapter.file.storage.MicrosoftGraphStorageProvider;
import org.apache.calcite.adapter.file.storage.SharePointTokenManager;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test for Microsoft Graph API-based SharePoint storage provider.
 * Compares functionality with the existing SharePoint REST API implementation.
 */
@Tag("integration")
public class MicrosoftGraphStorageProviderTest {

  private static Properties config;
  private static boolean credentialsAvailable = false;

  @BeforeAll
  static void loadConfiguration() {
    config = new Properties();

    // Load from local-test.properties
    File localProps = new File("local-test.properties");
    if (localProps.exists()) {
      try (FileInputStream fis = new FileInputStream(localProps)) {
        config.load(fis);
      } catch (IOException e) {
        System.err.println("Failed to load local-test.properties: " + e.getMessage());
      }
    }

    // Check if we have all required credentials
    credentialsAvailable = config.getProperty("SHAREPOINT_TENANT_ID") != null &&
                          config.getProperty("SHAREPOINT_CLIENT_ID") != null &&
                          config.getProperty("SHAREPOINT_CLIENT_SECRET") != null;

    if (credentialsAvailable) {
      System.out.println("Credentials loaded from local-test.properties");
    } else {
      System.out.println("Credentials not found in local-test.properties. Test will be skipped.");
    }
  }

  @Test void testMicrosoftGraphProviderConnection() throws IOException {
    assumeTrue(credentialsAvailable, "SharePoint credentials not available");

    String tenantId = config.getProperty("SHAREPOINT_TENANT_ID");
    String clientId = config.getProperty("SHAREPOINT_CLIENT_ID");
    String clientSecret = config.getProperty("SHAREPOINT_CLIENT_SECRET");
    String siteUrl = config.getProperty("SHAREPOINT_SITE_URL", "https://kenstott.sharepoint.com");

    System.out.println("\n=== Testing Microsoft Graph API Provider ===");
    System.out.println("Site URL: " + siteUrl);

    // Create token manager
    SharePointTokenManager tokenManager =
        new SharePointTokenManager(tenantId, clientId, clientSecret, siteUrl);

    // Create Microsoft Graph provider
    MicrosoftGraphStorageProvider graphProvider = new MicrosoftGraphStorageProvider(tokenManager);

    // Test connection and list files
    assertTrue(graphProvider.exists("/Shared Documents"));

    List<StorageProvider.FileEntry> entries = graphProvider.listFiles("/Shared Documents", false);
    assertNotNull(entries);

    System.out.println("Successfully connected via Microsoft Graph API!");
    System.out.println("Found " + entries.size() + " items in /Shared Documents:");
    entries.stream().limit(5).forEach(e -> {
      System.out.println(
          String.format(Locale.ROOT, "  %s %s (size: %d bytes)",
                       e.isDirectory() ? "[DIR] " : "[FILE]",
                       e.getName(),
                       e.getSize()));
    });
  }

  @Test void testMicrosoftGraphListFiles() throws IOException {
    assumeTrue(credentialsAvailable, "SharePoint credentials not available");

    String tenantId = config.getProperty("SHAREPOINT_TENANT_ID");
    String clientId = config.getProperty("SHAREPOINT_CLIENT_ID");
    String clientSecret = config.getProperty("SHAREPOINT_CLIENT_SECRET");
    String siteUrl = config.getProperty("SHAREPOINT_SITE_URL", "https://kenstott.sharepoint.com");

    // Create token manager
    SharePointTokenManager tokenManager =
        new SharePointTokenManager(tenantId, clientId, clientSecret, siteUrl);

    // Create provider
    MicrosoftGraphStorageProvider graphProvider = new MicrosoftGraphStorageProvider(tokenManager);

    System.out.println("\n=== Testing Microsoft Graph API ===");

    // List files
    List<StorageProvider.FileEntry> graphEntries = graphProvider.listFiles("/Shared Documents", false);

    System.out.println("Microsoft Graph API found: " + graphEntries.size() + " items");

    // Show results
    System.out.println("\nGraph API items:");
    graphEntries.stream().limit(5).forEach(e ->
        System.out.println("  - " + e.getName() + (e.isDirectory() ? " [DIR]" : "")));
  }

  @Test void testFileMetadata() throws IOException {
    assumeTrue(credentialsAvailable, "SharePoint credentials not available");

    String tenantId = config.getProperty("SHAREPOINT_TENANT_ID");
    String clientId = config.getProperty("SHAREPOINT_CLIENT_ID");
    String clientSecret = config.getProperty("SHAREPOINT_CLIENT_SECRET");
    String siteUrl = config.getProperty("SHAREPOINT_SITE_URL", "https://kenstott.sharepoint.com");

    SharePointTokenManager tokenManager =
        new SharePointTokenManager(tenantId, clientId, clientSecret, siteUrl);
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    // First, find a file to test
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents", false);
    StorageProvider.FileEntry testFile = entries.stream()
        .filter(e -> !e.isDirectory())
        .findFirst()
        .orElse(null);

    if (testFile != null) {
      System.out.println("\n=== Testing File Metadata ===");
      System.out.println("Test file: " + testFile.getName());

      // Get metadata
      StorageProvider.FileMetadata metadata = provider.getMetadata(testFile.getPath());
      assertNotNull(metadata);
      assertEquals(testFile.getSize(), metadata.getSize());

      System.out.println("File size: " + metadata.getSize() + " bytes");
      System.out.println("Content type: " + metadata.getContentType());
      System.out.println("ETag: " + metadata.getEtag());

      // Test that the file exists
      assertTrue(provider.exists(testFile.getPath()));
      assertFalse(provider.isDirectory(testFile.getPath()));
    } else {
      System.out.println("No files found to test metadata");
    }
  }

  @Test void testDirectoryOperations() throws IOException {
    assumeTrue(credentialsAvailable, "SharePoint credentials not available");

    String tenantId = config.getProperty("SHAREPOINT_TENANT_ID");
    String clientId = config.getProperty("SHAREPOINT_CLIENT_ID");
    String clientSecret = config.getProperty("SHAREPOINT_CLIENT_SECRET");
    String siteUrl = config.getProperty("SHAREPOINT_SITE_URL", "https://kenstott.sharepoint.com");

    SharePointTokenManager tokenManager =
        new SharePointTokenManager(tenantId, clientId, clientSecret, siteUrl);
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    System.out.println("\n=== Testing Directory Operations ===");

    // Test root exists and is a directory
    assertTrue(provider.exists("/Shared Documents"));
    assertTrue(provider.isDirectory("/Shared Documents"));

    // Find a subdirectory to test
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents", false);
    StorageProvider.FileEntry testDir = entries.stream()
        .filter(StorageProvider.FileEntry::isDirectory)
        .findFirst()
        .orElse(null);

    if (testDir != null) {
      System.out.println("Testing directory: " + testDir.getName());
      assertTrue(provider.exists(testDir.getPath()));
      assertTrue(provider.isDirectory(testDir.getPath()));

      // List contents of subdirectory
      List<StorageProvider.FileEntry> subEntries = provider.listFiles(testDir.getPath(), false);
      System.out.println("Subdirectory contains " + subEntries.size() + " items");
    } else {
      System.out.println("No subdirectories found to test");
    }
  }
}
