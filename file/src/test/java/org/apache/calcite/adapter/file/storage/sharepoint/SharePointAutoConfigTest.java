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
import org.apache.calcite.adapter.file.storage.MicrosoftGraphTokenManager;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Automated SharePoint test using Microsoft Graph API that loads credentials from multiple sources.
 *
 * This test will automatically run if credentials are found in any of:
 * 1. Environment variables
 * 2. System properties
 * 3. local-test.properties file
 * 4. ~/.sharepoint/credentials.properties
 *
 * No need for -DrunSharePointTests flag!
 */
public class SharePointAutoConfigTest {

  private static Properties config;
  private static boolean credentialsAvailable = false;

  @BeforeAll
  static void loadConfiguration() {
    config = new Properties();

    // Try multiple sources in order of preference
    loadFromEnvironment();
    loadFromSystemProperties();
    loadFromHomeDirectory();
    loadFromLocalFile(); // Load local-test.properties last so it takes precedence

    // Check if we have all required credentials
    credentialsAvailable = hasRequiredCredentials();

    if (credentialsAvailable) {
      System.out.println("SharePoint credentials loaded successfully from: " +
                         config.getProperty("_source"));
    } else {
      System.out.println("SharePoint credentials not found. Test will be skipped.");
      System.out.println("To run this test, provide credentials via:");
      System.out.println("  1. Environment variables (SHAREPOINT_TENANT_ID, etc.)");
      System.out.println("  2. local-test.properties file");
      System.out.println("  3. ~/.sharepoint/credentials.properties");
    }
  }

  private static void loadFromEnvironment() {
    String tenantId = System.getenv("SHAREPOINT_TENANT_ID");
    if (tenantId != null && !tenantId.isEmpty()) {
      config.setProperty("SHAREPOINT_TENANT_ID", tenantId);
      config.setProperty("SHAREPOINT_CLIENT_ID",
                        System.getenv("SHAREPOINT_CLIENT_ID"));
      config.setProperty("SHAREPOINT_CLIENT_SECRET",
                        System.getenv("SHAREPOINT_CLIENT_SECRET"));
      config.setProperty("SHAREPOINT_SITE_URL",
                        getOrDefault(System.getenv("SHAREPOINT_SITE_URL"),
                                   "https://kenstott.sharepoint.com"));
      config.setProperty("_source", "environment variables");
    }
  }

  private static void loadFromSystemProperties() {
    String tenantId = System.getProperty("SHAREPOINT_TENANT_ID");
    if (tenantId != null && !tenantId.isEmpty()) {
      config.setProperty("SHAREPOINT_TENANT_ID", tenantId);
      config.setProperty("SHAREPOINT_CLIENT_ID",
                        System.getProperty("SHAREPOINT_CLIENT_ID"));
      config.setProperty("SHAREPOINT_CLIENT_SECRET",
                        System.getProperty("SHAREPOINT_CLIENT_SECRET"));
      config.setProperty("SHAREPOINT_SITE_URL",
                        getOrDefault(System.getProperty("SHAREPOINT_SITE_URL"),
                                   "https://kenstott.sharepoint.com"));
      config.setProperty("_source", "system properties");
    }
  }

  private static void loadFromLocalFile() {
    File localProps = new File("local-test.properties");
    if (localProps.exists()) {
      try (FileInputStream fis = new FileInputStream(localProps)) {
        Properties props = new Properties();
        props.load(fis);

        // Only override if we have a tenant ID
        if (props.getProperty("SHAREPOINT_TENANT_ID") != null) {
          config.putAll(props);
          config.setProperty("_source", "local-test.properties");
        }
      } catch (IOException e) {
        // Ignore
      }
    }
  }

  private static void loadFromHomeDirectory() {
    Path homeCreds =
                               Paths.get(System.getProperty("user.home"), ".sharepoint", "credentials.properties");
    if (Files.exists(homeCreds)) {
      try (FileInputStream fis = new FileInputStream(homeCreds.toFile())) {
        Properties props = new Properties();
        props.load(fis);

        // Only override if we have a tenant ID
        if (props.getProperty("SHAREPOINT_TENANT_ID") != null) {
          config.putAll(props);
          config.setProperty("_source", "~/.sharepoint/credentials.properties");
        }
      } catch (IOException e) {
        // Ignore
      }
    }
  }

  private static boolean hasRequiredCredentials() {
    return config.getProperty("SHAREPOINT_TENANT_ID") != null &&
           config.getProperty("SHAREPOINT_CLIENT_ID") != null &&
           config.getProperty("SHAREPOINT_CLIENT_SECRET") != null;
  }

  private static String getOrDefault(String value, String defaultValue) {
    return (value != null && !value.isEmpty()) ? value : defaultValue;
  }

  @Test void testSharePointConnection() throws IOException {
    // Skip if no credentials
    assumeTrue(credentialsAvailable, "SharePoint credentials not available");

    String tenantId = config.getProperty("SHAREPOINT_TENANT_ID");
    String clientId = config.getProperty("SHAREPOINT_CLIENT_ID");
    String clientSecret = config.getProperty("SHAREPOINT_CLIENT_SECRET");
    String siteUrl = config.getProperty("SHAREPOINT_SITE_URL");

    System.out.println("Testing SharePoint connection to: " + siteUrl);

    // Create token manager and provider
    MicrosoftGraphTokenManager tokenManager =
        new MicrosoftGraphTokenManager(tenantId, clientId, clientSecret, siteUrl);
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    // Test connection
    assertTrue(provider.exists("/Shared Documents"));

    // List files
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents", false);
    assertNotNull(entries);

    System.out.println("Successfully connected! Found " + entries.size() + " items");
    entries.stream().limit(5).forEach(e -> {
      System.out.println(
          String.format(Locale.ROOT, "  %s %s",
                       e.isDirectory() ? "[DIR] " : "[FILE]",
                       e.getName()));
    });
  }

  @Test void testCredentialSetup() {
    // This test always runs to help with setup
    if (!credentialsAvailable) {
      System.out.println("\n=== SharePoint Credential Setup Guide ===\n");

      System.out.println("Option 1: Create local-test.properties in current directory:");
      System.out.println("  SHAREPOINT_TENANT_ID=your-tenant-id");
      System.out.println("  SHAREPOINT_CLIENT_ID=your-client-id");
      System.out.println("  SHAREPOINT_CLIENT_SECRET=your-client-secret");
      System.out.println("  SHAREPOINT_SITE_URL=https://kenstott.sharepoint.com");

      System.out.println("\nOption 2: Create ~/.sharepoint/credentials.properties:");
      System.out.println("  mkdir -p ~/.sharepoint");
      System.out.println("  chmod 700 ~/.sharepoint");
      System.out.println("  vi ~/.sharepoint/credentials.properties");
      System.out.println("  chmod 600 ~/.sharepoint/credentials.properties");

      System.out.println("\nOption 3: Set environment variables:");
      System.out.println("  export SHAREPOINT_TENANT_ID=your-tenant-id");
      System.out.println("  export SHAREPOINT_CLIENT_ID=your-client-id");
      System.out.println("  export SHAREPOINT_CLIENT_SECRET=your-client-secret");

      System.out.println("\nOnce configured, this test will run automatically!");
    } else {
      System.out.println("Credentials loaded from: " + config.getProperty("_source"));
      System.out.println("Site URL: " + config.getProperty("SHAREPOINT_SITE_URL"));
    }
  }
}
