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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify Microsoft Graph API credentials are available locally.
 * This test checks multiple credential sources and reports their status.
 */
public class MicrosoftGraphCredentialTest {

  @Test void verifyCredentialsAvailable() {
    Properties allCredentials = new Properties();
    boolean foundCredentials = false;

    System.out.println("\n=== Microsoft Graph API Credential Check ===\n");

    // Check environment variables
    System.out.println("1. Checking environment variables:");
    String tenantId = System.getenv("SHAREPOINT_TENANT_ID");
    String clientId = System.getenv("SHAREPOINT_CLIENT_ID");
    String clientSecret = System.getenv("SHAREPOINT_CLIENT_SECRET");
    String siteUrl = System.getenv("SHAREPOINT_SITE_URL");

    if (tenantId != null && clientId != null && clientSecret != null) {
      System.out.println("   ✓ Found credentials in environment variables");
      System.out.println("     - Tenant ID: " + maskSecret(tenantId));
      System.out.println("     - Client ID: " + maskSecret(clientId));
      System.out.println("     - Client Secret: " + (clientSecret.isEmpty() ? "[empty]" : "[set]"));
      System.out.println("     - Site URL: " + (siteUrl != null ? siteUrl : "[not set]"));
      foundCredentials = true;
      allCredentials.setProperty("env.tenantId", tenantId);
      allCredentials.setProperty("env.clientId", clientId);
      allCredentials.setProperty("env.clientSecret", clientSecret);
      if (siteUrl != null) {
        allCredentials.setProperty("env.siteUrl", siteUrl);
      }
    } else {
      System.out.println("   ✗ No credentials in environment variables");
    }

    // Check system properties
    System.out.println("\n2. Checking system properties:");
    tenantId = System.getProperty("SHAREPOINT_TENANT_ID");
    clientId = System.getProperty("SHAREPOINT_CLIENT_ID");
    clientSecret = System.getProperty("SHAREPOINT_CLIENT_SECRET");
    siteUrl = System.getProperty("SHAREPOINT_SITE_URL");

    if (tenantId != null && clientId != null && clientSecret != null) {
      System.out.println("   ✓ Found credentials in system properties");
      System.out.println("     - Tenant ID: " + maskSecret(tenantId));
      System.out.println("     - Client ID: " + maskSecret(clientId));
      System.out.println("     - Client Secret: " + (clientSecret.isEmpty() ? "[empty]" : "[set]"));
      System.out.println("     - Site URL: " + (siteUrl != null ? siteUrl : "[not set]"));
      foundCredentials = true;
      allCredentials.setProperty("sys.tenantId", tenantId);
      allCredentials.setProperty("sys.clientId", clientId);
      allCredentials.setProperty("sys.clientSecret", clientSecret);
      if (siteUrl != null) {
        allCredentials.setProperty("sys.siteUrl", siteUrl);
      }
    } else {
      System.out.println("   ✗ No credentials in system properties");
    }

    // Check local-test.properties
    System.out.println("\n3. Checking local-test.properties:");
    File localProps = new File("local-test.properties");
    if (localProps.exists()) {
      try (FileInputStream fis = new FileInputStream(localProps)) {
        Properties props = new Properties();
        props.load(fis);

        tenantId = props.getProperty("SHAREPOINT_TENANT_ID");
        clientId = props.getProperty("SHAREPOINT_CLIENT_ID");
        clientSecret = props.getProperty("SHAREPOINT_CLIENT_SECRET");
        siteUrl = props.getProperty("SHAREPOINT_SITE_URL");

        if (tenantId != null && clientId != null && clientSecret != null) {
          System.out.println("   ✓ Found credentials in local-test.properties");
          System.out.println("     - Tenant ID: " + maskSecret(tenantId));
          System.out.println("     - Client ID: " + maskSecret(clientId));
          System.out.println("     - Client Secret: " + (clientSecret.isEmpty() ? "[empty]" : "[set]"));
          System.out.println("     - Site URL: " + (siteUrl != null ? siteUrl : "[not set]"));
          foundCredentials = true;
          allCredentials.setProperty("local.tenantId", tenantId);
          allCredentials.setProperty("local.clientId", clientId);
          allCredentials.setProperty("local.clientSecret", clientSecret);
          if (siteUrl != null) {
            allCredentials.setProperty("local.siteUrl", siteUrl);
          }
        } else {
          System.out.println("   ✗ Incomplete credentials in local-test.properties");
        }
      } catch (IOException e) {
        System.out.println("   ✗ Error reading local-test.properties: " + e.getMessage());
      }
    } else {
      System.out.println("   ✗ File not found: " + localProps.getAbsolutePath());
    }

    // Check ~/.sharepoint/credentials.properties
    System.out.println("\n4. Checking ~/.sharepoint/credentials.properties:");
    Path homeCreds =
                               Paths.get(System.getProperty("user.home"), ".sharepoint", "credentials.properties");
    if (Files.exists(homeCreds)) {
      try (FileInputStream fis = new FileInputStream(homeCreds.toFile())) {
        Properties props = new Properties();
        props.load(fis);

        tenantId = props.getProperty("SHAREPOINT_TENANT_ID");
        clientId = props.getProperty("SHAREPOINT_CLIENT_ID");
        clientSecret = props.getProperty("SHAREPOINT_CLIENT_SECRET");
        siteUrl = props.getProperty("SHAREPOINT_SITE_URL");

        if (tenantId != null && clientId != null && clientSecret != null) {
          System.out.println("   ✓ Found credentials in " + homeCreds);
          System.out.println("     - Tenant ID: " + maskSecret(tenantId));
          System.out.println("     - Client ID: " + maskSecret(clientId));
          System.out.println("     - Client Secret: " + (clientSecret.isEmpty() ? "[empty]" : "[set]"));
          System.out.println("     - Site URL: " + (siteUrl != null ? siteUrl : "[not set]"));
          foundCredentials = true;
          allCredentials.setProperty("home.tenantId", tenantId);
          allCredentials.setProperty("home.clientId", clientId);
          allCredentials.setProperty("home.clientSecret", clientSecret);
          if (siteUrl != null) {
            allCredentials.setProperty("home.siteUrl", siteUrl);
          }
        } else {
          System.out.println("   ✗ Incomplete credentials in " + homeCreds);
        }
      } catch (IOException e) {
        System.out.println("   ✗ Error reading " + homeCreds + ": " + e.getMessage());
      }
    } else {
      System.out.println("   ✗ File not found: " + homeCreds);
    }

    // Summary
    System.out.println("\n=== Summary ===");
    if (foundCredentials) {
      System.out.println("✓ Microsoft Graph API credentials are available!");
      System.out.println("\nCredentials can be used for:");
      System.out.println("- SharePoint file access via Microsoft Graph API");
      System.out.println("- OneDrive for Business access");
      System.out.println("- Teams file access");

      // Check if we need to update the implementation
      System.out.println("\n=== Implementation Status ===");
      System.out.println("Current implementation uses: SharePoint REST API (/_api endpoints)");
      System.out.println("Recommended: Migrate to Microsoft Graph API for better compatibility");
      System.out.println("\nTo use Microsoft Graph API, the implementation should use:");
      System.out.println("- https://graph.microsoft.com/v1.0/sites/{site-id}/drive");
      System.out.println("- https://graph.microsoft.com/v1.0/drives/{drive-id}/items");

    } else {
      System.out.println("✗ No Microsoft Graph API credentials found!");
      System.out.println("\nTo set up credentials:");
      System.out.println("1. Register an app in Azure AD");
      System.out.println("2. Grant it SharePoint permissions (Sites.Read.All or Sites.ReadWrite.All)");
      System.out.println("3. Run: ./setup-sharepoint-creds.sh");
      System.out.println("   OR create local-test.properties with:");
      System.out.println("     SHAREPOINT_TENANT_ID=your-tenant-id");
      System.out.println("     SHAREPOINT_CLIENT_ID=your-client-id");
      System.out.println("     SHAREPOINT_CLIENT_SECRET=your-client-secret");
      System.out.println("     SHAREPOINT_SITE_URL=https://your-site.sharepoint.com");
    }

    // Assert for test result
    assertTrue(foundCredentials,
               "No Microsoft Graph API credentials found. See console output for setup instructions.");
  }

  private String maskSecret(String secret) {
    if (secret == null || secret.length() <= 8) {
      return "[hidden]";
    }
    return secret.substring(0, 4) + "..." + secret.substring(secret.length() - 4);
  }
}
