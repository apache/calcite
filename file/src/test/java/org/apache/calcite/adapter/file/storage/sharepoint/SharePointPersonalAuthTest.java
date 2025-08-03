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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Manual test for SharePoint using Microsoft Graph API with a personal access token.
 *
 * This test helps you get a valid token for testing without Azure AD app registration.
 *
 * Steps:
 * 1. Open browser in incognito/private mode
 * 2. Go to your SharePoint site
 * 3. Open Developer Tools (F12)
 * 4. Go to Network tab
 * 5. Navigate around SharePoint (click on Documents, etc.)
 * 6. Look for requests to /_api/ endpoints
 * 7. Find the Authorization header (starts with "Bearer ")
 * 8. Copy the token (everything after "Bearer ")
 *
 * Run with:
 * mvn test -DrunSharePointPersonalTests=true \
 *   -DSHAREPOINT_SITE_URL=https://kenstott.sharepoint.com/sites/YourSite \
 *   -DSHAREPOINT_BEARER_TOKEN="your-token-here"
 *
 * Note: These tokens expire quickly (usually 1 hour), so you'll need to
 * get a fresh one for each test session.
 */
@EnabledIfSystemProperty(named = "runSharePointPersonalTests", matches = "true")
public class SharePointPersonalAuthTest {

  @Test void testPersonalSharePointAccess() throws IOException {
    String siteUrl = System.getProperty("SHAREPOINT_SITE_URL");
    String bearerToken = System.getProperty("SHAREPOINT_BEARER_TOKEN");

    if (siteUrl == null || bearerToken == null) {
      fail("Please provide -DSHAREPOINT_SITE_URL and -DSHAREPOINT_BEARER_TOKEN");
    }

    // Remove quotes if accidentally included
    bearerToken = bearerToken.replace("\"", "").trim();

    System.out.println("Testing SharePoint access to: " + siteUrl);

    MicrosoftGraphTokenManager tokenManager = new MicrosoftGraphTokenManager(bearerToken, siteUrl);
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    // Test basic connectivity
    String testPath = "/Shared Documents";
    boolean exists = provider.exists(testPath);
    System.out.println("Shared Documents exists: " + exists);

    if (exists) {
      // List files
      List<StorageProvider.FileEntry> entries = provider.listFiles(testPath, false);
      System.out.println("\nFound " + entries.size() + " items:");

      entries.stream().limit(10).forEach(entry -> {
        System.out.printf(Locale.ROOT, "  %s %s (size: %d)%n",
            entry.isDirectory() ? "[DIR] " : "[FILE]",
            entry.getName(),
            entry.getSize());
      });
    }
  }

  @Test void testGetTokenInstructions() {
    System.out.println("\n=== How to Get a SharePoint Bearer Token ===\n");
    System.out.println("1. Open Chrome/Edge in Incognito mode");
    System.out.println("2. Go to your SharePoint site");
    System.out.println("3. Sign in with your Microsoft 365 account");
    System.out.println("4. Press F12 to open Developer Tools");
    System.out.println("5. Go to the Network tab");
    System.out.println("6. Click on 'Documents' or navigate to a library");
    System.out.println("7. Look for requests containing '/_api/' in the name");
    System.out.println("8. Click on one of these requests");
    System.out.println("9. Go to the Headers tab");
    System.out.println("10. Find 'Authorization: Bearer eyJ...' (very long string)");
    System.out.println("11. Copy everything after 'Bearer ' (the token)");
    System.out.println("\nThe token will look like:");
    System.out.println("eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6...(very long)");
    System.out.println("\nRun test with:");
    System.out.println("mvn test -DrunSharePointPersonalTests=true \\");
    System.out.println("  -DSHAREPOINT_SITE_URL=https://yourdomain.sharepoint.com \\");
    System.out.println("  -DSHAREPOINT_BEARER_TOKEN=\"paste-token-here\"");
  }
}
