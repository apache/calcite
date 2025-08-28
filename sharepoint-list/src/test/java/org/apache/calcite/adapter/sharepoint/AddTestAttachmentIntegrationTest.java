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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.adapter.sharepoint.auth.SharePointAuth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Integration test to add a test list item with an attachment.
 * This test creates a list item and adds an attachment to help with testing.
 */
@EnabledIf("isConfigured")
public class AddTestAttachmentIntegrationTest {

  private String tenantId;
  private String clientId;
  private String siteUrl;
  private String certPassword;
  private SharePointRestListClient restClient;
  private Properties props;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Check if integration test configuration is available.
   */
  static boolean isConfigured() {
    try {
      Properties props = new Properties();
      props.load(new FileInputStream("local-test.properties"));

      String tenantId = props.getProperty("SHAREPOINT_TENANT_ID");
      String clientId = props.getProperty("SHAREPOINT_CLIENT_ID");
      String certPassword = props.getProperty("SHAREPOINT_CERT_PASSWORD");
      String siteUrl = props.getProperty("SHAREPOINT_SITE_URL");

      boolean configured = tenantId != null && !tenantId.isEmpty() &&
                          clientId != null && !clientId.isEmpty() &&
                          certPassword != null && !certPassword.isEmpty() &&
                          siteUrl != null && !siteUrl.isEmpty();

      if (configured) {
        System.out.println("Add test attachment integration test enabled");
      }

      return configured;
    } catch (Exception e) {
      return false;
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    props = new Properties();
    props.load(new FileInputStream("local-test.properties"));

    tenantId = props.getProperty("SHAREPOINT_TENANT_ID");
    clientId = props.getProperty("SHAREPOINT_CLIENT_ID");
    certPassword = props.getProperty("SHAREPOINT_CERT_PASSWORD");
    siteUrl = props.getProperty("SHAREPOINT_SITE_URL");

    // Use certificate authentication for REST API
    org.apache.calcite.adapter.file.storage.SharePointCertificateTokenManager tokenManager =
        new org.apache.calcite.adapter.file.storage.SharePointCertificateTokenManager(
            tenantId, clientId, "../file/src/test/resources/SharePointAppOnlyCert.pfx",
            certPassword, siteUrl);

    SharePointAuth auth = new SharePointAuth() {
      @Override public String getAccessToken() throws IOException, InterruptedException {
        return tokenManager.getAccessToken();
      }
    };

    restClient = new SharePointRestListClient(siteUrl, auth);
  }

  @Test public void addTestListItemWithAttachment() {
    try {
      String listName = "TestListWithAttachments";

      System.out.println("\n=== Creating Test List and Item with Attachment ===");

      // First, create a simple list for testing
      ObjectNode listRequestBody = MAPPER.createObjectNode();
      ObjectNode metadata = MAPPER.createObjectNode();
      metadata.put("type", "SP.List");
      listRequestBody.set("__metadata", metadata);
      listRequestBody.put("Title", listName);
      listRequestBody.put("Description", "Test list for attachment testing - created by Calcite integration test");
      listRequestBody.put("BaseTemplate", 100); // Generic List template
      listRequestBody.put("EnableAttachments", true);

      // Create the list
      String createListUrl = siteUrl + "/_api/web/lists";

      try {
        JsonNode listResponse = restClient.executeRestCall("POST", createListUrl, listRequestBody);
        System.out.println("✅ List '" + listName + "' created successfully");
      } catch (Exception e) {
        if (e.getMessage().contains("already exists")) {
          System.out.println("📝 List '" + listName + "' already exists, continuing...");
        } else {
          throw e;
        }
      }

      // Create a test item in the list
      ObjectNode itemRequestBody = MAPPER.createObjectNode();
      ObjectNode itemMetadata = MAPPER.createObjectNode();
      itemMetadata.put("type", "SP.Data." + listName + "ListItem");
      itemRequestBody.set("__metadata", itemMetadata);
      itemRequestBody.put("Title", "Test Item with Attachment");

      String createItemUrl = siteUrl + "/_api/web/lists/getbytitle('" + listName + "')/items";
      JsonNode itemResponse = restClient.executeRestCall("POST", createItemUrl, itemRequestBody);

      String itemId = itemResponse.get("d").get("Id").asText();
      System.out.println("✅ List item created with ID: " + itemId);

      // Add an attachment to the item
      String attachmentContent = "This is a test attachment created by Apache Calcite SharePoint adapter.\n"
  +
          "List: " + listName + "\n"
  +
          "Item ID: " + itemId + "\n"
  +
          "Created: " + java.time.LocalDateTime.now() + "\n"
  +
          "Purpose: Testing attachment functionality";

      byte[] attachmentBytes = attachmentContent.getBytes(StandardCharsets.UTF_8);
      String fileName = "test-attachment.txt";

      // Upload attachment using REST API
      String uploadUrl = siteUrl + "/_api/web/lists/getbytitle('" + listName + "')/items(" + itemId + ")/AttachmentFiles/add(FileName='" + fileName + "')";

      // Use binary upload for attachment
      java.net.URL apiUrl = java.net.URI.create(uploadUrl).toURL();
      java.net.HttpURLConnection conn = (java.net.HttpURLConnection) apiUrl.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", "Bearer " + restClient.getSiteUrl()); // This will trigger token refresh in our auth
      conn.setRequestProperty("Accept", "application/json;odata=verbose");
      conn.setRequestProperty("Content-Type", "application/octet-stream");
      conn.setRequestProperty("Content-Length", String.valueOf(attachmentBytes.length));
      conn.setDoOutput(true);

      // Get fresh token
      org.apache.calcite.adapter.file.storage.SharePointCertificateTokenManager tokenManager =
          new org.apache.calcite.adapter.file.storage.SharePointCertificateTokenManager(
              tenantId, clientId, "../file/src/test/resources/SharePointAppOnlyCert.pfx",
              certPassword, siteUrl);
      String token = tokenManager.getAccessToken();
      conn.setRequestProperty("Authorization", "Bearer " + token);

      try (java.io.OutputStream os = conn.getOutputStream()) {
        os.write(attachmentBytes);
      }

      int responseCode = conn.getResponseCode();
      if (responseCode >= 200 && responseCode < 300) {
        System.out.println("✅ Attachment '" + fileName + "' uploaded successfully");

        // Verify the attachment exists
        String listAttachmentsUrl = siteUrl + "/_api/web/lists/getbytitle('" + listName + "')/items(" + itemId + ")/AttachmentFiles";
        JsonNode attachmentsResponse = restClient.executeRestCall("GET", listAttachmentsUrl, null);

        JsonNode results = attachmentsResponse.get("d").get("results");
        if (results.size() > 0) {
          System.out.println("✅ Verified attachment exists");
          for (JsonNode attachment : results) {
            String attachFileName = attachment.get("FileName").asText();
            String serverRelativeUrl = attachment.get("ServerRelativeUrl").asText();
            System.out.println("   - File: " + attachFileName);
            System.out.println("   - URL: " + siteUrl + serverRelativeUrl);
          }
        }

        System.out.println("\n🎯 SUCCESS! Test data created:");
        System.out.println("   📋 List Name: " + listName);
        System.out.println("   📄 Item ID: " + itemId);
        System.out.println("   📎 Attachment: " + fileName);
        System.out.println("\nYou can now test attachment functions with:");
        System.out.println("   SELECT * FROM TABLE(get_attachments('" + listName + "', '" + itemId + "'));");
        System.out.println("   SELECT get_attachment_content('" + listName + "', '" + itemId + "', '" + fileName + "');");

      } else {
        String error = "";
        try (java.io.InputStream errorStream = conn.getErrorStream()) {
          if (errorStream != null) {
            error = new String(errorStream.readAllBytes(), StandardCharsets.UTF_8);
          }
        }
        throw new IOException("Failed to upload attachment: HTTP " + responseCode + " - " + error);
      }

    } catch (Exception e) {
      System.err.println("Test failed: " + e.getMessage());
      e.printStackTrace();
      throw new RuntimeException("Failed to create test attachment: " + e.getMessage(), e);
    }
  }
}
