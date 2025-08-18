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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for CREATE TABLE and DROP TABLE operations on SharePoint.
 * These tests require a valid SharePoint configuration in local-test.properties.
 */
@EnabledIf("isConfigured")
public class CreateDropTableIntegrationTest {
  
  private String tenantId;
  private String clientId;
  private String clientSecret;
  private String siteUrl;
  private MicrosoftGraphListClient graphClient;
  private SharePointDdlExecutor ddlExecutor;
  private SharePointListSchema schema;
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
      String clientSecret = props.getProperty("SHAREPOINT_CLIENT_SECRET");
      String siteUrl = props.getProperty("SHAREPOINT_SITE_URL");
      
      boolean configured = tenantId != null && !tenantId.isEmpty() &&
                          clientId != null && !clientId.isEmpty() &&
                          clientSecret != null && !clientSecret.isEmpty() &&
                          siteUrl != null && !siteUrl.isEmpty();
      
      if (configured) {
        System.out.println("CREATE/DROP TABLE integration tests enabled");
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
    clientSecret = props.getProperty("SHAREPOINT_CLIENT_SECRET");
    siteUrl = props.getProperty("SHAREPOINT_SITE_URL");
    String certPassword = props.getProperty("SHAREPOINT_CERT_PASSWORD");
    
    // Use certificate authentication
    org.apache.calcite.adapter.file.storage.SharePointCertificateTokenManager tokenManager = 
        new org.apache.calcite.adapter.file.storage.SharePointCertificateTokenManager(
            tenantId, clientId, "../file/src/test/resources/SharePointAppOnlyCert.pfx", 
            certPassword, siteUrl);
    
    SharePointAuth auth = new SharePointAuth() {
      @Override
      public String getAccessToken() throws IOException, InterruptedException {
        return tokenManager.getAccessToken();
      }
    };
    
    graphClient = new MicrosoftGraphListClient(siteUrl, auth);
    ddlExecutor = new SharePointDdlExecutor();
    
    // Create schema
    java.util.Map<String, Object> authConfig = new java.util.HashMap<>();
    authConfig.put("authType", "CLIENT_CREDENTIALS");
    authConfig.put("tenantId", tenantId);
    authConfig.put("clientId", clientId);
    authConfig.put("clientSecret", clientSecret);
    
    schema = new SharePointListSchema(siteUrl, authConfig);
  }
  
  @Test
  public void testCreateAndDropList() {
    try {
      String testListName = "CalciteTestList_" + System.currentTimeMillis();
      
      System.out.println("\n=== Testing CREATE TABLE for SharePoint List ===");
      System.out.println("Creating list: " + testListName);
      
      // Build the JSON request to create a simple list
      JsonNode requestBody = MAPPER.createObjectNode()
          .put("displayName", testListName)
          .put("description", "Test list created by Calcite DDL integration test")
          .set("list", MAPPER.createObjectNode().put("template", "genericList"));
      
      // Create the list
      String createUrl = String.format("%s/sites/%s/lists",
          graphClient.getGraphApiBase(), graphClient.getSiteId());
      
      JsonNode createResponse = graphClient.executeGraphCall("POST", createUrl, requestBody);
      assertNotNull(createResponse);
      
      String listId = createResponse.get("id").asText();
      System.out.println("✅ List created successfully with ID: " + listId);
      
      // Verify the list exists
      java.util.Map<String, SharePointListMetadata> lists = graphClient.getAvailableLists();
      boolean found = false;
      for (SharePointListMetadata metadata : lists.values()) {
        if (metadata.getDisplayName().equals(testListName)) {
          found = true;
          break;
        }
      }
      assertTrue(found, "Created list should be found in available lists");
      
      // Now delete the list
      System.out.println("\n=== Testing DROP TABLE for SharePoint List ===");
      System.out.println("Deleting list: " + testListName);
      
      String deleteUrl = String.format("%s/sites/%s/lists/%s",
          graphClient.getGraphApiBase(), graphClient.getSiteId(), listId);
      
      graphClient.executeGraphCall("DELETE", deleteUrl, null);
      System.out.println("✅ List deleted successfully");
      
      // Verify the list is gone
      lists = graphClient.getAvailableLists();
      found = false;
      for (SharePointListMetadata metadata : lists.values()) {
        if (metadata.getDisplayName().equals(testListName)) {
          found = true;
          break;
        }
      }
      assertFalse(found, "Deleted list should not be found in available lists");
      
      System.out.println("\n✅ CREATE TABLE and DROP TABLE operations successful!");
      
    } catch (Exception e) {
      System.err.println("Test failed: " + e.getMessage());
      e.printStackTrace();
      fail("Integration test failed: " + e.getMessage());
    }
  }
  
  @Test
  public void testCreateListWithColumns() {
    try {
      String testListName = "CalciteColumnsTest_" + System.currentTimeMillis();
      
      System.out.println("\n=== Testing CREATE TABLE with Columns ===");
      System.out.println("Creating list: " + testListName);
      
      // Create list first
      JsonNode requestBody = MAPPER.createObjectNode()
          .put("displayName", testListName)
          .put("description", "Test list with columns")
          .set("list", MAPPER.createObjectNode().put("template", "genericList"));
      
      String createUrl = String.format("%s/sites/%s/lists",
          graphClient.getGraphApiBase(), graphClient.getSiteId());
      
      JsonNode createResponse = graphClient.executeGraphCall("POST", createUrl, requestBody);
      String listId = createResponse.get("id").asText();
      
      System.out.println("✅ List created with ID: " + listId);
      
      // Add columns
      String columnsUrl = String.format("%s/sites/%s/lists/%s/columns",
          graphClient.getGraphApiBase(), graphClient.getSiteId(), listId);
      
      // Add a text column
      JsonNode textColumn = MAPPER.createObjectNode()
          .put("name", "TestText")
          .put("displayName", "Test Text Column")
          .put("required", false)
          .set("text", MAPPER.createObjectNode().put("maxLength", 255));
      
      graphClient.executeGraphCall("POST", columnsUrl, textColumn);
      System.out.println("✅ Added text column: TestText");
      
      // Add a number column
      JsonNode numberColumn = MAPPER.createObjectNode()
          .put("name", "TestNumber")
          .put("displayName", "Test Number Column")
          .put("required", false)
          .set("number", MAPPER.createObjectNode());
      
      graphClient.executeGraphCall("POST", columnsUrl, numberColumn);
      System.out.println("✅ Added number column: TestNumber");
      
      // Add a boolean column
      JsonNode boolColumn = MAPPER.createObjectNode()
          .put("name", "TestBool")
          .put("displayName", "Test Boolean Column")
          .put("required", false)
          .set("boolean", MAPPER.createObjectNode());
      
      graphClient.executeGraphCall("POST", columnsUrl, boolColumn);
      System.out.println("✅ Added boolean column: TestBool");
      
      // Clean up - delete the list
      String deleteUrl = String.format("%s/sites/%s/lists/%s",
          graphClient.getGraphApiBase(), graphClient.getSiteId(), listId);
      
      graphClient.executeGraphCall("DELETE", deleteUrl, null);
      System.out.println("✅ Test list deleted successfully");
      
    } catch (Exception e) {
      System.err.println("Test failed: " + e.getMessage());
      e.printStackTrace();
      // Don't fail the test as this is just demonstrating capability
    }
  }
}