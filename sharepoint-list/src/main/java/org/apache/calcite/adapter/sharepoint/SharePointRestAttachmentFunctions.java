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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SQL functions for handling SharePoint attachments using SharePoint REST API.
 * These functions provide access to attachment operations using native SharePoint REST API
 * instead of Microsoft Graph API.
 */
public class SharePointRestAttachmentFunctions {
  
  /**
   * Gets attachments for a SharePoint list item using REST API.
   * Returns a table of (filename, url, size) for each attachment.
   * 
   * @param context Data context containing connection info
   * @param listName Name of the SharePoint list
   * @param itemId ID of the item
   * @return Enumerable of attachment metadata
   */
  public static Enumerable<Object[]> getAttachmentsRest(
      final DataContext context,
      final String listName,
      final String itemId) {
    
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        try {
          // Get the SharePoint REST client from context
          SharePointRestListClient client = getRestClient(context);
          
          // Fetch attachments via REST API
          List<Object[]> attachments = client.getAttachments(listName, itemId);
          
          return new ListEnumerator<>(attachments);
        } catch (Exception e) {
          throw new RuntimeException("Failed to get attachments via REST API", e);
        }
      }
    };
  }
  
  /**
   * Adds an attachment to a SharePoint list item using REST API.
   * 
   * @param context Data context
   * @param listName Name of the SharePoint list
   * @param itemId ID of the item
   * @param filename Name of the file to attach
   * @param content Binary content of the file
   * @return true if successful, false otherwise
   */
  public static boolean addAttachmentRest(
      DataContext context,
      String listName,
      String itemId,
      String filename,
      byte[] content) {
    
    try {
      SharePointRestListClient client = getRestClient(context);
      
      // Upload attachment via REST API
      return client.uploadAttachment(listName, itemId, filename, content);
      
    } catch (Exception e) {
      throw new RuntimeException("Failed to add attachment via REST API: " + e.getMessage(), e);
    }
  }
  
  /**
   * Deletes an attachment from a SharePoint list item using REST API.
   * 
   * @param context Data context
   * @param listName Name of the SharePoint list
   * @param itemId ID of the item
   * @param filename Name of the file to delete
   * @return true if successful, false otherwise
   */
  public static boolean deleteAttachmentRest(
      DataContext context,
      String listName,
      String itemId,
      String filename) {
    
    try {
      SharePointRestListClient client = getRestClient(context);
      
      // Delete attachment via REST API
      return client.deleteAttachment(listName, itemId, filename);
      
    } catch (Exception e) {
      throw new RuntimeException("Failed to delete attachment via REST API: " + e.getMessage(), e);
    }
  }
  
  /**
   * Gets the content of an attachment using REST API.
   * 
   * @param context Data context
   * @param listName Name of the SharePoint list
   * @param itemId ID of the item
   * @param filename Name of the file
   * @return Binary content of the attachment
   */
  public static byte[] getAttachmentContentRest(
      DataContext context,
      String listName,
      String itemId,
      String filename) {
    
    try {
      SharePointRestListClient client = getRestClient(context);
      
      // Download attachment content
      return client.downloadAttachment(listName, itemId, filename);
      
    } catch (Exception e) {
      throw new RuntimeException("Failed to get attachment content via REST API: " + e.getMessage(), e);
    }
  }
  
  /**
   * Counts attachments for a SharePoint list item using REST API.
   * 
   * @param context Data context
   * @param listName Name of the SharePoint list
   * @param itemId ID of the item
   * @return Number of attachments
   */
  public static int countAttachmentsRest(
      DataContext context,
      String listName,
      String itemId) {
    
    try {
      SharePointRestListClient client = getRestClient(context);
      
      // Count attachments
      List<Object[]> attachments = client.getAttachments(listName, itemId);
      return attachments.size();
      
    } catch (Exception e) {
      return 0; // Return 0 on error
    }
  }
  
  // Helper methods
  
  /**
   * Gets the SharePoint REST client from the data context.
   * This method needs to be implemented based on how REST client is stored in context.
   */
  @SuppressWarnings("deprecation")
  private static SharePointRestListClient getRestClient(DataContext context) {
    // Get the schema from the data context
    org.apache.calcite.schema.Schema schema = context.getRootSchema().getSubSchema("sharepoint");
    if (schema instanceof SharePointListSchema) {
      SharePointListSchema spSchema = (SharePointListSchema) schema;
      
      // For now, create a REST client using the existing Graph client's auth
      // In a real implementation, you'd want to configure this properly
      MicrosoftGraphListClient graphClient = spSchema.getClient();
      
      // Extract site URL and auth from the graph client
      // This is a simplified approach - you'd want proper configuration
      String siteUrl = extractSiteUrl(graphClient);
      org.apache.calcite.adapter.sharepoint.auth.SharePointAuth auth = extractAuth(graphClient);
      
      return new SharePointRestListClient(siteUrl, auth);
    }
    throw new IllegalStateException("SharePoint schema not found in context");
  }
  
  /**
   * Extracts site URL from Graph client.
   * This is a placeholder - in real implementation you'd have proper configuration.
   */
  private static String extractSiteUrl(MicrosoftGraphListClient graphClient) {
    // For demonstration purposes, assume a standard SharePoint URL
    // In real implementation, this would be properly configured
    return "https://example.sharepoint.com/sites/example";
  }
  
  /**
   * Extracts authentication from Graph client.
   * This is a placeholder - in real implementation you'd have proper configuration.
   */
  private static org.apache.calcite.adapter.sharepoint.auth.SharePointAuth extractAuth(MicrosoftGraphListClient graphClient) {
    // For demonstration purposes, create a dummy auth
    // In real implementation, this would use the same auth as the Graph client
    return new org.apache.calcite.adapter.sharepoint.auth.SharePointAuth() {
      @Override
      public String getAccessToken() throws java.io.IOException, InterruptedException {
        // This should use the same token as the Graph client
        // For now, return a placeholder
        return "placeholder-token";
      }
    };
  }
  
  private static <T> Enumerator<T> emptyEnumerator() {
    return new Enumerator<T>() {
      @Override public T current() { return null; }
      @Override public boolean moveNext() { return false; }
      @Override public void reset() { }
      @Override public void close() { }
    };
  }
  
  /**
   * Simple list-based enumerator.
   */
  private static class ListEnumerator<T> implements Enumerator<T> {
    private final List<T> list;
    private int index = -1;
    
    ListEnumerator(List<T> list) {
      this.list = list;
    }
    
    @Override
    public T current() {
      return list.get(index);
    }
    
    @Override
    public boolean moveNext() {
      return ++index < list.size();
    }
    
    @Override
    public void reset() {
      index = -1;
    }
    
    @Override
    public void close() {
      // Nothing to close
    }
  }
}