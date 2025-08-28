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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL functions for handling SharePoint attachments.
 * These functions provide access to attachment operations that aren't suitable
 * for regular column storage due to their binary nature and size.
 */
public class SharePointAttachmentFunctions {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Gets attachments for a SharePoint list item.
   * Returns a table of (filename, url, size) for each attachment.
   *
   * @param context Data context containing connection info
   * @param listName Name of the SharePoint list
   * @param itemId ID of the item
   * @return Enumerable of attachment metadata
   */
  public static Enumerable<Object[]> getAttachments(
      final DataContext context,
      final String listName,
      final String itemId) {

    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        try {
          // Get the SharePoint schema from context
          SharePointListSchema schema = getSchema(context);

          List<Object[]> attachments;
          if (schema.useRestApi()) {
            // Use REST API
            SharePointRestListClient restClient = schema.getRestClient();
            attachments = restClient.getAttachments(listName, itemId);
          } else {
            // Use Graph API (default)
            MicrosoftGraphListClient client = schema.getClient();
            String listId = findListId(client, listName);
            if (listId == null) {
              return emptyEnumerator();
            }
            attachments = fetchAttachments(client, listId, itemId);
          }

          return new ListEnumerator<>(attachments);
        } catch (Exception e) {
          throw new RuntimeException("Failed to get attachments", e);
        }
      }
    };
  }

  /**
   * Adds an attachment to a SharePoint list item.
   *
   * @param context Data context
   * @param listName Name of the SharePoint list
   * @param itemId ID of the item
   * @param filename Name of the file to attach
   * @param content Binary content of the file
   * @return true if successful, false otherwise
   */
  public static boolean addAttachment(
      DataContext context,
      String listName,
      String itemId,
      String filename,
      byte[] content) {

    try {
      SharePointListSchema schema = getSchema(context);

      if (schema.useRestApi()) {
        // Use REST API
        SharePointRestListClient restClient = schema.getRestClient();
        return restClient.uploadAttachment(listName, itemId, filename, content);
      } else {
        // Use Graph API (default)
        MicrosoftGraphListClient client = schema.getClient();
        String listId = findListId(client, listName);
        if (listId == null) {
          return false;
        }
        return uploadAttachment(client, listId, itemId, filename, content);
      }

    } catch (Exception e) {
      throw new RuntimeException("Failed to add attachment: " + e.getMessage(), e);
    }
  }

  /**
   * Deletes an attachment from a SharePoint list item.
   *
   * @param context Data context
   * @param listName Name of the SharePoint list
   * @param itemId ID of the item
   * @param filename Name of the file to delete
   * @return true if successful, false otherwise
   */
  public static boolean deleteAttachment(
      DataContext context,
      String listName,
      String itemId,
      String filename) {

    try {
      SharePointListSchema schema = getSchema(context);

      if (schema.useRestApi()) {
        // Use REST API
        SharePointRestListClient restClient = schema.getRestClient();
        return restClient.deleteAttachment(listName, itemId, filename);
      } else {
        // Use Graph API (default)
        MicrosoftGraphListClient client = schema.getClient();
        String listId = findListId(client, listName);
        if (listId == null) {
          return false;
        }
        return removeAttachment(client, listId, itemId, filename);
      }

    } catch (Exception e) {
      throw new RuntimeException("Failed to delete attachment: " + e.getMessage(), e);
    }
  }

  /**
   * Gets the content of an attachment.
   *
   * @param context Data context
   * @param listName Name of the SharePoint list
   * @param itemId ID of the item
   * @param filename Name of the file
   * @return Binary content of the attachment
   */
  public static byte[] getAttachmentContent(
      DataContext context,
      String listName,
      String itemId,
      String filename) {

    try {
      SharePointListSchema schema = getSchema(context);

      if (schema.useRestApi()) {
        // Use REST API
        SharePointRestListClient restClient = schema.getRestClient();
        return restClient.downloadAttachment(listName, itemId, filename);
      } else {
        // Use Graph API (default)
        MicrosoftGraphListClient client = schema.getClient();
        String listId = findListId(client, listName);
        if (listId == null) {
          return null;
        }
        return downloadAttachment(client, listId, itemId, filename);
      }

    } catch (Exception e) {
      throw new RuntimeException("Failed to get attachment content: " + e.getMessage(), e);
    }
  }

  /**
   * Counts attachments for a SharePoint list item.
   *
   * @param context Data context
   * @param listName Name of the SharePoint list
   * @param itemId ID of the item
   * @return Number of attachments
   */
  public static int countAttachments(
      DataContext context,
      String listName,
      String itemId) {

    try {
      SharePointListSchema schema = getSchema(context);

      List<Object[]> attachments;
      if (schema.useRestApi()) {
        // Use REST API
        SharePointRestListClient restClient = schema.getRestClient();
        attachments = restClient.getAttachments(listName, itemId);
      } else {
        // Use Graph API (default)
        MicrosoftGraphListClient client = schema.getClient();
        String listId = findListId(client, listName);
        if (listId == null) {
          return 0;
        }
        attachments = fetchAttachments(client, listId, itemId);
      }

      return attachments.size();

    } catch (Exception e) {
      return 0; // Return 0 on error
    }
  }

  // Helper methods

  @SuppressWarnings("deprecation")
  private static SharePointListSchema getSchema(DataContext context) {
    // Get the schema from the data context
    org.apache.calcite.schema.Schema schema = context.getRootSchema().getSubSchema("sharepoint");
    if (schema instanceof SharePointListSchema) {
      return (SharePointListSchema) schema;
    }
    throw new IllegalStateException("SharePoint schema not found in context");
  }

  private static String findListId(MicrosoftGraphListClient client, String listName)
      throws IOException, InterruptedException {
    Map<String, SharePointListMetadata> lists = client.getAvailableLists();
    for (SharePointListMetadata metadata : lists.values()) {
      if (metadata.getListName().equalsIgnoreCase(listName) ||
          metadata.getDisplayName().equalsIgnoreCase(listName)) {
        return metadata.getListId();
      }
    }
    return null;
  }

  private static List<Object[]> fetchAttachments(
      MicrosoftGraphListClient client,
      String listId,
      String itemId) throws IOException, InterruptedException {

    List<Object[]> result = new ArrayList<>();

    // Call Graph API to get attachments
    // GET /sites/{site-id}/lists/{list-id}/items/{item-id}/attachments
    String url =
        String.format("%s/sites/%s/lists/%s/items/%s/attachments", client.getGraphApiBase(), client.getSiteId(), listId, itemId);

    JsonNode response = client.executeGraphCall("GET", url, null);

    if (response.has("value")) {
      for (JsonNode attachment : response.get("value")) {
        String filename = attachment.get("name").asText();
        String attachUrl = attachment.get("webUrl").asText();
        long size = attachment.has("size") ? attachment.get("size").asLong() : 0;

        result.add(new Object[]{filename, attachUrl, size});
      }
    }

    return result;
  }

  private static boolean uploadAttachment(
      MicrosoftGraphListClient client,
      String listId,
      String itemId,
      String filename,
      byte[] content) throws IOException, InterruptedException {

    // POST /sites/{site-id}/lists/{list-id}/items/{item-id}/attachments/add
    String url =
        String.format("%s/sites/%s/lists/%s/items/%s/attachments/add", client.getGraphApiBase(), client.getSiteId(), listId, itemId);

    // Create request body with filename and content
    Map<String, Object> body = new HashMap<>();
    body.put("name", filename);
    body.put("content", Base64.getEncoder().encodeToString(content));

    JsonNode requestBody = OBJECT_MAPPER.valueToTree(body);
    JsonNode response = client.executeGraphCall("POST", url, requestBody);

    return response != null;
  }

  private static boolean removeAttachment(
      MicrosoftGraphListClient client,
      String listId,
      String itemId,
      String filename) throws IOException, InterruptedException {

    // DELETE /sites/{site-id}/lists/{list-id}/items/{item-id}/attachments/{filename}
    String url =
        String.format("%s/sites/%s/lists/%s/items/%s/attachments/%s", client.getGraphApiBase(), client.getSiteId(), listId, itemId, filename);

    client.executeGraphCall("DELETE", url, null);
    return true;
  }

  private static byte[] downloadAttachment(
      MicrosoftGraphListClient client,
      String listId,
      String itemId,
      String filename) throws IOException, InterruptedException {

    // GET /sites/{site-id}/lists/{list-id}/items/{item-id}/attachments/{filename}/$value
    String url =
        String.format("%s/sites/%s/lists/%s/items/%s/attachments/%s/$value", client.getGraphApiBase(), client.getSiteId(), listId, itemId, filename);

    // Direct binary download
    return client.downloadBinary(url);
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

    @Override public T current() {
      return list.get(index);
    }

    @Override public boolean moveNext() {
      return ++index < list.size();
    }

    @Override public void reset() {
      index = -1;
    }

    @Override public void close() {
      // Nothing to close
    }
  }
}
