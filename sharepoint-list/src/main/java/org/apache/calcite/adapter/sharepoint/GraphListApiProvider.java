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
import org.apache.calcite.adapter.sharepoint.auth.SharePointAuthProvider;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Microsoft Graph API implementation of SharePointListApiProvider.
 * Wraps the existing MicrosoftGraphListClient to provide the standard API interface.
 */
public class GraphListApiProvider implements SharePointListApiProvider {

  private final MicrosoftGraphListClient graphClient;
  private final SharePointAuthProvider authProvider;

  /**
   * Creates a GraphListApiProvider with an auth provider.
   */
  public GraphListApiProvider(SharePointAuthProvider authProvider) {
    this.authProvider = authProvider;

    // Create a SharePointAuth adapter for the existing client
    SharePointAuth auth = new SharePointAuthAdapter(authProvider);
    this.graphClient = new MicrosoftGraphListClient(authProvider.getSiteUrl(), auth);
  }

  @Override public List<SharePointListInfo> getListsInSite() throws IOException {
    try {
      Map<String, SharePointListMetadata> lists = graphClient.getAvailableLists();
      List<SharePointListInfo> result = new ArrayList<>();

      for (Map.Entry<String, SharePointListMetadata> entry : lists.entrySet()) {
        SharePointListMetadata metadata = entry.getValue();
        result.add(
            new SharePointListInfo(
            metadata.getListId(),
            metadata.getListName(),
            metadata.getDisplayName(),
            "" // Description not available in SharePointListMetadata));
      }

      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while fetching lists", e);
    }
  }

  @Override public ListSchemaInfo getListSchema(String listId) throws IOException {
    try {
      SharePointListMetadata metadata = graphClient.getListMetadataById(listId);
      return new ListSchemaInfo(
          metadata.getListName(),
          metadata.getDisplayName(),
          metadata.getColumns());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while fetching list schema", e);
    }
  }

  @Override public List<Map<String, Object>> getListItems(String listId, String filter,
      String select, String orderBy, int top, int skip) throws IOException {
    try {
      // The existing MicrosoftGraphListClient has limited query support
      // Using the available method signature
      List<Map<String, Object>> allItems;
      if (filter != null || select != null) {
        allItems = graphClient.getListItems(listId, filter, select, top > 0 ? top : 100);
      } else {
        allItems = graphClient.getListItems(listId);
      }

      // Apply skip if needed (client-side pagination)
      if (skip > 0 && allItems.size() > skip) {
        allItems = allItems.subList(skip, allItems.size());
      }

      // Apply top limit if not already done
      if (top > 0 && allItems.size() > top) {
        allItems = allItems.subList(0, top);
      }

      return allItems;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while fetching list items", e);
    }
  }

  @Override public Map<String, Object> getListItem(String listId, String itemId) throws IOException {
    try {
      // MicrosoftGraphListClient doesn't have a getListItem method
      // We'll use filter to get the specific item
      String filter = "id eq " + itemId;
      List<Map<String, Object>> items = graphClient.getListItems(listId, filter, null, 1);

      if (items.isEmpty()) {
        throw new IOException("Item not found: " + itemId);
      }

      return items.get(0);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while fetching list item", e);
    }
  }

  @Override public String createListItem(String listId, Map<String, Object> item) throws IOException {
    try {
      String itemId = graphClient.createListItem(listId, item);
      return itemId;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while creating list item", e);
    }
  }

  @Override public void updateListItem(String listId, String itemId, Map<String, Object> updates)
      throws IOException {
    try {
      graphClient.updateListItem(listId, itemId, updates);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while updating list item", e);
    }
  }

  @Override public void deleteListItem(String listId, String itemId) throws IOException {
    try {
      graphClient.deleteListItem(listId, itemId);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while deleting list item", e);
    }
  }

  @Override public String getApiType() {
    return "graph";
  }

  @Override public void close() {
    // No resources to close
  }

  /**
   * Converts a JsonNode value to appropriate Java type.
   */
  private Object convertJsonValue(JsonNode node) {
    if (node.isNull()) {
      return null;
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else if (node.isNumber()) {
      if (node.isInt()) {
        return node.asInt();
      } else if (node.isLong()) {
        return node.asLong();
      } else {
        return node.asDouble();
      }
    } else if (node.isArray()) {
      List<Object> list = new ArrayList<>();
      for (JsonNode element : node) {
        list.add(convertJsonValue(element));
      }
      return list;
    } else if (node.isObject()) {
      Map<String, Object> map = new HashMap<>();
      node.fields().forEachRemaining(field -> {
        map.put(field.getKey(), convertJsonValue(field.getValue()));
      });
      return map;
    } else {
      return node.asText();
    }
  }

  /**
   * Adapter to bridge SharePointAuthProvider to SharePointAuth interface.
   */
  private static class SharePointAuthAdapter implements SharePointAuth {
    private final SharePointAuthProvider provider;

    SharePointAuthAdapter(SharePointAuthProvider provider) {
      this.provider = provider;
    }

    @Override public String getAccessToken() throws IOException, InterruptedException {
      return provider.getAccessToken();
    }

    public boolean isConfigured() {
      return true;
    }
  }
}
