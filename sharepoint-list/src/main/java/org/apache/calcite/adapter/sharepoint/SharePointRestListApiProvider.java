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

import org.apache.calcite.adapter.sharepoint.auth.SharePointAuthProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * SharePoint REST API implementation of SharePointListApiProvider.
 * Provides access to SharePoint lists using the native REST API,
 * which is required for on-premises SharePoint deployments.
 */
public class SharePointRestListApiProvider implements SharePointListApiProvider {

  private final SharePointAuthProvider authProvider;
  private final ObjectMapper objectMapper;
  private final String siteUrl;
  private final String apiBase;

  /**
   * Creates a SharePointRestListApiProvider with an auth provider.
   */
  public SharePointRestListApiProvider(SharePointAuthProvider authProvider) {
    this.authProvider = authProvider;
    this.objectMapper = new ObjectMapper();
    this.siteUrl = authProvider.getSiteUrl();
    this.apiBase = siteUrl + (siteUrl.endsWith("/") ? "" : "/") + "_api";
  }

  @Override public List<SharePointListInfo> getListsInSite() throws IOException {
    String url = apiBase + "/web/lists?$filter=Hidden eq false";
    JsonNode response = executeRestCall("GET", url, null, null);

    List<SharePointListInfo> lists = new ArrayList<>();
    if (response.has("d") && response.get("d").has("results")) {
      JsonNode results = response.get("d").get("results");
      if (results.isArray()) {
        for (JsonNode list : results) {
          lists.add(
              new SharePointListInfo(
              list.get("Id").asText(),
              list.get("Title").asText(),
              list.has("DisplayName") ? list.get("DisplayName").asText() : list.get("Title").asText(),
              list.has("Description") ? list.get("Description").asText() : ""));
        }
      }
    }

    return lists;
  }

  @Override public ListSchemaInfo getListSchema(String listId) throws IOException {
    String url = apiBase + "/web/lists(guid'" + listId + "')/fields?$filter=Hidden eq false";
    JsonNode response = executeRestCall("GET", url, null, null);

    List<SharePointColumn> columns = new ArrayList<>();
    if (response.has("d") && response.get("d").has("results")) {
      JsonNode results = response.get("d").get("results");
      if (results.isArray()) {
        for (JsonNode field : results) {
          String internalName = field.get("InternalName").asText();
          String displayName = field.has("Title") ? field.get("Title").asText() : internalName;
          String fieldType = field.get("TypeAsString").asText();
          boolean required = field.has("Required") && field.get("Required").asBoolean();
          // SharePointColumn doesn't have readOnly parameter in constructor

          columns.add(new SharePointColumn(internalName, displayName, fieldType, required));
        }
      }
    }

    // Get list metadata
    String listUrl = apiBase + "/web/lists(guid'" + listId + "')";
    JsonNode listResponse = executeRestCall("GET", listUrl, null, null);
    JsonNode listData = listResponse.has("d") ? listResponse.get("d") : listResponse;

    String listName = listData.get("Title").asText();
    String displayName = listData.has("DisplayName") ? listData.get("DisplayName").asText() : listName;

    return new ListSchemaInfo(listName, displayName, columns);
  }

  @Override public List<Map<String, Object>> getListItems(String listId, String filter,
      String select, String orderBy, int top, int skip) throws IOException {

    StringBuilder urlBuilder = new StringBuilder(apiBase);
    urlBuilder.append("/web/lists(guid'").append(listId).append("')/items");

    // Build query parameters
    List<String> queryParts = new ArrayList<>();
    if (filter != null && !filter.isEmpty()) {
      queryParts.add("$filter=" + URLEncoder.encode(filter, StandardCharsets.UTF_8));
    }
    if (select != null && !select.isEmpty()) {
      queryParts.add("$select=" + URLEncoder.encode(select, StandardCharsets.UTF_8));
    }
    if (orderBy != null && !orderBy.isEmpty()) {
      queryParts.add("$orderby=" + URLEncoder.encode(orderBy, StandardCharsets.UTF_8));
    }
    if (top > 0) {
      queryParts.add("$top=" + top);
    }
    if (skip > 0) {
      queryParts.add("$skip=" + skip);
    }

    if (!queryParts.isEmpty()) {
      urlBuilder.append("?").append(String.join("&", queryParts));
    }

    JsonNode response = executeRestCall("GET", urlBuilder.toString(), null, null);

    List<Map<String, Object>> items = new ArrayList<>();
    if (response.has("d") && response.get("d").has("results")) {
      JsonNode results = response.get("d").get("results");
      if (results.isArray()) {
        for (JsonNode item : results) {
          Map<String, Object> itemMap = new HashMap<>();
          item.fields().forEachRemaining(field -> {
            String name = field.getKey();
            if (!name.startsWith("__")) { // Skip metadata fields
              itemMap.put(name, convertJsonValue(field.getValue()));
            }
          });
          items.add(itemMap);
        }
      }
    }

    return items;
  }

  @Override public Map<String, Object> getListItem(String listId, String itemId) throws IOException {
    String url = apiBase + "/web/lists(guid'" + listId + "')/items(" + itemId + ")";
    JsonNode response = executeRestCall("GET", url, null, null);

    Map<String, Object> itemMap = new HashMap<>();
    JsonNode item = response.has("d") ? response.get("d") : response;
    item.fields().forEachRemaining(field -> {
      String name = field.getKey();
      if (!name.startsWith("__")) { // Skip metadata fields
        itemMap.put(name, convertJsonValue(field.getValue()));
      }
    });

    return itemMap;
  }

  @Override public String createListItem(String listId, Map<String, Object> item) throws IOException {
    // Get list entity type (required for REST API)
    String listUrl = apiBase + "/web/lists(guid'" + listId + "')";
    JsonNode listResponse = executeRestCall("GET", listUrl, null, null);
    JsonNode listData = listResponse.has("d") ? listResponse.get("d") : listResponse;
    String entityType = listData.get("ListItemEntityTypeFullName").asText();

    // Build item JSON with metadata
    ObjectNode itemNode = objectMapper.createObjectNode();
    ObjectNode metadata = objectMapper.createObjectNode();
    metadata.put("type", entityType);
    itemNode.set("__metadata", metadata);

    // Add item fields
    for (Map.Entry<String, Object> entry : item.entrySet()) {
      itemNode.set(entry.getKey(), objectMapper.valueToTree(entry.getValue()));
    }

    String createUrl = apiBase + "/web/lists(guid'" + listId + "')/items";
    JsonNode response = executeRestCall("POST", createUrl, itemNode, null);

    JsonNode createdItem = response.has("d") ? response.get("d") : response;
    return createdItem.get("Id").asText();
  }

  @Override public void updateListItem(String listId, String itemId, Map<String, Object> updates)
      throws IOException {
    // Get list entity type (required for REST API)
    String listUrl = apiBase + "/web/lists(guid'" + listId + "')";
    JsonNode listResponse = executeRestCall("GET", listUrl, null, null);
    JsonNode listData = listResponse.has("d") ? listResponse.get("d") : listResponse;
    String entityType = listData.get("ListItemEntityTypeFullName").asText();

    // Build update JSON with metadata
    ObjectNode updateNode = objectMapper.createObjectNode();
    ObjectNode metadata = objectMapper.createObjectNode();
    metadata.put("type", entityType);
    updateNode.set("__metadata", metadata);

    // Add updated fields
    for (Map.Entry<String, Object> entry : updates.entrySet()) {
      updateNode.set(entry.getKey(), objectMapper.valueToTree(entry.getValue()));
    }

    String updateUrl = apiBase + "/web/lists(guid'" + listId + "')/items(" + itemId + ")";
    Map<String, String> headers = new HashMap<>();
    headers.put("IF-MATCH", "*");
    headers.put("X-HTTP-Method", "MERGE");

    executeRestCall("POST", updateUrl, updateNode, headers);
  }

  @Override public void deleteListItem(String listId, String itemId) throws IOException {
    String deleteUrl = apiBase + "/web/lists(guid'" + listId + "')/items(" + itemId + ")";
    Map<String, String> headers = new HashMap<>();
    headers.put("IF-MATCH", "*");
    headers.put("X-HTTP-Method", "DELETE");

    executeRestCall("POST", deleteUrl, null, headers);
  }

  @Override public String getApiType() {
    return "rest";
  }

  @Override public void close() {
    // No resources to close
  }

  /**
   * Executes a SharePoint REST API call.
   */
  private JsonNode executeRestCall(String method, String url, JsonNode body,
      Map<String, String> additionalHeaders) throws IOException {
    URL apiUrl = URI.create(url).toURL();
    HttpURLConnection conn = (HttpURLConnection) apiUrl.openConnection();
    conn.setRequestMethod(method);

    // Add authentication
    String accessToken = authProvider.getAccessToken();
    conn.setRequestProperty("Authorization", "Bearer " + accessToken);

    // Add standard headers
    conn.setRequestProperty("Accept", "application/json;odata=verbose");
    conn.setRequestProperty("Content-Type", "application/json;odata=verbose");

    // Add additional headers from auth provider
    Map<String, String> providerHeaders = authProvider.getAdditionalHeaders();
    for (Map.Entry<String, String> header : providerHeaders.entrySet()) {
      conn.setRequestProperty(header.getKey(), header.getValue());
    }

    // Add method-specific headers
    if (additionalHeaders != null) {
      for (Map.Entry<String, String> header : additionalHeaders.entrySet()) {
        conn.setRequestProperty(header.getKey(), header.getValue());
      }
    }

    // Send body if provided
    if (body != null) {
      conn.setDoOutput(true);
      try (OutputStream os = conn.getOutputStream()) {
        objectMapper.writeValue(os, body);
      }
    }

    // Check response
    int responseCode = conn.getResponseCode();
    if (responseCode >= 400) {
      String error;
      try (Scanner scanner = new Scanner(conn.getErrorStream(), StandardCharsets.UTF_8)) {
        error = scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
      }
      throw new IOException("SharePoint REST API error (HTTP " + responseCode + "): " + error);
    }

    // Parse response
    if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
      return objectMapper.createObjectNode(); // Empty response for DELETE
    }

    try (InputStream is = conn.getInputStream()) {
      return objectMapper.readTree(is);
    }
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
}
