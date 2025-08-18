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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Microsoft Graph API client for SharePoint Lists operations.
 * Provides full CRUD operations on lists and list items.
 */
public class MicrosoftGraphListClient {
  private static final String GRAPH_API_BASE = "https://graph.microsoft.com/v1.0";
  private static final int MAX_BATCH_SIZE = 20; // Graph API batch limit

  private final String siteUrl;
  private final SharePointAuth authenticator;
  private final ObjectMapper objectMapper;
  private String siteId;

  public MicrosoftGraphListClient(String siteUrl, SharePointAuth authenticator) {
    this.siteUrl = siteUrl.endsWith("/") ? siteUrl.substring(0, siteUrl.length() - 1) : siteUrl;
    this.authenticator = authenticator;
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Ensures site ID is initialized.
   */
  private void ensureInitialized() throws IOException, InterruptedException {
    if (siteId == null) {
      initializeSite();
    }
  }

  /**
   * Initializes site ID from the site URL.
   */
  private void initializeSite() throws IOException, InterruptedException {
    URI uri = URI.create(siteUrl);
    String hostname = uri.getHost();
    String sitePath = uri.getPath();

    String siteApiUrl;
    if (sitePath == null || sitePath.isEmpty() || sitePath.equals("/")) {
      siteApiUrl = String.format(Locale.ROOT, "%s/sites/%s", GRAPH_API_BASE, hostname);
    } else {
      siteApiUrl = String.format(Locale.ROOT, "%s/sites/%s:%s", GRAPH_API_BASE, hostname, sitePath);
    }

    JsonNode response = executeGraphCall("GET", siteApiUrl, null);
    this.siteId = response.get("id").asText();
  }

  /**
   * Gets all available SharePoint lists.
   */
  public Map<String, SharePointListMetadata> getAvailableLists()
      throws IOException, InterruptedException {
    ensureInitialized();

    // First get the lists
    String url = String.format(Locale.ROOT, "%s/sites/%s/lists", GRAPH_API_BASE, siteId);

    Map<String, SharePointListMetadata> lists = new LinkedHashMap<>();

    // Handle pagination
    String nextLink = url;
    while (nextLink != null) {
      JsonNode response = executeGraphCall("GET", nextLink, null);
      JsonNode value = response.get("value");

      if (value != null && value.isArray()) {
        for (JsonNode list : value) {
          // Skip hidden lists
          if (list.has("list") && list.get("list").has("hidden")
              && list.get("list").get("hidden").asBoolean()) {
            continue;
          }

          String listId = list.get("id").asText();
          String displayName = list.get("displayName").asText();
          String entityTypeName = list.get("name").asText();

          // Get columns separately
          List<SharePointColumn> columns = getListColumns(listId);
          SharePointListMetadata metadata = new SharePointListMetadata(listId, displayName, entityTypeName, columns);
          // Use SQL-friendly name as the key
          lists.put(metadata.getListName(), metadata);
        }
      }

      // Check for next page
      nextLink = response.has("@odata.nextLink") ? response.get("@odata.nextLink").asText() : null;
    }

    return lists;
  }

  /**
   * Gets columns for a specific list.
   */
  private List<SharePointColumn> getListColumns(String listId)
      throws IOException, InterruptedException {
    String url =
        String.format(Locale.ROOT, "%s/sites/%s/lists/%s/columns?$filter=hidden%%20eq%%20false", GRAPH_API_BASE, siteId, listId);

    JsonNode response = executeGraphCall("GET", url, null);
    return parseColumns(response.get("value"));
  }

  /**
   * Gets metadata for a specific list by ID.
   */
  public SharePointListMetadata getListMetadataById(String listId)
      throws IOException, InterruptedException {
    ensureInitialized();

    // Get list info
    String url = String.format(Locale.ROOT, "%s/sites/%s/lists/%s", GRAPH_API_BASE, siteId, listId);
    JsonNode response = executeGraphCall("GET", url, null);

    String displayName = response.get("displayName").asText();
    String entityTypeName = response.get("name").asText();

    // Get columns separately
    List<SharePointColumn> columns = getListColumns(listId);

    return new SharePointListMetadata(listId, displayName, entityTypeName, columns);
  }

  /**
   * Gets all items from a list.
   */
  public List<Map<String, Object>> getListItems(String listId)
      throws IOException, InterruptedException {
    return getListItems(listId, null, null, 5000);
  }

  /**
   * Gets items from a list with filtering and pagination.
   */
  public List<Map<String, Object>> getListItems(String listId, String filter, String select, int top)
      throws IOException, InterruptedException {
    ensureInitialized();

    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(
        String.format(Locale.ROOT, "%s/sites/%s/lists/%s/items?$expand=fields",
        GRAPH_API_BASE, siteId, listId));

    if (top > 0) {
      urlBuilder.append("&$top=").append(top);
    }

    if (filter != null && !filter.isEmpty()) {
      urlBuilder.append("&$filter=").append(URLEncoder.encode(filter, StandardCharsets.UTF_8));
    }

    if (select != null && !select.isEmpty()) {
      urlBuilder.append("&$select=").append(URLEncoder.encode(select, StandardCharsets.UTF_8));
    }

    List<Map<String, Object>> allItems = new ArrayList<>();
    String nextLink = urlBuilder.toString();

    while (nextLink != null) {
      JsonNode response = executeGraphCall("GET", nextLink, null);
      JsonNode value = response.get("value");

      if (value != null && value.isArray()) {
        for (JsonNode item : value) {
          Map<String, Object> itemMap = new LinkedHashMap<>();
          itemMap.put("id", item.get("id").asText());

          // Extract fields
          JsonNode fields = item.get("fields");
          if (fields != null) {
            fields.fields().forEachRemaining(entry -> {
              itemMap.put(entry.getKey(), convertJsonValue(entry.getValue()));
            });
          }

          allItems.add(itemMap);
        }
      }

      nextLink = response.has("@odata.nextLink") ? response.get("@odata.nextLink").asText() : null;
    }

    return allItems;
  }

  /**
   * Creates a new list item.
   */
  public String createListItem(String listId, Map<String, Object> fields)
      throws IOException, InterruptedException {
    ensureInitialized();

    String url =
        String.format(Locale.ROOT, "%s/sites/%s/lists/%s/items", GRAPH_API_BASE, siteId, listId);

    ObjectNode requestBody = objectMapper.createObjectNode();
    ObjectNode fieldsNode = requestBody.putObject("fields");

    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      addFieldToNode(fieldsNode, entry.getKey(), entry.getValue());
    }

    JsonNode response = executeGraphCall("POST", url, requestBody);
    return response.get("id").asText();
  }

  /**
   * Updates an existing list item.
   */
  public void updateListItem(String listId, String itemId, Map<String, Object> fields)
      throws IOException, InterruptedException {
    ensureInitialized();

    String url =
        String.format(Locale.ROOT, "%s/sites/%s/lists/%s/items/%s", GRAPH_API_BASE, siteId, listId, itemId);

    ObjectNode requestBody = objectMapper.createObjectNode();
    ObjectNode fieldsNode = requestBody.putObject("fields");

    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      addFieldToNode(fieldsNode, entry.getKey(), entry.getValue());
    }

    executeGraphCall("PATCH", url, requestBody);
  }

  /**
   * Deletes a list item.
   */
  public void deleteListItem(String listId, String itemId)
      throws IOException, InterruptedException {
    ensureInitialized();

    String url =
        String.format(Locale.ROOT, "%s/sites/%s/lists/%s/items/%s", GRAPH_API_BASE, siteId, listId, itemId);

    executeGraphCall("DELETE", url, null);
  }

  /**
   * Creates a new SharePoint list.
   * @param sqlName SQL-friendly name (will be converted to SharePoint display name)
   * @param columns List columns
   */
  public SharePointListMetadata createList(String sqlName, List<SharePointColumn> columns)
      throws IOException, InterruptedException {
    ensureInitialized();

    String url = String.format(Locale.ROOT, "%s/sites/%s/lists", GRAPH_API_BASE, siteId);

    // Convert SQL name to SharePoint display name
    String displayName = SharePointNameConverter.toSharePointName(sqlName);

    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("displayName", displayName);
    requestBody.putObject("list").put("template", "genericList");

    // Add columns
    if (columns != null && !columns.isEmpty()) {
      com.fasterxml.jackson.databind.node.ArrayNode columnsNode = requestBody.putArray("columns");
      for (SharePointColumn column : columns) {
        ObjectNode columnNode = objectMapper.createObjectNode();
        columnNode.put("name", column.getInternalName());
        columnNode.put("displayName", column.getDisplayName());
        columnNode.put("required", column.isRequired());

        // Map column type
        switch (column.getType().toLowerCase(Locale.ROOT)) {
        case "text":
          columnNode.putObject("text");
          break;
        case "number":
          columnNode.putObject("number");
          break;
        case "boolean":
          columnNode.putObject("boolean");
          break;
        case "datetime":
          columnNode.putObject("dateTime");
          break;
        case "choice":
          columnNode.putObject("choice").putArray("choices");
          break;
        default:
          columnNode.putObject("text");
        }

        columnsNode.add(columnNode);
      }
    }

    JsonNode response = executeGraphCall("POST", url, requestBody);
    String listId = response.get("id").asText();

    return getListMetadataById(listId);
  }

  /**
   * Deletes a SharePoint list.
   */
  public void deleteList(String listId) throws IOException, InterruptedException {
    ensureInitialized();

    String url =
        String.format(Locale.ROOT, "%s/sites/%s/lists/%s", GRAPH_API_BASE, siteId, listId);

    executeGraphCall("DELETE", url, null);
  }

  /**
   * Batch insert items for better performance.
   */
  public void batchInsertItems(String listId, List<Map<String, Object>> items)
      throws IOException, InterruptedException {
    ensureInitialized();

    // Process in batches
    for (int i = 0; i < items.size(); i += MAX_BATCH_SIZE) {
      int end = Math.min(i + MAX_BATCH_SIZE, items.size());
      List<Map<String, Object>> batch = items.subList(i, end);

      ObjectNode batchRequest = objectMapper.createObjectNode();
      com.fasterxml.jackson.databind.node.ArrayNode requests = batchRequest.putArray("requests");

      for (int j = 0; j < batch.size(); j++) {
        ObjectNode request = objectMapper.createObjectNode();
        request.put("id", String.valueOf(j + 1));
        request.put("method", "POST");
        request.put("url", String.format(Locale.ROOT, "/sites/%s/lists/%s/items", siteId, listId));

        ObjectNode body = request.putObject("body");
        ObjectNode fields = body.putObject("fields");

        for (Map.Entry<String, Object> entry : batch.get(j).entrySet()) {
          addFieldToNode(fields, entry.getKey(), entry.getValue());
        }

        request.putObject("headers").put("Content-Type", "application/json");
        requests.add(request);
      }

      executeGraphCall("POST", GRAPH_API_BASE + "/$batch", batchRequest);
    }
  }

  /**
   * Gets the Graph API base URL.
   */
  public String getGraphApiBase() {
    return GRAPH_API_BASE;
  }
  
  /**
   * Gets the site ID.
   */
  public String getSiteId() throws IOException, InterruptedException {
    ensureInitialized();
    return siteId;
  }
  
  /**
   * Downloads binary content from a URL.
   */
  public byte[] downloadBinary(String url) throws IOException, InterruptedException {
    URL apiUrl = URI.create(url).toURL();
    HttpURLConnection conn = (HttpURLConnection) apiUrl.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization", "Bearer " + authenticator.getAccessToken());
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    
    int responseCode = conn.getResponseCode();
    if (responseCode >= 200 && responseCode < 300) {
      try (InputStream in = conn.getInputStream()) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[8192];
        int bytesRead;
        while ((bytesRead = in.read(data)) != -1) {
          buffer.write(data, 0, bytesRead);
        }
        return buffer.toByteArray();
      }
    } else {
      throw new IOException("Failed to download binary: HTTP " + responseCode);
    }
  }
  
  /**
   * Executes a Microsoft Graph API call.
   */
  public JsonNode executeGraphCall(String method, String url, JsonNode requestBody)
      throws IOException, InterruptedException {
    URL apiUrl = URI.create(url).toURL();
    HttpURLConnection conn = (HttpURLConnection) apiUrl.openConnection();
    conn.setRequestMethod(method);
    conn.setRequestProperty("Authorization", "Bearer " + authenticator.getAccessToken());
    conn.setRequestProperty("Accept", "application/json");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);

    if (requestBody != null) {
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      objectMapper.writeValue(conn.getOutputStream(), requestBody);
    }

    int responseCode = conn.getResponseCode();

    // Handle no content response
    if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
      return objectMapper.createObjectNode();
    }

    if (responseCode >= 200 && responseCode < 300) {
      try (InputStream in = conn.getInputStream()) {
        return objectMapper.readTree(in);
      }
    } else {
      String error = "";
      try (InputStream errorStream = conn.getErrorStream()) {
        if (errorStream != null) {
          JsonNode errorJson = objectMapper.readTree(errorStream);
          if (errorJson.has("error") && errorJson.get("error").has("message")) {
            error = errorJson.get("error").get("message").asText();
          }
        }
      }
      throw new IOException("Microsoft Graph API error: HTTP " + responseCode +
          (error.isEmpty() ? "" : " - " + error));
    }
  }

  /**
   * Parses column definitions from Graph API response.
   */
  private List<SharePointColumn> parseColumns(JsonNode columns) {
    List<SharePointColumn> columnList = new ArrayList<>();

    if (columns != null && columns.isArray()) {
      for (JsonNode column : columns) {
        // Skip hidden and read-only columns
        if (column.has("hidden") && column.get("hidden").asBoolean()) {
          continue;
        }
        if (column.has("readOnly") && column.get("readOnly").asBoolean()) {
          continue;
        }

        String name = column.get("name").asText();
        String displayName = column.has("displayName") ?
            column.get("displayName").asText() : name;
        boolean required = column.has("required") && column.get("required").asBoolean();

        // Include important system columns (Title, ContentType, Attachments) in schema for transparency
        // but skip other system columns that are purely internal
        if (isExcludedSystemColumn(name)) {
          continue;
        }

        // Determine column type
        String type = "text";
        if (column.has("text")) {
          type = "text";
        } else if (column.has("number")) {
          type = "number";
        } else if (column.has("boolean")) {
          type = "boolean";
        } else if (column.has("dateTime")) {
          type = "datetime";
        } else if (column.has("choice")) {
          type = "choice";
        } else if (column.has("lookup")) {
          type = "lookup";
        } else if (column.has("personOrGroup")) {
          type = "user";
        } else if (column.has("currency")) {
          type = "currency";
        }

        // Make SharePoint's built-in Title column nullable for INSERT operations
        if (name.equals("Title")) {
          required = false;
        }

        columnList.add(new SharePointColumn(name, displayName, type, required));
      }
    }

    return columnList;
  }

  /**
   * Checks if a column is a SharePoint system column that should be excluded from the schema.
   * We include Title, ContentType and Attachments as they provide useful information to users.
   */
  private boolean isExcludedSystemColumn(String columnName) {
    // Common SharePoint system columns to exclude (but keep Title, ContentType and Attachments)
    return columnName.equals("Edit") ||
           columnName.equals("Type") ||
           columnName.equals("FileSizeDisplay") ||
           columnName.equals("ItemChildCount") ||
           columnName.equals("FolderChildCount") ||
           columnName.equals("AppAuthor") ||
           columnName.equals("AppEditor") ||
           columnName.equals("ComplianceAssetId") ||
           columnName.startsWith("_") ||  // Internal columns typically start with _
           columnName.equals("ID");       // We handle ID separately
  }

  /**
   * Converts JSON values to appropriate Java types.
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
      for (JsonNode item : node) {
        list.add(convertJsonValue(item));
      }
      return list;
    } else if (node.isObject()) {
      Map<String, Object> map = new LinkedHashMap<>();
      node.fields().forEachRemaining(entry -> {
        map.put(entry.getKey(), convertJsonValue(entry.getValue()));
      });
      return map;
    } else {
      return node.asText();
    }
  }

  /**
   * Adds a field value to a JSON node with appropriate type handling.
   */
  private void addFieldToNode(ObjectNode node, String fieldName, Object value) {
    if (value == null) {
      node.putNull(fieldName);
    } else if (value instanceof Boolean) {
      node.put(fieldName, (Boolean) value);
    } else if (value instanceof Integer) {
      node.put(fieldName, (Integer) value);
    } else if (value instanceof Long) {
      node.put(fieldName, (Long) value);
    } else if (value instanceof Double) {
      node.put(fieldName, (Double) value);
    } else if (value instanceof List) {
      node.putArray(fieldName).addAll((List<JsonNode>) value);
    } else {
      node.put(fieldName, value.toString());
    }
  }
}
