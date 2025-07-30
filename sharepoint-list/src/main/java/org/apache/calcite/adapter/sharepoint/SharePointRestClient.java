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

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * REST client for interacting with SharePoint Lists API.
 */
public class SharePointRestClient {
  private static final String API_BASE = "/_api/web/lists";

  private final String siteUrl;
  private final SharePointAuth authenticator;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  public SharePointRestClient(String siteUrl, SharePointAuth authenticator) {
    this.siteUrl = siteUrl.endsWith("/") ? siteUrl.substring(0, siteUrl.length() - 1) : siteUrl;
    this.authenticator = authenticator;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
    this.objectMapper = new ObjectMapper();
  }

  public Map<String, SharePointListMetadata> getAvailableLists()
      throws IOException, InterruptedException {
    String url = siteUrl + API_BASE + "?$filter=Hidden eq false and BaseTemplate eq 100";
    JsonNode response = sendGetRequest(url);

    Map<String, SharePointListMetadata> lists = new LinkedHashMap<>();
    JsonNode value = response.get("value");

    if (value != null && value.isArray()) {
      for (JsonNode list : value) {
        String listId = list.get("Id").asText();
        String title = list.get("Title").asText();
        String entityTypeName = list.get("EntityTypeName").asText();

        SharePointListMetadata metadata = getListMetadataById(listId);
        lists.put(title, metadata);
      }
    }

    return lists;
  }

  public SharePointListMetadata getListMetadataById(String listId)
      throws IOException, InterruptedException {
    String fieldsUrl = siteUrl + API_BASE + "(guid'" + listId + "')/Fields?$filter=Hidden eq false";
    JsonNode fieldsResponse = sendGetRequest(fieldsUrl);

    String listUrl = siteUrl + API_BASE + "(guid'" + listId + "')";
    JsonNode listResponse = sendGetRequest(listUrl);

    String title = listResponse.get("Title").asText();
    String entityTypeName = listResponse.get("EntityTypeName").asText();

    List<SharePointColumn> columns = parseColumns(fieldsResponse.get("value"));

    return new SharePointListMetadata(listId, title, entityTypeName, columns);
  }

  public SharePointListMetadata getListMetadataByName(String listName)
      throws IOException, InterruptedException {
    String url = siteUrl + API_BASE + "/GetByTitle('"
        + URLEncoder.encode(listName, StandardCharsets.UTF_8) + "')";
    JsonNode response = sendGetRequest(url);
    String listId = response.get("Id").asText();
    return getListMetadataById(listId);
  }

  public List<Map<String, Object>> getListItems(String listId)
      throws IOException, InterruptedException {
    return getListItems(listId, null, 5000); // Default max items
  }

  public List<Map<String, Object>> getListItems(String listId, String filter, int top)
      throws IOException, InterruptedException {
    String url = siteUrl + API_BASE + "(guid'" + listId + "')/items?$top=" + top;
    if (filter != null && !filter.isEmpty()) {
      url += "&$filter=" + URLEncoder.encode(filter, StandardCharsets.UTF_8);
    }

    List<Map<String, Object>> allItems = new ArrayList<>();
    String nextLink = url;

    while (nextLink != null) {
      JsonNode response = sendGetRequest(nextLink);
      JsonNode value = response.get("value");

      if (value != null && value.isArray()) {
        for (JsonNode item : value) {
          Map<String, Object> itemMap = objectMapper.convertValue(item, Map.class);
          allItems.add(itemMap);
        }
      }

      // Check for pagination
      JsonNode next = response.get("@odata.nextLink");
      nextLink = (next != null && !next.isNull()) ? next.asText() : null;
    }

    return allItems;
  }

  private List<SharePointColumn> parseColumns(JsonNode fields) {
    List<SharePointColumn> columns = new ArrayList<>();

    if (fields != null && fields.isArray()) {
      for (JsonNode field : fields) {
        if (field.get("ReadOnlyField").asBoolean()) {
          continue; // Skip read-only fields
        }

        String internalName = field.get("InternalName").asText();
        String displayName = field.get("Title").asText();
        String fieldType = field.get("TypeAsString").asText();
        boolean required = field.get("Required").asBoolean();

        columns.add(new SharePointColumn(internalName, displayName, fieldType, required));
      }
    }

    return columns;
  }

  private JsonNode sendGetRequest(String url) throws IOException, InterruptedException {
    String token = authenticator.getAccessToken();

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Authorization", "Bearer " + token)
        .header("Accept", "application/json;odata=verbose")
        .GET()
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("SharePoint API request failed: " + response.body());
    }

    return objectMapper.readTree(response.body());
  }
}
