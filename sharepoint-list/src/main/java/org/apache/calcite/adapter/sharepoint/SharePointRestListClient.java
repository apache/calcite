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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * SharePoint REST API client for List operations.
 * Provides attachment operations using SharePoint REST API instead of Microsoft Graph API.
 */
public class SharePointRestListClient {

  private final String siteUrl;
  private final SharePointAuth authenticator;
  private final ObjectMapper objectMapper;

  public SharePointRestListClient(String siteUrl, SharePointAuth authenticator) {
    this.siteUrl = siteUrl.endsWith("/") ? siteUrl.substring(0, siteUrl.length() - 1) : siteUrl;
    this.authenticator = authenticator;
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Gets the site URL.
   */
  public String getSiteUrl() {
    return siteUrl;
  }

  /**
   * Gets attachments for a SharePoint list item using REST API.
   *
   * @param listName Name or title of the list
   * @param itemId ID of the list item
   * @return List of attachment metadata
   * @throws IOException if API call fails
   * @throws InterruptedException if interrupted
   */
  public List<Object[]> getAttachments(String listName, String itemId)
      throws IOException, InterruptedException {

    List<Object[]> result = new ArrayList<>();

    // GET /_api/web/lists/getbytitle('{listName}')/items({itemId})/AttachmentFiles
    String apiUrl =
        String.format(Locale.ROOT, "%s/_api/web/lists/getbytitle('%s')/items(%s)/AttachmentFiles",
        siteUrl,
        URLEncoder.encode(listName, StandardCharsets.UTF_8),
        itemId);

    JsonNode response = executeRestCall("GET", apiUrl, null);

    if (response.has("d") && response.get("d").has("results")) {
      for (JsonNode attachment : response.get("d").get("results")) {
        String filename = attachment.get("FileName").asText();
        String serverRelativeUrl = attachment.get("ServerRelativeUrl").asText();
        // Build full URL for attachment
        String attachmentUrl = siteUrl + serverRelativeUrl;

        // Get file size (REST API doesn't provide it directly in attachment list)
        long size = 0;
        try {
          // Try to get file size from file properties
          String fileApiUrl =
              String.format(Locale.ROOT, "%s/_api/web/GetFileByServerRelativeUrl('%s')",
              siteUrl, URLEncoder.encode(serverRelativeUrl, StandardCharsets.UTF_8));

          JsonNode fileResponse = executeRestCall("GET", fileApiUrl, null);
          if (fileResponse.has("d") && fileResponse.get("d").has("Length")) {
            size = fileResponse.get("d").get("Length").asLong();
          }
        } catch (Exception e) {
          // If we can't get file size, continue without it
        }

        result.add(new Object[]{filename, attachmentUrl, size});
      }
    }

    return result;
  }

  /**
   * Uploads an attachment to a SharePoint list item using REST API.
   *
   * @param listName Name or title of the list
   * @param itemId ID of the list item
   * @param filename Name of the file to attach
   * @param content Binary content of the file
   * @return true if successful
   * @throws IOException if API call fails
   * @throws InterruptedException if interrupted
   */
  public boolean uploadAttachment(String listName, String itemId, String filename, byte[] content)
      throws IOException, InterruptedException {

    // POST /_api/web/lists/getbytitle('{listName}')/items({itemId})/AttachmentFiles/add(FileName='{filename}')
    String apiUrl =
        String.format(Locale.ROOT, "%s/_api/web/lists/getbytitle('%s')/items(%s)/AttachmentFiles/add(FileName='%s')",
        siteUrl,
        URLEncoder.encode(listName, StandardCharsets.UTF_8),
        itemId,
        URLEncoder.encode(filename, StandardCharsets.UTF_8));

    // SharePoint REST API expects binary content directly in request body
    JsonNode response = executeRestCallWithBinary("POST", apiUrl, content);

    return response != null;
  }

  /**
   * Deletes an attachment from a SharePoint list item using REST API.
   *
   * @param listName Name or title of the list
   * @param itemId ID of the list item
   * @param filename Name of the file to delete
   * @return true if successful
   * @throws IOException if API call fails
   * @throws InterruptedException if interrupted
   */
  public boolean deleteAttachment(String listName, String itemId, String filename)
      throws IOException, InterruptedException {

    // POST /_api/web/lists/getbytitle('{listName}')/items({itemId})/AttachmentFiles/getByFileName('{filename}')/delete
    String apiUrl =
        String.format(Locale.ROOT, "%s/_api/web/lists/getbytitle('%s')/items(%s)/AttachmentFiles/getByFileName('%s')/delete",
        siteUrl,
        URLEncoder.encode(listName, StandardCharsets.UTF_8),
        itemId,
        URLEncoder.encode(filename, StandardCharsets.UTF_8));

    // Use POST with X-HTTP-Method override for DELETE
    JsonNode response = executeRestCall("POST", apiUrl, null, Map.of("X-HTTP-Method", "DELETE"));

    return true; // If no exception thrown, consider success
  }

  /**
   * Downloads attachment content from a SharePoint list item using REST API.
   *
   * @param listName Name or title of the list
   * @param itemId ID of the list item
   * @param filename Name of the file to download
   * @return Binary content of the attachment
   * @throws IOException if API call fails
   * @throws InterruptedException if interrupted
   */
  public byte[] downloadAttachment(String listName, String itemId, String filename)
      throws IOException, InterruptedException {

    // First get the attachment URL
    String attachmentApiUrl =
        String.format(Locale.ROOT, "%s/_api/web/lists/getbytitle('%s')/items(%s)/AttachmentFiles/getByFileName('%s')",
        siteUrl,
        URLEncoder.encode(listName, StandardCharsets.UTF_8),
        itemId,
        URLEncoder.encode(filename, StandardCharsets.UTF_8));

    JsonNode attachmentInfo = executeRestCall("GET", attachmentApiUrl, null);

    if (attachmentInfo.has("d") && attachmentInfo.get("d").has("ServerRelativeUrl")) {
      String serverRelativeUrl = attachmentInfo.get("d").get("ServerRelativeUrl").asText();

      // Download the file content using $value endpoint
      String downloadUrl =
          String.format(Locale.ROOT, "%s/_api/web/GetFileByServerRelativeUrl('%s')/$value",
          siteUrl,
          URLEncoder.encode(serverRelativeUrl, StandardCharsets.UTF_8));

      return downloadBinary(downloadUrl);
    }

    throw new IOException("Attachment not found: " + filename);
  }

  /**
   * Downloads binary content from a URL.
   */
  private byte[] downloadBinary(String url) throws IOException, InterruptedException {
    URL apiUrl = URI.create(url).toURL();
    HttpURLConnection conn = (HttpURLConnection) apiUrl.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization", "Bearer " + authenticator.getAccessToken());
    conn.setRequestProperty("Accept", "application/octet-stream");
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
   * Executes a SharePoint REST API call.
   */
  public JsonNode executeRestCall(String method, String url, JsonNode requestBody)
      throws IOException, InterruptedException {
    return executeRestCall(method, url, requestBody, new HashMap<>());
  }

  /**
   * Executes a SharePoint REST API call with custom headers.
   */
  public JsonNode executeRestCall(String method, String url, JsonNode requestBody, Map<String, String> headers)
      throws IOException, InterruptedException {

    URL apiUrl = URI.create(url).toURL();
    HttpURLConnection conn = (HttpURLConnection) apiUrl.openConnection();
    conn.setRequestMethod(method);
    conn.setRequestProperty("Authorization", "Bearer " + authenticator.getAccessToken());
    conn.setRequestProperty("Accept", "application/json;odata=verbose");
    conn.setRequestProperty("Content-Type", "application/json;odata=verbose");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);

    // Check if we need to set Content-Length for empty body
    if (requestBody == null && ("POST".equals(method) || "PUT".equals(method))) {
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Length", "0");
    }

    // Add custom headers (after setting Content-Length)
    for (Map.Entry<String, String> header : headers.entrySet()) {
      conn.setRequestProperty(header.getKey(), header.getValue());
    }

    if (requestBody != null) {
      conn.setDoOutput(true);
      objectMapper.writeValue(conn.getOutputStream(), requestBody);
    } else if ("POST".equals(method) || "PUT".equals(method)) {
      // Write empty body for POST/PUT with no content
      conn.getOutputStream().close();
    }

    int responseCode = conn.getResponseCode();

    // Handle successful responses
    if (responseCode >= 200 && responseCode < 300) {
      if (conn.getContentLength() == 0 || responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
        return objectMapper.createObjectNode();
      }

      try (InputStream in = conn.getInputStream()) {
        return objectMapper.readTree(in);
      }
    } else {
      String error = "";
      try (InputStream errorStream = conn.getErrorStream()) {
        if (errorStream != null) {
          JsonNode errorJson = objectMapper.readTree(errorStream);
          if (errorJson.has("error") && errorJson.get("error").has("message")) {
            JsonNode messageNode = errorJson.get("error").get("message");
            if (messageNode.has("value")) {
              error = messageNode.get("value").asText();
            } else {
              error = messageNode.asText();
            }
          }
        }
      } catch (Exception e) {
        // Ignore stream reading errors
      }

      throw new IOException("SharePoint REST API error: HTTP " + responseCode +
          (error.isEmpty() ? "" : " - " + error));
    }
  }

  /**
   * Executes a SharePoint REST API call with binary content.
   */
  private JsonNode executeRestCallWithBinary(String method, String url, byte[] binaryContent)
      throws IOException, InterruptedException {

    URL apiUrl = URI.create(url).toURL();
    HttpURLConnection conn = (HttpURLConnection) apiUrl.openConnection();
    conn.setRequestMethod(method);
    conn.setRequestProperty("Authorization", "Bearer " + authenticator.getAccessToken());
    conn.setRequestProperty("Accept", "application/json;odata=verbose");
    conn.setRequestProperty("Content-Type", "application/octet-stream");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);

    if (binaryContent != null) {
      conn.setDoOutput(true);
      conn.getOutputStream().write(binaryContent);
    }

    int responseCode = conn.getResponseCode();

    if (responseCode >= 200 && responseCode < 300) {
      if (conn.getContentLength() == 0) {
        return objectMapper.createObjectNode();
      }

      try (InputStream in = conn.getInputStream()) {
        return objectMapper.readTree(in);
      }
    } else {
      String error = "";
      try (InputStream errorStream = conn.getErrorStream()) {
        if (errorStream != null) {
          JsonNode errorJson = objectMapper.readTree(errorStream);
          if (errorJson.has("error") && errorJson.get("error").has("message")) {
            JsonNode messageNode = errorJson.get("error").get("message");
            if (messageNode.has("value")) {
              error = messageNode.get("value").asText();
            } else {
              error = messageNode.asText();
            }
          }
        }
      } catch (Exception e) {
        // Ignore stream reading errors
      }

      throw new IOException("SharePoint REST API error: HTTP " + responseCode +
          (error.isEmpty() ? "" : " - " + error));
    }
  }
}
