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
package org.apache.calcite.adapter.file.storage;

import org.apache.calcite.adapter.file.storage.cache.PersistentStorageCache;
import org.apache.calcite.adapter.file.storage.cache.StorageCacheManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Storage provider implementation for SharePoint using Microsoft Graph API.
 * Provides better compatibility and access to modern SharePoint features.
 */
public class MicrosoftGraphStorageProvider implements StorageProvider {

  private static final String GRAPH_API_BASE = "https://graph.microsoft.com/v1.0";
  private static final Logger LOGGER = LoggerFactory.getLogger(MicrosoftGraphStorageProvider.class);

  // Common document library names in different languages
  private static final Set<String> DOCUMENT_LIBRARY_NAMES =
      new HashSet<>(
          Arrays.asList("Shared Documents",
      "Documents",
      "Documents Partagés",  // French
      "Documentos compartidos",  // Spanish
      "Gemeinsame Dokumente",  // German
      "Documenti condivisi",  // Italian
      "共享文档",  // Chinese
      "共有ドキュメント"));  // Japanese

  private final String siteUrl;
  private final SharePointTokenManager tokenManager;
  private final ObjectMapper mapper = new ObjectMapper();
  private String siteId;
  private String driveId;

  // Persistent cache for restart-survivable caching
  private final PersistentStorageCache persistentCache;

  /**
   * Creates a Microsoft Graph storage provider with automatic token refresh.
   */
  public MicrosoftGraphStorageProvider(SharePointTokenManager tokenManager) {
    this.siteUrl = tokenManager.getSiteUrl().endsWith("/") ?
        tokenManager.getSiteUrl().substring(0, tokenManager.getSiteUrl().length() - 1)
        : tokenManager.getSiteUrl();

    // Convert to MicrosoftGraphTokenManager if needed
    if (!(tokenManager instanceof MicrosoftGraphTokenManager)) {
      // Create a new Graph-specific token manager with the same credentials
      if (tokenManager.clientSecret != null) {
        this.tokenManager =
            new MicrosoftGraphTokenManager(tokenManager.tenantId, tokenManager.clientId,
            tokenManager.clientSecret, tokenManager.getSiteUrl());
      } else {
        // Use the existing token manager (might be static token)
        this.tokenManager = tokenManager;
      }
    } else {
      this.tokenManager = tokenManager;
    }

    // Initialize persistent cache if cache manager is available
    PersistentStorageCache cache = null;
    try {
      cache = StorageCacheManager.getInstance().getCache("sharepoint");
    } catch (IllegalStateException e) {
      // Cache manager not initialized, persistent cache will be null
    }
    this.persistentCache = cache;
  }

  /**
   * Ensures site and drive IDs are initialized.
   */
  private void ensureInitialized() throws IOException {
    if (siteId == null || driveId == null) {
      initializeSiteAndDrive();
    }
  }

  /**
   * Initializes site ID and default document library drive ID.
   */
  private void initializeSiteAndDrive() throws IOException {
    // Extract hostname and site path from URL
    URI uri = URI.create(siteUrl);
    String hostname = uri.getHost();
    String sitePath = uri.getPath();

    // Get site ID
    String siteApiUrl;
    if (sitePath == null || sitePath.isEmpty() || sitePath.equals("/")) {
      // Root site
      siteApiUrl = String.format(Locale.ROOT, "%s/sites/%s", GRAPH_API_BASE, hostname);
    } else {
      // Subsite
      siteApiUrl = String.format(Locale.ROOT, "%s/sites/%s:%s", GRAPH_API_BASE, hostname, sitePath);
    }

    JsonNode siteResponse = executeGraphCall(siteApiUrl);
    this.siteId = siteResponse.get("id").asText();

    // Get default document library (drive)
    String driveApiUrl = String.format(Locale.ROOT, "%s/sites/%s/drive", GRAPH_API_BASE, siteId);
    JsonNode driveResponse = executeGraphCall(driveApiUrl);
    this.driveId = driveResponse.get("id").asText();
  }

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    ensureInitialized();
    List<FileEntry> entries = new ArrayList<>();

    // Normalize and preserve the original path for building full paths
    String originalPath = path;
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    // Build Graph API URL
    String apiUrl;
    String apiPath = path; // Path to use for API call

    if (path.isEmpty() || isDocumentLibraryRoot(path) || path.equals("/")) {
      // Root of document library
      apiUrl = String.format(Locale.ROOT, "%s/drives/%s/root/children", GRAPH_API_BASE, driveId);
    } else {
      // Remove common document library prefixes if present for API call
      apiPath = removeDocumentLibraryPrefix(apiPath);

      String encodedPath = URLEncoder.encode(apiPath, StandardCharsets.UTF_8)
          .replace("+", "%20");
      apiUrl =
          String.format(Locale.ROOT, "%s/drives/%s/root:/%s:/children", GRAPH_API_BASE, driveId, encodedPath);
    }

    // Get items
    JsonNode response = executeGraphCall(apiUrl);
    JsonNode items = response.get("value");

    if (items != null && items.isArray()) {
      for (JsonNode item : items) {
        // Build the full path based on the directory we're listing
        String itemName = item.get("name").asText();
        String itemPath;

        // Construct full path based on the directory being listed
        if (path.isEmpty() || path.equals("/")) {
          itemPath = itemName;
        } else {
          itemPath = path + "/" + itemName;
        }

        boolean isFolder = item.has("folder");

        entries.add(
            new FileEntry(
            itemPath,
            itemName,
            isFolder,
            item.has("size") ? item.get("size").asLong() : 0,
            parseDateTime(item.get("lastModifiedDateTime").asText())));

        if (isFolder && recursive) {
          // Recursively list folder contents with the full path
          entries.addAll(listFiles(itemPath, true));
        }
      }
    }

    // Handle pagination
    while (response.has("@odata.nextLink")) {
      String nextLink = response.get("@odata.nextLink").asText();
      response = executeGraphCall(nextLink);
      items = response.get("value");

      if (items != null && items.isArray()) {
        for (JsonNode item : items) {
          // Build the full path based on the directory we're listing
          String itemName = item.get("name").asText();
          String itemPath;

          // Construct full path based on the directory being listed
          if (path.isEmpty() || path.equals("/")) {
            itemPath = itemName;
          } else {
            itemPath = path + "/" + itemName;
          }

          boolean isFolder = item.has("folder");

          entries.add(
              new FileEntry(
              itemPath,
              itemName,
              isFolder,
              item.has("size") ? item.get("size").asLong() : 0,
              parseDateTime(item.get("lastModifiedDateTime").asText())));

          if (isFolder && recursive) {
            entries.addAll(listFiles(itemPath, true));
          }
        }
      }
    }

    return entries;
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    ensureInitialized();

    // Preserve original path for return value
    String originalPath = path;

    // Normalize path
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    // For API call, we need to handle the nested structure properly
    // Remove document library prefix for API call
    String apiPath = removeDocumentLibraryPrefix(path);

    // Build Graph API URL
    String apiUrl;
    if (apiPath.isEmpty()) {
      apiUrl = String.format(Locale.ROOT, "%s/drives/%s/root", GRAPH_API_BASE, driveId);
    } else {
      String encodedPath = URLEncoder.encode(apiPath, StandardCharsets.UTF_8)
          .replace("+", "%20");
      apiUrl = String.format(Locale.ROOT, "%s/drives/%s/root:/%s", GRAPH_API_BASE, driveId, encodedPath);
    }

    JsonNode response = executeGraphCall(apiUrl);

    return new FileMetadata(
        originalPath,
        response.has("size") ? response.get("size").asLong() : 0,
        parseDateTime(response.get("lastModifiedDateTime").asText()),
        response.has("file") && response.get("file").has("mimeType")
            ? response.get("file").get("mimeType").asText()
            : "application/octet-stream",
        response.has("eTag") ? response.get("eTag").asText() : null);
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    // Check persistent cache first if available
    if (persistentCache != null) {
      byte[] cachedData = persistentCache.getCachedData(path);
      FileMetadata cachedMetadata = persistentCache.getCachedMetadata(path);

      if (cachedData != null && cachedMetadata != null) {
        // Check if cached data is still fresh
        try {
          if (!hasChanged(path, cachedMetadata)) {
            return new java.io.ByteArrayInputStream(cachedData);
          }
        } catch (IOException e) {
          // If we can't check freshness, use cached data anyway
          return new java.io.ByteArrayInputStream(cachedData);
        }
      }
    }

    ensureInitialized();

    // Normalize path
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    // For API call, we need to handle the nested structure properly
    // Remove document library prefix for API call
    String apiPath = removeDocumentLibraryPrefix(path);

    // Build download URL
    String encodedPath = URLEncoder.encode(apiPath, StandardCharsets.UTF_8)
        .replace("+", "%20");
    String downloadUrl =
        String.format(Locale.ROOT, "%s/drives/%s/root:/%s:/content", GRAPH_API_BASE, driveId, encodedPath);

    URL url = URI.create(downloadUrl).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty("Authorization", "Bearer " + tokenManager.getAccessToken());
    conn.setRequestProperty("Accept", "application/octet-stream");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);

    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      conn.disconnect();
      throw new IOException("Failed to download file from path '" + path + "' (API path: '" + apiPath + "'): HTTP " + responseCode);
    }

    // If persistent cache is available, read data and cache it
    if (persistentCache != null) {
      byte[] data = readAllBytes(conn.getInputStream());
      conn.disconnect();

      // Get file metadata for caching
      FileMetadata metadata = getMetadata(path);
      persistentCache.cacheData(path, data, metadata, 0); // No TTL for SharePoint

      return new java.io.ByteArrayInputStream(data);
    }

    return conn.getInputStream();
  }

  @Override public Reader openReader(String path) throws IOException {
    return new InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) throws IOException {
    try {
      getMetadata(path);
      return true;
    } catch (IOException e) {
      // Check if it's a 404 error
      if (e.getMessage().contains("404")) {
        return false;
      }
      throw e;
    }
  }

  @Override public boolean isDirectory(String path) throws IOException {
    ensureInitialized();

    // Normalize path
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    // Remove document library prefix if present
    path = removeDocumentLibraryPrefix(path);

    // Build Graph API URL
    String apiUrl;
    if (path.isEmpty()) {
      apiUrl = String.format(Locale.ROOT, "%s/drives/%s/root", GRAPH_API_BASE, driveId);
    } else {
      String encodedPath = URLEncoder.encode(path, StandardCharsets.UTF_8)
          .replace("+", "%20");
      apiUrl = String.format(Locale.ROOT, "%s/drives/%s/root:/%s", GRAPH_API_BASE, driveId, encodedPath);
    }

    try {
      JsonNode response = executeGraphCall(apiUrl);
      return response.has("folder");
    } catch (IOException e) {
      if (e.getMessage().contains("404")) {
        return false;
      }
      throw e;
    }
  }

  @Override public String getStorageType() {
    return "microsoft-graph";
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    if (relativePath.startsWith("http://") || relativePath.startsWith("https://")) {
      return relativePath;
    }

    String baseDir = basePath;
    if (!basePath.endsWith("/") && !isLikelyDirectory(basePath)) {
      // It's likely a file, get the parent directory
      int lastSlash = basePath.lastIndexOf('/');
      if (lastSlash > 0) {
        baseDir = basePath.substring(0, lastSlash);
      }
    }

    if (baseDir.endsWith("/")) {
      return baseDir + relativePath;
    } else {
      return baseDir + "/" + relativePath;
    }
  }

  private boolean isLikelyDirectory(String path) {
    // Simple heuristic: paths without extensions are likely directories
    int lastSlash = path.lastIndexOf('/');
    String name = lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
    return !name.contains(".");
  }

  private JsonNode executeGraphCall(String apiUrl) throws IOException {
    URL url = URI.create(apiUrl).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization", "Bearer " + tokenManager.getAccessToken());
    conn.setRequestProperty("Accept", "application/json");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    try {
      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        String error = "";
        try (InputStream errorStream = conn.getErrorStream()) {
          if (errorStream != null) {
            JsonNode errorJson = mapper.readTree(errorStream);
            if (errorJson.has("error") && errorJson.get("error").has("message")) {
              error = errorJson.get("error").get("message").asText();
            }
          }
        } catch (Exception e) {
          // Ignore JSON parsing errors
        }
        throw new IOException("Microsoft Graph API error: HTTP " + responseCode +
                              (error.isEmpty() ? "" : " - " + error));
      }

      try (InputStream in = conn.getInputStream()) {
        return mapper.readTree(in);
      }
    } finally {
      conn.disconnect();
    }
  }

  private String buildItemPath(JsonNode item) {
    // Build path from parentReference and name
    String name = item.get("name").asText();

    if (item.has("parentReference") && item.get("parentReference").has("path")) {
      String parentPath = item.get("parentReference").get("path").asText();
      // Remove the drive root prefix
      if (parentPath.contains(":")) {
        parentPath = parentPath.substring(parentPath.indexOf(":") + 1);
      }
      if (parentPath.startsWith("/")) {
        parentPath = parentPath.substring(1);
      }

      if (parentPath.isEmpty()) {
        // Item is at root - just return the name
        return name;
      } else {
        return parentPath + "/" + name;
      }
    }

    // Default to just the name if no parent reference
    return name;
  }

  private long parseDateTime(String dateTime) {
    try {
      // Microsoft Graph returns ISO 8601 format
      return Instant.parse(dateTime).toEpochMilli();
    } catch (Exception e) {
      return System.currentTimeMillis();
    }
  }

  /**
   * Checks if the given path represents a document library root.
   */
  private boolean isDocumentLibraryRoot(String path) {
    return DOCUMENT_LIBRARY_NAMES.contains(path);
  }

  /**
   * Removes common document library prefixes from a path for API calls.
   * This handles both single and nested document library paths.
   */
  private byte[] readAllBytes(InputStream inputStream) throws IOException {
    java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
    byte[] data = new byte[8192];
    int nRead;
    while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }
    return buffer.toByteArray();
  }

  private String removeDocumentLibraryPrefix(String path) {
    // Special handling for the nested "Shared Documents/Shared Documents" pattern
    // This occurs when the URL structure duplicates the library name
    if (path.startsWith("Shared Documents/Shared Documents/")) {
      // Remove only the first "Shared Documents/"
      return path.substring("Shared Documents/".length());
    }

    // For other document library names, only remove if it's the root
    for (String libraryName : DOCUMENT_LIBRARY_NAMES) {
      if (path.equals(libraryName)) {
        return "";
      }
      String prefix = libraryName + "/";
      if (path.startsWith(prefix)) {
        // For non-"Shared Documents" libraries, just remove the prefix once
        // Don't remove nested occurrences as they might be actual folder names
        if (!libraryName.equals("Shared Documents")) {
          return path.substring(prefix.length());
        }
      }
    }

    // Default case for "Shared Documents/" when not nested
    if (path.startsWith("Shared Documents/") && !path.startsWith("Shared Documents/Shared Documents/")) {
      return path.substring("Shared Documents/".length());
    }

    return path;
  }
}
