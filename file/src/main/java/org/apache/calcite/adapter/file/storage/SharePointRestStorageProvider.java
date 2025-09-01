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
 * Storage provider implementation for SharePoint using SharePoint REST API.
 * Uses the native SharePoint REST API instead of Microsoft Graph API.
 */
public class SharePointRestStorageProvider implements StorageProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SharePointRestStorageProvider.class);

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
              "共有ドキュメント"));

  private final String siteUrl;
  private final SharePointTokenManager tokenManager;
  private final ObjectMapper mapper = new ObjectMapper();
  private String webUrl;
  private String documentLibraryUrl;

  /**
   * Creates a SharePoint REST storage provider with automatic token refresh.
   */
  public SharePointRestStorageProvider(SharePointTokenManager tokenManager) {
    this.siteUrl = tokenManager.getSiteUrl().endsWith("/") ?
        tokenManager.getSiteUrl().substring(0, tokenManager.getSiteUrl().length() - 1)
        : tokenManager.getSiteUrl();

    // Convert to SharePointRestTokenManager if needed
    if (!(tokenManager instanceof SharePointRestTokenManager)) {
      // Create a new REST-specific token manager with the same credentials
      if (tokenManager.clientSecret != null) {
        this.tokenManager =
            new SharePointRestTokenManager(tokenManager.tenantId, tokenManager.clientId,
            tokenManager.clientSecret, tokenManager.getSiteUrl());
      } else {
        // Use the existing token manager (might be static token)
        this.tokenManager = tokenManager;
      }
    } else {
      this.tokenManager = tokenManager;
    }

    this.webUrl = siteUrl;
  }

  /**
   * Ensures the document library URL is initialized.
   */
  private void ensureInitialized() throws IOException {
    if (documentLibraryUrl == null) {
      initializeDocumentLibrary();
    }
  }

  /**
   * Initializes the document library URL.
   */
  private void initializeDocumentLibrary() throws IOException {
    // Get the default document library (usually "Shared Documents")
    String apiUrl = String.format(Locale.ROOT, "%s/_api/web/lists?$filter=BaseTemplate eq 101", webUrl);

    JsonNode response = executeRestCall(apiUrl);
    JsonNode results = response.get("d").get("results");

    if (results != null && results.isArray() && results.size() > 0) {
      // Get the first document library (usually "Shared Documents")
      JsonNode firstLibrary = results.get(0);
      String libraryTitle = firstLibrary.get("Title").asText();
      String libraryServerRelativeUrl = firstLibrary.get("RootFolder").get("__deferred").get("uri").asText();

      // Get the actual root folder URL
      JsonNode folderResponse = executeRestCall(libraryServerRelativeUrl);
      this.documentLibraryUrl = folderResponse.get("d").get("ServerRelativeUrl").asText();

      LOGGER.debug("Initialized document library: {} at {}", libraryTitle, documentLibraryUrl);
    } else {
      // Fallback to "Shared Documents"
      this.documentLibraryUrl = "/Shared Documents";
      LOGGER.warn("No document library found, using default: {}", documentLibraryUrl);
    }
  }

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    ensureInitialized();
    List<FileEntry> entries = new ArrayList<>();

    // Normalize path
    String originalPath = path;
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    // Build the folder path
    String folderPath;
    if (path.isEmpty() || isDocumentLibraryRoot(path)) {
      folderPath = documentLibraryUrl;
    } else {
      // Remove document library prefix if present
      path = removeDocumentLibraryPrefix(path);
      folderPath = documentLibraryUrl + "/" + path;
    }

    // Get files in the folder
    String filesApiUrl =
        String.format(Locale.ROOT, "%s/_api/web/GetFolderByServerRelativeUrl('%s')/Files",
        webUrl,
        URLEncoder.encode(folderPath, StandardCharsets.UTF_8).replace("+", "%20"));

    try {
      JsonNode filesResponse = executeRestCall(filesApiUrl);
      JsonNode files = filesResponse.get("d").get("results");

      if (files != null && files.isArray()) {
        for (JsonNode file : files) {
          String fileName = file.get("Name").asText();
          // Use originalPath to preserve the full path structure
          String itemPath = originalPath.isEmpty() ? fileName :
              (originalPath.startsWith("/") ? originalPath.substring(1) : originalPath) + "/" + fileName;

          entries.add(
              new FileEntry(
              itemPath,
              fileName,
              false,
              file.get("Length").asLong(),
              parseDateTime(file.get("TimeLastModified").asText())));
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Failed to get files for folder: {}", folderPath, e);
    }

    // Get subfolders
    String foldersApiUrl =
        String.format(Locale.ROOT, "%s/_api/web/GetFolderByServerRelativeUrl('%s')/Folders",
        webUrl,
        URLEncoder.encode(folderPath, StandardCharsets.UTF_8).replace("+", "%20"));

    try {
      JsonNode foldersResponse = executeRestCall(foldersApiUrl);
      JsonNode folders = foldersResponse.get("d").get("results");

      if (folders != null && folders.isArray()) {
        for (JsonNode folder : folders) {
          String folderName = folder.get("Name").asText();

          // Skip system folders
          if (folderName.startsWith("_") || folderName.equals("Forms")) {
            continue;
          }

          // Use originalPath to preserve the full path structure
          String itemPath = originalPath.isEmpty() ? folderName :
              (originalPath.startsWith("/") ? originalPath.substring(1) : originalPath) + "/" + folderName;

          entries.add(
              new FileEntry(
              itemPath,
              folderName,
              true,
              0,
              parseDateTime(folder.get("TimeLastModified").asText())));

          if (recursive) {
            // Recursively list folder contents
            entries.addAll(listFiles(itemPath, true));
          }
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Failed to get folders for: {}", folderPath, e);
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

    // Remove document library prefix if present
    path = removeDocumentLibraryPrefix(path);

    // Build the file path
    String filePath = documentLibraryUrl + "/" + path;

    // Get file metadata
    String apiUrl =
        String.format(Locale.ROOT, "%s/_api/web/GetFileByServerRelativeUrl('%s')",
        webUrl,
        URLEncoder.encode(filePath, StandardCharsets.UTF_8).replace("+", "%20"));

    JsonNode response = executeRestCall(apiUrl);
    JsonNode fileData = response.get("d");

    return new FileMetadata(
        originalPath,
        fileData.get("Length").asLong(),
        parseDateTime(fileData.get("TimeLastModified").asText()),
        fileData.has("ContentType") ? fileData.get("ContentType").asText() : "application/octet-stream",
        fileData.has("ETag") ? fileData.get("ETag").asText() : null);
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    ensureInitialized();

    LOGGER.debug("SharePointRestStorageProvider.openInputStream called with path: {}", path);

    // Normalize path
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    // Remove document library prefix if present
    path = removeDocumentLibraryPrefix(path);

    LOGGER.debug("After normalization: path={}, documentLibraryUrl={}", path, documentLibraryUrl);

    // Build the file path
    String filePath = documentLibraryUrl + "/" + path;

    // Build download URL using $value endpoint
    String downloadUrl =
        String.format(Locale.ROOT, "%s/_api/web/GetFileByServerRelativeUrl('%s')/$value",
        webUrl,
        URLEncoder.encode(filePath, StandardCharsets.UTF_8).replace("+", "%20"));

    LOGGER.debug("Download URL: {}", downloadUrl);

    URL url = URI.create(downloadUrl).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty("Authorization", "Bearer " + tokenManager.getAccessToken());
    conn.setRequestProperty("Accept", "application/octet-stream");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);

    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      conn.disconnect();
      LOGGER.error("Failed to download file: responseCode={}, path={}, downloadUrl={}",
          responseCode, path, downloadUrl);
      throw new IOException("Failed to download file from path '" + path + "': HTTP " + responseCode);
    }

    LOGGER.debug("Successfully opened input stream for path: {}", path);

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

    // Build the folder path
    String folderPath;
    if (path.isEmpty()) {
      folderPath = documentLibraryUrl;
    } else {
      folderPath = documentLibraryUrl + "/" + path;
    }

    // Check if it's a folder
    String apiUrl =
        String.format(Locale.ROOT, "%s/_api/web/GetFolderByServerRelativeUrl('%s')",
        webUrl,
        URLEncoder.encode(folderPath, StandardCharsets.UTF_8).replace("+", "%20"));

    try {
      JsonNode response = executeRestCall(apiUrl);
      return response.has("d") && response.get("d").has("Exists")
          && response.get("d").get("Exists").asBoolean();
    } catch (IOException e) {
      if (e.getMessage().contains("404")) {
        return false;
      }
      throw e;
    }
  }

  @Override public String getStorageType() {
    return "sharepoint-rest";
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

  private JsonNode executeRestCall(String apiUrl) throws IOException {
    // Properly encode the URL to handle spaces and special characters
    URL url;
    try {
      // Split URL into base and query parts
      int queryIndex = apiUrl.indexOf('?');
      if (queryIndex > 0) {
        String baseUrl = apiUrl.substring(0, queryIndex);
        String query = apiUrl.substring(queryIndex + 1);
        // Encode spaces in query parameters
        query = query.replace(" ", "%20");
        url = URI.create(baseUrl + "?" + query).toURL();
      } else {
        url = URI.create(apiUrl).toURL();
      }
    } catch (Exception e) {
      throw new IOException("Failed to create URL from: " + apiUrl, e);
    }
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    String token = tokenManager.getAccessToken();
    conn.setRequestProperty("Authorization", "Bearer " + token);
    conn.setRequestProperty("Accept", "application/json;odata=verbose");
    conn.setRequestProperty("Content-Type", "application/json;odata=verbose");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    LOGGER.debug("SharePoint REST API Call:");
    LOGGER.debug("  URL: " + apiUrl);
    LOGGER.debug("  Token length: " + (token != null ? token.length() : "null"));
    LOGGER.debug("  Token prefix: " + (token != null && token.length() > 20 ? token.substring(0, 20) + "..." : token));

    try {
      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        String error = "";
        String wwwAuthenticate = conn.getHeaderField("WWW-Authenticate");
        String xMsErrorCode = conn.getHeaderField("x-ms-diagnostics");

        try (InputStream errorStream = conn.getErrorStream()) {
          if (errorStream != null) {
            byte[] errorBytes = errorStream.readAllBytes();
            String errorBody = new String(errorBytes, StandardCharsets.UTF_8);
            LOGGER.debug("Error response body: " + errorBody);

            try {
              JsonNode errorJson = mapper.readTree(errorBytes);
              if (errorJson.has("error") && errorJson.get("error").has("message")) {
                JsonNode messageNode = errorJson.get("error").get("message");
                if (messageNode.has("value")) {
                  error = messageNode.get("value").asText();
                } else {
                  error = messageNode.asText();
                }
              } else if (errorJson.has("error_description")) {
                error = errorJson.get("error_description").asText();
              }
            } catch (Exception je) {
              // If not JSON, use raw error
              error = errorBody;
            }
          }
        } catch (Exception e) {
          // Ignore stream reading errors
        }

        String errorMessage = "SharePoint REST API error: HTTP " + responseCode;
        if (!error.isEmpty()) {
          errorMessage += " - " + error;
        }
        if (wwwAuthenticate != null) {
          errorMessage += " (WWW-Authenticate: " + wwwAuthenticate + ")";
        }
        if (xMsErrorCode != null) {
          errorMessage += " (x-ms-diagnostics: " + xMsErrorCode + ")";
        }

        throw new IOException(errorMessage);
      }

      try (InputStream in = conn.getInputStream()) {
        return mapper.readTree(in);
      }
    } finally {
      conn.disconnect();
    }
  }

  private long parseDateTime(String dateTime) {
    try {
      // SharePoint REST API returns dates in format: /Date(1234567890123)/
      if (dateTime.startsWith("/Date(") && dateTime.endsWith(")/")) {
        String timestamp = dateTime.substring(6, dateTime.length() - 2);
        // Handle timezone offset if present (e.g., /Date(1234567890123+0000)/)
        int plusIndex = timestamp.indexOf('+');
        int minusIndex = timestamp.indexOf('-');
        if (plusIndex > 0) {
          timestamp = timestamp.substring(0, plusIndex);
        } else if (minusIndex > 0) {
          timestamp = timestamp.substring(0, minusIndex);
        }
        return Long.parseLong(timestamp);
      }

      // Try ISO 8601 format as fallback
      return Instant.parse(dateTime).toEpochMilli();
    } catch (Exception e) {
      LOGGER.warn("Failed to parse date: {}", dateTime, e);
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
