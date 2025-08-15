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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Storage provider implementation for HTTP/HTTPS access.
 * Supports both GET and POST requests with configurable headers and body.
 */
public class HttpStorageProvider implements StorageProvider {

  private final String method;
  private final @Nullable String requestBody;
  private final Map<String, String> headers;
  private final @Nullable String mimeTypeOverride;
  private final @Nullable HttpConfig config;

  // Cache for HTTP responses (Phase 2 enhancement)
  private final Map<String, CachedResponse> cache = new ConcurrentHashMap<>();
  
  // Persistent cache for restart-survivable caching
  private PersistentStorageCache persistentCache;

  /**
   * Cached HTTP response with ETag support.
   */
  private static class CachedResponse {
    final byte[] data;
    final String etag;
    final long lastModified;
    final long cachedAt;

    CachedResponse(byte[] data, String etag, long lastModified) {
      this.data = data;
      this.etag = etag;
      this.lastModified = lastModified;
      this.cachedAt = System.currentTimeMillis();
    }

    boolean isExpired(long ttl) {
      return System.currentTimeMillis() - cachedAt > ttl;
    }
  }

  /**
   * Request/response for proxy pattern (Phase 3).
   */
  public static class ProxyRequest {
    public String url;
    public String method;
    public Map<String, String> headers;
    public String body;
  }

  public static class ProxyResponse {
    public int status;
    public Map<String, String> headers;
    public String body;
  }

  /**
   * Creates an HTTP storage provider with default GET method.
   */
  public HttpStorageProvider() {
    this("GET", null, new HashMap<>(), null);
  }

  /**
   * Creates an HTTP storage provider with specified configuration.
   *
   * @param method HTTP method (GET, POST, etc.)
   * @param requestBody Request body for POST/PUT requests
   * @param headers Additional HTTP headers
   * @param mimeTypeOverride Override for Content-Type detection
   */
  public HttpStorageProvider(
      String method,
      @Nullable String requestBody,
      Map<String, String> headers,
      @Nullable String mimeTypeOverride) {
    this(method, requestBody, headers, mimeTypeOverride, null);
  }

  /**
   * Creates an HTTP storage provider with auth configuration.
   *
   * @param method HTTP method
   * @param requestBody Request body
   * @param headers HTTP headers
   * @param mimeTypeOverride MIME type override
   * @param config Auth and cache configuration
   */
  public HttpStorageProvider(
      String method,
      @Nullable String requestBody,
      Map<String, String> headers,
      @Nullable String mimeTypeOverride,
      @Nullable HttpConfig config) {
    this.method = method != null ? method : "GET";
    this.requestBody = requestBody;
    this.headers = headers != null ? headers : new HashMap<>();
    this.mimeTypeOverride = mimeTypeOverride;
    this.config = config;
    
    // Initialize persistent cache if cache manager is available
    try {
      this.persistentCache = StorageCacheManager.getInstance().getCache("http");
    } catch (IllegalStateException e) {
      // Cache manager not initialized, persistent cache will be null
      this.persistentCache = null;
    }
  }

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    // HTTP doesn't support directory listing
    throw new UnsupportedOperationException("HTTP storage does not support directory listing");
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    // For POST requests with body, we can't use HEAD method
    // Instead, we'll use the actual request method and close immediately
    if ("POST".equalsIgnoreCase(method) && requestBody != null) {
      HttpURLConnection conn = createConnection(path);
      try {
        executeRequest(conn);

        long contentLength = conn.getContentLengthLong();
        long lastModified = conn.getLastModified();
        String contentType = getEffectiveContentType(conn);
        String etag = conn.getHeaderField("ETag");

        return new FileMetadata(path, contentLength, lastModified, contentType, etag);
      } finally {
        conn.disconnect();
      }
    }

    // For GET requests, use HEAD method for metadata
    URL url;
    try {
      url = new URI(path).toURL();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URL: " + path, e);
    }
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("HEAD");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);
    applyHeaders(conn);

    try {
      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException("HTTP error code: " + responseCode + " for URL: " + path);
      }

      long contentLength = conn.getContentLengthLong();
      long lastModified = conn.getLastModified();
      String contentType = getEffectiveContentType(conn);
      String etag = conn.getHeaderField("ETag");

      return new FileMetadata(path, contentLength, lastModified, contentType, etag);
    } finally {
      conn.disconnect();
    }
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    // Phase 3: Use proxy if configured
    if (config != null && config.getProxyEndpoint() != null) {
      return callViaProxy(path);
    }

    // Check persistent cache first if available
    if (persistentCache != null) {
      byte[] cachedData = persistentCache.getCachedData(path);
      FileMetadata cachedMetadata = persistentCache.getCachedMetadata(path);
      
      if (cachedData != null && cachedMetadata != null) {
        // Try conditional request to check if data is still fresh
        try {
          if (!hasChanged(path, cachedMetadata)) {
            return new ByteArrayInputStream(cachedData);
          }
        } catch (IOException e) {
          // If we can't check freshness, use cached data anyway
          return new ByteArrayInputStream(cachedData);
        }
      }
    }

    // Check in-memory cache if enabled (for backward compatibility)
    if (config != null && config.isCacheEnabled()) {
      CachedResponse cached = cache.get(path);
      if (cached != null && !cached.isExpired(config.getCacheTtl())) {
        // Try conditional request with ETag
        HttpURLConnection conn = createConnection(path);
        if (cached.etag != null) {
          conn.setRequestProperty("If-None-Match", cached.etag);
        }
        if (cached.lastModified > 0) {
          conn.setIfModifiedSince(cached.lastModified);
        }

        executeRequest(conn);

        if (conn.getResponseCode() == HttpURLConnection.HTTP_NOT_MODIFIED) {
          conn.disconnect();
          return new ByteArrayInputStream(cached.data);
        }

        // Cache miss, read and cache new data
        byte[] data = readAllBytes(conn.getInputStream());
        String etag = conn.getHeaderField("ETag");
        long lastModified = conn.getLastModified();
        cache.put(path, new CachedResponse(data, etag, lastModified));
        
        // Also cache in persistent cache if available
        if (persistentCache != null) {
          FileMetadata metadata = new FileMetadata(path, data.length, lastModified, 
              getEffectiveContentType(conn), etag);
          long ttl = config.getCacheTtl();
          persistentCache.cacheData(path, data, metadata, ttl);
        }
        
        conn.disconnect();
        return new ByteArrayInputStream(data);
      }
    }

    // Normal request
    HttpURLConnection conn = createConnection(path);
    executeRequest(conn);

    byte[] data = readAllBytes(conn.getInputStream());
    String etag = conn.getHeaderField("ETag");
    long lastModified = conn.getLastModified();
    String contentType = getEffectiveContentType(conn);
    
    // Cache in memory if enabled
    if (config != null && config.isCacheEnabled()) {
      cache.put(path, new CachedResponse(data, etag, lastModified));
    }
    
    // Cache in persistent cache if available
    if (persistentCache != null) {
      FileMetadata metadata = new FileMetadata(path, data.length, lastModified, contentType, etag);
      long ttl = (config != null && config.isCacheEnabled()) ? config.getCacheTtl() : 0;
      persistentCache.cacheData(path, data, metadata, ttl);
    }
    
    conn.disconnect();
    return new ByteArrayInputStream(data);
  }

  @Override public Reader openReader(String path) throws IOException {
    return new InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) throws IOException {
    try {
      // For POST with body, we can't use HEAD
      if ("POST".equalsIgnoreCase(method) && requestBody != null) {
        HttpURLConnection conn = createConnection(path);
        try {
          executeRequest(conn);
          return true;
        } finally {
          conn.disconnect();
        }
      }

      // For GET, use HEAD method
      URL url = new URI(path).toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("HEAD");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(30000);
      applyHeaders(conn);

      try {
        int responseCode = conn.getResponseCode();
        return responseCode == HttpURLConnection.HTTP_OK;
      } finally {
        conn.disconnect();
      }
    } catch (IOException | URISyntaxException e) {
      return false;
    }
  }

  @Override public boolean isDirectory(String path) {
    // HTTP URLs are never directories in the traditional sense
    return false;
  }

  @Override public String getStorageType() {
    return "http";
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    try {
      URI baseUri = new URI(basePath);
      URI resolved = baseUri.resolve(relativePath);
      return resolved.toString();
    } catch (Exception e) {
      // Fallback to simple string concatenation
      if (basePath.endsWith("/")) {
        return basePath + relativePath;
      } else {
        return basePath + "/" + relativePath;
      }
    }
  }

  /**
   * Creates and configures an HTTP connection.
   *
   * @param path The URL path
   * @return Configured HttpURLConnection
   * @throws IOException if connection fails
   */
  private HttpURLConnection createConnection(String path) throws IOException {
    URL url;
    try {
      url = new URI(path).toURL();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URL: " + path, e);
    }

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(method);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    // Apply custom headers
    applyHeaders(conn);

    // Set up for POST body if needed
    if (requestBody != null && ("POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method))) {
      conn.setDoOutput(true);
      if (!headers.containsKey("Content-Type")) {
        conn.setRequestProperty("Content-Type", "application/json");
      }
    }

    return conn;
  }

  /**
   * Applies custom headers to the connection.
   *
   * @param conn The connection to configure
   */
  private void applyHeaders(HttpURLConnection conn) {
    // Apply standard headers
    for (Map.Entry<String, String> header : headers.entrySet()) {
      conn.setRequestProperty(header.getKey(), header.getValue());
    }

    // Apply auth headers if configured
    if (config != null) {
      try {
        applyAuth(conn);
      } catch (IOException e) {
        // Log but continue - auth might not be required
        // In production, use proper logging
      }
    }
  }

  /**
   * Applies authentication based on configuration.
   * Implements Phase 1 and Phase 2 auth methods.
   *
   * @param conn The connection to configure
   * @throws IOException if auth setup fails
   */
  private void applyAuth(HttpURLConnection conn) throws IOException {
    if (config == null) {
      return;
    }

    // Phase 1: Simple static auth
    if (config.getBearerToken() != null) {
      conn.setRequestProperty("Authorization", "Bearer " + config.getBearerToken());
      return;
    }

    if (config.getApiKey() != null) {
      conn.setRequestProperty("X-API-Key", config.getApiKey());
      return;
    }

    if (config.getUsername() != null && config.getPassword() != null) {
      String encoded =
          Base64.getEncoder().encodeToString((config.getUsername() + ":" + config.getPassword()).getBytes(StandardCharsets.UTF_8));
      conn.setRequestProperty("Authorization", "Basic " + encoded);
      return;
    }

    // Phase 2: External token sources
    String token = null;

    if (config.getTokenEnv() != null) {
      token = getEnvironmentVariable(config.getTokenEnv());
    } else if (config.getTokenFile() != null) {
      File tokenFile = new File(config.getTokenFile());
      if (tokenFile.exists()) {
        token = new String(Files.readAllBytes(Paths.get(config.getTokenFile())), StandardCharsets.UTF_8).trim();
      }
    } else if (config.getTokenCommand() != null) {
      ProcessBuilder pb = new ProcessBuilder(config.getTokenCommand().split(" "));
      Process process = pb.start();
      try {
        int exitCode = process.waitFor();
        if (exitCode == 0) {
          try (InputStream is = process.getInputStream()) {
            token = new String(readAllBytes(is), StandardCharsets.UTF_8).trim();
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Token command interrupted", e);
      }
    } else if (config.getTokenEndpoint() != null) {
      token = fetchTokenFromEndpoint(config.getTokenEndpoint());
    }

    // Apply token with template headers
    if (token != null) {
      if (config.getAuthHeaders() != null) {
        // Apply template headers
        for (Map.Entry<String, String> header : config.getAuthHeaders().entrySet()) {
          String value = header.getValue().replace("${token}", token);
          conn.setRequestProperty(header.getKey(), value);
        }
      } else {
        // Default to Bearer token
        conn.setRequestProperty("Authorization", "Bearer " + token);
      }
    }
  }

  /**
   * Executes the HTTP request, including sending body for POST/PUT.
   *
   * @param conn The connection to execute
   * @throws IOException if request fails
   */
  private void executeRequest(HttpURLConnection conn) throws IOException {
    // Send request body if needed
    if (requestBody != null && ("POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method))) {
      try (OutputStream os = conn.getOutputStream()) {
        byte[] input = requestBody.getBytes(StandardCharsets.UTF_8);
        os.write(input, 0, input.length);
      }
    }

    // Check response code (304 is valid for cached responses)
    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK &&
        responseCode != HttpURLConnection.HTTP_CREATED &&
        responseCode != HttpURLConnection.HTTP_NOT_MODIFIED) {
      conn.disconnect();
      throw new IOException("HTTP error code: " + responseCode + " for URL: " + conn.getURL());
    }
  }

  /**
   * Fetches a token from an HTTP endpoint.
   *
   * @param endpoint The token endpoint URL
   * @return The token string
   * @throws IOException if fetch fails
   */
  private String fetchTokenFromEndpoint(String endpoint) throws IOException {
    URL url;
    try {
      url = new URI(endpoint).toURL();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid token endpoint URL: " + endpoint, e);
    }
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);

    try {
      int responseCode = conn.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        try (InputStream is = conn.getInputStream()) {
          return new String(readAllBytes(is), StandardCharsets.UTF_8).trim();
        }
      }
    } finally {
      conn.disconnect();
    }

    return null;
  }

  /**
   * Calls via proxy for complex auth (Phase 3).
   *
   * @param targetUrl The target URL
   * @return Input stream of the response
   * @throws IOException if proxy call fails
   */
  private InputStream callViaProxy(String targetUrl) throws IOException {
    ProxyRequest request = new ProxyRequest();
    request.url = targetUrl;
    request.method = this.method;
    request.headers = new HashMap<>(this.headers);
    request.body = this.requestBody;

    ObjectMapper mapper = new ObjectMapper();
    String jsonRequest = mapper.writeValueAsString(request);

    URL proxyUrl;
    try {
      proxyUrl = new URI(config.getProxyEndpoint()).toURL();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid proxy endpoint URL: " + config.getProxyEndpoint(), e);
    }
    HttpURLConnection conn = (HttpURLConnection) proxyUrl.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    // Send request to proxy
    try (OutputStream os = conn.getOutputStream()) {
      os.write(jsonRequest.getBytes(StandardCharsets.UTF_8));
    }

    // Get response from proxy
    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      conn.disconnect();
      throw new IOException("Proxy error: " + responseCode);
    }

    // Parse proxy response
    ProxyResponse response;
    try (InputStream is = conn.getInputStream()) {
      response = mapper.readValue(is, ProxyResponse.class);
    }
    conn.disconnect();

    // Check proxied response status
    if (response.status != 200 && response.status != 201) {
      throw new IOException("Proxied request failed: " + response.status);
    }

    // Return the proxied data
    return new ByteArrayInputStream(response.body.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Reads all bytes from an input stream.
   *
   * @param is The input stream
   * @return Byte array of contents
   * @throws IOException if read fails
   */
  private byte[] readAllBytes(InputStream is) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    int nRead;
    byte[] data = new byte[16384];
    while ((nRead = is.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }
    return buffer.toByteArray();
  }

  /**
   * Gets the effective content type, considering override.
   *
   * @param conn The connection
   * @return The content type to use
   */
  private String getEffectiveContentType(HttpURLConnection conn) {
    if (mimeTypeOverride != null) {
      return mimeTypeOverride;
    }
    return conn.getContentType();
  }

  /**
   * Gets an environment variable value.
   * Protected to allow overriding in tests.
   *
   * @param name The environment variable name
   * @return The value or null if not set
   */
  protected String getEnvironmentVariable(String name) {
    return System.getenv(name);
  }
}
