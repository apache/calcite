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
package org.apache.calcite.adapter.sharepoint.auth;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Proxy authentication provider for SharePoint.
 * Delegates authentication to an external service following the file adapter pattern.
 * 
 * <p>Phase 3: Full proxy delegation where an external service handles all
 * authentication complexity and returns ready-to-use tokens.
 * 
 * <p>The proxy service is expected to:
 * <ul>
 *   <li>Handle all authentication flows (OAuth2, SAML, certificates, etc.)</li>
 *   <li>Manage token refresh and rotation</li>
 *   <li>Provide audit logging and compliance</li>
 *   <li>Integrate with enterprise credential vaults</li>
 * </ul>
 */
public class ProxyAuthProvider implements SharePointAuthProvider {
  
  private final String siteUrl;
  private final String proxyEndpoint;
  private final @Nullable String apiKey;
  private final @Nullable String resource;
  private final Map<String, String> additionalHeaders;
  private final boolean cacheEnabled;
  private final long cacheTtl;
  
  private @Nullable String accessToken;
  private @Nullable Instant tokenExpiry;
  private @Nullable String cachedTenantId;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final ObjectMapper mapper = new ObjectMapper();
  
  // Buffer time before token expiry to trigger refresh (5 minutes)
  private static final long REFRESH_BUFFER_SECONDS = 300;
  
  /**
   * Creates a ProxyAuthProvider from configuration map.
   */
  public ProxyAuthProvider(Map<String, Object> config) {
    this.siteUrl = (String) config.get("siteUrl");
    if (siteUrl == null) {
      throw new IllegalArgumentException("siteUrl is required");
    }
    
    // Proxy configuration
    @SuppressWarnings("unchecked")
    Map<String, Object> proxyConfig = (Map<String, Object>) config.get("authProxy");
    if (proxyConfig == null) {
      throw new IllegalArgumentException("authProxy configuration is required");
    }
    
    this.proxyEndpoint = (String) proxyConfig.get("endpoint");
    if (proxyEndpoint == null) {
      throw new IllegalArgumentException("authProxy.endpoint is required");
    }
    
    this.apiKey = (String) proxyConfig.get("apiKey");
    this.resource = (String) proxyConfig.get("resource");
    
    // Cache settings
    Object cacheEnabled = proxyConfig.get("cacheEnabled");
    this.cacheEnabled = cacheEnabled == null || Boolean.TRUE.equals(cacheEnabled);
    
    Object cacheTtl = proxyConfig.get("cacheTtl");
    this.cacheTtl = cacheTtl instanceof Number 
        ? ((Number) cacheTtl).longValue() 
        : 300000; // 5 minutes default
    
    // Additional headers for proxy requests
    Map<String, String> headers = new HashMap<>();
    @SuppressWarnings("unchecked")
    Map<String, String> configHeaders = (Map<String, String>) proxyConfig.get("headers");
    if (configHeaders != null) {
      headers.putAll(configHeaders);
    }
    
    // Add standard headers
    if (apiKey != null) {
      String apiKeyHeader = (String) proxyConfig.getOrDefault("apiKeyHeader", "X-API-Key");
      headers.put(apiKeyHeader, apiKey);
    }
    
    this.additionalHeaders = headers;
  }
  
  @Override
  public String getAccessToken() throws IOException {
    if (!cacheEnabled) {
      // Always fetch fresh token if caching is disabled
      return fetchTokenFromProxy();
    }
    
    // Fast path - check with read lock
    lock.readLock().lock();
    try {
      if (isTokenValid()) {
        return accessToken;
      }
    } finally {
      lock.readLock().unlock();
    }
    
    // Slow path - refresh with write lock
    lock.writeLock().lock();
    try {
      // Double-check in case another thread already refreshed
      if (isTokenValid()) {
        return accessToken;
      }
      
      // Fetch new token from proxy
      String token = fetchTokenFromProxy();
      this.accessToken = token;
      this.tokenExpiry = Instant.now().plusMillis(cacheTtl);
      return token;
    } finally {
      lock.writeLock().unlock();
    }
  }
  
  @Override
  public Map<String, String> getAdditionalHeaders() {
    // Return headers that should be added to SharePoint requests
    // (not the proxy request headers)
    Map<String, String> headers = new HashMap<>();
    
    // Add correlation ID if provided by proxy
    if (cachedTenantId != null) {
      headers.put("X-Tenant-Id", cachedTenantId);
    }
    
    return headers;
  }
  
  @Override
  public void invalidateToken() {
    lock.writeLock().lock();
    try {
      this.accessToken = null;
      this.tokenExpiry = null;
    } finally {
      lock.writeLock().unlock();
    }
  }
  
  @Override
  public String getSiteUrl() {
    return siteUrl;
  }
  
  @Override
  public boolean supportsApiType(String apiType) {
    // Proxy can support any API type - it's up to the proxy service
    return true;
  }
  
  @Override
  public String getTenantId() {
    return cachedTenantId;
  }
  
  private boolean isTokenValid() {
    if (!cacheEnabled || accessToken == null || tokenExpiry == null) {
      return false;
    }
    
    // Check if token expires soon
    Instant bufferTime = Instant.now().plusSeconds(REFRESH_BUFFER_SECONDS);
    return tokenExpiry.isAfter(bufferTime);
  }
  
  private String fetchTokenFromProxy() throws IOException {
    // Build proxy request URL
    StringBuilder urlBuilder = new StringBuilder(proxyEndpoint);
    if (!proxyEndpoint.endsWith("/")) {
      urlBuilder.append("/");
    }
    urlBuilder.append("token");
    
    // Add query parameters
    boolean first = true;
    if (resource != null) {
      urlBuilder.append(first ? "?" : "&");
      urlBuilder.append("resource=").append(URLEncoder.encode(resource, StandardCharsets.UTF_8));
      first = false;
    }
    
    if (siteUrl != null) {
      urlBuilder.append(first ? "?" : "&");
      urlBuilder.append("site=").append(URLEncoder.encode(siteUrl, StandardCharsets.UTF_8));
    }
    
    URL url = URI.create(urlBuilder.toString()).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    
    // Add authentication headers for the proxy
    for (Map.Entry<String, String> header : additionalHeaders.entrySet()) {
      conn.setRequestProperty(header.getKey(), header.getValue());
    }
    
    // Add standard headers
    conn.setRequestProperty("Accept", "application/json");
    
    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      String error;
      try (Scanner scanner = new Scanner(conn.getErrorStream(), StandardCharsets.UTF_8)) {
        error = scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
      }
      throw new IOException("Proxy authentication failed (HTTP " + responseCode + "): " + error);
    }
    
    try (Scanner scanner = new Scanner(conn.getInputStream(), StandardCharsets.UTF_8)) {
      String response = scanner.useDelimiter("\\A").next();
      
      // Parse proxy response
      @SuppressWarnings("unchecked")
      Map<String, Object> json = mapper.readValue(response, Map.class);
      
      // Get the access token
      String token = (String) json.get("access_token");
      if (token == null) {
        token = (String) json.get("token");
      }
      if (token == null) {
        throw new IOException("Proxy response missing access_token or token field");
      }
      
      // Cache tenant ID if provided
      String tenantId = (String) json.get("tenant_id");
      if (tenantId == null) {
        tenantId = (String) json.get("tenantId");
      }
      this.cachedTenantId = tenantId;
      
      // Update expiry if provided
      Object expiresIn = json.get("expires_in");
      if (expiresIn instanceof Number) {
        long expirySeconds = ((Number) expiresIn).longValue();
        this.tokenExpiry = Instant.now().plusSeconds(expirySeconds);
      }
      
      return token;
    }
  }
}