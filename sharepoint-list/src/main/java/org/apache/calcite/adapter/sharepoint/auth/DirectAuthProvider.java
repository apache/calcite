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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Direct authentication provider for SharePoint.
 * Supports multiple authentication methods following the file adapter pattern:
 * <ul>
 *   <li>Phase 1: Client credentials (client ID + secret)</li>
 *   <li>Phase 1: Certificate authentication</li>
 *   <li>Phase 1: Static access token</li>
 *   <li>Phase 2: Token from command execution</li>
 *   <li>Phase 2: Token from environment variable</li>
 *   <li>Phase 2: Token from file</li>
 *   <li>Phase 2: Token from endpoint</li>
 *   <li>Phase 2: Legacy SharePoint auth (for on-premises)</li>
 * </ul>
 */
public class DirectAuthProvider implements SharePointAuthProvider {
  
  private final String siteUrl;
  private final @Nullable String tenantId;
  private final @Nullable String clientId;
  private final @Nullable String clientSecret;
  private final @Nullable String certificatePath;
  private final @Nullable String certificatePassword;
  private final @Nullable String staticToken;
  private final @Nullable String tokenCommand;
  private final @Nullable String tokenEnv;
  private final @Nullable String tokenFile;
  private final @Nullable String tokenEndpoint;
  private final boolean useLegacyAuth;
  private final boolean useGraphApi;
  private final Map<String, String> additionalHeaders;
  
  private @Nullable String accessToken;
  private @Nullable Instant tokenExpiry;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final ObjectMapper mapper = new ObjectMapper();
  
  // Buffer time before token expiry to trigger refresh (5 minutes)
  private static final long REFRESH_BUFFER_SECONDS = 300;
  
  /**
   * Creates a DirectAuthProvider from configuration map.
   */
  public DirectAuthProvider(Map<String, Object> config) {
    this.siteUrl = (String) config.get("siteUrl");
    if (siteUrl == null) {
      throw new IllegalArgumentException("siteUrl is required");
    }
    
    // Phase 1: Direct auth configuration
    this.tenantId = (String) config.get("tenantId");
    this.clientId = (String) config.get("clientId");
    this.clientSecret = (String) config.get("clientSecret");
    this.certificatePath = (String) config.get("certificatePath");
    this.certificatePassword = (String) config.get("certificatePassword");
    this.staticToken = (String) config.get("accessToken");
    
    // Phase 2: External token sources
    this.tokenCommand = (String) config.get("tokenCommand");
    this.tokenEnv = (String) config.get("tokenEnv");
    this.tokenFile = (String) config.get("tokenFile");
    this.tokenEndpoint = (String) config.get("tokenEndpoint");
    
    // API selection
    this.useLegacyAuth = Boolean.TRUE.equals(config.get("useLegacyAuth"));
    this.useGraphApi = Boolean.TRUE.equals(config.get("useGraphApi"));
    
    // Additional headers
    @SuppressWarnings("unchecked")
    Map<String, String> headers = (Map<String, String>) config.get("additionalHeaders");
    this.additionalHeaders = headers != null ? headers : Collections.emptyMap();
  }
  
  @Override
  public String getAccessToken() throws IOException {
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
      
      // Refresh the token
      refreshAccessToken();
      return accessToken;
    } finally {
      lock.writeLock().unlock();
    }
  }
  
  @Override
  public Map<String, String> getAdditionalHeaders() {
    return additionalHeaders;
  }
  
  @Override
  public void invalidateToken() {
    lock.writeLock().lock();
    try {
      this.tokenExpiry = Instant.now().minusSeconds(1);
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
    if ("graph".equals(apiType)) {
      // Graph API works with client credentials and static tokens
      return !useLegacyAuth;
    } else if ("rest".equals(apiType)) {
      // REST API works with legacy auth, certificates, or static tokens
      return true;
    }
    return false;
  }
  
  @Override
  public String getTenantId() {
    return tenantId;
  }
  
  private boolean isTokenValid() {
    if (accessToken == null || tokenExpiry == null) {
      return false;
    }
    
    // Check if token expires soon
    Instant bufferTime = Instant.now().plusSeconds(REFRESH_BUFFER_SECONDS);
    return tokenExpiry.isAfter(bufferTime);
  }
  
  private void refreshAccessToken() throws IOException {
    // Phase 2: External token sources (highest priority)
    if (tokenCommand != null) {
      refreshFromCommand();
    } else if (tokenEnv != null) {
      refreshFromEnvironment();
    } else if (tokenFile != null) {
      refreshFromFile();
    } else if (tokenEndpoint != null) {
      refreshFromEndpoint();
    }
    // Phase 1: Direct authentication
    else if (staticToken != null) {
      useStaticToken();
    } else if (clientSecret != null) {
      refreshWithClientCredentials();
    } else if (certificatePath != null) {
      refreshWithCertificate();
    } else {
      throw new IOException("No valid authentication method configured");
    }
  }
  
  private void refreshFromCommand() throws IOException {
    try {
      ProcessBuilder pb = new ProcessBuilder(tokenCommand.split("\\s+"));
      Process process = pb.start();
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        this.accessToken = reader.readLine();
        this.tokenExpiry = Instant.now().plusSeconds(3600); // Assume 1 hour
      }
      
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new IOException("Token command failed with exit code: " + exitCode);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Token command interrupted", e);
    }
  }
  
  private void refreshFromEnvironment() {
    String token = System.getenv(tokenEnv);
    if (token == null || token.isEmpty()) {
      throw new IllegalStateException("Environment variable " + tokenEnv + " is not set");
    }
    this.accessToken = token;
    this.tokenExpiry = Instant.now().plusSeconds(3600); // Assume 1 hour
  }
  
  private void refreshFromFile() throws IOException {
    File file = new File(tokenFile);
    if (!file.exists()) {
      throw new IOException("Token file not found: " + tokenFile);
    }
    
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      this.accessToken = reader.readLine();
      this.tokenExpiry = Instant.now().plusSeconds(3600); // Assume 1 hour
    }
  }
  
  private void refreshFromEndpoint() throws IOException {
    URL url = URI.create(tokenEndpoint).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    
    // Add any authentication headers for the token endpoint
    for (Map.Entry<String, String> header : additionalHeaders.entrySet()) {
      conn.setRequestProperty(header.getKey(), header.getValue());
    }
    
    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("Token endpoint returned status: " + responseCode);
    }
    
    try (Scanner scanner = new Scanner(conn.getInputStream(), StandardCharsets.UTF_8)) {
      String response = scanner.useDelimiter("\\A").next();
      
      // Try to parse as JSON first
      try {
        @SuppressWarnings("unchecked")
        Map<String, Object> json = mapper.readValue(response, Map.class);
        this.accessToken = (String) json.get("access_token");
        if (this.accessToken == null) {
          this.accessToken = (String) json.get("token");
        }
        
        // Get expiry if provided
        Object expiresIn = json.get("expires_in");
        long expirySeconds = 3600; // Default 1 hour
        if (expiresIn instanceof Number) {
          expirySeconds = ((Number) expiresIn).longValue();
        }
        this.tokenExpiry = Instant.now().plusSeconds(expirySeconds);
      } catch (Exception e) {
        // Assume plain text token
        this.accessToken = response.trim();
        this.tokenExpiry = Instant.now().plusSeconds(3600);
      }
    }
  }
  
  private void useStaticToken() {
    this.accessToken = staticToken;
    this.tokenExpiry = Instant.now().plusSeconds(3600); // Assume 1 hour
  }
  
  private void refreshWithClientCredentials() throws IOException {
    if (tenantId == null || clientId == null) {
      throw new IllegalStateException("tenantId and clientId required for client credentials");
    }
    
    String tokenUrl;
    String scope;
    
    if (useLegacyAuth) {
      // Legacy SharePoint authentication
      tokenUrl = String.format(Locale.ROOT, 
          "https://accounts.accesscontrol.windows.net/%s/tokens/OAuth/2", tenantId);
      
      // Extract SharePoint resource from site URL
      URI uri = URI.create(siteUrl);
      String resource = uri.getHost();
      scope = String.format(Locale.ROOT, "00000003-0000-0ff1-ce00-000000000000/%s@%s", 
          resource, tenantId);
    } else {
      // Modern Azure AD authentication
      tokenUrl = String.format(Locale.ROOT, 
          "https://login.microsoftonline.com/%s/oauth2/v2.0/token", tenantId);
      
      if (useGraphApi) {
        scope = "https://graph.microsoft.com/.default";
      } else {
        // SharePoint REST API scope
        URI uri = URI.create(siteUrl);
        scope = String.format(Locale.ROOT, "https://%s/.default", uri.getHost());
      }
    }
    
    // Build request body
    String requestBody = String.format(Locale.ROOT, 
        "grant_type=client_credentials&client_id=%s&client_secret=%s&scope=%s",
        URLEncoder.encode(clientId, StandardCharsets.UTF_8),
        URLEncoder.encode(clientSecret, StandardCharsets.UTF_8),
        URLEncoder.encode(scope, StandardCharsets.UTF_8));
    
    Map<String, Object> response = makeTokenRequest(tokenUrl, requestBody);
    updateTokenFromResponse(response);
  }
  
  private void refreshWithCertificate() throws IOException {
    // Certificate authentication would require additional dependencies
    // For now, throw an informative error
    throw new UnsupportedOperationException(
        "Certificate authentication not yet implemented. " +
        "Use client credentials or external token source instead.");
  }
  
  private Map<String, Object> makeTokenRequest(String tokenUrl, String requestBody) 
      throws IOException {
    URL url = URI.create(tokenUrl).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    conn.setDoOutput(true);
    
    // Send request
    conn.getOutputStream().write(requestBody.getBytes(StandardCharsets.UTF_8));
    
    // Read response
    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      try (Scanner scanner = new Scanner(conn.getErrorStream(), StandardCharsets.UTF_8)) {
        String error = scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
        throw new IOException("Failed to refresh access token: " + error);
      }
    }
    
    // Parse response
    try (Scanner scanner = new Scanner(conn.getInputStream(), StandardCharsets.UTF_8)) {
      String response = scanner.useDelimiter("\\A").next();
      @SuppressWarnings("unchecked")
      Map<String, Object> json = mapper.readValue(response, Map.class);
      return json;
    }
  }
  
  private void updateTokenFromResponse(Map<String, Object> response) {
    this.accessToken = (String) response.get("access_token");
    
    // Calculate expiry time
    Object expiresIn = response.get("expires_in");
    long expirySeconds = 3600; // Default 1 hour
    if (expiresIn instanceof Number) {
      expirySeconds = ((Number) expiresIn).longValue();
    }
    
    this.tokenExpiry = Instant.now().plusSeconds(expirySeconds);
  }
}