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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages SharePoint access tokens with automatic refresh.
 *
 * Supports multiple authentication flows:
 * 1. Client Credentials (app-only)
 * 2. User delegated with refresh token
 * 3. Static token (for testing)
 */
public class SharePointTokenManager {

  protected final String tenantId;
  protected final String clientId;
  protected final String clientSecret;
  protected final String refreshToken;
  private final String siteUrl;

  protected String accessToken;
  protected Instant tokenExpiry;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final ObjectMapper mapper = new ObjectMapper();

  // Buffer time before token expiry to trigger refresh (5 minutes)
  private static final long REFRESH_BUFFER_SECONDS = 300;

  /**
   * Creates a token manager for client credentials flow (app-only).
   */
  public SharePointTokenManager(String tenantId, String clientId, String clientSecret, String siteUrl) {
    this.tenantId = tenantId;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.refreshToken = null;
    this.siteUrl = siteUrl;
  }

  /**
   * Creates a token manager for user delegated flow with refresh token.
   */
  public SharePointTokenManager(String tenantId, String clientId, String refreshToken,
                               String siteUrl, boolean isRefreshToken) {
    this.tenantId = tenantId;
    this.clientId = clientId;
    this.clientSecret = null;
    this.refreshToken = refreshToken;
    this.siteUrl = siteUrl;
  }

  /**
   * Creates a token manager with a static token (no refresh).
   */
  public SharePointTokenManager(String accessToken, String siteUrl) {
    this.tenantId = null;
    this.clientId = null;
    this.clientSecret = null;
    this.refreshToken = null;
    this.siteUrl = siteUrl;
    this.accessToken = accessToken;
    this.tokenExpiry = Instant.now().plusSeconds(3600); // Assume 1 hour validity
  }

  /**
   * Gets a valid access token, refreshing if necessary.
   */
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

  /**
   * Checks if the current token is still valid.
   */
  private boolean isTokenValid() {
    if (accessToken == null || tokenExpiry == null) {
      return false;
    }

    // Check if token expires soon
    Instant bufferTime = Instant.now().plusSeconds(REFRESH_BUFFER_SECONDS);
    return tokenExpiry.isAfter(bufferTime);
  }

  /**
   * Refreshes the access token based on the authentication type.
   */
  private void refreshAccessToken() throws IOException {
    if (clientSecret != null) {
      // Client credentials flow
      refreshWithClientCredentials();
    } else if (refreshToken != null) {
      // Refresh token flow
      refreshWithRefreshToken();
    } else if (accessToken != null) {
      // Static token - check if expired
      if (tokenExpiry.isBefore(Instant.now())) {
        throw new IOException("Static access token has expired. Please provide a new token.");
      }
    } else {
      throw new IOException("No valid authentication method configured");
    }
  }

  /**
   * Refreshes token using client credentials (app-only) flow.
   */
  protected void refreshWithClientCredentials() throws IOException {
    String tokenUrl = String.format(Locale.ROOT, "https://login.microsoftonline.com/%s/oauth2/v2.0/token", tenantId);

    // For Microsoft Graph API, use graph scope
    // For SharePoint REST API, use SharePoint resource scope
    String scope = "https://graph.microsoft.com/.default";

    // Build request body
    String requestBody =
        String.format(Locale.ROOT, "grant_type=client_credentials&client_id=%s&client_secret=%s&scope=%s",
        URLEncoder.encode(clientId, StandardCharsets.UTF_8),
        URLEncoder.encode(clientSecret, StandardCharsets.UTF_8),
        URLEncoder.encode(scope, StandardCharsets.UTF_8));

    Map<String, Object> response = makeTokenRequest(tokenUrl, requestBody);
    updateTokenFromResponse(response);
  }

  /**
   * Refreshes token using refresh token flow.
   */
  private void refreshWithRefreshToken() throws IOException {
    String tokenUrl = String.format(Locale.ROOT, "https://login.microsoftonline.com/%s/oauth2/v2.0/token", tenantId);

    // Build request body
    String requestBody =
        String.format(Locale.ROOT, "grant_type=refresh_token&client_id=%s&refresh_token=%s&scope=Sites.Read.All",
        URLEncoder.encode(clientId, StandardCharsets.UTF_8),
        URLEncoder.encode(refreshToken, StandardCharsets.UTF_8));

    Map<String, Object> response = makeTokenRequest(tokenUrl, requestBody);
    updateTokenFromResponse(response);
  }

  /**
   * Makes a token request to Azure AD.
   */
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
      return mapper.readValue(response, Map.class);
    }
  }

  /**
   * Updates token and expiry from Azure AD response.
   */
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

  /**
   * Gets the site URL.
   */
  public String getSiteUrl() {
    return siteUrl;
  }

  /**
   * Forces a token refresh on the next request.
   */
  public void invalidateToken() {
    lock.writeLock().lock();
    try {
      this.tokenExpiry = Instant.now().minusSeconds(1);
    } finally {
      lock.writeLock().unlock();
    }
  }
}
