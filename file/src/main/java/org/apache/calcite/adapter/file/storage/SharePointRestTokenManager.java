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

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/**
 * Token manager specifically for SharePoint REST API access.
 * Uses SharePoint-specific scopes instead of Microsoft Graph scopes.
 */
public class SharePointRestTokenManager extends SharePointTokenManager {

  /**
   * Creates a token manager for client credentials flow (app-only) with SharePoint REST API.
   */
  public SharePointRestTokenManager(String tenantId, String clientId, String clientSecret, String siteUrl) {
    super(tenantId, clientId, clientSecret, siteUrl);
  }

  /**
   * Creates a token manager for user delegated flow with refresh token for SharePoint REST API.
   */
  public SharePointRestTokenManager(String tenantId, String clientId, String refreshToken,
                                   String siteUrl, boolean isRefreshToken) {
    super(tenantId, clientId, refreshToken, siteUrl, isRefreshToken);
  }

  /**
   * Creates a token manager with a static token for SharePoint REST API.
   */
  public SharePointRestTokenManager(String accessToken, String siteUrl) {
    super(accessToken, siteUrl);
  }

  /**
   * Refreshes token using client credentials (app-only) flow with SharePoint REST API scope.
   */
  @Override protected void refreshWithClientCredentials() throws IOException {
    String tokenUrl =
        String.format(Locale.ROOT, "https://login.microsoftonline.com/%s/oauth2/v2.0/token", super.tenantId);

    // Extract the SharePoint domain from the site URL
    URI uri = URI.create(getSiteUrl());
    String sharePointDomain = uri.getHost();
    
    // Use SharePoint-specific scope for REST API
    // Format: https://{sharepoint-domain}/.default
    String scope = String.format(Locale.ROOT, "https://%s/.default", sharePointDomain);

    // Build request body
    String requestBody =
        String.format(Locale.ROOT, "grant_type=client_credentials&client_id=%s&client_secret=%s&scope=%s",
        URLEncoder.encode(super.clientId, StandardCharsets.UTF_8),
        URLEncoder.encode(super.clientSecret, StandardCharsets.UTF_8),
        URLEncoder.encode(scope, StandardCharsets.UTF_8));

    Map<String, Object> response = makeTokenRequest(tokenUrl, requestBody);
    updateTokenFromResponse(response);
  }

  /**
   * Makes a token request to Azure AD.
   */
  private Map<String, Object> makeTokenRequest(String tokenUrl, String requestBody)
      throws IOException {
    // Use reflection to call the private method from parent class
    try {
      java.lang.reflect.Method method = SharePointTokenManager.class
          .getDeclaredMethod("makeTokenRequest", String.class, String.class);
      method.setAccessible(true);
      return (Map<String, Object>) method.invoke(this, tokenUrl, requestBody);
    } catch (Exception e) {
      throw new IOException("Failed to make token request", e);
    }
  }

  /**
   * Updates token and expiry from Azure AD response.
   */
  private void updateTokenFromResponse(Map<String, Object> response) {
    // Use reflection to call the private method from parent class
    try {
      java.lang.reflect.Method method = SharePointTokenManager.class
          .getDeclaredMethod("updateTokenFromResponse", Map.class);
      method.setAccessible(true);
      method.invoke(this, response);
    } catch (Exception e) {
      throw new RuntimeException("Failed to update token from response", e);
    }
  }
}