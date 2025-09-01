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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/**
 * Token manager for legacy SharePoint authentication using Azure ACS.
 * This uses the legacy authentication endpoint that supports client secrets
 * for SharePoint REST API access.
 *
 * Note: Apps must be registered via SharePoint's appregnew.aspx page,
 * not through Azure AD portal, to use this authentication method.
 */
public class SharePointLegacyTokenManager extends SharePointTokenManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SharePointLegacyTokenManager.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final String realm;

  /**
   * Creates a token manager for legacy SharePoint authentication.
   *
   * @param clientId The client ID from SharePoint app registration
   * @param clientSecret The client secret from SharePoint app registration
   * @param siteUrl The SharePoint site URL
   * @param realm The SharePoint realm (tenant ID)
   */
  public SharePointLegacyTokenManager(String clientId, String clientSecret,
                                     String siteUrl, String realm) {
    super(realm, clientId, clientSecret, siteUrl);
    this.realm = realm;
  }

  /**
   * Creates a token manager with automatic realm discovery.
   */
  public SharePointLegacyTokenManager(String clientId, String clientSecret, String siteUrl) {
    super(null, clientId, clientSecret, siteUrl);
    // Realm will be discovered on first authentication
    this.realm = null;
  }

  @Override protected void refreshWithClientCredentials() throws IOException {
    String effectiveRealm = realm;

    // If realm not provided, discover it
    if (effectiveRealm == null) {
      effectiveRealm = discoverRealm();
    }

    // Extract SharePoint resource from site URL
    URI uri = URI.create(getSiteUrl());
    String sharePointResource =
        String.format("%s@%s", "00000003-0000-0ff1-ce00-000000000000", // SharePoint principal ID
        effectiveRealm);

    // Build the legacy token endpoint URL
    String tokenUrl =
        String.format("https://accounts.accesscontrol.windows.net/%s/tokens/OAuth/2",
        effectiveRealm);

    // Create client ID in the required format
    String formattedClientId = String.format("%s@%s", clientId, effectiveRealm);

    // Build request body for legacy endpoint
    String requestBody =
        String.format("grant_type=client_credentials&client_id=%s&client_secret=%s&resource=%s",
        URLEncoder.encode(formattedClientId, StandardCharsets.UTF_8),
        URLEncoder.encode(clientSecret, StandardCharsets.UTF_8),
        URLEncoder.encode(sharePointResource, StandardCharsets.UTF_8));

    LOGGER.debug("SharePoint Legacy Authentication:");
    LOGGER.debug("  Token URL: {}", tokenUrl);
    LOGGER.debug("  Client ID: {}", formattedClientId);
    LOGGER.debug("  Resource: {}", sharePointResource);

    // Make token request
    Map<String, Object> response = makeLegacyTokenRequest(tokenUrl, requestBody);

    // Update token from response
    if (response.containsKey("access_token")) {
      this.accessToken = (String) response.get("access_token");

      // Calculate expiry (legacy endpoint returns expires_in as string)
      Object expiresIn = response.get("expires_in");
      int expirySeconds = 3600; // Default to 1 hour
      if (expiresIn != null) {
        try {
          expirySeconds = Integer.parseInt(expiresIn.toString());
        } catch (NumberFormatException e) {
          // Use default
        }
      }
      this.tokenExpiry = Instant.now().plusSeconds(expirySeconds - 60); // Subtract 60 seconds for safety

      LOGGER.debug("  Token acquired successfully (expires in {} seconds)", expirySeconds);
    } else {
      throw new IOException("No access token in response");
    }
  }

  /**
   * Discovers the SharePoint realm (tenant ID) by making an unauthenticated request.
   */
  private String discoverRealm() throws IOException {
    URI uri = URI.create(getSiteUrl());
    URL url =
        URI.create(String.format("https://%s/_vti_bin/client.svc", uri.getHost())).toURL();

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization", "Bearer");

    try {
      conn.getResponseCode(); // This will fail but return realm in header
    } catch (IOException e) {
      // Expected to fail
    }

    String authHeader = conn.getHeaderField("WWW-Authenticate");
    if (authHeader != null && authHeader.contains("Bearer realm=\"")) {
      int start = authHeader.indexOf("Bearer realm=\"") + 14;
      int end = authHeader.indexOf("\"", start);
      String discoveredRealm = authHeader.substring(start, end);
      LOGGER.debug("Discovered SharePoint realm: {}", discoveredRealm);
      return discoveredRealm;
    }

    throw new IOException("Could not discover SharePoint realm from: " + getSiteUrl());
  }

  /**
   * Makes a token request to the legacy Azure ACS endpoint.
   */
  private Map<String, Object> makeLegacyTokenRequest(String tokenUrl, String requestBody)
      throws IOException {
    URL url = URI.create(tokenUrl).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    conn.setRequestProperty("Accept", "application/json");
    conn.setDoOutput(true);

    try (OutputStream os = conn.getOutputStream()) {
      os.write(requestBody.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      String error = "";
      try {
        if (conn.getErrorStream() != null) {
          Map<String, Object> errorResponse = mapper.readValue(conn.getErrorStream(), Map.class);
          if (errorResponse.containsKey("error_description")) {
            error = (String) errorResponse.get("error_description");
          }
        }
      } catch (Exception e) {
        // Ignore JSON parsing errors
      }
      throw new IOException("Legacy token request failed: HTTP " + responseCode +
                          (error.isEmpty() ? "" : " - " + error));
    }

    return mapper.readValue(conn.getInputStream(), Map.class);
  }
}
