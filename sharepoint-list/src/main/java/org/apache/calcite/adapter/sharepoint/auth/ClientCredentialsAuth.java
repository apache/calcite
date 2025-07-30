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
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

/**
 * OAuth2 client credentials authentication for SharePoint.
 */
public class ClientCredentialsAuth implements SharePointAuth {
  private static final String TOKEN_ENDPOINT =
      "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
  private static final String SCOPE = "https://graph.microsoft.com/.default";

  private final String clientId;
  private final String clientSecret;
  private final String tenantId;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final ReentrantLock tokenLock;

  private String accessToken;
  private Instant tokenExpiry;

  public ClientCredentialsAuth(String clientId, String clientSecret, String tenantId) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.tenantId = tenantId;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
    this.objectMapper = new ObjectMapper();
    this.tokenLock = new ReentrantLock();
  }

  @Override public String getAccessToken() throws IOException, InterruptedException {
    tokenLock.lock();
    try {
      if (accessToken == null || isTokenExpired()) {
        refreshToken();
      }
      return accessToken;
    } finally {
      tokenLock.unlock();
    }
  }

  private boolean isTokenExpired() {
    return tokenExpiry == null || Instant.now().isAfter(tokenExpiry);
  }

  private void refreshToken() throws IOException, InterruptedException {
    String tokenUrl = TOKEN_ENDPOINT.replace("%s", tenantId);

    String formData = "client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
        + "&client_secret=" + URLEncoder.encode(clientSecret, StandardCharsets.UTF_8)
        + "&scope=" + URLEncoder.encode(SCOPE, StandardCharsets.UTF_8)
        + "&grant_type=client_credentials";

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(tokenUrl))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString(formData))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("Failed to authenticate: " + response.body());
    }

    JsonNode json = objectMapper.readTree(response.body());
    accessToken = json.get("access_token").asText();
    int expiresIn = json.get("expires_in").asInt();

    tokenExpiry = Instant.now().plusSeconds(expiresIn - 300);
  }
}
