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
 * Managed identity authentication for SharePoint.
 */
public class ManagedIdentityAuth implements SharePointAuth {
  private static final String IMDS_ENDPOINT =
      "http://169.254.169.254/metadata/identity/oauth2/token";
  private static final String API_VERSION = "2018-02-01";
  private static final String RESOURCE = "https://graph.microsoft.com/";

  private final String clientId; // Optional - for user-assigned identity
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final ReentrantLock tokenLock;

  private String accessToken;
  private Instant tokenExpiry;

  public ManagedIdentityAuth() {
    this(null);
  }

  public ManagedIdentityAuth(String clientId) {
    this.clientId = clientId;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
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
    StringBuilder urlBuilder = new StringBuilder(IMDS_ENDPOINT);
    urlBuilder.append("?api-version=").append(API_VERSION);
    urlBuilder.append("&resource=").append(URLEncoder.encode(RESOURCE, StandardCharsets.UTF_8));

    if (clientId != null && !clientId.isEmpty()) {
      urlBuilder.append("&client_id=").append(URLEncoder.encode(clientId, StandardCharsets.UTF_8));
    }

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(urlBuilder.toString()))
        .header("Metadata", "true")
        .GET()
        .timeout(Duration.ofSeconds(10))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("Failed to get managed identity token: " + response.body());
    }

    JsonNode json = objectMapper.readTree(response.body());
    accessToken = json.get("access_token").asText();
    String expiresOn = json.get("expires_on").asText();

    // expires_on is Unix timestamp
    long expiryTimestamp = Long.parseLong(expiresOn);
    tokenExpiry = Instant.ofEpochSecond(expiryTimestamp).minusSeconds(300);
  }
}
