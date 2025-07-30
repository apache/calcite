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
import java.util.function.Consumer;

/**
 * Device code authentication for SharePoint.
 */
public class DeviceCodeAuth implements SharePointAuth {
  private static final String DEVICE_CODE_ENDPOINT =
      "https://login.microsoftonline.com/%s/oauth2/v2.0/devicecode";
  private static final String TOKEN_ENDPOINT =
      "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
  private static final String SCOPE = "https://graph.microsoft.com/.default offline_access";

  private final String clientId;
  private final String tenantId;
  private final Consumer<String> messageHandler;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final ReentrantLock tokenLock;

  private String accessToken;
  private String refreshToken;
  private Instant tokenExpiry;

  public DeviceCodeAuth(String clientId, String tenantId, Consumer<String> messageHandler) {
    this.clientId = clientId;
    this.tenantId = tenantId;
    this.messageHandler = messageHandler != null ? messageHandler : System.out::println;
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
        if (refreshToken != null) {
          refreshWithToken();
        } else {
          performDeviceCodeFlow();
        }
      }
      return accessToken;
    } finally {
      tokenLock.unlock();
    }
  }

  private boolean isTokenExpired() {
    return tokenExpiry == null || Instant.now().isAfter(tokenExpiry);
  }

  private void performDeviceCodeFlow() throws IOException, InterruptedException {
    // Step 1: Get device code
    String deviceCodeUrl = DEVICE_CODE_ENDPOINT.replace("%s", tenantId);

    String formData = "client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
        + "&scope=" + URLEncoder.encode(SCOPE, StandardCharsets.UTF_8);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(deviceCodeUrl))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString(formData))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("Failed to get device code: " + response.body());
    }

    JsonNode json = objectMapper.readTree(response.body());
    String deviceCode = json.get("device_code").asText();
    String userCode = json.get("user_code").asText();
    String verificationUri = json.get("verification_uri").asText();
    int interval = json.get("interval").asInt();
    int expiresIn = json.get("expires_in").asInt();

    // Step 2: Display message to user
    messageHandler.accept(
        "To sign in, use a web browser to open the page " + verificationUri
        + " and enter the code " + userCode);

    // Step 3: Poll for token
    long endTime = System.currentTimeMillis() + (expiresIn * 1000L);

    while (System.currentTimeMillis() < endTime) {
      Thread.sleep(interval * 1000L);

      if (tryGetToken(deviceCode)) {
        return;
      }
    }

    throw new IOException("Device code flow timed out");
  }

  private boolean tryGetToken(String deviceCode) throws IOException, InterruptedException {
    String tokenUrl = TOKEN_ENDPOINT.replace("%s", tenantId);

    String formData = "client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
        + "&grant_type=urn:ietf:params:oauth:grant-type:device_code"
        + "&device_code=" + URLEncoder.encode(deviceCode, StandardCharsets.UTF_8);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(tokenUrl))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString(formData))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      JsonNode json = objectMapper.readTree(response.body());
      accessToken = json.get("access_token").asText();
      refreshToken = json.has("refresh_token") ? json.get("refresh_token").asText() : null;
      int expiresIn = json.get("expires_in").asInt();
      tokenExpiry = Instant.now().plusSeconds(expiresIn - 300);
      return true;
    } else if (response.statusCode() == 400) {
      JsonNode json = objectMapper.readTree(response.body());
      String error = json.get("error").asText();
      if ("authorization_pending".equals(error)) {
        return false; // Continue polling
      }
    }

    throw new IOException("Failed to get token: " + response.body());
  }

  private void refreshWithToken() throws IOException, InterruptedException {
    String tokenUrl = TOKEN_ENDPOINT.replace("%s", tenantId);

    String formData = "client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
        + "&refresh_token=" + URLEncoder.encode(refreshToken, StandardCharsets.UTF_8)
        + "&grant_type=refresh_token"
        + "&scope=" + URLEncoder.encode(SCOPE, StandardCharsets.UTF_8);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(tokenUrl))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString(formData))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      JsonNode json = objectMapper.readTree(response.body());
      accessToken = json.get("access_token").asText();
      if (json.has("refresh_token")) {
        refreshToken = json.get("refresh_token").asText();
      }
      int expiresIn = json.get("expires_in").asInt();
      tokenExpiry = Instant.now().plusSeconds(expiresIn - 300);
    } else {
      // Refresh failed, need to re-authenticate
      refreshToken = null;
      performDeviceCodeFlow();
    }
  }
}
