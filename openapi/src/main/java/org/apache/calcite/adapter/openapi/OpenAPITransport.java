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
package org.apache.calcite.adapter.openapi;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * HTTP transport layer for OpenAPI REST calls.
 * Handles authentication, request building, and response parsing.
 */
public class OpenAPITransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenAPITransport.class);

  private final String baseUrl;
  private final ObjectMapper mapper;
  private final CloseableHttpClient httpClient;
  private final OpenAPIConfig.Authentication authConfig;

  public OpenAPITransport(String baseUrl, ObjectMapper mapper,
      OpenAPIConfig.Authentication authConfig) {
    this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.authConfig = authConfig;
    this.httpClient = HttpClients.createDefault();
  }

  public ObjectMapper mapper() {
    return mapper;
  }

  /**
   * Execute an OpenAPI request and return the JSON response.
   */
  public JsonNode execute(OpenAPIRequest request) throws IOException {
    HttpRequestBase httpRequest = buildHttpRequest(request);

    // Add authentication
    if (authConfig != null) {
      addAuthentication(httpRequest, authConfig);
    }

    LOGGER.debug("OpenAPI Request: {} {}", httpRequest.getMethod(), httpRequest.getURI());

    try (org.apache.http.client.methods.CloseableHttpResponse response = httpClient.execute(httpRequest)) {
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        String errorBody = EntityUtils.toString(response.getEntity());
        throw new RuntimeException(String.format(
            "API request failed with status %d: %s",
            response.getStatusLine().getStatusCode(), errorBody));
      }

      HttpEntity entity = response.getEntity();
      if (entity == null) {
        throw new IOException("Empty response body");
      }

      String responseBody = EntityUtils.toString(entity);
      LOGGER.debug("OpenAPI Response: {}", responseBody);

      return mapper.readTree(responseBody);
    }
  }

  private HttpRequestBase buildHttpRequest(OpenAPIRequest request) throws IOException {
    try {
      String method = request.getVariant().getHttpMethod().toUpperCase();
      String path = buildPath(request);
      URI uri = new URIBuilder(baseUrl + path)
          .addParameters(request.getQueryParams())
          .build();

      HttpRequestBase httpRequest;
      switch (method) {
      case "GET":
        httpRequest = new HttpGet(uri);
        break;
      case "POST":
        HttpPost post = new HttpPost(uri);
        if (request.getBodyParams() != null && !request.getBodyParams().isEmpty()) {
          String json = mapper.writeValueAsString(request.getBodyParams());
          post.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
        }
        httpRequest = post;
        break;
      default:
        throw new UnsupportedOperationException("HTTP method not supported: " + method);
      }

      // Add headers
      request.getHeaders().forEach(httpRequest::addHeader);

      // Add default Accept header if not present
      if (!request.getHeaders().containsKey("Accept")) {
        httpRequest.addHeader("Accept", "application/json");
      }

      return httpRequest;
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URI", e);
    } catch (JsonProcessingException e) {
      throw new IOException("Failed to serialize request body", e);
    }
  }

  private String buildPath(OpenAPIRequest request) {
    String path = request.getVariant().getPath();

    // Replace path parameters: /users/{userId} -> /users/123
    for (Map.Entry<String, Object> entry : request.getPathParams().entrySet()) {
      String placeholder = "{" + entry.getKey() + "}";
      path = path.replace(placeholder, String.valueOf(entry.getValue()));
    }

    return path;
  }

  private void addAuthentication(HttpRequestBase request,
      OpenAPIConfig.Authentication config) {

    switch (config.getType()) {
    case API_KEY:
      addApiKeyAuth(request, config);
      break;
    case BASIC:
      addBasicAuth(request, config);
      break;
    case BEARER:
      addBearerAuth(request, config);
      break;
    case OAUTH2:
      // OAuth2 would require token management - simplified for now
      throw new UnsupportedOperationException("OAuth2 not yet implemented");
    default:
      throw new IllegalArgumentException("Unknown auth type: " + config.getType());
    }
  }

  private void addApiKeyAuth(HttpRequestBase request,
      OpenAPIConfig.Authentication config) {
    String value = resolveValue(config.getTemplate(), config.getCredentials());

    switch (config.getLocation()) {
    case HEADER:
      request.addHeader(config.getParamName(), value);
      break;
    case QUERY:
      // Query parameters are handled in buildHttpRequest
      throw new UnsupportedOperationException("API key in query not yet implemented");
    default:
      throw new IllegalArgumentException("Invalid location for API key: " + config.getLocation());
    }
  }

  private void addBasicAuth(HttpRequestBase request,
      OpenAPIConfig.Authentication config) {
    String username = config.getCredentials().get("username");
    String password = config.getCredentials().get("password");

    if (username == null || password == null) {
      throw new IllegalArgumentException("Basic auth requires username and password");
    }

    String credentials = username + ":" + password;
    String encoded = Base64.getEncoder().encodeToString(credentials.getBytes());
    request.addHeader("Authorization", "Basic " + encoded);
  }

  private void addBearerAuth(HttpRequestBase request,
      OpenAPIConfig.Authentication config) {
    String token = config.getCredentials().get("token");
    if (token == null) {
      throw new IllegalArgumentException("Bearer auth requires token");
    }
    request.addHeader("Authorization", "Bearer " + token);
  }

  private String resolveValue(String template, Map<String, String> credentials) {
    if (template == null) {
      return credentials.values().iterator().next(); // Return first credential
    }

    String result = template;
    for (Map.Entry<String, String> entry : credentials.entrySet()) {
      result = result.replace("${" + entry.getKey() + "}", entry.getValue());
    }
    return result;
  }

  public void close() {
    try {
      httpClient.close();
    } catch (IOException e) {
      LOGGER.warn("Failed to close HTTP client", e);
    }
  }
}
