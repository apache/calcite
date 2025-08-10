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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HTTP storage provider authentication features.
 * Tests Phase 1, 2, and 3 authentication methods without requiring
 * actual external services.
 */
public class HttpStorageProviderAuthTest {
  
  private HttpServer server;
  private int port;
  private String baseUrl;
  private final AtomicReference<String> capturedAuthHeader = new AtomicReference<>();
  private final AtomicReference<String> capturedApiKeyHeader = new AtomicReference<>();
  private final AtomicReference<Map<String, String>> capturedHeaders = new AtomicReference<>(new HashMap<>());
  
  @TempDir
  Path tempDir;
  
  @BeforeEach
  void setUp() throws IOException {
    // Start a test HTTP server
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;
    
    // Set up test endpoints
    server.createContext("/data", new TestDataHandler());
    server.createContext("/token", new TokenEndpointHandler());
    server.createContext("/proxy", new ProxyHandler());
    
    server.start();
  }
  
  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop(0);
    }
    // Clear captured headers
    capturedAuthHeader.set(null);
    capturedApiKeyHeader.set(null);
    capturedHeaders.set(new HashMap<>());
  }
  
  // ============== Phase 1: Simple Static Auth Tests ==============
  
  @Test
  void testBearerTokenAuth() throws IOException {
    HttpConfig config = new HttpConfig.Builder()
        .bearerToken("test-bearer-token-123")
        .build();
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "GET", null, new HashMap<>(), null, config);
    
    try (InputStream is = provider.openInputStream(baseUrl + "/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("test-data", response);
    }
    
    assertEquals("Bearer test-bearer-token-123", capturedAuthHeader.get());
  }
  
  @Test
  void testApiKeyAuth() throws IOException {
    HttpConfig config = new HttpConfig.Builder()
        .apiKey("test-api-key-456")
        .build();
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "GET", null, new HashMap<>(), null, config);
    
    try (InputStream is = provider.openInputStream(baseUrl + "/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("test-data", response);
    }
    
    assertEquals("test-api-key-456", capturedApiKeyHeader.get());
  }
  
  @Test
  void testBasicAuth() throws IOException {
    HttpConfig config = new HttpConfig.Builder()
        .basicAuth("testuser", "testpass")
        .build();
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "GET", null, new HashMap<>(), null, config);
    
    try (InputStream is = provider.openInputStream(baseUrl + "/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("test-data", response);
    }
    
    // Verify basic auth header
    String expectedAuth = "Basic " + Base64.getEncoder().encodeToString(
        "testuser:testpass".getBytes(StandardCharsets.UTF_8));
    assertEquals(expectedAuth, capturedAuthHeader.get());
  }
  
  // ============== Phase 2: External Token Sources Tests ==============
  
  @Test
  void testTokenFromEnvironment() throws IOException {
    // Set environment variable (simulated via system property for testing)
    String envVar = "TEST_TOKEN_ENV_" + System.currentTimeMillis();
    System.setProperty(envVar, "env-token-789");
    
    try {
      HttpConfig config = new HttpConfig.Builder()
          .tokenEnv(envVar)
          .build();
      
      // Mock the System.getenv by using System.getProperty in test
      HttpStorageProvider provider = new TestableHttpStorageProvider(
          "GET", null, new HashMap<>(), null, config);
      
      try (InputStream is = provider.openInputStream(baseUrl + "/data")) {
        String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        assertEquals("test-data", response);
      }
      
      assertEquals("Bearer env-token-789", capturedAuthHeader.get());
    } finally {
      System.clearProperty(envVar);
    }
  }
  
  @Test
  void testTokenFromFile() throws IOException {
    // Create a temp file with token
    File tokenFile = tempDir.resolve("token.txt").toFile();
    Files.writeString(tokenFile.toPath(), "file-token-abc");
    
    HttpConfig config = new HttpConfig.Builder()
        .tokenFile(tokenFile.getAbsolutePath())
        .build();
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "GET", null, new HashMap<>(), null, config);
    
    try (InputStream is = provider.openInputStream(baseUrl + "/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("test-data", response);
    }
    
    assertEquals("Bearer file-token-abc", capturedAuthHeader.get());
  }
  
  @Test
  void testTokenFromEndpoint() throws IOException {
    HttpConfig config = new HttpConfig.Builder()
        .tokenEndpoint(baseUrl + "/token")
        .build();
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "GET", null, new HashMap<>(), null, config);
    
    try (InputStream is = provider.openInputStream(baseUrl + "/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("test-data", response);
    }
    
    assertEquals("Bearer endpoint-token-xyz", capturedAuthHeader.get());
  }
  
  @Test
  void testTokenWithCustomHeaders() throws IOException {
    File tokenFile = tempDir.resolve("token.txt").toFile();
    Files.writeString(tokenFile.toPath(), "custom-token-123");
    
    Map<String, String> authHeaders = new HashMap<>();
    authHeaders.put("X-Custom-Auth", "Token ${token}");
    authHeaders.put("X-API-Version", "v2");
    
    HttpConfig config = new HttpConfig.Builder()
        .tokenFile(tokenFile.getAbsolutePath())
        .authHeaders(authHeaders)
        .build();
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "GET", null, new HashMap<>(), null, config);
    
    try (InputStream is = provider.openInputStream(baseUrl + "/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("test-data", response);
    }
    
    // HTTP headers are case-insensitive and HttpServer converts to lowercase
    assertEquals("Token custom-token-123", capturedHeaders.get().get("X-custom-auth"));
    assertEquals("v2", capturedHeaders.get().get("X-api-version"));
  }
  
  // ============== Phase 3: Proxy Pattern Tests ==============
  
  @Test
  void testProxyEndpoint() throws IOException {
    HttpConfig config = new HttpConfig.Builder()
        .proxyEndpoint(baseUrl + "/proxy")
        .build();
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "GET", null, new HashMap<>(), null, config);
    
    try (InputStream is = provider.openInputStream("https://api.example.com/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("proxied-data", response);
    }
  }
  
  // ============== Cache Tests ==============
  
  @Test
  void testCacheWithETag() throws IOException {
    HttpConfig config = new HttpConfig.Builder()
        .cacheEnabled(true)
        .cacheTtl(60000) // 1 minute
        .build();
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "GET", null, new HashMap<>(), null, config);
    
    // First request - should cache
    try (InputStream is = provider.openInputStream(baseUrl + "/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("test-data", response);
    }
    
    // Second request - should use cache (server will return 304 if If-None-Match is sent)
    try (InputStream is = provider.openInputStream(baseUrl + "/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("test-data", response);
    }
  }
  
  // ============== Configuration Parsing Tests ==============
  
  @Test
  void testConfigFromMap() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("method", "POST");
    configMap.put("body", "request-body");
    
    Map<String, Object> authConfig = new HashMap<>();
    authConfig.put("bearerToken", "token-from-map");
    authConfig.put("cacheEnabled", true);
    authConfig.put("cacheTtl", 30000L);
    configMap.put("authConfig", authConfig);
    
    HttpConfig config = HttpConfig.fromMap(configMap);
    
    assertEquals("POST", config.getMethod());
    assertEquals("request-body", config.getBody());
    assertEquals("token-from-map", config.getBearerToken());
    assertTrue(config.isCacheEnabled());
    assertEquals(30000L, config.getCacheTtl());
  }
  
  @Test
  void testAuthPriority() throws IOException {
    // When multiple auth methods are configured, only the first should be used
    HttpConfig config = new HttpConfig.Builder()
        .bearerToken("bearer-token")
        .apiKey("api-key")
        .basicAuth("user", "pass")
        .build();
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "GET", null, new HashMap<>(), null, config);
    
    try (InputStream is = provider.openInputStream(baseUrl + "/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("test-data", response);
    }
    
    // Only bearer token should be used (first in priority)
    assertEquals("Bearer bearer-token", capturedAuthHeader.get());
    assertNull(capturedApiKeyHeader.get());
  }
  
  // ============== Test Handlers ==============
  
  private class TestDataHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      // Capture headers
      capturedAuthHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
      capturedApiKeyHeader.set(exchange.getRequestHeaders().getFirst("X-API-Key"));
      
      Map<String, String> headers = new HashMap<>();
      exchange.getRequestHeaders().forEach((key, values) -> {
        if (!values.isEmpty()) {
          headers.put(key, values.get(0));
        }
      });
      capturedHeaders.set(headers);
      
      // Check for conditional request (ETag)
      String ifNoneMatch = exchange.getRequestHeaders().getFirst("If-None-Match");
      if ("\"test-etag\"".equals(ifNoneMatch)) {
        exchange.sendResponseHeaders(304, -1); // Not Modified
        return;
      }
      
      // Send response
      String response = "test-data";
      exchange.getResponseHeaders().add("ETag", "\"test-etag\"");
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes(StandardCharsets.UTF_8));
      }
    }
  }
  
  private class TokenEndpointHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String token = "endpoint-token-xyz";
      exchange.sendResponseHeaders(200, token.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(token.getBytes(StandardCharsets.UTF_8));
      }
    }
  }
  
  private class ProxyHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      // Parse proxy request
      ObjectMapper mapper = new ObjectMapper();
      HttpStorageProvider.ProxyRequest request;
      try (InputStream is = exchange.getRequestBody()) {
        request = mapper.readValue(is, HttpStorageProvider.ProxyRequest.class);
      }
      
      // Create proxy response
      HttpStorageProvider.ProxyResponse response = new HttpStorageProvider.ProxyResponse();
      response.status = 200;
      response.headers = new HashMap<>();
      response.headers.put("Content-Type", "text/plain");
      response.body = "proxied-data";
      
      String jsonResponse = mapper.writeValueAsString(response);
      exchange.sendResponseHeaders(200, jsonResponse.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(jsonResponse.getBytes(StandardCharsets.UTF_8));
      }
    }
  }
  
  // ============== Testable Subclass ==============
  
  /**
   * Testable version that uses System.getProperty instead of System.getenv
   * for environment variable testing.
   */
  private static class TestableHttpStorageProvider extends HttpStorageProvider {
    public TestableHttpStorageProvider(String method, String requestBody,
                                        Map<String, String> headers,
                                        String mimeTypeOverride,
                                        HttpConfig config) {
      super(method, requestBody, headers, mimeTypeOverride, config);
    }
    
    @Override
    protected String getEnvironmentVariable(String name) {
      // In tests, use system properties instead of environment variables
      return System.getProperty(name);
    }
  }
}