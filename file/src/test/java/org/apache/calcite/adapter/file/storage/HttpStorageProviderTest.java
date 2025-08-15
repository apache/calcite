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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for HttpStorageProvider with GET and POST support.
 */
@Tag("integration")
public class HttpStorageProviderTest {
  
  private HttpServer server;
  private int port;
  private String baseUrl;
  
  @BeforeEach
  public void setup() throws IOException {
    // Start a local HTTP server for testing
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;
    
    // Setup test endpoints
    setupTestEndpoints();
    
    server.start();
  }
  
  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }
  
  private void setupTestEndpoints() {
    // GET endpoint returning JSON
    server.createContext("/api/users", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if ("GET".equals(method)) {
          String response = "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]";
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length());
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes(StandardCharsets.UTF_8));
          }
        } else if ("HEAD".equals(method)) {
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, -1);
        } else {
          exchange.sendResponseHeaders(405, 0);
        }
        exchange.close();
      }
    });
    
    // POST endpoint for search
    server.createContext("/api/search", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        if ("POST".equals(exchange.getRequestMethod())) {
          // Read request body
          String requestBody = readRequestBody(exchange);
          
          // Simple response based on request
          String response;
          if (requestBody.contains("\"filter\":\"active\"")) {
            response = "{\"results\":[{\"id\":1,\"status\":\"active\"},{\"id\":3,\"status\":\"active\"}]}";
          } else {
            response = "{\"results\":[]}";
          }
          
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length());
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes(StandardCharsets.UTF_8));
          }
        } else {
          exchange.sendResponseHeaders(405, 0);
        }
        exchange.close();
      }
    });
    
    // GraphQL endpoint
    server.createContext("/graphql", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        if ("POST".equals(exchange.getRequestMethod())) {
          String requestBody = readRequestBody(exchange);
          
          String response;
          if (requestBody.contains("users") && requestBody.contains("orders")) {
            // Multi-entity response
            response = "{\"data\":{" +
                "\"users\":[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]," +
                "\"orders\":[{\"id\":101,\"total\":99.99},{\"id\":102,\"total\":149.99}]" +
                "}}";
          } else if (requestBody.contains("users")) {
            // Single entity response
            response = "{\"data\":{\"users\":[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]}}";
          } else {
            response = "{\"data\":{}}";
          }
          
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length());
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes(StandardCharsets.UTF_8));
          }
        } else {
          exchange.sendResponseHeaders(405, 0);
        }
        exchange.close();
      }
    });
    
    // CSV endpoint
    server.createContext("/data.csv", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if ("GET".equals(method)) {
          String response = "id,name,age\n1,Alice,30\n2,Bob,25\n";
          exchange.getResponseHeaders().set("Content-Type", "text/csv");
          exchange.sendResponseHeaders(200, response.length());
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes(StandardCharsets.UTF_8));
          }
        } else if ("HEAD".equals(method)) {
          exchange.getResponseHeaders().set("Content-Type", "text/csv");
          exchange.sendResponseHeaders(200, -1);
        } else {
          exchange.sendResponseHeaders(405, 0);
        }
        exchange.close();
      }
    });
    
    // Wrong content-type endpoint (returns JSON but says text/plain)
    server.createContext("/api/broken", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if ("GET".equals(method)) {
          String response = "[{\"id\":1,\"value\":\"test\"}]";
          exchange.getResponseHeaders().set("Content-Type", "text/plain"); // Wrong!
          exchange.sendResponseHeaders(200, response.length());
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes(StandardCharsets.UTF_8));
          }
        } else if ("HEAD".equals(method)) {
          exchange.getResponseHeaders().set("Content-Type", "text/plain"); // Wrong!
          exchange.sendResponseHeaders(200, -1);
        } else {
          exchange.sendResponseHeaders(405, 0);
        }
        exchange.close();
      }
    });
  }
  
  private String readRequestBody(HttpExchange exchange) throws IOException {
    try (InputStream is = exchange.getRequestBody();
         Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name())) {
      return scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
    }
  }
  
  @Test
  public void testSimpleGetRequest() throws IOException {
    HttpStorageProvider provider = new HttpStorageProvider();
    
    String url = baseUrl + "/api/users";
    assertTrue(provider.exists(url));
    
    StorageProvider.FileMetadata metadata = provider.getMetadata(url);
    assertNotNull(metadata);
    assertEquals("application/json", metadata.getContentType());
    
    try (InputStream is = provider.openInputStream(url)) {
      String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(content.contains("Alice"));
      assertTrue(content.contains("Bob"));
    }
  }
  
  @Test
  public void testPostRequestWithBody() throws IOException {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "POST",
        "{\"filter\":\"active\"}",
        headers,
        null
    );
    
    String url = baseUrl + "/api/search";
    assertTrue(provider.exists(url));
    
    try (InputStream is = provider.openInputStream(url)) {
      String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(content.contains("\"results\""));
      assertTrue(content.contains("\"status\":\"active\""));
    }
  }
  
  @Test
  public void testGraphQLQuery() throws IOException {
    String graphqlQuery = "{\"query\":\"{ users { id name } orders { id total } }\"}";
    
    HttpStorageProvider provider = new HttpStorageProvider(
        "POST",
        graphqlQuery,
        new HashMap<>(),
        null
    );
    
    String url = baseUrl + "/graphql";
    
    try (InputStream is = provider.openInputStream(url)) {
      String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(content.contains("\"users\""));
      assertTrue(content.contains("\"orders\""));
      assertTrue(content.contains("Alice"));
      assertTrue(content.contains("99.99"));
    }
  }
  
  @Test
  public void testMimeTypeOverride() throws IOException {
    // The /api/broken endpoint returns JSON but claims text/plain
    HttpStorageProvider provider = new HttpStorageProvider(
        "GET",
        null,
        new HashMap<>(),
        "application/json" // Override the wrong content-type
    );
    
    String url = baseUrl + "/api/broken";
    StorageProvider.FileMetadata metadata = provider.getMetadata(url);
    
    // Should use our override, not the server's wrong content-type
    assertEquals("application/json", metadata.getContentType());
  }
  
  @Test
  public void testCsvEndpoint() throws IOException {
    HttpStorageProvider provider = new HttpStorageProvider();
    
    String url = baseUrl + "/data.csv";
    StorageProvider.FileMetadata metadata = provider.getMetadata(url);
    assertEquals("text/csv", metadata.getContentType());
    
    try (InputStream is = provider.openInputStream(url)) {
      String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(content.contains("id,name,age"));
      assertTrue(content.contains("Alice"));
    }
  }
  
  @Test
  public void testHttpConfigFromMap() {
    Map<String, Object> config = new HashMap<>();
    config.put("method", "POST");
    config.put("body", "{\"test\":true}");
    config.put("mimeType", "application/json");
    
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Bearer token123");
    config.put("headers", headers);
    
    HttpConfig httpConfig = HttpConfig.fromMap(config);
    assertEquals("POST", httpConfig.getMethod());
    assertEquals("{\"test\":true}", httpConfig.getBody());
    assertEquals("application/json", httpConfig.getMimeType());
    assertEquals("Bearer token123", httpConfig.getHeaders().get("Authorization"));
    
    HttpStorageProvider provider = httpConfig.createStorageProvider();
    assertNotNull(provider);
  }
  
  @Test
  public void testDefaultHttpConfig() {
    HttpConfig config = new HttpConfig();
    assertEquals("GET", config.getMethod());
    assertNull(config.getBody());
    assertNull(config.getMimeType());
    assertTrue(config.getHeaders().isEmpty());
  }
  
  @Test
  public void testNonExistentUrl() throws IOException {
    HttpStorageProvider provider = new HttpStorageProvider();
    String url = baseUrl + "/does-not-exist";
    
    assertFalse(provider.exists(url));
    
    assertThrows(IOException.class, () -> {
      provider.openInputStream(url);
    });
  }
}