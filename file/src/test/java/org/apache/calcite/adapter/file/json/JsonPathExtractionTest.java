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
package org.apache.calcite.adapter.file.json;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test JSONPath extraction from HTTP API responses.
 * This test verifies that JsonMultiTableFactory is properly wired up.
 */
public class JsonPathExtractionTest {
  
  private HttpServer server;
  private int port;
  private String baseUrl;
  
  @TempDir
  Path tempDir;
  
  @BeforeEach
  public void setup() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;
    
    setupApiEndpoint();
    server.start();
  }
  
  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }
  
  private void setupApiEndpoint() {
    server.createContext("/api/data", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        // Return nested JSON with multiple arrays
        String response = "{" +
            "\"status\":\"success\"," +
            "\"timestamp\":\"2024-01-01T00:00:00Z\"," +
            "\"data\":{" +
            "  \"users\":[" +
            "    {\"id\":1,\"name\":\"Alice\",\"email\":\"alice@example.com\"}," +
            "    {\"id\":2,\"name\":\"Bob\",\"email\":\"bob@example.com\"}," +
            "    {\"id\":3,\"name\":\"Charlie\",\"email\":\"charlie@example.com\"}" +
            "  ]," +
            "  \"products\":[" +
            "    {\"id\":\"P001\",\"name\":\"Laptop\",\"price\":999.99}," +
            "    {\"id\":\"P002\",\"name\":\"Mouse\",\"price\":29.99}," +
            "    {\"id\":\"P003\",\"name\":\"Keyboard\",\"price\":79.99}" +
            "  ]," +
            "  \"orders\":[" +
            "    {\"orderId\":101,\"userId\":1,\"productId\":\"P001\",\"quantity\":1}," +
            "    {\"orderId\":102,\"userId\":2,\"productId\":\"P002\",\"quantity\":2}," +
            "    {\"orderId\":103,\"userId\":1,\"productId\":\"P003\",\"quantity\":1}" +
            "  ]" +
            "}," +
            "\"metadata\":{" +
            "  \"version\":\"1.0\"," +
            "  \"count\":9" +
            "}" +
            "}";
        
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length());
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(response.getBytes(StandardCharsets.UTF_8));
        }
      }
    });
  }
  
  @Test
  public void testJsonPathExtraction() throws Exception {
    // Create model with JSONPath extraction
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"api\",\n" +
        "  \"schemas\": [\n" +
        "    {\n" +
        "      \"name\": \"api\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"tables\": [\n" +
        "          {\n" +
        "            \"name\": \"api_response\",\n" +
        "            \"url\": \"" + baseUrl + "/api/data\",\n" +
        "            \"format\": \"json\",\n" +
        "            \"jsonSearchPaths\": [\"$.data.users\", \"$.data.products\", \"$.data.orders\"]\n" +
        "          }\n" +
        "        ]\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    
    Path modelFile = tempDir.resolve("model.json");
    Files.writeString(modelFile, modelJson);
    
    Properties info = new Properties();
    info.put("model", modelFile.toString());
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {
      
      // Query users table (extracted via JSONPath $.data.users)
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"users\" ORDER BY \"id\"")) {
        int count = 0;
        while (rs.next()) {
          count++;
          int id = rs.getInt("id");
          String name = rs.getString("name");
          String email = rs.getString("email");
          
          if (id == 1) {
            assertEquals("Alice", name);
            assertEquals("alice@example.com", email);
          } else if (id == 2) {
            assertEquals("Bob", name);
            assertEquals("bob@example.com", email);
          } else if (id == 3) {
            assertEquals("Charlie", name);
            assertEquals("charlie@example.com", email);
          }
        }
        assertEquals(3, count, "Should have 3 users");
      }
      
      // Query products table (extracted via JSONPath $.data.products)
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"products\" ORDER BY \"id\"")) {
        int count = 0;
        while (rs.next()) {
          count++;
          String id = rs.getString("id");
          String name = rs.getString("name");
          double price = rs.getDouble("price");
          
          if ("P001".equals(id)) {
            assertEquals("Laptop", name);
            assertEquals(999.99, price, 0.01);
          } else if ("P002".equals(id)) {
            assertEquals("Mouse", name);
            assertEquals(29.99, price, 0.01);
          }
        }
        assertEquals(3, count, "Should have 3 products");
      }
      
      // Query orders table (extracted via JSONPath $.data.orders)
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"orders\" WHERE \"quantity\" > 1")) {
        assertTrue(rs.next());
        assertEquals(102, rs.getInt("orderId"));
        assertEquals(2, rs.getInt("userId"));
        assertEquals("P002", rs.getString("productId"));
        assertEquals(2, rs.getInt("quantity"));
        assertFalse(rs.next(), "Should have only one order with quantity > 1");
      }
      
      // Verify the source table itself is NOT available
      try {
        stmt.executeQuery("SELECT * FROM \"api_response\"");
        fail("Source table should not exist when JSONPath extraction is used");
      } catch (Exception e) {
        // Expected - the source table should not exist
        assertTrue(e.getMessage().contains("api_response") || 
                   e.getMessage().contains("not found"),
                   "Error should indicate table not found");
      }
    }
  }
  
  @Test
  public void testJsonPathWithPostRequest() throws Exception {
    // Test with POST request and headers
    server.createContext("/graphql", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(405, 0);
          return;
        }
        
        String response = "{" +
            "\"data\":{" +
            "  \"company\":{" +
            "    \"employees\":[" +
            "      {\"id\":1,\"name\":\"John\",\"department\":\"Engineering\"}," +
            "      {\"id\":2,\"name\":\"Jane\",\"department\":\"Sales\"}" +
            "    ]," +
            "    \"departments\":[" +
            "      {\"id\":\"ENG\",\"name\":\"Engineering\",\"budget\":1000000}," +
            "      {\"id\":\"SALES\",\"name\":\"Sales\",\"budget\":500000}" +
            "    ]" +
            "  }" +
            "}" +
            "}";
        
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length());
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(response.getBytes(StandardCharsets.UTF_8));
        }
      }
    });
    
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"graphql\",\n" +
        "  \"schemas\": [\n" +
        "    {\n" +
        "      \"name\": \"graphql\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"tables\": [\n" +
        "          {\n" +
        "            \"name\": \"company\",\n" +
        "            \"url\": \"" + baseUrl + "/graphql\",\n" +
        "            \"format\": \"json\",\n" +
        "            \"method\": \"POST\",\n" +
        "            \"body\": \"{\\\"query\\\":\\\"{ company { employees { id name department } departments { id name budget } } }\\\"}\",\n" +
        "            \"headers\": {\"Content-Type\": \"application/json\"},\n" +
        "            \"jsonSearchPaths\": [\"$.data.company.employees\", \"$.data.company.departments\"]\n" +
        "          }\n" +
        "        ]\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    
    Path modelFile = tempDir.resolve("graphql_model.json");
    Files.writeString(modelFile, modelJson);
    
    Properties info = new Properties();
    info.put("model", modelFile.toString());
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {
      
      // Query employees table
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"employees\"")) {
        int count = 0;
        while (rs.next()) {
          count++;
        }
        assertEquals(2, count, "Should have 2 employees");
      }
      
      // Query departments table
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"departments\" WHERE \"budget\" > 600000")) {
        assertTrue(rs.next());
        assertEquals("ENG", rs.getString("id"));
        assertEquals(1000000, rs.getInt("budget"));
        assertFalse(rs.next());
      }
    }
  }
}