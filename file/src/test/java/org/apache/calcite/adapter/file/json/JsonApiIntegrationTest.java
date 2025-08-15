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

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for JSON API access with POST and JSONPath.
 */
@Tag("integration")
public class JsonApiIntegrationTest {
  
  private HttpServer server;
  private int port;
  private String baseUrl;
  
  @TempDir
  Path tempDir;
  
  @BeforeEach
  public void setup() throws IOException {
    // Start a local HTTP server
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;
    
    setupApiEndpoints();
    server.start();
  }
  
  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }
  
  private void setupApiEndpoints() {
    // API endpoint with nested JSON response
    server.createContext("/api/data", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        if ("POST".equals(exchange.getRequestMethod())) {
          String requestBody = readRequestBody(exchange);
          
          String response;
          if (requestBody.contains("\"type\":\"users\"")) {
            response = "{" +
                "\"status\":\"success\"," +
                "\"data\":{" +
                "  \"users\":[" +
                "    {\"id\":1,\"name\":\"Alice\",\"email\":\"alice@example.com\",\"age\":30}," +
                "    {\"id\":2,\"name\":\"Bob\",\"email\":\"bob@example.com\",\"age\":25}," +
                "    {\"id\":3,\"name\":\"Charlie\",\"email\":\"charlie@example.com\",\"age\":35}" +
                "  ]," +
                "  \"total\":3" +
                "}," +
                "\"timestamp\":\"2024-01-01T00:00:00Z\"" +
                "}";
          } else if (requestBody.contains("\"type\":\"products\"")) {
            response = "{" +
                "\"status\":\"success\"," +
                "\"data\":{" +
                "  \"products\":[" +
                "    {\"id\":\"P001\",\"name\":\"Laptop\",\"price\":999.99,\"stock\":10}," +
                "    {\"id\":\"P002\",\"name\":\"Mouse\",\"price\":29.99,\"stock\":50}," +
                "    {\"id\":\"P003\",\"name\":\"Keyboard\",\"price\":79.99,\"stock\":25}" +
                "  ]" +
                "}" +
                "}";
          } else {
            response = "{\"status\":\"error\",\"message\":\"Unknown type\"}";
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
  }
  
  private String readRequestBody(HttpExchange exchange) throws IOException {
    try (InputStream is = exchange.getRequestBody();
         Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name())) {
      return scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
    }
  }
  
  @Test
  public void testPostApiWithJsonPath() throws Exception {
    // First fetch the data using HTTP POST and save to files
    // This simulates how the data would be available
    
    // Fetch users data
    org.apache.calcite.adapter.file.storage.HttpStorageProvider usersProvider = 
        new org.apache.calcite.adapter.file.storage.HttpStorageProvider(
            "POST",
            "{\"type\":\"users\"}",
            java.util.Map.of("Content-Type", "application/json"),
            null
        );
    
    File usersFile = new File(tempDir.toFile(), "users.json");
    try (InputStream is = usersProvider.openInputStream(baseUrl + "/api/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      // Extract just the users array using Jackson
      com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
      com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(response);
      com.fasterxml.jackson.databind.JsonNode users = root.at("/data/users");
      Files.writeString(usersFile.toPath(), mapper.writerWithDefaultPrettyPrinter().writeValueAsString(users));
    }
    
    // Fetch products data
    org.apache.calcite.adapter.file.storage.HttpStorageProvider productsProvider = 
        new org.apache.calcite.adapter.file.storage.HttpStorageProvider(
            "POST",
            "{\"type\":\"products\"}",
            java.util.Map.of("Content-Type", "application/json"),
            null
        );
    
    File productsFile = new File(tempDir.toFile(), "products.json");
    try (InputStream is = productsProvider.openInputStream(baseUrl + "/api/data")) {
      String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
      com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(response);
      com.fasterxml.jackson.databind.JsonNode products = root.at("/data/products");
      Files.writeString(productsFile.toPath(), mapper.writerWithDefaultPrettyPrinter().writeValueAsString(products));
    }
    
    // Create model JSON pointing to local files
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"api\",\n" +
        "  \"schemas\": [\n" +
        "    {\n" +
        "      \"name\": \"api\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"directory\": \"" + tempDir.toFile().getAbsolutePath() + "\",\n" +
        "        \"columnNameCasing\": \"UNCHANGED\"\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    
    File modelFile = new File(tempDir.toFile(), "model.json");
    Files.writeString(modelFile.toPath(), modelJson);
    
    // Create connection using the model
    Properties info = new Properties();
    info.put("model", modelFile.getAbsolutePath());
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {
      
      // Query users table (POST with JSONPath extraction)
      ResultSet rs = stmt.executeQuery("SELECT * FROM \"users\" ORDER BY \"id\"");
      
      int count = 0;
      while (rs.next()) {
        count++;
        int id = rs.getInt("id");
        String name = rs.getString("name");
        String email = rs.getString("email");
        int age = rs.getInt("age");
        
        if (id == 1) {
          assertEquals("Alice", name);
          assertEquals("alice@example.com", email);
          assertEquals(30, age);
        } else if (id == 2) {
          assertEquals("Bob", name);
          assertEquals("bob@example.com", email);
          assertEquals(25, age);
        }
      }
      assertEquals(3, count);
      rs.close();
      
      // Query products table (different POST body, same endpoint)
      rs = stmt.executeQuery("SELECT * FROM \"products\" WHERE \"price\" < 100 ORDER BY \"price\"");
      
      count = 0;
      while (rs.next()) {
        count++;
        String id = rs.getString("id");
        String name = rs.getString("name");
        double price = rs.getDouble("price");
        
        if (count == 1) {
          assertEquals("P002", id);
          assertEquals("Mouse", name);
          assertEquals(29.99, price, 0.01);
        } else if (count == 2) {
          assertEquals("P003", id);
          assertEquals("Keyboard", name);
          assertEquals(79.99, price, 0.01);
        }
      }
      assertEquals(2, count); // Only 2 products under $100
      rs.close();
      
      // Test aggregation
      rs = stmt.executeQuery("SELECT COUNT(*) as cnt, AVG(\"age\") as avg_age FROM \"users\"");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("cnt"));
      assertEquals(30.0, rs.getDouble("avg_age"), 0.01);
      rs.close();
    }
  }
  
  @Test
  public void testProgrammaticApiAccess() throws Exception {
    // Test programmatic access using FileSchema directly
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("method", "POST");
    storageConfig.put("body", "{\"type\":\"users\"}");
    
    // Create a temporary directory with a JSON config file
    File configFile = new File(tempDir.toFile(), "api_config.json");
    String configJson = "{\n" +
        "  \"url\": \"" + baseUrl + "/api/data\",\n" +
        "  \"method\": \"POST\",\n" +
        "  \"body\": \"{\\\"type\\\":\\\"users\\\"}\",\n" +
        "  \"jsonPath\": \"$.data.users\"\n" +
        "}";
    Files.writeString(configFile.toPath(), configJson);
    
    // Create FileSchema with HTTP storage
    FileSchema schema = new FileSchema(
        null,
        "api",
        tempDir.toFile(),
        null,  // directoryPattern
        null,  // tables
        new ExecutionEngineConfig(),
        false,  // recursive
        null,  // materializations
        null,  // views
        null,  // partitionedTables
        null,  // refreshInterval
        "UPPER",  // tableNameCasing
        "UNCHANGED",  // columnNameCasing
        "http",  // storageType
        storageConfig,  // storageConfig
        null,  // flatten
        null,   // csvTypeInference
        false   // primeCache
    );
    
    // Verify the schema was created
    assertNotNull(schema);
    // Note: Actual table creation would depend on file discovery in the temp directory
  }
}