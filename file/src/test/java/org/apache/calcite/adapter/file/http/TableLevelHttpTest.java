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
package org.apache.calcite.adapter.file.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for table-level HTTP configuration with CSV, Parquet, and JSON formats.
 */
@Tag("integration")
public class TableLevelHttpTest {
  
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
    
    setupEndpoints();
    server.start();
  }
  
  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }
  
  private void setupEndpoints() {
    // CSV endpoint (GET)
    server.createContext("/data.csv", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        if ("GET".equals(exchange.getRequestMethod()) || "HEAD".equals(exchange.getRequestMethod())) {
          String csvData = 
              "id,name,value\n" +
              "1,Alpha,100.5\n" +
              "2,Beta,200.7\n" +
              "3,Gamma,300.9\n";
          
          exchange.getResponseHeaders().set("Content-Type", "text/csv");
          
          if ("HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(200, -1);
          } else {
            exchange.sendResponseHeaders(200, csvData.length());
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(csvData.getBytes(StandardCharsets.UTF_8));
            }
          }
        } else {
          exchange.sendResponseHeaders(405, 0);
        }
        exchange.close();
      }
    });
    
    // JSON endpoint (POST with body)
    server.createContext("/api/query", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        if ("POST".equals(exchange.getRequestMethod())) {
          String requestBody = readRequestBody(exchange);
          
          String response;
          if (requestBody.contains("\"query\":\"products\"")) {
            response = "[" +
                "{\"id\":\"P001\",\"name\":\"Laptop\",\"price\":999.99}," +
                "{\"id\":\"P002\",\"name\":\"Mouse\",\"price\":29.99}," +
                "{\"id\":\"P003\",\"name\":\"Keyboard\",\"price\":79.99}" +
                "]";
          } else if (requestBody.contains("\"query\":\"orders\"")) {
            response = "[" +
                "{\"orderId\":101,\"productId\":\"P001\",\"quantity\":2,\"total\":1999.98}," +
                "{\"orderId\":102,\"productId\":\"P002\",\"quantity\":5,\"total\":149.95}," +
                "{\"orderId\":103,\"productId\":\"P003\",\"quantity\":3,\"total\":239.97}" +
                "]";
          } else {
            response = "[]";
          }
          
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length());
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes(StandardCharsets.UTF_8));
          }
        } else if ("HEAD".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(200, -1);
        } else {
          exchange.sendResponseHeaders(405, 0);
        }
        exchange.close();
      }
    });
    
    // CSV endpoint that requires POST
    server.createContext("/export/csv", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        if ("POST".equals(exchange.getRequestMethod())) {
          String requestBody = readRequestBody(exchange);
          
          String csvData;
          if (requestBody.contains("\"type\":\"sales\"")) {
            csvData = 
                "date,region,amount\n" +
                "2024-01-01,North,1500.00\n" +
                "2024-01-01,South,2300.00\n" +
                "2024-01-01,East,1800.00\n" +
                "2024-01-01,West,2100.00\n";
          } else {
            csvData = "error\nNo data\n";
          }
          
          exchange.getResponseHeaders().set("Content-Type", "text/csv");
          exchange.sendResponseHeaders(200, csvData.length());
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(csvData.getBytes(StandardCharsets.UTF_8));
          }
        } else if ("HEAD".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(200, -1);
        } else {
          exchange.sendResponseHeaders(405, 0);
        }
        exchange.close();
      }
    });
    
    // JSON endpoint with wrong Content-Type (needs mime override)
    server.createContext("/broken/api", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        if ("GET".equals(exchange.getRequestMethod())) {
          String jsonData = "[{\"id\":1,\"status\":\"active\"},{\"id\":2,\"status\":\"inactive\"}]";
          
          // Intentionally wrong Content-Type
          exchange.getResponseHeaders().set("Content-Type", "text/plain");
          exchange.sendResponseHeaders(200, jsonData.length());
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(jsonData.getBytes(StandardCharsets.UTF_8));
          }
        } else if ("HEAD".equals(exchange.getRequestMethod())) {
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
  public void testSimpleCsvTable() throws Exception {
    // Test simple CSV fetch via GET
    String modelJson = "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"http_test\"," +
        "  \"schemas\": [" +
        "    {" +
        "      \"name\": \"http_test\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"columnNameCasing\": \"UNCHANGED\"," +
        "        \"tables\": [" +
        "          {" +
        "            \"name\": \"csv_data\"," +
        "            \"url\": \"" + baseUrl + "/data.csv\"," +
        "            \"format\": \"csv\"" +
        "          }" +
        "        ]" +
        "      }" +
        "    }" +
        "  ]" +
        "}";
    
    File modelFile = new File(tempDir.toFile(), "model_csv.json");
    Files.writeString(modelFile.toPath(), modelJson);
    
    Properties info = new Properties();
    info.put("model", modelFile.getAbsolutePath());
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {
      
      ResultSet rs = stmt.executeQuery("SELECT * FROM \"csv_data\" ORDER BY \"id\"");
      
      assertTrue(rs.next());
      assertEquals("1", rs.getString("id"));  // CSV data is read as strings
      assertEquals("Alpha", rs.getString("name"));
      assertEquals("100.5", rs.getString("value"));
      
      assertTrue(rs.next());
      assertEquals("2", rs.getString("id"));
      assertEquals("Beta", rs.getString("name"));
      
      assertTrue(rs.next());
      assertEquals("3", rs.getString("id"));
      assertEquals("Gamma", rs.getString("name"));
      
      assertFalse(rs.next());
      rs.close();
    }
  }
  
  // TODO: JSON table support needs enhancement to work with StorageProviderSource
  // @Test
  public void testPostJsonTable() throws Exception {
    // Test JSON fetch via POST with request body
    String modelJson = "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"http_test\"," +
        "  \"schemas\": [" +
        "    {" +
        "      \"name\": \"http_test\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"columnNameCasing\": \"UNCHANGED\"," +
        "        \"tables\": [" +
        "          {" +
        "            \"name\": \"products\"," +
        "            \"url\": \"" + baseUrl + "/api/query\"," +
        "            \"format\": \"json\"," +
        "            \"method\": \"POST\"," +
        "            \"body\": \"{\\\"query\\\":\\\"products\\\"}\"," +
        "            \"headers\": {" +
        "              \"Content-Type\": \"application/json\"" +
        "            }" +
        "          }," +
        "          {" +
        "            \"name\": \"orders\"," +
        "            \"url\": \"" + baseUrl + "/api/query\"," +
        "            \"format\": \"json\"," +
        "            \"method\": \"POST\"," +
        "            \"body\": \"{\\\"query\\\":\\\"orders\\\"}\"," +
        "            \"headers\": {" +
        "              \"Content-Type\": \"application/json\"" +
        "            }" +
        "          }" +
        "        ]" +
        "      }" +
        "    }" +
        "  ]" +
        "}";
    
    File modelFile = new File(tempDir.toFile(), "model_json.json");
    Files.writeString(modelFile.toPath(), modelJson);
    
    Properties info = new Properties();
    info.put("model", modelFile.getAbsolutePath());
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {
      
      // Query products table
      ResultSet rs = stmt.executeQuery("SELECT * FROM \"products\" WHERE \"price\" < 100 ORDER BY \"price\"");
      
      assertTrue(rs.next());
      assertEquals("P002", rs.getString("id"));
      assertEquals("Mouse", rs.getString("name"));
      assertEquals(29.99, rs.getDouble("price"), 0.01);
      
      assertTrue(rs.next());
      assertEquals("P003", rs.getString("id"));
      assertEquals("Keyboard", rs.getString("name"));
      assertEquals(79.99, rs.getDouble("price"), 0.01);
      
      assertFalse(rs.next());
      rs.close();
      
      // Query orders table
      rs = stmt.executeQuery("SELECT COUNT(*) as cnt, SUM(\"total\") as total FROM \"orders\"");
      
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("cnt"));
      assertEquals(2389.90, rs.getDouble("total"), 0.01);
      rs.close();
    }
  }
  
  @Test
  public void testPostCsvTable() throws Exception {
    // Test CSV fetch via POST
    String modelJson = "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"http_test\"," +
        "  \"schemas\": [" +
        "    {" +
        "      \"name\": \"http_test\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"columnNameCasing\": \"UNCHANGED\"," +
        "        \"tables\": [" +
        "          {" +
        "            \"name\": \"sales\"," +
        "            \"url\": \"" + baseUrl + "/export/csv\"," +
        "            \"format\": \"csv\"," +
        "            \"method\": \"POST\"," +
        "            \"body\": \"{\\\"type\\\":\\\"sales\\\"}\"," +
        "            \"headers\": {" +
        "              \"Content-Type\": \"application/json\"" +
        "            }" +
        "          }" +
        "        ]" +
        "      }" +
        "    }" +
        "  ]" +
        "}";
    
    File modelFile = new File(tempDir.toFile(), "model_post_csv.json");
    Files.writeString(modelFile.toPath(), modelJson);
    
    Properties info = new Properties();
    info.put("model", modelFile.getAbsolutePath());
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {
      
      // Note: Since CSV data is strings, we need to CAST for aggregations
      ResultSet rs = stmt.executeQuery("SELECT \"region\", SUM(CAST(\"amount\" AS DOUBLE)) as total FROM \"sales\" GROUP BY \"region\" ORDER BY \"region\"");
      
      assertTrue(rs.next());
      assertEquals("East", rs.getString("region"));
      assertEquals(1800.00, rs.getDouble("total"), 0.01);
      
      assertTrue(rs.next());
      assertEquals("North", rs.getString("region"));
      assertEquals(1500.00, rs.getDouble("total"), 0.01);
      
      assertTrue(rs.next());
      assertEquals("South", rs.getString("region"));
      assertEquals(2300.00, rs.getDouble("total"), 0.01);
      
      assertTrue(rs.next());
      assertEquals("West", rs.getString("region"));
      assertEquals(2100.00, rs.getDouble("total"), 0.01);
      
      assertFalse(rs.next());
      rs.close();
    }
  }
  
  // TODO: JSON table support needs enhancement to work with StorageProviderSource
  // @Test
  public void testMimeTypeOverride() throws Exception {
    // Test MIME type override for incorrectly served JSON
    String modelJson = "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"http_test\"," +
        "  \"schemas\": [" +
        "    {" +
        "      \"name\": \"http_test\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"columnNameCasing\": \"UNCHANGED\"," +
        "        \"tables\": [" +
        "          {" +
        "            \"name\": \"status\"," +
        "            \"url\": \"" + baseUrl + "/broken/api\"," +
        "            \"format\": \"json\"," +
        "            \"mimeType\": \"application/json\"" +
        "          }" +
        "        ]" +
        "      }" +
        "    }" +
        "  ]" +
        "}";
    
    File modelFile = new File(tempDir.toFile(), "model_mime.json");
    Files.writeString(modelFile.toPath(), modelJson);
    
    Properties info = new Properties();
    info.put("model", modelFile.getAbsolutePath());
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {
      
      ResultSet rs = stmt.executeQuery("SELECT * FROM \"status\" ORDER BY \"id\"");
      
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      assertEquals("active", rs.getString("status"));
      
      assertTrue(rs.next());
      assertEquals(2, rs.getInt("id"));
      assertEquals("inactive", rs.getString("status"));
      
      assertFalse(rs.next());
      rs.close();
    }
  }
  
  @Test  
  public void testMixedTablesInSchema() throws Exception {
    // Test mixing HTTP tables with local files in same schema
    
    // Create a local CSV file
    File localCsv = new File(tempDir.toFile(), "local.csv");
    Files.writeString(localCsv.toPath(), 
        "category,count\n" +
        "A,10\n" +
        "B,20\n" +
        "C,30\n");
    
    String modelJson = "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"mixed\"," +
        "  \"schemas\": [" +
        "    {" +
        "      \"name\": \"mixed\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"directory\": \"" + tempDir.toFile().getAbsolutePath() + "\"," +
        "        \"columnNameCasing\": \"UNCHANGED\"," +
        "        \"tables\": [" +
        "          {" +
        "            \"name\": \"remote_data\"," +
        "            \"url\": \"" + baseUrl + "/data.csv\"," +
        "            \"format\": \"csv\"" +
        "          }" +
        "        ]" +
        "      }" +
        "    }" +
        "  ]" +
        "}";
    
    File modelFile = new File(tempDir.toFile(), "model_mixed.json");
    Files.writeString(modelFile.toPath(), modelJson);
    
    Properties info = new Properties();
    info.put("model", modelFile.getAbsolutePath());
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {
      
      // Query local file (CSV columns are strings) - table name is lowercased by smart casing
      ResultSet rs = stmt.executeQuery("SELECT SUM(CAST(\"count\" AS INTEGER)) as total FROM \"local\"");
      assertTrue(rs.next());
      assertEquals(60, rs.getInt("total"));
      rs.close();
      
      // Query remote HTTP file
      rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"remote_data\"");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("cnt"));
      rs.close();
      
      // Join local and remote (though not meaningful, tests integration)
      rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"local\" l, \"remote_data\" r WHERE CAST(l.\"count\" AS INTEGER) > 15");
      assertTrue(rs.next());
      assertEquals(6, rs.getInt("cnt")); // 2 local rows * 3 remote rows
      rs.close();
    }
  }
}