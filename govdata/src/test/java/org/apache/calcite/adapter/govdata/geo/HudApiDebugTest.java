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
package org.apache.calcite.adapter.govdata.geo;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Debug test for HUD API authentication and endpoints.
 */
@Tag("integration")
public class HudApiDebugTest {

  private static String hudToken;
  private static String hudUsername;
  private static String hudPassword;

  @BeforeAll
  public static void setup() {
    hudToken = System.getenv("HUD_TOKEN");
    hudUsername = System.getenv("HUD_USERNAME");
    hudPassword = System.getenv("HUD_PASSWORD");
    
    System.out.println("HUD API Debug Test");
    System.out.println("==================");
    System.out.println("Token available: " + (hudToken != null && !hudToken.isEmpty()));
    System.out.println("Username available: " + (hudUsername != null && !hudUsername.isEmpty()));
    System.out.println("Password available: " + (hudPassword != null && !hudPassword.isEmpty()));
    System.out.println();
  }

  @Test
  public void testHudApiEndpoints() throws Exception {
    if (hudToken == null && (hudUsername == null || hudPassword == null)) {
      System.out.println("⚠ Skipping HUD API test - no credentials configured");
      return;
    }

    // Test different API endpoints and authentication methods
    String[] baseUrls = {
        "https://www.huduser.gov/hudapi/public/usps",
        "https://www.huduser.gov/portal/datasets/usps", 
        "https://api.hud.gov/usps"
    };
    
    String[] endpoints = {
        "/zip/county",
        "/zip_county", 
        "/crosswalk/zip_county",
        "/data/zip_county"
    };
    
    String[] quarters = {"2024Q2", "2024Q1", "2023Q4", "3", "2", "1"};
    
    for (String baseUrl : baseUrls) {
      for (String endpoint : endpoints) {
        for (String quarter : quarters) {
          testEndpoint(baseUrl + endpoint, quarter);
        }
      }
    }
  }
  
  private void testEndpoint(String baseUrl, String quarter) {
    try {
      // Try different URL patterns
      String[] urlPatterns = {
          baseUrl + "?quarter=" + quarter,
          baseUrl + "/" + quarter,
          baseUrl + "?q=" + quarter,
          baseUrl + "?period=" + quarter,
          baseUrl
      };
      
      for (String urlStr : urlPatterns) {
        testSingleUrl(urlStr);
      }
      
    } catch (Exception e) {
      // Continue testing other endpoints
    }
  }
  
  private void testSingleUrl(String urlStr) {
    try {
      URI uri = URI.create(urlStr);
      URL url = uri.toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);
      
      // Try token authentication first
      if (hudToken != null && !hudToken.isEmpty()) {
        conn.setRequestProperty("Authorization", "Bearer " + hudToken);
        testConnection(conn, urlStr, "Token");
      }
      
      // Try basic auth if token fails or not available
      if (hudUsername != null && hudPassword != null) {
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        
        String auth = hudUsername + ":" + hudPassword;
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
        testConnection(conn, urlStr, "Basic");
      }
      
      // Try without authentication
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);
      testConnection(conn, urlStr, "None");
      
    } catch (Exception e) {
      // Silent fail for this debug test
    }
  }
  
  private void testConnection(HttpURLConnection conn, String url, String authType) {
    try {
      int responseCode = conn.getResponseCode();
      
      System.out.printf("%-10s | %3d | %s\n", authType, responseCode, url);
      
      if (responseCode == 200) {
        // Read a sample of the response
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
          String line = reader.readLine();
          if (line != null) {
            System.out.println("         SUCCESS: " + line.substring(0, Math.min(100, line.length())));
          }
        }
      } else if (responseCode == 401) {
        System.out.println("         AUTH REQUIRED");
      } else if (responseCode == 403) {
        System.out.println("         FORBIDDEN - check credentials");
      } else if (responseCode == 404) {
        System.out.println("         NOT FOUND");
      } else {
        // Read error response
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
          String line = reader.readLine();
          if (line != null) {
            System.out.println("         ERROR: " + line.substring(0, Math.min(100, line.length())));
          }
        }
      }
      
    } catch (Exception e) {
      System.out.println("         EXCEPTION: " + e.getMessage());
    }
  }

  @Test
  public void testCredentialFormats() {
    if (hudToken != null && !hudToken.isEmpty()) {
      System.out.println("\nToken Analysis:");
      System.out.println("Length: " + hudToken.length());
      System.out.println("Starts with: " + hudToken.substring(0, Math.min(20, hudToken.length())) + "...");
      
      // Check if it's a JWT
      if (hudToken.contains(".")) {
        System.out.println("Format: Appears to be JWT");
        String[] parts = hudToken.split("\\.");
        System.out.println("JWT parts: " + parts.length);
      } else {
        System.out.println("Format: Simple token");
      }
    }
    
    if (hudUsername != null) {
      System.out.println("\nUsername: " + hudUsername);
    }
    
    if (hudPassword != null) {
      System.out.println("Password length: " + hudPassword.length());
    }
  }
}