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
package org.apache.calcite.test;

import org.apache.calcite.adapter.splunk.search.SplunkConnectionImpl;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Debug test to directly examine Splunk REST API responses.
 * Run with: -Dcalcite.test.splunk=true
 */
@Tag("integration")
class SplunkRestApiDebugTest {

  private static final String BASE_URL = "https://kentest.xyz:8089";
  private static final String USERNAME = "admin";
  private static final String PASSWORD = "admin123";


  @Test void testDirectRestApiCall() throws Exception {
    System.out.println("\n=== Direct REST API Debug Test ===");

    // Test various endpoints
    String[] endpoints = {
        "/services/data/models",
        "/servicesNS/-/-/data/models",
        "/servicesNS/admin/search/data/models",
        "/servicesNS/admin/Splunk_SA_CIM/data/models",
        "/services/apps/local"
    };

    for (String endpoint : endpoints) {
      System.out.println("\n--- Testing endpoint: " + endpoint + " ---");
      testEndpoint(endpoint);
    }
  }

  @SuppressWarnings("deprecation")
  private void testEndpoint(String endpoint) {
    try {
      URL url = new URL(BASE_URL + endpoint + "?output_mode=json&count=-1");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();

      // Disable SSL validation for test server
      if (conn instanceof javax.net.ssl.HttpsURLConnection) {
        javax.net.ssl.HttpsURLConnection httpsConn = (javax.net.ssl.HttpsURLConnection) conn;
        httpsConn.setSSLSocketFactory(createTrustAllSocketFactory());
        httpsConn.setHostnameVerifier((hostname, session) -> true);
      }

      // Set up authentication
      String auth = USERNAME + ":" + PASSWORD;
      String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
      conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
      conn.setRequestProperty("Accept", "application/json");

      int responseCode = conn.getResponseCode();
      System.out.println("Response Code: " + responseCode);

      if (responseCode == 200) {
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
          StringBuilder response = new StringBuilder();
          String line;
          while ((line = reader.readLine()) != null) {
            response.append(line).append("\n");
          }

          // Parse JSON response
          ObjectMapper mapper = new ObjectMapper();
          Map<String, Object> jsonResponse =
              mapper.readValue(response.toString(), HashMap.class);

          // Check for entry array
          if (jsonResponse.containsKey("entry")) {
            Object entry = jsonResponse.get("entry");
            if (entry instanceof java.util.List) {
              java.util.List<?> entries = (java.util.List<?>) entry;
              System.out.println("Found " + entries.size() + " entries");

              // Print first few entries
              int count = 0;
              for (Object e : entries) {
                if (count++ >= 5) break;
                if (e instanceof Map) {
                  Map<?, ?> entryMap = (Map<?, ?>) e;
                  System.out.println("  Entry: " + entryMap.get("name"));
                }
              }

              if (entries.size() > 5) {
                System.out.println("  ... and " + (entries.size() - 5) + " more");
              }
            }
          } else {
            System.out.println("Response structure: " + jsonResponse.keySet());
          }
        }
      } else {
        System.out.println("Error response from server");
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
          String line;
          while ((line = reader.readLine()) != null) {
            System.out.println("  " + line);
          }
        }
      }

    } catch (Exception e) {
      System.out.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private javax.net.ssl.SSLSocketFactory createTrustAllSocketFactory() {
    try {
      javax.net.ssl.TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[] {
          new javax.net.ssl.X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
              return null;
            }
            public void checkClientTrusted(
                java.security.cert.X509Certificate[] certs, String authType) {
            }
            public void checkServerTrusted(
                java.security.cert.X509Certificate[] certs, String authType) {
            }
          }
      };

      javax.net.ssl.SSLContext sc = javax.net.ssl.SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      return sc.getSocketFactory();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test @SuppressWarnings("deprecation")
  void testUsingConnectionImpl() throws Exception {
    System.out.println("\n=== Testing Using SplunkConnectionImpl ===");

    SplunkConnectionImpl connection =
        new SplunkConnectionImpl(BASE_URL, USERNAME, PASSWORD, true, null);

    // Test direct URL fetch
    URL modelsUrl = new URL(BASE_URL + "/services/data/models?output_mode=json&count=-1");
    System.out.println("Fetching: " + modelsUrl);

    try {
      HttpURLConnection conn = connection.openConnection(modelsUrl);
      int responseCode = conn.getResponseCode();
      System.out.println("Response Code: " + responseCode);

      if (responseCode == 200) {
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
          StringBuilder response = new StringBuilder();
          String line;
          int lineCount = 0;
          while ((line = reader.readLine()) != null && lineCount++ < 20) {
            response.append(line).append("\n");
          }
          System.out.println("Response preview:");
          System.out.println(response.toString());
          if (lineCount >= 20) {
            System.out.println("... (truncated)");
          }
        }
      }
    } catch (Exception e) {
      System.out.println("Error using SplunkConnectionImpl: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
