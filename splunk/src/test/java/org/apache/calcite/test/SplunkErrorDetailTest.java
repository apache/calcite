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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Test to get detailed error messages for failing queries.
 */
@Tag("integration")
class SplunkErrorDetailTest {
  public static final String SPLUNK_URL = "https://kentest.xyz:8089";
  public static final String SPLUNK_USER = "admin";
  public static final String SPLUNK_PASSWORD = "admin123";

  private void loadDriverClass() {
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("driver not found", e);
    }
  }

  @Test void testErrorDetails() throws Exception {
    loadDriverClass();
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.setProperty("user", SPLUNK_USER);
    info.setProperty("password", SPLUNK_PASSWORD);
    info.setProperty("disableSslValidation", "true");
    info.setProperty("app", "Splunk_SA_CIM");
    info.setProperty("datamodelCacheTtl", "0");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      System.out.println("=== Testing Error Details for Failing Queries ===\n");

      // First, get the session key for direct API calls
      SplunkConnectionImpl splunkConn = new SplunkConnectionImpl(SPLUNK_URL, SPLUNK_USER, SPLUNK_PASSWORD, true);
      String sessionKey = getSessionKey(splunkConn);

      // Test the problematic tables
      String[] problemTables = {"internal_server", "internal_audit_logs"};
      String[] searchStrings = {
        "| datamodel internal_server server search",
        "| datamodel internal_audit_logs Audit search"
      };

      for (int i = 0; i < problemTables.length; i++) {
        String table = problemTables[i];
        String search = searchStrings[i];

        System.out.println("Testing table: " + table);
        System.out.println("SPL search: " + search);

        // Try SQL query first
        try (Statement stmt = connection.createStatement()) {
          String sql = "SELECT COUNT(*) FROM \"splunk\".\"" + table + "\"";
          System.out.println("SQL query: " + sql);

          try (ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
              System.out.println("Result: " + rs.getLong(1) + " rows");
            }
          }
        } catch (Exception e) {
          System.out.println("SQL Error: " + e.getMessage());

          // Now try direct REST API call to get detailed error
          System.out.println("\nTrying direct REST API call...");
          String errorDetail = getDetailedError(sessionKey, search);
          if (errorDetail.contains("messages")) {
            System.out.println("Detailed Error Response:\n"
  + errorDetail);
          } else {
            System.out.println("Response length: " + errorDetail.length() + " chars");
            if (errorDetail.length() > 1000) {
              System.out.println("First 1000 chars:\n"
  + errorDetail.substring(0, 1000));
            } else {
              System.out.println("Full response:\n"
  + errorDetail);
            }
          }
        }

        System.out.println("\n"
  + "=".repeat(80) + "\n");
      }

      // Also test a working query for comparison
      System.out.println("Testing working table for comparison: authentication");
      String workingSearch = "| datamodel Authentication Authentication search";
      System.out.println("SPL search: " + workingSearch);
      String workingResponse = getDetailedError(sessionKey, workingSearch);
      System.out.println("Response (should work):\n"
  + workingResponse.substring(0, Math.min(500, workingResponse.length())) + "...");
    }
  }

  private String getSessionKey(SplunkConnectionImpl conn) throws Exception {
    // Use reflection to get the session key
    java.lang.reflect.Field sessionKeyField = SplunkConnectionImpl.class.getDeclaredField("sessionKey");
    sessionKeyField.setAccessible(true);
    return (String) sessionKeyField.get(conn);
  }

  private String getDetailedError(String sessionKey, String search) {
    try {
      // Build the search request
      String endpoint = SPLUNK_URL + "/servicesNS/nobody/Splunk_SA_CIM/search/jobs/export";
      URL url = URI.create(endpoint).toURL();

      HttpURLConnection conn = (HttpURLConnection) url.openConnection();

      // Disable SSL validation
      if (conn instanceof javax.net.ssl.HttpsURLConnection) {
        javax.net.ssl.HttpsURLConnection httpsConn = (javax.net.ssl.HttpsURLConnection) conn;
        httpsConn.setHostnameVerifier((hostname, session) -> true);

        javax.net.ssl.SSLContext sc = javax.net.ssl.SSLContext.getInstance("TLS");
        sc.init(null, new javax.net.ssl.TrustManager[] {
          new javax.net.ssl.X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() { return null; }
            public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) { }
            public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) { }
          }
        }, new java.security.SecureRandom());
        httpsConn.setSSLSocketFactory(sc.getSocketFactory());
      }

      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", "Splunk " + sessionKey);
      conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      conn.setDoOutput(true);

      // Build request body
      String params = "search=" + URLEncoder.encode(search, StandardCharsets.UTF_8.toString()) +
                     "&output_mode=json" +
                     "&earliest_time=-1h" +
                     "&latest_time=now";

      // Send request
      conn.getOutputStream().write(params.getBytes(StandardCharsets.UTF_8));

      // Read response
      int responseCode = conn.getResponseCode();
      System.out.println("Response Code: " + responseCode);

      StringBuilder response = new StringBuilder();
      BufferedReader reader;

      if (responseCode >= 400) {
        reader = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8));
      } else {
        reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
      }

      String line;
      while ((line = reader.readLine()) != null) {
        response.append(line).append("\n");
      }
      reader.close();

      // Try with different app contexts if it fails
      if (responseCode == 400 && endpoint.contains("Splunk_SA_CIM")) {
        System.out.println("\nTrying with 'search' app context instead of 'Splunk_SA_CIM'...");
        endpoint = endpoint.replace("Splunk_SA_CIM", "search");
        return getDetailedError(sessionKey, search);
      }

      return response.toString();

    } catch (Exception e) {
      return "Error making direct API call: " + e.getMessage();
    }
  }
}
