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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
 * Tests native Splunk data model discovery via REST API showing actual datasets.
 */
@Tag("integration")
public class SplunkDataModelRestApiTest {
  private static final String SPLUNK_URL = "https://kentest.xyz:8089";
  private static final String SPLUNK_USER = "admin";
  private static final String SPLUNK_PASSWORD = "admin123";

  @Test void testDataModelRestApi() throws Exception {
    System.out.println("=== Native Splunk Data Model REST API Discovery ===\n");

    // Create direct REST API call to Splunk
    String endpoint = SPLUNK_URL + "/servicesNS/-/Splunk_SA_CIM/data/models?output_mode=json&count=5";
    URL url = URI.create(endpoint).toURL();

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    // Set up authentication
    String auth = SPLUNK_USER + ":" + SPLUNK_PASSWORD;
    String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
    conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
    conn.setRequestProperty("Accept", "application/json");
    conn.setRequestMethod("GET");

    // Disable SSL validation for test
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

    try {
      int responseCode = conn.getResponseCode();
      System.out.println("Response Code: " + responseCode);

      if (responseCode == 200) {
        // Read response
        StringBuilder response = new StringBuilder();
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            response.append(line);
          }
        }

        // Parse JSON response
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(response.toString());

        if (root.has("entry")) {
          JsonNode entries = root.path("entry");
          System.out.println("Found " + entries.size() + " data models\n");

          for (JsonNode entry : entries) {
            String modelName = entry.path("name").asText();
            JsonNode content = entry.path("content");

            System.out.println("Data Model: " + modelName);
            System.out.println("Author: " + content.path("eai:acl").path("author").asText("Unknown"));
            System.out.println("App: " + content.path("eai:acl").path("app").asText("Unknown"));

            // Get the model definition by making a separate call
            String modelDef = getModelDefinition(modelName);
            if (!modelDef.isEmpty()) {
              JsonNode modelJson = mapper.readTree(modelDef);

              // Show model metadata
              System.out.println("Display Name: " + modelJson.path("displayName").asText(modelName));
              System.out.println("Description: " + modelJson.path("description").asText("No description"));

              // Show datasets (objects) in this model
              JsonNode objects = modelJson.path("objects");
              if (objects.isArray() && objects.size() > 0) {
                System.out.println("Datasets (" + objects.size() + "):");
                for (JsonNode obj : objects) {
                  String objName = getObjectName(obj);
                  String displayName = obj.path("displayName").asText(objName);
                  String parentName = obj.path("parentName").asText("");

                  System.out.println("  Dataset: " + objName);
                  if (!displayName.equals(objName)) {
                    System.out.println("    Display Name: " + displayName);
                  }
                  if (!parentName.isEmpty()) {
                    System.out.println("    Parent: " + parentName);
                  }

                  // Show field count and some sample fields
                  JsonNode fields = obj.path("fields");
                  if (fields.isArray()) {
                    System.out.println("    Fields: " + fields.size() + " total");
                    int fieldCount = 0;
                    for (JsonNode field : fields) {
                      if (fieldCount < 5) { // Show first 5 fields
                        String fieldName = field.path("fieldName").asText();
                        String fieldType = field.path("type").asText("string");
                        boolean required = field.path("required").asBoolean(false);
                        System.out.println("      " + fieldName + " (" + fieldType + ")" +
                                         (required ? " [required]" : ""));
                      }
                      fieldCount++;
                    }
                    if (fieldCount > 5) {
                      System.out.println("      ... and " + (fieldCount - 5) + " more fields");
                    }
                  }

                  // Show constraints if any
                  JsonNode constraints = obj.path("constraints");
                  if (constraints.isArray() && constraints.size() > 0) {
                    System.out.println("    Constraints: " + constraints.size());
                    for (JsonNode constraint : constraints) {
                      String search = constraint.path("search").asText();
                      if (!search.isEmpty()) {
                        System.out.println("      Search: " + search);
                      }
                    }
                  }

                  // Show calculations if any
                  JsonNode calculations = obj.path("calculations");
                  if (calculations.isArray() && calculations.size() > 0) {
                    System.out.println("    Calculations: " + calculations.size());
                    for (JsonNode calc : calculations) {
                      String calcName = calc.path("calculationID").asText();
                      String expression = calc.path("expression").asText();
                      if (!calcName.isEmpty() && !expression.isEmpty()) {
                        System.out.println("      " + calcName + " = " + expression);
                      }
                    }
                  }

                  System.out.println();
                }
              } else {
                System.out.println("No datasets found in this model");
              }
            } else {
              System.out.println("No model definition available");
            }

            System.out.println("=".repeat(80));
            System.out.println();
          }
        } else {
          System.out.println("No entries found in response");
        }

      } else {
        System.out.println("Failed to fetch data models. Response code: " + responseCode);
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            System.out.println("Error: " + line);
          }
        }
      }
    } finally {
      conn.disconnect();
    }
  }

  private String getModelDefinition(String modelName) throws Exception {
    String endpoint = SPLUNK_URL + "/servicesNS/-/Splunk_SA_CIM/data/models/" + modelName + "?output_mode=json";
    URL url = URI.create(endpoint).toURL();

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    // Set up authentication
    String auth = SPLUNK_USER + ":" + SPLUNK_PASSWORD;
    String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
    conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
    conn.setRequestProperty("Accept", "application/json");
    conn.setRequestMethod("GET");

    // Disable SSL validation for test
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

    try {
      int responseCode = conn.getResponseCode();
      if (responseCode == 200) {
        StringBuilder response = new StringBuilder();
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            response.append(line);
          }
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(response.toString());
        if (root.has("entry") && root.path("entry").isArray() && root.path("entry").size() > 0) {
          return root.path("entry").get(0).path("content").path("definition").asText("");
        }
      }
    } finally {
      conn.disconnect();
    }

    return "";
  }

  private String getObjectName(JsonNode obj) {
    if (obj.has("objectName") && !obj.path("objectName").asText().isEmpty()) {
      return obj.path("objectName").asText();
    } else if (obj.has("name") && !obj.path("name").asText().isEmpty()) {
      return obj.path("name").asText();
    }
    return "unnamed";
  }
}
