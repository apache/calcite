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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Test to show raw fields discovered from Splunk web data model.
 */
@Tag("integration")
@EnabledIf("splunkTestEnabled")
public class SplunkWebRawFieldsTest {

  private static boolean splunkTestEnabled() {
    return System.getProperty("CALCITE_TEST_SPLUNK", "false").equals("true") ||
           System.getenv("CALCITE_TEST_SPLUNK") != null;
  }

  @Test public void testWebModelRawFields() throws Exception {
    Properties props = new Properties();

    // Load from local-properties.settings file
    File propsFile = new File("local-properties.settings");
    if (!propsFile.exists()) {
      propsFile = new File("../local-properties.settings");
    }

    if (!propsFile.exists()) {
      System.out.println("Skipping test: Could not find local-properties.settings");
      return;
    }

    try (FileInputStream fis = new FileInputStream(propsFile)) {
      props.load(fis);
    }

    String urlStr = props.getProperty("splunk.url");
    String username = props.getProperty("splunk.username");
    String password = props.getProperty("splunk.password");

    if (urlStr == null || username == null || password == null) {
      System.out.println("Skipping test: Required Splunk connection properties not found");
      return;
    }

    URL url = URI.create(urlStr).toURL();
    boolean disableSsl = "true".equals(props.getProperty("splunk.ssl.insecure"));

    // Create connection to get REST API access
    SplunkConnectionImpl connection = new SplunkConnectionImpl(url, username, password, disableSsl);

    // Fetch the web data model directly
    String endpoint =
        String.format("%s://%s:%d/services/data/models/Web?output_mode=json", url.getProtocol(), url.getHost(), url.getPort());

    URL apiUrl = URI.create(endpoint).toURL();
    HttpURLConnection conn = connection.openConnection(apiUrl);

    try {
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Accept", "application/json");

      int responseCode = conn.getResponseCode();
      if (responseCode == 200) {
        StringBuilder response = new StringBuilder();
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            response.append(line).append("\n");
          }
        }

        // Parse response
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(response.toString());

        if (root.has("entry") && root.path("entry").isArray() && root.path("entry").size() > 0) {
          JsonNode entry = root.path("entry").get(0);
          String rawJson = entry.path("content").path("eai:data").asText();

          if (!rawJson.isEmpty()) {
            JsonNode modelDef = mapper.readTree(rawJson);

            System.out.println("=== Raw Web Data Model Fields from Splunk ===\n");
            System.out.println("Model Name: " + modelDef.path("modelName").asText());
            System.out.println("Display Name: " + modelDef.path("displayName").asText());

            // Find Web dataset/object
            JsonNode objects = modelDef.path("objects");
            if (objects.isArray()) {
              for (JsonNode obj : objects) {
                String objName = obj.path("objectName").asText();
                if (objName.isEmpty()) {
                  objName = obj.path("name").asText();
                }

                System.out.println("\nDataset: " + objName);
                System.out.println("Parent: " + obj.path("parentName").asText("(none)"));

                // Show fields
                JsonNode fields = obj.path("fields");
                if (fields.isArray()) {
                  System.out.println("\nFields defined in data model:");
                  System.out.println("Field Name                Type        Required   Multivalue");
                  System.out.println("---------                ----        --------   ----------");

                  for (JsonNode field : fields) {
                    String fieldName = field.path("fieldName").asText();
                    String fieldType = field.path("type").asText("string");
                    boolean required = field.path("required").asBoolean(false);
                    boolean multivalue = field.path("multivalue").asBoolean(false);

                    System.out.printf("%-25s %-11s %-10s %s%n",
                        fieldName, fieldType, required ? "YES" : "NO", multivalue ? "YES" : "NO");
                  }

                  System.out.println("\nTotal fields in raw model: " + fields.size());

                  // Check for specific fields
                  boolean hasStatus = false;
                  boolean hasBytes = false;
                  boolean hasMethod = false;
                  for (JsonNode field : fields) {
                    String name = field.path("fieldName").asText();
                    if ("status".equals(name)) hasStatus = true;
                    if ("bytes".equals(name)) hasBytes = true;
                    if ("method".equals(name)) hasMethod = true;
                  }

                  System.out.println("\nField presence in raw model:");
                  System.out.println("  status: " + hasStatus);
                  System.out.println("  bytes: " + hasBytes);
                  System.out.println("  method: " + hasMethod);
                }

                // Show calculations
                JsonNode calculations = obj.path("calculations");
                if (calculations.isArray() && calculations.size() > 0) {
                  System.out.println("\nCalculated fields:");
                  for (JsonNode calc : calculations) {
                    String calcId = calc.path("calculationID").asText();
                    String expression = calc.path("expression").asText();
                    System.out.println("  " + calcId + " = " + expression);
                  }
                }
              }
            }
          }
        }
      } else {
        System.out.println("Failed to fetch web data model. Response code: " + responseCode);
      }
    } finally {
      conn.disconnect();
    }
  }
}
