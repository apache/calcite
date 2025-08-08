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
package org.apache.calcite.adapter.splunk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Helper class for creating Calcite connections with Splunk model files
 * that automatically inject factory specifications.
 *
 * <p>This utility preprocesses model files to inject factory specifications
 * for Splunk schemas, allowing cleaner model files without explicit factory declarations.</p>
 *
 * <p>Usage examples:</p>
 * <pre>{@code
 * // Connect with automatic factory injection
 * Connection conn = SplunkModelHelper.connect("/path/to/federation-model.json");
 *
 * // Or use the preprocessed model directly
 * String preprocessedModel = SplunkModelHelper.preprocessModelFile("/path/to/model.json");
 * Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + preprocessedModel);
 * }</pre>
 */
public final class SplunkModelHelper {
  private SplunkModelHelper() {
    // Utility class
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkModelHelper.class);

  /**
   * Creates a Calcite connection using a model file with automatic factory injection.
   *
   * @param modelPath path to the model file
   * @return a Connection to the Calcite instance
   * @throws SQLException if the connection cannot be established
   * @throws IOException if the model file cannot be read or processed
   */
  public static Connection connect(String modelPath) throws SQLException, IOException {
    String preprocessedModel = ModelPreprocessor.preprocessModel(modelPath);
    String url = "jdbc:calcite:model=inline:" + preprocessedModel;

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Connecting with preprocessed model from: {}", modelPath);
    }

    return DriverManager.getConnection(url);
  }

  /**
   * Preprocesses a model file to inject factory specifications where needed.
   *
   * @param modelPath path to the model file
   * @return the preprocessed model content as JSON string
   * @throws IOException if the model file cannot be read or processed
   */
  public static String preprocessModelFile(String modelPath) throws IOException {
    return ModelPreprocessor.preprocessModel(modelPath);
  }

  /**
   * Creates a simple federation model for multiple Splunk app contexts without
   * requiring explicit factory specifications.
   *
   * @param baseUrl the Splunk server URL
   * @param token the authentication token
   * @param appContexts array of app context names
   * @return a JSON model string ready for use with Calcite
   */
  public static String createFederationModel(String baseUrl, String token, String... appContexts) {
    StringBuilder model = new StringBuilder();
    model.append("{\n");
    model.append("  \"version\": \"1.0\",\n");
    model.append("  \"defaultSchema\": \"").append(appContexts.length > 0 ? appContexts[0] : "splunk").append("\",\n");
    model.append("  \"schemas\": [\n");

    for (int i = 0; i < appContexts.length; i++) {
      String appContext = appContexts[i];
      String schemaName = appContext.toLowerCase(Locale.ROOT)
          .replace("splunk_ta_", "")
          .replace("splunk_sa_", "")
          .replace("-", "")
          .replace("_", "");

      if (i > 0) {
        model.append(",\n");
      }

      model.append("    {\n");
      model.append("      \"name\": \"").append(schemaName).append("\",\n");
      model.append("      \"type\": \"custom\",\n");
      model.append("      \"operand\": {\n");
      model.append("        \"url\": \"").append(baseUrl).append("\",\n");
      model.append("        \"token\": \"").append(token).append("\",\n");
      model.append("        \"app\": \"").append(appContext).append("\",\n");
      model.append("        \"datamodelCacheTtl\": -1\n");
      model.append("      }\n");
      model.append("    }");
    }

    model.append("\n  ]\n");
    model.append("}");

    return model.toString();
  }

  /**
   * Example method showing how to create a typical security federation setup.
   */
  public static Connection createSecurityFederation(String baseUrl, String token)
      throws SQLException, IOException {
    String model =
        createFederationModel(baseUrl, token, "Splunk_SA_CIM",
        "Splunk_TA_cisco-esa",
        "Splunk_TA_paloalto",
        "Splunk_TA_crowdstrike");

    // The factory will be auto-injected by the preprocessor
    String preprocessedModel = ModelPreprocessor.preprocessModel("inline:" + model);
    String url = "jdbc:calcite:model=inline:" + preprocessedModel;

    return DriverManager.getConnection(url);
  }
}
