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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.jdbc.Driver;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver for XBRL adapter.
 *
 * <p>Allows connections using "jdbc:sec:" URL format with automatic defaults:
 * <ul>
 *   <li>lex = ORACLE (unless overridden)</li>
 *   <li>unquotedCasing = TO_LOWER (unless overridden)</li>
 *   <li>Automatic model configuration</li>
 *   <li>Support for connection parameters</li>
 * </ul>
 *
 * <p>Usage examples:
 * <pre>
 * // Minimal connection - just specify companies
 * jdbc:sec:ciks=AAPL
 * jdbc:sec:ciks=AAPL,MSFT,GOOGL
 * jdbc:sec:ciks=MAGNIFICENT7
 *
 * // With additional parameters
 * jdbc:sec:ciks=FAANG&startYear=2020&endYear=2023
 * jdbc:sec:ciks=AAPL&dataDirectory=/Volumes/T9/sec-data
 * </pre>
 */
public class SecDriver extends Driver {
  private static final Logger LOGGER = Logger.getLogger(SecDriver.class.getName());

  static {
    new SecDriver().register();
  }

  @Override protected String getConnectStringPrefix() {
    return "jdbc:sec:";
  }

  @Override public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }

    // Apply XBRL-specific defaults
    Properties secInfo = new Properties();
    if (info != null) {
      secInfo.putAll(info);
    }

    // Set default lex and unquotedCasing if not specified
    if (!secInfo.containsKey("lex")) {
      secInfo.setProperty("lex", "ORACLE");
      LOGGER.fine("Using default lex=ORACLE");
    }
    if (!secInfo.containsKey("unquotedCasing")) {
      secInfo.setProperty("unquotedCasing", "TO_LOWER");
      LOGGER.fine("Using default unquotedCasing=TO_LOWER");
    }

    // Parse the XBRL-specific URL format
    String secUrl = parseSecUrl(url);

    // Check if it's a model file that needs preprocessing
    secUrl = preprocessModelIfNeeded(secUrl);

    // Connect using the transformed URL
    return super.connect(secUrl, secInfo);
  }

  /**
   * Parse XBRL URL format and convert to Calcite URL format.
   *
   * <p>Transforms:
   * <ul>
   *   <li>jdbc:sec:ciks=AAPL → jdbc:calcite:model=inline:sec?ciks=AAPL</li>
   *   <li>jdbc:sec:param1=val1&param2=val2 → jdbc:calcite:model=inline:sec?param1=val1&param2=val2</li>
   * </ul>
   */
  private String parseSecUrl(String url) throws SQLException {
    String prefix = getConnectStringPrefix();
    if (!url.startsWith(prefix)) {
      throw new SQLException("Invalid XBRL URL: " + url);
    }

    String params = url.substring(prefix.length());

    // Build the inline model JSON
    StringBuilder modelJson = new StringBuilder();
    modelJson.append("{");
    modelJson.append("\"version\":\"1.0\",");
    modelJson.append("\"defaultSchema\":\"XBRL\",");
    modelJson.append("\"schemas\":[{");
    modelJson.append("\"name\":\"XBRL\",");
    modelJson.append("\"factory\":\"org.apache.calcite.adapter.sec.SecEmbeddingSchemaFactory\"");

    // If we have parameters, add them to the operand
    if (!params.isEmpty()) {
      modelJson.append(",\"operand\":{");

      // Parse parameters and add to operand
      String[] paramPairs = params.split("&");
      boolean first = true;
      for (String pair : paramPairs) {
        if (!first) {
          modelJson.append(",");
        }
        String[] keyValue = pair.split("=", 2);
        if (keyValue.length == 2) {
          String key = keyValue[0];
          String value = keyValue[1];

          // Handle special cases
          if (key.equals("ciks") && value.contains(",")) {
            // Convert comma-separated CIKs to JSON array
            String[] ciks = value.split(",");
            modelJson.append("\"").append(key).append("\":[");
            for (int i = 0; i < ciks.length; i++) {
              if (i > 0) modelJson.append(",");
              modelJson.append("\"").append(ciks[i].trim()).append("\"");
            }
            modelJson.append("]");
          } else if (key.equals("startYear") || key.equals("endYear") ||
                     key.equals("embeddingDimension")) {
            // Numeric values
            modelJson.append("\"").append(key).append("\":").append(value);
          } else if (key.equals("debug") || key.equals("autoDownload")) {
            // Boolean values
            modelJson.append("\"").append(key).append("\":").append(value);
          } else {
            // String values
            modelJson.append("\"").append(key).append("\":\"").append(value).append("\"");
          }

          // Also set as system property for the schema factory to pick up
          System.setProperty(key, value);
        }
        first = false;
      }

      modelJson.append("}");
    }

    modelJson.append("}]}");

    // Convert to inline model URL
    String calciteUrl = "jdbc:calcite:model=inline:" + modelJson.toString();

    LOGGER.fine("Transformed XBRL URL to: " + calciteUrl);

    return calciteUrl;
  }

  /**
   * Preprocess model file if needed to add XBRL defaults.
   */
  private String preprocessModelIfNeeded(String url) {
    // Check if it's a calcite model file URL (not inline)
    if (url.startsWith("jdbc:calcite:model=") && !url.contains("model=inline:")) {
      String[] parts = url.split("model=", 2);
      if (parts.length == 2) {
        String[] modelParts = parts[1].split("\\?", 2);
        String modelPath = modelParts[0];
        String params = modelParts.length > 1 ? "?" + modelParts[1] : "";

        // Check if it's an XBRL model file that needs preprocessing
        File modelFile = new File(modelPath);
        if (modelFile.exists()) {
          try {
            // Read the file to check if it's an XBRL model
            String content = new String(java.nio.file.Files.readAllBytes(modelFile.toPath()));
            if (content.contains("\"XBRL\"") || content.contains("'XBRL'")) {
              // Check if preprocessing is needed
              if (SecModelPreprocessor.needsPreprocessing(modelFile)) {
                // Preprocess and convert to inline model
                String preprocessed = SecModelPreprocessor.preprocessModelFile(modelFile);
                String inlineUrl = parts[0] + "model=inline:" + preprocessed + params;
                LOGGER.info("Preprocessed XBRL model to add defaults");
                return inlineUrl;
              }
            }
          } catch (IOException e) {
            LOGGER.fine("Could not preprocess model file: " + e.getMessage());
          }
        }
      }
    }

    return url;
  }
}
