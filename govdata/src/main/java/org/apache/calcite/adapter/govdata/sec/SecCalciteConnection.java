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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * XBRL-specific Calcite connection that preprocesses model files.
 *
 * <p>This connection wrapper automatically:
 * <ul>
 *   <li>Preprocesses model files to add XBRL defaults</li>
 *   <li>Sets lex=ORACLE and unquotedCasing=TO_LOWER if not specified</li>
 *   <li>Handles inline model generation from parameters</li>
 * </ul>
 */
public class SecCalciteConnection {
  private static final Logger LOGGER = Logger.getLogger(SecCalciteConnection.class.getName());

  /**
   * Create an XBRL connection with preprocessing.
   *
   * @param url The JDBC URL
   * @param info Connection properties
   * @return A Calcite connection configured for XBRL
   */
  public static CalciteConnection create(String url, Properties info) throws SQLException {
    // Apply XBRL defaults to properties
    Properties secInfo = new Properties();
    if (info != null) {
      secInfo.putAll(info);
    }

    // Set defaults if not specified
    if (!secInfo.containsKey("lex")) {
      secInfo.setProperty("lex", "ORACLE");
    }
    if (!secInfo.containsKey("unquotedCasing")) {
      secInfo.setProperty("unquotedCasing", "TO_LOWER");
    }

    // Check if model needs preprocessing
    String processedUrl = preprocessModelUrl(url);

    // Create connection using standard Calcite driver
    Driver driver = new Driver();
    return (CalciteConnection) driver.connect(processedUrl, secInfo);
  }

  /**
   * Preprocess a model URL to add XBRL defaults.
   *
   * @param url The original JDBC URL
   * @return The URL with preprocessed model
   */
  private static String preprocessModelUrl(String url) throws SQLException {
    // Check if it's a model file URL
    if (url.contains("model=") && !url.contains("model=inline:")) {
      String[] parts = url.split("model=", 2);
      if (parts.length == 2) {
        String[] modelParts = parts[1].split("\\?", 2);
        String modelPath = modelParts[0];
        String params = modelParts.length > 1 ? "?" + modelParts[1] : "";

        // Check if it's an XBRL model file
        File modelFile = new File(modelPath);
        if (modelFile.exists() && modelFile.getName().contains("sec")) {
          try {
            // Check if preprocessing is needed
            if (SecModelPreprocessor.needsPreprocessing(modelFile)) {
              // Preprocess and convert to inline model
              String preprocessed = SecModelPreprocessor.preprocessModelFile(modelFile);
              String inlineUrl = parts[0] + "model=inline:" + preprocessed + params;
              LOGGER.fine("Preprocessed XBRL model file: " + modelPath);
              return inlineUrl;
            }
          } catch (IOException e) {
            LOGGER.warning("Failed to preprocess model file: " + e.getMessage());
          }
        }
      }
    }

    return url;
  }
}
