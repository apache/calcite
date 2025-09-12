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
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.govdata.sec.SecModelPreprocessor;
import org.apache.calcite.jdbc.Driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC driver for Government Data adapter.
 *
 * <p>Allows connections using "jdbc:govdata:" URL format with automatic defaults:
 * <ul>
 *   <li>lex = ORACLE (unless overridden)</li>
 *   <li>unquotedCasing = TO_LOWER (unless overridden)</li>
 *   <li>Automatic model configuration based on data source</li>
 *   <li>Support for connection parameters</li>
 * </ul>
 *
 * <p>Usage examples:
 * <pre>
 * // SEC data - specify data source and companies (tickers auto-convert to CIKs)
 * jdbc:govdata:source=sec&ciks=AAPL                    // Apple ticker → CIK 0000320193
 * jdbc:govdata:source=sec&ciks=AAPL,MSFT,GOOGL        // Multiple tickers
 * jdbc:govdata:source=sec&ciks=MAGNIFICENT7            // Predefined group → 7 CIKs
 * jdbc:govdata:source=sec&ciks=0000320193              // Raw CIK also supported
 *
 * // Mixed identifiers and additional parameters
 * jdbc:govdata:source=sec&ciks=FAANG&startYear=2020&endYear=2023
 * jdbc:govdata:source=sec&ciks=AAPL,0001018724&dataDirectory=/Volumes/T9/gov-data
 *
 * // Future: Other government data sources
 * jdbc:govdata:source=census&dataset=acs&geography=state
 * jdbc:govdata:source=irs&forms=1040&year=2023
 * </pre>
 *
 * <p>Backward compatibility: Also handles legacy "jdbc:sec:" URLs by
 * automatically redirecting to "jdbc:govdata:source=sec".
 */
public class GovDataDriver extends Driver {
  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataDriver.class);

  static {
    new GovDataDriver().register();
  }

  @Override protected String getConnectStringPrefix() {
    return "jdbc:govdata:";
  }

  @Override public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }

    LOGGER.info("Connecting to government data source: {}", url);

    // Apply GovData-specific defaults
    Properties govDataInfo = new Properties();
    if (info != null) {
      govDataInfo.putAll(info);
    }

    // Set default lex and unquotedCasing if not specified
    if (!govDataInfo.containsKey("lex")) {
      govDataInfo.setProperty("lex", "ORACLE");
      LOGGER.debug("Using default lex=ORACLE");
    }
    if (!govDataInfo.containsKey("unquotedCasing")) {
      govDataInfo.setProperty("unquotedCasing", "TO_LOWER");
      LOGGER.debug("Using default unquotedCasing=TO_LOWER");
    }

    try {
      // Extract connection parameters from URL
      String paramString = url.substring(getConnectStringPrefix().length());
      
      // Check if dataSource is specified, default to SEC for backward compatibility
      String dataSource = extractParameter(paramString, "source");
      if (dataSource == null) {
        dataSource = "sec";
        LOGGER.info("No data source specified, defaulting to 'sec'");
      }

      // Create appropriate model based on data source
      String modelPath = createModelFile(paramString, dataSource);
      
      // Set model path in connection properties
      govDataInfo.setProperty("model", modelPath);

      // Create standard Calcite connection with generated model
      String calciteUrl = "jdbc:calcite:";
      return super.connect(calciteUrl, govDataInfo);

    } catch (Exception e) {
      throw new SQLException("Failed to create government data connection: " + e.getMessage(), e);
    }
  }

  /**
   * Extract parameter value from URL parameter string.
   */
  private String extractParameter(String paramString, String paramName) {
    if (paramString == null || paramString.isEmpty()) {
      return null;
    }

    String[] params = paramString.split("&");
    for (String param : params) {
      String[] keyValue = param.split("=", 2);
      if (keyValue.length == 2 && keyValue[0].equals(paramName)) {
        return keyValue[1];
      }
    }
    return null;
  }

  /**
   * Create appropriate model file based on data source and parameters.
   */
  private String createModelFile(String paramString, String dataSource) throws IOException {
    switch (dataSource.toLowerCase()) {
      case "sec":
      case "edgar":
        return createSecModel(paramString);
      case "census":
        throw new UnsupportedOperationException("Census data source not yet implemented");
      case "irs":
        throw new UnsupportedOperationException("IRS data source not yet implemented");
      case "treasury":
        throw new UnsupportedOperationException("Treasury data source not yet implemented");
      default:
        throw new IllegalArgumentException("Unsupported data source: " + dataSource);
    }
  }

  /**
   * Create SEC model file from URL parameters.
   */
  private String createSecModel(String paramString) throws IOException {
    // For now, create a simple temporary model that uses the GovData factory
    // In a full implementation, this would parse URL parameters and create appropriate model
    
    // Extract basic parameters
    // Note: 'ciks' parameter accepts tickers (AAPL), groups (FAANG), or raw CIKs (0000320193)
    // The CikRegistry automatically resolves all identifiers to 10-digit CIK format
    String ciks = extractParameter(paramString, "ciks");
    if (ciks == null) {
      throw new IllegalArgumentException("SEC data source requires 'ciks' parameter (tickers, groups, or CIKs)");
    }
    
    String startYear = extractParameter(paramString, "startYear");
    String endYear = extractParameter(paramString, "endYear");
    String dataDirectory = extractParameter(paramString, "dataDirectory");
    
    // Create temporary model file
    File tempFile = File.createTempFile("govdata-sec-model", ".json");
    tempFile.deleteOnExit();
    
    String modelJson = String.format(
        "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"SEC\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"SEC\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"dataSource\": \"sec\",\n" +
        "      \"ciks\": \"%s\"%s%s%s,\n" +
        "      \"autoDownload\": true,\n" +
        "      \"testMode\": false,\n" +
        "      \"ephemeralCache\": true\n" +
        "    }\n" +
        "  }]\n" +
        "}",
        ciks,
        startYear != null ? ",\n      \"startYear\": " + startYear : "",
        endYear != null ? ",\n      \"endYear\": " + endYear : "",
        dataDirectory != null ? ",\n      \"dataDirectory\": \"" + dataDirectory + "\"" : "");
    
    try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
      writer.write(modelJson);
    }
    
    LOGGER.debug("Created temporary SEC model file: {}", tempFile.getAbsolutePath());
    return tempFile.getAbsolutePath();
  }
}