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

import org.apache.calcite.adapter.splunk.search.SplunkConnection;
import org.apache.calcite.adapter.splunk.search.SplunkConnectionImpl;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.checkerframework.checker.nullness.qual.Nullable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Enhanced JDBC driver for Splunk with CIM model, custom table, and token authentication support.
 *
 * <p>It accepts connect strings that start with "jdbc:splunk:" and supports
 * additional parameters for CIM models, custom tables, and enhanced features.</p>
 *
 * <p>Enhanced connection string examples:</p>
 * <ul>
 * <li>Token Auth: jdbc:splunk:url=<a href="https://localhost:8089;token=eyJhbGciOiJIUzI1NiI...">...</a></li>
 * <li>Basic Auth: jdbc:splunk:url=<a href="https://localhost:8089;user=admin;password=changeme">...</a></li>
 * <li>Single CIM model: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;model=Authentication</li>
 * <li>Multiple CIM models: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;models=Authentication,Web,Network_Traffic</li>
 * <li>Custom tables: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;customTables=[{"name":"alerts","columns":[...]}]</li>
 * </ul>
 *
 * <p>Note: For custom tables, the JSON must be URL-encoded. In Java 8:</p>
 * <pre>{@code
 * String customTablesJson = "[{\"name\":\"alerts\",\"columns\":[...]}]";
 * String encodedJson = URLEncoder.encode(customTablesJson, "UTF-8");
 * String url = "jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;customTables=" + encodedJson;
 * }</pre>
 */
public class SplunkDriver extends org.apache.calcite.jdbc.Driver {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkDriver.class);

  protected SplunkDriver() {
    super();
  }

  static {
    new SplunkDriver().register();
  }

  @Override protected String getConnectStringPrefix() {
    return "jdbc:splunk:";
  }

  @Override protected DriverVersion createDriverVersion() {
    return new SplunkDriverVersion();
  }

  @Override public Connection connect(String url, Properties info)
      throws SQLException {
    Connection connection = super.connect(url, info);
    CalciteConnection calciteConnection = (CalciteConnection) connection;

    try {
      // Extract and validate connection properties
      ConnectionProperties props = extractConnectionProperties(info);
      validateRequiredProperties(props);

      // Create Splunk connection with token or username/password authentication
      SplunkConnection splunkConnection = createSplunkConnection(props);

      // Create enhanced schema with new features
      createEnhancedSchema(calciteConnection, props, splunkConnection);

      if (props.debug) {
        LOGGER.info("Successfully created enhanced Splunk connection with features: " +
                "models={}, customTables={}, defaultTimeRange={}",
            props.models, props.customTables.size(),
            props.defaultTimeRange);
      }

    } catch (Exception e) {
      LOGGER.error("Failed to create enhanced Splunk connection", e);
      throw new SQLException("Failed to create enhanced Splunk connection: " + e.getMessage(), e);
    }

    return connection;
  }

  /**
   * Extracts and processes connection properties from the JDBC properties.
   * @param info the JDBC connection properties
   * @return non-null ConnectionProperties object
   */
  private ConnectionProperties extractConnectionProperties(Properties info) throws SQLException {
    ConnectionProperties props = new ConnectionProperties();

    // Basic required properties (can be null from Properties.getProperty())
    props.splunkUrl = info.getProperty("url");
    props.token = info.getProperty("token");
    props.user = info.getProperty("user");
    props.password = info.getProperty("password");

    // Enhanced feature properties
    props.model = info.getProperty("model");
    props.models = parseModels(info.getProperty("models"));
    props.defaultTimeRange = info.getProperty("defaultTimeRange");
    props.debug = Boolean.parseBoolean(info.getProperty("debug", "false"));
    props.logLevel = info.getProperty("logLevel");

    // Parse custom tables JSON if provided
    props.customTables = parseCustomTables(info.getProperty("customTables"));

    return props;
  }

  /**
   * Validates that required connection properties are present.
   */
  private void validateRequiredProperties(ConnectionProperties props) throws SQLException {
    if (props.splunkUrl == null) {
      throw new SQLException("Must specify 'url' property");
    }

    boolean hasToken = props.token != null && !props.token.trim().isEmpty();
    boolean hasCredentials = props.user != null && props.password != null &&
        !props.user.trim().isEmpty() && !props.password.trim().isEmpty();

    if (!hasToken && !hasCredentials) {
      throw new SQLException("Must specify either 'token' or both 'user' and 'password' properties");
    }
  }

  /**
   * Creates a SplunkConnection based on the provided properties.
   * Properties have been validated as non-null at this point.
   * @return non-null SplunkConnection instance
   */
  private SplunkConnection createSplunkConnection(ConnectionProperties props) throws SQLException {
    String splunkUrl = props.splunkUrl;

    if ("mock".equals(splunkUrl)) {
      return new MockSplunkConnection();
    } else {
      try {
        // Try token authentication first
        if (props.token != null && !props.token.trim().isEmpty()) {
          return new SplunkConnectionImpl(splunkUrl, props.token);
        }

        // Fall back to username/password
        return new SplunkConnectionImpl(splunkUrl, props.user, props.password);

      } catch (MalformedURLException e) {
        throw new SQLException("Invalid Splunk URL: " + splunkUrl, e);
      }
    }
  }

  /**
   * Creates the enhanced Splunk schema with CIM models and custom tables.
   */
  private void createEnhancedSchema(CalciteConnection calciteConnection,
      ConnectionProperties props,
      SplunkConnection splunkConnection) throws SQLException {
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // Build operand map for SplunkSchemaFactory
    Map<String, Object> operand = new HashMap<>();

    // These have been validated as non-null by this point
    String splunkUrl = props.splunkUrl;
    String user = props.user;
    String password = props.password;

    if (splunkUrl == null) {
      throw new SQLException("Connection properties not properly validated");
    }

    operand.put("url", splunkUrl);

    // Add authentication - token takes precedence
    if (props.token != null && !props.token.trim().isEmpty()) {
      operand.put("token", props.token);
    } else if (user != null && password != null) {
      operand.put("username", user);
      operand.put("password", password);
    }

    operand.put("splunkConnection", splunkConnection);

    // Add CIM model support
    if (props.model != null && !props.model.trim().isEmpty()) {
      operand.put("model", props.model);
      if (props.debug) {
        LOGGER.debug("Added single CIM model: {}", props.model);
      }
    }

    if (!props.models.isEmpty()) {
      operand.put("models", props.models);
      if (props.debug) {
        LOGGER.debug("Added multiple CIM models: {}", props.models);
      }
    }

    // Add optional parameters
    if (props.defaultTimeRange != null && !props.defaultTimeRange.trim().isEmpty()) {
      operand.put("defaultTimeRange", props.defaultTimeRange);
    }

    if (!props.customTables.isEmpty()) {
      operand.put("customTables", props.customTables);
      if (props.debug) {
        LOGGER.debug("Added {} custom tables", props.customTables.size());
      }
    }

    // Add debug settings
    operand.put("debug", props.debug);
    if (props.logLevel != null && !props.logLevel.trim().isEmpty()) {
      operand.put("logLevel", props.logLevel);
    }

    // Create the enhanced schema using SplunkSchemaFactory
    try {
      SplunkSchemaFactory factory = new SplunkSchemaFactory();
      Schema splunkSchema = factory.create(rootSchema, "splunk", operand);
      rootSchema.add("splunk", splunkSchema);
    } catch (Exception e) {
      throw new SQLException("Failed to create Splunk schema: " + e.getMessage(), e);
    }
  }

  /**
   * Parses comma-separated model names into a list.
   * @param modelsString the comma-separated string of model names (can be null or empty)
   * @return non-null list of model names (empty if no valid models found)
   */
  private List<String> parseModels(@Nullable String modelsString) {
    List<String> models = new ArrayList<>();

    if (modelsString != null && !modelsString.trim().isEmpty()) {
      for (String model : modelsString.split(",")) {
        String trimmed = model.trim();
        if (!trimmed.isEmpty()) {
          models.add(trimmed);
        }
      }
    }

    return models;
  }

  /**
   * Parses JSON string containing custom table definitions.
   * @param customTablesJson the JSON string containing table definitions (can be null or empty)
   * @return non-null list of custom table definitions (empty if no valid tables found)
   */
  private List<Map<String, Object>> parseCustomTables(@Nullable String customTablesJson) throws SQLException {
    List<Map<String, Object>> customTables = new ArrayList<>();

    if (customTablesJson != null && !customTablesJson.trim().isEmpty()) {
      try {
        // URL decode the JSON string (Java 8 compatible)
        String decodedJson = URLDecoder.decode(customTablesJson, "UTF-8");

        // Parse JSON using Jackson
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<List<Map<String, Object>>> typeRef =
            new TypeReference<List<Map<String, Object>>>() {};

        customTables = mapper.readValue(decodedJson, typeRef);
        if (customTables == null) {
          customTables = new ArrayList<>();
        }

        // Validate custom table structure
        validateCustomTables(customTables);

      } catch (UnsupportedEncodingException e) {
        throw new SQLException("UTF-8 encoding not supported: " + e.getMessage(), e);
      } catch (Exception e) {
        throw new SQLException("Invalid customTables JSON: " + e.getMessage(), e);
      }
    }

    return customTables;
  }

  /**
   * Validates the structure of custom table definitions.
   */
  private void validateCustomTables(List<Map<String, Object>> customTables) throws SQLException {
    for (int i = 0; i < customTables.size(); i++) {
      Map<String, Object> table = customTables.get(i);

      // Validate table name
      Object name = table.get("name");
      if (!(name instanceof String) || ((String) name).trim().isEmpty()) {
        throw new SQLException("Custom table at index " + i + " must have a non-empty 'name' property");
      }

      // Validate columns
      Object columns = table.get("columns");
      if (!(columns instanceof List)) {
        throw new SQLException("Custom table '" + name + "' must have a 'columns' array property");
      }

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> columnList = (List<Map<String, Object>>) columns;
      if (columnList.isEmpty()) {
        throw new SQLException("Custom table '" + name + "' must have at least one column");
      }

      // Validate each column
      for (int j = 0; j < columnList.size(); j++) {
        Map<String, Object> column = columnList.get(j);

        Object columnName = column.get("name");
        if (!(columnName instanceof String) || ((String) columnName).trim().isEmpty()) {
          throw new SQLException("Column at index " + j + " in table '" + name + "' must have a non-empty 'name' property");
        }

        Object columnType = column.get("type");
        if (!(columnType instanceof String) || ((String) columnType).trim().isEmpty()) {
          throw new SQLException("Column '" + columnName + "' in table '" + name + "' must have a non-empty 'type' property");
        }
      }
    }
  }

  /**
   * Internal class to hold parsed connection properties.
   */
  private static class ConnectionProperties {
    @Nullable String splunkUrl;
    @Nullable String token;
    @Nullable String user;
    @Nullable String password;
    @Nullable String model;
    List<String> models = new ArrayList<>();
    @Nullable String defaultTimeRange;
    boolean debug = false;
    @Nullable String logLevel;
    List<Map<String, Object>> customTables = new ArrayList<>();
  }

  /**
   * Mock implementation for testing purposes.
   */
  private static class MockSplunkConnection implements SplunkConnection {
    @Override
    public void getSearchResults(String search, Map<String, String> otherArgs,
        List<String> fieldList,
        org.apache.calcite.adapter.splunk.search.SearchResultListener srl) {
      // Mock implementation for testing - parameters intentionally unused
      LOGGER.debug("Mock connection: getSearchResults called");
    }

    @Override
    public org.apache.calcite.linq4j.Enumerator<Object> getSearchResultEnumerator(
        String search, Map<String, String> otherArgs, List<String> fieldList, Set<String> explicitFields) {
      // Mock implementation for testing - parameters intentionally unused
      LOGGER.debug("Mock connection: getSearchResultEnumerator called");
      return org.apache.calcite.linq4j.Linq4j.emptyEnumerator();
    }
  }
}
