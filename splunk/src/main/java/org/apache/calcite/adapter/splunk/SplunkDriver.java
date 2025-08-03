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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;
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
 * Enhanced JDBC driver for Splunk with dynamic data model discovery, federation support, advanced caching, and environment variable support.
 *
 * <p>It accepts connect strings that start with "jdbc:splunk:" and supports
 * additional parameters for CIM models, custom tables, and enhanced features.</p>
 *
 * <p>Enhanced connection string examples:</p>
 * <ul>
 * <li>Token Auth: jdbc:splunk:url=
 * <a href="https://localhost:8089;token=eyJhbGciOiJIUzI1NiI...">...</a></li>
 * <li>Basic Auth: jdbc:splunk:url=
 * <a href="https://localhost:8089;user=admin;password=changeme">...</a></li>
 * <li>Filtered discovery: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;
 * app=Splunk_SA_CIM;datamodelFilter=Authentication</li>
 * <li>Multiple models: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;
 * app=Splunk_SA_CIM;datamodelFilter=/^(Authentication|Web|Network_Traffic)$/</li>
 * <li>Custom default schema: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;
 * schema=myschema</li>
 * <li>Custom tables: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;
 * customTables=[{"name":"alerts","columns":[...]}]</li>
 * <li>App context: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;
 * app=Splunk_SA_CIM</li>
 * <li>Datamodel filter: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;
 * app=my_app;datamodelFilter=prod_*</li>
 * <li>Cache control: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;
 * datamodelCacheTtl=120;refreshDatamodels=true</li>
 * <li>Permanent cache: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;
 * datamodelCacheTtl=-1</li>
 * <li>No cache: jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;
 * datamodelCacheTtl=0</li>
 * <li>Environment variables: jdbc:splunk:datamodelCacheTtl=-1 (credentials from SPLUNK_URL,
 * SPLUNK_TOKEN, SPLUNK_APP environment variables)</li>
 * </ul>
 *
 * <p>Environment Variable Support (Production Deployments):</p>
 * <ul>
 * <li>SPLUNK_URL - Splunk server URL</li>
 * <li>SPLUNK_TOKEN - Authentication token (preferred)</li>
 * <li>SPLUNK_USER - Username</li>
 * <li>SPLUNK_PASSWORD - Password</li>
 * <li>SPLUNK_APP - App context</li>
 * </ul>
 * <p>Environment variables are used as fallback when parameters are not specified in URL or Properties.
 * This enables secure credential management in production deployments.</p>
 *
 * <p>Federation Support:</p>
 * <p>For multi-vendor security analytics, configure multiple schemas with different app contexts
 * in a Calcite model file to enable cross-platform queries and correlation:</p>
 * <pre>{@code
 * // Cross-vendor threat correlation
 * SELECT c.threat_category, p.action, cs.detection_name
 * FROM cisco.email c
 * JOIN paloalto.threat p ON c.src_ip = p.src_ip
 * JOIN crowdstrike.endpoint cs ON p.dest_ip = cs.dest_ip
 * WHERE c.time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
 * }</pre>
 *
 * <p>Default Schema Configuration:</p>
 * <ul>
 * <li>By default, unqualified table names resolve to the "splunk" schema</li>
 * <li>Use 'schema' or 'currentSchema' properties to override the default schema</li>
 * <li>Example: schema=myschema makes "web" resolve to "myschema"."web"</li>
 * </ul>
 *
 * <p>Note: For custom tables, the JSON must be URL-encoded. In Java 8:</p>
 * <pre>{@code
 * String customTablesJson = "[{\"name\":\"alerts\",\"columns\":[...]}]";
 * String encodedJson = URLEncoder.encode(customTablesJson, "UTF-8");
 * String url = "jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme;
 * customTables=" + encodedJson;
 * }</pre>
 */
public class SplunkDriver extends org.apache.calcite.jdbc.Driver {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkDriver.class);

  public SplunkDriver() {
    super();
  }

  static {
    System.setProperty("http.keepAlive", "false");
    System.setProperty("http.maxConnections", "1");
    System.setProperty("sun.net.http.retryPost", "false");
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
      // Parse URL parameters and merge with Properties
      Properties mergedInfo = parseUrlAndMergeProperties(url, info);

      // Extract and validate connection properties
      ConnectionProperties props = extractConnectionProperties(mergedInfo);
      validateRequiredProperties(props);

      // Create Splunk connection with token or username/password authentication
      SplunkConnection splunkConnection = createSplunkConnection(props);

      // Create enhanced schema with new features
      createEnhancedSchema(calciteConnection, props, splunkConnection);

      // Set default schema so unqualified table names resolve correctly
      // Support configurable default schema via 'schema' or 'currentSchema' properties
      String defaultSchema = props.defaultSchema;
      if (defaultSchema == null || defaultSchema.trim().isEmpty()) {
        defaultSchema = "splunk"; // Default fallback
      }
      calciteConnection.setSchema(defaultSchema);

      if (props.debug) {
        LOGGER.info("Successfully created enhanced Splunk connection with features: "
                + "cimModels={}, customTables={}, defaultTimeRange={}, defaultSchema={}",
            props.cimModels, props.customTables.size(),
            props.defaultTimeRange, defaultSchema);
      }

    } catch (Exception e) {
      LOGGER.error("Failed to create enhanced Splunk connection", e);
      throw new SQLException("Failed to create enhanced Splunk connection: " + e.getMessage(), e);
    }

    return connection;
  }

  /**
   * Parses URL parameters and merges them with the provided Properties.
   * URL parameters take precedence over Properties.
   *
   * @param url the JDBC URL (e.g., "jdbc:splunk:url=https://localhost:8089;user=admin;password=changeme")
   * @param info the original Properties object
   * @return merged Properties with URL parameters taking precedence
   */
  private Properties parseUrlAndMergeProperties(String url, Properties info) throws SQLException {
    Properties merged = new Properties();
    // Start with original properties
    merged.putAll(info);

    // Parse URL parameters if present
    if (url != null && url.startsWith(getConnectStringPrefix())) {
      String paramString = url.substring(getConnectStringPrefix().length());

      if (!paramString.isEmpty()) {
        try {
          // Split by semicolon and parse key=value pairs
          String[] params = paramString.split(";");
          for (String param : params) {
            String trimmedParam = param.trim();
            if (!trimmedParam.isEmpty()) {
              int equalsIndex = trimmedParam.indexOf('=');
              if (equalsIndex > 0 && equalsIndex < trimmedParam.length() - 1) {
                String key = trimmedParam.substring(0, equalsIndex).trim();
                String value = trimmedParam.substring(equalsIndex + 1).trim();

                // Remove surrounding quotes if present
                if (value.startsWith("'") && value.endsWith("'") && value.length() > 1) {
                  value = value.substring(1, value.length() - 1);
                } else if (value.startsWith("\"") && value.endsWith("\"") && value.length() > 1) {
                  value = value.substring(1, value.length() - 1);
                }

                // URL decode the value
                try {
                  value = URLDecoder.decode(value, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                  // UTF-8 should always be supported, but if not, use the value as-is
                  LOGGER.warn("Failed to URL decode parameter value: {}", value);
                }

                // Override property with URL parameter
                merged.setProperty(key, value);

                if (LOGGER.isDebugEnabled()) {
                  // Don't log sensitive values
                  String logValue = key.toLowerCase().contains("password") || key.toLowerCase().contains("token")
                      ? "***" : value;
                  LOGGER.debug("Parsed URL parameter: {}={}", key, logValue);
                }
              }
            }
          }
        } catch (Exception e) {
          throw new SQLException("Failed to parse URL parameters: " + e.getMessage(), e);
        }
      }
    }

    return merged;
  }

  /**
   * Extracts and processes connection properties from the JDBC properties.
   *
   * @param info the JDBC connection properties
   * @return non-null ConnectionProperties object
   */
  private ConnectionProperties extractConnectionProperties(Properties info) throws SQLException {
    ConnectionProperties props = new ConnectionProperties();

    // Basic required properties (can be null from Properties.getProperty())
    props.splunkUrl = getPropertyWithEnvFallback(info, "url", "SPLUNK_URL");
    props.token = getPropertyWithEnvFallback(info, "token", "SPLUNK_TOKEN");
    props.user = getPropertyWithEnvFallback(info, "user", "SPLUNK_USER");
    props.password = getPropertyWithEnvFallback(info, "password", "SPLUNK_PASSWORD");
    props.appContext = getPropertyWithEnvFallback(info, "app", "SPLUNK_APP");
    props.datamodelFilter = info.getProperty("datamodelFilter");
    props.datamodelCacheTtl = getLongProperty(info, "datamodelCacheTtl", 60L);
    props.refreshDatamodels = Boolean.parseBoolean(info.getProperty("refreshDatamodels", "false"));

    // Legacy CIM model parameters are no longer supported
    // Use app context and datamodelFilter instead
    props.defaultTimeRange = info.getProperty("defaultTimeRange");
    props.debug = Boolean.parseBoolean(info.getProperty("debug", "false"));
    props.logLevel = info.getProperty("logLevel");

    // Default schema configuration - support standard JDBC property names
    String defaultSchema = info.getProperty("schema");
    if (defaultSchema == null) {
      defaultSchema = info.getProperty("currentSchema");
    }
    props.defaultSchema = defaultSchema;

    // SSL validation - support both camelCase and snake_case
    String disableSsl = info.getProperty("disableSslValidation");
    if (disableSsl == null) {
      disableSsl = info.getProperty("disable_ssl_validation");
    }
    props.disableSslValidation = Boolean.parseBoolean(disableSsl);

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
    boolean hasCredentials = props.user != null && props.password != null
        && !props.user.trim().isEmpty() && !props.password.trim().isEmpty();

    if (!hasToken && !hasCredentials) {
      throw new SQLException("Must specify either 'token' or both 'user' and 'password' "
              + "properties");
    }
  }

  /**
   * Creates a SplunkConnection based on the provided properties.
   * Properties have been validated as non-null at this point.
   *
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
          return new SplunkConnectionImpl(splunkUrl, props.token, props.disableSslValidation,
              props.appContext);
        }

        // Fall back to username/password
        return new SplunkConnectionImpl(splunkUrl, props.user, props.password,
            props.disableSslValidation, props.appContext);

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

    // Dynamic discovery is now the default - no CIM model configuration needed
    if (props.debug) {
      LOGGER.debug("Using dynamic data model discovery");
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

    // Add SSL validation setting
    if (props.disableSslValidation) {
      operand.put("disableSslValidation", true);
    }

    // Add app context if specified
    if (props.appContext != null && !props.appContext.trim().isEmpty()) {
      operand.put("app", props.appContext);
    }

    // Add datamodel discovery parameters
    if (props.datamodelFilter != null && !props.datamodelFilter.trim().isEmpty()) {
      operand.put("datamodelFilter", props.datamodelFilter);
    }
    operand.put("datamodelCacheTtl", props.datamodelCacheTtl);
    operand.put("refreshDatamodels", props.refreshDatamodels);

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
   *
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
   *
   * @param customTablesJson the JSON string containing table definitions (can be null or empty)
   * @return non-null list of custom table definitions (empty if no valid tables found)
   */
  private List<Map<String, Object>> parseCustomTables(@Nullable String customTablesJson)
      throws SQLException {
    List<Map<String, Object>> customTables = new ArrayList<>();

    if (customTablesJson != null && !customTablesJson.trim().isEmpty()) {
      try {
        // URL decode the JSON string (Java 8 compatible)
        String decodedJson = URLDecoder.decode(customTablesJson, "UTF-8");

        // Parse JSON using Jackson
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<List<Map<String, Object>>> typeRef =
            new TypeReference<List<Map<String, Object>>>() {
            };

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
        throw new SQLException("Custom table at index " + i + " must have a non-empty 'name' "
                + "property");
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
          throw new SQLException("Column at index " + j + " in table '" + name
                  + "' must have a non-empty 'name' property");
        }

        Object columnType = column.get("type");
        if (!(columnType instanceof String) || ((String) columnType).trim().isEmpty()) {
          throw new SQLException("Column '" + columnName + "' in table '" + name
                  + "' must have a non-empty 'type' property");
        }
      }
    }
  }

  /**
   * Internal class to hold parsed connection properties.
   */
  private static class ConnectionProperties {
    @Nullable
    String splunkUrl;
    @Nullable
    String token;
    @Nullable
    String user;
    @Nullable
    String password;
    @Nullable
    String cimModel;
    List<String> cimModels = new ArrayList<>();
    @Nullable
    String defaultTimeRange;
    boolean debug = false;
    @Nullable
    String logLevel;
    @Nullable
    String defaultSchema;
    @Nullable
    String appContext;
    @Nullable
    String datamodelFilter;
    long datamodelCacheTtl = 60L;
    boolean refreshDatamodels = false;
    List<Map<String, Object>> customTables = new ArrayList<>();
    boolean disableSslValidation = false;
  }

  /**
   * Safely gets a Long property value.
   */
  private Long getLongProperty(Properties props, String key, Long defaultValue) {
    String value = props.getProperty(key);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid long value for property '{}': {}", key, value);
      return defaultValue;
    }
  }

  /**
   * Gets a property value with environment variable fallback.
   * Checks the Properties object first, then falls back to environment variable.
   *
   * @param props Properties object
   * @param propertyKey Property key to check
   * @param envVarName Environment variable name to check as fallback
   * @return Property value or null if neither is set
   */
  private String getPropertyWithEnvFallback(Properties props, String propertyKey, String envVarName) {
    // Check Properties object first
    String value = props.getProperty(propertyKey);
    if (value != null && !value.trim().isEmpty()) {
      return value;
    }

    // Fall back to environment variable
    String envValue = System.getenv(envVarName);
    if (envValue != null && !envValue.trim().isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        // Don't log sensitive values
        String logValue = propertyKey.toLowerCase().contains("password") ||
                         propertyKey.toLowerCase().contains("token") ? "***" : envValue;
        LOGGER.debug("Using environment variable {} for property {}: {}", envVarName, propertyKey, logValue);
      }
      return envValue;
    }

    return null;
  }

  /**
   * Mock implementation for testing purposes.
   */
  private static class MockSplunkConnection implements SplunkConnection {
    @Override public void getSearchResults(String search, Map<String, String> otherArgs,
        List<String> fieldList,
        org.apache.calcite.adapter.splunk.search.SearchResultListener srl) {
      // Mock implementation for testing - parameters intentionally unused
      LOGGER.debug("Mock connection: getSearchResults called");
    }

    @Override public org.apache.calcite.linq4j.Enumerator<Object> getSearchResultEnumerator(
        String search, Map<String, String> otherArgs, List<String> fieldList,
            Set<String> explicitFields) {
      // Mock implementation for testing - parameters intentionally unused
      LOGGER.debug("Mock connection: getSearchResultEnumerator called");
      return org.apache.calcite.linq4j.Linq4j.emptyEnumerator();
    }

    @Override public org.apache.calcite.linq4j.Enumerator<Object> getSearchResultEnumerator(
        String search, Map<String, String> otherArgs, List<String> fieldList,
            Set<String> explicitFields,
        Map<String, String> reverseFieldMapping) {
      // Mock implementation for testing - parameters intentionally unused
      LOGGER.debug("Mock connection: getSearchResultEnumerator with field mapping called");
      return org.apache.calcite.linq4j.Linq4j.emptyEnumerator();
    }
  }
}
