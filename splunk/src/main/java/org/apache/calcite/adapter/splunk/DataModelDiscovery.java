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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Discovers Splunk data models dynamically via REST API.
 * Replaces hardcoded CIM model definitions with runtime discovery.
 */
public class DataModelDiscovery {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataModelDiscovery.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Cache configuration
  private static final long DEFAULT_CACHE_TTL_MINUTES = 60; // 1 hour default
  private static final Map<String, CachedDataModels> CACHE = new ConcurrentHashMap<>();

  private final SplunkConnectionImpl connection;
  private final String appContext;
  private final long cacheTtlMinutes;
  private final Map<String, List<CalculatedFieldDef>> additionalCalculatedFields;

  public DataModelDiscovery(SplunkConnection connection, String appContext) {
    this(connection, appContext, DEFAULT_CACHE_TTL_MINUTES);
  }

  public DataModelDiscovery(SplunkConnection connection, String appContext, long cacheTtlMinutes) {
    this(connection, appContext, cacheTtlMinutes, new HashMap<>());
  }

  public DataModelDiscovery(SplunkConnection connection, String appContext, long cacheTtlMinutes,
      Map<String, List<CalculatedFieldDef>> additionalCalculatedFields) {
    if (!(connection instanceof SplunkConnectionImpl)) {
      throw new IllegalArgumentException("DataModelDiscovery requires SplunkConnectionImpl");
    }
    this.connection = (SplunkConnectionImpl) connection;
    this.appContext = appContext;
    this.cacheTtlMinutes = cacheTtlMinutes;
    this.additionalCalculatedFields = additionalCalculatedFields != null ?
        additionalCalculatedFields : new HashMap<>();
  }

  /**
   * Discovers all data models in the configured app context.
   *
   * @param typeFactory the type factory for creating schemas
   * @param modelFilter optional filter pattern (glob or regex)
   * @return map of table name to Table object
   */
  public Map<String, Table> discoverDataModels(RelDataTypeFactory typeFactory,
      String modelFilter) {
    return discoverDataModels(typeFactory, modelFilter, false);
  }

  /**
   * Discovers all data models with optional cache refresh.
   *
   * @param typeFactory the type factory for creating schemas
   * @param modelFilter optional filter pattern (glob or regex)
   * @param forceRefresh if true, bypasses cache
   * @return map of table name to Table object
   */
  public Map<String, Table> discoverDataModels(RelDataTypeFactory typeFactory,
      String modelFilter, boolean forceRefresh) {
    Map<String, Table> tables = new LinkedHashMap<>();

    try {
      // Check cache first
      String cacheKey = buildCacheKey();
      List<DataModelInfo> models = null;

      if (!forceRefresh && cacheTtlMinutes != 0) {
        CachedDataModels cached = CACHE.get(cacheKey);
        if (cached != null && !cached.isExpired()) {
          models = cached.models;
          LOGGER.debug("Using cached data models for key: {}", cacheKey);
        }
      }

      // Fetch from API if not cached or forced refresh
      if (models == null) {
        LOGGER.info("Fetching data models from API (cache miss or forced refresh)");
        models = fetchDataModels();
        LOGGER.info("Fetched {} data models from Splunk REST API", models.size());

        // Update cache (skip only if cacheTtlMinutes is 0)
        if (cacheTtlMinutes != 0) {
          CACHE.put(cacheKey, new CachedDataModels(models, cacheTtlMinutes));
        }
      }

      LOGGER.info("Processing {} data models in app context: {}",
          models.size(), appContext != null ? appContext : "default");

      // Log discovered models
      for (DataModelInfo model : models) {
        LOGGER.info("Discovered data model: {} (display: {})",
            model.name, model.displayName);
      }

      // Apply filter if provided
      DataModelFilter filter = DataModelFilter.parse(modelFilter);

      // Create table for each data model
      for (DataModelInfo model : models) {
        if (filter != null && !filter.matches(model.name)) {
          LOGGER.debug("Skipping data model '{}' due to filter", model.name);
          continue;
        }

        try {
          Table table = createTableForDataModel(typeFactory, model);
          String tableName = normalizeTableName(model.name);
          tables.put(tableName, table);
          LOGGER.debug("Created table '{}' for data model '{}'", tableName, model.name);
        } catch (Exception e) {
          LOGGER.warn("Failed to create table for data model '{}': {}",
              model.name, e.getMessage());
        }
      }

      LOGGER.info("Created {} tables after filtering", tables.size());
    } catch (Exception e) {
      LOGGER.error("Failed to discover data models: {}", e.getMessage(), e);
      // Return empty map on failure - adapter can still work with custom tables
    }

    return tables;
  }

  /**
   * Fetches data model information from Splunk REST API.
   */
  private List<DataModelInfo> fetchDataModels() throws IOException {
    String endpoint = buildDataModelsEndpoint();
    LOGGER.debug("Fetching data models from: {}", endpoint);

    URL url = URI.create(endpoint).toURL();
    HttpURLConnection conn = connection.openConnection(url);

    try {
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Accept", "application/json");

      LOGGER.info("Fetching datamodels from URL: {}", url);
      int responseCode = conn.getResponseCode();
      if (responseCode != 200) {
        // Try to read error response
        String errorResponse = "";
        try (InputStream errorStream = conn.getErrorStream()) {
          if (errorStream != null) {
            BufferedReader reader =
                new BufferedReader(new InputStreamReader(errorStream, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
              sb.append(line).append("\n");
            }
            errorResponse = sb.toString();
          }
        } catch (Exception e) {
          // Ignore error reading error stream
        }
        LOGGER.error("HTTP {} error. Response: {}", responseCode, errorResponse);
        throw new IOException("Failed to fetch data models: HTTP " + responseCode);
      }

      // Parse JSON response
      try (InputStream is = conn.getInputStream();
           BufferedReader reader =
               new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

        JsonNode root = MAPPER.readTree(reader);
        return parseDataModelsResponse(root);
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Builds the REST endpoint URL for fetching data models.
   */
  private String buildDataModelsEndpoint() {
    URL url = connection.getUrl();
    String baseUrl =
        String.format(Locale.ROOT, "%s://%s:%d", url.getProtocol(),
        url.getHost(),
        url.getPort());

    // Always use the global data/models endpoint regardless of app context
    // The endpoint returns all datamodels across all apps with proper app metadata
    return baseUrl + "/services/data/models?output_mode=json&count=-1";
  }

  /**
   * Parses the JSON response from data/models endpoint.
   */
  private List<DataModelInfo> parseDataModelsResponse(JsonNode root) {
    List<DataModelInfo> models = new ArrayList<>();

    JsonNode entries = root.path("entry");
    LOGGER.info("Found {} entries in response", entries.isArray() ? entries.size() : 0);
    if (entries.isArray()) {
      for (JsonNode entry : entries) {
        try {
          DataModelInfo model = parseDataModelEntry(entry);
          if (model != null) {
            models.add(model);
          }
        } catch (Exception e) {
          LOGGER.warn("Failed to parse data model entry: {}", e.getMessage());
        }
      }
    }

    return models;
  }

  /**
   * Parses a single data model entry from the JSON response.
   */
  private DataModelInfo parseDataModelEntry(JsonNode entry) {
    String name = entry.path("name").asText();
    if (name.isEmpty()) {
      return null;
    }

    DataModelInfo model = new DataModelInfo();
    model.name = name;

    // Parse content section for model details
    JsonNode content = entry.path("content");
    if (!content.isMissingNode()) {
      // Get the raw JSON definition
      String rawJson = content.path("eai:data").asText();
      if (!rawJson.isEmpty()) {
        try {
          JsonNode modelDef = MAPPER.readTree(rawJson);
          parseModelDefinition(model, modelDef);
        } catch (Exception e) {
          LOGGER.warn("Failed to parse model definition for '{}': {}", name, e.getMessage());
        }
      }
    }

    return model;
  }

  /**
   * Parses the model definition to extract objects and fields.
   */
  private void parseModelDefinition(DataModelInfo model, JsonNode modelDef) {
    // Extract model name (might be different from entry name)
    model.displayName = modelDef.path("displayName").asText(model.name);

    // Parse objects (datasets) in the model
    JsonNode objects = modelDef.path("objects");
    if (objects.isArray()) {
      for (JsonNode obj : objects) {
        DatasetInfo dataset = parseDatasetObject(obj);
        if (dataset != null) {
          model.datasets.add(dataset);
        }
      }
    }
  }

  /**
   * Parses a dataset object within a data model.
   */
  private DatasetInfo parseDatasetObject(JsonNode obj) {
    String objectName = obj.path("objectName").asText();
    if (objectName.isEmpty()) {
      return null;
    }

    DatasetInfo dataset = new DatasetInfo();
    dataset.name = objectName;
    dataset.displayName = obj.path("displayName").asText(objectName);

    // Parse fields
    JsonNode fields = obj.path("fields");
    if (fields.isArray()) {
      for (JsonNode field : fields) {
        FieldInfo fieldInfo = parseField(field);
        if (fieldInfo != null) {
          dataset.fields.add(fieldInfo);
        }
      }
    }

    // Parse parent for inheritance
    dataset.parentName = obj.path("parentName").asText();

    return dataset;
  }

  /**
   * Parses field information.
   */
  private FieldInfo parseField(JsonNode field) {
    String fieldName = field.path("fieldName").asText();
    if (fieldName.isEmpty()) {
      return null;
    }

    FieldInfo info = new FieldInfo();
    info.name = fieldName;
    info.displayName = field.path("displayName").asText(fieldName);
    info.type = field.path("type").asText("string");
    info.required = field.path("required").asBoolean(false);
    info.multivalue = field.path("multivalue").asBoolean(false);

    return info;
  }

  /**
   * Creates a Table object for a data model.
   * For now, creates a table for the root dataset of each model.
   */
  private Table createTableForDataModel(RelDataTypeFactory typeFactory, DataModelInfo model) {
    // Build schema from root dataset fields
    RelDataTypeFactory.Builder schemaBuilder = typeFactory.builder();
    Map<String, String> fieldMapping = new HashMap<>();

    // Find root dataset (usually has same name as model)
    DatasetInfo rootDataset = findRootDataset(model);
    if (rootDataset == null) {
      throw new IllegalStateException("No root dataset found for model: " + model.name);
    }

    // First add core Splunk fields that exist in all events
    addCoreSplunkFields(schemaBuilder, fieldMapping, typeFactory);

    // Add fields from root dataset
    for (FieldInfo field : rootDataset.fields) {
      String calciteFieldName = normalizeFieldName(field.name);
      fieldMapping.put(calciteFieldName, field.name);

      SqlTypeName sqlType = mapSplunkTypeToSql(field.type);
      RelDataType fieldType = typeFactory.createSqlType(sqlType);

      // Make field nullable unless explicitly required
      if (!field.required) {
        fieldType = typeFactory.createTypeWithNullability(fieldType, true);
      }

      schemaBuilder.add(calciteFieldName, fieldType);
    }

    // Add calculated fields for known CIM models
    addCimCalculatedFields(model.name, schemaBuilder, fieldMapping, typeFactory);

    // Add any additional calculated fields specified via operand
    addAdditionalCalculatedFields(model.name, schemaBuilder, fieldMapping, typeFactory);

    // Always add _extra field for unmapped data
    schemaBuilder.add("_extra", typeFactory.createSqlType(SqlTypeName.ANY));

    RelDataType schema = schemaBuilder.build();

    // Create search string for this data model
    String searchString =
        String.format("| datamodel %s %s search", model.name, rootDataset.name);

    return new SplunkTable(schema, fieldMapping, searchString);
  }

  /**
   * Adds core Splunk fields that exist in all events.
   */
  private void addCoreSplunkFields(RelDataTypeFactory.Builder schemaBuilder,
      Map<String, String> fieldMapping, RelDataTypeFactory typeFactory) {
    // _time - event timestamp (mapped to "time" in Calcite)
    schemaBuilder.add("time",
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true));
    fieldMapping.put("time", "_time");

    // host - host that generated the event
    schemaBuilder.add("host",
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
    fieldMapping.put("host", "host");

    // source - source of the event
    schemaBuilder.add("source",
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
    fieldMapping.put("source", "source");

    // sourcetype - type of data source
    schemaBuilder.add("sourcetype",
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
    fieldMapping.put("sourcetype", "sourcetype");

    // index - Splunk index where event is stored
    schemaBuilder.add("index",
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
    fieldMapping.put("index", "index");
  }

  /**
   * Finds the root dataset in a data model.
   * Usually it's the one with no parent or with the same name as the model.
   */
  private DatasetInfo findRootDataset(DataModelInfo model) {
    // First try to find dataset with same name as model
    for (DatasetInfo dataset : model.datasets) {
      if (dataset.name.equalsIgnoreCase(model.name)) {
        return dataset;
      }
    }

    // Then look for dataset with no parent
    for (DatasetInfo dataset : model.datasets) {
      if (dataset.parentName == null || dataset.parentName.isEmpty()) {
        return dataset;
      }
    }

    // If still not found, use first dataset
    return model.datasets.isEmpty() ? null : model.datasets.get(0);
  }

  /**
   * Adds calculated fields for known CIM models.
   */
  private void addCimCalculatedFields(String modelName, RelDataTypeFactory.Builder schemaBuilder,
      Map<String, String> fieldMapping, RelDataTypeFactory typeFactory) {
    String normalizedName = modelName.toLowerCase(Locale.ROOT);

    // Common calculated fields across multiple models
    switch (normalizedName) {
      case "authentication":
        // Common authentication fields not in base model
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "action", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "app", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "user", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "src", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "src_user", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "dest", SqlTypeName.VARCHAR);
        // Boolean computed fields
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "is_failure", SqlTypeName.BOOLEAN);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "is_success", SqlTypeName.BOOLEAN);
        break;

      case "web":
        // Common web fields
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "action", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "app", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "user", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "src", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "dest", SqlTypeName.VARCHAR);
        break;

      case "network_traffic":
      case "network":
        // Common network fields
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "action", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "app", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "src", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "dest", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "src_ip", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "dest_ip", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "src_port", SqlTypeName.INTEGER);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "dest_port", SqlTypeName.INTEGER);
        break;

      case "endpoint":
        // Common endpoint fields
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "action", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "user", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "process", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "process_name", SqlTypeName.VARCHAR);
        break;

      case "malware":
        // Common malware fields
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "action", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "user", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "src", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "dest", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "signature", SqlTypeName.VARCHAR);
        break;

      case "email":
        // Common email fields
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "action", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "user", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "src_user", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "recipient", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "sender", SqlTypeName.VARCHAR);
        break;

      case "dlp":
      case "data_loss_prevention":
        // Common DLP fields
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "action", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "user", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "src", SqlTypeName.VARCHAR);
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "dest", SqlTypeName.VARCHAR);
        break;

      default:
        // For other models, add common action field if it's a known CIM model
        if (isKnownCimModel(normalizedName)) {
          addCalculatedField(schemaBuilder, fieldMapping, typeFactory, "action", SqlTypeName.VARCHAR);
        }
        break;
    }
  }

  /**
   * Helper to add a calculated field if not already present.
   */
  private void addCalculatedField(RelDataTypeFactory.Builder schemaBuilder,
      Map<String, String> fieldMapping, RelDataTypeFactory typeFactory,
      String fieldName, SqlTypeName sqlType) {
    // Check if field already exists
    if (!fieldMapping.containsKey(fieldName)) {
      schemaBuilder.add(fieldName,
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(sqlType), true));
      fieldMapping.put(fieldName, fieldName);
    }
  }

  /**
   * Checks if a model name is a known CIM model.
   */
  private boolean isKnownCimModel(String modelName) {
    // List of 24 standard CIM models
    switch (modelName) {
      case "alerts":
      case "application_state":
      case "applicationstate":
      case "authentication":
      case "certificates":
      case "change":
      case "compute_inventory":
      case "computeinventory":
      case "data_access":
      case "dataaccess":
      case "databases":
      case "database":
      case "data_loss_prevention":
      case "dlp":
      case "email":
      case "endpoint":
      case "event_signatures":
      case "eventsignatures":
      case "interprocess_messaging":
      case "messaging":
      case "intrusion_detection":
      case "ids":
      case "inventory":
      case "jvm":
      case "malware":
      case "network_resolution":
      case "dns":
      case "network_sessions":
      case "networksessions":
      case "network_traffic":
      case "network":
      case "performance":
      case "ticket_management":
      case "ticketing":
      case "updates":
      case "vulnerabilities":
      case "vulnerability":
      case "web":
        return true;
      default:
        return false;
    }
  }

  /**
   * Adds additional calculated fields specified via operand.
   */
  private void addAdditionalCalculatedFields(String modelName, RelDataTypeFactory.Builder schemaBuilder,
      Map<String, String> fieldMapping, RelDataTypeFactory typeFactory) {
    List<CalculatedFieldDef> fields = additionalCalculatedFields.get(modelName.toLowerCase(Locale.ROOT));
    if (fields != null) {
      for (CalculatedFieldDef field : fields) {
        addCalculatedField(schemaBuilder, fieldMapping, typeFactory, field.name, field.type);
      }
    }
  }

  /**
   * Maps Splunk field types to SQL types.
   */
  private SqlTypeName mapSplunkTypeToSql(String splunkType) {
    if (splunkType == null) {
      return SqlTypeName.VARCHAR;
    }

    switch (splunkType.toLowerCase(Locale.ROOT)) {
      case "number":
      case "int":
      case "integer":
        return SqlTypeName.INTEGER;
      case "float":
      case "double":
        return SqlTypeName.DOUBLE;
      case "bool":
      case "boolean":
        return SqlTypeName.BOOLEAN;
      case "time":
      case "timestamp":
        return SqlTypeName.TIMESTAMP;
      case "date":
        return SqlTypeName.DATE;
      default:
        return SqlTypeName.VARCHAR;
    }
  }

  /**
   * Normalizes table names to valid SQL identifiers.
   */
  private String normalizeTableName(String name) {
    return name.toLowerCase(Locale.ROOT)
        .replaceAll("[^a-z0-9_]", "_")
        .replaceAll("_+", "_")
        .replaceAll("^_|_$", "");
  }

  /**
   * Normalizes field names to valid SQL identifiers.
   */
  private String normalizeFieldName(String name) {
    // Handle special Splunk fields
    if (name.equals("_time")) {
      return "time";
    }

    return name.toLowerCase(Locale.ROOT)
        .replaceAll("[^a-z0-9_]", "_")
        .replaceAll("_+", "_")
        .replaceAll("^_|_$", "");
  }

  /**
   * Builds cache key based on connection and app context.
   */
  private String buildCacheKey() {
    URL url = connection.getUrl();
    return String.format("%s:%d/%s",
        url.getHost(),
        url.getPort(),
        appContext != null ? appContext : "default");
  }

  /**
   * Clears the cache for this connection/app context.
   */
  public void clearCache() {
    String cacheKey = buildCacheKey();
    CACHE.remove(cacheKey);
    LOGGER.info("Cleared cache for key: {}", cacheKey);
  }

  /**
   * Clears all cached data models.
   */
  public static void clearAllCache() {
    CACHE.clear();
    LOGGER.info("Cleared all data model cache");
  }

  /**
   * Information about a data model.
   */
  private static class DataModelInfo {
    String name;
    String displayName;
    List<DatasetInfo> datasets = new ArrayList<>();
  }

  /**
   * Information about a dataset within a data model.
   */
  private static class DatasetInfo {
    String name;
    String displayName;
    String parentName;
    List<FieldInfo> fields = new ArrayList<>();
  }

  /**
   * Information about a field.
   */
  private static class FieldInfo {
    String name;
    String displayName;
    String type;
    boolean required;
    boolean multivalue;
  }

  /**
   * Filter for data model names.
   */
  public static class DataModelFilter {
    private final Pattern pattern;
    private final boolean exclude;

    private DataModelFilter(Pattern pattern, boolean exclude) {
      this.pattern = pattern;
      this.exclude = exclude;
    }

    public static DataModelFilter parse(String filterSpec) {
      if (filterSpec == null || filterSpec.trim().isEmpty()) {
        return null;
      }

      filterSpec = filterSpec.trim();

      // Check for exclusion prefix
      boolean exclude = filterSpec.startsWith("!");
      if (exclude) {
        filterSpec = filterSpec.substring(1);
      }

      // Check if it's a regex pattern
      if (filterSpec.startsWith("/") && filterSpec.endsWith("/") && filterSpec.length() > 2) {
        // Regex pattern
        String regex = filterSpec.substring(1, filterSpec.length() - 1);
        return new DataModelFilter(Pattern.compile(regex), exclude);
      } else {
        // Glob pattern - convert to regex
        String regex = globToRegex(filterSpec);
        return new DataModelFilter(Pattern.compile(regex, Pattern.CASE_INSENSITIVE), exclude);
      }
    }

    public boolean matches(String name) {
      boolean match = pattern.matcher(name).matches();
      return exclude ? !match : match;
    }

    private static String globToRegex(String glob) {
      StringBuilder regex = new StringBuilder("^");

      for (int i = 0; i < glob.length(); i++) {
        char c = glob.charAt(i);
        switch (c) {
          case '*':
            regex.append(".*");
            break;
          case '?':
            regex.append(".");
            break;
          case '.':
          case '(':
          case ')':
          case '[':
          case ']':
          case '{':
          case '}':
          case '+':
          case '^':
          case '$':
          case '|':
          case '\\':
            regex.append("\\").append(c);
            break;
          default:
            regex.append(c);
        }
      }

      regex.append("$");
      return regex.toString();
    }
  }

  /**
   * Cached data models with expiration.
   * TTL behavior:
   * - ttlMinutes > 0: Normal expiration after the specified time
   * - ttlMinutes = 0: No caching (not used in this class)
   * - ttlMinutes = -1: Permanent cache (never expires)
   */
  private static class CachedDataModels {
    final List<DataModelInfo> models;
    final long expirationTime;
    final boolean permanent;

    CachedDataModels(List<DataModelInfo> models, long ttlMinutes) {
      this.models = models;
      this.permanent = (ttlMinutes == -1);
      this.expirationTime = permanent ? Long.MAX_VALUE :
          System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(ttlMinutes);
    }

    boolean isExpired() {
      return !permanent && System.currentTimeMillis() > expirationTime;
    }
  }

  /**
   * Definition of a calculated field to be added to a model.
   */
  public static class CalculatedFieldDef {
    public final String name;
    public final SqlTypeName type;

    public CalculatedFieldDef(String name, SqlTypeName type) {
      this.name = name;
      this.type = type;
    }

    /**
     * Parse calculated field definitions from operand.
     * Expected format: modelName -> [ {name: "fieldName", type: "VARCHAR"}, ... ]
     */
    public static Map<String, List<CalculatedFieldDef>> parseFromOperand(Map<String, Object> operand) {
      Map<String, List<CalculatedFieldDef>> result = new HashMap<>();

      Object fieldsObj = operand.get("calculatedFields");
      if (fieldsObj instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldsMap = (Map<String, Object>) fieldsObj;

        for (Map.Entry<String, Object> entry : fieldsMap.entrySet()) {
          String modelName = entry.getKey().toLowerCase(Locale.ROOT);
          List<CalculatedFieldDef> fields = new ArrayList<>();

          if (entry.getValue() instanceof List) {
            @SuppressWarnings("unchecked")
            List<Map<String, String>> fieldDefs = (List<Map<String, String>>) entry.getValue();

            for (Map<String, String> fieldDef : fieldDefs) {
              String name = fieldDef.get("name");
              String type = fieldDef.get("type");

              if (name != null && type != null) {
                SqlTypeName sqlType = parseSqlType(type);
                fields.add(new CalculatedFieldDef(name, sqlType));
              }
            }
          }

          if (!fields.isEmpty()) {
            result.put(modelName, fields);
          }
        }
      }

      return result;
    }

    private static SqlTypeName parseSqlType(String type) {
      String upperType = type.toUpperCase(Locale.ROOT);
      switch (upperType) {
        case "VARCHAR":
        case "STRING":
        case "TEXT":
          return SqlTypeName.VARCHAR;
        case "INTEGER":
        case "INT":
          return SqlTypeName.INTEGER;
        case "BIGINT":
        case "LONG":
          return SqlTypeName.BIGINT;
        case "DOUBLE":
        case "FLOAT":
          return SqlTypeName.DOUBLE;
        case "BOOLEAN":
        case "BOOL":
          return SqlTypeName.BOOLEAN;
        case "TIMESTAMP":
        case "DATETIME":
          return SqlTypeName.TIMESTAMP;
        case "DATE":
          return SqlTypeName.DATE;
        case "TIME":
          return SqlTypeName.TIME;
        default:
          return SqlTypeName.VARCHAR;
      }
    }
  }
}
