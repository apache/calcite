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
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating Splunk schemas.
 * Supports multiple modes:
 * 1. Single CIM model: {"cim_model": "authentication"} -> creates table named after the model
 * 2. Multiple CIM models: {"cim_models": ["auth", "network"]} -> creates multiple named tables
 * 3. Custom tables: {"tables": [...]} -> creates user-defined tables with custom schemas
 * 4. Mixed mode: CIM models + custom tables can coexist
 *
 * Connection parameters can be specified in two ways:
 * 1. Complete URL: {"url": "https://host:port"}
 * 2. Individual components: {"host": "hostname", "port": 8089, "protocol": "https"}
 *
 * SSL Configuration:
 * - "disable_ssl_validation": true (WARNING: Only use in development/testing)
 * This setting is passed to SplunkConnectionImpl for per-connection SSL configuration.
 */
public class SplunkSchemaFactory implements SchemaFactory {

  @Override public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    ImmutableMap.Builder<String, Table> tableBuilder = ImmutableMap.builder();

    // Process environment variables for custom tables
    CustomTableConfigProcessor.processEnvironmentVariables(operand);

    // Process CIM models (both single and multiple)
    processCimModels(typeFactory, operand, tableBuilder);

    // Process custom table definitions
    processCustomTables(typeFactory, operand, tableBuilder);

    // Create connection
    SplunkConnection connection = createConnection(operand);
    Map<String, Table> tables = tableBuilder.build();

    if (tables.isEmpty()) {
      // No predefined tables - custom table mode via JSON definitions
      return new SplunkSchema(connection);
    } else {
      // Return schema with predefined tables
      return new SplunkSchema(connection, tables);
    }
  }

  /**
   * Processes CIM model definitions (both single and multiple).
   */
  private void processCimModels(RelDataTypeFactory typeFactory, Map<String, Object> operand,
      ImmutableMap.Builder<String, Table> tableBuilder) {

    // Handle multiple CIM models
    Object cimModelsObj = operand.get("cimModels");
    if (cimModelsObj instanceof List) {
      @SuppressWarnings("unchecked")
      List<String> cimModels = (List<String>) cimModelsObj;
      for (String cimModel : cimModels) {
        addCimModelTable(typeFactory, cimModel, tableBuilder);
      }
    }

    // Handle single CIM model
    String cimModel = (String) operand.get("cimModel");
    if (cimModel != null) {
      addCimModelTable(typeFactory, cimModel, tableBuilder);
    }
  }

  /**
   * Adds a single CIM model table to the builder.
   */
  private void addCimModelTable(RelDataTypeFactory typeFactory, String cimModel,
      ImmutableMap.Builder<String, Table> tableBuilder) {
      CimModelBuilder.CimSchemaResult result =
          CimModelBuilder.buildCimSchemaWithMapping(typeFactory, cimModel);

      SplunkTable table =
          new SplunkTable(result.getSchema(),
          result.getFieldMapping(),
          result.getSearchString());

      String tableName = normalizeTableName(cimModel);
    tableBuilder.put(tableName, table);
  }

  /**
   * Processes custom table definitions from the "tables" operand.
   */
  @SuppressWarnings("unchecked")
  private void processCustomTables(RelDataTypeFactory typeFactory, Map<String, Object> operand,
      ImmutableMap.Builder<String, Table> tableBuilder) {

    Object tablesObj = operand.get("tables");
    if (!(tablesObj instanceof List)) {
      return; // No custom tables defined
    }

    List<Object> tablesList = (List<Object>) tablesObj;
    for (Object tableObj : tablesList) {
      if (!(tableObj instanceof Map)) {
        System.err.println("Warning: Invalid table definition in operand. Expected Map, got: "
            + tableObj.getClass());
        continue;
      }

      Map<String, Object> tableConfig = (Map<String, Object>) tableObj;
      try {
        Table customTable = createCustomTable(typeFactory, tableConfig);
        String tableName = getRequiredString(tableConfig, "name");
        tableBuilder.put(tableName, customTable);
      } catch (Exception e) {
        System.err.println("Warning: Failed to create custom table: " + e.getMessage());
        e.printStackTrace();
      }
    }
  }

  /**
   * Creates a custom table from a table configuration map.
   */
  private Table createCustomTable(RelDataTypeFactory typeFactory, Map<String, Object> tableConfig) {
    String tableName = getRequiredString(tableConfig, "name");
    String searchString = (String) tableConfig.getOrDefault("search", "search");

    // Extract field mapping
    Map<String, String> fieldMapping = extractFieldMapping(tableConfig);

    // Build schema from field definitions
    RelDataType rowType = buildCustomTableSchema(typeFactory, tableConfig, fieldMapping);

    return new SplunkTable(rowType, fieldMapping, searchString);
  }

  /**
   * Builds the schema for a custom table from its configuration.
   */
  @SuppressWarnings("unchecked")
  private RelDataType buildCustomTableSchema(RelDataTypeFactory typeFactory,
      Map<String, Object> tableConfig, Map<String, String> fieldMapping) {

    RelDataTypeFactory.Builder builder = typeFactory.builder();

    // Check for explicit field definitions
    Object fieldsObj = tableConfig.get("fields");
    if (fieldsObj instanceof List) {
      List<Object> fieldsList = (List<Object>) fieldsObj;
      for (Object fieldObj : fieldsList) {
        if (fieldObj instanceof Map) {
          Map<String, Object> fieldConfig = (Map<String, Object>) fieldObj;
          addFieldToSchema(builder, typeFactory, fieldConfig);
        } else if (fieldObj instanceof String) {
          // Simple string field name - defaults to VARCHAR
          String fieldName = (String) fieldObj;
          builder.add(fieldName, typeFactory.createSqlType(SqlTypeName.VARCHAR));
        }
      }
    } else {
      // No explicit fields - infer from field mapping keys
      for (String schemaField : fieldMapping.keySet()) {
        builder.add(schemaField, typeFactory.createSqlType(SqlTypeName.VARCHAR));
      }
    }

    // Always add _extra field for unmapped data
    builder.add("_extra", typeFactory.createSqlType(SqlTypeName.ANY));

    return builder.build();
  }

  /**
   * Adds a single field to the schema builder from field configuration.
   */
  private void addFieldToSchema(RelDataTypeFactory.Builder builder,
      RelDataTypeFactory typeFactory, Map<String, Object> fieldConfig) {

    String fieldName = getRequiredString(fieldConfig, "name");
    String typeStr = (String) fieldConfig.getOrDefault("type", "VARCHAR");
    Boolean nullable = (Boolean) fieldConfig.getOrDefault("nullable", true);

    SqlTypeName sqlType;
    try {
      sqlType = SqlTypeName.valueOf(typeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      System.err.println("Warning: Unknown SQL type '" + typeStr + "' for field '"
          + fieldName + "'. Defaulting to VARCHAR.");
      sqlType = SqlTypeName.VARCHAR;
    }

    RelDataType fieldType = typeFactory.createSqlType(sqlType);
    if (!nullable) {
      fieldType = typeFactory.createTypeWithNullability(fieldType, false);
    }

    builder.add(fieldName, fieldType);
  }

  /**
   * Extracts field mapping from table configuration.
   * Supports both "fieldMapping" (Map) and "fieldMappings" (List of "key:value" strings).
   */
  @SuppressWarnings("unchecked")
  private Map<String, String> extractFieldMapping(Map<String, Object> tableConfig) {
    Map<String, String> fieldMapping = new HashMap<>();

    // Handle fieldMapping as Map<String, String>
    Object mappingObj = tableConfig.get("fieldMapping");
    if (mappingObj instanceof Map) {
      try {
        Map<String, Object> rawMapping = (Map<String, Object>) mappingObj;
        for (Map.Entry<String, Object> entry : rawMapping.entrySet()) {
          String key = entry.getKey();
          Object value = entry.getValue();
          if (value instanceof String) {
            fieldMapping.put(key, (String) value);
          }
        }
      } catch (ClassCastException e) {
        System.err.println("Warning: Invalid fieldMapping format. Expected Map<String, String>");
      }
    }

    // Handle fieldMappings as List<String> with "key:value" format
    Object mappingListObj = tableConfig.get("fieldMappings");
    if (mappingListObj instanceof List) {
      try {
        List<String> mappingList = (List<String>) mappingListObj;
        for (String mapping : mappingList) {
          String[] parts = mapping.split(":", 2);
          if (parts.length == 2) {
            String schemaField = parts[0].trim();
            String splunkField = parts[1].trim();
            if (!schemaField.isEmpty() && !splunkField.isEmpty()) {
              fieldMapping.put(schemaField, splunkField);
            }
          } else {
            System.err.println("Warning: Invalid field mapping format: '" + mapping
                + "'. Expected 'schema_field:splunk_field'");
          }
        }
      } catch (ClassCastException e) {
        System.err.println("Warning: Invalid fieldMappings format. Expected List<String>");
      }
    }

    return fieldMapping;
  }

  /**
   * Gets a required string parameter from a configuration map.
   */
  private String getRequiredString(Map<String, Object> config, String key) {
    Object value = config.get(key);
    if (!(value instanceof String)) {
      throw new IllegalArgumentException("Required parameter '" + key + "' is missing or not a string");
    }
    String str = (String) value;
    if (str.trim().isEmpty()) {
      throw new IllegalArgumentException("Required parameter '" + key + "' cannot be empty");
    }
    return str;
  }

  /**
   * Normalizes CIM model names to valid SQL table names.
   * Converts to lowercase and replaces spaces/special chars with underscores.
   */
  private String normalizeTableName(String cimModel) {
    return cimModel.toLowerCase()
        .replaceAll("[^a-z0-9_]", "_")
        .replaceAll("_+", "_")
        .replaceAll("^_|_$", "");
  }

  /**
   * Creates a Splunk connection from the operand parameters.
   * Supports both complete URL and individual component specification.
   */
  private SplunkConnection createConnection(Map<String, Object> operand) {
    String url = buildSplunkUrl(operand);
    Boolean disableSslValidation = (Boolean) operand.get("disableSslValidation");
    boolean disableSsl = Boolean.TRUE.equals(disableSslValidation);

    try {
      // Check for token authentication first
      String token = (String) operand.get("token");
      if (token != null && !token.trim().isEmpty()) {
        return new SplunkConnectionImpl(url, token, disableSsl);
      }

      // Fall back to username/password authentication
      String username = (String) operand.get("username");
      String password = (String) operand.get("password");
      return new SplunkConnectionImpl(url, username, password, disableSsl);

    } catch (MalformedURLException e) {
      throw new RuntimeException("Invalid Splunk URL: " + url, e);
    }
  }

  /**
   * Builds the Splunk URL from operand parameters.
   * Supports two approaches:
   * 1. Direct URL: {"url": "https://host:port"}
   * 2. Components: {"host": "hostname", "port": 8089, "protocol": "https"}
   */
  private String buildSplunkUrl(Map<String, Object> operand) {
    // Check if a complete URL is provided
    String url = (String) operand.get("url");
    if (url != null && !url.trim().isEmpty()) {
      // Validate the URL format using URI (modern approach)
      try {
        URI.create(url).toURL(); // Validates URI format and converts to URL
        return url;
      } catch (IllegalArgumentException | MalformedURLException e) {
        throw new RuntimeException("Invalid URL format: " + url, e);
      }
    }

    // Build URL from individual components
    String host = (String) operand.get("host");
    if (host == null || host.trim().isEmpty()) {
      throw new RuntimeException("Either 'url' or 'host' parameter must be provided");
    }

    Integer port = (Integer) operand.getOrDefault("port", 8089);
    String protocol = (String) operand.getOrDefault("protocol", "https");

    return String.format("%s://%s:%d", protocol, host, port);
  }
}
