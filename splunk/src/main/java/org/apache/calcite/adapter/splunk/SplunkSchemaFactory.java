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

import com.google.common.collect.ImmutableMap;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating Splunk schemas.
 * Supports multiple modes:
 * 1. Single CIM model: {"cim_model": "authentication"} -> creates table named after the model
 * 2. Multiple CIM models: {"cim_models": ["auth", "network"]} -> creates multiple named tables
 * 3. Custom tables: Uses standard Calcite table definitions with optional search strings
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

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    // Create type factory directly
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // Check for multiple CIM models first
    Object cimModelsObj = operand.get("cim_models");
    if (cimModelsObj instanceof List) {
      @SuppressWarnings("unchecked")
      List<String> cimModels = (List<String>) cimModelsObj;
      return createMultiCimSchema(typeFactory, cimModels, operand);
    }

    // Check for single CIM model
    String cimModel = (String) operand.get("cim_model");
    if (cimModel != null) {
      return createSingleCimSchema(typeFactory, cimModel, operand);
    }

    // Default: custom table mode (tables defined via JSON)
    return new SplunkSchema(createConnection(operand));
  }

  /**
   * Creates a schema with multiple tables, one for each CIM model.
   * Table names match the CIM model names.
   */
  private Schema createMultiCimSchema(RelDataTypeFactory typeFactory,
      List<String> cimModels, Map<String, Object> operand) {
    ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    for (String cimModel : cimModels) {
      CimModelBuilder.CimSchemaResult result = CimModelBuilder.buildCimSchemaWithMapping(
          typeFactory, cimModel);

      SplunkTable table = new SplunkTable(
          result.getSchema(),
          result.getFieldMapping(),
          result.getSearchString());

      // Use the CIM model name as the table name
      String tableName = normalizeTableName(cimModel);
      builder.put(tableName, table);
    }

    SplunkConnection connection = createConnection(operand);
    return new SplunkSchema(connection, builder.build());
  }

  /**
   * Creates a schema with a single table named after the CIM model.
   */
  private Schema createSingleCimSchema(RelDataTypeFactory typeFactory,
      String cimModel, Map<String, Object> operand) {
    CimModelBuilder.CimSchemaResult result = CimModelBuilder.buildCimSchemaWithMapping(
        typeFactory, cimModel);

    SplunkTable table = new SplunkTable(
        result.getSchema(),
        result.getFieldMapping(),
        result.getSearchString());

    // Use the normalized CIM model name as the table name (consistent with multi-model behavior)
    String tableName = normalizeTableName(cimModel);
    Map<String, Table> tables = Collections.singletonMap(tableName, table);
    SplunkConnection connection = createConnection(operand);
    return new SplunkSchema(connection, tables);
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
    Boolean disableSslValidation = (Boolean) operand.get("disable_ssl_validation");
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
