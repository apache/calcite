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
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating Splunk schemas.
 * Supports multiple modes:
 * 1. Single CIM model: {"cim_model": "authentication"} -> creates table named after the model
 * 2. Multiple CIM models: {"cim_models": ["auth", "network"]} -> creates multiple named tables
 * 3. Custom tables: Uses standard Calcite table definitions
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
      RelDataType tableSchema = CimModelBuilder.buildCimSchema(typeFactory, cimModel);
      SplunkTable table = new SplunkTable(tableSchema);

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
    RelDataType tableSchema = CimModelBuilder.buildCimSchema(typeFactory, cimModel);
    SplunkTable table = new SplunkTable(tableSchema);

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
   */
  private SplunkConnection createConnection(Map<String, Object> operand) {
    String host = (String) operand.get("host");
    Integer port = (Integer) operand.getOrDefault("port", 8089);
    String username = (String) operand.get("username");
    String password = (String) operand.get("password");
    String protocol = (String) operand.getOrDefault("protocol", "https");

    // Build the URL for SplunkConnectionImpl
    String url = String.format("%s://%s:%d", protocol, host, port);

    try {
      return new SplunkConnectionImpl(url, username, password);
    } catch (MalformedURLException e) {
      throw new RuntimeException("Invalid Splunk URL: " + url, e);
    }
  }
}
