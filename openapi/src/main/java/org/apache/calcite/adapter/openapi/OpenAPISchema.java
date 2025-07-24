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
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

/**
 * Schema that contains OpenAPI endpoint tables.
 * Each table represents a configured OpenAPI endpoint with its variants.
 */
public class OpenAPISchema extends AbstractSchema {

  private final OpenAPITransport transport;
  private final OpenAPIConfig config;
  private final Map<String, Table> tableMap;

  /**
   * Creates an OpenAPISchema.
   *
   * @param transport Transport layer for API calls
   * @param config Configuration defining tables and their endpoints
   */
  public OpenAPISchema(OpenAPITransport transport, OpenAPIConfig config) {
    super();
    this.transport = Objects.requireNonNull(transport, "transport");
    this.config = Objects.requireNonNull(config, "config");
    this.tableMap = createTables();
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  private Map<String, Table> createTables() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    // For now, create a single table per configuration
    // In a more sophisticated implementation, this would parse the config
    // to create multiple tables based on different endpoint groups
    String tableName = "api_table"; // Could be derived from config

    builder.put(tableName, new OpenAPITable(tableName, transport, config));

    return builder.build();
  }
}
