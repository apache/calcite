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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Factory that creates an {@link OpenAPISchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 */
@SuppressWarnings("UnusedDeclaration")
public class OpenAPISchemaFactory implements SchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenAPISchemaFactory.class);

  public OpenAPISchemaFactory() {
  }

  /**
   * Create an OpenAPI {@link Schema}.
   * The operand property accepts the following key/value pairs:
   *
   * <ul>
   *   <li><b>baseUrl</b>: The base URL for the API (required)</li>
   *   <li><b>configFile</b>: Path to the OpenAPI configuration file (required)</li>
   *   <li><b>authentication</b>: Authentication configuration (optional)</li>
   * </ul>
   *
   * @param parentSchema Parent schema
   * @param name Name of this schema
   * @param operand The "operand" JSON property
   * @return Returns a {@link Schema} for the OpenAPI endpoints.
   */
  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    final Map<String, Object> map = (Map<String, Object>) operand;

    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    try {
      // Get base URL
      String baseUrl = (String) map.get("baseUrl");
      if (baseUrl == null) {
        throw new IllegalArgumentException("baseUrl is required");
      }

      // Load configuration
      String configFile = (String) map.get("configFile");
      OpenAPIConfig config;
      if (configFile != null) {
        // Load from file
        config =
            mapper.readValue(getClass().getClassLoader().getResourceAsStream(configFile),
            OpenAPIConfig.class);
      } else {
        // Try to parse inline configuration
        config = mapper.convertValue(map.get("config"), OpenAPIConfig.class);
      }

      if (config == null) {
        throw new IllegalArgumentException("Either configFile or config must be provided");
      }

      // Create transport
      OpenAPITransport transport = new OpenAPITransport(baseUrl, mapper, config.getAuthentication());

      return new OpenAPISchema(transport, config);
    } catch (IOException e) {
      throw new RuntimeException("Cannot parse OpenAPI configuration", e);
    }
  }
}
