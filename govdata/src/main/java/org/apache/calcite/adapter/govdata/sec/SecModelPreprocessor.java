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
package org.apache.calcite.adapter.sec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Preprocessor for XBRL model files that adds smart defaults.
 *
 * <p>This preprocessor automatically adds:
 * <ul>
 *   <li>version: "1.0" if not specified</li>
 *   <li>factory: SecEmbeddingSchemaFactory for XBRL schemas without factory</li>
 *   <li>defaultSchema: "XBRL" if not specified and XBRL schema exists</li>
 * </ul>
 *
 * <p>This allows users to write minimal model files like:
 * <pre>
 * {
 *   "schemas": [{
 *     "name": "XBRL",
 *     "operand": {
 *       "ciks": "AAPL"
 *     }
 *   }]
 * }
 * </pre>
 */
public class SecModelPreprocessor {
  private static final Logger LOGGER = Logger.getLogger(SecModelPreprocessor.class.getName());
  private static final String XBRL_FACTORY = "org.apache.calcite.adapter.sec.SecEmbeddingSchemaFactory";

  /**
   * Preprocess a model file to add XBRL defaults.
   *
   * @param modelFile The model file to preprocess
   * @return The preprocessed model as JSON string
   */
  public static String preprocessModelFile(File modelFile) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(modelFile);
    return preprocessModel(root);
  }

  /**
   * Preprocess a model JSON string to add XBRL defaults.
   *
   * @param modelJson The model JSON string
   * @return The preprocessed model as JSON string
   */
  public static String preprocessModelJson(String modelJson) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(modelJson);
    return preprocessModel(root);
  }

  /**
   * Preprocess a model to add XBRL defaults.
   */
  private static String preprocessModel(JsonNode root) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode model = (ObjectNode) root;

    // Add version if not present
    if (!model.has("version")) {
      model.put("version", "1.0");
      LOGGER.fine("Added default version: 1.0");
    }

    // Process schemas
    if (model.has("schemas")) {
      ArrayNode schemas = (ArrayNode) model.get("schemas");
      boolean hasSecSchema = false;

      for (JsonNode schemaNode : schemas) {
        ObjectNode schema = (ObjectNode) schemaNode;
        String name = schema.has("name") ? schema.get("name").asText() : "";

        // If it's an XBRL schema without a factory, add the default factory
        if ("XBRL".equalsIgnoreCase(name) || name.isEmpty()) {
          hasSecSchema = true;

          // Ensure name is XBRL
          if (name.isEmpty()) {
            schema.put("name", "XBRL");
            LOGGER.fine("Added default schema name: XBRL");
          }

          // Add factory if not present
          if (!schema.has("factory")) {
            schema.put("factory", XBRL_FACTORY);
            LOGGER.fine("Added default factory: " + XBRL_FACTORY);
          }
        }
      }

      // Add defaultSchema if we have an XBRL schema and no default specified
      if (hasSecSchema && !model.has("defaultSchema")) {
        model.put("defaultSchema", "XBRL");
        LOGGER.fine("Added defaultSchema: XBRL");
      }
    }

    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(model);
  }

  /**
   * Check if a model file needs preprocessing.
   *
   * @param modelFile The model file to check
   * @return true if the file needs preprocessing
   */
  public static boolean needsPreprocessing(File modelFile) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(modelFile);

    // Check if version is missing
    if (!root.has("version")) {
      return true;
    }

    // Check if any XBRL schema is missing factory
    if (root.has("schemas")) {
      ArrayNode schemas = (ArrayNode) root.get("schemas");
      for (JsonNode schemaNode : schemas) {
        String name = schemaNode.has("name") ? schemaNode.get("name").asText() : "";
        if ("XBRL".equalsIgnoreCase(name) && !schemaNode.has("factory")) {
          return true;
        }
      }
    }

    return false;
  }
}
