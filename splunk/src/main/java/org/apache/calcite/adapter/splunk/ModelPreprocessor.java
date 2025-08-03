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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Preprocesses Calcite model files to automatically inject factory specifications
 * for known adapter types when they are not explicitly specified.
 *
 * <p>This allows users to write cleaner model files without having to remember
 * factory class names for common adapters like Splunk.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * // Original model without factory
 * {
 *   "schemas": [{
 *     "name": "splunk",
 *     "type": "custom",
 *     "operand": {
 *       "url": "https://splunk.com:8089",
 *       "token": "abc123"
 *     }
 *   }]
 * }
 *
 * // After preprocessing
 * {
 *   "schemas": [{
 *     "name": "splunk",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.splunk.SplunkSchemaFactory",
 *     "operand": {
 *       "url": "https://splunk.com:8089",
 *       "token": "abc123"
 *     }
 *   }]
 * }
 * }</pre>
 */
public class ModelPreprocessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ModelPreprocessor.class);

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
      .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
      .configure(JsonParser.Feature.ALLOW_COMMENTS, true);

  /**
   * Preprocesses a model file by reading it schemaless and injecting
   * factory specifications where they can be auto-detected.
   *
   * @param modelUri the model URI (file path, inline:, etc.)
   * @return the preprocessed model content as a JSON string
   * @throws IOException if the model cannot be read or processed
   */
  public static String preprocessModel(String modelUri) throws IOException {
    JsonNode rootNode = readModelAsJson(modelUri);

    if (rootNode.has("schemas")) {
      JsonNode schemasNode = rootNode.get("schemas");
      if (schemasNode.isArray()) {
        for (JsonNode schemaNode : schemasNode) {
          if (schemaNode.isObject()) {
            injectFactoryIfNeeded((ObjectNode) schemaNode);
          }
        }
      }
    }

    // Convert back to JSON string
    return JSON_MAPPER.writeValueAsString(rootNode);
  }

  /**
   * Reads a model file as raw JSON, handling both file paths and inline models.
   */
  private static JsonNode readModelAsJson(String modelUri) throws IOException {
    if (modelUri.startsWith("inline:")) {
      // Handle inline models
      String content = modelUri.substring("inline:".length()).trim();
      return JSON_MAPPER.readTree(content);
    } else {
      // Handle file-based models (JSON only for now)
      if (modelUri.endsWith(".yaml") || modelUri.endsWith(".yml")) {
        throw new IOException("YAML model files are not currently supported. Please use JSON format.");
      }
      return JSON_MAPPER.readTree(new File(modelUri));
    }
  }

  /**
   * Injects a factory specification and type into a schema node if they can be auto-detected
   * and are not already present.
   */
  private static void injectFactoryIfNeeded(ObjectNode schemaNode) {
    // Try to detect from operand first
    String detectedFactory = null;
    if (schemaNode.has("operand")) {
      JsonNode operandNode = schemaNode.get("operand");
      if (operandNode.isObject()) {
        detectedFactory = inferFactoryFromOperand((ObjectNode) operandNode);
      }
    }

    // If we detected a factory, inject both type and factory if not present
    if (detectedFactory != null) {
      // Inject type if not present
      if (!schemaNode.has("type")) {
        schemaNode.put("type", "custom");

        if (LOGGER.isDebugEnabled()) {
          String schemaName = schemaNode.has("name")
              ? schemaNode.get("name").asText() : "unnamed";
          LOGGER.debug("Auto-detected type for schema '{}': custom", schemaName);
        }
      }

      // Inject factory if not present
      if (!schemaNode.has("factory")) {
        schemaNode.put("factory", detectedFactory);

        if (LOGGER.isDebugEnabled()) {
          String schemaName = schemaNode.has("name")
              ? schemaNode.get("name").asText() : "unnamed";
          LOGGER.debug("Auto-detected factory for schema '{}': {}", schemaName, detectedFactory);
        }
      }

      return;
    }

    // Legacy behavior: only inject factory if type is already custom
    if (schemaNode.has("type") && "custom".equals(schemaNode.get("type").asText())
        && !schemaNode.has("factory") && schemaNode.has("operand")) {
      JsonNode operandNode = schemaNode.get("operand");
      if (operandNode.isObject()) {
        String factory = inferFactoryFromOperand((ObjectNode) operandNode);
        if (factory != null) {
          schemaNode.put("factory", factory);

          if (LOGGER.isDebugEnabled()) {
            String schemaName = schemaNode.has("name")
                ? schemaNode.get("name").asText() : "unnamed";
            LOGGER.debug("Auto-detected factory for schema '{}': {}", schemaName, factory);
          }
        }
      }
    }
  }

  /**
   * Infers the factory class name from an operand node for known adapter types.
   *
   * @param operandNode the operand configuration
   * @return the factory class name, or null if no factory can be inferred
   */
  private static String inferFactoryFromOperand(ObjectNode operandNode) {
    // Detect Splunk adapter based on presence of Splunk-specific properties
    if (operandNode.has("url")) {
      boolean hasAuth = operandNode.has("token") ||
          (operandNode.has("username") && operandNode.has("password")) ||
          (operandNode.has("user") && operandNode.has("password"));

      if (hasAuth) {
        // Additional Splunk-specific indicators
        boolean hasSplunkIndicators = operandNode.has("app") ||
                                     operandNode.has("datamodelFilter") ||
                                     operandNode.has("datamodelCacheTtl") ||
                                     operandNode.has("refreshDatamodels");

        // Check if URL looks like a Splunk URL
        String url = operandNode.get("url").asText();
        boolean looksLikeSplunk = url.contains(":8089") ||
                                 url.contains("splunk") ||
                                 hasSplunkIndicators;

        if (looksLikeSplunk) {
          return "org.apache.calcite.adapter.splunk.SplunkSchemaFactory";
        }
      }
    }

    // Could add other adapter auto-detection here in the future:
    // - JDBC adapter: detect jdbcUrl, jdbcDriver, etc.
    // - File adapter: detect directory, flavor, etc.
    // - Elasticsearch adapter: detect hosts, index, etc.

    return null;
  }
}
