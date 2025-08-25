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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;

import java.util.Map;
import java.util.Properties;

/**
 * Base class for file adapter tests.
 */
@Tag("unit")
public abstract class BaseFileTest {
  
  protected static final JavaTypeFactory typeFactory = 
      new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  
  /**
   * Gets the type factory for tests.
   */
  protected JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }
  
  /**
   * Gets the configured execution engine from environment.
   * This provides the test suite default which overrides FileSchema's default.
   * Individual tests can override this to use a specific engine.
   * 
   * @return the execution engine type, or null if not configured
   */
  protected String getExecutionEngine() {
    return System.getenv("CALCITE_FILE_ENGINE_TYPE");
  }
  
  /**
   * Gets an ExecutionEngineConfig based on the environment variable.
   * This ensures tests respect the CALCITE_FILE_ENGINE_TYPE environment variable.
   * 
   * @return ExecutionEngineConfig configured based on environment, or default if not set
   */
  protected static ExecutionEngineConfig getEngineConfig() {
    String engineType = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    if (engineType != null && !engineType.isEmpty()) {
      return new ExecutionEngineConfig(engineType, ExecutionEngineConfig.DEFAULT_BATCH_SIZE);
    }
    return new ExecutionEngineConfig();
  }
  
  /**
   * Applies standard engine defaults for SQL identifier casing to JDBC connection properties.
   * These settings ensure consistent identifier behavior across all tests:
   * - lex: ORACLE (SQL lexical rules)
   * - unquotedCasing: TO_LOWER (unquoted identifiers converted to lowercase)
   * 
   * This method should be called by all tests after setting the model property.
   * 
   * @param info the Properties object to update with engine defaults
   */
  public static void applyEngineDefaults(Properties info) {
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
  }
  
  /**
   * Adds ephemeralCache to a model JSON string for test isolation.
   * This method parses the model, adds ephemeralCache: true to each schema operand,
   * and returns the modified JSON string.
   * 
   * @param modelJson the original model JSON string
   * @return the modified model JSON with ephemeralCache added
   */
  public static String addEphemeralCacheToModel(String modelJson) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> model = mapper.readValue(modelJson, Map.class);
      
      // Add ephemeralCache to each schema's operand (not at model level to avoid Calcite validation errors)
      if (model.containsKey("schemas")) {
        Object schemasObj = model.get("schemas");
        if (schemasObj instanceof java.util.List) {
          java.util.List<Map<String, Object>> schemas = (java.util.List<Map<String, Object>>) schemasObj;
          for (Map<String, Object> schema : schemas) {
            if (schema.containsKey("operand")) {
              Object operandObj = schema.get("operand");
              if (operandObj instanceof Map) {
                Map<String, Object> operand = (Map<String, Object>) operandObj;
                // Only add if not already present (allows override)
                if (!operand.containsKey("ephemeralCache") && !operand.containsKey("ephemeral_cache")) {
                  operand.put("ephemeralCache", true);
                }
              }
            }
          }
        }
      }
      
      return mapper.writeValueAsString(model);
    } catch (Exception e) {
      // If parsing fails, return original (shouldn't happen with valid JSON)
      return modelJson;
    }
  }
  
  /**
   * Builds a model JSON string with the test suite's default engine.
   * This ensures all tests use the configured engine unless they explicitly override.
   * 
   * @param schemaName the schema name
   * @param directory the directory path
   * @param additionalOperands additional operand entries as key-value pairs
   * @return the model JSON string
   */
  protected String buildModelWithEngine(String schemaName, String directory, String... additionalOperands) {
    StringBuilder model = new StringBuilder();
    model.append("{\n");
    model.append("  \"version\": \"1.0\",\n");
    model.append("  \"defaultSchema\": \"").append(schemaName).append("\",\n");
    model.append("  \"schemas\": [\n");
    model.append("    {\n");
    model.append("      \"name\": \"").append(schemaName).append("\",\n");
    model.append("      \"type\": \"custom\",\n");
    model.append("      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n");
    model.append("      \"operand\": {\n");
    model.append("        \"directory\": \"").append(directory.replace("\\", "\\\\")).append("\"");
    
    // Add test suite default engine if configured
    String engine = getExecutionEngine();
    if (engine != null && !engine.isEmpty()) {
      model.append(",\n        \"executionEngine\": \"").append(engine).append("\"");
    }
    
    // Add additional operands
    for (int i = 0; i < additionalOperands.length; i += 2) {
      if (i + 1 < additionalOperands.length) {
        model.append(",\n        \"").append(additionalOperands[i]).append("\": ");
        String value = additionalOperands[i + 1];
        // Check if value is already JSON (starts with { or [)
        if (value.trim().startsWith("{") || value.trim().startsWith("[")) {
          model.append(value);
        } else {
          model.append("\"").append(value).append("\"");
        }
      }
    }
    
    model.append("\n      }\n");
    model.append("    }\n");
    model.append("  ]\n");
    model.append("}\n");
    
    return model.toString();
  }
  
  /**
   * Builds a model JSON string with ephemeralCache enabled for test isolation.
   * This is the preferred method for tests to ensure proper test isolation.
   * 
   * @param schemaName the schema name
   * @param directory the directory path
   * @param additionalOperands additional operand entries as key-value pairs
   * @return the model JSON string with ephemeralCache enabled
   */
  protected String buildTestModel(String schemaName, String directory, String... additionalOperands) {
    String model = buildModelWithEngine(schemaName, directory, additionalOperands);
    return addEphemeralCacheToModel(model);
  }
  
  /**
   * Adds execution engine to operand map if configured.
   * 
   * @param operand the operand map to update
   */
  protected void addExecutionEngine(java.util.Map<String, Object> operand) {
    String engine = getExecutionEngine();
    if (engine != null && !engine.isEmpty()) {
      operand.put("executionEngine", engine);
    }
  }
}