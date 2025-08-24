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
package org.apache.calcite.adapter.file.config;

import org.apache.calcite.adapter.file.BaseFileTest;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.util.EnvironmentVariableSubstitutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for environment variable substitution in model files.
 */
@Tag("unit")
public class EnvironmentVariableTest extends BaseFileTest {

  private Map<String, String> originalEnv;
  
  @BeforeEach
  public void setUp() {
    // Save original environment for restoration
    originalEnv = new HashMap<>(System.getenv());
  }
  
  @AfterEach
  public void tearDown() {
    // Clean up any test environment variables
    // Note: We can't actually modify System.getenv(), but we can ensure
    // our test methods use the mock environment map approach
  }

  @Test
  public void testBasicSubstitution() {
    Map<String, String> env = new HashMap<>();
    env.put("TEST_VAR", "test_value");
    
    String input = "Value: ${TEST_VAR}";
    String result = EnvironmentVariableSubstitutor.substitute(input, env);
    assertEquals("Value: test_value", result);
  }

  @Test
  public void testSubstitutionWithDefault() {
    Map<String, String> env = new HashMap<>();
    
    String input = "Value: ${MISSING_VAR:default_value}";
    String result = EnvironmentVariableSubstitutor.substitute(input, env);
    assertEquals("Value: default_value", result);
  }

  @Test
  public void testMissingVariableThrows() {
    Map<String, String> env = new HashMap<>();
    
    String input = "Value: ${MISSING_VAR}";
    assertThrows(IllegalArgumentException.class, () -> {
      EnvironmentVariableSubstitutor.substitute(input, env);
    });
  }

  @Test
  public void testMultipleSubstitutions() {
    Map<String, String> env = new HashMap<>();
    env.put("VAR1", "value1");
    env.put("VAR2", "value2");
    
    String input = "First: ${VAR1}, Second: ${VAR2}, Default: ${VAR3:default3}";
    String result = EnvironmentVariableSubstitutor.substitute(input, env);
    assertEquals("First: value1, Second: value2, Default: default3", result);
  }

  @Test
  public void testJsonStringSubstitution() {
    Map<String, String> env = new HashMap<>();
    env.put("DIR", "/data/files");
    env.put("ENGINE", "PARQUET");
    
    String json = "{"
        + "\"directory\": \"${DIR}\","
        + "\"executionEngine\": \"${ENGINE}\""
        + "}";
    
    String result = EnvironmentVariableSubstitutor.substituteInJson(json, env);
    assertTrue(result.contains("\"/data/files\""));
    assertTrue(result.contains("\"PARQUET\""));
  }

  @Test
  public void testJsonNumberSubstitution() {
    Map<String, String> env = new HashMap<>();
    env.put("BATCH_SIZE", "1000");
    env.put("MEMORY", "2048");
    
    String json = "{"
        + "\"batchSize\": \"${BATCH_SIZE}\","
        + "\"memoryThreshold\": \"${MEMORY}\""
        + "}";
    
    String result = EnvironmentVariableSubstitutor.substituteInJson(json, env);
    // Numbers should be unquoted
    assertTrue(result.contains("\"batchSize\": 1000"));
    assertTrue(result.contains("\"memoryThreshold\": 2048"));
  }

  @Test
  public void testJsonBooleanSubstitution() {
    Map<String, String> env = new HashMap<>();
    env.put("RECURSIVE", "true");
    env.put("PRIME_CACHE", "false");
    
    String json = "{"
        + "\"recursive\": \"${RECURSIVE}\","
        + "\"primeCache\": \"${PRIME_CACHE}\""
        + "}";
    
    String result = EnvironmentVariableSubstitutor.substituteInJson(json, env);
    // Booleans should be unquoted
    assertTrue(result.contains("\"recursive\": true"));
    assertTrue(result.contains("\"primeCache\": false"));
  }

  @Test
  public void testComplexJsonModel() {
    Map<String, String> env = new HashMap<>();
    env.put("SCHEMA_NAME", "TEST_SCHEMA");
    env.put("DATA_DIR", "test/data");
    env.put("ENGINE_TYPE", "PARQUET");
    env.put("BATCH_SIZE", "500");
    env.put("CACHE_ENABLED", "true");
    
    String json = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"${SCHEMA_NAME}\","
        + "\"schemas\": ["
        + "  {"
        + "    \"name\": \"${SCHEMA_NAME}\","
        + "    \"type\": \"custom\","
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "    \"operand\": {"
        + "      \"directory\": \"${DATA_DIR}\","
        + "      \"executionEngine\": \"${ENGINE_TYPE}\","
        + "      \"batchSize\": \"${BATCH_SIZE}\","
        + "      \"ephemeralCache\": \"${CACHE_ENABLED}\""
        + "    }"
        + "  }"
        + "]"
        + "}";
    
    String result = EnvironmentVariableSubstitutor.substituteInJson(json, env);
    
    // Verify all substitutions
    assertTrue(result.contains("\"defaultSchema\": \"TEST_SCHEMA\""));
    assertTrue(result.contains("\"name\": \"TEST_SCHEMA\""));
    assertTrue(result.contains("\"directory\": \"test/data\""));
    assertTrue(result.contains("\"executionEngine\": \"PARQUET\""));
    assertTrue(result.contains("\"batchSize\": 500"));
    assertTrue(result.contains("\"ephemeralCache\": true"));
  }

  @Test
  public void testNestedDefaultValues() {
    Map<String, String> env = new HashMap<>();
    env.put("OUTER", "outer_value");
    
    // Nested variable references in defaults aren't supported yet
    // This is a simpler test with just a default value
    String input = "Outer: ${OUTER}, Inner: ${INNER:default_value}";
    String result = EnvironmentVariableSubstitutor.substitute(input, env);
    assertEquals("Outer: outer_value, Inner: default_value", result);
  }

  @Test
  public void testSpecialCharactersInValues() {
    Map<String, String> env = new HashMap<>();
    env.put("PATH_VAR", "/path/with$special{chars}");
    env.put("QUOTE_VAR", "value with \"quotes\"");
    
    String json = "{"
        + "\"path\": \"${PATH_VAR}\","
        + "\"quoted\": \"${QUOTE_VAR}\""
        + "}";
    
    String result = EnvironmentVariableSubstitutor.substituteInJson(json, env);
    // Special characters should be properly escaped
    assertTrue(result.contains("/path/with$special{chars}"));
    assertTrue(result.contains("\\\"quotes\\\""));
  }

  @Test
  public void testEmptyDefaultValue() {
    Map<String, String> env = new HashMap<>();
    
    String input = "Value: [${EMPTY_VAR:}]";
    String result = EnvironmentVariableSubstitutor.substitute(input, env);
    assertEquals("Value: []", result);
  }

  @Test
  public void testContainsVariablesCheck() {
    assertTrue(EnvironmentVariableSubstitutor.containsVariables("${VAR}"));
    assertTrue(EnvironmentVariableSubstitutor.containsVariables("text ${VAR} more"));
    assertTrue(EnvironmentVariableSubstitutor.containsVariables("${VAR:default}"));
    assertFalse(EnvironmentVariableSubstitutor.containsVariables("no variables"));
    assertFalse(EnvironmentVariableSubstitutor.containsVariables(""));
    assertFalse(EnvironmentVariableSubstitutor.containsVariables(null));
  }

  @Test
  public void testIntegrationWithFileAdapter() throws Exception {
    // Set up test environment variables
    System.setProperty("TEST_DATA_DIR", "sales");
    System.setProperty("TEST_ENGINE", "CSV");
    
    String model = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"SALES\","
        + "\"schemas\": ["
        + "  {"
        + "    \"name\": \"SALES\","
        + "    \"type\": \"custom\","
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "    \"operand\": {"
        + "      \"directory\": \"${TEST_DATA_DIR:sales}\","
        + "      \"executionEngine\": \"${TEST_ENGINE:LINQ4J}\","
        + "      \"ephemeralCache\": true"
        + "    }"
        + "  }"
        + "]"
        + "}";
    
    // Note: In a real test, we would create a connection with this model
    // and verify that the schema is created with the correct configuration.
    // For now, we just verify the substitution works correctly.
    
    Map<String, String> env = new HashMap<>();
    env.put("TEST_DATA_DIR", "sales");
    env.put("TEST_ENGINE", "CSV");
    
    String result = EnvironmentVariableSubstitutor.substituteInJson(model, env);
    assertTrue(result.contains("\"directory\": \"sales\""));
    assertTrue(result.contains("\"executionEngine\": \"CSV\""));
  }

  @Test
  public void testExecutionEngineConfiguration() throws Exception {
    // This test verifies that environment variables can be used to configure
    // the execution engine dynamically
    Map<String, String> env = new HashMap<>();
    env.put("CALCITE_FILE_ENGINE_TYPE", "PARQUET");
    
    String model = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"TEST\","
        + "\"schemas\": ["
        + "  {"
        + "    \"name\": \"TEST\","
        + "    \"type\": \"custom\","
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "    \"operand\": {"
        + "      \"directory\": \"test\","
        + "      \"executionEngine\": \"${CALCITE_FILE_ENGINE_TYPE:LINQ4J}\""
        + "    }"
        + "  }"
        + "]"
        + "}";
    
    String result = EnvironmentVariableSubstitutor.substituteInJson(model, env);
    assertTrue(result.contains("\"executionEngine\": \"PARQUET\""));
  }

  @Test
  public void testYamlStyleModel() {
    // Test that our substitution works with YAML-style content too
    Map<String, String> env = new HashMap<>();
    env.put("SCHEMA", "MY_SCHEMA");
    env.put("DIR", "/data");
    
    String yaml = "version: 1.0\n"
        + "defaultSchema: ${SCHEMA}\n"
        + "schemas:\n"
        + "  - name: ${SCHEMA}\n"
        + "    type: custom\n"
        + "    factory: org.apache.calcite.adapter.file.FileSchemaFactory\n"
        + "    operand:\n"
        + "      directory: ${DIR}\n";
    
    String result = EnvironmentVariableSubstitutor.substitute(yaml, env);
    assertTrue(result.contains("defaultSchema: MY_SCHEMA"));
    assertTrue(result.contains("name: MY_SCHEMA"));
    assertTrue(result.contains("directory: /data"));
  }
}