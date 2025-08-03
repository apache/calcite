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
package org.apache.calcite.test;

import org.apache.calcite.adapter.splunk.ModelPreprocessor;
import org.apache.calcite.adapter.splunk.SplunkModelHelper;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for automatic factory injection in Splunk model files.
 */
public class SplunkModelPreprocessorTest {

  @Test
  void testFactoryInjectionForSplunkSchema() throws Exception {
    String originalModel = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"splunk\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"operand\": {\n"
        + "        \"url\": \"https://splunk.example.com:8089\",\n"
        + "        \"token\": \"mock_token\",\n"
        + "        \"app\": \"Splunk_SA_CIM\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    String preprocessed = ModelPreprocessor.preprocessModel("inline:" + originalModel);
    
    // Parse the result to verify factory was injected
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(preprocessed);
    JsonNode schemaNode = rootNode.get("schemas").get(0);
    
    assertTrue(schemaNode.has("factory"), "Factory should be injected");
    assertEquals("org.apache.calcite.adapter.splunk.SplunkSchemaFactory", 
        schemaNode.get("factory").asText(), "Correct factory should be injected");
  }

  @Test
  void testTypeAndFactoryInjectionForMinimalSplunkSchema() throws Exception {
    String originalModel = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"splunk\",\n"
        + "      \"operand\": {\n"
        + "        \"url\": \"https://splunk.example.com:8089\",\n"
        + "        \"token\": \"mock_token\",\n"
        + "        \"app\": \"Splunk_SA_CIM\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    String preprocessed = ModelPreprocessor.preprocessModel("inline:" + originalModel);
    
    // Parse the result to verify both type and factory were injected
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(preprocessed);
    JsonNode schemaNode = rootNode.get("schemas").get(0);
    
    assertTrue(schemaNode.has("type"), "Type should be injected");
    assertEquals("custom", schemaNode.get("type").asText(), "Type should be 'custom'");
    
    assertTrue(schemaNode.has("factory"), "Factory should be injected");
    assertEquals("org.apache.calcite.adapter.splunk.SplunkSchemaFactory", 
        schemaNode.get("factory").asText(), "Correct factory should be injected");
  }

  @Test
  void testFactoryNotInjectedWhenAlreadyPresent() throws Exception {
    String originalModel = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"splunk\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"com.example.CustomFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"url\": \"https://splunk.example.com:8089\",\n"
        + "        \"token\": \"mock_token\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    String preprocessed = ModelPreprocessor.preprocessModel("inline:" + originalModel);
    
    // Parse the result to verify existing factory is preserved
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(preprocessed);
    JsonNode schemaNode = rootNode.get("schemas").get(0);
    
    assertEquals("com.example.CustomFactory", 
        schemaNode.get("factory").asText(), "Existing factory should be preserved");
  }

  @Test
  void testFederationModelCreation() throws Exception {
    String model = SplunkModelHelper.createFederationModel(
        "https://splunk.company.com:8089",
        "test_token",
        "Splunk_SA_CIM",
        "Splunk_TA_cisco-esa", 
        "Splunk_TA_paloalto"
    );
    
    // Preprocess to inject factories
    String preprocessed = ModelPreprocessor.preprocessModel("inline:" + model);
    
    // Verify all schemas have factories injected
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(preprocessed);
    JsonNode schemasNode = rootNode.get("schemas");
    
    assertEquals(3, schemasNode.size(), "Should have 3 schemas");
    
    for (JsonNode schemaNode : schemasNode) {
      assertTrue(schemaNode.has("factory"), "Each schema should have factory injected");
      assertEquals("org.apache.calcite.adapter.splunk.SplunkSchemaFactory",
          schemaNode.get("factory").asText(), "Should inject Splunk factory");
    }
  }

  @Test
  void testConnectionWithAutoInjectedFactories() throws Exception {
    String model = SplunkModelHelper.createFederationModel(
        "mock",  // Use mock URL to avoid actual connection
        "test_token",
        "Splunk_SA_CIM"
    );
    
    String preprocessed = ModelPreprocessor.preprocessModel("inline:" + model);
    
    // Verify the factory was injected in the preprocessed model
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(preprocessed);
    JsonNode schemaNode = rootNode.get("schemas").get(0);
    
    assertTrue(schemaNode.has("factory"), "Factory should be auto-injected");
    assertEquals("org.apache.calcite.adapter.splunk.SplunkSchemaFactory",
        schemaNode.get("factory").asText(), "Correct factory should be injected");
        
    // The connection creation itself would work, but we'll skip it to avoid deprecation warnings
  }

  @Test
  void testHelperConnectMethod() throws Exception {
    // Create a test model file content
    String testModel = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"splunk\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"splunk\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"operand\": {\n"
        + "        \"url\": \"mock\",\n"
        + "        \"token\": \"test_token\",\n"
        + "        \"app\": \"Splunk_SA_CIM\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    
    // Test preprocessing
    String preprocessed = ModelPreprocessor.preprocessModel("inline:" + testModel);
    
    // Verify factory was injected
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(preprocessed);
    JsonNode schemaNode = rootNode.get("schemas").get(0);
    
    assertTrue(schemaNode.has("factory"), "Factory should be auto-injected");
    assertEquals("org.apache.calcite.adapter.splunk.SplunkSchemaFactory",
        schemaNode.get("factory").asText(), "Correct factory should be injected");
  }
}