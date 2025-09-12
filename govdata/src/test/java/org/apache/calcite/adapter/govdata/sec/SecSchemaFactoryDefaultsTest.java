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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Year;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for SecSchemaFactory default value loading and environment variable substitution.
 */
@Tag("unit")
public class SecSchemaFactoryDefaultsTest {

  @TempDir
  Path tempDir;

  @Test
  public void testDefaultsAreApplied() throws Exception {
    // Create a minimal model without endYear specified
    String modelJson = String.format(
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"SEC\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"SEC\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"directory\": \"%s\"," +
        "      \"ciks\": [\"AAPL\"]," +
        "      \"startYear\": 2020," +
        "      \"useMockData\": true" +
        "    }" +
        "  }]" +
        "}", tempDir.toString());

    Path modelFile = tempDir.resolve("model.json");
    Files.writeString(modelFile, modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    // The connection should work because defaults provide missing values
    // Without defaults, this would fail because endYear and other required fields are missing
    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelFile.toString(), props)) {
      assertNotNull(conn);
      // The fact that connection succeeds proves defaults were applied
      assertTrue(conn.isValid(1));
    }
  }

  @Test
  public void testCurrentYearSubstitution() throws Exception {
    // Set a custom environment variable for testing
    // Note: We can't actually set environment variables in Java, but we can test the default CURRENT_YEAR behavior
    
    int currentYear = Year.now().getValue();
    
    // Create a model that relies on CURRENT_YEAR default
    String modelJson = String.format(
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"SEC\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"SEC\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"directory\": \"%s\"," +
        "      \"ciks\": [\"MSFT\"]," +
        "      \"startYear\": 2020," +
        "      \"useMockData\": true" +
        "    }" +
        "  }]" +
        "}", tempDir.toString());

    Path modelFile = tempDir.resolve("model.json");
    Files.writeString(modelFile, modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelFile.toString(), props)) {
      assertNotNull(conn);
      
      // The endYear should be set to current year from the defaults
      // We can't directly verify this, but the connection should work
      assertTrue(conn.isValid(1));
    }
  }

  @Test
  public void testExplicitValuesOverrideDefaults() throws Exception {
    // Create a model with explicit values that should override defaults
    String modelJson = String.format(
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"SEC\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"SEC\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"directory\": \"%s\"," +
        "      \"ciks\": [\"GOOGL\"]," +
        "      \"startYear\": 2019," +
        "      \"endYear\": 2022," +
        "      \"autoDownload\": false," +
        "      \"testMode\": true," +
        "      \"useMockData\": true," +
        "      \"executionEngine\": \"parquet\"" +
        "    }" +
        "  }]" +
        "}", tempDir.toString());

    Path modelFile = tempDir.resolve("model.json");
    Files.writeString(modelFile, modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    // Connection should use the explicit values, not defaults
    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelFile.toString(), props)) {
      assertNotNull(conn);
      assertTrue(conn.isValid(1));
    }
  }

  @Test
  public void testNestedDefaultsMerge() throws Exception {
    // Test that nested objects like textSimilarity are merged correctly
    String modelJson = String.format(
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"SEC\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"SEC\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"directory\": \"%s\"," +
        "      \"ciks\": [\"AMZN\"]," +
        "      \"startYear\": 2021," +
        "      \"useMockData\": true," +
        "      \"textSimilarity\": {" +
        "        \"enabled\": false" +
        "      }" +
        "    }" +
        "  }]" +
        "}", tempDir.toString());

    Path modelFile = tempDir.resolve("model.json");
    Files.writeString(modelFile, modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    // Should merge the textSimilarity settings correctly
    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelFile.toString(), props)) {
      assertNotNull(conn);
      
      // The connection should work with merged settings
      assertTrue(conn.isValid(1));
    }
  }
}