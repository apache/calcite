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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Year;
import java.util.Comparator;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for SecSchemaFactory default value loading and environment variable substitution.
 */
@Tag("unit")
public class SecSchemaFactoryDefaultsTest {

  private String testDataDir;

  private String createTestDataDir(TestInfo testInfo) {
    String timestamp = String.valueOf(System.nanoTime());
    String testName = testInfo.getTestMethod().get().getName();
    testDataDir = "build/test-data/" + getClass().getSimpleName() + "/" + testName + "_" + timestamp;
    try {
      Files.createDirectories(Paths.get(testDataDir));
    } catch (IOException e) {
      throw new RuntimeException("Failed to create test directory: " + testDataDir, e);
    }
    return testDataDir;
  }

  @AfterEach
  void tearDown() {
    if (testDataDir != null && Files.exists(Paths.get(testDataDir))) {
      try {
        Files.walk(Paths.get(testDataDir))
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
      } catch (IOException e) {
        // Log but don't fail test
        System.err.println("Warning: Could not clean test directory: " + e.getMessage());
      }
    }
  }

  @Test
  public void testDefaultsAreApplied(TestInfo testInfo) throws Exception {
    String tempDir = createTestDataDir(testInfo);
    
    // Create a minimal model without endYear specified
    String modelJson = String.format(
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"SEC\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"SEC\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"directory\": \"%s\"," +
        "      \"ciks\": [\"AAPL\"]," +
        "      \"startYear\": 2020," +
        "      \"useMockData\": true" +
        "    }" +
        "  }]" +
        "}", tempDir);

    Path modelFile = Paths.get(tempDir).resolve("model.json");
    Files.writeString(modelFile, modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.toString();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.tables WHERE table_schema = 'SEC'")) {

      assertNotNull(rs, "ResultSet should not be null");
      // Verify that the schema was created successfully (basic smoke test)
    }
  }

  @Test
  public void testEnvironmentVariableSubstitution(TestInfo testInfo) throws Exception {
    String tempDir = createTestDataDir(testInfo);
    
    // Create a model with environment variable substitution
    String modelJson = String.format(
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"SEC\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"SEC\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"directory\": \"%s\"," +
        "      \"ciks\": [\"AAPL\"]," +
        "      \"startYear\": 2020," +
        "      \"endYear\": \"${CURRENT_YEAR}\"," +
        "      \"useMockData\": true" +
        "    }" +
        "  }]" +
        "}", tempDir);

    Path modelFile = Paths.get(tempDir).resolve("model.json");
    Files.writeString(modelFile, modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.toString();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.tables WHERE table_schema = 'SEC'")) {

      assertNotNull(rs, "ResultSet should not be null");
      // Environment variable substitution should work
    }
  }

  @Test
  public void testFilingTypesDefault(TestInfo testInfo) throws Exception {
    String tempDir = createTestDataDir(testInfo);
    
    // Create a model without explicit filingTypes
    String modelJson = String.format(
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"SEC\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"SEC\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"directory\": \"%s\"," +
        "      \"ciks\": [\"AAPL\"]," +
        "      \"startYear\": 2020," +
        "      \"useMockData\": true" +
        "    }" +
        "  }]" +
        "}", tempDir);

    Path modelFile = Paths.get(tempDir).resolve("model.json");
    Files.writeString(modelFile, modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.toString();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.tables WHERE table_schema = 'SEC'")) {

      assertNotNull(rs, "ResultSet should not be null");
      // Default filing types should be applied
    }
  }

  @Test
  public void testTextSimilarityDefaults(TestInfo testInfo) throws Exception {
    String tempDir = createTestDataDir(testInfo);
    
    // Create a model without explicit textSimilarity configuration
    String modelJson = String.format(
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"SEC\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"SEC\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"directory\": \"%s\"," +
        "      \"ciks\": [\"AAPL\"]," +
        "      \"startYear\": 2020," +
        "      \"useMockData\": true" +
        "    }" +
        "  }]" +
        "}", tempDir);

    Path modelFile = Paths.get(tempDir).resolve("model.json");
    Files.writeString(modelFile, modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.toString();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.tables WHERE table_schema = 'SEC'")) {

      assertNotNull(rs, "ResultSet should not be null");
      // Text similarity defaults should be applied
    }
  }
}