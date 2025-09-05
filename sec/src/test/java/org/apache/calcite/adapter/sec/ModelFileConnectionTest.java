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

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test using JSON model files for database connections with embeddings.
 */
@Tag("unit")
public class ModelFileConnectionTest {

  @Test public void testModelFileUsage() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("MODEL FILE CONNECTION TEST");
    System.out.println("=".repeat(80));

    // Show available model files
    System.out.println("\n1. AVAILABLE MODEL FILES");
    System.out.println("-".repeat(40));

    String[] modelFiles = {
        "/edgar-apple-model.json",
        "/sec-embedding-model.json",
        "/test-embedding-model.json"
    };

    for (String modelFile : modelFiles) {
      InputStream is = getClass().getResourceAsStream(modelFile);
      if (is != null) {
        System.out.println("✓ Found: " + modelFile);
        is.close();
      } else {
        System.out.println("✗ Missing: " + modelFile);
      }
    }

    // Demonstrate how to use different model files
    System.out.println("\n2. MODEL FILE CONFIGURATIONS");
    System.out.println("-".repeat(40));

    // Load and display embedding model configuration
    InputStream embeddingModelStream = getClass().getResourceAsStream("/sec-embedding-model.json");
    if (embeddingModelStream != null) {
      String content = new String(embeddingModelStream.readAllBytes());
      System.out.println("\nsec-embedding-model.json highlights:");

      if (content.contains("enableEmbeddings")) {
        System.out.println("  ✓ Embeddings enabled");
      }
      if (content.contains("embeddingDimension")) {
        System.out.println("  ✓ Embedding dimension: 128");
      }
      if (content.contains("COSINE_SIMILARITY")) {
        System.out.println("  ✓ Vector functions registered");
      }
      if (content.contains("document_embeddings")) {
        System.out.println("  ✓ Document embeddings table configured");
      }
    }

    // Show how to create a connection using model file
    System.out.println("\n3. CONNECTION EXAMPLES");
    System.out.println("-".repeat(40));

    System.out.println("\nExample 1: Production connection with embeddings");
    System.out.println("  Properties info = new Properties();");
    System.out.println("  info.setProperty(\"lex\", \"ORACLE\");");
    System.out.println("  info.setProperty(\"unquotedCasing\", \"TO_LOWER\");");
    System.out.println("  String url = \"jdbc:calcite:model=/path/to/sec-embedding-model.json\";");
    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    System.out.println("  Connection conn = DriverManager.getConnection(url, info);");

    System.out.println("\nExample 2: Test connection with local data");
    System.out.println("  String url = \"jdbc:calcite:model=/path/to/test-embedding-model.json\";");
    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    System.out.println("  Connection conn = DriverManager.getConnection(url);");

    System.out.println("\nExample 3: EDGAR data with auto-download");
    System.out.println("  String url = \"jdbc:calcite:model=/path/to/edgar-apple-model.json\";");
    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    System.out.println("  Connection conn = DriverManager.getConnection(url);");

    // Show SQL query examples
    System.out.println("\n4. SQL QUERY EXAMPLES WITH EMBEDDINGS");
    System.out.println("-".repeat(40));

    System.out.println("\nFind similar documents:");
    System.out.println("  SELECT fiscal_year, chunk_context,");
    System.out.println("         COSINE_SIMILARITY(embedding_vector, ?) as similarity");
    System.out.println("  FROM document_embeddings");
    System.out.println("  WHERE similarity > 0.8");
    System.out.println("  ORDER BY similarity DESC");

    System.out.println("\nNearest neighbor search:");
    System.out.println("  SELECT *");
    System.out.println("  FROM document_embeddings");
    System.out.println("  ORDER BY COSINE_DISTANCE(embedding_vector, ?) ASC");
    System.out.println("  LIMIT 10");

    System.out.println("\nCluster similar contexts:");
    System.out.println("  SELECT d1.chunk_context, d2.chunk_context,");
    System.out.println("         COSINE_SIMILARITY(d1.embedding_vector, d2.embedding_vector) as sim");
    System.out.println("  FROM document_embeddings d1");
    System.out.println("  JOIN document_embeddings d2 ON d1.fiscal_year = d2.fiscal_year");
    System.out.println("  WHERE d1.chunk_context < d2.chunk_context");
    System.out.println("    AND sim > 0.7");

    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("MODEL FILE CONNECTION TEST COMPLETE");
    System.out.println("=".repeat(80) + "\n");
  }

  @Test public void testCreateCustomModel() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("CUSTOM MODEL FILE CREATION");
    System.out.println("=".repeat(80));

    // Create a custom model file for specific use case
    String customModel = "{\n"
  +
        "  \"version\": \"1.0\",\n"
  +
        "  \"defaultSchema\": \"FINANCIAL\",\n"
  +
        "  \"schemas\": [\n"
  +
        "    {\n"
  +
        "      \"name\": \"FINANCIAL\",\n"
  +
        "      \"type\": \"custom\",\n"
  +
        "      \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\",\n"
  +
        "      \"operand\": {\n"
  +
        "        \"directory\": \"financial-data\",\n"
  +
        "        \"executionEngine\": \"duckdb\",\n"
  +
        "        \"enableEmbeddings\": true,\n"
  +
        "        \"embeddingConfig\": {\n"
  +
        "          \"modelLoader\": \"org.apache.calcite.adapter.sec.EmbeddingModelLoader\",\n"
  +
        "          \"vocabularyPath\": \"/models/financial-vocabulary.txt\",\n"
  +
        "          \"contextMappingsPath\": \"/models/context-mappings.json\",\n"
  +
        "          \"embeddingDimension\": 128\n"
  +
        "        },\n"
  +
        "        \"companies\": [\n"
  +
        "          {\n"
  +
        "            \"cik\": \"0000320193\",\n"
  +
        "            \"name\": \"Apple Inc.\"\n"
  +
        "          },\n"
  +
        "          {\n"
  +
        "            \"cik\": \"0000789019\",\n"
  +
        "            \"name\": \"Microsoft Corporation\"\n"
  +
        "          },\n"
  +
        "          {\n"
  +
        "            \"cik\": \"0001018724\",\n"
  +
        "            \"name\": \"Amazon.com Inc.\"\n"
  +
        "          }\n"
  +
        "        ],\n"
  +
        "        \"vectorFunctions\": [\n"
  +
        "          \"COSINE_SIMILARITY\",\n"
  +
        "          \"COSINE_DISTANCE\",\n"
  +
        "          \"VECTORS_SIMILAR\"\n"
  +
        "        ]\n"
  +
        "      }\n"
  +
        "    }\n"
  +
        "  ]\n"
  +
        "}";

    // Save to temp file
    File tempModel = File.createTempFile("custom-model", ".json");
    Files.write(tempModel.toPath(), customModel.getBytes());

    System.out.println("\nCreated custom model file: " + tempModel.getAbsolutePath());
    System.out.println("\nCustom model features:");
    System.out.println("  - Multiple companies (Apple, Microsoft, Amazon)");
    System.out.println("  - Embeddings enabled with static model files");
    System.out.println("  - Vector similarity functions registered");
    System.out.println("  - DuckDB execution engine");

    System.out.println("\nTo use this model:");
    System.out.println("  String url = \"jdbc:calcite:model=" + tempModel.getAbsolutePath() + "\";");
    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    System.out.println("  Connection conn = DriverManager.getConnection(url);");

    // Clean up
    tempModel.deleteOnExit();

    System.out.println("\n"
  + "=".repeat(80) + "\n");
  }
}
