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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.adapter.file.converters.JsonPathConverter;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.parallel.Isolated;import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Isolated;import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.Isolated;import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to verify JSONPath conversion tracking works.
 * This is a basic test to verify that JsonPathConverter properly records metadata.
 */
@Tag("unit")
@Isolated  // Required due to engine-specific behavior and shared state
public class SimpleJsonPathRefreshTest {

  @TempDir
  Path tempDir;
  
  private File schemaDir;
  
  @BeforeEach
  public void setupTestFiles() throws Exception {
    schemaDir = tempDir.toFile();
    
    // Don't use central metadata directory for tests - use local metadata files
    // This avoids conflicts when tests run in parallel
    ConversionMetadata.setCentralMetadataDirectory(null, null);
  }
  
  @AfterEach
  public void cleanup() throws Exception {
    // Clear the central metadata directory after each test
    ConversionMetadata.setCentralMetadataDirectory(null, null);
  }
  
  @Test
  public void testJsonPathConversionRecordsMetadata() throws Exception {
    System.out.println("\n=== TEST: JSONPath Conversion Metadata Recording ===");
    
    // Create a source JSON file
    File sourceJsonFile = new File(schemaDir, "source.json");
    try (FileWriter writer = new FileWriter(sourceJsonFile, StandardCharsets.UTF_8)) {
      writer.write("{\n");
      writer.write("  \"data\": {\n");
      writer.write("    \"users\": [\n");
      writer.write("      {\"id\": 1, \"name\": \"Alice\"},\n");
      writer.write("      {\"id\": 2, \"name\": \"Bob\"}\n");
      writer.write("    ]\n");
      writer.write("  }\n");
      writer.write("}\n");
    }
    
    // Extract using JSONPath
    File extractedFile = new File(schemaDir, "users_extracted.json");
    JsonPathConverter.extract(sourceJsonFile, extractedFile, "$.data.users");
    
    // Verify extraction worked
    assertTrue(extractedFile.exists(), "Extracted file should exist");
    String content = Files.readString(extractedFile.toPath());
    System.out.println("Extracted content: " + content);
    assertTrue(content.contains("Alice"), "Should contain Alice");
    assertTrue(content.contains("Bob"), "Should contain Bob");
    
    // Verify metadata was recorded
    ConversionMetadata metadata = new ConversionMetadata(schemaDir);
    File foundSource = metadata.findOriginalSource(extractedFile);
    assertNotNull(foundSource, "JSONPath conversion should be recorded in metadata");
    assertEquals(sourceJsonFile.getCanonicalPath(), foundSource.getCanonicalPath());
    
    System.out.println("✅ JSONPath conversion automatically recorded metadata");
    System.out.println("  Source: " + sourceJsonFile.getName());
    System.out.println("  Extracted: " + extractedFile.getName());
    System.out.println("  Found source: " + foundSource.getName());
  }
  
  @Test
  public void testJsonPathExtractionFileContent() throws Exception {
    System.out.println("\n=== TEST: JSONPath Extraction File Content ===");
    
    // Create a more complex source JSON
    File sourceJsonFile = new File(schemaDir, "api_response.json");
    try (FileWriter writer = new FileWriter(sourceJsonFile, StandardCharsets.UTF_8)) {
      writer.write("{\n");
      writer.write("  \"status\": \"success\",\n");
      writer.write("  \"data\": {\n");
      writer.write("    \"users\": [\n");
      writer.write("      {\"id\": 1, \"name\": \"Alice\", \"role\": \"admin\"},\n");
      writer.write("      {\"id\": 2, \"name\": \"Bob\", \"role\": \"user\"}\n");
      writer.write("    ],\n");
      writer.write("    \"products\": [\n");
      writer.write("      {\"id\": \"P001\", \"name\": \"Laptop\", \"price\": 999.99}\n");
      writer.write("    ]\n");
      writer.write("  }\n");
      writer.write("}\n");
    }
    
    // Extract users
    File usersFile = new File(schemaDir, "users_only.json");
    JsonPathConverter.extract(sourceJsonFile, usersFile, "$.data.users");
    
    // Extract products  
    File productsFile = new File(schemaDir, "products_only.json");
    JsonPathConverter.extract(sourceJsonFile, productsFile, "$.data.products");
    
    // Verify users extraction
    assertTrue(usersFile.exists(), "Users file should exist");
    String usersContent = Files.readString(usersFile.toPath());
    System.out.println("Users content: " + usersContent);
    assertTrue(usersContent.contains("Alice"), "Should contain Alice");
    assertTrue(usersContent.contains("admin"), "Should contain admin role");
    assertFalse(usersContent.contains("Laptop"), "Should not contain product data");
    
    // Verify products extraction
    assertTrue(productsFile.exists(), "Products file should exist");
    String productsContent = Files.readString(productsFile.toPath());
    System.out.println("Products content: " + productsContent);
    assertTrue(productsContent.contains("Laptop"), "Should contain Laptop");
    assertTrue(productsContent.contains("999.99"), "Should contain price");
    assertFalse(productsContent.contains("Alice"), "Should not contain user data");
    
    // Verify both conversions were recorded
    ConversionMetadata metadata = new ConversionMetadata(schemaDir);
    
    File usersSource = metadata.findOriginalSource(usersFile);
    assertNotNull(usersSource, "Users conversion should be recorded");
    assertEquals(sourceJsonFile.getCanonicalPath(), usersSource.getCanonicalPath());
    
    File productsSource = metadata.findOriginalSource(productsFile);
    assertNotNull(productsSource, "Products conversion should be recorded");
    assertEquals(sourceJsonFile.getCanonicalPath(), productsSource.getCanonicalPath());
    
    System.out.println("✅ Multiple JSONPath extractions from same source recorded correctly");
  }
}