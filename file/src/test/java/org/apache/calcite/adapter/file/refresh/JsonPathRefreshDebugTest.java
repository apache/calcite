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

import org.apache.calcite.adapter.file.converters.FileConversionManager;
import org.apache.calcite.adapter.file.converters.JsonPathConverter;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test for JSONPath refresh mechanism.
 * Tests the individual components to understand why refresh is failing.
 */
@Tag("temp")
public class JsonPathRefreshDebugTest {

  @TempDir
  Path tempDir;
  
  private File schemaDir;
  
  @BeforeEach
  public void setupTestFiles() throws Exception {
    schemaDir = tempDir.toFile();
    
    // Set central metadata directory for this test
    ConversionMetadata.setCentralMetadataDirectory(schemaDir, "test");
  }
  
  @AfterEach
  public void cleanup() throws Exception {
    // Clear the central metadata directory after each test
    ConversionMetadata.setCentralMetadataDirectory(null, null);
  }
  
  @Test
  public void testFileConversionManagerHandlesJsonPath() throws Exception {
    System.out.println("\n=== DEBUG: FileConversionManager JSONPath Handling ===");
    
    // Create a source JSON file
    File sourceJsonFile = new File(schemaDir, "source_data.json");
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
    File extractedFile = new File(schemaDir, "users_data.json");
    JsonPathConverter.extract(sourceJsonFile, extractedFile, "$.data.users");
    
    System.out.println("Created JSONPath extraction:");
    System.out.println("  Source: " + sourceJsonFile.getName());
    System.out.println("  Extracted: " + extractedFile.getName());
    
    // Verify metadata was recorded
    ConversionMetadata metadata = new ConversionMetadata(schemaDir);
    File foundSource = metadata.findOriginalSource(extractedFile);
    assertNotNull(foundSource, "JSONPath conversion should be recorded");
    assertEquals(sourceJsonFile.getCanonicalPath(), foundSource.getCanonicalPath());
    
    // Read initial extracted content
    String initialContent = Files.readString(extractedFile.toPath());
    System.out.println("Initial extracted content: " + initialContent);
    assertTrue(initialContent.contains("Alice"));
    assertTrue(initialContent.contains("Bob"));
    
    // Now update the source file
    Thread.sleep(1100); // Ensure timestamp changes
    try (FileWriter writer = new FileWriter(sourceJsonFile, StandardCharsets.UTF_8)) {
      writer.write("{\n");
      writer.write("  \"data\": {\n");
      writer.write("    \"users\": [\n");
      writer.write("      {\"id\": 1, \"name\": \"Alice\"},\n");
      writer.write("      {\"id\": 2, \"name\": \"Bob\"},\n");
      writer.write("      {\"id\": 3, \"name\": \"Charlie\"},\n");
      writer.write("      {\"id\": 4, \"name\": \"Diana\"}\n");
      writer.write("    ]\n");
      writer.write("  }\n");
      writer.write("}\n");
    }
    sourceJsonFile.setLastModified(System.currentTimeMillis());
    
    System.out.println("Updated source file with Charlie and Diana");
    
    // Test FileConversionManager.convertIfNeeded
    boolean wasConverted = FileConversionManager.convertIfNeeded(sourceJsonFile, schemaDir, "lower_snake");
    System.out.println("FileConversionManager.convertIfNeeded returned: " + wasConverted);
    
    // Check if the extracted file was updated
    String updatedContent = Files.readString(extractedFile.toPath());
    System.out.println("Updated extracted content: " + updatedContent);
    
    if (updatedContent.contains("Charlie") && updatedContent.contains("Diana")) {
      System.out.println("✅ JSONPath refresh working via FileConversionManager");
    } else {
      System.out.println("❌ JSONPath refresh NOT working via FileConversionManager");
    }
    
    // For debugging, let's also check the conversion record
    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(extractedFile);
    if (record != null) {
      System.out.println("Conversion record:");
      System.out.println("  Type: " + record.getConversionType());
      System.out.println("  Original: " + record.getOriginalPath());
      System.out.println("  Converted: " + record.convertedFile);
    } else {
      System.out.println("❌ No conversion record found!");
    }
  }
}